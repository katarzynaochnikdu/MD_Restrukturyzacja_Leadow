"""
Skrypt do masowego tagowania firm (Accounts) na podstawie listy ID z pliku CSV/XLSX.
Uproszczona wersja - taguje tylko podane ID (bez hierarchii).
"""

import csv
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
except ImportError:
    print("Błąd: Wymagana biblioteka pandas nie jest zainstalowana.", file=sys.stderr)
    print("Proszę ją zainstalować komendą: pip install pandas", file=sys.stderr)
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from refresh_zoho_access_token import refresh_access_token

# --- Konfiguracja API Zoho ---
ZOHO_CRM_API_BASE_URL = "https://www.zohoapis.eu/crm/v8"
ACCESS_TOKEN_HOLDER: Dict[str, Any] = {"token": None, "refreshed_at": 0.0}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("tag_accounts.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)


def get_access_token() -> str:
    """Pobiera token dostępowy."""
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN")

    token_info = refresh_access_token(client_id, client_secret, refresh_token)
    ACCESS_TOKEN_HOLDER["token"] = token_info["access_token"]
    ACCESS_TOKEN_HOLDER["refreshed_at"] = time.time()
    return token_info["access_token"]


def execute_api_request(access_token: str, url: str, method: str = "GET", data: Optional[Dict] = None) -> Optional[Dict]:
    """Wykonuje żądanie API."""
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    
    if method == "POST":
        headers["Content-Type"] = "application/json"
        json_data = json.dumps(data).encode("utf-8") if data else b""
        request = urllib.request.Request(url, data=json_data, headers=headers, method="POST")
    else:
        request = urllib.request.Request(url, headers=headers, method="GET")
    
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_data = response.read().decode("utf-8")
            if not response_data:
                return None
            return json.loads(response_data)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except:
            pass
        logging.error(f"HTTPError {e.code}: {body}")
        return None
    except Exception as e:
        logging.error(f"Błąd API: {e}")
        return None


def get_available_tags(access_token: str) -> List[Dict[str, str]]:
    """Pobiera listę dostępnych tagów dla modułu Accounts."""
    url = f"{ZOHO_CRM_API_BASE_URL}/settings/tags?module=Accounts"
    response = execute_api_request(access_token, url, method="GET")
    
    tags = []
    if response and "tags" in response:
        for tag in response["tags"]:
            tags.append({
                "id": str(tag.get("id", "")),
                "name": tag.get("name", ""),
                "color_code": tag.get("color_code", "")
            })
    return tags


def add_tags_to_accounts(access_token: str, account_ids: List[str], tag_name: str) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Dodaje tag do listy firm. Zwraca (sukcesy, błędy)."""
    successes = []
    failures = []
    
    # Batch po 100 ID
    for i in range(0, len(account_ids), 100):
        batch = account_ids[i:i+100]
        params = {"ids": ",".join(batch)}
        url = f"{ZOHO_CRM_API_BASE_URL}/Accounts/actions/add_tags?{urllib.parse.urlencode(params)}"
        body = {"tags": [{"name": tag_name}]}
        
        response = execute_api_request(access_token, url, method="POST", data=body)
        
        if response and "data" in response:
            for item in response["data"]:
                rid = str(item.get("details", {}).get("id", ""))
                status = (item.get("status") or "").upper()
                if status == "SUCCESS" and rid:
                    successes.append(rid)
                else:
                    failures.append((rid or "N/A", item.get("code", "Błąd")))
        else:
            for rid in batch:
                failures.append((rid, "Brak odpowiedzi API"))
        
        time.sleep(0.2)
    
    return successes, failures


def load_ids_from_file(file_path: str) -> List[str]:
    """Wczytuje ID z pliku CSV/XLSX."""
    # Obsługa ścieżek URL
    if file_path.startswith("file:///"):
        file_path = urllib.parse.unquote(file_path[8:]).replace("/", "\\")
    
    ext = os.path.splitext(file_path)[1].lower()
    
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path, dtype=str)
    else:
        df = pd.read_csv(file_path, dtype=str, sep=None, engine='python')
    
    # Szukaj kolumny z ID
    id_col = None
    for col in df.columns:
        col_lower = col.lower()
        if "id" in col_lower or "identyfikator" in col_lower or "placówki" in col_lower:
            id_col = col
            break
    
    if not id_col:
        id_col = df.columns[0]
    
    print(f"✓ Używam kolumny: {id_col}")
    
    ids = []
    for val in df[id_col]:
        if pd.notna(val):
            s = str(val).strip()
            if s and s.lower() not in ["nan", "none"]:
                ids.append(s)
    
    return ids


def main():
    print("\n" + "="*60)
    print("TAGOWANIE FIRM (ACCOUNTS)")
    print("="*60)
    
    # 1. Pobierz token
    print("\nPobieranie tokena...")
    try:
        access_token = get_access_token()
        print("✓ Token uzyskany")
    except Exception as e:
        print(f"✗ Błąd tokena: {e}")
        sys.exit(1)
    
    # 2. Pobierz dostępne tagi
    print("\nPobieranie dostępnych tagów...")
    tags = get_available_tags(access_token)
    
    if not tags:
        print("⚠ Nie znaleziono tagów w Zoho. Możesz wpisać nowy tag ręcznie.")
        print("\nPodaj nazwę nowego tagu:")
        tag_name = input("Tag: ").strip()
    else:
        print(f"\n{'='*60}")
        print("DOSTĘPNE TAGI:")
        print("="*60)
        for idx, tag in enumerate(tags, 1):
            print(f"{idx:3d}. {tag['name']}")
        print(f"{'='*60}")
        print("  0. Wpisz własny tag (nowy)")
        print("="*60)
        
        choice = input("\nWybierz numer tagu (lub 0 dla własnego): ").strip()
        
        if choice == "0" or not choice:
            print("\nPodaj nazwę nowego tagu:")
            tag_name = input("Tag: ").strip()
        elif choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(tags):
                tag_name = tags[idx]["name"]
            else:
                print("✗ Nieprawidłowy numer")
                sys.exit(1)
        else:
            # Może wpisał nazwę tagu bezpośrednio
            tag_name = choice
    
    if not tag_name:
        print("✗ Nie podano nazwy tagu")
        sys.exit(1)
    
    print(f"✓ Wybrany tag: {tag_name}")
    
    # 3. Podaj ścieżkę do pliku
    print("\nPrzeciągnij plik CSV/XLSX lub wpisz ścieżkę:")
    file_path = input("Ścieżka: ").strip().strip('"').strip("'")
    
    if not file_path:
        print("✗ Nie podano ścieżki")
        sys.exit(1)
    
    # 3. Wczytaj ID
    try:
        ids = load_ids_from_file(file_path)
    except Exception as e:
        print(f"✗ Błąd wczytywania pliku: {e}")
        sys.exit(1)
    
    print(f"✓ Wczytano {len(ids)} ID firm")
    
    # 4. Potwierdzenie
    print(f"\n{'='*60}")
    print(f"Tag: {tag_name}")
    print(f"Liczba firm do otagowania: {len(ids)}")
    print(f"{'='*60}")
    
    confirm = input("\nCzy kontynuować? (t/n): ").strip().lower()
    if confirm not in ["t", "tak", "y", "yes"]:
        print("Anulowano")
        sys.exit(0)
    
    # 5. Tagowanie
    print(f"\nTagowanie {len(ids)} firm...")
    
    if tqdm:
        # Z paskiem postępu
        successes = []
        failures = []
        for i in tqdm(range(0, len(ids), 100), desc="Tagowanie"):
            batch = ids[i:i+100]
            s, f = add_tags_to_accounts(access_token, batch, tag_name)
            successes.extend(s)
            failures.extend(f)
    else:
        successes, failures = add_tags_to_accounts(access_token, ids, tag_name)
    
    # 7. Podsumowanie
    print(f"\n{'='*60}")
    print("PODSUMOWANIE")
    print(f"{'='*60}")
    print(f"Sukces: {len(successes)}")
    print(f"Błędy: {len(failures)}")
    
    if failures:
        print("\nBłędy:")
        for rid, err in failures[:10]:
            print(f"  - {rid}: {err}")
        if len(failures) > 10:
            print(f"  ... i {len(failures) - 10} więcej")
    
    # 8. Zapis raportu
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs("wyniki_tagowania", exist_ok=True)
    
    report_path = f"wyniki_tagowania/tag_results_{timestamp}.csv"
    with open(report_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["ID", "Status", "Szczegóły"])
        for rid in successes:
            writer.writerow([rid, "SUKCES", ""])
        for rid, err in failures:
            writer.writerow([rid, "BŁĄD", err])
    
    print(f"\n✓ Raport zapisany: {report_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nPrzerwano")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
