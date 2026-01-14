"""
Skrypt do masowej aktualizacji statusu leadów w module Leads z pliku CSV/XLSX.
Pozwala na zmianę pola Lead_Status dla wielu leadów jednocześnie.
"""

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import pandas as pd
except ImportError:
    print("Błąd: Wymagana biblioteka pandas nie jest zainstalowana.", file=sys.stderr)
    print("Proszę ją zainstalować komendą: pip install pandas", file=sys.stderr)
    sys.exit(1)

try:
    import openpyxl
except ImportError:
    print("Błąd: Wymagana biblioteka openpyxl nie jest zainstalowana.", file=sys.stderr)
    print("Proszę ją zainstalować komendą: pip install openpyxl", file=sys.stderr)
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("Błąd: Wymagana biblioteka tqdm nie jest zainstalowana.", file=sys.stderr)
    print("Proszę ją zainstalować komendą: pip install tqdm", file=sys.stderr)
    sys.exit(1)

from refresh_zoho_access_token import refresh_access_token

# --- Konfiguracja API Zoho ---
ZOHO_CRM_API_BASE_URL = "https://www.zohoapis.eu/crm/v8"
REQUEST_SLEEP_S = 0.2
MAX_RETRY_TIMEOUT = 2

# Aktualny token
ACCESS_TOKEN_HOLDER: Dict[str, Any] = {"token": None, "refreshed_at": 0.0}

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("update_lead_status.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)


def get_access_token() -> str:
    """Pobiera token dostępowy z env lub refresh token."""
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID", "WPISZ_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET", "WPISZ_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN", "WPISZ_REFRESH_TOKEN")

    if any(val.startswith("WPISZ_") for val in [client_id, client_secret, refresh_token]):
        raise RuntimeError(
            "Błąd: Dane (client_id, client_secret, refresh_token) nie zostały skonfigurowane.\n"
            "Ustaw zmienne środowiskowe (np. ZOHO_MEDIDESK_CLIENT_ID) lub wpisz je bezpośrednio w pliku."
        )

    token_info = refresh_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    ACCESS_TOKEN_HOLDER["token"] = token_info["access_token"]
    ACCESS_TOKEN_HOLDER["refreshed_at"] = time.time()
    return token_info["access_token"]


def _maybe_refresh_token_cooldown() -> Optional[str]:
    """Odświeża token tylko jeśli minęło >= 15s od ostatniego odświeżenia."""
    try:
        last = float(ACCESS_TOKEN_HOLDER.get("refreshed_at") or 0.0)
        if time.time() - last < 15.0:
            return ACCESS_TOKEN_HOLDER.get("token")
        new_tok = get_access_token()
        return new_tok
    except Exception as e:
        logging.error(f"Cooldown refresh failed: {e}")
        return None


def execute_api_request(
    access_token: str, 
    url: str, 
    method: str = "GET", 
    data: Optional[Dict] = None
) -> Optional[Dict[str, Any]]:
    """Wykonuje żądanie API z automatycznym odświeżaniem tokena."""
    def _do_request(tok: str) -> Optional[Dict[str, Any]]:
        headers = {"Authorization": f"Zoho-oauthtoken {tok}"}
        
        if method == "GET":
            request = urllib.request.Request(url, headers=headers, method="GET")
            time.sleep(REQUEST_SLEEP_S)
            logging.debug(f"HTTP GET: {url}")
        elif method in ["PUT", "POST"]:
            headers["Content-Type"] = "application/json"
            json_data = json.dumps(data).encode("utf-8") if data else b""
            request = urllib.request.Request(url, data=json_data, headers=headers, method=method)
            time.sleep(REQUEST_SLEEP_S)
            logging.debug(f"HTTP {method}: {url}")
        else:
            raise ValueError(f"Nieobsługiwana metoda HTTP: {method}")
        
        with urllib.request.urlopen(request, timeout=30) as response:
            response_data = response.read().decode("utf-8")
            if not response_data:
                return None
            return json.loads(response_data)

    try:
        tok0 = ACCESS_TOKEN_HOLDER.get("token") or access_token
        return _do_request(tok0)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        logging.error(f"HTTPError {method} {e.code} for {url}: {body}")
        if e.code in (401, 403):
            try:
                new_tok = _maybe_refresh_token_cooldown() or access_token
                return _do_request(new_tok)
            except Exception as e2:
                logging.error(f"Ponowienie po odświeżeniu tokena nie powiodło się: {e2}")
                return None
        return None
    except Exception as e:
        err_text = str(e)
        if "timed out" in err_text or "10060" in err_text:
            for i in range(MAX_RETRY_TIMEOUT):
                time.sleep(2 + i * 3)
                try:
                    tok = ACCESS_TOKEN_HOLDER.get("token") or access_token
                    return _do_request(tok)
                except Exception:
                    continue
        logging.error(f"Błąd podczas wykonywania zapytania do API ({method} {url}): {e}")
        return None


def load_file(file_path: str) -> pd.DataFrame:
    """Wczytuje plik CSV lub XLSX do DataFrame."""
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Plik nie istnieje: {file_path}")
    
    ext = path.suffix.lower()
    
    print(f"Wczytywanie pliku...")
    
    if ext == ".csv":
        for encoding in ["utf-8-sig", "utf-8", "cp1252", "iso-8859-1"]:
            try:
                df = pd.read_csv(file_path, encoding=encoding, dtype=str)
                print(f"✓ Wczytano plik CSV (kodowanie: {encoding})")
                return df
            except UnicodeDecodeError:
                continue
        raise ValueError("Nie udało się wczytać pliku CSV - problem z kodowaniem")
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(file_path, dtype=str)
        print(f"✓ Wczytano plik XLSX")
        return df
    else:
        raise ValueError(f"Nieobsługiwany format pliku: {ext}. Użyj CSV lub XLSX")


def show_columns(df: pd.DataFrame) -> None:
    """Wyświetla listę kolumn z numerami."""
    print("\n" + "="*60)
    print("DOSTĘPNE KOLUMNY:")
    print("="*60)
    for idx, col in enumerate(df.columns, 1):
        non_empty = df[col].notna().sum()
        total = len(df)
        print(f"{idx:3d}. {col:40s} (zapełnienie: {non_empty}/{total})")
    print("="*60)


def get_column_by_number_or_name(df: pd.DataFrame, user_input: str) -> Optional[str]:
    """Zwraca nazwę kolumny na podstawie numeru lub nazwy."""
    user_input = user_input.strip()
    
    if user_input.isdigit():
        col_num = int(user_input)
        if 1 <= col_num <= len(df.columns):
            return df.columns[col_num - 1]
        else:
            print(f"✗ Nieprawidłowy numer kolumny: {col_num}")
            return None
    
    if user_input in df.columns:
        return user_input
    
    matches = [col for col in df.columns if user_input.lower() in col.lower()]
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        print(f"✗ Niejednoznaczna nazwa - znaleziono {len(matches)} dopasowań:")
        for m in matches:
            print(f"  - {m}")
        return None
    
    print(f"✗ Nie znaleziono kolumny: {user_input}")
    return None


def fetch_lead_info(access_token: str, lead_id: str) -> Optional[Dict[str, Any]]:
    """Pobiera podstawowe informacje o leadzie po ID."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Leads/{lead_id}?fields=Last_Name,Company,Lead_Status"
    payload = execute_api_request(access_token, url, method="GET")
    
    if payload and "data" in payload and len(payload["data"]) > 0:
        return payload["data"][0]
    return None


def update_lead_status(access_token: str, lead_id: str, new_status: str) -> Optional[Dict[str, Any]]:
    """Aktualizuje status leada."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Leads"
    payload = {
        "data": [{
            "id": lead_id,
            "Lead_Status": new_status
        }]
    }
    
    response = execute_api_request(access_token, url, method="PUT", data=payload)
    return response


def save_results(df: pd.DataFrame, output_dir: str, base_name: str) -> None:
    """Zapisuje wyniki do CSV i XLSX."""
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # CSV
    csv_path = os.path.join(output_dir, f"{base_name}_{timestamp}.csv")
    print(f"Zapisywanie CSV...")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"✓ Zapisano CSV: {csv_path}")
    
    # XLSX
    xlsx_path = os.path.join(output_dir, f"{base_name}_{timestamp}.xlsx")
    print(f"Zapisywanie XLSX...")
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    print(f"✓ Zapisano XLSX: {xlsx_path}")


def main() -> None:
    """Główna funkcja skryptu."""
    print("\n" + "="*60)
    print("MASOWA AKTUALIZACJA STATUSU LEADÓW")
    print("="*60)
    
    # 1. Pobierz ścieżkę do pliku
    if len(sys.argv) > 1:
        file_path = sys.argv[1].strip().strip('"').strip("'")
        print(f"\nPlik wejściowy: {file_path}")
    else:
        print("\nPrzeciągnij plik CSV/XLSX do terminala lub wpisz ścieżkę:")
        file_path = input("Ścieżka: ").strip().strip('"').strip("'")
    
    if not file_path:
        print("✗ Nie podano ścieżki do pliku")
        sys.exit(1)
    
    # 2. Wczytaj plik
    try:
        df = load_file(file_path)
    except Exception as e:
        print(f"✗ Błąd wczytywania pliku: {e}")
        sys.exit(1)
    
    print(f"✓ Wczytano {len(df)} wierszy, {len(df.columns)} kolumn")
    
    # 3. Wybierz kolumnę z ID leadów
    show_columns(df)
    print("\nWybierz kolumnę z ID leadów:")
    lead_col_input = input("Numer lub nazwa kolumny: ").strip()
    lead_col = get_column_by_number_or_name(df, lead_col_input)
    
    if not lead_col:
        print("✗ Nie wybrano kolumny z ID leadów")
        sys.exit(1)
    
    print(f"✓ Wybrano kolumnę: {lead_col}")
    
    # 4. Wybierz nowy Lead Status
    print("\n" + "="*60)
    print("WYBIERZ NOWY LEAD STATUS:")
    print("="*60)
    lead_statuses = [
        "Lead", 
        "Dzwonienie", 
        "Nurturing", 
        "Umówione spotkanie", 
        "Zakwalifikowane do sales",
        "Zdyskwalifikowany",
        "Call I", 
        "Dodzwoniono się",
        "Kontakt w przyszłości", 
        "Leady przegrane",
        "Podjęto próbę kontaktu", 
        "Skontaktowano się"
    ]
    for idx, status in enumerate(lead_statuses, 1):
        print(f"{idx:2d}. {status}")
    print("="*60)
    
    status_input = input("\nWybierz numer nowego statusu: ").strip()
    if status_input and status_input.isdigit():
        status_idx = int(status_input) - 1
        if 0 <= status_idx < len(lead_statuses):
            new_status = lead_statuses[status_idx]
        else:
            print("✗ Nieprawidłowy numer statusu")
            sys.exit(1)
    else:
        print("✗ Nie wybrano statusu")
        sys.exit(1)
    
    print(f"✓ Wybrany nowy status: {new_status}")
    
    # 5. Pobranie tokena
    print("\nPobieranie tokena dostępowego...")
    try:
        access_token = get_access_token()
        print("✓ Token uzyskany")
    except Exception as e:
        print(f"✗ Błąd pobierania tokena: {e}")
        sys.exit(1)
    
    # 6. Aktualizacja statusów
    print(f"\n{'='*60}")
    print(f"AKTUALIZACJA STATUSÓW LEADÓW")
    print(f"{'='*60}")
    print(f"Liczba leadów do przetworzenia: {len(df)}")
    print(f"Nowy status: {new_status}")
    
    confirm = input("\nCzy na pewno zaktualizować statusy leadów? (t/n): ").strip().lower()
    if confirm not in ["t", "tak", "y", "yes"]:
        print("Anulowano")
        sys.exit(0)
    
    results = []
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Aktualizacja statusów"):
        lead_id = str(row[lead_col]).strip()
        
        if not lead_id or lead_id.lower() in ["nan", "none", ""]:
            logging.warning(f"Wiersz {idx+1}: Brak ID leada - pomijam")
            results.append({
                "wiersz": idx+1,
                "lead_id": lead_id,
                "status": "BŁĄD",
                "szczegóły": "Brak ID leada"
            })
            continue
        
        # Pobierz info o leadzie (opcjonalne - do weryfikacji)
        lead_info = fetch_lead_info(access_token, lead_id)
        if not lead_info:
            logging.warning(f"Wiersz {idx+1}: Nie znaleziono leada o ID {lead_id}")
            results.append({
                "wiersz": idx+1,
                "lead_id": lead_id,
                "status": "BŁĄD",
                "szczegóły": f"Nie znaleziono leada o ID {lead_id}"
            })
            continue
        
        lead_name = lead_info.get("Last_Name", "")
        old_status = lead_info.get("Lead_Status", "")
        
        # Aktualizuj status
        response = update_lead_status(access_token, lead_id, new_status)
        
        if response and "data" in response and len(response["data"]) > 0:
            result_data = response["data"][0]
            if result_data.get("code") == "SUCCESS":
                logging.info(f"✓ Zaktualizowano lead: {lead_name} (ID: {lead_id})")
                results.append({
                    "wiersz": idx+1,
                    "lead_id": lead_id,
                    "lead_name": lead_name,
                    "stary_status": old_status,
                    "nowy_status": new_status,
                    "status": "SUKCES",
                    "szczegóły": ""
                })
            else:
                error_msg = result_data.get("message", "Nieznany błąd")
                logging.error(f"✗ Błąd aktualizacji leada {lead_id}: {error_msg}")
                results.append({
                    "wiersz": idx+1,
                    "lead_id": lead_id,
                    "lead_name": lead_name,
                    "stary_status": old_status,
                    "status": "BŁĄD",
                    "szczegóły": error_msg
                })
        else:
            logging.error(f"✗ Nieprawidłowa odpowiedź z API dla leada {lead_id}")
            results.append({
                "wiersz": idx+1,
                "lead_id": lead_id,
                "status": "BŁĄD",
                "szczegóły": "Nieprawidłowa odpowiedź z API"
            })
    
    # 7. Zapisz wyniki
    results_df = pd.DataFrame(results)
    output_dir = "wyniki_update_lead_status"
    base_name = "update_status_results"
    
    save_results(results_df, output_dir, base_name)
    
    # Statystyki
    success_count = len([r for r in results if r["status"] == "SUKCES"])
    error_count = len([r for r in results if r["status"] == "BŁĄD"])
    
    print(f"\n{'='*60}")
    print(f"PODSUMOWANIE")
    print(f"{'='*60}")
    print(f"Łącznie leadów: {len(results)}")
    print(f"Zaktualizowano: {success_count}")
    print(f"Błędy: {error_count}")
    print(f"{'='*60}")
    
    logging.info("Zakończono aktualizację statusów leadów")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Przerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
