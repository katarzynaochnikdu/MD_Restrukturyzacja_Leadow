"""
Skrypt do masowego tworzenia leadów w module Leads z pliku CSV/XLSX.
Pozwala na filtrowanie danych i tworzenie leadów testowych.
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
from zoho_users import get_active_users, get_access_token as get_users_access_token

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
        logging.FileHandler("create_leads_from_file.log", encoding="utf-8"),
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


def execute_api_request(access_token: str, url: str, method: str = "GET", data: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
    """Wykonuje żądanie API z automatycznym odświeżaniem tokena."""
    def _do_request(tok: str) -> Optional[Dict[str, Any]]:
        headers = {"Authorization": f"Zoho-oauthtoken {tok}"}
        
        if method == "GET":
            request = urllib.request.Request(url, headers=headers, method="GET")
            time.sleep(REQUEST_SLEEP_S)
            logging.debug(f"HTTP GET: {url}")
        elif method == "POST":
            headers["Content-Type"] = "application/json"
            json_data = json.dumps(data).encode("utf-8") if data else b""
            request = urllib.request.Request(url, data=json_data, headers=headers, method="POST")
            time.sleep(REQUEST_SLEEP_S)
            logging.debug(f"HTTP POST: {url}")
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


def select_owner(access_token: str) -> Optional[str]:
    """Zwraca wybrany Owner ID z listy aktywnych użytkowników."""
    users = get_active_users(access_token)
    if not users:
        print("Brak aktywnych użytkowników do wyboru.")
        return None

    print("\nAktualni użytkownicy Zoho CRM:")
    for idx, user in enumerate(users, 1):
        name = user.get("full_name") or user.get("name")
        role = user.get("role_name", "brak roli")
        profile = user.get("profile_name", "")
        print(f"{idx:2d}. {name} ({user.get('id')}) – {role} / {profile}")

    choice = input("Wybierz numer użytkownika jako Ownera (lub Enter, by pominąć): ").strip()
    if choice and choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(users):
            return users[idx].get("id")
    return None


def select_multiple_owners(access_token: str) -> List[Dict[str, str]]:
    """Zwraca listę wybranych Ownerów (ID + nazwa) do rozdziału round-robin."""
    users = get_active_users(access_token)
    if not users:
        print("Brak aktywnych użytkowników do wyboru.")
        return []

    print("\nAktualni użytkownicy Zoho CRM:")
    for idx, user in enumerate(users, 1):
        name = user.get("full_name") or user.get("name")
        role = user.get("role_name", "brak roli")
        profile = user.get("profile_name", "")
        print(f"{idx:2d}. {name} ({user.get('id')}) – {role} / {profile}")

    print("\n" + "="*60)
    print("WYBÓR WIELU OWNERÓW (round-robin)")
    print("="*60)
    print("Podaj numery użytkowników oddzielone przecinkami lub spacjami.")
    print("Przykład: 1,3 lub 1 3 lub 1, 3")
    print("Leady będą rozdzielane równomiernie między wybranych.")
    
    choice = input("\nWybierz numery użytkowników (lub Enter, by pominąć): ").strip()
    
    if not choice:
        return []
    
    # Parsowanie numerów (przecinki, spacje, lub mix)
    import re
    numbers = re.findall(r'\d+', choice)
    
    selected_owners = []
    for num_str in numbers:
        idx = int(num_str) - 1
        if 0 <= idx < len(users):
            user = users[idx]
            owner_info = {
                "id": user.get("id"),
                "name": user.get("full_name") or user.get("name")
            }
            if owner_info not in selected_owners:  # Unikaj duplikatów
                selected_owners.append(owner_info)
    
    return selected_owners


def load_file(file_path: str) -> pd.DataFrame:
    """Wczytuje plik CSV lub XLSX do DataFrame."""
    # Obsługa ścieżek URL (file:///) z przeciągania plików
    if file_path.startswith("file:///"):
        import urllib.parse
        file_path = urllib.parse.unquote(file_path[8:])  # Usuń "file:///" i dekoduj %20 itp.
        # Na Windows zamień / na \
        file_path = file_path.replace("/", "\\")
    
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Plik nie istnieje: {file_path}")
    
    ext = path.suffix.lower()
    
    print(f"Wczytywanie pliku...")
    
    if ext == ".csv":
        for encoding in ["utf-8-sig", "utf-8", "cp1252", "iso-8859-1"]:
            try:
                # Automatyczne wykrywanie separatora (przecinek lub średnik)
                df = pd.read_csv(file_path, encoding=encoding, dtype=str, sep=None, engine='python')
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


def fetch_account_name(access_token: str, account_id: str) -> Optional[str]:
    """Pobiera nazwę firmy (Account) po ID."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Accounts/{account_id}?fields=Account_Name"
    payload = execute_api_request(access_token, url, method="GET")
    
    if payload and "data" in payload and len(payload["data"]) > 0:
        return payload["data"][0].get("Account_Name")
    return None


def verify_contact_exists(access_token: str, contact_id: str) -> bool:
    """Sprawdza czy kontakt (Contact) istnieje w CRM."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Contacts/{contact_id}?fields=id"
    payload = execute_api_request(access_token, url, method="GET")
    
    if payload and "data" in payload and len(payload["data"]) > 0:
        return True
    return False


def create_lead(access_token: str, lead_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Tworzy pojedynczy lead w module Leads."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Leads"
    payload = {"data": [lead_data]}
    
    response = execute_api_request(access_token, url, method="POST", data=payload)
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
    print("MASOWE TWORZENIE LEADÓW Z PLIKU")
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
    
    # 3. Wybierz kolumnę z ID firm (Account ID)
    show_columns(df)
    print("\nWybierz kolumnę z ID firm (Account ID):")
    account_col_input = input("Numer lub nazwa kolumny: ").strip()
    account_col = get_column_by_number_or_name(df, account_col_input)
    
    if not account_col:
        print("✗ Nie wybrano kolumny z ID firm")
        sys.exit(1)
    
    print(f"✓ Wybrano kolumnę: {account_col}")
    
    # 4. Opcjonalnie wybierz kolumnę z ID kontaktów (Contact ID)
    use_contacts = input("\nCzy plik zawiera kolumnę z ID kontaktów? (t/n): ").strip().lower()
    contact_col = None
    
    if use_contacts in ["t", "tak", "y", "yes"]:
        show_columns(df)
        print("\nWybierz kolumnę z ID kontaktów (Contact ID):")
        contact_col_input = input("Numer lub nazwa kolumny: ").strip()
        contact_col = get_column_by_number_or_name(df, contact_col_input)
        
        if contact_col:
            print(f"✓ Wybrano kolumnę: {contact_col}")
        else:
            print("⚠ Nie wybrano kolumny z ID kontaktów - tworzenie bez kontaktów")
    
    # 5. Wybierz Lead Status
    print("\n" + "="*60)
    print("WYBIERZ LEAD STATUS:")
    print("="*60)
    lead_statuses = [
        "Lead", "Dzwonienie", "Nurturing", "Umówione spotkanie", 
        "Zakwalifikowane do sales", "Call I", "Dodzwoniono się",
        "Kontakt w przyszłości", "Podjęto próbę kontaktu", "Skontaktowano się"
    ]
    for idx, status in enumerate(lead_statuses, 1):
        print(f"{idx:2d}. {status}")
    print("="*60)
    
    status_input = input("\nWybierz numer statusu (domyślnie: 1 - Lead): ").strip()
    if status_input and status_input.isdigit():
        status_idx = int(status_input) - 1
        if 0 <= status_idx < len(lead_statuses):
            lead_status = lead_statuses[status_idx]
        else:
            lead_status = "Lead"
    else:
        lead_status = "Lead"
    
    print(f"✓ Wybrany status: {lead_status}")
    
    # 6. Prefiks nazwy
    print("\nCzy dodać prefiks przed nazwą firmy?")
    print("  Przykłady: [TEST], [CC], [AKWIZYCJA], lub własny")
    print("  Pozostaw puste = brak prefiksu")
    
    name_prefix = input("\nWpisz prefiks (lub Enter = brak): ").strip()
    if name_prefix:
        print(f"✓ Prefiks: {name_prefix}")
    
    # 7. Filtrowanie (opcjonalne) - pomijamy, bo niepotrzebne
    filter_choice = "n"
    
    filtered_df = df.copy()
    
    if filter_choice in ["t", "tak", "y", "yes"]:
        print("\n⚠ Funkcja filtrowania - użyj skryptu filter_csv.py przed uruchomieniem tego skryptu")
        print("Kontynuuję z wszystkimi wierszami...")
    
    # 8. Pobranie tokena
    print("\nPobieranie tokena dostępowego...")
    try:
        access_token = get_access_token()
        print("✓ Token uzyskany")
    except Exception as e:
        print(f"✗ Błąd pobierania tokena: {e}")
        sys.exit(1)

    # Wybór trybu przypisania Ownera
    print("\n" + "="*60)
    print("PRZYPISANIE OWNERA")
    print("="*60)
    print("1. Jeden Owner dla wszystkich leadów")
    print("2. Wielu Ownerów (round-robin - równomierny podział)")
    print("3. Bez Ownera")
    
    owner_mode = input("\nWybierz tryb (1/2/3, domyślnie 1): ").strip()
    
    owner_id = None
    owners_list = []
    
    if owner_mode == "2":
        owners_list = select_multiple_owners(access_token)
        if owners_list:
            print(f"\n✓ Wybrano {len(owners_list)} Ownerów do rozdziału round-robin:")
            for i, o in enumerate(owners_list, 1):
                print(f"   {i}. {o['name']} ({o['id']})")
        else:
            print("⚠ Nie wybrano Ownerów - leady utworzone bez Ownera")
    elif owner_mode == "3":
        print("ℹ Leady będą utworzone bez Ownera")
    else:
        owner_id = select_owner(access_token)
        if owner_id:
            print(f"✓ Owner ustawiony na ID: {owner_id}")
        else:
            print("ℹ Nie przypisano Ownera - leady utworzone bez Ownera")
    
    # 9. Tworzenie leadów
    print(f"\n{'='*60}")
    print(f"TWORZENIE LEADÓW")
    print(f"{'='*60}")
    print(f"Liczba wierszy do przetworzenia: {len(filtered_df)}")
    
    confirm = input("\nCzy na pewno utworzyć leady? (t/n): ").strip().lower()
    if confirm not in ["t", "tak", "y", "yes"]:
        print("Anulowano")
        sys.exit(0)
    
    results = []
    owner_index = 0  # Indeks dla round-robin
    
    for idx, row in tqdm(filtered_df.iterrows(), total=len(filtered_df), desc="Tworzenie leadów"):
        account_id = str(row[account_col]).strip()
        
        if not account_id or account_id.lower() in ["nan", "none", ""]:
            logging.warning(f"Wiersz {idx+1}: Brak ID firmy - pomijam")
            results.append({
                "wiersz": idx+1,
                "account_id": account_id,
                "status": "BŁĄD",
                "szczegóły": "Brak ID firmy"
            })
            continue
        
        # Pobierz nazwę firmy
        account_name = fetch_account_name(access_token, account_id)
        if not account_name:
            logging.warning(f"Wiersz {idx+1}: Nie znaleziono firmy o ID {account_id}")
            results.append({
                "wiersz": idx+1,
                "account_id": account_id,
                "status": "BŁĄD",
                "szczegóły": f"Nie znaleziono firmy o ID {account_id}"
            })
            continue
        
        # Opcjonalnie pobierz ID kontaktu i zweryfikuj jego istnienie
        contact_id = None
        if contact_col:
            contact_id = str(row[contact_col]).strip()
            if contact_id and contact_id.lower() not in ["nan", "none", ""]:
                # Sprawdź czy kontakt istnieje
                if not verify_contact_exists(access_token, contact_id):
                    logging.warning(f"Wiersz {idx+1}: Nie znaleziono kontaktu o ID {contact_id} - tworzenie bez kontaktu")
                    contact_id = None  # Resetuj jeśli nie istnieje
        
        # Zbuduj nazwę leada ZAWSZE z nazwy firmy (max 120 znaków)
        if name_prefix:
            lead_name = f"{name_prefix} {account_name}"
        else:
            lead_name = account_name
        
        # Ogranicz do 80 znaków (limit Zoho dla Last_Name)
        if len(lead_name) > 80:
            lead_name = lead_name[:77] + "..."
        
        # Zbuduj payload
        lead_payload = {
            "Last_Name": lead_name,
            "Company": account_name,
            "Firma_w_bazie": account_id,
            "Lead_Status": lead_status,
            "Czy_firma_jest_w_bazie": True,
            # Źródło: Polecenie od Partnera
            "Outbound_Inbound": "Inbound",
            "Zrodlo_inbound": "Polecenie",
            "Polecenie": "Partner polecający (Partner)",
            "Polecenie_partner": "751364000038601663",  # FUNDUSZE WSPARCIA
        }
        
        if contact_id:
            lead_payload["Kontakt_w_bazie"] = contact_id
        
        # Przypisanie Ownera (pojedynczy lub round-robin)
        current_owner_id = None
        current_owner_name = None
        if owners_list:
            # Round-robin: wybierz kolejnego ownera z listy
            current_owner = owners_list[owner_index % len(owners_list)]
            current_owner_id = current_owner["id"]
            current_owner_name = current_owner["name"]
            owner_index += 1
        elif owner_id:
            current_owner_id = owner_id
        
        if current_owner_id:
            lead_payload["Owner"] = current_owner_id
        
        # Utwórz lead
        response = create_lead(access_token, lead_payload)
        
        if response and "data" in response and len(response["data"]) > 0:
            result_data = response["data"][0]
            if result_data.get("code") == "SUCCESS":
                lead_id = result_data["details"]["id"]
                owner_info = current_owner_name or (current_owner_id if current_owner_id else "brak")
                logging.info(f"✓ Utworzono lead: {lead_name} (ID: {lead_id}, Owner: {owner_info})")
                results.append({
                    "wiersz": idx+1,
                    "account_id": account_id,
                    "account_name": account_name,
                    "contact_id": contact_id or "",
                    "lead_id": lead_id,
                    "lead_name": lead_name,
                    "owner_id": current_owner_id or "",
                    "owner_name": current_owner_name or "",
                    "status": "SUKCES",
                    "szczegóły": ""
                })
            else:
                error_msg = result_data.get("message", "Nieznany błąd")
                logging.error(f"✗ Błąd tworzenia leada: {error_msg}")
                results.append({
                    "wiersz": idx+1,
                    "account_id": account_id,
                    "account_name": account_name,
                    "status": "BŁĄD",
                    "szczegóły": error_msg
                })
        else:
            logging.error(f"✗ Nieprawidłowa odpowiedź z API dla wiersza {idx+1}")
            results.append({
                "wiersz": idx+1,
                "account_id": account_id,
                "status": "BŁĄD",
                "szczegóły": "Nieprawidłowa odpowiedź z API"
            })
    
    # 10. Zapisz wyniki
    results_df = pd.DataFrame(results)
    output_dir = "wyniki_create_leads"
    base_name = "create_leads_results"
    
    save_results(results_df, output_dir, base_name)
    
    # Statystyki
    success_count = len([r for r in results if r["status"] == "SUKCES"])
    error_count = len([r for r in results if r["status"] == "BŁĄD"])
    
    print(f"\n{'='*60}")
    print(f"PODSUMOWANIE")
    print(f"{'='*60}")
    print(f"Łącznie wierszy: {len(results)}")
    print(f"Sukces: {success_count}")
    print(f"Błąd: {error_count}")
    print(f"{'='*60}")
    
    logging.info("Zakończono tworzenie leadów")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Przerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
