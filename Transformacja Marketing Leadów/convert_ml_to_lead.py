"""
Skrypt do konwersji Marketing Lead → Lead (zamknięcie ML + utworzenie Lead).
Automatyczny flow dla jednego lub więcej rekordów.
"""

import csv
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

from refresh_zoho_access_token import refresh_access_token
from zoho_users import get_active_users

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# --- Konfiguracja API Zoho ---
ZOHO_CRM_API_BASE_URL = "https://www.zohoapis.eu/crm/v8"
REQUEST_SLEEP_S = 0.2

# Aktualny token
ACCESS_TOKEN_HOLDER: Dict[str, Any] = {"token": None, "refreshed_at": 0.0}

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("convert_ml_to_lead.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# Statusy do zamknięcia Marketing Lead
ML_CLOSE_STATUSES = [
    "odpad (odpadek)",
    "zdyskwalifikowany",
    "rezygnacja",
    "brak kontaktu",
    "duplikat",
]

# Statusy dla nowego Lead
LEAD_STATUSES = [
    "Lead",
    "Dzwonienie",
    "Rozmowa",
    "Spotkanie umówione",
    "Spotkanie odbyte",
    "Analiza potrzeb",
    "Oferta",
    "Negocjacje",
    "Zdyskwalifikowany",
]


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


def execute_api_request(access_token: str, url: str, method: str = "GET", data: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
    """Wykonuje żądanie API."""
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    
    if method in ["POST", "PUT"]:
        headers["Content-Type"] = "application/json"
        json_data = json.dumps(data).encode("utf-8") if data else b""
        request = urllib.request.Request(url, data=json_data, headers=headers, method=method)
    else:
        request = urllib.request.Request(url, headers=headers, method="GET")
    
    time.sleep(REQUEST_SLEEP_S)
    
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_data = response.read().decode("utf-8")
            if not response_data:
                return None
            return json.loads(response_data)
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        logging.error(f"HTTPError {method} {e.code} for {url}: {error_body}")
        return None
    except Exception as e:
        logging.error(f"Request error: {e}")
        return None


def extract_numeric_id(value: str) -> Optional[str]:
    """Wyciąga numeryczne ID z tekstu typu 'Nazwa (ID: 12345)'."""
    if not value:
        return None
    # Szukaj długiego numeru (Zoho ID to zwykle 18-19 cyfr)
    match = re.search(r'(\d{10,})', str(value))
    if match:
        return match.group(1)
    # Jeśli samo jest liczbą
    clean = str(value).strip()
    if clean.isdigit():
        return clean
    return None


def fetch_account_name(access_token: str, account_id: str) -> Optional[str]:
    """Pobiera nazwę firmy z Zoho."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Accounts/{account_id}"
    payload = execute_api_request(access_token, url, method="GET")
    if payload and "data" in payload and len(payload["data"]) > 0:
        return payload["data"][0].get("Account_Name")
    return None


def get_blueprint_transitions(access_token: str, ml_id: str) -> Optional[List[Dict[str, Any]]]:
    """Pobiera dostępne przejścia Blueprint dla rekordu."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Marketing_Leads/{ml_id}/actions/blueprint"
    response = execute_api_request(access_token, url, method="GET")
    
    if response and "blueprint" in response:
        blueprint = response["blueprint"]
        transitions = blueprint.get("transitions", [])
        current_value = blueprint.get("current_picklist_value", {}).get("name", "nieznany")
        logging.info(f"Blueprint: aktualny status = '{current_value}', dostępne przejścia: {len(transitions)}")
        return transitions
    return None


def find_transition_to_status(transitions: List[Dict[str, Any]], target_status: str) -> Optional[Dict[str, Any]]:
    """Znajduje transition prowadzący do danego statusu."""
    target_lower = target_status.lower()
    
    for trans in transitions:
        # Sprawdź next_field_value
        next_value = trans.get("next_field_value", "")
        if next_value and target_lower in next_value.lower():
            return trans
        
        # Sprawdź name transition
        trans_name = trans.get("name", "")
        if trans_name and target_lower in trans_name.lower():
            return trans
        
        # Sprawdź criteria_message
        criteria = trans.get("criteria_message", "")
        if criteria and target_lower in criteria.lower():
            return trans
    
    return None


def execute_blueprint_transition(access_token: str, ml_id: str, transition_id: str) -> Tuple[bool, str]:
    """Wykonuje przejście Blueprint."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Marketing_Leads/{ml_id}/actions/blueprint"
    payload = {
        "blueprint": [
            {
                "transition_id": transition_id,
                "data": {}
            }
        ]
    }
    response = execute_api_request(access_token, url, method="PUT", data=payload)
    
    if response and "data" in response and len(response["data"]) > 0:
        result = response["data"][0]
        if result.get("code") == "SUCCESS":
            return True, "Przejście Blueprint wykonane"
        else:
            return False, f"Błąd Blueprint: {result.get('message', 'nieznany błąd')}"
    
    # Czasem Blueprint zwraca inną strukturę
    if response and "code" in response:
        if response["code"] == "SUCCESS":
            return True, "Przejście Blueprint wykonane"
        else:
            return False, f"Błąd Blueprint: {response.get('message', 'nieznany błąd')}"
    
    return False, "Brak odpowiedzi z Blueprint API"


def close_marketing_lead_direct(access_token: str, ml_id: str, close_status: str) -> Tuple[bool, str, Optional[str]]:
    """
    Próbuje zamknąć ML przez zwykły PUT. 
    Zwraca (success, message, error_code) - error_code np. 'RECORD_IN_BLUEPRINT'.
    """
    url = f"{ZOHO_CRM_API_BASE_URL}/Marketing_Leads/{ml_id}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "Content-Type": "application/json"
    }
    payload = {"data": [{"Etap_kwalifikacji_HL": close_status}]}
    json_data = json.dumps(payload).encode("utf-8")
    
    request = urllib.request.Request(url, data=json_data, headers=headers, method="PUT")
    time.sleep(REQUEST_SLEEP_S)
    
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_data = response.read().decode("utf-8")
            if response_data:
                result = json.loads(response_data)
                if "data" in result and len(result["data"]) > 0:
                    item = result["data"][0]
                    if item.get("code") == "SUCCESS":
                        return True, f"Zamknięto ML {ml_id} jako '{close_status}'", None
                    else:
                        return False, item.get("message", "Błąd"), item.get("code")
            return True, "OK", None
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else ""
        logging.error(f"HTTPError PUT {e.code} for {url}: {error_body}")
        
        # Parsuj błąd żeby wyciągnąć kod
        try:
            error_json = json.loads(error_body)
            if "data" in error_json and len(error_json["data"]) > 0:
                error_code = error_json["data"][0].get("code")
                error_msg = error_json["data"][0].get("message", "Błąd")
                return False, error_msg, error_code
        except:
            pass
        
        return False, f"HTTP {e.code}", None
    except Exception as e:
        return False, str(e), None


def close_marketing_lead(access_token: str, ml_id: str, close_status: str, blueprint_only: bool = False) -> Tuple[bool, str]:
    """Zamyka Marketing Lead ustawiając status. Obsługuje Blueprint."""
    
    # Upewnij się że ID to czysta liczba
    numeric_id = ml_id.strip() if ml_id else ""
    if not numeric_id.isdigit():
        return False, f"Nieprawidłowe ID: {ml_id}"
    
    # Tryb tylko Blueprint - pomiń zwykły PUT
    if blueprint_only:
        logging.info(f"Tryb TYLKO Blueprint - pomijam PUT, od razu transition...")
    else:
        # 1. Najpierw spróbuj zwykłego PUT
        success, msg, error_code = close_marketing_lead_direct(access_token, numeric_id, close_status)
        
        if success:
            return True, msg
        
        # Jeśli nie RECORD_IN_BLUEPRINT - zwróć błąd
        if error_code != "RECORD_IN_BLUEPRINT":
            return False, msg
        
        logging.info(f"Rekord {numeric_id} jest w Blueprint - próbuję transition...")
    
    # 2. Użyj Blueprint API
    logging.info(f"Pobieram transitions dla {numeric_id}...")
    
    # Pobierz dostępne transitions
    transitions = get_blueprint_transitions(access_token, numeric_id)
    if not transitions:
        return False, "Rekord w Blueprint, ale brak dostępnych przejść"
    
    # Wyświetl dostępne transitions
    logging.info(f"Dostępne przejścia: {[t.get('name', t.get('id')) for t in transitions]}")
    
    # Znajdź transition do docelowego statusu
    target_trans = find_transition_to_status(transitions, close_status)
    
    if not target_trans:
        # Może "odpad" jest pod inną nazwą - szukaj podobnych
        for keyword in ["odpad", "dyskwalifikac", "rezygnac", "zamkn", "koniec"]:
            target_trans = find_transition_to_status(transitions, keyword)
            if target_trans:
                break
    
    if not target_trans:
        trans_names = [f"{t.get('name', '?')} → {t.get('next_field_value', '?')}" for t in transitions]
        return False, f"Nie znaleziono przejścia do '{close_status}'. Dostępne: {trans_names}"
    
    # Wykonaj transition
    trans_id = target_trans.get("id")
    trans_name = target_trans.get("name", trans_id)
    logging.info(f"Wykonuję transition: {trans_name} (ID: {trans_id})")
    
    bp_success, bp_msg = execute_blueprint_transition(access_token, numeric_id, trans_id)
    if bp_success:
        return True, f"Zamknięto ML {numeric_id} przez Blueprint (transition: {trans_name})"
    else:
        return False, bp_msg


def get_marketing_lead_current_status(access_token: str, ml_id: str) -> Optional[str]:
    """Pobiera AKTUALNY status Marketing Leada z API."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Marketing_Leads/{ml_id}?fields=Etap_kwalifikacji_HL"
    response = execute_api_request(access_token, url, method="GET")
    
    if response and "data" in response and len(response["data"]) > 0:
        return response["data"][0].get("Etap_kwalifikacji_HL")
    return None


def is_ml_already_closed(access_token: str, ml_id: str) -> Tuple[bool, str]:
    """
    Sprawdza czy Marketing Lead jest już zamknięty (przez API).
    Zwraca (is_closed, current_status).
    """
    CLOSED_KEYWORDS = ["odpad", "odpadek", "zdyskwalifikowany", "rezygnacja", "duplikat", "brak kontaktu"]
    
    current_status = get_marketing_lead_current_status(access_token, ml_id)
    if not current_status:
        return False, "nieznany"
    
    status_lower = current_status.lower()
    for keyword in CLOSED_KEYWORDS:
        if keyword in status_lower:
            return True, current_status
    
    return False, current_status


def check_lead_exists_for_account(access_token: str, account_id: str) -> Optional[Dict[str, Any]]:
    """Sprawdza czy istnieje Lead dla danej firmy (Account ID). Zwraca dane Leada lub None."""
    # Szukamy Leada gdzie Firma_w_bazie = account_id
    url = f"{ZOHO_CRM_API_BASE_URL}/Leads/search?criteria=(Firma_w_bazie:equals:{account_id})"
    response = execute_api_request(access_token, url, method="GET")
    
    if response and "data" in response and len(response["data"]) > 0:
        lead = response["data"][0]
        return {
            "id": lead.get("id"),
            "name": lead.get("Last_Name") or lead.get("Full_Name") or lead.get("Company"),
            "status": lead.get("Lead_Status"),
            "owner": lead.get("Owner", {}).get("name") if isinstance(lead.get("Owner"), dict) else None
        }
    return None


def create_lead(access_token: str, lead_data: Dict[str, Any]) -> Tuple[bool, str, Optional[str]]:
    """Tworzy Lead w Zoho."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Leads"
    payload = {"data": [lead_data]}
    response = execute_api_request(access_token, url, method="POST", data=payload)
    
    if response and "data" in response and len(response["data"]) > 0:
        result = response["data"][0]
        if result.get("code") == "SUCCESS":
            lead_id = result["details"]["id"]
            return True, f"Utworzono Lead (ID: {lead_id})", lead_id
        else:
            return False, f"Błąd: {result.get('message', 'nieznany błąd')}", None
    return False, "Brak odpowiedzi z API", None


def get_available_tags(access_token: str, module: str = "Leads") -> List[Dict[str, Any]]:
    """Pobiera listę dostępnych tagów dla modułu."""
    url = f"{ZOHO_CRM_API_BASE_URL}/settings/tags?module={module}"
    response = execute_api_request(access_token, url, method="GET")
    
    if response and "tags" in response:
        return response["tags"]
    return []


def add_tag_to_lead(access_token: str, lead_id: str, tag_name: str) -> Tuple[bool, str]:
    """Dodaje tag do utworzonego Leada."""
    url = f"{ZOHO_CRM_API_BASE_URL}/Leads/actions/add_tags?ids={lead_id}"
    payload = {"tags": [{"name": tag_name}]}
    response = execute_api_request(access_token, url, method="POST", data=payload)
    
    if response and "data" in response and len(response["data"]) > 0:
        result = response["data"][0]
        if result.get("code") == "SUCCESS" or result.get("status", "").upper() == "SUCCESS":
            return True, f"Dodano tag '{tag_name}'"
        else:
            return False, f"Błąd tagu: {result.get('message', 'nieznany błąd')}"
    
    # Czasem API zwraca sukces bez sekcji data
    if response and response.get("code") == "SUCCESS":
        return True, f"Dodano tag '{tag_name}'"
    
    return False, "Brak odpowiedzi z API tagów"


def select_tag(access_token: str) -> Optional[str]:
    """Pozwala wybrać tag z listy lub utworzyć nowy."""
    print("\n" + "="*60)
    print("WYBÓR TAGU DLA LEADA")
    print("="*60)
    
    print("\nPobieram listę dostępnych tagów...")
    tags = get_available_tags(access_token, "Leads")
    
    if tags:
        print(f"\nZnaleziono {len(tags)} tagów:\n")
        for i, tag in enumerate(tags, 1):
            tag_name = tag.get("name", "?")
            tag_color = tag.get("color_code", "")
            print(f"  {i}. {tag_name}")
    else:
        print("\n⚠ Nie znaleziono istniejących tagów (lub brak uprawnień)")
    
    print(f"\n  N. Wpisz NOWY tag")
    print(f"  0. Pomiń (bez tagu)")
    
    choice = input("\nWybierz numer tagu, wpisz 'N' dla nowego, lub 0 [0]: ").strip()
    
    if not choice or choice == "0":
        return None
    
    # Nowy tag
    if choice.upper() == "N":
        new_tag = input("Wpisz nazwę nowego tagu: ").strip()
        if new_tag:
            print(f"✓ Nowy tag: {new_tag}")
            return new_tag
        return None
    
    # Wybór z listy
    if choice.isdigit() and tags:
        idx = int(choice) - 1
        if 0 <= idx < len(tags):
            selected = tags[idx].get("name", "")
            print(f"✓ Wybrano tag: {selected}")
            return selected
    
    # Może użytkownik wpisał nazwę tagu bezpośrednio
    if len(choice) > 1:
        print(f"✓ Użyto tagu: {choice}")
        return choice
    
    print("⚠ Nieprawidłowy wybór - pominięto tag")
    return None


def select_owner(access_token: str) -> Optional[str]:
    """Pozwala wybrać właściciela leada z listy aktywnych użytkowników."""
    print("\n" + "="*60)
    print("WYBÓR WŁAŚCICIELA LEADA (Owner)")
    print("="*60)
    
    print("\nPobieram listę aktywnych użytkowników...")
    users = get_active_users(access_token)
    
    if not users:
        print("⚠ Nie udało się pobrać listy użytkowników")
        return None
    
    print(f"\nZnaleziono {len(users)} aktywnych użytkowników:\n")
    for i, user in enumerate(users, 1):
        role = user.get("role", {}).get("name", "brak roli") if isinstance(user.get("role"), dict) else "brak roli"
        print(f"  {i}. {user.get('full_name', 'Bez nazwy')} (ID: {user.get('id')}) - {role}")
    
    print(f"\n  0. Pomiń (domyślny owner)")
    
    choice = input("\nWybierz numer użytkownika lub wpisz ID [0]: ").strip()
    
    if not choice or choice == "0":
        return None
    
    # Jeśli podano numer z listy
    if choice.isdigit():
        idx = int(choice) - 1
        if 0 <= idx < len(users):
            selected = users[idx]
            print(f"✓ Wybrano: {selected.get('full_name')} (ID: {selected.get('id')})")
            return selected.get("id")
    
    # Może to być bezpośrednie ID
    if len(choice) > 10:
        print(f"✓ Użyto podanego ID: {choice}")
        return choice
    
    print("⚠ Nieprawidłowy wybór - pominięto owner'a")
    return None


def load_file(file_path: str) -> Optional[pd.DataFrame]:
    """Wczytuje plik CSV lub XLSX."""
    path = Path(file_path)
    if not path.exists():
        print(f"✗ Plik nie istnieje: {file_path}")
        return None
    
    try:
        if path.suffix.lower() == ".xlsx":
            return pd.read_excel(path, dtype=str)
        else:
            # Próbuj różne kodowania
            for encoding in ["utf-8", "cp1250", "latin1"]:
                try:
                    return pd.read_csv(path, dtype=str, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            return pd.read_csv(path, dtype=str, encoding="utf-8", errors="replace")
    except Exception as e:
        print(f"✗ Błąd wczytywania pliku: {e}")
        return None


def find_column(df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
    """Szuka kolumny po możliwych nazwach."""
    for name in possible_names:
        if name in df.columns:
            return name
        # Case-insensitive
        for col in df.columns:
            if col.lower() == name.lower():
                return col
    return None


def main():
    print("\n" + "="*60)
    print("  KONWERSJA MARKETING LEAD → LEAD")
    print("  (Zamknięcie ML + utworzenie Lead)")
    print("="*60)
    
    # 1. Wczytaj plik
    if len(sys.argv) > 1:
        file_path = sys.argv[1].strip().strip('"').strip("'")
    else:
        print("\nPodaj ścieżkę do pliku z Marketing Leads (CSV/XLSX):")
        file_path = input("> ").strip().strip('"').strip("'")
    
    if not file_path:
        print("✗ Nie podano pliku")
        sys.exit(1)
    
    df = load_file(file_path)
    if df is None or df.empty:
        print("✗ Nie udało się wczytać pliku lub plik jest pusty")
        sys.exit(1)
    
    print(f"\n✓ Wczytano {len(df)} rekordów")
    print(f"  Kolumny: {', '.join(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}")
    
    # 2. Znajdź kluczowe kolumny
    id_col = find_column(df, ["id", "ID", "Id", "marketing_lead_id", "ML_ID"])
    account_col = find_column(df, ["Firma_w_bazie", "Account_ID", "account_id", "Firma"])
    name_col = find_column(df, ["Name", "name", "Nazwa", "Imie", "Last_Name"])
    
    if not id_col:
        print("\n⚠ Nie znaleziono kolumny z ID Marketing Lead")
        print("Dostępne kolumny:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        choice = input("\nWybierz numer kolumny z ID: ").strip()
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(df.columns):
                id_col = df.columns[idx]
    
    if not account_col:
        print("\n⚠ Nie znaleziono kolumny z ID firmy (Firma_w_bazie)")
        print("Dostępne kolumny:")
        for i, col in enumerate(df.columns, 1):
            print(f"  {i}. {col}")
        choice = input("\nWybierz numer kolumny z ID firmy: ").strip()
        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(df.columns):
                account_col = df.columns[idx]
    
    if not id_col or not account_col:
        print("✗ Brak wymaganych kolumn (ID i Firma_w_bazie)")
        sys.exit(1)
    
    print(f"\n✓ Kolumna ID Marketing Lead: {id_col}")
    print(f"✓ Kolumna ID firmy: {account_col}")
    if name_col:
        print(f"✓ Kolumna nazwy: {name_col}")
    
    # 3. Znajdź kolumnę ze statusem ML i odfiltruj już zamknięte
    status_col = find_column(df, ["Etap_kwalifikacji_HL", "etap_kwalifikacji_hl", "Status", "status"])
    
    # Statusy oznaczające "zamknięty" - nie przetwarzamy takich rekordów
    CLOSED_STATUSES = ["odpad", "odpadek", "zdyskwalifikowany", "rezygnacja", "duplikat", "brak kontaktu"]
    
    def is_closed_status(status_value: str) -> bool:
        """Sprawdza czy status oznacza zamknięty rekord."""
        if not status_value or pd.isna(status_value):
            return False
        status_lower = str(status_value).lower().strip()
        for closed in CLOSED_STATUSES:
            if closed in status_lower:
                return True
        return False
    
    # Zlicz ile rekordów jest otwartych vs zamkniętych
    if status_col:
        open_count = sum(1 for _, row in df.iterrows() if not is_closed_status(str(row.get(status_col, ""))))
        closed_count = len(df) - open_count
        print(f"\n✓ Kolumna statusu: {status_col}")
        print(f"  Otwartych: {open_count}, Zamkniętych: {closed_count}")
    else:
        print("\n⚠ Nie znaleziono kolumny ze statusem - nie można filtrować zamkniętych")
    
    # 4. Wybór rekordów do przetworzenia
    print("\n" + "-"*60)
    print("WYBÓR REKORDÓW DO PRZETWORZENIA")
    print("-"*60)
    
    print("\nOpcje:")
    print("  1. Tylko PIERWSZY OTWARTY rekord (test)")
    print("  2. Wybrane rekordy (podaj numery wierszy)")
    print("  3. WSZYSTKIE OTWARTE rekordy (masowo)")
    
    mode_choice = input("\nWybierz opcję [1]: ").strip() or "1"
    
    if mode_choice == "1":
        # Znajdź pierwszy OTWARTY rekord - SPRAWDZAJĄC PRZEZ API!
        print("\nSzukam pierwszego OTWARTEGO rekordu (sprawdzam przez API)...")
        
        # Najpierw pobierz token żeby móc sprawdzać
        try:
            temp_token = get_access_token()
        except Exception as e:
            print(f"✗ Błąd pobierania tokena: {e}")
            sys.exit(1)
        
        first_open_idx = None
        for idx, (_, row) in enumerate(df.iterrows()):
            ml_id_check = extract_numeric_id(str(row[id_col]))
            if not ml_id_check:
                continue
            
            # Sprawdź AKTUALNY status przez API
            is_closed, api_status = is_ml_already_closed(temp_token, ml_id_check)
            status_display = api_status[:30] + "..." if len(api_status) > 30 else api_status
            
            if is_closed:
                print(f"  Wiersz {idx+1}: {status_display} - ZAMKNIĘTY, pomijam")
                continue
            else:
                print(f"  Wiersz {idx+1}: {status_display} - OTWARTY ✓")
                first_open_idx = idx
                break
        
        if first_open_idx is not None:
            selected_indices = [first_open_idx]
            print(f"\n✓ Wybrany: pierwszy OTWARTY rekord (wiersz {first_open_idx + 1})")
        else:
            print("\n✗ Nie znaleziono żadnego otwartego rekordu!")
            sys.exit(0)
    elif mode_choice == "2":
        print(f"  (Dostępne wiersze: 1-{len(df)})")
        rows_input = input("Podaj numery wierszy (np. 1,2,5 lub 1-5): ").strip()
        selected_indices = []
        for part in rows_input.split(","):
            part = part.strip()
            if "-" in part:
                try:
                    start, end = part.split("-")
                    for i in range(int(start)-1, int(end)):
                        if 0 <= i < len(df):
                            selected_indices.append(i)
                except:
                    pass
            elif part.isdigit():
                idx = int(part) - 1
                if 0 <= idx < len(df):
                    selected_indices.append(idx)
                else:
                    print(f"  ⚠ Wiersz {part} poza zakresem - pominięty")
        
        if not selected_indices:
            print("✗ Nie wybrano żadnych prawidłowych wierszy!")
            sys.exit(0)
        
        print(f"✓ Wybrane wiersze: {len(selected_indices)}")
    else:
        # Wszystkie OTWARTE rekordy
        if status_col:
            selected_indices = []
            for idx, (_, row) in enumerate(df.iterrows()):
                status_val = str(row.get(status_col, ""))
                if not is_closed_status(status_val):
                    selected_indices.append(idx)
            print(f"✓ Wszystkie OTWARTE rekordy: {len(selected_indices)}")
        else:
            selected_indices = list(range(len(df)))
            print(f"✓ Wszystkie rekordy: {len(selected_indices)} (brak filtra statusu)")
    
    # Filtruj do wybranych
    selected_df = df.iloc[selected_indices].copy()
    
    # 4. Pokaż podgląd
    print("\n" + "-"*60)
    print("PODGLĄD WYBRANYCH REKORDÓW")
    print("-"*60)
    
    for idx, (_, row) in enumerate(selected_df.iterrows()):
        ml_id = extract_numeric_id(str(row[id_col]))
        account_id = extract_numeric_id(str(row[account_col]))
        name = row.get(name_col, "brak nazwy") if name_col else "brak nazwy"
        current_status = row.get(status_col, "?") if status_col else "?"
        print(f"\n  {idx+1}. ML ID: {ml_id}")
        print(f"     Nazwa: {name}")
        print(f"     Status: {current_status}")
        print(f"     Firma ID: {account_id}")
    
    # 5. Pobierz token
    print("\n" + "-"*60)
    print("AUTORYZACJA")
    print("-"*60)
    
    print("\nPobieram token dostępowy...")
    try:
        access_token = get_access_token()
        print("✓ Token uzyskany")
    except Exception as e:
        print(f"✗ Błąd pobierania tokena: {e}")
        sys.exit(1)
    
    # 6. Wybór statusu zamknięcia ML
    print("\n" + "-"*60)
    print("STATUS ZAMKNIĘCIA MARKETING LEAD")
    print("-"*60)
    
    print("\nDostępne statusy zamknięcia:")
    for i, status in enumerate(ML_CLOSE_STATUSES, 1):
        print(f"  {i}. {status}")
    
    close_choice = input(f"\nWybierz status [1 = {ML_CLOSE_STATUSES[0]}]: ").strip() or "1"
    if close_choice.isdigit() and 1 <= int(close_choice) <= len(ML_CLOSE_STATUSES):
        close_status = ML_CLOSE_STATUSES[int(close_choice) - 1]
    else:
        close_status = ML_CLOSE_STATUSES[0]
    print(f"✓ Status zamknięcia ML: {close_status}")
    
    # 6b. Tryb Blueprint
    print("\nCzy użyć TYLKO Blueprint API? (szybsze jeśli wszystkie rekordy są w Blueprint)")
    blueprint_only = input("  Tylko Blueprint? (t/n) [t]: ").strip().lower() or "t"
    use_blueprint_only = blueprint_only in ["t", "tak", "y", "yes"]
    if use_blueprint_only:
        print("✓ Tryb: TYLKO Blueprint (pominięcie zwykłego PUT)")
    else:
        print("✓ Tryb: Najpierw PUT, potem Blueprint jako fallback")
    
    # 7. Wybór statusu nowego Lead
    print("\n" + "-"*60)
    print("STATUS NOWEGO LEAD")
    print("-"*60)
    
    print("\nDostępne statusy Lead:")
    for i, status in enumerate(LEAD_STATUSES, 1):
        print(f"  {i}. {status}")
    
    lead_choice = input(f"\nWybierz status [1 = {LEAD_STATUSES[0]}]: ").strip() or "1"
    if lead_choice.isdigit() and 1 <= int(lead_choice) <= len(LEAD_STATUSES):
        lead_status = LEAD_STATUSES[int(lead_choice) - 1]
    else:
        lead_status = LEAD_STATUSES[0]
    print(f"✓ Status nowego Lead: {lead_status}")
    
    # 8. Wybór Owner'a
    owner_id = select_owner(access_token)
    
    # 8b. Wybór tagu
    selected_tag = select_tag(access_token)
    
    # 9. Prefiks nazwy
    print("\n" + "-"*60)
    print("PREFIKS NAZWY LEADA")
    print("-"*60)
    
    print("\nCzy dodać prefiks przed nazwą firmy?")
    print("  Przykłady: [TEST], [CC], [AKWIZYCJA], lub własny")
    print("  Pozostaw puste = brak prefiksu")
    
    name_prefix = input("\nWpisz prefiks (lub Enter = brak): ").strip()
    if name_prefix:
        print(f"✓ Prefiks: {name_prefix}")
    
    # 10. Potwierdzenie
    print("\n" + "="*60)
    print("PODSUMOWANIE")
    print("="*60)
    print(f"\n  Rekordów do przetworzenia: {len(selected_df)}")
    print(f"  Status zamknięcia ML: {close_status}")
    print(f"  Tryb zamykania: {'TYLKO Blueprint' if use_blueprint_only else 'PUT + Blueprint fallback'}")
    print(f"  Status nowego Lead: {lead_status}")
    print(f"  Owner: {owner_id or 'domyślny'}")
    print(f"  Tag: {selected_tag or '(brak)'}")
    print(f"  Prefiks nazwy: {name_prefix or '(brak)'}")
    
    confirm = input("\n⚠ Czy wykonać operację? (t/n): ").strip().lower()
    if confirm not in ["t", "tak", "y", "yes"]:
        print("Anulowano")
        sys.exit(0)
    
    # 11. Wykonanie
    print("\n" + "="*60)
    print("WYKONYWANIE...")
    print("="*60)
    
    results = []
    total_records = len(selected_df)
    
    # Statystyki na bieżąco
    stats = {"zamkniete": 0, "utworzone": 0, "pominiete": 0, "bledy": 0}
    
    # Iterator z paskiem postępu
    if TQDM_AVAILABLE and total_records > 1:
        iterator = tqdm(selected_df.iterrows(), total=total_records, desc="Przetwarzanie", unit="rek")
    else:
        iterator = selected_df.iterrows()
    
    for idx, (orig_idx, row) in enumerate(iterator):
        ml_id = extract_numeric_id(str(row[id_col]))
        account_id = extract_numeric_id(str(row[account_col]))
        name = str(row.get(name_col, "")) if name_col else ""
        
        # Aktualizuj opis paska postępu
        if TQDM_AVAILABLE and total_records > 1:
            iterator.set_postfix({"OK": stats["utworzone"], "skip": stats["pominiete"], "err": stats["bledy"]})
        
        print(f"\n--- Rekord {idx+1}/{total_records} ---")
        print(f"  ML ID: {ml_id}")
        print(f"  Firma ID: {account_id}")
        
        result = {
            "wiersz": orig_idx + 1,
            "ml_id": ml_id,
            "account_id": account_id,
            "nazwa_ml": name,
            "ml_zamkniety": False,
            "ml_status": "",
            "lead_utworzony": False,
            "lead_id": "",
            "lead_nazwa": "",
            "tag": "",
            "błędy": []
        }
        
        if not ml_id:
            result["błędy"].append("Brak ID Marketing Lead")
            results.append(result)
            continue
        
        if not account_id:
            result["błędy"].append("Brak ID firmy")
            results.append(result)
            continue
        
        # A) Sprawdź AKTUALNY status ML z API (plik może być nieaktualny!)
        print(f"  → Sprawdzanie aktualnego statusu ML...")
        is_closed, current_status = is_ml_already_closed(access_token, ml_id)
        print(f"    Status w API: {current_status}")
        
        if is_closed:
            print(f"    ℹ ML już zamknięty ({current_status}) - sprawdzam Lead...")
            result["ml_zamkniety"] = True
            result["ml_status"] = f"już zamknięty ({current_status})"
            # NIE pomijamy - kontynuujemy do sprawdzenia Leada!
        else:
            # B) Zamknij Marketing Lead
            mode_text = "Blueprint" if use_blueprint_only else "PUT/Blueprint"
            print(f"  → Zamykanie ML jako '{close_status}' ({mode_text})...")
            success, msg = close_marketing_lead(access_token, ml_id, close_status, blueprint_only=use_blueprint_only)
            if success:
                print(f"    ✓ {msg}")
                result["ml_zamkniety"] = True
                result["ml_status"] = close_status
                stats["zamkniete"] += 1
            else:
                print(f"    ✗ {msg}")
                result["błędy"].append(f"ML: {msg}")
        
        # B) Pobierz nazwę firmy
        print(f"  → Pobieram nazwę firmy...")
        account_name = fetch_account_name(access_token, account_id)
        if not account_name:
            print(f"    ✗ Nie znaleziono firmy {account_id}")
            result["błędy"].append(f"Nie znaleziono firmy {account_id}")
            results.append(result)
            continue
        
        print(f"    ✓ Firma: {account_name}")
        
        # C) Sprawdź czy Lead już istnieje dla tej firmy
        print(f"  → Sprawdzanie czy Lead już istnieje...")
        existing_lead = check_lead_exists_for_account(access_token, account_id)
        
        # Statusy "nieaktywne" - jeśli Lead ma taki status, można tworzyć nowy
        INACTIVE_LEAD_STATUSES = [
            None, "", "-None-",
            "Zakwalifikowane do sales", "Pre-Qualified",
            "Zdyskwalifikowany", "Junk Lead",
            "AutoOdrzucony", "Not Qualified",
            "AutoOdrzucone1", "AutoOdrzucone",
            "Kontakt w przyszłości", "Contact in Future",
            "Leady przegrane", "Lost Lead",
        ]
        
        if existing_lead:
            existing_name = existing_lead.get("name", "?")
            existing_id = existing_lead.get("id", "?")
            existing_status = existing_lead.get("status")
            existing_owner = existing_lead.get("owner", "?")
            
            # Sprawdź czy status jest "nieaktywny" - można tworzyć nowy Lead
            status_is_inactive = (
                existing_status is None or 
                existing_status in INACTIVE_LEAD_STATUSES or
                any(inactive.lower() in str(existing_status).lower() for inactive in INACTIVE_LEAD_STATUSES if inactive)
            )
            
            if status_is_inactive:
                print(f"    ℹ Lead istnieje ale ma NIEAKTYWNY status: '{existing_status}'")
                print(f"      ID: {existing_id}, Nazwa: {existing_name}")
                print(f"    ✓ Można utworzyć nowy Lead")
            else:
                print(f"    ⚠ LEAD JUŻ ISTNIEJE (AKTYWNY)!")
                print(f"      ID: {existing_id}")
                print(f"      Nazwa: {existing_name}")
                print(f"      Status: {existing_status}")
                print(f"      Owner: {existing_owner}")
                result["błędy"].append(f"Lead już istnieje (ID: {existing_id}, Status: {existing_status})")
                results.append(result)
                stats["pominiete"] += 1
                continue
        else:
            print(f"    ✓ Brak istniejącego Leada - można utworzyć")
        
        # D) Utwórz Lead - nazwa max 120 znaków
        if name_prefix:
            lead_name = f"{name_prefix} {account_name}"
        else:
            lead_name = account_name
        
        # Ogranicz do 80 znaków (limit Zoho dla Last_Name)
        if len(lead_name) > 80:
            lead_name = lead_name[:77] + "..."
        
        lead_data = {
            "Last_Name": lead_name,
            "Company": account_name,
            "Firma_w_bazie": account_id,
            "Lead_Status": lead_status,
            "Czy_firma_jest_w_bazie": True,
            "Outbound_Inbound": "Outbound",
            "Zrodlo_outbound": "Akwizycja telefoniczna",
        }
        
        if owner_id:
            lead_data["Owner"] = owner_id
        
        print(f"  → Tworzenie Lead '{lead_name}'...")
        success, msg, new_lead_id = create_lead(access_token, lead_data)
        if success:
            print(f"    ✓ {msg}")
            result["lead_utworzony"] = True
            result["lead_id"] = new_lead_id
            result["lead_nazwa"] = lead_name
            stats["utworzone"] += 1
            
            # D) Dodaj tag jeśli wybrany
            if selected_tag and new_lead_id:
                print(f"  → Dodawanie tagu '{selected_tag}'...")
                tag_success, tag_msg = add_tag_to_lead(access_token, new_lead_id, selected_tag)
                if tag_success:
                    print(f"    ✓ {tag_msg}")
                    result["tag"] = selected_tag
                else:
                    print(f"    ⚠ {tag_msg}")
                    result["błędy"].append(f"Tag: {tag_msg}")
        else:
            print(f"    ✗ {msg}")
            stats["bledy"] += 1
            result["błędy"].append(f"Lead: {msg}")
        
        results.append(result)
    
    # 12. Zapisz raport
    print("\n" + "="*60)
    print("ZAPIS RAPORTU")
    print("="*60)
    
    output_dir = Path("wyniki_convert_ml_to_lead")
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"convert_results_{timestamp}.csv"
    xlsx_path = output_dir / f"convert_results_{timestamp}.xlsx"
    
    results_df = pd.DataFrame(results)
    results_df["błędy"] = results_df["błędy"].apply(lambda x: "; ".join(x) if isinstance(x, list) else x)
    
    results_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    results_df.to_excel(xlsx_path, index=False)
    
    print(f"\n✓ Raport CSV: {csv_path}")
    print(f"✓ Raport XLSX: {xlsx_path}")
    
    # 13. Podsumowanie
    print("\n" + "="*60)
    print("PODSUMOWANIE")
    print("="*60)
    
    ml_closed = sum(1 for r in results if r["ml_zamkniety"])
    leads_created = sum(1 for r in results if r["lead_utworzony"])
    
    # Rozdziel pominięcia od błędów
    skipped_ml_closed = sum(1 for r in results if "już zamknięty" in str(r.get("ml_status", "")))
    skipped_lead_exists = sum(1 for r in results if any("Lead już istnieje" in str(e) for e in r.get("błędy", [])))
    real_errors = sum(1 for r in results if r["błędy"] and not any("już zamknięty" in str(e) or "Lead już istnieje" in str(e) for e in r["błędy"]))
    
    print(f"\n  Marketing Leads zamknięte: {ml_closed}/{len(results)}")
    print(f"  Leads utworzone: {leads_created}/{len(results)}")
    print(f"  Pominięte (ML już zamknięty): {skipped_ml_closed}")
    print(f"  Pominięte (Lead już istnieje): {skipped_lead_exists}")
    if real_errors > 0:
        print(f"  ❌ Błędy: {real_errors}")
    
    print("\n✓ Zakończono")


if __name__ == "__main__":
    main()
