"""
Skrypt do masowej aktualizacji pola Etap_kwalifikacji_HL (statusu) w module Marketing_Leads.
"""

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

from refresh_zoho_access_token import refresh_access_token

# --- Konfiguracja API Zoho ---
ZOHO_CRM_API_BASE_URL = "https://www.zohoapis.eu/crm/v8"
REQUEST_SLEEP_S = 0.2
MAX_RETRY_TIMEOUT = 2

# --- Logi ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("update_marketing_lead_status.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

ACCESS_TOKEN_HOLDER: Dict[str, Any] = {"token": None, "refreshed_at": 0.0}


def get_access_token() -> str:
    client_id = os.environ.get("ZOHO_MEDIDESK_CLIENT_ID", "WPISZ_CLIENT_ID")
    client_secret = os.environ.get("ZOHO_MEDIDESK_CLIENT_SECRET", "WPISZ_CLIENT_SECRET")
    refresh_token = os.environ.get("ZOHO_MEDIDESK_REFRESH_TOKEN", "WPISZ_REFRESH_TOKEN")

    if any(val.startswith("WPISZ_") for val in [client_id, client_secret, refresh_token]):
        raise RuntimeError(
            "Błąd: Dane (client_id, client_secret, refresh_token) nie zostały skonfigurowane."
        )

    token_info = refresh_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )
    ACCESS_TOKEN_HOLDER["token"] = token_info["access_token"]
    ACCESS_TOKEN_HOLDER["refreshed_at"] = time.time()
    return token_info["access_token"]


def execute_api_request(access_token: str, url: str, method: str = "GET", data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    def _do_request(tok: str) -> Optional[Dict[str, Any]]:
        headers = {"Authorization": f"Zoho-oauthtoken {tok}"}
        if method in {"PUT", "POST"}:
            headers["Content-Type"] = "application/json"
            payload = json.dumps(data).encode("utf-8") if data else b""
            request = urllib.request.Request(url, data=payload, headers=headers, method=method)
        else:
            request = urllib.request.Request(url, headers=headers, method=method)
        time.sleep(REQUEST_SLEEP_S)
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else None

    try:
        tok = ACCESS_TOKEN_HOLDER.get("token") or access_token
        return _do_request(tok)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        logging.error(f"HTTPError {method} {e.code} for {url}: {body}")
        if e.code in (401, 403):
            new_token = get_access_token()
            return _do_request(new_token)
        return None
    except Exception as e:
        logging.error(f"Błąd API: {e}")
        return None


def load_file(file_path: str) -> pd.DataFrame:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Plik nie istnieje: {file_path}")

    if path.suffix.lower() == ".csv":
        for encoding in ["utf-8-sig", "utf-8", "cp1252", "iso-8859-1"]:
            try:
                return pd.read_csv(path, dtype=str, encoding=encoding)
            except UnicodeDecodeError:
                continue
        raise ValueError("Błąd kodowania pliku CSV")
    elif path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype=str)
    else:
        raise ValueError("Obsługiwane formaty: CSV, XLSX")


def show_columns(df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("DOSTĘPNE KOLUMNY:")
    print("=" * 60)
    for idx, col in enumerate(df.columns, 1):
        non_empty = df[col].notna().sum()
        print(f"{idx:3d}. {col:40s} (zapełnienie: {non_empty}/{len(df)})")
    print("=" * 60)


def get_column_by_number_or_name(df: pd.DataFrame, user_input: str) -> Optional[str]:
    user_input = user_input.strip()
    if user_input.isdigit():
        idx = int(user_input) - 1
        if 0 <= idx < len(df.columns):
            return df.columns[idx]
    matches = [col for col in df.columns if user_input.lower() in col.lower()]
    if len(matches) == 1:
        return matches[0]
    if user_input in df.columns:
        return user_input
    return None


def save_results(df: pd.DataFrame, output_dir: str, base_name: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path(output_dir) / f"{base_name}_{timestamp}.csv"
    xlsx_path = Path(output_dir) / f"{base_name}_{timestamp}.xlsx"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    logging.info(f"Zapisano raporty: {csv_path}, {xlsx_path}")


def update_marketing_lead(access_token: str, record_id: str, new_stage: str) -> Optional[Dict[str, Any]]:
    url = f"{ZOHO_CRM_API_BASE_URL}/Marketing_Leads"
    payload = {
        "data": [
            {
                "id": record_id,
                "Etap_kwalifikacji_HL": new_stage
            }
        ]
    }
    return execute_api_request(access_token, url, method="PUT", data=payload)


def main() -> None:
    print("\n==============================")
    print("AKTUALIZACJA STATUSU MARKETING LEADÓW")
    print("==============================\n")
    if len(sys.argv) > 1:
        file_path = sys.argv[1].strip().strip('"').strip("'")
    else:
        file_path = input("Podaj ścieżkę do pliku CSV lub XLSX: ").strip().strip('"').strip("'")
    if not file_path:
        print("✗ Nie podano pliku")
        sys.exit(1)

    df = load_file(file_path)
    print(f"✓ Wczytano {len(df)} wierszy, {len(df.columns)} kolumn")

    show_columns(df)
    col_input = input("\nPodaj numer lub nazwę kolumny z ID marketing leada: ").strip()
    lead_col = get_column_by_number_or_name(df, col_input)
    if not lead_col:
        print("✗ Nieprawidłowa kolumna")
        sys.exit(1)

    statuses = [
        "-None-", "odpad (odpadek)", "nowy", "przetworzony",
        "informacja (w trakcie przetwarzania)", "po analizie danych (weryfikacja powiązań)",
        "utworzony Lead/Deal (Utwórz rekord)", "informacja czy akcja ? (Informacja czy Akcja)", "akcja (Akcja)"
    ]
    print("\nDostępne statusy:")
    for idx, status in enumerate(statuses, 1):
        print(f"{idx:2d}. {status}")
    status_choice = input("Wybierz numer statusu: ").strip()
    if not status_choice.isdigit() or not (1 <= int(status_choice) <= len(statuses)):
        print("✗ Nieprawidłowy wybór statusu")
        sys.exit(1)
    stage_value = statuses[int(status_choice) - 1]

    print("\nPobieram token dostępu...")
    access_token = get_access_token()

    results = []
    for idx, row in df.iterrows():
        raw_id = str(row[lead_col]).strip()
        record_id: str = ""
        if raw_id:
            match = re.search(r"(\d{5,})", raw_id)
            record_id = match.group(1) if match else raw_id
        if not record_id or record_id.lower() in {"nan", "none", ""}:
            results.append({"wiersz": idx + 1, "lead_id": record_id, "status": "POMINIĘTY", "szczegóły": "brak ID"})
            continue
        response = update_marketing_lead(access_token, record_id, stage_value)
        if response and "data" in response and response["data"]:
            data = response["data"][0]
            if data.get("code") == "SUCCESS":
                results.append({"wiersz": idx + 1, "lead_id": record_id, "status": "SUKCES", "szczegóły": ""})
            else:
                results.append({"wiersz": idx + 1, "lead_id": record_id, "status": "BŁĄD", "szczegóły": data.get("message", "")})
        else:
            results.append({"wiersz": idx + 1, "lead_id": record_id, "status": "BŁĄD", "szczegóły": "brak odpowiedzi"})

    results_df = pd.DataFrame(results)
    save_results(results_df, "wyniki_update_marketing_lead_status", "update_marketing_lead_status_results")

    success = (results_df["status"] == "SUKCES").sum()
    failures = (results_df["status"] == "BŁĄD").sum()
    print(f"\nGotowe: {success} sukcesów, {failures} błędów.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Przerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
