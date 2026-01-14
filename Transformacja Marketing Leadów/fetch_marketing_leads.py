"""
Skrypt do pobierania wszystkich rekordów z modułu Marketing Leads w Zoho CRM.
Używa API v8 i zapisuje wyniki do CSV i XLSX.
"""

import csv
import json
import logging
import os
import pickle
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
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

# Aktualny token
ACCESS_TOKEN_HOLDER: Dict[str, Any] = {"token": None, "refreshed_at": 0.0}

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fetch_marketing_leads.log", encoding="utf-8"),
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


def execute_api_request(access_token: str, url: str) -> Optional[Dict[str, Any]]:
    """Wykonuje żądanie API z automatycznym odświeżaniem tokena."""
    def _do_request(tok: str) -> Optional[Dict[str, Any]]:
        headers = {"Authorization": f"Zoho-oauthtoken {tok}"}
        request = urllib.request.Request(url, headers=headers, method="GET")
        time.sleep(REQUEST_SLEEP_S)
        logging.debug(f"HTTP GET: {url}")
        with urllib.request.urlopen(request, timeout=30) as response:
            data = response.read().decode("utf-8")
            if not data:
                return None
            return json.loads(data)

    try:
        tok0 = ACCESS_TOKEN_HOLDER.get("token") or access_token
        return _do_request(tok0)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        logging.error(f"HTTPError GET {e.code} for {url}: {body}")
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
        logging.error(f"Błąd podczas wykonywania zapytania do API ({url}): {e}")
        return None


def chunk_fields(all_fields: List[str], chunk_size: int = 45) -> List[List[str]]:
    """Dzieli kolekcję pól na mniejsze porcje zawierające maksymalnie chunk_size elementów (zawsze zawiera 'id')."""
    unique_fields = []
    seen = set()
    for field in all_fields:
        if field not in seen:
            seen.add(field)
            unique_fields.append(field)

    chunks = []
    for i in range(0, len(unique_fields), chunk_size):
        chunk = unique_fields[i:i + chunk_size]
        if "id" not in chunk:
            chunk = ["id"] + chunk[:-1]
        chunks.append(chunk)
    return chunks


def get_marketing_leads_fields() -> List[str]:
    """
    Zwraca listę WSZYSTKICH pól API dla Marketing_Leads wg pliku csv.
    Lista używana będzie do podziału na porcje (max 50 pól na zapytanie).
    """
    return [
        "powiadomienieTeams", "powiadomienieTeams1", "Brak_funkcjonalnosci",
        "Czy_powiazac_w_bazie_osobe_z_firma", "Czy_zaopiekowany_namiar", "Data_Hot_Lead",
        "Deal_utworzony", "Decyzja_Partnera", "Departament", "Drugie_imie",
        "Etap_kwalifikacji_HL", "Facebook_source", "Firma", "Polecenie_firma",
        "Forma_Hot_Lead", "HTML", "Hot_Lead", "searchResultDeDuplicated",
        "existingContactsaccount", "Imie", "Komu_przekazujemy", "Komunikaty",
        "Konferencja", "Konferencja_medidesk", "Konferencja_zewnetrzna",
        "Kto_zaopiekowal_sie_namiarem", "L_gabinetow", "L_osob_w_rejestracji",
        "L_stanowisk_rejestracji", "Lead_utworzony", "Lead_zreinkarnowany",
        "searchResultDeDuplicatedCount", "existingContactspoemailuCount",
        "existingContactsTelkomorkowyCount", "existingContactsTelstacjonarnyCount",
        "existingContactsaccountCount", "NIP", "Nazwisko", "Notatki_przy_przekazaniu",
        "Osoba_kontynuujaca_komunikacje", "Outbound_Inbound", "Polecenie_partner",
        "Pobral_EBooka_FB", "Pobral_EBooka_FB_data", "Polecenie", "Powiazanie_z_CC",
        "Firma_w_bazie", "Kontakt_w_bazie", "Polecenie_pracownik",
        "Przyczyna_dyskwalifikacji", "Pytanie_1", "Pytanie_2", "Rekord_testowy",
        "Stanowisko", "Strona_internetowa_medidesk", "Zakres_szkolenia",
        "Telefon_komorkowy", "Telefon_stacjonarny", "Telefon_SMS",
        "Telefon_polaczenie", "Zgoda_poliyka_prywatnosci_tresc",
        "Tresc_wiadomosci_z_formularza", "Zgoda_handlowa_tresc",
        "Zgoda_inna_tresc", "Zgoda_marketingowa_tresc", "Umowil_sie_na_spotkanie",
        "Umowil_sie_na_spotkanie_data", "Webinar", "Webinar_medidesk",
        "Webinar_zewnetrzny", "Wiadomosc_Email", "Zapisal_sie_na_TTP",
        "Zapisal_sie_na_TTP_data", "Zgoda_handlowa", "Zgoda_inna",
        "Zgoda_marketingowa", "Zgoda_polityka_prywatnosci", "Lead_Source",
        "Plec", "ids_search", "utm_campaign", "utm_medium", "utm_source",
        "Zrodlo_inbound", "Zrodlo_uzupelnienia_formularza", "Email",
        "Created_Time", "Unsubscribed_Time", "Modified_Time", "Last_Activity_Time",
        "Data_Processing_Basis_Details", "Locked__s", "Name", "Owner",
        "Data_Processing_Basis", "Unsubscribed_Mode", "Layout", "Created_By",
        "Modified_By", "Tag", "Email_Opt_Out", "Data_Source",
    ]


def fetch_records_for_fields(access_token: str, fields: str) -> List[Dict[str, Any]]:
    """Pobiera wszystkie rekordy dla danego zestawu pól."""
    records: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    page = 1
    total_rows = 0

    while True:
        params: Dict[str, Any] = {
            "per_page": 200,
            "fields": fields
        }
        if page_token:
            params["page_token"] = page_token
        else:
            params["page"] = page

        url = f"{ZOHO_CRM_API_BASE_URL}/Marketing_Leads?{urllib.parse.urlencode(params)}"
        payload = execute_api_request(access_token, url) or {}
        data = payload.get("data", []) or []
        records.extend(data)
        total_rows += len(data)

        info = payload.get("info", {}) or {}
        if not info.get("more_records"):
            break

        page_token = info.get("next_page_token") or info.get("page_token")
        if not page_token:
            logging.warning("Brak page_token mimo more_records=True. Przerywam.")
            break

        page += 1

    logging.debug(f"Pobrano {total_rows} rekordów dla pola(e): {fields.split(',')[:3]}...")
    return records


def fetch_all_marketing_leads(access_token: str, use_cache: bool = True, cache_file: str = "marketing_leads_cache.pkl") -> List[Dict[str, Any]]:
    """
    Pobiera WSZYSTKIE rekordy z Marketing Leads w porcjach pól (do 45 na request).
    """
    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, "rb") as f:
                cached_data = pickle.load(f)
            logging.info(f"Wczytano dane z cache: {cache_file} ({len(cached_data)} rekordów)")
            return cached_data
        except Exception as e:
            logging.warning(f"Nie udało się wczytać cache ({cache_file}): {e}. Pobieram z API...")

    field_chunks = chunk_fields(get_marketing_leads_fields())
    logging.info(f"Pobieram dane w {len(field_chunks)} porcjach pól (max {len(field_chunks[0])} pól na zapytanie).")

    combined: Dict[str, Dict[str, Any]] = {}
    for idx, chunk in enumerate(field_chunks, 1):
        logging.info(f"Partia {idx}/{len(field_chunks)}: {len(chunk)} pól")
        records = fetch_records_for_fields(access_token, ",".join(chunk))
        for record in records:
            record_id = record.get("id")
            if not record_id:
                continue
            combined.setdefault(record_id, {}).update(record)

    all_records = list(combined.values())
    logging.info(f"Pobrano łącznie {len(all_records)} unikalnych rekordów Marketing Leads")

    if use_cache:
        try:
            with open(cache_file, "wb") as f:
                pickle.dump(all_records, f, protocol=pickle.HIGHEST_PROTOCOL)
            logging.info(f"Zapisano cache: {cache_file}")
        except Exception as e:
            logging.warning(f"Nie udało się zapisać cache ({cache_file}): {e}")

    return all_records


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spłaszcza zagnieżdżone struktury w rekordzie (np. lookup fields).
    Dla dict zamienia na string z ID i nazwą, dla list robi join.
    """
    flattened = {}
    for key, value in record.items():
        if value is None:
            flattened[key] = ""
        elif isinstance(value, dict):
            # Lookup field - wyciągnij id i name
            if "id" in value and "name" in value:
                flattened[key] = f"{value['name']} (ID: {value['id']})"
            elif "name" in value:
                flattened[key] = value["name"]
            elif "id" in value:
                flattened[key] = value["id"]
            else:
                flattened[key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, list):
            # Multi-select lub lista - join przez "; "
            str_items = []
            for item in value:
                if isinstance(item, dict):
                    if "name" in item:
                        str_items.append(item["name"])
                    elif "id" in item:
                        str_items.append(str(item["id"]))
                    else:
                        str_items.append(json.dumps(item, ensure_ascii=False))
                else:
                    str_items.append(str(item))
            flattened[key] = "; ".join(str_items)
        else:
            flattened[key] = value
    return flattened


def save_to_csv(records: List[Dict[str, Any]], output_file: str) -> None:
    """Zapisuje rekordy do pliku CSV."""
    if not records:
        logging.warning("Brak rekordów do zapisania do CSV")
        return
    
    # Spłaszcz rekordy
    flattened_records = [flatten_record(r) for r in records]
    
    # Zbierz wszystkie unikalne klucze (pola)
    all_keys = set()
    for record in flattened_records:
        all_keys.update(record.keys())
    
    fieldnames = sorted(all_keys)
    
    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flattened_records)
    
    logging.info(f"Zapisano {len(records)} rekordów do CSV: {output_file}")


def save_to_xlsx(records: List[Dict[str, Any]], output_file: str) -> None:
    """Zapisuje rekordy do pliku XLSX."""
    if not records:
        logging.warning("Brak rekordów do zapisania do XLSX")
        return
    
    # Spłaszcz rekordy
    flattened_records = [flatten_record(r) for r in records]
    
    # Utwórz DataFrame
    df = pd.DataFrame(flattened_records)
    
    # Zapisz do Excel
    df.to_excel(output_file, index=False, engine="openpyxl")
    
    logging.info(f"Zapisano {len(records)} rekordów do XLSX: {output_file}")


def main() -> None:
    """Główna funkcja skryptu."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pobieranie wszystkich rekordów z modułu Marketing Leads w Zoho CRM"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="wyniki_marketing_leads",
        help="Folder na wyniki (domyślnie: wyniki_marketing_leads)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Wyłącz użycie cache (wymuś pobieranie z API)",
    )
    parser.add_argument(
        "--cache-file",
        type=str,
        default="marketing_leads_cache.pkl",
        help="Ścieżka do pliku cache (domyślnie: marketing_leads_cache.pkl)",
    )
    
    args = parser.parse_args()
    
    logging.info("Rozpoczynam pobieranie Marketing Leads z Zoho CRM...")
    
    # Pobierz token
    access_token = get_access_token()
    
    # Pobierz rekordy
    use_cache = not args.no_cache
    records = fetch_all_marketing_leads(access_token, use_cache=use_cache, cache_file=args.cache_file)
    
    if not records:
        logging.warning("Nie znaleziono żadnych rekordów")
        return
    
    # Utwórz folder na wyniki
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    
    # Wygeneruj timestampa
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Zapisz do CSV
    csv_file = os.path.join(output_dir, f"marketing_leads_{timestamp}.csv")
    save_to_csv(records, csv_file)
    
    # Zapisz do XLSX
    xlsx_file = os.path.join(output_dir, f"marketing_leads_{timestamp}.xlsx")
    save_to_xlsx(records, xlsx_file)
    
    logging.info(f"Zakończono. Pobrano {len(records)} rekordów Marketing Leads")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Przerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
