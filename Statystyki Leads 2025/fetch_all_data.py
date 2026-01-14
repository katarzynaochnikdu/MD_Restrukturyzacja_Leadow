"""
Skrypt do pobierania wszystkich danych potrzebnych do analiz konwersji i ocen.
Pobiera: Marketing_Leads, Leads, Deals, Ankiety_Spotkan, Events.
Zapisuje surowe dane (RAW) jako JSON oraz curated jako CSV.

Użycie:
    python fetch_all_data.py              # normalne pobieranie
    python fetch_all_data.py --refetch    # wymuszenie pełnego odświeżenia z API
    python fetch_all_data.py --validate   # tylko walidacja istniejących dumpów
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    def tqdm(iterable, **kwargs):
        return iterable

from config import RAW_DATA_DIR, CURATED_DATA_DIR, ANALYSIS_YEAR
from zoho_api_client import (
    get_access_token,
    fetch_all_records,
    fetch_records_chunked_fields,
    fetch_related_list,
    flatten_record,
)

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fetch_all_data.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Plik z metrykami pobrania
FETCH_METRICS_FILE = os.path.join(RAW_DATA_DIR, "fetch_metrics.csv")


# ============================================================================
# Definicje pól dla modułów
# ============================================================================

MARKETING_LEADS_FIELDS = [
    "id", "Name", "Owner", "Created_Time", "Modified_Time",
    "Imie", "Nazwisko", "Email", "Firma", "Telefon_komorkowy", "Telefon_stacjonarny",
    "Lead_Source", "Outbound_Inbound", "Zrodlo_inbound",
    "Strona_internetowa_medidesk", "Facebook_source", "Webinar", "Webinar_medidesk",
    "Konferencja", "Konferencja_medidesk", "Konferencja_zewnetrzna",
    "Polecenie", "Polecenie_firma", "Polecenie_partner", "Polecenie_pracownik",
    "Etap_kwalifikacji_HL", "Hot_Lead", "Data_Hot_Lead", "Forma_Hot_Lead",
    "Lead_utworzony", "Deal_utworzony",
    "Firma_w_bazie", "Kontakt_w_bazie",
    "Kto_zaopiekowal_sie_namiarem", "Komu_przekazujemy",
    "utm_campaign", "utm_medium", "utm_source",
    "Created_By", "Modified_By", "Layout",
]

LEADS_FIELDS = [
    "id", "Full_Name", "First_Name", "Last_Name", "Owner", "Created_Time", "Modified_Time",
    "Email", "Company", "Phone", "Mobile",
    "Lead_Status", "Lead_Source",
    "Zrodlo_inbound", "Zrodlo_outbound",
    "Strona_internetowa_medidesk", "Facebook_source",
    "Webinar", "Webinar_medidesk", "Webinar_zewnetrzny",
    "Konferencja", "Konferencja_medidesk", "Konferencja_zewnetrzna",
    "Polecenie", "Partner_polecajacy", "Firma_polecajaca", "Pracownik_polecajacy",
    "Firma_w_bazie", "Kontakt_w_bazie",
    "Data_konwersji_z_Leada", "Deal_ID_after_conversion",
    "USER_created_at",
    "Created_By", "Modified_By", "Layout",
]

DEALS_FIELDS = [
    "id", "Deal_Name", "Owner", "Created_Time", "Modified_Time",
    "Stage", "Pipeline", "Amount", "Closing_Date",
    "Account_Name", "Contact_Name",
    "Lead_Source", "Outbound_Inbound",
    "Zrodlo_inbound", "Zrodlo_outbound",
    "Strona_internetowa_medidesk", "Facebook_source",
    "Webinar", "Webinar_medidesk",
    "Konferencja", "Konferencja_medidesk", "Konferencja_zewnetrzna",
    "Polecenie", "Partner_polecajacy", "Firma_polecajaca",
    "Hot_Lead", "Data_Hot_Lead",
    "Deal_source_as_unit",
    "Produkt", "Produkty_string",  # do filtrowania
    # Pola konwersji z Lead
    "Lead_ID_before_conversion", "Lead_Conversion_Time", "Lead_URL_before_conversion",
    "Unique_Lead",  # True = Deal ma powiązanego Leada
    "Lead_provided_by",  # Kto dostarczył Lead
    "Created_By", "Modified_By", "Layout",
]

ANKIETY_SPOTKAN_FIELDS = [
    "id", "Name", "Owner", "Created_Time", "Modified_Time",
    "Lead", "Deal", "Firma",
    "Data_spotkania", "PreSales", "Host",
    "Outbound_Inbound",
    "II_poziom_zrodla", "III_poziom_zrodla", "IV_poziom_zrodla",
    "Ogolna_ocena_jakosci_prospekta", "Ocena_spotkania",
    "Konwersja", "Konwersja_num",
    "Czy_Klient_rozumia_cel_spotkania", "Czy_Klient_rozumia_cel_spotkania1",
    "Czy_firma_spe_nia_kryteria_zgodno_ci_z_ICP1", "Czy_firma_klienta_spe_nia_kryteria_zgodno_ci_z_ICP",
    "Czy_klient_posiada_bud_et_na_zakup_od_medidesk1", "Czy_klient_posiada_bud_et",
    "Czy_na_spotkaniu_byla_osoba_decyzyjna", "osoba_decyzyjna",
    "Czy_zidentyfikowano_realny_problem1", "Czy_na_spotkaniu_zidentyfikowano_realny_problem",
    "Czy_problem_klienta_jest_wystarczaj_co_pilny1", "Czy_problem_Klienta_jest_pilny",
    "Czy_ustalono_kolejne_kroki_po_spotkaniu1", "Czy_ustalono_kolejne_kroki_po_spotkaniu",
    "Czy_znamy_termin_decyzji_zakupowej1", "Czy_znamy_termin_decyzji_zakupowej",
    "Czy_spotkanie_by_o_z_TTP", "Czy_spotkanie_by_o_z_TTP1",
    "Produkt", "Problem_klienta",
    "Created_By", "Modified_By",
]

EVENTS_FIELDS = [
    "id", "Event_Title", "Subject", "Owner", "Created_Time", "Modified_Time",
    "Start_DateTime", "End_DateTime",
    "What_Id", "Who_Id",  # powiązania
    "se_module",  # typ modułu powiązanego
]


# ============================================================================
# Funkcje pomocnicze
# ============================================================================

def ensure_dirs():
    """Tworzy katalogi na dane jeśli nie istnieją."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(CURATED_DATA_DIR, exist_ok=True)


def save_raw_json(data: List[Dict], filename: str):
    """Zapisuje surowe dane jako JSON."""
    filepath = os.path.join(RAW_DATA_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Zapisano RAW: {filepath} ({len(data)} rekordów)")


def load_raw_json(filename: str) -> List[Dict]:
    """Wczytuje surowe dane z JSON (do walidacji)."""
    filepath = os.path.join(RAW_DATA_DIR, filename)
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_fetch_metrics(metrics: List[Dict]):
    """Zapisuje metryki pobrania do CSV."""
    if not metrics:
        return
    
    fieldnames = ["module", "filename", "records_count", "pages", "fetched_at", "duration_s"]
    file_exists = os.path.exists(FETCH_METRICS_FILE)
    
    with open(FETCH_METRICS_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(metrics)
    logger.info(f"Zapisano metryki pobrania do {FETCH_METRICS_FILE}")


def validate_dump(filename: str, module_name: str) -> Tuple[bool, str]:
    """
    Waliduje spójność dumpu.
    
    Returns:
        (is_valid, message)
    """
    filepath = os.path.join(RAW_DATA_DIR, filename)
    if not os.path.exists(filepath):
        return False, f"Plik {filename} nie istnieje"
    
    try:
        data = load_raw_json(filename)
    except json.JSONDecodeError as e:
        return False, f"Błąd parsowania JSON: {e}"
    
    if not data:
        return False, f"Plik {filename} jest pusty"
    
    # Sprawdź czy wszystkie rekordy mają ID
    records_without_id = sum(1 for r in data if not r.get("id"))
    if records_without_id > 0:
        return False, f"{records_without_id} rekordów bez ID w {filename}"
    
    # Sprawdź unikalność ID
    ids = [r.get("id") for r in data]
    unique_ids = set(ids)
    if len(ids) != len(unique_ids):
        duplicates = len(ids) - len(unique_ids)
        return False, f"{duplicates} duplikatów ID w {filename}"
    
    # Sprawdź daty (czy są sensowne)
    date_field = "Created_Time"
    dates = []
    for r in data:
        dt_str = r.get(date_field)
        if dt_str:
            try:
                year = int(dt_str[:4])
                dates.append(year)
            except (ValueError, IndexError):
                pass
    
    if dates:
        min_year = min(dates)
        max_year = max(dates)
        if min_year < 2015 or max_year > 2030:
            logger.warning(f"Podejrzane lata w {filename}: {min_year}-{max_year}")
    
    return True, f"OK: {len(data)} rekordów, daty {min(dates) if dates else 'brak'}-{max(dates) if dates else 'brak'}"


def validate_all_dumps() -> bool:
    """Waliduje wszystkie dumpy. Zwraca True jeśli wszystkie OK."""
    files_to_validate = [
        ("marketing_leads_raw.json", "Marketing_Leads"),
        ("leads_raw.json", "Leads"),
        ("deals_raw.json", "Deals"),
        ("ankiety_spotkan_raw.json", "Ankiety_Spotkan"),
        ("events_raw.json", "Events"),
    ]
    
    all_valid = True
    logger.info("=== WALIDACJA DUMPÓW ===")
    for filename, module in files_to_validate:
        is_valid, msg = validate_dump(filename, module)
        status = "OK" if is_valid else "BŁĄD"
        logger.info(f"  [{status}] {module}: {msg}")
        if not is_valid:
            all_valid = False
    
    return all_valid


def save_curated_csv(data: List[Dict], filename: str):
    """Zapisuje spłaszczone dane jako CSV."""
    if not data:
        logger.warning(f"Brak danych do zapisu: {filename}")
        return

    flattened = [flatten_record(r) for r in data]

    # Zbierz wszystkie klucze
    all_keys = set()
    for record in flattened:
        all_keys.update(record.keys())
    fieldnames = sorted(all_keys)

    filepath = os.path.join(CURATED_DATA_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flattened)
    logger.info(f"Zapisano CURATED: {filepath} ({len(flattened)} rekordów)")


def filter_by_year(records: List[Dict], year: int, date_field: str = "Created_Time") -> List[Dict]:
    """Filtruje rekordy po roku na podstawie pola daty."""
    filtered = []
    for r in records:
        date_val = r.get(date_field)
        if date_val:
            try:
                if isinstance(date_val, str):
                    # Format: "2025-01-15T10:30:00+01:00" lub "2025-01-15"
                    year_part = int(date_val[:4])
                    if year_part == year:
                        filtered.append(r)
            except (ValueError, IndexError):
                pass
    return filtered


# ============================================================================
# Główne funkcje pobierania
# ============================================================================

def fetch_marketing_leads(token: str) -> Tuple[List[Dict], Dict]:
    """Pobiera Marketing Leads. Zwraca (records, metrics)."""
    logger.info("=== Pobieranie Marketing_Leads ===")
    start_time = datetime.now()
    records = fetch_records_chunked_fields("Marketing_Leads", MARKETING_LEADS_FIELDS, token)
    duration = (datetime.now() - start_time).total_seconds()
    
    save_raw_json(records, "marketing_leads_raw.json")
    
    # Filtruj rok 2025
    records_2025 = filter_by_year(records, ANALYSIS_YEAR)
    save_curated_csv(records_2025, "marketing_leads_2025.csv")
    logger.info(f"Marketing Leads 2025: {len(records_2025)} z {len(records)} wszystkich")
    
    metrics = {
        "module": "Marketing_Leads",
        "filename": "marketing_leads_raw.json",
        "records_count": len(records),
        "pages": (len(records) // 200) + 1,
        "fetched_at": datetime.now().isoformat(),
        "duration_s": round(duration, 1),
    }
    return records, metrics


def fetch_leads(token: str) -> Tuple[List[Dict], Dict]:
    """Pobiera Leads. Zwraca (records, metrics)."""
    logger.info("=== Pobieranie Leads ===")
    start_time = datetime.now()
    records = fetch_records_chunked_fields("Leads", LEADS_FIELDS, token)
    duration = (datetime.now() - start_time).total_seconds()
    
    save_raw_json(records, "leads_raw.json")
    
    # Filtruj rok 2025
    records_2025 = filter_by_year(records, ANALYSIS_YEAR)
    save_curated_csv(records_2025, "leads_2025.csv")
    logger.info(f"Leads 2025: {len(records_2025)} z {len(records)} wszystkich")
    
    metrics = {
        "module": "Leads",
        "filename": "leads_raw.json",
        "records_count": len(records),
        "pages": (len(records) // 200) + 1,
        "fetched_at": datetime.now().isoformat(),
        "duration_s": round(duration, 1),
    }
    return records, metrics


def fetch_deals(token: str) -> Tuple[List[Dict], Dict]:
    """Pobiera Deals. Zwraca (records, metrics)."""
    logger.info("=== Pobieranie Deals ===")
    start_time = datetime.now()
    records = fetch_records_chunked_fields("Deals", DEALS_FIELDS, token)
    duration = (datetime.now() - start_time).total_seconds()
    
    save_raw_json(records, "deals_raw.json")
    
    # Filtruj rok 2025
    records_2025 = filter_by_year(records, ANALYSIS_YEAR)
    save_curated_csv(records_2025, "deals_2025.csv")
    logger.info(f"Deals 2025: {len(records_2025)} z {len(records)} wszystkich")
    
    metrics = {
        "module": "Deals",
        "filename": "deals_raw.json",
        "records_count": len(records),
        "pages": (len(records) // 200) + 1,
        "fetched_at": datetime.now().isoformat(),
        "duration_s": round(duration, 1),
    }
    return records, metrics


def fetch_ankiety_spotkan(token: str) -> Tuple[List[Dict], Dict]:
    """Pobiera Ankiety Spotkań. Zwraca (records, metrics)."""
    logger.info("=== Pobieranie Ankiety_Spotkan ===")
    start_time = datetime.now()
    records = fetch_all_records("Ankiety_Spotkan", fields=ANKIETY_SPOTKAN_FIELDS, access_token=token)
    duration = (datetime.now() - start_time).total_seconds()
    
    save_raw_json(records, "ankiety_spotkan_raw.json")
    
    # Filtruj rok 2025
    records_2025 = filter_by_year(records, ANALYSIS_YEAR)
    save_curated_csv(records_2025, "ankiety_spotkan_2025.csv")
    logger.info(f"Ankiety Spotkań 2025: {len(records_2025)} z {len(records)} wszystkich")
    
    metrics = {
        "module": "Ankiety_Spotkan",
        "filename": "ankiety_spotkan_raw.json",
        "records_count": len(records),
        "pages": (len(records) // 200) + 1,
        "fetched_at": datetime.now().isoformat(),
        "duration_s": round(duration, 1),
    }
    return records, metrics


def fetch_events(token: str) -> Tuple[List[Dict], Dict]:
    """Pobiera Events (spotkania). Zwraca (records, metrics)."""
    logger.info("=== Pobieranie Events ===")
    start_time = datetime.now()
    records = fetch_all_records("Events", fields=EVENTS_FIELDS, access_token=token)
    duration = (datetime.now() - start_time).total_seconds()
    
    save_raw_json(records, "events_raw.json")
    
    # Filtruj rok 2025
    records_2025 = filter_by_year(records, ANALYSIS_YEAR)
    save_curated_csv(records_2025, "events_2025.csv")
    logger.info(f"Events 2025: {len(records_2025)} z {len(records)} wszystkich")
    
    metrics = {
        "module": "Events",
        "filename": "events_raw.json",
        "records_count": len(records),
        "pages": (len(records) // 200) + 1,
        "fetched_at": datetime.now().isoformat(),
        "duration_s": round(duration, 1),
    }
    return records, metrics


def fetch_events_for_leads_deals(token: str, leads: List[Dict], deals: List[Dict]) -> List[Dict]:
    """
    Pobiera Events powiązane z Leads i Deals (related lists).
    Zwraca listę eventów z informacją o parent (Lead/Deal).
    """
    logger.info("=== Pobieranie Events dla Leads i Deals (related lists) ===")
    all_events = []
    
    # Events dla Leads
    lead_ids = [r.get("id") for r in leads if r.get("id")]
    logger.info(f"Pobieranie Events dla {len(lead_ids)} Leads...")
    for lead_id in tqdm(lead_ids, desc="Leads Events", disable=not HAS_TQDM):
        events = fetch_related_list("Leads", lead_id, "Events", access_token=token)
        events_history = fetch_related_list("Leads", lead_id, "Events_History", access_token=token)
        for e in events + events_history:
            e["_parent_type"] = "Leads"
            e["_parent_id"] = lead_id
            all_events.append(e)
    
    # Events dla Deals
    deal_ids = [r.get("id") for r in deals if r.get("id")]
    logger.info(f"Pobieranie Events dla {len(deal_ids)} Deals...")
    for deal_id in tqdm(deal_ids, desc="Deals Events", disable=not HAS_TQDM):
        events = fetch_related_list("Deals", deal_id, "Events", access_token=token)
        events_history = fetch_related_list("Deals", deal_id, "Events_History", access_token=token)
        for e in events + events_history:
            e["_parent_type"] = "Deals"
            e["_parent_id"] = deal_id
            all_events.append(e)
    
    save_raw_json(all_events, "events_related_raw.json")
    save_curated_csv(all_events, "events_related.csv")
    logger.info(f"Pobrano {len(all_events)} powiązanych Events")
    return all_events


# ============================================================================
# Main
# ============================================================================

def parse_args():
    """Parsuje argumenty linii poleceń."""
    parser = argparse.ArgumentParser(description="Pobieranie danych z Zoho CRM")
    parser.add_argument(
        "--refetch",
        action="store_true",
        help="Wymuś pełne odświeżenie z API (bez cache)"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Tylko walidacja istniejących dumpów (bez pobierania)"
    )
    parser.add_argument(
        "--with-related-events",
        action="store_true",
        help="Pobierz również Events powiązane z Leads/Deals (wolne!)"
    )
    return parser.parse_args()


def main():
    """Główna funkcja pobierania danych."""
    args = parse_args()
    
    logger.info("=" * 60)
    if args.validate:
        logger.info("TRYB WALIDACJI DUMPÓW")
    elif args.refetch:
        logger.info("TRYB RE-FETCH: PEŁNE ODŚWIEŻENIE Z API")
    else:
        logger.info("ROZPOCZYNAM POBIERANIE DANYCH Z ZOHO CRM")
    logger.info("=" * 60)
    
    ensure_dirs()
    
    # Tryb tylko walidacja
    if args.validate:
        all_valid = validate_all_dumps()
        if all_valid:
            logger.info("Wszystkie dumpy są poprawne.")
            sys.exit(0)
        else:
            logger.error("Niektóre dumpy są niepoprawne!")
            sys.exit(1)
    
    # Pobierz token
    logger.info("Pobieranie tokena...")
    token = get_access_token()
    logger.info("Token uzyskany")
    
    # Zbieranie metryk
    all_metrics = []
    
    # Lista modułów do pobrania
    modules = [
        ("Marketing_Leads", fetch_marketing_leads),
        ("Leads", fetch_leads),
        ("Deals", fetch_deals),
        ("Ankiety_Spotkan", fetch_ankiety_spotkan),
        ("Events", fetch_events),
    ]
    
    # Pobierz wszystkie moduły z progress bar
    results = {}
    if HAS_TQDM:
        print("\n")  # nowa linia dla tqdm
    
    for module_name, fetch_func in tqdm(modules, desc="Moduły", disable=not HAS_TQDM):
        logger.info(f"\n>>> Pobieranie {module_name}...")
        records, metrics = fetch_func(token)
        results[module_name] = records
        all_metrics.append(metrics)
    
    marketing_leads = results.get("Marketing_Leads", [])
    leads = results.get("Leads", [])
    deals = results.get("Deals", [])
    ankiety = results.get("Ankiety_Spotkan", [])
    events = results.get("Events", [])
    
    # Zapisz metryki
    save_fetch_metrics(all_metrics)
    
    # Walidacja po pobraniu
    logger.info("")
    all_valid = validate_all_dumps()
    
    # Opcjonalnie: Pobierz Events powiązane z Leads/Deals
    if args.with_related_events:
        leads_2025 = filter_by_year(leads, ANALYSIS_YEAR)
        deals_2025 = filter_by_year(deals, ANALYSIS_YEAR)
        events_related = fetch_events_for_leads_deals(token, leads_2025, deals_2025)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("POBIERANIE ZAKOŃCZONE")
    logger.info("=" * 60)
    logger.info(f"RAW dane zapisane w: {RAW_DATA_DIR}/")
    logger.info(f"CURATED dane zapisane w: {CURATED_DATA_DIR}/")
    
    # Podsumowanie
    logger.info("")
    logger.info("PODSUMOWANIE POBRANIA:")
    for m in all_metrics:
        logger.info(f"  {m['module']}: {m['records_count']} rekordów w {m['duration_s']}s")
    
    if not all_valid:
        logger.warning("UWAGA: Niektóre dumpy mają problemy z walidacją!")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Przerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
