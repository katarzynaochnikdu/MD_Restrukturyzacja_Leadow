"""
Pobieranie Events powiązanych z Leads i Deals przez Related Lists API.

Ten skrypt pobiera eventy dokładnie z related lists, co daje pewność
że mamy wszystkie eventy przypisane do danego Lead/Deal.

Użycie:
    python fetch_related_events.py           # pobierz dla wszystkich Leads/Deals z 2025
    python fetch_related_events.py --all     # pobierz dla WSZYSTKICH Leads/Deals (wolne!)
    python fetch_related_events.py --validate  # sprawdź spójność cache
"""

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    def tqdm(iterable, **kwargs):
        return iterable

from config import RAW_DATA_DIR, CURATED_DATA_DIR, ANALYSIS_YEAR
from zoho_api_client import get_access_token, fetch_related_list

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("fetch_related_events.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# Ścieżki cache
CACHE_DIR = os.path.join(RAW_DATA_DIR, "related_events_cache")
LEADS_EVENTS_CACHE = os.path.join(CACHE_DIR, "leads_events.json")
DEALS_EVENTS_CACHE = os.path.join(CACHE_DIR, "deals_events.json")
EVENTS_DEDUP_FILE = os.path.join(RAW_DATA_DIR, "events_related_dedup.json")
EVENTS_INDEX_FILE = os.path.join(RAW_DATA_DIR, "events_by_parent.json")


def ensure_cache_dir():
    """Tworzy katalog cache."""
    os.makedirs(CACHE_DIR, exist_ok=True)


def load_json(filepath: str) -> Any:
    """Wczytuje JSON z pliku."""
    if not os.path.exists(filepath):
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(data: Any, filepath: str):
    """Zapisuje dane do JSON."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_raw_records(filename: str) -> List[Dict]:
    """Wczytuje surowe rekordy z raw_data."""
    filepath = os.path.join(RAW_DATA_DIR, filename)
    if not os.path.exists(filepath):
        logger.warning(f"Plik {filepath} nie istnieje")
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_by_year(records: List[Dict], year: int, date_field: str = "Created_Time") -> List[Dict]:
    """Filtruje rekordy po roku."""
    filtered = []
    for r in records:
        date_val = r.get(date_field)
        if date_val:
            try:
                year_part = int(str(date_val)[:4])
                if year_part == year:
                    filtered.append(r)
            except (ValueError, IndexError):
                pass
    return filtered


def load_cache() -> Tuple[Dict[str, List[Dict]], Dict[str, List[Dict]]]:
    """
    Wczytuje cache eventów dla Leads i Deals.
    
    Returns:
        (leads_events, deals_events) - słowniki {record_id: [events]}
    """
    leads_events = load_json(LEADS_EVENTS_CACHE) or {}
    deals_events = load_json(DEALS_EVENTS_CACHE) or {}
    return leads_events, deals_events


def save_cache(leads_events: Dict[str, List[Dict]], deals_events: Dict[str, List[Dict]]):
    """Zapisuje cache eventów."""
    save_json(leads_events, LEADS_EVENTS_CACHE)
    save_json(deals_events, DEALS_EVENTS_CACHE)
    logger.info(f"Zapisano cache: {len(leads_events)} leads, {len(deals_events)} deals")


def fetch_events_for_record(
    module: str,
    record_id: str,
    token: str,
    include_history: bool = True
) -> List[Dict]:
    """
    Pobiera wszystkie Events dla danego rekordu.
    
    Args:
        module: "Leads" lub "Deals"
        record_id: ID rekordu
        token: Token dostępu
        include_history: Czy pobierać też Events_History
    
    Returns:
        Lista eventów z metadanymi o parent
    """
    all_events = []
    
    # Events
    events = fetch_related_list(module, record_id, "Events", access_token=token)
    for e in events:
        e["_parent_module"] = module
        e["_parent_id"] = record_id
        e["_source"] = "Events"
        all_events.append(e)
    
    # Events_History (archiwalne)
    if include_history:
        events_history = fetch_related_list(module, record_id, "Events_History", access_token=token)
        for e in events_history:
            e["_parent_module"] = module
            e["_parent_id"] = record_id
            e["_source"] = "Events_History"
            all_events.append(e)
    
    return all_events


def fetch_all_related_events(
    leads: List[Dict],
    deals: List[Dict],
    token: str,
    use_cache: bool = True
) -> Tuple[Dict[str, List[Dict]], Dict[str, List[Dict]]]:
    """
    Pobiera Events dla wszystkich Leads i Deals.
    
    Args:
        leads: Lista rekordów Leads
        deals: Lista rekordów Deals
        token: Token dostępu
        use_cache: Czy używać cache (pomija już pobrane)
    
    Returns:
        (leads_events, deals_events)
    """
    # Wczytaj cache
    leads_events, deals_events = load_cache() if use_cache else ({}, {})
    
    # Pobierz dla Leads
    lead_ids = [str(r.get("id")) for r in leads if r.get("id")]
    leads_to_fetch = [lid for lid in lead_ids if lid not in leads_events]
    
    logger.info(f"Leads do pobrania: {len(leads_to_fetch)} (z cache: {len(lead_ids) - len(leads_to_fetch)})")
    
    for i, lead_id in enumerate(tqdm(leads_to_fetch, desc="Leads Events", disable=not HAS_TQDM)):
        if i % 50 == 0 and i > 0:
            # Zapisuj cache co 50 rekordów
            save_cache(leads_events, deals_events)
        
        events = fetch_events_for_record("Leads", lead_id, token)
        leads_events[lead_id] = events
    
    # Pobierz dla Deals
    deal_ids = [str(r.get("id")) for r in deals if r.get("id")]
    deals_to_fetch = [did for did in deal_ids if did not in deals_events]
    
    logger.info(f"Deals do pobrania: {len(deals_to_fetch)} (z cache: {len(deal_ids) - len(deals_to_fetch)})")
    
    for i, deal_id in enumerate(tqdm(deals_to_fetch, desc="Deals Events", disable=not HAS_TQDM)):
        if i % 50 == 0 and i > 0:
            save_cache(leads_events, deals_events)
        
        events = fetch_events_for_record("Deals", deal_id, token)
        deals_events[deal_id] = events
    
    # Zapisz końcowy cache
    save_cache(leads_events, deals_events)
    
    return leads_events, deals_events


def deduplicate_events(
    leads_events: Dict[str, List[Dict]],
    deals_events: Dict[str, List[Dict]]
) -> Tuple[List[Dict], Dict[str, List[str]]]:
    """
    Deduplikuje eventy i buduje indeks event_id -> parent_ids.
    
    Returns:
        (unique_events, index) gdzie index = {event_id: [parent_ids]}
    """
    events_by_id: Dict[str, Dict] = {}
    event_parents: Dict[str, Set[str]] = defaultdict(set)
    
    # Zbierz z Leads
    for lead_id, events in leads_events.items():
        for e in events:
            event_id = e.get("id")
            if not event_id:
                continue
            event_id = str(event_id)
            events_by_id[event_id] = e
            event_parents[event_id].add(f"Leads:{lead_id}")
    
    # Zbierz z Deals
    for deal_id, events in deals_events.items():
        for e in events:
            event_id = e.get("id")
            if not event_id:
                continue
            event_id = str(event_id)
            events_by_id[event_id] = e
            event_parents[event_id].add(f"Deals:{deal_id}")
    
    # Konwertuj set na list dla JSON
    index = {eid: list(parents) for eid, parents in event_parents.items()}
    unique_events = list(events_by_id.values())
    
    return unique_events, index


def build_parent_index(
    leads_events: Dict[str, List[Dict]],
    deals_events: Dict[str, List[Dict]]
) -> Dict[str, Dict[str, List[str]]]:
    """
    Buduje indeks parent_id -> [event_ids].
    
    Returns:
        {"leads": {lead_id: [event_ids]}, "deals": {deal_id: [event_ids]}}
    """
    index = {"leads": {}, "deals": {}}
    
    for lead_id, events in leads_events.items():
        event_ids = [str(e.get("id")) for e in events if e.get("id")]
        if event_ids:
            index["leads"][lead_id] = event_ids
    
    for deal_id, events in deals_events.items():
        event_ids = [str(e.get("id")) for e in events if e.get("id")]
        if event_ids:
            index["deals"][deal_id] = event_ids
    
    return index


def validate_cache() -> bool:
    """Sprawdza spójność cache."""
    logger.info("=== WALIDACJA CACHE EVENTÓW ===")
    
    leads_events, deals_events = load_cache()
    
    # Sprawdź czy cache istnieje
    if not leads_events and not deals_events:
        logger.warning("Cache jest pusty")
        return False
    
    # Statystyki
    total_leads = len(leads_events)
    total_deals = len(deals_events)
    
    leads_with_events = sum(1 for events in leads_events.values() if events)
    deals_with_events = sum(1 for events in deals_events.values() if events)
    
    total_events_leads = sum(len(events) for events in leads_events.values())
    total_events_deals = sum(len(events) for events in deals_events.values())
    
    logger.info(f"  Leads w cache: {total_leads} ({leads_with_events} z eventami, łącznie {total_events_leads} eventów)")
    logger.info(f"  Deals w cache: {total_deals} ({deals_with_events} z eventami, łącznie {total_events_deals} eventów)")
    
    # Sprawdź duplikaty w deduplikacji
    unique_events, index = deduplicate_events(leads_events, deals_events)
    logger.info(f"  Unikalne eventy: {len(unique_events)}")
    
    # Eventy z wieloma parentami
    multi_parent = sum(1 for parents in index.values() if len(parents) > 1)
    logger.info(f"  Eventy z >1 parent: {multi_parent}")
    
    return True


def parse_args():
    """Parsuje argumenty."""
    parser = argparse.ArgumentParser(description="Pobieranie Related Events z Zoho")
    parser.add_argument("--all", action="store_true", help="Pobierz dla WSZYSTKICH rekordów (nie tylko 2025)")
    parser.add_argument("--validate", action="store_true", help="Tylko walidacja cache")
    parser.add_argument("--no-cache", action="store_true", help="Ignoruj cache, pobierz wszystko od nowa")
    return parser.parse_args()


def main():
    args = parse_args()
    
    ensure_cache_dir()
    
    logger.info("=" * 60)
    logger.info("POBIERANIE RELATED EVENTS DLA LEADS I DEALS")
    logger.info("=" * 60)
    
    # Tryb walidacji
    if args.validate:
        validate_cache()
        return
    
    # Wczytaj rekordy
    logger.info("Wczytywanie Leads i Deals...")
    leads_all = load_raw_records("leads_raw.json")
    deals_all = load_raw_records("deals_raw.json")
    
    if not leads_all and not deals_all:
        logger.error("Brak danych w raw_data/. Uruchom najpierw fetch_all_data.py")
        sys.exit(1)
    
    # Filtruj do 2025 jeśli nie --all
    if args.all:
        leads = leads_all
        deals = deals_all
        logger.info(f"Tryb --all: przetwarzam wszystkie rekordy")
    else:
        leads = filter_by_year(leads_all, ANALYSIS_YEAR)
        deals = filter_by_year(deals_all, ANALYSIS_YEAR)
        logger.info(f"Filtrowanie do {ANALYSIS_YEAR}: {len(leads)} Leads, {len(deals)} Deals")
    
    # Pobierz token
    logger.info("Pobieranie tokena...")
    token = get_access_token()
    
    # Pobierz eventy
    use_cache = not args.no_cache
    leads_events, deals_events = fetch_all_related_events(leads, deals, token, use_cache)
    
    # Deduplikacja i indeksowanie
    logger.info("Deduplikacja i budowanie indeksów...")
    unique_events, event_index = deduplicate_events(leads_events, deals_events)
    parent_index = build_parent_index(leads_events, deals_events)
    
    # Zapisz wyniki
    save_json(unique_events, EVENTS_DEDUP_FILE)
    save_json(parent_index, EVENTS_INDEX_FILE)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("ZAKOŃCZONO")
    logger.info("=" * 60)
    logger.info(f"Unikalne eventy: {len(unique_events)}")
    logger.info(f"Leads z eventami: {len(parent_index['leads'])}")
    logger.info(f"Deals z eventami: {len(parent_index['deals'])}")
    logger.info(f"Zapisano: {EVENTS_DEDUP_FILE}")
    logger.info(f"Zapisano: {EVENTS_INDEX_FILE}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Przerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Błąd: {e}", exc_info=True)
        sys.exit(1)
