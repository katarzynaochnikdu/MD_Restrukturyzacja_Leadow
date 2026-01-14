"""
Buduje tabelę cyklu sprzedaży ML -> Lead -> Deal (lifecycle v1).

Każdy wiersz reprezentuje jeden unikalny cykl sprzedażowy z:
- Kluczami/ID (cycle_uid, cycle_id, ml_id, lead_id, deal_id)
- Account info (account_id, account_name)
- Datami start/end etapów
- Dead-time dla niekonwertowanych
- Deal outcome
- Ownerami
- Źródłami (4 tiery)
- Eventami per etap
- Meetingami (tylko Lead)
- Ankietami spotkań
- Czasem w etapach

Wynik: curated_data/lifecycle_2025.csv
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config import RAW_DATA_DIR, CURATED_DATA_DIR, ANALYSIS_YEAR
from source_extractor import extract_source, SourceTiers
from config import DEAL_WON_STAGES, DEAL_LOST_STAGES

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

OUT_PATH = os.path.join(CURATED_DATA_DIR, "lifecycle_2025.csv")

# Ścieżki do eventów z related lists (jeśli dostępne)
EVENTS_INDEX_FILE = os.path.join(RAW_DATA_DIR, "events_by_parent.json")


def load_json(filename: str) -> List[Dict[str, Any]]:
    """Wczytuje JSON z raw_data."""
    path = os.path.join(RAW_DATA_DIR, filename)
    if not os.path.exists(path):
        logger.warning(f"Plik {path} nie istnieje")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def to_dt(value: Any) -> Optional[datetime]:
    """Konwertuje string na datetime (zawsze naive UTC)."""
    if not value:
        return None
    try:
        s = str(value)
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            # Konwertuj na naive (bez timezone) dla spójności
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        else:
            return datetime.strptime(s[:10], "%Y-%m-%d")
    except (ValueError, IndexError):
        return None


def safe_str(value: Any) -> str:
    """Bezpieczna konwersja na string."""
    if value is None:
        return ""
    return str(value)


def extract_id(value: Any) -> Optional[str]:
    """Wyciąga ID z lookup lub wartości."""
    if value is None:
        return None
    if isinstance(value, dict):
        return safe_str(value.get("id")) if value.get("id") else None
    return safe_str(value) if value else None


def extract_name(value: Any) -> Optional[str]:
    """Wyciąga nazwę z lookup lub wartości."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get("name")
    return safe_str(value) if value else None


def first_non_null(*args):
    """Zwraca pierwszą niepustą wartość."""
    for arg in args:
        if arg is not None and arg != "":
            return arg
    return None


def get_owner_name(record: Optional[Dict]) -> str:
    """Wyciąga nazwę ownera z rekordu."""
    if not record:
        return ""
    owner = record.get("Owner")
    if isinstance(owner, dict):
        return owner.get("name", "")
    return safe_str(owner) if owner else ""


def get_deal_produkt(deal: Optional[Dict]) -> str:
    """Wyciąga Produkt z Deal (może być lista) i zwraca jako string."""
    if not deal:
        return ""
    produkt = deal.get("Produkt")
    if produkt is None:
        return ""
    if isinstance(produkt, list):
        return ", ".join(str(p) for p in produkt if p)
    return str(produkt)


def get_account_info(deal: Optional[Dict], lead: Optional[Dict]) -> Tuple[str, str]:
    """
    Wyciąga account_id i account_name z Deal lub Lead.
    Priorytet: Deal.Account_Name > Lead.Converted_Account > Lead.Company
    """
    account_id = ""
    account_name = ""
    
    # 1. Z Deal
    if deal:
        account_ref = deal.get("Account_Name")
        if account_ref:
            account_id = extract_id(account_ref) or ""
            account_name = extract_name(account_ref) or ""
            if account_id:
                return account_id, account_name
    
    # 2. Z Lead - Converted_Account
    if lead:
        converted_account = lead.get("Converted_Account")
        if converted_account:
            account_id = extract_id(converted_account) or ""
            account_name = extract_name(converted_account) or ""
            if account_id:
                return account_id, account_name
        
        # 3. Fallback - Company jako nazwa
        company = lead.get("Company")
        if company:
            account_name = safe_str(company)
    
    return account_id, account_name


def build_events_index(events_raw: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Buduje indeks eventów po record_id.
    Zbiera eventy z różnych pól powiązań (What_Id, Lead_Name, Deal_Name).
    """
    index = defaultdict(list)
    
    for ev in events_raw:
        event_id = ev.get("id")
        if not event_id:
            continue
        
        # What_Id (główne powiązanie)
        what_id = extract_id(ev.get("What_Id"))
        if what_id:
            index[what_id].append(ev)
        
        # Lead_Name (lookup)
        lead_name_id = extract_id(ev.get("Lead_Name"))
        if lead_name_id and lead_name_id != what_id:
            index[lead_name_id].append(ev)
        
        # Deal_Name (lookup)
        deal_name_id = extract_id(ev.get("Deal_Name"))
        if deal_name_id and deal_name_id != what_id:
            index[deal_name_id].append(ev)
        
        # Who_Id (kontakt)
        who_id = extract_id(ev.get("Who_Id"))
        if who_id and who_id not in (what_id, lead_name_id, deal_name_id):
            index[who_id].append(ev)
    
    # Deduplikacja eventów per record
    for record_id in index:
        seen = set()
        unique = []
        for ev in index[record_id]:
            eid = ev.get("id")
            if eid not in seen:
                seen.add(eid)
                unique.append(ev)
        index[record_id] = unique
    
    return dict(index)


def load_related_events_index() -> Optional[Dict[str, Dict[str, List[str]]]]:
    """
    Wczytuje indeks eventów z related lists (jeśli dostępny).
    Format: {"leads": {lead_id: [event_ids]}, "deals": {deal_id: [event_ids]}}
    """
    if not os.path.exists(EVENTS_INDEX_FILE):
        return None
    try:
        with open(EVENTS_INDEX_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def count_events_in_stage(
    events: List[Dict],
    start_time: Optional[datetime],
    end_time: Optional[datetime]
) -> int:
    """
    Liczy eventy w oknie czasowym etapu.
    Event jest w etapie jeśli Start_DateTime >= start_time i < end_time (lub brak end_time).
    """
    if not events or not start_time:
        return 0
    
    count = 0
    for ev in events:
        event_start = to_dt(ev.get("Start_DateTime"))
        if not event_start:
            continue
        
        if event_start >= start_time:
            if end_time is None or event_start < end_time:
                count += 1
    
    return count


def first_meeting(events: List[Dict]) -> Tuple[Optional[datetime], int]:
    """
    Znajduje pierwszy meeting i liczy wszystkie.
    
    Returns:
        (first_meeting_time, meetings_count)
    """
    if not events:
        return None, 0
    
    meeting_times = []
    for ev in events:
        start = to_dt(ev.get("Start_DateTime"))
        if start:
            meeting_times.append(start)
    
    if not meeting_times:
        return None, 0
    
    return min(meeting_times), len(meeting_times)


def build_ankiety_stats(
    ankiety: List[Dict],
    lead_id: Optional[str],
    deal_id: Optional[str]
) -> Dict[str, Any]:
    """
    Oblicza statystyki ankiet dla danego cyklu.
    
    Returns:
        {ankiety_count, ankieta_avg_quality, ankieta_avg_meeting, ankieta_presales_primary}
    """
    result = {
        "ankiety_count": 0,
        "ankieta_avg_quality": None,
        "ankieta_avg_meeting": None,
        "ankieta_presales_primary": "",
    }
    
    if not ankiety:
        return result
    
    # Filtruj ankiety dla tego Lead/Deal
    matching: List[Dict] = []
    for ank in ankiety:
        ank_lead_id = extract_id(ank.get("Lead"))
        ank_deal_id = extract_id(ank.get("Deal"))

        if (lead_id and ank_lead_id == lead_id) or (deal_id and ank_deal_id == deal_id):
            matching.append(ank)

    # Deduplikacja ankiet (po id)
    if matching:
        seen_ids = set()
        unique = []
        for ank in matching:
            aid = ank.get("id")
            if aid and aid in seen_ids:
                continue
            if aid:
                seen_ids.add(aid)
            unique.append(ank)
        matching = unique

    # Reguła biznesowa: na projekt powinna być maksymalnie 1 ankieta.
    # Jeśli są >1, wybierz jedną:
    # - preferuj ankietę z wypełnioną oceną OJP
    # - w ramach preferencji wybierz najnowszą (Modified_Time/Created_Time)
    if len(matching) > 1:
        def _ank_time(a: Dict) -> Optional[datetime]:
            return to_dt(a.get("Modified_Time")) or to_dt(a.get("Created_Time"))

        def _has_ojp(a: Dict) -> bool:
            v = a.get("Ogolna_ocena_jakosci_prospekta")
            try:
                return v is not None and str(v).strip() != "" and float(v) == float(v)
            except Exception:
                return False

        with_ojp = [a for a in matching if _has_ojp(a)]
        candidates = with_ojp if with_ojp else matching
        candidates = sorted(candidates, key=lambda a: (_ank_time(a) is None, _ank_time(a)), reverse=True)
        matching = [candidates[0]]
    
    if not matching:
        return result
    
    result["ankiety_count"] = len(matching)
    
    # Średnie oceny
    quality_scores = []
    meeting_scores = []
    presales_names = []
    
    for ank in matching:
        quality = ank.get("Ogolna_ocena_jakosci_prospekta")
        meeting = ank.get("Ocena_spotkania")
        presales = extract_name(ank.get("PreSales"))
        
        if quality is not None:
            try:
                quality_scores.append(float(quality))
            except (ValueError, TypeError):
                pass
        
        if meeting is not None:
            try:
                meeting_scores.append(float(meeting))
            except (ValueError, TypeError):
                pass
        
        if presales:
            presales_names.append(presales)
    
    if quality_scores:
        result["ankieta_avg_quality"] = round(sum(quality_scores) / len(quality_scores), 2)
    
    if meeting_scores:
        result["ankieta_avg_meeting"] = round(sum(meeting_scores) / len(meeting_scores), 2)
    
    if presales_names:
        # Najczęstszy presales
        from collections import Counter
        result["ankieta_presales_primary"] = Counter(presales_names).most_common(1)[0][0]
    
    return result


def days_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    """Oblicza liczbę dni między datami."""
    if not start or not end:
        return None
    return (end - start).days


def build_lifecycle() -> List[Dict[str, Any]]:
    """
    Buduje tabelę lifecycle łącząc ML -> Lead -> Deal.
    
    Returns:
        Lista wierszy lifecycle
    """
    # Wczytaj dane
    logger.info("Wczytywanie danych RAW...")
    ml_raw = load_json("marketing_leads_raw.json")
    leads_raw = load_json("leads_raw.json")
    deals_raw = load_json("deals_raw.json")
    events_raw = load_json("events_raw.json")
    ankiety_raw = load_json("ankiety_spotkan_raw.json")
    
    logger.info(f"  Marketing Leads: {len(ml_raw)}")
    logger.info(f"  Leads: {len(leads_raw)}")
    logger.info(f"  Deals: {len(deals_raw)}")
    logger.info(f"  Events: {len(events_raw)}")
    logger.info(f"  Ankiety: {len(ankiety_raw)}")
    
    # Buduj mapowania
    logger.info("Budowanie mapowań...")
    
    # ML -> Lead
    ml_to_lead = {}
    for ml in ml_raw:
        ml_id = safe_str(ml.get("id"))
        lead_ref = ml.get("Lead_utworzony")
        if ml_id and lead_ref:
            lead_id = extract_id(lead_ref)
            if lead_id:
                ml_to_lead[ml_id] = lead_id
    
    # ML -> Deal
    ml_to_deal = {}
    for ml in ml_raw:
        ml_id = safe_str(ml.get("id"))
        deal_ref = ml.get("Deal_utworzony")
        if ml_id and deal_ref:
            deal_id = extract_id(deal_ref)
            if deal_id:
                ml_to_deal[ml_id] = deal_id
    
    # Lead -> Deal
    lead_to_deal = {}
    for lead in leads_raw:
        lead_id = safe_str(lead.get("id"))
        deal_ref = lead.get("Deal_ID_after_conversion")
        if lead_id and deal_ref:
            deal_id = extract_id(deal_ref)
            if deal_id:
                lead_to_deal[lead_id] = deal_id
    
    # Indeksy
    ml_by_id = {safe_str(x.get("id")): x for x in ml_raw if x.get("id")}
    lead_by_id = {safe_str(x.get("id")): x for x in leads_raw if x.get("id")}
    deal_by_id = {safe_str(x.get("id")): x for x in deals_raw if x.get("id")}
    
    # Indeks eventów
    logger.info("Budowanie indeksu eventów...")
    events_index = build_events_index(events_raw)
    
    # Sprawdź czy mamy related events index
    related_events_index = load_related_events_index()
    if related_events_index:
        logger.info("Znaleziono indeks related events - będzie używany dla dokładności")
    
    # Buduj cykle
    logger.info("Budowanie cykli lifecycle...")
    chains = []
    seen_root = set()
    
    def get_events_for_record(record_id: str, module: str) -> List[Dict]:
        """Pobiera eventy dla rekordu z najlepszego źródła."""
        # Priorytet: related events > global events index
        if related_events_index:
            module_key = "leads" if module == "Leads" else "deals"
            event_ids = related_events_index.get(module_key, {}).get(record_id, [])
            if event_ids:
                # Odtwórz eventy z global index
                events = []
                for eid in event_ids:
                    for ev in events_raw:
                        if safe_str(ev.get("id")) == eid:
                            events.append(ev)
                            break
                return events
        
        # Fallback do global index
        return events_index.get(record_id, [])
    
    # 1) Start od ML
    for ml in ml_raw:
        ml_id = safe_str(ml.get("id"))
        if not ml_id:
            continue
        
        lead_id = ml_to_lead.get(ml_id)
        deal_id = first_non_null(
            ml_to_deal.get(ml_id),
            lead_to_deal.get(lead_id) if lead_id else None
        )
        
        root_id = first_non_null(deal_id, lead_id, ml_id)
        if not root_id or root_id in seen_root:
            continue
        seen_root.add(root_id)
        
        # Pobierz rekordy
        lead = lead_by_id.get(lead_id) if lead_id else None
        deal = deal_by_id.get(deal_id) if deal_id else None
        
        # Daty
        ml_created = to_dt(ml.get("Created_Time"))
        ml_modified = to_dt(ml.get("Modified_Time"))
        lead_created = to_dt(lead.get("Created_Time")) if lead else None
        lead_modified = to_dt(lead.get("Modified_Time")) if lead else None
        deal_created = to_dt(deal.get("Created_Time")) if deal else None
        deal_closing = to_dt(deal.get("Closing_Date")) if deal else None
        deal_modified = to_dt(deal.get("Modified_Time")) if deal else None
        
        # Stage end times
        ml_stage_end = lead_created if lead_created else ml_modified
        ml_dead_time = ml_modified if not lead_created else None
        lead_stage_end = deal_created if deal_created else lead_modified
        lead_dead_time = lead_modified if (lead and not deal_created) else None
        
        # Eventy
        all_events = []
        if ml_id:
            all_events.extend(events_index.get(ml_id, []))
        if lead_id:
            all_events.extend(get_events_for_record(lead_id, "Leads"))
        if deal_id:
            all_events.extend(get_events_for_record(deal_id, "Deals"))
        
        # Deduplikacja eventów
        seen_events = set()
        unique_events = []
        for ev in all_events:
            eid = ev.get("id")
            if eid and eid not in seen_events:
                seen_events.add(eid)
                unique_events.append(ev)
        
        # Liczenie eventów per etap
        events_lead_stage = count_events_in_stage(unique_events, lead_created, lead_stage_end) if lead_created else 0
        events_deal_stage = count_events_in_stage(unique_events, deal_created, deal_closing or deal_modified) if deal_created else 0
        events_total = len(unique_events)
        
        # Meeting I (tylko dla Lead stage)
        lead_events = get_events_for_record(lead_id, "Leads") if lead_id else []
        first_meeting_time, meetings_count = first_meeting(lead_events) if lead_events else (None, 0)
        lead_meeting1_days = days_between(lead_created, first_meeting_time)
        
        # Account info
        account_id, account_name = get_account_info(deal, lead)
        
        # Ankiety
        ankiety_stats = build_ankiety_stats(ankiety_raw, lead_id, deal_id)
        
        # Source
        source = extract_source(ml, "Marketing_Leads")
        
        chain = {
            # A) Klucze/ID
            "cycle_uid": f"ml:{ml_id}",
            "cycle_id": root_id,
            "ml_id": ml_id,
            "lead_id": lead_id or "",
            "deal_id": deal_id or "",
            
            # B) Account
            "account_id": account_id,
            "account_name": account_name,
            
            # C) Start etapu
            "ml_stage_start_time": ml_created,
            "lead_stage_start_time": lead_created,
            "deal_stage_start_time": deal_created,
            
            # D) End etapu / dead-time
            "ml_stage_end_time": ml_stage_end,
            "ml_dead_time": ml_dead_time,
            "lead_stage_end_time": lead_stage_end,
            "lead_dead_time": lead_dead_time,
            
            # E) Deal outcome
            "deal_stage_current": deal.get("Stage", "") if deal else "",
            "deal_closing_date": deal_closing,
            "deal_modified_time": deal_modified,
            "deal_is_won": (deal.get("Stage") in DEAL_WON_STAGES) if deal else False,
            "deal_is_lost": (deal.get("Stage") in DEAL_LOST_STAGES) if deal else False,
            "deal_pipeline": deal.get("Pipeline", "") if deal else "",
            "deal_produkt": get_deal_produkt(deal),
            "deal_amount": deal.get("Amount") if deal else None,
            
            # F) Ownerzy
            "ml_owner_current": get_owner_name(ml),
            "lead_owner_current": get_owner_name(lead),
            "lead_dla_salesa": lead.get("Dla_salesa", "") if lead else "",
            "deal_owner_current": get_owner_name(deal),
            
            # G) Źródła
            "project_type": "PreSales",  # ML/Lead → Deal
            "source_from": "ML",
            "source_I_tier": source.tier_1,
            "source_II_tier": source.tier_2,
            "source_III_tier": source.tier_3,
            "source_IV_tier": source.tier_4,
            
            # H) Eventy
            "events_count_total": events_total,
            "events_count_lead_stage": events_lead_stage,
            "events_count_deal_stage": events_deal_stage,
            
            # I) Meeting I (Lead)
            "lead_first_meeting_time": first_meeting_time,
            "lead_meetings_count": meetings_count,
            "lead_meeting1_days": lead_meeting1_days,
            
            # J) Ankiety
            **ankiety_stats,
            
            # K) Czasy
            "ml_days_in_stage": days_between(ml_created, ml_stage_end),
            "lead_days_in_stage": days_between(lead_created, lead_stage_end),
            "cycle_to_sales_days": days_between(
                first_non_null(ml_created, lead_created),
                deal_created
            ),
        }
        chains.append(chain)
    
    # 2) Leady bez ML
    for lead in leads_raw:
        lead_id = safe_str(lead.get("id"))
        if not lead_id or lead_id in seen_root:
            continue
        
        deal_id = lead_to_deal.get(lead_id)
        root_id = first_non_null(deal_id, lead_id)
        
        if root_id in seen_root:
            continue
        seen_root.add(root_id)
        
        deal = deal_by_id.get(deal_id) if deal_id else None
        
        # Daty
        lead_created = to_dt(lead.get("Created_Time"))
        lead_modified = to_dt(lead.get("Modified_Time"))
        deal_created = to_dt(deal.get("Created_Time")) if deal else None
        deal_closing = to_dt(deal.get("Closing_Date")) if deal else None
        deal_modified = to_dt(deal.get("Modified_Time")) if deal else None
        
        # Stage end times
        lead_stage_end = deal_created if deal_created else lead_modified
        lead_dead_time = lead_modified if not deal_created else None
        
        # Eventy
        all_events = get_events_for_record(lead_id, "Leads")
        if deal_id:
            all_events.extend(get_events_for_record(deal_id, "Deals"))
        
        # Deduplikacja
        seen_events = set()
        unique_events = []
        for ev in all_events:
            eid = ev.get("id")
            if eid and eid not in seen_events:
                seen_events.add(eid)
                unique_events.append(ev)
        
        events_lead_stage = count_events_in_stage(unique_events, lead_created, lead_stage_end)
        events_deal_stage = count_events_in_stage(unique_events, deal_created, deal_closing or deal_modified) if deal_created else 0
        events_total = len(unique_events)
        
        # Meeting I
        lead_events = get_events_for_record(lead_id, "Leads")
        first_meeting_time, meetings_count = first_meeting(lead_events)
        lead_meeting1_days = days_between(lead_created, first_meeting_time)
        
        # Account info
        account_id, account_name = get_account_info(deal, lead)
        
        # Ankiety
        ankiety_stats = build_ankiety_stats(ankiety_raw, lead_id, deal_id)
        
        # Source
        source = extract_source(lead, "Leads")
        
        chain = {
            "cycle_uid": f"lead:{lead_id}",
            "cycle_id": root_id,
            "ml_id": "",
            "lead_id": lead_id,
            "deal_id": deal_id or "",
            
            "account_id": account_id,
            "account_name": account_name,
            
            "ml_stage_start_time": None,
            "lead_stage_start_time": lead_created,
            "deal_stage_start_time": deal_created,
            
            "ml_stage_end_time": None,
            "ml_dead_time": None,
            "lead_stage_end_time": lead_stage_end,
            "lead_dead_time": lead_dead_time,
            
            "deal_stage_current": deal.get("Stage", "") if deal else "",
            "deal_closing_date": deal_closing,
            "deal_modified_time": deal_modified,
            "deal_is_won": (deal.get("Stage") in DEAL_WON_STAGES) if deal else False,
            "deal_is_lost": (deal.get("Stage") in DEAL_LOST_STAGES) if deal else False,
            "deal_pipeline": deal.get("Pipeline", "") if deal else "",
            "deal_produkt": get_deal_produkt(deal),
            "deal_amount": deal.get("Amount") if deal else None,
            
            "ml_owner_current": "",
            "lead_owner_current": get_owner_name(lead),
            "lead_dla_salesa": lead.get("Dla_salesa", ""),
            "deal_owner_current": get_owner_name(deal),
            
            "project_type": "PreSales",  # Lead → Deal
            "source_from": "Lead",
            "source_I_tier": source.tier_1,
            "source_II_tier": source.tier_2,
            "source_III_tier": source.tier_3,
            "source_IV_tier": source.tier_4,
            
            "events_count_total": events_total,
            "events_count_lead_stage": events_lead_stage,
            "events_count_deal_stage": events_deal_stage,
            
            "lead_first_meeting_time": first_meeting_time,
            "lead_meetings_count": meetings_count,
            "lead_meeting1_days": lead_meeting1_days,
            
            **ankiety_stats,
            
            "ml_days_in_stage": None,
            "lead_days_in_stage": days_between(lead_created, lead_stage_end),
            "cycle_to_sales_days": days_between(lead_created, deal_created),
        }
        chains.append(chain)
    
    # 3) Deale bez ML i Lead
    for deal in deals_raw:
        deal_id = safe_str(deal.get("id"))
        if not deal_id or deal_id in seen_root:
            continue
        
        seen_root.add(deal_id)
        
        # Daty
        deal_created = to_dt(deal.get("Created_Time"))
        deal_closing = to_dt(deal.get("Closing_Date"))
        deal_modified = to_dt(deal.get("Modified_Time"))
        
        # Eventy
        deal_events = get_events_for_record(deal_id, "Deals")
        events_deal_stage = count_events_in_stage(deal_events, deal_created, deal_closing or deal_modified)
        events_total = len(deal_events)
        
        # Account info
        account_id, account_name = get_account_info(deal, None)
        
        # Ankiety
        ankiety_stats = build_ankiety_stats(ankiety_raw, None, deal_id)
        
        # Source
        source = extract_source(deal, "Deals")
        
        chain = {
            "cycle_uid": f"deal:{deal_id}",
            "cycle_id": deal_id,
            "ml_id": "",
            "lead_id": "",
            "deal_id": deal_id,
            
            "account_id": account_id,
            "account_name": account_name,
            
            "ml_stage_start_time": None,
            "lead_stage_start_time": None,
            "deal_stage_start_time": deal_created,
            
            "ml_stage_end_time": None,
            "ml_dead_time": None,
            "lead_stage_end_time": None,
            "lead_dead_time": None,
            
            "deal_stage_current": deal.get("Stage", ""),
            "deal_closing_date": deal_closing,
            "deal_modified_time": deal_modified,
            "deal_is_won": deal.get("Stage") in DEAL_WON_STAGES,
            "deal_is_lost": deal.get("Stage") in DEAL_LOST_STAGES,
            "deal_pipeline": deal.get("Pipeline", ""),
            "deal_produkt": get_deal_produkt(deal),
            "deal_amount": deal.get("Amount"),
            
            "ml_owner_current": "",
            "lead_owner_current": "",
            "lead_dla_salesa": "",
            "deal_owner_current": get_owner_name(deal),
            
            # Deal pochodzi z Lead jeśli ma wskaźnik konwersji lub Lead_provided_by
            "project_type": "PreSales" if (
                deal.get("Lead_ID_before_conversion") or 
                deal.get("Lead_Conversion_Time") or 
                deal.get("Deal_source_as_unit") == "Konwersja" or
                deal.get("Lead_provided_by")
            ) else "Sales only",
            "source_from": "Deal",
            "source_I_tier": source.tier_1,
            "source_II_tier": source.tier_2,
            "source_III_tier": source.tier_3,
            "source_IV_tier": source.tier_4,
            
            "events_count_total": events_total,
            "events_count_lead_stage": 0,
            "events_count_deal_stage": events_deal_stage,
            
            "lead_first_meeting_time": None,
            "lead_meetings_count": 0,
            "lead_meeting1_days": None,
            
            **ankiety_stats,
            
            "ml_days_in_stage": None,
            "lead_days_in_stage": None,
            "cycle_to_sales_days": None,
        }
        chains.append(chain)
    
    return chains


def filter_by_year(chains: List[Dict], year: int) -> List[Dict]:
    """Filtruje cykle do tych z aktywnością w danym roku."""
    filtered = []
    for ch in chains:
        dates = [
            ch.get("ml_stage_start_time"),
            ch.get("lead_stage_start_time"),
            ch.get("deal_stage_start_time"),
        ]
        in_year = any(dt and dt.year == year for dt in dates if isinstance(dt, datetime))
        if in_year:
            filtered.append(ch)
    return filtered


def format_row(chain: Dict) -> Dict:
    """Formatuje wiersz do CSV (datetime -> ISO string)."""
    row = {}
    for key, value in chain.items():
        if isinstance(value, datetime):
            row[key] = value.isoformat()
        else:
            row[key] = value
    return row


def main():
    logger.info("=" * 60)
    logger.info("BUDOWANIE LIFECYCLE v1")
    logger.info("=" * 60)
    
    # Buduj lifecycle
    chains = build_lifecycle()
    logger.info(f"Zbudowano {len(chains)} cykli (wszystkie lata)")
    
    # Filtruj do 2025
    chains_2025 = filter_by_year(chains, ANALYSIS_YEAR)
    logger.info(f"Po filtracji do {ANALYSIS_YEAR}: {len(chains_2025)} cykli")
    
    # Statystyki
    with_ml = sum(1 for ch in chains_2025 if ch.get("ml_id"))
    with_lead = sum(1 for ch in chains_2025 if ch.get("lead_id"))
    with_deal = sum(1 for ch in chains_2025 if ch.get("deal_id"))
    won_deals = sum(1 for ch in chains_2025 if ch.get("deal_is_won"))
    
    logger.info("")
    logger.info("STATYSTYKI:")
    logger.info(f"  Cykli z ML: {with_ml}")
    logger.info(f"  Cykli z Lead: {with_lead}")
    logger.info(f"  Cykli z Deal: {with_deal}")
    logger.info(f"  Wygranych Deals: {won_deals}")
    
    # MLe = ML bez Lead
    mle_only = sum(1 for ch in chains_2025 if ch.get("ml_id") and not ch.get("lead_id"))
    logger.info(f"  MLe (ML bez Lead): {mle_only}")
    
    # Zapisz
    rows = [format_row(ch) for ch in chains_2025]
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
    
    logger.info("")
    logger.info(f"Zapisano {len(rows)} wierszy do {OUT_PATH}")
    
    # Podgląd kolumn
    logger.info("")
    logger.info(f"Kolumny ({len(df.columns)}):")
    for col in df.columns:
        logger.info(f"  - {col}")


if __name__ == "__main__":
    main()
