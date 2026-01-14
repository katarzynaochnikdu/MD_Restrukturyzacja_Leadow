"""
Walidacja kompletności i spójności dumpów z Zoho.

Sprawdza:
- Czy wszystkie pliki RAW istnieją
- Czy rekordy mają unikalne ID
- Czy daty są sensowne
- Porównanie liczności z poprzednim pobraniem (jeśli dostępne)

Użycie:
    python validate_dumps.py
"""

import csv
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from config import RAW_DATA_DIR, CURATED_DATA_DIR, ANALYSIS_YEAR

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Pliki do walidacji
FILES_TO_VALIDATE = [
    ("marketing_leads_raw.json", "Marketing_Leads"),
    ("leads_raw.json", "Leads"),
    ("deals_raw.json", "Deals"),
    ("ankiety_spotkan_raw.json", "Ankiety_Spotkan"),
    ("events_raw.json", "Events"),
]

FETCH_METRICS_FILE = os.path.join(RAW_DATA_DIR, "fetch_metrics.csv")


def load_json(filename: str) -> Optional[List[Dict]]:
    """Wczytuje JSON z raw_data."""
    filepath = os.path.join(RAW_DATA_DIR, filename)
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return None


def get_file_mtime(filename: str) -> Optional[datetime]:
    """Zwraca datę modyfikacji pliku."""
    filepath = os.path.join(RAW_DATA_DIR, filename)
    if not os.path.exists(filepath):
        return None
    return datetime.fromtimestamp(os.path.getmtime(filepath))


def load_last_metrics() -> Dict[str, Dict]:
    """Wczytuje ostatnie metryki pobrania."""
    if not os.path.exists(FETCH_METRICS_FILE):
        return {}
    
    metrics = {}
    try:
        with open(FETCH_METRICS_FILE, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                module = row.get("module")
                if module:
                    metrics[module] = row
    except Exception:
        pass
    return metrics


def validate_file(filename: str, module_name: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Waliduje pojedynczy plik dumpu.
    
    Returns:
        (is_valid, stats)
    """
    stats = {
        "module": module_name,
        "filename": filename,
        "exists": False,
        "records_count": 0,
        "unique_ids": 0,
        "duplicates": 0,
        "records_without_id": 0,
        "min_year": None,
        "max_year": None,
        "records_2025": 0,
        "file_mtime": None,
        "issues": [],
    }
    
    # Sprawdź czy plik istnieje
    filepath = os.path.join(RAW_DATA_DIR, filename)
    if not os.path.exists(filepath):
        stats["issues"].append(f"Plik nie istnieje: {filepath}")
        return False, stats
    
    stats["exists"] = True
    stats["file_mtime"] = get_file_mtime(filename)
    
    # Wczytaj dane
    data = load_json(filename)
    if data is None:
        stats["issues"].append("Błąd parsowania JSON")
        return False, stats
    
    if not data:
        stats["issues"].append("Plik jest pusty")
        return False, stats
    
    stats["records_count"] = len(data)
    
    # Sprawdź ID
    ids = []
    for r in data:
        record_id = r.get("id")
        if not record_id:
            stats["records_without_id"] += 1
        else:
            ids.append(str(record_id))
    
    stats["unique_ids"] = len(set(ids))
    stats["duplicates"] = len(ids) - stats["unique_ids"]
    
    if stats["records_without_id"] > 0:
        stats["issues"].append(f"{stats['records_without_id']} rekordów bez ID")
    
    if stats["duplicates"] > 0:
        stats["issues"].append(f"{stats['duplicates']} duplikatów ID")
    
    # Sprawdź daty
    years = []
    years_2025 = 0
    for r in data:
        date_val = r.get("Created_Time")
        if date_val:
            try:
                year = int(str(date_val)[:4])
                years.append(year)
                if year == ANALYSIS_YEAR:
                    years_2025 += 1
            except (ValueError, IndexError):
                pass
    
    if years:
        stats["min_year"] = min(years)
        stats["max_year"] = max(years)
        stats["records_2025"] = years_2025
        
        if stats["min_year"] < 2015:
            stats["issues"].append(f"Podejrzanie stary rok: {stats['min_year']}")
        if stats["max_year"] > 2030:
            stats["issues"].append(f"Podejrzanie przyszły rok: {stats['max_year']}")
    
    is_valid = len(stats["issues"]) == 0
    return is_valid, stats


def compare_with_metrics(stats: Dict[str, Any], metrics: Dict[str, Dict]) -> List[str]:
    """Porównuje aktualne statystyki z ostatnimi metrykami."""
    warnings = []
    
    module = stats["module"]
    if module not in metrics:
        return warnings
    
    last = metrics[module]
    last_count = int(last.get("records_count", 0))
    current_count = stats["records_count"]
    
    if last_count > 0:
        diff = current_count - last_count
        diff_pct = (diff / last_count) * 100
        
        if abs(diff_pct) > 10:
            direction = "więcej" if diff > 0 else "mniej"
            warnings.append(
                f"Zmiana liczności o {abs(diff_pct):.1f}% ({direction} {abs(diff)} rekordów)"
            )
    
    return warnings


def main():
    logger.info("=" * 60)
    logger.info("WALIDACJA DUMPÓW ZOHO CRM")
    logger.info("=" * 60)
    
    # Wczytaj ostatnie metryki
    last_metrics = load_last_metrics()
    
    all_valid = True
    all_stats = []
    
    for filename, module in FILES_TO_VALIDATE:
        is_valid, stats = validate_file(filename, module)
        all_stats.append(stats)
        
        # Porównaj z poprzednim pobraniem
        comparison_warnings = compare_with_metrics(stats, last_metrics)
        
        # Status
        if not stats["exists"]:
            status = "BRAK"
            all_valid = False
        elif not is_valid:
            status = "BŁĄD"
            all_valid = False
        else:
            status = "OK"
        
        # Wyświetl wynik
        logger.info("")
        logger.info(f"[{status}] {module}")
        logger.info(f"     Plik: {filename}")
        
        if stats["exists"]:
            logger.info(f"     Rekordów: {stats['records_count']} (unikalnych ID: {stats['unique_ids']})")
            if stats["min_year"]:
                logger.info(f"     Lata: {stats['min_year']}-{stats['max_year']}")
            logger.info(f"     Rekordów {ANALYSIS_YEAR}: {stats['records_2025']}")
            if stats["file_mtime"]:
                logger.info(f"     Ostatnia modyfikacja: {stats['file_mtime'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Issues
        for issue in stats["issues"]:
            logger.warning(f"     ! {issue}")
        
        # Porównanie
        for warning in comparison_warnings:
            logger.warning(f"     ~ {warning}")
    
    # Podsumowanie
    logger.info("")
    logger.info("=" * 60)
    
    if all_valid:
        logger.info("WYNIK: Wszystkie dumpy są poprawne")
        sys.exit(0)
    else:
        logger.error("WYNIK: Wykryto problemy z niektórymi dumpami!")
        sys.exit(1)


if __name__ == "__main__":
    main()
