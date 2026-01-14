"""
Konwertuje lifecycle_2025.csv na pivoty miesięczne do porównania z Excelem.

Tworzy dwa pivoty:
1. MLe - Marketing Leads bez konwersji na Leada (ml_id bez lead_id)
2. MLe+ Leady - Wszystkie Leady (lead_id present, deduplicated)

Porównuje z arkuszami w pliku wejściowym i raportuje różnice.

Wynik: curated_data/mle_monthly.csv, curated_data/mle_plus_leady_monthly.csv
"""

import csv
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config import (
    RAW_DATA_DIR, CURATED_DATA_DIR, ANALYSIS_YEAR,
    MONTH_NUM_TO_NAME, MONTHS_2025, EXCEL_SUMMARY_FILE
)

# --- Konfiguracja logowania ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

LIFECYCLE_FILE = os.path.join(CURATED_DATA_DIR, "lifecycle_2025.csv")
MLY_MONTHLY_OUT = os.path.join(CURATED_DATA_DIR, "mly_monthly.csv")
MLY_PLUS_MONTHLY_OUT = os.path.join(CURATED_DATA_DIR, "mly_plus_monthly.csv")


def load_lifecycle() -> pd.DataFrame:
    """Wczytuje lifecycle_2025.csv."""
    if not os.path.exists(LIFECYCLE_FILE):
        logger.error(f"Plik {LIFECYCLE_FILE} nie istnieje. Uruchom build_lifecycle.py")
        sys.exit(1)
    
    df = pd.read_csv(LIFECYCLE_FILE, dtype=str)
    logger.info(f"Wczytano {len(df)} wierszy z {LIFECYCLE_FILE}")
    return df


def parse_date_column(df: pd.DataFrame, col: str) -> pd.Series:
    """Parsuje kolumnę daty na datetime."""
    return pd.to_datetime(df[col], errors="coerce", utc=True)


def extract_month_from_date(dt_series: pd.Series) -> pd.Series:
    """Wyciąga numer miesiąca z serii datetime."""
    return dt_series.dt.month


def build_mle_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Buduje pivot MLe (ML bez konwersji na Leada).
    
    Definicja: ml_id niepuste AND lead_id puste
    Data: ml_stage_start_time
    Grupowanie: source_I_tier, source_II_tier, source_III_tier -> miesiące
    """
    # Filtruj MLe (ML bez Leada)
    mask = (df["ml_id"].notna() & (df["ml_id"] != "")) & \
           (df["lead_id"].isna() | (df["lead_id"] == ""))
    mle = df[mask].copy()
    
    logger.info(f"MLe (ML bez Lead): {len(mle)} rekordów")
    
    # Parsuj datę
    mle["_date"] = parse_date_column(mle, "ml_stage_start_time")
    mle["_month"] = extract_month_from_date(mle["_date"])
    
    # Grupuj
    pivot_data = []
    for (t1, t2, t3), group in mle.groupby(
        ["source_I_tier", "source_II_tier", "source_III_tier"], dropna=False
    ):
        row = {
            "I_tier": t1 or "brak danych",
            "II_tier": t2 or "brak danych",
            "III_tier": t3 or "brak danych",
        }
        
        for month in range(1, 13):
            month_name = MONTH_NUM_TO_NAME.get(month, f"M{month}")
            count = len(group[group["_month"] == month])
            row[month_name] = count
        
        row["SUMA"] = len(group)
        pivot_data.append(row)
    
    # Sortuj
    pivot_df = pd.DataFrame(pivot_data)
    pivot_df = pivot_df.sort_values(["I_tier", "II_tier", "III_tier"])
    
    return pivot_df


def build_mle_plus_leady_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Buduje pivot MLe+ Leady (wszystkie Leady).
    
    Definicja: lead_id niepuste (Lead z ML lub Lead bez ML = 1 wiersz per Lead)
    Data: lead_stage_start_time
    Grupowanie: source_I_tier, source_II_tier, source_III_tier -> miesiące
    """
    # Filtruj wszystkie z Lead
    mask = df["lead_id"].notna() & (df["lead_id"] != "")
    leads = df[mask].copy()
    
    logger.info(f"MLe+ Leady (wszystkie Leady): {len(leads)} rekordów")
    
    # Parsuj datę
    leads["_date"] = parse_date_column(leads, "lead_stage_start_time")
    leads["_month"] = extract_month_from_date(leads["_date"])
    
    # Grupuj
    pivot_data = []
    for (t1, t2, t3), group in leads.groupby(
        ["source_I_tier", "source_II_tier", "source_III_tier"], dropna=False
    ):
        row = {
            "I_tier": t1 or "brak danych",
            "II_tier": t2 or "brak danych",
            "III_tier": t3 or "brak danych",
        }
        
        for month in range(1, 13):
            month_name = MONTH_NUM_TO_NAME.get(month, f"M{month}")
            count = len(group[group["_month"] == month])
            row[month_name] = count
        
        row["SUMA"] = len(group)
        pivot_data.append(row)
    
    # Sortuj
    pivot_df = pd.DataFrame(pivot_data)
    pivot_df = pivot_df.sort_values(["I_tier", "II_tier", "III_tier"])
    
    return pivot_df


def load_excel_sheet(sheet_name: str) -> Optional[pd.DataFrame]:
    """Wczytuje arkusz z pliku Excel."""
    try:
        df = pd.read_excel(EXCEL_SUMMARY_FILE, sheet_name=sheet_name, header=None)
        return df
    except Exception as e:
        logger.warning(f"Nie można wczytać arkusza {sheet_name}: {e}")
        return None


def extract_monthly_totals_from_excel(sheet_df: pd.DataFrame, header_row: int = 1) -> Dict[str, int]:
    """
    Wyciąga sumy miesięczne z arkusza Excel.
    
    Returns:
        {nazwa_miesiąca: suma}
    """
    # Znajdź wiersz z nagłówkami
    headers = sheet_df.iloc[header_row].tolist()
    
    # Znajdź wiersz z sumami (ostatni wiersz z danymi lub wiersz SUMA/Suma ogółem)
    totals = {}
    
    # Szukaj kolumn miesięcy
    for col_idx, header in enumerate(headers):
        if isinstance(header, str) and header in MONTHS_2025:
            # Znajdź sumę w ostatnim wierszu lub wierszu z "SUMA"
            for row_idx in range(len(sheet_df) - 1, header_row, -1):
                cell_value = sheet_df.iloc[row_idx, col_idx]
                first_col = sheet_df.iloc[row_idx, 0]
                
                # Sprawdź czy to wiersz sumy
                if isinstance(first_col, str) and "SUMA" in str(first_col).upper():
                    try:
                        totals[header] = int(cell_value) if pd.notna(cell_value) else 0
                    except (ValueError, TypeError):
                        totals[header] = 0
                    break
    
    return totals


def compare_with_excel(
    pivot_df: pd.DataFrame,
    sheet_name: str,
    months_to_compare: List[str] = None
) -> Dict[str, Dict[str, Any]]:
    """
    Porównuje pivot z arkuszem Excel.
    
    Returns:
        {miesiąc: {calculated: X, excel: Y, diff: Z}}
    """
    if months_to_compare is None:
        # Domyślnie porównuj Styczeń-Październik (bez Listopada i Grudnia)
        months_to_compare = MONTHS_2025[:10]
    
    # Oblicz sumy z pivota
    calculated_totals = {}
    for month in months_to_compare:
        if month in pivot_df.columns:
            calculated_totals[month] = pivot_df[month].sum()
        else:
            calculated_totals[month] = 0
    
    # Wczytaj Excel
    excel_df = load_excel_sheet(sheet_name)
    if excel_df is None:
        return {m: {"calculated": calculated_totals.get(m, 0), "excel": None, "diff": None} 
                for m in months_to_compare}
    
    # Wyciąg sumy z Excel
    excel_totals = extract_monthly_totals_from_excel(excel_df)
    
    # Porównaj
    comparison = {}
    for month in months_to_compare:
        calc = calculated_totals.get(month, 0)
        excel = excel_totals.get(month)
        diff = (calc - excel) if excel is not None else None
        comparison[month] = {
            "calculated": calc,
            "excel": excel,
            "diff": diff,
        }
    
    return comparison


def print_comparison(title: str, comparison: Dict[str, Dict[str, Any]]):
    """Wyświetla porównanie."""
    logger.info("")
    logger.info(f"=== {title} ===")
    logger.info(f"{'Miesiąc':<12} {'Obliczone':>10} {'Excel':>10} {'Różnica':>10}")
    logger.info("-" * 45)
    
    total_calc = 0
    total_excel = 0
    total_diff = 0
    
    for month, data in comparison.items():
        calc = data["calculated"]
        excel = data["excel"]
        diff = data["diff"]
        
        excel_str = str(excel) if excel is not None else "N/A"
        diff_str = f"{diff:+d}" if diff is not None else "N/A"
        
        logger.info(f"{month:<12} {calc:>10} {excel_str:>10} {diff_str:>10}")
        
        total_calc += calc
        if excel is not None:
            total_excel += excel
            total_diff += diff
    
    logger.info("-" * 45)
    logger.info(f"{'SUMA':<12} {total_calc:>10} {total_excel:>10} {total_diff:+10}")


def main():
    logger.info("=" * 60)
    logger.info("KONWERSJA LIFECYCLE DO PIVOTÓW MIESIĘCZNYCH")
    logger.info("=" * 60)
    
    # Wczytaj lifecycle
    df = load_lifecycle()
    
    # Buduj pivoty
    logger.info("")
    logger.info("Budowanie pivota MLe...")
    mle_pivot = build_mle_pivot(df)
    
    logger.info("")
    logger.info("Budowanie pivota MLe+ Leady...")
    mle_plus_pivot = build_mle_plus_leady_pivot(df)
    
    # Zapisz
    mle_pivot.to_csv(MLY_MONTHLY_OUT, index=False, encoding="utf-8-sig")
    logger.info(f"Zapisano {MLY_MONTHLY_OUT}")
    
    mle_plus_pivot.to_csv(MLY_PLUS_MONTHLY_OUT, index=False, encoding="utf-8-sig")
    logger.info(f"Zapisano {MLY_PLUS_MONTHLY_OUT}")
    
    # Podsumowanie
    logger.info("")
    logger.info("PODSUMOWANIE MIESIĘCZNE:")
    
    # MLe
    logger.info("")
    logger.info("MLe (ML bez konwersji na Leada):")
    for month in MONTHS_2025[:12]:
        if month in mle_pivot.columns:
            total = mle_pivot[month].sum()
            logger.info(f"  {month}: {total}")
    logger.info(f"  SUMA: {mle_pivot['SUMA'].sum()}")
    
    # MLe+ Leady
    logger.info("")
    logger.info("MLe+ Leady (wszystkie Leady):")
    for month in MONTHS_2025[:12]:
        if month in mle_plus_pivot.columns:
            total = mle_plus_pivot[month].sum()
            logger.info(f"  {month}: {total}")
    logger.info(f"  SUMA: {mle_plus_pivot['SUMA'].sum()}")
    
    # Porównanie z Excelem
    logger.info("")
    logger.info("PORÓWNANIE Z EXCELEM (Jan-Paź):")
    
    mle_comparison = compare_with_excel(mle_pivot, "MLe", MONTHS_2025[:10])
    print_comparison("MLe", mle_comparison)
    
    mle_plus_comparison = compare_with_excel(mle_plus_pivot, "MLe+ Leady", MONTHS_2025[:10])
    print_comparison("MLe+ Leady", mle_plus_comparison)


if __name__ == "__main__":
    main()
