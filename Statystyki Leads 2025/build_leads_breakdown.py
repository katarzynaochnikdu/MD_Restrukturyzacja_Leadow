"""
Generuje rozbicie Leadów (MLe+ Leady) per źródło i miesiąc.

Struktura wyjściowa:
- I tier | II tier | III tier | Sty | Lut | ... | Gru | SUMA

Wynik: curated_data/mle_plus_leady_breakdown.csv
"""

import pandas as pd
from pathlib import Path

from config import CURATED_DATA_DIR, MONTHS_2025, MONTH_NUM_TO_NAME

LIFECYCLE_FILE = Path(CURATED_DATA_DIR) / "lifecycle_2025.csv"
OUTPUT_FILE = Path(CURATED_DATA_DIR) / "mle_plus_leady_breakdown.csv"


def get_month(date_str):
    """Wyciąga numer miesiąca z daty ISO."""
    if not date_str or pd.isna(date_str):
        return None
    try:
        return int(date_str[5:7])
    except (ValueError, IndexError):
        return None


def main():
    print("Wczytywanie lifecycle...")
    df = pd.read_csv(LIFECYCLE_FILE, dtype=str)
    
    # Filtruj: tylko z lead_id (wszystkie Leady)
    has_lead = df['lead_id'].notna() & (df['lead_id'] != '')
    leads = df[has_lead].copy()
    
    print(f"Leadów: {len(leads)}")
    
    # Dodaj kolumnę miesiąca (z lead_stage_start_time)
    leads['month'] = leads['lead_stage_start_time'].apply(get_month)
    
    # Grupuj po źródłach
    rows = []
    
    for (t1, t2, t3), group in leads.groupby(
        ['source_I_tier', 'source_II_tier', 'source_III_tier'], 
        dropna=False
    ):
        row = {
            'I_tier': t1 if pd.notna(t1) and t1 != '' else 'brak danych',
            'II_tier': t2 if pd.notna(t2) and t2 != '' else 'brak danych',
            'III_tier': t3 if pd.notna(t3) and t3 != '' else 'brak danych',
        }
        
        # Zlicz per miesiąc
        total = 0
        for month_num in range(1, 13):
            month_name = MONTH_NUM_TO_NAME.get(month_num, f'M{month_num}')
            count = len(group[group['month'] == month_num])
            row[month_name] = count
            total += count
        
        row['SUMA'] = total
        
        if total > 0:
            rows.append(row)
    
    result = pd.DataFrame(rows)
    
    # Sortuj
    result = result.sort_values(['I_tier', 'II_tier', 'III_tier'])
    
    # Zapisz
    result.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\nZapisano: {OUTPUT_FILE}")
    
    # Podsumowanie miesięczne
    print("\n=== PODSUMOWANIE MIESIĘCZNE ===")
    for month in MONTHS_2025:
        if month in result.columns:
            total = result[month].sum()
            print(f"{month}: {total}")
    print(f"SUMA: {result['SUMA'].sum()}")
    
    # Top źródła
    print("\n=== TOP 10 ŹRÓDEŁ ===")
    top = result.nlargest(10, 'SUMA')[['I_tier', 'II_tier', 'III_tier', 'SUMA']]
    print(top.to_string(index=False))


if __name__ == "__main__":
    main()
