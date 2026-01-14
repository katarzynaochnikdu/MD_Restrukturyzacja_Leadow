"""
Generuje rozbicie MLe per źródło i miesiąc.

Struktura wyjściowa (jak w oryginalnym Excelu):
- I tier | II tier | Sekcja | III tier | Sty | Lut | ... | Gru | SUMA

Sekcje:
- OGÓŁEM: wszystkie ML
- z leadem: ML skonwertowane na Lead
- bez leada: ML bez konwersji (MLe)

Wynik: curated_data/mle_breakdown.csv
"""

import pandas as pd
from pathlib import Path

from config import CURATED_DATA_DIR, MONTHS_2025, MONTH_NUM_TO_NAME

LIFECYCLE_FILE = Path(CURATED_DATA_DIR) / "lifecycle_2025.csv"
OUTPUT_FILE = Path(CURATED_DATA_DIR) / "mle_breakdown.csv"


def get_month(date_str):
    """Wyciąga numer miesiąca z daty ISO."""
    if not date_str or pd.isna(date_str):
        return None
    try:
        return int(date_str[5:7])
    except (ValueError, IndexError):
        return None


def build_breakdown(df: pd.DataFrame, section: str, filter_fn) -> pd.DataFrame:
    """
    Buduje rozbicie dla danej sekcji.
    
    Args:
        df: DataFrame z lifecycle
        section: Nazwa sekcji (OGÓŁEM, z leadem, bez leada)
        filter_fn: Funkcja filtrująca rekordy
    
    Returns:
        DataFrame z rozbiciem
    """
    # Filtruj dane
    filtered = df[filter_fn(df)].copy()
    
    # Dodaj kolumnę miesiąca
    filtered['month'] = filtered['ml_stage_start_time'].apply(get_month)
    
    # Grupuj po źródłach
    rows = []
    
    for (t1, t2, t3), group in filtered.groupby(
        ['source_I_tier', 'source_II_tier', 'source_III_tier'], 
        dropna=False
    ):
        row = {
            'I_tier': t1 if pd.notna(t1) and t1 != '' else 'brak danych',
            'II_tier': t2 if pd.notna(t2) and t2 != '' else 'brak danych',
            'Sekcja': section,
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
        
        if total > 0:  # Tylko wiersze z danymi
            rows.append(row)
    
    return pd.DataFrame(rows)


def main():
    print("Wczytywanie lifecycle...")
    df = pd.read_csv(LIFECYCLE_FILE, dtype=str)
    
    # Filtr: ma ml_id
    has_ml = lambda d: d['ml_id'].notna() & (d['ml_id'] != '')
    
    # Filtr: ma lead_id
    has_lead = lambda d: d['lead_id'].notna() & (d['lead_id'] != '')
    
    # Filtr: nie ma lead_id
    no_lead = lambda d: d['lead_id'].isna() | (d['lead_id'] == '')
    
    print("Budowanie sekcji OGÓŁEM...")
    df_ogolem = build_breakdown(df, 'OGÓŁEM', has_ml)
    
    print("Budowanie sekcji 'z leadem'...")
    df_z_leadem = build_breakdown(df, 'z leadem', lambda d: has_ml(d) & has_lead(d))
    
    print("Budowanie sekcji 'bez leada'...")
    df_bez_leada = build_breakdown(df, 'bez leada', lambda d: has_ml(d) & no_lead(d))
    
    # Połącz wszystkie sekcje
    result = pd.concat([df_ogolem, df_z_leadem, df_bez_leada], ignore_index=True)
    
    # Sortuj
    result = result.sort_values(['I_tier', 'II_tier', 'Sekcja', 'III_tier'])
    
    # Zapisz
    result.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"\nZapisano: {OUTPUT_FILE}")
    
    # Podsumowanie
    print("\n=== PODSUMOWANIE ===")
    for section in ['OGÓŁEM', 'z leadem', 'bez leada']:
        section_df = result[result['Sekcja'] == section]
        total = section_df['SUMA'].sum()
        print(f"{section}: {total} rekordów")
    
    # Pokaż przykład
    print("\n=== PRZYKŁAD (pierwsze 10 wierszy) ===")
    cols = ['I_tier', 'II_tier', 'Sekcja', 'III_tier', 'SUMA']
    print(result[cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
