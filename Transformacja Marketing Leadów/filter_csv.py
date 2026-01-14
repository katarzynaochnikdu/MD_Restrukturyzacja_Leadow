"""
Interaktywny skrypt do filtrowania plików CSV/XLSX.
Pozwala na filtrowanie po wybranych kolumnach z warunkiem "zawiera".
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

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


def load_file(file_path: str) -> pd.DataFrame:
    """Wczytuje plik CSV lub XLSX do DataFrame."""
    path = Path(file_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Plik nie istnieje: {file_path}")
    
    ext = path.suffix.lower()
    
    print(f"Wczytywanie pliku...")
    
    if ext == ".csv":
        # Próbuj różne kodowania
        for encoding in ["utf-8-sig", "utf-8", "cp1252", "iso-8859-1"]:
            try:
                df = pd.read_csv(file_path, encoding=encoding, dtype=str)
                print(f"✓ Wczytano plik CSV (kodowanie: {encoding})")
                return df
            except UnicodeDecodeError:
                continue
        raise ValueError("Nie udało się wczytać pliku CSV - problem z kodowaniem")
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_csv(file_path, dtype=str) if ext == ".csv" else pd.read_excel(file_path, dtype=str)
        print(f"✓ Wczytano plik XLSX")
        return df
    else:
        raise ValueError(f"Nieobsługiwany format pliku: {ext}. Użyj CSV lub XLSX")


def show_columns(df: pd.DataFrame) -> None:
    """Wyświetla listę kolumn z numerami."""
    print("\n" + "="*60)
    print("DOSTĘPNE KOLUMNY:")
    print("="*60)
    for idx, col in enumerate(df.columns, 1):
        non_empty = df[col].notna().sum()
        total = len(df)
        print(f"{idx:3d}. {col:40s} (zapełnienie: {non_empty}/{total})")
    print("="*60)


def show_unique_values(df: pd.DataFrame, column: str, max_display: int = 50) -> None:
    """Wyświetla unikalne wartości w kolumnie."""
    unique_vals = df[column].dropna().unique()
    unique_count = len(unique_vals)
    
    print(f"\n{'='*60}")
    print(f"UNIKALNE WARTOŚCI W KOLUMNIE: {column}")
    print(f"{'='*60}")
    print(f"Liczba unikalnych wartości: {unique_count}")
    print(f"{'='*60}")
    
    if unique_count <= max_display:
        for idx, val in enumerate(sorted([str(v) for v in unique_vals]), 1):
            print(f"{idx:3d}. {val}")
    else:
        print(f"(Za dużo wartości do wyświetlenia - pokazuję pierwsze {max_display})")
        for idx, val in enumerate(sorted([str(v) for v in unique_vals])[:max_display], 1):
            print(f"{idx:3d}. {val}")
        print(f"... i {unique_count - max_display} więcej")
    print("="*60)


def get_column_by_number_or_name(df: pd.DataFrame, user_input: str) -> Optional[str]:
    """Zwraca nazwę kolumny na podstawie numeru lub nazwy."""
    user_input = user_input.strip()
    
    # Sprawdź czy to numer
    if user_input.isdigit():
        col_num = int(user_input)
        if 1 <= col_num <= len(df.columns):
            return df.columns[col_num - 1]
        else:
            print(f"✗ Nieprawidłowy numer kolumny: {col_num}")
            return None
    
    # Sprawdź czy to nazwa kolumny
    if user_input in df.columns:
        return user_input
    
    # Sprawdź czy to częściowa nazwa (case-insensitive)
    matches = [col for col in df.columns if user_input.lower() in col.lower()]
    if len(matches) == 1:
        return matches[0]
    elif len(matches) > 1:
        print(f"✗ Niejednoznaczna nazwa - znaleziono {len(matches)} dopasowań:")
        for m in matches:
            print(f"  - {m}")
        return None
    
    print(f"✗ Nie znaleziono kolumny: {user_input}")
    return None


def apply_filter(df: pd.DataFrame, column: str, search_text: str, case_sensitive: bool = False) -> pd.DataFrame:
    """Filtruje DataFrame - wiersze gdzie kolumna zawiera search_text."""
    from tqdm import tqdm
    
    # Dla małych dataframe'ów (< 1000 wierszy) nie pokazuj progress bara
    if len(df) < 1000:
        if case_sensitive:
            mask = df[column].astype(str).str.contains(search_text, na=False, regex=False)
        else:
            mask = df[column].astype(str).str.contains(search_text, na=False, case=False, regex=False)
        return df[mask]
    
    # Dla dużych dataframe'ów pokaż progress bar
    print(f"Filtrowanie {len(df)} wierszy...")
    tqdm.pandas(desc="Filtrowanie", unit=" wierszy", file=sys.stderr)
    
    if case_sensitive:
        mask = df[column].astype(str).progress_apply(lambda x: search_text in str(x))
    else:
        search_lower = search_text.lower()
        mask = df[column].astype(str).progress_apply(lambda x: search_lower in str(x).lower())
    
    return df[mask]


def save_results(df: pd.DataFrame, output_dir: str, base_name: str) -> None:
    """Zapisuje wyniki do CSV i XLSX."""
    from tqdm import tqdm
    
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # CSV
    csv_path = os.path.join(output_dir, f"{base_name}_{timestamp}.csv")
    print(f"Zapisywanie CSV...")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"✓ Zapisano CSV: {csv_path}")
    
    # XLSX
    xlsx_path = os.path.join(output_dir, f"{base_name}_{timestamp}.xlsx")
    print(f"Zapisywanie XLSX...")
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    print(f"✓ Zapisano XLSX: {xlsx_path}")


def interactive_filter() -> None:
    """Główna funkcja interaktywna."""
    print("\n" + "="*60)
    print("INTERAKTYWNY FILTR CSV/XLSX")
    print("="*60)
    
    # 1. Pobierz ścieżkę do pliku
    if len(sys.argv) > 1:
        file_path = sys.argv[1].strip().strip('"').strip("'")
        print(f"\nPlik wejściowy: {file_path}")
    else:
        print("\nPrzeciągnij plik CSV/XLSX do terminala lub wpisz ścieżkę:")
        file_path = input("Ścieżka: ").strip().strip('"').strip("'")
    
    if not file_path:
        print("✗ Nie podano ścieżki do pliku")
        sys.exit(1)
    
    # 2. Wczytaj plik
    try:
        df = load_file(file_path)
    except Exception as e:
        print(f"✗ Błąd wczytywania pliku: {e}")
        sys.exit(1)
    
    print(f"✓ Wczytano {len(df)} wierszy, {len(df.columns)} kolumn")
    
    # 3. Przygotuj folder wynikowy
    input_path = Path(file_path)
    base_name = input_path.stem
    output_dir = f"wyniki_filtr_{base_name}"
    
    # 4. Filtrowanie - wiele rund
    filtered_df = df.copy()
    filters_applied: List[Dict[str, str]] = []
    
    while True:
        print(f"\nAktualnie: {len(filtered_df)} wierszy (z {len(df)} oryginalnych)")
        
        if filters_applied:
            print("\nZastosowane filtry:")
            for idx, f in enumerate(filters_applied, 1):
                print(f"  {idx}. '{f['column']}' zawiera '{f['value']}'")
        
        print("\n" + "="*60)
        print("OPCJE:")
        print("="*60)
        print("1. Pokaż kolumny")
        print("2. Pokaż unikalne wartości w kolumnie")
        print("3. Dodaj filtr (zawiera)")
        print("4. Zapisz wyniki i zakończ")
        print("5. Anuluj (bez zapisu)")
        print("="*60)
        
        choice = input("\nWybierz opcję (1-5): ").strip()
        
        if choice == "1":
            # Pokaż kolumny
            show_columns(filtered_df)
        
        elif choice == "2":
            # Pokaż unikalne wartości
            show_columns(filtered_df)
            col_input = input("\nPodaj numer lub nazwę kolumny: ").strip()
            column = get_column_by_number_or_name(filtered_df, col_input)
            if column:
                show_unique_values(filtered_df, column)
        
        elif choice == "3":
            # Dodaj filtr
            show_columns(filtered_df)
            
            col_input = input("\nPodaj numer lub nazwę kolumny do filtrowania: ").strip()
            column = get_column_by_number_or_name(filtered_df, col_input)
            
            if not column:
                continue
            
            # Zapytaj czy pokazać unikalne wartości
            show_vals = input(f"Pokazać unikalne wartości w '{column}'? (t/n): ").strip().lower()
            if show_vals in ["t", "tak", "y", "yes"]:
                show_unique_values(filtered_df, column)
            
            search_text = input(f"\nWpisz tekst do wyszukania w '{column}': ").strip()
            
            if not search_text:
                print("✗ Nie podano tekstu do wyszukania")
                continue
            
            # Case sensitive?
            case_sens = input("Rozróżniać wielkość liter? (t/n, domyślnie: n): ").strip().lower()
            case_sensitive = case_sens in ["t", "tak", "y", "yes"]
            
            # Zastosuj filtr
            before_count = len(filtered_df)
            filtered_df = apply_filter(filtered_df, column, search_text, case_sensitive)
            after_count = len(filtered_df)
            
            filters_applied.append({
                "column": column,
                "value": search_text,
                "case_sensitive": case_sensitive
            })
            
            print(f"\n✓ Filtr zastosowany: {before_count} → {after_count} wierszy")
            
            if after_count == 0:
                print("⚠ UWAGA: Filtr nie zwrócił żadnych wyników!")
                undo = input("Cofnąć ostatni filtr? (t/n): ").strip().lower()
                if undo in ["t", "tak", "y", "yes"]:
                    filters_applied.pop()
                    # Zastosuj filtry od początku
                    filtered_df = df.copy()
                    for f in filters_applied:
                        filtered_df = apply_filter(
                            filtered_df, 
                            f["column"], 
                            f["value"], 
                            f.get("case_sensitive", False)
                        )
                    print(f"✓ Cofnięto filtr. Aktualnie: {len(filtered_df)} wierszy")
        
        elif choice == "4":
            # Zapisz i zakończ
            if len(filtered_df) == 0:
                print("✗ Brak wierszy do zapisania")
                continue
            
            print(f"\nZapisuję {len(filtered_df)} wierszy do folderu: {output_dir}")
            
            # Nazwa bazowa z informacją o filtrach
            if filters_applied:
                filter_desc = "_".join([f"{f['column'][:10]}" for f in filters_applied[:2]])
                save_name = f"{base_name}_filtered_{filter_desc}"
            else:
                save_name = f"{base_name}_filtered"
            
            try:
                save_results(filtered_df, output_dir, save_name)
                print(f"\n✓ Zakończono pomyślnie!")
                print(f"✓ Przefiltrowano: {len(df)} → {len(filtered_df)} wierszy")
                break
            except Exception as e:
                print(f"✗ Błąd zapisu: {e}")
        
        elif choice == "5":
            # Anuluj
            confirm = input("Czy na pewno anulować bez zapisu? (t/n): ").strip().lower()
            if confirm in ["t", "tak", "y", "yes"]:
                print("Anulowano")
                sys.exit(0)
        
        else:
            print("✗ Nieprawidłowa opcja")


def main() -> None:
    """Główna funkcja."""
    try:
        interactive_filter()
    except KeyboardInterrupt:
        print("\n\nPrzerwano przez użytkownika")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Błąd: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
