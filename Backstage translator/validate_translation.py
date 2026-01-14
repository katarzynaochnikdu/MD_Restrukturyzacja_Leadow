#!/usr/bin/env python3
"""
Skrypt walidacyjny dla przetłumaczonych plików CSV/XLSX.
Sprawdza poprawność placeholderów, HTML i innych elementów.

Użycie:
    python validate_translation.py path/to/translated.csv
    python validate_translation.py path/to/translated.xlsx
"""

import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from rich.console import Console
from rich.table import Table

from translator_io_utils import identify_columns, read_csv
from translator_placeholders import (
    extract_placeholders,
    extract_html_signature,
    multiset,
)


def validate_row(
    key: str, en: str, pl: str, row_idx: int
) -> Tuple[bool, List[str], List[str]]:
    """
    Waliduje jeden wiersz tłumaczenia.
    
    Returns:
        (is_valid, errors, warnings)
    """
    errors = []
    warnings = []
    
    # Jeśli PL puste - pomiń
    if not pl or not pl.strip():
        return True, errors, warnings
    
    # 1. KRYTYCZNE: Placeholdery muszą się zgadzać
    en_ph = extract_placeholders(en)
    pl_ph = extract_placeholders(pl)
    
    if multiset(en_ph) != multiset(pl_ph):
        errors.append(f"🚨 PLACEHOLDER MISMATCH:")
        errors.append(f"   EN placeholders: {en_ph}")
        errors.append(f"   PL placeholders: {pl_ph}")
        
        # Szczegóły różnic
        en_counts = multiset(en_ph)
        pl_counts = multiset(pl_ph)
        
        missing = set(en_counts.keys()) - set(pl_counts.keys())
        extra = set(pl_counts.keys()) - set(en_counts.keys())
        wrong_count = {k for k in en_counts if k in pl_counts and en_counts[k] != pl_counts[k]}
        
        if missing:
            errors.append(f"   Brakujące: {list(missing)}")
        if extra:
            errors.append(f"   Nadmiarowe: {list(extra)}")
        if wrong_count:
            for k in wrong_count:
                errors.append(f"   Zła liczba '{k}': EN={en_counts[k]}, PL={pl_counts[k]}")
    
    # 2. KRYTYCZNE: HTML musi się zgadzać
    en_html = extract_html_signature(en)
    pl_html = extract_html_signature(pl)
    
    if en_html != pl_html:
        errors.append(f"🚨 HTML MISMATCH:")
        if len(en_html) == 0 and len(pl_html) > 0:
            errors.append(f"   EN: brak HTML, PL: {len(pl_html)} tagów")
        elif len(en_html) > 0 and len(pl_html) == 0:
            errors.append(f"   EN: {len(en_html)} tagów, PL: brak HTML")
        else:
            errors.append(f"   EN HTML: {en_html}")
            errors.append(f"   PL HTML: {pl_html}")
    
    # 3. OSTRZEŻENIE: Długość - red flag jeśli bardzo różna
    if pl and len(pl) > len(en) * 3:
        warnings.append(f"⚠️  PL jest 3x dłuższe niż EN (może być problem)")
        warnings.append(f"   EN length: {len(en)}, PL length: {len(pl)}")
    elif pl and len(en) > 0 and len(en) > len(pl) * 3:
        warnings.append(f"⚠️  PL jest 3x krótsze niż EN (może być problem)")
        warnings.append(f"   EN length: {len(en)}, PL length: {len(pl)}")
    
    # 4. OSTRZEŻENIE: Puste tłumaczenie gdy EN ma tekst
    if en.strip() and not pl.strip():
        warnings.append(f"⚠️  Puste tłumaczenie dla niepustego EN")
    
    # 5. OSTRZEŻENIE: PL zawiera EN tekst (może nie przetłumaczone)
    if en.strip() and pl.strip() and en.lower() in pl.lower() and len(en) > 10:
        warnings.append(f"⚠️  PL zawiera fragment EN (może nie przetłumaczone?)")
    
    is_valid = len(errors) == 0
    return is_valid, errors, warnings


def validate_translation_file(file_path: Path) -> None:
    """Główna funkcja walidacji."""
    console = Console()
    
    console.print(f"\n[bold cyan]═══════════════════════════════════════════════════[/bold cyan]")
    console.print(f"[bold cyan]  Walidacja tłumaczenia: {file_path.name}[/bold cyan]")
    console.print(f"[bold cyan]═══════════════════════════════════════════════════[/bold cyan]\n")
    
    # Wczytaj plik
    if file_path.suffix.lower() == '.xlsx':
        console.print("[yellow]Wczytywanie pliku XLSX...[/yellow]")
        df = pd.read_excel(file_path, dtype=str, keep_default_na=False)
    else:
        console.print("[yellow]Wczytywanie pliku CSV...[/yellow]")
        df, sep = read_csv(file_path)
    
    console.print(f"✅ Wczytano {len(df)} wierszy\n")
    
    # Identyfikuj kolumny
    cols = identify_columns(df)
    console.print(f"[bold]Kolumny:[/bold]")
    console.print(f"  • Klucz: {cols.key_col_name} (kolumna {cols.key_col_idx})")
    console.print(f"  • EN: {cols.source_col_name} (kolumna {cols.source_col_idx})")
    console.print(f"  • PL: {cols.target_col_name} (kolumna {cols.target_col_idx})\n")
    
    # Walidacja
    console.print("[bold yellow]Rozpoczynam walidację...[/bold yellow]\n")
    
    total_rows = 0
    validated_rows = 0
    error_rows = []
    warning_rows = []
    
    for idx, row in df.iterrows():
        key = str(row.iloc[cols.key_col_idx] or "")
        en = str(row.iloc[cols.source_col_idx] or "")
        pl = str(row.iloc[cols.target_col_idx] or "")
        
        # Pomiń wiersze bez EN
        if not en or not en.strip():
            continue
        
        total_rows += 1
        
        # Pomiń wiersze bez PL (nie przetłumaczone)
        if not pl or not pl.strip():
            continue
        
        validated_rows += 1
        
        is_valid, errors, warnings = validate_row(key, en, pl, idx)
        
        if errors:
            error_rows.append({
                'row_idx': idx,
                'key': key,
                'en': en[:60] + "..." if len(en) > 60 else en,
                'pl': pl[:60] + "..." if len(pl) > 60 else pl,
                'errors': errors,
                'warnings': warnings
            })
        elif warnings:
            warning_rows.append({
                'row_idx': idx,
                'key': key,
                'en': en[:60] + "..." if len(en) > 60 else en,
                'pl': pl[:60] + "..." if len(pl) > 60 else pl,
                'warnings': warnings
            })
    
    # Podsumowanie
    console.print("[bold cyan]═══════════════════════════════════════════════════[/bold cyan]")
    console.print("[bold]PODSUMOWANIE:[/bold]")
    console.print(f"  • Wiersze z EN: {total_rows}")
    console.print(f"  • Zwalidowane (mają PL): {validated_rows}")
    console.print(f"  • [red]Błędy krytyczne: {len(error_rows)}[/red]")
    console.print(f"  • [yellow]Ostrzeżenia: {len(warning_rows)}[/yellow]")
    
    if len(error_rows) == 0 and len(warning_rows) == 0:
        console.print("\n[bold green]✅ WSZYSTKIE TŁUMACZENIA POPRAWNE![/bold green]")
    
    # Wyświetl błędy
    if error_rows:
        console.print("\n[bold red]🚨 BŁĘDY KRYTYCZNE:[/bold red]")
        console.print(f"Znaleziono {len(error_rows)} wierszy z krytycznymi błędami:\n")
        
        for item in error_rows[:20]:  # Pokaż max 20
            console.print(f"[red]Wiersz {item['row_idx']} - Key: {item['key']}[/red]")
            console.print(f"  EN: {item['en']}")
            console.print(f"  PL: {item['pl']}")
            for error in item['errors']:
                console.print(f"  {error}")
            if item['warnings']:
                for warning in item['warnings']:
                    console.print(f"  {warning}")
            console.print()
        
        if len(error_rows) > 20:
            console.print(f"[red]... i {len(error_rows) - 20} więcej. Sprawdź szczegółowy raport.[/red]\n")
    
    # Wyświetl ostrzeżenia (tylko top 10)
    if warning_rows and not error_rows:
        console.print("\n[bold yellow]⚠️  OSTRZEŻENIA:[/bold yellow]")
        console.print(f"Znaleziono {len(warning_rows)} wierszy z ostrzeżeniami:\n")
        
        for item in warning_rows[:10]:
            console.print(f"[yellow]Wiersz {item['row_idx']} - Key: {item['key']}[/yellow]")
            console.print(f"  EN: {item['en']}")
            console.print(f"  PL: {item['pl']}")
            for warning in item['warnings']:
                console.print(f"  {warning}")
            console.print()
        
        if len(warning_rows) > 10:
            console.print(f"[yellow]... i {len(warning_rows) - 10} więcej.[/yellow]\n")
    
    # Zapisz raport
    if error_rows or warning_rows:
        report_path = file_path.parent / f"{file_path.stem}.validation_report.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(f"RAPORT WALIDACJI: {file_path.name}\n")
            f.write(f"{'='*80}\n\n")
            f.write(f"Wiersze z EN: {total_rows}\n")
            f.write(f"Zwalidowane: {validated_rows}\n")
            f.write(f"Błędy krytyczne: {len(error_rows)}\n")
            f.write(f"Ostrzeżenia: {len(warning_rows)}\n\n")
            
            if error_rows:
                f.write("BŁĘDY KRYTYCZNE:\n")
                f.write("="*80 + "\n\n")
                for item in error_rows:
                    f.write(f"Wiersz {item['row_idx']} - Key: {item['key']}\n")
                    f.write(f"EN: {item['en']}\n")
                    f.write(f"PL: {item['pl']}\n")
                    for error in item['errors']:
                        f.write(f"{error}\n")
                    if item['warnings']:
                        for warning in item['warnings']:
                            f.write(f"{warning}\n")
                    f.write("\n" + "-"*80 + "\n\n")
            
            if warning_rows:
                f.write("\nOSTRZEŻENIA:\n")
                f.write("="*80 + "\n\n")
                for item in warning_rows:
                    f.write(f"Wiersz {item['row_idx']} - Key: {item['key']}\n")
                    f.write(f"EN: {item['en']}\n")
                    f.write(f"PL: {item['pl']}\n")
                    for warning in item['warnings']:
                        f.write(f"{warning}\n")
                    f.write("\n" + "-"*80 + "\n\n")
        
        console.print(f"\n📝 Szczegółowy raport zapisany: [cyan]{report_path}[/cyan]")
    
    console.print("\n[bold cyan]═══════════════════════════════════════════════════[/bold cyan]\n")
    
    # Exit code
    if error_rows:
        sys.exit(1)  # Błąd - zwróć kod 1
    else:
        sys.exit(0)  # OK


def main():
    """Entry point."""
    if len(sys.argv) < 2:
        print("Użycie: python validate_translation.py <ścieżka_do_pliku>")
        print("Przykład: python validate_translation.py results/Microsite__20251118/Microsite.translated.csv")
        sys.exit(1)
    
    file_path = Path(sys.argv[1])
    
    if not file_path.exists():
        print(f"❌ Plik nie istnieje: {file_path}")
        sys.exit(1)
    
    if file_path.suffix.lower() not in ['.csv', '.xlsx']:
        print(f"❌ Nieobsługiwany format pliku: {file_path.suffix}")
        print("   Obsługiwane formaty: .csv, .xlsx")
        sys.exit(1)
    
    validate_translation_file(file_path)


if __name__ == "__main__":
    main()

