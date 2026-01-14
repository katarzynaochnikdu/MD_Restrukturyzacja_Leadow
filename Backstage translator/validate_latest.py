#!/usr/bin/env python3
"""
Helper: Automatycznie znajduje i waliduje najnowsze tłumaczenie.

Użycie:
    python validate_latest.py
"""

from pathlib import Path
import subprocess
import sys

def find_latest_translation() -> Path:
    """Znajduje najnowszy plik .translated.csv lub .translated.xlsx"""
    results_dir = Path("results")
    
    if not results_dir.exists():
        print("❌ Katalog 'results' nie istnieje!")
        sys.exit(1)
    
    # Znajdź najnowszy katalog
    result_dirs = sorted(
        [d for d in results_dir.iterdir() if d.is_dir()],
        key=lambda x: x.stat().st_mtime,
        reverse=True
    )
    
    if not result_dirs:
        print("❌ Brak katalogów w 'results'!")
        sys.exit(1)
    
    latest_dir = result_dirs[0]
    
    # Znajdź plik .translated.csv lub .translated.xlsx
    translated_files = list(latest_dir.glob("*.translated.csv")) + \
                      list(latest_dir.glob("*.translated.xlsx"))
    
    if not translated_files:
        print(f"❌ Brak plików .translated w {latest_dir}!")
        sys.exit(1)
    
    # Wybierz CSV jeśli jest, inaczej XLSX
    csv_files = [f for f in translated_files if f.suffix == '.csv']
    file = csv_files[0] if csv_files else translated_files[0]
    
    return file


def main():
    print("🔍 Szukam najnowszego tłumaczenia...\n")
    
    try:
        file = find_latest_translation()
        print(f"✅ Znaleziono: {file}\n")
        print("="*60)
        
        # Uruchom walidację
        result = subprocess.run(
            [sys.executable, "validate_translation.py", str(file)],
            cwd=Path.cwd()
        )
        
        sys.exit(result.returncode)
        
    except Exception as e:
        print(f"❌ Błąd: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

