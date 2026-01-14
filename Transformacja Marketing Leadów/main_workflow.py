"""
Główny program łączący wszystkie skrypty w logiczny przepływ pracy.
Pozwala na krok po kroku wykonywanie operacji: pobieranie → filtrowanie → tworzenie/aktualizacja.
"""

import os
import sys
import subprocess
from pathlib import Path
from typing import Optional, List
from datetime import datetime

# Kolory dla Windows
try:
    import colorama
    colorama.init()
    COLOR_GREEN = "\033[92m"
    COLOR_BLUE = "\033[94m"
    COLOR_YELLOW = "\033[93m"
    COLOR_RED = "\033[91m"
    COLOR_RESET = "\033[0m"
except ImportError:
    COLOR_GREEN = COLOR_BLUE = COLOR_YELLOW = COLOR_RED = COLOR_RESET = ""


class WorkflowState:
    """Stan aktualnego przepływu pracy."""
    def __init__(self):
        self.last_file: Optional[str] = None
        self.last_output_dir: Optional[str] = None
        self.workflow_history: List[str] = []
    
    def add_to_history(self, action: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.workflow_history.append(f"[{timestamp}] {action}")
    
    def show_history(self):
        if not self.workflow_history:
            print(f"{COLOR_YELLOW}Brak historii akcji{COLOR_RESET}")
            return
        
        print(f"\n{COLOR_BLUE}{'='*60}")
        print("HISTORIA AKCJI W TEJ SESJI:")
        print(f"{'='*60}{COLOR_RESET}")
        for entry in self.workflow_history:
            print(f"  {entry}")
        print(f"{COLOR_BLUE}{'='*60}{COLOR_RESET}")


def print_header(text: str):
    """Wyświetla nagłówek."""
    print(f"\n{COLOR_BLUE}{'='*60}")
    print(text.center(60))
    print(f"{'='*60}{COLOR_RESET}\n")


def print_success(text: str):
    """Wyświetla komunikat sukcesu."""
    print(f"{COLOR_GREEN}✓ {text}{COLOR_RESET}")


def print_error(text: str):
    """Wyświetla komunikat błędu."""
    print(f"{COLOR_RED}✗ {text}{COLOR_RESET}")


def print_info(text: str):
    """Wyświetla komunikat informacyjny."""
    print(f"{COLOR_YELLOW}ℹ {text}{COLOR_RESET}")


def find_latest_file_in_dir(directory: str, pattern: str = "*.csv") -> Optional[str]:
    """Znajduje najnowszy plik w katalogu."""
    path = Path(directory)
    if not path.exists():
        return None
    
    files = list(path.glob(pattern))
    if not files:
        # Sprawdź też .xlsx
        files = list(path.glob("*.xlsx"))
    
    if not files:
        return None
    
    # Sortuj po czasie modyfikacji
    latest = max(files, key=lambda p: p.stat().st_mtime)
    return str(latest)


def list_recent_files(directories: List[str]) -> List[str]:
    """Lista ostatnich plików z podanych katalogów."""
    recent_files = []
    
    for dir_pattern in directories:
        # Obsługa wildcards jak wyniki_*
        if "*" in dir_pattern:
            base_dir = Path(".")
            for subdir in base_dir.glob(dir_pattern):
                if subdir.is_dir():
                    latest = find_latest_file_in_dir(str(subdir))
                    if latest:
                        recent_files.append(latest)
        else:
            latest = find_latest_file_in_dir(dir_pattern)
            if latest:
                recent_files.append(latest)
    
    return recent_files


def run_script(script_name: str, args: List[str] = None) -> int:
    """Uruchamia skrypt Python."""
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except Exception as e:
        print_error(f"Błąd uruchamiania skryptu: {e}")
        return 1


def action_fetch_marketing_leads(state: WorkflowState):
    """Akcja: Pobierz Marketing Leads."""
    print_header("POBIERANIE MARKETING LEADS")
    
    print("Opcje:")
    print("1. Użyj cache (szybko)")
    print("2. Pobierz świeże dane z API (wolno)")
    
    choice = input("\nWybór (1/2, domyślnie 1): ").strip() or "1"
    
    args = []
    if choice == "2":
        args.append("--no-cache")
    
    print(f"\n{COLOR_YELLOW}Uruchamiam fetch_marketing_leads.py...{COLOR_RESET}\n")
    returncode = run_script("fetch_marketing_leads.py", args)
    
    if returncode == 0:
        # Znajdź najnowszy plik
        latest = find_latest_file_in_dir("wyniki_marketing_leads")
        if latest:
            state.last_file = latest
            state.last_output_dir = "wyniki_marketing_leads"
            print_success(f"Zapisano: {latest}")
            state.add_to_history(f"Pobrano Marketing Leads → {Path(latest).name}")
        else:
            print_error("Nie znaleziono pliku wynikowego")
    else:
        print_error("Pobieranie nie powiodło się")
    
    input("\nNaciśnij Enter aby kontynuować...")


def action_filter_csv(state: WorkflowState):
    """Akcja: Filtruj CSV/XLSX."""
    print_header("FILTROWANIE DANYCH")
    
    # Pokaż dostępne pliki
    recent_files = list_recent_files([
        "wyniki_marketing_leads",
        "wyniki_filtr_*",
        "wyniki_create_leads",
        "wyniki_update_lead_status"
    ])
    
    if state.last_file:
        print_info(f"Ostatni plik: {state.last_file}")
        use_last = input("Użyć tego pliku? (t/n, domyślnie t): ").strip().lower()
        if use_last in ["", "t", "tak", "y", "yes"]:
            file_to_filter = state.last_file
        else:
            file_to_filter = None
    else:
        file_to_filter = None
    
    if not file_to_filter:
        if recent_files:
            print("\nOstatnio utworzone pliki:")
            for idx, f in enumerate(recent_files[:10], 1):
                file_size = Path(f).stat().st_size / 1024
                print(f"{idx:2d}. {f} ({file_size:.1f} KB)")
            
            choice = input("\nWybierz numer pliku lub wpisz własną ścieżkę: ").strip()
            
            if choice.isdigit() and 1 <= int(choice) <= len(recent_files):
                file_to_filter = recent_files[int(choice) - 1]
            else:
                file_to_filter = choice
        else:
            file_to_filter = input("\nPodaj ścieżkę do pliku: ").strip().strip('"').strip("'")
    
    if not file_to_filter:
        print_error("Nie podano pliku")
        input("\nNaciśnij Enter aby kontynuować...")
        return
    
    print(f"\n{COLOR_YELLOW}Uruchamiam filter_csv.py...{COLOR_RESET}\n")
    returncode = run_script("filter_csv.py", [file_to_filter])
    
    if returncode == 0:
        # Znajdź folder wynikowy
        base_name = Path(file_to_filter).stem
        output_dir = f"wyniki_filtr_{base_name}"
        latest = find_latest_file_in_dir(output_dir)
        
        if latest:
            state.last_file = latest
            state.last_output_dir = output_dir
            print_success(f"Przefiltrowano: {latest}")
            state.add_to_history(f"Przefiltrowano → {Path(latest).name}")
        else:
            print_info("Filtrowanie anulowane lub brak wyników")
    else:
        print_error("Filtrowanie nie powiodło się")
    
    input("\nNaciśnij Enter aby kontynuować...")


def action_create_leads(state: WorkflowState):
    """Akcja: Utwórz leady z pliku."""
    print_header("TWORZENIE LEADÓW")
    
    # Podobna logika jak w filter_csv
    recent_files = list_recent_files([
        "wyniki_marketing_leads",
        "wyniki_filtr_*"
    ])
    
    if state.last_file:
        print_info(f"Ostatni plik: {state.last_file}")
        use_last = input("Użyć tego pliku? (t/n, domyślnie t): ").strip().lower()
        if use_last in ["", "t", "tak", "y", "yes"]:
            file_to_use = state.last_file
        else:
            file_to_use = None
    else:
        file_to_use = None
    
    if not file_to_use:
        if recent_files:
            print("\nOstatnio utworzone pliki:")
            for idx, f in enumerate(recent_files[:10], 1):
                file_size = Path(f).stat().st_size / 1024
                print(f"{idx:2d}. {f} ({file_size:.1f} KB)")
            
            choice = input("\nWybierz numer pliku lub wpisz własną ścieżkę: ").strip()
            
            if choice.isdigit() and 1 <= int(choice) <= len(recent_files):
                file_to_use = recent_files[int(choice) - 1]
            else:
                file_to_use = choice
        else:
            file_to_use = input("\nPodaj ścieżkę do pliku: ").strip().strip('"').strip("'")
    
    if not file_to_use:
        print_error("Nie podano pliku")
        input("\nNaciśnij Enter aby kontynuować...")
        return
    
    print(f"\n{COLOR_YELLOW}Uruchamiam create_leads_from_file.py...{COLOR_RESET}\n")
    returncode = run_script("create_leads_from_file.py", [file_to_use])
    
    if returncode == 0:
        latest = find_latest_file_in_dir("wyniki_create_leads")
        if latest:
            state.last_file = latest
            state.last_output_dir = "wyniki_create_leads"
            print_success(f"Utworzono leady: {latest}")
            state.add_to_history(f"Utworzono leady → {Path(latest).name}")
        else:
            print_info("Tworzenie anulowane")
    else:
        print_error("Tworzenie leadów nie powiodło się")
    
    input("\nNaciśnij Enter aby kontynuować...")


def action_update_status(state: WorkflowState):
    """Akcja: Zaktualizuj status leadów."""
    print_header("AKTUALIZACJA STATUSU LEADÓW")
    
    recent_files = list_recent_files([
        "wyniki_marketing_leads",
        "wyniki_filtr_*",
        "wyniki_create_leads"
    ])
    
    if state.last_file:
        print_info(f"Ostatni plik: {state.last_file}")
        use_last = input("Użyć tego pliku? (t/n, domyślnie t): ").strip().lower()
        if use_last in ["", "t", "tak", "y", "yes"]:
            file_to_use = state.last_file
        else:
            file_to_use = None
    else:
        file_to_use = None
    
    if not file_to_use:
        if recent_files:
            print("\nOstatnio utworzone pliki:")
            for idx, f in enumerate(recent_files[:10], 1):
                file_size = Path(f).stat().st_size / 1024
                print(f"{idx:2d}. {f} ({file_size:.1f} KB)")
            
            choice = input("\nWybierz numer pliku lub wpisz własną ścieżkę: ").strip()
            
            if choice.isdigit() and 1 <= int(choice) <= len(recent_files):
                file_to_use = recent_files[int(choice) - 1]
            else:
                file_to_use = choice
        else:
            file_to_use = input("\nPodaj ścieżkę do pliku: ").strip().strip('"').strip("'")
    
    if not file_to_use:
        print_error("Nie podano pliku")
        input("\nNaciśnij Enter aby kontynuować...")
        return
    
    print(f"\n{COLOR_YELLOW}Uruchamiam update_lead_status.py...{COLOR_RESET}\n")
    returncode = run_script("update_lead_status.py", [file_to_use])
    
    if returncode == 0:
        latest = find_latest_file_in_dir("wyniki_update_lead_status")
        if latest:
            state.last_file = latest
            state.last_output_dir = "wyniki_update_lead_status"
            print_success(f"Zaktualizowano statusy: {latest}")
            state.add_to_history(f"Zaktualizowano statusy → {Path(latest).name}")
        else:
            print_info("Aktualizacja anulowana")
    else:
        print_error("Aktualizacja statusów nie powiodła się")
    
    input("\nNaciśnij Enter aby kontynuować...")


def action_update_marketing_status(state: WorkflowState):
    """Akcja: Zaktualizuj status marketing leadów."""
    print_header("AKTUALIZACJA STATUSU MARKETING LEADÓW")

    recent_files = list_recent_files([
        "wyniki_marketing_leads",
        "wyniki_filtr_*"
    ])

    if state.last_file:
        print_info(f"Ostatni plik: {state.last_file}")
        use_last = input("Użyć tego pliku? (t/n, domyślnie t): ").strip().lower()
        if use_last in ["", "t", "tak", "y", "yes"]:
            file_to_use = state.last_file
        else:
            file_to_use = None
    else:
        file_to_use = None

    if not file_to_use:
        if recent_files:
            print("\nOstatnio utworzone pliki:")
            for idx, f in enumerate(recent_files[:10], 1):
                file_size = Path(f).stat().st_size / 1024
                print(f"{idx:2d}. {f} ({file_size:.1f} KB)")
            choice = input("\nWybierz numer pliku lub wpisz własną ścieżkę: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(recent_files):
                file_to_use = recent_files[int(choice) - 1]
            else:
                file_to_use = choice
        else:
            file_to_use = input("\nPodaj ścieżkę do pliku: ").strip().strip('"').strip("'")

    if not file_to_use:
        print_error("Nie podano pliku")
        input("\nNaciśnij Enter aby kontynuować...")
        return

    print(f"\n{COLOR_YELLOW}Uruchamiam update_marketing_lead_status.py...{COLOR_RESET}\n")
    returncode = run_script("update_marketing_lead_status.py", [file_to_use])

    if returncode == 0:
        latest = find_latest_file_in_dir("wyniki_update_marketing_lead_status")
        if latest:
            state.last_file = latest
            state.last_output_dir = "wyniki_update_marketing_lead_status"
            print_success(f"Zaktualizowano marketing leady: {latest}")
            state.add_to_history(f"Zaktualizowano marketing leady → {Path(latest).name}")
        else:
            print_info("Aktualizacja anulowana")
    else:
        print_error("Aktualizacja statusów marketing leadów nie powiodła się")

    input("\nNaciśnij Enter aby kontynuować...")


def action_convert_ml_to_lead(state: WorkflowState):
    """Akcja: Konwertuj Marketing Lead → Lead (zamknij ML + utwórz Lead)."""
    print_header("KONWERSJA ML → LEAD")
    
    recent_files = list_recent_files([
        "wyniki_marketing_leads",
        "wyniki_filtr_*"
    ])
    
    if state.last_file:
        print_info(f"Ostatni plik: {state.last_file}")
        use_last = input("Użyć tego pliku? (t/n, domyślnie t): ").strip().lower()
        if use_last in ["", "t", "tak", "y", "yes"]:
            file_to_use = state.last_file
        else:
            file_to_use = None
    else:
        file_to_use = None
    
    if not file_to_use:
        if recent_files:
            print("\nOstatnio utworzone pliki:")
            for idx, f in enumerate(recent_files[:10], 1):
                file_size = Path(f).stat().st_size / 1024
                print(f"{idx:2d}. {f} ({file_size:.1f} KB)")
            choice = input("\nWybierz numer pliku lub wpisz własną ścieżkę: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(recent_files):
                file_to_use = recent_files[int(choice) - 1]
            else:
                file_to_use = choice
        else:
            file_to_use = input("\nPodaj ścieżkę do pliku: ").strip().strip('"').strip("'")
    
    if not file_to_use:
        print_error("Nie podano pliku")
        input("\nNaciśnij Enter aby kontynuować...")
        return
    
    print(f"\n{COLOR_YELLOW}Uruchamiam convert_ml_to_lead.py...{COLOR_RESET}\n")
    returncode = run_script("convert_ml_to_lead.py", [file_to_use])
    
    if returncode == 0:
        latest = find_latest_file_in_dir("wyniki_convert_ml_to_lead")
        if latest:
            state.last_file = latest
            state.last_output_dir = "wyniki_convert_ml_to_lead"
            print_success(f"Konwersja zakończona: {latest}")
            state.add_to_history(f"Konwersja ML → Lead → {Path(latest).name}")
        else:
            print_info("Konwersja anulowana")
    else:
        print_error("Błąd podczas konwersji")
    
    input("\nNaciśnij Enter aby kontynuować...")


def show_main_menu(state: WorkflowState):
    """Pokazuje główne menu."""
    print_header("TRANSFORMACJA LEADÓW - MENU GŁÓWNE")
    
    if state.last_file:
        print_info(f"Ostatni plik: {Path(state.last_file).name}")
    
    print(f"\n{COLOR_GREEN}AKCJE:{COLOR_RESET}")
    print("1. Pobierz Marketing Leads z API")
    print("2. Filtruj plik CSV/XLSX")
    print("3. Utwórz leady z pliku")
    print("4. Zaktualizuj status leadów")
    print("5. Zaktualizuj status Marketing Leads")
    print(f"{COLOR_YELLOW}6. Konwertuj ML → Lead (zamknij ML + utwórz Lead){COLOR_RESET}")
    
    print(f"\n{COLOR_YELLOW}NARZĘDZIA:{COLOR_RESET}")
    print("7. Pokaż historię akcji")
    print("8. Otwórz folder z wynikami")
    
    print(f"\n{COLOR_BLUE}POMOC:{COLOR_RESET}")
    print("9. Pokaż przykładowe scenariusze")
    print("10. Informacje o projekcie")
    
    print(f"\n0. Wyjście")
    print("="*60)


def show_scenarios():
    """Pokazuje przykładowe scenariusze użycia."""
    print_header("PRZYKŁADOWE SCENARIUSZE")
    
    print(f"{COLOR_GREEN}Scenariusz 1: Tworzenie leadów testowych{COLOR_RESET}")
    print("  1. Akcja 1 → Pobierz Marketing Leads (opcjonalnie)")
    print("  2. Przygotuj plik CSV z ID firm (Account_ID)")
    print("  3. Akcja 2 → Przefiltruj dane (opcjonalnie)")
    print("  4. Akcja 3 → Utwórz leady (tryb testowy: TAK)")
    print("  5. Sprawdź w CRM czy leady się utworzyły")
    
    print(f"\n{COLOR_GREEN}Scenariusz 2: Aktualizacja statusów{COLOR_RESET}")
    print("  1. Akcja 1 → Pobierz Marketing Leads")
    print("  2. Akcja 2 → Przefiltruj po statusie (np. 'Lead')")
    print("  3. Akcja 4 → Zaktualizuj status (np. na 'Dzwonienie')")
    print("  4. Sprawdź wyniki w pliku raportowym")

    print(f"\n{COLOR_GREEN}Scenariusz 3: Aktualizacja statusów Marketing Leads{COLOR_RESET}")
    print("  1. Akcja 1 → Pobierz Marketing Leads")
    print("  2. Akcja 2 → Przefiltruj dane (np. po danych kontaktowych)")
    print("  3. Akcja 5 → Zaktualizuj Etap_kwalifikacji_HL")
    print("  4. Sprawdź raport: wyniki_update_marketing_lead_status/")

    print(f"\n{COLOR_YELLOW}Scenariusz 4: ⭐ KONWERSJA ML → Lead (zamknij + utwórz){COLOR_RESET}")
    print("  1. Akcja 1 → Pobierz Marketing Leads")
    print("  2. Akcja 2 → Przefiltruj (np. Name zawiera 'CC')")
    print("  3. Akcja 6 → Konwertuj ML → Lead")
    print("     - Wybierz JEDEN rekord (test) lub więcej")
    print("     - Wybierz status zamknięcia ML (np. 'odpad')")
    print("     - Wybierz status nowego Lead")
    print("     - Wybierz Owner'a (pracownika)")
    print("     - Potwierdź i obserwuj wynik")
    print("  4. Sprawdź raport: wyniki_convert_ml_to_lead/")

    print(f"\n{COLOR_GREEN}Scenariusz 5: Ciągła praca{COLOR_RESET}")
    print("  1. Akcja 1 → Pobierz dane")
    print("  2. Akcja 2 → Filtruj (krok 1)")
    print("  3. Akcja 2 → Filtruj ponownie (krok 2 - zawężenie)")
    print("  4. Akcja 3 → Utwórz leady")
    print("  5. Akcja 7 → Zobacz historię - co zostało zrobione")
    
    print("\n" + "="*60)
    input("\nNaciśnij Enter aby wrócić do menu...")


def show_info():
    """Pokazuje informacje o projekcie."""
    print_header("INFORMACJE O PROJEKCIE")
    
    print("Transformacja Marketing Leadów")
    print("Wersja: 1.0")
    print("Data: 2026-01-12")
    
    print("\nSkrypty:")
    print("  - fetch_marketing_leads.py    : Pobieranie Marketing Leads")
    print("  - filter_csv.py               : Filtrowanie danych")
    print("  - create_leads_from_file.py   : Tworzenie leadów")
    print("  - update_lead_status.py       : Aktualizacja statusów")
    print("  - update_marketing_lead_status.py : Aktualizacja statusów Marketing Leads")
    print("  - convert_ml_to_lead.py       : ⭐ Konwersja ML → Lead")
    print("  - list_zoho_users.py          : Lista aktywnych użytkowników")
    print("  - zoho_users.py               : Moduł pobierania użytkowników")
    
    print("\nFoldery wynikowe:")
    print("  - wyniki_marketing_leads/     : Pobrane Marketing Leads")
    print("  - wyniki_filtr_*/             : Przefiltrowane dane")
    print("  - wyniki_create_leads/        : Raporty tworzenia leadów")
    print("  - wyniki_update_lead_status/  : Raporty aktualizacji leadów")
    print("  - wyniki_update_marketing_lead_status/  : Raporty aktualizacji ML")
    print("  - wyniki_convert_ml_to_lead/  : ⭐ Raporty konwersji ML → Lead")
    
    print("\nLogi:")
    print("  - fetch_marketing_leads.log")
    print("  - create_leads_from_file.log")
    print("  - update_lead_status.log")
    print("  - update_marketing_lead_status.log")
    
    print("\n" + "="*60)
    input("\nNaciśnij Enter aby wrócić do menu...")


def open_results_folder(state: WorkflowState):
    """Otwiera folder z wynikami w eksploratorze."""
    if state.last_output_dir:
        folder = state.last_output_dir
    else:
        print("Który folder otworzyć?")
        print("1. wyniki_marketing_leads")
        print("2. wyniki_create_leads")
        print("3. wyniki_update_lead_status")
        print("4. wyniki_update_marketing_lead_status")
        print("5. wyniki_convert_ml_to_lead")
        print("6. Inny (podaj nazwę)")
        
        choice = input("\nWybór: ").strip()
        
        if choice == "1":
            folder = "wyniki_marketing_leads"
        elif choice == "2":
            folder = "wyniki_create_leads"
        elif choice == "3":
            folder = "wyniki_update_lead_status"
        elif choice == "4":
            folder = "wyniki_update_marketing_lead_status"
        elif choice == "5":
            folder = "wyniki_convert_ml_to_lead"
        elif choice == "6":
            folder = input("Nazwa folderu: ").strip()
        else:
            print_error("Nieprawidłowy wybór")
            input("\nNaciśnij Enter aby kontynuować...")
            return
    
    if not Path(folder).exists():
        print_error(f"Folder nie istnieje: {folder}")
        input("\nNaciśnij Enter aby kontynuować...")
        return
    
    # Otwórz w eksploratorze
    try:
        if sys.platform == "win32":
            os.startfile(folder)
        elif sys.platform == "darwin":  # macOS
            subprocess.run(["open", folder])
        else:  # Linux
            subprocess.run(["xdg-open", folder])
        
        print_success(f"Otwarto folder: {folder}")
    except Exception as e:
        print_error(f"Nie udało się otworzyć folderu: {e}")
    
    input("\nNaciśnij Enter aby kontynuować...")


def main():
    """Główna pętla programu."""
    state = WorkflowState()
    
    while True:
        # Wyczyść ekran (opcjonalnie)
        # os.system('cls' if os.name == 'nt' else 'clear')
        
        show_main_menu(state)
        
        choice = input("\nWybierz akcję: ").strip()
        
        if choice == "1":
            action_fetch_marketing_leads(state)
        elif choice == "2":
            action_filter_csv(state)
        elif choice == "3":
            action_create_leads(state)
        elif choice == "4":
            action_update_status(state)
        elif choice == "5":
            action_update_marketing_status(state)
        elif choice == "6":
            action_convert_ml_to_lead(state)
        elif choice == "7":
            state.show_history()
            input("\nNaciśnij Enter aby kontynuować...")
        elif choice == "8":
            open_results_folder(state)
        elif choice == "9":
            show_scenarios()
        elif choice == "10":
            show_info()
        elif choice == "0":
            print_info("Do widzenia!")
            break
        else:
            print_error("Nieprawidłowy wybór")
            input("\nNaciśnij Enter aby kontynuować...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{COLOR_YELLOW}Przerwano przez użytkownika{COLOR_RESET}")
        sys.exit(0)
    except Exception as e:
        print_error(f"Błąd: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
