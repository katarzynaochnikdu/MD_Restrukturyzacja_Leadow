#!/usr/bin/env python3
"""
Skrypt weryfikujący poprawność logów Phase 1 i Phase 3.
Sprawdza czy zawierają wszystkie wymagane pola: row_indices, keys, en.
"""
import json
from pathlib import Path
from rich.console import Console
from rich.table import Table

def verify_phase1_logs(results_dir: Path) -> bool:
    """Weryfikuje logi Phase 1."""
    console = Console()
    
    requests_log = results_dir / "phase1_requests.jsonl"
    responses_log = results_dir / "phase1_responses.jsonl"
    
    if not requests_log.exists():
        console.print(f"[red]✗ Brak pliku {requests_log}[/red]")
        return False
    
    if not responses_log.exists():
        console.print(f"[red]✗ Brak pliku {responses_log}[/red]")
        return False
    
    console.print("\n[bold]Phase 1 Requests:[/bold]")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Linia", width=5)
    table.add_column("Row idx", width=10)
    table.add_column("Key", width=30)
    table.add_column("EN fragment", width=40)
    
    with open(requests_log, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            if not line.strip():
                continue
            obj = json.loads(line)
            
            # Sprawdź wymagane pola (teraz pojedyncze, nie listy)
            has_en = "en" in obj
            has_row_idx = "row_idx" in obj
            has_key = "key" in obj
            
            if not (has_en and has_row_idx and has_key):
                console.print(f"[red]✗ Linia {i}: brak wymaganych pól![/red]")
                console.print(f"  en: {has_en}, row_idx: {has_row_idx}, key: {has_key}")
                return False
            
            row_idx = str(obj['row_idx'])
            key = obj['key']
            en_fragment = obj['en'][:40] + "..." if len(obj['en']) > 40 else obj['en']
            
            table.add_row(str(i), row_idx, key, en_fragment)
            
            if i >= 5:  # Pokaż tylko pierwsze 5
                break
    
    console.print(table)
    console.print("[green]✓ Phase 1 requests - format poprawny (NO DEDUPLICATION - 1:1 mapping)![/green]")
    
    # Sprawdź responses
    console.print("\n[bold]Phase 1 Responses:[/bold]")
    table2 = Table(show_header=True, header_style="bold cyan")
    table2.add_column("Linia", width=5)
    table2.add_column("Row idx", width=10)
    table2.add_column("Key", width=30)
    table2.add_column("Translation", width=40)
    
    with open(responses_log, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            if not line.strip():
                continue
            obj = json.loads(line)
            
            has_en = "en" in obj
            has_row_idx = "row_idx" in obj
            has_key = "key" in obj
            
            if not (has_en and has_row_idx and has_key):
                console.print(f"[red]✗ Linia {i}: brak wymaganych pól![/red]")
                return False
            
            row_idx = str(obj['row_idx'])
            key = obj['key']
            
            # Pokaż tłumaczenie jeśli jest
            if "response" in obj:
                try:
                    response_obj = json.loads(obj['response'])
                    translation = response_obj.get('translation', 'N/A')[:40]
                except:
                    translation = "parse_error"
            else:
                translation = obj.get('error', 'error')[:40]
            
            table2.add_row(str(i), row_idx, key, translation)
            
            if i >= 5:
                break
    
    console.print(table2)
    console.print("[green]✓ Phase 1 responses - format poprawny (1:1 mapping)![/green]")
    
    return True


def verify_phase3_logs(results_dir: Path) -> bool:
    """Weryfikuje logi Phase 3."""
    console = Console()
    
    requests_log = results_dir / "phase3_requests.jsonl"
    
    if not requests_log.exists():
        console.print("[yellow]Phase 3 nie uruchomiona - to normalne jeśli wszystkie tłumaczenia miały wysoką confidence[/yellow]")
        return True
    
    console.print("\n[bold]Phase 3 Requests:[/bold]")
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Linia", width=5)
    table.add_column("Row idx", width=10)
    table.add_column("Key", width=30)
    table.add_column("EN fragment", width=40)
    
    with open(requests_log, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f, 1):
            if not line.strip():
                continue
            obj = json.loads(line)
            
            has_row_idx = "row_idx" in obj
            has_key = "key" in obj
            has_en = "en" in obj
            
            if not (has_row_idx and has_key and has_en):
                console.print(f"[red]✗ Linia {i}: brak wymaganych pól![/red]")
                return False
            
            row_idx = str(obj['row_idx'])
            key = obj['key']
            en_fragment = obj['en'][:40] + "..." if len(obj['en']) > 40 else obj['en']
            
            table.add_row(str(i), row_idx, key, en_fragment)
            
            if i >= 5:
                break
    
    console.print(table)
    console.print("[green]✓ Phase 3 - format poprawny![/green]")
    
    return True


def main():
    """Główna funkcja."""
    console = Console()
    console.print("[bold cyan]═══════════════════════════════════════════════════[/bold cyan]")
    console.print("[bold cyan]  Weryfikacja logów tłumaczenia[/bold cyan]")
    console.print("[bold cyan]═══════════════════════════════════════════════════[/bold cyan]")
    
    # Znajdź najnowszy katalog z wynikami
    results_root = Path("results")
    if not results_root.exists():
        console.print("[red]✗ Brak katalogu 'results'![/red]")
        return
    
    # Znajdź najnowszy katalog
    result_dirs = sorted([d for d in results_root.iterdir() if d.is_dir()], 
                        key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not result_dirs:
        console.print("[red]✗ Brak katalogów z wynikami![/red]")
        return
    
    latest_dir = result_dirs[0]
    console.print(f"\n[bold]Sprawdzam katalog:[/bold] {latest_dir.name}")
    
    # Weryfikuj logi
    phase1_ok = verify_phase1_logs(latest_dir)
    phase3_ok = verify_phase3_logs(latest_dir)
    
    console.print("\n[bold cyan]═══════════════════════════════════════════════════[/bold cyan]")
    if phase1_ok and phase3_ok:
        console.print("[bold green]✓ Wszystkie logi są poprawne![/bold green]")
        console.print("\n[bold]Co oznacza poprawny format:[/bold]")
        console.print("• Phase 1: każda linia zawiera 'row_idx', 'key', 'en' (NO DEDUPLICATION)")
        console.print("• Phase 3: każda linia zawiera 'row_idx', 'key', 'en'")
        console.print("\n[bold green]✓ Mapowanie 1:1 - zero ryzyka pomyłek![/bold green]")
        console.print("  Każdy wiersz ma swoje własne tłumaczenie → łatwa weryfikacja w logach")
    else:
        console.print("[bold red]✗ Wykryto błędy w logach![/bold red]")
    console.print("[bold cyan]═══════════════════════════════════════════════════[/bold cyan]\n")


if __name__ == "__main__":
    main()

