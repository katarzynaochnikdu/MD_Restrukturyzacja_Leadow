from pathlib import Path

path = Path("c:/Users/kochn/.cursor/Medidesk/2026 Fundusze Wsparcia placówki POZ/Transformacja Marketing Leadów/README.md")
text = path.read_text(encoding="utf-8")
old = """**Scenariusz 1: Tworzenie testowych leadów**
```
Menu → Akcja 1 → Pobierz Marketing Leads (opcjonalnie)
Menu → Akcja 2 → Filtruj dane (jeśli potrzeba)
Menu → Akcja 3 → Utwórz leady
  - Program podpowie ostatni plik
  - Wybierz tryb testowy: TAK
  - Sprawdź wyniki
Menu → Akcja 6 → Otwórz folder z wynikami
```

**Scenariusz 2: Aktualizacja statusów**
```
Menu → Akcja 1 → Pobierz Marketing Leads
Menu → Akcja 2 → Przefiltruj po statusie "Lead"
Menu → Akcja 4 → Zaktualizuj status na "Dzwonienie"
Menu → Akcja 5 → Zobacz historię akcji
```
"""
new = """**Scenariusz 1: Tworzenie testowych leadów**
```
Menu → Akcja 1 → Pobierz Marketing Leads (opcjonalnie)
Menu → Akcja 2 → Filtruj dane (jeśli potrzeba)
Menu → Akcja 3 → Utwórz leady
  - Program podpowie ostatni plik
  - Wybierz tryb testowy: TAK
  - Sprawdź wyniki
Menu → Akcja 6 → Otwórz folder z wynikami
```

**Scenariusz 2: Aktualizacja statusów**
```
Menu → Akcja 1 → Pobierz Marketing Leads
Menu → Akcja 2 → Przefiltruj po statusie "Lead"
Menu → Akcja 4 → Zaktualizuj status na "Dzwonienie"
Menu → Akcja 5 → Zobacz historię akcji
```

**Scenariusz 3: Aktualizacja statusów Marketing Leads**
```
Menu → Akcja 1 → Pobierz Marketing Leads
Menu → Akcja 2 → Przefiltruj dane
Menu → Akcja 5 → Zaktualizuj etap (Etap_kwalifikacji_HL)
Menu → Akcja 7 → Otwórz folder `wyniki_update_marketing_lead_status/`
```
"""
if old not in text:
    raise SystemExit("pattern not found")
text = text.replace(old, new, 1)
path.write_text(text, encoding="utf-8")
