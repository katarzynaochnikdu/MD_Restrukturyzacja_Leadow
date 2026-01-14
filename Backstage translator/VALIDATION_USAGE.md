# 🔍 Walidacja tłumaczeń - Instrukcja użycia

## Plik: `validate_translation.py`

Osobny skrypt do walidacji gotowych tłumaczeń. Sprawdza:
- ✅ Placeholdery `{...}` - czy się zgadzają 1:1
- ✅ HTML tagi - czy są identyczne
- ⚠️ Długość tekstu - czy nie jest 3x różna
- ⚠️ Puste tłumaczenia
- ⚠️ Fragmenty EN w PL (może nie przetłumaczone)

## Użycie

### 1. Po przetłumaczeniu pliku
```bash
python validate_translation.py results/Microsite__20251118/Microsite.translated.csv
```

### 2. Lub dla XLSX
```bash
python validate_translation.py results/Microsite__20251118/Microsite.translated.xlsx
```

### 3. Automatycznie po tłumaczeniu
Możesz dodać do workflow:
```bash
python run.py
# Po zakończeniu tłumaczenia:
python validate_translation.py results/[najnowszy]/[plik].translated.csv
```

## Output

### ✅ Gdy wszystko OK:
```
═══════════════════════════════════════════════════
  Walidacja tłumaczenia: Microsite.translated.csv
═══════════════════════════════════════════════════

✅ Wczytano 200 wierszy

Kolumny:
  • Klucz: Reference Key (kolumna 0)
  • EN: Default Language (kolumna 1)
  • PL: PL (kolumna 2)

Rozpoczynam walidację...

═══════════════════════════════════════════════════
PODSUMOWANIE:
  • Wiersze z EN: 180
  • Zwalidowane (mają PL): 180
  • Błędy krytyczne: 0
  • Ostrzeżenia: 0

✅ WSZYSTKIE TŁUMACZENIA POPRAWNE!

═══════════════════════════════════════════════════
```

### 🚨 Gdy są błędy:
```
═══════════════════════════════════════════════════
PODSUMOWANIE:
  • Wiersze z EN: 180
  • Zwalidowane (mają PL): 180
  • Błędy krytyczne: 3
  • Ostrzeżenia: 5

🚨 BŁĘDY KRYTYCZNE:
Znaleziono 3 wierszy z krytycznymi błędami:

Wiersz 42 - Key: msg.error.otp.blocked
  EN: Try regenerating after {blockedTime} mins.
  PL: Spróbuj wygenerować po blockedTime minutach.
  🚨 PLACEHOLDER MISMATCH:
     EN placeholders: ['{blockedTime}']
     PL placeholders: []
     Brakujące: ['{blockedTime}']

Wiersz 53 - Key: msg.sent.sign.in.email
  EN: We've sent an email to <strong>{emailId}</strong>
  PL: Wysłaliśmy e-mail na adres {emailId}
  🚨 HTML MISMATCH:
     EN HTML: [('strong', ()), ('/strong', ())]
     PL HTML: []

...

📝 Szczegółowy raport zapisany: results/.../Microsite.translated.validation_report.txt
```

## Szczegółowy raport TXT

Jeśli znajdzie problemy, zapisze raport tekstowy:
```
results/Microsite__20251118/Microsite.translated.validation_report.txt
```

Format:
```
RAPORT WALIDACJI: Microsite.translated.csv
================================================================================

Wiersze z EN: 180
Zwalidowane: 180
Błędy krytyczne: 3
Ostrzeżenia: 5

BŁĘDY KRYTYCZNE:
================================================================================

Wiersz 42 - Key: msg.error.otp.blocked
EN: Try regenerating after {blockedTime} mins.
PL: Spróbuj wygenerować po blockedTime minutach.
🚨 PLACEHOLDER MISMATCH:
   EN placeholders: ['{blockedTime}']
   PL placeholders: []
   Brakujące: ['{blockedTime}']

--------------------------------------------------------------------------------
...
```

## Exit codes

- **0** - Wszystko OK lub tylko ostrzeżenia
- **1** - Znaleziono błędy krytyczne

Możesz użyć w skryptach:
```bash
python validate_translation.py file.csv
if [ $? -eq 0 ]; then
    echo "Walidacja OK!"
else
    echo "Błędy! Sprawdź raport."
fi
```

## Co jest sprawdzane?

### 🚨 BŁĘDY KRYTYCZNE (muszą być naprawione):

1. **Placeholder mismatch**
   - Brakujące placeholdery w PL
   - Nadmiarowe placeholdery w PL
   - Zła liczba tego samego placeholdera
   
2. **HTML mismatch**
   - Brakujące tagi HTML
   - Nadmiarowe tagi HTML
   - Różne tagi lub atrybuty

### ⚠️ OSTRZEŻENIA (warto sprawdzić):

3. **Długość tekstu**
   - PL jest 3x dłuższe niż EN
   - PL jest 3x krótsze niż EN

4. **Puste tłumaczenie**
   - EN ma tekst, PL jest puste

5. **EN w PL**
   - PL zawiera fragment EN (może nie przetłumaczone)

## Przykładowe workflow

### Pełny proces z walidacją:
```bash
# 1. Przetłumacz
python run.py

# 2. Zwaliduj (znajduje najnowszy plik automatycznie)
python validate_translation.py "results/$(ls -t results | head -1)/$(ls -t results/$(ls -t results | head -1) | grep translated.csv | head -1)"

# 3. Jeśli błędy - sprawdź raport
cat results/[najnowszy]/*.validation_report.txt

# 4. Jeśli OK - użyj pliku
cp results/[najnowszy]/Microsite.translated.csv /path/to/production/
```

### Walidacja tylko konkretnego pliku:
```bash
python validate_translation.py path/to/any/file.csv
```

## Integracja z CI/CD

Możesz użyć w pipeline:
```yaml
- name: Translate
  run: python run.py

- name: Validate
  run: |
    LATEST=$(ls -t results | head -1)
    python validate_translation.py "results/$LATEST/Microsite.translated.csv"

- name: Upload if valid
  if: success()
  run: upload_to_production.sh
```

## Uwagi

- ✅ Działa z CSV i XLSX
- ✅ Automatycznie wykrywa kolumny (jak translator)
- ✅ Obsługuje zagnieżdżone placeholdery `{startDate[MMM D, YYYY]}`
- ✅ Kolorowy output w terminalu (rich)
- ✅ Szczegółowy raport tekstowy
- ✅ Exit code dla automatyzacji

**Rekomendacja:** Uruchom ZAWSZE po tłumaczeniu, zanim użyjesz pliku w produkcji!

