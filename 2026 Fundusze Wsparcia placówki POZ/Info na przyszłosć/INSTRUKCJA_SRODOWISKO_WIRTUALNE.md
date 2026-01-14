# Instrukcja tworzenia środowiska wirtualnego Python 3.11

## 📋 Wymagania systemowe

- **System operacyjny:** Windows
- **Python:** 3.11 (zainstalowany przez Python Launcher)
- **Terminal:** PowerShell (zalecane) lub CMD

## 🔍 Sprawdzenie instalacji Pythona

### Sprawdź dostępne wersje Pythona:

```powershell
py -0
```

**Oczekiwany wynik:**
```
 -V:3.14 *        Python 3.14.2
 -V:3.13          Python 3.13 (64-bit)
 -V:3.11          Python 3.11 (64-bit)
```

Jeśli widzisz `-V:3.11`, Python 3.11 jest zainstalowany.

### Sprawdź wersję Pythona 3.11:

```powershell
py -3.11 --version
```

**Oczekiwany wynik:**
```
Python 3.11.x
```

## 📁 Lokalizacja projektu

Projekt znajduje się w:
```
C:\Users\kochn\.cursor\Medidesk\2026 Fundusze Wsparcia placówki POZ\Weryfikacja istnienia firm w ZOHO CRM
```

## 🚀 Metoda 1: Automatyczne tworzenie (ZALECANE)

### W PowerShell:

1. **Przejdź do folderu projektu:**
   ```powershell
   cd "C:\Users\kochn\.cursor\Medidesk\2026 Fundusze Wsparcia placówki POZ\Weryfikacja istnienia firm w ZOHO CRM"
   ```

2. **Ustaw politykę wykonywania (tylko pierwszy raz):**
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```
   Potwierdź przez `Y` lub `A`.

3. **Uruchom skrypt:**
   ```powershell
   .\setup_venv.ps1
   ```

### W CMD:

1. **Przejdź do folderu projektu:**
   ```cmd
   cd "C:\Users\kochn\.cursor\Medidesk\2026 Fundusze Wsparcia placówki POZ\Weryfikacja istnienia firm w ZOHO CRM"
   ```

2. **Uruchom skrypt:**
   ```cmd
   setup_venv.bat
   ```

## 🛠️ Metoda 2: Ręczne tworzenie środowiska

Jeśli automatyczne skrypty nie działają, wykonaj kroki ręcznie:

### Krok 1: Przejdź do folderu projektu

```powershell
cd "C:\Users\kochn\.cursor\Medidesk\2026 Fundusze Wsparcia placówki POZ\Weryfikacja istnienia firm w ZOHO CRM"
```

### Krok 2: Utwórz środowisko wirtualne

**WAŻNE:** Używamy `py -3.11` zamiast `python`, ponieważ Python jest zainstalowany przez Python Launcher.

```powershell
py -3.11 -m venv venv
```

**Alternatywnie** (jeśli `python` jest w PATH):
```powershell
python -m venv venv
```

### Krok 3: Aktywuj środowisko wirtualne

**W PowerShell:**
```powershell
venv\Scripts\Activate.ps1
```

**W CMD:**
```cmd
venv\Scripts\activate.bat
```

**Po aktywacji** zobaczysz `(venv)` na początku linii poleceń:
```
(venv) PS C:\Users\kochn\.cursor\...>
```

### Krok 4: Zaktualizuj pip

```powershell
python -m pip install --upgrade pip
```

**UWAGA:** Po aktywacji środowiska wirtualnego, polecenie `python` automatycznie wskazuje na Pythona w środowisku wirtualnym.

### Krok 5: Zainstaluj zależności

```powershell
pip install -r requirements.txt
```

**Zależności w projekcie:**
- `pandas>=2.0.0` - do przetwarzania danych i eksportu do Excel
- `openpyxl>=3.1.0` - silnik do zapisu plików Excel (.xlsx)

## ✅ Weryfikacja instalacji

Sprawdź, czy wszystko zostało zainstalowane poprawnie:

```powershell
pip list
```

Powinieneś zobaczyć:
```
Package    Version
---------- -------
openpyxl   3.x.x
pandas     2.x.x
pip        x.x.x
setuptools x.x.x
```

## 🎯 Uruchamianie skryptu

Po utworzeniu i aktywacji środowiska wirtualnego:

1. **Upewnij się, że środowisko jest aktywowane** (widzisz `(venv)` w wierszu poleceń)

2. **Ustaw zmienne środowiskowe** (jeśli potrzebne):
   ```powershell
   $env:ZOHO_MEDIDESK_CLIENT_ID="twoj_client_id"
   $env:ZOHO_MEDIDESK_CLIENT_SECRET="twoj_client_secret"
   $env:ZOHO_MEDIDESK_REFRESH_TOKEN="twoj_refresh_token"
   ```

   **W CMD:**
   ```cmd
   set ZOHO_MEDIDESK_CLIENT_ID=twoj_client_id
   set ZOHO_MEDIDESK_CLIENT_SECRET=twoj_client_secret
   set ZOHO_MEDIDESK_REFRESH_TOKEN=twoj_refresh_token
   ```

3. **Uruchom skrypt:**
   ```powershell
   python search_poz_companies_with_presentation.py
   ```

## 🔄 Deaktywacja środowiska

Aby wyłączyć środowisko wirtualne:

```powershell
deactivate
```

Po deaktywacji `(venv)` zniknie z wiersza poleceń.

## 🗑️ Usuwanie środowiska wirtualnego

Jeśli chcesz usunąć środowisko i stworzyć nowe:

1. **Deaktywuj środowisko** (jeśli jest aktywne):
   ```powershell
   deactivate
   ```

2. **Usuń folder venv:**
   ```powershell
   Remove-Item -Recurse -Force venv
   ```

3. **Utwórz nowe środowisko** (zgodnie z instrukcją powyżej)

## ⚠️ Rozwiązywanie problemów

### Problem: "Python 3.11 nie został znaleziony"

**Rozwiązanie:**
1. Sprawdź dostępne wersje: `py -0`
2. Jeśli Python 3.11 nie jest na liście, zainstaluj go z [python.org](https://www.python.org/downloads/)
3. Podczas instalacji zaznacz opcję "Add Python to PATH"

### Problem: "Cannot be loaded because running scripts is disabled"

**Rozwiązanie:**
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Problem: "pip install" kończy się błędem

**Rozwiązanie:**
1. Zaktualizuj pip:
   ```powershell
   python -m pip install --upgrade pip setuptools wheel
   ```
2. Spróbuj ponownie:
   ```powershell
   pip install -r requirements.txt
   ```

### Problem: Skrypt PowerShell zawiesza się

**Rozwiązanie:**
1. Przerwij wykonanie (Ctrl+C)
2. Użyj ręcznej metody tworzenia środowiska (Metoda 2)
3. Lub użyj skryptu `.bat` w CMD zamiast PowerShell

### Problem: Błędy kodowania znaków w PowerShell

**Rozwiązanie:**
- Użyj skryptu `.bat` w CMD zamiast PowerShell
- Lub ustaw kodowanie w PowerShell:
  ```powershell
  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
  ```

## 📝 Ważne uwagi

1. **Zawsze aktywuj środowisko wirtualne** przed uruchomieniem skryptów Python
2. **Używaj `py -3.11`** zamiast `python` jeśli Python jest zainstalowany przez launcher
3. **Po aktywacji venv**, polecenie `python` automatycznie wskazuje na Pythona w środowisku wirtualnym
4. **Folder `venv`** nie powinien być commitowany do repozytorium (dodaj do `.gitignore`)

## 🔗 Przydatne komendy

```powershell
# Sprawdź wersję Pythona w środowisku wirtualnym
python --version

# Sprawdź zainstalowane pakiety
pip list

# Zaktualizuj pakiet
pip install --upgrade nazwa_pakietu

# Usuń pakiet
pip uninstall nazwa_pakietu

# Eksportuj listę zależności (opcjonalnie)
pip freeze > requirements.txt
```

## 📚 Dodatkowe informacje

- **Dokumentacja venv:** https://docs.python.org/3/library/venv.html
- **Dokumentacja pip:** https://pip.pypa.io/
- **Python Launcher:** https://docs.python.org/3/using/windows.html#python-launcher-for-windows
