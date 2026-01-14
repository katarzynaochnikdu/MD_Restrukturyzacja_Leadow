# Instrukcja konfiguracji środowiska wirtualnego

## Wymagania

- Python 3.11
- pip (zazwyczaj dołączony do Pythona)

## Szybka konfiguracja (Windows)

### Opcja 1: Użycie skryptu PowerShell (ZALECANE w PowerShell)

1. Otwórz PowerShell w folderze projektu (kliknij prawym przyciskiem na folder → "Open in Terminal" lub "Otwórz w terminalu")
2. Jeśli po raz pierwszy uruchamiasz skrypty PowerShell, ustaw politykę wykonywania:
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```
   (Potwierdź polecenie przez `Y` lub `A`)
3. Uruchom skrypt:
   ```powershell
   .\setup_venv.ps1
   ```

### Opcja 2: Użycie skryptu .bat (CMD lub PowerShell)

**W CMD (wiersz poleceń):**
1. Otwórz wiersz poleceń (CMD) w folderze projektu
2. Uruchom:
   ```cmd
   setup_venv.bat
   ```

**W PowerShell:**
1. Otwórz PowerShell w folderze projektu
2. Uruchom przez cmd:
   ```powershell
   cmd /c setup_venv.bat
   ```
   LUB:
   ```powershell
   & .\setup_venv.bat
   ```

### Opcja 3: Ręczna konfiguracja

1. Utwórz środowisko wirtualne:
   ```cmd
   python -m venv venv
   ```

2. Aktywuj środowisko:
   - **CMD:**
     ```cmd
     venv\Scripts\activate.bat
     ```
   - **PowerShell:**
     ```powershell
     venv\Scripts\Activate.ps1
     ```

3. Zaktualizuj pip:
   ```cmd
   python -m pip install --upgrade pip
   ```

4. Zainstaluj zależności:
   ```cmd
   pip install -r requirements.txt
   ```

## Uruchamianie skryptu

1. Upewnij się, że środowisko wirtualne jest aktywowane (widać `(venv)` w wierszu poleceń)
2. Ustaw zmienne środowiskowe (jeśli potrzebne):
   ```cmd
   set ZOHO_MEDIDESK_CLIENT_ID=twoj_client_id
   set ZOHO_MEDIDESK_CLIENT_SECRET=twoj_client_secret
   set ZOHO_MEDIDESK_REFRESH_TOKEN=twoj_refresh_token
   ```
3. Uruchom skrypt:
   ```cmd
   python search_poz_companies_with_presentation.py
   ```

## Deaktywacja środowiska

Aby wyłączyć środowisko wirtualne:
```cmd
deactivate
```

## Zależności

Projekt wymaga następujących bibliotek (zawartych w `requirements.txt`):

- `pandas>=2.0.0` - do przetwarzania danych i eksportu do Excel
- `openpyxl>=3.1.0` - silnik do zapisu plików Excel (.xlsx)

## Rozwiązywanie problemów

### Python 3.11 nie został znaleziony

Jeśli masz wiele wersji Pythona, możesz użyć `py` zamiast `python`:
```cmd
py -3.11 -m venv venv
```

Sprawdź dostępne wersje:
```cmd
py -0
```

### Problemy z instalacją bibliotek

Spróbuj zaktualizować pip:
```cmd
python -m pip install --upgrade pip setuptools wheel
```

Następnie zainstaluj zależności ponownie:
```cmd
pip install -r requirements.txt
```
