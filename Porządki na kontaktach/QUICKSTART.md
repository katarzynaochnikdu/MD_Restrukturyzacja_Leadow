# 🚀 Zoho Data Cleanup - Szybki Start

## ⚠ WAŻNE - PRZED PIERWSZYM URUCHOMIENIEM

**Zawsze rozpoczynaj od testu na 1 rekordzie!**

## 📋 Krok po kroku

### 0. Web GUI (NAJŁATWIEJSZE - NOWE!) 🌐

```bash
# Instalacja Flask (jednorazowo)
pip install flask

# Uruchom
python cleanup_zoho.py
# Wybierz opcję [2] Web GUI
```

**Interfejs graficzny automatycznie otworzy się w przeglądarce!**

Program zapyta o:
1. Dry-run czy produkcja
2. Limit par duplikatów (domyślnie: 10)

**Następnie zobaczysz:**
- 🎨 Piękny interfejs graficzny
- 📊 Master vs Slave (scoring, pola)
- ✏️ Edytor pól (wybierz Master/Slave/Własna wartość)
- 🔄 Progress bar
- ⚡ Auto-scalanie prostych przypadków

**To jest NAJBEZPIECZNIEJSZY i NAJŁATWIEJSZY sposób!**

### 0b. Interaktywne menu CLI (klasyczne)

### 1. Konfiguracja (jednorazowo)

**OPCJA A: Zmienne środowiskowe (ZALECANE)**

Windows PowerShell:
```powershell
$env:ZOHO_MEDIDESK_CLIENT_ID="twoj_client_id"
$env:ZOHO_MEDIDESK_CLIENT_SECRET="twoj_client_secret"
$env:ZOHO_MEDIDESK_REFRESH_TOKEN="twoj_refresh_token"
```

**OPCJA B: Plik config.json**

```bash
# Skopiuj przykładową konfigurację
cp config.example.json Referencyjne/config.json

# Edytuj i uzupełnij credentials
notepad Referencyjne\config.json
```

W pliku `Referencyjne/config.json` wpisz:
```json
{
  "zoho": {
    "client_id": "TWOJ_CLIENT_ID",
    "client_secret": "TWOJ_CLIENT_SECRET", 
    "refresh_token": "TWOJ_REFRESH_TOKEN"
  }
}
```

💡 **Tip:** Zmienne środowiskowe są bezpieczniejsze (nie trafiają do git)

### 2. Test na 1 rekordzie (Accounts)

```bash
python cleanup_zoho.py --mode accounts --dry-run --limit 1
```

✅ Co sprawdzić po teście:
- Folder `run_YYYYMMDD_HHMMSS/` został utworzony
- Plik `cleanup.log` zawiera szczegółowe logi
- Plik `backup_accounts.json` zawiera backup
- Brak błędów w logach

### 3. Test na 1 rekordzie (Contacts)

```bash
python cleanup_zoho.py --mode contacts --dry-run --limit 1
```

✅ Co sprawdzić po teście:
- Folder `run_YYYYMMDD_HHMMSS/` został utworzony
- Plik `cleanup.log` zawiera szczegółowe logi
- Plik `backup_contacts.json` zawiera backup
- Plik `contacts_cleaned.csv` zawiera raport
- Brak błędów w logach

### 4. Test na 10 rekordach

```bash
# Accounts
python cleanup_zoho.py --mode accounts --dry-run --limit 10

# Contacts
python cleanup_zoho.py --mode contacts --dry-run --limit 10
```

✅ Przejrzyj raporty i logi - czy wszystko działa jak powinno?

### 5. Uruchomienie produkcyjne (z zapisem)

⚠️ **UWAGA: To zapisze zmiany do Zoho!**

```bash
# Accounts - CAŁA BAZA (z potwierdzeniem każdego scalenia)
python cleanup_zoho.py --mode accounts --apply

# Contacts - CAŁA BAZA
python cleanup_zoho.py --mode contacts --apply
```

**💡 Tryb interaktywny w --apply dla Accounts:**

Przed każdym scaleniem firm zobaczysz:
```
⚠️  SCALANIE FIRM (GRUPA 1/15)
Master (zachowaj): FIRMA A (score: 25)
Slave (scal i usuń): FIRMA A (score: 10)
Scalić te firmy? [T/n/p(omiń)/q(quit)]:
```

- **T** - Tak, scalić
- **N** - Nie, pomiń
- **Q** - Przerwij cały proces

**Maksymalne bezpieczeństwo!** 🛡️

## 📊 Interpretacja wyników

### Tryb Accounts

Sprawdź plik `run_*/accounts_merged.csv`:

| Master_Score | Slave_Score | Merged_Fields | Tags_Transferred | Deleted | Success |
|--------------|-------------|---------------|------------------|---------|---------|
| 25           | 10          | 5             | 2                | True    | True    |

- ✅ `Merged_Fields` - liczba pól skopiowanych ze slave do master
- ✅ `Tags_Transferred` - liczba tagów przeniesionych ze slave do master
- ✅ `Deleted=True` - slave został usunięty (scoring < 5, brak powiązań)
- ⚠️ `Deleted=False` - slave pozostaje (scoring >= 5 lub ma powiązania)
- ✅ `Success=True` - scalenie powiodło się
- ❌ `Success=False` - sprawdź kolumnę `Error`

### Tryb Contacts

Sprawdź pliki:
- `contacts_cleaned.csv` - wyczyszczone kontakty
- `contacts_manual_review.csv` - wymaga ręcznej weryfikacji

**Manual review:** Kontakty z wieloma firmami dla tej samej domeny.  
**Akcja:** Ręcznie przypisz właściwą firmę w Zoho.

## 🐛 Problemy?

### Token wygasł
```
Błąd: INVALID_TOKEN
```
**Rozwiązanie:**
```bash
del .zoho_token_cache.json
```

### Brak credentials
```
RuntimeError: Brak credentials w config.json
```
**Rozwiązanie:** Ustaw zmienne środowiskowe:
```powershell
$env:ZOHO_MEDIDESK_CLIENT_ID="..."
$env:ZOHO_MEDIDESK_CLIENT_SECRET="..."
$env:ZOHO_MEDIDESK_REFRESH_TOKEN="..."
```

### Brak uprawnień
```
Błąd: INSUFFICIENT_PERMISSIONS
```
**Rozwiązanie:** Sprawdź czy refresh token ma zakres:
- `ZohoCRM.modules.ALL`
- `ZohoCRM.settings.ALL`

### Inne błędy
Sprawdź `cleanup.log` w folderze `run_*/`

## 💡 Wskazówki

1. **Zawsze** rozpoczynaj od `--dry-run`
2. **Zawsze** testuj na małej próbce (`--limit 1` lub `--limit 10`)
3. **Zawsze** przeglądaj logi przed uruchomieniem produkcyjnym
4. **Backupy** są automatyczne - znajdziesz je w folderach `run_*/`
5. **Token** jest cache'owany - nie musisz go generować za każdym razem

## 📞 Pomoc

Szczegółowa dokumentacja: [README.md](README.md)

---

**Powodzenia!** 🎉

