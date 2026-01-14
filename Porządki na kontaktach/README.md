# Zoho Data Cleanup

System czyszczenia i deduplikacji danych w Zoho CRM dla modułów **Accounts** (firmy) i **Contacts** (kontakty).

## ⚠ UWAGA

**To jest kod o krytycznym wpływie na system!**

- Zawsze rozpoczynaj od **trybu dry-run** na **1 rekordzie**
- Nie uruchamiaj bez uprzedniej analizy wyników
- Każde uruchomienie tworzy automatyczny backup

## 🚀 Instalacja

### 1. Wymagania

- Python 3.9 lub nowszy
- Brak zewnętrznych zależności (tylko stdlib)

### 2. Konfiguracja credentials

**Metoda 1: Zmienne środowiskowe (ZALECANE - bezpieczniejsze)**

Windows (PowerShell):
```powershell
$env:ZOHO_MEDIDESK_CLIENT_ID="twoj_client_id"
$env:ZOHO_MEDIDESK_CLIENT_SECRET="twoj_client_secret"
$env:ZOHO_MEDIDESK_REFRESH_TOKEN="twoj_refresh_token"
```

Windows (CMD):
```cmd
set ZOHO_MEDIDESK_CLIENT_ID=twoj_client_id
set ZOHO_MEDIDESK_CLIENT_SECRET=twoj_client_secret
set ZOHO_MEDIDESK_REFRESH_TOKEN=twoj_refresh_token
```

Linux/Mac:
```bash
export ZOHO_MEDIDESK_CLIENT_ID="twoj_client_id"
export ZOHO_MEDIDESK_CLIENT_SECRET="twoj_client_secret"
export ZOHO_MEDIDESK_REFRESH_TOKEN="twoj_refresh_token"
```

**Metoda 2: Plik config.json (fallback)**

Skopiuj przykładową konfigurację i uzupełnij credentials:

```bash
cp config.example.json Referencyjne/config.json
```

Edytuj `Referencyjne/config.json`:

```json
{
  "zoho": {
    "client_id": "TWOJ_CLIENT_ID",
    "client_secret": "TWOJ_CLIENT_SECRET",
    "refresh_token": "TWOJ_REFRESH_TOKEN"
  }
}
```

**Priorytet:** Zmienne środowiskowe mają pierwszeństwo przed config.json

**Jak uzyskać credentials:**
1. Przejdź do [Zoho API Console](https://api-console.zoho.eu/)
2. Utwórz Self Client (Server-based Applications)
3. Wygeneruj refresh token z zakresem: `ZohoCRM.modules.ALL,ZohoCRM.settings.ALL`

## ⚡ Optymalizacja dla dużych baz

**Dla baz z dziesiątkami tysięcy rekordów**, skrypt jest zoptymalizowany:
- **Accounts**: Scoring tylko dla firm z duplikatami (zamiast wszystkich 50k)
- **Contacts**: Progress bar co 10% (zamiast każdego rekordu)
- **Oszczędność:** ~99% czasu (z 69 godzin do ~50 minut dla 50k firm)

📖 **Szczegóły:** [OPTIMIZATION.md](OPTIMIZATION.md)

## 📖 Użycie

### 🌐 Web GUI w przeglądarce (NAJŁATWIEJSZE - NOWE!)

**Graficzny interfejs do scalania duplikatów:**

```bash
python cleanup_zoho.py
# Wybierz opcję [2] Web GUI

# LUB bezpośrednio:
python cleanup_zoho.py --gui
```

**Co oferuje Web GUI:**
- ✅ Piękny interfejs graficzny (Tailwind CSS)
- ✅ Edytor pól (dropdown: Master/Slave/Własna wartość)
- ✅ Preview operacji przed scaleniem
- ✅ Automatyczne wykrywanie "prostych" przypadków (slave score < 5)
- ✅ Możliwość auto-scalenia prostych przypadków jednym klikiem
- ✅ Potwierdzenie każdego scalenia
- ✅ Postęp w czasie rzeczywistym

**Wymagania:**
```bash
pip install flask
```

### Interaktywne menu (CLI - KLASYCZNE)

Po prostu uruchom bez argumentów:

```bash
python cleanup_zoho.py
```

Program wyświetli menu i zapyta o:
1. **Tryb działania** (accounts/contacts)
2. **Zapisywanie zmian** (dry-run/apply)
3. **Limit rekordów** (domyślnie: 1)

**Przykład:**
```
================================================================================
ZOHO DATA CLEANUP - KONFIGURACJA
================================================================================

1. TRYB DZIAŁANIA:
   a) accounts - czyszczenie i scalanie firm
   b) contacts - czyszczenie kontaktów (emaile/telefony/firmy)

Wybierz tryb [a/b]: a
✓ Wybrano: accounts

2. ZAPISYWANIE ZMIAN:
   a) dry-run - TYLKO ANALIZA (bez zmian w Zoho) [ZALECANE dla testów]
   b) apply - FAKTYCZNE ZMIANY w Zoho ⚠️

Wybierz [a/b] (domyślnie: a): a
✓ Wybrano: dry-run (bez zmian)

3. LIMIT REKORDÓW:
   0 - wszystkie rekordy (cała baza)
   1 - ZALECANE dla pierwszego testu
   10 - ZALECANE dla drugiego testu
   N - dowolna liczba rekordów

Podaj limit (domyślnie: 1): 1
✓ Wybrano: 1

================================================================================
PODSUMOWANIE KONFIGURACJI:
================================================================================
Tryb:          accounts
Dry-run:       TAK (bez zmian)
Limit:         1
================================================================================

Kontynuować? [T/n]:
```

### Tryb Accounts (czyszczenie firm) - argumenty CLI

```bash
# TEST na 1 rekordzie (dry-run)
python cleanup_zoho.py --mode accounts --dry-run --limit 1

# TEST na 10 rekordach (dry-run)
python cleanup_zoho.py --mode accounts --dry-run --limit 10

# PRODUKCJA - wszystkie rekordy (z zapisem + POTWIERDZENIE KAŻDEGO SCALENIA)
python cleanup_zoho.py --mode accounts --apply
```

**⚠️ UWAGA:** W trybie `--apply` skrypt będzie pytał o potwierdzenie **PRZED KAŻDYM SCALENIEM**:

```
================================================================================
⚠️  SCALANIE FIRM (GRUPA 1/15)
================================================================================
Master (zachowaj): FIRMA A (ID: 123, score: 25)
Slave (scal i usuń): FIRMA A (ID: 456, score: 10)
================================================================================
Operacje które zostaną wykonane:
  1. Skopiowanie pustych pól ze Slave do Master
  2. Przeniesienie powiązań (Contacts, Leads, Deals, etc.)
  3. Przeniesienie tagów
  4. Usunięcie Slave (jeśli scoring < 5 i brak powiązań)
================================================================================
Scalić te firmy? [T/n/p(omiń)/q(quit)]:
```

**Odpowiedzi:**
- `T` / `tak` / `y` / Enter - Scalić (domyślnie)
- `N` / `nie` - Pomiń to scalenie
- `P` / `pomij` - Pomiń to scalenie
- `Q` / `quit` - Przerwij cały proces

**Co robi tryb Accounts:**
- Oblicza scoring firm (wypełnienie pól + powiązania)
- Identyfikuje duplikaty po nazwie i/lub NIP
- Scala duplikaty (kopiuje dane, przenosi powiązania, przenosi tagi)
- Usuwa firmy o niskim scoringu (< 5) bez powiązań

**Przenosi:**
- Dane (wypełnia puste pola z deduplikacją telefonów/emaili)
- Powiązania (Contacts, Leads, Marketing_Leads, EDU_Leads, Klienci, Deals, Notes, Tasks, Calls, Events)
- **Tagi** (z deduplikacją case-insensitive)

### Tryb Contacts (czyszczenie kontaktów) - argumenty CLI

```bash
# TEST na 1 rekordzie (dry-run)
python cleanup_zoho.py --mode contacts --dry-run --limit 1

# TEST na 10 rekordach (dry-run)
python cleanup_zoho.py --mode contacts --dry-run --limit 10

# PRODUKCJA - wszystkie rekordy (z zapisem)
python cleanup_zoho.py --mode contacts --apply
```

**Co robi tryb Contacts:**
- Deduplikuje emaile (3 sloty: Email, Secondary_Email, Email_3)
- Deduplikuje telefony komórkowe (3 sloty: Mobile, Home_Phone, Telefon_komorkowy_3)
- Deduplikuje telefony stacjonarne (3 sloty: Phone, Other_Phone, Telefon_stacjonarny_3)
- Formatuje telefony (mobile: XXX XXX XXX, stacjonarny: XX XXX XX XX)
- Przypisuje firmy po domenach emaili (tylko niepubliczne domeny)
- Weryfikuje czy przypisane firmy istnieją

## 📊 Outputy

Każde uruchomienie tworzy folder `run_YYYYMMDD_HHMMSS/` z:

### Wspólne dla obu trybów:
- `cleanup.log` - Szczegółowe logi z JSONami (API request/response)
- `backup_*.json` - Pełny backup danych przed zmianami

### Tryb Accounts:
- `accounts_merged.csv` - Raport scalonych firm

| Master_ID | Master_Name | Master_Score | Slave_ID | Slave_Name | Slave_Score | Merged_Fields | Tags_Transferred | Deleted |
|-----------|-------------|--------------|----------|------------|-------------|---------------|------------------|---------|
| 123       | Firma A     | 25           | 456      | Firma A    | 10          | 5             | 2                | True    |

### Tryb Contacts:
- `contacts_cleaned.csv` - Raport wyczyszczonych kontaktów

| Contact_ID | Full_Name   | Email_Dups | Mobile_Dups | Landline_Dups | Company_Assigned | Changes |
|------------|-------------|------------|-------------|---------------|------------------|---------|
| 789        | Jan Kowalski| 2          | 1           | 0             | Firma X          | Email: 3→1 |

- `contacts_manual_review.csv` - Kontakty wymagające ręcznej weryfikacji (wiele firm dla 1 domeny)

| Contact_ID | Full_Name    | Email_Domain  | Matching_Companies                    |
|------------|--------------|---------------|---------------------------------------|
| 789        | Anna Nowak   | medidesk.com  | MediDesk Sp. z o.o. (ID: 123); MediDesk Oddział (ID: 456) |

## 🔧 Architektura

```
cleanup_zoho.py              # Główny skrypt (CLI, workflow, raporty)
├── modules/
│   ├── token_manager.py     # Zarządzanie tokenem (cache, auto-refresh)
│   ├── zoho_api_client.py   # Klient API Zoho (rozszerzony)
│   ├── account_scorer.py    # Scoring firm (przepisany z Deluge)
│   ├── account_merger.py    # Scalanie duplikatów firm
│   ├── contact_cleaner.py   # Deduplikacja emaili/telefonów
│   ├── company_assigner.py  # Przypisywanie firm po domenach
│   ├── data_sanitizer.py    # Sanityzacja danych
│   └── phone_formatter.py   # Formatowanie telefonów
```

## 📋 Logika Scoringu Firm

### Komponenty scoringu:

1. **AccountScoreDetale** - Liczba wypełnionych pól (basic info, adresy, telefony, emaile)
2. **AccountScorePowiazaniaModuly** - Liczba modułów z powiązaniami
3. **AccountScorePowiazaniaRekordyModulow** - Liczba powiązanych rekordów
4. **AccountScoreFirmyPowiazane** - Liczba powiązań rodzic/potomkowie
5. **AccountScoreKlienci** - Liczba powiązań w module Klienci (ASU/Płatnik)

**Total Score = suma wszystkich komponentów**

### Warunki usunięcia firmy:
- Scoring < 5 **ORAZ**
- Brak powiązanych kontaktów **ORAZ**
- Brak relacji parent/child z firmą docelową

## 🔐 Bezpieczeństwo (3 WARSTWY OCHRONY)

### Warstwa 1: Dry-run domyślnie
- **Dry-run domyślnie** - faktyczne zmiany wymagają `--apply`
- Dry-run pokazuje CO ZOSTANIE ZROBIONE bez faktycznych zmian

### Warstwa 2: Automatyczny backup
- **Automatyczny backup** przed każdym uruchomieniem (pełne JSONy)
- Backup zapisany w `run_*/backup_*.json`

### Warstwa 3: Interaktywne potwierdzenie (--apply dla Accounts)
- **Potwierdzenie KAŻDEGO scalenia** w trybie produkcyjnym
- Możliwość pominięcia lub przerwania procesu
- Widoczne: Master vs Slave, scoring, operacje

### Dodatkowo:
- **Rate limiting** - 0.5s między requestami do API
- **Retry logic** - 3 próby przy timeout
- **Atomowość** - błąd przy jednym rekordzie nie przerywa całości
- **Szczegółowe logi** - każde API request/response z pełnymi JSONami
- **Filtrowanie parent/child/siblings** - nie scala firm powiązanych rodzinnie

## 🏷️ Ignorowane domeny (publiczne)

Domeny emaili pomijane przy przypisywaniu firm:

- Gmail: `gmail.com`, `googlemail.com`
- Microsoft: `outlook.com`, `hotmail.com`, `live.com`, `msn.com`
- Yahoo: `yahoo.com`, `yahoo.pl`, `ymail.com`
- Polskie: `wp.pl`, `o2.pl`, `onet.pl`, `interia.pl`, `tlen.pl`, `op.pl`, `poczta.fm`
- Inne: `icloud.com`, `protonmail.com`, `zoho.com`, `aol.com`

## 🐛 Rozwiązywanie problemów

### Token wygasł
```
Błąd: Błąd HTTP 401: INVALID_TOKEN
```
**Rozwiązanie:** Usuń plik `.zoho_token_cache.json` - zostanie automatycznie odświeżony

### Brak uprawnień
```
Błąd: Błąd HTTP 403: INSUFFICIENT_PERMISSIONS
```
**Rozwiązanie:** Sprawdź czy refresh token ma zakres `ZohoCRM.modules.ALL,ZohoCRM.settings.ALL`

### Rate limiting
```
Błąd: Błąd HTTP 429: TOO_MANY_REQUESTS
```
**Rozwiązanie:** Skrypt ma wbudowany rate limiting (0.5s), ale jeśli problem się powtarza, dodaj opóźnienie

## 📝 Przykładowy workflow

```bash
# 1. TEST na 1 rekordzie
python cleanup_zoho.py --mode accounts --dry-run --limit 1

# 2. Przejrzyj logi i raporty w run_YYYYMMDD_HHMMSS/
#    - cleanup.log - szczegółowe logi
#    - accounts_merged.csv - raport scalonych firm

# 3. TEST na 10 rekordach
python cleanup_zoho.py --mode accounts --dry-run --limit 10

# 4. Jeśli wszystko OK - uruchom na pełnej bazie (z zapisem)
python cleanup_zoho.py --mode accounts --apply

# 5. Powtórz dla kontaktów
python cleanup_zoho.py --mode contacts --dry-run --limit 1
python cleanup_zoho.py --mode contacts --dry-run --limit 10
python cleanup_zoho.py --mode contacts --apply
```

## ⚙️ Cache tokenu

Token jest przechowywany w pliku `.zoho_token_cache.json` i automatycznie odświeżany gdy wygaśnie (1h TTL).

**NIE DODAWAJ** tego pliku do git (już w `.gitignore`).

## 📞 Wsparcie

W razie problemów sprawdź:
1. `cleanup.log` w folderze `run_*/` - szczegółowe logi
2. Czy credentials w `Referencyjne/config.json` są poprawne
3. Czy refresh token ma odpowiednie zakresy uprawnień

---

**Autor:** AI Assistant  
**Wersja:** 1.0  
**Data:** 2025-10-31

