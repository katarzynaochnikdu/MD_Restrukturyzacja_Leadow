# Transformacja Marketing Leadów

Projekt do pobierania i przetwarzania rekordów z modułu **Marketing Leads** w Zoho CRM.

## 🚀 SZYBKI START

**Użyj głównego programu z menu:**

**Windows** - kliknij dwukrotnie:
```
START.bat
```

**Lub z terminala:**
```bash
python main_workflow.py
```

Program prowadzi krok po kroku przez cały proces:
- Pobieranie danych
- Filtrowanie
- Tworzenie leadów
- Aktualizacja statusów

Każda akcja zapisuje wyniki, które automatycznie są dostępne dla kolejnych akcji.

## 📁 Struktura projektu

```
Transformacja Marketing Leadów/
├── START.bat                        # ⭐ KLIKNIJ DWUKROTNIE (Windows) - uruchamia program
├── main_workflow.py                 # ⭐ GŁÓWNY PROGRAM - menu i przepływ pracy
├── zoho_oauth.py                    # Moduł do autoryzacji OAuth Zoho
├── refresh_zoho_access_token.py    # Moduł do odświeżania tokenów
├── fetch_marketing_leads.py         # Skrypt do pobierania rekordów
├── filter_csv.py                    # Interaktywny skrypt do filtrowania CSV/XLSX
├── create_leads_from_file.py        # Masowe tworzenie leadów z pliku
├── update_lead_status.py            # Masowa aktualizacja statusu leadów
├── update_marketing_lead_status.py  # Aktualizacja Etap_kwalifikacji_HL Marketing Leads
├── list_zoho_users.py               # Lista aktywnych użytkowników Zoho CRM
├── requirements.txt                 # Zależności Python
├── README.md                        # Ten plik
├── przykład_firmy_test.csv          # Przykładowy plik do testów
├── przykład_leady_test.csv          # Przykładowy plik do testów
├── marketing_leads_cache.pkl        # Cache pobieranych rekordów (generowany)
├── wyniki_marketing_leads/          # Folder z wynikami (generowany)
├── wyniki_filtr_*/                  # Foldery z wynikami filtrowania (generowane)
├── wyniki_create_leads/             # Folder z wynikami tworzenia leadów (generowany)
├── wyniki_update_lead_status/       # Folder z wynikami aktualizacji statusów (generowany)
└── wyniki_update_marketing_lead_status/ # Folder z raportami statusów Marketing Leads (generowany)
```

## 🚀 Instalacja

### 1. Utwórz środowisko wirtualne (opcjonalnie, ale zalecane)

```bash
python -m venv venv
venv\Scripts\activate  # Windows
```

### 2. Zainstaluj zależności

```bash
pip install -r requirements.txt
```

### 3. Skonfiguruj zmienne środowiskowe

Ustaw zmienne środowiskowe z danymi do Zoho CRM:

**Windows (PowerShell):**
```powershell
$env:ZOHO_MEDIDESK_CLIENT_ID = "twój_client_id"
$env:ZOHO_MEDIDESK_CLIENT_SECRET = "twój_client_secret"
$env:ZOHO_MEDIDESK_REFRESH_TOKEN = "twój_refresh_token"
```

**Windows (CMD):**
```cmd
set ZOHO_MEDIDESK_CLIENT_ID=twój_client_id
set ZOHO_MEDIDESK_CLIENT_SECRET=twój_client_secret
set ZOHO_MEDIDESK_REFRESH_TOKEN=twój_refresh_token
```

**Linux/Mac:**
```bash
export ZOHO_MEDIDESK_CLIENT_ID="twój_client_id"
export ZOHO_MEDIDESK_CLIENT_SECRET="twój_client_secret"
export ZOHO_MEDIDESK_REFRESH_TOKEN="twój_refresh_token"
```

## 📝 Użycie

### ⭐ Główny program: main_workflow.py - Menu i przepływ pracy

**ZALECANE DLA WIĘKSZOŚCI UŻYTKOWNIKÓW**

Interaktywny program łączący wszystkie skrypty w jeden logiczny przepływ:

```bash
python main_workflow.py
```

#### Funkcje menu:

**AKCJE:**
1. **Pobierz Marketing Leads** - z cache lub świeże z API
2. **Filtruj plik** - automatycznie podpowiada ostatni plik
3. **Utwórz leady** - z automatycznym wyborem pliku źródłowego
4. **Zaktualizuj statusy** - masowa zmiana statusu leadów
5. **Zaktualizuj status Marketing Leads** - ustaw etap kwalifikacji

**NARZĘDZIA:**
6. **Historia akcji** - pokaż co zostało zrobione w sesji
7. **Otwórz folder** - szybki dostęp do wyników

**POMOC:**
8. **Scenariusze** - przykłady użycia krok po kroku
9. **Informacje** - o projekcie i plikach

#### Zalety głównego programu:

✅ **Łatwe w użyciu** - menu prowadzi krok po kroku
✅ **Automatyczne podpowiedzi** - pamięta ostatni użyty plik
✅ **Historia akcji** - widzisz co zostało zrobione
✅ **Kolorowe interfejs** - czytelne komunikaty (zielone=sukces, czerwone=błąd)
✅ **Bez zapamiętywania** - nie musisz pamiętać nazw skryptów
✅ **Ciągłość pracy** - wynik jednej akcji to wejście dla następnej

#### Przykładowy przepływ w menu:

```
1. Wybierz akcję "1" → Pobierz Marketing Leads
   ✓ Zapisano: wyniki_marketing_leads/marketing_leads_20260112.csv

2. Wybierz akcję "2" → Filtruj
   → Program automatycznie podpowie ostatni plik
   → Wykonaj filtrowanie (np. po statusie)
   ✓ Zapisano: wyniki_filtr_*/filtered_20260112.csv

3. Wybierz akcję "3" → Utwórz leady
   → Program automatycznie podpowie przefiltrowany plik
   → Wybierz kolumny, status, tryb testowy
   ✓ Utworzono 15 leadów

4. Wybierz akcję "5" → Zaktualizuj status Marketing Leads
   → Ustaw `Etap_kwalifikacji_HL` (np. "nowy")
   ✓ Sprawdź: wyniki_update_marketing_lead_status/
```

---

### Skrypt 1: fetch_marketing_leads.py - Pobieranie danych

**Użyj gdy chcesz uruchomić tylko pobieranie bez menu.**

Pobranie wszystkich rekordów Marketing Leads:

```bash
python fetch_marketing_leads.py
```

Skrypt:
- Pobierze wszystkie rekordy z modułu Marketing Leads
- Zapisze je do cache'a (dla przyspieszenia kolejnych uruchomień)
- Wygeneruje 2 pliki: CSV i XLSX w folderze `wyniki_marketing_leads/`

### Opcje wiersza poleceń

```bash
# Określ własny folder na wyniki
python fetch_marketing_leads.py --output-dir moje_wyniki

# Wyłącz cache (wymuś pobieranie z API)
python fetch_marketing_leads.py --no-cache

# Określ własną ścieżkę do cache
python fetch_marketing_leads.py --cache-file moj_cache.pkl

# Połączenie opcji
python fetch_marketing_leads.py --output-dir dane_2026 --no-cache
```

### Pomoc

```bash
python fetch_marketing_leads.py --help
```

---

### Skrypt 2: create_leads_from_file.py - Masowe tworzenie leadów

Skrypt do tworzenia wielu leadów w module **Leads** na podstawie pliku CSV/XLSX z ID firm (Accounts).

#### Użycie:

**Metoda 1: Przeciągnij plik do terminala**
```bash
python create_leads_from_file.py
# Następnie przeciągnij plik CSV/XLSX do terminala
```

**Metoda 2: Podaj ścieżkę jako argument**
```bash
python create_leads_from_file.py "dane/firmy_do_leadow.csv"
```

#### Wymagana struktura pliku:

Plik musi zawierać co najmniej:
- **Kolumnę z ID firm** (Account ID z modułu Accounts)
- Opcjonalnie: **Kolumnę z ID kontaktów** (Contact ID z modułu Contacts)

#### Funkcje:

1. **Wybór kolumn** - wskaż kolumnę z ID firm (i opcjonalnie kontaktów)
   - ID firmy jest **obowiązkowe** (z tego pobierana jest nazwa leada)
   - ID kontaktu jest **opcjonalne** (tylko przypisuje kontakt do leada)
2. **Wybór Lead Status** - wybierz stage dla tworzonych leadów
3. **Wybór Ownera** - po pobraniu tokena skrypt podpowiada aktywnych użytkowników lub możesz najpierw uruchomić `python list_zoho_users.py` i wkleić wybrane ID
4. **Tryb testowy** - leady z prefiksem `[TEST]` w nazwie
5. **Automatyczne pobieranie nazw firm** - z API Zoho
6. **Weryfikacja kontaktów** - sprawdza czy podane ID kontaktów istnieją
7. **Zapis wyników** - raport w CSV i XLSX z sukcesami i błędami

#### Tworzone pola w leadzie:

- `Last_Name` - nazwa leada (ZAWSZE z nazwy firmy + opcjonalnie `[TEST]`)
- `Company` - nazwa firmy (tekstowo)
- `Firma_w_bazie` - lookup do Account (ID)
- `Kontakt_w_bazie` - lookup do Contact (ID) - opcjonalne
- `Lead_Status` - wybrany stage

**Przykład:** 
- Firma: "Przychodnia Medyczna SP. Z O.O."
- Tryb testowy: TAK
- Nazwa leada: `[TEST] Przychodnia Medyczna SP. Z O.O.`
- Owner: wpisz ID użytkownika (np. Mateusz Podlewski) uzyskane z `python list_zoho_users.py`

#### Przykładowy przepływ:

```
1. Uruchom: python create_leads_from_file.py dane.csv
2. Wybierz kolumnę z ID firm (np. "1" lub "Account_ID")
3. Wybierz czy plik ma kolumnę z ID kontaktów (t/n)
4. Wybierz Lead Status (np. "2" dla "Dzwonienie")
5. Wybierz tryb testowy (t/n)
6. Potwierdź utworzenie leadów
7. Wyniki zapisane w: wyniki_create_leads/
```

---

### Skrypt 3: update_lead_status.py - Aktualizacja statusu leadów

Skrypt do masowej zmiany statusu leadów w module **Leads** z pliku CSV/XLSX.

#### Użycie:

**Metoda 1: Przeciągnij plik do terminala**
```bash
python update_lead_status.py
# Następnie przeciągnij plik CSV/XLSX do terminala
```

**Metoda 2: Podaj ścieżkę jako argument**
```bash
python update_lead_status.py "dane/leady_do_aktualizacji.csv"
```

#### Wymagana struktura pliku:

Plik musi zawierać:
- **Kolumnę z ID leadów** (Lead ID z modułu Leads)

#### Funkcje:

1. **Wybór kolumny** - wskaż kolumnę z ID leadów
2. **Wybór nowego statusu** - z listy dostępnych Lead Status
3. **Weryfikacja** - sprawdzenie czy lead istnieje przed aktualizacją
4. **Zapis wyników** - raport w CSV i XLSX z:
   - Starym statusem
   - Nowym statusem
   - Sukcesami i błędami

#### Dostępne statusy:

1. Lead
2. Dzwonienie
3. Nurturing
4. Umówione spotkanie
5. Zakwalifikowane do sales
6. Zdyskwalifikowany
7. Call I
8. Dodzwoniono się
9. Kontakt w przyszłości
10. Leady przegrane
11. Podjęto próbę kontaktu
12. Skontaktowano się

#### Przykładowy przepływ:

```
1. Uruchom: python update_lead_status.py leady.csv
2. Wybierz kolumnę z ID leadów (np. "1" lub "Lead_ID")
3. Wybierz nowy status (np. "8" dla "Dodzwoniono się")
4. Potwierdź aktualizację statusów
5. Wyniki zapisane w: wyniki_update_lead_status/
```

---

### Skrypt 4: update_marketing_lead_status.py - Aktualizacja statusu Marketing Leads

Skrypt do masowej zmiany pola **Etap_kwalifikacji_HL** w module **Marketing_Leads** na podstawie pliku CSV/XLSX.

#### Użycie:

**Metoda 1: Przeciągnij plik do terminala**
```bash
python update_marketing_lead_status.py
# Następnie przeciągnij plik CSV/XLSX do terminala
```

**Metoda 2: Podaj ścieżkę jako argument**
```bash
python update_marketing_lead_status.py "dane/marketing_leads_do_aktualizacji.csv"
```

#### Wymagana struktura pliku:

Plik musi zawierać co najmniej:
- **Kolumnę z ID Marketing Leadów** (pole `id` z modułu Marketing_Leads)

#### Funkcje:

1. **Wybór kolumny** - wskaż kolumnę z ID marketing leadów (np. `id`)
2. **Wybór nowego etapu** - wybierz wartość `Etap_kwalifikacji_HL`
3. **Aktualizacja przez API** - wykonuje `PUT` w module Marketing_Leads
4. **Zapis wyników** - raport w CSV i XLSX z sukcesami/błędami (`wyniki_update_marketing_lead_status/`)

#### Dostępne etapy:

1. `-None-`
2. `odpad (odpadek)`
3. `nowy`
4. `przetworzony`
5. `informacja (w trakcie przetwarzania)`
6. `po analizie danych (weryfikacja powiązań)`
7. `utworzony Lead/Deal (Utwórz rekord)`
8. `informacja czy akcja ? (Informacja czy Akcja)`
9. `akcja (Akcja)`

#### Przykładowy przepływ:

```
1. Uruchom: python update_marketing_lead_status.py marketing_leads.csv
2. Wybierz kolumnę z ID marketing leadów (np. "1" lub "id")
3. Wybierz etap (np. "3" → nowy)
4. Potwierdź aktualizację
5. Wyniki zapisane w: wyniki_update_marketing_lead_status/
```

---

### Skrypt 5: filter_csv.py - Filtrowanie danych

Interaktywny skrypt do filtrowania pobranych danych lub dowolnych plików CSV/XLSX.

#### Użycie:

**Metoda 1: Przeciągnij plik do terminala**
```bash
python filter_csv.py
# Następnie przeciągnij plik CSV/XLSX do terminala
```

**Metoda 2: Podaj ścieżkę jako argument**
```bash
python filter_csv.py "wyniki_marketing_leads\marketing_leads_20260112.csv"
```

#### Funkcje:

1. **Przeglądanie kolumn** - wyświetla wszystkie kolumny z informacją o zapełnieniu
2. **Unikalne wartości** - pokazuje wszystkie unikalne wartości w wybranej kolumnie (dla select-ów)
3. **Filtrowanie "zawiera"** - filtruje wiersze gdzie kolumna zawiera podany tekst
4. **Wielokrotne filtry** - możliwość dodania wielu filtrów po kolei
5. **Cofanie filtrów** - jeśli filtr zwróci 0 wyników, można go cofnąć
6. **Zapis do CSV + XLSX** - wyniki zapisywane w obu formatach

#### Przykładowy przepływ:

```
1. Uruchom: python filter_csv.py
2. Podaj ścieżkę do pliku (lub przeciągnij)
3. Wybierz "1" → Zobacz dostępne kolumny
4. Wybierz "2" → Zobacz unikalne wartości w kolumnie (np. "Status")
5. Wybierz "3" → Dodaj filtr:
   - Wybierz kolumnę (np. numer "5" lub nazwa "Status")
   - Wpisz tekst do wyszukania (np. "Aktywny")
6. Powtórz krok 5 dla kolejnych filtrów
7. Wybierz "4" → Zapisz wyniki

Wyniki trafią do folderu: wyniki_filtr_[nazwa_pliku]/
```

#### Uwagi:

- Filtry są **kumulatywne** - każdy kolejny zawęża wyniki
- Domyślnie filtrowanie **nie rozróżnia** wielkości liter
- Skrypt tworzy folder wynikowy na bazie nazwy pliku wejściowego
- Wyniki zawierają timestamp w nazwie pliku

## 📊 Formaty wyjściowe

Skrypt generuje 2 pliki:

1. **CSV** - `marketing_leads_YYYYMMDD_HHMMSS.csv`
   - Format tekstowy z separatorem przecinek
   - Kodowanie UTF-8 z BOM (poprawne wyświetlanie polskich znaków w Excel)
   - Zagnieżdżone pola są spłaszczane

2. **XLSX** - `marketing_leads_YYYYMMDD_HHMMSS.xlsx`
   - Format Excel
   - Wszystkie pola jako kolumny
   - Gotowy do dalszej analizy

## 🔧 Funkcje

### Cache'owanie
- Domyślnie włączone
- Przyspiesza kolejne uruchomienia (nie pobiera ponownie z API)
- Plik cache: `marketing_leads_cache.pkl`
- Wyłącz opcją `--no-cache` aby wymusić świeże dane

### Stronicowanie
- Automatyczne pobieranie wszystkich rekordów
- Używa `page_token` z API v8
- Brak limitu 10k rekordów (jak w COQL)
- Progress bar pokazuje postęp

### Obsługa błędów
- Automatyczne odświeżanie tokenów
- Retry przy timeout'ach
- Szczegółowe logi w `fetch_marketing_leads.log`

### Spłaszczanie danych
- Lookup fields: `Nazwa firmy (ID: 123456789)`
- Multi-select: wartości połączone przez `; `
- Listy: elementy połączone przez `; `

## 🔄 Przykładowy przepływ pracy

### ⭐ Z użyciem głównego programu (ZALECANE)

```bash
python main_workflow.py
```

**Scenariusz 1: Tworzenie testowych leadów**
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
Menu → Akcja 2 → Filtruj po statusie "Lead"
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

### Skrypt 6: list_zoho_users.py - Lista aktywnych użytkowników

Przydatny gdy potrzebujesz ID użytkownika (np. Mateusza Podlewskiego), aby przypisać Lead lub `Owner`.

#### Użycie:
```bash
python list_zoho_users.py
```

#### Co robi:
1. Pobiera token OAuth Zoho
2. Wypisuje `full_name`, `id`, rolę i profil każdego aktywnego użytkownika (`ActiveUsers`)
3. Identyfikuje ID konkretnego pracownika, które możesz przekazać do innych skryptów

---

### Bez głównego programu (dla zaawansowanych)

### Scenariusz 1: Tworzenie leadów z listy firm

```bash
# Krok 1: Przygotuj plik CSV z ID firm (Account ID)
# Kolumny: Account_ID, Account_Name (opcjonalnie), Contact_ID (opcjonalnie)
# 
# Przykład:
# Account_ID,Contact_ID
# 1234567890123456789,
# 9876543210987654321,1111111111111111111

# Krok 2: Opcjonalnie przefiltruj dane
python filter_csv.py lista_firm.csv
# Zapisz przefiltrowane wyniki

# Krok 3: Utwórz leady
python create_leads_from_file.py wyniki_filtr_lista_firm/przefiltrowane.csv
# Wybierz kolumnę z Account_ID (np. 1)
# Czy plik ma Contact_ID? (n jeśli nie, t jeśli tak)
# Jeśli tak - wybierz kolumnę z Contact_ID (np. 2)
# Wybierz Lead Status (np. 2 - "Dzwonienie")
# Wybierz tryb testowy: t (zalecane przy pierwszym użyciu)
# 
# Rezultat: Leady z nazwą = "[TEST] NAZWA_FIRMY"

# Krok 4: Sprawdź wyniki
# Zobacz: wyniki_create_leads/create_leads_results_*.csv
# 
# Przykład wyniku:
# lead_id,lead_name,account_name,contact_id,status
# 555...,  [TEST] Przychodnia XYZ, Przychodnia XYZ, , SUKCES
# 666...,  [TEST] Medidesk Sp. z o.o., Medidesk Sp. z o.o., 111..., SUKCES
```

### Scenariusz 2: Aktualizacja statusów istniejących leadów

```bash
# Krok 1: Pobierz leady z Zoho (użyj fetch_marketing_leads.py lub eksportuj z CRM)
python fetch_marketing_leads.py

# Krok 2: Przefiltruj leady do aktualizacji
python filter_csv.py wyniki_marketing_leads/marketing_leads_*.csv
# Filtruj np. po Lead_Status = "Lead"

# Krok 3: Zaktualizuj statusy
python update_lead_status.py wyniki_filtr_*/przefiltrowane.csv
# Wybierz kolumnę z ID leadów
# Wybierz nowy status (np. "Dzwonienie")

# Krok 4: Sprawdź wyniki
# Zobacz: wyniki_update_lead_status/update_status_results_*.csv
```

### Scenariusz 3: Testowe tworzenie leadów

```bash
# 1. Utwórz kilka testowych leadów z [TEST] w nazwie
python create_leads_from_file.py test_firmy.csv
# Włącz tryb testowy: t

# 2. Sprawdź czy leady zostały utworzone poprawnie w CRM

# 3. Jeśli wszystko OK, zaktualizuj testowe leady na inny status
python update_lead_status.py wyniki_create_leads/create_leads_results_*.csv
# Kolumna: lead_id
# Nowy status: "Zdyskwalifikowany" (aby oznaczyć jako test)

# 4. Lub usuń testowe leady ręcznie z CRM
```

## 📋 Wymagania

- Python 3.7+
- pandas >= 2.0.0
- openpyxl >= 3.1.0
- tqdm >= 4.66.0
- colorama >= 0.4.6 (dla kolorowego interfejsu w main_workflow.py)

## 🔐 Bezpieczeństwo

**NIGDY** nie commituj do repozytorium:
- Client ID
- Client Secret
- Refresh Token
- Plików cache (*.pkl)

Dodaj do `.gitignore`:
```
*.pkl
*.log
wyniki_*/
venv/
__pycache__/
```

## 📚 API Zoho CRM v8

Projekt wykorzystuje API v8 Zoho CRM:
- Endpoint: `https://www.zohoapis.eu/crm/v8/Marketing_Leads`
- Region: EU (accounts.zoho.eu)
- Dokumentacja: https://www.zoho.com/crm/developer/docs/api/v8/

## ⚠️ Uwagi i dobre praktyki

### Tworzenie leadów (create_leads_from_file.py)

- **Zawsze używaj trybu testowego** przy pierwszym uruchomieniu
- Leady testowe oznacz `[TEST]` w nazwie - łatwiej je później znaleźć
- Plik musi zawierać **prawidłowe ID firm** z modułu Accounts
- **Nazwa leada (`Last_Name`) jest ZAWSZE z nazwy firmy** - nie z kontaktu!
- ID kontaktów są opcjonalne - lead może istnieć bez przypisanego kontaktu
  - Kontakt jest tylko **przypisywany** do leada (pole `Kontakt_w_bazie`)
  - Nie wpływa na nazwę leada
- Skrypt automatycznie pobiera nazwy firm z API (wymaga czasu ~0.2s na firmę)
- Przed masowym tworzeniem przetestuj na 2-3 rekordach

### Aktualizacja statusów (update_lead_status.py)

- **Zrób backup** przed masową aktualizacją (wyeksportuj leady z CRM)
- Skrypt sprawdza czy lead istnieje przed aktualizacją
- Nie można cofnąć operacji - używaj ostrożnie
- Wyniki zawierają stary i nowy status dla weryfikacji
- Przed masową aktualizacją przetestuj na 2-3 leadach

### Filtrowanie (filter_csv.py)

- Filtry są **kumulatywne** - każdy kolejny zawęża wyniki
- Zapisuj pliki pośrednie - łatwiej wrócić do poprzedniego kroku
- Używaj opisowych nazw plików (np. `firmy_POZ_aktywne.csv`)

### Bezpieczeństwo danych

- Nigdy nie udostępniaj plików z ID rekordów publicznie
- Pliki z wynikami mogą zawierać dane wrażliwe
- Sprawdź co tworzysz/aktualizujesz przed zatwierdzeniem
- Logi zawierają szczegółowe informacje o operacjach

## 🐛 Rozwiązywanie problemów

### Błąd: "Dane nie zostały skonfigurowane"
→ Ustaw zmienne środowiskowe z danymi OAuth

### Błąd: "HTTPError 401/403"
→ Token wygasł lub nieprawidłowy. Wygeneruj nowy refresh token

### Brak rekordów / puste pliki
→ Sprawdź czy moduł Marketing_Leads istnieje w Twoim CRM
→ Sprawdź uprawnienia użytkownika w Zoho

### Błąd importu bibliotek
→ Zainstaluj zależności: `pip install -r requirements.txt`

### Błąd "Nie znaleziono firmy o ID..."
→ Sprawdź czy ID firmy jest prawidłowe (istnieje w module Accounts)
→ Sprawdź czy użytkownik ma uprawnienia do odczytu modułu Accounts

### Błąd "Nie znaleziono leada o ID..."
→ Sprawdź czy ID leada jest prawidłowe (istnieje w module Leads)
→ Sprawdź czy użytkownik ma uprawnienia do zapisu w module Leads

### Tworzenie leadów trwa bardzo długo
→ Skrypt pobiera nazwę każdej firmy/kontaktu z API (0.2s opóźnienie)
→ Dla 100 firm = ~20 sekund minimum
→ To normalne zachowanie (ograniczenia API Zoho)

### Niektóre leady się utworzyły, inne nie
→ Zobacz plik wynikowy w `wyniki_create_leads/` - zawiera szczegóły błędów
→ Najczęstsze przyczyny:
  - Nieprawidłowe ID firmy
  - Brak uprawnień do firmy
  - Przekroczony limit API (rate limiting)

### Aktualizacja statusów kończy się błędem
→ Zobacz logi w `update_lead_status.log`
→ Sprawdź czy użytkownik ma uprawnienia do edycji leadów
→ Sprawdź czy wybrany status jest dozwolony w workflow Zoho

## 📞 Wsparcie

Logi szczegółowe znajdują się w plikach:
- `fetch_marketing_leads.log` - pobieranie Marketing Leads
- `create_leads_from_file.log` - tworzenie leadów
- `update_lead_status.log` - aktualizacja statusów
