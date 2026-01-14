# Sprawdzanie Duplikatów Kontaktów w Zoho CRM

Narzędzie w Pythonie, które przed dodaniem nowych kontaktów do Zoho CRM
sprawdza, czy w bazie istnieją potencjalne duplikaty.

## Funkcjonalność

- obsługa plików XLS/XLSX/CSV (separator wykrywany automatycznie)
- interaktywne mapowanie kolumn (Imię, Nazwisko, Email, Telefon, Firma, NIP)
- wyszukiwanie duplikatów na podstawie logiki Deluge (kombinacje pól)
- scoring jakości znalezionych rekordów i wybór najlepszego
- zwracanie pełnych danych istniejącego kontaktu (emaile, telefony, firma)
- logowanie procesu i zapisywanie wyników w folderze `wyniki/<timestamp>/`

## Wymagania

- Python 3.11+
- Dostęp do API Zoho (client_id, client_secret, refresh_token)

## Instalacja

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy config.json.example config.json
```

Uzupełnij `config.json` prawdziwymi danymi OAuth Zoho.

## Uruchomienie

```bash
python check_contacts.py
```

Program można uruchomić z dowolnego katalogu – automatycznie znajdzie plik
`config.json` znajdujący się obok skryptu. Podaj ścieżkę do pliku z kontaktami
(można przeciągnąć plik do terminala).

Po zakończeniu działania otrzymasz folder `wyniki/<timestamp>/` zawierający:

- `input.*` – kopię pliku wejściowego
- `wyniki.*` – plik wynikowy
- `process.log` – log procesu

## Struktura wyników

Plik wynikowy zawiera następujące sekcje kolumn:

1. Kolumny wejściowe wykorzystane do weryfikacji (Imię, Nazwisko, Email, Telefon, Firma, NIP)
2. Kolumny statusowe (`Status`, `Zoho_Contact_ID`, `Liczba_Duplikatow`, `Duplikat_Scoring`, `Wszystkie_Duplikaty_ID`)
3. Szczegóły kontaktu znalezionego w Zoho (`Zoho_...`)
4. Pozostałe kolumny z pliku źródłowego

## Licencja

Projekt wewnętrzny Medidesk.
