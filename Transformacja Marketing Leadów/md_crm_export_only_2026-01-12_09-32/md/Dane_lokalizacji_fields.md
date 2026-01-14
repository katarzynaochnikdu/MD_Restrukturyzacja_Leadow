# Pola Modułu: Dane_lokalizacji

## Statystyki

- **Łącznie pól:** 9
- **Pola niestandardowe:** 6
- **Pola standardowe:** 3
- **Pola formuł:** 0
- **Pola lookup:** 2
- **Pola wyboru (picklist):** 0

## Lista Pól

| Etykieta Pola (UI)                 | Nazwa API                | Typ Danych   | Pole Niestandardowe   | Wymagane   | Tylko Odczyt   | Typ Danych (Polski)   | Możliwe Wartości   | Powiązany Moduł          | Maks. Długość   | Wyrażenie Formuły   | Typ Zwracany Formuły   | Sekcja   | Wartość Domyślna   |
|:-----------------------------------|:-------------------------|:-------------|:----------------------|:-----------|:---------------|:----------------------|:-------------------|:-------------------------|:----------------|:--------------------|:-----------------------|:---------|:-------------------|
| Cecha adresu w rekordzie           | Cecha_adresu_w_rekordzie | text         | ✓                     |            |                | Tekst                 |                    |                          | 255             |                     |                        |          |                    |
| Kod pocztowy                       | Kod_pocztowy             | text         | ✓                     |            |                | Tekst                 |                    |                          | 30              |                     |                        |          |                    |
| Miejscowość                        | Miejscowosc              | text         | ✓                     |            |                | Tekst                 |                    |                          | 100             |                     |                        |          |                    |
| Nazwa lokalizacji                  | Nazwa_lokalizacji        | lookup       | ✓                     |            |                | Lookup                |                    | Accounts                 |                 |                     |                        |          |                    |
| Ulica                              | Ulica                    | text         | ✓                     |            |                | Tekst                 |                    |                          | 250             |                     |                        |          |                    |
| Województwo                        | Wojewodztwo              | text         | ✓                     |            |                | Tekst                 |                    |                          | 100             |                     |                        |          |                    |
| Czas Utworzenia                    | Created_Time             | datetime     |                       |            |                | Data i czas           |                    |                          |                 |                     |                        |          |                    |
| Czas modyfikacji                   | Modified_Time            | datetime     |                       |            |                | Data i czas           |                    |                          |                 |                     |                        |          |                    |
| Identyfikator elementu nadrzędnego | Parent_Id                | lookup       |                       |            | ✓              | Lookup                |                    | Dane_liczbowe_instalacji |                 |                     |                        |          |                    |

---

*Eksportowano z Zoho CRM API*
