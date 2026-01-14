# Pola Modułu: Historia_EtapPPartner

## Statystyki

- **Łącznie pól:** 14
- **Pola niestandardowe:** 4
- **Pola standardowe:** 10
- **Pola formuł:** 0
- **Pola lookup:** 1
- **Pola wyboru (picklist):** 3

## Lista Pól

| Etykieta Pola (UI)        | Nazwa API          | Typ Danych   | Pole Niestandardowe   | Wymagane   | Tylko Odczyt   | Typ Danych (Polski)   | Możliwe Wartości                                           | Powiązany Moduł   | Maks. Długość   | Wyrażenie Formuły   | Typ Zwracany Formuły   | Sekcja   | Wartość Domyślna   |
|:--------------------------|:-------------------|:-------------|:----------------------|:-----------|:---------------|:----------------------|:-----------------------------------------------------------|:------------------|:----------------|:--------------------|:-----------------------|:---------|:-------------------|
| Imie                      | Imie               | text         | ✓                     |            |                | Tekst                 |                                                            |                   | 255             |                     |                        |          |                    |
| NIP                       | NIP                | text         | ✓                     |            |                | Tekst                 |                                                            |                   | 255             |                     |                        |          |                    |
| Nazwisko                  | Nazwisko           | text         | ✓                     |            |                | Tekst                 |                                                            |                   | 255             |                     |                        |          |                    |
| Telefon komorkowy         | Telefon_komorkowy  | phone        | ✓                     |            |                | Telefon               |                                                            |                   |                 |                     |                        |          |                    |
| Adres Email               | Email              | email        |                       |            |                | Email                 |                                                            |                   |                 |                     |                        |          |                    |
| Czas modyfikacji          | Modified_Time      | datetime     |                       |            |                | Data i czas           |                                                            |                   |                 |                     |                        |          |                    |
| Czas ostatniej aktywności | Last_Activity_Time | datetime     |                       |            |                | Data i czas           |                                                            |                   |                 |                     |                        |          |                    |
| Czas trwania (dni)        | Duration_Days      | integer      |                       |            |                | Liczba całkowita      |                                                            |                   |                 |                     |                        |          |                    |
| EtapPPartner              | EtapPPartner       | picklist     |                       |            |                | Lista wyboru          | -None-; nowy; w trakcie przetwarzania; przetworzony; odpad |                   |                 |                     |                        |          |                    |
| Kurs wymiany              | Exchange_Rate      | double       |                       |            | ✓              | double                |                                                            |                   |                 |                     |                        |          |                    |
| Nazwa PPartnera           | Nazwa_PPartnera    | lookup       |                       |            |                | Lookup                |                                                            | PPartnerzy        |                 |                     |                        |          |                    |
| Przeniesiono do           | Moved_To__s        | picklist     |                       |            |                | Lista wyboru          | -None-; nowy; w trakcie przetwarzania; przetworzony; odpad |                   |                 |                     |                        |          |                    |
| Waluta                    | Currency           | picklist     |                       |            |                | Lista wyboru          | PLN; USD; EUR                                              |                   |                 |                     |                        |          |                    |
| Zmodyfikowany przez       | Modified_By        | ownerlookup  |                       |            |                | Właściciel            |                                                            |                   |                 |                     |                        |          |                    |

---

*Eksportowano z Zoho CRM API*
