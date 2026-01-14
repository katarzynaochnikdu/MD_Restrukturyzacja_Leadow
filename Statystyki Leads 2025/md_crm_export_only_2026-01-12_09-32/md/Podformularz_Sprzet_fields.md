# Pola Modułu: Podformularz_Sprzet

## Statystyki

- **Łącznie pól:** 10
- **Pola niestandardowe:** 6
- **Pola standardowe:** 4
- **Pola formuł:** 1
- **Pola lookup:** 2
- **Pola wyboru (picklist):** 0

## Lista Pól

| Etykieta Pola (UI)                 | Nazwa API                     | Typ Danych   | Pole Niestandardowe   | Wymagane   | Tylko Odczyt   | Typ Danych (Polski)   | Możliwe Wartości   | Powiązany Moduł   | Maks. Długość   | Wyrażenie Formuły   | Typ Zwracany Formuły   | Sekcja   | Wartość Domyślna   |
|:-----------------------------------|:------------------------------|:-------------|:----------------------|:-----------|:---------------|:----------------------|:-------------------|:------------------|:----------------|:--------------------|:-----------------------|:---------|:-------------------|
| Cena jednostkowa                   | List_Price_sprzet             | currency     | ✓                     |            |                | Waluta                |                    |                   |                 |                     |                        |          |                    |
| Elementy sprzętu                   | Elementy_sprzet               | lookup       | ✓                     |            |                | Lookup                |                    | Products          |                 |                     |                        |          |                    |
| Ilość                              | Ilosc_sprzet                  | integer      | ✓                     |            |                | Liczba całkowita      |                    |                   |                 |                     |                        |          |                    |
| Jednostka                          | Jednostka_sprzet              | text         | ✓                     |            |                | Tekst                 |                    |                   | 255             |                     |                        |          |                    |
| Segment wyceny                     | Segment_wyceny_sprzet         | text         | ✓                     |            |                | Tekst                 |                    |                   | 255             |                     |                        |          |                    |
| Wartość ogółem                     | Total_sprzet                  | formula      | ✓                     |            | ✓              | Formuła               |                    |                   |                 |                     | currency               |          |                    |
| Czas Utworzenia                    | Created_Time                  | datetime     |                       |            |                | Data i czas           |                    |                   |                 |                     |                        |          |                    |
| Czas modyfikacji                   | Modified_Time                 | datetime     |                       |            |                | Data i czas           |                    |                   |                 |                     |                        |          |                    |
| Identyfikator elementu nadrzędnego | Parent_Id                     | lookup       |                       |            | ✓              | Lookup                |                    | Quotes            |                 |                     |                        |          |                    |
| NR SER.                            | LinkingModule15_Serial_Number | bigint       |                       |            |                | Liczba (duża)         |                    |                   |                 |                     |                        |          |                    |

---

*Eksportowano z Zoho CRM API*
