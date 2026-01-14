# Pola Modułu: Sprzet_telefoniczny

## Statystyki

- **Łącznie pól:** 6
- **Pola niestandardowe:** 2
- **Pola standardowe:** 4
- **Pola formuł:** 0
- **Pola lookup:** 2
- **Pola wyboru (picklist):** 0

## Lista Pól

| Etykieta Pola (UI)                 | Nazwa API                     | Typ Danych   | Pole Niestandardowe   | Wymagane   | Tylko Odczyt   | Typ Danych (Polski)   | Możliwe Wartości   | Powiązany Moduł          | Maks. Długość   | Wyrażenie Formuły   | Typ Zwracany Formuły   | Sekcja   | Wartość Domyślna   |
|:-----------------------------------|:------------------------------|:-------------|:----------------------|:-----------|:---------------|:----------------------|:-------------------|:-------------------------|:----------------|:--------------------|:-----------------------|:---------|:-------------------|
| Ilość                              | Ilosc                         | integer      | ✓                     |            |                | Liczba całkowita      |                    |                          |                 |                     |                        |          |                    |
| Nazwa urządzenia                   | Nazwa_urzadzenia              | lookup       | ✓                     |            |                | Lookup                |                    | Products                 |                 |                     |                        |          |                    |
| Czas Utworzenia                    | Created_Time                  | datetime     |                       |            |                | Data i czas           |                    |                          |                 |                     |                        |          |                    |
| Czas modyfikacji                   | Modified_Time                 | datetime     |                       |            |                | Data i czas           |                    |                          |                 |                     |                        |          |                    |
| Identyfikator elementu nadrzędnego | Parent_Id                     | lookup       |                       |            | ✓              | Lookup                |                    | Dane_liczbowe_instalacji |                 |                     |                        |          |                    |
| NR SER.                            | LinkingModule16_Serial_Number | bigint       |                       |            |                | Liczba (duża)         |                    |                          |                 |                     |                        |          |                    |

---

*Eksportowano z Zoho CRM API*
