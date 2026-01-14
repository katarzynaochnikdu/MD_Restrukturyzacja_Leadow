# Pola Modułu: Attachments

## Statystyki

- **Łącznie pól:** 8
- **Pola niestandardowe:** 0
- **Pola standardowe:** 8
- **Pola formuł:** 0
- **Pola lookup:** 1
- **Pola wyboru (picklist):** 0

## Lista Pól

| Etykieta Pola (UI)                 | Nazwa API     | Typ Danych   | Pole Niestandardowe   | Wymagane   | Tylko Odczyt   | Typ Danych (Polski)   | Możliwe Wartości   | Powiązany Moduł   | Maks. Długość   | Wyrażenie Formuły   | Typ Zwracany Formuły   | Sekcja   | Wartość Domyślna   |
|:-----------------------------------|:--------------|:-------------|:----------------------|:-----------|:---------------|:----------------------|:-------------------|:------------------|:----------------|:--------------------|:-----------------------|:---------|:-------------------|
| Attachment Owner                   | Owner         | ownerlookup  |                       |            |                | Właściciel            |                    |                   |                 |                     |                        |          |                    |
| Czas Utworzenia                    | Created_Time  | datetime     |                       |            |                | Data i czas           |                    |                   |                 |                     |                        |          |                    |
| Czas modyfikacji                   | Modified_Time | datetime     |                       |            |                | Data i czas           |                    |                   |                 |                     |                        |          |                    |
| Identyfikator elementu nadrzędnego | Parent_Id     | lookup       |                       |            |                | Lookup                |                    | nan               |                 |                     |                        |          |                    |
| Nazwa pliku                        | File_Name     | text         |                       |            |                | Tekst                 |                    |                   | 100             |                     |                        |          |                    |
| Rozmiar                            | Size          | bigint       |                       |            |                | Liczba (duża)         |                    |                   |                 |                     |                        |          |                    |
| Utworzone przez                    | Created_By    | ownerlookup  |                       |            |                | Właściciel            |                    |                   |                 |                     |                        |          |                    |
| Zmodyfikowany przez                | Modified_By   | ownerlookup  |                       |            |                | Właściciel            |                    |                   |                 |                     |                        |          |                    |

---

*Eksportowano z Zoho CRM API*
