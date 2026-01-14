# Pola Modułu: Data_Subject_Requests

## Statystyki

- **Łącznie pól:** 8
- **Pola niestandardowe:** 0
- **Pola standardowe:** 8
- **Pola formuł:** 0
- **Pola lookup:** 1
- **Pola wyboru (picklist):** 2

## Lista Pól

| Etykieta Pola (UI)                 | Nazwa API      | Typ Danych   | Pole Niestandardowe   | Wymagane   | Tylko Odczyt   | Typ Danych (Polski)   | Możliwe Wartości                                                                                                                                                                                            | Maks. Długość   | Wyrażenie Formuły   | Typ Zwracany Formuły   | Sekcja   | Wartość Domyślna   | Powiązany Moduł   |
|:-----------------------------------|:---------------|:-------------|:----------------------|:-----------|:---------------|:----------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------|:--------------------|:-----------------------|:---------|:-------------------|:------------------|
| Closed By                          | Closed_By      | ownerlookup  |                       |            |                | Właściciel            |                                                                                                                                                                                                             |                 |                     |                        |          |                    |                   |
| Closed Date                        | Closed_Date    | datetime     |                       |            |                | Data i czas           |                                                                                                                                                                                                             |                 |                     |                        |          |                    |                   |
| Identyfikator elementu nadrzędnego | Parent_Id      | lookup       |                       |            |                | Lookup                |                                                                                                                                                                                                             |                 |                     |                        |          |                    | nan               |
| Is Closed                          | Is_Closed      | text         |                       |            |                | Tekst                 |                                                                                                                                                                                                             | 25              |                     |                        |          |                    |                   |
| Request Source                     | Request_Source | picklist     |                       |            |                | Lista wyboru          | Portal; Łącze żądań danych (Data Requests Link)                                                                                                                                                             |                 |                     |                        |          |                    |                   |
| Request Type                       | Request_Type   | picklist     |                       |            |                | Lista wyboru          | Zażądaj dostępu do danych (Access); Zażądaj wyczyszczenia danych (Rectify); Zażądaj eksportu danych (Export); Zażądaj zatrzymania przetwarzanie danych (Stop Processing); Zażądaj usunięcia danych (Delete) |                 |                     |                        |          |                    |                   |
| Requested Date                     | Requested_Date | datetime     |                       |            |                | Data i czas           |                                                                                                                                                                                                             |                 |                     |                        |          |                    |                   |
| Wnioskowane przez                  | Requested_By   | ownerlookup  |                       |            |                | Właściciel            |                                                                                                                                                                                                             |                 |                     |                        |          |                    |                   |

---

*Eksportowano z Zoho CRM API*
