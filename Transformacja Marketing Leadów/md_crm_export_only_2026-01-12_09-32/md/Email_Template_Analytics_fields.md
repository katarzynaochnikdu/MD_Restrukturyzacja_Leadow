# Pola Modułu: Email_Template_Analytics

## Statystyki

- **Łącznie pól:** 13
- **Pola niestandardowe:** 0
- **Pola standardowe:** 13
- **Pola formuł:** 0
- **Pola lookup:** 1
- **Pola wyboru (picklist):** 1

## Lista Pól

| Etykieta Pola (UI)        | Nazwa API             | Typ Danych   | Pole Niestandardowe   | Wymagane   | Tylko Odczyt   | Typ Danych (Polski)   | Możliwe Wartości                               | Powiązany Moduł   | Maks. Długość   | Wyrażenie Formuły   | Typ Zwracany Formuły   | Sekcja   | Wartość Domyślna   |
|:--------------------------|:----------------------|:-------------|:----------------------|:-----------|:---------------|:----------------------|:-----------------------------------------------|:------------------|:----------------|:--------------------|:-----------------------|:---------|:-------------------|
| Data                      | Date                  | datetime     |                       |            |                | Data i czas           |                                                |                   |                 |                     |                        |          |                    |
| Entity Source             | Entity_Source         | module       |                       |            |                | module                |                                                |                   |                 |                     |                        |          |                    |
| Kliknięty                 | Clicked               | integer      |                       |            |                | Liczba całkowita      |                                                |                   |                 |                     |                        |          |                    |
| Mail Source               | Mail_Source           | picklist     |                       |            |                | Lista wyboru          | -None-; Individual; WorkFlow Alert; Mass Email |                   |                 |                     |                        |          |                    |
| Nazwa szablonu            | Template_Name         | lookup       |                       |            |                | Lookup                |                                                | Email_Template__s |                 |                     |                        |          |                    |
| Odebrano                  | Received              | integer      |                       |            |                | Liczba całkowita      |                                                |                   |                 |                     |                        |          |                    |
| Odrzucony                 | Bounced               | integer      |                       |            |                | Liczba całkowita      |                                                |                   |                 |                     |                        |          |                    |
| Otwarty                   | Opened                | integer      |                       |            |                | Liczba całkowita      |                                                |                   |                 |                     |                        |          |                    |
| Przesłane                 | Sent                  | integer      |                       |            |                | Liczba całkowita      |                                                |                   |                 |                     |                        |          |                    |
| Udzielono odpowiedzi      | Replied               | integer      |                       |            |                | Liczba całkowita      |                                                |                   |                 |                     |                        |          |                    |
| Użytkownik                | User                  | ownerlookup  |                       |            | ✓              | Właściciel            |                                                |                   |                 |                     |                        |          |                    |
| Zwrócone wśród śledzonych | Bounced_Among_Tracked | integer      |                       |            |                | Liczba całkowita      |                                                |                   |                 |                     |                        |          |                    |
| Śledzone                  | Tracked               | integer      |                       |            |                | Liczba całkowita      |                                                |                   |                 |                     |                        |          |                    |

---

*Eksportowano z Zoho CRM API*
