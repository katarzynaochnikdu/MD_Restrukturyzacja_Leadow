# Pola Modułu: Ordered_Items

## Statystyki

- **Łącznie pól:** 15
- **Pola niestandardowe:** 0
- **Pola standardowe:** 15
- **Pola formuł:** 3
- **Pola lookup:** 3
- **Pola wyboru (picklist):** 0

## Lista Pól

| Etykieta Pola (UI)                 | Nazwa API            | Typ Danych   | Pole Niestandardowe   | Wymagane   | Tylko Odczyt   | Typ Danych (Polski)   | Możliwe Wartości   | Powiązany Moduł   | Maks. Długość   | Wyrażenie Formuły   | Typ Zwracany Formuły   | Sekcja   | Wartość Domyślna   |
|:-----------------------------------|:---------------------|:-------------|:----------------------|:-----------|:---------------|:----------------------|:-------------------|:------------------|:----------------|:--------------------|:-----------------------|:---------|:-------------------|
| Cena zalecana przez wytwórcę       | List_Price           | currency     |                       |            |                | Waluta                |                    |                   |                 |                     |                        |          |                    |
| Cennik wszystkich dostawców        | Price_Book_Name      | lookup       |                       |            |                | Lookup                |                    | Price_Books       |                 |                     |                        |          |                    |
| Czas Utworzenia                    | Created_Time         | datetime     |                       |            |                | Data i czas           |                    |                   |                 |                     |                        |          |                    |
| Czas modyfikacji                   | Modified_Time        | datetime     |                       |            |                | Data i czas           |                    |                   |                 |                     |                        |          |                    |
| Identyfikator elementu nadrzędnego | Parent_Id            | lookup       |                       |            | ✓              | Lookup                |                    | Sales_Orders      |                 |                     |                        |          |                    |
| Ilość                              | Quantity             | double       |                       |            |                | double                |                    |                   |                 |                     |                        |          |                    |
| NR SER.                            | Sequence_Number      | bigint       |                       |            |                | Liczba (duża)         |                    |                   |                 |                     |                        |          |                    |
| Nazwa produktu                     | Product_Name         | lookup       |                       |            |                | Lookup                |                    | Products          |                 |                     |                        |          |                    |
| Opis                               | Description          | textarea     |                       |            |                | Tekst wielowierszowy  |                    |                   | 2000            |                     |                        |          |                    |
| Podatek                            | Tax                  | currency     |                       |            |                | Waluta                |                    |                   |                 |                     |                        |          |                    |
| Podatek w ujęciu procentowym       | Line_Tax             | linetax      |                       |            |                | linetax               |                    |                   |                 |                     |                        |          |                    |
| Rabat                              | Discount             | currency     |                       |            |                | Waluta                |                    |                   |                 |                     |                        |          |                    |
| Suma                               | Net_Total            | formula      |                       |            | ✓              | Formuła               |                    |                   |                 |                     | currency               |          |                    |
| Suma z uwzględnieniem rabatu       | Total_After_Discount | formula      |                       |            | ✓              | Formuła               |                    |                   |                 |                     | currency               |          |                    |
| Wartość ogółem                     | Total                | formula      |                       |            | ✓              | Formuła               |                    |                   |                 |                     | currency               |          |                    |

---

*Eksportowano z Zoho CRM API*
