"""
Konfiguracja projektu Statystyki Leads 2025.
Zawiera stałe (etapy wygranych, mapowania źródeł itp.) używane w całym projekcie.
"""

# Wartości pola Stage w module Deals, które uznajemy za "wygraną" (umowę)
DEAL_WON_STAGES = frozenset([
    "Closed Won",
    "Wdrożenie",
    "Trial",
    "Wdrożeni Klienci",
])

# Wartości pola Stage w module Deals, które uznajemy za "przegraną"
DEAL_LOST_STAGES = frozenset([
    "Closed Lost",
])

# Wszystkie etapy zamknięte (won + lost)
DEAL_CLOSED_STAGES = DEAL_WON_STAGES | DEAL_LOST_STAGES

# Miesiące do analizy (2025)
MONTHS_2025 = [
    "Styczeń", "Luty", "Marzec", "Kwiecień", "Maj", "Czerwiec",
    "Lipiec", "Sierpień", "Wrzesień", "Październik", "Listopad", "Grudzień"
]

# Mapowanie numeru miesiąca na nazwę polską
MONTH_NUM_TO_NAME = {
    1: "Styczeń",
    2: "Luty",
    3: "Marzec",
    4: "Kwiecień",
    5: "Maj",
    6: "Czerwiec",
    7: "Lipiec",
    8: "Sierpień",
    9: "Wrzesień",
    10: "Październik",
    11: "Listopad",
    12: "Grudzień",
}

# Mapowanie nazwy miesiąca na numer
MONTH_NAME_TO_NUM = {v: k for k, v in MONTH_NUM_TO_NAME.items()}

# Rok analizy
ANALYSIS_YEAR = 2025

# Ścieżki plików
EXCEL_SUMMARY_FILE = "202511 Podsumowanie Leadów i MLi.xlsx"
OUTPUT_ANALYTICS_FILE = "Oceny_i_konwersje_2025.xlsx"
RAW_DATA_DIR = "raw_data"
CURATED_DATA_DIR = "curated_data"
