"""
Skrypt do naprawy pliku Excel - oznaczenie firm z aktywnymi dealami jako wykluczonych.
Firmy z dealami na etapach: Negocjacje, Oferta, Oferta zaakceptowana, Umowa w podpisie
powinny być wykluczone (nie do kontaktu).
"""

import pandas as pd
import os
import sys
from datetime import datetime

# Ścieżka do pliku źródłowego
SOURCE_FILE = r"C:\Users\kochn\.cursor\Medidesk\2026 Fundusze Wsparcia placówki POZ\wyniki_all_companies_with_presentation\2026-01-12_04-13-30\all_companies_with_presentation_20260112_041330.xlsx"

# Etapy dealów które POWINNY wykluczać firmę (nie do kontaktu)
# Zawierają krótkie i pełne nazwy dla pewności
ETAPY_DO_WYKLUCZENIA = [
    "Negocjacje",
    "Negocjace",  # literówka w oryginalnych danych
    "Negocjace (Negotiation/Review)",
    "Oferta",
    "Oferta (Value Proposition)",
    "Oferta zaakceptowana",
    "Oferta zaakceptowana (Akceptacja oferty)",
    "Umowa w podpisie",
    "Umowa w podpisie (Umowa)",
]


def main():
    print(f"Wczytywanie pliku: {SOURCE_FILE}")
    
    if not os.path.exists(SOURCE_FILE):
        print(f"BŁĄD: Plik nie istnieje: {SOURCE_FILE}")
        sys.exit(1)
    
    df = pd.read_excel(SOURCE_FILE, dtype=str)
    
    print(f"\nWczytano {len(df)} wierszy")
    print(f"Kolumny: {list(df.columns)}")
    
    # Analiza - ile jest Wykluczony=False (do kontaktu)
    df_do_kontaktu = df[df["Wykluczony"] == "False"]
    print(f"\nFirmy oznaczone jako DO KONTAKTU (Wykluczony=False): {len(df_do_kontaktu)}")
    
    # Sprawdź unikalne wartości Status_Lead_Deal
    print(f"\nUnikalne wartości Status_Lead_Deal w całym pliku:")
    status_counts = df["Status_Lead_Deal"].value_counts(dropna=False)
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    
    # Sprawdź Status_Lead_Deal dla firm "do kontaktu"
    print(f"\nStatus_Lead_Deal dla firm DO KONTAKTU (Wykluczony=False):")
    if len(df_do_kontaktu) > 0:
        status_counts_kontakt = df_do_kontaktu["Status_Lead_Deal"].value_counts(dropna=False)
        for status, count in status_counts_kontakt.items():
            print(f"  {status}: {count}")
    
    # Znajdź firmy do kontaktu, które MAJĄ aktywny deal na wykluczonych etapach
    print(f"\n--- SZUKANIE BŁĘDNIE OZNACZONYCH FIRM ---")
    
    def should_be_excluded(row) -> bool:
        """Sprawdza czy firma powinna być wykluczona na podstawie Status_Lead_Deal."""
        status = str(row.get("Status_Lead_Deal", "") or "").strip()
        typ = str(row.get("Typ_statusu", "") or "").strip()
        
        # Tylko patrzymy na Deals (nie Leads)
        if typ != "Deal":
            return False
        
        # Sprawdź czy status pasuje do wykluczonych etapów
        for etap in ETAPY_DO_WYKLUCZENIA:
            if etap.lower() == status.lower() or status.lower().startswith(etap.lower()):
                return True
        return False
    
    # Znajdź błędnie oznaczone
    mask_bledne = (df["Wykluczony"] == "False") & df.apply(should_be_excluded, axis=1)
    df_bledne = df[mask_bledne]
    
    print(f"\nZnaleziono {len(df_bledne)} firm BŁĘDNIE oznaczonych jako 'do kontaktu':")
    print("  (mają aktywny deal na etapie który powinien wykluczać)")
    
    if len(df_bledne) > 0:
        print("\nPrzykłady błędnie oznaczonych firm:")
        for idx, row in df_bledne.head(10).iterrows():
            print(f"  - NIP: {row['NIP']}, Status: {row['Status_Lead_Deal']}, Typ: {row['Typ_statusu']}, Nazwa: {row['Nazwa_firmy'][:50]}...")
    
    # POPRAWKA - ustawiamy Wykluczony=True i Powod_wykluczenia
    print(f"\n--- WYKONYWANIE POPRAWKI ---")
    
    df_poprawiony = df.copy()
    
    # Dla każdego błędnie oznaczonego wiersza, ustaw Wykluczony=True
    for idx in df_bledne.index:
        df_poprawiony.at[idx, "Wykluczony"] = "True"
        df_poprawiony.at[idx, "Powod_wykluczenia"] = "AKTYWNY_DEAL_W_RODZINIE_NIP"
    
    print(f"Poprawiono {len(df_bledne)} wierszy")
    
    # Weryfikacja po poprawce
    df_do_kontaktu_po = df_poprawiony[df_poprawiony["Wykluczony"] == "False"]
    print(f"\nPo poprawce - firmy DO KONTAKTU (Wykluczony=False): {len(df_do_kontaktu_po)}")
    
    # Zapisz nowy plik
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.dirname(SOURCE_FILE)
    output_file = os.path.join(output_dir, f"all_companies_with_presentation_POPRAWIONY_{timestamp}.xlsx")
    
    df_poprawiony.to_excel(output_file, index=False, engine="openpyxl")
    print(f"\nZapisano poprawiony plik: {output_file}")
    
    # Podsumowanie
    print("\n=== PODSUMOWANIE ===")
    print(f"Oryginalny plik: {len(df)} wierszy")
    print(f"  - Do kontaktu (Wykluczony=False): {len(df_do_kontaktu)}")
    print(f"  - Wykluczone (Wykluczony=True): {len(df) - len(df_do_kontaktu)}")
    print(f"\nPo poprawce:")
    print(f"  - Do kontaktu (Wykluczony=False): {len(df_do_kontaktu_po)}")
    print(f"  - Wykluczone (Wykluczony=True): {len(df_poprawiony) - len(df_do_kontaktu_po)}")
    print(f"  - Liczba poprawionych wierszy: {len(df_bledne)}")


if __name__ == "__main__":
    main()
