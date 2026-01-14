"""Porównanie listy PreSales z lifecycle."""

import pandas as pd
import csv
from difflib import SequenceMatcher

def similar(a: str, b: str) -> float:
    """Podobieństwo dwóch stringów 0-1."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def contains_key_words(short_name: str, full_name: str) -> bool:
    """Sprawdza czy kluczowe słowa z krótkiej nazwy są w pełnej."""
    # Usuń typowe końcówki
    stopwords = {'sp', 'zoo', 'z', 'o', 'spółka', 'ograniczoną', 'odpowiedzialnością', 
                 'sc', 'sa', 'spzoo', 'sj', 'centrum', 'medyczne', 'klinika', 'gabinet'}
    
    short_words = set(w for w in short_name.lower().split() if len(w) > 2 and w not in stopwords)
    full_lower = full_name.lower()
    
    if not short_words:
        return False
    
    # Ile kluczowych słów pasuje?
    matches = sum(1 for w in short_words if w in full_lower)
    return matches >= len(short_words) * 0.5  # co najmniej 50% słów

# Wczytaj listę PreSales
ps_list = []
with open('raw_data/sprzedaz_presales_source.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=';')
    next(reader)  # skip header
    for row in reader:
        if row and row[0].strip():
            ps_list.append({
                'klient': row[0].strip(),
                'produkt': row[1].strip() if len(row) > 1 else '',
                'presales': row[2].strip() if len(row) > 2 else ''
            })

print(f"Lista PreSales: {len(ps_list)} umów")

# Wczytaj lifecycle
df = pd.read_csv('curated_data/lifecycle_2025.csv')
won = df[df['deal_is_won'] == True].copy()

presales_won = won[won['project_type'] == 'PreSales']
sales_won = won[won['project_type'] == 'Sales only']

print(f"Moje umowy: {len(won)}")
print(f"  PreSales: {len(presales_won)}")
print(f"  Sales only: {len(sales_won)}")
print()

# Normalizacja nazw
def normalize(s):
    if pd.isna(s):
        return ""
    return s.lower().strip()

# Utwórz mapę nazw z lifecycle
lc_accounts = {}
for _, row in won.iterrows():
    name = normalize(row['account_name'])
    if name:
        if name not in lc_accounts:
            lc_accounts[name] = []
        lc_accounts[name].append(row)

# Dopasuj każdy rekord z listy PreSales
matched = []
not_found = []

for ps in ps_list:
    ps_name = normalize(ps['klient'])
    
    # Dokładne dopasowanie
    if ps_name in lc_accounts:
        rows = lc_accounts[ps_name]
        matched.append({
            'ps_klient': ps['klient'],
            'ps_produkt': ps['produkt'],
            'ps_presales': ps['presales'],
            'lc_account': rows[0]['account_name'],
            'lc_project_type': rows[0]['project_type'],
            'lc_deal_produkt': rows[0].get('deal_produkt', ''),
            'match_type': 'exact'
        })
    else:
        # Szukaj po kluczowych słowach
        best_match = None
        best_score = 0
        
        for lc_name, rows in lc_accounts.items():
            # Sprawdź czy kluczowe słowa pasują
            if contains_key_words(ps['klient'], lc_name):
                score = similar(ps_name, lc_name)
                if score > best_score:
                    best_score = score
                    best_match = (lc_name, rows, score, 'keywords')
            else:
                # Fallback na fuzzy
                score = similar(ps_name, lc_name)
                if score > best_score and score > 0.6:
                    best_score = score
                    best_match = (lc_name, rows, score, 'fuzzy')
        
        if best_match:
            lc_name, rows, score, match_method = best_match
            matched.append({
                'ps_klient': ps['klient'],
                'ps_produkt': ps['produkt'],
                'ps_presales': ps['presales'],
                'lc_account': rows[0]['account_name'],
                'lc_project_type': rows[0]['project_type'],
                'lc_deal_produkt': rows[0].get('deal_produkt', ''),
                'match_type': f'{match_method} ({score:.0%})'
            })
        else:
            not_found.append(ps)

print(f"=== DOPASOWANIE ===")
print(f"Dopasowane: {len(matched)}")
print(f"Nie znalezione: {len(not_found)}")
print()

# Statystyki dopasowanych
if matched:
    presales_correct = sum(1 for m in matched if m['lc_project_type'] == 'PreSales')
    sales_incorrect = sum(1 for m in matched if m['lc_project_type'] == 'Sales only')
    print(f"Z dopasowanych:")
    print(f"  Oznaczone jako PreSales: {presales_correct}")
    print(f"  Oznaczone jako Sales only: {sales_incorrect}")
    print()

# Nie znalezione
if not_found:
    print("=== NIE ZNALEZIONE W LIFECYCLE ===")
    for nf in not_found:
        print(f"  - {nf['klient']} ({nf['produkt']})")
    print()

# Błędnie oznaczone jako Sales only
sales_incorrect_list = [m for m in matched if m['lc_project_type'] == 'Sales only']
if sales_incorrect_list:
    print("=== BŁĘDNIE OZNACZONE JAKO SALES ONLY ===")
    for m in sales_incorrect_list:
        print(f"  - {m['ps_klient']} -> {m['lc_account']} [{m['match_type']}]")
    print()

# Co mam ekstra? (moje PreSales które nie są na liście)
print("=== MOJE PRESALES KTÓRYCH NIE MA NA LIŚCIE ===")
matched_accounts = set(normalize(m['lc_account']) for m in matched)
my_presales_accounts = presales_won['account_name'].dropna().str.strip().tolist()

extra = []
for acc in my_presales_accounts:
    if normalize(acc) not in matched_accounts:
        extra.append(acc)

print(f"Liczba: {len(extra)}")
if extra:
    for acc in sorted(set(extra)):
        print(f"  - {acc}")
