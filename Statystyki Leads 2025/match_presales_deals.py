"""Dopasowanie listy PreSales do konkretnych Deali."""

import pandas as pd
import csv
from difflib import SequenceMatcher

def similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def normalize(s):
    if pd.isna(s):
        return ""
    return s.lower().strip()

def extract_key_words(name: str) -> list:
    """Wyciągnij słowa kluczowe z nazwy."""
    # Usuń myślniki i inne znaki
    name_clean = name.replace('-', '').replace('.', ' ').replace(',', ' ')
    stopwords = {'sp', 'zoo', 'z', 'o', 'spółka', 'ograniczoną', 'odpowiedzialnością', 
                 'sc', 'sa', 'spzoo', 'sj', 'centrum', 'medyczne', 'klinika', 'gabinet',
                 'clinic', 'stomatologia', 'ośrodek', 'przychodnia', 'nzoz', 'spzoz'}
    words = [w for w in name_clean.lower().split() if len(w) >= 3 and w not in stopwords]
    return words

def key_word_match(short_name: str, full_name: str) -> tuple:
    """Sprawdź ile słów kluczowych pasuje."""
    keys = extract_key_words(short_name)
    if not keys:
        return False, 0.0
    
    full_lower = full_name.lower().replace('-', '')
    matches = sum(1 for k in keys if k in full_lower)
    
    if matches > 0:
        # Im więcej słów pasuje, tym lepiej
        match_ratio = matches / len(keys)
        base_score = similar(short_name, full_name)
        final_score = max(base_score, 0.5 + match_ratio * 0.4)
        return True, final_score
    return False, 0.0

# Wczytaj listę PreSales
ps_list = []
with open('raw_data/sprzedaz_presales_source.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=';')
    next(reader)
    for i, row in enumerate(reader, 1):
        if row and row[0].strip():
            ps_list.append({
                'lp': i,
                'klient': row[0].strip(),
                'produkt': row[1].strip() if len(row) > 1 else '',
                'presales': row[2].strip() if len(row) > 2 else ''
            })

print(f"Lista PreSales: {len(ps_list)} umów")

# Wczytaj lifecycle - tylko umowy (deal_id jako string żeby nie tracić precyzji!)
df = pd.read_csv('curated_data/lifecycle_2025.csv', dtype={'deal_id': str, 'ml_id': str, 'lead_id': str, 'account_id': str})
won = df[df['deal_is_won'] == True].copy()

# Utwórz mapę nazw z lifecycle
lc_accounts = {}
for _, row in won.iterrows():
    name = normalize(row['account_name'])
    if name:
        if name not in lc_accounts:
            lc_accounts[name] = []
        lc_accounts[name].append(row)

# Dopasuj każdy rekord z listy PreSales
results = []

for ps in ps_list:
    ps_name = normalize(ps['klient'])
    
    result = {
        'lp': ps['lp'],
        'ps_klient': ps['klient'],
        'ps_produkt': ps['produkt'],
        'ps_presales': ps['presales'],
        'deal_id': '',
        'account_id': '',
        'account_name_zoho': '',
        'moj_project_type': '',
        'deal_owner': '',
        'deal_created': '',
        'deal_stage': '',
        'deal_amount': '',
        'match_quality': 'NIE ZNALEZIONO',
        'uwagi': ''
    }
    
    # Dokładne dopasowanie
    if ps_name in lc_accounts:
        rows = lc_accounts[ps_name]
        row = rows[0]
        result['deal_id'] = str(row['deal_id']) if pd.notna(row['deal_id']) else ''
        result['account_id'] = str(row['account_id']) if pd.notna(row['account_id']) else ''
        result['account_name_zoho'] = row['account_name']
        result['moj_project_type'] = row['project_type']
        result['deal_owner'] = row['deal_owner_current']
        result['deal_created'] = str(row['deal_stage_start_time'])[:10] if pd.notna(row['deal_stage_start_time']) else ''
        result['deal_stage'] = row['deal_stage_current']
        result['deal_amount'] = row['deal_amount']
        result['match_quality'] = 'DOKŁADNE'
        if row['project_type'] == 'Sales only':
            result['uwagi'] = 'RÓŻNICA - u mnie Sales only!'
    else:
        # Szukaj po słowie kluczowym
        best_match = None
        best_score = 0
        
        for lc_name, rows in lc_accounts.items():
            # Najpierw sprawdź słowo kluczowe
            kw_match, kw_score = key_word_match(ps['klient'], lc_name)
            if kw_match and kw_score > best_score:
                best_score = kw_score
                best_match = (lc_name, rows, kw_score, 'keyword')
            elif not kw_match:
                # Fallback na fuzzy tylko jeśli nie znaleziono keyword match
                score = similar(ps_name, lc_name)
                if score > best_score and score > 0.6:
                    best_score = score
                    best_match = (lc_name, rows, score, 'fuzzy')
        
        if best_match:
            lc_name, rows, score, match_method = best_match
            row = rows[0]
            result['deal_id'] = str(row['deal_id']) if pd.notna(row['deal_id']) else ''
            result['account_id'] = str(row['account_id']) if pd.notna(row['account_id']) else ''
            result['account_name_zoho'] = row['account_name']
            result['moj_project_type'] = row['project_type']
            result['deal_owner'] = row['deal_owner_current']
            result['deal_created'] = str(row['deal_stage_start_time'])[:10] if pd.notna(row['deal_stage_start_time']) else ''
            result['deal_stage'] = row['deal_stage_current']
            result['deal_amount'] = row['deal_amount']
            result['match_quality'] = f'{match_method.upper()} ({score:.0%})'
            
            if match_method == 'fuzzy' and score < 0.75:
                result['uwagi'] = 'SPRAWDŹ - słabe dopasowanie!'
            elif row['project_type'] == 'Sales only':
                result['uwagi'] = 'RÓŻNICA - u mnie Sales only!'
    
    results.append(result)

# Zapisz do CSV
results_df = pd.DataFrame(results)
results_df.to_csv('raw_data/presales_deals_matching.csv', index=False, encoding='utf-8-sig')

print(f"\nZapisano: raw_data/presales_deals_matching.csv")
print(f"\nPodsumowanie:")
print(f"  Dopasowane dokładnie: {len([r for r in results if r['match_quality'] == 'DOKŁADNE'])}")
print(f"  Dopasowane keywords: {len([r for r in results if 'KEYWORDS' in r['match_quality']])}")
print(f"  Dopasowane fuzzy: {len([r for r in results if 'FUZZY' in r['match_quality']])}")
print(f"  Nie znalezione: {len([r for r in results if r['match_quality'] == 'NIE ZNALEZIONO'])}")
print(f"\n  RÓŻNICE (moje Sales only): {len([r for r in results if 'RÓŻNICA' in r['uwagi']])}")
print(f"  Do sprawdzenia (słabe dopasowanie): {len([r for r in results if 'SPRAWDŹ' in r['uwagi']])}")
