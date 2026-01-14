"""Weryfikacja dopasowania z pełnym plikiem źródłowym PreSales."""

import pandas as pd
import csv
import re

def extract_id_from_link(link):
    """Wyciągnij ID z linku Zoho."""
    if not link or pd.isna(link):
        return None, None
    
    link = str(link).strip()
    
    # Wzorce dla różnych typów
    patterns = [
        (r'/tab/Potentials/(\d+)', 'Deal'),
        (r'/tab/Leads/(\d+)', 'Lead'),
        (r'/tab/Accounts/(\d+)', 'Account'),
        (r'/tab/Events/(\d+)', 'Event'),
    ]
    
    for pattern, obj_type in patterns:
        match = re.search(pattern, link)
        if match:
            return match.group(1), obj_type
    
    return None, None

# Wczytaj plik źródłowy PreSales (styczeń-wrzesień)
source_data = []
with open('raw_data/sprzedaz_presales_source_sztyczen_wrzesien_umowy.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=';')
    next(reader)  # skip header
    for i, row in enumerate(reader, 1):
        if row and row[0].strip() and row[0].strip() not in ['miesiąc podpisania umowy', '']:
            link = row[7].strip() if len(row) > 7 else ''
            record_id, record_type = extract_id_from_link(link)
            source_data.append({
                'lp': i,
                'miesiac': row[0].strip(),
                'klient': row[1].strip(),
                'produkt': row[2].strip() if len(row) > 2 else '',
                'owner': row[3].strip() if len(row) > 3 else '',
                'link': link,
                'record_id': record_id,
                'record_type': record_type,
                'presales': row[10].strip() if len(row) > 10 else ''
            })

print(f"Plik źródłowy PreSales: {len(source_data)} rekordów")

# Statystyki typów linków
type_counts = {}
for r in source_data:
    t = r['record_type'] or 'BRAK'
    type_counts[t] = type_counts.get(t, 0) + 1

print("\nTypy rekordów w linkach:")
for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
    print(f"  {t}: {c}")

# Wczytaj moje dopasowanie
my_matching = pd.read_csv('raw_data/presales_deals_matching.csv', dtype=str)

# Wczytaj lifecycle żeby mieć mapowanie lead_id -> deal_id
lifecycle = pd.read_csv('curated_data/lifecycle_2025.csv', dtype={'deal_id': str, 'lead_id': str, 'ml_id': str, 'account_id': str})

# Mapowania
lead_to_deal = dict(zip(lifecycle['lead_id'].dropna(), lifecycle['deal_id']))
deal_to_account = dict(zip(lifecycle['deal_id'].dropna(), lifecycle['account_id']))

# Porównaj
print("\n" + "="*80)
print("WERYFIKACJA DOPASOWAŃ")
print("="*80)

results = []
for src in source_data:
    result = {
        'lp': src['lp'],
        'miesiac': src['miesiac'],
        'klient': src['klient'],
        'produkt': src['produkt'],
        'link_type': src['record_type'],
        'link_id': src['record_id'],
        'link': src['link'],
        'moje_deal_id': '',
        'moje_account_id': '',
        'match_status': '',
        'uwagi': ''
    }
    
    # Znajdź w moim dopasowaniu po nazwie klienta
    klient_lower = src['klient'].lower().replace('-', '')
    my_match = my_matching[my_matching['ps_klient'].str.lower().str.replace('-', '') == klient_lower]
    
    if len(my_match) == 0:
        # Szukaj po pierwszych słowach
        first_word = klient_lower.split()[0] if klient_lower.split() else ''
        my_match = my_matching[my_matching['ps_klient'].str.lower().str.contains(first_word, na=False)]
    
    if len(my_match) > 0:
        my_row = my_match.iloc[0]
        result['moje_deal_id'] = my_row['deal_id'] if pd.notna(my_row['deal_id']) else ''
        result['moje_account_id'] = my_row['account_id'] if pd.notna(my_row['account_id']) else ''
        
        # Sprawdź czy ID się zgadza
        src_id = src['record_id']
        src_type = src['record_type']
        
        if src_type == 'Deal':
            if src_id == result['moje_deal_id']:
                result['match_status'] = 'OK - Deal ID zgodne'
            else:
                result['match_status'] = 'RÓŻNICA - inne Deal ID'
                result['uwagi'] = f"Źródło: {src_id}, Moje: {result['moje_deal_id']}"
        
        elif src_type == 'Lead':
            # Sprawdź czy ten Lead przekonwertował się na mój Deal
            converted_deal = lead_to_deal.get(src_id)
            if converted_deal == result['moje_deal_id']:
                result['match_status'] = 'OK - Lead -> Deal zgodne'
                result['uwagi'] = f"Lead {src_id} -> Deal {converted_deal}"
            elif converted_deal:
                result['match_status'] = 'RÓŻNICA - Lead konwertował na inny Deal'
                result['uwagi'] = f"Lead {src_id} -> Deal {converted_deal}, Moje: {result['moje_deal_id']}"
            else:
                result['match_status'] = 'UWAGA - Lead nie znaleziony w lifecycle'
                result['uwagi'] = f"Lead {src_id} nie ma powiązanego Deala"
        
        elif src_type == 'Account':
            if src_id == result['moje_account_id']:
                result['match_status'] = 'OK - Account ID zgodne'
            else:
                result['match_status'] = 'RÓŻNICA - inne Account ID'
                result['uwagi'] = f"Źródło: {src_id}, Moje: {result['moje_account_id']}"
        
        elif src_type == 'Event':
            result['match_status'] = 'UWAGA - link do Event (sprawdź ręcznie)'
            result['uwagi'] = f"Event {src_id}"
        
        else:
            result['match_status'] = 'BRAK LINKU'
    else:
        result['match_status'] = 'NIE ZNALEZIONO w moim dopasowaniu'
    
    results.append(result)

# Statystyki
print("\nStatystyki weryfikacji:")
status_counts = {}
for r in results:
    s = r['match_status'].split(' - ')[0] if ' - ' in r['match_status'] else r['match_status']
    status_counts[s] = status_counts.get(s, 0) + 1

for s, c in sorted(status_counts.items(), key=lambda x: -x[1]):
    print(f"  {s}: {c}")

# Pokaż problematyczne
print("\n" + "="*80)
print("PROBLEMATYCZNE REKORDY")
print("="*80)

for r in results:
    if 'OK' not in r['match_status'] and 'BRAK LINKU' not in r['match_status']:
        print(f"\n{r['lp']}. {r['klient']} ({r['miesiac']})")
        print(f"   Link: {r['link_type']} {r['link_id']}")
        print(f"   Status: {r['match_status']}")
        if r['uwagi']:
            print(f"   Uwagi: {r['uwagi']}")

# Zapisz do CSV
results_df = pd.DataFrame(results)
results_df.to_csv('raw_data/presales_verification.csv', index=False, encoding='utf-8-sig')
print(f"\n\nZapisano: raw_data/presales_verification.csv")
