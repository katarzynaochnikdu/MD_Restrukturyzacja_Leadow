"""Sprawdź Leads z pliku źródłowego PreSales."""

import json
import re
import csv

# Wczytaj dump Leads
leads = json.load(open('raw_data/leads_raw.json', 'r', encoding='utf-8'))
leads_map = {str(l.get('id')): l for l in leads}

# Wyciągnij Lead IDs z pliku źródłowego
lead_ids = []
with open('raw_data/sprzedaz_presales_source_sztyczen_wrzesien_umowy.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=';')
    next(reader)
    for row in reader:
        if len(row) > 7 and row[7]:
            link = row[7].strip()
            match = re.search(r'/tab/Leads/(\d+)', link)
            if match:
                lead_ids.append({
                    'klient': row[1].strip(),
                    'lead_id': match.group(1)
                })

print(f"Leads z pliku źródłowego: {len(lead_ids)}")
print()

# Sprawdź każdy Lead
found = 0
not_found = 0
converted = 0

print("SZCZEGÓŁY:")
for item in lead_ids:
    lead = leads_map.get(item['lead_id'])
    if lead:
        found += 1
        status = lead.get('Lead_Status', '')
        converted_deal = lead.get('Converted_Deal', {})
        
        if converted_deal or status == 'Przekonwertowany':
            converted += 1
            deal_id = converted_deal.get('id') if isinstance(converted_deal, dict) else converted_deal
            print(f"[OK] {item['klient']}: Lead {item['lead_id']} -> KONWERTOWANY")
            if deal_id:
                print(f"    Converted_Deal: {deal_id}")
        else:
            print(f"[--] {item['klient']}: Lead {item['lead_id']} -> Status: {status}")
    else:
        not_found += 1
        print(f"[XX] {item['klient']}: Lead {item['lead_id']} -> NIE ZNALEZIONY W DUMPIE")

print()
print(f"PODSUMOWANIE:")
print(f"  Znalezione w dumpie: {found}")
print(f"  Nie znalezione: {not_found}")
print(f"  Skonwertowane: {converted}")
