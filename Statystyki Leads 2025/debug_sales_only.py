"""Debug Sales only firms from PreSales list."""
import json
import pandas as pd

# Wczytaj lifecycle i dump
df = pd.read_csv('curated_data/lifecycle_2025.csv', dtype={'deal_id': str})
deals = json.load(open('raw_data/deals_ALL_fields.json', 'r', encoding='utf-8'))
deals_map = {str(d.get('id')): d for d in deals}

won = df[df['deal_is_won'] == True]

# Firmy z listy PreSales które są u mnie Sales only
firms = ['paley', 'wellclinic', 'dunique', 'popularna']

for firm in firms:
    matches = won[won['account_name'].str.lower().str.contains(firm, na=False)]
    for _, row in matches.iterrows():
        acc = row['account_name']
        pt = row['project_type']
        did = row['deal_id']
        
        print(f"=== {firm.upper()} ===")
        print(f"Account: {acc}")
        print(f"project_type: {pt}")
        print(f"deal_id: {did}")
        
        # Sprawdź w dumpie
        d = deals_map.get(str(did), {})
        print(f"Lead_ID_before_conversion: {d.get('Lead_ID_before_conversion')}")
        print(f"Lead_Conversion_Time: {d.get('Lead_Conversion_Time')}")
        print(f"Deal_source_as_unit: {d.get('Deal_source_as_unit')}")
        print(f"Lead_provided_by: {d.get('Lead_provided_by')}")
        print(f"Owner: {d.get('Owner', {}).get('name') if isinstance(d.get('Owner'), dict) else d.get('Owner')}")
        print(f"Created_Time: {d.get('Created_Time')}")
        print()
