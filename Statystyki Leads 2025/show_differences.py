import pandas as pd

df = pd.read_csv('raw_data/presales_deals_matching.csv')

print("=== PODSUMOWANIE ===")
print(f"Dopasowanych: {len(df[df['match_quality'] != 'NIE ZNALEZIONO'])}")
print(f"Nie znaleziono: {len(df[df['match_quality'] == 'NIE ZNALEZIONO'])}")
print()

# Różnice - moje Sales only
roz = df[df['uwagi'].str.contains('RÓŻNICA', na=False)]
print(f"=== RÓŻNICE ({len(roz)}) - na liście PreSales, u mnie Sales only ===")
for _, row in roz.iterrows():
    print(f"  {row['lp']}. {row['ps_klient']}")
    print(f"     -> {row['account_name_zoho']}")
    print(f"     Deal ID: {row['deal_id']}, Owner: {row['deal_owner']}")
    print()

# Nie znalezione
nf = df[df['match_quality'] == 'NIE ZNALEZIONO']
print(f"=== NIE ZNALEZIONO ({len(nf)}) ===")
for _, row in nf.iterrows():
    print(f"  {row['lp']}. {row['ps_klient']} ({row['ps_produkt']})")
