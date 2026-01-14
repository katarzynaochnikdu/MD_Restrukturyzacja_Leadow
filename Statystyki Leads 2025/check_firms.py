import pandas as pd

df = pd.read_csv('curated_data/lifecycle_2025.csv')
won = df[df['deal_is_won'] == True]

# Szukaj konkretnych firm
firms = ['paley', 'wellclinic', 'dunique', 'viventi', 'cudmed', 'dermedik', 
         'calma', 'eksperci', 'ortod', 'kotowsc', 'popularna', 'artes']

for firm in firms:
    matches = won[won['account_name'].str.lower().str.contains(firm, na=False)]
    if len(matches) > 0:
        for _, row in matches.iterrows():
            acc = row['account_name']
            pt = row['project_type']
            print(f"{firm.upper()}: {acc} -> {pt}")
    else:
        print(f"{firm.upper()}: NIE ZNALEZIONO w umowach")
    print()
