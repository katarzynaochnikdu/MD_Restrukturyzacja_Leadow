import json

deals = json.load(open('raw_data/deals_raw.json', 'r', encoding='utf-8'))

# Wszystkie Stage
stages = {}
for d in deals:
    s = d.get('Stage', '')
    stages[s] = stages.get(s, 0) + 1

print('Wszystkie Stage w dump:')
for s, c in sorted(stages.items(), key=lambda x: -x[1]):
    print(f'  "{s}": {c}')

# Umowy (won stages)
won_stages = ['Closed Won', 'Wdrożenie', 'Trial', 'Wdrożeni Klienci']
won_deals = [d for d in deals if d.get('Stage') in won_stages]
print(f'\nUmowy (won_stages): {len(won_deals)}')

# Wg roku
by_year = {}
for d in won_deals:
    ct = d.get('Created_Time', '') or ''
    year = '2025' if '2025' in ct else ('2024' if '2024' in ct else 'inne')
    by_year[year] = by_year.get(year, 0) + 1

print('Wg roku:')
for y, c in sorted(by_year.items()):
    print(f'  {y}: {c}')
