from difflib import SequenceMatcher

def similar(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def extract_key_words(name):
    stopwords = {'sp', 'zoo', 'z', 'o', 'centrum', 'medyczne', 'klinika'}
    words = [w for w in name.lower().split() if len(w) >= 3 and w not in stopwords]
    return words

test_cases = [
    ('Paley Eurpean Institute', 'PALEY EUROPEAN INSTITUTE SPÓŁKA'),
    ('Wellclinic', 'WELLCLINIC SPÓŁKA Z OGRANICZONĄ'),
    ('Dunique', 'DUNIQUE I SPÓŁKA'),
    ('Cud-Med Kielce', 'CUDMED SPÓŁKA'),
]

for short, full in test_cases:
    keys = extract_key_words(short)
    matches = sum(1 for k in keys if k in full.lower())
    score = similar(short, full)
    print(f'{short} -> {full[:30]}...')
    print(f'  Keys: {keys}, Matches: {matches}, Score: {score:.0%}')
    print()
