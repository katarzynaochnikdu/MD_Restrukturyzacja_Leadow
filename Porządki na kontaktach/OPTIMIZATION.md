# ⚡ Optymalizacja dla dużych baz danych

## 🎯 Problem

Przy **50,000 firm** i **70,000 kontaktów**, pełny scoring dla wszystkich firm zajmowałby:
- 50,000 firm × ~10 API calls (sprawdzanie powiązań) = 500,000 requestów
- 500,000 × 0.5s (rate limit) = **~69 godzin!** 😱

## ✅ Rozwiązanie: Strategia 1 (zaimplementowana)

### Tryb Accounts - ZOPTYMALIZOWANY

**STARA strategia (nieefektywna):**
```
1. Pobierz wszystkie firmy → 50,000 firm
2. Oblicz scoring dla WSZYSTKICH → 50,000 × 5s = 69 godzin
3. Znajdź duplikaty
4. Scalaj
```

**NOWA strategia (zoptymalizowana):**
```
1. Pobierz wszystkie firmy → 50,000 firm (5 minut)
2. Znajdź duplikaty PO NAZWIE/NIP (szybko - bez API) → np. 500 duplikatów
3. Oblicz scoring TYLKO dla duplikatów → 500 × 5s = 42 minuty ✅
4. Scalaj
```

**Oszczędność czasu:** ~99% (z 69 godzin do ~50 minut)! 🚀

**SUPER OPTYMALIZACJA dla testów (--limit 1-1000):**
```
1. Pobieraj firmy PARTIAMI (200 za razem)
2. Po każdej partii: szukaj duplikatów
3. Gdy znajdziesz duplikaty → STOP (nie pobieraj dalej!)
4. Dla --limit 1: zamiast 26k firm → tylko 200-400 firm (2 sekundy zamiast 2 minut!)
```

**Przykład:**
- `--limit 1` + brak duplikatów w pierwszych 200 firmach → pobiera 400 firm
- `--limit 1` + duplikaty w pierwszych 200 firmach → pobiera 200 firm ✅ STOP!
- `--limit 0` (wszystkie) → pobiera całą bazę jak wcześniej

### Tryb Contacts - ZOPTYMALIZOWANY

**Kontakty są prostsze** (deduplikacja lokalna):
- 70,000 kontaktów × 0.5s (1 request na update) = ~10 godzin
- Z limitem (np. 1000 kontaktów) = ~8 minut

**Optymalizacja:**
- Progress bar co 10% (nie co kontakt)
- Batch updates (100 kontaktów naraz)
- Skip jeśli brak zmian

## 📊 Szacowane czasy

### Dla Twoich danych (50k firm, 70k kontaktów):

| Tryb     | Operacja                  | Czas          |
|----------|---------------------------|---------------|
| Accounts | Pobieranie (50k firm)     | ~5 minut      |
| Accounts | Backup (zapis JSON)       | ~1 minuta     |
| Accounts | Znajdź duplikaty          | ~30 sekund    |
| Accounts | Scoring duplikatów (500)  | ~40 minut*    |
| Accounts | Scalanie                  | ~5 minut      |
| **Accounts TOTAL**                 | **~50 minut** |
|          |                           |               |
| Contacts | Pobieranie (70k kontaktów)| ~7 minut      |
| Contacts | Backup (zapis JSON)       | ~2 minuty     |
| Contacts | Czyszczenie + update      | ~10 godzin*   |
| **Contacts TOTAL**                 | **~10 godzin**|

\* Zakładając ~1% duplikatów firm i aktualizację ~30% kontaktów

## 🎮 Rekomendacje

### Dla Accounts (50k firm):

```bash
# 1. TEST na 10 firmach
python cleanup_zoho.py --mode accounts --dry-run --limit 10

# 2. TEST na 100 firmach (sprawdź ile duplikatów)
python cleanup_zoho.py --mode accounts --dry-run --limit 100

# 3. Jeśli OK - uruchom na CAŁEJ bazie (zajmie ~50 minut)
python cleanup_zoho.py --mode accounts --apply
```

### Dla Contacts (70k kontaktów):

```bash
# 1. TEST na 10 kontaktach
python cleanup_zoho.py --mode contacts --dry-run --limit 10

# 2. TEST na 1000 kontaktach (~8 minut)
python cleanup_zoho.py --mode contacts --dry-run --limit 1000

# 3. Uruchom partiami (co 5000 kontaktów, ~40 minut każda partia)
python cleanup_zoho.py --mode contacts --apply --limit 5000
# Powtórz kilkanaście razy lub uruchom pełną bazę (~10 godzin)
```

## 📈 Progress monitoring

Skrypt wyświetla progress co 10%:

```
[100/1000 - 10%] Scoring dla firmy ID: 751364000040575007
[200/1000 - 20%] Scoring dla firmy ID: 751364000040547037
...
[1000/1000 - 100%] Scoring dla firmy ID: 751364000016063439
```

## 🔧 Dalsze optymalizacje (jeśli potrzeba)

### 1. Batch updates dla Contacts
Zamiast 1 request na kontakt → 100 kontaktów w 1 request (bulk update)

### 2. Parallel processing
Użyj ThreadPoolExecutor dla równoległych requestów (max 5 wątków)

### 3. Checkpoint/Resume
Zapisuj progress co 1000 rekordów → możliwość wznowienia

### 4. Dedykowana maszyna
Uruchom na serwerze z lepszym łączem (szybsze requesty)

---

**Obecna implementacja jest wystarczająco szybka dla Twoich potrzeb!** ✅

