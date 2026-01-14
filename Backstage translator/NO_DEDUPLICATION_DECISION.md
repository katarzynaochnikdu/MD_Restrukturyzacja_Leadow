# Decyzja: Usunięcie deduplikacji w Phase 1

## Podsumowanie
**Zdecydowano o całkowitym usunięciu deduplikacji** z `run_phase_1()` aby wyeliminować ryzyko pomyłek w mapowaniu tłumaczeń.

## Dlaczego?

### Problem z deduplikacją:
```python
# Z DEDUPLIKACJĄ (stary sposób):
unique_map = {"Sign in": [42, 137, 289]}  # ten sam EN w 3 miejscach
# Tłumaczenie 1x, potem kopiowanie wyniku do 3 wierszy
# ❌ Ryzyko: błąd w mapowaniu → 3 wiersze z błędem
```

### Rozwiązanie bez deduplikacji:
```python
# BEZ DEDUPLIKACJI (nowy sposób):
for row_idx, key, en, _ in rows:
    translation = api_call(en)  # każdy wiersz osobno
    results_by_row[row_idx] = translation  # 1:1
# ✅ Zero ryzyka pomyłek w mapowaniu
```

## Porównanie

| Aspekt | Z deduplikacją | Bez deduplikacji |
|--------|---------------|------------------|
| **Ryzyko pomyłek** | ⚠️ Średnie (mapowanie słownikowe) | ✅ Zero (1:1 mapping) |
| **Prostota kodu** | ⚠️ Złożony (grupowanie → rozgrupowanie) | ✅ Prosty (pętla for) |
| **Debugowanie** | ❌ Trudne (trzeba śledzić mapowanie) | ✅ Łatwe (linia logu = wiersz CSV) |
| **Koszt API** | ✅ Niższy (tłumaczenie unikalne) | ❌ Wyższy (każdy wiersz osobno) |
| **Spójność tłumaczeń** | ✅ Ten sam EN → to samo PL | ⚠️ Może się różnić (rzadko przy temp=0.25) |
| **Pewność działania** | ⚠️ Zależy od poprawności mapowania | ✅ Absolutna |

## Przykład: 1000 wierszy, 200 unikalnych EN

### Z deduplikacją:
- **Koszt**: 200 zapytań do API ≈ $0.50
- **Ryzyko**: Jeśli mapowanie się pomiesza → może być 1000 błędów
- **Czas debugowania**: Długi (trzeba zrozumieć deduplikację)

### Bez deduplikacji:
- **Koszt**: 1000 zapytań do API ≈ $2.50 (5x drożej)
- **Ryzyko**: Zero - każdy wiersz niezależny
- **Czas debugowania**: Krótki (1 linia logu = 1 wiersz)

## Decyzja użytkownika

**Priorytet: Pewność > Koszt**

Dla krytycznego pliku tłumaczeń UI, gdzie błędy są niedopuszczalne:
- ✅ Wyższy koszt API jest akceptowalny
- ✅ Prostszy kod = mniej bugów
- ✅ 1:1 mapping = zero ryzyka pomyłek
- ✅ Łatwiejsza weryfikacja

## Konsekwencje

### Kod (`translator_pipeline.py`):
```python
# PRZED (z deduplikacją):
unique_map: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
for row_idx, key, en, _ in rows:
    unique_map[en].append((row_idx, key))
# ... grupowanie, tłumaczenie, rozgrupowanie ...

# PO (bez deduplikacji):
for row_idx, key, en, _ in rows:
    # Tłumacz bezpośrednio
    translation = api_call(en)
    results_by_row[row_idx] = translation
```

### Logi:
```json
// PRZED (z deduplikacją):
{
  "en": "Sign in",
  "row_indices": [42, 137, 289],
  "keys": ["msg.signin", "lbl.login", "btn.auth"]
}

// PO (bez deduplikacji):
{"row_idx": 42, "key": "msg.signin", "en": "Sign in"}
{"row_idx": 137, "key": "lbl.login", "en": "Sign in"}
{"row_idx": 289, "key": "btn.auth", "en": "Sign in"}
```

### Koszty (przykładowe):
Zakładając plik z:
- 5000 wierszy do tłumaczenia
- 800 unikalnych tekstów EN
- Średnio 150 tokenów na request+response

**Z deduplikacją:**
- 800 requests × 150 tokens × $0.0000125 ≈ **$1.50**

**Bez deduplikacji:**
- 5000 requests × 150 tokens × $0.0000125 ≈ **$9.40**

**Różnica: ~$8 więcej**

Dla krytycznego pliku → **warto zapłacić** za pewność.

## Możliwość powrotu

Jeśli w przyszłości zechcesz wrócić do deduplikacji (np. dla oszczędności):

1. Odzyskaj kod z commita przed tą zmianą
2. Użyj wersji z `row_indices` i `keys` (wcześniejsza naprawa)
3. Dodaj testy jednostkowe dla mapowania
4. Waliduj wyniki przed zapisem

Ale dla krytycznych plików: **zostań przy NO DEDUPLICATION**.

## Podsumowanie

✅ **Decyzja: NO DEDUPLICATION**
- Prostszy kod
- Zero ryzyka pomyłek  
- Łatwe debugowanie
- 1:1 mapping (każda linia logu = dokładnie 1 wiersz CSV)
- Koszt wyższy, ale akceptowalny dla pewności

---

**Data decyzji**: 2025-01-18
**Priorytet**: Pewność działania > Oszczędność kosztów

