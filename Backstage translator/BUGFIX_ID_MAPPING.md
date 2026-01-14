# Naprawa: Problem z pomieszanymi ID w tłumaczeniach

## Problem
W Phase 1 logi (`phase1_requests.jsonl`, `phase1_responses.jsonl`) **nie zawierały informacji o row_idx i key**, co uniemożliwiało weryfikację, czy tłumaczenia zostały poprawnie przypisane do odpowiednich wierszy.

### Objawy:
- Tłumaczenia były "pomieszane" - tekst z jednego ID trafiał do innego
- Niemożność odtworzenia mapowania EN → PL dla konkretnych kluczy z logów
- Brak możliwości debugowania problemów z przypisaniem tłumaczeń

### Przyczyna:
W `translator_pipeline.py`, funkcja `run_phase_1()`:

**Przed naprawą:**
```python
# Logi zawierały tylko tekst EN:
req_f.write(json.dumps({"en": en, "messages": messages}, ensure_ascii=False))
resp_f.write(json.dumps({"en": en, "response": content, "usage": ...}, ensure_ascii=False))

# Mapowanie:
unique_map: Dict[str, List[int]] = defaultdict(list)  # tylko indeksy
for row_idx, key, en, _ in rows:
    unique_map[en].append(row_idx)  # gubiono informację o key!
```

**Problem**: Gdy ten sam tekst EN występował w wielu wierszach:
- Deduplikacja grupowała je razem (poprawnie)
- Ale logi nie zapisywały **których dokładnie wierszy i kluczy** to dotyczyło
- W razie błędu w mapowaniu - nie było sposobu to zweryfikować

## Rozwiązanie

### 1. Zmieniona struktura danych deduplikacji
```python
# Teraz przechowujemy pełne informacje:
unique_map: Dict[str, List[Tuple[int, str]]] = defaultdict(list)  # en -> [(row_idx, key), ...]
for row_idx, key, en, _ in rows:
    unique_map[en].append((row_idx, key))  # zachowujemy zarówno row_idx jak i key
```

### 2. Rozszerzone logowanie requests
```python
for en, row_info_list in unique_map.items():
    # Ekstrakcja wszystkich row_idx i keys:
    row_indices = [idx for idx, _ in row_info_list]
    keys = [key for _, key in row_info_list]
    
    # Pełne logowanie:
    req_f.write(json.dumps({
        "en": en,
        "row_indices": row_indices,  # NOWE!
        "keys": keys,                # NOWE!
        "messages": messages
    }, ensure_ascii=False) + "\n")
```

### 3. Rozszerzone logowanie responses
```python
resp_f.write(json.dumps({
    "en": en,
    "row_indices": row_indices,  # NOWE!
    "keys": keys,                # NOWE!
    "response": content,
    "usage": response.usage.model_dump() if response.usage else {}
}, ensure_ascii=False) + "\n")
```

### 4. Zaktualizowane mapowanie wyników
```python
# Mapowanie z powrotem używa nowej struktury:
results_by_row: Dict[int, Phase1Result] = {}
for en, row_info_list in unique_map.items():
    res = results_by_en[en]
    for idx, key in row_info_list:  # teraz mamy dostęp do key
        results_by_row[idx] = res
```

## Weryfikacja

Po następnym uruchomieniu tłumaczenia, uruchom:

```bash
python verify_logs.py
```

Skrypt sprawdzi czy:
1. ✓ Logi Phase 1 zawierają `row_indices`, `keys`, `en`
2. ✓ Logi Phase 3 zawierają `row_idx`, `key`, `en` (to już działało poprawnie)
3. ✓ Każde tłumaczenie ma pełne informacje o mapowaniu

### Przykład nowego formatu logu:

**phase1_requests.jsonl:**
```json
{
  "en": "Sign in to your account",
  "row_indices": [42, 137],
  "keys": ["msg.signin.title", "lbl.login.header"],
  "messages": [...]
}
```

**phase1_responses.jsonl:**
```json
{
  "en": "Sign in to your account",
  "row_indices": [42, 137],
  "keys": ["msg.signin.title", "lbl.login.header"],
  "response": "{\"translation\":\"Zaloguj się do swojego konta\", ...}",
  "usage": {"prompt_tokens": 120, ...}
}
```

**Interpretacja**: Ten sam tekst EN był użyty w 2 miejscach (wiersze 42 i 137, klucze `msg.signin.title` i `lbl.login.header`). Tłumaczenie "Zaloguj się do swojego konta" zostało przypisane do OBIE lokalizacji.

## Dlaczego to rozwiązuje problem

1. **Pełna audytowalność**: Każde tłumaczenie ma pełną listę row_idx i keys, których dotyczy
2. **Weryfikacja mapowania**: Można sprawdzić w logach czy tłumaczenie trafiło do właściwych wierszy
3. **Debugowanie**: W razie problemów można dokładnie zidentyfikować który row_idx dostał które tłumaczenie
4. **Deduplikacja nadal działa**: Nie wysyłamy duplikatów do API, ale wiemy które wiersze dostają to samo tłumaczenie

## Nie zmienione
- ✓ Model językowy (gpt-4.1) - bez zmian
- ✓ Prompty - bez zmian
- ✓ Logika deduplikacji - działa tak samo (grupuje identyczne EN)
- ✓ Temperatura i parametry - bez zmian
- ✓ Struktura outputu (CSV/XLSX) - bez zmian

## Co dalej

1. Uruchom kolejne tłumaczenie
2. Sprawdź logi używając `python verify_logs.py`
3. Jeśli zobaczysz pomieszane tłumaczenia, sprawdź w logach:
   - Jaki `en` tekst został przetłumaczony
   - Które `row_indices` i `keys` dostały to tłumaczenie
   - Czy mapowanie jest poprawne według logów

Jeśli mapowanie w logach jest poprawne ale output nadal jest pomieszany, problem jest w innym miejscu (np. w `apply_results()` lub zapisie CSV).

