## Backstage Translator (EN→PL) – Batch A+C

Minimal CLI to translate `Microsite.csv` (and similar) from English to Polish using `gpt-5` Batch with a hybrid A+C flow:
- A: initial translation with self-assessment
- C: selective verification only where needed

### Requirements
- Python 3.10+
- OpenAI API key in environment `OPENAI_API_KEY` (you can use a `.env` file)

```
pip install -r requirements.txt
```

### Run
```
python run.py
```

The CLI will ask for:
- path to input CSV (e.g. `Microsite.csv` or `Microsite short.csv`)
- mode: translate all vs only empty PL
- thresholds and batch sizes

### Output
Each run creates a timestamped result folder containing:
- input copy
- program log (full)
- Phase 1/3 request/response JSONL logs
- translated CSV and XLSX
- final report (JSON)

#### Phase 1 Logs Format (No Deduplication)
Phase 1 logs use **1:1 mapping** for maximum reliability:
- `row_idx`: single DataFrame row index
- `key`: single translation key (ID)
- `en`: the source English text
- `response`: the translation response from the model
- `usage`: token usage statistics

**No deduplication**: Each row is translated independently, eliminating any risk of mapping errors. Each log line corresponds exactly to one CSV row.

#### Verifying Log Integrity
After translation, run:
```bash
python verify_logs.py
```
This validates that all logs contain complete row and key information.

#### Post-Translation Validation
Validate the final translation for placeholder and HTML integrity:
```bash
# Validate specific file
python validate_translation.py results/[folder]/file.translated.csv

# Or auto-find latest translation
python validate_latest.py
```
This checks:
- 🚨 Placeholders `{...}` match 1:1
- 🚨 HTML tags are identical
- ⚠️ Text length is reasonable
- ⚠️ No untranslated fragments

See `VALIDATION_USAGE.md` for details.

### Notes
- CSV separator is auto-detected (prioritizes `;`)
- Protects placeholders `{...}` and HTML tags 1:1
- Uses masculine forms by default (per domain guidelines)
- Minimal console noise; progress bars via `tqdm`
- **No deduplication**: Each row is translated separately for maximum reliability (1:1 mapping)

