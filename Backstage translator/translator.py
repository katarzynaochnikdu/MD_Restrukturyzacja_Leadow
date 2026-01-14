from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
from rich.console import Console

from translator_config import AppConfig, get_api_key
from translator_io_utils import (
    copy_input_to_output,
    count_rows_to_translate,
    identify_columns,
    make_output_dir,
    preview_rows,
    read_csv,
    save_csv,
    save_xlsx,
    setup_file_logger,
    estimate_cost,
)
from translator_pipeline import Phase1Result, apply_results, run_phase_1, run_phase_3, should_verify
from translator_reporting import FinalReport
from translator_progress_ui import ProgressManager, print_config_summary


def ask_user_inputs() -> Tuple[Path, bool, int, int]:
    input_path = input("Ścieżka do pliku CSV (domyślnie Microsite short.csv): ").strip()
    # Clean PowerShell/bash artifacts: & '...' or "..."
    input_path = input_path.strip('"').strip("'")
    if input_path.startswith("& "):
        input_path = input_path[2:].strip().strip('"').strip("'")
    if not input_path:
        input_path = "Microsite short.csv"
    mode = input("Tryb: [1] tłumacz wszystko, [2] tylko puste PL (domyślne 2): ").strip() or "2"
    only_empty = mode != "1"
    vthr = input("VERIFY_THRESHOLD (domyślne 3): ").strip() or "3"
    longc = input("LONG_TEXT_CHARS (domyślne 100): ").strip() or "100"
    return Path(input_path), only_empty, int(vthr), int(longc)


def main() -> None:
    cfg = AppConfig()

    input_path, only_empty, vthr, longc = ask_user_inputs()
    cfg.verify_threshold = vthr
    cfg.long_text_chars = longc

    console = Console()
    console.print("Analiza pliku…")
    df, sep = read_csv(input_path)
    cols = identify_columns(df)

    total, to_translate, with_ph = count_rows_to_translate(df, cols, only_empty)
    sample = preview_rows(df, cols, limit=5)
    est_cost_min, est_cost_max, est_cost_avg = estimate_cost(
        num_requests=to_translate,
        avg_system_tokens=100,
        avg_user_tokens=80,
        avg_output_tokens=120,
        input_cost_per_1m=cfg.input_cost_per_1m,
        cached_input_cost_per_1m=cfg.cached_input_cost_per_1m,
        output_cost_per_1m=cfg.output_cost_per_1m,
    )

    print_config_summary(
        path=str(input_path),
        sep=sep,
        total=total,
        key_col=(cols.key_col_idx, cols.key_col_name),
        en_col=(cols.source_col_idx, cols.source_col_name),
        pl_col=(cols.target_col_idx, cols.target_col_name),
        to_translate=to_translate,
        placeholders=with_ph,
        sample=sample,
        est_cost_min=est_cost_min,
        est_cost_max=est_cost_max,
        est_cost_avg=est_cost_avg,
    )

    out_dir = make_output_dir(cfg.results_root, input_path)
    copied_input = copy_input_to_output(input_path, out_dir)
    log_path = setup_file_logger(out_dir)
    
    import logging
    logger = logging.getLogger("translator")
    logger.info(f"Started translation job for {input_path}")
    logger.info(f"Output directory: {out_dir}")
    logger.info(f"Total rows: {total}, to translate: {to_translate}, with placeholders: {with_ph}")

    rows_to_process: List[Tuple[int, str, str, str]] = []
    for idx, row in df.iterrows():
        en = str(row.iloc[cols.source_col_idx] or "")
        pl = str(row.iloc[cols.target_col_idx] or "")
        key = str(row.iloc[cols.key_col_idx] or "")
        if not en:
            continue
        if only_empty and pl:
            continue
        rows_to_process.append((idx, key, en, pl))

    if not rows_to_process:
        console.print("Brak wierszy do tłumaczenia. Koniec.")
        return

    pm = ProgressManager()
    pm.start()
    on_progress, on_avg = pm.make_phase_callbacks()
    api_key = get_api_key()
    
    try:
        phase1_by_row, phase1_stats = run_phase_1(cfg, out_dir, rows_to_process, on_progress=on_progress, on_avg_update=on_avg, api_key=api_key)
    except Exception as e:
        pm.stop()
        console.print(f"[red]Błąd w Phase 1: {e}[/red]")
        logger.error(f"Phase 1 error: {e}", exc_info=True)
        raise

    # Selection for verification
    on_progress("selection", 0, 1)
    to_verify: List[Tuple[int, str, str, Phase1Result]] = []
    for idx, key, en, _ in rows_to_process:
        r = phase1_by_row[idx]
        if should_verify(key, en, r, cfg):
            to_verify.append((idx, en, key, r))
    on_progress("selection", 1, 1)

    phase3_by_row: Dict[int, Dict] = {}
    phase3_stats = {'input_tokens': 0, 'output_tokens': 0, 'cached_tokens': 0, 'cost_usd': 0.0}
    if to_verify:
        phase3_by_row, phase3_stats = run_phase_3(cfg, out_dir, to_verify, on_progress=on_progress, api_key=api_key)

    on_progress("update_csv", 0, 1)
    updated_df, conf_hist, issues_count, critical_fixed = apply_results(df, cols, phase1_by_row, phase3_by_row)
    on_progress("update_csv", 1, 1)

    out_csv = out_dir / f"{input_path.stem}.translated.csv"
    out_xlsx = out_dir / f"{input_path.stem}.translated.xlsx"
    save_csv(updated_df, out_csv, sep)
    save_xlsx(updated_df, out_xlsx)

    # Final report
    total_cost = phase1_stats['cost_usd'] + phase3_stats['cost_usd']
    report = FinalReport(
        total_rows=total,
        translated_rows=len(rows_to_process),
        with_placeholders=with_ph,
        confidence_histogram={int(k): v for k, v in conf_hist.items()},
        total_issues=dict(issues_count),
        verified_count=len(phase3_by_row),
        fixed_count=sum(1 for v in phase3_by_row.values() if v.get("final_translation") not in (None, "", "BEZ_ZMIAN")),
        critical_fixed_keys=critical_fixed,
        output_files={
            "input_copy": str(copied_input),
            "log": str(log_path),
            "phase1_requests": str(out_dir / "phase1_requests.jsonl"),
            "phase1_responses": str(out_dir / "phase1_responses.jsonl"),
            "phase3_requests": str(out_dir / "phase3_requests.jsonl"),
            "phase3_responses": str(out_dir / "phase3_responses.jsonl"),
            "csv": str(out_csv),
            "xlsx": str(out_xlsx),
        },
        phase1_input_tokens=phase1_stats['input_tokens'],
        phase1_output_tokens=phase1_stats['output_tokens'],
        phase1_cached_tokens=phase1_stats['cached_tokens'],
        phase1_cost_usd=phase1_stats['cost_usd'],
        phase3_input_tokens=phase3_stats['input_tokens'],
        phase3_output_tokens=phase3_stats['output_tokens'],
        phase3_cached_tokens=phase3_stats['cached_tokens'],
        phase3_cost_usd=phase3_stats['cost_usd'],
        total_cost_usd=total_cost,
    )
    report.save(out_dir / "final_report.json")
    pm.stop()
    console.print("Gotowe.")


if __name__ == "__main__":
    main()

