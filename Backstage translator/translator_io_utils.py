from __future__ import annotations

import csv
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd


CSV_SEPARATORS = [";", ",", "\t", "|"]


@dataclass
class CsvColumns:
    key_col_idx: int
    key_col_name: str
    source_col_idx: int
    source_col_name: str
    target_col_idx: int
    target_col_name: str


def detect_separator(sample: str) -> str:
    # prioritize semicolon
    counts = {sep: sample.count(sep) for sep in CSV_SEPARATORS}
    if counts[";"]:
        return ";"
    # else pick the one with highest count
    return max(counts, key=counts.get)


def read_csv(path: Path) -> Tuple[pd.DataFrame, str]:
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        head = f.read(5000)
    sep = detect_separator(head)
    df = pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8-sig", keep_default_na=False)
    # If first row seems to be header but pandas used the first line as header incorrectly,
    # promote the first data row to header.
    header_lower = [str(c or "").strip().lower() for c in df.columns]
    looks_like_desc_header = all(col.startswith("(") for col in header_lower if col)
    if looks_like_desc_header and len(df) > 0:
        # Check if first data row contains the expected header names
        row0 = [str(x or "").strip() for x in df.iloc[0].tolist()]
        row0_lower = [x.lower() for x in row0]
        if any("reference key" in x for x in row0_lower) and any(
            "default language" in x or x == "en" for x in row0_lower
        ):
            df.columns = row0
            df = df.iloc[1:].reset_index(drop=True)
    
    # CRITICAL: Always ensure contiguous integer index (0, 1, 2, ...)
    # Even if the CSV loads fine, we MUST have predictable indices for mapping
    df = df.reset_index(drop=True)
    
    # Normalize whitespace at ends, but not inside placeholders/HTML
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df, sep


def identify_columns(df: pd.DataFrame) -> CsvColumns:
    header = list(df.columns)
    # Row 1 is description in file; actual header is row 2 in provided spec.
    # In our read, pandas treated row 1 as header already. We accommodate both cases.
    canonical = [h.strip().lower() for h in header]

    def find_col(candidates: Iterable[str], fallback_idx: int) -> Tuple[int, str]:
        for name in candidates:
            lname = name.lower()
            if lname in canonical:
                idx = canonical.index(lname)
                return idx, header[idx]
        # fallback
        if 0 <= fallback_idx < len(header):
            return fallback_idx, header[fallback_idx]
        raise ValueError("Could not detect required column")

    key_idx, key_name = 0, header[0]
    src_idx, src_name = find_col(["Default Language", "default language", "en"], 1)
    tgt_idx, tgt_name = find_col(["pl", "polish"], 2)

    return CsvColumns(
        key_col_idx=key_idx,
        key_col_name=key_name,
        source_col_idx=src_idx,
        source_col_name=src_name,
        target_col_idx=tgt_idx,
        target_col_name=tgt_name,
    )


def preview_rows(df: pd.DataFrame, cols: CsvColumns, limit: int = 5) -> List[Tuple[str, str, str]]:
    rows = []
    for _, row in df.iloc[:limit].iterrows():
        key = row.iloc[cols.key_col_idx] or ""
        en = row.iloc[cols.source_col_idx] or ""
        pl = row.iloc[cols.target_col_idx] or ""
        rows.append((key, en, pl))
    return rows


def count_rows_to_translate(df: pd.DataFrame, cols: CsvColumns, only_empty_pl: bool) -> Tuple[int, int, int]:
    total = len(df)
    placeholders = 0
    to_translate = 0
    for _, row in df.iterrows():
        en = str(row.iloc[cols.source_col_idx] or "")
        pl = str(row.iloc[cols.target_col_idx] or "")
        if not en:
            continue
        if only_empty_pl and pl:
            continue
        to_translate += 1
        if "{" in en and "}" in en:
            placeholders += 1
    return total, to_translate, placeholders


def make_output_dir(results_root: str, input_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = input_path.stem.replace(" ", "_")[:40]
    out = Path(results_root) / f"{stem}__{ts}"
    out.mkdir(parents=True, exist_ok=True)
    return out


def copy_input_to_output(input_path: Path, out_dir: Path) -> Path:
    dest = out_dir / input_path.name
    shutil.copy2(input_path, dest)
    return dest


def save_csv(df: pd.DataFrame, path: Path, sep: str) -> None:
    df.to_csv(path, sep=sep, index=False, encoding="utf-8-sig")


def save_xlsx(df: pd.DataFrame, path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)


def setup_file_logger(out_dir: Path) -> Path:
    import logging
    log_path = out_dir / "run.log"
    
    # Setup file logger
    logger = logging.getLogger("translator")
    logger.setLevel(logging.INFO)
    
    # File handler
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return log_path


def estimate_cost(
    num_requests: int,
    avg_system_tokens: int = 100,
    avg_user_tokens: int = 80,
    avg_output_tokens: int = 120,
    input_cost_per_1m: float = 1.25,
    cached_input_cost_per_1m: float = 0.125,
    output_cost_per_1m: float = 10.00,
) -> Tuple[float, float, float]:
    """
    Estimate cost with prompt caching.
    Returns: (min_cost_usd, max_cost_usd, avg_cost_usd)
    """
    # First request: full cost (no cache)
    first_input_tokens = avg_system_tokens + avg_user_tokens
    first_cost = (first_input_tokens / 1_000_000) * input_cost_per_1m + (avg_output_tokens / 1_000_000) * output_cost_per_1m
    
    # Subsequent requests: cached system + new user
    subsequent_input_tokens_cached = avg_system_tokens
    subsequent_input_tokens_new = avg_user_tokens
    subsequent_cost = (
        (subsequent_input_tokens_cached / 1_000_000) * cached_input_cost_per_1m +
        (subsequent_input_tokens_new / 1_000_000) * input_cost_per_1m +
        (avg_output_tokens / 1_000_000) * output_cost_per_1m
    )
    
    if num_requests <= 1:
        return first_cost, first_cost, first_cost
    
    # Total cost = first + (n-1) * subsequent
    total_cost = first_cost + (num_requests - 1) * subsequent_cost
    
    # Max (no cache), Min (full cache after first), Avg
    max_cost = num_requests * first_cost
    min_cost = total_cost
    avg_cost = (min_cost + max_cost) / 2
    
    return round(min_cost, 2), round(max_cost, 2), round(avg_cost, 2)

