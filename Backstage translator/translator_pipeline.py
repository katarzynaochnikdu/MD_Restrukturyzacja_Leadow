from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from translator_config import AppConfig
from translator_io_utils import CsvColumns
from translator_placeholders import compare_html, compare_placeholders, extract_placeholders
from translator_prompts import (
    SYSTEM_PHASE_1,
    SYSTEM_PHASE_3,
    build_user_phase_1,
    build_user_phase_3,
)


@dataclass
class Phase1Result:
    translation: str
    confidence: int
    confidence_reason: str
    placeholders_found: List[str]
    placeholders_preserved: bool
    html_preserved: bool
    issues: List[str]


def run_phase_1(
    cfg: AppConfig,
    out_dir: Path,
    rows: List[Tuple[int, str, str, str]],
    on_progress=None,
    on_avg_update=None,
    api_key: str = None,
) -> Dict[int, Phase1Result]:
    # rows: (row_idx, key, source_en, current_pl)
    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    # NO DEDUPLICATION - każdy wiersz osobno dla maksymalnej pewności mapowania
    results_by_row: Dict[int, Phase1Result] = {}
    conf_values: List[int] = []
    total = len(rows)
    processed = 0
    
    # Cost tracking
    input_tokens_total = 0
    output_tokens_total = 0
    cached_tokens_total = 0

    if on_progress:
        on_progress("process_results", 0, total)

    # Save request log
    requests_log = out_dir / "phase1_requests.jsonl"
    responses_log = out_dir / "phase1_responses.jsonl"
    req_f = open(requests_log, "w", encoding="utf-8")
    resp_f = open(responses_log, "w", encoding="utf-8")

    for row_idx, key, en, _ in rows:
        placeholders = extract_placeholders(en)
        user_prompt = build_user_phase_1(en, placeholders)
        messages = [
            {"role": "system", "content": SYSTEM_PHASE_1},
            {"role": "user", "content": user_prompt},
        ]
        
        # Log request - pojedynczy wiersz, jasne mapowanie 1:1
        req_f.write(json.dumps({
            "row_idx": row_idx,
            "key": key,
            "en": en,
            "messages": messages
        }, ensure_ascii=False) + "\n")
        req_f.flush()
        
        try:
            response = client.chat.completions.create(
                model=cfg.model,
                messages=messages,
                temperature=cfg.temperature,
                response_format={"type": "json_object"},
            )
            
            # Track usage
            usage = response.usage
            if usage:
                input_tokens_total += usage.prompt_tokens
                output_tokens_total += usage.completion_tokens
                if hasattr(usage, 'prompt_tokens_details') and usage.prompt_tokens_details:
                    cached_tokens_total += getattr(usage.prompt_tokens_details, 'cached_tokens', 0)
            
            content = response.choices[0].message.content
            
            # Log response - pojedynczy wiersz
            resp_f.write(json.dumps({
                "row_idx": row_idx,
                "key": key,
                "en": en,
                "response": content,
                "usage": response.usage.model_dump() if response.usage else {}
            }, ensure_ascii=False) + "\n")
            resp_f.flush()
            
            obj = json.loads(content)
            
            res = Phase1Result(
                translation=str(obj.get("translation", "")),
                confidence=int(obj.get("confidence", 1)),
                confidence_reason=str(obj.get("confidence_reason", "")),
                placeholders_found=list(obj.get("placeholders_found", [])),
                placeholders_preserved=bool(obj.get("placeholders_preserved", False)),
                html_preserved=bool(obj.get("html_preserved", False)),
                issues=list(obj.get("issues", [])),
            )
        except Exception as e:
            # Log error
            resp_f.write(json.dumps({
                "row_idx": row_idx,
                "key": key,
                "en": en,
                "error": str(e)
            }, ensure_ascii=False) + "\n")
            resp_f.flush()
            
            res = Phase1Result(
                translation="",
                confidence=1,
                confidence_reason=f"api_error: {str(e)}",
                placeholders_found=extract_placeholders(en),
                placeholders_preserved=False,
                html_preserved=False,
                issues=["api_error"],
            )
        
        # Direct 1:1 mapping - zero risk of mix-up
        results_by_row[row_idx] = res
        conf_values.append(res.confidence)
        processed += 1
        
        if on_progress:
            on_progress("process_results", processed, total)
        if on_avg_update and conf_values:
            on_avg_update(sum(conf_values) / len(conf_values))

    avg_conf = sum(conf_values) / max(1, len(conf_values))
    if on_avg_update:
        on_avg_update(avg_conf)
    
    # Calculate actual cost
    actual_cost = (
        (input_tokens_total - cached_tokens_total) / 1_000_000 * cfg.input_cost_per_1m +
        cached_tokens_total / 1_000_000 * cfg.cached_input_cost_per_1m +
        output_tokens_total / 1_000_000 * cfg.output_cost_per_1m
    )
    
    # Close log files
    req_f.close()
    resp_f.close()
    
    # Print cost summary
    from rich.console import Console
    console = Console()
    console.print(f"\n💰 Koszt Phase 1: ${actual_cost:.4f} | Tokeny: in={input_tokens_total}, out={output_tokens_total}, cached={cached_tokens_total}")
    
    # Return results + stats
    stats = {
        'input_tokens': input_tokens_total,
        'output_tokens': output_tokens_total,
        'cached_tokens': cached_tokens_total,
        'cost_usd': actual_cost
    }
    
    return results_by_row, stats


def should_verify(
    key: str,
    en: str,
    r: Phase1Result,
    cfg: AppConfig,
) -> bool:
    long_text = len(en) > cfg.long_text_chars
    critical = (
        key.startswith("msg.error.")
        or key.startswith("lbl.")
        or key.startswith("btn.")
        or key.startswith("format.")
    )
    # Always verify on low confidence or structural issues
    if r.confidence <= 2:
        return True
    ok_ph, _ = compare_placeholders(en, r.translation)
    ok_html, _ = compare_html(en, r.translation)
    if not ok_ph or not ok_html:
        return True
    # Conditional
    if r.confidence == 3 and (long_text or "{" in en or critical):
        return True
    # Optional
    if r.confidence == 4 and ("{" in en) and (long_text or critical):
        return True
    return False


def run_phase_3(
    cfg: AppConfig,
    out_dir: Path,
    items: List[Tuple[int, str, str, Phase1Result]],
    on_progress=None,
    api_key: str = None,
) -> Tuple[Dict[int, Dict], Dict]:
    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    if not items:
        return {}, {'input_tokens': 0, 'output_tokens': 0, 'cached_tokens': 0, 'cost_usd': 0.0}

    results_by_row: Dict[int, Dict] = {}
    total = len(items)
    processed = 0
    
    # Cost tracking
    input_tokens_total = 0
    output_tokens_total = 0
    cached_tokens_total = 0

    if on_progress:
        on_progress("verify_process", 0, total)

    # Save request/response logs
    requests_log = out_dir / "phase3_requests.jsonl"
    responses_log = out_dir / "phase3_responses.jsonl"
    req_f = open(requests_log, "w", encoding="utf-8")
    resp_f = open(responses_log, "w", encoding="utf-8")

    for row_idx, en, key, res in items:
        user_prompt = build_user_phase_3(
            source_en=en,
            proposal_pl=res.translation,
            confidence=res.confidence,
            reason=res.confidence_reason,
            placeholders_en=extract_placeholders(en),
        )
        messages = [
            {"role": "system", "content": SYSTEM_PHASE_3},
            {"role": "user", "content": user_prompt},
        ]
        
        # Log request
        req_f.write(json.dumps({"row_idx": row_idx, "key": key, "en": en, "messages": messages}, ensure_ascii=False) + "\n")
        req_f.flush()

        try:
            response = client.chat.completions.create(
                model=cfg.model,
                messages=messages,
                temperature=cfg.temperature,
                response_format={"type": "json_object"},
            )
            
            # Track usage
            usage = response.usage
            if usage:
                input_tokens_total += usage.prompt_tokens
                output_tokens_total += usage.completion_tokens
                if hasattr(usage, 'prompt_tokens_details') and usage.prompt_tokens_details:
                    cached_tokens_total += getattr(usage.prompt_tokens_details, 'cached_tokens', 0)
            
            content = response.choices[0].message.content
            
            # Log response
            resp_f.write(json.dumps({"row_idx": row_idx, "key": key, "response": content}, ensure_ascii=False) + "\n")
            resp_f.flush()
            
            obj = json.loads(content)
            results_by_row[row_idx] = obj
        except Exception as e:
            # Log error
            resp_f.write(json.dumps({"row_idx": row_idx, "key": key, "error": str(e)}, ensure_ascii=False) + "\n")
            resp_f.flush()
            
            results_by_row[row_idx] = {
                "final_translation": "",
                "final_confidence": 1,
                "final_reason": f"api_error: {str(e)}",
                "fixed_issues": ["api_error"],
            }

        processed += 1
        if on_progress:
            on_progress("verify_process", processed, total)

    req_f.close()
    resp_f.close()
    
    # Calculate cost
    actual_cost = (
        (input_tokens_total - cached_tokens_total) / 1_000_000 * cfg.input_cost_per_1m +
        cached_tokens_total / 1_000_000 * cfg.cached_input_cost_per_1m +
        output_tokens_total / 1_000_000 * cfg.output_cost_per_1m
    )
    
    stats = {
        'input_tokens': input_tokens_total,
        'output_tokens': output_tokens_total,
        'cached_tokens': cached_tokens_total,
        'cost_usd': actual_cost
    }
    
    if total > 0:
        from rich.console import Console
        console = Console()
        console.print(f"\n💰 Koszt Phase 3: ${actual_cost:.4f} | Tokeny: in={input_tokens_total}, out={output_tokens_total}, cached={cached_tokens_total}")
    
    return results_by_row, stats


def apply_results(
    df: pd.DataFrame,
    cols: CsvColumns,
    phase1_by_row: Dict[int, Phase1Result],
    phase3_by_row: Dict[int, Dict],
) -> Tuple[pd.DataFrame, Dict[str, int], Dict[str, int], List[str]]:
    conf_hist: Dict[str, int] = {str(i): 0 for i in range(1, 6)}
    issues_count: Dict[str, int] = defaultdict(int)
    critical_fixed_keys: List[str] = []

    for idx, row in df.iterrows():
        if idx not in phase1_by_row:
            continue
        p1 = phase1_by_row[idx]
        conf_hist[str(p1.confidence)] += 1
        for it in p1.issues:
            issues_count[it] += 1

        final_pl = p1.translation
        # auto validation after Phase 1
        ok_ph, ph_issues = compare_placeholders(row.iloc[cols.source_col_idx], final_pl)
        ok_html, html_issues = compare_html(row.iloc[cols.source_col_idx], final_pl)
        for it in ph_issues + html_issues:
            issues_count[it] += 1

        if idx in phase3_by_row:
            obj = phase3_by_row[idx]
            try:
                ft = str(obj.get("final_translation", ""))
                fc = int(obj.get("final_confidence", p1.confidence))
                fr = str(obj.get("final_reason", ""))
                fixed_issues = list(obj.get("fixed_issues", []))
            except Exception:
                ft = ""
                fc = p1.confidence
                fr = "invalid_json"
                fixed_issues = ["invalid_json"]

            if ft and ft != "BEZ_ZMIAN":
                final_pl = ft
                for it in fixed_issues:
                    issues_count[f"fixed:{it}"] += 1
                key = row.iloc[cols.key_col_idx]
                if key.startswith("msg.error.") or key.startswith("lbl.") or key.startswith("btn."):
                    critical_fixed_keys.append(key)

        # CRITICAL FIX: Use df.at (label-based) NOT df.iat (position-based)!
        # df.iat uses POSITION (0,1,2...), but idx from iterrows() is PANDAS INDEX
        # If DataFrame has non-contiguous indices (e.g. [0,5,7,10]), df.iat[5] 
        # would access the 5th POSITION (which might be index 7), not index 5!
        df.at[idx, cols.target_col_name] = final_pl

    return df, conf_hist, issues_count, critical_fixed_keys

