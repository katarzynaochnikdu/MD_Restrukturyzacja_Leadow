from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


@dataclass
class RowAssessment:
    key: str
    confidence: int
    issues: List[str] = field(default_factory=list)
    selected_for_verification: bool = False
    fixed_in_verification: bool = False


@dataclass
class FinalReport:
    total_rows: int
    translated_rows: int
    with_placeholders: int
    confidence_histogram: Dict[int, int]
    total_issues: Dict[str, int]
    verified_count: int
    fixed_count: int
    critical_fixed_keys: List[str]
    output_files: Dict[str, str]
    # Token usage and cost
    phase1_input_tokens: int = 0
    phase1_output_tokens: int = 0
    phase1_cached_tokens: int = 0
    phase1_cost_usd: float = 0.0
    phase3_input_tokens: int = 0
    phase3_output_tokens: int = 0
    phase3_cached_tokens: int = 0
    phase3_cost_usd: float = 0.0
    total_cost_usd: float = 0.0

    def save(self, out_path: Path) -> None:
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(self.__dict__, f, ensure_ascii=False, indent=2)

