"""
⚠️ UNUSED - This file is NOT currently used in the codebase.
   
The translator now uses DIRECT API calls (client.chat.completions.create)
instead of Batch API for maximum reliability and 1:1 mapping.

This file is kept for reference in case you want to implement Batch API 
in the future (50% cheaper, but takes up to 24h for results).

To use Batch API again, you would need to:
1. Import OpenAIBatchClient in translator_pipeline.py
2. Modify run_phase_1() to use batch submission
3. Add polling and result retrieval logic

Current approach: Single requests (immediate, simple, reliable)
Batch approach: Bulk requests (cheaper, slower, more complex)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from openai import OpenAI


@dataclass
class BatchJob:
    id: str
    status: str
    input_file_id: str
    output_file_id: Optional[str]


class OpenAIBatchClient:
    def __init__(self, model: str, completion_window: str = "24h", api_key: Optional[str] = None) -> None:
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.completion_window = completion_window

    def _write_jsonl(self, requests: List[Dict], path: Path) -> None:
        with path.open("w", encoding="utf-8") as f:
            for req in requests:
                f.write(json.dumps(req, ensure_ascii=False) + "\n")

    def _prepare_requests_chat(self, items: Iterable[Tuple[str, List[Dict]]]) -> List[Dict]:
        # items: (custom_id, messages)
        requests: List[Dict] = []
        for custom_id, messages in items:
            body = {
                "model": self.model,
                "messages": messages,
                "temperature": 0.25,
                "response_format": {"type": "json_object"},
            }
            requests.append({
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": body,
            })
        return requests

    def submit_chat_batch(self, items: Iterable[Tuple[str, List[Dict]]], work_dir: Path, job_name: str) -> BatchJob:
        reqs = self._prepare_requests_chat(items)
        input_jsonl = work_dir / f"{job_name}.input.jsonl"
        self._write_jsonl(reqs, input_jsonl)

        input_upload = self.client.files.create(file=open(input_jsonl, "rb"), purpose="batch")
        batch = self.client.batches.create(
            input_file_id=input_upload.id,
            endpoint="/v1/chat/completions",
            completion_window=self.completion_window,
        )
        return BatchJob(id=batch.id, status=batch.status, input_file_id=input_upload.id, output_file_id=None)

    def poll(self, job: BatchJob, interval_s: float = 5.0) -> BatchJob:
        while True:
            b = self.client.batches.retrieve(job.id)
            status = b.status
            if status in {"completed", "failed", "expired", "cancelled"}:
                return BatchJob(id=b.id, status=status, input_file_id=b.input_file_id, output_file_id=b.output_file_id)
            time.sleep(interval_s)

    def download_output(self, job: BatchJob, work_dir: Path, job_name: str) -> Path:
        if not job.output_file_id:
            raise RuntimeError("Batch has no output_file_id")
        out = self.client.files.content(job.output_file_id)
        out_path = work_dir / f"{job_name}.output.jsonl"
        with out_path.open("wb") as f:
            for chunk in out.iter_bytes():
                f.write(chunk)
        return out_path

    @staticmethod
    def parse_output_chat(output_jsonl: Path) -> Dict[str, Dict]:
        results: Dict[str, Dict] = {}
        with output_jsonl.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                obj = json.loads(line)
                custom_id = obj.get("custom_id")
                body = obj.get("response", {}).get("body", {})
                choices = body.get("choices", [])
                if not choices:
                    results[custom_id] = {"error": obj.get("error") or body}
                    continue
                content = choices[0]["message"]["content"]
                # The model returns a JSON string; we attempt parsing strictly
                try:
                    parsed = json.loads(content)
                    results[custom_id] = parsed
                except Exception as e:
                    results[custom_id] = {"parse_error": str(e), "raw": content}
        return results

