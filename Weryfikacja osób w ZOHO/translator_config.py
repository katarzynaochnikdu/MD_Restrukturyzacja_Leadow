import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


@dataclass
class AppConfig:
    model: str = os.getenv("OPENAI_MODEL", "gpt-4.1")
    temperature: float = float(os.getenv("TEMPERATURE", "0.25"))
    batch_completion_window: str = os.getenv("BATCH_COMPLETION_WINDOW", "24h")
    # Default thresholds
    verify_threshold: int = int(os.getenv("VERIFY_THRESHOLD", "3"))
    long_text_chars: int = int(os.getenv("LONG_TEXT_CHARS", "100"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "500"))
    max_parallel_batches: int = int(os.getenv("MAX_PARALLEL_BATCHES", "1"))
    # Files/dirs
    results_root: str = os.getenv("RESULTS_ROOT", "results")
    logs_dirname: str = os.getenv("LOGS_DIRNAME", "logs")
    # Pricing for gpt-4.1 (per 1M tokens, Standard tier)
    input_cost_per_1m: float = 2.50
    cached_input_cost_per_1m: float = 1.25
    output_cost_per_1m: float = 10.00


def get_api_key() -> str:
    api_key = os.getenv("API_KEY_OPENAI_medidesk")
    if not api_key:
        raise RuntimeError("Missing API_KEY_OPENAI_medidesk in environment or .env")
    return api_key

