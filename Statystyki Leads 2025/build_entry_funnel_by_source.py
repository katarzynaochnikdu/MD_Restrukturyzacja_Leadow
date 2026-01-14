"""
Buduje biznesowy funnel (entry -> meeting -> deal -> won) z lifecycle_2025.csv.

Definicje:
- entry_time = ml_stage_start_time jeśli istnieje, w przeciwnym razie lead_stage_start_time
- has_meeting = events_count_lead_stage > 0 (spotkania liczymy tylko na etapie Lead)
- time_to_meeting_days = lead_first_meeting_time - entry_time (dni; tylko tam gdzie jest lead_first_meeting_time)
- converted_to_deal = deal_id != '' (oraz deal_stage_start_time niepuste jeśli chcesz ostrzej)
- won = deal_is_won == True

Wyniki:
- curated_data/funnel_entry_month_source.csv  (miesiąc + źródło I/II/III)
- curated_data/funnel_entry_month_global.csv  (miesiąc globalnie)
- curated_data/funnel_source_global.csv       (źródło globalnie)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from config import CURATED_DATA_DIR, ANALYSIS_YEAR


LIFECYCLE_FILE = Path(CURATED_DATA_DIR) / "lifecycle_2025.csv"
OUT_MONTH_SOURCE = Path(CURATED_DATA_DIR) / "funnel_entry_month_source.csv"
OUT_MONTH_GLOBAL = Path(CURATED_DATA_DIR) / "funnel_entry_month_global.csv"
OUT_SOURCE_GLOBAL = Path(CURATED_DATA_DIR) / "funnel_source_global.csv"


def _to_dt(series: pd.Series) -> pd.Series:
    # lifecycle zapisuje ISO; robimy spójne daty (naive)
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    # pandas -> tz-aware; na potrzeby różnic w dniach nie ma znaczenia, ale ujednolicamy
    return dt.dt.tz_convert(None)


def _to_bool(series: pd.Series) -> pd.Series:
    # W CSV mamy "True"/"False"/puste
    s = series.fillna("").astype(str).str.strip().str.lower()
    return s.isin({"true", "1", "yes", "y", "t"})


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)


def _norm_text(series: pd.Series, fallback: str = "brak danych") -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    return s.mask(s == "", fallback)


def _month_key(dt: pd.Series) -> pd.Series:
    # YYYY-MM
    return dt.dt.strftime("%Y-%m")


def _year_filter(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Filtruje do rekordów z aktywnością w danym roku.
    Lifecycle_2025.csv już jest filtrowany do 2025 - zostawiamy wszystkie rekordy.
    """
    # Nie filtrujemy - lifecycle jest już przefiltrowany do 2025
    return df.copy()


def _compute_metrics(group: pd.DataFrame) -> pd.Series:
    n_entered = int(len(group))
    n_with_meeting = int(group["has_meeting"].sum())
    n_to_deal = int(group["has_deal"].sum())
    n_won = int(group["is_won"].sum())
    # OJP: średnia "na projekt" (projekty bez ankiet są pomijane)
    avg_ojp = round(group["ankieta_avg_quality_num"].mean(), 2) if pd.notna(group["ankieta_avg_quality_num"].mean()) else ""
    # OJP: średnia ważona liczbą ankiet (średnia "z ankiet")
    ank_cnt = pd.to_numeric(group.get("ankiety_count", pd.Series(dtype=str)), errors="coerce").fillna(0)
    ojp = group["ankieta_avg_quality_num"]
    sum_ankiety = int(ank_cnt.sum())
    denom = float(ank_cnt[ojp.notna()].sum())
    if denom > 0:
        avg_ojp_weighted = round(float((ojp * ank_cnt).sum(skipna=True) / denom), 2)
    else:
        avg_ojp_weighted = ""

    # time_to_meeting_days: tylko gdzie mamy policzone
    ttm = group["time_to_meeting_days"].dropna()
    median_days = float(ttm.median()) if len(ttm) else float("nan")
    mean_days = float(ttm.mean()) if len(ttm) else float("nan")

    def pct(num: int, den: int) -> float:
        return round((num / den) * 100.0, 2) if den else 0.0

    return pd.Series(
        {
            "N_entered": n_entered,
            "N_with_meeting": n_with_meeting,
            "pct_with_meeting": pct(n_with_meeting, n_entered),
            "median_days_to_meeting": round(median_days, 2) if pd.notna(median_days) else "",
            "mean_days_to_meeting": round(mean_days, 2) if pd.notna(mean_days) else "",
            "N_to_deal": n_to_deal,
            "pct_to_deal": pct(n_to_deal, n_entered),
            "N_won": n_won,
            "pct_won_of_entered": pct(n_won, n_entered),
            "win_rate_of_deals": pct(n_won, n_to_deal),
            "avg_ojp": avg_ojp,
            "sum_ankiety": sum_ankiety,
            "avg_ojp_weighted": avg_ojp_weighted,
        }
    )


def main() -> None:
    if not LIFECYCLE_FILE.exists():
        raise FileNotFoundError(f"Brak {LIFECYCLE_FILE}. Uruchom najpierw build_lifecycle.py")

    df = pd.read_csv(LIFECYCLE_FILE, dtype=str)

    # Daty
    ml_start = _to_dt(df.get("ml_stage_start_time", pd.Series(dtype=str)))
    lead_start = _to_dt(df.get("lead_stage_start_time", pd.Series(dtype=str)))
    lead_first_meeting = _to_dt(df.get("lead_first_meeting_time", pd.Series(dtype=str)))

    df["ml_stage_start_time_dt"] = ml_start
    df["lead_stage_start_time_dt"] = lead_start
    df["lead_first_meeting_time_dt"] = lead_first_meeting
    df["deal_stage_start_time_dt"] = _to_dt(df.get("deal_stage_start_time", pd.Series(dtype=str)))

    # entry_time = ML jeśli jest, inaczej Lead, inaczej Deal (dla deal-only)
    df["entry_time"] = (
        df["ml_stage_start_time_dt"]
        .where(df["ml_stage_start_time_dt"].notna(), df["lead_stage_start_time_dt"])
        .where(
            df["ml_stage_start_time_dt"].notna() | df["lead_stage_start_time_dt"].notna(),
            df["deal_stage_start_time_dt"]
        )
    )
    df = _year_filter(df, ANALYSIS_YEAR)

    # Źródła
    df["source_I_tier"] = _norm_text(df.get("source_I_tier", pd.Series(dtype=str)))
    df["source_II_tier"] = _norm_text(df.get("source_II_tier", pd.Series(dtype=str)))
    df["source_III_tier"] = _norm_text(df.get("source_III_tier", pd.Series(dtype=str)))

    # Meeting (TAK/NIE)
    df["events_count_lead_stage_int"] = _to_int(df.get("events_count_lead_stage", pd.Series(dtype=str)))
    df["has_meeting"] = df["events_count_lead_stage_int"] > 0

    # Deal / Won
    df["deal_id"] = df.get("deal_id", "").fillna("").astype(str)
    df["has_deal"] = df["deal_id"].str.strip() != ""
    df["is_won"] = _to_bool(df.get("deal_is_won", pd.Series(dtype=str)))

    # Ankiety: liczby
    df["ankieta_avg_quality_num"] = pd.to_numeric(df.get("ankieta_avg_quality", pd.Series(dtype=str)), errors="coerce")

    # Time to meeting (od entry do pierwszego spotkania)
    df["time_to_meeting_days"] = (df["lead_first_meeting_time_dt"] - df["entry_time"]).dt.days
    df.loc[df["time_to_meeting_days"] < 0, "time_to_meeting_days"] = pd.NA  # sanity

    # Klucz miesięczny
    df["entry_month"] = _month_key(df["entry_time"])
    
    # Project type (PreSales vs Sales only)
    df["project_type"] = _norm_text(df.get("project_type", pd.Series(dtype=str)), fallback="brak danych")

    # Month + Source (z podziałem na project_type)
    month_source = (
        df.groupby(["project_type", "entry_month", "source_I_tier", "source_II_tier", "source_III_tier"], dropna=False)
        .apply(_compute_metrics)
        .reset_index()
        .sort_values(["project_type", "entry_month", "source_I_tier", "source_II_tier", "source_III_tier"])
    )
    month_source.to_csv(OUT_MONTH_SOURCE, index=False, encoding="utf-8-sig")

    # Month global (z podziałem na project_type)
    month_global = (
        df.groupby(["project_type", "entry_month"], dropna=False)
        .apply(_compute_metrics)
        .reset_index()
        .sort_values(["project_type", "entry_month"])
    )
    month_global.to_csv(OUT_MONTH_GLOBAL, index=False, encoding="utf-8-sig")

    # Source global (z podziałem na project_type)
    source_global = (
        df.groupby(["project_type", "source_I_tier", "source_II_tier", "source_III_tier"], dropna=False)
        .apply(_compute_metrics)
        .reset_index()
        .sort_values(["project_type", "source_I_tier", "source_II_tier", "source_III_tier"])
    )
    source_global.to_csv(OUT_SOURCE_GLOBAL, index=False, encoding="utf-8-sig")

    print("Zapisano:")
    print(f"- {OUT_MONTH_SOURCE}")
    print(f"- {OUT_MONTH_GLOBAL}")
    print(f"- {OUT_SOURCE_GLOBAL}")


if __name__ == "__main__":
    main()

