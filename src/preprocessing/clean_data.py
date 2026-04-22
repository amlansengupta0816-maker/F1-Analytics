from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd


PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return df


def try_parse_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if "date" in col or "time" in col:
            try:
                df[col] = pd.to_datetime(df[col], errors="ignore")
            except Exception:
                pass
    return df


def clean_sessions(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = try_parse_datetimes(df)
    df = df.drop_duplicates()
    return df


def clean_drivers(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = df.drop_duplicates(subset=["session_key", "driver_number"], keep="last")
    return df


def clean_laps(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = df.drop_duplicates()

    numeric_cols = [
        "lap_number",
        "driver_number",
        "i1_speed",
        "i2_speed",
        "st_speed",
        "duration_sector_1",
        "duration_sector_2",
        "duration_sector_3",
        "lap_duration",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def clean_position(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = try_parse_datetimes(df)
    if "position" in df.columns:
        df["position"] = pd.to_numeric(df["position"], errors="coerce")
    return df.drop_duplicates()


def clean_stints(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = df.drop_duplicates()
    for col in ["driver_number", "lap_start", "lap_end", "stint_number"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_weather(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = try_parse_datetimes(df)
    return df.drop_duplicates()


def clean_race_control(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_columns(df)
    df = try_parse_datetimes(df)
    return df.drop_duplicates()


def clean_all(raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    cleaned = {
        "sessions": clean_sessions(raw_data["sessions"]),
        "drivers": clean_drivers(raw_data["drivers"]),
        "laps": clean_laps(raw_data["laps"]),
        "position": clean_position(raw_data["position"]),
        "stints": clean_stints(raw_data["stints"]),
        "weather": clean_weather(raw_data["weather"]),
        "race_control": clean_race_control(raw_data["race_control"]),
    }

    for name, df in cleaned.items():
        df.to_csv(PROCESSED_DIR / f"clean_{name}.csv", index=False)

    return cleaned
