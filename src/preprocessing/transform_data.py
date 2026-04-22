from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd


PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def build_driver_lap_summary(laps: pd.DataFrame, drivers: pd.DataFrame) -> pd.DataFrame:
    if laps.empty:
        return pd.DataFrame()

    group_cols = ["session_key", "driver_number"]
    summary = (
        laps.groupby(group_cols, dropna=False)
        .agg(
            total_laps=("lap_number", "max"),
            avg_lap_duration=("lap_duration", "mean"),
            best_lap_duration=("lap_duration", "min"),
            avg_sector_1=("duration_sector_1", "mean"),
            avg_sector_2=("duration_sector_2", "mean"),
            avg_sector_3=("duration_sector_3", "mean"),
        )
        .reset_index()
    )

    if not drivers.empty:
        driver_cols = [c for c in ["session_key", "driver_number", "full_name", "team_name", "name_acronym"] if c in drivers.columns]
        summary = summary.merge(
            drivers[driver_cols].drop_duplicates(),
            on=["session_key", "driver_number"],
            how="left",
        )

    if "best_lap_duration" in summary.columns and "avg_lap_duration" in summary.columns:
        summary["lap_consistency_gap"] = summary["avg_lap_duration"] - summary["best_lap_duration"]

    return summary


def build_final_positions(position: pd.DataFrame, drivers: pd.DataFrame) -> pd.DataFrame:
    if position.empty or "date" not in position.columns:
        return pd.DataFrame()

    pos = position.copy().sort_values(["session_key", "driver_number", "date"])
    final_pos = pos.groupby(["session_key", "driver_number"], dropna=False).tail(1)

    keep_cols = [c for c in ["session_key", "driver_number", "position", "date"] if c in final_pos.columns]
    final_pos = final_pos[keep_cols].rename(columns={"position": "final_position", "date": "position_timestamp"})

    if not drivers.empty:
        driver_cols = [c for c in ["session_key", "driver_number", "full_name", "team_name", "name_acronym"] if c in drivers.columns]
        final_pos = final_pos.merge(
            drivers[driver_cols].drop_duplicates(),
            on=["session_key", "driver_number"],
            how="left",
        )

    return final_pos


def build_stint_summary(stints: pd.DataFrame, drivers: pd.DataFrame) -> pd.DataFrame:
    if stints.empty:
        return pd.DataFrame()

    st = stints.copy()
    if {"lap_start", "lap_end"}.issubset(st.columns):
        st["stint_length"] = st["lap_end"] - st["lap_start"] + 1

    if not drivers.empty:
        driver_cols = [c for c in ["session_key", "driver_number", "full_name", "team_name", "name_acronym"] if c in drivers.columns]
        st = st.merge(
            drivers[driver_cols].drop_duplicates(),
            on=["session_key", "driver_number"],
            how="left",
        )

    return st


def build_weather_summary(weather: pd.DataFrame) -> pd.DataFrame:
    if weather.empty:
        return pd.DataFrame()

    agg_map = {}
    for col in ["air_temperature", "track_temperature", "humidity", "rainfall", "wind_speed"]:
        if col in weather.columns:
            agg_map[col] = "mean"

    if not agg_map:
        return weather.copy()

    return weather.groupby("session_key", dropna=False).agg(agg_map).reset_index()


def build_powerbi_fact_table(
    driver_lap_summary: pd.DataFrame,
    final_positions: pd.DataFrame,
) -> pd.DataFrame:
    if driver_lap_summary.empty:
        return pd.DataFrame()

    fact = driver_lap_summary.copy()

    if not final_positions.empty:
        join_cols = [c for c in ["session_key", "driver_number", "final_position"] if c in final_positions.columns]
        fact = fact.merge(
            final_positions[join_cols],
            on=["session_key", "driver_number"],
            how="left",
        )

    if {"final_position", "best_lap_duration"}.issubset(fact.columns):
        fact["is_podium"] = fact["final_position"].apply(
            lambda x: 1 if pd.notna(x) and x <= 3 else 0
        )

    return fact


def transform_all(cleaned: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    driver_lap_summary = build_driver_lap_summary(cleaned["laps"], cleaned["drivers"])
    final_positions = build_final_positions(cleaned["position"], cleaned["drivers"])
    stint_summary = build_stint_summary(cleaned["stints"], cleaned["drivers"])
    weather_summary = build_weather_summary(cleaned["weather"])
    powerbi_fact = build_powerbi_fact_table(driver_lap_summary, final_positions)

    transformed = {
        "driver_lap_summary": driver_lap_summary,
        "final_positions": final_positions,
        "stint_summary": stint_summary,
        "weather_summary": weather_summary,
        "powerbi_fact_race_performance": powerbi_fact,
    }

    for name, df in transformed.items():
        df.to_csv(PROCESSED_DIR / f"{name}.csv", index=False)

    return transformed
