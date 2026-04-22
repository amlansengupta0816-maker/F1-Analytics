from __future__ import annotations

from typing import Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config import settings


def get_engine() -> Engine:
    return create_engine(settings.db_url, future=True)


def write_table(
    df: pd.DataFrame,
    table_name: str,
    engine: Engine,
    if_exists: str = "replace",
) -> None:
    if df.empty:
        print(f"Skipping empty table: {table_name}")
        return

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=1000,
    )
    print(f"Loaded table: {table_name} ({len(df)} rows)")


def load_dataset_group(
    data: Dict[str, pd.DataFrame],
    engine: Engine,
    prefix: str = "",
    if_exists: str = "replace",
) -> None:
    for name, df in data.items():
        table_name = f"{prefix}{name}" if prefix else name
        write_table(df, table_name, engine, if_exists=if_exists)


def create_sql_views(engine: Engine) -> None:
    # SQLite-friendly simple view creation
    view_sql = """
    CREATE VIEW IF NOT EXISTS vw_driver_performance AS
    SELECT
        session_key,
        driver_number,
        full_name,
        team_name,
        total_laps,
        avg_lap_duration,
        best_lap_duration,
        lap_consistency_gap,
        final_position,
        is_podium
    FROM mart_powerbi_fact_race_performance
    """
    with engine.begin() as conn:
        conn.execute(text("DROP VIEW IF EXISTS vw_driver_performance"))
        conn.execute(text(view_sql))


def initialize_database() -> Engine:
    engine = get_engine()
    return engine
