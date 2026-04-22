from __future__ import annotations

import argparse

from src.config import settings
from src.data_ingestion.fetch_data import fetch_weekend_data
from src.preprocessing.clean_data import clean_all
from src.preprocessing.transform_data import transform_all
from src.db.db import initialize_database, load_dataset_group, create_sql_views


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the F1 data pipeline.")
    parser.add_argument("--year", type=int, default=settings.default_year, help="Race year")
    parser.add_argument(
        "--country",
        type=str,
        default=settings.default_country,
        help="Grand Prix country name, e.g. Japan, Italy, Bahrain",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Starting pipeline for {args.country} {args.year}...")
    raw_data = fetch_weekend_data(year=args.year, country_name=args.country)

    print("Cleaning raw data...")
    cleaned = clean_all(raw_data)

    print("Transforming cleaned data...")
    transformed = transform_all(cleaned)

    print("Loading into database...")
    engine = initialize_database()
    load_dataset_group(cleaned, engine=engine, prefix="stg_")
    load_dataset_group(transformed, engine=engine, prefix="mart_")

    print("Creating SQL views...")
    create_sql_views(engine)

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
