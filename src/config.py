from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    db_url: str = os.getenv("DB_URL", "sqlite:///f1_pipeline.db")
    openf1_base_url: str = os.getenv("OPENF1_BASE_URL", "https://api.openf1.org/v1")
    default_year: int = int(os.getenv("DEFAULT_YEAR", "2025"))
    default_country: str = os.getenv("DEFAULT_COUNTRY", "Japan")


settings = Settings()
