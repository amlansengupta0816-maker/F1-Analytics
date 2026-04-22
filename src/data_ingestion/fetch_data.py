from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
import requests

from src.config import settings


RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)


class OpenF1Client:
    def __init__(self, base_url: str | None = None, timeout: int = 30) -> None:
        self.base_url = (base_url or settings.openf1_base_url).rstrip("/")
        self.timeout = timeout

    def _get(self, endpoint: str, params: dict | None = None) -> list[dict]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_sessions(self, year: int, country_name: str | None = None) -> pd.DataFrame:
        params = {"year": year}
        if country_name:
            params["country_name"] = country_name
        data = self._get("sessions", params=params)
        return pd.DataFrame(data)

    def get_drivers(self, session_key: int) -> pd.DataFrame:
        data = self._get("drivers", params={"session_key": session_key})
        return pd.DataFrame(data)

    def get_laps(self, session_key: int) -> pd.DataFrame:
        data = self._get("laps", params={"session_key": session_key})
        return pd.DataFrame(data)

    def get_position(self, session_key: int) -> pd.DataFrame:
        data = self._get("position", params={"session_key": session_key})
        return pd.DataFrame(data)

    def get_stints(self, session_key: int) -> pd.DataFrame:
        data = self._get("stints", params={"session_key": session_key})
        return pd.DataFrame(data)

    def get_weather(self, session_key: int) -> pd.DataFrame:
        data = self._get("weather", params={"session_key": session_key})
        return pd.DataFrame(data)

    def get_race_control(self, session_key: int) -> pd.DataFrame:
        data = self._get("race_control", params={"session_key": session_key})
        return pd.DataFrame(data)


def save_df(df: pd.DataFrame, filename: str) -> Path:
    path = RAW_DIR / filename
    df.to_csv(path, index=False)
    return path


def fetch_weekend_data(year: int, country_name: str) -> Dict[str, pd.DataFrame]:
    client = OpenF1Client()

    sessions = client.get_sessions(year=year, country_name=country_name)
    if sessions.empty:
        raise ValueError(f"No sessions found for year={year}, country_name={country_name}")

    # Prefer the race session if present, otherwise take the latest session
    race_sessions = sessions[sessions["session_name"].astype(str).str.lower() == "race"]
    chosen = race_sessions.iloc[0] if not race_sessions.empty else sessions.sort_values(
        by="date_start", ascending=False
    ).iloc[0]

    session_key = int(chosen["session_key"])

    drivers = client.get_drivers(session_key=session_key)
    laps = client.get_laps(session_key=session_key)
    position = client.get_position(session_key=session_key)
    stints = client.get_stints(session_key=session_key)
    weather = client.get_weather(session_key=session_key)
    race_control = client.get_race_control(session_key=session_key)

    data = {
        "sessions": sessions,
        "drivers": drivers,
        "laps": laps,
        "position": position,
        "stints": stints,
        "weather": weather,
        "race_control": race_control,
    }

    save_df(sessions, f"{year}_{country_name.lower()}_sessions.csv")
    save_df(drivers, f"{year}_{country_name.lower()}_drivers.csv")
    save_df(laps, f"{year}_{country_name.lower()}_laps.csv")
    save_df(position, f"{year}_{country_name.lower()}_position.csv")
    save_df(stints, f"{year}_{country_name.lower()}_stints.csv")
    save_df(weather, f"{year}_{country_name.lower()}_weather.csv")
    save_df(race_control, f"{year}_{country_name.lower()}_race_control.csv")

    return data


if __name__ == "__main__":
    result = fetch_weekend_data(
        year=settings.default_year,
        country_name=settings.default_country,
    )
    for name, df in result.items():
        print(f"{name}: {df.shape}")
