"""
Football match data ingestion pipeline.

Downloads match data from Football-Data.co.uk and loads it into PostgreSQL.
Parameterized by league and season for flexibility and backfill support.
"""

import os
import logging
import pandas as pd
import requests
from io import StringIO
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Source URL pattern
BASE_URL = "https://www.football-data.co.uk/mmz4281/{season}/{league}.csv"

# Columns we care about (core match data, no betting odds)
COLUMNS_MAPPING = {
    "Div": "division",
    "Date": "match_date",
    "Time": "match_time",
    "HomeTeam": "home_team",
    "AwayTeam": "away_team",
    "FTHG": "ft_home_goals",
    "FTAG": "ft_away_goals",
    "FTR": "ft_result",
    "HTHG": "ht_home_goals",
    "HTAG": "ht_away_goals",
    "HTR": "ht_result",
    "Referee": "referee",
    "HS": "home_shots",
    "AS": "away_shots",
    "HST": "home_shots_on_target",
    "AST": "away_shots_on_target",
    "HF": "home_fouls",
    "AF": "away_fouls",
    "HC": "home_corners",
    "AC": "away_corners",
    "HY": "home_yellows",
    "AY": "away_yellows",
    "HR": "home_reds",
    "AR": "away_reds",
}


def get_db_engine():
    """Create a SQLAlchemy engine from environment variables."""
    user = os.getenv("POSTGRES_USER", "football")
    password = os.getenv("POSTGRES_PASSWORD", "football123")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "football_db")

    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def download_data(league: str, season: str) -> pd.DataFrame:
    """
    Download match data CSV for a given league and season.

    Args:
        league: League code ('E0' for Premier League)
        season: Season code ('2425' for 2024/25)

    Returns:
        Raw DataFrame from the CSV source.
    """
    url = BASE_URL.format(season=season, league=league)
    logger.info(f"Downloading data from {url}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    csv_text = response.text.replace("\ufeff", "")
    df = pd.read_csv(StringIO(csv_text))
    df.columns = df.columns.str.strip()
    # Override Div from the league parameter
    df["Div"] = league
    logger.info(f"Downloaded {len(df)} rows for {league} season {season}")
    return df


def clean_data(df: pd.DataFrame, season: str) -> pd.DataFrame:
    """
    Clean and standardize raw match data.

    - Selects only relevant columns (drops betting odds)
    - Renames columns to snake_case
    - Parses dates
    - Adds season column
    - Drops rows with missing core data

    Args:
        df: Raw DataFrame from CSV download.
        season: Season code to tag the data.

    Returns:
        Cleaned DataFrame ready for loading.
    """
    available_cols = [col for col in COLUMNS_MAPPING.keys() if col in df.columns]
    df = df[available_cols].copy()
    rename_map = {k: v for k, v in COLUMNS_MAPPING.items() if k in available_cols}
    df = df.rename(columns=rename_map)

    # Parse dates as format varies: DD/MM/YYYY or DD/MM/YY
    df["match_date"] = pd.to_datetime(df["match_date"], dayfirst=True, format="mixed")

    # Add season column
    df["season"] = season

    # Drop rows where essential data is missing
    essential = ["match_date", "home_team", "away_team", "ft_home_goals", "ft_away_goals"]
    df = df.dropna(subset=essential)

    # Cast goal/stat columns to int where possible
    int_columns = [
        "ft_home_goals", "ft_away_goals", "ht_home_goals", "ht_away_goals",
        "home_shots", "away_shots", "home_shots_on_target", "away_shots_on_target",
        "home_fouls", "away_fouls", "home_corners", "away_corners",
        "home_yellows", "away_yellows", "home_reds", "away_reds",
    ]
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    logger.info(f"Cleaned data: {len(df)} rows, {len(df.columns)} columns")
    return df


def load_data(df: pd.DataFrame, engine) -> int:
    """
    Load cleaned match data into PostgreSQL.

    Uses an upsert pattern: deletes existing data for the same
    division+season before inserting, to ensure idempotency.

    Args:
        df: Cleaned DataFrame to load.
        engine: SQLAlchemy engine.

    Returns:
        Number of rows loaded.
    """
    if df.empty:
        logger.warning("No data to load — DataFrame is empty")
        return 0

    division = df["division"].iloc[0]
    season = df["season"].iloc[0]

    with engine.begin() as conn:
        # Delete existing data for this league+season (idempotent reload)
        conn.execute(
            text("DELETE FROM raw_matches WHERE division = :div AND season = :season"),
            {"div": division, "season": season},
        )
        logger.info(f"Cleared existing data for {division} {season}")

    # Load new data
    with engine.begin() as conn:
        df.to_sql("raw_matches", conn, if_exists="append", index=False)
    logger.info(f"Loaded {len(df)} rows for {division} season {season}")
    return len(df)


def ingest(leagues: list[str], seasons: list[str]):
    """
    Run the full ingestion pipeline for given leagues and seasons.

    Args:
        leagues: List of league codes (e.g., ['E0', 'D1'])
        seasons: List of season codes (e.g., ['2324', '2425'])
    """
    engine = get_db_engine()
    total_rows = 0

    for season in seasons:
        for league in leagues:
            try:
                raw_df = download_data(league, season)
                clean_df = clean_data(raw_df, season)
                rows = load_data(clean_df, engine)
                total_rows += rows
            except requests.exceptions.HTTPError as e:
                logger.error(f"Failed to download {league}/{season}: {e}")
            except Exception as e:
                logger.error(f"Error processing {league}/{season}: {e}")

    logger.info(f"Ingestion complete. Total rows loaded: {total_rows}")


if __name__ == "__main__":
    leagues = os.getenv("LEAGUES", "E0,D1,SP1,I1,F1").split(",")
    seasons = os.getenv("SEASONS", "2324,2425").split(",")
    logger.info(f"Starting ingestion for leagues={leagues}, seasons={seasons}")
    ingest(leagues, seasons)
