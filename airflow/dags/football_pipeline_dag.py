"""
Airflow DAG for the football match data pipeline.

Orchestrates three steps:
1. Download match data from Football-Data.co.uk
2. Load data into PostgreSQL
3. Transform raw data into team statistics

Scheduled weekly (every Monday) to capture weekend match results.
Supports backfills for historical seasons.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from io import StringIO

logger = logging.getLogger(__name__)

# Config
BASE_URL = "https://www.football-data.co.uk/mmz4281/{season}/{league}.csv"

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

LEAGUES = os.getenv("LEAGUES", "E0,D1,SP1,I1,F1").split(",")
SEASONS = os.getenv("SEASONS", "2324,2425").split(",")


def get_db_conn():
    """Create a psycopg2 connection from environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "football_db"),
        user=os.getenv("POSTGRES_USER", "football"),
        password=os.getenv("POSTGRES_PASSWORD", "football123"),
    )


# Task 1: Download
def download_data(**context):
    """Download CSVs from source and push to XCom for the next task."""
    all_data = []

    for season in SEASONS:
        for league in LEAGUES:
            url = BASE_URL.format(season=season, league=league)
            logger.info(f"Downloading {url}")
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                csv_text = response.text.replace("\ufeff", "")
                df = pd.read_csv(StringIO(csv_text))
                df.columns = df.columns.str.strip()
                # Override Div from the league parameter
                df["Div"] = league
                df["_season"] = season
                all_data.append(df)
                logger.info(f"Downloaded {len(df)} rows for {league} {season}")
            except Exception as e:
                logger.error(f"Failed to download {league}/{season}: {e}")

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        context["ti"].xcom_push(key="raw_data", value=combined.to_json())
        logger.info(f"Total downloaded: {len(combined)} rows")
    else:
        raise ValueError("No data downloaded from any source")


# Task 2: Load into PostgreSQL
def load_data(**context):
    """Clean raw data and load into PostgreSQL using psycopg2."""
    raw_json = context["ti"].xcom_pull(key="raw_data", task_ids="download")
    df = pd.read_json(raw_json)

    # Select and rename columns
    available_cols = [c for c in COLUMNS_MAPPING if c in df.columns]
    rename_map = {k: v for k, v in COLUMNS_MAPPING.items() if k in available_cols}
    clean = df[available_cols + ["_season"]].copy()
    clean = clean.rename(columns={**rename_map, "_season": "season"})

    # Ensure season and division are strings
    clean["season"] = clean["season"].astype(str)
    clean["division"] = clean["division"].astype(str)

    # Parse dates JSON round-trip stores as ms timestamps
    clean["match_date"] = pd.to_datetime(clean["match_date"], unit="ms", errors="coerce")

    # Drop rows missing essential fields
    clean = clean.dropna(subset=["match_date", "home_team", "away_team", "ft_home_goals", "ft_away_goals"])

    # Cast numeric columns
    int_cols = [
        "ft_home_goals", "ft_away_goals", "ht_home_goals", "ht_away_goals",
        "home_shots", "away_shots", "home_shots_on_target", "away_shots_on_target",
        "home_fouls", "away_fouls", "home_corners", "away_corners",
        "home_yellows", "away_yellows", "home_reds", "away_reds",
    ]
    for col in int_cols:
        if col in clean.columns:
            clean[col] = pd.to_numeric(clean[col], errors="coerce")

    # Target columns for INSERT (must match raw_matches table, excluding id)
    insert_cols = [
        "division", "season", "match_date", "match_time", "home_team", "away_team",
        "ft_home_goals", "ft_away_goals", "ft_result",
        "ht_home_goals", "ht_away_goals", "ht_result", "referee",
        "home_shots", "away_shots", "home_shots_on_target", "away_shots_on_target",
        "home_fouls", "away_fouls", "home_corners", "away_corners",
        "home_yellows", "away_yellows", "home_reds", "away_reds",
    ]

    conn = get_db_conn()
    cur = conn.cursor()

    try:
        # clear and reload per division, season
        for (div, season), group in clean.groupby(["division", "season"]):
            cur.execute(
                "DELETE FROM raw_matches WHERE division = %s AND season = %s",
                (str(div), str(season)),
            )

            # Prepare rows for insert
            rows = []
            for _, row in group.iterrows():
                values = []
                for col in insert_cols:
                    val = row.get(col)
                    if pd.isna(val):
                        values.append(None)
                    elif col == "match_date":
                        values.append(val.strftime("%Y-%m-%d"))
                    elif col in int_cols:
                        values.append(int(val))
                    else:
                        values.append(str(val))
                rows.append(tuple(values))

            if rows:
                placeholders = ",".join(["%s"] * len(insert_cols))
                cols_str = ",".join(insert_cols)
                query = f"INSERT INTO raw_matches ({cols_str}) VALUES ({placeholders})"
                psycopg2.extras.execute_batch(cur, query, rows)

            logger.info(f"Loaded {len(rows)} rows for {div} {season}")

        conn.commit()
        logger.info(f"Total loaded: {len(clean)} rows")
    finally:
        cur.close()
        conn.close()


# Task 3: Transform
def transform_data(**context):
    """Transform raw matches into team statistics."""
    conn = get_db_conn()

    try:
        df = pd.read_sql("SELECT * FROM raw_matches ORDER BY match_date", conn)
    finally:
        conn.close()

    if df.empty:
        raise ValueError("No raw matches found in database")

    # home team perspective + away team perspective
    home = df.copy()
    home["team"] = home["home_team"]
    home["venue"] = "home"
    home["goals_scored"] = home["ft_home_goals"]
    home["goals_conceded"] = home["ft_away_goals"]
    home["shots"] = home["home_shots"]
    home["shots_on_target"] = home["home_shots_on_target"]
    home["corners"] = home["home_corners"]
    home["fouls"] = home["home_fouls"]
    home["yellows"] = home["home_yellows"]
    home["reds"] = home["home_reds"]
    home["result"] = home["ft_result"].map({"H": "W", "D": "D", "A": "L"})

    away = df.copy()
    away["team"] = away["away_team"]
    away["venue"] = "away"
    away["goals_scored"] = away["ft_away_goals"]
    away["goals_conceded"] = away["ft_home_goals"]
    away["shots"] = away["away_shots"]
    away["shots_on_target"] = away["away_shots_on_target"]
    away["corners"] = away["away_corners"]
    away["fouls"] = away["away_fouls"]
    away["yellows"] = away["away_yellows"]
    away["reds"] = away["away_reds"]
    away["result"] = away["ft_result"].map({"H": "L", "D": "D", "A": "W"})

    cols = [
        "season", "division", "match_date", "team", "venue",
        "goals_scored", "goals_conceded", "result",
        "shots", "shots_on_target", "corners", "fouls", "yellows", "reds",
    ]
    team_matches = pd.concat([home[cols], away[cols]], ignore_index=True)
    team_matches = team_matches.sort_values(["team", "match_date"])

    # Aggregate per team/season/division
    results = []
    for (season, division, team), g in team_matches.groupby(["season", "division", "team"]):
        g = g.sort_values("match_date")
        total_shots = g["shots"].sum()
        home_g = g[g["venue"] == "home"]
        away_g = g[g["venue"] == "away"]
        last5 = g.tail(5)

        results.append((
            str(season), str(division), str(team), len(g),
            int((g["result"] == "W").sum()),
            int((g["result"] == "D").sum()),
            int((g["result"] == "L").sum()),
            int(g["goals_scored"].sum()),
            int(g["goals_conceded"].sum()),
            int(g["goals_scored"].sum() - g["goals_conceded"].sum()),
            int((g["goals_conceded"] == 0).sum()),
            round(float(g["goals_scored"].mean()), 2),
            round(float(g["goals_conceded"].mean()), 2),
            round(float(g["shots"].mean()), 2),
            round(float(g["shots_on_target"].mean()), 2),
            round(float(g["shots_on_target"].sum() / total_shots * 100), 2) if total_shots > 0 else 0,
            round(float(g["corners"].mean()), 2),
            round(float(g["fouls"].mean()), 2),
            round(float(g["yellows"].mean()), 2),
            round(float(g["reds"].mean()), 2),
            int((home_g["result"] == "W").sum()),
            int((home_g["result"] == "D").sum()),
            int((home_g["result"] == "L").sum()),
            int((away_g["result"] == "W").sum()),
            int((away_g["result"] == "D").sum()),
            int((away_g["result"] == "L").sum()),
            int((last5["result"] == "W").sum()),
            int((last5["result"] == "D").sum()),
            int((last5["result"] == "L").sum()),
            int(last5["goals_scored"].sum()),
            int(last5["goals_conceded"].sum()),
        ))

    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE team_stats")
        insert_query = """
            INSERT INTO team_stats (
                season, division, team, matches_played,
                wins, draws, losses,
                goals_scored, goals_conceded, goal_difference, clean_sheets,
                avg_goals_scored, avg_goals_conceded,
                avg_shots, avg_shots_on_target, shot_accuracy,
                avg_corners, avg_fouls, avg_yellows, avg_reds,
                home_wins, home_draws, home_losses,
                away_wins, away_draws, away_losses,
                last5_wins, last5_draws, last5_losses,
                last5_goals_scored, last5_goals_conceded
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """
        psycopg2.extras.execute_batch(cur, insert_query, results)
        conn.commit()
        logger.info(f"Saved {len(results)} team stat rows")
    finally:
        cur.close()
        conn.close()


# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="football_pipeline",
    default_args=default_args,
    description="Ingest football match data and compute team statistics",
    schedule_interval="@weekly",
    start_date=datetime(2024, 8, 1),
    catchup=False,
    tags=["football", "pipeline"],
) as dag:

    download = PythonOperator(
        task_id="download",
        python_callable=download_data,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
    )

    download >> load >> transform
