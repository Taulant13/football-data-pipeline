"""
Football match data transformation pipeline.

Reads raw match data from PostgreSQL and computes team-level statistics
for opponent analysis. Creates a team_stats table that the coaching staff
can query to prepare for upcoming matches.
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_db_engine():
    """Create a SQLAlchemy engine from environment variables."""
    user = os.getenv("POSTGRES_USER", "football")
    password = os.getenv("POSTGRES_PASSWORD", "football123")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "football_db")

    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def load_raw_matches(engine) -> pd.DataFrame:
    """Read all raw match data from PostgreSQL."""
    query = "SELECT * FROM raw_matches ORDER BY match_date"
    df = pd.read_sql(query, engine)
    logger.info(f"Loaded {len(df)} raw matches from database")
    return df


def build_team_matches(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape match data so each row represents one team's perspective in a match.
    A single match produces two rows: one for the home team, one for the away team.
    """
    home = df.copy()
    home["team"] = home["home_team"]
    home["opponent"] = home["away_team"]
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
    away["opponent"] = away["home_team"]
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
        "season", "division", "match_date", "team", "opponent", "venue",
        "goals_scored", "goals_conceded", "result",
        "shots", "shots_on_target", "corners", "fouls", "yellows", "reds",
    ]
    combined = pd.concat([home[cols], away[cols]], ignore_index=True)
    combined = combined.sort_values(["team", "match_date"]).reset_index(drop=True)

    logger.info(f"Built {len(combined)} team-match rows")
    return combined


def compute_team_stats(team_matches: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate team-match rows into team-level statistics per season and division.

    Computes:
    - Overall record (W/D/L), goals, goal difference
    - Averages: goals, shots, shot accuracy, corners, fouls, cards
    - Home/away record splits
    - Last 5 matches form
    - Clean sheets count
    """
    results = []

    for (season, division, team), group in team_matches.groupby(
        ["season", "division", "team"]
    ):
        g = group.sort_values("match_date")
        n = len(g)

        # Overall record
        wins = (g["result"] == "W").sum()
        draws = (g["result"] == "D").sum()
        losses = (g["result"] == "L").sum()
        goals_scored = g["goals_scored"].sum()
        goals_conceded = g["goals_conceded"].sum()
        clean_sheets = (g["goals_conceded"] == 0).sum()

        # Averages
        avg_goals_scored = g["goals_scored"].mean()
        avg_goals_conceded = g["goals_conceded"].mean()
        avg_shots = g["shots"].mean()
        avg_sot = g["shots_on_target"].mean()
        total_shots = g["shots"].sum()
        shot_accuracy = (g["shots_on_target"].sum() / total_shots * 100) if total_shots > 0 else 0
        avg_corners = g["corners"].mean()
        avg_fouls = g["fouls"].mean()
        avg_yellows = g["yellows"].mean()
        avg_reds = g["reds"].mean()

        # Home/away splits
        home_g = g[g["venue"] == "home"]
        away_g = g[g["venue"] == "away"]
        home_wins = (home_g["result"] == "W").sum()
        home_draws = (home_g["result"] == "D").sum()
        home_losses = (home_g["result"] == "L").sum()
        away_wins = (away_g["result"] == "W").sum()
        away_draws = (away_g["result"] == "D").sum()
        away_losses = (away_g["result"] == "L").sum()

        # Last 5 matches form
        last5 = g.tail(5)
        last5_wins = (last5["result"] == "W").sum()
        last5_draws = (last5["result"] == "D").sum()
        last5_losses = (last5["result"] == "L").sum()
        last5_goals_scored = last5["goals_scored"].sum()
        last5_goals_conceded = last5["goals_conceded"].sum()

        results.append({
            "season": season,
            "division": division,
            "team": team,
            "matches_played": n,
            "wins": int(wins),
            "draws": int(draws),
            "losses": int(losses),
            "goals_scored": int(goals_scored),
            "goals_conceded": int(goals_conceded),
            "goal_difference": int(goals_scored - goals_conceded),
            "clean_sheets": int(clean_sheets),
            "avg_goals_scored": round(avg_goals_scored, 2),
            "avg_goals_conceded": round(avg_goals_conceded, 2),
            "avg_shots": round(avg_shots, 2),
            "avg_shots_on_target": round(avg_sot, 2),
            "shot_accuracy": round(shot_accuracy, 2),
            "avg_corners": round(avg_corners, 2),
            "avg_fouls": round(avg_fouls, 2),
            "avg_yellows": round(avg_yellows, 2),
            "avg_reds": round(avg_reds, 2),
            "home_wins": int(home_wins),
            "home_draws": int(home_draws),
            "home_losses": int(home_losses),
            "away_wins": int(away_wins),
            "away_draws": int(away_draws),
            "away_losses": int(away_losses),
            "last5_wins": int(last5_wins),
            "last5_draws": int(last5_draws),
            "last5_losses": int(last5_losses),
            "last5_goals_scored": int(last5_goals_scored),
            "last5_goals_conceded": int(last5_goals_conceded),
        })

    stats_df = pd.DataFrame(results)
    logger.info(f"Computed stats for {len(stats_df)} teams")
    return stats_df


def save_team_stats(df: pd.DataFrame, engine):
    """
    Save team stats to PostgreSQL.
    Replaces all existing data (full refresh) to ensure consistency.
    """
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE team_stats"))
        df.to_sql("team_stats", conn, if_exists="append", index=False)
    logger.info(f"Saved {len(df)} team stat rows to database")


def transform():
    """Run the full transformation pipeline."""
    engine = get_db_engine()
    raw_matches = load_raw_matches(engine)

    if raw_matches.empty:
        logger.warning("No raw matches found — nothing to transform")
        return

    team_matches = build_team_matches(raw_matches)
    team_stats = compute_team_stats(team_matches)
    save_team_stats(team_stats, engine)
    logger.info("Transformation complete")


if __name__ == "__main__":
    transform()
