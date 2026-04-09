# Football Match Analysis Pipeline

An end-to-end batch data pipeline that ingests football match data, transforms it into team-level statistics, and serves it for pre-match opponent analysis.

## Use Case

**Persona:** Analytics department of a football club.

**Problem:** Before each match, the coaching staff needs structured data on their upcoming opponent recent form, attacking/defensive strength, set-piece tendencies, and discipline record. Manually gathering this data is slow and error-prone.

**Solution:** This pipeline automates the process by ingesting match data from [Football-Data.co.uk](https://www.football-data.co.uk), loading it into PostgreSQL, and transforming it into queryable team profiles.

## Architecture

```
Football-Data.co.uk (CSV)
        │
        ▼
  [Download CSV]       ← Airflow Task 1
        │
        ▼
  [Clean & Load]       ← Airflow Task 2
        │
        ▼
   PostgreSQL
  (raw_matches)
        │
        ▼
  [Transform]          ← Airflow Task 3
        │
        ▼
   PostgreSQL
  (team_stats)
        │
        ▼
   pgAdmin (query UI)
```

## Data Source

- **Source:** Football-Data.co.uk
- **Format:** CSV files, one per league per season
- **Leagues:** Premier League (E0), Bundesliga (D1), La Liga (SP1), Serie A (I1), Ligue 1 (F1)
- **Seasons:** 2023/24, 2024/25 (configurable)
- **Key fields:** Match date, teams, goals (FT/HT), shots, shots on target, corners, fouls, yellow/red cards

## Prerequisites

- Docker and Docker Compose installed
- Ports `5432`, `8080`, `8081` available

## Quick Start

### 1. Clone the repository

First clone the repository and go to the directory of the project.

```bash
cd project
```

### 2. Set up environment variables

```bash
cp .env.example .env
```

Edit `.env` if you want to change default credentials.

### 3. Build and start all services

```bash
docker compose up --build -d
```

This starts:
- **PostgreSQL** on port `5432`
- **pgAdmin** on port `8080`
- **Airflow Webserver** on port `8081`
- **Airflow Scheduler** (background)

Wait for all services to initialize.

### 4. Trigger the pipeline via Airflow

1. Open Airflow at [http://localhost:8081](http://localhost:8081)
2. Login with username `admin`, password `admin`
3. Find the `football_pipeline` DAG
4. Toggle the DAG **ON**
5. Click the **Play** button -> "Trigger DAG"
6. Wait for all 3 tasks to complete (they turn green)

### 5. Verify the data in pgAdmin

1. Open pgAdmin at [http://localhost:8080](http://localhost:8080)
2. Login with email `admin@admin.com`, password `admin123`
3. Connect to the "Football DB" server (password: `football123`)
4. Navigate to: **Football DB → Databases → football_db → Schemas → public → Tables**
5. Right-click `raw_matches` → **View/Edit Data → All Rows** to see ingested match data
6. Right-click `team_stats` → **View/Edit Data → All Rows** to see transformed team profiles

### Example query: Opponent analysis

```sql
-- Get Bayern Munich's season profile
SELECT team, matches_played, wins, draws, losses,
       goals_scored, goals_conceded, goal_difference,
       avg_shots, shot_accuracy, avg_corners,
       last5_wins, last5_draws, last5_losses
FROM team_stats
WHERE team = 'Bayern Munich' AND season = '2425';
```

## Stopping the services

```bash
docker compose down
```

To remove all data and start fresh:

```bash
docker compose down -v
```

## Project Structure

```
project/
├── README.md                       # This file
├── docker-compose.yml              # All service definitions
├── .env.example                    # Environment variable template
├── .gitignore
├── sql/
│   └── init.sql                    # PostgreSQL table schemas
├── pipeline/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── ingest.py                   # Ingestion script (standalone)
│   └── transform.py                # Transformation script (standalone)
├── airflow/
│   ├── Dockerfile                  # Custom Airflow image with dependencies
│   ├── requirements.txt
│   ├── dags/
│   │   └── football_pipeline_dag.py  # Airflow DAG definition
│   └── logs/                       # Airflow logs (gitignored)
└── pgadmin/
    └── servers.json                # Auto-registers PostgreSQL in pgAdmin
```

## Configuration

All configuration is done via the `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| `LEAGUES` | Comma-separated league codes | `E0,D1,SP1,I1,F1` |
| `SEASONS` | Comma-separated season codes | `2425,2526` |
| `POSTGRES_USER` | Database username | `football` |
| `POSTGRES_PASSWORD` | Database password | `football123` |
| `POSTGRES_DB` | Database name | `football_db` |

## Transformation Details

The transformation step converts raw match data into team-level statistics:

| Metric | Description | Why it matters |
|--------|-------------|---------------|
| Record (W/D/L) | Overall season results | Overall team strength |
| Goal difference | Goals scored minus conceded | Net performance indicator |
| Shot accuracy | Shots on target / total shots (%) | Attacking efficiency |
| Avg corners | Average corners per game | Set piece tendency |
| Avg fouls/cards | Average fouls, yellows, reds | Discipline and aggression |
| Home/Away splits | Record broken down by venue | Venue-dependent performance |
| Last 5 form | Results from last 5 matches | Current momentum |

## Orchestration

- **Tool:** Apache Airflow
- **Schedule:** Weekly (`@weekly`) captures weekend match results
- **Backfill:** Supported via Airflow's built-in backfill mechanism
- **Manual trigger:** Available through the Airflow web UI
- **DAG structure:** `download → load → transform` (3 sequential tasks)
