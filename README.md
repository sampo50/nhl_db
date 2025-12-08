NHL Data Pipeline (2025–26 Season)

This project builds a full NHL data pipeline using the unofficial NHL API, the Python package nhl-api-py, and a PostgreSQL database.
It automatically retrieves:

NHL teams

The full season schedule

Boxscore data for every game

Player metadata

Player game-by-game statistics

All data is stored in a relational SQL schema suitable for analytics, modelling, dashboards, and machine learning.

Features
Feature	Status
Fetch NHL teams and map unique abbreviations	✓
Store all games for the 2025–26 season	✓
Fetch boxscore stats for every game	✓
Populate players and player_game_stats tables	✓
Incremental updates (planned)	In progress
Play-by-play + xG model	Future milestone
Tech Stack
Component	Choice
Language	Python 3.11+
Data Source	nhl-api-py (unofficial NHL API wrapper)
Database	PostgreSQL (recommended version ≥ 15)
GUI	DBeaver (optional)
Installation

Clone the project:

git clone https://github.com/YOURUSERNAME/nhl-2025-26-db.git
cd nhl-2025-26-db


Install dependencies:

pip install -r requirements.txt


(Create requirements.txt with:)

nhl-api-py
psycopg2-binary

Database Setup

Before running the script, load database migrations.

This project uses two SQL migration layers located in /migrations:

001_init.sql → Creates teams and games

002_players.sql → Creates players and player_game_stats

Run them in order:

-- in DBeaver or psql:
\i migrations/001_init.sql
\i migrations/002_players.sql


After applying migrations, refresh tables and confirm they exist.

Running the Loader

The main script is:

nhl_loader_2025_26.py


Execute it:

python nhl_loader_2025_26.py


What the process does:

Fetches and upserts all NHL teams

Iterates through every day in the 2025–26 schedule window

Stores all games with metadata

Fetches boxscore data and fills:

players

player_game_stats

Database Schema Overview
teams
 └─ team_id (PK)

games
 └─ FK → teams.home_team_id
 └─ FK → teams.away_team_id

players
 └─ player_id (PK)
 └─ FK → teams.current_team_id

player_game_stats
 └─ (game_id, player_id) PK
 └─ FK → games.game_id
 └─ FK → players.player_id
 └─ FK → teams.team_id


This schema supports powerful analytics such as:

Player season totals

Game logs

Shot/goal distributions

Powerplay vs. even-strength breakdowns

Predictive modelling

Example Queries
Top 10 Scorers
SELECT p.full_name, SUM(s.goals + s.assists) AS points
FROM player_game_stats s
JOIN players p ON s.player_id = p.player_id
JOIN games g ON s.game_id = g.game_id
WHERE g.season = '20252026'
GROUP BY p.full_name
ORDER BY points DESC
LIMIT 10;

Team Standings (Points)
SELECT t.abbreviation,
       SUM(CASE
             WHEN (home_score > away_score AND t.team_id = home_team_id)
               OR (away_score > home_score AND t.team_id = away_team_id)
             THEN 2 ELSE 0 END) AS points
FROM games
JOIN teams t ON t.team_id IN (home_team_id, away_team_id)
WHERE season = '20252026'
GROUP BY abbreviation
ORDER BY points DESC;

Roadmap
Milestone	Target
Add incremental update mode	Next
Automated cron runner	Q2
Full play-by-play ingestion	Q3
xG model and Tableau/PowerBI dashboards	Q4
Web API wrapper for queries	Optional
Contributing

Pull requests, bug reports, and new feature ideas are welcome.

If you add new fields to the pipeline, update:

migrations/*.sql

nhl_loader2.py

README

License

MIT License — free to use and modify.
