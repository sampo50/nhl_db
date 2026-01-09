# NHL Data Warehouse – PostgreSQL + Python Scraping Pipeline

End-to-end NHL analytics data warehouse built by scraping official NHL data with Python and modeling it into a star-schema warehouse in PostgreSQL.

---

## Project Overview

This project builds a complete NHL analytics warehouse using:

- Python (nhlpy + psycopg2) for data ingestion
- PostgreSQL as the data warehouse
- DBeaver for SQL development
- Star schema dimensional modeling
- Fact tables at multiple grains (game, team, player, event)

The warehouse supports advanced analytics such as:

- Player performance analysis  
- Goalie vs skater comparisons  
- Team performance by game and season  
- Event-level spatial analysis  

---

## Architecture

### Pipeline

NHL API → Python Scrapers → PostgreSQL (nhl_dw schema) → Analytics / BI / SQL

yaml
Kopioi koodi

### Core Design Principles

- Surrogate keys for all dimensions  
- Natural keys preserved as business identifiers  
- Fact tables modeled at correct analytical grain  
- Referential integrity enforced with foreign keys  
- Event-level data stored in raw JSON for extensibility  

---

## Data Model

### Dimensions

| Table | Description |
|------|------------|
| dim_date | Calendar dimension |
| dim_player | Player master data |
| dim_team | NHL teams |
| dim_season | NHL seasons |
| dim_venue | Game venues |

---

### Fact Tables

| Table | Grain |
|------|------|
| fact_game | One row per NHL game |
| fact_skater_game | One row per skater per game |
| fact_goalie_game | One row per goalie per game |
| fact_team_game | One row per team per game |
| event_play | One row per play event |

---

### Relationship Overview

dim_player 1 ────< fact_skater_game
dim_player 1 ────< fact_goalie_game

dim_team 1 ────< fact_team_game
dim_team 1 ────< fact_skater_game
dim_team 1 ────< fact_goalie_game

dim_season 1 ────< fact_game
dim_date 1 ────< fact_game
dim_venue 1 ────< fact_game

fact_game 1 ────< event_play


This structure enables multi-grain analytics while maintaining star-schema clarity.

---

## Python Scraping Layer

The Python pipeline uses:

- `nhlpy.NHLClient`
- PostgreSQL UPSERT logic
- Data normalization across multiple endpoints
- Roster + stats fusion to ensure player completeness

### Key features

- Combines roster and stats endpoints to avoid missing players
- Normalizes inconsistent NHL API structures
- Uses `ON CONFLICT` UPSERT logic to preserve and enrich records
- Maintains player dimension as a slowly evolving entity

### Example logic

- Roster API → best bio fields  
- Stats API → guarantees statistical coverage  
- Both feed the same `dim_player` table using conflict resolution  

---

## Data Quality Strategy

- Natural keys enforced as UNIQUE constraints  
- Surrogate keys used for warehouse joins  
- Foreign keys enforce referential integrity  
- JSON stored for event extensibility  
- Deduplication handled during ingestion  

---

## Use Cases

This warehouse supports:

- Player career analysis  
- Team performance tracking  
- Goalie efficiency analysis  
- Event heatmaps  
- Advanced derived metrics (xG, clutch, possession, etc.)  
- BI dashboards and Python analytics  

---

## Repository Structure

/sql
/ddl
/dimensions
/facts

/python
nhl_populate_dim_player.py
...

/docs
erd.png
architecture.png

---

## Limitations

- No orchestration layer yet  
- No incremental change detection for facts  
- No automated data quality framework
- No spatial data ingested
- No seasons before 2025-26

These are intentionally left as future extensions.

---

## Planned Extensions

- Incremental load logic  
- Season-over-season history tracking  
- Advanced derived metrics  
- BI dashboards  
- ML-ready feature tables  

---

## Author

Sami Olavuo  
Sports Analytics  

---

## License

Shared for educational and portfolio purposes.
