#!/usr/bin/env python3
"""
nhl_populate_dim_player.py

Populate nhl_dw.dim_player using:
  1) team rosters (team_roster)
  2) season stats (skater_stats_summary + goalie_stats_summary)

Expected dim_player schema (minimum):

  CREATE TABLE nhl_dw.dim_player (
      player_key      BIGSERIAL PRIMARY KEY,
      player_id       INTEGER UNIQUE,       -- natural NHL player id
      full_name       TEXT,
      first_name      TEXT,
      last_name       TEXT,
      birth_date      DATE,
      shoots_catches  TEXT
  );
"""

from typing import Dict, Any, Iterable, Set, Tuple

import psycopg2
from nhlpy import NHLClient

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "nhl_db"
DB_USER = "nhl_user"
DB_PASSWORD = "strongpassword"  # change to your own

# Choose which season's players you want to cover (YYYYYYYY format)
# Used for both rosters and stats.
SEASON_ID = "20252026"  # e.g. 2025–26 season

# Stats season range (can be widened later if you want multiple seasons)
STATS_START_SEASON = SEASON_ID
STATS_END_SEASON = SEASON_ID


# ---------------------------------------------------------------------------
# DB CONNECTION
# ---------------------------------------------------------------------------

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


# ---------------------------------------------------------------------------
# NHL CLIENT
# ---------------------------------------------------------------------------

client = NHLClient(
    debug=True,
    timeout=30,
)


# ---------------------------------------------------------------------------
# COMMON NAME HELPERS
# ---------------------------------------------------------------------------

def _extract_name_from_dict_or_str(name_obj: Any) -> str | None:
    """
    For roster payload:
      - {'default': 'Nathan'}  (usual format)
      - 'Nathan'
      - None
    """
    if not name_obj:
        return None
    if isinstance(name_obj, dict):
        return name_obj.get("default") or next(iter(name_obj.values()), None)
    if isinstance(name_obj, str):
        return name_obj
    return None


def _split_full_name(full_name: str | None) -> Tuple[str | None, str | None]:
    """
    Naive split: first token = first_name, rest = last_name.
    Good enough for ETL purposes.
    """
    if not full_name:
        return None, None
    parts = full_name.strip().split()
    if len(parts) == 1:
        return parts[0], None
    return parts[0], " ".join(parts[1:])


# ---------------------------------------------------------------------------
# NORMALIZERS
# ---------------------------------------------------------------------------

def normalize_roster_player(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    Player dict from team_roster() -> normalized flat dict.

    Example roster player:

      {
        "id": 8479414,
        "firstName": {"default": "Nathan"},
        "lastName": {"default": "Bastian"},
        "sweaterNumber": 14,
        "positionCode": "R",
        "shootsCatches": "R",
        "birthDate": "1997-12-06",
        ...
      }
    """
    player_id = int(p["id"])

    first_name = _extract_name_from_dict_or_str(p.get("firstName"))
    last_name = _extract_name_from_dict_or_str(p.get("lastName"))

    if first_name or last_name:
        full_name = f"{(first_name or '').strip()} {(last_name or '').strip()}".strip()
    else:
        full_name = None

    birth_date = p.get("birthDate")          # YYYY-MM-DD string or None
    shoots_catches = p.get("shootsCatches")  # 'L', 'R', or None

    return {
        "player_id": player_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "birth_date": birth_date,
        "shoots_catches": shoots_catches,
    }


def normalize_stats_player(p: Dict[str, Any]) -> Dict[str, Any]:
    """
    Player dict from stats.skater_stats_summary / goalie_stats_summary.

    Typical skater stats row contains (among others):
      - playerId
      - skaterFullName
    Typical goalie stats row contains:
      - playerId
      - goalieFullName

    Bio fields like birthDate and shootsCatches may or may not be present.
    """
    raw_player_id = (
        p.get("playerId")
        or p.get("skaterId")
        or p.get("goalieId")
    )
    if raw_player_id is None:
        raise ValueError(f"stats player row missing playerId: {p}")

    player_id = int(raw_player_id)

    full_name = (
        p.get("skaterFullName")
        or p.get("goalieFullName")
        or p.get("playerName")
    )

    first_name, last_name = _split_full_name(full_name)

    birth_date = p.get("birthDate")          # may be missing
    shoots_catches = p.get("shootsCatches")  # may be missing

    return {
        "player_id": player_id,
        "first_name": first_name,
        "last_name": last_name,
        "full_name": full_name,
        "birth_date": birth_date,
        "shoots_catches": shoots_catches,
    }


# ---------------------------------------------------------------------------
# UPSERT INTO dim_player (NORMALIZED)
# ---------------------------------------------------------------------------

def upsert_player_normalized(conn, p: Dict[str, Any]) -> None:
    """
    Upsert a single normalized player into nhl_dw.dim_player using natural player_id.

    p must contain:
      - player_id
      - full_name
      - first_name
      - last_name
      - birth_date (YYYY-MM-DD or None)
      - shoots_catches
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO nhl_dw.dim_player (
                player_id,
                full_name,
                first_name,
                last_name,
                birth_date,
                shoots_catches
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE
            SET full_name      = COALESCE(EXCLUDED.full_name, nhl_dw.dim_player.full_name),
                first_name     = COALESCE(EXCLUDED.first_name, nhl_dw.dim_player.first_name),
                last_name      = COALESCE(EXCLUDED.last_name, nhl_dw.dim_player.last_name),
                birth_date     = COALESCE(EXCLUDED.birth_date, nhl_dw.dim_player.birth_date),
                shoots_catches = COALESCE(EXCLUDED.shoots_catches, nhl_dw.dim_player.shoots_catches);
            """,
            (
                p["player_id"],
                p["full_name"],
                p["first_name"],
                p["last_name"],
                p["birth_date"],
                p["shoots_catches"],
            ),
        )


def upsert_player_from_roster(conn, raw_player: Dict[str, Any]) -> None:
    p = normalize_roster_player(raw_player)
    upsert_player_normalized(conn, p)


def upsert_player_from_stats(conn, raw_player: Dict[str, Any]) -> None:
    p = normalize_stats_player(raw_player)
    upsert_player_normalized(conn, p)


# ---------------------------------------------------------------------------
# ROSTER ITERATOR
# ---------------------------------------------------------------------------

def iter_roster_players(roster: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Flatten roster['forwards'], roster['defensemen'], roster['goalies'] into
    a single stream of player dicts.
    """
    if not roster:
        return []

    groups = []
    for key in ("forwards", "defensemen", "goalies"):
        group = roster.get(key)
        if group:
            groups.append(group)

    for group in groups:
        for player in group:
            yield player


# ---------------------------------------------------------------------------
# LOADERS
# ---------------------------------------------------------------------------

def load_players_from_rosters(conn) -> None:
    """
    For all current teams from client.teams.teams():
      - call team_roster(team_abbr=..., season=SEASON_ID)
      - upsert all players into dim_player
    """
    teams = client.teams.teams()
    total_rows = 0
    unique_ids: Set[int] = set()

    for t in teams:
        team_abbr = t["abbr"]
        team_name = t["name"]

        print(f"[ROSTER] {team_abbr} – fetching roster for season {SEASON_ID}...")
        roster = client.teams.team_roster(team_abbr=team_abbr, season=SEASON_ID)

        count_for_team = 0
        for raw_player in iter_roster_players(roster):
            upsert_player_from_roster(conn, raw_player)
            total_rows += 1
            count_for_team += 1
            pid = int(raw_player["id"])
            unique_ids.add(pid)

        print(f"[ROSTER] {team_abbr} ({team_name}) – {count_for_team} players processed")

    print(
        f"[ROSTER] dim_player: processed {total_rows} roster rows, "
        f"{len(unique_ids)} unique player_ids."
    )


def load_players_from_stats(conn) -> None:
    """
    Use stats.skater_stats_summary + goalie_stats_summary to capture
    ALL players with stats in the season range, even if not currently
    on a roster.

    This covers:
      - Players traded mid-season
      - Players sent down / not on current roster
      - Short-term call-ups
    """
    print(f"[STATS] Fetching skater stats for seasons {STATS_START_SEASON}-{STATS_END_SEASON}...")
    skater_stats = client.stats.skater_stats_summary(
        start_season=STATS_START_SEASON,
        end_season=STATS_END_SEASON,
    )

    skater_count = 0
    skater_ids: Set[int] = set()
    for row in skater_stats:
        upsert_player_from_stats(conn, row)
        raw_id = row.get("playerId") or row.get("skaterId")
        if raw_id is not None:
            skater_ids.add(int(raw_id))
        skater_count += 1

    print(f"[STATS] Skaters: processed {skater_count} rows, {len(skater_ids)} unique player_ids.")

    print(f"[STATS] Fetching goalie stats for seasons {STATS_START_SEASON}-{STATS_END_SEASON}...")
    goalie_stats = client.stats.goalie_stats_summary(
        start_season=STATS_START_SEASON,
        end_season=STATS_END_SEASON,
    )

    goalie_count = 0
    goalie_ids: Set[int] = set()
    for row in goalie_stats:
        upsert_player_from_stats(conn, row)
        raw_id = row.get("playerId") or row.get("goalieId")
        if raw_id is not None:
            goalie_ids.add(int(raw_id))
        goalie_count += 1

    print(f"[STATS] Goalies: processed {goalie_count} rows, {len(goalie_ids)} unique player_ids.")


def load_all_players(conn) -> None:
    """
    Full pipeline:
      1) Load from rosters -> good bio fields for current players.
      2) Load from stats -> ensures all players with stats in the season
         range exist in dim_player, even if not on any current roster.
    """
    load_players_from_rosters(conn)
    load_players_from_stats(conn)
    conn.commit()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    print("=== Populating nhl_dw.dim_player from rosters + stats ===")
    conn = get_conn()
    try:
        load_all_players(conn)
    finally:
        conn.close()
        print("DB connection closed.")


if __name__ == "__main__":
    main()
