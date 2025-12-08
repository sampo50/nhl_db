#!/usr/bin/env python3
"""
nhl_loader_2025_26.py

Lataa kauden 2025–26 datan (teams, games, players, player_game_stats)
PostgreSQL-tietokantaan nhl-api-py -kirjastolla.

Ennen käyttöä:
    pip install nhl-api-py psycopg2-binary
"""

from datetime import date, timedelta
from typing import Dict, Any, Iterable

from nhlpy import NHLClient
import psycopg2


# ---------------------------------------------------------------------------
# KONFIGURAATIO
# ---------------------------------------------------------------------------

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "nhl_db"
DB_USER = "nhl_user"
DB_PASSWORD = "strongpassword"   # vaihda omiin tietoihin

# Kausi 2025–26: päivämäärät ja season-koodi tietokantaan
# Päivämäärät kannattaa päivittää vastaamaan oikeaa runkosarjan ikkunaa.
SEASON_START_DATE = date(2025, 10, 8)
SEASON_END_DATE   = date(2026, 4, 15)
SEASON_CODE       = "20252026"   # tallennetaan games.season -kenttään


# ---------------------------------------------------------------------------
# YHTEYS TIETOKANTAAN
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
# NHL API -CLIENT
# ---------------------------------------------------------------------------

client = NHLClient(
    debug=True,
    timeout=30,
)


# ---------------------------------------------------------------------------
# TEAMS
# ---------------------------------------------------------------------------

def upsert_teams(conn):
    """
    Hakee kaikki nykyiset joukkueet ja upserttaa teams-tauluun.
    Odottaa skeemaa:
      teams(team_id IDENTITY PK, name, abbreviation UNIQUE, city, conference, division)
    """
    teams = client.teams.teams()

    with conn.cursor() as cur:
        for t in teams:
            abbr = t["abbr"]
            name = t["name"]
            conference = t["conference"]["name"]
            division = t["division"]["name"]
            city = None  # halutessa voi täydentää, jos API palauttaa paikkakunnan

            cur.execute(
                """
                INSERT INTO teams (name, abbreviation, city, conference, division)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (abbreviation) DO UPDATE
                SET name       = EXCLUDED.name,
                    city       = EXCLUDED.city,
                    conference = EXCLUDED.conference,
                    division   = EXCLUDED.division
                RETURNING team_id;
                """,
                (name, abbr, city, conference, division),
            )
            team_id = cur.fetchone()[0]
            print(f"Teams: {abbr} -> team_id={team_id}")

    conn.commit()
    print(f"Teams: tallennettu {len(teams)} joukkuetta.")


def get_team_id_by_abbr(conn, abbr: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT team_id FROM teams WHERE abbreviation = %s",
            (abbr,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Team-ID puuttuu abbreviationille {abbr}")
    return row[0]


# ---------------------------------------------------------------------------
# GAMES
# ---------------------------------------------------------------------------

def upsert_games_for_date(conn, d: date):
    """
    Hakee yhden päivän pelit ja upserttaa games-tauluun.
    Odottaa skeemaa:
      games(game_id PK, season TEXT, game_type INT, game_date TIMESTAMPTZ,
            home_team_id FK, away_team_id FK, home_score, away_score, venue)
    """
    schedule_payload = client.schedule.daily_schedule(date=d.isoformat())
    games = schedule_payload.get("games", [])
    if not games:
        print(f"Games: ei pelejä päivälle {d.isoformat()}.")
        return

    with conn.cursor() as cur:
        for g in games:
            home_abbr = g["homeTeam"]["abbrev"]
            away_abbr = g["awayTeam"]["abbrev"]

            home_team_id = get_team_id_by_abbr(conn, home_abbr)
            away_team_id = get_team_id_by_abbr(conn, away_abbr)

            link = g["gameCenterLink"]
            game_id = int(link.split("/")[-1])

            game_date = g.get("startTimeUTC")
            game_type = g.get("gameType")
            home_score = g["homeTeam"].get("score")
            away_score = g["awayTeam"].get("score")

            # Kaikki nämä pelit kuuluvat kauteen 2025–26
            season = SEASON_CODE
            venue = None  # voidaan hakea tarkemmin boxscore/landing endpointista myöhemmin

            cur.execute(
                """
                INSERT INTO games (
                    game_id, season, game_type, game_date,
                    home_team_id, away_team_id, home_score, away_score, venue
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (game_id) DO UPDATE
                SET season       = EXCLUDED.season,
                    game_type    = EXCLUDED.game_type,
                    game_date    = EXCLUDED.game_date,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    home_score   = EXCLUDED.home_score,
                    away_score   = EXCLUDED.away_score,
                    venue        = EXCLUDED.venue;
                """,
                (
                    game_id,
                    season,
                    game_type,
                    game_date,
                    home_team_id,
                    away_team_id,
                    home_score,
                    away_score,
                    venue,
                ),
            )

            print(f"Games: {away_abbr} @ {home_abbr} ({game_id}) upsertattu.")

    conn.commit()
    print(f"Games: tallennettu {len(games)} ottelua päivältä {d.isoformat()}.")


# ---------------------------------------------------------------------------
# PLAYERS & PLAYER_GAME_STATS – BOXSCORISTA
# ---------------------------------------------------------------------------

def upsert_player_from_boxscore_player(
    conn,
    player: Dict[str, Any],
    team_id: int,
):
    """
    playerByGameStats-pelaaja -> players-taulu.
    Odottaa skeemaa:
      players(player_id PK, full_name, first_name, last_name,
              shoots_catches, primary_position, sweater_number,
              birth_date, current_team_id FK)
    """
    player_id = int(player["playerId"])
    first_name = player.get("firstName")
    last_name = player.get("lastName")
    full_name = (
        player.get("playerName")
        or player.get("fullName")
        or ((first_name or "") + " " + (last_name or "")).strip()
    )

    shoots_catches = player.get("shootsCatches")
    position_code = player.get("positionCode")
    sweater_number = player.get("sweaterNumber")
    birth_date = player.get("birthDate")  # ei välttämättä mukana

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO players (
                player_id, full_name, first_name, last_name,
                shoots_catches, primary_position, sweater_number,
                birth_date, current_team_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (player_id) DO UPDATE
            SET full_name        = EXCLUDED.full_name,
                first_name       = EXCLUDED.first_name,
                last_name        = EXCLUDED.last_name,
                shoots_catches   = EXCLUDED.shoots_catches,
                primary_position = EXCLUDED.primary_position,
                sweater_number   = EXCLUDED.sweater_number,
                birth_date       = COALESCE(EXCLUDED.birth_date, players.birth_date),
                current_team_id  = EXCLUDED.current_team_id;
            """,
            (
                player_id,
                full_name,
                first_name,
                last_name,
                shoots_catches,
                position_code,
                sweater_number,
                birth_date,
                team_id,
            ),
        )


def upsert_player_game_stats_from_boxscore_player(
    conn,
    game_id: int,
    team_id: int,
    player: Dict[str, Any],
):
    """
    Pelaajan ottelukohtaiset tilastot -> player_game_stats.
    Odottaa skeemaa, jossa on ainakin sarakkeet:
      game_id, player_id, team_id, position_code, time_on_ice,
      goals, assists, points, shots, hits, blocks,
      plus_minus, penalty_minutes,
      faceoff_wins, faceoff_losses,
      saves, shots_against, goals_against, save_pct
    """
    player_id = int(player["playerId"])
    position_code = player.get("positionCode")

    goals = player.get("goals")
    assists = player.get("assists")
    points = player.get("points")
    shots = player.get("shots")
    hits = player.get("hits")
    blocks = player.get("blockedShots") or player.get("blocks")
    plus_minus = player.get("plusMinus")
    toi = player.get("timeOnIce")

    pim = player.get("penaltyMinutes")
    faceoff_wins = player.get("faceoffWins")
    faceoff_losses = player.get("faceoffLosses")

    saves = player.get("saves")
    shots_against = player.get("shotsAgainst")
    goals_against = player.get("goalsAgainst")
    save_pct = player.get("savePct") or player.get("savePercentage")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO player_game_stats (
                game_id, player_id, team_id,
                position_code, time_on_ice,
                goals, assists, points, shots, hits, blocks,
                plus_minus, penalty_minutes,
                faceoff_wins, faceoff_losses,
                saves, shots_against, goals_against, save_pct
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (game_id, player_id) DO UPDATE
            SET team_id         = EXCLUDED.team_id,
                position_code   = EXCLUDED.position_code,
                time_on_ice     = EXCLUDED.time_on_ice,
                goals           = EXCLUDED.goals,
                assists         = EXCLUDED.assists,
                points          = EXCLUDED.points,
                shots           = EXCLUDED.shots,
                hits            = EXCLUDED.hits,
                blocks          = EXCLUDED.blocks,
                plus_minus      = EXCLUDED.plus_minus,
                penalty_minutes = EXCLUDED.penalty_minutes,
                faceoff_wins    = EXCLUDED.faceoff_wins,
                faceoff_losses  = EXCLUDED.faceoff_losses,
                saves           = EXCLUDED.saves,
                shots_against   = EXCLUDED.shots_against,
                goals_against   = EXCLUDED.goals_against,
                save_pct        = EXCLUDED.save_pct;
            """,
            (
                game_id,
                player_id,
                team_id,
                position_code,
                toi,
                goals,
                assists,
                points,
                shots,
                hits,
                blocks,
                plus_minus,
                pim,
                faceoff_wins,
                faceoff_losses,
                saves,
                shots_against,
                goals_against,
                save_pct,
            ),
        )


def _iter_boxscore_players(team_block: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Palauttaa kaikki pelaajat annettujen ryhmien alta
    (forwards, defensemen/defense, goalies).
    """
    if not team_block:
        return []

    groups = []
    for key in ("forwards", "defensemen", "defense", "goalies"):
        group = team_block.get(key)
        if group:
            groups.append(group)

    for group in groups:
        for player in group:
            yield player


def load_player_stats_for_game(conn, game_id: int, home_team_id: int, away_team_id: int):
    """
    Hakee boxscoren yhdelle pelille ja täyttää players + player_game_stats.
    """
    boxscore = client.game_center.boxscore(game_id=str(game_id))

    pbs = boxscore.get("playerByGameStats", {})
    home_block = pbs.get("homeTeam", {})
    away_block = pbs.get("awayTeam", {})

    # Käsitellään kaikki kotijoukkueen pelaajat
    for p in _iter_boxscore_players(home_block):
        upsert_player_from_boxscore_player(conn, p, home_team_id)
        upsert_player_game_stats_from_boxscore_player(conn, game_id, home_team_id, p)

    # Käsitellään kaikki vierasjoukkueen pelaajat
    for p in _iter_boxscore_players(away_block):
        upsert_player_from_boxscore_player(conn, p, away_team_id)
        upsert_player_game_stats_from_boxscore_player(conn, game_id, away_team_id, p)

    conn.commit()
    print(f"Player stats: ladattu peli {game_id}.")


def load_player_stats_for_all_games(conn):
    """
    Hakee boxscoret vain 2025–26 kauden peleille (season = SEASON_CODE).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT game_id, home_team_id, away_team_id
            FROM games
            WHERE season = %s
            ORDER BY game_date NULLS LAST, game_id;
            """,
            (SEASON_CODE,),
        )
        rows = cur.fetchall()

    for game_id, home_team_id, away_team_id in rows:
        load_player_stats_for_game(conn, game_id, home_team_id, away_team_id)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    conn = get_conn()
    try:
        print("=== Päivitetään joukkueet ===")
        upsert_teams(conn)

        print("=== Päivitetään ottelut koko kaudelle 2025–26 ===")
        d = SEASON_START_DATE
        while d <= SEASON_END_DATE:
            print(f"-- Päivä {d.isoformat()} --")
            upsert_games_for_date(conn, d)
            d += timedelta(days=1)

        print("=== Päivitetään pelaajat ja player_game_stats boxscoreista (vain 2025–26) ===")
        load_player_stats_for_all_games(conn)

    finally:
        conn.close()
        print("Yhteys tietokantaan suljettu.")


if __name__ == "__main__":
    main()
