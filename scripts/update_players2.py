#!/usr/bin/env python3

"""
populate_players_from_boxscores.py

Täyttää players-taulun pelaajien nimillä, ID:illä ja perusdatalla
käyttämällä nhl-api-py:n game_center.boxscore -dataa.

Käyttää samoja INSERT/UPSERT-logiikoita kuin varsinainen ETL,
mutta EI koske player_game_stats-tauluun.
"""

from typing import Dict, Any, Iterable

from nhlpy import NHLClient
import psycopg2


# ---------------------------------------------------------
# DATABASE CONFIG
# ---------------------------------------------------------

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "nhl_db"
DB_USER = "nhl_user"
DB_PASSWORD = "strongpassword"   # muuta tarvittaessa


# ---------------------------------------------------------
# DB CONNECTION
# ---------------------------------------------------------

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


# ---------------------------------------------------------
# NHL CLIENT
# ---------------------------------------------------------

client = NHLClient(debug=False, timeout=30)


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def _iter_boxscore_players(team_block: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Kerää kaikki pelaajat homeTeam/awayTeam playerByGameStats -rakenneosasta.
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


def upsert_player_from_boxscore_player(cur, player: Dict[str, Any], team_id: int):
    """
    Pelaaja (dict boxscoresta) -> INSERT/UPDATE players-tauluun.
    Odottaa skeemaa:
      players(player_id, full_name, first_name, last_name,
              shoots_catches, primary_position, sweater_number,
              birth_date, current_team_id)
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
    birth_date = player.get("birthDate")

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


# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

def populate_players_from_all_games(conn, season_code: str = "20252026"):
    """
    Käy läpi kaikki annetun kauden pelit, hakee boxscoret,
    ja päivittää players-taulun nimillä ym.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT game_id, home_team_id, away_team_id
            FROM games
            WHERE season = %s
            ORDER BY game_date NULLS LAST, game_id;
            """,
            (season_code,),
        )
        games = cur.fetchall()

    print(f"Löytyi {len(games)} peliä kaudelta {season_code}.")

    for idx, (game_id, home_team_id, away_team_id) in enumerate(games, start=1):
        print(f"[{idx}/{len(games)}] Käsitellään peli {game_id}...")

        boxscore = client.game_center.boxscore(game_id=str(game_id))
        pbs = boxscore.get("playerByGameStats", {})
        home_block = pbs.get("homeTeam", {})
        away_block = pbs.get("awayTeam", {})

        with conn.cursor() as cur:
            for p in _iter_boxscore_players(home_block):
                upsert_player_from_boxscore_player(cur, p, home_team_id)

            for p in _iter_boxscore_players(away_block):
                upsert_player_from_boxscore_player(cur, p, away_team_id)

        conn.commit()

    print("=== PLAYERS-taulu täytetty boxscorejen perusteella ===")


# ---------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------

def main():
    conn = get_conn()
    try:
        # Vaihda season_code, jos haluat käyttää eri kautta
        populate_players_from_all_games(conn, season_code="20252026")
    finally:
        conn.close()
        print("Tietokantayhteys suljettu.")


if __name__ == "__main__":
    main()
