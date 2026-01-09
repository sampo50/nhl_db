#!/usr/bin/env python3

"""
update_player_game_stats_fields.py

Päivittää player_game_stats-taulun seuraavat kentät boxscore-datasta:

  - time_on_ice
  - shots
  - penalty_minutes
  - faceoff_wins
  - faceoff_losses

Olettaa, että player_game_stats-rivit (game_id, player_id) on jo olemassa.
"""

from typing import Dict, Any, Iterable

import psycopg2
from nhlpy import NHLClient


# ---------------------------------------------------------
# DB-KONFIGURAATIO
# ---------------------------------------------------------

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "nhl_db"
DB_USER = "nhl_user"
DB_PASSWORD = "strongpassword"  # vaihda omaksesi

SEASON_CODE = "20252026"        # kauden tunniste games.season-kentässä


# ---------------------------------------------------------
# YHTEYS JA NHL-CLIENT
# ---------------------------------------------------------

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


client = NHLClient(debug=False, timeout=30)


# ---------------------------------------------------------
# APURIT
# ---------------------------------------------------------

def _iter_boxscore_players(team_block: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Kerää kaikki pelaajat playerByGameStats['homeTeam'/'awayTeam'] -blockista.
    """
    if not team_block:
        return []

    groups = []
    for key in ("forwards", "defensemen", "defense", "goalies"):
        grp = team_block.get(key)
        if grp:
            groups.append(grp)

    for grp in groups:
        for player in grp:
            yield player


def _extract_player_id(player: Dict[str, Any]) -> int:
    """
    Hakee playerId-kentän boxscore-pelaajasta.
    """
    if "playerId" in player and player["playerId"] is not None:
        return int(player["playerId"])
    if "id" in player and player["id"] is not None:
        return int(player["id"])
    raise KeyError(f"Ei playerId/id-avainetta pelaajassa: {list(player.keys())}")


# ---------------------------------------------------------
# PÄIVITYS YHDELLE PELILLE
# ---------------------------------------------------------

def update_stats_for_game(conn, game_id: int):
    """
    Hakee boxscoren yhdelle pelille ja päivittää
    player_game_stats-taulun lisäkentät tälle pelille.
    """
    boxscore = client.game_center.boxscore(game_id=str(game_id))
    pbs = boxscore.get("playerByGameStats", {})

    home_block = pbs.get("homeTeam", {})
    away_block = pbs.get("awayTeam", {})

    updated_rows = 0

    with conn.cursor() as cur:
        # Kotijoukkueen pelaajat
        for p in _iter_boxscore_players(home_block):
            try:
                player_id = _extract_player_id(p)
            except KeyError as e:
                print(f"[WARN] game {game_id}: ei playerId kotijoukkueen pelaajalla: {e}")
                continue

            time_on_ice     = p.get("timeOnIce")
            shots           = p.get("shots")
            penalty_minutes = p.get("penaltyMinutes")
            faceoff_wins    = p.get("faceoffWins")
            faceoff_losses  = p.get("faceoffLosses")

            cur.execute(
                """
                UPDATE player_game_stats
                SET time_on_ice      = %s,
                    shots           = %s,
                    penalty_minutes = %s,
                    faceoff_wins    = %s,
                    faceoff_losses  = %s
                WHERE game_id = %s
                  AND player_id = %s;
                """,
                (
                    time_on_ice,
                    shots,
                    penalty_minutes,
                    faceoff_wins,
                    faceoff_losses,
                    game_id,
                    player_id,
                ),
            )
            updated_rows += cur.rowcount

        # Vierasjoukkueen pelaajat
        for p in _iter_boxscore_players(away_block):
            try:
                player_id = _extract_player_id(p)
            except KeyError as e:
                print(f"[WARN] game {game_id}: ei playerId vierasjoukkueen pelaajalla: {e}")
                continue

            time_on_ice     = p.get("timeOnIce")
            shots           = p.get("shots")
            penalty_minutes = p.get("penaltyMinutes")
            faceoff_wins    = p.get("faceoffWins")
            faceoff_losses  = p.get("faceoffLosses")

            cur.execute(
                """
                UPDATE player_game_stats
                SET time_on_ice      = %s,
                    shots           = %s,
                    penalty_minutes = %s,
                    faceoff_wins    = %s,
                    faceoff_losses  = %s
                WHERE game_id = %s
                  AND player_id = %s;
                """,
                (
                    time_on_ice,
                    shots,
                    penalty_minutes,
                    faceoff_wins,
                    faceoff_losses,
                    game_id,
                    player_id,
                ),
            )
            updated_rows += cur.rowcount

    conn.commit()
    print(f"[GAME] {game_id}: päivitetty {updated_rows} riviä player_game_stats-taulussa.")


# ---------------------------------------------------------
# KAIKKI PELIT KAUSELTA
# ---------------------------------------------------------

def update_all_games_for_season(conn, season_code: str):
    """
    Lukee games-taulusta kaikki kauden pelit ja päivittää
    player_game_stats-lisäkentät jokaiselle pelille.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT game_id
            FROM games
            WHERE season = %s
            ORDER BY game_date NULLS LAST, game_id;
            """,
            (season_code,),
        )
        games = [row[0] for row in cur.fetchall()]

    print(f"Löytyi {len(games)} peliä kaudelta {season_code}.")

    for idx, game_id in enumerate(games, start=1):
        print(f"[{idx}/{len(games)}] Päivitetään peli {game_id}...")
        update_stats_for_game(conn, game_id)

    print("=== VALMIS: player_game_stats lisäkentät päivitetty kaikille peleille ===")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    conn = get_conn()
    try:
        update_all_games_for_season(conn, SEASON_CODE)
    finally:
        conn.close()
        print("Tietokantayhteys suljettu.")


if __name__ == "__main__":
    main()
