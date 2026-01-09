#!/usr/bin/env python3

from nhlpy import NHLClient
import psycopg2
from datetime import date, timedelta

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "nhl_db"
DB_USER = "nhl_user"
DB_PASSWORD = "strongpassword"

SEASON_ID = "20252026"

client = NHLClient(debug=True, timeout=30)

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def get_pbp(game_id: str):
    endpoint = f"/v1/gamecenter/{game_id}/play-by-play"
    return client._http_client.get(endpoint=endpoint)


def load_events_for_game(conn, game_key, game_id):
    pbp = get_pbp(str(game_id))

    plays = pbp.get("plays", [])
    print(f"Game {game_id}: {len(plays)} events")

    with conn.cursor() as cur:
        for idx, play in enumerate(plays):
            details = play.get("details", {})

            cur.execute("""
                INSERT INTO nhl_dw.event_play (
                    game_key,
                    event_index,
                    period,
                    time_in_period,
                    type_code,
                    type_desc,
                    x, y,
                    shooter_id,
                    goalie_id,
                    team_id,
                    raw_json
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,
                    %s,%s,%s,
                    %s
                );
            """, (
                game_key,
                idx,
                play.get("period"),
                play.get("timeInPeriod"),
                play.get("typeCode"),
                play.get("typeDescKey"),
                details.get("xCoord"),
                details.get("yCoord"),
                details.get("shooterId"),
                details.get("goalieId"),
                details.get("eventOwnerTeamId"),
                play  # JSONB
            ))


    conn.commit()


def load_season_events(conn):
    # Get all games for the season
    with conn.cursor() as cur:
        cur.execute("""
            SELECT game_key, game_id
            FROM nhl_dw.fact_game
            WHERE season_key = (
                SELECT season_key FROM nhl_dw.dim_season WHERE season_id = %s
            )
            ORDER BY game_id;
        """, (SEASON_ID,))
        games = cur.fetchall()

    print(f"Found {len(games)} games for season {SEASON_ID}")

    for game_key, game_id in games:
        print(f"==== Loading events for game_id={game_id} ====")
        try:
            load_events_for_game(conn, game_key, game_id)
        except Exception as e:
            print(f"Error loading game {game_id}: {e}")
            conn.rollback()


def main():
    conn = get_conn()
    try:
        load_season_events(conn)
    finally:
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()
