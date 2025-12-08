#!/usr/bin/env python3

"""
update_players.py

Tämä skripti päivittää PostgreSQL-tietokannan `players`-taulun
käyttämällä NHL roster dataa nhl-api-py kirjaston kautta.

Tämä EI hae otteluita tai boxscorea – vain nimet, numerot, pelipaikat ja nykyinen joukkue.

Ennen ajoa:
    pip install nhl-api-py psycopg2-binary
"""

from nhlpy import NHLClient
import psycopg2


# ---------------------------------------------------------
# DATABASE CONFIG
# ---------------------------------------------------------

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "nhl_db"
DB_USER = "nhl_user"
DB_PASSWORD = "strongpassword"   # Muuta omaksi


# ---------------------------------------------------------
# DB CONNECTION
# ---------------------------------------------------------

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# ---------------------------------------------------------
# PLAYER UPSERT LOGIC
# ---------------------------------------------------------

client = NHLClient(debug=False)


def get_all_team_abbreviations(conn):
    """
    Hakee teams-taulusta joukkueiden abbreviations + IDs
    """
    with conn.cursor() as cur:
        cur.execute("SELECT team_id, abbreviation FROM teams ORDER BY abbreviation;")
        return cur.fetchall()


def upsert_roster_players(conn):
    """
    Hakee rosterit ja upserttaa pelaajat tietokantaan.
    """

    teams = get_all_team_abbreviations(conn)

    for team_id, abbr in teams:
        print(f"[ROSTER] Haetaan joukkueelle: {abbr}")

        try:
            roster = client.teams.roster(team_abbrev=abbr)
        except Exception as e:
            print(f"[VIRHE] Ei saatu rosteria joukkueelle {abbr}: {e}")
            continue

        players = roster.get("players", [])

        with conn.cursor() as cur:
            for p in players:
                player_id = int(p["playerId"])
                full_name = p.get("fullName")
                first_name = p.get("firstName")
                last_name = p.get("lastName")
                sweater_number = p.get("sweaterNumber")
                shoots_catches = p.get("shootsCatches")
                primary_position = p.get("positionCode")
                birth_date = p.get("birthDate")

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
                        primary_position,
                        sweater_number,
                        birth_date,
                        team_id,
                    ),
                )

        conn.commit()
        print(f"[OK] {len(players)} pelaajaa päivitetty joukkueelta {abbr}")

    print("\n=== PLAYERS TAULU ON NYT PÄIVITETTY ===")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    conn = get_conn()
    try:
        upsert_roster_players(conn)
    finally:
        conn.close()
        print("Tietokantayhteys suljettu.")


if __name__ == "__main__":
    main()
