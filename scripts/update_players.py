#!/usr/bin/env python3
"""
update_players.py

Päivittää PostgreSQL-tietokannan `players`-taulun NHL:n
joukkue-rosterien perusteella käyttäen nhl-api-py:n team_roster-metodia.

Rakenne roster-pelaajalle (esimerkki):

{
  'id': 8484153,
  'firstName': {'default': 'Leo'},
  'lastName': {'default': 'Carlsson'},
  'sweaterNumber': 91,
  'positionCode': 'C',
  'shootsCatches': 'L',
  'birthDate': '2004-12-26',
  ...
}
"""

from typing import Dict, Any, Iterable

import psycopg2
from nhlpy import NHLClient


# ---------------------------------------------------------
# DATABASE CONFIG
# ---------------------------------------------------------

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "nhl_db"
DB_USER = "nhl_user"
DB_PASSWORD = "strongpassword"  # muuta tarvittaessa

SEASON_CODE = "20252026"        # esim. "20252026"


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

def _get_localized(value: Any) -> Any:
    """
    NHL:n uusi API käyttää usein muotoa:
      {"default": "Leo"}
    Tämä funktio palauttaa value["default"], jos value on dict,
    muuten value sellaisenaan.
    """
    if isinstance(value, dict):
        # tyypillisesti {'default': 'Leo'}
        return value.get("default") or next(iter(value.values()), None)
    return value


def get_all_team_abbreviations(conn) -> list[tuple[int, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT team_id, abbreviation "
            "FROM teams "
            "ORDER BY abbreviation;"
        )
        return cur.fetchall()


def iter_roster_players(roster: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    """
    Palauttaa kaikki pelaaja-dictit roster-rakenteesta.

    roster-objekti sisältää tyypillisesti:
      - 'forwards':   [...]
      - 'defensemen': [...]
      - 'goalies':    [...]
    ja jokainen listan alkio on yksittäinen pelaaja-dict kuten DEBUG-printti näytti.
    """
    if not roster:
        return []

    groups: list[list[Dict[str, Any]]] = []
    for key in ("forwards", "defensemen", "goalies"):
        grp = roster.get(key)
        if grp:
            groups.append(grp)

    for grp in groups:
        for player in grp:
            yield player


def extract_player_id(player: Dict[str, Any]) -> int:
    """
    Hakee pelaaja-ID:n roster-pelaajasta.

    DEBUG-datan mukaan avain on 'id'.
    """
    if "id" in player and player["id"] is not None:
        return int(player["id"])

    # fallbackit, jos joskus rakenne muuttuu
    for key in ("playerId", "playerID"):
        if key in player and player[key] is not None:
            return int(player[key])

    raise KeyError(f"Pelaaja-ID:tä ei löytynyt. Avaimet: {list(player.keys())}")


def upsert_player(cur, player: Dict[str, Any], team_id: int) -> None:
    """
    Yksittäinen roster-pelaaja -> INSERT/UPDATE players-tauluun.

    Odotettu players-skeema:
      player_id (PK),
      full_name,
      first_name,
      last_name,
      shoots_catches,
      primary_position,
      sweater_number,
      birth_date,
      current_team_id
    """
    player_id = extract_player_id(player)

    # Nimet: muotoa {'default': 'Leo'} -> "Leo"
    first_name = _get_localized(player.get("firstName"))
    last_name = _get_localized(player.get("lastName"))

    # Täydellinen nimi: joko fullName tai first + last
    full_name = _get_localized(player.get("fullName"))
    if not full_name:
        full_name = ((first_name or "") + " " + (last_name or "")).strip() or None

    shoots_catches = player.get("shootsCatches")
    position_code = player.get("positionCode")
    sweater_number = player.get("sweaterNumber")
    birth_date = player.get("birthDate")  # 'YYYY-MM-DD' string, Postgres DATE osaa tämän

    cur.execute(
        """
        INSERT INTO players (
            player_id,
            full_name,
            first_name,
            last_name,
            shoots_catches,
            primary_position,
            sweater_number,
            birth_date,
            current_team_id
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

def update_players_from_rosters(conn, season_code: str) -> None:
    teams = get_all_team_abbreviations(conn)
    total_players = 0
    debug_done = False

    for team_id, abbr in teams:
        print(f"[ROSTER] Haetaan joukkueelle {abbr}, kausi {season_code}...")

        try:
            roster = client.teams.team_roster(
                team_abbr=abbr,
                season=season_code,
            )
        except Exception as e:
            print(f"[VIRHE] Roster-haku epäonnistui joukkueelle {abbr}: {e}")
            continue

        players = list(iter_roster_players(roster))
        print(f"  -> löytyi {len(players)} pelaajaa")

        # Yksi debug-print ensimmäisestä pelaajasta, jos haluat tarkistaa rakenteen
        if players and not debug_done:
            sample = players[0]
            print("\n[DEBUG] Esimerkkipelaajan avaimet:")
            print(list(sample.keys()))
            print("[DEBUG] Esimerkkipelaajan data:")
            print(sample)
            print("----\n")
            debug_done = True

        with conn.cursor() as cur:
            for p in players:
                try:
                    upsert_player(cur, p, team_id)
                except KeyError as ke:
                    print(f"[VAROITUS] Pelaaja skippattiin joukkueelta {abbr}: {ke}")
                    continue

        conn.commit()
        total_players += len(players)

    print(f"\n=== VALMIS: käsitelty yhteensä noin {total_players} roster-pelaajaa ===")


# ---------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------

def main() -> None:
    conn = get_conn()
    try:
        update_players_from_rosters(conn, season_code=SEASON_CODE)
    finally:
        conn.close()
        print("Tietokantayhteys suljettu.")


if __name__ == "__main__":
    main()
