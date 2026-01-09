-- DROP SCHEMA nhl_dw;

CREATE SCHEMA nhl_dw AUTHORIZATION nhl_user;

-- DROP SEQUENCE nhl_dw.dim_player_player_key_seq;

CREATE SEQUENCE nhl_dw.dim_player_player_key_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    START 1
    CACHE 1
    NO CYCLE;
-- DROP SEQUENCE nhl_dw.dim_season_season_key_seq;

CREATE SEQUENCE nhl_dw.dim_season_season_key_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    START 1
    CACHE 1
    NO CYCLE;
-- DROP SEQUENCE nhl_dw.dim_team_team_key_seq;

CREATE SEQUENCE nhl_dw.dim_team_team_key_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    START 1
    CACHE 1
    NO CYCLE;
-- DROP SEQUENCE nhl_dw.dim_venue_venue_key_seq;

CREATE SEQUENCE nhl_dw.dim_venue_venue_key_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    START 1
    CACHE 1
    NO CYCLE;
-- DROP SEQUENCE nhl_dw.event_play_event_key_seq;

CREATE SEQUENCE nhl_dw.event_play_event_key_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    START 1
    CACHE 1
    NO CYCLE;
-- DROP SEQUENCE nhl_dw.fact_game_game_key_seq;

CREATE SEQUENCE nhl_dw.fact_game_game_key_seq
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 2147483647
    START 1
    CACHE 1
    NO CYCLE;-- nhl_dw.dim_date definition

-- Drop table

-- DROP TABLE nhl_dw.dim_date;

CREATE TABLE nhl_dw.dim_date (
    date_key date NOT NULL,
    "year" int2 NULL,
    "month" int2 NULL,
    "day" int2 NULL,
    day_of_week int2 NULL,
    week_of_year int2 NULL,
    month_name text NULL,
    is_weekend bool NULL,
    CONSTRAINT dim_date_pkey PRIMARY KEY (date_key)
);


-- nhl_dw.dim_player definition

-- Drop table

-- DROP TABLE nhl_dw.dim_player;

CREATE TABLE nhl_dw.dim_player (
    player_key serial4 NOT NULL,
    player_id int8 NOT NULL,
    full_name text NOT NULL,
    first_name text NULL,
    last_name text NULL,
    shoots_catches text NULL,
    primary_position text NULL,
    sweater_number int2 NULL,
    birth_date date NULL,
    active bool DEFAULT true NULL,
    created_at timestamptz DEFAULT now() NULL,
    updated_at timestamptz DEFAULT now() NULL,
    CONSTRAINT dim_player_pkey PRIMARY KEY (player_key),
    CONSTRAINT dim_player_player_id_key UNIQUE (player_id)
);


-- nhl_dw.dim_season definition

-- Drop table

-- DROP TABLE nhl_dw.dim_season;

CREATE TABLE nhl_dw.dim_season (
    season_key serial4 NOT NULL,
    season_id text NOT NULL,
    start_year int2 NULL,
    end_year int2 NULL,
    is_current bool NULL,
    created_at timestamptz DEFAULT now() NULL,
    CONSTRAINT dim_season_pkey PRIMARY KEY (season_key),
    CONSTRAINT dim_season_season_id_key UNIQUE (season_id)
);


-- nhl_dw.dim_team definition

-- Drop table

-- DROP TABLE nhl_dw.dim_team;

CREATE TABLE nhl_dw.dim_team (
    team_key serial4 NOT NULL,
    team_id int4 NOT NULL,
    team_name text NOT NULL,
    team_abbrev text NOT NULL,
    team_short text NULL,
    city text NULL,
    franchise_id int4 NULL,
    conference text NULL,
    division text NULL,
    created_at timestamptz DEFAULT now() NULL,
    updated_at timestamptz DEFAULT now() NULL,
    CONSTRAINT dim_team_pkey PRIMARY KEY (team_key),
    CONSTRAINT dim_team_team_id_key UNIQUE (team_id)
);


-- nhl_dw.dim_venue definition

-- Drop table

-- DROP TABLE nhl_dw.dim_venue;

CREATE TABLE nhl_dw.dim_venue (
    venue_key serial4 NOT NULL,
    venue_id int4 NULL,
    venue_name text NOT NULL,
    city text NULL,
    state text NULL,
    country text NULL,
    CONSTRAINT dim_venue_pkey PRIMARY KEY (venue_key),
    CONSTRAINT dim_venue_venue_id_key UNIQUE (venue_id),
    CONSTRAINT dim_venue_venue_name_key UNIQUE (venue_name)
);


-- nhl_dw.fact_game definition

-- Drop table

-- DROP TABLE nhl_dw.fact_game;

CREATE TABLE nhl_dw.fact_game (
    game_key serial4 NOT NULL,
    game_id int8 NOT NULL,
    season_key int4 NOT NULL,
    date_key date NOT NULL,
    home_team_key int4 NOT NULL,
    away_team_key int4 NOT NULL,
    home_score int2 NULL,
    away_score int2 NULL,
    game_type text NULL,
    venue_key int4 NULL,
    start_time_utc timestamptz NULL,
    end_time_utc timestamptz NULL,
    went_overtime bool NULL,
    went_shootout bool NULL,
    created_at timestamptz DEFAULT now() NULL,
    updated_at timestamptz DEFAULT now() NULL,
    CONSTRAINT fact_game_game_id_key UNIQUE (game_id),
    CONSTRAINT fact_game_pkey PRIMARY KEY (game_key),
    CONSTRAINT fact_game_away_team_key_fkey FOREIGN KEY (away_team_key) REFERENCES nhl_dw.dim_team(team_key),
    CONSTRAINT fact_game_date_key_fkey FOREIGN KEY (date_key) REFERENCES nhl_dw.dim_date(date_key),
    CONSTRAINT fact_game_home_team_key_fkey FOREIGN KEY (home_team_key) REFERENCES nhl_dw.dim_team(team_key),
    CONSTRAINT fact_game_season_key_fkey FOREIGN KEY (season_key) REFERENCES nhl_dw.dim_season(season_key),
    CONSTRAINT fact_game_venue_key_fkey FOREIGN KEY (venue_key) REFERENCES nhl_dw.dim_venue(venue_key)
);
CREATE INDEX fact_game_date_idx ON nhl_dw.fact_game USING btree (date_key);
CREATE INDEX fact_game_season_idx ON nhl_dw.fact_game USING btree (season_key);


-- nhl_dw.fact_goalie_game definition

-- Drop table

-- DROP TABLE nhl_dw.fact_goalie_game;

CREATE TABLE nhl_dw.fact_goalie_game (
    game_key int4 NOT NULL,
    player_key int4 NOT NULL,
    team_key int4 NOT NULL,
    toi_seconds int4 NULL,
    shots_against int2 NULL,
    saves int2 NULL,
    goals_against int2 NULL,
    save_pct numeric(5, 3) NULL,
    shutout bool NULL,
    created_at timestamptz DEFAULT now() NULL,
    updated_at timestamptz DEFAULT now() NULL,
    CONSTRAINT fact_goalie_game_pkey PRIMARY KEY (game_key, player_key),
    CONSTRAINT fact_goalie_game_game_key_fkey FOREIGN KEY (game_key) REFERENCES nhl_dw.fact_game(game_key),
    CONSTRAINT fact_goalie_game_player_key_fkey FOREIGN KEY (player_key) REFERENCES nhl_dw.dim_player(player_key),
    CONSTRAINT fact_goalie_game_team_key_fkey FOREIGN KEY (team_key) REFERENCES nhl_dw.dim_team(team_key)
);


-- nhl_dw.fact_skater_game definition

-- Drop table

-- DROP TABLE nhl_dw.fact_skater_game;

CREATE TABLE nhl_dw.fact_skater_game (
    game_key int4 NOT NULL,
    player_key int4 NOT NULL,
    team_key int4 NOT NULL,
    toi_seconds int4 NULL,
    goals int2 NULL,
    assists int2 NULL,
    points int2 NULL,
    shots int2 NULL,
    hits int2 NULL,
    blocks int2 NULL,
    plus_minus int2 NULL,
    penalty_minutes int2 NULL,
    created_at timestamptz DEFAULT now() NULL,
    updated_at timestamptz DEFAULT now() NULL,
    CONSTRAINT fact_skater_game_pkey PRIMARY KEY (game_key, player_key),
    CONSTRAINT fact_skater_game_game_key_fkey FOREIGN KEY (game_key) REFERENCES nhl_dw.fact_game(game_key),
    CONSTRAINT fact_skater_game_player_key_fkey FOREIGN KEY (player_key) REFERENCES nhl_dw.dim_player(player_key),
    CONSTRAINT fact_skater_game_team_key_fkey FOREIGN KEY (team_key) REFERENCES nhl_dw.dim_team(team_key)
);


-- nhl_dw.fact_team_game definition

-- Drop table

-- DROP TABLE nhl_dw.fact_team_game;

CREATE TABLE nhl_dw.fact_team_game (
    game_key int4 NOT NULL,
    team_key int4 NOT NULL,
    is_home bool NULL,
    goals int2 NULL,
    shots int2 NULL,
    hits int2 NULL,
    pim int2 NULL,
    powerplay_goals int2 NULL,
    powerplay_opps int2 NULL,
    faceoff_pct numeric(5, 2) NULL,
    created_at timestamptz DEFAULT now() NULL,
    updated_at timestamptz DEFAULT now() NULL,
    CONSTRAINT fact_team_game_pkey PRIMARY KEY (game_key, team_key),
    CONSTRAINT fact_team_game_game_key_fkey FOREIGN KEY (game_key) REFERENCES nhl_dw.fact_game(game_key),
    CONSTRAINT fact_team_game_team_key_fkey FOREIGN KEY (team_key) REFERENCES nhl_dw.dim_team(team_key)
);


-- nhl_dw.event_play definition

-- Drop table

-- DROP TABLE nhl_dw.event_play;

CREATE TABLE nhl_dw.event_play (
    event_key bigserial NOT NULL,
    game_key int4 NOT NULL,
    event_index int4 NULL,
    "period" int4 NULL,
    time_in_period text NULL,
    type_code text NULL,
    type_desc text NULL,
    x int4 NULL,
    y int4 NULL,
    shooter_id int4 NULL,
    goalie_id int4 NULL,
    team_id int4 NULL,
    raw_json jsonb NULL,
    created_at timestamptz DEFAULT now() NULL,
    CONSTRAINT event_play_pkey PRIMARY KEY (event_key),
    CONSTRAINT event_play_game_key_fkey FOREIGN KEY (game_key) REFERENCES nhl_dw.fact_game(game_key)
);