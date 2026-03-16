"""
Run this once to create the SQLite schema.
Then use seed_db.py to populate managers, permanent players, etc.
"""
import sqlite3, os

DB_PATH = os.environ.get('DB_PATH', 'fantasy.db')

SCHEMA = '''
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS managers (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS players (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    mlb_id        INTEGER UNIQUE,
    name          TEXT NOT NULL,
    team          TEXT,
    position_type TEXT NOT NULL CHECK(position_type IN ('batter','pitcher'))
);

CREATE TABLE IF NOT EXISTS permanent_players (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    manager_id      INTEGER NOT NULL REFERENCES managers(id),
    player_id       INTEGER NOT NULL REFERENCES players(id),
    is_backup       INTEGER NOT NULL DEFAULT 0,
    has_been_swapped INTEGER NOT NULL DEFAULT 0,
    UNIQUE(manager_id, player_id, is_backup)
);

-- One row per roster slot per week per manager
CREATE TABLE IF NOT EXISTS lineups (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    manager_id   INTEGER NOT NULL REFERENCES managers(id),
    week         INTEGER NOT NULL,
    position     TEXT NOT NULL,   -- e.g. 'C','1B','SP1','RP2'
    player_id    INTEGER NOT NULL REFERENCES players(id),
    is_permanent INTEGER NOT NULL DEFAULT 0,
    UNIQUE(manager_id, week, position)
);

-- Cumulative weekly stats (updated every 15 min by stat_fetcher)
CREATE TABLE IF NOT EXISTS weekly_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    manager_id      INTEGER NOT NULL REFERENCES managers(id),
    week            INTEGER NOT NULL,
    player_id       INTEGER NOT NULL REFERENCES players(id),
    lineup_position TEXT,

    -- Batter counting stats
    singles     INTEGER DEFAULT 0,
    doubles     INTEGER DEFAULT 0,
    triples     INTEGER DEFAULT 0,
    homeruns    INTEGER DEFAULT 0,
    ab          INTEGER DEFAULT 0,
    total_bases INTEGER DEFAULT 0,
    slg         REAL    DEFAULT 0,
    rbi         INTEGER DEFAULT 0,
    bb          INTEGER DEFAULT 0,
    sb          INTEGER DEFAULT 0,
    k           INTEGER DEFAULT 0,

    -- Pitcher counting stats
    ip          REAL    DEFAULT 0,
    er          INTEGER DEFAULT 0,
    h           INTEGER DEFAULT 0,
    p_bb        INTEGER DEFAULT 0,
    h_plus_bb   INTEGER DEFAULT 0,
    sv          INTEGER DEFAULT 0,
    hd          INTEGER DEFAULT 0,
    bs          INTEGER DEFAULT 0,
    era         REAL    DEFAULT 0,
    whip        REAL    DEFAULT 0,
    so          INTEGER DEFAULT 0,
    qs          INTEGER DEFAULT 0,
    sv_hd_bs    INTEGER DEFAULT 0,

    UNIQUE(manager_id, week, player_id)
);

-- Today-only stats (wiped and rewritten each day)
CREATE TABLE IF NOT EXISTS today_stats (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    manager_id      INTEGER NOT NULL REFERENCES managers(id),
    player_id       INTEGER NOT NULL REFERENCES players(id),

    -- shared
    game_status     TEXT DEFAULT 'scheduled',  -- 'live','final','scheduled'
    opponent        TEXT,
    game_score      TEXT,   -- e.g. "PHI 3 - NYM 1"
    inning          TEXT,   -- e.g. "T7" or "Bot 3" or "Final"

    -- batter
    singles     INTEGER DEFAULT 0,
    doubles     INTEGER DEFAULT 0,
    triples     INTEGER DEFAULT 0,
    homeruns    INTEGER DEFAULT 0,
    ab          INTEGER DEFAULT 0,
    rbi         INTEGER DEFAULT 0,
    bb          INTEGER DEFAULT 0,
    sb          INTEGER DEFAULT 0,
    k           INTEGER DEFAULT 0,

    -- pitcher
    ip          REAL    DEFAULT 0,
    er          INTEGER DEFAULT 0,
    h           INTEGER DEFAULT 0,
    p_bb        INTEGER DEFAULT 0,
    sv          INTEGER DEFAULT 0,
    hd          INTEGER DEFAULT 0,
    bs          INTEGER DEFAULT 0,
    so          INTEGER DEFAULT 0,
    qs          INTEGER DEFAULT 0,

    UNIQUE(manager_id, player_id)
);

-- Category wins summary (updated after each stat refresh)
CREATE TABLE IF NOT EXISTS category_wins (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    manager    TEXT NOT NULL,
    week       INTEGER NOT NULL,
    wins       REAL NOT NULL DEFAULT 0,
    UNIQUE(manager, week)
);

-- Full MLB active roster for player search / autocomplete.
-- Synced once on startup and then weekly via sync_mlb_roster job.
CREATE TABLE IF NOT EXISTS mlb_roster (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    mlb_id       INTEGER UNIQUE NOT NULL,
    name         TEXT NOT NULL,
    name_ascii   TEXT NOT NULL,
    team         TEXT,
    team_full    TEXT,
    position     TEXT,                  -- normalised: SP, RP, C, 1B, 2B, 3B, SS, OF, DH
    position_type TEXT NOT NULL
        CHECK(position_type IN ('batter','pitcher')),
    position_pinned INTEGER DEFAULT 0,  -- 1 = manually seeded, sync won't overwrite position
    last_updated TEXT
);

CREATE INDEX IF NOT EXISTS idx_roster_name_ascii ON mlb_roster(name_ascii);
CREATE INDEX IF NOT EXISTS idx_roster_position_type ON mlb_roster(position_type);
CREATE INDEX IF NOT EXISTS idx_roster_position ON mlb_roster(position);
'''

def init():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print(f"Database initialised at {DB_PATH}")

if __name__ == '__main__':
    init()
