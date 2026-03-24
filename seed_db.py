"""
seed_db.py — Run once to populate managers, permanent players, and backups.
Also seeds mlb_roster entries for all permanent/backup players so position
labels (SP/RP/C/OF etc.) are correct immediately without waiting for a full sync.

Run:  python seed_db.py
"""

import sqlite3, os, unicodedata
from init_db import init

DB_PATH = os.environ.get('DB_PATH', 'fantasy.db')

# ── League configuration ───────────────────────────────────────────────────────

MANAGERS = ['Noah', 'Erik']

PERMANENT_PLAYERS = {
    'Noah': {
        'batters':  ['Bobby Witt Jr.', 'Juan Soto', 'Julio Rodríguez'],
        'pitchers': ['Paul Skenes', 'Garrett Crochet', 'Mason Miller'],
    },
    'Erik': {
        'batters':  ['Cal Raleigh', 'José Ramírez', 'Aaron Judge'],
        'pitchers': ['Tarik Skubal', 'Hunter Brown', 'Jhoan Duran'],
    },
}

BACKUP_PLAYERS = {
    'Noah': {
        'batters':  ['Vladimir Guerrero Jr.', 'Gunnar Henderson', 'Fernando Tatis Jr.'],
        'pitchers': ['Max Fried', 'Bryan Woo', 'Cade Smith'],
    },
    'Erik': {
        'batters':  ['Nick Kurtz', 'Junior Caminero', 'Roman Anthony'],
        'pitchers': ['Cristopher Sánchez', 'Jacob Misiorowski', 'Andrés Muñoz'],
    },
}

# Explicit player data: (mlb_id, canonical_name, team, position, position_type)
# position must be the normalised value: SP, RP, C, 1B, 2B, 3B, SS, OF, DH
PLAYER_DATA = {
    # ── Noah permanents ───────────────────────────────────────────────
    'Bobby Witt Jr.':       (677951, 'Bobby Witt Jr.',       'KC',  'SS', 'batter'),
    'Juan Soto':            (665742, 'Juan Soto',            'NYM', 'OF', 'batter'),
    'Julio Rodríguez':      (677594, 'Julio Rodríguez',      'SEA', 'OF', 'batter'),
    'Paul Skenes':          (694973, 'Paul Skenes',          'PIT', 'SP', 'pitcher'),
    'Garrett Crochet':      (676979, 'Garrett Crochet',      'BOS', 'SP', 'pitcher'),
    'Mason Miller':         (695243, 'Mason Miller',         'SD',  'RP', 'pitcher'),

    # ── Noah backups ──────────────────────────────────────────────────
    'Vladimir Guerrero Jr.':(665489, 'Vladimir Guerrero Jr.','TOR', '1B', 'batter'),
    'Gunnar Henderson':     (683002, 'Gunnar Henderson',     'BAL', 'SS', 'batter'),
    'Fernando Tatis Jr.':   (665487, 'Fernando Tatis Jr.',   'SD',  'OF', 'batter'),
    'Max Fried':            (608331, 'Max Fried',            'NYY', 'SP', 'pitcher'),
    'Bryan Woo':            (693433, 'Bryan Woo',            'SEA', 'SP', 'pitcher'),
    'Cade Smith':           (671922, 'Cade Smith',           'CLE', 'RP', 'pitcher'),

    # ── Erik permanents ───────────────────────────────────────────────
    'Cal Raleigh':          (663728, 'Cal Raleigh',          'SEA', 'C',  'batter'),
    'José Ramírez':         (608070, 'José Ramírez',         'CLE', '3B', 'batter'),
    'Aaron Judge':          (592450, 'Aaron Judge',          'NYY', 'OF', 'batter'),
    'Tarik Skubal':         (669373, 'Tarik Skubal',         'DET', 'SP', 'pitcher'),
    'Hunter Brown':         (686613, 'Hunter Brown',         'HOU', 'SP', 'pitcher'),
    'Jhoan Duran':          (661395, 'Jhoan Duran',          'PHI', 'RP', 'pitcher'),

    # ── Erik backups ──────────────────────────────────────────────────
    'Nick Kurtz':           (701762, 'Nick Kurtz',           'ATH', '1B', 'batter'),
    'Junior Caminero':      (691406, 'Junior Caminero',      'TB',  '3B', 'batter'),
    'Roman Anthony':        (701350, 'Roman Anthony',        'BOS', 'OF', 'batter'),
    'Cristopher Sánchez':   (650911, 'Cristopher Sánchez',  'PHI', 'SP', 'pitcher'),
    'Jacob Misiorowski':    (694819, 'Jacob Misiorowski',   'MIL', 'SP', 'pitcher'),
    'Andrés Muñoz':         (662253, 'Andrés Muñoz',        'SEA', 'RP', 'pitcher'),
}

# ── Seed logic ─────────────────────────────────────────────────────────────────

def strip_accents(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

def seed():
    init()  # create tables if not exist
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    today = __import__('datetime').date.today().isoformat()

    # Insert managers
    for name in MANAGERS:
        conn.execute('INSERT OR IGNORE INTO managers (name) VALUES (?)', (name,))
    conn.commit()

    # Upsert all player data into both `players` and `mlb_roster`
    for key, (mlb_id, canonical_name, team, position, pos_type) in PLAYER_DATA.items():
        name_ascii = strip_accents(canonical_name)

        # players table
        conn.execute('''
            INSERT INTO players (mlb_id, name, team, position_type)
            VALUES (?,?,?,?)
            ON CONFLICT(mlb_id) DO UPDATE SET
                name=excluded.name, team=excluded.team,
                position_type=excluded.position_type
        ''', (mlb_id, canonical_name, team, pos_type))

        # mlb_roster table — position_pinned=1 so nightly sync won't overwrite
        conn.execute('''
            INSERT INTO mlb_roster
                (mlb_id, name, name_ascii, team, team_full, position,
                 position_type, position_pinned, last_updated)
            VALUES (?,?,?,?,?,?,?,1,?)
            ON CONFLICT(mlb_id) DO UPDATE SET
                name=excluded.name, name_ascii=excluded.name_ascii,
                team=excluded.team, position=excluded.position,
                position_type=excluded.position_type,
                position_pinned=1, last_updated=excluded.last_updated
        ''', (mlb_id, canonical_name, name_ascii, team, team, position,
              pos_type, today))

    conn.commit()

    # Insert permanent / backup player associations
    for manager_name in MANAGERS:
        mgr = conn.execute('SELECT id FROM managers WHERE name=?',
                           (manager_name,)).fetchone()
        mid = mgr['id']

        for is_backup, source in [(0, PERMANENT_PLAYERS), (1, BACKUP_PLAYERS)]:
            data = source.get(manager_name, {})
            for pos_key, names in [('batters', 'batter'), ('pitchers', 'pitcher')]:
                for pname in data.get(pos_key, []):
                    # Find by name or accent-stripped name in PLAYER_DATA
                    lookup = pname
                    if pname not in PLAYER_DATA:
                        stripped = strip_accents(pname)
                        lookup = next(
                            (k for k in PLAYER_DATA if strip_accents(k) == stripped),
                            None
                        )
                    if not lookup or lookup not in PLAYER_DATA:
                        print(f"  WARNING: {pname} not in PLAYER_DATA — skipping")
                        continue

                    mlb_id = PLAYER_DATA[lookup][0]
                    player = conn.execute(
                        'SELECT id FROM players WHERE mlb_id=?', (mlb_id,)
                    ).fetchone()
                    if not player:
                        print(f"  WARNING: {pname} (id={mlb_id}) not in players table")
                        continue

                    conn.execute('''
                        INSERT OR IGNORE INTO permanent_players
                            (manager_id, player_id, is_backup, has_been_swapped)
                        VALUES (?,?,?,0)
                    ''', (mid, player['id'], is_backup))

        conn.commit()
        print(f"  Seeded {manager_name}")

    conn.close()
    print("Seed complete.")

if __name__ == '__main__':
    seed()
