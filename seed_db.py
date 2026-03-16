"""
seed_db.py — Run once to populate managers, permanent players, and backups.
Also seeds mlb_roster entries for all permanent/backup players so position
labels (SP/RP/C/OF etc.) are correct immediately without waiting for a full sync.

Run:  python seed_db.py
"""

import sqlite3, os
from init_db import init

DB_PATH = os.environ.get('DB_PATH', 'fantasy.db')

# ── League configuration ───────────────────────────────────────────────────────

MANAGERS = ['Noah', 'Erik']

PERMANENT_PLAYERS = {
    'Noah': {
        'batters':  ['Bryce Harper', 'Kyle Schwarber', 'Trea Turner'],
        'pitchers': ['Zack Wheeler', 'Cristopher Sanchez', 'Jhoan Duran'],
    },
    'Erik': {
        'batters':  ['Vladimir Guerrero Jr.', 'Bobby Witt Jr.', 'Jarren Duran'],
        'pitchers': ['Paul Skenes', 'Aaron Nola', 'Devin Williams'],
    },
}

BACKUP_PLAYERS = {
    'Noah': {
        'batters':  ['Julio Rodriguez', 'Kyle Tucker', 'Eugenio Suarez'],
        'pitchers': ['Tarik Skubal', 'Cole Ragans', 'Emmanuel Clase'],
    },
    'Erik': {
        'batters':  ['Aaron Judge', 'Alec Bohm', 'Gunnar Henderson'],
        'pitchers': ['Ranger Suarez', 'Yoshinobu Yamamoto', 'Mason Miller'],
    },
}

# Explicit player data: (mlb_id, canonical_name, team, position, position_type)
# position must be the normalised value: SP, RP, C, 1B, 2B, 3B, SS, OF, DH
PLAYER_DATA = {
    # Noah permanents
    'Bryce Harper':           (522975,  'Bryce Harper',           'PHI', '1B',  'batter'),
    'Kyle Schwarber':         (656941,  'Kyle Schwarber',         'PHI', 'DH',  'batter'),
    'Trea Turner':            (607208,  'Trea Turner',            'PHI', 'SS',  'batter'),
    'Zack Wheeler':           (554430,  'Zack Wheeler',           'PHI', 'SP',  'pitcher'),
    'Cristopher Sanchez':     (656945,  'Cristopher Sanchez',     'PHI', 'SP',  'pitcher'),
    'Jhoan Duran':            (661858,  'Jhoan Duran',            'PHI', 'RP',  'pitcher'),
    # Noah backups
    'Julio Rodriguez':        (677594,  'Julio Rodriguez',        'SEA', 'OF',  'batter'),
    'Kyle Tucker':            (663855,  'Kyle Tucker',            'CHC', 'OF',  'batter'),
    'Eugenio Suarez':         (553993,  'Eugenio Suarez',         'ARI', '3B',  'batter'),
    'Tarik Skubal':           (669373,  'Tarik Skubal',           'DET', 'SP',  'pitcher'),
    'Cole Ragans':            (669712,  'Cole Ragans',            'KC',  'SP',  'pitcher'),
    'Emmanuel Clase':         (667555,  'Emmanuel Clase',         'CLE', 'RP',  'pitcher'),
    # Erik permanents
    'Vladimir Guerrero Jr.':  (665489,  'Vladimir Guerrero Jr.',  'TOR', '1B',  'batter'),
    'Bobby Witt Jr.':         (677951,  'Bobby Witt Jr.',         'KC',  'SS',  'batter'),
    'Jarren Duran':           (680776,  'Jarren Duran',           'BOS', 'OF',  'batter'),
    'Paul Skenes':            (694973,  'Paul Skenes',            'PIT', 'SP',  'pitcher'),
    'Aaron Nola':             (605400,  'Aaron Nola',             'PHI', 'SP',  'pitcher'),
    'Devin Williams':         (669203,  'Devin Williams',         'NYY', 'RP',  'pitcher'),
    # Erik backups
    'Aaron Judge':            (592450,  'Aaron Judge',            'NYY', 'OF',  'batter'),
    'Alec Bohm':              (664353,  'Alec Bohm',              'PHI', '3B',  'batter'),
    'Gunnar Henderson':       (683002,  'Gunnar Henderson',       'BAL', 'SS',  'batter'),
    'Ranger Suarez':          (661482,  'Ranger Suarez',          'PHI', 'SP',  'pitcher'),
    'Yoshinobu Yamamoto':     (808982,  'Yoshinobu Yamamoto',     'LAD', 'SP',  'pitcher'),
    'Mason Miller':           (694984,  'Mason Miller',           'OAK', 'RP',  'pitcher'),
}

# ── Seed logic ─────────────────────────────────────────────────────────────────

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
        import unicodedata
        name_ascii = unicodedata.normalize('NFKD', canonical_name).encode('ASCII','ignore').decode('ASCII')

        # players table (used for lineup tracking)
        conn.execute('''
            INSERT INTO players (mlb_id, name, team, position_type)
            VALUES (?,?,?,?)
            ON CONFLICT(mlb_id) DO UPDATE SET
                name=excluded.name, team=excluded.team, position_type=excluded.position_type
        ''', (mlb_id, canonical_name, team, pos_type))

        # mlb_roster table (used for autocomplete + position labels)
        # position_pinned=1 prevents the nightly sync from overwriting these known-correct values
        conn.execute('''
            INSERT INTO mlb_roster (mlb_id, name, name_ascii, team, team_full, position, position_type, position_pinned, last_updated)
            VALUES (?,?,?,?,?,?,?,1,?)
            ON CONFLICT(mlb_id) DO UPDATE SET
                name=excluded.name, name_ascii=excluded.name_ascii,
                team=excluded.team, position=excluded.position,
                position_type=excluded.position_type,
                position_pinned=1,
                last_updated=excluded.last_updated
        ''', (mlb_id, canonical_name, name_ascii, team, team, position, pos_type, today))

    conn.commit()

    # Insert permanent / backup player associations
    for manager_name in MANAGERS:
        mgr = conn.execute('SELECT id FROM managers WHERE name=?', (manager_name,)).fetchone()
        mid = mgr['id']

        for is_backup, source in [(0, PERMANENT_PLAYERS), (1, BACKUP_PLAYERS)]:
            data = source.get(manager_name, {})
            for pos_type_key, names in [('batters', 'batter'), ('pitchers', 'pitcher')]:
                for pname in data.get(pos_type_key, []):
                    # Find the canonical name key (handles accent variants)
                    lookup_name = pname
                    if pname not in PLAYER_DATA:
                        # Try accent-stripped version
                        import unicodedata as _u
                        stripped = _u.normalize('NFKD', pname).encode('ASCII','ignore').decode('ASCII')
                        if stripped in PLAYER_DATA:
                            lookup_name = stripped

                    if lookup_name not in PLAYER_DATA:
                        print(f"  WARNING: {pname} not in PLAYER_DATA — skipping")
                        continue

                    mlb_id = PLAYER_DATA[lookup_name][0]
                    player = conn.execute('SELECT id FROM players WHERE mlb_id=?', (mlb_id,)).fetchone()
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
