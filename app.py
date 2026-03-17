from flask import Flask, render_template, jsonify, request, abort
from apscheduler.schedulers.background import BackgroundScheduler
import sqlite3, os, json, unicodedata
from datetime import datetime, date, timedelta
from jobs.stat_fetcher import run_stat_update, run_today_update, strip_accents
from jobs.roster_sync import sync_mlb_roster
from week_schedule import current_week, week_dates, all_week_options, total_weeks, WEEKS

app = Flask(__name__)
DB_PATH = os.environ.get('DB_PATH', 'fantasy.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_season_weeks():
    db = get_db()
    rows = db.execute('SELECT DISTINCT week FROM weekly_stats ORDER BY week').fetchall()
    db.close()
    return [r['week'] for r in rows]

# Canonical lineup slot order — used for sorting on all pages
SLOT_ORDER = [
    'C-0',
    '1B-0',
    '2B-0',
    '3B-0',
    'SS-0',
    'OF-0', 'OF-1', 'OF-2',
    'DH-0',
    'SP-0', 'SP-1', 'SP-2', 'SP-3', 'SP-4',
    'RP-0', 'RP-1', 'RP-2',
]
_SLOT_RANK = {slot: i for i, slot in enumerate(SLOT_ORDER)}

def slot_display(slot_key):
    """'C-0' → 'C',  'OF-1' → 'OF',  'SP-2' → 'SP'"""
    return slot_key.split('-')[0] if slot_key and '-' in slot_key else (slot_key or '')

def sort_by_slot(rows, key='lineup_position'):
    """Sort a list of row-dicts by canonical slot order."""
    return sorted(rows, key=lambda r: _SLOT_RANK.get(
        r[key] if isinstance(r, dict) else getattr(r, key, ''), 999))

# ── Canonical permanent-player slot assignment ─────────────────────────────────
# Maps a player's primary roster position to a lineup slot key.
# Pitchers: SPs go SP-0..SP-1, RPs go RP-0
# Batters: mapped by their defensive position
_POSITION_TO_SLOT = {
    'C':   'C-0',
    '1B':  '1B-0',
    '2B':  '2B-0',
    '3B':  '3B-0',
    'SS':  'SS-0',
    'OF':  'OF-0',   # first OF slot; additional OFs get OF-1, OF-2 sequentially
    'LF':  'OF-0',
    'CF':  'OF-0',
    'RF':  'OF-0',
    'DH':  'DH-0',
    'IF':  '1B-0',   # utility infielder — default to 1B
    'UT':  'DH-0',   # utility — default to DH
    'SP':  'SP-0',   # first SP slot; additional SPs get SP-1
    'RP':  'RP-0',
    'CP':  'RP-0',
    'P':   'SP-0',   # generic pitcher — assume SP
}

def _auto_populate_permanents(db, managers, week_num):
    """
    For each manager, insert their active permanent players into lineup slots
    for the given week if those slots aren't already filled.
    Uses canonical position → slot mapping and avoids double-booking a slot.
    """
    for m in managers:
        # Get active permanent players (not swapped out).
        # Position comes from mlb_roster (already normalised to SP/RP/C/OF/etc.)
        # Fallback: pitchers default to 'RP' (safer — can always move to SP slot),
        # batters default to 'OF'.
        perm_rows = db.execute('''
            SELECT
                p.id as player_id, p.name, p.position_type,
                COALESCE(
                    (SELECT mr2.position FROM mlb_roster mr2
                     WHERE mr2.mlb_id = p.mlb_id LIMIT 1),
                    CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END
                ) as position
            FROM permanent_players pp
            JOIN players p ON pp.player_id = p.id
            WHERE pp.manager_id = ? AND pp.is_backup = 0 AND pp.has_been_swapped = 0
            GROUP BY p.id
        ''', (m['id'],)).fetchall()

        # Get already-filled slots for this manager/week
        filled = db.execute(
            'SELECT position FROM lineups WHERE manager_id = ? AND week = ?',
            (m['id'], week_num)
        ).fetchall()
        used_slots = {r['position'] for r in filled}

        sp_idx = 0
        of_idx = 0

        for row in perm_rows:
            pos      = (row['position'] or '').upper()
            pos_type = row['position_type']

            # Determine target slot
            if pos_type == 'pitcher':
                if pos == 'RP':
                    slot = 'RP-0'
                    # If RP-0 taken, try RP-1, RP-2
                    for rp_i in range(3):
                        candidate = f'RP-{rp_i}'
                        if candidate not in used_slots:
                            slot = candidate
                            break
                else:  # SP or unknown pitcher
                    slot = f'SP-{sp_idx}'
                    sp_idx += 1
            else:
                if pos in ('OF', 'LF', 'CF', 'RF'):
                    slot = f'OF-{of_idx}'
                    of_idx += 1
                else:
                    slot = _POSITION_TO_SLOT.get(pos, 'DH-0')

            if slot in used_slots:
                continue

            # Don't re-insert if already present in any slot
            existing = db.execute('''
                SELECT id FROM lineups
                WHERE manager_id = ? AND week = ? AND player_id = ?
            ''', (m['id'], week_num, row['player_id'])).fetchone()
            if existing:
                continue

            used_slots.add(slot)
            db.execute('''
                INSERT INTO lineups (manager_id, week, position, player_id, is_permanent)
                VALUES (?, ?, ?, ?, 1)
                ON CONFLICT(manager_id, week, position) DO NOTHING
            ''', (m['id'], week_num, slot, row['player_id']))

    db.commit()

# ── routes ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    db = get_db()
    weeks = get_season_weeks()
    week_num = current_week()

    # Total category wins
    totals = db.execute('''
        SELECT manager, SUM(wins) as total FROM category_wins GROUP BY manager
    ''').fetchall()
    totals = {r['manager']: r['total'] for r in totals}

    # Per-week category wins
    week_wins = db.execute('SELECT week, manager, wins FROM category_wins ORDER BY week').fetchall()
    by_week = {}
    for r in week_wins:
        by_week.setdefault(r['week'], {})[r['manager']] = r['wins']

    managers = db.execute('SELECT name FROM managers ORDER BY id').fetchall()
    manager_names = [m['name'] for m in managers]

    # Weeks won: count completed weeks only (skip the current in-progress week)
    weeks_won = {name: 0 for name in manager_names}
    for week, wdata in by_week.items():
        if week == week_num:
            continue   # week still in progress
        if len(wdata) == 2:
            scores = list(wdata.items())
            if scores[0][1] != scores[1][1]:  # not a tied week
                winner = max(scores, key=lambda x: x[1])[0]
                weeks_won[winner] = weeks_won.get(winner, 0) + 1

    # Current week category results (summary for home page)
    cur_week_data = {}
    for m in db.execute('SELECT * FROM managers ORDER BY id').fetchall():
        batters = db.execute('''
            SELECT ws.* FROM weekly_stats ws JOIN players p ON ws.player_id=p.id
            WHERE ws.manager_id=? AND ws.week=? AND p.position_type='batter'
        ''', (m['id'], week_num)).fetchall()
        pitchers = db.execute('''
            SELECT ws.* FROM weekly_stats ws JOIN players p ON ws.player_id=p.id
            WHERE ws.manager_id=? AND ws.week=? AND p.position_type='pitcher'
        ''', (m['id'], week_num)).fetchall()
        cur_week_data[m['name']] = {'batters': batters, 'pitchers': pitchers}

    cur_week_cats = _compute_category_winners(cur_week_data)

    db.close()
    return render_template('index.html',
        weeks=weeks,
        week_num=week_num,
        totals=totals,
        by_week=by_week,
        manager_names=manager_names,
        cur_week_cats=cur_week_cats,
        weeks_won=weeks_won,
        week_options=all_week_options(),
    )

@app.route('/week/<int:n>')
def week_view(n):
    db = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()

    # Ensure permanent players are seeded into the lineup for this week
    _auto_populate_permanents(db, managers, n)
    data = {}
    for m in managers:
        # Get all players in the lineup for this week
        lineup_rows = db.execute('''
            SELECT l.position, p.id as player_id, p.name, p.team, p.position_type
            FROM lineups l JOIN players p ON l.player_id = p.id
            WHERE l.manager_id = ? AND l.week = ?
        ''', (m['id'], n)).fetchall()

        print(f"[week_view] week={n}, manager={m['name']}, lineup_rows={len(lineup_rows)}")
        for lr in lineup_rows:
            print(f"  {lr['position']} {lr['name']}")

        # Get stats that have been fetched so far (keyed by player_id)
        stats_rows = db.execute('''
            SELECT ws.player_id, p.name, p.team, ws.*
            FROM weekly_stats ws JOIN players p ON ws.player_id = p.id
            WHERE ws.manager_id = ? AND ws.week = ?
        ''', (m['id'], n)).fetchall()
        stats_by_pid = {r['player_id']: dict(r) for r in stats_rows}

        batters  = []
        pitchers = []
        for lr in lineup_rows:
            pid = lr['player_id']
            if pid in stats_by_pid:
                # Use real stats, but ensure lineup_position from lineups table
                row = dict(stats_by_pid[pid])
                row['lineup_position'] = lr['position']
                row['team'] = row['team'] or lr['team']
            else:
                # Player in lineup but no stats yet — show zeros
                if lr['position_type'] == 'batter':
                    row = _empty_batter_row({'position': lr['position'],
                                             'name': lr['name'], 'team': lr['team']})
                else:
                    row = _empty_pitcher_row({'position': lr['position'],
                                              'name': lr['name'], 'team': lr['team']})

            if lr['position_type'] == 'batter':
                batters.append(row)
            else:
                pitchers.append(row)

        data[m['name']] = {
            'batters':  sort_by_slot(batters),
            'pitchers': sort_by_slot(pitchers),
        }

    cats = _compute_category_winners(data)
    max_week = WEEKS[-1][0]
    db.close()
    return render_template('week.html',
        n=n, data=data, cats=cats,
        manager_names=[m['name'] for m in managers],
        week_options=all_week_options(),
        max_week=max_week,
        slot_display=slot_display,
    )

def _empty_batter_row(r):
    """Return a dict mimicking a weekly_stats row with zero batting stats."""
    return {
        'name': r['name'], 'team': r['team'],
        'lineup_position': r['position'],
        'singles': 0, 'doubles': 0, 'triples': 0, 'homeruns': 0,
        'ab': 0, 'total_bases': 0, 'slg': 0,
        'rbi': 0, 'bb': 0, 'sb': 0, 'k': 0,
    }

def _empty_pitcher_row(r):
    """Return a dict mimicking a weekly_stats row with zero pitching stats."""
    return {
        'name': r['name'], 'team': r['team'],
        'lineup_position': r['position'],
        'ip': 0, 'er': 0, 'h': 0, 'p_bb': 0, 'h_plus_bb': 0,
        'sv': 0, 'hd': 0, 'bs': 0,
        'era': 0, 'whip': 0, 'so': 0, 'qs': 0, 'sv_hd_bs': 0,
    }

@app.route('/lineups')
def lineups_view():
    return lineups_week_view(current_week())

@app.route('/lineups/<int:week_num>')
def lineups_week_view(week_num):
    db = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()

    # Auto-populate permanent players into lineup slots for this week
    _auto_populate_permanents(db, managers, week_num)

    # Used TEMPORARY players per manager in all weeks OTHER than this one
    used = {}
    for m in managers:
        rows = db.execute('''
            SELECT DISTINCT p.name, p.position_type,
                   COALESCE(
                       (SELECT mr2.position FROM mlb_roster mr2
                        WHERE mr2.mlb_id = p.mlb_id LIMIT 1),
                       p.position_type
                   ) as position
            FROM lineups l
            JOIN players p ON l.player_id = p.id
            WHERE l.manager_id = ? AND l.is_permanent = 0 AND l.week != ?
        ''', (m['id'], week_num)).fetchall()
        used[m['name']] = [dict(r) for r in rows]

    # All non-permanent players used this week (for conflict detection)
    used_this_week = {}
    for m in managers:
        rows = db.execute('''
            SELECT DISTINCT p.name FROM lineups l
            JOIN players p ON l.player_id = p.id
            WHERE l.week = ? AND l.is_permanent = 0
        ''', (week_num,)).fetchall()
        used_this_week[m['name']] = [r['name'] for r in rows]

    # Current week lineups keyed by slot position
    lineups = {}
    for m in managers:
        rows = db.execute('''
            SELECT l.position, p.name, l.week, l.is_permanent
            FROM lineups l JOIN players p ON l.player_id = p.id
            WHERE l.manager_id = ? AND l.week = ?
            ORDER BY l.position
        ''', (m['id'], week_num)).fetchall()
        lineups[m['name']] = {r['position']: dict(r) for r in rows}

    # Permanent players — one row per player per manager, no duplicates
    perms = {}
    for m in managers:
        rows = db.execute('''
            SELECT
                p.name,
                p.mlb_id,
                p.position_type,
                COALESCE(
                    (SELECT mr2.position FROM mlb_roster mr2
                     WHERE mr2.mlb_id = p.mlb_id LIMIT 1),
                    CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END
                ) as position,
                pp.is_backup,
                MIN(pp.has_been_swapped) as has_been_swapped
            FROM permanent_players pp
            JOIN players p ON pp.player_id = p.id
            WHERE pp.manager_id = ?
            GROUP BY p.name, p.position_type, pp.is_backup
            ORDER BY pp.is_backup, p.position_type
        ''', (m['id'],)).fetchall()
        perms[m['name']] = [dict(r) for r in rows]

    db.close()
    return render_template('lineups.html',
        week_num=week_num,
        save_week=week_num,
        managers=[m['name'] for m in managers],
        used=used,
        used_this_week=used_this_week,
        lineups=lineups,
        perms=perms,
        week_options=all_week_options(),
    )

@app.route('/roster')
def roster_view():
    db = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()
    roster_data = {}
    for m in managers:
        rows = db.execute('''
            SELECT
                p.name,
                p.team,
                p.position_type,
                COALESCE(
                    (SELECT mr2.position FROM mlb_roster mr2
                     WHERE mr2.mlb_id = p.mlb_id LIMIT 1),
                    CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END
                ) as position,
                pp.is_backup,
                MIN(pp.has_been_swapped) as has_been_swapped
            FROM permanent_players pp
            JOIN players p ON pp.player_id = p.id
            WHERE pp.manager_id = ?
            GROUP BY p.name, p.position_type, pp.is_backup
            ORDER BY pp.is_backup, p.position_type
        ''', (m['id'],)).fetchall()
        roster_data[m['name']] = [dict(r) for r in rows]
    db.close()
    return render_template('roster.html', roster_data=roster_data)

# ── API ────────────────────────────────────────────────────────────────────────

@app.route('/api/week/<int:n>')
def api_week(n):
    db = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()
    result = {}
    for m in managers:
        batters = [dict(r) for r in db.execute('''
            SELECT p.name, p.team, ws.*
            FROM weekly_stats ws JOIN players p ON ws.player_id = p.id
            WHERE ws.manager_id=? AND ws.week=? AND p.position_type='batter'
            ORDER BY ws.lineup_position
        ''', (m['id'], n))]
        pitchers = [dict(r) for r in db.execute('''
            SELECT p.name, p.team, ws.*
            FROM weekly_stats ws JOIN players p ON ws.player_id = p.id
            WHERE ws.manager_id=? AND ws.week=? AND p.position_type='pitcher'
            ORDER BY ws.lineup_position
        ''', (m['id'], n))]
        result[m['name']] = {'batters': batters, 'pitchers': pitchers}
    db.close()
    return jsonify(result)

@app.route('/api/today')
def api_today():
    db = get_db()
    week_num = current_week()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()
    result = {}
    slot_rank = _SLOT_RANK
    for m in managers:
        rows = db.execute('''
            SELECT p.name, p.team, p.position_type, l.position as lineup_position, ts.*
            FROM today_stats ts
            JOIN players p ON ts.player_id = p.id
            LEFT JOIN lineups l ON l.player_id = ts.player_id
                AND l.manager_id = ts.manager_id AND l.week = ?
            WHERE ts.manager_id = ?
        ''', (week_num, m['id'])).fetchall()
        sorted_rows = sorted(
            [dict(r) for r in rows],
            key=lambda r: slot_rank.get(r.get('lineup_position') or '', 999)
        )
        result[m['name']] = sorted_rows
    db.close()
    return jsonify(result)

@app.route('/api/validate_player', methods=['POST'])
def validate_player():
    data = request.json
    player_name  = data.get('player_name', '').strip()
    manager_name = data.get('manager', '').strip()
    week         = data.get('week', current_week())
    position     = data.get('position', '')   # current slot being filled

    db = get_db()
    manager = db.execute('SELECT id FROM managers WHERE name = ?', (manager_name,)).fetchone()
    if not manager:
        db.close()
        return jsonify({'valid': False, 'reason': 'Unknown manager'})

    # Check already used by this manager in a previous week (temp only)
    used = db.execute('''
        SELECT l.week FROM lineups l JOIN players p ON l.player_id = p.id
        WHERE l.manager_id = ? AND p.name = ? AND l.is_permanent = 0 AND l.week != ?
    ''', (manager['id'], player_name, week)).fetchone()
    if used:
        db.close()
        return jsonify({'valid': False, 'reason': f'Already used in Week {used["week"]}'})

    # Check already in THIS manager's lineup THIS week in a DIFFERENT slot
    dupe = db.execute('''
        SELECT l.position FROM lineups l JOIN players p ON l.player_id = p.id
        WHERE l.manager_id = ? AND p.name = ? AND l.week = ? AND l.position != ?
    ''', (manager['id'], player_name, week, position)).fetchone()
    if dupe:
        db.close()
        return jsonify({'valid': False, 'reason': f'Already in your lineup ({dupe["position"]})'})

    # Check used by other manager this week (temp only)
    other = db.execute('''
        SELECT m.name FROM lineups l
        JOIN players p ON l.player_id = p.id
        JOIN managers m ON l.manager_id = m.id
        WHERE m.name != ? AND p.name = ? AND l.week = ? AND l.is_permanent = 0
    ''', (manager_name, player_name, week)).fetchone()
    if other:
        db.close()
        return jsonify({'valid': False, 'reason': f'Already used by {other["name"]} this week'})

    db.close()
    return jsonify({'valid': True})

@app.route('/api/remove_lineup', methods=['POST'])
def remove_lineup():
    data         = request.json
    manager_name = data.get('manager', '').strip()
    week         = data.get('week', current_week())
    position     = data.get('position', '').strip()

    db = get_db()
    manager = db.execute('SELECT id FROM managers WHERE name=?', (manager_name,)).fetchone()
    if not manager:
        db.close()
        return jsonify({'success': False, 'reason': 'Unknown manager'})

    db.execute('''
        DELETE FROM lineups WHERE manager_id=? AND week=? AND position=?
    ''', (manager['id'], week, position))
    db.commit()
    db.close()
    return jsonify({'success': True})

@app.route('/api/set_lineup', methods=['POST'])
def set_lineup():
    data         = request.json
    manager_name = data.get('manager')
    week         = data.get('week', current_week())
    position     = data.get('position')
    player_name  = data.get('player_name', '').strip()
    mlb_id       = data.get('mlb_id', '')
    is_permanent = int(data.get('is_permanent', 0))

    print(f"[set_lineup] manager={manager_name} week={week} position={position} player={player_name} is_perm={is_permanent}")

    db = get_db()
    manager = db.execute('SELECT id FROM managers WHERE name = ?', (manager_name,)).fetchone()
    if not manager:
        db.close()
        return jsonify({'success': False, 'reason': 'Unknown manager'})

    player = None

    # Prefer mlb_id lookup — unambiguous even for accent variants
    if mlb_id:
        try:
            mid = int(mlb_id)
            player = db.execute('SELECT id FROM players WHERE mlb_id = ?', (mid,)).fetchone()
            if not player:
                # Auto-create from mlb_roster using mlb_id
                roster_row = db.execute(
                    'SELECT * FROM mlb_roster WHERE mlb_id = ?', (mid,)
                ).fetchone()
                if roster_row:
                    db.execute('''
                        INSERT OR IGNORE INTO players (mlb_id, name, team, position_type)
                        VALUES (?,?,?,?)
                    ''', (roster_row['mlb_id'], roster_row['name'],
                          roster_row['team'], roster_row['position_type']))
                    db.commit()
                    player = db.execute('SELECT id FROM players WHERE mlb_id = ?', (mid,)).fetchone()
        except (ValueError, TypeError):
            pass

    # Fallback: exact name match in players table
    if not player:
        player = db.execute('SELECT id FROM players WHERE name = ?', (player_name,)).fetchone()

    # Fallback: exact name match in mlb_roster → auto-create player
    if not player:
        roster_row = db.execute(
            'SELECT * FROM mlb_roster WHERE name = ?', (player_name,)
        ).fetchone()
        if roster_row:
            db.execute('''
                INSERT OR IGNORE INTO players (mlb_id, name, team, position_type)
                VALUES (?,?,?,?)
            ''', (roster_row['mlb_id'], roster_row['name'],
                  roster_row['team'], roster_row['position_type']))
            db.commit()
            player = db.execute('SELECT id FROM players WHERE mlb_id = ?',
                                 (roster_row['mlb_id'],)).fetchone()

    if not player:
        db.close()
        return jsonify({'success': False, 'reason': f'Player "{player_name}" not found'})

    db.execute('''
        INSERT INTO lineups (manager_id, week, position, player_id, is_permanent)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(manager_id, week, position) DO UPDATE SET
            player_id=excluded.player_id,
            is_permanent=MAX(is_permanent, excluded.is_permanent)
    ''', (manager['id'], week, position, player['id'], is_permanent))
    db.commit()
    db.close()
    return jsonify({'success': True})

@app.route('/api/swap_permanent', methods=['POST'])
def swap_permanent():
    data         = request.json
    manager_name = data.get('manager')
    perm_name    = data.get('permanent_player')
    backup_name  = data.get('backup_player')
    swap_type    = data.get('swap_type', 'permanent')
    week         = data.get('week', current_week())

    db = get_db()
    manager = db.execute('SELECT id FROM managers WHERE name=?', (manager_name,)).fetchone()
    if not manager:
        db.close()
        return jsonify({'success': False, 'reason': 'Unknown manager'})

    perm_player   = db.execute('SELECT id FROM players WHERE name=?', (perm_name,)).fetchone()
    backup_player = db.execute('SELECT id FROM players WHERE name=?', (backup_name,)).fetchone()

    if not perm_player or not backup_player:
        db.close()
        return jsonify({'success': False, 'reason': 'Player not found'})

    if swap_type == 'permanent':
        # Mark old permanent as swapped out
        db.execute('''
            UPDATE permanent_players SET has_been_swapped=1
            WHERE manager_id=? AND is_backup=0 AND player_id=?
        ''', (manager['id'], perm_player['id']))
        # Activate backup as permanent
        db.execute('''
            UPDATE permanent_players SET has_been_swapped=1
            WHERE manager_id=? AND is_backup=1 AND player_id=?
        ''', (manager['id'], backup_player['id']))

        # Look up the backup player's actual position from mlb_roster
        backup_info = db.execute('''
            SELECT p.position_type,
                   COALESCE(
                       (SELECT mr2.position FROM mlb_roster mr2
                        WHERE mr2.mlb_id = p.mlb_id LIMIT 1),
                       CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END
                   ) as position
            FROM players p WHERE p.id=?
        ''', (backup_player['id'],)).fetchone()

        backup_pos      = (backup_info['position']      if backup_info else 'OF').upper()
        backup_pos_type = (backup_info['position_type'] if backup_info else 'batter')

        # Determine which canonical slot the backup should occupy.
        # For pitchers: SP→SP-0/1/2..., RP→RP-0/1/2...
        # For batters: use _POSITION_TO_SLOT mapping.
        # We reuse the same logic as _auto_populate_permanents.
        def _slot_for(pos, pos_type, used_slots):
            if pos_type == 'pitcher':
                candidates = [f'SP-{i}' for i in range(5)] if pos == 'SP' \
                             else [f'RP-{i}' for i in range(3)]
            elif pos in ('OF', 'LF', 'CF', 'RF'):
                candidates = [f'OF-{i}' for i in range(3)]
            else:
                base = _POSITION_TO_SLOT.get(pos, 'DH-0')
                candidates = [base]
            for c in candidates:
                if c not in used_slots:
                    return c
            return candidates[0]  # fall back to first if all taken

        # Find all weeks where the old permanent player has a lineup row
        old_rows = db.execute('''
            SELECT week, position FROM lineups
            WHERE manager_id=? AND player_id=? AND is_permanent=1
        ''', (manager['id'], perm_player['id'])).fetchall()

        for row in old_rows:
            wk       = row['week']
            old_slot = row['position']

            # Get other occupied slots for this manager/week (excluding the old slot)
            occupied = db.execute('''
                SELECT position FROM lineups
                WHERE manager_id=? AND week=? AND position != ?
            ''', (manager['id'], wk, old_slot)).fetchall()
            used_slots = {r['position'] for r in occupied}

            new_slot = _slot_for(backup_pos, backup_pos_type, used_slots)

            # Remove the old slot and insert at the new correct slot
            db.execute('''
                DELETE FROM lineups WHERE manager_id=? AND week=? AND position=?
            ''', (manager['id'], wk, old_slot))
            db.execute('''
                INSERT INTO lineups (manager_id, week, position, player_id, is_permanent)
                VALUES (?,?,?,?,1)
                ON CONFLICT(manager_id, week, position) DO UPDATE SET
                    player_id=excluded.player_id, is_permanent=1
            ''', (manager['id'], wk, new_slot, backup_player['id']))

    else:
        # Temporary: replace just this week's lineup slot
        slot = db.execute('''
            SELECT l.position FROM lineups l
            WHERE l.manager_id=? AND l.week=? AND l.player_id=? AND l.is_permanent=1
        ''', (manager['id'], week, perm_player['id'])).fetchone()

        if slot:
            db.execute('''
                UPDATE lineups SET player_id=?, is_permanent=1
                WHERE manager_id=? AND week=? AND position=?
            ''', (backup_player['id'], manager['id'], week, slot['position']))

    db.commit()
    db.close()
    return jsonify({'success': True, 'swap_type': swap_type})

@app.route('/api/roster_search')
def api_roster_search():
    q             = request.args.get('q', '').strip()
    position_type = request.args.get('position_type', '')  # 'batter' or 'pitcher'
    manager_name  = request.args.get('manager', '')
    week          = request.args.get('week', current_week(), type=int)
    limit         = min(int(request.args.get('limit', 15)), 50)

    if len(q) < 2:
        return jsonify([])

    q_ascii = strip_accents(q).lower()
    like    = f'%{q_ascii}%'

    db = get_db()

    # Build conflict sets
    used_by_this_manager     = set()
    used_this_week_by_other  = set()
    already_in_lineup        = set()
    if manager_name:
        mgr = db.execute('SELECT id FROM managers WHERE name=?', (manager_name,)).fetchone()
        if mgr:
            rows = db.execute('''
                SELECT p.name FROM lineups l JOIN players p ON l.player_id=p.id
                WHERE l.manager_id=? AND l.is_permanent=0 AND l.week!=?
            ''', (mgr['id'], week)).fetchall()
            used_by_this_manager = {r['name'] for r in rows}

            rows = db.execute('''
                SELECT p.name FROM lineups l
                JOIN players p ON l.player_id=p.id
                JOIN managers m ON l.manager_id=m.id
                WHERE m.name!=? AND l.week=? AND l.is_permanent=0
            ''', (manager_name, week)).fetchall()
            used_this_week_by_other = {r['name'] for r in rows}

            rows = db.execute('''
                SELECT p.name FROM lineups l JOIN players p ON l.player_id=p.id
                WHERE l.manager_id=? AND l.week=?
            ''', (mgr['id'], week)).fetchall()
            already_in_lineup = {r['name'] for r in rows}

    # For pitchers, normalise position into SP or RP for display,
    # but don't filter — let any pitcher appear in any pitching slot.
    # For batters, no position filtering at all.
    sql = '''
        SELECT mlb_id, name, team, position, position_type
        FROM mlb_roster
        WHERE (LOWER(name_ascii) LIKE ? OR LOWER(name) LIKE ?)
    '''
    params = [like, f'%{q.lower()}%']

    if position_type:
        sql += ' AND position_type = ?'
        params.append(position_type)

    sql += ' ORDER BY name_ascii LIMIT ?'
    params.append(limit)

    rows = db.execute(sql, params).fetchall()
    db.close()

    results = []
    for r in rows:
        entry = {
            'mlb_id':        r['mlb_id'],
            'name':          r['name'],
            'team':          r['team'],
            'position':      r['position'],
            'position_type': r['position_type'],
            'conflict':      None,
        }
        if r['name'] in already_in_lineup:
            entry['conflict'] = 'in_lineup'    # already in this lineup this week
        elif r['name'] in used_by_this_manager:
            entry['conflict'] = 'used'          # used in a prior week
        elif r['name'] in used_this_week_by_other:
            entry['conflict'] = 'other_week'    # taken by other manager this week
        results.append(entry)

    return jsonify(results)

@app.route('/api/roster_sync', methods=['POST'])
def api_roster_sync():
    sync_mlb_roster()
    return jsonify({'success': True})

@app.route('/api/stat_update', methods=['POST'])
def api_stat_update():
    """Manually trigger a stat update. Runs in background thread to avoid timeout."""
    import threading
    threading.Thread(target=run_stat_update, daemon=True).start()
    return jsonify({'success': True, 'message': 'Stat update started'})

@app.route('/api/roster_last_updated')
def api_roster_last_updated():
    db = get_db()
    row = db.execute('SELECT MAX(last_updated) as lu FROM mlb_roster').fetchone()
    db.close()
    return jsonify({'last_updated': row['lu'] if row else None})

@app.route('/api/permanent_players/<manager_name>')
def api_permanent_players(manager_name):
    """Return active permanent players for a manager."""
    db = get_db()
    mgr = db.execute('SELECT id FROM managers WHERE name=?', (manager_name,)).fetchone()
    if not mgr:
        db.close()
        return jsonify([])
    rows = db.execute('''
        SELECT p.name, p.position_type,
               COALESCE(mr.position, p.position_type) as position,
               pp.is_backup, pp.has_been_swapped
        FROM permanent_players pp
        JOIN players p ON pp.player_id = p.id
        LEFT JOIN mlb_roster mr ON mr.mlb_id = p.mlb_id
        WHERE pp.manager_id = ?
        ORDER BY pp.is_backup, p.position_type
    ''', (mgr['id'],)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

# ── Category math ──────────────────────────────────────────────────────────────

def _ip_display_to_true(ip_display):
    """Convert display IP (6.2 = 6⅔ innings) to true decimal (6.667)."""
    try:
        val   = float(ip_display)
        whole = int(val)
        thirds = round((val - whole) * 10)
        return whole + thirds / 3.0
    except (TypeError, ValueError):
        return 0.0

def _compute_category_winners(data):
    managers = list(data.keys())
    if len(managers) < 2:
        return {}

    def val(r, field):
        """Access a field from either a sqlite3.Row or a plain dict."""
        try:
            return r[field] or 0
        except (KeyError, IndexError, TypeError):
            return 0

    def total(rows, field):
        return sum(val(r, field) for r in rows)

    def wavg(rows, num, den):
        n = sum(val(r, num) for r in rows)
        d = sum(_ip_display_to_true(val(r, den)) for r in rows)
        return n / d if d else 0

    def wavg_plain(rows, num, den):
        n = sum(val(r, num) for r in rows)
        d = sum(val(r, den) for r in rows)
        return n / d if d else 0

    m1, m2 = managers[0], managers[1]
    results = {}

    hitting = [
        ('SLG', lambda m: wavg_plain(data[m]['batters'], 'total_bases', 'ab'), False),
        ('RBI', lambda m: total(data[m]['batters'], 'rbi'),                    False),
        ('BB',  lambda m: total(data[m]['batters'], 'bb'),                     False),
        ('SB',  lambda m: total(data[m]['batters'], 'sb'),                     False),
        ('K',   lambda m: total(data[m]['batters'], 'k'),                      True),
    ]
    pitching = [
        ('ERA',      lambda m: wavg(data[m]['pitchers'], 'er', 'ip') * 9,      True),
        ('WHIP',     lambda m: wavg(data[m]['pitchers'], 'h_plus_bb', 'ip'),   True),
        ('SO',       lambda m: total(data[m]['pitchers'], 'so'),                False),
        ('QS',       lambda m: total(data[m]['pitchers'], 'qs'),                False),
        ('SV+HD-BS', lambda m: total(data[m]['pitchers'], 'sv_hd_bs'),         False),
    ]
    for cat, fn, lower in hitting + pitching:
        v1, v2 = fn(m1), fn(m2)
        if v1 == v2:
            winner = 'Tie'
            pts = {m1: 0.5, m2: 0.5}
        elif (lower and v1 < v2) or (not lower and v1 > v2):
            winner = m1
            pts = {m1: 1, m2: 0}
        else:
            winner = m2
            pts = {m1: 0, m2: 1}
        results[cat] = {
            'winner': winner,
            m1: round(v1, 3),
            m2: round(v2, 3),
            'pts': pts,
        }

    return results

# ── Scheduler ──────────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()
scheduler.add_job(run_stat_update,  'interval', minutes=15, id='stat_update')
scheduler.add_job(run_today_update, 'interval', minutes=1,  id='today_update')
scheduler.add_job(sync_mlb_roster,  'interval', hours=24,   id='roster_sync')
scheduler.start()

with app.app_context():
    try:
        db = get_db()

        # Migration: add position_pinned column if this is an existing DB without it
        try:
            db.execute('ALTER TABLE mlb_roster ADD COLUMN position_pinned INTEGER DEFAULT 0')
            db.commit()
            print("[app] Migrated mlb_roster: added position_pinned column")
        except Exception:
            pass  # column already exists — that's fine

        # Migration: remove duplicate permanent_players rows (keep lowest id per group)
        db.execute('''
            DELETE FROM permanent_players
            WHERE id NOT IN (
                SELECT MIN(id) FROM permanent_players
                GROUP BY manager_id, player_id, is_backup
            )
        ''')
        db.commit()

        # Normalise any stale position values for non-pinned rows
        db.execute("UPDATE mlb_roster SET position='SP' WHERE position_type='pitcher' AND position='P' AND position_pinned=0")
        db.execute("UPDATE mlb_roster SET position='RP' WHERE position_type='pitcher' AND position IN ('CP','RL','CL','MR','SU','SW','RS') AND position_pinned=0")
        db.commit()

        count = db.execute('SELECT COUNT(*) as c FROM mlb_roster').fetchone()['c']
        db.close()

        # Always sync on startup in a background thread.
        # If the table already has data (e.g. from seed_db or a previous run) the
        # sync is still safe — it uses ON CONFLICT DO UPDATE and respects position_pinned.
        # This ensures the full ~900-player roster is always available for lineup search.
        if count < 100:
            print(f"[app] mlb_roster has {count} rows — running full sync now...")
        else:
            print(f"[app] mlb_roster has {count} rows — refreshing in background...")
        import threading
        threading.Thread(target=sync_mlb_roster, daemon=True).start()

    except Exception as e:
        print(f"[app] Startup error: {e}")

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
