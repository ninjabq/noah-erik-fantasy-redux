from flask import Flask, render_template, jsonify, request, abort
from apscheduler.schedulers.background import BackgroundScheduler
import sqlite3, os, json, unicodedata
from datetime import datetime, date, timedelta
from jobs.stat_fetcher import run_stat_update, run_today_update, run_yesterday_update, run_permanent_stats_update, strip_accents
from jobs.roster_sync import sync_mlb_roster
from week_schedule import current_week, week_dates, all_week_options, total_weeks, WEEKS

app = Flask(__name__)
DB_PATH = os.environ.get('DB_PATH', 'fantasy.db')

def get_db():
    """
    Open SQLite with WAL journal mode and a 30-second busy timeout.
    WAL allows concurrent reads during writes; the timeout prevents instant
    'database is locked' failures when a background job holds the write lock.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    return conn

def get_season_weeks():
    db = get_db()
    rows = db.execute('SELECT DISTINCT week FROM weekly_stats ORDER BY week').fetchall()
    db.close()
    return [r['week'] for r in rows]

SLOT_ORDER = [
    'C-0', '1B-0', '2B-0', '3B-0', 'SS-0',
    'OF-0', 'OF-1', 'OF-2', 'DH-0',
    'SP-0', 'SP-1', 'SP-2', 'SP-3', 'SP-4',
    'RP-0', 'RP-1', 'RP-2',
]
_SLOT_RANK = {slot: i for i, slot in enumerate(SLOT_ORDER)}

def slot_display(slot_key):
    return slot_key.split('-')[0] if slot_key and '-' in slot_key else (slot_key or '')

def sort_by_slot(rows, key='lineup_position'):
    return sorted(rows, key=lambda r: _SLOT_RANK.get(
        r[key] if isinstance(r, dict) else getattr(r, key, ''), 999))

# ── Jinja filter: convert display IP to true decimal ──────────────────────────
def _ip_display_to_true(ip_display):
    try:
        val    = float(ip_display)
        whole  = int(val)
        thirds = round((val - whole) * 10)
        return whole + thirds / 3.0
    except (TypeError, ValueError):
        return 0.0

app.jinja_env.filters['ip_to_true'] = _ip_display_to_true

_POSITION_TO_SLOT = {
    'C': 'C-0', '1B': '1B-0', '2B': '2B-0', '3B': '3B-0', 'SS': 'SS-0',
    'OF': 'OF-0', 'LF': 'OF-0', 'CF': 'OF-0', 'RF': 'OF-0',
    'DH': 'DH-0', 'IF': '1B-0', 'UT': 'DH-0',
    'SP': 'SP-0', 'RP': 'RP-0', 'CP': 'RP-0', 'P': 'SP-0',
}

def _auto_populate_permanents(db, managers, week_num):
    for m in managers:
        perm_rows = db.execute('''
            SELECT p.id as player_id, p.name, p.position_type,
                COALESCE(
                    (SELECT mr2.position FROM mlb_roster mr2 WHERE mr2.mlb_id = p.mlb_id LIMIT 1),
                    CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END
                ) as position
            FROM permanent_players pp
            JOIN players p ON pp.player_id = p.id
            WHERE pp.manager_id = ? AND pp.is_backup = 0 AND pp.has_been_swapped = 0
            GROUP BY p.id
        ''', (m['id'],)).fetchall()

        filled    = db.execute('SELECT position FROM lineups WHERE manager_id=? AND week=?',
                               (m['id'], week_num)).fetchall()
        used_slots = {r['position'] for r in filled}
        sp_idx = of_idx = 0

        for row in perm_rows:
            pos      = (row['position'] or '').upper()
            pos_type = row['position_type']
            if pos_type == 'pitcher':
                if pos == 'RP':
                    slot = next((f'RP-{i}' for i in range(3) if f'RP-{i}' not in used_slots), 'RP-0')
                else:
                    slot = f'SP-{sp_idx}'; sp_idx += 1
            else:
                if pos in ('OF', 'LF', 'CF', 'RF'):
                    slot = f'OF-{of_idx}'; of_idx += 1
                else:
                    slot = _POSITION_TO_SLOT.get(pos, 'DH-0')
            if slot in used_slots:
                continue
            existing = db.execute('SELECT id FROM lineups WHERE manager_id=? AND week=? AND player_id=?',
                                  (m['id'], week_num, row['player_id'])).fetchone()
            if existing:
                continue
            used_slots.add(slot)
            db.execute('''
                INSERT INTO lineups (manager_id, week, position, player_id, is_permanent)
                VALUES (?,?,?,?,1) ON CONFLICT(manager_id, week, position) DO NOTHING
            ''', (m['id'], week_num, slot, row['player_id']))
    db.commit()

# ── Season stats aggregation for permanent/backup players ─────────────────────

def _build_roster_season_stats(db, managers):
    """
    Returns roster_season: { manager_name: { 'batters': [...], 'pitchers': [...] } }
    Each player dict has their roster metadata + full-season aggregated stats.

    Stats are read from the `permanent_stats` table, which is populated by the
    daily `run_permanent_stats_update()` job.  This guarantees backup players
    have stats even if they've never appeared in a lineup.

    Falls back gracefully to zeros if the table is empty (e.g. first run before
    the job has fired).
    """
    all_perm_rows = db.execute('''
        SELECT DISTINCT
            p.id          AS player_id,
            p.name,
            p.team,
            p.position_type,
            COALESCE(
                (SELECT mr2.position FROM mlb_roster mr2 WHERE mr2.mlb_id = p.mlb_id LIMIT 1),
                CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END
            ) AS position,
            pp.manager_id,
            pp.is_backup,
            pp.has_been_swapped
        FROM permanent_players pp
        JOIN players p ON pp.player_id = p.id
        ORDER BY pp.manager_id, pp.is_backup, p.position_type, p.name
    ''').fetchall()

    # Pull stats from permanent_stats (keyed by player_id)
    ps_rows = db.execute('SELECT * FROM permanent_stats').fetchall()
    perm_stats_by_pid = {r['player_id']: dict(r) for r in ps_rows}

    season_stats = {}
    for row in all_perm_rows:
        pid = row['player_id']
        if pid in season_stats:
            continue

        ps = perm_stats_by_pid.get(pid)

        if row['position_type'] == 'batter':
            if ps:
                agg = {
                    'singles':     ps['singles']     or 0,
                    'doubles':     ps['doubles']     or 0,
                    'triples':     ps['triples']     or 0,
                    'homeruns':    ps['homeruns']    or 0,
                    'ab':          ps['ab']          or 0,
                    'total_bases': ps['total_bases'] or 0,
                    'rbi':         ps['rbi']         or 0,
                    'bb':          ps['bb']          or 0,
                    'sb':          ps['sb']          or 0,
                    'k':           ps['k']           or 0,
                }
            else:
                agg = dict(singles=0, doubles=0, triples=0, homeruns=0,
                           ab=0, total_bases=0, rbi=0, bb=0, sb=0, k=0)
            agg['hits'] = agg['singles'] + agg['doubles'] + agg['triples'] + agg['homeruns']
            agg['slg']  = round(agg['total_bases'] / agg['ab'], 3) if agg['ab'] else 0.0
        else:
            if ps:
                true_ip = _ip_display_to_true(ps['ip'] or 0)
                agg = {
                    'ip':       ps['ip']       or 0,
                    'er':       ps['er']       or 0,
                    'h':        ps['h']        or 0,
                    'p_bb':     ps['p_bb']     or 0,
                    'sv':       ps['sv']       or 0,
                    'hd':       ps['hd']       or 0,
                    'bs':       ps['bs']       or 0,
                    'so':       ps['so']       or 0,
                    'qs':       ps['qs']       or 0,
                    'sv_hd_bs': ps['sv_hd_bs'] or 0,
                    'era':      round(ps['er'] / true_ip * 9,                      2) if true_ip else 0.0,
                    'whip':     round((ps['h'] + ps['p_bb']) / true_ip,            2) if true_ip else 0.0,
                }
            else:
                agg = dict(ip=0, er=0, h=0, p_bb=0, sv=0, hd=0, bs=0,
                           so=0, qs=0, sv_hd_bs=0, era=0.0, whip=0.0)

        season_stats[pid] = agg

    # Assemble per-manager structure
    roster_season = {}
    for m in managers:
        batters, pitchers = [], []
        for row in all_perm_rows:
            if row['manager_id'] != m['id']:
                continue
            entry = dict(row)
            entry.update(season_stats.get(row['player_id'], {}))
            (pitchers if row['position_type'] == 'pitcher' else batters).append(entry)
        roster_season[m['name']] = {'batters': batters, 'pitchers': pitchers}

    return roster_season

# ── routes ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    db = get_db()
    weeks    = get_season_weeks()
    week_num = current_week()

    totals   = {r['manager']: r['total'] for r in db.execute(
        'SELECT manager, SUM(wins) as total FROM category_wins GROUP BY manager')}
    by_week  = {}
    for r in db.execute('SELECT week, manager, wins FROM category_wins ORDER BY week'):
        by_week.setdefault(r['week'], {})[r['manager']] = r['wins']

    managers      = db.execute('SELECT name FROM managers ORDER BY id').fetchall()
    manager_names = [m['name'] for m in managers]

    weeks_won = {name: 0 for name in manager_names}
    for week, wdata in by_week.items():
        if week == week_num or len(wdata) < 2:
            continue
        scores = list(wdata.items())
        if scores[0][1] != scores[1][1]:
            weeks_won[max(scores, key=lambda x: x[1])[0]] += 1

    cur_week_data = {}
    for m in db.execute('SELECT * FROM managers ORDER BY id').fetchall():
        cur_week_data[m['name']] = {
            'batters':  db.execute('''SELECT ws.* FROM weekly_stats ws JOIN players p
                ON ws.player_id=p.id WHERE ws.manager_id=? AND ws.week=? AND p.position_type='batter'
                ''', (m['id'], week_num)).fetchall(),
            'pitchers': db.execute('''SELECT ws.* FROM weekly_stats ws JOIN players p
                ON ws.player_id=p.id WHERE ws.manager_id=? AND ws.week=? AND p.position_type='pitcher'
                ''', (m['id'], week_num)).fetchall(),
        }

    db.close()
    return render_template('index.html',
        weeks=weeks, week_num=week_num, totals=totals, by_week=by_week,
        manager_names=manager_names,
        cur_week_cats=_compute_category_winners(cur_week_data),
        weeks_won=weeks_won, week_options=all_week_options())

@app.route('/week/<int:n>')
def week_view(n):
    db       = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()
    _auto_populate_permanents(db, managers, n)

    data = {}
    for m in managers:
        lineup_rows = db.execute('''
            SELECT l.position, p.id as player_id, p.name, p.team, p.position_type
            FROM lineups l JOIN players p ON l.player_id=p.id
            WHERE l.manager_id=? AND l.week=?
        ''', (m['id'], n)).fetchall()
        stats_by_pid = {r['player_id']: dict(r) for r in db.execute('''
            SELECT ws.player_id, p.name, p.team, ws.*
            FROM weekly_stats ws JOIN players p ON ws.player_id=p.id
            WHERE ws.manager_id=? AND ws.week=?
        ''', (m['id'], n))}

        batters, pitchers = [], []
        for lr in lineup_rows:
            pid = lr['player_id']
            if pid in stats_by_pid:
                row = dict(stats_by_pid[pid])
                row['lineup_position'] = lr['position']
                row['team'] = row['team'] or lr['team']
            else:
                fn  = _empty_batter_row if lr['position_type'] == 'batter' else _empty_pitcher_row
                row = fn({'position': lr['position'], 'name': lr['name'], 'team': lr['team']})
            (pitchers if lr['position_type'] == 'pitcher' else batters).append(row)

        data[m['name']] = {'batters': sort_by_slot(batters), 'pitchers': sort_by_slot(pitchers)}

    db.close()
    return render_template('week.html',
        n=n, data=data, cats=_compute_category_winners(data),
        manager_names=[m['name'] for m in managers],
        week_options=all_week_options(), max_week=WEEKS[-1][0], slot_display=slot_display)

def _empty_batter_row(r):
    return {'name': r['name'], 'team': r['team'], 'lineup_position': r['position'],
            'singles': 0, 'doubles': 0, 'triples': 0, 'homeruns': 0,
            'ab': 0, 'total_bases': 0, 'slg': 0, 'rbi': 0, 'bb': 0, 'sb': 0, 'k': 0}

def _empty_pitcher_row(r):
    return {'name': r['name'], 'team': r['team'], 'lineup_position': r['position'],
            'ip': 0, 'er': 0, 'h': 0, 'p_bb': 0, 'h_plus_bb': 0,
            'sv': 0, 'hd': 0, 'bs': 0, 'era': 0, 'whip': 0, 'so': 0, 'qs': 0, 'sv_hd_bs': 0}

@app.route('/lineups')
def lineups_view():
    return lineups_week_view(current_week())

@app.route('/lineups/<int:week_num>')
def lineups_week_view(week_num):
    db       = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()
    _auto_populate_permanents(db, managers, week_num)

    used = {}
    for m in managers:
        rows = db.execute('''
            SELECT DISTINCT p.name, p.position_type,
                COALESCE((SELECT mr2.position FROM mlb_roster mr2
                          WHERE mr2.mlb_id=p.mlb_id LIMIT 1), p.position_type) as position
            FROM lineups l JOIN players p ON l.player_id=p.id
            WHERE l.manager_id=? AND l.is_permanent=0 AND l.week!=?
        ''', (m['id'], week_num)).fetchall()
        used[m['name']] = [dict(r) for r in rows]

    used_this_week = {}
    for m in managers:
        rows = db.execute('''
            SELECT DISTINCT p.name FROM lineups l JOIN players p ON l.player_id=p.id
            WHERE l.week=? AND l.is_permanent=0
        ''', (week_num,)).fetchall()
        used_this_week[m['name']] = [r['name'] for r in rows]

    lineups = {}
    for m in managers:
        rows = db.execute('''
            SELECT l.position, p.name, l.week, l.is_permanent
            FROM lineups l JOIN players p ON l.player_id=p.id
            WHERE l.manager_id=? AND l.week=? ORDER BY l.position
        ''', (m['id'], week_num)).fetchall()
        lineups[m['name']] = {r['position']: dict(r) for r in rows}

    perms = {}
    for m in managers:
        rows = db.execute('''
            SELECT p.name, p.mlb_id, p.position_type,
                COALESCE((SELECT mr2.position FROM mlb_roster mr2
                          WHERE mr2.mlb_id=p.mlb_id LIMIT 1),
                         CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END) as position,
                pp.is_backup, MIN(pp.has_been_swapped) as has_been_swapped
            FROM permanent_players pp JOIN players p ON pp.player_id=p.id
            WHERE pp.manager_id=?
            GROUP BY p.name, p.position_type, pp.is_backup
            ORDER BY pp.is_backup, p.position_type
        ''', (m['id'],)).fetchall()
        perms[m['name']] = [dict(r) for r in rows]

    db.close()
    return render_template('lineups.html',
        week_num=week_num, save_week=week_num,
        managers=[m['name'] for m in managers],
        used=used, used_this_week=used_this_week,
        lineups=lineups, perms=perms, week_options=all_week_options())

@app.route('/roster')
def roster_view():
    db       = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()

    roster_data = {}
    for m in managers:
        rows = db.execute('''
            SELECT p.name, p.team, p.position_type,
                COALESCE((SELECT mr2.position FROM mlb_roster mr2
                          WHERE mr2.mlb_id=p.mlb_id LIMIT 1),
                         CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END) as position,
                pp.is_backup, MIN(pp.has_been_swapped) as has_been_swapped
            FROM permanent_players pp JOIN players p ON pp.player_id=p.id
            WHERE pp.manager_id=?
            GROUP BY p.name, p.position_type, pp.is_backup
            ORDER BY pp.is_backup, p.position_type
        ''', (m['id'],)).fetchall()
        roster_data[m['name']] = [dict(r) for r in rows]

    roster_season = _build_roster_season_stats(db, managers)
    db.close()

    return render_template('roster.html',
        roster_data=roster_data,
        roster_season=roster_season,
        manager_names=[m['name'] for m in managers])

# ── API ────────────────────────────────────────────────────────────────────────

@app.route('/api/week/<int:n>')
def api_week(n):
    db = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()
    result = {}
    for m in managers:
        result[m['name']] = {
            'batters':  [dict(r) for r in db.execute('''
                SELECT p.name, p.team, ws.* FROM weekly_stats ws
                JOIN players p ON ws.player_id=p.id
                WHERE ws.manager_id=? AND ws.week=? AND p.position_type='batter'
                ORDER BY ws.lineup_position''', (m['id'], n))],
            'pitchers': [dict(r) for r in db.execute('''
                SELECT p.name, p.team, ws.* FROM weekly_stats ws
                JOIN players p ON ws.player_id=p.id
                WHERE ws.manager_id=? AND ws.week=? AND p.position_type='pitcher'
                ORDER BY ws.lineup_position''', (m['id'], n))],
        }
    db.close()
    return jsonify(result)

@app.route('/api/today')
def api_today():
    db       = get_db()
    week_num = current_week()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()
    result   = {}
    for m in managers:
        rows = db.execute('''
            SELECT p.name, p.team, p.position_type, l.position as lineup_position, ts.*
            FROM today_stats ts JOIN players p ON ts.player_id=p.id
            LEFT JOIN lineups l ON l.player_id=ts.player_id
                AND l.manager_id=ts.manager_id AND l.week=?
            WHERE ts.manager_id=?
        ''', (week_num, m['id'])).fetchall()
        result[m['name']] = sorted([dict(r) for r in rows],
            key=lambda r: _SLOT_RANK.get(r.get('lineup_position') or '', 999))
    db.close()
    return jsonify(result)

@app.route('/api/yesterday')
def api_yesterday():
    from jobs.stat_fetcher import get_game_ids_for_date, get_live_boxscore, BatterStats, PitcherStats
    db           = get_db()
    week_num     = current_week()
    yesterday_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    managers     = db.execute('SELECT * FROM managers ORDER BY id').fetchall()

    game_ids = get_game_ids_for_date(yesterday_str)
    game_info, all_players = {}, {}
    for gid in game_ids:
        bs, status, score, inning, away, home = get_live_boxscore(gid)
        game_info[gid] = (status, score, inning, away, home)
        for pid_key, pdata in {**bs.get('home',{}).get('players',{}),
                                **bs.get('away',{}).get('players',{})}.items():
            all_players[pid_key] = (pdata, gid)

    team_to_game = {}
    for gid in game_ids:
        _, _, _, away, home = game_info[gid]
        team_to_game[away.upper()] = gid
        team_to_game[home.upper()] = gid

    result = {}
    for m in managers:
        lineup = db.execute('''
            SELECT l.player_id, p.name, p.mlb_id, p.position_type, p.team,
                   l.position as lineup_position
            FROM lineups l JOIN players p ON l.player_id=p.id
            WHERE l.manager_id=? AND l.week=?
        ''', (m['id'], week_num)).fetchall()

        rows = []
        for slot in lineup:
            key  = f"ID{slot['mlb_id']}"
            team = (slot['team'] or '').upper()
            base = {k: slot[k] for k in ('name','team','position_type','lineup_position')}
            if key in all_players:
                pdata, gid = all_players[key]
                status, score, inning, away, home = game_info[gid]
                opponent = home if team == away.upper() else away
            elif team in team_to_game:
                gid = team_to_game[team]
                status, score, inning, away, home = game_info[gid]
                opponent = home if team == away.upper() else away
                pdata = None
            else:
                base.update({'game_status':'off','opponent':'—','game_score':'—','inning':'Off'})
                rows.append(base); continue
            base.update({'game_status':status,'opponent':opponent,'game_score':score,'inning':inning})
            if slot['position_type'] == 'batter':
                s = BatterStats()
                if pdata: s.update(pdata.get('stats',{}))
                base.update({'singles':s.singles,'doubles':s.doubles,'triples':s.triples,
                             'homeruns':s.homeruns,'ab':s.ab,'rbi':s.rbi,'bb':s.bb,'sb':s.sb,'k':s.k})
            else:
                s = PitcherStats()
                if pdata: s.update(pdata.get('stats',{}))
                base.update({'ip':s.ip_display,'er':s.er,'h':s.h,'p_bb':s.bb,
                             'sv':s.sv,'hd':s.hd,'bs':s.bs,'so':s.so,'qs':s.qs})
            rows.append(base)
        result[m['name']] = sorted(rows,
            key=lambda r: _SLOT_RANK.get(r.get('lineup_position') or '', 999))

    db.close()
    return jsonify({'date': yesterday_str, 'data': result})

@app.route('/api/validate_player', methods=['POST'])
def validate_player():
    data         = request.json
    player_name  = data.get('player_name','').strip()
    manager_name = data.get('manager','').strip()
    week         = data.get('week', current_week())
    position     = data.get('position','')
    db           = get_db()
    manager      = db.execute('SELECT id FROM managers WHERE name=?',(manager_name,)).fetchone()
    if not manager:
        db.close(); return jsonify({'valid':False,'reason':'Unknown manager'})
    if db.execute('''SELECT l.week FROM lineups l JOIN players p ON l.player_id=p.id
        WHERE l.manager_id=? AND p.name=? AND l.is_permanent=0 AND l.week!=?''',
        (manager['id'],player_name,week)).fetchone():
        db.close(); return jsonify({'valid':False,'reason':'Already used in a previous week'})
    dupe = db.execute('''SELECT l.position FROM lineups l JOIN players p ON l.player_id=p.id
        WHERE l.manager_id=? AND p.name=? AND l.week=? AND l.position!=?''',
        (manager['id'],player_name,week,position)).fetchone()
    if dupe:
        db.close(); return jsonify({'valid':False,'reason':f'Already in your lineup ({dupe["position"]})'})
    other_perm = db.execute('''SELECT m.name FROM permanent_players pp
        JOIN players p ON pp.player_id=p.id JOIN managers m ON pp.manager_id=m.id
        WHERE pp.manager_id!=? AND p.name=? AND pp.is_backup=0 AND pp.has_been_swapped=0''',
        (manager['id'],player_name)).fetchone()
    if other_perm:
        db.close(); return jsonify({'valid':False,'reason':f"Permanent player for {other_perm['name']}"})
    other = db.execute('''SELECT m.name FROM lineups l JOIN players p ON l.player_id=p.id
        JOIN managers m ON l.manager_id=m.id
        WHERE m.name!=? AND p.name=? AND l.week=? AND l.is_permanent=0''',
        (manager_name,player_name,week)).fetchone()
    if other:
        db.close(); return jsonify({'valid':False,'reason':f'Already used by {other["name"]} this week'})
    db.close(); return jsonify({'valid':True})

@app.route('/api/remove_lineup', methods=['POST'])
def remove_lineup():
    data         = request.json
    manager_name = data.get('manager','').strip()
    week         = data.get('week', current_week())
    position     = data.get('position','').strip()
    db           = get_db()
    manager      = db.execute('SELECT id FROM managers WHERE name=?',(manager_name,)).fetchone()
    if not manager:
        db.close(); return jsonify({'success':False,'reason':'Unknown manager'})
    db.execute('DELETE FROM lineups WHERE manager_id=? AND week=? AND position=?',
               (manager['id'],week,position))
    db.commit(); db.close()
    return jsonify({'success':True})

@app.route('/api/set_lineup', methods=['POST'])
def set_lineup():
    data         = request.json
    manager_name = data.get('manager')
    week         = data.get('week', current_week())
    position     = data.get('position')
    player_name  = data.get('player_name','').strip()
    mlb_id       = data.get('mlb_id','')
    is_permanent = int(data.get('is_permanent',0))
    print(f"[set_lineup] manager={manager_name} week={week} pos={position} player={player_name} perm={is_permanent}")
    db      = get_db()
    manager = db.execute('SELECT id FROM managers WHERE name=?',(manager_name,)).fetchone()
    if not manager:
        db.close(); return jsonify({'success':False,'reason':'Unknown manager'})
    player = None
    if mlb_id:
        try:
            mid    = int(mlb_id)
            player = db.execute('SELECT id FROM players WHERE mlb_id=?',(mid,)).fetchone()
            if not player:
                rr = db.execute('SELECT * FROM mlb_roster WHERE mlb_id=?',(mid,)).fetchone()
                if rr:
                    db.execute('INSERT OR IGNORE INTO players (mlb_id,name,team,position_type) VALUES (?,?,?,?)',
                               (rr['mlb_id'],rr['name'],rr['team'],rr['position_type']))
                    db.commit()
                    player = db.execute('SELECT id FROM players WHERE mlb_id=?',(mid,)).fetchone()
        except (ValueError,TypeError): pass
    if not player:
        player = db.execute('SELECT id FROM players WHERE name=?',(player_name,)).fetchone()
    if not player:
        rr = db.execute('SELECT * FROM mlb_roster WHERE name=?',(player_name,)).fetchone()
        if rr:
            db.execute('INSERT OR IGNORE INTO players (mlb_id,name,team,position_type) VALUES (?,?,?,?)',
                       (rr['mlb_id'],rr['name'],rr['team'],rr['position_type']))
            db.commit()
            player = db.execute('SELECT id FROM players WHERE mlb_id=?',(rr['mlb_id'],)).fetchone()
    if not player:
        db.close(); return jsonify({'success':False,'reason':f'Player "{player_name}" not found'})
    db.execute('''INSERT INTO lineups (manager_id,week,position,player_id,is_permanent)
        VALUES (?,?,?,?,?) ON CONFLICT(manager_id,week,position) DO UPDATE SET
        player_id=excluded.player_id,
        is_permanent=MAX(is_permanent,excluded.is_permanent)''',
        (manager['id'],week,position,player['id'],is_permanent))
    db.commit(); db.close()
    return jsonify({'success':True})

@app.route('/api/swap_permanent', methods=['POST'])
def swap_permanent():
    data         = request.json
    manager_name = data.get('manager')
    perm_name    = data.get('permanent_player')
    backup_name  = data.get('backup_player')
    swap_type    = data.get('swap_type','permanent')
    week         = data.get('week', current_week())
    db           = get_db()
    manager      = db.execute('SELECT id FROM managers WHERE name=?',(manager_name,)).fetchone()
    if not manager: db.close(); return jsonify({'success':False,'reason':'Unknown manager'})
    pp  = db.execute('SELECT id FROM players WHERE name=?',(perm_name,)).fetchone()
    bp  = db.execute('SELECT id FROM players WHERE name=?',(backup_name,)).fetchone()
    if not pp or not bp: db.close(); return jsonify({'success':False,'reason':'Player not found'})

    if swap_type == 'permanent':
        db.execute('UPDATE permanent_players SET has_been_swapped=1 WHERE manager_id=? AND is_backup=0 AND player_id=?',(manager['id'],pp['id']))
        db.execute('UPDATE permanent_players SET has_been_swapped=1 WHERE manager_id=? AND is_backup=1 AND player_id=?',(manager['id'],bp['id']))
        bi = db.execute('''SELECT p.position_type,
            COALESCE((SELECT mr2.position FROM mlb_roster mr2 WHERE mr2.mlb_id=p.mlb_id LIMIT 1),
                     CASE p.position_type WHEN 'pitcher' THEN 'RP' ELSE 'OF' END) as position
            FROM players p WHERE p.id=?''',(bp['id'],)).fetchone()
        bpos  = (bi['position'] if bi else 'OF').upper()
        btype = bi['position_type'] if bi else 'batter'
        def _slot_for(pos, pos_type, used):
            if pos_type=='pitcher':
                cands = [f'SP-{i}' for i in range(5)] if pos=='SP' else [f'RP-{i}' for i in range(3)]
            elif pos in ('OF','LF','CF','RF'):
                cands = [f'OF-{i}' for i in range(3)]
            else:
                cands = [_POSITION_TO_SLOT.get(pos,'DH-0')]
            return next((c for c in cands if c not in used), cands[0])
        for row in db.execute('SELECT week,position FROM lineups WHERE manager_id=? AND player_id=? AND is_permanent=1',(manager['id'],pp['id'])).fetchall():
            wk,old = row['week'],row['position']
            used   = {r['position'] for r in db.execute('SELECT position FROM lineups WHERE manager_id=? AND week=? AND position!=?',(manager['id'],wk,old))}
            new    = _slot_for(bpos,btype,used)
            db.execute('DELETE FROM lineups WHERE manager_id=? AND week=? AND position=?',(manager['id'],wk,old))
            db.execute('''INSERT INTO lineups (manager_id,week,position,player_id,is_permanent)
                VALUES (?,?,?,?,1) ON CONFLICT(manager_id,week,position) DO UPDATE SET
                player_id=excluded.player_id,is_permanent=1''',(manager['id'],wk,new,bp['id']))
    else:
        slot = db.execute('SELECT position FROM lineups WHERE manager_id=? AND week=? AND player_id=? AND is_permanent=1',(manager['id'],week,pp['id'])).fetchone()
        if slot:
            db.execute('UPDATE lineups SET player_id=?,is_permanent=1 WHERE manager_id=? AND week=? AND position=?',(bp['id'],manager['id'],week,slot['position']))

    db.commit(); db.close()
    return jsonify({'success':True,'swap_type':swap_type})

@app.route('/api/roster_search')
def api_roster_search():
    q             = request.args.get('q','').strip()
    position_type = request.args.get('position_type','')
    manager_name  = request.args.get('manager','')
    week          = request.args.get('week', current_week(), type=int)
    limit         = min(int(request.args.get('limit',15)),50)
    if len(q) < 2: return jsonify([])
    q_ascii = strip_accents(q).lower()
    like    = f'%{q_ascii}%'
    db      = get_db()
    used_by_mgr, used_other_week, in_lineup, other_perms = set(),set(),set(),set()
    if manager_name:
        mgr = db.execute('SELECT id FROM managers WHERE name=?',(manager_name,)).fetchone()
        if mgr:
            used_by_mgr    = {r['name'] for r in db.execute('SELECT p.name FROM lineups l JOIN players p ON l.player_id=p.id WHERE l.manager_id=? AND l.is_permanent=0 AND l.week!=?',(mgr['id'],week))}
            used_other_week= {r['name'] for r in db.execute('SELECT p.name FROM lineups l JOIN players p ON l.player_id=p.id JOIN managers m ON l.manager_id=m.id WHERE m.name!=? AND l.week=? AND l.is_permanent=0',(manager_name,week))}
            in_lineup      = {r['name'] for r in db.execute('SELECT p.name FROM lineups l JOIN players p ON l.player_id=p.id WHERE l.manager_id=? AND l.week=?',(mgr['id'],week))}
            other_perms    = {r['name'] for r in db.execute('SELECT p.name FROM permanent_players pp JOIN players p ON pp.player_id=p.id WHERE pp.manager_id!=? AND pp.is_backup=0 AND pp.has_been_swapped=0',(mgr['id'],))}
    sql    = 'SELECT mlb_id,name,team,position,position_type FROM mlb_roster WHERE (LOWER(name_ascii) LIKE ? OR LOWER(name) LIKE ?)'
    params = [like, f'%{q.lower()}%']
    if position_type:
        sql += ' AND position_type=?'; params.append(position_type)
    sql += ' ORDER BY name_ascii LIMIT ?'; params.append(limit)
    rows   = db.execute(sql,params).fetchall()
    db.close()
    results = []
    for r in rows:
        conflict = ('in_lineup' if r['name'] in in_lineup else
                    'other_perm' if r['name'] in other_perms else
                    'used' if r['name'] in used_by_mgr else
                    'other_week' if r['name'] in used_other_week else None)
        results.append({'mlb_id':r['mlb_id'],'name':r['name'],'team':r['team'],
                         'position':r['position'],'position_type':r['position_type'],'conflict':conflict})
    return jsonify(results)

@app.route('/api/roster_sync', methods=['POST'])
def api_roster_sync():
    sync_mlb_roster(); return jsonify({'success':True})

@app.route('/api/stat_update', methods=['POST'])
def api_stat_update():
    """
    Trigger a stat update for a specific week (or the current week if not specified).
    Accepts JSON body: { "week": <int> }
    Runs in a background thread so the HTTP response returns immediately.
    """
    import threading
    data = request.get_json(silent=True) or {}
    week = data.get('week')          # None → run_stat_update will use current_week()
    if week is not None:
        try:
            week = int(week)
        except (ValueError, TypeError):
            week = None
    threading.Thread(target=run_stat_update, args=(week,), daemon=True).start()
    return jsonify({'success': True, 'message': f'Stat update started for week {week or "current"}'})

@app.route('/api/roster_last_updated')
def api_roster_last_updated():
    db  = get_db()
    row = db.execute('SELECT MAX(last_updated) as lu FROM mlb_roster').fetchone()
    db.close()
    return jsonify({'last_updated': row['lu'] if row else None})

@app.route('/api/permanent_players/<manager_name>')
def api_permanent_players(manager_name):
    db  = get_db()
    mgr = db.execute('SELECT id FROM managers WHERE name=?',(manager_name,)).fetchone()
    if not mgr: db.close(); return jsonify([])
    rows = db.execute('''SELECT p.name,p.position_type,
        COALESCE(mr.position,p.position_type) as position,pp.is_backup,pp.has_been_swapped
        FROM permanent_players pp JOIN players p ON pp.player_id=p.id
        LEFT JOIN mlb_roster mr ON mr.mlb_id=p.mlb_id
        WHERE pp.manager_id=? ORDER BY pp.is_backup,p.position_type''',(mgr['id'],)).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])

# ── Category math ──────────────────────────────────────────────────────────────

def _compute_category_winners(data):
    managers = list(data.keys())
    if len(managers) < 2: return {}
    def val(r,f):
        try: return r[f] or 0
        except: return 0
    def total(rows,f): return sum(val(r,f) for r in rows)
    def wavg(rows,num,den):
        n = sum(val(r,num) for r in rows)
        d = sum(_ip_display_to_true(val(r,den)) for r in rows)
        return n/d if d else 0
    def wavg_plain(rows,num,den):
        n = sum(val(r,num) for r in rows)
        d = sum(val(r,den) for r in rows)
        return n/d if d else 0
    m1,m2  = managers[0], managers[1]
    cats   = [
        ('SLG',      lambda m: wavg_plain(data[m]['batters'],'total_bases','ab'), False),
        ('RBI',      lambda m: total(data[m]['batters'],'rbi'),                   False),
        ('BB',       lambda m: total(data[m]['batters'],'bb'),                    False),
        ('SB',       lambda m: total(data[m]['batters'],'sb'),                    False),
        ('K',        lambda m: total(data[m]['batters'],'k'),                     True),
        ('ERA',      lambda m: wavg(data[m]['pitchers'],'er','ip')*9,             True),
        ('WHIP',     lambda m: wavg(data[m]['pitchers'],'h_plus_bb','ip'),        True),
        ('SO',       lambda m: total(data[m]['pitchers'],'so'),                   False),
        ('QS',       lambda m: total(data[m]['pitchers'],'qs'),                   False),
        ('SV+HD-BS', lambda m: total(data[m]['pitchers'],'sv_hd_bs'),             False),
    ]
    results = {}
    for cat,fn,lower in cats:
        v1,v2  = fn(m1), fn(m2)
        if v1==v2:           winner,pts = 'Tie',{m1:0.5,m2:0.5}
        elif (lower and v1<v2) or (not lower and v1>v2): winner,pts = m1,{m1:1,m2:0}
        else:                winner,pts = m2,{m1:0,m2:1}
        results[cat] = {'winner':winner, m1:round(v1,3), m2:round(v2,3), 'pts':pts}
    return results

# ── Scheduler ──────────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler()

# coalesce=True  → if multiple firings were missed while the job was running,
#                  only execute once when it becomes free (no pile-up).
# max_instances=1 → never run two copies of the same job simultaneously.
# misfire_grace_time → how many seconds late a job is still allowed to start;
#                      set to half the interval so a briefly-delayed job still runs.
scheduler.add_job(
    run_stat_update, 'interval', minutes=15, id='stat_update',
    coalesce=True, max_instances=1, misfire_grace_time=450,
)
scheduler.add_job(
    run_today_update, 'interval', minutes=1, id='today_update',
    coalesce=True, max_instances=1, misfire_grace_time=30,
)
scheduler.add_job(
    sync_mlb_roster, 'interval', hours=24, id='roster_sync',
    coalesce=True, max_instances=1, misfire_grace_time=3600,
)
scheduler.add_job(
    run_permanent_stats_update, 'interval', hours=24, id='permanent_stats',
    coalesce=True, max_instances=1, misfire_grace_time=3600,
)
scheduler.start()

with app.app_context():
    try:
        db = get_db()
        try:
            db.execute('ALTER TABLE mlb_roster ADD COLUMN position_pinned INTEGER DEFAULT 0')
            db.commit()
        except Exception: pass

        # Create permanent_stats table if it doesn't exist yet
        db.executescript('''
            CREATE TABLE IF NOT EXISTS permanent_stats (
                player_id   INTEGER PRIMARY KEY REFERENCES players(id),
                -- batter
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
                -- pitcher
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
                last_updated TEXT
            );
        ''')
        db.commit()

        db.execute('''
            DELETE FROM permanent_players
            WHERE id NOT IN (
                SELECT MIN(id) FROM permanent_players GROUP BY manager_id,player_id,is_backup
            )
        ''')
        db.execute("UPDATE mlb_roster SET position='SP' WHERE position_type='pitcher' AND position='P' AND position_pinned=0")
        db.execute("UPDATE mlb_roster SET position='RP' WHERE position_type='pitcher' AND position IN ('CP','RL','CL','MR','SU','SW','RS') AND position_pinned=0")
        db.commit()

        count = db.execute('SELECT COUNT(*) as c FROM mlb_roster').fetchone()['c']
        ps_count = db.execute('SELECT COUNT(*) as c FROM permanent_stats').fetchone()['c']
        db.close()

        print(f"[app] mlb_roster has {count} rows — refreshing in background...")
        import threading
        threading.Thread(target=sync_mlb_roster, daemon=True).start()

        # Run permanent stats on startup if table is empty (first deploy or new table)
        if ps_count == 0:
            print("[app] permanent_stats is empty — running initial fetch in background...")
            threading.Thread(target=run_permanent_stats_update, daemon=True).start()
        else:
            print(f"[app] permanent_stats has {ps_count} rows — daily job will refresh.")

    except Exception as e:
        print(f"[app] Startup error: {e}")

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
