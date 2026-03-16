"""
Background job that replaces AWS Lambda + Google Sheets backend.
Fetches MLB stats via mlbstatsapi / statsapi and writes to SQLite.
"""

import os, sqlite3, unicodedata
from datetime import datetime, timedelta, date

import statsapi as mlb
import mlbstatsapi as mlb2

from week_schedule import current_week as _sched_current_week, week_dates as _sched_week_dates

DB_PATH = os.environ.get('DB_PATH', 'fantasy.db')

# Players that the MLB API lookup struggles with — keyed by the name as the
# manager would type it (may or may not have accents).
MISSING_PLAYERS = {
    "Tyler O'Neill":      {"player_id": 641933, "team": "BOS"},
    "Jake Burger":        {"player_id": 669394, "team": "MIA"},
    "Garrett Whitlock":   {"player_id": 676477, "team": "BOS"},
    "Pete Fairbanks":     {"player_id": 664126, "team": "TB"},
    "Joe Kelly":          {"player_id": 523260, "team": "LAD"},
    "Gleyber Torres":     {"player_id": 650402, "team": "NYY"},
    "Gabriel Moreno":     {"player_id": 672515, "team": "ARI"},
    "J.P. Crawford":      {"player_id": 641487, "team": "SEA"},
    "Reese Olson":        {"player_id": 681857, "team": "DET"},
    "Julio Rodríguez":    {"player_id": 677594, "team": "SEA"},
    "Julio Rodriguez":    {"player_id": 677594, "team": "SEA"},
    "Freddie Freeman":    {"player_id": 518692, "team": "LAD"},
    "Bryan Reynolds":     {"player_id": 668804, "team": "PIT"},
    "Ryan Weathers":      {"player_id": 677960, "team": "MIA"},
    "Max Muncy":          {"player_id": 571970, "team": "LAD"},
    # Permanent / backup players — pinned to avoid API lookup failures
    "Emmanuel Clase":     {"player_id": 667555, "team": "CLE"},
    "Cristopher Sanchez": {"player_id": 656945, "team": "PHI"},
    "Cristopher Sánchez": {"player_id": 656945, "team": "PHI"},
    "Jhoan Duran":        {"player_id": 661858, "team": "MIN"},
    "Devin Williams":     {"player_id": 669203, "team": "NYY"},
    "Mason Miller":       {"player_id": 694984, "team": "OAK"},
    "Ranger Suarez":      {"player_id": 661482, "team": "PHI"},
    "Yoshinobu Yamamoto": {"player_id": 808982, "team": "LAD"},
    "Cole Ragans":        {"player_id": 669712, "team": "KC"},
    "Tarik Skubal":       {"player_id": 669373, "team": "DET"},
    "Paul Skenes":        {"player_id": 694973, "team": "PIT"},
    "Eugenio Suarez":     {"player_id": 553993, "team": "ARI"},
    "Eugenio Suárez":     {"player_id": 553993, "team": "ARI"},
    "Alec Bohm":          {"player_id": 664353, "team": "PHI"},
    "Gunnar Henderson":   {"player_id": 683002, "team": "BAL"},
    "Aaron Judge":        {"player_id": 592450, "team": "NYY"},
    "Kyle Tucker":        {"player_id": 663855, "team": "CHC"},
    "Jarren Duran":       {"player_id": 680776, "team": "BOS"},
}

TEAM_OVERRIDES = {
    "Tyler Ferguson":    "ATH",
    "Chad Patrick":      "MIL",
    "Jason Alexander":   "HOU",
    "Nacho Alvarez Jr.": "WAS",
}

# ── Unicode helpers ────────────────────────────────────────────────────────────

def strip_accents(text):
    """'Cristopher Sánchez' → 'Cristopher Sanchez'"""
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

def name_variants(name):
    """Return a list of name strings to try in order."""
    variants = [name]
    stripped = strip_accents(name)
    if stripped != name:
        variants.append(stripped)
    return variants

# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── Player lookup ──────────────────────────────────────────────────────────────

def _team_for_pid(pid, name):
    """Resolve team abbreviation for a known MLB player ID."""
    try:
        team_code = mlb.lookup_team(
            mlb.player_stat_data(pid)['current_team']
        )[0]['fileCode'].upper()
        return team_code
    except (IndexError, KeyError):
        return TEAM_OVERRIDES.get(name, 'N/A')

def get_player_id_and_team(name):
    """
    Look up MLB player ID and team for a given name string.
    Strategy:
      1. Check MISSING_PLAYERS dict (handles API-unfriendly names).
      2. Try mlbstatsapi with original name.
      3. Try mlbstatsapi with accent-stripped name.
      4. Fall back to mlb.lookup_player (statsapi) with stripped name.
      5. Try DB lookup by name (mlb_roster table) as last resort.
    """
    # Step 1 — explicit override dict (both accented and plain variants keyed)
    if name in MISSING_PLAYERS:
        d = MISSING_PLAYERS[name]
        return d["player_id"], d["team"]

    # Step 2 & 3 — mlbstatsapi with original and accent-stripped names
    for variant in name_variants(name):
        try:
            results = mlb2.Mlb().get_people_id(variant)
            if results:
                pid = results[0]
                return pid, _team_for_pid(pid, name)
        except Exception:
            pass

    # Step 4 — statsapi lookup_player with accent-stripped name
    stripped = strip_accents(name)
    try:
        results = mlb.lookup_player(stripped)
        if results:
            pid = results[0]['id']
            return pid, _team_for_pid(pid, name)
    except Exception:
        pass

    # Step 5 — check mlb_roster table in DB (populated by sync_mlb_roster job)
    try:
        db = get_db()
        # Try exact match first, then accent-stripped match
        row = db.execute(
            'SELECT mlb_id, team FROM mlb_roster WHERE name=? OR name_ascii=?',
            (name, stripped)
        ).fetchone()
        db.close()
        if row:
            return row['mlb_id'], row['team']
    except Exception:
        pass

    print(f"[stat_fetcher] Could not find player: {name}")
    return 0, 'N/A'

# ── Game ID / boxscore fetching ────────────────────────────────────────────────

def get_game_ids(start_date_str, end_date_str, include_spring_training=False):
    game_ids = []
    current = datetime.strptime(start_date_str, '%Y-%m-%d')
    end     = datetime.strptime(end_date_str,   '%Y-%m-%d')
    sport_ids = [1]
    if include_spring_training:
        sport_ids.append(17)   # 17 = Spring Training
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        for sport_id in sport_ids:
            sched = mlb.get('schedule', {'date': date_str, 'sportId': sport_id})
            try:
                for game in sched['dates'][0]['games']:
                    gid = game['gamePk']
                    if gid not in game_ids:   # deduplicate
                        game_ids.append(gid)
            except (IndexError, KeyError):
                pass
        current += timedelta(days=1)
    return game_ids

def get_boxscores(game_ids):
    return [mlb.boxscore_data(gid) for gid in game_ids]

def _today_et():
    """Return today's date in US Eastern time — matches MLB schedule reference timezone."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt
        return _dt.now(ZoneInfo('America/New_York')).date()
    except Exception:
        try:
            import pytz
            from datetime import datetime as _dt
            return _dt.now(pytz.timezone('America/New_York')).date()
        except Exception:
            # Final fallback: UTC-4 (EDT) offset — safe enough
            from datetime import datetime as _dt, timezone, timedelta as _td
            return (_dt.now(timezone.utc) - _td(hours=4)).date()

def get_today_game_ids():
    today = _today_et().strftime('%Y-%m-%d')
    sched = mlb.get('schedule', {'date': today, 'sportId': 1})
    ids = []
    try:
        for game in sched['dates'][0]['games']:
            ids.append(game['gamePk'])
    except (IndexError, KeyError):
        pass
    return ids

def get_live_boxscore(game_id):
    """Returns boxscore_data plus live game status info."""
    data = mlb.boxscore_data(game_id)
    # Fetch live status
    try:
        live = mlb.get('game', {'gamePk': game_id})
        status = live['gameData']['status']['abstractGameState']  # Live/Final/Preview
        linescore = live['liveData']['linescore']
        inning_num = linescore.get('currentInning', '')
        inning_half = linescore.get('inningHalf', '')
        inning_str = f"{inning_half[:3]} {inning_num}" if inning_num else 'Scheduled'
        if status == 'Final':
            inning_str = 'Final'
        away_score = linescore.get('teams', {}).get('away', {}).get('runs', 0)
        home_score = linescore.get('teams', {}).get('home', {}).get('runs', 0)
        away_name = live['gameData']['teams']['away']['abbreviation']
        home_name = live['gameData']['teams']['home']['abbreviation']
        score_str = f"{away_name} {away_score} – {home_name} {home_score}"
        game_status = status.lower()  # 'live', 'final', 'preview'
        if game_status == 'preview':
            game_time = live['gameData']['datetime'].get('time', '')
            ampm = live['gameData']['datetime'].get('ampm', '')
            inning_str = f"{game_time} {ampm}"
            game_status = 'scheduled'
    except Exception as e:
        print(f"[stat_fetcher] live status error for {game_id}: {e}")
        away_name = data.get('teamInfo', {}).get('away', {}).get('abbreviation', '?')
        home_name = data.get('teamInfo', {}).get('home', {}).get('abbreviation', '?')
        score_str = f"{away_name} vs {home_name}"
        inning_str = ''
        game_status = 'unknown'

    return data, game_status, score_str, inning_str, away_name, home_name

# ── Stat accumulation ──────────────────────────────────────────────────────────

class BatterStats:
    def __init__(self):
        self.singles = self.doubles = self.triples = self.homeruns = 0
        self.ab = self.rbi = self.bb = self.sb = self.k = 0

    def update(self, stats):
        s = stats.get('batting', {})
        if not s:
            return
        h  = s.get('hits', 0)
        d  = s.get('doubles', 0)
        t  = s.get('triples', 0)
        hr = s.get('homeRuns', 0)
        self.doubles  += d
        self.triples  += t
        self.homeruns += hr
        self.singles  += h - d - t - hr
        self.ab       += s.get('atBats', 0)
        self.rbi      += s.get('rbi', 0)
        self.bb       += s.get('baseOnBalls', 0)
        self.sb       += s.get('stolenBases', 0)
        self.k        += s.get('strikeOuts', 0)

    @property
    def total_bases(self):
        return self.singles + 2*self.doubles + 3*self.triples + 4*self.homeruns

    @property
    def slg(self):
        return self.total_bases / self.ab if self.ab else 0

class PitcherStats:
    def __init__(self):
        self.ip_raw = 0.0   # fractional innings e.g. 6.2
        self.er = self.h = self.bb = 0
        self.sv = self.hd = self.bs = self.so = self.qs = 0

    def _add_ip(self, ip_str):
        """Convert '6.2' style IP to true decimal, accumulate, convert back."""
        try:
            val = float(ip_str)
            whole = int(val)
            thirds = round((val - whole) * 10)
            self.ip_raw += whole + thirds / 3.0
        except (ValueError, TypeError):
            pass

    @property
    def ip_display(self):
        whole = int(self.ip_raw)
        frac  = self.ip_raw - whole
        thirds = round(frac * 3)
        return whole + thirds * 0.1

    def update(self, stats):
        s = stats.get('pitching', {})
        if not s:
            return
        self._add_ip(s.get('inningsPitched', 0))
        self.er += s.get('earnedRuns', 0)
        self.h  += s.get('hits', 0)
        self.bb += s.get('baseOnBalls', 0)
        self.so += s.get('strikeOuts', 0)
        self.hd += s.get('holds', 0)
        self.bs += s.get('blownSaves', 0)
        try:
            ip_val = float(s.get('inningsPitched', 0))
            er_val = s.get('earnedRuns', 0)
            if ip_val >= 6.0 and er_val <= 3:
                self.qs += 1
        except (ValueError, TypeError):
            pass
        note = s.get('note', '')
        if note and 'S' in note:
            self.sv += 1

    @property
    def era(self):
        return (self.er / self.ip_raw) * 9 if self.ip_raw else 0

    @property
    def whip(self):
        return (self.h + self.bb) / self.ip_raw if self.ip_raw else 0

    @property
    def sv_hd_bs(self):
        return self.sv + self.hd - self.bs

def collect_stats_from_boxscores(boxscores, player_id, position_type):
    stat_obj = BatterStats() if position_type == 'batter' else PitcherStats()
    key = f"ID{player_id}"
    for bs in boxscores:
        all_players = {**bs.get('home', {}).get('players', {}),
                       **bs.get('away', {}).get('players', {})}
        if key in all_players:
            stat_obj.update(all_players[key].get('stats', {}))
    return stat_obj

# ── Weekly stat update ─────────────────────────────────────────────────────────

def _current_week():
    return _sched_current_week()

def _week_dates(week):
    return _sched_week_dates(week)

def run_stat_update():
    week = _current_week()
    start_str, end_str = _week_dates(week)
    print(f"[stat_fetcher] Updating week {week} ({start_str} – {end_str})")

    # Week 0 fetches Spring Training games in addition to regular season
    game_ids = get_game_ids(start_str, end_str, include_spring_training=(week == 0))

    boxscores = get_boxscores(game_ids)

    db = get_db()
    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()

    for manager in managers:
        lineup = db.execute('''
            SELECT l.position, l.player_id, p.name, p.mlb_id, p.position_type
            FROM lineups l
            JOIN players p ON l.player_id = p.id
            WHERE l.manager_id = ? AND l.week = ?
        ''', (manager['id'], week)).fetchall()

        for slot in lineup:
            mlb_id = slot['mlb_id']
            pos_type = slot['position_type']
            stats = collect_stats_from_boxscores(boxscores, mlb_id, pos_type)

            if pos_type == 'batter':
                db.execute('''
                    INSERT INTO weekly_stats
                        (manager_id, week, player_id, lineup_position,
                         singles, doubles, triples, homeruns, ab, total_bases, slg,
                         rbi, bb, sb, k)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(manager_id,week,player_id) DO UPDATE SET
                        singles=excluded.singles, doubles=excluded.doubles,
                        triples=excluded.triples, homeruns=excluded.homeruns,
                        ab=excluded.ab, total_bases=excluded.total_bases,
                        slg=excluded.slg, rbi=excluded.rbi, bb=excluded.bb,
                        sb=excluded.sb, k=excluded.k
                ''', (
                    manager['id'], week, slot['player_id'], slot['position'],
                    stats.singles, stats.doubles, stats.triples, stats.homeruns,
                    stats.ab, stats.total_bases, round(stats.slg, 4),
                    stats.rbi, stats.bb, stats.sb, stats.k
                ))
            else:
                db.execute('''
                    INSERT INTO weekly_stats
                        (manager_id, week, player_id, lineup_position,
                         ip, er, h, p_bb, h_plus_bb, sv, hd, bs,
                         era, whip, so, qs, sv_hd_bs)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(manager_id,week,player_id) DO UPDATE SET
                        ip=excluded.ip, er=excluded.er, h=excluded.h,
                        p_bb=excluded.p_bb, h_plus_bb=excluded.h_plus_bb,
                        sv=excluded.sv, hd=excluded.hd, bs=excluded.bs,
                        era=excluded.era, whip=excluded.whip, so=excluded.so,
                        qs=excluded.qs, sv_hd_bs=excluded.sv_hd_bs
                ''', (
                    manager['id'], week, slot['player_id'], slot['position'],
                    stats.ip_display, stats.er, stats.h, stats.bb,
                    stats.h + stats.bb, stats.sv, stats.hd, stats.bs,
                    round(stats.era, 4), round(stats.whip, 4),
                    stats.so, stats.qs, stats.sv_hd_bs
                ))

    _update_category_wins(db, week, managers)
    db.commit()
    db.close()
    print(f"[stat_fetcher] Week {week} update complete.")

def _display_ip_to_true(ip_display):
    """Convert display IP (6.2 = 6⅔ innings) to true decimal."""
    try:
        val = float(ip_display)
        whole = int(val)
        thirds = round((val - whole) * 10)
        return whole + thirds / 3.0
    except (TypeError, ValueError):
        return 0.0

def _update_category_wins(db, week, managers):
    """Recompute category wins for this week and write to category_wins."""
    if len(managers) < 2:
        return
    m1, m2 = managers[0], managers[1]

    def batters(mid):
        return db.execute('''
            SELECT * FROM weekly_stats ws JOIN players p ON ws.player_id=p.id
            WHERE ws.manager_id=? AND ws.week=? AND p.position_type='batter'
        ''', (mid, week)).fetchall()

    def pitchers(mid):
        return db.execute('''
            SELECT * FROM weekly_stats ws JOIN players p ON ws.player_id=p.id
            WHERE ws.manager_id=? AND ws.week=? AND p.position_type='pitcher'
        ''', (mid, week)).fetchall()

    def tot(rows, f):
        return sum(r[f] or 0 for r in rows)

    def avg_ip(rows, num):
        """Sum num / sum of true IP decimal."""
        n = sum(r[num] or 0 for r in rows)
        d = sum(_display_ip_to_true(r['ip']) for r in rows)
        return n / d if d else 0

    def avg_plain(rows, num, den):
        n = sum(r[num] or 0 for r in rows)
        d = sum(r[den] or 0 for r in rows)
        return n / d if d else 0

    b1, b2 = batters(m1['id']), batters(m2['id'])
    p1, p2 = pitchers(m1['id']), pitchers(m2['id'])

    cats = {
        'SLG':     (avg_plain(b1,'total_bases','ab'), avg_plain(b2,'total_bases','ab'), False),
        'RBI':     (tot(b1,'rbi'),                    tot(b2,'rbi'),                    False),
        'BB':      (tot(b1,'bb'),                     tot(b2,'bb'),                     False),
        'SB':      (tot(b1,'sb'),                     tot(b2,'sb'),                     False),
        'K':       (tot(b1,'k'),                      tot(b2,'k'),                      True),
        'ERA':     (avg_ip(p1,'er')*9,                avg_ip(p2,'er')*9,                True),
        'WHIP':    (avg_ip(p1,'h_plus_bb'),           avg_ip(p2,'h_plus_bb'),           True),
        'SO':      (tot(p1,'so'),                     tot(p2,'so'),                     False),
        'QS':      (tot(p1,'qs'),                     tot(p2,'qs'),                     False),
        'SV+HD-BS':(tot(p1,'sv_hd_bs'),              tot(p2,'sv_hd_bs'),               False),
    }

    w1 = w2 = 0
    for cat, (v1, v2, lower) in cats.items():
        if v1 == v2:
            w1 += 0.5; w2 += 0.5
        elif (lower and v1 < v2) or (not lower and v1 > v2):
            w1 += 1
        else:
            w2 += 1

    for manager, wins in [(m1['name'], w1), (m2['name'], w2)]:
        db.execute('''
            INSERT INTO category_wins (manager, week, wins) VALUES (?,?,?)
            ON CONFLICT(manager,week) DO UPDATE SET wins=excluded.wins
        ''', (manager, week, wins))

# ── Today's stats update ───────────────────────────────────────────────────────

def run_today_update():
    """Fetch today-only stats for all players in the current week's lineup."""
    print("[stat_fetcher] Updating today's stats...")
    week = _current_week()
    game_ids = get_today_game_ids()

    db = get_db()
    db.execute('DELETE FROM today_stats')

    managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()

    # Fetch all game data once
    game_info = {}       # gid -> (status, score, inning, away_abbr, home_abbr)
    live_boxscores = {}  # gid -> boxscore_data dict
    all_players_today = {}  # "ID{mlb_id}" -> (stats_dict, gid)

    for gid in game_ids:
        bs, status, score, inning, away, home = get_live_boxscore(gid)
        game_info[gid] = (status, score, inning, away, home)
        live_boxscores[gid] = bs
        # Index every player in this game by their ID key
        combined = {**bs.get('home', {}).get('players', {}),
                    **bs.get('away', {}).get('players', {})}
        for pid_key, pdata in combined.items():
            all_players_today[pid_key] = (pdata, gid)

    # Build team -> game map for the "Off" check (no game today)
    team_to_game = {}
    for gid in game_ids:
        _, _, _, away, home = game_info[gid]
        team_to_game[away.upper()] = gid
        team_to_game[home.upper()] = gid

    for manager in managers:
        lineup = db.execute('''
            SELECT l.player_id, p.name, p.mlb_id, p.position_type, p.team
            FROM lineups l JOIN players p ON l.player_id=p.id
            WHERE l.manager_id=? AND l.week=?
        ''', (manager['id'], week)).fetchall()

        for slot in lineup:
            key  = f"ID{slot['mlb_id']}"
            team = (slot['team'] or '').upper()

            # Find which game this player appeared in (by mlb_id, not team)
            if key in all_players_today:
                pdata, gid = all_players_today[key]
                status, score, inning, away, home = game_info[gid]
                opponent = home if team == away.upper() else away
            elif team in team_to_game:
                # Player's team is playing but player didn't appear (DNP)
                gid = team_to_game[team]
                status, score, inning, away, home = game_info[gid]
                opponent = home if team == away.upper() else away
                pdata = None
            else:
                # Team not playing today
                db.execute('''
                    INSERT OR REPLACE INTO today_stats
                    (manager_id, player_id, game_status, opponent, game_score, inning)
                    VALUES (?,?,'off','—','—','Off')
                ''', (manager['id'], slot['player_id']))
                continue

            if slot['position_type'] == 'batter':
                s = BatterStats()
                if pdata:
                    s.update(pdata.get('stats', {}))
                db.execute('''
                    INSERT OR REPLACE INTO today_stats
                    (manager_id, player_id, game_status, opponent, game_score, inning,
                     singles, doubles, triples, homeruns, ab, rbi, bb, sb, k)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''', (
                    manager['id'], slot['player_id'], status, opponent, score, inning,
                    s.singles, s.doubles, s.triples, s.homeruns,
                    s.ab, s.rbi, s.bb, s.sb, s.k
                ))
            else:
                s = PitcherStats()
                if pdata:
                    s.update(pdata.get('stats', {}))
                db.execute('''
                    INSERT OR REPLACE INTO today_stats
                    (manager_id, player_id, game_status, opponent, game_score, inning,
                     ip, er, h, p_bb, sv, hd, bs, so, qs)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''', (
                    manager['id'], slot['player_id'], status, opponent, score, inning,
                    s.ip_display, s.er, s.h, s.bb,
                    s.sv, s.hd, s.bs, s.so, s.qs
                ))

    db.commit()
    db.close()
    print("[stat_fetcher] Today's stats update complete.")
