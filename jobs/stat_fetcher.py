"""
Background job that replaces AWS Lambda + Google Sheets backend.
Fetches MLB stats via mlbstatsapi / statsapi and writes to SQLite.
"""

import os, sqlite3, unicodedata
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeout
from datetime import datetime, timedelta, date

import statsapi as mlb
import mlbstatsapi as mlb2

from week_schedule import current_week as _sched_current_week, week_dates as _sched_week_dates

DB_PATH = os.environ.get('DB_PATH', 'fantasy.db')

# How long (seconds) to wait for a locked DB before giving up.
# 30 s is generous — jobs run at 1 min / 15 min intervals so contention is brief.
DB_TIMEOUT = 30

# MLB API call timeout in seconds.  statsapi uses requests internally;
# we monkey-patch the session timeout so no call can block forever.
API_TIMEOUT = 20

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
    "Emmanuel Clase":     {"player_id": 667555, "team": "CLE"},
    "Cristopher Sanchez": {"player_id": 656945, "team": "PHI"},
    "Cristopher Sánchez": {"player_id": 656945, "team": "PHI"},
    "Jhoan Duran":        {"player_id": 661395, "team": "PHI"},
    "Devin Williams":     {"player_id": 669203, "team": "NYY"},
    "Mason Miller":       {"player_id": 695243, "team": "SD"},
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

# ── Timeout helper ─────────────────────────────────────────────────────────────

class _TimeoutError(Exception):
    pass

def _call_with_timeout(seconds, label, fn, *args, **kwargs):
    """
    Run fn(*args, **kwargs) in a ThreadPoolExecutor and raise _TimeoutError if
    it doesn't complete within `seconds`.  Works from any thread (APScheduler
    workers, Flask threads, daemon threads) — unlike SIGALRM which only works
    on the main thread of the main interpreter.
    """
    with ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(fn, *args, **kwargs)
        try:
            return future.result(timeout=seconds)
        except _FuturesTimeout:
            raise _TimeoutError(f"{label} timed out after {seconds}s")

@contextmanager
def _timeout(seconds, label='operation'):
    """
    No-op context manager kept for call-site compatibility.
    Actual per-call timeouts are enforced via _call_with_timeout() inside
    each API helper below.
    """
    yield


# ── Unicode helpers ────────────────────────────────────────────────────────────

def strip_accents(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

def name_variants(name):
    variants = [name]
    stripped = strip_accents(name)
    if stripped != name:
        variants.append(stripped)
    return variants

# ── DB helpers ─────────────────────────────────────────────────────────────────

def get_db():
    """
    Open the SQLite database with WAL journal mode and a generous lock timeout.
    WAL lets readers proceed even while a writer holds the lock, which prevents
    the most common 'database is locked' scenarios in a multi-job environment.
    """
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT)
    conn.row_factory = sqlite3.Row
    # WAL mode: survives process restart and allows concurrent reads during writes.
    conn.execute('PRAGMA journal_mode=WAL')
    # Busy timeout as belt-and-suspenders (in ms) — matches DB_TIMEOUT above.
    conn.execute(f'PRAGMA busy_timeout={DB_TIMEOUT * 1000}')
    return conn

# ── Player lookup ──────────────────────────────────────────────────────────────

def _team_for_pid(pid, name):
    try:
        def _fetch():
            return mlb.lookup_team(
                mlb.player_stat_data(pid)['current_team']
            )[0]['fileCode'].upper()
        return _call_with_timeout(API_TIMEOUT, 'team lookup', _fetch)
    except (IndexError, KeyError, _TimeoutError):
        return TEAM_OVERRIDES.get(name, 'N/A')

def get_player_id_and_team(name):
    if name in MISSING_PLAYERS:
        d = MISSING_PLAYERS[name]
        return d["player_id"], d["team"]

    for variant in name_variants(name):
        try:
            results = _call_with_timeout(API_TIMEOUT, f'mlbstatsapi lookup {variant}',
                                         mlb2.Mlb().get_people_id, variant)
            if results:
                pid = results[0]
                return pid, _team_for_pid(pid, name)
        except (Exception, _TimeoutError):
            pass

    stripped = strip_accents(name)
    try:
        results = _call_with_timeout(API_TIMEOUT, f'statsapi lookup {stripped}',
                                     mlb.lookup_player, stripped)
        if results:
            pid = results[0]['id']
            return pid, _team_for_pid(pid, name)
    except (Exception, _TimeoutError):
        pass

    try:
        db = get_db()
        try:
            row = db.execute(
                'SELECT mlb_id, team FROM mlb_roster WHERE name=? OR name_ascii=?',
                (name, stripped)
            ).fetchone()
        finally:
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
    current  = datetime.strptime(start_date_str, '%Y-%m-%d')
    end      = datetime.strptime(end_date_str,   '%Y-%m-%d')
    sport_ids = [1] + ([17] if include_spring_training else [])
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        for sport_id in sport_ids:
            try:
                sched = _call_with_timeout(API_TIMEOUT, f'schedule fetch {date_str}',
                                           mlb.get, 'schedule', {'date': date_str, 'sportId': sport_id})
                for game in sched['dates'][0]['games']:
                    gid = game['gamePk']
                    if gid not in game_ids:
                        game_ids.append(gid)
            except (IndexError, KeyError):
                pass
            except _TimeoutError:
                print(f"[stat_fetcher] Schedule fetch timed out for {date_str} sport={sport_id}")
        current += timedelta(days=1)
    return game_ids

def get_game_ids_for_date(date_str):
    try:
        sched = _call_with_timeout(API_TIMEOUT, f'schedule fetch {date_str}',
                                   mlb.get, 'schedule', {'date': date_str, 'sportId': 1})
        ids = []
        for game in sched['dates'][0]['games']:
            ids.append(game['gamePk'])
        return ids
    except (IndexError, KeyError):
        return []
    except _TimeoutError:
        print(f"[stat_fetcher] Schedule fetch timed out for {date_str}")
        return []

def get_boxscores(game_ids):
    boxscores = []
    for gid in game_ids:
        try:
            boxscores.append(_call_with_timeout(API_TIMEOUT, f'boxscore {gid}',
                                                mlb.boxscore_data, gid))
        except _TimeoutError:
            print(f"[stat_fetcher] Boxscore fetch timed out for game {gid} — skipping")
        except Exception as e:
            print(f"[stat_fetcher] Boxscore error for game {gid}: {e} — skipping")
    return boxscores

def _today_et():
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
            from datetime import datetime as _dt, timezone, timedelta as _td
            return (_dt.now(timezone.utc) - _td(hours=4)).date()

def get_live_boxscore(game_id):
    try:
        data = _call_with_timeout(API_TIMEOUT, f'boxscore_data {game_id}',
                                  mlb.boxscore_data, game_id)
    except _TimeoutError:
        print(f"[stat_fetcher] boxscore_data timed out for {game_id}")
        data = {}

    try:
        live = _call_with_timeout(API_TIMEOUT, f'game live {game_id}',
                                  mlb.get, 'game', {'gamePk': game_id})
        status    = live['gameData']['status']['abstractGameState']
        linescore = live['liveData']['linescore']
        inning_num  = linescore.get('currentInning', '')
        inning_half = linescore.get('inningHalf', '')
        inning_str  = f"{inning_half[:3]} {inning_num}" if inning_num else 'Scheduled'
        if status == 'Final':
            inning_str = 'Final'
        away_score = linescore.get('teams', {}).get('away', {}).get('runs', 0)
        home_score = linescore.get('teams', {}).get('home', {}).get('runs', 0)
        away_name  = live['gameData']['teams']['away']['abbreviation']
        home_name  = live['gameData']['teams']['home']['abbreviation']
        score_str  = f"{away_name} {away_score} – {home_name} {home_score}"
        game_status = status.lower()
        if game_status == 'preview':
            game_time  = live['gameData']['datetime'].get('time', '')
            ampm       = live['gameData']['datetime'].get('ampm', '')
            inning_str = f"{game_time} {ampm}"
            game_status = 'scheduled'
    except (Exception, _TimeoutError) as e:
        print(f"[stat_fetcher] live status error for {game_id}: {e}")
        away_name   = data.get('teamInfo', {}).get('away', {}).get('abbreviation', '?')
        home_name   = data.get('teamInfo', {}).get('home', {}).get('abbreviation', '?')
        score_str   = f"{away_name} vs {home_name}"
        inning_str  = ''
        game_status = 'unknown'

    return data, game_status, score_str, inning_str, away_name, home_name

# ── Stat accumulation ──────────────────────────────────────────────────────────

class BatterStats:
    def __init__(self):
        self.singles = self.doubles = self.triples = self.homeruns = 0
        self.ab = self.rbi = self.bb = self.sb = self.k = 0

    def update(self, stats):
        s = stats.get('batting', {})
        if not s: return
        h  = s.get('hits', 0)
        d  = s.get('doubles', 0)
        t  = s.get('triples', 0)
        hr = s.get('homeRuns', 0)
        self.doubles  += d; self.triples  += t; self.homeruns += hr
        self.singles  += h - d - t - hr
        self.ab  += s.get('atBats', 0);  self.rbi += s.get('rbi', 0)
        self.bb  += s.get('baseOnBalls', 0)
        self.sb  += s.get('stolenBases', 0)
        self.k   += s.get('strikeOuts', 0)

    @property
    def total_bases(self):
        return self.singles + 2*self.doubles + 3*self.triples + 4*self.homeruns

    @property
    def slg(self):
        return self.total_bases / self.ab if self.ab else 0

class PitcherStats:
    def __init__(self):
        self.ip_raw = 0.0
        self.er = self.h = self.bb = 0
        self.sv = self.hd = self.bs = self.so = self.qs = 0

    def _add_ip(self, ip_str):
        try:
            val    = float(ip_str)
            whole  = int(val)
            thirds = round((val - whole) * 10)
            self.ip_raw += whole + thirds / 3.0
        except (ValueError, TypeError):
            pass

    @property
    def ip_display(self):
        whole  = int(self.ip_raw)
        frac   = self.ip_raw - whole
        thirds = round(frac * 3)
        return whole + thirds * 0.1

    def update(self, stats):
        s = stats.get('pitching', {})
        if not s: return
        self._add_ip(s.get('inningsPitched', 0))
        self.er += s.get('earnedRuns', 0)
        self.h  += s.get('hits', 0)
        self.bb += s.get('baseOnBalls', 0)
        self.so += s.get('strikeOuts', 0)
        self.hd += s.get('holds', 0)
        self.bs += s.get('blownSaves', 0)
        try:
            if float(s.get('inningsPitched', 0)) >= 6.0 and s.get('earnedRuns', 0) <= 3:
                self.qs += 1
        except (ValueError, TypeError):
            pass
        if s.get('note', '') and 'S' in s.get('note', ''):
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

def _display_ip_to_true(ip_display):
    try:
        val    = float(ip_display)
        whole  = int(val)
        thirds = round((val - whole) * 10)
        return whole + thirds / 3.0
    except (TypeError, ValueError):
        return 0.0

def run_stat_update(week=None):
    """
    Fetch and store cumulative stats for all lineup slots for the given week.
    If week is None, defaults to the current week.
    Passing an explicit week number allows historical recalculation (e.g. from
    the /api/stat_update endpoint when the app has been down).
    """
    if week is None:
        week = _sched_current_week()
    start_str, end_str = _sched_week_dates(week)
    print(f"[stat_fetcher] Updating week {week} ({start_str} – {end_str})")

    # Fetch all game data BEFORE opening the DB — keeps the DB lock window minimal
    game_ids  = get_game_ids(start_str, end_str, include_spring_training=(week == 0))
    boxscores = get_boxscores(game_ids)

    db = get_db()
    try:
        managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()
        for manager in managers:
            lineup = db.execute('''
                SELECT l.position, l.player_id, p.name, p.mlb_id, p.position_type
                FROM lineups l JOIN players p ON l.player_id=p.id
                WHERE l.manager_id=? AND l.week=?
            ''', (manager['id'], week)).fetchall()

            for slot in lineup:
                stats = collect_stats_from_boxscores(boxscores, slot['mlb_id'], slot['position_type'])
                if slot['position_type'] == 'batter':
                    db.execute('''
                        INSERT INTO weekly_stats
                            (manager_id,week,player_id,lineup_position,
                             singles,doubles,triples,homeruns,ab,total_bases,slg,
                             rbi,bb,sb,k)
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
                            (manager_id,week,player_id,lineup_position,
                             ip,er,h,p_bb,h_plus_bb,sv,hd,bs,era,whip,so,qs,sv_hd_bs)
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
        print(f"[stat_fetcher] Week {week} update complete.")
    except Exception as e:
        print(f"[stat_fetcher] run_stat_update error: {e}")
        raise
    finally:
        db.close()

def _update_category_wins(db, week, managers):
    if len(managers) < 2:
        return
    m1, m2 = managers[0], managers[1]

    def batters(mid):
        return db.execute('''SELECT * FROM weekly_stats ws JOIN players p
            ON ws.player_id=p.id WHERE ws.manager_id=? AND ws.week=? AND p.position_type='batter'
            ''', (mid, week)).fetchall()

    def pitchers(mid):
        return db.execute('''SELECT * FROM weekly_stats ws JOIN players p
            ON ws.player_id=p.id WHERE ws.manager_id=? AND ws.week=? AND p.position_type='pitcher'
            ''', (mid, week)).fetchall()

    def tot(rows, f): return sum(r[f] or 0 for r in rows)
    def avg_ip(rows, num):
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
        'RBI':     (tot(b1,'rbi'),   tot(b2,'rbi'),   False),
        'BB':      (tot(b1,'bb'),    tot(b2,'bb'),     False),
        'SB':      (tot(b1,'sb'),    tot(b2,'sb'),     False),
        'K':       (tot(b1,'k'),     tot(b2,'k'),      True),
        'ERA':     (avg_ip(p1,'er')*9,  avg_ip(p2,'er')*9,  True),
        'WHIP':    (avg_ip(p1,'h_plus_bb'), avg_ip(p2,'h_plus_bb'), True),
        'SO':      (tot(p1,'so'),    tot(p2,'so'),     False),
        'QS':      (tot(p1,'qs'),    tot(p2,'qs'),     False),
        'SV+HD-BS':(tot(p1,'sv_hd_bs'), tot(p2,'sv_hd_bs'), False),
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
        db.execute('''INSERT INTO category_wins (manager,week,wins) VALUES (?,?,?)
            ON CONFLICT(manager,week) DO UPDATE SET wins=excluded.wins''',
            (manager, week, wins))

# ── Today's stats update ───────────────────────────────────────────────────────

def _build_player_game_index(game_ids):
    """
    Fetch all games for the given IDs and return:
      game_info:   {gid: (status, score, inning, away, home)}
      all_players: {"ID{mlb_id}": (pdata, gid)}
      team_to_game:{team_abbr: gid}
    """
    game_info    = {}
    all_players  = {}
    team_to_game = {}

    for gid in game_ids:
        bs, status, score, inning, away, home = get_live_boxscore(gid)
        game_info[gid] = (status, score, inning, away, home)
        combined = {**bs.get('home', {}).get('players', {}),
                    **bs.get('away', {}).get('players', {})}
        for pid_key, pdata in combined.items():
            all_players[pid_key] = (pdata, gid)
        team_to_game[away.upper()] = gid
        team_to_game[home.upper()] = gid

    return game_info, all_players, team_to_game

def run_today_update():
    """Fetch today-only stats for all players in the current week's lineup."""
    print("[stat_fetcher] Updating today's stats...")
    week      = _sched_current_week()
    today_str = _today_et().strftime('%Y-%m-%d')

    # ── Fetch ALL game data before touching the DB ─────────────────────────────
    game_ids = get_game_ids_for_date(today_str)
    game_info, all_players, team_to_game = _build_player_game_index(game_ids)

    # ── Single short DB transaction ────────────────────────────────────────────
    db = get_db()
    try:
        db.execute('DELETE FROM today_stats')
        managers = db.execute('SELECT * FROM managers ORDER BY id').fetchall()

        for manager in managers:
            lineup = db.execute('''
                SELECT l.player_id, p.name, p.mlb_id, p.position_type, p.team
                FROM lineups l JOIN players p ON l.player_id=p.id
                WHERE l.manager_id=? AND l.week=?
            ''', (manager['id'], week)).fetchall()

            for slot in lineup:
                key  = f"ID{slot['mlb_id']}"
                team = (slot['team'] or '').upper()

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
                    db.execute('''INSERT OR REPLACE INTO today_stats
                        (manager_id,player_id,game_status,opponent,game_score,inning)
                        VALUES (?,?,'off','—','—','Off')''',
                        (manager['id'], slot['player_id']))
                    continue

                if slot['position_type'] == 'batter':
                    s = BatterStats()
                    if pdata: s.update(pdata.get('stats', {}))
                    db.execute('''INSERT OR REPLACE INTO today_stats
                        (manager_id,player_id,game_status,opponent,game_score,inning,
                         singles,doubles,triples,homeruns,ab,rbi,bb,sb,k)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
                        manager['id'], slot['player_id'], status, opponent, score, inning,
                        s.singles, s.doubles, s.triples, s.homeruns,
                        s.ab, s.rbi, s.bb, s.sb, s.k
                    ))
                else:
                    s = PitcherStats()
                    if pdata: s.update(pdata.get('stats', {}))
                    db.execute('''INSERT OR REPLACE INTO today_stats
                        (manager_id,player_id,game_status,opponent,game_score,inning,
                         ip,er,h,p_bb,sv,hd,bs,so,qs)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
                        manager['id'], slot['player_id'], status, opponent, score, inning,
                        s.ip_display, s.er, s.h, s.bb,
                        s.sv, s.hd, s.bs, s.so, s.qs
                    ))

        db.commit()
        print("[stat_fetcher] Today's stats update complete.")
    except Exception as e:
        print(f"[stat_fetcher] run_today_update error: {e}")
        raise
    finally:
        db.close()   # ALWAYS released, even if MLB API or DB write fails

def run_permanent_stats_update():
    """
    Daily job: fetch full-season stats for every permanent and backup player,
    regardless of whether they appear in any lineup.  Writes into the
    `permanent_stats` table (schema created by app.py on startup).

    This is what powers the Season Stats table on the Roster page.
    It runs the complete date range from the first week of the season through
    today so results are always current even if the app was down for days.
    """
    from week_schedule import WEEKS
    print("[stat_fetcher] Updating permanent player season stats...")

    # Date range: season start → today
    season_start = WEEKS[1][1].strftime('%Y-%m-%d')   # Week 1 start
    season_end   = _today_et().strftime('%Y-%m-%d')

    # Fetch all games for the full season-to-date — this is slow but runs only once/day
    game_ids  = get_game_ids(season_start, season_end)
    boxscores = get_boxscores(game_ids)

    db = get_db()
    try:
        # Get all permanent/backup players with their mlb_id and position_type
        perm_players = db.execute('''
            SELECT DISTINCT p.id as player_id, p.mlb_id, p.position_type
            FROM permanent_players pp
            JOIN players p ON pp.player_id = p.id
        ''').fetchall()

        for player in perm_players:
            stats = collect_stats_from_boxscores(
                boxscores, player['mlb_id'], player['position_type']
            )
            if player['position_type'] == 'batter':
                db.execute('''
                    INSERT INTO permanent_stats
                        (player_id, singles, doubles, triples, homeruns,
                         ab, total_bases, slg, rbi, bb, sb, k)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(player_id) DO UPDATE SET
                        singles=excluded.singles, doubles=excluded.doubles,
                        triples=excluded.triples, homeruns=excluded.homeruns,
                        ab=excluded.ab, total_bases=excluded.total_bases,
                        slg=excluded.slg, rbi=excluded.rbi, bb=excluded.bb,
                        sb=excluded.sb, k=excluded.k,
                        last_updated=CURRENT_TIMESTAMP
                ''', (
                    player['player_id'],
                    stats.singles, stats.doubles, stats.triples, stats.homeruns,
                    stats.ab, stats.total_bases, round(stats.slg, 4),
                    stats.rbi, stats.bb, stats.sb, stats.k
                ))
            else:
                db.execute('''
                    INSERT INTO permanent_stats
                        (player_id, ip, er, h, p_bb, h_plus_bb,
                         sv, hd, bs, era, whip, so, qs, sv_hd_bs)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(player_id) DO UPDATE SET
                        ip=excluded.ip, er=excluded.er, h=excluded.h,
                        p_bb=excluded.p_bb, h_plus_bb=excluded.h_plus_bb,
                        sv=excluded.sv, hd=excluded.hd, bs=excluded.bs,
                        era=excluded.era, whip=excluded.whip, so=excluded.so,
                        qs=excluded.qs, sv_hd_bs=excluded.sv_hd_bs,
                        last_updated=CURRENT_TIMESTAMP
                ''', (
                    player['player_id'],
                    stats.ip_display, stats.er, stats.h, stats.bb,
                    stats.h + stats.bb, stats.sv, stats.hd, stats.bs,
                    round(stats.era, 4), round(stats.whip, 4),
                    stats.so, stats.qs, stats.sv_hd_bs
                ))

        db.commit()
        print(f"[stat_fetcher] Permanent stats update complete ({len(perm_players)} players).")
    except Exception as e:
        print(f"[stat_fetcher] run_permanent_stats_update error: {e}")
        raise
    finally:
        db.close()

def run_yesterday_update():
    """Fetch yesterday's final stats (used by /api/yesterday — does not persist)."""
    pass  # logic lives in the API endpoint; this stub satisfies the import
