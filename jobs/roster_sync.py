"""
jobs/roster_sync.py

Fetches all active MLB players from the Stats API and writes them into the
mlb_roster table. Called once on app startup and then every 24 hours so
call-ups / trades / DFA moves stay current.

Position mapping:
  Pitchers:  P, SP, RP, CP  → position_type='pitcher'
  Everyone else              → position_type='batter'
"""

import os, sqlite3, unicodedata, json
from datetime import date

import statsapi as mlb

DB_PATH = os.environ.get('DB_PATH', 'fantasy.db')

PITCHER_POSITIONS = {'P', 'SP', 'RP', 'CP', 'RL', 'CL', 'MR', 'SU', 'SW', 'RS'}

# All pitcher subtypes → normalised to 'SP' or 'RP' for clean storage.
# 'P' alone (generic pitcher) is treated as SP since the MLB API only assigns
# 'P' to two-way players and true starters; relievers always get a relief code.
POSITION_MAP = {
    'P':  'SP', 'SP': 'SP',
    'RP': 'RP', 'CP': 'RP', 'RL': 'RP', 'CL': 'RP',
    'MR': 'RP', 'SU': 'RP', 'SW': 'RP', 'RS': 'RP',
    'C':  'C',  '1B': '1B', '2B': '2B', '3B': '3B',
    'SS': 'SS', 'LF': 'OF', 'CF': 'OF', 'RF': 'OF',
    'OF': 'OF', 'DH': 'DH', 'IF': 'IF', 'UT': 'UT',
}


def strip_accents(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')


def _get_roster_for_team(team_id):
    """
    Fetch players for a team across multiple roster types and merge them.
    Returns a deduplicated list of roster entry dicts keyed by person.id.
    Uses direct HTTP requests for reliability.
    """
    import requests

    # Fetch these roster types and merge — nonRosterInvitees adds Spring Training
    # call-ups who aren't yet on the 40-man roster
    roster_types = ('fullRoster', 'nonRosterInvitees')
    seen_ids = set()
    all_entries = []

    for roster_type in roster_types:
        try:
            url = (
                f'https://statsapi.mlb.com/api/v1/teams/{team_id}/roster'
                f'?rosterType={roster_type}'
            )
            resp = requests.get(url, timeout=10)
            if not resp.ok:
                continue
            for entry in resp.json().get('roster', []):
                pid = entry.get('person', {}).get('id')
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    all_entries.append(entry)
        except Exception:
            continue

    # Fallback to statsapi if requests failed entirely
    if not all_entries:
        for endpoint in ('team_roster', 'roster'):
            for roster_type in ('fullRoster', '40Man', 'active'):
                try:
                    data = mlb.get(endpoint, {'teamId': team_id, 'rosterType': roster_type})
                    entries = data.get('roster', [])
                    if entries:
                        return entries
                except Exception:
                    continue

    return all_entries


def sync_mlb_roster():
    """
    Pull all players for every MLB team and upsert into mlb_roster.
    Safe to run repeatedly — uses INSERT OR REPLACE.
    """
    print("[roster_sync] Starting MLB roster sync...")
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt
        today = _dt.now(ZoneInfo('America/New_York')).date().isoformat()
    except Exception:
        today = date.today().isoformat()

    # Fetch all MLB teams
    try:
        teams_data = mlb.get('teams', {'sportId': 1, 'activeStatus': 'Y'})
        all_teams = teams_data.get('teams', [])
        # Filter to MLB teams only (sport id 1), exclude MiLB affiliates
        teams = [t for t in all_teams
                 if t.get('sport', {}).get('id') == 1]
        print(f"[roster_sync] Found {len(teams)} MLB teams")
    except Exception as e:
        print(f"[roster_sync] Failed to fetch teams: {e}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    total = 0
    for team in teams:
        team_id   = team['id']
        team_abbr = team.get('abbreviation', '').upper()
        team_full = team.get('name', '')

        entries = _get_roster_for_team(team_id)
        if not entries:
            print(f"[roster_sync] No roster data for {team_abbr} (team_id={team_id})")
            continue

        for entry in entries:
            person   = entry.get('person', {})
            pos_info = entry.get('position', {})

            mlb_id = person.get('id')
            name   = person.get('fullName', '')
            pos_abbr = pos_info.get('abbreviation', '').upper()

            if not mlb_id or not name:
                continue

            position      = POSITION_MAP.get(pos_abbr, pos_abbr)
            position_type = 'pitcher' if pos_abbr in PITCHER_POSITIONS else 'batter'
            name_ascii    = strip_accents(name)

            # Sanitise: any unknown pitcher code becomes 'RP', unknown batter becomes 'OF'
            if position_type == 'pitcher' and position not in ('SP', 'RP'):
                position = 'RP'
            elif position_type == 'batter' and position not in ('C','1B','2B','3B','SS','OF','DH','IF','UT'):
                position = 'OF'

            conn.execute('''
                INSERT INTO mlb_roster
                    (mlb_id, name, name_ascii, team, team_full, position, position_type, position_pinned, last_updated)
                VALUES (?,?,?,?,?,?,?,0,?)
                ON CONFLICT(mlb_id) DO UPDATE SET
                    name=excluded.name,
                    name_ascii=excluded.name_ascii,
                    team=excluded.team,
                    team_full=excluded.team_full,
                    -- Only update position/position_type if this row wasn't manually pinned
                    position=CASE WHEN position_pinned=1 THEN position ELSE excluded.position END,
                    position_type=CASE WHEN position_pinned=1 THEN position_type ELSE excluded.position_type END,
                    last_updated=excluded.last_updated
            ''', (mlb_id, name, name_ascii, team_abbr, team_full,
                  position, position_type, today))
            total += 1

        conn.commit()
        print(f"[roster_sync]   {team_abbr}: {len(entries)} players")

    conn.close()
    print(f"[roster_sync] Sync complete — {total} players upserted.")


if __name__ == '__main__':
    sync_mlb_roster()
