# Fantasy Baseball Web App

A Flask-based web app replacing the Google Sheets frontend and AWS Lambda backend.

## Stack
- **Flask** — web framework
- **SQLite** — file-based database (no separate server needed)
- **APScheduler** — runs stat updates every 15 min, today's stats every 1 min, roster sync every 24 hr
- **MLB-StatsAPI==1.9.0** and **python-mlb-statsapi==0.7.2** — MLB data
- **Render.com** — recommended hosting (free tier with persistent disk)

---

## Local Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create the database schema
python init_db.py

# 3. Edit seed_db.py — fill in your actual permanent/backup player names
#    (the PERMANENT_PLAYERS and BACKUP_PLAYERS dicts at the top)
python seed_db.py

# 4. Run the app
python app.py
# → http://localhost:5000
```

---

## Deploying to Render

1. Push this folder to a GitHub repo.
2. Go to [render.com](https://render.com) → New → Web Service → connect your repo.
3. Render will auto-detect `render.yaml` and configure everything.
4. The SQLite file is stored on a persistent `/data` disk (1 GB, free tier).
5. Set the `DB_PATH` env var to `/data/fantasy.db` (already in `render.yaml`).

After first deploy, SSH into the Render shell and run:
```bash
python init_db.py
python seed_db.py
```

---

## Pages

| Route | Description |
|-------|-------------|
| `/` | Season overview: total wins, per-week grid, Today's Stats, permanent rosters |
| `/week/<n>` | Weekly stat tables with green/red highlighting and category winners |
| `/lineups` | Lineup entry with real-time validation (red = reused or conflicted player) |
| `/roster` | Permanent and backup player display |

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/week/<n>` | GET | Returns raw weekly stats as JSON |
| `/api/today` | GET | Returns today's live stats for all players |
| `/api/validate_player` | POST | Checks if a player is valid for a given manager/week |
| `/api/set_lineup` | POST | Saves a lineup slot to the database |
| `/api/swap_permanent` | POST | Permanently swaps in a backup player |

---

## Background Jobs

| Job | Frequency | Description |
|-----|-----------|-------------|
| `run_stat_update` | Every 15 min | Fetches cumulative weekly stats from MLB API → SQLite |
| `run_today_update` | Every 1 min | Fetches today-only stats with live game status → SQLite |
| `sync_mlb_roster` | Every 24 hr (+ startup) | Syncs full MLB active roster for autocomplete search |

All jobs run automatically when the Flask app starts (via APScheduler).
The roster sync also runs once on startup if the `mlb_roster` table is empty.

---

## Player Search / Autocomplete

The lineup entry page has a live search dropdown for each slot. It:
- Queries `/api/roster_search` as you type (debounced 200ms, min 2 chars)
- Filters by **position type** automatically (e.g. the C slot only shows catchers, SP only shows starting pitchers)
- Searches both accented names (Cristopher Sánchez) and plain ASCII (Cristopher Sanchez)
- Data comes from the `mlb_roster` table, updated every 24 hours

To manually trigger a roster refresh (e.g. after the trade deadline):
```bash
# Via API (POST to admin endpoint):
curl -X POST http://localhost:5000/api/roster_sync

# Or directly:
python -c "from jobs.roster_sync import sync_mlb_roster; sync_mlb_roster()"
```

---

## Adding Players Mid-Season

If a player isn't found by the MLB API lookup, add them to `MISSING_PLAYERS` 
in `jobs/stat_fetcher.py` (same pattern as the original Lambda function):

```python
MISSING_PLAYERS = {
    "Player Name": {"player_id": 123456, "team": "NYY"},
    ...
}
```

---

## Database Schema

See `init_db.py` for the full schema. Key tables:

- `managers` — Noah, Erik
- `players` — all players with MLB ID, team, position type
- `permanent_players` — each manager's 6 permanent + 6 backup players
- `lineups` — one row per roster slot per week per manager
- `weekly_stats` — cumulative stats updated every 15 min
- `today_stats` — today-only stats with game status, wiped and rewritten each minute
- `category_wins` — summary of wins per manager per week
