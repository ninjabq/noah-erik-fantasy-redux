"""
week_schedule.py — single source of truth for the 2026 season week definitions.

Week 1:  Wed Mar 25 – Sun Apr 5   (opening series + first full week combined)
Week 2:  Mon Apr 6  – Sun Apr 12
...
All-Star break exception:
  The short week Mon Jul 13 – Wed Jul 15 is merged with the following week,
  creating a single "week" spanning Thu Jul 16 – Sun Jul 26.

This module exposes:
  WEEKS        — ordered list of (week_num, start_date, end_date)
  current_week()  — returns the week number for today
  week_dates(n)   — returns (start_str, end_str) for week n
  total_weeks()   — number of weeks in the season
"""

from datetime import date, timedelta

# Build the schedule explicitly so every edge case is clear.
# Format: (week_number, start_date, end_date)
def _build_schedule():
    weeks = []

    # Week 0: debug / pre-season window (Mar 14 – Mar 24)
    weeks.append((0, date(2026, 3, 14), date(2026, 3, 24)))

    # Week 1: opening day Wed Mar 25 through Sun Apr 5
    weeks.append((1, date(2026, 3, 25), date(2026, 4, 5)))

    # Regular Mon–Sun weeks starting Apr 6
    week_num = 2
    monday = date(2026, 4, 6)

    while monday.month <= 10:  # season ends in October
        sunday = monday + timedelta(days=6)

        # All-Star break: skip Mon Jul 13 – Wed Jul 15 entirely,
        # and merge Thu Jul 16 – Sun Jul 26 into one extended week.
        if monday == date(2026, 7, 13):
            extended_end = date(2026, 7, 26)
            weeks.append((week_num, date(2026, 7, 16), extended_end))
            week_num += 1
            monday = date(2026, 7, 27)
            continue

        weeks.append((week_num, monday, sunday))
        week_num += 1
        monday += timedelta(weeks=1)

        if monday > date(2026, 10, 31):
            break

    return weeks

WEEKS = _build_schedule()
_BY_NUMBER = {w[0]: (w[1], w[2]) for w in WEEKS}


def week_dates(n):
    """Return (start_str, end_str) for week n."""
    if n == 0:
        # Week 0 fetches same date range as week 1 for stats (debug purposes)
        s, e = _BY_NUMBER[0]
        return s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d')
    if n not in _BY_NUMBER:
        raise ValueError(f"Unknown week number: {n}")
    s, e = _BY_NUMBER[n]
    return s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d')


def _today_et():
    """Return today's date in US Eastern time."""
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

def current_week():
    """Return the current week number based on today's date (ET)."""
    today = _today_et()
    for wnum, start, end in WEEKS:
        if start <= today <= end:
            return wnum
    # Before week 0 starts
    if today < WEEKS[0][1]:
        return 0
    # After season ends
    return WEEKS[-1][0]


def total_weeks():
    return len(WEEKS)


def all_week_options():
    """Return list of (week_num, label) for dropdowns."""
    result = []
    for wnum, start, end in WEEKS:
        try:
            label = f"Week {wnum}  ({start.strftime('%b %-d')} – {end.strftime('%b %-d')})"
        except ValueError:
            # Windows doesn't support %-d
            label = f"Week {wnum}  ({start.strftime('%b %d').lstrip('0')} – {end.strftime('%b %d').lstrip('0')})"
        result.append((wnum, label))
    return result
