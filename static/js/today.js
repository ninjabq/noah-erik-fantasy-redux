/**
 * today.js — fetches /api/today and renders the Today's Stats panel.
 */

const SLOT_ORDER = [
  'C-0','1B-0','2B-0','3B-0','SS-0','OF-0','OF-1','OF-2','DH-0',
  'SP-0','SP-1','SP-2','SP-3','SP-4','RP-0','RP-1','RP-2'
];

function slotDisplay(key) {
  return key ? key.split('-')[0] : '—';
}

/**
 * Start auto-refreshing today's stats.
 * Returns the interval handle so the caller can cancel it (e.g. when switching to yesterday).
 */
function initTodayStats(intervalMs = 60000) {
  fetchAndRender();
  return setInterval(fetchAndRender, intervalMs);
}

async function fetchAndRender() {
  try {
    const res  = await fetch('/api/today');
    const data = await res.json();
    renderToday(data);
    document.getElementById('today-updated').textContent =
      'Updated ' + new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
  } catch(e) {
    console.error('today fetch error', e);
  }
}

function renderToday(data) {
  const container = document.getElementById('today-container');
  if (!container) return;
  const managers = Object.keys(data);
  if (!managers.length) {
    container.innerHTML = '<div class="today-loading">No lineup data for today.</div>';
    return;
  }
  container.innerHTML = managers.map(mgr => {
    const players  = data[mgr];
    const batters  = players.filter(p => p.position_type === 'batter');
    const pitchers = players.filter(p => p.position_type === 'pitcher');
    return `
      <div class="today-manager-panel">
        <div class="today-mgr-name">${mgr}</div>
        ${batters.length  ? renderBatterTable(batters)  : ''}
        ${pitchers.length ? renderPitcherTable(pitchers) : ''}
      </div>`;
  }).join('');
}

function statusBadge(status, inning) {
  const cls = {
    live: 'status-live', final: 'status-final',
    scheduled: 'status-scheduled', off: 'status-off',
  }[status] || 'status-scheduled';
  const label = status === 'live'  ? `🔴 ${inning}` :
                status === 'final' ? 'Final' :
                status === 'off'   ? 'Off'   :
                inning || 'Sched';
  return `<span class="status-badge ${cls}">${label}</span>`;
}

function renderBatterTable(batters) {
  const rows = batters.map(p => {
    const hits = (p.singles||0) + (p.doubles||0) + (p.triples||0) + (p.homeruns||0);
    const pos  = slotDisplay(p.lineup_position);
    return `
      <tr>
        <td class="pos-cell">${escHtml(pos)}</td>
        <td class="name-cell">${escHtml(p.name)}</td>
        <td class="team-cell">${escHtml(p.team||'—')}</td>
        <td>${statusBadge(p.game_status, p.inning)}</td>
        <td class="inning-cell" title="${escHtml(p.game_score||'')}">${abbrevScore(p.game_score, p.opponent)}</td>
        <td>${hits}-${p.ab||0}</td>
        <td>${p.homeruns||0}</td>
        <td>${p.rbi||0}</td>
        <td>${p.bb||0}</td>
        <td>${p.sb||0}</td>
        <td>${p.k||0}</td>
      </tr>`;
  }).join('');
  return `
    <div class="table-label" style="margin-top:.75rem">Hitters</div>
    <div class="table-scroll">
      <table class="today-table">
        <thead>
          <tr>
            <th>POS</th><th>Player</th><th>Team</th><th>Status</th><th>Game</th>
            <th>H-AB</th><th>HR</th><th>RBI</th><th>BB</th><th>SB</th><th>K</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function renderPitcherTable(pitchers) {
  const rows = pitchers.map(p => {
    const pos = slotDisplay(p.lineup_position);
    return `
      <tr>
        <td class="pos-cell">${escHtml(pos)}</td>
        <td class="name-cell">${escHtml(p.name)}</td>
        <td class="team-cell">${escHtml(p.team||'—')}</td>
        <td>${statusBadge(p.game_status, p.inning)}</td>
        <td class="inning-cell" title="${escHtml(p.game_score||'')}">${abbrevScore(p.game_score, p.opponent)}</td>
        <td>${p.ip||0}</td>
        <td>${p.er||0}</td>
        <td>${p.h||0}</td>
        <td>${p.p_bb||0}</td>
        <td>${p.so||0}</td>
        <td>${p.sv||0}</td>
        <td>${p.hd||0}</td>
        <td>${p.bs||0}</td>
      </tr>`;
  }).join('');
  return `
    <div class="table-label" style="margin-top:.75rem">Pitchers</div>
    <div class="table-scroll">
      <table class="today-table">
        <thead>
          <tr>
            <th>POS</th><th>Player</th><th>Team</th><th>Status</th><th>Game</th>
            <th>IP</th><th>ER</th><th>H</th><th>BB</th><th>SO</th>
            <th>SV</th><th>HD</th><th>BS</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

function abbrevScore(scoreStr, opponent) {
  if (!scoreStr || scoreStr === '—') return escHtml(opponent || '—');
  return escHtml(scoreStr.length > 18 ? scoreStr.slice(0, 18) + '…' : scoreStr);
}

function escHtml(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
