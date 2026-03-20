/**
 * lineups.js
 * - Autocomplete: batters unrestricted by position; pitchers show SP/RP label
 * - Conflict highlighting: used / in_lineup / other_week
 * - Same-lineup duplicate prevention
 * - Remove (×) button per slot
 * - Permanent player quick-fill with correct slot targeting
 */

document.addEventListener('DOMContentLoaded', () => {
  document.querySelectorAll('.lineup-input').forEach(initInput);
  document.querySelectorAll('.perm-qf-btn').forEach(initPermBtn);
  document.querySelectorAll('.slot-remove-btn').forEach(initRemoveBtn);
});

// ── Input init ─────────────────────────────────────────────────────────────────
function initInput(input) {
  const dropdown  = input.parentElement.querySelector('.autocomplete-dropdown');
  let debounce    = null;
  // Start committed=true for ANY pre-filled input so blur doesn't re-save it.
  // Permanent inputs especially must never be re-saved as is_permanent=0.
  let committed   = input.value.trim() !== '';

  if (input.value.trim()) {
    input.classList.contains('is-permanent') ? setPermState(input) : setValid(input);
  }

  input.addEventListener('input', () => {
    committed = false;
    clearValidation(input);
    const q = input.value.trim();
    if (q.length < 2) { hideDropdown(dropdown); return; }
    clearTimeout(debounce);
    debounce = setTimeout(() => fetchSuggestions(input, dropdown, q), 200);
  });

  input.addEventListener('focus', () => {
    if (!committed && input.value.trim().length >= 2)
      fetchSuggestions(input, dropdown, input.value.trim());
  });

  input.addEventListener('blur', () => {
    setTimeout(() => hideDropdown(dropdown), 200);
    if (!committed && input.value.trim())
      validateAndSave(input, input.value.trim()).then(ok => { if (ok) committed = true; });
  });

  input.addEventListener('keydown', e => {
    const items = [...dropdown.querySelectorAll('.ac-item:not(.ac-item-blocked)')];
    const hi    = dropdown.querySelector('.ac-item.highlighted');
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      const next = hi ? items[items.indexOf(hi) + 1] : items[0];
      hi?.classList.remove('highlighted'); next?.classList.add('highlighted');
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      const prev = hi ? items[items.indexOf(hi) - 1] : items[items.length - 1];
      hi?.classList.remove('highlighted'); prev?.classList.add('highlighted');
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const hi = dropdown.querySelector('.ac-item.highlighted:not(.ac-item-blocked)');
      if (hi) hi.click();
    } else if (e.key === 'Escape') {
      hideDropdown(dropdown);
    }
  });
}

// ── Autocomplete fetch ─────────────────────────────────────────────────────────
async function fetchSuggestions(input, dropdown, q) {
  const posType   = input.dataset.positionType || '';  // 'batter' or 'pitcher'
  const manager   = input.dataset.manager;
  const week      = input.dataset.week;

  // No position filter — just filter by batter/pitcher type
  const params = new URLSearchParams({
    q,
    position_type: posType,
    manager,
    week,
    limit: 12,
  });

  try {
    const res     = await fetch(`/api/roster_search?${params}`);
    const players = await res.json();
    renderDropdown(dropdown, players, input);
  } catch(e) { console.error('autocomplete error', e); }
}

// ── Render dropdown ────────────────────────────────────────────────────────────
function renderDropdown(dropdown, players, input) {
  if (!players.length) {
    dropdown.innerHTML = '<div class="ac-empty">No players found</div>';
    dropdown.style.display = 'block';
    return;
  }

  dropdown.innerHTML = players.map(p => {
    // 'used' and 'in_lineup' are blocked (not selectable)
    const blocked = p.conflict === 'used' || p.conflict === 'in_lineup' || p.conflict === 'other_perm';
    let cls = '';
    let note = '';
    let nameStyle = '';

    if (p.conflict === 'used') {
      cls = 'ac-item-conflict-used ac-item-blocked';
      note = '<span class="ac-conflict-note">Already used</span>';
      nameStyle = 'text-decoration:line-through';
    } else if (p.conflict === 'in_lineup') {
      cls = 'ac-item-conflict-used ac-item-blocked';
      note = '<span class="ac-conflict-note">Already in lineup</span>';
      nameStyle = 'text-decoration:line-through';
    } else if (p.conflict === 'other_perm') {
      cls = 'ac-item-conflict-used ac-item-blocked';
      note = '<span class="ac-conflict-note">Other manager\'s permanent</span>';
      nameStyle = 'text-decoration:line-through';
    } else if (p.conflict === 'other_week') {
      cls = 'ac-item-conflict-other';
      note = '<span class="ac-conflict-note ac-conflict-other">Other manager</span>';
    }

    return `
      <div class="ac-item ${cls}" data-name="${escHtml(p.name)}" data-mlb-id="${p.mlb_id||''}" data-blocked="${blocked}">
        <span class="ac-name" style="${nameStyle}">${escHtml(p.name)}</span>
        <span class="ac-meta">${escHtml(p.team||'')} &middot; ${escHtml(p.position||'')}</span>
        ${note}
      </div>`;
  }).join('');

  dropdown.querySelectorAll('.ac-item').forEach(item => {
    // preventDefault on mousedown stops the input losing focus before click fires
    item.addEventListener('mousedown', e => e.preventDefault());
    item.addEventListener('click', () => {
      if (item.dataset.blocked === 'true') return;
      const name   = item.dataset.name;
      const mlb_id = item.dataset.mlbId || '';
      input.value = name;
      input.dataset.selectedMlbId = mlb_id;
      hideDropdown(dropdown);
      validateAndSave(input, name, false, mlb_id);
    });
  });

  dropdown.style.display = 'block';
}

function hideDropdown(d) { d.style.display = 'none'; }

// ── Validate + save ────────────────────────────────────────────────────────────
async function validateAndSave(input, playerName, isPermanent = false, mlbId = '') {
  if (isPermanent) {
    setPermState(input);
    await saveLineupSlot(input, playerName, true, mlbId);
    return true;
  }
  const valid = await validatePlayer(input, playerName);
  if (valid) await saveLineupSlot(input, playerName, false, mlbId);
  return valid;
}

async function validatePlayer(input, playerName) {
  setLoading(input);
  try {
    const res = await fetch('/api/validate_player', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        player_name: playerName,
        manager:     input.dataset.manager,
        week:        parseInt(input.dataset.week),
        position:    input.dataset.position,
      }),
    });
    const data = await res.json();
    if (data.valid) { setValid(input); return true; }
    else            { setInvalid(input, data.reason || 'Invalid'); return false; }
  } catch { setInvalid(input, 'Server error'); return false; }
}

async function saveLineupSlot(input, playerName, isPermanent = false, mlbId = '') {
  try {
    const res = await fetch('/api/set_lineup', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        manager:      input.dataset.manager,
        week:         parseInt(input.dataset.week),
        position:     input.dataset.position,
        player_name:  playerName,
        mlb_id:       mlbId || input.dataset.selectedMlbId || '',
        is_permanent: isPermanent ? 1 : 0,
      }),
    });
    const data = await res.json();
    if (!data.success) setInvalid(input, data.reason || 'Save failed');
  } catch { console.error('save error'); }
}

// ── Remove button ──────────────────────────────────────────────────────────────
function initRemoveBtn(btn) {
  btn.addEventListener('click', async () => {
    const row      = btn.closest('.lineup-row');
    const input    = row.querySelector('.lineup-input');
    const manager  = input.dataset.manager;
    const week     = parseInt(input.dataset.week);
    const position = input.dataset.position;

    try {
      await fetch('/api/remove_lineup', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ manager, week, position }),
      });
    } catch(e) { console.error('remove error', e); }

    input.value = '';
    input.dataset.isPermanent = '0';
    clearValidation(input);
  });
}

// ── Permanent player quick-fill buttons ───────────────────────────────────────
function initPermBtn(btn) {
  btn.addEventListener('click', () => {
    if (btn.classList.contains('swapped-out')) return;

    const manager    = btn.dataset.manager;
    const playerName = btn.dataset.player;
    const mlbId      = btn.dataset.mlbId || '';
    const posType    = btn.dataset.posType;
    const position   = (btn.dataset.position || '').toUpperCase();
    const panel      = btn.closest('.lineup-panel');
    let target       = null;

    if (posType === 'pitcher') {
      if (position === 'RP' || position === 'CP') {
        target = _firstEmpty(panel, ['RP-0','RP-1','RP-2']);
        if (!target) target = panel.querySelector('[data-position="RP-0"]');
      } else {
        target = _firstEmpty(panel, ['SP-0','SP-1','SP-2','SP-3','SP-4']);
        if (!target) target = panel.querySelector('[data-position="SP-0"]');
      }
    } else {
      const candidates = {
        'C':  ['C-0'],
        '1B': ['1B-0'],
        '2B': ['2B-0'],
        '3B': ['3B-0'],
        'SS': ['SS-0'],
        'OF': ['OF-0','OF-1','OF-2'],
        'LF': ['OF-0','OF-1','OF-2'],
        'CF': ['OF-0','OF-1','OF-2'],
        'RF': ['OF-0','OF-1','OF-2'],
        'DH': ['DH-0'],
        'IF': ['3B-0','2B-0','SS-0','1B-0'],
        'UT': ['DH-0','1B-0'],
      }[position] || ['DH-0'];
      target = _firstEmpty(panel, candidates);
      if (!target) target = panel.querySelector(`[data-position="${candidates[0]}"]`);
    }

    if (!target) return;
    target.value = playerName;
    target.dataset.selectedMlbId = mlbId;
    target.classList.add('is-permanent');
    target.dataset.isPermanent = '1';
    setPermState(target);
    saveLineupSlot(target, playerName, true, mlbId);
  });
}

function _firstEmpty(panel, keys) {
  for (const k of keys) {
    const inp = panel.querySelector(`[data-position="${k}"]`);
    if (inp && !inp.value.trim()) return inp;
  }
  return null;
}

// ── Visual state helpers ───────────────────────────────────────────────────────
function clearValidation(input) {
  input.classList.remove('is-valid','is-invalid','is-loading','is-permanent');
  const icon = getIcon(input);
  if (icon) { icon.textContent = ''; icon.style.color = ''; }
}
function setLoading(input) {
  input.classList.remove('is-valid','is-invalid'); input.classList.add('is-loading');
  const icon = getIcon(input);
  if (icon) icon.textContent = '…';
}
function setValid(input) {
  input.classList.remove('is-invalid','is-loading'); input.classList.add('is-valid');
  const icon = getIcon(input);
  if (icon) { icon.textContent = '✓'; icon.style.color = 'var(--green)'; }
}
function setPermState(input) {
  input.classList.remove('is-invalid','is-loading','is-valid');
  input.classList.add('is-permanent');
  const icon = getIcon(input);
  if (icon) { icon.textContent = '★'; icon.style.color = 'var(--accent2)'; }
}
function setInvalid(input, reason) {
  input.classList.remove('is-valid','is-loading'); input.classList.add('is-invalid');
  const icon = getIcon(input);
  if (icon) { icon.textContent = '✗'; icon.style.color = 'var(--red)'; }
  input.title = reason;
}
function getIcon(input) {
  return input.closest('.lineup-row')?.querySelector('.validation-icon');
}
function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
