// ============================================================
// Creative Maker — Dashboard (Redesigned)
// ============================================================

// -----------------------------------------------------------
// STATE
// -----------------------------------------------------------

let ws = null;
let reconnectTimer = null;
let timerInterval = null;
let timerStart = null;
let selectedPhases = [1, 2, 3];
let availableOutputs = [];   // [{slug, name, phase, icon, available}, ...]
let resultIndex = 0;         // current result being viewed
let loadedResults = [];       // [{slug, name, icon, data}, ...]
let pipelineRunning = false;

// How many agents total per phase selection
const AGENT_SLUGS = {
  1: ['agent_01a', 'agent_01b'],
  2: ['agent_02', 'agent_03'],
  3: ['agent_04', 'agent_05', 'agent_06', 'agent_07'],
};

// -----------------------------------------------------------
// VIEW MANAGEMENT
// -----------------------------------------------------------

function goToView(name) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  const target = document.getElementById(`view-${name}`);
  if (target) target.classList.add('active');

  // Update stepper
  const steps = ['brief', 'pipeline', 'results'];
  const idx = steps.indexOf(name);
  document.querySelectorAll('.stepper .step').forEach((s, i) => {
    s.classList.remove('active', 'done');
    if (i < idx) s.classList.add('done');
    if (i === idx) s.classList.add('active');
  });
  document.querySelectorAll('.stepper .step-line').forEach((l, i) => {
    l.classList.toggle('done', i < idx);
  });

  // If going to results, load them
  if (name === 'results') loadResults();
}

// -----------------------------------------------------------
// WEBSOCKET
// -----------------------------------------------------------

function connectWS() {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${protocol}//${location.host}/ws`);

  ws.onopen = () => {
    document.getElementById('ws-dot').classList.add('on');
    document.getElementById('ws-dot').classList.remove('off');
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  };

  ws.onclose = () => {
    document.getElementById('ws-dot').classList.remove('on');
    document.getElementById('ws-dot').classList.add('off');
    reconnectTimer = setTimeout(connectWS, 2000);
  };

  ws.onerror = () => ws.close();

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    handleMessage(msg);
  };
}

function handleMessage(msg) {
  switch (msg.type) {
    case 'state_sync':
      if (msg.running) {
        pipelineRunning = true;
        goToView('pipeline');
        startTimer();
        setRunDisabled(true);
      }
      (msg.completed_agents || []).forEach(slug => setCardState(slug, 'done'));
      if (msg.current_agent) setCardState(msg.current_agent, 'running');
      updateProgress();
      (msg.log || []).forEach(e => appendLog(e));
      break;

    case 'pipeline_start':
      pipelineRunning = true;
      resetAllCards();
      clearLog();
      goToView('pipeline');
      startTimer();
      document.getElementById('pipeline-title').textContent = 'Building your ads...';
      document.getElementById('pipeline-subtitle').textContent = 'The agents are working. This usually takes a few minutes.';
      break;

    case 'phase_start':
      appendLog({ time: ts(), level: 'info', message: `Phase ${msg.phase} started` });
      break;

    case 'agent_start':
      setCardState(msg.slug, 'running');
      updateProgress();
      appendLog({ time: ts(), level: 'info', message: `Starting ${msg.name}...` });
      scrollToCard(msg.slug);
      break;

    case 'agent_complete':
      setCardState(msg.slug, 'done', msg.elapsed);
      updateProgress();
      appendLog({ time: ts(), level: 'success', message: `${msg.name} completed (${msg.elapsed}s)` });
      break;

    case 'agent_error':
      setCardState(msg.slug, 'failed', null, msg.error);
      updateProgress();
      appendLog({ time: ts(), level: 'error', message: `${msg.name} failed: ${msg.error}` });
      break;

    case 'pipeline_complete':
      pipelineRunning = false;
      stopTimer();
      setRunDisabled(false);
      document.getElementById('pipeline-title').textContent = 'All done!';
      document.getElementById('pipeline-subtitle').textContent = `Pipeline finished in ${msg.elapsed}s. Click "Results" to browse the output.`;
      appendLog({ time: ts(), level: 'success', message: `Pipeline complete in ${msg.elapsed}s` });
      // Reset results header for fresh run
      document.getElementById('results-title').textContent = 'Your results are ready';
      document.getElementById('results-subtitle').textContent = 'Browse through each agent\'s output below.';
      // Auto-navigate to results after a brief pause
      setTimeout(() => goToView('results'), 1500);
      break;

    case 'pipeline_error':
      pipelineRunning = false;
      stopTimer();
      setRunDisabled(false);
      document.getElementById('pipeline-title').textContent = 'Pipeline stopped';
      document.getElementById('pipeline-subtitle').textContent = msg.message || 'An error occurred.';
      appendLog({ time: ts(), level: 'error', message: `Pipeline error: ${msg.message}` });
      break;
  }
}

// -----------------------------------------------------------
// PIPELINE CARDS
// -----------------------------------------------------------

function setCardState(slug, state, elapsed, error) {
  const card = document.getElementById(`card-${slug}`);
  if (!card) return;

  card.className = `agent-card ${state}`;
  const badge = card.querySelector('.status-badge');
  if (!badge) return;

  switch (state) {
    case 'running':
      badge.className = 'status-badge running';
      badge.innerHTML = '<span class="spinner"></span> Running';
      break;
    case 'done':
      badge.className = 'status-badge done';
      badge.textContent = elapsed ? `Done in ${elapsed}s` : 'Done';
      break;
    case 'failed':
      badge.className = 'status-badge failed';
      badge.textContent = 'Failed';
      break;
    default:
      badge.className = 'status-badge waiting';
      badge.textContent = 'Waiting';
  }
}

function resetAllCards() {
  document.querySelectorAll('.agent-card').forEach(card => {
    card.className = 'agent-card waiting';
    const badge = card.querySelector('.status-badge');
    if (badge) {
      badge.className = 'status-badge waiting';
      badge.textContent = 'Waiting';
    }
  });
  document.getElementById('progress-fill').style.width = '0%';
}

function scrollToCard(slug) {
  const card = document.getElementById(`card-${slug}`);
  if (card) card.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function updateProgress() {
  const doneCount = document.querySelectorAll('.agent-card.done').length;
  const total = document.querySelectorAll('.agent-card').length;
  const pct = total > 0 ? Math.round((doneCount / total) * 100) : 0;
  document.getElementById('progress-fill').style.width = pct + '%';
}

// -----------------------------------------------------------
// TIMER
// -----------------------------------------------------------

function startTimer() {
  timerStart = Date.now();
  stopTimer();
  timerInterval = setInterval(() => {
    const s = Math.floor((Date.now() - timerStart) / 1000);
    const m = Math.floor(s / 60);
    const sec = s % 60;
    document.getElementById('timer').textContent = `${m}:${sec.toString().padStart(2, '0')}`;
  }, 1000);
}

function stopTimer() {
  if (timerInterval) { clearInterval(timerInterval); timerInterval = null; }
}

// -----------------------------------------------------------
// LOG
// -----------------------------------------------------------

function appendLog(entry) {
  const box = document.getElementById('log-container');
  if (!box) return;
  const div = document.createElement('div');
  div.className = `log-entry ${entry.level || ''}`;
  div.innerHTML = `<span class="log-time">${entry.time || ''}</span><span class="log-msg">${esc(entry.message || '')}</span>`;
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}

function clearLog() {
  const box = document.getElementById('log-container');
  if (box) box.innerHTML = '';
}

function ts() {
  return new Date().toLocaleTimeString('en-US', { hour12: false });
}

function esc(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

// -----------------------------------------------------------
// BRIEF FORM
// -----------------------------------------------------------

const FIELDS = [
  ['f-brand',          'brand_name'],
  ['f-product',        'product_name'],
  ['f-description',    'product_description'],
  ['f-price',          'price_point'],
  ['f-niche',          'niche'],
  ['f-reviews',        'customer_reviews'],
  ['f-competitors',    'competitor_info'],
  ['f-landing',        'landing_page_info'],
  ['f-compliance',     'compliance_category'],
  ['f-context',        'additional_context'],
];

function populateForm(data) {
  for (const [id, key] of FIELDS) {
    const el = document.getElementById(id);
    if (!el || !(key in data)) continue;
    const val = data[key];
    el.value = Array.isArray(val) ? val.join('\n') : (val || '');
  }
}

function readForm() {
  const out = {};
  for (const [id, key] of FIELDS) {
    const el = document.getElementById(id);
    if (!el) continue;
    const val = el.value.trim();
    if (!val) continue;
    out[key] = val;
  }
  return out;
}

async function loadSample() {
  try {
    const resp = await fetch('/api/sample-input');
    const data = await resp.json();
    populateForm(data);
  } catch (e) {
    console.error('Failed to load sample', e);
  }
}

function clearBrief() {
  for (const [id] of FIELDS) {
    const el = document.getElementById(id);
    if (el) el.value = '';
  }
}

// -----------------------------------------------------------
// PHASE PICKER
// -----------------------------------------------------------

function selectPhases(btn) {
  document.querySelectorAll('.phase-btn').forEach(b => b.classList.remove('selected'));
  btn.classList.add('selected');
  selectedPhases = btn.dataset.phases.split(',').map(Number);
}

// -----------------------------------------------------------
// RUN PIPELINE
// -----------------------------------------------------------

function setRunDisabled(disabled) {
  const btn = document.getElementById('btn-run');
  if (btn) btn.disabled = disabled;
}

async function startPipeline() {
  const inputs = readForm();
  if (!inputs.brand_name || !inputs.product_name) {
    alert('Please fill in at least a Brand Name and Product Name.');
    return;
  }

  setRunDisabled(true);

  try {
    const resp = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ phases: selectedPhases, inputs }),
    });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      setRunDisabled(false);
    }
    // Pipeline view transition happens via WS message
  } catch (e) {
    alert('Failed to start pipeline: ' + e.message);
    setRunDisabled(false);
  }
}

// -----------------------------------------------------------
// RESULTS
// -----------------------------------------------------------

async function loadResults() {
  try {
    const resp = await fetch('/api/outputs');
    const outputs = await resp.json();
    availableOutputs = outputs.filter(o => o.available);

    if (availableOutputs.length === 0) {
      document.getElementById('result-content').innerHTML = '<div class="empty-state">No outputs yet. Run the pipeline first.</div>';
      document.getElementById('nav-agent-name').textContent = '—';
      document.getElementById('nav-counter').textContent = '0 of 0';
      return;
    }

    // Load all available outputs
    loadedResults = [];
    for (const o of availableOutputs) {
      try {
        const r = await fetch(`/api/outputs/${o.slug}`);
        const d = await r.json();
        loadedResults.push({ slug: o.slug, name: o.name, icon: o.icon, data: d.data });
      } catch (e) {
        console.error('Failed to load', o.slug, e);
      }
    }

    resultIndex = 0;
    showResult(resultIndex);
  } catch (e) {
    console.error('Failed to load results', e);
  }
}

function showResult(idx) {
  if (!loadedResults.length) return;
  resultIndex = Math.max(0, Math.min(idx, loadedResults.length - 1));
  const r = loadedResults[resultIndex];

  document.getElementById('nav-agent-name').textContent = `${r.icon}  ${r.name}`;
  document.getElementById('nav-counter').textContent = `${resultIndex + 1} of ${loadedResults.length}`;
  document.getElementById('nav-prev').disabled = resultIndex === 0;
  document.getElementById('nav-next').disabled = resultIndex === loadedResults.length - 1;

  const content = document.getElementById('result-content');
  content.innerHTML = renderOutput(r.data);
}

function prevResult() { showResult(resultIndex - 1); }
function nextResult() { showResult(resultIndex + 1); }

// -----------------------------------------------------------
// SMART OUTPUT RENDERER
// -----------------------------------------------------------

function renderOutput(data) {
  if (!data || typeof data !== 'object') {
    return `<div class="empty-state">No data to display.</div>`;
  }

  let html = '';
  for (const [key, val] of Object.entries(data)) {
    html += renderSection(key, val, 0);
  }
  return html;
}

function renderSection(key, val, depth) {
  if (val === null || val === undefined) return '';

  const title = humanize(key);

  // Simple string
  if (typeof val === 'string') {
    if (depth === 0) {
      return `<div class="out-section">
        <div class="out-heading">${esc(title)}</div>
        <p class="out-text">${escMultiline(val)}</p>
      </div>`;
    }
    return `<div class="out-field">
      <span class="out-field-key">${esc(title)}</span>
      <span class="out-field-val">${escMultiline(val)}</span>
    </div>`;
  }

  // Number
  if (typeof val === 'number') {
    if (depth === 0) {
      return `<div class="out-section">
        <div class="out-heading">${esc(title)}</div>
        <p class="out-text"><span class="out-number">${val}</span></p>
      </div>`;
    }
    return `<div class="out-field">
      <span class="out-field-key">${esc(title)}</span>
      <span class="out-field-val"><span class="out-number">${val}</span></span>
    </div>`;
  }

  // Boolean
  if (typeof val === 'boolean') {
    const label = val ? 'Yes' : 'No';
    const cls = val ? 'green' : 'red';
    return `<div class="out-field">
      <span class="out-field-key">${esc(title)}</span>
      <span class="out-field-val"><span class="out-badge ${cls}">${label}</span></span>
    </div>`;
  }

  // Array of strings
  if (Array.isArray(val) && val.length > 0 && val.every(v => typeof v === 'string')) {
    return `<div class="out-section">
      <div class="${depth === 0 ? 'out-heading' : 'out-subheading'}">${esc(title)}</div>
      <ul class="out-list">${val.map(s => `<li>${esc(s)}</li>`).join('')}</ul>
    </div>`;
  }

  // Array of objects
  if (Array.isArray(val) && val.length > 0 && typeof val[0] === 'object') {
    let inner = '';
    val.forEach((item, i) => {
      if (typeof item !== 'object' || item === null) {
        inner += `<p class="out-text">${esc(String(item))}</p>`;
        return;
      }
      const cardTitle = item.name || item.title || item.hook_text || item.headline || item.idea_name || item.concept_name || item.label || `#${i + 1}`;
      let fields = '';
      for (const [k, v] of Object.entries(item)) {
        if (['name', 'title', 'hook_text', 'headline', 'idea_name', 'concept_name', 'label'].includes(k) && v === cardTitle) continue;
        fields += renderCardField(k, v);
      }
      inner += `<div class="out-card"><div class="out-card-title">${esc(String(cardTitle))}</div>${fields}</div>`;
    });

    // If there are many items, wrap in a collapsible
    if (val.length > 4 && depth === 0) {
      return `<div class="out-section">
        <div class="out-heading">${esc(title)} <span class="out-badge purple">${val.length}</span></div>
        <div>${inner}</div>
      </div>`;
    }

    return `<div class="out-section">
      <div class="${depth === 0 ? 'out-heading' : 'out-subheading'}">${esc(title)} <span class="out-badge purple">${val.length}</span></div>
      ${inner}
    </div>`;
  }

  // Empty array
  if (Array.isArray(val) && val.length === 0) {
    return `<div class="out-field">
      <span class="out-field-key">${esc(title)}</span>
      <span class="out-field-val" style="color:var(--text-dim)">None</span>
    </div>`;
  }

  // Plain object
  if (typeof val === 'object' && !Array.isArray(val)) {
    let inner = '';
    for (const [k, v] of Object.entries(val)) {
      inner += renderSection(k, v, depth + 1);
    }

    if (depth === 0) {
      return `<div class="out-section">
        <div class="out-heading">${esc(title)}</div>
        ${inner}
      </div>`;
    }

    // Nested object — use collapsible
    return `<div class="out-collapse" onclick="this.classList.toggle('open')">
      <div class="out-collapse-toggle">${esc(title)}</div>
      <div class="out-collapse-body">${inner}</div>
    </div>`;
  }

  return '';
}

function renderCardField(key, val) {
  const label = humanize(key);

  if (val === null || val === undefined) return '';

  if (typeof val === 'string') {
    // Check for score-like fields
    if (key.includes('score') || key.includes('rating')) {
      return `<div class="out-field">
        <span class="out-field-key">${esc(label)}</span>
        <span class="out-field-val"><span class="out-badge yellow">${esc(val)}</span></span>
      </div>`;
    }
    return `<div class="out-field">
      <span class="out-field-key">${esc(label)}</span>
      <span class="out-field-val">${escMultiline(val)}</span>
    </div>`;
  }

  if (typeof val === 'number') {
    const cls = key.includes('score') || key.includes('rating') ? 'yellow' : 'purple';
    return `<div class="out-field">
      <span class="out-field-key">${esc(label)}</span>
      <span class="out-field-val"><span class="out-badge ${cls}">${val}</span></span>
    </div>`;
  }

  if (typeof val === 'boolean') {
    return `<div class="out-field">
      <span class="out-field-key">${esc(label)}</span>
      <span class="out-field-val"><span class="out-badge ${val ? 'green' : 'red'}">${val ? 'Yes' : 'No'}</span></span>
    </div>`;
  }

  if (Array.isArray(val)) {
    if (val.length === 0) return '';
    if (val.every(v => typeof v === 'string')) {
      return `<div class="out-field" style="flex-direction:column;gap:4px">
        <span class="out-field-key">${esc(label)}</span>
        <ul class="out-list">${val.map(s => `<li>${esc(String(s))}</li>`).join('')}</ul>
      </div>`;
    }
    // Nested array of objects — render inline
    let inner = '';
    val.forEach((item, i) => {
      if (typeof item === 'object' && item !== null) {
        const subTitle = item.name || item.title || item.label || `#${i + 1}`;
        let fields = '';
        for (const [k, v] of Object.entries(item)) {
          if (v === subTitle) continue;
          fields += renderCardField(k, v);
        }
        inner += `<div class="out-card" style="margin-left:0">${subTitle !== `#${i + 1}` ? `<div class="out-card-title">${esc(String(subTitle))}</div>` : ''}${fields}</div>`;
      } else {
        inner += `<p class="out-text">${esc(String(item))}</p>`;
      }
    });
    return `<div style="margin:8px 0">
      <div class="out-subheading">${esc(label)} <span class="out-badge purple">${val.length}</span></div>
      ${inner}
    </div>`;
  }

  if (typeof val === 'object') {
    let inner = '';
    for (const [k, v] of Object.entries(val)) {
      inner += renderCardField(k, v);
    }
    return `<div style="margin:8px 0">
      <div class="out-subheading">${esc(label)}</div>
      ${inner}
    </div>`;
  }

  return `<div class="out-field">
    <span class="out-field-key">${esc(label)}</span>
    <span class="out-field-val">${esc(String(val))}</span>
  </div>`;
}

// -----------------------------------------------------------
// HELPERS
// -----------------------------------------------------------

function humanize(key) {
  // snake_case → Title Case, with some smart replacements
  const map = {
    'voc': 'VoC',
    'cta': 'CTA',
    'tof': 'Top of Funnel',
    'mof': 'Mid Funnel',
    'bof': 'Bottom of Funnel',
    'ugc': 'UGC',
    'roi': 'ROI',
    'cpm': 'CPM',
    'ctr': 'CTR',
    'roas': 'ROAS',
    'p1': 'Pass 1',
    'p2': 'Pass 2',
    'url': 'URL',
    'id': 'ID',
    'ai': 'AI',
  };

  return key
    .replace(/_/g, ' ')
    .replace(/\b\w+/g, word => {
      const lower = word.toLowerCase();
      if (map[lower]) return map[lower];
      return word.charAt(0).toUpperCase() + word.slice(1);
    });
}

function escMultiline(str) {
  // Escape HTML and convert newlines to <br>
  return esc(str).replace(/\n/g, '<br>');
}

// -----------------------------------------------------------
// HISTORY
// -----------------------------------------------------------

let historyOpen = false;
let renameRunId = null;

function toggleHistory() {
  const panel = document.getElementById('history-panel');
  historyOpen = !historyOpen;
  if (historyOpen) {
    panel.classList.remove('hidden');
    loadHistory();
  } else {
    panel.classList.add('hidden');
  }
}

async function loadHistory() {
  const list = document.getElementById('history-list');
  list.innerHTML = '<div class="empty-state">Loading...</div>';

  try {
    const resp = await fetch('/api/runs?limit=50');
    const runs = await resp.json();

    if (!runs.length) {
      list.innerHTML = '<div class="empty-state">No runs yet. Start your first pipeline run!</div>';
      return;
    }

    list.innerHTML = runs.map(r => {
      const displayName = r.label || r.brand_name || `Run #${r.id}`;
      const dateStr = r.created_at || '';
      const elapsed = r.elapsed_seconds ? `${r.elapsed_seconds}s` : '—';
      const phases = r.phases || '';

      return `
        <div class="run-card" onclick="loadHistoryRun(${r.id})">
          <div class="run-card-top">
            <span class="run-card-label">${esc(displayName)}</span>
            <span class="run-card-status ${r.status}">${r.status}</span>
          </div>
          <div class="run-card-meta">
            <span>${esc(dateStr)}</span>
            <span>Phases ${esc(phases)}</span>
            <span>${r.agent_count} agent${r.agent_count !== 1 ? 's' : ''}</span>
            <span>${elapsed}</span>
          </div>
          <div class="run-card-actions" onclick="event.stopPropagation()">
            <button class="btn btn-ghost btn-sm" onclick="openRename(${r.id}, '${esc(r.label || '')}')">Rename</button>
            <button class="btn btn-ghost btn-sm" onclick="deleteHistoryRun(${r.id})">Delete</button>
          </div>
        </div>
      `;
    }).join('');
  } catch (e) {
    list.innerHTML = '<div class="empty-state">Failed to load history.</div>';
    console.error('Failed to load history', e);
  }
}

async function loadHistoryRun(runId) {
  try {
    const resp = await fetch(`/api/runs/${runId}`);
    const run = await resp.json();

    if (!run.agents || !run.agents.length) {
      alert('This run has no agent outputs.');
      return;
    }

    // Close history panel
    toggleHistory();

    // Load the run's outputs into the results viewer
    loadedResults = run.agents
      .filter(a => a.status === 'completed' && a.data)
      .map(a => ({
        slug: a.agent_slug,
        name: a.agent_name,
        icon: getAgentIcon(a.agent_slug),
        data: a.data,
      }));

    if (!loadedResults.length) {
      alert('This run has no completed outputs to view.');
      return;
    }

    const runLabel = run.label || run.inputs?.brand_name || `Run #${run.id}`;
    document.getElementById('results-title').textContent = runLabel;
    document.getElementById('results-subtitle').textContent = `${run.created_at} — ${run.agents.length} agents — ${run.elapsed_seconds || '?'}s`;

    resultIndex = 0;
    showResult(0);
    goToView('results');
  } catch (e) {
    console.error('Failed to load run', e);
    alert('Failed to load this run.');
  }
}

function getAgentIcon(slug) {
  const icons = {
    agent_01a: '🔬', agent_01b: '📡',
    agent_02: '💡', agent_03: '🔍',
    agent_04: '✍️', agent_05: '🎣',
    agent_06: '🔍', agent_07: '🔀',
  };
  return icons[slug] || '📄';
}

async function deleteHistoryRun(runId) {
  if (!confirm(`Delete run #${runId}? This can't be undone.`)) return;
  try {
    await fetch(`/api/runs/${runId}`, { method: 'DELETE' });
    loadHistory();
  } catch (e) {
    console.error('Delete failed', e);
  }
}

function openRename(runId, currentLabel) {
  renameRunId = runId;
  document.getElementById('rename-input').value = currentLabel;
  document.getElementById('rename-dialog').classList.remove('hidden');
  document.getElementById('rename-input').focus();
}

function closeRename() {
  document.getElementById('rename-dialog').classList.add('hidden');
  renameRunId = null;
}

async function saveRename() {
  if (!renameRunId) return;
  const label = document.getElementById('rename-input').value.trim();
  try {
    await fetch(`/api/runs/${renameRunId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label }),
    });
    closeRename();
    loadHistory();
  } catch (e) {
    console.error('Rename failed', e);
  }
}

// Handle Enter key in rename dialog
document.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && renameRunId) saveRename();
  if (e.key === 'Escape' && renameRunId) closeRename();
  if (e.key === 'Escape' && historyOpen) toggleHistory();
});

// -----------------------------------------------------------
// INIT
// -----------------------------------------------------------

connectWS();
loadSample(); // Pre-populate form with sample brand

// Check if there are existing outputs on page load
fetch('/api/outputs')
  .then(r => r.json())
  .then(outputs => {
    if (outputs.some(o => o.available)) {
      // Show indicator on Results step
      const resultsStep = document.querySelector('.step[data-step="results"]');
      if (resultsStep) resultsStep.title = 'Previous results available';
    }
  })
  .catch(() => {});
