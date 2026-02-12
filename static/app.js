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
let selectedPhases = [1, 2, 3]; // Always run full pipeline — phase gates handle the pausing
// Models are now hardcoded per-agent in config.py:
//   1A  = google/gemini-3.0-pro-deep-research
//   1A2 = anthropic/claude-opus-4-6
//   1B  = google/gemini-3.0-pro-deep-research
let availableOutputs = [];   // [{slug, name, phase, icon, available}, ...]
let resultIndex = 0;         // current result being viewed
let loadedResults = [];       // [{slug, name, icon, data}, ...]
let pipelineRunning = false;

// How many agents total per phase selection
const AGENT_SLUGS = {
  1: ['agent_01a', 'agent_01a2', 'agent_01b'],
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
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  };

  ws.onclose = () => {
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
      clearPreviewCache();
      clearLog();
      resetCost();
      goToView('pipeline');
      startTimer();
      showAbortButton(true);
      {
        const isQuick = document.getElementById('cb-quick-mode')?.checked;
        document.getElementById('pipeline-title').textContent = isQuick
          ? 'Quick test run...'
          : 'Building your ads...';
        document.getElementById('pipeline-subtitle').textContent = isQuick
          ? 'Running with standard LLM calls (no deep research). This should be fast.'
          : 'The agents are working. This usually takes a few minutes.';
      }
      break;

    case 'pipeline_aborting':
      {
        const abortBtn = document.getElementById('btn-abort');
        if (abortBtn) {
          abortBtn.textContent = 'Stopping...';
          abortBtn.disabled = true;
          abortBtn.classList.add('aborting');
        }
        document.getElementById('pipeline-subtitle').textContent = 'Stopping pipeline...';
        appendLog({ time: ts(), level: 'warning', message: msg.message });
      }
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
      updateCost(msg.cost);
      appendLog({ time: ts(), level: 'success', message: `${msg.name} completed (${msg.elapsed}s)` });
      break;

    case 'agent_error':
      setCardState(msg.slug, 'failed', null, msg.error);
      updateProgress();
      appendLog({ time: ts(), level: 'error', message: `${msg.name} failed: ${msg.error}` });
      break;

    case 'phase_gate':
      showPhaseGate(msg.completed_phase, msg.next_phase, msg.message);
      appendLog({ time: ts(), level: 'info', message: msg.message });
      break;

    case 'phase_gate_cleared':
      hidePhaseGate();
      break;

    case 'pipeline_complete':
      pipelineRunning = false;
      stopTimer();
      setRunDisabled(false);
      showAbortButton(false);
      updateCost(msg.cost);
      {
        const costStr = msg.cost ? (msg.cost.total_cost >= 0.01 ? `$${msg.cost.total_cost.toFixed(2)}` : `$${msg.cost.total_cost.toFixed(4)}`) : '';
        const costSuffix = costStr ? ` | Cost: ${costStr}` : '';
        document.getElementById('pipeline-title').textContent = 'All done!';
        document.getElementById('pipeline-subtitle').textContent = `Pipeline finished in ${msg.elapsed}s${costSuffix}. Click "Results" to browse the output.`;
      }
      appendLog({ time: ts(), level: 'success', message: `Pipeline complete in ${msg.elapsed}s` });
      // Reset results header for fresh run
      document.getElementById('results-title').textContent = 'Your results are ready';
      document.getElementById('results-subtitle').textContent = 'Browse through each agent\'s output below.';
      // Stay on pipeline view — user can review outputs inline via card previews
      break;

    case 'pipeline_error':
      pipelineRunning = false;
      stopTimer();
      setRunDisabled(false);
      showAbortButton(false);
      updateCost(msg.cost);
      {
        const wasAborted = msg.aborted === true;
        const costStr = msg.cost ? (msg.cost.total_cost >= 0.01 ? `$${msg.cost.total_cost.toFixed(2)}` : `$${msg.cost.total_cost.toFixed(4)}`) : '';
        const costSuffix = costStr ? ` | Cost so far: ${costStr}` : '';
        document.getElementById('pipeline-title').textContent = wasAborted ? 'Pipeline aborted' : 'Pipeline stopped';
        document.getElementById('pipeline-subtitle').textContent = (msg.message || 'An error occurred.') + costSuffix;
        appendLog({
          time: ts(),
          level: wasAborted ? 'warning' : 'error',
          message: wasAborted ? 'Pipeline aborted by user' : `Pipeline error: ${msg.message}`,
        });
        // Mark any running agent cards as stopped
        if (wasAborted) {
          document.querySelectorAll('.agent-card.running').forEach(card => {
            const slug = card.dataset.slug;
            if (slug) setCardState(slug, 'failed', null, 'Aborted');
          });
        }
      }
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

  // Remove any existing error tooltip
  const existing = card.querySelector('.card-error');
  if (existing) existing.remove();

  // Show/hide the rerun button
  const rerunBtn = card.querySelector('.btn-rerun');

  switch (state) {
    case 'running':
      badge.className = 'status-badge running';
      badge.innerHTML = '<span class="spinner"></span> Running';
      if (rerunBtn) rerunBtn.classList.add('hidden');
      break;
    case 'done':
      badge.className = 'status-badge done';
      badge.textContent = elapsed ? `Done in ${elapsed}s` : 'Done';
      if (rerunBtn) rerunBtn.classList.remove('hidden');
      break;
    case 'failed':
      badge.className = 'status-badge failed';
      badge.textContent = 'Failed';
      if (rerunBtn) rerunBtn.classList.remove('hidden');
      // Show the actual error message on the card
      if (error) {
        const errDiv = document.createElement('div');
        errDiv.className = 'card-error';
        errDiv.textContent = error;
        card.appendChild(errDiv);
      }
      break;
    default:
      badge.className = 'status-badge waiting';
      badge.textContent = 'Waiting';
      if (rerunBtn) rerunBtn.classList.add('hidden');
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
  ['f-website',        'website_url'],
  ['f-reviews',        'customer_reviews'],
  ['f-competitors',    'competitor_info'],
  ['f-landing',        'landing_page_info'],
  ['f-compliance',     'compliance_category'],
  ['f-context',        'additional_context'],
];

function populateForm(data) {
  for (const [id, key] of FIELDS) {
    const el = document.getElementById(id);
    if (!el) continue;
    if (!(key in data)) {
      el.value = '';  // Clear fields not in the sample data
      continue;
    }
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

async function loadSample(name = 'animus') {
  try {
    const resp = await fetch(`/api/sample-input?name=${name}`);
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

// Model selection removed — models are hardcoded per-agent in config.py

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

  // Re-check health before running
  if (!healthOk) {
    await checkHealth();
    if (!healthOk) {
      alert(
        'Cannot start pipeline — no API keys configured.\n\n' +
        '1. Copy .env.example to .env\n' +
        '2. Add your API key (e.g. OPENAI_API_KEY=sk-...)\n' +
        '3. Restart the server'
      );
      return;
    }
  }

  setRunDisabled(true);

  const quickMode = document.getElementById('cb-quick-mode')?.checked || false;

  try {
    const resp = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        phases: selectedPhases,
        inputs,
        quick_mode: quickMode,
      }),
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
// ABORT PIPELINE
// -----------------------------------------------------------

function showAbortButton(show) {
  const btn = document.getElementById('btn-abort');
  if (!btn) return;
  if (show) {
    btn.classList.remove('hidden');
    btn.disabled = false;
    btn.textContent = 'Stop Pipeline';
    btn.classList.remove('aborting');
  } else {
    btn.classList.add('hidden');
    btn.disabled = false;
    btn.textContent = 'Stop Pipeline';
    btn.classList.remove('aborting');
  }
}

async function abortPipeline() {
  const btn = document.getElementById('btn-abort');
  if (btn) {
    btn.textContent = 'Stopping...';
    btn.disabled = true;
  }
  try {
    const resp = await fetch('/api/abort', { method: 'POST' });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      if (btn) {
        btn.textContent = 'Stop Pipeline';
        btn.disabled = false;
      }
    }
  } catch (e) {
    alert('Failed to abort: ' + e.message);
    if (btn) {
      btn.textContent = 'Stop Pipeline';
      btn.disabled = false;
    }
  }
}

// -----------------------------------------------------------
// PHASE GATE (pause between phases for review)
// -----------------------------------------------------------

function showPhaseGate(completedPhase, nextPhase, message) {
  // Stop the "running" feel — hide abort, show the gate
  showAbortButton(false);

  // Remove any existing gate UI
  hidePhaseGate();

  const agentList = document.getElementById('agent-list');
  if (!agentList) return;

  // Find the phase divider for the next phase and insert the gate before it
  const dividers = agentList.querySelectorAll('.phase-divider');
  let insertBefore = null;
  dividers.forEach(d => {
    if (d.textContent.includes(`Phase ${nextPhase}`)) {
      insertBefore = d;
    }
  });

  const gate = document.createElement('div');
  gate.id = 'phase-gate-bar';
  gate.className = 'phase-gate';
  gate.innerHTML = `
    <div class="phase-gate-content">
      <div class="phase-gate-message">
        <span class="phase-gate-icon">✅</span>
        <span>Phase ${completedPhase} complete — review the outputs above, then continue when satisfied.</span>
      </div>
      <div class="phase-gate-actions">
        <button class="btn btn-primary" onclick="continuePhase()">Continue to Phase ${nextPhase}</button>
        <button class="btn btn-stop" onclick="abortPipeline()">Stop Here</button>
      </div>
    </div>
  `;

  if (insertBefore) {
    agentList.insertBefore(gate, insertBefore);
  } else {
    agentList.appendChild(gate);
  }

  // Scroll to the gate
  gate.scrollIntoView({ behavior: 'smooth', block: 'center' });

  // Update header
  document.getElementById('pipeline-title').textContent = `Phase ${completedPhase} done — review & continue`;
  document.getElementById('pipeline-subtitle').textContent = message;
}

function hidePhaseGate() {
  const existing = document.getElementById('phase-gate-bar');
  if (existing) existing.remove();
}

async function continuePhase() {
  const btn = document.querySelector('#phase-gate-bar .btn-primary');
  if (btn) {
    btn.textContent = 'Continuing...';
    btn.disabled = true;
  }

  try {
    const resp = await fetch('/api/continue', { method: 'POST' });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      if (btn) { btn.textContent = 'Continue'; btn.disabled = false; }
      return;
    }
    // Gate will be cleared via WS message
    showAbortButton(true);
    document.getElementById('pipeline-title').textContent = 'Building your ads...';
    document.getElementById('pipeline-subtitle').textContent = 'The agents are working.';
  } catch (e) {
    alert('Failed to continue: ' + e.message);
    if (btn) { btn.textContent = 'Continue'; btn.disabled = false; }
  }
}

// -----------------------------------------------------------
// RERUN SINGLE AGENT
// -----------------------------------------------------------

async function rerunAgent(slug) {
  // Gather inputs from the brief form (or use what's already cached)
  const inputs = readForm();
  if (!inputs.brand_name || !inputs.product_name) {
    alert('Please make sure the Brief has at least a Brand Name and Product Name.');
    return;
  }

  const quickMode = document.getElementById('cb-quick-mode')?.checked || false;

  // Set card to running state
  setCardState(slug, 'running');
  // Clear any cached preview
  delete cardPreviewCache[slug];
  closeCardPreview(slug);

  appendLog({ time: ts(), level: 'info', message: `Rerunning ${slug}...` });

  try {
    const resp = await fetch('/api/rerun', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ slug, inputs, quick_mode: quickMode }),
    });
    const data = await resp.json();

    if (data.error) {
      setCardState(slug, 'failed', null, data.error);
      appendLog({ time: ts(), level: 'error', message: `Rerun ${slug} failed: ${data.error}` });
      return;
    }

    // Success
    setCardState(slug, 'done', data.elapsed);
    if (data.cost) updateCost(data.cost);
    appendLog({ time: ts(), level: 'success', message: `${slug} rerun completed (${data.elapsed}s)` });
  } catch (e) {
    setCardState(slug, 'failed', null, e.message);
    appendLog({ time: ts(), level: 'error', message: `Rerun ${slug} error: ${e.message}` });
  }
}

// -----------------------------------------------------------
// COST TRACKER
// -----------------------------------------------------------

function updateCost(costData) {
  if (!costData) return;
  const el = document.getElementById('header-cost');
  if (!el) return;
  const cost = costData.total_cost || 0;
  el.textContent = cost >= 0.10 ? `$${cost.toFixed(2)}` : `$${cost.toFixed(3)}`;
  el.classList.add('has-cost');
}

function resetCost() {
  const el = document.getElementById('header-cost');
  if (el) {
    el.textContent = '$0.00';
    el.classList.remove('has-cost');
  }
}

// -----------------------------------------------------------
// CARD PREVIEW (inline output on pipeline view)
// -----------------------------------------------------------

const cardPreviewCache = {};  // slug -> data

async function toggleCardPreview(slug) {
  const card = document.getElementById(`card-${slug}`);
  const preview = document.getElementById(`preview-${slug}`);
  if (!card || !preview) return;

  // Only allow expanding done cards
  if (!card.classList.contains('done')) return;

  // Toggle open/closed
  if (!preview.classList.contains('hidden')) {
    preview.classList.add('hidden');
    card.classList.remove('expanded');
    return;
  }

  // Show loading state
  preview.classList.remove('hidden');
  card.classList.add('expanded');
  preview.innerHTML = '<div class="card-preview-loading">Loading output...</div>';

  // Fetch if not cached
  if (!cardPreviewCache[slug]) {
    try {
      const resp = await fetch(`/api/outputs/${slug}`);
      const d = await resp.json();
      cardPreviewCache[slug] = d.data;
    } catch (e) {
      preview.innerHTML = '<div class="card-preview-error">Failed to load output.</div>';
      console.error('Failed to load preview for', slug, e);
      return;
    }
  }

  // Render the output using the existing renderer
  preview.innerHTML = `
    <div class="card-preview-header">
      <span class="card-preview-title">Output</span>
      <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation(); closeCardPreview('${slug}')">Collapse</button>
    </div>
    <div class="card-preview-body">${renderOutput(cardPreviewCache[slug])}</div>
  `;
}

function closeCardPreview(slug) {
  const card = document.getElementById(`card-${slug}`);
  const preview = document.getElementById(`preview-${slug}`);
  if (preview) preview.classList.add('hidden');
  if (card) card.classList.remove('expanded');
}

// Clear preview cache when a new pipeline starts
function clearPreviewCache() {
  for (const key in cardPreviewCache) delete cardPreviewCache[key];
  // Collapse all open previews
  document.querySelectorAll('.card-preview').forEach(p => p.classList.add('hidden'));
  document.querySelectorAll('.agent-card.expanded').forEach(c => c.classList.remove('expanded'));
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

// Top-level keys to hide from rendered output (meta fields)
const HIDDEN_OUTPUT_KEYS = new Set([
  'brand_name', 'product_name', 'generated_date', 'batch_id',
]);

function renderOutput(data) {
  if (!data || typeof data !== 'object') {
    return `<div class="empty-state">No data to display.</div>`;
  }

  let html = '';
  for (const [key, val] of Object.entries(data)) {
    if (HIDDEN_OUTPUT_KEYS.has(key)) continue;
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
    agent_01a: '🔬', agent_01a2: '📐', agent_01b: '📡',
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
// HEALTH CHECK
// -----------------------------------------------------------

let healthOk = false;

async function checkHealth() {
  try {
    const resp = await fetch('/api/health');
    const data = await resp.json();
    healthOk = data.ok;

    // Remove any existing warning
    const existing = document.getElementById('env-warning');
    if (existing) existing.remove();

    if (!data.ok) {
      const runBar = document.querySelector('.run-bar');
      const briefInner = runBar ? runBar.parentElement : document.querySelector('.view-inner');
      if (briefInner) {
        const warning = document.createElement('div');
        warning.id = 'env-warning';
        warning.className = 'env-warning';

        if (!data.any_provider_configured) {
          warning.innerHTML = `
            <strong>No API keys configured — pipeline will fail</strong>
            No LLM provider keys found. Copy the example env file and add at least one key:<br>
            <code>cp .env.example .env</code><br><br>
            Then add your API key (e.g. <code>OPENAI_API_KEY=sk-...</code>) and restart the server.
          `;
        } else {
          warning.innerHTML = `
            <strong>Default provider "${esc(data.default_provider)}" has no API key</strong>
            ${data.warnings.map(w => esc(w)).join('<br>')}<br><br>
            Add your <code>${esc(data.default_provider.toUpperCase())}_API_KEY</code> to <code>.env</code> and restart, or change <code>DEFAULT_PROVIDER</code> to a configured provider.
          `;
        }

        // Insert warning before the run bar
        if (runBar) {
          runBar.parentElement.insertBefore(warning, runBar);
        }
      }

      // Disable run button
      const btn = document.getElementById('btn-run');
      if (btn) {
        btn.disabled = true;
        btn.title = 'Fix API key configuration first';
      }
    }
  } catch (e) {
    console.error('Health check failed', e);
  }
}

// -----------------------------------------------------------
// INIT
// -----------------------------------------------------------

connectWS();
loadSample('animus'); // Pre-populate form with sample brand
checkHealth(); // Verify API keys are configured

// Check if there are existing outputs on page load — restore card states
fetch('/api/outputs')
  .then(r => r.json())
  .then(outputs => {
    const hasAny = outputs.some(o => o.available);
    if (hasAny) {
      // Show indicator on Results step
      const resultsStep = document.querySelector('.step[data-step="results"]');
      if (resultsStep) resultsStep.title = 'Previous results available';

      // Restore pipeline card states from saved outputs
      // (so completed agents show "Done" + rerun button even after refresh)
      if (!pipelineRunning) {
        outputs.forEach(o => {
          if (o.available) {
            setCardState(o.slug, 'done');
          }
        });
      }
    }
  })
  .catch(() => {});
