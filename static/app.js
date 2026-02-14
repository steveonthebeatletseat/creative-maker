// ============================================================
// Creative Maker — Dashboard (Redesigned)
// ============================================================

// -----------------------------------------------------------
// STATE
// -----------------------------------------------------------

let ws = null;
let reconnectTimer = null;
let selectedPhases = [1]; // Start Pipeline runs Phase 1 only (branching starts at Phase 2)
// Model defaults per-agent in config.py — can be overridden via Model Settings in the Brief view
let availableOutputs = [];   // [{slug, name, phase, icon, available}, ...]
let resultIndex = 0;         // current result being viewed
let loadedResults = [];       // [{slug, name, icon, data}, ...]
let pipelineRunning = false;
let agentTimers = {};  // slug -> { startTime, intervalId }
let statusPollTimer = null;
let serverLogSeen = new Set();
let serverLogSeenOrder = [];

// Brand state
let activeBrandSlug = null;
let brandList = [];
let brandSelectorOpen = false;

const AGENT_NAMES = {
  agent_01a: 'Foundation Research',
  agent_02: 'Creative Engine',
  agent_04: 'Copywriter',
  agent_05: 'Hook Specialist',
};

// How many agents total per phase selection
const AGENT_SLUGS = {
  1: ['agent_01a'],
  2: ['agent_02'],
  3: ['agent_04', 'agent_05'],
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
      const pausedAtGate = Boolean(msg.waiting_for_approval && msg.gate_info);
      // Track active brand from server
      if (msg.active_brand_slug) {
        activeBrandSlug = msg.active_brand_slug;
      }
      if (msg.running) {
        pipelineRunning = true;
        goToView('pipeline');
        startTimer();
        // If paused at a manual gate, allow starting a fresh run from Brief.
        setRunDisabled(!pausedAtGate);
        // Track active branch if set
        if (msg.active_branch) {
          activeBranchId = msg.active_branch;
        }
      } else {
        // Server says pipeline is NOT running — stop all timers
        pipelineRunning = false;
        stopTimer();
        stopAllAgentTimers();
        setRunDisabled(false);
        showAbortButton(false);
      }
      (msg.completed_agents || []).forEach(slug => setCardState(slug, 'done'));
      if (msg.current_agent && !pausedAtGate) {
        setCardState(msg.current_agent, 'running');
        startAgentTimer(msg.current_agent);
      } else if (pausedAtGate) {
        stopAllAgentTimers();
      }
      updateProgress();
      (msg.log || []).forEach(e => appendLog(e));
      if ((msg.server_log_tail || []).length) {
        appendServerLogLines(msg.server_log_tail);
      }

      // Restore phase gate if pipeline is paused waiting for approval
      if (pausedAtGate) {
        showPhaseGate(msg.gate_info);
        showAbortButton(false);
        setRunDisabled(false);
        document.getElementById('pipeline-title').textContent = msg.gate_info.next_agent_name
          ? `Ready: ${msg.gate_info.next_agent_name}`
          : 'Review and continue';
        document.getElementById('pipeline-subtitle').textContent = 'Review the outputs above, then click Continue when ready.';
      } else if (msg.running) {
        showAbortButton(true);
        openLiveTerminal();
      }
      break;

    case 'pipeline_start':
      pipelineRunning = true;
      // Track brand slug from pipeline start
      if (msg.brand_slug) {
        activeBrandSlug = msg.brand_slug;
      }
      // Phase 1 = fresh pipeline → branches were cleared on the backend
      if (msg.phases && msg.phases.includes(1)) {
        branches = [];
        activeBranchId = null;
      }
      // Track run context: branch run vs main pipeline run
      if (msg.branch_id) {
        activeBranchId = msg.branch_id;
      } else if (!msg.phases || !msg.phases.includes(1)) {
        activeBranchId = null;
      }
      renderBranchTabs();
      // Only reset cards for phases being run — keep completed phases intact
      if (msg.phases) {
        const phaseSlugs = [];
        msg.phases.forEach(p => { if (AGENT_SLUGS[p]) phaseSlugs.push(...AGENT_SLUGS[p]); });
        phaseSlugs.forEach(slug => setCardState(slug, 'waiting'));
      } else {
        resetAllCards();
      }
      clearPreviewCache();
      clearLog();
      clearServerLog();
      resetCost();
      updateModelTags();
      goToView('pipeline');
      startTimer();
      showAbortButton(true);
      openLiveTerminal();
      // Hide phase start buttons during pipeline run
      document.querySelectorAll('.btn-start-phase').forEach(b => b.classList.add('hidden'));
      const row3 = document.getElementById('phase-3-start-row');
      if (row3) row3.classList.add('hidden');
      {
        const branchLabel = msg.branch_id ? branches.find(b => b.id === msg.branch_id)?.label : null;
        const isQuick = document.getElementById('cb-quick-mode')?.checked;
        document.getElementById('pipeline-title').textContent = branchLabel
          ? `Running: ${branchLabel}`
          : (isQuick ? 'Quick test run...' : 'Building your ads...');
        document.getElementById('pipeline-subtitle').textContent = branchLabel
          ? ''
          : (isQuick ? 'Running with standard LLM calls (no deep research).' : '');
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
      startAgentTimer(msg.slug);
      if (msg.model) setModelTagFromWS(msg.slug, msg.model, msg.provider);
      updateProgress();
      {
        const modelSuffix = msg.model ? ` [${msg.model}]` : '';
        appendLog({ time: ts(), level: 'info', message: `Starting ${msg.name}${modelSuffix}...` });
      }
      scrollToCard(msg.slug);
      break;

    case 'stream_progress':
      // Live streaming progress from LLM — update the activity log
      appendLog({ time: ts(), level: 'info', message: `${AGENT_NAMES[msg.slug] || msg.slug}: ${msg.message}` });
      break;

    case 'agent_complete':
      stopAgentTimer(msg.slug);
      setCardState(msg.slug, 'done', msg.elapsed);
      updateProgress();
      updateCost(msg.cost);
      appendLog({ time: ts(), level: 'success', message: `${msg.name} completed (${msg.elapsed}s)` });
      break;

    case 'agent_error':
      stopAgentTimer(msg.slug);
      setCardState(msg.slug, 'failed', null, msg.error);
      updateProgress();
      appendLog({ time: ts(), level: 'error', message: `${msg.name} failed: ${msg.error}` });
      break;

    case 'server_log':
      // Stream of real server-side log lines
      appendServerLogLines(msg.lines || []);
      break;

    case 'phase_gate':
      showPhaseGate(msg);
      appendLog({ time: ts(), level: 'info', message: msg.next_agent_name ? `${msg.next_agent_name} ready` : 'Review and continue' });
      document.getElementById('pipeline-title').textContent = msg.next_agent_name
        ? `Ready: ${msg.next_agent_name}`
        : 'Review and continue';
      document.getElementById('pipeline-subtitle').textContent = msg.show_concept_selection
        ? 'Review concepts, then continue.'
        : `Choose model and start ${msg.next_agent_name || 'next agent'}.`;
      // Auto-open concept review drawer
      if (msg.show_concept_selection) {
        loadAndOpenConceptReviewDrawer();
      }
      break;

    case 'phase_gate_cleared':
      hidePhaseGate();
      document.getElementById('pipeline-title').textContent = 'Building your ads...';
      document.getElementById('pipeline-subtitle').textContent = '';
      break;

    case 'pipeline_complete':
      pipelineRunning = false;
      stopTimer();
      stopAllAgentTimers();
      setRunDisabled(false);
      showAbortButton(false);
      updateCost(msg.cost);
      updatePhaseStartButtons();
      loadBranches(); // Refresh branch tabs to show updated status
      {
        const costStr = msg.cost ? (msg.cost.total_cost >= 0.01 ? `$${msg.cost.total_cost.toFixed(2)}` : `$${msg.cost.total_cost.toFixed(4)}`) : '';
        const costSuffix = costStr ? ` | Cost: ${costStr}` : '';
        const branchLabel = msg.branch_id ? branches.find(b => b.id === msg.branch_id)?.label : null;
        document.getElementById('pipeline-title').textContent = branchLabel
          ? `${branchLabel} — Done!`
          : 'All done!';
        document.getElementById('pipeline-subtitle').textContent = `Pipeline finished in ${msg.elapsed}s${costSuffix}. Click any card to view output.`;
      }
      appendLog({ time: ts(), level: 'success', message: `Pipeline complete in ${msg.elapsed}s` });
      document.getElementById('results-title').textContent = 'Your results are ready';
      document.getElementById('results-subtitle').textContent = 'Browse through each agent\'s output below.';
      break;

    case 'branch_created':
      loadBranches();
      break;

    case 'branch_deleted':
      loadBranches();
      break;

    case 'pipeline_error':
      pipelineRunning = false;
      stopTimer();
      stopAllAgentTimers();
      setRunDisabled(false);
      showAbortButton(false);
      updateCost(msg.cost);
      loadBranches(); // Refresh branch tabs to show updated status
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

  // Show/hide the rerun group (button + dropdown arrow)
  const rerunGroup = card.querySelector('.rerun-group');

  switch (state) {
    case 'running':
      badge.className = 'status-badge running';
      badge.innerHTML = '<span class="spinner"></span> Running';
      if (rerunGroup) rerunGroup.classList.add('hidden');
      break;
    case 'done':
      badge.className = 'status-badge done';
      badge.textContent = elapsed ? `Done in ${elapsed}s` : 'Done';
      if (rerunGroup) rerunGroup.classList.remove('hidden');
      break;
    case 'failed':
      badge.className = 'status-badge failed';
      badge.textContent = 'Failed';
      if (rerunGroup) rerunGroup.classList.remove('hidden');
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
      if (rerunGroup) rerunGroup.classList.add('hidden');
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

function startTimer() { /* elapsed display removed */ }
function stopTimer() { /* elapsed display removed */ }

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
// LIVE TERMINAL (server log stream)
// -----------------------------------------------------------

function openLiveTerminal() {
  const panel = document.getElementById('live-terminal');
  if (panel) panel.classList.remove('collapsed');
  const toggle = document.getElementById('terminal-toggle');
  if (toggle) toggle.textContent = 'Hide';
}

function closeLiveTerminal() {
  const panel = document.getElementById('live-terminal');
  if (panel) panel.classList.add('collapsed');
  const toggle = document.getElementById('terminal-toggle');
  if (toggle) toggle.textContent = 'Show Live Logs';
}

function toggleLiveTerminal() {
  const panel = document.getElementById('live-terminal');
  if (!panel) return;
  if (panel.classList.contains('collapsed')) {
    openLiveTerminal();
  } else {
    closeLiveTerminal();
  }
}

function appendServerLogLines(lines) {
  const box = document.getElementById('terminal-output');
  if (!box) return;
  for (const line of lines) {
    if (!line || serverLogSeen.has(line)) continue;
    serverLogSeen.add(line);
    serverLogSeenOrder.push(line);
    while (serverLogSeenOrder.length > 2000) {
      const old = serverLogSeenOrder.shift();
      if (old) serverLogSeen.delete(old);
    }

    const div = document.createElement('div');
    div.className = 'terminal-line';
    // Highlight key patterns
    if (line.includes('Deep Research: status=in_progress')) {
      div.classList.add('t-progress');
    } else if (line.includes('Deep Research: completed') || line.includes('completed in')) {
      div.classList.add('t-success');
    } else if (line.includes('ERROR') || line.includes('failed') || line.includes('Failed')) {
      div.classList.add('t-error');
    } else if (line.includes('Token usage') || line.includes('cost=')) {
      div.classList.add('t-cost');
    } else if (line.includes('=== Agent')) {
      div.classList.add('t-agent');
    }
    div.textContent = line;
    box.appendChild(div);
  }
  // Auto-scroll
  box.scrollTop = box.scrollHeight;
  // Trim old lines (keep last 500)
  while (box.children.length > 500) {
    box.removeChild(box.firstChild);
  }
}

function clearServerLog() {
  const box = document.getElementById('terminal-output');
  if (box) box.innerHTML = '';
  serverLogSeen.clear();
  serverLogSeenOrder = [];
}

function startStatusPolling() {
  if (statusPollTimer) return;
  statusPollTimer = setInterval(async () => {
    try {
      const resp = await fetch('/api/status');
      if (!resp.ok) return;
      const data = await resp.json();
      if ((data.server_log_tail || []).length) {
        appendServerLogLines(data.server_log_tail);
      }
    } catch (e) {
      // Ignore transient polling errors.
    }
  }, 4000);
}

// -----------------------------------------------------------
// AGENT ELAPSED TIMER (shows ticking seconds on running cards)
// -----------------------------------------------------------

function startAgentTimer(slug) {
  stopAgentTimer(slug); // clear any existing
  const startTime = Date.now();
  const intervalId = setInterval(() => {
    const card = document.getElementById(`card-${slug}`);
    if (!card || !card.classList.contains('running')) {
      stopAgentTimer(slug);
      return;
    }
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    const badge = card.querySelector('.status-badge');
    if (badge) {
      const m = Math.floor(elapsed / 60);
      const s = elapsed % 60;
      const timeStr = m > 0 ? `${m}m ${s}s` : `${s}s`;
      badge.innerHTML = `<span class="spinner"></span> Running · ${timeStr}`;
    }
  }, 1000);
  agentTimers[slug] = { startTime, intervalId };
}

function stopAgentTimer(slug) {
  if (agentTimers[slug]) {
    clearInterval(agentTimers[slug].intervalId);
    delete agentTimers[slug];
  }
}

function stopAllAgentTimers() {
  for (const slug in agentTimers) {
    clearInterval(agentTimers[slug].intervalId);
  }
  agentTimers = {};
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

  // If a run is active/paused, stop it first so we can start fresh.
  if (pipelineRunning) {
    const proceed = confirm(
      'A pipeline run is currently active or paused. Starting fresh will stop it first. Continue?'
    );
    if (!proceed) return;

    try {
      await fetch('/api/abort', { method: 'POST' });
    } catch (e) {
      // Ignore and continue to status polling.
    }

    // Wait briefly for backend to clear running state.
    for (let i = 0; i < 20; i++) {
      try {
        const s = await fetch('/api/status');
        const data = await s.json();
        if (!data.running) break;
      } catch (e) {
        // If status check fails transiently, keep trying.
      }
      await new Promise(r => setTimeout(r, 150));
    }
    pipelineRunning = false;
    setRunDisabled(false);
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
        phases: [1],
        inputs,
        quick_mode: quickMode,
        model_overrides: {},
      }),
    });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      setRunDisabled(false);
    } else if (data.brand_slug) {
      activeBrandSlug = data.brand_slug;
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

function showPhaseGate(gateInfo) {
  // gateInfo: { completed_agent, next_agent, next_agent_name, phase, show_concept_selection }
  showAbortButton(false);
  hidePhaseGate();

  const agentList = document.getElementById('agent-list');
  if (!agentList) return;

  const nextSlug = gateInfo.next_agent || '';
  const nextName = gateInfo.next_agent_name || 'Next Agent';
  const showConceptSelection = gateInfo.show_concept_selection || false;
  const failedCopywriterCount =
    gateInfo.completed_agent === 'agent_04' ? (gateInfo.copywriter_failed_count || 0) : 0;

  // Find the next agent wrapper and insert gate before it
  let insertBefore = null;
  const nextWrapper =
    document.getElementById(`wrapper-${nextSlug}`) ||
    document.getElementById(`card-${nextSlug}`)?.closest('.agent-card-wrapper');
  if (nextWrapper && nextWrapper.parentElement === agentList) {
    insertBefore = nextWrapper;
  } else {
    // Fallback: insert before the next phase divider
    const dividers = agentList.querySelectorAll('.phase-divider');
    const nextPhase = (gateInfo.phase || 1) + 1;
    dividers.forEach(d => {
      if (d.textContent.includes(`Phase ${nextPhase}`)) insertBefore = d;
    });
  }

  const gate = document.createElement('div');
  gate.id = 'phase-gate-bar';
  gate.className = 'phase-gate';

  // Build concept selection UI if this is the Agent 02→04 gate
  let selectionHtml = '';
  if (showConceptSelection) {
    // Initialize review state from cached data
    const cachedData = cardPreviewCache['agent_02'];
    if (cachedData) {
      _ceReviewData = cachedData;
      initCeReviewState(cachedData);
    }
    selectionHtml = buildConceptSelectionUI();
  }

  // Build model picker
  const modelPickerHtml = buildModelPicker(nextSlug, nextName);

  const messageText = showConceptSelection
    ? `Creative Engine complete — select concepts, choose model, then start ${nextName}.`
    : `Review the outputs above, choose model, then start ${nextName}.`;

  const rewriteFailedBtnHtml = failedCopywriterCount > 0
    ? `<button class="btn btn-ghost" id="btn-rewrite-failed-copywriter" onclick="rewriteFailedCopywriter()">
         Rewrite Failed (${failedCopywriterCount})
       </button>`
    : '';

  gate.innerHTML = `
    <div class="phase-gate-content">
      <div class="phase-gate-message">
        <span class="phase-gate-icon">✅</span>
        <span>${messageText}</span>
      </div>
      ${selectionHtml}
      <div class="phase-gate-model-row">
        ${modelPickerHtml}
      </div>
      <div class="phase-gate-actions">
        <span id="gate-selection-count" class="gate-selection-count" style="display:${showConceptSelection ? 'inline' : 'none'}"></span>
        ${rewriteFailedBtnHtml}
        <button class="btn btn-primary" onclick="continuePhase(${showConceptSelection ? 'true' : 'false'})">
          Start ${esc(nextName)}
        </button>
        <button class="btn btn-stop" onclick="abortPipeline()">Stop Here</button>
      </div>
    </div>
  `;

  if (insertBefore) {
    agentList.insertBefore(gate, insertBefore);
  } else {
    agentList.appendChild(gate);
  }

  if (showConceptSelection) updateGateSelectionCount();

  setTimeout(() => {
    gate.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }, 100);
}

function buildModelPicker(slug, agentName) {
  const options = [
    { value: '', label: 'Default' },
    { value: 'anthropic/claude-opus-4-6', label: 'Claude Opus 4.6' },
    { value: 'openai/gpt-5.2', label: 'GPT 5.2' },
    { value: 'google/gemini-3.0-pro', label: 'Gemini 3.0 Pro' },
  ];

  // For Foundation Research, add Deep Research as default
  if (slug === 'agent_01a') {
    options[0].label = 'Deep Research (default)';
  }

  const optionsHtml = options.map(o =>
    `<option value="${o.value}">${esc(o.label)}</option>`
  ).join('');

  return `
    <div class="gate-model-picker">
      <label class="gate-model-label" for="gate-model-select">Model for ${esc(agentName)}:</label>
      <select class="gate-model-select" id="gate-model-select">
        ${optionsHtml}
      </select>
    </div>
  `;
}

function getGateModelOverride() {
  const sel = document.getElementById('gate-model-select');
  if (!sel || !sel.value) return {};
  const slashIdx = sel.value.indexOf('/');
  if (slashIdx < 0) return {};
  return {
    provider: sel.value.substring(0, slashIdx),
    model: sel.value.substring(slashIdx + 1),
  };
}

function buildConceptSelectionUI() {
  return `<div class="gate-concept-summary">
    <div class="cr-gate-row">
      <span class="cr-gate-stats" id="cr-gate-stats"></span>
      <button class="btn btn-primary btn-sm" onclick="openConceptReviewDrawerFromGate()">Review Concepts</button>
    </div>
  </div>`;
}

function updateGateSelectionCount() {
  const total = Object.keys(_ceReviewState).length;
  const approved = Object.values(_ceReviewState).filter(s => s === 'approved').length;

  const el = document.getElementById('gate-selection-count');
  if (el) {
    el.textContent = `${approved} of ${total} concepts approved`;
    el.style.color = approved === 0 ? 'var(--error)' : '';
  }

  const gateStats = document.getElementById('cr-gate-stats');
  if (gateStats) {
    gateStats.textContent = `${approved} approved`;
  }

  const btn = document.querySelector('#phase-gate-bar .btn-primary');
  if (btn && total > 0) {
    btn.disabled = approved === 0;
  }
}

function gateSelectAll() {
  crApproveAll();
  updateGateSelectionCount();
}

function gateDeselectAll() {
  crRejectAll();
  updateGateSelectionCount();
}

function gateSelectFirst() {
  crFirstPerAngle();
  updateGateSelectionCount();
}

function hidePhaseGate() {
  const existing = document.getElementById('phase-gate-bar');
  if (existing) existing.remove();
}

async function continuePhase(withConceptSelection) {
  const btn = document.querySelector('#phase-gate-bar .btn-primary');
  const modelOverride = getCrModelOverride() || getGateModelOverride();

  // If concept selection is active, gather selections and send them first
  if (withConceptSelection) {
    const selections = getCeSelections();

    if (selections.length === 0) {
      alert('Please select at least one video concept.');
      return;
    }

    if (btn) {
      btn.textContent = `Starting...`;
      btn.disabled = true;
    }

    try {
      const resp = await fetch('/api/select-concepts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ selected: selections, model_override: modelOverride }),
      });
      const data = await resp.json();
      if (data.error) {
        alert(data.error);
        if (btn) { btn.textContent = 'Start Copywriter'; btn.disabled = false; }
        return;
      }
      showAbortButton(true);
      document.getElementById('pipeline-title').textContent = 'Building your ads...';
      document.getElementById('pipeline-subtitle').textContent = '';
    } catch (e) {
      alert('Failed to send selections: ' + e.message);
      if (btn) { btn.textContent = 'Start Copywriter'; btn.disabled = false; }
    }
    return;
  }

  // Standard continue with model override
  if (btn) {
    btn.textContent = 'Starting...';
    btn.disabled = true;
  }

  try {
    const resp = await fetch('/api/continue', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model_override: modelOverride }),
    });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      if (btn) { btn.textContent = 'Continue'; btn.disabled = false; }
      return;
    }
    showAbortButton(true);
    document.getElementById('pipeline-title').textContent = 'Building your ads...';
    document.getElementById('pipeline-subtitle').textContent = 'The agents are working.';
  } catch (e) {
    alert('Failed to continue: ' + e.message);
    if (btn) { btn.textContent = 'Continue'; btn.disabled = false; }
  }
}

async function rewriteFailedCopywriter() {
  const btn = document.getElementById('btn-rewrite-failed-copywriter');
  const modelOverride = getGateModelOverride();

  if (btn) {
    btn.textContent = 'Rewriting...';
    btn.disabled = true;
  }

  setCardState('agent_04', 'running');
  startAgentTimer('agent_04');

  try {
    const resp = await fetch('/api/rewrite-failed-copywriter', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model_override: modelOverride }),
    });
    const data = await resp.json();

    stopAgentTimer('agent_04');

    if (data.error) {
      setCardState('agent_04', 'failed', null, data.error);
      alert(data.error);
      if (btn) {
        btn.textContent = 'Rewrite Failed';
        btn.disabled = false;
      }
      return;
    }

    setCardState('agent_04', 'done');
    if (data.cost) updateCost(data.cost);
    delete cardPreviewCache['agent_04'];
    closeCardPreview('agent_04');

    const rewritten = data.rewritten || 0;
    const remaining = data.remaining_failed || 0;
    appendLog({
      time: ts(),
      level: remaining === 0 ? 'success' : 'warning',
      message: `Copywriter rewrite finished: ${rewritten} recovered, ${remaining} still failed.`,
    });

    if (btn) {
      if (remaining > 0) {
        btn.textContent = `Rewrite Failed (${remaining})`;
        btn.disabled = false;
      } else {
        btn.remove();
      }
    }
  } catch (e) {
    stopAgentTimer('agent_04');
    setCardState('agent_04', 'failed', null, e.message);
    alert('Failed to rewrite: ' + e.message);
    if (btn) {
      btn.textContent = 'Rewrite Failed';
      btn.disabled = false;
    }
  }
}

// -----------------------------------------------------------
// RERUN SINGLE AGENT
// -----------------------------------------------------------

// Available models for the rerun dropdown
const RERUN_MODEL_OPTIONS = [
  { label: 'Default', provider: null, model: null },
  { label: 'GPT 5.2', provider: 'openai', model: 'gpt-5.2' },
  { label: 'Claude Opus 4.6', provider: 'anthropic', model: 'claude-opus-4-6' },
  { label: 'Gemini 3.0 Pro', provider: 'google', model: 'gemini-3.0-pro' },
  { label: 'Gemini 2.5 Flash', provider: 'google', model: 'gemini-2.5-flash' },
];

function toggleRerunMenu(slug) {
  const menu = document.getElementById(`rerun-menu-${slug}`);
  if (!menu) return;

  // Close all other open menus first
  document.querySelectorAll('.rerun-menu').forEach(m => {
    if (m.id !== `rerun-menu-${slug}`) m.classList.add('hidden');
  });

  if (!menu.classList.contains('hidden')) {
    menu.classList.add('hidden');
    return;
  }

  // Populate the menu
  const currentDefault = agentModelDefaults[slug];
  const defaultLabel = currentDefault ? currentDefault.label : 'Default';

  menu.innerHTML = RERUN_MODEL_OPTIONS.map(opt => {
    const isDefault = !opt.provider;
    const label = isDefault ? `${defaultLabel} (default)` : opt.label;
    const providerClass = opt.provider ? `provider-${opt.provider}` : (currentDefault ? `provider-${currentDefault.provider}` : '');
    return `<button class="rerun-menu-item ${providerClass}" onclick="event.stopPropagation(); rerunAgent('${slug}', ${opt.provider ? `'${opt.provider}'` : 'null'}, ${opt.model ? `'${opt.model}'` : 'null'})">${esc(label)}</button>`;
  }).join('');

  menu.classList.remove('hidden');
}

// Close rerun menus when clicking elsewhere
document.addEventListener('click', () => {
  document.querySelectorAll('.rerun-menu').forEach(m => m.classList.add('hidden'));
});

async function rerunAgent(slug, overrideProvider, overrideModel) {
  // Close any open menu
  document.querySelectorAll('.rerun-menu').forEach(m => m.classList.add('hidden'));

  // Gather inputs from the brief form (or use what's already cached)
  const inputs = readForm();
  if (!inputs.brand_name || !inputs.product_name) {
    alert('Please make sure the Brief has at least a Brand Name and Product Name.');
    return;
  }

  const quickMode = document.getElementById('cb-quick-mode')?.checked || false;

  // Set card to running state
  setCardState(slug, 'running');
  startAgentTimer(slug);
  // Clear any cached preview
  delete cardPreviewCache[slug];
  closeCardPreview(slug);

  // Determine model label for the log
  const _labelMap = {
    'gpt-5.2': 'GPT 5.2',
    'gemini-3.0-pro': 'Gemini 3.0 Pro',
    'gemini-2.5-pro': 'Gemini 2.5 Pro',
    'gemini-2.5-flash': 'Gemini 2.5 Flash',
    'claude-opus-4-6': 'Claude Opus 4.6',
  };
  const modelLabel = overrideModel ? (_labelMap[overrideModel] || overrideModel) : 'default';
  appendLog({ time: ts(), level: 'info', message: `Rerunning ${slug} [${modelLabel}]...` });

  // Update the model tag on the card
  if (overrideProvider && overrideModel) {
    setModelTagFromWS(slug, _labelMap[overrideModel] || overrideModel, overrideProvider);
  }

  const body = { slug, inputs, quick_mode: quickMode };
  if (overrideProvider) body.provider = overrideProvider;
  if (overrideModel) body.model = overrideModel;

  try {
    const resp = await fetch('/api/rerun', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();

    stopAgentTimer(slug);

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
    stopAgentTimer(slug);
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
  // Delegate to branch-aware version for Phase 2+ agents
  return toggleCardPreviewBranchAware(slug);
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
  chatHistories = {};
  // Collapse all open previews
  document.querySelectorAll('.card-preview').forEach(p => p.classList.add('hidden'));
  document.querySelectorAll('.agent-card.expanded').forEach(c => c.classList.remove('expanded'));
  // Reset concept review drawer state
  _ceReviewState = {};
  _ceReviewSelectedKey = null;
  _ceReviewData = null;
  _ceReviewFilter = 'all';
  if (_ceReviewDrawerOpen) closeConceptReviewDrawer();
}

// -----------------------------------------------------------
// CHAT WITH AGENT OUTPUT
// -----------------------------------------------------------

let chatHistories = {};  // slug -> [{role, content}, ...]

async function sendChatMessage(slug) {
  const input = document.getElementById(`chat-input-${slug}`);
  const messagesEl = document.getElementById(`chat-messages-${slug}`);
  if (!input || !messagesEl) return;

  const message = input.value.trim();
  if (!message) return;

  // Init history for this slug
  if (!chatHistories[slug]) chatHistories[slug] = [];

  // Add user message to UI
  appendChatMessage(slug, 'user', message);
  input.value = '';
  input.disabled = true;

  // Show typing indicator
  const typingId = `typing-${slug}-${Date.now()}`;
  messagesEl.insertAdjacentHTML('beforeend',
    `<div class="chat-msg assistant typing" id="${typingId}"><div class="chat-msg-bubble"><span class="chat-typing-dots"><span>.</span><span>.</span><span>.</span></span></div></div>`
  );
  messagesEl.scrollTop = messagesEl.scrollHeight;

  // Get selected model
  const modelSelect = document.getElementById(`chat-model-${slug}`);
  const modelVal = modelSelect ? modelSelect.value : 'google/gemini-2.5-flash';
  const [provider, model] = modelVal.split('/');

  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        slug,
        message,
        history: chatHistories[slug],
        provider,
        model,
      }),
    });
    const data = await resp.json();

    // Remove typing indicator
    const typingEl = document.getElementById(typingId);
    if (typingEl) typingEl.remove();

    if (data.error) {
      appendChatMessage(slug, 'error', data.error);
    } else {
      // Add to history
      chatHistories[slug].push({ role: 'user', content: message });
      chatHistories[slug].push({ role: 'assistant', content: data.response });

      // Show response
      appendChatMessage(slug, 'assistant', data.response);

      // If there are changes, show the apply button
      if (data.has_changes && data.modified_output) {
        showApplyChanges(slug, data.modified_output);
      }
    }
  } catch (e) {
    const typingEl = document.getElementById(typingId);
    if (typingEl) typingEl.remove();
    appendChatMessage(slug, 'error', 'Failed to send message: ' + e.message);
  }

  input.disabled = false;
  input.focus();
}

function appendChatMessage(slug, role, content) {
  const messagesEl = document.getElementById(`chat-messages-${slug}`);
  if (!messagesEl) return;

  const div = document.createElement('div');
  div.className = `chat-msg ${role}`;

  // Format content — convert markdown-like formatting
  let html = esc(content)
    .replace(/\n/g, '<br>')
    .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code>$1</code>');

  div.innerHTML = `<div class="chat-msg-bubble">${html}</div>`;
  messagesEl.appendChild(div);
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function showApplyChanges(slug, modifiedOutput) {
  const messagesEl = document.getElementById(`chat-messages-${slug}`);
  if (!messagesEl) return;

  // Store the modified output temporarily
  window._pendingChanges = window._pendingChanges || {};
  window._pendingChanges[slug] = modifiedOutput;

  const div = document.createElement('div');
  div.className = 'chat-apply-bar';
  div.innerHTML = `
    <span>The output has been modified.</span>
    <button class="btn btn-primary btn-sm" onclick="event.stopPropagation(); applyChatChanges('${slug}')">Apply Changes</button>
    <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation(); this.parentElement.remove()">Dismiss</button>
  `;
  messagesEl.appendChild(div);
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

async function applyChatChanges(slug) {
  const modifiedOutput = window._pendingChanges && window._pendingChanges[slug];
  if (!modifiedOutput) return;

  try {
    const resp = await fetch('/api/chat/apply', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ slug, output: modifiedOutput }),
    });
    const data = await resp.json();
    if (data.error) {
      appendChatMessage(slug, 'error', 'Failed to apply: ' + data.error);
      return;
    }

    // Update the cached output and re-render the preview
    cardPreviewCache[slug] = modifiedOutput;
    saveCeCheckboxState();
    const previewBody = document.querySelector(`#preview-${slug} .card-preview-body`);
    if (previewBody) {
      previewBody.innerHTML = renderOutput(modifiedOutput);
      restoreCeCheckboxState();
    }

    // Remove the apply bar
    document.querySelectorAll(`#chat-messages-${slug} .chat-apply-bar`).forEach(el => el.remove());

    appendChatMessage(slug, 'assistant', 'Changes applied and saved.');
    delete window._pendingChanges[slug];
  } catch (e) {
    appendChatMessage(slug, 'error', 'Failed to apply changes: ' + e.message);
  }
}

// -----------------------------------------------------------
// RESULTS
// -----------------------------------------------------------

async function loadResults() {
  try {
    const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
    const resp = await fetch(`/api/outputs${brandParam}`);
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
        const r = await fetch(`/api/outputs/${o.slug}${brandParam}`);
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

  // Special renderer for Creative Engine (Agent 02) output
  if (data.angles && Array.isArray(data.angles) && data.angles.length > 0 && data.angles[0].funnel_stage) {
    return renderCreativeEngineOutput(data);
  }

  let html = '';
  for (const [key, val] of Object.entries(data)) {
    if (HIDDEN_OUTPUT_KEYS.has(key)) continue;
    html += renderSection(key, val, 0);
  }
  return html;
}


// -----------------------------------------------------------
// CREATIVE ENGINE (Agent 02) — Custom Renderer
// -----------------------------------------------------------

function renderCreativeEngineOutput(data) {
  const angles = data.angles || [];

  // Group angles by funnel stage
  const groups = { tof: [], mof: [], bof: [] };
  angles.forEach(a => {
    const stage = (a.funnel_stage || '').toLowerCase();
    if (groups[stage]) groups[stage].push(a);
    else groups.tof.push(a); // fallback
  });

  const stageLabels = {
    tof: 'Top of Funnel',
    mof: 'Mid Funnel',
    bof: 'Bottom of Funnel',
  };

  // Build filter buttons
  let filtersHtml = `<div class="ce-filters">`;
  filtersHtml += `<button class="ce-filter-btn active" data-filter="all" onclick="ceFilterAngles(this, 'all')">All <span class="ce-filter-count">${angles.length}</span></button>`;
  for (const [stage, arr] of Object.entries(groups)) {
    if (arr.length === 0) continue;
    filtersHtml += `<button class="ce-filter-btn" data-filter="${stage}" onclick="ceFilterAngles(this, '${stage}')">${stage.toUpperCase()} <span class="ce-filter-count">${arr.length}</span></button>`;
  }
  filtersHtml += `</div>`;

  // Build angle cards
  let anglesHtml = `<div class="ce-angles">`;
  angles.forEach((angle, i) => {
    const stage = (angle.funnel_stage || '').toLowerCase();
    const stageBadge = stage === 'tof' ? 'purple' : stage === 'mof' ? 'yellow' : 'green';
    const conceptCount = (angle.video_concepts || []).length;

    anglesHtml += `
    <div class="ce-angle" data-stage="${stage}">
      <div class="ce-angle-header" onclick="this.parentElement.classList.toggle('open')">
        <div class="ce-angle-header-left">
          <span class="ce-angle-chevron">▸</span>
          <span class="ce-angle-id out-badge ${stageBadge}">${esc(angle.angle_id || '')}</span>
          <span class="ce-angle-name">${esc(angle.angle_name || `Angle #${i + 1}`)}</span>
        </div>
        <div class="ce-angle-header-right">
          <span class="ce-angle-meta">${esc(angle.target_segment || '')}</span>
          <span class="ce-angle-meta">${esc(angle.emotional_lever || '')}</span>
          <span class="out-badge purple">${conceptCount} concept${conceptCount !== 1 ? 's' : ''}</span>
        </div>
      </div>
      <div class="ce-angle-body">
        <div class="ce-angle-details">
          <div class="ce-detail-grid">
            ${ceDetailField('Target Segment', angle.target_segment)}
            ${ceDetailField('Awareness', humanize(angle.target_awareness || ''))}
            ${ceDetailField('Core Desire', angle.core_desire)}
            ${ceDetailField('Emotional Lever', angle.emotional_lever)}
            ${ceDetailField('VoC Anchor', angle.voc_anchor, true)}
            ${ceDetailField('White Space', angle.white_space_link)}
            ${ceDetailField('Mechanism', angle.mechanism_hint)}
            ${ceDetailField('Objection Addressed', angle.objection_addressed)}
          </div>
        </div>
        ${renderVideoConcepts(angle.video_concepts || [], angle.angle_id || '')}
      </div>
    </div>`;
  });
  anglesHtml += `</div>`;

  return filtersHtml + anglesHtml;
}

function ceDetailField(label, value, isQuote) {
  if (!value) return '';
  const valHtml = isQuote
    ? `<span class="ce-detail-val ce-quote">"${esc(value)}"</span>`
    : `<span class="ce-detail-val">${esc(value)}</span>`;
  return `<div class="ce-detail-field">
    <span class="ce-detail-key">${esc(label)}</span>
    ${valHtml}
  </div>`;
}

function renderVideoConcepts(concepts, angleId) {
  if (!concepts || concepts.length === 0) return '';

  let html = `<div class="ce-concepts">
    <div class="ce-concepts-label">Video Concepts <span class="out-badge purple">${concepts.length}</span></div>`;

  concepts.forEach((c, i) => {
    const platforms = (c.platform_targets || []).map(p => esc(humanize(p))).join(', ');
    const cbId = `ce-cb-${esc(angleId)}-${i}`;
    html += `
    <div class="ce-concept" data-angle-id="${esc(angleId)}" data-concept-index="${i}">
      <div class="ce-concept-header">
        <label class="ce-concept-check" for="${cbId}" onclick="event.stopPropagation()">
          <input type="checkbox" id="${cbId}" class="ce-concept-cb"
            data-angle-id="${esc(angleId)}" data-concept-index="${i}"
            checked onchange="updateCeSelectionCount(); updateGateSelectionCount();" />
        </label>
        <span class="ce-concept-number">${i + 1}.</span>
        <span class="ce-concept-name">${esc(c.concept_name || `Concept ${i + 1}`)}</span>
        <span class="ce-concept-format">${esc(c.video_format || '')}</span>
      </div>
      <div class="ce-concept-body">
        ${c.scene_concept ? `<div class="ce-concept-scene">${escMultiline(c.scene_concept)}</div>` : ''}
        ${c.why_this_format ? ceDetailField('Why This Format', c.why_this_format) : ''}
        ${c.reference_examples ? ceDetailField('Reference Examples', c.reference_examples) : ''}
        ${platforms ? ceDetailField('Platforms', platforms) : ''}
        ${c.sound_music_direction ? ceDetailField('Sound/Music', c.sound_music_direction) : ''}
        ${c.proof_approach ? ceDetailField('Proof Approach', humanize(c.proof_approach)) : ''}
        ${c.proof_description ? ceDetailField('Proof', c.proof_description) : ''}
      </div>
    </div>`;
  });

  html += `</div>`;
  return html;
}

// Persistent checkbox state: { "angle_id:concept_index": true/false }
const _ceCheckboxState = {};

function saveCeCheckboxState() {
  document.querySelectorAll('.ce-concept-cb').forEach(cb => {
    const key = `${cb.dataset.angleId}:${cb.dataset.conceptIndex}`;
    _ceCheckboxState[key] = cb.checked;
  });
}

function restoreCeCheckboxState() {
  document.querySelectorAll('.ce-concept-cb').forEach(cb => {
    const key = `${cb.dataset.angleId}:${cb.dataset.conceptIndex}`;
    if (key in _ceCheckboxState) {
      cb.checked = _ceCheckboxState[key];
    }
  });
  updateCeSelectionCount();
}

function updateCeSelectionCount() {
  // Save state whenever something changes
  saveCeCheckboxState();
  // Update visual state of concept cards based on checkbox
  document.querySelectorAll('.ce-concept-cb').forEach(cb => {
    const card = cb.closest('.ce-concept');
    if (card) {
      card.classList.toggle('ce-concept-selected', cb.checked);
      card.classList.toggle('ce-concept-deselected', !cb.checked);
    }
  });
}

function getCeSelections() {
  const selections = [];
  for (const [key, state] of Object.entries(_ceReviewState)) {
    if (state === 'approved') {
      const [angleId, idx] = key.split(':');
      selections.push({
        angle_id: angleId,
        concept_index: parseInt(idx, 10),
      });
    }
  }
  return selections;
}

function ceFilterAngles(btn, stage) {
  // Update active button
  btn.closest('.ce-filters').querySelectorAll('.ce-filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');

  // Show/hide angles
  const container = btn.closest('.ce-filters').nextElementSibling;
  if (!container) return;
  container.querySelectorAll('.ce-angle').forEach(el => {
    if (stage === 'all' || el.dataset.stage === stage) {
      el.style.display = '';
    } else {
      el.style.display = 'none';
    }
  });
}

// -----------------------------------------------------------
// CONCEPT REVIEW DRAWER
// -----------------------------------------------------------

let _ceReviewState = {};          // { "angle_id:concept_index": "approved"|"rejected" }
let _ceReviewDrawerOpen = false;
let _ceReviewSelectedKey = null;  // "angle_id:concept_index"
let _ceReviewData = null;         // Cached agent_02 output (angles array)
let _ceReviewFilter = 'all';     // Current funnel filter

function initCeReviewState(data) {
  if (!data || !data.angles) return;
  data.angles.forEach(angle => {
    (angle.video_concepts || []).forEach((_, i) => {
      const key = `${angle.angle_id}:${i}`;
      if (!(key in _ceReviewState)) {
        _ceReviewState[key] = 'approved';
      }
    });
  });
}

function openConceptReviewDrawer(data) {
  if (!data || !data.angles) return;
  _ceReviewData = data;
  initCeReviewState(data);

  // Migrate any old checkbox state
  for (const [key, checked] of Object.entries(_ceCheckboxState)) {
    if (!(key in _ceReviewState)) {
      _ceReviewState[key] = checked ? 'approved' : 'rejected';
    }
  }

  renderCrFilters();
  renderCrLeftPanel(_ceReviewFilter);
  renderCrModelPicker();
  updateCrStats();

  // Select first concept
  const firstKey = _ceReviewSelectedKey || getFirstConceptKey();
  if (firstKey) crSelectConcept(firstKey);

  document.getElementById('concept-review-drawer').classList.remove('hidden');
  _ceReviewDrawerOpen = true;
}

function closeConceptReviewDrawer() {
  document.getElementById('concept-review-drawer').classList.add('hidden');
  _ceReviewDrawerOpen = false;
  // Sync state back to gate counter
  updateGateSelectionCount();
}

function getFirstConceptKey() {
  if (!_ceReviewData || !_ceReviewData.angles) return null;
  for (const a of _ceReviewData.angles) {
    if ((a.video_concepts || []).length > 0) {
      return `${a.angle_id}:0`;
    }
  }
  return null;
}

function getAllConceptKeys(filterStage) {
  const keys = [];
  if (!_ceReviewData || !_ceReviewData.angles) return keys;
  _ceReviewData.angles.forEach(angle => {
    const stage = (angle.funnel_stage || '').toLowerCase();
    if (filterStage && filterStage !== 'all' && stage !== filterStage) return;
    (angle.video_concepts || []).forEach((_, i) => {
      keys.push(`${angle.angle_id}:${i}`);
    });
  });
  return keys;
}

function findConceptByKey(key) {
  if (!_ceReviewData || !key) return null;
  const [angleId, idxStr] = key.split(':');
  const idx = parseInt(idxStr, 10);
  const angle = _ceReviewData.angles.find(a => a.angle_id === angleId);
  if (!angle) return null;
  const concept = (angle.video_concepts || [])[idx];
  if (!concept) return null;
  return { angle, concept, conceptIndex: idx };
}

function renderCrFilters() {
  const el = document.getElementById('cr-filters');
  if (!el || !_ceReviewData) return;

  const angles = _ceReviewData.angles || [];
  const counts = { all: angles.length, tof: 0, mof: 0, bof: 0 };
  angles.forEach(a => {
    const s = (a.funnel_stage || '').toLowerCase();
    if (counts[s] !== undefined) counts[s]++;
  });

  let html = `<button class="cr-filter-btn ${_ceReviewFilter === 'all' ? 'active' : ''}" onclick="crFilterConcepts('all')">All <span class="cr-filter-count">${counts.all}</span></button>`;
  for (const stage of ['tof', 'mof', 'bof']) {
    if (counts[stage] === 0) continue;
    html += `<button class="cr-filter-btn ${_ceReviewFilter === stage ? 'active' : ''}" onclick="crFilterConcepts('${stage}')">${stage.toUpperCase()} <span class="cr-filter-count">${counts[stage]}</span></button>`;
  }
  el.innerHTML = html;
}

function renderCrLeftPanel(filterStage) {
  const el = document.getElementById('cr-left-list');
  if (!el || !_ceReviewData) return;

  filterStage = filterStage || 'all';
  const angles = _ceReviewData.angles || [];

  const groups = { tof: [], mof: [], bof: [] };
  angles.forEach(a => {
    const s = (a.funnel_stage || '').toLowerCase();
    if (groups[s]) groups[s].push(a);
    else groups.tof.push(a);
  });

  const stageLabels = { tof: 'Top of Funnel', mof: 'Mid Funnel', bof: 'Bottom of Funnel' };
  let html = '';

  for (const [stage, stageAngles] of Object.entries(groups)) {
    if (stageAngles.length === 0) continue;
    if (filterStage !== 'all' && filterStage !== stage) continue;

    // Count total concepts in this stage
    let stageConceptCount = 0;
    stageAngles.forEach(a => stageConceptCount += (a.video_concepts || []).length);

    html += `<div class="cr-stage-group">`;
    html += `<div class="cr-stage-header">${stageLabels[stage] || stage.toUpperCase()} (${stageConceptCount})</div>`;

    stageAngles.forEach(angle => {
      (angle.video_concepts || []).forEach((c, i) => {
        const key = `${angle.angle_id}:${i}`;
        const state = _ceReviewState[key] || 'approved';
        const selected = key === _ceReviewSelectedKey ? 'selected' : '';

        html += `
        <div class="cr-concept-card ${state} ${selected}" data-key="${esc(key)}" onclick="crSelectConcept('${esc(key)}')">
          <div class="cr-card-top">
            <span class="cr-card-name">${esc(c.concept_name || `Concept ${i + 1}`)}</span>
            <span class="cr-card-format">${esc(c.video_format || '')}</span>
          </div>
          <div class="cr-card-bottom">
            <span class="cr-card-angle">${esc(angle.angle_name || angle.angle_id)}</span>
            <div class="cr-card-actions">
              <button class="cr-btn-approve ${state === 'approved' ? 'active' : ''}" data-key="${esc(key)}" onclick="event.stopPropagation(); crSetConceptState('${esc(key)}', 'approved')" title="Approve">&#10003;</button>
              <button class="cr-btn-reject ${state === 'rejected' ? 'active' : ''}" data-key="${esc(key)}" onclick="event.stopPropagation(); crSetConceptState('${esc(key)}', 'rejected')" title="Reject">&#10005;</button>
            </div>
          </div>
        </div>`;
      });
    });

    html += `</div>`;
  }

  el.innerHTML = html || '<div class="cr-empty-detail">No concepts found</div>';
}

function crSelectConcept(key) {
  _ceReviewSelectedKey = key;

  // Update selected state in left panel
  document.querySelectorAll('.cr-concept-card').forEach(card => {
    card.classList.toggle('selected', card.dataset.key === key);
  });

  // Render detail
  const found = findConceptByKey(key);
  if (!found) return;
  renderCrDetail(found.angle, found.concept, found.conceptIndex);

  // Scroll selected card into view
  const selectedCard = document.querySelector(`.cr-concept-card[data-key="${key}"]`);
  if (selectedCard) {
    selectedCard.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }
}

function renderCrDetail(angle, concept, conceptIndex) {
  const body = document.getElementById('cr-right-body');
  const title = document.getElementById('cr-right-title');
  if (!body) return;

  const key = `${angle.angle_id}:${conceptIndex}`;
  const state = _ceReviewState[key] || 'approved';

  if (title) title.textContent = `${angle.angle_id} / Concept ${conceptIndex + 1}`;

  const platforms = (concept.platform_targets || []).map(p =>
    `<span class="cr-platform-badge">${esc(humanize(p))}</span>`
  ).join('');

  body.innerHTML = `
    <div class="cr-detail-header">
      <div class="cr-detail-header-left">
        <div class="cr-detail-concept-name">${esc(concept.concept_name || `Concept ${conceptIndex + 1}`)}</div>
        <span class="cr-detail-format">${esc(concept.video_format || '')}</span>
      </div>
      <div class="cr-detail-actions">
        <button class="cr-detail-btn approve-btn ${state === 'approved' ? 'active' : ''}" onclick="crSetConceptState('${esc(key)}', 'approved')">&#10003; Approve</button>
        <button class="cr-detail-btn reject-btn ${state === 'rejected' ? 'active' : ''}" onclick="crSetConceptState('${esc(key)}', 'rejected')">&#10005; Reject</button>
      </div>
    </div>

    ${concept.scene_concept ? `<div class="cr-detail-scene">${escMultiline(concept.scene_concept)}</div>` : ''}

    ${platforms ? `<div class="cr-detail-platforms">${platforms}</div>` : ''}

    <div class="cr-detail-section">
      <div class="cr-detail-section-title">Angle Strategy</div>
      <div class="cr-detail-grid">
        ${ceDetailField('Angle', angle.angle_name)}
        ${ceDetailField('Target Segment', angle.target_segment)}
        ${ceDetailField('Awareness', humanize(angle.target_awareness || ''))}
        ${ceDetailField('Core Desire', angle.core_desire)}
        ${ceDetailField('Emotional Lever', angle.emotional_lever)}
        ${ceDetailField('VoC Anchor', angle.voc_anchor, true)}
        ${ceDetailField('White Space', angle.white_space_link)}
        ${ceDetailField('Mechanism', angle.mechanism_hint)}
        ${ceDetailField('Objection Addressed', angle.objection_addressed)}
      </div>
    </div>

    <div class="cr-detail-section">
      <div class="cr-detail-section-title">Concept Details</div>
      <div class="cr-detail-grid">
        ${concept.why_this_format ? ceDetailField('Why This Format', concept.why_this_format) : ''}
        ${concept.reference_examples ? ceDetailField('Reference Examples', concept.reference_examples) : ''}
        ${concept.sound_music_direction ? ceDetailField('Sound/Music', concept.sound_music_direction) : ''}
        ${concept.proof_approach ? ceDetailField('Proof Approach', humanize(concept.proof_approach)) : ''}
        ${concept.proof_description ? ceDetailField('Proof', concept.proof_description) : ''}
      </div>
    </div>
  `;
}

function crSetConceptState(key, newState) {
  const current = _ceReviewState[key];
  // Toggle: clicking the same state removes it (back to approved as default)
  if (current === newState) {
    _ceReviewState[key] = newState === 'approved' ? 'rejected' : 'approved';
  } else {
    _ceReviewState[key] = newState;
  }

  // Update left panel card
  const card = document.querySelector(`.cr-concept-card[data-key="${key}"]`);
  if (card) {
    const s = _ceReviewState[key];
    card.classList.remove('approved', 'rejected');
    card.classList.add(s);
    card.querySelector('.cr-btn-approve').classList.toggle('active', s === 'approved');
    card.querySelector('.cr-btn-reject').classList.toggle('active', s === 'rejected');
  }

  // Update detail panel buttons if this is the selected concept
  if (_ceReviewSelectedKey === key) {
    const s = _ceReviewState[key];
    document.querySelectorAll('.cr-detail-btn.approve-btn').forEach(b => b.classList.toggle('active', s === 'approved'));
    document.querySelectorAll('.cr-detail-btn.reject-btn').forEach(b => b.classList.toggle('active', s === 'rejected'));
  }

  updateCrStats();
  // Sync back to old checkbox state for compatibility
  _ceCheckboxState[key] = _ceReviewState[key] === 'approved';
}

function crApproveAll() {
  getAllConceptKeys(_ceReviewFilter).forEach(k => {
    _ceReviewState[k] = 'approved';
    _ceCheckboxState[k] = true;
  });
  renderCrLeftPanel(_ceReviewFilter);
  if (_ceReviewSelectedKey) {
    const found = findConceptByKey(_ceReviewSelectedKey);
    if (found) renderCrDetail(found.angle, found.concept, found.conceptIndex);
  }
  updateCrStats();
}

function crRejectAll() {
  getAllConceptKeys(_ceReviewFilter).forEach(k => {
    _ceReviewState[k] = 'rejected';
    _ceCheckboxState[k] = false;
  });
  renderCrLeftPanel(_ceReviewFilter);
  if (_ceReviewSelectedKey) {
    const found = findConceptByKey(_ceReviewSelectedKey);
    if (found) renderCrDetail(found.angle, found.concept, found.conceptIndex);
  }
  updateCrStats();
}

function crFirstPerAngle() {
  if (!_ceReviewData) return;
  _ceReviewData.angles.forEach(angle => {
    (angle.video_concepts || []).forEach((_, i) => {
      const key = `${angle.angle_id}:${i}`;
      _ceReviewState[key] = i === 0 ? 'approved' : 'rejected';
      _ceCheckboxState[key] = i === 0;
    });
  });
  renderCrLeftPanel(_ceReviewFilter);
  if (_ceReviewSelectedKey) {
    const found = findConceptByKey(_ceReviewSelectedKey);
    if (found) renderCrDetail(found.angle, found.concept, found.conceptIndex);
  }
  updateCrStats();
}

function crFilterConcepts(stage) {
  _ceReviewFilter = stage;
  renderCrFilters();
  renderCrLeftPanel(stage);
}

function updateCrStats() {
  const total = Object.keys(_ceReviewState).length;
  const approved = Object.values(_ceReviewState).filter(s => s === 'approved').length;
  const rejected = Object.values(_ceReviewState).filter(s => s === 'rejected').length;

  const statsEl = document.getElementById('cr-bottom-stats');
  if (statsEl) {
    statsEl.innerHTML = `
      <span class="cr-stat"><span class="cr-stat-dot approved"></span> ${approved} approved</span>
      <span class="cr-stat"><span class="cr-stat-dot rejected"></span> ${rejected} rejected</span>
    `;
  }

  const badge = document.getElementById('cr-total-count');
  if (badge) badge.textContent = `${total} concepts`;

  const btn = document.getElementById('cr-continue-btn');
  if (btn) btn.disabled = approved === 0;
}

function renderCrModelPicker() {
  const el = document.getElementById('cr-model-picker');
  if (!el) return;
  el.innerHTML = buildModelPicker('agent_04', 'Copywriter');
  // Rename the select so it doesn't conflict with the gate's select
  const sel = el.querySelector('#gate-model-select');
  if (sel) sel.id = 'cr-gate-model-select';
}

function getCrModelOverride() {
  const sel = document.getElementById('cr-gate-model-select');
  if (!sel || !sel.value) return null;
  const slashIdx = sel.value.indexOf('/');
  if (slashIdx < 0) return null;
  return {
    provider: sel.value.substring(0, slashIdx),
    model: sel.value.substring(slashIdx + 1),
  };
}

async function crContinuePhase() {
  const selections = getCeSelections();
  if (selections.length === 0) {
    alert('Please approve at least one concept.');
    return;
  }

  const btn = document.getElementById('cr-continue-btn');
  if (btn) { btn.textContent = 'Starting...'; btn.disabled = true; }

  const modelOverride = getCrModelOverride() || getGateModelOverride();

  try {
    const resp = await fetch('/api/select-concepts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ selected: selections, model_override: modelOverride }),
    });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      if (btn) { btn.textContent = 'Continue to Copywriter'; btn.disabled = false; }
      return;
    }
    closeConceptReviewDrawer();
    hidePhaseGate();
    showAbortButton(true);
    document.getElementById('pipeline-title').textContent = 'Building your ads...';
    document.getElementById('pipeline-subtitle').textContent = '';
  } catch (e) {
    alert('Failed to send selections: ' + e.message);
    if (btn) { btn.textContent = 'Continue to Copywriter'; btn.disabled = false; }
  }
}

async function loadAndOpenConceptReviewDrawer() {
  if (!cardPreviewCache['agent_02']) {
    try {
      const useBranch = activeBranchId;
      const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
      const url = useBranch
        ? `/api/branches/${activeBranchId}/outputs/agent_02${brandParam}`
        : `/api/outputs/agent_02${brandParam}`;
      const resp = await fetch(url);
      if (resp.ok) {
        const d = await resp.json();
        cardPreviewCache['agent_02'] = d.data;
      }
    } catch (e) {
      console.error('Failed to load agent_02 output for drawer:', e);
    }
  }
  if (cardPreviewCache['agent_02']) {
    openConceptReviewDrawer(cardPreviewCache['agent_02']);
  }
}

function openConceptReviewDrawerFromGate() {
  loadAndOpenConceptReviewDrawer();
}

// Navigate concepts with arrow keys when drawer is open
function crNavigateConcepts(direction) {
  const keys = getAllConceptKeys(_ceReviewFilter);
  if (keys.length === 0) return;
  const currentIdx = keys.indexOf(_ceReviewSelectedKey);
  let nextIdx;
  if (direction === 'next') {
    nextIdx = currentIdx < keys.length - 1 ? currentIdx + 1 : 0;
  } else {
    nextIdx = currentIdx > 0 ? currentIdx - 1 : keys.length - 1;
  }
  crSelectConcept(keys[nextIdx]);
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
// BRAND SELECTOR
// -----------------------------------------------------------

function normalizeAvailableAgents(value) {
  if (Array.isArray(value)) return value;
  if (value && typeof value === 'object') {
    return Object.entries(value)
      .filter(([, isAvailable]) => Boolean(isAvailable))
      .map(([slug]) => slug);
  }
  return [];
}

// -----------------------------------------------------------
// MODEL OVERRIDES (now handled per-agent at phase gates)
// -----------------------------------------------------------

function getModelOverrides() {
  // Model selection moved to inline phase gate pickers — always empty from Brief
  return {};
}

// -----------------------------------------------------------
// BRANCH MANAGEMENT
// -----------------------------------------------------------

let branches = [];        // [{id, label, status, inputs, completed_agents, ...}]
let activeBranchId = null; // currently selected branch ID
let pendingDefaultPhase2Setup = false;

async function loadBranches() {
  try {
    const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
    const resp = await fetch(`/api/branches${brandParam}`);
    branches = await resp.json();
    renderBranchTabs();
    updateBranchManagerVisibility();
  } catch (e) {
    console.error('Failed to load branches', e);
  }
}

function updateBranchManagerVisibility() {
  const manager = document.getElementById('branch-manager');
  if (!manager) return;

  // Show branch manager if Phase 1 is done (agent_01a output exists)
  const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
  fetch(`/api/outputs${brandParam}`)
    .then(r => r.json())
    .then(outputs => {
      const phase1Done = outputs.some(o => o.slug === 'agent_01a' && o.available);
      if (phase1Done) {
        manager.classList.remove('hidden');
      } else {
        manager.classList.add('hidden');
      }
    })
    .catch(() => {});
}

function renderBranchTabs() {
  const container = document.getElementById('branch-tabs');
  if (!container) return;

  // Auto-select first branch if nothing is active
  if (!activeBranchId && branches.length > 0) {
    activeBranchId = branches[0].id;
  }

  // Build pills for each branch
  const pills = branches.map((b, idx) => {
    const isActive = b.id === activeBranchId;
    const cls = ['branch-pill', isActive ? 'active' : ''].filter(Boolean).join(' ');

    const funnelInfo = b.inputs
      ? `${b.inputs.tof_count || 10}/${b.inputs.mof_count || 5}/${b.inputs.bof_count || 2}`
      : '10/5/2';

    const isDefault = idx === 0;
    const label = isDefault ? (b.label || 'Default') : esc(b.label);

    // Only show delete on non-default branches
    const deleteBtn = isDefault
      ? ''
      : `<button class="branch-pill-x" onclick="event.stopPropagation(); deleteBranch('${b.id}')" title="Remove branch">&times;</button>`;

    return `<button class="${cls}" onclick="switchBranch('${b.id}')" data-branch="${b.id}">` +
      `<span class="branch-dot ${b.status}"></span>` +
      `<span>${label}</span>` +
      `<span class="branch-funnel">${funnelInfo}</span>` +
      deleteBtn +
      `</button>`;
  }).join('');

  // Add the "+ Branch" button at the end
  const addBtn = `<button class="branch-add-btn" onclick="openNewBranchModal()">+ Branch</button>`;

  container.innerHTML = pills + addBtn;
}

async function switchBranch(branchId) {
  activeBranchId = branchId;
  renderBranchTabs();

  const branch = branches.find(b => b.id === branchId);
  if (!branch) return;

  // Clear existing Phase 2+ card states and previews
  ['agent_02', 'agent_04', 'agent_05'].forEach(slug => {
    setCardState(slug, 'waiting');
    delete cardPreviewCache[slug];
    closeCardPreview(slug);
  });

  // Refresh branches from server and set card states for the active branch
  try {
    const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
    const resp = await fetch(`/api/branches${brandParam}`);
    const freshBranches = await resp.json();
    branches = freshBranches; // update global so button logic has fresh data
    const fresh = freshBranches.find(b => b.id === branchId);
    if (fresh) {
      (fresh.available_agents || []).forEach(slug => {
        setCardState(slug, 'done');
      });
      (fresh.failed_agents || []).forEach(slug => {
        setCardState(slug, 'failed');
      });
    }
  } catch (e) {
    console.error('Failed to load branch state', e);
  }

  // Re-evaluate start buttons for this branch's state
  updatePhaseStartButtons();
  updateProgress();
}

function setNewBranchModalContent(defaultPhase2Start) {
  const title = document.getElementById('nb-title');
  const subtitle = document.getElementById('nb-subtitle');
  const createBtn = document.getElementById('nb-create-btn');
  if (!title || !subtitle || !createBtn) return;

  if (defaultPhase2Start) {
    title.textContent = 'Start Phase 2';
    subtitle.textContent = 'Set your ToF/MoF/BoF creative counts for your default branch before the first Phase 2 run.';
    createBtn.textContent = 'Start Phase 2';
  } else {
    title.textContent = 'New Creative Branch';
    subtitle.textContent = "Configure this branch's inputs. Uses the same research from Phase 1.";
    createBtn.textContent = 'Create & Run Phase 2';
  }
}

function openNewBranchModal(options = {}) {
  const defaultPhase2Start = Boolean(options.defaultPhase2Start);
  pendingDefaultPhase2Setup = defaultPhase2Start;
  setNewBranchModalContent(defaultPhase2Start);

  // Pre-fill from active branch (if any), else defaults
  const activeBranch = branches.find(b => b.id === activeBranchId);
  const tof = String(activeBranch?.inputs?.tof_count || 10);
  const mof = String(activeBranch?.inputs?.mof_count || 5);
  const bof = String(activeBranch?.inputs?.bof_count || 2);

  document.getElementById('nb-tof').value = tof;
  document.getElementById('nb-mof').value = mof;
  document.getElementById('nb-bof').value = bof;
  document.getElementById('nb-label').value = defaultPhase2Start ? 'Default' : '';

  document.getElementById('new-branch-modal').classList.remove('hidden');
  document.getElementById('nb-label').focus();
}

function closeNewBranchModal() {
  document.getElementById('new-branch-modal').classList.add('hidden');
  pendingDefaultPhase2Setup = false;
  setNewBranchModalContent(false);
}

async function createBranch() {
  const label = document.getElementById('nb-label').value.trim();
  const finalLabel = label || (pendingDefaultPhase2Setup ? 'Default' : '');
  const tof = parseInt(document.getElementById('nb-tof').value) || 10;
  const mof = parseInt(document.getElementById('nb-mof').value) || 5;
  const bof = parseInt(document.getElementById('nb-bof').value) || 2;
  const isDefaultPhase2Setup = pendingDefaultPhase2Setup;

  const btn = document.getElementById('nb-create-btn');
  if (btn) {
    btn.textContent = 'Creating...';
    btn.disabled = true;
  }

  try {
    // Create the branch
    const resp = await fetch('/api/branches', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        label: finalLabel,
        tof_count: tof,
        mof_count: mof,
        bof_count: bof,
        temperature: parseFloat(document.getElementById('nb-temp')?.value || '0.9'),
        model_overrides: {},
        brand: activeBrandSlug || '',
      }),
    });
    const branch = await resp.json();

    if (branch.error) {
      alert(branch.error);
      if (btn) { btn.textContent = 'Create & Run Phase 2'; btn.disabled = false; }
      return;
    }

    // Refresh branch list and select the new branch
    await loadBranches();
    activeBranchId = branch.id;
    renderBranchTabs();

    // Immediately run Phase 2 for this branch
    const runResult = await runBranch(branch.id, { silent: true });
    if (!runResult.ok) {
      // Button says "Create & Run" — if run can't start, rollback branch creation.
      const rollbackBrandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
      await fetch(`/api/branches/${branch.id}${rollbackBrandParam}`, { method: 'DELETE' });
      if (activeBranchId === branch.id) activeBranchId = null;
      await loadBranches();
      alert(runResult.error || 'Failed to run branch.');
      return;
    }

    pendingDefaultPhase2Setup = false;
    closeNewBranchModal();

  } catch (e) {
    alert('Failed to create branch: ' + e.message);
  } finally {
    if (btn) {
      btn.textContent = isDefaultPhase2Setup ? 'Start Phase 2' : 'Create & Run Phase 2';
      btn.disabled = false;
    }
  }
}

async function runBranch(branchId, options = {}) {
  const silent = Boolean(options.silent);
  const inputs = readForm();
  const brandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';

  try {
    const resp = await fetch(`/api/branches/${branchId}/run${brandParam}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        phases: [2, 3],
        inputs,
        model_overrides: {},
        brand: activeBrandSlug || '',
      }),
    });
    const data = await resp.json();
    if (data.error) {
      if (!silent) alert(data.error);
      return { ok: false, error: data.error };
    }
    // Pipeline view transition and state updates happen via WS
    return { ok: true, data };
  } catch (e) {
    const message = 'Failed to run branch: ' + e.message;
    if (!silent) alert(message);
    return { ok: false, error: message };
  }
}

async function deleteBranch(branchId) {
  const branch = branches.find(b => b.id === branchId);
  const label = branch ? branch.label : branchId;
  if (!confirm(`Delete branch "${label}"? This removes all its outputs.`)) return;

  try {
    const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
    await fetch(`/api/branches/${branchId}${brandParam}`, { method: 'DELETE' });

    // If the deleted branch was active, deselect
    if (activeBranchId === branchId) {
      activeBranchId = null;
      // Reset Phase 2+ cards
      ['agent_02', 'agent_04', 'agent_05'].forEach(slug => {
        setCardState(slug, 'waiting');
        delete cardPreviewCache[slug];
        closeCardPreview(slug);
      });
    }

    await loadBranches();
    updatePhaseStartButtons();
  } catch (e) {
    console.error('Delete branch failed', e);
  }
}

// Override card preview loading to use branch outputs when a branch is active
const _originalToggleCardPreview = toggleCardPreview;

// We'll replace toggleCardPreview with a branch-aware version below
async function toggleCardPreviewBranchAware(slug) {
  const card = document.getElementById(`card-${slug}`);
  const preview = document.getElementById(`preview-${slug}`);
  if (!card || !preview) return;

  if (!card.classList.contains('done')) return;

  // Toggle
  if (!preview.classList.contains('hidden')) {
    preview.classList.add('hidden');
    card.classList.remove('expanded');
    return;
  }

  preview.classList.remove('hidden');
  card.classList.add('expanded');
  preview.innerHTML = '<div class="card-preview-loading">Loading output...</div>';

  // Determine which source to load from
  const isPhase2Plus = ['agent_02', 'agent_04', 'agent_05'].includes(slug);
  const useBranch = isPhase2Plus && activeBranchId;

  if (!cardPreviewCache[slug]) {
    try {
      const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
      let url;
      if (useBranch) {
        url = `/api/branches/${activeBranchId}/outputs/${slug}${brandParam}`;
      } else {
        url = `/api/outputs/${slug}${brandParam}`;
      }
      const resp = await fetch(url);
      if (!resp.ok) {
        preview.innerHTML = '<div class="card-preview-error">Output not available for this branch.</div>';
        return;
      }
      const d = await resp.json();
      cardPreviewCache[slug] = d.data;
    } catch (e) {
      preview.innerHTML = '<div class="card-preview-error">Failed to load output.</div>';
      console.error('Failed to load preview for', slug, e);
      return;
    }
  }

  // Agent 02: open the concept review drawer instead of inline preview
  if (slug === 'agent_02') {
    preview.classList.add('hidden');
    card.classList.remove('expanded');
    openConceptReviewDrawer(cardPreviewCache[slug]);
    return;
  }

  // Render using the existing renderer
  // Save checkbox state before re-rendering (so it survives innerHTML replacement)
  saveCeCheckboxState();

  preview.innerHTML = `
    <div class="card-preview-header">
      <span class="card-preview-title">Output${useBranch ? ' (Branch)' : ''}</span>
      <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation(); closeCardPreview('${slug}')">Collapse</button>
    </div>
    <div class="card-preview-body">${renderOutput(cardPreviewCache[slug])}</div>
    <div class="card-chat" id="chat-${slug}" onclick="event.stopPropagation()">
      <div class="card-chat-header">
        <span class="card-chat-title">Chat with this output</span>
        <select class="card-chat-model" id="chat-model-${slug}">
          <option value="google/gemini-3.0-pro" selected>Gemini 3.0 Pro</option>
          <option value="anthropic/claude-opus-4-6">Claude Opus 4.6</option>
          <option value="openai/gpt-5.2">GPT 5.2</option>
          <option value="google/gemini-2.5-flash">Gemini 2.5 Flash</option>
        </select>
      </div>
      <div class="card-chat-messages" id="chat-messages-${slug}"></div>
      <div class="card-chat-input-row">
        <input type="text" class="card-chat-input" id="chat-input-${slug}"
          placeholder="Ask a question or request changes..."
          onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();event.stopPropagation();sendChatMessage('${slug}');}">
        <button class="btn btn-primary btn-sm card-chat-send" onclick="event.stopPropagation(); sendChatMessage('${slug}')">Send</button>
      </div>
    </div>
  `;

  // Restore checkbox state after re-render (for Creative Engine concept selections)
  restoreCeCheckboxState();
}

// -----------------------------------------------------------
// PIPELINE MAP
// -----------------------------------------------------------

async function startFromPhase(phase) {
  const inputs = readForm();

  // Determine which phases to run
  const phases = [];
  if (phase <= 2) phases.push(2);
  if (phase <= 3) phases.push(3);

  const btn = document.getElementById(`btn-start-phase-${phase}`);
  if (btn) {
    btn.textContent = 'Starting...';
    btn.disabled = true;
  }

  const fallbackBtnLabel = phase === 2 ? 'Start Phase 2' : 'Start Copywriter';

  // Phase 2+ is always branch-scoped.
  if (!activeBranchId && branches.length > 0) {
    activeBranchId = branches[0].id;
    renderBranchTabs();
  }

  // Read the inline model picker for this phase's first agent
  const modelOverrides = {};
  if (phase === 3) {
    const sel = document.getElementById('phase3-model-select');
    if (sel && sel.value) {
      const slashIdx = sel.value.indexOf('/');
      if (slashIdx > 0) {
        modelOverrides['agent_04'] = {
          provider: sel.value.substring(0, slashIdx),
          model: sel.value.substring(slashIdx + 1),
        };
      }
    }
  }

  // First Phase 2 run with no branches: ask for ToF/MoF/BoF counts before creating default branch.
  if (phase === 2 && !activeBranchId && branches.length === 0) {
    openNewBranchModal({ defaultPhase2Start: true });
    if (btn) { btn.textContent = fallbackBtnLabel; btn.disabled = false; }
    return;
  }

  if (!activeBranchId) {
    alert('No branch selected. Start Phase 2 to create/select a branch first.');
    if (btn) { btn.textContent = fallbackBtnLabel; btn.disabled = false; }
    return;
  }

  const brandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
  const endpoint = `/api/branches/${activeBranchId}/run${brandParam}`;
  const requestBody = {
    phases,
    inputs,
    model_overrides: modelOverrides,
    brand: activeBrandSlug || '',
  };

  try {
    const resp = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody),
    });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      if (btn) { btn.textContent = fallbackBtnLabel; btn.disabled = false; }
    }
    // Pipeline view transition happens via WS message
  } catch (e) {
    alert('Failed to start: ' + e.message);
    if (btn) { btn.textContent = fallbackBtnLabel; btn.disabled = false; }
  }
}

function updatePhaseStartButtons() {
  // Show "Start Phase X" buttons based on which prior phases have outputs on disk
  const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
  fetch(`/api/outputs${brandParam}`)
    .then(r => r.json())
    .then(outputs => {
      if (pipelineRunning) {
        document.querySelectorAll('.btn-start-phase').forEach(b => b.classList.add('hidden'));
        const r3 = document.getElementById('phase-3-start-row');
        if (r3) r3.classList.add('hidden');
        return;
      }

      const available = new Set(outputs.filter(o => o.available).map(o => o.slug));
      const phase1Done = available.has('agent_01a');

      // Show "Start Phase 2" if Phase 1 is done and the active branch hasn't run Phase 2 yet
      const btn2 = document.getElementById('btn-start-phase-2');
      if (btn2) {
        const activeBranch = branches.find(b => b.id === activeBranchId);
        const branchHasPhase2 = Boolean(
          activeBranch
          && ((activeBranch.available_agents || []).includes('agent_02')
              || (activeBranch.completed_agents || []).includes('agent_02'))
        );
        const showBtn2 = phase1Done && (!activeBranch || !branchHasPhase2);
        if (showBtn2) {
          btn2.classList.remove('hidden');
        } else {
          btn2.classList.add('hidden');
        }
      }

      // Show "Start Copywriter" only for the active branch.
      const activeBranch = branches.find(b => b.id === activeBranchId);
      const branchAvailable = new Set(activeBranch?.available_agents || []);
      const phase2Done = branchAvailable.has('agent_02');
      const phase3Done = branchAvailable.has('agent_04');
      const row3 = document.getElementById('phase-3-start-row');
      if (row3) {
        if (phase2Done && !phase3Done) {
          row3.classList.remove('hidden');
        } else {
          row3.classList.add('hidden');
        }
      }

      // Update branch manager visibility
      updateBranchManagerVisibility();
    })
    .catch(() => {});
}

// -----------------------------------------------------------

function togglePipelineMap() {
  const panel = document.getElementById('pipeline-map');
  panel.classList.toggle('hidden');
}

function toggleBrandSelector() {
  const panel = document.getElementById('brand-panel');
  brandSelectorOpen = !brandSelectorOpen;
  if (brandSelectorOpen) {
    panel.classList.remove('hidden');
    loadBrandList();
  } else {
    panel.classList.add('hidden');
  }
}

async function loadBrandList() {
  const list = document.getElementById('brand-list');
  list.innerHTML = '<div class="empty-state">Loading...</div>';

  try {
    const resp = await fetch('/api/brands');
    brandList = await resp.json();

    if (!brandList.length) {
      list.innerHTML = '<div class="empty-state">No brands yet. Fill out the brief and start a pipeline to create one.</div>';
      return;
    }

    list.innerHTML = brandList.map(b => {
      const isActive = b.slug === activeBrandSlug;
      const dateStr = b.updated_at || b.created_at || '';
      const availableAgents = normalizeAvailableAgents(b.available_agents);
      const agentCount = availableAgents.length;

      return `
        <div class="brand-card ${isActive ? 'active' : ''}" onclick="openBrand('${esc(b.slug)}')">
          <div class="brand-card-top">
            <span class="brand-card-name">${esc(b.brand_name)}</span>
            ${isActive ? '<span class="brand-card-badge active">Active</span>' : ''}
          </div>
          <div class="brand-card-meta">
            <span>${esc(b.product_name || '')}</span>
            <span>${esc(dateStr)}</span>
            ${agentCount ? `<span>${agentCount} agent${agentCount !== 1 ? 's' : ''}</span>` : ''}
          </div>
          <div class="brand-card-actions" onclick="event.stopPropagation()">
            <button class="btn btn-ghost btn-sm" onclick="deleteBrand('${esc(b.slug)}')">Delete</button>
          </div>
        </div>
      `;
    }).join('');
  } catch (e) {
    list.innerHTML = '<div class="empty-state">Failed to load brands.</div>';
    console.error('Failed to load brands', e);
  }
}

async function openBrand(slug) {
  try {
    // Touch last_opened_at and fetch full brand data
    const resp = await fetch(`/api/brands/${slug}/open`, { method: 'POST' });
    const brand = await resp.json();
    if (brand.error) {
      alert(brand.error);
      return;
    }

    activeBrandSlug = slug;

    // Close brand selector panel
    if (brandSelectorOpen) toggleBrandSelector();

    // Populate brief form with brand data
    if (brand.brief) {
      populateForm(brand.brief);
    }

    // Reset all pipeline cards
    resetAllCards();
    clearPreviewCache();

    // Restore card states from brand's available agents
    const availableAgents = normalizeAvailableAgents(brand.available_agents);
    if (availableAgents.length) {
      availableAgents.forEach(agentSlug => {
        setCardState(agentSlug, 'done');
      });
    }

    // Load brand's branches
    branches = brand.branches || [];
    activeBranchId = branches.length > 0 ? branches[0].id : null;
    renderBranchTabs();
    updateBranchManagerVisibility();

    // If a branch is active, restore its Phase 2+ card states
    if (activeBranchId) {
      const activeBranch = branches.find(b => b.id === activeBranchId);
      if (activeBranch) {
        (activeBranch.available_agents || []).forEach(s => setCardState(s, 'done'));
        (activeBranch.failed_agents || []).forEach(s => setCardState(s, 'failed'));
      }
    }

    updateProgress();
    updatePhaseStartButtons();

    // Update pipeline header
    document.getElementById('pipeline-title').textContent = brand.brand_name;
    document.getElementById('pipeline-subtitle').textContent = 'Click any card to view output.';

    // Navigate to pipeline view if there are outputs
    if (availableAgents.length > 0) {
      goToView('pipeline');
    } else {
      goToView('brief');
    }
  } catch (e) {
    console.error('Failed to open brand', e);
    alert('Failed to open brand.');
  }
}

async function deleteBrand(slug) {
  const brand = brandList.find(b => b.slug === slug);
  const name = brand ? brand.brand_name : slug;
  if (!confirm(`Delete brand "${name}"? This removes all outputs and can't be undone.`)) return;

  try {
    await fetch(`/api/brands/${slug}`, { method: 'DELETE' });

    // If the deleted brand was active, clear state
    if (activeBrandSlug === slug) {
      activeBrandSlug = null;
      resetAllCards();
      clearPreviewCache();
      branches = [];
      activeBranchId = null;
      renderBranchTabs();
    }

    loadBrandList();
  } catch (e) {
    console.error('Delete brand failed', e);
  }
}

// Handle Enter key in rename dialog and new branch modal
document.addEventListener('keydown', (e) => {
  // Concept review drawer shortcuts (highest priority when open)
  if (_ceReviewDrawerOpen) {
    if (e.key === 'Escape') { closeConceptReviewDrawer(); return; }
    if (e.key === 'ArrowDown' || e.key === 'j') { e.preventDefault(); crNavigateConcepts('next'); return; }
    if (e.key === 'ArrowUp' || e.key === 'k') { e.preventDefault(); crNavigateConcepts('prev'); return; }
    if (e.key === 'a' && _ceReviewSelectedKey) { crSetConceptState(_ceReviewSelectedKey, 'approved'); return; }
    if (e.key === 'r' && _ceReviewSelectedKey) { crSetConceptState(_ceReviewSelectedKey, 'rejected'); return; }
    return; // Don't process other shortcuts while drawer is open
  }

  if (e.key === 'Escape' && brandSelectorOpen) toggleBrandSelector();
  // New branch modal
  if (e.key === 'Enter' && !document.getElementById('new-branch-modal').classList.contains('hidden')) {
    createBranch();
  }
  if (e.key === 'Escape' && !document.getElementById('new-branch-modal').classList.contains('hidden')) {
    closeNewBranchModal();
  }
});

// -----------------------------------------------------------
// HEALTH CHECK
// -----------------------------------------------------------

let healthOk = false;

async function checkHealth() {
  try {
    const resp = await fetch('/api/health');
    const data = await resp.json();
    // Allow pipeline start when ANY provider is configured; model overrides
    // and per-agent defaults may use non-default providers.
    healthOk = Boolean(data.any_provider_configured);

    // Remove any existing warning
    const existing = document.getElementById('env-warning');
    if (existing) existing.remove();

    if (!healthOk) {
      const runBar = document.querySelector('.run-bar');
      const briefInner = runBar ? runBar.parentElement : document.querySelector('.view-inner');
      if (briefInner) {
        const warning = document.createElement('div');
        warning.id = 'env-warning';
        warning.className = 'env-warning';
        warning.innerHTML = `
          <strong>No API keys configured — pipeline will fail</strong>
          No LLM provider keys found. Copy the example env file and add at least one key:<br>
          <code>cp .env.example .env</code><br><br>
          Then add your API key (e.g. <code>OPENAI_API_KEY=sk-...</code>) and restart the server.
        `;

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
    } else {
      // Re-enable run when health is now OK (e.g., after config change + refresh).
      const btn = document.getElementById('btn-run');
      if (btn) {
        btn.disabled = false;
        btn.title = '';
      }
    }
  } catch (e) {
    console.error('Health check failed', e);
  }
}

// -----------------------------------------------------------
// AGENT MODEL LABELS
// -----------------------------------------------------------

let agentModelDefaults = {};  // slug -> { provider, model, label }

async function loadAgentModels() {
  try {
    const resp = await fetch('/api/agent-models');
    agentModelDefaults = await resp.json();
    updateModelTags();
  } catch (e) {
    console.error('Failed to load agent models', e);
  }
}

function updateModelTags() {
  // Model overrides now come from phase gates, not the brief page
  for (const slug in agentModelDefaults) {
    const tag = document.getElementById(`model-${slug}`);
    if (!tag) continue;

    let label = agentModelDefaults[slug].label;
    let provider = agentModelDefaults[slug].provider;

    // Deep Research agents: provider is "google" for the label
    if (label === 'Deep Research') provider = 'google';

    tag.textContent = label;
    tag.className = `agent-model-tag provider-${provider}`;
  }
}

function setModelTagFromWS(slug, modelLabel, provider) {
  const tag = document.getElementById(`model-${slug}`);
  if (!tag) return;
  tag.textContent = modelLabel;
  // Deep Research is google-powered
  if (modelLabel === 'Deep Research') provider = 'google';
  tag.className = `agent-model-tag provider-${provider || ''}`;
}

// -----------------------------------------------------------
// INIT
// -----------------------------------------------------------

connectWS();
// loadSample('animus'); // Disabled — no auto-population
checkHealth(); // Verify API keys are configured
loadAgentModels(); // Load per-agent model assignments for card labels
startStatusPolling(); // Fallback log/state hydration if websocket delivery drops

// Re-update model tags when user changes model settings on the brief page
document.addEventListener('change', (e) => {
  if (e.target.classList.contains('ms-select')) {
    updateModelTags();
  }
});

// Auto-load most recently opened brand on page load
async function initBrand() {
  let preferredSlug = activeBrandSlug;
  try {
    const statusResp = await fetch('/api/status');
    if (statusResp.ok) {
      const status = await statusResp.json();
      if (status.active_brand_slug) preferredSlug = status.active_brand_slug;
    }
  } catch (e) {
    console.error('Failed to load initial status', e);
  }

  try {
    const resp = await fetch('/api/brands');
    const brands = await resp.json();
    if (brands.length > 0) {
      const targetSlug = preferredSlug || brands[0].slug; // /api/brands sorted by last_opened_at DESC
      activeBrandSlug = targetSlug;

      // Load the brand's brief into the form
      const brandResp = await fetch(`/api/brands/${targetSlug}/open`, { method: 'POST' });
      const brandData = await brandResp.json();
      if (brandData.brief) {
        populateForm(brandData.brief);
      }

      // Restore Phase 1 card states
      const availableAgents = normalizeAvailableAgents(brandData.available_agents);
      if (!pipelineRunning && availableAgents.length) {
        availableAgents.forEach(agentSlug => {
          if (agentSlug === 'agent_01a') {
            setCardState(agentSlug, 'done');
          }
        });
      }
    }
  } catch (e) {
    console.error('Failed to init brand', e);
  }

  // Now load branches (needs activeBrandSlug set first)
  await loadBranches();
  updatePhaseStartButtons();
}

initBrand();
