// ============================================================
// Creative Maker — Dashboard (Redesigned)
// ============================================================

// -----------------------------------------------------------
// STATE
// -----------------------------------------------------------

let ws = null;
let reconnectTimer = null;
let selectedPhases = [1, 2, 3]; // Always run full pipeline — phase gates handle the pausing
// Model defaults per-agent in config.py — can be overridden via Model Settings in the Brief view
let availableOutputs = [];   // [{slug, name, phase, icon, available}, ...]
let resultIndex = 0;         // current result being viewed
let loadedResults = [];       // [{slug, name, icon, data}, ...]
let pipelineRunning = false;
let agentTimers = {};  // slug -> { startTime, intervalId }

const AGENT_NAMES = {
  agent_01a: 'Foundation Research',
  agent_02: 'Creative Engine',
  agent_04: 'Copywriter',
  agent_05: 'Hook Specialist',
  agent_07: 'Versioning Engine',
};

// How many agents total per phase selection
const AGENT_SLUGS = {
  1: ['agent_01a'],
  2: ['agent_02'],
  3: ['agent_04', 'agent_05', 'agent_07'],
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
        // Track active branch if set
        if (msg.active_branch) {
          activeBranchId = msg.active_branch;
        }
      } else {
        // Server says pipeline is NOT running — stop all timers
        if (pipelineRunning) {
          pipelineRunning = false;
          stopTimer();
          stopAllAgentTimers();
          setRunDisabled(false);
          showAbortButton(false);
        }
      }
      (msg.completed_agents || []).forEach(slug => setCardState(slug, 'done'));
      if (msg.current_agent) {
        setCardState(msg.current_agent, 'running');
        startAgentTimer(msg.current_agent);
      }
      updateProgress();
      (msg.log || []).forEach(e => appendLog(e));

      // Restore phase gate if pipeline is paused waiting for approval
      if (msg.waiting_for_approval && msg.gate_info) {
        showPhaseGate(msg.gate_info);
        showAbortButton(false);
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
      // If this is a branch run, track it
      if (msg.branch_id) {
        activeBranchId = msg.branch_id;
        renderBranchTabs();
      }
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
        ? 'Select concepts and choose model, then continue.'
        : `Choose model and start ${msg.next_agent_name || 'next agent'}.`;
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
  // Funnel counts
  out.tof_count = parseInt(document.getElementById('f-tof-count')?.value) || 10;
  out.mof_count = parseInt(document.getElementById('f-mof-count')?.value) || 5;
  out.bof_count = parseInt(document.getElementById('f-bof-count')?.value) || 2;
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
        model_overrides: {},
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

function showPhaseGate(gateInfo) {
  // gateInfo: { completed_agent, next_agent, next_agent_name, phase, show_concept_selection }
  showAbortButton(false);
  hidePhaseGate();

  const agentList = document.getElementById('agent-list');
  if (!agentList) return;

  const nextSlug = gateInfo.next_agent || '';
  const nextName = gateInfo.next_agent_name || 'Next Agent';
  const showConceptSelection = gateInfo.show_concept_selection || false;

  // Find the next agent's card and insert gate before it
  let insertBefore = null;
  const nextCard = document.getElementById(`card-${nextSlug}`);
  if (nextCard) {
    insertBefore = nextCard;
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
    selectionHtml = buildConceptSelectionUI();
  }

  // Build model picker
  const modelPickerHtml = buildModelPicker(nextSlug, nextName);

  const messageText = showConceptSelection
    ? `Creative Engine complete — select concepts, choose model, then start ${nextName}.`
    : `Review the outputs above, choose model, then start ${nextName}.`;

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
    { value: 'google/gemini-2.5-pro', label: 'Gemini 2.5 Pro' },
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
  // Instead of a duplicate selection panel, show a summary that reads from the output checkboxes
  const cbs = document.querySelectorAll('.ce-concept-cb');
  if (cbs.length === 0) {
    return '<div class="gate-no-data">Expand the Creative Engine output above to select/deselect concepts using the checkboxes, then continue.</div>';
  }

  return `<div class="gate-concept-summary">
    <div class="gate-summary-text">Use the checkboxes on each video concept above to select which ones to produce.</div>
    <div class="gate-quick-actions">
      <button class="btn btn-ghost btn-sm" onclick="gateSelectAll()">Select All</button>
      <button class="btn btn-ghost btn-sm" onclick="gateDeselectAll()">Deselect All</button>
      <button class="btn btn-ghost btn-sm" onclick="gateSelectFirst()">First Concept Per Angle</button>
    </div>
  </div>`;
}

function updateGateSelectionCount() {
  const cbs = document.querySelectorAll('.ce-concept-cb');
  const checked = document.querySelectorAll('.ce-concept-cb:checked');
  const el = document.getElementById('gate-selection-count');
  if (el) {
    el.textContent = `${checked.length} of ${cbs.length} concepts selected`;
    el.style.color = checked.length === 0 ? 'var(--error)' : '';
  }
  // Disable continue if nothing selected
  const btn = document.querySelector('#phase-gate-bar .btn-primary');
  if (btn && cbs.length > 0) {
    btn.disabled = checked.length === 0;
  }
  // Also update visual state
  updateCeSelectionCount();
}

function gateSelectAll() {
  document.querySelectorAll('.ce-concept-cb').forEach(cb => cb.checked = true);
  // Also update persistent state for all known keys
  for (const key of Object.keys(_ceCheckboxState)) _ceCheckboxState[key] = true;
  updateGateSelectionCount();
}

function gateDeselectAll() {
  document.querySelectorAll('.ce-concept-cb').forEach(cb => cb.checked = false);
  for (const key of Object.keys(_ceCheckboxState)) _ceCheckboxState[key] = false;
  updateGateSelectionCount();
}

function gateSelectFirst() {
  // Select only the first concept per angle
  const seen = new Set();
  document.querySelectorAll('.ce-concept-cb').forEach(cb => {
    const aid = cb.dataset.angleId;
    if (!seen.has(aid)) {
      cb.checked = true;
      seen.add(aid);
    } else {
      cb.checked = false;
    }
  });
  saveCeCheckboxState();
  updateGateSelectionCount();
}

function hidePhaseGate() {
  const existing = document.getElementById('phase-gate-bar');
  if (existing) existing.remove();
}

async function continuePhase(withConceptSelection) {
  const btn = document.querySelector('#phase-gate-bar .btn-primary');
  const modelOverride = getGateModelOverride();

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
  // Read from persistent state (works even if DOM was re-rendered)
  saveCeCheckboxState();
  const selections = [];
  for (const [key, checked] of Object.entries(_ceCheckboxState)) {
    if (checked) {
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

async function loadBranches() {
  try {
    const resp = await fetch('/api/branches');
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
  fetch('/api/outputs')
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

  if (branches.length === 0) {
    container.innerHTML = '<div class="branch-empty">No branches yet. Click "+ New Branch" to explore a creative direction.</div>';
    return;
  }

  container.innerHTML = branches.map(b => {
    const isActive = b.id === activeBranchId;
    const isRunning = b.status === 'running';
    const cls = [
      'branch-tab',
      isActive ? 'active' : '',
      isRunning ? 'running' : '',
    ].filter(Boolean).join(' ');

    const statusCls = `branch-tab-status ${b.status}`;
    const statusLabels = {
      pending: 'Ready',
      running: 'Running',
      completed: 'Done',
      failed: 'Failed',
    };
    const statusLabel = statusLabels[b.status] || b.status;

    const funnelInfo = b.inputs
      ? `${b.inputs.tof_count || 10}/${b.inputs.mof_count || 5}/${b.inputs.bof_count || 2}`
      : '10/5/2';

    return `
      <button class="${cls}" onclick="switchBranch('${b.id}')" data-branch="${b.id}">
        <span class="branch-tab-label">${esc(b.label)}</span>
        <span style="font-size:11px;color:var(--text-dim)">${funnelInfo}</span>
        <span class="${statusCls}">${statusLabel}</span>
        <span class="branch-tab-actions">
          <button class="branch-tab-delete" onclick="event.stopPropagation(); deleteBranch('${b.id}')" title="Delete branch">&times;</button>
        </span>
      </button>
    `;
  }).join('');
}

async function switchBranch(branchId) {
  activeBranchId = branchId;
  renderBranchTabs();

  const branch = branches.find(b => b.id === branchId);
  if (!branch) return;

  // Clear existing Phase 2+ card states and previews
  ['agent_02', 'agent_04', 'agent_05', 'agent_07'].forEach(slug => {
    setCardState(slug, 'waiting');
    delete cardPreviewCache[slug];
    closeCardPreview(slug);
  });

  // Load this branch's outputs and set card states
  try {
    const resp = await fetch('/api/branches');
    const freshBranches = await resp.json();
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

  // Hide/show "Start Phase 2" button based on branch status
  const btn2 = document.getElementById('btn-start-phase-2');
  if (btn2) {
    // Hide the regular start button when a branch is active
    btn2.classList.add('hidden');
  }

  // Show run button for pending branches
  updatePhaseStartButtons();
  updateProgress();
}

function openNewBranchModal() {
  // Pre-fill with current brief values
  const tof = document.getElementById('f-tof-count')?.value || '10';
  const mof = document.getElementById('f-mof-count')?.value || '5';
  const bof = document.getElementById('f-bof-count')?.value || '2';

  document.getElementById('nb-tof').value = tof;
  document.getElementById('nb-mof').value = mof;
  document.getElementById('nb-bof').value = bof;
  document.getElementById('nb-label').value = '';

  document.getElementById('new-branch-modal').classList.remove('hidden');
  document.getElementById('nb-label').focus();
}

function closeNewBranchModal() {
  document.getElementById('new-branch-modal').classList.add('hidden');
}

async function createBranch() {
  const label = document.getElementById('nb-label').value.trim();
  const tof = parseInt(document.getElementById('nb-tof').value) || 10;
  const mof = parseInt(document.getElementById('nb-mof').value) || 5;
  const bof = parseInt(document.getElementById('nb-bof').value) || 2;

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
        label,
        tof_count: tof,
        mof_count: mof,
        bof_count: bof,
        temperature: parseFloat(document.getElementById('nb-temp')?.value || '0.9'),
        model_overrides: {},
      }),
    });
    const branch = await resp.json();

    if (branch.error) {
      alert(branch.error);
      if (btn) { btn.textContent = 'Create & Run Phase 2'; btn.disabled = false; }
      return;
    }

    closeNewBranchModal();

    // Refresh branch list and select the new branch
    await loadBranches();
    activeBranchId = branch.id;
    renderBranchTabs();

    // Immediately run Phase 2 for this branch
    await runBranch(branch.id);

  } catch (e) {
    alert('Failed to create branch: ' + e.message);
  } finally {
    if (btn) { btn.textContent = 'Create & Run Phase 2'; btn.disabled = false; }
  }
}

async function runBranch(branchId) {
  const inputs = readForm();
  if (!inputs.brand_name || !inputs.product_name) {
    alert('Please fill in at least a Brand Name and Product Name on the Brief page.');
    return;
  }

  try {
    const resp = await fetch(`/api/branches/${branchId}/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        phases: [2, 3],
        inputs,
        model_overrides: {},
      }),
    });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      return;
    }
    // Pipeline view transition and state updates happen via WS
  } catch (e) {
    alert('Failed to run branch: ' + e.message);
  }
}

async function deleteBranch(branchId) {
  const branch = branches.find(b => b.id === branchId);
  const label = branch ? branch.label : branchId;
  if (!confirm(`Delete branch "${label}"? This removes all its outputs.`)) return;

  try {
    await fetch(`/api/branches/${branchId}`, { method: 'DELETE' });

    // If the deleted branch was active, deselect
    if (activeBranchId === branchId) {
      activeBranchId = null;
      // Reset Phase 2+ cards
      ['agent_02', 'agent_04', 'agent_05', 'agent_07'].forEach(slug => {
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
  const isPhase2Plus = ['agent_02', 'agent_04', 'agent_05', 'agent_07'].includes(slug);
  const useBranch = isPhase2Plus && activeBranchId;

  if (!cardPreviewCache[slug]) {
    try {
      let url;
      if (useBranch) {
        url = `/api/branches/${activeBranchId}/outputs/${slug}`;
      } else {
        url = `/api/outputs/${slug}`;
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
  // Only require brand/product for Phase 1-2 starts.
  // Phase 3+ loads brand info from saved Phase 1 output on the backend.
  if (phase <= 2 && (!inputs.brand_name || !inputs.product_name)) {
    alert('Please fill in at least a Brand Name and Product Name on the Brief page.');
    return;
  }

  // Determine which phases to run
  const phases = [];
  if (phase <= 2) phases.push(2);
  if (phase <= 3) phases.push(3);

  const btn = document.getElementById(`btn-start-phase-${phase}`);
  if (btn) {
    btn.textContent = 'Starting...';
    btn.disabled = true;
  }

  const quickMode = document.getElementById('cb-quick-mode')?.checked || false;

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

  try {
    const resp = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        phases,
        inputs,
        quick_mode: quickMode,
        model_overrides: modelOverrides,
      }),
    });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      if (btn) { btn.textContent = 'Start Copywriter'; btn.disabled = false; }
    }
    // Pipeline view transition happens via WS message
  } catch (e) {
    alert('Failed to start: ' + e.message);
    if (btn) { btn.textContent = 'Start Copywriter'; btn.disabled = false; }
  }
}

function updatePhaseStartButtons() {
  // Show "Start Phase X" buttons based on which prior phases have outputs on disk
  fetch('/api/outputs')
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

      // Show "Start Phase 2" only if Phase 1 is done AND no branches exist yet
      // (once branches exist, the branch manager handles Phase 2)
      const btn2 = document.getElementById('btn-start-phase-2');
      if (btn2) {
        if (phase1Done && branches.length === 0 && !available.has('agent_02')) {
          btn2.classList.remove('hidden');
        } else {
          btn2.classList.add('hidden');
        }
      }

      // Show "Start Copywriter" row if Creative Engine (Phase 2) is done but Copywriter hasn't run
      const phase2Done = available.has('agent_02');
      const phase3Done = available.has('agent_04');
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

    const completedAgents = run.agents.filter(a => a.status === 'completed' && a.data);
    if (!completedAgents.length) {
      alert('This run has no completed outputs to view.');
      return;
    }

    // Reset all cards first, then mark completed ones as done
    resetAllCards();
    clearPreviewCache();

    // Cache the history run's outputs so card previews work
    for (const a of completedAgents) {
      cardPreviewCache[a.agent_slug] = a.data;
      setCardState(a.agent_slug, 'done');
    }

    // Mark failed agents
    run.agents
      .filter(a => a.status === 'failed')
      .forEach(a => setCardState(a.agent_slug, 'failed', null, a.error || 'Failed'));

    updateProgress();

    // Update pipeline header with run info
    const runLabel = run.label || run.inputs?.brand_name || `Run #${run.id}`;
    const elapsed = run.elapsed_seconds ? `${run.elapsed_seconds}s` : '';
    const costSuffix = elapsed ? ` in ${elapsed}` : '';
    document.getElementById('pipeline-title').textContent = runLabel;
    document.getElementById('pipeline-subtitle').textContent =
      `${run.created_at} — ${completedAgents.length} agent${completedAgents.length !== 1 ? 's' : ''}${costSuffix}. Click any card to view output.`;

    // Navigate to pipeline view
    goToView('pipeline');
  } catch (e) {
    console.error('Failed to load run', e);
    alert('Failed to load this run.');
  }
}

function getAgentIcon(slug) {
  const icons = {
    agent_01a: '🔬',
    agent_02: '💡',
    agent_04: '✍️', agent_05: '🎣',
    agent_07: '🔀',
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

// Handle Enter key in rename dialog and new branch modal
document.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && renameRunId) saveRename();
  if (e.key === 'Escape' && renameRunId) closeRename();
  if (e.key === 'Escape' && historyOpen) toggleHistory();
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
loadBranches(); // Load existing branches

// Re-update model tags when user changes model settings on the brief page
document.addEventListener('change', (e) => {
  if (e.target.classList.contains('ms-select')) {
    updateModelTags();
  }
});

// Check if there are existing outputs on page load — restore card states
fetch('/api/outputs')
  .then(r => r.json())
  .then(outputs => {
    const hasAny = outputs.some(o => o.available);
    if (hasAny) {
      const resultsStep = document.querySelector('.step[data-step="results"]');
      if (resultsStep) resultsStep.title = 'Previous results available';

      // Restore Phase 1 card states always (shared)
      if (!pipelineRunning) {
        outputs.forEach(o => {
          if (o.available && o.slug === 'agent_01a') {
            setCardState(o.slug, 'done');
          }
        });
        // Phase 2+ cards: only mark done if no branches exist yet
        // (if branches exist, switching a branch will set the states)
        if (branches.length === 0) {
          outputs.forEach(o => {
            if (o.available && o.slug !== 'agent_01a') {
              setCardState(o.slug, 'done');
            }
          });
        }
      }
    }
    updatePhaseStartButtons();
  })
  .catch(() => {});
