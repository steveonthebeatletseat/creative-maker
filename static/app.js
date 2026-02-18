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
  foundation_research: 'Foundation Research',
  creative_engine: 'Matrix Planner',
  copywriter: 'Copywriter',
  hook_specialist: 'Hook Specialist',
};

const MATRIX_AWARENESS_LEVELS = [
  'unaware',
  'problem_aware',
  'solution_aware',
  'product_aware',
  'most_aware',
];

let matrixMaxPerCell = 50;
let phase2MatrixOnlyMode = false;
let phase3Disabled = false;
let phase3V2Enabled = false;
let phase3V2HooksEnabled = false;
let phase3V2ScenesEnabled = false;
let phase3V2ReviewerRoleDefault = 'client_founder';
let phase3V2SdkTogglesDefault = { core_script_drafter: false };
let phase3V2HookDefaults = {
  candidatesPerUnit: 20,
  finalVariantsPerUnit: 5,
  maxParallel: 4,
  maxRepairRounds: 1,
};
let phase3V2SceneDefaults = {
  maxParallel: 4,
  maxRepairRounds: 1,
  maxDifficulty: 8,
  maxConsecutiveMode: 3,
  minARollLines: 1,
};
let phase3V2Prepared = null;
let phase3V2RunsCache = [];
let phase3V2CurrentRunId = '';
let phase3V2CurrentRunDetail = null;
let phase3V2PollTimer = null;
let phase3V2LoadedBranchKey = '';
let phase3V2RevealedUnits = new Set();
let phase3V2ControlsKey = '';
let phase3V2Preparing = false;
let phase3V2PreparedAt = '';
let phase3V2HooksPrepared = null;
let phase3V2ExpandedArms = [];
let phase3V2ExpandedIndex = -1;
let phase3V2ExpandedCurrent = null;
let phase3V2ArmTab = 'script';
let phase3V2ChatPendingDraft = null;
let phase3V2HookExpandedItems = [];
let phase3V2HookExpandedIndex = -1;
let phase3V2HookExpandedCurrent = null;
let phase3V2HookTab = 'hook';
let phase3V2HookChatPendingHook = null;
let phase3V2SceneExpandedItems = [];
let phase3V2SceneExpandedIndex = -1;
let phase3V2SceneExpandedCurrent = null;
let phase3V2SceneTab = 'scene';
let phase3V2SceneChatPendingPlan = null;
let phase3V2DraggingEditorRow = null;
let phase3V2SceneDraggingRow = null;
let phase3V2UnitFilters = { awareness: 'all', emotion: 'all' };
let phase3V2Collapsed = true;
let phase3V2HooksCollapsed = true;
let phase3V2ScenesCollapsed = true;

// How many agents total per phase selection
const AGENT_SLUGS = {
  1: ['foundation_research'],
  2: ['creative_engine'],
  3: ['copywriter', 'hook_specialist'],
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
      (msg.log || []).forEach(e => appendServerLog(e));
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
        document.getElementById('pipeline-subtitle').textContent =
          msg.gate_info.message || 'Review the outputs above, then click Continue when ready.';
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
      clearServerLog();
      resetCost();
      updateModelTags();
      goToView('pipeline');
      startTimer();
      showAbortButton(true);
      openLiveTerminal();
      // Hide phase start buttons during pipeline run
      document.querySelectorAll('.btn-start-phase').forEach(b => b.classList.add('hidden'));
      const v2Panel = document.getElementById('phase-3-v2-panel');
      const hooksPanel = document.getElementById('phase3-v2-hooks-panel');
      const scenesPanel = document.getElementById('phase3-v2-scenes-panel');
      if (v2Panel) v2Panel.classList.add('hidden');
      if (hooksPanel) hooksPanel.classList.add('hidden');
      if (scenesPanel) scenesPanel.classList.add('hidden');
      {
        const branchLabel = msg.branch_id ? branches.find(b => b.id === msg.branch_id)?.label : null;
        document.getElementById('pipeline-title').textContent = branchLabel
          ? `Running: ${branchLabel}`
          : '';
        document.getElementById('pipeline-subtitle').textContent = '';
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
        appendServerLog({ time: ts(), level: 'warning', message: msg.message });
      }
      break;

    case 'phase_start':
      appendServerLog({ time: ts(), level: 'info', message: `Phase ${msg.phase} started` });
      break;

    case 'agent_start':
      setCardState(msg.slug, 'running');
      startAgentTimer(msg.slug);
      if (msg.model) setModelTagFromWS(msg.slug, msg.model, msg.provider);
      updateProgress();
      {
        const modelSuffix = msg.model ? ` [${msg.model}]` : '';
        appendServerLog({ time: ts(), level: 'info', message: `Starting ${msg.name}${modelSuffix}...` });
      }
      scrollToCard(msg.slug);
      break;

    case 'stream_progress':
      // Live streaming progress from LLM — update the activity log
      appendServerLog({ time: ts(), level: 'info', message: `${AGENT_NAMES[msg.slug] || msg.slug}: ${msg.message}` });
      break;

    case 'agent_complete':
      stopAgentTimer(msg.slug);
      setCardState(msg.slug, 'done', msg.elapsed);
      updateProgress();
      updateCost(msg.cost);
      appendServerLog({ time: ts(), level: 'success', message: `${msg.name} completed (${msg.elapsed}s)` });
      if (msg.slug === 'foundation_research' && msg.phase1_step === 'collectors_complete') {
        appendServerLog({
          time: ts(),
          level: 'info',
          message: 'Phase 1 Step 1/2 complete — collector outputs are ready (Step 2 auto-starts).',
        });
        document.getElementById('pipeline-title').textContent = 'Phase 1 Step 1/2 complete';
        document.getElementById('pipeline-subtitle').textContent =
          'Collector outputs saved. Step 2 is starting automatically.';
      }
      if (msg.slug === 'foundation_research') {
        // Force-refresh Foundation preview once Step 2 completes so users do not
        // keep seeing stale Step 1 snapshot content from in-memory cache.
        if (msg.phase1_step !== 'collectors_complete') {
          delete cardPreviewCache.foundation_research;
          const preview = document.getElementById('preview-foundation_research');
          if (preview && !preview.classList.contains('hidden')) {
            closeCardPreview('foundation_research');
            setTimeout(() => { toggleCardPreviewBranchAware('foundation_research'); }, 0);
          }
        }
        handleFoundationQualityGateVisibility(msg.quality_gate_report || null);
      }
      if (msg.slug === 'creative_engine') {
        // Ensure branch state is refreshed immediately after Phase 2 so
        // Phase 3 v2 controls appear without requiring a full page refresh.
        loadBranches();
      }
      break;

    case 'agent_error':
      stopAgentTimer(msg.slug);
      setCardState(msg.slug, 'failed', null, msg.error);
      updateProgress();
      appendServerLog({ time: ts(), level: 'error', message: `${msg.name} failed: ${msg.error}` });
      if (msg.slug === 'foundation_research' && msg.quality_gate_report) {
        handleFoundationQualityGateVisibility(msg.quality_gate_report);
      }
      break;

    case 'server_log':
      // Stream of real server-side log lines
      appendServerLogLines(msg.lines || []);
      break;

    case 'phase_gate':
      showPhaseGate(msg);
      appendServerLog({ time: ts(), level: 'info', message: msg.next_agent_name ? `${msg.next_agent_name} ready` : 'Review and continue' });
      document.getElementById('pipeline-title').textContent = msg.next_agent_name
        ? `Ready: ${msg.next_agent_name}`
        : 'Review and continue';
      document.getElementById('pipeline-subtitle').textContent = msg.message || (
        msg.show_concept_selection
          ? 'Review concepts, then continue.'
          : `Choose model and start ${msg.next_agent_name || 'next agent'}.`
      );
      // Auto-open concept review drawer
      if (msg.show_concept_selection) {
        loadAndOpenConceptReviewDrawer();
      }
      break;

    case 'phase_gate_cleared':
      hidePhaseGate();
      document.getElementById('pipeline-title').textContent = '';
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
        document.getElementById('pipeline-subtitle').textContent = `Pipeline finished in ${msg.elapsed}s${costSuffix}.`;
      }
      appendServerLog({ time: ts(), level: 'success', message: `Pipeline complete in ${msg.elapsed}s` });
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
        appendServerLog({
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
  const isFoundation = slug === 'foundation_research';

  card.className = `agent-card ${state}`;
  const badge = card.querySelector('.status-badge');
  if (!badge) return;

  // Remove any existing error tooltip
  const existing = card.querySelector('.card-error');
  if (existing) existing.remove();
  const warning = card.querySelector('.card-warning');
  if (warning) warning.remove();

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
      badge.textContent = elapsed ? `Completed in ${elapsed}s` : 'Completed';
      if (rerunGroup) {
        if (isFoundation) rerunGroup.classList.add('hidden');
        else rerunGroup.classList.remove('hidden');
      }
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
    const err = card.querySelector('.card-error');
    if (err) err.remove();
    const warning = card.querySelector('.card-warning');
    if (warning) warning.remove();
  });
  const progressFill = document.getElementById('progress-fill');
  if (progressFill) {
    progressFill.style.width = '0%';
  }
}

function scrollToCard(slug) {
  const card = document.getElementById(`card-${slug}`);
  if (card) card.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

function updateProgress() {
  const doneCount = document.querySelectorAll('.agent-card.done').length;
  const total = document.querySelectorAll('.agent-card').length;
  const pct = total > 0 ? Math.round((doneCount / total) * 100) : 0;
  const progressFill = document.getElementById('progress-fill');
  if (progressFill) {
    progressFill.style.width = pct + '%';
  }
}

// -----------------------------------------------------------
// TIMER
// -----------------------------------------------------------

function startTimer() { /* elapsed display removed */ }
function stopTimer() { /* elapsed display removed */ }

function ts() {
  return new Date().toLocaleTimeString('en-US', { hour12: false });
}

function esc(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

function formatFetchError(e, actionLabel = 'request') {
  const raw = String((e && e.message) ? e.message : e || '').trim();
  const lowered = raw.toLowerCase();
  if (lowered.includes('failed to fetch') || lowered.includes('networkerror')) {
    return `Cannot reach backend for ${actionLabel}. Make sure server is running at http://localhost:8000, then retry.`;
  }
  return raw || `Unexpected error during ${actionLabel}.`;
}

function gateLabel(gateId) {
  const labels = {
    global_evidence_coverage: 'Global Evidence Coverage',
    source_contradiction_audit: 'Source Contradiction Audit',
    pillar_1_profile_completeness: 'Pillar 1 Profile Completeness',
    pillar_2_voc_depth: 'Pillar 2 VOC Depth',
    pillar_3_competitive_depth: 'Pillar 3 Competitive Depth',
    pillar_4_mechanism_strength: 'Pillar 4 Mechanism Strength',
    pillar_5_awareness_validity: 'Pillar 5 Awareness Validity',
    pillar_6_emotion_dominance: 'Pillar 6 Emotion Dominance',
    pillar_7_proof_coverage: 'Pillar 7 Proof Coverage',
    cross_pillar_consistency: 'Cross-Pillar Consistency',
  };
  return labels[gateId] || humanize(gateId || 'unknown_gate');
}

let _foundationQualityDigest = '';

function qualityReportDigest(report) {
  if (!report || typeof report !== 'object') return '';
  const failed = Array.isArray(report.failed_gate_ids) ? report.failed_gate_ids.join('|') : '';
  const checks = Array.isArray(report.checks) ? report.checks.filter(c => c && c.passed === false).map(c => `${c.gate_id}:${c.actual || ''}`).join('|') : '';
  return `${report.overall_pass ? 'pass' : 'fail'}:${failed}:${checks}:${report.retry_rounds_used || 0}`;
}

function clearFoundationQualityBanner() {
  const card = document.getElementById('card-foundation_research');
  if (!card) return;
  card.querySelectorAll('.card-warning').forEach(el => el.remove());
}

function renderFoundationQualityBanner(report) {
  clearFoundationQualityBanner();
  const card = document.getElementById('card-foundation_research');
  if (!card || !report || typeof report !== 'object') return;

  const overallPass = Boolean(report.overall_pass);
  const retries = Number(report.retry_rounds_used || 0);
  const checks = Array.isArray(report.checks) ? report.checks : [];
  const failedChecks = checks.filter(c => c && c.passed === false);
  if (overallPass || failedChecks.length === 0) return;

  const topFailures = failedChecks.slice(0, 3).map((check) => {
    const required = String(check.required || 'n/a');
    const actual = String(check.actual || 'n/a');
    return `<li><strong>${esc(gateLabel(check.gate_id))}</strong><br><span>Required: ${esc(required)}</span><br><span>Actual: ${esc(actual)}</span></li>`;
  }).join('');

  const extraCount = Math.max(0, failedChecks.length - 3);
  const extraText = extraCount > 0 ? `<div class="card-warning-extra">+${extraCount} more failed gate${extraCount > 1 ? 's' : ''} in output details.</div>` : '';

  const banner = document.createElement('div');
  banner.className = 'card-warning';
  banner.innerHTML = `
    <div class="card-warning-title">Quality Gates Failed (retries: ${retries})</div>
    <ul class="card-warning-list">${topFailures}</ul>
    ${extraText}
  `;
  card.appendChild(banner);
}

function appendFoundationQualityLogs(report) {
  if (!report || typeof report !== 'object') return;
  const digest = qualityReportDigest(report);
  if (!digest || digest === _foundationQualityDigest) return;
  _foundationQualityDigest = digest;

  const overallPass = Boolean(report.overall_pass);
  const failed = Array.isArray(report.failed_gate_ids) ? report.failed_gate_ids : [];
  const checks = Array.isArray(report.checks) ? report.checks : [];
  const failedChecks = checks.filter(c => c && c.passed === false);
  const retries = Number(report.retry_rounds_used || 0);

  if (overallPass) {
    appendServerLog({ time: ts(), level: 'success', message: `Phase 1 quality gates passed (retries: ${retries}).` });
    return;
  }

  appendServerLog({
    time: ts(),
    level: 'warning',
    message: `Phase 1 quality gates failed: ${failed.map(g => gateLabel(g)).join(', ') || 'Unknown'}`,
  });
  failedChecks.slice(0, 5).forEach((check) => {
    const required = String(check.required || 'n/a');
    const actual = String(check.actual || 'n/a');
    const details = String(check.details || '').trim();
    const detailsSuffix = details ? ` | Details: ${details.slice(0, 180)}` : '';
    appendServerLog({
      time: ts(),
      level: 'warning',
      message: `${gateLabel(check.gate_id)} — Required: ${required} | Actual: ${actual}${detailsSuffix}`,
    });
  });
}

async function fetchFoundationQualityReport() {
  try {
    const brandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
    const resp = await fetch(`/api/outputs/foundation_research${brandParam}`);
    if (!resp.ok) return null;
    const payload = await resp.json();
    const data = payload && payload.data ? payload.data : null;
    return data && typeof data === 'object' ? data.quality_gate_report || null : null;
  } catch (_) {
    return null;
  }
}

async function handleFoundationQualityGateVisibility(qualityReport) {
  let report = qualityReport;
  if (!report) {
    report = await fetchFoundationQualityReport();
  }
  if (!report) return;
  renderFoundationQualityBanner(report);
  appendFoundationQualityLogs(report);
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

function appendServerLog(entry) {
  const level = String(entry?.level || '').trim().toUpperCase();
  const time = String(entry?.time || ts()).trim();
  const message = String(entry?.message || '').trim();
  if (!message) return;
  const levelPrefix = level ? `[${level}] ` : '';
  appendServerLogLines([`${time} ${levelPrefix}${message}`]);
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

  try {
    const resp = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        phases: [1],
        inputs,
        quick_mode: false,
        model_overrides: {},
        phase1_step_review: false,
      }),
    });
    const data = await resp.json();
    if (data.error) {
      alert(data.error);
      setRunDisabled(false);
    } else if (data.brand_slug) {
      activeBrandSlug = data.brand_slug;
      loadBrandList();
    }
    // Pipeline view transition happens via WS message
  } catch (e) {
    alert(formatFetchError(e, 'pipeline start'));
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
  const gateMode = gateInfo.gate_mode || 'standard';
  const isPhase1CollectorsGate = gateMode === 'phase1_collectors_review';
  const failedCopywriterCount =
    gateInfo.completed_agent === 'copywriter' ? (gateInfo.copywriter_failed_count || 0) : 0;

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
    const cachedData = cardPreviewCache['creative_engine'];
    if (cachedData) {
      _ceReviewData = cachedData;
      initCeReviewState(cachedData);
    }
    selectionHtml = buildConceptSelectionUI();
  }
  if (isPhase1CollectorsGate) {
    selectionHtml += buildPhase1CollectorsGateSummary(gateInfo);
  }

  // Build model picker
  const modelPickerHtml = buildModelPicker(nextSlug, nextName);

  const messageText = gateInfo.message || (showConceptSelection
    ? `Creative Engine complete — select concepts, choose model, then start ${nextName}.`
    : `Review the outputs above, choose model, then start ${nextName}.`);
  const primaryLabel = gateInfo.continue_label || `Start ${nextName}`;

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
          ${esc(primaryLabel)}
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

  // For Foundation Research Step 2, show the real backend default model.
  if (slug === 'foundation_research') {
    options[0].label = 'Claude Opus 4.6 (default)';
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

function buildPhase1CollectorsGateSummary(gateInfo) {
  const collectorSummary = Array.isArray(gateInfo.collector_summary) ? gateInfo.collector_summary : [];
  const evidenceCount = gateInfo.evidence_count || 0;
  const evidenceSummary = gateInfo.evidence_summary || {};
  const providerDist = evidenceSummary.provider_distribution || {};
  const sourceDist = evidenceSummary.source_type_distribution || {};

  const collectorRows = collectorSummary.length
    ? collectorSummary.map((row) => {
        const ok = row.success ? 'ok' : 'fail';
        const error = row.error ? `<div class="phase1-gate-error">${esc(row.error)}</div>` : '';
        return `
          <div class="phase1-gate-collector ${ok}">
            <div class="phase1-gate-collector-head">
              <span class="phase1-gate-provider">${esc(row.provider || 'unknown')}</span>
              <span class="phase1-gate-status">${row.success ? 'success' : 'failed'}</span>
            </div>
            <div class="phase1-gate-metrics">
              <span>${row.report_chars || 0} chars</span>
              <span>${row.evidence_rows || 0} direct rows</span>
            </div>
            ${error}
          </div>
        `;
      }).join('')
    : '<div class="phase1-gate-empty">No collector summary available.</div>';

  const providerChips = Object.keys(providerDist).length
    ? Object.entries(providerDist).map(([k, v]) =>
        `<span class="phase1-gate-chip">${esc(k)}: ${v}</span>`
      ).join('')
    : '<span class="phase1-gate-chip muted">No provider mix yet</span>';

  const sourceChips = Object.keys(sourceDist).length
    ? Object.entries(sourceDist).map(([k, v]) =>
        `<span class="phase1-gate-chip">${esc(k)}: ${v}</span>`
      ).join('')
    : '<span class="phase1-gate-chip muted">No source mix yet</span>';

  return `
    <div class="phase1-gate-summary">
      <div class="phase1-gate-top">
        <div class="phase1-gate-count">Evidence collected: <strong>${evidenceCount}</strong></div>
        <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation(); toggleCardPreview('foundation_research')">
          View Collector Snapshot
        </button>
      </div>
      <div class="phase1-gate-collectors">${collectorRows}</div>
      <div class="phase1-gate-dist">
        <div class="phase1-gate-dist-row"><span class="phase1-gate-dist-label">Providers</span>${providerChips}</div>
        <div class="phase1-gate-dist-row"><span class="phase1-gate-dist-label">Sources</span>${sourceChips}</div>
      </div>
    </div>
  `;
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
  const originalLabel = btn ? btn.textContent : 'Continue';
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
      if (btn) { btn.textContent = originalLabel; btn.disabled = false; }
      return;
    }
    showAbortButton(true);
    document.getElementById('pipeline-title').textContent = '';
    document.getElementById('pipeline-subtitle').textContent = '';
    } catch (e) {
      alert('Failed to send selections: ' + e.message);
      if (btn) { btn.textContent = originalLabel; btn.disabled = false; }
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
    if (btn) { btn.textContent = originalLabel; btn.disabled = false; }
    return;
  }
  showAbortButton(true);
  document.getElementById('pipeline-title').textContent = '';
  document.getElementById('pipeline-subtitle').textContent = 'The agents are working.';
  } catch (e) {
    alert('Failed to continue: ' + e.message);
    if (btn) { btn.textContent = originalLabel; btn.disabled = false; }
  }
}

async function rewriteFailedCopywriter() {
  const btn = document.getElementById('btn-rewrite-failed-copywriter');
  const modelOverride = getGateModelOverride();

  if (btn) {
    btn.textContent = 'Rewriting...';
    btn.disabled = true;
  }

  setCardState('copywriter', 'running');
  startAgentTimer('copywriter');

  try {
    const resp = await fetch('/api/rewrite-failed-copywriter', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model_override: modelOverride }),
    });
    const data = await resp.json();

    stopAgentTimer('copywriter');

    if (data.error) {
      setCardState('copywriter', 'failed', null, data.error);
      alert(data.error);
      if (btn) {
        btn.textContent = 'Rewrite Failed';
        btn.disabled = false;
      }
      return;
    }

    setCardState('copywriter', 'done');
    if (data.cost) updateCost(data.cost);
    delete cardPreviewCache['copywriter'];
    closeCardPreview('copywriter');

    const rewritten = data.rewritten || 0;
    const remaining = data.remaining_failed || 0;
    appendServerLog({
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
    stopAgentTimer('copywriter');
    setCardState('copywriter', 'failed', null, e.message);
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

function toggleRerunMenuFor(slug, menuId) {
  const menu = document.getElementById(menuId);
  if (!menu) return;

  // Close all other open menus first
  document.querySelectorAll('.rerun-menu').forEach(m => {
    if (m.id !== menuId) m.classList.add('hidden');
  });

  if (!menu.classList.contains('hidden')) {
    menu.classList.add('hidden');
    return;
  }

  // Populate the menu
  const currentDefault = agentModelDefaults[slug];
  const defaultLabel = slug === 'foundation_research'
    ? 'Claude Opus 4.6'
    : (currentDefault ? currentDefault.label : 'Default');

  menu.innerHTML = RERUN_MODEL_OPTIONS.map(opt => {
    const isDefault = !opt.provider;
    const label = isDefault ? `${defaultLabel} (default)` : opt.label;
    const providerClass = opt.provider ? `provider-${opt.provider}` : (currentDefault ? `provider-${currentDefault.provider}` : '');
    return `<button class="rerun-menu-item ${providerClass}" onclick="event.stopPropagation(); rerunAgent('${slug}', ${opt.provider ? `'${opt.provider}'` : 'null'}, ${opt.model ? `'${opt.model}'` : 'null'})">${esc(label)}</button>`;
  }).join('');

  menu.classList.remove('hidden');
}

function toggleRerunMenu(slug) {
  return toggleRerunMenuFor(slug, `rerun-menu-${slug}`);
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
  appendServerLog({ time: ts(), level: 'info', message: `Rerunning ${slug} [${modelLabel}]...` });

  // Update the model tag on the card
  if (overrideProvider && overrideModel) {
    setModelTagFromWS(slug, _labelMap[overrideModel] || overrideModel, overrideProvider);
  }

  const body = { slug, inputs, quick_mode: false };
  if (slug === 'creative_engine' && activeBranchId) {
    const activeBranch = branches.find(b => b.id === activeBranchId);
    const matrixCells = Array.isArray(activeBranch?.inputs?.matrix_cells)
      ? activeBranch.inputs.matrix_cells
      : [];
    body.inputs.matrix_cells = matrixCells;
  }
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
      appendServerLog({ time: ts(), level: 'error', message: `Rerun ${slug} failed: ${data.error}` });
      return;
    }

    // Success
    setCardState(slug, 'done', data.elapsed);
    if (data.cost) updateCost(data.cost);
    appendServerLog({ time: ts(), level: 'success', message: `${slug} rerun completed (${data.elapsed}s)` });
  } catch (e) {
    stopAgentTimer(slug);
    const errMsg = formatFetchError(e, `${slug} rerun`);
    setCardState(slug, 'failed', null, errMsg);
    appendServerLog({ time: ts(), level: 'error', message: `Rerun ${slug} error: ${errMsg}` });
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

function openOutputFullscreenModal(slug) {
  const preview = document.getElementById(`preview-${slug}`);
  const modal = document.getElementById('output-fullscreen-modal');
  const body = document.getElementById('output-fullscreen-body');
  const title = document.getElementById('output-fullscreen-title');
  if (!preview || !modal || !body || !title) return;

  const sourceBody = preview.querySelector('.card-preview-body');
  if (!sourceBody) return;

  const cardTitle = (AGENT_NAMES[slug] || humanize(slug || 'output')).trim();
  title.textContent = `${cardTitle} — Full Screen`;
  body.innerHTML = sourceBody.innerHTML;
  modal.classList.remove('hidden');
}

function closeOutputFullscreenModal() {
  const modal = document.getElementById('output-fullscreen-modal');
  const body = document.getElementById('output-fullscreen-body');
  if (modal) modal.classList.add('hidden');
  if (body) body.innerHTML = '';
}

function switchFoundationOutputView(mode, triggerEl = null) {
  const root = (triggerEl && triggerEl.closest('.foundation-output-switcher'))
    || document.getElementById('foundation-output-switcher');
  if (!root) return;
  const finalPanel = root.querySelector('[data-foundation-panel="final"]');
  const step1Panel = root.querySelector('[data-foundation-panel="step1"]');
  const finalBtn = root.querySelector('[data-foundation-tab="final"]');
  const step1Btn = root.querySelector('[data-foundation-tab="step1"]');
  const showFinal = mode !== 'step1';

  if (finalPanel) finalPanel.classList.toggle('hidden', !showFinal);
  if (step1Panel) step1Panel.classList.toggle('hidden', showFinal);
  if (finalBtn) finalBtn.classList.toggle('active', showFinal);
  if (step1Btn) step1Btn.classList.toggle('active', !showFinal);
}

function renderFoundationOutputSwitcher(finalData, collectorsSnapshot, useBranch) {
  if (!collectorsSnapshot || typeof collectorsSnapshot !== 'object') {
    return renderOutput(finalData);
  }
  const finalReady = !isPhase1CollectorsSnapshot(finalData);
  const finalHtml = finalReady
    ? renderOutput(finalData)
    : `<div class="empty-state">Step 2 final brief is not ready yet. Step 1 collector snapshot is available now.</div>`;
  return `
    <div class="foundation-output-switcher" id="foundation-output-switcher">
      <div class="foundation-output-tabs">
        <button class="foundation-output-tab active" data-foundation-tab="final"
          onclick="event.stopPropagation(); switchFoundationOutputView('final', this)">Step 2 Final Report</button>
        <button class="foundation-output-tab" data-foundation-tab="step1"
          onclick="event.stopPropagation(); switchFoundationOutputView('step1', this)">Step 1 Collectors Snapshot</button>
      </div>
      <div class="foundation-output-panel" data-foundation-panel="final">
        ${finalHtml}
      </div>
      <div class="foundation-output-panel hidden" data-foundation-panel="step1">
        ${renderOutput(collectorsSnapshot)}
      </div>
    </div>
  `;
}

// Clear preview cache when a new pipeline starts
function clearPreviewCache() {
  for (const key in cardPreviewCache) delete cardPreviewCache[key];
  chatHistories = {};
  _foundationQualityDigest = '';
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

function isProbablyMarkdown(text) {
  const raw = String(text || '');
  return /(^|\n)\s{0,3}(#{1,6}\s+|[-*]\s+|\d+\.\s+)/.test(raw);
}

function formatInlineMarkdown(rawText) {
  let text = esc(String(rawText || ''));
  text = text.replace(/`([^`]+)`/g, '<code>$1</code>');
  text = text.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  text = text.replace(/\*([^*]+)\*/g, '<em>$1</em>');
  return text;
}

function renderMarkdownLite(text) {
  const lines = String(text || '').replace(/\r\n/g, '\n').split('\n');
  let html = '';
  let para = [];
  let inUl = false;
  let inOl = false;

  function closeParagraph() {
    if (!para.length) return;
    html += `<p>${para.join('<br>')}</p>`;
    para = [];
  }

  function closeLists() {
    if (inUl) {
      html += '</ul>';
      inUl = false;
    }
    if (inOl) {
      html += '</ol>';
      inOl = false;
    }
  }

  for (const rawLine of lines) {
    const line = rawLine.trimEnd();
    const trimmed = line.trim();

    if (!trimmed) {
      closeParagraph();
      closeLists();
      continue;
    }

    const heading = trimmed.match(/^(#{1,6})\s+(.+)$/);
    if (heading) {
      closeParagraph();
      closeLists();
      const level = heading[1].length;
      html += `<h${level}>${formatInlineMarkdown(heading[2])}</h${level}>`;
      continue;
    }

    const ulItem = trimmed.match(/^[-*]\s+(.+)$/);
    if (ulItem) {
      closeParagraph();
      if (inOl) {
        html += '</ol>';
        inOl = false;
      }
      if (!inUl) {
        html += '<ul>';
        inUl = true;
      }
      html += `<li>${formatInlineMarkdown(ulItem[1])}</li>`;
      continue;
    }

    const olItem = trimmed.match(/^\d+\.\s+(.+)$/);
    if (olItem) {
      closeParagraph();
      if (inUl) {
        html += '</ul>';
        inUl = false;
      }
      if (!inOl) {
        html += '<ol>';
        inOl = true;
      }
      html += `<li>${formatInlineMarkdown(olItem[1])}</li>`;
      continue;
    }

    closeLists();
    para.push(formatInlineMarkdown(trimmed));
  }

  closeParagraph();
  closeLists();
  return `<div class="out-md">${html || `<p>${escMultiline(String(text || ''))}</p>`}</div>`;
}

function renderQualityGateReportSection(title, report, depth) {
  const checks = Array.isArray(report?.checks) ? report.checks : [];
  const failedChecks = checks.filter(c => c && c.passed === false);
  const passedChecks = checks.filter(c => c && c.passed === true);
  const failedGateNames = Array.isArray(report?.failed_gate_ids)
    ? report.failed_gate_ids.map(g => gateLabel(g)).filter(Boolean)
    : [];
  const overallPass = Boolean(report?.overall_pass);
  const retries = Number(report?.retry_rounds_used || 0);
  const warning = String(report?.warning || '').trim();
  const summaryBadge = overallPass
    ? '<span class="out-badge green">PASS</span>'
    : '<span class="out-badge red">FAIL</span>';

  const checkRows = [...failedChecks, ...passedChecks].map((check) => {
    const passed = Boolean(check.passed);
    const stateCls = passed ? 'pass' : 'fail';
    const details = String(check.details || '').trim();
    return `
      <div class="phase1-quality-check ${stateCls}">
        <div class="phase1-quality-check-head">
          <span class="phase1-quality-check-title">${esc(gateLabel(check.gate_id || 'unknown'))}</span>
          <span class="out-badge ${passed ? 'green' : 'red'}">${passed ? 'PASS' : 'FAIL'}</span>
        </div>
        <div class="phase1-quality-check-line"><strong>Required:</strong> ${esc(String(check.required || 'n/a'))}</div>
        <div class="phase1-quality-check-line"><strong>Actual:</strong> ${esc(String(check.actual || 'n/a'))}</div>
        ${details ? `<div class="phase1-quality-check-line"><strong>Details:</strong> ${esc(details)}</div>` : ''}
      </div>
    `;
  }).join('');

  const wrapperClass = 'out-section';
  const heading = depth === 0 ? 'out-heading' : 'out-subheading';
  return `
    <div class="${wrapperClass}">
      <div class="${heading}">
        ${esc(title)} ${summaryBadge}
      </div>
      <div class="phase1-quality-overview">
        <span>Failed: <strong>${failedChecks.length}</strong></span>
        <span>Passed: <strong>${passedChecks.length}</strong></span>
        <span>Retries used: <strong>${retries}</strong></span>
      </div>
      ${failedGateNames.length ? `<div class="phase1-quality-check-line"><strong>Failed gates:</strong> ${esc(failedGateNames.join(', '))}</div>` : ''}
      ${warning ? `<div class="phase1-quality-check-line"><strong>Warning:</strong> ${esc(warning)}</div>` : ''}
      <div class="phase1-quality-check-grid">${checkRows || '<div class="phase1-quality-empty">No gate checks available.</div>'}</div>
    </div>
  `;
}

function renderContradictionsSection(title, contradictions, depth) {
  const rows = Array.isArray(contradictions) ? contradictions : [];
  const highUnresolved = rows.filter(r => String(r?.severity || '').toLowerCase() === 'high' && !r?.resolved);
  const medium = rows.filter(r => String(r?.severity || '').toLowerCase() === 'medium');
  const low = rows.filter(r => String(r?.severity || '').toLowerCase() === 'low');
  const sample = highUnresolved.slice(0, 8);
  const cards = sample.map((row, idx) => {
    const desc = String(row?.conflict_description || '').trim();
    const resolution = String(row?.resolution || '').trim();
    const claimA = String(row?.claim_a_id || '').trim();
    const claimB = String(row?.claim_b_id || '').trim();
    return `
      <div class="out-card">
        <div class="out-card-title">High Conflict ${idx + 1}</div>
        <div class="out-field"><span class="out-field-key">Claim A</span><span class="out-field-val">${esc(claimA || 'n/a')}</span></div>
        <div class="out-field"><span class="out-field-key">Claim B</span><span class="out-field-val">${esc(claimB || 'n/a')}</span></div>
        <div class="out-field"><span class="out-field-key">Conflict</span><span class="out-field-val">${esc(desc || 'n/a')}</span></div>
        ${resolution ? `<div class="out-field"><span class="out-field-key">Resolution</span><span class="out-field-val">${esc(resolution)}</span></div>` : ''}
      </div>
    `;
  }).join('');

  return `
    <div class="out-section">
      <div class="${depth === 0 ? 'out-heading' : 'out-subheading'}">${esc(title)} <span class="out-badge purple">${rows.length}</span></div>
      <div class="phase1-quality-overview">
        <span>High unresolved: <strong>${highUnresolved.length}</strong></span>
        <span>Medium: <strong>${medium.length}</strong></span>
        <span>Low: <strong>${low.length}</strong></span>
      </div>
      ${cards || '<div class="phase1-quality-empty">No high-severity unresolved contradictions.</div>'}
    </div>
  `;
}

function renderRetryAuditSection(title, audit, depth) {
  const entries = Array.isArray(audit) ? audit : [];
  const statusBadge = (statusRaw) => {
    const status = String(statusRaw || '').trim().toLowerCase();
    if (status === 'resolved') return '<span class="out-badge green">resolved</span>';
    if (status === 'improved') return '<span class="out-badge purple">improved</span>';
    if (status === 'collector_failed') return '<span class="out-badge red">collector_failed</span>';
    return '<span class="out-badge purple">unchanged</span>';
  };
  const rows = entries.map((entry) => {
    const before = Array.isArray(entry.failed_gate_ids_before)
      ? entry.failed_gate_ids_before.map(g => gateLabel(g)).join(', ')
      : 'None';
    const after = Array.isArray(entry.failed_gate_ids_after)
      ? entry.failed_gate_ids_after.map(g => gateLabel(g)).join(', ')
      : 'None';
    const warning = String(entry.warning || '').trim();
    return `
      <div class="out-card">
        <div class="out-card-title">Retry Round ${Number(entry.round_index || 0)}</div>
        <div class="out-field"><span class="out-field-key">Selected Collector</span><span class="out-field-val">${esc(String(entry.selected_collector || 'n/a'))}</span></div>
        <div class="out-field"><span class="out-field-key">Failed Before</span><span class="out-field-val">${esc(before || 'None')}</span></div>
        <div class="out-field"><span class="out-field-key">Added Evidence</span><span class="out-field-val"><span class="out-number">${Number(entry.added_evidence_count || 0)}</span></span></div>
        <div class="out-field"><span class="out-field-key">Failed After</span><span class="out-field-val">${esc(after || 'None')}</span></div>
        <div class="out-field"><span class="out-field-key">Status</span><span class="out-field-val">${statusBadge(entry.status)}</span></div>
        ${warning ? `<div class="out-field"><span class="out-field-key">Warning</span><span class="out-field-val">${esc(warning)}</span></div>` : ''}
      </div>
    `;
  }).join('');

  return `
    <div class="out-section">
      <div class="${depth === 0 ? 'out-heading' : 'out-subheading'}">${esc(title)} <span class="out-badge purple">${entries.length}</span></div>
      ${rows || '<div class="phase1-quality-empty">No retry rounds executed.</div>'}
    </div>
  `;
}

function renderOutput(data) {
  if (!data || typeof data !== 'object') {
    return `<div class="empty-state">No data to display.</div>`;
  }

  // Special renderer for Phase 1 Step 1 collector snapshot output.
  if (isPhase1CollectorsSnapshot(data)) {
    return renderPhase1CollectorsSnapshot(data);
  }

  // Special renderer for Matrix Planner output.
  if (data.schema_version === 'matrix_plan_v1' && Array.isArray(data.cells)) {
    return renderMatrixPlanOutput(data);
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

let _phase1CollectorSnapshotSeq = 0;

function isPhase1CollectorsSnapshot(data) {
  return (
    data &&
    typeof data === 'object' &&
    data.stage === 'collectors_complete' &&
    Array.isArray(data.collector_reports)
  );
}

function normalizeCollectorProviderToken(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return 'collector';
  if (raw.includes('gemini')) return 'gemini';
  if (raw.includes('claude')) return 'claude';
  if (raw.includes('voc')) return 'voc_api';
  return raw.replace(/[^a-z0-9_-]+/g, '_');
}

function collectorProviderLabel(provider) {
  const map = {
    gemini: 'Gemini',
    claude: 'Claude',
    voc_api: 'Direct VOC',
  };
  return map[provider] || humanize(provider || 'collector');
}

function collectorProviderSortRank(provider) {
  const rank = {
    gemini: 1,
    claude: 2,
    voc_api: 3,
  };
  return rank[provider] || 99;
}

function renderPhase1CollectorsSnapshot(data) {
  const reportsRaw = Array.isArray(data.collector_reports) ? data.collector_reports : [];
  if (!reportsRaw.length) {
    return `<div class="empty-state">Collector snapshot has no report previews yet.</div>`;
  }

  const groupedReports = {};
  reportsRaw.forEach((report, idx) => {
    const provider = normalizeCollectorProviderToken(report.provider || report.label || `collector_${idx + 1}`);
    if (!groupedReports[provider]) groupedReports[provider] = [];
    groupedReports[provider].push({
      chars: Number(report.report_chars || 0),
      text: String(report.report_preview || '').trim(),
    });
  });

  const providers = Object.keys(groupedReports).sort((a, b) => {
    const rankDiff = collectorProviderSortRank(a) - collectorProviderSortRank(b);
    if (rankDiff !== 0) return rankDiff;
    return a.localeCompare(b);
  });
  const defaultProvider = providers.includes('gemini')
    ? 'gemini'
    : (providers.includes('claude') ? 'claude' : providers[0]);
  const rootId = `phase1-collector-snapshot-${++_phase1CollectorSnapshotSeq}`;

  const pillsHtml = providers.map((provider) => {
    const isActive = provider === defaultProvider ? ' active' : '';
    const reportCount = groupedReports[provider].length;
    return `
      <button
        class="phase1-collector-pill${isActive}"
        data-provider="${provider}"
        onclick="event.stopPropagation(); selectPhase1CollectorReport('${rootId}', '${provider}', this)"
      >
        ${esc(collectorProviderLabel(provider))}
        <span class="phase1-collector-pill-count">${reportCount}</span>
      </button>
    `;
  }).join('');

  const panelsHtml = providers.map((provider) => {
    const panelActive = provider === defaultProvider ? ' active' : '';
    const reportBlocks = groupedReports[provider].map((row, i) => `
      <div class="phase1-collector-report-block">
        <div class="phase1-collector-report-meta">
          <span>${esc(collectorProviderLabel(provider))} report ${i + 1}</span>
          <span>${row.chars} chars</span>
        </div>
        <div class="phase1-collector-report-text">${
          isProbablyMarkdown(row.text)
            ? renderMarkdownLite(row.text)
            : escMultiline(row.text || '(No report preview text available.)')
        }</div>
      </div>
    `).join('');
    return `
      <div class="phase1-collector-report-panel${panelActive}" data-provider="${provider}">
        ${reportBlocks}
      </div>
    `;
  }).join('');

  const runtime = data.phase1_runtime_seconds ? `${data.phase1_runtime_seconds}s` : 'n/a';
  const evidenceCount = data.evidence_count || 0;

  return `
    <div class="phase1-collector-view" id="${rootId}">
      <div class="phase1-collector-head">
        <span class="phase1-collector-head-title">Collector Outputs</span>
        <span class="phase1-collector-head-meta">Evidence ${evidenceCount} · Runtime ${runtime}</span>
      </div>
      <div class="phase1-collector-pill-row">${pillsHtml}</div>
      <div class="phase1-collector-panels">${panelsHtml}</div>
    </div>
  `;
}

function selectPhase1CollectorReport(rootId, provider, triggerEl = null) {
  const root = (triggerEl && triggerEl.closest('.phase1-collector-view'))
    || document.getElementById(rootId);
  if (!root) return;

  root.querySelectorAll('.phase1-collector-pill').forEach((pill) => {
    pill.classList.toggle('active', pill.dataset.provider === provider);
  });
  root.querySelectorAll('.phase1-collector-report-panel').forEach((panel) => {
    panel.classList.toggle('active', panel.dataset.provider === provider);
  });
}


// -----------------------------------------------------------
// MATRIX PLANNER (Phase 2) — Custom Renderer
// -----------------------------------------------------------

function renderMatrixPlanOutput(data) {
  const awareness = Array.isArray(data.awareness_axis?.levels) && data.awareness_axis.levels.length
    ? data.awareness_axis.levels
    : MATRIX_AWARENESS_LEVELS;
  const emotionRows = Array.isArray(data.emotion_axis?.rows) ? data.emotion_axis.rows : [];
  const cells = Array.isArray(data.cells) ? data.cells : [];
  const total = parseInt(data.totals?.total_briefs, 10) || 0;

  if (!emotionRows.length) {
    return `<div class="empty-state">Matrix plan has no emotion rows.</div>`;
  }

  const cellMap = {};
  cells.forEach((cell) => {
    const key = `${String(cell.awareness_level || '').toLowerCase()}::${normalizeEmotionKey(cell.emotion_key || '')}`;
    cellMap[key] = parseInt(cell.brief_count, 10) || 0;
  });

  const headCols = awareness.map(level => `<th>${esc(humanizeAwareness(level))}</th>`).join('');
  const bodyRows = emotionRows.map((row) => {
    const emotionKey = normalizeEmotionKey(row.emotion_key || row.emotion_label || '');
    const emotionLabel = row.emotion_label || row.emotion_key || 'Emotion';
    const rowCells = awareness.map((level) => {
      const key = `${level}::${emotionKey}`;
      const count = cellMap[key] || 0;
      return `<td><span class="out-number">${count}</span></td>`;
    }).join('');
    return `<tr><td class="nb-matrix-row-head">${esc(emotionLabel)}</td>${rowCells}</tr>`;
  }).join('');

  return `
    <div class="out-section">
      <div class="out-heading">Awareness × Emotion Matrix <span class="out-badge purple">${total} briefs</span></div>
      <div class="nb-matrix-editor">
        <table class="nb-matrix-table">
          <thead>
            <tr>
              <th>Emotion \\ Awareness</th>
              ${headCols}
            </tr>
          </thead>
          <tbody>
            ${bodyRows}
          </tbody>
        </table>
      </div>
    </div>
  `;
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
let _ceReviewData = null;         // Cached creative_engine output (angles array)
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
  el.innerHTML = buildModelPicker('copywriter', 'Copywriter');
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
  document.getElementById('pipeline-title').textContent = '';
  document.getElementById('pipeline-subtitle').textContent = '';
  } catch (e) {
    alert('Failed to send selections: ' + e.message);
    if (btn) { btn.textContent = 'Continue to Copywriter'; btn.disabled = false; }
  }
}

async function loadAndOpenConceptReviewDrawer() {
  if (!cardPreviewCache['creative_engine']) {
    try {
      const useBranch = activeBranchId;
      const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
      const url = useBranch
        ? `/api/branches/${activeBranchId}/outputs/creative_engine${brandParam}`
        : `/api/outputs/creative_engine${brandParam}`;
      const resp = await fetch(url);
      if (resp.ok) {
        const d = await resp.json();
        cardPreviewCache['creative_engine'] = d.data;
      }
    } catch (e) {
      console.error('Failed to load creative_engine output for drawer:', e);
    }
  }
  if (cardPreviewCache['creative_engine']) {
    openConceptReviewDrawer(cardPreviewCache['creative_engine']);
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

  if (key === 'quality_gate_report' && typeof val === 'object' && !Array.isArray(val)) {
    return renderQualityGateReportSection(title, val, depth);
  }
  if (key === 'retry_audit' && Array.isArray(val)) {
    return renderRetryAuditSection(title, val, depth);
  }
  if (key === 'contradictions' && Array.isArray(val)) {
    return renderContradictionsSection(title, val, depth);
  }

  // Simple string
  if (typeof val === 'string') {
    const richText = isProbablyMarkdown(val) ? renderMarkdownLite(val) : escMultiline(val);
    if (depth === 0) {
      return `<div class="out-section">
        <div class="out-heading">${esc(title)}</div>
        <div class="out-text">${richText}</div>
      </div>`;
    }
    return `<div class="out-field">
      <span class="out-field-key">${esc(title)}</span>
      <span class="out-field-val">${richText}</span>
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
    const richText = isProbablyMarkdown(val) ? renderMarkdownLite(val) : escMultiline(val);
    // Check for score-like fields
    if (key.includes('score') || key.includes('rating')) {
      return `<div class="out-field">
        <span class="out-field-key">${esc(label)}</span>
        <span class="out-field-val"><span class="out-badge yellow">${esc(val)}</span></span>
      </div>`;
    }
    return `<div class="out-field">
      <span class="out-field-key">${esc(label)}</span>
      <span class="out-field-val">${richText}</span>
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
const PHASE2_AGENT_SLUGS = ['creative_engine', 'copywriter', 'hook_specialist'];

function applyBranchAgentStates(branch) {
  if (!branch) return;

  const doneAgents = new Set([
    ...(Array.isArray(branch.available_agents) ? branch.available_agents : []),
    ...(Array.isArray(branch.completed_agents) ? branch.completed_agents : []),
  ]);
  const failedAgents = new Set(Array.isArray(branch.failed_agents) ? branch.failed_agents : []);

  PHASE2_AGENT_SLUGS.forEach(slug => {
    if (failedAgents.has(slug)) {
      setCardState(slug, 'failed');
    } else if (doneAgents.has(slug)) {
      setCardState(slug, 'done');
    } else {
      setCardState(slug, 'waiting');
    }
  });
}

async function loadBranches() {
  try {
    const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
    const resp = await fetch(`/api/branches${brandParam}`);
    const freshBranches = await resp.json();
    const hasActive = Boolean(activeBranchId && freshBranches.some(b => b.id === activeBranchId));
    branches = freshBranches;
    if (!hasActive) {
      activeBranchId = freshBranches.length ? freshBranches[0].id : null;
    }
    const activeBranch = branches.find(b => b.id === activeBranchId);
    applyBranchAgentStates(activeBranch);
    renderBranchTabs();
    updateBranchManagerVisibility();
    updatePhaseStartButtons();
  } catch (e) {
    console.error('Failed to load branches', e);
  }
}

function updateBranchManagerVisibility() {
  const manager = document.getElementById('branch-manager');
  if (!manager) return;

  // Show branch manager if Phase 1 is done (foundation_research output exists)
  const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
  fetch(`/api/outputs${brandParam}`)
    .then(r => r.json())
    .then(outputs => {
      const phase1Done = outputs.some(o => o.slug === 'foundation_research' && o.available);
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

    const matrixCells = Array.isArray(b.inputs?.matrix_cells) ? b.inputs.matrix_cells : [];
    const plannedBriefs = matrixCells.reduce((sum, c) => sum + (parseInt(c?.brief_count, 10) || 0), 0);
    const funnelInfo = `${plannedBriefs} briefs`;

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

  const activeIndex = branches.findIndex(b => b.id === activeBranchId);
  const canDeleteActive = activeIndex > 0;

  // Add action buttons at the end
  const addBtn = `<button class="branch-add-btn" onclick="openNewBranchModal()">+ Branch</button>`;
  const deleteBtn = canDeleteActive
    ? `<button class="branch-delete-btn" onclick="deleteActiveBranch()">Delete Branch</button>`
    : `<button class="branch-delete-btn disabled" disabled title="Default branch cannot be deleted">Delete Branch</button>`;

  container.innerHTML = pills + addBtn + deleteBtn;
}

function deleteActiveBranch() {
  if (!activeBranchId) {
    alert('No active branch selected.');
    return;
  }
  const activeIndex = branches.findIndex(b => b.id === activeBranchId);
  if (activeIndex <= 0) {
    alert('Default branch cannot be deleted.');
    return;
  }
  deleteBranch(activeBranchId);
}

async function switchBranch(branchId) {
  activeBranchId = branchId;
  const wasScriptCollapsed = (() => {
    const panel = document.getElementById('phase-3-v2-panel');
    if (!panel) return phase3V2Collapsed;
    return panel.classList.contains('collapsed');
  })();
  const wasHooksCollapsed = (() => {
    const panel = document.getElementById('phase3-v2-hooks-panel');
    if (!panel) return phase3V2HooksCollapsed;
    return panel.classList.contains('collapsed');
  })();
  const wasScenesCollapsed = (() => {
    const panel = document.getElementById('phase3-v2-scenes-panel');
    if (!panel) return phase3V2ScenesCollapsed;
    return panel.classList.contains('collapsed');
  })();
  phase3V2ResetStateForBranch({
    preservedCollapsed: wasScriptCollapsed,
    preservedHooksCollapsed: wasHooksCollapsed,
    preservedScenesCollapsed: wasScenesCollapsed,
  });
  renderBranchTabs();

  const branch = branches.find(b => b.id === branchId);
  if (!branch) return;
  const creativeCard = document.getElementById('card-creative_engine');
  const creativePreview = document.getElementById('preview-creative_engine');
  const keepCreativeOpen = Boolean(
    creativeCard?.classList.contains('expanded') &&
    creativePreview &&
    !creativePreview.classList.contains('hidden')
  );

  // Clear existing Phase 2+ card states and previews
  ['copywriter', 'hook_specialist'].forEach(slug => {
    setCardState(slug, 'waiting');
    delete cardPreviewCache[slug];
    closeCardPreview(slug);
  });
  setCardState('creative_engine', 'waiting');
  delete cardPreviewCache.creative_engine;
  if (!keepCreativeOpen) {
    closeCardPreview('creative_engine');
  }

  // Refresh branches from server and set card states for the active branch
  try {
    const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
    const resp = await fetch(`/api/branches${brandParam}`);
    const freshBranches = await resp.json();
    branches = freshBranches; // update global so button logic has fresh data
    const fresh = freshBranches.find(b => b.id === branchId);
    applyBranchAgentStates(fresh);
    if (keepCreativeOpen) {
      const shouldShowCreative = Boolean(
        fresh && (
          (Array.isArray(fresh.available_agents) && fresh.available_agents.includes('creative_engine')) ||
          (Array.isArray(fresh.completed_agents) && fresh.completed_agents.includes('creative_engine'))
        )
      );
      if (shouldShowCreative && creativeCard && creativePreview) {
        creativeCard.classList.remove('expanded');
        creativePreview.classList.add('hidden');
        delete cardPreviewCache.creative_engine;
        await toggleCardPreviewBranchAware('creative_engine');
      } else {
        closeCardPreview('creative_engine');
      }
    }
  } catch (e) {
    console.error('Failed to load branch state', e);
  }

  // Re-evaluate start buttons for this branch's state
  updatePhaseStartButtons();
  phase3V2RefreshForActiveBranch(true);
  updateProgress();
}

function setNewBranchModalContent(defaultPhase2Start) {
  const title = document.getElementById('nb-title');
  const subtitle = document.getElementById('nb-subtitle');
  const createBtn = document.getElementById('nb-create-btn');
  if (!title || !subtitle || !createBtn) return;

  if (defaultPhase2Start) {
    title.textContent = 'Start Phase 2';
    subtitle.textContent = 'Build your default Awareness × Emotion matrix plan before the first Phase 2 run.';
    createBtn.textContent = 'Start Phase 2';
  } else {
    title.textContent = 'New Matrix Branch';
    subtitle.textContent = 'Configure per-cell brief counts. Uses emotional drivers from Phase 1.';
    createBtn.textContent = 'Create & Run Phase 2';
  }
}

function humanizeAwareness(value) {
  const labels = {
    unaware: 'Unaware',
    problem_aware: 'Problem Aware',
    solution_aware: 'Solution Aware',
    product_aware: 'Product Aware',
    most_aware: 'Most Aware',
  };
  return labels[value] || humanize(value);
}

function normalizeEmotionKey(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function getStoredMatrixCellMap(branch) {
  const map = {};
  const cells = Array.isArray(branch?.inputs?.matrix_cells) ? branch.inputs.matrix_cells : [];
  cells.forEach((cell) => {
    const awareness = String(cell?.awareness_level || '').toLowerCase();
    const emotionKey = normalizeEmotionKey(cell?.emotion_key || cell?.emotion_label || '');
    if (!awareness || !emotionKey) return;
    const key = `${awareness}::${emotionKey}`;
    map[key] = Math.max(0, parseInt(cell?.brief_count, 10) || 0);
  });
  return map;
}

async function loadMatrixAxes() {
  const brandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
  const resp = await fetch(`/api/matrix-axes${brandParam}`);
  const data = await resp.json();
  if (!resp.ok || data.error) {
    throw new Error(data.error || `Failed to load matrix axes (HTTP ${resp.status})`);
  }
  matrixMaxPerCell = parseInt(data.max_briefs_per_cell, 10) || matrixMaxPerCell;
  return data;
}

function renderNewBranchMatrixEditor(awarenessLevels, emotionRows, existingMap) {
  const mount = document.getElementById('nb-matrix-editor');
  if (!mount) return;

  if (!Array.isArray(awarenessLevels) || awarenessLevels.length === 0) {
    mount.innerHTML = '<div class="nb-matrix-loading">No awareness levels available.</div>';
    return;
  }
  if (!Array.isArray(emotionRows) || emotionRows.length === 0) {
    mount.innerHTML = '<div class="nb-matrix-loading">No emotional drivers found in Phase 1 output.</div>';
    return;
  }

  const headCols = awarenessLevels
    .map(level => `<th>${esc(humanizeAwareness(level))}</th>`)
    .join('');

  const rowsHtml = emotionRows.map((row) => {
    const emotionLabel = row.emotion_label || row.emotion || row.emotion_key;
    const emotionKey = normalizeEmotionKey(row.emotion_key || emotionLabel);
    const cells = awarenessLevels.map((level) => {
      const key = `${level}::${emotionKey}`;
      const val = existingMap[key] ?? 0;
      return `<td>
        <input
          type="number"
          class="nb-matrix-input"
          min="0"
          max="${matrixMaxPerCell}"
          value="${val}"
          data-awareness="${esc(level)}"
          data-emotion-key="${esc(emotionKey)}"
          data-emotion-label="${esc(emotionLabel)}"
          oninput="onMatrixInputChange(this)"
        >
      </td>`;
    }).join('');
    return `<tr>
      <td class="nb-matrix-row-head">${esc(emotionLabel)}</td>
      ${cells}
    </tr>`;
  }).join('');

  mount.innerHTML = `
    <table class="nb-matrix-table">
      <thead>
        <tr>
          <th>Emotion \\ Awareness</th>
          ${headCols}
        </tr>
      </thead>
      <tbody>
        ${rowsHtml}
      </tbody>
    </table>
  `;
  updateMatrixTotal();
}

function onMatrixInputChange(input) {
  const max = parseInt(input.max, 10) || matrixMaxPerCell;
  let v = parseInt(input.value, 10);
  if (Number.isNaN(v)) v = 0;
  if (v < 0) v = 0;
  if (v > max) v = max;
  input.value = String(v);
  updateMatrixTotal();
}

function updateMatrixTotal() {
  const totalEl = document.getElementById('nb-matrix-total');
  if (!totalEl) return;
  const inputs = Array.from(document.querySelectorAll('#nb-matrix-editor .nb-matrix-input'));
  const total = inputs.reduce((sum, input) => sum + (parseInt(input.value, 10) || 0), 0);
  totalEl.textContent = `Total planned briefs: ${total}`;
}

function collectMatrixCellsFromModal() {
  const inputs = Array.from(document.querySelectorAll('#nb-matrix-editor .nb-matrix-input'));
  return inputs.map((input) => ({
    awareness_level: String(input.dataset.awareness || '').toLowerCase(),
    emotion_key: normalizeEmotionKey(input.dataset.emotionKey || ''),
    emotion_label: String(input.dataset.emotionLabel || ''),
    brief_count: Math.max(0, parseInt(input.value, 10) || 0),
  }));
}

async function openNewBranchModal(options = {}) {
  const defaultPhase2Start = Boolean(options.defaultPhase2Start);
  pendingDefaultPhase2Setup = defaultPhase2Start;
  setNewBranchModalContent(defaultPhase2Start);
  const activeBranch = branches.find(b => b.id === activeBranchId);
  document.getElementById('nb-label').value = defaultPhase2Start ? 'Default' : '';
  const editor = document.getElementById('nb-matrix-editor');
  if (editor) {
    editor.innerHTML = '<div class="nb-matrix-loading">Loading matrix axes...</div>';
  }
  updateMatrixTotal();

  document.getElementById('new-branch-modal').classList.remove('hidden');
  document.getElementById('nb-label').focus();

  try {
    const axes = await loadMatrixAxes();
    const awarenessLevels = Array.isArray(axes.awareness_levels) && axes.awareness_levels.length
      ? axes.awareness_levels
      : MATRIX_AWARENESS_LEVELS;
    const emotionRows = Array.isArray(axes.emotion_rows) ? axes.emotion_rows : [];
    renderNewBranchMatrixEditor(awarenessLevels, emotionRows, getStoredMatrixCellMap(activeBranch));
  } catch (e) {
    if (editor) {
      editor.innerHTML = `<div class="nb-matrix-loading">${esc(e.message || 'Failed to load matrix axes.')}</div>`;
    }
  }
}

function closeNewBranchModal() {
  document.getElementById('new-branch-modal').classList.add('hidden');
  pendingDefaultPhase2Setup = false;
  setNewBranchModalContent(false);
}

async function createBranch() {
  const label = document.getElementById('nb-label').value.trim();
  const finalLabel = label || (pendingDefaultPhase2Setup ? 'Default' : '');
  const matrixCells = collectMatrixCellsFromModal();
  const totalPlannedBriefs = matrixCells.reduce((sum, c) => sum + (parseInt(c.brief_count, 10) || 0), 0);
  if (totalPlannedBriefs <= 0) {
    alert('Set at least one matrix cell above 0 before starting Phase 2.');
    return;
  }
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
        matrix_cells: matrixCells,
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
        phases: [2],
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
    const message = formatFetchError(e, 'branch run');
    if (!silent) alert(message);
    return { ok: false, error: message };
  }
}

async function deleteBranch(branchId) {
  const branch = branches.find(b => b.id === branchId);
  const branchIndex = branches.findIndex(b => b.id === branchId);
  if (branchIndex === 0) {
    alert('Default branch cannot be deleted.');
    return;
  }
  const label = branch ? branch.label : branchId;
  if (!confirm(`Delete branch "${label}"? This removes all its outputs.`)) return;

  try {
    const brandParam = activeBrandSlug ? `?brand=${activeBrandSlug}` : '';
    await fetch(`/api/branches/${branchId}${brandParam}`, { method: 'DELETE' });

    // If the deleted branch was active, deselect
    if (activeBranchId === branchId) {
      activeBranchId = null;
      phase3V2ResetStateForBranch();
      // Reset Phase 2+ cards
      ['creative_engine', 'copywriter', 'hook_specialist'].forEach(slug => {
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
  const isPhase2Plus = ['creative_engine', 'copywriter', 'hook_specialist'].includes(slug);
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

  let foundationCollectorsSnapshot = null;
  if (slug === 'foundation_research' && !useBranch) {
    const snapshotKey = `${slug}__collectors_snapshot`;
    if (!(snapshotKey in cardPreviewCache)) {
      try {
        const brandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
        const snapResp = await fetch(`/api/outputs/foundation_research/collectors${brandParam}`);
        if (snapResp.ok) {
          const snapData = await snapResp.json();
          cardPreviewCache[snapshotKey] = snapData.data || null;
        } else {
          cardPreviewCache[snapshotKey] = null;
        }
      } catch (_) {
        cardPreviewCache[snapshotKey] = null;
      }
    }
    foundationCollectorsSnapshot = cardPreviewCache[snapshotKey];
  }

  // Agent 02 legacy output: open concept review drawer for angle/concept payloads.
  if (slug === 'creative_engine' && Array.isArray(cardPreviewCache[slug]?.angles)) {
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
      <div class="card-preview-actions">
        ${slug === 'foundation_research' ? `
          <div class="rerun-group">
            <button class="btn-rerun" onclick="event.stopPropagation(); rerunAgent('foundation_research')">Rerun</button>
            <button class="btn-rerun-menu" onclick="event.stopPropagation(); toggleRerunMenuFor('foundation_research', 'rerun-menu-preview-foundation_research')">▾</button>
            <div class="rerun-menu hidden" id="rerun-menu-preview-foundation_research"></div>
          </div>
        ` : ''}
        <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation(); openOutputFullscreenModal('${slug}')">View Full Screen</button>
      </div>
    </div>
    <div class="card-preview-body">${
      slug === 'foundation_research'
        ? renderFoundationOutputSwitcher(cardPreviewCache[slug], foundationCollectorsSnapshot, useBranch)
        : renderOutput(cardPreviewCache[slug])
    }</div>
  `;

  // Restore checkbox state after re-render (for Creative Engine concept selections)
  restoreCeCheckboxState();
}

// -----------------------------------------------------------
// PIPELINE MAP
// -----------------------------------------------------------

async function startFromPhase(phase) {
  if (phase === 3) {
    if (!phase3V2Enabled) {
      alert('Phase 3 v2 is disabled on this server.');
      return;
    }
    await phase3V2Run();
    return;
  }

  const inputs = readForm();

  // Determine which phases to run
  const phases = [];
  if (phase === 2) phases.push(2);
  if (phase === 3) phases.push(3);

  const btn = document.getElementById(`btn-start-phase-${phase}`);
  if (btn) {
    btn.textContent = 'Starting...';
    btn.disabled = true;
  }

  const fallbackBtnLabel = phase === 2 ? 'Start Phase 2' : 'Run Scripts';

  // Phase 2+ is always branch-scoped.
  if (!activeBranchId && branches.length > 0) {
    activeBranchId = branches[0].id;
    renderBranchTabs();
  }

  const modelOverrides = {};

  // First Phase 2 run with no branches: ask for matrix cell counts before creating the default branch.
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
  const brandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
  fetch(`/api/outputs${brandParam}`)
    .then(r => r.json())
    .then(outputs => {
      const v2Panel = document.getElementById('phase-3-v2-panel');
      const hooksPanel = document.getElementById('phase3-v2-hooks-panel');
      const scenesPanel = document.getElementById('phase3-v2-scenes-panel');

      if (pipelineRunning) {
        document.querySelectorAll('.btn-start-phase').forEach(b => b.classList.add('hidden'));
        if (v2Panel) v2Panel.classList.add('hidden');
        if (hooksPanel) hooksPanel.classList.add('hidden');
        if (scenesPanel) scenesPanel.classList.add('hidden');
        phase3V2StopPolling();
        return;
      }

      const available = new Set(outputs.filter(o => o.available).map(o => o.slug));
      const phase1Done = available.has('foundation_research');

      // Show "Start Phase 2" if Phase 1 is done and the active branch hasn't run Phase 2 yet
      const btn2 = document.getElementById('btn-start-phase-2');
      if (btn2) {
        const activeBranch = branches.find(b => b.id === activeBranchId);
        const branchHasPhase2 = Boolean(
          activeBranch
          && ((activeBranch.available_agents || []).includes('creative_engine')
              || (activeBranch.completed_agents || []).includes('creative_engine'))
        );
        const showBtn2 = phase1Done && (!activeBranch || !branchHasPhase2);
        if (showBtn2) {
          btn2.classList.remove('hidden');
        } else {
          btn2.classList.add('hidden');
        }
      }

      const activeBranch = branches.find(b => b.id === activeBranchId);
      const branchAvailable = new Set([
        ...(Array.isArray(activeBranch?.available_agents) ? activeBranch.available_agents : []),
        ...(Array.isArray(activeBranch?.completed_agents) ? activeBranch.completed_agents : []),
      ]);
      const phase2Done = branchAvailable.has('creative_engine');
      const showV2 = phase3V2Enabled && phase2Done;
      if (v2Panel) {
        if (showV2) {
          v2Panel.classList.remove('hidden');
          phase3V2ApplyCollapseState();
          phase3V2SyncDefaultsToControls();
          phase3V2RefreshForActiveBranch();
          phase3V2RenderHooksSection();
          phase3V2RenderScenesSection();
        } else {
          v2Panel.classList.add('hidden');
          if (hooksPanel) hooksPanel.classList.add('hidden');
          if (scenesPanel) scenesPanel.classList.add('hidden');
          phase3V2StopPolling();
        }
      }

      // Update branch manager visibility
      updateBranchManagerVisibility();
    })
    .catch(() => {});
}

// -----------------------------------------------------------

function getActiveBranch() {
  return branches.find(b => b.id === activeBranchId) || null;
}

function activeBranchHasPhase2() {
  const branch = getActiveBranch();
  const available = new Set([
    ...(Array.isArray(branch?.available_agents) ? branch.available_agents : []),
    ...(Array.isArray(branch?.completed_agents) ? branch.completed_agents : []),
  ]);
  return available.has('creative_engine');
}

function isPhase3V2Selected() {
  return Boolean(phase3V2Enabled);
}

function phase3V2ApplyCollapseState() {
  const panel = document.getElementById('phase-3-v2-panel');
  const body = document.getElementById('phase3-v2-body');
  if (panel) panel.classList.toggle('collapsed', phase3V2Collapsed);
  if (body) body.classList.toggle('hidden', phase3V2Collapsed);
}

function phase3V2SetCollapsed(collapsed) {
  phase3V2Collapsed = Boolean(collapsed);
  phase3V2ApplyCollapseState();
}

function phase3V2ToggleCollapse() {
  phase3V2SetCollapsed(!phase3V2Collapsed);
}

function phase3V2ApplyHooksCollapseState() {
  const panel = document.getElementById('phase3-v2-hooks-panel');
  const body = document.getElementById('phase3-v2-hooks-body');
  if (panel) panel.classList.toggle('collapsed', phase3V2HooksCollapsed);
  if (body) body.classList.toggle('hidden', phase3V2HooksCollapsed);
}

function phase3V2SetHooksCollapsed(collapsed) {
  phase3V2HooksCollapsed = Boolean(collapsed);
  phase3V2ApplyHooksCollapseState();
}

function phase3V2ToggleHooksCollapse() {
  phase3V2SetHooksCollapsed(!phase3V2HooksCollapsed);
}

// Backward-compatible handler name used by the HTML onclick.
function phase3V2HooksToggleCollapse() {
  phase3V2ToggleHooksCollapse();
}

function phase3V2ApplyScenesCollapseState() {
  const panel = document.getElementById('phase3-v2-scenes-panel');
  const body = document.getElementById('phase3-v2-scenes-body');
  if (panel) panel.classList.toggle('collapsed', phase3V2ScenesCollapsed);
  if (body) body.classList.toggle('hidden', phase3V2ScenesCollapsed);
}

function phase3V2SetScenesCollapsed(collapsed) {
  phase3V2ScenesCollapsed = Boolean(collapsed);
  phase3V2ApplyScenesCollapseState();
}

function phase3V2ToggleScenesCollapse() {
  phase3V2SetScenesCollapsed(!phase3V2ScenesCollapsed);
}

function phase3V2ScenesToggleCollapse() {
  phase3V2ToggleScenesCollapse();
}

function phase3V2SyncDefaultsToControls() {
  const branchKey = `${activeBrandSlug || ''}:${activeBranchId || ''}`;
  if (!branchKey || phase3V2ControlsKey === branchKey) return;
  phase3V2ControlsKey = branchKey;
  phase3V2ApplyCollapseState();
}

function phase3V2ResetStateForBranch(options = {}) {
  const preserveCollapsed = typeof options.preservedCollapsed === 'boolean'
    ? options.preservedCollapsed
    : null;
  const preserveHooksCollapsed = typeof options.preservedHooksCollapsed === 'boolean'
    ? options.preservedHooksCollapsed
    : null;
  const preserveScenesCollapsed = typeof options.preservedScenesCollapsed === 'boolean'
    ? options.preservedScenesCollapsed
    : null;
  const nextCollapsed = preserveCollapsed === null ? phase3V2Collapsed : preserveCollapsed;
  const nextHooksCollapsed = preserveHooksCollapsed === null ? phase3V2HooksCollapsed : preserveHooksCollapsed;
  const nextScenesCollapsed = preserveScenesCollapsed === null ? phase3V2ScenesCollapsed : preserveScenesCollapsed;
  phase3V2Prepared = null;
  phase3V2PreparedAt = '';
  phase3V2HooksPrepared = null;
  phase3V2RunsCache = [];
  phase3V2CurrentRunId = '';
  phase3V2CurrentRunDetail = null;
  phase3V2LoadedBranchKey = '';
  phase3V2ControlsKey = '';
  phase3V2Collapsed = nextCollapsed;
  phase3V2HooksCollapsed = nextHooksCollapsed;
  phase3V2ScenesCollapsed = nextScenesCollapsed;
  phase3V2RevealedUnits = new Set();
  phase3V2ExpandedCurrent = null;
  phase3V2ArmTab = 'script';
  phase3V2ChatPendingDraft = null;
  phase3V2HookExpandedCurrent = null;
  phase3V2HookTab = 'hook';
  phase3V2HookChatPendingHook = null;
  phase3V2SceneExpandedCurrent = null;
  phase3V2SceneTab = 'scene';
  phase3V2SceneChatPendingPlan = null;
  phase3V2UnitFilters = { awareness: 'all', emotion: 'all' };
  phase3V2CloseArmExpanded();
  phase3V2CloseHookExpanded();
  phase3V2CloseSceneExpanded();
  phase3V2SetPrepareBusy(false);
  phase3V2SetPrepareState('Not prepared yet.');
  phase3V2StopPolling();
  phase3V2RenderPrepareSummary();
  phase3V2RenderApprovalBar(null);
  phase3V2RenderCurrentRun();
  phase3V2RenderHooksSection();
  phase3V2RenderScenesSection();
  phase3V2RenderRunSelect();
  phase3V2ApplyCollapseState();
  phase3V2ApplyHooksCollapseState();
  phase3V2ApplyScenesCollapseState();
}

function phase3V2BrandParam() {
  return activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
}

function phase3V2SetStatus(text, state = '') {
  const el = document.getElementById('phase3-v2-status');
  if (!el) return;
  el.textContent = text;
  el.classList.remove('running', 'done', 'completed', 'failed');
  if (state) {
    el.classList.add(state);
  }
}

function phase3V2GetDecisionProgress(detail = phase3V2CurrentRunDetail) {
  if (!detail || typeof detail !== 'object') {
    return { total_required: 0, approved: 0, revise: 0, reject: 0, pending: 0, all_approved: false };
  }
  const p = detail.decision_progress;
  if (p && typeof p === 'object') {
    const total = parseInt(p.total_required, 10) || 0;
    const approved = parseInt(p.approved, 10) || 0;
    const revise = parseInt(p.revise, 10) || 0;
    const reject = parseInt(p.reject, 10) || 0;
    const pending = parseInt(p.pending, 10);
    const normalizedPending = Number.isNaN(pending) ? Math.max(0, total - (approved + revise + reject)) : pending;
    return {
      total_required: total,
      approved,
      revise,
      reject,
      pending: Math.max(0, normalizedPending),
      all_approved: Boolean(p.all_approved) || (total > 0 && approved === total),
    };
  }

  const units = Array.isArray(detail.brief_units) ? detail.brief_units : [];
  const draftsByArm = detail.drafts_by_arm && typeof detail.drafts_by_arm === 'object' ? detail.drafts_by_arm : {};
  const runArmsRaw = Array.isArray(detail.run?.arms) ? detail.run.arms : Object.keys(draftsByArm);
  const arms = runArmsRaw.map(v => String(v || '').trim()).filter(Boolean);
  const index = phase3V2DecisionMap(detail);
  let total = 0;
  let approved = 0;
  let revise = 0;
  let reject = 0;
  units.forEach((unit) => {
    const unitId = String(unit?.brief_unit_id || '').trim();
    if (!unitId) return;
    arms.forEach((arm) => {
      total += 1;
      const value = index[`${unitId}::${arm}`] || '';
      if (value === 'approve') approved += 1;
      else if (value === 'revise') revise += 1;
      else if (value === 'reject') reject += 1;
    });
  });
  const pending = Math.max(0, total - (approved + revise + reject));
  return {
    total_required: total,
    approved,
    revise,
    reject,
    pending,
    all_approved: total > 0 && approved === total,
  };
}

function phase3V2IsLocked(detail = phase3V2CurrentRunDetail) {
  return Boolean(detail?.final_lock && detail.final_lock.locked);
}

function phase3V2RenderApprovalBar(detail = phase3V2CurrentRunDetail) {
  const bar = document.getElementById('phase3-v2-approval-bar');
  if (!bar) return;
  bar.classList.add('hidden');
}

function phase3V2FormatClock(dateObj) {
  return dateObj.toLocaleTimeString('en-US', { hour12: false });
}

function phase3V2SetPrepareState(message, state = '') {
  const el = document.getElementById('phase3-v2-prepare-state');
  if (!el) return;
  el.textContent = message;
  el.classList.remove('preparing', 'ready', 'failed');
  if (state) el.classList.add(state);
}

function phase3V2SetPrepareBusy(isBusy) {
  phase3V2Preparing = Boolean(isBusy);
  const btn = document.getElementById('phase3-v2-prepare-btn');
  const runBtn = document.getElementById('phase3-v2-run-btn');
  if (btn) {
    btn.disabled = phase3V2Preparing;
    btn.textContent = phase3V2Preparing ? 'Preparing...' : 'Prepare';
  }
  if (runBtn) {
    runBtn.disabled = phase3V2Preparing;
  }
}

function phase3V2StopPolling() {
  if (phase3V2PollTimer) {
    clearInterval(phase3V2PollTimer);
    phase3V2PollTimer = null;
  }
}

function phase3V2StartPolling(runId) {
  phase3V2StopPolling();
  phase3V2PollTimer = setInterval(async () => {
    await phase3V2LoadRunDetail(runId, { startPolling: false, silent: true });
  }, 2000);
}

function phase3V2RefreshForActiveBranch(force = false) {
  if (!isPhase3V2Selected()) return;
  if (!activeBranchId || !activeBranchHasPhase2()) return;
  const branchKey = `${activeBrandSlug || ''}:${activeBranchId || ''}`;
  if (!force && phase3V2LoadedBranchKey === branchKey) return;
  phase3V2LoadedBranchKey = branchKey;
  phase3V2Prepare();
  phase3V2LoadRuns({ selectLatest: true });
}

function phase3V2RenderPrepareSummary() {
  const mount = document.getElementById('phase3-v2-prepare-summary');
  if (!mount) return;
  if (!isPhase3V2Selected()) {
    mount.textContent = '';
    phase3V2SetPrepareState('');
    return;
  }
  if (!activeBranchId) {
    mount.innerHTML = '<span class="phase3-v2-planned-pill">Planned Brief Units: <strong>-</strong></span>';
    phase3V2SetPrepareState('Not ready: no branch selected.');
    return;
  }
  if (!activeBranchHasPhase2()) {
    mount.innerHTML = '<span class="phase3-v2-planned-pill">Planned Brief Units: <strong>-</strong></span>';
    phase3V2SetPrepareState('Not ready: Phase 2 output missing.');
    return;
  }
  if (!phase3V2Prepared || typeof phase3V2Prepared !== 'object') {
    mount.innerHTML = '<span class="phase3-v2-planned-pill">Planned Brief Units: <strong>-</strong></span>';
    if (!phase3V2Preparing) {
      phase3V2SetPrepareState('Not prepared yet.');
    }
    return;
  }

  const plannedBriefUnits = parseInt(
    phase3V2Prepared.planned_brief_units ?? phase3V2Prepared.pilot_size,
    10,
  ) || 0;
  mount.innerHTML = `<span class="phase3-v2-planned-pill">Planned Brief Units: <strong>${plannedBriefUnits}</strong></span>`;
  if (!phase3V2Preparing) {
    if (phase3V2PreparedAt) {
      phase3V2SetPrepareState(`Prepared at ${phase3V2PreparedAt}.`, 'ready');
    } else {
      phase3V2SetPrepareState('Prepared.', 'ready');
    }
  }
}

async function phase3V2Prepare(options = {}) {
  const silent = Boolean(options.silent);
  if (!isPhase3V2Selected()) return;
  if (!activeBranchId || !activeBranchHasPhase2()) {
    phase3V2Prepared = null;
    phase3V2PreparedAt = '';
    phase3V2SetPrepareBusy(false);
    phase3V2RenderPrepareSummary();
    return false;
  }
  phase3V2SetPrepareBusy(true);
  phase3V2SetPrepareState('Preparing Brief Units and evidence check...', 'preparing');
  try {
    const params = new URLSearchParams();
    if (activeBrandSlug) params.set('brand', activeBrandSlug);
    const resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/prepare?${params.toString()}`);
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Prepare failed (HTTP ${resp.status})`);
    }
    phase3V2Prepared = data;
    phase3V2PreparedAt = phase3V2FormatClock(new Date());
    phase3V2SetPrepareState(`Prepared at ${phase3V2PreparedAt}.`, 'ready');
    phase3V2RenderPrepareSummary();
    return true;
  } catch (e) {
    phase3V2Prepared = null;
    phase3V2PreparedAt = '';
    phase3V2SetPrepareState(e.message || 'Prepare failed.', 'failed');
    const mount = document.getElementById('phase3-v2-prepare-summary');
    if (mount) mount.textContent = e.message || 'Failed to prepare Brief Units.';
    if (!silent) {
      alert(e.message || 'Failed to prepare Brief Units.');
    }
    return false;
  } finally {
    phase3V2SetPrepareBusy(false);
  }
}

async function phase3V2Run() {
  if (!isPhase3V2Selected()) return;
  if (!activeBranchId) {
    alert('No branch selected.');
    return;
  }
  if (!activeBranchHasPhase2()) {
    alert('Run Phase 2 for this branch first.');
    return;
  }

  const prepared = await phase3V2Prepare({ silent: true });
  if (!prepared) {
    phase3V2SetStatus('Failed', 'failed');
    alert('Could not start scripts because prepare failed. Check the prep status and retry.');
    return;
  }

  const runBtn = document.getElementById('phase3-v2-run-btn');
  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = 'Starting...';
  }

  phase3V2SetStatus('Running', 'running');
  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: activeBrandSlug || '',
        selected_brief_unit_ids: [],
        ab_mode: false,
        sdk_toggles: { core_script_drafter: true },
        reviewer_role: phase3V2ReviewerRoleDefault,
      }),
    });
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Run failed to start (HTTP ${resp.status})`);
    }
    phase3V2CurrentRunId = String(data.run_id || '');
    phase3V2RevealedUnits = new Set();
    await phase3V2LoadRuns({ selectLatest: true });
    phase3V2SetStatus('Running', 'running');
  } catch (e) {
    phase3V2SetStatus('Failed', 'failed');
    alert(e.message || 'Failed to start Phase 3 v2 run.');
  } finally {
    if (runBtn) {
      runBtn.disabled = false;
      runBtn.textContent = 'Run Scripts';
    }
  }
}

function phase3V2RenderRunSelect() {
  const select = document.getElementById('phase3-v2-run-select');
  if (!select) return;
  if (!phase3V2RunsCache.length) {
    select.innerHTML = '<option value="">No runs yet</option>';
    return;
  }
  const options = phase3V2RunsCache.map((run) => {
    const runId = String(run.run_id || '');
    const status = String(run.status || 'unknown');
    const label = `${runId} · ${status}`;
    const selected = runId === phase3V2CurrentRunId ? 'selected' : '';
    return `<option value="${esc(runId)}" ${selected}>${esc(label)}</option>`;
  });
  select.innerHTML = options.join('');
}

async function phase3V2LoadRuns(options = {}) {
  if (!isPhase3V2Selected() || !activeBranchId || !activeBranchHasPhase2()) return;
  const selectLatest = options.selectLatest !== false;
  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/runs${phase3V2BrandParam()}`);
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Failed to load runs (HTTP ${resp.status})`);
    }
    phase3V2RunsCache = Array.isArray(data) ? data : [];
    if (!phase3V2CurrentRunId || !phase3V2RunsCache.some(r => String(r.run_id || '') === phase3V2CurrentRunId)) {
      phase3V2CurrentRunId = selectLatest && phase3V2RunsCache.length ? String(phase3V2RunsCache[0].run_id || '') : '';
    }
    phase3V2RenderRunSelect();
    if (phase3V2CurrentRunId) {
      await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: true, silent: true });
    } else {
      phase3V2CurrentRunDetail = null;
      phase3V2RenderApprovalBar(null);
      phase3V2RenderCurrentRun();
      phase3V2RenderHooksSection();
      phase3V2RenderScenesSection();
      phase3V2SetStatus('Idle');
    }
  } catch (e) {
    console.error('Failed to load phase3 v2 runs', e);
  }
}

async function phase3V2SelectRun(runId) {
  const selected = String(runId || '').trim();
  if (!selected) return;
  phase3V2CurrentRunId = selected;
  phase3V2RevealedUnits = new Set();
  phase3V2UnitFilters = { awareness: 'all', emotion: 'all' };
  await phase3V2LoadRunDetail(selected, { startPolling: true });
}

async function phase3V2LoadRunDetail(runId, options = {}) {
  if (!activeBranchId) return;
  const startPolling = Boolean(options.startPolling);
  const silent = Boolean(options.silent);
  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(runId)}${phase3V2BrandParam()}`);
    const detail = await resp.json();
    if (!resp.ok || detail.error) {
      throw new Error(detail.error || `Failed to load run detail (HTTP ${resp.status})`);
    }
    phase3V2CurrentRunDetail = detail;
    phase3V2CurrentRunId = String(runId);
    phase3V2RenderRunSelect();
    phase3V2RenderApprovalBar(detail);
    phase3V2RenderCurrentRun();
    phase3V2RenderHooksSection();
    phase3V2RenderScenesSection();

    if (phase3V2ExpandedCurrent) {
      const currentKey = phase3V2ExpandedCurrent.key;
      phase3V2ExpandedArms = phase3V2BuildExpandedItems();
      phase3V2ExpandedIndex = phase3V2ExpandedArms.findIndex(item => item.key === currentKey);
      if (phase3V2ExpandedIndex >= 0) {
        phase3V2RenderExpandedModal();
      } else {
        phase3V2CloseArmExpanded();
      }
    }
    if (phase3V2HookExpandedCurrent) {
      const currentKey = phase3V2HookExpandedCurrent.key;
      phase3V2HookExpandedItems = phase3V2BuildHookExpandedItems();
      phase3V2HookExpandedIndex = phase3V2HookExpandedItems.findIndex(item => item.key === currentKey);
      if (phase3V2HookExpandedIndex >= 0) {
        phase3V2RenderHookExpandedModal();
      } else {
        phase3V2CloseHookExpanded();
      }
    }
    if (phase3V2SceneExpandedCurrent) {
      const currentKey = phase3V2SceneExpandedCurrent.key;
      phase3V2SceneExpandedItems = phase3V2BuildSceneExpandedItems();
      phase3V2SceneExpandedIndex = phase3V2SceneExpandedItems.findIndex(item => item.key === currentKey);
      if (phase3V2SceneExpandedIndex >= 0) {
        phase3V2RenderSceneExpandedModal();
      } else {
        phase3V2CloseSceneExpanded();
      }
    }

    const status = String(detail.run?.status || '');
    const hookStatus = String(detail.hook_stage?.status || '').toLowerCase();
    const sceneStatus = String(detail.scene_stage?.status || '').toLowerCase();
    if (status === 'running' || hookStatus === 'running' || sceneStatus === 'running') {
      phase3V2SetStatus('Running', 'running');
      if (startPolling) {
        phase3V2StartPolling(runId);
      }
    } else if (status === 'failed' || hookStatus === 'failed' || sceneStatus === 'failed') {
      phase3V2StopPolling();
      phase3V2SetStatus('Failed', 'failed');
    } else if (status === 'completed') {
      phase3V2StopPolling();
      phase3V2SetStatus('Completed', 'done');
    } else {
      phase3V2SetStatus('Idle');
    }
  } catch (e) {
    if (!silent) {
      alert(e.message || 'Failed to load run detail.');
    }
  }
}

function phase3V2ArmDisplayName(arm) {
  if (arm === 'claude_sdk') return 'Claude SDK';
  return 'Control';
}

function phase3V2ApiErrorMessage(data, fallback) {
  if (data && typeof data === 'object') {
    const direct = String(data.error || '').trim();
    if (direct) return direct;
    const detail = data.detail;
    if (typeof detail === 'string' && detail.trim()) return detail.trim();
    if (detail && typeof detail === 'object' && typeof detail.msg === 'string' && detail.msg.trim()) {
      return detail.msg.trim();
    }
  }
  return fallback;
}

function phase3V2SectionLooksPlaceholder(sectionName, value) {
  const text = String(value || '').trim().toLowerCase();
  if (!text) return true;
  if (['hook', 'problem', 'mechanism', 'proof', 'cta'].includes(text)) return true;
  if (text === String(sectionName || '').trim().toLowerCase()) return true;
  if (/^l\d{2}(\s*-\s*l\d{2})?$/.test(text)) return true;
  return false;
}

function phase3V2LinesSnippet(lines) {
  if (!Array.isArray(lines) || !lines.length) return '';
  const ordered = [...lines].sort((a, b) => {
    const an = parseInt(String(a?.line_id || '').replace(/[^\d]/g, ''), 10) || 0;
    const bn = parseInt(String(b?.line_id || '').replace(/[^\d]/g, ''), 10) || 0;
    return an - bn;
  });
  const picked = ordered.slice(0, 5).map((line) => String(line?.text || '').trim()).filter(Boolean);
  if (!picked.length) return '';
  return picked.join(' ').slice(0, 700);
}

function phase3V2UnitArmSnippet(draft) {
  if (!draft || typeof draft !== 'object') return 'No draft generated.';
  if (String(draft.status || '') === 'blocked') {
    return `Blocked: ${draft.error || 'insufficient evidence'}`;
  }
  if (String(draft.status || '') === 'error') {
    return `Error: ${draft.error || 'generation failed'}`;
  }
  const sections = draft.sections || {};
  const parts = [];
  if (sections.hook && !phase3V2SectionLooksPlaceholder('hook', sections.hook)) parts.push(`Hook: ${sections.hook}`);
  if (sections.problem && !phase3V2SectionLooksPlaceholder('problem', sections.problem)) parts.push(`Problem: ${sections.problem}`);
  if (sections.mechanism && !phase3V2SectionLooksPlaceholder('mechanism', sections.mechanism)) parts.push(`Mechanism: ${sections.mechanism}`);
  if (sections.proof && !phase3V2SectionLooksPlaceholder('proof', sections.proof)) parts.push(`Proof: ${sections.proof}`);
  if (sections.cta && !phase3V2SectionLooksPlaceholder('cta', sections.cta)) parts.push(`CTA: ${sections.cta}`);
  const text = parts.join('\n');
  if (!text) {
    const fallback = phase3V2LinesSnippet(draft.lines);
    if (fallback) return fallback;
    return 'No sections returned.';
  }
  return text.length > 700 ? `${text.slice(0, 700)}...` : text;
}

function phase3V2EvidenceChipsHtml(evidenceIds) {
  const refs = Array.isArray(evidenceIds)
    ? evidenceIds.map(v => String(v || '').trim()).filter(Boolean)
    : [];
  if (!refs.length) {
    return '<span class="phase3-v2-evidence-empty">No evidence</span>';
  }
  return refs.map(ref => `<span class="phase3-v2-evidence-chip">${esc(ref)}</span>`).join('');
}

function phase3V2UnitArmExpandedHtml(draft) {
  if (!draft || typeof draft !== 'object') {
    return '<div class="phase3-v2-expanded-alert">No draft generated.</div>';
  }
  const status = String(draft.status || '');
  if (status === 'blocked') {
    return `<div class="phase3-v2-expanded-alert">Blocked: ${esc(draft.error || 'insufficient evidence')}</div>`;
  }
  if (status === 'error') {
    return `<div class="phase3-v2-expanded-alert">Error: ${esc(draft.error || 'generation failed')}</div>`;
  }

  const sections = draft.sections || {};
  const sectionRows = [
    ['Hook', sections.hook],
    ['Problem', sections.problem],
    ['Mechanism', sections.mechanism],
    ['Proof', sections.proof],
    ['CTA', sections.cta],
  ]
    .map(([label, text]) => [label, String(text || '').trim()])
    .filter(([, text]) => Boolean(text));
  const sectionsHtml = sectionRows.length
    ? `<div class="phase3-v2-expanded-sections">${
        sectionRows
          .map(([label, text]) => (
            `<div class="phase3-v2-expanded-section"><div class="phase3-v2-expanded-section-label">${esc(label)}</div><div class="phase3-v2-expanded-section-text">${esc(text)}</div></div>`
          ))
          .join('')
      }</div>`
    : '';

  const ordered = Array.isArray(draft.lines)
    ? [...draft.lines].sort((a, b) => {
        const an = parseInt(String(a?.line_id || '').replace(/[^\d]/g, ''), 10) || 0;
        const bn = parseInt(String(b?.line_id || '').replace(/[^\d]/g, ''), 10) || 0;
        return an - bn;
      })
    : [];

  const lineRows = ordered
    .map((line) => {
      const lineId = String(line?.line_id || '').trim() || 'line';
      const text = String(line?.text || '').trim();
      if (!text) return '';
      return `
        <tr>
          <td class="phase3-v2-lines-col">
            <div class="phase3-v2-line-id">${esc(lineId)}</div>
            <div class="phase3-v2-line-text">${esc(text)}</div>
          </td>
          <td class="phase3-v2-evidence-col">${phase3V2EvidenceChipsHtml(line?.evidence_ids)}</td>
        </tr>
      `;
    })
    .filter(Boolean)
    .join('');

  const linesHtml = lineRows
    ? `
      <table class="phase3-v2-lines-table">
        <thead>
          <tr>
            <th>Line Copy</th>
            <th>Evidence</th>
          </tr>
        </thead>
        <tbody>${lineRows}</tbody>
      </table>
    `
    : '<div class="phase3-v2-expanded-alert">No lines returned.</div>';

  return `${sectionsHtml}${linesHtml}`;
}

function phase3V2BuildExpandedItems() {
  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object') return [];
  const units = Array.isArray(detail.brief_units) ? detail.brief_units : [];
  const draftsByArm = detail.drafts_by_arm && typeof detail.drafts_by_arm === 'object' ? detail.drafts_by_arm : {};
  const runArmsRaw = Array.isArray(detail.run?.arms) ? detail.run.arms : Object.keys(draftsByArm);
  const runArms = runArmsRaw.map(v => String(v || '').trim()).filter(Boolean);
  const items = [];
  units.forEach((unit) => {
    const unitId = String(unit?.brief_unit_id || '').trim();
    if (!unitId) return;
    runArms.forEach((arm, armIndex) => {
      const rows = Array.isArray(draftsByArm[arm]) ? draftsByArm[arm] : [];
      const draft = rows.find(row => String(row?.brief_unit_id || '').trim() === unitId) || null;
      items.push({
        key: `${unitId}::${arm}`,
        briefUnitId: unitId,
        arm,
        armIndex,
        awarenessLevel: String(unit?.awareness_level || ''),
        emotionLabel: String(unit?.emotion_label || unit?.emotion_key || ''),
        status: String(draft?.status || 'missing'),
        draft,
      });
    });
  });
  return items;
}

function phase3V2CurrentExpandedItem() {
  if (phase3V2ExpandedIndex < 0 || phase3V2ExpandedIndex >= phase3V2ExpandedArms.length) return null;
  return phase3V2ExpandedArms[phase3V2ExpandedIndex] || null;
}

function phase3V2BuildEditorLineRow(text = '', evidenceText = '', disabled = false) {
  const lockAttr = disabled ? 'disabled' : '';
  const askBtn = `<button class="btn btn-ghost btn-sm phase3-v2-line-chat" onclick="phase3V2AddLineToChat(this)" ${lockAttr}>Ask Claude</button>`;
  const removeBtn = disabled
    ? ''
    : '<button class="btn btn-ghost btn-sm phase3-v2-line-remove" onclick="phase3V2RemoveEditorLine(this)">Remove</button>';
  const dragAttr = disabled ? 'false' : 'true';
  const actions = `${askBtn}${removeBtn}`;
  return `
    <tr class="p3v2-edit-line-row">
      <td class="phase3-v2-reorder-col">
        <button
          type="button"
          class="p3v2-drag-handle"
          draggable="${dragAttr}"
          ${lockAttr}
          title="Drag to reorder line"
          aria-label="Drag to reorder line"
        >⋮⋮</button>
        <span class="p3v2-line-order">L--</span>
      </td>
      <td class="phase3-v2-lines-col">
        <textarea class="p3v2-edit-line-text" placeholder="Line copy" ${lockAttr}>${esc(text)}</textarea>
      </td>
      <td class="phase3-v2-evidence-col">
        <input class="p3v2-edit-evidence-text" type="text" placeholder="e.g. VOC-001, PROOF-003" value="${esc(evidenceText)}" ${lockAttr}>
      </td>
      <td class="phase3-v2-edit-col"><div class="phase3-v2-line-actions">${actions}</div></td>
    </tr>
  `;
}

function phase3V2AddLineToChat(btn) {
  const row = btn?.closest?.('.p3v2-edit-line-row');
  if (!row) return;
  const lineOrder = String(row.querySelector('.p3v2-line-order')?.textContent || 'Line').trim() || 'Line';
  const lineText = String(row.querySelector('.p3v2-edit-line-text')?.value || '').trim();
  const evidenceText = String(row.querySelector('.p3v2-edit-evidence-text')?.value || '').trim();
  if (!lineText) {
    alert('This line is empty. Add text first, then send it to chat.');
    return;
  }

  phase3V2SwitchArmTab('chat');
  const input = document.getElementById('phase3-v2-chat-input');
  if (!input) return;

  const prompt = evidenceText
    ? `${lineText}\n${evidenceText}`
    : lineText;

  const existing = String(input.value || '').trim();
  input.value = existing ? `${existing}\n\n${prompt}` : prompt;
  input.focus();
  input.setSelectionRange(input.value.length, input.value.length);
  phase3V2SetChatStatus(`${lineOrder} added to chat prompt.`, 'ok');
}

function phase3V2RefreshEditorLineOrderLabels() {
  const rows = Array.from(document.querySelectorAll('#p3v2-edit-lines-body .p3v2-edit-line-row'));
  rows.forEach((row, idx) => {
    const label = row.querySelector('.p3v2-line-order');
    if (label) {
      label.textContent = `L${String(idx + 1).padStart(2, '0')}`;
    }
  });
}

function phase3V2WireEditorReorder() {
  const tbody = document.getElementById('p3v2-edit-lines-body');
  if (!tbody || tbody.dataset.reorderBound === '1') {
    phase3V2RefreshEditorLineOrderLabels();
    return;
  }
  tbody.dataset.reorderBound = '1';

  tbody.addEventListener('dragstart', (event) => {
    const handle = event.target?.closest?.('.p3v2-drag-handle');
    if (!handle || phase3V2IsLocked()) {
      event.preventDefault();
      return;
    }
    const row = handle.closest('.p3v2-edit-line-row');
    if (!row) {
      event.preventDefault();
      return;
    }
    phase3V2DraggingEditorRow = row;
    row.classList.add('dragging');
    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = 'move';
      event.dataTransfer.setData('text/plain', 'reorder-line');
    }
  });

  tbody.addEventListener('dragover', (event) => {
    if (!phase3V2DraggingEditorRow || phase3V2IsLocked()) return;
    const targetRow = event.target?.closest?.('.p3v2-edit-line-row');
    if (!targetRow || targetRow === phase3V2DraggingEditorRow) return;
    event.preventDefault();
    if (event.dataTransfer) {
      event.dataTransfer.dropEffect = 'move';
    }
    const rect = targetRow.getBoundingClientRect();
    const insertBefore = event.clientY < rect.top + (rect.height / 2);
    if (insertBefore) {
      tbody.insertBefore(phase3V2DraggingEditorRow, targetRow);
    } else {
      tbody.insertBefore(phase3V2DraggingEditorRow, targetRow.nextSibling);
    }
  });

  tbody.addEventListener('drop', (event) => {
    if (!phase3V2DraggingEditorRow) return;
    event.preventDefault();
    phase3V2RefreshEditorLineOrderLabels();
  });

  tbody.addEventListener('dragend', () => {
    if (phase3V2DraggingEditorRow) {
      phase3V2DraggingEditorRow.classList.remove('dragging');
      phase3V2DraggingEditorRow = null;
    }
    phase3V2RefreshEditorLineOrderLabels();
  });

  phase3V2RefreshEditorLineOrderLabels();
}

function phase3V2RenderArmScriptPane(item) {
  const mount = document.getElementById('phase3-v2-arm-script-pane');
  if (!mount) return;
  const draft = item?.draft || null;
  const locked = phase3V2IsLocked();
  if (!draft || typeof draft !== 'object') {
    mount.innerHTML = '<div class="phase3-v2-expanded-alert">No draft generated for this Brief Unit.</div>';
    return;
  }
  const status = String(draft.status || '');
  if (status === 'blocked') {
    mount.innerHTML = `<div class="phase3-v2-expanded-alert">Blocked: ${esc(draft.error || 'insufficient evidence')}</div>`;
    return;
  }
  if (status === 'error') {
    mount.innerHTML = `<div class="phase3-v2-expanded-alert">Error: ${esc(draft.error || 'generation failed')}</div>`;
    return;
  }

  const sections = draft.sections || {};
  const lines = Array.isArray(draft.lines) ? draft.lines : [];
  const orderedLines = [...lines].sort((a, b) => {
    const an = parseInt(String(a?.line_id || '').replace(/[^\d]/g, ''), 10) || 0;
    const bn = parseInt(String(b?.line_id || '').replace(/[^\d]/g, ''), 10) || 0;
    return an - bn;
  });
  const lineRows = orderedLines.length
    ? orderedLines.map((line) => (
        phase3V2BuildEditorLineRow(
          String(line?.text || '').trim(),
          Array.isArray(line?.evidence_ids) ? line.evidence_ids.join(', ') : '',
          locked,
        )
      )).join('')
    : phase3V2BuildEditorLineRow('', '', locked);

  mount.innerHTML = `
    ${locked ? '<div class="phase3-v2-locked-banner">This run is Final Locked. Editing is disabled.</div>' : ''}
    <div class="phase3-v2-expanded-sections phase3-v2-edit-sections">
      <label class="phase3-v2-edit-field"><span>Hook</span><textarea id="p3v2-edit-hook" ${locked ? 'disabled' : ''}>${esc(String(sections.hook || ''))}</textarea></label>
      <label class="phase3-v2-edit-field"><span>Problem</span><textarea id="p3v2-edit-problem" ${locked ? 'disabled' : ''}>${esc(String(sections.problem || ''))}</textarea></label>
      <label class="phase3-v2-edit-field"><span>Mechanism</span><textarea id="p3v2-edit-mechanism" ${locked ? 'disabled' : ''}>${esc(String(sections.mechanism || ''))}</textarea></label>
      <label class="phase3-v2-edit-field"><span>Proof</span><textarea id="p3v2-edit-proof" ${locked ? 'disabled' : ''}>${esc(String(sections.proof || ''))}</textarea></label>
      <label class="phase3-v2-edit-field"><span>CTA</span><textarea id="p3v2-edit-cta" ${locked ? 'disabled' : ''}>${esc(String(sections.cta || ''))}</textarea></label>
    </div>
    <table class="phase3-v2-lines-table phase3-v2-edit-table">
      <thead>
        <tr>
          <th>Order</th>
          <th>Line Copy</th>
          <th>Evidence IDs</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="p3v2-edit-lines-body">${lineRows}</tbody>
    </table>
    <div class="phase3-v2-editor-actions">
      <button class="btn btn-ghost btn-sm" onclick="phase3V2AddEditorLine()" ${locked ? 'disabled' : ''}>Add Line</button>
      <button class="btn btn-primary btn-sm" id="phase3-v2-save-script-btn" onclick="phase3V2SaveScriptEdits()" ${locked ? 'disabled' : ''}>Save Script</button>
    </div>
  `;
  phase3V2WireEditorReorder();
}

function phase3V2SetSaveScriptButtonState(state) {
  const btn = document.getElementById('phase3-v2-save-script-btn');
  if (!btn) return;
  if (state === 'saving') {
    btn.disabled = true;
    btn.textContent = 'Saving...';
    return;
  }
  if (state === 'saved') {
    btn.disabled = false;
    btn.innerHTML = '&#10003; Saved';
    return;
  }
  if (state === 'error') {
    btn.disabled = false;
    btn.textContent = 'Save Failed';
    return;
  }
  btn.disabled = false;
  btn.textContent = 'Save Script';
}

function phase3V2SetChatStatus(message = '', state = '') {
  const el = document.getElementById('phase3-v2-chat-status');
  if (!el) return;
  el.textContent = message || '';
  el.classList.remove('ok', 'error', 'pending');
  if (state) el.classList.add(state);
}

function phase3V2RenderChatApplyButton() {
  const btn = document.getElementById('phase3-v2-chat-apply-btn');
  if (!btn) return;
  const hasDraft = Boolean(phase3V2ChatPendingDraft);
  const locked = phase3V2IsLocked();
  btn.classList.toggle('hidden', !hasDraft);
  btn.disabled = !hasDraft || locked;
}

function phase3V2RenderChatMessages(messages = []) {
  const mount = document.getElementById('phase3-v2-chat-messages');
  if (!mount) return;
  if (!Array.isArray(messages) || !messages.length) {
    mount.innerHTML = '<div class="phase3-v2-chat-empty">No chat yet. Ask Claude to improve this script.</div>';
    return;
  }
  mount.innerHTML = messages.map((row) => {
    const role = String(row?.role || '').trim().toLowerCase() === 'user' ? 'user' : 'assistant';
    const content = String(row?.content || '').trim();
    const rendered = renderMarkdownLite(content || '(empty)');
    const meta = role === 'assistant'
      ? `${esc(String(row?.provider || ''))} · ${esc(String(row?.model || ''))}`
      : '';
    return `
      <div class="phase3-v2-chat-msg ${role}">
        <div class="phase3-v2-chat-msg-role">${role === 'user' ? 'You' : 'Claude Opus 4.6'}</div>
        <div class="phase3-v2-chat-msg-body">${rendered}</div>
        ${meta ? `<div class="phase3-v2-chat-msg-meta">${meta}</div>` : ''}
      </div>
    `;
  }).join('');
  mount.scrollTop = mount.scrollHeight;
}

function phase3V2SwitchArmTab(tab) {
  phase3V2ArmTab = tab === 'chat' ? 'chat' : 'script';
  const scriptBtn = document.getElementById('phase3-v2-arm-tab-script');
  const chatBtn = document.getElementById('phase3-v2-arm-tab-chat');
  const scriptPane = document.getElementById('phase3-v2-arm-script-pane');
  const chatPane = document.getElementById('phase3-v2-arm-chat-pane');
  if (!scriptBtn || !chatBtn || !scriptPane || !chatPane) return;
  const scriptActive = phase3V2ArmTab === 'script';
  scriptBtn.classList.toggle('active', scriptActive);
  chatBtn.classList.toggle('active', !scriptActive);
  scriptPane.classList.toggle('hidden', !scriptActive);
  chatPane.classList.toggle('hidden', scriptActive);
  if (!scriptActive) {
    phase3V2RenderChatApplyButton();
    phase3V2LoadChatThread();
  }
}

function phase3V2HandleChatInputKeydown(event) {
  if (!event || event.key !== 'Enter') return;
  if (event.shiftKey || event.isComposing) return;
  event.preventDefault();
  phase3V2SendChat();
}

function phase3V2RenderExpandedModal() {
  const modal = document.getElementById('phase3-v2-arm-modal');
  const title = document.getElementById('phase3-v2-arm-modal-title');
  const subtitle = document.getElementById('phase3-v2-arm-modal-subtitle');
  if (!modal || !title || !subtitle) return;
  const item = phase3V2CurrentExpandedItem();
  if (!item) {
    phase3V2CloseArmExpanded();
    return;
  }
  const itemLabel = `${phase3V2ExpandedIndex + 1}/${phase3V2ExpandedArms.length}`;
  title.textContent = phase3V2ArmDisplayName(item.arm);
  subtitle.textContent = `${item.briefUnitId} · ${humanizeAwareness(item.awarenessLevel)} × ${item.emotionLabel} · ${itemLabel}`;
  phase3V2ExpandedCurrent = item;
  phase3V2ChatPendingDraft = null;
  phase3V2RenderArmScriptPane(item);
  phase3V2RenderChatMessages([]);
  phase3V2SetChatStatus('');
  phase3V2RenderChatApplyButton();
  phase3V2SwitchArmTab(phase3V2ArmTab || 'script');
  modal.classList.remove('hidden');
}

async function phase3V2LoadChatThread() {
  const item = phase3V2ExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  const input = document.getElementById('phase3-v2-chat-input');
  const sendBtn = document.getElementById('phase3-v2-chat-send-btn');
  const reloadBtn = document.getElementById('phase3-v2-chat-reload-btn');
  const applyBtn = document.getElementById('phase3-v2-chat-apply-btn');
  const locked = phase3V2IsLocked();
  if (input) input.disabled = locked;
  if (sendBtn) sendBtn.disabled = locked;
  if (reloadBtn) reloadBtn.disabled = false;
  if (applyBtn) applyBtn.disabled = locked || !phase3V2ChatPendingDraft;
  phase3V2RenderChatApplyButton();
  if (locked) {
    phase3V2SetChatStatus('This run is Final Locked. Chat is read-only.', 'pending');
  } else {
    phase3V2SetChatStatus('Loading chat...', 'pending');
  }

  try {
    const params = new URLSearchParams();
    params.set('brief_unit_id', item.briefUnitId);
    params.set('arm', item.arm);
    if (activeBrandSlug) params.set('brand', activeBrandSlug);
    const resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/chat?${params.toString()}`);
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Failed to load chat (HTTP ${resp.status})`);
    }
    phase3V2RenderChatMessages(Array.isArray(data.messages) ? data.messages : []);
    if (locked) {
      phase3V2SetChatStatus('Final Locked: you can view chat history only.', 'pending');
    } else {
      phase3V2SetChatStatus('');
    }
  } catch (e) {
    phase3V2RenderChatMessages([]);
    phase3V2SetChatStatus(e.message || 'Failed to load chat.', 'error');
  }
}

async function phase3V2SendChat() {
  const item = phase3V2ExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }
  const input = document.getElementById('phase3-v2-chat-input');
  const sendBtn = document.getElementById('phase3-v2-chat-send-btn');
  if (!input || !sendBtn) return;
  const message = String(input.value || '').trim();
  if (!message) return;

  sendBtn.disabled = true;
  input.disabled = true;
  phase3V2ChatPendingDraft = null;
  phase3V2RenderChatApplyButton();
  phase3V2SetChatStatus('Claude is thinking...', 'pending');
  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: activeBrandSlug || '',
        brief_unit_id: item.briefUnitId,
        arm: item.arm,
        message,
      }),
    });
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Chat failed (HTTP ${resp.status})`);
    }
    input.value = '';
    phase3V2RenderChatMessages(Array.isArray(data.messages) ? data.messages : []);
    phase3V2ChatPendingDraft = data.has_proposed_draft ? data.proposed_draft : null;
    phase3V2RenderChatApplyButton();
    if (phase3V2ChatPendingDraft) {
      phase3V2SetChatStatus('Claude proposed script changes. Click Apply Claude Changes to update this draft.', 'ok');
    } else {
      phase3V2SetChatStatus('Reply received.', 'ok');
    }
  } catch (e) {
    phase3V2SetChatStatus(e.message || 'Chat request failed.', 'error');
  } finally {
    sendBtn.disabled = false;
    input.disabled = false;
    input.focus();
  }
}

async function phase3V2ApplyChatChanges() {
  const item = phase3V2ExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId || !phase3V2ChatPendingDraft) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }
  phase3V2RenderChatApplyButton();
  phase3V2SetChatStatus('Applying Claude changes...', 'pending');
  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/chat/apply`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: activeBrandSlug || '',
        brief_unit_id: item.briefUnitId,
        arm: item.arm,
        proposed_draft: phase3V2ChatPendingDraft,
      }),
    });
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Apply failed (HTTP ${resp.status})`);
    }
    phase3V2ChatPendingDraft = null;
    phase3V2RenderChatApplyButton();
    phase3V2SetChatStatus('Claude changes applied.', 'ok');
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
  } catch (e) {
    phase3V2SetChatStatus(e.message || 'Failed to apply Claude changes.', 'error');
  }
}

function phase3V2AddEditorLine() {
  if (phase3V2IsLocked()) return;
  const tbody = document.getElementById('p3v2-edit-lines-body');
  if (!tbody) return;
  tbody.insertAdjacentHTML('beforeend', phase3V2BuildEditorLineRow('', '', false));
  phase3V2RefreshEditorLineOrderLabels();
}

function phase3V2RemoveEditorLine(btn) {
  if (phase3V2IsLocked()) return;
  const row = btn?.closest?.('tr');
  const tbody = document.getElementById('p3v2-edit-lines-body');
  if (!tbody || !row) return;
  const allRows = tbody.querySelectorAll('.p3v2-edit-line-row');
  if (allRows.length <= 1) return;
  row.remove();
  phase3V2RefreshEditorLineOrderLabels();
}

async function phase3V2SaveScriptEdits() {
  const item = phase3V2ExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }

  const hook = String(document.getElementById('p3v2-edit-hook')?.value || '').trim();
  const problem = String(document.getElementById('p3v2-edit-problem')?.value || '').trim();
  const mechanism = String(document.getElementById('p3v2-edit-mechanism')?.value || '').trim();
  const proof = String(document.getElementById('p3v2-edit-proof')?.value || '').trim();
  const cta = String(document.getElementById('p3v2-edit-cta')?.value || '').trim();
  if (!hook || !problem || !mechanism || !proof || !cta) {
    alert('All sections (Hook, Problem, Mechanism, Proof, CTA) are required.');
    return;
  }

  const rowEls = Array.from(document.querySelectorAll('#p3v2-edit-lines-body .p3v2-edit-line-row'));
  const lines = rowEls.map((row) => {
    const text = String(row.querySelector('.p3v2-edit-line-text')?.value || '').trim();
    const evidenceText = String(row.querySelector('.p3v2-edit-evidence-text')?.value || '').trim();
    const evidence_ids = evidenceText
      ? evidenceText.split(',').map(v => String(v || '').trim()).filter(Boolean)
      : [];
    return { line_id: '', text, evidence_ids };
  }).filter(row => Boolean(row.text));

  if (!lines.length) {
    alert('Add at least one non-empty line before saving.');
    return;
  }

  phase3V2SetSaveScriptButtonState('saving');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/drafts/${encodeURIComponent(item.arm)}/${encodeURIComponent(item.briefUnitId)}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          source: 'manual',
          sections: { hook, problem, mechanism, proof, cta },
          lines,
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Save failed (HTTP ${resp.status})`);
    }
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
    phase3V2SetSaveScriptButtonState('saved');
    setTimeout(() => {
      phase3V2SetSaveScriptButtonState('idle');
    }, 1600);
    phase3V2SetChatStatus('Script edits saved.', 'ok');
  } catch (e) {
    phase3V2SetSaveScriptButtonState('error');
    setTimeout(() => {
      phase3V2SetSaveScriptButtonState('idle');
    }, 1600);
    alert(e.message || 'Failed to save script edits.');
  }
}

function phase3V2OpenArmExpanded(briefUnitId, arm) {
  const unitId = String(briefUnitId || '').trim();
  const armKey = String(arm || '').trim();
  if (!unitId || !armKey) return;
  phase3V2ExpandedArms = phase3V2BuildExpandedItems();
  phase3V2ExpandedIndex = phase3V2ExpandedArms.findIndex(item => item.key === `${unitId}::${armKey}`);
  if (phase3V2ExpandedIndex < 0) return;
  phase3V2ArmTab = 'script';
  phase3V2RenderExpandedModal();
}

function phase3V2CloseArmExpanded() {
  const modal = document.getElementById('phase3-v2-arm-modal');
  if (modal) modal.classList.add('hidden');
  phase3V2ExpandedArms = [];
  phase3V2ExpandedIndex = -1;
  phase3V2ExpandedCurrent = null;
  phase3V2ChatPendingDraft = null;
  phase3V2ArmTab = 'script';
}

function phase3V2PrevArmExpanded() {
  if (!phase3V2ExpandedArms.length) return;
  phase3V2ExpandedIndex = (phase3V2ExpandedIndex - 1 + phase3V2ExpandedArms.length) % phase3V2ExpandedArms.length;
  phase3V2RenderExpandedModal();
}

function phase3V2NextArmExpanded() {
  if (!phase3V2ExpandedArms.length) return;
  phase3V2ExpandedIndex = (phase3V2ExpandedIndex + 1) % phase3V2ExpandedArms.length;
  phase3V2RenderExpandedModal();
}

function phase3V2BuildHookExpandedItems() {
  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object') return [];
  const eligibility = detail.hook_eligibility && typeof detail.hook_eligibility === 'object'
    ? detail.hook_eligibility
    : { eligible: [] };
  const eligibleRows = Array.isArray(eligibility.eligible) ? eligibility.eligible : [];
  const items = [];
  eligibleRows.forEach((row) => {
    const briefUnitId = String(row?.brief_unit_id || '').trim();
    const arm = String(row?.arm || '').trim();
    if (!briefUnitId || !arm) return;
    const bundle = phase3V2FindHookBundle(detail, briefUnitId, arm) || {};
    const variants = Array.isArray(bundle.variants) ? bundle.variants : [];
    const awarenessLevel = String(row?.awareness_level || '').trim();
    const emotionLabel = String(row?.emotion_label || row?.emotion_key || '').trim();
    variants.forEach((variant, idx) => {
      const hookId = String(variant?.hook_id || '').trim();
      if (!hookId) return;
      items.push({
        key: `${briefUnitId}::${arm}::${hookId}`,
        briefUnitId,
        arm,
        hookId,
        awarenessLevel,
        emotionLabel,
        variantIndex: idx,
        variant,
      });
    });
  });
  return items;
}

function phase3V2CurrentHookExpandedItem() {
  if (phase3V2HookExpandedIndex < 0 || phase3V2HookExpandedIndex >= phase3V2HookExpandedItems.length) return null;
  return phase3V2HookExpandedItems[phase3V2HookExpandedIndex] || null;
}

function phase3V2SetSaveHookButtonState(state) {
  const btn = document.getElementById('phase3-v2-save-hook-btn');
  if (!btn) return;
  if (state === 'saving') {
    btn.disabled = true;
    btn.textContent = 'Saving...';
    return;
  }
  if (state === 'saved') {
    btn.disabled = false;
    btn.innerHTML = '&#10003; Saved';
    return;
  }
  if (state === 'error') {
    btn.disabled = false;
    btn.textContent = 'Save Failed';
    return;
  }
  btn.disabled = false;
  btn.textContent = 'Save Hook';
}

function phase3V2RenderHookEditorPane(item) {
  const mount = document.getElementById('phase3-v2-hook-editor-pane');
  if (!mount) return;
  const variant = item?.variant || null;
  const locked = phase3V2IsLocked();
  if (!variant || typeof variant !== 'object') {
    mount.innerHTML = '<div class="phase3-v2-expanded-alert">No hook variant found.</div>';
    return;
  }
  const evidenceText = Array.isArray(variant.evidence_ids)
    ? variant.evidence_ids.map((v) => String(v || '').trim()).filter(Boolean).join(', ')
    : '';
  mount.innerHTML = `
    ${locked ? '<div class="phase3-v2-locked-banner">This run is Final Locked. Editing is disabled.</div>' : ''}
    <div class="phase3-v2-expanded-sections phase3-v2-edit-sections phase3-v2-hook-edit-grid">
      <label class="phase3-v2-edit-field phase3-v2-hook-edit-field">
        <span>Verbal Open</span>
        <textarea id="p3v2-hook-edit-verbal" ${locked ? 'disabled' : ''}>${esc(String(variant.verbal_open || ''))}</textarea>
      </label>
      <label class="phase3-v2-edit-field phase3-v2-hook-edit-field">
        <span>Visual Pattern Interrupt</span>
        <textarea id="p3v2-hook-edit-visual" ${locked ? 'disabled' : ''}>${esc(String(variant.visual_pattern_interrupt || ''))}</textarea>
      </label>
      <label class="phase3-v2-edit-field phase3-v2-hook-edit-field">
        <span>On-screen Text</span>
        <textarea id="p3v2-hook-edit-onscreen" ${locked ? 'disabled' : ''}>${esc(String(variant.on_screen_text || ''))}</textarea>
      </label>
      <label class="phase3-v2-edit-field phase3-v2-hook-edit-field">
        <span>Evidence IDs (comma-separated)</span>
        <input id="p3v2-hook-edit-evidence" type="text" value="${esc(evidenceText)}" placeholder="e.g. VOC-001, PROOF-003" ${locked ? 'disabled' : ''}>
      </label>
    </div>
    <div class="phase3-v2-editor-actions">
      <button class="btn btn-primary btn-sm" id="phase3-v2-save-hook-btn" onclick="phase3V2SaveHookEdits()" ${locked ? 'disabled' : ''}>Save Hook</button>
    </div>
  `;
}

function phase3V2SetHookChatStatus(message = '', state = '') {
  const el = document.getElementById('phase3-v2-hook-chat-status');
  if (!el) return;
  el.textContent = message || '';
  el.classList.remove('ok', 'error', 'pending');
  if (state) el.classList.add(state);
}

function phase3V2RenderHookChatApplyButton() {
  const btn = document.getElementById('phase3-v2-hook-chat-apply-btn');
  if (!btn) return;
  const hasProposal = Boolean(phase3V2HookChatPendingHook);
  const locked = phase3V2IsLocked();
  btn.classList.toggle('hidden', !hasProposal);
  btn.disabled = !hasProposal || locked;
}

function phase3V2RenderHookChatMessages(messages = []) {
  const mount = document.getElementById('phase3-v2-hook-chat-messages');
  if (!mount) return;
  if (!Array.isArray(messages) || !messages.length) {
    mount.innerHTML = '<div class="phase3-v2-chat-empty">No chat yet. Ask Claude to improve this hook.</div>';
    return;
  }
  mount.innerHTML = messages.map((row) => {
    const role = String(row?.role || '').trim().toLowerCase() === 'user' ? 'user' : 'assistant';
    const content = String(row?.content || '').trim();
    const rendered = renderMarkdownLite(content || '(empty)');
    const meta = role === 'assistant'
      ? `${esc(String(row?.provider || ''))} · ${esc(String(row?.model || ''))}`
      : '';
    return `
      <div class="phase3-v2-chat-msg ${role}">
        <div class="phase3-v2-chat-msg-role">${role === 'user' ? 'You' : 'Claude Opus 4.6'}</div>
        <div class="phase3-v2-chat-msg-body">${rendered}</div>
        ${meta ? `<div class="phase3-v2-chat-msg-meta">${meta}</div>` : ''}
      </div>
    `;
  }).join('');
  mount.scrollTop = mount.scrollHeight;
}

function phase3V2SwitchHookTab(tab) {
  phase3V2HookTab = tab === 'chat' ? 'chat' : 'hook';
  const hookBtn = document.getElementById('phase3-v2-hook-tab-editor');
  const chatBtn = document.getElementById('phase3-v2-hook-tab-chat');
  const hookPane = document.getElementById('phase3-v2-hook-editor-pane');
  const chatPane = document.getElementById('phase3-v2-hook-chat-pane');
  if (!hookBtn || !chatBtn || !hookPane || !chatPane) return;
  const hookActive = phase3V2HookTab === 'hook';
  hookBtn.classList.toggle('active', hookActive);
  chatBtn.classList.toggle('active', !hookActive);
  hookPane.classList.toggle('hidden', !hookActive);
  chatPane.classList.toggle('hidden', hookActive);
  if (!hookActive) {
    phase3V2RenderHookChatApplyButton();
    phase3V2LoadHookChatThread();
  }
}

function phase3V2HandleHookChatInputKeydown(event) {
  if (!event || event.key !== 'Enter') return;
  if (event.shiftKey || event.isComposing) return;
  event.preventDefault();
  phase3V2SendHookChat();
}

function phase3V2RenderHookExpandedModal() {
  const modal = document.getElementById('phase3-v2-hook-modal');
  const title = document.getElementById('phase3-v2-hook-modal-title');
  const subtitle = document.getElementById('phase3-v2-hook-modal-subtitle');
  if (!modal || !title || !subtitle) return;
  const item = phase3V2CurrentHookExpandedItem();
  if (!item) {
    phase3V2CloseHookExpanded();
    return;
  }
  const itemLabel = `${phase3V2HookExpandedIndex + 1}/${phase3V2HookExpandedItems.length}`;
  title.textContent = item.hookId || 'Hook Variant';
  subtitle.textContent = `${item.briefUnitId} · ${humanizeAwareness(item.awarenessLevel)} × ${item.emotionLabel} · ${phase3V2ArmDisplayName(item.arm)} · ${itemLabel}`;
  phase3V2HookExpandedCurrent = item;
  phase3V2HookChatPendingHook = null;
  phase3V2RenderHookEditorPane(item);
  phase3V2RenderHookChatMessages([]);
  phase3V2SetHookChatStatus('');
  phase3V2RenderHookChatApplyButton();
  phase3V2SwitchHookTab(phase3V2HookTab || 'hook');
  modal.classList.remove('hidden');
}

async function phase3V2LoadHookChatThread() {
  const item = phase3V2HookExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  const input = document.getElementById('phase3-v2-hook-chat-input');
  const sendBtn = document.getElementById('phase3-v2-hook-chat-send-btn');
  const reloadBtn = document.getElementById('phase3-v2-hook-chat-reload-btn');
  const applyBtn = document.getElementById('phase3-v2-hook-chat-apply-btn');
  const locked = phase3V2IsLocked();
  if (input) input.disabled = locked;
  if (sendBtn) sendBtn.disabled = locked;
  if (reloadBtn) reloadBtn.disabled = false;
  if (applyBtn) applyBtn.disabled = locked || !phase3V2HookChatPendingHook;
  phase3V2RenderHookChatApplyButton();
  phase3V2SetHookChatStatus(locked ? 'This run is Final Locked. Chat is read-only.' : 'Loading chat...', 'pending');

  try {
    const params = new URLSearchParams();
    params.set('brief_unit_id', item.briefUnitId);
    params.set('arm', item.arm);
    params.set('hook_id', item.hookId);
    if (activeBrandSlug) params.set('brand', activeBrandSlug);
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/hooks/chat?${params.toString()}`
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(phase3V2ApiErrorMessage(data, `Failed to load hook chat (HTTP ${resp.status})`));
    }
    phase3V2RenderHookChatMessages(Array.isArray(data.messages) ? data.messages : []);
    if (locked) {
      phase3V2SetHookChatStatus('Final Locked: you can view chat history only.', 'pending');
    } else {
      phase3V2SetHookChatStatus('');
    }
  } catch (e) {
    phase3V2RenderHookChatMessages([]);
    phase3V2SetHookChatStatus(e.message || 'Failed to load hook chat.', 'error');
  }
}

async function phase3V2SendHookChat() {
  const item = phase3V2HookExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }
  const input = document.getElementById('phase3-v2-hook-chat-input');
  const sendBtn = document.getElementById('phase3-v2-hook-chat-send-btn');
  if (!input || !sendBtn) return;
  const message = String(input.value || '').trim();
  if (!message) return;

  sendBtn.disabled = true;
  input.disabled = true;
  phase3V2HookChatPendingHook = null;
  phase3V2RenderHookChatApplyButton();
  phase3V2SetHookChatStatus('Claude is thinking...', 'pending');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/hooks/chat`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          brief_unit_id: item.briefUnitId,
          arm: item.arm,
          hook_id: item.hookId,
          message,
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(phase3V2ApiErrorMessage(data, `Hook chat failed (HTTP ${resp.status})`));
    }
    input.value = '';
    phase3V2RenderHookChatMessages(Array.isArray(data.messages) ? data.messages : []);
    phase3V2HookChatPendingHook = data.has_proposed_hook ? data.proposed_hook : null;
    phase3V2RenderHookChatApplyButton();
    if (phase3V2HookChatPendingHook) {
      phase3V2SetHookChatStatus('Claude proposed hook changes. Click Apply Claude Changes to update this hook.', 'ok');
    } else {
      phase3V2SetHookChatStatus('Reply received.', 'ok');
    }
  } catch (e) {
    phase3V2SetHookChatStatus(e.message || 'Hook chat request failed.', 'error');
  } finally {
    sendBtn.disabled = false;
    input.disabled = false;
    input.focus();
  }
}

async function phase3V2ApplyHookChatChanges() {
  const item = phase3V2HookExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId || !phase3V2HookChatPendingHook) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }
  phase3V2RenderHookChatApplyButton();
  phase3V2SetHookChatStatus('Applying Claude changes...', 'pending');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/hooks/chat/apply`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          brief_unit_id: item.briefUnitId,
          arm: item.arm,
          hook_id: item.hookId,
          proposed_hook: phase3V2HookChatPendingHook,
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(phase3V2ApiErrorMessage(data, `Apply failed (HTTP ${resp.status})`));
    }
    phase3V2HookChatPendingHook = null;
    phase3V2RenderHookChatApplyButton();
    phase3V2SetHookChatStatus('Claude changes applied.', 'ok');
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
  } catch (e) {
    phase3V2SetHookChatStatus(e.message || 'Failed to apply Claude changes.', 'error');
  }
}

async function phase3V2SaveHookEdits() {
  const item = phase3V2HookExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }
  const verbal = String(document.getElementById('p3v2-hook-edit-verbal')?.value || '').trim();
  const visual = String(document.getElementById('p3v2-hook-edit-visual')?.value || '').trim();
  const onScreen = String(document.getElementById('p3v2-hook-edit-onscreen')?.value || '').trim();
  const evidenceText = String(document.getElementById('p3v2-hook-edit-evidence')?.value || '').trim();
  if (!verbal) {
    alert('Verbal Open is required.');
    return;
  }
  if (!visual) {
    alert('Visual Pattern Interrupt is required.');
    return;
  }
  const evidenceIds = evidenceText
    ? [...new Set(evidenceText.split(',').map((v) => String(v || '').trim()).filter(Boolean))]
    : [];

  phase3V2SetSaveHookButtonState('saving');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/hooks/update`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          brief_unit_id: item.briefUnitId,
          arm: item.arm,
          hook_id: item.hookId,
          verbal_open: verbal,
          visual_pattern_interrupt: visual,
          on_screen_text: onScreen,
          evidence_ids: evidenceIds,
          source: 'manual',
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(phase3V2ApiErrorMessage(data, `Save failed (HTTP ${resp.status})`));
    }
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
    phase3V2SetSaveHookButtonState('saved');
    setTimeout(() => {
      phase3V2SetSaveHookButtonState('idle');
    }, 1600);
    phase3V2SetHookChatStatus('Hook edits saved.', 'ok');
  } catch (e) {
    phase3V2SetSaveHookButtonState('error');
    setTimeout(() => {
      phase3V2SetSaveHookButtonState('idle');
    }, 1600);
    alert(e.message || 'Failed to save hook edits.');
  }
}

function phase3V2OpenHookExpanded(briefUnitId, arm, hookId) {
  const unitId = String(briefUnitId || '').trim();
  const armKey = String(arm || '').trim();
  const variantId = String(hookId || '').trim();
  if (!unitId || !armKey || !variantId) return;
  phase3V2HookExpandedItems = phase3V2BuildHookExpandedItems();
  phase3V2HookExpandedIndex = phase3V2HookExpandedItems.findIndex(
    (item) => item.key === `${unitId}::${armKey}::${variantId}`,
  );
  if (phase3V2HookExpandedIndex < 0) return;
  phase3V2HookTab = 'hook';
  phase3V2RenderHookExpandedModal();
}

function phase3V2CloseHookExpanded() {
  const modal = document.getElementById('phase3-v2-hook-modal');
  if (modal) modal.classList.add('hidden');
  phase3V2HookExpandedItems = [];
  phase3V2HookExpandedIndex = -1;
  phase3V2HookExpandedCurrent = null;
  phase3V2HookTab = 'hook';
  phase3V2HookChatPendingHook = null;
}

function phase3V2PrevHookExpanded() {
  if (!phase3V2HookExpandedItems.length) return;
  phase3V2HookExpandedIndex = (phase3V2HookExpandedIndex - 1 + phase3V2HookExpandedItems.length) % phase3V2HookExpandedItems.length;
  phase3V2RenderHookExpandedModal();
}

function phase3V2NextHookExpanded() {
  if (!phase3V2HookExpandedItems.length) return;
  phase3V2HookExpandedIndex = (phase3V2HookExpandedIndex + 1) % phase3V2HookExpandedItems.length;
  phase3V2RenderHookExpandedModal();
}

function phase3V2DecisionMap(detail) {
  const map = {};
  if (!detail || typeof detail !== 'object') return map;

  const decisions = Array.isArray(detail.decisions) ? detail.decisions : [];
  if (decisions.length) {
    decisions.forEach((row) => {
      if (!row || typeof row !== 'object') return;
      const unitId = String(row.brief_unit_id || '').trim();
      const arm = String(row.arm || '').trim();
      if (!unitId || !arm) return;
      map[`${unitId}::${arm}`] = String(row.decision || '').trim().toLowerCase();
    });
    return map;
  }

  const reviews = Array.isArray(detail.reviews) ? detail.reviews : [];
  reviews.forEach((row) => {
    if (!row || typeof row !== 'object') return;
    const unitId = String(row.brief_unit_id || '').trim();
    const arm = String(row.arm || '').trim();
    if (!unitId || !arm) return;
    map[`${unitId}::${arm}`] = String(row.decision || '').trim().toLowerCase();
  });
  return map;
}

function phase3V2IsManualSkipDecision(decisionValue) {
  const value = String(decisionValue || '').trim().toLowerCase();
  return value === 'revise' || value === 'reject';
}

function phase3V2IsAutoSkippedDraft(draft) {
  if (!draft || typeof draft !== 'object') return true;
  const status = String(draft.status || '').trim().toLowerCase();
  return status === 'blocked' || status === 'error' || status === 'missing';
}

function phase3V2SelectionStats(detail) {
  if (!detail || typeof detail !== 'object') {
    return {
      total: 0,
      included: 0,
      manuallySkipped: 0,
      autoSkipped: 0,
      autoSkippedUnits: 0,
    };
  }
  const units = Array.isArray(detail.brief_units) ? detail.brief_units : [];
  const draftsByArm = detail.drafts_by_arm && typeof detail.drafts_by_arm === 'object' ? detail.drafts_by_arm : {};
  const runArmsRaw = Array.isArray(detail.run?.arms) ? detail.run.arms : Object.keys(draftsByArm);
  const runArms = runArmsRaw.map(v => String(v || '').trim()).filter(Boolean);
  const decisionMap = phase3V2DecisionMap(detail);

  const draftMaps = {};
  runArms.forEach((arm) => {
    const rows = Array.isArray(draftsByArm[arm]) ? draftsByArm[arm] : [];
    const map = {};
    rows.forEach((row) => {
      const unitId = String(row?.brief_unit_id || '').trim();
      if (unitId) map[unitId] = row;
    });
    draftMaps[arm] = map;
  });

  let total = 0;
  let included = 0;
  let manuallySkipped = 0;
  let autoSkipped = 0;
  const autoSkippedUnits = new Set();

  units.forEach((unit) => {
    const unitId = String(unit?.brief_unit_id || '').trim();
    if (!unitId) return;
    runArms.forEach((arm) => {
      total += 1;
      const draft = draftMaps[arm]?.[unitId] || null;
      const decisionKey = `${unitId}::${arm}`;
      if (phase3V2IsAutoSkippedDraft(draft)) {
        autoSkipped += 1;
        autoSkippedUnits.add(unitId);
        return;
      }
      if (phase3V2IsManualSkipDecision(decisionMap[decisionKey])) {
        manuallySkipped += 1;
      } else {
        included += 1;
      }
    });
  });

  return {
    total,
    included,
    manuallySkipped,
    autoSkipped,
    autoSkippedUnits: autoSkippedUnits.size,
  };
}

function phase3V2SetUnitFilter(type, value) {
  const filterType = type === 'emotion' ? 'emotion' : 'awareness';
  phase3V2UnitFilters[filterType] = String(value || 'all').trim() || 'all';
  phase3V2RenderCurrentRun();
}

function phase3V2ClearUnitFilters() {
  phase3V2UnitFilters = { awareness: 'all', emotion: 'all' };
  phase3V2RenderCurrentRun();
}

function phase3V2BuildUnitFilterOptions(units) {
  const awarenessSet = new Set();
  const emotionMap = new Map();

  units.forEach((unit) => {
    const awareness = String(unit?.awareness_level || '').trim().toLowerCase();
    if (awareness) awarenessSet.add(awareness);

    const rawEmotion = String(unit?.emotion_key || unit?.emotion_label || '').trim();
    const emotionKey = normalizeEmotionKey(rawEmotion);
    if (!emotionKey) return;
    const label = String(unit?.emotion_label || unit?.emotion_key || emotionKey).trim();
    if (!emotionMap.has(emotionKey)) emotionMap.set(emotionKey, label);
  });

  const awareness = Array.from(awarenessSet).sort();
  const emotion = Array.from(emotionMap.entries())
    .map(([value, label]) => ({ value, label }))
    .sort((a, b) => a.label.localeCompare(b.label));

  return { awareness, emotion };
}

function phase3V2RenderCurrentRun() {
  const mount = document.getElementById('phase3-v2-results');
  if (!mount) return;

  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object') {
    phase3V2CloseArmExpanded();
    mount.innerHTML = '<div class="empty-state">Run Script Writer to review outputs.</div>';
    return;
  }

  const units = Array.isArray(detail.brief_units) ? detail.brief_units : [];
  const draftsByArm = detail.drafts_by_arm && typeof detail.drafts_by_arm === 'object' ? detail.drafts_by_arm : {};
  const runArmsRaw = Array.isArray(detail.run?.arms) ? detail.run.arms : Object.keys(draftsByArm);
  const runArms = runArmsRaw
    .map(v => String(v || '').trim())
    .filter(Boolean);
  const decisionMap = phase3V2DecisionMap(detail);
  const locked = phase3V2IsLocked(detail);
  const selectionStats = phase3V2SelectionStats(detail);

  const draftMaps = {};
  runArms.forEach((arm) => {
    const rows = Array.isArray(draftsByArm[arm]) ? draftsByArm[arm] : [];
    const map = {};
    rows.forEach((row) => {
      const unitId = String(row?.brief_unit_id || '').trim();
      if (unitId) map[unitId] = row;
    });
    draftMaps[arm] = map;
  });

  if (!units.length || !runArms.length) {
    phase3V2CloseArmExpanded();
    mount.innerHTML = '<div class="empty-state">No Brief Units found for this run.</div>';
    return;
  }

  const filterOptions = phase3V2BuildUnitFilterOptions(units);
  if (
    phase3V2UnitFilters.awareness !== 'all' &&
    !filterOptions.awareness.includes(phase3V2UnitFilters.awareness)
  ) {
    phase3V2UnitFilters.awareness = 'all';
  }
  const emotionValues = filterOptions.emotion.map((o) => o.value);
  if (
    phase3V2UnitFilters.emotion !== 'all' &&
    !emotionValues.includes(phase3V2UnitFilters.emotion)
  ) {
    phase3V2UnitFilters.emotion = 'all';
  }

  const filteredUnits = units.filter((unit) => {
    const awareness = String(unit?.awareness_level || '').trim().toLowerCase();
    const emotion = normalizeEmotionKey(unit?.emotion_key || unit?.emotion_label || '');
    const awarenessMatch = phase3V2UnitFilters.awareness === 'all' || awareness === phase3V2UnitFilters.awareness;
    const emotionMatch = phase3V2UnitFilters.emotion === 'all' || emotion === phase3V2UnitFilters.emotion;
    return awarenessMatch && emotionMatch;
  });

  const armHeaders = runArms
    .map((arm) => `<th>${esc(phase3V2ArmDisplayName(arm))}</th>`)
    .join('');

  const rows = filteredUnits.map((unit) => {
    const unitId = String(unit?.brief_unit_id || '');

    const armCells = runArms.map((arm) => {
      const draft = draftMaps[arm]?.[unitId] || null;
      return `
        <td>
          <div class="phase3-v2-arm-card">
            <div class="phase3-v2-arm-head">
              <span class="phase3-v2-arm-label">${esc(phase3V2ArmDisplayName(arm))}</span>
            </div>
            <div class="phase3-v2-script-snippet">${esc(phase3V2UnitArmSnippet(draft))}</div>
            <div class="phase3-v2-arm-tools">
              <button class="btn btn-ghost btn-sm" onclick="phase3V2OpenArmExpanded('${esc(unitId)}','${esc(arm)}')">Expand</button>
            </div>
          </div>
        </td>
      `;
    }).join('');

    const reviewCell = runArms.map((arm) => {
      const decisionKey = `${unitId}::${arm}`;
      const existingDecision = decisionMap[decisionKey] || '';
      const draft = draftMaps[arm]?.[unitId] || null;
      const autoSkipped = phase3V2IsAutoSkippedDraft(draft);
      const checked = autoSkipped || phase3V2IsManualSkipDecision(existingDecision);
      const label = runArms.length > 1 ? `Skip ${phase3V2ArmDisplayName(arm)}` : 'Skip';
      const autoReason = String(draft?.status || '').toLowerCase();
      const hint = autoSkipped
        ? `Auto-skipped (${autoReason || 'missing'})`
        : 'Exclude this output from next phase';
      return `
        <label class="phase3-v2-approve-toggle ${autoSkipped ? 'auto' : ''}" title="${esc(hint)}">
          <input
            type="checkbox"
            ${checked ? 'checked' : ''}
            ${(locked || autoSkipped) ? 'disabled' : ''}
            onchange="phase3V2ToggleSkip('${esc(unitId)}','${esc(arm)}', this.checked, this)"
          >
          <span>${esc(autoSkipped ? 'Auto-skip' : label)}</span>
        </label>
      `;
    }).join('');

    return `
      <tr>
        <td class="phase3-v2-brief-meta">
          <div class="phase3-v2-brief-id">${esc(unitId)}</div>
          <div>${esc(humanizeAwareness(unit.awareness_level || ''))} × ${esc(unit.emotion_label || unit.emotion_key || '')}</div>
          <div class="muted">${esc(unit.matrix_cell_id || '')}</div>
        </td>
        ${armCells}
        <td class="phase3-v2-approve-cell">${reviewCell}</td>
      </tr>
    `;
  }).join('');

  const awarenessOptionsHtml = filterOptions.awareness
    .map((value) => `<option value="${esc(value)}" ${phase3V2UnitFilters.awareness === value ? 'selected' : ''}>${esc(humanizeAwareness(value))}</option>`)
    .join('');
  const emotionOptionsHtml = filterOptions.emotion
    .map((opt) => `<option value="${esc(opt.value)}" ${phase3V2UnitFilters.emotion === opt.value ? 'selected' : ''}>${esc(opt.label)}</option>`)
    .join('');
  const hasActiveFilter = phase3V2UnitFilters.awareness !== 'all' || phase3V2UnitFilters.emotion !== 'all';

  mount.innerHTML = `
    ${locked ? '<div class="phase3-v2-locked-banner">Final Locked: outputs are read-only.</div>' : ''}
    <div class="phase3-v2-filter-bar">
      <div class="phase3-v2-filter-controls">
        <label class="phase3-v2-filter-field">
          <span>Awareness</span>
          <select class="gate-model-select" onchange="phase3V2SetUnitFilter('awareness', this.value)">
            <option value="all">All awareness levels</option>
            ${awarenessOptionsHtml}
          </select>
        </label>
        <label class="phase3-v2-filter-field">
          <span>Emotion</span>
          <select class="gate-model-select" onchange="phase3V2SetUnitFilter('emotion', this.value)">
            <option value="all">All emotions</option>
            ${emotionOptionsHtml}
          </select>
        </label>
      </div>
      <div class="phase3-v2-filter-meta">
        <span>Showing ${filteredUnits.length} of ${units.length} brief units</span>
        ${hasActiveFilter ? `<button class="btn btn-ghost btn-sm" onclick="phase3V2ClearUnitFilters()">Clear filters</button>` : ''}
      </div>
    </div>
    ${selectionStats.autoSkipped > 0 ? `
      <div class="phase3-v2-skip-warning">
        ${esc(String(selectionStats.autoSkipped))} output${selectionStats.autoSkipped === 1 ? '' : 's'} auto-skipped (blocked/error). You can optionally skip more weak outputs.
      </div>
    ` : ''}
    ${filteredUnits.length ? `
      <table class="phase3-v2-results-table">
        <thead>
          <tr>
            <th>Brief Unit</th>
            ${armHeaders}
            <th>Skip</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    ` : `
      <div class="phase3-v2-filter-empty">No brief units match the current filters.</div>
    `}
  `;
}

async function phase3V2ToggleSkip(briefUnitId, arm, isChecked, inputEl = null) {
  const unitId = String(briefUnitId || '').trim();
  const armName = String(arm || '').trim();
  if (!unitId || !armName || !phase3V2CurrentRunId || !activeBranchId || !phase3V2CurrentRunDetail) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    if (inputEl) inputEl.checked = !isChecked;
    return;
  }

  if (inputEl) inputEl.disabled = true;
  const decision = isChecked ? 'revise' : 'approve';

  try {
    let resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/decisions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        run_id: phase3V2CurrentRunId,
        brand: activeBrandSlug || '',
        reviewer_role: phase3V2ReviewerRoleDefault,
        decisions: [
          {
            brief_unit_id: unitId,
            arm: armName,
            decision,
            reviewer_id: '',
          },
        ],
      }),
    });

    let data = await resp.json();
    const routeMissing = resp.status === 404 && String(data?.detail || '').toLowerCase() === 'not found';
    if (routeMissing) {
      // Backward compatibility: older servers only expose /reviews.
      resp = await fetch(`/api/branches/${activeBranchId}/phase3-v2/reviews`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          run_id: phase3V2CurrentRunId,
          brand: activeBrandSlug || '',
          reviewer_role: phase3V2ReviewerRoleDefault,
          reviews: [
            {
              brief_unit_id: unitId,
              arm: armName,
              reviewer_id: '',
              quality_score_1_10: isChecked ? 10 : 5,
              decision,
              notes: '',
            },
          ],
        }),
      });
      data = await resp.json();
    }

    if (!resp.ok || data.error) {
      throw new Error(data.error || `Failed to save skip setting (HTTP ${resp.status})`);
    }
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
  } catch (e) {
    alert(e.message || 'Failed to save skip setting.');
    if (inputEl) inputEl.checked = !isChecked;
  } finally {
    if (inputEl) inputEl.disabled = false;
  }
}

async function phase3V2FinalLock() {
  if (!phase3V2CurrentRunId || !activeBranchId) return;
  if (phase3V2IsLocked()) return;

  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/final-lock`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          reviewer_role: phase3V2ReviewerRoleDefault,
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Final lock failed (HTTP ${resp.status})`);
    }
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
  } catch (e) {
    alert(e.message || 'Failed to final lock this run.');
  }
}

function phase3V2HooksSetStatus(text, state = '') {
  const el = document.getElementById('phase3-v2-hooks-status');
  if (!el) return;
  el.textContent = text;
  el.classList.remove('running', 'done', 'failed');
  if (state) el.classList.add(state);
}

function phase3V2HooksSetPrepareState(message, state = '') {
  const el = document.getElementById('phase3-v2-hooks-prepare-state');
  if (!el) return;
  el.textContent = message || '';
  el.classList.remove('preparing', 'ready', 'failed');
  if (state) el.classList.add(state);
}

function phase3V2FindHookBundle(detail, briefUnitId, arm) {
  const bundlesByArm = detail?.hook_bundles_by_arm;
  if (!bundlesByArm || typeof bundlesByArm !== 'object') return null;
  const rows = Array.isArray(bundlesByArm[arm]) ? bundlesByArm[arm] : [];
  return rows.find((row) => String(row?.brief_unit_id || '').trim() === briefUnitId) || null;
}

function phase3V2HooksSelectionMap(detail) {
  const map = {};
  const rows = Array.isArray(detail?.hook_selections) ? detail.hook_selections : [];
  rows.forEach((row) => {
    const key = `${String(row?.brief_unit_id || '').trim()}::${String(row?.arm || '').trim()}`;
    if (!key.startsWith('::')) map[key] = row;
  });
  return map;
}

async function phase3V2HooksPrepare(options = {}) {
  const silent = Boolean(options.silent);
  if (!phase3V2HooksEnabled) {
    if (!silent) alert('Hook Generator is disabled on this server.');
    return null;
  }
  if (!activeBranchId || !phase3V2CurrentRunId) {
    if (!silent) alert('Select a script run first.');
    return null;
  }
  const btn = document.getElementById('phase3-v2-hooks-prepare-btn');
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Preparing...';
  }
  phase3V2HooksSetPrepareState('Preparing hook eligibility...', 'preparing');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/hooks/prepare${phase3V2BrandParam()}`
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Prepare hooks failed (HTTP ${resp.status})`);
    }
    phase3V2HooksPrepared = data;
    phase3V2HooksSetPrepareState(
      `Prepared: ${parseInt(data.eligible_count, 10) || 0} eligible, ${parseInt(data.skipped_count, 10) || 0} skipped.`,
      'ready',
    );
    phase3V2RenderHooksSection();
    return data;
  } catch (e) {
    phase3V2HooksPrepared = null;
    phase3V2HooksSetPrepareState(e.message || 'Hook prepare failed.', 'failed');
    if (!silent) alert(e.message || 'Hook prepare failed.');
    return null;
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = 'Prepare Hooks';
    }
  }
}

async function phase3V2HooksRun() {
  if (!phase3V2HooksEnabled) {
    alert('Hook Generator is disabled on this server.');
    return;
  }
  if (!activeBranchId || !phase3V2CurrentRunId) {
    alert('Select a script run first.');
    return;
  }

  const runBtn = document.getElementById('phase3-v2-hooks-run-btn');
  if (runBtn?.disabled) return;
  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = 'Preparing...';
  }

  const prepared = await phase3V2HooksPrepare({ silent: true });
  if (!prepared) {
    if (runBtn) {
      runBtn.disabled = false;
      runBtn.textContent = 'Run Hooks';
    }
    alert('Could not run hooks because prepare failed.');
    return;
  }

  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = 'Starting...';
  }
  phase3V2HooksSetStatus('Running', 'running');
  phase3V2HooksSetPrepareState('Hook generation started...', 'preparing');

  const candidateInput = document.getElementById('phase3-v2-hooks-candidates-input');
  const finalInput = document.getElementById('phase3-v2-hooks-finals-input');
  const candidateTarget = Math.max(1, parseInt(candidateInput?.value || '', 10) || phase3V2HookDefaults.candidatesPerUnit || 20);
  const minNewHooks = phase3V2HookDefaults.minNewVariants || 4;
  const finalVariants = Math.max(minNewHooks, parseInt(finalInput?.value || '', 10) || phase3V2HookDefaults.finalVariantsPerUnit || 5);

  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/hooks/run`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          selected_brief_unit_ids: [],
          candidate_target_per_unit: candidateTarget,
          final_variants_per_unit: finalVariants,
          model_overrides: {},
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Run Hooks failed (HTTP ${resp.status})`);
    }
    phase3V2HooksSetPrepareState(
      `Hook run started (${parseInt(data.eligible_count, 10) || 0} units).`,
      'ready',
    );
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: true, silent: true });
  } catch (e) {
    phase3V2HooksSetStatus('Failed', 'failed');
    phase3V2HooksSetPrepareState(e.message || 'Run Hooks failed.', 'failed');
    alert(e.message || 'Run Hooks failed.');
  } finally {
    if (runBtn) {
      runBtn.disabled = false;
      runBtn.textContent = 'Run Hooks';
    }
  }
}

async function phase3V2HooksSaveSelection(payload) {
  if (!activeBranchId || !phase3V2CurrentRunId) return;
  const resp = await fetch(
    `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/hooks/selections`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: activeBrandSlug || '',
        selections: [payload],
      }),
    },
  );
  const data = await resp.json();
  if (!resp.ok || data.error) {
    throw new Error(data.error || `Failed to save hook selection (HTTP ${resp.status})`);
  }
  return data;
}

async function phase3V2HooksSelectAll() {
  if (!activeBranchId || !phase3V2CurrentRunId) return;
  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object') return;
  if (phase3V2IsLocked(detail)) {
    alert('Run is locked. Hook selections are read-only.');
    return;
  }

  const eligibility = detail.hook_eligibility && typeof detail.hook_eligibility === 'object'
    ? detail.hook_eligibility
    : { eligible: [] };
  const eligibleRows = Array.isArray(eligibility.eligible) ? eligibility.eligible : [];
  if (!eligibleRows.length) {
    alert('No eligible hook units to select.');
    return;
  }

  const selections = [];
  eligibleRows.forEach((row) => {
    const briefUnitId = String(row?.brief_unit_id || '').trim();
    const arm = String(row?.arm || '').trim();
    if (!briefUnitId || !arm) return;
    const bundle = phase3V2FindHookBundle(detail, briefUnitId, arm) || {};
    const variants = Array.isArray(bundle.variants) ? bundle.variants : [];
    const hookIds = [...new Set(
      variants.map((v) => String(v?.hook_id || '').trim()).filter(Boolean)
    )];
    if (!hookIds.length) return;
    selections.push({
      brief_unit_id: briefUnitId,
      arm,
      selected_hook_ids: hookIds,
      selected_hook_id: hookIds[0] || '',
      skip: false,
    });
  });

  if (!selections.length) {
    alert('No hook variants found to select.');
    return;
  }

  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/hooks/selections`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          selections,
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Failed to select all hooks (HTTP ${resp.status})`);
    }
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
  } catch (e) {
    alert(e.message || 'Failed to select all hooks.');
  }
}

async function phase3V2HooksSetSelectedHooks(briefUnitId, arm, hookIds) {
  const normalized = Array.isArray(hookIds)
    ? [...new Set(hookIds.map((v) => String(v || '').trim()).filter(Boolean))]
    : [];
  try {
    await phase3V2HooksSaveSelection({
      brief_unit_id: briefUnitId,
      arm,
      selected_hook_ids: normalized,
      selected_hook_id: normalized[0] || '',
      skip: false,
    });
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
  } catch (e) {
    alert(e.message || 'Failed to save hook selection.');
  }
}

function phase3V2HooksCardToggle(briefUnitId, arm, hookId, isSelected, locked) {
  if (locked) return;
  const selectionMap = phase3V2HooksSelectionMap(phase3V2CurrentRunDetail || {});
  const row = selectionMap[`${briefUnitId}::${arm}`] || {};
  const current = Array.isArray(row.selected_hook_ids)
    ? row.selected_hook_ids.map((v) => String(v || '').trim()).filter(Boolean)
    : [];
  if (!current.length) {
    const legacy = String(row.selected_hook_id || '').trim();
    if (legacy) current.push(legacy);
  }
  const next = isSelected
    ? current.filter((id) => id !== hookId)
    : [...current, hookId];
  phase3V2HooksSetSelectedHooks(briefUnitId, arm, next);
}

function phase3V2RenderHooksSection() {
  const panel = document.getElementById('phase3-v2-hooks-panel');
  const progressEl = document.getElementById('phase3-v2-hooks-progress');
  const resultsEl = document.getElementById('phase3-v2-hooks-results');
  const runBtn = document.getElementById('phase3-v2-hooks-run-btn');
  const selectAllBtn = document.getElementById('phase3-v2-hooks-select-all-btn');
  if (!panel || !progressEl || !resultsEl) return;

  const shouldShow = Boolean(
    isPhase3V2Selected()
    && phase3V2HooksEnabled
    && activeBranchId
    && activeBranchHasPhase2()
  );
  panel.classList.toggle('hidden', !shouldShow);
  if (!shouldShow) return;
  phase3V2ApplyHooksCollapseState();

  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object' || !phase3V2CurrentRunId) {
    phase3V2HooksSetStatus('Idle');
    phase3V2HooksSetPrepareState('Select a run to prepare hooks.');
    progressEl.textContent = '';
    resultsEl.innerHTML = '<div class="empty-state">Select a script run to open Hook Generator.</div>';
    if (runBtn) runBtn.disabled = true;
    if (selectAllBtn) selectAllBtn.disabled = true;
    return;
  }

  const hookStage = detail.hook_stage && typeof detail.hook_stage === 'object' ? detail.hook_stage : {};
  const hookStatus = String(hookStage.status || 'idle').toLowerCase();
  if (hookStatus === 'running') phase3V2HooksSetStatus('Running', 'running');
  else if (hookStatus === 'completed') phase3V2HooksSetStatus('Completed', 'done');
  else if (hookStatus === 'failed') phase3V2HooksSetStatus('Failed', 'failed');
  else phase3V2HooksSetStatus('Idle');

  const hookProgress = detail.hook_selection_progress && typeof detail.hook_selection_progress === 'object'
    ? detail.hook_selection_progress
    : { total_required: 0, selected: 0, skipped: 0, stale: 0, pending: 0, ready: false };
  const locked = phase3V2IsLocked(detail);
  const ready = Boolean(hookProgress.ready);
  const readyBadge = ready ? 'Scene Handoff Ready' : 'Scene Handoff Pending';
  progressEl.textContent = `Selected ${hookProgress.selected || 0}/${hookProgress.total_required || 0} · stale ${hookProgress.stale || 0} · ${readyBadge}`;
  if (runBtn) runBtn.disabled = hookStatus === 'running' || locked;
  if (selectAllBtn) selectAllBtn.disabled = hookStatus === 'running' || locked;

  const eligibility = detail.hook_eligibility && typeof detail.hook_eligibility === 'object'
    ? detail.hook_eligibility
    : { eligible: [], skipped: [] };
  const eligibleRows = Array.isArray(eligibility.eligible) ? eligibility.eligible : [];
  if (!eligibleRows.length) {
    const skipped = parseInt(eligibility.skipped_count, 10) || 0;
    resultsEl.innerHTML = `<div class="phase3-v2-hook-empty">No eligible units for hooks. Skipped: ${skipped}.</div>`;
    if (selectAllBtn) selectAllBtn.disabled = true;
    return;
  }
  if (selectAllBtn) selectAllBtn.disabled = hookStatus === 'running' || locked;

  const selectionMap = phase3V2HooksSelectionMap(detail);
  const html = eligibleRows.map((row) => {
    const briefUnitId = String(row?.brief_unit_id || '').trim();
    const arm = String(row?.arm || '').trim();
    if (!briefUnitId || !arm) return '';
    const bundle = phase3V2FindHookBundle(detail, briefUnitId, arm) || {};
    const variants = Array.isArray(bundle.variants) ? bundle.variants : [];
    const selection = selectionMap[`${briefUnitId}::${arm}`] || null;
    const stale = Boolean(selection?.stale);
    const selectedHookIds = Array.isArray(selection?.selected_hook_ids)
      ? selection.selected_hook_ids.map((v) => String(v || '').trim()).filter(Boolean)
      : [];
    if (!selectedHookIds.length) {
      const legacyId = String(selection?.selected_hook_id || '').trim();
      if (legacyId) selectedHookIds.push(legacyId);
    }
    const title = `${humanizeAwareness(row.awareness_level || '')} × ${row.emotion_label || row.emotion_key || ''}`;

    const cardsHtml = variants.length
      ? variants.map((variant) => {
          const hookId = String(variant?.hook_id || '').trim();
          const isSelected = Boolean(selectedHookIds.includes(hookId) && !stale);
          const gatePass = Boolean(variant?.gate_pass);
          const scrollScore = parseInt(variant?.scroll_stop_score, 10) || 0;
          const specificity = parseInt(variant?.specificity_score, 10) || 0;
          const evidence = Array.isArray(variant?.evidence_ids) ? variant.evidence_ids.map((v) => String(v || '').trim()).filter(Boolean) : [];
          return `
            <div
              class="phase3-v2-hook-card ${isSelected ? 'selected' : ''}"
              role="button"
              tabindex="${locked ? '-1' : '0'}"
              aria-pressed="${isSelected ? 'true' : 'false'}"
              onclick="phase3V2HooksCardToggle('${esc(briefUnitId)}','${esc(arm)}','${esc(hookId)}',${isSelected ? 'true' : 'false'},${locked ? 'true' : 'false'})"
              onkeydown="if ((event.key === 'Enter' || event.key === ' ') && !${locked ? 'true' : 'false'}) { event.preventDefault(); phase3V2HooksCardToggle('${esc(briefUnitId)}','${esc(arm)}','${esc(hookId)}',${isSelected ? 'true' : 'false'},${locked ? 'true' : 'false'}); }"
            >
              <div class="phase3-v2-hook-card-head">
                <div class="phase3-v2-hook-card-title">${esc(hookId || 'Hook')}</div>
                <span class="phase3-v2-hook-chip ${gatePass ? 'pass' : 'fail'}">${gatePass ? 'Gate Pass' : 'Needs Repair'}</span>
              </div>
              <div class="phase3-v2-hook-score-row">
                <span class="phase3-v2-hook-chip">Scroll ${scrollScore}</span>
                <span class="phase3-v2-hook-chip">Specificity ${specificity}</span>
                <span class="phase3-v2-hook-chip">${esc(String(variant?.lane_id || 'lane'))}</span>
              </div>
              <div class="phase3-v2-hook-copy"><strong>Verbal:</strong> ${esc(String(variant?.verbal_open || ''))}</div>
              <div class="phase3-v2-hook-copy"><strong>Visual:</strong> ${esc(String(variant?.visual_pattern_interrupt || ''))}</div>
              ${String(variant?.on_screen_text || '').trim() ? `<div class="phase3-v2-hook-copy"><strong>On-screen:</strong> ${esc(String(variant.on_screen_text || ''))}</div>` : ''}
              <div class="phase3-v2-hook-copy"><strong>Evidence:</strong> ${evidence.length ? esc(evidence.join(', ')) : 'none'}</div>
              <div class="phase3-v2-hook-actions">
                <button
                  class="btn btn-ghost btn-sm"
                  onclick="event.stopPropagation(); phase3V2OpenHookExpanded('${esc(briefUnitId)}','${esc(arm)}','${esc(hookId)}')"
                >
                  Expand
                </button>
                <label class="phase3-v2-hook-select" onclick="event.stopPropagation();" onkeydown="event.stopPropagation();">
                  <input
                    type="checkbox"
                    ${isSelected ? 'checked' : ''}
                    ${locked ? 'disabled' : ''}
                    onclick="event.stopPropagation(); phase3V2HooksCardToggle('${esc(briefUnitId)}','${esc(arm)}','${esc(hookId)}',${isSelected ? 'true' : 'false'},${locked ? 'true' : 'false'});"
                  >
                  <span>${isSelected ? 'Selected' : 'Select'}</span>
                </label>
              </div>
            </div>
          `;
        }).join('')
      : '<div class="phase3-v2-hook-empty">No hook variants yet. Run Hooks for this run.</div>';

    return `
      <div class="phase3-v2-hook-unit">
        <div class="phase3-v2-hook-unit-head">
          <div>
            <div class="phase3-v2-hook-unit-title">${esc(briefUnitId)}</div>
            <div class="phase3-v2-hook-unit-sub">${esc(title)} · ${esc(phase3V2ArmDisplayName(arm))}</div>
          </div>
        </div>
        ${selectedHookIds.length ? `<div class="phase3-v2-hook-stale">Selected hooks: ${selectedHookIds.length}</div>` : ''}
        ${stale ? '<div class="phase3-v2-hook-stale">Script changed after hook selection. Re-select a hook for this unit.</div>' : ''}
        <div class="phase3-v2-hook-grid">${cardsHtml}</div>
      </div>
    `;
  }).join('');

  resultsEl.innerHTML = html || '<div class="phase3-v2-hook-empty">No hook units to render.</div>';
}

function phase3V2ScenesSetStatus(text, state = '') {
  const el = document.getElementById('phase3-v2-scenes-status');
  if (!el) return;
  el.textContent = text;
  el.classList.remove('running', 'done', 'completed', 'failed');
  if (state) el.classList.add(state);
}

function phase3V2ScenesSetPrepareState(message, state = '') {
  const el = document.getElementById('phase3-v2-scenes-prepare-state');
  if (!el) return;
  el.textContent = message || '';
  el.classList.remove('preparing', 'ready', 'failed');
  if (state) el.classList.add(state);
}

function phase3V2FindScenePlan(detail, briefUnitId, arm, hookId) {
  const plansByArm = detail?.scene_plans_by_arm;
  if (!plansByArm || typeof plansByArm !== 'object') return null;
  const rows = Array.isArray(plansByArm[arm]) ? plansByArm[arm] : [];
  return rows.find((row) =>
    String(row?.brief_unit_id || '').trim() === briefUnitId
    && String(row?.hook_id || '').trim() === hookId
  ) || null;
}

function phase3V2FindSceneGate(detail, briefUnitId, arm, hookId) {
  const gatesByArm = detail?.scene_gate_reports_by_arm;
  if (!gatesByArm || typeof gatesByArm !== 'object') return null;
  const rows = Array.isArray(gatesByArm[arm]) ? gatesByArm[arm] : [];
  return rows.find((row) =>
    String(row?.brief_unit_id || '').trim() === briefUnitId
    && String(row?.hook_id || '').trim() === hookId
  ) || null;
}

function phase3V2SceneDirectionSnippet(line) {
  if (!line || typeof line !== 'object') return '';
  const mode = String(line.mode || '').trim().toLowerCase();
  if (mode === 'a_roll') {
    const a = line.a_roll && typeof line.a_roll === 'object' ? line.a_roll : {};
    return [
      a.framing,
      a.creator_action,
      a.performance_direction,
      a.product_interaction,
      a.location,
      line.on_screen_text,
    ]
      .map((v) => String(v || '').trim())
      .filter(Boolean)
      .join(' · ');
  }
  const b = line.b_roll && typeof line.b_roll === 'object' ? line.b_roll : {};
  return [
    b.shot_description,
    b.subject_action,
    b.camera_motion,
    b.props_assets,
    b.transition_intent,
    line.on_screen_text,
  ]
    .map((v) => String(v || '').trim())
    .filter(Boolean)
    .join(' · ');
}

function phase3V2SceneSnippet(lines) {
  if (!Array.isArray(lines) || !lines.length) return 'No scene lines yet.';
  const selected = lines.slice(0, 2).map((line) => {
    const scriptLineId = String(line?.script_line_id || '').trim() || 'line';
    const mode = String(line?.mode || '').trim() || 'mode';
    const text = phase3V2SceneDirectionSnippet(line);
    return `${scriptLineId} [${mode}]: ${text || 'No direction text'}`;
  });
  return selected.join('\n');
}

function phase3V2BuildSceneExpandedItems() {
  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object') return [];

  const briefRows = Array.isArray(detail.brief_units) ? detail.brief_units : [];
  const briefMeta = {};
  briefRows.forEach((row) => {
    const unitId = String(row?.brief_unit_id || '').trim();
    if (!unitId) return;
    briefMeta[unitId] = row;
  });

  const packetItems = Array.isArray(detail.production_handoff_packet?.items)
    ? detail.production_handoff_packet.items
    : [];
  const items = [];

  packetItems.forEach((row) => {
    const briefUnitId = String(row?.brief_unit_id || '').trim();
    const arm = String(row?.arm || '').trim();
    const hookId = String(row?.hook_id || '').trim();
    if (!briefUnitId || !arm || !hookId) return;
    const meta = briefMeta[briefUnitId] || {};
    const persistedPlan = phase3V2FindScenePlan(detail, briefUnitId, arm, hookId);
    const plan = persistedPlan || {
      scene_plan_id: String(row?.scene_plan_id || `sp_${briefUnitId}_${hookId}_${arm}`),
      run_id: phase3V2CurrentRunId,
      brief_unit_id: briefUnitId,
      arm,
      hook_id: hookId,
      lines: Array.isArray(row?.lines) ? row.lines : [],
      total_duration_seconds: 0,
      a_roll_line_count: 0,
      b_roll_line_count: 0,
      max_consecutive_mode: 0,
      status: String(row?.status || 'missing'),
      stale: Boolean(row?.stale),
      stale_reason: String(row?.stale_reason || ''),
      error: '',
      generated_at: '',
    };
    const gate = phase3V2FindSceneGate(detail, briefUnitId, arm, hookId)
      || (row?.gate_report && typeof row.gate_report === 'object' ? row.gate_report : null);
    items.push({
      key: `${briefUnitId}::${arm}::${hookId}`,
      briefUnitId,
      arm,
      hookId,
      awarenessLevel: String(meta?.awareness_level || '').trim(),
      emotionLabel: String(meta?.emotion_label || meta?.emotion_key || '').trim(),
      scenePlan: plan,
      hasPersistedPlan: Boolean(persistedPlan),
      gateReport: gate,
      status: String(row?.status || plan?.status || 'missing'),
      stale: Boolean(row?.stale || plan?.stale),
      staleReason: String(row?.stale_reason || plan?.stale_reason || ''),
    });
  });

  return items;
}

function phase3V2CurrentSceneExpandedItem() {
  if (phase3V2SceneExpandedIndex < 0 || phase3V2SceneExpandedIndex >= phase3V2SceneExpandedItems.length) return null;
  return phase3V2SceneExpandedItems[phase3V2SceneExpandedIndex] || null;
}

async function phase3V2ScenesPrepare(options = {}) {
  const silent = Boolean(options.silent);
  if (!phase3V2ScenesEnabled) {
    if (!silent) alert('Scene Writer is disabled on this server.');
    return null;
  }
  if (!activeBranchId || !phase3V2CurrentRunId) {
    if (!silent) alert('Select a run first.');
    return null;
  }
  phase3V2ScenesSetPrepareState('Preparing scene eligibility...', 'preparing');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/scenes/prepare${phase3V2BrandParam()}`
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Prepare scenes failed (HTTP ${resp.status})`);
    }
    const eligible = parseInt(data.eligible_count, 10) || 0;
    const skipped = parseInt(data.skipped_count, 10) || 0;
    phase3V2ScenesSetPrepareState(`Prepared: ${eligible} eligible, ${skipped} skipped.`, 'ready');
    return data;
  } catch (e) {
    phase3V2ScenesSetPrepareState(e.message || 'Scene prepare failed.', 'failed');
    if (!silent) alert(e.message || 'Scene prepare failed.');
    return null;
  }
}

async function phase3V2ScenesRun() {
  if (!phase3V2ScenesEnabled) {
    alert('Scene Writer is disabled on this server.');
    return;
  }
  if (!activeBranchId || !phase3V2CurrentRunId) {
    alert('Select a script run first.');
    return;
  }
  if (phase3V2IsLocked()) {
    alert('Run is locked. Scene stage is read-only.');
    return;
  }

  const runBtn = document.getElementById('phase3-v2-scenes-run-btn');
  if (runBtn?.disabled) return;
  if (runBtn) {
    runBtn.disabled = true;
    runBtn.textContent = 'Preparing...';
  }

  const prepared = await phase3V2ScenesPrepare({ silent: true });
  if (!prepared) {
    if (runBtn) {
      runBtn.disabled = false;
      runBtn.textContent = 'Run Scenes';
    }
    alert('Could not run scenes because prepare failed.');
    return;
  }
  if ((parseInt(prepared.eligible_count, 10) || 0) <= 0) {
    if (runBtn) {
      runBtn.disabled = false;
      runBtn.textContent = 'Run Scenes';
    }
    alert('No eligible scene units. Select hooks first, then rerun.');
    return;
  }

  if (runBtn) runBtn.textContent = 'Starting...';
  phase3V2ScenesSetStatus('Running', 'running');

  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/scenes/run`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          selected_brief_unit_ids: [],
          model_overrides: {},
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Run Scenes failed (HTTP ${resp.status})`);
    }
    phase3V2ScenesSetPrepareState(`Scene run started (${parseInt(data.eligible_count, 10) || 0} units).`, 'ready');
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: true, silent: true });
  } catch (e) {
    phase3V2ScenesSetStatus('Failed', 'failed');
    phase3V2ScenesSetPrepareState(e.message || 'Run Scenes failed.', 'failed');
    alert(e.message || 'Run Scenes failed.');
  } finally {
    if (runBtn) {
      runBtn.disabled = false;
      runBtn.textContent = 'Run Scenes';
    }
  }
}

function phase3V2RenderScenesSection() {
  const panel = document.getElementById('phase3-v2-scenes-panel');
  const progressEl = document.getElementById('phase3-v2-scenes-progress');
  const resultsEl = document.getElementById('phase3-v2-scenes-results');
  const runBtn = document.getElementById('phase3-v2-scenes-run-btn');
  if (!panel || !progressEl || !resultsEl) return;

  const shouldShow = Boolean(
    isPhase3V2Selected()
    && phase3V2ScenesEnabled
    && activeBranchId
    && activeBranchHasPhase2()
  );
  panel.classList.toggle('hidden', !shouldShow);
  if (!shouldShow) return;
  phase3V2ApplyScenesCollapseState();

  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object' || !phase3V2CurrentRunId) {
    phase3V2ScenesSetStatus('Idle');
    phase3V2ScenesSetPrepareState('Select a run to open Scene Writer.');
    progressEl.textContent = '';
    resultsEl.innerHTML = '<div class="empty-state">Select a run to open Scene Writer.</div>';
    if (runBtn) runBtn.disabled = true;
    return;
  }

  const sceneStage = detail.scene_stage && typeof detail.scene_stage === 'object' ? detail.scene_stage : {};
  const sceneStatus = String(sceneStage.status || 'idle').toLowerCase();
  if (sceneStatus === 'running') phase3V2ScenesSetStatus('Running', 'running');
  else if (sceneStatus === 'completed') phase3V2ScenesSetStatus('Completed', 'done');
  else if (sceneStatus === 'failed') phase3V2ScenesSetStatus('Failed', 'failed');
  else phase3V2ScenesSetStatus('Idle');

  const progress = detail.scene_progress && typeof detail.scene_progress === 'object'
    ? detail.scene_progress
    : { total_required: 0, generated: 0, ready: 0, failed: 0, stale: 0, missing: 0, ready_for_handoff: false };
  const locked = phase3V2IsLocked(detail);
  const handoffReady = Boolean(detail.scene_handoff_ready);
  const readyForHandoff = Boolean(progress.ready_for_handoff);
  const runDisabled = sceneStatus === 'running' || locked || !handoffReady;
  if (runBtn) runBtn.disabled = runDisabled;

  progressEl.textContent = `Generated ${progress.generated || 0}/${progress.total_required || 0} · ready ${progress.ready || 0} · failed ${progress.failed || 0} · stale ${progress.stale || 0} · ${readyForHandoff ? 'Production Handoff Ready' : 'Production Handoff Pending'}`;
  if (!handoffReady) {
    phase3V2ScenesSetPrepareState('Select at least one hook per unit in Hook Generator first.', 'failed');
  } else if (sceneStatus === 'running') {
    phase3V2ScenesSetPrepareState('Generating scene plans...', 'preparing');
  } else if (sceneStatus === 'completed') {
    phase3V2ScenesSetPrepareState('Scene generation completed.', 'ready');
  } else if (sceneStatus === 'failed') {
    phase3V2ScenesSetPrepareState(String(sceneStage.error || 'Scene generation failed.'), 'failed');
  } else {
    phase3V2ScenesSetPrepareState('Scene handoff ready. Click Run Scenes.', 'ready');
  }

  const packet = detail.production_handoff_packet && typeof detail.production_handoff_packet === 'object'
    ? detail.production_handoff_packet
    : { items: [] };
  const items = Array.isArray(packet.items) ? packet.items : [];
  if (!items.length) {
    resultsEl.innerHTML = '<div class="phase3-v2-hook-empty">No scene units yet. Run hooks and select hooks first.</div>';
    return;
  }

  const briefRows = Array.isArray(detail.brief_units) ? detail.brief_units : [];
  const briefMeta = {};
  briefRows.forEach((row) => {
    const unitId = String(row?.brief_unit_id || '').trim();
    if (!unitId) return;
    briefMeta[unitId] = row;
  });

  const html = items.map((row) => {
    const briefUnitId = String(row?.brief_unit_id || '').trim();
    const arm = String(row?.arm || '').trim();
    const hookId = String(row?.hook_id || '').trim();
    if (!briefUnitId || !arm || !hookId) return '';
    const meta = briefMeta[briefUnitId] || {};
    const lines = Array.isArray(row?.lines) ? row.lines : [];
    const status = String(row?.status || 'missing').trim().toLowerCase();
    const statusClass = status === 'ready' ? 'pass' : (status === 'failed' ? 'fail' : '');
    const gate = row?.gate_report && typeof row.gate_report === 'object' ? row.gate_report : null;
    const aRollCount = lines.filter((line) => String(line?.mode || '').trim() === 'a_roll').length;
    const bRollCount = lines.filter((line) => String(line?.mode || '').trim() === 'b_roll').length;
    const title = `${humanizeAwareness(meta?.awareness_level || '')} × ${meta?.emotion_label || meta?.emotion_key || ''}`;

    return `
      <div class="phase3-v2-hook-unit phase3-v2-scene-unit">
        <div class="phase3-v2-hook-unit-head">
          <div>
            <div class="phase3-v2-hook-unit-title">${esc(briefUnitId)}</div>
            <div class="phase3-v2-hook-unit-sub">${esc(title)} · ${esc(phase3V2ArmDisplayName(arm))} · ${esc(hookId)}</div>
          </div>
          <span class="phase3-v2-hook-chip ${statusClass}">${esc(status)}</span>
        </div>
        ${Boolean(row?.stale) ? `<div class="phase3-v2-hook-stale">${esc(String(row?.stale_reason || 'Scene plan is stale. Rerun scenes after upstream edits.'))}</div>` : ''}
        <div class="phase3-v2-hook-score-row">
          <span class="phase3-v2-hook-chip">Lines ${lines.length}</span>
          <span class="phase3-v2-hook-chip">A-Roll ${aRollCount}</span>
          <span class="phase3-v2-hook-chip">B-Roll ${bRollCount}</span>
          <span class="phase3-v2-hook-chip ${gate?.overall_pass ? 'pass' : 'fail'}">${gate?.overall_pass ? 'Gate Pass' : 'Gate Check'}</span>
        </div>
        <div class="phase3-v2-script-snippet">${esc(phase3V2SceneSnippet(lines))}</div>
        <div class="phase3-v2-hook-actions">
          <button class="btn btn-ghost btn-sm" onclick="phase3V2OpenSceneExpanded('${esc(briefUnitId)}','${esc(arm)}','${esc(hookId)}')">Expand</button>
        </div>
      </div>
    `;
  }).join('');

  resultsEl.innerHTML = html || '<div class="phase3-v2-hook-empty">No scene units to render.</div>';
}

function phase3V2BuildSceneEditorLineRow(line = {}, disabled = false) {
  const mode = String(line?.mode || 'a_roll').trim().toLowerCase() === 'b_roll' ? 'b_roll' : 'a_roll';
  const scriptLineId = String(line?.script_line_id || '').trim();
  const onScreen = String(line?.on_screen_text || '').trim();
  const duration = Math.max(0.1, parseFloat(line?.duration_seconds || 2.0) || 2.0);
  const difficulty = Math.max(1, Math.min(10, parseInt(line?.difficulty_1_10, 10) || 5));
  const evidenceText = Array.isArray(line?.evidence_ids)
    ? line.evidence_ids.map((v) => String(v || '').trim()).filter(Boolean).join(', ')
    : '';
  const aRoll = line?.a_roll && typeof line.a_roll === 'object' ? line.a_roll : {};
  const bRoll = line?.b_roll && typeof line.b_roll === 'object' ? line.b_roll : {};
  const lockAttr = disabled ? 'disabled' : '';

  return `
    <div class="p3v2-scene-line-row" draggable="${disabled ? 'false' : 'true'}">
      <div class="p3v2-scene-line-head">
        <button class="p3v2-drag-handle p3v2-scene-drag-handle" type="button" ${lockAttr} title="Drag to reorder">↕</button>
        <span class="p3v2-line-order p3v2-scene-order"></span>
        <label class="p3v2-scene-inline-field">
          <span>Script Line ID</span>
          <input class="p3v2-scene-script-line-id" type="text" value="${esc(scriptLineId)}" placeholder="L01" ${lockAttr}>
        </label>
        <label class="p3v2-scene-inline-field">
          <span>Mode</span>
          <select class="p3v2-scene-mode-select" onchange="phase3V2SceneRowModeChanged(this)" ${lockAttr}>
            <option value="a_roll" ${mode === 'a_roll' ? 'selected' : ''}>A-Roll</option>
            <option value="b_roll" ${mode === 'b_roll' ? 'selected' : ''}>B-Roll</option>
          </select>
        </label>
        <button class="btn btn-ghost btn-sm phase3-v2-line-chat" type="button" onclick="phase3V2AddSceneLineToChat(this)" ${lockAttr}>Ask Claude</button>
        <button class="btn btn-ghost btn-sm phase3-v2-line-remove" type="button" onclick="phase3V2RemoveSceneLine(this)" ${lockAttr}>Remove</button>
      </div>

      <div class="p3v2-scene-meta-grid">
        <label class="phase3-v2-edit-field">
          <span>On-screen Text</span>
          <textarea class="p3v2-scene-onscreen" ${lockAttr}>${esc(onScreen)}</textarea>
        </label>
        <label class="phase3-v2-edit-field">
          <span>Evidence IDs (comma-separated)</span>
          <textarea class="p3v2-scene-evidence" ${lockAttr}>${esc(evidenceText)}</textarea>
        </label>
        <label class="p3v2-scene-inline-field">
          <span>Duration (s)</span>
          <input class="p3v2-scene-duration" type="number" min="0.1" max="30" step="0.1" value="${esc(String(duration))}" ${lockAttr}>
        </label>
        <label class="p3v2-scene-inline-field">
          <span>Difficulty (1-10)</span>
          <input class="p3v2-scene-difficulty" type="number" min="1" max="10" step="1" value="${esc(String(difficulty))}" ${lockAttr}>
        </label>
      </div>

      <div class="p3v2-scene-mode-block p3v2-scene-aroll-block ${mode === 'a_roll' ? '' : 'hidden'}">
        <label class="phase3-v2-edit-field"><span>Framing</span><textarea class="p3v2-scene-a-framing" ${lockAttr}>${esc(String(aRoll.framing || ''))}</textarea></label>
        <label class="phase3-v2-edit-field"><span>Creator Action</span><textarea class="p3v2-scene-a-creator-action" ${lockAttr}>${esc(String(aRoll.creator_action || ''))}</textarea></label>
        <label class="phase3-v2-edit-field"><span>Performance Direction</span><textarea class="p3v2-scene-a-performance-direction" ${lockAttr}>${esc(String(aRoll.performance_direction || ''))}</textarea></label>
        <label class="phase3-v2-edit-field"><span>Product Interaction</span><textarea class="p3v2-scene-a-product-interaction" ${lockAttr}>${esc(String(aRoll.product_interaction || ''))}</textarea></label>
        <label class="phase3-v2-edit-field"><span>Location</span><textarea class="p3v2-scene-a-location" ${lockAttr}>${esc(String(aRoll.location || ''))}</textarea></label>
      </div>

      <div class="p3v2-scene-mode-block p3v2-scene-broll-block ${mode === 'b_roll' ? '' : 'hidden'}">
        <label class="phase3-v2-edit-field"><span>Shot Description</span><textarea class="p3v2-scene-b-shot-description" ${lockAttr}>${esc(String(bRoll.shot_description || ''))}</textarea></label>
        <label class="phase3-v2-edit-field"><span>Subject Action</span><textarea class="p3v2-scene-b-subject-action" ${lockAttr}>${esc(String(bRoll.subject_action || ''))}</textarea></label>
        <label class="phase3-v2-edit-field"><span>Camera Motion</span><textarea class="p3v2-scene-b-camera-motion" ${lockAttr}>${esc(String(bRoll.camera_motion || ''))}</textarea></label>
        <label class="phase3-v2-edit-field"><span>Props / Assets</span><textarea class="p3v2-scene-b-props-assets" ${lockAttr}>${esc(String(bRoll.props_assets || ''))}</textarea></label>
        <label class="phase3-v2-edit-field"><span>Transition Intent</span><textarea class="p3v2-scene-b-transition-intent" ${lockAttr}>${esc(String(bRoll.transition_intent || ''))}</textarea></label>
      </div>
    </div>
  `;
}

function phase3V2SceneRowModeChanged(selectEl) {
  const row = selectEl?.closest('.p3v2-scene-line-row');
  if (!row) return;
  const mode = String(selectEl.value || 'a_roll').trim().toLowerCase() === 'b_roll' ? 'b_roll' : 'a_roll';
  const aBlock = row.querySelector('.p3v2-scene-aroll-block');
  const bBlock = row.querySelector('.p3v2-scene-broll-block');
  if (aBlock) aBlock.classList.toggle('hidden', mode !== 'a_roll');
  if (bBlock) bBlock.classList.toggle('hidden', mode !== 'b_roll');
}

function phase3V2RefreshSceneEditorLineOrderLabels() {
  const rows = Array.from(document.querySelectorAll('#p3v2-scene-lines .p3v2-scene-line-row'));
  rows.forEach((row, idx) => {
    const order = row.querySelector('.p3v2-scene-order');
    if (order) order.textContent = `Line ${idx + 1}`;
  });
}

function phase3V2WireSceneEditorReorder() {
  const container = document.getElementById('p3v2-scene-lines');
  if (!container) return;
  const rows = Array.from(container.querySelectorAll('.p3v2-scene-line-row'));
  rows.forEach((row) => {
    row.addEventListener('dragstart', (event) => {
      const handle = event.target?.closest?.('.p3v2-scene-drag-handle');
      if (!handle || phase3V2IsLocked()) {
        event.preventDefault();
        return;
      }
      phase3V2SceneDraggingRow = row;
      row.classList.add('dragging');
      if (event.dataTransfer) event.dataTransfer.effectAllowed = 'move';
    });
    row.addEventListener('dragend', () => {
      row.classList.remove('dragging');
      phase3V2SceneDraggingRow = null;
      phase3V2RefreshSceneEditorLineOrderLabels();
    });
    row.addEventListener('dragover', (event) => {
      if (!phase3V2SceneDraggingRow || phase3V2SceneDraggingRow === row) return;
      event.preventDefault();
      const rect = row.getBoundingClientRect();
      const before = event.clientY < rect.top + rect.height / 2;
      if (before) container.insertBefore(phase3V2SceneDraggingRow, row);
      else container.insertBefore(phase3V2SceneDraggingRow, row.nextSibling);
    });
  });
}

function phase3V2SetSaveSceneButtonState(state) {
  const btn = document.getElementById('phase3-v2-save-scene-btn');
  if (!btn) return;
  if (state === 'saving') {
    btn.disabled = true;
    btn.textContent = 'Saving...';
    return;
  }
  if (state === 'saved') {
    btn.disabled = false;
    btn.innerHTML = '&#10003; Saved';
    return;
  }
  if (state === 'error') {
    btn.disabled = false;
    btn.textContent = 'Save Failed';
    return;
  }
  btn.disabled = false;
  btn.textContent = 'Save Scene';
}

function phase3V2RenderSceneEditorPane(item) {
  const mount = document.getElementById('phase3-v2-scene-editor-pane');
  if (!mount) return;
  const locked = phase3V2IsLocked();
  const plan = item?.scenePlan;
  if (!plan || typeof plan !== 'object') {
    mount.innerHTML = '<div class="phase3-v2-expanded-alert">No scene plan found for this unit/hook.</div>';
    return;
  }
  if (!item?.hasPersistedPlan) {
    mount.innerHTML = '<div class="phase3-v2-expanded-alert">Run Scenes first to generate this scene plan.</div>';
    return;
  }
  const gate = item?.gateReport && typeof item.gateReport === 'object' ? item.gateReport : {};
  const lines = Array.isArray(plan.lines) ? plan.lines : [];
  const rowsHtml = lines.length
    ? lines.map((line) => phase3V2BuildSceneEditorLineRow(line, locked)).join('')
    : '<div class="phase3-v2-expanded-alert">No scene lines yet. Add one to continue.</div>';
  const gateChip = gate?.overall_pass
    ? '<span class="phase3-v2-hook-chip pass">Gate Pass</span>'
    : '<span class="phase3-v2-hook-chip fail">Needs Repair</span>';

  mount.innerHTML = `
    ${locked ? '<div class="phase3-v2-locked-banner">This run is Final Locked. Editing is disabled.</div>' : ''}
    ${item?.stale ? `<div class="phase3-v2-hook-stale">${esc(String(item?.staleReason || 'Scene plan is stale.'))}</div>` : ''}
    <div class="phase3-v2-hook-score-row">
      <span class="phase3-v2-hook-chip">A-Roll min ${phase3V2SceneDefaults.minARollLines}</span>
      <span class="phase3-v2-hook-chip">Max difficulty ${phase3V2SceneDefaults.maxDifficulty}</span>
      <span class="phase3-v2-hook-chip">Max same-mode run ${phase3V2SceneDefaults.maxConsecutiveMode}</span>
      ${gateChip}
    </div>
    <div id="p3v2-scene-lines" class="p3v2-scene-lines">${rowsHtml}</div>
    <div class="phase3-v2-editor-actions">
      <button class="btn btn-ghost btn-sm" onclick="phase3V2AddSceneLine()" ${locked ? 'disabled' : ''}>Add Scene Line</button>
      <button class="btn btn-primary btn-sm" id="phase3-v2-save-scene-btn" onclick="phase3V2SaveSceneEdits()" ${locked ? 'disabled' : ''}>Save Scene</button>
    </div>
  `;
  phase3V2RefreshSceneEditorLineOrderLabels();
  phase3V2WireSceneEditorReorder();
}

function phase3V2AddSceneLine() {
  if (phase3V2IsLocked()) return;
  const container = document.getElementById('p3v2-scene-lines');
  if (!container) return;
  const wrapper = document.createElement('div');
  wrapper.innerHTML = phase3V2BuildSceneEditorLineRow({}, false);
  const row = wrapper.firstElementChild;
  if (!row) return;
  container.appendChild(row);
  phase3V2RefreshSceneEditorLineOrderLabels();
  phase3V2WireSceneEditorReorder();
}

function phase3V2RemoveSceneLine(btn) {
  if (phase3V2IsLocked()) return;
  const row = btn?.closest?.('.p3v2-scene-line-row');
  if (!row) return;
  const container = document.getElementById('p3v2-scene-lines');
  if (!container) return;
  const rows = container.querySelectorAll('.p3v2-scene-line-row');
  if (rows.length <= 1) {
    alert('At least one scene line is required.');
    return;
  }
  row.remove();
  phase3V2RefreshSceneEditorLineOrderLabels();
}

function phase3V2AddSceneLineToChat(btn) {
  const row = btn?.closest?.('.p3v2-scene-line-row');
  if (!row) return;
  const scriptLineId = String(row.querySelector('.p3v2-scene-script-line-id')?.value || '').trim() || 'line';
  const mode = String(row.querySelector('.p3v2-scene-mode-select')?.value || 'a_roll').trim();
  const evidence = String(row.querySelector('.p3v2-scene-evidence')?.value || '').trim();
  const direction = mode === 'a_roll'
    ? [
        row.querySelector('.p3v2-scene-a-framing')?.value,
        row.querySelector('.p3v2-scene-a-creator-action')?.value,
        row.querySelector('.p3v2-scene-a-performance-direction')?.value,
        row.querySelector('.p3v2-scene-a-product-interaction')?.value,
        row.querySelector('.p3v2-scene-a-location')?.value,
      ]
    : [
        row.querySelector('.p3v2-scene-b-shot-description')?.value,
        row.querySelector('.p3v2-scene-b-subject-action')?.value,
        row.querySelector('.p3v2-scene-b-camera-motion')?.value,
        row.querySelector('.p3v2-scene-b-props-assets')?.value,
        row.querySelector('.p3v2-scene-b-transition-intent')?.value,
      ];
  const directionText = direction.map((v) => String(v || '').trim()).filter(Boolean).join(' | ');
  const payload = `${scriptLineId}\nmode: ${mode}\ndirection: ${directionText}\nevidence: ${evidence || 'none'}`;
  const input = document.getElementById('phase3-v2-scene-chat-input');
  if (!input) return;
  input.value = payload;
  phase3V2SwitchSceneTab('chat');
  input.focus();
  input.setSelectionRange(input.value.length, input.value.length);
}

async function phase3V2SaveSceneEdits() {
  const item = phase3V2SceneExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  if (!item.hasPersistedPlan) {
    alert('Run Scenes first before saving edits for this unit.');
    return;
  }
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }
  const rowEls = Array.from(document.querySelectorAll('#p3v2-scene-lines .p3v2-scene-line-row'));
  const lines = rowEls.map((row) => {
    const scriptLineId = String(row.querySelector('.p3v2-scene-script-line-id')?.value || '').trim();
    const mode = String(row.querySelector('.p3v2-scene-mode-select')?.value || 'a_roll').trim().toLowerCase() === 'b_roll'
      ? 'b_roll'
      : 'a_roll';
    const onScreen = String(row.querySelector('.p3v2-scene-onscreen')?.value || '').trim();
    const duration = Math.max(0.1, Math.min(30, parseFloat(row.querySelector('.p3v2-scene-duration')?.value || '2') || 2));
    const difficulty = Math.max(1, Math.min(10, parseInt(row.querySelector('.p3v2-scene-difficulty')?.value || '5', 10) || 5));
    const evidenceIds = String(row.querySelector('.p3v2-scene-evidence')?.value || '')
      .split(',')
      .map((v) => String(v || '').trim())
      .filter(Boolean);
    const aRoll = {
      framing: String(row.querySelector('.p3v2-scene-a-framing')?.value || '').trim(),
      creator_action: String(row.querySelector('.p3v2-scene-a-creator-action')?.value || '').trim(),
      performance_direction: String(row.querySelector('.p3v2-scene-a-performance-direction')?.value || '').trim(),
      product_interaction: String(row.querySelector('.p3v2-scene-a-product-interaction')?.value || '').trim(),
      location: String(row.querySelector('.p3v2-scene-a-location')?.value || '').trim(),
    };
    const bRoll = {
      shot_description: String(row.querySelector('.p3v2-scene-b-shot-description')?.value || '').trim(),
      subject_action: String(row.querySelector('.p3v2-scene-b-subject-action')?.value || '').trim(),
      camera_motion: String(row.querySelector('.p3v2-scene-b-camera-motion')?.value || '').trim(),
      props_assets: String(row.querySelector('.p3v2-scene-b-props-assets')?.value || '').trim(),
      transition_intent: String(row.querySelector('.p3v2-scene-b-transition-intent')?.value || '').trim(),
    };
    return {
      scene_line_id: '',
      script_line_id: scriptLineId,
      mode,
      a_roll: aRoll,
      b_roll: bRoll,
      on_screen_text: onScreen,
      duration_seconds: duration,
      evidence_ids: evidenceIds,
      difficulty_1_10: difficulty,
    };
  }).filter((row) => Boolean(row.script_line_id));

  if (!lines.length) {
    alert('Add at least one scene line with a script line ID before saving.');
    return;
  }

  phase3V2SetSaveSceneButtonState('saving');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/scenes/update`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          brief_unit_id: item.briefUnitId,
          arm: item.arm,
          hook_id: item.hookId,
          lines,
          source: 'manual',
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(phase3V2ApiErrorMessage(data, `Save failed (HTTP ${resp.status})`));
    }
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
    phase3V2SetSaveSceneButtonState('saved');
    setTimeout(() => phase3V2SetSaveSceneButtonState('idle'), 1600);
    phase3V2SetSceneChatStatus('Scene edits saved.', 'ok');
  } catch (e) {
    phase3V2SetSaveSceneButtonState('error');
    setTimeout(() => phase3V2SetSaveSceneButtonState('idle'), 1600);
    alert(e.message || 'Failed to save scene edits.');
  }
}

function phase3V2SetSceneChatStatus(message = '', state = '') {
  const el = document.getElementById('phase3-v2-scene-chat-status');
  if (!el) return;
  el.textContent = message || '';
  el.classList.remove('ok', 'error', 'pending');
  if (state) el.classList.add(state);
}

function phase3V2RenderSceneChatApplyButton() {
  const btn = document.getElementById('phase3-v2-scene-chat-apply-btn');
  if (!btn) return;
  const hasProposal = Boolean(phase3V2SceneChatPendingPlan);
  const locked = phase3V2IsLocked();
  btn.classList.toggle('hidden', !hasProposal);
  btn.disabled = !hasProposal || locked;
}

function phase3V2RenderSceneChatMessages(messages = []) {
  const mount = document.getElementById('phase3-v2-scene-chat-messages');
  if (!mount) return;
  if (!Array.isArray(messages) || !messages.length) {
    mount.innerHTML = '<div class="phase3-v2-chat-empty">No chat yet. Ask Claude to improve this scene plan.</div>';
    return;
  }
  mount.innerHTML = messages.map((row) => {
    const role = String(row?.role || '').trim().toLowerCase() === 'user' ? 'user' : 'assistant';
    const content = String(row?.content || '').trim();
    const rendered = renderMarkdownLite(content || '(empty)');
    const meta = role === 'assistant'
      ? `${esc(String(row?.provider || ''))} · ${esc(String(row?.model || ''))}`
      : '';
    return `
      <div class="phase3-v2-chat-msg ${role}">
        <div class="phase3-v2-chat-msg-role">${role === 'user' ? 'You' : 'Claude Opus 4.6'}</div>
        <div class="phase3-v2-chat-msg-body">${rendered}</div>
        ${meta ? `<div class="phase3-v2-chat-msg-meta">${meta}</div>` : ''}
      </div>
    `;
  }).join('');
  mount.scrollTop = mount.scrollHeight;
}

function phase3V2SwitchSceneTab(tab) {
  phase3V2SceneTab = tab === 'chat' ? 'chat' : 'scene';
  const sceneBtn = document.getElementById('phase3-v2-scene-tab-editor');
  const chatBtn = document.getElementById('phase3-v2-scene-tab-chat');
  const scenePane = document.getElementById('phase3-v2-scene-editor-pane');
  const chatPane = document.getElementById('phase3-v2-scene-chat-pane');
  if (!sceneBtn || !chatBtn || !scenePane || !chatPane) return;
  const sceneActive = phase3V2SceneTab === 'scene';
  sceneBtn.classList.toggle('active', sceneActive);
  chatBtn.classList.toggle('active', !sceneActive);
  scenePane.classList.toggle('hidden', !sceneActive);
  chatPane.classList.toggle('hidden', sceneActive);
  if (!sceneActive) {
    phase3V2RenderSceneChatApplyButton();
    phase3V2LoadSceneChatThread();
  }
}

function phase3V2HandleSceneChatInputKeydown(event) {
  if (!event || event.key !== 'Enter') return;
  if (event.shiftKey || event.isComposing) return;
  event.preventDefault();
  phase3V2SendSceneChat();
}

function phase3V2RenderSceneExpandedModal() {
  const modal = document.getElementById('phase3-v2-scene-modal');
  const title = document.getElementById('phase3-v2-scene-modal-title');
  const subtitle = document.getElementById('phase3-v2-scene-modal-subtitle');
  if (!modal || !title || !subtitle) return;
  const item = phase3V2CurrentSceneExpandedItem();
  if (!item) {
    phase3V2CloseSceneExpanded();
    return;
  }
  const itemLabel = `${phase3V2SceneExpandedIndex + 1}/${phase3V2SceneExpandedItems.length}`;
  const planId = String(item?.scenePlan?.scene_plan_id || `sp_${item.briefUnitId}_${item.hookId}_${item.arm}`);
  title.textContent = planId;
  subtitle.textContent = `${item.briefUnitId} · ${humanizeAwareness(item.awarenessLevel)} × ${item.emotionLabel} · ${phase3V2ArmDisplayName(item.arm)} · ${item.hookId} · ${itemLabel}`;
  phase3V2SceneExpandedCurrent = item;
  phase3V2SceneChatPendingPlan = null;
  phase3V2RenderSceneEditorPane(item);
  phase3V2RenderSceneChatMessages([]);
  phase3V2SetSceneChatStatus('');
  phase3V2RenderSceneChatApplyButton();
  phase3V2SwitchSceneTab(phase3V2SceneTab || 'scene');
  modal.classList.remove('hidden');
}

function phase3V2OpenSceneExpanded(briefUnitId, arm, hookId) {
  const unitId = String(briefUnitId || '').trim();
  const armKey = String(arm || '').trim();
  const hookVariantId = String(hookId || '').trim();
  if (!unitId || !armKey || !hookVariantId) return;
  phase3V2SceneExpandedItems = phase3V2BuildSceneExpandedItems();
  phase3V2SceneExpandedIndex = phase3V2SceneExpandedItems.findIndex(
    (item) => item.key === `${unitId}::${armKey}::${hookVariantId}`,
  );
  if (phase3V2SceneExpandedIndex < 0) return;
  phase3V2SceneTab = 'scene';
  phase3V2RenderSceneExpandedModal();
}

function phase3V2CloseSceneExpanded() {
  const modal = document.getElementById('phase3-v2-scene-modal');
  if (modal) modal.classList.add('hidden');
  phase3V2SceneExpandedItems = [];
  phase3V2SceneExpandedIndex = -1;
  phase3V2SceneExpandedCurrent = null;
  phase3V2SceneTab = 'scene';
  phase3V2SceneChatPendingPlan = null;
}

function phase3V2PrevSceneExpanded() {
  if (!phase3V2SceneExpandedItems.length) return;
  phase3V2SceneExpandedIndex = (phase3V2SceneExpandedIndex - 1 + phase3V2SceneExpandedItems.length) % phase3V2SceneExpandedItems.length;
  phase3V2RenderSceneExpandedModal();
}

function phase3V2NextSceneExpanded() {
  if (!phase3V2SceneExpandedItems.length) return;
  phase3V2SceneExpandedIndex = (phase3V2SceneExpandedIndex + 1) % phase3V2SceneExpandedItems.length;
  phase3V2RenderSceneExpandedModal();
}

async function phase3V2LoadSceneChatThread() {
  const item = phase3V2SceneExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  const input = document.getElementById('phase3-v2-scene-chat-input');
  const sendBtn = document.getElementById('phase3-v2-scene-chat-send-btn');
  const reloadBtn = document.getElementById('phase3-v2-scene-chat-reload-btn');
  const applyBtn = document.getElementById('phase3-v2-scene-chat-apply-btn');
  const locked = phase3V2IsLocked();
  if (input) input.disabled = locked;
  if (sendBtn) sendBtn.disabled = locked;
  if (reloadBtn) reloadBtn.disabled = false;
  if (applyBtn) applyBtn.disabled = locked || !phase3V2SceneChatPendingPlan;
  phase3V2RenderSceneChatApplyButton();
  phase3V2SetSceneChatStatus(locked ? 'This run is Final Locked. Chat is read-only.' : 'Loading chat...', 'pending');

  try {
    const params = new URLSearchParams();
    params.set('brief_unit_id', item.briefUnitId);
    params.set('arm', item.arm);
    params.set('hook_id', item.hookId);
    if (activeBrandSlug) params.set('brand', activeBrandSlug);
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/scenes/chat?${params.toString()}`
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(phase3V2ApiErrorMessage(data, `Failed to load scene chat (HTTP ${resp.status})`));
    }
    phase3V2RenderSceneChatMessages(Array.isArray(data.messages) ? data.messages : []);
    if (locked) {
      phase3V2SetSceneChatStatus('Final Locked: you can view chat history only.', 'pending');
    } else {
      phase3V2SetSceneChatStatus('');
    }
  } catch (e) {
    phase3V2RenderSceneChatMessages([]);
    phase3V2SetSceneChatStatus(e.message || 'Failed to load scene chat.', 'error');
  }
}

async function phase3V2SendSceneChat() {
  const item = phase3V2SceneExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }
  const input = document.getElementById('phase3-v2-scene-chat-input');
  const sendBtn = document.getElementById('phase3-v2-scene-chat-send-btn');
  if (!input || !sendBtn) return;
  const message = String(input.value || '').trim();
  if (!message) return;

  sendBtn.disabled = true;
  input.disabled = true;
  phase3V2SceneChatPendingPlan = null;
  phase3V2RenderSceneChatApplyButton();
  phase3V2SetSceneChatStatus('Claude is thinking...', 'pending');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/scenes/chat`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          brief_unit_id: item.briefUnitId,
          arm: item.arm,
          hook_id: item.hookId,
          message,
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(phase3V2ApiErrorMessage(data, `Scene chat failed (HTTP ${resp.status})`));
    }
    input.value = '';
    phase3V2RenderSceneChatMessages(Array.isArray(data.messages) ? data.messages : []);
    phase3V2SceneChatPendingPlan = data.has_proposed_scene_plan ? data.proposed_scene_plan : null;
    phase3V2RenderSceneChatApplyButton();
    if (phase3V2SceneChatPendingPlan) {
      phase3V2SetSceneChatStatus('Claude proposed scene changes. Click Apply Claude Changes to update this scene.', 'ok');
    } else {
      phase3V2SetSceneChatStatus('Reply received.', 'ok');
    }
  } catch (e) {
    phase3V2SetSceneChatStatus(e.message || 'Scene chat request failed.', 'error');
  } finally {
    sendBtn.disabled = false;
    input.disabled = false;
    input.focus();
  }
}

async function phase3V2ApplySceneChatChanges() {
  const item = phase3V2SceneExpandedCurrent;
  if (!item || !activeBranchId || !phase3V2CurrentRunId || !phase3V2SceneChatPendingPlan) return;
  if (phase3V2IsLocked()) {
    alert('This run is Final Locked and cannot be changed.');
    return;
  }
  phase3V2RenderSceneChatApplyButton();
  phase3V2SetSceneChatStatus('Applying Claude changes...', 'pending');
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3V2CurrentRunId)}/scenes/chat/apply`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          brief_unit_id: item.briefUnitId,
          arm: item.arm,
          hook_id: item.hookId,
          proposed_scene_plan: phase3V2SceneChatPendingPlan,
        }),
      },
    );
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(phase3V2ApiErrorMessage(data, `Apply failed (HTTP ${resp.status})`));
    }
    phase3V2SceneChatPendingPlan = null;
    phase3V2RenderSceneChatApplyButton();
    phase3V2SetSceneChatStatus('Claude changes applied.', 'ok');
    await phase3V2LoadRunDetail(phase3V2CurrentRunId, { startPolling: false, silent: true });
  } catch (e) {
    phase3V2SetSceneChatStatus(e.message || 'Failed to apply Claude changes.', 'error');
  }
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

function buildBrandCardMarkup(brand, opts = {}) {
  const { showDelete = true, brief = false } = opts;
  const isActive = brand.slug === activeBrandSlug;
  const dateStr = brand.updated_at || brand.created_at || '';
  const availableAgents = normalizeAvailableAgents(brand.available_agents);
  const agentCount = availableAgents.length;

  const metaParts = [];
  if (brand.product_name) metaParts.push(`<span>${esc(brand.product_name)}</span>`);
  if (dateStr) metaParts.push(`<span>${esc(dateStr)}</span>`);
  if (agentCount) metaParts.push(`<span>${agentCount} agent${agentCount !== 1 ? 's' : ''}</span>`);
  if (!metaParts.length) metaParts.push('<span>No output yet</span>');

  const actionButtons = [];
  if (brief) {
    actionButtons.push(`<button class="btn btn-ghost btn-sm" onclick="openBrand('${esc(brand.slug)}')">Open</button>`);
  }
  if (showDelete) {
    actionButtons.push(`<button class="btn btn-ghost btn-sm" onclick="deleteBrand('${esc(brand.slug)}')">Delete</button>`);
  }

  return `
    <div class="brand-card ${brief ? 'brief-brand-card' : ''} ${isActive ? 'active' : ''}" onclick="openBrand('${esc(brand.slug)}')">
      <div class="brand-card-top">
        <span class="brand-card-name">${esc(brand.brand_name || 'Untitled Brand')}</span>
        ${isActive ? '<span class="brand-card-badge active">Active</span>' : ''}
      </div>
      <div class="brand-card-meta">
        ${metaParts.join('')}
      </div>
      ${actionButtons.length ? `<div class="brand-card-actions" onclick="event.stopPropagation()">${actionButtons.join('')}</div>` : ''}
    </div>
  `;
}

function renderBrandSelectorList() {
  const list = document.getElementById('brand-list');
  if (!list) return;
  if (!Array.isArray(brandList) || !brandList.length) {
    list.innerHTML = '<div class="empty-state">No brands yet. Fill out the brief and start a pipeline to create one.</div>';
    return;
  }
  list.innerHTML = brandList.map((b) => buildBrandCardMarkup(b, { showDelete: true, brief: false })).join('');
}

function renderBriefBrandList() {
  const list = document.getElementById('brief-brand-list');
  if (!list) return;
  if (!Array.isArray(brandList) || !brandList.length) {
    list.innerHTML = '<div class="empty-state">No brands yet. Run your first pipeline to create one.</div>';
    return;
  }

  const maxCards = 6;
  const cards = brandList
    .slice(0, maxCards)
    .map((b) => buildBrandCardMarkup(b, { showDelete: false, brief: true }))
    .join('');
  const remaining = Math.max(0, brandList.length - maxCards);
  const more = remaining
    ? `<button class="brief-brand-more" onclick="toggleBrandSelector()">+${remaining} more brands</button>`
    : '';
  list.innerHTML = cards + more;
}

async function loadBrandList() {
  const list = document.getElementById('brand-list');
  if (list) list.innerHTML = '<div class="empty-state">Loading...</div>';
  const briefList = document.getElementById('brief-brand-list');
  if (briefList && (!Array.isArray(brandList) || !brandList.length)) {
    briefList.innerHTML = '<div class="empty-state">Loading brands...</div>';
  }

  try {
    const resp = await fetch('/api/brands');
    const brands = await resp.json();
    brandList = Array.isArray(brands) ? brands : [];
    renderBrandSelectorList();
    renderBriefBrandList();
  } catch (e) {
    if (list) list.innerHTML = '<div class="empty-state">Failed to load brands.</div>';
    if (briefList) briefList.innerHTML = '<div class="empty-state">Failed to load brands.</div>';
    console.error('Failed to load brands', e);
  }
}

async function openBrand(slug) {
  try {
    let brand = null;
    let openResp = null;

    // Preferred path: touch last_opened_at and fetch full brand data
    try {
      openResp = await fetch(`/api/brands/${slug}/open`, { method: 'POST' });
      const openData = await openResp.json();
      if (openResp.ok && !openData.error) {
        brand = openData;
      }
    } catch (e) {
      // Fall through to read-only fallback below
    }

    // Fallback path: still allow opening the brand even if "touch open" failed.
    if (!brand) {
      const fallbackResp = await fetch(`/api/brands/${slug}`);
      const fallbackData = await fallbackResp.json();
      if (!fallbackResp.ok || fallbackData.error) {
        const errMsg = fallbackData.error || `Failed to open brand (HTTP ${fallbackResp.status}).`;
        alert(errMsg);
        return;
      }
      brand = fallbackData;
    }

    if (!brand || typeof brand !== 'object') {
      alert('Failed to open brand: empty response.');
      return;
    }

    activeBrandSlug = slug;
    renderBrandSelectorList();
    renderBriefBrandList();

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
    branches = Array.isArray(brand.branches)
      ? brand.branches.filter(b => b && typeof b === 'object')
      : [];
    activeBranchId = branches.length > 0 ? branches[0].id : null;
    phase3V2ResetStateForBranch();
    renderBranchTabs();
    updateBranchManagerVisibility();

    // If a branch is active, restore its Phase 2+ card states
    if (activeBranchId) {
      const activeBranch = branches.find(b => b.id === activeBranchId);
      applyBranchAgentStates(activeBranch);
    }

    updateProgress();
    updatePhaseStartButtons();

    // Update pipeline header
    const titleEl = document.getElementById('pipeline-title');
    if (titleEl) titleEl.textContent = brand.brand_name || 'Pipeline';
    const subtitleEl = document.getElementById('pipeline-subtitle');
    if (subtitleEl) subtitleEl.textContent = '';

    // Navigate to pipeline view if there are outputs
    if (availableAgents.length > 0) {
      goToView('pipeline');
    } else {
      goToView('brief');
    }
  } catch (e) {
    console.error('Failed to open brand', e);
    alert(`Failed to open brand: ${e?.message || 'unknown error'}`);
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
      phase3V2ResetStateForBranch();
      renderBranchTabs();
    }

    loadBrandList();
  } catch (e) {
    console.error('Delete brand failed', e);
  }
}

// Handle Enter key in rename dialog and new branch modal
document.addEventListener('keydown', (e) => {
  const phase3SceneModal = document.getElementById('phase3-v2-scene-modal');
  if (phase3SceneModal && !phase3SceneModal.classList.contains('hidden')) {
    if (e.key === 'Escape') { phase3V2CloseSceneExpanded(); return; }
    if (e.key === 'ArrowLeft') { e.preventDefault(); phase3V2PrevSceneExpanded(); return; }
    if (e.key === 'ArrowRight') { e.preventDefault(); phase3V2NextSceneExpanded(); return; }
    return;
  }

  const phase3HookModal = document.getElementById('phase3-v2-hook-modal');
  if (phase3HookModal && !phase3HookModal.classList.contains('hidden')) {
    if (e.key === 'Escape') { phase3V2CloseHookExpanded(); return; }
    if (e.key === 'ArrowLeft') { e.preventDefault(); phase3V2PrevHookExpanded(); return; }
    if (e.key === 'ArrowRight') { e.preventDefault(); phase3V2NextHookExpanded(); return; }
    return;
  }

  const phase3ArmModal = document.getElementById('phase3-v2-arm-modal');
  if (phase3ArmModal && !phase3ArmModal.classList.contains('hidden')) {
    if (e.key === 'Escape') { phase3V2CloseArmExpanded(); return; }
    if (e.key === 'ArrowLeft') { e.preventDefault(); phase3V2PrevArmExpanded(); return; }
    if (e.key === 'ArrowRight') { e.preventDefault(); phase3V2NextArmExpanded(); return; }
    return;
  }

  const fullscreenModal = document.getElementById('output-fullscreen-modal');
  if (fullscreenModal && !fullscreenModal.classList.contains('hidden')) {
    if (e.key === 'Escape') {
      closeOutputFullscreenModal();
    }
    return;
  }

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
    phase2MatrixOnlyMode = Boolean(data.phase2_matrix_only_mode);
    phase3Disabled = Boolean(data.phase3_disabled);
    phase3V2Enabled = Boolean(data.phase3_v2_enabled);
    phase3V2HooksEnabled = Boolean(data.phase3_v2_hooks_enabled);
    phase3V2ScenesEnabled = Boolean(data.phase3_v2_scenes_enabled);
    phase3V2ReviewerRoleDefault = String(data.phase3_v2_reviewer_role_default || 'client_founder').trim() || 'client_founder';
    phase3V2HookDefaults = {
      candidatesPerUnit: parseInt(data.phase3_v2_hook_candidates_per_unit, 10) || 20,
      finalVariantsPerUnit: parseInt(data.phase3_v2_hook_final_variants_per_unit, 10) || 5,
      minNewVariants: parseInt(data.phase3_v2_hook_min_new_variants, 10) || 4,
      maxParallel: parseInt(data.phase3_v2_hook_max_parallel, 10) || 4,
      maxRepairRounds: parseInt(data.phase3_v2_hook_max_repair_rounds, 10) || 1,
    };
    phase3V2SceneDefaults = {
      maxParallel: parseInt(data.phase3_v2_scene_max_parallel, 10) || 4,
      maxRepairRounds: parseInt(data.phase3_v2_scene_max_repair_rounds, 10) || 1,
      maxDifficulty: parseInt(data.phase3_v2_scene_max_difficulty, 10) || 8,
      maxConsecutiveMode: parseInt(data.phase3_v2_scene_max_consecutive_mode, 10) || 3,
      minARollLines: parseInt(data.phase3_v2_scene_min_a_roll_lines, 10) || 1,
    };
    const sdkDefaults = data.phase3_v2_sdk_toggles_default;
    phase3V2SdkTogglesDefault = (sdkDefaults && typeof sdkDefaults === 'object')
      ? {
          core_script_drafter: Boolean(sdkDefaults.core_script_drafter),
          hook_generator: Boolean(sdkDefaults.hook_generator),
          scene_planner: Boolean(sdkDefaults.scene_planner),
          targeted_repair: Boolean(sdkDefaults.targeted_repair),
        }
      : { core_script_drafter: false };

    // Remove any existing warning
    const existing = document.getElementById('env-warning');
    if (existing) existing.remove();

    const candidateInput = document.getElementById('phase3-v2-hooks-candidates-input');
    const finalInput = document.getElementById('phase3-v2-hooks-finals-input');
    if (candidateInput) candidateInput.value = String(phase3V2HookDefaults.candidatesPerUnit);
    if (finalInput) {
      finalInput.value = String(Math.max(phase3V2HookDefaults.minNewVariants || 4, phase3V2HookDefaults.finalVariantsPerUnit));
      finalInput.min = String(phase3V2HookDefaults.minNewVariants || 4);
    }

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
    updatePhaseStartButtons();
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
    brandList = Array.isArray(brands) ? brands : [];
    renderBriefBrandList();
    renderBrandSelectorList();
    if (brands.length > 0) {
      const targetSlug = preferredSlug || brands[0].slug; // /api/brands sorted by last_opened_at DESC
      activeBrandSlug = targetSlug;
      renderBriefBrandList();
      renderBrandSelectorList();

      // Load the brand's brief into the form.
      // If touch-open fails, fall back to a read-only brand fetch.
      let brandData = null;
      try {
        const brandResp = await fetch(`/api/brands/${targetSlug}/open`, { method: 'POST' });
        const maybeBrand = await brandResp.json();
        if (brandResp.ok && !maybeBrand.error) {
          brandData = maybeBrand;
        }
      } catch (e) {
        // Fall through to GET fallback
      }
      if (!brandData) {
        const fallbackResp = await fetch(`/api/brands/${targetSlug}`);
        const fallbackData = await fallbackResp.json();
        if (fallbackResp.ok && !fallbackData.error) {
          brandData = fallbackData;
        } else {
          throw new Error(fallbackData.error || `Failed to open brand ${targetSlug}`);
        }
      }
      if (brandData.brief) {
        populateForm(brandData.brief);
      }

      // Restore Phase 1 card states
      const availableAgents = normalizeAvailableAgents(brandData.available_agents);
      if (!pipelineRunning && availableAgents.length) {
        availableAgents.forEach(agentSlug => {
          if (agentSlug === 'foundation_research') {
            setCardState(agentSlug, 'done');
          }
        });
      }

      // Keep branch state shape consistent for downstream UI code.
      branches = Array.isArray(brandData.branches)
        ? brandData.branches.filter(b => b && typeof b === 'object')
        : [];
      activeBranchId = branches.length > 0 ? branches[0].id : null;
      phase3V2ResetStateForBranch();
    }
  } catch (e) {
    console.error('Failed to init brand', e);
    renderBriefBrandList();
  }

  // Now load branches (needs activeBrandSlug set first)
  await loadBranches();
  updatePhaseStartButtons();
}

initBrand();
