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
let liveLogAutoFollow = true;

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
  candidatesPerUnit: 10,
  finalVariantsPerUnit: 5,
  maxParallel: 4,
  maxRepairRounds: 1,
};
let phase3V2SceneDefaults = {
  maxParallel: 4,
  maxRepairRounds: 1,
  maxConsecutiveMode: 3,
  minARollLines: 1,
  enableBeatSplit: true,
  beatTargetWordsMin: 10,
  beatTargetWordsMax: 18,
  beatHardMaxWords: 24,
  maxBeatsPerLine: 3,
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
let phase3V2SceneNarrationIndex = {};
let phase4V1Enabled = false;
let phase4VoicePresets = [];
let phase4RunsCache = [];
let phase4CurrentRunId = '';
let phase4CurrentRunDetail = null;
let phase4Collapsed = true;
let phase4PollTimer = null;
let phase4PollRunId = '';
let phase4PollInFlight = false;
let phase4PollGraceUntilMs = 0;
const PHASE4_GUIDE_CHECKED_STORAGE_KEY = 'phase4_v1_checked_files';
let phase4GuideCheckedByRun = {};
let phase4GuideCheckedLoaded = false;
const OUTPUT_FULLSCREEN_CONTEXT_AGENT = 'agent-output';
const OUTPUT_FULLSCREEN_CONTEXT_HOOK_SCRIPT = 'hook-script';
const OUTPUT_FULLSCREEN_CONTEXT_PHASE4_GUIDE = 'phase4-guide';

let storyboardVideoRunId = '';
let storyboardLastInitKey = '';
let storyboardAssignmentStatus = null;
let storyboardRunDetail = null;
let storyboardProfiles = [];
let storyboardBrollFiles = [];
let storyboardActivityLog = [];
let storyboardActivityDrawerOpen = false;
let storyboardStatusPollTimer = null;
let storyboardActivitySource = 'activity';
let storyboardActivityAutoFollowBySource = { activity: true, live_server: true };
let storyboardSavedVersions = [];
let storyboardSelectedVersionId = 'live';
let storyboardSceneDescriptionMap = {};
let storyboardSceneDescriptionRunId = '';
let storyboardSceneDescriptionFetchPromise = null;
let storyboardBrollExpandedOpen = false;
let storyboardBrollExpandedSearch = '';
let storyboardBrollExpandedFilter = 'all';

const PHASE3_V2_HOOK_THEME_PALETTE = [
  { accent: '#0ea5e9', border: '#0369a1', soft: 'rgba(14, 165, 233, 0.24)', wash: 'rgba(14, 165, 233, 0.15)', chip: 'rgba(14, 165, 233, 0.18)' },
  { accent: '#0ea5a9', border: '#0f766e', soft: 'rgba(6, 182, 212, 0.24)', wash: 'rgba(6, 182, 212, 0.15)', chip: 'rgba(6, 182, 212, 0.18)' },
  { accent: '#84cc16', border: '#4d7c0f', soft: 'rgba(132, 204, 22, 0.24)', wash: 'rgba(132, 204, 22, 0.15)', chip: 'rgba(132, 204, 22, 0.18)' },
  { accent: '#f59e0b', border: '#b45309', soft: 'rgba(245, 158, 11, 0.24)', wash: 'rgba(245, 158, 11, 0.15)', chip: 'rgba(245, 158, 11, 0.18)' },
  { accent: '#a855f7', border: '#7e22ce', soft: 'rgba(168, 85, 247, 0.24)', wash: 'rgba(168, 85, 247, 0.15)', chip: 'rgba(168, 85, 247, 0.18)' },
];

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
  const steps = ['brief', 'pipeline', 'storyboard'];
  const stepperTarget = name === 'results' ? 'storyboard' : name;
  const idx = steps.indexOf(stepperTarget);
  document.querySelectorAll('.stepper .step').forEach((s, i) => {
    s.classList.remove('active', 'done');
    if (idx >= 0 && i < idx) s.classList.add('done');
    if (idx >= 0 && i === idx) s.classList.add('active');
  });
  document.querySelectorAll('.stepper .step-line').forEach((l, i) => {
    l.classList.toggle('done', idx >= 0 && i < idx);
  });

  // If going to results, load them
  if (name === 'results') loadResults();
  if (name === 'storyboard') storyboardEnsureInitialized();
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
      // Only apply transient run-state completions while a run is active.
      // Idle card state should come from brand/branch persistence.
      if (msg.running || pausedAtGate) {
        (msg.completed_agents || []).forEach(slug => setCardState(slug, 'done'));
      }
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
      if (msg.cost) {
        updateCost(msg.cost);
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
        ['creative_engine', 'copywriter', 'hook_specialist'].forEach(slug => setCardState(slug, 'waiting'));
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
      if (msg.cost) updateCost(msg.cost);
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
      const phase4Panel = document.getElementById('phase4-v1-panel');
      if (v2Panel) v2Panel.classList.add('hidden');
      if (hooksPanel) hooksPanel.classList.add('hidden');
      if (scenesPanel) scenesPanel.classList.add('hidden');
      if (phase4Panel) phase4Panel.classList.add('hidden');
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
      if (msg.cost) updateCost(msg.cost);
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
      if (msg.cost) updateCost(msg.cost);
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
    pillar_2_segment_alignment: 'Pillar 2 Segment Alignment',
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
  ensureLiveTerminalScrollTracking();
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

function liveTerminalIsNearBottom(box) {
  if (!box) return true;
  const distance = box.scrollHeight - box.scrollTop - box.clientHeight;
  return distance <= 48;
}

function ensureLiveTerminalScrollTracking() {
  const box = document.getElementById('terminal-output');
  if (!box || box.dataset.scrollTracking === '1') return;
  box.dataset.scrollTracking = '1';
  liveLogAutoFollow = liveTerminalIsNearBottom(box);
  box.addEventListener('scroll', () => {
    liveLogAutoFollow = liveTerminalIsNearBottom(box);
  });
}

function appendServerLogLines(lines) {
  const box = document.getElementById('terminal-output');
  if (!box) return;
  ensureLiveTerminalScrollTracking();
  const shouldFollow = liveLogAutoFollow || liveTerminalIsNearBottom(box);
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
  // Trim old lines (keep last 500)
  while (box.children.length > 500) {
    box.removeChild(box.firstChild);
  }
  if (shouldFollow) {
    box.scrollTop = box.scrollHeight;
    liveLogAutoFollow = true;
  }
  if (storyboardActivitySource === 'live_server') {
    storyboardRenderActivityLog();
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
  liveLogAutoFollow = true;
  if (storyboardActivitySource === 'live_server') {
    storyboardRenderActivityLog();
  }
}

function startStatusPolling() {
  if (statusPollTimer) return;
  statusPollTimer = setInterval(async () => {
    try {
      const brandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
      const resp = await fetch(`/api/status${brandParam}`);
      if (!resp.ok) return;
      const data = await resp.json();
      if ((data.server_log_tail || []).length) {
        appendServerLogLines(data.server_log_tail);
      }
      if (data.active_brand_slug && !activeBrandSlug) {
        activeBrandSlug = data.active_brand_slug;
      }
      if (data.cost) {
        updateCost(data.cost);
      }
    } catch (e) {
      // Ignore transient polling errors.
    }
  }, 2000);
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
    const selectedAudience = String(activeBranch?.inputs?.selected_audience_segment || '').trim();
    body.inputs.matrix_cells = matrixCells;
    body.inputs.selected_audience_segment = selectedAudience;
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

async function rebuildFoundationPillar6(triggerEl) {
  if (pipelineRunning) {
    alert('Stop the active pipeline run before rebuilding Pillar 6.');
    return;
  }
  if (!activeBrandSlug) {
    alert('Open a brand first.');
    return;
  }

  const btn = triggerEl instanceof HTMLElement ? triggerEl : null;
  const originalText = btn ? btn.textContent : '';
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Rebuilding...';
  }

  try {
    const brandParam = `?brand=${encodeURIComponent(activeBrandSlug)}`;
    const resp = await fetch(`/api/foundation/pillar6/rebuild${brandParam}`, {
      method: 'POST',
    });
    const data = await resp.json();
    if (!resp.ok || data.error) {
      throw new Error(data.error || `Failed to rebuild Pillar 6 (HTTP ${resp.status})`);
    }

    delete cardPreviewCache.foundation_research;
    await handleFoundationQualityGateVisibility(data.quality_gate_report || null);

    appendServerLog({
      time: ts(),
      level: 'success',
      message: `Pillar 6 rebuilt in-place: ${data.emotion_count_after || 0} raw emotions, ${data.lf8_rows_total || 0} LF8 rows across ${data.lf8_segments_count || 0} segments (${data.changed || data.lf8_changed ? 'updated' : 'unchanged'}).`,
    });

    const foundationCard = document.getElementById('card-foundation_research');
    const foundationPreview = document.getElementById('preview-foundation_research');
    const previewOpen = Boolean(
      foundationCard &&
      foundationPreview &&
      foundationCard.classList.contains('expanded') &&
      !foundationPreview.classList.contains('hidden')
    );
    if (previewOpen) {
      foundationPreview.classList.add('hidden');
      foundationCard.classList.remove('expanded');
      await toggleCardPreviewBranchAware('foundation_research');
    }

    const modal = document.getElementById('new-branch-modal');
    const audienceSelect = document.getElementById('nb-audience-select');
    const modalOpen = Boolean(modal && !modal.classList.contains('hidden'));
    const selectedAudience = String(audienceSelect?.value || '').trim();
    if (modalOpen && selectedAudience) {
      await reloadNewBranchMatrixForAudience(selectedAudience, {
        forceRefresh: true,
        existingMap: {},
      });
    }
  } catch (e) {
    const msg = formatFetchError(e, 'pillar 6 rebuild');
    appendServerLog({ time: ts(), level: 'error', message: `Pillar 6 rebuild failed: ${msg}` });
    alert(`Failed to rebuild Pillar 6: ${msg}`);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = originalText || 'Rebuild Pillar 6 Only';
    }
  }
}

// -----------------------------------------------------------
// COST TRACKER
// -----------------------------------------------------------

function updateCost(costData) {
  if (!costData) return;
  const el = document.getElementById('header-cost');
  if (!el) return;
  const rawCost = Number(costData.total_cost ?? costData.total_cost_usd ?? 0);
  const cost = Number.isFinite(rawCost) ? Math.max(0, rawCost) : 0;
  if (cost <= 0) {
    el.textContent = '$0.00';
    el.classList.remove('has-cost');
    return;
  }
  el.textContent = cost >= 0.01 ? `$${cost.toFixed(2)}` : `$${cost.toFixed(4)}`;
  el.classList.add('has-cost');
  saveCostSnapshot(cost);
}

function costSnapshotStorageKey() {
  return `creative-maker:cost:${activeBrandSlug || 'global'}`;
}

function saveCostSnapshot(totalCost) {
  const cost = Number(totalCost);
  if (!Number.isFinite(cost) || cost <= 0) return;
  try {
    localStorage.setItem(costSnapshotStorageKey(), JSON.stringify({
      total_cost: cost,
      saved_at: Date.now(),
    }));
  } catch (_) {
    // Ignore localStorage failures.
  }
}

function restoreSavedCostSnapshot() {
  try {
    const raw = localStorage.getItem(costSnapshotStorageKey());
    if (!raw) return false;
    const parsed = JSON.parse(raw);
    const cost = Number(parsed?.total_cost ?? 0);
    if (!Number.isFinite(cost) || cost <= 0) return false;
    updateCost({ total_cost: cost });
    return true;
  } catch (_) {
    return false;
  }
}

async function refreshCostTracker() {
  try {
    const brandParam = activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
    const resp = await fetch(`/api/status${brandParam}`);
    if (!resp.ok) {
      const restored = restoreSavedCostSnapshot();
      if (!restored) resetCost();
      return restored;
    }
    const data = await resp.json();
    if (data.cost) {
      updateCost(data.cost);
      return true;
    }
    const restored = restoreSavedCostSnapshot();
    if (!restored) resetCost();
    return restored;
  } catch (_) {
    const restored = restoreSavedCostSnapshot();
    if (!restored) resetCost();
    return restored;
  }
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

function setOutputFullscreenContext(context = '') {
  const modal = document.getElementById('output-fullscreen-modal');
  const body = document.getElementById('output-fullscreen-body');
  const clean = String(context || '').trim();
  if (modal) {
    if (clean) modal.dataset.fullscreenContext = clean;
    else delete modal.dataset.fullscreenContext;
  }
  if (body) {
    if (clean) body.dataset.fullscreenContext = clean;
    else delete body.dataset.fullscreenContext;
  }
}

function getOutputFullscreenContext() {
  const modal = document.getElementById('output-fullscreen-modal');
  if (!modal) return '';
  return String(modal.dataset.fullscreenContext || '').trim();
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
  setOutputFullscreenContext(OUTPUT_FULLSCREEN_CONTEXT_AGENT);
  modal.classList.remove('hidden');
}

function closeOutputFullscreenModal() {
  const modal = document.getElementById('output-fullscreen-modal');
  const body = document.getElementById('output-fullscreen-body');
  if (modal) modal.classList.add('hidden');
  if (body) body.innerHTML = '';
  setOutputFullscreenContext('');
}

function openOutputFullscreenTextModal(titleText = 'Output', text = '') {
  const modal = document.getElementById('output-fullscreen-modal');
  const body = document.getElementById('output-fullscreen-body');
  const title = document.getElementById('output-fullscreen-title');
  if (!modal || !body || !title) return;

  const safeText = String(text || '').trim() || 'No content.';
  title.textContent = String(titleText || 'Output');
  body.innerHTML = `<pre class="phase3-v2-script-full-text">${esc(safeText)}</pre>`;
  setOutputFullscreenContext(OUTPUT_FULLSCREEN_CONTEXT_HOOK_SCRIPT);
  modal.classList.remove('hidden');
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

function openPrintWindowForPdf(docTitle, bodyHtml) {
  const printStyles = `
    :root { color-scheme: light; }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 20mm 18mm;
      font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: #0f172a;
      background: #ffffff;
      line-height: 1.45;
      font-size: 12px;
    }
    .pdf-shell { max-width: 900px; margin: 0 auto; }
    .pdf-title { margin: 0 0 8px; font-size: 22px; line-height: 1.2; font-weight: 800; color: #0b1220; }
    .pdf-stamp { margin: 0 0 16px; color: #475569; font-size: 11px; }
    .out-section { margin-bottom: 14px; break-inside: avoid; }
    .out-heading, .out-subheading { margin: 0 0 8px; font-weight: 700; color: #0b1220; }
    .out-heading { font-size: 15px; }
    .out-subheading { font-size: 13px; }
    .out-list { margin: 0; padding-left: 18px; }
    .out-list-item { margin: 0 0 7px; }
    .out-field { margin: 0 0 6px; }
    .out-field-key { display: inline-block; min-width: 170px; color: #334155; font-weight: 600; margin-right: 8px; vertical-align: top; }
    .out-field-val { color: #0f172a; }
    .out-card, .phase1-collector-report-block, .phase1-quality-check {
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      background: #ffffff;
      padding: 10px;
      margin: 0 0 10px;
      break-inside: avoid;
    }
    .phase1-collector-report-meta {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      margin: 0 0 6px;
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: #475569;
    }
    .phase1-collector-report-text { white-space: pre-wrap; word-break: break-word; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 11px; color: #0f172a; }
    .out-badge { border: 1px solid #cbd5e1; border-radius: 999px; padding: 1px 8px; font-size: 10px; color: #334155; }
    .out-badge.red { border-color: #fecaca; color: #991b1b; }
    .out-badge.green { border-color: #bbf7d0; color: #166534; }
    .out-badge.purple { border-color: #cbd5e1; color: #334155; }
    .out-md h1, .out-md h2, .out-md h3, .out-md h4 { color: #0b1220; margin: 10px 0 7px; }
    .out-md h1 { font-size: 22px; }
    .out-md h2 { font-size: 18px; }
    .out-md h3 { font-size: 15px; }
    .out-md h4 { font-size: 13px; }
    .out-md p { margin: 0 0 8px; color: #0f172a; }
    .out-md ul, .out-md ol { margin: 0 0 8px 18px; color: #0f172a; }
    .out-md code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; background: #e2e8f0; border-radius: 4px; padding: 1px 4px; }
    .phase1-quality-overview { display: flex; flex-wrap: wrap; gap: 10px; margin: 6px 0 10px; color: #334155; }
    .phase1-quality-check-line { margin: 0 0 6px; color: #0f172a; }
    .phase1-quality-check-grid { display: block; }
    @media print {
      body { padding: 0; }
      .pdf-shell { max-width: 100%; }
    }
  `;

  const safeTitle = esc(String(docTitle || 'Foundation Research Export'));
  const safeTimestamp = esc(new Date().toLocaleString());
  const html = `
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <title>${safeTitle}</title>
        <style>${printStyles}</style>
      </head>
      <body>
        <div class="pdf-shell">
          <h1 class="pdf-title">${safeTitle}</h1>
          <p class="pdf-stamp">Exported ${safeTimestamp}</p>
          ${bodyHtml}
        </div>
      </body>
    </html>
  `;

  const popup = window.open('', '_blank', 'noopener,noreferrer');
  if (!popup) {
    // Fallback path when popup blockers reject window.open.
    printHtmlViaHiddenIframe(html);
    return;
  }

  popup.document.open();
  popup.document.write(html);
  popup.document.close();
  popup.focus();

  const doPrint = () => {
    try {
      popup.print();
    } catch (_) {}
  };
  if (popup.document.readyState === 'complete') {
    setTimeout(doPrint, 80);
  } else {
    popup.onload = () => setTimeout(doPrint, 80);
  }
}

function printHtmlViaHiddenIframe(html) {
  const iframeId = 'foundation-pdf-print-frame';
  let frame = document.getElementById(iframeId);
  if (!frame) {
    frame = document.createElement('iframe');
    frame.id = iframeId;
    frame.style.position = 'fixed';
    frame.style.width = '0';
    frame.style.height = '0';
    frame.style.right = '0';
    frame.style.bottom = '0';
    frame.style.border = '0';
    frame.style.opacity = '0';
    frame.setAttribute('aria-hidden', 'true');
    document.body.appendChild(frame);
  }

  const doc = frame.contentDocument || frame.contentWindow?.document;
  if (!doc || !frame.contentWindow) {
    alert('Unable to open print dialog on this browser.');
    return;
  }

  doc.open();
  doc.write(html);
  doc.close();

  const runPrint = () => {
    try {
      frame.contentWindow.focus();
      frame.contentWindow.print();
    } catch (_) {
      alert('Unable to open print dialog on this browser.');
    }
  };

  if (doc.readyState === 'complete') {
    setTimeout(runPrint, 80);
  } else {
    frame.onload = () => setTimeout(runPrint, 80);
  }
}

function exportFoundationOutputPdf(kind, triggerEl = null) {
  const root = (triggerEl && triggerEl.closest('.foundation-output-switcher'))
    || document.getElementById('foundation-output-switcher');
  if (!root) return;

  const brand = String(document.getElementById('f-brand')?.value || '').trim();
  const product = String(document.getElementById('f-product')?.value || '').trim();
  const nameBits = [brand, product].filter(Boolean).join(' - ');
  const prefix = nameBits || 'Foundation Research';

  if (kind === 'final') {
    const finalPanel = root.querySelector('[data-foundation-panel="final"]');
    if (!finalPanel || finalPanel.querySelector('.empty-state')) {
      alert('Step 2 final output is not ready yet.');
      return;
    }
    openPrintWindowForPdf(`${prefix} - Step 2 Final Report`, finalPanel.innerHTML);
    return;
  }

  const provider = kind === 'gemini' ? 'gemini' : (kind === 'claude' ? 'claude' : '');
  if (!provider) return;

  const providerPanel = root.querySelector(
    `[data-foundation-panel="step1"] .phase1-collector-report-panel[data-provider="${provider}"]`
  );
  if (!providerPanel) {
    alert(`Step 1 ${provider === 'gemini' ? 'Gemini' : 'Claude'} report is not available.`);
    return;
  }

  const providerLabel = provider === 'gemini' ? 'Gemini' : 'Claude';
  const bodyHtml = `
    <div class="out-section">
      <div class="out-heading">Step 1 Collector Report: ${esc(providerLabel)}</div>
      ${providerPanel.innerHTML}
    </div>
  `;
  openPrintWindowForPdf(`${prefix} - Step 1 ${providerLabel} Report`, bodyHtml);
}

function renderFoundationOutputSwitcher(finalData, collectorsSnapshot, useBranch) {
  if (!collectorsSnapshot || typeof collectorsSnapshot !== 'object') {
    return renderOutput(finalData);
  }
  const reportsRaw = Array.isArray(collectorsSnapshot.collector_reports)
    ? collectorsSnapshot.collector_reports
    : [];
  const providers = new Set(
    reportsRaw.map((report) => normalizeCollectorProviderToken(report.provider || report.label || ''))
  );
  const hasGemini = providers.has('gemini');
  const hasClaude = providers.has('claude');
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
      <div class="foundation-output-export-row">
        <button class="btn btn-ghost btn-sm foundation-output-export-btn"
          onclick="event.stopPropagation(); exportFoundationOutputPdf('final', this)">Export Step 2 PDF</button>
        ${hasGemini
          ? `<button class="btn btn-ghost btn-sm foundation-output-export-btn"
              onclick="event.stopPropagation(); exportFoundationOutputPdf('gemini', this)">Export Gemini PDF</button>`
          : ''}
        ${hasClaude
          ? `<button class="btn btn-ghost btn-sm foundation-output-export-btn"
              onclick="event.stopPropagation(); exportFoundationOutputPdf('claude', this)">Export Claude PDF</button>`
          : ''}
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

function isFoundationFinalReport(data) {
  return (
    data &&
    typeof data === 'object' &&
    String(data.schema_version || '').trim() === '2.0'
  );
}

async function foundationFinalReadyForActiveBrand() {
  if (!activeBrandSlug) return false;
  const brandParam = `?brand=${encodeURIComponent(activeBrandSlug)}`;
  try {
    const resp = await fetch(`/api/outputs/foundation_research${brandParam}`);
    if (!resp.ok) return false;
    const payload = await resp.json();
    return isFoundationFinalReport(payload?.data);
  } catch (_) {
    return false;
  }
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
let newBranchAwarenessLevels = [...MATRIX_AWARENESS_LEVELS];
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

async function updateBranchManagerVisibility() {
  const manager = document.getElementById('branch-manager');
  if (!manager) return;

  // Show branch manager only after Foundation Step 2 final report is available.
  const phase1Done = await foundationFinalReadyForActiveBrand();
  if (phase1Done) {
    manager.classList.remove('hidden');
  } else {
    manager.classList.add('hidden');
  }
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
  phase4ResetStateForBranch();
  storyboardResetStateForBranch();
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
  phase4RefreshForActiveBranch(true);
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

function normalizeAudienceSegment(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
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

function getStoredAudienceSegment(branch) {
  return String(branch?.inputs?.selected_audience_segment || '').trim();
}

function renderNewBranchAudienceSelector(audienceOptions, selectedAudience) {
  const select = document.getElementById('nb-audience-select');
  const help = document.getElementById('nb-audience-help');
  if (!select) return;

  const options = Array.isArray(audienceOptions) ? audienceOptions : [];
  if (!options.length) {
    select.innerHTML = '<option value="">No Pillar 1 audiences found</option>';
    select.disabled = true;
    if (help) {
      help.textContent = 'Run Foundation Research again if Pillar 1 audiences are missing.';
    }
    return;
  }

  const selectedKey = normalizeAudienceSegment(selectedAudience);
  const rows = options.map((row) => {
    const segmentName = String(row?.segment_name || '').trim();
    if (!segmentName) return '';
    const goals = Array.isArray(row?.goals) ? row.goals : [];
    const pains = Array.isArray(row?.pains) ? row.pains : [];
    const snapshot = [];
    if (goals.length) snapshot.push(`Goal: ${goals[0]}`);
    if (pains.length) snapshot.push(`Pain: ${pains[0]}`);
    const label = snapshot.length ? `${segmentName} — ${snapshot.join(' | ')}` : segmentName;
    const isSelected = normalizeAudienceSegment(segmentName) === selectedKey;
    return `<option value="${esc(segmentName)}"${isSelected ? ' selected' : ''}>${esc(label)}</option>`;
  }).filter(Boolean);

  select.disabled = false;
  select.innerHTML = [
    '<option value="">Select a Pillar 1 audience...</option>',
    ...rows,
  ].join('');
  if (help) {
    help.textContent = 'This branch will target exactly one Pillar 1 audience. Matrix rows use LF8 evidence-gated emotions.';
  }
}

async function loadMatrixAxes(options = {}) {
  const forceRefresh = options.forceRefresh !== false;
  const selectedAudienceSegment = String(options.selectedAudienceSegment || '').trim();
  if (!activeBrandSlug) {
    throw new Error('Open a brand before loading matrix axes.');
  }
  const params = new URLSearchParams();
  params.set('brand', activeBrandSlug);
  if (selectedAudienceSegment) {
    params.set('selected_audience_segment', selectedAudienceSegment);
  }
  if (forceRefresh) {
    params.set('_ts', String(Date.now()));
  }
  const resp = await fetch(`/api/matrix-axes?${params.toString()}`, { cache: 'no-store' });
  const data = await resp.json();
  if (!resp.ok || data.error) {
    throw new Error(data.error || `Failed to load matrix axes (HTTP ${resp.status})`);
  }
  matrixMaxPerCell = parseInt(data.max_briefs_per_cell, 10) || matrixMaxPerCell;
  if (!Array.isArray(data.awareness_levels) || !data.awareness_levels.length) {
    data.awareness_levels = [...MATRIX_AWARENESS_LEVELS];
  }
  data.audience_options = Array.isArray(data.audience_options) ? data.audience_options : [];
  data.emotion_rows = Array.isArray(data.emotion_rows) ? data.emotion_rows : [];
  data.requires_audience_selection = Boolean(data.requires_audience_selection);
  data.emotion_source_mode = String(data.emotion_source_mode || (selectedAudienceSegment ? 'lf8_audience_scoped' : 'lf8_empty'));
  data.message = String(data.message || '');
  return data;
}

function renderNewBranchMatrixPlaceholder(message) {
  const mount = document.getElementById('nb-matrix-editor');
  if (!mount) return;
  mount.innerHTML = `<div class="nb-matrix-loading">${esc(message || 'Select a Pillar 1 audience to load matrix rows.')}</div>`;
  updateMatrixTotal();
}

function lf8RowTitle(row) {
  const lf8Label = String(row?.lf8_label || row?.emotion_label || row?.emotion || row?.emotion_key || '').trim();
  const angle = String(row?.emotion_angle || '').trim();
  return angle ? `${lf8Label} — ${angle}` : lf8Label;
}

function lf8RowChips(row) {
  const quotes = Math.max(0, parseInt(row?.tagged_quote_count, 10) || 0);
  const domains = Math.max(0, parseInt(row?.unique_domains, 10) || 0);
  const proof = String(row?.required_proof || '').trim();
  const chips = [
    `Quotes: ${quotes}`,
    `Domains: ${domains}`,
  ];
  if (proof) chips.push(`Proof: ${proof}`);
  return chips.map((label) => `<span class="nb-emotion-chip">${esc(label)}</span>`).join('');
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
    const emotionLabel = String(row?.lf8_label || row?.emotion_label || row?.emotion || row?.emotion_key || '').trim();
    const emotionKey = normalizeEmotionKey(row?.emotion_key || row?.lf8_code || emotionLabel);
    const rowTitle = lf8RowTitle(row);
    const chips = lf8RowChips(row);
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
      <td class="nb-matrix-row-head">
        <div class="nb-emotion-row-title">${esc(rowTitle)}</div>
        <div class="nb-emotion-row-chips">${chips}</div>
      </td>
      ${cells}
    </tr>`;
  }).join('');

  mount.innerHTML = `
    <table class="nb-matrix-table">
      <thead>
        <tr>
          <th>LF8 Emotion \\ Awareness</th>
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

function clearNewBranchMatrixCells() {
  const inputs = Array.from(document.querySelectorAll('#nb-matrix-editor .nb-matrix-input'));
  inputs.forEach((input) => {
    input.value = '0';
  });
  updateMatrixTotal();
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

async function reloadNewBranchMatrixForAudience(selectedAudienceSegment, options = {}) {
  const selectedAudience = String(selectedAudienceSegment || '').trim();
  const forceRefresh = options.forceRefresh !== false;
  const existingMap = (options.existingMap && typeof options.existingMap === 'object')
    ? options.existingMap
    : {};
  const audienceHelp = document.getElementById('nb-audience-help');

  if (!selectedAudience) {
    renderNewBranchMatrixPlaceholder('Select a Pillar 1 audience to load audience-scoped LF8 rows.');
    if (audienceHelp) {
      audienceHelp.textContent = 'Audience selection is required before matrix rows can load.';
    }
    return;
  }

  renderNewBranchMatrixPlaceholder('Loading audience-scoped LF8 rows...');
  try {
    const axes = await loadMatrixAxes({
      forceRefresh,
      selectedAudienceSegment: selectedAudience,
    });
    newBranchAwarenessLevels = Array.isArray(axes.awareness_levels) && axes.awareness_levels.length
      ? axes.awareness_levels
      : [...MATRIX_AWARENESS_LEVELS];
    const emotionRows = Array.isArray(axes.emotion_rows) ? axes.emotion_rows : [];
    const guidance = String(axes.message || '').trim();

    if (emotionRows.length) {
      renderNewBranchMatrixEditor(newBranchAwarenessLevels, emotionRows, existingMap);
      if (audienceHelp) {
        audienceHelp.textContent = guidance || 'This branch will target exactly one Pillar 1 audience.';
      }
      return;
    }

    renderNewBranchMatrixPlaceholder(
      guidance || 'No audience-scoped LF8 rows found. Rebuild Pillar 6 or rerun Foundation Research for this audience.'
    );
    if (audienceHelp) {
      audienceHelp.textContent = guidance || 'No audience-scoped LF8 rows found for this audience.';
    }
  } catch (e) {
    const message = String(e?.message || 'Failed to load audience-scoped LF8 matrix rows.');
    renderNewBranchMatrixPlaceholder(message);
    if (audienceHelp) {
      audienceHelp.textContent = message;
    }
  }
}

async function openNewBranchModal(options = {}) {
  const defaultPhase2Start = Boolean(options.defaultPhase2Start);
  pendingDefaultPhase2Setup = defaultPhase2Start;
  newBranchAwarenessLevels = [...MATRIX_AWARENESS_LEVELS];
  setNewBranchModalContent(defaultPhase2Start);
  const activeBranch = branches.find(b => b.id === activeBranchId);
  document.getElementById('nb-label').value = defaultPhase2Start ? 'Default' : '';
  const editor = document.getElementById('nb-matrix-editor');
  if (editor) {
    editor.innerHTML = '<div class="nb-matrix-loading">Loading audience options...</div>';
  }
  const audienceSelect = document.getElementById('nb-audience-select');
  const audienceHelp = document.getElementById('nb-audience-help');
  if (audienceSelect) {
    audienceSelect.disabled = true;
    audienceSelect.innerHTML = '<option value="">Loading Pillar 1 audiences...</option>';
  }
  if (audienceHelp) {
    audienceHelp.textContent = '';
  }
  updateMatrixTotal();

  document.getElementById('new-branch-modal').classList.remove('hidden');
  document.getElementById('nb-label').focus();

  try {
    const axes = await loadMatrixAxes({ forceRefresh: true });
    newBranchAwarenessLevels = Array.isArray(axes.awareness_levels) && axes.awareness_levels.length
      ? axes.awareness_levels
      : [...MATRIX_AWARENESS_LEVELS];
    const audienceOptions = Array.isArray(axes.audience_options) ? axes.audience_options : [];
    const guidanceMessage = String(axes.message || '').trim();
    const storedAudience = getStoredAudienceSegment(activeBranch);
    const hasStoredAudience = audienceOptions.some((row) =>
      normalizeAudienceSegment(row?.segment_name) === normalizeAudienceSegment(storedAudience)
    );
    const selectedAudience = hasStoredAudience ? storedAudience : '';

    renderNewBranchAudienceSelector(audienceOptions, selectedAudience);
    if (!audienceOptions.length) {
      renderNewBranchMatrixPlaceholder(
        guidanceMessage || 'No Pillar 1 audiences found. Rerun Foundation Research before creating a branch.'
      );
      return;
    }
    if (audienceSelect) {
      audienceSelect.onchange = () => {
        const nextAudience = String(audienceSelect.value || '').trim();
        void reloadNewBranchMatrixForAudience(nextAudience, {
          forceRefresh: true,
          existingMap: {},
        });
      };
    }
    if (audienceHelp && guidanceMessage && !selectedAudience) {
      audienceHelp.textContent = guidanceMessage;
    }
    await reloadNewBranchMatrixForAudience(selectedAudience, {
      forceRefresh: true,
      existingMap: selectedAudience ? getStoredMatrixCellMap(activeBranch) : {},
    });
  } catch (e) {
    if (editor) {
      editor.innerHTML = `<div class="nb-matrix-loading">${esc(e.message || 'Failed to load matrix axes.')}</div>`;
    }
    if (audienceSelect) {
      audienceSelect.disabled = true;
      audienceSelect.innerHTML = '<option value="">Failed to load audiences</option>';
    }
    if (audienceHelp) {
      audienceHelp.textContent = String(e.message || 'Failed to load audiences.');
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
  const selectedAudienceSegment = String(document.getElementById('nb-audience-select')?.value || '').trim();
  if (!selectedAudienceSegment) {
    alert('Select one Pillar 1 audience before creating this branch.');
    return;
  }
  const matrixInputEls = Array.from(document.querySelectorAll('#nb-matrix-editor .nb-matrix-input'));
  if (!matrixInputEls.length) {
    alert('This audience has no LF8 rows yet. Rebuild Pillar 6 or rerun Foundation Research for this brand/audience before creating a branch.');
    return;
  }
  const matrixCells = collectMatrixCellsFromModal();
  const totalPlannedBriefs = matrixCells.reduce((sum, c) => sum + (parseInt(c.brief_count, 10) || 0), 0);
  if (totalPlannedBriefs <= 0) {
    alert('Set at least one matrix cell above 0 before starting Phase 2.');
    return;
  }
  const isDefaultPhase2Setup = pendingDefaultPhase2Setup;
  const idleCreateLabel = isDefaultPhase2Setup ? 'Start Phase 2' : 'Create & Run Phase 2';

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
        selected_audience_segment: selectedAudienceSegment,
        temperature: parseFloat(document.getElementById('nb-temp')?.value || '0.9'),
        model_overrides: {},
        brand: activeBrandSlug || '',
      }),
    });
    const branch = await resp.json();

    if (branch.error) {
      alert(branch.error);
      if (btn) { btn.textContent = idleCreateLabel; btn.disabled = false; }
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
      btn.textContent = idleCreateLabel;
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
      phase4ResetStateForBranch();
      storyboardResetStateForBranch();
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
          <button class="btn btn-ghost btn-sm" onclick="event.stopPropagation(); rebuildFoundationPillar6(this)">Rebuild Pillar 6 Only</button>
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
    .then(async (outputs) => {
      const v2Panel = document.getElementById('phase-3-v2-panel');
      const hooksPanel = document.getElementById('phase3-v2-hooks-panel');
      const scenesPanel = document.getElementById('phase3-v2-scenes-panel');
      const phase4Panel = document.getElementById('phase4-v1-panel');

      if (pipelineRunning) {
        document.querySelectorAll('.btn-start-phase').forEach(b => b.classList.add('hidden'));
        if (v2Panel) v2Panel.classList.add('hidden');
        if (hooksPanel) hooksPanel.classList.add('hidden');
        if (scenesPanel) scenesPanel.classList.add('hidden');
        if (phase4Panel) phase4Panel.classList.add('hidden');
        phase3V2StopPolling();
        return;
      }

      const phase1Done = await foundationFinalReadyForActiveBrand();

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
      const showPhase4 = phase4V1Enabled && phase2Done;
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
      if (phase4Panel) {
        if (showPhase4) {
          phase4Panel.classList.remove('hidden');
          phase4ApplyCollapseState();
          phase4RefreshForActiveBranch();
        } else {
          phase4Panel.classList.add('hidden');
          phase4ResetStateForBranch();
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
  const runBtn = document.getElementById('phase3-v2-run-btn');
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

    const status = String(detail.run?.status || '').toLowerCase();
    const hookStatus = String(detail.hook_stage?.status || '').toLowerCase();
    const sceneStatus = String(detail.scene_stage?.status || '').toLowerCase();
    const anyStageRunning = status === 'running' || hookStatus === 'running' || sceneStatus === 'running';

    // Keep polling while any stage is active (script, hooks, or scenes).
    if (anyStageRunning) {
      if (startPolling) {
        phase3V2StartPolling(runId);
      }
    } else {
      phase3V2StopPolling();
    }

    // Script Writer badge should reflect script run status only.
    if (status === 'running') {
      phase3V2SetStatus('Running', 'running');
    } else if (status === 'failed') {
      phase3V2SetStatus('Failed', 'failed');
    } else if (status === 'completed') {
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
  if (arm === 'claude_sdk') return 'Script';
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

function phase3V2FindDraftForHookUnit(detail, briefUnitId, arm) {
  const runDetail = detail && typeof detail === 'object' ? detail : phase3V2CurrentRunDetail;
  if (!runDetail || typeof runDetail !== 'object') return null;
  const unitId = String(briefUnitId || '').trim();
  const armKey = String(arm || '').trim();
  if (!unitId || !armKey) return null;
  const draftsByArm = runDetail.drafts_by_arm && typeof runDetail.drafts_by_arm === 'object'
    ? runDetail.drafts_by_arm
    : {};
  const rows = Array.isArray(draftsByArm[armKey]) ? draftsByArm[armKey] : [];
  return rows.find((row) => String(row?.brief_unit_id || '').trim() === unitId) || null;
}

function phase3V2BuildHookUnitScriptText(draft) {
  if (!draft || typeof draft !== 'object') {
    return 'No draft generated.';
  }
  const status = String(draft.status || '').trim().toLowerCase();
  if (status === 'blocked') {
    return `Script blocked: ${draft.error || 'insufficient evidence'}`;
  }
  if (status === 'error') {
    return `Script error: ${draft.error || 'generation failed'}`;
  }

  const orderedLines = Array.isArray(draft.lines)
    ? [...draft.lines].sort((a, b) => {
      const an = parseInt(String(a?.line_id || '').replace(/[^\d]/g, ''), 10) || 0;
      const bn = parseInt(String(b?.line_id || '').replace(/[^\d]/g, ''), 10) || 0;
      return an - bn;
    })
    : [];
  const lineBlocks = orderedLines
    .map(line => String(line?.text || '').trim())
    .filter(Boolean);

  if (!lineBlocks.length) {
    return 'No lines returned.';
  }

  return lineBlocks.join('\n');
}

function phase3V2ViewHookUnitScript(briefUnitId, arm) {
  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object') return;
  const unitId = String(briefUnitId || '').trim();
  const armKey = String(arm || '').trim();
  if (!unitId || !armKey) return;

  const draft = phase3V2FindDraftForHookUnit(detail, unitId, armKey);
  const scriptText = phase3V2BuildHookUnitScriptText(draft);
  openOutputFullscreenTextModal(`${unitId} · ${phase3V2ArmDisplayName(armKey)} Script`, scriptText);
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

function phase3V2HookThemeForId(hookId = '', themeIndex = null) {
  const token = String(hookId || '').trim().toLowerCase();
  let hash = 5381;
  for (let i = 0; i < token.length; i += 1) {
    hash = ((hash << 5) + hash + token.charCodeAt(i)) >>> 0;
  }
  const palette = PHASE3_V2_HOOK_THEME_PALETTE.length
    ? PHASE3_V2_HOOK_THEME_PALETTE
    : [{ accent: '#64748b', border: '#334155', soft: 'rgba(100, 116, 139, 0.24)', wash: 'rgba(100, 116, 139, 0.15)', chip: 'rgba(100, 116, 139, 0.18)' }];
  const normalizedIndex = Number.isFinite(Number(themeIndex))
    ? Number(themeIndex)
    : Math.abs(hash);
  return palette[Math.abs(normalizedIndex) % palette.length];
}

function phase3V2HookThemeInlineStyle(hookId = '', themeIndex = null) {
  const theme = phase3V2HookThemeForId(hookId, themeIndex);
  return [
    `--p3v2-hook-accent:${theme.accent}`,
    `--p3v2-hook-border:${theme.border}`,
    `--p3v2-hook-soft:${theme.soft}`,
    `--p3v2-hook-wash:${theme.wash}`,
    `--p3v2-hook-chip:${theme.chip}`,
  ].join(';');
}

function phase3V2BuildHookVariantChip(hookId = '', primaryHookId = '', index = 0) {
  const value = String(hookId || '').trim();
  if (!value) return '';
  const primary = String(primaryHookId || '').trim();
  const isPrimary = Boolean(primary && value === primary);
  const style = phase3V2HookThemeInlineStyle(value);
  return `
    <span class="phase3-v2-hook-variant-chip ${isPrimary ? 'primary' : ''}" style="${style}">
      <span class="phase3-v2-hook-variant-chip-tag">Hook ${index + 1}</span>
    </span>
  `;
}

function phase3V2ApplyHookThemeToModal(modalEl, hookId = '') {
  if (!modalEl) return;
  const panel = modalEl.querySelector('.phase3-v2-arm-modal-panel');
  if (!panel) return;
  const theme = phase3V2HookThemeForId(hookId);
  panel.style.setProperty('--p3v2-hook-accent', theme.accent);
  panel.style.setProperty('--p3v2-hook-border', theme.border);
  panel.style.setProperty('--p3v2-hook-soft', theme.soft);
  panel.style.setProperty('--p3v2-hook-wash', theme.wash);
  panel.style.setProperty('--p3v2-hook-chip', theme.chip);
  panel.classList.add('phase3-v2-hook-themed-panel');
}

function phase3V2ClearHookThemeFromModal(modalEl) {
  if (!modalEl) return;
  const panel = modalEl.querySelector('.phase3-v2-arm-modal-panel');
  if (!panel) return;
  panel.classList.remove('phase3-v2-hook-themed-panel');
  panel.style.removeProperty('--p3v2-hook-accent');
  panel.style.removeProperty('--p3v2-hook-border');
  panel.style.removeProperty('--p3v2-hook-soft');
  panel.style.removeProperty('--p3v2-hook-wash');
  panel.style.removeProperty('--p3v2-hook-chip');
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
  mount.innerHTML = `
    ${locked ? '<div class="phase3-v2-locked-banner">This run is Final Locked. Editing is disabled.</div>' : ''}
    <div class="phase3-v2-expanded-sections phase3-v2-edit-sections phase3-v2-hook-edit-grid">
      <label class="phase3-v2-edit-field phase3-v2-hook-edit-field">
        <span>Hook</span>
        <textarea id="p3v2-hook-edit-verbal" ${locked ? 'disabled' : ''}>${esc(String(variant.verbal_open || ''))}</textarea>
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
  if (!verbal) {
    alert('Hook text is required.');
    return;
  }
  const evidenceIds = Array.isArray(item?.variant?.evidence_ids)
    ? [...new Set(item.variant.evidence_ids.map((v) => String(v || '').trim()).filter(Boolean))]
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
          visual_pattern_interrupt: '',
          on_screen_text: '',
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
  if (modal) {
    phase3V2ClearHookThemeFromModal(modal);
    modal.classList.add('hidden');
  }
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
          <div class="phase3-v2-brief-id">${esc(humanizeAwareness(unit.awareness_level || ''))} × ${esc(unit.emotion_label || unit.emotion_key || '')}</div>
          <div class="phase3-v2-brief-unit-id">${esc(unitId)}</div>
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

function phase3V2CountSelectedHookVariants(detail, eligibleRows = []) {
  if (!detail || typeof detail !== 'object') return 0;
  const allowed = new Set(
    (Array.isArray(eligibleRows) ? eligibleRows : [])
      .map((row) => `${String(row?.brief_unit_id || '').trim()}::${String(row?.arm || '').trim()}`)
      .filter((key) => !key.startsWith('::')),
  );
  const hasAllowedFilter = allowed.size > 0;
  const selectionMap = phase3V2HooksSelectionMap(detail);
  let total = 0;
  Object.entries(selectionMap).forEach(([key, row]) => {
    if (!row || typeof row !== 'object') return;
    if (hasAllowedFilter && !allowed.has(key)) return;
    if (row.skip || row.stale) return;
    const ids = Array.isArray(row.selected_hook_ids)
      ? row.selected_hook_ids.map((v) => String(v || '').trim()).filter(Boolean)
      : [];
    if (!ids.length) {
      const legacyId = String(row.selected_hook_id || '').trim();
      if (legacyId) ids.push(legacyId);
    }
    total += new Set(ids).size;
  });
  return total;
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

  const candidateTarget = Math.max(1, phase3V2HookDefaults.candidatesPerUnit || 10);
  const finalVariants = Math.max(1, phase3V2HookDefaults.finalVariantsPerUnit || 5);

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
  const selectedCountEl = document.getElementById('phase3-v2-hooks-selected-count');
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
    if (selectedCountEl) selectedCountEl.textContent = 'Total selected: 0';
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

  const eligibility = detail.hook_eligibility && typeof detail.hook_eligibility === 'object'
    ? detail.hook_eligibility
    : { eligible: [], skipped: [] };
  const eligibleRows = Array.isArray(eligibility.eligible) ? eligibility.eligible : [];
  const locked = phase3V2IsLocked(detail);
  const selectedTotal = phase3V2CountSelectedHookVariants(detail, eligibleRows);
  progressEl.textContent = '';
  if (selectedCountEl) selectedCountEl.textContent = `Total selected: ${selectedTotal}`;
  if (runBtn) runBtn.disabled = hookStatus === 'running' || locked;
  if (selectAllBtn) selectAllBtn.disabled = hookStatus === 'running' || locked;

  if (!eligibleRows.length) {
    const skipped = parseInt(eligibility.skipped_count, 10) || 0;
    if (selectedCountEl) selectedCountEl.textContent = 'Total selected: 0';
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
      ? variants.map((variant, variantIndex) => {
            const hookId = String(variant?.hook_id || '').trim();
          const isSelected = Boolean(selectedHookIds.includes(hookId) && !stale);
          const gatePass = Boolean(variant?.gate_pass);
          const hookNumber = `Hook ${variantIndex + 1}`;
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
                <div class="phase3-v2-hook-card-title">${esc(hookNumber)}</div>
                <span class="phase3-v2-hook-chip ${gatePass ? 'pass' : 'fail'}">${gatePass ? 'Gate Pass' : 'Needs Repair'}</span>
              </div>
              <div class="phase3-v2-hook-copy">${esc(String(variant?.verbal_open || ''))}</div>
              <div class="phase3-v2-hook-actions">
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
          <div class="phase3-v2-hook-unit-head-actions">
            <button class="btn btn-ghost btn-sm phase3-v2-hook-view-script-btn" onclick="event.stopPropagation(); phase3V2ViewHookUnitScript('${esc(briefUnitId)}', '${esc(arm)}')">View Script</button>
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
  const exact = rows.find((row) =>
    String(row?.brief_unit_id || '').trim() === briefUnitId
    && String(row?.hook_id || '').trim() === hookId
  ) || null;
  if (exact) return exact;
  const sameBrief = rows.filter((row) => String(row?.brief_unit_id || '').trim() === briefUnitId);
  if (sameBrief.length === 1) return sameBrief[0];
  return null;
}

function phase3V2FindSceneGate(detail, briefUnitId, arm, hookId) {
  const gatesByArm = detail?.scene_gate_reports_by_arm;
  if (!gatesByArm || typeof gatesByArm !== 'object') return null;
  const rows = Array.isArray(gatesByArm[arm]) ? gatesByArm[arm] : [];
  const exact = rows.find((row) =>
    String(row?.brief_unit_id || '').trim() === briefUnitId
    && String(row?.hook_id || '').trim() === hookId
  ) || null;
  if (exact) return exact;
  const sameBrief = rows.filter((row) => String(row?.brief_unit_id || '').trim() === briefUnitId);
  if (sameBrief.length === 1) return sameBrief[0];
  return null;
}

function phase3V2BuildScriptLineNarrationIndex(detail = phase3V2CurrentRunDetail) {
  const index = {};
  const draftsByArm = detail?.drafts_by_arm && typeof detail.drafts_by_arm === 'object'
    ? detail.drafts_by_arm
    : {};
  Object.entries(draftsByArm).forEach(([armName, rows]) => {
    const arm = String(armName || '').trim();
    if (!arm || !Array.isArray(rows)) return;
    rows.forEach((draft) => {
      const briefUnitId = String(draft?.brief_unit_id || '').trim();
      if (!briefUnitId) return;
      const lines = Array.isArray(draft?.lines) ? draft.lines : [];
      lines.forEach((line) => {
        const lineId = String(line?.line_id || '').trim();
        const text = String(line?.text || '').trim();
        if (!lineId || !text) return;
        index[`${briefUnitId}::${arm}::${lineId.toUpperCase()}`] = text;
      });
    });
  });

  // Also index split beat IDs from scene outputs so L02.1/L02.2 resolve directly.
  const productionItems = Array.isArray(detail?.production_handoff_packet?.items)
    ? detail.production_handoff_packet.items
    : [];
  productionItems.forEach((item) => {
    const briefUnitId = String(item?.brief_unit_id || '').trim();
    const arm = String(item?.arm || '').trim();
    if (!briefUnitId || !arm) return;
    const lines = Array.isArray(item?.lines) ? item.lines : [];
    lines.forEach((line) => {
      const scriptLineId = String(line?.script_line_id || '').trim().toUpperCase();
      const sourceLineId = String(line?.source_script_line_id || '').trim().toUpperCase();
      const text = String(line?.narration_line || line?.beat_text || line?.script_line_text || line?.narration_text || '').trim();
      if (!text) return;
      if (scriptLineId) index[`${briefUnitId}::${arm}::${scriptLineId}`] = text;
      if (sourceLineId && !index[`${briefUnitId}::${arm}::${sourceLineId}`]) {
        index[`${briefUnitId}::${arm}::${sourceLineId}`] = text;
      }
    });
  });
  return index;
}

function phase3V2SceneNarrationForLine(line, briefUnitId, arm, narrationIndex = phase3V2SceneNarrationIndex) {
  if (!line || typeof line !== 'object') return '';
  const direct = String(line?.narration_line || line?.script_line_text || line?.narration_text || line?.beat_text || '').trim();
  if (direct) return direct;
  const scriptLineId = String(line?.script_line_id || '').trim();
  if (!scriptLineId || !briefUnitId || !arm) return '';
  const keyPrefix = `${briefUnitId}::${arm}::`;
  const scriptKey = scriptLineId.toUpperCase();
  const directLookup = String(narrationIndex?.[`${keyPrefix}${scriptKey}`] || '').trim();
  if (directLookup) return directLookup;

  const sourceLineId = String(line?.source_script_line_id || '').trim().toUpperCase();
  if (sourceLineId) {
    const sourceLookup = String(narrationIndex?.[`${keyPrefix}${sourceLineId}`] || '').trim();
    if (sourceLookup) return sourceLookup;
  }

  const baseScriptId = scriptKey.includes('.') ? scriptKey.split('.')[0] : '';
  if (baseScriptId) {
    return String(narrationIndex?.[`${keyPrefix}${baseScriptId}`] || '').trim();
  }
  return '';
}

function phase3V2SceneFirstNonEmpty(...values) {
  for (const value of values) {
    const text = String(value || '').trim();
    if (text) return text;
  }
  return '';
}

function phase3V2SceneNormalizeMode(value = '') {
  const mode = String(value || '').trim().toLowerCase();
  if (mode === 'a_roll') return 'a_roll';
  if (mode === 'animation_broll') return 'animation_broll';
  return 'b_roll';
}

function phase3V2SceneModeLabel(value = '') {
  const mode = phase3V2SceneNormalizeMode(value);
  if (mode === 'a_roll') return 'A-Roll';
  if (mode === 'animation_broll') return 'Animation B-Roll';
  return 'B-Roll';
}

function phase3V2SceneDirectionSnippet(line) {
  if (!line || typeof line !== 'object') return '';
  const canonical = String(line?.scene_description || '').trim();
  if (canonical) return canonical;
  const a = line.a_roll && typeof line.a_roll === 'object' ? line.a_roll : {};
  const b = line.b_roll && typeof line.b_roll === 'object' ? line.b_roll : {};
  return phase3V2SceneFirstNonEmpty(
    b.shot_description,
    b.subject_action,
    a.creator_action,
    a.framing,
    a.performance_direction,
  );
}

function phase3V2SceneSelectedHookVariants(item, detail = phase3V2CurrentRunDetail) {
  if (!item || typeof item !== 'object' || !detail || typeof detail !== 'object') return [];
  const selectedHookIds = Array.isArray(item.selectedHookIds)
    ? item.selectedHookIds.map((v) => String(v || '').trim()).filter(Boolean)
    : (item.hookId ? [String(item.hookId).trim()] : []);
  if (!selectedHookIds.length) return [];

  const bundle = phase3V2FindHookBundle(detail, String(item.briefUnitId || '').trim(), String(item.arm || '').trim()) || {};
  const variants = Array.isArray(bundle.variants) ? bundle.variants : [];
  const variantMap = {};
  variants.forEach((row) => {
    const hookId = String(row?.hook_id || '').trim();
    if (hookId) variantMap[hookId] = row;
  });

  return selectedHookIds.map((hookId) => {
    const row = variantMap[hookId] || {};
    return {
      hookId,
      isPrimary: hookId === String(item.hookId || '').trim(),
      verbal: String(row?.verbal_open || '').trim(),
      visual: String(row?.visual_pattern_interrupt || '').trim(),
      onScreen: String(row?.on_screen_text || '').trim(),
      evidenceIds: Array.isArray(row?.evidence_ids)
        ? row.evidence_ids.map((v) => String(v || '').trim()).filter(Boolean)
        : [],
    };
  });
}

function phase3V2FindHookOpeningLine(item, hookId, detail = phase3V2CurrentRunDetail) {
  if (!item || typeof item !== 'object') return null;
  const briefUnitId = String(item?.briefUnitId || '').trim();
  const arm = String(item?.arm || '').trim();
  const targetHookId = String(hookId || '').trim();
  if (!briefUnitId || !arm || !targetHookId) return null;

  const productionItems = Array.isArray(detail?.production_handoff_packet?.items)
    ? detail.production_handoff_packet.items
    : [];
  const unit = productionItems.find((row) =>
    String(row?.brief_unit_id || '').trim() === briefUnitId
    && String(row?.arm || '').trim() === arm
    && String(row?.hook_id || '').trim() === targetHookId
  ) || null;
  const unitLines = Array.isArray(unit?.lines) ? unit.lines : [];
  if (unitLines.length && unitLines[0] && typeof unitLines[0] === 'object') {
    return unitLines[0];
  }

  const plansByArm = detail?.scene_plans_by_arm && typeof detail.scene_plans_by_arm === 'object'
    ? detail.scene_plans_by_arm
    : {};
  const armPlans = Array.isArray(plansByArm[arm]) ? plansByArm[arm] : [];
  const plan = armPlans.find((row) =>
    String(row?.brief_unit_id || '').trim() === briefUnitId
    && String(row?.hook_id || '').trim() === targetHookId
  ) || null;
  const planLines = Array.isArray(plan?.lines) ? plan.lines : [];
  if (planLines.length && planLines[0] && typeof planLines[0] === 'object') {
    return planLines[0];
  }
  return null;
}

function phase3V2FallbackHookOpeningDirection(hook = {}, mode = 'a_roll') {
  const narration = String(hook?.verbal || '').trim();
  const visual = String(hook?.visual || '').trim();
  const onScreen = String(hook?.onScreen || '').trim();
  const narrationCue = narration.length > 120 ? `${narration.slice(0, 117)}...` : narration;
  const description = phase3V2SceneFirstNonEmpty(
    visual,
    onScreen ? `On-screen emphasis: ${onScreen}` : '',
    narrationCue ? (mode === 'a_roll'
      ? `On-camera delivery: ${narrationCue}`
      : `Visual-first opening keyed to this line: ${narrationCue}`) : '',
    'Hook-specific opening direction not generated yet.',
  );
  return description;
}

function phase3V2RenderSceneHookOpeningBlock(hook, primaryLine, index = 0) {
  const line = primaryLine && typeof primaryLine === 'object' ? primaryLine : null;
  const rawModeToken = String(line?.mode || '').trim().toLowerCase();
  const mode = rawModeToken === 'a_roll' || rawModeToken === 'b_roll' || rawModeToken === 'animation_broll'
    ? phase3V2SceneNormalizeMode(rawModeToken)
    : (String(hook?.visual || '').trim() ? 'b_roll' : 'a_roll');
  const sceneDescription = phase3V2SceneFirstNonEmpty(
    line?.scene_description,
    phase3V2SceneDirectionSnippet(line),
    phase3V2FallbackHookOpeningDirection(hook, mode),
  );
  const modeLabel = phase3V2SceneModeLabel(mode);
  const titlePrefix = `Hook ${index + 1}`;
  const hookId = String(hook?.hookId || '').trim();
  const narration = String(hook?.verbal || '').trim();
  const hookThemeStyle = phase3V2HookThemeInlineStyle(hookId, index);

  return `
    <div class="phase3-v2-scene-opening-block p3v2-hook-open-row" style="${hookThemeStyle}">
      <div class="p3v2-hook-open-rail">
        <span class="phase3-v2-scene-hook-chip">${titlePrefix}</span>
      </div>
      <div class="p3v2-hook-open-main">
        <div class="p3v2-hook-open-top">
          <div class="phase3-v2-scene-opening-field p3v2-hook-open-mode-field">
            <div class="phase3-v2-scene-opening-label">Mode</div>
            <div class="phase3-v2-scene-opening-value p3v2-hook-open-mode-value">${esc(modeLabel)}</div>
          </div>
          <div class="phase3-v2-scene-opening-field p3v2-hook-open-narration-field">
            <div class="phase3-v2-scene-opening-label">Narration Line</div>
            <div class="phase3-v2-scene-opening-value">${narration ? esc(narration) : 'Missing narration for this hook'}</div>
          </div>
        </div>
        <div class="p3v2-scene-mode-block p3v2-hook-open-direction">
          <div class="phase3-v2-scene-opening-field">
            <div class="phase3-v2-scene-opening-label">Scene Description</div>
            <div class="phase3-v2-scene-opening-value">${esc(sceneDescription) || 'none'}</div>
          </div>
        </div>
      </div>
    </div>
  `;
}

function phase3V2SceneSnippet(lines, options = {}) {
  if (!Array.isArray(lines) || !lines.length) return 'No scene lines yet.';
  const briefUnitId = String(options.briefUnitId || '').trim();
  const arm = String(options.arm || '').trim();
  const narrationIndex = options.narrationIndex || phase3V2SceneNarrationIndex;
  const selected = lines.slice(0, 2).map((line) => {
    const mode = String(line?.mode || '').trim() || 'mode';
    const narration = phase3V2SceneNarrationForLine(line, briefUnitId, arm, narrationIndex);
    const text = phase3V2SceneDirectionSnippet(line);
    const narrationPart = narration ? ` "${narration}"` : '';
    return `[${mode}]${narrationPart}: ${text || 'No direction text'}`;
  });
  return selected.join('\n');
}

function phase3V2BuildSceneExpandedItems() {
  const detail = phase3V2CurrentRunDetail;
  if (!detail || typeof detail !== 'object') return [];
  phase3V2SceneNarrationIndex = phase3V2BuildScriptLineNarrationIndex(detail);

  const briefRows = Array.isArray(detail.brief_units) ? detail.brief_units : [];
  const briefMeta = {};
  const narrationIndex = phase3V2BuildScriptLineNarrationIndex(detail);
  phase3V2SceneNarrationIndex = narrationIndex;
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
    const selectedHookIds = Array.isArray(row?.selected_hook_ids)
      ? row.selected_hook_ids.map((v) => String(v || '').trim()).filter(Boolean)
      : (hookId ? [hookId] : []);
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
      selectedHookIds,
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

function phase3V2ContinueToStoryboard() {
  if (!phase3V2CurrentRunId) {
    alert('Run Scenes first, then continue to Story Board.');
    return;
  }
  goToView('storyboard');
}

function phase3V2RenderScenesSection() {
  const panel = document.getElementById('phase3-v2-scenes-panel');
  const progressEl = document.getElementById('phase3-v2-scenes-progress');
  const resultsEl = document.getElementById('phase3-v2-scenes-results');
  const runBtn = document.getElementById('phase3-v2-scenes-run-btn');
  const continueBtn = document.getElementById('phase3-v2-scenes-continue-btn');
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
  if (!detail || typeof detail !== 'object') {
    phase3V2ScenesSetStatus('Idle');
    phase3V2ScenesSetPrepareState('Select a run to open Scene Writer.');
    progressEl.textContent = '';
    resultsEl.innerHTML = '<div class="empty-state">Select a run to open Scene Writer.</div>';
    if (runBtn) runBtn.disabled = true;
    if (continueBtn) {
      continueBtn.classList.add('hidden');
      continueBtn.disabled = true;
    }
    return;
  }
  const inferredRunId = String(
    phase3V2CurrentRunId
    || detail?.run?.run_id
    || detail?.run_id
    || phase3V2RunsCache?.[0]?.run_id
    || ''
  ).trim();
  if (inferredRunId && inferredRunId !== phase3V2CurrentRunId) {
    phase3V2CurrentRunId = inferredRunId;
    phase3V2RenderRunSelect();
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
  const packet = detail.scene_handoff_packet && typeof detail.scene_handoff_packet === 'object'
    ? detail.scene_handoff_packet
    : { items: [] };
  const packetItems = Array.isArray(packet.items) ? packet.items : [];
  const readyItems = packetItems.filter((row) => String(row?.status || '').trim().toLowerCase() === 'ready');
  const readyBriefIds = new Set(
    readyItems
      .map((row) => String(row?.brief_unit_id || '').trim())
      .filter(Boolean)
  );
  const locked = phase3V2IsLocked(detail);
  const handoffReady = Boolean(detail.scene_handoff_ready);
  const readyForHandoff = Boolean(progress.ready_for_handoff);
  const runDisabled = sceneStatus === 'running' || locked || !handoffReady || !phase3V2CurrentRunId;
  if (runBtn) runBtn.disabled = runDisabled;
  const canContinueToStoryboard = Boolean(
    phase3V2CurrentRunId
    && handoffReady
    && (readyForHandoff || packetItems.length > 0)
  );
  if (continueBtn) {
    continueBtn.classList.toggle('hidden', !canContinueToStoryboard);
    continueBtn.disabled = !canContinueToStoryboard;
  }

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
    const totalRequired = parseInt(progress.total_required, 10) || 0;
    const briefCount = readyBriefIds.size;
    if (totalRequired > 0) {
      phase3V2ScenesSetPrepareState(
        `Ready to run ${totalRequired} scene unit${totalRequired === 1 ? '' : 's'} across ${briefCount} brief${briefCount === 1 ? '' : 's'}.`,
        'ready'
      );
    } else {
      phase3V2ScenesSetPrepareState('Scene handoff ready. Click Run Scenes.', 'ready');
    }
  }

  const productionPacket = detail.production_handoff_packet && typeof detail.production_handoff_packet === 'object'
    ? detail.production_handoff_packet
    : { items: [] };
  const items = Array.isArray(productionPacket.items) ? productionPacket.items : [];
  if (!items.length) {
    resultsEl.innerHTML = '<div class="phase3-v2-hook-empty">No scene units yet. Run hooks and select hooks first.</div>';
    return;
  }

  const briefRows = Array.isArray(detail.brief_units) ? detail.brief_units : [];
  const briefMeta = {};
  const narrationIndex = phase3V2BuildScriptLineNarrationIndex(detail);
  phase3V2SceneNarrationIndex = narrationIndex;
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
    const title = `${humanizeAwareness(meta?.awareness_level || '')} × ${meta?.emotion_label || meta?.emotion_key || ''}`;
    const hookThemeStyle = phase3V2HookThemeInlineStyle(hookId);

    return `
      <div class="phase3-v2-hook-unit phase3-v2-scene-unit phase3-v2-hook-themed" style="${hookThemeStyle}">
        <div class="phase3-v2-hook-unit-head">
          <div>
            <div class="phase3-v2-hook-unit-title">${esc(title)}</div>
          </div>
          <span class="phase3-v2-hook-chip ${statusClass}">${esc(status)}</span>
        </div>
        ${Boolean(row?.stale) ? `<div class="phase3-v2-hook-stale">${esc(String(row?.stale_reason || 'Scene plan is stale. Rerun scenes after upstream edits.'))}</div>` : ''}
        <div class="phase3-v2-hook-score-row">
          <span class="phase3-v2-hook-chip ${gate?.overall_pass ? 'pass' : 'fail'}">${gate?.overall_pass ? 'Gate Pass' : 'Gate Check'}</span>
        </div>
        <div class="phase3-v2-script-snippet">${esc(phase3V2SceneSnippet(lines, { briefUnitId, arm, narrationIndex }))}</div>
        <div class="phase3-v2-hook-actions">
          <button class="btn btn-ghost btn-sm" onclick="phase3V2OpenSceneExpanded('${esc(briefUnitId)}','${esc(arm)}','${esc(hookId)}')">Expand</button>
        </div>
      </div>
    `;
  }).join('');

  resultsEl.innerHTML = html || '<div class="phase3-v2-hook-empty">No scene units to render.</div>';
}

function phase3V2BuildSceneEditorLineRow(line = {}, disabled = false, options = {}) {
  const mode = phase3V2SceneNormalizeMode(line?.mode || 'a_roll');
  const explicitScriptLineId = String(line?.script_line_id || '').trim();
  const fallbackLineIndex = Number.isFinite(Number(options?.lineIndex))
    ? Math.max(1, Number(options.lineIndex) + 1)
    : 1;
  const scriptLineId = explicitScriptLineId || `L${String(fallbackLineIndex).padStart(2, '0')}`;
  const sourceScriptLineId = String(line?.source_script_line_id || '').trim() || scriptLineId.split('.')[0];
  const beatIndex = Math.max(1, parseInt(line?.beat_index, 10) || (scriptLineId.includes('.') ? parseInt(scriptLineId.split('.')[1], 10) || 1 : 1));
  const beatText = String(line?.beat_text || '').trim();
  const briefUnitId = String(options?.briefUnitId || '').trim();
  const arm = String(options?.arm || '').trim();
  const narration = phase3V2SceneNarrationForLine(
    line,
    briefUnitId,
    arm,
    options?.narrationIndex || phase3V2SceneNarrationIndex
  );
  const duration = Math.max(0.1, parseFloat(line?.duration_seconds || 2.0) || 2.0);
  const lockAttr = disabled ? 'disabled' : '';
  const sceneDescription = phase3V2SceneFirstNonEmpty(
    line?.scene_description,
    phase3V2SceneDirectionSnippet(line),
  );
  const lineThemeStyle = phase3V2HookThemeInlineStyle('', options?.lineIndex || 0);

  return `
    <div class="p3v2-scene-line-row"
         draggable="false"
         data-script-line-id="${esc(scriptLineId)}"
         data-source-script-line-id="${esc(sourceScriptLineId)}"
         data-beat-index="${esc(String(beatIndex))}"
         data-beat-text="${esc(beatText)}"
         data-duration-seconds="${esc(String(duration))}"
         style="${lineThemeStyle}">
      <div class="p3v2-scene-line-rail">
        <span class="p3v2-line-order p3v2-scene-order"></span>
      </div>
      <div class="p3v2-scene-line-main">
        <div class="p3v2-scene-line-top">
          <div class="phase3-v2-scene-opening-field p3v2-scene-mode-field">
            <div class="phase3-v2-scene-opening-label">Mode</div>
            <select class="p3v2-scene-mode-select p3v2-scene-mode-select-hook" ${lockAttr}>
              <option value="a_roll" ${mode === 'a_roll' ? 'selected' : ''}>A-Roll</option>
              <option value="b_roll" ${mode === 'b_roll' ? 'selected' : ''}>B-Roll</option>
              <option value="animation_broll" ${mode === 'animation_broll' ? 'selected' : ''}>Animation B-Roll</option>
            </select>
          </div>
          <div class="p3v2-scene-narration-field phase3-v2-scene-opening-field">
            <div class="phase3-v2-scene-opening-label">Narration Line</div>
            <div class="phase3-v2-scene-opening-value p3v2-scene-narration">${esc(narration || 'No narration found for this line ID yet.')}</div>
          </div>
        </div>
        <div class="p3v2-scene-mode-block">
          <label class="phase3-v2-edit-field p3v2-scene-line-detail-field"><span>Scene Description</span><textarea class="p3v2-scene-description" ${lockAttr}>${esc(sceneDescription)}</textarea></label>
        </div>
      </div>
    </div>
  `;
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
  if (!container || container.dataset.sceneReorderWired === '1') return;
  container.dataset.sceneReorderWired = '1';

  container.addEventListener('dragstart', (event) => {
    const handle = event.target?.closest?.('.p3v2-scene-drag-handle');
    if (!handle || phase3V2IsLocked()) {
      event.preventDefault();
      return;
    }
    const row = handle.closest('.p3v2-scene-line-row');
    if (!row) {
      event.preventDefault();
      return;
    }
    phase3V2SceneDraggingRow = row;
    row.classList.add('dragging');
    if (event.dataTransfer) {
      event.dataTransfer.effectAllowed = 'move';
      event.dataTransfer.setData('text/plain', 'scene-line-reorder');
    }
  });

  container.addEventListener('dragover', (event) => {
    if (!phase3V2SceneDraggingRow || phase3V2SceneDraggingRow === null || phase3V2IsLocked()) {
      return;
    }
    const row = event.target?.closest?.('.p3v2-scene-line-row');
    if (!row || row === phase3V2SceneDraggingRow) return;
    event.preventDefault();
    if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
    const rect = row.getBoundingClientRect();
    const before = event.clientY < rect.top + rect.height / 2;
    if (before) container.insertBefore(phase3V2SceneDraggingRow, row);
    else container.insertBefore(phase3V2SceneDraggingRow, row.nextSibling);
  });

  container.addEventListener('drop', (event) => {
    if (!phase3V2SceneDraggingRow) return;
    event.preventDefault();
    phase3V2RefreshSceneEditorLineOrderLabels();
  });

  container.addEventListener('dragend', () => {
    if (phase3V2SceneDraggingRow) {
      phase3V2SceneDraggingRow.classList.remove('dragging');
    }
    phase3V2SceneDraggingRow = null;
    phase3V2RefreshSceneEditorLineOrderLabels();
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
  const lines = Array.isArray(plan.lines) ? plan.lines : [];
  const selectedHookIds = Array.isArray(item?.selectedHookIds)
    ? item.selectedHookIds.map((v) => String(v || '').trim()).filter(Boolean)
    : (item?.hookId ? [String(item.hookId).trim()] : []);
  const hookVariants = phase3V2SceneSelectedHookVariants(item);
  const hookLegendHtml = selectedHookIds.length
    ? `
      <div class="phase3-v2-hook-variant-box">
        <div class="phase3-v2-hook-variant-label">Hook Variations (${selectedHookIds.length})</div>
        <div class="phase3-v2-hook-variant-legend">
          ${selectedHookIds.map((hookId, index) => phase3V2BuildHookVariantChip(hookId, item?.hookId, index)).join('')}
        </div>
      </div>
    `
    : '';
  const hookOpeningCardsHtml = hookVariants.length
    ? `
      <div class="phase3-v2-scene-hook-openings">
        <div class="phase3-v2-scene-hook-openings-label">Hook Openings</div>
        <div class="phase3-v2-scene-hook-openings-grid">
          ${hookVariants.map((hook, index) => {
            const defaultModeOnly = lines[0] && typeof lines[0] === 'object'
              ? { mode: String(lines[0]?.mode || '').trim() }
              : null;
            const openingLine = phase3V2FindHookOpeningLine(item, hook?.hookId) || (hook?.isPrimary ? lines[0] : defaultModeOnly);
            return phase3V2RenderSceneHookOpeningBlock(hook, openingLine, index);
          }).join('')}
        </div>
      </div>
    `
    : '';
  const narrationIndex = phase3V2SceneNarrationIndex || phase3V2BuildScriptLineNarrationIndex();
  const rowsHtml = lines.length
    ? lines.map((line, lineIndex) => phase3V2BuildSceneEditorLineRow(
      line,
      locked,
      { briefUnitId: item?.briefUnitId, arm: item?.arm, narrationIndex, lineIndex }
    )).join('')
    : '<div class="phase3-v2-expanded-alert">No scene lines were generated for this unit yet.</div>';
  mount.innerHTML = `
    <div class="p3v2-scene-editor-layout">
      ${locked ? '<div class="phase3-v2-locked-banner">This run is Final Locked. Editing is disabled.</div>' : ''}
      ${item?.stale ? `<div class="phase3-v2-hook-stale">${esc(String(item?.staleReason || 'Scene plan is stale.'))}</div>` : ''}
      ${hookLegendHtml}
      ${hookOpeningCardsHtml}
      <div class="p3v2-scene-lines-wrap">
        <div class="p3v2-scene-lines-label">Scene Lines</div>
        <div id="p3v2-scene-lines" class="p3v2-scene-lines">${rowsHtml}</div>
      </div>
      <div class="phase3-v2-editor-actions">
        <button class="btn btn-primary btn-sm" id="phase3-v2-save-scene-btn" onclick="phase3V2SaveSceneEdits()" ${locked ? 'disabled' : ''}>Save Scene</button>
      </div>
    </div>
  `;
  phase3V2RefreshSceneEditorLineOrderLabels();
  phase3V2WireSceneEditorReorder();
}

function phase3V2AddSceneLine() {
  if (phase3V2IsLocked()) return;
  const container = document.getElementById('p3v2-scene-lines');
  if (!container) return;
  const item = phase3V2CurrentSceneExpandedItem();
  const lineIndex = container.querySelectorAll('.p3v2-scene-line-row').length;
  const wrapper = document.createElement('div');
  wrapper.innerHTML = phase3V2BuildSceneEditorLineRow(
    {},
    false,
    {
      briefUnitId: String(item?.briefUnitId || '').trim(),
      arm: String(item?.arm || '').trim(),
      narrationIndex: phase3V2SceneNarrationIndex,
      lineIndex,
    }
  );
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
  const scriptLineId = String(row.dataset.scriptLineId || '').trim() || 'line';
  const mode = String(row.querySelector('.p3v2-scene-mode-select')?.value || 'a_roll').trim();
  const direction = [
    row.querySelector('.p3v2-scene-description')?.value,
  ];
  const directionText = direction.map((v) => String(v || '').trim()).filter(Boolean).join(' | ');
  const payload = `${scriptLineId}\nmode: ${mode}\ndirection: ${directionText}`;
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
    const scriptLineId = String(row.dataset.scriptLineId || '').trim();
    const sourceScriptLineId = String(row.dataset.sourceScriptLineId || '').trim();
    const beatIndex = Math.max(1, parseInt(row.dataset.beatIndex || '1', 10) || 1);
    const beatText = String(row.dataset.beatText || '').trim();
    const mode = phase3V2SceneNormalizeMode(
      String(row.querySelector('.p3v2-scene-mode-select')?.value || 'a_roll').trim()
    );
    const duration = Math.max(0.1, Math.min(30, parseFloat(row.dataset.durationSeconds || '2') || 2));
    const narrationLine = String(row.querySelector('.p3v2-scene-narration')?.textContent || '').trim();
    const sceneDescription = String(row.querySelector('.p3v2-scene-description')?.value || '').trim();
    return {
      scene_line_id: '',
      script_line_id: scriptLineId,
      source_script_line_id: sourceScriptLineId,
      beat_index: beatIndex,
      beat_text: beatText,
      mode,
      narration_line: narrationLine,
      scene_description: sceneDescription,
      duration_seconds: duration,
    };
  }).filter((row) => Boolean(row.script_line_id));

  if (!lines.length) {
    alert('Add at least one scene line before saving.');
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
  const awarenessEmotionTitle = `${humanizeAwareness(item.awarenessLevel)} × ${item.emotionLabel}`;
  const selectedHookIds = Array.isArray(item?.selectedHookIds)
    ? item.selectedHookIds.map((v) => String(v || '').trim()).filter(Boolean)
    : [];
  const hookSummary = selectedHookIds.length ? `hooks ${selectedHookIds.length}` : 'hooks 1';
  title.textContent = awarenessEmotionTitle;
  subtitle.textContent = `${item.briefUnitId} · ${phase3V2ArmDisplayName(item.arm)} · ${hookSummary} · ${itemLabel}`;
  phase3V2SceneExpandedCurrent = item;
  phase3V2SceneChatPendingPlan = null;
  const sceneTabBtn = document.getElementById('phase3-v2-scene-tab-editor');
  const chatTabBtn = document.getElementById('phase3-v2-scene-tab-chat');
  const chatPane = document.getElementById('phase3-v2-scene-chat-pane');
  if (sceneTabBtn) sceneTabBtn.classList.remove('hidden');
  if (chatTabBtn) chatTabBtn.classList.add('hidden');
  if (chatPane) chatPane.classList.add('hidden');
  phase3V2RenderSceneEditorPane(item);
  phase3V2ApplyHookThemeToModal(modal, item.hookId);
  phase3V2SceneTab = 'scene';
  phase3V2SwitchSceneTab('scene');
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
  if (modal) {
    phase3V2ClearHookThemeFromModal(modal);
    modal.classList.add('hidden');
  }
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
    resetCost();
    await refreshCostTracker();

    // Close brand selector panel
    if (brandSelectorOpen) toggleBrandSelector();

    // Populate brief form with brand data
    if (brand.brief) {
      populateForm(brand.brief);
    }

    // Reset all pipeline cards
    resetAllCards();
    clearPreviewCache();

    // Restore Phase 1 state only from final Foundation output readiness.
    const availableAgents = normalizeAvailableAgents(brand.available_agents);
    if (await foundationFinalReadyForActiveBrand()) {
      setCardState('foundation_research', 'done');
    }

    // Load brand's branches
    branches = Array.isArray(brand.branches)
      ? brand.branches.filter(b => b && typeof b === 'object')
      : [];
    activeBranchId = branches.length > 0 ? branches[0].id : null;
    phase3V2ResetStateForBranch();
    phase4ResetStateForBranch();
    storyboardResetStateForBranch();
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
      resetCost();
      branches = [];
      activeBranchId = null;
      phase3V2ResetStateForBranch();
      phase4ResetStateForBranch();
      storyboardResetStateForBranch();
      renderBranchTabs();
    }

    loadBrandList();
  } catch (e) {
    console.error('Delete brand failed', e);
  }
}

// -----------------------------------------------------------
// PHASE 4 V1 (VIDEO GENERATION TEST MODE)
// -----------------------------------------------------------

function phase4BrandParam() {
  return activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
}

function phase4LoadGuideCheckedMap() {
  try {
    const raw = localStorage.getItem(PHASE4_GUIDE_CHECKED_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch (_e) {
    return {};
  }
}

function phase4EnsureGuideCheckedLoaded() {
  if (phase4GuideCheckedLoaded) return;
  phase4GuideCheckedByRun = phase4LoadGuideCheckedMap();
  phase4GuideCheckedLoaded = true;
}

function phase4SaveGuideCheckedMap() {
  try {
    localStorage.setItem(PHASE4_GUIDE_CHECKED_STORAGE_KEY, JSON.stringify(phase4GuideCheckedByRun || {}));
  } catch (_e) {
    // ignore localStorage failures
  }
}

function phase4GetCheckedSet(runId) {
  phase4EnsureGuideCheckedLoaded();
  const key = String(runId || '').trim();
  if (!key) return new Set();
  const raw = phase4GuideCheckedByRun && phase4GuideCheckedByRun[key];
  if (!Array.isArray(raw)) return new Set();
  return new Set(raw.map((v) => String(v || '')).filter(Boolean));
}

function phase4SetCheckedSet(runId, setValue) {
  const key = String(runId || '').trim();
  if (!key) return;
  const next = Array.from(setValue || []).map((v) => String(v || '')).filter(Boolean);
  if (next.length) phase4GuideCheckedByRun[key] = next;
  else delete phase4GuideCheckedByRun[key];
  phase4SaveGuideCheckedMap();
}

function phase4CaptureGuideScrollState() {
  const panelWrap = document.querySelector('#phase4-v1-guide .phase4-guide-table-wrap');
  const modalWrap = document.querySelector('#output-fullscreen-body .phase4-guide-table-wrap');
  return {
    panel: panelWrap ? Number(panelWrap.scrollTop || 0) : 0,
    modal: modalWrap ? Number(modalWrap.scrollTop || 0) : 0,
  };
}

function phase4RestoreGuideScrollState(state) {
  if (!state || typeof state !== 'object') return;
  const panelWrap = document.querySelector('#phase4-v1-guide .phase4-guide-table-wrap');
  const modalWrap = document.querySelector('#output-fullscreen-body .phase4-guide-table-wrap');
  if (panelWrap && Number.isFinite(state.panel)) panelWrap.scrollTop = state.panel;
  if (modalWrap && Number.isFinite(state.modal)) modalWrap.scrollTop = state.modal;
}

async function phase4CopyGuideFilename(event, fileNameEncoded) {
  if (event && typeof event.stopPropagation === 'function') event.stopPropagation();
  const fileName = decodeURIComponent(String(fileNameEncoded || ''));
  if (!fileName) return;
  try {
    if (!navigator?.clipboard?.writeText) {
      throw new Error('Clipboard API unavailable');
    }
    await navigator.clipboard.writeText(fileName);
  } catch (_e) {
    try {
      window.prompt('Copy filename:', fileName);
    } catch (_ignore) {
      // ignore fallback failures
    }
  }
}

function phase4ToggleGuideRow(runIdEncoded, fileNameEncoded) {
  const runId = decodeURIComponent(String(runIdEncoded || ''));
  const fileName = decodeURIComponent(String(fileNameEncoded || ''));
  if (!runId || !fileName) return;
  const scrollState = phase4CaptureGuideScrollState();
  const checked = phase4GetCheckedSet(runId);
  if (checked.has(fileName)) checked.delete(fileName);
  else checked.add(fileName);
  phase4SetCheckedSet(runId, checked);
  phase4RenderStartFrameGuide();
  phase4RestoreGuideScrollState(scrollState);
}

function phase4SetStatus(text, state = '') {
  const el = document.getElementById('phase4-v1-status');
  if (!el) return;
  el.textContent = text || 'Idle';
  el.classList.remove('running', 'done', 'completed', 'failed');
  if (state) el.classList.add(state);
}

function phase4ApplyCollapseState() {
  const panel = document.getElementById('phase4-v1-panel');
  const body = document.getElementById('phase4-v1-body');
  if (panel) panel.classList.toggle('collapsed', phase4Collapsed);
  if (body) body.classList.toggle('hidden', phase4Collapsed);
}

function phase4ToggleCollapse() {
  phase4Collapsed = !phase4Collapsed;
  phase4ApplyCollapseState();
}

function phase4StopPolling() {
  if (phase4PollTimer) {
    clearInterval(phase4PollTimer);
    phase4PollTimer = null;
  }
  phase4PollRunId = '';
  phase4PollInFlight = false;
  phase4PollGraceUntilMs = 0;
}

function phase4ShouldPollDetail(detail = phase4CurrentRunDetail) {
  const run = detail?.run && typeof detail.run === 'object' ? detail.run : {};
  const state = String(run.workflow_state || run.status || '').trim().toLowerCase();
  return state === 'validating_assets' || state === 'generating' || state === 'generation_in_progress';
}

function phase4StartPolling(runId, options = {}) {
  const targetRunId = String(runId || phase4CurrentRunId || '').trim();
  if (!targetRunId || !activeBranchId || !activeBrandSlug) return;
  const graceMs = Math.max(0, parseInt(options.graceMs, 10) || 0);
  if (graceMs > 0) {
    phase4PollGraceUntilMs = Math.max(phase4PollGraceUntilMs, Date.now() + graceMs);
  }
  if (phase4PollTimer && phase4PollRunId === targetRunId) return;
  phase4StopPolling();
  if (graceMs > 0) {
    phase4PollGraceUntilMs = Date.now() + graceMs;
  }
  phase4PollRunId = targetRunId;
  phase4PollTimer = setInterval(async () => {
    if (phase4PollInFlight) return;
    if (!activeBranchId || !activeBrandSlug || !phase4CurrentRunId || phase4CurrentRunId !== phase4PollRunId) {
      phase4StopPolling();
      return;
    }
    phase4PollInFlight = true;
    try {
      await phase4LoadRunDetail(phase4PollRunId, { silent: true });
    } finally {
      phase4PollInFlight = false;
    }
  }, 2000);
}

function phase4SyncPollingFromDetail(detail = phase4CurrentRunDetail) {
  const runId = String(detail?.run?.video_run_id || phase4CurrentRunId || '').trim();
  const withinGrace = runId && runId === phase4PollRunId && Date.now() < phase4PollGraceUntilMs;
  if (runId && (phase4ShouldPollDetail(detail) || withinGrace)) {
    phase4StartPolling(runId);
    return;
  }
  phase4StopPolling();
}

function phase4ResetStateForBranch() {
  phase4StopPolling();
  phase4RunsCache = [];
  phase4CurrentRunId = '';
  phase4CurrentRunDetail = null;
  const select = document.getElementById('phase4-v1-run-select');
  if (select) select.innerHTML = '<option value="">No runs yet</option>';
  const clipsEl = document.getElementById('phase4-v1-clips');
  if (clipsEl) clipsEl.innerHTML = '<div class="empty-state">Create or select a Phase 4 run.</div>';
  const guideEl = document.getElementById('phase4-v1-guide');
  if (guideEl) guideEl.textContent = 'Create or select a Phase 4 run to see the upload guide.';
  const summaryEl = document.getElementById('phase4-v1-run-summary');
  if (summaryEl) summaryEl.textContent = 'No run selected.';
  phase4SetStatus('Idle');
  if (getOutputFullscreenContext() === OUTPUT_FULLSCREEN_CONTEXT_PHASE4_GUIDE) {
    closeOutputFullscreenModal();
  }
}

function phase4RenderVoicePresetOptions() {
  const select = document.getElementById('phase4-v1-voice-select');
  if (!select) return;
  const rows = Array.isArray(phase4VoicePresets) ? phase4VoicePresets : [];
  if (!rows.length) {
    select.innerHTML = '<option value="">No voice presets</option>';
    return;
  }
  const current = select.value;
  select.innerHTML = rows
    .map((row) => {
      const id = esc(String(row.voice_preset_id || ''));
      const name = esc(String(row.name || row.voice_preset_id || 'Voice'));
      return `<option value="${id}">${name}</option>`;
    })
    .join('');
  if (current && rows.some((row) => String(row.voice_preset_id || '') === current)) {
    select.value = current;
  }
}

function phase4RenderRunSelect() {
  const select = document.getElementById('phase4-v1-run-select');
  if (!select) return;
  if (!phase4RunsCache.length) {
    select.innerHTML = '<option value="">No runs yet</option>';
    return;
  }
  select.innerHTML = phase4RunsCache
    .map((row) => {
      const runId = String(row.video_run_id || '');
      const state = String(row.workflow_state || row.status || 'unknown');
      return `<option value="${esc(runId)}">${esc(runId)} · ${esc(state)}</option>`;
    })
    .join('');
  if (phase4CurrentRunId) {
    select.value = phase4CurrentRunId;
  } else {
    phase4CurrentRunId = String(phase4RunsCache[0]?.video_run_id || '');
    select.value = phase4CurrentRunId;
  }
}

function phase4RenderCurrentRun() {
  const summaryEl = document.getElementById('phase4-v1-run-summary');
  const clipsEl = document.getElementById('phase4-v1-clips');
  if (!summaryEl || !clipsEl) return;
  if (!phase4CurrentRunDetail || !phase4CurrentRunDetail.run) {
    summaryEl.textContent = 'No run selected.';
    clipsEl.innerHTML = '<div class="empty-state">Create or select a Phase 4 run.</div>';
    const guideEl = document.getElementById('phase4-v1-guide');
    if (guideEl) guideEl.textContent = 'Create or select a Phase 4 run to see the upload guide.';
    phase4SetStatus('Idle');
    phase4RenderGuideFullscreenIfOpen();
    return;
  }
  const run = phase4CurrentRunDetail.run || {};
  const state = String(run.workflow_state || run.status || 'unknown');
  const approved = Array.isArray(phase4CurrentRunDetail.clips)
    ? phase4CurrentRunDetail.clips.filter((c) => String(c.status || '') === 'approved').length
    : 0;
  const total = Array.isArray(phase4CurrentRunDetail.clips) ? phase4CurrentRunDetail.clips.length : 0;
  summaryEl.textContent = `${run.video_run_id || ''} · ${state} · approved ${approved}/${total}`;
  const stateClass = state === 'completed' ? 'completed' : state === 'failed' ? 'failed' : (state === 'generating' ? 'running' : '');
  phase4SetStatus(state, stateClass);
  phase4RenderStartFrameGuide();

  const clips = Array.isArray(phase4CurrentRunDetail.clips) ? phase4CurrentRunDetail.clips : [];
  if (!clips.length) {
    clipsEl.innerHTML = '<div class="empty-state">No clips found.</div>';
    return;
  }
  clipsEl.innerHTML = clips
    .slice()
    .sort((a, b) => (parseInt(a.line_index, 10) || 0) - (parseInt(b.line_index, 10) || 0))
    .map((clip) => {
      const clipId = String(clip.clip_id || '');
      const clipDisplayId = clipId.includes('__clip__')
        ? `clip__${clipId.split('__clip__').slice(1).join('__clip__')}`
        : clipId;
      const mode = String(clip.mode || '');
      const status = String(clip.status || '');
      const scriptLine = String(clip.script_line_id || '');
      const previewUrl = String(clip.preview_url || '').trim();
      const previewAssetType = String(clip.preview_asset_type || '').trim();
      const revision = clip.current_revision && typeof clip.current_revision === 'object'
        ? clip.current_revision
        : {};
      const snapshot = revision.input_snapshot && typeof revision.input_snapshot === 'object'
        ? revision.input_snapshot
        : {};
      const modelIds = snapshot.model_ids && typeof snapshot.model_ids === 'object'
        ? snapshot.model_ids
        : {};
      const narrationLine = String(clip.narration_line || snapshot.narration_text || clip.narration_text || '').trim();
      const generationPrompt = String(clip.generation_prompt || narrationLine).trim();
      const generationModel = String(
        clip.generation_model
        || (mode === 'a_roll' ? modelIds.fal_talking_head : modelIds.fal_broll)
        || ''
      ).trim();
      const transformPrompt = String(clip.transform_prompt || snapshot.transform_prompt || '').trim();
      const startFrameFilename = String(
        clip.start_frame_filename || snapshot.start_frame_filename || snapshot.avatar_filename || ''
      ).trim();
      const startFrameUrl = String(clip.start_frame_url || '').trim();
      const revStatus = String(revision.status || '');
      const reviewStatus = revStatus || status;
      const reviewable = reviewStatus === 'pending_review';
      const busyStatuses = new Set(['transforming', 'generating_tts', 'generating_a_roll', 'generating_b_roll']);
      const canRevise = !busyStatuses.has(reviewStatus);
      const approveDisabled = reviewable ? '' : 'disabled';
      const needsRevisionDisabled = reviewable ? '' : 'disabled';
      const reviseDisabled = canRevise ? '' : 'disabled';
      const approveTitle = reviewable ? '' : 'title="Available after generation when clip status is pending_review."';
      const needsRevisionTitle = reviewable ? '' : 'title="Available after generation when clip status is pending_review."';
      const reviseTitle = canRevise ? '' : 'title="Unavailable while clip is currently generating."';
      const previewBadge = previewAssetType ? `<span class="phase4-clip-preview-badge">${esc(previewAssetType)}</span>` : '';
      const narrationRow = narrationLine
        ? `<div class="phase4-clip-meta-row"><span class="phase4-clip-meta-label">Narration</span><span class="phase4-clip-meta-value">${esc(narrationLine)}</span></div>`
        : '';
      const promptRow = generationPrompt
        ? `<div class="phase4-clip-meta-row"><span class="phase4-clip-meta-label">Video Prompt</span><span class="phase4-clip-meta-value">${esc(generationPrompt)}</span></div>`
        : '';
      const modelRow = generationModel
        ? `<div class="phase4-clip-meta-row"><span class="phase4-clip-meta-label">Model</span><code class="phase4-clip-meta-code">${esc(generationModel)}</code></div>`
        : '';
      const transformRow = transformPrompt
        ? `<div class="phase4-clip-meta-row"><span class="phase4-clip-meta-label">Frame Edit</span><span class="phase4-clip-meta-value">${esc(transformPrompt)}</span></div>`
        : '';
      const startFrameTitle = startFrameFilename ? esc(startFrameFilename) : 'Start frame not resolved yet';
      const startFrameName = startFrameFilename
        ? (startFrameUrl
          ? `<a class="phase4-clip-start-frame-name" href="${esc(startFrameUrl)}" target="_blank" rel="noopener" title="${startFrameTitle}">${esc(startFrameFilename)}</a>`
          : `<span class="phase4-clip-start-frame-name" title="${startFrameTitle}">${esc(startFrameFilename)}</span>`)
        : '<span class="phase4-clip-start-frame-name phase4-clip-start-frame-empty">Not set</span>';
      const startFrameThumb = startFrameUrl
        ? `<img class="phase4-clip-start-frame-thumb" src="${esc(startFrameUrl)}" alt="Start frame" loading="lazy" onerror="this.style.display='none'" />`
        : '';
      const startFrameRow = `
        <div class="phase4-clip-meta-row phase4-clip-meta-row-start-frame">
          <span class="phase4-clip-meta-label">Start Frame</span>
          <div class="phase4-clip-start-frame">
            ${startFrameName}
            ${startFrameThumb}
          </div>
        </div>
      `;
      const metaBlock = `
        <div class="phase4-clip-meta">
          ${narrationRow}
          ${promptRow}
          ${modelRow}
          ${transformRow}
          ${startFrameRow}
        </div>
      `;
      const previewBlock = previewUrl
        ? `
          <div class="phase4-clip-preview">
            <video controls preload="metadata" src="${esc(previewUrl)}"></video>
            <a class="phase4-clip-preview-link" href="${esc(previewUrl)}" target="_blank" rel="noopener">Open MP4</a>
          </div>
        `
        : '';
      return `
        <div class="phase4-clip-row">
          <div class="phase4-clip-main">
            <strong>${esc(scriptLine)} · ${esc(mode)} ${previewBadge}</strong>
            <span class="phase4-clip-id" title="${esc(clipId)}">${esc(clipDisplayId)}</span>
            <span class="phase4-clip-state">${esc(status)}${revStatus ? ` / rev: ${esc(revStatus)}` : ''}</span>
            ${metaBlock}
            ${previewBlock}
          </div>
          <div class="phase4-clip-actions">
            <button class="btn btn-ghost btn-sm" onclick="phase4ReviewClip('${clipId}', 'approve')" ${approveDisabled} ${approveTitle}>Approve</button>
            <button class="btn btn-ghost btn-sm" onclick="phase4ReviewClip('${clipId}', 'needs_revision')" ${needsRevisionDisabled} ${needsRevisionTitle}>Needs Revision</button>
            <button class="btn btn-ghost btn-sm" onclick="phase4ReviseClip('${clipId}')" ${reviseDisabled} ${reviseTitle}>Revise + Regen</button>
          </div>
        </div>
      `;
    })
    .join('');
}

function phase4NextActionForState(state) {
  const key = String(state || '').trim().toLowerCase();
  if (key === 'draft') return 'Click Generate Brief.';
  if (key === 'brief_generated') return 'Review file list, then click Approve Brief.';
  if (key === 'brief_approved') return 'Choose Local Folder (or paste Drive URL), then click Validate Drive.';
  if (key === 'validating_assets') return 'Wait for validation to complete.';
  if (key === 'validation_failed') return 'Fix missing/invalid files, then Validate Drive again.';
  if (key === 'assets_validated') return 'Click Start Generation.';
  if (key === 'generating') return 'Wait for generation to finish.';
  if (key === 'review_pending') return 'Review each generated clip and approve or request revision.';
  if (key === 'completed') return 'Run is complete. Export/share assets when ready.';
  if (key === 'failed') return 'Review errors, revise clips, then start generation again.';
  return 'Follow Phase 4 steps in order: brief -> approve -> validate -> generate -> review.';
}

function phase4TruncateWords(text, maxWords = 16) {
  const raw = String(text || '').trim();
  if (!raw) return '';
  const parts = raw.split(/\s+/).filter(Boolean);
  if (parts.length <= maxWords) return raw;
  return `${parts.slice(0, maxWords).join(' ')}...`;
}

function phase4BuildShotBriefFromNarration(narration) {
  const text = String(narration || '').trim();
  const low = text.toLowerCase();
  const has = (...tokens) => tokens.some((token) => low.includes(String(token).toLowerCase()));
  let base = '';

  if (!text) base = 'Simple literal visual of this line in a realistic lifestyle setting.';
  if (has('hands won\'t stop shaking', 'shaking hands', 'hands shaking', 'jitters', 'shaking')) {
    base = 'Close-up of hands visibly trembling over a work desk, with a coffee mug in frame.';
  }
  else if (has('coffee', '10 am', 'spike', 'crash')) {
    base = 'Morning desk shot with coffee cup; subject looks tense/anxious as the crash sets in.';
  }
  else if (has('capsules', 'powders', 'mushroom supplements', 'tasted like')) {
    base = 'Tabletop shot of capsules/powder with a frustrated reaction from the subject.';
  }
  else if (has('dissolvable strip', 'under your tongue', 'sublingual', 'dissolve under your tongue')) {
    base = 'Macro shot of a dissolvable strip being placed under the tongue.';
  }
  else if (has('no brewing', 'five-second ritual', 'before you start your day')) {
    base = 'Fast morning routine shot: strip, quick prep, ready-to-go energy.';
  }
  else if (has('rating', 'stars', 'review', '2,800', '4.9', '4.5', '94%')) {
    base = 'Phone close-up showing strong star ratings and review counts.';
  }
  else if (has('headache', 'brain fog', 'irritable', 'irritability', 'foggy')) {
    base = 'Subject rubbing temples at desk, unfocused and uncomfortable.';
  }
  else if (has('felt nothing', 'nothing at all', 'expensive nothing', 'wasted money')) {
    base = 'Frustrated subject looking at supplement bottle and untouched pills.';
  }
  else if (has('bypassing your gut', 'gut', 'first-pass liver', 'bloodstream')) {
    base = 'Clean product-first shot emphasizing direct delivery (strip in foreground).';
  }
  else if (has('animus focus strips', 'focus strips')) {
    base = 'Hero product shot of Animus Focus Strips on a clean desk with natural light.';
  }
  else {
    base = `Literal visual of: ${phase4TruncateWords(text, 14)}`;
  }
  return `${base} Vertical 9:16 framing only.`;
}

function phase4BuildShotIdeaOptions({ narration = '', mode = 'b_roll', role = '' } = {}) {
  const text = String(narration || '').trim();
  const low = text.toLowerCase();
  const has = (...tokens) => tokens.some((token) => low.includes(String(token).toLowerCase()));

  if (String(role || '').trim() === 'avatar_master') {
    return [
      'Front-facing head-and-shoulders portrait, neutral expression, soft daylight, clean background.',
      'Desk setup medium close-up, subject centered, natural skin tone, no strong shadows.',
      'Simple indoor portrait with consistent look and wardrobe you want for all A-roll lines.',
    ];
  }

  if (mode === 'a_roll') {
    return [
      'Talking-head frame: subject to camera, chest-up, neutral background, natural light.',
      'Slight side-angle talking-head frame with clean depth and calm expression.',
      'Desk-based talking-head frame with minimal props and consistent appearance.',
    ];
  }

  if (has('hands won\'t stop shaking', 'shaking hands', 'hands shaking', 'jitters', 'shaking')) {
    return [
      'Close-up of trembling hands above desk with a coffee mug and keyboard edge in frame.',
      'Top-down desk shot showing shaky hands trying to type or hold a pen.',
      'Side close-up of hands and forearms visibly jittery with blurred work background.',
    ];
  }
  if (has('capsules', 'powders', 'mushroom supplements', 'tasted like')) {
    return [
      'Tabletop capsules and powder with subject reacting in frustration in background.',
      'Hand holding capsules over table while subject makes a disgusted expression.',
      'Powder spoon + capsules + glass of water arranged messily to signal disappointment.',
    ];
  }
  if (has('dissolvable strip', 'under your tongue', 'sublingual', 'dissolve under your tongue')) {
    return [
      'Tight close-up of hand placing strip under tongue in one clear motion.',
      'Mirror-angle shot showing face and hand during strip placement.',
      'Macro product-to-mouth moment with clean bathroom or kitchen context.',
    ];
  }
  if (has('no brewing', 'five-second ritual', 'before you start your day', 'morning routine')) {
    return [
      'Morning counter shot: strip moment plus keys/phone ready to leave.',
      'Fast routine scene with subject grabbing essentials and looking energized.',
      'Kitchen-to-door transition frame that communicates quick, no-prep ritual.',
    ];
  }
  if (has('rating', 'stars', 'review', '2,800', '4.9', '4.5', '94%')) {
    return [
      'Phone close-up showing high star rating layout and review count style UI.',
      'Hand-held phone angle with ratings in focus and face reaction softly blurred behind.',
      'Over-shoulder shot of phone review page with clear positive social proof feel.',
    ];
  }
  if (has('headache', 'brain fog', 'irritable', 'irritability', 'foggy')) {
    return [
      'Subject rubbing temples at desk with tired eyes and unfocused posture.',
      'Workspace medium shot showing mental fatigue and slight slouch.',
      'Close-up face + hand at forehead with stressed expression and muted lighting.',
    ];
  }
  if (has('felt nothing', 'nothing at all', 'expensive nothing', 'wasted money')) {
    return [
      'Subject staring at supplement bottle with disappointed expression.',
      'Unopened or barely used supplement products on table, subject frustrated.',
      'Receipt/product combo shot suggesting wasted spend and no results.',
    ];
  }

  return [
    `${phase4BuildShotBriefFromNarration(text || 'Literal visual of the narration line.')}`,
    'Same scene from a tighter close-up that emphasizes emotion or product interaction.',
    'Same scene with a cleaner background and stronger subject focus for AI generation.',
  ];
}

function phase4BuildGuideRenderContext() {
  const detail = phase4CurrentRunDetail;
  if (!detail || !detail.run) return null;
  const run = detail.run || {};
  const state = String(run.workflow_state || run.status || 'unknown');
  const nextAction = phase4NextActionForState(state);
  const brief = detail.start_frame_brief && typeof detail.start_frame_brief === 'object'
    ? detail.start_frame_brief
    : {};
  const approval = detail.start_frame_brief_approval && typeof detail.start_frame_brief_approval === 'object'
    ? detail.start_frame_brief_approval
    : {};
  const required = Array.isArray(brief.required_items) ? brief.required_items : [];
  const optional = Array.isArray(brief.optional_items) ? brief.optional_items : [];
  const approved = Boolean(approval.approved);
  const runId = String(run.video_run_id || '').trim();
  const checkedSet = phase4GetCheckedSet(runId);
  const clips = Array.isArray(detail.clips) ? detail.clips : [];

  const bySceneLine = new Map();
  clips.forEach((clip) => {
    const sid = String(clip.scene_line_id || '').trim();
    if (!sid) return;
    bySceneLine.set(sid, clip);
  });

  return {
    nextAction,
    required,
    optional,
    approved,
    runId,
    checkedSet,
    bySceneLine,
  };
}

function phase4BuildGuideRowsHtml(context) {
  const { required, runId, checkedSet, bySceneLine } = context;
  return required
    .map((item) => {
      const fileName = String(item.filename || '');
      const fileNameEncoded = encodeURIComponent(fileName);
      const runIdEncoded = encodeURIComponent(runId);
      const checked = checkedSet.has(fileName);
      const rowClass = checked ? 'phase4-guide-row is-done' : 'phase4-guide-row';
      const doneChip = checked ? '&#10003;' : '';
      const onToggle = `phase4ToggleGuideRow('${runIdEncoded}','${fileNameEncoded}')`;
      const onCopy = `phase4CopyGuideFilename(event,'${fileNameEncoded}')`;
      const role = String(item.file_role || '');
      if (role === 'avatar_master') {
        const ideas = phase4BuildShotIdeaOptions({ role, mode: 'a_roll' });
        const ideasHtml = ideas
          .map((idea) => `<li>${esc(idea)} Vertical 9:16 framing only.</li>`)
          .join('');
        return `
          <tr class="${rowClass}">
            <td class="phase4-guide-done phase4-guide-cell-toggle" onclick="${onToggle}">${doneChip}</td>
            <td class="phase4-guide-file phase4-guide-file-copy" onclick="${onCopy}" title="Click to copy filename">${esc(fileName)}</td>
            <td class="phase4-guide-cell-toggle" onclick="${onToggle}">
              <div class="phase4-guide-visual">A-roll avatar master image: front-facing subject, consistent look, clean lighting/background. Vertical 9:16 framing only.</div>
              <div class="phase4-guide-ideas-label">Idea options:</div>
              <ul class="phase4-guide-ideas">${ideasHtml}</ul>
            </td>
          </tr>
        `;
      }
      const sid = String(item.scene_line_id || '').trim();
      const clip = sid ? bySceneLine.get(sid) : null;
      const narration = String(clip?.narration_text || '').trim();
      const fallback = String(item.rationale || '').trim();
      const visualHint = phase4BuildShotBriefFromNarration(narration || fallback);
      const ideas = phase4BuildShotIdeaOptions({
        narration: narration || fallback,
        mode: String(item.mode || 'b_roll'),
        role,
      });
      const ideasHtml = ideas
        .map((idea) => `<li>${esc(idea)}${String(idea).toLowerCase().includes('9:16') ? '' : ' Vertical 9:16 framing only.'}</li>`)
        .join('');
      const sourceText = narration || fallback;
      return `
        <tr class="${rowClass}">
          <td class="phase4-guide-done phase4-guide-cell-toggle" onclick="${onToggle}">${doneChip}</td>
          <td class="phase4-guide-file phase4-guide-file-copy" onclick="${onCopy}" title="Click to copy filename">${esc(fileName)}</td>
          <td class="phase4-guide-cell-toggle" onclick="${onToggle}">
            <div class="phase4-guide-visual">${esc(visualHint)}</div>
            <div class="phase4-guide-ideas-label">Idea options:</div>
            <ul class="phase4-guide-ideas">${ideasHtml}</ul>
            ${sourceText ? `<div class="phase4-guide-source">Source line: ${esc(sourceText)}</div>` : ''}
          </td>
        </tr>
      `;
    })
    .join('');
}

function phase4BuildGuideTableHtml(context, options = {}) {
  const forFullscreen = Boolean(options.forFullscreen);
  const rowsHtml = phase4BuildGuideRowsHtml(context);
  const toolbar = forFullscreen
    ? ''
    : `
      <div class="phase4-guide-toolbar">
        <button class="btn btn-ghost btn-sm" onclick="phase4OpenGuideFullscreen()">View Full Screen</button>
      </div>
    `;
  const tableWrapClass = forFullscreen
    ? 'phase4-guide-table-wrap phase4-guide-table-wrap-fullscreen'
    : 'phase4-guide-table-wrap';

  return `
    ${toolbar}
    <div class="${tableWrapClass}">
      <table class="phase4-guide-table">
        <thead>
          <tr>
            <th>Done</th>
            <th>Filename</th>
            <th>What To Upload</th>
          </tr>
        </thead>
        <tbody>
          ${rowsHtml}
        </tbody>
      </table>
    </div>
  `;
}

function phase4BuildGuideMarkup(context, options = {}) {
  const forFullscreen = Boolean(options.forFullscreen);
  const shellClass = forFullscreen ? 'phase4-guide-shell phase4-guide-shell-fullscreen' : 'phase4-guide-shell';
  const checkedCount = context.required.filter((item) => context.checkedSet.has(String(item.filename || ''))).length;
  if (!context.required.length) {
    return `
      <div class="${shellClass}">
        <div class="phase4-guide-title">Start Frame Guide</div>
        <div class="phase4-guide-status">Brief not generated yet.</div>
        <div class="phase4-guide-next"><strong>Next:</strong> ${esc(context.nextAction)}</div>
        <div class="phase4-guide-empty">When you click <strong>Generate Brief</strong>, this section will show exact filenames and what each image should depict.</div>
      </div>
    `;
  }

  return `
    <div class="${shellClass}">
      <div class="phase4-guide-title">Start Frame Guide</div>
      <div class="phase4-guide-status">Required: ${context.required.length} files · Checked: ${checkedCount}/${context.required.length} · Optional: ${context.optional.length} · Brief approved: ${context.approved ? 'yes' : 'no'} · Aspect: 9:16 only</div>
      <div class="phase4-guide-next"><strong>Next:</strong> ${esc(context.nextAction)}</div>
      <div class="phase4-guide-tip">Tip: tap a row to mark that file as done.</div>
      ${phase4BuildGuideTableHtml(context, { forFullscreen })}
    </div>
  `;
}

function phase4OpenGuideFullscreen() {
  const context = phase4BuildGuideRenderContext();
  if (!context) return;
  const modal = document.getElementById('output-fullscreen-modal');
  const body = document.getElementById('output-fullscreen-body');
  const title = document.getElementById('output-fullscreen-title');
  if (!modal || !body || !title) return;
  title.textContent = 'Phase 4 Start Frame Guide — Full Screen';
  body.innerHTML = phase4BuildGuideMarkup(context, { forFullscreen: true });
  setOutputFullscreenContext(OUTPUT_FULLSCREEN_CONTEXT_PHASE4_GUIDE);
  modal.classList.remove('hidden');
}

function phase4RenderGuideFullscreenIfOpen() {
  const modal = document.getElementById('output-fullscreen-modal');
  const body = document.getElementById('output-fullscreen-body');
  if (!modal || !body) return;
  if (modal.classList.contains('hidden')) return;
  if (getOutputFullscreenContext() !== OUTPUT_FULLSCREEN_CONTEXT_PHASE4_GUIDE) return;
  const context = phase4BuildGuideRenderContext();
  if (!context) {
    closeOutputFullscreenModal();
    return;
  }
  body.innerHTML = phase4BuildGuideMarkup(context, { forFullscreen: true });
}

function phase4RenderStartFrameGuide() {
  const guideEl = document.getElementById('phase4-v1-guide');
  if (!guideEl) return;
  const context = phase4BuildGuideRenderContext();
  if (!context) {
    guideEl.textContent = 'Create or select a Phase 4 run to see the upload guide.';
    phase4RenderGuideFullscreenIfOpen();
    return;
  }
  guideEl.innerHTML = phase4BuildGuideMarkup(context, { forFullscreen: false });
  phase4RenderGuideFullscreenIfOpen();
}

async function phase4RefreshForActiveBranch(force = false) {
  if (!phase4V1Enabled || !activeBranchId || !activeBrandSlug) {
    phase4StopPolling();
    if (force) phase4ResetStateForBranch();
    return;
  }
  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/runs${phase4BrandParam()}`);
    const data = await resp.json();
    if (!resp.ok) {
      throw new Error(data.error || `Failed to load Phase 4 runs (HTTP ${resp.status})`);
    }
    phase4RunsCache = Array.isArray(data) ? data : [];
    phase4RenderRunSelect();
    if (!phase4CurrentRunId && phase4RunsCache.length) {
      phase4CurrentRunId = String(phase4RunsCache[0].video_run_id || '');
    }
    if (phase4CurrentRunId) {
      await phase4LoadRunDetail(phase4CurrentRunId, { silent: true });
    } else {
      phase4CurrentRunDetail = null;
      phase4RenderCurrentRun();
      phase4StopPolling();
    }
  } catch (e) {
    console.error(e);
    if (!force) {
      alert(formatFetchError(e, 'phase4 run list'));
    }
  }
}

async function phase4SelectRun(runId) {
  phase4CurrentRunId = String(runId || '').trim();
  if (!phase4CurrentRunId) {
    phase4StopPolling();
    phase4CurrentRunDetail = null;
    phase4RenderCurrentRun();
    return;
  }
  await phase4LoadRunDetail(phase4CurrentRunId);
}

async function phase4LoadRunDetail(runId, options = {}) {
  const silent = Boolean(options.silent);
  if (!activeBranchId || !activeBrandSlug || !runId) return;
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(runId)}${phase4BrandParam()}`
    );
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `Failed to load Phase 4 detail (HTTP ${resp.status})`);
    phase4CurrentRunDetail = data;
    phase4CurrentRunId = String(runId || '');
    phase4RenderRunSelect();
    phase4RenderCurrentRun();
    phase4SyncPollingFromDetail(data);
  } catch (e) {
    if (!silent) alert(formatFetchError(e, 'phase4 run detail'));
  }
}

async function phase4CreateRun() {
  if (!activeBranchId || !activeBrandSlug) {
    alert('Select a branch first.');
    return;
  }
  const phase3Input = document.getElementById('phase4-v1-phase3-run-id');
  const voiceSelect = document.getElementById('phase4-v1-voice-select');
  const typedPhase3RunId = String(phase3Input?.value || '').trim();
  const inferredPhase3RunId = String(phase3V2CurrentRunId || '').trim();
  let phase3RunId = typedPhase3RunId;
  const voicePresetId = String(voiceSelect?.value || '').trim();
  if (!phase3RunId && inferredPhase3RunId) {
    const useInferred = confirm(
      `Phase 3 Run ID is blank.\n\nUse currently selected Phase 3 run?\n${inferredPhase3RunId}`
    );
    if (!useInferred) {
      if (phase3Input) phase3Input.focus();
      return;
    }
    phase3RunId = inferredPhase3RunId;
    if (phase3Input) phase3Input.value = phase3RunId;
  }
  if (!phase3RunId) {
    alert('Enter a Phase 3 run ID first.');
    if (phase3Input) phase3Input.focus();
    return;
  }
  if (!voicePresetId) {
    alert('Select a voice preset first.');
    return;
  }
  try {
    const detailResp = await fetch(
      `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3RunId)}${phase3V2BrandParam()}`
    );
    const detailData = await detailResp.json();
    if (detailResp.ok) {
      const items = Array.isArray(detailData?.production_handoff_packet?.items)
        ? detailData.production_handoff_packet.items
        : [];
      const clipEstimate = items.reduce((total, item) => {
        const lines = Array.isArray(item?.lines) ? item.lines : [];
        return total + lines.length;
      }, 0);
      if (clipEstimate > 0) {
        const proceed = confirm(
          `This Phase 3 run will create ${clipEstimate} clip slots.\n\nContinue creating the Phase 4 run?`
        );
        if (!proceed) return;
      }
    }
  } catch (e) {
    console.warn('Phase 4 preflight clip estimate failed', e);
  }
  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/runs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: activeBrandSlug,
        phase3_run_id: phase3RunId,
        voice_preset_id: voicePresetId,
      }),
    });
    const raw = await resp.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch (_err) {
      throw new Error(`Server returned non-JSON response (HTTP ${resp.status}).`);
    }
    if (!resp.ok) throw new Error(data.error || `Create run failed (HTTP ${resp.status})`);
    phase4CurrentRunId = String(data?.run?.video_run_id || data?.manifest?.video_run_id || '');
    await phase4RefreshForActiveBranch(true);
  } catch (e) {
    alert(formatFetchError(e, 'create phase4 run'));
  }
}

async function phase4GenerateBrief() {
  if (!phase4CurrentRunId || !activeBranchId) return;
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(phase4CurrentRunId)}/start-frame-brief/generate`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '' }),
      },
    );
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `Generate brief failed (HTTP ${resp.status})`);
    await phase4LoadRunDetail(phase4CurrentRunId, { silent: true });
    alert('Start-frame brief generated.');
  } catch (e) {
    alert(formatFetchError(e, 'generate start-frame brief'));
  }
}

async function phase4ApproveBrief() {
  if (!phase4CurrentRunId || !activeBranchId) return;
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(phase4CurrentRunId)}/start-frame-brief/approve`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '', approved_by: 'operator' }),
      },
    );
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `Approve brief failed (HTTP ${resp.status})`);
    await phase4LoadRunDetail(phase4CurrentRunId, { silent: true });
  } catch (e) {
    alert(formatFetchError(e, 'approve start-frame brief'));
  }
}

function phase4PickLocalFolder() {
  if (!phase4CurrentRunId || !activeBranchId) {
    alert('Create/select a Phase 4 run first.');
    return;
  }
  const picker = document.getElementById('phase4-v1-local-folder-input');
  if (!picker) {
    alert('Local folder picker is not available in this browser.');
    return;
  }
  picker.click();
}

async function phase4HandleLocalFolderPicked(event) {
  const picker = event?.target;
  const files = Array.from(picker?.files || []);
  if (!files.length) return;
  if (!phase4CurrentRunId || !activeBranchId) {
    alert('Create/select a Phase 4 run first.');
    if (picker) picker.value = '';
    return;
  }

  const form = new FormData();
  form.append('brand', activeBrandSlug || '');
  files.forEach((file) => form.append('files', file, file.name));

  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(phase4CurrentRunId)}/drive/local-folder-ingest`,
      {
        method: 'POST',
        body: form,
      },
    );
    const raw = await resp.text();
    let data = {};
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch (_err) {
      throw new Error(`Folder ingest returned non-JSON response (HTTP ${resp.status}).`);
    }
    if (!resp.ok) throw new Error(data.error || `Local folder ingest failed (HTTP ${resp.status})`);

    const driveInput = document.getElementById('phase4-v1-drive-url');
    if (driveInput) {
      driveInput.value = String(data.folder_path || '');
    }
    alert(`Folder selected. Staged ${parseInt(data.file_count, 10) || files.length} files. Click Validate Drive.`);
  } catch (e) {
    alert(formatFetchError(e, 'local folder ingest'));
  } finally {
    if (picker) picker.value = '';
  }
}

async function phase4ValidateDrive() {
  if (!phase4CurrentRunId || !activeBranchId) return;
  const input = document.getElementById('phase4-v1-drive-url');
  const folderUrl = String(input?.value || '').trim();
  if (!folderUrl) {
    alert('Enter a Drive folder URL (or local folder path in test mode).');
    return;
  }
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(phase4CurrentRunId)}/drive/validate`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '', folder_url: folderUrl }),
      },
    );
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `Validation failed (HTTP ${resp.status})`);
    await phase4LoadRunDetail(phase4CurrentRunId, { silent: true });
    alert(data.status === 'passed' ? 'Drive validation passed.' : 'Drive validation failed. Check issues.');
  } catch (e) {
    alert(formatFetchError(e, 'validate drive assets'));
  }
}

async function phase4StartGeneration() {
  if (!phase4CurrentRunId || !activeBranchId) return;
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(phase4CurrentRunId)}/generation/start`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '' }),
      },
    );
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `Start generation failed (HTTP ${resp.status})`);
    phase4StartPolling(phase4CurrentRunId, { graceMs: 30000 });
    await phase4LoadRunDetail(phase4CurrentRunId, { silent: true });
    setTimeout(() => {
      if (!phase4CurrentRunId) return;
      phase4LoadRunDetail(phase4CurrentRunId, { silent: true });
    }, 1200);
  } catch (e) {
    alert(formatFetchError(e, 'start generation'));
  }
}

async function phase4ReviewClip(clipId, decision) {
  if (!phase4CurrentRunId || !activeBranchId || !clipId) return;
  let note = '';
  if (decision === 'needs_revision') {
    note = prompt('Revision note (required):', '') || '';
    if (!note.trim()) {
      alert('A revision note is required.');
      return;
    }
  }
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(phase4CurrentRunId)}/clips/${encodeURIComponent(clipId)}/review${phase4BrandParam()}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ decision, note }),
      },
    );
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `Review failed (HTTP ${resp.status})`);
    await phase4LoadRunDetail(phase4CurrentRunId, { silent: true });
  } catch (e) {
    alert(formatFetchError(e, 'clip review'));
  }
}

async function phase4ReviseClip(clipId) {
  if (!phase4CurrentRunId || !activeBranchId || !clipId) return;
  const note = prompt('Revision note:', '') || '';
  const transformPrompt = prompt('Optional transform prompt (leave blank to skip):', '') || '';
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(phase4CurrentRunId)}/clips/${encodeURIComponent(clipId)}/revise${phase4BrandParam()}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ note, transform_prompt: transformPrompt }),
      },
    );
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || `Revise failed (HTTP ${resp.status})`);
    await phase4LoadRunDetail(phase4CurrentRunId, { silent: true });
    alert('Revision created. Start generation again to render the new revision.');
  } catch (e) {
    alert(formatFetchError(e, 'revise clip'));
  }
}

// -----------------------------------------------------------
// STORYBOARD (PHASE 4 V1)
// -----------------------------------------------------------

function storyboardBrandParam() {
  return activeBrandSlug ? `?brand=${encodeURIComponent(activeBrandSlug)}` : '';
}

function storyboardResolvePhase3RunId() {
  return String(
    phase3V2CurrentRunId
    || phase3V2CurrentRunDetail?.run?.run_id
    || phase3V2CurrentRunDetail?.run_id
    || phase3V2RunsCache?.[0]?.run_id
    || ''
  ).trim();
}

function storyboardBuildInitKey() {
  return `${activeBrandSlug || ''}:${activeBranchId || ''}:${storyboardResolvePhase3RunId() || ''}`;
}

function storyboardSetStatus(phaseText = 'Idle', summaryText = '') {
  const phaseEl = document.getElementById('storyboard-status-phase');
  const summaryEl = document.getElementById('storyboard-status-summary');
  if (phaseEl) phaseEl.textContent = phaseText || 'Idle';
  if (summaryEl) summaryEl.textContent = summaryText || 'Upload A-roll and B-roll images, then click Continue.';
}

function storyboardAddActivity(text, tone = '') {
  const trimmed = String(text || '').trim();
  if (!trimmed) return;
  storyboardActivityLog.push({
    at: new Date().toISOString(),
    text: trimmed,
    tone: String(tone || '').trim(),
  });
  if (storyboardActivityLog.length > 200) storyboardActivityLog = storyboardActivityLog.slice(-200);
  storyboardRenderActivityLog();
}

function storyboardBuildLiveServerLogItems() {
  const lines = Array.isArray(serverLogSeenOrder) ? serverLogSeenOrder : [];
  return lines.map((line) => {
    const raw = String(line || '').trim();
    if (!raw) {
      return { timeText: '', text: '', toneClass: '' };
    }
    const match = raw.match(/^(\d{1,2}:\d{2}:\d{2}(?:\s*[AP]M)?)\s+(.*)$/i);
    const timeText = match ? String(match[1] || '').trim() : '';
    const body = match ? String(match[2] || '').trim() : raw;
    const lower = body.toLowerCase();
    let toneClass = '';
    if (lower.includes('error') || lower.includes('failed')) toneClass = 'error';
    else if (lower.includes('warn') || lower.includes('retry')) toneClass = 'warn';
    else if (lower.includes('completed') || lower.includes('success')) toneClass = 'success';
    return { timeText, text: body, toneClass };
  }).filter((row) => row.text);
}

function storyboardRenderActivitySourceToggle() {
  const activityBtn = document.getElementById('storyboard-activity-source-activity');
  const liveBtn = document.getElementById('storyboard-activity-source-live');
  if (activityBtn) activityBtn.classList.toggle('active', storyboardActivitySource === 'activity');
  if (liveBtn) liveBtn.classList.toggle('active', storyboardActivitySource === 'live_server');
}

function storyboardSetActivitySource(source = 'activity') {
  const normalized = source === 'live_server' ? 'live_server' : 'activity';
  storyboardActivitySource = normalized;
  storyboardRenderActivitySourceToggle();
  storyboardRenderActivityLog();
}

function storyboardActivityListIsNearBottom(list) {
  if (!list) return true;
  const distance = list.scrollHeight - list.scrollTop - list.clientHeight;
  return distance <= 48;
}

function storyboardEnsureActivityScrollTracking() {
  const list = document.getElementById('storyboard-activity-list');
  if (!list || list.dataset.scrollTracking === '1') return;
  list.dataset.scrollTracking = '1';
  list.addEventListener('scroll', () => {
    const sourceKey = storyboardActivitySource === 'live_server' ? 'live_server' : 'activity';
    storyboardActivityAutoFollowBySource[sourceKey] = storyboardActivityListIsNearBottom(list);
  });
}

function storyboardRenderActivityLog() {
  const list = document.getElementById('storyboard-activity-list');
  if (!list) return;
  storyboardEnsureActivityScrollTracking();
  const showingLiveServer = storyboardActivitySource === 'live_server';
  const sourceKey = showingLiveServer ? 'live_server' : 'activity';
  const previousScrollTop = list.scrollTop;
  const shouldFollow = Boolean(storyboardActivityAutoFollowBySource[sourceKey] || storyboardActivityListIsNearBottom(list));
  const rows = showingLiveServer
    ? storyboardBuildLiveServerLogItems()
    : [...storyboardActivityLog].sort((a, b) => {
      const atA = new Date(String(a?.at || '')).getTime();
      const atB = new Date(String(b?.at || '')).getTime();
      if (Number.isNaN(atA) && Number.isNaN(atB)) return 0;
      if (Number.isNaN(atA)) return -1;
      if (Number.isNaN(atB)) return 1;
      return atA - atB;
    }).map((row) => {
      const ts = new Date(String(row.at || ''));
      const timeText = Number.isNaN(ts.getTime())
        ? ''
        : ts.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      const toneClass = ['success', 'warn', 'error'].includes(row.tone) ? row.tone : '';
      return { timeText, text: String(row.text || ''), toneClass };
    });

  if (!rows.length) {
    list.innerHTML = `<div class="storyboard-activity-empty">${
      showingLiveServer ? 'No live server logs yet.' : 'No activity yet.'
    }</div>`;
    storyboardActivityAutoFollowBySource[sourceKey] = true;
    return;
  }
  list.innerHTML = rows.map((row) => {
    return `
      <div class="storyboard-activity-item ${esc(row.toneClass || '')}">
        <div class="storyboard-activity-time">${esc(String(row.timeText || ''))}</div>
        <div class="storyboard-activity-text">${esc(String(row.text || ''))}</div>
      </div>
    `;
  }).join('');
  if (shouldFollow) {
    list.scrollTop = list.scrollHeight;
    storyboardActivityAutoFollowBySource[sourceKey] = true;
  } else {
    list.scrollTop = previousScrollTop;
  }
}

function toggleActivityLog() {
  const drawer = document.getElementById('storyboard-activity-drawer');
  const btn = document.getElementById('storyboard-activity-toggle-btn');
  if (!drawer) return;
  storyboardActivityDrawerOpen = !storyboardActivityDrawerOpen;
  drawer.classList.toggle('hidden', !storyboardActivityDrawerOpen);
  document.body.classList.toggle('storyboard-activity-open', storyboardActivityDrawerOpen);
  if (btn) btn.classList.toggle('is-open', storyboardActivityDrawerOpen);
  if (storyboardActivityDrawerOpen) {
    storyboardRenderActivitySourceToggle();
    storyboardRenderActivityLog();
  }
}

function storyboardCloseActivityDrawer() {
  const drawer = document.getElementById('storyboard-activity-drawer');
  const btn = document.getElementById('storyboard-activity-toggle-btn');
  storyboardActivityDrawerOpen = false;
  if (drawer) drawer.classList.add('hidden');
  document.body.classList.remove('storyboard-activity-open');
  if (btn) btn.classList.remove('is-open');
}

function storyboardClearActivityLog() {
  if (storyboardActivitySource === 'live_server') {
    clearServerLog();
  } else {
    storyboardActivityLog = [];
  }
  storyboardRenderActivityLog();
}

function storyboardFormatBytes(bytes) {
  const n = Math.max(0, Number(bytes) || 0);
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function storyboardFormatAddedAt(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const dt = new Date(raw);
  if (Number.isNaN(dt.getTime())) return raw;
  return dt.toLocaleString();
}

function storyboardBrollMetadata(row) {
  return row?.metadata && typeof row.metadata === 'object' ? row.metadata : {};
}

function storyboardBrollTags(row) {
  const metadata = storyboardBrollMetadata(row);
  if (!Array.isArray(metadata.tags)) return [];
  return metadata.tags
    .map((tag) => String(tag || '').trim())
    .filter(Boolean);
}

function storyboardBrollDisplayType(row) {
  const direct = String(row?.display_type || '').trim();
  if (direct) return direct;
  const metadata = storyboardBrollMetadata(row);
  const modeHint = String(metadata.mode_hint || '').trim().toLowerCase();
  const aiGenerated = Boolean(metadata.ai_generated);
  if (aiGenerated && (modeHint === 'b_roll' || modeHint === 'animation_broll')) return 'ai modified broll';
  if (modeHint === 'a_roll') return 'a roll';
  if (modeHint === 'animation_broll') return 'animation broll';
  return 'broll';
}

function storyboardBrollIsOriginal(row) {
  const metadata = storyboardBrollMetadata(row);
  const rawType = String(metadata.library_item_type || '').trim().toLowerCase();
  if (rawType) return rawType === 'original_upload';
  return !Boolean(metadata.ai_generated);
}

function storyboardBrollMatchesFilter(row, filter) {
  const key = String(filter || 'all').trim().toLowerCase();
  if (!key || key === 'all') return true;
  const displayType = storyboardBrollDisplayType(row);
  if (key === 'ai_modified') return displayType === 'ai modified broll';
  if (key === 'original') return storyboardBrollIsOriginal(row);
  if (key === 'type_a_roll') return displayType === 'a roll';
  if (key === 'type_broll') return displayType === 'broll';
  if (key === 'type_animation') return displayType === 'animation broll';
  return true;
}

function storyboardBrollMatchesSearch(row, searchTerm) {
  const q = String(searchTerm || '').trim().toLowerCase();
  if (!q) return true;
  const fileName = String(row?.file_name || '').toLowerCase();
  if (fileName.includes(q)) return true;
  const tags = storyboardBrollTags(row);
  return tags.some((tag) => String(tag || '').toLowerCase().includes(q));
}

function storyboardUpdateExpandedFilterButtons() {
  const wrap = document.getElementById('storyboard-broll-expanded-filters');
  if (!wrap) return;
  wrap.querySelectorAll('button[data-filter]').forEach((btn) => {
    const key = String(btn.getAttribute('data-filter') || '').trim().toLowerCase();
    btn.classList.toggle('active', key === String(storyboardBrollExpandedFilter || 'all').trim().toLowerCase());
  });
}

function storyboardRenderExpandedBrollLibrary() {
  const summaryEl = document.getElementById('storyboard-broll-expanded-summary');
  const gridEl = document.getElementById('storyboard-broll-expanded-grid');
  const searchEl = document.getElementById('storyboard-broll-expanded-search');
  if (!summaryEl || !gridEl) return;
  if (searchEl && String(searchEl.value || '') !== String(storyboardBrollExpandedSearch || '')) {
    searchEl.value = String(storyboardBrollExpandedSearch || '');
  }
  storyboardUpdateExpandedFilterButtons();

  const rows = Array.isArray(storyboardBrollFiles) ? storyboardBrollFiles : [];
  const filtered = rows.filter((row) => {
    if (!row || typeof row !== 'object') return false;
    if (!storyboardBrollMatchesFilter(row, storyboardBrollExpandedFilter)) return false;
    if (!storyboardBrollMatchesSearch(row, storyboardBrollExpandedSearch)) return false;
    return true;
  });
  summaryEl.textContent = `${filtered.length}/${rows.length} image${rows.length === 1 ? '' : 's'} shown`;

  if (!filtered.length) {
    gridEl.innerHTML = '<div class="storyboard-broll-expanded-empty">No images match the current filter.</div>';
    return;
  }

  gridEl.innerHTML = filtered.map((row) => {
    const fileName = String(row?.file_name || '').trim();
    const displayType = storyboardBrollDisplayType(row);
    const tags = storyboardBrollTags(row);
    const thumbUrl = String(row?.thumbnail_url || '').trim();
    const encodedName = encodeURIComponent(fileName);
    const encodedThumbUrl = encodeURIComponent(thumbUrl);
    const encodedLabel = encodeURIComponent(`${fileName} · ${displayType}`);
    const overlayTagHtml = tags.length
      ? tags.slice(0, 4).map((tag) => `<span class="storyboard-broll-tag">${esc(tag)}</span>`).join('')
      : '<span class="storyboard-broll-tag empty">no tags</span>';
    const bodyTagHtml = tags.length
      ? tags.slice(0, 6).map((tag) => `<span class="storyboard-broll-tag">${esc(tag)}</span>`).join('')
      : '<span class="storyboard-broll-tag empty">no tags</span>';
    return `
      <article class="storyboard-broll-card">
        <div class="storyboard-broll-card-thumb-wrap">
          ${thumbUrl
            ? `<img class="storyboard-broll-card-thumb" src="${esc(thumbUrl)}" alt="${esc(fileName)}">`
            : '<div class="storyboard-broll-card-thumb-empty">No preview</div>'
          }
          <div class="storyboard-broll-card-quick-actions">
            <button class="storyboard-broll-quick-btn" onclick="event.stopPropagation();storyboardRenameBrollFile('${encodedName}')">Rename</button>
            <button class="storyboard-broll-quick-btn danger" onclick="event.stopPropagation();storyboardRemoveBrollFile('${encodedName}')">Delete</button>
          </div>
          <div class="storyboard-broll-card-overlay">
            <div class="storyboard-broll-card-overlay-file" title="${esc(fileName)}">${esc(fileName)}</div>
            <div class="storyboard-broll-card-overlay-tags">${overlayTagHtml}</div>
          </div>
        </div>
        <div class="storyboard-broll-card-body">
          <div class="storyboard-broll-card-file" title="${esc(fileName)}">${esc(fileName)}</div>
          <div class="storyboard-broll-card-type">${esc(displayType)}</div>
          <div class="storyboard-broll-card-tags">${bodyTagHtml}</div>
          <div class="storyboard-broll-card-actions">
            <button class="btn btn-ghost btn-sm" ${thumbUrl ? `onclick="storyboardOpenImageModal('${encodedThumbUrl}','${encodedLabel}')"` : 'disabled'}>Open</button>
            <button class="btn btn-ghost btn-sm" onclick="storyboardRenameBrollFile('${encodedName}')">Rename</button>
            <button class="btn btn-ghost btn-sm" onclick="storyboardEditBrollTags('${encodedName}')">Edit Tags</button>
            <button class="btn btn-ghost btn-sm" onclick="storyboardRemoveBrollFile('${encodedName}')">Delete</button>
          </div>
        </div>
      </article>
    `;
  }).join('');
}

function storyboardOpenBrollLibraryExpanded() {
  if (!activeBrandSlug || !activeBranchId) {
    alert('Select a brand and branch first.');
    return;
  }
  const modal = document.getElementById('storyboard-broll-expanded-modal');
  if (!modal) return;
  storyboardBrollExpandedOpen = true;
  modal.classList.remove('hidden');
  storyboardRenderExpandedBrollLibrary();
}

function storyboardCloseBrollLibraryExpanded() {
  const modal = document.getElementById('storyboard-broll-expanded-modal');
  storyboardBrollExpandedOpen = false;
  if (modal) modal.classList.add('hidden');
}

function storyboardSetExpandedFilter(filterKey = 'all') {
  storyboardBrollExpandedFilter = String(filterKey || 'all').trim().toLowerCase() || 'all';
  storyboardRenderExpandedBrollLibrary();
}

function storyboardHandleExpandedSearch(event) {
  storyboardBrollExpandedSearch = String(event?.target?.value || '').trim();
  storyboardRenderExpandedBrollLibrary();
}

function storyboardOpenExpandedAddImages() {
  if (!activeBrandSlug || !activeBranchId) {
    alert('Select a brand and branch first.');
    return;
  }
  const picker = document.getElementById('storyboard-library-file-input');
  if (!picker) return;
  picker.click();
}

function storyboardRemoveBrollFileFromCompact(event, encodedName) {
  if (event && typeof event.stopPropagation === 'function') event.stopPropagation();
  storyboardRemoveBrollFile(encodedName);
}

function storyboardRenderBrollLibrary() {
  const libraryEl = document.getElementById('storyboard-broll-library');
  const selectedEl = document.getElementById('storyboard-broll-selected');
  if (!libraryEl || !selectedEl) return;

  const count = Array.isArray(storyboardBrollFiles) ? storyboardBrollFiles.length : 0;
  selectedEl.textContent = count > 0
    ? `${count} B-roll image${count === 1 ? '' : 's'} saved for this brand.`
    : 'No B-roll images saved.';

  if (!count) {
    libraryEl.innerHTML = '<div class="storyboard-broll-library-empty">Upload images to build your B-roll library.</div>';
    if (storyboardBrollExpandedOpen) storyboardRenderExpandedBrollLibrary();
    return;
  }

  libraryEl.innerHTML = storyboardBrollFiles.map((row) => {
    const fileName = String(row?.file_name || '').trim();
    const sizeText = storyboardFormatBytes(row?.size_bytes || 0);
    const addedText = storyboardFormatAddedAt(row?.added_at || '');
    const displayType = storyboardBrollDisplayType(row);
    const encodedName = encodeURIComponent(fileName);
    return `
      <div class="storyboard-broll-library-item">
        <div class="storyboard-broll-library-item-main">
          <div class="storyboard-broll-library-file">${esc(fileName)}</div>
          <div class="storyboard-broll-library-meta">${esc(displayType)} · ${esc(sizeText)}${addedText ? ` · ${esc(addedText)}` : ''}</div>
        </div>
        <button class="btn btn-ghost btn-sm" onclick="storyboardRemoveBrollFileFromCompact(event,'${encodedName}')">Remove</button>
      </div>
    `;
  }).join('');
  if (storyboardBrollExpandedOpen) storyboardRenderExpandedBrollLibrary();
}

function storyboardBuildSceneDescriptionMapFromPhase3Detail(detail) {
  const map = {};
  if (!detail || typeof detail !== 'object') return map;
  const addLine = (line) => {
    if (!line || typeof line !== 'object') return;
    const sceneLineId = String(line.scene_line_id || '').trim();
    if (!sceneLineId) return;
    const sceneDescription = String(
      line.scene_description
      || line.a_roll
      || line.b_roll
      || line.direction
      || ''
    ).trim();
    if (!sceneDescription) return;
    if (!map[sceneLineId]) map[sceneLineId] = sceneDescription;
  };

  const production = detail.production_handoff_packet && typeof detail.production_handoff_packet === 'object'
    ? detail.production_handoff_packet
    : {};
  const items = Array.isArray(production.items) ? production.items : [];
  items.forEach((item) => {
    if (!item || typeof item !== 'object') return;
    const lines = Array.isArray(item.lines) ? item.lines : [];
    lines.forEach(addLine);
    const hooks = Array.isArray(item.selected_hooks) ? item.selected_hooks : [];
    hooks.forEach((hook) => {
      if (!hook || typeof hook !== 'object') return;
      const sceneLines = Array.isArray(hook.scene_lines) ? hook.scene_lines : [];
      sceneLines.forEach(addLine);
    });
  });

  const scenePlansByArm = detail.scene_plans_by_arm && typeof detail.scene_plans_by_arm === 'object'
    ? detail.scene_plans_by_arm
    : {};
  Object.values(scenePlansByArm).forEach((rows) => {
    if (!Array.isArray(rows)) return;
    rows.forEach((row) => {
      if (!row || typeof row !== 'object') return;
      const lines = Array.isArray(row.lines) ? row.lines : [];
      lines.forEach(addLine);
    });
  });

  const sceneHandoff = detail.scene_handoff_packet && typeof detail.scene_handoff_packet === 'object'
    ? detail.scene_handoff_packet
    : {};
  const sceneItems = Array.isArray(sceneHandoff.items) ? sceneHandoff.items : [];
  sceneItems.forEach((item) => {
    if (!item || typeof item !== 'object') return;
    const lines = Array.isArray(item.lines) ? item.lines : [];
    lines.forEach(addLine);
  });

  return map;
}

function storyboardHasAnySceneDescription(value) {
  return Boolean(value && typeof value === 'object' && Object.keys(value).length);
}

async function storyboardEnsureSceneDescriptionMap(options = {}) {
  const force = Boolean(options.force);
  const phase3RunId = storyboardResolvePhase3RunId();
  if (!phase3RunId || !activeBranchId || !activeBrandSlug) {
    storyboardSceneDescriptionMap = {};
    storyboardSceneDescriptionRunId = '';
    return {};
  }
  if (!force && storyboardSceneDescriptionRunId === phase3RunId && storyboardHasAnySceneDescription(storyboardSceneDescriptionMap)) {
    return storyboardSceneDescriptionMap;
  }

  if (!force && storyboardSceneDescriptionFetchPromise) {
    return storyboardSceneDescriptionFetchPromise;
  }

  const localDetail = (phase3V2CurrentRunDetail && typeof phase3V2CurrentRunDetail === 'object')
    ? phase3V2CurrentRunDetail
    : null;
  const localMap = storyboardBuildSceneDescriptionMapFromPhase3Detail(localDetail);
  if (!force && storyboardHasAnySceneDescription(localMap)) {
    storyboardSceneDescriptionMap = localMap;
    storyboardSceneDescriptionRunId = phase3RunId;
    return localMap;
  }

  storyboardSceneDescriptionFetchPromise = (async () => {
    try {
      const resp = await fetch(
        `/api/branches/${activeBranchId}/phase3-v2/runs/${encodeURIComponent(phase3RunId)}${phase3V2BrandParam()}`
      );
      const detail = await storyboardReadJsonResponse(resp, 'Load scene descriptions');
      const map = storyboardBuildSceneDescriptionMapFromPhase3Detail(detail);
      storyboardSceneDescriptionMap = map;
      storyboardSceneDescriptionRunId = phase3RunId;
      return map;
    } catch (_e) {
      return {};
    } finally {
      storyboardSceneDescriptionFetchPromise = null;
    }
  })();
  return storyboardSceneDescriptionFetchPromise;
}

function storyboardActiveSavedVersion() {
  if (storyboardSelectedVersionId === 'live') return null;
  const rows = Array.isArray(storyboardSavedVersions) ? storyboardSavedVersions : [];
  return rows.find((row) => String(row?.version_id || '') === storyboardSelectedVersionId) || null;
}

function storyboardVersionSummaryText(version) {
  if (!version || typeof version !== 'object') return '';
  const totals = version.totals && typeof version.totals === 'object' ? version.totals : {};
  const assigned = parseInt(totals.assigned, 10) || 0;
  const needsReview = parseInt(totals.assigned_needs_review, 10) || 0;
  const failed = parseInt(totals.failed, 10) || 0;
  return `${assigned} assigned · ${needsReview} review · ${failed} failed`;
}

function storyboardRenderVersionOptions() {
  const select = document.getElementById('storyboard-version-select');
  if (!select) return;
  const rows = Array.isArray(storyboardSavedVersions) ? storyboardSavedVersions : [];
  const current = String(storyboardSelectedVersionId || 'live') || 'live';
  const options = ['<option value="live">Live (Current)</option>'];
  rows.forEach((row) => {
    const versionId = String(row?.version_id || '').trim();
    if (!versionId) return;
    const label = String(row?.label || '').trim() || 'Untitled Version';
    options.push(`<option value="${esc(versionId)}">${esc(label)}</option>`);
  });
  select.innerHTML = options.join('');
  const hasCurrent = current === 'live' || rows.some((row) => String(row?.version_id || '') === current);
  storyboardSelectedVersionId = hasCurrent ? current : 'live';
  select.value = storyboardSelectedVersionId;
}

async function storyboardLoadSavedVersions() {
  if (!activeBrandSlug || !activeBranchId || !storyboardVideoRunId) {
    storyboardSavedVersions = [];
    storyboardSelectedVersionId = 'live';
    storyboardRenderVersionOptions();
    return;
  }
  const resp = await fetch(
    `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/storyboard/versions${storyboardBrandParam()}`
  );
  const data = await storyboardReadJsonResponse(resp, 'Load storyboard saved versions');
  storyboardSavedVersions = Array.isArray(data.versions) ? data.versions : [];
  storyboardRenderVersionOptions();
}

function storyboardHandleVersionSelection(event) {
  const value = String(event?.target?.value || 'live').trim() || 'live';
  storyboardSelectedVersionId = value;
  const version = storyboardActiveSavedVersion();
  if (version) {
    const label = String(version?.label || 'saved version').trim();
    storyboardSetStatus('Viewing Saved Version', `Viewing "${label}". Switch back to Live to continue generating.`);
  } else if (storyboardAssignmentStatus?.status) {
    storyboardRefreshAssignmentStatus().catch(() => {
      storyboardSetStatus('Ready', 'Switched back to live storyboard view.');
    });
  }
  storyboardRenderGrid();
  storyboardSetControlState();
}

async function storyboardSaveCurrentVersion() {
  if (!activeBrandSlug || !activeBranchId || !storyboardVideoRunId) return;
  try {
    const labelInput = prompt('Name this saved version (optional):', '');
    if (labelInput === null) return;
    const label = String(labelInput || '').trim();
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/storyboard/versions/save`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '', label }),
      }
    );
    const data = await storyboardReadJsonResponse(resp, 'Save storyboard version');
    storyboardSavedVersions = Array.isArray(data.versions) ? data.versions : storyboardSavedVersions;
    storyboardRenderVersionOptions();
    if (data?.version?.version_id) {
      storyboardSelectedVersionId = String(data.version.version_id);
      storyboardRenderVersionOptions();
    }
    const savedLabel = String(data?.version?.label || label || 'Saved Version').trim();
    storyboardAddActivity(`Saved storyboard version "${savedLabel}".`, 'success');
    storyboardRenderGrid();
    storyboardSetControlState();
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, 'save storyboard version'));
    storyboardAddActivity(formatFetchError(e, 'save storyboard version'), 'error');
  }
}

async function storyboardDeleteSelectedVersion() {
  if (!activeBrandSlug || !activeBranchId || !storyboardVideoRunId) return;
  const version = storyboardActiveSavedVersion();
  if (!version) {
    alert('Select a saved version first.');
    return;
  }
  const versionId = String(version?.version_id || '').trim();
  if (!versionId) {
    alert('Selected version is missing an ID.');
    return;
  }
  const label = String(version?.label || 'Saved Version').trim();
  const confirmed = confirm(`Delete saved version "${label}"? This cannot be undone.`);
  if (!confirmed) return;
  try {
    let resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/storyboard/versions`,
      {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '', version_id: versionId }),
      }
    );
    if (resp.status === 405) {
      resp = await fetch(
        `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/storyboard/versions/delete`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ brand: activeBrandSlug || '', version_id: versionId }),
        }
      );
    }
    const data = await storyboardReadJsonResponse(resp, 'Delete storyboard version');
    storyboardSavedVersions = Array.isArray(data.versions) ? data.versions : [];
    storyboardSelectedVersionId = 'live';
    storyboardRenderVersionOptions();
    storyboardRenderGrid();
    storyboardSetControlState();
    storyboardSetStatus('Live View', `Deleted "${label}".`);
    storyboardAddActivity(`Deleted storyboard version "${label}".`, 'warn');
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, 'delete storyboard version'));
    storyboardAddActivity(formatFetchError(e, 'delete storyboard version'), 'error');
  }
}

function storyboardRenderGrid() {
  const grid = document.getElementById('storyboard-grid');
  if (!grid) return;
  const savedVersion = storyboardActiveSavedVersion();
  const clips = savedVersion && Array.isArray(savedVersion.clips)
    ? savedVersion.clips
    : (Array.isArray(storyboardRunDetail?.clips) ? storyboardRunDetail.clips : []);
  if (!clips.length) {
    grid.innerHTML = '<div class="empty-state">Run Scene Writer and click Continue to load your storyboard.</div>';
    return;
  }

  grid.innerHTML = clips.map((clip) => {
    const clipId = String(clip?.clip_id || '').trim();
    const sceneLineId = String(clip?.scene_line_id || '').trim();
    const mode = String(clip?.mode || '').trim();
    const rawStatus = String(clip?.assignment_status || 'pending').trim();
    const status = rawStatus || 'pending';
    const statusLabel = status.replace(/_/g, ' ');
    const score = parseInt(clip?.assignment_score, 10) || 0;
    const narration = String(clip?.narration_line || clip?.narration_text || '').trim();
    let sceneDescription = String(clip?.scene_description || '').trim();
    if (!sceneDescription && sceneLineId) {
      sceneDescription = String(storyboardSceneDescriptionMap[sceneLineId] || '').trim();
    }
    const promptText = String(clip?.transform_prompt || '').trim();
    const startFrameUrl = String(clip?.start_frame_url || '').trim();
    const previewUrl = String(clip?.preview_url || '').trim();
    const encodedFrameUrl = encodeURIComponent(startFrameUrl);
    const encodedFrameLabel = encodeURIComponent(`${sceneLineId || clipId} · ${mode || 'clip'}`);
    const encodedPrompt = encodeURIComponent(promptText);
    const encodedPromptTitle = encodeURIComponent(`Transform Prompt · ${sceneLineId || clipId}`);
    const encodedPromptMeta = encodeURIComponent(`Mode: ${mode || 'n/a'} · Score: ${score || 0}`);
    const statusClass = status === 'failed'
      ? 'failed'
      : ((score > 0 && score < 6) || status.includes('needs_review') ? 'needs-review' : '');

    const frameBlock = startFrameUrl
      ? `
        <button class="storyboard-start-frame is-clickable" onclick="storyboardOpenImageModal('${encodedFrameUrl}','${encodedFrameLabel}','${encodedPrompt}','${encodedPromptMeta}')">
          <img src="${esc(startFrameUrl)}" alt="${esc(sceneLineId || clipId)} start frame">
        </button>
      `
      : '<div class="storyboard-start-frame"><span class="muted">No start frame yet</span></div>';

    return `
      <article class="storyboard-card ${statusClass}">
        ${frameBlock}
        <div class="storyboard-card-meta">
          <span class="storyboard-chip">${esc(mode || 'unknown')}</span>
          <span class="storyboard-chip ${statusClass}">${esc(statusLabel)}</span>
          ${score ? `<span class="storyboard-chip">score ${esc(String(score))}/10</span>` : ''}
          ${promptText ? `<button class="storyboard-chip storyboard-chip-prompt" onclick="storyboardOpenPromptModal('${encodedPrompt}','${encodedPromptTitle}','${encodedPromptMeta}')">View Prompt</button>` : ''}
        </div>
        <div class="storyboard-copy">
          <div class="storyboard-field storyboard-narration-line">
            <div class="storyboard-field-label">Narration</div>
            <div class="storyboard-field-value">${esc(narration || '(missing)')}</div>
          </div>
          <div class="storyboard-field storyboard-scene-description">
            <div class="storyboard-field-label">Scene Description</div>
            <div class="storyboard-field-value">${esc(sceneDescription || '(missing)')}</div>
          </div>
          ${previewUrl ? `
            <div class="storyboard-field">
              <div class="storyboard-field-label">Generated Preview</div>
              <div class="storyboard-field-value"><a href="${esc(previewUrl)}" target="_blank" rel="noopener noreferrer">Open video</a></div>
            </div>
          ` : ''}
        </div>
      </article>
    `;
  }).join('');
}

function storyboardSetControlState() {
  const continueBtn = document.getElementById('storyboard-continue-btn');
  const stopBtn = document.getElementById('storyboard-stop-btn');
  const resetBtn = document.getElementById('storyboard-reset-btn');
  const saveVersionBtn = document.getElementById('storyboard-save-version-btn');
  const deleteVersionBtn = document.getElementById('storyboard-delete-version-btn');
  const versionSelect = document.getElementById('storyboard-version-select');
  const profileUploadBtn = document.getElementById('storyboard-profile-upload-btn');
  const profileImageUploadBtn = document.getElementById('storyboard-profile-image-upload-btn');
  const brollFolderUploadBtn = document.getElementById('storyboard-pick-folder-btn');
  const brollUploadBtn = document.getElementById('storyboard-pick-btn');
  const expandedAddBtn = document.getElementById('storyboard-expanded-add-btn');
  const activityBtn = document.getElementById('storyboard-activity-toggle-btn');

  const hasRun = Boolean(storyboardVideoRunId);
  const hasBroll = Array.isArray(storyboardBrollFiles) && storyboardBrollFiles.length > 0;
  const hasProfile = Boolean(String(document.getElementById('storyboard-profile-select')?.value || '').trim());
  const status = String(storyboardAssignmentStatus?.status || '').trim().toLowerCase();
  const running = status === 'running';
  const viewingSavedVersion = storyboardSelectedVersionId !== 'live';

  if (continueBtn) continueBtn.disabled = !(hasRun && hasBroll && hasProfile) || running || viewingSavedVersion;
  if (stopBtn) stopBtn.disabled = !running;
  if (resetBtn) resetBtn.disabled = !hasRun || running || viewingSavedVersion;
  if (saveVersionBtn) saveVersionBtn.disabled = !hasRun || running;
  if (deleteVersionBtn) deleteVersionBtn.disabled = !hasRun || running || !viewingSavedVersion;
  if (versionSelect) versionSelect.disabled = !hasRun;
  if (profileUploadBtn) profileUploadBtn.disabled = !activeBranchId || !activeBrandSlug || !phase4V1Enabled;
  if (profileImageUploadBtn) profileImageUploadBtn.disabled = !activeBranchId || !activeBrandSlug || !phase4V1Enabled;
  if (brollFolderUploadBtn) brollFolderUploadBtn.disabled = !activeBranchId || !activeBrandSlug || !phase4V1Enabled;
  if (brollUploadBtn) brollUploadBtn.disabled = !activeBranchId || !activeBrandSlug || !phase4V1Enabled;
  if (expandedAddBtn) expandedAddBtn.disabled = !activeBranchId || !activeBrandSlug || !phase4V1Enabled;
  if (activityBtn) activityBtn.classList.remove('hidden');
  storyboardRenderVersionOptions();
  storyboardRenderActivitySourceToggle();
}

async function storyboardReadJsonResponse(resp, fallbackLabel) {
  const raw = await resp.text();
  let data = {};
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch (_err) {
    throw new Error(`${fallbackLabel} returned non-JSON response (HTTP ${resp.status}).`);
  }
  if (!resp.ok) {
    throw new Error(data.error || `${fallbackLabel} failed (HTTP ${resp.status})`);
  }
  return data;
}

async function storyboardEnsureBootstrap(options = {}) {
  const force = Boolean(options.force);
  if (!phase4V1Enabled) {
    storyboardSetStatus('Disabled', 'Storyboard is disabled on this server.');
    return false;
  }
  if (!activeBrandSlug || !activeBranchId) {
    storyboardSetStatus('Idle', 'Open a brand and select a branch first.');
    return false;
  }
  const phase3RunId = storyboardResolvePhase3RunId();
  if (!phase3RunId) {
    storyboardSetStatus('Idle', 'Run Scene Writer first, then open Story Board.');
    return false;
  }

  const initKey = storyboardBuildInitKey();
  if (!force && storyboardVideoRunId && storyboardLastInitKey === initKey) {
    return true;
  }

  const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/storyboard/bootstrap`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      brand: activeBrandSlug || '',
      phase3_run_id: phase3RunId,
    }),
  });
  const data = await storyboardReadJsonResponse(resp, 'Storyboard bootstrap');
  storyboardVideoRunId = String(data.video_run_id || '').trim();
  storyboardLastInitKey = initKey;
  if (!storyboardVideoRunId) {
    throw new Error('Storyboard bootstrap succeeded but no run ID was returned.');
  }
  storyboardAddActivity(
    data.reused_existing_run
      ? `Loaded storyboard run ${storyboardVideoRunId}.`
      : `Created storyboard run ${storyboardVideoRunId}.`,
    'success',
  );
  return true;
}

async function storyboardLoadProfiles() {
  if (!activeBrandSlug || !activeBranchId) return;
  const listResp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/talking-head/profiles${storyboardBrandParam()}`);
  const listData = await storyboardReadJsonResponse(listResp, 'Load talking-head profiles');
  storyboardProfiles = Array.isArray(listData.profiles) ? listData.profiles : [];

  let selectedProfileId = '';
  let selectedProfile = null;
  if (storyboardVideoRunId) {
    const selectedResp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/talking-head/profile${storyboardBrandParam()}`
    );
    const selectedData = await storyboardReadJsonResponse(selectedResp, 'Load selected talking-head profile');
    selectedProfileId = String(selectedData.selected_profile_id || '').trim();
    if (selectedData.profile && typeof selectedData.profile === 'object') {
      selectedProfile = selectedData.profile;
    }
  }

  const select = document.getElementById('storyboard-profile-select');
  if (select) {
    const options = [
      '<option value="">No profile selected</option>',
      ...storyboardProfiles.map((profile) => {
        const profileId = String(profile?.profile_id || '').trim();
        const name = String(profile?.name || profileId || 'Profile').trim();
        const count = parseInt(profile?.source_count, 10) || 0;
        return `<option value="${esc(profileId)}">${esc(name)} (${count})</option>`;
      }),
    ];
    select.innerHTML = options.join('');
    if (selectedProfileId) select.value = selectedProfileId;
  }

  const selectedTextEl = document.getElementById('storyboard-profile-selected');
  if (selectedTextEl) {
    if (selectedProfile && typeof selectedProfile === 'object') {
      const label = String(selectedProfile.name || selectedProfile.profile_id || 'profile');
      const count = parseInt(selectedProfile.source_count, 10) || 0;
      selectedTextEl.textContent = `${label} selected (${count} image${count === 1 ? '' : 's'}).`;
    } else {
      selectedTextEl.textContent = 'No talking head profile selected.';
    }
  }
}

async function storyboardLoadBrollLibrary() {
  if (!activeBrandSlug || !activeBranchId) return;
  const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/storyboard/broll-library${storyboardBrandParam()}`);
  const data = await storyboardReadJsonResponse(resp, 'Load B-roll library');
  storyboardBrollFiles = Array.isArray(data.files) ? data.files : [];
  storyboardRenderBrollLibrary();
}

async function storyboardLoadRunDetail() {
  if (!activeBrandSlug || !activeBranchId || !storyboardVideoRunId) return;
  const resp = await fetch(
    `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}${storyboardBrandParam()}`
  );
  const data = await storyboardReadJsonResponse(resp, 'Load storyboard run detail');
  const clips = Array.isArray(data?.clips) ? data.clips : [];
  const hasMissingDescriptions = clips.some((clip) => !String(clip?.scene_description || '').trim());
  if (hasMissingDescriptions) {
    await storyboardEnsureSceneDescriptionMap();
  }
  const hydratedClips = clips.map((clip) => {
    if (!clip || typeof clip !== 'object') return clip;
    const currentDescription = String(clip.scene_description || '').trim();
    if (currentDescription) return clip;
    const sceneLineId = String(clip.scene_line_id || '').trim();
    if (!sceneLineId) return clip;
    const fallbackDescription = String(storyboardSceneDescriptionMap[sceneLineId] || '').trim();
    if (!fallbackDescription) return clip;
    return { ...clip, scene_description: fallbackDescription };
  });
  storyboardRunDetail = { ...data, clips: hydratedClips };
  storyboardSavedVersions = Array.isArray(data?.storyboard_saved_versions) ? data.storyboard_saved_versions : storyboardSavedVersions;
  storyboardRenderVersionOptions();
  storyboardRenderGrid();
}

function storyboardStopStatusPolling() {
  if (storyboardStatusPollTimer) {
    clearInterval(storyboardStatusPollTimer);
    storyboardStatusPollTimer = null;
  }
}

async function storyboardRefreshAssignmentStatus() {
  if (!activeBrandSlug || !activeBranchId || !storyboardVideoRunId) return;
  const resp = await fetch(
    `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/storyboard/assign/status${storyboardBrandParam()}`
  );
  const data = await storyboardReadJsonResponse(resp, 'Load storyboard assignment status');
  storyboardAssignmentStatus = data;
  const status = String(data.status || 'idle').trim().toLowerCase();
  const totals = data.totals && typeof data.totals === 'object' ? data.totals : {};
  const total = parseInt(totals.total || totals.required || totals.clip_count, 10) || 0;
  const assigned = parseInt(totals.assigned || totals.assigned_needs_review, 10) || 0;
  const failed = parseInt(totals.failed, 10) || 0;
  if (status === 'running') {
    storyboardSetStatus('Running', `Assigning start frames... ${assigned}/${total || '?'} assigned${failed ? `, ${failed} failed` : ''}.`);
  } else if (status === 'completed') {
    storyboardSetStatus('Completed', `Storyboard assignment completed. ${assigned}/${total || '?'} assigned${failed ? `, ${failed} failed` : ''}.`);
    storyboardStopStatusPolling();
    await storyboardLoadRunDetail();
  } else if (status === 'failed') {
    storyboardSetStatus('Failed', String(data.error || 'Storyboard assignment failed.'));
    storyboardStopStatusPolling();
  } else if (status === 'aborted') {
    storyboardSetStatus('Stopped', String(data.error || 'Storyboard assignment stopped.'));
    storyboardStopStatusPolling();
  } else {
    storyboardSetStatus('Idle', 'Upload A-roll and B-roll images, then click Continue.');
  }
  storyboardSetControlState();
}

function storyboardStartStatusPolling() {
  storyboardStopStatusPolling();
  storyboardStatusPollTimer = setInterval(() => {
    storyboardRefreshAssignmentStatus().catch((e) => {
      console.error(e);
      storyboardAddActivity(e?.message || 'Failed to refresh storyboard assignment status.', 'error');
    });
  }, 2000);
}

async function storyboardEnsureInitialized(options = {}) {
  const force = Boolean(options.force);
  const summaryEl = document.getElementById('storyboard-status-summary');
  const phaseEl = document.getElementById('storyboard-status-phase');
  if (!summaryEl || !phaseEl) return;

  try {
    if (!phase4V1Enabled) {
      storyboardSetStatus('Disabled', 'Storyboard is disabled on this server.');
      storyboardSetControlState();
      return;
    }

    if (!activeBrandSlug || !activeBranchId) {
      storyboardSetStatus('Idle', 'Open a brand and select a branch first.');
      storyboardRenderBrollLibrary();
      storyboardSetControlState();
      return;
    }

    const phase3RunId = storyboardResolvePhase3RunId();
    if (!phase3RunId) {
      storyboardVideoRunId = '';
      storyboardAssignmentStatus = null;
      storyboardRunDetail = null;
    } else {
      const ok = await storyboardEnsureBootstrap({ force });
      if (!ok) {
        storyboardSetControlState();
        return;
      }
    }

    await storyboardLoadProfiles();
    await storyboardLoadBrollLibrary();
    if (storyboardVideoRunId) {
      await storyboardLoadRunDetail();
      await storyboardLoadSavedVersions();
      await storyboardRefreshAssignmentStatus();
    } else {
      storyboardSavedVersions = [];
      storyboardSelectedVersionId = 'live';
      storyboardRenderVersionOptions();
      storyboardRenderGrid();
      storyboardSetStatus('Ready', 'Upload A-roll and B-roll now. Select a Scene Writer run to enable Continue.');
    }
  } catch (e) {
    console.error(e);
    storyboardSetStatus('Error', formatFetchError(e, 'storyboard'));
    storyboardAddActivity(formatFetchError(e, 'storyboard'), 'error');
  } finally {
    storyboardSetControlState();
    storyboardRenderActivityLog();
  }
}

function storyboardResetStateForBranch() {
  storyboardStopStatusPolling();
  storyboardCloseBrollLibraryExpanded();
  storyboardVideoRunId = '';
  storyboardLastInitKey = '';
  storyboardAssignmentStatus = null;
  storyboardRunDetail = null;
  storyboardSceneDescriptionMap = {};
  storyboardSceneDescriptionRunId = '';
  storyboardSceneDescriptionFetchPromise = null;
  storyboardProfiles = [];
  storyboardBrollFiles = [];
  storyboardSavedVersions = [];
  storyboardSelectedVersionId = 'live';
  storyboardBrollExpandedSearch = '';
  storyboardBrollExpandedFilter = 'all';
  storyboardActivityAutoFollowBySource = { activity: true, live_server: true };
  storyboardAddActivity('Storyboard context reset for branch change.');

  const profileSelect = document.getElementById('storyboard-profile-select');
  if (profileSelect) profileSelect.innerHTML = '<option value="">No profile selected</option>';
  const profileSelected = document.getElementById('storyboard-profile-selected');
  if (profileSelected) profileSelected.textContent = 'No talking head profile selected.';
  const grid = document.getElementById('storyboard-grid');
  if (grid) grid.innerHTML = '<div class="empty-state">Run Scene Writer and click Continue to load your storyboard.</div>';
  storyboardRenderBrollLibrary();
  storyboardSetStatus('Idle', 'Upload A-roll and B-roll images, then click Continue.');
  storyboardSetControlState();
}

function storyboardPickTalkingHeadFolder() {
  if (!activeBrandSlug || !activeBranchId) {
    alert('Select a brand and branch first.');
    return;
  }
  const picker = document.getElementById('storyboard-talking-head-folder-input');
  if (!picker) return;
  picker.click();
}

function storyboardPickTalkingHeadImages() {
  if (!activeBrandSlug || !activeBranchId) {
    alert('Select a brand and branch first.');
    return;
  }
  const picker = document.getElementById('storyboard-talking-head-image-input');
  if (!picker) return;
  picker.click();
}

async function storyboardUploadTalkingHeadFiles(files, options = {}) {
  const uploadLabel = String(options?.uploadLabel || 'A-roll images').trim() || 'A-roll images';
  try {
    const form = new FormData();
    form.append('brand', activeBrandSlug || '');
    const firstPath = String(files[0]?.webkitRelativePath || '').replace(/\\/g, '/');
    const inferredName = firstPath.includes('/') ? firstPath.split('/')[0] : '';
    form.append('name', inferredName || '');
    files.forEach((file) => form.append('files', file, file.webkitRelativePath || file.name));

    const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/talking-head/profiles/upload`, {
      method: 'POST',
      body: form,
    });
    const data = await storyboardReadJsonResponse(resp, `Upload ${uploadLabel}`);
    const profile = data.profile && typeof data.profile === 'object' ? data.profile : null;
    const profileId = String(profile?.profile_id || '').trim();
    if (profileId && !storyboardVideoRunId && storyboardResolvePhase3RunId()) {
      try {
        await storyboardEnsureBootstrap();
      } catch (_err) {
        // Non-fatal: profile is still uploaded and listed.
      }
    }
    if (profileId && storyboardVideoRunId) {
      const selectResp = await fetch(
        `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/talking-head/profile/select`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ brand: activeBrandSlug || '', profile_id: profileId }),
        }
      );
      await storyboardReadJsonResponse(selectResp, 'Select talking-head profile');
    }

    await storyboardLoadProfiles();
    storyboardSetStatus('Ready', 'A-roll profile uploaded. Add B-roll images, then click Continue.');
    storyboardAddActivity(`Uploaded A-roll profile${profile?.name ? ` "${profile.name}"` : ''}.`, 'success');
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, `upload ${uploadLabel}`));
    storyboardAddActivity(formatFetchError(e, `upload ${uploadLabel}`), 'error');
  } finally {
    storyboardSetControlState();
  }
}

async function storyboardHandleTalkingHeadFolderPicked(event) {
  const picker = event?.target;
  const files = Array.from(picker?.files || []);
  if (!files.length) return;
  if (!activeBrandSlug || !activeBranchId) {
    if (picker) picker.value = '';
    alert('Select a brand and branch first.');
    return;
  }
  await storyboardUploadTalkingHeadFiles(files, { uploadLabel: 'A-roll folder' });
  if (picker) picker.value = '';
}

async function storyboardHandleTalkingHeadImagesPicked(event) {
  const picker = event?.target;
  const files = Array.from(picker?.files || []);
  if (!files.length) return;
  if (!activeBrandSlug || !activeBranchId) {
    if (picker) picker.value = '';
    alert('Select a brand and branch first.');
    return;
  }
  await storyboardUploadTalkingHeadFiles(files, { uploadLabel: 'A-roll images' });
  if (picker) picker.value = '';
}

async function storyboardHandleProfileSelection(event) {
  const profileId = String(event?.target?.value || '').trim();
  if (!activeBrandSlug || !activeBranchId) return;
  try {
    if (!storyboardVideoRunId && storyboardResolvePhase3RunId()) {
      await storyboardEnsureBootstrap();
    }
    if (!storyboardVideoRunId) {
      storyboardAddActivity('Profile saved. Continue is enabled after Scene Writer run is selected.', 'warn');
      return;
    }
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/talking-head/profile/select`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '', profile_id: profileId }),
      }
    );
    await storyboardReadJsonResponse(resp, 'Select talking-head profile');
    await storyboardLoadProfiles();
    storyboardAddActivity(profileId ? 'Selected A-roll profile.' : 'Cleared A-roll profile.');
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, 'select A-roll profile'));
    storyboardAddActivity(formatFetchError(e, 'select A-roll profile'), 'error');
  } finally {
    storyboardSetControlState();
  }
}

function storyboardPickFolder() {
  if (!activeBrandSlug || !activeBranchId) {
    alert('Select a brand and branch first.');
    return;
  }
  const picker = document.getElementById('storyboard-folder-input');
  if (!picker) return;
  picker.click();
}

function storyboardPickBrollImages() {
  if (!activeBrandSlug || !activeBranchId) {
    alert('Select a brand and branch first.');
    return;
  }
  const picker = document.getElementById('storyboard-library-file-input');
  if (!picker) return;
  picker.click();
}

async function storyboardHandleFolderPicked(event) {
  const picker = event?.target;
  const files = Array.from(picker?.files || []);
  if (!files.length) return;
  if (!activeBrandSlug || !activeBranchId) {
    if (picker) picker.value = '';
    alert('Select a brand and branch first.');
    return;
  }
  await storyboardUploadBrollFiles(files, { modeHint: 'unknown', activityLabel: 'Added B-roll images' });
  if (picker) picker.value = '';
}

async function storyboardHandleLibraryFilePicked(event) {
  const picker = event?.target;
  const files = Array.from(picker?.files || []);
  if (!files.length) return;
  if (!activeBrandSlug || !activeBranchId) {
    if (picker) picker.value = '';
    alert('Select a brand and branch first.');
    return;
  }
  await storyboardUploadBrollFiles(files, { modeHint: 'unknown', activityLabel: 'Added image bank files' });
  if (picker) picker.value = '';
}

async function storyboardUploadBrollFiles(files, options = {}) {
  const modeHint = String(options?.modeHint || 'unknown').trim() || 'unknown';
  const activityLabel = String(options?.activityLabel || 'Added B-roll images').trim() || 'Added B-roll images';
  try {
    const form = new FormData();
    form.append('brand', activeBrandSlug || '');
    form.append('mode_hint', modeHint);
    files.forEach((file) => form.append('files', file, file.webkitRelativePath || file.name));
    const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/storyboard/broll-library/files`, {
      method: 'POST',
      body: form,
    });
    const data = await storyboardReadJsonResponse(resp, 'Add B-roll images');
    storyboardBrollFiles = Array.isArray(data.files) ? data.files : storyboardBrollFiles;
    storyboardRenderBrollLibrary();
    storyboardSetStatus('Ready', 'B-roll images saved. Upload/select A-roll profile, then click Continue.');
    storyboardAddActivity(`${activityLabel}: ${parseInt(data.added_count, 10) || files.length} file(s).`, 'success');
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, 'add B-roll images'));
    storyboardAddActivity(formatFetchError(e, 'add B-roll images'), 'error');
  } finally {
    storyboardSetControlState();
  }
}

function storyboardNormalizeFileNameInput(value) {
  return String(value || '')
    .replace(/\\/g, '/')
    .split('/')
    .pop()
    .trim();
}

function storyboardSupportsImageFileName(fileName) {
  return /\.(png|jpg|jpeg|webp)$/i.test(String(fileName || ''));
}

function storyboardResolveRenameInput(currentName, requestedName) {
  const current = storyboardNormalizeFileNameInput(currentName);
  let target = storyboardNormalizeFileNameInput(requestedName);
  if (!target) return '';
  if (!/\.[^./\\]+$/.test(target)) {
    const ext = (current.match(/\.[^./\\]+$/) || [''])[0];
    target = `${target}${ext}`;
  }
  return target;
}

function storyboardCloneLibraryRows(rows) {
  if (!Array.isArray(rows)) return [];
  return rows.map((row) => {
    const cloned = row && typeof row === 'object' ? { ...row } : {};
    if (cloned.metadata && typeof cloned.metadata === 'object') {
      cloned.metadata = { ...cloned.metadata };
      if (Array.isArray(cloned.metadata.tags)) {
        cloned.metadata.tags = [...cloned.metadata.tags];
      }
    }
    return cloned;
  });
}

function storyboardParseTagsInput(value) {
  const seen = new Set();
  return String(value || '')
    .split(',')
    .map((tag) => String(tag || '').trim())
    .filter((tag) => {
      if (!tag) return false;
      const key = tag.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

async function storyboardRenameBrollFile(encodedName) {
  const fileName = decodeURIComponent(String(encodedName || ''));
  if (!fileName || !activeBrandSlug || !activeBranchId) return;
  const asked = prompt('Rename image file:', fileName);
  if (asked == null) return;
  const resolvedName = storyboardResolveRenameInput(fileName, asked);
  if (!resolvedName) {
    alert('File name cannot be empty.');
    return;
  }
  if (!storyboardSupportsImageFileName(resolvedName)) {
    alert('File must end with .png, .jpg, .jpeg, or .webp');
    return;
  }

  const previousRows = storyboardCloneLibraryRows(storyboardBrollFiles);
  storyboardBrollFiles = (Array.isArray(storyboardBrollFiles) ? storyboardBrollFiles : []).map((row) => {
    if (String(row?.file_name || '').toLowerCase() !== String(fileName || '').toLowerCase()) return row;
    return { ...row, file_name: resolvedName };
  });
  storyboardRenderBrollLibrary();

  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/storyboard/broll-library/files/rename`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: activeBrandSlug || '',
        file_name: fileName,
        new_file_name: resolvedName,
      }),
    });
    const data = await storyboardReadJsonResponse(resp, 'Rename B-roll image');
    storyboardBrollFiles = Array.isArray(data.files) ? data.files : previousRows;
    storyboardRenderBrollLibrary();
    storyboardAddActivity(`Renamed "${fileName}" to "${String(data.new_file_name || resolvedName)}".`, 'success');
  } catch (e) {
    storyboardBrollFiles = previousRows;
    storyboardRenderBrollLibrary();
    console.error(e);
    alert(formatFetchError(e, 'rename B-roll image'));
    storyboardAddActivity(formatFetchError(e, 'rename B-roll image'), 'error');
  } finally {
    storyboardSetControlState();
  }
}

async function storyboardEditBrollTags(encodedName) {
  const fileName = decodeURIComponent(String(encodedName || ''));
  if (!fileName || !activeBrandSlug || !activeBranchId) return;
  const currentRow = (Array.isArray(storyboardBrollFiles) ? storyboardBrollFiles : [])
    .find((row) => String(row?.file_name || '').toLowerCase() === fileName.toLowerCase());
  const currentTags = storyboardBrollTags(currentRow);
  const asked = prompt('Edit tags (comma-separated):', currentTags.join(', '));
  if (asked == null) return;
  const tags = storyboardParseTagsInput(asked);

  const previousRows = storyboardCloneLibraryRows(storyboardBrollFiles);
  storyboardBrollFiles = (Array.isArray(storyboardBrollFiles) ? storyboardBrollFiles : []).map((row) => {
    if (String(row?.file_name || '').toLowerCase() !== fileName.toLowerCase()) return row;
    const metadata = row?.metadata && typeof row.metadata === 'object' ? { ...row.metadata } : {};
    metadata.tags = tags;
    return { ...row, metadata };
  });
  storyboardRenderBrollLibrary();

  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/storyboard/broll-library/files/metadata`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: activeBrandSlug || '',
        file_name: fileName,
        tags,
      }),
    });
    const data = await storyboardReadJsonResponse(resp, 'Update B-roll tags');
    storyboardBrollFiles = Array.isArray(data.files) ? data.files : previousRows;
    storyboardRenderBrollLibrary();
    storyboardAddActivity(`Updated tags for "${fileName}".`, 'success');
  } catch (e) {
    storyboardBrollFiles = previousRows;
    storyboardRenderBrollLibrary();
    console.error(e);
    alert(formatFetchError(e, 'update B-roll tags'));
    storyboardAddActivity(formatFetchError(e, 'update B-roll tags'), 'error');
  } finally {
    storyboardSetControlState();
  }
}

async function storyboardRemoveBrollFile(encodedName) {
  const fileName = decodeURIComponent(String(encodedName || ''));
  if (!fileName) return;
  if (!activeBrandSlug || !activeBranchId) return;
  if (!confirm(`Remove "${fileName}" from saved B-roll images?`)) return;
  try {
    const resp = await fetch(`/api/branches/${activeBranchId}/phase4-v1/storyboard/broll-library/files`, {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brand: activeBrandSlug || '',
        file_names: [fileName],
      }),
    });
    const data = await storyboardReadJsonResponse(resp, 'Remove B-roll image');
    storyboardBrollFiles = Array.isArray(data.files) ? data.files : [];
    storyboardRenderBrollLibrary();
    storyboardAddActivity(`Removed B-roll image "${fileName}".`, 'warn');
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, 'remove B-roll image'));
    storyboardAddActivity(formatFetchError(e, 'remove B-roll image'), 'error');
  } finally {
    storyboardSetControlState();
  }
}

function storyboardHandleImageModelSelection(_event) {
  storyboardAddActivity('Updated image model selection.');
}

function storyboardHandlePromptModelSelection(_event) {
  storyboardAddActivity('Updated prompt model selection.');
}

async function storyboardContinueAssignment() {
  try {
    const ok = await storyboardEnsureBootstrap();
    if (!ok || !storyboardVideoRunId) return;
    const profileId = String(document.getElementById('storyboard-profile-select')?.value || '').trim();
    if (!profileId) {
      alert('Upload/select an A-roll profile first.');
      return;
    }
    if (!Array.isArray(storyboardBrollFiles) || !storyboardBrollFiles.length) {
      alert('Add B-roll images first.');
      return;
    }

    const imageEditModel = String(document.getElementById('storyboard-image-model-select')?.value || '').trim();
    const promptModel = String(document.getElementById('storyboard-prompt-model-select')?.value || '').trim();
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/storyboard/assign/start`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          brand: activeBrandSlug || '',
          folder_url: '',
          image_edit_model: imageEditModel,
          prompt_model: promptModel,
        }),
      }
    );
    const data = await storyboardReadJsonResponse(resp, 'Start storyboard assignment');
    storyboardAssignmentStatus = { ...storyboardAssignmentStatus, status: String(data.status || 'running') };
    storyboardSetStatus('Running', 'Storyboard assignment started.');
    storyboardAddActivity('Started storyboard assignment.', 'success');
    storyboardSetControlState();
    storyboardStartStatusPolling();
    await storyboardRefreshAssignmentStatus();
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, 'start storyboard assignment'));
    storyboardAddActivity(formatFetchError(e, 'start storyboard assignment'), 'error');
    storyboardSetControlState();
  }
}

async function storyboardStopAssignment() {
  if (!storyboardVideoRunId || !activeBranchId) return;
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/storyboard/assign/stop`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '' }),
      }
    );
    await storyboardReadJsonResponse(resp, 'Stop storyboard assignment');
    storyboardStopStatusPolling();
    await storyboardRefreshAssignmentStatus();
    storyboardAddActivity('Stopped storyboard assignment.', 'warn');
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, 'stop storyboard assignment'));
    storyboardAddActivity(formatFetchError(e, 'stop storyboard assignment'), 'error');
  } finally {
    storyboardSetControlState();
  }
}

async function storyboardResetAssignment() {
  if (!storyboardVideoRunId || !activeBranchId) return;
  if (!confirm('Reset storyboard assignment and clear assigned start frames?')) return;
  try {
    const resp = await fetch(
      `/api/branches/${activeBranchId}/phase4-v1/runs/${encodeURIComponent(storyboardVideoRunId)}/storyboard/assign/reset`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ brand: activeBrandSlug || '' }),
      }
    );
    await storyboardReadJsonResponse(resp, 'Reset storyboard assignment');
    storyboardStopStatusPolling();
    await storyboardLoadRunDetail();
    await storyboardRefreshAssignmentStatus();
    storyboardAddActivity('Reset storyboard assignment.', 'warn');
  } catch (e) {
    console.error(e);
    alert(formatFetchError(e, 'reset storyboard assignment'));
    storyboardAddActivity(formatFetchError(e, 'reset storyboard assignment'), 'error');
  } finally {
    storyboardSetControlState();
  }
}

function storyboardOpenPromptModal(encodedPrompt, encodedTitle = '', encodedMeta = '') {
  const modal = document.getElementById('storyboard-prompt-modal');
  const titleEl = document.getElementById('storyboard-prompt-modal-title');
  const metaEl = document.getElementById('storyboard-prompt-modal-meta');
  const bodyEl = document.getElementById('storyboard-prompt-modal-body');
  if (!modal || !titleEl || !metaEl || !bodyEl) return;
  const promptText = decodeURIComponent(String(encodedPrompt || ''));
  const titleText = decodeURIComponent(String(encodedTitle || '')) || 'Image Edit Prompt';
  const metaText = decodeURIComponent(String(encodedMeta || ''));
  titleEl.textContent = titleText;
  metaEl.textContent = metaText;
  bodyEl.textContent = promptText || '(No prompt text)';
  modal.classList.remove('hidden');
}

function storyboardClosePromptModal() {
  const modal = document.getElementById('storyboard-prompt-modal');
  if (modal) modal.classList.add('hidden');
}

function storyboardOpenImageModal(encodedUrl, encodedLabel = '', encodedPrompt = '', encodedMeta = '') {
  const modal = document.getElementById('storyboard-image-modal');
  const img = document.getElementById('storyboard-image-modal-img');
  const label = document.getElementById('storyboard-image-modal-label');
  const promptBody = document.getElementById('storyboard-image-modal-prompt');
  const promptMeta = document.getElementById('storyboard-image-modal-prompt-meta');
  if (!modal || !img || !label || !promptBody || !promptMeta) return;
  const url = decodeURIComponent(String(encodedUrl || ''));
  const labelText = decodeURIComponent(String(encodedLabel || ''));
  const promptText = decodeURIComponent(String(encodedPrompt || '')).trim();
  const metaText = decodeURIComponent(String(encodedMeta || '')).trim();
  img.src = url;
  label.textContent = labelText;
  promptMeta.textContent = metaText || '';
  promptBody.textContent = promptText || 'No image edit prompt was recorded for this frame.';
  modal.classList.remove('hidden');
}

function storyboardCloseImageModal() {
  const modal = document.getElementById('storyboard-image-modal');
  const img = document.getElementById('storyboard-image-modal-img');
  const promptBody = document.getElementById('storyboard-image-modal-prompt');
  const promptMeta = document.getElementById('storyboard-image-modal-prompt-meta');
  if (modal) modal.classList.add('hidden');
  if (img) img.src = '';
  if (promptBody) promptBody.textContent = '';
  if (promptMeta) promptMeta.textContent = '';
}

// Handle Enter key in rename dialog and new branch modal
document.addEventListener('keydown', (e) => {
  const storyboardBrollModal = document.getElementById('storyboard-broll-expanded-modal');
  if (storyboardBrollModal && !storyboardBrollModal.classList.contains('hidden')) {
    if (e.key === 'Escape') {
      storyboardCloseBrollLibraryExpanded();
      return;
    }
  }

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

  if (e.key === 'Escape' && storyboardActivityDrawerOpen) {
    storyboardCloseActivityDrawer();
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
    phase4V1Enabled = Boolean(data.phase4_v1_enabled);
    phase4VoicePresets = Array.isArray(data.phase4_v1_voice_presets) ? data.phase4_v1_voice_presets : [];
    phase3V2ReviewerRoleDefault = String(data.phase3_v2_reviewer_role_default || 'client_founder').trim() || 'client_founder';
    phase3V2HookDefaults = {
      candidatesPerUnit: parseInt(data.phase3_v2_hook_candidates_per_unit, 10) || 10,
      finalVariantsPerUnit: parseInt(data.phase3_v2_hook_final_variants_per_unit, 10) || 5,
      maxParallel: parseInt(data.phase3_v2_hook_max_parallel, 10) || 4,
      maxRepairRounds: parseInt(data.phase3_v2_hook_max_repair_rounds, 10) || 1,
    };
    phase3V2SceneDefaults = {
      maxParallel: parseInt(data.phase3_v2_scene_max_parallel, 10) || 4,
      maxRepairRounds: parseInt(data.phase3_v2_scene_max_repair_rounds, 10) || 1,
      maxConsecutiveMode: parseInt(data.phase3_v2_scene_max_consecutive_mode, 10) || 3,
      minARollLines: parseInt(data.phase3_v2_scene_min_a_roll_lines, 10) || 1,
      enableBeatSplit: Boolean(data.phase3_v2_scene_enable_beat_split),
      beatTargetWordsMin: parseInt(data.phase3_v2_scene_beat_target_words_min, 10) || 10,
      beatTargetWordsMax: parseInt(data.phase3_v2_scene_beat_target_words_max, 10) || 18,
      beatHardMaxWords: parseInt(data.phase3_v2_scene_beat_hard_max_words, 10) || 24,
      maxBeatsPerLine: parseInt(data.phase3_v2_scene_max_beats_per_line, 10) || 3,
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

    phase4RenderVoicePresetOptions();

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
    if (document.getElementById('view-storyboard')?.classList.contains('active')) {
      storyboardEnsureInitialized({ force: true });
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
restoreSavedCostSnapshot(); // Optimistic cost hydrate before server sync returns

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
      resetCost();
      await refreshCostTracker();

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

      // Restore Phase 1 card state only when final Foundation output is ready.
      if (!pipelineRunning && await foundationFinalReadyForActiveBrand()) {
        setCardState('foundation_research', 'done');
      }

      // Keep branch state shape consistent for downstream UI code.
      branches = Array.isArray(brandData.branches)
        ? brandData.branches.filter(b => b && typeof b === 'object')
        : [];
      activeBranchId = branches.length > 0 ? branches[0].id : null;
      phase3V2ResetStateForBranch();
      phase4ResetStateForBranch();
      storyboardResetStateForBranch();
    } else {
      await refreshCostTracker();
    }
  } catch (e) {
    console.error('Failed to init brand', e);
    renderBriefBrandList();
    await refreshCostTracker();
  }

  // Now load branches (needs activeBrandSlug set first)
  await loadBranches();
  updatePhaseStartButtons();
}

initBrand();
