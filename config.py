"""Pipeline configuration — LLM providers, per-agent model assignments, paths."""

import json
import os
from pathlib import Path
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(_PROJECT_ROOT / ".env")
load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT_DIR = _PROJECT_ROOT
OUTPUT_DIR = ROOT_DIR / os.getenv("OUTPUT_DIR", "outputs")
RESEARCH_DIR = ROOT_DIR / "5 research files"

# ---------------------------------------------------------------------------
# LLM Provider API Keys
# ---------------------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")
FAL_KEY = os.getenv("FAL_KEY", "").strip()

# Optional credentials for direct VOC collectors.
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
AMAZON_PAAPI_ACCESS_KEY = os.getenv("AMAZON_PAAPI_ACCESS_KEY", "")
AMAZON_PAAPI_SECRET_KEY = os.getenv("AMAZON_PAAPI_SECRET_KEY", "")

# ---------------------------------------------------------------------------
# Model names (centralized so they're easy to update)
# ---------------------------------------------------------------------------
OPENAI_FRONTIER = "gpt-5.2"
OPENAI_MINI = "gpt-5.2-mini"
GOOGLE_FRONTIER = "gemini-2.5-pro"
ANTHROPIC_FRONTIER = "claude-opus-4-6"

# ---------------------------------------------------------------------------
# Per-Agent Model Assignments
#
# Each agent can specify: provider, model, temperature, max_tokens.
# Providers: "openai", "anthropic", "google"
# Override any agent via env: AGENT_FOUNDATION_RESEARCH_PROVIDER=anthropic
#                             AGENT_FOUNDATION_RESEARCH_MODEL=claude-opus-4-6
# Legacy numeric env names (AGENT_01A_*, AGENT_02_*, etc.) still work.
# ---------------------------------------------------------------------------

DEFAULT_PROVIDER = os.getenv("DEFAULT_PROVIDER", "openai")
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", OPENAI_FRONTIER)

# ---------------------------------------------------------------------------
# Creative Engine (Agent 02) research + budget controls
# ---------------------------------------------------------------------------

# Hard cap for the full Agent 02 run (Step 1 + Step 2 + Step 3).
CREATIVE_ENGINE_MAX_COST_USD = float(os.getenv("CREATIVE_ENGINE_MAX_COST_USD", "20"))

# Claude SDK scout settings (Step 2).
CREATIVE_SCOUT_MODEL = os.getenv("CREATIVE_SCOUT_MODEL", ANTHROPIC_FRONTIER)
CREATIVE_SCOUT_MAX_TURNS = int(os.getenv("CREATIVE_SCOUT_MAX_TURNS", "12"))
CREATIVE_SCOUT_MAX_THINKING_TOKENS = int(
    os.getenv("CREATIVE_SCOUT_MAX_THINKING_TOKENS", "12000")
)
CREATIVE_SCOUT_MAX_BUDGET_USD = float(
    os.getenv("CREATIVE_SCOUT_MAX_BUDGET_USD", str(CREATIVE_ENGINE_MAX_COST_USD))
)
# Run one specialized Creative Engine worker per funnel stage (ToF/MoF/BoF) in parallel.
CREATIVE_ENGINE_PARALLEL_BY_STAGE = os.getenv("CREATIVE_ENGINE_PARALLEL_BY_STAGE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
CREATIVE_ENGINE_PARALLEL_MAX_WORKERS = int(os.getenv("CREATIVE_ENGINE_PARALLEL_MAX_WORKERS", "3"))
# If true, Step 2 must succeed via Claude Agent SDK or fail (no fallback chain).
CREATIVE_SCOUT_SDK_ONLY = os.getenv("CREATIVE_SCOUT_SDK_ONLY", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# Legacy Anthropic Messages API fallback settings for Step 2.
CREATIVE_SCOUT_WEB_MAX_USES = int(os.getenv("CREATIVE_SCOUT_WEB_MAX_USES", "20"))
CREATIVE_SCOUT_WEB_MAX_TOKENS = int(os.getenv("CREATIVE_SCOUT_WEB_MAX_TOKENS", "20000"))

# ---------------------------------------------------------------------------
# Phase 1 (Agent 1A) v2 controls
# ---------------------------------------------------------------------------

PHASE1_MAX_RUNTIME_MINUTES = int(os.getenv("PHASE1_MAX_RUNTIME_MINUTES", "35"))
PHASE1_GAP_FILL_ROUNDS = int(os.getenv("PHASE1_GAP_FILL_ROUNDS", "1"))
PHASE1_ENABLE_CLAUDE_SCOUT = (
    os.getenv("PHASE1_ENABLE_CLAUDE_SCOUT", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_ENABLE_GEMINI_RESEARCH = (
    os.getenv("PHASE1_ENABLE_GEMINI_RESEARCH", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_MIN_VOC_QUOTES = int(os.getenv("PHASE1_MIN_VOC_QUOTES", "150"))
PHASE1_MIN_EVIDENCE_ITEMS = int(os.getenv("PHASE1_MIN_EVIDENCE_ITEMS", "250"))
PHASE1_MIN_COMPETITORS = int(os.getenv("PHASE1_MIN_COMPETITORS", "10"))
PHASE1_MIN_COMPETITORS_FLOOR = int(os.getenv("PHASE1_MIN_COMPETITORS_FLOOR", "4"))
PHASE1_TARGET_COMPETITORS = int(os.getenv("PHASE1_TARGET_COMPETITORS", "10"))
PHASE1_COMPETITOR_GATE_MODE = os.getenv("PHASE1_COMPETITOR_GATE_MODE", "dynamic_4_10").strip().lower()
PHASE1_MIN_SOURCE_TYPES = int(os.getenv("PHASE1_MIN_SOURCE_TYPES", "6"))
PHASE1_MIN_PROOFS_PER_TYPE = int(os.getenv("PHASE1_MIN_PROOFS_PER_TYPE", "2"))
PHASE1_STRICT_HARD_BLOCK = (
    os.getenv("PHASE1_STRICT_HARD_BLOCK", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_ENABLE_CHECKPOINTS = (
    os.getenv("PHASE1_ENABLE_CHECKPOINTS", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_REUSE_COLLECTOR_CHECKPOINT = (
    os.getenv("PHASE1_REUSE_COLLECTOR_CHECKPOINT", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_FORCE_FRESH_COLLECTORS = (
    os.getenv("PHASE1_FORCE_FRESH_COLLECTORS", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_CHECKPOINT_TTL_HOURS = int(os.getenv("PHASE1_CHECKPOINT_TTL_HOURS", "72"))
PHASE1_SYNTH_REPORT_MAX_CHARS = int(os.getenv("PHASE1_SYNTH_REPORT_MAX_CHARS", "100000"))
PHASE1_ENABLE_CONTRADICTION_DETECTION = (
    os.getenv("PHASE1_ENABLE_CONTRADICTION_DETECTION", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_STRICT_CONTRADICTION_BLOCK = (
    os.getenv("PHASE1_STRICT_CONTRADICTION_BLOCK", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_CONTRADICTION_USE_LLM = (
    os.getenv("PHASE1_CONTRADICTION_USE_LLM", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_ALLOW_VOC_RECYCLING = (
    os.getenv("PHASE1_ALLOW_VOC_RECYCLING", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_ALLOW_PROOF_CLONING = (
    os.getenv("PHASE1_ALLOW_PROOF_CLONING", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_GAP_FILL_MODE = os.getenv("PHASE1_GAP_FILL_MODE", "targeted_collectors").strip().lower()
PHASE1_TARGETED_COLLECTOR_MAX_ROUNDS = int(os.getenv("PHASE1_TARGETED_COLLECTOR_MAX_ROUNDS", "1"))
PHASE1_RETRY_STRATEGY = os.getenv("PHASE1_RETRY_STRATEGY", "single_focused_collector").strip().lower()
PHASE1_ENFORCE_SINGLE_RETRY = (
    os.getenv("PHASE1_ENFORCE_SINGLE_RETRY", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_RETRY_ROUNDS_MAX = int(os.getenv("PHASE1_RETRY_ROUNDS_MAX", "1"))
PHASE1_RETRY_ESCALATION_MODE = os.getenv("PHASE1_RETRY_ESCALATION_MODE", "none").strip().lower()
PHASE1_WARN_ON_UNRESOLVED_GATES = (
    os.getenv("PHASE1_WARN_ON_UNRESOLVED_GATES", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_ENABLE_VOC_COLLECTOR = (
    os.getenv("PHASE1_ENABLE_VOC_COLLECTOR", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_VOC_ENABLE_REDDIT = (
    os.getenv("PHASE1_VOC_ENABLE_REDDIT", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_VOC_ENABLE_AMAZON = (
    os.getenv("PHASE1_VOC_ENABLE_AMAZON", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_VOC_ENABLE_TRUSTPILOT = (
    os.getenv("PHASE1_VOC_ENABLE_TRUSTPILOT", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_VOC_REDDIT_SUBREDDITS = os.getenv(
    "PHASE1_VOC_REDDIT_SUBREDDITS",
    "OculusQuest,MetaQuestVR,VRGaming,virtualreality",
).strip()
PHASE1_VOC_AMAZON_ASINS = os.getenv("PHASE1_VOC_AMAZON_ASINS", "").strip()
PHASE1_VOC_TRUSTPILOT_DOMAINS = os.getenv("PHASE1_VOC_TRUSTPILOT_DOMAINS", "").strip()
PHASE1_VOC_TRUSTPILOT_MAX_PAGES = int(os.getenv("PHASE1_VOC_TRUSTPILOT_MAX_PAGES", "2"))
PHASE1_SEMANTIC_DEDUPE = (
    os.getenv("PHASE1_SEMANTIC_DEDUPE", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_ENABLE_EVIDENCE_SUMMARY = (
    os.getenv("PHASE1_ENABLE_EVIDENCE_SUMMARY", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_ENABLE_COMPETITOR_CATALOG_BACKFILL = (
    os.getenv("PHASE1_ENABLE_COMPETITOR_CATALOG_BACKFILL", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE1_URL_CANONICALIZE = (
    os.getenv("PHASE1_URL_CANONICALIZE", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)

# Phase 2 is intentionally blocked during the Foundation v2 migration.
PHASE2_MATRIX_ONLY_MODE = (
    os.getenv("PHASE2_MATRIX_ONLY_MODE", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE2_TEMPORARILY_DISABLED = (
    os.getenv("PHASE2_TEMPORARILY_DISABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE2_DISABLED_MESSAGE = (
    "Phase 2 is temporarily disabled pending Step 2 migration to Foundation v2."
)
PHASE3_TEMPORARILY_DISABLED = (
    os.getenv("PHASE3_TEMPORARILY_DISABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE3_DISABLED_MESSAGE = (
    "Phase 3 is temporarily disabled pending full rebuild."
)
PHASE3_V2_ENABLED = (
    os.getenv("PHASE3_V2_ENABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE3_V2_DEFAULT_PATH = os.getenv("PHASE3_V2_DEFAULT_PATH", "v2").strip().lower()
if PHASE3_V2_DEFAULT_PATH != "v2":
    PHASE3_V2_DEFAULT_PATH = "v2"
PHASE3_V2_DEFAULT_PILOT_SIZE = max(1, int(os.getenv("PHASE3_V2_DEFAULT_PILOT_SIZE", "20")))
PHASE3_V2_REVIEWER_ROLE_DEFAULT = os.getenv(
    "PHASE3_V2_REVIEWER_ROLE_DEFAULT", "client_founder"
).strip().lower() or "client_founder"
PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS = max(
    30,
    int(os.getenv("PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS", "900")),
)
PHASE3_V2_CORE_DRAFTER_MAX_PARALLEL = max(
    1,
    int(os.getenv("PHASE3_V2_CORE_DRAFTER_MAX_PARALLEL", "4")),
)
PHASE3_V2_HOOKS_ENABLED = (
    os.getenv("PHASE3_V2_HOOKS_ENABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE3_V2_HOOK_MAX_PARALLEL = max(
    1,
    int(os.getenv("PHASE3_V2_HOOK_MAX_PARALLEL", "4")),
)
PHASE3_V2_HOOK_CANDIDATES_PER_UNIT = max(
    1,
    int(os.getenv("PHASE3_V2_HOOK_CANDIDATES_PER_UNIT", "20")),
)
PHASE3_V2_HOOK_FINAL_VARIANTS_PER_UNIT = max(
    1,
    int(os.getenv("PHASE3_V2_HOOK_FINAL_VARIANTS_PER_UNIT", "5")),
)
PHASE3_V2_HOOK_MIN_NEW_VARIANTS = max(
    1,
    int(os.getenv("PHASE3_V2_HOOK_MIN_NEW_VARIANTS", "4")),
)
PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS = max(
    0,
    int(os.getenv("PHASE3_V2_HOOK_MAX_REPAIR_ROUNDS", "1")),
)
PHASE3_V2_HOOK_MIN_SCROLL_STOP_SCORE = max(
    0,
    min(100, int(os.getenv("PHASE3_V2_HOOK_MIN_SCROLL_STOP_SCORE", "75"))),
)
PHASE3_V2_HOOK_MIN_SPECIFICITY_SCORE = max(
    0,
    min(100, int(os.getenv("PHASE3_V2_HOOK_MIN_SPECIFICITY_SCORE", "70"))),
)
PHASE3_V2_HOOK_DIVERSITY_THRESHOLD = max(
    0.0,
    min(1.0, float(os.getenv("PHASE3_V2_HOOK_DIVERSITY_THRESHOLD", "0.85"))),
)
PHASE3_V2_HOOK_MIN_LANE_COVERAGE = max(
    1,
    int(os.getenv("PHASE3_V2_HOOK_MIN_LANE_COVERAGE", "3")),
)
PHASE3_V2_HOOK_MODEL_GENERATION = os.getenv(
    "PHASE3_V2_HOOK_MODEL_GENERATION",
    ANTHROPIC_FRONTIER,
).strip() or ANTHROPIC_FRONTIER
PHASE3_V2_HOOK_MODEL_GATE = os.getenv(
    "PHASE3_V2_HOOK_MODEL_GATE",
    OPENAI_FRONTIER,
).strip() or OPENAI_FRONTIER
PHASE3_V2_HOOK_MODEL_REPAIR = os.getenv(
    "PHASE3_V2_HOOK_MODEL_REPAIR",
    ANTHROPIC_FRONTIER,
).strip() or ANTHROPIC_FRONTIER
PHASE3_V2_HOOK_MODEL_RANK = os.getenv(
    "PHASE3_V2_HOOK_MODEL_RANK",
    OPENAI_FRONTIER,
).strip() or OPENAI_FRONTIER
PHASE3_V2_SCENES_ENABLED = (
    os.getenv("PHASE3_V2_SCENES_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE3_V2_SCENE_MAX_PARALLEL = max(
    1,
    int(os.getenv("PHASE3_V2_SCENE_MAX_PARALLEL", "4")),
)
PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS = max(
    0,
    int(os.getenv("PHASE3_V2_SCENE_MAX_REPAIR_ROUNDS", "1")),
)
PHASE3_V2_SCENE_MAX_DIFFICULTY = max(
    1,
    min(10, int(os.getenv("PHASE3_V2_SCENE_MAX_DIFFICULTY", "8"))),
)
PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE = max(
    1,
    int(os.getenv("PHASE3_V2_SCENE_MAX_CONSECUTIVE_MODE", "3")),
)
PHASE3_V2_SCENE_MIN_A_ROLL_LINES = max(
    1,
    int(os.getenv("PHASE3_V2_SCENE_MIN_A_ROLL_LINES", "1")),
)
PHASE3_V2_SCENE_MODEL_DRAFT = os.getenv(
    "PHASE3_V2_SCENE_MODEL_DRAFT",
    ANTHROPIC_FRONTIER,
).strip() or ANTHROPIC_FRONTIER
PHASE3_V2_SCENE_MODEL_REPAIR = os.getenv(
    "PHASE3_V2_SCENE_MODEL_REPAIR",
    ANTHROPIC_FRONTIER,
).strip() or ANTHROPIC_FRONTIER
PHASE3_V2_SCENE_MODEL_POLISH = os.getenv(
    "PHASE3_V2_SCENE_MODEL_POLISH",
    ANTHROPIC_FRONTIER,
).strip() or ANTHROPIC_FRONTIER
PHASE3_V2_SCENE_MODEL_GATE = os.getenv(
    "PHASE3_V2_SCENE_MODEL_GATE",
    OPENAI_FRONTIER,
).strip() or OPENAI_FRONTIER
PHASE3_V2_SDK_TOGGLES_DEFAULT: dict[str, bool] = {
    "core_script_drafter": False,
    "hook_generator": False,
    "scene_planner": False,
    "targeted_repair": False,
}

# ---------------------------------------------------------------------------
# Phase 4 v1 (Video generation test mode)
# ---------------------------------------------------------------------------

PHASE4_V1_ENABLED = (
    os.getenv("PHASE4_V1_ENABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN = (
    os.getenv("PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE4_V1_MAX_PARALLEL_CLIPS = max(
    1,
    int(os.getenv("PHASE4_V1_MAX_PARALLEL_CLIPS", "1")),
)
PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS = (
    os.getenv("PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
PHASE4_V1_DRIVE_SERVICE_ACCOUNT_JSON_PATH = os.getenv(
    "PHASE4_V1_DRIVE_SERVICE_ACCOUNT_JSON_PATH",
    "",
).strip()
PHASE4_V1_FAL_BROLL_MODEL_ID = (
    os.getenv("PHASE4_V1_FAL_BROLL_MODEL_ID", "fal-ai/minimax/video-01-live/image-to-video")
    .strip()
)
PHASE4_V1_FAL_TALKING_HEAD_MODEL_ID = (
    os.getenv("PHASE4_V1_FAL_TALKING_HEAD_MODEL_ID", "fal-ai/live-avatar")
    .strip()
)
PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID = (
    os.getenv("PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID", "gemini-2.5-flash-image")
    .strip()
)
PHASE4_V1_TTS_MODEL = os.getenv(
    "PHASE4_V1_TTS_MODEL",
    "gpt-4o-mini-tts",
).strip()
PHASE4_V1_TTS_SPEED = float(os.getenv("PHASE4_V1_TTS_SPEED", "1.0"))
PHASE4_V1_TTS_PITCH = float(os.getenv("PHASE4_V1_TTS_PITCH", "0.0"))
PHASE4_V1_TTS_GAIN_DB = float(os.getenv("PHASE4_V1_TTS_GAIN_DB", "0.0"))
PHASE4_V1_VOICE_PRESET_CATALOG_JSON = os.getenv(
    "PHASE4_V1_VOICE_PRESET_CATALOG_JSON",
    "",
).strip()

_DEFAULT_PHASE4_VOICE_PRESETS: list[dict[str, object]] = [
    {
        "voice_preset_id": "calm_female_en_us_v1",
        "name": "Calm Female (US)",
        "provider": "openai",
        "tts_model": PHASE4_V1_TTS_MODEL,
        "style": "balanced",
        "settings": {
            "speed": PHASE4_V1_TTS_SPEED,
            "pitch": PHASE4_V1_TTS_PITCH,
            "gain_db": PHASE4_V1_TTS_GAIN_DB,
        },
    },
    {
        "voice_preset_id": "clear_male_en_us_v1",
        "name": "Clear Male (US)",
        "provider": "openai",
        "tts_model": PHASE4_V1_TTS_MODEL,
        "style": "balanced",
        "settings": {
            "speed": PHASE4_V1_TTS_SPEED,
            "pitch": PHASE4_V1_TTS_PITCH,
            "gain_db": PHASE4_V1_TTS_GAIN_DB,
        },
    },
]

try:
    _phase4_voice_raw = json.loads(PHASE4_V1_VOICE_PRESET_CATALOG_JSON) if PHASE4_V1_VOICE_PRESET_CATALOG_JSON else []
except Exception:
    _phase4_voice_raw = []
if not isinstance(_phase4_voice_raw, list) or not _phase4_voice_raw:
    _phase4_voice_raw = _DEFAULT_PHASE4_VOICE_PRESETS

PHASE4_V1_VOICE_PRESETS: list[dict[str, object]] = []
for _row in _phase4_voice_raw:
    if not isinstance(_row, dict):
        continue
    voice_preset_id = str(_row.get("voice_preset_id") or "").strip()
    if not voice_preset_id:
        continue
    settings = _row.get("settings") if isinstance(_row.get("settings"), dict) else {}
    PHASE4_V1_VOICE_PRESETS.append(
        {
            "voice_preset_id": voice_preset_id,
            "name": str(_row.get("name") or voice_preset_id),
            "provider": str(_row.get("provider") or "openai"),
            "tts_model": str(_row.get("tts_model") or PHASE4_V1_TTS_MODEL),
            "style": str(_row.get("style") or "balanced"),
            "settings": {
                "speed": float(settings.get("speed", PHASE4_V1_TTS_SPEED)),
                "pitch": float(settings.get("pitch", PHASE4_V1_TTS_PITCH)),
                "gain_db": float(settings.get("gain_db", PHASE4_V1_TTS_GAIN_DB)),
            },
        }
    )
if not PHASE4_V1_VOICE_PRESETS:
    PHASE4_V1_VOICE_PRESETS = list(_DEFAULT_PHASE4_VOICE_PRESETS)

AGENT_LLM_CONFIG: dict[str, dict] = {
    # --- PHASE 1: RESEARCH ---
    # Foundation Research — default to Claude Opus 4.6 for Step 2 synthesis/QA quality.
    "foundation_research": {
        "provider": os.getenv("AGENT_FOUNDATION_RESEARCH_PROVIDER", os.getenv("AGENT_01A_PROVIDER", "anthropic")),
        "model": os.getenv("AGENT_FOUNDATION_RESEARCH_MODEL", os.getenv("AGENT_01A_MODEL", ANTHROPIC_FRONTIER)),
        "temperature": 0.4,
        "max_tokens": 50_000,
    },
    # --- ARCHITECTURE COUNCIL (pre-implementation design workflow) ---
    "architecture_requirements_miner": {
        "provider": os.getenv("AGENT_ARCH_REQ_MINER_PROVIDER", "google"),
        "model": os.getenv("AGENT_ARCH_REQ_MINER_MODEL", GOOGLE_FRONTIER),
        "temperature": 0.2,
        "max_tokens": 40_000,
    },
    "architecture_strategy_designer": {
        "provider": os.getenv("AGENT_ARCH_STRATEGY_PROVIDER", "openai"),
        "model": os.getenv("AGENT_ARCH_STRATEGY_MODEL", OPENAI_FRONTIER),
        "temperature": 0.6,
        "max_tokens": 28_000,
    },
    "architecture_red_team": {
        "provider": os.getenv("AGENT_ARCH_RED_TEAM_PROVIDER", "anthropic"),
        "model": os.getenv("AGENT_ARCH_RED_TEAM_MODEL", ANTHROPIC_FRONTIER),
        "temperature": 0.4,
        "max_tokens": 20_000,
    },
    "architecture_judge": {
        "provider": os.getenv("AGENT_ARCH_JUDGE_PROVIDER", "openai"),
        "model": os.getenv("AGENT_ARCH_JUDGE_MODEL", OPENAI_FRONTIER),
        "temperature": 0.2,
        "max_tokens": 18_000,
    },
    "architecture_blueprint_writer": {
        "provider": os.getenv("AGENT_ARCH_BLUEPRINT_PROVIDER", "openai"),
        "model": os.getenv("AGENT_ARCH_BLUEPRINT_MODEL", OPENAI_FRONTIER),
        "temperature": 0.3,
        "max_tokens": 20_000,
    },
    # --- PHASE 2: IDEATION ---
    # Creative Engine — receives foundation research directly, finds angles + builds ideas
    # Needs 40K+ tokens: 30 ideas with inline strategic grounding + distribution audit
    "creative_engine": {
        "provider": os.getenv("AGENT_CREATIVE_ENGINE_PROVIDER", os.getenv("AGENT_02_PROVIDER", "anthropic")),
        "model": os.getenv("AGENT_CREATIVE_ENGINE_MODEL", os.getenv("AGENT_02_MODEL", ANTHROPIC_FRONTIER)),
        "temperature": 0.9,
        "max_tokens": 40_000,
    },
    # --- PHASE 3: SCRIPTING ---
    # Copywriter — creative writing, strong language model
    "copywriter": {
        "provider": os.getenv("AGENT_COPYWRITER_PROVIDER", os.getenv("AGENT_04_PROVIDER", DEFAULT_PROVIDER)),
        "model": os.getenv("AGENT_COPYWRITER_MODEL", os.getenv("AGENT_04_MODEL", OPENAI_FRONTIER)),
        "temperature": 0.8,
        "max_tokens": 32_000,
    },
    # Hook Specialist — short-form creative + pattern knowledge
    "hook_specialist": {
        "provider": os.getenv("AGENT_HOOK_SPECIALIST_PROVIDER", os.getenv("AGENT_05_PROVIDER", DEFAULT_PROVIDER)),
        "model": os.getenv("AGENT_HOOK_SPECIALIST_MODEL", os.getenv("AGENT_05_MODEL", OPENAI_FRONTIER)),
        "temperature": 0.85,
        "max_tokens": 12_000,
    },
    # --- PIPELINE ORCHESTRATOR SUB-AGENTS ---
    "pipeline_doc_agent": {
        "provider": os.getenv("AGENT_PIPELINE_DOC_PROVIDER", "anthropic"),
        "model": os.getenv("AGENT_PIPELINE_DOC_MODEL", ANTHROPIC_FRONTIER),
        "temperature": 0.2,
        "max_tokens": 12_000,
    },
    "pipeline_map_agent": {
        "provider": os.getenv("AGENT_PIPELINE_MAP_PROVIDER", "anthropic"),
        "model": os.getenv("AGENT_PIPELINE_MAP_MODEL", ANTHROPIC_FRONTIER),
        "temperature": 0.2,
        "max_tokens": 12_000,
    },
    "pipeline_observability_agent": {
        "provider": os.getenv("AGENT_PIPELINE_OBSERVABILITY_PROVIDER", "anthropic"),
        "model": os.getenv("AGENT_PIPELINE_OBSERVABILITY_MODEL", ANTHROPIC_FRONTIER),
        "temperature": 0.1,
        "max_tokens": 10_000,
    },
    "pipeline_strategy_ux_agent": {
        "provider": os.getenv("AGENT_PIPELINE_STRATEGY_UX_PROVIDER", "anthropic"),
        "model": os.getenv("AGENT_PIPELINE_STRATEGY_UX_MODEL", ANTHROPIC_FRONTIER),
        "temperature": 0.3,
        "max_tokens": 12_000,
    },
    # --- PHASE 4: PRODUCTION ---
    # 08: screen writer — visual creative direction
    "agent_08": {
        "provider": os.getenv("AGENT_08_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_08_MODEL", OPENAI_FRONTIER),
        "temperature": 0.7,
        "max_tokens": 14_000,
    },
    # 09: clip maker — technical asset assignment, mechanical
    "agent_09": {
        "provider": os.getenv("AGENT_09_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_09_MODEL", OPENAI_MINI),
        "temperature": 0.5,
        "max_tokens": 10_000,
    },
    # 10: AI UGC maker — technical direction, mechanical
    "agent_10": {
        "provider": os.getenv("AGENT_10_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_10_MODEL", OPENAI_MINI),
        "temperature": 0.5,
        "max_tokens": 10_000,
    },
    # --- PHASE 5: QA & LAUNCH ---
    # 11: clip verify — QA checklist, deterministic
    "agent_11": {
        "provider": os.getenv("AGENT_11_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_11_MODEL", OPENAI_MINI),
        "temperature": 0.3,
        "max_tokens": 8_000,
    },
    # 12: compliance — MUST be precise, zero hallucination tolerance
    "agent_12": {
        "provider": os.getenv("AGENT_12_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_12_MODEL", OPENAI_FRONTIER),
        "temperature": 0.3,
        "max_tokens": 14_000,
    },
    # 13: pre-launch QA — measurement setup, structured
    "agent_13": {
        "provider": os.getenv("AGENT_13_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_13_MODEL", OPENAI_MINI),
        "temperature": 0.4,
        "max_tokens": 8_000,
    },
    # 14: launch to Meta — API config, deterministic
    "agent_14": {
        "provider": os.getenv("AGENT_14_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_14_MODEL", OPENAI_MINI),
        "temperature": 0.3,
        "max_tokens": 10_000,
    },
    # --- PHASE 6: ANALYZE & SCALE ---
    # 15A: performance analyzer — data interpretation, strong reasoning
    "agent_15a": {
        "provider": os.getenv("AGENT_15A_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_15A_MODEL", OPENAI_FRONTIER),
        "temperature": 0.5,
        "max_tokens": 14_000,
    },
    # 15B: learning updater — pattern synthesis
    "agent_15b": {
        "provider": os.getenv("AGENT_15B_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_15B_MODEL", OPENAI_FRONTIER),
        "temperature": 0.6,
        "max_tokens": 12_000,
    },
    # 16: winner scaling — strategic decisions
    "agent_16": {
        "provider": os.getenv("AGENT_16_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_16_MODEL", OPENAI_FRONTIER),
        "temperature": 0.5,
        "max_tokens": 12_000,
    },
}

# Legacy slug compatibility while migrating to descriptive slugs.
for _legacy, _canonical in {
    "agent_01a": "foundation_research",
    "agent_02": "creative_engine",
    "agent_04": "copywriter",
    "agent_05": "hook_specialist",
}.items():
    if _canonical in AGENT_LLM_CONFIG and _legacy not in AGENT_LLM_CONFIG:
        AGENT_LLM_CONFIG[_legacy] = dict(AGENT_LLM_CONFIG[_canonical])


def get_agent_llm_config(agent_slug: str) -> dict:
    """Return the LLM config for a specific agent, with defaults."""
    defaults = {
        "provider": DEFAULT_PROVIDER,
        "model": DEFAULT_MODEL,
        "temperature": 0.7,
        "max_tokens": 16_000,
    }
    agent_conf = AGENT_LLM_CONFIG.get(agent_slug, {})
    return {**defaults, **agent_conf}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Ensure output dir exists
OUTPUT_DIR.mkdir(exist_ok=True)
