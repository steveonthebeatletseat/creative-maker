"""Pipeline configuration — LLM providers, per-agent model assignments, paths."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).parent
OUTPUT_DIR = ROOT_DIR / os.getenv("OUTPUT_DIR", "outputs")
RESEARCH_DIR = ROOT_DIR / "5 research files"

# ---------------------------------------------------------------------------
# LLM Provider API Keys
# ---------------------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")

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
# Override any agent via env: AGENT_01A_PROVIDER=anthropic
#                             AGENT_01A_MODEL=claude-opus-4-20250514
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

# Legacy Anthropic Messages API fallback settings for Step 2.
CREATIVE_SCOUT_WEB_MAX_USES = int(os.getenv("CREATIVE_SCOUT_WEB_MAX_USES", "20"))
CREATIVE_SCOUT_WEB_MAX_TOKENS = int(os.getenv("CREATIVE_SCOUT_WEB_MAX_TOKENS", "20000"))

AGENT_LLM_CONFIG: dict[str, dict] = {
    # --- PHASE 1: RESEARCH ---
    # 1A: Foundation Research — Gemini 2.5 Pro (1M context, 65K output, strong reasoning)
    "agent_01a": {
        "provider": os.getenv("AGENT_01A_PROVIDER", "google"),
        "model": os.getenv("AGENT_01A_MODEL", GOOGLE_FRONTIER),
        "temperature": 0.4,
        "max_tokens": 50_000,
    },
    # --- PHASE 2: IDEATION ---
    # 02: Creative Engine — receives 1A + 1B directly, finds angles + builds ideas
    # Needs 40K+ tokens: 30 ideas with inline strategic grounding + distribution audit
    "agent_02": {
        "provider": os.getenv("AGENT_02_PROVIDER", "anthropic"),
        "model": os.getenv("AGENT_02_MODEL", ANTHROPIC_FRONTIER),
        "temperature": 0.9,
        "max_tokens": 40_000,
    },
    # --- PHASE 3: SCRIPTING ---
    # 04: copywriter — creative writing, strong language model
    "agent_04": {
        "provider": os.getenv("AGENT_04_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_04_MODEL", OPENAI_FRONTIER),
        "temperature": 0.8,
        "max_tokens": 32_000,
    },
    # 05: hook specialist — short-form creative + pattern knowledge
    "agent_05": {
        "provider": os.getenv("AGENT_05_PROVIDER", DEFAULT_PROVIDER),
        "model": os.getenv("AGENT_05_MODEL", OPENAI_FRONTIER),
        "temperature": 0.85,
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
