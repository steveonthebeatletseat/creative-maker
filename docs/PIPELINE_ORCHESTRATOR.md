# Pipeline Orchestrator (LLM Sub-Agents, Review First)

This runs 4 LLM sub-agents in sequence:

1. `DocAgent`
2. `MapAgent`
3. `ObservabilityAgent`
4. `StrategyUXAgent`

## Commands

Run review first:

```bash
python3 scripts/pipeline_orchestrator.py review --change "Added new product step"
```

Apply only safe fixes from that review:

```bash
python3 scripts/pipeline_orchestrator.py apply
```

Council alias:

```bash
python3 scripts/pipeline_orchestrator.py council-of-dogs --mode review --change "Added new product step"
python3 scripts/pipeline_orchestrator.py council-of-dogs --mode apply
```

Scene Writer target:

```bash
python3 scripts/pipeline_orchestrator.py council-of-dogs --mode review --target scene_writer --change "Run scene writer audit"
python3 scripts/pipeline_orchestrator.py council-of-dogs --mode apply
```

Hook + Foundation target:

```bash
python3 scripts/pipeline_orchestrator.py council-of-dogs --mode review --target hook_foundation_research --change "Run hook + foundation audit"
python3 scripts/pipeline_orchestrator.py council-of-dogs --mode apply
```

## What it does

- `review`: each sub-agent analyzes its domain and proposes concrete edit operations.
- `apply`: executes proposed edits in order, then re-runs `review` for verification.

Domain focus:

- `DocAgent`: updates/syncs `PIPELINE_ARCHITECTURE.md`.
- `MapAgent`: updates/syncs pipeline map constants in `static/app.js`.
- `ObservabilityAgent`: updates/checks live log wiring in `server.py`.
- `StrategyUXAgent`: enforces `prompts/architecture_council/strategy_designer.md` contract and strategy alignment.

## Target scopes

- `pipeline_core`: default architecture/map/logging/strategy sync.
- `scene_writer`: scene-writer-specific audit across architecture docs, scene writer UI map/state wiring, scene logs, and strategy alignment.
- `hook_foundation_research`: hook-generator + foundation-research-specific audit across architecture docs, UI map/state wiring, logs, and strategy alignment.

## Model config

Defaults are in `config.py` under:

- `pipeline_doc_agent`
- `pipeline_map_agent`
- `pipeline_observability_agent`
- `pipeline_strategy_ux_agent`

Override via `.env`:

- `AGENT_PIPELINE_DOC_PROVIDER`, `AGENT_PIPELINE_DOC_MODEL`
- `AGENT_PIPELINE_MAP_PROVIDER`, `AGENT_PIPELINE_MAP_MODEL`
- `AGENT_PIPELINE_OBSERVABILITY_PROVIDER`, `AGENT_PIPELINE_OBSERVABILITY_MODEL`
- `AGENT_PIPELINE_STRATEGY_UX_PROVIDER`, `AGENT_PIPELINE_STRATEGY_UX_MODEL`

## Reports

Review output is written to:

- `outputs/orchestrator/last_review.md`
- `outputs/orchestrator/last_review.json`
