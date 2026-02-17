# Architecture Council Runbook

## Purpose
Use a multi-agent council to select the best Phase 1 architecture-mining strategy before implementation.

## Run Command
```bash
python3 main.py architecture-council --source PIPELINE_ARCHITECTURE.md
```

Optional custom goal:
```bash
python3 main.py architecture-council \
  --source PIPELINE_ARCHITECTURE.md \
  --goal "Maximize Phase 1 data quality for downstream script reliability."
```

## Where Outputs Go
Default folder:
- `outputs/architecture_council/`

Artifacts:
- `architecture_requirements_brief.json`: extracted non-negotiables and quality dimensions
- `architecture_strategy_portfolio.json`: 3 candidate multi-agent strategies
- `architecture_red_team_reports.json`: adversarial risk analysis for each strategy
- `architecture_decision_report.json`: scorecard + winner
- `architecture_blueprint_report.json`: operator-friendly workflow for the winner
- `architecture_council_run.json`: full combined package

## What To Read First
1. `architecture_decision_report.json`
2. `architecture_blueprint_report.json`
3. `architecture_red_team_reports.json`

## Decision Rule
Use the winner in `architecture_decision_report.json` as your locked strategy for the next decision window.
Only reopen architecture when a listed `reopen_trigger` is hit.
