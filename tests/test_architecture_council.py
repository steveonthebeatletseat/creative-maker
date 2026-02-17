from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.architecture_council import run_architecture_council
from schemas.architecture_council import (
    AgentNode,
    BlueprintReport,
    CouncilRequirement,
    DecisionReport,
    OptionScore,
    QualityDimension,
    RedTeamReport,
    RequirementsBrief,
    RiskFinding,
    StrategyOption,
    StrategyPortfolio,
    WorkflowStage,
)


def _requirements() -> RequirementsBrief:
    reqs = [
        CouncilRequirement(
            requirement_id=f"REQ-{i:03d}",
            title=f"Requirement {i}",
            description="Must preserve source fidelity and traceability.",
            downstream_dependency="Phase 2 and script quality",
            source_anchor="Phase 1 section",
            priority="critical" if i <= 3 else "high",
        )
        for i in range(1, 7)
    ]
    dims = [
        QualityDimension(
            name=f"Dimension {i}",
            why_it_matters="Downstream quality depends on this.",
            measurement="Binary gate",
            pass_threshold="Pass",
        )
        for i in range(1, 6)
    ]
    return RequirementsBrief(
        objective_statement="Max quality extraction",
        scope_boundaries=["Phase 1 design only"],
        non_negotiables=reqs,
        quality_dimensions=dims,
        known_unknowns=["Final architecture may change"],
    )


def _option(option_id: str, name: str) -> StrategyOption:
    nodes = [
        AgentNode(
            agent_name=f"Agent {i}",
            purpose="Analyze quality risks",
            input_contract=["requirements brief"],
            output_contract=["structured artifact"],
            pass_fail_gate="Gate pass",
        )
        for i in range(1, 5)
    ]
    return StrategyOption(
        option_id=option_id,
        name=name,
        summary="High-quality multi-agent design",
        design_principles=["traceability", "quality gates", "determinism"],
        agent_graph=nodes,
        qa_gates=["gate 1", "gate 2", "gate 3", "gate 4"],
        strengths=["strong quality", "clear artifacts", "risk visibility"],
        risks=["complexity", "latency", "prompt drift"],
        best_for="High-stakes outputs",
    )


def _red_team(option_id: str) -> RedTeamReport:
    findings = [
        RiskFinding(
            severity="high" if i < 3 else "medium",
            failure_mode=f"Failure {i}",
            why_likely="Likely due to missing controls",
            downstream_damage="Lower script quality",
            mitigation="Add stronger gate",
            validation_test="Run gate test",
        )
        for i in range(1, 6)
    ]
    return RedTeamReport(
        option_id=option_id,
        overall_risk_level="high",
        findings=findings,
        kill_criteria=["If traceability cannot be proven"],
    )


def _decision() -> DecisionReport:
    scores = [
        OptionScore(
            option_id="option_a",
            quality_fidelity=8,
            traceability=8,
            robustness=8,
            downstream_fit=7,
            operational_clarity=7,
            weighted_total=7.8,
            justification="Strong but not best",
        ),
        OptionScore(
            option_id="option_b",
            quality_fidelity=10,
            traceability=9,
            robustness=9,
            downstream_fit=9,
            operational_clarity=8,
            weighted_total=9.2,
            justification="Best quality and reliability",
        ),
        OptionScore(
            option_id="option_c",
            quality_fidelity=7,
            traceability=7,
            robustness=7,
            downstream_fit=7,
            operational_clarity=7,
            weighted_total=7.0,
            justification="Balanced but weaker",
        ),
    ]
    return DecisionReport(
        winner_option_id="option_b",
        winner_name="Hybrid Reliability",
        confidence=0.82,
        scorecard=scores,
        reasons_winner_beats_runner_up=["Higher source fidelity", "Better risk controls"],
        reopen_triggers=["Traceability under 95%", "Gate failures exceed threshold"],
    )


def _blueprint() -> BlueprintReport:
    workflow = [
        WorkflowStage(
            stage_name=f"Stage {i}",
            objective="Complete stage objective",
            owner_agent="Council Agent",
            required_inputs=["prior artifact"],
            output_artifact=f"artifact_{i}.json",
            gate_to_pass="Quality gate",
        )
        for i in range(1, 6)
    ]
    return BlueprintReport(
        mission="Maximize Phase 1 mining quality",
        recommended_strategy_id="option_b",
        recommended_strategy_name="Hybrid Reliability",
        workflow=workflow,
        operator_checklist=[
            "Confirm source file",
            "Run council",
            "Review winner",
            "Review risks",
            "Lock decision window",
        ],
        quality_dashboard=[
            "Traceability coverage",
            "Gate pass rate",
            "Consistency score",
            "Unknowns unresolved count",
        ],
        first_round_deliverables=[
            "Requirements brief",
            "Strategy portfolio",
            "Red-team reports",
            "Decision memo",
        ],
    )


class ArchitectureCouncilTests(unittest.TestCase):
    @patch("pipeline.architecture_council.call_llm_structured")
    def test_run_architecture_council_persists_artifacts(self, mock_call):
        requirements = _requirements()
        portfolio = StrategyPortfolio(
            options=[
                _option("option_a", "Conservative"),
                _option("option_b", "Hybrid Reliability"),
                _option("option_c", "Aggressive"),
            ]
        )
        red_reports = [
            _red_team("option_a"),
            _red_team("option_b"),
            _red_team("option_c"),
        ]
        decision = _decision()
        blueprint = _blueprint()

        responses = {
            "RequirementsBrief": [requirements],
            "StrategyPortfolio": [portfolio],
            "RedTeamReport": list(red_reports),
            "DecisionReport": [decision],
            "BlueprintReport": [blueprint],
        }

        def _side_effect(*, response_model, **kwargs):
            queue = responses[response_model.__name__]
            return queue.pop(0)

        mock_call.side_effect = _side_effect

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "PIPELINE_ARCHITECTURE.md"
            output_dir = root / "outputs"
            source.write_text("Phase 1 architecture and quality details.", "utf-8")

            run = run_architecture_council(
                source_path=source,
                output_dir=output_dir,
                parallel_red_team=False,
            )

            self.assertEqual(run.decision_report.winner_option_id, "option_b")
            self.assertEqual(mock_call.call_count, 7)

            expected_files = [
                "architecture_requirements_brief.json",
                "architecture_strategy_portfolio.json",
                "architecture_red_team_reports.json",
                "architecture_decision_report.json",
                "architecture_blueprint_report.json",
                "architecture_council_run.json",
            ]
            for name in expected_files:
                path = output_dir / name
                self.assertTrue(path.exists(), msg=f"Missing artifact: {name}")
                self.assertTrue(json.loads(path.read_text("utf-8")))

    def test_missing_source_raises_file_not_found(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = Path(tmpdir) / "missing.md"
            with self.assertRaises(FileNotFoundError):
                run_architecture_council(source_path=missing)


if __name__ == "__main__":
    unittest.main()
