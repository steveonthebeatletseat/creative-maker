from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.responses import JSONResponse

import server
from pipeline.phase3_v2_engine import (
    build_evidence_pack,
    compile_script_spec_v1,
    evaluate_m1_gates,
    expand_brief_units,
    run_phase3_v2_m1,
)
from schemas.phase3_v2 import CoreScriptDraftV1, CoreScriptLineV1, CoreScriptSectionsV1


MATRIX_PLAN = {
    "schema_version": "matrix_plan_v1",
    "awareness_axis": {"levels": ["unaware", "problem_aware"]},
    "emotion_axis": {
        "rows": [
            {"emotion_key": "frustration_pain", "emotion_label": "Frustration / Pain"},
            {
                "emotion_key": "desire_freedom_immersion",
                "emotion_label": "Desire for Freedom / Immersion",
            },
        ]
    },
    "cells": [
        {
            "awareness_level": "unaware",
            "emotion_key": "frustration_pain",
            "emotion_label": "Frustration / Pain",
            "brief_count": 2,
        },
        {
            "awareness_level": "unaware",
            "emotion_key": "desire_freedom_immersion",
            "emotion_label": "Desire for Freedom / Immersion",
            "brief_count": 1,
        },
        {
            "awareness_level": "problem_aware",
            "emotion_key": "frustration_pain",
            "emotion_label": "Frustration / Pain",
            "brief_count": 1,
        },
        {
            "awareness_level": "problem_aware",
            "emotion_key": "desire_freedom_immersion",
            "emotion_label": "Desire for Freedom / Immersion",
            "brief_count": 3,
        },
    ],
}


def foundation_with_coverage() -> dict:
    return {
        "schema_version": "2.0",
        "pillar_2_voc_language_bank": {
            "quotes": [
                {
                    "quote_id": "q_1",
                    "quote": "I hate battery swaps mid game.",
                    "dominant_emotion": "frustration_pain",
                    "source_url": "https://example.com/reviews/1",
                    "source_type": "review",
                }
            ]
        },
        "pillar_4_product_mechanism_analysis": {
            "primary_mechanism_name": "Balanced Weight Strap",
            "why_problem_exists": "Most straps front-load weight and cause face pressure.",
            "why_solution_uniquely_works": "Rear battery counterbalances headset load.",
            "mechanism_supporting_evidence_ids": ["proof_1"],
        },
        "pillar_7_proof_credibility_inventory": {
            "assets": [
                {
                    "asset_id": "proof_1",
                    "proof_type": "testimonial",
                    "title": "Comfort increase",
                    "detail": "Users report much longer sessions.",
                    "source_url": "https://example.com/proof/1",
                }
            ]
        },
    }


class Phase3V2M1EngineTests(unittest.TestCase):
    def test_expand_brief_units_round_robin_and_deterministic(self):
        units_a = expand_brief_units(
            MATRIX_PLAN,
            branch_id="branch_1",
            brand_slug="brand_x",
            pilot_size=5,
            selection_strategy="round_robin",
        )
        units_b = expand_brief_units(
            MATRIX_PLAN,
            branch_id="branch_1",
            brand_slug="brand_x",
            pilot_size=5,
            selection_strategy="round_robin",
        )

        ids = [u.brief_unit_id for u in units_a]
        self.assertEqual(
            ids,
            [
                "bu_unaware_frustration_pain_001",
                "bu_unaware_desire_freedom_immersion_001",
                "bu_problem_aware_frustration_pain_001",
                "bu_problem_aware_desire_freedom_immersion_001",
                "bu_unaware_frustration_pain_002",
            ],
        )
        self.assertEqual([u.model_dump() for u in units_a], [u.model_dump() for u in units_b])

    def test_evidence_pack_coverage_flags(self):
        unit = expand_brief_units(
            MATRIX_PLAN,
            branch_id="branch_1",
            brand_slug="brand_x",
            pilot_size=1,
            selection_strategy="round_robin",
        )[0]

        good_pack = build_evidence_pack(unit, foundation_with_coverage())
        self.assertTrue(good_pack.coverage_report.has_voc)
        self.assertTrue(good_pack.coverage_report.has_proof)
        self.assertTrue(good_pack.coverage_report.has_mechanism)
        self.assertFalse(good_pack.coverage_report.blocked_evidence_insufficient)

        missing_mechanism = foundation_with_coverage()
        missing_mechanism["pillar_4_product_mechanism_analysis"] = {}
        blocked_pack = build_evidence_pack(unit, missing_mechanism)
        self.assertFalse(blocked_pack.coverage_report.has_mechanism)
        self.assertTrue(blocked_pack.coverage_report.blocked_evidence_insufficient)

    def test_evaluate_m1_gates_rejects_missing_line_citations(self):
        unit = expand_brief_units(
            MATRIX_PLAN,
            branch_id="branch_1",
            brand_slug="brand_x",
            pilot_size=1,
            selection_strategy="round_robin",
        )[0]
        pack = build_evidence_pack(unit, foundation_with_coverage())
        spec = compile_script_spec_v1(unit, pack)

        draft = CoreScriptDraftV1(
            script_id="script_1",
            brief_unit_id=unit.brief_unit_id,
            arm="control",
            status="ok",
            sections=CoreScriptSectionsV1(
                hook="Hook",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            lines=[CoreScriptLineV1(line_id="L01", text="One line", evidence_ids=[])],
            model_metadata={"provider": "openai", "model": "gpt-test", "sdk_used": False},
        )

        report = evaluate_m1_gates(draft, spec=spec, evidence_pack=pack)
        self.assertFalse(report["overall_pass"])
        line_gate = next(c for c in report["checks"] if c["gate_id"] == "line_citations_valid")
        self.assertFalse(line_gate["passed"])

    def test_run_phase3_v2_m1_arm_selection_respects_ab_and_sdk_toggle(self):
        def _fake_draft(unit, spec, pack, *, run_mode, provider=None, model=None):
            _ = spec, provider, model
            return CoreScriptDraftV1(
                script_id=f"script_{unit.brief_unit_id}_{run_mode}",
                brief_unit_id=unit.brief_unit_id,
                arm=run_mode,
                status="ok",
                sections=CoreScriptSectionsV1(
                    hook="Hook",
                    problem="Problem",
                    mechanism="Mechanism",
                    proof="Proof",
                    cta="CTA",
                ),
                lines=[CoreScriptLineV1(line_id="L01", text="Line", evidence_ids=["proof_1"])],
                model_metadata={"provider": "openai", "model": "gpt-test", "sdk_used": run_mode == "claude_sdk"},
            )

        with patch("pipeline.phase3_v2_engine.draft_core_script_v1", side_effect=_fake_draft), patch(
            "pipeline.phase3_v2_engine.evaluate_m1_gates",
            return_value={"overall_pass": True, "checks": []},
        ):
            ab_run = run_phase3_v2_m1(
                matrix_plan=MATRIX_PLAN,
                foundation_brief=foundation_with_coverage(),
                branch_id="branch_1",
                brand_slug="brand_x",
                pilot_size=2,
                ab_mode=True,
                sdk_toggles={"core_script_drafter": True},
            )
            self.assertEqual(ab_run["arms"], ["control", "claude_sdk"])

            single_sdk_run = run_phase3_v2_m1(
                matrix_plan=MATRIX_PLAN,
                foundation_brief=foundation_with_coverage(),
                branch_id="branch_1",
                brand_slug="brand_x",
                pilot_size=2,
                ab_mode=False,
                sdk_toggles={"core_script_drafter": True},
            )
            self.assertEqual(single_sdk_run["arms"], ["claude_sdk"])

    def test_run_phase3_v2_m1_isolates_per_unit_failures(self):
        call_count = {"n": 0}

        def _mixed_draft(unit, spec, pack, *, run_mode, provider=None, model=None):
            _ = spec, pack, provider, model
            call_count["n"] += 1
            if call_count["n"] == 1:
                return CoreScriptDraftV1(
                    script_id=f"script_{unit.brief_unit_id}_{run_mode}",
                    brief_unit_id=unit.brief_unit_id,
                    arm=run_mode,
                    status="error",
                    error="simulated_failure",
                    model_metadata={"provider": "openai", "model": "gpt-test", "sdk_used": False},
                )
            return CoreScriptDraftV1(
                script_id=f"script_{unit.brief_unit_id}_{run_mode}",
                brief_unit_id=unit.brief_unit_id,
                arm=run_mode,
                status="ok",
                sections=CoreScriptSectionsV1(
                    hook="Hook",
                    problem="Problem",
                    mechanism="Mechanism",
                    proof="Proof",
                    cta="CTA",
                ),
                lines=[CoreScriptLineV1(line_id="L01", text="Line", evidence_ids=["proof_1"])],
                model_metadata={"provider": "openai", "model": "gpt-test", "sdk_used": False},
            )

        with patch("pipeline.phase3_v2_engine.draft_core_script_v1", side_effect=_mixed_draft), patch(
            "pipeline.phase3_v2_engine.evaluate_m1_gates",
            return_value={"overall_pass": False, "checks": []},
        ):
            result = run_phase3_v2_m1(
                matrix_plan=MATRIX_PLAN,
                foundation_brief=foundation_with_coverage(),
                branch_id="branch_1",
                brand_slug="brand_x",
                pilot_size=2,
                ab_mode=False,
                sdk_toggles={"core_script_drafter": False},
            )

        drafts = result["drafts_by_arm"]["control"]
        self.assertEqual(len(drafts), 2)
        self.assertEqual([d["status"] for d in drafts], ["error", "ok"])


class Phase3V2M1ApiTests(unittest.TestCase):
    def setUp(self):
        server.pipeline_state["running"] = False
        server.pipeline_state["active_brand_slug"] = "brand_x"

    def test_prepare_returns_clean_error_when_matrix_missing(self):
        def _inject_foundation(inputs, brand_slug=""):
            _ = brand_slug
            inputs["foundation_brief"] = foundation_with_coverage()
            return None

        with patch("server.config.PHASE3_V2_ENABLED", True), patch(
            "server._get_branch", return_value={"id": "branch_1"}
        ), patch("server._ensure_foundation_for_creative_engine", side_effect=_inject_foundation), patch(
            "server._load_matrix_plan_for_branch",
            return_value=(None, "No matrix plan found for this branch. Run Phase 2 first."),
        ):
            resp = asyncio.run(
                server.api_phase3_v2_prepare("branch_1", brand="brand_x")
            )

        self.assertIsInstance(resp, JSONResponse)
        self.assertEqual(resp.status_code, 400)
        payload = json.loads(resp.body.decode("utf-8"))
        self.assertIn("No matrix plan found", payload.get("error", ""))

    def test_run_routes_to_single_sdk_arm_when_ab_off_and_sdk_toggle_on(self):
        def _inject_foundation(inputs, brand_slug=""):
            _ = brand_slug
            inputs["foundation_brief"] = foundation_with_coverage()
            return None

        async def _noop_execute(**kwargs):
            _ = kwargs
            return None

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ), patch("server._get_branch", return_value={"id": "branch_1"}), patch(
                "server._ensure_foundation_for_creative_engine", side_effect=_inject_foundation
            ), patch(
                "server._load_matrix_plan_for_branch",
                return_value=(MATRIX_PLAN, None),
            ), patch("server._phase3_v2_execute_run", side_effect=_noop_execute):
                req = server.Phase3V2RunRequest(
                    brand="brand_x",
                    pilot_size=3,
                    ab_mode=False,
                    sdk_toggles={"core_script_drafter": True},
                )
                resp = asyncio.run(server.api_phase3_v2_run("branch_1", req))

        self.assertEqual(resp.get("status"), "started")
        self.assertEqual(resp.get("arms"), ["claude_sdk"])
        self.assertEqual(resp.get("arm_ids"), ["claude_sdk"])

    def test_review_submission_updates_summary_winner(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_test_run"
        brief_unit_id = "bu_unaware_frustration_pain_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ):
                server.pipeline_state["active_brand_slug"] = brand_slug
                server._save_branches(
                    brand_slug,
                    [
                        {
                            "id": branch_id,
                            "label": "Default",
                            "status": "ready",
                            "available_agents": ["creative_engine"],
                            "completed_agents": ["creative_engine"],
                            "failed_agents": [],
                            "inputs": {},
                        }
                    ],
                )

                server._save_phase3_v2_runs_manifest(
                    brand_slug,
                    branch_id,
                    [
                        {
                            "run_id": run_id,
                            "status": "completed",
                            "created_at": "2026-02-17T00:00:00",
                            "arms": ["control", "claude_sdk"],
                            "reviewer_role": "client_founder",
                        }
                    ],
                )

                run_dir = server._phase3_v2_run_dir(brand_slug, branch_id, run_id)
                server._phase3_v2_write_json(
                    run_dir / "brief_units.json",
                    [
                        {
                            "brief_unit_id": brief_unit_id,
                            "matrix_cell_id": "cell_unaware_frustration_pain",
                            "awareness_level": "unaware",
                            "emotion_key": "frustration_pain",
                            "emotion_label": "Frustration / Pain",
                        }
                    ],
                )
                server._phase3_v2_write_json(run_dir / "evidence_packs.json", [])
                server._phase3_v2_write_json(run_dir / "reviews.json", [])
                server._phase3_v2_write_json(run_dir / "summary.json", {})
                server._phase3_v2_write_json(
                    run_dir / "arm_control_core_scripts.json",
                    [
                        {
                            "brief_unit_id": brief_unit_id,
                            "status": "ok",
                            "gate_report": {"overall_pass": True},
                            "latency_seconds": 1.4,
                            "cost_usd": 0.10,
                        }
                    ],
                )
                server._phase3_v2_write_json(
                    run_dir / "arm_claude_sdk_core_scripts.json",
                    [
                        {
                            "brief_unit_id": brief_unit_id,
                            "status": "ok",
                            "gate_report": {"overall_pass": True},
                            "latency_seconds": 2.2,
                            "cost_usd": 0.20,
                        }
                    ],
                )

                req = server.Phase3V2ReviewRequest(
                    run_id=run_id,
                    brand=brand_slug,
                    reviews=[
                        server.Phase3V2ReviewPayload(
                            brief_unit_id=brief_unit_id,
                            arm="control",
                            quality_score_1_10=9,
                            decision="approve",
                        ),
                        server.Phase3V2ReviewPayload(
                            brief_unit_id=brief_unit_id,
                            arm="claude_sdk",
                            quality_score_1_10=7,
                            decision="revise",
                        ),
                    ],
                )
                resp = asyncio.run(server.api_phase3_v2_reviews(branch_id, req))

                self.assertTrue(resp.get("ok"))
                self.assertEqual(resp.get("review_count"), 2)
                self.assertEqual(resp.get("summary", {}).get("winner"), "control")

                saved_summary = json.loads((run_dir / "summary.json").read_text("utf-8"))
                self.assertEqual(saved_summary.get("winner"), "control")


if __name__ == "__main__":
    unittest.main()
