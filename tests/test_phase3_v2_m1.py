from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.responses import JSONResponse

import config
import server
from pipeline.phase3_v2_engine import (
    build_evidence_pack,
    compile_script_spec_v1,
    draft_core_script_v1,
    evaluate_m1_gates,
    expand_brief_units,
    run_phase3_v2_m1,
)
from schemas.phase3_v2 import (
    Phase3V2ChatReplyV1,
    CoreScriptDraftV1,
    CoreScriptGeneratedV1,
    CoreScriptLineV1,
    CoreScriptSectionsV1,
)


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

    def test_run_phase3_v2_m1_arm_selection_forces_claude_sdk_only(self):
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
                model_metadata={"provider": "anthropic", "model": "claude-opus-4-6", "sdk_used": run_mode == "claude_sdk"},
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
            self.assertEqual(ab_run["arms"], ["claude_sdk"])

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

            no_sdk_toggle_run = run_phase3_v2_m1(
                matrix_plan=MATRIX_PLAN,
                foundation_brief=foundation_with_coverage(),
                branch_id="branch_1",
                brand_slug="brand_x",
                pilot_size=2,
                ab_mode=False,
                sdk_toggles={"core_script_drafter": False},
            )
            self.assertEqual(no_sdk_toggle_run["arms"], ["claude_sdk"])

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
                    model_metadata={"provider": "anthropic", "model": "claude-opus-4-6", "sdk_used": run_mode == "claude_sdk"},
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
                model_metadata={"provider": "anthropic", "model": "claude-opus-4-6", "sdk_used": run_mode == "claude_sdk"},
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

        drafts = result["drafts_by_arm"]["claude_sdk"]
        self.assertEqual(len(drafts), 2)
        self.assertEqual([d["status"] for d in drafts], ["error", "ok"])

    def test_run_phase3_v2_m1_parallel_preserves_deterministic_order(self):
        expected_units = expand_brief_units(
            MATRIX_PLAN,
            branch_id="branch_1",
            brand_slug="brand_x",
            pilot_size=5,
            selection_strategy="round_robin",
        )
        delay_by_id = {
            expected_units[0].brief_unit_id: 0.12,
            expected_units[1].brief_unit_id: 0.01,
            expected_units[2].brief_unit_id: 0.08,
            expected_units[3].brief_unit_id: 0.02,
            expected_units[4].brief_unit_id: 0.05,
        }

        def _delayed_draft(unit, spec, pack, *, run_mode, provider=None, model=None):
            _ = spec, pack, provider, model
            time.sleep(delay_by_id.get(unit.brief_unit_id, 0.0))
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
                model_metadata={"provider": "anthropic", "model": "claude-opus-4-6", "sdk_used": run_mode == "claude_sdk"},
            )

        with patch.object(config, "PHASE3_V2_CORE_DRAFTER_MAX_PARALLEL", 4), patch(
            "pipeline.phase3_v2_engine.draft_core_script_v1",
            side_effect=_delayed_draft,
        ), patch(
            "pipeline.phase3_v2_engine.evaluate_m1_gates",
            return_value={"overall_pass": True, "checks": []},
        ):
            result = run_phase3_v2_m1(
                matrix_plan=MATRIX_PLAN,
                foundation_brief=foundation_with_coverage(),
                branch_id="branch_1",
                brand_slug="brand_x",
                pilot_size=5,
                ab_mode=False,
                sdk_toggles={"core_script_drafter": True},
            )

        draft_ids = [row["brief_unit_id"] for row in result["drafts_by_arm"]["claude_sdk"]]
        expected_ids = [unit.brief_unit_id for unit in expected_units]
        self.assertEqual(draft_ids, expected_ids)

    def test_sdk_drafter_defaults_to_anthropic_frontier_model(self):
        unit = expand_brief_units(
            MATRIX_PLAN,
            branch_id="branch_1",
            brand_slug="brand_x",
            pilot_size=1,
            selection_strategy="round_robin",
        )[0]
        pack = build_evidence_pack(unit, foundation_with_coverage())
        spec = compile_script_spec_v1(unit, pack)
        captured: dict[str, object] = {}

        def _fake_sdk_call(**kwargs):
            captured["model"] = kwargs.get("model")
            captured["timeout_seconds"] = kwargs.get("timeout_seconds")
            return CoreScriptGeneratedV1(
                sections=CoreScriptSectionsV1(
                    hook="Hook",
                    problem="Problem",
                    mechanism="Mechanism",
                    proof="Proof",
                    cta="CTA",
                ),
                lines=[CoreScriptLineV1(line_id="L01", text="Line", evidence_ids=["proof_1"])],
            )

        with patch("pipeline.phase3_v2_engine.call_claude_agent_structured", side_effect=_fake_sdk_call):
            draft = draft_core_script_v1(unit, spec, pack, run_mode="claude_sdk")

        self.assertEqual(captured.get("model"), config.ANTHROPIC_FRONTIER)
        self.assertEqual(
            captured.get("timeout_seconds"),
            float(config.PHASE3_V2_CLAUDE_SDK_TIMEOUT_SECONDS),
        )
        self.assertEqual(draft.model_metadata.get("provider"), "anthropic")
        self.assertEqual(draft.model_metadata.get("model"), config.ANTHROPIC_FRONTIER)

    def test_sdk_drafter_rejects_non_claude_model_override_and_falls_back(self):
        unit = expand_brief_units(
            MATRIX_PLAN,
            branch_id="branch_1",
            brand_slug="brand_x",
            pilot_size=1,
            selection_strategy="round_robin",
        )[0]
        pack = build_evidence_pack(unit, foundation_with_coverage())
        spec = compile_script_spec_v1(unit, pack)
        captured: dict[str, object] = {}

        def _fake_sdk_call(**kwargs):
            captured["model"] = kwargs.get("model")
            return CoreScriptGeneratedV1(
                sections=CoreScriptSectionsV1(
                    hook="Hook",
                    problem="Problem",
                    mechanism="Mechanism",
                    proof="Proof",
                    cta="CTA",
                ),
                lines=[CoreScriptLineV1(line_id="L01", text="Line", evidence_ids=["proof_1"])],
            )

        with patch("pipeline.phase3_v2_engine.call_claude_agent_structured", side_effect=_fake_sdk_call):
            draft = draft_core_script_v1(
                unit,
                spec,
                pack,
                run_mode="claude_sdk",
                provider="openai",
                model="gpt-5.2",
            )

        self.assertEqual(captured.get("model"), config.ANTHROPIC_FRONTIER)
        self.assertEqual(draft.model_metadata.get("provider"), "anthropic")
        self.assertEqual(draft.model_metadata.get("model"), config.ANTHROPIC_FRONTIER)


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

    def test_runs_endpoint_reconciles_orphaned_running_entry(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_orphan"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ):
                server.pipeline_state["active_brand_slug"] = brand_slug
                server.phase3_v2_tasks.clear()
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
                            "status": "running",
                            "created_at": "2026-02-17T00:00:00",
                            "arms": ["claude_sdk"],
                        }
                    ],
                )

                runs = asyncio.run(server.api_phase3_v2_runs(branch_id, brand=brand_slug))

                self.assertEqual(len(runs), 1)
                self.assertEqual(runs[0].get("status"), "failed")
                self.assertIn("interrupted", str(runs[0].get("error", "")).lower())

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


class Phase3V2EditorialWorkflowTests(unittest.TestCase):
    def setUp(self):
        server.pipeline_state["running"] = False
        server.pipeline_state["active_brand_slug"] = "brand_x"

    def _seed_run(
        self,
        brand_slug: str,
        branch_id: str,
        run_id: str,
        *,
        brief_unit_ids: list[str],
        locked: bool = False,
    ) -> Path:
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
                    "arms": ["claude_sdk"],
                    "reviewer_role": "client_founder",
                }
            ],
        )

        run_dir = server._phase3_v2_run_dir(brand_slug, branch_id, run_id)
        brief_units = []
        evidence_packs = []
        drafts = []
        for idx, brief_unit_id in enumerate(brief_unit_ids, start=1):
            brief_units.append(
                {
                    "brief_unit_id": brief_unit_id,
                    "matrix_cell_id": "cell_unaware_frustration_pain",
                    "awareness_level": "unaware",
                    "emotion_key": "frustration_pain",
                    "emotion_label": "Frustration / Pain",
                    "branch_id": branch_id,
                    "brand_slug": brand_slug,
                    "ordinal_in_cell": idx,
                    "source_matrix_plan_hash": "hash_matrix",
                }
            )
            evidence_packs.append(
                {
                    "pack_id": f"pack_{brief_unit_id}",
                    "brief_unit_id": brief_unit_id,
                    "voc_quote_refs": [
                        {
                            "quote_id": f"q_{idx}",
                            "quote_excerpt": "This hurts after 10 minutes.",
                            "source_url": "https://example.com/review",
                            "source_type": "review",
                        }
                    ],
                    "proof_refs": [
                        {
                            "asset_id": f"proof_{idx}",
                            "proof_type": "testimonial",
                            "title": "Comfort",
                            "detail": "Users report better comfort.",
                            "source_url": "https://example.com/proof",
                        }
                    ],
                    "mechanism_refs": [
                        {
                            "mechanism_id": f"mech_{idx}",
                            "title": "Counterweight",
                            "detail": "Rear battery rebalances headset load.",
                            "support_evidence_ids": [f"proof_{idx}"],
                        }
                    ],
                    "coverage_report": {
                        "has_voc": True,
                        "has_proof": True,
                        "has_mechanism": True,
                        "voc_count": 1,
                        "proof_count": 1,
                        "mechanism_count": 1,
                        "blocked_evidence_insufficient": False,
                    },
                }
            )
            drafts.append(
                {
                    "script_id": f"script_{brief_unit_id}",
                    "brief_unit_id": brief_unit_id,
                    "arm": "claude_sdk",
                    "status": "ok",
                    "error": "",
                    "sections": {
                        "hook": "Hook",
                        "problem": "Problem",
                        "mechanism": "Mechanism",
                        "proof": "Proof",
                        "cta": "CTA",
                    },
                    "lines": [
                        {"line_id": "L01", "text": "Line one.", "evidence_ids": [f"proof_{idx}"]},
                    ],
                    "model_metadata": {"provider": "anthropic", "model": "claude-opus-4-6", "sdk_used": True},
                    "gate_report": {"overall_pass": True, "checks": []},
                    "latency_seconds": 1.2,
                    "cost_usd": 0.12,
                }
            )

        server._phase3_v2_write_json(run_dir / "brief_units.json", brief_units)
        server._phase3_v2_write_json(run_dir / "evidence_packs.json", evidence_packs)
        server._phase3_v2_write_json(run_dir / "reviews.json", [])
        server._phase3_v2_write_json(run_dir / "summary.json", {})
        server._phase3_v2_write_json(run_dir / "arm_claude_sdk_core_scripts.json", drafts)
        server._phase3_v2_write_json(run_dir / "decisions.json", [])
        server._phase3_v2_write_json(run_dir / "chat_threads.json", {})
        server._phase3_v2_write_json(
            run_dir / "final_lock.json",
            {
                "run_id": run_id,
                "locked": locked,
                "locked_at": "2026-02-17T01:00:00" if locked else "",
                "locked_by_role": "client_founder" if locked else "",
            },
        )
        return run_dir

    def test_decisions_upsert_and_progress(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_decisions"
        unit_ids = ["bu_1", "bu_2"]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ):
                self._seed_run(brand_slug, branch_id, run_id, brief_unit_ids=unit_ids)
                req = server.Phase3V2DecisionRequest(
                    run_id=run_id,
                    brand=brand_slug,
                    reviewer_role="client_founder",
                    decisions=[
                        server.Phase3V2DecisionPayload(
                            brief_unit_id="bu_1",
                            arm="claude_sdk",
                            decision="approve",
                        ),
                        server.Phase3V2DecisionPayload(
                            brief_unit_id="bu_2",
                            arm="claude_sdk",
                            decision="revise",
                        ),
                    ],
                )
                resp = asyncio.run(server.api_phase3_v2_decisions(branch_id, req))

                self.assertTrue(resp.get("ok"))
                self.assertEqual(resp.get("decision_count"), 2)
                progress = resp.get("decision_progress", {})
                self.assertEqual(progress.get("approved"), 1)
                self.assertEqual(progress.get("revise"), 1)
                self.assertEqual(progress.get("pending"), 0)

                saved = json.loads((server._phase3_v2_run_dir(brand_slug, branch_id, run_id) / "decisions.json").read_text("utf-8"))
                self.assertEqual(len(saved), 2)

    def test_final_lock_requires_all_approved(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_lock_fail"
        unit_ids = ["bu_1", "bu_2"]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ):
                self._seed_run(brand_slug, branch_id, run_id, brief_unit_ids=unit_ids)
                decision_req = server.Phase3V2DecisionRequest(
                    run_id=run_id,
                    brand=brand_slug,
                    decisions=[
                        server.Phase3V2DecisionPayload(brief_unit_id="bu_1", arm="claude_sdk", decision="approve"),
                        server.Phase3V2DecisionPayload(brief_unit_id="bu_2", arm="claude_sdk", decision="revise"),
                    ],
                )
                _ = asyncio.run(server.api_phase3_v2_decisions(branch_id, decision_req))

                lock_req = server.Phase3V2FinalLockRequest(brand=brand_slug, reviewer_role="client_founder")
                resp = asyncio.run(server.api_phase3_v2_final_lock(branch_id, run_id, lock_req))
                self.assertIsInstance(resp, JSONResponse)
                self.assertEqual(resp.status_code, 400)
                payload = json.loads(resp.body.decode("utf-8"))
                self.assertIn("must be approved", payload.get("error", ""))

    def test_final_lock_succeeds_when_all_approved(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_lock_ok"
        unit_ids = ["bu_1", "bu_2"]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ):
                self._seed_run(brand_slug, branch_id, run_id, brief_unit_ids=unit_ids)
                decision_req = server.Phase3V2DecisionRequest(
                    run_id=run_id,
                    brand=brand_slug,
                    decisions=[
                        server.Phase3V2DecisionPayload(brief_unit_id="bu_1", arm="claude_sdk", decision="approve"),
                        server.Phase3V2DecisionPayload(brief_unit_id="bu_2", arm="claude_sdk", decision="approve"),
                    ],
                )
                _ = asyncio.run(server.api_phase3_v2_decisions(branch_id, decision_req))

                lock_req = server.Phase3V2FinalLockRequest(brand=brand_slug, reviewer_role="client_founder")
                resp = asyncio.run(server.api_phase3_v2_final_lock(branch_id, run_id, lock_req))
                self.assertTrue(resp.get("ok"))
                self.assertTrue(resp.get("final_lock", {}).get("locked"))

                detail = asyncio.run(server.api_phase3_v2_run_detail(branch_id, run_id, brand=brand_slug))
                self.assertTrue(detail.get("final_lock", {}).get("locked"))

    def test_locked_run_rejects_decision_edit_chat_and_apply(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_locked"
        unit_id = "bu_1"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ):
                self._seed_run(
                    brand_slug,
                    branch_id,
                    run_id,
                    brief_unit_ids=[unit_id],
                    locked=True,
                )

                decision_req = server.Phase3V2DecisionRequest(
                    run_id=run_id,
                    brand=brand_slug,
                    decisions=[server.Phase3V2DecisionPayload(brief_unit_id=unit_id, arm="claude_sdk", decision="approve")],
                )
                decision_resp = asyncio.run(server.api_phase3_v2_decisions(branch_id, decision_req))
                self.assertIsInstance(decision_resp, JSONResponse)
                self.assertEqual(decision_resp.status_code, 409)

                edit_req = server.Phase3V2DraftUpdateRequest(
                    brand=brand_slug,
                    lines=[server.Phase3V2DraftLinePayload(text="Edited line", evidence_ids=["proof_1"])],
                    source="manual",
                )
                edit_resp = asyncio.run(
                    server.api_phase3_v2_update_draft(branch_id, run_id, "claude_sdk", unit_id, edit_req)
                )
                self.assertIsInstance(edit_resp, JSONResponse)
                self.assertEqual(edit_resp.status_code, 409)

                chat_req = server.Phase3V2ChatRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    message="Improve the hook",
                )
                chat_resp = asyncio.run(server.api_phase3_v2_chat_post(branch_id, run_id, chat_req))
                self.assertIsInstance(chat_resp, JSONResponse)
                self.assertEqual(chat_resp.status_code, 409)

                apply_req = server.Phase3V2ChatApplyRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    proposed_draft=CoreScriptGeneratedV1(
                        sections=CoreScriptSectionsV1(
                            hook="Hook",
                            problem="Problem",
                            mechanism="Mechanism",
                            proof="Proof",
                            cta="CTA",
                        ),
                        lines=[CoreScriptLineV1(line_id="L01", text="Line", evidence_ids=["proof_1"])],
                    ),
                )
                apply_resp = asyncio.run(server.api_phase3_v2_chat_apply(branch_id, run_id, apply_req))
                self.assertIsInstance(apply_resp, JSONResponse)
                self.assertEqual(apply_resp.status_code, 409)

    def test_manual_edit_renumbers_lines_and_persists(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_edit"
        unit_id = "bu_1"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ):
                run_dir = self._seed_run(brand_slug, branch_id, run_id, brief_unit_ids=[unit_id])

                req = server.Phase3V2DraftUpdateRequest(
                    brand=brand_slug,
                    source="manual",
                    lines=[
                        server.Phase3V2DraftLinePayload(line_id="x", text="First replacement line", evidence_ids=["VOC-001"]),
                        server.Phase3V2DraftLinePayload(line_id="y", text="   ", evidence_ids=[]),
                        server.Phase3V2DraftLinePayload(line_id="z", text="Second replacement line", evidence_ids=["PROOF-001"]),
                    ],
                )
                resp = asyncio.run(
                    server.api_phase3_v2_update_draft(branch_id, run_id, "claude_sdk", unit_id, req)
                )
                self.assertTrue(resp.get("ok"))

                saved = json.loads((run_dir / "arm_claude_sdk_core_scripts.json").read_text("utf-8"))
                self.assertEqual(len(saved), 1)
                lines = saved[0].get("lines", [])
                self.assertEqual([row.get("line_id") for row in lines], ["L01", "L02"])
                self.assertEqual([row.get("text") for row in lines], ["First replacement line", "Second replacement line"])
                self.assertEqual(saved[0].get("model_metadata", {}).get("edited_source"), "manual")

    def test_chat_thread_persistence_round_trip(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_chat"
        unit_id = "bu_1"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED", True
            ), patch(
                "pipeline.llm.call_llm_structured",
                return_value=Phase3V2ChatReplyV1(
                    assistant_message="Tighten the hook and sharpen the CTA.",
                    proposed_draft=None,
                ),
            ):
                _ = self._seed_run(brand_slug, branch_id, run_id, brief_unit_ids=[unit_id])
                post_req = server.Phase3V2ChatRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    message="Make this stronger but keep it credible.",
                )
                post_resp = asyncio.run(server.api_phase3_v2_chat_post(branch_id, run_id, post_req))
                self.assertIn("assistant_message", post_resp)
                self.assertFalse(post_resp.get("has_proposed_draft"))
                self.assertEqual(len(post_resp.get("messages", [])), 2)

                get_resp = asyncio.run(
                    server.api_phase3_v2_chat_get(
                        branch_id,
                        run_id,
                        brief_unit_id=unit_id,
                        arm="claude_sdk",
                        brand=brand_slug,
                    )
                )
                self.assertEqual(len(get_resp.get("messages", [])), 2)
                self.assertEqual(get_resp["messages"][0]["role"], "user")
                self.assertEqual(get_resp["messages"][1]["role"], "assistant")


if __name__ == "__main__":
    unittest.main()
