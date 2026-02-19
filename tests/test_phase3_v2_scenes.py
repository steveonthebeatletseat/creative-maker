from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import server
from pipeline import phase3_v2_scene_engine as scene_engine
from schemas.phase3_v2 import (
    ARollDirectionV1,
    BRollDirectionV1,
    CoreScriptLineV1,
    CoreScriptSectionsV1,
    ProductionHandoffPacketV1,
    SceneChatReplyV1,
    SceneGateReportV1,
    SceneLinePlanV1,
    ScenePlanV1,
)


def _default_variant(unit_id: str, awareness: str, emotion_key: str) -> dict:
    return {
        "hook_id": f"hk_{unit_id}_001",
        "brief_unit_id": unit_id,
        "arm": "claude_sdk",
        "verbal_open": "Default hook verbal",
        "visual_pattern_interrupt": "Default visual interrupt",
        "on_screen_text": "On-screen text",
        "awareness_level": awareness,
        "emotion_key": emotion_key,
        "evidence_ids": ["PROOF-001"],
        "scroll_stop_score": 82,
        "specificity_score": 80,
        "lane_id": "script_default",
        "selection_status": "selected",
        "gate_pass": True,
        "rank": 1,
    }


def _default_scene_plan(run_id: str, unit_id: str, hook_id: str) -> dict:
    return {
        "scene_plan_id": f"sp_{unit_id}_{hook_id}_claude_sdk",
        "run_id": run_id,
        "brief_unit_id": unit_id,
        "arm": "claude_sdk",
        "hook_id": hook_id,
        "lines": [
            {
                "scene_line_id": f"sl_{unit_id}_{hook_id}_L01",
                "script_line_id": "L01",
                "mode": "a_roll",
                "a_roll": {
                    "framing": "Medium talking head",
                    "creator_action": "Creator holds the Quest strap",
                    "performance_direction": "Confident and direct",
                    "product_interaction": "Points to forehead pressure area",
                    "location": "Desk setup",
                },
                "b_roll": None,
                "on_screen_text": "Pain starts at 10 minutes",
                "duration_seconds": 2.5,
                "evidence_ids": ["PROOF-001"],
                "difficulty_1_10": 4,
            },
            {
                "scene_line_id": f"sl_{unit_id}_{hook_id}_L02",
                "script_line_id": "L02",
                "mode": "b_roll",
                "a_roll": None,
                "b_roll": {
                    "shot_description": "Close-up of strap pressure marks",
                    "subject_action": "Hand removes headset",
                    "camera_motion": "Slow push in",
                    "props_assets": "Quest 3 headset",
                    "transition_intent": "Bridge pain to mechanism fix",
                },
                "on_screen_text": "Pressure points",
                "duration_seconds": 2.0,
                "evidence_ids": ["VOC-001"],
                "difficulty_1_10": 5,
            },
        ],
        "total_duration_seconds": 4.5,
        "a_roll_line_count": 1,
        "b_roll_line_count": 1,
        "max_consecutive_mode": 1,
        "status": "ok",
        "stale": False,
        "stale_reason": "",
        "error": "",
        "generated_at": "2026-02-18T00:00:00",
    }


def _default_scene_gate(run_id: str, unit_id: str, hook_id: str) -> dict:
    return {
        "scene_plan_id": f"sp_{unit_id}_{hook_id}_claude_sdk",
        "scene_unit_id": f"su_{unit_id}_{hook_id}",
        "run_id": run_id,
        "brief_unit_id": unit_id,
        "arm": "claude_sdk",
        "hook_id": hook_id,
        "line_coverage_pass": True,
        "mode_pass": True,
        "ugc_pass": True,
        "evidence_pass": True,
        "claim_safety_pass": True,
        "pacing_pass": True,
        "post_polish_pass": True,
        "overall_pass": True,
        "failure_reasons": [],
        "failing_line_ids": [],
        "repair_rounds_used": 0,
        "evaluated_at": "2026-02-18T00:00:00",
        "evaluator_metadata": {},
    }


def _seed_phase3_v2_scene_run(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    unit_ids: list[str],
    include_scene_files: bool = True,
    lock_run: bool = False,
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
                "created_at": "2026-02-18T00:00:00",
                "arms": ["claude_sdk"],
                "reviewer_role": "client_founder",
                "hook_stage_status": "completed",
                "scene_stage_status": "idle",
            }
        ],
    )

    run_dir = server._phase3_v2_run_dir(brand_slug, branch_id, run_id)
    brief_units = []
    evidence_packs = []
    drafts = []
    hook_bundles = []
    hook_selections = []
    for idx, unit_id in enumerate(unit_ids, start=1):
        awareness = "problem_aware" if idx % 2 == 0 else "unaware"
        emotion_key = "frustration_pain" if idx % 2 == 0 else "desire_freedom_immersion"
        emotion_label = "Frustration / Pain" if emotion_key == "frustration_pain" else "Desire for Freedom / Immersion"
        brief_units.append(
            {
                "brief_unit_id": unit_id,
                "matrix_cell_id": f"cell_{awareness}_{emotion_key}",
                "branch_id": branch_id,
                "brand_slug": brand_slug,
                "awareness_level": awareness,
                "emotion_key": emotion_key,
                "emotion_label": emotion_label,
                "lf8_code": "lf8_3" if emotion_key == "frustration_pain" else "lf8_6",
                "lf8_label": "Freedom from Fear / Pain" if emotion_key == "frustration_pain" else "Status & Winning",
                "emotion_angle": "Remove risk with concrete proof." if emotion_key == "frustration_pain" else "Signal disciplined high-performance identity.",
                "blocking_objection": "Does it actually work?" if emotion_key == "frustration_pain" else "",
                "required_proof": "Dose + efficacy proof with third-party support." if emotion_key == "frustration_pain" else "Identity/status proof from credible peers.",
                "confidence": 0.81 if emotion_key == "frustration_pain" else 0.62,
                "sample_quote_ids": ["VOC-001"],
                "ordinal_in_cell": 1,
                "source_matrix_plan_hash": "matrix_hash",
            }
        )
        evidence_packs.append(
            {
                "pack_id": f"pack_{unit_id}",
                "brief_unit_id": unit_id,
                "voc_quote_refs": [
                    {
                        "quote_id": "VOC-001",
                        "quote_excerpt": "The strap hurts after ten minutes.",
                        "source_url": "https://example.com/review",
                        "source_type": "review",
                    }
                ],
                "proof_refs": [
                    {
                        "asset_id": "PROOF-001",
                        "proof_type": "testimonial",
                        "title": "Comfort improvement",
                        "detail": "Users report lower face pressure.",
                        "source_url": "https://example.com/proof",
                    }
                ],
                "mechanism_refs": [
                    {
                        "mechanism_id": "MECH-001",
                        "title": "Counterweight balance",
                        "detail": "Rear weight reduces front pressure.",
                        "support_evidence_ids": ["PROOF-001"],
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
                "script_id": f"script_{unit_id}",
                "brief_unit_id": unit_id,
                "arm": "claude_sdk",
                "status": "ok",
                "error": "",
                "sections": CoreScriptSectionsV1(
                    hook="Hook line",
                    problem="Problem line",
                    mechanism="Mechanism line",
                    proof="Proof line",
                    cta="CTA line",
                ).model_dump(),
                "lines": [
                    CoreScriptLineV1(line_id="L01", text="Script line one", evidence_ids=["PROOF-001"]).model_dump(),
                    CoreScriptLineV1(line_id="L02", text="Script line two", evidence_ids=["VOC-001"]).model_dump(),
                ],
                "model_metadata": {"provider": "anthropic", "model": "claude-opus-4-6", "sdk_used": True},
                "gate_report": {"overall_pass": True, "checks": []},
                "latency_seconds": 1.2,
                "cost_usd": 0.12,
            }
        )

        variant = _default_variant(unit_id, awareness, emotion_key)
        hook_bundles.append(
            {
                "hook_run_id": "hkv2_1",
                "brief_unit_id": unit_id,
                "arm": "claude_sdk",
                "script_id": f"script_{unit_id}",
                "variants": [variant],
                "candidate_count": 10,
                "passed_gate_count": 8,
                "repair_rounds_used": 0,
                "deficiency_flags": [],
                "status": "ok",
                "error": "",
                "generated_at": "2026-02-18T00:00:00",
            }
        )
        hook_selections.append(
            {
                "run_id": run_id,
                "hook_run_id": "hkv2_1",
                "brief_unit_id": unit_id,
                "arm": "claude_sdk",
                "selected_hook_ids": [variant["hook_id"]],
                "selected_hook_id": variant["hook_id"],
                "skip": False,
                "stale": False,
                "stale_reason": "",
                "updated_at": "2026-02-18T00:00:00",
            }
        )

    server._phase3_v2_write_json(
        run_dir / "manifest.json",
        {
            "run_id": run_id,
            "status": "completed",
            "created_at": "2026-02-18T00:00:00",
            "completed_at": "2026-02-18T00:01:00",
            "arms": ["claude_sdk"],
            "pilot_size": len(unit_ids),
        },
    )
    server._phase3_v2_write_json(run_dir / "brief_units.json", brief_units)
    server._phase3_v2_write_json(run_dir / "evidence_packs.json", evidence_packs)
    server._phase3_v2_write_json(run_dir / "arm_claude_sdk_core_scripts.json", drafts)
    server._phase3_v2_write_json(run_dir / "reviews.json", [])
    server._phase3_v2_write_json(run_dir / "summary.json", {})
    server._phase3_v2_write_json(run_dir / "decisions.json", [])
    server._phase3_v2_write_json(run_dir / "chat_threads.json", {})
    server._phase3_v2_write_json(
        run_dir / "final_lock.json",
        {
            "run_id": run_id,
            "locked": lock_run,
            "locked_at": datetime.now().isoformat() if lock_run else "",
            "locked_by_role": "client_founder" if lock_run else "",
        },
    )

    server._phase3_v2_write_json(run_dir / "hook_selections.json", hook_selections)
    server._phase3_v2_write_json(run_dir / "hook_chat_threads.json", {})
    server._phase3_v2_write_json(
        run_dir / "hook_stage_manifest.json",
        {
            "run_id": run_id,
            "hook_run_id": "hkv2_1",
            "status": "completed",
            "created_at": "2026-02-18T00:00:00",
            "started_at": "2026-02-18T00:00:00",
            "completed_at": "2026-02-18T00:00:05",
            "error": "",
            "eligible_count": len(unit_ids),
            "processed_count": len(unit_ids),
            "failed_count": 0,
            "skipped_count": 0,
            "candidate_target_per_unit": 20,
            "final_variants_per_unit": 5,
            "max_parallel": 4,
            "max_repair_rounds": 1,
            "model_registry": {},
            "metrics": {},
        },
    )
    server._phase3_v2_write_json(server._phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, "claude_sdk"), hook_bundles)
    server._phase3_v2_write_json(server._phase3_v2_hook_candidates_path(brand_slug, branch_id, run_id, "claude_sdk"), [])
    server._phase3_v2_write_json(server._phase3_v2_hook_gate_reports_path(brand_slug, branch_id, run_id, "claude_sdk"), [])
    server._phase3_v2_write_json(server._phase3_v2_hook_scores_path(brand_slug, branch_id, run_id, "claude_sdk"), [])

    server._phase3_v2_write_json(
        run_dir / "scene_stage_manifest.json",
        {
            "run_id": run_id,
            "scene_run_id": "",
            "status": "idle",
            "created_at": "",
            "started_at": "",
            "completed_at": "",
            "error": "",
            "eligible_count": 0,
            "processed_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "stale_count": 0,
            "max_parallel": 4,
            "max_repair_rounds": 1,
            "max_consecutive_mode": 3,
            "min_a_roll_lines": 1,
            "model_registry": {},
            "metrics": {},
        },
    )
    server._phase3_v2_write_json(run_dir / "scene_chat_threads.json", {})
    if include_scene_files:
        scene_plans = []
        scene_gates = []
        for unit_id in unit_ids:
            hook_id = f"hk_{unit_id}_001"
            scene_plans.append(_default_scene_plan(run_id, unit_id, hook_id))
            scene_gates.append(_default_scene_gate(run_id, unit_id, hook_id))
        server._phase3_v2_write_json(server._phase3_v2_scene_plans_path(brand_slug, branch_id, run_id, "claude_sdk"), scene_plans)
        server._phase3_v2_write_json(server._phase3_v2_scene_gate_reports_path(brand_slug, branch_id, run_id, "claude_sdk"), scene_gates)
    else:
        server._phase3_v2_write_json(server._phase3_v2_scene_plans_path(brand_slug, branch_id, run_id, "claude_sdk"), [])
        server._phase3_v2_write_json(server._phase3_v2_scene_gate_reports_path(brand_slug, branch_id, run_id, "claude_sdk"), [])
    server._phase3_v2_write_json(run_dir / "production_handoff_packet.json", {})
    server._phase3_v2_write_json(run_dir / "scene_handoff_packet.json", {})

    server._phase3_v2_refresh_scene_handoff(
        brand_slug=brand_slug,
        branch_id=branch_id,
        run_id=run_id,
    )
    return run_dir


class Phase3V2SceneEngineTests(unittest.TestCase):
    def test_scene_ids_are_deterministic(self):
        self.assertEqual(scene_engine._scene_unit_id("bu_1", "hk_bu_1_001"), "su_bu_1_hk_bu_1_001")
        self.assertEqual(scene_engine._scene_plan_id("bu_1", "hk_bu_1_001", "claude_sdk"), "sp_bu_1_hk_bu_1_001_claude_sdk")
        self.assertEqual(scene_engine._scene_line_id("bu_1", "hk_bu_1_001", "L01"), "sl_bu_1_hk_bu_1_001_L01")

    def test_compile_scene_constraints_ir_includes_audience_context(self):
        scene_item = {
            "run_id": "run_1",
            "arm": "claude_sdk",
            "brief_unit": {
                "brief_unit_id": "bu_1",
                "matrix_cell_id": "cell_problem_aware_frustration_pain",
                "branch_id": "branch_1",
                "brand_slug": "brand_x",
                "awareness_level": "problem_aware",
                "emotion_key": "frustration_pain",
                "emotion_label": "Frustration / Pain",
                "audience_segment_name": "Biohacker Professional",
                "audience_goals": ["Sustain deep focus"],
                "audience_pains": ["Afternoon energy crashes"],
                "audience_triggers": ["Deadline-heavy days"],
                "audience_objections": ["Skeptical of overhyped formulas"],
                "audience_information_sources": ["Reddit"],
                "lf8_code": "lf8_3",
                "lf8_label": "Freedom from Fear / Pain",
                "emotion_angle": "Remove risk that this is placebo and ineffective.",
                "blocking_objection": "Does it actually work?",
                "required_proof": "Dose + efficacy proof with third-party support.",
                "confidence": 0.84,
                "sample_quote_ids": ["q_1", "q_2"],
                "ordinal_in_cell": 1,
                "source_matrix_plan_hash": "matrix_hash",
            },
            "draft": {
                "script_id": "script_1",
                "brief_unit_id": "bu_1",
                "arm": "claude_sdk",
                "sections": CoreScriptSectionsV1(
                    hook="Hook",
                    problem="Problem",
                    mechanism="Mechanism",
                    proof="Proof",
                    cta="CTA",
                ).model_dump(),
                "lines": [CoreScriptLineV1(line_id="L01", text="Line one", evidence_ids=["PROOF-001"]).model_dump()],
                "status": "ok",
            },
            "evidence_pack": {
                "pack_id": "pack_1",
                "brief_unit_id": "bu_1",
                "voc_quote_refs": [],
                "proof_refs": [
                    {
                        "asset_id": "PROOF-001",
                        "proof_type": "testimonial",
                        "title": "Proof",
                        "detail": "Detail",
                        "source_url": "https://example.com/proof",
                    }
                ],
                "mechanism_refs": [],
                "coverage_report": {
                    "has_voc": True,
                    "has_proof": True,
                    "has_mechanism": True,
                    "blocked_evidence_insufficient": False,
                },
            },
            "hook": _default_variant("bu_1", "problem_aware", "frustration_pain"),
        }

        ir = scene_engine.compile_scene_constraints_ir(scene_item)

        self.assertEqual(ir.get("audience_segment_name"), "Biohacker Professional")
        self.assertEqual(ir.get("audience_goals"), ["Sustain deep focus"])
        self.assertEqual(ir.get("audience_pains"), ["Afternoon energy crashes"])
        self.assertEqual(ir.get("audience", {}).get("segment_name"), "Biohacker Professional")
        self.assertEqual(ir.get("audience", {}).get("information_sources"), ["Reddit"])
        self.assertEqual(ir.get("lf8_code"), "lf8_3")
        self.assertEqual(ir.get("lf8_label"), "Freedom from Fear / Pain")
        self.assertEqual(ir.get("blocking_objection"), "Does it actually work?")
        self.assertTrue(bool(ir.get("required_proof")))
        self.assertEqual(ir.get("lf8_context", {}).get("lf8_code"), "lf8_3")

    def test_preprocess_script_lines_for_beats_splits_long_line_with_lineage(self):
        script_lines = [
            {"line_id": "L01", "text": "Short hook line.", "evidence_ids": ["PROOF-001"]},
            {
                "line_id": "L02",
                "text": (
                    "The default design dumps all the weight onto your face, "
                    "so pressure builds fast and your forehead aches before the first game ends."
                ),
                "evidence_ids": ["VOC-001"],
            },
        ]
        beats, beat_map, source_ids = scene_engine.preprocess_script_lines_for_beats(script_lines)
        beat_ids = [str(row.get("line_id") or "") for row in beats]

        self.assertEqual(source_ids, ["L01", "L02"])
        self.assertEqual(beat_ids, ["L01", "L02.1", "L02.2"])
        self.assertEqual(beat_map.get("L02.1"), "L02")
        self.assertEqual(beat_map.get("L02.2"), "L02")

    def test_normalize_scene_lines_preserves_split_lineage(self):
        ir = {
            "brief_unit_id": "bu_1",
            "hook_id": "hk_1",
            "script_lines": [
                {
                    "line_id": "L02.1",
                    "text": "Beat one text",
                    "evidence_ids": ["VOC-001"],
                    "source_line_id": "L02",
                    "beat_index": 1,
                    "beat_text": "Beat one text",
                }
            ],
        }
        raw_lines = [
            scene_engine._SceneDraftLineModel(
                script_line_id="L02.1",
                mode="b_roll",
                b_roll=BRollDirectionV1(shot_description="Visual one"),
                evidence_ids=["VOC-001"],
                difficulty_1_10=7,
            )
        ]

        normalized = scene_engine._normalize_scene_lines(raw_lines, ir)
        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized[0].script_line_id, "L02.1")
        self.assertEqual(normalized[0].source_script_line_id, "L02")
        self.assertEqual(normalized[0].beat_index, 1)
        self.assertEqual(normalized[0].beat_text, "Beat one text")

    def test_scene_count_gate_fails_when_split_count_exceeds_cap(self):
        ir = {
            "source_script_lines": [
                {"line_id": "L01", "text": "original", "evidence_ids": ["PROOF-001"]},
            ],
            "script_lines": [
                {"line_id": "L01.1", "text": "beat 1", "evidence_ids": ["PROOF-001"]},
                {"line_id": "L01.2", "text": "beat 2", "evidence_ids": ["PROOF-001"]},
                {"line_id": "L01.3", "text": "beat 3", "evidence_ids": ["PROOF-001"]},
            ],
        }
        plan = ScenePlanV1(
            scene_plan_id="sp_test",
            run_id="run_1",
            brief_unit_id="bu_1",
            arm="claude_sdk",
            hook_id="hk_1",
            lines=[
                SceneLinePlanV1(
                    scene_line_id="sl_1",
                    script_line_id="L01.1",
                    source_script_line_id="L01",
                    beat_index=1,
                    beat_text="beat 1",
                    mode="a_roll",
                    a_roll=ARollDirectionV1(framing="Medium shot"),
                    evidence_ids=["PROOF-001"],
                    duration_seconds=2.0,
                    difficulty_1_10=4,
                ),
                SceneLinePlanV1(
                    scene_line_id="sl_2",
                    script_line_id="L01.2",
                    source_script_line_id="L01",
                    beat_index=2,
                    beat_text="beat 2",
                    mode="b_roll",
                    b_roll=BRollDirectionV1(shot_description="Visual two"),
                    evidence_ids=["PROOF-001"],
                    duration_seconds=2.0,
                    difficulty_1_10=4,
                ),
                SceneLinePlanV1(
                    scene_line_id="sl_3",
                    script_line_id="L01.3",
                    source_script_line_id="L01",
                    beat_index=3,
                    beat_text="beat 3",
                    mode="a_roll",
                    a_roll=ARollDirectionV1(framing="Medium shot"),
                    evidence_ids=["PROOF-001"],
                    duration_seconds=2.0,
                    difficulty_1_10=4,
                ),
            ],
        )
        with patch("pipeline.phase3_v2_scene_engine.call_llm_structured", side_effect=RuntimeError("offline")):
            report = scene_engine.evaluate_scene_gates(scene_plan=plan, ir=ir)
        self.assertFalse(report.overall_pass)
        self.assertIn("scene_count_excessive", report.failure_reasons)

    def test_line_coverage_gate_fails_when_missing_script_line(self):
        ir = {
            "script_lines": [
                {"line_id": "L01", "text": "line 1", "evidence_ids": ["PROOF-001"]},
                {"line_id": "L02", "text": "line 2", "evidence_ids": ["VOC-001"]},
            ]
        }
        plan = ScenePlanV1(
            scene_plan_id="sp_test",
            run_id="run_1",
            brief_unit_id="bu_1",
            arm="claude_sdk",
            hook_id="hk_1",
            lines=[
                SceneLinePlanV1(
                    scene_line_id="sl_1",
                    script_line_id="L01",
                    mode="a_roll",
                    a_roll=ARollDirectionV1(framing="Medium shot"),
                    evidence_ids=["PROOF-001"],
                    duration_seconds=2.0,
                    difficulty_1_10=4,
                )
            ],
        )
        with patch("pipeline.phase3_v2_scene_engine.call_llm_structured", side_effect=RuntimeError("offline")):
            report = scene_engine.evaluate_scene_gates(scene_plan=plan, ir=ir)
        self.assertFalse(report.overall_pass)
        self.assertIn("line_coverage_failed", report.failure_reasons)

    def test_evidence_gate_fails_when_line_uses_unsupported_evidence(self):
        ir = {
            "script_lines": [
                {"line_id": "L01", "text": "line 1", "evidence_ids": ["PROOF-001"]},
            ]
        }
        plan = ScenePlanV1(
            scene_plan_id="sp_test",
            run_id="run_1",
            brief_unit_id="bu_1",
            arm="claude_sdk",
            hook_id="hk_1",
            lines=[
                SceneLinePlanV1(
                    scene_line_id="sl_1",
                    script_line_id="L01",
                    mode="a_roll",
                    a_roll=ARollDirectionV1(framing="Medium shot"),
                    evidence_ids=["VOC-999"],
                    duration_seconds=2.0,
                    difficulty_1_10=4,
                )
            ],
        )
        with patch("pipeline.phase3_v2_scene_engine.call_llm_structured", side_effect=RuntimeError("offline")):
            report = scene_engine.evaluate_scene_gates(scene_plan=plan, ir=ir)
        self.assertFalse(report.overall_pass)
        self.assertIn("evidence_subset_failed", report.failure_reasons)

    def test_mode_gate_fails_when_direction_block_is_missing(self):
        ir = {
            "script_lines": [
                {"line_id": "L01", "text": "line 1", "evidence_ids": ["PROOF-001"]},
            ]
        }
        plan = ScenePlanV1(
            scene_plan_id="sp_test",
            run_id="run_1",
            brief_unit_id="bu_1",
            arm="claude_sdk",
            hook_id="hk_1",
            lines=[
                SceneLinePlanV1(
                    scene_line_id="sl_1",
                    script_line_id="L01",
                    mode="a_roll",
                    a_roll=None,
                    b_roll=None,
                    evidence_ids=["PROOF-001"],
                    duration_seconds=2.0,
                    difficulty_1_10=4,
                )
            ],
        )
        with patch("pipeline.phase3_v2_scene_engine.call_llm_structured", side_effect=RuntimeError("offline")):
            report = scene_engine.evaluate_scene_gates(scene_plan=plan, ir=ir)
        self.assertFalse(report.overall_pass)
        self.assertIn("mode_missing_or_direction_missing", report.failure_reasons)

    def test_run_phase3_v2_scenes_isolates_unit_failures(self):
        scene_items = [
            {"run_id": "run_1", "brief_unit_id": "bu_1", "arm": "claude_sdk", "hook_id": "hk_1"},
            {"run_id": "run_1", "brief_unit_id": "bu_2", "arm": "claude_sdk", "hook_id": "hk_2"},
        ]

        ok_result = scene_engine._SceneUnitResult(
            arm="claude_sdk",
            brief_unit_id="bu_1",
            hook_id="hk_1",
            scene_unit_id="su_bu_1_hk_1",
            scene_plan=ScenePlanV1(
                scene_plan_id="sp_bu_1_hk_1_claude_sdk",
                run_id="run_1",
                brief_unit_id="bu_1",
                arm="claude_sdk",
                hook_id="hk_1",
                status="ok",
            ),
            gate_report=SceneGateReportV1(
                scene_plan_id="sp_bu_1_hk_1_claude_sdk",
                scene_unit_id="su_bu_1_hk_1",
                run_id="run_1",
                brief_unit_id="bu_1",
                arm="claude_sdk",
                hook_id="hk_1",
                line_coverage_pass=True,
                mode_pass=True,
                ugc_pass=True,
                evidence_pass=True,
                claim_safety_pass=True,
                pacing_pass=True,
                post_polish_pass=True,
                overall_pass=True,
            ),
            elapsed_seconds=0.01,
            error="",
        )

        def _fake_run_scene_unit(**kwargs):
            if kwargs["scene_item"]["brief_unit_id"] == "bu_2":
                raise RuntimeError("scene_unit_failure")
            return ok_result

        with patch("pipeline.phase3_v2_scene_engine.run_scene_unit", side_effect=_fake_run_scene_unit):
            result = scene_engine.run_phase3_v2_scenes(
                run_id="run_1",
                scene_run_id="scv2_1",
                scene_items=scene_items,
                model_overrides={},
            )

        manifest = result["scene_stage_manifest"]
        self.assertEqual(int(manifest.get("processed_count", 0)), 2)
        self.assertEqual(int(manifest.get("failed_count", 0)), 1)
        plans = result["scene_plans_by_arm"].get("claude_sdk", [])
        self.assertEqual(len(plans), 2)
        packet = ProductionHandoffPacketV1.model_validate(result["production_handoff_packet"])
        self.assertFalse(packet.ready)
        self.assertEqual(packet.total_required, 2)

    def test_production_handoff_ready_requires_all_units_passing(self):
        scene_items = [
            {"brief_unit_id": "bu_1", "arm": "claude_sdk", "hook_id": "hk_1"},
            {"brief_unit_id": "bu_2", "arm": "claude_sdk", "hook_id": "hk_2"},
        ]
        ok_result = scene_engine._SceneUnitResult(
            arm="claude_sdk",
            brief_unit_id="bu_1",
            hook_id="hk_1",
            scene_unit_id="su_bu_1_hk_1",
            scene_plan=ScenePlanV1(
                scene_plan_id="sp_bu_1_hk_1_claude_sdk",
                run_id="run_1",
                brief_unit_id="bu_1",
                arm="claude_sdk",
                hook_id="hk_1",
                status="ok",
            ),
            gate_report=SceneGateReportV1(
                scene_plan_id="sp_bu_1_hk_1_claude_sdk",
                scene_unit_id="su_bu_1_hk_1",
                run_id="run_1",
                brief_unit_id="bu_1",
                arm="claude_sdk",
                hook_id="hk_1",
                line_coverage_pass=True,
                mode_pass=True,
                ugc_pass=True,
                evidence_pass=True,
                claim_safety_pass=True,
                pacing_pass=True,
                post_polish_pass=True,
                overall_pass=True,
            ),
            elapsed_seconds=0.01,
            error="",
        )
        failed_result = scene_engine._SceneUnitResult(
            arm="claude_sdk",
            brief_unit_id="bu_2",
            hook_id="hk_2",
            scene_unit_id="su_bu_2_hk_2",
            scene_plan=ScenePlanV1(
                scene_plan_id="sp_bu_2_hk_2_claude_sdk",
                run_id="run_1",
                brief_unit_id="bu_2",
                arm="claude_sdk",
                hook_id="hk_2",
                status="error",
                error="scene_gate_failed",
            ),
            gate_report=SceneGateReportV1(
                scene_plan_id="sp_bu_2_hk_2_claude_sdk",
                scene_unit_id="su_bu_2_hk_2",
                run_id="run_1",
                brief_unit_id="bu_2",
                arm="claude_sdk",
                hook_id="hk_2",
                line_coverage_pass=False,
                mode_pass=False,
                ugc_pass=False,
                evidence_pass=False,
                claim_safety_pass=False,
                pacing_pass=False,
                post_polish_pass=False,
                overall_pass=False,
                failure_reasons=["line_coverage_failed"],
            ),
            elapsed_seconds=0.01,
            error="scene_gate_failed",
        )
        packet = scene_engine._build_production_handoff_packet(
            run_id="run_1",
            scene_run_id="scv2_1",
            scene_items=scene_items,
            results=[ok_result, failed_result],
        )
        self.assertFalse(packet.ready)
        self.assertEqual(packet.ready_count, 1)
        self.assertEqual(packet.total_required, 2)


class Phase3V2SceneApiTests(unittest.TestCase):
    def setUp(self):
        server.pipeline_state["running"] = False
        server.pipeline_state["active_brand_slug"] = "brand_x"
        server.phase3_v2_scene_tasks.clear()

    def test_scenes_prepare_returns_ready_units_from_hook_handoff(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_scene_prepare"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch("server.config.PHASE3_V2_ENABLED", True), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ), patch("server.config.PHASE3_V2_SCENES_ENABLED", True):
                _seed_phase3_v2_scene_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=["bu_1", "bu_2"],
                    include_scene_files=False,
                )
                resp = asyncio.run(server.api_phase3_v2_scenes_prepare(branch_id, run_id, brand=brand_slug))

        self.assertEqual(int(resp.get("eligible_count", 0)), 2)
        self.assertEqual(int(resp.get("skipped_count", 0)), 0)

    def test_scenes_run_respects_selected_brief_units(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_scene_run_selected"

        async def _noop_execute_scenes(**kwargs):
            _ = kwargs
            return None

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch("server.config.PHASE3_V2_ENABLED", True), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ), patch("server.config.PHASE3_V2_SCENES_ENABLED", True), patch(
                "server._phase3_v2_execute_scenes",
                side_effect=_noop_execute_scenes,
            ):
                _seed_phase3_v2_scene_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=["bu_1", "bu_2"],
                )
                req = server.Phase3V2SceneRunRequest(
                    brand=brand_slug,
                    selected_brief_unit_ids=["bu_1"],
                    model_overrides={},
                )
                resp = asyncio.run(server.api_phase3_v2_scenes_run(branch_id, run_id, req))

        self.assertEqual(resp.get("status"), "started")
        self.assertEqual(int(resp.get("eligible_count", 0)), 1)

    def test_scenes_update_persists_lines(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_scene_update"
        unit_id = "bu_1"
        hook_id = f"hk_{unit_id}_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch("server.config.PHASE3_V2_ENABLED", True), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ), patch("server.config.PHASE3_V2_SCENES_ENABLED", True):
                run_dir = _seed_phase3_v2_scene_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                )
                req = server.Phase3V2SceneUpdateRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    source="manual",
                    lines=[
                        server.Phase3V2SceneLinePayload(
                            script_line_id="L01.1",
                            source_script_line_id="L01",
                            beat_index=1,
                            beat_text="Split beat one",
                            mode="a_roll",
                            a_roll={
                                "framing": "Close medium",
                                "creator_action": "Creator points to strap",
                                "performance_direction": "Urgent",
                                "product_interaction": "Shows battery pack",
                                "location": "Desk",
                            },
                            b_roll={},
                            on_screen_text="Pressure is real",
                            duration_seconds=2.2,
                            evidence_ids=["PROOF-001"],
                            difficulty_1_10=4,
                        )
                    ],
                )
                resp = asyncio.run(server.api_phase3_v2_scenes_update(branch_id, run_id, req))
                saved = json.loads(
                    (run_dir / "arm_claude_sdk_scene_plans.json").read_text("utf-8")
                )

        self.assertTrue(bool(resp.get("ok")))
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["lines"][0]["script_line_id"], "L01.1")
        self.assertEqual(saved[0]["lines"][0]["source_script_line_id"], "L01")
        self.assertEqual(int(saved[0]["lines"][0]["beat_index"]), 1)
        self.assertEqual(saved[0]["lines"][0]["on_screen_text"], "Pressure is real")
        self.assertEqual(saved[0]["lines"][0]["mode"], "a_roll")

    def test_scenes_chat_round_trip(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_scene_chat_round_trip"
        unit_id = "bu_1"
        hook_id = f"hk_{unit_id}_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch("server.config.PHASE3_V2_ENABLED", True), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ), patch("server.config.PHASE3_V2_SCENES_ENABLED", True), patch(
                "pipeline.llm.call_llm_structured",
                return_value=SceneChatReplyV1(
                    assistant_message="Use tighter A-roll framing.",
                    proposed_scene_plan=None,
                ),
            ):
                _seed_phase3_v2_scene_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                )
                post_req = server.Phase3V2SceneChatRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    message="Improve line 1",
                )
                post_resp = asyncio.run(server.api_phase3_v2_scenes_chat_post(branch_id, run_id, post_req))
                get_resp = asyncio.run(
                    server.api_phase3_v2_scenes_chat_get(
                        branch_id,
                        run_id,
                        brief_unit_id=unit_id,
                        arm="claude_sdk",
                        hook_id=hook_id,
                        brand=brand_slug,
                    )
                )

        self.assertEqual(post_resp.get("assistant_message"), "Use tighter A-roll framing.")
        self.assertEqual(len(post_resp.get("messages", [])), 2)
        self.assertEqual(len(get_resp.get("messages", [])), 2)

    def test_scenes_chat_apply_updates_scene_plan(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_scene_chat_apply"
        unit_id = "bu_1"
        hook_id = f"hk_{unit_id}_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch("server.config.PHASE3_V2_ENABLED", True), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ), patch("server.config.PHASE3_V2_SCENES_ENABLED", True):
                run_dir = _seed_phase3_v2_scene_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                )
                proposed = ScenePlanV1(
                    scene_plan_id=f"sp_{unit_id}_{hook_id}_claude_sdk",
                    run_id=run_id,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    lines=[
                        SceneLinePlanV1(
                            scene_line_id=f"sl_{unit_id}_{hook_id}_L01_1",
                            script_line_id="L01.1",
                            source_script_line_id="L01",
                            beat_index=1,
                            beat_text="Split beat one",
                            mode="a_roll",
                            a_roll=ARollDirectionV1(
                                framing="Tight close-up",
                                creator_action="Creator squeezes forehead cushion",
                                performance_direction="Serious",
                                product_interaction="Holds battery strap",
                                location="Studio desk",
                            ),
                            on_screen_text="This is the pressure point",
                            duration_seconds=2.4,
                            evidence_ids=["PROOF-001"],
                            difficulty_1_10=5,
                        )
                    ],
                )
                req = server.Phase3V2SceneChatApplyRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    proposed_scene_plan=proposed,
                )
                resp = asyncio.run(server.api_phase3_v2_scenes_chat_apply(branch_id, run_id, req))
                saved = json.loads((run_dir / "arm_claude_sdk_scene_plans.json").read_text("utf-8"))

        self.assertTrue(bool(resp.get("ok")))
        self.assertEqual(saved[0]["lines"][0]["on_screen_text"], "This is the pressure point")
        self.assertEqual(saved[0]["lines"][0]["script_line_id"], "L01.1")
        self.assertEqual(saved[0]["lines"][0]["source_script_line_id"], "L01")
        self.assertEqual(saved[0]["lines"][0]["a_roll"]["framing"], "Tight close-up")

    def test_locked_run_rejects_scene_mutations(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_scene_locked"
        unit_id = "bu_1"
        hook_id = f"hk_{unit_id}_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch("server.config.PHASE3_V2_ENABLED", True), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ), patch("server.config.PHASE3_V2_SCENES_ENABLED", True):
                _seed_phase3_v2_scene_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    lock_run=True,
                )
                update_req = server.Phase3V2SceneUpdateRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    lines=[
                        server.Phase3V2SceneLinePayload(
                            script_line_id="L01",
                            mode="a_roll",
                            a_roll={},
                            b_roll={},
                            on_screen_text="x",
                            duration_seconds=2.0,
                            evidence_ids=["PROOF-001"],
                            difficulty_1_10=4,
                        )
                    ],
                )
                update_resp = asyncio.run(server.api_phase3_v2_scenes_update(branch_id, run_id, update_req))
                self.assertIsInstance(update_resp, server.JSONResponse)
                self.assertEqual(update_resp.status_code, 409)

                chat_req = server.Phase3V2SceneChatRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    message="Improve this",
                )
                chat_resp = asyncio.run(server.api_phase3_v2_scenes_chat_post(branch_id, run_id, chat_req))
                self.assertIsInstance(chat_resp, server.JSONResponse)
                self.assertEqual(chat_resp.status_code, 409)

                apply_req = server.Phase3V2SceneChatApplyRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    proposed_scene_plan=ScenePlanV1(
                        scene_plan_id=f"sp_{unit_id}_{hook_id}_claude_sdk",
                        run_id=run_id,
                        brief_unit_id=unit_id,
                        arm="claude_sdk",
                        hook_id=hook_id,
                        lines=[],
                    ),
                )
                apply_resp = asyncio.run(server.api_phase3_v2_scenes_chat_apply(branch_id, run_id, apply_req))
                self.assertIsInstance(apply_resp, server.JSONResponse)
                self.assertEqual(apply_resp.status_code, 409)

    def test_run_detail_backward_compatible_without_scene_files(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_scene_legacy"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch("server.config.PHASE3_V2_ENABLED", True), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ), patch("server.config.PHASE3_V2_SCENES_ENABLED", True):
                _seed_phase3_v2_scene_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=["bu_1"],
                    include_scene_files=False,
                )
                detail = asyncio.run(server.api_phase3_v2_run_detail(branch_id, run_id, brand=brand_slug))

        self.assertIn("scene_stage", detail)
        self.assertIn("scene_plans_by_arm", detail)
        self.assertIn("scene_gate_reports_by_arm", detail)
        self.assertIn("production_handoff_packet", detail)
        self.assertIn("production_handoff_ready", detail)


if __name__ == "__main__":
    unittest.main()
