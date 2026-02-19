from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import server
from pipeline import phase3_v2_hook_engine as hook_engine
from schemas.phase3_v2 import (
    BriefUnitV1,
    CoreScriptDraftV1,
    CoreScriptLineV1,
    CoreScriptSectionsV1,
    EvidenceCoverageReportV1,
    EvidencePackV1,
    HookBundleV1,
    HookCandidateV1,
    HookContextV1,
    HookGateResultV1,
    HookScoreV1,
)


def _seed_phase3_v2_run(
    *,
    brand_slug: str,
    branch_id: str,
    run_id: str,
    unit_ids: list[str],
    script_status_by_unit: dict[str, str] | None = None,
    evidence_blocked_units: set[str] | None = None,
    write_hook_files: bool = False,
) -> Path:
    script_status_by_unit = dict(script_status_by_unit or {})
    evidence_blocked_units = set(evidence_blocked_units or set())

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
    brief_units: list[dict] = []
    evidence_packs: list[dict] = []
    drafts: list[dict] = []
    for idx, unit_id in enumerate(unit_ids, start=1):
        awareness = "problem_aware" if idx % 2 == 0 else "unaware"
        emotion_key = "frustration_pain" if idx % 2 == 0 else "desire_freedom_immersion"
        brief_units.append(
            {
                "brief_unit_id": unit_id,
                "matrix_cell_id": f"cell_{awareness}_{emotion_key}",
                "branch_id": branch_id,
                "brand_slug": brand_slug,
                "awareness_level": awareness,
                "emotion_key": emotion_key,
                "emotion_label": "Frustration / Pain" if emotion_key == "frustration_pain" else "Desire for Freedom / Immersion",
                "ordinal_in_cell": 1,
                "source_matrix_plan_hash": "matrix_hash",
            }
        )
        blocked = unit_id in evidence_blocked_units
        evidence_packs.append(
            {
                "pack_id": f"pack_{unit_id}",
                "brief_unit_id": unit_id,
                "voc_quote_refs": [
                    {
                        "quote_id": f"VOC-{idx:03d}",
                        "quote_excerpt": "This hurts after 10 minutes.",
                        "source_url": "https://example.com/review",
                        "source_type": "review",
                    }
                ],
                "proof_refs": [
                    {
                        "asset_id": f"PROOF-{idx:03d}",
                        "proof_type": "testimonial",
                        "title": "Comfort increase",
                        "detail": "Users report better comfort.",
                        "source_url": "https://example.com/proof",
                    }
                ],
                "mechanism_refs": [
                    {
                        "mechanism_id": f"MECH-{idx:03d}",
                        "title": "Counterweight mechanism",
                        "detail": "Rear battery rebalances headset load.",
                        "support_evidence_ids": [f"PROOF-{idx:03d}"],
                    }
                ],
                "coverage_report": {
                    "has_voc": True,
                    "has_proof": True,
                    "has_mechanism": True,
                    "voc_count": 1,
                    "proof_count": 1,
                    "mechanism_count": 1,
                    "blocked_evidence_insufficient": blocked,
                },
            }
        )

        status = str(script_status_by_unit.get(unit_id, "ok")).strip().lower() or "ok"
        drafts.append(
            {
                "script_id": f"script_{unit_id}",
                "brief_unit_id": unit_id,
                "arm": "claude_sdk",
                "status": status,
                "error": "" if status == "ok" else f"script_{status}",
                "sections": {
                    "hook": "Hook",
                    "problem": "Problem",
                    "mechanism": "Mechanism",
                    "proof": "Proof",
                    "cta": "CTA",
                },
                "lines": [
                    {"line_id": "L01", "text": "Script line one.", "evidence_ids": [f"PROOF-{idx:03d}"]},
                    {"line_id": "L02", "text": "Script line two.", "evidence_ids": [f"VOC-{idx:03d}"]},
                ],
                "model_metadata": {"provider": "anthropic", "model": "claude-opus-4-6", "sdk_used": True},
                "gate_report": {"overall_pass": status == "ok", "checks": []},
                "latency_seconds": 1.1,
                "cost_usd": 0.11,
            }
        )

    server._phase3_v2_write_json(
        run_dir / "manifest.json",
        {
            "run_id": run_id,
            "status": "completed",
            "created_at": "2026-02-17T00:00:00",
            "completed_at": "2026-02-17T00:01:00",
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
            "locked": False,
            "locked_at": "",
            "locked_by_role": "",
        },
    )

    if write_hook_files:
        server._phase3_v2_write_json(run_dir / "hook_selections.json", [])
        server._phase3_v2_write_json(run_dir / "hook_chat_threads.json", {})
        server._phase3_v2_write_json(
            run_dir / "hook_stage_manifest.json",
            {
                "run_id": run_id,
                "hook_run_id": "",
                "status": "idle",
                "created_at": "",
                "started_at": "",
                "completed_at": "",
                "error": "",
                "eligible_count": 0,
                "processed_count": 0,
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
        server._phase3_v2_write_json(
            run_dir / "scene_handoff_packet.json",
            {
                "run_id": run_id,
                "hook_run_id": "",
                "ready": False,
                "ready_count": 0,
                "total_required": 0,
                "generated_at": "",
                "items": [],
            },
        )

    return run_dir


class Phase3V2HookEngineTests(unittest.TestCase):
    def test_build_hook_context_carries_audience_fields(self):
        brief_unit = BriefUnitV1(
            brief_unit_id="bu_test_001",
            matrix_cell_id="cell_problem_aware_frustration_pain",
            branch_id="branch_1",
            brand_slug="brand_x",
            awareness_level="problem_aware",
            emotion_key="frustration_pain",
            emotion_label="Frustration / Pain",
            audience_segment_name="Biohacker Professional",
            audience_goals=["Sustain deep focus"],
            audience_pains=["Afternoon energy crashes"],
            audience_triggers=["Deadline-heavy days"],
            audience_objections=["Skeptical of overhyped formulas"],
            audience_information_sources=["Reddit"],
            lf8_code="lf8_3",
            lf8_label="Freedom from Fear / Pain",
            emotion_angle="Remove risk that this is placebo and ineffective.",
            blocking_objection="Does it actually work?",
            required_proof="Dose + efficacy proof with third-party support.",
            confidence=0.82,
            sample_quote_ids=["q_101", "q_102"],
            ordinal_in_cell=1,
            source_matrix_plan_hash="matrix_hash",
        )
        draft = CoreScriptDraftV1(
            script_id="script_1",
            brief_unit_id=brief_unit.brief_unit_id,
            arm="claude_sdk",
            sections=CoreScriptSectionsV1(
                hook="Hook",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            lines=[CoreScriptLineV1(line_id="L01", text="Line one", evidence_ids=[])],
            status="ok",
        )
        evidence_pack = EvidencePackV1(
            pack_id="pack_1",
            brief_unit_id=brief_unit.brief_unit_id,
            coverage_report=EvidenceCoverageReportV1(
                has_voc=True,
                has_proof=True,
                has_mechanism=True,
                blocked_evidence_insufficient=False,
            ),
        )

        context = hook_engine.build_hook_context(
            run_id="run_1",
            brief_unit=brief_unit,
            arm="claude_sdk",
            draft=draft,
            evidence_pack=evidence_pack,
        )

        self.assertEqual(context.audience_segment_name, "Biohacker Professional")
        self.assertEqual(context.audience_goals, ["Sustain deep focus"])
        self.assertEqual(context.audience_pains, ["Afternoon energy crashes"])
        self.assertEqual(context.audience_triggers, ["Deadline-heavy days"])
        self.assertEqual(context.audience_objections, ["Skeptical of overhyped formulas"])
        self.assertEqual(context.audience_information_sources, ["Reddit"])
        self.assertEqual(context.lf8_code, "lf8_3")
        self.assertEqual(context.lf8_label, "Freedom from Fear / Pain")
        self.assertEqual(context.blocking_objection, "Does it actually work?")
        self.assertTrue(bool(context.required_proof))
        self.assertGreater(context.confidence, 0.0)
        self.assertEqual(context.sample_quote_ids, ["q_101", "q_102"])

    def test_generate_candidates_keeps_default_script_verbal_as_first_hook(self):
        context = HookContextV1(
            run_id="run_1",
            brief_unit_id="bu_test_001",
            arm="claude_sdk",
            awareness_level="problem_aware",
            emotion_key="frustration_pain",
            emotion_label="Frustration / Pain",
            script_id="script_1",
            script_sections=CoreScriptSectionsV1(
                hook="Default verbal from script",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            script_lines=[CoreScriptLineV1(line_id="L01", text="Default verbal from script", evidence_ids=["PROOF-001"])],
            evidence_ids_allowed=["PROOF-001"],
            evidence_catalog={"PROOF-001": "Proof"},
        )

        generated = SimpleNamespace(
            candidates=[
                SimpleNamespace(
                    lane_id="script_default",
                    lane_label="Script Default",
                    verbal_open="Mutated verbal from model",
                    visual_pattern_interrupt="Open with headset slipping",
                    on_screen_text="Pain starts in 10 minutes",
                    evidence_ids=["PROOF-001"],
                    rationale="Anchor row",
                ),
                SimpleNamespace(
                    lane_id="pain_spike",
                    lane_label="Pain Spike",
                    verbal_open="Alternative verbal",
                    visual_pattern_interrupt="Forehead pressure close-up",
                    on_screen_text="Face pain",
                    evidence_ids=["PROOF-001"],
                    rationale="Alternative",
                ),
            ]
        )

        with patch(
            "pipeline.phase3_v2_hook_engine.call_claude_agent_structured",
            return_value=(generated, {}),
        ):
            rows = hook_engine.generate_candidates_divergent(
                context=context,
                candidate_target_per_unit=5,
            )

        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0].lane_id, "script_default")
        self.assertEqual(rows[0].verbal_open, "Default verbal from script")
        self.assertEqual(str(rows[0].visual_pattern_interrupt or ""), "")

    def test_score_and_rank_forces_default_hook_first(self):
        candidates = [
            HookCandidateV1(
                candidate_id="hc_default",
                brief_unit_id="bu_test_001",
                arm="claude_sdk",
                lane_id="script_default",
                verbal_open="Default verbal from script",
                visual_pattern_interrupt="Default visual",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
            HookCandidateV1(
                candidate_id="hc_alt",
                brief_unit_id="bu_test_001",
                arm="claude_sdk",
                lane_id="pain_spike",
                verbal_open="Alternative verbal",
                visual_pattern_interrupt="Alternative visual",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
        ]
        gate_rows = [
            HookGateResultV1(candidate_id="hc_default", brief_unit_id="bu_test_001", arm="claude_sdk", gate_pass=True),
            HookGateResultV1(candidate_id="hc_alt", brief_unit_id="bu_test_001", arm="claude_sdk", gate_pass=True),
        ]
        score_rows = [
            HookScoreV1(candidate_id="hc_default", brief_unit_id="bu_test_001", arm="claude_sdk", composite_score=80.0),
            HookScoreV1(candidate_id="hc_alt", brief_unit_id="bu_test_001", arm="claude_sdk", composite_score=99.0),
        ]

        with patch("pipeline.phase3_v2_hook_engine.config.PHASE3_V2_HOOK_MIN_NEW_VARIANTS", 1):
            selected_ids, _ = hook_engine.score_and_rank_candidates(
                candidates=candidates,
                gate_rows=gate_rows,
                score_rows=score_rows,
                final_variants_per_unit=2,
                forced_first_candidate_id="hc_default",
            )

        self.assertEqual(selected_ids[0], "hc_default")

    def test_default_hook_uses_first_line_when_section_has_meta_terms(self):
        context = HookContextV1(
            run_id="run_1",
            brief_unit_id="bu_test_001",
            arm="claude_sdk",
            awareness_level="unaware",
            emotion_key="anxiety_fear",
            emotion_label="Anxiety / Fear",
            script_id="script_1",
            script_sections=CoreScriptSectionsV1(
                hook="A pattern-interrupt moment that frames the pain.",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            script_lines=[
                CoreScriptLineV1(
                    line_id="L01",
                    text="Your Quest strap is hurting your face every session.",
                    evidence_ids=["PROOF-001"],
                )
            ],
            evidence_ids_allowed=["PROOF-001"],
            evidence_catalog={"PROOF-001": "Proof"},
        )

        generated = SimpleNamespace(candidates=[])
        with patch(
            "pipeline.phase3_v2_hook_engine.call_claude_agent_structured",
            return_value=(generated, {}),
        ):
            rows = hook_engine.generate_candidates_divergent(
                context=context,
                candidate_target_per_unit=5,
            )

        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0].lane_id, "script_default")
        self.assertEqual(rows[0].verbal_open, "Your Quest strap is hurting your face every session.")

    def test_default_hook_uses_first_line_when_section_is_summary_leadin(self):
        context = HookContextV1(
            run_id="run_1",
            brief_unit_id="bu_test_001",
            arm="claude_sdk",
            awareness_level="solution_aware",
            emotion_key="frustration_pain",
            emotion_label="Frustration / Pain",
            script_id="script_1",
            script_sections=CoreScriptSectionsV1(
                hook="Calls out solution-aware owners who still feel pain.",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            script_lines=[
                CoreScriptLineV1(
                    line_id="L01",
                    text="You upgraded your strap and it still hurts.",
                    evidence_ids=["PROOF-001"],
                )
            ],
            evidence_ids_allowed=["PROOF-001"],
            evidence_catalog={"PROOF-001": "Proof"},
        )

        generated = SimpleNamespace(candidates=[])
        with patch(
            "pipeline.phase3_v2_hook_engine.call_claude_agent_structured",
            return_value=(generated, {}),
        ):
            rows = hook_engine.generate_candidates_divergent(
                context=context,
                candidate_target_per_unit=5,
            )

        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0].lane_id, "script_default")
        self.assertEqual(rows[0].verbal_open, "You upgraded your strap and it still hurts.")

    def test_gate_rejects_meta_copy_term_in_verbal(self):
        context = HookContextV1(
            run_id="run_1",
            brief_unit_id="bu_test_001",
            arm="claude_sdk",
            awareness_level="problem_aware",
            emotion_key="frustration_pain",
            emotion_label="Frustration / Pain",
            script_id="script_1",
            script_sections=CoreScriptSectionsV1(
                hook="Hook",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            script_lines=[CoreScriptLineV1(line_id="L01", text="Line one", evidence_ids=["PROOF-001"])],
            evidence_ids_allowed=["PROOF-001"],
            evidence_catalog={"PROOF-001": "Trusted proof"},
        )
        candidate = HookCandidateV1(
            candidate_id="hc_meta",
            brief_unit_id="bu_test_001",
            arm="claude_sdk",
            lane_id="pattern_interrupt",
            lane_label="Pattern Interrupt",
            verbal_open="A pattern interupt setup that explains the idea.",
            visual_pattern_interrupt="Close-up shot",
            on_screen_text="Pain",
            awareness_level="problem_aware",
            emotion_key="frustration_pain",
            evidence_ids=["PROOF-001"],
            rationale="Meta wording",
        )

        with patch("pipeline.phase3_v2_hook_engine.call_llm_structured", side_effect=RuntimeError("offline")):
            gate_rows, _ = hook_engine.run_alignment_evidence_gate(
                context=context,
                candidates=[candidate],
            )

        self.assertEqual(len(gate_rows), 1)
        self.assertFalse(gate_rows[0].gate_pass)
        self.assertIn("meta_copy_term_in_verbal", gate_rows[0].failure_reasons)

    def test_meta_detector_flags_confronts_style_leadin(self):
        self.assertTrue(
            hook_engine._contains_meta_copy_terms(
                "Confronts the frustration of spending $650 on a Quest 3 only to rip it off your face."
            )
        )

    def test_generate_candidates_sanitizes_meta_default_verbal(self):
        context = HookContextV1(
            run_id="run_1",
            brief_unit_id="bu_test_001",
            arm="claude_sdk",
            awareness_level="problem_aware",
            emotion_key="frustration_pain",
            emotion_label="Frustration / Pain",
            script_id="script_1",
            script_sections=CoreScriptSectionsV1(
                hook="Confronts the pain in a summary-style line.",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            script_lines=[
                CoreScriptLineV1(
                    line_id="L01",
                    text="You paid for a Quest 3, not ten minutes of pain.",
                    evidence_ids=["PROOF-001"],
                )
            ],
            evidence_ids_allowed=["PROOF-001"],
            evidence_catalog={"PROOF-001": "Proof"},
        )
        generated = SimpleNamespace(
            candidates=[
                SimpleNamespace(
                    lane_id="script_default",
                    lane_label="Script Default",
                    verbal_open="Confronts the frustration of spending $650 on a Quest 3.",
                    visual_pattern_interrupt="Close-up",
                    on_screen_text="Pain starts fast",
                    evidence_ids=["PROOF-001"],
                    rationale="Meta summary",
                ),
            ]
        )
        with patch(
            "pipeline.phase3_v2_hook_engine.call_claude_agent_structured",
            return_value=(generated, {}),
        ):
            rows = hook_engine.generate_candidates_divergent(
                context=context,
                candidate_target_per_unit=5,
            )
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0].lane_id, "script_default")
        self.assertEqual(rows[0].verbal_open, "You paid for a Quest 3, not ten minutes of pain.")

    def test_score_and_rank_never_backfills_meta_summary_candidates(self):
        candidates = [
            HookCandidateV1(
                candidate_id="hc_default",
                brief_unit_id="bu_test_001",
                arm="claude_sdk",
                lane_id="script_default",
                verbal_open="You paid for a Quest 3, not ten minutes of pain.",
                visual_pattern_interrupt="Default visual",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
            HookCandidateV1(
                candidate_id="hc_meta",
                brief_unit_id="bu_test_001",
                arm="claude_sdk",
                lane_id="pain_spike",
                verbal_open="Calls out the frustration of paying for a painful headset.",
                visual_pattern_interrupt="Meta visual",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
            HookCandidateV1(
                candidate_id="hc_non_meta_fallback",
                brief_unit_id="bu_test_001",
                arm="claude_sdk",
                lane_id="mechanism_reveal",
                verbal_open="You keep readjusting because the stock strap dumps weight on your face.",
                visual_pattern_interrupt="Show the headset sag and pressure marks in one cut.",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
        ]
        gate_rows = [
            HookGateResultV1(candidate_id="hc_default", brief_unit_id="bu_test_001", arm="claude_sdk", gate_pass=True),
            HookGateResultV1(candidate_id="hc_meta", brief_unit_id="bu_test_001", arm="claude_sdk", gate_pass=False),
            HookGateResultV1(candidate_id="hc_non_meta_fallback", brief_unit_id="bu_test_001", arm="claude_sdk", gate_pass=False),
        ]
        score_rows = [
            HookScoreV1(candidate_id="hc_default", brief_unit_id="bu_test_001", arm="claude_sdk", composite_score=88.0),
            HookScoreV1(candidate_id="hc_meta", brief_unit_id="bu_test_001", arm="claude_sdk", composite_score=90.0),
            HookScoreV1(candidate_id="hc_non_meta_fallback", brief_unit_id="bu_test_001", arm="claude_sdk", composite_score=80.0),
        ]

        with patch("pipeline.phase3_v2_hook_engine.config.PHASE3_V2_HOOK_MIN_NEW_VARIANTS", 1):
            selected_ids, _ = hook_engine.score_and_rank_candidates(
                candidates=candidates,
                gate_rows=gate_rows,
                score_rows=score_rows,
                final_variants_per_unit=3,
                forced_first_candidate_id="hc_default",
            )

        self.assertIn("hc_default", selected_ids)
        self.assertIn("hc_non_meta_fallback", selected_ids)
        self.assertNotIn("hc_meta", selected_ids)

    def test_gate_rejects_alignment_and_invalid_evidence(self):
        context = HookContextV1(
            run_id="run_1",
            brief_unit_id="bu_test_001",
            arm="claude_sdk",
            awareness_level="problem_aware",
            emotion_key="frustration_pain",
            emotion_label="Frustration / Pain",
            script_id="script_1",
            script_sections=CoreScriptSectionsV1(
                hook="Hook",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            script_lines=[CoreScriptLineV1(line_id="L01", text="Line one", evidence_ids=["PROOF-001"])],
            evidence_ids_allowed=["PROOF-001", "VOC-001"],
            evidence_catalog={"PROOF-001": "Trusted proof", "VOC-001": "Customer quote"},
        )
        candidate = HookCandidateV1(
            candidate_id="hc_bu_test_001_pain_spike_001",
            brief_unit_id="bu_test_001",
            arm="claude_sdk",
            lane_id="pain_spike",
            lane_label="Pain Spike",
            verbal_open="You are not this audience.",
            visual_pattern_interrupt="Slow zoom",
            on_screen_text="Ignored issue",
            awareness_level="problem_aware",
            emotion_key="frustration_pain",
            evidence_ids=["UNKNOWN-EV"],
            rationale="Bad fit",
        )
        fake_eval = SimpleNamespace(
            evaluations=[
                SimpleNamespace(
                    candidate_id=candidate.candidate_id,
                    alignment_pass=False,
                    claim_boundary_pass=True,
                    scroll_stop_score=92,
                    specificity_score=90,
                    rationale="Off target awareness/emotion",
                )
            ]
        )
        with patch("pipeline.phase3_v2_hook_engine.call_llm_structured", return_value=fake_eval):
            gate_rows, score_rows = hook_engine.run_alignment_evidence_gate(
                context=context,
                candidates=[candidate],
            )

        self.assertEqual(len(gate_rows), 1)
        self.assertEqual(len(score_rows), 1)
        row = gate_rows[0]
        self.assertFalse(row.gate_pass)
        self.assertIn("alignment_mismatch", row.failure_reasons)
        self.assertIn("missing_or_invalid_evidence", row.failure_reasons)

    def test_score_and_rank_enforces_diversity(self):
        candidates = [
            HookCandidateV1(
                candidate_id="hc_a",
                brief_unit_id="bu_test_001",
                arm="claude_sdk",
                lane_id="pattern_interrupt",
                verbal_open="Quest strap pain starts at minute ten",
                visual_pattern_interrupt="Headset slips forward",
                on_screen_text="Pain spike",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
            HookCandidateV1(
                candidate_id="hc_b",
                brief_unit_id="bu_test_001",
                arm="claude_sdk",
                lane_id="pattern_interrupt",
                verbal_open="Quest strap pain starts at minute ten",
                visual_pattern_interrupt="Headset slips forward",
                on_screen_text="Pain spike",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
            HookCandidateV1(
                candidate_id="hc_c",
                brief_unit_id="bu_test_001",
                arm="claude_sdk",
                lane_id="mechanism_reveal",
                verbal_open="Battery counterweight rebalances your Quest in one move",
                visual_pattern_interrupt="Battery rear-mount demonstration",
                on_screen_text="Rebalanced comfort",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
        ]
        gate_rows = [
            HookGateResultV1(candidate_id="hc_a", brief_unit_id="bu_test_001", arm="claude_sdk", gate_pass=True),
            HookGateResultV1(candidate_id="hc_b", brief_unit_id="bu_test_001", arm="claude_sdk", gate_pass=True),
            HookGateResultV1(candidate_id="hc_c", brief_unit_id="bu_test_001", arm="claude_sdk", gate_pass=True),
        ]
        score_rows = [
            HookScoreV1(candidate_id="hc_a", brief_unit_id="bu_test_001", arm="claude_sdk", composite_score=95.0),
            HookScoreV1(candidate_id="hc_b", brief_unit_id="bu_test_001", arm="claude_sdk", composite_score=93.0),
            HookScoreV1(candidate_id="hc_c", brief_unit_id="bu_test_001", arm="claude_sdk", composite_score=90.0),
        ]

        with patch("pipeline.phase3_v2_hook_engine.config.PHASE3_V2_HOOK_MIN_NEW_VARIANTS", 1), patch(
            "pipeline.phase3_v2_hook_engine.config.PHASE3_V2_HOOK_MIN_LANE_COVERAGE",
            1,
        ), patch(
            "pipeline.phase3_v2_hook_engine.config.PHASE3_V2_HOOK_DIVERSITY_THRESHOLD",
            0.85,
        ):
            selected_ids, deficiency = hook_engine.score_and_rank_candidates(
                candidates=candidates,
                gate_rows=gate_rows,
                score_rows=score_rows,
                final_variants_per_unit=2,
            )

        self.assertEqual(selected_ids, ["hc_a", "hc_c"])
        self.assertNotIn("diversity_similarity_high", deficiency)

    def test_build_final_bundle_uses_deterministic_hook_ids(self):
        context = HookContextV1(
            run_id="run_1",
            brief_unit_id="bu_problem_aware_frustration_pain_001",
            arm="claude_sdk",
            awareness_level="problem_aware",
            emotion_key="frustration_pain",
        )
        candidates = [
            HookCandidateV1(
                candidate_id="hc_bu_problem_aware_frustration_pain_001_pattern_interrupt_001",
                brief_unit_id=context.brief_unit_id,
                arm="claude_sdk",
                lane_id="pattern_interrupt",
                verbal_open="Line A",
                visual_pattern_interrupt="Visual A",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
            HookCandidateV1(
                candidate_id="hc_bu_problem_aware_frustration_pain_001_mechanism_reveal_001",
                brief_unit_id=context.brief_unit_id,
                arm="claude_sdk",
                lane_id="mechanism_reveal",
                verbal_open="Line B",
                visual_pattern_interrupt="Visual B",
                awareness_level="problem_aware",
                emotion_key="frustration_pain",
                evidence_ids=["PROOF-001"],
            ),
        ]
        gate_rows = [
            HookGateResultV1(candidate_id=candidates[0].candidate_id, brief_unit_id=context.brief_unit_id, arm="claude_sdk", gate_pass=True),
            HookGateResultV1(candidate_id=candidates[1].candidate_id, brief_unit_id=context.brief_unit_id, arm="claude_sdk", gate_pass=True),
        ]
        score_rows = [
            HookScoreV1(candidate_id=candidates[0].candidate_id, brief_unit_id=context.brief_unit_id, arm="claude_sdk", scroll_stop_score=88, specificity_score=84),
            HookScoreV1(candidate_id=candidates[1].candidate_id, brief_unit_id=context.brief_unit_id, arm="claude_sdk", scroll_stop_score=85, specificity_score=83),
        ]

        bundle = hook_engine.build_final_bundle(
            hook_run_id="hkv2_1",
            context=context,
            candidates=candidates,
            gate_rows=gate_rows,
            score_rows=score_rows,
            selected_candidate_ids=[candidates[0].candidate_id, candidates[1].candidate_id],
            deficiency_flags=[],
            repair_rounds_used=0,
            final_variants_per_unit=2,
        )
        hook_ids = [v.hook_id for v in bundle.variants]
        self.assertEqual(
            hook_ids,
            [
                "hk_bu_problem_aware_frustration_pain_001_001",
                "hk_bu_problem_aware_frustration_pain_001_002",
            ],
        )

    def test_hook_stage_failure_isolated_per_unit(self):
        brief_1 = {
            "brief_unit_id": "bu_1",
            "matrix_cell_id": "cell_unaware_frustration_pain",
            "branch_id": "branch_1",
            "brand_slug": "brand_x",
            "awareness_level": "unaware",
            "emotion_key": "frustration_pain",
            "emotion_label": "Frustration / Pain",
            "ordinal_in_cell": 1,
            "source_matrix_plan_hash": "hash",
        }
        brief_2 = {**brief_1, "brief_unit_id": "bu_2"}
        draft = CoreScriptDraftV1(
            script_id="script_1",
            brief_unit_id="bu_1",
            arm="claude_sdk",
            sections=CoreScriptSectionsV1(
                hook="Hook",
                problem="Problem",
                mechanism="Mechanism",
                proof="Proof",
                cta="CTA",
            ),
            lines=[CoreScriptLineV1(line_id="L01", text="Line", evidence_ids=["PROOF-001"])],
            status="ok",
        )
        evidence = EvidencePackV1(
            pack_id="pack_1",
            brief_unit_id="bu_1",
            voc_quote_refs=[],
            proof_refs=[],
            mechanism_refs=[],
            coverage_report=EvidenceCoverageReportV1(
                has_voc=True,
                has_proof=True,
                has_mechanism=True,
                blocked_evidence_insufficient=False,
            ),
        )
        hook_items = [
            {"brief_unit_id": "bu_1", "arm": "claude_sdk", "brief_unit": brief_1, "draft": draft.model_dump(), "evidence_pack": evidence.model_dump()},
            {"brief_unit_id": "bu_2", "arm": "claude_sdk", "brief_unit": brief_2, "draft": {**draft.model_dump(), "brief_unit_id": "bu_2"}, "evidence_pack": {**evidence.model_dump(), "brief_unit_id": "bu_2"}},
        ]

        def _fake_run_unit(**kwargs):
            if kwargs["brief_unit"].brief_unit_id == "bu_2":
                raise RuntimeError("simulated_hook_failure")
            bundle = HookBundleV1(
                hook_run_id="hkv2_1",
                brief_unit_id="bu_1",
                arm="claude_sdk",
                variants=[],
                candidate_count=0,
                passed_gate_count=0,
                repair_rounds_used=0,
                deficiency_flags=[],
                status="ok",
                error="",
            )
            return hook_engine._HookUnitResult(
                arm="claude_sdk",
                brief_unit_id="bu_1",
                candidates=[],
                gate_rows=[],
                score_rows=[],
                bundle=bundle,
                elapsed_seconds=0.01,
                error="",
            )

        with patch("pipeline.phase3_v2_hook_engine._run_hook_unit", side_effect=_fake_run_unit):
            result = hook_engine.run_phase3_v2_hooks(
                run_id="run_1",
                hook_run_id="hkv2_1",
                hook_items=hook_items,
                candidate_target_per_unit=4,
                final_variants_per_unit=2,
            )

        manifest = result["hook_stage_manifest"]
        self.assertEqual(int(manifest.get("processed_count", 0)), 2)
        self.assertEqual(int(manifest.get("failed_count", 0)), 1)
        bundles = result["hook_bundles_by_arm"].get("claude_sdk", [])
        self.assertEqual(len(bundles), 2)
        self.assertTrue(any(str(row.get("status")) == "error" for row in bundles))


class Phase3V2HookApiTests(unittest.TestCase):
    def setUp(self):
        server.pipeline_state["running"] = False
        server.pipeline_state["active_brand_slug"] = "brand_x"
        server.phase3_v2_hook_tasks.clear()

    def test_hooks_prepare_returns_eligible_and_skipped(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_prepare"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=["bu_1", "bu_2"],
                    script_status_by_unit={"bu_2": "blocked"},
                    write_hook_files=True,
                )

                resp = asyncio.run(server.api_phase3_v2_hooks_prepare(branch_id, run_id, brand=brand_slug))

        self.assertEqual(resp.get("eligible_count"), 1)
        self.assertEqual(resp.get("skipped_count"), 1)
        skipped = resp.get("skipped_units", [])
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0].get("reason"), "script_blocked")

    def test_hooks_run_respects_selected_brief_units(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_run_select"

        async def _noop_execute_hooks(**kwargs):
            _ = kwargs
            return None

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ), patch(
                "server._phase3_v2_execute_hooks",
                side_effect=_noop_execute_hooks,
            ):
                _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=["bu_1", "bu_2"],
                    write_hook_files=True,
                )
                req = server.Phase3V2HookRunRequest(
                    brand=brand_slug,
                    selected_brief_unit_ids=["bu_1"],
                    candidate_target_per_unit=8,
                    final_variants_per_unit=3,
                )
                resp = asyncio.run(server.api_phase3_v2_hooks_run(branch_id, run_id, req))

        self.assertEqual(resp.get("status"), "started")
        self.assertEqual(int(resp.get("eligible_count", 0)), 1)

    def test_hooks_selections_accept_multiple_hook_ids(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_multi_select"
        unit_id = "bu_1"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                run_dir = _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    write_hook_files=True,
                )
                server._phase3_v2_write_json(
                    server._phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, "claude_sdk"),
                    [
                        {
                            "hook_run_id": "hkv2_1",
                            "brief_unit_id": unit_id,
                            "arm": "claude_sdk",
                            "script_id": f"script_{unit_id}",
                            "variants": [
                                {"hook_id": "hk_bu_1_001", "brief_unit_id": unit_id, "arm": "claude_sdk", "verbal_open": "A", "visual_pattern_interrupt": "VA", "awareness_level": "unaware", "emotion_key": "desire_freedom_immersion", "evidence_ids": ["PROOF-001"], "scroll_stop_score": 80, "specificity_score": 80, "lane_id": "script_default", "selection_status": "candidate", "gate_pass": True, "rank": 1},
                                {"hook_id": "hk_bu_1_002", "brief_unit_id": unit_id, "arm": "claude_sdk", "verbal_open": "B", "visual_pattern_interrupt": "VB", "awareness_level": "unaware", "emotion_key": "desire_freedom_immersion", "evidence_ids": ["PROOF-001"], "scroll_stop_score": 79, "specificity_score": 78, "lane_id": "pain_spike", "selection_status": "candidate", "gate_pass": True, "rank": 2},
                            ],
                            "candidate_count": 10,
                            "passed_gate_count": 8,
                            "repair_rounds_used": 0,
                            "deficiency_flags": [],
                            "status": "ok",
                            "error": "",
                            "generated_at": "2026-02-17T00:00:00",
                        }
                    ],
                )

                req = server.Phase3V2HookSelectionRequest(
                    brand=brand_slug,
                    selections=[
                        server.Phase3V2HookSelectionPayload(
                            brief_unit_id=unit_id,
                            arm="claude_sdk",
                            selected_hook_ids=["hk_bu_1_001", "hk_bu_1_002"],
                        )
                    ],
                )
                resp = asyncio.run(server.api_phase3_v2_hooks_selections(branch_id, run_id, req))
                saved = json.loads((run_dir / "hook_selections.json").read_text("utf-8"))

        self.assertTrue(bool(resp.get("ok")))
        self.assertEqual(resp.get("hook_selection_progress", {}).get("selected"), 1)
        self.assertTrue(bool(resp.get("hook_selection_progress", {}).get("ready")))
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0].get("selected_hook_ids"), ["hk_bu_1_001", "hk_bu_1_002"])
        self.assertEqual(saved[0].get("selected_hook_id"), "hk_bu_1_001")

    def test_hooks_update_persists_variant_fields(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_update"
        unit_id = "bu_1"
        hook_id = "hk_bu_1_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                run_dir = _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    write_hook_files=True,
                )
                server._phase3_v2_write_json(
                    server._phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, "claude_sdk"),
                    [
                        {
                            "hook_run_id": "hkv2_1",
                            "brief_unit_id": unit_id,
                            "arm": "claude_sdk",
                            "script_id": f"script_{unit_id}",
                            "variants": [
                                {
                                    "hook_id": hook_id,
                                    "brief_unit_id": unit_id,
                                    "arm": "claude_sdk",
                                    "verbal_open": "Old verbal",
                                    "visual_pattern_interrupt": "Old visual",
                                    "on_screen_text": "Old text",
                                    "awareness_level": "unaware",
                                    "emotion_key": "desire_freedom_immersion",
                                    "evidence_ids": ["VOC-001"],
                                    "scroll_stop_score": 80,
                                    "specificity_score": 80,
                                    "lane_id": "script_default",
                                    "selection_status": "candidate",
                                    "gate_pass": True,
                                    "rank": 1,
                                }
                            ],
                            "candidate_count": 1,
                            "passed_gate_count": 1,
                            "repair_rounds_used": 0,
                            "deficiency_flags": [],
                            "status": "ok",
                            "error": "",
                            "generated_at": "2026-02-17T00:00:00",
                        }
                    ],
                )

                req = server.Phase3V2HookUpdateRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    verbal_open="New verbal",
                    visual_pattern_interrupt="New visual",
                    on_screen_text="New text",
                    evidence_ids=["VOC-002", "PROOF-001", "VOC-002"],
                    source="manual",
                )
                resp = asyncio.run(server.api_phase3_v2_hooks_update(branch_id, run_id, req))
                saved = json.loads(
                    (
                        run_dir
                        / "arm_claude_sdk_hook_bundles.json"
                    ).read_text("utf-8")
                )

        self.assertTrue(bool(resp.get("ok")))
        variant = saved[0]["variants"][0]
        self.assertEqual(variant.get("verbal_open"), "New verbal")
        self.assertEqual(variant.get("visual_pattern_interrupt"), "New visual")
        self.assertEqual(variant.get("on_screen_text"), "New text")
        self.assertEqual(variant.get("evidence_ids"), ["VOC-002", "PROOF-001"])
        self.assertEqual(variant.get("edited_source"), "manual")

    def test_hooks_update_rejects_meta_summary_verbal(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_update_meta_reject"
        unit_id = "bu_1"
        hook_id = "hk_bu_1_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    write_hook_files=True,
                )
                server._phase3_v2_write_json(
                    server._phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, "claude_sdk"),
                    [
                        {
                            "hook_run_id": "hkv2_1",
                            "brief_unit_id": unit_id,
                            "arm": "claude_sdk",
                            "script_id": f"script_{unit_id}",
                            "variants": [
                                {
                                    "hook_id": hook_id,
                                    "brief_unit_id": unit_id,
                                    "arm": "claude_sdk",
                                    "verbal_open": "Old verbal",
                                    "visual_pattern_interrupt": "Old visual",
                                    "on_screen_text": "",
                                    "awareness_level": "unaware",
                                    "emotion_key": "desire_freedom_immersion",
                                    "evidence_ids": ["VOC-001"],
                                    "scroll_stop_score": 80,
                                    "specificity_score": 80,
                                    "lane_id": "script_default",
                                    "selection_status": "candidate",
                                    "gate_pass": True,
                                    "rank": 1,
                                }
                            ],
                            "candidate_count": 1,
                            "passed_gate_count": 1,
                            "repair_rounds_used": 0,
                            "deficiency_flags": [],
                            "status": "ok",
                            "error": "",
                            "generated_at": "2026-02-17T00:00:00",
                        }
                    ],
                )

                req = server.Phase3V2HookUpdateRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    verbal_open="Confronts the frustration of paying for a painful headset.",
                    visual_pattern_interrupt="New visual",
                    on_screen_text="New text",
                    evidence_ids=["VOC-002"],
                    source="manual",
                )
                resp = asyncio.run(server.api_phase3_v2_hooks_update(branch_id, run_id, req))

        self.assertIsInstance(resp, server.JSONResponse)
        self.assertEqual(resp.status_code, 400)

    def test_hooks_chat_thread_round_trip_per_hook(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_chat"
        unit_id = "bu_1"
        hook_id = "hk_bu_1_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                run_dir = _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    write_hook_files=True,
                )
                server._phase3_v2_write_json(
                    server._phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, "claude_sdk"),
                    [
                        {
                            "hook_run_id": "hkv2_1",
                            "brief_unit_id": unit_id,
                            "arm": "claude_sdk",
                            "script_id": f"script_{unit_id}",
                            "variants": [
                                {
                                    "hook_id": hook_id,
                                    "brief_unit_id": unit_id,
                                    "arm": "claude_sdk",
                                    "verbal_open": "Old verbal",
                                    "visual_pattern_interrupt": "Old visual",
                                    "on_screen_text": "Old text",
                                    "awareness_level": "unaware",
                                    "emotion_key": "desire_freedom_immersion",
                                    "evidence_ids": ["VOC-001"],
                                    "scroll_stop_score": 80,
                                    "specificity_score": 80,
                                    "lane_id": "script_default",
                                    "selection_status": "candidate",
                                    "gate_pass": True,
                                    "rank": 1,
                                }
                            ],
                            "candidate_count": 1,
                            "passed_gate_count": 1,
                            "repair_rounds_used": 0,
                            "deficiency_flags": [],
                            "status": "ok",
                            "error": "",
                            "generated_at": "2026-02-17T00:00:00",
                        }
                    ],
                )

                with patch(
                    "pipeline.llm.call_llm_structured",
                    return_value=server.Phase3V2HookChatReply(
                        assistant_message="Here is a tighter verbal line.",
                        proposed_hook=None,
                    ),
                ):
                    post_req = server.Phase3V2HookChatRequest(
                        brand=brand_slug,
                        brief_unit_id=unit_id,
                        arm="claude_sdk",
                        hook_id=hook_id,
                        message="Give me one tighter verbal rewrite.",
                    )
                    post_resp = asyncio.run(server.api_phase3_v2_hooks_chat_post(branch_id, run_id, post_req))
                    get_resp = asyncio.run(
                        server.api_phase3_v2_hooks_chat_get(
                            branch_id,
                            run_id,
                            brief_unit_id=unit_id,
                            arm="claude_sdk",
                            hook_id=hook_id,
                            brand=brand_slug,
                        )
                    )
                    saved_threads = json.loads((run_dir / "hook_chat_threads.json").read_text("utf-8"))

        self.assertEqual(post_resp.get("assistant_message"), "Here is a tighter verbal line.")
        self.assertEqual(len(post_resp.get("messages", [])), 2)
        self.assertEqual(len(get_resp.get("messages", [])), 2)
        self.assertIn(f"{unit_id}::claude_sdk::{hook_id}", saved_threads)

    def test_hooks_chat_apply_updates_variant(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_chat_apply"
        unit_id = "bu_1"
        hook_id = "hk_bu_1_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                run_dir = _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    write_hook_files=True,
                )
                server._phase3_v2_write_json(
                    server._phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, "claude_sdk"),
                    [
                        {
                            "hook_run_id": "hkv2_1",
                            "brief_unit_id": unit_id,
                            "arm": "claude_sdk",
                            "script_id": f"script_{unit_id}",
                            "variants": [
                                {
                                    "hook_id": hook_id,
                                    "brief_unit_id": unit_id,
                                    "arm": "claude_sdk",
                                    "verbal_open": "Old verbal",
                                    "visual_pattern_interrupt": "Old visual",
                                    "on_screen_text": "",
                                    "awareness_level": "unaware",
                                    "emotion_key": "desire_freedom_immersion",
                                    "evidence_ids": ["VOC-001"],
                                    "scroll_stop_score": 80,
                                    "specificity_score": 80,
                                    "lane_id": "script_default",
                                    "selection_status": "candidate",
                                    "gate_pass": True,
                                    "rank": 1,
                                }
                            ],
                            "candidate_count": 1,
                            "passed_gate_count": 1,
                            "repair_rounds_used": 0,
                            "deficiency_flags": [],
                            "status": "ok",
                            "error": "",
                            "generated_at": "2026-02-17T00:00:00",
                        }
                    ],
                )

                apply_req = server.Phase3V2HookChatApplyRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    proposed_hook=server.Phase3V2HookProposedPayload(
                        verbal_open="Applied verbal",
                        visual_pattern_interrupt="Applied visual",
                        on_screen_text="Applied text",
                        evidence_ids=["PROOF-001", "PROOF-001"],
                    ),
                )
                resp = asyncio.run(server.api_phase3_v2_hooks_chat_apply(branch_id, run_id, apply_req))
                saved = json.loads(
                    (
                        run_dir
                        / "arm_claude_sdk_hook_bundles.json"
                    ).read_text("utf-8")
                )

        self.assertTrue(bool(resp.get("ok")))
        variant = saved[0]["variants"][0]
        self.assertEqual(variant.get("verbal_open"), "Applied verbal")
        self.assertEqual(variant.get("visual_pattern_interrupt"), "Applied visual")
        self.assertEqual(variant.get("on_screen_text"), "Applied text")
        self.assertEqual(variant.get("evidence_ids"), ["PROOF-001"])
        self.assertEqual(variant.get("edited_source"), "chat_apply")

    def test_hooks_chat_apply_rejects_meta_summary_verbal(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_chat_apply_meta_reject"
        unit_id = "bu_1"
        hook_id = "hk_bu_1_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    write_hook_files=True,
                )
                server._phase3_v2_write_json(
                    server._phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, "claude_sdk"),
                    [
                        {
                            "hook_run_id": "hkv2_1",
                            "brief_unit_id": unit_id,
                            "arm": "claude_sdk",
                            "script_id": f"script_{unit_id}",
                            "variants": [
                                {
                                    "hook_id": hook_id,
                                    "brief_unit_id": unit_id,
                                    "arm": "claude_sdk",
                                    "verbal_open": "Old verbal",
                                    "visual_pattern_interrupt": "Old visual",
                                    "on_screen_text": "",
                                    "awareness_level": "unaware",
                                    "emotion_key": "desire_freedom_immersion",
                                    "evidence_ids": ["VOC-001"],
                                    "scroll_stop_score": 80,
                                    "specificity_score": 80,
                                    "lane_id": "script_default",
                                    "selection_status": "candidate",
                                    "gate_pass": True,
                                    "rank": 1,
                                }
                            ],
                            "candidate_count": 1,
                            "passed_gate_count": 1,
                            "repair_rounds_used": 0,
                            "deficiency_flags": [],
                            "status": "ok",
                            "error": "",
                            "generated_at": "2026-02-17T00:00:00",
                        }
                    ],
                )

                apply_req = server.Phase3V2HookChatApplyRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    proposed_hook=server.Phase3V2HookProposedPayload(
                        verbal_open="Calls out the frustration of paying for a painful headset.",
                        visual_pattern_interrupt="Applied visual",
                        on_screen_text="Applied text",
                        evidence_ids=["PROOF-001"],
                    ),
                )
                resp = asyncio.run(server.api_phase3_v2_hooks_chat_apply(branch_id, run_id, apply_req))

        self.assertIsInstance(resp, server.JSONResponse)
        self.assertEqual(resp.status_code, 400)

    def test_locked_run_rejects_hook_edit_and_chat_mutations(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_locked"
        unit_id = "bu_1"
        hook_id = "hk_bu_1_001"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    write_hook_files=True,
                )
                server._phase3_v2_write_json(
                    server._phase3_v2_hook_bundles_path(brand_slug, branch_id, run_id, "claude_sdk"),
                    [
                        {
                            "hook_run_id": "hkv2_1",
                            "brief_unit_id": unit_id,
                            "arm": "claude_sdk",
                            "script_id": f"script_{unit_id}",
                            "variants": [
                                {
                                    "hook_id": hook_id,
                                    "brief_unit_id": unit_id,
                                    "arm": "claude_sdk",
                                    "verbal_open": "Old verbal",
                                    "visual_pattern_interrupt": "Old visual",
                                    "on_screen_text": "",
                                    "awareness_level": "unaware",
                                    "emotion_key": "desire_freedom_immersion",
                                    "evidence_ids": ["VOC-001"],
                                    "scroll_stop_score": 80,
                                    "specificity_score": 80,
                                    "lane_id": "script_default",
                                    "selection_status": "candidate",
                                    "gate_pass": True,
                                    "rank": 1,
                                }
                            ],
                            "candidate_count": 1,
                            "passed_gate_count": 1,
                            "repair_rounds_used": 0,
                            "deficiency_flags": [],
                            "status": "ok",
                            "error": "",
                            "generated_at": "2026-02-17T00:00:00",
                        }
                    ],
                )
                server._phase3_v2_write_json(
                    server._phase3_v2_final_lock_path(brand_slug, branch_id, run_id),
                    {
                        "run_id": run_id,
                        "locked": True,
                        "locked_at": "2026-02-17T00:00:00",
                        "locked_by_role": "client_founder",
                    },
                )

                update_req = server.Phase3V2HookUpdateRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    verbal_open="x",
                    visual_pattern_interrupt="y",
                    on_screen_text="",
                    evidence_ids=["VOC-001"],
                    source="manual",
                )
                update_resp = asyncio.run(server.api_phase3_v2_hooks_update(branch_id, run_id, update_req))
                self.assertIsInstance(update_resp, server.JSONResponse)
                self.assertEqual(update_resp.status_code, 409)

                chat_req = server.Phase3V2HookChatRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    message="Improve this",
                )
                chat_resp = asyncio.run(server.api_phase3_v2_hooks_chat_post(branch_id, run_id, chat_req))
                self.assertIsInstance(chat_resp, server.JSONResponse)
                self.assertEqual(chat_resp.status_code, 409)

                apply_req = server.Phase3V2HookChatApplyRequest(
                    brand=brand_slug,
                    brief_unit_id=unit_id,
                    arm="claude_sdk",
                    hook_id=hook_id,
                    proposed_hook=server.Phase3V2HookProposedPayload(
                        verbal_open="a",
                        visual_pattern_interrupt="b",
                        on_screen_text="",
                        evidence_ids=["VOC-001"],
                    ),
                )
                apply_resp = asyncio.run(server.api_phase3_v2_hooks_chat_apply(branch_id, run_id, apply_req))
                self.assertIsInstance(apply_resp, server.JSONResponse)
                self.assertEqual(apply_resp.status_code, 409)

    def test_script_edit_marks_hook_selection_stale(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_hooks_stale"
        unit_id = "bu_1"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ), patch(
                "server.config.PHASE3_V2_HOOKS_ENABLED",
                True,
            ):
                run_dir = _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=[unit_id],
                    write_hook_files=True,
                )
                server._phase3_v2_write_json(
                    run_dir / "hook_selections.json",
                    [
                        {
                            "run_id": run_id,
                            "hook_run_id": "hkv2_1",
                            "brief_unit_id": unit_id,
                            "arm": "claude_sdk",
                            "selected_hook_id": "hk_bu_1_001",
                            "skip": False,
                            "stale": False,
                            "stale_reason": "",
                            "updated_at": "2026-02-17T00:00:00",
                        }
                    ],
                )

                req = server.Phase3V2DraftUpdateRequest(
                    brand=brand_slug,
                    source="manual",
                    sections=CoreScriptSectionsV1(
                        hook="Edited hook",
                        problem="Edited problem",
                        mechanism="Edited mechanism",
                        proof="Edited proof",
                        cta="Edited CTA",
                    ),
                    lines=[
                        server.Phase3V2DraftLinePayload(
                            line_id="",
                            text="Edited line",
                            evidence_ids=["PROOF-001"],
                        )
                    ],
                )
                resp = asyncio.run(
                    server.api_phase3_v2_update_draft(
                        branch_id,
                        run_id,
                        "claude_sdk",
                        unit_id,
                        req,
                    )
                )

                self.assertTrue(resp.get("ok"))
                saved = json.loads((run_dir / "hook_selections.json").read_text("utf-8"))

        self.assertEqual(len(saved), 1)
        self.assertTrue(saved[0].get("stale"))
        self.assertEqual(saved[0].get("stale_reason"), "script_updated_after_hook_run")

    def test_run_detail_backward_compatible_without_hook_files(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        run_id = "p3v2_legacy_no_hook_files"

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)), patch(
                "server.config.PHASE3_V2_ENABLED",
                True,
            ):
                _seed_phase3_v2_run(
                    brand_slug=brand_slug,
                    branch_id=branch_id,
                    run_id=run_id,
                    unit_ids=["bu_1"],
                    write_hook_files=False,
                )

                detail = asyncio.run(server.api_phase3_v2_run_detail(branch_id, run_id, brand=brand_slug))

        self.assertIn("hook_stage", detail)
        self.assertIn("hook_bundles_by_arm", detail)
        self.assertIn("hook_selection_progress", detail)
        self.assertIn("scene_handoff_packet", detail)
        self.assertEqual(detail.get("hook_stage", {}).get("status"), "idle")
        self.assertFalse(bool(detail.get("scene_handoff_ready")))


if __name__ == "__main__":
    unittest.main()
