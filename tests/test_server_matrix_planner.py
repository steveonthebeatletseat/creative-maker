from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.responses import JSONResponse

import server


def _foundation_fixture() -> dict:
    return {
        "schema_version": "2.0",
        "brand_name": "Animus Labs",
        "product_name": "Focus Strips",
        "pillar_1_prospect_profile": {
            "segment_profiles": [
                {
                    "segment_name": "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)",
                    "goals": ["Sustain deep focus during work blocks"],
                    "pains": ["Afternoon energy crashes"],
                    "triggers": ["Deadline-heavy days"],
                    "objections": ["Skeptical of overhyped formulas"],
                    "information_sources": ["Reddit", "YouTube reviews"],
                },
                {
                    "segment_name": "Burned-Out Startup Operator",
                    "goals": ["Recover predictable focus"],
                    "pains": ["Mental fatigue by 3pm"],
                    "triggers": ["Back-to-back meetings"],
                    "objections": ["Wary of stimulant crash"],
                    "information_sources": ["Founder forums"],
                },
                {
                    "segment_name": "Evidence-Light Segment",
                    "goals": ["Find a quick fix"],
                    "pains": ["No consistency"],
                    "triggers": ["High-pressure mornings"],
                    "objections": ["No proof yet"],
                    "information_sources": ["TikTok"],
                },
            ]
        },
        "pillar_2_voc_language_bank": {
            "quotes": [
                {
                    "quote_id": "q_001",
                    "quote": "I keep crashing by mid-afternoon.",
                    "dominant_emotion": "Frustration / Pain",
                    "segment_name": "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)",
                },
                {
                    "quote_id": "q_002",
                    "quote": "I want clean focus without jitters.",
                    "dominant_emotion": "Desire for Freedom / Immersion",
                    "segment_name": "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)",
                },
                {
                    "quote_id": "q_003",
                    "quote": "I hit a wall after every meeting block.",
                    "dominant_emotion": "Frustration / Pain",
                    "segment_name": "Burned-Out Startup Operator",
                },
                {
                    "quote_id": "q_004",
                    "quote": "I doubt these claims because nobody shows data.",
                    "dominant_emotion": "Skepticism / Distrust",
                    "segment_name": "Burned-Out Startup Operator",
                },
                {
                    "quote_id": "q_005",
                    "quote": "Unknown bucket quote should be ignored.",
                    "dominant_emotion": "Other emotion",
                    "segment_name": "Burned-Out Startup Operator",
                },
            ]
        },
        "pillar_6_emotional_driver_inventory": {
            "dominant_emotions": [
                {
                    "emotion": "Frustration / Pain",
                    "tagged_quote_count": 9,
                    "share_of_voc": 0.45,
                    "sample_quote_ids": ["q_001", "q_003"],
                },
                {
                    "emotion": "Desire for Freedom / Immersion",
                    "tagged_quote_count": 7,
                    "share_of_voc": 0.35,
                    "sample_quote_ids": ["q_002"],
                },
                {
                    "emotion": "Skepticism / Distrust",
                    "tagged_quote_count": 4,
                    "share_of_voc": 0.2,
                    "sample_quote_ids": ["q_004"],
                },
                {
                    "emotion": "frustration pain",
                    "tagged_quote_count": 2,
                    "share_of_voc": 0.1,
                    "sample_quote_ids": ["q_999"],
                },
            ],
            "lf8_mode": "strict_lf8",
            "lf8_rows_by_segment": {
                "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)": [
                    {
                        "lf8_code": "lf8_3",
                        "lf8_label": "Freedom from Fear / Pain",
                        "emotion_angle": "Remove fear that the product is placebo and ineffective.",
                        "segment_name": "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)",
                        "tagged_quote_count": 2,
                        "share_of_segment_voc": 1.0,
                        "unique_domains": 2,
                        "sample_quote_ids": ["q_001", "q_002"],
                        "support_evidence_ids": ["ev_101", "ev_102"],
                        "blocking_objection": "Does it actually work?",
                        "required_proof": "Dose + efficacy proof with third-party support.",
                        "contradiction_risk": "low",
                        "confidence": 0.82,
                        "buying_power_score": 88.2,
                    },
                    {
                        "lf8_code": "lf8_6",
                        "lf8_label": "Status & Winning",
                        "emotion_angle": "Signal disciplined high-performance identity.",
                        "segment_name": "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)",
                        "tagged_quote_count": 2,
                        "share_of_segment_voc": 1.0,
                        "unique_domains": 1,
                        "sample_quote_ids": ["q_001", "q_002"],
                        "support_evidence_ids": ["ev_103"],
                        "blocking_objection": "",
                        "required_proof": "Identity/status proof from credible peers or performance outcomes.",
                        "contradiction_risk": "low",
                        "confidence": 0.64,
                        "buying_power_score": 61.1,
                    },
                ],
                "Burned-Out Startup Operator": [
                    {
                        "lf8_code": "lf8_3",
                        "lf8_label": "Freedom from Fear / Pain",
                        "emotion_angle": "De-risk buying decision with clear efficacy proof.",
                        "segment_name": "Burned-Out Startup Operator",
                        "tagged_quote_count": 2,
                        "share_of_segment_voc": 1.0,
                        "unique_domains": 2,
                        "sample_quote_ids": ["q_003", "q_004"],
                        "support_evidence_ids": ["ev_201"],
                        "blocking_objection": "I doubt these claims because nobody shows data.",
                        "required_proof": "Risk-reduction proof that the claim works in real use.",
                        "contradiction_risk": "low",
                        "confidence": 0.79,
                        "buying_power_score": 84.5,
                    }
                ],
                "Evidence-Light Segment": [],
            },
        },
        "pillar_3_competitive_intelligence": {
            "direct_competitors": [],
            "substitute_categories": [],
            "mechanism_saturation_map": [],
        },
        "pillar_4_product_mechanism_analysis": {
            "why_problem_exists": "People lack consistent focus routines.",
            "why_solution_uniquely_works": "Fast format lowers friction to start.",
            "primary_mechanism_name": "Ritual Cue",
            "mechanism_supporting_evidence_ids": [],
        },
        "pillar_5_awareness_classification": {
            "segment_classifications": [],
        },
        "pillar_7_proof_credibility_inventory": {
            "assets": [],
        },
        "evidence_ledger": [],
        "cross_pillar_consistency_report": {
            "objections_represented_in_voc": True,
            "mechanism_alignment_with_competition": True,
            "dominant_emotions_traced_to_voc": True,
            "issues": [],
        },
        "quality_gate_report": {
            "overall_pass": False,
            "failed_gate_ids": [],
            "checks": [],
            "retry_rounds_used": 0,
        },
    }


def _write_foundation_output(tmpdir: str, brand_slug: str, foundation: dict) -> None:
    base = Path(tmpdir) / brand_slug
    base.mkdir(parents=True, exist_ok=True)
    path = server._output_write_path(base, "foundation_research")
    path.write_text(json.dumps(foundation, indent=2), encoding="utf-8")


def _write_collectors_snapshot(tmpdir: str, brand_slug: str, reports: list[str]) -> None:
    base = Path(tmpdir) / brand_slug
    base.mkdir(parents=True, exist_ok=True)
    payload = {
        "stage": "collectors_complete",
        "collector_reports": [
            {
                "label": f"report_{idx}",
                "provider": f"provider_{idx}",
                "report_chars": len(text),
                "report_preview": text,
            }
            for idx, text in enumerate(reports, start=1)
        ],
    }
    (base / "foundation_research_collectors_snapshot.json").write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )


class MatrixPlannerServerTests(unittest.TestCase):
    def setUp(self) -> None:
        server.pipeline_state["running"] = False
        server.pipeline_state["active_brand_slug"] = "brand_x"

    def test_extract_matrix_axes_global_legacy_uses_pillar6_order_and_dedupe(self):
        awareness_levels, emotion_rows, mode, requires_selection, message = server._extract_matrix_axes(
            _foundation_fixture(),
            allow_global_legacy=True,
        )

        self.assertEqual(awareness_levels, list(server.MATRIX_AWARENESS_LEVELS))
        self.assertEqual(mode, "global_legacy")
        self.assertFalse(requires_selection)
        self.assertEqual(message, "")
        self.assertEqual(
            [row.get("emotion_label") for row in emotion_rows],
            [
                "Frustration / Pain",
                "Desire for Freedom / Immersion",
                "Skepticism / Distrust",
            ],
        )
        self.assertEqual(int(emotion_rows[0].get("tagged_quote_count", 0)), 9)
        self.assertEqual(float(emotion_rows[0].get("share_of_voc", 0.0)), 0.45)
        self.assertEqual(emotion_rows[0].get("sample_quote_ids"), ["q_001", "q_003"])

    def test_extract_matrix_axes_requires_audience_selection(self):
        awareness_levels, emotion_rows, mode, requires_selection, message = server._extract_matrix_axes(
            _foundation_fixture(),
            require_audience_selection=True,
            allow_global_legacy=False,
        )
        self.assertEqual(awareness_levels, list(server.MATRIX_AWARENESS_LEVELS))
        self.assertEqual(emotion_rows, [])
        self.assertEqual(mode, "lf8_empty")
        self.assertTrue(requires_selection)
        self.assertIn("Select one Pillar 1 audience", message)

    def test_extract_matrix_axes_selected_audience_filters_rows(self):
        awareness_levels, emotion_rows, mode, requires_selection, message = server._extract_matrix_axes(
            _foundation_fixture(),
            selected_audience_segment="the optimization-obsessed knowledge worker (biohacker professional)",
            require_audience_selection=True,
        )
        self.assertEqual(awareness_levels, list(server.MATRIX_AWARENESS_LEVELS))
        self.assertEqual(mode, "lf8_audience_scoped")
        self.assertFalse(requires_selection)
        self.assertEqual(message, "")
        self.assertEqual(
            [row.get("emotion_label") for row in emotion_rows],
            ["Freedom from Fear / Pain", "Status & Winning"],
        )
        self.assertEqual(str(emotion_rows[0].get("lf8_code")), "lf8_3")
        self.assertEqual(int(emotion_rows[0].get("tagged_quote_count", 0)), 2)
        self.assertEqual(float(emotion_rows[0].get("share_of_segment_voc", 0.0)), 1.0)
        self.assertEqual(emotion_rows[0].get("sample_quote_ids"), ["q_001", "q_002"])
        self.assertEqual(str(emotion_rows[1].get("lf8_code")), "lf8_6")

    def test_extract_matrix_axes_selected_audience_can_be_empty(self):
        awareness_levels, emotion_rows, mode, requires_selection, message = server._extract_matrix_axes(
            _foundation_fixture(),
            selected_audience_segment="Evidence-Light Segment",
            require_audience_selection=True,
        )
        self.assertEqual(awareness_levels, list(server.MATRIX_AWARENESS_LEVELS))
        self.assertEqual(emotion_rows, [])
        self.assertEqual(mode, "lf8_empty")
        self.assertFalse(requires_selection)
        self.assertIn("No LF8 rows passed evidence gates", message)

    def test_build_matrix_plan_embeds_selected_audience_and_supports_legacy_blank(self):
        foundation = _foundation_fixture()
        awareness = server.MATRIX_AWARENESS_LEVELS[0]
        valid_inputs = {
            "foundation_brief": foundation,
            "matrix_cells": [
                {
                    "awareness_level": awareness,
                    "emotion_key": "lf8_3",
                    "brief_count": 1,
                }
            ],
            "selected_audience_segment": "  the optimization-obsessed knowledge worker (biohacker professional)  ",
        }

        plan = server._build_matrix_plan(valid_inputs)
        self.assertEqual(plan.get("emotion_source_mode"), "lf8_audience_scoped")
        self.assertEqual(
            plan.get("selected_audience_segment"),
            "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)",
        )
        self.assertEqual(
            plan.get("audience", {}).get("segment_name"),
            "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)",
        )
        self.assertEqual(
            plan.get("audience", {}).get("goals"),
            ["Sustain deep focus during work blocks"],
        )

        legacy_inputs = dict(valid_inputs)
        legacy_inputs["selected_audience_segment"] = ""
        legacy_inputs["matrix_cells"] = [
            {
                "awareness_level": awareness,
                "emotion_key": "frustration_pain",
                "brief_count": 1,
            }
        ]
        legacy_plan = server._build_matrix_plan(legacy_inputs)
        self.assertEqual(legacy_plan.get("emotion_source_mode"), "global_legacy")
        self.assertEqual(legacy_plan.get("selected_audience_segment"), "")
        self.assertEqual(legacy_plan.get("audience", {}).get("segment_name"), "")
        self.assertEqual(legacy_plan.get("audience", {}).get("goals"), [])

        invalid_inputs = dict(valid_inputs)
        invalid_inputs["selected_audience_segment"] = "Unknown Segment"
        with self.assertRaises(RuntimeError):
            server._build_matrix_plan(invalid_inputs)

    def test_api_matrix_axes_requires_audience_then_returns_scoped_rows(self):
        brand_slug = "brand_x"
        foundation = _foundation_fixture()
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)):
                _write_foundation_output(tmpdir, brand_slug, foundation)

                empty_resp = asyncio.run(server.api_matrix_axes(brand=brand_slug))
                self.assertIsInstance(empty_resp, dict)
                self.assertEqual(empty_resp.get("emotion_rows"), [])
                self.assertTrue(bool(empty_resp.get("requires_audience_selection")))
                self.assertEqual(empty_resp.get("emotion_source_mode"), "lf8_empty")

                scoped_resp = asyncio.run(
                    server.api_matrix_axes(
                        brand=brand_slug,
                        selected_audience_segment="burned-out startup operator",
                    )
                )
                self.assertIsInstance(scoped_resp, dict)
                self.assertFalse(bool(scoped_resp.get("requires_audience_selection")))
                self.assertEqual(scoped_resp.get("emotion_source_mode"), "lf8_audience_scoped")
                self.assertEqual(
                    [row.get("emotion_label") for row in scoped_resp.get("emotion_rows", [])],
                    ["Freedom from Fear / Pain"],
                )

                no_rows_resp = asyncio.run(
                    server.api_matrix_axes(
                        brand=brand_slug,
                        selected_audience_segment="Evidence-Light Segment",
                    )
                )
                self.assertIsInstance(no_rows_resp, dict)
                self.assertEqual(no_rows_resp.get("emotion_rows"), [])
                self.assertEqual(no_rows_resp.get("emotion_source_mode"), "lf8_empty")
                self.assertIn("No LF8 rows passed evidence gates", str(no_rows_resp.get("message") or ""))

                invalid_resp = asyncio.run(
                    server.api_matrix_axes(
                        brand=brand_slug,
                        selected_audience_segment="Unknown Segment",
                    )
                )
                self.assertIsInstance(invalid_resp, JSONResponse)
                self.assertEqual(invalid_resp.status_code, 400)
                self.assertIn("was not found in Pillar 1 segment_profiles", invalid_resp.body.decode("utf-8"))

    def test_api_create_branch_requires_valid_audience_with_non_empty_rows(self):
        brand_slug = "brand_x"
        foundation = _foundation_fixture()
        awareness = server.MATRIX_AWARENESS_LEVELS[0]
        matrix_cells = [
            {
                "awareness_level": awareness,
                "emotion_key": "lf8_3",
                "brief_count": 1,
            }
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)):
                _write_foundation_output(tmpdir, brand_slug, foundation)

                missing_resp = asyncio.run(
                    server.api_create_branch(
                        server.CreateBranchRequest(
                            label="Test Branch",
                            matrix_cells=matrix_cells,
                            selected_audience_segment="",
                            brand=brand_slug,
                        ),
                        brand=brand_slug,
                    )
                )
                self.assertIsInstance(missing_resp, JSONResponse)
                self.assertEqual(missing_resp.status_code, 400)
                self.assertIn("Select one Pillar 1 audience", missing_resp.body.decode("utf-8"))

                invalid_resp = asyncio.run(
                    server.api_create_branch(
                        server.CreateBranchRequest(
                            label="Test Branch",
                            matrix_cells=matrix_cells,
                            selected_audience_segment="unknown audience",
                            brand=brand_slug,
                        ),
                        brand=brand_slug,
                    )
                )
                self.assertIsInstance(invalid_resp, JSONResponse)
                self.assertEqual(invalid_resp.status_code, 400)
                self.assertIn("was not found in Pillar 1 segment_profiles", invalid_resp.body.decode("utf-8"))

                no_rows_resp = asyncio.run(
                    server.api_create_branch(
                        server.CreateBranchRequest(
                            label="Test Branch",
                            matrix_cells=matrix_cells,
                            selected_audience_segment="Evidence-Light Segment",
                            brand=brand_slug,
                        ),
                        brand=brand_slug,
                    )
                )
                self.assertIsInstance(no_rows_resp, JSONResponse)
                self.assertEqual(no_rows_resp.status_code, 400)
                self.assertIn("No LF8 rows passed evidence gates", no_rows_resp.body.decode("utf-8"))

                valid_resp = asyncio.run(
                    server.api_create_branch(
                        server.CreateBranchRequest(
                            label="Test Branch",
                            matrix_cells=matrix_cells,
                            selected_audience_segment="the optimization-obsessed knowledge worker (biohacker professional)",
                            brand=brand_slug,
                        ),
                        brand=brand_slug,
                    )
                )
                self.assertIsInstance(valid_resp, dict)
                self.assertEqual(
                    valid_resp.get("inputs", {}).get("selected_audience_segment"),
                    "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)",
                )

    def test_api_run_branch_forwards_selected_audience_into_inputs(self):
        brand_slug = "brand_x"
        selected_audience = "The Optimization-Obsessed Knowledge Worker (Biohacker Professional)"
        branch_id = "branch_1"
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)):
                _write_foundation_output(tmpdir, brand_slug, _foundation_fixture())
                server._save_branches(
                    brand_slug,
                    [
                        {
                            "id": branch_id,
                            "label": "Branch 1",
                            "inputs": {
                                "tof_count": 10,
                                "mof_count": 5,
                                "bof_count": 2,
                                "matrix_cells": [],
                                "selected_audience_segment": selected_audience,
                            },
                        }
                    ],
                )

                captured_inputs: dict = {}

                def _fake_create_task(coro):
                    frame = getattr(coro, "cr_frame", None)
                    if frame and isinstance(frame.f_locals, dict):
                        raw_inputs = frame.f_locals.get("inputs")
                        if isinstance(raw_inputs, dict):
                            captured_inputs.update(raw_inputs)
                    coro.close()
                    return object()

                with patch("server.asyncio.create_task", side_effect=_fake_create_task):
                    resp = asyncio.run(
                        server.api_run_branch(
                            branch_id,
                            server.RunBranchRequest(phases=[2], inputs={}, brand=brand_slug),
                            brand=brand_slug,
                        )
                    )

        self.assertEqual(resp.get("status"), "started")
        self.assertEqual(captured_inputs.get("selected_audience_segment"), selected_audience)

    def test_api_rebuild_foundation_pillar6_uses_step1_collectors(self):
        brand_slug = "brand_x"
        foundation = _foundation_fixture()
        foundation["pillar_2_voc_language_bank"]["quotes"] = [
            {
                "quote_id": f"q_stuck_{idx}",
                "quote": f"I feel stuck and foggy before deep work {idx}.",
                "category": "pain",
                "theme": "stagnation",
                "segment_name": "Burned-Out Startup Operator",
                "dominant_emotion": "Frustration / Pain",
                "source_type": "forum",
                "source_url": f"https://example.com/stuck/{idx}",
            }
            for idx in range(1, 5)
        ] + [
            {
                "quote_id": f"q_placebo_{idx}",
                "quote": f"Thirty milligrams feels like placebo fairy dust {idx}.",
                "category": "objection",
                "theme": "dosage skepticism",
                "segment_name": "Burned-Out Startup Operator",
                "dominant_emotion": "Skepticism / Distrust",
                "source_type": "review",
                "source_url": f"https://example.com/placebo/{idx}",
            }
            for idx in range(1, 4)
        ]
        foundation["pillar_6_emotional_driver_inventory"] = {
            "dominant_emotions": [
                {
                    "emotion": "Legacy Emotion Bucket",
                    "tagged_quote_count": 10,
                    "share_of_voc": 1.0,
                    "sample_quote_ids": ["legacy_q_1"],
                }
            ]
        }
        collectors_reports = [
            "\n".join(
                [
                    "# Collector A Report (Gemini Breadth)",
                    "## Pillar 6: Emotional Drivers & Objections",
                    "### Key Findings",
                    "- **Driver: Fear of Stagnation:** Users feel stuck and foggy before focus blocks.",
                    "- **Objection: \"Fairy Dusting\":** The dosage sounds placebo-level.",
                    "### Evidence Lines",
                    "- sample",
                    "## Pillar 7: Proof Assets",
                ]
            ),
            "\n".join(
                [
                    "# Collector B Report (Claude Precision)",
                    "## Pillar 6: Emotional Drivers & Objections",
                    "### Key Findings",
                    "- **Dominant emotions:** fear of stagnation, skepticism about placebo dosing.",
                    "- **Top objection pattern #1 — \"Fairy Dusting\":** buyers call the dosage ineffective.",
                    "### Evidence Lines",
                    "- sample",
                    "## Pillar 7: Proof Assets",
                ]
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)):
                _write_foundation_output(tmpdir, brand_slug, foundation)
                _write_collectors_snapshot(tmpdir, brand_slug, collectors_reports)
                resp = asyncio.run(server.api_rebuild_foundation_pillar6(brand=brand_slug))
                self.assertIsInstance(resp, dict)
                self.assertEqual(resp.get("status"), "ok")
                self.assertTrue(bool(resp.get("changed")))
                self.assertEqual(int(resp.get("emotion_count_after", 0)), 2)
                self.assertEqual(
                    resp.get("emotions_after"),
                    ["Fear of Stagnation", "Fairy Dusting"],
                )
                self.assertEqual(int(resp.get("collector_reports_used", 0)), 2)
                self.assertGreaterEqual(int(resp.get("lf8_segments_count", 0)), 1)
                self.assertGreaterEqual(int(resp.get("lf8_rows_total", 0)), 1)
                self.assertTrue(isinstance(resp.get("lf8_rows_by_segment_counts"), dict))

                saved = server._load_output("foundation_research", brand_slug=brand_slug)
                self.assertIsInstance(saved, dict)
                labels = [
                    row.get("emotion")
                    for row in saved.get("pillar_6_emotional_driver_inventory", {}).get("dominant_emotions", [])
                    if isinstance(row, dict)
                ]
                self.assertEqual(labels, ["Fear of Stagnation", "Fairy Dusting"])
                saved_p6 = saved.get("pillar_6_emotional_driver_inventory", {})
                self.assertEqual(saved_p6.get("lf8_mode"), "strict_lf8")
                self.assertTrue(isinstance(saved_p6.get("lf8_rows_by_segment"), dict))

    def test_api_rebuild_foundation_pillar6_requires_step1_reports(self):
        brand_slug = "brand_x"
        foundation = _foundation_fixture()
        foundation["pillar_2_voc_language_bank"]["quotes"] = [
            {
                "quote_id": "q_1",
                "quote": "I feel stuck by 2pm.",
                "category": "pain",
                "theme": "stagnation",
                "segment_name": "Burned-Out Startup Operator",
                "dominant_emotion": "Frustration / Pain",
                "source_type": "forum",
                "source_url": "https://example.com/q1",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(server.config, "OUTPUT_DIR", Path(tmpdir)):
                _write_foundation_output(tmpdir, brand_slug, foundation)
                resp = asyncio.run(server.api_rebuild_foundation_pillar6(brand=brand_slug))
                self.assertIsInstance(resp, JSONResponse)
                self.assertEqual(resp.status_code, 400)
                self.assertIn("No Step 1 collector reports found", resp.body.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
