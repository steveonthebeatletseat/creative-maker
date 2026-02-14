from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline.prompt_lab_agent02 import (
    InputDataError,
    LLMCallExecutionError,
    PromptLabSettings,
    create_run_directory,
    load_step1_output,
    load_step2_output,
    run_chain,
    run_step1,
    run_step2,
    write_run_artifacts,
)
from schemas.idea_generator import (
    AngleResearch,
    CreativeEngineBrief,
    CreativeScoutReport,
    FormatRecommendation,
    MarketingAngle,
    MarketingAngleStep1,
    ScoutCitation,
    ScoutEvidence,
    Step1Output,
    VideoConceptOption,
)


def fixture_step1_output() -> Step1Output:
    return Step1Output(
        brand_name="Test Brand",
        product_name="Test Product",
        angles=[
            MarketingAngleStep1(
                angle_id="tof_01",
                funnel_stage="tof",
                angle_name="Hook the unaware",
                target_segment="New users",
                target_awareness="problem_aware",
                core_desire="Get results quickly",
                emotional_lever="hope",
                voc_anchor="I just want something that works",
                white_space_link="Competitors are too generic",
                mechanism_hint="Counterweight comfort mechanism",
                objection_addressed="Will this fit my headset?",
            )
        ],
    )


def fixture_step2_output() -> CreativeScoutReport:
    citation = ScoutCitation(
        source_url="https://example.com/ad1",
        source_title="Ad Example",
        publisher="Example",
        source_date="2026-02-14",
        relevance_note="Shows conversion-focused direct response format",
    )
    evidence = ScoutEvidence(claim="Format is converting", confidence=0.8, citations=[citation])
    rec1 = FormatRecommendation(
        video_format="Direct-to-camera demo",
        platform_targets=["tiktok"],
        why_fit="Matches proof-heavy angle",
        style_notes="Fast cut pacing",
        evidence=[evidence],
        watchouts=[],
    )
    rec2 = FormatRecommendation(
        video_format="Mechanism explainer",
        platform_targets=["meta_reels"],
        why_fit="Clarifies unique mechanism",
        style_notes="Overlay text and zoom-ins",
        evidence=[evidence],
        watchouts=[],
    )
    return CreativeScoutReport(
        brand_name="Test Brand",
        product_name="Test Product",
        generated_date="2026-02-14",
        angle_research=[
            AngleResearch(
                angle_id="tof_01",
                angle_name="Hook the unaware",
                recommended_formats=[rec1, rec2],
                trend_signals=["Short-form proof-first creative"],
                source_count=1,
            )
        ],
        global_insights=["Proof moments early perform better"],
    )


def fixture_step3_output() -> CreativeEngineBrief:
    concept = VideoConceptOption(
        concept_name="Proof Hook",
        video_format="Direct-to-camera demo",
        scene_concept="Creator opens with discomfort pain, then demonstrates instant fit.",
        why_this_format="Pairs emotional pain with quick proof",
        reference_examples="Example ad references",
        platform_targets=["tiktok"],
        sound_music_direction="Punchy percussive beat",
        proof_approach="demonstration",
        proof_description="Side-by-side comfort comparison",
    )
    angle = MarketingAngle(
        angle_id="tof_01",
        funnel_stage="tof",
        angle_name="Hook the unaware",
        target_segment="New users",
        target_awareness="problem_aware",
        core_desire="Get results quickly",
        emotional_lever="hope",
        voc_anchor="I just want something that works",
        white_space_link="Competitors are too generic",
        mechanism_hint="Counterweight comfort mechanism",
        objection_addressed="Will this fit my headset?",
        video_concepts=[concept],
    )
    return CreativeEngineBrief(
        brand_name="Test Brand",
        product_name="Test Product",
        generated_date="2026-02-14",
        batch_id="batch_test",
        angles=[angle],
    )


class PromptLabTests(unittest.TestCase):
    def test_cli_help_lists_subcommands(self):
        result = subprocess.run(
            [sys.executable, "scripts/prompt_lab_agent02.py", "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("step1", result.stdout)
        self.assertIn("step2", result.stdout)
        self.assertIn("step3", result.stdout)
        self.assertIn("chain", result.stdout)

    def test_cli_arg_validation_for_missing_required_flag(self):
        result = subprocess.run(
            [sys.executable, "scripts/prompt_lab_agent02.py", "step1"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)

    def test_step1_requires_foundation_brief(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = PromptLabSettings(
                out_root=Path(tmp) / "out",
                prompt_dir=Path("prompts/prompt_lab"),
            )
            _, _, run_dir = create_run_directory(settings)
            with self.assertRaises(InputDataError):
                run_step1(input_data={"brand_name": "X"}, run_dir=run_dir, settings=settings)

    def test_step1_uses_prompt_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            prompt_dir = Path(tmp) / "prompts"
            prompt_dir.mkdir(parents=True, exist_ok=True)
            override = "OVERRIDE STEP1 PROMPT"
            (prompt_dir / "agent_02_step1_system.md").write_text(override, encoding="utf-8")

            settings = PromptLabSettings(out_root=Path(tmp) / "out", prompt_dir=prompt_dir)
            _, _, run_dir = create_run_directory(settings)

            with patch("pipeline.prompt_lab_agent02.call_llm_structured", return_value=fixture_step1_output()):
                execution, _ = run_step1(
                    input_data={
                        "brand_name": "Test Brand",
                        "product_name": "Test Product",
                        "foundation_brief": {"segments": []},
                    },
                    run_dir=run_dir,
                    settings=settings,
                )

            system_prompt_path = execution.artifacts["system_prompt"]
            self.assertEqual(system_prompt_path.read_text(encoding="utf-8"), override)

    def test_schema_loading_valid_and_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            step1_path = tmp_path / "step1.json"
            step2_path = tmp_path / "step2.json"
            invalid_path = tmp_path / "invalid.json"

            step1_path.write_text(fixture_step1_output().model_dump_json(indent=2), encoding="utf-8")
            step2_path.write_text(fixture_step2_output().model_dump_json(indent=2), encoding="utf-8")
            invalid_path.write_text("{}", encoding="utf-8")

            self.assertEqual(load_step1_output(step1_path).brand_name, "Test Brand")
            self.assertEqual(load_step2_output(step2_path).brand_name, "Test Brand")

            with self.assertRaises(Exception):
                load_step1_output(invalid_path)
            with self.assertRaises(Exception):
                load_step2_output(invalid_path)

    def test_step2_strict_mode_fails_without_anthropic_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = PromptLabSettings(
                out_root=Path(tmp) / "out",
                prompt_dir=Path("prompts/prompt_lab"),
                strict_sdk_only=True,
            )
            _, _, run_dir = create_run_directory(settings)

            with patch("pipeline.prompt_lab_agent02.config.ANTHROPIC_API_KEY", ""):
                with self.assertRaises(LLMCallExecutionError):
                    run_step2(
                        step1_output=fixture_step1_output(),
                        run_dir=run_dir,
                        settings=settings,
                    )

    def test_chain_generates_manifest_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = PromptLabSettings(
                out_root=Path(tmp) / "out",
                prompt_dir=Path("prompts/prompt_lab"),
                strict_sdk_only=True,
            )
            run_id, created_at, run_dir = create_run_directory(settings)

            def structured_side_effect(*_, response_model=None, **__):
                if response_model is Step1Output:
                    return fixture_step1_output()
                if response_model is CreativeEngineBrief:
                    return fixture_step3_output()
                raise AssertionError("Unexpected response model")

            with patch("pipeline.prompt_lab_agent02.call_llm_structured", side_effect=structured_side_effect):
                with patch("pipeline.prompt_lab_agent02.call_claude_agent_structured", return_value=fixture_step2_output()):
                    with patch("pipeline.prompt_lab_agent02.config.ANTHROPIC_API_KEY", "test-key"):
                        run = run_chain(
                            input_data={
                                "brand_name": "Test Brand",
                                "product_name": "Test Product",
                                "foundation_brief": {"segments": []},
                                "batch_id": "batch_test",
                            },
                            run_dir=run_dir,
                            settings=settings,
                        )

            run.run_id = run_id
            run.created_at = created_at
            run.run_dir = run_dir
            files = write_run_artifacts(
                run=run,
                inputs_snapshot={"brand_name": "Test Brand"},
            )

            self.assertTrue(files["manifest"].exists())
            self.assertTrue(files["summary"].exists())
            self.assertTrue(files["inputs_snapshot"].exists())

            manifest = json.loads(files["manifest"].read_text(encoding="utf-8"))
            self.assertEqual(manifest["step_status"]["step1"], "completed")
            self.assertEqual(manifest["step_status"]["step2"], "completed")
            self.assertEqual(manifest["step_status"]["step3"], "completed")


if __name__ == "__main__":
    unittest.main()
