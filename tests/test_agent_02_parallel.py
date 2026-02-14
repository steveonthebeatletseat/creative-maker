from __future__ import annotations

import unittest
from unittest.mock import patch

from agents.agent_02_idea_generator import Agent02IdeaGenerator
from schemas.idea_generator import CreativeEngineBrief, MarketingAngle, VideoConceptOption


def make_angle(stage: str, idx: int) -> MarketingAngle:
    concept = VideoConceptOption(
        concept_name=f"{stage.upper()} Concept {idx}",
        video_format="Direct-to-camera demo",
        scene_concept="A clear, filmable scene with quick proof.",
        why_this_format="Matches stage intent and lands proof quickly.",
        reference_examples="Example pattern",
        platform_targets=["tiktok"],
        sound_music_direction="Upbeat",
        proof_approach="demonstration",
        proof_description="Immediate before/after comparison",
    )
    return MarketingAngle(
        angle_id=f"{stage}_{idx:02d}",
        funnel_stage=stage,
        angle_name=f"{stage.upper()} angle {idx}",
        target_segment="Core segment",
        target_awareness="problem_aware",
        core_desire="Get a better result faster",
        emotional_lever="hope",
        voc_anchor="I just want this to work",
        white_space_link="Competitors miss this mechanism",
        mechanism_hint="Counterweight mechanism",
        objection_addressed="Will this actually fit?",
        video_concepts=[concept],
    )


class Agent02ParallelTests(unittest.TestCase):
    def setUp(self):
        self.agent = Agent02IdeaGenerator()

    def test_parallel_stage_mode_merges_in_stage_order(self):
        def fake_stage_run(stage, _stage_inputs, requested_count):
            return [make_angle(stage, i + 1) for i in range(requested_count)]

        with patch("agents.agent_02_idea_generator.config.CREATIVE_ENGINE_PARALLEL_BY_STAGE", True), patch(
            "agents.agent_02_idea_generator.config.CREATIVE_ENGINE_PARALLEL_MAX_WORKERS", 3
        ), patch.object(self.agent, "_run_stage_pipeline", side_effect=fake_stage_run), patch.object(
            self.agent, "_save_output", return_value=None
        ):
            result = self.agent.run(
                {
                    "brand_name": "Nord Labs Gaming",
                    "product_name": "Comfort Strap",
                    "batch_id": "batch_parallel",
                    "tof_count": 2,
                    "mof_count": 1,
                    "bof_count": 1,
                }
            )

        self.assertIsInstance(result, CreativeEngineBrief)
        self.assertEqual(len(result.angles), 4)
        stages = [getattr(a.funnel_stage, "value", str(a.funnel_stage)) for a in result.angles]
        self.assertEqual(stages, ["tof", "tof", "mof", "bof"])

    def test_single_stage_specialized_path_when_parallel_enabled(self):
        single_stage_result = [make_angle("tof", 1), make_angle("tof", 2), make_angle("tof", 3)]

        with patch("agents.agent_02_idea_generator.config.CREATIVE_ENGINE_PARALLEL_BY_STAGE", True), patch.object(
            self.agent, "_run_stage_pipeline", return_value=single_stage_result
        ) as stage_mock, patch.object(self.agent, "_run_monolithic_engine", side_effect=AssertionError("should not call monolithic")), patch.object(
            self.agent, "_save_output", return_value=None
        ):
            result = self.agent.run(
                {
                    "brand_name": "Nord Labs Gaming",
                    "product_name": "Comfort Strap",
                    "batch_id": "batch_tof_only",
                    "tof_count": 3,
                    "mof_count": 0,
                    "bof_count": 0,
                }
            )

        self.assertEqual(len(result.angles), 3)
        stage_mock.assert_called_once()
        args, _kwargs = stage_mock.call_args
        self.assertEqual(args[0], "tof")
        self.assertEqual(args[2], 3)

    def test_monolithic_path_when_parallel_disabled(self):
        monolithic = CreativeEngineBrief(
            brand_name="Nord Labs Gaming",
            product_name="Comfort Strap",
            generated_date="2026-02-14",
            batch_id="batch_monolithic",
            angles=[make_angle("tof", 1)],
        )

        with patch("agents.agent_02_idea_generator.config.CREATIVE_ENGINE_PARALLEL_BY_STAGE", False), patch.object(
            self.agent, "_run_monolithic_engine", return_value=monolithic
        ) as mono_mock, patch.object(self.agent, "_run_stage_pipeline", side_effect=AssertionError("should not call stage pipeline")), patch.object(
            self.agent, "_save_output", return_value=None
        ):
            result = self.agent.run(
                {
                    "brand_name": "Nord Labs Gaming",
                    "product_name": "Comfort Strap",
                    "batch_id": "batch_monolithic",
                    "tof_count": 2,
                    "mof_count": 1,
                    "bof_count": 1,
                }
            )

        self.assertIs(result, monolithic)
        mono_mock.assert_called_once()

    def test_run_errors_when_all_counts_zero(self):
        with patch("agents.agent_02_idea_generator.config.CREATIVE_ENGINE_PARALLEL_BY_STAGE", True), patch.object(
            self.agent, "_save_output", return_value=None
        ):
            with self.assertRaises(RuntimeError):
                self.agent.run(
                    {
                        "brand_name": "Nord Labs Gaming",
                        "product_name": "Comfort Strap",
                        "tof_count": 0,
                        "mof_count": 0,
                        "bof_count": 0,
                    }
                )


if __name__ == "__main__":
    unittest.main()
