from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import server
from pipeline import storage as storage_mod
from pipeline.phase4_video_providers import MockFalVideoProvider, MockGeminiImageEditProvider, MockTTSProvider, MockVisionSceneProvider


class _VisionWinnerProvider:
    def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
        _ = (model_id, idempotency_key)
        return {
            "caption": image_path.stem,
            "subjects": [image_path.stem],
            "actions": [],
            "setting": "office",
            "camera_angle": "eye_level",
            "shot_type": "medium",
            "lighting": "soft",
            "mood": "focused",
            "product_visibility": "none",
            "text_present": False,
            "style_tags": [image_path.stem],
            "attention_hooks": [],
            "quality_issues": [],
        }

    def score_scene_match(self, *, image_path: Path, scene_intent: dict, style_profile: dict, model_id: str, idempotency_key: str):
        _ = (scene_intent, style_profile, model_id, idempotency_key)
        score = 9 if "winner" in image_path.name else 3
        return {
            "score_1_to_10": score,
            "reason_short": f"score={score}",
            "fit_subject": score,
            "fit_action": score,
            "fit_emotion": score,
            "fit_composition": score,
            "consistency_with_style_profile": score,
            "edit_recommended": score <= 5,
        }


class _VisionEditProvider:
    def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
        return MockVisionSceneProvider().analyze_image(
            image_path=image_path,
            model_id=model_id,
            idempotency_key=idempotency_key,
        )

    def score_scene_match(self, *, image_path: Path, scene_intent: dict, style_profile: dict, model_id: str, idempotency_key: str):
        _ = (scene_intent, style_profile, model_id, idempotency_key)
        transformed = "__storyboard__" in image_path.name
        score = 7 if transformed else 4
        return {
            "score_1_to_10": score,
            "reason_short": f"score={score}",
            "fit_subject": score,
            "fit_action": score,
            "fit_emotion": score,
            "fit_composition": score,
            "consistency_with_style_profile": score,
            "edit_recommended": score <= 5,
        }


class _VisionLowConfidenceProvider:
    def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
        return MockVisionSceneProvider().analyze_image(
            image_path=image_path,
            model_id=model_id,
            idempotency_key=idempotency_key,
        )

    def score_scene_match(self, *, image_path: Path, scene_intent: dict, style_profile: dict, model_id: str, idempotency_key: str):
        _ = (scene_intent, style_profile, model_id, idempotency_key)
        return {
            "score_1_to_10": 4,
            "reason_short": "low confidence",
            "fit_subject": 4,
            "fit_action": 4,
            "fit_emotion": 4,
            "fit_composition": 4,
            "consistency_with_style_profile": 4,
            "edit_recommended": True,
        }


class _VisionFlatProvider:
    def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
        return MockVisionSceneProvider().analyze_image(
            image_path=image_path,
            model_id=model_id,
            idempotency_key=idempotency_key,
        )

    def score_scene_match(self, *, image_path: Path, scene_intent: dict, style_profile: dict, model_id: str, idempotency_key: str):
        _ = (image_path, scene_intent, style_profile, model_id, idempotency_key)
        return {
            "score_1_to_10": 8,
            "reason_short": "flat score",
            "fit_subject": 8,
            "fit_action": 8,
            "fit_emotion": 8,
            "fit_composition": 8,
            "consistency_with_style_profile": 8,
            "edit_recommended": False,
        }


class _VisionDupAwareProvider:
    def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
        return MockVisionSceneProvider().analyze_image(
            image_path=image_path,
            model_id=model_id,
            idempotency_key=idempotency_key,
        )

    def score_scene_match(self, *, image_path: Path, scene_intent: dict, style_profile: dict, model_id: str, idempotency_key: str):
        _ = (image_path, scene_intent, style_profile, model_id, idempotency_key)
        return {
            "score_1_to_10": 6,
            "reason_short": "duplicate-source test",
            "fit_subject": 6,
            "fit_action": 6,
            "fit_emotion": 6,
            "fit_composition": 6,
            "consistency_with_style_profile": 6,
            "edit_recommended": False,
        }


class _VisionProfileBiasProvider:
    def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
        return MockVisionSceneProvider().analyze_image(
            image_path=image_path,
            model_id=model_id,
            idempotency_key=idempotency_key,
        )

    def score_scene_match(self, *, image_path: Path, scene_intent: dict, style_profile: dict, model_id: str, idempotency_key: str):
        _ = (scene_intent, style_profile, model_id, idempotency_key)
        score = 9 if "profile" in image_path.name else 3
        return {
            "score_1_to_10": score,
            "reason_short": f"profile_pool={score}",
            "fit_subject": score,
            "fit_action": score,
            "fit_emotion": score,
            "fit_composition": score,
            "consistency_with_style_profile": score,
            "edit_recommended": score <= 5,
        }


class _VisionCountingProvider:
    def __init__(self):
        self.score_call_count = 0

    def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
        return MockVisionSceneProvider().analyze_image(
            image_path=image_path,
            model_id=model_id,
            idempotency_key=idempotency_key,
        )

    def score_scene_match(self, *, image_path: Path, scene_intent: dict, style_profile: dict, model_id: str, idempotency_key: str):
        _ = (image_path, scene_intent, style_profile, model_id, idempotency_key)
        self.score_call_count += 1
        return {
            "score_1_to_10": 8,
            "reason_short": "counting provider",
            "fit_subject": 8,
            "fit_action": 8,
            "fit_emotion": 8,
            "fit_composition": 8,
            "consistency_with_style_profile": 8,
            "edit_recommended": False,
        }


class StoryboardAssignmentEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.root = Path(self.tmpdir.name)

        self._old_db_path = storage_mod.DB_PATH
        storage_mod.reset_storage_connection_for_tests()
        storage_mod.DB_PATH = self.root / "creative_maker_test.db"
        storage_mod.init_db()

        server.pipeline_state["running"] = False
        server.pipeline_state["active_brand_slug"] = "brand_x"
        server.phase4_v1_generation_tasks.clear()
        server.phase4_v1_storyboard_assign_tasks.clear()
        server.phase4_v1_storyboard_assign_state.clear()

        self.output_dir = self.root / "outputs"
        self._output_patcher = patch.object(server.config, "OUTPUT_DIR", self.output_dir)
        self._output_patcher.start()
        self.addCleanup(self._output_patcher.stop)
        self._vision_provider_patcher = patch.object(
            server,
            "build_vision_scene_provider",
            return_value=MockVisionSceneProvider(),
        )
        self._vision_provider_patcher.start()
        self.addCleanup(self._vision_provider_patcher.stop)

    def tearDown(self):
        storage_mod.reset_storage_connection_for_tests()
        storage_mod.DB_PATH = self._old_db_path

    def _seed_phase3_run(
        self,
        brand_slug: str,
        branch_id: str,
        phase3_run_id: str,
        *,
        line_modes: tuple[str, str] = ("b_roll", "b_roll"),
    ):
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
                    "run_id": phase3_run_id,
                    "status": "completed",
                    "created_at": "2026-02-18T10:00:00",
                    "arms": ["claude_sdk"],
                    "reviewer_role": "client_founder",
                }
            ],
        )
        run_dir = server._phase3_v2_run_dir(brand_slug, branch_id, phase3_run_id)
        server._phase3_v2_write_json(run_dir / "brief_units.json", [{"brief_unit_id": "bu_001"}])
        server._phase3_v2_write_json(run_dir / "evidence_packs.json", [])
        server._phase3_v2_write_json(run_dir / "reviews.json", [])
        server._phase3_v2_write_json(run_dir / "summary.json", {})
        server._phase3_v2_write_json(run_dir / "decisions.json", [])
        server._phase3_v2_write_json(run_dir / "chat_threads.json", {})
        server._phase3_v2_write_json(run_dir / "hook_chat_threads.json", {})
        server._phase3_v2_write_json(run_dir / "scene_chat_threads.json", {})
        server._phase3_v2_write_json(
            run_dir / "final_lock.json",
            {
                "run_id": phase3_run_id,
                "locked": True,
                "locked_at": "2026-02-18T10:00:00",
                "locked_by_role": "client_founder",
            },
        )
        server._phase3_v2_write_json(run_dir / "hook_selections.json", [])
        server._phase3_v2_write_json(run_dir / "scene_handoff_packet.json", {})
        server._phase3_v2_write_json(
            run_dir / "hook_stage_manifest.json",
            {
                "run_id": phase3_run_id,
                "hook_run_id": "hk_run_1",
                "status": "completed",
                "created_at": "",
                "started_at": "",
                "completed_at": "",
                "error": "",
                "eligible_count": 1,
                "processed_count": 1,
                "failed_count": 0,
                "skipped_count": 0,
                "candidate_target_per_unit": 20,
                "final_variants_per_unit": 5,
                "max_parallel": 1,
                "max_repair_rounds": 1,
                "model_registry": {},
                "metrics": {},
            },
        )
        server._phase3_v2_write_json(
            run_dir / "scene_stage_manifest.json",
            {
                "run_id": phase3_run_id,
                "scene_run_id": "scene_run_1",
                "status": "completed",
                "created_at": "",
                "started_at": "",
                "completed_at": "",
                "error": "",
                "eligible_count": 1,
                "processed_count": 1,
                "failed_count": 0,
                "skipped_count": 0,
                "stale_count": 0,
                "max_parallel": 1,
                "max_repair_rounds": 1,
                "max_consecutive_mode": 3,
                "min_a_roll_lines": 1,
                "model_registry": {},
                "metrics": {},
            },
        )
        server._phase3_v2_write_json(
            run_dir / "arm_claude_sdk_core_scripts.json",
            [
                {
                    "brief_unit_id": "bu_001",
                    "lines": [
                        {"line_id": "L01", "text": "Scene one."},
                        {"line_id": "L02", "text": "Scene two."},
                    ],
                }
            ],
        )
        server._phase3_v2_write_json(
            run_dir / "production_handoff_packet.json",
            {
                "run_id": phase3_run_id,
                "scene_run_id": "scene_run_1",
                "ready": True,
                "ready_count": 1,
                "total_required": 1,
                "generated_at": "2026-02-18T10:01:00",
                "items": [
                    {
                        "scene_unit_id": "su_001",
                        "scene_plan_id": "sp_001",
                        "run_id": phase3_run_id,
                        "brief_unit_id": "bu_001",
                        "arm": "claude_sdk",
                        "hook_id": "hk_001",
                        "status": "ready",
                        "stale": False,
                        "stale_reason": "",
                        "lines": [
                            {
                                "scene_line_id": "sl_001",
                                "script_line_id": "L01",
                                "mode": str(line_modes[0] if len(line_modes) > 0 else "b_roll"),
                                "narration_line": "Scene one.",
                                "scene_description": "Show desk action.",
                                "duration_seconds": 2.5,
                                "on_screen_text": "",
                            },
                            {
                                "scene_line_id": "sl_002",
                                "script_line_id": "L02",
                                "mode": str(line_modes[1] if len(line_modes) > 1 else "b_roll"),
                                "narration_line": "Scene two.",
                                "scene_description": "Show follow-up action.",
                                "duration_seconds": 2.5,
                                "on_screen_text": "",
                            },
                        ],
                    }
                ],
            },
        )

    def _bootstrap(self, branch_id: str, brand_slug: str, phase3_run_id: str) -> str:
        payload = asyncio.run(
            server.api_phase4_v1_storyboard_bootstrap(
                branch_id,
                server.StoryboardBootstrapRequestV1(brand=brand_slug, phase3_run_id=phase3_run_id),
            )
        )
        return str(payload["video_run_id"])

    @staticmethod
    def _analysis_for_file(file_name: str) -> dict:
        stem = Path(str(file_name or "")).stem or "image"
        return {
            "caption": stem,
            "subjects": [stem],
            "actions": [],
            "setting": "office",
            "camera_angle": "eye_level",
            "shot_type": "medium",
            "lighting": "soft",
            "mood": "focused",
            "product_visibility": "none",
            "text_present": False,
            "style_tags": [stem],
            "attention_hooks": [],
            "quality_issues": [],
            "provider": "mock_vision_scene",
            "model_id": "gpt-5.2",
        }

    def _seed_ready_library_files(
        self,
        *,
        brand_slug: str,
        branch_id: str,
        files: list[dict],
    ) -> Path:
        library_dir = server._phase4_v1_broll_library_dir(brand_slug, branch_id)
        rows: list[dict] = []
        for idx, spec in enumerate(files):
            file_name = str(spec.get("file_name") or "").strip()
            if not file_name:
                continue
            content = spec.get("content")
            if isinstance(content, bytes):
                payload = content
            else:
                payload = str(content if content is not None else f"img-{idx}").encode("utf-8")
            file_path = library_dir / file_name
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_bytes(payload)
            checksum = hashlib.sha256(payload).hexdigest()
            mode_hint = str(spec.get("mode_hint") or "b_roll")
            usage_count = int(spec.get("usage_count") or 0)
            ai_generated = bool(spec.get("ai_generated"))
            analysis = spec.get("analysis")
            if not isinstance(analysis, dict):
                analysis = self._analysis_for_file(file_name)
            metadata = {
                "library_item_type": "ai_generated" if ai_generated else "original_upload",
                "ai_generated": ai_generated,
                "mode_hint": mode_hint,
                "usage_count": usage_count,
                "tags": list(spec.get("tags") or []),
                "source_checksum_sha256": checksum,
                "indexing_status": str(spec.get("indexing_status") or "ready"),
                "indexing_error": str(spec.get("indexing_error") or ""),
                "indexed_at": "2026-02-20T00:00:00",
                "indexing_provider": "mock_vision_scene",
                "indexing_model_id": "gpt-5.2",
                "indexing_input_checksum": checksum,
                "analysis": analysis,
            }
            rows.append(
                {
                    "file_name": file_name,
                    "size_bytes": len(payload),
                    "added_at": f"2026-02-20T00:00:{idx:02d}",
                    "metadata": metadata,
                }
            )
        server._phase4_v1_save_broll_library(brand_slug, branch_id, rows)
        return library_dir

    def test_metadata_and_supported_file_filtering(self):
        provider = MockVisionSceneProvider()
        img_path = self.root / "focus_desk.png"
        img_path.write_bytes(b"img")
        analysis = provider.analyze_image(image_path=img_path, model_id="mock", idempotency_key="k")
        self.assertIn("caption", analysis)
        self.assertIn("style_tags", analysis)

        folder = self.root / "bank"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "a.png").write_bytes(b"a")
        (folder / "b.jpg").write_bytes(b"b")
        (folder / "notes.txt").write_bytes(b"n")
        supported = server._phase4_v1_storyboard_candidate_files(folder)
        names = sorted([p.name for p in supported])
        self.assertEqual(names, ["a.png", "b.jpg"])

    def test_candidate_selection_prefers_highest_score(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[
                    {"file_name": "loser.png", "content": b"x", "mode_hint": "b_roll"},
                    {"file_name": "winner.png", "content": b"y", "mode_hint": "b_roll"},
                ],
            )

            with patch.object(server, "build_vision_scene_provider", return_value=_VisionWinnerProvider()), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=1,
                        low_flag_threshold=6,
                        job_id="job_test",
                    )
                )

            status = server._phase4_v1_storyboard_load_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
            )
            scene = status["by_scene_line_id"]["sl_001"]
            self.assertEqual(scene["assignment_status"], "assigned")
            self.assertEqual(scene["source_image_filename"], "winner.png")

    def test_edit_path_and_low_confidence_flags(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[{"file_name": "candidate.png", "content": b"z", "mode_hint": "b_roll"}],
            )

            with patch.object(server, "build_vision_scene_provider", return_value=_VisionLowConfidenceProvider()), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=5,
                        low_flag_threshold=6,
                        job_id="job_low_conf",
                    )
                )

            status = server._phase4_v1_storyboard_load_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
            )
            scene = status["by_scene_line_id"]["sl_001"]
            self.assertEqual(scene["assignment_status"], "assigned_needs_review")
            self.assertTrue(bool(scene["low_confidence"]))

            assets = server.list_video_assets(video_run_id)
            transformed = [a for a in assets if str(a.get("asset_type")) == "transformed_frame"]
            self.assertGreater(len(transformed), 0)

    def test_prevents_back_to_back_same_source_image(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[
                    {"file_name": "a.png", "content": b"a", "mode_hint": "b_roll"},
                    {"file_name": "b.png", "content": b"b", "mode_hint": "b_roll"},
                ],
            )

            with patch.object(server, "build_vision_scene_provider", return_value=_VisionFlatProvider()), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=1,
                        low_flag_threshold=6,
                        job_id="job_no_repeat",
                    )
                )

            status = server._phase4_v1_storyboard_load_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
            )
            scenes = status.get("by_scene_line_id") or {}
            first = scenes.get("sl_001") or {}
            second = scenes.get("sl_002") or {}
            self.assertTrue(str(first.get("source_image_filename") or ""))
            self.assertTrue(str(second.get("source_image_filename") or ""))
            self.assertNotEqual(first.get("source_image_filename"), second.get("source_image_filename"))

    def test_duplicate_source_images_trigger_edit_variation(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[
                    {"file_name": "dup_a.png", "content": b"same-image", "mode_hint": "b_roll"},
                    {"file_name": "dup_b.png", "content": b"same-image", "mode_hint": "b_roll"},
                ],
            )

            with patch.object(server, "build_vision_scene_provider", return_value=_VisionDupAwareProvider()), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=5,
                        low_flag_threshold=6,
                        job_id="job_dup_variation",
                    )
                )

            status = server._phase4_v1_storyboard_load_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
            )
            scenes = status.get("by_scene_line_id") or {}
            first = scenes.get("sl_001") or {}
            second = scenes.get("sl_002") or {}
            self.assertTrue(bool(first.get("edited")))
            self.assertTrue(bool(second.get("edited")))

    def test_a_roll_uses_selected_a_roll_library_pool(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(
            brand_slug,
            branch_id,
            phase3_run_id,
            line_modes=("a_roll", "b_roll"),
        )

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[
                    {"file_name": "general_scene.png", "content": b"general", "mode_hint": "b_roll"},
                    {"file_name": "general_alt.png", "content": b"general-2", "mode_hint": "b_roll"},
                    {"file_name": "profile_face.png", "content": b"profile-face", "mode_hint": "a_roll"},
                ],
            )

            with patch.object(server, "build_vision_scene_provider", return_value=_VisionProfileBiasProvider()), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=1,
                        low_flag_threshold=6,
                        selected_a_roll_files=["profile_face.png"],
                        selected_b_roll_files=["general_scene.png", "general_alt.png"],
                        job_id="job_profile_pool",
                    )
                )

            status = server._phase4_v1_storyboard_load_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
            )
            scenes = status.get("by_scene_line_id") or {}
            first = scenes.get("sl_001") or {}
            self.assertEqual(str(first.get("mode") or ""), "a_roll")
            self.assertEqual(str(first.get("source_image_filename") or ""), "profile_face.png")

    def test_assigned_autosaves_ai_generated_library_rows(self):
        brand_slug = "brand_x"
        branch_id = "branch_autosave"
        phase3_run_id = "p3v2_seed_autosave"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[
                    {"file_name": "winner.png", "content": b"winner", "mode_hint": "b_roll"},
                    {"file_name": "loser.png", "content": b"loser", "mode_hint": "b_roll"},
                ],
            )

            with patch.object(server, "build_vision_scene_provider", return_value=_VisionWinnerProvider()), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=1,
                        low_flag_threshold=1,
                        job_id="job_autosave",
                    )
                )

            library_rows = server._phase4_v1_load_broll_library(brand_slug, branch_id)
            ai_rows = []
            for row in library_rows:
                metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
                if bool(metadata.get("ai_generated")):
                    ai_rows.append(row)
            self.assertGreater(len(ai_rows), 0)
            sample = ai_rows[0]
            sample_meta = sample.get("metadata") if isinstance(sample.get("metadata"), dict) else {}
            self.assertEqual(str(sample_meta.get("library_item_type") or ""), "ai_generated")
            self.assertEqual(str(sample_meta.get("originating_video_run_id") or ""), video_run_id)
            self.assertEqual(str(sample_meta.get("assignment_status") or ""), "assigned")

    def test_assigned_needs_review_does_not_autosave(self):
        brand_slug = "brand_x"
        branch_id = "branch_no_autosave"
        phase3_run_id = "p3v2_seed_no_autosave"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[{"file_name": "candidate.png", "content": b"candidate", "mode_hint": "b_roll"}],
            )
            before_rows = server._phase4_v1_load_broll_library(brand_slug, branch_id)
            before_ai_count = sum(
                1
                for row in before_rows
                if bool(((row.get("metadata") or {}) if isinstance(row, dict) else {}).get("ai_generated"))
            )

            with patch.object(server, "build_vision_scene_provider", return_value=_VisionLowConfidenceProvider()), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=5,
                        low_flag_threshold=6,
                        job_id="job_no_autosave",
                    )
                )

            library_rows = server._phase4_v1_load_broll_library(brand_slug, branch_id)
            after_ai_count = sum(
                1
                for row in library_rows
                if bool(((row.get("metadata") or {}) if isinstance(row, dict) else {}).get("ai_generated"))
            )
            self.assertEqual(after_ai_count, before_ai_count)

    def test_checksum_dedupe_updates_usage_without_duplicates(self):
        brand_slug = "brand_x"
        branch_id = "branch_dedupe"
        server._save_branches(
            brand_slug,
            [
                {
                    "id": branch_id,
                    "label": branch_id,
                    "status": "ready",
                    "available_agents": ["creative_engine"],
                    "completed_agents": ["creative_engine"],
                    "failed_agents": [],
                    "inputs": {},
                }
            ],
        )

        source = self.root / "dedupe_source.png"
        source.write_bytes(b"same-bytes")
        rows: list[dict] = []
        row_index: dict[str, int] = {}
        checksum_index: dict[str, str] = {}
        meta = server._phase4_v1_storyboard_ai_library_metadata(
            mode_hint="b_roll",
            source_pool="image_bank",
            source_image_asset_id="asset_src",
            source_image_filename="dedupe_source.png",
            originating_video_run_id="run_x",
            originating_scene_line_id="sl_x",
            originating_clip_id="clip_x",
            assignment_score=8,
            assignment_status="assigned",
            prompt_model_provider="anthropic",
            prompt_model_id="claude-opus-4-6",
            prompt_model_label="Claude Opus 4.6",
            image_edit_model_id="mock-image",
            image_edit_model_label="Mock Image",
            edit_provider="mock",
            edit_prompt="prompt",
        )

        first = server._phase4_v1_broll_upsert_from_source(
            brand_slug=brand_slug,
            branch_id=branch_id,
            source_path=source,
            preferred_file_name="first.png",
            metadata_updates=meta,
            rows=rows,
            row_index=row_index,
            checksum_index=checksum_index,
            increment_usage_count=True,
        )
        rows = first["rows"]
        second = server._phase4_v1_broll_upsert_from_source(
            brand_slug=brand_slug,
            branch_id=branch_id,
            source_path=source,
            preferred_file_name="second.png",
            metadata_updates=meta,
            rows=rows,
            row_index=row_index,
            checksum_index=checksum_index,
            increment_usage_count=True,
        )
        rows = second["rows"]
        self.assertEqual(len(rows), 1)
        self.assertTrue(bool(second.get("dedup_hit")))
        only_meta = rows[0].get("metadata") if isinstance(rows[0].get("metadata"), dict) else {}
        self.assertEqual(int(only_meta.get("usage_count") or 0), 2)

    def test_balanced_reuse_prefers_lower_usage_when_scores_tied(self):
        brand_slug = "brand_x"
        branch_id = "branch_balanced"
        phase3_run_id = "p3v2_seed_balanced"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[
                    {"file_name": "high_usage.png", "content": b"high", "mode_hint": "b_roll", "usage_count": 25},
                    {
                        "file_name": "low_usage.png",
                        "content": b"low",
                        "mode_hint": "b_roll",
                        "usage_count": 1,
                        "ai_generated": True,
                    },
                ],
            )

            with patch.object(server, "build_vision_scene_provider", return_value=_VisionFlatProvider()), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=1,
                        low_flag_threshold=1,
                        job_id="job_balanced",
                    )
                )

            status = server._phase4_v1_storyboard_load_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
            )
            first_scene = (status.get("by_scene_line_id") or {}).get("sl_001") or {}
            self.assertEqual(str(first_scene.get("source_image_filename") or ""), "low_usage.png")

    def test_scoring_uses_shortlist_cap_before_assignment(self):
        brand_slug = "brand_x"
        branch_id = "branch_shortlist_cap"
        phase3_run_id = "p3v2_seed_shortlist_cap"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.dict(
            os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False
        ):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self._seed_ready_library_files(
                brand_slug=brand_slug,
                branch_id=branch_id,
                files=[
                    {
                        "file_name": f"candidate_{idx:02d}.png",
                        "content": f"img-{idx}".encode("utf-8"),
                        "mode_hint": "b_roll",
                    }
                    for idx in range(30)
                ],
            )

            counting_provider = _VisionCountingProvider()
            with patch.object(server, "build_vision_scene_provider", return_value=counting_provider), patch.object(
                server, "build_generation_providers", return_value=(MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider())
            ):
                asyncio.run(
                    server._phase4_v1_execute_storyboard_assignment(
                        brand_slug=brand_slug,
                        branch_id=branch_id,
                        video_run_id=video_run_id,
                        folder_url="",
                        edit_threshold=1,
                        low_flag_threshold=1,
                        job_id="job_shortlist_cap",
                    )
                )

            # Two storyboard scenes are seeded by _seed_phase3_run.
            # Each scene should score at most the shortlist cap plus one transformed-score check.
            expected_max_calls = (server._PHASE4_STORYBOARD_SHORTLIST_SIZE + 1) * 2
            self.assertGreater(counting_provider.score_call_count, 0)
            self.assertLessEqual(counting_provider.score_call_count, expected_max_calls)

    def test_backfill_latest_run_only_is_idempotent(self):
        brand_slug = "brand_x"
        branch_id = "branch_backfill"
        phase3_run_id = "p3v2_seed_backfill"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True):
            video_run_id = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            clips = server.list_video_clips(video_run_id)
            self.assertTrue(clips)
            clip = clips[0]
            clip_id = str(clip.get("clip_id") or "")
            scene_line_id = str(clip.get("scene_line_id") or "")
            mode = str(clip.get("mode") or "b_roll")
            start_frame_name = "backfill_frame.png"
            start_frame_path = server._phase4_v1_assets_root(brand_slug, branch_id, video_run_id) / "start_frames" / start_frame_name
            start_frame_path.parent.mkdir(parents=True, exist_ok=True)
            start_frame_path.write_bytes(b"backfill-frame")
            server.create_video_asset(
                asset_id=f"asset_{video_run_id}_backfill",
                video_run_id=video_run_id,
                clip_id=clip_id,
                asset_type="start_frame",
                storage_path=str(start_frame_path),
                source_url=str(start_frame_path),
                file_name=start_frame_name,
                mime_type="image/png",
                byte_size=int(start_frame_path.stat().st_size),
                checksum_sha256=server.hashlib.sha256(start_frame_path.read_bytes()).hexdigest(),
                metadata={"assignment_stage": "storyboard"},
            )
            status_payload = server._phase4_v1_storyboard_build_initial_status(
                video_run_id=video_run_id,
                clips=clips,
            )
            status_row = ((status_payload.get("by_scene_line_id") or {}).get(scene_line_id) or {})
            status_row.update(
                {
                    "clip_id": clip_id,
                    "mode": mode,
                    "scene_line_id": scene_line_id,
                    "assignment_status": "assigned",
                    "assignment_score": 8,
                    "start_frame_filename": start_frame_name,
                    "source_image_asset_id": "asset_source",
                    "source_image_filename": "source.png",
                }
            )
            status_payload["by_scene_line_id"][scene_line_id] = status_row
            server._phase4_v1_storyboard_save_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
                payload=status_payload,
            )

            first = server._phase4_v1_storyboard_backfill_latest_assigned_outputs(
                brand_slug=brand_slug,
                branch_id=branch_id,
            )
            second = server._phase4_v1_storyboard_backfill_latest_assigned_outputs(
                brand_slug=brand_slug,
                branch_id=branch_id,
            )
            self.assertEqual(str(first.get("latest_backfilled_run_id") or ""), video_run_id)
            self.assertEqual(str(second.get("latest_backfilled_run_id") or ""), video_run_id)
            self.assertGreaterEqual(int(first.get("imported_count") or 0), 1)
            self.assertEqual(int(second.get("imported_count") or 0), 0)


if __name__ == "__main__":
    unittest.main()
