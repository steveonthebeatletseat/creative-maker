from __future__ import annotations

import asyncio
import base64
import io
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.responses import JSONResponse
from starlette.datastructures import UploadFile

import server
from pipeline import storage as storage_mod
from pipeline.phase4_video_providers import MockVisionSceneProvider


class _SlowVisionProvider:
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
        _ = (image_path, scene_intent, style_profile, model_id, idempotency_key)
        time.sleep(0.2)
        return {
            "score_1_to_10": 7,
            "reason_short": "slow mock score",
            "fit_subject": 7,
            "fit_action": 7,
            "fit_emotion": 7,
            "fit_composition": 7,
            "consistency_with_style_profile": 7,
            "edit_recommended": False,
        }


class StoryboardAssignmentApiTests(unittest.TestCase):
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

    def _seed_phase3_run(self, brand_slug: str, branch_id: str, phase3_run_id: str):
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
                        {"line_id": "L01", "text": "A-roll narration line."},
                        {"line_id": "L02", "text": "B-roll support line."},
                    ],
                }
            ],
        )
        server._phase3_v2_write_json(run_dir / "scene_handoff_packet.json", {})
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
                                "mode": "a_roll",
                                "narration_line": "A-roll narration line.",
                                "scene_description": "Talk to camera.",
                                "duration_seconds": 3.0,
                                "on_screen_text": "",
                            },
                            {
                                "scene_line_id": "sl_002",
                                "script_line_id": "L02",
                                "mode": "b_roll",
                                "narration_line": "B-roll support line.",
                                "scene_description": "Show desk action.",
                                "duration_seconds": 4.0,
                                "on_screen_text": "",
                            },
                        ],
                    }
                ],
            },
        )

    def _bootstrap(self, branch_id: str, brand_slug: str, phase3_run_id: str):
        return asyncio.run(
            server.api_phase4_v1_storyboard_bootstrap(
                branch_id,
                server.StoryboardBootstrapRequestV1(
                    brand=brand_slug,
                    phase3_run_id=phase3_run_id,
                ),
            )
        )

    @staticmethod
    def _branch_row(branch_id: str) -> dict:
        return {
            "id": branch_id,
            "label": branch_id,
            "status": "ready",
            "available_agents": ["creative_engine"],
            "completed_agents": ["creative_engine"],
            "failed_agents": [],
            "inputs": {},
        }

    @staticmethod
    def _tiny_png_bytes() -> bytes:
        return base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Zs2kAAAAASUVORK5CYII="
        )

    def test_storyboard_bootstrap_creates_and_reuses_active_run(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True):
            first = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self.assertFalse(first["reused_existing_run"])
            self.assertTrue(first["video_run_id"])
            self.assertGreaterEqual(int(first["clip_count"]), 2)

            second = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            self.assertTrue(second["reused_existing_run"])
            self.assertEqual(first["video_run_id"], second["video_run_id"])

            run_row = server.get_video_run(first["video_run_id"])
            self.assertEqual(str(run_row.get("workflow_state")), "brief_approved")

    def test_storyboard_bootstrap_auto_resolves_single_active_test_mode_conflict(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_a = "p3v2_seed_a"
        phase3_run_b = "p3v2_seed_b"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_a)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN", True
        ):
            first = self._bootstrap(branch_id, brand_slug, phase3_run_a)
            first_run_id = str(first["video_run_id"])
            self.assertTrue(first_run_id)

            self._seed_phase3_run(brand_slug, branch_id, phase3_run_b)
            second = self._bootstrap(branch_id, brand_slug, phase3_run_b)
            second_run_id = str(second["video_run_id"])
            self.assertTrue(second_run_id)
            self.assertNotEqual(first_run_id, second_run_id)

            old_row = server.get_video_run(first_run_id) or {}
            new_row = server.get_video_run(second_run_id) or {}
            self.assertEqual(str(old_row.get("status")), "aborted")
            self.assertEqual(str(new_row.get("status")), "active")

    def test_storyboard_run_detail_includes_scene_description_per_clip(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True):
            bootstrap = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            video_run_id = str(bootstrap.get("video_run_id") or "")
            self.assertTrue(video_run_id)

            detail = asyncio.run(
                server.api_phase4_v1_run_detail(branch_id, video_run_id, brand=brand_slug)
            )
            self.assertIsInstance(detail, dict)
            clips = detail.get("clips")
            self.assertIsInstance(clips, list)
            self.assertGreaterEqual(len(clips), 2)

            by_scene = {}
            for clip in clips:
                if not isinstance(clip, dict):
                    continue
                by_scene[str(clip.get("scene_line_id") or "")] = str(clip.get("scene_description") or "")
            self.assertEqual(by_scene.get("sl_001"), "Talk to camera.")
            self.assertEqual(by_scene.get("sl_002"), "Show desk action.")

    def test_storyboard_run_detail_prefers_assignment_edit_prompt_and_model_metadata(self):
        brand_slug = "brand_x"
        branch_id = "branch_prompt_meta"
        phase3_run_id = "p3v2_seed_prompt_meta"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True):
            bootstrap = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            video_run_id = str(bootstrap.get("video_run_id") or "")
            self.assertTrue(video_run_id)

            clips = server.list_video_clips(video_run_id)
            status_payload = server._phase4_v1_storyboard_build_initial_status(video_run_id=video_run_id, clips=clips)
            by_scene = status_payload.get("by_scene_line_id") if isinstance(status_payload.get("by_scene_line_id"), dict) else {}
            row = by_scene.get("sl_001") if isinstance(by_scene.get("sl_001"), dict) else {}
            row.update(
                {
                    "assignment_status": "assigned",
                    "assignment_score": 8,
                    "edit_prompt": "Use moody close-up lighting and keyboard focus.",
                    "edit_model_id": "gemini-2.5-flash-image",
                    "edit_provider": "google",
                }
            )
            by_scene["sl_001"] = row
            status_payload["by_scene_line_id"] = by_scene
            status_payload["status"] = "completed"
            server._phase4_v1_storyboard_write_runtime_status(
                brand_slug=brand_slug,
                branch_id=branch_id,
                video_run_id=video_run_id,
                task_key=server._phase4_v1_storyboard_task_key(brand_slug, branch_id, video_run_id),
                payload=status_payload,
            )
            server._phase4_v1_storyboard_update_metrics(
                video_run_id=video_run_id,
                updates={
                    "storyboard_prompt_model_provider": "anthropic",
                    "storyboard_prompt_model_id": "claude-opus-4-6",
                    "storyboard_prompt_model_label": "Claude Opus 4.6",
                    "storyboard_image_edit_model_id": "gemini-2.5-flash-image",
                    "storyboard_image_edit_model_label": "Nano Banana Pro 1K/2K",
                },
            )

            detail = asyncio.run(
                server.api_phase4_v1_run_detail(branch_id, video_run_id, brand=brand_slug)
            )
            self.assertIsInstance(detail, dict)
            clips_out = detail.get("clips") if isinstance(detail.get("clips"), list) else []
            sl_001 = next(
                (
                    clip
                    for clip in clips_out
                    if isinstance(clip, dict) and str(clip.get("scene_line_id") or "") == "sl_001"
                ),
                {},
            )
            self.assertEqual(
                str(sl_001.get("transform_prompt") or ""),
                "Use moody close-up lighting and keyboard focus.",
            )
            self.assertEqual(
                str(sl_001.get("edit_prompt") or ""),
                "Use moody close-up lighting and keyboard focus.",
            )
            self.assertEqual(str(sl_001.get("prompt_model_label") or ""), "Claude Opus 4.6")
            self.assertEqual(str(sl_001.get("image_edit_model_label") or ""), "Nano Banana Pro 1K/2K")

    def test_local_folder_ingest_auto_renames_duplicates(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            bootstrap = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            video_run_id = bootstrap["video_run_id"]

            uploads = [
                UploadFile(filename="folder_a/desk.png", file=io.BytesIO(b"img1")),
                UploadFile(filename="folder_b/desk.png", file=io.BytesIO(b"img2")),
                UploadFile(filename="folder_b/notes.txt", file=io.BytesIO(b"not-image")),
            ]
            ingest = asyncio.run(
                server.api_phase4_v1_ingest_local_folder(
                    branch_id,
                    video_run_id,
                    brand=brand_slug,
                    files=uploads,
                )
            )
            self.assertIsInstance(ingest, dict)
            self.assertEqual(int(ingest["file_count"]), 3)
            self.assertEqual(len(ingest.get("renamed_files", [])), 1)
            renamed = ingest["renamed_files"][0]
            self.assertEqual(renamed["original_name"], "desk.png")
            self.assertTrue(str(renamed["stored_name"]).startswith("desk__dup"))
            self.assertEqual(int(ingest.get("supported_image_count", 0)), 2)
            self.assertEqual(len(ingest.get("skipped_files", [])), 1)
            self.assertEqual(ingest["skipped_files"][0]["original_name"], "notes.txt")

    def test_broll_library_is_shared_per_brand_across_branches(self):
        brand_slug = "brand_x"
        branch_a = "branch_a"
        branch_b = "branch_b"
        server._save_branches(
            brand_slug,
            [self._branch_row(branch_a), self._branch_row(branch_b)],
        )

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_a,
                    brand=brand_slug,
                    files=[
                        UploadFile(filename="bank/desk.png", file=io.BytesIO(b"desk")),
                        UploadFile(filename="bank/hands.jpg", file=io.BytesIO(b"hands")),
                    ],
                )
            )
            self.assertIsInstance(added, dict)
            self.assertEqual(int(added.get("added_count") or 0), 2)

            listed_from_b = asyncio.run(
                server.api_phase4_v1_list_broll_library(branch_b, brand=brand_slug)
            )
            self.assertIsInstance(listed_from_b, dict)
            self.assertEqual(int(listed_from_b.get("file_count") or 0), 2)
            names_from_b = {
                str(row.get("file_name") or "")
                for row in listed_from_b.get("files") or []
                if isinstance(row, dict)
            }
            self.assertIn("desk.png", names_from_b)
            self.assertIn("hands.jpg", names_from_b)

            deleted = asyncio.run(
                server.api_phase4_v1_delete_broll_library_files(
                    branch_b,
                    server.BrollCatalogDeleteRequestV1(
                        brand=brand_slug,
                        file_names=["desk.png"],
                    ),
                )
            )
            self.assertIsInstance(deleted, dict)
            self.assertEqual(deleted.get("removed_file_names"), ["desk.png"])

            listed_from_a = asyncio.run(
                server.api_phase4_v1_list_broll_library(branch_a, brand=brand_slug)
            )
            names_from_a = {
                str(row.get("file_name") or "")
                for row in listed_from_a.get("files") or []
                if isinstance(row, dict)
            }
            self.assertNotIn("desk.png", names_from_a)
            self.assertIn("hands.jpg", names_from_a)

    def test_broll_library_add_preserves_existing_metadata(self):
        brand_slug = "brand_x"
        branch_id = "branch_meta"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            first = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/scene.png", file=io.BytesIO(b"scene"))],
                )
            )
            self.assertIsInstance(first, dict)
            manifest_path = server._phase4_v1_broll_library_manifest_path(brand_slug, branch_id)
            manifest_rows = server._phase4_v1_read_json(manifest_path, [])
            self.assertIsInstance(manifest_rows, list)
            self.assertEqual(len(manifest_rows), 1)
            manifest_rows[0]["metadata"] = {"quality": "approved", "tags": ["desk"]}
            manifest_rows[0]["custom_note"] = "keep-me"
            server._phase4_v1_write_json(manifest_path, manifest_rows)

            second = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/new-shot.webp", file=io.BytesIO(b"new"))],
                )
            )
            self.assertIsInstance(second, dict)
            self.assertEqual(int(second.get("added_count") or 0), 1)

            persisted = server._phase4_v1_read_json(manifest_path, [])
            scene_row = None
            for row in persisted:
                if str((row or {}).get("file_name") or "") == "scene.png":
                    scene_row = row
                    break
            self.assertIsInstance(scene_row, dict)
            self.assertEqual(str(scene_row.get("custom_note") or ""), "keep-me")
            metadata = scene_row.get("metadata") if isinstance(scene_row.get("metadata"), dict) else {}
            self.assertEqual(str(metadata.get("quality") or ""), "approved")
            self.assertEqual(metadata.get("tags"), ["desk"])

    def test_broll_library_migrates_legacy_branch_scope(self):
        brand_slug = "brand_x"
        branch_id = "branch_legacy"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        legacy_dir = server._branch_output_dir(brand_slug, branch_id) / "phase4_broll_library"
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / "legacy.png").write_bytes(b"legacy-image")
        server._phase4_v1_write_json(
            legacy_dir / "manifest.json",
            [
                {
                    "file_name": "legacy.png",
                    "size_bytes": 12,
                    "added_at": "2026-02-20T00:00:00",
                    "metadata": {"source": "legacy-branch"},
                }
            ],
        )

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            listed = asyncio.run(
                server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug)
            )
            self.assertIsInstance(listed, dict)
            self.assertEqual(int(listed.get("file_count") or 0), 1)
            file_row = (listed.get("files") or [{}])[0]
            self.assertEqual(str(file_row.get("file_name") or ""), "legacy.png")
            self.assertEqual(
                str(((file_row.get("metadata") if isinstance(file_row.get("metadata"), dict) else {}).get("source")) or ""),
                "legacy-branch",
            )

            brand_manifest = server._phase4_v1_broll_library_manifest_path(brand_slug, branch_id)
            brand_dir = brand_manifest.parent
            self.assertTrue((brand_dir / "legacy.png").exists())

    def test_broll_library_metadata_patch_updates_tags(self):
        brand_slug = "brand_x"
        branch_id = "branch_tags"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/scene.png", file=io.BytesIO(b"scene"))],
                )
            )
            self.assertIsInstance(added, dict)
            patched = asyncio.run(
                server.api_phase4_v1_update_broll_library_file_metadata(
                    branch_id,
                    server.BrollCatalogUpdateMetadataRequestV1(
                        brand=brand_slug,
                        file_name="scene.png",
                        tags=["desk", " Desk ", "night"],
                    ),
                )
            )
            self.assertIsInstance(patched, dict)
            updated = patched.get("updated_file") or {}
            metadata = updated.get("metadata") if isinstance(updated.get("metadata"), dict) else {}
            self.assertEqual(metadata.get("tags"), ["desk", "night"])

            listed = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            file_rows = listed.get("files") if isinstance(listed, dict) else []
            self.assertIsInstance(file_rows, list)
            row = next(
                (item for item in file_rows if str((item or {}).get("file_name") or "") == "scene.png"),
                {},
            )
            row_meta = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            self.assertEqual(row_meta.get("tags"), ["desk", "night"])

    def test_broll_library_list_includes_original_url_and_filter_kind(self):
        brand_slug = "brand_x"
        branch_id = "branch_contract"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/scene.png", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )
            self.assertIsInstance(added, dict)
            listed = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            self.assertIsInstance(listed, dict)
            file_rows = listed.get("files")
            self.assertIsInstance(file_rows, list)
            self.assertEqual(len(file_rows), 1)
            row = file_rows[0] if isinstance(file_rows[0], dict) else {}
            self.assertTrue(str(row.get("thumbnail_url") or "").startswith("/outputs/"))
            self.assertTrue(str(row.get("original_url") or "").startswith("/outputs/"))
            self.assertEqual(str(row.get("filter_kind") or ""), "broll")

    @unittest.skipIf(server.Image is None, "Pillow not available")
    def test_broll_library_valid_image_generates_thumbnail_cache(self):
        brand_slug = "brand_x"
        branch_id = "branch_thumb"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/thumb_source.png", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )
            self.assertIsInstance(added, dict)
            listed = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            row = (listed.get("files") or [{}])[0]
            thumb_url = str((row if isinstance(row, dict) else {}).get("thumbnail_url") or "")
            self.assertIn("/.thumbs/", thumb_url)
            manifest = server._phase4_v1_read_json(
                server._phase4_v1_broll_thumbs_manifest_path(brand_slug, branch_id),
                {},
            )
            checksums = manifest.get("checksums") if isinstance(manifest, dict) else {}
            self.assertTrue(isinstance(checksums, dict) and checksums)

    def test_broll_library_invalid_image_falls_back_to_original_thumbnail(self):
        brand_slug = "brand_x"
        branch_id = "branch_thumb_fallback"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/not-an-image.png", file=io.BytesIO(b"bad-bytes"))],
                )
            )
            self.assertIsInstance(added, dict)
            listed = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            row = (listed.get("files") or [{}])[0]
            row_dict = row if isinstance(row, dict) else {}
            self.assertEqual(
                str(row_dict.get("thumbnail_url") or ""),
                str(row_dict.get("original_url") or ""),
            )

    def test_broll_filter_kind_classification(self):
        self.assertEqual(
            server._phase4_v1_broll_filter_kind({"mode_hint": "a_roll", "ai_generated": False, "library_item_type": "original_upload"}),
            "a_roll",
        )
        self.assertEqual(
            server._phase4_v1_broll_filter_kind({"mode_hint": "b_roll", "ai_generated": True, "library_item_type": "ai_generated"}),
            "ai_modified",
        )
        self.assertEqual(
            server._phase4_v1_broll_filter_kind({"mode_hint": "animation_broll", "ai_generated": False, "library_item_type": "original_upload"}),
            "animation_broll",
        )
        self.assertEqual(
            server._phase4_v1_broll_filter_kind({"mode_hint": "unknown", "ai_generated": False, "library_item_type": "original_upload"}),
            "broll",
        )

    def test_broll_library_rename_updates_manifest_and_disk(self):
        brand_slug = "brand_x"
        branch_id = "branch_rename"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/scene.png", file=io.BytesIO(b"scene"))],
                )
            )
            self.assertIsInstance(added, dict)
            renamed = asyncio.run(
                server.api_phase4_v1_rename_broll_library_file(
                    branch_id,
                    server.BrollCatalogRenameRequestV1(
                        brand=brand_slug,
                        file_name="scene.png",
                        new_file_name="scene_renamed",
                    ),
                )
            )
            self.assertIsInstance(renamed, dict)
            self.assertEqual(str(renamed.get("old_file_name") or ""), "scene.png")
            self.assertEqual(str(renamed.get("new_file_name") or ""), "scene_renamed.png")

            library_dir = server._phase4_v1_broll_library_dir(brand_slug, branch_id)
            self.assertFalse((library_dir / "scene.png").exists())
            self.assertTrue((library_dir / "scene_renamed.png").exists())

            listed = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            file_names = {
                str((row or {}).get("file_name") or "")
                for row in (listed.get("files") if isinstance(listed, dict) else [])
                if isinstance(row, dict)
            }
            self.assertIn("scene_renamed.png", file_names)
            self.assertNotIn("scene.png", file_names)

    @unittest.skipIf(server.Image is None, "Pillow not available")
    def test_broll_library_rename_keeps_checksum_thumbnail(self):
        brand_slug = "brand_x"
        branch_id = "branch_rename_thumb"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/source.png", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )
            self.assertIsInstance(added, dict)
            before = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            before_row = (before.get("files") or [{}])[0]
            before_row = before_row if isinstance(before_row, dict) else {}

            renamed = asyncio.run(
                server.api_phase4_v1_rename_broll_library_file(
                    branch_id,
                    server.BrollCatalogRenameRequestV1(
                        brand=brand_slug,
                        file_name="source.png",
                        new_file_name="renamed_source.png",
                    ),
                )
            )
            self.assertIsInstance(renamed, dict)
            after = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            file_rows = after.get("files") if isinstance(after, dict) else []
            self.assertIsInstance(file_rows, list)
            after_row = next((row for row in file_rows if isinstance(row, dict)), {})
            self.assertEqual(
                str(before_row.get("thumbnail_url") or ""),
                str(after_row.get("thumbnail_url") or ""),
            )
            self.assertNotEqual(
                str(before_row.get("original_url") or ""),
                str(after_row.get("original_url") or ""),
            )

    @unittest.skipIf(server.Image is None, "Pillow not available")
    def test_broll_library_delete_prunes_thumbnail_manifest_and_file(self):
        brand_slug = "brand_x"
        branch_id = "branch_delete_thumb"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[UploadFile(filename="bank/delete_me.png", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )
            self.assertIsInstance(added, dict)
            listed = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            row = (listed.get("files") or [{}])[0]
            row = row if isinstance(row, dict) else {}
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            checksum = str(metadata.get("source_checksum_sha256") or "").strip().lower()
            manifest_before = server._phase4_v1_read_json(
                server._phase4_v1_broll_thumbs_manifest_path(brand_slug, branch_id),
                {},
            )
            checksums_before = manifest_before.get("checksums") if isinstance(manifest_before, dict) else {}
            self.assertIn(checksum, checksums_before)
            thumb_name = str(((checksums_before.get(checksum) if isinstance(checksums_before.get(checksum), dict) else {}).get("thumb_file_name")) or "")
            thumb_path = server._phase4_v1_broll_thumbs_dir(brand_slug, branch_id) / thumb_name
            self.assertTrue(thumb_path.exists())

            removed = asyncio.run(
                server.api_phase4_v1_delete_broll_library_files(
                    branch_id,
                    server.BrollCatalogDeleteRequestV1(
                        brand=brand_slug,
                        file_names=[str(row.get("file_name") or "")],
                    ),
                )
            )
            self.assertIsInstance(removed, dict)

            manifest_after = server._phase4_v1_read_json(
                server._phase4_v1_broll_thumbs_manifest_path(brand_slug, branch_id),
                {},
            )
            checksums_after = manifest_after.get("checksums") if isinstance(manifest_after, dict) else {}
            self.assertNotIn(checksum, checksums_after)
            self.assertFalse(thumb_path.exists())

    def test_broll_library_rename_collision_returns_409(self):
        brand_slug = "brand_x"
        branch_id = "branch_collision"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    files=[
                        UploadFile(filename="bank/a.png", file=io.BytesIO(b"a")),
                        UploadFile(filename="bank/b.png", file=io.BytesIO(b"b")),
                    ],
                )
            )
            self.assertIsInstance(added, dict)
            collided = asyncio.run(
                server.api_phase4_v1_rename_broll_library_file(
                    branch_id,
                    server.BrollCatalogRenameRequestV1(
                        brand=brand_slug,
                        file_name="a.png",
                        new_file_name="b.png",
                    ),
                )
            )
            self.assertIsInstance(collided, JSONResponse)
            self.assertEqual(int(collided.status_code), 409)

            listed = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_slug))
            file_names = {
                str((row or {}).get("file_name") or "")
                for row in (listed.get("files") if isinstance(listed, dict) else [])
                if isinstance(row, dict)
            }
            self.assertIn("a.png", file_names)
            self.assertIn("b.png", file_names)

    def test_broll_library_cross_brand_isolation_for_mutations(self):
        brand_a = "brand_a"
        brand_b = "brand_b"
        branch_id = "branch_1"
        server._save_branches(brand_a, [self._branch_row(branch_id)])
        server._save_branches(brand_b, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ):
            added_a = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_a,
                    files=[UploadFile(filename="bank/shared.png", file=io.BytesIO(b"a"))],
                )
            )
            self.assertIsInstance(added_a, dict)
            self.assertEqual(int(added_a.get("added_count") or 0), 1)

            listed_b = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_b))
            self.assertIsInstance(listed_b, dict)
            self.assertEqual(int(listed_b.get("file_count") or 0), 0)

            rename_b = asyncio.run(
                server.api_phase4_v1_rename_broll_library_file(
                    branch_id,
                    server.BrollCatalogRenameRequestV1(
                        brand=brand_b,
                        file_name="shared.png",
                        new_file_name="other.png",
                    ),
                )
            )
            self.assertIsInstance(rename_b, JSONResponse)
            self.assertEqual(int(rename_b.status_code), 404)

            delete_b = asyncio.run(
                server.api_phase4_v1_delete_broll_library_files(
                    branch_id,
                    server.BrollCatalogDeleteRequestV1(
                        brand=brand_b,
                        file_names=["shared.png"],
                    ),
                )
            )
            self.assertIsInstance(delete_b, dict)
            self.assertEqual(delete_b.get("removed_file_names"), [])

            listed_a = asyncio.run(server.api_phase4_v1_list_broll_library(branch_id, brand=brand_a))
            self.assertEqual(int(listed_a.get("file_count") or 0), 1)
            self.assertEqual(str((listed_a.get("files") or [{}])[0].get("file_name") or ""), "shared.png")

    def test_broll_library_upload_indexes_ready_metadata(self):
        brand_slug = "brand_x"
        branch_id = "branch_idx_ready"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.object(server, "build_vision_scene_provider", return_value=MockVisionSceneProvider()):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="b_roll",
                    files=[UploadFile(filename="bank/ready.png", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )
            self.assertIsInstance(added, dict)
            self.assertEqual(int(added.get("added_count") or 0), 1)
            self.assertEqual(int(added.get("indexed_count") or 0), 1)
            self.assertEqual(int(added.get("index_failed_count") or 0), 0)
            rows = added.get("files") if isinstance(added.get("files"), list) else []
            self.assertEqual(len(rows), 1)
            metadata = rows[0].get("metadata") if isinstance(rows[0].get("metadata"), dict) else {}
            self.assertEqual(str(metadata.get("indexing_status") or ""), "ready")
            self.assertTrue(isinstance(metadata.get("analysis"), dict) and bool(metadata.get("analysis")))
            self.assertTrue(str(metadata.get("indexing_model_id") or "").strip())

    def test_broll_library_upload_failure_marks_row_failed_without_batch_abort(self):
        brand_slug = "brand_x"
        branch_id = "branch_idx_fail"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        class _FailOneVisionProvider:
            def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
                _ = (model_id, idempotency_key)
                if "bad" in image_path.name:
                    raise RuntimeError("forced-index-failure")
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
                    "provider": "mock_vision_scene",
                    "model_id": "gpt-5.2",
                }

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.object(server, "build_vision_scene_provider", return_value=_FailOneVisionProvider()):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="b_roll",
                    files=[
                        UploadFile(filename="bank/good.png", file=io.BytesIO(self._tiny_png_bytes())),
                        UploadFile(filename="bank/bad.png", file=io.BytesIO(b"intentionally-different-image-bytes")),
                    ],
                )
            )
            self.assertIsInstance(added, dict)
            self.assertEqual(int(added.get("added_count") or 0), 2)
            self.assertEqual(int(added.get("indexed_count") or 0), 1)
            self.assertEqual(int(added.get("index_failed_count") or 0), 1)
            rows = added.get("files") if isinstance(added.get("files"), list) else []
            by_name = {str(row.get("file_name") or ""): row for row in rows if isinstance(row, dict)}
            bad_meta = (by_name.get("bad.png") or {}).get("metadata") if isinstance((by_name.get("bad.png") or {}).get("metadata"), dict) else {}
            self.assertEqual(str(bad_meta.get("indexing_status") or ""), "failed")
            self.assertTrue(str(bad_meta.get("indexing_error") or "").strip())
            good_meta = (by_name.get("good.png") or {}).get("metadata") if isinstance((by_name.get("good.png") or {}).get("metadata"), dict) else {}
            self.assertEqual(str(good_meta.get("indexing_status") or ""), "ready")

    def test_broll_library_reindex_failed_file_succeeds(self):
        brand_slug = "brand_x"
        branch_id = "branch_reindex"
        server._save_branches(brand_slug, [self._branch_row(branch_id)])

        class _AlwaysFailVisionProvider:
            def analyze_image(self, *, image_path: Path, model_id: str, idempotency_key: str):
                _ = (image_path, model_id, idempotency_key)
                raise RuntimeError("forced-index-failure")

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.object(server, "build_vision_scene_provider", return_value=_AlwaysFailVisionProvider()):
            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="b_roll",
                    files=[UploadFile(filename="bank/reindex_me.png", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )
            self.assertEqual(int(added.get("index_failed_count") or 0), 1)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.object(server, "build_vision_scene_provider", return_value=MockVisionSceneProvider()):
            result = asyncio.run(
                server.api_phase4_v1_reindex_broll_library_files(
                    branch_id,
                    server.BrollCatalogDeleteRequestV1(
                        brand=brand_slug,
                        file_names=["reindex_me.png"],
                    ),
                )
            )
            self.assertIsInstance(result, dict)
            self.assertEqual(len(result.get("failed") or []), 0)
            reindexed = result.get("reindexed") if isinstance(result.get("reindexed"), list) else []
            self.assertEqual(len(reindexed), 1)
            rows = result.get("files") if isinstance(result.get("files"), list) else []
            row = next((item for item in rows if str(item.get("file_name") or "") == "reindex_me.png"), {})
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
            self.assertEqual(str(metadata.get("indexing_status") or ""), "ready")

    def test_storyboard_source_selection_patch_persists_and_reloads(self):
        brand_slug = "brand_x"
        branch_id = "branch_selection"
        phase3_run_id = "p3v2_seed_selection"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.object(server, "build_vision_scene_provider", return_value=MockVisionSceneProvider()):
            bootstrap = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            video_run_id = bootstrap["video_run_id"]

            asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="a_roll",
                    files=[UploadFile(filename="bank/a_face.png", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )
            asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="b_roll",
                    files=[
                        UploadFile(filename="bank/b1.png", file=io.BytesIO(self._tiny_png_bytes())),
                        UploadFile(filename="bank/b2.png", file=io.BytesIO(self._tiny_png_bytes())),
                    ],
                )
            )

            patched = asyncio.run(
                server.api_phase4_v1_storyboard_source_selection(
                    branch_id,
                    video_run_id,
                    server.StoryboardSourceSelectionRequestV1(
                        brand=brand_slug,
                        selected_a_roll_files=["a_face.png"],
                        selected_b_roll_files=["b2.png"],
                    ),
                )
            )
            self.assertIsInstance(patched, dict)
            self.assertEqual(patched.get("selected_a_roll_files"), ["a_face.png"])
            self.assertEqual(patched.get("selected_b_roll_files"), ["b2.png"])

            detail = asyncio.run(
                server.api_phase4_v1_run_detail(
                    branch_id,
                    video_run_id,
                    brand=brand_slug,
                )
            )
            source_selection = detail.get("storyboard_source_selection") if isinstance(detail.get("storyboard_source_selection"), dict) else {}
            self.assertEqual(source_selection.get("selected_a_roll_files"), ["a_face.png"])
            self.assertEqual(source_selection.get("selected_b_roll_files"), ["b2.png"])

    def test_storyboard_assign_start_rejects_without_ready_selected_broll(self):
        brand_slug = "brand_x"
        branch_id = "branch_no_broll"
        phase3_run_id = "p3v2_seed_no_broll"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.object(server, "build_vision_scene_provider", return_value=MockVisionSceneProvider()):
            bootstrap = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            video_run_id = bootstrap["video_run_id"]
            asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="a_roll",
                    files=[UploadFile(filename="bank/a_face.png", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )

            started = asyncio.run(
                server.api_phase4_v1_storyboard_assign_start(
                    branch_id,
                    video_run_id,
                    server.StoryboardAssignStartRequestV1(
                        brand=brand_slug,
                        selected_a_roll_files=["a_face.png"],
                        selected_b_roll_files=[],
                    ),
                )
            )
            self.assertIsInstance(started, JSONResponse)
            self.assertEqual(int(started.status_code), 400)
            payload = json.loads(started.body.decode("utf-8"))
            self.assertIn("No ready B-roll images selected", str(payload.get("error") or ""))

    def test_storyboard_assign_flow_updates_clip_snapshots(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.dict(os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False), patch.object(
            server, "build_vision_scene_provider", return_value=MockVisionSceneProvider()
        ):
            bootstrap = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            video_run_id = bootstrap["video_run_id"]

            added = asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="b_roll",
                    files=[
                        UploadFile(filename="bank/desk_focus.jpg", file=io.BytesIO(self._tiny_png_bytes())),
                        UploadFile(filename="bank/animation_scene.png", file=io.BytesIO(self._tiny_png_bytes())),
                    ],
                )
            )
            self.assertIsInstance(added, dict)
            self.assertGreaterEqual(int(added.get("indexed_count") or 0), 1)
            asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="a_roll",
                    files=[UploadFile(filename="bank/talking_head.webp", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )

            async def _run_assignment():
                started = await server.api_phase4_v1_storyboard_assign_start(
                    branch_id,
                    video_run_id,
                    server.StoryboardAssignStartRequestV1(
                        brand=brand_slug,
                        edit_threshold=10,
                        low_flag_threshold=6,
                    ),
                )
                self.assertIsInstance(started, dict)
                self.assertEqual(started.get("status"), "running")

                status_payload = None
                for _ in range(80):
                    await asyncio.sleep(0.05)
                    status_payload = await server.api_phase4_v1_storyboard_assign_status(
                        branch_id,
                        video_run_id,
                        brand=brand_slug,
                    )
                    if isinstance(status_payload, JSONResponse):
                        break
                    state = str(status_payload.get("status") or "").strip().lower()
                    if state in {"completed", "failed"}:
                        break
                return status_payload

            status_payload = asyncio.run(_run_assignment())
            self.assertIsInstance(status_payload, dict)
            self.assertEqual(str(status_payload.get("status")), "completed")
            by_scene = status_payload.get("by_scene_line_id")
            self.assertIsInstance(by_scene, dict)
            self.assertEqual(len(by_scene), 2)

            clips = server.list_video_clips(video_run_id)
            for clip in clips:
                revision = server._phase4_v1_get_current_revision_row(clip)
                self.assertIsNotNone(revision)
                snapshot = revision.get("input_snapshot") if isinstance(revision.get("input_snapshot"), dict) else {}
                self.assertTrue(str(snapshot.get("start_frame_filename") or "").strip())
                if str(clip.get("mode") or "").strip() == "a_roll":
                    self.assertTrue(str(snapshot.get("avatar_filename") or "").strip())

            assets = server.list_video_assets(video_run_id)
            transformed_assets = [a for a in assets if str(a.get("asset_type")) == "transformed_frame"]
            self.assertGreater(len(transformed_assets), 0)

            report = server._phase4_v1_validation_report_model(video_run_id)
            self.assertIsNotNone(report)
            self.assertEqual(str(report.status), "passed")

    def test_storyboard_assign_stop_marks_aborted(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.dict(os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False), patch.object(
            server, "build_vision_scene_provider", return_value=_SlowVisionProvider()
        ):
            bootstrap = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            video_run_id = bootstrap["video_run_id"]

            asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="b_roll",
                    files=[
                        UploadFile(filename="bank/desk_focus.jpg", file=io.BytesIO(self._tiny_png_bytes())),
                        UploadFile(filename="bank/animation_scene.png", file=io.BytesIO(self._tiny_png_bytes())),
                    ],
                )
            )
            asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="a_roll",
                    files=[UploadFile(filename="bank/talking_head.webp", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )

            async def _start_then_stop():
                started = await server.api_phase4_v1_storyboard_assign_start(
                    branch_id,
                    video_run_id,
                    server.StoryboardAssignStartRequestV1(
                        brand=brand_slug,
                        edit_threshold=1,
                        low_flag_threshold=6,
                    ),
                )
                self.assertEqual(str(started.get("status") or ""), "running")
                await asyncio.sleep(0.05)
                stopped = await server.api_phase4_v1_storyboard_assign_stop(
                    branch_id,
                    video_run_id,
                    server.StoryboardAssignControlRequestV1(brand=brand_slug),
                )
                await asyncio.sleep(0.05)
                status = await server.api_phase4_v1_storyboard_assign_status(
                    branch_id,
                    video_run_id,
                    brand=brand_slug,
                )
                return stopped, status

            stopped, status = asyncio.run(_start_then_stop())
            self.assertIsInstance(stopped, dict)
            self.assertEqual(str(stopped.get("status") or ""), "aborted")
            self.assertIsInstance(status, dict)
            self.assertEqual(str(status.get("status") or ""), "aborted")

    def test_storyboard_assign_reset_clears_snapshot_fields(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS", True
        ), patch.dict(os.environ, {"PHASE4_V1_FORCE_MOCK_GENERATION": "true"}, clear=False), patch.object(
            server, "build_vision_scene_provider", return_value=MockVisionSceneProvider()
        ):
            bootstrap = self._bootstrap(branch_id, brand_slug, phase3_run_id)
            video_run_id = bootstrap["video_run_id"]

            asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="b_roll",
                    files=[UploadFile(filename="bank/desk_focus.jpg", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )
            asyncio.run(
                server.api_phase4_v1_add_broll_library_files(
                    branch_id,
                    brand=brand_slug,
                    mode_hint="a_roll",
                    files=[UploadFile(filename="bank/talking_head.webp", file=io.BytesIO(self._tiny_png_bytes()))],
                )
            )

            async def _run_and_reset():
                started = await server.api_phase4_v1_storyboard_assign_start(
                    branch_id,
                    video_run_id,
                    server.StoryboardAssignStartRequestV1(
                        brand=brand_slug,
                        edit_threshold=10,
                        low_flag_threshold=6,
                    ),
                )
                self.assertEqual(str(started.get("status") or ""), "running")
                for _ in range(80):
                    await asyncio.sleep(0.05)
                    status_payload = await server.api_phase4_v1_storyboard_assign_status(
                        branch_id,
                        video_run_id,
                        brand=brand_slug,
                    )
                    if isinstance(status_payload, JSONResponse):
                        break
                    state = str(status_payload.get("status") or "").strip().lower()
                    if state in {"completed", "failed"}:
                        break
                reset_resp = await server.api_phase4_v1_storyboard_assign_reset(
                    branch_id,
                    video_run_id,
                    server.StoryboardAssignControlRequestV1(brand=brand_slug),
                )
                reset_status = await server.api_phase4_v1_storyboard_assign_status(
                    branch_id,
                    video_run_id,
                    brand=brand_slug,
                )
                return reset_resp, reset_status

            reset_resp, reset_status = asyncio.run(_run_and_reset())
            self.assertIsInstance(reset_resp, dict)
            self.assertEqual(str(reset_resp.get("status") or ""), "reset")
            self.assertIsInstance(reset_status, dict)
            self.assertEqual(str(reset_status.get("status") or ""), "idle")

            by_scene = reset_status.get("by_scene_line_id")
            self.assertIsInstance(by_scene, dict)
            self.assertTrue(by_scene)
            for row in by_scene.values():
                self.assertEqual(str((row or {}).get("assignment_status") or ""), "pending")

            clips = server.list_video_clips(video_run_id)
            for clip in clips:
                revision = server._phase4_v1_get_current_revision_row(clip) or {}
                snapshot = (
                    revision.get("input_snapshot")
                    if isinstance(revision.get("input_snapshot"), dict)
                    else {}
                )
                self.assertEqual(str(snapshot.get("start_frame_filename") or ""), "")
                self.assertEqual(str(snapshot.get("avatar_filename") or ""), "")


if __name__ == "__main__":
    unittest.main()
