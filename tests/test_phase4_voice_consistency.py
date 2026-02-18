from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import server
from pipeline import storage as storage_mod


class Phase4VoiceConsistencyTests(unittest.TestCase):
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
        self.output_dir = self.root / "outputs"
        self._output_patcher = patch.object(server.config, "OUTPUT_DIR", self.output_dir)
        self._output_patcher.start()
        self.addCleanup(self._output_patcher.stop)

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
                "max_difficulty": 8,
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
                                "duration_seconds": 3.0,
                                "on_screen_text": "",
                            },
                            {
                                "scene_line_id": "sl_002",
                                "script_line_id": "L02",
                                "mode": "b_roll",
                                "duration_seconds": 4.0,
                                "on_screen_text": "",
                            },
                        ],
                    }
                ],
            },
        )

    def test_voice_preset_is_consistent_for_all_a_roll_revisions(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True):
            run_req = server.CreateVideoRunRequestV1(
                brand=brand_slug,
                phase3_run_id=phase3_run_id,
                voice_preset_id="calm_female_en_us_v1",
            )
            run_resp = asyncio.run(server.api_phase4_v1_create_run(branch_id, run_req))
            video_run_id = run_resp["run"]["video_run_id"]

            brief = asyncio.run(
                server.api_phase4_v1_generate_start_frame_brief(
                    branch_id,
                    video_run_id,
                    server.GenerateBriefRequestV1(brand=brand_slug),
                )
            )
            _ = asyncio.run(
                server.api_phase4_v1_approve_start_frame_brief(
                    branch_id,
                    video_run_id,
                    server.ApproveBriefRequestV1(brand=brand_slug, approved_by="tester"),
                )
            )

            drive_folder = self.root / "drive_upload"
            drive_folder.mkdir(parents=True, exist_ok=True)
            for item in brief["required_items"]:
                (drive_folder / item["filename"]).write_bytes(b"img")

            validate_resp = asyncio.run(
                server.api_phase4_v1_validate_drive(
                    branch_id,
                    video_run_id,
                    server.DriveValidateRequestV1(brand=brand_slug, folder_url=str(drive_folder)),
                )
            )
            self.assertEqual(validate_resp["status"], "passed")

            asyncio.run(server._phase4_v1_execute_generation(brand_slug, branch_id, video_run_id))

            clips = server.list_video_clips(video_run_id)
            a_roll_clips = [c for c in clips if c.get("mode") == "a_roll"]
            self.assertTrue(a_roll_clips)

            for clip in a_roll_clips:
                revision = server._phase4_v1_get_current_revision_row(clip)
                self.assertIsNotNone(revision)
                provenance = revision.get("provenance") if isinstance(revision.get("provenance"), dict) else {}
                self.assertEqual(provenance.get("voice_preset_id"), "calm_female_en_us_v1")
                self.assertTrue(provenance.get("audio_asset_id"))
                self.assertTrue(provenance.get("talking_head_asset_id"))

                calls = server.list_video_provider_calls(
                    video_run_id,
                    clip_id=clip["clip_id"],
                    revision_id=revision["revision_id"],
                )
                talking_call = next((c for c in calls if c.get("operation") == "talking_head"), None)
                self.assertIsNotNone(talking_call)
                request_payload = talking_call.get("request_payload") if isinstance(talking_call.get("request_payload"), dict) else {}
                self.assertEqual(
                    request_payload.get("audio_asset_id"),
                    provenance.get("audio_asset_id"),
                )


if __name__ == "__main__":
    unittest.main()
