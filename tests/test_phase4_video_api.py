from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.responses import JSONResponse

import server
from pipeline import storage as storage_mod


class Phase4ApiTests(unittest.TestCase):
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
                        {"line_id": "L01", "text": "This is line one."},
                        {"line_id": "L02", "text": "This is line two."},
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

    def test_create_run_enforces_single_active_lock(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN", True
        ):
            req = server.CreateVideoRunRequestV1(
                brand=brand_slug,
                phase3_run_id=phase3_run_id,
                voice_preset_id="calm_female_en_us_v1",
            )
            first = asyncio.run(server.api_phase4_v1_create_run(branch_id, req))
            self.assertIn("run", first)
            self.assertEqual(first["run"]["workflow_state"], "draft")

            second = asyncio.run(server.api_phase4_v1_create_run(branch_id, req))
            self.assertIsInstance(second, JSONResponse)
            self.assertEqual(second.status_code, 409)
            payload = json.loads(second.body.decode("utf-8"))
            self.assertIn("one active Phase 4 run", payload.get("error", ""))

    def test_list_and_detail_return_created_run(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN", True
        ):
            req = server.CreateVideoRunRequestV1(
                brand=brand_slug,
                phase3_run_id=phase3_run_id,
                voice_preset_id="calm_female_en_us_v1",
            )
            created = asyncio.run(server.api_phase4_v1_create_run(branch_id, req))
            video_run_id = created["run"]["video_run_id"]

            runs = asyncio.run(server.api_phase4_v1_list_runs(branch_id, brand=brand_slug))
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["video_run_id"], video_run_id)

            detail = asyncio.run(server.api_phase4_v1_run_detail(branch_id, video_run_id, brand=brand_slug))
            self.assertEqual(detail["run"]["video_run_id"], video_run_id)
            self.assertEqual(len(detail["clips"]), 2)
            self.assertEqual(detail["run"]["workflow_state"], "draft")

    def test_recreate_run_after_abort_does_not_collide_clip_ids(self):
        brand_slug = "brand_x"
        branch_id = "branch_1"
        phase3_run_id = "p3v2_seed"
        self._seed_phase3_run(brand_slug, branch_id, phase3_run_id)

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config, "PHASE4_V1_TEST_MODE_SINGLE_ACTIVE_RUN", True
        ):
            req = server.CreateVideoRunRequestV1(
                brand=brand_slug,
                phase3_run_id=phase3_run_id,
                voice_preset_id="calm_female_en_us_v1",
            )
            first = asyncio.run(server.api_phase4_v1_create_run(branch_id, req))
            self.assertIn("run", first)
            first_run_id = first["run"]["video_run_id"]

            storage_mod.update_video_run(
                first_run_id,
                status="aborted",
                workflow_state="aborted",
                error="test abort",
            )

            second = asyncio.run(server.api_phase4_v1_create_run(branch_id, req))
            self.assertIn("run", second)
            second_run_id = second["run"]["video_run_id"]
            self.assertNotEqual(first_run_id, second_run_id)

            detail = asyncio.run(server.api_phase4_v1_run_detail(branch_id, second_run_id, brand=brand_slug))
            self.assertEqual(len(detail["clips"]), 2)
            self.assertTrue(
                all(str(row.get("clip_id", "")).startswith(f"{second_run_id}__") for row in detail["clips"])
            )


if __name__ == "__main__":
    unittest.main()
