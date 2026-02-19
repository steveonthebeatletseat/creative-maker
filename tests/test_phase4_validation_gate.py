from __future__ import annotations

import asyncio
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import server
from pipeline import storage as storage_mod
from starlette.datastructures import UploadFile


class _FakeDriveClient:
    def __init__(self, assets):
        self.assets = assets

    def list_assets(self, folder_url: str):
        _ = folder_url
        return list(self.assets)


class Phase4ValidationGateTests(unittest.TestCase):
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

    def _create_run_and_brief(self):
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
            return brand_slug, branch_id, video_run_id, brief

    def test_start_frame_brief_is_deterministic(self):
        brand_slug, branch_id, video_run_id, brief_first = self._create_run_and_brief()
        with patch.object(server.config, "PHASE4_V1_ENABLED", True):
            brief_second = asyncio.run(
                server.api_phase4_v1_generate_start_frame_brief(
                    branch_id,
                    video_run_id,
                    server.GenerateBriefRequestV1(brand=brand_slug),
                )
            )

        first_names = sorted([row["filename"] for row in brief_first["required_items"]])
        second_names = sorted([row["filename"] for row in brief_second["required_items"]])
        self.assertEqual(first_names, second_names)

    def test_drive_validation_reports_specific_failure_reasons(self):
        brand_slug, branch_id, video_run_id, brief = self._create_run_and_brief()
        required = {row["file_role"]: row["filename"] for row in brief["required_items"]}
        avatar_name = required["avatar_master"]
        broll_name = required["line_start_frame"]

        cases = [
            (
                "missing_required",
                [],
                "missing_required",
            ),
            (
                "duplicate_ambiguous",
                [
                    {
                        "name": avatar_name,
                        "mime_type": "image/png",
                        "size_bytes": 128,
                        "checksum_sha256": "a1",
                        "readable": True,
                        "source_id": "asset_a",
                        "source_url": "asset_a",
                    },
                    {
                        "name": avatar_name,
                        "mime_type": "image/png",
                        "size_bytes": 128,
                        "checksum_sha256": "a2",
                        "readable": True,
                        "source_id": "asset_b",
                        "source_url": "asset_b",
                    },
                    {
                        "name": broll_name,
                        "mime_type": "image/png",
                        "size_bytes": 128,
                        "checksum_sha256": "b1",
                        "readable": True,
                        "source_id": "asset_c",
                        "source_url": "asset_c",
                    },
                ],
                "duplicate_ambiguous",
            ),
            (
                "unsupported_mime",
                [
                    {
                        "name": avatar_name,
                        "mime_type": "image/png",
                        "size_bytes": 128,
                        "checksum_sha256": "a1",
                        "readable": True,
                        "source_id": "asset_a",
                        "source_url": "asset_a",
                    },
                    {
                        "name": broll_name,
                        "mime_type": "application/pdf",
                        "size_bytes": 128,
                        "checksum_sha256": "b1",
                        "readable": True,
                        "source_id": "asset_b",
                        "source_url": "asset_b",
                    },
                ],
                "unsupported_mime",
            ),
            (
                "permission_denied",
                [
                    {
                        "name": avatar_name,
                        "mime_type": "image/png",
                        "size_bytes": 128,
                        "checksum_sha256": "a1",
                        "readable": False,
                        "source_id": "asset_a",
                        "source_url": "asset_a",
                    },
                    {
                        "name": broll_name,
                        "mime_type": "image/png",
                        "size_bytes": 128,
                        "checksum_sha256": "b1",
                        "readable": True,
                        "source_id": "asset_b",
                        "source_url": "asset_b",
                    },
                ],
                "permission_denied",
            ),
            (
                "zero_byte",
                [
                    {
                        "name": avatar_name,
                        "mime_type": "image/png",
                        "size_bytes": 128,
                        "checksum_sha256": "a1",
                        "readable": True,
                        "source_id": "asset_a",
                        "source_url": "asset_a",
                    },
                    {
                        "name": broll_name,
                        "mime_type": "image/png",
                        "size_bytes": 0,
                        "checksum_sha256": "b1",
                        "readable": True,
                        "source_id": "asset_b",
                        "source_url": "asset_b",
                    },
                ],
                "zero_byte",
            ),
        ]

        with patch.object(server.config, "PHASE4_V1_ENABLED", True):
            for _, assets, expected_issue in cases:
                with self.subTest(issue=expected_issue):
                    with patch("server.build_drive_client_for_folder", return_value=_FakeDriveClient(assets)):
                        report = asyncio.run(
                            server.api_phase4_v1_validate_drive(
                                branch_id,
                                video_run_id,
                                server.DriveValidateRequestV1(brand=brand_slug, folder_url="fake://drive/folder"),
                            )
                        )
                        self.assertEqual(report["status"], "failed")
                        issue_codes = {row["issue_code"] for row in report["items"] if row.get("issue_code")}
                        self.assertIn(expected_issue, issue_codes)

    def test_local_folder_ingest_stages_uploaded_files(self):
        brand_slug, branch_id, video_run_id, brief = self._create_run_and_brief()
        required_names = [row["filename"] for row in brief["required_items"]]
        self.assertGreaterEqual(len(required_names), 2)

        upload_a = UploadFile(filename=required_names[0], file=io.BytesIO(b"avatar-bytes"))
        upload_b = UploadFile(filename=required_names[1], file=io.BytesIO(b"broll-bytes"))

        with patch.object(server.config, "PHASE4_V1_ENABLED", True), patch.object(
            server.config,
            "PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS",
            True,
        ):
            payload = asyncio.run(
                server.api_phase4_v1_ingest_local_folder(
                    branch_id=branch_id,
                    video_run_id=video_run_id,
                    brand=brand_slug,
                    files=[upload_a, upload_b],
                )
            )

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["file_count"], 2)
        folder_path = Path(payload["folder_path"])
        self.assertTrue(folder_path.exists())
        self.assertEqual((folder_path / required_names[0]).read_bytes(), b"avatar-bytes")
        self.assertEqual((folder_path / required_names[1]).read_bytes(), b"broll-bytes")


if __name__ == "__main__":
    unittest.main()
