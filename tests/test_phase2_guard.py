from __future__ import annotations

import asyncio
import json
import unittest
from unittest.mock import patch

from fastapi.responses import JSONResponse

import main
import server
from server import RunRequest


class Phase2GuardTests(unittest.TestCase):
    def setUp(self):
        server.pipeline_state["running"] = False

    def test_validate_foundation_context_rejects_legacy_schema(self):
        msg = server._validate_foundation_context(
            {
                "foundation_brief": {
                    "brand_name": "Brand",
                    "product_name": "Product",
                },
                "brand_name": "Brand",
                "product_name": "Product",
            }
        )
        self.assertIsNotNone(msg)
        self.assertIn("schema_version=2.0", msg)

    def test_api_run_blocks_phase2_when_disabled(self):
        with patch("server.config.PHASE2_TEMPORARILY_DISABLED", True), patch(
            "server.config.PHASE2_DISABLED_MESSAGE",
            "Phase 2 is temporarily disabled pending Step 2 migration to Foundation v2.",
        ):
            resp = asyncio.run(
                server.api_run(
                    RunRequest(phases=[2], inputs={"brand_name": "Brand"})
                )
            )

        self.assertIsInstance(resp, JSONResponse)
        self.assertEqual(resp.status_code, 400)
        payload = json.loads(resp.body.decode("utf-8"))
        self.assertIn("temporarily disabled", payload.get("error", ""))

    def test_cli_phase2_guard_returns_empty(self):
        with patch("main.config.PHASE2_TEMPORARILY_DISABLED", True), patch(
            "main.config.PHASE2_DISABLED_MESSAGE",
            "Phase 2 is temporarily disabled pending Step 2 migration to Foundation v2.",
        ):
            result = main.run_phase2({"brand_name": "Brand"})
        self.assertEqual(result, {})

    def test_api_run_blocks_phase3_when_disabled(self):
        with patch("server.config.PHASE3_TEMPORARILY_DISABLED", True), patch(
            "server.config.PHASE3_DISABLED_MESSAGE",
            "Phase 3 is temporarily disabled pending full rebuild.",
        ):
            resp = asyncio.run(
                server.api_run(
                    RunRequest(phases=[3], inputs={"brand_name": "Brand"})
                )
            )

        self.assertIsInstance(resp, JSONResponse)
        self.assertEqual(resp.status_code, 400)
        payload = json.loads(resp.body.decode("utf-8"))
        self.assertIn("temporarily disabled", payload.get("error", ""))

    def test_cli_phase3_guard_returns_empty(self):
        with patch("main.config.PHASE3_TEMPORARILY_DISABLED", True), patch(
            "main.config.PHASE3_DISABLED_MESSAGE",
            "Phase 3 is temporarily disabled pending full rebuild.",
        ):
            result = main.run_phase3({"brand_name": "Brand"})
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
