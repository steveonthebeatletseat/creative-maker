from __future__ import annotations

import asyncio
import unittest

import server


class ServerLogClearApiTests(unittest.TestCase):
    def setUp(self):
        server._reset_server_log_stream()

    def tearDown(self):
        server._reset_server_log_stream()

    def test_api_clear_server_log_clears_tail_and_queue(self):
        server._recent_server_logs.append("line 1")
        server._recent_server_logs.append("line 2")
        server._log_queue.put_nowait("queued line")

        before = asyncio.run(server.api_status())
        self.assertIn("line 1", before.get("server_log_tail", []))
        self.assertIn("line 2", before.get("server_log_tail", []))

        resp = asyncio.run(server.api_clear_server_log())
        self.assertEqual(resp, {"ok": True})

        after = asyncio.run(server.api_status())
        self.assertEqual(after.get("server_log_tail"), [])
        self.assertEqual(len(server._recent_server_logs), 0)
        self.assertTrue(server._log_queue.empty())


if __name__ == "__main__":
    unittest.main()
