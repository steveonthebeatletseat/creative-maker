from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from pipeline.phase4_video_providers import (
    _ANTHROPIC_IMAGE_MAX_BASE64_BYTES,
    _base64_encoded_size,
    _prepare_anthropic_image_payload,
)


class AnthropicImagePayloadTests(unittest.TestCase):
    def test_base64_size_boundary_matches_5mb_limit(self):
        max_raw_that_fits = (_ANTHROPIC_IMAGE_MAX_BASE64_BYTES // 4) * 3
        self.assertLessEqual(_base64_encoded_size(max_raw_that_fits), _ANTHROPIC_IMAGE_MAX_BASE64_BYTES)
        self.assertGreater(_base64_encoded_size(max_raw_that_fits + 1), _ANTHROPIC_IMAGE_MAX_BASE64_BYTES)

    def test_prepare_payload_rejects_raw_file_that_exceeds_base64_limit(self):
        max_raw_that_fits = (_ANTHROPIC_IMAGE_MAX_BASE64_BYTES // 4) * 3
        oversize_raw = max_raw_that_fits + 1

        with tempfile.TemporaryDirectory() as tmp:
            image_path = Path(tmp) / "oversize.png"
            image_path.write_bytes(b"a" * oversize_raw)
            ffmpeg_failed = Mock(return_value=Mock(returncode=1))
            with patch("pipeline.phase4_video_providers.subprocess.run", ffmpeg_failed):
                encoded, mime_type, payload_size, source = _prepare_anthropic_image_payload(image_path)

        self.assertEqual(encoded, "")
        self.assertEqual(mime_type, "")
        self.assertEqual(source, "oversize_unresolved")
        self.assertGreater(payload_size, _ANTHROPIC_IMAGE_MAX_BASE64_BYTES)


if __name__ == "__main__":
    unittest.main()
