"""Provider adapters for Phase 4 video generation (Drive/Fal/Gemini/TTS/workflow)."""

from __future__ import annotations

import asyncio
import json
import hashlib
import mimetypes
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol

import config


class DriveClient(Protocol):
    def list_assets(self, folder_url: str) -> list[dict[str, Any]]:
        """Return a normalized file listing for the provided folder URL."""


class TTSProvider(Protocol):
    def synthesize(
        self,
        *,
        text: str,
        voice_preset_id: str,
        tts_model: str,
        output_path: Path,
        speed: float,
        pitch: float,
        gain_db: float,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Return synthesized narration metadata."""


class FalVideoProvider(Protocol):
    def generate_talking_head(
        self,
        *,
        avatar_image_path: Path,
        narration_audio_path: Path,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
        planned_duration_seconds: float = 0.0,
        prompt: str = "",
    ) -> dict[str, Any]:
        """Return generated talking-head metadata."""

    def generate_broll(
        self,
        *,
        start_frame_path: Path,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
        planned_duration_seconds: float = 0.0,
        prompt: str = "",
    ) -> dict[str, Any]:
        """Return generated B-roll metadata."""


class GeminiImageEditProvider(Protocol):
    def transform_image(
        self,
        *,
        input_path: Path,
        prompt: str,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Return transformed image metadata."""


class WorkflowBackend(Protocol):
    def start_job(self, key: str, coro_factory: Callable[[], Awaitable[Any]]) -> asyncio.Task:
        """Start or return an in-flight workflow task."""

    def get_job(self, key: str) -> asyncio.Task | None:
        """Lookup in-flight workflow task by key."""

    def clear_job(self, key: str) -> None:
        """Clear workflow handle."""


class InProcessWorkflowBackend:
    """In-process workflow backend seam (Temporal-ready boundary)."""

    def __init__(self):
        self._jobs: dict[str, asyncio.Task] = {}

    def start_job(self, key: str, coro_factory: Callable[[], Awaitable[Any]]) -> asyncio.Task:
        existing = self._jobs.get(key)
        if existing and not existing.done():
            return existing
        task = asyncio.create_task(coro_factory())
        self._jobs[key] = task
        return task

    def get_job(self, key: str) -> asyncio.Task | None:
        task = self._jobs.get(key)
        if task and task.done():
            return None
        return task

    def clear_job(self, key: str) -> None:
        self._jobs.pop(key, None)


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _probe_media(path: Path) -> tuple[float, bool]:
    """Return (duration_seconds, has_audio_stream) when ffprobe is available."""
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_entries",
                "format=duration",
                "-show_streams",
                str(path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return 0.0, False
    if proc.returncode != 0:
        return 0.0, False
    try:
        payload = json.loads(proc.stdout or "{}")
    except Exception:
        return 0.0, False

    duration = 0.0
    format_row = payload.get("format")
    if isinstance(format_row, dict):
        try:
            duration = float(format_row.get("duration") or 0.0)
        except Exception:
            duration = 0.0
    streams = payload.get("streams")
    has_audio = False
    if isinstance(streams, list):
        has_audio = any(str((row or {}).get("codec_type") or "") == "audio" for row in streams if isinstance(row, dict))
    return max(0.0, round(duration, 3)), has_audio


def _download_binary_file(url: str, target_path: Path) -> Path:
    import httpx

    clean_url = str(url or "").strip()
    if not clean_url:
        raise RuntimeError("Missing downloadable URL from provider output.")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", clean_url, follow_redirects=True, timeout=300.0) as response:
        response.raise_for_status()
        with target_path.open("wb") as fh:
            for chunk in response.iter_bytes():
                if chunk:
                    fh.write(chunk)
    return target_path


def _extract_video_url(payload: Any) -> str:
    """Best-effort extraction of an MP4/video URL from a Fal response payload."""
    stack: list[Any] = [payload]
    seen: set[int] = set()
    while stack:
        node = stack.pop(0)
        if id(node) in seen:
            continue
        seen.add(id(node))
        if isinstance(node, dict):
            video = node.get("video")
            if isinstance(video, dict):
                direct = str(video.get("url") or "").strip()
                if direct:
                    return direct
            videos = node.get("videos")
            if isinstance(videos, list):
                for row in videos:
                    if isinstance(row, dict):
                        direct = str(row.get("url") or "").strip()
                        if direct:
                            return direct
            for value in node.values():
                stack.append(value)
            continue
        if isinstance(node, list):
            stack.extend(node)
            continue
        if isinstance(node, str):
            value = node.strip()
            if value.startswith("https://") and (".mp4" in value or "/video" in value):
                return value
    return ""


def _resolve_fal_target(model_id: str) -> tuple[str, str]:
    """
    Convert a model identifier into (application, path) for fal_client.
    Example: `fal-ai/minimax/video-01-live/image-to-video`
      -> (`fal-ai/minimax`, `video-01-live/image-to-video`)
    """
    raw = str(model_id or "").strip().strip("/")
    if not raw:
        raise RuntimeError("Missing Fal model id")
    parts = [part for part in raw.split("/") if part]
    if len(parts) <= 2:
        return raw, ""
    return "/".join(parts[:2]), "/".join(parts[2:])


def _is_truthy(value: str, *, default: bool = False) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


class LocalDriveClient:
    """Test/local Drive adapter: treats `folder_url` as a local path."""

    def _resolve_local_path(self, folder_url: str) -> Path:
        raw = str(folder_url or "").strip()
        if raw.startswith("file://"):
            raw = raw[7:]
        if not raw:
            raise ValueError("Folder URL is required")
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        return path

    def list_assets(self, folder_url: str) -> list[dict[str, Any]]:
        folder = self._resolve_local_path(folder_url)
        if not folder.exists() or not folder.is_dir():
            raise FileNotFoundError(f"Folder not found: {folder}")

        rows: list[dict[str, Any]] = []
        for file_path in sorted(folder.iterdir(), key=lambda p: p.name.lower()):
            if not file_path.is_file():
                continue
            mime = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
            size = int(file_path.stat().st_size)
            readable = True
            checksum = ""
            try:
                checksum = _sha256_file(file_path)
            except Exception:
                readable = False
            rows.append(
                {
                    "name": file_path.name,
                    "mime_type": mime,
                    "size_bytes": size,
                    "checksum_sha256": checksum,
                    "readable": readable,
                    "source_id": str(file_path),
                    "source_url": str(file_path),
                }
            )
        return rows

    def download_asset(self, source_id: str, target_path: Path) -> Path:
        source = Path(str(source_id or "")).expanduser()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Local asset not found: {source}")
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target_path)
        return target_path


class GoogleDriveServiceAccountClient:
    """Google Drive adapter for shared-folder service-account listing."""

    _FOLDER_PATTERNS = (
        re.compile(r"/folders/([A-Za-z0-9_-]+)"),
        re.compile(r"[?&]id=([A-Za-z0-9_-]+)"),
    )

    def __init__(self, service_account_json_path: str):
        self._service_account_json_path = str(service_account_json_path or "").strip()
        if not self._service_account_json_path:
            raise ValueError("Missing Google service account JSON path")

        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build
        except Exception as exc:
            raise RuntimeError(
                "Google Drive dependencies are missing. Install google-api-python-client + google-auth."
            ) from exc

        scopes = ["https://www.googleapis.com/auth/drive.readonly"]
        creds = service_account.Credentials.from_service_account_file(
            self._service_account_json_path,
            scopes=scopes,
        )
        self._drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    @classmethod
    def _extract_folder_id(cls, folder_url: str) -> str:
        raw = str(folder_url or "").strip()
        for pattern in cls._FOLDER_PATTERNS:
            match = pattern.search(raw)
            if match:
                return match.group(1)
        if re.fullmatch(r"[A-Za-z0-9_-]{10,}", raw):
            return raw
        raise ValueError("Could not extract Google Drive folder id from URL")

    def list_assets(self, folder_url: str) -> list[dict[str, Any]]:
        folder_id = self._extract_folder_id(folder_url)
        rows: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            response = self._drive.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields=(
                    "nextPageToken,"
                    "files(id,name,mimeType,size,webViewLink,capabilities/canDownload)"
                ),
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageSize=1000,
                pageToken=page_token,
            ).execute()
            for row in response.get("files", []):
                capabilities = row.get("capabilities") or {}
                rows.append(
                    {
                        "name": str(row.get("name") or ""),
                        "mime_type": str(row.get("mimeType") or "application/octet-stream"),
                        "size_bytes": int(row.get("size") or 0),
                        "checksum_sha256": "",
                        "readable": bool(capabilities.get("canDownload", True)),
                        "source_id": str(row.get("id") or ""),
                        "source_url": str(row.get("webViewLink") or ""),
                    }
                )
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        rows.sort(key=lambda r: str(r.get("name") or "").lower())
        return rows

    def download_asset(self, source_id: str, target_path: Path) -> Path:
        file_id = str(source_id or "").strip()
        if not file_id:
            raise ValueError("Missing source_id for Google Drive download")
        try:
            from googleapiclient.http import MediaIoBaseDownload
        except Exception as exc:
            raise RuntimeError("google-api-python-client is required for Drive downloads") from exc

        request = self._drive.files().get_media(fileId=file_id, supportsAllDrives=True)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        with target_path.open("wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        return target_path


class OpenAITTSProvider:
    """Real TTS provider using OpenAI audio speech."""

    _VOICE_MAP = {
        "calm_female_en_us_v1": "nova",
        "clear_male_en_us_v1": "alloy",
    }
    _ALLOWED_OPENAI_VOICES = {
        "alloy",
        "ash",
        "ballad",
        "coral",
        "echo",
        "fable",
        "nova",
        "onyx",
        "sage",
        "shimmer",
    }

    def __init__(self):
        if not str(config.OPENAI_API_KEY or "").strip():
            raise RuntimeError("OPENAI_API_KEY is required for real narration generation.")
        from openai import OpenAI

        self._client = OpenAI(api_key=config.OPENAI_API_KEY)

    @classmethod
    def _resolve_openai_voice(cls, voice_preset_id: str) -> str:
        preset = str(voice_preset_id or "").strip()
        if preset in cls._ALLOWED_OPENAI_VOICES:
            return preset
        if preset in cls._VOICE_MAP:
            return cls._VOICE_MAP[preset]
        low = preset.lower()
        if "female" in low:
            return "nova"
        if "male" in low:
            return "alloy"
        return "alloy"

    def synthesize(
        self,
        *,
        text: str,
        voice_preset_id: str,
        tts_model: str,
        output_path: Path,
        speed: float,
        pitch: float,
        gain_db: float,
        idempotency_key: str,
    ) -> dict[str, Any]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        voice = self._resolve_openai_voice(voice_preset_id)
        request_speed = max(0.25, min(4.0, float(speed or 1.0)))

        response = self._client.audio.speech.create(
            model=str(tts_model or config.PHASE4_V1_TTS_MODEL),
            voice=voice,
            input=str(text or ""),
            response_format="wav",
            speed=request_speed,
        )
        if hasattr(response, "stream_to_file"):
            response.stream_to_file(str(output_path))
        else:
            content = getattr(response, "content", None)
            if content is None:
                content = bytes(response)
            output_path.write_bytes(content)

        duration_seconds, _ = _probe_media(output_path)
        if duration_seconds <= 0.0:
            # Last-resort estimate if ffprobe cannot read.
            words = len([w for w in str(text or "").split() if w.strip()])
            duration_seconds = max(0.4, round(words / 3.2, 3))

        return {
            "provider": "openai",
            "voice_preset_id": voice_preset_id,
            "provider_voice": voice,
            "tts_model": tts_model,
            "duration_seconds": duration_seconds,
            "size_bytes": int(output_path.stat().st_size),
            "checksum_sha256": _sha256_file(output_path),
            "speed": request_speed,
            "pitch": float(pitch or 0.0),
            "gain_db": float(gain_db or 0.0),
            "idempotency_key": idempotency_key,
        }


class FalClientVideoProvider:
    """Real Fal adapter for talking-head and image-to-video generation."""

    _LEGACY_MODEL_MAP = {
        # Legacy placeholders from early Phase 4 scaffolding.
        "fal-ai/kling-video-v2": "fal-ai/minimax/video-01-live/image-to-video",
        "fal-ai/hedra-character-3": "fal-ai/live-avatar",
    }

    def __init__(self, *, fal_key: str):
        key = str(fal_key or "").strip()
        if not key:
            raise RuntimeError("FAL_KEY is required for real video generation.")
        try:
            import fal_client
        except Exception as exc:
            raise RuntimeError("fal-client is not installed. Add `fal-client` to requirements.") from exc

        self._fal_client = fal_client
        self._client = fal_client.SyncClient(key=key)

    def _run_model(self, model_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        app_id = self._LEGACY_MODEL_MAP.get(str(model_id or "").strip(), str(model_id or "").strip())
        application, path = _resolve_fal_target(app_id)
        result = self._client.run(
            application,
            payload,
            path=path,
            timeout=900.0,
            start_timeout=240.0,
        )
        if not isinstance(result, dict):
            raise RuntimeError(f"Unexpected Fal response type for `{model_id}`: {type(result).__name__}")
        return result

    def generate_talking_head(
        self,
        *,
        avatar_image_path: Path,
        narration_audio_path: Path,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
        planned_duration_seconds: float = 0.0,
        prompt: str = "",
    ) -> dict[str, Any]:
        image_url = self._client.upload_file(str(avatar_image_path))
        audio_url = self._client.upload_file(str(narration_audio_path))

        clip_seconds = max(2, min(10, int(round(float(planned_duration_seconds or 2.0)))))
        payload: dict[str, Any] = {
            "image_url": image_url,
            "audio_url": audio_url,
            "prompt": (
                str(prompt or "").strip()
                or "Natural talking-head delivery to camera, realistic motion, clean background."
            ),
            "num_clips": clip_seconds,
            "frames_per_clip": 16,
            "output_size": "portrait_16_9",
        }

        result = self._run_model(model_id, payload)
        video_url = _extract_video_url(result)
        if not video_url:
            raise RuntimeError(
                f"Fal talking-head response did not contain a downloadable video URL. Response keys: {list(result.keys())}"
            )
        _download_binary_file(video_url, output_path)
        duration_seconds, has_audio = _probe_media(output_path)

        return {
            "provider": "fal",
            "model_id": model_id,
            "duration_seconds": duration_seconds,
            "size_bytes": int(output_path.stat().st_size),
            "checksum_sha256": _sha256_file(output_path),
            "has_audio_stream": has_audio,
            "source_video_url": video_url,
            "idempotency_key": idempotency_key,
            "raw_response": result,
        }

    def generate_broll(
        self,
        *,
        start_frame_path: Path,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
        planned_duration_seconds: float = 0.0,
        prompt: str = "",
    ) -> dict[str, Any]:
        image_url = self._client.upload_file(str(start_frame_path))
        normalized_prompt = (
            str(prompt or "").strip()
            or "Cinematic product-focused motion shot, natural camera movement, vertical composition."
        )
        payload: dict[str, Any] = {
            "image_url": image_url,
            "prompt": f"{normalized_prompt} Vertical 9:16 framing.",
        }

        result = self._run_model(model_id, payload)
        video_url = _extract_video_url(result)
        if not video_url:
            raise RuntimeError(
                f"Fal image-to-video response did not contain a downloadable video URL. Response keys: {list(result.keys())}"
            )
        _download_binary_file(video_url, output_path)
        duration_seconds, has_audio = _probe_media(output_path)

        return {
            "provider": "fal",
            "model_id": model_id,
            "duration_seconds": duration_seconds,
            "size_bytes": int(output_path.stat().st_size),
            "checksum_sha256": _sha256_file(output_path),
            "has_audio_stream": has_audio,
            "source_video_url": video_url,
            "idempotency_key": idempotency_key,
            "planned_duration_seconds": float(planned_duration_seconds or 0.0),
            "raw_response": result,
        }


class MockTTSProvider:
    """Deterministic test-friendly narration synthesizer."""

    def synthesize(
        self,
        *,
        text: str,
        voice_preset_id: str,
        tts_model: str,
        output_path: Path,
        speed: float,
        pitch: float,
        gain_db: float,
        idempotency_key: str,
    ) -> dict[str, Any]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        words = len([w for w in str(text or "").split() if w.strip()])
        # Keep mock durations aligned with short-form line timing so QC is meaningful in local test mode.
        duration = max(1.2, min(8.0, round(words / 4.2, 3)))
        payload = (
            f"VOICE={voice_preset_id}\n"
            f"MODEL={tts_model}\n"
            f"SPEED={speed}\n"
            f"PITCH={pitch}\n"
            f"GAIN_DB={gain_db}\n"
            f"KEY={idempotency_key}\n"
            f"DURATION_SECONDS={duration}\n"
            f"TEXT={text}\n"
        ).encode("utf-8")
        output_path.write_bytes(payload)

        return {
            "provider": "mock_tts",
            "voice_preset_id": voice_preset_id,
            "tts_model": tts_model,
            "duration_seconds": duration,
            "size_bytes": int(output_path.stat().st_size),
            "checksum_sha256": _sha256_file(output_path),
        }


class MockFalVideoProvider:
    """Deterministic Fal stand-in for local testing."""

    @staticmethod
    def _read_mock_audio_duration_seconds(narration_audio_path: Path) -> float:
        try:
            raw = narration_audio_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return 0.0
        for line in raw.splitlines():
            if line.startswith("DURATION_SECONDS="):
                try:
                    value = float(line.split("=", 1)[1].strip())
                except Exception:
                    return 0.0
                return max(0.0, value)
        return 0.0

    def generate_talking_head(
        self,
        *,
        avatar_image_path: Path,
        narration_audio_path: Path,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
        planned_duration_seconds: float = 0.0,
        prompt: str = "",
    ) -> dict[str, Any]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        duration_seconds = self._read_mock_audio_duration_seconds(narration_audio_path)
        if duration_seconds <= 0.0:
            duration_seconds = max(1.2, float(planned_duration_seconds or 0.0))
        duration_seconds = round(duration_seconds, 3)
        payload = (
            f"MODEL={model_id}\n"
            f"MODE=talking_head\n"
            f"AVATAR={avatar_image_path}\n"
            f"AUDIO={narration_audio_path}\n"
            f"KEY={idempotency_key}\n"
            f"DURATION_SECONDS={duration_seconds}\n"
        ).encode("utf-8")
        output_path.write_bytes(payload)
        return {
            "provider": "mock_fal",
            "model_id": model_id,
            "duration_seconds": duration_seconds,
            "size_bytes": int(output_path.stat().st_size),
            "checksum_sha256": _sha256_file(output_path),
            "has_audio_stream": True,
        }

    def generate_broll(
        self,
        *,
        start_frame_path: Path,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
        planned_duration_seconds: float = 0.0,
        prompt: str = "",
    ) -> dict[str, Any]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        duration_seconds = max(1.2, float(planned_duration_seconds or 3.0))
        duration_seconds = round(duration_seconds, 3)
        payload = (
            f"MODEL={model_id}\n"
            f"MODE=broll\n"
            f"START={start_frame_path}\n"
            f"KEY={idempotency_key}\n"
            f"DURATION_SECONDS={duration_seconds}\n"
        ).encode("utf-8")
        output_path.write_bytes(payload)
        return {
            "provider": "mock_fal",
            "model_id": model_id,
            "duration_seconds": duration_seconds,
            "size_bytes": int(output_path.stat().st_size),
            "checksum_sha256": _sha256_file(output_path),
            "has_audio_stream": False,
        }


class MockGeminiImageEditProvider:
    """Deterministic image transform stand-in."""

    def transform_image(
        self,
        *,
        input_path: Path,
        prompt: str,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(input_path, output_path)
        with output_path.open("ab") as f:
            f.write(f"\nTRANSFORM={model_id}|{idempotency_key}|{prompt}".encode("utf-8"))
        return {
            "provider": "mock_gemini",
            "model_id": model_id,
            "size_bytes": int(output_path.stat().st_size),
            "checksum_sha256": _sha256_file(output_path),
        }


def build_drive_client_for_folder(folder_url: str) -> DriveClient:
    raw = str(folder_url or "").strip()
    if raw.startswith("file://") or raw.startswith("/") or raw.startswith("."):
        return LocalDriveClient()
    if bool(config.PHASE4_V1_DRIVE_ALLOW_LOCAL_PATHS):
        path = Path(raw).expanduser()
        if path.exists():
            return LocalDriveClient()
    if not str(config.PHASE4_V1_DRIVE_SERVICE_ACCOUNT_JSON_PATH or "").strip():
        raise RuntimeError(
            "Google Drive service account credentials are not configured. "
            "Set PHASE4_V1_DRIVE_SERVICE_ACCOUNT_JSON_PATH or use a local folder path in test mode."
        )
    return GoogleDriveServiceAccountClient(str(config.PHASE4_V1_DRIVE_SERVICE_ACCOUNT_JSON_PATH))


def build_generation_providers() -> tuple[TTSProvider, FalVideoProvider, GeminiImageEditProvider]:
    force_mock = _is_truthy(os.getenv("PHASE4_V1_FORCE_MOCK_GENERATION", ""), default=False)
    if force_mock:
        return MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider()

    fal_key = str(getattr(config, "FAL_KEY", "") or os.getenv("FAL_KEY", "")).strip()
    openai_key = str(getattr(config, "OPENAI_API_KEY", "")).strip()

    # Auto-upgrade to real providers when credentials are available.
    if fal_key and openai_key:
        return OpenAITTSProvider(), FalClientVideoProvider(fal_key=fal_key), MockGeminiImageEditProvider()

    # Keep local tests/dev deterministic when API credentials are absent.
    return MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider()
