"""Provider adapters for Phase 4 video generation (Drive/Fal/Gemini/TTS/workflow)."""

from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import re
import shutil
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
    ) -> dict[str, Any]:
        """Return generated talking-head metadata."""

    def generate_broll(
        self,
        *,
        start_frame_path: Path,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
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
        payload = (
            f"VOICE={voice_preset_id}\n"
            f"MODEL={tts_model}\n"
            f"SPEED={speed}\n"
            f"PITCH={pitch}\n"
            f"GAIN_DB={gain_db}\n"
            f"KEY={idempotency_key}\n"
            f"TEXT={text}\n"
        ).encode("utf-8")
        output_path.write_bytes(payload)

        words = len([w for w in str(text or "").split() if w.strip()])
        duration = max(0.4, round(words / 2.6, 3))
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

    def generate_talking_head(
        self,
        *,
        avatar_image_path: Path,
        narration_audio_path: Path,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            f"MODEL={model_id}\n"
            f"MODE=talking_head\n"
            f"AVATAR={avatar_image_path}\n"
            f"AUDIO={narration_audio_path}\n"
            f"KEY={idempotency_key}\n"
        ).encode("utf-8")
        output_path.write_bytes(payload)
        return {
            "provider": "mock_fal",
            "model_id": model_id,
            "duration_seconds": max(0.5, round(narration_audio_path.stat().st_size / 500.0, 3)),
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
    ) -> dict[str, Any]:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = (
            f"MODEL={model_id}\n"
            f"MODE=broll\n"
            f"START={start_frame_path}\n"
            f"KEY={idempotency_key}\n"
        ).encode("utf-8")
        output_path.write_bytes(payload)
        return {
            "provider": "mock_fal",
            "model_id": model_id,
            "duration_seconds": 3.0,
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
    return MockTTSProvider(), MockFalVideoProvider(), MockGeminiImageEditProvider()
