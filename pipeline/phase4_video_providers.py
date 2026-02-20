"""Provider adapters for Phase 4 video generation (Drive/Fal/Gemini/TTS/workflow)."""

from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict
import json
import hashlib
import logging
import mimetypes
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol

import config

logger = logging.getLogger(__name__)

_ANTHROPIC_IMAGE_MAX_BYTES = 5 * 1024 * 1024
_ANTHROPIC_IMAGE_TARGET_BYTES = _ANTHROPIC_IMAGE_MAX_BYTES - (128 * 1024)
_ANTHROPIC_IMAGE_DIMENSION_STEPS = (2048, 1600, 1280, 1024, 896, 768, 640, 512)
_ANTHROPIC_IMAGE_QUALITY_STEPS = (6, 10, 14, 18, 22, 26, 30)
_ANTHROPIC_IMAGE_PAYLOAD_CACHE_MAX = 64
_ANTHROPIC_IMAGE_PAYLOAD_CACHE: OrderedDict[str, tuple[str, str, int, str]] = OrderedDict()
_ANTHROPIC_IMAGE_PAYLOAD_CACHE_LOCK = threading.Lock()


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


class VisionSceneProvider(Protocol):
    def analyze_image(
        self,
        *,
        image_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Return structured metadata for a candidate storyboard image."""

    def score_scene_match(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Return structured 1-10 scene fit scoring for the image."""

    def compose_transform_prompt(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        image_analysis: dict[str, Any] | None,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Return JSON with at least `edit_prompt` for image-to-image generation."""


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


def _tokenize_filename_tags(value: str) -> list[str]:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())
    return [part for part in text.split() if len(part) >= 3]


def _default_image_analysis(image_path: Path) -> dict[str, Any]:
    tags = _tokenize_filename_tags(image_path.stem)
    caption = " ".join(tags[:10]).strip() or "generic lifestyle scene"
    return {
        "caption": caption,
        "subjects": tags[:4],
        "actions": tags[4:8],
        "setting": "indoor",
        "camera_angle": "unknown",
        "shot_type": "unknown",
        "lighting": "unknown",
        "mood": "neutral",
        "product_visibility": "unknown",
        "text_present": False,
        "style_tags": tags[:8],
        "attention_hooks": tags[:5],
        "quality_issues": [],
    }


def _build_simple_score(
    *,
    scene_intent: dict[str, Any],
    image_analysis: dict[str, Any],
) -> dict[str, Any]:
    intent_text = " ".join(
        str(scene_intent.get(k) or "")
        for k in ("mode", "narration_line", "scene_description")
    ).strip().lower()
    intent_tokens = {t for t in re.findall(r"[a-z0-9]{3,}", intent_text)}
    image_text = " ".join(
        str(image_analysis.get(k) or "")
        for k in ("caption", "subjects", "actions", "setting", "mood", "style_tags")
    ).strip().lower()
    image_tokens = {t for t in re.findall(r"[a-z0-9]{3,}", image_text)}
    overlap = len(intent_tokens & image_tokens)
    token_density = max(1, min(5, overlap))
    score = max(1, min(10, 3 + token_density))
    return {
        "score_1_to_10": int(score),
        "reason_short": "Keyword overlap scoring fallback.",
        "fit_subject": int(max(1, min(10, score))),
        "fit_action": int(max(1, min(10, score - 1))),
        "fit_emotion": int(max(1, min(10, score - 1))),
        "fit_composition": int(max(1, min(10, score - 1))),
        "consistency_with_style_profile": int(max(1, min(10, score - 1))),
        "edit_recommended": bool(score <= 5),
    }


def _default_transform_prompt(
    *,
    scene_intent: dict[str, Any],
    style_profile: dict[str, Any],
    image_analysis: dict[str, Any] | None = None,
) -> str:
    mode = str(scene_intent.get("mode") or "b_roll").strip()
    script_line_id = str(scene_intent.get("script_line_id") or "").strip()
    narration = str(scene_intent.get("narration_line") or "").strip()
    description = str(scene_intent.get("scene_description") or "").strip()

    style_chunks: list[str] = []
    for key in ("shot_type", "camera_angle", "lighting", "mood", "setting"):
        value = str(style_profile.get(key) or "").strip()
        if value:
            style_chunks.append(f"{key}: {value}")
    tags = style_profile.get("style_tags") if isinstance(style_profile.get("style_tags"), list) else []
    if tags:
        style_chunks.append(f"style tags: {', '.join([str(v) for v in tags[:8]])}")
    style_text = "; ".join(style_chunks) or "preserve the source style family"

    analysis = image_analysis if isinstance(image_analysis, dict) else {}
    source_caption = str(analysis.get("caption") or "").strip()
    source_subjects = analysis.get("subjects") if isinstance(analysis.get("subjects"), list) else []
    source_actions = analysis.get("actions") if isinstance(analysis.get("actions"), list) else []
    source_traits: list[str] = []
    if source_caption:
        source_traits.append(f"source caption: {source_caption}")
    if source_subjects:
        source_traits.append(f"source subjects: {', '.join([str(v) for v in source_subjects[:6]])}")
    if source_actions:
        source_traits.append(f"source actions: {', '.join([str(v) for v in source_actions[:6]])}")
    source_text = "; ".join(source_traits) or "source scene is creator lifestyle content"

    return (
        "You are editing a real creator image into a new start frame for short-form video.\n"
        "Make a bold, clearly different variation from the source while keeping the same person/product identity.\n"
        f"Mode: {mode}\n"
        f"Script line ID: {script_line_id or 'unknown'}\n"
        f"Narration line: {narration}\n"
        f"Target scene: {description}\n"
        f"Source clues: {source_text}\n"
        f"Style guidance: {style_text}\n"
        "Required output intent:\n"
        "- Change composition and camera framing to fit the target scene.\n"
        "- Change pose/action/background props to match the target scene.\n"
        "- Keep same person identity and product consistency.\n"
        "- 9:16 vertical composition.\n"
        "- No unrelated logos/text overlays/watermarks."
    )


def _image_data_url(image_path: Path) -> str:
    mime_type = mimetypes.guess_type(image_path.name)[0] or "image/png"
    encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _prepare_anthropic_image_payload(image_path: Path) -> tuple[str, str, int, str]:
    """
    Return base64 payload under Anthropic's 5MB image limit whenever possible.
    Falls back to iterative ffmpeg JPEG compression for oversized uploads.
    """
    cache_key = ""
    try:
        st = image_path.stat()
        cache_key = f"{image_path.resolve()}::{int(st.st_size)}::{int(st.st_mtime_ns)}"
    except Exception:
        cache_key = ""

    if cache_key:
        with _ANTHROPIC_IMAGE_PAYLOAD_CACHE_LOCK:
            cached = _ANTHROPIC_IMAGE_PAYLOAD_CACHE.get(cache_key)
            if cached:
                _ANTHROPIC_IMAGE_PAYLOAD_CACHE.move_to_end(cache_key)
                return cached

    def _cache_store(value: tuple[str, str, int, str]) -> tuple[str, str, int, str]:
        if not cache_key:
            return value
        with _ANTHROPIC_IMAGE_PAYLOAD_CACHE_LOCK:
            _ANTHROPIC_IMAGE_PAYLOAD_CACHE[cache_key] = value
            _ANTHROPIC_IMAGE_PAYLOAD_CACHE.move_to_end(cache_key)
            while len(_ANTHROPIC_IMAGE_PAYLOAD_CACHE) > _ANTHROPIC_IMAGE_PAYLOAD_CACHE_MAX:
                _ANTHROPIC_IMAGE_PAYLOAD_CACHE.popitem(last=False)
        return value

    raw_bytes = image_path.read_bytes()
    raw_mime_type = mimetypes.guess_type(image_path.name)[0] or "image/png"
    if len(raw_bytes) <= _ANTHROPIC_IMAGE_MAX_BYTES:
        return _cache_store((
            base64.b64encode(raw_bytes).decode("ascii"),
            raw_mime_type,
            len(raw_bytes),
            "original",
        ))

    best_bytes = raw_bytes
    best_mime_type = raw_mime_type
    best_source = "original_oversize"
    tmp_root = Path(tempfile.mkdtemp(prefix="phase4_vision_img_"))
    try:
        for max_dim in _ANTHROPIC_IMAGE_DIMENSION_STEPS:
            for quality in _ANTHROPIC_IMAGE_QUALITY_STEPS:
                candidate = tmp_root / f"vision_{max_dim}_q{quality}.jpg"
                proc = subprocess.run(
                    [
                        "ffmpeg",
                        "-y",
                        "-hide_banner",
                        "-loglevel",
                        "error",
                        "-i",
                        str(image_path),
                        "-vf",
                        f"scale={max_dim}:{max_dim}:force_original_aspect_ratio=decrease",
                        "-frames:v",
                        "1",
                        "-q:v",
                        str(quality),
                        str(candidate),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if proc.returncode != 0 or not candidate.exists():
                    continue
                candidate_bytes = candidate.read_bytes()
                if not candidate_bytes:
                    continue
                if len(candidate_bytes) < len(best_bytes):
                    best_bytes = candidate_bytes
                    best_mime_type = "image/jpeg"
                    best_source = f"ffmpeg_{max_dim}_q{quality}"
                if len(candidate_bytes) <= _ANTHROPIC_IMAGE_TARGET_BYTES:
                    return _cache_store((
                        base64.b64encode(candidate_bytes).decode("ascii"),
                        "image/jpeg",
                        len(candidate_bytes),
                        best_source,
                    ))
    except Exception as exc:
        logger.warning("Anthropic vision image preprocess failed for %s: %s", image_path, exc)
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

    if len(best_bytes) <= _ANTHROPIC_IMAGE_MAX_BYTES:
        return _cache_store((
            base64.b64encode(best_bytes).decode("ascii"),
            best_mime_type,
            len(best_bytes),
            best_source,
        ))

    return _cache_store(("", "", len(best_bytes), "oversize_unresolved"))


def _parse_json_object_text(raw: Any) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        snippet = text[start : end + 1]
        try:
            parsed = json.loads(snippet)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


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


class OpenAIImageEditProvider:
    """OpenAI image edit adapter for image-to-image transformations."""

    def __init__(self):
        api_key = str(config.OPENAI_API_KEY or "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required for OpenAI image edits.")
        from openai import OpenAI

        self._client = OpenAI(api_key=api_key)

    def transform_image(
        self,
        *,
        input_path: Path,
        prompt: str,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        if not input_path.exists() or not input_path.is_file():
            raise RuntimeError(f"Input image does not exist: {input_path}")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        prompt_text = str(prompt or "").strip()
        requested_model = str(model_id or config.PHASE4_V1_OPENAI_IMAGE_EDIT_MODEL_ID).strip()
        model_candidates: list[str] = []
        for candidate in (
            requested_model,
            str(config.PHASE4_V1_OPENAI_IMAGE_EDIT_MODEL_ID).strip(),
            "gpt-image-1.5",
            "gpt-image-1",
            "chatgpt-image-latest",
            "dall-e-2",
        ):
            value = str(candidate or "").strip()
            if value and value not in model_candidates:
                model_candidates.append(value)

        last_error: Exception | None = None
        final_model_name = requested_model or (model_candidates[0] if model_candidates else "")
        for model_name in model_candidates:
            final_model_name = model_name
            try:
                with input_path.open("rb") as image_fh:
                    response = self._client.images.edit(
                        model=model_name,
                        image=image_fh,
                        prompt=prompt_text,
                        response_format="b64_json",
                        output_format="png",
                        size="1024x1536",
                        quality="high",
                    )
            except Exception as tuned_exc:
                logger.warning(
                    "OpenAI image edit tuned params failed for model=%s; retrying minimal call: %s",
                    model_name,
                    tuned_exc,
                )
                try:
                    with input_path.open("rb") as image_fh:
                        response = self._client.images.edit(
                            model=model_name,
                            image=image_fh,
                            prompt=prompt_text,
                            response_format="b64_json",
                        )
                except Exception as minimal_exc:
                    last_error = minimal_exc
                    continue

            data_rows = response.data if response and isinstance(response.data, list) else []
            first = data_rows[0] if data_rows else None
            b64_json = str(getattr(first, "b64_json", "") or "").strip() if first is not None else ""
            out_url = str(getattr(first, "url", "") or "").strip() if first is not None else ""
            if b64_json:
                output_path.write_bytes(base64.b64decode(b64_json))
                break
            if out_url:
                _download_binary_file(out_url, output_path)
                break
            last_error = RuntimeError("OpenAI image edit response missing both b64_json and url.")
        else:
            raise RuntimeError(
                f"OpenAI image edit failed for all model candidates: {model_candidates}. "
                f"Last error: {last_error}"
            )

        return {
            "provider": "openai_image",
            "model_id": final_model_name,
            "size_bytes": int(output_path.stat().st_size),
            "checksum_sha256": _sha256_file(output_path),
            "idempotency_key": idempotency_key,
        }


class GoogleGeminiImageEditProvider:
    """Real Gemini image edit adapter (Nano Banana-compatible models)."""

    def __init__(self):
        api_key = str(config.GOOGLE_API_KEY or "").strip()
        if not api_key:
            raise RuntimeError("GOOGLE_API_KEY is required for Gemini image edits.")
        from google import genai

        self._client = genai.Client(api_key=api_key)
        self._openai_fallback: OpenAIImageEditProvider | None = None
        if str(config.OPENAI_API_KEY or "").strip():
            try:
                self._openai_fallback = OpenAIImageEditProvider()
            except Exception as exc:
                logger.warning("OpenAI image edit fallback unavailable for Gemini adapter: %s", exc)

    def transform_image(
        self,
        *,
        input_path: Path,
        prompt: str,
        output_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        from google.genai import types

        if not input_path.exists() or not input_path.is_file():
            raise RuntimeError(f"Input image does not exist: {input_path}")

        image_bytes = input_path.read_bytes()
        mime_type = mimetypes.guess_type(input_path.name)[0] or "image/png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        requested_model = str(model_id or config.PHASE4_V1_GEMINI_IMAGE_EDIT_MODEL_ID).strip()

        if self._openai_fallback and requested_model.lower().startswith(("gpt-", "chatgpt-", "dall-e")):
            direct_openai = self._openai_fallback.transform_image(
                input_path=input_path,
                prompt=prompt,
                output_path=output_path,
                model_id=requested_model,
                idempotency_key=idempotency_key,
            )
            direct_openai["fallback_from"] = "google_gemini"
            direct_openai["fallback_error"] = "model_routed_to_openai"
            return direct_openai

        try:
            prompt_text = str(prompt or "").strip()
            try:
                # Prefer native image-edit endpoint with explicit aspect ratio controls.
                edit_response = self._client.models.edit_image(
                    model=requested_model,
                    prompt=prompt_text,
                    reference_images=[
                        types.RawReferenceImage(
                            reference_id=1,
                            reference_image=types.Image(image_bytes=image_bytes, mime_type=mime_type),
                        )
                    ],
                    config=types.EditImageConfig(
                        aspect_ratio="9:16",
                        output_mime_type="image/png",
                    ),
                )
                generated = (
                    edit_response.generated_images
                    if edit_response is not None and isinstance(edit_response.generated_images, list)
                    else []
                )
                first_image = generated[0].image if generated and generated[0] is not None else None
                out_bytes = (
                    bytes(first_image.image_bytes)
                    if first_image is not None and getattr(first_image, "image_bytes", None)
                    else b""
                )
                if out_bytes:
                    output_path.write_bytes(out_bytes)
                    return {
                        "provider": "google_gemini",
                        "model_id": requested_model,
                        "size_bytes": int(output_path.stat().st_size),
                        "checksum_sha256": _sha256_file(output_path),
                        "idempotency_key": idempotency_key,
                    }
                raise RuntimeError("Gemini edit_image returned no image bytes.")
            except Exception as edit_exc:
                logger.warning("Gemini edit_image unavailable/failed; trying generate_content fallback: %s", edit_exc)

            response = self._client.models.generate_content(
                model=requested_model,
                contents=[
                    types.Part.from_text(text=f"{prompt_text}\nOutput aspect ratio: 9:16."),
                    types.Part.from_bytes(data=image_bytes, mime_type=mime_type),
                ],
                config=types.GenerateContentConfig(
                    response_modalities=["TEXT", "IMAGE"],
                    image_config=types.ImageConfig(aspect_ratio="9:16"),
                ),
            )

            out_bytes = None
            candidates = response.candidates if response and isinstance(response.candidates, list) else []
            for candidate in candidates:
                content = candidate.content if candidate is not None else None
                parts = content.parts if content is not None and isinstance(content.parts, list) else []
                for part in parts:
                    inline_data = getattr(part, "inline_data", None)
                    data = getattr(inline_data, "data", None) if inline_data is not None else None
                    if data:
                        out_bytes = data
                        break
                if out_bytes:
                    break
            if not out_bytes:
                raise RuntimeError("Gemini generate_content image edit response missing generated image bytes.")

            output_path.write_bytes(out_bytes)
            return {
                "provider": "google_gemini",
                "model_id": requested_model,
                "size_bytes": int(output_path.stat().st_size),
                "checksum_sha256": _sha256_file(output_path),
                "idempotency_key": idempotency_key,
            }
        except Exception as exc:
            if self._openai_fallback is None:
                raise
            logger.warning("Gemini image edit failed; using OpenAI fallback: %s", exc)
            fallback_result = self._openai_fallback.transform_image(
                input_path=input_path,
                prompt=prompt,
                output_path=output_path,
                model_id=requested_model,
                idempotency_key=idempotency_key,
            )
            fallback_result["fallback_from"] = "google_gemini"
            fallback_result["fallback_error"] = str(exc)
            return fallback_result


class MockVisionSceneProvider:
    """Deterministic multimodal stand-in for storyboard assignment."""

    def analyze_image(
        self,
        *,
        image_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        _ = (model_id, idempotency_key)
        result = _default_image_analysis(image_path)
        result["provider"] = "mock_vision"
        return result

    def score_scene_match(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        _ = (style_profile, model_id, idempotency_key)
        result = _build_simple_score(
            scene_intent=scene_intent,
            image_analysis=_default_image_analysis(image_path),
        )
        result["provider"] = "mock_vision"
        return result

    def compose_transform_prompt(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        image_analysis: dict[str, Any] | None,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        _ = (image_path, model_id, idempotency_key)
        return {
            "edit_prompt": _default_transform_prompt(
                scene_intent=scene_intent,
                style_profile=style_profile,
                image_analysis=image_analysis,
            ),
            "provider": "mock_vision",
        }


class OpenAIVisionSceneProvider:
    """OpenAI multimodal adapter for storyboard analysis + scoring."""

    def __init__(self):
        if not str(config.OPENAI_API_KEY or "").strip():
            raise RuntimeError("OPENAI_API_KEY is required for vision scene analysis.")
        from openai import OpenAI

        self._client = OpenAI(api_key=config.OPENAI_API_KEY)

    def _chat_json(
        self,
        *,
        image_path: Path,
        model_id: str,
        instructions: str,
        user_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = user_payload or {}
        model_candidates: list[str] = []
        for candidate in (
            str(model_id or "").strip(),
            str(config.PHASE4_V1_VISION_SCENE_MODEL_ID or "").strip(),
            "gpt-4o-mini",
            "gpt-4.1-mini",
        ):
            if candidate and candidate not in model_candidates:
                model_candidates.append(candidate)
        for candidate in model_candidates:
            try:
                response = self._client.chat.completions.create(
                    model=candidate,
                    response_format={"type": "json_object"},
                    temperature=0.2,
                    max_completion_tokens=900,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are a strict JSON-only storyboard vision evaluator. "
                                "Return only valid JSON with requested keys."
                            ),
                        },
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": instructions},
                                {"type": "text", "text": json.dumps(payload, ensure_ascii=True)},
                                {"type": "image_url", "image_url": {"url": _image_data_url(image_path)}},
                            ],
                        },
                    ],
                )
                raw = str(response.choices[0].message.content or "").strip()
                if not raw:
                    continue
                parsed = _parse_json_object_text(raw)
                if parsed:
                    return parsed
            except Exception as exc:
                logger.warning("OpenAI vision scene call failed (model=%s): %s", candidate, exc)
        return {}

    def analyze_image(
        self,
        *,
        image_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "Analyze this image for storyboard retrieval. Return JSON with keys exactly: "
            "caption, subjects, actions, setting, camera_angle, shot_type, lighting, "
            "mood, product_visibility, text_present, style_tags, attention_hooks, quality_issues."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={"idempotency_key": idempotency_key},
        )
        if not result:
            result = _default_image_analysis(image_path)
        result["provider"] = "openai_vision"
        return result

    def score_scene_match(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "Score image fit to scene intent for short-form start-frame selection. "
            "Return JSON keys exactly: score_1_to_10, reason_short, fit_subject, fit_action, "
            "fit_emotion, fit_composition, consistency_with_style_profile, edit_recommended. "
            "All score fields are integers 1-10."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={
                "idempotency_key": idempotency_key,
                "scene_intent": scene_intent,
                "style_profile": style_profile,
            },
        )
        if not result:
            result = _build_simple_score(
                scene_intent=scene_intent,
                image_analysis=_default_image_analysis(image_path),
            )
        result["provider"] = "openai_vision"
        return result

    def compose_transform_prompt(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        image_analysis: dict[str, Any] | None,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "You are an elite short-form creative director. "
            "Look at the source image and generate one high-impact image-edit prompt. "
            "The goal is a clearly transformed frame that matches the scene intent, not a tiny tweak. "
            "Return JSON keys exactly: edit_prompt, change_summary. "
            "edit_prompt must be plain text for an image-to-image model. "
            "Keep identity/product consistency, vertical 9:16 framing, and no unrelated text/logos."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={
                "idempotency_key": idempotency_key,
                "scene_intent": scene_intent,
                "style_profile": style_profile,
                "image_analysis": image_analysis or {},
            },
        )
        prompt_text = str(result.get("edit_prompt") if isinstance(result, dict) else "").strip()
        if not prompt_text:
            prompt_text = _default_transform_prompt(
                scene_intent=scene_intent,
                style_profile=style_profile,
                image_analysis=image_analysis,
            )
        return {
            "edit_prompt": prompt_text,
            "change_summary": str(result.get("change_summary") if isinstance(result, dict) else "").strip(),
            "provider": "openai_vision",
        }


class AnthropicVisionSceneProvider:
    """Anthropic multimodal adapter for storyboard analysis + scoring."""

    def __init__(self):
        if not str(config.ANTHROPIC_API_KEY or "").strip():
            raise RuntimeError("ANTHROPIC_API_KEY is required for vision scene analysis.")
        from anthropic import Anthropic

        self._client = Anthropic(api_key=config.ANTHROPIC_API_KEY)

    def _chat_json(
        self,
        *,
        image_path: Path,
        model_id: str,
        instructions: str,
        user_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = user_payload or {}
        encoded, mime_type, payload_size, payload_source = _prepare_anthropic_image_payload(image_path)
        if not encoded:
            logger.warning(
                "Anthropic vision skipped oversize image for storyboard (image=%s bytes=%d source=%s idempotency=%s)",
                image_path.name,
                payload_size,
                payload_source,
                str(payload.get("idempotency_key") or ""),
            )
            return {}
        model_candidates: list[str] = []
        for candidate in (
            str(model_id or "").strip(),
            str(config.ANTHROPIC_FRONTIER or "").strip(),
        ):
            if candidate and candidate not in model_candidates:
                model_candidates.append(candidate)
        for candidate in model_candidates:
            try:
                response = self._client.messages.create(
                    model=candidate,
                    max_tokens=900,
                    temperature=0.2,
                    system=(
                        "You are a strict JSON-only storyboard vision evaluator. "
                        "Return only valid JSON with requested keys."
                    ),
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": instructions},
                                {"type": "text", "text": json.dumps(payload, ensure_ascii=True)},
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": mime_type,
                                        "data": encoded,
                                    },
                                },
                            ],
                        }
                    ],
                )
                blocks = response.content if response and isinstance(response.content, list) else []
                text_parts: list[str] = []
                for block in blocks:
                    if getattr(block, "type", "") == "text":
                        text_parts.append(str(getattr(block, "text", "") or ""))
                raw = "\n".join(text_parts).strip()
                if not raw:
                    continue
                parsed = _parse_json_object_text(raw)
                if parsed:
                    return parsed
            except Exception as exc:
                logger.warning(
                    "Anthropic vision scene call failed (model=%s image=%s bytes=%d source=%s idempotency=%s): %s",
                    candidate,
                    image_path.name,
                    payload_size,
                    payload_source,
                    str(payload.get("idempotency_key") or ""),
                    exc,
                )
        return {}

    def analyze_image(
        self,
        *,
        image_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "Analyze this image for storyboard retrieval. Return JSON with keys exactly: "
            "caption, subjects, actions, setting, camera_angle, shot_type, lighting, "
            "mood, product_visibility, text_present, style_tags, attention_hooks, quality_issues."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={"idempotency_key": idempotency_key},
        )
        if not result:
            result = _default_image_analysis(image_path)
        result["provider"] = "anthropic_vision"
        return result

    def score_scene_match(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "Score image fit to scene intent for short-form start-frame selection. "
            "Return JSON keys exactly: score_1_to_10, reason_short, fit_subject, fit_action, "
            "fit_emotion, fit_composition, consistency_with_style_profile, edit_recommended. "
            "All score fields are integers 1-10."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={
                "idempotency_key": idempotency_key,
                "scene_intent": scene_intent,
                "style_profile": style_profile,
            },
        )
        if not result:
            result = _build_simple_score(
                scene_intent=scene_intent,
                image_analysis=_default_image_analysis(image_path),
            )
        result["provider"] = "anthropic_vision"
        return result

    def compose_transform_prompt(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        image_analysis: dict[str, Any] | None,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "You are an elite short-form creative director. "
            "Look at the source image and generate one high-impact image-edit prompt. "
            "The goal is a clearly transformed frame that matches the scene intent, not a tiny tweak. "
            "Return JSON keys exactly: edit_prompt, change_summary. "
            "edit_prompt must be plain text for an image-to-image model. "
            "Keep identity/product consistency, vertical 9:16 framing, and no unrelated text/logos."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={
                "idempotency_key": idempotency_key,
                "scene_intent": scene_intent,
                "style_profile": style_profile,
                "image_analysis": image_analysis or {},
            },
        )
        prompt_text = str(result.get("edit_prompt") if isinstance(result, dict) else "").strip()
        if not prompt_text:
            prompt_text = _default_transform_prompt(
                scene_intent=scene_intent,
                style_profile=style_profile,
                image_analysis=image_analysis,
            )
        return {
            "edit_prompt": prompt_text,
            "change_summary": str(result.get("change_summary") if isinstance(result, dict) else "").strip(),
            "provider": "anthropic_vision",
        }


class GoogleVisionSceneProvider:
    """Google Gemini multimodal adapter for storyboard analysis + scoring."""

    def __init__(self):
        if not str(config.GOOGLE_API_KEY or "").strip():
            raise RuntimeError("GOOGLE_API_KEY is required for vision scene analysis.")
        from google import genai

        self._client = genai.Client(api_key=config.GOOGLE_API_KEY)

    def _chat_json(
        self,
        *,
        image_path: Path,
        model_id: str,
        instructions: str,
        user_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        from google.genai import types

        payload = user_payload or {}
        mime_type = mimetypes.guess_type(image_path.name)[0] or "image/png"
        image_bytes = image_path.read_bytes()
        model_candidates: list[str] = []
        for candidate in (
            str(model_id or "").strip(),
            str(config.GOOGLE_FRONTIER or "").strip(),
            "gemini-2.5-pro",
        ):
            if candidate and candidate not in model_candidates:
                model_candidates.append(candidate)
        for candidate_model in model_candidates:
            try:
                response = self._client.models.generate_content(
                    model=candidate_model,
                    contents=[
                        types.Part.from_text(text=instructions),
                        types.Part.from_text(text=json.dumps(payload, ensure_ascii=True)),
                        types.Part.from_bytes(data=image_bytes, mime_type=mime_type),
                    ],
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.2,
                    ),
                )
                raw = str(getattr(response, "text", "") or "").strip()
                if not raw:
                    candidates = response.candidates if response and isinstance(response.candidates, list) else []
                    text_parts: list[str] = []
                    for candidate in candidates:
                        content = candidate.content if candidate is not None else None
                        parts = content.parts if content is not None and isinstance(content.parts, list) else []
                        for part in parts:
                            part_text = str(getattr(part, "text", "") or "").strip()
                            if part_text:
                                text_parts.append(part_text)
                    raw = "\n".join(text_parts).strip()
                if not raw:
                    continue
                parsed = _parse_json_object_text(raw)
                if parsed:
                    return parsed
            except Exception as exc:
                logger.warning("Google vision scene call failed (model=%s): %s", candidate_model, exc)
        return {}

    def analyze_image(
        self,
        *,
        image_path: Path,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "Analyze this image for storyboard retrieval. Return JSON with keys exactly: "
            "caption, subjects, actions, setting, camera_angle, shot_type, lighting, "
            "mood, product_visibility, text_present, style_tags, attention_hooks, quality_issues."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={"idempotency_key": idempotency_key},
        )
        if not result:
            result = _default_image_analysis(image_path)
        result["provider"] = "google_vision"
        return result

    def score_scene_match(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "Score image fit to scene intent for short-form start-frame selection. "
            "Return JSON keys exactly: score_1_to_10, reason_short, fit_subject, fit_action, "
            "fit_emotion, fit_composition, consistency_with_style_profile, edit_recommended. "
            "All score fields are integers 1-10."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={
                "idempotency_key": idempotency_key,
                "scene_intent": scene_intent,
                "style_profile": style_profile,
            },
        )
        if not result:
            result = _build_simple_score(
                scene_intent=scene_intent,
                image_analysis=_default_image_analysis(image_path),
            )
        result["provider"] = "google_vision"
        return result

    def compose_transform_prompt(
        self,
        *,
        image_path: Path,
        scene_intent: dict[str, Any],
        style_profile: dict[str, Any],
        image_analysis: dict[str, Any] | None,
        model_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        instructions = (
            "You are an elite short-form creative director. "
            "Look at the source image and generate one high-impact image-edit prompt. "
            "The goal is a clearly transformed frame that matches the scene intent, not a tiny tweak. "
            "Return JSON keys exactly: edit_prompt, change_summary. "
            "edit_prompt must be plain text for an image-to-image model. "
            "Keep identity/product consistency, vertical 9:16 framing, and no unrelated text/logos."
        )
        result = self._chat_json(
            image_path=image_path,
            model_id=model_id,
            instructions=instructions,
            user_payload={
                "idempotency_key": idempotency_key,
                "scene_intent": scene_intent,
                "style_profile": style_profile,
                "image_analysis": image_analysis or {},
            },
        )
        prompt_text = str(result.get("edit_prompt") if isinstance(result, dict) else "").strip()
        if not prompt_text:
            prompt_text = _default_transform_prompt(
                scene_intent=scene_intent,
                style_profile=style_profile,
                image_analysis=image_analysis,
            )
        return {
            "edit_prompt": prompt_text,
            "change_summary": str(result.get("change_summary") if isinstance(result, dict) else "").strip(),
            "provider": "google_vision",
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
    google_key = str(getattr(config, "GOOGLE_API_KEY", "")).strip()

    gemini_provider: GeminiImageEditProvider = MockGeminiImageEditProvider()
    if google_key:
        try:
            gemini_provider = GoogleGeminiImageEditProvider()
        except Exception as exc:
            logger.warning("Gemini image edit unavailable; trying OpenAI image edit provider: %s", exc)
            if openai_key:
                try:
                    gemini_provider = OpenAIImageEditProvider()
                except Exception as openai_exc:
                    logger.warning("OpenAI image edit unavailable; using mock image edit provider: %s", openai_exc)
    elif openai_key:
        try:
            gemini_provider = OpenAIImageEditProvider()
        except Exception as exc:
            logger.warning("OpenAI image edit unavailable; using mock image edit provider: %s", exc)

    # Auto-upgrade to real providers when credentials are available.
    if fal_key and openai_key:
        return OpenAITTSProvider(), FalClientVideoProvider(fal_key=fal_key), gemini_provider

    # Keep local tests/dev deterministic when API credentials are absent.
    return MockTTSProvider(), MockFalVideoProvider(), gemini_provider


def build_vision_scene_provider(preferred_provider: str = "") -> VisionSceneProvider:
    force_mock = _is_truthy(os.getenv("PHASE4_V1_FORCE_MOCK_GENERATION", ""), default=False)
    if force_mock:
        return MockVisionSceneProvider()
    provider_key = str(preferred_provider or "").strip().lower()

    def _try_openai() -> VisionSceneProvider | None:
        if not str(config.OPENAI_API_KEY or "").strip():
            return None
        try:
            return OpenAIVisionSceneProvider()
        except Exception as exc:
            logger.warning("OpenAI vision unavailable: %s", exc)
            return None

    def _try_anthropic() -> VisionSceneProvider | None:
        if not str(config.ANTHROPIC_API_KEY or "").strip():
            return None
        try:
            return AnthropicVisionSceneProvider()
        except Exception as exc:
            logger.warning("Anthropic vision unavailable: %s", exc)
            return None

    def _try_google() -> VisionSceneProvider | None:
        if not str(config.GOOGLE_API_KEY or "").strip():
            return None
        try:
            return GoogleVisionSceneProvider()
        except Exception as exc:
            logger.warning("Google vision unavailable: %s", exc)
            return None

    preferred_chain: list[Callable[[], VisionSceneProvider | None]]
    if provider_key == "anthropic":
        preferred_chain = [_try_anthropic, _try_openai, _try_google]
    elif provider_key == "google":
        preferred_chain = [_try_google, _try_openai, _try_anthropic]
    elif provider_key == "openai":
        preferred_chain = [_try_openai, _try_anthropic, _try_google]
    else:
        preferred_chain = [_try_openai, _try_anthropic, _try_google]

    for builder in preferred_chain:
        provider = builder()
        if provider is not None:
            return provider
    return MockVisionSceneProvider()
