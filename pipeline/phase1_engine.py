"""Phase 1 v2 engine: hybrid DAG research with hard quality gates."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

import config
from pipeline.llm import call_llm_structured
from pipeline.phase1_adjudicate import adjudicate_pillars
from pipeline.phase1_collect_claude import collect_with_claude
from pipeline.phase1_collect_gemini import collect_with_gemini
from pipeline.phase1_collect_voc import collect_with_voc_api
from pipeline.phase1_contradiction import apply_contradiction_flags, detect_contradictions
from pipeline.phase1_evidence import dedupe_evidence, extract_seed_evidence, is_valid_http_url
from pipeline.phase1_hardening import harden_adjudicated_output
from pipeline.phase1_quality_gates import evaluate_quality_gates
from pipeline.phase1_synthesize_pillars import synthesize_pillars_dag
from pipeline.phase1_text_filters import is_malformed_quote
from schemas.foundation_research import (
    ContradictionReport,
    EvidenceItem,
    FoundationResearchBriefV2,
    ResearchModelTraceEntry,
    RetryAuditEntry,
    VocQuote,
)

logger = logging.getLogger(__name__)


class GapFillOutput(BaseModel):
    additional_evidence: list[EvidenceItem] = Field(default_factory=list)
    notes: str = ""


class _CollectorCheckpoint(BaseModel):
    schema_version: str = "1"
    created_at: str
    context_hash: str
    collector_reports: list[str] = Field(default_factory=list)
    collector_results: list[dict[str, Any]] = Field(default_factory=list)
    evidence: list[EvidenceItem] = Field(default_factory=list)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _prompt(path_name: str) -> str:
    path = Path(config.ROOT_DIR) / "prompts" / "phase1" / path_name
    return path.read_text("utf-8").strip()


def _build_context(inputs: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "brand_name",
        "product_name",
        "product_description",
        "target_market",
        "price_point",
        "niche",
        "website_url",
        "compliance_category",
        "key_differentiators",
        "customer_reviews",
        "competitor_info",
        "landing_page_info",
        "website_intel",
        "previous_performance",
        "additional_context",
    ]
    context = {k: inputs.get(k) for k in keys if inputs.get(k) not in (None, "", [], {})}
    context.setdefault("brand_name", "Unknown Brand")
    context.setdefault("product_name", "Unknown Product")
    context["context_date"] = date.today().isoformat()
    return context


def _persist_support_artifacts(
    *,
    output_dir: Path,
    evidence: list[EvidenceItem],
    quality_report: dict[str, Any],
    trace: list[ResearchModelTraceEntry],
    contradictions: list[ContradictionReport] | None = None,
    evidence_summary: dict[str, Any] | None = None,
    retry_audit: list[RetryAuditEntry] | None = None,
):
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "foundation_research_evidence_ledger.json").write_text(
        json.dumps([e.model_dump() for e in evidence], indent=2, default=str),
        "utf-8",
    )
    (output_dir / "foundation_research_quality_report.json").write_text(
        json.dumps(quality_report, indent=2, default=str),
        "utf-8",
    )
    (output_dir / "foundation_research_trace.json").write_text(
        json.dumps([t.model_dump() for t in trace], indent=2, default=str),
        "utf-8",
    )
    (output_dir / "foundation_research_contradictions.json").write_text(
        json.dumps([c.model_dump() for c in (contradictions or [])], indent=2, default=str),
        "utf-8",
    )
    (output_dir / "foundation_research_retry_audit.json").write_text(
        json.dumps([r.model_dump() for r in (retry_audit or [])], indent=2, default=str),
        "utf-8",
    )
    if evidence_summary is not None:
        (output_dir / "foundation_research_evidence_summary.json").write_text(
            json.dumps(evidence_summary, indent=2, default=str),
            "utf-8",
        )


def _checkpoint_path(output_dir: Path) -> Path:
    return output_dir / "phase1_collector_checkpoint.json"


def _context_hash(context: dict[str, Any]) -> str:
    payload = json.dumps(context, sort_keys=True, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _load_collector_checkpoint(*, output_dir: Path, context_hash: str) -> _CollectorCheckpoint | None:
    if not config.PHASE1_ENABLE_CHECKPOINTS or not config.PHASE1_REUSE_COLLECTOR_CHECKPOINT:
        return None
    if config.PHASE1_FORCE_FRESH_COLLECTORS:
        return None

    path = _checkpoint_path(output_dir)
    if not path.exists():
        return None

    try:
        data = json.loads(path.read_text("utf-8"))
        checkpoint = _CollectorCheckpoint.model_validate(data)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Phase1 checkpoint: unreadable; ignoring (%s)", exc)
        return None

    if checkpoint.context_hash != context_hash:
        logger.info("Phase1 checkpoint: context changed, not reusing collector cache")
        return None

    try:
        created = datetime.fromisoformat(checkpoint.created_at)
    except ValueError:
        return None

    ttl = max(1, int(config.PHASE1_CHECKPOINT_TTL_HOURS))
    if datetime.now(timezone.utc) - created > timedelta(hours=ttl):
        logger.info("Phase1 checkpoint: cache expired (ttl=%sh), not reused", ttl)
        return None

    logger.info(
        "Phase1 checkpoint: reusing cached collectors (%d reports, %d evidence)",
        len(checkpoint.collector_reports),
        len(checkpoint.evidence),
    )
    return checkpoint


def _save_collector_checkpoint(
    *,
    output_dir: Path,
    context_hash: str,
    collector_reports: list[str],
    collector_results: list[dict[str, Any]],
    evidence: list[EvidenceItem],
):
    if not config.PHASE1_ENABLE_CHECKPOINTS:
        return
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = _CollectorCheckpoint(
        created_at=_now_iso(),
        context_hash=context_hash,
        collector_reports=collector_reports,
        collector_results=collector_results,
        evidence=evidence,
    )
    _checkpoint_path(output_dir).write_text(
        json.dumps(checkpoint.model_dump(), indent=2, default=str),
        "utf-8",
    )
    logger.info(
        "Phase1 checkpoint: saved collector cache (%d reports, %d evidence)",
        len(collector_reports),
        len(evidence),
    )


def _failed_gates_to_pillars(failed_gate_ids: list[str]) -> set[str]:
    mapping = {
        "global_evidence_coverage": {"pillar_1", "pillar_2", "pillar_3", "pillar_4", "pillar_7"},
        "source_contradiction_audit": {"pillar_1", "pillar_2", "pillar_3", "pillar_4", "pillar_5", "pillar_6", "pillar_7"},
        "pillar_1_profile_completeness": {"pillar_1", "pillar_5"},
        "pillar_2_voc_depth": {"pillar_2", "pillar_6", "pillar_5"},
        "pillar_2_segment_alignment": {"pillar_1", "pillar_2"},
        "pillar_3_competitive_depth": {"pillar_3", "pillar_5"},
        "pillar_4_mechanism_strength": {"pillar_4", "pillar_5"},
        "pillar_5_awareness_validity": {"pillar_5"},
        "pillar_6_emotion_dominance": {"pillar_6"},
        "pillar_7_proof_coverage": {"pillar_7"},
        "cross_pillar_consistency": {"pillar_2", "pillar_3", "pillar_4", "pillar_5", "pillar_6"},
    }
    targets: set[str] = set()
    for gate in failed_gate_ids:
        targets.update(mapping.get(gate, set()))
    return targets or {"pillar_1", "pillar_2", "pillar_3", "pillar_4", "pillar_5", "pillar_6", "pillar_7"}


def _sanitize_voc_quotes(quotes: list[VocQuote]) -> list[VocQuote]:
    cleaned: list[VocQuote] = []
    seen: set[str] = set()
    for quote in quotes:
        q_text = (quote.quote or "").strip()
        source_type = (quote.source_type or "").strip().lower()
        has_url = is_valid_http_url(quote.source_url)
        if not q_text:
            continue
        if is_malformed_quote(q_text):
            continue
        # P0 authenticity rule: no unsourced/opaque VOC rows.
        if (not has_url) or source_type == "other":
            continue
        dedupe_key = f"{q_text.lower()}|{quote.source_url.strip().lower()}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        cleaned.append(quote)
    return cleaned


def _gap_fill_task_brief(quality_report: Any | None) -> list[str]:
    if quality_report is None:
        return []
    tasks: list[str] = []
    failed = set(quality_report.failed_gate_ids or [])
    if "global_evidence_coverage" in failed:
        tasks.append(
            "Increase normalized evidence coverage to >=300 items with >=6 source types; exclude headers/scaffold text."
        )
    if "pillar_2_voc_depth" in failed:
        tasks.append(
            "Add VOC quotes with real URLs only until thresholds are met; target Amazon/Reddit/Meta Forums/Steam/YouTube comments/Trustpilot."
        )
    if "pillar_2_segment_alignment" in failed:
        tasks.append(
            "Keep Pillar 2 segment labels strictly aligned to Pillar 1 segment names (or leave blank when uncertain)."
        )
    if "pillar_3_competitive_depth" in failed:
        floor = max(1, int(getattr(config, "PHASE1_MIN_COMPETITORS_FLOOR", config.PHASE1_MIN_COMPETITORS)))
        target = max(floor, int(getattr(config, "PHASE1_TARGET_COMPETITORS", config.PHASE1_MIN_COMPETITORS)))
        tasks.append(
            f"Expand direct competitor profiles to >={floor} with full fields (target {target}) and include substitutes >=3."
        )
    if "pillar_4_mechanism_strength" in failed:
        tasks.append(
            "Add third-party mechanism validation evidence until mechanism support IDs >=10."
        )
    if "pillar_5_awareness_validity" in failed:
        tasks.append(
            "Increase support evidence IDs per segment to >=5 and maintain awareness distribution sums at 1.0±0.05."
        )
    if "pillar_6_emotion_dominance" in failed:
        tasks.append(
            "Improve VOC emotional coverage so at least 5 dominant emotions have count >=8 and share >=0.05."
        )
    if "pillar_7_proof_coverage" in failed:
        tasks.append(
            "Add proof assets so each type has >=2 assets including >=1 top-tier, with testimonial coverage >=2."
        )
    if "cross_pillar_consistency" in failed:
        tasks.append(
            "Fix cross-pillar mismatches: objections<->VOC, mechanism<->competition, emotions<->VOC traceability."
        )
    return tasks


def _allowed_pillar1_segments(pillar_1: Any) -> list[str]:
    profiles = getattr(pillar_1, "segment_profiles", [])
    if not isinstance(profiles, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for profile in profiles:
        if isinstance(profile, dict):
            segment_name = str(profile.get("segment_name") or "").strip()
        else:
            segment_name = str(getattr(profile, "segment_name", "") or "").strip()
        if not segment_name:
            continue
        key = " ".join(segment_name.lower().split())
        if key in seen:
            continue
        seen.add(key)
        out.append(segment_name)
    return out


def _build_evidence_summary(evidence: list[EvidenceItem]) -> dict[str, Any]:
    provider_counts = Counter((item.provider or "").strip() or "unknown" for item in evidence)
    source_counts = Counter((item.source_type or "").strip() or "unknown" for item in evidence)
    conflict_counts = Counter((item.conflict_flag or "").strip() or "none" for item in evidence)
    top_claims = sorted(evidence, key=lambda item: item.confidence, reverse=True)[:20]
    return {
        "evidence_count": len(evidence),
        "provider_distribution": dict(sorted(provider_counts.items())),
        "source_type_distribution": dict(sorted(source_counts.items())),
        "conflict_flag_distribution": dict(sorted(conflict_counts.items())),
        "top_claims": [
            {
                "evidence_id": item.evidence_id,
                "confidence": item.confidence,
                "source_type": item.source_type,
                "provider": item.provider,
                "claim": item.claim,
                "source_url": item.source_url,
            }
            for item in top_claims
        ],
    }


def _log_provider_asymmetry(evidence: list[EvidenceItem]) -> None:
    if not evidence:
        return
    provider_counts = Counter(item.provider for item in evidence)
    total = len(evidence)
    dominant, dominant_count = provider_counts.most_common(1)[0]
    share = dominant_count / max(total, 1)
    if share >= 0.80:
        logger.warning(
            "Phase1 evidence asymmetry: provider '%s' contributes %.1f%% (%d/%d items)",
            dominant,
            share * 100,
            dominant_count,
            total,
        )


def _build_targeted_collector_prompt(
    *,
    context: dict[str, Any],
    failed_gate_ids: list[str],
    task_brief: list[str],
    evidence: list[EvidenceItem],
    allowed_segments: list[str] | None = None,
) -> str:
    template = _prompt("collector_targeted.md")
    allowed_segment_list = [
        str(v or "").strip()
        for v in (allowed_segments or [])
        if str(v or "").strip()
    ]
    allowed_segment_payload = json.dumps(allowed_segment_list, indent=2, default=str)
    return (
        f"{template}\n\n"
        "Failed gates:\n"
        f"{json.dumps(failed_gate_ids, indent=2, default=str)}\n\n"
        "Allowed Pillar 1 segments (strict set for Pillar 2 labels):\n"
        f"{allowed_segment_payload}\n\n"
        "Targeted tasks:\n"
        f"{json.dumps(task_brief, indent=2, default=str)}\n\n"
        "Context:\n"
        f"{json.dumps(context, indent=2, default=str)}\n\n"
        "Existing evidence sample:\n"
        f"{json.dumps([e.model_dump() for e in evidence[:500]], indent=2, default=str)}\n"
    )


def _available_retry_collectors() -> dict[str, Any]:
    collectors: dict[str, Any] = {}
    if config.PHASE1_ENABLE_CLAUDE_SCOUT:
        collectors["claude"] = collect_with_claude
    if config.PHASE1_ENABLE_GEMINI_RESEARCH:
        collectors["gemini"] = collect_with_gemini
    if config.PHASE1_ENABLE_VOC_COLLECTOR:
        collectors["voc_api"] = collect_with_voc_api
    return collectors


def _select_targeted_collector(failed_gate_ids: list[str]) -> str:
    failed = set(failed_gate_ids or [])
    collectors = _available_retry_collectors()
    if not collectors:
        return ""

    voc_like = {"pillar_2_voc_depth", "pillar_6_emotion_dominance"}
    contradiction_like = {"source_contradiction_audit"}

    if failed & contradiction_like:
        preference = ("claude", "gemini", "voc_api")
    elif failed & voc_like:
        preference = ("voc_api", "claude", "gemini")
    else:
        preference = ("claude", "gemini", "voc_api")

    for candidate in preference:
        if candidate in collectors:
            return candidate
    return next(iter(collectors.keys()))


def _retry_status(
    *,
    failed_before: list[str],
    failed_after: list[str],
    collector_failed: bool,
) -> str:
    if collector_failed:
        return "collector_failed"
    if not failed_after:
        return "resolved"
    before_set = set(failed_before or [])
    after_set = set(failed_after or [])
    if len(after_set) < len(before_set):
        return "improved"
    return "unchanged"


def _run_targeted_recollection(
    *,
    context: dict[str, Any],
    failed_gate_ids: list[str],
    task_brief: list[str],
    evidence: list[EvidenceItem],
    allowed_segments: list[str] | None = None,
    selected_collector: str = "",
) -> tuple[list[EvidenceItem], list[ResearchModelTraceEntry]]:
    prompt = _build_targeted_collector_prompt(
        context=context,
        failed_gate_ids=failed_gate_ids,
        task_brief=task_brief,
        evidence=evidence,
        allowed_segments=allowed_segments,
    )

    traces: list[ResearchModelTraceEntry] = []
    collected: list[EvidenceItem] = []

    collectors = _available_retry_collectors()
    if not collectors:
        return [], traces
    collector_name = selected_collector if selected_collector in collectors else next(iter(collectors.keys()))
    collector_fn = collectors[collector_name]

    targeted_context = dict(context)
    allowed_segment_list = [
        str(v or "").strip()
        for v in (allowed_segments or [])
        if str(v or "").strip()
    ]
    targeted_context["allowed_pillar1_segments"] = allowed_segment_list
    targeted_context["additional_context"] = (
        f"{context.get('additional_context', '')}\n\n"
        f"TARGETED_COLLECTION_PROMPT:\n{prompt}"
    ).strip()

    logger.info(
        "Phase1 targeted recollection: collector=%s failed_gates=%s",
        collector_name,
        sorted(set(failed_gate_ids or [])),
    )
    try:
        result = collector_fn(targeted_context)
    except Exception as exc:
        traces.append(
            ResearchModelTraceEntry(
                stage="collector",
                provider=collector_name,
                model="targeted-recollection",
                status="failed",
                started_at=_now_iso(),
                finished_at=_now_iso(),
                duration_seconds=0.0,
                notes=str(exc),
            )
        )
        return [], traces

    trace = result.get("trace")
    if trace:
        traces.append(trace)
    payload = result.get("evidence", [])
    if isinstance(payload, list) and payload:
        for row in payload:
            try:
                collected.append(EvidenceItem.model_validate(row))
            except Exception:
                continue
    report = result.get("report", "")
    if report:
        provider_name = result.get("provider", "unknown")
        collected.extend(
            extract_seed_evidence(
                report,
                provider=provider_name,
                pillar_tags=list(_failed_gates_to_pillars(failed_gate_ids)),
            )
        )

    logger.info(
        "Phase1 targeted recollection complete: collector=%s evidence_items=%d",
        collector_name,
        len(collected),
    )
    return dedupe_evidence(collected), traces


def _run_gap_fill(
    *,
    context: dict[str, Any],
    failed_gate_ids: list[str],
    failed_gate_checks: list[dict[str, Any]],
    task_brief: list[str],
    existing_evidence: list[EvidenceItem],
    allowed_segments: list[str] | None = None,
    provider: str,
    model: str,
    temperature: float,
    max_tokens: int,
) -> GapFillOutput:
    allowed_segment_list = [
        str(v or "").strip()
        for v in (allowed_segments or [])
        if str(v or "").strip()
    ]
    user_prompt = (
        f"Failed gates: {failed_gate_ids}\n\n"
        "Allowed Pillar 1 segments (strict set for Pillar 2 segment_name):\n"
        f"{json.dumps(allowed_segment_list, indent=2, default=str)}\n\n"
        f"Targeted tasks:\n{json.dumps(task_brief, indent=2, default=str)}\n\n"
        "Failed gate details:\n"
        f"{json.dumps(failed_gate_checks, indent=2, default=str)}\n\n"
        "Context:\n"
        f"{json.dumps(context, indent=2, default=str)}\n\n"
        "Existing evidence ledger (partial):\n"
        f"{json.dumps([e.model_dump() for e in existing_evidence[:800]], indent=2, default=str)}\n\n"
        "Important: do not duplicate evidence IDs/claims already present. Add only net-new, source-backed evidence."
    )
    return call_llm_structured(
        system_prompt=_prompt("gap_fill.md"),
        user_prompt=user_prompt,
        response_model=GapFillOutput,
        provider=provider,
        model=model,
        temperature=temperature,
        max_tokens=max(3000, min(max_tokens, 16000)),
    )


def run_phase1_collectors_only(
    *,
    inputs: dict[str, Any],
    provider: str,
    model: str,
    temperature: float,
    max_tokens: int,
    output_dir: Path,
) -> dict[str, Any]:
    """Run only the collector stage and persist a review snapshot artifact.

    This is used for manual review gating before synthesis/adjudication starts.
    """
    start_ts = time.time()
    trace: list[ResearchModelTraceEntry] = []
    context = _build_context(inputs)

    checkpoint_hash = _context_hash(context)
    checkpoint = _load_collector_checkpoint(output_dir=output_dir, context_hash=checkpoint_hash)

    collector_reports: list[str] = []
    evidence: list[EvidenceItem] = []
    collector_results: list[dict[str, Any]] = []

    if checkpoint is not None:
        collector_reports = [r for r in checkpoint.collector_reports if r]
        evidence = dedupe_evidence(checkpoint.evidence)
        collector_results = checkpoint.collector_results
        trace.append(
            ResearchModelTraceEntry(
                stage="collector",
                provider="internal",
                model="checkpoint-cache",
                status="success",
                started_at=_now_iso(),
                finished_at=_now_iso(),
                duration_seconds=0.0,
                notes=(
                    f"reused_collectors={len(collector_reports)}; "
                    f"cached_evidence={len(evidence)}; ttl_hours={config.PHASE1_CHECKPOINT_TTL_HOURS}"
                ),
            )
        )
    else:
        collector_calls = []
        if config.PHASE1_ENABLE_GEMINI_RESEARCH:
            collector_calls.append(("gemini", collect_with_gemini))
        if config.PHASE1_ENABLE_CLAUDE_SCOUT:
            collector_calls.append(("claude", collect_with_claude))
        if config.PHASE1_ENABLE_VOC_COLLECTOR:
            collector_calls.append(("voc_api", collect_with_voc_api))
        if not collector_calls:
            raise RuntimeError("Phase 1 collectors are disabled by config")

        logger.info("Phase1 collectors-only: dispatching collectors=%s", [name for name, _ in collector_calls])
        with ThreadPoolExecutor(max_workers=len(collector_calls)) as pool:
            futures = {}
            for name, fn in collector_calls:
                logger.info("Phase1 collectors-only: collector[%s] start", name)
                futures[pool.submit(fn, context)] = name
            for future in as_completed(futures):
                collector_name = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "success": False,
                        "provider": collector_name,
                        "report": "",
                        "error": str(exc),
                        "trace": ResearchModelTraceEntry(
                            stage="collector",
                            provider=collector_name,
                            model="unknown",
                            status="failed",
                            started_at=_now_iso(),
                            finished_at=_now_iso(),
                            duration_seconds=0.0,
                            notes=str(exc),
                        ),
                    }
                collector_results.append(result)
                if result.get("trace"):
                    trace.append(result["trace"])

        successful = [row for row in collector_results if row.get("success")]
        if not successful:
            raise RuntimeError("Phase 1 failed: all collectors failed")

        for result in successful:
            provider_name = result.get("provider", "unknown")
            report = (result.get("report") or "").strip()
            payload = result.get("evidence", [])

            if isinstance(payload, list):
                for row in payload:
                    try:
                        evidence.append(EvidenceItem.model_validate(row))
                    except Exception:
                        continue

            if report:
                collector_reports.append(report)
                evidence.extend(
                    extract_seed_evidence(
                        report,
                        provider=provider_name,
                        pillar_tags=[
                            "pillar_1",
                            "pillar_2",
                            "pillar_3",
                            "pillar_4",
                            "pillar_5",
                            "pillar_6",
                            "pillar_7",
                        ],
                    )
                )

        evidence = dedupe_evidence(evidence)
        _save_collector_checkpoint(
            output_dir=output_dir,
            context_hash=checkpoint_hash,
            collector_reports=collector_reports,
            collector_results=successful,
            evidence=evidence,
        )

    _log_provider_asymmetry(evidence)
    evidence_summary = _build_evidence_summary(evidence)
    runtime_seconds = round(time.time() - start_ts, 2)

    collector_summary: list[dict[str, Any]] = []
    collector_reports_labeled: list[dict[str, Any]] = []
    for result in collector_results:
        payload = result.get("evidence", [])
        report_text = (result.get("report") or "").strip()
        provider_name = result.get("provider", "unknown")
        collector_summary.append(
            {
                "provider": provider_name,
                "success": bool(result.get("success")),
                "report_chars": len(report_text),
                "evidence_rows": len(payload) if isinstance(payload, list) else 0,
                "error": (result.get("error", "") or "")[:400],
            }
        )
        if report_text:
            collector_reports_labeled.append(
                {
                    "label": f"{provider_name} report",
                    "provider": provider_name,
                    "report_chars": len(report_text),
                    # Keep full report text for Step 1 review UI so users can inspect the entire output.
                    "report_preview": report_text,
                }
            )

    snapshot = {
        "brand_name": context.get("brand_name", "Unknown Brand"),
        "product_name": context.get("product_name", "Unknown Product"),
        "generated_date": date.today().isoformat(),
        "schema_version": "2.0-collectors-preview",
        "stage": "collectors_complete",
        "phase1_runtime_seconds": runtime_seconds,
        "collector_count": len(collector_summary),
        "collector_summary": collector_summary,
        "collector_reports": collector_reports_labeled,
        "collector_report_previews": [report[:12000] for report in collector_reports],
        "evidence_count": len(evidence),
        "evidence_summary": evidence_summary,
        "evidence_sample": [item.model_dump() for item in evidence[:120]],
        "research_model_trace": [item.model_dump() for item in trace],
        "artifact_paths": {
            "collector_checkpoint": str(_checkpoint_path(output_dir)),
            "collectors_snapshot": str(output_dir / "foundation_research_collectors_snapshot.json"),
        },
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "foundation_research_collectors_snapshot.json").write_text(
        json.dumps(snapshot, indent=2, default=str),
        "utf-8",
    )

    return snapshot


def run_phase1_engine(
    *,
    inputs: dict[str, Any],
    provider: str,
    model: str,
    temperature: float,
    max_tokens: int,
    output_dir: Path,
) -> FoundationResearchBriefV2:
    start_ts = time.time()
    trace: list[ResearchModelTraceEntry] = []
    context = _build_context(inputs)
    gap_fill_mode = (config.PHASE1_GAP_FILL_MODE or "targeted_collectors").strip().lower()
    retry_strategy = (config.PHASE1_RETRY_STRATEGY or "single_focused_collector").strip().lower()
    configured_rounds = (
        int(config.PHASE1_TARGETED_COLLECTOR_MAX_ROUNDS)
        if gap_fill_mode == "targeted_collectors"
        else int(config.PHASE1_GAP_FILL_ROUNDS)
    )
    if retry_strategy == "single_focused_collector" and config.PHASE1_RETRY_ESCALATION_MODE != "none":
        logger.info(
            "Phase1 engine: retry escalation mode '%s' ignored because retry_strategy=single_focused_collector",
            config.PHASE1_RETRY_ESCALATION_MODE,
        )
    if config.PHASE1_ENFORCE_SINGLE_RETRY:
        configured_rounds = min(configured_rounds, max(0, int(config.PHASE1_RETRY_ROUNDS_MAX)))
    logger.info(
        "Phase1 engine: start (brand=%s, product=%s, hard_block=%s, gap_fill_mode=%s, retry_strategy=%s, retry_rounds=%d)",
        context.get("brand_name", ""),
        context.get("product_name", ""),
        config.PHASE1_STRICT_HARD_BLOCK,
        gap_fill_mode,
        retry_strategy,
        configured_rounds,
    )

    checkpoint_hash = _context_hash(context)
    checkpoint = _load_collector_checkpoint(output_dir=output_dir, context_hash=checkpoint_hash)

    collector_reports: list[str] = []
    evidence: list[EvidenceItem] = []
    successful_collectors: list[dict[str, Any]] = []
    contradictions: list[ContradictionReport] = []
    evidence_summary: dict[str, Any] | None = None
    retry_audit: list[RetryAuditEntry] = []
    pending_retry_audit_idx: int | None = None

    if checkpoint is not None:
        collector_reports = [r for r in checkpoint.collector_reports if r]
        evidence = dedupe_evidence(checkpoint.evidence)
        successful_collectors = [r for r in checkpoint.collector_results if r.get("success")]
        trace.append(
            ResearchModelTraceEntry(
                stage="collector",
                provider="internal",
                model="checkpoint-cache",
                status="success",
                started_at=_now_iso(),
                finished_at=_now_iso(),
                duration_seconds=0.0,
                notes=(
                    f"reused_collectors={len(collector_reports)}; "
                    f"cached_evidence={len(evidence)}; ttl_hours={config.PHASE1_CHECKPOINT_TTL_HOURS}"
                ),
            )
        )
    else:
        collector_calls = []
        if config.PHASE1_ENABLE_GEMINI_RESEARCH:
            collector_calls.append(("gemini", collect_with_gemini))
        if config.PHASE1_ENABLE_CLAUDE_SCOUT:
            collector_calls.append(("claude", collect_with_claude))
        if config.PHASE1_ENABLE_VOC_COLLECTOR:
            collector_calls.append(("voc_api", collect_with_voc_api))

        if not collector_calls:
            raise RuntimeError("Phase 1 collectors are disabled by config")
        logger.info("Phase1 engine: collectors enabled=%s", [name for name, _ in collector_calls])

        collector_results: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=len(collector_calls)) as pool:
            futures = {}
            for name, fn in collector_calls:
                logger.info("Phase1 engine: collector[%s] start", name)
                futures[pool.submit(fn, context)] = name
            for future in as_completed(futures):
                collector_name = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = {
                        "success": False,
                        "provider": collector_name,
                        "report": "",
                        "error": str(exc),
                        "trace": ResearchModelTraceEntry(
                            stage="collector",
                            provider=collector_name,
                            model="unknown",
                            status="failed",
                            started_at=_now_iso(),
                            finished_at=_now_iso(),
                            duration_seconds=0.0,
                            notes=str(exc),
                        ),
                    }
                collector_results.append(result)
                if result.get("trace"):
                    trace.append(result["trace"])
                logger.info(
                    "Phase1 engine: collector[%s] status=%s report_chars=%d evidence_rows=%d error=%s",
                    collector_name,
                    "success" if result.get("success") else "failed",
                    len(result.get("report", "") or ""),
                    len(result.get("evidence", []) or []),
                    (result.get("error", "") or "none")[:240],
                )

        successful_collectors = [r for r in collector_results if r.get("success")]
        if not successful_collectors:
            _persist_support_artifacts(
                output_dir=output_dir,
                evidence=[],
                quality_report={"overall_pass": False, "failed_gate_ids": ["collectors_failed"]},
                trace=trace,
                contradictions=[],
                evidence_summary=None,
                retry_audit=retry_audit,
            )
            raise RuntimeError("Phase 1 failed: all collectors failed")

        for result in successful_collectors:
            provider_name = result.get("provider", "unknown")
            report = (result.get("report") or "").strip()
            payload = result.get("evidence", [])

            if isinstance(payload, list):
                for row in payload:
                    try:
                        evidence.append(EvidenceItem.model_validate(row))
                    except Exception:
                        continue

            if report:
                collector_reports.append(report)
                evidence.extend(
                    extract_seed_evidence(
                        report,
                        provider=provider_name,
                        pillar_tags=[
                            "pillar_1",
                            "pillar_2",
                            "pillar_3",
                            "pillar_4",
                            "pillar_5",
                            "pillar_6",
                            "pillar_7",
                        ],
                    )
                )

        evidence = dedupe_evidence(evidence)
        _save_collector_checkpoint(
            output_dir=output_dir,
            context_hash=checkpoint_hash,
            collector_reports=collector_reports,
            collector_results=successful_collectors,
            evidence=evidence,
        )

    logger.info(
        "Phase1 engine: evidence normalized (%d items, %d successful collectors)",
        len(evidence),
        len(successful_collectors),
    )
    _log_provider_asymmetry(evidence)

    if not successful_collectors and not collector_reports:
        _persist_support_artifacts(
            output_dir=output_dir,
            evidence=evidence,
            quality_report={"overall_pass": False, "failed_gate_ids": ["collectors_failed"]},
            trace=trace,
            contradictions=[],
            evidence_summary=(_build_evidence_summary(evidence) if config.PHASE1_ENABLE_EVIDENCE_SUMMARY else None),
            retry_audit=retry_audit,
        )
        raise RuntimeError("Phase 1 failed: no collector reports available")

    if config.PHASE1_ENABLE_CONTRADICTION_DETECTION:
        contradictions = detect_contradictions(
            evidence=evidence,
            provider=provider,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        evidence = apply_contradiction_flags(evidence, contradictions)
        high_unresolved = [c for c in contradictions if c.severity == "high" and not c.resolved]
        if high_unresolved:
            logger.warning(
                "Phase1 contradiction audit: unresolved high conflicts=%d",
                len(high_unresolved),
            )
        if config.PHASE1_STRICT_CONTRADICTION_BLOCK and high_unresolved:
            evidence_summary = _build_evidence_summary(evidence) if config.PHASE1_ENABLE_EVIDENCE_SUMMARY else None
            _persist_support_artifacts(
                output_dir=output_dir,
                evidence=evidence,
                quality_report={
                    "overall_pass": False,
                    "failed_gate_ids": ["source_contradiction_audit"],
                    "details": "strict contradiction block triggered before synthesis",
                },
                trace=trace,
                contradictions=contradictions,
                evidence_summary=evidence_summary,
                retry_audit=retry_audit,
            )
            raise RuntimeError(
                f"Phase 1 blocked: unresolved high-severity contradictions ({len(high_unresolved)})"
            )

    if config.PHASE1_ENABLE_EVIDENCE_SUMMARY:
        evidence_summary = _build_evidence_summary(evidence)

    pillars: dict[str, Any] = {}
    final_adjudicated = None
    final_quality = None
    final_warning_note = ""

    rounds = max(0, configured_rounds)
    for retry_round in range(rounds + 1):
        logger.info("Phase1 engine: synthesis round %d/%d", retry_round + 1, rounds + 1)
        s_start = _now_iso()
        t0 = time.time()

        target_pillars = None
        if retry_round > 0 and final_quality is not None:
            target_pillars = _failed_gates_to_pillars(final_quality.failed_gate_ids)

        pillars = synthesize_pillars_dag(
            context=context,
            evidence=evidence,
            collector_reports=collector_reports,
            provider=provider,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            target_pillars=target_pillars,
            existing_pillars=pillars,
        )
        trace.append(
            ResearchModelTraceEntry(
                stage="synthesis",
                provider=provider,
                model=model,
                status="success",
                started_at=s_start,
                finished_at=_now_iso(),
                duration_seconds=max(time.time() - t0, 0.0),
                notes=f"retry_round={retry_round}; target_pillars={sorted(target_pillars) if target_pillars else 'all'}",
            )
        )

        a_start = _now_iso()
        t1 = time.time()
        adjudicated = adjudicate_pillars(
            context=context,
            evidence=evidence,
            pillars=pillars,
            provider=provider,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        trace.append(
            ResearchModelTraceEntry(
                stage="adjudication",
                provider=provider,
                model=model,
                status="success",
                started_at=a_start,
                finished_at=_now_iso(),
                duration_seconds=max(time.time() - t1, 0.0),
                notes=f"retry_round={retry_round}",
            )
        )

        adjudicated.pillar_2_voc_language_bank.quotes = _sanitize_voc_quotes(
            adjudicated.pillar_2_voc_language_bank.quotes
        )
        harden_adjudicated_output(
            adjudicated,
            evidence,
            collector_reports=collector_reports,
        )

        q_start = _now_iso()
        t2 = time.time()
        quality = evaluate_quality_gates(
            evidence=evidence,
            pillar_1=adjudicated.pillar_1_prospect_profile,
            pillar_2=adjudicated.pillar_2_voc_language_bank,
            pillar_3=adjudicated.pillar_3_competitive_intelligence,
            pillar_4=adjudicated.pillar_4_product_mechanism_analysis,
            pillar_5=adjudicated.pillar_5_awareness_classification,
            pillar_6=adjudicated.pillar_6_emotional_driver_inventory,
            pillar_7=adjudicated.pillar_7_proof_credibility_inventory,
            cross_report=adjudicated.cross_pillar_consistency_report,
            retry_rounds_used=retry_round,
        )
        trace.append(
            ResearchModelTraceEntry(
                stage="quality_gate",
                provider="internal",
                model="rule-engine",
                status="success" if quality.overall_pass else "failed",
                started_at=q_start,
                finished_at=_now_iso(),
                duration_seconds=max(time.time() - t2, 0.0),
                notes=f"retry_round={retry_round}; failed={quality.failed_gate_ids}",
            )
        )

        final_adjudicated = adjudicated
        final_quality = quality
        if pending_retry_audit_idx is not None and pending_retry_audit_idx < len(retry_audit):
            entry = retry_audit[pending_retry_audit_idx]
            entry.failed_gate_ids_after = list(quality.failed_gate_ids or [])
            entry.status = _retry_status(
                failed_before=entry.failed_gate_ids_before,
                failed_after=entry.failed_gate_ids_after,
                collector_failed=(entry.status == "collector_failed"),
            )
            if entry.failed_gate_ids_after and config.PHASE1_WARN_ON_UNRESOLVED_GATES:
                entry.warning = (
                    "Focused retry did not fully resolve gates: "
                    + ", ".join(entry.failed_gate_ids_after)
                )
            pending_retry_audit_idx = None

        if quality.overall_pass:
            logger.info("Phase1 engine: quality gates passed (round=%d)", retry_round)
            break
        logger.warning(
            "Phase1 engine: quality gates failed (round=%d) failed=%s",
            retry_round,
            quality.failed_gate_ids,
        )
        for check in quality.checks:
            if check.passed:
                continue
            gate = check.gate_id
            required = (check.required or "").strip()
            actual = (check.actual or "").strip()
            details = (check.details or "").strip()
            msg = (
                "Phase1 gate detail: %s | required=%s | actual=%s"
                % (gate, required or "n/a", actual or "n/a")
            )
            if details:
                msg += f" | details={details[:220]}"
            logger.warning(msg)
        if retry_round >= rounds:
            if config.PHASE1_WARN_ON_UNRESOLVED_GATES:
                final_warning_note = (
                    "Quality gates unresolved after maximum retries: "
                    + ", ".join(quality.failed_gate_ids)
                )
                if retry_audit:
                    retry_audit[-1].warning = retry_audit[-1].warning or final_warning_note
                else:
                    retry_audit.append(
                        RetryAuditEntry(
                            round_index=retry_round,
                            failed_gate_ids_before=list(quality.failed_gate_ids),
                            selected_collector="none",
                            added_evidence_count=0,
                            failed_gate_ids_after=list(quality.failed_gate_ids),
                            status="unchanged",
                            warning=final_warning_note,
                        )
                    )
            break

        g_start = _now_iso()
        t3 = time.time()
        task_brief = _gap_fill_task_brief(quality)
        allowed_segments = _allowed_pillar1_segments(adjudicated.pillar_1_prospect_profile)
        added: list[EvidenceItem] = []
        selected_collector = ""
        collector_failed = False
        use_targeted_retry = (
            retry_strategy == "single_focused_collector"
            or gap_fill_mode == "targeted_collectors"
        )
        if use_targeted_retry:
            selected_collector = _select_targeted_collector(quality.failed_gate_ids)
            added, recollect_trace = _run_targeted_recollection(
                context=context,
                failed_gate_ids=quality.failed_gate_ids,
                task_brief=task_brief,
                evidence=evidence,
                allowed_segments=allowed_segments,
                selected_collector=selected_collector,
            )
            trace.extend(recollect_trace)
            collector_failed = any(
                (entry.stage == "collector" and entry.status == "failed")
                for entry in recollect_trace
            )
            stage_model = f"targeted-collector:{selected_collector or 'none'}"
        else:
            failed_checks = [check.model_dump() for check in quality.checks if not check.passed]
            gap = _run_gap_fill(
                context=context,
                failed_gate_ids=quality.failed_gate_ids,
                failed_gate_checks=failed_checks,
                task_brief=task_brief,
                existing_evidence=evidence,
                allowed_segments=allowed_segments,
                provider=provider,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            added = gap.additional_evidence
            stage_model = "legacy-gap-fill"
            selected_collector = "legacy-gap-fill"

        retry_entry = RetryAuditEntry(
            round_index=retry_round + 1,
            failed_gate_ids_before=list(quality.failed_gate_ids),
            selected_collector=selected_collector,
            added_evidence_count=len(added),
            failed_gate_ids_after=[],
            status="collector_failed" if collector_failed else "unchanged",
            warning=(
                "Focused collector failed; proceeding without synthetic evidence."
                if collector_failed
                else ""
            ),
        )
        retry_audit.append(retry_entry)
        pending_retry_audit_idx = len(retry_audit) - 1

        evidence = dedupe_evidence([*evidence, *added])
        _log_provider_asymmetry(evidence)

        if config.PHASE1_ENABLE_CONTRADICTION_DETECTION:
            contradictions = detect_contradictions(
                evidence=evidence,
                provider=provider,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            evidence = apply_contradiction_flags(evidence, contradictions)
            if config.PHASE1_STRICT_CONTRADICTION_BLOCK:
                high_unresolved = [c for c in contradictions if c.severity == "high" and not c.resolved]
                if high_unresolved:
                    evidence_summary = _build_evidence_summary(evidence) if config.PHASE1_ENABLE_EVIDENCE_SUMMARY else None
                    _persist_support_artifacts(
                        output_dir=output_dir,
                        evidence=evidence,
                        quality_report={
                            "overall_pass": False,
                            "failed_gate_ids": ["source_contradiction_audit"],
                            "details": "strict contradiction block triggered during retry",
                        },
                        trace=trace,
                        contradictions=contradictions,
                        evidence_summary=evidence_summary,
                        retry_audit=retry_audit,
                    )
                    raise RuntimeError(
                        f"Phase 1 blocked: unresolved high-severity contradictions ({len(high_unresolved)})"
                    )

        if config.PHASE1_ENABLE_EVIDENCE_SUMMARY:
            evidence_summary = _build_evidence_summary(evidence)

        logger.info(
            "Phase1 engine: recollection round %d mode=%s added=%d total=%d",
            retry_round + 1,
            gap_fill_mode,
            len(added),
            len(evidence),
        )
        trace.append(
            ResearchModelTraceEntry(
                stage="collector" if use_targeted_retry else "synthesis",
                provider=selected_collector or provider,
                model=stage_model,
                status="success",
                started_at=g_start,
                finished_at=_now_iso(),
                duration_seconds=max(time.time() - t3, 0.0),
                notes=(
                    f"retry_round={retry_round + 1}; added_evidence={len(added)}; "
                    f"mode={gap_fill_mode}; strategy={retry_strategy}; collector={selected_collector or 'n/a'}"
                ),
            )
        )

        if time.time() - start_ts > (config.PHASE1_MAX_RUNTIME_MINUTES * 60):
            logger.warning("Phase1 engine: max runtime reached; ending retries early")
            if pending_retry_audit_idx is not None and pending_retry_audit_idx < len(retry_audit):
                entry = retry_audit[pending_retry_audit_idx]
                entry.failed_gate_ids_after = list(entry.failed_gate_ids_before)
                entry.status = _retry_status(
                    failed_before=entry.failed_gate_ids_before,
                    failed_after=entry.failed_gate_ids_after,
                    collector_failed=(entry.status == "collector_failed"),
                )
                if config.PHASE1_WARN_ON_UNRESOLVED_GATES:
                    entry.warning = (
                        entry.warning
                        or "Retry evaluation was cut short by max runtime; unresolved gates remain."
                    )
                    final_warning_note = entry.warning
                pending_retry_audit_idx = None
            break

    runtime_seconds = round(time.time() - start_ts, 2)
    assert final_adjudicated is not None
    assert final_quality is not None

    if (not final_quality.overall_pass) and config.PHASE1_WARN_ON_UNRESOLVED_GATES and not final_warning_note:
        final_warning_note = (
            "Quality gates unresolved after focused retry policy: "
            + ", ".join(final_quality.failed_gate_ids)
        )
        if retry_audit:
            retry_audit[-1].warning = retry_audit[-1].warning or final_warning_note

    if config.PHASE1_ENABLE_EVIDENCE_SUMMARY and evidence_summary is None:
        evidence_summary = _build_evidence_summary(evidence)

    brief = FoundationResearchBriefV2(
        brand_name=context.get("brand_name", "Unknown Brand"),
        product_name=context.get("product_name", "Unknown Product"),
        generated_date=date.today().isoformat(),
        schema_version="2.0",
        phase1_runtime_seconds=runtime_seconds,
        research_model_trace=trace,
        pillar_1_prospect_profile=final_adjudicated.pillar_1_prospect_profile,
        pillar_2_voc_language_bank=final_adjudicated.pillar_2_voc_language_bank,
        pillar_3_competitive_intelligence=final_adjudicated.pillar_3_competitive_intelligence,
        pillar_4_product_mechanism_analysis=final_adjudicated.pillar_4_product_mechanism_analysis,
        pillar_5_awareness_classification=final_adjudicated.pillar_5_awareness_classification,
        pillar_6_emotional_driver_inventory=final_adjudicated.pillar_6_emotional_driver_inventory,
        pillar_7_proof_credibility_inventory=final_adjudicated.pillar_7_proof_credibility_inventory,
        evidence_ledger=evidence,
        contradictions=contradictions,
        retry_audit=retry_audit,
        quality_gate_report=final_quality,
        cross_pillar_consistency_report=final_adjudicated.cross_pillar_consistency_report,
    )

    quality_payload = final_quality.model_dump()
    if final_warning_note:
        quality_payload["warning"] = final_warning_note

    _persist_support_artifacts(
        output_dir=output_dir,
        evidence=evidence,
        quality_report=quality_payload,
        trace=trace,
        contradictions=contradictions,
        evidence_summary=evidence_summary,
        retry_audit=retry_audit,
    )
    logger.info(
        "Phase1 engine: artifacts saved (overall_pass=%s, evidence=%d, contradictions=%d, runtime=%.2fs)",
        final_quality.overall_pass,
        len(evidence),
        len(contradictions),
        runtime_seconds,
    )

    if config.PHASE1_STRICT_HARD_BLOCK and not final_quality.overall_pass:
        failed = ", ".join(final_quality.failed_gate_ids)
        raise RuntimeError(
            f"Phase 1 quality gates failed after {final_quality.retry_rounds_used} retries: {failed}"
        )
    if not config.PHASE1_STRICT_HARD_BLOCK and not final_quality.overall_pass:
        logger.warning(
            "Phase1 engine: soft mode active — returning output despite failed gates: %s | warning=%s",
            final_quality.failed_gate_ids,
            final_warning_note or "n/a",
        )

    return brief
