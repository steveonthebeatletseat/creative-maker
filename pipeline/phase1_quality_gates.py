"""Elite strict quality gates for Phase 1 v2."""

from __future__ import annotations

from collections import Counter, defaultdict

import config
from pipeline.phase1_evidence import is_valid_http_url
from pipeline.phase1_text_filters import is_malformed_quote
from schemas.foundation_research import (
    CrossPillarConsistencyReport,
    EvidenceItem,
    Pillar1ProspectProfile,
    Pillar2VocLanguageBank,
    Pillar3CompetitiveIntelligence,
    Pillar4ProductMechanismAnalysis,
    Pillar5AwarenessClassification,
    Pillar6EmotionalDriverInventory,
    Pillar7ProofCredibilityInventory,
    QualityGateCheck,
    QualityGateReport,
)


def _check(
    checks: list[QualityGateCheck],
    gate_id: str,
    passed: bool,
    required: str,
    actual: str,
    details: str = "",
):
    checks.append(
        QualityGateCheck(
            gate_id=gate_id,
            passed=passed,
            required=required,
            actual=actual,
            details=details,
        )
    )


def evaluate_quality_gates(
    *,
    evidence: list[EvidenceItem],
    pillar_1: Pillar1ProspectProfile,
    pillar_2: Pillar2VocLanguageBank,
    pillar_3: Pillar3CompetitiveIntelligence,
    pillar_4: Pillar4ProductMechanismAnalysis,
    pillar_5: Pillar5AwarenessClassification,
    pillar_6: Pillar6EmotionalDriverInventory,
    pillar_7: Pillar7ProofCredibilityInventory,
    cross_report: CrossPillarConsistencyReport,
    retry_rounds_used: int,
) -> QualityGateReport:
    checks: list[QualityGateCheck] = []

    def _normalize_segment_name(value: str) -> str:
        return " ".join(str(value or "").strip().lower().split())

    def _valid_voc_quote(quote) -> bool:
        source_type = (quote.source_type or "").strip().lower()
        has_url = is_valid_http_url(quote.source_url)
        has_text = bool((quote.quote or "").strip())
        is_other = source_type == "other"
        malformed = is_malformed_quote(quote.quote or "")
        return has_text and has_url and not is_other and not malformed

    # 1) Global evidence coverage
    source_types = {e.source_type for e in evidence if e.source_type}
    min_evidence_items = max(1, int(getattr(config, "PHASE1_MIN_EVIDENCE_ITEMS", 250)))
    _check(
        checks,
        "global_evidence_coverage",
        passed=(len(source_types) >= config.PHASE1_MIN_SOURCE_TYPES and len(evidence) >= min_evidence_items),
        required=f">={config.PHASE1_MIN_SOURCE_TYPES} source types and >={min_evidence_items} evidence items",
        actual=f"{len(source_types)} source types, {len(evidence)} evidence items",
    )

    # 1b) Source contradiction audit
    high_conflicts = [e.evidence_id for e in evidence if (e.conflict_flag or "").strip().lower() == "high_unresolved"]
    _check(
        checks,
        "source_contradiction_audit",
        passed=len(high_conflicts) == 0,
        required="no evidence items marked high_unresolved",
        actual=f"high_unresolved={len(high_conflicts)}",
        details=(f"sample_ids={high_conflicts[:20]}" if high_conflicts else ""),
    )

    # 2) Pillar 1
    p1_complete = True
    for seg in pillar_1.segment_profiles:
        if not (seg.goals and seg.pains and seg.triggers and seg.information_sources and seg.objections):
            p1_complete = False
            break
    p1_sources = {
        e.source_type
        for e in evidence
        if "pillar_1" in e.pillar_tags or "pillar_1_prospect_profile" in e.pillar_tags
    }
    _check(
        checks,
        "pillar_1_profile_completeness",
        passed=(p1_complete and len(p1_sources) >= config.PHASE1_MIN_SOURCE_TYPES),
        required=f"segment coverage complete and >= {config.PHASE1_MIN_SOURCE_TYPES} source types",
        actual=f"complete={p1_complete}, source_types={len(p1_sources)}",
    )

    # 3) Pillar 2
    valid_quotes = [q for q in pillar_2.quotes if _valid_voc_quote(q)]
    invalid_quotes = [q.quote_id for q in pillar_2.quotes if not _valid_voc_quote(q)]
    allowed_segment_keys = {
        _normalize_segment_name(seg.segment_name)
        for seg in pillar_1.segment_profiles
        if _normalize_segment_name(seg.segment_name)
    }
    invalid_segment_quotes = [
        q.quote_id
        for q in pillar_2.quotes
        if str(q.segment_name or "").strip()
        and _normalize_segment_name(q.segment_name) not in allowed_segment_keys
    ]
    category_counts = Counter(q.category for q in valid_quotes)
    p2_pass = (
        len(valid_quotes) >= config.PHASE1_MIN_VOC_QUOTES
        and category_counts.get("pain", 0) >= 20
        and category_counts.get("desire", 0) >= 20
        and category_counts.get("objection", 0) >= 20
        and category_counts.get("trigger", 0) >= 10
        and category_counts.get("proof", 0) >= 10
        and pillar_2.saturation_last_30_new_themes <= 3
        and len(invalid_quotes) == 0
        and len(invalid_segment_quotes) == 0
    )
    _check(
        checks,
        "pillar_2_voc_depth",
        passed=p2_pass,
        required=(
            "VOC(valid)>=150; pain/desire/objection>=20; trigger/proof>=10; saturation<=3; "
            "all VOC must have valid URL and source_type != other; segment labels must align to Pillar 1"
        ),
        actual=(
            f"voc_total={len(pillar_2.quotes)}, voc_valid={len(valid_quotes)}, "
            f"invalid={len(invalid_quotes)}, pain={category_counts.get('pain', 0)}, "
            f"desire={category_counts.get('desire', 0)}, objection={category_counts.get('objection', 0)}, "
            f"trigger={category_counts.get('trigger', 0)}, proof={category_counts.get('proof', 0)}, "
            f"sat={pillar_2.saturation_last_30_new_themes}, invalid_segments={len(invalid_segment_quotes)}"
        ),
        details=(
            " | ".join(
                token
                for token in [
                    f"invalid_quote_ids={invalid_quotes[:30]}" if invalid_quotes else "",
                    f"invalid_segment_quote_ids={invalid_segment_quotes[:30]}" if invalid_segment_quotes else "",
                ]
                if token
            )
        ),
    )

    _check(
        checks,
        "pillar_2_segment_alignment",
        passed=len(invalid_segment_quotes) == 0,
        required="every non-empty Pillar 2 segment_name must exist in Pillar 1 segment_profiles",
        actual=f"invalid_segment_quotes={len(invalid_segment_quotes)}",
        details=(f"invalid_segment_quote_ids={invalid_segment_quotes[:30]}" if invalid_segment_quotes else ""),
    )

    # 4) Pillar 3
    p3_complete = all(
        c.primary_promise and c.mechanism and c.offer_style and c.proof_style and c.creative_pattern
        for c in pillar_3.direct_competitors
    )
    competitors_count = len(pillar_3.direct_competitors)
    substitutes_count = len(pillar_3.substitute_categories)
    gate_mode = (config.PHASE1_COMPETITOR_GATE_MODE or "").strip().lower()
    if gate_mode == "dynamic_4_10":
        competitor_floor = max(1, int(config.PHASE1_MIN_COMPETITORS_FLOOR))
        competitor_target = max(competitor_floor, int(config.PHASE1_TARGET_COMPETITORS))
    else:
        competitor_floor = max(1, int(config.PHASE1_MIN_COMPETITORS))
        competitor_target = competitor_floor
    p3_pass = (
        competitors_count >= competitor_floor
        and substitutes_count >= 3
        and p3_complete
    )
    _check(
        checks,
        "pillar_3_competitive_depth",
        passed=p3_pass,
        required=(
            f"competitors>={competitor_floor} (target={competitor_target}), "
            "substitutes>=3, all competitor fields complete"
        ),
        actual=(
            f"competitors={competitors_count}, substitutes={substitutes_count}, "
            f"complete={p3_complete}"
        ),
        details=(
            f"mode={gate_mode or 'legacy'}; progress_to_target={competitors_count}/{competitor_target}"
        ),
    )

    # 5) Pillar 4
    p4_pass = (
        bool(pillar_4.why_problem_exists.strip())
        and bool(pillar_4.why_solution_uniquely_works.strip())
        and len(pillar_4.mechanism_supporting_evidence_ids) >= 10
    )
    _check(
        checks,
        "pillar_4_mechanism_strength",
        passed=p4_pass,
        required="two-part mechanism present; supporting evidence ids >=10",
        actual=(
            f"problem_exists={bool(pillar_4.why_problem_exists.strip())}, "
            f"solution_unique={bool(pillar_4.why_solution_uniquely_works.strip())}, "
            f"support_ids={len(pillar_4.mechanism_supporting_evidence_ids)}"
        ),
    )

    # 6) Pillar 5
    p5_pass = True
    p5_detail = []
    for seg in pillar_5.segment_classifications:
        total = sum(float(v) for v in seg.awareness_distribution.values())
        valid_total = abs(total - 1.0) <= 0.05
        valid_support = len(seg.support_evidence_ids) >= 5
        if not (valid_total and valid_support):
            p5_pass = False
        p5_detail.append(
            f"{seg.segment_name}: total={total:.3f}, support={len(seg.support_evidence_ids)}"
        )
    _check(
        checks,
        "pillar_5_awareness_validity",
        passed=(p5_pass and bool(pillar_5.segment_classifications)),
        required="each segment distribution sums 1.0±0.05 and support ids>=5",
        actual="; ".join(p5_detail)[:1000] if p5_detail else "no segment classifications",
    )

    # 7) Pillar 6
    emotion_count = len(pillar_6.dominant_emotions)
    high_conf_count = 0
    for emo in pillar_6.dominant_emotions:
        if emo.tagged_quote_count >= 8 and emo.share_of_voc >= 0.05:
            high_conf_count += 1
    min_emotions = max(1, int(getattr(config, "PHASE1_MIN_DOMINANT_EMOTIONS", 3)))
    min_high_conf = max(1, int(getattr(config, "PHASE1_MIN_HIGH_CONF_EMOTIONS", 2)))
    p6_pass = emotion_count >= min_emotions and high_conf_count >= min_high_conf
    _check(
        checks,
        "pillar_6_emotion_dominance",
        passed=p6_pass,
        required=(
            f">={min_emotions} dominant emotions; "
            f"at least {min_high_conf} have count>=8 and share>=0.05"
        ),
        actual=(
            f"emotion_count={emotion_count}; high_conf_count={high_conf_count}; "
            + ", ".join(
                f"{e.emotion}:{e.tagged_quote_count}/{e.share_of_voc:.2f}" for e in pillar_6.dominant_emotions
            )[:900]
        ),
    )

    # 8) Pillar 7
    by_type = defaultdict(list)
    for asset in pillar_7.assets:
        by_type[asset.proof_type].append(asset)
    required_types = ["statistical", "testimonial", "authority", "story"]
    p7_pass = True
    for proof_type in required_types:
        assets = by_type.get(proof_type, [])
        if len(assets) < config.PHASE1_MIN_PROOFS_PER_TYPE:
            p7_pass = False
            break
        if not any(a.strength == "top_tier" for a in assets):
            p7_pass = False
            break
    _check(
        checks,
        "pillar_7_proof_coverage",
        passed=p7_pass,
        required=f"each proof type has >={config.PHASE1_MIN_PROOFS_PER_TYPE} assets and >=1 top_tier",
        actual=", ".join(f"{k}:{len(v)}" for k, v in sorted(by_type.items())),
    )

    # 9) Cross-pillar consistency
    cross_pass = (
        cross_report.objections_represented_in_voc
        and cross_report.mechanism_alignment_with_competition
        and cross_report.dominant_emotions_traced_to_voc
    )
    _check(
        checks,
        "cross_pillar_consistency",
        passed=cross_pass,
        required="objections/voc, mechanism/competition, emotions/voc all pass",
        actual=(
            f"objections_voc={cross_report.objections_represented_in_voc}, "
            f"mechanism_competition={cross_report.mechanism_alignment_with_competition}, "
            f"emotions_voc={cross_report.dominant_emotions_traced_to_voc}, "
            f"issues={len(cross_report.issues)}"
        ),
        details="; ".join(cross_report.issues)[:1000],
    )

    failed = [c.gate_id for c in checks if not c.passed]
    return QualityGateReport(
        overall_pass=(len(failed) == 0),
        failed_gate_ids=failed,
        checks=checks,
        retry_rounds_used=retry_rounds_used,
    )
