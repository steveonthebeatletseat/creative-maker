from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import config
from pipeline.phase1_adjudicate import AdjudicationOutput
from pipeline.phase1_collect_claude import _build_prompt as build_claude_prompt
from pipeline.phase1_collect_gemini import _build_prompt as build_gemini_prompt
from pipeline.phase1_contradiction import apply_contradiction_flags, detect_contradictions
from pipeline.phase1_engine import (
    GapFillOutput,
    _build_targeted_collector_prompt,
    _sanitize_voc_quotes,
    run_phase1_collectors_only,
    run_phase1_engine,
)
from pipeline.phase1_evidence import dedupe_evidence
from pipeline.phase1_hardening import harden_adjudicated_output
from pipeline.phase1_quality_gates import evaluate_quality_gates
from pipeline.phase1_synthesize_pillars import (
    _pillar_instruction,
    _summarize_reports,
    derive_emotional_inventory_from_collectors,
    derive_emotional_inventory_from_voc,
)
from pipeline.phase1_text_filters import is_malformed_quote
from schemas.foundation_research import (
    AwarenessLevel,
    AwarenessSegmentClassification,
    CompetitorProfile,
    ContradictionReport,
    CrossPillarConsistencyReport,
    DominantEmotion,
    EvidenceItem,
    FoundationResearchBriefV2,
    MechanismSaturationEntry,
    Pillar1ProspectProfile,
    Pillar2VocLanguageBank,
    Pillar3CompetitiveIntelligence,
    Pillar4ProductMechanismAnalysis,
    Pillar5AwarenessClassification,
    Pillar6EmotionalDriverInventory,
    Pillar7ProofCredibilityInventory,
    ProofAsset,
    ProspectSegment,
    QualityGateReport,
    ResearchModelTraceEntry,
    VocQuote,
)


def _trace(provider: str, status: str = "success") -> ResearchModelTraceEntry:
    return ResearchModelTraceEntry(
        stage="collector",
        provider=provider,
        model="test-model",
        status=status,
        started_at="2026-02-14T00:00:00Z",
        finished_at="2026-02-14T00:00:01Z",
        duration_seconds=1.0,
        notes="",
    )


def make_valid_bundle():
    p1 = Pillar1ProspectProfile(
        segment_profiles=[
            ProspectSegment(
                segment_name="Segment A",
                goals=["Goal 1"],
                pains=["Pain 1"],
                triggers=["Trigger 1"],
                information_sources=["Reddit"],
                objections=["Too expensive"],
            ),
            ProspectSegment(
                segment_name="Segment B",
                goals=["Goal 2"],
                pains=["Pain 2"],
                triggers=["Trigger 2"],
                information_sources=["Reviews"],
                objections=["Will it work"],
            ),
        ],
        synthesis_summary="summary",
    )

    quotes: list[VocQuote] = []
    categories = [
        ("pain", 35),
        ("desire", 35),
        ("objection", 30),
        ("trigger", 25),
        ("proof", 25),
    ]
    idx = 1
    for cat, count in categories:
        for _ in range(count):
            quotes.append(
                VocQuote(
                    quote_id=f"q_{idx}",
                    quote=f"Quote {idx} about {cat}",
                    category=cat,
                    theme=f"{cat}_theme",
                    segment_name="Segment A" if idx % 2 == 0 else "Segment B",
                    awareness_level=AwarenessLevel.PROBLEM_AWARE,
                    dominant_emotion={
                        "pain": "Frustration / Pain",
                        "desire": "Desire for Freedom / Immersion",
                        "objection": "Skepticism / Distrust",
                        "trigger": "Anxiety / Fear",
                        "proof": "Relief / Satisfaction",
                    }.get(cat, ""),
                    emotional_intensity=4,
                    source_type="review",
                    source_url=f"https://example.com/{idx}",
                )
            )
            idx += 1
    p2 = Pillar2VocLanguageBank(quotes=quotes, saturation_last_30_new_themes=3)

    competitors = [
        CompetitorProfile(
            competitor_name=f"Competitor {i}",
            primary_promise="Promise",
            mechanism="Mechanism A",
            offer_style="bundle",
            proof_style="testimonial",
            creative_pattern="ugc demo",
            source_url=f"https://competitor{i}.com",
        )
        for i in range(1, 11)
    ]
    p3 = Pillar3CompetitiveIntelligence(
        direct_competitors=competitors,
        substitute_categories=["DIY", "Do nothing", "Professional service"],
        mechanism_saturation_map=[
            MechanismSaturationEntry(mechanism="Mechanism A", saturation_score=7),
            MechanismSaturationEntry(mechanism="Mechanism B", saturation_score=5),
        ],
    )

    p4 = Pillar4ProductMechanismAnalysis(
        why_problem_exists="Problem exists because old solutions ignore the root cause.",
        why_solution_uniquely_works="Our solution works because it addresses the root cause directly.",
        primary_mechanism_name="Mechanism A",
        mechanism_supporting_evidence_ids=[f"ev_{i}" for i in range(1, 11)],
    )

    p5 = Pillar5AwarenessClassification(
        segment_classifications=[
            AwarenessSegmentClassification(
                segment_name="Segment A",
                primary_awareness=AwarenessLevel.PROBLEM_AWARE,
                awareness_distribution={
                    "unaware": 0.1,
                    "problem_aware": 0.4,
                    "solution_aware": 0.3,
                    "product_aware": 0.15,
                    "most_aware": 0.05,
                },
                support_evidence_ids=[f"ev_{i}" for i in range(1, 6)],
            ),
            AwarenessSegmentClassification(
                segment_name="Segment B",
                primary_awareness=AwarenessLevel.SOLUTION_AWARE,
                awareness_distribution={
                    "unaware": 0.05,
                    "problem_aware": 0.25,
                    "solution_aware": 0.4,
                    "product_aware": 0.2,
                    "most_aware": 0.1,
                },
                support_evidence_ids=[f"ev_{i}" for i in range(6, 11)],
            ),
        ]
    )

    p6 = Pillar6EmotionalDriverInventory(
        dominant_emotions=[
            DominantEmotion(emotion="relief", tagged_quote_count=12, share_of_voc=0.12, sample_quote_ids=["q_1", "q_2"]),
            DominantEmotion(emotion="hope", tagged_quote_count=14, share_of_voc=0.14, sample_quote_ids=["q_3", "q_4"]),
            DominantEmotion(emotion="fear", tagged_quote_count=10, share_of_voc=0.1, sample_quote_ids=["q_5", "q_6"]),
            DominantEmotion(emotion="frustration", tagged_quote_count=9, share_of_voc=0.09, sample_quote_ids=["q_7", "q_8"]),
            DominantEmotion(emotion="pride", tagged_quote_count=8, share_of_voc=0.08, sample_quote_ids=["q_9", "q_10"]),
        ]
    )

    assets: list[ProofAsset] = []
    for proof_type in ["statistical", "testimonial", "authority", "story"]:
        assets.append(
            ProofAsset(
                asset_id=f"{proof_type}_1",
                proof_type=proof_type,
                title=f"{proof_type} top",
                detail="High quality proof",
                strength="top_tier",
                source_url="https://proof.example/top",
            )
        )
        assets.append(
            ProofAsset(
                asset_id=f"{proof_type}_2",
                proof_type=proof_type,
                title=f"{proof_type} supporting",
                detail="Supporting proof",
                strength="strong",
                source_url="https://proof.example/strong",
            )
        )
    p7 = Pillar7ProofCredibilityInventory(assets=assets)

    source_types = ["review", "reddit", "forum", "ad_library", "support", "social"]
    evidence = [
        EvidenceItem(
            evidence_id=f"ev_{i}",
            claim=f"Claim {i}",
            verbatim=f"Verbatim {i}",
            source_url=f"https://example.com/e/{i}",
            source_type=source_types[i % len(source_types)],
            published_date="2026-02-14",
            pillar_tags=["pillar_1", "pillar_2", "pillar_3", "pillar_4", "pillar_7"],
            confidence=0.7,
            provider="gemini",
        )
        for i in range(1, 321)
    ]

    cross = CrossPillarConsistencyReport(
        objections_represented_in_voc=True,
        mechanism_alignment_with_competition=True,
        dominant_emotions_traced_to_voc=True,
        issues=[],
    )

    adjudicated = AdjudicationOutput(
        pillar_1_prospect_profile=p1,
        pillar_2_voc_language_bank=p2,
        pillar_3_competitive_intelligence=p3,
        pillar_4_product_mechanism_analysis=p4,
        pillar_5_awareness_classification=p5,
        pillar_6_emotional_driver_inventory=p6,
        pillar_7_proof_credibility_inventory=p7,
        cross_pillar_consistency_report=cross,
    )

    pillars = {
        "pillar_1": p1,
        "pillar_2": p2,
        "pillar_3": p3,
        "pillar_4": p4,
        "pillar_5": p5,
        "pillar_6": p6,
        "pillar_7": p7,
    }

    return pillars, adjudicated, evidence


class Phase1V2Tests(unittest.TestCase):
    def test_quality_gates_pass_with_elite_bundle(self):
        pillars, adjudicated, evidence = make_valid_bundle()
        report = evaluate_quality_gates(
            evidence=evidence,
            pillar_1=adjudicated.pillar_1_prospect_profile,
            pillar_2=adjudicated.pillar_2_voc_language_bank,
            pillar_3=adjudicated.pillar_3_competitive_intelligence,
            pillar_4=adjudicated.pillar_4_product_mechanism_analysis,
            pillar_5=adjudicated.pillar_5_awareness_classification,
            pillar_6=adjudicated.pillar_6_emotional_driver_inventory,
            pillar_7=adjudicated.pillar_7_proof_credibility_inventory,
            cross_report=adjudicated.cross_pillar_consistency_report,
            retry_rounds_used=0,
        )
        self.assertTrue(report.overall_pass)

    def test_quality_gates_fail_on_cross_consistency(self):
        _, adjudicated, evidence = make_valid_bundle()
        broken = CrossPillarConsistencyReport(
            objections_represented_in_voc=False,
            mechanism_alignment_with_competition=False,
            dominant_emotions_traced_to_voc=False,
            issues=["mismatch"],
        )
        report = evaluate_quality_gates(
            evidence=evidence,
            pillar_1=adjudicated.pillar_1_prospect_profile,
            pillar_2=adjudicated.pillar_2_voc_language_bank,
            pillar_3=adjudicated.pillar_3_competitive_intelligence,
            pillar_4=adjudicated.pillar_4_product_mechanism_analysis,
            pillar_5=adjudicated.pillar_5_awareness_classification,
            pillar_6=adjudicated.pillar_6_emotional_driver_inventory,
            pillar_7=adjudicated.pillar_7_proof_credibility_inventory,
            cross_report=broken,
            retry_rounds_used=0,
        )
        self.assertFalse(report.overall_pass)
        self.assertIn("cross_pillar_consistency", report.failed_gate_ids)

    def test_quality_gate_rejects_unsourced_voc(self):
        _, adjudicated, evidence = make_valid_bundle()
        adjudicated.pillar_2_voc_language_bank.quotes[0].source_type = "other"
        adjudicated.pillar_2_voc_language_bank.quotes[0].source_url = ""
        report = evaluate_quality_gates(
            evidence=evidence,
            pillar_1=adjudicated.pillar_1_prospect_profile,
            pillar_2=adjudicated.pillar_2_voc_language_bank,
            pillar_3=adjudicated.pillar_3_competitive_intelligence,
            pillar_4=adjudicated.pillar_4_product_mechanism_analysis,
            pillar_5=adjudicated.pillar_5_awareness_classification,
            pillar_6=adjudicated.pillar_6_emotional_driver_inventory,
            pillar_7=adjudicated.pillar_7_proof_credibility_inventory,
            cross_report=adjudicated.cross_pillar_consistency_report,
            retry_rounds_used=0,
        )
        self.assertFalse(report.overall_pass)
        self.assertIn("pillar_2_voc_depth", report.failed_gate_ids)

    def test_competitor_gate_passes_with_six_complete_profiles_dynamic_mode(self):
        _, adjudicated, evidence = make_valid_bundle()
        adjudicated.pillar_3_competitive_intelligence.direct_competitors = (
            adjudicated.pillar_3_competitive_intelligence.direct_competitors[:6]
        )
        with patch("pipeline.phase1_quality_gates.config.PHASE1_COMPETITOR_GATE_MODE", "dynamic_4_10"), patch(
            "pipeline.phase1_quality_gates.config.PHASE1_MIN_COMPETITORS_FLOOR",
            4,
        ), patch(
            "pipeline.phase1_quality_gates.config.PHASE1_TARGET_COMPETITORS",
            10,
        ):
            report = evaluate_quality_gates(
                evidence=evidence,
                pillar_1=adjudicated.pillar_1_prospect_profile,
                pillar_2=adjudicated.pillar_2_voc_language_bank,
                pillar_3=adjudicated.pillar_3_competitive_intelligence,
                pillar_4=adjudicated.pillar_4_product_mechanism_analysis,
                pillar_5=adjudicated.pillar_5_awareness_classification,
                pillar_6=adjudicated.pillar_6_emotional_driver_inventory,
                pillar_7=adjudicated.pillar_7_proof_credibility_inventory,
                cross_report=adjudicated.cross_pillar_consistency_report,
                retry_rounds_used=0,
            )
        self.assertTrue(report.overall_pass)

    def test_competitor_gate_fails_below_floor(self):
        _, adjudicated, evidence = make_valid_bundle()
        adjudicated.pillar_3_competitive_intelligence.direct_competitors = (
            adjudicated.pillar_3_competitive_intelligence.direct_competitors[:3]
        )
        with patch("pipeline.phase1_quality_gates.config.PHASE1_COMPETITOR_GATE_MODE", "dynamic_4_10"), patch(
            "pipeline.phase1_quality_gates.config.PHASE1_MIN_COMPETITORS_FLOOR",
            4,
        ), patch(
            "pipeline.phase1_quality_gates.config.PHASE1_TARGET_COMPETITORS",
            10,
        ):
            report = evaluate_quality_gates(
                evidence=evidence,
                pillar_1=adjudicated.pillar_1_prospect_profile,
                pillar_2=adjudicated.pillar_2_voc_language_bank,
                pillar_3=adjudicated.pillar_3_competitive_intelligence,
                pillar_4=adjudicated.pillar_4_product_mechanism_analysis,
                pillar_5=adjudicated.pillar_5_awareness_classification,
                pillar_6=adjudicated.pillar_6_emotional_driver_inventory,
                pillar_7=adjudicated.pillar_7_proof_credibility_inventory,
                cross_report=adjudicated.cross_pillar_consistency_report,
                retry_rounds_used=0,
            )
        self.assertFalse(report.overall_pass)
        self.assertIn("pillar_3_competitive_depth", report.failed_gate_ids)

    def test_malformed_voc_quote_rejected_by_quality_gate(self):
        _, adjudicated, evidence = make_valid_bundle()
        adjudicated.pillar_2_voc_language_bank.quotes[0].quote = 'quote": "bad", "theme": "x"'
        report = evaluate_quality_gates(
            evidence=evidence,
            pillar_1=adjudicated.pillar_1_prospect_profile,
            pillar_2=adjudicated.pillar_2_voc_language_bank,
            pillar_3=adjudicated.pillar_3_competitive_intelligence,
            pillar_4=adjudicated.pillar_4_product_mechanism_analysis,
            pillar_5=adjudicated.pillar_5_awareness_classification,
            pillar_6=adjudicated.pillar_6_emotional_driver_inventory,
            pillar_7=adjudicated.pillar_7_proof_credibility_inventory,
            cross_report=adjudicated.cross_pillar_consistency_report,
            retry_rounds_used=0,
        )
        self.assertFalse(report.overall_pass)
        self.assertIn("pillar_2_voc_depth", report.failed_gate_ids)

    def test_hardening_drops_malformed_voc_candidates(self):
        _, adjudicated, _ = make_valid_bundle()
        adjudicated.pillar_2_voc_language_bank.quotes = []
        evidence = [
            EvidenceItem(
                evidence_id="ev_bad",
                claim='quote": "broken", "theme": "json leak"',
                verbatim='quote": "broken", "theme": "json leak"',
                source_url="https://example.com/bad",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2"],
                confidence=0.88,
                provider="claude",
            ),
            EvidenceItem(
                evidence_id="ev_good",
                claim="I love this strap but the battery still dies too fast.",
                verbatim="I love this strap but the battery still dies too fast.",
                source_url="https://example.com/good",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2"],
                confidence=0.86,
                provider="gemini",
            ),
        ]
        with patch("pipeline.phase1_hardening.config.PHASE1_MIN_VOC_QUOTES", 1):
            harden_adjudicated_output(adjudicated, evidence)
        quotes = adjudicated.pillar_2_voc_language_bank.quotes
        self.assertTrue(quotes)
        self.assertTrue(all('quote":' not in q.quote.lower() for q in quotes))

    def test_engine_sanitize_drops_malformed_quotes(self):
        clean = VocQuote(
            quote_id="q_clean",
            quote="My headset hurts after 20 minutes.",
            category="pain",
            theme="comfort pain",
            source_type="review",
            source_url="https://example.com/clean",
        )
        malformed = VocQuote(
            quote_id="q_bad",
            quote='quote": "oops", "theme": "bad"',
            category="pain",
            theme="bad",
            source_type="review",
            source_url="https://example.com/bad",
        )
        sanitized = _sanitize_voc_quotes([clean, malformed])
        self.assertEqual([q.quote_id for q in sanitized], ["q_clean"])

    def test_text_filter_flags_control_tag_and_retry_scaffold_leakage(self):
        self.assertTrue(
            is_malformed_quote('[gate=pillar_2_voc_depth][source_type=review][confidence=0.89] "quote body"')
        )
        self.assertTrue(
            is_malformed_quote("Targeted tasks: add more quotes from reddit")
        )
        self.assertFalse(
            is_malformed_quote("I get calm focus without the crash.")
        )

    def test_hardening_canonicalizes_and_blanks_segment_names(self):
        _, adjudicated, evidence = make_valid_bundle()
        q0_id = adjudicated.pillar_2_voc_language_bank.quotes[0].quote_id
        q1_id = adjudicated.pillar_2_voc_language_bank.quotes[1].quote_id
        adjudicated.pillar_2_voc_language_bank.quotes[0].segment_name = "segment a"
        adjudicated.pillar_2_voc_language_bank.quotes[1].segment_name = "Unknown Segment Label"

        harden_adjudicated_output(adjudicated, evidence)
        quotes_by_id = {q.quote_id: q for q in adjudicated.pillar_2_voc_language_bank.quotes}
        self.assertEqual(quotes_by_id[q0_id].segment_name, "Segment A")
        self.assertEqual(quotes_by_id[q1_id].segment_name, "")

    def test_quality_gate_fails_invalid_segment_labels(self):
        _, adjudicated, evidence = make_valid_bundle()
        adjudicated.pillar_2_voc_language_bank.quotes[0].segment_name = "Alien Segment"
        report = evaluate_quality_gates(
            evidence=evidence,
            pillar_1=adjudicated.pillar_1_prospect_profile,
            pillar_2=adjudicated.pillar_2_voc_language_bank,
            pillar_3=adjudicated.pillar_3_competitive_intelligence,
            pillar_4=adjudicated.pillar_4_product_mechanism_analysis,
            pillar_5=adjudicated.pillar_5_awareness_classification,
            pillar_6=adjudicated.pillar_6_emotional_driver_inventory,
            pillar_7=adjudicated.pillar_7_proof_credibility_inventory,
            cross_report=adjudicated.cross_pillar_consistency_report,
            retry_rounds_used=0,
        )
        self.assertFalse(report.overall_pass)
        self.assertIn("pillar_2_segment_alignment", report.failed_gate_ids)

    def test_targeted_collector_prompt_includes_allowed_segment_context(self):
        prompt = _build_targeted_collector_prompt(
            context={"brand_name": "Brand", "product_name": "Product"},
            failed_gate_ids=["pillar_2_segment_alignment"],
            task_brief=["Fix segment labels"],
            evidence=[],
            allowed_segments=["Segment A", "Segment B"],
        )
        self.assertIn("Allowed Pillar 1 segments", prompt)
        self.assertIn("Segment A", prompt)
        self.assertIn("Segment B", prompt)

    def test_vr_defaults_removed_from_config_prompts_and_collector_fallback(self):
        repo_root = Path(config.ROOT_DIR)
        config_text = (repo_root / "config.py").read_text("utf-8")
        voc_collector_text = (repo_root / "pipeline" / "phase1_collect_voc.py").read_text("utf-8")
        hardening_text = (repo_root / "pipeline" / "phase1_hardening.py").read_text("utf-8")
        gap_fill_text = (repo_root / "prompts" / "phase1" / "gap_fill.md").read_text("utf-8")
        targeted_text = (repo_root / "prompts" / "phase1" / "collector_targeted.md").read_text("utf-8")

        self.assertNotIn("OculusQuest,MetaQuestVR,VRGaming,virtualreality", config_text)
        self.assertNotIn("vr comfort strap battery review", voc_collector_text)
        self.assertNotIn("review comfort issue battery", voc_collector_text)
        self.assertIn("customer reviews pain points objections", voc_collector_text)
        self.assertNotIn("BOBOVR", hardening_text)
        self.assertNotIn("Meta Elite Strap with Battery", hardening_text)
        self.assertNotIn("no VR context detected", hardening_text)
        self.assertNotIn("OculusQuest", gap_fill_text)
        self.assertNotIn("MetaQuestVR", targeted_text)

    def test_collector_prompts_include_optional_context(self):
        context = {
            "brand_name": "Brand",
            "product_name": "Product",
            "niche": "VR",
            "product_description": "Description",
            "target_market": "Gamers",
            "website_url": "https://example.com",
            "competitor_info": "Competitor A, Competitor B",
            "landing_page_info": "Landing page angles",
            "website_intel": "Traffic notes",
            "customer_reviews": "Review snippets",
            "previous_performance": "ROAS and CPA",
            "additional_context": "TARGETED_COLLECTION_PROMPT: focus on VOC only",
        }
        gemini_prompt = build_gemini_prompt(context)
        claude_prompt = build_claude_prompt(context)

        for prompt in (gemini_prompt, claude_prompt):
            self.assertIn("Known Competitors:", prompt)
            self.assertIn("Landing Page Notes:", prompt)
            self.assertIn("Website Intel:", prompt)
            self.assertIn("Customer Reviews Seed:", prompt)
            self.assertIn("Previous Performance:", prompt)
            self.assertIn("Additional Context:", prompt)
            self.assertIn("LF8 Mapping Guidance", prompt)
            self.assertIn("candidate_lf8", prompt)

    def test_collector_prompts_omit_absent_optional_context(self):
        context = {
            "brand_name": "Brand",
            "product_name": "Product",
            "niche": "VR",
            "product_description": "Description",
            "target_market": "Gamers",
            "website_url": "https://example.com",
        }
        gemini_prompt = build_gemini_prompt(context)
        claude_prompt = build_claude_prompt(context)
        for prompt in (gemini_prompt, claude_prompt):
            self.assertNotIn("Known Competitors:", prompt)
            self.assertNotIn("Landing Page Notes:", prompt)
            self.assertNotIn("Website Intel:", prompt)
            self.assertNotIn("Customer Reviews Seed:", prompt)
            self.assertNotIn("Previous Performance:", prompt)
            self.assertNotIn("Additional Context:", prompt)

    def test_summarize_reports_allocates_across_collectors(self):
        report_a = "A" * 8000
        report_b = "B" * 2000
        summary = _summarize_reports([report_a, report_b], max_chars=4000)
        part_a, part_b = summary.split("\n\n---\n\n")
        self.assertGreater(len(part_a), len(part_b))
        self.assertTrue(part_a.startswith("A"))
        self.assertTrue(part_b.startswith("B"))

    def test_summarize_reports_preserves_single_report_behavior(self):
        report = "X" * 1200
        summary = _summarize_reports([report], max_chars=1000)
        self.assertEqual(summary, "X" * 1000)

    def test_pillar_instructions_include_framework_clauses(self):
        self.assertIn("Carlton", _pillar_instruction("pillar_1"))
        self.assertIn("Georgi", _pillar_instruction("pillar_4"))
        self.assertIn("Schwartz", _pillar_instruction("pillar_5"))
        self.assertIn("Makepeace", _pillar_instruction("pillar_7"))

    def test_contradiction_detector_flags_cross_provider_conflict(self):
        evidence = [
            EvidenceItem(
                evidence_id="ev_a",
                claim="Users say this battery strap is reliable and works great.",
                verbatim="This works great and I recommend it.",
                source_url="https://a.example.com",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_3"],
                confidence=0.82,
                provider="gemini",
            ),
            EvidenceItem(
                evidence_id="ev_b",
                claim="Users say this battery strap is a scam and constantly broken.",
                verbatim="This is a scam and does not work.",
                source_url="https://b.example.com",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_3"],
                confidence=0.81,
                provider="claude",
            ),
        ]
        with patch("pipeline.phase1_contradiction.config.PHASE1_CONTRADICTION_USE_LLM", False):
            contradictions = detect_contradictions(
                evidence=evidence,
                provider="openai",
                model="gpt-5.2",
                temperature=0.2,
                max_tokens=8000,
            )
        self.assertTrue(contradictions)
        self.assertIn(contradictions[0].severity, {"high", "medium", "low"})
        flagged = apply_contradiction_flags(evidence, contradictions)
        self.assertTrue(any(item.conflict_flag for item in flagged))

    def test_dedupe_two_tier_behavior(self):
        items = [
            EvidenceItem(
                evidence_id="ev_1",
                claim="Battery strap improves comfort and stability for long sessions.",
                verbatim="Battery strap improves comfort and stability for long sessions.",
                source_url="https://example.com/review?a=1&utm_source=x",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2"],
                confidence=0.61,
                provider="gemini",
            ),
            EvidenceItem(
                evidence_id="ev_2",
                claim="Battery strap improves comfort and stability for long sessions.",
                verbatim="Battery strap improves comfort and stability for long sessions.",
                source_url="https://example.com/review?a=1",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2"],
                confidence=0.84,
                provider="gemini",
            ),
            EvidenceItem(
                evidence_id="ev_3",
                claim="Battery strap improves comfort and stability for long sessions.",
                verbatim="Battery strap improves comfort and stability for long sessions.",
                source_url="https://mirror.example.com/review?a=1",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2"],
                confidence=0.77,
                provider="claude",
            ),
        ]
        with patch("pipeline.phase1_evidence.config.PHASE1_SEMANTIC_DEDUPE", True), patch(
            "pipeline.phase1_evidence.config.PHASE1_URL_CANONICALIZE", True
        ):
            deduped = dedupe_evidence(items)
        self.assertEqual(len(deduped), 1)
        self.assertGreaterEqual(deduped[0].confidence, 0.89)

    def test_no_voc_recycling_when_disabled(self):
        _, adjudicated, _ = make_valid_bundle()
        adjudicated.pillar_2_voc_language_bank.quotes = []
        sparse_evidence = [
            EvidenceItem(
                evidence_id="ev_sparse_1",
                claim="My battery dies quickly and it hurts my forehead.",
                verbatim="My battery dies quickly and it hurts my forehead.",
                source_url="https://example.com/sparse1",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2"],
                confidence=0.72,
                provider="gemini",
            ),
            EvidenceItem(
                evidence_id="ev_sparse_2",
                claim="I wish this was more comfortable for long sessions.",
                verbatim="I wish this was more comfortable for long sessions.",
                source_url="https://example.com/sparse2",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2"],
                confidence=0.71,
                provider="gemini",
            ),
        ]
        with patch("pipeline.phase1_hardening.config.PHASE1_ALLOW_VOC_RECYCLING", False), patch(
            "pipeline.phase1_hardening.config.PHASE1_MIN_VOC_QUOTES",
            150,
        ):
            harden_adjudicated_output(adjudicated, sparse_evidence)
        self.assertLess(len(adjudicated.pillar_2_voc_language_bank.quotes), 150)

    def test_no_proof_cloning_when_disabled(self):
        _, adjudicated, evidence = make_valid_bundle()
        adjudicated.pillar_7_proof_credibility_inventory.assets = [
            ProofAsset(
                asset_id="proof_stat",
                proof_type="statistical",
                title="Stat proof",
                detail="Detail",
                strength="top_tier",
                source_url="https://example.com/stat",
            )
        ]
        with patch("pipeline.phase1_hardening.config.PHASE1_ALLOW_PROOF_CLONING", False), patch(
            "pipeline.phase1_hardening.config.PHASE1_MIN_PROOFS_PER_TYPE",
            3,
        ):
            harden_adjudicated_output(adjudicated, evidence[:20])
        self.assertFalse(
            any(asset.asset_id.endswith("_x2") or "_x" in asset.asset_id for asset in adjudicated.pillar_7_proof_credibility_inventory.assets)
        )

    def test_emotions_are_derivable_from_voc(self):
        _, adjudicated, _ = make_valid_bundle()
        p6 = derive_emotional_inventory_from_voc(adjudicated.pillar_2_voc_language_bank)
        self.assertGreaterEqual(len(p6.dominant_emotions), 5)
        self.assertTrue(all(e.tagged_quote_count > 0 for e in p6.dominant_emotions))

    def test_emotion_inventory_no_longer_forces_five_rows(self):
        quotes: list[VocQuote] = []
        for i in range(1, 13):
            quotes.append(
                VocQuote(
                    quote_id=f"pain_{i}",
                    quote=f"I'm frustrated and distracted {i}",
                    category="pain",
                    theme="friction",
                    dominant_emotion="Frustration / Pain",
                    source_type="review",
                    source_url=f"https://example.com/pain/{i}",
                )
            )
        for i in range(1, 10):
            quotes.append(
                VocQuote(
                    quote_id=f"desire_{i}",
                    quote=f"I want calm focus without crashes {i}",
                    category="desire",
                    theme="focus",
                    dominant_emotion="Calm Confidence",
                    source_type="review",
                    source_url=f"https://example.com/desire/{i}",
                )
            )

        p2 = Pillar2VocLanguageBank(quotes=quotes, saturation_last_30_new_themes=0)
        p6 = derive_emotional_inventory_from_voc(p2)
        labels = [e.emotion for e in p6.dominant_emotions]
        self.assertEqual(labels, ["Frustration / Pain", "Calm Confidence"])
        self.assertEqual(len(p6.dominant_emotions), 2)

    def test_emotion_inventory_from_collectors_preserves_brand_specific_labels(self):
        p2 = Pillar2VocLanguageBank(
            quotes=[
                VocQuote(
                    quote_id="q_stuck_1",
                    quote="I feel stuck and foggy when I try to start work.",
                    category="pain",
                    theme="focus initiation",
                    segment_name="Segment A",
                    dominant_emotion="Frustration / Pain",
                    source_type="forum",
                    source_url="https://example.com/stuck/1",
                ),
                VocQuote(
                    quote_id="q_stuck_2",
                    quote="I'm in stagnation mode all afternoon and can't get moving.",
                    category="pain",
                    theme="afternoon slump",
                    segment_name="Segment A",
                    dominant_emotion="Frustration / Pain",
                    source_type="forum",
                    source_url="https://example.com/stuck/2",
                ),
                VocQuote(
                    quote_id="q_placebo_1",
                    quote="Thirty milligrams feels like placebo fairy dust.",
                    category="objection",
                    theme="dosage skepticism",
                    segment_name="Segment A",
                    dominant_emotion="Skepticism / Distrust",
                    source_type="forum",
                    source_url="https://example.com/placebo/1",
                ),
                VocQuote(
                    quote_id="q_placebo_2",
                    quote="This dose is too low and feels like paying for placebo.",
                    category="objection",
                    theme="efficacy concern",
                    segment_name="Segment A",
                    dominant_emotion="Skepticism / Distrust",
                    source_type="review",
                    source_url="https://example.com/placebo/2",
                ),
            ],
            saturation_last_30_new_themes=0,
        )
        reports = [
            "\n".join(
                [
                    "# Collector A Report (Gemini Breadth)",
                    "## Pillar 6: Emotional Drivers & Objections",
                    "### Key Findings",
                    "- **Driver: Fear of Stagnation:** People feel stuck and foggy if they cannot start quickly.",
                    "- **Objection: \"Fairy Dusting\":** The dosage sounds like placebo.",
                    "### Evidence Lines",
                    "- sample",
                    "## Pillar 7: Proof Assets",
                ]
            ),
            "\n".join(
                [
                    "# Collector B Report (Claude Precision)",
                    "## Pillar 6: Emotional Drivers & Objections",
                    "### Key Findings",
                    "- **Dominant emotions:** fear of stagnation, skepticism about placebo dosing.",
                    "- **Top objection pattern #1 — \"Fairy Dusting\":** buyers call the dosage ineffective.",
                    "### Evidence Lines",
                    "- sample",
                    "## Pillar 7: Proof Assets",
                ]
            ),
        ]
        p6 = derive_emotional_inventory_from_collectors(
            reports,
            p2,
            mutate_pillar2_labels=True,
        )
        labels = [row.emotion for row in p6.dominant_emotions]
        self.assertIn("Fear of Stagnation", labels)
        self.assertIn("Fairy Dusting", labels)
        self.assertNotIn("Desire / Aspiration", labels)
        by_id = {row.quote_id: row.dominant_emotion for row in p2.quotes}
        self.assertEqual(by_id.get("q_stuck_1"), "Fear of Stagnation")
        self.assertEqual(by_id.get("q_placebo_1"), "Fairy Dusting")
        self.assertEqual(p6.lf8_mode, "strict_lf8")
        self.assertIn("Segment A", p6.lf8_rows_by_segment)
        self.assertGreaterEqual(len(p6.lf8_rows_by_segment.get("Segment A", [])), 1)

    def test_lf8_rows_are_dropped_when_quote_support_is_below_balanced_threshold(self):
        p2 = Pillar2VocLanguageBank(
            quotes=[
                VocQuote(
                    quote_id="q_only_1",
                    quote="This feels like placebo and I don't trust it.",
                    category="objection",
                    theme="efficacy concern",
                    segment_name="Segment A",
                    dominant_emotion="Skepticism / Distrust",
                    source_type="forum",
                    source_url="https://example.com/placebo/only",
                )
            ],
            saturation_last_30_new_themes=0,
        )
        reports = [
            "\n".join(
                [
                    "# Collector A Report (Gemini Breadth)",
                    "## Pillar 6: Emotional Drivers & Objections",
                    "### Key Findings",
                    "- Objection: Does it actually work? candidate_lf8=lf8_3",
                    "## Pillar 7: Proof Assets",
                ]
            ),
            "\n".join(
                [
                    "# Collector B Report (Claude Precision)",
                    "## Pillar 6: Emotional Drivers & Objections",
                    "### Key Findings",
                    "- Objection: Placebo risk and distrust. candidate_lf8=lf8_3",
                    "## Pillar 7: Proof Assets",
                ]
            ),
        ]

        p6 = derive_emotional_inventory_from_collectors(
            reports,
            p2,
            allowed_segments=["Segment A"],
        )
        self.assertEqual(p6.lf8_mode, "strict_lf8")
        self.assertEqual(p6.lf8_rows_by_segment.get("Segment A", []), [])

    def test_lf8_rows_are_dropped_when_contradiction_risk_is_high(self):
        p2 = Pillar2VocLanguageBank(
            quotes=[
                VocQuote(
                    quote_id="q_1",
                    quote="This feels like placebo and maybe scammy.",
                    category="objection",
                    theme="efficacy concern",
                    segment_name="Segment A",
                    dominant_emotion="Skepticism / Distrust",
                    source_type="forum",
                    source_url="https://example.com/placebo/risk1",
                ),
                VocQuote(
                    quote_id="q_2",
                    quote="I am worried this does not work for real.",
                    category="objection",
                    theme="efficacy concern",
                    segment_name="Segment A",
                    dominant_emotion="Skepticism / Distrust",
                    source_type="review",
                    source_url="https://example.com/placebo/risk2",
                ),
            ],
            saturation_last_30_new_themes=0,
        )
        evidence = [
            EvidenceItem(
                evidence_id="ev_1",
                claim="placebo and scam concern",
                verbatim="placebo and scam concern",
                source_url="https://example.com/placebo/risk1",
                source_type="forum",
                published_date="2026-02-14",
                pillar_tags=["pillar_2", "pillar_6"],
                confidence=0.88,
                provider="gemini",
                conflict_flag="high_unresolved",
            ),
            EvidenceItem(
                evidence_id="ev_2",
                claim="does not work concern",
                verbatim="does not work concern",
                source_url="https://example.com/placebo/risk2",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2", "pillar_6"],
                confidence=0.87,
                provider="claude",
                conflict_flag="high_unresolved",
            ),
        ]
        reports = [
            "\n".join(
                [
                    "# Collector A Report (Gemini Breadth)",
                    "## Pillar 6: Emotional Drivers & Objections",
                    "### Key Findings",
                    "- Objection: Does it actually work? candidate_lf8=lf8_3",
                    "## Pillar 7: Proof Assets",
                ]
            ),
            "\n".join(
                [
                    "# Collector B Report (Claude Precision)",
                    "## Pillar 6: Emotional Drivers & Objections",
                    "### Key Findings",
                    "- Objection: Trust risk is high. candidate_lf8=lf8_3",
                    "## Pillar 7: Proof Assets",
                ]
            ),
        ]

        p6 = derive_emotional_inventory_from_collectors(
            reports,
            p2,
            evidence=evidence,
            allowed_segments=["Segment A"],
        )
        self.assertEqual(p6.lf8_rows_by_segment.get("Segment A", []), [])

    def test_emotion_inventory_from_collectors_falls_back_without_pillar6_section(self):
        p2 = Pillar2VocLanguageBank(
            quotes=[
                VocQuote(
                    quote_id="q_1",
                    quote="I'm frustrated by this workflow.",
                    category="pain",
                    theme="workflow",
                    dominant_emotion="Frustration / Pain",
                    source_type="review",
                    source_url="https://example.com/q1",
                ),
                VocQuote(
                    quote_id="q_2",
                    quote="I want calm focus all day.",
                    category="desire",
                    theme="focus",
                    dominant_emotion="Calm Confidence",
                    source_type="review",
                    source_url="https://example.com/q2",
                ),
            ],
            saturation_last_30_new_themes=0,
        )
        reports = ["# Collector A Report\n## Pillar 5: Awareness\n- No pillar 6 content"]
        rebuilt = derive_emotional_inventory_from_collectors(reports, p2)
        fallback = derive_emotional_inventory_from_voc(p2)
        self.assertEqual(
            [row.emotion for row in rebuilt.dominant_emotions],
            [row.emotion for row in fallback.dominant_emotions],
        )

    def test_phase1_engine_success_with_partial_collector_failure(self):
        pillars, adjudicated, evidence = make_valid_bundle()

        def fake_collect_gemini(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "A long report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        def fake_collect_claude(_context):
            return {
                "success": False,
                "provider": "claude",
                "report": "",
                "error": "sdk unavailable",
                "trace": _trace("anthropic", "failed"),
            }

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect_gemini), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect_claude
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch(
                "pipeline.phase1_engine.evaluate_quality_gates",
                return_value=QualityGateReport(overall_pass=True, failed_gate_ids=[], checks=[], retry_rounds_used=0),
            ), patch("pipeline.phase1_engine.extract_seed_evidence", return_value=evidence):
                result = run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )

            self.assertIsInstance(result, FoundationResearchBriefV2)
            self.assertEqual(result.schema_version, "2.0")
            self.assertTrue((out_dir / "foundation_research_evidence_ledger.json").exists())
            self.assertTrue((out_dir / "foundation_research_quality_report.json").exists())
            self.assertTrue((out_dir / "foundation_research_trace.json").exists())

    def test_phase1_engine_retries_then_raises_when_gates_fail(self):
        pillars, adjudicated, evidence = make_valid_bundle()

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        def always_fail_quality(*_args, **kwargs):
            return QualityGateReport(
                overall_pass=False,
                failed_gate_ids=["pillar_2_voc_depth"],
                checks=[],
                retry_rounds_used=kwargs.get("retry_rounds_used", 0),
            )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch("pipeline.phase1_engine.evaluate_quality_gates", side_effect=always_fail_quality), patch(
                "pipeline.phase1_engine._run_gap_fill", return_value=GapFillOutput(additional_evidence=[], notes=""),
            ), patch("pipeline.phase1_engine.extract_seed_evidence", return_value=evidence), patch(
                "pipeline.phase1_engine.config.PHASE1_GAP_FILL_ROUNDS", 1
            ), patch("pipeline.phase1_engine.config.PHASE1_STRICT_HARD_BLOCK", True):
                with self.assertRaises(RuntimeError):
                    run_phase1_engine(
                        inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                        provider="openai",
                        model="gpt-5.2",
                        temperature=0.2,
                        max_tokens=8000,
                        output_dir=out_dir,
                    )

            self.assertTrue((out_dir / "foundation_research_quality_report.json").exists())
            quality = json.loads((out_dir / "foundation_research_quality_report.json").read_text("utf-8"))
            self.assertIn("pillar_2_voc_depth", quality.get("failed_gate_ids", []))

    def test_phase1_targeted_recollection_mode_avoids_legacy_gap_fill(self):
        pillars, adjudicated, evidence = make_valid_bundle()

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        gate_states = [
            QualityGateReport(
                overall_pass=False,
                failed_gate_ids=["pillar_2_voc_depth"],
                checks=[],
                retry_rounds_used=0,
            ),
            QualityGateReport(
                overall_pass=True,
                failed_gate_ids=[],
                checks=[],
                retry_rounds_used=1,
            ),
        ]
        added = EvidenceItem(
            evidence_id="ev_collector_added",
            claim="New collector sourced evidence row with valid public URL.",
            verbatim="New collector sourced evidence row with valid public URL.",
            source_url="https://example.com/new",
            source_type="review",
            published_date="2026-02-14",
            pillar_tags=["pillar_2"],
            confidence=0.82,
            provider="claude",
        )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch("pipeline.phase1_engine.evaluate_quality_gates", side_effect=gate_states), patch(
                "pipeline.phase1_engine.extract_seed_evidence", return_value=evidence
            ), patch(
                "pipeline.phase1_engine._run_targeted_recollection",
                return_value=([added], []),
            ), patch(
                "pipeline.phase1_engine._run_gap_fill",
                side_effect=AssertionError("legacy gap fill should not run"),
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_GAP_FILL_MODE",
                "targeted_collectors",
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_TARGETED_COLLECTOR_MAX_ROUNDS",
                1,
            ):
                result = run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )
        evidence_ids = {item.evidence_id for item in result.evidence_ledger}
        self.assertIn("ev_collector_added", evidence_ids)

    def test_targeted_retry_selects_single_collector_by_gate(self):
        pillars, adjudicated, evidence = make_valid_bundle()
        selected: list[str] = []

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        gate_states = [
            QualityGateReport(
                overall_pass=False,
                failed_gate_ids=["pillar_3_competitive_depth"],
                checks=[],
                retry_rounds_used=0,
            ),
            QualityGateReport(
                overall_pass=True,
                failed_gate_ids=[],
                checks=[],
                retry_rounds_used=1,
            ),
        ]

        def fake_recollect(**kwargs):
            selected.append(kwargs.get("selected_collector", ""))
            return ([], [])

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch("pipeline.phase1_engine.evaluate_quality_gates", side_effect=gate_states), patch(
                "pipeline.phase1_engine.extract_seed_evidence", return_value=evidence
            ), patch("pipeline.phase1_engine._run_targeted_recollection", side_effect=fake_recollect), patch(
                "pipeline.phase1_engine.config.PHASE1_RETRY_STRATEGY",
                "single_focused_collector",
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_ENFORCE_SINGLE_RETRY",
                True,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_RETRY_ROUNDS_MAX",
                1,
            ):
                run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )
        self.assertEqual(selected, ["claude"])

    def test_retry_rounds_clamped_to_one_when_enforced(self):
        pillars, adjudicated, evidence = make_valid_bundle()
        recollect_calls = {"count": 0}
        quality_calls = {"count": 0}

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        def always_fail_quality(*_args, **kwargs):
            quality_calls["count"] += 1
            return QualityGateReport(
                overall_pass=False,
                failed_gate_ids=["pillar_2_voc_depth"],
                checks=[],
                retry_rounds_used=kwargs.get("retry_rounds_used", 0),
            )

        def fake_recollect(**_kwargs):
            recollect_calls["count"] += 1
            return ([], [])

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch("pipeline.phase1_engine.evaluate_quality_gates", side_effect=always_fail_quality), patch(
                "pipeline.phase1_engine.extract_seed_evidence", return_value=evidence
            ), patch("pipeline.phase1_engine._run_targeted_recollection", side_effect=fake_recollect), patch(
                "pipeline.phase1_engine.config.PHASE1_TARGETED_COLLECTOR_MAX_ROUNDS",
                5,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_ENFORCE_SINGLE_RETRY",
                True,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_RETRY_ROUNDS_MAX",
                1,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_STRICT_HARD_BLOCK",
                True,
            ):
                with self.assertRaises(RuntimeError):
                    run_phase1_engine(
                        inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                        provider="openai",
                        model="gpt-5.2",
                        temperature=0.2,
                        max_tokens=8000,
                        output_dir=out_dir,
                    )
        self.assertEqual(recollect_calls["count"], 1)
        self.assertEqual(quality_calls["count"], 2)

    def test_soft_mode_returns_output_with_retry_warning_after_failed_retry(self):
        pillars, adjudicated, evidence = make_valid_bundle()

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        def always_fail_quality(*_args, **kwargs):
            return QualityGateReport(
                overall_pass=False,
                failed_gate_ids=["pillar_3_competitive_depth"],
                checks=[],
                retry_rounds_used=kwargs.get("retry_rounds_used", 0),
            )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch("pipeline.phase1_engine.evaluate_quality_gates", side_effect=always_fail_quality), patch(
                "pipeline.phase1_engine.extract_seed_evidence", return_value=evidence
            ), patch(
                "pipeline.phase1_engine._run_targeted_recollection",
                return_value=([], []),
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_STRICT_HARD_BLOCK",
                False,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_WARN_ON_UNRESOLVED_GATES",
                True,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_ENFORCE_SINGLE_RETRY",
                True,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_RETRY_ROUNDS_MAX",
                1,
            ):
                result = run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )
        self.assertFalse(result.quality_gate_report.overall_pass)
        self.assertTrue(result.retry_audit)
        self.assertTrue(result.retry_audit[-1].warning)

    def test_strict_mode_raises_after_failed_single_retry(self):
        pillars, adjudicated, evidence = make_valid_bundle()

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        def always_fail_quality(*_args, **kwargs):
            return QualityGateReport(
                overall_pass=False,
                failed_gate_ids=["pillar_2_voc_depth"],
                checks=[],
                retry_rounds_used=kwargs.get("retry_rounds_used", 0),
            )

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch("pipeline.phase1_engine.evaluate_quality_gates", side_effect=always_fail_quality), patch(
                "pipeline.phase1_engine.extract_seed_evidence", return_value=evidence
            ), patch(
                "pipeline.phase1_engine._run_targeted_recollection",
                return_value=([], []),
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_STRICT_HARD_BLOCK",
                True,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_ENFORCE_SINGLE_RETRY",
                True,
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_RETRY_ROUNDS_MAX",
                1,
            ):
                with self.assertRaises(RuntimeError):
                    run_phase1_engine(
                        inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                        provider="openai",
                        model="gpt-5.2",
                        temperature=0.2,
                        max_tokens=8000,
                        output_dir=out_dir,
                    )

    def test_retry_audit_artifact_written_with_expected_fields(self):
        pillars, adjudicated, evidence = make_valid_bundle()

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        gate_states = [
            QualityGateReport(
                overall_pass=False,
                failed_gate_ids=["pillar_2_voc_depth"],
                checks=[],
                retry_rounds_used=0,
            ),
            QualityGateReport(
                overall_pass=True,
                failed_gate_ids=[],
                checks=[],
                retry_rounds_used=1,
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch("pipeline.phase1_engine.evaluate_quality_gates", side_effect=gate_states), patch(
                "pipeline.phase1_engine.extract_seed_evidence", return_value=evidence
            ), patch(
                "pipeline.phase1_engine._run_targeted_recollection",
                return_value=([], []),
            ):
                run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )
            artifact = out_dir / "foundation_research_retry_audit.json"
            self.assertTrue(artifact.exists())
            payload = json.loads(artifact.read_text("utf-8"))
            self.assertTrue(payload)
            row = payload[0]
            for key in (
                "round_index",
                "failed_gate_ids_before",
                "selected_collector",
                "added_evidence_count",
                "failed_gate_ids_after",
                "status",
                "warning",
            ):
                self.assertIn(key, row)

    def test_phase1_contradiction_strict_block_toggle(self):
        pillars, adjudicated, evidence = make_valid_bundle()

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        conflicts = [
            ContradictionReport(
                claim_a_id="ev_1",
                claim_b_id="ev_2",
                provider_a="gemini",
                provider_b="claude",
                conflict_description="conflict",
                severity="high",
                resolution="manual review required",
                resolved=False,
            )
        ]

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            shared_patches = [
                patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect),
                patch("pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect),
                patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars),
                patch("pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated),
                patch(
                    "pipeline.phase1_engine.evaluate_quality_gates",
                    return_value=QualityGateReport(overall_pass=True, failed_gate_ids=[], checks=[], retry_rounds_used=0),
                ),
                patch("pipeline.phase1_engine.extract_seed_evidence", return_value=evidence),
                patch("pipeline.phase1_engine.detect_contradictions", return_value=conflicts),
            ]
            for p in shared_patches:
                p.start()
            try:
                with patch("pipeline.phase1_engine.config.PHASE1_ENABLE_CONTRADICTION_DETECTION", True), patch(
                    "pipeline.phase1_engine.config.PHASE1_STRICT_CONTRADICTION_BLOCK",
                    False,
                ):
                    run_phase1_engine(
                        inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                        provider="openai",
                        model="gpt-5.2",
                        temperature=0.2,
                        max_tokens=8000,
                        output_dir=out_dir,
                    )

                with patch("pipeline.phase1_engine.config.PHASE1_ENABLE_CONTRADICTION_DETECTION", True), patch(
                    "pipeline.phase1_engine.config.PHASE1_STRICT_CONTRADICTION_BLOCK",
                    True,
                ):
                    with self.assertRaises(RuntimeError):
                        run_phase1_engine(
                            inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                            provider="openai",
                            model="gpt-5.2",
                            temperature=0.2,
                            max_tokens=8000,
                            output_dir=out_dir,
                        )
            finally:
                for p in reversed(shared_patches):
                    p.stop()

    def test_phase1_retry_adds_only_collector_sourced_evidence(self):
        pillars, adjudicated, evidence = make_valid_bundle()

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        gate_states = [
            QualityGateReport(
                overall_pass=False,
                failed_gate_ids=["pillar_2_voc_depth"],
                checks=[],
                retry_rounds_used=0,
            ),
            QualityGateReport(
                overall_pass=True,
                failed_gate_ids=[],
                checks=[],
                retry_rounds_used=1,
            ),
        ]

        added = [
            EvidenceItem(
                evidence_id="ev_collector_retry",
                claim="Retry collection produced new proof with source URL.",
                verbatim="Retry collection produced new proof with source URL.",
                source_url="https://example.com/retry",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_7"],
                confidence=0.83,
                provider="claude",
            )
        ]

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch("pipeline.phase1_engine.evaluate_quality_gates", side_effect=gate_states), patch(
                "pipeline.phase1_engine.extract_seed_evidence", return_value=evidence
            ), patch(
                "pipeline.phase1_engine._run_targeted_recollection",
                return_value=(added, []),
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_GAP_FILL_MODE",
                "targeted_collectors",
            ), patch(
                "pipeline.phase1_engine.config.PHASE1_TARGETED_COLLECTOR_MAX_ROUNDS",
                1,
            ):
                result = run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )

        providers = {item.provider for item in result.evidence_ledger}
        self.assertIn("claude", providers)
        self.assertNotIn("synthesis", providers)

    def test_phase1_malformed_voc_zero_in_final_output(self):
        pillars, adjudicated, evidence = make_valid_bundle()
        adjudicated.pillar_2_voc_language_bank.quotes[0].quote = 'quote": "bad", "theme": "x"'

        def fake_collect(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report with https://example.com/source",
                "error": "",
                "trace": _trace("google", "success"),
            }

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect
            ), patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars), patch(
                "pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated
            ), patch(
                "pipeline.phase1_engine.evaluate_quality_gates",
                return_value=QualityGateReport(overall_pass=True, failed_gate_ids=[], checks=[], retry_rounds_used=0),
            ), patch("pipeline.phase1_engine.extract_seed_evidence", return_value=evidence):
                result = run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )

        self.assertTrue(
            all('quote":' not in quote.quote.lower() for quote in result.pillar_2_voc_language_bank.quotes)
        )

    def test_collectors_only_snapshot_writes_artifact(self):
        def fake_gemini(_context):
            return {
                "success": True,
                "provider": "gemini",
                "report": "Collector report one https://example.com/a",
                "error": "",
                "trace": _trace("google", "success"),
            }

        def fake_claude(_context):
            return {
                "success": True,
                "provider": "claude",
                "report": "Collector report two https://example.com/b",
                "error": "",
                "trace": _trace("anthropic", "success"),
            }

        evidence = [
            EvidenceItem(
                evidence_id="ev_one",
                claim="Claim one with a valid source URL and customer signal.",
                verbatim="Claim one with a valid source URL and customer signal.",
                source_url="https://example.com/e1",
                source_type="review",
                published_date="2026-02-14",
                pillar_tags=["pillar_2"],
                confidence=0.76,
                provider="gemini",
            )
        ]

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_gemini), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_claude
            ), patch("pipeline.phase1_engine.extract_seed_evidence", return_value=evidence):
                snapshot = run_phase1_collectors_only(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )

            self.assertEqual(snapshot.get("stage"), "collectors_complete")
            self.assertGreaterEqual(snapshot.get("collector_count", 0), 2)
            reports = snapshot.get("collector_reports", [])
            providers = {row.get("provider") for row in reports if isinstance(row, dict)}
            self.assertIn("gemini", providers)
            self.assertIn("claude", providers)
            self.assertTrue((out_dir / "foundation_research_collectors_snapshot.json").exists())

    def test_collectors_only_raises_when_all_collectors_fail(self):
        def fake_fail(_context):
            return {
                "success": False,
                "provider": "gemini",
                "report": "",
                "error": "collector down",
                "trace": _trace("google", "failed"),
            }

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            with patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_fail), patch(
                "pipeline.phase1_engine.collect_with_claude", side_effect=fake_fail
            ):
                with self.assertRaises(RuntimeError):
                    run_phase1_collectors_only(
                        inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                        provider="openai",
                        model="gpt-5.2",
                        temperature=0.2,
                        max_tokens=8000,
                        output_dir=out_dir,
                    )

    def test_phase1_reuses_collector_checkpoint_on_second_run(self):
        pillars, adjudicated, evidence = make_valid_bundle()
        call_counter = {"gemini": 0, "claude": 0}

        def fake_collect_gemini(_context):
            call_counter["gemini"] += 1
            return {
                "success": True,
                "provider": "gemini",
                "report": "Gemini report with https://example.com/source-g",
                "error": "",
                "trace": _trace("google", "success"),
            }

        def fake_collect_claude(_context):
            call_counter["claude"] += 1
            return {
                "success": True,
                "provider": "claude",
                "report": "Claude report with https://example.com/source-c",
                "error": "",
                "trace": _trace("anthropic", "success"),
            }

        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            common_patches = [
                patch("pipeline.phase1_engine.collect_with_gemini", side_effect=fake_collect_gemini),
                patch("pipeline.phase1_engine.collect_with_claude", side_effect=fake_collect_claude),
                patch("pipeline.phase1_engine.synthesize_pillars_dag", return_value=pillars),
                patch("pipeline.phase1_engine.adjudicate_pillars", return_value=adjudicated),
                patch(
                    "pipeline.phase1_engine.evaluate_quality_gates",
                    return_value=QualityGateReport(overall_pass=True, failed_gate_ids=[], checks=[], retry_rounds_used=0),
                ),
                patch("pipeline.phase1_engine.extract_seed_evidence", return_value=evidence),
                patch("pipeline.phase1_engine.config.PHASE1_ENABLE_CHECKPOINTS", True),
                patch("pipeline.phase1_engine.config.PHASE1_REUSE_COLLECTOR_CHECKPOINT", True),
                patch("pipeline.phase1_engine.config.PHASE1_FORCE_FRESH_COLLECTORS", False),
                patch("pipeline.phase1_engine.config.PHASE1_CHECKPOINT_TTL_HOURS", 72),
            ]

            for p in common_patches:
                p.start()
            try:
                run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )
                self.assertEqual(call_counter["gemini"], 1)
                self.assertEqual(call_counter["claude"], 1)

                run_phase1_engine(
                    inputs={"brand_name": "Test Brand", "product_name": "Test Product"},
                    provider="openai",
                    model="gpt-5.2",
                    temperature=0.2,
                    max_tokens=8000,
                    output_dir=out_dir,
                )
                self.assertEqual(call_counter["gemini"], 1)
                self.assertEqual(call_counter["claude"], 1)
                self.assertTrue((out_dir / "phase1_collector_checkpoint.json").exists())
            finally:
                for p in reversed(common_patches):
                    p.stop()

    def test_hardening_backfills_critical_pillars(self):
        _, adjudicated, _ = make_valid_bundle()
        adjudicated.pillar_2_voc_language_bank.quotes = adjudicated.pillar_2_voc_language_bank.quotes[:4]
        adjudicated.pillar_3_competitive_intelligence.direct_competitors = (
            adjudicated.pillar_3_competitive_intelligence.direct_competitors[:2]
        )
        adjudicated.pillar_4_product_mechanism_analysis.mechanism_supporting_evidence_ids = ["ev_1", "ev_2"]
        for seg in adjudicated.pillar_5_awareness_classification.segment_classifications:
            seg.support_evidence_ids = seg.support_evidence_ids[:2]
        adjudicated.pillar_7_proof_credibility_inventory.assets = (
            adjudicated.pillar_7_proof_credibility_inventory.assets[:2]
        )

        rich_evidence: list[EvidenceItem] = []
        competitor_snippets = [
            "Compared BoboVR M3 Pro and it was okay but battery buzzed.",
            "KIWI K4 Boost felt better than stock strap.",
            "Meta Elite Strap broke after months for my friend.",
            "YOGES battery strap was cheap but inconsistent.",
            "AMVR comfort kit helped but not enough battery.",
            "AUBIKA strap was stable in workouts.",
            "ZyberVR has lots of Quest accessories.",
            "DESTEK pricing is attractive for entry buyers.",
            "BINBOK option looked promising for budget setups.",
            "GOMRVR had a decent integrated battery.",
        ]
        idx = 1
        for i in range(170):
            source_type = ["review", "reddit", "forum", "social", "support"][i % 5]
            rich_evidence.append(
                EvidenceItem(
                    evidence_id=f"ev_rich_{idx}",
                    claim=f"I get forehead pain after 30 minutes and my battery dies mid-session {i}.",
                    verbatim=f"I need this to stop hurting and play longer after work {i}.",
                    source_url=f"https://example.com/review/{idx}",
                    source_type=source_type,
                    published_date="2026-02-14",
                    pillar_tags=["pillar_1", "pillar_2", "pillar_4"],
                    confidence=0.76,
                    provider="gemini",
                )
            )
            idx += 1
        for snippet in competitor_snippets:
            rich_evidence.append(
                EvidenceItem(
                    evidence_id=f"ev_cmp_{idx}",
                    claim=snippet,
                    verbatim=f"My take: {snippet}",
                    source_url=f"https://example.com/comp/{idx}",
                    source_type="review",
                    published_date="2026-02-14",
                    pillar_tags=["pillar_3", "pillar_4", "pillar_7"],
                    confidence=0.8,
                    provider="claude",
                )
            )
            idx += 1
        for i in range(30):
            rich_evidence.append(
                EvidenceItem(
                    evidence_id=f"ev_stat_{idx}",
                    claim=f"Users report 4.8/5 rating and {4 + (i % 3)} extra hours in tests ({50 + i}%).",
                    verbatim=f"My setup lasted {4 + (i % 3)} hours and felt like a game changer {i}.",
                    source_url=f"https://example.com/stat/{idx}",
                    source_type="review",
                    published_date="2026-02-14",
                    pillar_tags=["pillar_2", "pillar_7"],
                    confidence=0.82,
                    provider="gemini",
                )
            )
            idx += 1

        harden_adjudicated_output(adjudicated, rich_evidence)

        self.assertGreaterEqual(
            len(adjudicated.pillar_2_voc_language_bank.quotes),
            150,
        )
        self.assertGreaterEqual(
            len(adjudicated.pillar_3_competitive_intelligence.direct_competitors),
            2,
        )
        self.assertGreaterEqual(
            len(adjudicated.pillar_3_competitive_intelligence.substitute_categories),
            3,
        )
        competitor_names = {
            c.competitor_name.strip().lower()
            for c in adjudicated.pillar_3_competitive_intelligence.direct_competitors
        }
        self.assertNotIn("bobovr", competitor_names)
        self.assertNotIn("kiwi design", competitor_names)
        self.assertGreaterEqual(
            len(adjudicated.pillar_4_product_mechanism_analysis.mechanism_supporting_evidence_ids),
            10,
        )
        self.assertTrue(
            all(
                len(seg.support_evidence_ids) >= 5
                for seg in adjudicated.pillar_5_awareness_classification.segment_classifications
            )
        )
        by_type = {}
        for asset in adjudicated.pillar_7_proof_credibility_inventory.assets:
            by_type.setdefault(asset.proof_type, []).append(asset)
        for proof_type in ["statistical", "testimonial", "authority", "story"]:
            self.assertGreaterEqual(len(by_type.get(proof_type, [])), 2)
            self.assertTrue(any(a.strength == "top_tier" for a in by_type.get(proof_type, [])))


if __name__ == "__main__":
    unittest.main()
