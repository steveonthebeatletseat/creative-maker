"""Agent 04 output schema — Copywriter.

Production-ready ad scripts with time-coded beat sheets.
Each script includes spoken dialogue, on-screen text, visual direction,
SFX/music cues, timing, copy framework, metadata, and CTA variations.

Built from agent_04_copywriter.md research doc.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import (
    AwarenessLevel,
    ComplianceRisk,
    CreativeFormat,
    FunnelStage,
    ProofType,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class CopyFramework(str, Enum):
    PAS = "pas"
    AIDA = "aida"
    BAB = "bab"
    STAR_STORY_SOLUTION = "star_story_solution"
    CLAIM_PROOF_OFFER = "claim_proof_offer"
    MYTH_TRUTH_FIX = "myth_truth_fix"
    OBJECTION_FIRST = "objection_first"
    THREE_BEAT_DEMO = "three_beat_demo"
    NEGATIVE_REVERSE = "negative_reverse"
    RMBC = "rmbc"


class SceneShotType(str, Enum):
    A_ROLL_SELFIE = "a_roll_selfie"
    A_ROLL_MEDIUM = "a_roll_medium"
    B_ROLL_DEMO = "b_roll_demo"
    B_ROLL_LIFESTYLE = "b_roll_lifestyle"
    OVERLAY_SCREENSHOT = "overlay_screenshot"
    OVERLAY_TEXT = "overlay_text"
    SPLIT_SCREEN = "split_screen"
    GREEN_SCREEN = "green_screen"
    PRODUCT_CLOSEUP = "product_closeup"
    BEFORE_AFTER = "before_after"
    SCREEN_RECORDING = "screen_recording"


# ---------------------------------------------------------------------------
# Script Components
# ---------------------------------------------------------------------------

class Mechanism(BaseModel):
    """The 'why it works' component — every script must have one."""
    name: str = Field(..., description="Named mechanism (e.g. 'X-peptide delivery')")
    one_line_explanation: str = Field(
        ..., description="Why old solutions fail + why this works, in one sentence"
    )
    spoken_line: str = Field(
        ..., description="How the mechanism is communicated in the script"
    )
    on_screen_text: Optional[str] = Field(
        None, description="Optional on-screen reinforcement"
    )


class ProofMoment(BaseModel):
    """A proof element within the script."""
    proof_type: ProofType
    asset_needed: str = Field(
        ..., description="Specific asset: 'before/after photo', '5-star screenshot', 'demo clip'"
    )
    spoken_line: str = Field(..., description="What's said during this proof moment")
    timing: str = Field(..., description="When this appears, e.g. '10-15s'")


class ObjectionHandle(BaseModel):
    """How the script addresses a key objection."""
    objection: str = Field(..., description="The objection being addressed")
    response_line: str = Field(..., description="The script line that handles it")


class CTABlock(BaseModel):
    """Call to action — single, clear, platform-appropriate."""
    primary_cta: str = Field(..., description="The main CTA text")
    spoken_cta: str = Field(..., description="How CTA is spoken in the script")
    on_screen_cta: str = Field(..., description="On-screen text for CTA")
    timing: str = Field(..., description="When CTA appears, e.g. '26-30s'")


class CTAVariation(BaseModel):
    """Alternative CTA for testing."""
    variant_name: str = Field(
        ..., description="urgency, curiosity, social_proof, risk_reversal"
    )
    cta_text: str
    on_screen_text: str


class ScriptBeat(BaseModel):
    """A single time-coded beat in the script — the editor's blueprint."""
    t_start: float = Field(..., description="Start time in seconds")
    t_end: float = Field(..., description="End time in seconds")
    scene_type: str = Field(
        ...,
        description="hook, problem, agitation, mechanism, proof, objection, cta, transition"
    )
    shot_type: SceneShotType
    visual_direction: str = Field(
        ..., description="What the viewer sees (camera, framing, action, props)"
    )
    spoken_dialogue: str = Field(
        ..., description="What's spoken (empty string if no dialogue)"
    )
    on_screen_text: str = Field(
        ..., description="Text overlay (empty string if none)"
    )
    captions: str = Field(
        default="", description="Caption/subtitle text if different from spoken"
    )
    sfx_music: str = Field(
        default="", description="Sound effects or music cue"
    )
    editor_notes: str = Field(
        default="", description="Notes for the editor (pacing, transitions, emphasis)"
    )
    asset_callouts: list[str] = Field(
        default_factory=list,
        description="Specific assets needed: 'INSERT: app screen recording'"
    )


# ---------------------------------------------------------------------------
# Complete Script
# ---------------------------------------------------------------------------

class AdScript(BaseModel):
    """A complete production-ready ad script."""
    script_id: str = Field(..., description="Unique ID: e.g. script_tof_01_30s")
    concept_id: str = Field(
        ..., description="References the idea_id from Agent 03 survivors"
    )
    idea_name: str

    # Strategic metadata
    funnel_stage: FunnelStage
    awareness_target: AwarenessLevel
    target_segment: str
    copy_framework: CopyFramework
    format: CreativeFormat

    # The "spine" of the script
    big_idea: str = Field(
        ..., description="One sentence belief shift the entire script reinforces"
    )
    single_core_promise: str = Field(
        ..., description="One sentence promise — if the script has 2 unrelated promises, it fails"
    )
    dominant_desire: str = Field(..., description="The desire being channeled")
    primary_objection: str = Field(
        ..., description="The #1 objection this script must overcome"
    )
    believability_device: str = Field(
        ..., description="mechanism, proof, demo, or 'deadly sincerity'"
    )

    # Duration & pacing
    duration_seconds: int = Field(..., description="Target duration: 15, 30, or 60")
    total_word_count: int = Field(
        ..., description="Total spoken words (should be ~150-160 WPM)"
    )
    words_per_minute: float = Field(
        ..., description="Actual WPM for this script"
    )

    # Core components
    mechanism: Mechanism
    proof_moments: list[ProofMoment] = Field(
        ..., min_length=1,
        description="At least 1 proof moment per script"
    )
    objection_handling: list[ObjectionHandle] = Field(
        ..., min_length=1,
        description="At least 1 objection handled"
    )
    cta: CTABlock
    cta_variations: list[CTAVariation] = Field(
        ..., min_length=2, max_length=4,
        description="2-4 CTA variations for testing"
    )

    # The beat sheet (editor blueprint)
    beats: list[ScriptBeat] = Field(
        ..., min_length=3,
        description="Time-coded beats covering the full script"
    )

    # Fascination beats (for scripts >20s)
    fascination_beats: list[str] = Field(
        default_factory=list,
        description="Curiosity open-loops resolved later (3+ for scripts >20s)"
    )

    # Compliance
    compliance_risk: ComplianceRisk
    compliance_flags: list[str] = Field(
        default_factory=list,
        description="Specific compliance concerns for Agent 12"
    )

    # Quality gate self-check
    quality_gates_passed: dict[str, bool] = Field(
        ...,
        description=(
            "Self-check: one_idea, mechanism_exists, proof_exists, "
            "objection_addressed, cta_singular, pacing_ok, on_screen_readable"
        ),
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Copywriter Brief
# ---------------------------------------------------------------------------

class CopywriterBrief(BaseModel):
    """Complete Agent 04 output — production-ready scripts for all 15 concepts."""
    brand_name: str
    product_name: str
    generated_date: str
    batch_id: str = Field(default="", description="Batch identifier")

    # All scripts
    scripts: list[AdScript] = Field(
        ..., min_length=15, max_length=15,
        description="Exactly 15 production-ready scripts (one per surviving concept)"
    )

    # Summary stats
    total_scripts: int = Field(default=15)
    scripts_by_funnel: dict[str, int] = Field(
        ..., description="Count of scripts per funnel stage"
    )
    scripts_by_framework: dict[str, int] = Field(
        ..., description="Count of scripts per copy framework used"
    )
    scripts_by_duration: dict[str, int] = Field(
        ..., description="Count of scripts per duration"
    )

    # Notes for Agent 05 (Hook Specialist)
    hook_engineering_notes: list[str] = Field(
        ..., min_length=3, max_length=7,
        description="Guidance for Agent 05 on hook engineering across these scripts"
    )

    # Overall quality
    average_wpm: float = Field(
        ..., description="Average WPM across all scripts"
    )
    quality_gate_failures: list[str] = Field(
        default_factory=list,
        description="Any scripts that failed quality gates (should be empty)"
    )
