"""Agent 05 output schema — Hook Specialist.

Engineers the first 3 seconds of every script — the highest-leverage
element in the pipeline. Produces 3-5 hook variations per script with
verbal + visual as matched pairs, sound-on/sound-off versions,
platform-specific variants, and hook category tags.

Built from agent_05_hook_specialist.md research doc.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import AwarenessLevel, ComplianceRisk, FunnelStage


# ---------------------------------------------------------------------------
# Hook Components
# ---------------------------------------------------------------------------

class HookEditNotes(BaseModel):
    """Time-coded edit instructions for the first 3 seconds."""
    beat_1: str = Field(..., description="0.0-0.7s: what happens visually + verbally")
    beat_2: str = Field(..., description="0.7-1.5s: what happens next")
    beat_3: str = Field(..., description="1.5-3.0s: what completes the hook")
    transition_to_body: str = Field(
        ..., description="How the hook hands off to the script body at second 3"
    )


class SoundOnVariant(BaseModel):
    """Sound-on encoding (TikTok-first)."""
    spoken_line: str = Field(
        ..., description="First words spoken — carries personality + tension"
    )
    delivery_notes: str = Field(
        ..., description="Tone, pacing, emphasis (e.g. 'whispered urgency', 'excited casual')"
    )
    sfx: str = Field(default="", description="Sound effect cue if any")
    music_note: str = Field(default="", description="Music/trending sound note")


class SoundOffVariant(BaseModel):
    """Sound-off encoding (Meta Feed-first)."""
    primary_text_overlay: str = Field(
        ..., description="Main on-screen text (4-8 words max, high contrast)"
    )
    secondary_text: Optional[str] = Field(
        None, description="Optional second line (smaller, supporting)"
    )
    visual_carries_message: str = Field(
        ..., description="How the visual alone communicates the hook without audio"
    )


class PlatformVariant(BaseModel):
    """Platform-specific hook adjustments."""
    platform: str = Field(
        ..., description="meta_feed, ig_reels, tiktok, youtube_shorts"
    )
    adjustments: str = Field(
        ..., description="What changes for this platform"
    )
    first_frame_spec: str = Field(
        ..., description="First frame requirements for this platform"
    )


class HookRiskFlag(BaseModel):
    """Compliance or brand-safety risk on a hook."""
    risk_type: str = Field(
        ...,
        description="claim_risk, personal_attribute, before_after, medical_claim, "
                    "guaranteed_outcome, authority_impersonation, other"
    )
    description: str = Field(..., description="What the risk is and why")
    severity: ComplianceRisk


# ---------------------------------------------------------------------------
# Single Hook Variation
# ---------------------------------------------------------------------------

class HookVariation(BaseModel):
    """A complete hook variation — verbal + visual as a matched pair."""
    hook_id: str = Field(
        ..., description="Unique ID: e.g. script_tof_01_hook_a"
    )
    hook_family: str = Field(
        ...,
        description=(
            "Hook taxonomy family: identity_callout, problem_agitation, "
            "outcome_transformation, mechanism_reveal, myth_bust, social_proof, "
            "curiosity_gap, fear_urgency, confession_vulnerability, challenge_gamified, "
            "instructional_command, reverse_psychology, numeric_specificity"
        ),
    )

    # Verbal hook
    verbal_open: str = Field(
        ..., description="First words spoken or shown (0-1.5s)"
    )

    # Visual hook
    visual_first_frame: str = Field(
        ..., description="Shot type, subject, action, props, composition"
    )

    # On-screen text
    on_screen_text: str = Field(
        ..., description="First frame + optional second beat text overlay"
    )

    # Matched pair rationale
    pairing_rationale: str = Field(
        ..., description="Why this verbal + visual combination works together"
    )

    # Edit notes (time-coded)
    edit_notes: HookEditNotes

    # Sound variants
    sound_on_variant: SoundOnVariant
    sound_off_variant: SoundOffVariant

    # Platform variants
    platform_variants: list[PlatformVariant] = Field(
        ..., min_length=2, max_length=4,
        description="Platform-specific versions (at least Meta + TikTok)"
    )

    # Risk flags
    risk_flags: list[HookRiskFlag] = Field(
        default_factory=list,
        description="Compliance/claims risk flags"
    )

    # Targeting metadata
    intended_awareness_stage: AwarenessLevel
    expected_metric_target: str = Field(
        ...,
        description=(
            "Expected hook rate tier: failing (<20%), serviceable (20-30%), "
            "good (30-40%), excellent (40-55%), elite (55%+)"
        ),
    )

    # Testing taxonomy
    hook_category_tags: list[str] = Field(
        ..., min_length=2, max_length=5,
        description="Tags for testing taxonomy (e.g. 'face_first', 'text_heavy', 'demo_open')"
    )


# ---------------------------------------------------------------------------
# Per-Script Hook Set
# ---------------------------------------------------------------------------

class ScriptHookSet(BaseModel):
    """All hook variations for a single script."""
    script_id: str = Field(
        ..., description="References the script_id from Agent 04"
    )
    concept_id: str
    idea_name: str
    funnel_stage: FunnelStage

    hooks: list[HookVariation] = Field(
        ..., min_length=3, max_length=5,
        description="3-5 hook variations for this script"
    )

    recommended_lead_hook: str = Field(
        ..., description="hook_id of the recommended primary hook for testing"
    )
    testing_notes: str = Field(
        ..., description="Notes on how to test these hooks against each other"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Hook Specialist Brief
# ---------------------------------------------------------------------------

class HookSpecialistBrief(BaseModel):
    """Complete Agent 05 output — hook variations for all 15 scripts."""
    brand_name: str
    product_name: str
    generated_date: str
    batch_id: str = Field(default="", description="Batch identifier")

    # Core output: hook sets for all scripts
    script_hook_sets: list[ScriptHookSet] = Field(
        ..., min_length=15, max_length=15,
        description="Hook variations for each of the 15 scripts"
    )

    # Summary stats
    total_hooks_generated: int = Field(
        ..., description="Total individual hook variations across all scripts"
    )
    hook_family_distribution: dict[str, int] = Field(
        ..., description="Count of hooks per hook family"
    )

    # Testing strategy
    top_hooks_to_test_first: list[str] = Field(
        ..., min_length=5, max_length=10,
        description="Top 5-10 hook_ids to prioritize for testing"
    )
    hook_testing_methodology: str = Field(
        ..., description="Recommended approach for A/B testing hooks"
    )

    # Compliance summary
    hooks_with_risk_flags: list[str] = Field(
        default_factory=list,
        description="hook_ids that have compliance risk flags"
    )

    # Trend alignment
    trend_aligned_hooks: list[str] = Field(
        default_factory=list,
        description="hook_ids that leverage current trends from Agent 1B"
    )
