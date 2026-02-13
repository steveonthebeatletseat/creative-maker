"""Agent 02 output schema — Creative Engine.

2-step agent:
  Step 1: Find marketing angles from Foundation Research (structured LLM)
  Step 2: Web crawl for best video styles per angle (Gemini Deep Research)
  Step 3: Merge into final output with 1-3 video concepts per angle

Each marketing angle is grounded in segments/desires/VoC/white space.
Each video concept option is informed by real-world ad research.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from schemas.foundation_research import (
    AwarenessLevel,
    ComplianceRisk,
    FunnelStage,
    ProofType,
)


# ---------------------------------------------------------------------------
# Video Concept Option (from web research)
# ---------------------------------------------------------------------------

class VideoConceptOption(BaseModel):
    """A specific video format/style option for executing a marketing angle.
    Informed by web research on what's actually working."""
    concept_name: str = Field(
        ..., description="Short label for this concept, e.g. 'ASMR Unboxing Demo'"
    )
    video_format: str = Field(
        ..., description="The video format/style: e.g. 'ASMR demo', 'green screen reaction', "
        "'founder story', 'POV challenge', 'day in the life', 'split-screen comparison'"
    )
    scene_concept: str = Field(
        ..., description="Vivid, filmable description of what the viewer sees — "
        "specific enough that a creative director could brief a team on it"
    )
    why_this_format: str = Field(
        ..., description="Why this format is the best choice for this specific angle — "
        "how the format serves the persuasion goal"
    )
    reference_examples: str = Field(
        ..., description="Real-world examples or patterns found during web research "
        "that inspired this concept"
    )
    platform_targets: list[str] = Field(
        ..., description="Target platforms: meta_reels, tiktok, meta_feed, ig_stories, etc."
    )
    sound_music_direction: str = Field(
        ..., description="Sound/music approach for this concept"
    )
    proof_approach: ProofType = Field(
        ..., description="How proof is delivered in this concept"
    )
    proof_description: str = Field(
        ..., description="What specific proof would be shown and how"
    )


# ---------------------------------------------------------------------------
# Marketing Angle (from Foundation Research)
# ---------------------------------------------------------------------------

class MarketingAngle(BaseModel):
    """A single marketing angle — a strategic persuasion hypothesis
    grounded in Foundation Research data."""
    angle_id: str = Field(
        ..., description="Unique ID: e.g. tof_01, mof_03, bof_02"
    )
    funnel_stage: FunnelStage
    angle_name: str = Field(
        ..., description="Descriptive name for this angle"
    )

    # Strategic grounding (all from Foundation Research)
    target_segment: str = Field(
        ..., description="Which segment from the Foundation Research this targets (by name)"
    )
    target_awareness: AwarenessLevel = Field(
        ..., description="Awareness level this angle is designed for"
    )
    core_desire: str = Field(
        ..., description="The specific desire from the segment data this angle addresses"
    )
    emotional_lever: str = Field(
        ..., description="Primary emotion activated: relief, pride, disgust, hope, fear, "
        "curiosity, anger, belonging, FOMO, etc."
    )
    voc_anchor: str = Field(
        ..., description="Verbatim customer language from the VoC library this angle is built on"
    )
    white_space_link: str = Field(
        ..., description="Which competitive gap or white-space hypothesis this angle exploits"
    )
    mechanism_hint: str = Field(
        ..., description="The 'why it works differently' unique mechanism for this angle"
    )
    objection_addressed: str = Field(
        ..., description="The key objection this angle handles"
    )

    # Video concepts (from web research, 1-3 options)
    video_concepts: list[VideoConceptOption] = Field(
        ..., min_length=1, max_length=3,
        description="1-3 video concept options for executing this angle, "
        "each informed by web research on what's working"
    )


# ---------------------------------------------------------------------------
# Step 1 Output — Marketing Angles Only (intermediate)
# ---------------------------------------------------------------------------

class MarketingAngleStep1(BaseModel):
    """Intermediate output from Step 1 — angles without video concepts yet."""
    angle_id: str
    funnel_stage: FunnelStage
    angle_name: str
    target_segment: str
    target_awareness: AwarenessLevel
    core_desire: str
    emotional_lever: str
    voc_anchor: str
    white_space_link: str
    mechanism_hint: str
    objection_addressed: str


class Step1Output(BaseModel):
    """Step 1 output — marketing angles from Foundation Research."""
    brand_name: str
    product_name: str
    angles: list[MarketingAngleStep1] = Field(
        ..., description="Marketing angles, one per requested ad slot"
    )


# ---------------------------------------------------------------------------
# Top-Level Output — Creative Engine Brief
# ---------------------------------------------------------------------------

class CreativeEngineBrief(BaseModel):
    """Complete Agent 02 output — marketing angles paired with
    web-researched video concept options.

    The user will review and select which video concepts to pass
    to the Copywriter.
    """
    brand_name: str
    product_name: str
    generated_date: str
    batch_id: str = Field(default="", description="Batch identifier")

    # Core output: angles with video concepts
    angles: list[MarketingAngle] = Field(
        ..., description="Marketing angles, each with 1-3 video concept options"
    )
