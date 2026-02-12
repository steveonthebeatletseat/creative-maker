"""Website Intelligence schema — structured output from AI-powered website scraping.

Extracted from a brand's product/landing page before the pipeline starts.
Feeds into Agent 1A (Foundation Research) and Agent 1B (Trend Intel) to
provide richer, real-data-grounded research.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class WebsiteIntel(BaseModel):
    """Structured marketing intelligence extracted from a brand's website."""

    # --- Headline & Positioning ---
    hero_headline: str = Field(
        default="",
        description="The main headline on the page (H1 or hero section).",
    )
    subheadline: str = Field(
        default="",
        description="The subheadline or supporting tagline.",
    )
    unique_selling_proposition: str = Field(
        default="",
        description="The core USP — what makes this product different, in one sentence.",
    )

    # --- Benefits & Features ---
    key_benefits: list[str] = Field(
        default_factory=list,
        description="Top benefits listed on the page (outcome-focused).",
    )
    key_features: list[str] = Field(
        default_factory=list,
        description="Product features, specs, or ingredients listed on the page.",
    )

    # --- Social Proof ---
    testimonials: list[str] = Field(
        default_factory=list,
        description="Customer testimonials or review quotes found on the page (verbatim).",
    )
    social_proof_stats: list[str] = Field(
        default_factory=list,
        description="Quantified social proof (e.g. '10,000+ sold', '4.8 stars', '500+ reviews').",
    )
    trust_signals: list[str] = Field(
        default_factory=list,
        description="Trust badges, certifications, press mentions, 'as seen in' logos.",
    )

    # --- Offer ---
    price_info: str = Field(
        default="",
        description="Pricing details found on the page (tiers, subscriptions, one-time).",
    )
    guarantee: str = Field(
        default="",
        description="Money-back guarantee or risk-reversal offer.",
    )
    bonuses_or_offers: list[str] = Field(
        default_factory=list,
        description="Special offers, bonuses, free shipping, bundles, discounts.",
    )

    # --- Brand Voice ---
    brand_voice: str = Field(
        default="",
        description="Assessment of the brand's tone and personality (e.g. 'casual and playful', 'clinical and authoritative').",
    )

    # --- Claims & Compliance ---
    claims_made: list[str] = Field(
        default_factory=list,
        description="Specific product claims made on the page (important for compliance review).",
    )

    # --- FAQ / Objections ---
    faq_items: list[str] = Field(
        default_factory=list,
        description="FAQ questions and answers found on the page (these reveal top buyer objections).",
    )

    # --- Summary ---
    page_summary: str = Field(
        default="",
        description="A condensed summary of the page's overall messaging strategy and positioning.",
    )
