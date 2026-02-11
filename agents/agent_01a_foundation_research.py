"""Agent 1A: Foundation Research — the truth layer everything depends on.

Cadence: quarterly (not per-batch).
Inputs: brand/product info, customer data, competitor landscape.
Outputs: FoundationResearchBrief → all downstream agents.
"""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from pipeline.base_agent import BaseAgent
from prompts.agent_01a_system import SYSTEM_PROMPT
from schemas.foundation_research import FoundationResearchBrief


class Agent01AFoundationResearch(BaseAgent):
    name = "Agent 1A: Foundation Research"
    slug = "agent_01a"
    description = (
        "Deep customer/market intelligence. Produces the foundation truth layer "
        "that all downstream agents depend on — segments, awareness playbook, "
        "VoC language bank, competitive map, angle inventory, testing plan, "
        "and compliance pre-brief."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return FoundationResearchBrief

    def build_user_prompt(self, inputs: dict[str, Any]) -> str:
        """Build user prompt from brand/product inputs.

        Expected inputs:
          - brand_name: str
          - product_name: str
          - product_description: str
          - target_market: str (geos, demographics)
          - price_point: str
          - key_differentiators: list[str]
          - customer_reviews: str (raw reviews text or summary)
          - competitor_info: str (competitor names, ad descriptions)
          - landing_page_info: str (LP copy, offer details)
          - compliance_category: str (e.g. "supplements", "skincare", "finance")
          - additional_context: str (any other relevant info)
        """
        sections = []

        sections.append("# BRAND & PRODUCT INFORMATION")
        sections.append(f"Brand: {inputs.get('brand_name', 'Unknown')}")
        sections.append(f"Product: {inputs.get('product_name', 'Unknown')}")
        sections.append(f"Description: {inputs.get('product_description', 'Not provided')}")
        sections.append(f"Target market: {inputs.get('target_market', 'Not provided')}")
        sections.append(f"Price point: {inputs.get('price_point', 'Not provided')}")

        if inputs.get("key_differentiators"):
            sections.append("\n## Key Differentiators")
            for diff in inputs["key_differentiators"]:
                sections.append(f"- {diff}")

        if inputs.get("customer_reviews"):
            sections.append("\n# CUSTOMER REVIEWS & VOC DATA")
            sections.append(inputs["customer_reviews"])

        if inputs.get("competitor_info"):
            sections.append("\n# COMPETITOR INTELLIGENCE")
            sections.append(inputs["competitor_info"])

        if inputs.get("landing_page_info"):
            sections.append("\n# LANDING PAGE & OFFER DETAILS")
            sections.append(inputs["landing_page_info"])

        if inputs.get("compliance_category"):
            sections.append(f"\n# COMPLIANCE CATEGORY: {inputs['compliance_category']}")
            sections.append(
                "Pay special attention to category-specific compliance risks, "
                "prohibited claims, and required disclaimers for this vertical."
            )

        if inputs.get("previous_performance"):
            sections.append("\n# PREVIOUS CREATIVE PERFORMANCE DATA")
            sections.append(inputs["previous_performance"])

        if inputs.get("additional_context"):
            sections.append("\n# ADDITIONAL CONTEXT")
            sections.append(inputs["additional_context"])

        sections.append(
            "\n# YOUR TASK\n"
            "Produce a complete Foundation Research Brief covering ALL 9 sections:\n"
            "1. Category Snapshot\n"
            "2. Segmentation (3-7 buckets)\n"
            "3. Schwartz Awareness Playbook (per segment)\n"
            "4. Market Sophistication Diagnosis\n"
            "5. VoC Language Bank (verbatim entries)\n"
            "6. Competitive Messaging Map + White Space\n"
            "7. Angle Inventory (20-60 angles)\n"
            "8. Testing Plan\n"
            "9. Compliance Pre-Brief\n\n"
            "Be thorough and specific. Every angle needs hook templates. "
            "Every segment needs objection handling. "
            "Every claim needs a compliance risk rating."
        )

        return "\n".join(sections)
