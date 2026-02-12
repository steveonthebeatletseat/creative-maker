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
        "VoC language bank, competitive map, and compliance pre-brief."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def system_prompt(self) -> str:
        return SYSTEM_PROMPT

    @property
    def output_schema(self) -> type[BaseModel]:
        return FoundationResearchBrief

    def run(self, inputs: dict[str, Any]) -> BaseModel:
        """Use Deep Research for web intelligence, then structured parse."""
        return self.run_with_deep_research(inputs)

    def build_research_prompt(self, inputs: dict[str, Any]) -> str:
        """Build the prompt for Gemini Deep Research web intelligence.

        This prompt tells the Deep Research agent what to investigate
        on the web — customer reviews, competitor ads, market dynamics.
        """
        brand = inputs.get("brand_name", "Unknown Brand")
        product = inputs.get("product_name", "Unknown Product")
        description = inputs.get("product_description", "")
        niche = inputs.get("niche", "")
        market = inputs.get("target_market", "")
        price = inputs.get("price_point", "")
        website = inputs.get("website_url", "")

        prompt = f"""Conduct deep market research for the brand "{brand}" and their product "{product}".

Product description: {description}
Category/niche: {niche}
Target market: {market}
Price point: {price}
{"Website: " + website if website else ""}

Research the following and provide a comprehensive report:

1. CUSTOMER VOICE & REVIEWS
   - Find real customer reviews on Amazon, Reddit, TikTok, YouTube, forums
   - Capture exact verbatim language: complaints, praise, "I wish...", "I was afraid..."
   - Identify before/after transformation stories
   - Note the emotional language customers use (frustration, relief, excitement)
   - Find objections people raise before buying

2. COMPETITOR LANDSCAPE
   - Identify the top 10-20 competitors in this space
   - Analyze their primary claims, mechanisms, and proof styles
   - What promises do competitors lead with?
   - What creative formats dominate (UGC, demos, founder stories)?
   - What claims are overused/saturated?

3. MARKET DYNAMICS
   - How sophisticated is this market? Are customers skeptical?
   - What buying triggers exist (seasonality, events, life changes)?
   - What are the dominant buying channels?
   - What substitutes exist (doing nothing, DIY, professional services)?

4. DEMAND SIGNALS
   - What are people searching for related to this product?
   - What questions do people ask before buying?
   - What content trends exist around this category on TikTok/YouTube/Reddit?

5. COMPLIANCE & CLAIMS LANDSCAPE
   - What types of claims are common in this category?
   - Are there regulated claim areas (health, income, before/after)?
   - What platform-specific ad policies apply (Meta, TikTok)?

Format your report with clear sections, include source URLs where possible,
and prioritize VERBATIM customer language over summaries.
"""
        return prompt

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

        # Website intelligence (auto-scraped from brand's website)
        if inputs.get("website_intel") and isinstance(inputs["website_intel"], dict):
            wi = inputs["website_intel"]
            sections.append("\n# WEBSITE INTELLIGENCE (auto-scraped from brand website)")
            sections.append(
                "The following data was automatically extracted from the brand's live website. "
                "Use it as a PRIMARY source — it reflects how the brand positions itself today."
            )
            if wi.get("hero_headline"):
                sections.append(f"\n## Headline & Positioning")
                sections.append(f"Hero headline: {wi['hero_headline']}")
                if wi.get("subheadline"):
                    sections.append(f"Subheadline: {wi['subheadline']}")
                if wi.get("unique_selling_proposition"):
                    sections.append(f"USP: {wi['unique_selling_proposition']}")
            if wi.get("key_benefits"):
                sections.append(f"\n## Key Benefits (from website)")
                for b in wi["key_benefits"]:
                    sections.append(f"- {b}")
            if wi.get("key_features"):
                sections.append(f"\n## Key Features (from website)")
                for f in wi["key_features"]:
                    sections.append(f"- {f}")
            if wi.get("testimonials"):
                sections.append(f"\n## Customer Testimonials (from website — VERBATIM)")
                sections.append(
                    "These are real customer voices. Mine them for VoC language, "
                    "before/after states, objection handling, and proof points."
                )
                for t in wi["testimonials"]:
                    sections.append(f'- "{t}"')
            if wi.get("social_proof_stats"):
                sections.append(f"\n## Social Proof Stats")
                for s in wi["social_proof_stats"]:
                    sections.append(f"- {s}")
            if wi.get("trust_signals"):
                sections.append(f"\n## Trust Signals")
                for ts_item in wi["trust_signals"]:
                    sections.append(f"- {ts_item}")
            if wi.get("price_info"):
                sections.append(f"\n## Pricing: {wi['price_info']}")
            if wi.get("guarantee"):
                sections.append(f"## Guarantee: {wi['guarantee']}")
            if wi.get("bonuses_or_offers"):
                sections.append(f"\n## Offers & Bonuses")
                for o in wi["bonuses_or_offers"]:
                    sections.append(f"- {o}")
            if wi.get("brand_voice"):
                sections.append(f"\n## Brand Voice: {wi['brand_voice']}")
            if wi.get("claims_made"):
                sections.append(f"\n## Claims Made on Website (COMPLIANCE-CRITICAL)")
                sections.append(
                    "Flag any claims that may need softening or disclaimers for paid social."
                )
                for c in wi["claims_made"]:
                    sections.append(f"- {c}")
            if wi.get("faq_items"):
                sections.append(f"\n## FAQ (reveals top buyer objections)")
                for faq in wi["faq_items"]:
                    sections.append(f"- {faq}")
            if wi.get("page_summary"):
                sections.append(f"\n## Page Summary: {wi['page_summary']}")

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

        # Deep Research report (injected by run_with_deep_research)
        if inputs.get("_deep_research_report"):
            sections.append(
                "\n# WEB RESEARCH REPORT (from Gemini Deep Research)\n"
                "The following is a comprehensive research report gathered from live web sources "
                "including customer reviews, competitor analysis, market data, and more. "
                "This is your PRIMARY intelligence source — use the verbatim customer language, "
                "competitor claims, and market signals found here as the foundation for your analysis.\n"
            )
            sections.append(inputs["_deep_research_report"])

        sections.append(
            "\n# YOUR TASK\n"
            "Produce a complete Foundation Research Brief covering ALL 7 sections:\n"
            "1. Category Snapshot\n"
            "2. Segmentation (3-7 buckets)\n"
            "3. Schwartz Awareness Playbook (per segment)\n"
            "4. Market Sophistication Diagnosis\n"
            "5. VoC Language Bank (verbatim entries — go DEEP, this is the raw material for angles)\n"
            "6. Competitive Messaging Map + White Space\n"
            "7. Compliance Pre-Brief\n\n"
            "NOTE: You do NOT produce angles or a testing plan. A dedicated Angle Architect\n"
            "(Agent 1A2) will receive your full output and build grounded angles from it.\n\n"
            "Be thorough and specific. Every segment needs rich objection handling.\n"
            "Every VoC entry should be tagged with segment, awareness level, and intensity.\n"
            "Every white space hypothesis needs evidence and risk assessment.\n"
            "The deeper your research, the better the downstream angles will be."
        )

        return "\n".join(sections)
