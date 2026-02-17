"""Shared text filters for Phase 1 quality enforcement."""

from __future__ import annotations


_MALFORMED_QUOTE_MARKERS = (
    'quote":',
    'theme":',
    'evidence":',
    'section_title":',
    'pain":',
    'desire":',
    'differentiator":',
    'proof_style":',
    'weakness":',
    'source_type":',
    'source_url":',
)


def is_malformed_quote(text: str) -> bool:
    """Return True when quote text appears to be leaked JSON payload."""
    normalized = " ".join((text or "").strip().lower().split())
    if not normalized:
        return False
    return any(marker in normalized for marker in _MALFORMED_QUOTE_MARKERS)
