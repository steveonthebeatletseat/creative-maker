#!/usr/bin/env python3
"""Audit helper for Phase 1 outputs."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.phase1_text_filters import is_malformed_quote


def _load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text("utf-8"))


def run_audit(brand_dir: Path) -> dict:
    ledger = _load_json(brand_dir / "foundation_research_evidence_ledger.json") or []
    quality = _load_json(brand_dir / "foundation_research_quality_report.json") or {}
    output = _load_json(brand_dir / "foundation_research_output.json") or {}

    provider_counts = Counter()
    source_type_counts = Counter()
    for row in ledger:
        provider_counts[str(row.get("provider", ""))] += 1
        source_type_counts[str(row.get("source_type", ""))] += 1

    quotes = (
        output.get("pillar_2_voc_language_bank", {}).get("quotes", [])
        if isinstance(output, dict)
        else []
    )
    malformed = [
        q for q in quotes if is_malformed_quote(str(q.get("quote", "")))
    ]

    return {
        "brand_dir": str(brand_dir),
        "evidence_count": len(ledger),
        "source_type_distribution": dict(sorted(source_type_counts.items())),
        "provider_share": dict(sorted(provider_counts.items())),
        "voc_quote_count": len(quotes),
        "voc_malformed_count": len(malformed),
        "failed_gates": list(quality.get("failed_gate_ids", [])),
        "overall_pass": bool(quality.get("overall_pass", False)),
    }


def main():
    parser = argparse.ArgumentParser(description="Audit Phase 1 output quality")
    parser.add_argument(
        "--brand-dir",
        required=True,
        help="Absolute path to brand output folder (contains foundation_research_*.json files)",
    )
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="Write phase1_baseline_audit.json inside the brand directory",
    )
    args = parser.parse_args()

    brand_dir = Path(args.brand_dir).expanduser().resolve()
    audit = run_audit(brand_dir)
    print(json.dumps(audit, indent=2))

    if args.write_baseline:
        target = brand_dir / "phase1_baseline_audit.json"
        target.write_text(json.dumps(audit, indent=2), "utf-8")
        print(f"\nWrote baseline: {target}")


if __name__ == "__main__":
    main()
