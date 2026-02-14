#!/usr/bin/env python3
"""CLI for Agent 02 Prompt Lab (stepwise execution)."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pipeline.prompt_lab_agent02 import (
    InputDataError,
    LLMCallExecutionError,
    PromptLabError,
    PromptLabSettings,
    SchemaValidationFailure,
    create_run_directory,
    load_step1_output,
    load_step2_output,
    read_json_file,
    run_chain,
    run_step1,
    run_step2,
    run_step3,
    write_run_artifacts,
)

logger = logging.getLogger("prompt_lab_agent02")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Agent 02 Prompt Lab — run Step 1/2/3 independently with artifacts",
    )
    parser.add_argument("--run-label", default="prompt-lab", help="Label included in run directory name")
    parser.add_argument("--out-dir", default="outputs/prompt_lab", help="Output root for prompt lab runs")
    parser.add_argument(
        "--prompt-dir",
        default="prompts/prompt_lab",
        help="Directory containing optional lab prompt override markdown files",
    )
    parser.add_argument("--provider", default=None, help="Override provider for Step 1 and Step 3")
    parser.add_argument("--model", default=None, help="Override model for Step 1 and Step 3")
    parser.add_argument("--temperature", type=float, default=None, help="Override temperature for Step 1 and Step 3")
    parser.add_argument("--max-tokens", type=int, default=None, help="Override max_tokens for Step 1 and Step 3")
    parser.add_argument(
        "--strict-sdk-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If true, Step 2 must use Claude Agent SDK and fail on any SDK failure",
    )
    parser.add_argument("--force", action="store_true", help="Allow overwriting an existing run directory")

    subparsers = parser.add_subparsers(dest="command", required=True)

    p1 = subparsers.add_parser("step1", help="Run Step 1 only")
    p1.add_argument("--input-json", required=True, help="Path to prompt-lab input JSON (must include foundation_brief)")

    p2 = subparsers.add_parser("step2", help="Run Step 2 only")
    p2.add_argument("--step1-json", required=True, help="Path to Step 1 output JSON")

    p3 = subparsers.add_parser("step3", help="Run Step 3 only")
    p3.add_argument("--step1-json", required=True, help="Path to Step 1 output JSON")
    p3.add_argument("--step2-json", required=True, help="Path to Step 2 output JSON")
    p3.add_argument(
        "--input-json",
        required=False,
        default=None,
        help="Optional context JSON (brand/product/batch_id/etc.)",
    )

    chain = subparsers.add_parser("chain", help="Run Step 1 -> Step 2 -> Step 3")
    chain.add_argument("--input-json", required=True, help="Path to prompt-lab input JSON (must include foundation_brief)")

    return parser


def _settings_from_args(args: argparse.Namespace) -> PromptLabSettings:
    return PromptLabSettings(
        run_label=args.run_label,
        out_root=Path(args.out_dir),
        prompt_dir=Path(args.prompt_dir),
        provider=args.provider,
        model=args.model,
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        strict_sdk_only=bool(args.strict_sdk_only),
        force=bool(args.force),
    )


def _print_run_paths(paths: dict[str, Path]):
    print("Prompt Lab artifacts:")
    for key, path in paths.items():
        print(f"- {key}: {path}")


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = _build_parser()
    args = parser.parse_args(argv)
    settings = _settings_from_args(args)

    run_id, created_at, run_dir = create_run_directory(settings)
    run = None
    inputs_snapshot: dict[str, Any] = {}

    try:
        from pipeline.prompt_lab_agent02 import RunExecution

        run = RunExecution(
            run_id=run_id,
            created_at=created_at,
            run_dir=run_dir,
            settings=settings,
        )

        if args.command == "step1":
            input_data = read_json_file(Path(args.input_json))
            inputs_snapshot = input_data
            step_exec, _ = run_step1(input_data=input_data, run_dir=run_dir, settings=settings)
            run.steps["step1"] = step_exec

        elif args.command == "step2":
            step1 = load_step1_output(Path(args.step1_json))
            inputs_snapshot = {"step1_json": str(Path(args.step1_json))}
            step_exec, _ = run_step2(step1_output=step1, run_dir=run_dir, settings=settings)
            run.steps["step2"] = step_exec

        elif args.command == "step3":
            step1 = load_step1_output(Path(args.step1_json))
            step2 = load_step2_output(Path(args.step2_json))
            input_data = read_json_file(Path(args.input_json)) if args.input_json else {}
            inputs_snapshot = {
                "step1_json": str(Path(args.step1_json)),
                "step2_json": str(Path(args.step2_json)),
                "input_json": str(Path(args.input_json)) if args.input_json else None,
            }
            step_exec, _ = run_step3(
                input_data=input_data,
                step1_output=step1,
                step2_output=step2,
                run_dir=run_dir,
                settings=settings,
            )
            run.steps["step3"] = step_exec

        elif args.command == "chain":
            input_data = read_json_file(Path(args.input_json))
            inputs_snapshot = input_data
            run = run_chain(input_data=input_data, run_dir=run_dir, settings=settings)
            run.run_id = run_id
            run.created_at = created_at
            run.run_dir = run_dir

        paths = write_run_artifacts(run=run, inputs_snapshot=inputs_snapshot)
        _print_run_paths(paths)
        return 0

    except PromptLabError as exc:
        if run is not None:
            run.errors.append(str(exc))
            paths = write_run_artifacts(run=run, inputs_snapshot=inputs_snapshot)
            _print_run_paths(paths)
        print(f"ERROR: {exc}", file=sys.stderr)
        return exc.exit_code
    except Exception as exc:
        if run is not None:
            run.errors.append(str(exc))
            paths = write_run_artifacts(run=run, inputs_snapshot=inputs_snapshot)
            _print_run_paths(paths)
        print(f"ERROR: unexpected failure: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
