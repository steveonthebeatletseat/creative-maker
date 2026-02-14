# Agent 02 Prompt Lab

Standalone prompt-iteration harness for Creative Engine (Agent 02).

## Goals

- Run Step 1, Step 2, Step 3 independently.
- Inspect exact system/user prompts and parsed outputs per step.
- Keep production server and pipeline flow untouched.
- Enforce strict Claude Agent SDK-only behavior for Step 2 by default.

## Files

- CLI: `scripts/prompt_lab_agent02.py`
- Runner module: `pipeline/prompt_lab_agent02.py`
- Prompt overrides:
  - `prompts/prompt_lab/agent_02_step1_system.md`
  - `prompts/prompt_lab/agent_02_step2_system.md`
  - `prompts/prompt_lab/agent_02_step3_system.md`

## Usage

Run from repo root.

```bash
.venv/bin/python scripts/prompt_lab_agent02.py --help
```

### Step 1 only

```bash
.venv/bin/python scripts/prompt_lab_agent02.py step1 \
  --input-json /absolute/path/to/input_with_foundation_brief.json
```

### Step 2 only

```bash
.venv/bin/python scripts/prompt_lab_agent02.py step2 \
  --step1-json /absolute/path/to/step1_response_parsed.json
```

### Step 3 only

```bash
.venv/bin/python scripts/prompt_lab_agent02.py step3 \
  --step1-json /absolute/path/to/step1_response_parsed.json \
  --step2-json /absolute/path/to/step2_response_parsed.json \
  --input-json /absolute/path/to/optional_context.json
```

### Full chain (1 -> 2 -> 3)

```bash
.venv/bin/python scripts/prompt_lab_agent02.py chain \
  --input-json /absolute/path/to/input_with_foundation_brief.json
```

## Defaults

- Output root: `outputs/prompt_lab/`
- Prompt override dir: `prompts/prompt_lab/`
- Step 2 strict mode: `--strict-sdk-only` (default true)
- Model/provider defaults: inherited from `config.py` unless overridden by flags.

## Artifacts

Each run writes:

- `manifest.json`
- `summary.md`
- `inputs_snapshot.json`

Each step writes:

- `system_prompt.md`
- `user_prompt.md`
- `response_parsed.json`
- `response_raw.txt`
- `validation.json`
- `meta.json`

If raw text is unavailable in the call path, `response_raw.txt` contains:

- `RAW_NOT_CAPTURED_IN_THIS_PATH`

## Exit Codes

- `2`: input error
- `3`: prompt resolution error
- `4`: LLM/SDK execution error
- `5`: schema validation failure

## Notes

- Step 2 strict mode will fail if `ANTHROPIC_API_KEY` is missing or SDK call fails.
- Prompt overrides are optional. If missing, defaults from `prompts/agent_02_system.py` are used.
