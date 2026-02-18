# Hook Generator UI Spec

## Goal
Hook Generator should mirror the Script Writer section style so operators can scan and control it with the same mental model.

## Section Layout
- **Panel card**: collapsible, bordered container with title row and status pill.
- **Section title**: `Hook Generator`.
- **Header click target**: clicking the header toggles collapsed state.
- **Status pill**: should reflect `Idle`, `Running`, `Completed`, or `Failed`.
- **Body controls (when expanded)**:
  - `Run Hooks` button.
  - `Candidates / Unit` numeric input.
  - `Final Variants / Unit` numeric input.
  - Prepare/Progress text row.
  - Hook results grid/list.

## Visual Rules
- Match Script Writer container rhythm:
  - Rounded card shell, subtle border, card-level hover behavior.
  - Same icon + title block style.
  - Same spacing for heading and body padding.
- Hook panel accent: subtle warm/glow tint only (no new visual theme).
- Avoid additional nested “expand / collapse” buttons; header click is sufficient.

## Behavioral Rules
- Default state: collapsed.
- Open/closed state should persist per branch when switching branches.
- Collapse state should not force-clear run status or results.
- If Phase 3B prerequisites are missing, hide the entire panel.
- On each refresh/rerender, only update state; never re-layout the card unexpectedly.
- Hook cards: clicking anywhere on a card should toggle selection for that hook.
- Clicking a selected card should clear that selection (return the unit to pending, not skipped).
- Keyboard accessibility:
  - Hook cards should be focusable (`Tab`) and toggle on `Enter`/`Space`.
  - The radio/select control should keep working and mirror the same toggle behavior.

## Data/Status Behavior
- `Run Hooks` is disabled while running or when run is locked.
- `runBtn` re-enables only when hook stage is idle and lock is not active.
- Progress text must always show selection count and ready state (`Selected X/Y · stale N · Scene Handoff ...`).

## Failure/Empty States
- If no script run selected: message should be `Select a run to open Hook Generator.`
- If no eligible units: show a clear “No eligible units for hooks” state with skipped count.
