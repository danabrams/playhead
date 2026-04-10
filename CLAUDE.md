# Playhead — Claude Code Instructions

## Decision Authority

**Never swap frameworks, APIs, or architectural approaches without explicit approval.** Present the options and tradeoffs, then wait for a decision. This applies to:
- Switching between Apple framework APIs (e.g. SpeechAnalyzer vs SFSpeechRecognizer)
- Adding or removing dependencies
- Changing persistence strategies
- Altering the service/actor architecture

When investigation reveals a framework is broken, present findings and proposed alternatives — don't implement the swap.

## Issue Tracking

**Use `bd` (beads, Homebrew `beads` formula) for ALL issue tracking in this repo.** Canonical data lives in `.beads/dolt/`.

**Do NOT use `br` (beads_rust, cargo `beads_rust`) even if you find it installed.** It is a separate reimplementation with its own database and issue prefix (`bd-*` vs bd's `playhead-*`); using it creates a parallel ghost tracker whose IDs never resolve in the real `bd` database. If you find `br` installed, leave it alone and use `bd`. If any command or skill suggests `br`, substitute `bd`.

## Testing

Two test plans exist. **Use the correct one for your context:**

**Per-bead work (implementation, review, fix cycles):**
```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro'
```
Skips all integration/benchmark/replay suites. Runs in ~3 minutes.

**Phase-close verification only (final gate before closing an epic):**
```bash
xcodebuild test -scheme Playhead -testPlan PlayheadIntegrationTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro'
```
Runs corpus verification, FM classifier, replay harness, pipeline benchmarks. Takes ~30 minutes. **Do not run this during per-bead work.**

The `PlayheadFastTests` plan is the default in Xcode (Cmd+U). Never run the full `-only-testing:PlayheadTests` suite without a test plan — it will include both and take 30+ minutes.
