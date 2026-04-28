# Transcript shadow-gate JSONL sink — design (playhead-b58j / gtt9.1.1)

## Goal

Replace `NoOpTranscriptShadowGateLogger` in production runtime with a real JSONL writer so the gtt9.1 shadow-mode acoustic transcript gate's per-shard decisions land on disk and the eval procedure (would-skip-rate + host-read miss-rate) can run.

## Context

`playhead-dzmu` (gtt9.1) shipped:

- `TranscriptShadowGateEntry` schema (Codable, `schemaVersion=1`)
- `TranscriptShadowGateLogging` protocol with `record(_:) async`
- `NoOpTranscriptShadowGateLogger` (current default in `AnalysisJobRunner`)
- `RecordingTranscriptShadowGateLogger` (test stub)
- 3 emit sites in `AnalysisJobRunner.swift` already wired

Missing: the concrete actor-backed JSONL writer, runtime wiring, build-SHA seam, source canary.

The strongest precedent is `Playhead/Services/AdDetection/DecisionLogger.swift` — actor-backed, lazy-bootstrap, 10 MB rotation, `migrate()`-deferred I/O, DEBUG-only construction in `PlayheadRuntime`. This design clones that pattern.

## Decisions (resolved during brainstorm)

1. **Schema**: bump to v2 with a new `buildCommitSHA: String?` field. v1 rows decode with `buildCommitSHA = nil`.
2. **Build-SHA seam**: build-phase script writes `Playhead/Generated/BuildInfo.swift` containing a static `commitSHA`. Generated file is gitignored.
3. **Build environment**: DEBUG-only (mirrors `DecisionLogger`). Release builds keep the no-op.
4. **Source canary**: extend the existing string-match canary table in `PlayheadRuntimeWiringSourceCanaryTests` plus a new test that pins the DEBUG construction.

## Schema (v2)

```swift
struct TranscriptShadowGateEntry: Codable, Equatable, Sendable {
    let schemaVersion: Int            // = 2
    let timestamp: Double              // unchanged: Unix epoch seconds
    let analysisAssetID: String
    let episodeID: String
    let shardID: Int
    let shardStart: Double
    let shardEnd: Double
    let likelihood: Double?
    let threshold: Double
    let decision: Decision
    let wouldGate: Bool
    let transcribed: Bool
    let buildCommitSHA: String?       // NEW. nil on v1 rows; always set on v2.

    static let currentSchemaVersion: Int = 2
}
```

Custom `Codable` impl mirrors the `DecisionLogger.LedgerEntry` pattern (`init(from:)` / `encode(to:)`) so v1 rows round-trip cleanly with `buildCommitSHA = nil` and v2 always emits the key (even when `unknown`).

The `Decision` enum is unchanged.

## `BuildInfo.commitSHA` reuse

**Discovered after design approval — playhead-gtt9.21 already shipped this seam.** `Playhead/Support/BuildInfo.swift` exposes `BuildInfo.commitSHA: String` (always non-empty by contract; falls back to `"unknown"` outside a git context). The SHA is stamped into a dedicated `BuildProvenance.plist` resource by a `postBuildScripts` block in `project.yml`, then read at runtime via `Bundle(for:)`.

This obsoletes the originally-planned "Section 2: BuildInfo.swift generation" entirely. The shadow-gate logger consumes `BuildInfo.commitSHA` directly. No new build script, no `.gitignore` edits, no `project.yml` changes.

## `TranscriptShadowGateLogger` actor

New file: `Playhead/Services/AnalysisJobRunner/TranscriptShadowGateLoggerImpl.swift`. Sibling to the existing `TranscriptShadowGateLogger.swift` (which keeps the schema + protocol + no-op). The actor file is the new code; the existing file gets the schema-v2 changes only.

Mechanical clone of `DecisionLogger`:

- `actor TranscriptShadowGateLogger: TranscriptShadowGateLogging`
- `static let activeLogFilename = "transcript-shadow-gate.jsonl"`
- `static let rotatedPrefix = "transcript-shadow-gate"`, `rotatedSuffix = ".jsonl"`
- `static let defaultRotationThresholdBytes = 10 * 1024 * 1024`
- Two inits (convenience / `directory:`); both defer Documents lookup, dir create, rotation-index scan to `ensureBootstrapped()`.
- `func migrate() throws { try ensureBootstrapped() }` — idempotent.
- `func record(_:) async` does `ensureBootstrapped → appendEntry → rotateIfNeeded`. Catches and warns via `OSLog`.
- `appendEntry` reads `BuildInfo.commitSHA` once at actor init (stored in a `let buildCommitSHA: String`) and stamps every encoded entry. JSONEncoder configured with `[.sortedKeys]` + `.iso8601` (mirrors `DecisionLogger`).
- Rotation reuses the same livelock guard (`lineCount ≤ 1` skip), `replaceItemAt`-vs-`moveItem` crash-safe swap, idempotent `nextRotationIndex` scan.
- Test hooks: `currentNextRotationIndex()`, `flushAndClose()`, `activeLogURL`, `rotatedLogURLs()`.

## `PlayheadRuntime` wiring

Three edits, all parallel to the `preBuiltDecisionLogger` block:

1. After the existing `#if DEBUG` `preBuiltDecisionLogger` block (PlayheadRuntime.swift:666-677), add:

   ```swift
   let preBuiltShadowGateLogger: TranscriptShadowGateLogging?
   #if DEBUG
   do {
       preBuiltShadowGateLogger = try TranscriptShadowGateLogger()
   } catch {
       Logger(subsystem: "com.playhead", category: "Runtime")
           .warning("TranscriptShadowGateLogger init failed — shadow logging disabled: \(error.localizedDescription, privacy: .public)")
       preBuiltShadowGateLogger = nil
   }
   #else
   preBuiltShadowGateLogger = nil
   #endif
   ```

2. At the `AnalysisJobRunner(...)` construction (around line 795), pass:

   ```swift
   transcriptShadowGateLogger: preBuiltShadowGateLogger ?? NoOpTranscriptShadowGateLogger()
   ```

3. In the deferred init Task (where `decisionLogger.migrate()` lives), add a sibling:

   ```swift
   if let shadowGateLogger = preBuiltShadowGateLogger as? TranscriptShadowGateLogger {
       do {
           try await shadowGateLogger.migrate()
       } catch {
           Logger(subsystem: "com.playhead", category: "Runtime")
               .warning("TranscriptShadowGateLogger deferred migrate failed: \(error.localizedDescription, privacy: .public)")
       }
   }
   ```

   Capture `preBuiltShadowGateLogger` in the `Task { [...] in` closure list alongside `preBuiltDecisionLogger`.

## Tests

### Logger behavior — Swift Testing

New `PlayheadTests/Services/AnalysisJobRunner/TranscriptShadowGateLoggerTests.swift`:

- `writesOneJSONLLinePerRecord`
- `appendsAcrossMultipleRecords`
- `rotatesWhenActiveFileExceedsThreshold`
- `rotationIndicesIncrementIdempotentlyAcrossReInit`
- `livelockGuardSkipsRotationWhenActiveHasOneLine`
- `everyEntryStampedWithBuildCommitSHA`
- `decodesV1RowsWithNilBuildCommitSHA` (round-trips a hand-crafted v1 line)

Tests use the `directory:` init pointing at a temp directory.

### Source canary — XCTest

Edit `PlayheadTests/App/PlayheadRuntimeWiringSourceCanaryTests.swift`:

1. Extend the `expected` table at line 108 from 5 to 6 entries: add `("shadowGateLogger.migrate()", "TranscriptShadowGateLogger")`.
2. Add `func testShadowGateLoggerIsConstructedInDebug()` that loads `PlayheadRuntime.swift` via `SwiftSourceInspector` and asserts the source contains `try TranscriptShadowGateLogger()` and the surrounding `#if DEBUG` ... `#else` / `#endif` shape (regex over the relevant region — same approach the existing canary uses for `preBuiltDecisionLogger`).

## Acceptance

- `transcript-shadow-gate.jsonl` appears in Documents on a DEBUG dogfood run with one row per gate-eligible shard.
- File rotates at 10 MB.
- Each row carries `schemaVersion=2`, a non-empty `buildCommitSHA` (or `"unknown"` if git unavailable at build time).
- Eval tooling can join `transcript-shadow-gate.jsonl × decision-log.jsonl` on `(analysisAssetID, shardStart, shardEnd)`.
- Source canary fails loudly if a future refactor drops the construction or the `migrate()` call.
- Full `PlayheadFastTests` plan green.

## Out of scope

- The eval tooling itself (separate sibling bead if it grows).
- Flipping `skipEnabled = true` (separate bead once eval data exists).
- Backfilling `DecisionLogger` to also stamp `BuildInfo.commitSHA` (small follow-up bead — `BuildInfo.swift` is reusable).

## Risks

- **Build-phase script ordering.** If the script runs after "Compile Sources" instead of before, `BuildInfo.swift` is stale on the first build of a session. Mitigation: pin the phase to the top of the build-phases list in `project.yml` and add a `Notes for implementers` line in the plan calling out the verification step.
- **`xcodegen` regeneration.** Worktrees regenerate the Xcode project per build. The script's `project.yml` entry must survive `xcodegen generate`.
- **`BuildInfo.swift` not present on first clone.** Solution: the build-phase script creates the file if missing; CI/dev cold builds run the script before compile so the file exists by the time the compiler reaches `BuildInfo.commitSHA`.
