# playhead-zx6i — B4: Fast Local Revalidation From Persisted Features

**Branch:** `bead/playhead-zx6i`
**Worktree:** `/Users/dabrams/playhead/.worktrees/bd-zx6i`
**Foundation:** playhead-7mq (version columns), Phase 4 shadow harness, Phase 3 shadow harness.

## 1. Problem

When the ad-detection `model_version`, `policy_version`, or
`feature_schema_version` is bumped (e.g. tuning a rule, releasing a new
classifier seed), every previously-analyzed episode is silently
invalidated. Today the only way to refresh those decisions is a full
re-analysis: decode → feature extraction → ASR → classifier → fusion →
boundary refinement. The first three stages dominate runtime by 10×+
and produce byte-identical outputs across the version bump — the
`TranscriptChunk` + `FeatureWindow` rows persisted on the prior run are
still valid.

B4's job is to skip those three deterministic stages and re-run only
classifier + fusion + boundary against the persisted rows when the
version triple has changed.

## 2. Scope

**IN:**
1. New feature flag `b4_revalidation_from_features_enabled` (default OFF) in `PreAnalysisConfig`.
2. New `PipelineVersions` struct + `current()` reader threading the three version axes.
3. New `RevalidationStateStore` — UserDefaults-backed per-asset snapshot of "last completed PipelineVersions".
4. New `AdDetectionService.revalidateFromFeatures(...)` entry point that delegates to existing `runBackfill` logic (which already consumes persisted chunks + features).
5. `AnalysisJobRunner.run` short-circuit: when flag ON + persisted chunks exist + version-bump detected, skip stages 1–3 and call the revalidation entry point.
6. Stamp current versions on successful `runBackfill` completion so subsequent runs can detect bumps.
7. Diagnostics toggle for the flag, mirroring xr3t/24cm/2hpn wiring.
8. Tests: unit (state store, version detection, flag gating), integration (Phase 3 harness — versions stamped after first run; second run with bumped versions takes the revalidation path; no false_ready_rate regression).

**OUT:**
- Modifying ASR / transcription pipeline (constraint).
- Changing feature-extraction outputs (constraint).
- Touching beh3 or 2hpn code (constraint).
- Server-driven revalidation triggers (later bead).
- Cross-asset revalidation queueing (later bead — this bead is per-asset, lazy, triggered on next analysis pass).

## 3. Design

### 3.1 Version triple

```swift
struct PipelineVersions: Sendable, Equatable, Codable {
    let modelVersion: String              // AdDetectionConfig.default.detectorVersion
    let policyVersion: String             // SkipPolicyConfig.default.policyVersion
    let featureSchemaVersion: Int         // SharedVersionConstants.featureSchemaVersion

    static func current() -> PipelineVersions
}
```

`current()` reads from the three canonical sources. The 7mq sentinel
(`'pre-instrumentation' / 0 / 0`) is defined on `PipelineVersions` for
documentation/regression-test parity, but the state-store never
writes a sentinel value — `recordCompleted` is only called inside the
flag-ON stamp-write path of `runBackfill`, which always writes
`PipelineVersions.current()`. The sentinel is therefore handled
implicitly by `!=`: any persisted snapshot that decodes to the
sentinel is structurally inequal to a non-sentinel `current()` and
flows down the "version bump needed" branch. No bespoke sentinel
check is required in the consumer. (Originally R1 doc audit corrected
a misleading claim that the state store explicitly recognised the
sentinel.)

### 3.2 Per-asset state store

UserDefaults-backed, keyed by `analysisAssetId`. Mirrors
`LightweightInventoryChecksSettings`'s namespacing convention but is
implemented as a stateless `enum` (every method `static`; no instance
state, hence `enum` rather than `struct`):

```swift
enum RevalidationStateStore {
    static let keyPrefix = "playhead.zx6i.completedVersions."

    static func loadCompletedVersions(forAsset: String,
                                      defaults: UserDefaults = .standard) -> PipelineVersions?
    static func recordCompleted(versions: PipelineVersions,
                                forAsset: String,
                                defaults: UserDefaults = .standard)
    static func clear(forAsset: String,
                      defaults: UserDefaults = .standard)   // tests-only; no production caller
}
```

JSON-encoded per asset under namespaced key
`playhead.zx6i.completedVersions.<assetId>`. Absent value → `nil` → caller
treats as "needs analysis" (cold start). Stored value mismatch with
`PipelineVersions.current()` → "needs revalidation".

Why UserDefaults and not the playhead-7mq schema columns? The 7mq
columns are row-level (per-chunk, per-window). The "last completed
analysis versions" is asset-level. Asset-level metadata in UserDefaults
matches existing patterns (`xr3t`, `2hpn`); a new SwiftData column on
`analysis_assets` is a heavier migration for the same information.
Future bead can promote to SwiftData if cross-process visibility is
needed (extension targets, etc.).

### 3.3 AdDetectionService entry point

```swift
extension AdDetectionService {
    /// B4 fast revalidation: consumes persisted TranscriptChunk +
    /// FeatureWindow rows, re-runs classifier + fusion + boundary
    /// stages. ASR is NOT re-run.
    func revalidateFromFeatures(
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double,
        sessionId: String? = nil
    ) async throws
}
```

Implementation: fetch persisted chunks via `store.fetchTranscriptChunks`,
delegate to `runBackfill(chunks:...)`. The existing `runBackfill`
already (a) accepts chunks as a parameter rather than re-running ASR,
(b) fetches FeatureWindows internally. So this is a thin adapter, not a
duplicated pipeline.

### 3.4 Successful-completion stamp

At the end of `runBackfill` (after the cascade fully succeeds —
classifier + fusion + boundary all wrote), stamp
`PipelineVersions.current()` to `RevalidationStateStore` for this
asset. This is the only producer; the only consumer is the
`AnalysisJobRunner` short-circuit decision.

The flag-gate that wraps the stamp-write re-loads the flag LIVE via
`PreAnalysisConfig.load()` rather than reading a snapshot captured at
`AdDetectionService` init. See §3.5.1 for the rationale; the short
version is "producer and consumer must agree on the live value,
otherwise a mid-session flag-ON unlocks the consumer but leaves the
producer's cached `false` in place, so no stamp ever lands."

A `runBackfill` that exits early via `guard !chunks.isEmpty else {
return }` does NOT stamp (no fusion ran, no decisions to validate).

### 3.5 AnalysisJobRunner short-circuit

In `run(_:)`, before the `decode` stage. The gate is read LIVE on
every call (see §3.5.1 for asymmetry rationale):

```swift
if b4RevalidationEnabledProvider() {                       // live read of PreAnalysisConfig
    let persistedChunks = (try? await store.fetchTranscriptChunks(assetId: assetId)) ?? []
    if !persistedChunks.isEmpty,
       let completed = completedPipelineVersionsLoader(assetId) {
        let current = currentPipelineVersionsProvider()
        if completed != current {
            // Revalidate-from-features path: skip decode/features/ASR.
            try await adDetection.revalidateFromFeatures(...)
            return makeOutcome(...)
        }
    }
}
```

If chunks exist but versions match → no work needed (fall through to
the existing skip-hot-path / skip-backfill no-op branches, which already
handle this case).

If chunks exist but `completed == nil` → this is a pre-zx6i asset; we
take the full re-analysis path to populate the stamp. After the run
completes, the stamp is set; subsequent version bumps will take the
revalidation fast path.

If chunks are empty → no persisted state to revalidate from; full
analysis required.

If chunks + stamp + bump are all met BUT
`analysis_assets.episodeDurationSec` is NULL or zero → fall through to
full analysis. Rationale: the classifier's per-span position priors
need `episodeDuration` as a denominator (early-position vs late-
position weighting). The full-path stage 1 computes this from the
freshly decoded shards; the revalidation path skips decode, so it
relies on the cached `analysis_assets.episodeDurationSec` column
written by playhead-5uvz.6's gap-7 fix. A NULL column means the asset
predates 5uvz.6 or never completed stage 1 successfully; one full
re-analysis re-populates the column and unlocks the fast path for
subsequent bumps. Pinned by
`AnalysisJobRunnerRevalidationShortCircuitTests.missingDurationFallsBackToFullPath`.

### 3.5.1 Asymmetry: zx6i is live-read; 2hpn / xr3t are snapshot-at-init

The other `PreAnalysisConfig`-gated flags in this codebase (2hpn
`scopedMusicBedGeneralization`, xr3t lightweight-inventory) cache the
flag value at consumer-init. Flipping those in Settings takes effect
on the next consumer construction — typically next app launch.

The zx6i wiring deliberately diverges. Both the consumer
(`AnalysisJobRunner.b4RevalidationEnabledProvider`'s default closure)
and the producer (`AdDetectionService.runBackfill`'s stamp-write gate)
re-read `PreAnalysisConfig.load().b4RevalidationFromFeaturesEnabled` on
every call. Rollback latency for the zx6i flag is therefore the next
analysis run, not next app launch.

Rationale:
1. zx6i gates a perf optimization that can move the
   `false_ready_rate`. Instant rollback minimizes the blast radius if
   a beta cohort surfaces a regression.
2. Producer/consumer parity. If the consumer is live and the producer
   is cached, a mid-session flag-ON unlocks the consumer but the
   producer never writes a stamp — so the feature silently never
   activates until the next launch and the gates disagree on the same
   value. R1 review caught and fixed exactly this bug. Keeping both
   live is the only configuration where producer and consumer agree
   for arbitrary flag-flip timing.
3. Cost. `PreAnalysisConfig.load()` reads UserDefaults
   (in-memory-cached after first sync) and JSON-decodes ~200 bytes per
   call. For one call per `run(_:)` and one per successful `runBackfill`,
   the overhead is in the microseconds range — well below the cost of
   the rest of the pipeline. We have not regressed any benchmark.

Mid-job flag-flip semantics (formally enumerated for the four cases):

| Flag at `run(_:)` top | Flag at stamp-write | Resulting behavior                                     |
|---|---|---|
| OFF                   | OFF                 | No short-circuit, no stamp. Byte-identical to pre-zx6i. |
| OFF                   | ON                  | No short-circuit on this run, but stamp writes. Next run with bumped versions revalidates. |
| ON                    | OFF                 | Short-circuit may have fired this run; no NEW stamp is written, but the PRIOR stamp (which was a precondition for the short-circuit firing) remains in place unchanged. `RevalidationStateStore.recordCompleted` only writes; it never clears, and there is no flag-OFF eviction. If the next run has flag ON and versions are still bumped, the same prior stamp triggers an idempotent re-revalidation; if the next run has flag OFF, the prior stamp is simply unread. The "cold-start" branch is NOT taken — that branch requires `completedPipelineVersionsLoader` to return `nil`. |
| ON                    | ON                  | Normal happy path. Stamp lands, future bumps revalidate. |

All four are intentional and observably correct (see
`AnalysisJobRunnerRevalidationShortCircuitTests` for ON/OFF
short-circuit coverage and `AdDetectionServiceRevalidationTests` for
ON/OFF stamp coverage).

### 3.6 Flag gating

`PreAnalysisConfig.b4RevalidationFromFeaturesEnabled: Bool = false`.

When `false`: `AnalysisJobRunner` never takes the short-circuit branch
(the `if b4RevalidationEnabledProvider()` guard at the top of `run(_:)`
short-circuits BEFORE any `loadCompletedVersions` read), and the
producer-side stamp-write inside `runBackfill` is also guarded by the
live flag read — so no NEW stamp is written. Runtime behavior is
byte-identical to pre-zx6i. Note however that flag-OFF does NOT clear
existing storage: any stamp written during a prior flag-ON period
remains in UserDefaults under
`playhead.zx6i.completedVersions.<assetId>` and is simply unread. See
§3.5.1 truth-table row 3 — that single row covers BOTH the ON→OFF
"no NEW stamp + prior remains" outcome AND the subsequent ON→ON
"idempotent re-revalidation off the unchanged prior stamp" outcome
(both consequences are documented inline within the row's "Resulting
behavior" cell).

When `true`: the short-circuit fires when the version triple has
bumped. Stamp is written at end of every successful `runBackfill` so
the next bump will detect.

### 3.7 Settings toggle

Add `slug == "zx6i"` branch to the existing
`Toggle…featureFlagValues` set closure in `SettingsView.swift`. Add
`featureFlagValues["zx6i"] = pre.b4RevalidationFromFeaturesEnabled` in
the `.onAppear` hydration. Slug already exists in
`FeatureFlagPlaceholders.orderedSlugs`.

## 4. Acceptance Criteria

- **AC1:** With flag OFF, `Phase3ShadowReplayHarnessTests` passes
  byte-identically (no new code path is taken).
- **AC2:** With flag ON, after a first `runBackfill`, the state store
  records the current `PipelineVersions`.
- **AC3:** With flag ON + versions changed (test injects a different
  `policyVersion`-equivalent), `AnalysisJobRunner.run` takes the
  revalidation path: no `audioProvider.decode`, no
  `featureService.extractAndPersist`, no `transcriptEngine.startTranscription`
  calls (verified via stub call-count assertions).
- **AC4:** With flag ON + versions unchanged, no revalidation is
  triggered.
- **AC5:** `false_ready_rate` parity check against the lexical-only
  baseline (same assertion shape as
  `shadowHarnessPreservesLexicalCueCount`): the set of `AdWindow`s the
  revalidation path produces equals the set produced by a fresh
  `runBackfill` over the same chunks.

## 5. File Plan

**New:**
- `Playhead/Services/AdDetection/PipelineVersions.swift`
- `Playhead/Services/AdDetection/RevalidationStateStore.swift`
- `PlayheadTests/Services/AdDetection/PipelineVersionsTests.swift`
- `PlayheadTests/Services/AdDetection/RevalidationStateStoreTests.swift`
- `PlayheadTests/Services/AdDetection/AdDetectionServiceRevalidationTests.swift`
- `PlayheadTests/Services/AnalysisJobRunner/AnalysisJobRunnerRevalidationShortCircuitTests.swift`

**Edit:**
- `Playhead/Services/PreAnalysis/PreAnalysisConfig.swift` — add flag field, init param, custom-decoder fallback, coding key.
- `Playhead/Services/AdDetection/AdDetectionService.swift` — add `revalidateFromFeatures` method + protocol member; stamp completion at end of `runBackfill`.
- `Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift` — short-circuit branch at top of `run(_:)`.
- `Playhead/Views/Settings/SettingsView.swift` — onAppear hydration + zx6i toggle wiring.
- `PlayheadTests/Helpers/Stubs.swift` — `StubAdDetectionProvider.revalidateFromFeatures` to satisfy protocol.

## 6. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| The "completed versions" stamp writes even on a partial run, causing future bumps to skip a needed full re-analysis. | Stamp lives at end of `runBackfill` after all stages succeeded. Failure paths return / throw early without stamping. |
| `PipelineVersions.current()` returns a different triple under DEBUG vs RELEASE (e.g. dev override) and triggers spurious revalidation. | All three sources are static `let`s today; if a future override is added it must be opt-in and stamped to a parallel key. (Out of scope for this bead.) |
| Per-asset UserDefaults growth (one row per asset, never pruned). | Stamp value is ~120 bytes; 10k episodes = 1.2 MB. Acceptable. Pruning hook can be added later if an issue. |
| Race between the short-circuit reading `loadCompletedVersions` and a concurrent stamp from another in-flight run on the same asset. | `AnalysisJobRunner.run` is single-shot per asset (already enforced by the analysis-jobs lease); concurrent runs against the same asset don't occur in production. UserDefaults reads/writes are atomic. |
| Flag-OFF callers writing the stamp would couple OFF behavior to the new code. | Stamp is gated on `preAnalysisConfig.b4RevalidationFromFeaturesEnabled` at the call site. Flag OFF → no stamp, no read, full backward compatibility. |

## 7. Implementation Order

1. `PipelineVersions.swift` (no dependencies).
2. `RevalidationStateStore.swift` (depends on `PipelineVersions`).
3. `PreAnalysisConfig` flag (Codable contract — needs the custom-decoder fallback so old blobs decode).
4. `AdDetectionService.revalidateFromFeatures` + protocol member + completion stamp.
5. `AnalysisJobRunner.run` short-circuit.
6. `StubAdDetectionProvider` update (compile-fix for the protocol change).
7. `SettingsView` toggle wiring.
8. Unit tests (1, 2, 3, 4).
9. Integration tests (5, 6).
10. Adversarial self-review pass.
