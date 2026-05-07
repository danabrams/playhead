// PersistedTerminalStateReconcileTests.swift
// playhead-hygc.1.3: launch-time one-shot sweep that repairs persisted
// terminal-completion rows whose `analysisState` contradicts the
// canonical coverage proven by transcript chunks + `episodeDurationSec`.
//
// Background: the May 6 dogfood export contained assets stamped
// `analysisState=completeFull` whose persisted coverage falsified the
// terminal label. Concrete examples from the bead:
//   - `3B96D187`: completeFull, terminalReason "transcript 1.000,
//     feature 0.999", but fast transcript watermark = 1.5min on a
//     36.4min episode; no final-pass coverage stored.
//   - `8A9DFC82`: completeFull, terminalReason "transcript 1.163,
//     feature 1.724", stored feature coverage 21.5min on 94.1min
//     episode, transcript watermark 1.5min.
//   - `9C109975`: completeFull, fast transcript watermark 1.5min on
//     66.0min episode, but transcript chunks prove more coverage.
//   - `E8F0F867`: terminalReason ratios > 1.0 (transcript 6.891,
//     feature 8.106) — denominator math was broken in older builds.
//
// The live `classifyBackfillTerminal` is already strong against these
// shapes after FinalizeBackfillDenominatorFixTests / ClassifyBackfillTerminalTests
// landed; this sweep exists to heal pre-existing bad rows that older
// builds wrote.
//
// Tests pin the contract:
//   - rows that fail the invariant get reclassified to a non-completion
//     terminal (failedFeature / failedTranscript / completeTranscriptPartial)
//   - the original `terminalReason` is preserved as audit trail in the
//     new reason via the `[autoRepaired:...]` prefix
//   - healthy `completeFull` rows are unchanged
//   - unknown duration is fail-safe (skip, not destructive)
//   - the run is idempotent across launches via
//     `_meta.did_terminal_state_reconcile_v1`
//   - non-completion terminals (failedTranscript, etc.) are never
//     touched by this sweep
//   - parseHighestRatioInTerminalReason correctly extracts ratios > 1.0
//     and ignores the audit-prefix tail of an already-repaired reason

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisCoordinator – reconcilePersistedTerminalStatesIfNeeded (hygc.1.3)", .serialized)
struct PersistedTerminalStateReconcileTests {

    // MARK: - Construction helpers

    private func makeStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "PersistedTerminalStateReconcileTests")
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private func makeCoordinator(store: AnalysisStore) -> AnalysisCoordinator {
        let speechService = SpeechService(
            vocabularyProvider: ASRVocabularyProvider(store: store)
        )
        return AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: speechService,
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
    }

    private func makeTerminalAsset(
        id: String,
        analysisState: SessionState,
        terminalReason: String?,
        episodeDurationSec: Double?,
        featureCoverageEndTime: Double?,
        fastTranscriptCoverageEndTime: Double?
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(id).m4a",
            featureCoverageEndTime: featureCoverageEndTime,
            fastTranscriptCoverageEndTime: fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: nil,
            analysisState: analysisState.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDurationSec,
            terminalReason: terminalReason
        )
    }

    /// Insert an asset and (when non-nil) persist its `terminalReason` via
    /// `updateAssetState`, since `insertAsset` does not bind that column.
    /// Mirrors how production rows reach the disk: `insertAsset` writes the
    /// row, then later transitions via `updateAssetState(id:state:terminalReason:)`
    /// stamp the terminal reason at finalize time.
    private func seedAsset(
        store: AnalysisStore,
        _ asset: AnalysisAsset
    ) async throws {
        try await store.insertAsset(asset)
        if asset.terminalReason != nil {
            try await store.updateAssetState(
                id: asset.id,
                state: asset.analysisState,
                terminalReason: asset.terminalReason
            )
        }
    }

    private func makeChunk(
        assetId: String,
        chunkIndex: Int,
        startTime: Double,
        endTime: Double,
        pass: TranscriptPassType = .fast
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "\(assetId)-chunk-\(pass.rawValue)-\(chunkIndex)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(assetId)-\(chunkIndex)",
            chunkIndex: chunkIndex,
            startTime: startTime,
            endTime: endTime,
            text: "x",
            normalizedText: "x",
            pass: pass.rawValue,
            modelVersion: "speech-v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            weakAnchorMetadata: nil
        )
    }

    // MARK: - Pure verdict — bad rows

    @Test("3B96D187 shape: completeFull with low transcript coverage routes to non-completeFull")
    func badRow_completeFull_lowTranscript_repairs() {
        // 36.4 min episode = 2184s; transcript watermark = 1.5 min = 90s.
        // Feature coverage ~equal to duration (asset claims completeFull).
        let asset = makeTerminalAsset(
            id: "3B96D187",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 1.000, feature 0.999",
            episodeDurationSec: 2184,
            featureCoverageEndTime: 2180,
            fastTranscriptCoverageEndTime: 90
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 90, // pre-final-pass: only fast chunks exist
            featureCoverageEnd: 2180,
            episodeDuration: 2184
        )
        guard case .repair(let state, let reason) = verdict else {
            #expect(Bool(false), "expected .repair; got \(verdict)")
            return
        }
        // Feature is fine; transcript is short — classifier maps to .completeTranscriptPartial.
        #expect(state == .completeTranscriptPartial)
        #expect(reason.hasPrefix(AnalysisCoordinator.terminalStateRepairedReasonPrefix))
        #expect(reason.contains("full coverage: transcript 1.000, feature 0.999"))
    }

    @Test("8A9DFC82 shape: completeFull with stale-denominator ratios > 1.0 reclassifies via canonical duration")
    func badRow_completeFull_impossibleRatio_repairs() {
        // 94.1min = 5646s; feature coverage 21.5min = 1290s; transcript = 90s.
        // terminalReason carries the old bug ratio.
        let asset = makeTerminalAsset(
            id: "8A9DFC82",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 1.163, feature 1.724",
            episodeDurationSec: 5646,
            featureCoverageEndTime: 1290,
            fastTranscriptCoverageEndTime: 90
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 90,
            featureCoverageEnd: 1290,
            episodeDuration: 5646
        )
        guard case .repair(let state, let reason) = verdict else {
            #expect(Bool(false), "expected .repair; got \(verdict)")
            return
        }
        // Feature 1290/5646 = 0.229 < 0.95 → .failedFeature.
        #expect(state == .failedFeature)
        // Audit-prefix invariant: support diagnosis depends on the
        // original (impossible-ratio) reason being preserved verbatim
        // in the rewritten string. R2: was previously asserted only on
        // the .completeTranscriptPartial shape; pin it here too so a
        // regression that drops the audit on the .failedFeature branch
        // gets caught.
        #expect(reason.hasPrefix(AnalysisCoordinator.terminalStateRepairedReasonPrefix))
        #expect(reason.contains("full coverage: transcript 1.163, feature 1.724"))
    }

    @Test("9C109975 shape: chunks prove more coverage than asset watermark — still must not be completeFull")
    func badRow_completeFull_chunksProveCoverageButShortOfDuration_repairs() {
        // 66.0min = 3960s. Asset watermark 1.5min, but chunks extend
        // further (say 30min = 1800s). Still well short of 95% of 3960.
        let asset = makeTerminalAsset(
            id: "9C109975",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 1.000, feature 0.999",
            episodeDurationSec: 3960,
            featureCoverageEndTime: 3960,
            fastTranscriptCoverageEndTime: 90
        )
        // Chunks reach 1800s — far past the watermark, but still short.
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 1800,
            featureCoverageEnd: 3960,
            episodeDuration: 3960
        )
        guard case .repair(let state, _) = verdict else {
            #expect(Bool(false), "expected .repair; got \(verdict)")
            return
        }
        #expect(state == .completeTranscriptPartial)
    }

    @Test("E8F0F867 shape: terminalReason ratios > 1.0 reclassifies even if coverage values look ok in isolation")
    func badRow_terminalReasonImpossibleRatio_repairs() {
        // The denominator was wrong at write-time so we cannot trust the
        // asset's own coverage numbers either. Even if feature looks
        // adequate against the *current* duration, an impossible ratio
        // in the persisted reason proves the row was scored against a
        // garbage denominator.
        let asset = makeTerminalAsset(
            id: "E8F0F867",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 6.891, feature 8.106",
            episodeDurationSec: 1800,
            featureCoverageEndTime: 1800,
            fastTranscriptCoverageEndTime: 1800
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 1800,
            featureCoverageEnd: 1800,
            episodeDuration: 1800
        )
        // Coverage values themselves are full — but the impossible ratio
        // in terminalReason forces a reclassify.
        guard case .repair(let state, let reason) = verdict else {
            #expect(Bool(false), "expected .repair (impossible ratio); got \(verdict)")
            return
        }
        // Both coverage values match the canonical duration, so the
        // classifier returns .completeFull — and the audited reason will
        // make clear this row was reclassified, with the original
        // impossible-ratio text preserved for support.
        #expect(state == .completeFull)
        // R2: audit-trail invariant — the only signal that distinguishes
        // a reconciled row from an organically-completeFull row is the
        // prefix + preserved original reason. Without these assertions a
        // regression that drops the prefix on the same-state branch
        // would silently strip the audit history and the test would
        // still pass.
        #expect(reason.hasPrefix(AnalysisCoordinator.terminalStateRepairedReasonPrefix))
        #expect(reason.contains("full coverage: transcript 6.891, feature 8.106"))
        // R3: pin the EXACT canonical wrapper shape so a wrong impl
        // that pads `[autoRepaired:` with whitespace, doubles the
        // closing bracket, or omits the space before the new reason
        // still fails. `hasPrefix` + `contains` alone are satisfied by
        // any string that contains the prefix and original substring,
        // even with arbitrary decoration in between.
        #expect(
            reason.hasPrefix("[autoRepaired:full coverage: transcript 6.891, feature 8.106] "),
            "expected canonical `[autoRepaired:<original>] ` shape; got \(reason)"
        )
    }

    // MARK: - Pure verdict — healthy rows

    @Test("Healthy completeFull row stays completeFull")
    func healthyRow_completeFull_unchanged() {
        let asset = makeTerminalAsset(
            id: "healthy",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 0.998, feature 0.999",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 3580,
            fastTranscriptCoverageEndTime: 3590
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 3590,
            featureCoverageEnd: 3580,
            episodeDuration: 3600
        )
        #expect(verdict == .unchanged)
    }

    @Test("Healthy completeFeatureOnly row (transcript == 0) stays")
    func healthyRow_completeFeatureOnly_unchanged() {
        let asset = makeTerminalAsset(
            id: "fo",
            analysisState: .completeFeatureOnly,
            terminalReason: "feature-only (feature 0.999, transcript 0)",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 3580,
            fastTranscriptCoverageEndTime: 0
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 0,
            featureCoverageEnd: 3580,
            episodeDuration: 3600
        )
        #expect(verdict == .unchanged)
    }

    @Test("Healthy completeTranscriptPartial row (feature ok, transcript short) stays")
    func healthyRow_completeTranscriptPartial_unchanged() {
        let asset = makeTerminalAsset(
            id: "tp",
            analysisState: .completeTranscriptPartial,
            terminalReason: "partial transcript 1800.0/3600.0s (ratio 0.500 < 0.950)",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 3580,
            fastTranscriptCoverageEndTime: 1800
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 1800,
            featureCoverageEnd: 3580,
            episodeDuration: 3600
        )
        #expect(verdict == .unchanged)
    }

    // MARK: - Pure verdict — bad rows on .completeFeatureOnly / .completeTranscriptPartial

    @Test("Bad completeFeatureOnly row (feature short of canonical denominator) repairs and preserves audit prefix")
    func badRow_completeFeatureOnly_featureShort_repairs() {
        // R5 coverage gap: prior tests only covered the healthy
        // `.completeFeatureOnly` case. A regression that no-ops the
        // `state == .completeFeatureOnly` branch would still pass every
        // other test. Pin the repair contract on this state explicitly.
        //
        // Shape: row claims feature-only completion against a 3600s
        // episode but feature coverage is only 1290s (ratio 0.358 <
        // 0.95). Classifier maps to .failedFeature on repair.
        let asset = makeTerminalAsset(
            id: "fo-bad",
            analysisState: .completeFeatureOnly,
            terminalReason: "feature-only (feature 0.999, transcript 0)",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 1290,
            fastTranscriptCoverageEndTime: 0
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 0,
            featureCoverageEnd: 1290,
            episodeDuration: 3600
        )
        guard case .repair(let state, let reason) = verdict else {
            #expect(Bool(false), "expected .repair; got \(verdict)")
            return
        }
        #expect(state == .failedFeature)
        #expect(reason.hasPrefix(AnalysisCoordinator.terminalStateRepairedReasonPrefix))
        #expect(reason.contains("feature-only (feature 0.999, transcript 0)"))
    }

    @Test("Bad completeFeatureOnly row with bug-shape ratio in reason repairs even if feature ratio looks fine")
    func badRow_completeFeatureOnly_impossibleRatioInReason_repairs() {
        // R5 coverage gap: pin the `reasonClaimsImpossibleRatio` branch
        // for .completeFeatureOnly. Without this test a regression that
        // only checked feature shortness on this state would silently
        // skip rows whose poisoned terminalReason is the only remaining
        // signal of denominator-bug history.
        let asset = makeTerminalAsset(
            id: "fo-impossible",
            analysisState: .completeFeatureOnly,
            terminalReason: "feature-only (feature 1.724, transcript 0)",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 3590,
            fastTranscriptCoverageEndTime: 0
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 0,
            featureCoverageEnd: 3590,
            episodeDuration: 3600
        )
        guard case .repair(_, let reason) = verdict else {
            #expect(Bool(false), "expected .repair (impossible ratio); got \(verdict)")
            return
        }
        // Feature ratio against canonical duration is 0.997 (>= 0.95), so
        // the classifier emits .completeFeatureOnly again — the repair's
        // value is the audit trail, not a state change. Pin the audit
        // shape so a regression that drops the prefix on same-state
        // repair is caught.
        #expect(reason.hasPrefix(AnalysisCoordinator.terminalStateRepairedReasonPrefix))
        #expect(reason.contains("feature-only (feature 1.724, transcript 0)"))
    }

    @Test("Bad completeTranscriptPartial row (feature short of canonical denominator) repairs and preserves audit prefix")
    func badRow_completeTranscriptPartial_featureShort_repairs() {
        // R5 coverage gap: prior tests only covered the healthy
        // `.completeTranscriptPartial` case. A regression that no-ops
        // the `state == .completeTranscriptPartial` branch would still
        // pass every other test. Pin the repair contract on this state.
        //
        // Shape: row claims transcript-partial completion (which
        // requires feature pipeline to have cleared the floor) but
        // feature coverage is only 600s on a 3600s episode (ratio 0.166
        // < 0.95). Classifier maps to .failedFeature on repair.
        let asset = makeTerminalAsset(
            id: "tp-bad",
            analysisState: .completeTranscriptPartial,
            terminalReason: "partial transcript 1800.0/3600.0s (ratio 0.500 < 0.950)",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 600,
            fastTranscriptCoverageEndTime: 1800
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 1800,
            featureCoverageEnd: 600,
            episodeDuration: 3600
        )
        guard case .repair(let state, let reason) = verdict else {
            #expect(Bool(false), "expected .repair; got \(verdict)")
            return
        }
        #expect(state == .failedFeature)
        #expect(reason.hasPrefix(AnalysisCoordinator.terminalStateRepairedReasonPrefix))
        #expect(reason.contains("partial transcript 1800.0/3600.0s (ratio 0.500 < 0.950)"))
    }

    // MARK: - Pure verdict — fail-safes

    @Test("Unknown episode duration returns .skipUnknownDuration (not destructive)")
    func unknownDuration_skipFailSafe() {
        let asset = makeTerminalAsset(
            id: "unknown-dur",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 1.000, feature 1.000",
            episodeDurationSec: nil,
            featureCoverageEndTime: 100,
            fastTranscriptCoverageEndTime: 100
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 100,
            featureCoverageEnd: 100,
            episodeDuration: nil
        )
        #expect(verdict == .skipUnknownDuration)
    }

    @Test("Zero episode duration returns .skipUnknownDuration")
    func zeroDuration_skipFailSafe() {
        let asset = makeTerminalAsset(
            id: "zero-dur",
            analysisState: .completeFull,
            terminalReason: nil,
            episodeDurationSec: 0,
            featureCoverageEndTime: 0,
            fastTranscriptCoverageEndTime: 0
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 0,
            featureCoverageEnd: 0,
            episodeDuration: 0
        )
        #expect(verdict == .skipUnknownDuration)
    }

    // MARK: - Pure verdict — non-completion states are not touched

    @Test("failedTranscript is left alone")
    func failedTranscript_unchanged() {
        let asset = makeTerminalAsset(
            id: "ft",
            analysisState: .failedTranscript,
            terminalReason: "transcript pipeline error",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 3600,
            fastTranscriptCoverageEndTime: 0
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 0,
            featureCoverageEnd: 3600,
            episodeDuration: 3600
        )
        #expect(verdict == .unchanged)
    }

    @Test("Legacy .complete (pre-gtt9.8) is left alone")
    func legacyComplete_unchanged() {
        let asset = makeTerminalAsset(
            id: "legacy",
            analysisState: .complete,
            terminalReason: nil,
            episodeDurationSec: 3600,
            featureCoverageEndTime: 100,
            fastTranscriptCoverageEndTime: 100
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 100,
            featureCoverageEnd: 100,
            episodeDuration: 3600
        )
        #expect(verdict == .unchanged)
    }

    @Test("Non-terminal states (queued, backfill, etc.) are left alone")
    func nonTerminalStates_unchanged() {
        for state in [SessionState.queued, .spooling, .featuresReady,
                      .hotPathReady, .waitingForBackfill, .backfill] {
            let asset = makeTerminalAsset(
                id: "x-\(state.rawValue)",
                analysisState: state,
                terminalReason: nil,
                episodeDurationSec: 3600,
                featureCoverageEndTime: 100,
                fastTranscriptCoverageEndTime: 100
            )
            let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
                asset: asset,
                transcriptCoverageEnd: 100,
                featureCoverageEnd: 100,
                episodeDuration: 3600
            )
            #expect(verdict == .unchanged, "state \(state.rawValue) must be unchanged")
        }
    }

    // MARK: - parseHighestRatioInTerminalReason

    @Test("parseHighestRatioInTerminalReason extracts both ratios and returns the max")
    func parseRatio_pickMax() {
        let r = AnalysisCoordinator.parseHighestRatioInTerminalReason(
            "full coverage: transcript 1.163, feature 1.724"
        )
        #expect(r != nil)
        #expect(abs((r ?? 0) - 1.724) < 1e-6)
    }

    @Test("parseHighestRatioInTerminalReason returns nil for nil/empty input")
    func parseRatio_nil() {
        #expect(AnalysisCoordinator.parseHighestRatioInTerminalReason(nil) == nil)
        #expect(AnalysisCoordinator.parseHighestRatioInTerminalReason("") == nil)
    }

    @Test("parseHighestRatioInTerminalReason ignores slash-bearing tokens but extracts the parenthesized ratio")
    func parseRatio_partialShape() {
        // partial-shape: "partial transcript 870.0/3600.0s (ratio 0.241 < 0.950)"
        // The next-token-after-`transcript` is "870.0/3600.0s" — slash
        // bearing, skipped. After R2's punctuation-trim fix, `(ratio` is
        // recognized as the `ratio` label and we pick up `0.241`. Both
        // contracts pinned: (a) 870 is NOT swept up by accident, (b)
        // the parenthesized ratio IS extracted so a future format that
        // emits `(ratio 1.5 < 0.950)` would trigger the impossible-ratio
        // gate.
        let r = AnalysisCoordinator.parseHighestRatioInTerminalReason(
            "partial transcript 870.0/3600.0s (ratio 0.241 < 0.950)"
        )
        #expect(r != nil, "parser must recognize `(ratio` after punctuation trim")
        #expect(abs((r ?? 0) - 0.241) < 1e-6)
        #expect((r ?? 0) <= AnalysisCoordinator.terminalStateRepairRatioCeiling)
    }

    @Test("parseHighestRatioInTerminalReason extracts feature ratio from feature-only paren shape")
    func parseRatio_featureOnlyParenShape() {
        // R2 fix: `feature-only (feature 0.998, transcript 0)` — before
        // the fix, the parser missed `(feature` because of the leading
        // paren and only picked up `transcript 0)` → 0.0. That left a
        // hole where a feature-only row with a bug-shape ratio of
        // `(feature 1.724, transcript 0)` would NOT trigger the
        // impossible-ratio gate. Pin the fix so the gap can't reopen.
        let healthy = AnalysisCoordinator.parseHighestRatioInTerminalReason(
            "feature-only (feature 0.998, transcript 0)"
        )
        #expect(healthy != nil)
        #expect(abs((healthy ?? 0) - 0.998) < 1e-6)

        let bug = AnalysisCoordinator.parseHighestRatioInTerminalReason(
            "feature-only (feature 1.724, transcript 0)"
        )
        #expect(bug != nil)
        #expect((bug ?? 0) > AnalysisCoordinator.terminalStateRepairRatioCeiling)
    }

    @Test("parseHighestRatioInTerminalReason ignores the tail of an already-repaired reason")
    func parseRatio_skipAuditPrefix() {
        // The audit prefix carries the original (bad) reason. The
        // post-prefix tail should be the only thing that informs the
        // ratio decision — otherwise an already-repaired row would
        // re-trigger repair on next launch.
        // R2: tighten the assertion. After the punctuation-trim fix,
        // `(ratio 0.229 < 0.950)` parses to 0.229 — the test now pins
        // the EXACT extracted value so a regression that returned `nil`
        // (parser stopped working) or 8.106 (prefix not stripped) would
        // both fail.
        let r = AnalysisCoordinator.parseHighestRatioInTerminalReason(
            "[autoRepaired:full coverage: transcript 6.891, feature 8.106] partial transcript 1290.0/5646.0s (ratio 0.229 < 0.950)"
        )
        #expect(r != nil, "post-prefix tail must yield a parseable ratio")
        #expect(abs((r ?? 0) - 0.229) < 1e-6,
                "expected 0.229 from `(ratio 0.229 < 0.950)`; got \(String(describing: r))")
        #expect((r ?? 0) <= AnalysisCoordinator.terminalStateRepairRatioCeiling)
    }

    @Test("parseHighestRatioInTerminalReason picks ratio > ceiling when it is in the live tail")
    func parseRatio_aboveCeiling() {
        // Construct a reason whose live tail (after any audit prefix)
        // carries a ratio > the ceiling. Defensive against future
        // classifier-emitted formats that surface real rather than
        // bug-shape ratios.
        let r = AnalysisCoordinator.parseHighestRatioInTerminalReason(
            "full coverage: transcript 1.163, feature 1.724"
        )
        #expect((r ?? 0) > AnalysisCoordinator.terminalStateRepairRatioCeiling)
    }

    // MARK: - Audit-prefix sanitization (R3: ] in original is neutralized)

    @Test("Original reason containing `]` is sanitized so audit-prefix boundary stays unambiguous")
    func auditPrefixSanitizesEmbeddedCloseBracket() {
        // R3 fix: the audit-prefix encoding uses the first `]` after
        // `[autoRepaired:` as the boundary. Today's
        // `classifyBackfillTerminal` never emits `]`, but
        // `transitionAndPersist` accepts arbitrary terminalReason
        // strings (any future code path could plant a `]`). Without
        // sanitization, the read-side parser truncates at the embedded
        // `]` — `parseHighestRatioInTerminalReason` would scan a
        // trailing tail that still contains the bug-shape ratio, and
        // `unwrappedAuditOriginal(from:)` would peel a partial original
        // on re-repair. The fix replaces `]` with `}` at write time so
        // the wrapper is unambiguous regardless of original content.
        let asset = makeTerminalAsset(
            id: "embedded-bracket",
            analysisState: .completeFull,
            // Synthetic original carrying `]` — proxy for any future
            // reason format that escapes the canonical 8 shapes.
            terminalReason: "garbage] full coverage: transcript 6.891, feature 8.106",
            episodeDurationSec: 1800,
            featureCoverageEndTime: 1800,
            fastTranscriptCoverageEndTime: 1800
        )
        let verdict = AnalysisCoordinator.reconcilePersistedTerminalAssetVerdict(
            asset: asset,
            transcriptCoverageEnd: 1800,
            featureCoverageEnd: 1800,
            episodeDuration: 1800
        )
        guard case .repair(_, let reason) = verdict else {
            #expect(Bool(false), "expected .repair (impossible ratio); got \(verdict)")
            return
        }
        // Exactly one `]` between `[autoRepaired:` and the new reason —
        // the embedded `]` from the original must be sanitized to `}`.
        let prefix = AnalysisCoordinator.terminalStateRepairedReasonPrefix
        guard let prefixRange = reason.range(of: prefix) else {
            #expect(Bool(false), "expected prefix in \(reason)")
            return
        }
        let afterPrefix = reason[prefixRange.upperBound...]
        guard let firstClose = afterPrefix.firstIndex(of: "]") else {
            #expect(Bool(false), "expected `]` after prefix in \(reason)")
            return
        }
        let wrappedOriginal = afterPrefix[..<firstClose]
        // The full original must be present (length-equivalently) inside
        // the wrapper — no truncation. The single `]` is replaced with
        // `}`, so the wrapped form has the same number of characters.
        #expect(
            wrappedOriginal == "garbage} full coverage: transcript 6.891, feature 8.106",
            "expected `]` -> `}` sanitization preserving full original length; got \(wrappedOriginal)"
        )
        // And the new reason follows the closing bracket immediately
        // (no second `]` from the original survived to confuse parsing).
        let tail = afterPrefix[afterPrefix.index(after: firstClose)...]
        #expect(!tail.contains("]"),
                "audit-wrapper tail must not contain `]`; got \(tail)")
    }

    // MARK: - Audit-prefix length bound (R2: prevent stacking)

    @Test("Re-repair flattens audit prefix; chain does not stack to unbounded length")
    func reRepairFlattensAuditPrefix() async throws {
        // Simulate the upgrade path where a row gets repaired under
        // `_v1`, then a future bead bumps the meta key to `_v2` and the
        // sweep re-evaluates the same row. Without the length-bound
        // helper the persisted reason would accumulate
        // `[autoRepaired:[autoRepaired:[autoRepaired:original]...]...]`
        // and grow with every revision. After R2's
        // `unwrappedAuditOriginal(from:)` the chain stays at one level.
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        // Seed an already-once-repaired row directly: state is
        // .failedFeature and terminalReason carries the audit prefix
        // wrapping the original bug-shape reason.
        let originalReason = "full coverage: transcript 1.163, feature 1.724"
        let firstRepairedReason =
            "\(AnalysisCoordinator.terminalStateRepairedReasonPrefix)\(originalReason)] feature coverage 1290.0/5646.0s (ratio 0.229 < 0.950)"
        let asset = makeTerminalAsset(
            id: "stacked-audit",
            analysisState: .completeFull, // pretend the bug came back
            terminalReason: firstRepairedReason,
            episodeDurationSec: 5646,
            featureCoverageEndTime: 1290,
            fastTranscriptCoverageEndTime: 90
        )
        try await seedAsset(store: store, asset)
        try await store.insertTranscriptChunks([
            makeChunk(assetId: asset.id, chunkIndex: 0, startTime: 0, endTime: 90)
        ])

        let summary = await coordinator.reconcilePersistedTerminalStatesIfNeeded()
        #expect(summary.repairedAssetIds == [asset.id])

        let after = try await store.fetchAsset(id: asset.id)
        let rewrittenReason = after?.terminalReason ?? ""

        // Exactly ONE `[autoRepaired:` prefix must remain in the
        // persisted form. The original reason is preserved verbatim.
        let prefix = AnalysisCoordinator.terminalStateRepairedReasonPrefix
        let occurrences = rewrittenReason.components(separatedBy: prefix).count - 1
        #expect(occurrences == 1, "expected exactly 1 audit prefix; got \(occurrences) in \(rewrittenReason)")
        #expect(rewrittenReason.contains(originalReason))

        // Length stays bounded by `prefix + originalReason + "] " +
        // newReason` rather than the previously-repaired wrapping.
        // Concretely: rewritten reason must be SHORTER than naïve
        // stacking would produce.
        let naiveStacked = "\(prefix)\(firstRepairedReason)] dummy"
        #expect(
            rewrittenReason.count < naiveStacked.count,
            "audit prefix must flatten; got \(rewrittenReason.count) chars vs naive \(naiveStacked.count)"
        )
    }

    // MARK: - Sweep integration

    @Test("Sweep repairs a bad completeFull row, sets idempotence marker, and a second call short-circuits")
    func sweepRepairsAndIsIdempotent() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        // Seed a bad 8A9DFC82-shape row.
        let badAsset = makeTerminalAsset(
            id: "bad-8A9DFC82",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 1.163, feature 1.724",
            episodeDurationSec: 5646,
            featureCoverageEndTime: 1290,
            fastTranscriptCoverageEndTime: 90
        )
        try await seedAsset(store: store, badAsset)
        try await store.insertTranscriptChunks([
            makeChunk(assetId: badAsset.id, chunkIndex: 0, startTime: 0, endTime: 90)
        ])

        // Seed a healthy completeFull row that must NOT be touched.
        let healthyAsset = makeTerminalAsset(
            id: "healthy-row",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 0.998, feature 0.999",
            episodeDurationSec: 3600,
            featureCoverageEndTime: 3580,
            fastTranscriptCoverageEndTime: 3590
        )
        try await seedAsset(store: store, healthyAsset)
        try await store.insertTranscriptChunks([
            makeChunk(assetId: healthyAsset.id, chunkIndex: 0, startTime: 0, endTime: 3590)
        ])

        // Run sweep — first invocation should repair the bad row.
        let summary1 = await coordinator.reconcilePersistedTerminalStatesIfNeeded()
        #expect(summary1.alreadyDone == false)
        #expect(summary1.repairedAssetIds == [badAsset.id])
        #expect(summary1.unchanged == 1)

        // Verify the bad row is now reclassified and carries an audit
        // prefix preserving the original (impossible-ratio) reason.
        let badAfter = try await store.fetchAsset(id: badAsset.id)
        #expect(badAfter?.analysisState == SessionState.failedFeature.rawValue)
        let repairedReason = badAfter?.terminalReason ?? ""
        #expect(repairedReason.hasPrefix(AnalysisCoordinator.terminalStateRepairedReasonPrefix))
        #expect(repairedReason.contains("full coverage: transcript 1.163, feature 1.724"))

        // Verify the healthy row is unchanged.
        let healthyAfter = try await store.fetchAsset(id: healthyAsset.id)
        #expect(healthyAfter?.analysisState == SessionState.completeFull.rawValue)
        #expect(
            healthyAfter?.terminalReason == "full coverage: transcript 0.998, feature 0.999"
        )

        // Second invocation must short-circuit via the meta flag.
        let summary2 = await coordinator.reconcilePersistedTerminalStatesIfNeeded()
        #expect(summary2.alreadyDone == true)
        #expect(summary2.repairedCount == 0)
    }

    @Test("Sweep skips rows with unknown episode duration (fail-safe; duration backfill heals on next launch)")
    func sweepSkipsUnknownDuration() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let asset = makeTerminalAsset(
            id: "no-dur",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 1.000, feature 1.000",
            episodeDurationSec: nil,
            featureCoverageEndTime: 100,
            fastTranscriptCoverageEndTime: 100
        )
        try await seedAsset(store: store, asset)

        let summary = await coordinator.reconcilePersistedTerminalStatesIfNeeded()
        #expect(summary.skippedUnknownDuration == 1)
        #expect(summary.repairedCount == 0)

        // State must be unchanged.
        let after = try await store.fetchAsset(id: asset.id)
        #expect(after?.analysisState == SessionState.completeFull.rawValue)
    }

    @Test("Sweep does not touch failure or non-terminal states")
    func sweepDoesNotTouchOtherStates() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        // Seed several rows in a mix of non-completion-terminal states.
        // Each carries impossible coverage (which the sweep WOULD repair
        // if it scanned this state) — proving the state filter is the
        // gate, not the coverage check.
        let states: [SessionState] = [
            .failedTranscript, .failedFeature, .cancelledBudget,
            .failed, .complete, .queued, .backfill
        ]
        for (i, state) in states.enumerated() {
            let asset = makeTerminalAsset(
                id: "skip-\(i)",
                analysisState: state,
                terminalReason: "synthetic",
                episodeDurationSec: 3600,
                featureCoverageEndTime: 100,
                fastTranscriptCoverageEndTime: 100
            )
            try await seedAsset(store: store, asset)
        }

        let summary = await coordinator.reconcilePersistedTerminalStatesIfNeeded()
        #expect(summary.repairedCount == 0)

        // Every row's state must be unchanged.
        for (i, state) in states.enumerated() {
            let after = try await store.fetchAsset(id: "skip-\(i)")
            #expect(
                after?.analysisState == state.rawValue,
                "row \(i) (state \(state.rawValue)) must not be touched"
            )
        }
    }

    @Test("Repaired row is reflected in EpisodeSurfaceStatusObserver.analysisState (no longer .done)")
    func repairChangesActivitySurfaceMapping() async throws {
        let store = try await makeStore()
        let coordinator = makeCoordinator(store: store)

        let asset = makeTerminalAsset(
            id: "surface-bad",
            analysisState: .completeFull,
            terminalReason: "full coverage: transcript 1.163, feature 1.724",
            episodeDurationSec: 5646,
            featureCoverageEndTime: 1290,
            fastTranscriptCoverageEndTime: 90
        )
        try await seedAsset(store: store, asset)
        try await store.insertTranscriptChunks([
            makeChunk(assetId: asset.id, chunkIndex: 0, startTime: 0, endTime: 90)
        ])

        // Pre-sweep: the persisted row maps to .done because
        // analysisState == completeFull.
        let preState = try await store.fetchAsset(id: asset.id)
        #expect(preState != nil)
        let preMapped = EpisodeSurfaceStatusObserver.analysisState(from: preState!)
        #expect(preMapped.persistedStatus == .done)

        // Sweep.
        _ = await coordinator.reconcilePersistedTerminalStatesIfNeeded()

        // Post-sweep: row is .failedFeature → maps to .failed (not .done).
        let postState = try await store.fetchAsset(id: asset.id)
        #expect(postState?.analysisState == SessionState.failedFeature.rawValue)
        let postMapped = EpisodeSurfaceStatusObserver.analysisState(from: postState!)
        #expect(postMapped.persistedStatus != .done)
        #expect(postMapped.persistedStatus == .failed)
    }
}
