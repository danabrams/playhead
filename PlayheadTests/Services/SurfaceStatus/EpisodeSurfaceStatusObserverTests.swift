// EpisodeSurfaceStatusObserverTests.swift
// Verifies the production consumer wires real `AnalysisStore` rows
// through the reducer + transition emitter and fires `ready_entered`
// exactly once per transition INTO a ready-for-playback disposition.
//
// Scope: playhead-o45p — scope-gap fix (emitter exists but had zero
// production call sites before this consumer landed).

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeSurfaceStatusObserver — production consumer wiring (playhead-o45p)", .serialized)
struct EpisodeSurfaceStatusObserverTests {

    // MARK: - Helpers

    /// Captures emitter sink invocations so tests can assert on the
    /// fired trigger without going through the global JSONL file.
    private final class SinkRecorder: @unchecked Sendable {
        struct Invocation: Equatable {
            let episodeIdHash: String?
            let trigger: SurfaceStateTransitionEntryTrigger?
        }
        private let lock = NSLock()
        private var _invocations: [Invocation] = []
        var invocations: [Invocation] {
            lock.lock(); defer { lock.unlock() }
            return _invocations
        }
        lazy var sink: @Sendable (String?, SurfaceStateTransitionEntryTrigger?) -> Void = { [weak self] hash, trigger in
            guard let self else { return }
            self.lock.lock(); defer { self.lock.unlock() }
            self._invocations.append(Invocation(episodeIdHash: hash, trigger: trigger))
        }
    }

    private static func makeTestAsset(
        episodeId: String,
        state: String,
        confirmedCoverage: Double? = nil
    ) -> AnalysisAsset {
        AnalysisAsset(
            id: "asset-\(episodeId)",
            episodeId: episodeId,
            assetFingerprint: "fp-\(episodeId)",
            weakFingerprint: nil,
            sourceURL: "https://example.invalid/\(episodeId).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: confirmedCoverage,
            analysisState: state,
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// Build a fully-eligible `AnalysisEligibility` verdict for tests
    /// that exercise the happy path. playhead-4nt1: the observer no
    /// longer takes a capability snapshot — it consumes a structured
    /// `AnalysisEligibility` directly.
    private static func makeEligibility(
        hardwareSupported: Bool = true,
        appleIntelligenceEnabled: Bool = true,
        regionSupported: Bool = true,
        languageSupported: Bool = true,
        modelAvailableNow: Bool = true
    ) -> AnalysisEligibility {
        AnalysisEligibility(
            hardwareSupported: hardwareSupported,
            appleIntelligenceEnabled: appleIntelligenceEnabled,
            regionSupported: regionSupported,
            languageSupported: languageSupported,
            modelAvailableNow: modelAvailableNow,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    private static func makeObserver(
        store: AnalysisStore,
        eligibility: AnalysisEligibility = makeEligibility(),
        sink: @escaping @Sendable (String?, SurfaceStateTransitionEntryTrigger?) -> Void
    ) -> EpisodeSurfaceStatusObserver {
        // playhead-jzdc: tests use the sink-closure seam instead of
        // injecting an emitter object. The emitter is owned internally
        // by the observer; the sink lets tests observe what the emitter
        // would emit without ever holding a reference to it.
        // playhead-4nt1: the snapshot provider became an eligibility
        // provider — tests inject the structured verdict directly.
        return EpisodeSurfaceStatusObserver(
            store: store,
            eligibilityProvider: { eligibility },
            episodeIdHasher: { "hash-\($0)" },
            emitterSink: sink
        )
    }

    // MARK: - Primary wiring tests

    @Test("Cold start on an already-complete episode fires ready_entered with coldStart trigger")
    func coldStartOnCompleteEpisodeFiresReadyEnteredWithColdStartTrigger() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            Self.makeTestAsset(episodeId: "ep-cold", state: SessionState.complete.rawValue)
        )
        let recorder = SinkRecorder()
        let observer = Self.makeObserver(store: store, sink: recorder.sink)

        await observer.observeEpisodePlayStarted(episodeId: "ep-cold")

        #expect(recorder.invocations.count == 1)
        let first = try #require(recorder.invocations.first)
        #expect(first.episodeIdHash == "hash-ep-cold")
        // Emitter infers `.coldStart` because this episode has never been
        // seen before in this process.
        #expect(first.trigger == .coldStart)
    }

    @Test("Analysis completion transition fires ready_entered with analysisCompleted trigger")
    func analysisCompletionFiresReadyEnteredWithAnalysisCompletedTrigger() async throws {
        let store = try await makeTestStore()
        // Seed a queued asset — the "transition to complete" path writes
        // `.complete` before calling the observer.
        try await store.insertAsset(
            Self.makeTestAsset(episodeId: "ep-finish", state: SessionState.queued.rawValue)
        )
        let recorder = SinkRecorder()
        let observer = Self.makeObserver(store: store, sink: recorder.sink)

        // Simulate the coordinator's transition: write `.complete`, then
        // call the observer exactly as `AnalysisCoordinator.transition`
        // does after a successful state write.
        try await store.updateAssetState(
            id: "asset-ep-finish",
            state: SessionState.complete.rawValue
        )
        await observer.observeAnalysisSessionComplete(episodeId: "ep-finish")

        #expect(recorder.invocations.count == 1)
        let first = try #require(recorder.invocations.first)
        #expect(first.episodeIdHash == "hash-ep-finish")
        #expect(first.trigger == .analysisCompleted)
    }

    @Test("Repeated play-started calls on the same episode fire only once")
    func repeatedPlayStartedIsIdempotent() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            Self.makeTestAsset(episodeId: "ep-again", state: SessionState.complete.rawValue)
        )
        let recorder = SinkRecorder()
        let observer = Self.makeObserver(store: store, sink: recorder.sink)

        await observer.observeEpisodePlayStarted(episodeId: "ep-again")
        await observer.observeEpisodePlayStarted(episodeId: "ep-again")
        await observer.observeEpisodePlayStarted(episodeId: "ep-again")

        // The emitter's per-episode memory guards repeated ready
        // reductions: only the first transition INTO ready fires.
        #expect(recorder.invocations.count == 1)
    }

    @Test("An ineligible device (modelAvailableNow=false) does NOT fire ready_entered")
    func ineligibleModelDoesNotFireReadyEntered() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            Self.makeTestAsset(episodeId: "ep-inel", state: SessionState.complete.rawValue)
        )
        let recorder = SinkRecorder()
        // `modelAvailableNow=false` flips eligibility to ineligible →
        // Rule 1 short-circuits to `.unavailable` (not ready).
        let observer = Self.makeObserver(
            store: store,
            eligibility: Self.makeEligibility(modelAvailableNow: false),
            sink: recorder.sink
        )

        await observer.observeEpisodePlayStarted(episodeId: "ep-inel")

        #expect(recorder.invocations.isEmpty)
    }

    // MARK: - playhead-4nt1: per-axis ineligibility suppression

    @Test("hardwareSupported=false suppresses ready_entered (playhead-4nt1)")
    func hardwareUnsupportedSuppressesReadyEntered() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            Self.makeTestAsset(episodeId: "ep-no-hw", state: SessionState.complete.rawValue)
        )
        let recorder = SinkRecorder()
        // The previous observer hardcoded `hardwareSupported = true`,
        // which would have let this fire incorrectly on ineligible
        // SoCs. Now the verdict comes from the evaluator and Rule 1
        // suppresses.
        let observer = Self.makeObserver(
            store: store,
            eligibility: Self.makeEligibility(hardwareSupported: false),
            sink: recorder.sink
        )

        await observer.observeEpisodePlayStarted(episodeId: "ep-no-hw")

        #expect(recorder.invocations.isEmpty)
    }

    @Test("regionSupported=false suppresses ready_entered (playhead-4nt1)")
    func regionUnsupportedSuppressesReadyEntered() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            Self.makeTestAsset(episodeId: "ep-no-region", state: SessionState.complete.rawValue)
        )
        let recorder = SinkRecorder()
        // Same as above for region — pre-4nt1 the observer hardcoded
        // `regionSupported = true`, mis-attributing ready transitions
        // for non-US dogfooders.
        let observer = Self.makeObserver(
            store: store,
            eligibility: Self.makeEligibility(regionSupported: false),
            sink: recorder.sink
        )

        await observer.observeEpisodePlayStarted(episodeId: "ep-no-region")

        #expect(recorder.invocations.isEmpty)
    }

    @Test("hardware+region both true: ready_entered fires (playhead-4nt1 sanity check)")
    func eligibleHardwareAndRegionFiresReadyEntered() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            Self.makeTestAsset(episodeId: "ep-eligible", state: SessionState.complete.rawValue)
        )
        let recorder = SinkRecorder()
        // Sanity check: the eligibility-evaluator wiring still permits
        // emission when every axis is true.
        let observer = Self.makeObserver(
            store: store,
            eligibility: Self.makeEligibility(),
            sink: recorder.sink
        )

        await observer.observeEpisodePlayStarted(episodeId: "ep-eligible")

        #expect(recorder.invocations.count == 1)
    }

    @Test("Observer for an episode with no persisted asset does NOT fire")
    func noAssetDoesNotFireReadyEntered() async throws {
        let store = try await makeTestStore()
        // Deliberately do NOT insert an asset row.
        let recorder = SinkRecorder()
        let observer = Self.makeObserver(store: store, sink: recorder.sink)

        await observer.observeEpisodePlayStarted(episodeId: "ep-missing")

        #expect(recorder.invocations.isEmpty)
    }

    // MARK: - Mapping-helper tests

    @Test("analysisState maps every SessionState to the expected PersistedStatus")
    func analysisStateMappingCoversEverySessionState() {
        let cases: [(SessionState, AnalysisState.PersistedStatus)] = [
            (.queued, .queued),
            (.spooling, .running),
            (.featuresReady, .running),
            (.hotPathReady, .running),
            (.backfill, .running),
            (.complete, .done),
            (.failed, .failed),
        ]
        for (sessionState, expected) in cases {
            let asset = Self.makeTestAsset(episodeId: "ep", state: sessionState.rawValue)
            let mapped = EpisodeSurfaceStatusObserver.analysisState(from: asset)
            #expect(
                mapped.persistedStatus == expected,
                "Expected SessionState.\(sessionState.rawValue) to map to .\(expected.rawValue), got .\(mapped.persistedStatus.rawValue)"
            )
        }
    }

    @Test("analysisState maps an unknown persisted string to .new")
    func analysisStateMappingFallsBackToNewOnUnknownString() {
        let asset = Self.makeTestAsset(episodeId: "ep", state: "some_future_state")
        let mapped = EpisodeSurfaceStatusObserver.analysisState(from: asset)
        #expect(mapped.persistedStatus == .new)
    }

    @Test("eligibility(from:) copies FM-related fields from the snapshot")
    func eligibilityCopiesFMFieldsFromSnapshot() {
        // playhead-4nt1: this static helper is no longer on the
        // observer's runtime path — its tests still pin its behavior
        // for the remaining non-observer callers
        // (`LiveActivitySnapshotProvider`).
        let usable = CapabilitySnapshot(
            foundationModelsAvailable: true,
            foundationModelsUsable: true,
            appleIntelligenceEnabled: true,
            foundationModelsLocaleSupported: true,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10_000_000_000,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let eligible = EpisodeSurfaceStatusObserver.eligibility(from: usable)
        #expect(eligible.isFullyEligible == true)

        let notUsable = CapabilitySnapshot(
            foundationModelsAvailable: false,
            foundationModelsUsable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10_000_000_000,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let ineligible = EpisodeSurfaceStatusObserver.eligibility(from: notUsable)
        #expect(ineligible.isFullyEligible == false)
        #expect(ineligible.modelAvailableNow == false)
    }

    @Test("eligibility(from: nil) is fully eligible so missing snapshot does not suppress emission")
    func eligibilityWithNilSnapshotIsFullyEligible() {
        let eligibility = EpisodeSurfaceStatusObserver.eligibility(from: nil)
        #expect(eligibility.isFullyEligible == true)
    }
}
