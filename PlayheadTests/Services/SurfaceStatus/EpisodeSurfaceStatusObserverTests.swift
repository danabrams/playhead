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
        lazy var sink: SurfaceStatusReadyTransitionEmitter.LoggerSink = { [weak self] hash, trigger in
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

    private static func makeSnapshot(canUseFM: Bool = true) -> CapabilitySnapshot {
        CapabilitySnapshot(
            foundationModelsAvailable: canUseFM,
            foundationModelsUsable: canUseFM,
            appleIntelligenceEnabled: canUseFM,
            foundationModelsLocaleSupported: canUseFM,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 10_000_000_000,
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    private static func makeObserver(
        store: AnalysisStore,
        snapshot: CapabilitySnapshot? = makeSnapshot(),
        sink: @escaping SurfaceStatusReadyTransitionEmitter.LoggerSink
    ) -> EpisodeSurfaceStatusObserver {
        let reducer: SurfaceStatusReadyTransitionEmitter.Reducer = {
            state, cause, eligibility, coverage, anchor in
            episodeSurfaceStatus(
                state: state,
                cause: cause,
                eligibility: eligibility,
                coverage: coverage,
                readinessAnchor: anchor
            )
        }
        let emitter = SurfaceStatusReadyTransitionEmitter(
            reducer: reducer,
            loggerSink: sink
        )
        return EpisodeSurfaceStatusObserver(
            store: store,
            capabilitySnapshotProvider: { snapshot },
            episodeIdHasher: { "hash-\($0)" },
            emitter: emitter
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

    @Test("An ineligible device does NOT fire ready_entered")
    func ineligibleDeviceDoesNotFireReadyEntered() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            Self.makeTestAsset(episodeId: "ep-inel", state: SessionState.complete.rawValue)
        )
        let recorder = SinkRecorder()
        // A snapshot with FM unusable maps to `modelAvailableNow=false`
        // which flips eligibility to ineligible → Rule 1 short-circuits
        // to `.unavailable` (not ready).
        let observer = Self.makeObserver(
            store: store,
            snapshot: Self.makeSnapshot(canUseFM: false),
            sink: recorder.sink
        )

        await observer.observeEpisodePlayStarted(episodeId: "ep-inel")

        #expect(recorder.invocations.isEmpty)
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
        let usable = Self.makeSnapshot(canUseFM: true)
        let eligible = EpisodeSurfaceStatusObserver.eligibility(from: usable)
        #expect(eligible.isFullyEligible == true)

        let notUsable = Self.makeSnapshot(canUseFM: false)
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
