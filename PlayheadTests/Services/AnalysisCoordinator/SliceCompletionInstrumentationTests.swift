// SliceCompletionInstrumentationTests.swift
// playhead-1nl6: unit tests for SliceMetadata JSON shape, SliceCounters
// per-device aggregation, CauseAttributionPolicy.primaryCause precedence,
// and the CauseEmissionRegistry enum-exhaustiveness guarantee.
//
// These tests deliberately exercise the *library* level of the feature:
//   - SliceMetadata encode/decode round-trip + required keys
//   - SliceCounters counts per-device, per-cause, and resets
//   - primaryCause wrapper honors resolve()'s precedence output
//   - CauseEmissionRegistry declares an emitter tag for every
//     Phase-1 emitting variant
//
// They do NOT spin up an AnalysisStore or write real WorkJournal rows —
// that integration is covered by the existing AnalysisCoordinator /
// AnalysisStore tests. This file is a guardrail against regressing the
// three library contracts (metadata shape, precedence, exhaustiveness).

import Foundation
import Testing

@testable import Playhead

@Suite("SliceCompletionInstrumentation")
struct SliceCompletionInstrumentationTests {

    // MARK: - SliceMetadata

    @Test("SliceMetadata JSON has the four required top-level keys")
    func metadataJSONHasRequiredKeys() throws {
        let meta = SliceMetadata(
            sliceDurationMs: 1_500,
            bytesProcessed: 42_000,
            shardsCompleted: 2,
            deviceClass: DeviceClass.iPhone17Pro.rawValue
        )
        let json = meta.encodeJSON()
        let data = Data(json.utf8)
        let obj = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        let keys: Set<String> = Set(obj?.keys.map { $0 } ?? [])
        #expect(keys.contains("slice_duration_ms"))
        #expect(keys.contains("bytes_processed"))
        #expect(keys.contains("shards_completed"))
        #expect(keys.contains("device_class"))
    }

    @Test("SliceMetadata round-trips through JSONDecoder")
    func metadataRoundTripsThroughJSONDecoder() throws {
        let original = SliceMetadata(
            sliceDurationMs: 12_345,
            bytesProcessed: 987_654,
            shardsCompleted: 9,
            deviceClass: DeviceClass.iPhone16Pro.rawValue,
            extras: ["stage": "transcription", "retry": "2"]
        )
        let json = original.encodeJSON()
        let decoded = try JSONDecoder().decode(
            SliceMetadata.self,
            from: Data(json.utf8)
        )
        #expect(decoded.sliceDurationMs == original.sliceDurationMs)
        #expect(decoded.bytesProcessed == original.bytesProcessed)
        #expect(decoded.shardsCompleted == original.shardsCompleted)
        #expect(decoded.deviceClass == original.deviceClass)
        #expect(decoded.extras == original.extras)
    }

    @Test("SliceMetadata extras render as flat sibling keys, not nested")
    func metadataExtrasAreFlatSiblings() throws {
        let meta = SliceMetadata(
            sliceDurationMs: 0,
            bytesProcessed: 0,
            shardsCompleted: 0,
            deviceClass: DeviceClass.iPhoneSE3.rawValue,
            extras: ["stage": "featureExtraction"]
        )
        let json = meta.encodeJSON()
        let obj = try JSONSerialization.jsonObject(
            with: Data(json.utf8)
        ) as? [String: Any]
        // `stage` should appear at the top level, not under an "extras" key.
        #expect(obj?["stage"] as? String == "featureExtraction")
        #expect(obj?["extras"] == nil)
    }

    @Test("SliceMetadata refuses to let extras overwrite reserved keys")
    func metadataExtrasCannotOverrideReservedKeys() throws {
        let meta = SliceMetadata(
            sliceDurationMs: 1_000,
            bytesProcessed: 2_000,
            shardsCompleted: 3,
            deviceClass: DeviceClass.iPhone17.rawValue,
            extras: [
                "slice_duration_ms": "HIJACK",
                "bytes_processed": "HIJACK",
                "shards_completed": "HIJACK",
                "device_class": "HIJACK",
            ]
        )
        let json = meta.encodeJSON()
        let obj = try JSONSerialization.jsonObject(
            with: Data(json.utf8)
        ) as? [String: Any]
        // Reserved keys must hold the typed values, not the hijack strings.
        #expect(obj?["slice_duration_ms"] as? Int == 1_000)
        #expect(obj?["bytes_processed"] as? Int == 2_000)
        #expect(obj?["shards_completed"] as? Int == 3)
        #expect(obj?["device_class"] as? String == DeviceClass.iPhone17.rawValue)
    }

    // MARK: - SliceCounters

    @Test("SliceCounters aggregate started/completed per device class")
    func countersAggregateStartedAndCompleted() async {
        let counters = SliceCounters()
        await counters.incrementStarted(deviceClass: .iPhone17Pro)
        await counters.incrementStarted(deviceClass: .iPhone17Pro)
        await counters.incrementStarted(deviceClass: .iPhone16Pro)
        await counters.incrementCompleted(deviceClass: .iPhone17Pro)

        let pro = await counters.snapshot(deviceClass: .iPhone17Pro)
        let sixteen = await counters.snapshot(deviceClass: .iPhone16Pro)
        let se = await counters.snapshot(deviceClass: .iPhoneSE3)

        #expect(pro.slicesStarted == 2)
        #expect(pro.slicesCompleted == 1)
        #expect(sixteen.slicesStarted == 1)
        #expect(sixteen.slicesCompleted == 0)
        #expect(se.slicesStarted == 0)
        #expect(se.slicesCompleted == 0)
    }

    @Test("SliceCounters aggregate paused/failed keyed by cause")
    func countersAggregatePausedAndFailedByCause() async {
        let counters = SliceCounters()
        await counters.incrementPaused(deviceClass: .iPhone17Pro, cause: .thermal)
        await counters.incrementPaused(deviceClass: .iPhone17Pro, cause: .thermal)
        await counters.incrementPaused(deviceClass: .iPhone17Pro, cause: .userPreempted)
        await counters.incrementFailed(deviceClass: .iPhone17Pro, cause: .pipelineError)

        let snap = await counters.snapshot(deviceClass: .iPhone17Pro)
        #expect(snap.slicesPaused[.thermal] == 2)
        #expect(snap.slicesPaused[.userPreempted] == 1)
        #expect(snap.slicesFailed[.pipelineError] == 1)
        // An unused cause bucket stays absent, not zero.
        #expect(snap.slicesPaused[.noNetwork] == nil)
        #expect(snap.slicesFailed[.asrFailed] == nil)
    }

    @Test("SliceCounters reset clears every device-class bucket")
    func countersResetClearsAllDeviceClasses() async {
        let counters = SliceCounters()
        await counters.incrementStarted(deviceClass: .iPhone17Pro)
        await counters.incrementFailed(deviceClass: .iPhone16, cause: .pipelineError)
        await counters.reset()

        let pro = await counters.snapshot(deviceClass: .iPhone17Pro)
        let sixteen = await counters.snapshot(deviceClass: .iPhone16)
        #expect(pro.slicesStarted == 0)
        #expect(pro.slicesCompleted == 0)
        #expect(pro.slicesPaused.isEmpty)
        #expect(pro.slicesFailed.isEmpty)
        #expect(sixteen.slicesFailed.isEmpty)
    }

    // MARK: - primaryCause precedence

    @Test("primaryCause picks the highest-tier cause (userPreempted beats thermal)")
    func primaryCauseHonorsPrecedence() {
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 3
        )
        // thermal is environmental-transient; userPreempted is user-initiated
        // — userPreempted must win regardless of input order.
        let forward = CauseAttributionPolicy.primaryCause(
            among: [.thermal, .userPreempted],
            context: context
        )
        let reverse = CauseAttributionPolicy.primaryCause(
            among: [.userPreempted, .thermal],
            context: context
        )
        #expect(forward == .userPreempted)
        #expect(reverse == .userPreempted)
    }

    @Test("primaryCause returns nil on an empty cause list")
    func primaryCauseEmptyReturnsNil() {
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 3
        )
        #expect(
            CauseAttributionPolicy.primaryCause(
                among: [],
                context: context
            ) == nil
        )
    }

    @Test("primaryCause returns the single cause when only one is live")
    func primaryCauseSingleReturnsThatCause() {
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 3
        )
        #expect(
            CauseAttributionPolicy.primaryCause(
                among: [.thermal],
                context: context
            ) == .thermal
        )
    }

    // MARK: - CauseEmissionRegistry exhaustiveness

    @Test("every Phase-1 emitting InternalMissCause has a registered (live-or-planned) emitter")
    func everyPhase1EmittingCauseHasEmitter() {
        SliceCompletionInstrumentation.bootstrap()
        for cause in CauseEmissionRegistry.phase1EmittingCauses {
            #expect(
                CauseEmissionRegistry.isDeclared(cause: cause),
                "No production emitter registered for \(cause); add a CauseEmissionRegistry.declareLive(...) or declarePlanned(...) call in declareEmitters()"
            )
        }
    }

    @Test("every Phase-1 LIVE cause has at least one isLiveEmitter tag")
    func everyPhase1LiveCauseHasLiveEmitter() {
        SliceCompletionInstrumentation.bootstrap()
        for cause in CauseEmissionRegistry.phase1LiveCauses {
            #expect(
                CauseEmissionRegistry.isLiveEmitter(cause: cause),
                "No LIVE emitter registered for \(cause); phase1LiveCauses advertises it but declareEmitters() does not declareLive() it"
            )
        }
    }

    @Test("phase1LiveCauses is a subset of phase1EmittingCauses")
    func phase1LiveCausesAreSubsetOfEmitters() {
        #expect(
            CauseEmissionRegistry.phase1LiveCauses.isSubset(
                of: CauseEmissionRegistry.phase1EmittingCauses
            )
        )
    }

    @Test("planned emitters are declared but not live")
    func plannedEmittersAreDeclaredButNotLive() {
        SliceCompletionInstrumentation.bootstrap()
        let plannedCauses = CauseEmissionRegistry.phase1EmittingCauses
            .subtracting(CauseEmissionRegistry.phase1LiveCauses)
        // There is one edge case: a cause that is present in both
        // `phase1LiveCauses` and planned (pipelineError has both a live
        // DownloadManager site and a planned runner site). For any cause
        // that is NOT in phase1LiveCauses, `isLiveEmitter` must be false.
        for cause in plannedCauses {
            #expect(
                !CauseEmissionRegistry.isLiveEmitter(cause: cause),
                "\(cause) is outside phase1LiveCauses so it must have no live emitter yet"
            )
            // But it should still be declared at all so the
            // exhaustiveness test passes.
            #expect(CauseEmissionRegistry.isDeclared(cause: cause))
        }
    }

    @Test("phase1EmittingCauses + phase1NonEmittingCauses partition InternalMissCause")
    func phase1PartitionCoversFullDomain() {
        let emitting = CauseEmissionRegistry.phase1EmittingCauses
        let nonEmitting = CauseEmissionRegistry.phase1NonEmittingCauses
        let union = emitting.union(nonEmitting)
        #expect(emitting.isDisjoint(with: nonEmitting),
                "emitting and non-emitting Phase-1 cause sets must be disjoint")
        #expect(union == Set(InternalMissCause.allCases),
                "Every InternalMissCause must be classified as emitting or non-emitting in Phase 1")
    }

    @Test("phase1NonEmittingCauses covers eligibility-only variants")
    func phase1NonEmittersAreEligibilityOnly() {
        // If new variants get added to the non-emitting set in later phases,
        // this test will need updating. Keeping it pinned to the bead spec
        // means accidental drift trips the check immediately.
        #expect(CauseEmissionRegistry.phase1NonEmittingCauses == [
            .noRuntimeGrant,
            .modelTemporarilyUnavailable,
            .unsupportedEpisodeLanguage,
        ])
    }

    @Test("bootstrap() is idempotent — repeated calls do not duplicate tags")
    func bootstrapIsIdempotent() {
        SliceCompletionInstrumentation.bootstrap()
        let first = CauseEmissionRegistry.tags(for: .pipelineError)
        SliceCompletionInstrumentation.bootstrap()
        SliceCompletionInstrumentation.bootstrap()
        let third = CauseEmissionRegistry.tags(for: .pipelineError)
        #expect(first == third)
    }

    // MARK: - Live upgrades (1nl6 fix cycle)

    @Test("taskExpired has a live emitter after scheduler wire-up")
    func taskExpiredIsLive() {
        SliceCompletionInstrumentation.bootstrap()
        #expect(CauseEmissionRegistry.isLiveEmitter(cause: .taskExpired))
    }

    @Test("userCancelled has a live emitter after scheduler wire-up")
    func userCancelledIsLive() {
        SliceCompletionInstrumentation.bootstrap()
        #expect(CauseEmissionRegistry.isLiveEmitter(cause: .userCancelled))
    }

    @Test("phase1LiveCauses.count is at least 7 after the 1nl6 fix cycle")
    func phase1LiveCausesAtLeastSeven() {
        #expect(CauseEmissionRegistry.phase1LiveCauses.count >= 7)
    }

    // MARK: - SliceCompletionInstrumentation emission helpers

    @Test("recordPaused builds metadata and increments SliceCounters.slicesPaused")
    func recordPausedBuildsMetadataAndIncrementsCounters() async {
        // Snapshot the current count so we don't depend on suite ordering.
        let before = await SliceCompletionInstrumentation.counters.snapshot(
            deviceClass: .iPhone17Pro
        )
        let beforeThermal = before.slicesPaused[.thermal] ?? 0

        let metadata = await SliceCompletionInstrumentation.recordPaused(
            cause: .thermal,
            deviceClass: .iPhone17Pro,
            sliceDurationMs: 2_500,
            bytesProcessed: 8_192,
            shardsCompleted: 1,
            extras: ["stage": "test"]
        )

        #expect(metadata.sliceDurationMs == 2_500)
        #expect(metadata.bytesProcessed == 8_192)
        #expect(metadata.shardsCompleted == 1)
        #expect(metadata.deviceClass == DeviceClass.iPhone17Pro.rawValue)
        #expect(metadata.extras["stage"] == "test")

        let after = await SliceCompletionInstrumentation.counters.snapshot(
            deviceClass: .iPhone17Pro
        )
        #expect((after.slicesPaused[.thermal] ?? 0) == beforeThermal + 1)
    }

    @Test("recordFailed builds metadata and increments SliceCounters.slicesFailed")
    func recordFailedBuildsMetadataAndIncrementsCounters() async {
        let before = await SliceCompletionInstrumentation.counters.snapshot(
            deviceClass: .iPhone16
        )
        let beforePipeline = before.slicesFailed[.pipelineError] ?? 0

        let metadata = await SliceCompletionInstrumentation.recordFailed(
            cause: .pipelineError,
            deviceClass: .iPhone16,
            sliceDurationMs: 100,
            bytesProcessed: 1_024,
            shardsCompleted: 0,
            extras: ["stage": "test.failed"]
        )

        #expect(metadata.deviceClass == DeviceClass.iPhone16.rawValue)

        let after = await SliceCompletionInstrumentation.counters.snapshot(
            deviceClass: .iPhone16
        )
        #expect((after.slicesFailed[.pipelineError] ?? 0) == beforePipeline + 1)
    }

    // MARK: - Spec F: finalized writes cause = NULL

    /// Bead spec explicitly requires an assertion that `finalized` rows
    /// write `cause = NULL`. The parallel idempotence test in
    /// `EpisodeLeaseAndWorkJournalTests` confirms finalized rounds
    /// through `releaseEpisodeLease(cause: nil)`; this test repeats
    /// the read-back contract in the Slice-completion suite so a
    /// drift in the append path (stamping a non-nil cause) fails here
    /// rather than only in the Ad-detection suite.
    @Test("releaseEpisodeLease(finalized) writes cause = nil to work_journal")
    func finalizedRowStoresNullCause() async throws {
        let store = try await makeTestStore()
        let episodeId = "ep-1nl6-finalized-cause-null"
        try await store.insertJob(
            makeAnalysisJob(
                jobId: "j-1nl6-fin",
                episodeId: episodeId,
                workKey: "wk-1nl6-fin"
            )
        )
        let gen = UUID()
        let epoch = try await store.fetchSchedulerEpoch() ?? 0
        _ = try await store.acquireEpisodeLease(
            episodeId: episodeId,
            ownerWorkerId: "worker-1nl6",
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            now: 7_000_000,
            ttlSeconds: 30
        )

        try await store.releaseEpisodeLease(
            episodeId: episodeId,
            generationID: gen.uuidString,
            schedulerEpoch: epoch,
            eventType: .finalized,
            cause: nil,
            now: 7_000_100
        )

        let entries = try await store.fetchWorkJournalEntries(
            episodeId: episodeId,
            generationID: gen.uuidString
        )
        let finalized = entries.filter { $0.eventType == .finalized }
        #expect(finalized.count == 1)
        #expect(finalized.first?.cause == nil,
                "finalized rows must write cause = NULL; got \(String(describing: finalized.first?.cause))")
    }
}

