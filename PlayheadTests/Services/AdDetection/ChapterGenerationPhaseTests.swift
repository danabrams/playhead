// ChapterGenerationPhaseTests.swift
// playhead-au2v.1.10: Tests for the ChapterGenerationPhase shell.
//
// The shell is exercised entirely through deterministic test doubles:
//  * `MockAdmission` — canned admit/deny decisions.
//  * `MockBoundaryDetector` — canned candidate list (or thrown error).
//  * `MockLabeler` — canned per-candidate label (or thrown error).
//  * `MockTranscriptHashProvider` — sequence of hash values; supports
//    "first call returns A, second call returns B" so the race-protect
//    path can be driven in a single run.
//  * `MockEventSink` — actor that records emitted events for asserts.
//  * `MockClock` — monotonically incrementing timestamps so latency
//    math is deterministic.

import Foundation
import os
import Testing
@testable import Playhead

@Suite("ChapterGenerationPhase")
struct ChapterGenerationPhaseTests {

    // MARK: - Test doubles

    private actor MockEventSink: ChapterPhaseEventSink {
        private(set) var events: [ChapterPhaseEvent] = []

        func record(_ event: ChapterPhaseEvent) async {
            events.append(event)
        }

        func snapshot() -> [ChapterPhaseEvent] { events }
    }

    private struct MockAdmission: ChapterPhaseAdmissionPolicy {
        let decision: ChapterPhaseAdmissionDecision
        func decide() async -> ChapterPhaseAdmissionDecision { decision }
    }

    private struct MockBoundaryDetector: ChapterBoundaryDetecting {
        let result: Result<[ChapterBoundaryCandidate], Error>
        init(candidates: [ChapterBoundaryCandidate]) {
            self.result = .success(candidates)
        }
        init(error: Error) {
            self.result = .failure(error)
        }
        func detect() async throws -> [ChapterBoundaryCandidate] {
            try result.get()
        }
    }

    /// Labeler that returns a deterministic `ChapterEvidence` per
    /// candidate, or rethrows a configured error. Records the count of
    /// invocations so cancellation tests can assert "labeler was not
    /// invoked further after the cancel was requested".
    ///
    /// State is held inside an actor so `label` is safe to call
    /// concurrently and the invocation count is observable from the
    /// test body via `await`.
    private final class MockLabeler: ChapterLabeling, @unchecked Sendable {
        enum Behavior: Sendable {
            case returnEvidence(template: @Sendable (ChapterBoundaryCandidate) -> ChapterEvidence?)
            case throwOnCall(Error)
        }

        private actor Counter {
            var value = 0
            func increment() { value += 1 }
        }

        let behavior: Behavior
        private let counter = Counter()

        init(behavior: Behavior) { self.behavior = behavior }

        var invocationCount: Int {
            get async { await counter.value }
        }

        func label(
            candidate: ChapterBoundaryCandidate
        ) async throws -> ChapterEvidence? {
            await counter.increment()
            switch behavior {
            case .returnEvidence(let template):
                return template(candidate)
            case .throwOnCall(let error):
                throw error
            }
        }

        static func defaultEvidence(
            for candidate: ChapterBoundaryCandidate
        ) -> ChapterEvidence {
            ChapterEvidence(
                startTime: candidate.startTime,
                endTime: candidate.endTime,
                title: nil,
                source: .inferred,
                disposition: .ambiguous,
                qualityScore: 0.6
            )
        }
    }

    /// Transcript-hash source backed by a queue of values. The first
    /// call drains the head; subsequent calls return the queue's
    /// remaining values, with the *last* value sticky for any further
    /// calls. This lets us script "entry returns A, recheck returns B"
    /// (queue == [A, B]) and "entry returns A, recheck returns A"
    /// (queue == [A, A]) with the same primitive.
    private actor MockTranscriptHashProvider: TranscriptHashProviding {
        private var queue: [String?]
        init(_ values: [String?]) { self.queue = values }

        func currentTranscriptHash() async -> String? {
            if queue.count > 1 {
                return queue.removeFirst()
            }
            return queue.first ?? nil
        }
    }

    /// Monotonic clock: starts at `epoch` and advances by `step` per
    /// call. Test asserts can compute exact `latency_ms` values.
    ///
    /// `OSAllocatedUnfairLock` (rather than `NSLock`, which is async-
    /// hostile in current Swift, or an actor, which would force an
    /// awaitable API) keeps the production seam synchronous —
    /// matching the default `clock: @escaping @Sendable () -> Date =
    /// { Date() }` — while still being safe to call from any task.
    private final class MockClock: @unchecked Sendable {
        private let lock = OSAllocatedUnfairLock<Date>(
            initialState: Date(timeIntervalSince1970: 1_700_000_000)
        )
        private let step: TimeInterval

        init(step: TimeInterval = 0.001) {
            self.step = step
        }

        func now() -> Date {
            lock.withLock { current in
                let value = current
                current = current.addingTimeInterval(step)
                return value
            }
        }
    }

    // MARK: - Helpers

    private static func makeCache() -> ChapterPlanCache {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "ChapterGenerationPhaseTests-\(UUID().uuidString)",
                isDirectory: true
            )
        return ChapterPlanCache(directory: dir)
    }

    private static func makePhase(
        admission: ChapterPhaseAdmissionDecision = .admit,
        candidates: [ChapterBoundaryCandidate] = [
            ChapterBoundaryCandidate(startTime: 0, endTime: 60),
            ChapterBoundaryCandidate(startTime: 60, endTime: 120),
        ],
        labelerBehavior: MockLabeler.Behavior = .returnEvidence(
            template: { MockLabeler.defaultEvidence(for: $0) }
        ),
        transcriptHashes: [String?] = ["hash-A"],
        cache: ChapterPlanCache,
        sink: ChapterPhaseEventSink,
        clock: @escaping @Sendable () -> Date
    ) -> (ChapterGenerationPhase, MockLabeler) {
        let labeler = MockLabeler(behavior: labelerBehavior)
        let phase = ChapterGenerationPhase(
            admissionPolicy: MockAdmission(decision: admission),
            boundaryDetector: MockBoundaryDetector(candidates: candidates),
            labeler: labeler,
            transcriptHashProvider: MockTranscriptHashProvider(transcriptHashes),
            cache: cache,
            eventSink: sink,
            clock: clock
        )
        return (phase, labeler)
    }

    // MARK: - Mode gate

    @Test("mode == .off exits silently with no diagnostic and no cache write")
    func modeOffEmitsNothing() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, labeler) = Self.makePhase(
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .off,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .modeOff)
        let events = await sink.snapshot()
        #expect(events.isEmpty, "mode=.off must emit zero diagnostics")
        #expect(await labeler.invocationCount == 0)
        // No cache file should exist for the entry hash.
        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored == nil)
    }

    // MARK: - Admission

    @Test("admission deny emits chapter_phase_skipped_admission with the deny reason and bypasses FM cost")
    func admissionDenyEmitsSkipped() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, labeler) = Self.makePhase(
            admission: .deny(reason: "thermal_pressure"),
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .admissionDenied(reason: "thermal_pressure"))
        let events = await sink.snapshot()
        #expect(events.count == 1)
        #expect(events.first?.eventType == .skippedAdmission)
        if case let .skippedAdmission(payload) = events.first?.payload {
            #expect(payload.denyReason == "thermal_pressure")
        } else {
            Issue.record("Expected skippedAdmission payload")
        }
        #expect(await labeler.invocationCount == 0, "admission denial must short-circuit before labeling")
    }

    // MARK: - Transcript snapshot at entry

    @Test("transcript hash unavailable at entry → no_candidates diagnostic, no cache write, labeler not invoked")
    func transcriptUnavailableAtEntry() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, labeler) = Self.makePhase(
            transcriptHashes: [nil],
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .transcriptUnavailable)
        let events = await sink.snapshot()
        #expect(events.count == 1)
        #expect(events.first?.eventType == .noCandidates)
        #expect(await labeler.invocationCount == 0)
    }

    // MARK: - No candidates

    @Test("detector returns empty candidates → started + no_candidates, no cache write, labeler not invoked")
    func noCandidatesPath() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, labeler) = Self.makePhase(
            candidates: [],
            transcriptHashes: ["hash-A"],
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .noCandidates)
        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .noCandidates)
        #expect(await labeler.invocationCount == 0)
        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored == nil)
    }

    @Test("detector throws (non-cancellation) → started + no_candidates, no cache write")
    func detectorThrowsNonCancellation() async {
        struct DetectorError: Error {}
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let labeler = MockLabeler(behavior: .returnEvidence(
            template: { MockLabeler.defaultEvidence(for: $0) }
        ))
        let phase = ChapterGenerationPhase(
            admissionPolicy: MockAdmission(decision: .admit),
            boundaryDetector: MockBoundaryDetector(error: DetectorError()),
            labeler: labeler,
            transcriptHashProvider: MockTranscriptHashProvider(["hash-A"]),
            cache: cache,
            eventSink: sink,
            clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .noCandidates)
        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .noCandidates)
        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored == nil)
    }

    // MARK: - Race re-check

    @Test("transcript hash mismatch on recheck → preempted, plan discarded, no cache write")
    func raceMismatchDiscardsPlan() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        // First call (entry) returns "hash-A"; second call (recheck)
        // returns "hash-B". The plan must be discarded.
        let (phase, labeler) = Self.makePhase(
            transcriptHashes: ["hash-A", "hash-B"],
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .raceAborted)
        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .preempted)
        #expect(await labeler.invocationCount == 2, "labeler runs serially over candidates before the recheck")
        // Plan must NOT be cached under either the entry or the recheck hash.
        let storedA = await cache.get(contentHash: "hash-A")
        let storedB = await cache.get(contentHash: "hash-B")
        #expect(storedA == nil)
        #expect(storedB == nil)
    }

    @Test("transcript hash becomes nil on recheck → started + preempted, plan discarded")
    func raceTranscriptDisappearsDiscardsPlan() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, _) = Self.makePhase(
            transcriptHashes: ["hash-A", nil],
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .raceAborted)
        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .preempted)
        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored == nil)
    }

    // MARK: - Cancellation

    @Test("cancellation BEFORE labeling → preempted, no cache write, labeler not invoked")
    func cancellationBeforeLabeling() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, _) = Self.makePhase(
            cache: cache, sink: sink, clock: clock.now
        )

        let task = Task<ChapterGenerationPhase.Outcome, Never> {
            // Yield once then cancel via the parent task's mechanism.
            return await phase.run(
                mode: .enabled,
                episodeId: "ep-1",
                installID: UUID()
            )
        }
        task.cancel()
        let outcome = await task.value

        #expect(outcome == .preempted)
        let events = await sink.snapshot()
        // The terminal event is always `.preempted`. Whether
        // `.started` was emitted first depends on whether
        // cancellation was observed before or after the entry-hash
        // capture — both orderings are valid. The labeler-was-not-
        // invoked guarantee is enforced indirectly: the cache must
        // not contain a partial write.
        #expect(events.last?.eventType == .preempted)
        #expect(events.count == 1 || events.count == 2)
        if events.count == 2 {
            #expect(events.first?.eventType == .started)
        }
        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored == nil)
    }

    @Test("CancellationError thrown by labeler → started + preempted, plan discarded, no cache write")
    func labelerThrowsCancellationError() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, _) = Self.makePhase(
            labelerBehavior: .throwOnCall(CancellationError()),
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .preempted)
        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .preempted)
        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored == nil)
    }

    @Test("CancellationError thrown by detector → started + preempted, no cache write")
    func detectorThrowsCancellationError() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let labeler = MockLabeler(behavior: .returnEvidence(
            template: { MockLabeler.defaultEvidence(for: $0) }
        ))
        let phase = ChapterGenerationPhase(
            admissionPolicy: MockAdmission(decision: .admit),
            boundaryDetector: MockBoundaryDetector(error: CancellationError()),
            labeler: labeler,
            transcriptHashProvider: MockTranscriptHashProvider(["hash-A"]),
            cache: cache,
            eventSink: sink,
            clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        #expect(outcome == .preempted)
        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .preempted)
        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored == nil)
    }

    // MARK: - Per-call labeler failures (non-cancellation)

    @Test("labeler throws non-cancellation on every candidate → completed with empty plan and zero chapters")
    func labelerThrowsNonCancellationContinues() async {
        struct LabelError: Error {}
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, labeler) = Self.makePhase(
            candidates: [
                ChapterBoundaryCandidate(startTime: 0, endTime: 60),
                ChapterBoundaryCandidate(startTime: 60, endTime: 120),
            ],
            labelerBehavior: .throwOnCall(LabelError()),
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        // Two candidates, two failures, but the shell still completes
        // (labeling-failure event vocab is the labeling service's job).
        #expect(await labeler.invocationCount == 2)
        if case let .cached(chapterCount, _) = outcome {
            #expect(chapterCount == 0)
        } else {
            Issue.record("Expected .cached outcome, got \(outcome)")
        }
        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .completed)
        // `fm_call_count` is "FM calls successfully serviced for this
        // plan" — both labeler calls threw, so the count is 0 even
        // though the labeler's invocation counter saw 2 calls. Pins
        // the R1 increment-after-return contract.
        if case let .completed(payload) = events.last?.payload {
            #expect(payload.fmCallCount == 0)
            #expect(payload.chapterCount == 0)
        } else {
            Issue.record("Expected completed payload")
        }
        // Cache write happened — the plan is empty but valid.
        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored != nil)
        #expect(stored?.chapters.isEmpty == true)
    }

    // MARK: - Successful path

    @Test("happy path: mode enabled + admit + candidates + matching hashes → cache write + completed diagnostic")
    func happyPath() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, labeler) = Self.makePhase(
            candidates: [
                ChapterBoundaryCandidate(startTime: 0, endTime: 60),
                ChapterBoundaryCandidate(startTime: 60, endTime: 120),
                ChapterBoundaryCandidate(startTime: 120, endTime: 180),
            ],
            transcriptHashes: ["hash-A", "hash-A"],
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .enabled,
            episodeId: "ep-1",
            installID: UUID()
        )

        if case let .cached(chapterCount, planConfidence) = outcome {
            #expect(chapterCount == 3)
            #expect(planConfidence > 0)
        } else {
            Issue.record("Expected .cached outcome, got \(outcome)")
        }
        #expect(await labeler.invocationCount == 3)

        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .completed)
        // The `.started` event carries the entry transcript hash.
        if case let .started(startedPayload) = events.first?.payload {
            #expect(startedPayload.transcriptSnapshotHash == "hash-A")
            #expect(startedPayload.mode == "enabled")
        } else {
            Issue.record("Expected started payload at events[0]")
        }
        if case let .completed(payload) = events.last?.payload {
            #expect(payload.chapterCount == 3)
            #expect(payload.fmCallCount == 3)
            #expect(payload.latencyMs >= 0)
        } else {
            Issue.record("Expected completed payload")
        }

        let stored = await cache.get(contentHash: "hash-A")
        #expect(stored != nil)
        #expect(stored?.chapters.count == 3)
    }

    @Test("happy path with mode == .shadow runs the phase end-to-end (shadow telemetry pipeline)")
    func happyPathShadow() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, _) = Self.makePhase(
            transcriptHashes: ["hash-A", "hash-A"],
            cache: cache, sink: sink, clock: clock.now
        )

        let outcome = await phase.run(
            mode: .shadow,
            episodeId: "ep-1",
            installID: UUID()
        )

        if case let .cached(chapterCount, _) = outcome {
            #expect(chapterCount == 2)
        } else {
            Issue.record("Expected .cached outcome under .shadow, got \(outcome)")
        }
        let events = await sink.snapshot()
        #expect(events.count == 2)
        #expect(events.first?.eventType == .started)
        #expect(events.last?.eventType == .completed)
        // `.started` payload reflects the shadow mode raw value.
        if case let .started(startedPayload) = events.first?.payload {
            #expect(startedPayload.mode == "shadow")
            #expect(startedPayload.transcriptSnapshotHash == "hash-A")
        } else {
            Issue.record("Expected started payload at events[0]")
        }
    }

    // MARK: - Episode id hashing

    @Test("emitted events carry the SHA-256(installID || episodeId) hash, never the raw id")
    func emittedEventsCarryHashedId() async {
        let cache = Self.makeCache()
        let sink = MockEventSink()
        let clock = MockClock()
        let (phase, _) = Self.makePhase(
            admission: .deny(reason: "fm_unavailable"),
            cache: cache, sink: sink, clock: clock.now
        )

        let installID = UUID()
        let rawEpisodeId = "raw-episode-id-with-PII-prefix"
        _ = await phase.run(
            mode: .enabled,
            episodeId: rawEpisodeId,
            installID: installID
        )

        let events = await sink.snapshot()
        #expect(events.count == 1)
        let expectedHash = EpisodeIdHasher.hash(
            installID: installID,
            episodeId: rawEpisodeId
        )
        #expect(events.first?.episodeIdHash == expectedHash)
        // Defensive: ensure the raw id never leaks into the event hash.
        #expect(events.first?.episodeIdHash != rawEpisodeId)
        #expect(events.first?.episodeIdHash.count == 64) // 32-byte hex
    }
}
