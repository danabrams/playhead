// AnalysisJobRunnerSubscribeBeforeStartTests.swift
//
// playhead-ajr subscribe-before-start (RC-4): pin the ordering invariant
// that `AnalysisJobRunner.run(...)` now depends on — the runner MUST
// subscribe to `transcriptEngine.events()` BEFORE it calls
// `startTranscription(...)` / `finishAppending(...)`.
//
// THE BUG (pre-fix ordering):
//   run() called startTranscription + finishAppending and only THEN
//   `events()`. `TranscriptEngineService.events()` returns a fresh,
//   NON-replaying `AsyncStream` continuation — an event emitted before a
//   subscriber registers its continuation is delivered to no one. Under
//   full-suite / device load, the engine's detached transcription Task
//   could reach `emitEvent(.completed(...))` in the window between
//   `startTranscription` and `events()`. The runner then missed the
//   completion, parked on the sibling 300 s timeout arm, and finally
//   returned a spurious `.failed("transcription:zeroCoverage")` after a
//   multi-minute hang (the flightcast "Operation Interrupted"/hang class).
//
// THE FIX:
//   run() subscribes first (subscribe-before-start), so the continuation
//   is registered on the engine actor before any emission can begin.
//
// WHY THIS TEST DRIVES THE ENGINE SEAM (not run() directly):
//   The acceptance criterion permits driving "the runner (or the engine
//   seam) so `.completed` is emitted in the pre-subscribe window on the
//   OLD ordering ... and assert the NEW ordering catches it." A faithful
//   *runner-level* fail-on-old-ordering test is not tractable without
//   larger production changes: `AnalysisJobRunner` holds a CONCRETE
//   `TranscriptEngineService` (no protocol seam to inject a fake that
//   emits synchronously), the real engine runs the loop as a DETACHED
//   Task (so an emission cannot be forced synchronously into the
//   pre-subscribe gap), and the 300 s timeout is hardcoded (a
//   fail-on-old test would hang for minutes). Rather than introduce a
//   protocol seam + injectable timeout (an architectural change), these
//   tests reproduce the exact mechanism the fix relies on, against the
//   REAL engine, deterministically:
//     - `earlySubscribeCatchesCompletion` — subscribe-then-start (the
//       fixed runner ordering) reliably catches `.completed`.
//     - `lateSubscribeMissesCompletion` — a subscriber that registers
//       AFTER the engine emitted `.completed` (the OLD runner ordering)
//       never receives it (no replay). This is precisely the missed
//       completion that fed the old runner's 300 s-timeout failure.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

/// Await the first `.completed` event on the stream, unbounded. Use for
/// POSITIVE expectations only — the `.timeLimit` trait is the backstop, so
/// a genuine "never completes" regression fails deterministically there
/// rather than via a too-short poll window.
private func awaitFirstCompletion(
    on events: AsyncStream<TranscriptEngineEvent>
) async -> String? {
    for await event in events {
        if case .completed(let assetId) = event {
            return assetId
        }
    }
    return nil
}

private func seedTranscriptAsset(store: AnalysisStore, id: String) async throws {
    let asset = AnalysisAsset(
        id: id,
        episodeId: "ep-\(id)",
        assetFingerprint: "fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///test/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "queued",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
    try await store.insertAsset(asset)
}

/// Build a loaded engine over a non-stalling mock recognizer that yields a
/// single early segment for every shard. `serializesRecognizerRequests:
/// false` keeps each test independent of the process-wide recognizer gate,
/// so these run safely in parallel under the full FastTests suite.
private func makeLoadedEngine(store: AnalysisStore) async throws -> TranscriptEngineService {
    let recognizer = MockSpeechRecognizer()
    recognizer.transcribeResult = [
        TranscriptSegment(
            id: 0,
            words: [TranscriptWord(text: "hello", startTime: 0, endTime: 0.5, confidence: 0.95)],
            text: "hello",
            startTime: 0,
            endTime: 0.5,
            avgConfidence: 0.95,
            passType: .fast
        )
    ]
    let speech = SpeechService(
        recognizer: recognizer,
        serializesRecognizerRequests: false
    )
    try await speech.loadFastModel()
    return TranscriptEngineService(speechService: speech, store: store)
}

private let liveSnapshot = PlaybackSnapshot(playheadTime: 0, playbackRate: 1.0, isPlaying: true)

// MARK: - Tests

@Suite("AnalysisJobRunner – subscribe-before-start (playhead-ajr RC-4)")
struct AnalysisJobRunnerSubscribeBeforeStartTests {

    /// THE FIX. Mirror the runner's post-fix ordering: subscribe to
    /// `events()` FIRST, then start + finishAppending. The subscriber must
    /// receive the asset's `.completed` — no window exists in which the
    /// engine could emit before the continuation is registered.
    @Test("Subscribe-before-start catches .completed (fixed runner ordering)",
          .timeLimit(.minutes(1)))
    func earlySubscribeCatchesCompletion() async throws {
        let store = try await makeTestStore()
        try await seedTranscriptAsset(store: store, id: "asset-early")
        let engine = try await makeLoadedEngine(store: store)

        // Subscribe BEFORE start — the fixed ordering.
        let stream = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-early", startTime: 0, duration: 30)],
            analysisAssetId: "asset-early",
            snapshot: liveSnapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-early")

        let completedId = await awaitFirstCompletion(on: stream)
        #expect(completedId == "asset-early",
                "A subscriber established before startTranscription must receive .completed")
    }

    /// THE BUG. A subscriber that registers AFTER the engine has already
    /// emitted `.completed` for the in-flight asset never receives it
    /// (`events()` does not replay). This is exactly the missed completion
    /// the OLD runner ordering suffered — start + finishAppending, then a
    /// too-late `events()` — which fell through to the 300 s timeout arm and
    /// returned `.failed("transcription:zeroCoverage")`.
    ///
    /// Determinism: a `detector` stream (subscribed before any start) gives
    /// an event-driven signal that `.completed(A)` has actually been
    /// emitted. Only after that do we subscribe `late`, then drive a second
    /// asset B to completion so `late` has a positive signal to await. If
    /// `late` sees A first, no-replay is broken and the fix's premise is
    /// false; the first completion it observes must be B.
    @Test("Subscribe-after-emit misses .completed — no replay (old runner ordering)",
          .timeLimit(.minutes(1)))
    func lateSubscribeMissesCompletion() async throws {
        let store = try await makeTestStore()
        try await seedTranscriptAsset(store: store, id: "asset-A")
        try await seedTranscriptAsset(store: store, id: "asset-B")
        let engine = try await makeLoadedEngine(store: store)

        // Instrument: subscribed before any start, so it is guaranteed to
        // observe .completed(asset-A) and thereby confirm the emission
        // happened before we subscribe `late` below.
        let detector = await engine.events()

        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-A", startTime: 0, duration: 30)],
            analysisAssetId: "asset-A",
            snapshot: liveSnapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-A")

        let detectedA = await awaitFirstCompletion(on: detector)
        #expect(detectedA == "asset-A", "Engine must emit .completed for asset-A")

        // asset-A's .completed has now definitely fired. Subscribe LATE —
        // this models the OLD runner ordering (subscribe AFTER the engine
        // already emitted completion for the in-flight asset).
        let late = await engine.events()

        // Drive a second asset so `late` has a positive completion to await.
        await engine.startTranscription(
            shards: [makeShard(id: 0, episodeID: "ep-asset-B", startTime: 0, duration: 30)],
            analysisAssetId: "asset-B",
            snapshot: liveSnapshot
        )
        await engine.finishAppending(analysisAssetId: "asset-B")

        let firstOnLate = await awaitFirstCompletion(on: late)
        #expect(firstOnLate == "asset-B",
                """
                A subscriber established after .completed(asset-A) was emitted must \
                NOT receive it (events() has no replay); the first completion it sees \
                must be asset-B. Receiving "asset-A" would mean the stream replayed a \
                past event — but the real failure this guards is the opposite: the OLD \
                runner ordering subscribed too late and missed the completion entirely, \
                then hung on the 300 s timeout. Got: \(firstOnLate ?? "nil")
                """)
    }
}
