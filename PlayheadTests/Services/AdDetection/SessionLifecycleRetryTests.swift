// SessionLifecycleRetryTests.swift
//
// Targeted tests verifying that each retry attempt in FoundationModelClassifier
// mints a fresh LanguageModelSession rather than reusing the session from the
// failed attempt. Reusing a session accumulates conversation history across
// attempts and violates the single-use session lifecycle established in bd-34e.
//
// Covered paths:
//   - Coarse pass .backoffAndRetry (rate-limited windows)
//   - Coarse pass .shrinkWindowAndRetryOnce legacy midpoint split (context overflow)
//   - Refinement pass .backoffAndRetry (rate-limited windows)
//   - Refinement pass .shrinkWindowAndRetryOnce (context overflow)
//   - Retry count / limit behaviour is unaffected by session freshness

import Foundation
import Testing

@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

// MARK: - Session-tracking runtime

/// Assigns a monotonically-increasing ID to every `makeSession()` call and
/// records which session ID performed each `respond` / prewarm call.
/// Used to assert that retries receive distinct sessions rather than reusing
/// the session from the failed attempt.
private actor SessionTracker {
    struct PrewarmRecord: Sendable, Equatable {
        let sessionID: Int
        let promptPrefix: String
    }

    struct RespondRecord: Sendable, Equatable {
        let sessionID: Int
    }

    private var _sessionCount = 0
    private(set) var prewarmRecords: [PrewarmRecord] = []
    private(set) var coarseRecords: [RespondRecord] = []
    private(set) var refinementRecords: [RespondRecord] = []

    private var coarseQueue: [Result<CoarseScreeningSchema, Error>] = []
    private var refinementQueue: [Result<RefinementWindowSchema, Error>] = []

    var sessionCount: Int { _sessionCount }

    func enqueueCoarse(_ result: Result<CoarseScreeningSchema, Error>) {
        coarseQueue.append(result)
    }

    func enqueueRefinement(_ result: Result<RefinementWindowSchema, Error>) {
        refinementQueue.append(result)
    }

    func nextSession() -> Int {
        _sessionCount += 1
        return _sessionCount
    }

    func recordPrewarm(sessionID: Int, promptPrefix: String) {
        prewarmRecords.append(PrewarmRecord(sessionID: sessionID, promptPrefix: promptPrefix))
    }

    func respondCoarse(sessionID: Int) throws -> CoarseScreeningSchema {
        coarseRecords.append(RespondRecord(sessionID: sessionID))
        if coarseQueue.isEmpty {
            return CoarseScreeningSchema(disposition: .noAds, support: nil)
        }
        return try coarseQueue.removeFirst().get()
    }

    func respondRefinement(sessionID: Int) throws -> RefinementWindowSchema {
        refinementRecords.append(RespondRecord(sessionID: sessionID))
        if refinementQueue.isEmpty {
            return RefinementWindowSchema(spans: [])
        }
        return try refinementQueue.removeFirst().get()
    }
}

/// Build a `FoundationModelClassifier.Runtime` backed by `tracker` with
/// configurable per-call token count so planner window sizes are predictable.
private func makeTrackedRuntime(
    tracker: SessionTracker,
    contextSize: Int,
    tokenCountPerCall: Int,
    coarseSchemaTokens: Int = 4,
    refinementSchemaTokens: Int = 32
) -> FoundationModelClassifier.Runtime {
    FoundationModelClassifier.Runtime(
        availabilityStatus: { _ in nil },
        contextSize: { contextSize },
        tokenCount: { _ in tokenCountPerCall },
        coarseSchemaTokenCount: { coarseSchemaTokens },
        refinementSchemaTokenCount: { refinementSchemaTokens },
        boundarySchemaTokenCount: { 32 },
        makeSession: {
            let id = await tracker.nextSession()
            return FoundationModelClassifier.Runtime.Session(
                prewarm: { prefix in
                    await tracker.recordPrewarm(sessionID: id, promptPrefix: prefix)
                },
                respondCoarse: { _ in
                    try await tracker.respondCoarse(sessionID: id)
                },
                respondRefinement: { _ in
                    try await tracker.respondRefinement(sessionID: id)
                }
            )
        }
    )
}

// MARK: - Error helpers

private func rateLimitedError() -> Error {
    #if canImport(FoundationModels)
    if #available(iOS 26.0, *) {
        return LanguageModelSession.GenerationError.rateLimited(
            LanguageModelSession.GenerationError.Context(debugDescription: "session-lifecycle-test")
        )
    }
    #endif
    return NSError(domain: "SessionLifecycleRetryTests", code: 429,
                   userInfo: [NSLocalizedDescriptionKey: "rateLimited"])
}

private func exceededContextError() -> Error {
    #if canImport(FoundationModels)
    if #available(iOS 26.0, *) {
        return LanguageModelSession.GenerationError.exceededContextWindowSize(
            LanguageModelSession.GenerationError.Context(debugDescription: "session-lifecycle-test")
        )
    }
    #endif
    return NSError(domain: "SessionLifecycleRetryTests", code: 413,
                   userInfo: [NSLocalizedDescriptionKey: "exceededContextWindow"])
}

// MARK: - Segment helpers

private func seg(_ index: Int, text: String = "segment text") -> AdTranscriptSegment {
    AdTranscriptSegment(
        atoms: [
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "asset-slr",
                    transcriptVersion: "tx-slr",
                    atomOrdinal: index
                ),
                contentHash: "hash-\(index)",
                startTime: Double(index) * 5,
                endTime: Double(index) * 5 + 5,
                text: text,
                chunkIndex: index
            )
        ],
        segmentIndex: index
    )
}

// MARK: - Tests

@Suite("Session lifecycle: retry paths mint fresh sessions", .serialized)
struct SessionLifecycleRetryTests {

    // MARK: - Coarse pass: backoffAndRetry (rate-limited)

    /// A rate-limited window retries once on a FRESH session (session 2),
    /// not the same session that failed (session 1). With one segment packed
    /// into one window, total calls = 2 (initial fail + retry success) and
    /// total sessions = 2.
    @Test("coarse backoff retry uses a fresh session distinct from the failed attempt")
    func coarseBackoffRetryUsesFreshSession() async throws {
        let tracker = SessionTracker()

        // One window: initial call rate-limited, retry succeeds.
        await tracker.enqueueCoarse(.failure(rateLimitedError()))
        await tracker.enqueueCoarse(.success(CoarseScreeningSchema(disposition: .noAds, support: nil)))

        // contextSize=96, tokenCount=8, coarseSchemaTokens=4 → budget=10.
        // One segment per window (8 tokens fits, 2 × 8 = 16 doesn't → 1 window).
        let runtime = makeTrackedRuntime(
            tracker: tracker,
            contextSize: 96,
            tokenCountPerCall: 8,
            coarseSchemaTokens: 4
        )
        let classifier = FoundationModelClassifier(
            runtime: runtime,
            config: .init(safetyMarginTokens: 4, maximumResponseTokens: 6)
        )

        let output = try await classifier.coarsePassA(segments: [seg(0)])

        let coarseRecords = await tracker.coarseRecords
        let prewarmRecords = await tracker.prewarmRecords
        let sessionCount = await tracker.sessionCount

        // The window recovers after backoff retry.
        #expect(output.status == .success)

        // Exactly 2 respond calls: initial (fail) + retry (success).
        #expect(coarseRecords.count == 2,
            "expected 2 respond calls (initial + retry), got \(coarseRecords.count)")

        // The initial attempt and its backoff retry MUST use different session IDs.
        let initialID = coarseRecords[0].sessionID
        let retryID = coarseRecords[1].sessionID
        #expect(initialID != retryID,
            "backoff retry must mint a fresh session, not reuse session \(initialID)")

        // Both sessions must have been prewarmed before their respond call.
        let prewarmedIDs = Set(prewarmRecords.map(\.sessionID))
        #expect(prewarmedIDs.contains(initialID),
            "initial session \(initialID) was not prewarmed")
        #expect(prewarmedIDs.contains(retryID),
            "retry session \(retryID) was not prewarmed")

        // Total session count must equal the number of unique sessions used.
        #expect(sessionCount == 2)
    }

    /// When both the initial attempt and the single backoff retry fail with
    /// rate-limited, the window is abandoned — the retry count limit is exactly
    /// one. The second retry call must still use a fresh session.
    @Test("coarse backoff retry limit: two rate-limited failures abandon the window")
    func coarseBackoffRetryLimitAbandonedWindow() async throws {
        let tracker = SessionTracker()

        // One window: initial fail + retry fail → window abandoned.
        await tracker.enqueueCoarse(.failure(rateLimitedError()))
        await tracker.enqueueCoarse(.failure(rateLimitedError()))

        let runtime = makeTrackedRuntime(
            tracker: tracker,
            contextSize: 96,
            tokenCountPerCall: 8,
            coarseSchemaTokens: 4
        )
        let classifier = FoundationModelClassifier(
            runtime: runtime,
            config: .init(safetyMarginTokens: 4, maximumResponseTokens: 6)
        )

        let output = try await classifier.coarsePassA(segments: [seg(0)])

        let coarseRecords = await tracker.coarseRecords
        let sessionCount = await tracker.sessionCount

        // Window is gracefully abandoned; pass still returns success with 0 windows.
        #expect(output.failedWindowStatuses == [.rateLimited])

        // Exactly 2 calls: initial + one backoff retry. No further attempts.
        #expect(coarseRecords.count == 2,
            "expected exactly 2 respond calls (initial + 1 backoff retry), got \(coarseRecords.count)")

        // The retry must use a distinct session from the initial.
        #expect(coarseRecords[0].sessionID != coarseRecords[1].sessionID,
            "failed backoff retry must use a fresh session, not reuse session \(coarseRecords[0].sessionID)")

        // Two sessions total.
        #expect(sessionCount == 2)
    }

    // MARK: - Coarse pass: shrinkWindowAndRetryOnce (context overflow)

    /// When the coarse pass falls back to the legacy midpoint splitter after a
    /// context overflow, each retry sub-plan must use its own fresh session —
    /// none may reuse the session from the overflowed attempt.
    @Test("coarse shrink retry: each sub-plan mints a fresh session")
    func coarseShrinkRetryFreshSessionPerSubPlan() async throws {
        let tracker = SessionTracker()

        // One window of 4 segments at 8 tokens each: initial call overflows,
        // midpoint split produces 2 sub-plans of 2 segments each (both succeed).
        await tracker.enqueueCoarse(.failure(exceededContextError()))
        await tracker.enqueueCoarse(.success(CoarseScreeningSchema(
            disposition: .containsAd,
            support: CoarseSupportSchema(supportLineRefs: [1], certainty: .strong)
        )))
        await tracker.enqueueCoarse(.success(CoarseScreeningSchema(
            disposition: .noAds, support: nil
        )))

        // contextSize=96, tokenCount=8, safetyMargin=4 →
        //   budget = min((96 - 4 - 6 - 4) / 8, 96 / 8) = min(10, 12) = 10.
        // 4 segments × 8 = 32 tokens → exceeds budget of 10, so planner fits
        // them all in one window but coarseResponses throws; then the midpoint
        // splitter retries with [[0,1],[2,3]].
        let runtime = makeTrackedRuntime(
            tracker: tracker,
            contextSize: 96,
            tokenCountPerCall: 8,
            coarseSchemaTokens: 4
        )
        let classifier = FoundationModelClassifier(
            runtime: runtime,
            config: .init(safetyMarginTokens: 4, maximumResponseTokens: 6)
        )

        let output = try await classifier.coarsePassA(segments: [seg(0), seg(1), seg(2), seg(3)])

        let coarseRecords = await tracker.coarseRecords
        let prewarmRecords = await tracker.prewarmRecords

        #expect(output.status == .success)

        // 3 calls: initial overflow + 2 retry sub-plans.
        #expect(coarseRecords.count == 3,
            "expected 3 respond calls (initial + 2 sub-plans), got \(coarseRecords.count)")

        // All three calls must use strictly distinct session IDs.
        let ids = coarseRecords.map(\.sessionID)
        #expect(Set(ids).count == 3,
            "each retry sub-plan must use a distinct session; got sessionIDs \(ids)")

        // In particular, the overflow session must NOT be reused for either retry.
        let overflowID = ids[0]
        #expect(ids[1] != overflowID,
            "first sub-plan must mint a fresh session, not reuse the overflow session \(overflowID)")
        #expect(ids[2] != overflowID,
            "second sub-plan must mint a fresh session, not reuse the overflow session \(overflowID)")

        // All sessions must have been prewarmed before their first respond call.
        let prewarmedIDs = Set(prewarmRecords.map(\.sessionID))
        for record in coarseRecords {
            #expect(prewarmedIDs.contains(record.sessionID),
                "session \(record.sessionID) responded without prior prewarm call")
        }
    }

    // MARK: - Refinement pass: backoffAndRetry (rate-limited)

    /// A rate-limited refinement attempt must be retried on a fresh session —
    /// the retry session ID must differ from the initial session ID.
    @Test("refinement backoff retry uses a fresh session distinct from the failed attempt")
    func refinementBackoffRetryUsesFreshSession() async throws {
        let tracker = SessionTracker()

        // One plan: initial call rate-limited, retry succeeds.
        await tracker.enqueueRefinement(.failure(rateLimitedError()))
        await tracker.enqueueRefinement(.success(RefinementWindowSchema(spans: [])))

        let runtime = makeTrackedRuntime(
            tracker: tracker,
            contextSize: 4_096,
            tokenCountPerCall: 1
        )
        let classifier = FoundationModelClassifier(runtime: runtime)

        let zoomPlan = RefinementWindowPlan(
            windowIndex: 0,
            sourceWindowIndex: 0,
            lineRefs: [0, 1],
            focusLineRefs: [0, 1],
            focusClusters: [[0, 1]],
            prompt: "Refine ad spans.\nL0> \"line a\"\nL1> \"line b\"",
            promptTokenCount: 4,
            startTime: 0,
            endTime: 10,
            stopReason: .minimumSpan,
            promptEvidence: []
        )

        let output = try await classifier.refinePassB(
            zoomPlans: [zoomPlan],
            segments: [seg(0), seg(1)],
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: "asset-slr",
                transcriptVersion: "tx-slr",
                entries: []
            )
        )

        let refinementRecords = await tracker.refinementRecords
        let prewarmRecords = await tracker.prewarmRecords
        let sessionCount = await tracker.sessionCount

        // Plan recovers after backoff retry.
        #expect(output.status == .success)

        // Exactly 2 refinement calls: initial (fail) + retry (success).
        #expect(refinementRecords.count == 2,
            "expected 2 refinement calls (initial + retry), got \(refinementRecords.count)")

        // Initial and retry must use different session IDs.
        let initialID = refinementRecords[0].sessionID
        let retryID = refinementRecords[1].sessionID
        #expect(initialID != retryID,
            "refinement backoff retry must mint a fresh session, not reuse session \(initialID)")

        // Both sessions must have been prewarmed.
        let prewarmedIDs = Set(prewarmRecords.map(\.sessionID))
        #expect(prewarmedIDs.contains(initialID),
            "initial refinement session \(initialID) was not prewarmed")
        #expect(prewarmedIDs.contains(retryID),
            "retry refinement session \(retryID) was not prewarmed")

        #expect(sessionCount == 2)
    }

    /// When both the initial refinement attempt and its single backoff retry
    /// fail, the window is abandoned. The retry count limit is exactly one.
    @Test("refinement backoff retry limit: two failures abandon the window, pass continues")
    func refinementBackoffRetryLimitAbandonedWindowPassContinues() async throws {
        let tracker = SessionTracker()

        // Plan 0: initial fail + retry fail → abandoned.
        // Plan 1: succeed immediately.
        await tracker.enqueueRefinement(.failure(rateLimitedError()))
        await tracker.enqueueRefinement(.failure(rateLimitedError()))
        await tracker.enqueueRefinement(.success(RefinementWindowSchema(spans: [])))

        let runtime = makeTrackedRuntime(
            tracker: tracker,
            contextSize: 4_096,
            tokenCountPerCall: 1
        )
        let classifier = FoundationModelClassifier(runtime: runtime)

        let zoomPlans = (0..<2).map { idx in
            RefinementWindowPlan(
                windowIndex: idx,
                sourceWindowIndex: idx,
                lineRefs: [idx * 2, idx * 2 + 1],
                focusLineRefs: [idx * 2, idx * 2 + 1],
                focusClusters: [[idx * 2, idx * 2 + 1]],
                prompt: "Refine ad spans.\nL\(idx * 2)> \"line a\"\nL\(idx * 2 + 1)> \"line b\"",
                promptTokenCount: 4,
                startTime: Double(idx * 10),
                endTime: Double(idx * 10 + 10),
                stopReason: .minimumSpan,
                promptEvidence: []
            )
        }

        let output = try await classifier.refinePassB(
            zoomPlans: zoomPlans,
            segments: (0..<4).map { seg($0) },
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: "asset-slr",
                transcriptVersion: "tx-slr",
                entries: []
            )
        )

        let refinementRecords = await tracker.refinementRecords
        let sessionCount = await tracker.sessionCount

        // Pass continues; plan 0 is gracefully dropped.
        #expect(output.status == .success)
        #expect(output.failedWindowStatuses == [.rateLimited])

        // 3 calls: initial + retry (both fail) for plan 0, then plan 1 succeeds.
        #expect(refinementRecords.count == 3,
            "expected 3 refinement calls, got \(refinementRecords.count)")

        // The retry must use a different session from the initial.
        #expect(refinementRecords[0].sessionID != refinementRecords[1].sessionID,
            "failed refinement retry must use a fresh session, not reuse session \(refinementRecords[0].sessionID)")

        // Plan 1's session must also be fresh (different from both plan 0 sessions).
        let plan1ID = refinementRecords[2].sessionID
        #expect(plan1ID != refinementRecords[0].sessionID)
        #expect(plan1ID != refinementRecords[1].sessionID)

        // 3 unique sessions total.
        #expect(sessionCount == 3)
    }

    // MARK: - Refinement pass: shrinkWindowAndRetryOnce (context overflow)

    /// A context overflow in the refinement pass triggers a single shrink retry
    /// on a fresh session. The retry must NOT reuse the session from the overflow.
    @Test("refinement shrink retry uses a fresh session distinct from the overflowed attempt")
    func refinementShrinkRetryUsesFreshSession() async throws {
        let tracker = SessionTracker()

        // One plan: initial call overflows, shrunken retry succeeds.
        await tracker.enqueueRefinement(.failure(exceededContextError()))
        await tracker.enqueueRefinement(.success(RefinementWindowSchema(spans: [])))

        // Tight token budget so the plan's promptTokenCount is over budget and
        // triggers the shrink path. Use a contextSize small enough that the
        // shrunken plan fits but the original does not.
        let runtime = makeTrackedRuntime(
            tracker: tracker,
            contextSize: 128,
            tokenCountPerCall: 1,
            coarseSchemaTokens: 4,
            refinementSchemaTokens: 8
        )
        let classifier = FoundationModelClassifier(
            runtime: runtime,
            config: .init(
                safetyMarginTokens: 4,
                coarseMaximumResponseTokens: 6,
                refinementMaximumResponseTokens: 10,
                maximumRefinementSpansPerWindow: 3
            )
        )

        // Build a plan with enough lineRefs that the shrunken version is smaller.
        // The shrunken plan must have at least minimumZoomSpanLines line refs to
        // be valid; the default is 2 so 4 lineRefs shrinks to 2.
        let zoomPlan = RefinementWindowPlan(
            windowIndex: 0,
            sourceWindowIndex: 0,
            lineRefs: [0, 1, 2, 3],
            focusLineRefs: [1, 2],
            focusClusters: [[1, 2]],
            prompt: "Refine ad spans.\nL0> \"line a\"\nL1> \"line b\"\nL2> \"line c\"\nL3> \"line d\"",
            promptTokenCount: 8,
            startTime: 0,
            endTime: 20,
            stopReason: .minimumSpan,
            promptEvidence: []
        )

        let output = try await classifier.refinePassB(
            zoomPlans: [zoomPlan],
            segments: (0..<4).map { seg($0) },
            evidenceCatalog: EvidenceCatalog(
                analysisAssetId: "asset-slr",
                transcriptVersion: "tx-slr",
                entries: []
            )
        )

        let refinementRecords = await tracker.refinementRecords
        let prewarmRecords = await tracker.prewarmRecords

        // The shrink path must always fire: initial overflow + one retry.
        // A shrunken plan with 2 lineRefs meets the minimumZoomSpanLines=2 threshold.
        #expect(refinementRecords.count == 2,
            "expected 2 refinement calls (initial overflow + shrink retry), got \(refinementRecords.count)")

        let overflowID = refinementRecords[0].sessionID
        let retryID = refinementRecords[1].sessionID
        #expect(overflowID != retryID,
            "refinement shrink retry must mint a fresh session, not reuse session \(overflowID)")

        // Both sessions must have been prewarmed.
        let prewarmedIDs = Set(prewarmRecords.map(\.sessionID))
        #expect(prewarmedIDs.contains(overflowID))
        #expect(prewarmedIDs.contains(retryID))

        // Retry succeeded (both enqueued results are consumed).
        #expect(output.status == .success)
    }
}
