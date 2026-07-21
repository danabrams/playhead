// BackfillReconcileOwnershipTests.swift
// playhead-ud4n: backfill authoritative over nonterminal hot-path ad windows.
//
// The hot path already reconciles + retires; runBackfill previously only
// APPENDED fresh-UUID fusion windows, so a hot false-positive rejected by
// backfill survived beside the authoritative fusion result and could still
// auto-skip (durably, via the beginEpisode preload). This suite pins the
// ownership contract implemented under Design B:
//   • content-addressed fusion ids (BackfillJobRunner.makeFusionWindowId) →
//     idempotent reruns by construction,
//   • the reconcilable predicate (AdDetectionService.isReconcilableBackfillWindow)
//     — the correctness backbone,
//   • pure set-difference reconcile (reconcileBackfillWindows) +
//   • atomic INSERT-OR-REPLACE(new) + DELETE(retired)
//     (AnalysisStore.reconcileBackfillAdWindows), and
//   • retire-before-replace delivery to SkipOrchestrator in runBackfill Step 17.
//
// 13-case matrix: T1 retire FP (persistence), T2 retire→no-auto-skip
// (orchestrator, core AC), T3 replace matched, T4 add new, T5 idempotency,
// T6 .applied preserved, T7 .reverted preserved, T8 correction-replay
// preserved, T9 imported-share preserved, T10 other-detectorVersion untouched,
// T11 rollback atomic, T12 retire-before-replace ordering, T13 suppressed
// rerun flip. Plus a direct predicate unit test.

import Foundation
import Testing
@testable import Playhead

// MARK: - Shared helpers (self-contained; sibling suites keep theirs `private`)

private let kReconcileDetectorVersion = "test-detection-v1"

private func makeReconcileAdChunks(assetId: String) -> [TranscriptChunk] {
    let texts = [
        "Welcome back to the show today.",
        "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.",
        "Back to our conversation about technology and the future of podcasting."
    ]
    return texts.enumerated().map { idx, text in
        TranscriptChunk(
            id: "c\(idx)-\(assetId)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(idx)",
            chunkIndex: idx,
            startTime: Double(idx) * 30,
            endTime: Double(idx + 1) * 30,
            text: text,
            normalizedText: text.lowercased(),
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }
}

private func makeReconcileCleanChunks(assetId: String) -> [TranscriptChunk] {
    let texts = [
        "Welcome to the show. Today we discuss science.",
        "Here is the main topic of today's episode about physics.",
        "Thank you for listening. See you next time."
    ]
    return texts.enumerated().map { idx, text in
        TranscriptChunk(
            id: "c\(idx)-\(assetId)",
            analysisAssetId: assetId,
            segmentFingerprint: "fp-\(idx)",
            chunkIndex: idx,
            startTime: Double(idx) * 30,
            endTime: Double(idx + 1) * 30,
            text: text,
            normalizedText: text.lowercased(),
            pass: "final",
            modelVersion: "test-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
    }
}

private func makeReconcileService(
    store: AnalysisStore,
    orchestrator: SkipOrchestrator? = nil,
    detectorVersion: String = kReconcileDetectorVersion
) -> AdDetectionService {
    let config = AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: detectorVersion,
        fmBackfillMode: .off
    )
    return AdDetectionService(
        store: store,
        classifier: RuleBasedClassifier(),
        metadataExtractor: FallbackExtractor(),
        config: config,
        skipOrchestrator: orchestrator
    )
}

/// A fully-formed `AdWindow` row with a caller-chosen id / decision / boundary
/// state and (by default) the reconcile detector version, so tests can seed
/// rows that the reconcile predicate classifies deterministically.
private func makeReconcileWindow(
    id: String,
    assetId: String,
    startTime: Double = 60,
    endTime: Double = 120,
    confidence: Double = 0.85,
    boundaryState: String = "lexical",
    decisionState: String,
    detectorVersion: String = kReconcileDetectorVersion,
    metadataSource: String = "none",
    eligibilityGate: String? = nil
) -> AdWindow {
    AdWindow(
        id: id,
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime,
        confidence: confidence,
        boundaryState: boundaryState,
        decisionState: decisionState,
        detectorVersion: detectorVersion,
        advertiser: nil,
        product: nil,
        adDescription: nil,
        evidenceText: nil,
        evidenceStartTime: startTime,
        metadataSource: metadataSource,
        metadataConfidence: nil,
        metadataPromptVersion: nil,
        wasSkipped: false,
        userDismissedBanner: false,
        eligibilityGate: eligibilityGate
    )
}

// MARK: - T1–T5, T13: reconcile persistence

@Suite("playhead-ud4n — backfill reconcile persistence")
struct BackfillReconcilePersistenceTests {

    // T1: a reconcilable hot false-positive absent from the backfill output is
    // hard-deleted from ad_windows.
    @Test("T1: retire hot false-positive — reconcilable hot row deleted on clean backfill")
    func t1RetireFalsePositive() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ud4n-t1"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        let hot = makeReconcileWindow(id: "hot-fp-t1", assetId: assetId, decisionState: "candidate")
        try await store.insertAdWindow(hot)
        #expect(try await store.fetchAdWindows(assetId: assetId).contains { $0.id == "hot-fp-t1" })

        let service = makeReconcileService(store: store)
        try await service.runBackfill(
            chunks: makeReconcileCleanChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let after = try await store.fetchAdWindows(assetId: assetId)
        #expect(!after.contains { $0.id == "hot-fp-t1" },
                "clean backfill must retire the superseded hot candidate")
    }

    // T3: a pre-existing reconcilable hot candidate over the same region is
    // retired and replaced by a single content-addressed fusion window carrying
    // the fusion fields (clean retire + mint-new, Design B / Q5).
    @Test("T3: replace matched — hot row retired, one fusion window with fusion fields")
    func t3ReplaceMatched() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ud4n-t3"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        let hot = makeReconcileWindow(
            id: "hot-match-t3", assetId: assetId,
            startTime: 30, endTime: 60, decisionState: "candidate"
        )
        try await store.insertAdWindow(hot)

        let service = makeReconcileService(store: store)
        try await service.runBackfill(
            chunks: makeReconcileAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let after = try await store.fetchAdWindows(assetId: assetId)
        #expect(!after.contains { $0.id == "hot-match-t3" }, "the hot candidate must be retired")

        let fusion = after.filter { $0.id.hasPrefix("fusion-") }
        #expect(!fusion.isEmpty, "ad-signal backfill must mint at least one content-addressed fusion window")
        for window in fusion {
            // boundaryState is stamped by buildFusionAdWindow and survives the
            // Step-15 metadata update (which only rewrites advertiser/product/
            // evidence/metadataSource). The `fusion-` id prefix is the
            // content-address proof.
            #expect(window.boundaryState == AdBoundaryState.acousticRefined.rawValue)
        }
        // "One active window": every current-detector reconcilable row is now a
        // fusion window — no stale hot UUID row lingering beside it.
        let reconcilable = after.filter {
            AdDetectionService.isReconcilableBackfillWindow($0, detectorVersion: kReconcileDetectorVersion)
        }
        #expect(reconcilable.allSatisfy { $0.id.hasPrefix("fusion-") })
    }

    // T4: no pre-existing reconcilable rows ⇒ pure add, empty retire set.
    @Test("T4: add new — reconcile returns empty retiredIDs when nothing to supersede")
    func t4AddNew() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ud4n-t4"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        let service = makeReconcileService(store: store)
        let freshFusion = makeReconcileWindow(
            id: "fusion-fake-t4", assetId: assetId,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "confirmed", metadataSource: "fusion-v1"
        )
        let result = try await service.reconcileBackfillWindows([freshFusion], analysisAssetId: assetId)
        #expect(result.retiredIDs.isEmpty, "no existing reconcilable rows ⇒ nothing retired")
        #expect(result.windows.map(\.id) == ["fusion-fake-t4"], "fusion windows are returned unchanged")
    }

    // T5: content-addressed ids make reruns idempotent — same ids, no new rows,
    // no churn, empty retire set on the second reconcile.
    @Test("T5: idempotency — rerun yields stable ids, no duplicate rows, empty retiredIDs")
    func t5Idempotency() async throws {
        // Direct determinism of the id factory.
        let a = BackfillJobRunner.makeFusionWindowId(
            analysisAssetId: "asset-x", detectorVersion: "v1", spanStartOrdinal: 3, spanEndOrdinal: 9
        )
        let b = BackfillJobRunner.makeFusionWindowId(
            analysisAssetId: "asset-x", detectorVersion: "v1", spanStartOrdinal: 3, spanEndOrdinal: 9
        )
        #expect(a == b, "identical inputs must yield an identical fusion id")
        #expect(a.hasPrefix("fusion-"))
        let different = BackfillJobRunner.makeFusionWindowId(
            analysisAssetId: "asset-x", detectorVersion: "v1", spanStartOrdinal: 3, spanEndOrdinal: 10
        )
        #expect(a != different, "different ordinals must yield different ids")

        let store = try await makeTestStore()
        let assetId = "asset-ud4n-t5"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))
        let service = makeReconcileService(store: store)
        let chunks = makeReconcileAdChunks(assetId: assetId)

        try await service.runBackfill(
            chunks: chunks, analysisAssetId: assetId,
            podcastId: "podcast-test", episodeDuration: 90.0
        )
        let firstIDs = Set(try await store.fetchAdWindows(assetId: assetId).map(\.id))
        #expect(!firstIDs.isEmpty)

        // Second run over identical input.
        try await service.runBackfill(
            chunks: chunks, analysisAssetId: assetId,
            podcastId: "podcast-test", episodeDuration: 90.0
        )
        let secondRows = try await store.fetchAdWindows(assetId: assetId)
        let secondIDs = Set(secondRows.map(\.id))
        #expect(secondIDs == firstIDs, "rerun must produce byte-stable ids (no new rows)")
        #expect(secondRows.count == firstIDs.count, "rerun must not duplicate rows")

        // A third reconcile over the same output retires nothing (no churn).
        let refetched = secondRows.filter { $0.id.hasPrefix("fusion-") }
        let reconcile = try await service.reconcileBackfillWindows(refetched, analysisAssetId: assetId)
        #expect(reconcile.retiredIDs.isEmpty, "reconciling backfill's own prior output must retire nothing")
    }

    // T13: a stale reconcilable suppressed row absent from the new output is
    // reconciled away; the fresh confirmed fusion output remains.
    @Test("T13: suppressed rerun flip — stale suppressed row retired, fusion output remains")
    func t13SuppressedFlip() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ud4n-t13"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        let stale = makeReconcileWindow(
            id: "stale-suppressed-t13", assetId: assetId,
            startTime: 200, endTime: 230,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "suppressed", metadataSource: "fusion-v1"
        )
        try await store.insertAdWindow(stale)

        let service = makeReconcileService(store: store)
        try await service.runBackfill(
            chunks: makeReconcileAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 240.0
        )

        let after = try await store.fetchAdWindows(assetId: assetId)
        #expect(!after.contains { $0.id == "stale-suppressed-t13" },
                "a reconcilable suppressed row absent from the new output must be retired")
        #expect(after.contains { $0.id.hasPrefix("fusion-") },
                "the fresh fusion output must remain")
    }
}

// MARK: - T6–T10 + predicate: preservation

@Suite("playhead-ud4n — reconcile preserves protected rows")
struct BackfillReconcilePreservationTests {

    private func seedAndCleanBackfill(
        assetId: String,
        seed: AdWindow
    ) async throws -> [AdWindow] {
        let store = try await makeTestStore()
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))
        try await store.insertAdWindow(seed)
        let service = makeReconcileService(store: store)
        try await service.runBackfill(
            chunks: makeReconcileCleanChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )
        return try await store.fetchAdWindows(assetId: assetId)
    }

    // T6: terminal .applied history is never retired.
    @Test("T6: .applied preserved across a clean backfill")
    func t6AppliedPreserved() async throws {
        let seed = makeReconcileWindow(id: "applied-t6", assetId: "asset-ud4n-t6", decisionState: "applied")
        let after = try await seedAndCleanBackfill(assetId: "asset-ud4n-t6", seed: seed)
        #expect(after.contains { $0.id == "applied-t6" })
        #expect(!AdDetectionService.isReconcilableBackfillWindow(seed, detectorVersion: kReconcileDetectorVersion))
    }

    // T7: terminal .reverted history is never retired.
    @Test("T7: .reverted preserved across a clean backfill")
    func t7RevertedPreserved() async throws {
        let seed = makeReconcileWindow(id: "reverted-t7", assetId: "asset-ud4n-t7", decisionState: "reverted")
        let after = try await seedAndCleanBackfill(assetId: "asset-ud4n-t7", seed: seed)
        #expect(after.contains { $0.id == "reverted-t7" })
        #expect(!AdDetectionService.isReconcilableBackfillWindow(seed, detectorVersion: kReconcileDetectorVersion))
    }

    // T8: correction-replay rows (user-correction-backed) are never retired.
    @Test("T8: correction-replay row preserved across a clean backfill")
    func t8CorrectionReplayPreserved() async throws {
        let seed = makeReconcileWindow(
            id: "replay-t8", assetId: "asset-ud4n-t8",
            boundaryState: "correctionReplay", decisionState: "candidate"
        )
        let after = try await seedAndCleanBackfill(assetId: "asset-ud4n-t8", seed: seed)
        #expect(after.contains { $0.id == "replay-t8" })
        #expect(!AdDetectionService.isReconcilableBackfillWindow(seed, detectorVersion: kReconcileDetectorVersion))
    }

    // T9: imported cross-user share rows ("shared-" ids) are never retired.
    @Test("T9: imported share preserved across a clean backfill")
    func t9ImportedSharePreserved() async throws {
        let seed = makeReconcileWindow(
            id: "shared-abc123-t9", assetId: "asset-ud4n-t9", decisionState: "confirmed"
        )
        let after = try await seedAndCleanBackfill(assetId: "asset-ud4n-t9", seed: seed)
        #expect(after.contains { $0.id == "shared-abc123-t9" })
        #expect(!AdDetectionService.isReconcilableBackfillWindow(seed, detectorVersion: kReconcileDetectorVersion))
    }

    // T10: rows from a different detector version are out of scope and untouched.
    @Test("T10: other-detectorVersion row untouched across a clean backfill")
    func t10OtherDetectorVersionUntouched() async throws {
        let seed = makeReconcileWindow(
            id: "otherver-t10", assetId: "asset-ud4n-t10",
            decisionState: "candidate", detectorVersion: "other-detector-v99"
        )
        let after = try await seedAndCleanBackfill(assetId: "asset-ud4n-t10", seed: seed)
        #expect(after.contains { $0.id == "otherver-t10" })
        #expect(!AdDetectionService.isReconcilableBackfillWindow(seed, detectorVersion: kReconcileDetectorVersion))
    }

    // Direct predicate unit test: the reconcilable invariant classifies each
    // axis correctly (the correctness backbone, factored `static`).
    @Test("Predicate: isReconcilableBackfillWindow classifies every axis")
    func predicateAxes() async throws {
        let asset = "asset-pred"
        func w(id: String = "w", decision: String = "candidate", boundary: String = "lexical",
               version: String = kReconcileDetectorVersion) -> AdWindow {
            makeReconcileWindow(id: id, assetId: asset, boundaryState: boundary,
                                decisionState: decision, detectorVersion: version)
        }
        // Reconcilable states.
        #expect(AdDetectionService.isReconcilableBackfillWindow(w(decision: "candidate"), detectorVersion: kReconcileDetectorVersion))
        #expect(AdDetectionService.isReconcilableBackfillWindow(w(decision: "confirmed"), detectorVersion: kReconcileDetectorVersion))
        #expect(AdDetectionService.isReconcilableBackfillWindow(w(decision: "suppressed"), detectorVersion: kReconcileDetectorVersion))
        // Terminal states excluded.
        #expect(!AdDetectionService.isReconcilableBackfillWindow(w(decision: "applied"), detectorVersion: kReconcileDetectorVersion))
        #expect(!AdDetectionService.isReconcilableBackfillWindow(w(decision: "reverted"), detectorVersion: kReconcileDetectorVersion))
        // Protected boundary states excluded.
        #expect(!AdDetectionService.isReconcilableBackfillWindow(w(boundary: "correctionReplay"), detectorVersion: kReconcileDetectorVersion))
        #expect(!AdDetectionService.isReconcilableBackfillWindow(w(boundary: "userMarked"), detectorVersion: kReconcileDetectorVersion))
        #expect(!AdDetectionService.isReconcilableBackfillWindow(w(boundary: "userConfirmedSuggested"), detectorVersion: kReconcileDetectorVersion))
        // Imported share excluded.
        #expect(!AdDetectionService.isReconcilableBackfillWindow(w(id: "shared-xyz"), detectorVersion: kReconcileDetectorVersion))
        // Other detector version excluded.
        #expect(!AdDetectionService.isReconcilableBackfillWindow(w(version: "other-v"), detectorVersion: kReconcileDetectorVersion))
    }

    // Terminal-collision guard: content-addressed ids are ordinal-keyed, so a
    // re-detected already-applied span mints the SAME fusion id. The reconcile
    // must NOT hand that colliding window back for INSERT-OR-REPLACE — the
    // persisted terminal row wins. (Not one of the 13 clean-backfill cases; it
    // hardens T6/T7 against an ad-signal rerun.)
    @Test("Guard: a colliding new window never overwrites a protected terminal row")
    func terminalCollisionGuard() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ud4n-collide"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        // A persisted TERMINAL fusion row.
        let collidingId = "fusion-deadbeefcafe0001"
        let applied = makeReconcileWindow(
            id: collidingId, assetId: assetId,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "applied", metadataSource: "fusion-v1"
        )
        try await store.insertAdWindow(applied)

        // A fresh fusion window that re-detects the same span (same id).
        let redetected = makeReconcileWindow(
            id: collidingId, assetId: assetId,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "confirmed", metadataSource: "fusion-v1"
        )
        let service = makeReconcileService(store: store)
        let result = try await service.reconcileBackfillWindows([redetected], analysisAssetId: assetId)
        #expect(!result.windows.contains { $0.id == collidingId },
                "a window colliding with a protected terminal row must be dropped from persist")
        #expect(!result.retiredIDs.contains(collidingId),
                "the terminal row is not reconcilable, so it is never retired either")
    }
}

// MARK: - T11: atomic rollback

@Suite("playhead-ud4n — reconcile transaction is atomic")
struct BackfillReconcileRollbackTests {

    // T11: a mid-transaction failure (FK violation on a later insert) rolls the
    // whole reconcile back — the earlier successful insert is undone AND the
    // pending delete never lands.
    @Test("T11: mid-transaction error rolls back inserts and the delete atomically")
    func t11RollbackAtomic() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ud4n-t11"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        // A reconcilable row that the reconcile would retire (deleted last).
        let retireTarget = makeReconcileWindow(id: "retire-target-t11", assetId: assetId, decisionState: "candidate")
        try await store.insertAdWindow(retireTarget)

        // First window is valid; second violates the analysisAssetId FK
        // (parent asset does not exist) → the INSERT throws mid-transaction.
        let valid = makeReconcileWindow(
            id: "fusion-valid-t11", assetId: assetId,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "confirmed", metadataSource: "fusion-v1"
        )
        let fkViolating = makeReconcileWindow(
            id: "fusion-bad-t11", assetId: "no-such-asset",
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "confirmed", metadataSource: "fusion-v1"
        )

        var threw = false
        do {
            try await store.reconcileBackfillAdWindows(
                [valid, fkViolating],
                retiredIDs: ["retire-target-t11"]
            )
        } catch {
            threw = true
        }
        #expect(threw, "an FK-violating insert must throw out of the reconcile transaction")

        let after = try await store.fetchAdWindows(assetId: assetId)
        #expect(!after.contains { $0.id == "fusion-valid-t11" },
                "the earlier successful insert must be rolled back")
        #expect(after.contains { $0.id == "retire-target-t11" },
                "the pending delete must be rolled back — the retire target survives")
    }
}

// MARK: - T2, T12: orchestrator delivery

@Suite("playhead-ud4n — retired windows cannot auto-skip")
struct BackfillReconcileOrchestratorTests {

    // T2 (CORE AC): a hot false-positive retired by backfill is gone from the DB
    // and can never be re-armed by the durable beginEpisode preload — even in
    // auto mode with confidence well above the enter threshold, it never fires.
    @Test("T2: retired hot false-positive never auto-skips on the durable preload path")
    func t2RetiredNeverAutoSkips() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ud4n-t2"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        // Durable hot FP: conf 0.85 (≥ preload 0.7), candidate, eligibilityGate
        // nil (auto-skip-eligible), reconcile detector version.
        let hot = makeReconcileWindow(
            id: "hot-fp-t2", assetId: assetId,
            startTime: 60, endTime: 120, confidence: 0.85, decisionState: "candidate"
        )
        try await store.insertAdWindow(hot)

        // Backfill rejects the span (clean chunks) → retires + deletes the row.
        let service = makeReconcileService(store: store)
        try await service.runBackfill(
            chunks: makeReconcileCleanChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )
        #expect(!(try await store.fetchAdWindows(assetId: assetId)).contains { $0.id == "hot-fp-t2" })

        // Fresh launch: a brand-new auto-mode orchestrator preloads from the DB.
        let trustService = try await makeSkipTestTrustService(mode: "auto", trustScore: 0.9, observations: 10)
        let orchestrator = SkipOrchestrator(store: store, trustService: trustService)
        await orchestrator.beginEpisode(analysisAssetId: assetId, episodeId: assetId, podcastId: "podcast-1")

        // The retired FP is not managed, and advancing the playhead through its
        // former span never fires an auto-skip for it.
        #expect(!(await orchestrator.activeWindowIDs()).contains("hot-fp-t2"))
        await orchestrator.updatePlayheadTime(90)
        let log = await orchestrator.getDecisionLog()
        #expect(!log.contains { $0.adWindowId == "hot-fp-t2" && $0.decision == .applied },
                "a backfill-retired hot false-positive must never auto-skip")
    }

    // T12: retire-before-replace ordering. A wired orchestrator that already
    // manages the preloaded hot candidate has it retired from live orchestration
    // by Step 17 before/with the replacement fusion decisions — no stale hot
    // duplicate survives beside the fusion window, so it cannot double-fire.
    @Test("T12: retire-before-replace — stale hot window dropped from live orchestration")
    func t12RetireBeforeReplaceOrdering() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-ud4n-t12"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        let hot = makeReconcileWindow(
            id: "hot-dup-t12", assetId: assetId,
            startTime: 30, endTime: 60, confidence: 0.85, decisionState: "candidate"
        )
        try await store.insertAdWindow(hot)

        // Shadow mode: preloaded candidate is managed (logged) but never applied,
        // so it stays a nonterminal, retire-eligible row.
        let orchestrator = SkipOrchestrator(store: store, trustService: nil)
        let service = makeReconcileService(store: store, orchestrator: orchestrator)
        await orchestrator.beginEpisode(analysisAssetId: assetId, episodeId: assetId, podcastId: nil)
        #expect((await orchestrator.activeWindowIDs()).contains("hot-dup-t12"),
                "precondition: the hot candidate is managed via preload")

        try await service.runBackfill(
            chunks: makeReconcileAdChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-test",
            episodeDuration: 90.0
        )

        let active = await orchestrator.activeWindowIDs()
        #expect(!active.contains("hot-dup-t12"),
                "Step 17 retire-before-replace must drop the stale hot window from live orchestration")
        #expect(active.allSatisfy { $0.hasPrefix("fusion-") },
                "no stale hot UUID window may linger beside the fusion replacement")
        let log = await orchestrator.getDecisionLog()
        #expect(!log.contains { $0.adWindowId == "hot-dup-t12" && $0.decision == .applied },
                "the retired hot window must not have fired an auto-skip")
    }
}

// MARK: - W1–W4: store-level atomic terminal guard (playhead-w17m)

// The ud4n detection-level guard (`reconcileBackfillWindows`) computes its
// `protectedIDs` from a snapshot READ taken OUTSIDE the write transaction. A
// `candidate → .applied` transition that lands BETWEEN that snapshot read and
// the write is invisible to `protectedIDs`; because fusion ids are content-
// addressed (ordinal-keyed, not decisionState-keyed), a rerun of the now-applied
// span mints the SAME `fusion-` id, and the blind INSERT-OR-REPLACE inside
// `reconcileBackfillAdWindows` would clobber the just-applied terminal row back
// to `.confirmed` (losing `wasSkipped`) — corrupting DB history even though the
// in-memory orchestrator `.applied` still holds.
//
// This suite pins the SECOND, atomic backstop: `reconcileBackfillAdWindows`
// itself refuses to overwrite an existing row whose live (in-transaction)
// decisionState is terminal (`.applied` / `.reverted`). Because the guard reads
// live DB state INSIDE the same transaction as the write, it closes the race
// regardless of what the outer snapshot saw. It composes with (does not replace)
// the detection-level guard — belt (drop protected from persist set) and
// suspenders (atomic store backstop).
@Suite("playhead-w17m — reconcile store-level atomic terminal guard")
struct BackfillReconcileTerminalGuardTests {

    /// A terminal row carrying user-facing history: `wasSkipped == true` and a
    /// distinctive advertiser/confidence so a clobber is observable. Built
    /// directly (not via `makeReconcileWindow`, which hardcodes
    /// `wasSkipped: false`).
    private func makeTerminalSeed(
        id: String,
        assetId: String,
        decisionState: String
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: decisionState,
            detectorVersion: kReconcileDetectorVersion,
            advertiser: "TerminalAdvertiser",
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 60,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false
        )
    }

    /// A same-id "re-detection" whose fields differ from the terminal seed so a
    /// clobber (or absence of one) is unambiguous: `.confirmed`,
    /// `wasSkipped` false, different advertiser + confidence.
    private func makeCollidingRedetection(id: String, assetId: String) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: 60,
            endTime: 120,
            confidence: 0.55,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "confirmed",
            detectorVersion: kReconcileDetectorVersion,
            advertiser: "ClobberAdvertiser",
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 60,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    // W1: a persisted `.applied` terminal row is NEVER overwritten by a same-id
    // window in the reconcile write set — the race the outer snapshot missed is
    // closed atomically at the store. decisionState stays `.applied`, wasSkipped
    // stays true, and the colliding window's data does not land.
    @Test("W1: .applied terminal row is not overwritten by a same-id reconcile window")
    func w1AppliedNotOverwritten() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-w17m-w1"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        let collidingId = "fusion-deadbeefw17m0001"
        try await store.insertAdWindow(makeTerminalSeed(id: collidingId, assetId: assetId, decisionState: "applied"))

        // Simulate the raced state: the detection-level snapshot never saw the
        // `.applied` transition, so the colliding window is (wrongly) in the
        // persist set handed to the store.
        try await store.reconcileBackfillAdWindows(
            [makeCollidingRedetection(id: collidingId, assetId: assetId)],
            retiredIDs: []
        )

        let row = try await store.fetchAdWindow(id: collidingId)
        let unwrapped = try #require(row, "the terminal row must still exist")
        #expect(unwrapped.decisionState == "applied",
                "the store-level guard must leave the terminal .applied decisionState intact")
        #expect(unwrapped.wasSkipped == true,
                "wasSkipped history must be preserved (not clobbered to the colliding window's false)")
        #expect(unwrapped.advertiser == "TerminalAdvertiser",
                "no field of the colliding window may overwrite the terminal row")
        #expect(unwrapped.confidence == 0.85)
    }

    // W2: same protection for the other terminal state, `.reverted` (user
    // listened through the ad — history that a rerun must never resurface).
    @Test("W2: .reverted terminal row is not overwritten by a same-id reconcile window")
    func w2RevertedNotOverwritten() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-w17m-w2"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        let collidingId = "fusion-deadbeefw17m0002"
        try await store.insertAdWindow(makeTerminalSeed(id: collidingId, assetId: assetId, decisionState: "reverted"))

        try await store.reconcileBackfillAdWindows(
            [makeCollidingRedetection(id: collidingId, assetId: assetId)],
            retiredIDs: []
        )

        let row = try await store.fetchAdWindow(id: collidingId)
        let unwrapped = try #require(row, "the terminal row must still exist")
        #expect(unwrapped.decisionState == "reverted",
                "the store-level guard must leave the terminal .reverted decisionState intact")
        #expect(unwrapped.wasSkipped == true)
        #expect(unwrapped.advertiser == "TerminalAdvertiser")
    }

    // W3: the guard is surgical, not blanket. A NON-terminal existing row
    // (candidate / confirmed / suppressed) with a colliding id is still
    // INSERT-OR-REPLACE'd — the normal idempotent reconcile path is preserved.
    @Test("W3: non-terminal rows (candidate/confirmed/suppressed) are still replaced on collision")
    func w3NonTerminalStillReplaced() async throws {
        for nonTerminal in ["candidate", "confirmed", "suppressed"] {
            let store = try await makeTestStore()
            let assetId = "asset-w17m-w3-\(nonTerminal)"
            try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

            let collidingId = "fusion-w17m-w3-\(nonTerminal)"
            // Seed carries wasSkipped=true + a distinctive advertiser so the
            // replace is observable. (makeTerminalSeed sets decisionState via the
            // param, so this is a non-terminal row here despite the helper name.)
            let seed = makeTerminalSeed(id: collidingId, assetId: assetId, decisionState: nonTerminal)
            try await store.insertAdWindow(seed)

            try await store.reconcileBackfillAdWindows(
                [makeCollidingRedetection(id: collidingId, assetId: assetId)],
                retiredIDs: []
            )

            let row = try await store.fetchAdWindow(id: collidingId)
            let unwrapped = try #require(row, "the non-terminal row must still exist (replaced, not deleted)")
            #expect(unwrapped.decisionState == "confirmed",
                    "a non-terminal \(nonTerminal) row must be replaced by the colliding window")
            #expect(unwrapped.wasSkipped == false,
                    "the replace must overwrite wasSkipped to the colliding window's value")
            #expect(unwrapped.advertiser == "ClobberAdvertiser",
                    "the colliding window's fields must land on a non-terminal replace")
            #expect(unwrapped.confidence == 0.55)
        }
    }

    // W4: the guard is per-row and does not abort the rest of the reconcile. In
    // one call: a colliding terminal row is preserved, a fresh new window is
    // inserted, and a retiredID is still deleted — the transaction otherwise
    // succeeds normally.
    @Test("W4: terminal guard is per-row — new inserts and retires in the same batch still land")
    func w4GuardIsPerRowNonBlocking() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-w17m-w4"
        try await store.insertAsset(makeSkipTestAnalysisAsset(id: assetId))

        let terminalId = "fusion-w17m-w4-applied"
        try await store.insertAdWindow(makeTerminalSeed(id: terminalId, assetId: assetId, decisionState: "applied"))

        // A reconcilable row that this reconcile retires.
        let retireId = "fusion-w17m-w4-retire"
        try await store.insertAdWindow(
            makeReconcileWindow(id: retireId, assetId: assetId, decisionState: "candidate")
        )

        let freshId = "fusion-w17m-w4-fresh"
        let fresh = makeReconcileWindow(
            id: freshId, assetId: assetId,
            startTime: 200, endTime: 230,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: "confirmed", metadataSource: "fusion-v1"
        )

        try await store.reconcileBackfillAdWindows(
            [makeCollidingRedetection(id: terminalId, assetId: assetId), fresh],
            retiredIDs: [retireId]
        )

        let after = try await store.fetchAdWindows(assetId: assetId)
        // Terminal row preserved.
        let terminal = try #require(after.first { $0.id == terminalId })
        #expect(terminal.decisionState == "applied")
        #expect(terminal.wasSkipped == true)
        // Fresh window inserted.
        #expect(after.contains { $0.id == freshId && $0.decisionState == "confirmed" },
                "a fresh (non-colliding) window in the same batch must still be inserted")
        // Retired window deleted.
        #expect(!after.contains { $0.id == retireId },
                "the retiredID must still be deleted — the guard does not abort the transaction")
    }
}
