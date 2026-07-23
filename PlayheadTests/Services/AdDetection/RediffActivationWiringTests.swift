// RediffActivationWiringTests.swift
// playhead-xsdz.36: end-to-end coverage for the ACTIVATION wiring — the full
// production loop `RediffRefetchService` sweep → rotation → B-copy handoff
// (`RevalidatingRediffBSideConsumer` stage → `revalidateFromFeatures` →
// unstage) → `computeRediffSlotPass` byte differ → persisted `.rediffSlot`
// width marks → B-copy DELETED — plus the OFF/empty byte-identity claims:
//
//   • activation switch pinned ON (the xsdz.36 mark-only ship state);
//   • an injected-but-EMPTY staging provider (every ordinary analysis run
//     under activation) produces spans identical to the no-provider run;
//   • the A-side capture branch in `AnalysisJobRunner` is opt-in per
//     instance: default constructions persist nothing (byte-identity for
//     every existing call site), the activation flag persists the exact
//     extractor stream, and the duration cap skips over-long episodes.
//
// Synthetic A/B MP3 fixtures mirror `RediffByteFirstEndToEndTests`.

import Foundation
import Testing

@testable import Playhead

@Suite("Rediff activation wiring end-to-end (playhead-xsdz.36)")
struct RediffActivationWiringTests {

    typealias Policy = RediffRefetchPolicy
    static let day = Policy.Configuration.secondsPerDay

    // MARK: - Fixtures (mirroring RediffByteFirstEndToEndTests)

    private func chunks(assetId: String) -> [TranscriptChunk] {
        let specs: [(Double, Double, String)] = [
            (0, 100, "Welcome to the show. We talk at length about science and history here."),
            (100, 160, "This segment is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Build your website today."),
            (160, 280, "Back to the conversation about the future and what comes next for all of us.")
        ]
        return specs.enumerated().map { idx, s in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)", analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)", chunkIndex: idx,
                startTime: s.0, endTime: s.1, text: s.2,
                normalizedText: s.2.lowercased(), pass: "final",
                modelVersion: "test-v1", transcriptVersion: nil, atomOrdinal: nil
            )
        }
    }

    /// A/B synthetic MP3 pair: A carries an ID3-separated ad block over
    /// [~95, ~165] s; B is the same content without it (same construction as
    /// the byte-first e2e suite).
    private struct BytePair {
        let aURL: URL
        let bURL: URL
        static func stage(in directory: URL) throws -> BytePair {
            let adStartFrame = 3637   // ≈ 95.008 s
            let adFrames = 2680       // ≈ 70.008 s
            let contentFrames = 10719 // ≈ 280.0 s of played (A) audio
            let c1 = SyntheticMP3.frames(count: adStartFrame, seed: 0xC0FFEE)
            let c2 = SyntheticMP3.frames(count: contentFrames - adStartFrame - adFrames, seed: 0xFACADE)
            let ad = SyntheticMP3.frames(count: adFrames, seed: 0xAD_B10C)
            let aData = SyntheticMP3.file(c1 + [SyntheticMP3.id3v2(payloadBytes: 32)] + ad + c2)
            let bData = SyntheticMP3.file(c1 + c2)
            let aURL = directory.appendingPathComponent("act-a.mp3", isDirectory: false)
            let bURL = directory.appendingPathComponent("act-b.fresh.mp3", isDirectory: false)
            try aData.write(to: aURL)
            try bData.write(to: bURL)
            return BytePair(aURL: aURL, bURL: bURL)
        }
    }

    private final class StubDecoder: AudioFileDecoding, @unchecked Sendable {
        func decodeMono16kHz(fileURL: URL) async throws -> [Float] { [] }
    }

    private func makeService(store: AnalysisStore, provider: RediffBSideProvider?) -> AdDetectionService {
        let config = AdDetectionConfig(
            candidateThreshold: 0.40, confirmationThreshold: 0.70,
            suppressionThreshold: 0.25, hotPathLookahead: 90.0,
            detectorVersion: "test-detection-v1", fmBackfillMode: .off,
            rediffSlotOwnershipEnabled: true
        )
        return AdDetectionService(
            store: store, classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(), config: config,
            rediffBSideProvider: provider
        )
    }

    private func insertActivationAsset(
        store: AnalysisStore,
        assetId: String,
        sourceURL: String,
        duration: Double = 280
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: "ep-\(assetId)", assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil, sourceURL: sourceURL,
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil,
            episodeDurationSec: duration
        ))
    }

    // MARK: - Ship-state pins

    @Test("the single activation switch is ON (xsdz.36 mark-only rung) and the legacy compile-time flag stays OFF")
    func activationSwitchPins() {
        #expect(RediffActivation.isEnabledByDefault == true)
        // The pinned xsdz.27 constant is NOT the activation vehicle — the
        // runner's injected flag is (EpisodeFingerprintCaptureTests pins the
        // constant itself).
        #expect(EpisodeFingerprintCapture.captureEnabledByDefault == false)
        #expect(RediffActivation.maxASideCaptureDurationSeconds == 3 * 60 * 60)
    }

    // MARK: - The full ON loop

    @Test("sweep → rotation → staged B → revalidate → byte-exact .rediffSlot marks persisted; B deleted; staging empty")
    func fullActivationLoopProducesWidthMarks() async throws {
        let assetId = "asset-activation"
        let dir = try makeTempDir(prefix: "RediffActivation-\(assetId)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try BytePair.stage(in: dir)

        // The "downloaded fresh copy" the stub fetcher hands the service —
        // a distinct file so the sweep's deletion is observable.
        let downloadedB = dir.appendingPathComponent("downloaded-b.mp3")
        try FileManager.default.copyItem(at: pair.bURL, to: downloadedB)

        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: pair.aURL.absoluteString)
        try await store.insertTranscriptChunks(chunks(assetId: assetId))

        let staging = RediffBSideStagingProvider(decoder: StubDecoder(), durationProbe: { _ in nil })
        let adService = makeService(store: store, provider: staging)
        let consumer = RevalidatingRediffBSideConsumer(staging: staging, store: store, adDetection: adService)

        // Refetch service with production-shaped stubs: rotated pre-check,
        // stub full-fetch serving the downloaded copy, spy recorder.
        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(
            fingerprint: Policy.sampleFingerprint(head: Data("fresh".utf8), tail: Data("fresh".utf8), totalLength: 2),
            bytesTransferred: 131_072
        )
        let local = StubLocalSampler()
        local.defaultFingerprint = Policy.sampleFingerprint(head: Data("played".utf8), tail: Data("played".utf8), totalLength: 1)
        let full = StubFullFetcher()
        full.fileToReturn = downloadedB
        full.byteCount = 54_000_000
        let recorder = SpyRefetchRecorder()
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = [RediffRefetchCandidate(
            assetId: assetId,
            enclosureURL: URL(string: "https://cdn.example.com/current.mp3")!,
            downloadedAt: 0,
            localAudioURL: pair.aURL,
            attemptState: .initial
        )]

        let refetch = RediffRefetchService(
            enabled: true,
            config: .production,
            enumerator: enumerator,
            rangedSampler: sampler,
            localSampler: local,
            fullFetcher: full,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: FileManagerTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            bsideConsumer: consumer,
            now: { 100 * Self.day }
        )

        let summary = await refetch.runRefetchSweep()

        // Sweep bookkeeping: one rotation, resolved state, bytes accounted.
        #expect(summary.rotatedCount == 1)
        #expect(summary.fullFetchBytes == 54_000_000)
        guard case let .rotated(_, cost, fingerprintCount, newState) = recorder.outcomes.first else {
            Issue.record("expected .rotated, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(cost.fullFetchBytes == 54_000_000)
        #expect(fingerprintCount == 0, "consumer path skips the standalone fingerprint")
        #expect(newState.resolved, "consumed rotation is terminal")

        // The actual product outcome: byte-exact .rediffSlot width marks.
        let spans = try await store.fetchDecodedSpans(assetId: assetId)
        let rediffOwned = spans.filter { $0.anchorProvenance.contains(.rediffSlot) }
        #expect(rediffOwned.count == 1, "exactly the ad span is rediff-width-owned, got \(spans.map(\.anchorProvenance))")
        if let span = rediffOwned.first {
            #expect(span.startTime >= 94.5 && span.startTime <= 95.5, "start ≈ 95, got \(span.startTime)")
            #expect(span.endTime >= 164.5 && span.endTime <= 165.5, "end ≈ 165, got \(span.endTime)")
        }

        // Hygiene: the fetched copy is gone; no stale staging mapping.
        #expect(!FileManager.default.fileExists(atPath: downloadedB.path), "B-copy must be deleted after consumption")
        #expect(await staging.stagedCount == 0)
    }

    /// Build the K day-0 B-copies (real files on disk) + the refetch service
    /// wired for the byte-exact DAY-0 MINT path (`dayZeroMinter`), with pre-check
    /// samplers rigged to THROW if ever consulted (day-0 bypasses the pre-check).
    private func makeDayZeroMintService(
        store: AnalysisStore,
        bFiles: [URL],
        recorder: SpyRefetchRecorder
    ) -> RediffRefetchService {
        let adService = makeService(
            store: store,
            provider: RediffBSideStagingProvider(decoder: StubDecoder(), durationProbe: { _ in nil })
        )
        let sampler = StubRangedSampler()
        sampler.errorToThrow = NSError(domain: "precheck-must-not-run", code: 1)
        let local = StubLocalSampler()
        local.errorToThrow = NSError(domain: "precheck-must-not-run", code: 2)
        return RediffRefetchService(
            enabled: true,
            config: .production,
            enumerator: StubRefetchEnumerator(),
            rangedSampler: sampler,
            localSampler: local,
            fullFetcher: RealFilesKWayFetcher(files: bFiles),
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: FileManagerTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            dayZeroMinter: AdDetectionDayZeroByteExactMinter(adDetection: adService),
            now: { 100 * Self.day }
        )
    }

    private static let day0Enclosure = URL(string: "https://cdn.example.com/current.mp3")!

    @Test("DAY-0 (xsdz.36.4): a TRUE first listen (NO persisted chunks/analysis) mints byte-exact MARK-ONLY AdWindow banners from ≥2-persona-robust divergence; .dayZeroMarked; B deleted")
    func dayZeroFirstListenMintsByteExactMarks() async throws {
        let assetId = "asset-day0-firstlisten"
        let dir = try makeTempDir(prefix: "RediffDay0FL-\(assetId)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try BytePair.stage(in: dir)

        // TWO real B copies (K=2, distinct personas) — both diverge from A
        // identically → the region is ≥2-persona-robust (the mint quorum).
        let b0 = dir.appendingPathComponent("dl-b0.mp3")
        let b1 = dir.appendingPathComponent("dl-b1.mp3")
        try FileManager.default.copyItem(at: pair.bURL, to: b0)
        try FileManager.default.copyItem(at: pair.bURL, to: b1)

        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: pair.aURL.absoluteString)
        // NO transcript chunks — a TRUE first listen (the case the checkpoint
        // failed: `revalidateFromFeatures` early-returns on empty chunks).

        let recorder = SpyRefetchRecorder()
        let refetch = makeDayZeroMintService(store: store, bFiles: [b0, b1], recorder: recorder)
        let candidate = RediffRefetchCandidate(
            assetId: assetId, enclosureURL: Self.day0Enclosure,
            downloadedAt: 0, localAudioURL: pair.aURL, attemptState: .initial
        )
        let summary = await refetch.runDayZeroRefetch(for: candidate, kWayFetchCount: 2)

        // A mark was produced → summary + poisoning-safe RESOLVED state.
        #expect(summary.rotatedCount == 1)
        #expect(summary.fullFetchBytes == 2 * 54_000_000)
        guard case let .dayZeroMarked(_, cost, markCount, newState) = recorder.outcomes.first else {
            Issue.record("expected .dayZeroMarked, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(markCount == 1)
        #expect(cost.precheckBytes == 0, "day-0 spends no pre-check bytes")
        #expect(newState.resolved)

        // The product outcome: a byte-exact MARK-ONLY AdWindow banner — WITHOUT
        // any persisted transcript/analysis (the byte-exact slot is its OWN
        // presence core). NOT a decoded span, NOT auto-skip.
        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows.count == 1, "exactly the byte-exact ad slot is marked, got \(windows.map { ($0.startTime, $0.endTime) })")
        if let w = windows.first {
            #expect(w.eligibilityGate == SkipEligibilityGate.markOnly.rawValue, "mark-only banner, never auto-skip")
            #expect(w.confidence == 1.0, "deterministic byte-exact certainty")
            #expect(w.startTime >= 94.5 && w.startTime <= 95.5, "byte-exact start ≈ 95, got \(w.startTime)")
            #expect(w.endTime >= 164.5 && w.endTime <= 165.5, "byte-exact end ≈ 165, got \(w.endTime)")
        }
        // No analysis ran → no decoded spans (the marks are AdWindows only).
        #expect(try await store.fetchDecodedSpans(assetId: assetId).isEmpty,
                "no transcript/analysis ran ⇒ no decoded spans")
        // Never-persist-B: both copies deleted on exit.
        #expect(!FileManager.default.fileExists(atPath: b0.path))
        #expect(!FileManager.default.fileExists(atPath: b1.path))
    }

    @Test("DAY-0 narrowness: a SINGLE byte-exact divergent B (no ≥2-persona quorum) mints NOTHING → .dayZeroUnmarked (poisoning-safe: no resolve, asset stays a lagged candidate)")
    func dayZeroSinglePersonaMintsNothing() async throws {
        let assetId = "asset-day0-single"
        let dir = try makeTempDir(prefix: "RediffDay0Single-\(assetId)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try BytePair.stage(in: dir)
        let b0 = dir.appendingPathComponent("dl-b0.mp3")
        try FileManager.default.copyItem(at: pair.bURL, to: b0)

        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: pair.aURL.absoluteString)

        let recorder = SpyRefetchRecorder()
        let refetch = makeDayZeroMintService(store: store, bFiles: [b0], recorder: recorder)
        let candidate = RediffRefetchCandidate(
            assetId: assetId, enclosureURL: Self.day0Enclosure,
            downloadedAt: 0, localAudioURL: pair.aURL, attemptState: .initial
        )
        // K=1 — a lone divergence cannot reach the ≥2-persona robustness quorum.
        let summary = await refetch.runDayZeroRefetch(for: candidate, kWayFetchCount: 1)

        #expect(summary.rotatedCount == 0, "a single-persona diff mints nothing")
        guard case let .dayZeroUnmarked(_, cost, _) = recorder.outcomes.first else {
            Issue.record("expected .dayZeroUnmarked, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(cost.fullFetchBytes == 54_000_000, "bytes still accounted")
        #expect(try await store.fetchAdWindows(assetId: assetId).isEmpty, "no marks minted")
        // The MINT path itself never touches `rediff_refetch_state` (state is the
        // recorder's job — the store-level poisoning contract is pinned by
        // `RediffRefetchRecorderTests.dayZeroOutcomesPoisoningSafe`). Here the
        // `.dayZeroUnmarked` outcome above is the poisoning-safe signal.
        #expect(try await store.fetchRediffRefetchStates().isEmpty,
                "the day-0 mint writes only AdWindows, never rediff_refetch_state")
        #expect(!FileManager.default.fileExists(atPath: b0.path))
    }

    @Test("DAY-0 byte-EXACT only: ≥2 personas whose diffs FALL BACK to chroma (re-encode / no shared frames) mint NOTHING → .dayZeroUnmarked")
    func dayZeroChromaFallbackMintsNothing() async throws {
        let assetId = "asset-day0-chroma"
        let dir = try makeTempDir(prefix: "RediffDay0Chroma-\(assetId)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try BytePair.stage(in: dir)

        // Two B copies that share NO frames with A (a whole-file re-encode
        // analogue) → the byte gate rejects each (`rejectedNoChainedRuns`), the
        // very "fall back to chroma" trigger — from which day-0 mints NOTHING.
        let reencoded = SyntheticMP3.file(SyntheticMP3.frames(count: 10_719, seed: 0x5EED_5EED))
        let b0 = dir.appendingPathComponent("reenc-b0.mp3")
        let b1 = dir.appendingPathComponent("reenc-b1.mp3")
        try reencoded.write(to: b0)
        try reencoded.write(to: b1)

        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: pair.aURL.absoluteString)

        let recorder = SpyRefetchRecorder()
        let refetch = makeDayZeroMintService(store: store, bFiles: [b0, b1], recorder: recorder)
        let candidate = RediffRefetchCandidate(
            assetId: assetId, enclosureURL: Self.day0Enclosure,
            downloadedAt: 0, localAudioURL: pair.aURL, attemptState: .initial
        )
        let summary = await refetch.runDayZeroRefetch(for: candidate, kWayFetchCount: 2)

        #expect(summary.rotatedCount == 0, "chroma-fallback diffs mint nothing (byte-exact only)")
        guard case .dayZeroUnmarked = recorder.outcomes.first else {
            Issue.record("expected .dayZeroUnmarked, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(try await store.fetchAdWindows(assetId: assetId).isEmpty, "no byte-exact slot ⇒ no marks")
    }

    @Test("DAY-0 idempotency: a robust byte-exact slot already covered by an existing AdWindow is NOT re-marked")
    func dayZeroDoesNotDoubleMarkExistingWindow() async throws {
        let assetId = "asset-day0-idem"
        let dir = try makeTempDir(prefix: "RediffDay0Idem-\(assetId)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try BytePair.stage(in: dir)
        let b0 = dir.appendingPathComponent("dl-b0.mp3")
        let b1 = dir.appendingPathComponent("dl-b1.mp3")
        try FileManager.default.copyItem(at: pair.bURL, to: b0)
        try FileManager.default.copyItem(at: pair.bURL, to: b1)

        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: pair.aURL.absoluteString)
        // Pre-seed an AdWindow already covering the ~[95,165] byte slot.
        try await store.upsertHotPathAdWindows([AdWindow(
            id: "pre-existing", analysisAssetId: assetId, startTime: 90, endTime: 170,
            confidence: 0.7, boundaryState: "userMarked",
            decisionState: AdDecisionState.candidate.rawValue, detectorVersion: "test-detection-v1",
            advertiser: nil, product: nil, adDescription: nil, evidenceText: nil, evidenceStartTime: 90,
            metadataSource: "test", metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false, evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue, catalogStoreMatchSimilarity: nil
        )], existingIDs: [], retiredIDs: [])

        let recorder = SpyRefetchRecorder()
        let refetch = makeDayZeroMintService(store: store, bFiles: [b0, b1], recorder: recorder)
        let candidate = RediffRefetchCandidate(
            assetId: assetId, enclosureURL: Self.day0Enclosure,
            downloadedAt: 0, localAudioURL: pair.aURL, attemptState: .initial
        )
        _ = await refetch.runDayZeroRefetch(for: candidate, kWayFetchCount: 2)

        // No new window (the pre-existing one already covers the slot).
        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows.count == 1, "the covered slot is not re-marked, got \(windows.count) windows")
        #expect(windows.first?.id == "pre-existing")
        guard case .dayZeroUnmarked = recorder.outcomes.first else {
            Issue.record("expected .dayZeroUnmarked (all robust slots already covered), got \(String(describing: recorder.outcomes.first))"); return
        }
    }

    @Test("DAY-0 marks are RECONCILE-PROTECTED: they survive a later analysis run (not a reconcilable backfill window) and are recognized (never abort a cross-user snapshot)")
    func dayZeroMarksAreReconcileProtected() {
        // The literal used at the protection sites must match the constant.
        #expect(AdDetectionService.dayZeroRediffByteExactBoundaryState == "dayZeroRediffByteExact")
        #expect(AdDetectionService.reconcileProtectedBoundaryStates
            .contains(AdDetectionService.dayZeroRediffByteExactBoundaryState),
            "a day-0 byte-exact mark must survive a later backfill's reconciliation")

        // A day-0 window is NOT a reconcilable backfill window — the algorithmic
        // detector's transcript/FM fusion would not re-emit this deterministic
        // byte-exact mark, so retiring it would silently delete a correct mark.
        let dayZeroWindow = AdWindow(
            id: UUID().uuidString, analysisAssetId: "a", startTime: 95, endTime: 165,
            confidence: 1.0, boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: AdDecisionState.candidate.rawValue, detectorVersion: "v1",
            advertiser: nil, product: nil, adDescription: nil, evidenceText: nil, evidenceStartTime: 95,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil, metadataPromptVersion: nil, wasSkipped: false, userDismissedBanner: false,
            evidenceSources: nil, eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil
        )
        #expect(!AdDetectionService.isReconcilableBackfillWindow(dayZeroWindow, detectorVersion: "v1"),
                "day-0 marks are protected from backfill retirement (retirable only by an explicit user veto)")
    }

    @Test("DAY-0 marks do NOT abort a cross-user snapshot: an asset carrying one still exports (the mark is a recognized local-only disposition, itself excluded)")
    func dayZeroMarkDoesNotAbortCrossUserSnapshot() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-day0-share"
        // A valid share fixture: 64-hex assetFingerprint + positive analysisVersion.
        try await store.insertAsset(AnalysisAsset(
            id: assetId, episodeId: "ep-\(assetId)",
            assetFingerprint: String(repeating: "a", count: 64),
            weakFingerprint: nil, sourceURL: "file:///tmp/x.mp3",
            featureCoverageEndTime: nil, fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil, analysisState: "new",
            analysisVersion: 1, capabilitySnapshot: nil, episodeDurationSec: 280
        ))
        // One exportable window + one day-0 byte-exact mark on the same asset.
        try await store.upsertHotPathAdWindows([
            AdWindow(
                id: "exportable", analysisAssetId: assetId, startTime: 10, endTime: 40,
                confidence: 0.9, boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                decisionState: AdDecisionState.confirmed.rawValue, detectorVersion: "test-detection-v1",
                advertiser: nil, product: nil, adDescription: nil, evidenceText: nil, evidenceStartTime: 10,
                metadataSource: "fusion", metadataConfidence: nil, metadataPromptVersion: nil,
                wasSkipped: false, userDismissedBanner: false, evidenceSources: nil,
                eligibilityGate: SkipEligibilityGate.eligible.rawValue, catalogStoreMatchSimilarity: nil
            ),
            AdWindow(
                id: "day0", analysisAssetId: assetId, startTime: 95, endTime: 165,
                confidence: 1.0, boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
                decisionState: AdDecisionState.candidate.rawValue, detectorVersion: "test-detection-v1",
                advertiser: nil, product: nil, adDescription: nil, evidenceText: nil, evidenceStartTime: 95,
                metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
                metadataConfidence: nil, metadataPromptVersion: nil, wasSkipped: false, userDismissedBanner: false,
                evidenceSources: nil, eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
                catalogStoreMatchSimilarity: nil
            )
        ], existingIDs: [], retiredIDs: [])

        let snapshot = try await store.exportCrossUserAnalysisSnapshot(
            assetId: assetId, podcastId: "podcast-share"
        )
        // The day-0 mark is a RECOGNIZED disposition (so the snapshot is NOT
        // aborted by `hasKnownExportDisposition`), but it is local-only (so it is
        // NOT in the exported windows — another user's DAI stitch differs).
        let exported = try #require(snapshot, "an asset with a day-0 mark must still export a snapshot")
        #expect(exported.windows.contains { $0.sourceWindowId == "exportable" })
        #expect(!exported.windows.contains { $0.sourceWindowId == "day0" },
                "the local-only day-0 mark is never exported to other users")
    }

    // Scope: this covers the Download & Analyze GATING (`forceDeepScanOptIn`
    // permits an unplugged WiFi fetch) + the byte-exact MINT given a READY
    // episode (asset + A-side present, no chunks). The runtime step that WAITS
    // for the asset + file to materialize after `prepare()` is pinned separately
    // by `DayZeroPreparationReadinessTests`.
    @Test("DAY-0 Download & Analyze (playhead-3xtw): the tap grants the deep-scan opt-in, so an UNPLUGGED WiFi first-listen mints byte-exact marks with NO persisted analysis")
    func dayZeroDownloadAndAnalyzeUnpluggedFirstListenMintsMarks() async throws {
        let assetId = "asset-day0-dna"
        let dir = try makeTempDir(prefix: "RediffDay0DNA-\(assetId)")
        defer { try? FileManager.default.removeItem(at: dir) }
        let pair = try BytePair.stage(in: dir)
        let b0 = dir.appendingPathComponent("dl-b0.mp3")
        let b1 = dir.appendingPathComponent("dl-b1.mp3")
        try FileManager.default.copyItem(at: pair.bURL, to: b0)
        try FileManager.default.copyItem(at: pair.bURL, to: b1)

        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: pair.aURL.absoluteString)
        // NO transcript chunks — first-listen Download & Analyze.

        let recorder = SpyRefetchRecorder()
        let service = makeDayZeroMintService(store: store, bFiles: [b0, b1], recorder: recorder)
        // The Download & Analyze trigger: UNPLUGGED, settings opt-in OFF — only
        // the tap-as-opt-in (`forceDeepScanOptIn: true`) can permit this. K=2 so
        // the divergence is ≥2-persona-robust.
        let trigger = DayZeroRediffTrigger(
            service: service, enabled: true, kWayFetchCount: 2,
            reachabilityProvider: { .wifi },
            chargeStateProvider: { false },          // unplugged
            deepScanOptInProvider: { false }         // settings opt-in OFF
        )
        let summary = await trigger.triggerIfEligible(
            analysisAssetId: assetId,
            enclosureURL: Self.day0Enclosure,
            playedFileURL: pair.aURL,
            forceDeepScanOptIn: true                 // the explicit tap
        )

        #expect(summary.rotatedCount == 1, "the Download & Analyze tap fires day-0 unplugged on WiFi")
        let windows = try await store.fetchAdWindows(assetId: assetId)
        #expect(windows.count == 1, "byte-exact first-listen mark from the Download & Analyze trigger")
        #expect(windows.first?.eligibilityGate == SkipEligibilityGate.markOnly.rawValue)
    }

    @Test("a consume failure records .failed (no resolve, R2 state advanced), deletes the B-copy, and leaves no marks")
    func consumeFailureIsRetriedNotResolved() async throws {
        let dir = try makeTempDir(prefix: "RediffActivation-fail")
        defer { try? FileManager.default.removeItem(at: dir) }
        let downloadedB = dir.appendingPathComponent("downloaded-b.mp3")
        try Data(repeating: 9, count: 2048).write(to: downloadedB)

        // Store WITHOUT the asset row → consumer throws .assetMissing.
        let store = try await makeTestStore()
        let staging = RediffBSideStagingProvider(decoder: StubDecoder(), durationProbe: { _ in nil })
        let adService = makeService(store: store, provider: staging)
        let consumer = RevalidatingRediffBSideConsumer(staging: staging, store: store, adDetection: adService)

        let sampler = StubRangedSampler()
        sampler.defaultSample = RemoteAudioSample(
            fingerprint: Policy.sampleFingerprint(head: Data("f".utf8), tail: Data("f".utf8), totalLength: 2),
            bytesTransferred: 131_072
        )
        let local = StubLocalSampler()
        local.defaultFingerprint = Policy.sampleFingerprint(head: Data("p".utf8), tail: Data("p".utf8), totalLength: 1)
        let full = StubFullFetcher()
        full.fileToReturn = downloadedB
        full.byteCount = 2_048
        let recorder = SpyRefetchRecorder()
        let enumerator = StubRefetchEnumerator()
        enumerator.candidatesToReturn = [RediffRefetchCandidate(
            assetId: "ghost",
            enclosureURL: URL(string: "https://cdn.example.com/ghost.mp3")!,
            downloadedAt: 0,
            localAudioURL: dir.appendingPathComponent("nonexistent-a.mp3"),
            attemptState: .initial
        )]

        let refetch = RediffRefetchService(
            enabled: true,
            enumerator: enumerator,
            rangedSampler: sampler,
            localSampler: local,
            fullFetcher: full,
            bsideFingerprinter: StubBSideFingerprinter(),
            recorder: recorder,
            fileRemover: FileManagerTempFileRemover(),
            taskScheduler: StubTaskScheduler(),
            bsideConsumer: consumer,
            now: { 100 * Self.day }
        )
        _ = await refetch.runRefetchSweep()

        guard case let .failed(_, cost, failureClass, newState, _) = recorder.outcomes.first else {
            Issue.record("expected .failed, got \(String(describing: recorder.outcomes.first))"); return
        }
        #expect(failureClass == .staleAsset)
        #expect(newState.resolved == false, "a failed consume must stay retryable")
        #expect(newState.sameClassFailureStreak == 1)
        #expect(cost.fullFetchBytes == 2_048, "bytes spent are still accounted")
        #expect(!FileManager.default.fileExists(atPath: downloadedB.path), "B-copy deleted on the failure path too")
        #expect(await staging.stagedCount == 0)
    }

    // MARK: - Byte-identity: provider injected but nothing staged

    @Test("an EMPTY staging provider yields spans identical to the no-provider run (every ordinary backfill under activation)")
    func emptyStagingProviderIsByteIdenticalToNoProvider() async throws {
        func runAndProject(provider: RediffBSideProvider?) async throws -> [String] {
            let assetId = "asset-identity"
            let store = try await makeTestStore()
            try await insertActivationAsset(
                store: store, assetId: assetId,
                sourceURL: "file:///tmp/nonexistent-played.mp3"
            )
            let service = makeService(store: store, provider: provider)
            try await service.runBackfill(
                chunks: chunks(assetId: assetId), analysisAssetId: assetId,
                podcastId: "podcast-identity", episodeDuration: 280.0
            )
            // Project to content fields — row ids are per-run UUIDs.
            return try await store.fetchDecodedSpans(assetId: assetId)
                .map { span in
                    let provenance = span.anchorProvenance
                        .map { String(describing: $0) }
                        .sorted()
                        .joined(separator: ",")
                    return "\(span.startTime)|\(span.endTime)|\(provenance)"
                }
                .sorted()
        }

        let withoutProvider = try await runAndProject(provider: nil)
        let withEmptyStaging = try await runAndProject(
            provider: RediffBSideStagingProvider(decoder: StubDecoder(), durationProbe: { _ in nil })
        )
        #expect(withEmptyStaging == withoutProvider)
        #expect(!withEmptyStaging.contains { $0.contains("rediffSlot") },
                "no staged B-side ⇒ no rediff-owned spans")
    }

    // MARK: - A-side capture wiring in AnalysisJobRunner

    /// A 16 kHz multi-tone shard pair long enough to fingerprint.
    private func toneShards(episodeID: String) -> [AnalysisShard] {
        let samples = (0..<120_000).map { k -> Float in
            let t = Double(k) / 16_000.0
            return Float(0.4 * sin(2 * .pi * 220.0 * t) + 0.3 * sin(2 * .pi * 523.25 * t))
        }
        let mid = samples.count / 2
        return [
            AnalysisShard(id: 0, episodeID: episodeID, startTime: 0, duration: 3.75,
                          samples: Array(samples[0..<mid])),
            AnalysisShard(id: 1, episodeID: episodeID, startTime: 3.75, duration: 3.75,
                          samples: Array(samples[mid...])),
        ]
    }

    private func makeRunner(
        store: AnalysisStore,
        shards: [AnalysisShard],
        rediffASideCaptureEnabled: Bool?
    ) async throws -> AnalysisJobRunner {
        let featureService = FeatureExtractionService(store: store)
        let speechService = SpeechService(recognizer: StubSpeechRecognizer())
        try await speechService.loadFastModel()
        let transcriptEngine = TranscriptEngineService(speechService: speechService, store: store)
        let audioStub = StubAnalysisAudioProvider()
        audioStub.shardsToReturn = shards
        if let rediffASideCaptureEnabled {
            return AnalysisJobRunner(
                store: store,
                audioProvider: audioStub,
                featureService: featureService,
                transcriptEngine: transcriptEngine,
                adDetection: StubAdDetectionProvider(),
                rediffASideCaptureEnabled: rediffASideCaptureEnabled
            )
        }
        // DEFAULT construction — byte-identity for every existing call site.
        return AnalysisJobRunner(
            store: store,
            audioProvider: audioStub,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: StubAdDetectionProvider()
        )
    }

    private func makeRunnerRequest(dir: URL, assetId: String) -> AnalysisRangeRequest {
        let audioFile = dir.appendingPathComponent("episode.m4a")
        FileManager.default.createFile(atPath: audioFile.path, contents: Data())
        return AnalysisRangeRequest(
            jobId: UUID().uuidString,
            episodeId: "ep-\(assetId)",
            podcastId: "test-pod",
            analysisAssetId: assetId,
            audioURL: LocalAudioURL(audioFile)!,
            desiredCoverageSec: 120,
            mode: .preRollWarmup,
            outputPolicy: .writeWindowsAndCues,
            priority: .medium
        )
    }

    @Test("default runner construction captures nothing (byte-identity); the activation flag persists the exact extractor stream")
    func runnerCaptureFlagWiring() async throws {
        // OFF (default): no fingerprint row.
        do {
            let assetId = "asset-cap-off"
            let dir = try makeTempDir(prefix: "RediffActCap-off")
            defer { try? FileManager.default.removeItem(at: dir) }
            let store = try await makeTestStore()
            try await insertActivationAsset(store: store, assetId: assetId, sourceURL: "")
            let shards = toneShards(episodeID: "ep-\(assetId)")
            let runner = try await makeRunner(store: store, shards: shards, rediffASideCaptureEnabled: nil)
            _ = await runner.run(makeRunnerRequest(dir: dir, assetId: assetId))
            #expect(try await store.fetchEpisodeFingerprints(assetId: assetId) == nil)
        }

        // ON: the persisted stream equals the canonical extractor output.
        do {
            let assetId = "asset-cap-on"
            let dir = try makeTempDir(prefix: "RediffActCap-on")
            defer { try? FileManager.default.removeItem(at: dir) }
            let store = try await makeTestStore()
            try await insertActivationAsset(store: store, assetId: assetId, sourceURL: "")
            let shards = toneShards(episodeID: "ep-\(assetId)")
            let runner = try await makeRunner(store: store, shards: shards, rediffASideCaptureEnabled: true)
            _ = await runner.run(makeRunnerRequest(dir: dir, assetId: assetId))
            let record = try #require(try await store.fetchEpisodeFingerprints(assetId: assetId))
            var mono = [Float]()
            for shard in shards { mono.append(contentsOf: shard.samples) }
            #expect(record.fingerprints == EpisodeFingerprintCapture.fingerprints(mono16kHz: mono))
            #expect(record.sourceAudioIdentity == "fp-\(assetId)")
        }

        // ON but over the duration cap: capture skipped. NOTE (R4): with no
        // A-side stream row the episode also drops out of re-fetch candidacy
        // entirely (candidacy = current-version row in `episode_fingerprints`)
        // — see `RediffActivation.maxASideCaptureDurationSeconds`.
        do {
            let assetId = "asset-cap-long"
            let dir = try makeTempDir(prefix: "RediffActCap-long")
            defer { try? FileManager.default.removeItem(at: dir) }
            let store = try await makeTestStore()
            try await insertActivationAsset(store: store, assetId: assetId, sourceURL: "")
            // Duration fields exceed the cap; sample payloads stay tiny.
            var shards = toneShards(episodeID: "ep-\(assetId)")
            shards = shards.map {
                AnalysisShard(id: $0.id, episodeID: $0.episodeID, startTime: $0.startTime,
                              duration: RediffActivation.maxASideCaptureDurationSeconds,
                              samples: $0.samples)
            }
            let runner = try await makeRunner(store: store, shards: shards, rediffASideCaptureEnabled: true)
            _ = await runner.run(makeRunnerRequest(dir: dir, assetId: assetId))
            #expect(try await store.fetchEpisodeFingerprints(assetId: assetId) == nil,
                    "over-cap episodes must skip the chroma A-side capture")
        }
    }

    @Test("recapture guard (R3): a matching-identity stream is not recomputed on later passes (capturedAt stable); a stale identity IS recaptured")
    func runnerSkipsRecaptureForMatchingIdentity() async throws {
        let assetId = "asset-cap-guard"
        let dir = try makeTempDir(prefix: "RediffActCap-guard")
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeTestStore()
        try await insertActivationAsset(store: store, assetId: assetId, sourceURL: "")
        let shards = toneShards(episodeID: "ep-\(assetId)")
        let runner = try await makeRunner(store: store, shards: shards, rediffASideCaptureEnabled: true)
        let request = makeRunnerRequest(dir: dir, assetId: assetId)

        // A pre-existing CURRENT-version stream whose identity does NOT match
        // the asset (a re-downloaded copy under a reused id) must be replaced.
        try await store.upsertEpisodeFingerprints(EpisodeFingerprintRecord(
            analysisAssetId: assetId,
            algorithmVersion: ChromaFingerprinter.algorithmVersion,
            secondsPerFingerprint: ChromaFingerprinter.secondsPerFingerprint,
            fingerprints: [1, 2, 3],
            sourceAudioIdentity: "stale-identity",
            capturedAt: 123
        ))
        _ = await runner.run(request)
        let first = try #require(try await store.fetchEpisodeFingerprints(assetId: assetId))
        #expect(first.sourceAudioIdentity == "fp-\(assetId)", "stale identity must be recaptured")
        #expect(first.capturedAt != 123)
        #expect(first.fingerprints != [1, 2, 3])

        // Second pass over the SAME audio identity: capture skipped — the
        // record (including `capturedAt`, the re-fetch enumerator's
        // downloaded-at baseline) is byte-identical, so repeat passes cannot
        // re-arm the ~3d first-attempt gate or re-spend the extractor walk.
        _ = await runner.run(request)
        let second = try #require(try await store.fetchEpisodeFingerprints(assetId: assetId))
        #expect(second.capturedAt == first.capturedAt, "a repeat pass must not bump capturedAt")
        #expect(second.fingerprints == first.fingerprints)
    }
}
