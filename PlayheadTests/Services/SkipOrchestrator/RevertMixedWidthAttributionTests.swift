// RevertMixedWidthAttributionTests.swift
// playhead-1mq1.2.1 — a reverted mixed-width window must not become a
// whole-span negative label.
//
// THE FIXTURE IS THE BEAD. On THEMOVE the listener reverted the auto window
// 3493.02–3536.90 because its WIDTH was wrong: it opened on the show sign-off
// (through 3498.48), then silence, then a REAL ad ran 3505.74–3536.10, with a
// second ad starting immediately at 3536.40. One tap is the only gesture
// available, so the correction seams read the revert as "the whole window was
// a false positive" and turned ~30s of true ad into negative training
// material.
//
// The two learning surfaces that a revert can poison, and how each is
// observed here:
//
//   • the hard-negative copy bank — `recordListenRevert` banks the window's
//     whole `evidenceText` as a confirmed FP, which on this fixture is the
//     ad's own copy. Observed by reading the bank.
//   • the FUZZY recurrence sweep — `revokeRecurrenceEvidence` fingerprints
//     the window and asks the catalog to revoke every row in the show that
//     resembles it. On a window that is mostly ad, that fingerprint IS the
//     ad's, so it deletes the legitimately learned entry for it. Observed by
//     seeding exactly that entry and asserting it survives.
//
// Both assertions are paired with a CLEAN control in the same shape, because
// "nothing was learned" is trivially satisfiable by a dead write path — the
// controls prove the surfaces are live under this fixture's wiring.

import Foundation
import Testing

@testable import Playhead

// MARK: - Local fixture helpers

/// Counts skip-cue republications so "playback still restores" is an
/// observation rather than an assumption.
private final class MixedWidthCuePushCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var value = 0

    func record() {
        lock.lock()
        value += 1
        lock.unlock()
    }

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}

/// Feature coverage for one span, shaped so
/// `AcousticFingerprint.fromFeatureWindows` accepts it: 2-second windows (a
/// `MusicDetectionConfig.supportedWindowDurations` value), the CURRENT feature
/// version, non-negative finite scalars, no overlap, and values that vary
/// across the span so the derived vector is non-zero.
private func mixedWidthFeatureWindows(
    assetId: String,
    from start: Double,
    to end: Double
) -> [FeatureWindow] {
    var out: [FeatureWindow] = []
    var t = start
    var i = 0
    while t + 2 <= end {
        let phase = Double(i % 5) / 10.0
        out.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: t,
            endTime: t + 2,
            rms: 0.3 + phase,
            spectralFlux: 0.2 + phase,
            musicProbability: 0.1 + phase,
            speakerChangeProxyScore: 0.05 + phase,
            musicBedChangeScore: phase,
            musicBedOnsetScore: phase / 2,
            musicBedOffsetScore: phase / 3,
            musicBedLevel: .none,
            pauseProbability: 0.05 + phase,
            speakerClusterId: 0,
            jingleHash: nil,
            featureVersion: FeatureExtractionConfig.default.featureVersion
        ))
        t += 2
        i += 1
    }
    return out
}

/// The commercial anchors `EvidenceCatalogBuilder` extracts from the HAIKS
/// read: a sponsorship disclosure, a vanity URL, and a promo code, all inside
/// 3505.74–3536.10 and none of them touching the sign-off that precedes it.
private func haiksAdEvidenceCatalog(
    assetId: String
) -> EvidenceCatalog {
    EvidenceCatalog(
        analysisAssetId: assetId,
        transcriptVersion: "v1",
        entries: [
            EvidenceEntry(
                evidenceRef: 0,
                category: .disclosurePhrase,
                matchedText: "this episode is sponsored by",
                normalizedText: "this episode is sponsored by",
                atomOrdinal: 41,
                startTime: 3508.0,
                endTime: 3510.2
            ),
            // Deliberately a REPEATED anchor: the same vanity URL was also read
            // in the pre-roll at 120s. That makes this entry's
            // `coverageStartTime`/`coverageEndTime` hull span nearly the whole
            // episode, and reading the hull instead of the occurrences would
            // mark the entire reverted window as ad evidence — which flips it
            // to CLEAN and banks the ad's copy after all. The hazard runs the
            // other way too on other geometries: a hull clipped into the window
            // can push the evidence-free remainder INTO the real ad.
            EvidenceEntry(
                evidenceRef: 1,
                category: .url,
                matchedText: "haiks dot com slash themove",
                normalizedText: "haiks.com/themove",
                atomOrdinal: 44,
                startTime: 3521.4,
                endTime: 3523.9,
                count: 2,
                firstTime: 120.0,
                lastTime: 3523.9
            ),
            EvidenceEntry(
                evidenceRef: 2,
                category: .promoCode,
                matchedText: "code THEMOVE",
                normalizedText: "code themove",
                atomOrdinal: 46,
                startTime: 3530.1,
                endTime: 3531.6
            ),
        ]
    )
}

// MARK: - Suite

@Suite("A reverted mixed-width window must not negate the ad inside it (playhead-1mq1.2.1)")
struct RevertMixedWidthAttributionTests {

    private let assetId = "asset-themove"
    private let episodeId = "ep-themove"
    private let podcastId = "podcast-themove"

    /// The exact THEMOVE window and its true internal structure.
    private let windowStart = 3493.02
    private let windowEnd = 3536.90
    private let trueAdStart = 3505.74
    private let trueAdEnd = 3536.10

    /// The ad's own copy. Comfortably above the bank's 4-token floor so
    /// "no hard negative" can never pass merely because the text was too short
    /// to store.
    private let adCopy =
        "this episode is sponsored by haiks go to haiks dot com slash themove and use code themove at checkout"

    /// A second, textually disjoint copy used by the bank barrier below. It
    /// shares no distinctive token with `adCopy`, so "which one survived" is
    /// decidable from the stored tokens.
    private let sentinelCopy =
        "welcome back everyone to another edition of the weekly rider mailbag segment"

    // MARK: Acceptance criterion 4 — the fixture

    @Test(
        "THEMOVE: reverting 3493.02-3536.90 lands no negative label on the true ad 3505.74-3536.10",
        .timeLimit(.minutes(1))
    )
    func themoveMixedWidthRevertLeavesTheTrueAdUnlabelled() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )

        // Feature coverage exists ONLY over the true ad. That is what makes
        // the catalog clause discriminating: before this guard the whole-window
        // fingerprint was derived from exactly this audio, so the seeded entry
        // was swept away. With the guard the ad's feature windows are not
        // attributable and no fuzzy fingerprint is produced at all.
        try await store.insertFeatureWindows(
            mixedWidthFeatureWindows(
                assetId: assetId,
                from: trueAdStart,
                to: trueAdEnd
            )
        )
        let adFingerprint = AcousticFingerprint.fromFeatureWindows(
            try await store.fetchFeatureWindows(
                assetId: assetId,
                from: windowStart,
                to: windowEnd
            )
        )
        try #require(
            !adFingerprint.isZero,
            "a zero fingerprint would make the catalog clause vacuous"
        )

        let catalog = try AdCatalogStore(
            directoryURL: try makeTempDir(prefix: "1mq1221-mixed-catalog")
        )
        let learned = try #require(
            try await catalog.insert(
                showId: podcastId,
                episodePosition: .midRoll,
                durationSec: trueAdEnd - trueAdStart,
                acousticFingerprint: adFingerprint,
                originalConfidence: 0.99,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                // Deliberately NOT the reverted window's asset/window: the
                // exact-source tombstone is always written, so naming the same
                // source here would let it clear this row and the assertion
                // would stop discriminating between "tombstoned its own
                // learning" and "swept the show's creative evidence".
                sourceAssetId: "source-asset-haiks",
                sourceWindowId: "source-window-haiks",
                // The store requires `durationSec` to match this span.
                sourceStartTime: 1200,
                sourceEndTime: 1200 + (trueAdEnd - trueAdStart)
            )
        )

        let negativeBank = try NegativeFingerprintBank(
            directoryURL: try makeTempDir(prefix: "1mq1221-mixed-bank")
        )
        let trustStore = try await makeTestStore()
        try await seedSkipTestTrustProfile(
            in: trustStore,
            podcastId: podcastId,
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: TrustScoringService(store: trustStore),
            correctionStore: correctionStore,
            adCatalogStore: catalog
        )
        await orchestrator.setNegativeFingerprintBank(negativeBank)
        let cuePushes = MixedWidthCuePushCounter()
        await orchestrator.setSkipCueHandler { _ in cuePushes.record() }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId
        )
        await orchestrator.setEvidenceCatalog(
            haiksAdEvidenceCatalog(assetId: assetId)
        )

        let window = makeSkipTestAdWindow(
            id: "ad-themove",
            assetId: assetId,
            startTime: windowStart,
            endTime: windowEnd,
            confidence: 0.9,
            decisionState: "confirmed",
            evidenceText: adCopy
        )
        // The BANK BARRIER. "The bank stayed empty" is not directly observable:
        // `ingestNegativeFingerprint` fires a Task that hops to the bank actor,
        // and `drainOrchestratorEffects` only orders behind work enqueued on
        // the ORCHESTRATOR, so a read issued straight afterwards can beat the
        // ingest to the bank and pass for the wrong reason. (Measured: with the
        // guard deleted, it did — the mutation battery's P01 survived until
        // this window existed.) So a CLEAN revert is issued SECOND on the same
        // bank; once its entry has landed, a mixed-window ingest issued before
        // it would already be there too, and the count discriminates.
        let sentinel = makeSkipTestAdWindow(
            id: "ad-signoff-sentinel",
            assetId: assetId,
            startTime: 600,
            endTime: 660,
            confidence: 0.9,
            decisionState: "confirmed",
            evidenceText: sentinelCopy
        )
        try await store.insertAdWindow(window)
        try await store.insertAdWindow(sentinel)
        await orchestrator.receiveAdWindows([window, sentinel])

        // Criterion 1: the gesture still succeeds and playback state is
        // restored. This is the user-visible half and it must not regress.
        let pushesBeforeRevert = cuePushes.count
        #expect(
            await orchestrator.recordListenRevert(
                windowId: "ad-themove",
                podcastId: podcastId
            ),
            "the one-tap revert must still succeed"
        )
        let row = try #require(try await store.fetchAdWindow(id: "ad-themove"))
        #expect(
            row.decisionState == AdDecisionState.reverted.rawValue,
            "the correction must still be durable"
        )
        #expect(
            cuePushes.count > pushesBeforeRevert,
            "the revert must still republish skip cues so playback resumes"
        )

        // The receipt still commits — ACCEPT THE RECEIPT is unchanged.
        let receipts = try await awaitCorrectionReceipts(
            store,
            orchestrator: orchestrator,
            correctionStore: correctionStore,
            assetId: assetId,
            expected: 1
        )
        #expect(receipts.count == 1)
        #expect(receipts.first?.source == .listenRevert)
        #expect(receipts.first?.podcastId == podcastId)

        // Criterion 2/4: no whole-span negative label. Barrier first.
        #expect(
            await orchestrator.recordListenRevert(
                windowId: "ad-signoff-sentinel",
                podcastId: podcastId
            )
        )
        let negatives = try await awaitNegativeBankEntries(
            negativeBank,
            orchestrator: orchestrator,
            expected: 1
        )
        #expect(
            negatives.count == 1,
            """
            The reverted window contains a real ad from \(trueAdStart) to \
            \(trueAdEnd). Its copy is the window's `evidenceText`, so banking \
            that text as a confirmed false positive labels ~30s of TRUE ad as \
            negative and suppresses this ad on this show forever. A gesture \
            that cannot say "wrong edges" must not be read as "not an ad". \
            Two entries means the mixed window banked one alongside the \
            sentinel.
            """
        )
        #expect(
            negatives.first?.tokensJoined.contains("haiks") == false,
            "the surviving negative must be the sentinel's copy, not the ad's"
        )

        // Criterion 3: catalog evidence is not negated wholesale.
        let entries = try await catalog.allEntries()
        #expect(entries.count == 1, "the seeded entry is the only catalog row")
        #expect(
            entries.first?.id == learned.id,
            "the assertion below must be reading the seeded row"
        )
        #expect(
            entries.first?.revokedAt == nil,
            """
            The fuzzy revocation sweep is keyed on a fingerprint taken over the \
            reverted window. On a mixed window that fingerprint is the AD's, so \
            the sweep deletes the legitimately learned row for the very ad the \
            user did not dispute. Only material inside the evidence-free \
            remainder may drive it.
            """
        )

        await negativeBank.close()
        await catalog.close()
    }

    // MARK: Controls — the same surfaces are live for a CLEAN revert

    @Test(
        "A revert with no ad evidence inside it still banks the whole-span hard negative",
        .timeLimit(.minutes(1))
    )
    func cleanRevertStillBanksTheWholeSpanNegative() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let negativeBank = try NegativeFingerprintBank(
            directoryURL: try makeTempDir(prefix: "1mq1221-clean-bank")
        )
        let trustStore = try await makeTestStore()
        try await seedSkipTestTrustProfile(
            in: trustStore,
            podcastId: podcastId,
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: TrustScoringService(store: trustStore),
            correctionStore: correctionStore
        )
        await orchestrator.setNegativeFingerprintBank(negativeBank)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId
        )
        // The catalog is wired, but its anchors sit far outside the reverted
        // window — "no evidence INSIDE" is the discriminator, not "no catalog".
        await orchestrator.setEvidenceCatalog(
            haiksAdEvidenceCatalog(assetId: assetId)
        )

        let window = makeSkipTestAdWindow(
            id: "ad-clean",
            assetId: assetId,
            startTime: 600,
            endTime: 660,
            confidence: 0.9,
            decisionState: "confirmed",
            evidenceText: adCopy
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])

        #expect(
            await orchestrator.recordListenRevert(
                windowId: "ad-clean",
                podcastId: podcastId
            )
        )
        let negatives = try await awaitNegativeBankEntries(
            negativeBank,
            orchestrator: orchestrator,
            expected: 1
        )
        #expect(
            negatives.count == 1,
            """
            The mixed-width guard must not disable the hard-negative bank. A \
            revert with no ad evidence localized inside it is the confirmed \
            false positive the bank exists to learn from.
            """
        )
        #expect(negatives.first?.showId == podcastId)
        await negativeBank.close()
    }

    @Test(
        "Evidence covering the whole reverted window is still a whole-span negative",
        .timeLimit(.minutes(1))
    )
    func fullyEvidencedRevertStillBanksTheWholeSpanNegative() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let negativeBank = try NegativeFingerprintBank(
            directoryURL: try makeTempDir(prefix: "1mq1221-covered-bank")
        )
        let trustStore = try await makeTestStore()
        try await seedSkipTestTrustProfile(
            in: trustStore,
            podcastId: podcastId,
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: TrustScoringService(store: trustStore),
            correctionStore: correctionStore
        )
        await orchestrator.setNegativeFingerprintBank(negativeBank)
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId
        )
        await orchestrator.setEvidenceCatalog(
            haiksAdEvidenceCatalog(assetId: assetId)
        )

        // A window drawn tightly around the anchors: the halo covers it end to
        // end, so there is no evidence-free remainder and nothing to defer for.
        // This is the "that ad-sounding copy was NOT an ad" correction, which
        // must keep teaching.
        let window = makeSkipTestAdWindow(
            id: "ad-covered",
            assetId: assetId,
            startTime: 3506.0,
            endTime: 3536.0,
            confidence: 0.9,
            decisionState: "confirmed",
            evidenceText: adCopy
        )
        try await store.insertAdWindow(window)
        await orchestrator.receiveAdWindows([window])

        #expect(
            await orchestrator.recordListenRevert(
                windowId: "ad-covered",
                podcastId: podcastId
            )
        )
        let negatives = try await awaitNegativeBankEntries(
            negativeBank,
            orchestrator: orchestrator,
            expected: 1
        )
        #expect(
            negatives.count == 1,
            """
            Evidence that covers the window end to end says nothing the revert \
            did not already overrule, so it must NOT be read as a width \
            complaint. Deferring here would silently retire the bank for every \
            well-drawn ad the user disputes.
            """
        )
        await negativeBank.close()
    }

    // MARK: Partition unit rails

    @Test("The THEMOVE span partitions to the sign-off, never to the ad")
    func partitionKeepsNegativesOffTheAd() {
        let span = RevertEvidencePartition.Interval(
            startTime: windowStart,
            endTime: windowEnd
        )
        let partition = RevertEvidencePartition.resolve(
            span: span,
            adEvidence: [
                .init(startTime: 3508.0, endTime: 3510.2),
                .init(startTime: 3521.4, endTime: 3523.9),
                .init(startTime: 3530.1, endTime: 3531.6),
            ]
        )
        #expect(partition.isMixed)
        #expect(!partition.allowsWholeSpanNegativeLabel)
        #expect(
            partition.adEvidence.count == 1,
            "anchors from one read must bridge into a single ad region"
        )
        for attributable in partition.negativeAttributionSpans {
            #expect(
                attributable.endTime <= trueAdStart,
                """
                \(attributable.startTime)-\(attributable.endTime) overlaps the \
                true ad \(trueAdStart)-\(trueAdEnd); no negative may be \
                attributed there.
                """
            )
        }
        #expect(
            !partition.allowsNegativeAttribution(
                startTime: trueAdStart,
                endTime: trueAdEnd
            )
        )
        #expect(
            partition.allowsNegativeAttribution(
                startTime: windowStart,
                endTime: 3495.0
            ),
            "the sign-off remainder must stay attributable"
        )
    }

    @Test("No evidence partitions CLEAN over the whole span")
    func partitionWithNoEvidenceIsClean() {
        let span = RevertEvidencePartition.Interval(
            startTime: 100,
            endTime: 160
        )
        let partition = RevertEvidencePartition.resolve(
            span: span,
            adEvidence: []
        )
        #expect(!partition.isMixed)
        #expect(partition.allowsWholeSpanNegativeLabel)
        #expect(partition.negativeAttributionSpans == [span])
        #expect(
            partition.allowsNegativeAttribution(startTime: 100, endTime: 160)
        )
    }

    @Test("A remainder below the minimum is not a width complaint")
    func trivialRemainderIsClean() {
        let span = RevertEvidencePartition.Interval(
            startTime: 100,
            endTime: 160
        )
        // Haloed, this anchor reaches 101..160 — a 1s remainder, under the
        // 3s floor.
        let partition = RevertEvidencePartition.resolve(
            span: span,
            adEvidence: [.init(startTime: 109, endTime: 155)]
        )
        #expect(!partition.isMixed)
        #expect(partition.negativeAttributionSpans == [span])
    }

    @Test("Two separated ad regions leave the content between them attributable")
    func twoRegionsLeaveTheGapAttributable() {
        let span = RevertEvidencePartition.Interval(
            startTime: 0,
            endTime: 400
        )
        let partition = RevertEvidencePartition.resolve(
            span: span,
            adEvidence: [
                .init(startTime: 30, endTime: 60),
                .init(startTime: 300, endTime: 330),
            ]
        )
        #expect(partition.isMixed)
        #expect(
            partition.adEvidence.count == 2,
            "regions further apart than the bridge gap must stay separate"
        )
        #expect(partition.negativeAttributionSpans.count == 3)
        #expect(
            !partition.allowsNegativeAttribution(startTime: 40, endTime: 50)
        )
        #expect(
            !partition.allowsNegativeAttribution(startTime: 305, endTime: 320)
        )
        #expect(
            partition.allowsNegativeAttribution(startTime: 150, endTime: 200),
            "the editorial content between two ads must remain attributable"
        )
    }

    @Test("A feature window straddling the evidence edge is not attributable")
    func straddlingMaterialIsRefused() {
        let span = RevertEvidencePartition.Interval(
            startTime: 0,
            endTime: 200
        )
        let partition = RevertEvidencePartition.resolve(
            span: span,
            adEvidence: [.init(startTime: 108, endTime: 190)]
        )
        #expect(partition.isMixed)
        // Evidence starts at 100 after the halo; a window 98..102 crosses it.
        #expect(
            !partition.allowsNegativeAttribution(startTime: 98, endTime: 102),
            """
            Partial containment would let audio from inside the ad contribute \
            to the negative fingerprint through a straddling feature window.
            """
        )
        #expect(
            partition.allowsNegativeAttribution(startTime: 96, endTime: 100)
        )
    }

    @Test("A malformed span fails open rather than silently disabling revocation")
    func malformedSpanFailsOpen() {
        let span = RevertEvidencePartition.Interval(
            startTime: 200,
            endTime: 100
        )
        let partition = RevertEvidencePartition.resolve(
            span: span,
            adEvidence: [.init(startTime: 120, endTime: 140)]
        )
        #expect(!partition.isMixed)
        #expect(partition.negativeAttributionSpans == [span])
    }
}
