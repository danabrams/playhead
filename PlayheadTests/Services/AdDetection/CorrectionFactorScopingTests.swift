// CorrectionFactorScopingTests.swift
// playhead-ar60: a correction the user makes at 210 s must not rewrite the
// confidence of a span 2,600 s away that has independent evidence.
//
// Before this bead `AdDetectionService.runBackfill` resolved ONE
// `passthrough * boost` scalar per run and handed it to every span's
// `DecisionMapper`. The witness, re-derived from the 2026-08-02 device pull on
// episode DE0784D8: across all five fusion windows of a run the ratio
// `skipConfidence / proposalConfidence` was BIT-IDENTICAL — 0.00252168501165
// at 2026-08-01 00:42:34, then 8.16025237649e-07 at 15:24:40 (seven seconds
// after a veto), then 1.77560023944e-05, then 0.00683 and 0.00708 the next
// day. A per-span quantity cannot have a per-asset ratio. Each window's own
// `proposalConfidence` never moved by a bit across all five runs.
//
// These tests pin the arithmetic of `CorrectionFactorSnapshot` and then the
// end-to-end scoping through the real `PersistentUserCorrectionStore` using
// Dan's actual correction spans on that episode.

import Foundation
import Testing

@testable import Playhead

@Suite("Per-span correction factor scoping (playhead-ar60)")
struct CorrectionFactorScopingTests {

    private typealias Pull = DE0784D8MidRollPodFixture

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    // MARK: - The pure arithmetic

    @Test("identity leaves every span at exactly 1.0")
    func identityIsNeutral() {
        let snapshot = CorrectionFactorSnapshot.identity
        #expect(snapshot.factor(overlapping: 0, 60) == 1.0)
        #expect(snapshot.factor(overlapping: 4329.96, 4342.2) == 1.0)
    }

    @Test("a span-scoped veto suppresses the span it overlaps and NOTHING else")
    func spanScopedVetoIsLocal() {
        // Weight 0.997 is the shape of a fresh veto's decay weight: it drives
        // passthrough to 0.003, which is what put the DE0784D8 rows at ~0.003x
        // their proposal.
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [.init(weight: 0.997, range: 210.12...239.82)],
            boosters: []
        )
        // The vetoed span: suppressed, hard.
        #expect(abs(snapshot.factor(overlapping: 210.12, 239.82) - 0.003) < 1e-9)
        // Touching the edge counts as overlap.
        #expect(snapshot.factor(overlapping: 235, 260) < 0.01)
        // The span 2,600 s away with independent evidence: untouched. This is
        // the bead's acceptance criterion, stated as an assertion.
        #expect(snapshot.factor(overlapping: 2828.4, 2836.44) == 1.0)
        #expect(snapshot.factor(overlapping: 4329.96, 4342.2) == 1.0)
        // Adjacent but disjoint — a half-open comparison, so an end that meets
        // a start does not overlap.
        #expect(snapshot.factor(overlapping: 239.82, 300) == 1.0)
    }

    @Test("a span-scoped MARK boosts only what it overlaps — the inflation direction")
    func spanScopedBoostIsLocal() {
        // The boost direction is the one that pushes toward auto-skip, and on
        // the pull it was the larger effect: two marks nearly DOUBLED seven
        // spans they did not overlap, five of which crossed the 0.7 preload
        // floor on inflation alone.
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [],
            boosters: [.init(weight: 0.994, range: 2838.18...2897.94)]
        )
        #expect(abs(snapshot.factor(overlapping: 2838.18, 2897.94) - 1.994) < 1e-9)
        #expect(snapshot.factor(overlapping: 1396.2, 1407.1) == 1.0,
                "a mark at 2838 must not inflate a span at 1396 past a detector floor")
    }

    @Test("show-wide SUPPRESSORS still apply everywhere — they assert a sponsor, not a position")
    func showWideScopesRemainAssetWide() {
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [.init(weight: 0.5, range: nil)],
            boosters: []
        )
        #expect(snapshot.factor(overlapping: 0, 60) == 0.5)
        #expect(snapshot.factor(overlapping: 5000, 5100) == 0.5)
    }

    @Test("suppress and boost compose exactly as the two asset-wide factors did")
    func arithmeticMatchesTheFactorsItReplaces() {
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [
                .init(weight: 0.4, range: 0...100),
                .init(weight: 0.9, range: 0...100),
            ],
            boosters: [
                .init(weight: 0.2, range: 0...100),
                .init(weight: 0.75, range: 0...100),
            ]
        )
        // max weight in each direction, exactly like `correctionPassthroughFactor`
        // (`1 - max`) and `correctionBoostFactor` (`1 + max`).
        #expect(abs(snapshot.factor(overlapping: 10, 20) - (0.1 * 1.75)) < 1e-12)
    }

    @Test("the asset-wide compatibility init reproduces the pre-ar60 blanket")
    func assetWideShimIsTheOldBlanket() {
        let shim = CorrectionFactorSnapshot(
            assetWidePassthrough: 0.0025216850116,
            boost: 1.0
        )
        // Same value for every span — that IS the blanket, and it is the
        // correct answer for a store that cannot report scope.
        let a = shim.factor(overlapping: 210.12, 239.82)
        let b = shim.factor(overlapping: 4329.96, 4342.2)
        #expect(a == b)
        #expect(abs(a - 0.0025216850116) < 1e-15)
        #expect(CorrectionFactorSnapshot(assetWidePassthrough: 1, boost: 1)
            .factor(overlapping: 0, 10) == 1.0)
    }

    @Test("a non-finite or inverted span sees only show-wide entries — never a guess")
    func unusableSpanFallsBackToShowWideOnly() {
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [
                .init(weight: 0.9, range: 0...100),
                .init(weight: 0.5, range: nil),
            ],
            boosters: []
        )
        #expect(snapshot.factor(overlapping: .nan, 100) == 0.5)
        #expect(snapshot.factor(overlapping: 100, 10) == 0.5)
        #expect(snapshot.factor(overlapping: .infinity, .infinity) == 0.5)
    }

    // MARK: - hasSpanScopedSuppressor (ar60 R1 review)

    @Test("only a span-scoped suppressor that reaches the span counts as a judgement about it")
    func spanScopedSuppressorPredicate() {
        let snapshot = CorrectionFactorSnapshot(
            suppressors: [
                .init(weight: 0.9, range: 100...200),
                // Show-wide, and how an UNPLACEABLE suppressor is recorded.
                .init(weight: 0.95, range: nil),
                // Fully decayed: moves the factor by nothing, so it cannot be
                // what blocked the span.
                .init(weight: 0.0, range: 300...400),
            ],
            boosters: [.init(weight: 0.5, range: 100...200)]
        )
        #expect(snapshot.hasSpanScopedSuppressor(overlapping: 150, 160))
        #expect(snapshot.hasSpanScopedSuppressor(overlapping: 190, 260))
        #expect(!snapshot.hasSpanScopedSuppressor(overlapping: 210, 260))
        #expect(!snapshot.hasSpanScopedSuppressor(overlapping: 320, 380),
                "a zero-weight entry is not a live veto")
        #expect(!snapshot.hasSpanScopedSuppressor(overlapping: .nan, 160))
        #expect(!snapshot.hasSpanScopedSuppressor(overlapping: 160, 150))
        // A booster is not a veto, however precisely it is scoped.
        #expect(!CorrectionFactorSnapshot(
            suppressors: [],
            boosters: [.init(weight: 0.9, range: 100...200)]
        ).hasSpanScopedSuppressor(overlapping: 150, 160))
        // The pre-ar60 blanket shim reports show-wide, so it never authorises
        // withholding a banner.
        #expect(!CorrectionFactorSnapshot(assetWidePassthrough: 0.001, boost: 1)
            .hasSpanScopedSuppressor(overlapping: 150, 160))
        #expect(!CorrectionFactorSnapshot.identity
            .hasSpanScopedSuppressor(overlapping: 150, 160))
    }

    // MARK: - End to end, through the real store, on Dan's real corrections

    @Test("DE0784D8: five vetoes and five marks, and the span at 4329.96 sees a factor of exactly 1.0")
    func realCorrectionsDoNotReachAnUnrelatedSpan() async throws {
        let dir = try makeTempDir(prefix: "CorrectionFactorScoping")
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "DE0784D8-BE2F-4BEB-8BA1-3D9EF51AD659"
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: assetId))

        let corrections = PersistentUserCorrectionStore(store: store)
        for veto in Pull.falsePositiveVetoSpans {
            await corrections.recordVeto(
                startTime: veto.lowerBound,
                endTime: veto.upperBound,
                assetId: assetId,
                podcastId: nil,
                source: CorrectionSource.manualVeto
            )
        }
        for mark in Pull.falseNegativeMarkSpans {
            await corrections.recordVeto(
                startTime: mark.lowerBound,
                endTime: mark.upperBound,
                assetId: assetId,
                podcastId: nil,
                source: CorrectionSource.falseNegative
            )
        }

        let snapshot = await corrections.correctionFactorSnapshot(for: assetId)

        // The asset-wide blanket this replaces: with ten corrections on the
        // asset, the old scalar was far from 1.0 for EVERY span.
        let blanketPassthrough = await corrections
            .correctionPassthroughFactor(for: assetId)
        let blanketBoost = await corrections.correctionBoostFactor(for: assetId)
        let blanket = blanketPassthrough * blanketBoost
        #expect(blanket < 0.5,
                "precondition: the pre-ar60 blanket really did collapse this asset")

        // THE ACCEPTANCE CRITERION. The fusion window at 4329.96-4342.20 has
        // independent evidence and overlaps none of the ten corrections; it
        // shipped at skipConfidence 0.0039 with gate `blockedByUserCorrection`.
        let uncorrected = Pull.uncorrectedFusionSpan
        #expect(
            snapshot.factor(
                overlapping: uncorrected.lowerBound,
                uncorrected.upperBound
            ) == 1.0,
            """
            A correction the user made at 210 s (or 2,670, or 4,800) must not \
            reach the span at \(uncorrected.lowerBound)-\(uncorrected.upperBound). \
            The pre-ar60 blanket was \(blanket).
            """
        )

        // …while the span he DID veto is still suppressed. The fix narrows the
        // blast radius; it does not stop honouring the veto.
        let vetoed = try #require(Pull.falsePositiveVetoSpans.first {
            $0.lowerBound == 2828.400
        })
        let vetoedFactor = snapshot.factor(
            overlapping: vetoed.lowerBound,
            vetoed.upperBound
        )
        #expect(vetoedFactor < 1.0,
                "the seam FP Dan vetoed must still be suppressed — at \(vetoedFactor)")

        // …and a span he MARKED is still boosted.
        let marked = try #require(Pull.falseNegativeMarkSpans.first {
            $0.lowerBound == 2838.180
        })
        #expect(
            snapshot.factor(
                overlapping: marked.lowerBound,
                marked.upperBound
            ) > 1.0,
            "a mark must still boost the span it names"
        )
    }

    // MARK: - playhead-q6y3: the show-wide scope, in the BOOST direction

    /// The exact event `NowPlayingView.onAlwaysSkipSponsorAsync` writes for
    /// "Always skip <sponsor> on this show".
    private func alwaysSkipSponsorEvent(
        assetId: String,
        podcastId: String = "pod-q6y3",
        sponsor: String = "squarespace"
    ) -> CorrectionEvent {
        CorrectionEvent(
            analysisAssetId: assetId,
            scope: CorrectionScope.sponsorOnShow(
                podcastId: podcastId,
                sponsor: sponsor
            ).serialized,
            source: .falseNegative,
            podcastId: podcastId,
            correctionType: .falseNegative,
            targetRefs: CorrectionTargetRefs(sponsorEntity: sponsor)
        )
    }

    /// `SkipOrchestrator.preloadAdmissibleWindows` compares this against
    /// `actuationConfidence` (playhead-atr3). It is `private static` there, so
    /// the value is restated rather than read; the rail that pins the
    /// orchestrator's own use of it is
    /// `SkipOrchestratorPreloadActuationFloorTests`.
    private static let preloadFloor = 0.7

    @Test("a show-wide REINFORCEMENT lifts no span, and cannot carry one over the 0.7 preload floor")
    func showWideBoosterIsAppliedNowhere() async throws {
        let dir = try makeTempDir(prefix: "Q6y3ShowWideBoost")
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "q6y3-show-wide-boost"
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: assetId))

        let corrections = PersistentUserCorrectionStore(store: store)
        try await corrections.record(alwaysSkipSponsorEvent(assetId: assetId))

        // ANTI-VACUITY. The row is really there, and the store really does read
        // it as a BOOSTER — the asset-wide reader, which does not decode scope,
        // reports a boost well above 1.0 from this single event. So everything
        // below is the snapshot declining to PLACE a booster it can see, not
        // the test quietly asserting over an empty table or a row the store
        // classified as something else.
        let assetWideBoost = await corrections.correctionBoostFactor(for: assetId)
        #expect(assetWideBoost > 1.5,
                """
                precondition: a fresh sponsorOnShow false-negative must read as a \
                strong asset-wide boost (got \(assetWideBoost)) — otherwise this \
                test proves nothing about scoping
                """)

        let snapshot = await corrections.correctionFactorSnapshot(for: assetId)
        #expect(snapshot.boosters.isEmpty,
                """
                a scope that names a SPONSOR cannot say where the ads are, so it \
                must not be recorded as a booster over every span — got \
                \(snapshot.boosters.count)
                """)
        #expect(snapshot.factor(overlapping: 10, 20) == 1.0)
        #expect(snapshot.factor(overlapping: 2828.4, 2836.44) == 1.0)
        #expect(snapshot.factor(overlapping: 4329.96, 4342.2) == 1.0)

        // THE ACCEPTANCE CRITERION, stated as arithmetic. A span whose detector
        // number sits below the cross-launch preload floor must still sit below
        // it after the button has been pressed. Pre-fix the show-wide booster
        // was recorded with `range: nil`, so `factor` returned the full
        // `min(2, 1 + weight)` for EVERY span: 0.5 x ~1.99 = ~0.99, over the
        // floor, on one tap and on evidence about no span at all.
        let detection = 0.5
        let actuation = detection * snapshot.factor(overlapping: 4329.96, 4342.2)
        #expect(actuation < Self.preloadFloor,
                """
                pressing "always skip <sponsor>" must not admit an unrelated \
                0.5-confidence span to the cross-launch preload — got \(actuation)
                """)
        #expect(detection * assetWideBoost >= Self.preloadFloor,
                """
                anti-vacuity for the line above: the blanket this replaces DID \
                cross the floor, so the assertion is about scoping and not about \
                a boost too small to matter
                """)
    }

    @Test("a show-wide VETO still suppresses every span — the split is direction, not scope")
    func showWideSuppressorIsUnchanged() async throws {
        let dir = try makeTempDir(prefix: "Q6y3ShowWideVeto")
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "q6y3-show-wide-veto"
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: assetId))

        // What `PersistentUserCorrectionStore.recordVeto(span:ledgerEntries:)`
        // writes for a brandSpan on a vetoed span: a GENUINE sponsor veto,
        // deliberately left in the suppress direction by playhead-q6y3.
        let corrections = PersistentUserCorrectionStore(store: store)
        try await corrections.record(
            CorrectionEvent(
                analysisAssetId: assetId,
                scope: CorrectionScope.sponsorOnShow(
                    podcastId: "pod-q6y3",
                    sponsor: "squarespace"
                ).serialized,
                source: .manualVeto,
                podcastId: "pod-q6y3",
                correctionType: .falsePositive
            )
        )

        let snapshot = await corrections.correctionFactorSnapshot(for: assetId)
        #expect(snapshot.suppressors.count == 1)
        #expect(snapshot.suppressors.first?.range == nil,
                "a vetoed sponsor is asserted over the whole asset — that has not changed")
        #expect(snapshot.factor(overlapping: 10, 20) < 1.0)
        #expect(snapshot.factor(overlapping: 4329.96, 4342.2) < 1.0)
    }

    @Test("a PLACEABLE reinforcement on the same asset still boosts the span it names")
    func placedBoosterSurvivesAlongsideAShowWideOne() async throws {
        let dir = try makeTempDir(prefix: "Q6y3MixedBoost")
        defer { try? FileManager.default.removeItem(at: dir) }

        let assetId = "q6y3-mixed-boost"
        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: assetId))

        let corrections = PersistentUserCorrectionStore(store: store)
        try await corrections.record(alwaysSkipSponsorEvent(assetId: assetId))
        // A transcript mark — the boost direction, scoped to a span.
        await corrections.recordVeto(
            startTime: 2838.18,
            endTime: 2897.94,
            assetId: assetId,
            podcastId: "pod-q6y3",
            source: CorrectionSource.falseNegative
        )

        let snapshot = await corrections.correctionFactorSnapshot(for: assetId)
        // ANTI-VACUITY. Exactly ONE booster survives — the placed one. If the
        // change had simply broken the boost direction, this would be zero and
        // the assertion below would pass for the wrong reason.
        #expect(snapshot.boosters.count == 1)
        #expect(snapshot.boosters.first?.range != nil)
        #expect(snapshot.factor(overlapping: 2838.18, 2897.94) > 1.0,
                "a mark must still boost the span it names")
        #expect(snapshot.factor(overlapping: 4329.96, 4342.2) == 1.0,
                "and must reach nothing else, show-wide reinforcement included")
    }

    @Test("a store that reports only the asset-wide scalars keeps the old blanket behaviour")
    func protocolDefaultPreservesLegacyBehaviour() async {
        // `NoOpUserCorrectionStore` takes the protocol default, which builds
        // the snapshot from the two asset-wide factors. No corrections ⇒ 1.0
        // everywhere, which is exactly what it reported before.
        let noOp = NoOpUserCorrectionStore()
        let snapshot = await noOp.correctionFactorSnapshot(for: "any-asset")
        #expect(snapshot.factor(overlapping: 0, 60) == 1.0)
        #expect(snapshot.factor(overlapping: 4329.96, 4342.2) == 1.0)
        #expect(snapshot == .identity)
    }
}
