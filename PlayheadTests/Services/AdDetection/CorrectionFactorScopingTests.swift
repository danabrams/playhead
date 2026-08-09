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

    @Test("show-wide scopes still apply everywhere — they assert a sponsor, not a position")
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
