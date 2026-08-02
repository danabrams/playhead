import Foundation
import Testing

@testable import Playhead

// playhead-avbn: WHO IS ALLOWED TO VOTE THAT THERE IS NO AD.
//
// `FMSuppressionGuard` fires on two overlapping `.noAds` windows at `.moderate`+
// certainty, after which `FMSuppressionApplicator` downweights every non-strong
// ledger entry by 0.3x and caps the span. The window list used to be built from
// EVERY scan row that overlapped the span in time — so two rows meaning "the
// refiner found no edges" or "no FM work was performed" were sufficient, on
// their own, to suppress the lexical and acoustic detectors that DID find
// something. These rails pin the admission rule that closed that.
@Suite("playhead-avbn: FM suppression voting windows")
struct FMSuppressionVotingWindowTests {

    // MARK: - Fixtures

    private static func scanRow(
        scanPass: String,
        disposition: CoarseDisposition,
        transcriptQuality: TranscriptQuality = .good,
        status: SemanticScanStatus = .success,
        errorContext: String? = nil,
        startTime: Double,
        endTime: Double
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "scan-\(scanPass)-\(startTime)-\(endTime)-\(disposition.rawValue)",
            analysisAssetId: "asset-1",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 10,
            windowStartTime: startTime,
            windowEndTime: endTime,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: "[]",
            status: status,
            attemptCount: 1,
            errorContext: errorContext,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: 1,
            prewarmHit: false,
            scanCohortJSON: "{}",
            transcriptVersion: "v1"
        )
    }

    private static func votes(
        _ rows: [SemanticScanResult],
        spanStartTime: Double = 100,
        spanEndTime: Double = 200
    ) -> [FMSuppressionWindow] {
        FMSuppressionWindow.votingWindows(
            spanStartTime: spanStartTime,
            spanEndTime: spanEndTime,
            scanResults: rows
        )
    }

    private static func triggers(_ rows: [SemanticScanResult]) -> Bool {
        FMSuppressionGuard(
            overlappingFMResults: votes(rows),
            ledger: [
                EvidenceLedgerEntry(
                    source: .lexical,
                    weight: 0.4,
                    detail: .lexical(matchedCategories: ["cta"]),
                    classificationTrust: 1.0
                )
            ],
            anchorProvenance: []
        ).evaluate().isTriggered
    }

    // MARK: - The coarse pass is the only presence verdict

    @Test("a coarse noAds row that examined its window votes")
    func coarseNoAdsVotes() {
        let rows = [
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 90, endTime: 150)
        ]
        let result = Self.votes(rows)
        #expect(result.count == 1)
        #expect(result.first?.disposition == .noAds)
        #expect(result.first?.band == .moderate)
    }

    @Test("a pass-B refinement that found no spans does NOT vote")
    func refinementEmptyDoesNotVote() {
        // The exact row `BackfillJobRunner.makeRefinementScanResult` writes when
        // the refiner returns zero spans: it means "I could not find the edges",
        // and it exists only because the COARSE pass already said containsAd.
        let rows = [
            Self.scanRow(scanPass: "passB", disposition: .noAds, startTime: 90, endTime: 150)
        ]
        #expect(Self.votes(rows).isEmpty)
    }

    @Test("two empty pass-B refinements cannot manufacture a noAds consensus")
    func twoEmptyRefinementsDoNotTrigger() {
        // The defect in its live form. Both rows overlap the span, both read
        // `.noAds`, and before playhead-avbn both banded `.moderate` because
        // pass B hardcoded `transcriptQuality = .good` — exactly the two-window
        // quorum `FMSuppressionGuard` requires.
        let rows = [
            Self.scanRow(scanPass: "passB", disposition: .noAds, startTime: 90, endTime: 150),
            Self.scanRow(scanPass: "passB", disposition: .noAds, startTime: 150, endTime: 210)
        ]
        #expect(Self.votes(rows).isEmpty)
        #expect(!Self.triggers(rows))
    }

    @Test("a pass-B refinement that DID find spans does not vote either")
    func refinementContainsAdDoesNotVote() {
        // Symmetry check, and it matters: the rule is about which pass answers
        // the presence question, not about which answer we like. A refinement
        // never votes in either direction.
        let rows = [
            Self.scanRow(scanPass: "passB", disposition: .containsAd, startTime: 90, endTime: 150)
        ]
        #expect(Self.votes(rows).isEmpty)
    }

    // MARK: - "No work was performed" is not "there is no ad"

    @Test("a no-work sentinel does NOT vote")
    func noWorkSentinelDoesNotVote() {
        // playhead-pz32's sentinel: `status == .noAds`, `disposition == .noAds`,
        // spanning the WHOLE attempted range, written precisely because zero FM
        // calls were made.
        let rows = [
            Self.scanRow(
                scanPass: "passA",
                disposition: .noAds,
                status: .noAds,
                errorContext:
                    "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)phaseProducedNoAnchors",
                startTime: 0,
                endTime: 3600
            )
        ]
        #expect(Self.votes(rows).isEmpty)
    }

    @Test("two no-work sentinels cannot manufacture a noAds consensus")
    func twoNoWorkSentinelsDoNotTrigger() {
        let rows = [
            Self.scanRow(
                scanPass: "passA",
                disposition: .noAds,
                status: .noAds,
                errorContext:
                    "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)phaseProducedNoAnchors",
                startTime: 0,
                endTime: 3600
            ),
            Self.scanRow(
                scanPass: "passA",
                disposition: .noAds,
                status: .noAds,
                errorContext:
                    "\(SemanticScanResult.noWorkSentinelErrorContextPrefix)emptySegments",
                startTime: 0,
                endTime: 3600
            )
        ]
        #expect(Self.votes(rows).isEmpty)
        #expect(!Self.triggers(rows))
    }

    @Test("a genuine permissive noAds row on the coarse pass still votes")
    func permissiveNoAdsStillVotes() {
        // The sentinel's exclusion must key on the marker, not on the status —
        // `.noAds` is also the permissive path's honest "I looked and there is
        // nothing here". Losing this would silently disarm real suppression.
        let rows = [
            Self.scanRow(
                scanPass: "passA",
                disposition: .noAds,
                status: .noAds,
                errorContext: nil,
                startTime: 90,
                endTime: 150
            )
        ]
        #expect(Self.votes(rows).count == 1)
    }

    // MARK: - The pre-existing rules must survive

    @Test("a coarse row that does not overlap the span does not vote")
    func nonOverlappingDoesNotVote() {
        let rows = [
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 0, endTime: 50)
        ]
        #expect(Self.votes(rows).isEmpty)
    }

    @Test("a row that merely abuts the span does not vote")
    func abuttingDoesNotVote() {
        let rows = [
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 0, endTime: 100),
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 200, endTime: 300)
        ]
        #expect(Self.votes(rows).isEmpty)
    }

    @Test("a degraded transcript bands weak, a good transcript bands moderate")
    func bandFollowsTranscriptQuality() {
        let degraded = Self.votes([
            Self.scanRow(
                scanPass: "passA",
                disposition: .noAds,
                transcriptQuality: .degraded,
                startTime: 90,
                endTime: 150
            )
        ])
        #expect(degraded.first?.band == .weak)

        let good = Self.votes([
            Self.scanRow(
                scanPass: "passA",
                disposition: .noAds,
                transcriptQuality: .good,
                startTime: 90,
                endTime: 150
            )
        ])
        #expect(good.first?.band == .moderate)
    }

    @Test("an FM failure row is inert because it records abstain, not noAds")
    func failureRowIsInert() {
        let rows = [
            Self.scanRow(
                scanPass: "passA",
                disposition: .abstain,
                status: .failedTransient,
                startTime: 90,
                endTime: 150
            ),
            Self.scanRow(
                scanPass: "passA",
                disposition: .abstain,
                status: .refusal,
                startTime: 150,
                endTime: 210
            )
        ]
        #expect(!Self.triggers(rows))
    }

    // MARK: - Suppression that SHOULD fire still fires

    @Test("two genuine coarse noAds windows still trigger suppression")
    func twoCoarseNoAdsStillTrigger() {
        // The negative control for every rail above: the fix narrows WHO may
        // vote, and must not disarm the mechanism for rows that legitimately do.
        let rows = [
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 90, endTime: 150),
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 150, endTime: 210)
        ]
        #expect(Self.votes(rows).count == 2)
        #expect(Self.triggers(rows))
    }

    @Test("a coarse noAds pair mixed with refinement rows still triggers on the pair alone")
    func coarsePairSurvivesRefinementNoise() {
        let rows = [
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 90, endTime: 150),
            Self.scanRow(scanPass: "passB", disposition: .noAds, startTime: 95, endTime: 145),
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 150, endTime: 210)
        ]
        #expect(Self.votes(rows).count == 2)
        #expect(Self.triggers(rows))
    }

    @Test("one genuine coarse noAds window is still below the two-window quorum")
    func singleCoarseNoAdsDoesNotTrigger() {
        let rows = [
            Self.scanRow(scanPass: "passA", disposition: .noAds, startTime: 90, endTime: 150),
            Self.scanRow(scanPass: "passB", disposition: .noAds, startTime: 150, endTime: 210)
        ]
        #expect(Self.votes(rows).count == 1)
        #expect(!Self.triggers(rows))
    }
}
