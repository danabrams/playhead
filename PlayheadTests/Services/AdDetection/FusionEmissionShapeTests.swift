// FusionEmissionShapeTests.swift
// playhead-ar60: the three mechanisms, verified LIVE through `runBackfill` →
// `ad_windows`, with `decision_events` (written by the same run) as the
// independent witness for what each column should hold.
//
// These are the fix-detectors. The frozen-data pins in
// `DE0784D8MidRollPodRegressionTests` record what the device SHIPPED and
// cannot observe a production change; these run the real pipeline.
//
//   1. correctionFactor is per-SPAN: a veto far from the produced window
//      leaves it at the same confidence as a run with no corrections at all,
//      while a veto that OVERLAPS it does suppress it.
//   2. `ad_windows.confidence` holds DETECTION and `skipConfidence` holds
//      ACTUATION — cross-checked against the run's own `decision_events` row.
//   3. a span gated `blockedByUserCorrection` does not persist `.confirmed`.

import Foundation
import Testing

@testable import Playhead

@Suite("Fusion emission shape: detection vs actuation, per-span corrections (playhead-ar60)")
struct FusionEmissionShapeTests {

    private static let adChunkStart: Double = 30
    private static let adChunkEnd: Double = 60

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

    /// The same lexical ad signal `BackfillFusionPipelineTests` uses, over a
    /// long enough episode that the post-roll guard is irrelevant.
    private func makeChunks(assetId: String) -> [TranscriptChunk] {
        let texts = [
            "Welcome back to the show today.",
            "This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show. Sign up today and make your website.",
            "Back to our conversation about technology and the future of podcasting.",
            "We were talking about how the industry has changed over the last decade.",
            "And that brings us to the end of this segment of the programme.",
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

    private func makeService(store: AnalysisStore) -> AdDetectionService {
        AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: AdDetectionConfig(
                candidateThreshold: 0.40,
                confirmationThreshold: 0.70,
                suppressionThreshold: 0.25,
                hotPathLookahead: 90.0,
                detectorVersion: "test-v1",
                fmBackfillMode: .off,
                certaintyTieredSkipEnabled: false,
                unanchoredExtentBlocksAutoSkip: false
            )
        )
    }

    /// Run one backfill with the supplied vetoes already recorded, and return
    /// the fusion windows it persisted.
    private func runBackfill(
        assetId: String,
        vetoes: [ClosedRange<Double>],
        showWideVetoedSponsors: [String] = []
    ) async throws -> (windows: [AdWindow], events: [DecisionEvent]) {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()
        try await store.insertAsset(makeAsset(id: assetId))

        let service = makeService(store: store)
        if !vetoes.isEmpty || !showWideVetoedSponsors.isEmpty {
            let corrections = PersistentUserCorrectionStore(store: store)
            for veto in vetoes {
                await corrections.recordVeto(
                    startTime: veto.lowerBound,
                    endTime: veto.upperBound,
                    assetId: assetId,
                    podcastId: nil,
                    source: CorrectionSource.manualVeto
                )
            }
            // A SHOW-WIDE `sponsorOnShow` scope in the SUPPRESS direction, on
            // the current asset — a genuine sponsor veto, which is what
            // `PersistentUserCorrectionStore.recordVeto(span:ledgerEntries:)`
            // writes for every `brandSpan` on a vetoed span.
            //
            // playhead-q6y3: this used to name the always-skip-sponsor button
            // as the writer. It no longer is — that gesture means "yes, this
            // IS an ad" and now writes the REINFORCEMENT direction. The
            // behaviour these tests pin (a show-wide veto suppresses the whole
            // asset yet must not withhold the banner) is unchanged and is
            // still reachable; only the attribution was wrong.
            for sponsor in showWideVetoedSponsors {
                try await corrections.record(
                    CorrectionEvent(
                        analysisAssetId: assetId,
                        scope: CorrectionScope.sponsorOnShow(
                            podcastId: "podcast-ar60",
                            sponsor: sponsor
                        ).serialized,
                        source: .manualVeto,
                        podcastId: "podcast-ar60",
                        correctionType: .falsePositive
                    )
                )
            }
            await service.setUserCorrectionStore(corrections)
        }

        try await service.runBackfill(
            chunks: makeChunks(assetId: assetId),
            analysisAssetId: assetId,
            podcastId: "podcast-ar60",
            episodeDuration: 150.0
        )
        let windows = try await store.fetchAdWindows(assetId: assetId)
            .filter { $0.id.hasPrefix("fusion-") }
        let events = try await store.loadDecisionEvents(for: assetId)
        return (windows, events)
    }

    // MARK: - Mechanism 2: two quantities, two columns

    @Test("a persisted fusion row's confidence is the run's proposalConfidence and its skipConfidence is the run's skipConfidence")
    func persistedColumnsMatchTheRunsOwnDecisionEvent() async throws {
        let (windows, events) = try await runBackfill(
            assetId: "ar60-columns",
            vetoes: []
        )
        #expect(!windows.isEmpty,
                "the lexical fixture must produce a fusion window — pipeline regression if zero")

        var checked = 0
        for window in windows {
            guard let event = events
                .filter({ $0.windowId == window.id })
                .max(by: { $0.createdAt < $1.createdAt })
            else { continue }
            checked += 1
            // The DETECTION column holds the detection number…
            #expect(window.confidence == event.proposalConfidence,
                    "ad_windows.confidence must be the proposal, not the actuation number")
            // …and the ACTUATION column holds the actuation number, separately.
            #expect(window.skipConfidence == event.skipConfidence)
            #expect(window.actuationConfidence == event.skipConfidence)
            // Fusion no longer squats on the metadata extractor's column. With
            // `FallbackExtractor` wired the extractor overwrites it anyway —
            // which is precisely why parking a detection score there lost it.
            #expect(window.metadataConfidence != event.proposalConfidence)
        }
        #expect(checked > 0, "at least one window must have a decision_events witness")
    }

    // MARK: - Mechanism 1: the correction blanket is per-span

    @Test("a veto 2,600 s from the produced window leaves its confidence bit-identical to an uncorrected run")
    func distantVetoDoesNotReachTheWindow() async throws {
        let clean = try await runBackfill(assetId: "ar60-clean", vetoes: [])
        // A veto nowhere near the ad chunk, in the shape the UI writes.
        let distant = try await runBackfill(
            assetId: "ar60-distant",
            vetoes: [2670.300...2699.820, 4800.480...4949.820]
        )

        let cleanWindow = try #require(clean.windows.first)
        let distantWindow = try #require(
            distant.windows.first { $0.startTime == cleanWindow.startTime }
        )
        #expect(distantWindow.confidence == cleanWindow.confidence)
        // The number that actually decides whether a skip may fire.
        #expect(
            distantWindow.actuationConfidence == cleanWindow.actuationConfidence,
            """
            A veto at 2,670 s must not change the actuation confidence of a \
            window at \(cleanWindow.startTime)-\(cleanWindow.endTime). Clean run: \
            \(cleanWindow.actuationConfidence); with two distant vetoes: \
            \(distantWindow.actuationConfidence).
            """
        )
        #expect(distantWindow.eligibilityGate == cleanWindow.eligibilityGate,
                "and it must not acquire a blockedByUserCorrection gate either")
    }

    @Test("a veto that OVERLAPS the window still suppresses it — the fix narrows the blast radius, it does not stop honouring the veto")
    func overlappingVetoStillSuppresses() async throws {
        let clean = try await runBackfill(assetId: "ar60-clean2", vetoes: [])
        let cleanWindow = try #require(clean.windows.first)

        let overlapped = try await runBackfill(
            assetId: "ar60-overlap",
            vetoes: [cleanWindow.startTime...cleanWindow.endTime]
        )
        let suppressedWindow = try #require(
            overlapped.windows.first { $0.startTime == cleanWindow.startTime }
        )
        #expect(suppressedWindow.actuationConfidence < cleanWindow.actuationConfidence,
                "the vetoed span's permission to act must drop")
        #expect(suppressedWindow.eligibilityGate
                    == SkipEligibilityGate.blockedByUserCorrection.rawValue)
        // The DETECTION number is not a matter of opinion: a veto changes what
        // we are ALLOWED to do, not what the evidence said.
        #expect(suppressedWindow.confidence == cleanWindow.confidence,
                "a correction must not rewrite the detection score")
    }

    // MARK: - Mechanism 3: detectOnly is not unconditionally confirmed

    @Test("a span gated blockedByUserCorrection persists suppressed, not confirmed")
    func blockedByUserCorrectionPersistsSuppressed() async throws {
        let clean = try await runBackfill(assetId: "ar60-clean3", vetoes: [])
        let cleanWindow = try #require(clean.windows.first)
        // Precondition: with no corrections the same span persists as a
        // user-visible row, so the change below is about the VETO and not
        // about the span being uninteresting.
        #expect(cleanWindow.decisionState != AdDecisionState.suppressed.rawValue)

        let overlapped = try await runBackfill(
            assetId: "ar60-suppressed",
            vetoes: [cleanWindow.startTime...cleanWindow.endTime]
        )
        let vetoed = try #require(
            overlapped.windows.first { $0.startTime == cleanWindow.startTime }
        )
        #expect(vetoed.eligibilityGate
                    == SkipEligibilityGate.blockedByUserCorrection.rawValue)
        #expect(
            vetoed.decisionState == AdDecisionState.suppressed.rawValue,
            """
            A span whose gate records the user's own "not an ad" must not come \
            back as a confirmed banner row. Persisted state: \(vetoed.decisionState).
            """
        )
    }

    /// ar60 R1 review. The `.suppressed` arm has to be about THIS span, and a
    /// show-wide sponsor veto is not — it is one tap on the shipped "Always
    /// skip this sponsor" button, and applying it here would take EVERY fusion
    /// banner in the episode away.
    @Test("a show-wide sponsor veto gates the span but still persists a bannerable row")
    func showWideVetoDoesNotSuppressTheBanner() async throws {
        let clean = try await runBackfill(assetId: "ar60-showwide-clean", vetoes: [])
        let cleanWindow = try #require(clean.windows.first)
        #expect(cleanWindow.decisionState != AdDecisionState.suppressed.rawValue)

        let showWide = try await runBackfill(
            assetId: "ar60-showwide",
            vetoes: [],
            showWideVetoedSponsors: ["squarespace"]
        )
        let gated = try #require(showWide.windows.first)
        // Precondition: the show-wide veto really does reach this span — it is
        // asset-wide by construction, so the actuation number must have moved.
        #expect(
            gated.actuationConfidence < cleanWindow.actuationConfidence,
            "a show-wide veto still suppresses ACTUATION on every span"
        )
        #expect(
            gated.decisionState != AdDecisionState.suppressed.rawValue,
            """
            A sponsor-scoped veto says nothing about WHERE the ads are, so it \
            must not withhold the banner. Persisted state: \(gated.decisionState).
            """
        )
    }

    // MARK: - The mapping itself, exhaustively

    @Test("the fusion decision-state mapping is exhaustive over policy action x gate x scope")
    func decisionStateMappingIsExhaustive() {
        for action in SkipPolicyAction.allCases {
            for gate in SkipEligibilityGate.allCases {
                for spanScoped in [true, false] {
                    let state = AdDetectionService.fusionDecisionState(
                        policyAction: action,
                        eligibilityGate: gate,
                        userCorrectionIsSpanScoped: spanScoped
                    )
                    switch action {
                    case .autoSkipEligible:
                        #expect(state == (gate == .eligible ? .confirmed : .candidate))
                    case .detectOnly, .logOnly:
                        // ONLY the user's own veto OF THIS SPAN demotes. Every
                        // other blocked gate is the system's uncertainty, and a
                        // banner Dan can answer is what that population is for
                        // — as is a veto that was about a sponsor rather than
                        // about a position.
                        #expect(state == (
                            gate == .blockedByUserCorrection && spanScoped
                                ? .suppressed
                                : .confirmed
                        ))
                    case .suppress:
                        #expect(state == .suppressed)
                    }
                }
            }
        }
        // The specific regressions, named.
        #expect(
            AdDetectionService.fusionDecisionState(
                policyAction: .detectOnly,
                eligibilityGate: .blockedByUserCorrection,
                userCorrectionIsSpanScoped: true
            ) == .suppressed
        )
        #expect(
            AdDetectionService.fusionDecisionState(
                policyAction: .detectOnly,
                eligibilityGate: .blockedByUserCorrection,
                userCorrectionIsSpanScoped: false
            ) == .confirmed,
            "a show-wide or unplaceable veto must not withhold the banner"
        )
        #expect(
            AdDetectionService.fusionDecisionState(
                policyAction: .detectOnly,
                eligibilityGate: .markOnly,
                userCorrectionIsSpanScoped: true
            ) == .confirmed
        )
    }
}
