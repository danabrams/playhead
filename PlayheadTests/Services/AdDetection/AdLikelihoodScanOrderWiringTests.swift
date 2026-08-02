// AdLikelihoodScanOrderWiringTests.swift
// playhead-lxkq — the ordering policy has to reach the FM call, not just exist.
//
// `AdLikelihoodScanOrderTests` proves the POLICY. This file proves the WIRING:
// that a measured acoustic seam late in an episode actually changes which FM
// call happens first, all the way from `BackfillJobRunner.AssetInputs` through
// `planPassA` to the prompts `TestFMRuntime` records.
//
// WHY THAT SEPARATION IS THE WHOLE POINT OF THE BEAD. playhead-y3ya found that
// `FMBackfillMode.canProposeNewRegions` had ZERO production consumers — a
// correct mechanism nothing was allowed to call. A scan-ORDER policy that
// nothing passes seeds to would be the same defect one bead later. Every test
// here asserts against the recorded PROMPT SEQUENCE, which is the only thing on
// device that costs fifteen hours.
//
// THE FIXTURE IS THE FIELD SHAPE. 40 segments of 75 s tile a 3,000 s episode;
// the acoustic seam sits at 2,828 s, which is where the channel really fired on
// DE0784D8, at the leading edge of the 2,838–2,954 pod Dan lost. In episode
// order that neighbourhood is ~37 windows deep.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-lxkq: ad-likelihood scan order reaches the FM call")
struct AdLikelihoodScanOrderWiringTests {

    // MARK: - Fixture

    private static let assetId = "asset-lxkq-wiring"
    private static let transcriptVersion = "tx-lxkq-v1"
    private static let segmentCount = 40
    private static let segmentWidth = 75.0

    /// The seam the acoustic channel really emitted on DE0784D8, at the leading
    /// edge of the missed pod.
    private static let seamTime = 2_828.0

    /// A window budget that admits exactly ONE segment, so the recorded prompt
    /// sequence IS the window sequence. Each test asserts the plan count to keep
    /// this assumption honest rather than implicit.
    private static let oneSegmentPerWindow: @Sendable (String) -> Int = { prompt in
        max(1, TestFMRuntime.submittedLineRefsForTesting(from: prompt).count * 300)
    }

    private func segments() -> [AdTranscriptSegment] {
        makeFMSegments(
            analysisAssetId: Self.assetId,
            transcriptVersion: Self.transcriptVersion,
            lines: (0..<Self.segmentCount).map { index in
                let start = Double(index) * Self.segmentWidth
                return (
                    start: start,
                    end: start + Self.segmentWidth,
                    text: "Editorial line \(index) about the topic of the day."
                )
            }
        )
    }

    /// The segment index the seam falls inside.
    private var seamSegmentIndex: Int {
        Int(Self.seamTime / Self.segmentWidth)
    }

    /// Every segment the seam's NEIGHBOURHOOD touches, derived from the policy's
    /// own radius rather than hard-coded.
    ///
    /// This is the unit these tests must assert against, and getting it wrong is
    /// how they shipped red. A point seam at 2,828 s opens [2,738, 2,918], which
    /// at 75 s per window covers segments 36, 37 AND 38 — all scoring identically,
    /// because the score is per-neighbourhood, not per-distance. The documented
    /// contract then breaks that tie to the EARLIEST window (`order`'s rule 4:
    /// "equal scores break by earlier `start`, then by original index"), which is
    /// what makes the sweep a pure function of its inputs. So the first attempt is
    /// segment 36, not the seam's own 37.
    ///
    /// These tests originally asserted `first == seamSegmentIndex`. That is a
    /// claim the policy never made, and it was never caught because the file was
    /// missing from `project.pbxproj` and had never been compiled. What the bead
    /// actually needs — and what is asserted below — is that the sweep OPENS
    /// inside the seeded neighbourhood instead of at segment 0, and reaches the
    /// pod-bearing segment within the neighbourhood's own width.
    private var seamNeighbourhoodSegmentIndices: [Int] {
        let radius = AdLikelihoodScanOrder.defaultNeighbourhoodRadiusSeconds
        let lo = Self.seamTime - radius
        let hi = Self.seamTime + radius
        return (0..<Self.segmentCount).filter { index in
            let start = Double(index) * Self.segmentWidth
            let end = start + Self.segmentWidth
            return lo < end && hi > start
        }
    }

    private func seamSeed(strength: Double = 0.62) -> AdLikelihoodSeed {
        AdLikelihoodSeed(
            startTime: Self.seamTime,
            endTime: Self.seamTime,
            kind: .acousticSeam,
            strength: strength
        )
    }

    // MARK: - planPassA

    @Test("lxkq wiring: planPassA returns episode order when no seed is supplied")
    func planPassAIsLinearWithoutSeeds() async throws {
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)

        let plans = try await classifier.planPassA(segments: segments(), budget: 400)

        #expect(plans.count == Self.segmentCount, "fixture must be one segment per window")
        #expect(plans.map(\.startTime) == plans.map(\.startTime).sorted())
        #expect(plans.first?.startTime == 0)
    }

    @Test("lxkq wiring: planPassA attempts the seeded window first")
    func planPassAPromotesTheSeededWindow() async throws {
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)

        let plans = try await classifier.planPassA(
            segments: segments(),
            budget: 400,
            seeds: [seamSeed()]
        )

        #expect(plans.count == Self.segmentCount)
        // The sweep OPENS in the seeded neighbourhood rather than at segment 0.
        #expect(plans.first?.lineRefs.count == 1, "fixture must be one segment per window")
        let firstRef = plans.first?.lineRefs.first
        #expect(
            firstRef.map(seamNeighbourhoodSegmentIndices.contains) == true,
            "expected the first attempt inside \(seamNeighbourhoodSegmentIndices), got \(firstRef.map(String.init) ?? "none")"
        )
        // Non-vacuity: episode order would open at 0, which is not in that set.
        #expect(firstRef != 0)
        // And the seam's OWN segment — the one carrying the pod edge — is reached
        // within the neighbourhood's width, not 37 windows in.
        let seamPosition = plans.firstIndex { $0.lineRefs == [seamSegmentIndex] }
        #expect(
            (seamPosition ?? .max) < seamNeighbourhoodSegmentIndices.count,
            "expected segment \(seamSegmentIndex) within the first \(seamNeighbourhoodSegmentIndices.count) attempts, got \(seamPosition.map(String.init) ?? "never")"
        )
    }

    /// The reordering must not disturb the identity `windowIndex` carries.
    /// `CoarseWindowFailure.planWindowIndex`, the honest coverage cursor and
    /// every structural attribution in `BackfillJobRunner` read it as "which
    /// EPISODE position is this", and they would all mis-attribute if promotion
    /// renumbered it.
    @Test("lxkq wiring: windowIndex still names the EPISODE position after promotion")
    func windowIndexSurvivesPromotion() async throws {
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)

        let plans = try await classifier.planPassA(
            segments: segments(),
            budget: 400,
            seeds: [seamSeed()]
        )

        #expect(
            plans.first.map { seamNeighbourhoodSegmentIndices.contains($0.windowIndex) } == true,
            "the promoted head must still carry its EPISODE index, not a renumbered one"
        )
        // And it is still a complete 0..<n set — promotion permutes, it does
        // not renumber and it does not drop.
        #expect(Set(plans.map(\.windowIndex)) == Set(0..<Self.segmentCount))
    }

    @Test("lxkq wiring: promotion does not change a single prompt byte")
    func promotionLeavesPromptsUntouched() async throws {
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)
        let linear = try await classifier.planPassA(segments: segments(), budget: 400)
        let promoted = try await classifier.planPassA(
            segments: segments(),
            budget: 400,
            seeds: [seamSeed()]
        )

        let linearByIndex = Dictionary(uniqueKeysWithValues: linear.map { ($0.windowIndex, $0.prompt) })
        let promotedByIndex = Dictionary(uniqueKeysWithValues: promoted.map { ($0.windowIndex, $0.prompt) })

        #expect(linearByIndex == promotedByIndex)
        // Vacuity control: the two lists genuinely differ in ORDER, so the
        // equality above is about content and not about the lists being equal.
        #expect(linear.map(\.windowIndex) != promoted.map(\.windowIndex))
    }

    // MARK: - coarsePassA: the recorded FM call sequence

    @Test("lxkq wiring: the first FM call of the pass is the seeded neighbourhood")
    func coarsePassSubmitsTheSeededWindowFirst() async throws {
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)

        let output = try await classifier.coarsePassA(
            segments: segments(),
            seeds: [seamSeed()]
        )

        #expect(output.plans.count == Self.segmentCount, "fixture must be one segment per window")
        let submitted = await runtime.snapshotSubmittedCoarseLineRefs()
        let firstRef = submitted.first?.first
        #expect(
            firstRef.map(seamNeighbourhoodSegmentIndices.contains) == true,
            "expected the first FM call inside \(seamNeighbourhoodSegmentIndices), got \(firstRef.map(String.init) ?? "none")"
        )
        #expect(firstRef != 0)
        // The pod-bearing segment is reached in the neighbourhood's own width
        // rather than ~37 FM calls in. That number IS the bead.
        let seamPosition = submitted.firstIndex(of: [seamSegmentIndex])
        #expect((seamPosition ?? .max) < seamNeighbourhoodSegmentIndices.count)
    }

    /// The control that gives the test above its meaning, and the BEFORE
    /// measurement: unseeded, the same neighbourhood is 37 calls in.
    @Test("lxkq wiring control: unseeded, the same neighbourhood is 37 FM calls in")
    func coarsePassIsLinearWithoutSeeds() async throws {
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)

        _ = try await classifier.coarsePassA(segments: segments())

        let submitted = await runtime.snapshotSubmittedCoarseLineRefs()
        #expect(submitted.first == [0])
        #expect(submitted.firstIndex(of: [seamSegmentIndex]) == seamSegmentIndex)
    }

    /// `BackfillJobRunner` reads `coarse.plans` as its coverage DENOMINATOR and
    /// takes `unattemptedPlans.first` as "where the pass stopped". Both are
    /// questions about the episode, not about attempt sequence, so the reported
    /// plan list must come back in episode order however the pass ran.
    @Test("lxkq wiring: the REPORTED plan list is still in episode order")
    func reportedPlansStayInEpisodeOrder() async throws {
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)

        let output = try await classifier.coarsePassA(
            segments: segments(),
            seeds: [seamSeed()]
        )

        #expect(output.plans.map(\.windowIndex) == Array(0..<Self.segmentCount))
        // Vacuity control: the pass really did run out of order.
        let submitted = await runtime.snapshotSubmittedCoarseLineRefs()
        #expect(submitted.first != [0])
    }

    @Test("lxkq wiring: every window is still attempted exactly once")
    func everyWindowIsStillAttempted() async throws {
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let classifier = FoundationModelClassifier(runtime: runtime.runtime)

        _ = try await classifier.coarsePassA(segments: segments(), seeds: [seamSeed()])

        let submitted = await runtime.snapshotSubmittedCoarseLineRefs()
        #expect(submitted.count == Self.segmentCount)
        #expect(Set(submitted.flatMap { $0 }) == Set(0..<Self.segmentCount))
    }

    // MARK: - The runner: seeds are derived and passed, or they are not

    private func makeAsset() -> AnalysisAsset {
        AnalysisAsset(
            id: Self.assetId,
            episodeId: "ep-\(Self.assetId)",
            assetFingerprint: "fp-\(Self.assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(Self.assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: Double(Self.segmentCount) * Self.segmentWidth
        )
    }

    private func makeInputs(acousticBreaks: [AcousticBreak]) -> BackfillJobRunner.AssetInputs {
        let segs = segments()
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: Self.assetId,
            podcastId: "podcast-lxkq",
            segments: segs,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segs.flatMap(\.atoms),
                analysisAssetId: Self.assetId,
                transcriptVersion: Self.transcriptVersion
            ),
            transcriptVersion: Self.transcriptVersion,
            // Cold start: exactly the field state. `observedEpisodeCount = 0`
            // means `CoveragePlanner` returns `fullCoverage` / `.fullEpisodeScan`
            // — the linear sweep this bead reorders.
            plannerContext: CoveragePlannerContext(
                observedEpisodeCount: 0,
                stableRecall: false,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: 0,
                periodicFullRescanIntervalEpisodes: 10
            ),
            acousticBreaks: acousticBreaks
        )
    }

    private func makeRunner(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime,
        adLikelihoodScanOrderEnabled: Bool
    ) -> BackfillJobRunner {
        BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            adLikelihoodScanOrderEnabled: adLikelihoodScanOrderEnabled
        )
    }

    @Test("lxkq wiring: a runner with the flag ON scans the seam neighbourhood first")
    func runnerWithFlagOnPromotesTheSeam() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let runner = makeRunner(
            store: store,
            runtime: runtime.runtime,
            adLikelihoodScanOrderEnabled: true
        )

        _ = try await runner.runPendingBackfill(
            for: makeInputs(acousticBreaks: [
                AcousticBreak(time: Self.seamTime, breakStrength: 0.62, signals: [.energyDrop])
            ])
        )

        let submitted = await runtime.snapshotSubmittedCoarseLineRefs()
        let firstRef = submitted.first?.first
        #expect(
            firstRef.map(seamNeighbourhoodSegmentIndices.contains) == true,
            "flag ON must open the sweep in the seam neighbourhood \(seamNeighbourhoodSegmentIndices), got \(firstRef.map(String.init) ?? "none")"
        )
        // Non-vacuity, and the exact contrast with the OFF arm below.
        #expect(firstRef != 0)
    }

    /// The flag's OFF arm. This is the rollback guarantee: an identical run with
    /// the shipped-OFF default sweeps front to back exactly as it did before.
    @Test("lxkq wiring: a runner with the flag OFF sweeps front to back")
    func runnerWithFlagOffSweepsLinearly() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let runner = makeRunner(
            store: store,
            runtime: runtime.runtime,
            adLikelihoodScanOrderEnabled: false
        )

        _ = try await runner.runPendingBackfill(
            for: makeInputs(acousticBreaks: [
                AcousticBreak(time: Self.seamTime, breakStrength: 0.62, signals: [.energyDrop])
            ])
        )

        let submitted = await runtime.snapshotSubmittedCoarseLineRefs()
        #expect(submitted.first == [0])
        #expect(submitted.firstIndex(of: [seamSegmentIndex]) == seamSegmentIndex)
    }

    /// The negative the bead asks for at the runner level: an episode where no
    /// channel fired still gets a sensible plan, and that plan is the linear
    /// sweep. Flag ON, zero seeds — the fallback must be reachable in
    /// production, not only in a policy unit test.
    @Test("lxkq wiring: flag ON with no acoustic seam still sweeps front to back")
    func runnerWithNoSeedsFallsBackToLinear() async throws {
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset())
        let runtime = TestFMRuntime(tokenCountRule: Self.oneSegmentPerWindow)
        let runner = makeRunner(
            store: store,
            runtime: runtime.runtime,
            adLikelihoodScanOrderEnabled: true
        )

        _ = try await runner.runPendingBackfill(for: makeInputs(acousticBreaks: []))

        let submitted = await runtime.snapshotSubmittedCoarseLineRefs()
        #expect(submitted.first == [0])
        #expect(submitted.count == Self.segmentCount)
    }

    /// `AdDetectionConfig.default` is what production actually builds from.
    /// A flag that ships OFF by accident is the y3ya defect again.
    @Test("lxkq wiring: the shipped config has the scan order ON")
    func shippedConfigEnablesTheScanOrder() {
        #expect(AdDetectionConfig.default.adLikelihoodScanOrderEnabled)
    }
}
