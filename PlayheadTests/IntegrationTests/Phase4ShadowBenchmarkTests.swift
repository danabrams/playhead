// Phase4ShadowBenchmarkTests.swift
// Benchmark the Phase 4 region pipeline (`RegionProposalBuilder` +
// `RegionFeatureExtractor`) end-to-end against the Conan
// "Fanhausen Revisited" fixture. Parallel to RealEpisodeBenchmarkTests
// (which only exercises Phase 2 via `runHotPath`), this suite drives
// `AdDetectionService.runBackfill` with an injected `RegionShadowObserver`
// so the step-10 shadow phase actually runs and we can read the recorded
// `RegionFeatureBundle`s back out.
//
// These are BENCHMARK tests, not pass/fail gates. They print
// recall / precision / coverage to the test log so we can eyeball Phase 4
// quality across commits. Hard assertions are intentionally lenient.

import Foundation
import Testing
@testable import Playhead

@Suite("Phase 4 Shadow Benchmark - Conan Fanhausen Revisited")
struct Phase4ShadowBenchmarkTests {

    // MARK: - Helpers

    private static func ts(_ seconds: Double) -> String {
        let s = Int(seconds)
        return String(format: "%d:%02d", s / 60, s % 60)
    }

    /// Tracks temp directories created for the benchmark's full-pipeline run.
    private static let benchStoreDirs = TestTempDirTracker()

    /// Creates a temp-directory-backed AnalysisStore for the benchmark.
    /// Duplicated from RealEpisodeBenchmarkTests.makeBenchmarkStore() — kept
    /// local so this benchmark stays independent of the Phase 2 suite.
    private static func makeBenchmarkStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "PlayheadPhase4Benchmark")
        benchStoreDirs.track(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private static func makeBenchmarkAsset(id: String, episodeId: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "phase4-bench-fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///benchmark/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// Builds 1s synthetic feature windows spanning [0, duration). Same
    /// shape as RealEpisodeBenchmarkTests.buildBenchmarkFeatureWindows() —
    /// duplicated locally to avoid coupling the two suites.
    private static func buildBenchmarkFeatureWindows(
        assetId: String,
        duration: Double
    ) -> [FeatureWindow] {
        var windows: [FeatureWindow] = []
        windows.reserveCapacity(Int(duration) + 1)
        for start in stride(from: 0.0, to: duration, by: 1.0) {
            let end = min(start + 1.0, duration)
            windows.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: start,
                endTime: end,
                rms: 0.15,
                spectralFlux: 0.5,
                musicProbability: 0.0,
                pauseProbability: 0.1,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            ))
        }
        return windows
    }

    /// Returns true if the [a0, a1) and [b0, b1) intervals have any overlap.
    private static func intervalsOverlap(
        _ a0: Double, _ a1: Double, _ b0: Double, _ b1: Double
    ) -> Bool {
        a0 < b1 && a1 > b0
    }

    /// Region scoring against ground-truth ads.
    private struct RegionScoring {
        let totalRegions: Int
        let caughtAds: [GroundTruthAd]
        let missedAds: [GroundTruthAd]
        let recall: Double
        let precision: Double
        let adSecondCoverage: Double
        let orphanRegions: [RegionFeatureBundle]
    }

    private static func scoreRegions(
        _ bundles: [RegionFeatureBundle]
    ) -> RegionScoring {
        let groundTruth = ConanFanhausenRevisitedFixture.groundTruthAds
        let totalAdSeconds = groundTruth.reduce(0.0) { $0 + $1.duration }

        var caught: [GroundTruthAd] = []
        var missed: [GroundTruthAd] = []
        for ad in groundTruth {
            let overlaps = bundles.contains { b in
                // ±10s grace to match the lexical recall rule in the
                // existing Phase 2 benchmark.
                intervalsOverlap(
                    b.region.startTime, b.region.endTime,
                    ad.startTime - 10, ad.endTime + 10
                )
            }
            if overlaps { caught.append(ad) } else { missed.append(ad) }
        }
        let recall = groundTruth.isEmpty
            ? 0.0
            : Double(caught.count) / Double(groundTruth.count)

        // Region precision: of all produced regions, how many overlap
        // any ground-truth ad (no grace — strict overlap with the actual
        // ad span). This catches Phase 4 over-detection.
        let truePositiveRegions = bundles.filter { b in
            groundTruth.contains { ad in
                intervalsOverlap(
                    b.region.startTime, b.region.endTime,
                    ad.startTime, ad.endTime
                )
            }
        }
        let precision = bundles.isEmpty
            ? 0.0
            : Double(truePositiveRegions.count) / Double(bundles.count)

        // Ad-second coverage: union of (region ∩ ad) intervals divided
        // by total ad seconds.
        var totalCovered = 0.0
        for ad in groundTruth {
            var intervals: [(Double, Double)] = []
            for b in bundles {
                let s = max(ad.startTime, b.region.startTime)
                let e = min(ad.endTime, b.region.endTime)
                if e > s { intervals.append((s, e)) }
            }
            intervals.sort { $0.0 < $1.0 }
            var unioned = 0.0
            var cursor = -Double.infinity
            var curEnd = -Double.infinity
            for (s, e) in intervals {
                if s > curEnd {
                    if curEnd > cursor { unioned += curEnd - cursor }
                    cursor = s
                    curEnd = e
                } else {
                    curEnd = max(curEnd, e)
                }
            }
            if curEnd > cursor { unioned += curEnd - cursor }
            totalCovered += unioned
        }
        let adSecondCoverage = totalAdSeconds > 0 ? totalCovered / totalAdSeconds : 0.0

        // Orphan regions: produced regions that don't overlap any
        // ground-truth ad (strict overlap, no grace). These are the
        // false positives the user wants to eyeball.
        let orphans = bundles.filter { b in
            !groundTruth.contains { ad in
                intervalsOverlap(
                    b.region.startTime, b.region.endTime,
                    ad.startTime, ad.endTime
                )
            }
        }

        return RegionScoring(
            totalRegions: bundles.count,
            caughtAds: caught,
            missedAds: missed,
            recall: recall,
            precision: precision,
            adSecondCoverage: adSecondCoverage,
            orphanRegions: orphans
        )
    }

    private static func describeOrigins(_ origins: ProposedRegionOrigins) -> String {
        var parts: [String] = []
        if origins.contains(.lexical)        { parts.append("lex") }
        if origins.contains(.acoustic)       { parts.append("ac") }
        if origins.contains(.sponsor)        { parts.append("sp") }
        if origins.contains(.fingerprint)    { parts.append("fp") }
        if origins.contains(.foundationModel){ parts.append("fm") }
        return parts.isEmpty ? "-" : parts.joined(separator: "+")
    }

    private static func describeBundle(_ b: RegionFeatureBundle) -> String {
        let span = "\(ts(b.region.startTime))-\(ts(b.region.endTime))"
        let ords = "\(b.region.firstAtomOrdinal)..\(b.region.lastAtomOrdinal)"
        let lex = String(format: "%.2f", b.lexicalScore)
        let fmStrength = String(format: "%.2f", b.region.fmConsensusStrength.value)
        return "[\(span)] atoms=\(ords) origins=\(describeOrigins(b.region.origins)) lex=\(lex) hits=\(b.lexicalHitCount) fm=\(fmStrength)"
    }

    // MARK: - Test

    @Test("Phase 4 region pipeline shadow benchmark on real transcript")
    func phase4RegionPipelineBenchmark() async throws {
        let store = try await Self.makeBenchmarkStore()
        let chunks = ConanFanhausenRevisitedFixture.parseChunks()
        let assetId = ConanFanhausenRevisitedFixture.assetId
        let episodeId = ConanFanhausenRevisitedFixture.episodeId
        let duration = ConanFanhausenRevisitedFixture.duration

        try await store.insertAsset(Self.makeBenchmarkAsset(id: assetId, episodeId: episodeId))
        try await store.insertTranscriptChunks(chunks)
        try await store.insertFeatureWindows(
            Self.buildBenchmarkFeatureWindows(assetId: assetId, duration: duration)
        )

        let observer = RegionShadowObserver()

        // Disable the Phase 3 FM shadow path for this benchmark — we want
        // to exercise the Phase 4 shadow phase (step 10 of runBackfill)
        // without needing an FM runner factory. Phase 4's `fmWindows` input
        // will be empty, which mirrors what production sees today.
        let config = AdDetectionConfig(
            candidateThreshold: 0.40,
            confirmationThreshold: 0.70,
            suppressionThreshold: 0.25,
            hotPathLookahead: 90.0,
            detectorVersion: "phase4-bench",
            fmBackfillMode: .disabled
        )

        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: config,
            podcastProfile: nil,
            regionShadowObserver: observer
        )

        try await detector.runBackfill(
            chunks: chunks,
            analysisAssetId: assetId,
            podcastId: ConanFanhausenRevisitedFixture.podcastTitle,
            episodeDuration: duration
        )

        let bundles = (await observer.latestBundles(for: assetId)) ?? []
        let scoring = Self.scoreRegions(bundles)

        let groundTruth = ConanFanhausenRevisitedFixture.groundTruthAds
        let totalAdSeconds = groundTruth.reduce(0.0) { $0 + $1.duration }

        // ----- Print: header -----
        print("\n=== Phase 4 Region Shadow Benchmark ===")
        print("Bundles recorded: \(bundles.count)")
        print("Total regions: \(scoring.totalRegions)")
        print("Total ground-truth ad seconds: \(Int(totalAdSeconds))s across \(groundTruth.count) ads")

        // ----- Print: per-ad alignment -----
        print("\n--- Ground truth alignment ---")
        for ad in groundTruth {
            let overlapping = bundles.filter { b in
                Self.intervalsOverlap(
                    b.region.startTime, b.region.endTime,
                    ad.startTime, ad.endTime
                )
            }
            let mark = overlapping.isEmpty ? "[MISS]" : "[HIT ]"
            let span = "\(Self.ts(ad.startTime)) - \(Self.ts(ad.endTime))"
            print("\(mark) [\(span)] (\(ad.advertiser))")
            for b in overlapping {
                print("        \(Self.describeBundle(b))")
            }
        }

        // ----- Print: orphan / false-positive regions -----
        print("\n--- Orphan regions (no ground-truth overlap) ---")
        if scoring.orphanRegions.isEmpty {
            print("  (none)")
        } else {
            for b in scoring.orphanRegions {
                print("  \(Self.describeBundle(b))")
            }
        }

        // ----- Print: scoring summary -----
        let totalAds = groundTruth.count
        let truePositiveRegionCount = scoring.totalRegions - scoring.orphanRegions.count
        print("\n--- Scoring ---")
        print("Region recall:           \(Int(scoring.recall * 100))% (\(scoring.caughtAds.count)/\(totalAds))")
        print("Region precision:        \(Int(scoring.precision * 100))% (\(truePositiveRegionCount)/\(scoring.totalRegions))")
        print("Region ad-second coverage: \(Int(scoring.adSecondCoverage * 100))%")
        print("  Caught: \(scoring.caughtAds.map(\.advertiser).joined(separator: ", "))")
        print("  Missed: \(scoring.missedAds.map(\.advertiser).joined(separator: ", "))")

        // ----- Lenient assertions -----
        // Pin only that the shadow phase actually ran and produced
        // something. The user will eyeball the printed numbers and decide
        // whether the underlying quality is acceptable.
        #expect(bundles.count > 0, "Phase 4 shadow phase should record at least one bundle")
        #expect(scoring.totalRegions > 0, "Phase 4 should produce at least one region")
    }
}
