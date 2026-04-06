// RealEpisodeBenchmarkTests.swift
// Benchmark Phase 2 components against a real podcast episode with
// ground-truth ad annotations. Re-run after code changes to track
// precision/recall regressions.
//
// These are BENCHMARK tests, not pass/fail gates. They print numbers
// to the test log so we can compare detector quality across commits.
// Hard assertions are intentionally lenient.

import Foundation
import Testing
@testable import Playhead

@Suite("Real Episode Benchmark - Conan Fanhausen Revisited")
struct RealEpisodeBenchmarkTests {

    // MARK: - Helpers

    private static func ts(_ seconds: Double) -> String {
        let s = Int(seconds)
        return String(format: "%d:%02d", s / 60, s % 60)
    }

    /// Tracks temp directories created for the benchmark's full-pipeline run.
    private static let benchStoreDirs = TestTempDirTracker()

    /// Creates a temp-directory-backed AnalysisStore for the benchmark.
    /// Duplicated from PipelineIntegrationTests.makeIntegrationStore() which is
    /// file-private there; we don't want to change its visibility.
    private static func makeBenchmarkStore() async throws -> AnalysisStore {
        let dir = try makeTempDir(prefix: "PlayheadBenchmark")
        benchStoreDirs.track(dir)
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()
        return store
    }

    private static func makeBenchmarkAsset(id: String, episodeId: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: episodeId,
            assetFingerprint: "bench-fp-\(id)",
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

    /// Builds realistic 1s synthetic feature windows spanning [0, duration).
    /// Values approximate the real Conan episode's acoustic stats (mean rms
    /// ~0.151, mean spectralFlux ~0.5). Pause probability is moderate because
    /// the real feature extractor produces ~0 on this audio (known bug); this
    /// will understate the classifier's acoustic boost at ad boundaries, but
    /// still lets us verify whether the lexical-only path clears the 0.40
    /// candidate threshold end-to-end.
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

    /// Sets up a fresh temp store, seeds it with the fixture asset, chunks,
    /// and synthetic feature windows, then runs the real AdDetectionService
    /// hot path end-to-end. Returns both the detected AdWindows and the
    /// chunks used (so callers can correlate).
    private static func runFullPipeline() async throws -> (chunks: [TranscriptChunk], detected: [AdWindow]) {
        let store = try await makeBenchmarkStore()
        let chunks = ConanFanhausenRevisitedFixture.parseChunks()
        let assetId = ConanFanhausenRevisitedFixture.assetId
        let episodeId = ConanFanhausenRevisitedFixture.episodeId
        let duration = ConanFanhausenRevisitedFixture.duration

        let asset = makeBenchmarkAsset(id: assetId, episodeId: episodeId)
        try await store.insertAsset(asset)
        try await store.insertTranscriptChunks(chunks)

        let featureWindows = buildBenchmarkFeatureWindows(
            assetId: assetId, duration: duration
        )
        try await store.insertFeatureWindows(featureWindows)

        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: .default,
            podcastProfile: nil
        )

        let detected = try await detector.runHotPath(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration
        )
        return (chunks, detected)
    }

    /// Compute AdWindow recall and ad-second coverage for a list of detected
    /// windows versus the fixture's ground truth. Uses a ±10s grace for recall
    /// (matching the existing lexical benchmark) and an exact union-of-
    /// intervals intersection for ad-second coverage.
    private static func scoreDetections(
        _ detected: [AdWindow]
    ) -> (caught: [GroundTruthAd], missed: [GroundTruthAd], recall: Double, adSecondCoverage: Double) {
        let groundTruth = ConanFanhausenRevisitedFixture.groundTruthAds
        var caught: [GroundTruthAd] = []
        var missed: [GroundTruthAd] = []
        for ad in groundTruth {
            let overlaps = detected.contains { w in
                w.startTime < ad.endTime + 10 && w.endTime > ad.startTime - 10
            }
            if overlaps { caught.append(ad) } else { missed.append(ad) }
        }
        let recall = groundTruth.isEmpty
            ? 0.0
            : Double(caught.count) / Double(groundTruth.count)

        // Ad-second coverage via unioned intersection per ad.
        let totalAdSeconds = groundTruth.reduce(0.0) { $0 + $1.duration }
        var totalCovered = 0.0
        for ad in groundTruth {
            var intervals: [(Double, Double)] = []
            for w in detected {
                let s = max(ad.startTime, w.startTime)
                let e = min(ad.endTime, w.endTime)
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
        return (caught, missed, recall, adSecondCoverage)
    }

    // MARK: - Tests

    @Test("Phase 2 evidence catalog coverage on real transcript")
    func evidenceCatalogCoverage() throws {
        let chunks = ConanFanhausenRevisitedFixture.parseChunks()
        #expect(chunks.count > 0, "Fixture should parse to non-empty chunks")

        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
            normalizationHash: "benchmark-v1",
            sourceHash: "whisperkit-v1"
        )

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
            transcriptVersion: version.transcriptVersion
        )

        print("\n=== Evidence Catalog Benchmark ===")
        print("Chunks parsed: \(chunks.count)")
        print("Atoms produced: \(atoms.count)")
        print("Total evidence entries: \(catalog.entries.count)")
        for entry in catalog.entries {
            print("  [\(Self.ts(entry.startTime))] \(entry.category.rawValue): \"\(entry.matchedText)\"")
        }

        // For each ground truth ad, check if we have ANY evidence entry in the time range
        // (with a small grace window since timing is interpolated).
        var caught: [GroundTruthAd] = []
        var missed: [GroundTruthAd] = []
        for ad in ConanFanhausenRevisitedFixture.groundTruthAds {
            let hasEvidence = catalog.entries.contains { entry in
                entry.startTime >= ad.startTime - 5 && entry.startTime <= ad.endTime + 5
            }
            if hasEvidence { caught.append(ad) } else { missed.append(ad) }
        }

        let total = ConanFanhausenRevisitedFixture.groundTruthAds.count
        let recall = total > 0 ? Double(caught.count) / Double(total) : 0
        print("\nEvidence catalog recall: \(Int(recall * 100))% (\(caught.count)/\(total))")
        print("  Caught: \(caught.map(\.advertiser).joined(separator: ", "))")
        print("  Missed: \(missed.map(\.advertiser).joined(separator: ", "))")

        print("\nKnown non-ad signals (false-positive watch list):")
        for nonAd in ConanFanhausenRevisitedFixture.knownFalsePositives {
            let fires = catalog.entries.contains { entry in
                entry.startTime >= nonAd.startTime - 3 && entry.startTime <= nonAd.endTime + 3
            }
            print("  [\(fires ? "FIRES" : "clean")] \(nonAd.description)")
        }

        // Hard assertion: matches current Phase 2 baseline (recall = 0.50, 2/4
        // ground-truth ads have an evidence entry within ±5s). If this drops,
        // it's a real regression and the baseline should not be weakened to
        // make this pass.
        #expect(caught.count >= 2,
                "Evidence catalog should catch at least 2/4 ground-truth ads (current baseline: 50% recall). Got \(caught.count)/\(total).")
    }

    @Test("LexicalScanner coverage on real transcript")
    func lexicalScannerCoverage() throws {
        let chunks = ConanFanhausenRevisitedFixture.parseChunks()
        #expect(chunks.count > 0)

        let scanner = LexicalScanner()
        let candidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId
        )

        print("\n=== LexicalScanner Benchmark ===")
        print("Total candidates: \(candidates.count)")
        for c in candidates {
            print("  [\(Self.ts(c.startTime))-\(Self.ts(c.endTime))] conf=\(String(format: "%.2f", c.confidence)) hits=\(c.hitCount) \"\(c.evidenceText)\"")
        }

        var caught: [GroundTruthAd] = []
        var missed: [GroundTruthAd] = []
        for ad in ConanFanhausenRevisitedFixture.groundTruthAds {
            let overlap = candidates.contains { cand in
                cand.startTime < ad.endTime + 10 && cand.endTime > ad.startTime - 10
            }
            if overlap { caught.append(ad) } else { missed.append(ad) }
        }

        let total = ConanFanhausenRevisitedFixture.groundTruthAds.count
        let recall = total > 0 ? Double(caught.count) / Double(total) : 0
        print("\nLexicalScanner recall: \(Int(recall * 100))% (\(caught.count)/\(total))")
        print("  Caught: \(caught.map(\.advertiser).joined(separator: ", "))")
        print("  Missed: \(missed.map(\.advertiser).joined(separator: ", "))")

        // Hard assertions tied to the current Phase 2 baseline
        // (lexicalCandidateRecall = 0.75, lexicalCandidateCount = 3).
        // If these fail, do not weaken — investigate the regression.
        #expect(caught.count >= 3,
                "LexicalScanner should overlap at least 3/4 ground-truth ads within ±10s (current baseline: 75% recall). Got \(caught.count)/\(total).")
        #expect(candidates.count >= 2 && candidates.count <= 6,
                "Lexical candidate count should fall in [2, 6] (current baseline: 3). Got \(candidates.count).")
    }

    @Test("Boundary span coverage on real transcript")
    func boundarySpanCoverage() throws {
        let chunks = ConanFanhausenRevisitedFixture.parseChunks()
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
            normalizationHash: "benchmark-v1",
            sourceHash: "whisperkit-v1"
        )
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
            transcriptVersion: version.transcriptVersion
        )

        // For each ad, measure how much of its span is covered by evidence entries.
        // e.g. CVS spans 0:00-0:26 but URL only hits at 0:21-0:26 = ~19% coverage.
        print("\n=== Boundary Span Coverage ===")
        for ad in ConanFanhausenRevisitedFixture.groundTruthAds {
            let evidenceInRange = catalog.entries.filter { entry in
                entry.startTime >= ad.startTime - 5 && entry.startTime <= ad.endTime + 5
            }
            if evidenceInRange.isEmpty {
                print("  [MISSED] \(ad.advertiser): no evidence in range (ad=\(Self.ts(ad.startTime))-\(Self.ts(ad.endTime)))")
                continue
            }
            let firstHit = evidenceInRange.map(\.startTime).min() ?? ad.startTime
            let lastHit = evidenceInRange.map(\.endTime).max() ?? ad.endTime
            let hitSpan = max(0, lastHit - firstHit)
            let adSpan = ad.duration
            let coverage = adSpan > 0 ? hitSpan / adSpan : 0
            print("  [\(ad.advertiser)] ad=\(String(format: "%.0fs", adSpan)) hit=\(String(format: "%.0fs", hitSpan)) coverage=\(Int(coverage * 100))%")
        }

        // Soft assertion by design: span coverage is intentionally low at this
        // phase. Evidence entries are point-in-time hits, not span detectors —
        // CVS hits at 0:21 inside a 0:00-0:26 ad gives ~19% coverage at best.
        // Phase 4+ (boundary refinement) is where this metric will get teeth.
        // Until then we just assert that the test ran end-to-end on real data.
        #expect(catalog.entries.count >= 0)
    }

    @Test("Full AdDetectionService hot path on real transcript")
    func fullAdDetectionHotPath() async throws {
        let (_, detected) = try await Self.runFullPipeline()

        print("\n=== Full AdDetectionService Hot Path ===")
        print("Detected AdWindows: \(detected.count)")
        for w in detected {
            let conf = String(format: "%.2f", w.confidence)
            print("  [\(Self.ts(w.startTime))-\(Self.ts(w.endTime))] conf=\(conf) decision=\(w.decisionState) boundary=\(w.boundaryState)")
            if let ev = w.evidenceText {
                print("      evidence: \"\(ev.prefix(80))\"")
            }
        }

        let score = Self.scoreDetections(detected)
        let total = ConanFanhausenRevisitedFixture.groundTruthAds.count
        print("\nAdWindow recall: \(Int(score.recall * 100))% (\(score.caught.count)/\(total))")
        print("  Caught: \(score.caught.map(\.advertiser).joined(separator: ", "))")
        print("  Missed: \(score.missed.map(\.advertiser).joined(separator: ", "))")
        print("Ad-second coverage: \(Int(score.adSecondCoverage * 100))% of \(Int(ConanFanhausenRevisitedFixture.groundTruthAds.reduce(0.0) { $0 + $1.duration }))s total ad content")

        // Hard assertion: detected AdWindow count should stay within a sane
        // range. Current baseline is 0 (Phase 2 has no AdWindow emission yet);
        // when Phase 3+ starts producing windows, this allows headroom up to
        // 5 before the test fails. A regression that suddenly emits 50 windows
        // would trip this loudly.
        #expect(detected.count >= 0 && detected.count <= 5,
                "Detected AdWindow count should be in [0, 5] (current baseline: 0). Got \(detected.count).")
    }

    @Test("Compare current run to baseline history")
    func compareToBaselineHistory() async throws {
        let chunks = ConanFanhausenRevisitedFixture.parseChunks()
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
            normalizationHash: "benchmark-v1",
            sourceHash: "whisperkit-v1"
        )
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId,
            transcriptVersion: version.transcriptVersion
        )
        let scanner = LexicalScanner()
        let candidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: ConanFanhausenRevisitedFixture.assetId
        )

        let groundTruth = ConanFanhausenRevisitedFixture.groundTruthAds
        let totalAds = groundTruth.count
        let totalAdSeconds = groundTruth.reduce(0.0) { $0 + $1.duration }

        // Evidence catalog recall (with ±5s tolerance), per-ad catch list.
        var caughtByEvidence: Set<String> = []
        for ad in groundTruth {
            let hit = catalog.entries.contains { entry in
                entry.startTime >= ad.startTime - 5 && entry.startTime <= ad.endTime + 5
            }
            if hit { caughtByEvidence.insert(ad.id) }
        }
        let evidenceCatalogRecall = totalAds > 0
            ? Double(caughtByEvidence.count) / Double(totalAds)
            : 0.0

        // Evidence catalog precision: how many entries fall inside (or near) a real ad.
        let truePositiveEntries = catalog.entries.filter { entry in
            groundTruth.contains { ad in
                entry.startTime >= ad.startTime - 5 && entry.startTime <= ad.endTime + 5
            }
        }
        let evidenceCatalogPrecision = catalog.entries.isEmpty
            ? 0.0
            : Double(truePositiveEntries.count) / Double(catalog.entries.count)

        // Lexical candidate recall (with ±10s tolerance, matching existing test).
        var caughtByLexical: Set<String> = []
        for ad in groundTruth {
            let hit = candidates.contains { cand in
                cand.startTime < ad.endTime + 10 && cand.endTime > ad.startTime - 10
            }
            if hit { caughtByLexical.insert(ad.id) }
        }
        let lexicalCandidateRecall = totalAds > 0
            ? Double(caughtByLexical.count) / Double(totalAds)
            : 0.0

        // Run the full production AdDetectionService hot path so we can
        // populate real AdWindow metrics (replacing the hardcoded zeros that
        // used to live here).
        let (_, detectedWindows) = try await Self.runFullPipeline()
        let detectionScore = Self.scoreDetections(detectedWindows)
        let adWindowRecall = detectionScore.recall
        let adWindowCount = detectedWindows.count
        let adSecondCoverageFromWindows = detectionScore.adSecondCoverage

        print("\n=== Full pipeline (baseline-compare) ===")
        print("Detected AdWindows: \(adWindowCount)")
        for w in detectedWindows {
            let conf = String(format: "%.2f", w.confidence)
            print("  [\(Self.ts(w.startTime))-\(Self.ts(w.endTime))] conf=\(conf) decision=\(w.decisionState)")
            if let ev = w.evidenceText { print("      evidence: \"\(ev.prefix(80))\"") }
        }
        print("AdWindow recall: \(Int(adWindowRecall * 100))%")
        print("Ad-second coverage (from AdWindows): \(Int(adSecondCoverageFromWindows * 100))%")
        print("  Caught: \(detectionScore.caught.map(\.advertiser).joined(separator: ", "))")
        print("  Missed: \(detectionScore.missed.map(\.advertiser).joined(separator: ", "))")

        // Per-ad span coverage. Detection regions:
        //   - each evidence entry "covers" 1 second at its startTime
        //   - each lexical candidate covers its full [startTime, endTime)
        // For each ad, compute union overlap / ad duration.
        var perAdSpanCoverage: [String: Double] = [:]
        var totalCoveredSeconds = 0.0
        for ad in groundTruth {
            // Build the set of covered intervals intersected with [ad.startTime, ad.endTime].
            var intervals: [(Double, Double)] = []
            for entry in catalog.entries {
                let s = max(ad.startTime, entry.startTime)
                let e = min(ad.endTime, entry.startTime + 1.0)
                if e > s { intervals.append((s, e)) }
            }
            for cand in candidates {
                let s = max(ad.startTime, cand.startTime)
                let e = min(ad.endTime, cand.endTime)
                if e > s { intervals.append((s, e)) }
            }
            // Union the intervals.
            intervals.sort { $0.0 < $1.0 }
            var unioned: Double = 0
            var cursor: Double = -.infinity
            var curEnd: Double = -.infinity
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
            let coverage = ad.duration > 0 ? unioned / ad.duration : 0.0
            perAdSpanCoverage[ad.id] = coverage
            totalCoveredSeconds += unioned
        }
        // Retain the legacy evidence/lexical-union coverage for diagnostics,
        // but the `current` benchmark below reports real AdWindow coverage.
        _ = totalCoveredSeconds  // diagnostic only
        let adSecondCoverage = adSecondCoverageFromWindows

        // Weighted recall: each ad's catch (by evidence OR lexical) weighted by skipConfidence.
        let weightSum = groundTruth.reduce(0.0) { $0 + $1.skipConfidence }
        let weightedHits = groundTruth.reduce(0.0) { acc, ad in
            let caught = caughtByEvidence.contains(ad.id) || caughtByLexical.contains(ad.id)
            return acc + (caught ? ad.skipConfidence : 0.0)
        }
        let weightedRecall = weightSum > 0 ? weightedHits / weightSum : 0.0

        // AdWindow metrics are measured from the real production
        // AdDetectionService.runHotPath pipeline run above. Ad-second
        // coverage reflects the union of detected AdWindow intervals
        // intersected with ground-truth ad spans (not the evidence/lexical
        // union used previously).
        let current = DetectionBenchmark(
            label: "current",
            measuredOn: ISO8601DateFormatter().string(from: Date()),
            commitHash: nil,
            totalAds: totalAds,
            totalAdSeconds: totalAdSeconds,
            adWindowCount: adWindowCount,
            adWindowRecall: adWindowRecall,
            adSecondCoverage: adSecondCoverage,
            evidenceCatalogEntries: catalog.entries.count,
            evidenceCatalogRecall: evidenceCatalogRecall,
            evidenceCatalogPrecision: evidenceCatalogPrecision,
            lexicalCandidateCount: candidates.count,
            lexicalCandidateRecall: lexicalCandidateRecall,
            weightedRecall: weightedRecall,
            perAdSpanCoverage: perAdSpanCoverage
        )

        let baseline = DetectionBenchmarkHistory.latest

        // Side-by-side table.
        func pct(_ x: Double) -> String { String(format: "%5.1f%%", x * 100) }
        func col(_ s: String, _ width: Int) -> String {
            s.padding(toLength: width, withPad: " ", startingAt: 0)
        }
        print("\n=== Benchmark vs \(baseline.label) (\(baseline.measuredOn)) ===")
        print(col("metric", 28) + col("baseline", 12) + col("current", 12))
        print(String(repeating: "-", count: 52))
        let rows: [(String, Double, Double)] = [
            ("AdWindow recall",          baseline.adWindowRecall,          current.adWindowRecall),
            ("Ad-second coverage",       baseline.adSecondCoverage,        current.adSecondCoverage),
            ("Evidence catalog recall",  baseline.evidenceCatalogRecall,   current.evidenceCatalogRecall),
            ("Evidence precision",       baseline.evidenceCatalogPrecision, current.evidenceCatalogPrecision),
            ("Lexical recall",           baseline.lexicalCandidateRecall,  current.lexicalCandidateRecall),
            ("Weighted recall",          baseline.weightedRecall,          current.weightedRecall),
        ]
        for (name, b, c) in rows {
            print(col(name, 28) + col(pct(b), 12) + col(pct(c), 12))
        }
        print("Evidence entries: baseline=\(baseline.evidenceCatalogEntries) current=\(current.evidenceCatalogEntries)")
        print("Lexical candidates: baseline=\(baseline.lexicalCandidateCount) current=\(current.lexicalCandidateCount)")

        print("\nPer-ad span coverage:")
        let ids = Array(Set(baseline.perAdSpanCoverage.keys).union(current.perAdSpanCoverage.keys)).sorted()
        for id in ids {
            let b = baseline.perAdSpanCoverage[id] ?? 0
            let c = current.perAdSpanCoverage[id] ?? 0
            print("  \(id.padding(toLength: 22, withPad: " ", startingAt: 0)) baseline=\(pct(b)) current=\(pct(c))")
        }

        let delta = current.compareTo(baseline)
        print("")
        print(delta.summary)

        // Tight regression gates: every metric in DetectionBenchmark is
        // asserted with a 5pp tolerance against the latest baseline. The
        // baseline is now an honest measurement of the production pipeline
        // (post C1/C2 fix), so any drift greater than 5pp is a real signal.
        #expect(delta.adWindowRecallDelta >= -0.05,
                "AdWindow recall regressed by more than 5pp vs \(baseline.label) (delta=\(String(format: "%.3f", delta.adWindowRecallDelta)))")
        #expect(delta.adSecondCoverageDelta >= -0.05,
                "Ad-second coverage regressed by more than 5pp vs \(baseline.label) (delta=\(String(format: "%.3f", delta.adSecondCoverageDelta)))")
        #expect(delta.evidenceCatalogRecallDelta >= -0.05,
                "Evidence catalog recall regressed by more than 5pp vs \(baseline.label) (delta=\(String(format: "%.3f", delta.evidenceCatalogRecallDelta)))")
        #expect(delta.evidenceCatalogPrecisionDelta >= -0.05,
                "Evidence catalog precision regressed by more than 5pp vs \(baseline.label) (delta=\(String(format: "%.3f", delta.evidenceCatalogPrecisionDelta)))")
        #expect(delta.lexicalCandidateRecallDelta >= -0.05,
                "Lexical candidate recall regressed by more than 5pp vs \(baseline.label) (delta=\(String(format: "%.3f", delta.lexicalCandidateRecallDelta)))")
        #expect(delta.weightedRecallDelta >= -0.05,
                "Weighted recall regressed by more than 5pp vs \(baseline.label) (delta=\(String(format: "%.3f", delta.weightedRecallDelta)))")
    }
}
