// CatalogLedgerHullCorpusEvalTests.swift
// playhead-0u3e: the corpus lane that measures what the coverage HULL was worth
// to the FUSION LEDGER — the site whose output reaches persisted scoring.
//
// WHY A LANE AND NOT A FIXTURE
// ----------------------------
// The claim is about REPEATS: one sponsor read twice in an episode collapses to
// ONE `EvidenceEntry` whose `firstTime`/`lastTime` hull then spans everything
// between. A synthetic fixture contains only the repeats its author thought to
// write, so any rate it produces is a property of the fixture. The population
// that matters is a real device corpus with the device's own persisted
// `ad_windows` rows, where a repeat is either a second ad break or a host saying
// "like I said, betterhelp.com" and nobody chose which.
//
// WHY THE PERSISTED ROWS ARE THE RIGHT DENOMINATOR HERE, unlike rty3's lane
// ------------------------------------------------------------------------
// `buildFusionAdWindow` persists `startTime: span.startTime, endTime:
// span.endTime` — for a `detection-v1` row the window bounds ARE the refined
// span the ledger was built over. So the two arms below are evaluated on the
// same interval `buildCatalogLedgerEntries` actually saw, not on a proxy for it.
// (Two exceptions are named under WHAT IT CANNOT SEE.)
//
// WHAT IT RUNS
// ------------
// Production code, end to end, per asset:
//
//   TranscriptChunkCanonicalizer.canonicalize
//   -> TranscriptAtomizer.atomize
//   -> EvidenceCatalogBuilder.build
//
// then, per persisted `ad_windows` row, both arms of the catalog LEDGER
// selector and the weight each produces under the production
// `FusionWeightConfig`:
//
//   HULL arm        the expression `buildCatalogLedgerEntries` carried before
//                   playhead-0u3e, restated verbatim because the fix deletes
//                   it — half-open, on `coverageStartTime`/`coverageEndTime`.
//   OCCURRENCE arm  `EvidenceEntry.locatedInTimeWindow(start:end:)`, the SHIPPED
//                   primitive, called rather than restated.
//
// The weight formula is read from the production expression's own constant and
// the production cap (`FusionWeightConfig().catalogCap`), not typed as 0.2.
//
// WHAT IT CANNOT SEE — read this before quoting a confidence number
// -----------------------------------------------------------------
// * THE REST OF THE LEDGER IS NOT IN THE EXPORT. Fusion confidence is
//   `min(1, Σ calibrated weights) × durationMultiplier`, and the corpus carries
//   neither the FM scan results, the classifier scores nor the acoustic feature
//   windows. So this lane measures the CATALOG term exactly and can only BOUND
//   what it does to the fused number: the drop is `Δw × m` with
//   `m ∈ [floorMultiplier, peakMultiplier]` of `DurationPrior` (0.75…1.10), and
//   for a row persisted at exactly 1.0 the sum was clamped and the post-fix
//   value is not determined at all — only bounded above.
// * ONLY `detection-v1` ROWS REACH THIS SITE. `userCorrection`,
//   `semantic-sweep-v1` and `pod-continuation-v1` rows are composed elsewhere
//   and never call `buildCatalogLedgerEntries`. They are counted and excluded,
//   never silently dropped.
// * A ROW THE FIX WOULD DELETE IS STILL IN THE DENOMINATOR. Tightening can only
//   REMOVE weight, so it can only remove windows, never add them — every window
//   the fix could affect is already persisted. But a span that fusion REJECTED
//   pre-fix is invisible here, which is fine in this direction and would not be
//   in the other.
// * `PreRollStartClamp` can rewrite a persisted `startTime` to 0.0 after the
//   ledger was built, so for a clamped row the interval measured here is WIDER
//   than the one the ledger saw. Rows starting at exactly 0.0 are counted and
//   reported separately for that reason.
//
// STAGING: `PLAYHEAD_0U3E_CORPUS` (or `TEST_RUNNER_…`, or the `PLAYHEAD_RTY3_*`
// / `PLAYHEAD_04RX_*` variables — same exporter, same export shape, same pull)
// points at a JSON export of a device `analysis.sqlite`
// (`scripts/l2f-04rx-export-corpus.py`). The export is a copy of a device pull
// and is NOT in the repo, so with the variables unset this SKIPS. Set
// `PLAYHEAD_0U3E_OUT` to write the full per-window report.

import Foundation
import XCTest
@testable import Playhead

final class CatalogLedgerHullCorpusEvalTests: XCTestCase {

    /// The export shape is 04rx's, deliberately not a second copy of it: these
    /// lanes read the same file produced by the same exporter, and a duplicated
    /// `Decodable` is how they come to disagree about a field.
    typealias Corpus = RepeatedOccurrenceAnchoringCorpusEvalTests.Corpus

    /// The detector version `buildFusionAdWindow` stamps. Rows carrying anything
    /// else were composed by a path that never builds a catalog ledger entry.
    static let fusionDetectorVersion = "detection-v1"

    // MARK: - Report shape

    struct WindowRow: Encodable {
        let assetId: String
        let title: String?
        let startTime: Double
        let endTime: Double
        let decisionState: String
        let eligibilityGate: String?
        let detectorVersion: String
        /// Persisted `ad_windows.confidence` — `decision.proposalConfidence`.
        let persistedConfidence: Double
        /// Entry counts each arm feeds to the weight formula.
        let hullCount: Int
        let occurrenceCount: Int
        /// Selected by the hull, with no mention inside this window.
        let hullOnlyRefs: [Int]
        let hullOnlyTexts: [String]
        /// How far the nearest mention of each hull-only entry lies OUTSIDE this
        /// window, in seconds. The size of the error, per entry.
        let hullOnlyNearestMissSeconds: [Double]
        /// Selected by the CLOSED occurrence test and not by the half-open hull
        /// test — the boundary-convention delta the primitive swap introduces.
        let boundaryOnlyRefs: [Int]
        /// A witness for each of those, so the claim "exact float equality on an
        /// edge" is checkable rather than asserted: the entry's hull and the
        /// mention that touches the edge.
        let boundaryOnlyDetail: [String]
        let hullWeight: Double
        let occurrenceWeight: Double
        let weightDelta: Double
        /// True when the fix removes the `.catalog` entry entirely, so the span
        /// also loses a corroborating KIND, not just score.
        let losesCatalogKind: Bool
        /// `[low, high]` bound on the fused-confidence drop. See the header.
        let confidenceDropBound: [Double]
        /// The persisted confidence is exactly 1.0, so the ledger sum clamped
        /// and the post-fix value is bounded above but not determined.
        let confidenceWasClamped: Bool
        /// `startTime == 0`, so `PreRollStartClamp` may have widened this row.
        let mayBePreRollClamped: Bool
    }

    struct AssetReport: Encodable {
        let assetId: String
        let title: String?
        let durationSec: Double?
        let atoms: Int
        let catalogEntries: Int
        let repeatedEntries: Int
        let widestHullSeconds: Double
        let windows: [WindowRow]
        let excludedWindowsByDetectorVersion: [String: Int]
    }

    struct Report: Encodable {
        let source: String
        let catalogCap: Double
        let assets: Int
        let fusionWindows: Int
        let excludedWindows: Int
        let windowsWithHullOnlyEntry: Int
        let windowsLosingCatalogKind: Int
        let totalHullWeight: Double
        let totalOccurrenceWeight: Double
        let perAsset: [AssetReport]
    }

    // MARK: - The two arms

    /// The selector `buildCatalogLedgerEntries` used before playhead-0u3e,
    /// restated so the control arm exists after the fix deletes it. VERBATIM,
    /// including the HALF-OPEN comparisons:
    ///
    ///     entry.coverageStartTime < span.endTime && entry.coverageEndTime > span.startTime
    ///
    /// `coverageStartTime`/`coverageEndTime` are `firstTime`/`lastTime`, so this
    /// is the hull and that is the whole point of the arm.
    static func hullArmEntries(catalog: EvidenceCatalog, start: Double, end: Double) -> [EvidenceEntry] {
        catalog.entries.filter { entry in
            entry.coverageStartTime < end && entry.coverageEndTime > start
        }
    }

    /// The shipped selector, called rather than restated.
    static func occurrenceArmEntries(catalog: EvidenceCatalog, start: Double, end: Double) -> [EvidenceEntry] {
        catalog.entries.compactMap { $0.locatedInTimeWindow(start: start, end: end) }
    }

    /// The production weight expression, over an entry COUNT. Restated here for
    /// the control arm; the `0.05` and the cap are the production values, and
    /// `catalogLedgerWeightPerEntry` is the same constant the site multiplies by
    /// so the two cannot drift.
    static func catalogWeight(count: Int, cap: Double) -> Double {
        min(Double(count) * AdDetectionService.catalogLedgerWeightPerEntry * cap, cap)
    }

    // MARK: - The lane

    func testCatalogLedgerWeightOnDeviceCorpus() throws {
        let environment = ProcessInfo.processInfo.environment
        let path = ["PLAYHEAD_0U3E_CORPUS", "TEST_RUNNER_PLAYHEAD_0U3E_CORPUS",
                    "PLAYHEAD_RTY3_CORPUS", "TEST_RUNNER_PLAYHEAD_RTY3_CORPUS",
                    "PLAYHEAD_04RX_CORPUS", "TEST_RUNNER_PLAYHEAD_04RX_CORPUS"]
            .lazy.compactMap { environment[$0] }
            .first { FileManager.default.fileExists(atPath: $0) }
        guard let path else {
            throw XCTSkip(
                "catalog ledger lane needs PLAYHEAD_0U3E_CORPUS pointing at a device analysis.sqlite JSON export"
            )
        }
        let corpus = try JSONDecoder().decode(
            Corpus.self,
            from: try Data(contentsOf: URL(fileURLWithPath: path))
        )
        try XCTSkipIf(corpus.assets.isEmpty, "corpus export has no assets")

        let cap = FusionWeightConfig().catalogCap
        var perAsset: [AssetReport] = []
        for asset in corpus.assets where !asset.chunks.isEmpty {
            perAsset.append(evaluate(asset: asset, cap: cap))
        }

        let allWindows = perAsset.flatMap(\.windows)
        let withHullOnly = allWindows.filter { !$0.hullOnlyRefs.isEmpty }
        let losingKind = allWindows.filter(\.losesCatalogKind)
        let excluded = perAsset.reduce(0) { $0 + $1.excludedWindowsByDetectorVersion.values.reduce(0, +) }

        let report = Report(
            source: corpus.source,
            catalogCap: cap,
            assets: perAsset.count,
            fusionWindows: allWindows.count,
            excludedWindows: excluded,
            windowsWithHullOnlyEntry: withHullOnly.count,
            windowsLosingCatalogKind: losingKind.count,
            totalHullWeight: allWindows.reduce(0) { $0 + $1.hullWeight },
            totalOccurrenceWeight: allWindows.reduce(0) { $0 + $1.occurrenceWeight },
            perAsset: perAsset
        )

        print("[0u3e] source=\(corpus.source) catalogCap=\(cap) weightPerEntry=\(AdDetectionService.catalogLedgerWeightPerEntry)")
        print("[0u3e] assets=\(perAsset.count) fusionWindows=\(allWindows.count) excludedByDetectorVersion=\(excluded)")
        var excludedByVersion: [String: Int] = [:]
        for asset in perAsset {
            for (version, count) in asset.excludedWindowsByDetectorVersion {
                excludedByVersion[version, default: 0] += count
            }
        }
        print("[0u3e] excluded detail: " + excludedByVersion.sorted { $0.key < $1.key }
            .map { "\($0.key)=\($0.value)" }.joined(separator: " "))
        print("[0u3e] catalogEntries=\(perAsset.reduce(0) { $0 + $1.catalogEntries }) repeatedEntries=\(perAsset.reduce(0) { $0 + $1.repeatedEntries })")
        print("[0u3e] NUMERATORS over \(allWindows.count) persisted detection-v1 ad_windows rows:")
        print("[0u3e]   carrying >=1 entry no mention of which is inside the window: \(withHullOnly.count)")
        print("[0u3e]   whose catalog LEDGER WEIGHT falls:                          \(allWindows.filter { $0.weightDelta > 0 }.count)")
        print("[0u3e]   which LOSE the .catalog entry entirely (a quorum KIND):     \(losingKind.count)")
        print("[0u3e]   persisted at confidence 1.0 (ledger sum clamped, drop bounded only): \(allWindows.filter(\.confidenceWasClamped).count)")
        print("[0u3e]   startTime == 0 (PreRollStartClamp may have widened the interval):     \(allWindows.filter(\.mayBePreRollClamped).count)")
        print("[0u3e]   selected by the CLOSED occurrence test and not the half-open hull:   \(allWindows.filter { !$0.boundaryOnlyRefs.isEmpty }.count)")
        for row in allWindows where !row.boundaryOnlyRefs.isEmpty {
            for detail in row.boundaryOnlyDetail {
                print("[0u3e]     BOUNDARY \(row.assetId.prefix(8)) \(detail)")
            }
        }
        print("[0u3e] catalog ledger weight summed over rows: hull=\(String(format: "%.3f", report.totalHullWeight)) occurrence=\(String(format: "%.3f", report.totalOccurrenceWeight))")
        for gate in ["markOnly", "eligible", "blockedByUserCorrection", "unknown"] {
            let rows = allWindows.filter { ($0.eligibilityGate ?? "unknown") == gate }
            guard !rows.isEmpty else { continue }
            print("[0u3e]   gate=\(gate): windows=\(rows.count) hullOnly=\(rows.filter { !$0.hullOnlyRefs.isEmpty }.count) losingKind=\(rows.filter(\.losesCatalogKind).count)")
        }
        for row in allWindows.filter({ $0.weightDelta > 0 })
            .sorted(by: { $0.weightDelta > $1.weightDelta }).prefix(40) {
            print("[0u3e]   DROP \(row.assetId.prefix(8)) win=\(String(format: "%.0f", row.startTime))-\(String(format: "%.0f", row.endTime)) gate=\(row.eligibilityGate ?? "nil") \(row.decisionState) conf=\(String(format: "%.3f", row.persistedConfidence)) count \(row.hullCount)->\(row.occurrenceCount) w \(String(format: "%.3f", row.hullWeight))->\(String(format: "%.3f", row.occurrenceWeight)) drop∈[\(String(format: "%.3f", row.confidenceDropBound[0])),\(String(format: "%.3f", row.confidenceDropBound[1]))] kindLost=\(row.losesCatalogKind) miss=\(row.hullOnlyNearestMissSeconds.map { String(format: "%.0f", $0) }.joined(separator: ","))")
        }

        if let outPath = environment["PLAYHEAD_0U3E_OUT"]
            ?? environment["TEST_RUNNER_PLAYHEAD_0U3E_OUT"] {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            try encoder.encode(report).write(to: URL(fileURLWithPath: outPath))
            print("[0u3e] wrote \(outPath)")
        }

        // The lane is a measurement, not a threshold. The only things asserted
        // are that it measured something and that the population it claims to
        // measure is present — a corpus with no repeated entry would print a
        // reassuring "0 windows affected" that says nothing about the defect.
        XCTAssertGreaterThan(allWindows.count, 0, "corpus produced no detection-v1 ad windows")
        XCTAssertGreaterThan(
            perAsset.reduce(0) { $0 + $1.repeatedEntries }, 0,
            "corpus contains no repeated catalog entry — nothing to measure"
        )
        // Direction, asserted rather than assumed: tightening the selector can
        // only REMOVE weight. A row where the occurrence arm outscores the hull
        // arm by more than the boundary-convention delta can explain would mean
        // the arms are wrong, not that the fix is generous.
        for row in allWindows where row.boundaryOnlyRefs.isEmpty {
            XCTAssertLessThanOrEqual(
                row.occurrenceWeight, row.hullWeight + 1e-12,
                "occurrence arm outscored the hull arm with no boundary-only entry: \(row.assetId) \(row.startTime)-\(row.endTime)"
            )
        }
    }

    // MARK: - Per-asset evaluation

    private func evaluate(asset: Corpus.Asset, cap: Double) -> AssetReport {
        let chunks = asset.chunks.map { chunk in
            TranscriptChunk(
                id: chunk.id,
                analysisAssetId: asset.assetId,
                segmentFingerprint: chunk.segmentFingerprint,
                chunkIndex: chunk.chunkIndex,
                startTime: chunk.startTime,
                endTime: chunk.endTime,
                text: chunk.text,
                normalizedText: chunk.normalizedText,
                pass: chunk.pass,
                modelVersion: chunk.modelVersion,
                transcriptVersion: chunk.transcriptVersion,
                atomOrdinal: chunk.atomOrdinal,
                speakerId: chunk.speakerId,
                avgConfidence: chunk.avgConfidence.map(Float.init)
            )
        }
        let canonical = TranscriptChunkCanonicalizer.canonicalize(chunks)
        let atomized = TranscriptAtomizer.atomize(
            chunks: canonical.chunks,
            analysisAssetId: asset.assetId,
            normalizationHash: "0u3e-lane",
            sourceHash: "0u3e-lane"
        )
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atomized.atoms,
            analysisAssetId: asset.assetId,
            transcriptVersion: atomized.version.transcriptVersion
        )

        let repeated = catalog.entries.filter { $0.count > 1 }
        let widestHull = catalog.entries
            .map { $0.coverageEndTime - $0.coverageStartTime }
            .max() ?? 0

        var rows: [WindowRow] = []
        var excluded: [String: Int] = [:]
        for window in asset.windows {
            guard window.detectorVersion == Self.fusionDetectorVersion else {
                excluded[window.detectorVersion, default: 0] += 1
                continue
            }
            let start = min(window.startTime, window.endTime)
            let end = max(window.startTime, window.endTime)
            let hull = Self.hullArmEntries(catalog: catalog, start: start, end: end)
            let occurrence = Self.occurrenceArmEntries(catalog: catalog, start: start, end: end)
            let hullRefs = Set(hull.map(\.evidenceRef))
            let occurrenceRefs = Set(occurrence.map(\.evidenceRef))
            let hullOnly = hull.filter { !occurrenceRefs.contains($0.evidenceRef) }
            let boundaryOnly = occurrence.filter { !hullRefs.contains($0.evidenceRef) }

            let hullWeight = Self.catalogWeight(count: hull.count, cap: cap)
            let occurrenceWeight = Self.catalogWeight(count: occurrence.count, cap: cap)
            let delta = hullWeight - occurrenceWeight
            // `proposalConfidence = min(1, Σw) × m`, so the drop is `Δw × m`
            // bounded by the duration prior's own floor and peak. The prior is a
            // per-show resolution this export does not carry; the BAND is a
            // property of `DurationPrior` and holds for every resolution of it.
            let band = [
                delta * DurationPrior.standard.floorMultiplier,
                delta * DurationPrior.standard.peakMultiplier
            ]

            rows.append(WindowRow(
                assetId: asset.assetId,
                title: asset.title,
                startTime: start,
                endTime: end,
                decisionState: window.decisionState,
                eligibilityGate: window.eligibilityGate,
                detectorVersion: window.detectorVersion,
                persistedConfidence: window.confidence,
                hullCount: hull.count,
                occurrenceCount: occurrence.count,
                hullOnlyRefs: hullOnly.map(\.evidenceRef),
                hullOnlyTexts: hullOnly.map(\.matchedText),
                hullOnlyNearestMissSeconds: hullOnly.map {
                    BannerEvidenceWindowCorpusEvalTests.nearestMiss(entry: $0, start: start, end: end)
                },
                boundaryOnlyRefs: boundaryOnly.map(\.evidenceRef),
                boundaryOnlyDetail: boundaryOnly.map { view in
                    let full = catalog.entries.first { $0.evidenceRef == view.evidenceRef }
                    let occurrences = (full?.anchorableOccurrences ?? [])
                        .map { String(format: "%.4f-%.4f", $0.startTime, $0.endTime) }
                        .joined(separator: ",")
                    return String(
                        format: "ref=%d text=%@ hull=%.4f-%.4f window=%.4f-%.4f mentions=%@",
                        view.evidenceRef, view.matchedText,
                        full?.coverageStartTime ?? .nan, full?.coverageEndTime ?? .nan,
                        start, end, occurrences
                    )
                },
                hullWeight: hullWeight,
                occurrenceWeight: occurrenceWeight,
                weightDelta: delta,
                losesCatalogKind: !hull.isEmpty && occurrence.isEmpty,
                confidenceDropBound: band,
                confidenceWasClamped: window.confidence >= 1.0,
                mayBePreRollClamped: start == 0
            ))
        }

        return AssetReport(
            assetId: asset.assetId,
            title: asset.title,
            durationSec: asset.durationSec,
            atoms: atomized.atoms.count,
            catalogEntries: catalog.entries.count,
            repeatedEntries: repeated.count,
            widestHullSeconds: widestHull,
            windows: rows,
            excludedWindowsByDetectorVersion: excluded
        )
    }
}
