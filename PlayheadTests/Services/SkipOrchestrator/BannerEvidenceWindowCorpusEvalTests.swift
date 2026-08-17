// BannerEvidenceWindowCorpusEvalTests.swift
// playhead-rty3: the corpus lane that measures how often a banner's evidence
// names an advertiser the banner's own window never heard.
//
// WHY A LANE AND NOT A FIXTURE
// ----------------------------
// The claim under test is about REPEATS — the same sponsor URL read twice in
// one episode, whose deduplicated catalog entry then has a `firstTime`/
// `lastTime` HULL spanning everything between. A synthetic fixture can only
// contain the repeats its author thought to write, so the rate it produces is a
// property of the fixture. The population that matters is a real device
// transcript corpus with the device's own persisted ad windows, where a repeat
// is either a second ad break or a host saying "like I said, betterhelp.com"
// and nobody chose which.
//
// WHAT IT RUNS
// ------------
// Production code, end to end, per asset:
//
//   TranscriptChunkCanonicalizer.canonicalize
//   -> TranscriptAtomizer.atomize
//   -> EvidenceCatalogBuilder.build
//
// then, per persisted `ad_windows` row, both arms of the banner evidence
// selector and the banner's own rendering of each:
//
//   HULL arm       the expression `SkipOrchestrator.catalogEntries(overlapping:end:)`
//                  carried before playhead-rty3, restated here verbatim because
//                  the fix deletes it. It is two comparisons and it is quoted
//                  in `hullArmEntries` so a reader can diff it against the
//                  commit that removed it.
//   OCCURRENCE arm `EvidenceEntry.locatedInTimeWindow(start:end:)` — the SHIPPED
//                  function, called directly. The treatment arm is not a
//                  restatement of anything.
//
//   -> AdBannerView.evidenceLines(for:)   the strings the listener actually reads
//
// WHAT IT CANNOT SEE — read this before quoting the rate
// -----------------------------------------------------
// * The denominator is PERSISTED `ad_windows` rows, not banner presentations.
//   A row is a window the device believed in; whether a card was ever shown for
//   it also depends on the playhead reaching it with a live subscriber, which
//   no database row records. So this over-counts presentations for windows
//   never listened to and under-counts nothing.
// * Window bounds here are the row's own `startTime`/`endTime`. The auto-skip
//   card carries `managed.snappedStart`/`snappedEnd` instead, which differ by
//   `AutoSkipEdgePadding`-scale seconds. That moves an edge, never a hull: the
//   entries this bead is about sit hundreds of seconds outside the window.
// * `eligibilityGate` is used to split suggest-tier from auto-skip-tier rows.
//   It is the gate the device recorded at write time, not proof of which card
//   fired; three rows in the 2026-08-02 export carry no gate at all and are
//   reported as `unknown` rather than assigned to either.
//
// STAGING: `PLAYHEAD_RTY3_CORPUS` (or `TEST_RUNNER_PLAYHEAD_RTY3_CORPUS`, or the
// `PLAYHEAD_04RX_CORPUS` variables — it is the same export shape and the same
// pull) points at a JSON export of a device `analysis.sqlite`. The export is a
// copy of a device pull and is NOT in the repo, so with the variables unset
// this SKIPS. Set `PLAYHEAD_RTY3_OUT` to write the full per-window report.

import Foundation
import XCTest
@testable import Playhead

final class BannerEvidenceWindowCorpusEvalTests: XCTestCase {

    /// The export shape is 04rx's, deliberately not a second copy of it: the
    /// two lanes read the same file produced by the same exporter, and a
    /// duplicated `Decodable` is how they come to disagree about a field.
    typealias Corpus = RepeatedOccurrenceAnchoringCorpusEvalTests.Corpus

    // MARK: - Report shape

    struct WindowRow: Encodable {
        let assetId: String
        let title: String?
        let startTime: Double
        let endTime: Double
        let decisionState: String
        let eligibilityGate: String?
        let tier: String
        /// evidenceRefs each arm selects.
        let hullRefs: [Int]
        let occurrenceRefs: [Int]
        /// Selected by the hull and by no mention inside the window.
        let hullOnlyRefs: [Int]
        /// How far the nearest mention of each hull-only entry lies OUTSIDE
        /// this window, in seconds. The listener-facing size of the error.
        let hullOnlyNearestMissSeconds: [Double]
        let hullOnlyTexts: [String]
        /// What the card renders, both arms.
        let hullLines: [String]
        let occurrenceLines: [String]
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
    }

    struct Report: Encodable {
        let source: String
        let assets: Int
        let windows: Int
        let windowsWithHullOnlyEntry: Int
        let windowsWithChangedLines: Int
        let windowsLosingALine: Int
        let perAsset: [AssetReport]
    }

    // MARK: - The two arms

    /// The selector `SkipOrchestrator.catalogEntries(overlapping:end:)` used
    /// before playhead-rty3, restated so the control arm exists after the fix
    /// deletes it. VERBATIM, including the closed interval:
    ///
    ///     entry.coverageStartTime <= end && entry.coverageEndTime >= start
    ///
    /// `coverageStartTime`/`coverageEndTime` are `firstTime`/`lastTime`, so this
    /// is the HULL and that is the whole point of the arm.
    static func hullArmEntries(catalog: EvidenceCatalog, start: Double, end: Double) -> [EvidenceEntry] {
        catalog.entries.filter { entry in
            entry.coverageStartTime <= end && entry.coverageEndTime >= start
        }
    }

    /// The shipped selector, called rather than restated.
    static func occurrenceArmEntries(catalog: EvidenceCatalog, start: Double, end: Double) -> [EvidenceEntry] {
        catalog.entries.compactMap { $0.locatedInTimeWindow(start: start, end: end) }
    }

    // MARK: - The lane

    func testBannerEvidenceWindowSelectionOnDeviceCorpus() throws {
        let environment = ProcessInfo.processInfo.environment
        let path = ["PLAYHEAD_RTY3_CORPUS", "TEST_RUNNER_PLAYHEAD_RTY3_CORPUS",
                    "PLAYHEAD_04RX_CORPUS", "TEST_RUNNER_PLAYHEAD_04RX_CORPUS"]
            .lazy.compactMap { environment[$0] }
            .first { FileManager.default.fileExists(atPath: $0) }
        guard let path else {
            throw XCTSkip(
                "banner evidence lane needs PLAYHEAD_RTY3_CORPUS pointing at a device analysis.sqlite JSON export"
            )
        }
        let corpus = try JSONDecoder().decode(
            Corpus.self,
            from: try Data(contentsOf: URL(fileURLWithPath: path))
        )
        try XCTSkipIf(corpus.assets.isEmpty, "corpus export has no assets")

        var perAsset: [AssetReport] = []
        for asset in corpus.assets where !asset.chunks.isEmpty {
            perAsset.append(evaluate(asset: asset))
        }

        let allWindows = perAsset.flatMap(\.windows)
        let withHullOnly = allWindows.filter { !$0.hullOnlyRefs.isEmpty }
        let changedLines = allWindows.filter { $0.hullLines != $0.occurrenceLines }
        let losingALine = allWindows.filter { row in
            !Set(row.hullLines).subtracting(row.occurrenceLines).isEmpty
        }

        let report = Report(
            source: corpus.source,
            assets: perAsset.count,
            windows: allWindows.count,
            windowsWithHullOnlyEntry: withHullOnly.count,
            windowsWithChangedLines: changedLines.count,
            windowsLosingALine: losingALine.count,
            perAsset: perAsset
        )

        print("[rty3] source=\(corpus.source)")
        print("[rty3] assets=\(perAsset.count) windows=\(allWindows.count) catalogEntries=\(perAsset.reduce(0) { $0 + $1.catalogEntries }) repeatedEntries=\(perAsset.reduce(0) { $0 + $1.repeatedEntries })")
        print("[rty3] widest hull per asset (s): " + perAsset
            .filter { $0.widestHullSeconds > 0 }
            .sorted { $0.widestHullSeconds > $1.widestHullSeconds }
            .prefix(8)
            .map { "\($0.assetId.prefix(8))=\(String(format: "%.0f", $0.widestHullSeconds))" }
            .joined(separator: " "))
        print("[rty3] NUMERATORS over \(allWindows.count) persisted ad_windows rows:")
        print("[rty3]   carrying >=1 entry no mention of which is inside the window: \(withHullOnly.count)")
        print("[rty3]   whose RENDERED evidence lines differ between arms:           \(changedLines.count)")
        print("[rty3]   which LOSE a rendered line (a line the listener saw and should not have): \(losingALine.count)")
        for tier in ["suggest", "autoSkip", "unknown"] {
            let rows = allWindows.filter { $0.tier == tier }
            guard !rows.isEmpty else { continue }
            let hull = rows.filter { !$0.hullOnlyRefs.isEmpty }.count
            let lost = rows.filter { !Set($0.hullLines).subtracting($0.occurrenceLines).isEmpty }.count
            print("[rty3]   tier=\(tier): windows=\(rows.count) hullOnly=\(hull) losingALine=\(lost)")
        }
        for row in losingALine.prefix(40) {
            let removed = Set(row.hullLines).subtracting(row.occurrenceLines).sorted()
            print("[rty3]   LOST \(row.assetId.prefix(8)) win=\(String(format: "%.0f", row.startTime))-\(String(format: "%.0f", row.endTime)) tier=\(row.tier) \(row.decisionState) miss=\(row.hullOnlyNearestMissSeconds.map { String(format: "%.0f", $0) }.joined(separator: ",")) removed=\(removed)")
        }

        if let outPath = environment["PLAYHEAD_RTY3_OUT"]
            ?? environment["TEST_RUNNER_PLAYHEAD_RTY3_OUT"] {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            try encoder.encode(report).write(to: URL(fileURLWithPath: outPath))
            print("[rty3] wrote \(outPath)")
        }

        // The lane is a measurement, not a threshold. The only things asserted
        // are that it measured something and that the population it claims to
        // measure is present — a corpus with no repeated entry would print a
        // reassuring "0 windows affected" that says nothing about the defect.
        XCTAssertGreaterThan(allWindows.count, 0, "corpus produced no ad windows")
        XCTAssertGreaterThan(
            perAsset.reduce(0) { $0 + $1.repeatedEntries }, 0,
            "corpus contains no repeated catalog entry — nothing to measure"
        )
    }

    // MARK: - Per-asset evaluation

    private func evaluate(asset: Corpus.Asset) -> AssetReport {
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
            normalizationHash: "rty3-lane",
            sourceHash: "rty3-lane"
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
        for window in asset.windows {
            let start = min(window.startTime, window.endTime)
            let end = max(window.startTime, window.endTime)
            let hull = Self.hullArmEntries(catalog: catalog, start: start, end: end)
            let occurrence = Self.occurrenceArmEntries(catalog: catalog, start: start, end: end)
            let occurrenceRefs = Set(occurrence.map(\.evidenceRef))
            let hullOnly = hull.filter { !occurrenceRefs.contains($0.evidenceRef) }

            rows.append(WindowRow(
                assetId: asset.assetId,
                title: asset.title,
                startTime: start,
                endTime: end,
                decisionState: window.decisionState,
                eligibilityGate: window.eligibilityGate,
                tier: Self.tier(for: window),
                hullRefs: hull.map(\.evidenceRef),
                occurrenceRefs: occurrence.map(\.evidenceRef),
                hullOnlyRefs: hullOnly.map(\.evidenceRef),
                hullOnlyNearestMissSeconds: hullOnly.map {
                    Self.nearestMiss(entry: $0, start: start, end: end)
                },
                hullOnlyTexts: hullOnly.map(\.matchedText),
                hullLines: AdBannerView.evidenceLines(for: hull),
                occurrenceLines: AdBannerView.evidenceLines(for: occurrence)
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
            windows: rows
        )
    }

    /// How far outside `[start, end]` the entry's NEAREST mention lies. Zero
    /// would mean a mention was in the window, which for a hull-only entry
    /// cannot happen — so a zero here is a bug in the arms, not a small error.
    static func nearestMiss(entry: EvidenceEntry, start: Double, end: Double) -> Double {
        entry.anchorableOccurrences.map { occurrence in
            max(start - occurrence.endTime, occurrence.startTime - end, 0)
        }.min() ?? 0
    }

    /// Which card this row would build. `markOnly` is the suggest tier — the
    /// card that asks a question whose answer is banked; `eligible` is the
    /// auto-skip tier. Anything else, including a row with no recorded gate, is
    /// reported as `unknown` rather than assigned to either.
    static func tier(for window: Corpus.Asset.Window) -> String {
        switch window.eligibilityGate {
        case "markOnly": return "suggest"
        case "eligible": return "autoSkip"
        default: return "unknown"
        }
    }
}
