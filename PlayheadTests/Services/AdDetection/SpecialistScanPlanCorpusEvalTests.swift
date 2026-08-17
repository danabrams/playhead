// SpecialistScanPlanCorpusEvalTests.swift
// playhead-x7rk: the corpus lane that measures what the coverage HULL does to
// the SPECIALIST SCAN PLAN — where the on-device FM specialist spends a
// background grant.
//
// WHY A LANE AND NOT A FIXTURE
// ----------------------------
// Same reason as playhead-04rx's and playhead-rty3's, which this reuses rather
// than replaces. The claim is about REPEATS — a sponsor read twice in one
// episode, whose deduplicated catalog entry then has a `firstTime`/`lastTime`
// hull spanning everything between. A synthetic fixture contains the repeats its
// author thought to write, so the effect it shows is a property of the fixture.
// What is wanted is how often a real device transcript produces a hull wide
// enough to swallow the episode, and what that costs the plan.
//
// WHAT IT RUNS
// ------------
// Production code, end to end, per asset:
//
//   TranscriptChunkCanonicalizer.canonicalize     (as `runBackfill` does first)
//   -> TranscriptAtomizer.atomize
//   -> EvidenceCatalogBuilder.build
//   -> TranscriptSegmenter.segment                (`AdDetectionService` line 9621)
//   -> SpecialistScanPlanner().selectWindows      (the site under test)
//
// THE TWO ARMS, AND WHY THE CONTROL IS NOT A RESTATEMENT
// -----------------------------------------------------
// playhead-rty3's lane had to restate its control arm as two comparisons,
// because the fix DELETED the expression. Here it does not have to, and that is
// better evidence. The deleted expression was
//
//     let lo = min(entry.coverageStartTime, entry.coverageEndTime)
//     let hi = max(entry.coverageStartTime, entry.coverageEndTime)
//
// i.e. ONE anchor per entry spanning `firstTime`..`lastTime`. The shipped code
// now emits one anchor per element of `anchorableOccurrences`. So feeding the
// SHIPPED planner a catalog whose every entry carries exactly ONE occurrence
// equal to its own hull reproduces the old anchor set EXACTLY — same planner,
// same tiling, same density metric, same sort, one input transformed.
//
// That equivalence is not assumed: `hullCollapsedEquivalenceHolds` checks it for
// every entry of every asset in the corpus, against the deleted expression
// restated once in `deletedHullAnchor` purely as the thing being checked.
//
// A THIRD ARM, the LEXICAL-ONLY plan (empty catalog), is what makes
// "this window exists only because of a hull" a measurable predicate rather
// than an inference: a hull-plan window with no MENTION inside it and no
// overlap with any lexical-only window is nominated by nothing but the hull.
//
// BUDGET. `selectWindows` sorts densest-first and returns `prefix(budget)`, so
// the plan at budget B is exactly the first B windows of the plan at budget ∞.
// The lane therefore runs each arm ONCE, uncapped, and reads the shipped
// `defaultBudget` view off the prefix. Both are reported: uncapped is the
// planner's opinion, capped is what a grant actually pays for.
//
// WHAT IT CANNOT SEE — read this before quoting any number
// -------------------------------------------------------
// * `ad_windows` rows are what the DEVICE believed, not ground truth. They were
//   produced by a pipeline that did not include the specialist lane at all
//   (`runSpecialistHostReadScan` is two-key gated and persists to
//   `specialist_scan_results`, acting on nothing). So "ad-window seconds
//   covered" measures agreement with the shipped detector, NOT recall against
//   real ads. A host-read the device missed is invisible to it in both arms.
// * The planner's inputs here are a transcript export. The live runner's
//   `AssetInputs.segments` come from the same producers, but a live run also
//   passes `featureWindows: []` — so the music-bed arm is dormant in
//   production AND here, and this lane says nothing about it.
// * The corpus is 31 assets from ONE device pull. `corpus-2026-08-02.json` is
//   named for its export date, not a capture date; its own `source` names
//   `playhead-gate-artifacts/3gzp/ground-truth.sqlite`, whose newest row
//   postdates 08-02.
// * Nothing here measures FM COST. A window is 25 s of transcript; the token
//   cost of a prompt is a function of how much text that is, which varies. The
//   scan-seconds delta is a proxy for budget, not a token count.
//
// STAGING: `PLAYHEAD_X7RK_CORPUS` (or the `PLAYHEAD_RTY3_CORPUS` /
// `PLAYHEAD_04RX_CORPUS` variables — one export, one pull, three lanes) points
// at a JSON export of a device `analysis.sqlite`. It is NOT in the repo, so with
// the variables unset this SKIPS. `PLAYHEAD_X7RK_OUT` writes the full report.

import Foundation
import XCTest
@testable import Playhead

final class SpecialistScanPlanCorpusEvalTests: XCTestCase {

    /// One export shape, decoded once. A duplicated `Decodable` is how two lanes
    /// come to disagree about a field.
    typealias Corpus = RepeatedOccurrenceAnchoringCorpusEvalTests.Corpus

    /// Large enough that `prefix(budget)` never bites; the planner's own
    /// `defaultBudget` view is read off this plan's prefix.
    static let uncapped = 1_000_000

    // MARK: - Report shape

    struct WindowRow: Encodable {
        let startTime: Double
        let endTime: Double
        /// Seconds from this window to the nearest MENTION of any catalog entry.
        /// Zero means a mention is inside it.
        let nearestMentionMissSeconds: Double
        /// True when no mention is inside it AND no lexical-only window overlaps
        /// it: the window exists because of a hull and nothing else.
        let hullNominated: Bool
        /// Rank in the arm's densest-first order (what the budget truncates).
        let rank: Int
    }

    struct AssetReport: Encodable {
        let assetId: String
        let title: String?
        let durationSec: Double?
        let atoms: Int
        let segments: Int
        let episodeSpanSeconds: Double
        let catalogEntries: Int
        /// Entries whose (category, text) pair matched more than once.
        let repeatedEntries: Int
        /// Entries that can anchor in more than one ATOM. Differs from
        /// `repeatedEntries` because `count` counts MATCHES and occurrences
        /// count ATOMS.
        let multiOccurrenceEntries: Int
        let widestHullSeconds: Double
        let widestOccurrenceSeconds: Double

        // Anchors
        let hullAnchorSeconds: Double
        let occurrenceAnchors: Int
        let occurrenceAnchorSeconds: Double

        // Plans, uncapped
        let hullWindowsUncapped: Int
        let occurrenceWindowsUncapped: Int
        let hullRunsUncapped: Int
        let occurrenceRunsUncapped: Int
        let hullScanSecondsUncapped: Double
        let occurrenceScanSecondsUncapped: Double

        // Plans at the shipped budget
        let hullWindowsCapped: Int
        let occurrenceWindowsCapped: Int
        let hullScanSecondsCapped: Double
        let occurrenceScanSecondsCapped: Double
        let hullBudgetSaturated: Bool
        let occurrenceBudgetSaturated: Bool
        /// Jaccard of the two capped plans by window bounds — how much of the
        /// grant's actual spend moves.
        let cappedPlanJaccard: Double

        // The defect, counted
        let hullNominatedWindowsUncapped: Int
        let hullNominatedSecondsUncapped: Double
        let hullNominatedWindowsCapped: Int
        let hullNominatedSecondsCapped: Double
        let widestHullNominatedMissSeconds: Double

        // Agreement with what the device believed
        let deviceAdWindows: Int
        let deviceAdWindowSeconds: Double
        let hullCoveredAdWindows: Int
        let occurrenceCoveredAdWindows: Int
        let hullCoveredAdSeconds: Double
        let occurrenceCoveredAdSeconds: Double

        let hullWindowRows: [WindowRow]
        let occurrenceWindowRows: [WindowRow]
    }

    // MARK: - The arms

    /// The DELETED expression, restated once, solely so the control arm can be
    /// checked against it rather than trusted. Nothing else calls this.
    static func deletedHullAnchor(_ entry: EvidenceEntry) -> (start: Double, end: Double) {
        (min(entry.coverageStartTime, entry.coverageEndTime),
         max(entry.coverageStartTime, entry.coverageEndTime))
    }

    /// An entry rewritten so its ONLY anchorable occurrence is its own hull.
    ///
    /// Feed the shipped planner a catalog of these and its evidence-anchor set
    /// is byte-identical to what the pre-x7rk loop produced — same count, same
    /// spans, same clamping — so the control arm is the shipped function on a
    /// transformed input rather than a second copy of the planner.
    static func hullCollapsed(_ entry: EvidenceEntry) -> EvidenceEntry {
        let hull = deletedHullAnchor(entry)
        return EvidenceEntry(
            evidenceRef: entry.evidenceRef,
            category: entry.category,
            matchedText: entry.matchedText,
            normalizedText: entry.normalizedText,
            atomOrdinal: entry.atomOrdinal,
            startTime: hull.start,
            endTime: hull.end,
            count: entry.count,
            firstTime: entry.firstTime,
            lastTime: entry.lastTime,
            occurrences: [EvidenceOccurrence(
                atomOrdinal: entry.atomOrdinal,
                startTime: hull.start,
                endTime: hull.end
            )]
        )
    }

    static func hullCollapsed(_ catalog: EvidenceCatalog) -> EvidenceCatalog {
        EvidenceCatalog(
            analysisAssetId: catalog.analysisAssetId,
            transcriptVersion: catalog.transcriptVersion,
            entries: catalog.entries.map(hullCollapsed)
        )
    }

    static func emptied(_ catalog: EvidenceCatalog) -> EvidenceCatalog {
        EvidenceCatalog(
            analysisAssetId: catalog.analysisAssetId,
            transcriptVersion: catalog.transcriptVersion,
            entries: []
        )
    }

    // MARK: - The lane

    func testSpecialistScanPlanOnDeviceCorpus() throws {
        let environment = ProcessInfo.processInfo.environment
        let path = ["PLAYHEAD_X7RK_CORPUS", "TEST_RUNNER_PLAYHEAD_X7RK_CORPUS",
                    "PLAYHEAD_RTY3_CORPUS", "TEST_RUNNER_PLAYHEAD_RTY3_CORPUS",
                    "PLAYHEAD_04RX_CORPUS", "TEST_RUNNER_PLAYHEAD_04RX_CORPUS"]
            .lazy.compactMap { environment[$0] }
            .first { FileManager.default.fileExists(atPath: $0) }
        guard let path else {
            throw XCTSkip(
                "specialist scan plan lane needs PLAYHEAD_X7RK_CORPUS pointing at a device analysis.sqlite JSON export"
            )
        }
        let corpus = try JSONDecoder().decode(
            Corpus.self,
            from: try Data(contentsOf: URL(fileURLWithPath: path))
        )
        try XCTSkipIf(corpus.assets.isEmpty, "corpus export has no assets")

        var reports: [AssetReport] = []
        var equivalenceChecked = 0
        for asset in corpus.assets where !asset.chunks.isEmpty {
            let (report, checked) = evaluate(asset: asset)
            reports.append(report)
            equivalenceChecked += checked
        }

        print("[x7rk] source=\(corpus.source)")
        print("[x7rk] assets=\(reports.count) atoms=\(reports.reduce(0) { $0 + $1.atoms }) segments=\(reports.reduce(0) { $0 + $1.segments })")
        print("[x7rk] control-arm equivalence to the DELETED expression verified on \(equivalenceChecked) entries")

        let entries = reports.reduce(0) { $0 + $1.catalogEntries }
        let repeated = reports.reduce(0) { $0 + $1.repeatedEntries }
        let multi = reports.reduce(0) { $0 + $1.multiOccurrenceEntries }
        print("[x7rk] catalog entries=\(entries) count>1=\(repeated) anchorable-in->1-atom=\(multi)")
        print("[x7rk] widest hull per asset (s): " + reports
            .filter { $0.widestHullSeconds > 0 }
            .sorted { $0.widestHullSeconds > $1.widestHullSeconds }
            .prefix(8)
            .map { "\($0.assetId.prefix(8))=\(fmt($0.widestHullSeconds))" }
            .joined(separator: " "))

        func sum(_ keyPath: KeyPath<AssetReport, Double>) -> Double {
            reports.reduce(0) { $0 + $1[keyPath: keyPath] }
        }
        func sum(_ keyPath: KeyPath<AssetReport, Int>) -> Int {
            reports.reduce(0) { $0 + $1[keyPath: keyPath] }
        }

        print("[x7rk] --- THE PLAN, UNCAPPED (the planner's opinion) ---")
        print("[x7rk]   window RUNS (>= candidate regions):  hull=\(sum(\.hullRunsUncapped))  occurrence=\(sum(\.occurrenceRunsUncapped))")
        print("[x7rk]   tiled windows:                       hull=\(sum(\.hullWindowsUncapped))  occurrence=\(sum(\.occurrenceWindowsUncapped))")
        print("[x7rk]   scan seconds:                        hull=\(fmt(sum(\.hullScanSecondsUncapped)))  occurrence=\(fmt(sum(\.occurrenceScanSecondsUncapped)))")

        print("[x7rk] --- THE PLAN AT defaultBudget=\(SpecialistScanPlanner.defaultBudget) (what a grant pays for) ---")
        print("[x7rk]   windows:      hull=\(sum(\.hullWindowsCapped))  occurrence=\(sum(\.occurrenceWindowsCapped))")
        print("[x7rk]   scan seconds: hull=\(fmt(sum(\.hullScanSecondsCapped)))  occurrence=\(fmt(sum(\.occurrenceScanSecondsCapped)))")
        print("[x7rk]   assets saturating the cap: hull=\(reports.filter(\.hullBudgetSaturated).count)/\(reports.count)  occurrence=\(reports.filter(\.occurrenceBudgetSaturated).count)/\(reports.count)")

        print("[x7rk] --- THE DEFECT, NUMERATOR OVER DENOMINATOR ---")
        print("[x7rk]   HULL-NOMINATED = a planned window with NO mention inside it and no lexical window overlapping it")
        print("[x7rk]   uncapped: \(sum(\.hullNominatedWindowsUncapped)) of \(sum(\.hullWindowsUncapped)) windows, \(fmt(sum(\.hullNominatedSecondsUncapped))) of \(fmt(sum(\.hullScanSecondsUncapped))) scan seconds")
        print("[x7rk]   capped:   \(sum(\.hullNominatedWindowsCapped)) of \(sum(\.hullWindowsCapped)) windows, \(fmt(sum(\.hullNominatedSecondsCapped))) of \(fmt(sum(\.hullScanSecondsCapped))) scan seconds")
        print("[x7rk]   widest nearest-mention miss on a hull-nominated window: \(fmt(reports.map(\.widestHullNominatedMissSeconds).max() ?? 0)) s")

        print("[x7rk] --- AGREEMENT WITH WHAT THE DEVICE BELIEVED (ad_windows, NOT ground truth) ---")
        print("[x7rk]   device ad_windows: \(sum(\.deviceAdWindows)) rows, \(fmt(sum(\.deviceAdWindowSeconds))) s")
        print("[x7rk]   rows touched by >=1 capped plan window: hull=\(sum(\.hullCoveredAdWindows)) occurrence=\(sum(\.occurrenceCoveredAdWindows))")
        print("[x7rk]   ad seconds inside the capped plan:      hull=\(fmt(sum(\.hullCoveredAdSeconds))) occurrence=\(fmt(sum(\.occurrenceCoveredAdSeconds)))")

        print("[x7rk] --- PER ASSET (only where the arms differ) ---")
        for report in reports.sorted(by: { $0.widestHullSeconds > $1.widestHullSeconds })
        where report.hullWindowsUncapped != report.occurrenceWindowsUncapped
            || report.hullNominatedWindowsUncapped > 0 {
            print("[x7rk]   \(report.assetId.prefix(8)) span=\(fmt(report.episodeSpanSeconds))s hull=\(fmt(report.widestHullSeconds))s"
                + " runs \(report.hullRunsUncapped)->\(report.occurrenceRunsUncapped)"
                + " win \(report.hullWindowsUncapped)->\(report.occurrenceWindowsUncapped)"
                + " scan \(fmt(report.hullScanSecondsUncapped))->\(fmt(report.occurrenceScanSecondsUncapped))s"
                + " hullNominated=\(report.hullNominatedWindowsUncapped)(\(fmt(report.hullNominatedSecondsUncapped))s)"
                + " capJaccard=\(String(format: "%.3f", report.cappedPlanJaccard))"
                + " adRows \(report.hullCoveredAdWindows)/\(report.occurrenceCoveredAdWindows) of \(report.deviceAdWindows)")
        }

        if let outPath = environment["PLAYHEAD_X7RK_OUT"] ?? environment["TEST_RUNNER_PLAYHEAD_X7RK_OUT"] {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            try encoder.encode(reports).write(to: URL(fileURLWithPath: outPath))
            print("[x7rk] wrote \(outPath)")
        }

        // The lane is a measurement, not a threshold. What IS asserted is that
        // it measured something and that the population it claims to measure is
        // present — a corpus with no repeated entry would print a reassuring
        // "0 windows affected" that says nothing about the defect.
        XCTAssertGreaterThan(reports.count, 0, "corpus produced no assets")
        XCTAssertGreaterThan(sum(\.hullWindowsUncapped), 0, "the control arm planned no windows at all")
        XCTAssertGreaterThan(multi, 0, "corpus contains no entry anchorable in more than one atom — nothing to measure")
        XCTAssertGreaterThan(equivalenceChecked, 0, "the control arm's equivalence was never checked")
    }

    // MARK: - Per-asset evaluation

    // swiftlint:disable:next function_body_length
    private func evaluate(asset: Corpus.Asset) -> (AssetReport, Int) {
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
            normalizationHash: "x7rk-lane",
            sourceHash: "x7rk-lane"
        )
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atomized.atoms,
            analysisAssetId: asset.assetId,
            transcriptVersion: atomized.version.transcriptVersion
        )
        let segments = TranscriptSegmenter.segment(atoms: atomized.atoms)

        // The control arm's equivalence to the deleted expression, per entry.
        var checked = 0
        let hullCatalog = Self.hullCollapsed(catalog)
        for (original, collapsed) in zip(catalog.entries, hullCatalog.entries) {
            let expected = Self.deletedHullAnchor(original)
            let produced = collapsed.anchorableOccurrences
            XCTAssertEqual(produced.count, 1,
                           "hull-collapsed entry must anchor in exactly one place")
            XCTAssertEqual(produced.first?.startTime, expected.start, accuracy: 1e-9)
            XCTAssertEqual(produced.first?.endTime, expected.end, accuracy: 1e-9)
            checked += 1
        }

        let episodeStart = segments.map(\.startTime).min() ?? 0
        let episodeEnd = segments.map(\.endTime).max() ?? 0

        let planner = SpecialistScanPlanner()
        let hullPlan = planner.selectWindows(
            segments: segments, evidenceCatalog: hullCatalog,
            featureWindows: [], budget: Self.uncapped)
        let occurrencePlan = planner.selectWindows(
            segments: segments, evidenceCatalog: catalog,
            featureWindows: [], budget: Self.uncapped)
        let lexicalPlan = planner.selectWindows(
            segments: segments, evidenceCatalog: Self.emptied(catalog),
            featureWindows: [], budget: Self.uncapped)

        // Every MENTION, clamped exactly as the planner clamps its anchors.
        let mentions: [(start: Double, end: Double)] = catalog.entries.flatMap { entry in
            entry.anchorableOccurrences.map { occurrence -> (Double, Double) in
                let lo = min(occurrence.startTime, occurrence.endTime)
                let hi = max(occurrence.startTime, occurrence.endTime)
                return (min(max(lo, episodeStart), episodeEnd),
                        min(max(hi, episodeStart), episodeEnd))
            }
        }

        func rows(_ plan: [SpecialistScanWindow]) -> [WindowRow] {
            plan.enumerated().map { rank, window in
                let miss = mentions.map { mention in
                    max(window.startTime - mention.end, mention.start - window.endTime, 0)
                }.min() ?? .infinity
                let lexicalOverlap = lexicalPlan.contains {
                    $0.startTime < window.endTime && $0.endTime > window.startTime
                }
                return WindowRow(
                    startTime: window.startTime,
                    endTime: window.endTime,
                    nearestMentionMissSeconds: miss,
                    hullNominated: miss > 0 && !lexicalOverlap,
                    rank: rank
                )
            }
        }

        let hullRows = rows(hullPlan)
        let occurrenceRows = rows(occurrencePlan)
        let budget = SpecialistScanPlanner.defaultBudget
        let hullCapped = Array(hullPlan.prefix(budget))
        let occurrenceCapped = Array(occurrencePlan.prefix(budget))
        let hullCappedRows = Array(hullRows.prefix(budget))

        let deviceWindows: [(start: Double, end: Double)] = asset.windows.map {
            (min($0.startTime, $0.endTime), max($0.startTime, $0.endTime))
        }

        let report = AssetReport(
            assetId: asset.assetId,
            title: asset.title,
            durationSec: asset.durationSec,
            atoms: atomized.atoms.count,
            segments: segments.count,
            episodeSpanSeconds: max(episodeEnd - episodeStart, 0),
            catalogEntries: catalog.entries.count,
            repeatedEntries: catalog.entries.filter { $0.count > 1 }.count,
            multiOccurrenceEntries: catalog.entries.filter { $0.anchorableOccurrences.count > 1 }.count,
            widestHullSeconds: catalog.entries
                .map { $0.coverageEndTime - $0.coverageStartTime }.max() ?? 0,
            widestOccurrenceSeconds: catalog.entries
                .flatMap { $0.anchorableOccurrences.map { $0.endTime - $0.startTime } }.max() ?? 0,
            hullAnchorSeconds: hullCatalog.entries
                .reduce(0) { $0 + ($1.coverageEndTime - $1.coverageStartTime) },
            occurrenceAnchors: mentions.count,
            occurrenceAnchorSeconds: mentions.reduce(0) { $0 + ($1.end - $1.start) },
            hullWindowsUncapped: hullPlan.count,
            occurrenceWindowsUncapped: occurrencePlan.count,
            hullRunsUncapped: Self.contiguousRuns(hullPlan),
            occurrenceRunsUncapped: Self.contiguousRuns(occurrencePlan),
            hullScanSecondsUncapped: Self.scanSeconds(hullPlan),
            occurrenceScanSecondsUncapped: Self.scanSeconds(occurrencePlan),
            hullWindowsCapped: hullCapped.count,
            occurrenceWindowsCapped: occurrenceCapped.count,
            hullScanSecondsCapped: Self.scanSeconds(hullCapped),
            occurrenceScanSecondsCapped: Self.scanSeconds(occurrenceCapped),
            hullBudgetSaturated: hullPlan.count > budget,
            occurrenceBudgetSaturated: occurrencePlan.count > budget,
            cappedPlanJaccard: Self.jaccard(hullCapped, occurrenceCapped),
            hullNominatedWindowsUncapped: hullRows.filter(\.hullNominated).count,
            hullNominatedSecondsUncapped: hullRows.filter(\.hullNominated)
                .reduce(0) { $0 + ($1.endTime - $1.startTime) },
            hullNominatedWindowsCapped: hullCappedRows.filter(\.hullNominated).count,
            hullNominatedSecondsCapped: hullCappedRows.filter(\.hullNominated)
                .reduce(0) { $0 + ($1.endTime - $1.startTime) },
            widestHullNominatedMissSeconds: hullRows.filter(\.hullNominated)
                .map(\.nearestMentionMissSeconds).max() ?? 0,
            deviceAdWindows: deviceWindows.count,
            deviceAdWindowSeconds: deviceWindows.reduce(0) { $0 + ($1.end - $1.start) },
            hullCoveredAdWindows: Self.touchedRows(deviceWindows, by: hullCapped),
            occurrenceCoveredAdWindows: Self.touchedRows(deviceWindows, by: occurrenceCapped),
            hullCoveredAdSeconds: Self.coveredSeconds(deviceWindows, by: hullCapped),
            occurrenceCoveredAdSeconds: Self.coveredSeconds(deviceWindows, by: occurrenceCapped),
            hullWindowRows: hullRows,
            occurrenceWindowRows: occurrenceRows
        )
        return (report, checked)
    }

    // MARK: - Plan geometry

    static func scanSeconds(_ plan: [SpecialistScanWindow]) -> Double {
        plan.reduce(0) { $0 + ($1.endTime - $1.startTime) }
    }

    /// Maximal runs of windows that touch end-to-start.
    ///
    /// This is an OBSERVABLE PROXY for the planner's candidate-region count, and
    /// it is an UPPER bound: tiling walks a region in fixed steps, so all tiles
    /// of one region touch — unless a tile covering no transcript is dropped,
    /// which splits one region into two runs. `runs >= regions`, always.
    static func contiguousRuns(_ plan: [SpecialistScanWindow]) -> Int {
        let sorted = plan.sorted { $0.startTime < $1.startTime }
        guard var previousEnd = sorted.first?.endTime else { return 0 }
        var runs = 1
        for window in sorted.dropFirst() {
            if window.startTime > previousEnd + 1e-6 { runs += 1 }
            previousEnd = max(previousEnd, window.endTime)
        }
        return runs
    }

    static func jaccard(_ lhs: [SpecialistScanWindow], _ rhs: [SpecialistScanWindow]) -> Double {
        func key(_ window: SpecialistScanWindow) -> String {
            String(format: "%.3f-%.3f", window.startTime, window.endTime)
        }
        let a = Set(lhs.map(key)), b = Set(rhs.map(key))
        guard !a.isEmpty || !b.isEmpty else { return 1 }
        return Double(a.intersection(b).count) / Double(a.union(b).count)
    }

    static func touchedRows(
        _ deviceWindows: [(start: Double, end: Double)],
        by plan: [SpecialistScanWindow]
    ) -> Int {
        deviceWindows.filter { row in
            plan.contains { $0.startTime < row.end && $0.endTime > row.start }
        }.count
    }

    /// Seconds of device-believed ad time that fall inside SOME planned window.
    /// Planned windows within a region tile without overlapping, so a plain sum
    /// of clipped intersections cannot double-count.
    static func coveredSeconds(
        _ deviceWindows: [(start: Double, end: Double)],
        by plan: [SpecialistScanWindow]
    ) -> Double {
        deviceWindows.reduce(0) { total, row in
            let merged = plan
                .map { (max($0.startTime, row.start), min($0.endTime, row.end)) }
                .filter { $0.1 > $0.0 }
                .sorted { $0.0 < $1.0 }
            var covered = 0.0
            var cursor = -Double.infinity
            for span in merged {
                let lo = max(span.0, cursor)
                if span.1 > lo { covered += span.1 - lo }
                cursor = max(cursor, span.1)
            }
            return total + covered
        }
    }

    private func fmt(_ value: Double) -> String { String(format: "%.1f", value) }
}
