// SpecialistScanPlannerTests.swift
// playhead-b6jq PR 4: pure, sim-safe coverage of the specialist scan candidate
// selector. These tests drive the planner with SYNTHETIC segments + evidence
// catalog (no live model, no store) and pin the candidate gate, densest-first
// ordering, budget cap, empty behavior, and the music-bed recall lever.

import Testing

@testable import Playhead

@Suite("SpecialistScanPlanner (playhead-b6jq PR4)")
struct SpecialistScanPlannerTests {

    private let assetId = "asset-planner"

    /// Neutral, non-ad segments spanning `0..<(count*10)` seconds. Deliberately
    /// free of any sponsor / promo / URL vocabulary so `LexicalScanner`
    /// contributes nothing — these tests isolate the catalog / music-bed gates.
    private func neutralSegments(count: Int = 60) -> [AdTranscriptSegment] {
        let lines = (0..<count).map { idx -> (Double, Double, String) in
            (Double(idx) * 10.0, Double(idx) * 10.0 + 10.0,
             "Nature discussion part \(idx) covering rivers, trees and mountains.")
        }
        return makeFMSegments(analysisAssetId: assetId, transcriptVersion: "tx-planner", lines: lines)
    }

    private func catalog(_ spans: [(Double, Double)]) -> EvidenceCatalog {
        EvidenceCatalog(
            analysisAssetId: assetId,
            transcriptVersion: "tx-planner",
            entries: spans.enumerated().map { idx, span in
                EvidenceEntry(
                    evidenceRef: idx,
                    category: .promoCode,
                    matchedText: "CODE\(idx)",
                    normalizedText: "code\(idx)",
                    atomOrdinal: Int(span.0 / 10.0),
                    startTime: span.0,
                    endTime: span.1
                )
            }
        )
    }

    private var emptyCatalog: EvidenceCatalog {
        EvidenceCatalog(analysisAssetId: assetId, transcriptVersion: "tx-planner", entries: [])
    }

    private func covers(_ windows: [SpecialistScanWindow], time: Double) -> Bool {
        windows.contains { $0.startTime <= time && $0.endTime >= time }
    }

    // MARK: - Config default guard

    @Test("defaultBudget is 160 (full-episode fallback ceiling)")
    func defaultBudgetGuard() {
        #expect(SpecialistScanPlanner.defaultBudget == 160)
    }

    // MARK: - Candidate selection

    @Test("only cluster-overlapping windows are produced; never full-episode; the gap is untouched")
    func selectsOnlyClusterOverlappingWindows() {
        let segments = neutralSegments()
        // Two far-apart catalog clusters over a 600s episode.
        let windows = SpecialistScanPlanner().selectWindows(
            segments: segments,
            evidenceCatalog: catalog([(100, 105), (500, 505)]),
            featureWindows: []
        )

        #expect(!windows.isEmpty)
        // No window is full-episode: each is at most one window-width wide.
        for window in windows {
            #expect(window.endTime - window.startTime <= SpecialistScanPlanner.windowWidthSeconds + 0.001)
            #expect(!window.lineRefs.isEmpty)
        }
        // Every window sits inside one of the two padded cluster regions
        // ([95,110] or [495,510]) — nothing near the mid-episode gap.
        for window in windows {
            let inA = window.startTime >= 95 - 0.001 && window.endTime <= 110 + 0.001
            let inB = window.startTime >= 495 - 0.001 && window.endTime <= 510 + 0.001
            #expect(inA || inB, "window \(window.startTime)..\(window.endTime) escaped both clusters")
        }
        #expect(!covers(windows, time: 300), "mid-episode gap must not be scanned")
    }

    @Test("windows are returned densest-cue-first")
    func densestFirstOrdering() {
        let segments = neutralSegments()
        // Cluster A near t=100 has 3 overlapping cues (dense); cluster B near
        // t=500 has 1 cue (sparse).
        let windows = SpecialistScanPlanner().selectWindows(
            segments: segments,
            evidenceCatalog: catalog([(100, 102), (103, 105), (106, 108), (500, 503)]),
            featureWindows: []
        )
        #expect(windows.count >= 2)
        // The densest window (cluster A) is first.
        #expect(windows.first!.startTime < 200, "densest cluster (near t=100) must sort first")
    }

    @Test("budget cap keeps exactly `budget` windows and the densest survive")
    func budgetCapKeepsDensest() {
        let segments = neutralSegments()
        // Four disjoint regions with descending density: 3, 2, 1, 1.
        let windows = SpecialistScanPlanner().selectWindows(
            segments: segments,
            evidenceCatalog: catalog([
                (100, 102), (103, 105), (106, 108),  // region 1: density 3
                (200, 202), (203, 205),              // region 2: density 2
                (300, 302),                          // region 3: density 1
                (400, 402),                          // region 4: density 1
            ]),
            featureWindows: [],
            budget: 2
        )
        #expect(windows.count == 2)
        // The two survivors are the density-3 (near 100) and density-2 (near 200)
        // regions; nothing from the density-1 regions near 300/400.
        #expect(covers(windows, time: 104))
        #expect(covers(windows, time: 204))
        #expect(!covers(windows, time: 301))
        #expect(!covers(windows, time: 401))
    }

    @Test("empty catalog with neutral segments and no feature windows returns no windows")
    func emptyCatalogReturnsEmpty() {
        let windows = SpecialistScanPlanner().selectWindows(
            segments: neutralSegments(),
            evidenceCatalog: emptyCatalog,
            featureWindows: []
        )
        #expect(windows.isEmpty)
    }

    @Test("empty segments returns no windows")
    func emptySegmentsReturnsEmpty() {
        let windows = SpecialistScanPlanner().selectWindows(
            segments: [],
            evidenceCatalog: catalog([(100, 105)]),
            featureWindows: []
        )
        #expect(windows.isEmpty)
    }

    @Test("budget <= 0 returns no windows")
    func nonPositiveBudgetReturnsEmpty() {
        let windows = SpecialistScanPlanner().selectWindows(
            segments: neutralSegments(),
            evidenceCatalog: catalog([(100, 105)]),
            featureWindows: [],
            budget: 0
        )
        #expect(windows.isEmpty)
    }

    // MARK: - Lexical union

    @Test("lexical ad copy in the segment text contributes windows even with an empty catalog")
    func lexicalCopyContributesWindows() {
        // Canonical ad copy that LexicalScanner recognizes (same shape the
        // runner fixtures rely on), placed at t=300..330.
        var lines: [(Double, Double, String)] = (0..<60).map { idx in
            (Double(idx) * 10.0, Double(idx) * 10.0 + 10.0,
             "Nature discussion part \(idx) covering rivers and trees.")
        }
        lines[30] = (300, 310, "This episode is brought to you by ExampleCo.")
        lines[31] = (310, 320, "Visit example.com slash deal and use promo code PLAYHEAD.")
        let segments = makeFMSegments(analysisAssetId: assetId, transcriptVersion: "tx-planner", lines: lines)

        let windows = SpecialistScanPlanner().selectWindows(
            segments: segments,
            evidenceCatalog: emptyCatalog,
            featureWindows: []
        )
        #expect(!windows.isEmpty, "lexical candidate gate must contribute windows")
        // The windows should cluster around the ad copy at 300..320.
        #expect(windows.contains { $0.startTime < 330 && $0.endTime > 300 })
    }

    // MARK: - Music-bed recall lever

    @Test("music-bed feature windows add flanked candidate windows the catalog/lexical gate misses")
    func musicBedFeatureWindowsAddWindows() {
        let segments = neutralSegments()
        // A high music-bed-change feature window at t=300 where there is NO
        // catalog or lexical cue.
        let musicBed = FeatureWindow(
            analysisAssetId: assetId,
            startTime: 300,
            endTime: 305,
            rms: 0.1,
            spectralFlux: 0.1,
            musicProbability: 0.2,
            musicBedChangeScore: 0.9,
            pauseProbability: 0.0,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 5
        )

        // Without the feature window: empty (no cue-less coverage).
        let withoutMB = SpecialistScanPlanner().selectWindows(
            segments: segments,
            evidenceCatalog: emptyCatalog,
            featureWindows: []
        )
        #expect(withoutMB.isEmpty)

        // With the music-bed feature window: a flanked window appears near t=300.
        let withMB = SpecialistScanPlanner().selectWindows(
            segments: segments,
            evidenceCatalog: emptyCatalog,
            featureWindows: [musicBed]
        )
        #expect(!withMB.isEmpty)
        #expect(withMB.contains { $0.startTime < 305 && $0.endTime > 300 })
    }

    // MARK: - Repeated sponsors anchor per MENTION, not across the hull (playhead-x7rk)

    /// A catalog entry that was said in several places: one deduplicated entry
    /// whose `firstTime`/`lastTime` hull spans them all and whose
    /// `anchorableOccurrences` names each one.
    ///
    /// This is the shape `EvidenceCatalogBuilder.build` produces for a repeated
    /// sponsor — `count` counts MATCHES, the occurrence list counts ATOMS — and
    /// building it by hand here is what lets these rails be pure.
    private func repeatedEntry(
        ref: Int,
        at times: [(Double, Double)],
        text: String = "acme.com"
    ) -> EvidenceEntry {
        precondition(!times.isEmpty)
        return EvidenceEntry(
            evidenceRef: ref,
            category: .url,
            matchedText: text,
            normalizedText: text,
            atomOrdinal: Int(times[0].0 / 10.0),
            startTime: times[0].0,
            endTime: times[0].1,
            count: times.count,
            firstTime: times.map(\.0).min(),
            lastTime: times.map(\.1).max(),
            occurrences: times.map { time in
                EvidenceOccurrence(
                    atomOrdinal: Int(time.0 / 10.0),
                    startTime: time.0,
                    endTime: time.1
                )
            }
        )
    }

    private func catalog(entries: [EvidenceEntry]) -> EvidenceCatalog {
        EvidenceCatalog(analysisAssetId: assetId, transcriptVersion: "tx-planner", entries: entries)
    }

    /// One entry, two mentions 400 s apart on a 600 s episode. Its hull is
    /// [100, 505] — 405 s, two thirds of the episode.
    ///
    /// Three separate `@Test`s read this one plan, deliberately. "Both reads are
    /// scanned and the gap is not" is three claims, and a single test asserting
    /// all three cannot say WHICH one a regression broke: anchoring only the
    /// first mention, anchoring only the last, and anchoring the hull between
    /// them are three different defects that one rail reports identically.
    private func twoReadsPlan() -> [SpecialistScanWindow] {
        SpecialistScanPlanner().selectWindows(
            segments: neutralSegments(),
            evidenceCatalog: catalog(entries: [repeatedEntry(ref: 0, at: [(100, 105), (500, 505)])]),
            featureWindows: []
        )
    }

    @Test("a sponsor read twice is scanned at the FIRST read")
    func repeatedSponsorAnchorsTheFirstRead() {
        #expect(covers(twoReadsPlan(), time: 102))
    }

    @Test("a sponsor read twice is scanned at the SECOND read")
    func repeatedSponsorAnchorsTheSecondRead() {
        // The mention playhead-04rx's dedup used to forget entirely, and the one
        // a "just use the representative" fix silently drops.
        #expect(covers(twoReadsPlan(), time: 502))
    }

    @Test("the audio BETWEEN two reads of one sponsor is not scanned")
    func repeatedSponsorDoesNotScanTheGap() {
        // The 395 s between the two reads is not evidence of anything. Under the
        // hull it was one candidate region and every tile in it was planned.
        let windows = twoReadsPlan()
        #expect(!covers(windows, time: 300), "the gap between two mentions must not be scanned")
        for window in windows {
            let nearFirst = window.startTime >= 95 - 0.001 && window.endTime <= 110 + 0.001
            let nearSecond = window.startTime >= 495 - 0.001 && window.endTime <= 510 + 0.001
            #expect(nearFirst || nearSecond,
                    "window \(window.startTime)..\(window.endTime) is between the mentions, not at one")
        }
    }

    @Test("a repeated sponsor's hull does not swallow a distant unrelated cue into one region")
    func repeatedSponsorDoesNotMergeAwayOtherAnchors() {
        // The hull [100, 500] straddles an unrelated promo code at 300. Under
        // the hull all three merge into ONE region spanning 95..505 — 17 tiles,
        // every one of them scoring the hull anchor as a cue. Per mention there
        // are three regions and the gaps between them are not planned.
        let windows = SpecialistScanPlanner().selectWindows(
            segments: neutralSegments(),
            evidenceCatalog: catalog(entries: [
                repeatedEntry(ref: 0, at: [(100, 102), (500, 502)]),
                EvidenceEntry(
                    evidenceRef: 1, category: .promoCode, matchedText: "SAVE10",
                    normalizedText: "save10", atomOrdinal: 30, startTime: 300, endTime: 302
                ),
            ]),
            featureWindows: []
        )

        #expect(covers(windows, time: 101))
        #expect(covers(windows, time: 301))
        #expect(covers(windows, time: 501))
        // Three separate reads, three separate regions: the halfway points
        // between them carry no cue at all.
        #expect(!covers(windows, time: 200), "the 100->300 gap must not be planned")
        #expect(!covers(windows, time: 400), "the 300->500 gap must not be planned")
        // Three regions of one tile each (the padded spans are 12 s wide, under
        // the 25 s tile), so the whole plan is three windows.
        #expect(windows.count == 3, "expected one tile per read, got \(windows.count)")
    }

    /// A plain, unrepeated cue at `time`.
    private func cue(_ ref: Int, _ start: Double, _ end: Double) -> EvidenceEntry {
        EvidenceEntry(
            evidenceRef: ref, category: .promoCode, matchedText: "CUE\(ref)",
            normalizedText: "cue\(ref)", atomOrdinal: Int(start / 10.0),
            startTime: start, endTime: end
        )
    }

    @Test("a hull that ENDS is not a uniform vote: density must not lift a lone cue over a real pair")
    func densityIsNotInflatedByASpanningHull() {
        // `AdLikelihoodScanOrder.seeds` licensed this site with "a wide anchor
        // contributes uniformly and is harmless". Uniform WITHIN the hull, yes —
        // and that is the half of the claim that is true, which is why a naive
        // test of it passes under the defect. But a hull ENDS: a tile inside it
        // is lifted by one vote and a tile outside it is not, so the hull
        // decides ties across its own boundary.
        //
        // One cue at 250 INSIDE a [100, 402] hull, two real cues at 500/503
        // OUTSIDE it, budget 1. Per mention the pair wins 2–1. Under the hull
        // the lone cue is lifted to 2, ties, and takes the window on the
        // earliest-start tiebreak — the grant is spent where one thing was said
        // instead of where two were.
        let windows = SpecialistScanPlanner().selectWindows(
            segments: neutralSegments(),
            evidenceCatalog: catalog(entries: [
                repeatedEntry(ref: 0, at: [(100, 102), (400, 402)]),
                cue(1, 250, 252),
                cue(2, 500, 502),
                cue(3, 503, 505),
            ]),
            featureWindows: [],
            budget: 1
        )
        #expect(windows.count == 1)
        #expect(covers(windows, time: 502),
                "the one window must go to the two-cue cluster, not to a lone cue a hull lifts")
        #expect(!covers(windows, time: 251))
    }

    @Test("a repeated entry's FIRST anchor ends at its own mention, not at the last one")
    func representativeAnchorIsNotWidenedToTheLastMention() {
        // The HALF-FIX direction, and the only rail that separates it from a
        // full revert: anchor every occurrence, but let the representative's
        // anchor still run to `lastTime`. Every "both reads are scanned" rail
        // stays green and the plan is still wrong.
        //
        // Entry at 100 and 400, a two-cue cluster at 500, budget 1. Correctly,
        // the cluster wins 2–1. If the representative's anchor reaches 402 it
        // overlaps the 400 mention's own tile, which then ties the cluster at 2
        // and wins on the earliest-start tiebreak.
        let windows = SpecialistScanPlanner().selectWindows(
            segments: neutralSegments(),
            evidenceCatalog: catalog(entries: [
                repeatedEntry(ref: 0, at: [(100, 102), (400, 402)]),
                cue(1, 500, 502),
                cue(2, 503, 505),
            ]),
            featureWindows: [],
            budget: 1
        )
        #expect(windows.count == 1)
        #expect(covers(windows, time: 502),
                "the one window must go to the two-cue cluster, not to a mention a widened anchor doubles")
    }

    @Test("an entry said ONCE plans exactly what it did before playhead-x7rk")
    func singleMentionEntryIsUnchanged() {
        // The confinement claim. For `count == 1`, `firstTime`/`lastTime` are
        // the representative's own times, so the hull expression and the
        // per-occurrence expression are the same anchor. Driven both ways: an
        // entry with an explicit one-element occurrence list, and one with none
        // at all (the `EvidenceEntry` convenience init the other tests use).
        let segments = neutralSegments()
        let planner = SpecialistScanPlanner()
        let withList = planner.selectWindows(
            segments: segments,
            evidenceCatalog: catalog(entries: [repeatedEntry(ref: 0, at: [(100, 105)], text: "acme.com")]),
            featureWindows: []
        )
        let withoutList = planner.selectWindows(
            segments: segments,
            evidenceCatalog: catalog(entries: [
                EvidenceEntry(
                    evidenceRef: 0, category: .url, matchedText: "acme.com",
                    normalizedText: "acme.com", atomOrdinal: 10, startTime: 100, endTime: 105
                ),
            ]),
            featureWindows: []
        )
        #expect(!withList.isEmpty)
        #expect(withList.map(\.startTime) == withoutList.map(\.startTime))
        #expect(withList.map(\.endTime) == withoutList.map(\.endTime))
    }

    @Test("an entry with NO recorded occurrence list anchors at its representative, not its hull")
    func unrecordedOccurrenceListAnchorsTheRepresentative() {
        // `occurrences: nil` means "nobody recorded the population" — a value
        // persisted before playhead-04rx. `anchorableOccurrences` resolves it to
        // the representative, which is the pre-04rx behaviour and NOT the hull.
        // The distinction is invisible unless the two disagree, so this entry is
        // built with a wide `firstTime`/`lastTime` and no list.
        let windows = SpecialistScanPlanner().selectWindows(
            segments: neutralSegments(),
            evidenceCatalog: catalog(entries: [
                EvidenceEntry(
                    evidenceRef: 0, category: .url, matchedText: "acme.com",
                    normalizedText: "acme.com", atomOrdinal: 10,
                    startTime: 100, endTime: 105,
                    count: 2, firstTime: 100, lastTime: 500, occurrences: nil
                ),
            ]),
            featureWindows: []
        )
        #expect(covers(windows, time: 102), "the representative must still anchor")
        #expect(!covers(windows, time: 300), "an unrecorded population is not a licence to scan the hull")
        #expect(!covers(windows, time: 498), "an unrecorded population is not a licence to scan the hull")
    }

    @Test("a low music-bed-change feature window is ignored (below the gate)")
    func lowMusicBedScoreIgnored() {
        let lowMB = FeatureWindow(
            analysisAssetId: assetId,
            startTime: 300,
            endTime: 305,
            rms: 0.1,
            spectralFlux: 0.1,
            musicProbability: 0.2,
            musicBedChangeScore: 0.1,  // below musicBedChangeThreshold
            pauseProbability: 0.0,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 5
        )
        let windows = SpecialistScanPlanner().selectWindows(
            segments: neutralSegments(),
            evidenceCatalog: emptyCatalog,
            featureWindows: [lowMB]
        )
        #expect(windows.isEmpty)
    }
}
