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
