// InventorySanityFilterTests.swift
// playhead-xr3t — unit + integration coverage for the post-hoc
// lightweight inventory sanity filter.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeChapter(
    startTime: TimeInterval,
    endTime: TimeInterval?,
    title: String?,
    source: ChapterSource = .rssInline,
    disposition: ChapterDisposition = .content,
    qualityScore: Float = 0.8
) -> ChapterEvidence {
    ChapterEvidence(
        startTime: startTime,
        endTime: endTime,
        title: title,
        source: source,
        disposition: disposition,
        qualityScore: qualityScore
    )
}

// MARK: - Filter unit tests

@Suite("InventorySanityFilter — rule (a) duration floor")
struct InventorySanityFilterDurationTests {

    @Test("Duration just under 2 s is rejected as .tooShort")
    func underTwoSecondsRejected() {
        let filter = InventorySanityFilter(isEnabled: true)
        let result = filter.evaluate(
            startTime: 60,
            endTime: 61.999,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(result == .rejected(reason: .tooShort))
    }

    @Test("Duration exactly 2 s passes (boundary)")
    func exactlyTwoSecondsPasses() {
        // Spec: "exactly 2.0s" should pass.
        let filter = InventorySanityFilter(isEnabled: true)
        let result = filter.evaluate(
            startTime: 60,
            endTime: 62.0,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(result == .passed)
    }

    @Test("Long span passes")
    func longSpanPasses() {
        let filter = InventorySanityFilter(isEnabled: true)
        let result = filter.evaluate(
            startTime: 60,
            endTime: 120,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(result == .passed)
    }

    @Test("Degenerate span (end <= start) is rejected as .tooShort")
    func degenerateSpanRejected() {
        let filter = InventorySanityFilter(isEnabled: true)
        let zero = filter.evaluate(
            startTime: 60,
            endTime: 60,
            episodeDuration: 600,
            declaredChapters: []
        )
        let inverted = filter.evaluate(
            startTime: 100,
            endTime: 50,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(zero == .rejected(reason: .tooShort))
        #expect(inverted == .rejected(reason: .tooShort))
    }

    @Test("NaN endpoints fall into .tooShort, not a silent pass")
    func nanEndpointRejected() {
        let filter = InventorySanityFilter(isEnabled: true)
        let result = filter.evaluate(
            startTime: 60,
            endTime: .nan,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(result == .rejected(reason: .tooShort))
    }
}

@Suite("InventorySanityFilter — rule (b) edge guards")
struct InventorySanityFilterEdgeTests {

    @Test("Span starting before 3 s is .tooEarly")
    func earlySpanRejected() {
        let filter = InventorySanityFilter(isEnabled: true)
        let result = filter.evaluate(
            startTime: 1.0,
            endTime: 30.0,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(result == .rejected(reason: .tooEarly))
    }

    @Test("Span starting at exactly 3.0 s passes (boundary)")
    func exactlyAtHeadEdgePasses() {
        // Spec: "exactly at 3.0s edge" should pass.
        let filter = InventorySanityFilter(isEnabled: true)
        let result = filter.evaluate(
            startTime: 3.0,
            endTime: 30.0,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(result == .passed)
    }

    @Test("Span ending after duration - 3 s is .tooLate")
    func lateSpanRejected() {
        let filter = InventorySanityFilter(isEnabled: true)
        let result = filter.evaluate(
            startTime: 500,
            endTime: 599,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(result == .rejected(reason: .tooLate))
    }

    @Test("Span ending at exactly duration - 3 s passes (boundary)")
    func exactlyAtTailEdgePasses() {
        let filter = InventorySanityFilter(isEnabled: true)
        let result = filter.evaluate(
            startTime: 500,
            endTime: 597.0,
            episodeDuration: 600,
            declaredChapters: []
        )
        #expect(result == .passed)
    }

    @Test("Unknown duration disables only the tail-edge guard")
    func unknownDurationKeepsHeadEdge() {
        let filter = InventorySanityFilter(isEnabled: true)
        // Head edge still rejects when duration is nil.
        let early = filter.evaluate(
            startTime: 1.0,
            endTime: 30.0,
            episodeDuration: nil,
            declaredChapters: []
        )
        #expect(early == .rejected(reason: .tooEarly))

        // No tail-edge rejection possible with nil duration.
        let likelyTail = filter.evaluate(
            startTime: 500,
            endTime: 1_000,
            episodeDuration: nil,
            declaredChapters: []
        )
        #expect(likelyTail == .passed)
    }

    @Test("Zero / non-finite duration disables tail-edge guard")
    func nonPositiveDurationDisablesTail() {
        let filter = InventorySanityFilter(isEnabled: true)
        let zero = filter.evaluate(
            startTime: 500,
            endTime: 600,
            episodeDuration: 0,
            declaredChapters: []
        )
        #expect(zero == .passed)
        let inf = filter.evaluate(
            startTime: 500,
            endTime: 600,
            episodeDuration: .infinity,
            declaredChapters: []
        )
        #expect(inf == .passed)
    }
}

@Suite("InventorySanityFilter — rule (c) declared-chapter overlap")
struct InventorySanityFilterChapterTests {

    @Test("Span fully inside a declared content chapter is rejected")
    func insideContentChapterRejected() {
        let filter = InventorySanityFilter(isEnabled: true)
        let chapters = [
            makeChapter(startTime: 100, endTime: 300, title: "Interview with guest"),
        ]
        let result = filter.evaluate(
            startTime: 150,
            endTime: 250,
            episodeDuration: 600,
            declaredChapters: chapters
        )
        #expect(result == .rejected(reason: .overlapsDeclaredChapter))
    }

    @Test("Span touching but not overlapping chapter boundary passes")
    func touchingBoundaryPasses() {
        // Spec: "span touching but not overlapping chapter boundary" passes.
        let filter = InventorySanityFilter(isEnabled: true)
        let chapters = [
            makeChapter(startTime: 100, endTime: 200, title: "Some content"),
        ]
        // Span ends exactly at chapter start — touches but doesn't overlap.
        let leftTouch = filter.evaluate(
            startTime: 60,
            endTime: 100,
            episodeDuration: 600,
            declaredChapters: chapters
        )
        // Span starts exactly at chapter end — touches but doesn't overlap.
        let rightTouch = filter.evaluate(
            startTime: 200,
            endTime: 240,
            episodeDuration: 600,
            declaredChapters: chapters
        )
        #expect(leftTouch == .passed)
        #expect(rightTouch == .passed)
    }

    @Test("Ad-break chapters never trigger rejection — overlap is intended")
    func adBreakChapterDoesNotReject() {
        let filter = InventorySanityFilter(isEnabled: true)
        let chapters = [
            makeChapter(
                startTime: 100,
                endTime: 200,
                title: "Sponsor break",
                disposition: .adBreak
            ),
        ]
        let result = filter.evaluate(
            startTime: 110,
            endTime: 190,
            episodeDuration: 600,
            declaredChapters: chapters
        )
        #expect(result == .passed)
    }

    @Test("Inferred chapters (not creator-source) are ignored")
    func inferredChaptersIgnored() {
        // Spec: declared = creator-source only. `.inferred` chapters
        // (playhead-w7oi / playhead-au2v.1 outputs) MUST NOT cause
        // rejection — the filter is a defense-in-depth check that
        // even if an inferred chapter slips into the list, it's
        // dropped.
        let filter = InventorySanityFilter(isEnabled: true)
        let chapters = [
            makeChapter(
                startTime: 100,
                endTime: 300,
                title: "Generated chapter",
                source: .inferred
            ),
        ]
        let result = filter.evaluate(
            startTime: 150,
            endTime: 250,
            episodeDuration: 600,
            declaredChapters: chapters
        )
        #expect(result == .passed)
    }

    @Test("All three creator-source variants trigger rejection")
    func allCreatorSourcesTriggerRejection() {
        let filter = InventorySanityFilter(isEnabled: true)
        for source in [ChapterSource.id3, .pc20, .rssInline] {
            let chapter = makeChapter(
                startTime: 100,
                endTime: 200,
                title: "Editorial content",
                source: source
            )
            let result = filter.evaluate(
                startTime: 150,
                endTime: 170,
                episodeDuration: 600,
                declaredChapters: [chapter]
            )
            #expect(result == .rejected(reason: .overlapsDeclaredChapter))
        }
    }

    @Test("Chapter without endTime is treated as open-ended")
    func openEndedChapterCoversToEnd() {
        // An RSS chapter with no end time runs through the rest of the
        // episode. A span starting after the chapter start should
        // still be flagged as overlap.
        let filter = InventorySanityFilter(isEnabled: true)
        let chapters = [
            makeChapter(startTime: 100, endTime: nil, title: "Outro"),
        ]
        let result = filter.evaluate(
            startTime: 200,
            endTime: 300,
            episodeDuration: 1_000,
            declaredChapters: chapters
        )
        #expect(result == .rejected(reason: .overlapsDeclaredChapter))
    }

    @Test("Ambiguous-disposition chapters still trigger rejection")
    func ambiguousChapterTriggersRejection() {
        // Per spec: rule (c) rejects on overlap with any declared
        // *content* chapter. Ambiguous chapters aren't ad breaks, so
        // they remain in the rule-(c) set.
        let filter = InventorySanityFilter(isEnabled: true)
        let chapters = [
            makeChapter(
                startTime: 100,
                endTime: 200,
                title: nil,
                disposition: .ambiguous
            ),
        ]
        let result = filter.evaluate(
            startTime: 150,
            endTime: 170,
            episodeDuration: 600,
            declaredChapters: chapters
        )
        #expect(result == .rejected(reason: .overlapsDeclaredChapter))
    }
}

@Suite("InventorySanityFilter — feature-flag rollback")
struct InventorySanityFilterRollbackTests {

    @Test("Disabled filter passes every otherwise-invalid span")
    func disabledFilterIsNoOp() {
        let filter = InventorySanityFilter(isEnabled: false)
        let chapters = [
            makeChapter(startTime: 0, endTime: 600, title: "Episode"),
        ]
        // Each of (a), (b)-head, (b)-tail, (c) would reject if enabled.
        let cases: [(Double, Double)] = [
            (60.0, 60.5),     // (a) duration < 2s
            (0.5, 30.0),      // (b)-head start < 3s
            (500.0, 599.0),   // (b)-tail end > 597s
            (200.0, 220.0),   // (c) overlaps declared chapter
        ]
        for (start, end) in cases {
            let result = filter.evaluate(
                startTime: start,
                endTime: end,
                episodeDuration: 600,
                declaredChapters: chapters
            )
            #expect(result == .passed, "Expected pass for [\(start), \(end)] when disabled")
        }
    }

    @Test("Production factory respects the persisted flag value")
    func productionFactoryReadsSettings() {
        let onSettings = LightweightInventoryChecksSettings(enabled: true)
        #expect(InventorySanityFilter.production(settings: onSettings).isEnabled)

        let offSettings = LightweightInventoryChecksSettings(enabled: false)
        #expect(!InventorySanityFilter.production(settings: offSettings).isEnabled)
    }
}

@Suite("InventorySanityFilter — synthetic corpus rejection rate")
struct InventorySanityFilterRejectionRateTests {

    /// Builds a synthetic corpus of invalid spans across the three
    /// rule categories. Bead acceptance criterion: ≥ 98% of these
    /// are rejected. The corpus mixes per-rule and combination cases
    /// so a regression in any single rule shows up as a measurable
    /// rate drop.
    private static func syntheticInvalidCorpus(
        episodeDuration: Double,
        chapters: [ChapterEvidence]
    ) -> [(start: Double, end: Double)] {
        var spans: [(Double, Double)] = []

        // Rule (a): a range of "too short" durations distributed across
        // the episode (40 spans).
        for i in 0..<40 {
            let start = 10.0 + Double(i) * 5.0
            let badDuration = 0.1 + Double(i % 19) * 0.1  // 0.1 .. 1.9 s
            spans.append((start, start + badDuration))
        }

        // Rule (b)-head: 20 spans starting in the first 3 s.
        for i in 0..<20 {
            let start = Double(i) * 0.14  // 0.0 .. 2.66 s
            spans.append((start, start + 30.0))
        }

        // Rule (b)-tail: 20 spans ending in the last 3 s.
        for i in 0..<20 {
            let end = (episodeDuration - 2.99) + Double(i) * 0.15
            spans.append((max(end - 30.0, 0), end))
        }

        // Rule (c): 30 spans overlapping declared content chapters.
        for chapter in chapters {
            let chapterStart = chapter.startTime
            let chapterEnd = chapter.endTime ?? episodeDuration
            let mid = (chapterStart + chapterEnd) / 2
            // Three overlap shapes per chapter: fully inside, left lap,
            // right lap. Padded so duration / edges don't accidentally
            // shadow the chapter-overlap rejection (each is well clear
            // of the head/tail edges and the duration floor).
            spans.append((mid - 1.0, mid + 1.0))                       // fully inside, 2s
            spans.append((chapterStart - 5.0, chapterStart + 5.0))     // left lap, ~10s
            spans.append((chapterEnd - 5.0, chapterEnd + 5.0))         // right lap, ~10s
        }

        return spans
    }

    @Test("Filter rejects ≥ 98% of synthetic invalid spans")
    func rejectionRateMeetsBeadAcceptance() {
        let filter = InventorySanityFilter(isEnabled: true)
        let episodeDuration = 1_800.0
        // 10 widely-spaced content chapters across the episode.
        let chapters: [ChapterEvidence] = (0..<10).map { i in
            let start = 100.0 + Double(i) * 150.0
            return makeChapter(
                startTime: start,
                endTime: start + 60,
                title: "Editorial part \(i + 1)"
            )
        }

        let corpus = Self.syntheticInvalidCorpus(
            episodeDuration: episodeDuration,
            chapters: chapters
        )
        let rejected = corpus.filter { (start, end) in
            filter.evaluate(
                startTime: start,
                endTime: end,
                episodeDuration: episodeDuration,
                declaredChapters: chapters
            ) != .passed
        }.count

        let rate = Double(rejected) / Double(corpus.count)
        #expect(
            rate >= 0.98,
            "Rejection rate \(rate) on \(corpus.count) invalid spans must meet 98% bar"
        )
    }

    @Test("Filter passes a corpus of clearly-valid spans")
    func validSpansSurvive() {
        // Cross-check: the filter must not be a brute-force pass-rejecter.
        // A corpus of obviously-clean spans should survive at 100%.
        let filter = InventorySanityFilter(isEnabled: true)
        let episodeDuration = 1_800.0
        let chapters: [ChapterEvidence] = [
            makeChapter(startTime: 0, endTime: 60, title: "Intro"),
            makeChapter(startTime: 1_400, endTime: 1_500, title: "Editorial"),
        ]
        // Build candidate spans in the open regions between chapters,
        // each at least 5s long, well clear of edges.
        let valid: [(Double, Double)] = [
            (100, 130), (200, 280), (400, 460),
            (700, 760), (900, 940), (1_100, 1_180),
            (1_550, 1_590), (1_650, 1_700), (1_720, 1_790),
        ]
        for (start, end) in valid {
            let result = filter.evaluate(
                startTime: start,
                endTime: end,
                episodeDuration: episodeDuration,
                declaredChapters: chapters
            )
            #expect(result == .passed, "Expected pass for [\(start), \(end)]")
        }
    }
}

// MARK: - LightweightInventoryChecksSettings persistence

@Suite("LightweightInventoryChecksSettings persistence (playhead-xr3t)")
struct LightweightInventoryChecksSettingsTests {

    private func freshDefaults(
        suiteName: String = "xr3t.flag.\(UUID().uuidString)"
    ) -> UserDefaults {
        let d = UserDefaults(suiteName: suiteName)!
        d.removePersistentDomain(forName: suiteName)
        return d
    }

    @Test("Fresh install (no key persisted) loads as enabled = true per spec")
    func defaultsOnNewInstall() {
        let d = freshDefaults()
        let s = LightweightInventoryChecksSettings.load(from: d)
        #expect(s.enabled == true)
    }

    @Test("Persisted false round-trips as false (the rollback flip)")
    func persistedFalseSurvivesReload() {
        let d = freshDefaults()
        LightweightInventoryChecksSettings(enabled: false).save(to: d)
        let s = LightweightInventoryChecksSettings.load(from: d)
        #expect(s.enabled == false)
    }

    @Test("Persisted true round-trips as true")
    func persistedTrueSurvivesReload() {
        let d = freshDefaults()
        LightweightInventoryChecksSettings(enabled: true).save(to: d)
        let s = LightweightInventoryChecksSettings.load(from: d)
        #expect(s.enabled == true)
    }
}

// MARK: - SkipOrchestrator boundary integration

@Suite("SkipOrchestrator + InventorySanityFilter integration (playhead-xr3t)")
struct SkipOrchestratorInventoryFilterIntegrationTests {

    /// Build an orchestrator with the filter forced ON, asset row
    /// carrying a known duration.
    private func makeOrchestrator(
        episodeDuration: Double?,
        filterEnabled: Bool = true
    ) async throws -> (SkipOrchestrator, AnalysisStore) {
        let store = try await makeTestStore()
        let asset = AnalysisAsset(
            id: "asset-xr3t",
            episodeId: "ep-xr3t",
            assetFingerprint: "fp-xr3t",
            weakFingerprint: nil,
            sourceURL: "file:///test/asset-xr3t.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: episodeDuration
        )
        try await store.insertAsset(asset)

        let trustService = try await makeSkipTestTrustService(
            mode: "auto",
            trustScore: 0.9,
            observations: 10
        )
        let filter = InventorySanityFilter(isEnabled: filterEnabled)
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            inventoryFilter: filter
        )
        await orchestrator.beginEpisode(
            analysisAssetId: "asset-xr3t",
            episodeId: "ep-xr3t",
            podcastId: "podcast-1"
        )
        return (orchestrator, store)
    }

    @Test("Hot-path span < 2 s is filtered out before reaching active set")
    func shortSpanIsFiltered() async throws {
        let (orchestrator, _) = try await makeOrchestrator(episodeDuration: 600)
        let shortWindow = makeSkipTestAdWindow(
            id: "ad-too-short",
            assetId: "asset-xr3t",
            startTime: 60,
            endTime: 61,         // 1s duration → rejected
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([shortWindow])

        let active = await orchestrator.activeWindowIDs()
        #expect(!active.contains("ad-too-short"))
    }

    @Test("Span starting in first 3 s is filtered out")
    func earlySpanIsFiltered() async throws {
        let (orchestrator, _) = try await makeOrchestrator(episodeDuration: 600)
        let earlyWindow = makeSkipTestAdWindow(
            id: "ad-too-early",
            assetId: "asset-xr3t",
            startTime: 0.5,
            endTime: 30,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([earlyWindow])

        let active = await orchestrator.activeWindowIDs()
        #expect(!active.contains("ad-too-early"))
    }

    @Test("Span ending in last 3 s is filtered out when duration is known")
    func lateSpanIsFiltered() async throws {
        let (orchestrator, _) = try await makeOrchestrator(episodeDuration: 600)
        let lateWindow = makeSkipTestAdWindow(
            id: "ad-too-late",
            assetId: "asset-xr3t",
            startTime: 540,
            endTime: 599,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([lateWindow])

        let active = await orchestrator.activeWindowIDs()
        #expect(!active.contains("ad-too-late"))
    }

    @Test("Chapter overlap filters the span — and requires the chapters push")
    func chapterOverlapIsFilteredAfterPush() async throws {
        let (orchestrator, _) = try await makeOrchestrator(episodeDuration: 1_800)
        let chapters = [
            makeChapter(startTime: 200, endTime: 400, title: "Interview"),
        ]
        await orchestrator.setDeclaredChapters(chapters, analysisAssetId: "asset-xr3t")

        let overlapping = makeSkipTestAdWindow(
            id: "ad-overlaps-chapter",
            assetId: "asset-xr3t",
            startTime: 250,
            endTime: 300,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([overlapping])

        let active = await orchestrator.activeWindowIDs()
        #expect(!active.contains("ad-overlaps-chapter"))
    }

    @Test("Inferred chapters are filtered out of the orchestrator's declared list")
    func inferredChaptersAreNotConsulted() async throws {
        let (orchestrator, _) = try await makeOrchestrator(episodeDuration: 1_800)
        let chapters = [
            makeChapter(
                startTime: 200,
                endTime: 400,
                title: "FM-generated chapter",
                source: .inferred
            ),
        ]
        await orchestrator.setDeclaredChapters(chapters, analysisAssetId: "asset-xr3t")

        let inChapter = makeSkipTestAdWindow(
            id: "ad-overlapping-inferred",
            assetId: "asset-xr3t",
            startTime: 250,
            endTime: 350,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([inChapter])

        let active = await orchestrator.activeWindowIDs()
        #expect(active.contains("ad-overlapping-inferred"))
    }

    @Test("Valid spans survive the filter unchanged")
    func validSpansSurvive() async throws {
        let (orchestrator, _) = try await makeOrchestrator(episodeDuration: 600)
        let good = makeSkipTestAdWindow(
            id: "ad-good",
            assetId: "asset-xr3t",
            startTime: 60,
            endTime: 120,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([good])

        let active = await orchestrator.activeWindowIDs()
        #expect(active.contains("ad-good"))
    }

    @Test("Flag OFF restores pre-bead behaviour (every span survives)")
    func disabledFilterPassesEverything() async throws {
        let (orchestrator, _) = try await makeOrchestrator(
            episodeDuration: 600,
            filterEnabled: false
        )
        let chapters = [
            makeChapter(startTime: 0, endTime: 600, title: "Whole episode"),
        ]
        await orchestrator.setDeclaredChapters(chapters, analysisAssetId: "asset-xr3t")

        let windows = [
            makeSkipTestAdWindow(id: "ad-short", assetId: "asset-xr3t", startTime: 60, endTime: 61),
            makeSkipTestAdWindow(id: "ad-early", assetId: "asset-xr3t", startTime: 0.5, endTime: 30),
            makeSkipTestAdWindow(id: "ad-late", assetId: "asset-xr3t", startTime: 540, endTime: 599),
            makeSkipTestAdWindow(id: "ad-chapter", assetId: "asset-xr3t", startTime: 200, endTime: 220),
        ]
        await orchestrator.receiveAdWindows(windows)

        let active = await orchestrator.activeWindowIDs()
        for id in ["ad-short", "ad-early", "ad-late", "ad-chapter"] {
            #expect(active.contains(id), "Filter OFF: \(id) must survive")
        }
    }

    @Test("Filter also gates AdDecisionResult path (fusion → backfill push)")
    func fusionAdDecisionResultPathIsGated() async throws {
        let (orchestrator, _) = try await makeOrchestrator(episodeDuration: 600)

        let tooShort = AdDecisionResult(
            id: "fusion-short",
            analysisAssetId: "asset-xr3t",
            startTime: 60,
            endTime: 61,         // 1s → rejected by rule (a)
            skipConfidence: 0.9,
            eligibilityGate: .eligible,
            recomputationRevision: 1
        )
        let valid = AdDecisionResult(
            id: "fusion-good",
            analysisAssetId: "asset-xr3t",
            startTime: 60,
            endTime: 120,
            skipConfidence: 0.9,
            eligibilityGate: .eligible,
            recomputationRevision: 1
        )
        await orchestrator.receiveAdDecisionResults([tooShort, valid])

        let active = await orchestrator.activeWindowIDs()
        #expect(!active.contains("fusion-short"))
        #expect(active.contains("fusion-good"))
    }

    @Test("Mismatched-asset push to setDeclaredChapters is a silent no-op")
    func mismatchedAssetChapterPushIsDropped() async throws {
        let (orchestrator, _) = try await makeOrchestrator(episodeDuration: 1_800)
        let chapters = [
            makeChapter(startTime: 200, endTime: 400, title: "Interview"),
        ]
        // Wrong asset id — must NOT install these chapters.
        await orchestrator.setDeclaredChapters(chapters, analysisAssetId: "wrong-asset")

        let inChapter = makeSkipTestAdWindow(
            id: "ad-not-actually-in-chapter",
            assetId: "asset-xr3t",
            startTime: 250,
            endTime: 350,
            confidence: 0.85,
            decisionState: "confirmed"
        )
        await orchestrator.receiveAdWindows([inChapter])

        let active = await orchestrator.activeWindowIDs()
        #expect(active.contains("ad-not-actually-in-chapter"))
    }
}
