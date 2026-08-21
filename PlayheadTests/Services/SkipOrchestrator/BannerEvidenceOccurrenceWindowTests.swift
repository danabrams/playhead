// BannerEvidenceOccurrenceWindowTests.swift
// playhead-rty3: a banner's evidence is the mention its OWN window can hear.
//
// THE DEFECT. `SkipOrchestrator.catalogEntries(overlapping:end:)` selected on
// `coverageStartTime`/`coverageEndTime` — which are `firstTime`/`lastTime`, the
// HULL from a deduplicated entry's earliest mention to its latest. A sponsor
// read twice therefore had a "coverage" span covering everything in between, so
// it overlapped EVERY window in that range. Both callers put the result on a
// card the listener reads:
//
//   emitBannerItem(for: ManagedWindow)  -> the AUTO-SKIP card
//   makeSuggestBannerItem(for: AdWindow) -> the SUGGEST card, whose answer is
//                                           banked per-class in trust scoring
//
// MEASURED on the 2026-08-02 device pull (`BannerEvidenceWindowCorpusEvalTests`,
// 31 assets / 115 persisted `ad_windows` rows): 28 of 115 windows carried at
// least one entry no mention of which was inside the window, and all 28 of
// those rendered a line the listener would have read — nearest miss 7 s to
// 3,140 s. The widest hull is 7,268 s on a ~7,300 s episode.
//
// WHY THESE TESTS AND NOT JUST THE LANE. The lane needs a device corpus and
// skips without one. These run everywhere and are written so each one FAILS on
// the pre-rty3 hull selector: every "must not carry" assertion is paired with a
// control entry that MUST be carried, so a selector returning nothing scores as
// a failure rather than a pass.

import Foundation
import Testing

@testable import Playhead

// MARK: - Reader

/// Single-consumer reader over a banner stream. Owns the iterator, so a pull
/// returns already-buffered items without depending on any other task getting
/// scheduled. Copied in spirit from `SuggestBannerEntryGateTests`, and for the
/// same reason: emission happens synchronously inside the orchestrator actor,
/// so a sentinel driven after the operation under test is an exact frame
/// boundary and no sleep or deadline is needed.
private struct BannerReader {
    private var iterator: AsyncStream<AdSkipBannerItem>.AsyncIterator

    init(_ stream: AsyncStream<AdSkipBannerItem>) {
        iterator = stream.makeAsyncIterator()
    }

    mutating func drain(until sentinel: String) async -> [AdSkipBannerItem] {
        var collected: [AdSkipBannerItem] = []
        while let item = await iterator.next() {
            if item.windowId == sentinel { return collected }
            collected.append(item)
        }
        return collected
    }
}

// MARK: - Selector-level

@Suite("Evidence entry: which mention a window can see (playhead-rty3)")
struct EvidenceEntryWindowSelectionTests {

    /// A sponsor URL read at 60 s and again at 3,600 s. One entry, two
    /// occurrences, a hull of 3,541 s.
    private func repeatedEntry(
        firstStart: Double = 60,
        firstEnd: Double = 61,
        secondStart: Double = 3_600,
        secondEnd: Double = 3_601
    ) -> EvidenceEntry {
        EvidenceEntry(
            evidenceRef: 7,
            category: .url,
            matchedText: "NetSuite.ai",
            normalizedText: "netsuite.ai",
            atomOrdinal: 12,
            startTime: firstStart,
            endTime: firstEnd,
            count: 2,
            firstTime: firstStart,
            lastTime: secondEnd,
            occurrences: [
                EvidenceOccurrence(atomOrdinal: 12, startTime: firstStart, endTime: firstEnd),
                EvidenceOccurrence(atomOrdinal: 940, startTime: secondStart, endTime: secondEnd),
            ]
        )
    }

    @Test("A window BETWEEN two mentions sees neither — the hull is not a place")
    func windowBetweenMentionsSeesNothing() {
        let entry = repeatedEntry()
        // The hull [60, 3601] covers this window completely; no mention does.
        #expect(entry.locatedInTimeWindow(start: 1_800, end: 1_860) == nil)
        // Proof the arm is not vacuous: the hull selector DOES pick it up.
        #expect(entry.coverageStartTime <= 1_860 && entry.coverageEndTime >= 1_800)
    }

    @Test("A window on the LATER mention gets the entry located THERE, not at the first")
    func laterMentionRelocatesTheEntry() throws {
        let entry = repeatedEntry()
        let located = try #require(entry.locatedInTimeWindow(start: 3_590, end: 3_650))
        #expect(located.atomOrdinal == 940, "must name the mention this window heard")
        #expect(located.startTime == 3_600)
        #expect(located.endTime == 3_601)
        // Identity, text and DENSITY are carried through untouched — the
        // rendered line is identical, only the position moves.
        #expect(located.evidenceRef == entry.evidenceRef)
        #expect(located.matchedText == entry.matchedText)
        #expect(located.count == entry.count)
        #expect(located.firstTime == entry.firstTime)
        #expect(located.lastTime == entry.lastTime)
    }

    @Test("A window on the REPRESENTATIVE returns the entry itself, occurrence list intact")
    func representativeReturnsSelfUnchanged() throws {
        let entry = repeatedEntry()
        let located = try #require(entry.locatedInTimeWindow(start: 55, end: 65))
        #expect(located == entry, "the representative branch must return `self`")
        #expect(
            located.occurrences?.count == 2,
            """
            `viewOfOccurrence` deliberately drops the occurrence list, so \
            rebuilding through it for the representative would silently delete \
            the population — got \(located.occurrences?.count.description ?? "nil").
            """
        )
    }

    @Test("A window spanning BOTH mentions selects the earliest, exactly once")
    func spanningWindowSelectsEarliest() throws {
        let entry = repeatedEntry()
        let located = try #require(entry.locatedInTimeWindow(start: 0, end: 7_200))
        #expect(located.atomOrdinal == 12)
        #expect(located == entry)
    }

    @Test("An unrepeated entry is unaffected in membership and in content")
    func unrepeatedEntryUnchanged() throws {
        let single = EvidenceEntry(
            evidenceRef: 0,
            category: .disclosurePhrase,
            matchedText: "sponsored by",
            normalizedText: "sponsored by",
            atomOrdinal: 3,
            startTime: 75,
            endTime: 76,
            count: 1,
            firstTime: 75,
            lastTime: 76,
            occurrences: [EvidenceOccurrence(atomOrdinal: 3, startTime: 75, endTime: 76)]
        )
        #expect(try #require(single.locatedInTimeWindow(start: 60, end: 120)) == single)
        #expect(single.locatedInTimeWindow(start: 200, end: 300) == nil)
    }

    @Test("The interval stays CLOSED on both ends")
    func closedIntervalPreserved() throws {
        // A zero-duration mention exactly on each boundary must still surface —
        // typical of short FM-bounded windows where the disclosure phrase
        // straddles the snap edge.
        let atStart = EvidenceEntry(
            evidenceRef: 1, category: .url, matchedText: "a.com", normalizedText: "a.com",
            atomOrdinal: 1, startTime: 60, endTime: 60, count: 1,
            occurrences: [EvidenceOccurrence(atomOrdinal: 1, startTime: 60, endTime: 60)]
        )
        let atEnd = EvidenceEntry(
            evidenceRef: 2, category: .url, matchedText: "b.com", normalizedText: "b.com",
            atomOrdinal: 2, startTime: 120, endTime: 120, count: 1,
            occurrences: [EvidenceOccurrence(atomOrdinal: 2, startTime: 120, endTime: 120)]
        )
        #expect(atStart.locatedInTimeWindow(start: 60, end: 120) != nil)
        #expect(atEnd.locatedInTimeWindow(start: 60, end: 120) != nil)
        // And one hair outside each edge is not visible.
        #expect(atStart.locatedInTimeWindow(start: 60.001, end: 120) == nil)
        #expect(atEnd.locatedInTimeWindow(start: 60, end: 119.999) == nil)
    }

    @Test("An entry with NO recorded occurrence list falls back to its representative")
    func unrecordedOccurrenceListFallsBackToRepresentative() {
        // `occurrences == nil` means "nobody recorded the population" — a value
        // persisted before playhead-04rx, or an entry built by a caller that
        // supplied no list. `anchorableOccurrences` resolves it to the
        // representative, which is NARROWER than the hull. That is the
        // deliberate direction: an unrecorded population must not license the
        // hull, because the hull is the thing that was wrong.
        let legacy = EvidenceEntry(
            evidenceRef: 3, category: .url, matchedText: "c.com", normalizedText: "c.com",
            atomOrdinal: 4, startTime: 60, endTime: 61, count: 2,
            firstTime: 60, lastTime: 3_601, occurrences: nil
        )
        #expect(legacy.locatedInTimeWindow(start: 55, end: 65) != nil)
        #expect(legacy.locatedInTimeWindow(start: 1_800, end: 1_860) == nil)
        #expect(legacy.locatedInTimeWindow(start: 3_590, end: 3_650) == nil)
    }

    @Test("locatedInWindow is domain-agnostic: the earliest ADMITTED occurrence wins")
    func earliestAdmittedOccurrenceWins() throws {
        let entry = repeatedEntry()
        // A predicate that admits BOTH must still yield one result, the first.
        let both = try #require(entry.locatedInWindow { _ in true })
        #expect(both.atomOrdinal == 12)
        // A predicate that admits only the later one yields the later one.
        let later = try #require(entry.locatedInWindow { $0.atomOrdinal == 940 })
        #expect(later.atomOrdinal == 940)
        #expect(entry.locatedInWindow { _ in false } == nil)
    }

    @Test("The FM prompt selector and the banner selector are the same answer")
    func promptSelectorSharesTheSameSelection() throws {
        // playhead-ad9n's `PromptEvidenceEntry.forWindow` now calls
        // `locatedInWindow`. Pin that they agree, so a future edit to one
        // cannot silently give the two callers different answers again — which
        // is how this defect came to have two homes.
        let entry = repeatedEntry()
        let lineRefByAtomOrdinal = [12: 100, 940: 400]
        let onlyLater = try #require(
            PromptEvidenceEntry.forWindow(
                entry: entry,
                allowedLineRefs: [400],
                lineRefByAtomOrdinal: lineRefByAtomOrdinal
            )
        )
        #expect(onlyLater.lineRef == 400)
        #expect(onlyLater.entry.atomOrdinal == 940)
        #expect(onlyLater.entry == entry.locatedInTimeWindow(start: 3_590, end: 3_650))

        let onlyFirst = try #require(
            PromptEvidenceEntry.forWindow(
                entry: entry,
                allowedLineRefs: [100],
                lineRefByAtomOrdinal: lineRefByAtomOrdinal
            )
        )
        #expect(onlyFirst.lineRef == 100)
        #expect(onlyFirst.entry == entry, "the representative branch returns `self` on both sides")

        #expect(
            PromptEvidenceEntry.forWindow(
                entry: entry,
                allowedLineRefs: [222],
                lineRefByAtomOrdinal: lineRefByAtomOrdinal
            ) == nil
        )
    }
}

// MARK: - Banner-level, through the real orchestrator

@Suite("Banner evidence names the window's own advertiser (playhead-rty3)", .timeLimit(.minutes(1)))
struct BannerEvidenceOccurrenceWindowTests {

    private static let assetId = "asset-1"
    private static let episodeId = "asset-1"
    private static let podcastId = "podcast-1"

    /// The corpus shape, minimised: a sponsor URL read in the pre-roll and
    /// again in the last ad break, plus a LOCAL disclosure phrase inside the
    /// mid-roll under test.
    ///
    /// The local entry is the anti-vacuity control. Without it a selector that
    /// returned nothing at all would pass every "must not carry" assertion.
    private static func corpusCatalog() -> EvidenceCatalog {
        EvidenceCatalog(
            analysisAssetId: assetId,
            transcriptVersion: "v-test",
            entries: [
                EvidenceEntry(
                    evidenceRef: 0,
                    category: .url,
                    matchedText: "NetSuite.ai",
                    normalizedText: "netsuite.ai",
                    atomOrdinal: 12,
                    startTime: 100,
                    endTime: 101,
                    count: 2,
                    firstTime: 100,
                    lastTime: 3_601,
                    occurrences: [
                        EvidenceOccurrence(atomOrdinal: 12, startTime: 100, endTime: 101),
                        EvidenceOccurrence(atomOrdinal: 940, startTime: 3_600, endTime: 3_601),
                    ]
                ),
                EvidenceEntry(
                    evidenceRef: 1,
                    category: .disclosurePhrase,
                    matchedText: "brought to you by",
                    normalizedText: "brought to you by",
                    atomOrdinal: 500,
                    startTime: 1_805,
                    endTime: 1_806,
                    count: 1,
                    firstTime: 1_805,
                    lastTime: 1_806,
                    occurrences: [
                        EvidenceOccurrence(atomOrdinal: 500, startTime: 1_805, endTime: 1_806)
                    ]
                ),
            ]
        )
    }

    private static func makeHarness() async throws -> SkipOrchestrator {
        let store = try await makeTestStore()
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let trustService = try await makeSkipTestTrustService(
            mode: "auto", trustScore: 0.9, observations: 10
        )
        let orchestrator = SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store)
        )
        await orchestrator.beginEpisode(
            analysisAssetId: assetId,
            episodeId: episodeId,
            podcastId: podcastId
        )
        await orchestrator.setSkipCueHandler { _ in }
        await orchestrator.setEvidenceCatalog(corpusCatalog())
        return orchestrator
    }

    private static func makeSuggestion(id: String, start: Double, end: Double) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 0.41,
            boundaryState: AdBoundaryState.segmentAggregated.rawValue,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: start,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    // MARK: - The suggest card — the one whose answer is banked

    @Test("SUGGEST card: a mid-roll does not name a sponsor read an hour away")
    func suggestCardCarriesOnlyItsOwnWindowsEvidence() async throws {
        let orchestrator = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "rty3-midroll", start: 1_800, end: 1_860),
            Self.makeSuggestion(id: "rty3-sentinel", start: 5_000, end: 5_060),
        ])
        await orchestrator.updatePlayheadTime(1_805)
        await orchestrator.updatePlayheadTime(5_005)

        let received = await reader.drain(until: "rty3-sentinel")
        let banner = try #require(
            received.first { $0.windowId == "rty3-midroll" },
            "mid-roll did not banner; got \(received.map(\.windowId))"
        )

        let refs = banner.evidenceCatalogEntries.map(\.evidenceRef).sorted()
        #expect(
            refs == [1],
            """
            The mid-roll [1800, 1860] heard the disclosure phrase at 1805 s and \
            nothing else. NetSuite.ai was read at 100 s and 3600 s, so its HULL \
            covers this window and its MENTIONS do not. Got refs \(refs).
            """
        )
        // `evidenceLines` is main-actor isolated (it is a `View` member), so the
        // hop is required rather than decorative — and it is worth taking: the
        // strings it returns are what the listener reads, and asserting on
        // `evidenceRef`s alone would not notice a rendering that reintroduced
        // the wrong sponsor by another route.
        let entries = banner.evidenceCatalogEntries
        let lines = await MainActor.run { AdBannerView.evidenceLines(for: entries) }
        #expect(
            !lines.contains("Sponsor link: NetSuite.ai"),
            """
            This is the listener-visible statement: the suggest card asks \
            whether the assessment was right and banks the answer, so a line \
            naming the wrong advertiser teaches the wrong thing. Got \(lines).
            """
        )
        #expect(lines.contains("Sponsor disclosure: \u{201C}brought to you by\u{201D}"),
                "anti-vacuity: the window's OWN evidence must still be shown; got \(lines)")
    }

    @Test("SUGGEST card: the window that DID hear the repeat carries it, located there")
    func suggestCardOnTheLaterMentionCarriesIt() async throws {
        let orchestrator = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        await orchestrator.receiveAdWindows([
            Self.makeSuggestion(id: "rty3-postroll", start: 3_580, end: 3_640),
            Self.makeSuggestion(id: "rty3-sentinel-2", start: 5_000, end: 5_060),
        ])
        await orchestrator.updatePlayheadTime(3_585)
        await orchestrator.updatePlayheadTime(5_005)

        let received = await reader.drain(until: "rty3-sentinel-2")
        let banner = try #require(
            received.first { $0.windowId == "rty3-postroll" },
            "post-roll did not banner; got \(received.map(\.windowId))"
        )
        let entry = try #require(
            banner.evidenceCatalogEntries.first { $0.evidenceRef == 0 },
            "the window that actually heard the second read must carry it"
        )
        #expect(entry.atomOrdinal == 940, "and it must name the mention THIS window heard")
        #expect(entry.startTime == 3_600)
        #expect(entry.count == 2, "density is the entry's, not the occurrence's")
    }

    // MARK: - The auto-skip card

    @Test("AUTO-SKIP card: a mid-roll does not name a sponsor read an hour away")
    func autoSkipCardCarriesOnlyItsOwnWindowsEvidence() async throws {
        let orchestrator = try await Self.makeHarness()
        var reader = BannerReader(await orchestrator.bannerItemStream())

        // playhead-bwxi: the injection ARMS; the PLAYHEAD ENTRY presents. So the
        // frame boundary is the second `updatePlayheadTime`, not the second
        // injection — the header comment this replaced said the opposite and
        // was true only while `evaluateAndPush` emitted inline.
        await orchestrator.injectUserMarkedAd(
            start: 1_800, end: 1_860, analysisAssetId: Self.assetId, windowId: "rty3-auto-midroll"
        )
        await orchestrator.injectUserMarkedAd(
            start: 5_000, end: 5_060, analysisAssetId: Self.assetId, windowId: "rty3-auto-sentinel"
        )
        await orchestrator.updatePlayheadTime(1_805)
        await orchestrator.updatePlayheadTime(5_005)

        let received = await reader.drain(until: "rty3-auto-sentinel")
        let banner = try #require(
            received.first { $0.windowId == "rty3-auto-midroll" },
            "auto-skip card did not emit; got \(received.map(\.windowId))"
        )
        let refs = banner.evidenceCatalogEntries.map(\.evidenceRef).sorted()
        #expect(
            refs == [1],
            "the auto-skip card carries only the evidence its own span heard; got \(refs)"
        )
        // `evidenceLines` is main-actor isolated (it is a `View` member), so the
        // hop is required rather than decorative — and it is worth taking: the
        // strings it returns are what the listener reads, and asserting on
        // `evidenceRef`s alone would not notice a rendering that reintroduced
        // the wrong sponsor by another route.
        let entries = banner.evidenceCatalogEntries
        let lines = await MainActor.run { AdBannerView.evidenceLines(for: entries) }
        #expect(!lines.contains("Sponsor link: NetSuite.ai"), "got \(lines)")
        #expect(lines.contains("Sponsor disclosure: \u{201C}brought to you by\u{201D}"),
                "anti-vacuity: the window's OWN evidence must still be shown; got \(lines)")
    }
}
