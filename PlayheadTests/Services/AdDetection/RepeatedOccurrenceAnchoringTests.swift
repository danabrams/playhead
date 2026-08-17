// RepeatedOccurrenceAnchoringTests.swift
// playhead-04rx: a repeated sponsor URL anchors EVERY mention, not just its first.
//
// The rails here were derived by enumerating the EVENTS the change can perform
// and asking which of them nothing observes — the method that closed
// playhead-q93o's fifth round. The events are:
//
//   the builder RECORDS a second occurrence          (RO01, RO02)
//   the builder DOES NOT record a second one for a
//     repeat inside a single atom                    (RO03)
//   each occurrence carries ITS OWN time             (RO04)
//   the entry RESOLVES an absent list to one anchor  (RO05, RO06)
//   the entry's per-occurrence VIEW keeps identity
//     and replaces position                          (RO07, RO08)
//   the projector ANCHORS every occurrence           (RO09)
//   the projector NAMES the occurrence it anchored   (RO10)
//   the projector STILL refuses untrusted categories (RO11)
//   a veto STILL wins                                (RO12)
//   the decoder KEEPS both anchors distinct          (RO13)
//   persistence ROUND-TRIPS the list                 (RO14)
//   persistence resolves an OLD row to ONE anchor,
//     never to zero                                  (RO15)
//   persistence WRITES NOTHING when there is no list (RO16)
//   the FM prompt DOES NOT MOVE                      (RO17, RO18)
//
// The last pair is the one a reader is most likely to assume rather than check:
// `renderForPrompt` and `evidenceRef` are the FM-side contract the bead said to
// watch, and the whole point of keeping ONE entry per (category, text) is that
// they do not move. An assumption is not a rail.

import Foundation
import Testing
@testable import Playhead

@Suite("Repeated occurrence anchoring (playhead-04rx)")
struct RepeatedOccurrenceAnchoringTests {

    // MARK: - Fixtures

    private static func atom(_ ordinal: Int, _ text: String, start: Double? = nil) -> TranscriptAtom {
        let begin = start ?? Double(ordinal) * 10
        return TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "asset-04rx",
                transcriptVersion: "v-04rx",
                atomOrdinal: ordinal
            ),
            contentHash: "hash-\(ordinal)",
            startTime: begin,
            endTime: begin + 6,
            text: text,
            chunkIndex: ordinal
        )
    }

    /// A pre-roll read of one sponsor, unrelated show talk, then a post-roll
    /// read of the SAME sponsor. This is the shape the bead is about.
    private static func preRollAndPostRoll() -> [TranscriptAtom] {
        [
            atom(0, "go to acmewidgets.com slash offer for a free trial"),
            atom(1, "so anyway the thing about deep sea trenches is the pressure"),
            atom(2, "and that is why the mantis shrimp punches so hard"),
            atom(3, "this episode was brought to you by acmewidgets.com slash offer")
        ]
    }

    private static func build(_ atoms: [TranscriptAtom]) -> EvidenceCatalog {
        EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "asset-04rx",
            transcriptVersion: "v-04rx"
        )
    }

    private static func project(
        _ catalog: EvidenceCatalog,
        _ atoms: [TranscriptAtom],
        masks: [Int: CorrectionState] = [:]
    ) async -> [AtomEvidence] {
        await AtomEvidenceProjector().project(
            regions: [],
            catalog: catalog,
            atoms: atoms,
            correctionMaskProvider: StubCorrectionMaskProvider(masks: masks)
        )
    }

    private struct StubCorrectionMaskProvider: CorrectionMaskProvider {
        let masks: [Int: CorrectionState]
        func correctionMasks(
            for ordinals: ClosedRange<Int>,
            in assetId: String
        ) async -> [Int: CorrectionState] { masks }
    }

    private static func urlEntry(_ catalog: EvidenceCatalog, _ text: String) -> EvidenceEntry? {
        catalog.entries.first { $0.category == .url && $0.normalizedText.contains(text) }
    }

    // MARK: - RO01/RO02 — the builder records the second occurrence

    @Test("a domain read twice in one episode keeps ONE entry and BOTH places")
    func repeatKeepsOneEntryAndBothPlaces() {
        let atoms = Self.preRollAndPostRoll()
        let catalog = Self.build(atoms)

        let matching = catalog.entries.filter {
            $0.category == .url && $0.normalizedText.contains("acmewidgets.com")
        }
        #expect(matching.count == 1, "dedup must still collapse the repeat into ONE catalog entry")

        let entry = try? #require(matching.first)
        #expect(entry?.anchorableOccurrences.map(\.atomOrdinal) == [0, 3])
        #expect(entry?.atomOrdinal == 0, "the representative stays the EARLIEST mention")
    }

    @Test("occurrences come out earliest first, and count them by ATOM")
    func occurrencesAreOrderedEarliestFirst() {
        let atoms = [
            Self.atom(0, "go to acmewidgets.com slash offer"),
            Self.atom(1, "unrelated show talk about trenches"),
            Self.atom(2, "again that is acmewidgets.com slash offer"),
            Self.atom(3, "one more time acmewidgets.com slash offer")
        ]
        let entry = Self.urlEntry(Self.build(atoms), "acmewidgets.com")
        let ordinals = entry?.anchorableOccurrences.map(\.atomOrdinal)
        #expect(ordinals == [0, 2, 3])
        #expect(ordinals == ordinals?.sorted(), "ascending, so the representative is occurrence 0")
    }

    // MARK: - RO03 — count and occurrences are different quantities

    @Test("the same text twice in ONE atom raises count but adds no second place")
    func repeatInsideOneAtomIsOnePlace() {
        let atoms = [
            Self.atom(0, "go to acmewidgets.com slash offer that is acmewidgets.com slash offer"),
            Self.atom(1, "unrelated show talk about trenches")
        ]
        let entry = Self.urlEntry(Self.build(atoms), "acmewidgets.com")
        #expect(entry?.anchorableOccurrences.count == 1,
                "one atom is one place to anchor, however many times the text appears in it")
        #expect((entry?.count ?? 0) >= 1)
    }

    // MARK: - RO04 — each occurrence carries its own time

    @Test("a later occurrence carries its OWN time, not the entry's earliest")
    func laterOccurrenceCarriesItsOwnTime() {
        let atoms = Self.preRollAndPostRoll()
        let entry = Self.urlEntry(Self.build(atoms), "acmewidgets.com")
        let occurrences = entry?.anchorableOccurrences ?? []
        #expect(occurrences.count == 2)
        guard occurrences.count == 2 else { return }
        #expect(occurrences[0].startTime < 10, "pre-roll mention sits in atom 0")
        #expect(occurrences[1].startTime >= 30, "post-roll mention sits in atom 3, thirty seconds in")
        #expect(entry?.startTime == occurrences[0].startTime,
                "the entry's own time still names the representative")
    }

    // MARK: - RO05/RO06 — absent and empty both resolve to ONE anchor

    @Test("an entry with no recorded occurrence list still offers its representative")
    func absentOccurrenceListResolvesToRepresentative() {
        let entry = EvidenceEntry(
            evidenceRef: 7, category: .url, matchedText: "acme.com", normalizedText: "acme.com",
            atomOrdinal: 42, startTime: 100, endTime: 105
        )
        #expect(entry.anchorableOccurrences.map(\.atomOrdinal) == [42])
        #expect(entry.anchorableOccurrences.first?.startTime == 100)
    }

    @Test("an EMPTY occurrence list resolves to the representative, never to nothing")
    func emptyOccurrenceListResolvesToRepresentative() {
        let entry = EvidenceEntry(
            evidenceRef: 7, category: .url, matchedText: "acme.com", normalizedText: "acme.com",
            atomOrdinal: 42, startTime: 100, endTime: 105, occurrences: []
        )
        #expect(entry.anchorableOccurrences.map(\.atomOrdinal) == [42],
                "an empty list would otherwise anchor NOTHING, which loses a span rather than adding one")
    }

    // MARK: - RO07/RO08 — the per-occurrence view

    @Test("the per-occurrence view keeps identity and density, and moves position")
    func occurrenceViewKeepsIdentityAndMovesPosition() {
        let entry = EvidenceEntry(
            evidenceRef: 3, category: .url, matchedText: "Acme.com", normalizedText: "acme.com",
            atomOrdinal: 1, startTime: 10, endTime: 12, count: 4, firstTime: 10, lastTime: 900,
            occurrences: [
                EvidenceOccurrence(atomOrdinal: 1, startTime: 10, endTime: 12),
                EvidenceOccurrence(atomOrdinal: 88, startTime: 890, endTime: 900)
            ]
        )
        let view = entry.viewOfOccurrence(entry.anchorableOccurrences[1])
        #expect(view.evidenceRef == 3)
        #expect(view.category == .url)
        #expect(view.matchedText == "Acme.com")
        #expect(view.normalizedText == "acme.com")
        #expect(view.count == 4, "density is a property of the ENTRY and rides through")
        #expect(view.firstTime == 10)
        #expect(view.lastTime == 900)
        #expect(view.atomOrdinal == 88, "position is the OCCURRENCE'S")
        #expect(view.startTime == 890)
        #expect(view.endTime == 900)
    }

    @Test("the view of the representative is the pre-04rx entry, unchanged")
    func viewOfRepresentativeIsUnchanged() {
        let entry = EvidenceEntry(
            evidenceRef: 3, category: .url, matchedText: "Acme.com", normalizedText: "acme.com",
            atomOrdinal: 1, startTime: 10, endTime: 12, count: 4, firstTime: 10, lastTime: 900,
            occurrences: [
                EvidenceOccurrence(atomOrdinal: 1, startTime: 10, endTime: 12),
                EvidenceOccurrence(atomOrdinal: 88, startTime: 890, endTime: 900)
            ]
        )
        let view = entry.viewOfOccurrence(entry.anchorableOccurrences[0])
        #expect(view.atomOrdinal == entry.atomOrdinal)
        #expect(view.startTime == entry.startTime)
        #expect(view.endTime == entry.endTime)
        #expect(view.occurrences == nil, "a view names ONE mention, so it carries no list")
    }

    // MARK: - RO09/RO10 — the projector

    @Test("the projector anchors the post-roll mention as well as the pre-roll one")
    func projectorAnchorsEveryOccurrence() async {
        let atoms = Self.preRollAndPostRoll()
        let evidence = await Self.project(Self.build(atoms), atoms)
        let anchored = evidence.filter(\.isAnchored).map(\.atomOrdinal)
        #expect(anchored.contains(0), "pre-roll")
        #expect(anchored.contains(3), "post-roll — silent before playhead-04rx")
    }

    @Test("the anchor on the post-roll atom names the POST-ROLL mention")
    func anchorNamesTheOccurrenceItAnchored() async {
        let atoms = Self.preRollAndPostRoll()
        let evidence = await Self.project(Self.build(atoms), atoms)
        let postRoll = evidence.first { $0.atomOrdinal == 3 }
        let refs = postRoll?.anchorProvenance ?? []
        var sawUrlAnchorNamingAtomThree = false
        for ref in refs {
            guard case .evidenceCatalog(let entry) = ref, entry.category == .url else { continue }
            sawUrlAnchorNamingAtomThree = sawUrlAnchorNamingAtomThree || entry.atomOrdinal == 3
            #expect(entry.atomOrdinal == 3,
                    "provenance on atom 3 must not claim the evidence sits back in the pre-roll")
            #expect(entry.startTime >= 30)
        }
        #expect(sawUrlAnchorNamingAtomThree)
    }

    // MARK: - RO11 — the untrusted category is still untrusted

    @Test("a repeated brand span still anchors nothing, in either place")
    func repeatedBrandSpanStillDoesNotAnchor() async {
        // "acmewidgets" seeds a brandSpan from the URL stem in commercial
        // context; brandSpan is excluded from anchoring and must stay excluded
        // for every occurrence, not just the first.
        let atoms = Self.preRollAndPostRoll()
        let catalog = Self.build(atoms)
        let brandOnly = EvidenceCatalog(
            analysisAssetId: catalog.analysisAssetId,
            transcriptVersion: catalog.transcriptVersion,
            entries: catalog.entries.filter { $0.category == .brandSpan }
        )
        #expect(!brandOnly.entries.isEmpty, "the fixture must actually produce a brand span")
        // The suite name says REPEATED, so measure it rather than assume it —
        // a brand span occurring once would pass this test for a reason that
        // has nothing to do with the widening.
        #expect(brandOnly.entries.contains { $0.anchorableOccurrences.count > 1 },
                "the fixture must produce a brand span in more than one atom")
        let evidence = await Self.project(brandOnly, atoms)
        #expect(evidence.allSatisfy { !$0.isAnchored },
                "brandSpan is too noisy to anchor — widening occurrences must not widen trust")
    }

    // MARK: - RO12 — a veto still wins

    @Test("a user veto on the post-roll atom still de-anchors it")
    func vetoStillWinsOverARepeatAnchor() async {
        let atoms = Self.preRollAndPostRoll()
        let evidence = await Self.project(Self.build(atoms), atoms, masks: [3: .userVetoed])
        let postRoll = evidence.first { $0.atomOrdinal == 3 }
        #expect(postRoll?.isAnchored == false)
        #expect(evidence.first { $0.atomOrdinal == 0 }?.isAnchored == true)
    }

    // MARK: - RO13 — the decoder keeps the two anchors apart

    @Test("two mentions of one sponsor inside one span survive as two provenance refs")
    func decoderKeepsBothOccurrenceRefs() async {
        // Adjacent atoms, both carrying the same URL, so one run covers both.
        let atoms = [
            Self.atom(0, "go to acmewidgets.com slash offer"),
            Self.atom(1, "that is acmewidgets.com slash offer"),
            Self.atom(2, "so anyway back to the mantis shrimp")
        ]
        let evidence = await Self.project(Self.build(atoms), atoms)
        let spans = MinimalContiguousSpanDecoder().decode(atoms: evidence, assetId: "asset-04rx")
        let span = spans.first { $0.firstAtomOrdinal == 0 }
        let urlRefs = (span?.anchorProvenance ?? []).compactMap { ref -> EvidenceEntry? in
            guard case .evidenceCatalog(let entry) = ref, entry.category == .url else { return nil }
            return entry
        }
        #expect(Set(urlRefs.map(\.atomOrdinal)) == [0, 1],
                "the decoder dedups provenance by (evidenceRef, atomOrdinal), so two mentions stay two")
    }

    // MARK: - RO14/RO15/RO16 — persistence

    @Test("the occurrence list survives a persistence round trip")
    func occurrencesRoundTrip() throws {
        let entry = EvidenceEntry(
            evidenceRef: 1, category: .url, matchedText: "acme.com", normalizedText: "acme.com",
            atomOrdinal: 4, startTime: 40, endTime: 42, count: 2, firstTime: 40, lastTime: 900,
            occurrences: [
                EvidenceOccurrence(atomOrdinal: 4, startTime: 40, endTime: 42),
                EvidenceOccurrence(atomOrdinal: 90, startTime: 895, endTime: 900)
            ]
        )
        let data = try JSONEncoder().encode(entry)
        let decoded = try JSONDecoder().decode(EvidenceEntry.self, from: data)
        #expect(decoded.anchorableOccurrences.map(\.atomOrdinal) == [4, 90])
        #expect(decoded.anchorableOccurrences[1].startTime == 895)
    }

    @Test("a row persisted before playhead-04rx decodes to ONE anchor, never to zero")
    func legacyRowResolvesToOneAnchor() throws {
        let legacy = """
        {"evidenceRef":2,"category":"url","matchedText":"acme.com","normalizedText":"acme.com",
         "atomOrdinal":11,"count":3,"firstTime":10,"lastTime":900,"startTime":10,"endTime":12}
        """
        let decoded = try JSONDecoder().decode(EvidenceEntry.self, from: Data(legacy.utf8))
        #expect(decoded.occurrences == nil, "absent means nobody recorded the population")
        #expect(decoded.anchorableOccurrences.map(\.atomOrdinal) == [11],
                "resolving an old row to zero anchors would DELETE a persisted span's provenance")
    }

    @Test("an entry with no occurrence list writes no occurrences key at all")
    func entryWithoutOccurrencesEncodesNoKey() throws {
        let entry = EvidenceEntry(
            evidenceRef: 1, category: .url, matchedText: "acme.com", normalizedText: "acme.com",
            atomOrdinal: 4, startTime: 40, endTime: 42
        )
        let json = String(decoding: try JSONEncoder().encode(entry), as: UTF8.self)
        #expect(!json.contains("occurrences"),
                "the projector pins every ref to ONE occurrence, so persisted provenance stays byte-stable")
    }

    // MARK: - RO17/RO18 — the FM side does not move

    @Test("the FM prompt rendering is identical with and without occurrence lists")
    func promptRenderingIsUnchanged() {
        let atoms = Self.preRollAndPostRoll()
        let catalog = Self.build(atoms)
        let stripped = EvidenceCatalog(
            analysisAssetId: catalog.analysisAssetId,
            transcriptVersion: catalog.transcriptVersion,
            entries: catalog.entries.map {
                EvidenceEntry(
                    evidenceRef: $0.evidenceRef, category: $0.category,
                    matchedText: $0.matchedText, normalizedText: $0.normalizedText,
                    atomOrdinal: $0.atomOrdinal, startTime: $0.startTime, endTime: $0.endTime,
                    count: $0.count, firstTime: $0.firstTime, lastTime: $0.lastTime
                )
            }
        )
        #expect(catalog.renderForPrompt() == stripped.renderForPrompt())
        #expect(!catalog.renderForPrompt().isEmpty)
    }

    @Test("a repeat produces one evidenceRef, and refs stay a gapless zero-based run")
    func evidenceRefsStayStable() {
        let atoms = Self.preRollAndPostRoll()
        let catalog = Self.build(atoms)
        #expect(catalog.entries.map(\.evidenceRef) == Array(0 ..< catalog.entries.count))
        let urls = catalog.entries.filter {
            $0.category == .url && $0.normalizedText.contains("acmewidgets.com")
        }
        #expect(urls.count == 1, "two reads of one domain must not become two refs")
        // The prompt names the representative atom, exactly as before.
        #expect(catalog.renderForPrompt().contains("atom \(urls.first?.atomOrdinal ?? -1)"))
    }
}
