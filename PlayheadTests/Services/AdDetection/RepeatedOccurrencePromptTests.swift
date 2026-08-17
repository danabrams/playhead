// RepeatedOccurrencePromptTests.swift
// playhead-ad9n: the FM half of first-occurrence evidence dedup.
//
// playhead-04rx made an `EvidenceEntry` remember every atom its text was said
// in, and taught the PROJECTOR to anchor all of them. Two FM-side consumers
// still read `entry.atomOrdinal` — the representative, i.e. the EARLIEST
// mention:
//
//   * `FoundationModelClassifier.promptEvidenceEntries` decided which catalog
//     entries ride a refinement prompt, so an entry whose first mention was a
//     pre-roll was dropped from every later window. The model refining a
//     post-roll got no evidence ref for a sponsor URL that the same prompt
//     carried in its own transcript lines.
//   * `TargetedWindowNarrower.evidenceLineRefs` decided which windows are
//     scanned AT ALL, so a repeated sponsor's later reads seeded no scan window
//     and got no 20-atom lookback.
//
// These tests pin the fix from both directions: the repeat is now visible, and
// everything that is NOT a repeat is byte-identical.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-ad9n: a repeated sponsor read is visible to the model reading it")
struct RepeatedOccurrencePromptTests {

    // MARK: - Fixture

    /// An episode with ONE advertiser read twice: a pre-roll at atom 1 and a
    /// post-roll at atom 41, with unrelated show content in between. The two
    /// reads collapse to one catalog entry whose representative is the pre-roll
    /// — which is the whole shape this bead is about.
    static func atoms(
        repeatOrdinal: Int = 41,
        atomCount: Int = 60,
        adText: String = "just go to acmeco.com slash offer today"
    ) -> [TranscriptAtom] {
        (0 ..< atomCount).map { ordinal in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "ad9n",
                    transcriptVersion: "ad9n-v1",
                    atomOrdinal: ordinal
                ),
                contentHash: "ad9n-\(ordinal)",
                startTime: Double(ordinal) * 10,
                endTime: Double(ordinal) * 10 + 9,
                text: (ordinal == 1 || ordinal == repeatOrdinal)
                    ? adText
                    : "the hosts keep talking about something entirely unrelated here",
                chunkIndex: ordinal
            )
        }
    }

    static func catalog(_ atoms: [TranscriptAtom]) -> EvidenceCatalog {
        EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "ad9n",
            transcriptVersion: "ad9n-v1"
        )
    }

    /// One segment per atom: the geometry stays trivial so a failure is about
    /// occurrence selection and never about segmentation.
    static func segments(_ atoms: [TranscriptAtom]) -> [AdTranscriptSegment] {
        atoms.map { AdTranscriptSegment(atoms: [$0], segmentIndex: $0.atomKey.atomOrdinal) }
    }

    static func lineRefByAtomOrdinal(_ segments: [AdTranscriptSegment]) -> [Int: Int] {
        var mapping: [Int: Int] = [:]
        for segment in segments {
            for atom in segment.atoms {
                mapping[atom.atomKey.atomOrdinal] = segment.segmentIndex
            }
        }
        return mapping
    }

    static func urlEntry(_ catalog: EvidenceCatalog) throws -> EvidenceEntry {
        let entry = try #require(catalog.entries.first { $0.category == .url })
        // The fixture is only meaningful if dedup really collapsed two reads.
        #expect(entry.count >= 2)
        #expect(entry.anchorableOccurrences.count == 2)
        return entry
    }

    // MARK: - The prompt half

    @Test("a window over the SECOND read gets an evidence line for it")
    func laterOccurrenceEarnsAPromptLine() throws {
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let map = Self.lineRefByAtomOrdinal(Self.segments(atoms))
        let entry = try Self.urlEntry(catalog)

        // The representative is the pre-roll and is nowhere near this window.
        #expect(entry.atomOrdinal == 1)
        let selected = try #require(PromptEvidenceEntry.forWindow(
            entry: entry,
            allowedLineRefs: [40, 41, 42],
            lineRefByAtomOrdinal: map
        ))
        #expect(selected.lineRef == 41)
        #expect(selected.entry.evidenceRef == entry.evidenceRef)
    }

    @Test("the re-located entry carries the OCCURRENCE's position, not the representative's")
    func relocatedEntryCarriesItsOwnTime() throws {
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let map = Self.lineRefByAtomOrdinal(Self.segments(atoms))
        let entry = try Self.urlEntry(catalog)

        let selected = try #require(PromptEvidenceEntry.forWindow(
            entry: entry,
            allowedLineRefs: [41],
            lineRefByAtomOrdinal: map
        ))
        // `PromptEvidenceEntry.entry` becomes `ResolvedEvidenceAnchor.entry`.
        // A record pointing at line 41 while carrying the pre-roll's ~10s is a
        // value naming one thing and read as another.
        #expect(selected.entry.atomOrdinal == 41)
        #expect(selected.entry.startTime >= 410)
        #expect(selected.entry.startTime != entry.startTime)
        // Identity and density survive untouched: the catalog span still spans
        // BOTH reads, which is what the prompt renders as "×2, 10s–419s".
        #expect(selected.entry.count == entry.count)
        #expect(selected.entry.firstTime == entry.firstTime)
        #expect(selected.entry.lastTime == entry.lastTime)
        #expect(selected.entry.matchedText == entry.matchedText)
    }

    @Test("the rendered line differs only in its line ref — text, count and span are unmoved")
    func renderedLineIsStableApartFromPosition() throws {
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let map = Self.lineRefByAtomOrdinal(Self.segments(atoms))
        let entry = try Self.urlEntry(catalog)

        let atPreRoll = try #require(PromptEvidenceEntry.forWindow(
            entry: entry, allowedLineRefs: [1], lineRefByAtomOrdinal: map
        ))
        let atPostRoll = try #require(PromptEvidenceEntry.forWindow(
            entry: entry, allowedLineRefs: [41], lineRefByAtomOrdinal: map
        ))
        #expect(atPreRoll.renderForPrompt() == atPostRoll.renderForPrompt()
            .replacingOccurrences(of: "line 41", with: "line 1"))
    }

    @Test("an entry whose representative IS in the window is byte-identical to pre-ad9n")
    func representativeInWindowIsUnchanged() throws {
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let map = Self.lineRefByAtomOrdinal(Self.segments(atoms))

        for entry in catalog.entries {
            guard let representativeLine = map[entry.atomOrdinal] else { continue }
            let selected = try #require(PromptEvidenceEntry.forWindow(
                entry: entry,
                // A window spanning the whole episode: every occurrence is in.
                allowedLineRefs: Set(0 ..< 60),
                lineRefByAtomOrdinal: map
            ))
            #expect(selected.lineRef == representativeLine)
            // Byte-identical, INCLUDING the occurrence list — the selector must
            // not quietly swap in a `viewOfOccurrence` (which drops it) for the
            // representative case.
            #expect(selected.entry == entry)
        }
    }

    @Test("an entry earns at most ONE line even when several of its mentions are in the window")
    func atMostOneLinePerEntry() throws {
        // Both reads inside one window: the prompt must not gain a second
        // `[E0]`, because `evidenceRef` is an identity the model points back at.
        let atoms = Self.atoms(repeatOrdinal: 3, atomCount: 10)
        let catalog = Self.catalog(atoms)
        let map = Self.lineRefByAtomOrdinal(Self.segments(atoms))
        let entry = try Self.urlEntry(catalog)

        let allowed = Set(0 ..< 10)
        let selected = try #require(PromptEvidenceEntry.forWindow(
            entry: entry, allowedLineRefs: allowed, lineRefByAtomOrdinal: map
        ))
        // The earliest in-window occurrence wins, which for a window containing
        // the representative IS the representative.
        #expect(selected.lineRef == 1)

        // And the whole catalog renders one line per entry, not one per mention.
        let all = catalog.entries.compactMap {
            PromptEvidenceEntry.forWindow(
                entry: $0, allowedLineRefs: allowed, lineRefByAtomOrdinal: map
            )
        }
        #expect(Set(all.map(\.entry.evidenceRef)).count == all.count)
    }

    @Test("an entry with no mention in the window earns no line")
    func absentEntryEarnsNoLine() throws {
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let map = Self.lineRefByAtomOrdinal(Self.segments(atoms))
        let entry = try Self.urlEntry(catalog)

        #expect(PromptEvidenceEntry.forWindow(
            entry: entry,
            allowedLineRefs: [20, 21, 22],
            lineRefByAtomOrdinal: map
        ) == nil)
    }

    @Test("an entry with no recorded occurrence list still resolves to its representative")
    func unrecordedOccurrencesFallBackToRepresentative() {
        // A span decoded from a row persisted before playhead-04rx carries no
        // occurrence list. `nil` means "nobody recorded the population", and the
        // only safe reading is the pre-04rx one — anchor the representative
        // rather than nothing.
        let entry = EvidenceEntry(
            evidenceRef: 7,
            category: .url,
            matchedText: "legacy.com",
            normalizedText: "legacy.com",
            atomOrdinal: 4,
            startTime: 40,
            endTime: 44,
            occurrences: nil
        )
        let selected = PromptEvidenceEntry.forWindow(
            entry: entry, allowedLineRefs: [9], lineRefByAtomOrdinal: [4: 9]
        )
        #expect(selected?.lineRef == 9)
        #expect(selected?.entry.atomOrdinal == 4)
        #expect(PromptEvidenceEntry.forWindow(
            entry: entry, allowedLineRefs: [11], lineRefByAtomOrdinal: [4: 9]
        ) == nil)
    }

    @Test("the pre-ad9n selector is exactly forWindow over a representative-only catalog")
    func preAd9nSelectorIsTheDegenerateCase() throws {
        // The corpus lane's baseline arm is a literal transcription of the
        // deleted `guard let lineRef = lineRefByAtomOrdinal[entry.atomOrdinal]`
        // body. It asserts this equivalence on every window of every asset; this
        // is the same claim at fixture scale, so a build with no corpus staged
        // still carries it.
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let map = Self.lineRefByAtomOrdinal(Self.segments(atoms))
        let truncated = RepeatedOccurrencePromptCorpusEvalTests.truncatedToRepresentative(catalog)

        for window in [Set(0 ..< 60), Set([1, 2, 3]), Set([40, 41, 42]), Set([20])] {
            let frozen = RepeatedOccurrencePromptCorpusEvalTests.preAd9nEvidenceRefs(
                catalog: catalog,
                lineRefs: Array(window),
                lineRefByAtomOrdinal: map
            )
            let viaProduction = RepeatedOccurrencePromptCorpusEvalTests.evidenceRefs(
                catalog: truncated,
                lineRefs: Array(window),
                lineRefByAtomOrdinal: map
            )
            #expect(
                RepeatedOccurrencePromptCorpusEvalTests.keys(frozen)
                    == RepeatedOccurrencePromptCorpusEvalTests.keys(viaProduction)
            )
        }
        // And the fixture must actually discriminate: over the post-roll window
        // the two arms MUST differ, or this test is measuring nothing.
        let frozenPostRoll = RepeatedOccurrencePromptCorpusEvalTests.preAd9nEvidenceRefs(
            catalog: catalog, lineRefs: [40, 41, 42], lineRefByAtomOrdinal: map
        )
        let livePostRoll = RepeatedOccurrencePromptCorpusEvalTests.evidenceRefs(
            catalog: catalog, lineRefs: [40, 41, 42], lineRefByAtomOrdinal: map
        )
        #expect(frozenPostRoll.isEmpty)
        #expect(!livePostRoll.isEmpty)
    }

    // MARK: - The prompt half, end to end

    @Test("planAdaptiveZoom puts the repeat's evidence ref in the plan the model is sent")
    func adaptiveZoomPlanCarriesTheRepeat() async throws {
        // The unit tests above pin `forWindow`. This one pins that
        // `promptEvidenceEntries` actually routes through it — a correct helper
        // nobody calls is the shape this repo keeps finding.
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let segments = Self.segments(atoms)
        let entry = try Self.urlEntry(catalog)

        let recorder = TestFMRuntime(
            contextSize: 4_096,
            coarseSchemaTokenCount: 128,
            refinementSchemaTokenCount: 256,
            tokenCountRule: { FoundationModelClassifier.fallbackTokenEstimate(for: $0) }
        )
        let classifier = FoundationModelClassifier(runtime: recorder.runtime, config: .default)

        let coarse = FMCoarseScanOutput(
            status: .success,
            windows: [
                FMCoarseWindowOutput(
                    windowIndex: 0,
                    lineRefs: [40, 41, 42],
                    startTime: 400,
                    endTime: 429,
                    transcriptQuality: .good,
                    screening: CoarseScreeningSchema(
                        disposition: .containsAd,
                        support: CoarseSupportSchema(
                            supportLineRefs: [41],
                            certainty: .moderate
                        )
                    ),
                    latencyMillis: 10
                )
            ],
            latencyMillis: 25,
            prewarmHit: false
        )
        let plans = try await classifier.planAdaptiveZoom(
            coarse: coarse,
            segments: segments,
            evidenceCatalog: catalog
        )
        let plan = try #require(plans.first)
        #expect(plan.promptEvidence.contains { $0.entry.evidenceRef == entry.evidenceRef })
        #expect(plan.prompt.contains("[E\(entry.evidenceRef)]"))
        // And the line points at the post-roll, not at the pre-roll.
        let promptEntry = try #require(
            plan.promptEvidence.first { $0.entry.evidenceRef == entry.evidenceRef }
        )
        #expect(promptEntry.lineRef == 41)
    }

    // MARK: - The narrowing half

    @Test("the targeted phase nominates the SECOND ad break, which it previously never saw")
    func narrowingSeedsEveryOccurrence() throws {
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let segments = Self.segments(atoms)
        let baselineCatalog = RepeatedOccurrencePromptCorpusEvalTests
            .truncatedToRepresentative(catalog)

        func nominated(_ catalog: EvidenceCatalog) -> Set<Int> {
            let result = TargetedWindowNarrower.narrow(
                phase: .scanHarvesterProposals,
                inputs: TargetedWindowNarrower.Inputs(
                    analysisAssetId: "ad9n",
                    podcastId: "ad9n",
                    transcriptVersion: "ad9n-v1",
                    segments: segments,
                    evidenceCatalog: catalog,
                    auditWindowSampleRate: 0.1
                )
            )
            return Set((result.narrowedSegments ?? []).map(\.segmentIndex))
        }

        // Before: the post-roll's whole neighbourhood is unscanned by this phase.
        #expect(nominated(baselineCatalog).isDisjoint(with: Set(35 ... 47)))
        // After: the post-roll is nominated, with ad body either side of it.
        let treatment = nominated(catalog)
        #expect(treatment.contains(41))
        #expect(treatment.contains(40))
        // How MUCH lookback survives is xsdz.2's business, not this bead's: the
        // 20-atom seed expansion runs first and the lexical-cluster snap then
        // pulls the merged interval back toward the ad-dense core with a
        // 3-segment margin. So assert that a real ad-body margin is nominated,
        // not that the full 20 atoms are — the latter would be asserting that
        // the tightening is off.
        #expect(treatment.contains(38))
    }

    @Test("seeding every occurrence is strictly additive — no window stops being scanned")
    func narrowingIsAdditive() throws {
        let atoms = Self.atoms()
        let catalog = Self.catalog(atoms)
        let segments = Self.segments(atoms)
        let baselineCatalog = RepeatedOccurrencePromptCorpusEvalTests
            .truncatedToRepresentative(catalog)

        func nominated(_ catalog: EvidenceCatalog, _ phase: BackfillJobPhase) -> Set<Int> {
            let result = TargetedWindowNarrower.narrow(
                phase: phase,
                inputs: TargetedWindowNarrower.Inputs(
                    analysisAssetId: "ad9n",
                    podcastId: "ad9n",
                    transcriptVersion: "ad9n-v1",
                    segments: segments,
                    evidenceCatalog: catalog,
                    auditWindowSampleRate: 0.1
                )
            )
            return Set((result.narrowedSegments ?? []).map(\.segmentIndex))
        }

        for phase in [BackfillJobPhase.scanHarvesterProposals, .metadataSeededRegion] {
            let baseline = nominated(baselineCatalog, phase)
            let treatment = nominated(catalog, phase)
            #expect(baseline.isSubset(of: treatment), "\(phase) lost segments")
            #expect(treatment.count > baseline.count, "\(phase) gained nothing — fixture is inert")
        }
    }

    @Test("a catalog with no repeats narrows byte-identically")
    func noRepeatsIsUnchanged() throws {
        // The control. If this fixture ALSO changed, the delta measured on the
        // corpus would be a property of the change rather than of repeats.
        let atoms = (0 ..< 30).map { ordinal in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "ad9n",
                    transcriptVersion: "ad9n-v1",
                    atomOrdinal: ordinal
                ),
                contentHash: "ad9n-solo-\(ordinal)",
                startTime: Double(ordinal) * 10,
                endTime: Double(ordinal) * 10 + 9,
                text: ordinal == 5
                    ? "just go to soloco.com slash offer today"
                    : "the hosts keep talking about something entirely unrelated here",
                chunkIndex: ordinal
            )
        }
        let catalog = Self.catalog(atoms)
        let segments = Self.segments(atoms)
        let baselineCatalog = RepeatedOccurrencePromptCorpusEvalTests
            .truncatedToRepresentative(catalog)
        let map = Self.lineRefByAtomOrdinal(segments)

        func nominated(_ catalog: EvidenceCatalog) -> Set<Int> {
            let result = TargetedWindowNarrower.narrow(
                phase: .scanHarvesterProposals,
                inputs: TargetedWindowNarrower.Inputs(
                    analysisAssetId: "ad9n",
                    podcastId: "ad9n",
                    transcriptVersion: "ad9n-v1",
                    segments: segments,
                    evidenceCatalog: catalog,
                    auditWindowSampleRate: 0.1
                )
            )
            return Set((result.narrowedSegments ?? []).map(\.segmentIndex))
        }
        #expect(nominated(catalog) == nominated(baselineCatalog))
        #expect(catalog.entries.allSatisfy { $0.anchorableOccurrences.count == 1 })
        #expect(
            RepeatedOccurrencePromptCorpusEvalTests.keys(
                RepeatedOccurrencePromptCorpusEvalTests.evidenceRefs(
                    catalog: catalog, lineRefs: Array(0 ..< 30), lineRefByAtomOrdinal: map
                )
            ) == RepeatedOccurrencePromptCorpusEvalTests.keys(
                RepeatedOccurrencePromptCorpusEvalTests.preAd9nEvidenceRefs(
                    catalog: catalog, lineRefs: Array(0 ..< 30), lineRefByAtomOrdinal: map
                )
            )
        )
    }
}
