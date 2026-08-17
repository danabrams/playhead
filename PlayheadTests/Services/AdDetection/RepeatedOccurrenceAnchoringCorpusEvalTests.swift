// RepeatedOccurrenceAnchoringCorpusEvalTests.swift
// playhead-04rx: the corpus lane that measures what first-occurrence evidence
// dedup costs in decoded spans, and what anchoring every occurrence costs in
// precision.
//
// WHY A LANE AND NOT A FIXTURE
// ----------------------------
// The claim under test is about REPEATS — the same sponsor domain read twice in
// one episode. A synthetic fixture can only ever contain the repeats its author
// thought to write, so the count it produces is a property of the fixture. The
// population that matters is a real device transcript corpus, where a repeat is
// either a second ad break or a host saying "like I said, betterhelp.com" and
// nobody chose which.
//
// WHAT IT RUNS
// ------------
// Production code, end to end, per asset:
//
//   TranscriptChunkCanonicalizer.canonicalize   (final replaces the fast
//                                                coverage it spans — this is
//                                                what `runBackfill` does FIRST,
//                                                and skipping it double-counts)
//   -> TranscriptAtomizer.atomize
//   -> EvidenceCatalogBuilder.build
//   -> AtomEvidenceProjector.project(regions: [], mask: none)
//   -> MinimalContiguousSpanDecoder.decode
//
// `regions: []` and `NoCorrectionMaskProvider` isolate the LEXICAL lane: every
// span reported here exists because the evidence catalog anchored an atom, so a
// delta between arms is attributable to the catalog and to nothing else.
//
// STAGING: `PLAYHEAD_04RX_CORPUS` (or `TEST_RUNNER_PLAYHEAD_04RX_CORPUS`) points
// at a JSON export of a device `analysis.sqlite`
// (`scripts/l2f-04rx-export-corpus.py`). The export is a copy of a device pull
// and is NOT in the repo, so with the variable unset this SKIPS. Set
// `PLAYHEAD_04RX_OUT` to write the full per-span report as JSON for
// adjudication.

import Foundation
import XCTest
@testable import Playhead

final class RepeatedOccurrenceAnchoringCorpusEvalTests: XCTestCase {

    // MARK: - Corpus export shape

    struct Corpus: Decodable {
        struct Asset: Decodable {
            struct Chunk: Decodable {
                let id: String
                let segmentFingerprint: String
                let chunkIndex: Int
                let startTime: Double
                let endTime: Double
                let text: String
                let normalizedText: String
                let pass: String
                let modelVersion: String
                let transcriptVersion: String?
                let atomOrdinal: Int?
                let speakerId: Int?
                let avgConfidence: Double?
            }
            struct Window: Decodable {
                let startTime: Double
                let endTime: Double
                let confidence: Double
                let boundaryState: String
                let decisionState: String
                let detectorVersion: String
                let advertiser: String?
                let evidenceSources: String?
                let eligibilityGate: String?
                let startEdgeAnchor: String?
                let endEdgeAnchor: String?
            }
            let assetId: String
            let title: String?
            let durationSec: Double?
            let chunks: [Chunk]
            let windows: [Window]
        }
        let source: String
        let assets: [Asset]
    }

    // MARK: - Report shape

    struct SpanRow: Encodable {
        let assetId: String
        let title: String?
        let startTime: Double
        let endTime: Double
        let firstAtomOrdinal: Int
        let lastAtomOrdinal: Int
        let provenance: [String]
        let text: String
        /// Persisted device windows this span overlaps, so a span can be read
        /// against what the device itself believed.
        let overlappingWindows: [String]
    }

    struct EntryRow: Encodable {
        let evidenceRef: Int
        let category: String
        let matchedText: String
        let normalizedText: String
        let atomOrdinal: Int
        let count: Int
        let firstTime: Double
        let lastTime: Double
    }

    /// One occurrence that the `allOccurrences` arm anchors and the `firstOnly`
    /// arm does not. This is the row precision is adjudicated from: the atom's
    /// own text plus three atoms either side, so a reader can tell an ad read
    /// from "like I said, betterhelp.com" without leaving the report.
    struct OccurrenceRow: Encodable {
        let assetId: String
        let title: String?
        let evidenceRef: Int
        let category: String
        let matchedText: String
        let normalizedText: String
        let atomOrdinal: Int
        let atomStart: Double
        let atomEnd: Double
        let representativeAtomOrdinal: Int
        let representativeStart: Double
        let atomText: String
        let contextBefore: String
        let contextAfter: String
        let overlappingWindows: [String]
        /// True when this occurrence's atom ends up inside a span the
        /// `allOccurrences` arm emits — i.e. the anchor survived the 5 s floor
        /// and actually became a claim.
        let landedInSpan: Bool
    }

    struct AssetReport: Encodable {
        let assetId: String
        let title: String?
        let durationSec: Double?
        let rawChunks: Int
        let canonicalChunks: Int
        let atoms: Int
        let catalogEntries: Int
        let anchoringEntries: Int
        let repeatedAnchoringEntries: Int
        let firstOnlyAnchoredAtoms: Int
        let allOccurrencesAnchoredAtoms: Int
        let firstOnlySpans: [SpanRow]
        let allOccurrencesSpans: [SpanRow]
        let repeatedEntries: [EntryRow]
        let newlyAnchoredOccurrences: [OccurrenceRow]
        /// Prompt rendering, both arms, for the FM-side stability check.
        let promptLength: Int
        let promptSHA: String
        let evidenceRefs: [Int]
    }

    struct Report: Encodable {
        let source: String
        let arm: String
        let assets: Int
        let rawChunks: Int
        let canonicalChunks: Int
        let atoms: Int
        let firstOnlySpanCount: Int
        let allOccurrencesSpanCount: Int
        let newlyAnchoredOccurrenceCount: Int
        let totalPromptLength: Int
        let perAsset: [AssetReport]
    }

    /// The projector's own anchoring set, restated here so the lane can count
    /// which catalog entries could ever anchor. Pinned against the projector by
    /// `RepeatedOccurrenceAnchoringTests.laneAnchoringSetMatchesProjector`.
    static let anchoringCategories: Set<EvidenceCategory> = [
        .url, .promoCode, .disclosurePhrase, .ctaPhrase
    ]

    // MARK: - The lane

    func testRepeatedOccurrenceAnchoringOnDeviceCorpus() throws {
        let environment = ProcessInfo.processInfo.environment
        let key = environment["PLAYHEAD_04RX_CORPUS"]
            ?? environment["TEST_RUNNER_PLAYHEAD_04RX_CORPUS"]
        guard let path = key, FileManager.default.fileExists(atPath: path) else {
            throw XCTSkip(
                "corpus lane needs PLAYHEAD_04RX_CORPUS pointing at a device analysis.sqlite JSON export"
            )
        }
        let corpus = try JSONDecoder().decode(
            Corpus.self,
            from: try Data(contentsOf: URL(fileURLWithPath: path))
        )
        try XCTSkipIf(corpus.assets.isEmpty, "corpus export has no assets")

        var perAsset: [AssetReport] = []
        var totalRaw = 0
        var totalCanonical = 0
        var totalAtoms = 0

        for asset in corpus.assets where !asset.chunks.isEmpty {
            let report = try evaluate(asset: asset)
            totalRaw += report.rawChunks
            totalCanonical += report.canonicalChunks
            totalAtoms += report.atoms
            perAsset.append(report)
        }

        let firstOnlyTotal = perAsset.reduce(0) { $0 + $1.firstOnlySpans.count }
        let allTotal = perAsset.reduce(0) { $0 + $1.allOccurrencesSpans.count }
        let newTotal = perAsset.reduce(0) { $0 + $1.newlyAnchoredOccurrences.count }
        let repeatTotal = perAsset.reduce(0) { $0 + $1.repeatedAnchoringEntries }
        let promptTotal = perAsset.reduce(0) { $0 + $1.promptLength }

        let arm = try Self.armLabel()
        let report = Report(
            source: corpus.source,
            arm: arm,
            assets: perAsset.count,
            rawChunks: totalRaw,
            canonicalChunks: totalCanonical,
            atoms: totalAtoms,
            firstOnlySpanCount: firstOnlyTotal,
            allOccurrencesSpanCount: allTotal,
            newlyAnchoredOccurrenceCount: newTotal,
            totalPromptLength: promptTotal,
            perAsset: perAsset
        )

        print("[04rx] arm=\(arm) source=\(corpus.source)")
        print("[04rx] assets=\(perAsset.count) rawChunks=\(totalRaw) canonicalChunks=\(totalCanonical) atoms=\(totalAtoms)")
        print("[04rx] decoded spans: firstOnly=\(firstOnlyTotal) allOccurrences=\(allTotal) delta=\(allTotal - firstOnlyTotal)")
        print("[04rx] repeated anchoring entries=\(repeatTotal) newlyAnchoredOccurrences=\(newTotal)")
        print("[04rx] renderForPrompt total characters=\(promptTotal) (both arms — one entry per (category, text))")
        for entry in perAsset
        where !entry.allOccurrencesSpans.isEmpty || !entry.newlyAnchoredOccurrences.isEmpty {
            print("[04rx]   \(entry.assetId.prefix(8)) spans \(entry.firstOnlySpans.count)->\(entry.allOccurrencesSpans.count) anchoredAtoms \(entry.firstOnlyAnchoredAtoms)->\(entry.allOccurrencesAnchoredAtoms) newOcc=\(entry.newlyAnchoredOccurrences.count) prompt=\(entry.promptLength) \(entry.title ?? "")")
        }
        for entry in perAsset {
            for row in entry.newlyAnchoredOccurrences {
                print("[04rx]   NEW \(entry.assetId.prefix(8)) t=\(String(format: "%.2f", row.atomStart))-\(String(format: "%.2f", row.atomEnd)) atom=\(row.atomOrdinal) rep=\(row.representativeAtomOrdinal)@\(String(format: "%.2f", row.representativeStart)) [\(row.category)] \"\(row.normalizedText)\" landedInSpan=\(row.landedInSpan) windows=\(row.overlappingWindows.count)")
            }
        }

        if let outPath = environment["PLAYHEAD_04RX_OUT"]
            ?? environment["TEST_RUNNER_PLAYHEAD_04RX_OUT"] {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            try encoder.encode(report).write(to: URL(fileURLWithPath: outPath))
            print("[04rx] wrote \(outPath)")
        }

        // The lane is a measurement, not a threshold. The only thing asserted
        // is that it actually measured something — a silently empty corpus
        // would otherwise print a reassuring "0 -> 0" and read as a result.
        XCTAssertGreaterThan(totalAtoms, 0, "corpus produced no atoms")
        XCTAssertGreaterThan(firstOnlyTotal, 0, "corpus produced no baseline spans")
        XCTAssertGreaterThan(repeatTotal, 0, "corpus contains no repeated anchoring entry — nothing to measure")
    }

    /// Which arm this build measures. The label is printed and written into the
    /// report so a JSON file on disk cannot be misread as the other arm.
    ///
    /// It is DERIVED from production code rather than typed, because a hand-set
    /// label is exactly the shape this repo keeps getting wrong: a value that
    /// names one thing read as though it named another. A catalog entry built
    /// from two occurrences of the same text either offers the projector both
    /// ordinals or it does not, and that is what the label reports.
    /// Probe atoms: the same sponsor URL read at ordinal 0 and again at ordinal
    /// 2, with unrelated text between. Deduplication collapses the two reads
    /// into one entry either way; what differs between the arms is whether the
    /// projector anchors ONE atom or BOTH.
    static func armProbeAtoms() -> [TranscriptAtom] {
        (0 ..< 3).map { ordinal in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "arm-probe",
                    transcriptVersion: "arm-probe",
                    atomOrdinal: ordinal
                ),
                contentHash: "arm-probe-\(ordinal)",
                startTime: Double(ordinal) * 10,
                endTime: Double(ordinal) * 10 + 5,
                text: ordinal == 1
                    ? "and now for something completely unrelated"
                    : "go to armprobe.com slash offer",
                chunkIndex: ordinal
            )
        }
    }

    static func armLabel() throws -> String {
        let atoms = armProbeAtoms()
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "arm-probe",
            transcriptVersion: "arm-probe"
        )
        let projector = AtomEvidenceProjector()
        let evidence = try Self.awaitValue {
            await projector.project(
                regions: [],
                catalog: catalog,
                atoms: atoms,
                correctionMaskProvider: NoCorrectionMaskProvider()
            )
        }
        let anchored = evidence.filter(\.isAnchored).map(\.atomOrdinal)
        return anchored == [0, 2] ? "allOccurrences"
            : anchored == [0] ? "firstOccurrence"
            : "unrecognised(\(anchored))"
    }

    // MARK: - Per-asset evaluation

    private func evaluate(asset: Corpus.Asset) throws -> AssetReport {
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
            normalizationHash: "04rx-lane",
            sourceHash: "04rx-lane"
        )
        let atoms = atomized.atoms
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: asset.assetId,
            transcriptVersion: atomized.version.transcriptVersion
        )

        // The `firstOnly` arm: the SAME catalog with every entry's occurrence
        // list truncated to its representative. That is exactly the data the
        // projector saw before playhead-04rx, so this arm replays the shipped
        // pipeline rather than approximating it — and the number it produces was
        // checked against an unmodified `main` build of this lane.
        let firstOnlyCatalog = EvidenceCatalog(
            analysisAssetId: catalog.analysisAssetId,
            transcriptVersion: catalog.transcriptVersion,
            entries: catalog.entries.map { entry in
                EvidenceEntry(
                    evidenceRef: entry.evidenceRef,
                    category: entry.category,
                    matchedText: entry.matchedText,
                    normalizedText: entry.normalizedText,
                    atomOrdinal: entry.atomOrdinal,
                    startTime: entry.startTime,
                    endTime: entry.endTime,
                    count: entry.count,
                    firstTime: entry.firstTime,
                    lastTime: entry.lastTime,
                    occurrences: [EvidenceOccurrence(
                        atomOrdinal: entry.atomOrdinal,
                        startTime: entry.startTime,
                        endTime: entry.endTime
                    )]
                )
            }
        )

        let atomsByOrdinal = Dictionary(
            uniqueKeysWithValues: atoms.map { ($0.atomKey.atomOrdinal, $0) }
        )

        let firstOnly = try project(catalog: firstOnlyCatalog, atoms: atoms, asset: asset,
                                    atomsByOrdinal: atomsByOrdinal)
        let allOccurrences = try project(catalog: catalog, atoms: atoms, asset: asset,
                                         atomsByOrdinal: atomsByOrdinal)

        let anchoring = catalog.entries.filter { Self.anchoringCategories.contains($0.category) }
        let repeated = anchoring.filter { $0.count > 1 }

        // Which ordinals does the treatment arm cover with a span? Used to say
        // whether a newly-anchored occurrence became a CLAIM or died at the 5 s
        // floor — the two are very different precision costs.
        var coveredOrdinals = Set<Int>()
        for span in allOccurrences.spans {
            coveredOrdinals.formUnion(span.firstAtomOrdinal ... span.lastAtomOrdinal)
        }

        var newRows: [OccurrenceRow] = []
        for entry in anchoring {
            for occurrence in entry.anchorableOccurrences
            where occurrence.atomOrdinal != entry.atomOrdinal {
                guard let atom = atomsByOrdinal[occurrence.atomOrdinal] else { continue }
                newRows.append(OccurrenceRow(
                    assetId: asset.assetId,
                    title: asset.title,
                    evidenceRef: entry.evidenceRef,
                    category: entry.category.rawValue,
                    matchedText: entry.matchedText,
                    normalizedText: entry.normalizedText,
                    atomOrdinal: occurrence.atomOrdinal,
                    atomStart: atom.startTime,
                    atomEnd: atom.endTime,
                    representativeAtomOrdinal: entry.atomOrdinal,
                    representativeStart: entry.startTime,
                    atomText: atom.text,
                    contextBefore: (max(0, occurrence.atomOrdinal - 3) ..< occurrence.atomOrdinal)
                        .compactMap { atomsByOrdinal[$0]?.text }.joined(separator: " "),
                    contextAfter: ((occurrence.atomOrdinal + 1) ... (occurrence.atomOrdinal + 3))
                        .compactMap { atomsByOrdinal[$0]?.text }.joined(separator: " "),
                    overlappingWindows: asset.windows
                        .filter { $0.startTime < atom.endTime && atom.startTime < $0.endTime }
                        .map { "\($0.decisionState)/\($0.boundaryState)/\($0.detectorVersion) \($0.startTime)-\($0.endTime)" },
                    landedInSpan: coveredOrdinals.contains(occurrence.atomOrdinal)
                ))
            }
        }

        let prompt = catalog.renderForPrompt()

        return AssetReport(
            assetId: asset.assetId,
            title: asset.title,
            durationSec: asset.durationSec,
            rawChunks: chunks.count,
            canonicalChunks: canonical.chunks.count,
            atoms: atoms.count,
            catalogEntries: catalog.entries.count,
            anchoringEntries: anchoring.count,
            repeatedAnchoringEntries: repeated.count,
            firstOnlyAnchoredAtoms: firstOnly.anchoredAtoms,
            allOccurrencesAnchoredAtoms: allOccurrences.anchoredAtoms,
            firstOnlySpans: firstOnly.spans,
            allOccurrencesSpans: allOccurrences.spans,
            repeatedEntries: repeated.map {
                EntryRow(
                    evidenceRef: $0.evidenceRef,
                    category: $0.category.rawValue,
                    matchedText: $0.matchedText,
                    normalizedText: $0.normalizedText,
                    atomOrdinal: $0.atomOrdinal,
                    count: $0.count,
                    firstTime: $0.firstTime,
                    lastTime: $0.lastTime
                )
            },
            newlyAnchoredOccurrences: newRows,
            promptLength: prompt.count,
            promptSHA: Self.digest(prompt),
            evidenceRefs: catalog.entries.map(\.evidenceRef)
        )
    }

    private func project(
        catalog: EvidenceCatalog,
        atoms: [TranscriptAtom],
        asset: Corpus.Asset,
        atomsByOrdinal: [Int: TranscriptAtom]
    ) throws -> (spans: [SpanRow], anchoredAtoms: Int) {
        let projector = AtomEvidenceProjector()
        let evidence = try Self.awaitValue {
            await projector.project(
                regions: [],
                catalog: catalog,
                atoms: atoms,
                correctionMaskProvider: NoCorrectionMaskProvider()
            )
        }
        let decoder = MinimalContiguousSpanDecoder()
        let spans = decoder.decode(atoms: evidence, assetId: asset.assetId)
        let rows = spans.map { span in
            SpanRow(
                assetId: asset.assetId,
                title: asset.title,
                startTime: span.startTime,
                endTime: span.endTime,
                firstAtomOrdinal: span.firstAtomOrdinal,
                lastAtomOrdinal: span.lastAtomOrdinal,
                provenance: span.anchorProvenance.map { String(describing: $0) },
                text: (span.firstAtomOrdinal ... span.lastAtomOrdinal)
                    .compactMap { atomsByOrdinal[$0]?.text }
                    .joined(separator: " "),
                overlappingWindows: asset.windows
                    .filter { $0.startTime < span.endTime && span.startTime < $0.endTime }
                    .map { "\($0.decisionState)/\($0.boundaryState)/\($0.detectorVersion) \($0.startTime)-\($0.endTime)" }
            )
        }
        return (rows, evidence.filter(\.isAnchored).count)
    }

    /// A short stable digest of a prompt rendering, so the report can prove the
    /// FM-side text did not move without carrying megabytes of it.
    static func digest(_ text: String) -> String {
        var hash: UInt64 = 0xcbf29ce484222325
        for byte in text.utf8 {
            hash ^= UInt64(byte)
            hash = hash &* 0x100000001b3
        }
        return String(hash, radix: 16)
    }

    /// Bridge one `async` call into this XCTest body without making the whole
    /// suite async — the corpus loop is otherwise synchronous and an async test
    /// would hop actors 31 times for no benefit.
    static func awaitValue<T: Sendable>(_ body: @escaping @Sendable () async -> T) throws -> T {
        let box = ResultBox<T>()
        let semaphore = DispatchSemaphore(value: 0)
        Task {
            let value = await body()
            box.set(value)
            semaphore.signal()
        }
        semaphore.wait()
        guard let value = box.get() else {
            throw XCTSkip("projection produced no value")
        }
        return value
    }

    private final class ResultBox<T: Sendable>: @unchecked Sendable {
        private let lock = NSLock()
        private var value: T?
        func set(_ newValue: T) { lock.lock(); value = newValue; lock.unlock() }
        func get() -> T? { lock.lock(); defer { lock.unlock() }; return value }
    }
}
