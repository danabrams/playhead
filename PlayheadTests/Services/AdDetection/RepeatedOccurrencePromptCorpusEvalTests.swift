// RepeatedOccurrencePromptCorpusEvalTests.swift
// playhead-ad9n: the corpus lane that measures what first-occurrence evidence
// dedup costs on the FM side — the prompt the model reads, and the windows the
// targeted phases nominate for a scan at all.
//
// WHY A SECOND LANE AND NOT A SECOND ASSERTION IN 04rx's
// -----------------------------------------------------
// `RepeatedOccurrenceAnchoringCorpusEvalTests` measures the PROJECTOR: which
// atoms get anchored and which spans the decoder emits. That lane deliberately
// pins `renderForPrompt` at a constant, because 04rx changed nothing the FM
// sees. This bead changes exactly that, so the quantities are different ones:
//   * how many EVIDENCE LINES a window's prompt gains, and how many CHARACTERS
//     and estimated TOKENS that costs against the real per-call budget;
//   * how many SEGMENTS the targeted narrowing phases nominate, and whether the
//     addition tips a phase over `maxNarrowedSegmentsPerPhase` — which does not
//     shrink the scan, it makes the runner fall back to the WHOLE EPISODE.
//
// BOTH ARMS IN ONE RUN, AND THE ARM LABEL IS DERIVED
// --------------------------------------------------
// The baseline arm is the SAME catalog with every entry's occurrence list
// truncated to its representative — the exact data the FM side saw before this
// bead, since nothing on this side read `occurrences`. So one build measures
// both, and running this lane on an UNMODIFIED tree must print a delta of zero
// everywhere: that zero is the reproduction of the shape, not an absence of
// one. `armLabel()` derives which arm production is in from production code
// rather than from a constant somebody typed.
//
// STAGING: `PLAYHEAD_AD9N_CORPUS` (or `TEST_RUNNER_PLAYHEAD_AD9N_CORPUS`)
// points at the JSON export of a device `analysis.sqlite` produced by
// `scripts/l2f-04rx-export-corpus.py` — the same export format and the same
// device pull 04rx measured, so the two lanes' numbers are commensurable. The
// export is not in the repo, so with the variable unset this SKIPS. Set
// `PLAYHEAD_AD9N_OUT` to write the full per-window report as JSON.

import Foundation
import XCTest
@testable import Playhead

final class RepeatedOccurrencePromptCorpusEvalTests: XCTestCase {

    typealias Corpus = RepeatedOccurrenceAnchoringCorpusEvalTests.Corpus

    // MARK: - Budgets, derived from production rather than typed

    /// The refinement path's per-call prompt budget on a 4,096-token on-device
    /// window, computed by the production formula rather than restated.
    ///
    /// This is NOT the ~468 the coarse path gets: `promptEvidenceEntries` is
    /// only ever called from `planAdaptiveZoom` / `refineExpandedWindow` /
    /// `planBoundaryExtractionWindows`, all of which divide by
    /// `refinementBudgetDivisor` (2) rather than the coarse ÷8. Quoting the
    /// coarse budget here would understate the headroom by ~3×.
    static let refinementPromptBudgetTokens = FoundationModelClassifier
        .maximumEstimatedPromptTokensSafeFor(
            contextSize: 4_096,
            schemaTokens: 256,
            maximumResponseTokens: FoundationModelClassifier.Config.default.refinementMaximumResponseTokens,
            safetyMarginTokens: FoundationModelClassifier.Config.default.safetyMarginTokens
        )

    /// The COARSE budget, quoted only so the report can name both and nobody
    /// re-derives the wrong one. This is the ~468 `playhead-hzpa` measured.
    static let coarsePromptBudgetTokens = FoundationModelClassifier
        .maximumEstimatedPromptTokensSafeFor(
            contextSize: 4_096,
            schemaTokens: 128,
            maximumResponseTokens: FoundationModelClassifier.Config.default.coarseMaximumResponseTokens,
            safetyMarginTokens: FoundationModelClassifier.Config.default.safetyMarginTokens,
            divisor: FoundationModelClassifier.effectiveCoarseBudgetDivisor(contextSize: 4_096)
        )

    // MARK: - Report shape

    /// One coarse window whose evidence block changes between the arms.
    struct WindowRow: Encodable {
        let assetId: String
        let title: String?
        let windowIndex: Int
        let lineRefs: [Int]
        let startTime: Double
        let endTime: Double
        /// Evidence refs the pre-ad9n selector put in this window's prompt.
        let baselineRefs: [Int]
        /// Evidence refs the occurrence-aware selector puts in it.
        let treatmentRefs: [Int]
        /// The refs that are new, with the text the model now gets to see.
        let addedRefs: [AddedRefRow]
        let baselinePromptChars: Int
        let treatmentPromptChars: Int
        let baselinePromptTokens: Int
        let treatmentPromptTokens: Int
        /// Persisted device windows this coarse window overlaps.
        let overlappingWindows: [String]
    }

    struct AddedRefRow: Encodable {
        let evidenceRef: Int
        let category: String
        let matchedText: String
        let normalizedText: String
        /// Where the prompt line now points.
        let lineRef: Int
        let occurrenceAtomOrdinal: Int
        let occurrenceStart: Double
        /// Where the pre-ad9n selector believed this evidence was.
        let representativeAtomOrdinal: Int
        let representativeStart: Double
        /// The rendered prompt line, so the character cost is readable rather
        /// than inferred.
        let renderedLine: String
        /// The transcript line the model reads at `lineRef`, so a reader can
        /// tell a sponsor read from "like I said, betterhelp.com" without
        /// leaving the report.
        let lineText: String
        let overlappingWindows: [String]
    }

    /// The targeted-narrowing half: what the phase nominated, under each arm.
    struct NarrowingRow: Encodable {
        let assetId: String
        let title: String?
        let phase: String
        let baselineSegments: Int
        let treatmentSegments: Int
        let baselineAborted: Bool
        let treatmentAborted: Bool
        let baselineEmpty: Bool
        let treatmentEmpty: Bool
        let addedSegments: [AddedSegmentRow]
    }

    struct AddedSegmentRow: Encodable {
        let segmentIndex: Int
        let startTime: Double
        let endTime: Double
        let text: String
        let overlappingWindows: [String]
    }

    /// The UNION across every targeted phase — what the runner actually scans.
    ///
    /// Reported separately because a per-phase delta overstates the marginal
    /// value if some OTHER phase already reached the same segments.
    /// `.scanLikelyAdSlots` seeds itself from `LexicalScanner` rather than from
    /// the catalog, and it is entirely plausible that it already nominates the
    /// second ad break — in which case this bead's narrowing half buys nothing
    /// the runner did not already have. The union is the number that settles
    /// that, and it is the same quantity `recallSample` is computed from.
    struct UnionRow: Encodable {
        let assetId: String
        let title: String?
        let baselineLineRefs: Int
        let treatmentLineRefs: Int
        let addedSegments: [AddedSegmentRow]
    }

    struct AssetReport: Encodable {
        let assetId: String
        let title: String?
        let atoms: Int
        let segments: Int
        let catalogEntries: Int
        let repeatedEntries: Int
        let coarseWindows: Int
        let windowsWithAddedEvidence: Int
        let addedEvidenceLines: Int
        let baselineTotalPromptChars: Int
        let treatmentTotalPromptChars: Int
        let baselineTotalPromptTokens: Int
        let treatmentTotalPromptTokens: Int
        let windows: [WindowRow]
        let narrowing: [NarrowingRow]
        let union: UnionRow
    }

    struct Report: Encodable {
        let source: String
        let arm: String
        let refinementBudgetTokens: Int
        let coarseBudgetTokens: Int
        let assets: Int
        let atoms: Int
        let coarseWindows: Int
        let windowsWithAddedEvidence: Int
        let addedEvidenceLines: Int
        let baselineTotalPromptChars: Int
        let treatmentTotalPromptChars: Int
        let baselineTotalPromptTokens: Int
        let treatmentTotalPromptTokens: Int
        let perAsset: [AssetReport]
    }

    // MARK: - Arm label, derived from production

    /// Which arm this build measures, asked of production code.
    ///
    /// Probe: one sponsor URL read at atom 0 and again at atom 40, far enough
    /// apart that they land in different segments. A window over the SECOND
    /// read either carries an evidence line for it or does not, and that is the
    /// whole question this bead is about.
    static func armLabel() -> String {
        let atoms = (0 ..< 60).map { ordinal in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "ad9n-arm-probe",
                    transcriptVersion: "ad9n-arm-probe",
                    atomOrdinal: ordinal
                ),
                contentHash: "ad9n-arm-probe-\(ordinal)",
                startTime: Double(ordinal) * 10,
                endTime: Double(ordinal) * 10 + 9,
                text: (ordinal == 0 || ordinal == 40)
                    ? "go to armprobe.com slash offer"
                    : "and now for something completely unrelated",
                chunkIndex: ordinal
            )
        }
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: "ad9n-arm-probe",
            transcriptVersion: "ad9n-arm-probe"
        )
        // One segment per atom keeps the probe's geometry trivial.
        let segments = atoms.map { AdTranscriptSegment(atoms: [$0], segmentIndex: $0.atomKey.atomOrdinal) }
        let lineRefByAtomOrdinal = Dictionary(
            uniqueKeysWithValues: segments.map { ($0.firstAtomOrdinal, $0.segmentIndex) }
        )
        // A window over the SECOND read only.
        let selected = evidenceRefs(
            catalog: catalog,
            lineRefs: [39, 40, 41],
            lineRefByAtomOrdinal: lineRefByAtomOrdinal
        )
        let urlRefs = catalog.entries
            .filter { $0.category == .url }
            .map(\.evidenceRef)
        guard !urlRefs.isEmpty else { return "unrecognised(probe built no url entry)" }
        let sawURL = selected.contains { urlRefs.contains($0.entry.evidenceRef) }
        return sawURL ? "allOccurrences" : "firstOccurrence"
    }

    /// The PRE-ad9n selector, transcribed literally, frozen.
    ///
    /// The baseline arm does not go through production code, on purpose. If it
    /// did, the measurement would be "production compared with production
    /// handed different data", and a bug in the new selector could cancel out
    /// of both sides. This is the deleted `guard let lineRef =
    /// lineRefByAtomOrdinal[entry.atomOrdinal]` body, and nothing else.
    ///
    /// It is cross-checked against production at corpus scale rather than
    /// against a fixture: `evaluate` asserts, for EVERY window of EVERY asset,
    /// that this function agrees exactly with `forWindow` run over a catalog
    /// whose occurrence lists are truncated to their representatives. A
    /// transcription error therefore fails the lane rather than shifting its
    /// numbers.
    static func preAd9nEvidenceRefs(
        catalog: EvidenceCatalog,
        lineRefs: [Int],
        lineRefByAtomOrdinal: [Int: Int]
    ) -> [PromptEvidenceEntry] {
        let allowedLineRefs = Set(lineRefs)
        return catalog.entries
            .compactMap { entry -> PromptEvidenceEntry? in
                guard let lineRef = lineRefByAtomOrdinal[entry.atomOrdinal],
                      allowedLineRefs.contains(lineRef) else {
                    return nil
                }
                return PromptEvidenceEntry(entry: entry, lineRef: lineRef)
            }
            .sorted {
                if $0.lineRef == $1.lineRef {
                    return $0.entry.evidenceRef < $1.entry.evidenceRef
                }
                return $0.lineRef < $1.lineRef
            }
    }

    /// A comparable identity for one rendered evidence line: the ref, where it
    /// points, and the text the model reads. Two selectors agree when their
    /// keys agree.
    static func keys(_ entries: [PromptEvidenceEntry]) -> [String] {
        entries.map { "\($0.entry.evidenceRef)@\($0.lineRef):\($0.renderForPrompt())" }
    }

    /// The production selector, applied to a window. Extracted here so the lane
    /// and the arm label go through exactly one path.
    static func evidenceRefs(
        catalog: EvidenceCatalog,
        lineRefs: [Int],
        lineRefByAtomOrdinal: [Int: Int]
    ) -> [PromptEvidenceEntry] {
        let allowed = Set(lineRefs)
        return catalog.entries
            .compactMap {
                PromptEvidenceEntry.forWindow(
                    entry: $0,
                    allowedLineRefs: allowed,
                    lineRefByAtomOrdinal: lineRefByAtomOrdinal
                )
            }
            .sorted {
                if $0.lineRef == $1.lineRef {
                    return $0.entry.evidenceRef < $1.entry.evidenceRef
                }
                return $0.lineRef < $1.lineRef
            }
    }

    // MARK: - The lane

    func testRepeatedOccurrencePromptVisibilityOnDeviceCorpus() async throws {
        let environment = ProcessInfo.processInfo.environment
        let key = environment["PLAYHEAD_AD9N_CORPUS"]
            ?? environment["TEST_RUNNER_PLAYHEAD_AD9N_CORPUS"]
        guard let path = key, FileManager.default.fileExists(atPath: path) else {
            throw XCTSkip(
                "corpus lane needs PLAYHEAD_AD9N_CORPUS pointing at a device analysis.sqlite JSON export"
            )
        }
        let corpus = try JSONDecoder().decode(
            Corpus.self,
            from: try Data(contentsOf: URL(fileURLWithPath: path))
        )
        try XCTSkipIf(corpus.assets.isEmpty, "corpus export has no assets")

        var perAsset: [AssetReport] = []
        for asset in corpus.assets where !asset.chunks.isEmpty {
            perAsset.append(try await evaluate(asset: asset))
        }

        let arm = Self.armLabel()
        let report = Report(
            source: corpus.source,
            arm: arm,
            refinementBudgetTokens: Self.refinementPromptBudgetTokens,
            coarseBudgetTokens: Self.coarsePromptBudgetTokens,
            assets: perAsset.count,
            atoms: perAsset.reduce(0) { $0 + $1.atoms },
            coarseWindows: perAsset.reduce(0) { $0 + $1.coarseWindows },
            windowsWithAddedEvidence: perAsset.reduce(0) { $0 + $1.windowsWithAddedEvidence },
            addedEvidenceLines: perAsset.reduce(0) { $0 + $1.addedEvidenceLines },
            baselineTotalPromptChars: perAsset.reduce(0) { $0 + $1.baselineTotalPromptChars },
            treatmentTotalPromptChars: perAsset.reduce(0) { $0 + $1.treatmentTotalPromptChars },
            baselineTotalPromptTokens: perAsset.reduce(0) { $0 + $1.baselineTotalPromptTokens },
            treatmentTotalPromptTokens: perAsset.reduce(0) { $0 + $1.treatmentTotalPromptTokens },
            perAsset: perAsset
        )

        print("[ad9n] arm=\(arm) source=\(corpus.source)")
        print("[ad9n] budgets: refinement=\(Self.refinementPromptBudgetTokens) coarse=\(Self.coarsePromptBudgetTokens) tokens (contextSize 4096)")
        print("[ad9n] assets=\(report.assets) atoms=\(report.atoms) coarseWindows=\(report.coarseWindows)")
        print("[ad9n] windows gaining evidence=\(report.windowsWithAddedEvidence) added evidence lines=\(report.addedEvidenceLines)")
        print("[ad9n] prompt chars: baseline=\(report.baselineTotalPromptChars) treatment=\(report.treatmentTotalPromptChars) delta=\(report.treatmentTotalPromptChars - report.baselineTotalPromptChars)")
        print("[ad9n] prompt tokens (fallbackTokenEstimate): baseline=\(report.baselineTotalPromptTokens) treatment=\(report.treatmentTotalPromptTokens) delta=\(report.treatmentTotalPromptTokens - report.baselineTotalPromptTokens)")
        for asset in perAsset {
            for window in asset.windows where !window.addedRefs.isEmpty {
                print("[ad9n] WIN \(asset.assetId.prefix(8)) w\(window.windowIndex) L\(window.lineRefs.first ?? -1)-\(window.lineRefs.last ?? -1) t=\(fmt(window.startTime))-\(fmt(window.endTime)) refs \(window.baselineRefs.count)->\(window.treatmentRefs.count) chars \(window.baselinePromptChars)->\(window.treatmentPromptChars) tokens \(window.baselinePromptTokens)->\(window.treatmentPromptTokens) windows=\(window.overlappingWindows.count)")
                for added in window.addedRefs {
                    print("[ad9n]     +E\(added.evidenceRef) [\(added.category)] \"\(added.normalizedText)\" line=\(added.lineRef) atom=\(added.occurrenceAtomOrdinal)@\(fmt(added.occurrenceStart)) rep=\(added.representativeAtomOrdinal)@\(fmt(added.representativeStart)) windows=\(added.overlappingWindows.count)")
                    print("[ad9n]        line: \(added.lineText.prefix(220))")
                }
            }
        }
        let unionBase = perAsset.reduce(0) { $0 + $1.union.baselineLineRefs }
        let unionTreat = perAsset.reduce(0) { $0 + $1.union.treatmentLineRefs }
        print("[ad9n] targeted UNION across all phases: \(unionBase)->\(unionTreat) line refs (+\(unionTreat - unionBase))")
        for asset in perAsset where !asset.union.addedSegments.isEmpty {
            print("[ad9n] UNION \(asset.assetId.prefix(8)) refs \(asset.union.baselineLineRefs)->\(asset.union.treatmentLineRefs) added=\(asset.union.addedSegments.count)")
            for segment in asset.union.addedSegments {
                print("[ad9n]     +L\(segment.segmentIndex) t=\(fmt(segment.startTime))-\(fmt(segment.endTime)) windows=\(segment.overlappingWindows.count) \(segment.text.prefix(160))")
            }
        }
        for asset in perAsset {
            for row in asset.narrowing where row.baselineSegments != row.treatmentSegments
                || row.baselineAborted != row.treatmentAborted || row.baselineEmpty != row.treatmentEmpty {
                print("[ad9n] NARROW \(asset.assetId.prefix(8)) \(row.phase) segs \(row.baselineSegments)->\(row.treatmentSegments) aborted \(row.baselineAborted)->\(row.treatmentAborted) empty \(row.baselineEmpty)->\(row.treatmentEmpty) added=\(row.addedSegments.count)")
                for segment in row.addedSegments {
                    print("[ad9n]     +L\(segment.segmentIndex) t=\(fmt(segment.startTime))-\(fmt(segment.endTime)) windows=\(segment.overlappingWindows.count) \(segment.text.prefix(200))")
                }
            }
        }

        if let outPath = environment["PLAYHEAD_AD9N_OUT"]
            ?? environment["TEST_RUNNER_PLAYHEAD_AD9N_OUT"] {
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            try encoder.encode(report).write(to: URL(fileURLWithPath: outPath))
            print("[ad9n] wrote \(outPath)")
        }

        // The lane is a measurement, not a threshold. Assert only that it
        // measured something — an empty corpus would otherwise print a
        // reassuring "0 -> 0" and read as a result.
        XCTAssertGreaterThan(report.atoms, 0, "corpus produced no atoms")
        XCTAssertGreaterThan(report.coarseWindows, 0, "corpus produced no coarse windows")
        XCTAssertGreaterThan(
            perAsset.reduce(0) { $0 + $1.repeatedEntries },
            0,
            "corpus contains no repeated catalog entry — nothing to measure"
        )
    }

    // MARK: - Per-asset evaluation

    private func evaluate(asset: Corpus.Asset) async throws -> AssetReport {
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
            normalizationHash: "ad9n-lane",
            sourceHash: "ad9n-lane"
        )
        let atoms = atomized.atoms
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: asset.assetId,
            transcriptVersion: atomized.version.transcriptVersion
        )
        let baselineCatalog = Self.truncatedToRepresentative(catalog)
        let segments = TranscriptSegmenter.segment(atoms: atoms)

        var lineRefByAtomOrdinal: [Int: Int] = [:]
        for segment in segments {
            for atom in segment.atoms {
                lineRefByAtomOrdinal[atom.atomKey.atomOrdinal] = segment.segmentIndex
            }
        }
        let segmentByIndex = Dictionary(
            segments.map { ($0.segmentIndex, $0) },
            uniquingKeysWith: { first, _ in first }
        )

        // Coarse windows, planned by PRODUCTION `planPassA` against the real
        // 4,096-token budget with the device's own fallback tokenizer. A
        // refinement window is always a SUBSET of the coarse window it came
        // from (`planAdaptiveZoom` selects clusters inside `window.lineRefs`),
        // so an entry whose representative is outside the coarse window is
        // outside every refinement window derived from it. This frame is the
        // conservative one for the DEFECT and the generous one for the COST.
        let recorder = TestFMRuntime(
            contextSize: 4_096,
            coarseSchemaTokenCount: 128,
            refinementSchemaTokenCount: 256,
            boundarySchemaTokenCount: 256,
            tokenCountRule: { FoundationModelClassifier.fallbackTokenEstimate(for: $0) }
        )
        let classifier = FoundationModelClassifier(runtime: recorder.runtime, config: .default)
        let coarsePlans = try await classifier.planPassA(segments: segments)

        var windowRows: [WindowRow] = []
        var baselineChars = 0
        var treatmentChars = 0
        var baselineTokens = 0
        var treatmentTokens = 0
        var windowsWithAdded = 0
        var addedLines = 0

        for plan in coarsePlans.sorted(by: { $0.windowIndex < $1.windowIndex }) {
            let lineRefs = plan.lineRefs.sorted()
            let windowSegments = lineRefs.compactMap { segmentByIndex[$0] }
            let baselineEvidence = Self.preAd9nEvidenceRefs(
                catalog: catalog,
                lineRefs: lineRefs,
                lineRefByAtomOrdinal: lineRefByAtomOrdinal
            )
            // The cross-check that stops the frozen baseline from being an
            // unverified transcription. Production, handed a catalog whose
            // occurrence lists are truncated to their representatives, must
            // reproduce the deleted selector EXACTLY — same refs, same lines,
            // same rendered text — on every window of every asset.
            XCTAssertEqual(
                Self.keys(baselineEvidence),
                Self.keys(Self.evidenceRefs(
                    catalog: baselineCatalog,
                    lineRefs: lineRefs,
                    lineRefByAtomOrdinal: lineRefByAtomOrdinal
                )),
                "pre-ad9n selector and forWindow-on-truncated-catalog disagree at \(asset.assetId) window \(plan.windowIndex)"
            )
            let treatmentEvidence = Self.evidenceRefs(
                catalog: catalog,
                lineRefs: lineRefs,
                lineRefByAtomOrdinal: lineRefByAtomOrdinal
            )
            let basePrompt = FoundationModelClassifier.buildRefinementPrompt(
                for: windowSegments,
                promptEvidence: baselineEvidence,
                maximumSpans: FoundationModelClassifier.Config.default.maximumRefinementSpansPerWindow
            )
            let treatPrompt = FoundationModelClassifier.buildRefinementPrompt(
                for: windowSegments,
                promptEvidence: treatmentEvidence,
                maximumSpans: FoundationModelClassifier.Config.default.maximumRefinementSpansPerWindow
            )
            let baseTok = FoundationModelClassifier.fallbackTokenEstimate(for: basePrompt)
            let treatTok = FoundationModelClassifier.fallbackTokenEstimate(for: treatPrompt)
            baselineChars += basePrompt.count
            treatmentChars += treatPrompt.count
            baselineTokens += baseTok
            treatmentTokens += treatTok

            let baselineRefs = Set(baselineEvidence.map(\.entry.evidenceRef))
            let added = treatmentEvidence.filter { !baselineRefs.contains($0.entry.evidenceRef) }
            if !added.isEmpty {
                windowsWithAdded += 1
                addedLines += added.count
            }
            let representativeByRef = Dictionary(
                catalog.entries.map { ($0.evidenceRef, $0) },
                uniquingKeysWith: { first, _ in first }
            )

            let startTime = windowSegments.map(\.startTime).min() ?? 0
            let endTime = windowSegments.map(\.endTime).max() ?? 0
            windowRows.append(WindowRow(
                assetId: asset.assetId,
                title: asset.title,
                windowIndex: plan.windowIndex,
                lineRefs: lineRefs,
                startTime: startTime,
                endTime: endTime,
                baselineRefs: baselineEvidence.map(\.entry.evidenceRef),
                treatmentRefs: treatmentEvidence.map(\.entry.evidenceRef),
                addedRefs: added.map { promptEntry in
                    let representative = representativeByRef[promptEntry.entry.evidenceRef]
                    return AddedRefRow(
                        evidenceRef: promptEntry.entry.evidenceRef,
                        category: promptEntry.entry.category.rawValue,
                        matchedText: promptEntry.entry.matchedText,
                        normalizedText: promptEntry.entry.normalizedText,
                        lineRef: promptEntry.lineRef,
                        occurrenceAtomOrdinal: promptEntry.entry.atomOrdinal,
                        occurrenceStart: promptEntry.entry.startTime,
                        representativeAtomOrdinal: representative?.atomOrdinal ?? -1,
                        representativeStart: representative?.startTime ?? -1,
                        renderedLine: promptEntry.renderForPrompt(),
                        lineText: segmentByIndex[promptEntry.lineRef]?.text ?? "",
                        overlappingWindows: Self.windowLabels(
                            asset: asset,
                            start: promptEntry.entry.startTime,
                            end: promptEntry.entry.endTime
                        )
                    )
                },
                baselinePromptChars: basePrompt.count,
                treatmentPromptChars: treatPrompt.count,
                baselinePromptTokens: baseTok,
                treatmentPromptTokens: treatTok,
                overlappingWindows: Self.windowLabels(asset: asset, start: startTime, end: endTime)
            ))
        }

        // The narrowing half.
        var narrowingRows: [NarrowingRow] = []
        for phase in [BackfillJobPhase.scanHarvesterProposals, .metadataSeededRegion] {
            let baselineResult = TargetedWindowNarrower.narrow(
                phase: phase,
                inputs: Self.inputs(asset: asset, segments: segments, catalog: baselineCatalog)
            )
            let treatmentResult = TargetedWindowNarrower.narrow(
                phase: phase,
                inputs: Self.inputs(asset: asset, segments: segments, catalog: catalog)
            )
            let baselineIndices = Set((baselineResult.narrowedSegments ?? []).map(\.segmentIndex))
            let treatmentIndices = Set((treatmentResult.narrowedSegments ?? []).map(\.segmentIndex))
            let addedIndices = treatmentIndices.subtracting(baselineIndices).sorted()
            narrowingRows.append(NarrowingRow(
                assetId: asset.assetId,
                title: asset.title,
                phase: phase.rawValue,
                baselineSegments: baselineIndices.count,
                treatmentSegments: treatmentIndices.count,
                baselineAborted: baselineResult.aborted,
                treatmentAborted: treatmentResult.aborted,
                baselineEmpty: baselineResult.wasEmpty,
                treatmentEmpty: treatmentResult.wasEmpty,
                addedSegments: addedIndices.compactMap { index in
                    guard let segment = segmentByIndex[index] else { return nil }
                    return AddedSegmentRow(
                        segmentIndex: index,
                        startTime: segment.startTime,
                        endTime: segment.endTime,
                        text: segment.text,
                        overlappingWindows: Self.windowLabels(
                            asset: asset,
                            start: segment.startTime,
                            end: segment.endTime
                        )
                    )
                }
            ))
        }

        let baselineUnion = TargetedWindowNarrower.predictedTargetedLineRefs(
            inputs: Self.inputs(asset: asset, segments: segments, catalog: baselineCatalog)
        )
        let treatmentUnion = TargetedWindowNarrower.predictedTargetedLineRefs(
            inputs: Self.inputs(asset: asset, segments: segments, catalog: catalog)
        )
        let unionRow = UnionRow(
            assetId: asset.assetId,
            title: asset.title,
            baselineLineRefs: baselineUnion.count,
            treatmentLineRefs: treatmentUnion.count,
            addedSegments: treatmentUnion.subtracting(baselineUnion).sorted().compactMap { index in
                guard let segment = segmentByIndex[index] else { return nil }
                return AddedSegmentRow(
                    segmentIndex: index,
                    startTime: segment.startTime,
                    endTime: segment.endTime,
                    text: segment.text,
                    overlappingWindows: Self.windowLabels(
                        asset: asset,
                        start: segment.startTime,
                        end: segment.endTime
                    )
                )
            }
        )

        return AssetReport(
            assetId: asset.assetId,
            title: asset.title,
            atoms: atoms.count,
            segments: segments.count,
            catalogEntries: catalog.entries.count,
            repeatedEntries: catalog.entries.filter { $0.count > 1 }.count,
            coarseWindows: coarsePlans.count,
            windowsWithAddedEvidence: windowsWithAdded,
            addedEvidenceLines: addedLines,
            baselineTotalPromptChars: baselineChars,
            treatmentTotalPromptChars: treatmentChars,
            baselineTotalPromptTokens: baselineTokens,
            treatmentTotalPromptTokens: treatmentTokens,
            windows: windowRows,
            narrowing: narrowingRows,
            union: unionRow
        )
    }

    // MARK: - Helpers

    /// The pre-ad9n catalog: every entry's occurrence list truncated to its
    /// representative. That is EXACTLY what the FM side saw before this bead,
    /// because nothing on the FM side read `occurrences` at all.
    static func truncatedToRepresentative(_ catalog: EvidenceCatalog) -> EvidenceCatalog {
        EvidenceCatalog(
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
    }

    static func inputs(
        asset: Corpus.Asset,
        segments: [AdTranscriptSegment],
        catalog: EvidenceCatalog
    ) -> TargetedWindowNarrower.Inputs {
        TargetedWindowNarrower.Inputs(
            analysisAssetId: asset.assetId,
            podcastId: "ad9n-lane",
            transcriptVersion: catalog.transcriptVersion,
            segments: segments,
            evidenceCatalog: catalog,
            auditWindowSampleRate: 0.1
        )
    }

    static func windowLabels(asset: Corpus.Asset, start: Double, end: Double) -> [String] {
        asset.windows
            .filter { $0.startTime < end && start < $0.endTime }
            .map { "\($0.decisionState)/\($0.boundaryState)/\($0.detectorVersion) \($0.startTime)-\($0.endTime)" }
    }

    private func fmt(_ value: Double) -> String {
        String(format: "%.2f", value)
    }
}
