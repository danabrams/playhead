// CoarseCertaintyProvenanceGateTests.swift
// playhead-iw7q: the COARSE-side certainty gate, and the asymmetry that makes
// it a different rule from the refined-side one playhead-92im already shipped.
//
// THE TWO SHAPES ARE NOT SYMMETRICAL, and that is the whole design.
//
//   * A `passB` payload is an ARRAY of refined spans and each span carries
//     `ownershipInferenceWasSuppressed` — the discriminator travels WITH the
//     value it qualifies, so it reads correctly on a row of any vintage.
//   * A `passA` payload is ONE `CoarseSupportSchema` object with exactly two
//     fields, neither of which is a provenance. There is nowhere in the payload
//     to put one, which is why closing this half needed a COLUMN
//     (`semantic_scan_results.usedPermissiveFallback`, schema V61) and not a
//     read.
//
// So the coarse gate asks the ROW and the refined gate asks the SPAN, and the
// tests below pin BOTH directions of that: a `.unknown` row must NOT veto a
// refined payload (its spans speak for themselves, and vetoing would silently
// re-grade every refinement written before V61), while a `.unknown` row MUST
// veto a coarse one (nothing else can speak for it).
//
// UNKNOWN IS NOT ZERO. `.permissive` and `.unknown` return the same band —
// `nil` — for two different reasons: one is "the runner graded this" and the
// other is "nobody recorded who graded this". They agree on what may be SPENT
// and disagree on what is KNOWN, which is why both states exist on the row and
// are collapsed only here.

import Foundation
import Testing

@testable import Playhead

@Suite("a coarse band is the MODEL'S only when the row says so (playhead-iw7q)")
struct CoarseCertaintyProvenanceGateTests {

    // MARK: - Fixtures

    /// The `passA` shape `BackfillJobRunner.encodeSupport` writes. Verbatim from
    /// the 2026-08-19 t4 pull's `CD2976E6` [1131.6–1210.9] row.
    private static func coarsePayload(_ certainty: CertaintyBand) -> String {
        #"{"supportLineRefs":[46],"certainty":"\#(certainty.rawValue)"}"#
    }

    /// The `passB` shape `encodeRefinedSpans` writes.
    private static func refinedPayload(
        _ certainty: CertaintyBand,
        ownershipInferenceWasSuppressed: Bool
    ) -> String {
        let flag = ownershipInferenceWasSuppressed ? "true" : "false"
        return "[" + #"{"anchors":[],"certainty":"\#(certainty.rawValue)","#
            + #""commercialIntent":"paid","firstLineRef":2,"lastLineRef":2,"#
            + #""ownership":"thirdParty","ownershipInferenceWasSuppressed":\#(flag)}"# + "]"
    }

    private static func row(
        spansJSON: String,
        provenance: ScanVerdictProvenance,
        scanPass: String = "passA",
        transcriptQuality: TranscriptQuality = .good
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: "scan-gate",
            analysisAssetId: "asset-gate",
            windowFirstAtomOrdinal: 0,
            windowLastAtomOrdinal: 1,
            windowStartTime: 100,
            windowEndTime: 160,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: .containsAd,
            spansJSON: spansJSON,
            status: .success,
            attemptCount: 1,
            errorContext: nil,
            inputTokenCount: nil,
            outputTokenCount: nil,
            latencyMs: nil,
            scanCohortJSON: makeCohortJSON(promptLabel: "iw7q"),
            transcriptVersion: "tv-1",
            verdictProvenance: provenance
        )
    }

    // MARK: - 1. The coarse gate

    @Test("a MODEL coarse row keeps its band, at every rung of the ladder")
    func modelRowKeepsItsBand() {
        for band in [CertaintyBand.strong, .moderate, .weak] {
            let row = Self.row(spansJSON: Self.coarsePayload(band), provenance: .model)
            #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == band)
            #expect(SemanticSweepMarkComposer.certaintyFactor(of: row)
                    == SemanticSweepMarkComposer.certaintyFactor(band))
        }
    }

    @Test("a PERMISSIVE coarse row is ungraded — the runner wrote that .strong")
    func permissiveRowIsUngraded() {
        let row = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .permissive)
        #expect(row.spansJSON.contains("strong"), "the payload still SAYS strong")
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: row) == 0.5)
    }

    @Test("an UNKNOWN coarse row is ungraded too — silence is not a licence")
    func unknownRowIsUngraded() {
        let row = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .unknown)
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: row) == 0.5)
    }

    /// The direction that would have been shipped by a `DEFAULT 0` backfill:
    /// `.unknown` reading as `.model`. Pinned as an INEQUALITY against the
    /// `.model` row so a mutant that collapses the two is killed by a rail
    /// whose failure message names the confusion.
    @Test("unknown does NOT read as model, on rows that are otherwise identical")
    func unknownIsNotModel() {
        let known = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .model)
        let unknown = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .unknown)
        #expect(known.spansJSON == unknown.spansJSON, "byte-identical payloads")
        #expect(SemanticSweepMarkComposer.certaintyBand(of: known)
                != SemanticSweepMarkComposer.certaintyBand(of: unknown))
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: known) == 1.0)
        #expect(SemanticSweepMarkComposer.certaintyFactor(of: unknown) == 0.5)
    }

    @Test("a coarse row with NO support payload is ungraded whatever its provenance")
    func emptyPayloadIsUngradedForEveryProvenance() {
        for provenance in ScanVerdictProvenance.allCases {
            let row = Self.row(spansJSON: "[]", provenance: provenance)
            #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
        }
    }

    // MARK: - 2. The refined side keeps ITS rule, and the row does not override it

    @Test("a refined payload on an UNKNOWN row keeps its band — the SPAN carries the flag")
    func unknownRowDoesNotVetoARefinedPayload() {
        // This is the asymmetry. Every `passB` row on disk today reads
        // `.unknown`, and vetoing here would silently re-grade all of them
        // while discarding a discriminator the payload actually holds.
        let row = Self.row(
            spansJSON: Self.refinedPayload(.strong, ownershipInferenceWasSuppressed: false),
            provenance: .unknown,
            scanPass: "passB"
        )
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == .strong)
    }

    @Test("playhead-92im's span gate is unchanged: a suppressed span is ungraded on a MODEL row")
    func suppressedSpanStaysUngraded() {
        let row = Self.row(
            spansJSON: Self.refinedPayload(.strong, ownershipInferenceWasSuppressed: true),
            provenance: .model,
            scanPass: "passB"
        )
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
    }

    @Test("a refined payload on a PERMISSIVE row is vetoed — that is a CLAIM, not an absence")
    func permissiveRowVetoesARefinedPayload() {
        let row = Self.row(
            spansJSON: Self.refinedPayload(.strong, ownershipInferenceWasSuppressed: false),
            provenance: .permissive,
            scanPass: "passB"
        )
        #expect(SemanticSweepMarkComposer.certaintyBand(of: row) == nil)
    }

    // MARK: - 3. The consequence a mark actually carries

    @Test("the mark a pre-V61 coarse row backs grades at the FLOOR, not the ceiling")
    func markConfidenceFallsToTheFloor() {
        let unknown = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .unknown)
        let model = Self.row(spansJSON: Self.coarsePayload(.strong), provenance: .model)

        let unknownConfidence = SemanticSweepMarkComposer.markConfidence(
            certaintyFactor: SemanticSweepMarkComposer.certaintyFactor(of: unknown),
            transcriptQuality: .good,
            affirming: 1,
            dissenting: 0
        )
        let modelConfidence = SemanticSweepMarkComposer.markConfidence(
            certaintyFactor: SemanticSweepMarkComposer.certaintyFactor(of: model),
            transcriptQuality: .good,
            affirming: 1,
            dissenting: 0
        )
        // 0.700 -> 0.350 is the median move measured over the 2026-08-21 t6
        // pull: 113 of 125 recomposed extents, every one of them DOWNWARD.
        #expect(abs(modelConfidence - SemanticSweepMarkComposer.maximumMarkConfidence) < 1e-12)
        #expect(abs(unknownConfidence - SemanticSweepMarkComposer.unevidencedMarkConfidence) < 1e-12)
        #expect(unknownConfidence < modelConfidence, "the direction is DOWN, always")
    }

    /// The whole change is MONOTONE NON-INCREASING, which is what makes it safe
    /// to ship against a population nobody can attribute: no row can come out of
    /// this gate stronger than it went in.
    @Test("the gate can only ever DEDUCT — no provenance raises a band")
    func gateIsMonotoneNonIncreasing() {
        for band in [CertaintyBand.strong, .moderate, .weak] {
            let ungatedFactor = SemanticSweepMarkComposer.certaintyFactor(band)
            for provenance in ScanVerdictProvenance.allCases {
                let row = Self.row(spansJSON: Self.coarsePayload(band), provenance: provenance)
                #expect(SemanticSweepMarkComposer.certaintyFactor(of: row) <= ungatedFactor)
            }
        }
    }
}

// MARK: - The wire, which no unit test can reach

/// A column that is written `.model` on the very path that fabricates the band
/// is worse than no column: it does not merely fail to record the bypass, it
/// CERTIFIES the fabrication. Every test above hands the composer a row it
/// built itself, so all of them stay green while `FoundationModelClassifier`
/// labels the permissive arms `.model` and every permissive coarse row on the
/// device claims the model graded it.
///
/// This is a SOURCE canary for the reason `SemanticSweepSupportLineWiring-
/// SourceCanaryTests` is: driving the coarse permissive route for real needs a
/// `SensitiveWindowRouter`, a live `PermissiveAdClassifier` and an iOS 26
/// FoundationModels session, and FoundationModels is gracefully unavailable on
/// the simulator — `KellyRipaFMSafetyTests` says so in as many words where it
/// declines to drive the same path.
///
/// # The predicate is STRUCTURAL, not a character window
///
/// The two producers of a `CoarseScreeningSchema` in that file are
/// `sanitize(schema:)` — the `@Generable` path's sanitizer, which preserves
/// the model's own `certainty` — and the permissive bypass
/// (`permissive.classify(` / `classifyPermissively(`), whose `certainty` is
/// `PermissiveAdGrammar.parse`'s hardcoded `.strong`. So the rule is: whichever
/// producer is NEARER above a `verdictProvenance:` literal is the one that
/// produced the screening it labels.
///
/// Measured on the file this shipped against, the separation is not marginal:
/// the three `.permissive` sites have NO `sanitize(` and no `respondCoarse(`
/// above them anywhere in the file, and the four `.model` sites sit 767–1,056
/// characters below a `sanitize(` while the nearest permissive call is
/// 2,330–115,557 characters away. There is no constant in the check, so
/// nothing to tune — the comparison is between two measured distances.
@Suite("the permissive coarse arms label their rows .permissive (playhead-iw7q)",
       .timeLimit(.minutes(1)))
struct PermissiveCoarseProvenanceSourceCanaryTests {

    /// `PlayheadTests/Services/AdDetection/<this file>` → repo root.
    private static func source(_ relative: String, filePath: String = #filePath) throws -> String {
        let root = URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()   // AdDetection
            .deletingLastPathComponent()   // Services
            .deletingLastPathComponent()   // PlayheadTests
            .deletingLastPathComponent()   // repo root
        return try String(contentsOf: root.appendingPathComponent(relative), encoding: .utf8)
    }

    private static let classifierPath =
        "Playhead/Services/AdDetection/FoundationModelClassifier.swift"

    /// Offsets of every occurrence of `needle`, cheap and exact.
    private static func offsets(of needles: [String], in text: String) -> [Int] {
        var found: [Int] = []
        for needle in needles {
            var cursor = text.startIndex
            while let range = text.range(of: needle, range: cursor..<text.endIndex) {
                found.append(text.distance(from: text.startIndex, to: range.lowerBound))
                cursor = range.upperBound
            }
        }
        return found.sorted()
    }

    @Test("every FMCoarseWindowOutput in the classifier states a provenance")
    func everyConstructionStatesAProvenance() throws {
        let text = try Self.source(Self.classifierPath)
        let constructions = text.components(separatedBy: "FMCoarseWindowOutput(").dropFirst()
        #expect(!constructions.isEmpty, "the classifier no longer builds coarse windows at all")
        for construction in constructions {
            // Up to the closing paren of the argument list: every argument in
            // this type's initialiser is a simple expression except the nested
            // `CoarseScreeningSchema(...)`, so stop at the LAST `)` before the
            // next construction instead of balancing.
            let head = construction.prefix(2_000)
            #expect(head.contains("verdictProvenance:"),
                    """
                    an FMCoarseWindowOutput omits verdictProvenance — the parameter is \
                    required, so this can only mean the type changed: \(head.prefix(400))
                    """)
        }
    }

    @Test("a .permissive label sits under a permissive call, and a .model label under sanitize")
    func everyLabelMatchesItsNearestProducer() throws {
        let text = try Self.source(Self.classifierPath)
        let permissiveProducers = Self.offsets(
            of: ["permissive.classify(", "classifyPermissively("], in: text
        )
        // TWO SPELLINGS, and the first draft had only one of them. `sanitize(`
        // is written `sanitize(\n    schema: response,` at three of its four
        // sites, so the literal `"sanitize(schema:"` matched ONE occurrence out
        // of eight and the check failed on every `.model` label — loudly, which
        // is the rail discriminating rather than passing vacuously. Both needles
        // separate the population cleanly on their own (measured below); they
        // are both here so a reformat of either call cannot silence the check.
        let modelProducers = Self.offsets(of: ["sanitize(", "respondCoarse("], in: text)
        #expect(!permissiveProducers.isEmpty, "no permissive coarse producer remains")
        #expect(modelProducers.count >= 8,
                """
                the @Generable coarse producers thinned out to \(modelProducers.count) — \
                a needle that matches too little makes this check pass vacuously for \
                `.permissive` labels and fail wrongly for `.model` ones
                """)

        /// Distance from `site` back to the nearest producer above it, or `nil`
        /// when there is none.
        ///
        /// OPTIONAL, not a sentinel. The first draft returned `Int.min` for
        /// "none" and then computed `site - Int.min`, which OVERFLOWS and traps
        /// — and it traps on exactly the shape this canary exists for: all
        /// three `.permissive` labels have NO `sanitize(schema:` above them
        /// anywhere in the file. The suite took the test host down and reported
        /// four failures none of which was the real one. A sentinel that stands
        /// for an absence, arithmetic'd as though it were a value, is the same
        /// defect class the column below it exists to end.
        func distanceAbove(_ producers: [Int], _ site: Int) -> Int? {
            producers.last(where: { $0 < site }).map { site - $0 }
        }

        /// `nil` means "no producer of this kind above the site at all", which
        /// is FARTHER than any real distance.
        func isNearer(_ lhs: Int?, than rhs: Int?) -> Bool {
            guard let lhs else { return false }
            guard let rhs else { return true }
            return lhs < rhs
        }

        var permissiveLabels = 0
        var modelLabels = 0
        for label in ["permissive", "model"] {
            for site in Self.offsets(of: ["verdictProvenance: .\(label)"], in: text) {
                let permissiveDistance = distanceAbove(permissiveProducers, site)
                let modelDistance = distanceAbove(modelProducers, site)
                if label == "permissive" {
                    permissiveLabels += 1
                    #expect(isNearer(permissiveDistance, than: modelDistance),
                            """
                            a `.permissive` label at offset \(site) is nearer a sanitize() \
                            than a permissive call — a `@Generable` verdict is being \
                            recorded as a runner fabrication
                            """)
                } else {
                    modelLabels += 1
                    #expect(isNearer(modelDistance, than: permissiveDistance),
                            """
                            a `.model` label at offset \(site) is nearer a permissive call \
                            than a sanitize() — a runner-hardcoded `.strong` is being \
                            recorded as the model's own grade, which is the whole defect \
                            playhead-iw7q exists to end
                            """)
                }
            }
        }
        // Anti-vacuity: a predicate that iterates an empty set passes forever.
        #expect(permissiveLabels >= 3, "the permissive arms lost a label: \(permissiveLabels)")
        #expect(modelLabels >= 4, "the @Generable arms lost a label: \(modelLabels)")
    }

    /// The re-wraps must FORWARD. The coarse pass renumbers a recovered window
    /// into `windows.count`, and stamping a literal there would overwrite what
    /// the recovery actually observed — the same class of loss as the column
    /// not existing, one layer up.
    @Test("the three re-wraps forward the provenance rather than re-deriving it")
    func theRewrapsForward() throws {
        let text = try Self.source(Self.classifierPath)
        let forwards = Self.offsets(
            of: ["verdictProvenance: output.verdictProvenance",
                 "verdictProvenance: recovered.verdictProvenance"],
            in: text
        )
        #expect(forwards.count >= 3,
                "a re-wrap stopped forwarding the provenance it was handed: \(forwards.count)")
    }

    /// And the runner's two row builders must carry it to the row, which is
    /// where the column finally gets written.
    @Test("BackfillJobRunner carries the provenance onto both row shapes")
    func theRunnerCarriesIt() throws {
        let text = try Self.source("Playhead/Services/AdDetection/BackfillJobRunner.swift")
        #expect(text.contains("verdictProvenance: windowOutput.verdictProvenance"),
                "the coarse row builder no longer forwards the window's provenance")
        #expect(text.contains("ScanVerdictProvenance(\n                observedPermissiveFallback: windowOutput.usedPermissiveFallback\n            )"),
                "the refinement row builder no longer converts eu1's observation")
    }
}
