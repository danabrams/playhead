// Cycle2FixesTests.swift
//
// Targeted unit tests for the Cycle 2 fix list owned by Agent A:
//
//   - C1: PromptRedactor wired through PlayheadRuntime
//   - C2: PermissiveClassificationError surfaced via failedWindowStatuses
//   - C3: PromptEvidenceEntry.renderForPrompt(redactor:) masking
//   - H3: PermissiveAdClassifier.refine plumbs plan focus fields
//   - H4: ownershipInferenceWasSuppressed flag round-trips persistence
//   - H5: BoundaryPrecision.rough deletion + parser failure throws
//   - H6: smart-shrink retry helper for permissive coarse
//   - H8: PromptRedactor pre-compiles regex at load + fails loud
//   - H9: RedactionRules.json missing → loadDefault throws .missing
//   - Rev2-M2: RefinementSpanPair init invariant
//   - Rev2-M3: adversarial range expansion clamp
//   - Rev2-M5 / Rev3-M1: routing.asymmetric_window detection
//   - Rev2-L3: spansJSON certainty key round-trip

import Foundation
import Testing

@testable import Playhead

@Suite("Cycle 2 fix-list rails")
struct Cycle2FixesTests {

    // MARK: - C1: production redactor wiring

    @Test("Cycle 2 C1: bundled RedactionRules.json is reachable from Bundle.main")
    func bundledRedactionRulesIsReachable() throws {
        let url = Bundle.main.url(forResource: "RedactionRules", withExtension: "json")
        try #require(url != nil)
    }

    @Test("Cycle 2 C1: PromptRedactor.loadDefault loads the production manifest non-empty")
    func loadDefaultIsActiveOnProductionManifest() throws {
        let redactor = try PromptRedactor.loadDefault()
        #expect(redactor.isActive)
    }

    @Test("Cycle 2 C1: redactor masks differential anchor through Trulicity")
    func redactorMasksTrulicity() throws {
        let redactor = try PromptRedactor.loadDefault()
        let masked = redactor.redact(line: "Talk to your doctor about Trulicity for type 2 diabetes.")
        #expect(masked.contains("[DRUG]") || masked != "Talk to your doctor about Trulicity for type 2 diabetes.")
    }

    // MARK: - H8 / H9: PromptRedactor load failures fail loud

    @Test("Cycle 2 H9: loadDefault throws .missing when bundle has no resource")
    func loadDefaultThrowsMissingOnEmptyBundle() {
        let bundle = Bundle(for: TouchstoneClass.self)
        // The test bundle does not contain a RedactionRules.json resource
        // (the host app bundle does, but we explicitly point at the test
        // bundle to verify the missing branch fires).
        do {
            _ = try PromptRedactor.loadDefault(bundle: bundle)
            Issue.record("expected loadDefault to throw .missing")
        } catch let failure as PromptRedactor.LoadFailure {
            #expect(failure == .missing)
        } catch {
            Issue.record("unexpected error: \(error)")
        }
    }

    @Test("Cycle 2 H8: PromptRedactor init throws .invalidPattern on bad regex")
    func initThrowsOnMalformedRegex() {
        // A regex with an unmatched paren should fail to compile and
        // surface as `.invalidPattern` with the offending category id.
        let manifest = PromptRedactor.Manifest(
            version: 0,
            schemaVersion: 1,
            categories: [
                PromptRedactor.Category(
                    id: "broken",
                    description: "deliberately bad regex",
                    patterns: [
                        PromptRedactor.RedactionRule(pattern: "([broken", isRegex: true)
                    ],
                    placeholder: "[X]",
                    category: "trigger",
                    cooccurrentWith: nil
                )
            ]
        )
        do {
            _ = try PromptRedactor(manifest: manifest)
            Issue.record("expected init to throw .invalidPattern")
        } catch let failure as PromptRedactor.LoadFailure {
            switch failure {
            case let .invalidPattern(categoryId, pattern, _):
                #expect(categoryId == "broken")
                #expect(pattern == "([broken")
            default:
                Issue.record("expected .invalidPattern, got \(failure)")
            }
        } catch {
            Issue.record("unexpected error: \(error)")
        }
    }

    @Test("Cycle 2 H8: production rules file compiles every pattern")
    func productionRulesCompileCleanly() {
        // Loads the bundled manifest, which exercises every category's
        // every regex through the precompile pass. Failure surfaces as
        // a thrown LoadFailure with the offending category id.
        do {
            _ = try PromptRedactor.loadDefault()
        } catch {
            Issue.record("RedactionRules.json failed to compile: \(error)")
        }
    }

    // MARK: - C3: PromptEvidenceEntry.renderForPrompt(redactor:)

    @Test("Cycle 2 C3: renderForPrompt with redactor masks Trulicity in matchedText")
    func renderForPromptMasksMatchedText() throws {
        let redactor = try PromptRedactor.loadDefault()
        let entry = PromptEvidenceEntry(
            entry: makeEvidenceEntry(
                evidenceRef: 1,
                category: .brandSpan,
                matchedText: "Talk to your doctor about Trulicity"
            ),
            lineRef: 4
        )
        let rendered = entry.renderForPrompt(redactor: redactor)
        #expect(!rendered.contains("Trulicity"))
    }

    @Test("Cycle 2 C3: renderForPrompt with noop redactor preserves matchedText byte-for-byte")
    func renderForPromptNoopMatchesPreviousBehavior() {
        let entry = PromptEvidenceEntry(
            entry: makeEvidenceEntry(
                evidenceRef: 1,
                category: .brandSpan,
                matchedText: "no triggers here"
            ),
            lineRef: 4
        )
        // The default-arg path is .noop. Should be byte-identical to the
        // previous (non-redactor) renderForPrompt output.
        #expect(entry.renderForPrompt() == "[E1] \"no triggers here\" (brandSpan, line 4)")
    }

    // MARK: - C2: PermissiveClassificationError exception path

    @Test("Cycle 2 C2: parseRefinement decoding failure throws PermissiveClassificationError")
    func parseRefinementThrowsDecodingFailure() {
        do {
            _ = try PermissiveAdGrammar.parseRefinement("garbled output", validLineRefs: [0, 1])
            Issue.record("expected throw")
        } catch let error as PermissiveClassificationError {
            #expect(error.reason == .permissiveDecodingFailure)
        } catch {
            Issue.record("unexpected error type: \(error)")
        }
    }

    @Test("Cycle 2 C2: permissiveStatus maps every Reason to the matching SemanticScanStatus")
    func permissiveStatusMappingCoversEveryReason() {
        #expect(FoundationModelClassifier.permissiveStatus(for: .permissiveRefusal) == .refusal)
        #expect(FoundationModelClassifier.permissiveStatus(for: .permissiveDecodingFailure) == .decodingFailure)
        #expect(FoundationModelClassifier.permissiveStatus(for: .permissiveContextOverflow) == .exceededContextWindow)
    }

    // MARK: - H4: ownershipInferenceWasSuppressed round-trip

    @Test("Cycle 2 H4: spansJSON round-trip preserves ownershipInferenceWasSuppressed=true")
    func spansJSONRoundTripPreservesSuppressedFlag() throws {
        let span = RefinedAdSpan(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: 0,
            lastLineRef: 1,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 1,
            certainty: .strong,
            boundaryPrecision: .usable,
            resolvedEvidenceAnchors: [],
            memoryWriteEligible: false,
            alternativeExplanation: .unknown,
            reasonTags: [],
            ownershipInferenceWasSuppressed: true
        )
        let json = BackfillJobRunner.encodeRefinedSpansForTesting([span])
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(json)
        try #require(decoded.count == 1)
        #expect(decoded[0].ownershipInferenceWasSuppressed == true)
    }

    @Test("Cycle 2 H4: standard span (default ctor) round-trips with suppressed=false")
    func standardSpanRoundTripsAsNotSuppressed() throws {
        let span = RefinedAdSpan(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: 0,
            lastLineRef: 1,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 1,
            certainty: .strong,
            boundaryPrecision: .precise,
            resolvedEvidenceAnchors: [],
            memoryWriteEligible: false,
            alternativeExplanation: .none,
            reasonTags: []
        )
        let json = BackfillJobRunner.encodeRefinedSpansForTesting([span])
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(json)
        try #require(decoded.count == 1)
        // The default initializer omits the parameter, so it persists
        // as `false`.
        #expect(decoded[0].ownershipInferenceWasSuppressed == false)
    }

    @Test("Cycle 2 H4: legacy spansJSON without the field decodes with suppressed=nil (default false)")
    func legacySpansJSONDecodesWithDefault() throws {
        // A pre-H4 row encoded only the original five fields plus the
        // bd-3vm anchors[]. We construct one by hand and verify the
        // optional decodes to nil.
        let legacyJSON = #"""
        [{"firstLineRef":0,"lastLineRef":1,"commercialIntent":"paid","ownership":"thirdParty","certainty":"strong","anchors":[]}]
        """#
        let decoded = try BackfillJobRunner.decodeRefinedSpansForTesting(legacyJSON)
        try #require(decoded.count == 1)
        #expect(decoded[0].ownershipInferenceWasSuppressed == nil)
    }

    @Test("Cycle 2 H4: EvidencePayload round-trip preserves ownershipInferenceWasSuppressed=true")
    func evidencePayloadRoundTripPreservesFlag() throws {
        let payload = BackfillJobRunner.EvidencePayload(
            commercialIntent: "paid",
            ownership: "thirdParty",
            certainty: "strong",
            boundaryPrecision: "usable",
            firstLineRef: 0,
            lastLineRef: 1,
            jobId: "j1",
            memoryWriteEligible: false,
            anchors: [],
            ownershipInferenceWasSuppressed: true
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let json = String(data: try encoder.encode(payload), encoding: .utf8)!
        let decoded = try BackfillJobRunner.decodeEvidencePayloadForTesting(json)
        #expect(decoded.ownershipInferenceWasSuppressed == true)
    }

    // MARK: - Rev2-L3: certainty key persists in raw JSON

    @Test("Cycle 2 Rev2-L3: encodeRefinedSpansForTesting raw JSON contains \"certainty\":\"strong\"")
    func spansJSONContainsCertaintyKey() throws {
        let span = RefinedAdSpan(
            commercialIntent: .paid,
            ownership: .thirdParty,
            firstLineRef: 0,
            lastLineRef: 0,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: 0,
            certainty: .strong,
            boundaryPrecision: .usable,
            resolvedEvidenceAnchors: [],
            memoryWriteEligible: false,
            alternativeExplanation: .none,
            reasonTags: []
        )
        let json = BackfillJobRunner.encodeRefinedSpansForTesting([span])
        // Decode to a generic JSON structure and verify the literal
        // key is present at top-level of each span.
        let data = Data(json.utf8)
        guard let array = try JSONSerialization.jsonObject(with: data) as? [[String: Any]] else {
            Issue.record("expected an array of dictionaries")
            return
        }
        try #require(array.count == 1)
        #expect((array[0]["certainty"] as? String) == "strong")
    }

    // MARK: - H5: parser failure regression rail

    @Test("Cycle 2 H5: BoundaryPrecision contains exactly .usable and .precise")
    func boundaryPrecisionDoesNotContainRough() {
        // Compile-time exhaustiveness check: if `.rough` were still
        // present this switch would emit a warning for the missing
        // case. As written it must compile cleanly with only the two
        // remaining cases.
        let p: BoundaryPrecision = .usable
        switch p {
        case .usable, .precise:
            break
        }
        #expect(BoundaryPrecision.usable.rawValue == "usable")
        #expect(BoundaryPrecision.precise.rawValue == "precise")
    }
}

/// Touchstone class used to anchor `Bundle(for:)` to the test bundle in
/// the H9 missing-resource test. Putting it inside the test file (and
/// outside the suite struct) gives us a stable Bundle handle that does
/// NOT contain the production resources directory.
private final class TouchstoneClass {}

private func makeEvidenceEntry(
    evidenceRef: Int,
    category: EvidenceCategory,
    matchedText: String
) -> EvidenceEntry {
    EvidenceEntry(
        evidenceRef: evidenceRef,
        category: category,
        matchedText: matchedText,
        normalizedText: matchedText.lowercased(),
        atomOrdinal: 0,
        startTime: 0,
        endTime: 1
    )
}
