// PromptRedactorTests.swift
// bd-1en: unit tests for the deterministic prompt redactor that strips
// trigger vocabulary (vaccine words, pharma brands, etc.) before FM
// prompt construction. The CVS pre-roll golden test below mirrors the
// CVS-D1 PASS-AD pattern from the 124-probe matrix in commit a40d0ef.

import Foundation
import Testing

@testable import Playhead

@Suite("PromptRedactor")
struct PromptRedactorTests {

    // MARK: - Dictionary loader

    @Test("loadDefault parses bundled RedactionRules.json")
    func loadDefaultParsesBundledDictionary() throws {
        // PlayheadTests is a hosted unit-test bundle, so Bundle.main is
        // the Playhead app and the JSON resource is reachable via the
        // default loader path.
        let redactor = PromptRedactor.loadDefault()
        try #require(redactor != nil)
        #expect(redactor!.isActive)

        // Round-trip through the Codable shape so a schema regression
        // surfaces here, not at the first call site.
        let url = try #require(Bundle.main.url(forResource: "RedactionRules", withExtension: "json"))
        let data = try Data(contentsOf: url)
        let dict = try JSONDecoder().decode(PromptRedactor.Dictionary.self, from: data)
        #expect(dict.version == 1)
        #expect(dict.schemaVersion == 1)
        #expect(dict.categories.count == 5)

        let byID = Dictionary(uniqueKeysWithValues: dict.categories.map { ($0.id, $0) })
        let vaccine = try #require(byID["vaccine-vocabulary"])
        #expect(vaccine.category == "trigger")
        let disease = try #require(byID["disease-names-cooccurrent"])
        #expect(disease.category == "cooccurrent")
        #expect(disease.cooccurrentWith?.contains("vaccine-vocabulary") == true)
    }

    // MARK: - Trigger / cooccurrence behavior

    @Test("trigger category masks vaccine vocabulary in isolation")
    func triggerMatchOnVaccineWord() {
        let redactor = makeFixtureRedactor()
        let line = "Get your vaccines this fall."
        let out = redactor.redact(line: line)
        #expect(out == "Get your [PRODUCT] this fall.")
    }

    @Test("disease names alone are NOT masked when no trigger present")
    func diseaseAloneIsNotMasked() {
        let redactor = makeFixtureRedactor()
        let line = "Common conditions include shingles, RSV, and pneumococcal pneumonia."
        let out = redactor.redact(line: line)
        // No trigger word in the line, so the cooccurrent category never
        // fires. Disease names pass through untouched.
        #expect(out == line)
    }

    @Test("cooccurrence rule masks both trigger and disease in same line")
    func cooccurrenceMasksBoth() {
        let redactor = makeFixtureRedactor()
        let line = "Schedule your shingles vaccine today."
        let out = redactor.redact(line: line)
        // Both `shingles` and `vaccine` get masked because the trigger
        // category fired, unlocking the cooccurrent category.
        #expect(out == "Schedule your [CONDITION] [PRODUCT] today.")
    }

    @Test("CVS pre-roll golden output matches probe matrix CVS-D1")
    func cvsPrerollGolden() {
        let redactor = makeFixtureRedactor()
        let line = "Schedule your shingles, RSV, pneumococcal pneumonia vaccine today at cvs.com or on the CVS Health app."
        let out = redactor.redact(line: line)
        // CVS-D1 pattern from the 124-probe matrix: BOTH disease names
        // AND vaccine vocabulary masked, while the structural ad signals
        // (Schedule + cvs.com URL + CVS Health app brand) survive intact.
        #expect(out == "Schedule your [CONDITION], [CONDITION], [CONDITION] [PRODUCT] today at cvs.com or on the CVS Health app.")
    }

    @Test("non-medical ad copy is unchanged")
    func nonMedicalUnchanged() {
        let redactor = makeFixtureRedactor()
        let lines = [
            "Save twenty percent on your first Casper mattress.",
            "HelloFresh delivers fresh ingredients to your door each week.",
            "Manscaped trimmers — built for the modern man.",
            "Welcome back to the show — today we're talking about the new fall lineup."
        ]
        for line in lines {
            #expect(redactor.redact(line: line) == line)
        }
    }

    @Test("word boundary: vaccinated matches but vaccinator does not")
    func wordBoundaryCorrectness() {
        let redactor = makeFixtureRedactor()
        // `vaccinat(e|ed|es|ing|ion|ions)` is the regex; `vaccinated`
        // matches `vaccinated`, while `vaccinator` does NOT match the
        // alternation suffixes and therefore stays intact.
        #expect(redactor.redact(line: "Be vaccinated this fall.") == "Be [PRODUCT] this fall.")
        #expect(redactor.redact(line: "The vaccinator was busy.") == "The vaccinator was busy.")
    }

    @Test("pharma drug brand triggers regardless of context")
    func pharmaBrandTrigger() {
        let redactor = makeFixtureRedactor()
        let line = "Ask your doctor about Trulicity."
        #expect(redactor.redact(line: line) == "Ask your doctor about [DRUG].")
    }

    @Test("noop redactor returns input unchanged")
    func noopUnchanged() {
        let line = "Schedule your shingles, RSV, pneumococcal pneumonia vaccine today at cvs.com."
        #expect(PromptRedactor.noop.redact(line: line) == line)
        #expect(!PromptRedactor.noop.isActive)
    }

    // MARK: - FoundationModelClassifier integration

    @Test("buildPrompt with default noop redactor leaves text intact")
    func buildPromptDefaultIsNoop() {
        let segment = makeRedactorTestSegment(
            index: 0,
            text: "Schedule your shingles vaccine today at cvs.com."
        )
        // No `redactor:` argument — must default to .noop and produce
        // a prompt that contains the original text verbatim.
        let prompt = FoundationModelClassifier.buildPrompt(for: [segment])
        #expect(prompt.contains("Schedule your shingles vaccine today at cvs.com."))
        #expect(!prompt.contains("[PRODUCT]"))
        #expect(!prompt.contains("[CONDITION]"))
    }

    @Test("buildPrompt with active redactor masks per-segment text and preserves L<n>>")
    func buildPromptWithActiveRedactor() {
        let redactor = makeFixtureRedactor()
        let segments = [
            makeRedactorTestSegment(index: 4, text: "Welcome back to the show."),
            makeRedactorTestSegment(index: 5, text: "Schedule your shingles, RSV, pneumococcal pneumonia vaccine today at cvs.com.")
        ]
        let prompt = FoundationModelClassifier.buildPrompt(for: segments, redactor: redactor)
        // Line refs are preserved 1:1 — the FM still references segments
        // by their original segmentIndex, even after the visible text
        // got rewritten.
        #expect(prompt.contains("L4>"))
        #expect(prompt.contains("L5>"))
        #expect(prompt.contains("Welcome back to the show."))
        #expect(prompt.contains("[CONDITION]"))
        #expect(prompt.contains("[PRODUCT]"))
        #expect(!prompt.contains("shingles"))
        #expect(!prompt.contains("vaccine"))
    }

    @Test("classifier initialised with explicit redactor submits redacted prompts to runtime")
    func classifierSubmitsRedactedPrompts() async throws {
        // Round-trip via planPassA: the returned plan's `prompt` field
        // is exactly what the classifier would submit to the runtime.
        // We bypass the env-var gate by passing an explicit redactor to
        // the init, which is the same code path the gate exercises in
        // production once `PLAYHEAD_FM_REDACT=1` is set.
        let redactor = makeFixtureRedactor()
        let runtime = makeStubRuntime()
        let classifier = FoundationModelClassifier(
            runtime: runtime,
            config: .init(safetyMarginTokens: 5, maximumResponseTokens: 6),
            redactor: redactor
        )
        let segments = [
            makeRedactorTestSegment(index: 0, text: "Schedule your shingles vaccine today.")
        ]
        let plans = try await classifier.planPassA(segments: segments)
        try #require(!plans.isEmpty)
        let prompt = plans[0].prompt
        #expect(prompt.contains("L0>"))
        #expect(prompt.contains("[CONDITION]"))
        #expect(prompt.contains("[PRODUCT]"))
        #expect(!prompt.contains("shingles"))
        #expect(!prompt.contains("vaccine"))
    }

    @Test("classifier with default redactor (no env, no override) submits original prompts")
    func classifierDefaultRedactorIsNoop() async throws {
        // The env var is not set in the test environment, so init falls
        // through to .noop and the prompt is unchanged. This is the
        // load-bearing invariant: existing FM tests must see byte-
        // identical prompts when PLAYHEAD_FM_REDACT is unset.
        let alreadySet = ProcessInfo.processInfo.environment["PLAYHEAD_FM_REDACT"] != nil
        guard !alreadySet else { return }

        let runtime = makeStubRuntime()
        let classifier = FoundationModelClassifier(
            runtime: runtime,
            config: .init(safetyMarginTokens: 5, maximumResponseTokens: 6)
        )
        let segments = [
            makeRedactorTestSegment(index: 0, text: "Schedule your shingles vaccine today.")
        ]
        let plans = try await classifier.planPassA(segments: segments)
        try #require(!plans.isEmpty)
        let prompt = plans[0].prompt
        #expect(prompt.contains("shingles vaccine"))
        #expect(!prompt.contains("[CONDITION]"))
        #expect(!prompt.contains("[PRODUCT]"))
    }

    // MARK: - bd-1en v2: window-wide cooccurrence (round 5 fix)

    @Test("redact(lines:) fires cooccurrent disease masking on a sibling line when the trigger lives elsewhere in the window")
    func crossLineCooccurrenceMasksDiseaseOnSeparateLine() {
        let redactor = makeFixtureRedactor()
        // Mirrors the Conan CVS pre-roll multi-segment shape:
        //   L0: vaccines (trigger word, no diseases on this line)
        //   L1: disease enumeration (no trigger on this line)
        //   L2: structural ad pattern (no medical content)
        //
        // The pre-fix per-line redactor would mask only L0; cross-line
        // cooccurrence masks the diseases on L1 too.
        let input = [
            "vaccines",
            "shingles, RSV, pneumococcal pneumonia",
            "Schedule yours today at cvs.com"
        ]
        let out = redactor.redact(lines: input)
        #expect(out[0] == "[PRODUCT]")
        #expect(out[1].contains("[CONDITION]"))
        #expect(!out[1].lowercased().contains("shingles"))
        #expect(!out[1].lowercased().contains("rsv"))
        #expect(!out[1].lowercased().contains("pneumococcal"))
        #expect(!out[1].lowercased().contains("pneumonia"))
        #expect(out[2] == "Schedule yours today at cvs.com")
    }

    @Test("redact(lines:) does NOT fire cooccurrence when no trigger appears in the window")
    func crossLineCooccurrenceDoesNotFireWithoutTrigger() {
        let redactor = makeFixtureRedactor()
        // R2-disease-list-passes-noad invariant: disease enumeration alone
        // is benign and must not be touched.
        let input = [
            "Common conditions include shingles, RSV, and pneumococcal pneumonia.",
            "Today our guest discusses what they felt like."
        ]
        let out = redactor.redact(lines: input)
        #expect(out == input)
    }

    @Test("redact(lines:) preserves line order and count")
    func crossLineRedactionPreservesOrderAndCount() {
        let redactor = makeFixtureRedactor()
        let input = [
            "alpha line with vaccines",
            "beta line with shingles",
            "gamma line with nothing relevant",
            "delta line with Trulicity",
            "epsilon line with RSV"
        ]
        let out = redactor.redact(lines: input)
        #expect(out.count == input.count)
        // L0 has vaccine trigger → masked
        #expect(out[0].contains("[PRODUCT]"))
        // L1 disease cooccurrent fires (vaccine trigger fired in window)
        #expect(out[1].contains("[CONDITION]"))
        // L2 unchanged
        #expect(out[2] == input[2])
        // L3 has pharma drug brand (Trulicity) → masked to [DRUG]
        #expect(out[3].contains("[DRUG]"))
        #expect(!out[3].contains("Trulicity"))
        // L4 RSV is cooccurrent — should fire because vaccine fired in window
        #expect(out[4].contains("[CONDITION]"))
    }

    @Test("redact(lines:) noop returns input array unchanged")
    func crossLineNoopUnchanged() {
        let input = [
            "anything goes",
            "shingles vaccine cvs.com schedule today",
            "BetterHelp Trulicity"
        ]
        let out = PromptRedactor.noop.redact(lines: input)
        #expect(out == input)
    }

    @Test("redact(line:) backward compat: single-line CVS-D1 still produces the same output")
    func singleLineBackwardCompat() {
        let redactor = makeFixtureRedactor()
        // The CVS-D1 winning shape from the 124-probe matrix.
        let input = "Schedule your shingles, RSV, pneumococcal pneumonia vaccine today at cvs.com or on the CVS Health app."
        let single = redactor.redact(line: input)
        let batch = redactor.redact(lines: [input])
        #expect(batch.count == 1)
        #expect(batch[0] == single)
        // Both forms must mask both vaccine and diseases.
        #expect(single.contains("[PRODUCT]"))
        #expect(single.contains("[CONDITION]"))
    }

    // bd-1en v2 catalog redaction is verified end-to-end via the
    // on-device PlayheadFMSmoke run rather than a unit test, because
    // building a synthetic EvidenceCatalog + FoundationModelClassifier
    // wired through the recorder requires a lot of cross-module
    // scaffolding. The pure-redactor cross-line tests above already
    // validate the load-bearing behavior (window-wide cooccurrence);
    // the catalog hook in `buildRefinementPrompt` is a thin call site
    // that batches `segments + promptEvidence.matchedText` through the
    // same `redact(lines:)` API.
}

// MARK: - Helpers

private func makeFixtureRedactor() -> PromptRedactor {
    // Load the bundled RedactionRules.json from the host app bundle.
    // All assertions run against the production dictionary, not a test-
    // only fixture, so a schema regression surfaces real behavior.
    guard let redactor = PromptRedactor.loadDefault() else {
        Issue.record("RedactionRules.json missing from app bundle")
        return .noop
    }
    return redactor
}

private func makeRedactorTestSegment(
    index: Int,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String
) -> AdTranscriptSegment {
    AdTranscriptSegment(
        atoms: [
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "asset-redactor-test",
                    transcriptVersion: "transcript-v1",
                    atomOrdinal: index
                ),
                contentHash: "hash-\(index)",
                startTime: startTime,
                endTime: endTime,
                text: text,
                chunkIndex: index
            )
        ],
        segmentIndex: index
    )
}

private func makeStubRuntime() -> FoundationModelClassifier.Runtime {
    FoundationModelClassifier.Runtime(
        availabilityStatus: { _ in nil },
        contextSize: { 4096 },
        tokenCount: { prompt in
            // Cheap line-count estimator — well under any practical
            // budget for the small fixtures these tests use.
            prompt.split(separator: "\n", omittingEmptySubsequences: false).count * 5
        },
        coarseSchemaTokenCount: { 4 },
        refinementSchemaTokenCount: { 8 },
        makeSession: {
            FoundationModelClassifier.Runtime.Session(
                prewarm: { _ in },
                respondCoarse: { _ in
                    CoarseScreeningSchema(disposition: .noAds, support: nil)
                },
                respondRefinement: { _ in
                    RefinementWindowSchema(spans: [])
                }
            )
        }
    )
}
