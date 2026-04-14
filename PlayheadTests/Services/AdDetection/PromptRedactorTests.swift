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
    func loadDefaultParsesBundledManifest() throws {
        // PlayheadTests is a hosted unit-test bundle, so Bundle.main is
        // the Playhead app and the JSON resource is reachable via the
        // default loader path.
        let redactor = try PromptRedactor.loadDefault()
        #expect(redactor.isActive)

        // Round-trip through the Codable shape so a schema regression
        // surfaces here, not at the first call site.
        let url = try #require(Bundle.main.url(forResource: "RedactionRules", withExtension: "json"))
        let data = try Data(contentsOf: url)
        let manifest = try JSONDecoder().decode(PromptRedactor.Manifest.self, from: data)
        #expect(manifest.version == 1)
        #expect(manifest.schemaVersion == 1)
        #expect(manifest.categories.count == 5)

        let byID = Dictionary(uniqueKeysWithValues: manifest.categories.map { ($0.id, $0) })
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

    // MARK: - B11: Typed placeholder assignment

    @Test("typed redaction assigns _A to first unique string in a category")
    func typedPlaceholderFirstOccurrence() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        let line = "Ask your doctor about Ozempic."
        let out = redactor.redact(line: line, policy: .typed, pass: pass)
        #expect(out == "Ask your doctor about [DRUG_A].")
    }

    @Test("typed redaction assigns _B to second unique string in same category")
    func typedPlaceholderSecondOccurrence() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        _ = redactor.redact(line: "Ask about Ozempic.", policy: .typed, pass: pass)
        let out = redactor.redact(line: "Try Trulicity instead.", policy: .typed, pass: pass)
        #expect(out == "Try [DRUG_B] instead.")
    }

    @Test("same string always maps to same ID within a pass")
    func typedPlaceholderConsistentMapping() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        let out1 = redactor.redact(line: "Ozempic works.", policy: .typed, pass: pass)
        let out2 = redactor.redact(line: "Ozempic is great.", policy: .typed, pass: pass)
        #expect(out1 == "[DRUG_A] works.")
        #expect(out2 == "[DRUG_A] is great.")
    }

    @Test("typed placeholders across multiple categories are independent")
    func typedPlaceholderCrossCategory() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        _ = redactor.redact(line: "Ozempic.", policy: .typed, pass: pass)
        let out = redactor.redact(line: "BetterHelp.", policy: .typed, pass: pass)
        // Ozempic is [DRUG_A], BetterHelp is [SERVICE_A] — independent counters.
        #expect(out == "[SERVICE_A].")
    }

    @Test("typed redaction with cooccurrent gating produces typed placeholders for both")
    func typedPlaceholderCooccurrent() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        let out = redactor.redact(
            line: "Schedule your shingles vaccine today.",
            policy: .typed,
            pass: pass
        )
        #expect(out == "Schedule your [CONDITION_A] [PRODUCT_A] today.")
    }

    @Test("multiple occurrences of same string in one line all get same suffix")
    func typedPlaceholderSameStringMultipleTimes() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        let out = redactor.redact(
            line: "Ozempic is better than not Ozempic.",
            policy: .typed,
            pass: pass
        )
        #expect(out == "[DRUG_A] is better than not [DRUG_A].")
    }

    // MARK: - B11: Two-tier policy switching

    @Test("minimal policy passes text through unchanged")
    func minimalPolicyNoRedaction() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        let line = "Ask your doctor about Ozempic and Trulicity."
        let out = redactor.redact(line: line, policy: .minimal, pass: pass)
        #expect(out == line)
    }

    @Test("typed policy applies full typed redaction")
    func typedPolicyRedacts() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        let line = "Ask your doctor about Ozempic."
        let out = redactor.redact(line: line, policy: .typed, pass: pass)
        #expect(out.contains("[DRUG_A]"))
        #expect(!out.contains("Ozempic"))
    }

    @Test("non-medical ad copy unchanged under typed policy")
    func typedPolicyNonMedicalUnchanged() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        let line = "Save twenty percent on your first Casper mattress."
        let out = redactor.redact(line: line, policy: .typed, pass: pass)
        #expect(out == line)
    }

    @Test("original redact(line:) method still works unchanged")
    func legacyRedactMethodUnchanged() {
        let redactor = makeFixtureRedactor()
        // Original method should continue to work identically.
        let line = "Ask your doctor about Trulicity."
        let out = redactor.redact(line: line)
        #expect(out == "Ask your doctor about [DRUG].")
    }

    // MARK: - B11: sponsorEntityHandle preservation

    @Test("typed redaction populates sponsorEntityHandles")
    func sponsorEntityHandlePopulated() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        _ = redactor.redact(line: "Ask about Ozempic.", policy: .typed, pass: pass)
        let handles = pass.sponsorEntityHandles
        #expect(handles.count == 1)
        let handle = handles["ozempic"]
        #expect(handle?.originalText == "Ozempic")
        #expect(handle?.redactedPlaceholder == "[DRUG_A]")
    }

    @Test("multiple entities produce distinct handles")
    func sponsorEntityHandleMultiple() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        _ = redactor.redact(line: "Ozempic vs Trulicity.", policy: .typed, pass: pass)
        let handles = pass.sponsorEntityHandles
        #expect(handles.count == 2)
        #expect(handles["ozempic"] != nil)
        #expect(handles["trulicity"] != nil)
    }

    @Test("minimal policy does not populate sponsorEntityHandles")
    func sponsorEntityHandleMinimalEmpty() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        _ = redactor.redact(line: "Ask about Ozempic.", policy: .minimal, pass: pass)
        #expect(pass.sponsorEntityHandles.isEmpty)
    }

    @Test("sponsorEntityHandle preserved across multiple lines in same pass")
    func sponsorEntityHandleAcrossLines() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        _ = redactor.redact(line: "Ozempic works.", policy: .typed, pass: pass)
        _ = redactor.redact(line: "BetterHelp helps.", policy: .typed, pass: pass)
        _ = redactor.redact(line: "Ozempic again.", policy: .typed, pass: pass)
        let handles = pass.sponsorEntityHandles
        #expect(handles.count == 2)
        #expect(handles["ozempic"]?.redactedPlaceholder == "[DRUG_A]")
        #expect(handles["betterhelp"]?.redactedPlaceholder == "[SERVICE_A]")
    }

    // MARK: - B11: TypedRedactionPass suffix letter generation

    @Test("suffixLetter produces A-Z then AA-AZ then BA for bijective base-26")
    func suffixLetterGeneration() {
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 0) == "A")
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 1) == "B")
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 25) == "Z")
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 26) == "AA")
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 27) == "AB")
        // Extended range: AZ, BA, ZZ, AAA boundaries
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 51) == "AZ")
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 52) == "BA")
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 701) == "ZZ")
        #expect(PromptRedactor.TypedRedactionPass.suffixLetter(for: 702) == "AAA")
    }

    @Test("empty string returns empty without crashing")
    func emptyStringEdgeCase() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        let out = redactor.redact(line: "", policy: .typed, pass: pass)
        #expect(out == "")
    }

    @Test("case-insensitive sponsor entity handles map to same suffix")
    func caseInsensitiveSponsorHandles() {
        let redactor = makeFixtureRedactor()
        let pass = PromptRedactor.TypedRedactionPass()
        // "Ozempic" and "OZEMPIC" should both map to [DRUG_A]
        let out1 = redactor.redact(line: "Ozempic is great.", policy: .typed, pass: pass)
        let out2 = redactor.redact(line: "OZEMPIC is great.", policy: .typed, pass: pass)
        #expect(out1 == "[DRUG_A] is great.")
        #expect(out2 == "[DRUG_A] is great.")
        // Verify the handle preserves original casing from first occurrence
        let handles = pass.sponsorEntityHandles
        #expect(handles["ozempic"]?.originalText == "Ozempic")
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
}

// MARK: - Helpers

private func makeFixtureRedactor() -> PromptRedactor {
    // Load the bundled RedactionRules.json from the host app bundle.
    // All assertions run against the production manifest, not a test-
    // only fixture, so a schema regression surfaces real behavior.
    do {
        return try PromptRedactor.loadDefault()
    } catch {
        Issue.record("RedactionRules.json failed to load: \(error)")
        return .noop
    }
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
        boundarySchemaTokenCount: { 8 },
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
