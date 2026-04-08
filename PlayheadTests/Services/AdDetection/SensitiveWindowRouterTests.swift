// SensitiveWindowRouterTests.swift
// bd-1en Phase 1: routing tests for the dispatch layer that decides
// which coarse-pass windows go through the permissive
// SystemLanguageModel path. The router pulls trigger rules from the
// shared `RedactionRules.json` dictionary, so these tests use the
// real bundled dictionary loaded via `PromptRedactor.loadDefault`.

import Foundation
import Testing

@testable import Playhead

@Suite("SensitiveWindowRouter")
struct SensitiveWindowRouterTests {

    // MARK: - Single-segment routing

    @Test("non-sensitive content routes to .normal")
    func normalContentRoutesNormal() throws {
        let router = makeRouter()
        let segment = makeSegment(index: 0, text: "Conan was joined this week by his old friend.")
        #expect(router.route(window: [segment]) == .normal)
    }

    @Test("vaccine vocabulary routes to .sensitive")
    func vaccineVocabularyRoutesSensitive() throws {
        let router = makeRouter()
        let segment = makeSegment(index: 0, text: "Schedule your flu vaccine at CVS today.")
        #expect(router.route(window: [segment]) == .sensitive)
    }

    @Test("pharma drug brand routes to .sensitive")
    func pharmaBrandRoutesSensitive() throws {
        let router = makeRouter()
        let segment = makeSegment(index: 0, text: "Ask your doctor about Trulicity.")
        #expect(router.route(window: [segment]) == .sensitive)
    }

    @Test("mental health service brand routes to .sensitive")
    func mentalHealthRoutesSensitive() throws {
        let router = makeRouter()
        let segment = makeSegment(index: 0, text: "Try BetterHelp for online therapy support.")
        #expect(router.route(window: [segment]) == .sensitive)
    }

    @Test("regulated medical test routes to .sensitive")
    func regulatedMedicalTestRoutesSensitive() throws {
        let router = makeRouter()
        let segment = makeSegment(index: 0, text: "Get a skin cancer screening at our clinic.")
        #expect(router.route(window: [segment]) == .sensitive)
    }

    // MARK: - Cooccurrent vocabulary

    @Test("disease name alone (no trigger) routes to .normal")
    func diseaseNameAloneRoutesNormal() throws {
        // The dictionary marks disease names as `cooccurrent` — they
        // only mask when they share a line with a trigger word, and
        // on their own they don't refuse the safety classifier (R2 in
        // the bd-1en probe matrix). The router must mirror that and
        // leave windows that only mention diseases on the normal path.
        let router = makeRouter()
        let segment = makeSegment(index: 0, text: "He recovered quickly from his shingles last spring.")
        #expect(router.route(window: [segment]) == .normal)
    }

    // MARK: - Multi-segment / mixed windows

    @Test("multi-segment window with one trigger segment routes to .sensitive")
    func multiSegmentWithTriggerRoutesSensitive() throws {
        let router = makeRouter()
        let segments: [AdTranscriptSegment] = [
            makeSegment(index: 0, text: "Welcome back to the show."),
            makeSegment(index: 1, text: "Today we're talking about late-night television."),
            makeSegment(index: 2, text: "Schedule your flu shot at CVS in minutes."),
            makeSegment(index: 3, text: "Now back to the interview."),
            makeSegment(index: 4, text: "Where were we?"),
        ]
        #expect(router.route(window: segments) == .sensitive)
    }

    @Test("multi-segment window with no triggers routes to .normal")
    func multiSegmentWithoutTriggerRoutesNormal() throws {
        let router = makeRouter()
        let segments: [AdTranscriptSegment] = [
            makeSegment(index: 0, text: "Welcome back to the show."),
            makeSegment(index: 1, text: "Today we're talking about late-night television."),
            makeSegment(index: 2, text: "Conan tells a story about his dog."),
        ]
        #expect(router.route(window: segments) == .normal)
    }

    // MARK: - Case sensitivity

    @Test("trigger match is case-insensitive")
    func caseInsensitiveMatch() throws {
        let router = makeRouter()
        let segment = makeSegment(index: 0, text: "VACCINE clinics open all weekend.")
        #expect(router.route(window: [segment]) == .sensitive)
    }

    // MARK: - Noop fallback

    @Test("noop router always returns .normal even on sensitive content")
    func noopRouterAlwaysNormal() {
        let router = SensitiveWindowRouter.noop
        #expect(router.hasRules == false)
        let segment = makeSegment(index: 0, text: "Schedule your flu vaccine at CVS.")
        #expect(router.route(window: [segment]) == .normal)
    }

    @Test("router built from a redactor with no categories has no rules")
    func emptyDictionaryRouterHasNoRules() {
        let dict = PromptRedactor.Dictionary(version: 0, schemaVersion: 1, categories: [])
        let redactor = PromptRedactor(dictionary: dict)
        let router = SensitiveWindowRouter(redactor: redactor)
        #expect(router.hasRules == false)
    }

    // MARK: - bd-1en Phase 2: literal-prompt routing via routeText

    @Test("routeText fires when the rendered prompt embeds pharma trigger vocabulary")
    func routeTextFiresOnEmbeddedPharma() {
        // The refinement-pass code path passes `plan.prompt` (the literal
        // rendered prompt the FM will see) to `routeText`. This catches
        // pharma triggers regardless of whether they live in the window
        // segments or in the embedded evidence-catalog snippet.
        let router = makeRouter()
        let prompt = """
        Refine ad spans.
        L4> "Hey everyone, welcome to the show"
        L5> "Let's talk off camera"
        [E1] "Schedule your shingles vaccine at CVS today" (brandSpan, line 0)
        """
        #expect(router.routeText(prompt) == .sensitive)
    }

    @Test("routeText stays normal when the rendered prompt is clean")
    func routeTextStaysNormalOnCleanPrompt() {
        let router = makeRouter()
        let prompt = """
        Refine ad spans.
        L0> "Welcome back to the show."
        L1> "Conan tells a joke about his dog."
        """
        #expect(router.routeText(prompt) == .normal)
    }
}

// MARK: - Fixtures

private func makeRouter() -> SensitiveWindowRouter {
    // The PlayheadTests bundle is a hosted unit-test bundle, so the
    // bundled `RedactionRules.json` is reachable through Bundle.main
    // (the host app's bundle).
    guard let redactor = PromptRedactor.loadDefault() else {
        // If the dictionary failed to load the routing tests have
        // nothing to assert; surface the failure clearly rather than
        // silently passing on a noop router.
        Issue.record("RedactionRules.json failed to load — bundle resource missing")
        return SensitiveWindowRouter.noop
    }
    return SensitiveWindowRouter(redactor: redactor)
}

private func makeSegment(
    index: Int,
    text: String
) -> AdTranscriptSegment {
    AdTranscriptSegment(
        atoms: [
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: "asset-router-test",
                    transcriptVersion: "transcript-v1",
                    atomOrdinal: index
                ),
                contentHash: "hash-\(index)",
                startTime: Double(index),
                endTime: Double(index) + 1,
                text: text,
                chunkIndex: index
            )
        ],
        segmentIndex: index
    )
}
