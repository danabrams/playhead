import Foundation

/// bd-1en Phase 1: routes coarse-pass windows between the standard
/// `@Generable` Foundation Models path and the permissive
/// `SystemLanguageModel(guardrails: .permissiveContentTransformations)`
/// path based on whether the window contains trigger vocabulary that
/// historically refused under the default safety classifier.
///
/// Architectural background — the bd-1en story:
///
/// 1. We initially built `PromptRedactor` to mask trigger vocabulary
///    (vaccine words, pharma drug brands, mental-health services,
///    regulated medical tests) BEFORE prompt construction. The probe
///    matrix in `PlayheadFMSmokeTests::testSafetyClassifierProbeMatrix`
///    proved that masking the right combinations recovered some refused
///    pharma probes, but the surface area was wide and the heuristics
///    were brittle.
///
/// 2. An iOS expert (FoundationModels specialist) reviewed the problem
///    and pointed out that Apple's `.permissiveContentTransformations`
///    guardrails mode is the documented escape hatch — but it ONLY
///    relaxes the safety classifier when the model is generating a
///    plain `String`. `@Generable` guided generation always runs the
///    default guardrails. So the supported architecture is to *route*
///    sensitive content through a separate
///    `SystemLanguageModel(guardrails: .permissiveContentTransformations)`
///    initialization with a string output grammar, while keeping the
///    existing `@Generable` schema for normal content.
///
/// 3. The 124-probe validation matrix in
///    `PlayheadFMSmokeTests::testPermissiveTransformationProbeMatrix`
///    ran on a real device against the permissive path and recovered
///    20/20 known-pharma probes (CVS pre-roll family, Trulicity,
///    Ozempic, Rinvoq, BetterHelp, regulated medical tests) with zero
///    `GenerationError.refusal` cases — empirically validating the
///    architectural recommendation.
///
/// 4. The router REUSES the `RedactionRules.json` dictionary, but as a
///    *routing* dictionary instead of a *redaction* dictionary. Same
///    data, different purpose. Sensitive windows are passed through
///    the permissive path with their original text intact (no
///    masking) — the permissive guardrails handle the safety
///    classification.
///
/// Disease names alone are NOT a trigger here. The dictionary marks
/// them as `cooccurrent` (only redacted when sharing a line with a
/// trigger), and on their own they don't refuse the classifier — so a
/// window that just mentions "shingles" stays on the normal
/// `@Generable` path.
struct SensitiveWindowRouter: Sendable {

    enum Route: Sendable, Equatable {
        /// Window has no trigger vocabulary — use the existing
        /// `FoundationModelClassifier` `@Generable` path.
        case normal
        /// Window contains trigger vocabulary that historically refuses
        /// under the default safety classifier — route through the
        /// permissive `SystemLanguageModel` path instead.
        case sensitive
    }

    private let triggerRules: [PromptRedactor.RedactionRule]

    /// Construct a router from a `PromptRedactor`. The router pulls the
    /// trigger-category rules out of the dictionary the redactor already
    /// loaded, so the routing vocabulary stays in lockstep with the
    /// shared `RedactionRules.json` dictionary.
    init(redactor: PromptRedactor) {
        self.triggerRules = redactor.allTriggerRules()
    }

    /// Direct-rules constructor used by tests that build a router from a
    /// hand-crafted rule list (and by the noop fallback below).
    init(triggerRules: [PromptRedactor.RedactionRule]) {
        self.triggerRules = triggerRules
    }

    /// Router that always returns `.normal` — used as the fallback when
    /// `RedactionRules.json` failed to load. Production behavior with
    /// the noop router is byte-identical to the pre-bd-1en path.
    static let noop = SensitiveWindowRouter(triggerRules: [])

    /// True when the router has at least one trigger rule loaded.
    var hasRules: Bool { !triggerRules.isEmpty }

    /// Classify a window of segments. Returns `.sensitive` as soon as
    /// any rule matches any segment in the window; otherwise `.normal`.
    func route(window segments: [AdTranscriptSegment]) -> Route {
        guard !triggerRules.isEmpty else { return .normal }
        for segment in segments {
            if matchesAnyTrigger(in: segment.text) {
                return .sensitive
            }
        }
        return .normal
    }

    /// Convenience: classify a single piece of text. Used by router unit
    /// tests so they don't need to construct full `AdTranscriptSegment`
    /// fixtures, and by the integration path when the caller already has
    /// a flattened transcript window string.
    func routeText(_ text: String) -> Route {
        guard !triggerRules.isEmpty else { return .normal }
        return matchesAnyTrigger(in: text) ? .sensitive : .normal
    }

    private func matchesAnyTrigger(in text: String) -> Bool {
        for rule in triggerRules {
            if PromptRedactor.ruleMatches(rule, in: text) {
                return true
            }
        }
        return false
    }
}
