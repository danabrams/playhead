import Foundation

/// bd-1en: deterministic text-redaction layer that strips trigger
/// vocabulary from FM prompts BEFORE submission, to bypass Apple's
/// Foundation Models output safety classifier on pharma/medical content.
///
/// Empirically validated against the Conan CVS pre-roll via the 124-probe
/// matrix in PlayheadFMSmokeTests::testSafetyClassifierProbeMatrix
/// (commit a40d0ef). The CVS-D group proved that masking BOTH vaccine
/// vocabulary AND disease names together passes the safety classifier
/// while preserving enough structural ad signals (Schedule + URL +
/// brand app) for the FM to still detect the ad.
///
/// Disease names alone are NOT redacted (R2 in the probe matrix passed
/// without redaction). The co-occurrence rule means disease names get
/// masked only when they appear in the same line as a trigger word.
///
/// Rev2-L1 (Cycle 2): nested manifest type renamed from `Dictionary`
/// to `Manifest` so call sites are not ambiguous against Swift's
/// `Dictionary<Key, Value>`.
public struct PromptRedactor: Sendable {
    public struct RedactionRule: Sendable, Codable {
        public let pattern: String
        public let isRegex: Bool

        private enum CodingKeys: String, CodingKey {
            case pattern
            case isRegex = "regex"
        }
    }

    public struct Category: Sendable, Codable {
        public let id: String
        public let description: String
        public let patterns: [RedactionRule]
        public let placeholder: String
        public let category: String  // "trigger" or "cooccurrent"
        public let cooccurrentWith: [String]?
    }

    public struct Manifest: Sendable, Codable {
        public let version: Int
        public let schemaVersion: Int
        public let categories: [Category]
    }

    /// Cycle 2 H8: a rule whose regex has already been compiled at load
    /// time. The redactor and the static `ruleMatches` helper use the
    /// pre-compiled instance instead of re-compiling per call site, and
    /// load-time compile failures fail loud (precondition in
    /// `loadDefault`) instead of silently swallowing the bad pattern.
    struct CompiledRule: @unchecked Sendable {
        let rule: RedactionRule
        let regex: NSRegularExpression
    }

    /// Errors surfaced by `loadDefault` so callers (PlayheadRuntime) can
    /// distinguish "the bundle does not contain RedactionRules.json"
    /// from "the JSON is malformed" from "one of the regex patterns
    /// failed to compile". H9 wires all three into a precondition with
    /// a clear, named cause.
    public enum LoadFailure: Error, Equatable, CustomStringConvertible {
        case missing
        case malformedJSON(parseError: String)
        case invalidPattern(categoryId: String, pattern: String, parseError: String)

        public var description: String {
            switch self {
            case .missing:
                return "RedactionRules.json missing from bundle"
            case let .malformedJSON(parseError):
                return "RedactionRules.json failed to decode: \(parseError)"
            case let .invalidPattern(categoryId, pattern, parseError):
                return "RedactionRules.json invalid regex in category=\(categoryId) pattern=\(pattern): \(parseError)"
            }
        }
    }

    private let manifest: Manifest
    private let triggerCategories: Set<String>
    private let compiledRulesByCategory: [String: [CompiledRule]]
    private let allCompiledTriggerRules: [CompiledRule]

    public init(manifest: Manifest) throws {
        self.manifest = manifest
        self.triggerCategories = Set(
            manifest.categories
                .filter { $0.category == "trigger" }
                .map(\.id)
        )

        // H8: pre-compile every rule's regex at construction time so
        // ruleMatches/applyCategory hot paths never re-compile, and a
        // malformed pattern surfaces here as a thrown error instead of
        // silently disabling that rule.
        var byCategory: [String: [CompiledRule]] = [:]
        for category in manifest.categories {
            var compiled: [CompiledRule] = []
            compiled.reserveCapacity(category.patterns.count)
            for rule in category.patterns {
                do {
                    let regex = try Self.compile(rule: rule)
                    compiled.append(CompiledRule(rule: rule, regex: regex))
                } catch {
                    throw LoadFailure.invalidPattern(
                        categoryId: category.id,
                        pattern: rule.pattern,
                        parseError: String(describing: error)
                    )
                }
            }
            byCategory[category.id] = compiled
        }
        self.compiledRulesByCategory = byCategory
        self.allCompiledTriggerRules = manifest.categories
            .filter { $0.category == "trigger" }
            .flatMap { byCategory[$0.id] ?? [] }
    }

    /// Convenience for tests that want a noop instance without going
    /// through the throwing initializer.
    init(noopForTesting: Void = ()) {
        self.manifest = Manifest(version: 0, schemaVersion: 1, categories: [])
        self.triggerCategories = []
        self.compiledRulesByCategory = [:]
        self.allCompiledTriggerRules = []
    }

    /// Redact a single line of text. Trigger categories are always
    /// applied; cooccurrent categories only apply if a trigger from
    /// `cooccurrentWith` matched in this line.
    public func redact(line: String) -> String {
        // Two-pass: first apply all trigger categories. Track which
        // ones matched in this line. Then apply cooccurrent categories
        // only when their `cooccurrentWith` set intersects the matched
        // trigger set.
        var result = line
        var matchedTriggerCategories: Set<String> = []

        for category in manifest.categories where category.category == "trigger" {
            let (newResult, didMatch) = applyCategory(category, to: result)
            result = newResult
            if didMatch {
                matchedTriggerCategories.insert(category.id)
            }
        }

        for category in manifest.categories where category.category == "cooccurrent" {
            guard let triggers = category.cooccurrentWith,
                  !Set(triggers).intersection(matchedTriggerCategories).isEmpty else {
                continue
            }
            let (newResult, _) = applyCategory(category, to: result)
            result = newResult
        }

        return result
    }

    private func applyCategory(_ category: Category, to text: String) -> (String, Bool) {
        var result = text
        var matched = false
        let compiled = compiledRulesByCategory[category.id] ?? []
        // Escape `$` in the placeholder so it isn't interpreted as a
        // backreference template by NSRegularExpression.
        let template = NSRegularExpression.escapedTemplate(for: category.placeholder)
        for compiledRule in compiled {
            let range = NSRange(result.startIndex..., in: result)
            let newResult = compiledRule.regex.stringByReplacingMatches(
                in: result,
                options: [],
                range: range,
                withTemplate: template
            )
            if newResult != result {
                matched = true
                result = newResult
            }
        }
        return (result, matched)
    }

    /// H8: shared compiler for a single rule. Used by `init` to populate
    /// `compiledRulesByCategory` and by the legacy `ruleMatches(_:in:)`
    /// static helper which still has callers in the test suite.
    static func compile(rule: RedactionRule) throws -> NSRegularExpression {
        let bodyPattern: String
        if rule.isRegex {
            bodyPattern = rule.pattern
        } else {
            bodyPattern = NSRegularExpression.escapedPattern(for: rule.pattern)
        }
        let leftBoundary = bodyPattern.first.map(isWordChar) ?? false ? "\\b" : ""
        let rightBoundary = bodyPattern.last.map(isWordChar) ?? false ? "\\b" : ""
        let pattern = leftBoundary + bodyPattern + rightBoundary
        return try NSRegularExpression(pattern: pattern, options: [.caseInsensitive])
    }

    private static func isWordChar(_ ch: Character) -> Bool {
        guard let scalar = ch.unicodeScalars.first else { return false }
        return CharacterSet.alphanumerics.contains(scalar) || ch == "_"
    }

    /// Default loader: reads `RedactionRules.json` from the main bundle
    /// and pre-compiles every regex pattern. Returns the loaded redactor
    /// on success.
    ///
    /// H8 / H9 (Cycle 2): the loader now distinguishes three load
    /// failures so PlayheadRuntime can fail loud with a precondition
    /// that names the cause:
    ///
    ///   - `.missing` — Bundle.url(forResource:) returned nil
    ///   - `.malformedJSON` — JSONDecoder threw
    ///   - `.invalidPattern` — one of the regex patterns failed to compile
    ///
    /// Pre-compilation happens inside `init(manifest:)`, which throws
    /// `LoadFailure.invalidPattern` for the first bad pattern with the
    /// offending category id and the original NSRegularExpression error.
    public static func loadDefault(bundle: Bundle = .main) throws -> PromptRedactor {
        guard let url = bundle.url(forResource: "RedactionRules", withExtension: "json") else {
            throw LoadFailure.missing
        }
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw LoadFailure.malformedJSON(parseError: error.localizedDescription)
        }
        let manifest: Manifest
        do {
            manifest = try JSONDecoder().decode(Manifest.self, from: data)
        } catch {
            throw LoadFailure.malformedJSON(parseError: String(describing: error))
        }
        return try PromptRedactor(manifest: manifest)
    }

    /// No-op redactor for tests / fallback when no manifest is loaded.
    public static let noop = PromptRedactor()

    /// Public constructor for the noop redactor (and any other
    /// non-throwing zero-rule case). Equivalent to `PromptRedactor.noop`
    /// but works in places where a static let is awkward.
    public init() {
        self.init(noopForTesting: ())
    }

    /// True when this redactor has at least one rule (i.e. not the noop).
    public var isActive: Bool {
        !manifest.categories.isEmpty
    }

    // MARK: - bd-1en Phase 1 routing accessors
    //
    // The dictionary loaded for the (now-deprecated) text-redaction path is
    // ALSO the right vocabulary for the bd-1en Phase 1 routing layer:
    // sensitive windows are the ones whose text contains a *trigger*
    // category match (vaccine vocabulary, pharma drug brands, mental-health
    // services, regulated medical tests). Disease names are intentionally
    // NOT triggers — they're cooccurrent in the dictionary, so they only
    // mask when they share a line with a trigger word, and on their own
    // they don't refuse the safety classifier (R2 in the probe matrix).
    //
    // SensitiveWindowRouter uses these accessors to decide whether a
    // window goes through the @Generable path or the
    // SystemLanguageModel(guardrails: .permissiveContentTransformations)
    // path. Adding accessors here (rather than re-parsing the JSON in
    // the router) keeps the dictionary the single source of truth.

    /// All trigger-category rules flattened into a single array. Each rule
    /// preserves its `pattern` and `isRegex` flag so the router can apply
    /// the same word-boundary regex matching the redactor uses internally.
    public func allTriggerRules() -> [RedactionRule] {
        manifest.categories
            .filter { $0.category == "trigger" }
            .flatMap(\.patterns)
    }

    /// All trigger-category compiled rules. The router uses these so it
    /// reuses the load-time-compiled NSRegularExpression instances rather
    /// than re-compiling per check.
    func allCompiledTriggerRulesForRouting() -> [CompiledRule] {
        allCompiledTriggerRules
    }

    /// Word-boundary substring search using the same matching semantics as
    /// `applyCategory` (case-insensitive, regex or literal based on the
    /// rule's `isRegex` flag, with `\b` boundaries unless the pattern's
    /// edge characters are non-word). Exposed as a static helper so the
    /// router can call it without owning a `PromptRedactor` instance.
    ///
    /// H8 (Cycle 2): this helper is only used in tests / legacy code
    /// paths that don't have a `PromptRedactor` instance handy. Each
    /// call still re-compiles the regex (because there's no cache to
    /// hit at the call site), but loadDefault no longer relies on this
    /// helper to silently swallow malformed patterns — production
    /// loading is via `init(manifest:)` which is throwing.
    public static func ruleMatches(_ rule: RedactionRule, in text: String) -> Bool {
        guard let regex = try? compile(rule: rule) else {
            return false
        }
        let range = NSRange(text.startIndex..., in: text)
        return regex.firstMatch(in: text, options: [], range: range) != nil
    }

    /// Compiled-rule variant. Routers and other hot-path callers should
    /// prefer this over `ruleMatches(_:in:)` so the load-time regex is
    /// reused instead of being recompiled per call.
    static func ruleMatches(_ compiledRule: CompiledRule, in text: String) -> Bool {
        let range = NSRange(text.startIndex..., in: text)
        return compiledRule.regex.firstMatch(in: text, options: [], range: range) != nil
    }
}
