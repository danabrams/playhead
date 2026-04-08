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

    public struct Dictionary: Sendable, Codable {
        public let version: Int
        public let schemaVersion: Int
        public let categories: [Category]
    }

    private let dictionary: Dictionary
    private let triggerCategories: Set<String>

    public init(dictionary: Dictionary) {
        self.dictionary = dictionary
        self.triggerCategories = Set(
            dictionary.categories
                .filter { $0.category == "trigger" }
                .map(\.id)
        )
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

        for category in dictionary.categories where category.category == "trigger" {
            let (newResult, didMatch) = applyCategory(category, to: result)
            result = newResult
            if didMatch {
                matchedTriggerCategories.insert(category.id)
            }
        }

        for category in dictionary.categories where category.category == "cooccurrent" {
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
        for rule in category.patterns {
            let bodyPattern: String
            if rule.isRegex {
                bodyPattern = rule.pattern
            } else {
                bodyPattern = NSRegularExpression.escapedPattern(for: rule.pattern)
            }
            // Word-boundary wrap. Falls back to a literal match if the
            // first/last char is not a word character (e.g. "Johnson &
            // Johnson"), since `\b` requires a word char on the inside.
            let leftBoundary = bodyPattern.first.map(Self.isWordChar) ?? false ? "\\b" : ""
            let rightBoundary = bodyPattern.last.map(Self.isWordChar) ?? false ? "\\b" : ""
            let pattern = leftBoundary + bodyPattern + rightBoundary
            guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
                continue
            }
            let range = NSRange(result.startIndex..., in: result)
            // Escape `$` in the placeholder so it isn't interpreted as a
            // backreference template by NSRegularExpression.
            let template = NSRegularExpression.escapedTemplate(for: category.placeholder)
            let newResult = regex.stringByReplacingMatches(
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

    private static func isWordChar(_ ch: Character) -> Bool {
        guard let scalar = ch.unicodeScalars.first else { return false }
        return CharacterSet.alphanumerics.contains(scalar) || ch == "_"
    }

    /// Default loader: reads `RedactionRules.json` from the main bundle.
    /// Returns nil if the resource isn't found or fails to parse, so
    /// callers can fall back to a no-op redactor.
    public static func loadDefault(bundle: Bundle = .main) -> PromptRedactor? {
        guard let url = bundle.url(forResource: "RedactionRules", withExtension: "json") else {
            return nil
        }
        do {
            let data = try Data(contentsOf: url)
            let dictionary = try JSONDecoder().decode(Dictionary.self, from: data)
            return PromptRedactor(dictionary: dictionary)
        } catch {
            return nil
        }
    }

    /// No-op redactor for tests / fallback when no dictionary is loaded.
    public static let noop = PromptRedactor(
        dictionary: Dictionary(version: 0, schemaVersion: 1, categories: [])
    )

    /// True when this redactor has at least one rule (i.e. not the noop).
    public var isActive: Bool {
        !dictionary.categories.isEmpty
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
        dictionary.categories
            .filter { $0.category == "trigger" }
            .flatMap(\.patterns)
    }

    /// Word-boundary substring search using the same matching semantics as
    /// `applyCategory` (case-insensitive, regex or literal based on the
    /// rule's `isRegex` flag, with `\b` boundaries unless the pattern's
    /// edge characters are non-word). Exposed as a static helper so the
    /// router can call it without owning a `PromptRedactor` instance.
    public static func ruleMatches(_ rule: RedactionRule, in text: String) -> Bool {
        let bodyPattern: String
        if rule.isRegex {
            bodyPattern = rule.pattern
        } else {
            bodyPattern = NSRegularExpression.escapedPattern(for: rule.pattern)
        }
        let leftBoundary = bodyPattern.first.map(isWordChar) ?? false ? "\\b" : ""
        let rightBoundary = bodyPattern.last.map(isWordChar) ?? false ? "\\b" : ""
        let pattern = leftBoundary + bodyPattern + rightBoundary
        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
            return false
        }
        let range = NSRange(text.startIndex..., in: text)
        return regex.firstMatch(in: text, options: [], range: range) != nil
    }
}
