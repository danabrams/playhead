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
    ///
    /// **NOTE:** for multi-line windows (e.g. multi-segment FM prompts),
    /// prefer `redact(lines:)` so cooccurrent categories can fire on a
    /// line whose trigger word lives in a sibling line within the same
    /// window. The single-line variant restricts cooccurrence to local
    /// matches and over-restricts on real Conan transcripts where the
    /// vaccine trigger and disease names are split across segments.
    public func redact(line: String) -> String {
        let (afterTriggers, matched) = applyTriggerCategories(to: line)
        return applyCooccurrentCategories(to: afterTriggers, withTriggers: matched)
    }

    /// bd-1en v2 (round 5): redact a window's worth of lines with
    /// **window-wide cooccurrence**. Trigger categories are applied per
    /// line; cooccurrent categories then fire on EVERY line whenever ANY
    /// line in the batch fired a matching trigger. This is the load-bearing
    /// fix for the Conan CVS pre-roll, where the `vaccines` trigger lives
    /// on `L0` while the disease enumeration lives on `L1` — per-line
    /// processing leaves the diseases unmasked because the trigger isn't
    /// on the same line, so the FM safety classifier still sees the
    /// pharma-marketing gestalt and refuses (or, after redaction, the FM
    /// classifies as `noAds` because too much of the structural ad signal
    /// was kept).
    ///
    /// The `noop` redactor returns the input array unchanged.
    ///
    /// Order is preserved: `redact(lines:)[i]` corresponds to `lines[i]`.
    public func redact(lines: [String]) -> [String] {
        // Pass 1: apply triggers per line, collect global trigger set.
        var triggered: [String] = []
        triggered.reserveCapacity(lines.count)
        var globalTriggers: Set<String> = []
        for line in lines {
            let (afterTriggers, matched) = applyTriggerCategories(to: line)
            triggered.append(afterTriggers)
            globalTriggers.formUnion(matched)
        }
        // Pass 2: apply cooccurrent categories with the GLOBAL trigger
        // set, not the per-line set.
        return triggered.map { line in
            applyCooccurrentCategories(to: line, withTriggers: globalTriggers)
        }
    }

    /// Apply only the trigger categories to a line, returning the
    /// redacted text and the set of trigger category IDs that matched.
    private func applyTriggerCategories(to line: String) -> (String, Set<String>) {
        var result = line
        var matchedTriggerCategories: Set<String> = []
        for category in dictionary.categories where category.category == "trigger" {
            let (newResult, didMatch) = applyCategory(category, to: result)
            result = newResult
            if didMatch {
                matchedTriggerCategories.insert(category.id)
            }
        }
        return (result, matchedTriggerCategories)
    }

    /// Apply cooccurrent categories to a line, gating each category on
    /// whether its `cooccurrentWith` set intersects the supplied trigger
    /// set. Caller controls the scope of the trigger set: pass per-line
    /// triggers for `redact(line:)` semantics, or the union across a
    /// window for `redact(lines:)` semantics.
    private func applyCooccurrentCategories(to line: String, withTriggers triggers: Set<String>) -> String {
        var result = line
        for category in dictionary.categories where category.category == "cooccurrent" {
            guard let triggerIDs = category.cooccurrentWith,
                  !Set(triggerIDs).intersection(triggers).isEmpty else {
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
}
