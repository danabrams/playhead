// FeedTextNormalizer.swift
// Strips HTML, decodes entities, and truncates RSS text for safe storage.
// Used by feed sync to normalize description/summary before persistence.

import Foundation

enum FeedTextNormalizer {

    /// Maximum stored character count for normalized text fields.
    static let maxLength = 4000

    /// Normalize raw RSS text: strip HTML tags, decode entities, collapse whitespace,
    /// and truncate to `maxLength`.
    static func normalize(_ raw: String?) -> String? {
        guard let raw, !raw.isEmpty else { return nil }

        var text = raw

        // 1a. Strip <script>...</script> and <style>...</style> blocks entirely
        //     (including content) before general tag stripping, to prevent CSS/JS
        //     text from leaking into normalized output.
        text = text.replacingOccurrences(
            of: "<script[^>]*>[\\s\\S]*?</script>",
            with: "",
            options: [.regularExpression, .caseInsensitive]
        )
        text = text.replacingOccurrences(
            of: "<style[^>]*>[\\s\\S]*?</style>",
            with: "",
            options: [.regularExpression, .caseInsensitive]
        )

        // 1b. Strip HTML tags (greedy, handles multi-line)
        text = text.replacingOccurrences(
            of: "<[^>]+>",
            with: "",
            options: .regularExpression
        )

        // 2. Decode common HTML entities
        text = decodeEntities(text)

        // 3. Collapse whitespace runs (spaces, tabs, newlines) to single space
        text = text.replacingOccurrences(
            of: "\\s+",
            with: " ",
            options: .regularExpression
        )

        // 4. Trim
        text = text.trimmingCharacters(in: .whitespacesAndNewlines)

        guard !text.isEmpty else { return nil }

        // 5. Truncate
        if text.count > maxLength {
            let endIndex = text.index(text.startIndex, offsetBy: maxLength)
            text = String(text[..<endIndex])
        }

        return text
    }

    /// Stable hash of raw source text for rebuild detection.
    /// Uses FNV-1a 64-bit for speed and determinism (no cryptographic need).
    static func stableHash(_ raw: String?) -> UInt64? {
        guard let raw, !raw.isEmpty else { return nil }
        let bytes = Array(raw.utf8)
        return fnv1a64(bytes)
    }

    /// Build `FeedDescriptionMetadata` from raw parsed episode fields.
    static func makeMetadata(
        rawDescription: String?,
        rawSummary: String?
    ) -> FeedDescriptionMetadata? {
        let descHash = stableHash(rawDescription)
        let sumHash = stableHash(rawSummary)

        // If both sources are nil/empty, don't create metadata at all.
        guard descHash != nil || sumHash != nil else { return nil }

        return FeedDescriptionMetadata(
            feedDescription: normalize(rawDescription),
            feedSummary: normalize(rawSummary),
            sourceHashes: .init(
                descriptionHash: descHash,
                summaryHash: sumHash
            )
        )
    }

    // MARK: - Private

    /// Decode common HTML/XML entities.
    private static func decodeEntities(_ text: String) -> String {
        var result = text
        // NOTE: &amp; is decoded LAST to prevent double-decoding.
        // If &amp; is first, "&amp;lt;" becomes "&lt;" then "<".
        let entities: [(String, String)] = [
            ("&lt;", "<"),
            ("&gt;", ">"),
            ("&quot;", "\""),
            ("&apos;", "'"),
            ("&#39;", "'"),
            ("&nbsp;", " "),
            ("&ndash;", "\u{2013}"),
            ("&mdash;", "\u{2014}"),
            ("&hellip;", "\u{2026}"),
            ("&lsquo;", "\u{2018}"),
            ("&rsquo;", "\u{2019}"),
            ("&ldquo;", "\u{201C}"),
            ("&rdquo;", "\u{201D}"),
            ("&amp;", "&"),
        ]
        for (entity, replacement) in entities {
            result = result.replacingOccurrences(of: entity, with: replacement)
        }
        // Decode numeric entities: &#123; and &#x1F;
        result = decodeNumericEntities(result)
        return result
    }

    /// Decode numeric HTML entities (decimal &#NNN; and hex &#xHHH;).
    private static func decodeNumericEntities(_ text: String) -> String {
        var result = text

        // Decimal: &#123;
        let decimalPattern = "&#(\\d+);"
        if let regex = try? NSRegularExpression(pattern: decimalPattern) {
            let range = NSRange(result.startIndex..., in: result)
            let matches = regex.matches(in: result, range: range).reversed()
            for match in matches {
                guard let codeRange = Range(match.range(at: 1), in: result),
                      let code = UInt32(result[codeRange]),
                      let scalar = Unicode.Scalar(code) else { continue }
                let fullRange = Range(match.range, in: result)!
                result.replaceSubrange(fullRange, with: String(scalar))
            }
        }

        // Hex: &#x1F;
        let hexPattern = "&#x([0-9a-fA-F]+);"
        if let regex = try? NSRegularExpression(pattern: hexPattern) {
            let range = NSRange(result.startIndex..., in: result)
            let matches = regex.matches(in: result, range: range).reversed()
            for match in matches {
                guard let codeRange = Range(match.range(at: 1), in: result),
                      let code = UInt32(result[codeRange], radix: 16),
                      let scalar = Unicode.Scalar(code) else { continue }
                let fullRange = Range(match.range, in: result)!
                result.replaceSubrange(fullRange, with: String(scalar))
            }
        }

        return result
    }

    /// FNV-1a 64-bit hash. Deterministic, fast, no external dependencies.
    private static func fnv1a64(_ bytes: [UInt8]) -> UInt64 {
        var hash: UInt64 = 0xcbf29ce484222325 // FNV offset basis
        let prime: UInt64 = 0x100000001b3      // FNV prime
        for byte in bytes {
            hash ^= UInt64(byte)
            hash &*= prime
        }
        return hash
    }
}
