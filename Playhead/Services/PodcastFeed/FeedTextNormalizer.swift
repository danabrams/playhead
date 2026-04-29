// FeedTextNormalizer.swift
// Strips HTML, decodes entities, and truncates RSS text for safe storage.
// Used by feed sync to normalize description/summary before persistence.

import Foundation

enum FeedTextNormalizer {

    /// Maximum stored character count for normalized text fields.
    static let maxLength = 4000

    /// Pre-regex byte cap. Truncates the raw input before the regex pipeline
    /// runs so a multi-megabyte feed description (RSS in the wild has been
    /// observed at >1 MB on misconfigured shows) cannot trigger a perf
    /// cliff in the four uncapped regex passes below. The cap is
    /// deliberately generous — typical descriptions are <10 KB — so this
    /// only fires on adversarial / pathological input.
    ///
    /// Final post-regex truncation to `maxLength` (4 000 chars) still
    /// applies; this just bounds the regex working set.
    static let preRegexByteCap = 256 * 1024  // 256 KB

    /// Normalize raw RSS text: strip HTML tags, decode entities, collapse whitespace,
    /// and truncate to `maxLength`.
    static func normalize(_ raw: String?) -> String? {
        guard let raw, !raw.isEmpty else { return nil }

        var text = raw

        // 0. Cheap byte-cap up front. The regex pipeline below has four
        //    uncapped passes whose worst-case behavior on adversarial
        //    multi-MB input dominates this function's cost; truncate
        //    first so the pathological path is bounded.
        //
        //    Trade-off (L2 / rfu-mn): the byte-cap walk-back lands on
        //    a Character boundary but does NOT inspect HTML structure.
        //    If an open `<script>...` or `<style>...` block straddles
        //    the 256 KB boundary, the closing tag is dropped along
        //    with everything past the cap. The follow-on `<script>`
        //    and `<style>` content strippers (steps 1a) require
        //    matching open/close pairs and therefore leave the
        //    pre-cap remnant in place. Any leaked content is bounded
        //    by the post-regex `maxLength` truncation (4 000 chars)
        //    AND by the generic `<[^>]+>` tag stripper (step 1b),
        //    which removes the open `<script ...>` tag itself even if
        //    the close was lost. Net effect: at most 4 000 chars of
        //    de-tagged JS/CSS body text from the pre-cap window can
        //    survive into the normalized output — the same upper
        //    bound that already governs every other stripped-tag
        //    fragment. Adding a structure-aware walk-back to fix
        //    this is not worth the complexity for content past the
        //    cap, which is already adversarial.
        if text.utf8.count > Self.preRegexByteCap {
            // Find a UTF-8-safe truncation point: cap by UTF-8 byte count
            // but advance to a Character boundary so we don't split a
            // multi-byte scalar mid-stream.
            let utf8 = text.utf8
            var endByteIndex = utf8.index(utf8.startIndex, offsetBy: Self.preRegexByteCap)
            // Walk backward to a valid Character boundary if the cap
            // landed inside a scalar.
            while endByteIndex > utf8.startIndex,
                  String.Index(endByteIndex, within: text) == nil {
                endByteIndex = utf8.index(before: endByteIndex)
            }
            if let stringIndex = String.Index(endByteIndex, within: text) {
                text = String(text[..<stringIndex])
            }
        }

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
    ///
    /// Returns `Int64?` holding the raw bit pattern of the FNV-1a UInt64.
    /// We bit-cast rather than store UInt64 directly because SwiftData
    /// persists `FeedDescriptionMetadata` as a Codable blob, and the
    /// NSNumber bridge on the read path traps on UInt64 values > Int64.max.
    /// Equality is preserved across the cast: equal UInt64s produce equal
    /// Int64s. Consumers only compare these hashes for identity.
    static func stableHash(_ raw: String?) -> Int64? {
        guard let raw, !raw.isEmpty else { return nil }
        let bytes = Array(raw.utf8)
        return Int64(bitPattern: fnv1a64(bytes))
    }

    /// Build `FeedDescriptionMetadata` from raw parsed episode fields.
    ///
    /// playhead-gtt9.22: Optional `chapterEvidence` and `chaptersFeedURL`
    /// arguments allow the persistence layer to seed chapter-derived
    /// signal at feed-refresh time. When both raw text fields are nil/
    /// empty AND there is no chapter evidence AND no chapters URL,
    /// metadata creation is suppressed (consumers expect a non-nil
    /// metadata blob to mean "we have *something* useful for this
    /// episode" — preserving that invariant prevents needless allocs
    /// for chapter-less feeds).
    static func makeMetadata(
        rawDescription: String?,
        rawSummary: String?,
        chapterEvidence: [ChapterEvidence]? = nil,
        chaptersFeedURL: URL? = nil
    ) -> FeedDescriptionMetadata? {
        let descHash = stableHash(rawDescription)
        let sumHash = stableHash(rawSummary)

        let hasChapters = !(chapterEvidence?.isEmpty ?? true)
        let hasChaptersURL = chaptersFeedURL != nil

        // Suppress empty metadata for episodes with literally no signal.
        guard descHash != nil || sumHash != nil || hasChapters || hasChaptersURL else {
            return nil
        }

        return FeedDescriptionMetadata(
            feedDescription: normalize(rawDescription),
            feedSummary: normalize(rawSummary),
            sourceHashes: .init(
                descriptionHash: descHash,
                summaryHash: sumHash
            ),
            chapterEvidence: hasChapters ? chapterEvidence : nil,
            chaptersFeedURL: chaptersFeedURL
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
