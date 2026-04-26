// SwiftSourceInspector.swift
// playhead-2axy: shared brace-aware Swift source-body parser.
//
// Extracted from `PermissiveClassifierBoxLazinessTests` (playhead-jndk)
// and `PlayheadRuntimeLoggerLazinessTests` (playhead-jncn) — both files
// shipped near-identical copies of the same walker. Centralising it lets
// future source-canary tests (the launch-perf init guards in this bead,
// and any per-launch-path canary that follows) share a single
// implementation that has been stress-tested across both call sites.
//
// Scope: this is a TEST-ONLY helper. It is intentionally permissive: it
// does NOT attempt to be a real Swift parser. It tracks brace depth,
// recognises `//` line comments, `/* */` block comments, and double-
// quoted string literals, and stops there. That is enough to isolate
// the body of a Swift function or initialiser whose opening `{` is at
// a known offset.
//
// Why XCTest-friendly (not Swift Testing) helpers: the canaries that
// invoke this helper want to be filterable through the Xcode test
// plan's `skippedTests` list. `xctestplan` silently ignores Swift
// Testing identifiers, so canaries are XCTest classes and their
// helpers stay XCTest-shaped.

import Foundation

/// Static helpers that walk Swift source text without invoking a real
/// parser. All functions are pure and deterministic — they only read
/// the input string and never touch the file system.
enum SwiftSourceInspector {

    // MARK: - Public API

    /// Returns the text of the brace-delimited block whose opening `{`
    /// is at `startIndex` in `source`. Tracks nesting depth so inner
    /// braces don't terminate the body early. Treats `//` line comments,
    /// `/* */` block comments, and `"..."` string literals as opaque
    /// (their braces don't count).
    ///
    /// Precondition: `source[startIndex] == "{"`.
    /// Returns the empty string if the brace is unbalanced (caller
    /// should treat that as a malformed source — typically a sign that
    /// the canary's anchor signature has drifted out of sync with the
    /// production code being canaried).
    static func bracedBody(
        in source: String,
        startingAt startIndex: String.Index
    ) -> String {
        precondition(source[startIndex] == "{", "bracedBody called with non-brace start index")
        var depth = 0
        var i = startIndex
        var inLineComment = false
        var inBlockComment = false
        var inString = false
        let endIdx = source.endIndex
        var bodyStart: String.Index?

        while i < endIdx {
            let c = source[i]
            let next = source.index(after: i) < endIdx ? source[source.index(after: i)] : Character("\0")

            if inLineComment {
                if c == "\n" { inLineComment = false }
                i = source.index(after: i)
                continue
            }
            if inBlockComment {
                if c == "*" && next == "/" {
                    inBlockComment = false
                    i = source.index(i, offsetBy: 2)
                    continue
                }
                i = source.index(after: i)
                continue
            }
            if inString {
                if c == "\\" && source.index(after: i) < endIdx {
                    i = source.index(i, offsetBy: 2)
                    continue
                }
                if c == "\"" { inString = false }
                i = source.index(after: i)
                continue
            }

            if c == "/" && next == "/" {
                inLineComment = true
                i = source.index(i, offsetBy: 2)
                continue
            }
            if c == "/" && next == "*" {
                inBlockComment = true
                i = source.index(i, offsetBy: 2)
                continue
            }
            if c == "\"" {
                inString = true
                i = source.index(after: i)
                continue
            }

            if c == "{" {
                depth += 1
                if depth == 1 {
                    bodyStart = source.index(after: i)
                }
            } else if c == "}" {
                depth -= 1
                if depth == 0 {
                    if let start = bodyStart {
                        return String(source[start..<i])
                    }
                    return ""
                }
            }
            i = source.index(after: i)
        }
        return ""
    }

    /// Returns `source` with all `//` line comments and `/* */` block
    /// comments replaced by spaces (newlines are preserved so line
    /// numbers in error messages still line up). String literals are
    /// left intact — callers usually want to grep for tokens that
    /// happen inside string interpolations as well as in code.
    ///
    /// Use this on the output of ``bracedBody(in:startingAt:)`` before
    /// running regex / `contains` checks: the brace walker correctly
    /// tracks depth across comments, but it returns the raw source
    /// slice (including the comment text). Naïve token greps on that
    /// raw slice false-positive on documentation comments that
    /// reference a forbidden symbol.
    static func strippingComments(_ source: String) -> String {
        var out = String()
        out.reserveCapacity(source.count)
        var i = source.startIndex
        var inLineComment = false
        var inBlockComment = false
        var inString = false
        let endIdx = source.endIndex

        while i < endIdx {
            let c = source[i]
            let next = source.index(after: i) < endIdx ? source[source.index(after: i)] : Character("\0")

            if inLineComment {
                if c == "\n" {
                    inLineComment = false
                    out.append(c) // preserve line breaks
                } else {
                    out.append(" ")
                }
                i = source.index(after: i)
                continue
            }
            if inBlockComment {
                if c == "*" && next == "/" {
                    inBlockComment = false
                    out.append("  ")
                    i = source.index(i, offsetBy: 2)
                    continue
                }
                if c == "\n" {
                    out.append(c)
                } else {
                    out.append(" ")
                }
                i = source.index(after: i)
                continue
            }
            if inString {
                if c == "\\" && source.index(after: i) < endIdx {
                    out.append(c)
                    out.append(source[source.index(after: i)])
                    i = source.index(i, offsetBy: 2)
                    continue
                }
                if c == "\"" { inString = false }
                out.append(c)
                i = source.index(after: i)
                continue
            }

            if c == "/" && next == "/" {
                inLineComment = true
                out.append("  ")
                i = source.index(i, offsetBy: 2)
                continue
            }
            if c == "/" && next == "*" {
                inBlockComment = true
                out.append("  ")
                i = source.index(i, offsetBy: 2)
                continue
            }
            if c == "\"" {
                inString = true
                out.append(c)
                i = source.index(after: i)
                continue
            }

            out.append(c)
            i = source.index(after: i)
        }
        return out
    }

    /// Returns `source` with both comments AND double-quoted string
    /// literal **contents** replaced by spaces. The opening and
    /// closing quote characters of each string literal are preserved
    /// so a regex anchored on a quote boundary still matches. Newlines
    /// inside multi-line content are preserved so line numbers in
    /// error messages still line up.
    ///
    /// Use this in source canaries whose token of interest could
    /// plausibly appear inside a string literal — e.g. a regex that
    /// matches `try ... .write(` would false-trigger on a
    /// `print("retrying try foo.write(...)")` log line. Stripping
    /// string contents neutralises that false positive without
    /// affecting genuine code mentions of the token.
    ///
    /// This is the stricter cousin of ``strippingComments(_:)``;
    /// callers that need to grep for tokens that should match inside
    /// string interpolations (e.g. an asset key that the codebase
    /// hardcodes via a literal) should keep using
    /// ``strippingComments(_:)``.
    static func strippingCommentsAndStrings(_ source: String) -> String {
        var out = String()
        out.reserveCapacity(source.count)
        var i = source.startIndex
        var inLineComment = false
        var inBlockComment = false
        var inString = false
        let endIdx = source.endIndex

        while i < endIdx {
            let c = source[i]
            let next = source.index(after: i) < endIdx ? source[source.index(after: i)] : Character("\0")

            if inLineComment {
                if c == "\n" {
                    inLineComment = false
                    out.append(c) // preserve line breaks
                } else {
                    out.append(" ")
                }
                i = source.index(after: i)
                continue
            }
            if inBlockComment {
                if c == "*" && next == "/" {
                    inBlockComment = false
                    out.append("  ")
                    i = source.index(i, offsetBy: 2)
                    continue
                }
                if c == "\n" {
                    out.append(c)
                } else {
                    out.append(" ")
                }
                i = source.index(after: i)
                continue
            }
            if inString {
                // Preserve the closing quote so the boundary survives;
                // blank everything else inside the literal so no token
                // inside the string contents can match a regex.
                if c == "\\" && source.index(after: i) < endIdx {
                    let escaped = source[source.index(after: i)]
                    // Preserve newlines inside the literal so line
                    // numbers stay aligned, blank everything else.
                    out.append(" ")
                    out.append(escaped == "\n" ? "\n" : " ")
                    i = source.index(i, offsetBy: 2)
                    continue
                }
                if c == "\"" {
                    inString = false
                    out.append(c) // preserve closing quote
                } else if c == "\n" {
                    out.append(c) // preserve raw newlines
                } else {
                    out.append(" ")
                }
                i = source.index(after: i)
                continue
            }

            if c == "/" && next == "/" {
                inLineComment = true
                out.append("  ")
                i = source.index(i, offsetBy: 2)
                continue
            }
            if c == "/" && next == "*" {
                inBlockComment = true
                out.append("  ")
                i = source.index(i, offsetBy: 2)
                continue
            }
            if c == "\"" {
                inString = true
                out.append(c) // preserve opening quote
                i = source.index(after: i)
                continue
            }

            out.append(c)
            i = source.index(after: i)
        }
        return out
    }

    /// Returns the index of the first `{` at or after `position` that
    /// is not inside a string literal or comment. Returns `nil` if no
    /// such brace exists in the rest of the file.
    static func findOpenBrace(
        in source: String,
        after position: String.Index
    ) -> String.Index? {
        var i = position
        var inLineComment = false
        var inBlockComment = false
        var inString = false
        let endIdx = source.endIndex

        while i < endIdx {
            let c = source[i]
            let next = source.index(after: i) < endIdx ? source[source.index(after: i)] : Character("\0")

            if inLineComment {
                if c == "\n" { inLineComment = false }
                i = source.index(after: i); continue
            }
            if inBlockComment {
                if c == "*" && next == "/" {
                    inBlockComment = false
                    i = source.index(i, offsetBy: 2); continue
                }
                i = source.index(after: i); continue
            }
            if inString {
                if c == "\\" && source.index(after: i) < endIdx {
                    i = source.index(i, offsetBy: 2); continue
                }
                if c == "\"" { inString = false }
                i = source.index(after: i); continue
            }

            if c == "/" && next == "/" {
                inLineComment = true
                i = source.index(i, offsetBy: 2); continue
            }
            if c == "/" && next == "*" {
                inBlockComment = true
                i = source.index(i, offsetBy: 2); continue
            }
            if c == "\"" {
                inString = true
                i = source.index(after: i); continue
            }
            if c == "{" {
                return i
            }
            i = source.index(after: i)
        }
        return nil
    }

    /// Counts non-overlapping literal occurrences of `needle` in
    /// `haystack`. The match is byte-for-byte; whitespace is NOT
    /// normalised. For whitespace-tolerant matching use
    /// ``regexOccurrences(of:in:)`` with a `\s*`-flexible pattern.
    static func occurrences(of needle: String, in haystack: String) -> Int {
        var count = 0
        var searchRange = haystack.startIndex..<haystack.endIndex
        while let range = haystack.range(of: needle, range: searchRange) {
            count += 1
            searchRange = range.upperBound..<haystack.endIndex
        }
        return count
    }

    /// Counts non-overlapping regex matches of `pattern` in `haystack`.
    /// Uses `.dotMatchesLineSeparators` so a swift-format reflow that
    /// breaks the pattern across lines (e.g. a closure literal split
    /// onto multiple lines) still matches.
    static func regexOccurrences(of pattern: String, in haystack: String) -> Int {
        guard let regex = try? NSRegularExpression(
            pattern: pattern,
            options: [.dotMatchesLineSeparators]
        ) else {
            return 0
        }
        let range = NSRange(haystack.startIndex..<haystack.endIndex, in: haystack)
        return regex.numberOfMatches(in: haystack, options: [], range: range)
    }

    /// Locates the body of a function/initialiser by scanning for the
    /// first `{` that opens a balanced block at or after `signature`'s
    /// occurrence in `source`. Returns `nil` if the signature isn't
    /// found or the brace is unbalanced.
    ///
    /// The signature search is literal — callers that want to support
    /// multiple signature variants (e.g. one-line vs split-line)
    /// should pass each through ``firstBody(in:matchingAnyOf:)``.
    static func firstBody(in source: String, after signature: String) -> String? {
        guard let range = source.range(of: signature) else { return nil }
        guard let braceIdx = findOpenBrace(in: source, after: range.lowerBound) else {
            return nil
        }
        return bracedBody(in: source, startingAt: braceIdx)
    }

    /// Returns the concatenated bodies of EVERY init/function whose
    /// signature matches any of the supplied candidates. This matches
    /// the historical behaviour of
    /// `PlayheadRuntimeLoggerLazinessTests.assertInitBodyHasNoFileSystemCalls`,
    /// which audits both a façade-init AND an internal-state-init in
    /// a single pass (`SurfaceStatusInvariantLogger` + `LoggerState`).
    ///
    /// Returns `nil` if NONE of the signatures are found.
    static func combinedBodies(in source: String, matchingAnyOf signatures: [String]) -> String? {
        var combinedBody = ""
        var foundAny = false
        var searchStart = source.startIndex
        while searchStart < source.endIndex {
            // Find earliest occurrence of any signature on or after searchStart.
            var earliestRange: Range<String.Index>?
            for sig in signatures {
                if let r = source.range(of: sig, range: searchStart..<source.endIndex) {
                    if earliestRange == nil || r.lowerBound < earliestRange!.lowerBound {
                        earliestRange = r
                    }
                }
            }
            guard let r = earliestRange else { break }
            foundAny = true

            guard let openBraceIdx = findOpenBrace(in: source, after: r.lowerBound) else {
                searchStart = r.upperBound
                continue
            }
            let body = bracedBody(in: source, startingAt: openBraceIdx)
            let offset = source.distance(from: source.startIndex, to: r.lowerBound)
            combinedBody += "\n// === init body at offset \(offset) ===\n"
            combinedBody += body
            searchStart = r.upperBound
        }
        return foundAny ? combinedBody : nil
    }

    /// Resolves a path relative to the repository root by walking up
    /// from a `#filePath` value. The walk strips trailing path
    /// components until it finds a directory containing `Playhead.xcodeproj`.
    /// Returns nil if the project root cannot be located.
    ///
    /// Canaries should prefer this over hard-coded
    /// `.deletingLastPathComponent()` chains: the chain encodes the
    /// canary's location in the test tree, and silently breaks if the
    /// test is moved between folders.
    static func repositoryRoot(from filePath: String) -> URL? {
        var url = URL(fileURLWithPath: filePath).deletingLastPathComponent()
        while url.path != "/" {
            let projectMarker = url.appendingPathComponent("Playhead.xcodeproj")
            if FileManager.default.fileExists(atPath: projectMarker.path) {
                return url
            }
            url.deleteLastPathComponent()
        }
        return nil
    }

    /// Convenience: read the file at `relativePath` (from the repo
    /// root, located by walking up from `filePath`) into a string.
    /// Throws on I/O or charset failure; caller decides whether to
    /// `XCTFail` or otherwise surface the error.
    static func loadSource(
        repoRelativePath relativePath: String,
        from filePath: String = #filePath
    ) throws -> String {
        guard let root = repositoryRoot(from: filePath) else {
            throw NSError(
                domain: "SwiftSourceInspector",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "could not locate repo root from \(filePath)"]
            )
        }
        let url = root.appendingPathComponent(relativePath)
        return try String(contentsOf: url, encoding: .utf8)
    }
}
