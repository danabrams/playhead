// TestDisplayNameUniquenessCanaryTests.swift
// playhead-0dsti: every Swift Testing @Test display name in the tree is unique.
//
// Two shipped instruments key on the display name and on NOTHING else:
// `scripts/gate_baseline.py`'s crashed-host census (same-named tests share a
// key, so one test's verdict answers for another's, in both directions) and
// `scripts/mutation-battery.sh`'s scoring (a mutant predicted to kill A is
// credited by B failing — a FALSE KILL, silent, looks exactly like success).
// Measured at 64078664: 56 names in more than one file, 9 more repeated within
// a file, overwhelmingly NEIGHBOURING suites — a V62 migration suite and a V63
// one, the ledger and its successor — exactly the suites a bead runs together.
//
// The canary walks every `@Test("…")` literal under PlayheadTests/ (single-line
// and triple-quoted) and fails on any name that occurs twice anywhere, naming
// the files. Renaming is cheap; a false kill is not.

import Foundation
import Testing

@Suite("every @Test display name is unique across the tree (playhead-0dsti)")
struct TestDisplayNameUniquenessCanaryTests {
    private static var testsRoot: URL {
        // …/PlayheadTests/Design/<this file> → …/PlayheadTests
        URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
    }

    /// Every `@Test("name"` and `@Test("""…"""` display name, with its file.
    static func displayNames(under root: URL) throws -> [(name: String, file: String)] {
        let singleLine = try NSRegularExpression(pattern: #"@Test\(\s*"((?:[^"\\]|\\.)+)""#)
        let tripleQuoted = try NSRegularExpression(pattern: #"@Test\(\s*"""\s*\n([\s\S]*?)\n\s*""""#)
        var found: [(String, String)] = []
        let enumerator = FileManager.default.enumerator(at: root, includingPropertiesForKeys: nil)
        while let url = enumerator?.nextObject() as? URL {
            guard url.pathExtension == "swift" else { continue }
            let source = try String(contentsOf: url, encoding: .utf8)
            let range = NSRange(source.startIndex..., in: source)
            let rel = url.path.replacingOccurrences(of: root.path + "/", with: "")
            for m in singleLine.matches(in: source, range: range) {
                guard let r = Range(m.range(at: 1), in: source) else { continue }
                found.append((String(source[r]), rel))
            }
            for m in tripleQuoted.matches(in: source, range: range) {
                guard let r = Range(m.range(at: 1), in: source) else { continue }
                let name = source[r].split(separator: "\n").map { $0.trimmingCharacters(in: .whitespaces) }.joined(separator: " ")
                found.append((name, rel))
            }
        }
        return found.map { (name: $0.0, file: $0.1) }
    }

    @Test("no @Test display name occurs twice, across files or within one")
    func displayNamesAreUnique() throws {
        let names = try Self.displayNames(under: Self.testsRoot)
        #expect(names.count > 10_000, "the walk found \(names.count) names — the root is wrong if this is small")
        var byName: [String: [String]] = [:]
        for entry in names { byName[entry.name, default: []].append(entry.file) }
        let duplicates = byName.filter { $0.value.count > 1 }.sorted { $0.key < $1.key }
        #expect(duplicates.isEmpty, """
            \(duplicates.count) display name(s) occur more than once — rename, do not share:
            \(duplicates.prefix(20).map { "  \"\($0.key)\" in \($0.value.joined(separator: ", "))" }.joined(separator: "\n"))
            """)
    }

    @Test("the walker reads triple-quoted display names, so a collision cannot hide in one")
    func walkerReadsTripleQuotedNames() throws {
        let names = try Self.displayNames(under: Self.testsRoot)
        let multiLine = names.filter { $0.file.hasSuffix("DayZeroDownloadTimeStoreTests.swift") }
        #expect(multiLine.count >= 2, "DayZeroDownloadTimeStoreTests writes its names triple-quoted; the walker saw \(multiLine.count)")
    }
}
