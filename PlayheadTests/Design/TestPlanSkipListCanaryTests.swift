// TestPlanSkipListCanaryTests.swift
// playhead-wwbr: make a test-plan entry that CANNOT work fail at test time.
//
// ## The defect this exists to prevent
//
// An `xctestplan` filter honours XCTest **class names** and silently ignores
// Swift Testing identifiers — no error, no warning, no log line. The entry is
// accepted by the file format and does nothing.
//
// That is not hypothetical. On 2026-04-10 (d55b0c1c) `PlayheadFastTests`
// declared 21 suites skipped; 20 of them were Swift Testing `@Suite` structs,
// so exactly ONE took effect. By 2026-08-13 the list had grown to 32 entries
// of which 12 worked, and **163 tests ran in every fast gate while the plan
// said they did not**. One of them — `Phase3ShadowReplayHarnessTests`'s
// `benchmarkGate` — FAILED every one of those runs for four months and was
// triaged each time as one more starvation flake, because nobody looks at a
// test the plan says is not running (playhead-o89d, playhead-wwbr).
//
// The audit that found it cost a day. This canary costs ~30 ms and names the
// file, the entry and the remedy.
//
// ## Why XCTest-shaped
//
// A canary about `skippedTests` must itself be filterable through
// `skippedTests`. The repo's other source canaries are XCTest for the same
// reason — see `SwiftSourceInspector`'s header.

import Foundation
import XCTest

final class TestPlanSkipListCanaryTests: XCTestCase {

    // MARK: - Canary 1: every filter entry must be able to take effect

    /// Every name in every plan's `skippedTests` / `selectedTests` must resolve
    /// to a class that reaches `XCTestCase`. Anything else — a Swift Testing
    /// `@Suite struct`, a renamed class, a typo — is a declaration the runner
    /// will silently ignore, which is strictly worse than no declaration at
    /// all: it reads as coverage policy and is fiction.
    func testEveryPlanFilterEntryResolvesToAnXCTestClass() throws {
        let root = try Self.repoRoot()
        let index = try Self.declarationIndex(underTestTreeAt: root)
        let plans = try Self.testPlans(at: root)

        XCTAssertFalse(plans.isEmpty, "No .xctestplan files found under TestPlans/")

        // Anti-vacuity. A source walk that finds nothing reports every entry
        // as "resolves to no type" — or, if the loop is ever inverted, reports
        // nothing at all and passes. Both halves of the resolver are pinned
        // against types this file is guaranteed to contain: itself (a class
        // that reaches XCTestCase through no intermediate) and the enum next
        // to it in the test tree.
        XCTAssertEqual(index["TestPlanSkipListCanaryTests"]?.kind, "class",
                       "declaration walk did not find this very file — the walk is broken, not the plans")
        XCTAssertTrue(Self.reachesXCTestCase("TestPlanSkipListCanaryTests", in: index),
                      "XCTestCase ancestry resolution is broken — it cannot see a direct subclass")
        XCTAssertEqual(index["PerfGate"]?.kind, "enum",
                       "declaration walk does not distinguish kinds — a struct would pass as a class")

        var violations: [String] = []
        for plan in plans {
            for entry in try Self.filterEntries(inPlanAt: plan) {
                // `Suite/method()` is a legal entry shape; the type name is the
                // part the filter has to resolve.
                let typeName = String(entry.name.split(separator: "/").first ?? "")
                guard let decl = index[typeName] else {
                    violations.append(
                        "\(plan.lastPathComponent): \(entry.list) entry '\(entry.name)' resolves "
                        + "to no type under PlayheadTests/ — a renamed or deleted class filters "
                        + "nothing, silently."
                    )
                    continue
                }
                guard decl.kind == "class" else {
                    violations.append(
                        "\(plan.lastPathComponent): \(entry.list) entry '\(entry.name)' is a "
                        + "\(decl.kind) in \(decl.file). xctestplan filters SILENTLY IGNORE Swift "
                        + "Testing identifiers, so this entry does nothing and the tests run. "
                        + "Gate it in SOURCE instead — an env-var trait like "
                        + "PlayheadTests/Helpers/PerfGate.swift "
                        + "(.enabled(if:) on the @Test or @Suite) works for both frameworks. "
                        + "See playhead-wwbr."
                    )
                    continue
                }
                if !Self.reachesXCTestCase(typeName, in: index) {
                    violations.append(
                        "\(plan.lastPathComponent): \(entry.list) entry '\(entry.name)' is a class "
                        + "in \(decl.file) that does not inherit from XCTestCase "
                        + "(declared: \(decl.inherits.joined(separator: ", "))) — the filter cannot "
                        + "match it."
                    )
                }
            }
        }

        XCTAssertTrue(
            violations.isEmpty,
            "Test plan filter entries that cannot take effect:\n"
            + violations.joined(separator: "\n")
        )
    }

    // MARK: - Canary 2: the integration plan really is a superset

    /// CLAUDE.md calls `PlayheadIntegrationTests` a "true superset of
    /// FastTests". Nothing checked it. A skip added to the superset plan would
    /// leave the affected tests running NOWHERE while both plan files still
    /// looked reasonable in isolation — the same class of silent coverage loss
    /// this file exists for, one level up.
    func testIntegrationPlanSkipsNothingTheFastPlanRuns() throws {
        let root = try Self.repoRoot()
        let fast = try Self.filterEntries(
            inPlanAt: Self.planURL(root, "PlayheadFastTests")
        )
        let integration = try Self.filterEntries(
            inPlanAt: Self.planURL(root, "PlayheadIntegrationTests")
        )

        let fastSkips = Set(fast.filter { $0.list == "skippedTests" }.map(\.name))
        let integrationSkips = Set(integration.filter { $0.list == "skippedTests" }.map(\.name))

        let extra = integrationSkips.subtracting(fastSkips).sorted()
        XCTAssertTrue(
            extra.isEmpty,
            "PlayheadIntegrationTests skips \(extra), which PlayheadFastTests runs. The "
            + "integration plan is documented as a true superset; a skip that exists only "
            + "there means those tests run in no plan at all."
        )
    }

    // MARK: - Plan parsing

    private struct FilterEntry {
        let list: String
        let name: String
    }

    private static func planURL(_ root: URL, _ name: String) -> URL {
        root
            .appendingPathComponent("TestPlans", isDirectory: true)
            .appendingPathComponent("\(name).xctestplan")
    }

    private static func testPlans(at root: URL) throws -> [URL] {
        let dir = root.appendingPathComponent("TestPlans", isDirectory: true)
        let contents = try FileManager.default.contentsOfDirectory(
            at: dir, includingPropertiesForKeys: nil
        )
        return contents.filter { $0.pathExtension == "xctestplan" }.sorted { $0.path < $1.path }
    }

    private static func json(at url: URL) throws -> [String: Any] {
        let data = try Data(contentsOf: url)
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw NSError(
                domain: "TestPlanSkipListCanaryTests", code: 1,
                userInfo: [NSLocalizedDescriptionKey: "\(url.lastPathComponent) is not a JSON object"]
            )
        }
        return object
    }

    private static func filterEntries(inPlanAt url: URL) throws -> [FilterEntry] {
        let object = try json(at: url)
        let targets = object["testTargets"] as? [[String: Any]] ?? []
        var entries: [FilterEntry] = []
        for target in targets {
            for list in ["skippedTests", "selectedTests"] {
                for name in (target[list] as? [String] ?? []) {
                    entries.append(FilterEntry(list: list, name: name))
                }
            }
        }
        return entries
    }

    // MARK: - Declaration index

    private struct Declaration {
        let kind: String
        let inherits: [String]
        let file: String
    }

    /// `class|struct|actor|enum Name: A, B {`. Comments are stripped first, so
    /// a prose mention of a class in a header cannot register as a declaration.
    private static let declPattern = try? NSRegularExpression(
        pattern: #"\b(class|struct|actor|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?::\s*([^{\n]*?))?\s*\{"#
    )

    private static func declarationIndex(underTestTreeAt root: URL) throws -> [String: Declaration] {
        let testRoot = root.appendingPathComponent("PlayheadTests", isDirectory: true)
        guard let regex = declPattern else { return [:] }
        var index: [String: Declaration] = [:]

        let enumerator = FileManager.default.enumerator(
            at: testRoot, includingPropertiesForKeys: nil
        )
        while let url = enumerator?.nextObject() as? URL {
            guard url.pathExtension == "swift" else { continue }
            guard let raw = try? String(contentsOf: url, encoding: .utf8) else { continue }
            let source = SwiftSourceInspector.strippingComments(raw)
            let range = NSRange(source.startIndex..., in: source)
            for match in regex.matches(in: source, range: range) {
                guard let kindRange = Range(match.range(at: 1), in: source),
                      let nameRange = Range(match.range(at: 2), in: source) else { continue }
                let name = String(source[nameRange])
                // First declaration wins; a nested type sharing a short name
                // must not shadow a top-level suite.
                guard index[name] == nil else { continue }
                var inherits: [String] = []
                if let inheritRange = Range(match.range(at: 3), in: source) {
                    inherits = String(source[inheritRange])
                        .split(separator: ",")
                        .map { part in
                            // Drop generic arguments and whitespace: `Base<T>` -> `Base`.
                            String(part.split(separator: "<").first ?? part)
                                .trimmingCharacters(in: .whitespacesAndNewlines)
                        }
                        .filter { !$0.isEmpty }
                }
                index[name] = Declaration(
                    kind: String(source[kindRange]),
                    inherits: inherits,
                    file: url.lastPathComponent
                )
            }
        }
        return index
    }

    /// XCTest subclassing is transitive: `BackgroundingCycleTests` inherits
    /// from `InterruptionCycleSuiteBase`, which is the `XCTestCase`. A check
    /// that looked only one level up would report eight working entries broken.
    private static func reachesXCTestCase(
        _ name: String,
        in index: [String: Declaration],
        depth: Int = 0
    ) -> Bool {
        guard depth < 16, let decl = index[name] else { return false }
        if decl.inherits.contains("XCTestCase") { return true }
        return decl.inherits.contains { reachesXCTestCase($0, in: index, depth: depth + 1) }
    }

    private static func repoRoot(file: StaticString = #filePath) throws -> URL {
        guard let root = SwiftSourceInspector.repositoryRoot(from: String(describing: file)) else {
            throw NSError(
                domain: "TestPlanSkipListCanaryTests", code: 2,
                userInfo: [NSLocalizedDescriptionKey: "Repository root not found from \(file)"]
            )
        }
        return root
    }
}
