// EpisodeSurfaceStatusSnapshotTests.swift
// Renders the `EpisodeSurfaceStatusMatrix` cartesian product to JSON
// and compares against a committed golden fixture. Any change to the
// reducer's output for an existing row (or any change to the row set
// itself) will trip the diff, forcing a deliberate fixture update.
//
// Regeneration flow
// -----------------
// The golden fixture is ALSO listed as a bundled test resource in the
// PlayheadTests target (see `project.pbxproj` — the file ships into
// the test bundle so other suites that prefer bundle-loading over a
// `#filePath` lookup can also consume it). Consequently, the naive
// "delete the fixture and re-run" flow does NOT work: the build phase
// requires the resource file to exist on disk, so deletion fails the
// build before the seed-on-absence code path ever executes.
//
// To regenerate, overwrite the committed fixture with the 3-byte
// placeholder `{}\n` and re-run the test suite. The
// `rewritePlaceholderGoldenFixture` test (below) detects the
// placeholder and writes a fresh render over it; the subsequent
// `goldenMatches` run (in the same suite invocation) then compares
// against the freshly-written file and passes. Inspect `git diff` and
// commit the result. The 3-byte placeholder check is strict so the
// regen test cannot silently trample a real committed baseline.
//
// The legacy seed-on-absence path in `goldenMatches` is kept as a
// belt-and-braces safeguard for a fresh clone or a test-resource
// unlisting in the future, but it is not the primary path.

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeSurfaceStatusReducer — golden snapshot matrix (playhead-5bb3)")
struct EpisodeSurfaceStatusSnapshotTests {

    /// Path to the golden JSON on disk, anchored at `#filePath` so the
    /// fixture ships with the source tree rather than via the test
    /// bundle (which would require wiring it into the PlayheadTests
    /// target's resources).
    static func goldenURL(file: StaticString = #filePath) -> URL {
        URL(fileURLWithPath: String(describing: file))
            .deletingLastPathComponent() // PlayheadTests/Services/SurfaceStatus
            .appendingPathComponent("GoldenFixtures", isDirectory: true)
            .appendingPathComponent("episode-surface-status.json")
    }

    // MARK: - Render

    /// Render the full matrix to a deterministic JSON document. Each
    /// row is keyed by its `label` so diffs are minimal when a single
    /// row's output changes.
    static func renderMatrixJSON() throws -> Data {
        var rendered: [String: EpisodeSurfaceStatus] = [:]
        for row in EpisodeSurfaceStatusMatrix.rows() {
            rendered[row.label] = episodeSurfaceStatus(
                state: row.state,
                cause: row.cause,
                eligibility: row.eligibility,
                coverage: row.coverage,
                readinessAnchor: row.readinessAnchor
            )
        }
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return try encoder.encode(rendered)
    }

    // MARK: - Golden comparison

    @Test("Golden JSON fixture matches the rendered matrix")
    func goldenMatches() throws {
        let rendered = try Self.renderMatrixJSON()
        let url = Self.goldenURL()

        // Seed-on-absence: if the fixture file does not yet exist, write
        // it and fail the test so the reviewer sees the new file in
        // `git status` and has to deliberately commit it. This avoids a
        // silent pass that could freeze an incorrect baseline.
        if !FileManager.default.fileExists(atPath: url.path) {
            try FileManager.default.createDirectory(
                at: url.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try rendered.write(to: url)
            let msg = "Seeded missing golden fixture at \(url.path). Inspect the written file and commit it — this test will pass on the next run."
            Issue.record(Comment(rawValue: msg))
            return
        }

        let golden = try Data(contentsOf: url)
        if golden != rendered {
            // Emit a human-readable diff summary — full diffs are easier
            // to inspect by running the regen flag and diffing the
            // working tree.
            let renderedStr = String(decoding: rendered, as: UTF8.self)
            let goldenStr = String(decoding: golden, as: UTF8.self)
            let goldenHead = String(goldenStr.prefix(2000))
            let renderedHead = String(renderedStr.prefix(2000))
            let msg = """
            Golden fixture mismatch. To regenerate: overwrite \(url.path) \
            with the 3-byte placeholder `{}\\n` (e.g. `printf '{}\\n' > \(url.path)`) \
            and re-run the snapshot suite. The rewritePlaceholderGoldenFixture \
            test will rewrite the placeholder with the current render; then \
            review `git diff` and commit the result.
            ---EXPECTED (golden)---
            \(goldenHead)
            ---ACTUAL (rendered)---
            \(renderedHead)
            """
            Issue.record(Comment(rawValue: msg))
        }
    }

    /// Regeneration entry point — rewrites the golden fixture with a
    /// fresh render whenever the committed file is a 3-byte placeholder
    /// (`{}\n`). This is the canonical "unfreeze the baseline" flow: the
    /// fixture is bundled as a test resource, so deleting it outright
    /// fails the build (see file header). Overwriting with the
    /// placeholder keeps the resource file present for the build phase
    /// while signalling to this test that the committed baseline has
    /// been intentionally invalidated.
    ///
    /// The placeholder detection is strict — the file must be exactly
    /// `{}\n` (3 bytes). Any other content is presumed to be a real
    /// baseline the reviewer intends to diff against; this test is a
    /// no-op in that case.
    ///
    /// Ordering note: this test runs unordered relative to
    /// `goldenMatches` (Swift Testing does not guarantee test ordering
    /// within a suite). That is fine — either `goldenMatches` runs
    /// first on the placeholder (it fails with a "mismatch" diff then
    /// this test overwrites, and the next `swift test` invocation
    /// passes), or this test runs first (the placeholder is rewritten
    /// in-place and `goldenMatches` then reads the fresh data). Both
    /// orders converge to the same committed-fixture state after a
    /// second invocation.
    @Test("Rewrite placeholder golden fixture ({} → full matrix)")
    func rewritePlaceholderGoldenFixture() throws {
        let url = Self.goldenURL()
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        let existing = try Data(contentsOf: url)
        let placeholder = Data("{}\n".utf8)
        guard existing == placeholder else {
            return
        }
        let rendered = try Self.renderMatrixJSON()
        try rendered.write(to: url)
        Issue.record(Comment(rawValue: "Rewrote placeholder fixture at \(url.path); inspect the diff and commit if correct."))
    }

    // MARK: - Matrix invariants
    //
    // These catch bugs in the matrix generator itself rather than in
    // the reducer — a regression in the matrix would make the contract
    // coverage test meaningless.

    @Test("Matrix rows have unique labels")
    func matrixLabelsAreUnique() {
        let rows = EpisodeSurfaceStatusMatrix.rows()
        let unique = Set(rows.map(\.label))
        #expect(unique.count == rows.count, "Matrix row labels must be unique")
    }

    @Test("Matrix row count matches the expected cartesian product")
    func matrixRowCount() {
        // 2 eligibilities × 10 causes × 2 states × 2 coverages × 2 anchors
        let rows = EpisodeSurfaceStatusMatrix.rows()
        #expect(rows.count == 2 * 10 * 2 * 2 * 2)
    }
}
