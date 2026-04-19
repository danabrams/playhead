// EpisodeSurfaceStatusSnapshotTests.swift
// Renders the `EpisodeSurfaceStatusMatrix` cartesian product to JSON
// and compares against a committed golden fixture. Any change to the
// reducer's output for an existing row (or any change to the row set
// itself) will trip the diff, forcing a deliberate fixture update.
//
// To regenerate the golden fixture after an intentional reducer change,
// delete the fixture file from disk (`PlayheadTests/Services/SurfaceStatus/
// GoldenFixtures/episode-surface-status.json`) and re-run the test —
// the first run with no fixture on disk seeds a fresh one (and fails
// the test so the reviewer sees a diff in `git status`). The file-
// absence seed path is chosen over an env-var flag because xcodebuild
// does not forward environment variables to test processes by default,
// which made the env-var approach silently no-op in CI.

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
            Golden fixture mismatch. To regenerate: delete the fixture at \
            \(url.path) and re-run — the seed-on-absence path will write a fresh \
            file and fail; review and commit the result.
            ---EXPECTED (golden)---
            \(goldenHead)
            ---ACTUAL (rendered)---
            \(renderedHead)
            """
            Issue.record(Comment(rawValue: msg))
        }
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
