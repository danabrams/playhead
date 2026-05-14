// ActivityViewFocusedEpisodeTests.swift
// Pure-projection tests for `ActivityView.applyFocus(snapshot:focusedEpisodeId:)`,
// the helper that powers the `focusedEpisodeId` sheet entry point added in
// playhead-3bv.4.
//
// The helper is a stateless filter: nil focus → identity; focus that
// matches → collapse to just the matching rows; focus with zero
// matches → fall back to the unfiltered snapshot (better than an empty
// sheet). These tests pin each branch.

import Foundation
import XCTest
@testable import Playhead

@MainActor
final class ActivityViewFocusedEpisodeTests: XCTestCase {

    // MARK: - Fixtures

    private func makeNow(_ id: String) -> ActivityNowRow {
        ActivityNowRow(
            episodeId: id,
            title: "Now-\(id)",
            podcastTitle: "Show",
            progressPhrase: "Analyzing",
            downloadFraction: nil,
            transcriptFraction: nil,
            analysisFraction: nil
        )
    }

    private func makeUpNext(_ id: String) -> ActivityUpNextRow {
        ActivityUpNextRow(
            episodeId: id,
            title: "Up-\(id)",
            podcastTitle: "Show",
            downloadFraction: nil,
            transcriptFraction: nil,
            analysisFraction: nil
        )
    }

    private func makePaused(_ id: String) -> ActivityPausedRow {
        ActivityPausedRow(
            episodeId: id,
            title: "Paused-\(id)",
            podcastTitle: "Show",
            reason: .phoneIsHot,
            hint: .wait,
            downloadFraction: nil,
            transcriptFraction: nil,
            analysisFraction: nil
        )
    }

    private func makeFinished(_ id: String) -> ActivityRecentlyFinishedRow {
        ActivityRecentlyFinishedRow(
            episodeId: id,
            title: "Finished-\(id)",
            podcastTitle: "Show",
            outcome: .success,
            finishedAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
    }

    private func makeSnapshot() -> ActivitySnapshot {
        ActivitySnapshot(
            now: [makeNow("ep-now-1"), makeNow("ep-now-2")],
            upNext: [makeUpNext("ep-up-1"), makeUpNext("ep-up-2")],
            paused: [makePaused("ep-paused-1")],
            recentlyFinished: [makeFinished("ep-finished-1")]
        )
    }

    // MARK: - Identity (no focus)

    /// `nil` focus must return the input snapshot unchanged. The most
    /// important branch — drift here would silently break the unscoped
    /// Activity tab.
    func testNilFocusReturnsIdentity() {
        let snap = makeSnapshot()
        let result = ActivityView.applyFocus(snapshot: snap, focusedEpisodeId: nil)
        XCTAssertEqual(result, snap)
    }

    // MARK: - Match in a single section

    /// Focus matching a single Now row collapses every section to only
    /// that row's matches. The other Now rows AND every Up Next / Paused /
    /// Finished row are filtered out.
    func testFocusOnNowRowCollapsesAllSections() {
        let snap = makeSnapshot()
        let result = ActivityView.applyFocus(
            snapshot: snap,
            focusedEpisodeId: "ep-now-1"
        )
        XCTAssertEqual(result.now.map(\.episodeId), ["ep-now-1"])
        XCTAssertTrue(result.upNext.isEmpty)
        XCTAssertTrue(result.paused.isEmpty)
        XCTAssertTrue(result.recentlyFinished.isEmpty)
    }

    /// Focus matching an Up Next row collapses identically — the focused
    /// episode can live in any section, and the sheet must surface
    /// whichever section's row it lands in.
    func testFocusOnUpNextRowCollapsesAllSections() {
        let snap = makeSnapshot()
        let result = ActivityView.applyFocus(
            snapshot: snap,
            focusedEpisodeId: "ep-up-2"
        )
        XCTAssertTrue(result.now.isEmpty)
        XCTAssertEqual(result.upNext.map(\.episodeId), ["ep-up-2"])
        XCTAssertTrue(result.paused.isEmpty)
        XCTAssertTrue(result.recentlyFinished.isEmpty)
    }

    /// Focus matching a Paused row.
    func testFocusOnPausedRowCollapsesAllSections() {
        let snap = makeSnapshot()
        let result = ActivityView.applyFocus(
            snapshot: snap,
            focusedEpisodeId: "ep-paused-1"
        )
        XCTAssertTrue(result.now.isEmpty)
        XCTAssertTrue(result.upNext.isEmpty)
        XCTAssertEqual(result.paused.map(\.episodeId), ["ep-paused-1"])
        XCTAssertTrue(result.recentlyFinished.isEmpty)
    }

    /// Focus matching a Recently Finished row.
    func testFocusOnFinishedRowCollapsesAllSections() {
        let snap = makeSnapshot()
        let result = ActivityView.applyFocus(
            snapshot: snap,
            focusedEpisodeId: "ep-finished-1"
        )
        XCTAssertTrue(result.now.isEmpty)
        XCTAssertTrue(result.upNext.isEmpty)
        XCTAssertTrue(result.paused.isEmpty)
        XCTAssertEqual(result.recentlyFinished.map(\.episodeId), ["ep-finished-1"])
    }

    // MARK: - Fallback (zero matches)

    /// Focus that matches nothing falls back to the unfiltered snapshot —
    /// presenting an empty sheet would be worse than showing the
    /// regular Activity layout. Author's documented choice
    /// (`ActivityView.swift` comment block on the property).
    func testFocusWithZeroMatchesFallsBackToUnfilteredSnapshot() {
        let snap = makeSnapshot()
        let result = ActivityView.applyFocus(
            snapshot: snap,
            focusedEpisodeId: "ep-not-in-any-section"
        )
        XCTAssertEqual(result, snap, "Zero-match focus must surface the full snapshot, not an empty one")
    }

    /// Empty snapshot + focus also falls back to identity (still empty).
    /// Edge case: nothing to filter, but the helper must not crash.
    func testEmptySnapshotFocusIsIdempotent() {
        let result = ActivityView.applyFocus(
            snapshot: .empty,
            focusedEpisodeId: "ep-anything"
        )
        XCTAssertEqual(result, .empty)
    }
}
