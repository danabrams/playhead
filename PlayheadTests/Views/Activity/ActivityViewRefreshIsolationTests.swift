// ActivityViewRefreshIsolationTests.swift
// Source-level canary: verifies the `.onReceive` handler for
// `ActivityRefreshNotification` in `ActivityView.swift` pins its
// spawned `Task` to `@MainActor`.
//
// Why source-inspection: `ActivityRefreshNotification` is posted from a
// background Task (e.g. AnalysisWorkScheduler). `.onReceive`'s closure
// executes on whatever thread the publisher delivered on, so a bare
// `Task { await refresh() }` inherits non-MainActor isolation. Inside,
// `refresh()` mutates observable view-model state — off-main — which
// trips Combine's "Publishing changes from background threads is not
// allowed" warning.
//
// The real bug is a compile-time isolation annotation. A runtime
// reproducer would be flaky (depends on which queue delivered the
// notification). A source-level assert that the `Task` carries
// `@MainActor` is the right invariant to lock in.

import Foundation
import Testing

// Intentionally no `@testable import Playhead` — this suite operates on
// source text only, to avoid coupling to any View internals.

@Suite("ActivityView refresh isolation (onReceive → Task @MainActor)")
struct ActivityViewRefreshIsolationTests {

    // MARK: - Helpers

    private static let repoRoot: URL = {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // .../Activity/
            .deletingLastPathComponent() // .../Views/
            .deletingLastPathComponent() // .../PlayheadTests/
            .deletingLastPathComponent() // .../<repo root>/
    }()

    private func read(_ relative: String) throws -> String {
        let url = Self.repoRoot.appendingPathComponent(relative)
        return try String(contentsOf: url, encoding: .utf8)
    }

    // MARK: - Invariant

    @Test("onReceive handler's Task is pinned to @MainActor")
    func refreshTaskIsMainActorIsolated() throws {
        let source = try read("Playhead/Views/Activity/ActivityView.swift")

        // The file MUST still subscribe to ActivityRefreshNotification;
        // if the wiring is removed, the refresh story is gone entirely.
        #expect(
            source.contains("ActivityRefreshNotification.name"),
            "ActivityView must still subscribe to ActivityRefreshNotification"
        )

        // Find the `.onReceive(... ActivityRefreshNotification.name ...)`
        // publisher call site and locate the Task inside its trailing
        // closure. A bare `Task { await refresh() }` is the bug; the fix
        // is `Task { @MainActor in await refresh() }`.
        guard
            let onReceiveRange = source.range(of: ".onReceive("),
            let notificationRange = source.range(
                of: "ActivityRefreshNotification.name",
                range: onReceiveRange.upperBound ..< source.endIndex
            )
        else {
            Issue.record("Expected .onReceive(...ActivityRefreshNotification.name...) in ActivityView")
            return
        }

        // Search after the notification name for the Task that drives
        // refresh(). The trailing closure lives just past the
        // publisher(for:) argument list.
        guard
            let refreshTaskRange = source.range(
                of: "Task",
                range: notificationRange.upperBound ..< source.endIndex
            )
        else {
            Issue.record("Expected a Task { ... await refresh() ... } inside the onReceive closure")
            return
        }

        // Look at a small window starting at `Task` — enough to see the
        // opening brace and any isolation annotation.
        let windowEnd = source.index(
            refreshTaskRange.lowerBound,
            offsetBy: 80,
            limitedBy: source.endIndex
        ) ?? source.endIndex
        let window = String(source[refreshTaskRange.lowerBound ..< windowEnd])

        // The Task opening must carry a `@MainActor in` isolation
        // annotation. Accept either `Task { @MainActor in` or
        // `Task{ @MainActor in` (no space) to stay robust to
        // auto-formatters.
        let hasMainActorAnnotation =
            window.contains("Task { @MainActor in")
            || window.contains("Task{ @MainActor in")
            || window.contains("Task {@MainActor in")

        #expect(
            hasMainActorAnnotation,
            """
            The Task spawned from the ActivityRefreshNotification \
            .onReceive handler must be pinned to @MainActor, e.g. \
            `Task { @MainActor in await refresh() }`. Without the \
            annotation, the Task inherits the notification publisher's \
            (background) isolation and viewModel mutations trip \
            Combine's "Publishing changes from background threads" \
            warning. Window seen: \(window)
            """
        )
    }
}
