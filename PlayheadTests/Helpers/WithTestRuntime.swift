// WithTestRuntime.swift
// C7 (cycle 2): scoped helper that constructs a `PlayheadRuntime`, runs a
// caller-supplied closure against it, and guarantees `runtime.shutdown()`
// runs on the way out — even if the closure throws.
//
// The helper exists to enforce a single rule: every test that constructs a
// `PlayheadRuntime` MUST shut it down before returning. The previous
// pattern of `let runtime = PlayheadRuntime(...)` followed by no teardown
// leaked the shadow-retry observer's task across test boundaries (the
// `deinit` used to spawn an unbounded `Task { await observer.stop() }` to
// chase the cleanup, which raced with subsequent constructions).
//
// Usage:
//
//     try await withTestRuntime { runtime in
//         // exercise runtime here
//     }
//
// The helper is `@MainActor` because `PlayheadRuntime` is `@MainActor`.

import Foundation
@testable import Playhead

@MainActor
func withTestRuntime<T>(
    isPreviewRuntime: Bool = true,
    _ body: (PlayheadRuntime) async throws -> T
) async rethrows -> T {
    let runtime = PlayheadRuntime(isPreviewRuntime: isPreviewRuntime)
    do {
        let result = try await body(runtime)
        await runtime.shutdown()
        return result
    } catch {
        await runtime.shutdown()
        throw error
    }
}
