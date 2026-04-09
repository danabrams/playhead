// RegionShadowObserver.swift
// playhead-xba (Phase 4 shadow wire-up):
//
// Observation-only sink for the Phase 4 region pipeline
// (`RegionProposalBuilder` → `RegionFeatureExtractor`). The production
// `AdDetectionService.runBackfill` path writes `RegionFeatureBundle`s here
// after the live backfill completes and after the existing Phase 3 FM shadow
// phase runs. Nothing downstream reads from this observer — it exists so
// DEBUG builds (and integration tests) can inspect the Phase 4 output without
// any risk of affecting live decision logic.
//
// Contract:
//   • Compiled in all configurations. The shadow phase only runs when an
//     observer is injected; PlayheadRuntime constructs the observer behind
//     `#if DEBUG`, so production builds never reach this code. This mirrors
//     the injection pattern used by `FoundationModelsFeedbackStore` in
//     `PlayheadRuntime` — production release builds never construct one, so
//     no sandbox/persistence footprint.
//   • Writes are per-asset. Repeated writes for the same asset overwrite the
//     previous bundle set (backfill re-runs are allowed to refresh the
//     snapshot; tests inspect the latest).
//   • Reads return the most recent bundles for an asset, or nil if nothing
//     has been recorded yet.
//   • The observer is an actor because backfill runs on an arbitrary task
//     executor and tests assert from the main actor; a lock-free snapshot
//     across concurrency domains is exactly what actors give us.
//
// This type is deliberately tiny. Anything more ambitious — persistence,
// fanout, metrics — belongs in later Phase 4 follow-ups once the runner
// surfaces `FMRefinementWindowOutput` through `RunResult` and FM-origin
// region clustering can be exercised end-to-end. See playhead-xba.

import Foundation
import OSLog

actor RegionShadowObserver {

    private let logger = Logger(
        subsystem: "com.playhead",
        category: "RegionShadowObserver"
    )

    private var latest: [String: [RegionFeatureBundle]] = [:]
    private var recordCounts: [String: Int] = [:]

    init() {}

    /// Record the latest Phase 4 region-feature bundles for an asset.
    ///
    /// Callers should pass the complete set of bundles produced by a single
    /// `RegionFeatureExtractor.extract(...)` invocation. An empty array is a
    /// legitimate outcome (no regions proposed) and is recorded as such so
    /// tests can distinguish "pipeline ran, produced nothing" from "pipeline
    /// never ran".
    func record(assetId: String, bundles: [RegionFeatureBundle]) {
        latest[assetId] = bundles
        recordCounts[assetId, default: 0] += 1
        logger.debug(
            "Recorded \(bundles.count, privacy: .public) region bundles for asset \(assetId, privacy: .public)"
        )
    }

    /// Most recently recorded bundles for an asset, or nil if none recorded.
    func latestBundles(for assetId: String) -> [RegionFeatureBundle]? {
        latest[assetId]
    }

    /// Number of times `record` has been called for an asset. Useful for
    /// tests that want to verify the shadow path actually executed.
    func recordCount(for assetId: String) -> Int {
        recordCounts[assetId, default: 0]
    }

    /// Total number of assets that have ever had bundles recorded.
    func recordedAssetCount() -> Int {
        latest.count
    }
}
