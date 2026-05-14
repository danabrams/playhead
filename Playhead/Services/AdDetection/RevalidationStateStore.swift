// RevalidationStateStore.swift
// playhead-zx6i — B4 fast revalidation from persisted features.
//
// Per-asset, UserDefaults-backed snapshot of the `PipelineVersions`
// that ran the LAST successful `AdDetectionService.runBackfill`
// against this asset. Acts as the producer/consumer pair for the B4
// short-circuit:
//
//   producer — end of a successful `runBackfill`, stamps
//     `PipelineVersions.current()` for the asset.
//   consumer — top of `AnalysisJobRunner.run`, compares the persisted
//     stamp against `PipelineVersions.current()`. If they differ and
//     the asset has persisted chunks, take the revalidation path.
//
// Persistence model mirrors `LightweightInventoryChecksSettings` (xr3t)
// and `PreAnalysisConfig` (24cm / 2hpn) — a namespaced UserDefaults
// key, JSON-encoded body. Per-asset state (one entry per asset id) is
// stored under a per-asset key suffix so the read path stays O(1) and
// we don't have to hydrate a cross-asset dictionary on every call.
//
// Absence semantics: a `loadCompletedVersions` returning `nil` means
// "this asset has never been stamped" — either pre-zx6i (the stamp
// didn't exist) or a fresh install. Callers must treat that as "do
// not take the short-circuit" — we have no baseline to compare
// against, so a full analysis pass is required to establish one.

import Foundation

/// Per-asset UserDefaults persistence for the
/// `b4_revalidation_from_features` state. Stateless type — every
/// method is `static` and takes the asset id + an optional `UserDefaults`
/// override (tests pass an isolated suite to avoid polluting standard
/// defaults).
enum RevalidationStateStore {

    /// Namespace for every key this store reads or writes. Kept as a
    /// single constant so a future move to SwiftData is a one-line
    /// change (and so the migration check has a single grep target).
    static let keyPrefix = "playhead.zx6i.completedVersions."

    /// Build the per-asset UserDefaults key. Exposed for tests that
    /// want to assert the key shape; production callers should use
    /// `loadCompletedVersions` / `recordCompleted` rather than poking
    /// the defaults directly.
    static func key(forAsset assetId: String) -> String {
        keyPrefix + assetId
    }

    /// Load the `PipelineVersions` stamped at the end of the last
    /// successful `runBackfill` for `assetId`. Returns `nil` when no
    /// stamp exists (the absence-semantics case described in the file
    /// header) OR when the stored JSON fails to decode against the
    /// current `PipelineVersions` shape — a decode failure is treated
    /// as "absent" rather than thrown because the producer-side
    /// guarantee is "encode round-trip"; a decode failure means the
    /// shape changed under us and the safe thing to do is to behave
    /// as if no stamp exists (and let the next successful `runBackfill`
    /// re-stamp the new shape).
    static func loadCompletedVersions(
        forAsset assetId: String,
        defaults: UserDefaults = .standard
    ) -> PipelineVersions? {
        guard let data = defaults.data(forKey: key(forAsset: assetId)) else {
            return nil
        }
        return try? JSONDecoder().decode(PipelineVersions.self, from: data)
    }

    /// Stamp `versions` as the most-recent successful run for
    /// `assetId`. Idempotent: stamping the same value twice is a
    /// no-op semantically. Encoding errors are silently ignored
    /// (`try?`) — they are unrecoverable here (a struct of 3 fields
    /// cannot fail to JSON-encode in practice) and the consumer's
    /// safe fallback is the "no stamp" branch anyway. A failed stamp
    /// just means the next analysis pass will re-do the work, which
    /// is the correct fail-open behavior for a perf-only feature.
    static func recordCompleted(
        versions: PipelineVersions,
        forAsset assetId: String,
        defaults: UserDefaults = .standard
    ) {
        guard let data = try? JSONEncoder().encode(versions) else { return }
        defaults.set(data, forKey: key(forAsset: assetId))
    }

    /// Remove the stamp for `assetId`. Used by tests that want to
    /// simulate a fresh-install / pre-zx6i state without flushing the
    /// entire UserDefaults domain. Production has no caller today —
    /// the producer always overwrites with a fresh stamp on
    /// completion rather than clearing.
    static func clear(
        forAsset assetId: String,
        defaults: UserDefaults = .standard
    ) {
        defaults.removeObject(forKey: key(forAsset: assetId))
    }
}
