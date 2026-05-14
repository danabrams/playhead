// RevalidationStateStoreTests.swift
// playhead-zx6i — Unit tests for the per-asset UserDefaults-backed
// "last completed PipelineVersions" snapshot that drives the B4
// short-circuit decision.

import Foundation
import Testing
@testable import Playhead

@Suite("RevalidationStateStore")
struct RevalidationStateStoreTests {

    /// Build an isolated `UserDefaults` suite so concurrent test runs
    /// cannot pollute each other through `.standard`. Each test gets
    /// a fresh suite scoped to its own UUID so cleanup is trivial.
    private func makeIsolatedDefaults() -> UserDefaults {
        let suiteName = "RevalidationStateStoreTests.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)
        return defaults
    }

    @Test("loadCompletedVersions returns nil when no stamp exists")
    func loadReturnsNilWhenAbsent() {
        let defaults = makeIsolatedDefaults()
        let result = RevalidationStateStore.loadCompletedVersions(
            forAsset: "asset-1",
            defaults: defaults
        )
        #expect(result == nil)
    }

    @Test("recordCompleted then load round-trips")
    func recordThenLoadRoundTrips() {
        let defaults = makeIsolatedDefaults()
        let versions = PipelineVersions(
            modelVersion: "detection-v1",
            policyVersion: "skip-policy-v1",
            featureSchemaVersion: 1
        )
        RevalidationStateStore.recordCompleted(
            versions: versions,
            forAsset: "asset-1",
            defaults: defaults
        )
        let loaded = RevalidationStateStore.loadCompletedVersions(
            forAsset: "asset-1",
            defaults: defaults
        )
        #expect(loaded == versions)
    }

    @Test("per-asset isolation: writes against different assets do not collide")
    func perAssetIsolation() {
        let defaults = makeIsolatedDefaults()
        let vA = PipelineVersions(modelVersion: "mA", policyVersion: "pA", featureSchemaVersion: 1)
        let vB = PipelineVersions(modelVersion: "mB", policyVersion: "pB", featureSchemaVersion: 2)
        RevalidationStateStore.recordCompleted(versions: vA, forAsset: "asset-A", defaults: defaults)
        RevalidationStateStore.recordCompleted(versions: vB, forAsset: "asset-B", defaults: defaults)

        #expect(RevalidationStateStore.loadCompletedVersions(forAsset: "asset-A", defaults: defaults) == vA)
        #expect(RevalidationStateStore.loadCompletedVersions(forAsset: "asset-B", defaults: defaults) == vB)
    }

    @Test("recordCompleted is idempotent on repeated stamps with the same value")
    func recordIsIdempotent() {
        let defaults = makeIsolatedDefaults()
        let versions = PipelineVersions.current()
        RevalidationStateStore.recordCompleted(versions: versions, forAsset: "asset-1", defaults: defaults)
        RevalidationStateStore.recordCompleted(versions: versions, forAsset: "asset-1", defaults: defaults)
        RevalidationStateStore.recordCompleted(versions: versions, forAsset: "asset-1", defaults: defaults)
        #expect(RevalidationStateStore.loadCompletedVersions(forAsset: "asset-1", defaults: defaults) == versions)
    }

    @Test("recordCompleted overwrites a prior stamp")
    func recordOverwrites() {
        let defaults = makeIsolatedDefaults()
        let v1 = PipelineVersions(modelVersion: "m1", policyVersion: "p1", featureSchemaVersion: 1)
        let v2 = PipelineVersions(modelVersion: "m2", policyVersion: "p2", featureSchemaVersion: 2)
        RevalidationStateStore.recordCompleted(versions: v1, forAsset: "asset-1", defaults: defaults)
        RevalidationStateStore.recordCompleted(versions: v2, forAsset: "asset-1", defaults: defaults)
        #expect(RevalidationStateStore.loadCompletedVersions(forAsset: "asset-1", defaults: defaults) == v2)
    }

    @Test("clear removes a prior stamp and load returns nil")
    func clearRemovesStamp() {
        let defaults = makeIsolatedDefaults()
        let versions = PipelineVersions.current()
        RevalidationStateStore.recordCompleted(versions: versions, forAsset: "asset-1", defaults: defaults)
        #expect(RevalidationStateStore.loadCompletedVersions(forAsset: "asset-1", defaults: defaults) == versions)

        RevalidationStateStore.clear(forAsset: "asset-1", defaults: defaults)
        #expect(RevalidationStateStore.loadCompletedVersions(forAsset: "asset-1", defaults: defaults) == nil)
    }

    @Test("key shape is namespaced and per-asset")
    func keyShape() {
        // Lock down the key shape so a future refactor that, say,
        // bakes the prefix into a hash cannot accidentally invalidate
        // every pre-existing stamp on upgrade. The key is what's
        // persisted in production user UserDefaults; changing it is
        // a migration concern.
        #expect(RevalidationStateStore.key(forAsset: "abc") == "playhead.zx6i.completedVersions.abc")
        #expect(RevalidationStateStore.keyPrefix == "playhead.zx6i.completedVersions.")
    }

    @Test("decode failure returns nil (treats malformed stored value as absent)")
    func decodeFailureReturnsNil() {
        let defaults = makeIsolatedDefaults()
        // Inject a value that is not a valid `PipelineVersions` JSON
        // payload (just random bytes). The consumer must treat it as
        // "no stamp" so the runner falls through to full analysis
        // and the next successful run re-stamps cleanly.
        defaults.set(Data([0xde, 0xad, 0xbe, 0xef]), forKey: RevalidationStateStore.key(forAsset: "asset-1"))
        let loaded = RevalidationStateStore.loadCompletedVersions(forAsset: "asset-1", defaults: defaults)
        #expect(loaded == nil)
    }
}
