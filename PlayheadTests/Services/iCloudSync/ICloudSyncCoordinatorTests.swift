// ICloudSyncCoordinatorTests.swift
// playhead-5c1t — TDD Cycle 4: end-to-end behavior of the sync coordinator.
//
// The coordinator owns:
//   * Pushing local subscription/entitlement edits to CloudKit.
//   * Pulling remote edits and merging them locally.
//   * Queueing writes when offline / not signed in, draining when reachable.
//   * Pausing on iCloud sign-out without deleting local data.
//   * Idempotent first-launch fetch on a fresh device.
//
// The coordinator does NOT:
//   * Touch SwiftData directly — it operates on `SubscriptionRecord`
//     and `EntitlementRecord` value types. The SwiftData ↔ Record
//     bridging lives at the call site (a SubscriptionLibrary actor we
//     ship in a follow-up cycle).
//
// Conflict resolution rule: per-record-type. See `SubscriptionRecord
// .merge` and `EntitlementRecord.merge` for the deterministic rules.

import CloudKit
import Foundation
import Testing

@testable import Playhead

@Suite("ICloudSyncCoordinator — sync, offline queue, sign-out, initial fetch")
struct ICloudSyncCoordinatorTests {

    // MARK: - Helpers

    private static func makeSubscription(
        feed: String = "https://example.com/feed.xml",
        title: String = "Show",
        isRemoved: Bool = false,
        modifiedAt epoch: TimeInterval = 1_700_000_000
    ) -> SubscriptionRecord {
        SubscriptionRecord(
            feedURL: URL(string: feed)!,
            title: title,
            author: "Author",
            artworkURL: nil,
            subscribedAt: Date(timeIntervalSince1970: epoch),
            isRemoved: isRemoved,
            lastModified: Date(timeIntervalSince1970: epoch)
        )
    }

    private static func makeEntitlement(
        granted: Bool,
        at epoch: TimeInterval = 1_700_000_000
    ) -> EntitlementRecord {
        EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: granted,
            grantedAt: Date(timeIntervalSince1970: epoch),
            sourceDeviceID: "test-device"
        )
    }

    // MARK: - Subscription push

    @Test("Push: local subscription edit saves to CloudKit when account is available")
    func pushSubscription() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)

        let sub = Self.makeSubscription()
        try await coordinator.upsertSubscription(sub)

        let saved = await provider.records
        #expect(saved.count == 1)
        #expect(saved.first?.recordType == SubscriptionRecord.recordType)
    }

    @Test("Push: when not signed in, write is queued and drains on next reachable push")
    func offlineQueueDrainsOnReachability() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .noAccount)
        let coordinator = ICloudSyncCoordinator(provider: provider)

        // First write happens while signed-out — coordinator queues it.
        try await coordinator.upsertSubscription(Self.makeSubscription())

        // Nothing should have hit CloudKit yet.
        #expect(await provider.records.isEmpty)
        #expect(await coordinator.pendingWriteCount == 1)

        // Sign in and pump the queue.
        await provider.setAccountStatus(.available)
        try await coordinator.flushPendingWrites()

        #expect(await provider.records.count == 1)
        #expect(await coordinator.pendingWriteCount == 0)
    }

    @Test("Push: network-unavailable error queues write for retry rather than dropping it")
    func networkErrorQueues() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        await provider.scriptNextError(.networkUnavailable)

        let coordinator = ICloudSyncCoordinator(provider: provider)
        try await coordinator.upsertSubscription(Self.makeSubscription())

        // The save call is consumed (1) but the record never landed.
        #expect(await provider.records.isEmpty)
        #expect(await coordinator.pendingWriteCount == 1)

        // A flush retry succeeds because the scripted error was one-shot.
        try await coordinator.flushPendingWrites()
        #expect(await provider.records.count == 1)
        #expect(await coordinator.pendingWriteCount == 0)
    }

    // MARK: - Pull / initial fetch

    @Test("Initial fetch returns server subscriptions for a fresh device")
    func initialFetchHydrates() async throws {
        let provider = FakeCloudKitProvider()
        let seed = Self.makeSubscription(feed: "https://a.com/f", title: "Pre-existing")
        await provider.seed(seed.toCKRecord())

        let coordinator = ICloudSyncCoordinator(provider: provider)
        let result = try await coordinator.initialSubscriptionFetch()
        #expect(result.count == 1)
        #expect(result.first?.feedURL == seed.feedURL)
    }

    @Test("Initial fetch is a no-op when not signed in (returns empty, leaves local alone)")
    func initialFetchSignedOutIsNoOp() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .noAccount)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        let result = try await coordinator.initialSubscriptionFetch()
        #expect(result.isEmpty)
    }

    // MARK: - Conflict resolution

    @Test("Same subscription added on two devices simultaneously merges idempotently")
    func idempotentDuplicateAdd() async throws {
        let provider = FakeCloudKitProvider()
        let coordinator = ICloudSyncCoordinator(provider: provider)

        let s1 = Self.makeSubscription(modifiedAt: 1_700_000_000)
        let s2 = Self.makeSubscription(modifiedAt: 1_700_000_500)
        try await coordinator.upsertSubscription(s1)
        try await coordinator.upsertSubscription(s2)

        // Same feed URL → single CKRecord ID → upsert collapses to one record.
        let saved = await provider.records
        #expect(saved.count == 1)
        let decoded = try SubscriptionRecord(ckRecord: saved[0])
        #expect(decoded.lastModified == s2.lastModified,
                "Newer write must win the upsert.")
    }

    // MARK: - Entitlement: the core trust win

    @Test("Push entitlement: device A's grant is visible to device B on next fetch")
    func entitlementCrossDevice() async throws {
        // Both devices share the same fake-CloudKit instance.
        let provider = FakeCloudKitProvider()
        let deviceA = ICloudSyncCoordinator(provider: provider)
        let deviceB = ICloudSyncCoordinator(provider: provider)

        // Device A purchases.
        let granted = Self.makeEntitlement(granted: true, at: 1_700_000_000)
        try await deviceA.upsertEntitlement(granted)

        // Device B fetches.
        let remote = try await deviceB.fetchEntitlement()
        #expect(remote?.isGranted == true,
                "Device B must observe Device A's purchase via CloudKit — this is the core trust win.")
    }

    @Test("Entitlement merge applies grant-wins on conflicting writes")
    func entitlementGrantWins() async throws {
        let provider = FakeCloudKitProvider()
        // Server thinks NOT granted, with a more-recent timestamp.
        await provider.seed(
            Self.makeEntitlement(granted: false, at: 1_700_000_500).toCKRecord()
        )
        let coordinator = ICloudSyncCoordinator(provider: provider)

        // Local write says granted, with an EARLIER timestamp. Grant must
        // still win, not last-write-wins.
        let local = Self.makeEntitlement(granted: true, at: 1_700_000_000)
        let merged = try await coordinator.upsertEntitlementMerging(local)
        #expect(merged.isGranted == true)

        // The merged record was persisted.
        let server = try await provider.fetch(
            recordID: EntitlementRecord.recordID(forProductID: "com.playhead.premium")
        )
        #expect(server.flatMap { try? EntitlementRecord(ckRecord: $0) }?.isGranted == true)
    }

    // MARK: - Sign-out behavior

    @Test("Sign-out: local data is preserved (coordinator does NOT wipe local state)")
    func signOutPreservesLocal() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        try await coordinator.upsertSubscription(Self.makeSubscription())

        // User signs out. The coordinator's only response is to pause —
        // it must not contact the in-memory fake to delete records (that
        // would be a model-level erase the coordinator has no business
        // performing) and must not throw.
        await provider.setAccountStatus(.noAccount)
        await coordinator.handleAccountStatusChange()

        // Subsequent writes queue rather than throwing.
        try await coordinator.upsertSubscription(
            Self.makeSubscription(feed: "https://b.com/f", title: "After sign-out")
        )
        #expect(await coordinator.pendingWriteCount == 1)

        // Re-signing in drains the queue.
        await provider.setAccountStatus(.available)
        await coordinator.handleAccountStatusChange()
        try await coordinator.flushPendingWrites()
        // 1 pre-signout record + 1 queued = 2.
        #expect(await provider.records.count == 2)
    }

    @Test("Sign-out: isSyncEnabled reports false; sign-in flips it back")
    func signOutFlipsSyncEnabled() async {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        await coordinator.handleAccountStatusChange()
        #expect(await coordinator.isSyncEnabled == true)

        await provider.setAccountStatus(.noAccount)
        await coordinator.handleAccountStatusChange()
        #expect(await coordinator.isSyncEnabled == false)

        await provider.setAccountStatus(.available)
        await coordinator.handleAccountStatusChange()
        #expect(await coordinator.isSyncEnabled == true)
    }

    // MARK: - Subscriptions

    @Test("Coordinator installs CKQuerySubscriptions for both record types on startup")
    func installsSubscriptions() async throws {
        let provider = FakeCloudKitProvider()
        let coordinator = ICloudSyncCoordinator(provider: provider)
        try await coordinator.installRemoteChangeSubscriptions()

        let installed = await provider.subscribedRecordTypes
        #expect(installed.contains(SubscriptionRecord.recordType))
        #expect(installed.contains(EntitlementRecord.recordType))
    }
}
