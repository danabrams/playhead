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

    // MARK: - Subscription merge (last-write-wins)

    @Test("upsertSubscriptionMerging: stale local edit loses to newer server copy")
    func subscriptionMergeStaleLocalLosesToNewerServer() async throws {
        // Server already has a newer rename for the same feed.
        let provider = FakeCloudKitProvider()
        let serverRecord = Self.makeSubscription(
            title: "Server Renamed",
            modifiedAt: 1_700_000_500
        )
        await provider.seed(serverRecord.toCKRecord())

        let coordinator = ICloudSyncCoordinator(provider: provider)

        // Local has an older edit (e.g. stale offline). After merge the
        // server copy must win — last-write-wins, NOT GRANT-WINS.
        let localStale = Self.makeSubscription(
            title: "Local Stale",
            modifiedAt: 1_700_000_000
        )
        let merged = try await coordinator.upsertSubscriptionMerging(localStale)
        #expect(merged.title == "Server Renamed",
                "Newer server lastModified must win over older local edit.")

        // The server-side record was preserved (not clobbered with stale
        // local title).
        let saved = try await provider.fetch(
            recordID: SubscriptionRecord.recordID(forFeedURL: localStale.feedURL)
        )
        let decoded = try #require(saved.flatMap { try? SubscriptionRecord(ckRecord: $0) })
        #expect(decoded.title == "Server Renamed")
    }

    @Test("upsertSubscriptionMerging: newer local edit beats older server copy")
    func subscriptionMergeNewerLocalBeatsOlderServer() async throws {
        let provider = FakeCloudKitProvider()
        await provider.seed(
            Self.makeSubscription(title: "Old Server", modifiedAt: 1_700_000_000).toCKRecord()
        )

        let coordinator = ICloudSyncCoordinator(provider: provider)
        let local = Self.makeSubscription(title: "New Local", modifiedAt: 1_700_000_500)
        let merged = try await coordinator.upsertSubscriptionMerging(local)
        #expect(merged.title == "New Local")

        let saved = try await provider.fetch(
            recordID: SubscriptionRecord.recordID(forFeedURL: local.feedURL)
        )
        let decoded = try #require(saved.flatMap { try? SubscriptionRecord(ckRecord: $0) })
        #expect(decoded.title == "New Local")
    }

    @Test("upsertSubscriptionMerging: unsubscribe tombstone after server add still propagates")
    func subscriptionMergeTombstonePropagates() async throws {
        let provider = FakeCloudKitProvider()
        // Server has an older add.
        await provider.seed(
            Self.makeSubscription(modifiedAt: 1_700_000_000).toCKRecord()
        )
        let coordinator = ICloudSyncCoordinator(provider: provider)

        // Local emits a newer tombstone — must win the merge.
        let tombstone = Self.makeSubscription(
            isRemoved: true,
            modifiedAt: 1_700_000_500
        )
        let merged = try await coordinator.upsertSubscriptionMerging(tombstone)
        #expect(merged.isRemoved == true,
                "Newer remove must beat older add — otherwise unsubscribe never propagates.")
    }

    // MARK: - syncEnabledUpdates stream

    @Test("syncEnabledUpdates: yields current value to a fresh subscriber")
    func syncEnabledUpdatesSeedsCurrent() async {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        await coordinator.handleAccountStatusChange()

        let stream = await coordinator.syncEnabledUpdates()
        var iterator = stream.makeAsyncIterator()
        let first = await iterator.next()
        #expect(first == true,
                "Late subscribers must observe the current sync-enabled value, not hang waiting for the next account-status change.")
    }

    @Test("syncEnabledUpdates: sign-out mid-session flips the published value")
    func syncEnabledUpdatesFlipOnSignOut() async {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        await coordinator.handleAccountStatusChange()

        let stream = await coordinator.syncEnabledUpdates()
        var iterator = stream.makeAsyncIterator()
        // Drain the seed value.
        _ = await iterator.next()

        // Drive the provider to unavailable and re-poll.
        await provider.setAccountStatus(.noAccount)
        await coordinator.handleAccountStatusChange()

        let next = await iterator.next()
        #expect(next == false,
                "Sign-out must flip the stream's published value so the Settings footer updates without a view re-appear.")
    }

    @Test("flushPendingWrites: a newer concurrent edit replaces the queued record before retry succeeds")
    func flushDoesNotClobberConcurrentEdit() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)

        // Force the first save to fail so the record lands in the queue.
        await provider.scriptNextError(.networkUnavailable)
        let stale = Self.makeSubscription(title: "Stale", modifiedAt: 1_700_000_000)
        try await coordinator.upsertSubscription(stale)
        #expect(await coordinator.pendingWriteCount == 1)

        // A concurrent edit lands BEFORE the flush runs — same recordID
        // (same feed URL) but a newer record. The coordinator must replace
        // the queued record so the retry uploads the newer copy. If the
        // === identity guard regressed and the old `stale` reference were
        // dropped after the retry succeeded, the user's most recent edit
        // would be silently lost.
        let newer = Self.makeSubscription(title: "Newer", modifiedAt: 1_700_000_500)
        try await coordinator.upsertSubscription(newer)

        // Flush — the retry should upload the newer record, not the stale one.
        try await coordinator.flushPendingWrites()
        let saved = await provider.records
        #expect(saved.count == 1)
        let decoded = try #require(saved.first.flatMap { try? SubscriptionRecord(ckRecord: $0) })
        #expect(decoded.title == "Newer",
                "Concurrent newer edit must survive — stale retry must not clobber it.")
        #expect(await coordinator.pendingWriteCount == 0)
    }

    @Test("flushPendingWrites: in-flight account-unavailable yields false to syncEnabledUpdates")
    func flushAccountUnavailableNotifiesSubscribers() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        await coordinator.handleAccountStatusChange()

        // Subscribe BEFORE the flush so we can observe the in-flight flip.
        let stream = await coordinator.syncEnabledUpdates()
        var iterator = stream.makeAsyncIterator()
        // Drain the seed `true`.
        let seed = await iterator.next()
        #expect(seed == true)

        // Queue a write that will fail with `.accountUnavailable` during
        // the flush. The coordinator's handler must flip `cachedAccountStatus`
        // AND notify subscribers — anything else leaves Settings UI stale
        // until the next external account event.
        await provider.scriptNextError(.networkUnavailable)
        try await coordinator.upsertSubscription(Self.makeSubscription())
        await provider.scriptNextError(.accountUnavailable)
        do {
            try await coordinator.flushPendingWrites()
            #expect(Bool(false), "Flush should have thrown on .accountUnavailable")
        } catch {
            // Expected.
        }

        // The stream must have emitted `false` because of the in-flight flip.
        let next = await iterator.next()
        #expect(next == false,
                "In-flight .accountUnavailable must yield to syncEnabledUpdates subscribers.")
    }

    @Test("flushPendingWrites: in-flight === guard preserves a concurrent edit that lands during the suspended save")
    func flushReentrancyGuardPreservesInFlightConcurrentEdit() async throws {
        // The classic reentrancy hazard: a record is being saved by
        // `flushPendingWrites`, the actor suspends on the network
        // round-trip, a fresh edit for the same recordID lands in
        // `pendingWrites`, then the original save returns successfully.
        // The post-save cleanup must NOT remove the queue entry now,
        // because the entry is no longer the record we just saved —
        // it's the user's most recent intent and getting dropped
        // would silently lose data.
        let provider = SuspendingFakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)

        // Phase 1: queue an "old" record by failing the first direct save.
        await provider.scriptNextError(.networkUnavailable)
        let old = Self.makeSubscription(title: "Old", modifiedAt: 1_700_000_000)
        try await coordinator.upsertSubscription(old)
        #expect(await coordinator.pendingWriteCount == 1)

        // Phase 2: arm the gate so the next save (the flush retry of the
        // OLD record) will suspend at provider entry, AND script a
        // network error for the save after that — Task B's concurrent
        // edit. Ordering inside `preSave` is gate-first, then scripted
        // error, so this is well-defined.
        await provider.gateNextSave()
        await provider.scriptNextError(.networkUnavailable)

        // Phase 3: kick off the flush. It will suspend inside the gated
        // save. We do NOT await this task yet — we deliberately overlap
        // it with Task B below.
        let flushTask = Task { try await coordinator.flushPendingWrites() }

        // Wait until the gate has been entered so we know the flush is
        // parked on the network round-trip.
        await provider.waitForGatedSaveEntry()

        // Phase 4: concurrent edit lands. The gated save has already
        // released the actor (it's suspended on the gate), so this call
        // can run on the actor and mutate `pendingWrites`. Its own
        // save() throws the scripted networkUnavailable, and
        // handleSaveError swaps `pendingWrites[id]` to the NEW CKRecord.
        let newer = Self.makeSubscription(title: "Newer", modifiedAt: 1_700_000_500)
        try await coordinator.upsertSubscription(newer)
        #expect(await coordinator.pendingWriteCount == 1,
                "After concurrent edit, queue still holds exactly one record (the newer one).")

        // Phase 5: release the gate. The original save returns
        // successfully and the reentrancy guard runs:
        //   if pendingWrites[id] === record { remove }
        // Since pendingWrites[id] now holds the newer CKRecord, the
        // guard correctly leaves it in place.
        await provider.releaseGatedSave()
        _ = try await flushTask.value

        // The newer record must still be queued — the in-flight save's
        // success did NOT clobber it.
        #expect(await coordinator.pendingWriteCount == 1,
                "=== guard regression: a successful in-flight save dropped a fresher concurrent edit.")

        // The fake's store currently has the OLD record (that's the one
        // that flushed). A second flush should now drain the newer
        // record cleanly, proving the queue still holds the right copy.
        try await coordinator.flushPendingWrites()
        #expect(await coordinator.pendingWriteCount == 0)
        let saved = await provider.records
        #expect(saved.count == 1)
        let decoded = try #require(saved.first.flatMap { try? SubscriptionRecord(ckRecord: $0) })
        #expect(decoded.title == "Newer",
                "Second flush must upload the concurrent newer edit; it survived the reentrancy window.")
    }

    @Test("upsertSubscriptionMerging: queues local copy when account unavailable")
    func subscriptionMergeQueuesWhenSignedOut() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .noAccount)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        let local = Self.makeSubscription()

        let returned = try await coordinator.upsertSubscriptionMerging(local)
        // Queued — nothing reached the fake DB and the merge couldn't run.
        #expect(returned == local)
        #expect(await provider.records.isEmpty)
        #expect(await coordinator.pendingWriteCount == 1)
    }

}
