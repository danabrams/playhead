// ICloudSyncCoordinator.swift
// playhead-5c1t — actor that owns the iCloud sync lifecycle for podcast
// subscriptions + the premium entitlement.
//
// Responsibilities:
//   * Push local edits to CloudKit (private DB), with deterministic
//     conflict-resolution rules baked into the per-record-type merge.
//   * Pull remote edits and surface merged values to the call site.
//   * Queue writes when the user is signed-out / network is offline,
//     drain when reachable.
//   * Pause sync (without wiping local data) on iCloud sign-out.
//   * Install `CKQuerySubscription`s so other devices' writes push.
//
// Out of scope (deliberate):
//   * SwiftData ↔ Record bridging. The coordinator works in
//     `SubscriptionRecord` / `EntitlementRecord` value types; the call
//     site (a future SubscriptionLibrary actor) translates to/from
//     SwiftData. This keeps the coordinator framework-agnostic.
//   * Episode-level state and user corrections. The bead description
//     names these as future work; explicit scope per the implementation
//     prompt is "subscriptions + entitlements only".

import CloudKit
import Foundation
import OSLog

/// Coordinator actor — single-writer concurrency story for iCloud sync.
/// Use `await` from any actor. Constructed once per app launch and
/// retained by `PlayheadRuntime`.
actor ICloudSyncCoordinator {
    private let logger = Logger(subsystem: "com.playhead", category: "iCloudSync")

    private let provider: CloudKitProviding

    /// Cached account status. Refreshed on every `handleAccountStatusChange`
    /// and consulted before each write to decide queue-vs-save.
    private var cachedAccountStatus: CloudKitAccountStatus = .couldNotDetermine

    /// FIFO of CKRecords waiting to be saved. Keyed by recordID so a
    /// later edit of the same record collapses with the earlier queued
    /// edit (idempotent — the queue is a bag of latest-known-good
    /// snapshots, not a strict event log).
    private var pendingWrites: [CKRecord.ID: CKRecord] = [:]

    /// Subscribers receive a fresh `Bool` whenever the cached account
    /// status changes (true = `isSyncEnabled`). The Settings footer
    /// uses this so sign-out mid-session flips its label without a
    /// view re-appear. We use a `.bufferingNewest(1)` policy so a slow
    /// consumer that misses an intermediate value still sees the
    /// latest status on resume. Multiple-consumer fan-out is
    /// implemented via an array of continuations — one per subscriber.
    private var syncEnabledContinuations: [UUID: AsyncStream<Bool>.Continuation] = [:]

    init(provider: CloudKitProviding) {
        self.provider = provider
    }

    // MARK: - Lifecycle

    /// One-shot at app launch. Reads the current account status,
    /// installs change subscriptions when available, and pulls the
    /// initial server state. Failures are logged — call site decides
    /// whether to surface them.
    func start() async {
        await handleAccountStatusChange()
        guard cachedAccountStatus == .available else {
            logger.info("iCloud unavailable at start (status=\(String(describing: self.cachedAccountStatus))); sync paused.")
            return
        }
        do {
            try await installRemoteChangeSubscriptions()
        } catch {
            logger.warning("Failed to install change subscriptions: \(error.localizedDescription)")
        }
    }

    /// Refresh the cached account status from the provider. Drives
    /// `isSyncEnabled` and gating logic for writes.
    func handleAccountStatusChange() async {
        let next = await provider.accountStatus()
        let changed = next != cachedAccountStatus
        cachedAccountStatus = next
        logger.info("Account status: \(String(describing: self.cachedAccountStatus))")
        if changed {
            let enabled = (next == .available)
            for (_, continuation) in syncEnabledContinuations {
                continuation.yield(enabled)
            }
        }
    }

    /// True when sync would attempt a network call. Read by the
    /// Settings UI footer and by tests.
    var isSyncEnabled: Bool {
        cachedAccountStatus == .available
    }

    /// Live stream of `isSyncEnabled` updates. The first element is the
    /// current status (so a late subscriber sees the value rather than
    /// hanging until the next account-status change). Subsequent
    /// elements are emitted only when `handleAccountStatusChange`
    /// observes a change. The stream is buffered to size 1 — slow
    /// consumers see the latest value on resume rather than a stale
    /// in-flight one.
    func syncEnabledUpdates() -> AsyncStream<Bool> {
        AsyncStream(bufferingPolicy: .bufferingNewest(1)) { continuation in
            let id = UUID()
            self.syncEnabledContinuations[id] = continuation
            // Seed with the current value so a subscriber that joins
            // after `start()` doesn't have to wait for the next change.
            continuation.yield(self.cachedAccountStatus == .available)
            continuation.onTermination = { [weak self] _ in
                guard let self else { return }
                Task { await self.removeContinuation(id: id) }
            }
        }
    }

    private func removeContinuation(id: UUID) {
        syncEnabledContinuations[id] = nil
    }

    var pendingWriteCount: Int {
        pendingWrites.count
    }

    // MARK: - Subscription push

    /// Push (upsert) a subscription. Falls back to the offline queue
    /// when not signed-in or when CloudKit returns a transient error.
    func upsertSubscription(_ record: SubscriptionRecord) async throws {
        let ckRecord = record.toCKRecord()
        try await save(ckRecord, kind: "subscription")
    }

    func upsertEntitlement(_ record: EntitlementRecord) async throws {
        let ckRecord = record.toCKRecord()
        try await save(ckRecord, kind: "entitlement")
    }

    /// Save with last-write-wins merging against the server copy.
    /// Subscriptions are mundane mutable state (title, artwork, tombstone
    /// flag), NOT a trust signal — so collisions resolve by latest
    /// `lastModified` rather than the GRANT-WINS rule used for
    /// entitlements. Returns the merged record (which is what got
    /// persisted to the server, modulo a queued offline write).
    @discardableResult
    func upsertSubscriptionMerging(_ local: SubscriptionRecord) async throws -> SubscriptionRecord {
        // Signed-out: queue the local copy. The next flush will run
        // through `save(_:)`, which on a `serverRecordChanged` collision
        // bottoms out in the queue again — at which point the call site
        // can re-run the merge with a fresh server fetch. The user's
        // most recent local intent is never silently dropped.
        await refreshAccountStatusIfUnknown()
        if cachedAccountStatus != .available {
            pendingWrites[local.toCKRecord().recordID] = local.toCKRecord()
            return local
        }
        let merged: SubscriptionRecord
        do {
            if let serverRecord = try await provider.fetch(
                recordID: SubscriptionRecord.recordID(forFeedURL: local.feedURL)
            ),
                let serverDecoded = try? SubscriptionRecord(ckRecord: serverRecord)
            {
                merged = SubscriptionRecord.merge(local: local, remote: serverDecoded)
            } else {
                merged = local
            }
            try await save(merged.toCKRecord(), kind: "subscription")
            return merged
        } catch let cloudError as CloudKitProviderError {
            handleSaveError(cloudError, record: local.toCKRecord())
            return local
        }
    }

    /// Save with grant-wins merging against the server copy. If the
    /// server has a stronger (granted) record, that record is preserved.
    /// Returns the resulting record (post-merge).
    @discardableResult
    func upsertEntitlementMerging(_ local: EntitlementRecord) async throws -> EntitlementRecord {
        // If signed-out, queue the LOCAL record — the merge will run on
        // the next flush after reading the server copy at that time.
        await refreshAccountStatusIfUnknown()
        if cachedAccountStatus != .available {
            pendingWrites[local.toCKRecord().recordID] = local.toCKRecord()
            return local
        }
        let merged: EntitlementRecord
        do {
            if let serverRecord = try await provider.fetch(
                recordID: EntitlementRecord.recordID(forProductID: local.productID)
            ),
                let serverDecoded = try? EntitlementRecord(ckRecord: serverRecord)
            {
                merged = EntitlementRecord.merge(local: local, remote: serverDecoded)
            } else {
                merged = local
            }
            try await save(merged.toCKRecord(), kind: "entitlement")
            return merged
        } catch let cloudError as CloudKitProviderError {
            // Pre-emptive merge fetch failed; queue and let the next
            // flush apply the merge.
            handleSaveError(cloudError, record: local.toCKRecord())
            return local
        }
    }

    // MARK: - Pull

    /// Initial-fetch path used on a fresh device. Returns the server's
    /// known subscriptions or an empty array when sync is disabled.
    func initialSubscriptionFetch() async throws -> [SubscriptionRecord] {
        await refreshAccountStatusIfUnknown()
        guard cachedAccountStatus == .available else { return [] }
        let raw = try await provider.fetchAll(recordType: SubscriptionRecord.recordType)
        return raw.compactMap { try? SubscriptionRecord(ckRecord: $0) }
    }

    /// Returns the current server-side entitlement record, if any.
    func fetchEntitlement(productID: String = PlayheadProduct.premiumUnlock) async throws -> EntitlementRecord? {
        await refreshAccountStatusIfUnknown()
        guard cachedAccountStatus == .available else { return nil }
        guard let raw = try await provider.fetch(
            recordID: EntitlementRecord.recordID(forProductID: productID)
        ) else { return nil }
        return try? EntitlementRecord(ckRecord: raw)
    }

    // MARK: - Subscriptions to remote changes

    func installRemoteChangeSubscriptions() async throws {
        try await provider.subscribeToChanges(
            recordType: SubscriptionRecord.recordType,
            subscriptionID: "playhead.subscription.changes"
        )
        try await provider.subscribeToChanges(
            recordType: EntitlementRecord.recordType,
            subscriptionID: "playhead.entitlement.changes"
        )
    }

    // MARK: - Queue draining

    /// Attempt to flush every queued write. Stops on the first error
    /// (the failed record stays in the queue for the next attempt) so
    /// rate-limits don't pound the server. Returns the number of
    /// records successfully drained.
    ///
    /// Always refreshes account status before draining — the queue's
    /// whole reason for existing is to outlive transient unavailability,
    /// so a sticky cached `noAccount` would defeat the point.
    @discardableResult
    func flushPendingWrites() async throws -> Int {
        cachedAccountStatus = await provider.accountStatus()
        guard cachedAccountStatus == .available else { return 0 }
        var drained = 0
        // Snapshot the queue so we can mutate `pendingWrites` while iterating.
        let snapshot = pendingWrites
        for (id, record) in snapshot {
            do {
                _ = try await provider.save(record)
                pendingWrites.removeValue(forKey: id)
                drained += 1
            } catch let cloudError as CloudKitProviderError {
                handleSaveError(cloudError, record: record)
                throw cloudError
            }
        }
        return drained
    }

    // MARK: - Internals

    private func save(_ ckRecord: CKRecord, kind: String) async throws {
        await refreshAccountStatusIfUnknown()
        guard cachedAccountStatus == .available else {
            pendingWrites[ckRecord.recordID] = ckRecord
            logger.info("\(kind) write queued — account unavailable")
            return
        }
        do {
            _ = try await provider.save(ckRecord)
        } catch let cloudError as CloudKitProviderError {
            handleSaveError(cloudError, record: ckRecord)
        }
    }

    private func handleSaveError(_ error: CloudKitProviderError, record: CKRecord) {
        switch error {
        case .accountUnavailable:
            cachedAccountStatus = .noAccount
            pendingWrites[record.recordID] = record
            logger.info("Save failed: account unavailable; queued.")
        case .networkUnavailable:
            pendingWrites[record.recordID] = record
            logger.info("Save failed: network unavailable; queued.")
        case .rateLimited(let retryAfter):
            pendingWrites[record.recordID] = record
            logger.warning("Save rate-limited; retry after \(retryAfter ?? 0) s.")
        case .serverRecordChanged:
            // Per-type merge happens at the call site that owns the
            // record. For now, queue and let the next merge-aware push
            // resolve.
            pendingWrites[record.recordID] = record
            logger.info("Server record changed; queued for merge on next push.")
        case .other(let message):
            // Conservative: queue rather than drop. Test expectations
            // depend on this fallback so a transient driver/error doesn't
            // cause silent data loss.
            pendingWrites[record.recordID] = record
            logger.warning("Save failed (other: \(message)); queued.")
        }
    }

    private func refreshAccountStatusIfUnknown() async {
        if cachedAccountStatus == .couldNotDetermine {
            cachedAccountStatus = await provider.accountStatus()
        }
    }
}
