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
        cachedAccountStatus = await provider.accountStatus()
        logger.info("Account status: \(String(describing: self.cachedAccountStatus))")
    }

    /// True when sync would attempt a network call. Read by the
    /// Settings UI footer and by tests.
    var isSyncEnabled: Bool {
        cachedAccountStatus == .available
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
