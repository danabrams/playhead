// CloudKitProviding.swift
// playhead-5c1t — minimal protocol abstraction over the CloudKit
// container/database surface area we actually use. Wrapping the
// framework lets us:
//   * Swap in a deterministic in-memory fake under unit tests, where
//     `CKContainer.default()` would otherwise hit the real
//     iCloud-account-status/network surface and blow up under XCTest.
//   * Keep the rest of the sync coordinator framework-agnostic — only
//     this file imports `CloudKit`.
//
// Production wiring uses `CKContainerCloudKitProvider`, which forwards
// to a `CKContainer` private database. Tests wire `FakeCloudKitProvider`
// (declared in the test target).

import CloudKit
import Foundation

// MARK: - Account status

/// Mirror of `CKAccountStatus`. Re-declared so call sites can match
/// against this without importing CloudKit.
enum CloudKitAccountStatus: Sendable, Equatable {
    case available
    case noAccount
    case restricted
    case couldNotDetermine
    case temporarilyUnavailable
}

// MARK: - CloudKit error abstraction

/// High-level error surface emitted by `CloudKitProviding`. Mapped from
/// the underlying `CKError.Code` cases the sync coordinator actually
/// treats differently. Anything we don't recognize collapses into
/// `.other` so callers don't grow open-ended switch statements.
enum CloudKitProviderError: Error, Equatable, Sendable {
    /// The user is not signed in to iCloud, or has restricted access.
    /// Sync should pause — do NOT delete local data.
    case accountUnavailable
    /// The network is offline. The write should be queued for retry.
    case networkUnavailable
    /// The server rejected the write because of a rate limit / quota.
    case rateLimited(retryAfterSeconds: TimeInterval?)
    /// The CKRecord we tried to save was older than the server copy.
    /// The caller should fetch the server record and merge.
    case serverRecordChanged
    /// Catch-all.
    case other(message: String)
}

// MARK: - Provider protocol

/// Slim async surface that the sync coordinator depends on. Any larger
/// CloudKit feature surface (subscriptions to record-zone changes, etc.)
/// lives behind these methods.
protocol CloudKitProviding: Sendable {
    /// Current account status. Production reads `CKContainer.accountStatus`;
    /// tests return a value directly.
    func accountStatus() async -> CloudKitAccountStatus

    /// Save a single record to the private database. Returns the saved
    /// record (with a fresh change tag) on success; throws on failure.
    @discardableResult
    func save(_ record: CKRecord) async throws -> CKRecord

    /// Fetch a record by ID. Returns `nil` when the record does not
    /// exist (vs. throwing) so call sites can branch on absence cleanly.
    func fetch(recordID: CKRecord.ID) async throws -> CKRecord?

    /// Fetch all records of a given type. Used by the initial-sync path
    /// on a fresh device.
    func fetchAll(recordType: String) async throws -> [CKRecord]

    /// Install a query subscription so the device receives a push when
    /// the given record type changes server-side. Idempotent — calling
    /// twice with the same identifier is a no-op.
    func subscribeToChanges(recordType: String, subscriptionID: String) async throws
}
