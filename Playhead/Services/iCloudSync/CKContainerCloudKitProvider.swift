// CKContainerCloudKitProvider.swift
// playhead-5c1t — production `CloudKitProviding` backed by a real
// `CKContainer` private database. Tests never instantiate this; they
// wire `FakeCloudKitProvider` instead.
//
// Container ID: development one used here is `iCloud.com.playhead.app`.
// Final production rollout will require an iCloud capability + matching
// CloudKit dashboard schema. This shim exists so the runtime can wire
// CloudKit on real devices without further protocol changes — schema
// rollout is a separate ops bead.

import CloudKit
import Foundation
import OSLog

struct CKContainerCloudKitProvider: CloudKitProviding {
    /// Default container identifier. Mirrors `Playhead.entitlements`
    /// `com.apple.developer.icloud-container-identifiers` key. Keeping
    /// this in code lets a future per-environment override land without
    /// touching the protocol.
    static let defaultContainerIdentifier = "iCloud.com.playhead.app"

    private let logger = Logger(subsystem: "com.playhead", category: "iCloudSync.Provider")

    let container: CKContainer
    var database: CKDatabase { container.privateCloudDatabase }

    init(containerIdentifier: String = CKContainerCloudKitProvider.defaultContainerIdentifier) {
        self.container = CKContainer(identifier: containerIdentifier)
    }

    func accountStatus() async -> CloudKitAccountStatus {
        do {
            let status = try await container.accountStatus()
            switch status {
            case .available: return .available
            case .noAccount: return .noAccount
            case .restricted: return .restricted
            case .couldNotDetermine: return .couldNotDetermine
            case .temporarilyUnavailable: return .temporarilyUnavailable
            @unknown default: return .couldNotDetermine
            }
        } catch {
            logger.warning("accountStatus query failed: \(error.localizedDescription)")
            return .couldNotDetermine
        }
    }

    @discardableResult
    func save(_ record: CKRecord) async throws -> CKRecord {
        do {
            return try await database.save(record)
        } catch {
            throw mapError(error)
        }
    }

    func fetch(recordID: CKRecord.ID) async throws -> CKRecord? {
        do {
            return try await database.record(for: recordID)
        } catch let error as CKError where error.code == .unknownItem {
            return nil
        } catch {
            throw mapError(error)
        }
    }

    func fetchAll(recordType: String) async throws -> [CKRecord] {
        let query = CKQuery(
            recordType: recordType,
            predicate: NSPredicate(value: true)
        )
        do {
            return try await Self.collectPages(
                pageSize: pageSize,
                maxPages: maxPages,
                recordTypeForLog: recordType,
                logger: logger,
                fetchFirst: { limit -> PageResult<CKRecord, CKQueryOperation.Cursor> in
                    let page = try await self.database.records(
                        matching: query, resultsLimit: limit
                    )
                    return PageResult(
                        records: page.matchResults.compactMap { try? $1.get() },
                        cursor: page.queryCursor
                    )
                },
                fetchNext: { cursor, limit -> PageResult<CKRecord, CKQueryOperation.Cursor> in
                    let page = try await self.database.records(
                        continuingMatchFrom: cursor, resultsLimit: limit
                    )
                    return PageResult(
                        records: page.matchResults.compactMap { try? $1.get() },
                        cursor: page.queryCursor
                    )
                }
            )
        } catch {
            throw mapError(error)
        }
    }

    /// Single page of paginated records; returned by both `fetchFirst`
    /// and `fetchNext` injection points so the cursor-paging loop is
    /// testable without a real CKContainer.
    /// Generic over `Record` and `Cursor` so the test suite can drive
    /// the loop with plain Swift types instead of having to synthesize
    /// `CKQueryOperation.Cursor` (which has no public initializer).
    struct PageResult<Record, Cursor> {
        let records: [Record]
        let cursor: Cursor?
    }

    /// Cursor pagination with a defensive page cap. Extracted as a
    /// static helper so tests can drive it with stubbed page-fetcher
    /// closures rather than spinning a real CKDatabase.
    /// 200 pages × 200 rows = 40k records, well above the realistic
    /// ceiling for a single user's entire subscription + entitlement
    /// universe — anything past it indicates a misbehaving server or
    /// SDK bug and we'd rather log + truncate than memory-bloat.
    static func collectPages<Record, Cursor>(
        pageSize: Int,
        maxPages: Int,
        recordTypeForLog: String,
        logger: Logger,
        fetchFirst: (Int) async throws -> PageResult<Record, Cursor>,
        fetchNext: (Cursor, Int) async throws -> PageResult<Record, Cursor>
    ) async throws -> [Record] {
        var collected: [Record] = []
        var page = try await fetchFirst(pageSize)
        collected.append(contentsOf: page.records)
        var pagesFetched = 1
        while let cursor = page.cursor, pagesFetched < maxPages {
            page = try await fetchNext(cursor, pageSize)
            collected.append(contentsOf: page.records)
            pagesFetched += 1
        }
        if page.cursor != nil {
            logger.warning("fetchAll(\(recordTypeForLog)) hit page cap \(maxPages); truncating result.")
        }
        return collected
    }

    private var pageSize: Int { 200 }
    private var maxPages: Int { 200 }

    func subscribeToChanges(recordType: String, subscriptionID: String) async throws {
        let predicate = NSPredicate(value: true)
        let subscription = CKQuerySubscription(
            recordType: recordType,
            predicate: predicate,
            subscriptionID: subscriptionID,
            options: [.firesOnRecordCreation, .firesOnRecordUpdate, .firesOnRecordDeletion]
        )
        // Silent push — no user-visible alert.
        let info = CKSubscription.NotificationInfo()
        info.shouldSendContentAvailable = true
        subscription.notificationInfo = info

        do {
            _ = try await database.save(subscription)
        } catch let error as CKError where error.code == .serverRejectedRequest {
            // Existing subscription with the same ID — treat as success
            // (idempotency contract on `subscribeToChanges`).
            logger.info("Subscription \(subscriptionID) already installed.")
        } catch {
            throw mapError(error)
        }
    }

    private func mapError(_ error: Error) -> CloudKitProviderError {
        guard let ck = error as? CKError else {
            return .other(message: error.localizedDescription)
        }
        switch ck.code {
        case .notAuthenticated, .badContainer, .managedAccountRestricted:
            return .accountUnavailable
        case .networkUnavailable, .networkFailure:
            return .networkUnavailable
        case .requestRateLimited, .zoneBusy, .serviceUnavailable:
            let retry = ck.userInfo[CKErrorRetryAfterKey] as? TimeInterval
            return .rateLimited(retryAfterSeconds: retry)
        case .serverRecordChanged:
            return .serverRecordChanged
        default:
            return .other(message: "\(ck.code.rawValue): \(ck.localizedDescription)")
        }
    }
}
