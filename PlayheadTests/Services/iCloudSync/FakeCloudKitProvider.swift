// FakeCloudKitProvider.swift
// playhead-5c1t — in-memory fake for `CloudKitProviding`. Lets the sync
// coordinator be exercised under XCTest without contacting real iCloud
// servers. Tests configure account status, scripted errors, and the
// in-memory record store; saves and fetches behave like a single-device
// CKContainer.

import CloudKit
import Foundation

@testable import Playhead

/// In-memory fake of a CloudKit private database. Thread-safe via an
/// internal actor — every method hops onto the actor before mutating
/// state, so tests can fan out without synchronization races.
final class FakeCloudKitProvider: CloudKitProviding, @unchecked Sendable {
    private let state: State

    init(initialAccountStatus: CloudKitAccountStatus = .available) {
        self.state = State(accountStatus: initialAccountStatus)
    }

    // MARK: - Test seam

    func setAccountStatus(_ status: CloudKitAccountStatus) async {
        await state.setAccountStatus(status)
    }

    /// Scripts the next save / fetch / fetchAll / subscribe call to
    /// throw the given error before mutating the in-memory store. The
    /// next operation consumes the script; subsequent calls behave
    /// normally. Use to exercise `serverRecordChanged`, `rateLimited`,
    /// network-down paths, etc.
    func scriptNextError(_ error: CloudKitProviderError) async {
        await state.scriptNextError(error)
    }

    /// Pre-seed a record into the fake DB. Bypasses error scripting.
    func seed(_ record: CKRecord) async {
        await state.seed(record)
    }

    /// Snapshot of every record currently in the fake DB. Tests use
    /// this to assert what was saved.
    var records: [CKRecord] {
        get async { await state.allRecords() }
    }

    var subscribedRecordTypes: Set<String> {
        get async { await state.subscribedTypes() }
    }

    var saveCallCount: Int {
        get async { await state.saveCount }
    }

    // MARK: - CloudKitProviding

    func accountStatus() async -> CloudKitAccountStatus {
        await state.accountStatus
    }

    @discardableResult
    func save(_ record: CKRecord) async throws -> CKRecord {
        if let scripted = await state.consumeScriptedError() {
            throw scripted
        }
        return await state.upsert(record)
    }

    func fetch(recordID: CKRecord.ID) async throws -> CKRecord? {
        if let scripted = await state.consumeScriptedError() {
            throw scripted
        }
        return await state.fetch(recordID: recordID)
    }

    func fetchAll(recordType: String) async throws -> [CKRecord] {
        if let scripted = await state.consumeScriptedError() {
            throw scripted
        }
        return await state.fetchAll(recordType: recordType)
    }

    func subscribeToChanges(recordType: String, subscriptionID: String) async throws {
        if let scripted = await state.consumeScriptedError() {
            throw scripted
        }
        await state.addSubscription(recordType: recordType)
    }

    // MARK: - State actor

    private actor State {
        var accountStatus: CloudKitAccountStatus
        private var store: [CKRecord.ID: CKRecord] = [:]
        private var scriptedError: CloudKitProviderError?
        private var subscriptions: Set<String> = []
        private(set) var saveCount: Int = 0

        init(accountStatus: CloudKitAccountStatus) {
            self.accountStatus = accountStatus
        }

        func setAccountStatus(_ status: CloudKitAccountStatus) {
            accountStatus = status
        }

        func scriptNextError(_ error: CloudKitProviderError) {
            scriptedError = error
        }

        func consumeScriptedError() -> CloudKitProviderError? {
            defer { scriptedError = nil }
            return scriptedError
        }

        func seed(_ record: CKRecord) {
            store[record.recordID] = record
        }

        func upsert(_ record: CKRecord) -> CKRecord {
            saveCount += 1
            store[record.recordID] = record
            return record
        }

        func fetch(recordID: CKRecord.ID) -> CKRecord? {
            store[recordID]
        }

        func fetchAll(recordType: String) -> [CKRecord] {
            store.values.filter { $0.recordType == recordType }
        }

        func allRecords() -> [CKRecord] {
            Array(store.values)
        }

        func subscribedTypes() -> Set<String> {
            subscriptions
        }

        func addSubscription(recordType: String) {
            subscriptions.insert(recordType)
        }
    }
}
