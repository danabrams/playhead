// SuspendingFakeCloudKitProvider.swift
// playhead-5c1t — variant of `FakeCloudKitProvider` that can suspend a
// `save(_:)` call mid-flight so a test can deterministically inject a
// concurrent edit during the suspension window. Used to exercise the
// `===` reentrancy guard in `flushPendingWrites` — verifying that a
// successful in-flight save does NOT clobber a fresher record that
// landed in `pendingWrites` while we were suspended on the network.
//
// Why this lives separately from `FakeCloudKitProvider`: the gate
// adds quite a bit of state (entry waiter, suspension continuation)
// that the vast majority of tests don't need, and conflating the two
// makes the simple fake harder to reason about.

import CloudKit
import Foundation

@testable import Playhead

final class SuspendingFakeCloudKitProvider: CloudKitProviding, @unchecked Sendable {
    private let state: State

    init(initialAccountStatus: CloudKitAccountStatus = .available) {
        self.state = State(accountStatus: initialAccountStatus)
    }

    // MARK: - Test seam

    func setAccountStatus(_ status: CloudKitAccountStatus) async {
        await state.setAccountStatus(status)
    }

    /// Arms the gate so that the very NEXT call to `save(_:)` suspends
    /// at the entry point until `releaseGatedSave()` resumes it. Only
    /// the first save after arming is gated; subsequent saves run
    /// normally (or as scripted).
    func gateNextSave() async {
        await state.gateNextSave()
    }

    /// Suspends the caller until the gated save has been entered. Used
    /// by the test so it knows the in-flight save is parked and it is
    /// safe to fire the concurrent edit.
    func waitForGatedSaveEntry() async {
        await state.waitForGatedSaveEntry()
    }

    /// Releases the gated save with success — it will resume and
    /// upsert the record into the in-memory store as usual.
    func releaseGatedSave() async {
        await state.releaseGatedSave()
    }

    /// Scripts the next `save`, `fetch`, `fetchAll`, or `subscribe`
    /// call to throw before mutating state. Consumed on first use.
    func scriptNextError(_ error: CloudKitProviderError) async {
        await state.scriptNextError(error)
    }

    var records: [CKRecord] {
        get async { await state.allRecords() }
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
        try await state.preSave()
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
    }

    // MARK: - State actor

    private actor State {
        var accountStatus: CloudKitAccountStatus
        private var store: [CKRecord.ID: CKRecord] = [:]
        private(set) var saveCount: Int = 0
        private var scriptedError: CloudKitProviderError?

        // Gate state. `gateArmed` flips off as soon as a save enters
        // the gate so that only ONE save is suspended at a time —
        // a re-armed gate is a deliberate test step, not a default.
        private var gateArmed: Bool = false
        private var gateEntered: Bool = false
        private var gateContinuation: CheckedContinuation<Void, Never>?
        private var entryWaiters: [CheckedContinuation<Void, Never>] = []

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

        func gateNextSave() {
            gateArmed = true
            gateEntered = false
        }

        func waitForGatedSaveEntry() async {
            if gateEntered { return }
            await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
                entryWaiters.append(c)
            }
        }

        func releaseGatedSave() {
            // Disarming here too: a release with no live gate would
            // otherwise leave the next save suspended on the resume
            // call below being nil — surfaces test bugs early.
            gateArmed = false
            if let cont = gateContinuation {
                gateContinuation = nil
                cont.resume()
            }
        }

        /// Pre-save hook. Order matters:
        /// 1. If the gate is armed, suspend HERE without consuming the
        ///    scripted error — that error is reserved for the next
        ///    (non-gated) save.
        /// 2. Otherwise, throw a scripted error if any.
        ///
        /// The split lets a test set up `gateNextSave` + scripted error
        /// for two different in-flight saves (the gated one succeeds,
        /// the concurrent one throws).
        func preSave() async throws {
            if gateArmed {
                gateArmed = false
                gateEntered = true
                let waiters = entryWaiters
                entryWaiters.removeAll()
                for w in waiters { w.resume() }
                await withCheckedContinuation { (c: CheckedContinuation<Void, Never>) in
                    gateContinuation = c
                }
                return
            }
            if let scripted = scriptedError {
                scriptedError = nil
                throw scripted
            }
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
    }
}
