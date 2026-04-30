// EntitlementRecordTests.swift
// playhead-5c1t — TDD Cycle 2: EntitlementRecord ↔ CKRecord marshaling.
//
// Conflict-resolution rule for entitlement: GRANT-WINS.
//   "Once you've paid, you've paid." If any device has the entitlement,
//   it is granted on all devices. This is the core trust win — a refund
//   /revocation flips the entitlement off via StoreKit's normal
//   `revocationDate` path, which is independent of CloudKit.

import CloudKit
import Foundation
import Testing

@testable import Playhead

@Suite("EntitlementRecord — CKRecord round-trip + grant-wins merge")
struct EntitlementRecordTests {

    @Test("Round-trips through CKRecord losslessly")
    func roundTrip() throws {
        let original = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: true,
            grantedAt: Date(timeIntervalSince1970: 1_700_000_000),
            sourceDeviceID: "device-A"
        )
        let decoded = try EntitlementRecord(ckRecord: original.toCKRecord())
        #expect(decoded == original)
    }

    @Test("Two records for the same product share a CKRecord ID")
    func deterministicID() {
        let a = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: true,
            grantedAt: .now,
            sourceDeviceID: "A"
        )
        let b = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: false,
            grantedAt: .now,
            sourceDeviceID: "B"
        )
        #expect(a.toCKRecord().recordID == b.toCKRecord().recordID,
                "Per-product entitlement record must have a single CKRecord ID across devices.")
    }

    @Test("Decoding fails on missing productID")
    func decodeRejectsMissing() {
        let bare = CKRecord(recordType: EntitlementRecord.recordType)
        #expect(throws: EntitlementRecord.DecodeError.self) {
            _ = try EntitlementRecord(ckRecord: bare)
        }
    }

    // MARK: - Grant-wins merge

    @Test("Merge: granted beats not-granted regardless of timestamp")
    func grantWins() {
        let granted = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: true,
            grantedAt: Date(timeIntervalSince1970: 1_700_000_000),
            sourceDeviceID: "A"
        )
        let notGranted = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: false,
            grantedAt: Date(timeIntervalSince1970: 1_700_000_999),
            sourceDeviceID: "B"
        )
        #expect(EntitlementRecord.merge(local: granted, remote: notGranted) == granted)
        #expect(EntitlementRecord.merge(local: notGranted, remote: granted) == granted)
    }

    @Test("Merge: when both granted, earliest grantedAt wins (stable trust signal)")
    func bothGrantedEarliestWins() {
        let earlier = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: true,
            grantedAt: Date(timeIntervalSince1970: 1_700_000_000),
            sourceDeviceID: "A"
        )
        let later = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: true,
            grantedAt: Date(timeIntervalSince1970: 1_700_000_500),
            sourceDeviceID: "B"
        )
        #expect(EntitlementRecord.merge(local: later, remote: earlier) == earlier)
        #expect(EntitlementRecord.merge(local: earlier, remote: later) == earlier)
    }

    @Test("Merge: when neither granted, the record is the canonical not-granted state")
    func bothNotGrantedNoOp() {
        let a = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: false,
            grantedAt: Date(timeIntervalSince1970: 1_700_000_000),
            sourceDeviceID: "A"
        )
        let b = EntitlementRecord(
            productID: "com.playhead.premium",
            isGranted: false,
            grantedAt: Date(timeIntervalSince1970: 1_700_000_500),
            sourceDeviceID: "B"
        )
        // Either is acceptable; the implementation picks `local` for stability.
        #expect(EntitlementRecord.merge(local: a, remote: b) == a)
    }
}
