// EntitlementRecord.swift
// playhead-5c1t — wire-format representation of a user's premium
// entitlement for iCloud sync via CloudKit.
//
// Conflict-resolution rule: GRANT-WINS. Once any device has flipped the
// entitlement to granted, it stays granted on every device. The user
// paid; the user paid forever (modulo a real StoreKit revocation, which
// flows through `Transaction.revocationDate` not CloudKit).
//
// CloudKit acts as a backstop here. StoreKit + Apple ID is already the
// primary cross-device unlock channel; the CloudKit record exists so a
// device that is briefly unable to reach the App Store still sees the
// unlock land via the iCloud push channel.

import CloudKit
import Foundation

struct EntitlementRecord: Equatable, Sendable {
    static let recordType: String = "Entitlement"

    enum Field {
        static let productID = "productID"
        static let isGranted = "isGranted"
        static let grantedAt = "grantedAt"
        static let sourceDeviceID = "sourceDeviceID"
    }

    var productID: String
    var isGranted: Bool
    /// When the grant was first recorded by the originating device. For
    /// not-granted records this is the lastModified-equivalent. Once any
    /// device has flipped granted=true, that earliest grantedAt is
    /// preserved across merges so the trust signal is stable.
    var grantedAt: Date
    /// Opaque device identifier of the device that originated this
    /// record. Diagnostic only — never displayed to the user.
    var sourceDeviceID: String

    enum DecodeError: Error, Equatable {
        case missingField(String)
    }

    /// Per-product CKRecord ID. The whole entitlement model is a single
    /// row (one product), so the record name is constant. If a future
    /// product joins the lineup this must include a per-product key.
    static func recordID(forProductID productID: String) -> CKRecord.ID {
        CKRecord.ID(recordName: "ent_\(productID)")
    }

    func toCKRecord() -> CKRecord {
        let r = CKRecord(
            recordType: Self.recordType,
            recordID: Self.recordID(forProductID: productID)
        )
        r[Field.productID] = productID as NSString
        r[Field.isGranted] = (isGranted ? 1 : 0) as NSNumber
        r[Field.grantedAt] = grantedAt as NSDate
        r[Field.sourceDeviceID] = sourceDeviceID as NSString
        return r
    }

    init(ckRecord: CKRecord) throws {
        guard let productID = ckRecord[Field.productID] as? String else {
            throw DecodeError.missingField(Field.productID)
        }
        guard let grantedAt = ckRecord[Field.grantedAt] as? Date else {
            throw DecodeError.missingField(Field.grantedAt)
        }
        let isGranted = (ckRecord[Field.isGranted] as? NSNumber)?.boolValue ?? false
        let source = (ckRecord[Field.sourceDeviceID] as? String) ?? ""
        self.init(
            productID: productID,
            isGranted: isGranted,
            grantedAt: grantedAt,
            sourceDeviceID: source
        )
    }

    init(productID: String, isGranted: Bool, grantedAt: Date, sourceDeviceID: String) {
        self.productID = productID
        self.isGranted = isGranted
        self.grantedAt = grantedAt
        self.sourceDeviceID = sourceDeviceID
    }

    /// Grant-wins merge. If either side is granted, the merged record is
    /// granted with the earliest known `grantedAt` (stable trust signal).
    /// When both sides are not-granted the most recently-modified copy
    /// wins (using `grantedAt` as the lastModified-equivalent for
    /// not-granted records, per the field doc).
    ///
    /// Tie-break on identical `grantedAt` (common after CloudKit's
    /// millisecond-precision round-trip): the record with the
    /// lexicographically smaller `sourceDeviceID` wins. This makes the
    /// merge fully deterministic across devices, preventing
    /// `sourceDeviceID` from oscillating in diagnostics. The tie-break
    /// applies symmetrically to both the (true, true) and (false,
    /// false) branches — anything else leaves diagnostics unstable.
    static func merge(local: EntitlementRecord, remote: EntitlementRecord) -> EntitlementRecord {
        switch (local.isGranted, remote.isGranted) {
        case (true, true):
            if local.grantedAt < remote.grantedAt { return local }
            if remote.grantedAt < local.grantedAt { return remote }
            return local.sourceDeviceID <= remote.sourceDeviceID ? local : remote
        case (true, false):
            return local
        case (false, true):
            return remote
        case (false, false):
            // Both are not-granted: pick the most recently-modified
            // record (via `grantedAt`-as-lastModified). Ties broken
            // lexicographically on `sourceDeviceID` for determinism.
            if local.grantedAt > remote.grantedAt { return local }
            if remote.grantedAt > local.grantedAt { return remote }
            return local.sourceDeviceID <= remote.sourceDeviceID ? local : remote
        }
    }
}
