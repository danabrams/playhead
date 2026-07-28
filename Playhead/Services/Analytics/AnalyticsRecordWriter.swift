// AnalyticsRecordWriter.swift
// playhead-jw63.3 — the only outbound path, and the flag that keeps it shut.
//
// Transport is the CloudKit **public** database: write-only, anonymous,
// no user record types, no queries from the device, no subscriptions, no
// push. Nothing here reads anything back — a read path is how an analytics
// system becomes a profile store.
//
// One thing the application layer does NOT control, stated plainly because
// it qualifies the "unlinkable" claim: CloudKit stamps every
// public-database record with server-side system metadata including
// `creatorUserRecordID`, derived from the writer's iCloud account. Our
// payload carries no identifier and our record names are fresh UUIDs, but
// the platform's own metadata does link records written by one account.
// This is the same class of unavoidable transport-layer metadata as
// envelope §4.3's treatment of IP addresses. Two consequences, both
// deliberate: the rollup (`scripts/analytics-rollup.sh`) reads only the
// nine counter fields and never a system field, and the question is put to
// counsel explicitly in Addendum A §5. If counsel is not satisfied, the
// answer is a different transport, not a different payload — which is why
// `AnalyticsRecordWriting` is a one-method protocol.
//
// The gate below is not a feature flag. `docs/legal/telemetry-envelope-v1.md`
// §7 requires that until counsel signs the envelope, downstream enforcers
// "treat this envelope as a draft and gate all non-local transport paths
// behind an internal-only build flag". The five product counters also need
// the addendum in
// `docs/legal/telemetry-envelope-v1-addendum-a-product-counters.md` signed,
// because they are not on the v1 §2 allow-list. So: counters accumulate
// locally today, and nothing is sent. Flipping `legalSignoffRecorded` is a
// deliberate, reviewable one-line change that should happen in the same
// commit that records the signoff in the addendum.

import CloudKit
import Foundation

// MARK: - Legal gate

enum AnalyticsUploadGate {
    /// Set to `true` only when the addendum carries counsel's signature.
    /// Engineering must not self-sign — see envelope §7.
    static let legalSignoffRecorded = false

    /// Whether automatic upload may run at all in this process.
    static var isAutomaticUploadPermitted: Bool {
        legalSignoffRecorded && !isRunningUnderXCTest
    }

    static var isRunningUnderXCTest: Bool {
        let environment = ProcessInfo.processInfo.environment
        return environment["XCTestConfigurationFilePath"] != nil
            || environment["XCTestSessionIdentifier"] != nil
            || NSClassFromString("XCTestCase") != nil
    }
}

// MARK: - Writer

/// Write-only sink for validated increment records.
protocol AnalyticsRecordWriting: Sendable {
    /// Sends every record, or throws. A throw means "not accepted" and the
    /// caller must not advance its watermark.
    func write(_ records: [[String: AnalyticsFieldValue]]) async throws
}

enum AnalyticsWriterError: Error, Equatable {
    /// Upload is not permitted in this build (legal gate, or under test).
    case uploadNotPermitted
    /// A record failed envelope validation on the way out.
    case recordRejectedByEnvelope
}

/// The no-op writer wired in every build that has not had counsel signoff,
/// and in tests. It never touches CloudKit — constructing a `CKContainer`
/// in an unsigned test host traps, and a disabled path must not be able to
/// trap.
struct DisabledAnalyticsRecordWriter: AnalyticsRecordWriting {
    func write(_: [[String: AnalyticsFieldValue]]) async throws {
        throw AnalyticsWriterError.uploadNotPermitted
    }
}

/// CloudKit public-database writer.
struct CloudKitPublicAnalyticsWriter: AnalyticsRecordWriting {
    static let recordType = "AnalyticsIncrement"
    static let defaultContainerIdentifier = "iCloud.com.playhead.app"

    private let containerIdentifier: String

    init(containerIdentifier: String = CloudKitPublicAnalyticsWriter.defaultContainerIdentifier) {
        self.containerIdentifier = containerIdentifier
    }

    func write(_ records: [[String: AnalyticsFieldValue]]) async throws {
        let database = CKContainer(identifier: containerIdentifier).publicCloudDatabase
        for fields in records {
            try await database.save(Self.makeRecord(fields: fields))
        }
    }

    /// Materializes a `CKRecord` from an already-validated field set.
    ///
    /// Re-validates first. The re-check is not paranoia theatre: this is the
    /// last point at which a field can be inspected, and the cost of the
    /// check is nothing against the cost of being wrong. A record name is a
    /// fresh UUID with no relationship to the device — records are never
    /// updated, only appended, so nothing needs to find them again.
    static func makeRecord(fields: [String: AnalyticsFieldValue]) throws -> CKRecord {
        guard let validated = TelemetryEnvelopeV1AllowList.validate(fields) else {
            throw AnalyticsWriterError.recordRejectedByEnvelope
        }
        let record = CKRecord(
            recordType: recordType,
            recordID: CKRecord.ID(recordName: UUID().uuidString)
        )
        for (key, value) in validated {
            switch value {
            case .integer(let number): record[key] = number
            case .token(let token):    record[key] = token
            }
        }
        return record
    }
}
