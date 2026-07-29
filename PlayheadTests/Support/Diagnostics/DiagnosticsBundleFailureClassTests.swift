// DiagnosticsBundleFailureClassTests.swift
// playhead-8ysk part 2 — the failure class has to SURVIVE the projection.
//
// Naming the failure inside the app is worthless if the name cannot leave the
// device in a support bundle. `runner_reason` already reached SQLite before
// this bead and was still undiagnosable, because `DiagnosticsBundle` drops
// `metadata` wholesale — correctly, since callers stash arbitrary JSON there.
//
// So `failure_class` and `failure_code` are RECOVERED from metadata rather
// than forwarded from it, and the recovery is a whitelist strong enough to
// stand on its own:
//
//   * `failure_class` is admitted only if it round-trips through
//     `TranscriptFailureClass(rawValue:)` — a set of compile-time literals.
//     A future emitter that writes an error message, a URL or an episode
//     title under that key exports NOTHING.
//   * `failure_code` is parsed to an `Int`, which cannot carry PII at all.
//
// That is playhead-p70f's shape: its free-form `lastDetail` (which can carry
// an enclosure URL) is not projected, and the run ledger's free-form
// annotation is parsed into integers rather than shipped.
//
// This file is the adversarial half. `DiagnosticsBundleShapeTests` remains
// the legal-checklist guard on the bundle's key set.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-8ysk — failure_class survives the bundle projection, free text does not")
struct DiagnosticsBundleFailureClassTests {

    private static let installID = UUID(uuidString: "22222222-2222-4222-8222-222222222222")!
    private static let t0: Double = 1_700_000_000

    private static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: Date(timeIntervalSince1970: t0)
    )

    private static func entry(metadata: String) -> WorkJournalEntry {
        WorkJournalEntry(
            id: UUID().uuidString,
            episodeId: "ep-8ysk",
            generationID: UUID(),
            schedulerEpoch: 3,
            timestamp: t0,
            eventType: .failed,
            cause: .asrFailed,
            metadata: metadata,
            artifactClass: .scratch
        )
    }

    private static func tailRecord(metadata: String) throws -> DefaultBundle.WorkJournalRecord {
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 27", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: eligible,
            workJournalEntries: [entry(metadata: metadata)], installID: installID
        )
        return try #require(bundle.workJournalTail.first)
    }

    // MARK: - It survives

    @Test("a real emitted row carries its failure class and code into the tail")
    func failureClassSurvives() throws {
        let metadata = """
            {"slice_duration_ms":4,"bytes_processed":0,"shards_completed":0,\
            "device_class":"iPhone17Pro","stage":"analysisJobRunner.run.transcriptionTimeout",\
            "failure_class":"silent_shard","failure_code":"1101","failed_shard_count":"97"}
            """
        let record = try Self.tailRecord(metadata: metadata)
        #expect(record.failureClass == TranscriptFailureClass.silentShard.rawValue)
        #expect(record.failureCode == 1101)
        // The pre-existing contract still holds: `cause` alone was all a
        // support engineer used to get.
        #expect(record.cause == InternalMissCause.asrFailed.rawValue)
    }

    /// Every variant has to make it across, not just the one the fixture
    /// happens to use — a hand-written allowlist in the builder would pass a
    /// single-value test and silently drop the other fifteen.
    @Test("every TranscriptFailureClass variant round-trips through the projection")
    func everyVariantRoundTrips() throws {
        for failureClass in TranscriptFailureClass.allCases {
            let record = try Self.tailRecord(
                metadata: #"{"failure_class":"\#(failureClass.rawValue)"}"#
            )
            #expect(record.failureClass == failureClass.rawValue,
                    "\(failureClass.rawValue) did not survive the projection")
        }
    }

    @Test("a row with no failure metadata reports absent, not empty")
    func absentFailureIsNil() throws {
        let record = try Self.tailRecord(metadata: "{}")
        #expect(record.failureClass == nil)
        #expect(record.failureCode == nil)
    }

    // MARK: - Free text does not

    /// THE ADVERSARIAL CASE, and the reason the projection validates rather
    /// than forwards. A future emitter — or a careless one — putting a raw
    /// message under `failure_class` must export nothing at all. If this ever
    /// starts passing the string through, an episode title, a feed URL or a
    /// file path can reach a support bundle through a key that was reviewed
    /// as a closed vocabulary.
    @Test(
        "anything outside the vocabulary is dropped, not forwarded",
        arguments: [
            "The operation couldn’t be completed. (kAFAssistantErrorDomain error 1101.)",
            "https://traffic.megaphone.fm/some-show/episode-1234.mp3",
            "/var/mobile/Containers/Data/Application/ABC/Documents/ep.mp3",
            "Diary of a CEO — Episode 412",
            "silent_shard ",
            "SILENT_SHARD",
            "",
        ]
    )
    func freeTextIsDropped(text: String) throws {
        let encoded = try #require(
            String(data: try JSONEncoder().encode(["failure_class": text]), encoding: .utf8)
        )
        let record = try Self.tailRecord(metadata: encoded)
        #expect(record.failureClass == nil,
                "'\(text)' escaped the closed vocabulary")
    }

    /// And nothing of it reaches the encoded bundle either — the field being
    /// nil in the struct is only half the claim.
    @Test("rejected text appears nowhere in the encoded bundle JSON")
    func rejectedTextIsAbsentFromEncodedJSON() throws {
        let secret = "Diary of a CEO — Episode 412"
        let encoded = try #require(
            String(data: try JSONEncoder().encode(["failure_class": secret]), encoding: .utf8)
        )
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 27", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: [Self.entry(metadata: encoded)], installID: Self.installID
        )
        let json = try #require(String(data: try JSONEncoder().encode(bundle), encoding: .utf8))
        #expect(!json.contains(secret))
        #expect(!json.contains("Episode 412"))
    }

    /// THE DURATION PROXY MUST NOT SHIP (review r3).
    ///
    /// `failed_shard_count` is written to `work_journal.metadata` for on-device
    /// forensics and deliberately not projected: in a total failure it equals
    /// the shard count, which discloses the episode's duration to ±30 s. That
    /// is a usable fingerprint in a bundle that goes to the trouble of hashing
    /// episode ids, and re-identifying an episode by duration against a public
    /// feed is not hard.
    ///
    /// Review r2 exported it, then noticed and reverted — which is exactly why
    /// this belongs in a test rather than in a comment. `WorkJournalRecord`
    /// being a fixed-field `Codable` is a real barrier, but it is one edit
    /// away from not being one, and the round-trip test asserts only that the
    /// WRITE side emits the key. This asserts the property that actually
    /// matters: the value does not appear in the bytes that leave the device.
    ///
    /// Per the standing correction that `DiagnosticsBundleShapeTests` is not
    /// the legal guard here — it passed while an episode title leaked — the
    /// check is against the ENCODED JSON, not against the struct.
    @Test("failed_shard_count reaches the journal but never the encoded bundle")
    func failedShardCountIsNotExported() throws {
        // Two runs identical in every respect EXCEPT the shard count — a
        // 48-minute episode and a 5-hour one. Asserting the encoded bundles are
        // byte-identical is the precise form of "the count did not leak":
        // a bare substring search for the number cannot work, because any small
        // integer also occurs inside hashes and timestamps, and that noise is
        // what makes such a test either flaky or vacuous.
        //
        // Everything else is pinned to a constant so the only free variable is
        // the one under test — `Self.entry` mints a fresh `id`/`generationID`
        // per call, which would defeat the comparison.
        func bundleJSON(failedShardCount: Int) throws -> String {
            let extras = AnalysisJobRunner.failureExtras(
                TranscriptFailureReason(
                    failureClass: .silentShard, code: 1107,
                    failedShardCount: failedShardCount
                )
            )
            #expect(
                extras[DiagnosticsFailureKeys.failedShardCount] == String(failedShardCount),
                "the write side must still record it on-device, or this test guards nothing"
            )
            let entry = WorkJournalEntry(
                id: "fixed-journal-id",
                episodeId: "ep-8ysk",
                generationID: UUID(uuidString: "00000000-0000-0000-0000-0000000008AC")!,
                schedulerEpoch: 3,
                timestamp: Self.t0,
                eventType: .failed,
                cause: .asrFailed,
                metadata: String(data: try JSONEncoder().encode(extras), encoding: .utf8)!,
                artifactClass: .scratch
            )
            let bundle = DiagnosticsBundleBuilder.buildDefault(
                appVersion: "1.0", osVersion: "iOS 27", deviceClass: .iPhone17Pro,
                buildType: .debug, eligibility: Self.eligible,
                workJournalEntries: [entry], installID: Self.installID
            )
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.sortedKeys]
            return String(data: try encoder.encode(bundle), encoding: .utf8)!
        }

        let short = try bundleJSON(failedShardCount: 97)     // ~48 min
        let long = try bundleJSON(failedShardCount: 601)     // ~5 h

        #expect(
            short == long,
            """
            the exported bundle varies with the shard count, so it carries the \
            episode's duration to ±30 s — from a bundle that hashes episode ids \
            precisely to prevent that identification
            """
        )
        #expect(
            !short.contains(DiagnosticsFailureKeys.failedShardCount),
            "the key itself must not appear in the exported bundle"
        )

        // The positive control: what IS meant to cross did cross, so a bundle
        // that simply dropped everything would satisfy the checks above.
        #expect(short.contains(TranscriptFailureClass.silentShard.rawValue))
        #expect(short.contains("\"failure_code\":1107"))
    }

    /// A non-numeric code is dropped for the same reason. `failure_code` is
    /// declared as an integer precisely so it cannot carry text.
    @Test("a non-numeric failure_code is dropped")
    func nonNumericCodeIsDropped() throws {
        let metadata = #"{"failure_class":"model_not_loaded","failure_code":"ep-user-12345"}"#
        let record = try Self.tailRecord(metadata: metadata)
        #expect(record.failureClass == TranscriptFailureClass.modelNotLoaded.rawValue)
        #expect(record.failureCode == nil)
    }

    @Test("malformed metadata degrades to absent rather than throwing")
    func malformedMetadataIsSafe() throws {
        for metadata in ["", "not json", "[1,2,3]", "null"] {
            let record = try Self.tailRecord(metadata: metadata)
            #expect(record.failureClass == nil)
            #expect(record.failureCode == nil)
        }
    }

    // MARK: - The write side, joined to the read side

    /// The two halves live in different files and different layers, and the
    /// whole mechanism fails silently if they disagree about a key name — the
    /// symptom would be bundles quietly reverting to "`asr_failed`, cause
    /// unknown", which is the condition this bead exists to end. So the
    /// emitter's own output is fed through the real projection here rather
    /// than asserted against hand-written key strings.
    @Test("what the runner writes is exactly what the projection recovers")
    func writeSideRoundTripsThroughTheProjection() throws {
        let failure = TranscriptFailureReason(
            failureClass: .analyzerFormatUnavailable, code: 1101, failedShardCount: 12
        )
        let extras = AnalysisJobRunner.failureExtras(failure)
        let encoded = try #require(
            String(data: try JSONEncoder().encode(extras), encoding: .utf8)
        )
        let record = try Self.tailRecord(metadata: encoded)
        #expect(record.failureClass == TranscriptFailureClass.analyzerFormatUnavailable.rawValue)
        #expect(record.failureCode == 1101)
        // The shard count rides along in metadata for on-device forensics but
        // is deliberately NOT projected — it is a count of a private
        // episode's structure, and the class plus the code are what a support
        // engineer needs.
        #expect(extras[DiagnosticsFailureKeys.failedShardCount] == "12")
    }

    /// A Swift-native error has no meaningful code, and the emitter must omit
    /// the key rather than write a placeholder — a `failure_code` of 0 would
    /// read as a real framework code in an export.
    @Test("a failure with no code omits the key entirely")
    func absentCodeOmitsTheKey() throws {
        let extras = AnalysisJobRunner.failureExtras(
            TranscriptFailureReason(failureClass: .modelNotLoaded, code: nil, failedShardCount: 3)
        )
        #expect(extras[DiagnosticsFailureKeys.failureCode] == nil)
        #expect(extras[DiagnosticsFailureKeys.failureClass]
                == TranscriptFailureClass.modelNotLoaded.rawValue)
    }

    @Test("no failure writes no failure keys at all")
    func noFailureWritesNothing() {
        #expect(AnalysisJobRunner.failureExtras(nil).isEmpty)
    }

    // MARK: - playhead-ngev: the field that is present when the class is not

    /// THE OVERLOADED BLANK. A missing `failure_class` meant any of four
    /// unrelated things — cancelled by playback, torn down, `.completed` over
    /// an empty transcript, or nothing heard for five minutes — and a support
    /// engineer reading the bundle saw one empty column for all four. The
    /// observation is written on every zero-coverage row, so the blank is
    /// always accompanied by a statement of what the runner itself saw.
    @Test("every observation variant round-trips through the projection")
    func everyObservationRoundTrips() throws {
        for observation in AnalysisJobRunner.TranscriptRunObservation.allCases {
            let record = try Self.tailRecord(
                metadata: #"{"failure_observation":"\#(observation.rawValue)"}"#
            )
            #expect(record.failureObservation == observation.rawValue,
                    "\(observation.rawValue) did not survive the projection")
        }
    }

    @Test("every termination variant round-trips through the projection")
    func everyTerminationRoundTrips() throws {
        for termination in TranscriptRunTermination.allCases {
            let record = try Self.tailRecord(
                metadata: #"{"failure_termination":"\#(termination.rawValue)"}"#
            )
            #expect(record.failureTermination == termination.rawValue,
                    "\(termination.rawValue) did not survive the projection")
        }
    }

    /// A row that carries no observation reports absent, so an old bundle and
    /// a new one are not confusable.
    @Test("a row with no observation reports absent, not a default")
    func absentObservationIsNil() throws {
        let record = try Self.tailRecord(metadata: "{}")
        #expect(record.failureObservation == nil)
        #expect(record.failureTermination == nil)
    }

    /// THE ADVERSARIAL CASE FOR THE NEW KEYS, and the reason they are admitted
    /// by round-trip rather than forwarded. The legal checklist does NOT
    /// inspect nested record keys — it passed while an episode title leaked
    /// into encoded JSON — so this test is the enforcement, not the audit.
    @Test(
        "free text under the new keys is dropped, not forwarded",
        arguments: [
            "The operation couldn’t be completed. (kAFAssistantErrorDomain error 1101.)",
            "https://traffic.megaphone.fm/some-show/episode-1234.mp3",
            "/var/mobile/Containers/Data/Application/ABC/Documents/ep.mp3",
            "Diary of a CEO — Episode 412",
            "engine_reported ",
            "ENGINE_REPORTED",
            "interrupted!",
            "",
        ]
    )
    func freeTextUnderNewKeysIsDropped(text: String) throws {
        let encoded = try #require(
            String(
                data: try JSONEncoder().encode([
                    "failure_observation": text,
                    "failure_termination": text,
                ]),
                encoding: .utf8
            )
        )
        let record = try Self.tailRecord(metadata: encoded)
        #expect(record.failureObservation == nil, "'\(text)' escaped the observation vocabulary")
        #expect(record.failureTermination == nil, "'\(text)' escaped the termination vocabulary")
    }

    /// And none of it reaches the encoded bytes either — the struct field
    /// being nil is only half the claim, per the standing correction that
    /// `DiagnosticsBundleShapeTests` is not the legal guard here.
    @Test("text rejected from the new keys appears nowhere in the encoded bundle")
    func rejectedNewKeyTextIsAbsentFromEncodedJSON() throws {
        let secret = "Diary of a CEO — Episode 412"
        let encoded = try #require(
            String(
                data: try JSONEncoder().encode([
                    "failure_observation": secret,
                    "failure_termination": secret,
                ]),
                encoding: .utf8
            )
        )
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0", osVersion: "iOS 27", deviceClass: .iPhone17Pro,
            buildType: .debug, eligibility: Self.eligible,
            workJournalEntries: [Self.entry(metadata: encoded)], installID: Self.installID
        )
        let json = try #require(String(data: try JSONEncoder().encode(bundle), encoding: .utf8))
        #expect(!json.contains(secret))
        #expect(!json.contains("Episode 412"))
    }

    /// The write side joined to the read side for the termination, the same
    /// way `writeSideRoundTripsThroughTheProjection` does for the class. The
    /// two halves live in different files; a key renamed on one side would
    /// otherwise fail silently, and the symptom would be bundles quietly
    /// losing the only field that says a scrub ended the run.
    @Test("what the runner writes for an interrupted run is what the projection recovers")
    func interruptedWriteSideRoundTrips() throws {
        let extras = AnalysisJobRunner.failureExtras(
            TranscriptFailureReason(
                failureClass: .modelNotLoaded, code: nil, failedShardCount: 4,
                termination: .interrupted
            )
        )
        #expect(extras[DiagnosticsFailureKeys.failureTermination]
                == TranscriptRunTermination.interrupted.rawValue)
        let encoded = try #require(
            String(data: try JSONEncoder().encode(extras), encoding: .utf8)
        )
        let record = try Self.tailRecord(metadata: encoded)
        #expect(record.failureClass == TranscriptFailureClass.modelNotLoaded.rawValue)
        #expect(record.failureTermination == TranscriptRunTermination.interrupted.rawValue)
    }

    /// The default is written too, not omitted — otherwise "this run finished"
    /// and "this build predates the field" would look alike.
    @Test("a run that reached its own conclusion says so explicitly")
    func concludedRunWritesItsTermination() {
        let extras = AnalysisJobRunner.failureExtras(
            TranscriptFailureReason(failureClass: .vadFailed, failedShardCount: 2)
        )
        #expect(extras[DiagnosticsFailureKeys.failureTermination]
                == TranscriptRunTermination.ranToConclusion.rawValue)
    }
}
