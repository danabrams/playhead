// DiagnosticsBundleMusicBedProfilesTests.swift
// playhead-2hpn: cover the `music_bed_profiles` projection on the
// support-safe diagnostics default bundle. Verifies:
//
//   * The bundle's `musicBedProfiles` array is empty by default (no
//     show snapshots passed in).
//   * Each snapshot is projected into a `MusicBedProfileSummary` with:
//       - the raw `showIdentifier` replaced by a 64-char SHA-256 hex
//         derived from `installID || showIdentifier` (legal checklist
//         item a — never the raw catalogue/feed identifier).
//       - confirmation/miss/storedHash counts forwarded verbatim.
//       - `is_confirmed` reflecting the snapshot's derived state.
//       - `version_stamp` forwarded so consumers can detect drift.
//   * The raw 64-bit jingle hash bits are NEVER serialised in the
//     bundle JSON — only the COUNT is exposed.
//   * Decoding a legacy bundle JSON without the new key produces an
//     empty array (decodeIfPresent default).

import Foundation
import Testing

@testable import Playhead

@Suite("DiagnosticsBundle — music_bed_profiles (playhead-2hpn)")
struct DiagnosticsBundleMusicBedProfilesTests {

    private static let installID = UUID(uuidString: "11111111-1111-1111-1111-111111111111")!
    private static let now = Date(timeIntervalSince1970: 1_700_000_000)

    private static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: now
    )

    @Test("defaults to empty when no snapshots supplied")
    func defaultsEmpty() {
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0",
            osVersion: "iOS 26",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: Self.eligible,
            workJournalEntries: [],
            installID: Self.installID
        )
        #expect(bundle.musicBedProfiles.isEmpty)
    }

    @Test("snapshot fields project verbatim (counts, is_confirmed, version)")
    func snapshotFieldsForwarded() {
        let hash = RepeatedAdFingerprint(bits: 0xDEAD_BEEF_DEAD_BEEF)
        let snapshot = ShowMusicBedProfileSnapshot(
            showIdentifier: "https://example.com/show.xml",
            confirmedJingleHashes: [hash, .zero],
            confirmationCount: 4,
            consecutiveMissCount: 0,
            versionStamp: ShowMusicBedProfile.currentVersionStamp,
            createdAt: Self.now,
            updatedAt: Self.now
        )
        #expect(snapshot.isConfirmed == true,
                "Sanity: this snapshot must be marked confirmed for the test below to be meaningful")

        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0",
            osVersion: "iOS 26",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: Self.eligible,
            workJournalEntries: [],
            installID: Self.installID,
            musicBedProfileSnapshots: [snapshot]
        )
        #expect(bundle.musicBedProfiles.count == 1)
        let summary = bundle.musicBedProfiles[0]
        #expect(summary.confirmationCount == 4)
        #expect(summary.consecutiveMissCount == 0)
        #expect(summary.storedHashCount == 2)
        #expect(summary.isConfirmed == true)
        #expect(summary.versionStamp == ShowMusicBedProfile.currentVersionStamp)
    }

    @Test("show identifier is salted-hashed; raw feed URL never appears")
    func showIdentifierIsHashed() throws {
        let rawId = "https://example.com/show.xml"
        let snapshot = ShowMusicBedProfileSnapshot(
            showIdentifier: rawId,
            confirmedJingleHashes: [],
            confirmationCount: 0,
            consecutiveMissCount: 0,
            versionStamp: 1,
            createdAt: Self.now,
            updatedAt: Self.now
        )
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0",
            osVersion: "iOS 26",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: Self.eligible,
            workJournalEntries: [],
            installID: Self.installID,
            musicBedProfileSnapshots: [snapshot]
        )
        let summary = try #require(bundle.musicBedProfiles.first)
        let expectedHash = EpisodeIdHasher.hash(installID: Self.installID, episodeId: rawId)
        #expect(summary.showIdentifierHash == expectedHash,
                "show_identifier_hash MUST equal EpisodeIdHasher(installID, showIdentifier)")
        #expect(summary.showIdentifierHash != rawId,
                "Raw catalog identifier MUST NOT appear in the bundle")
    }

    @Test("raw jingle hash bits never appear in encoded JSON")
    func rawHashBitsNeverEncoded() throws {
        let hash = RepeatedAdFingerprint(bits: 0xCAFE_BABE_DEAD_BEEF)
        let snapshot = ShowMusicBedProfileSnapshot(
            showIdentifier: "https://example.com/show.xml",
            confirmedJingleHashes: [hash],
            confirmationCount: 1,
            consecutiveMissCount: 0,
            versionStamp: 1,
            createdAt: Self.now,
            updatedAt: Self.now
        )
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0",
            osVersion: "iOS 26",
            deviceClass: .iPhone17Pro,
            buildType: .debug,
            eligibility: Self.eligible,
            workJournalEntries: [],
            installID: Self.installID,
            musicBedProfileSnapshots: [snapshot]
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(bundle)
        let jsonString = String(decoding: data, as: UTF8.self)
        // Various string forms the bit pattern could appear as:
        #expect(!jsonString.contains("0xCAFE"),
                "Raw hash bits must not appear as hex literal")
        // Decimal form of the UInt64 value:
        #expect(!jsonString.contains("\(hash.bits)"),
                "Raw hash bits must not appear as decimal literal")
        // Decimal form of the signed Int64 storage (in case any serialisation
        // path leaked the persisted form):
        #expect(!jsonString.contains("\(Int64(bitPattern: hash.bits))"),
                "Raw hash bits must not appear as signed-decimal literal")
        // But the count IS exposed.
        #expect(jsonString.contains("\"stored_hash_count\":1"))
    }

    @Test("legacy JSON without music_bed_profiles decodes as empty array")
    func legacyJsonDecodesAsEmpty() throws {
        // Synthetic pre-2hpn default bundle that omits the new key.
        let json = """
        {
          "app_version": "1.0",
          "os_version": "iOS 26",
          "device_class": "iPhone17Pro",
          "build_type": "debug",
          "eligibility_snapshot": {
            "hardwareSupported": true,
            "appleIntelligenceEnabled": true,
            "regionSupported": true,
            "languageSupported": true,
            "modelAvailableNow": true,
            "capturedAt": 1700000000
          },
          "scheduler_events": [],
          "work_journal_tail": []
        }
        """
        let data = Data(json.utf8)
        let decoded = try JSONDecoder().decode(DefaultBundle.self, from: data)
        #expect(decoded.musicBedProfiles.isEmpty,
                "Legacy bundle without the key MUST decode as empty array")
        #expect(decoded.chapterPhaseEvents.isEmpty,
                "Sanity: pre-au2v.1.3 bundle still decodes as well")
    }
}
