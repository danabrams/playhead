// CorpusAnnotationTests.swift
// Tests for the playhead-l2f labeled-corpus schema, validator, and
// fingerprint helper. Exercises:
//   - Round-trip Codable encode/decode.
//   - Validator catches overlap, gap, off-by-one, range, and
//     fingerprint-prefix problems.
//   - Streaming SHA-256 fingerprint is deterministic and chunk-independent.
//   - Loader scans the on-disk annotations directory and excludes
//     templates by filename convention.

import CryptoKit
import Darwin
import Foundation
import Testing
@testable import Playhead

private final class CorpusThreadOutcome<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var result: Result<Value, Error>?

    func store(_ result: Result<Value, Error>) {
        lock.lock()
        self.result = result
        lock.unlock()
    }

    func resolve() throws -> Value {
        lock.lock()
        defer { lock.unlock() }
        return try #require(result).get()
    }
}

// MARK: - Schema Round-trip

@Suite("CorpusAnnotation – Codable round-trip")
struct CorpusAnnotationRoundTripTests {

    @Test("Encodes and decodes a complete annotation losslessly")
    func roundTrip() throws {
        let original = makeWellFormedAnnotation()
        let data = try CorpusAnnotation.encoder.encode(original)
        let decoded = try CorpusAnnotation.decoder.decode(CorpusAnnotation.self, from: data)
        #expect(decoded == original)
    }

    @Test("Decodes the JSON shape from the bead specification")
    func decodesBeadSpecJSON() throws {
        let json = """
        {
          "episode_id": "corpus-001",
          "show_name": "Example Podcast",
          "duration_seconds": 600,
          "ad_windows": [
            {
              "start_seconds": 180.0,
              "end_seconds": 240.0,
              "advertiser": "Squarespace",
              "product": "Website builder",
              "ad_type": "host_read",
              "transition_type": "explicit",
              "confidence_notes": "Clear brought to you by intro"
            }
          ],
          "content_windows": [
            {
              "start_seconds": 0.0,
              "end_seconds": 180.0,
              "notes": "Interview content — must NEVER be skipped"
            },
            {
              "start_seconds": 240.0,
              "end_seconds": 600.0,
              "notes": "Post-ad content"
            }
          ],
          "variant_of": null,
          "audio_fingerprint": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        }
        """
        let data = json.data(using: .utf8)!
        let decoded = try CorpusAnnotation.decoder.decode(CorpusAnnotation.self, from: data)
        #expect(decoded.episodeId == "corpus-001")
        #expect(decoded.showName == "Example Podcast")
        #expect(decoded.durationSeconds == 600)
        #expect(decoded.adWindows.count == 1)
        #expect(decoded.adWindows[0].adType == .hostRead)
        #expect(decoded.adWindows[0].transitionType == .explicit)
        #expect(decoded.contentWindows.count == 2)
        #expect(decoded.variantOf == nil)
        #expect(decoded.audioFingerprint == "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
    }

    @Test("AdWindow.duration computes correctly")
    func adWindowDuration() {
        let w = CorpusAnnotation.AdWindow(
            startSeconds: 100,
            endSeconds: 175.5,
            advertiser: nil,
            product: nil,
            adType: .promo,
            transitionType: .blended,
            confidenceNotes: nil
        )
        #expect(w.durationSeconds == 75.5)
    }

    @Test("Preserves promotion metadata and unknown provenance losslessly")
    func promotionMetadataRoundTrip() throws {
        let json = """
        {
          "episode_id": "auto-1",
          "show_name": "Example Podcast",
          "duration_seconds": 120,
          "ad_windows": [{
            "start_seconds": 10,
            "end_seconds": 30,
            "advertiser": null,
            "product": null,
            "ad_type": "dai",
            "transition_type": null,
            "confidence_notes": "proposal",
            "auto_promoted": true,
            "auto_promoted_at": "2026-06-03T07:36:17Z",
            "auto_promoted_by": "future-window-tool",
            "provenance": ["rediff", "future_source"],
            "audit_priority": 1
          }],
          "content_windows": [
            {"start_seconds": 0, "end_seconds": 10, "notes": null},
            {"start_seconds": 30, "end_seconds": 120, "notes": null}
          ],
          "variant_of": null,
          "audio_fingerprint": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
          "auto_promoted": true,
          "auto_promoted_at": "2026-06-03T07:36:17Z",
          "auto_promoted_by": "scripts/l2f-auto-promote.py",
          "provenance": ["future_episode_source"],
          "audit_priority": 1
        }
        """

        let decoded = try CorpusAnnotation.decoder.decode(
            CorpusAnnotation.self,
            from: Data(json.utf8)
        )
        #expect(decoded.autoPromoted == true)
        #expect(decoded.autoPromotedAt == "2026-06-03T07:36:17Z")
        #expect(decoded.autoPromotedBy == "scripts/l2f-auto-promote.py")
        #expect(decoded.provenance == ["future_episode_source"])
        #expect(decoded.auditPriority == 1)
        #expect(decoded.adWindows[0].provenance == ["rediff", "future_source"])
        #expect(decoded.adWindows[0].autoPromotedAt == "2026-06-03T07:36:17Z")
        #expect(decoded.adWindows[0].autoPromotedBy == "future-window-tool")
        #expect(decoded.adWindows[0].labelTier == .boundaryProposal)
        #expect(decoded.labelTier(for: decoded.adWindows[0]) == .boundaryProposal)

        let roundTripped = try CorpusAnnotation.decoder.decode(
            CorpusAnnotation.self,
            from: CorpusAnnotation.encoder.encode(decoded)
        )
        #expect(roundTripped == decoded)
    }

    @Test("Only explicit second-pass labels are gold")
    func derivedLabelTiers() {
        let legacy = makeWellFormedAnnotation()
        #expect(legacy.hasVerifiedReviewAttestations)
        #expect(legacy.labelTier == .gold)
        #expect(legacy.isEligibleForGoldEvaluation)
        #expect(legacy.adWindows.allSatisfy { legacy.labelTier(for: $0) == .gold })

        let missingProvenance = CorpusAnnotation(
            episodeId: legacy.episodeId,
            showName: legacy.showName,
            durationSeconds: legacy.durationSeconds,
            adWindows: legacy.adWindows.map {
                .init(
                    startSeconds: $0.startSeconds,
                    endSeconds: $0.endSeconds,
                    advertiser: $0.advertiser,
                    product: $0.product,
                    adType: $0.adType,
                    transitionType: $0.transitionType,
                    confidenceNotes: $0.confidenceNotes
                )
            },
            contentWindows: legacy.contentWindows,
            variantOf: legacy.variantOf,
            audioFingerprint: legacy.audioFingerprint
        )
        #expect(missingProvenance.labelTier == .silver)
        #expect(!missingProvenance.isEligibleForGoldEvaluation)

        let oneArtifact = "sha256:" + String(repeating: "a", count: 64)
        let duplicateReviewer = CorpusAnnotation(
            episodeId: legacy.episodeId,
            showName: legacy.showName,
            durationSeconds: legacy.durationSeconds,
            adWindows: legacy.adWindows,
            contentWindows: legacy.contentWindows,
            variantOf: nil,
            audioFingerprint: legacy.audioFingerprint,
            provenance: ["human_reviewed"],
            reviewAttestations: [
                .init(reviewer: "Dan", reviewedAt: "2026-05-12T03:06:35Z",
                      audioFingerprint: legacy.audioFingerprint, reviewArtifactId: oneArtifact),
                .init(reviewer: "dan", reviewedAt: "2026-07-10T12:00:00Z",
                      audioFingerprint: legacy.audioFingerprint,
                      reviewArtifactId: "sha256:" + String(repeating: "b", count: 64)),
            ]
        )
        #expect(duplicateReviewer.labelTier == .silver)

        let unicodeEquivalentReviewer = CorpusAnnotation(
            episodeId: legacy.episodeId,
            showName: legacy.showName,
            durationSeconds: legacy.durationSeconds,
            adWindows: legacy.adWindows,
            contentWindows: legacy.contentWindows,
            variantOf: nil,
            audioFingerprint: legacy.audioFingerprint,
            provenance: ["human_reviewed"],
            reviewAttestations: [
                .init(reviewer: "Straße", reviewedAt: "2026-05-12T03:06:35Z",
                      audioFingerprint: legacy.audioFingerprint, reviewArtifactId: oneArtifact),
                .init(reviewer: "STRASSE", reviewedAt: "2026-07-10T12:00:00Z",
                      audioFingerprint: legacy.audioFingerprint,
                      reviewArtifactId: "sha256:" + String(repeating: "b", count: 64)),
            ]
        )
        #expect(unicodeEquivalentReviewer.labelTier == .silver)

        let duplicateArtifact = CorpusAnnotation(
            episodeId: legacy.episodeId,
            showName: legacy.showName,
            durationSeconds: legacy.durationSeconds,
            adWindows: legacy.adWindows,
            contentWindows: legacy.contentWindows,
            variantOf: nil,
            audioFingerprint: legacy.audioFingerprint,
            provenance: ["human_reviewed"],
            reviewAttestations: [
                .init(reviewer: "Dan", reviewedAt: "2026-05-12T03:06:35Z",
                      audioFingerprint: legacy.audioFingerprint, reviewArtifactId: oneArtifact),
                .init(reviewer: "Alex", reviewedAt: "2026-07-10T12:00:00Z",
                      audioFingerprint: legacy.audioFingerprint, reviewArtifactId: oneArtifact),
            ]
        )
        #expect(duplicateArtifact.labelTier == .silver)

        let silver = CorpusAnnotation.AdWindow(
            startSeconds: 10,
            endSeconds: 20,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil,
            autoPromoted: true,
            provenance: ["drafter", "rediff"],
            auditPriority: 3
        )
        #expect(silver.labelTier == .silver)

        let unknown = CorpusAnnotation.AdWindow(
            startSeconds: 10,
            endSeconds: 20,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil,
            provenance: ["future_source"]
        )
        #expect(unknown.labelTier == .silver)

        let firstListenerOnly = CorpusAnnotation.AdWindow(
            startSeconds: 10,
            endSeconds: 20,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil,
            provenance: ["human"]
        )
        #expect(firstListenerOnly.labelTier == .silver)

        let emptyProvenance = CorpusAnnotation.AdWindow(
            startSeconds: 10,
            endSeconds: 20,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil,
            provenance: []
        )
        #expect(emptyProvenance.labelTier == .silver)

        let toolMarkerOnly = CorpusAnnotation.AdWindow(
            startSeconds: 10,
            endSeconds: 20,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil,
            autoPromotedBy: "future-promotion-tool"
        )
        #expect(toolMarkerOnly.labelTier == .silver)

        let zeroPriorityMarker = CorpusAnnotation.AdWindow(
            startSeconds: 10,
            endSeconds: 20,
            advertiser: nil,
            product: nil,
            adType: .dai,
            transitionType: nil,
            confidenceNotes: nil,
            auditPriority: 0
        )
        #expect(zeroPriorityMarker.labelTier == .silver)

        let markerOnlyEpisode = CorpusAnnotation(
            episodeId: "marker-only",
            showName: "Marker only",
            durationSeconds: legacy.durationSeconds,
            adWindows: legacy.adWindows,
            contentWindows: legacy.contentWindows,
            variantOf: nil,
            audioFingerprint: legacy.audioFingerprint,
            autoPromotedBy: "future-promotion-tool"
        )
        #expect(markerOnlyEpisode.labelTier == .silver)
        #expect(!markerOnlyEpisode.isEligibleForGoldEvaluation)
        #expect(markerOnlyEpisode.labelTier(for: markerOnlyEpisode.adWindows[0]) == .silver)

        let zeroPriorityEpisode = CorpusAnnotation(
            episodeId: "priority-zero",
            showName: "Priority zero",
            durationSeconds: legacy.durationSeconds,
            adWindows: legacy.adWindows,
            contentWindows: legacy.contentWindows,
            variantOf: nil,
            audioFingerprint: legacy.audioFingerprint,
            auditPriority: 0
        )
        #expect(zeroPriorityEpisode.labelTier == .silver)
        #expect(!zeroPriorityEpisode.isEligibleForGoldEvaluation)

        let mixedEpisode = CorpusAnnotation(
            episodeId: "mixed",
            showName: "Mixed",
            durationSeconds: legacy.durationSeconds,
            adWindows: [legacy.adWindows[0], silver],
            contentWindows: legacy.contentWindows,
            variantOf: nil,
            audioFingerprint: legacy.audioFingerprint
        )
        #expect(mixedEpisode.labelTier == .silver)
        #expect(!mixedEpisode.isEligibleForGoldEvaluation)
    }

    @Test("Gold review evidence requires one artifact from each review pass")
    func goldReviewArtifactKinds() {
        #expect(CorpusAnnotationLoader.hasRequiredGoldReviewArtifactKinds([
            "human_first_pass_attestation",
            "corpus_review_attestation",
        ]))
        #expect(!CorpusAnnotationLoader.hasRequiredGoldReviewArtifactKinds([
            "human_first_pass_attestation",
            "human_first_pass_attestation",
        ]))
        #expect(!CorpusAnnotationLoader.hasRequiredGoldReviewArtifactKinds([
            "corpus_review_attestation",
            "corpus_review_attestation",
        ]))
    }

    @Test("Gold review evidence requires exactly two attestations")
    func goldReviewAttestationCardinality() {
        let original = makeWellFormedAnnotation()
        let attestations = (original.reviewAttestations ?? []) + [
            CorpusAnnotation.ReviewAttestation(
                reviewer: "Reviewer Three",
                reviewedAt: "2026-07-11T12:00:00Z",
                audioFingerprint: original.audioFingerprint,
                reviewArtifactId: "sha256:" + String(repeating: "c", count: 64)
            ),
        ]
        let annotation = CorpusAnnotation(
            episodeId: original.episodeId,
            showName: original.showName,
            durationSeconds: original.durationSeconds,
            adWindows: original.adWindows,
            contentWindows: original.contentWindows,
            variantOf: original.variantOf,
            audioFingerprint: original.audioFingerprint,
            autoPromoted: original.autoPromoted,
            autoPromotedAt: original.autoPromotedAt,
            autoPromotedBy: original.autoPromotedBy,
            provenance: original.provenance,
            auditPriority: original.auditPriority,
            reviewAttestations: attestations
        )

        #expect(!annotation.hasVerifiedReviewAttestations)
        #expect(annotation.labelTier == .silver)
        #expect(
            CorpusAnnotationLoader.validate(annotation).contains {
                $0.kind == .humanReviewedWithoutTwoAttestations
            }
        )
    }

    @Test("Human-only provenance normalization always requires attestations")
    func normalizedHumanProvenanceRequiresAttestations() {
        let original = makeWellFormedAnnotation()
        for provenance in [
            ["HUMAN_REVIEWED"],
            ["human_reviewed", "HUMAN_REVIEWED"],
        ] {
            let annotation = CorpusAnnotation(
                episodeId: original.episodeId,
                showName: original.showName,
                durationSeconds: original.durationSeconds,
                adWindows: original.adWindows,
                contentWindows: original.contentWindows,
                variantOf: original.variantOf,
                audioFingerprint: original.audioFingerprint,
                provenance: provenance
            )

            #expect(annotation.hasHumanOnlyProvenance)
            #expect(annotation.labelTier == .silver)
            #expect(
                CorpusAnnotationLoader.validate(annotation).contains {
                    $0.kind == .humanReviewedWithoutTwoAttestations
                }
            )
        }
    }

    @Test("Review timestamps reject normalized impossible calendar dates")
    func reviewTimestampsRejectImpossibleDates() {
        let original = makeWellFormedAnnotation()
        let attestations = (original.reviewAttestations ?? []).map {
            CorpusAnnotation.ReviewAttestation(
                reviewer: $0.reviewer,
                reviewedAt: "2026-02-30T12:00:00Z",
                audioFingerprint: $0.audioFingerprint,
                reviewArtifactId: $0.reviewArtifactId
            )
        }
        let annotation = CorpusAnnotation(
            episodeId: original.episodeId,
            showName: original.showName,
            durationSeconds: original.durationSeconds,
            adWindows: original.adWindows,
            contentWindows: original.contentWindows,
            variantOf: original.variantOf,
            audioFingerprint: original.audioFingerprint,
            provenance: original.provenance,
            reviewAttestations: attestations
        )

        #expect(!CorpusAnnotation.isCanonicalReviewTimestamp("2026-02-30T12:00:00Z"))
        #expect(CorpusAnnotation.isCanonicalReviewTimestamp("2026-07-11T12:00:00Z"))
        #expect(!annotation.reviewAttestationsAreWellFormed)
        #expect(
            CorpusAnnotationLoader.validate(annotation).contains {
                $0.kind == .reviewAttestationInvalid
            }
        )
    }
}

// MARK: - Validator

@Suite("CorpusAnnotationLoader – Validator")
struct CorpusAnnotationValidatorTests {

    @Test("A well-formed annotation reports no issues")
    func wellFormed() {
        let issues = CorpusAnnotationLoader.validate(makeWellFormedAnnotation())
        #expect(issues.isEmpty, "unexpected issues: \(issues)")
    }

    @Test("Rejects empty episode and show identity")
    func emptyIdentityMetadata() {
        let original = makeWellFormedAnnotation()
        let annotation = CorpusAnnotation(
            episodeId: "",
            showName: "",
            durationSeconds: original.durationSeconds,
            adWindows: original.adWindows,
            contentWindows: original.contentWindows,
            variantOf: original.variantOf,
            audioFingerprint: original.audioFingerprint
        )
        let kinds = Set(CorpusAnnotationLoader.validate(annotation).map(\.kind))
        #expect(kinds.contains(.episodeIDEmpty))
        #expect(kinds.contains(.showNameEmpty))
    }

    @Test("Catches non-positive duration")
    func nonPositiveDuration() {
        var ann = makeWellFormedAnnotation()
        ann = CorpusAnnotation(
            episodeId: ann.episodeId,
            showName: ann.showName,
            durationSeconds: 0,
            adWindows: ann.adWindows,
            contentWindows: ann.contentWindows,
            variantOf: ann.variantOf,
            audioFingerprint: ann.audioFingerprint
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .nonPositiveDuration })
    }

    @Test("Catches missing sha256: prefix on fingerprint")
    func missingFingerprintPrefix() {
        var ann = makeWellFormedAnnotation()
        ann = CorpusAnnotation(
            episodeId: ann.episodeId,
            showName: ann.showName,
            durationSeconds: ann.durationSeconds,
            adWindows: ann.adWindows,
            contentWindows: ann.contentWindows,
            variantOf: ann.variantOf,
            audioFingerprint: "abcdef0123"
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .fingerprintMissingPrefix })
    }

    @Test("Catches negative window start")
    func negativeWindowStart() {
        let ann = makeAnnotation(
            duration: 600,
            ads: [(-5, 30)],
            content: [(30, 600)]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .windowStartNegative })
    }

    @Test("Catches end before or equal to start")
    func endLessOrEqualStart() {
        let ann = makeAnnotation(
            duration: 600,
            ads: [(100, 100)],
            content: [(0, 100), (100, 600)]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .windowEndBeforeStart })
    }

    @Test("Catches window overshooting episode duration")
    func windowOvershootsDuration() {
        let ann = makeAnnotation(
            duration: 100,
            ads: [(50, 200)],
            content: [(0, 50)]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .windowOutOfRange })
    }

    @Test("Catches overlapping ad windows")
    func overlappingAds() {
        let ann = makeAnnotation(
            duration: 600,
            ads: [(100, 200), (150, 300)],
            content: [(0, 100), (300, 600)]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .adWindowsOverlap })
    }

    @Test("Catches overlap between ad and content")
    func adContentOverlap() {
        let ann = makeAnnotation(
            duration: 600,
            ads: [(100, 200)],
            content: [(0, 150), (200, 600)]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .adContentOverlap })
    }

    @Test("Catches gap in timeline coverage")
    func timelineGap() {
        let ann = makeAnnotation(
            duration: 600,
            ads: [(100, 200)],
            // Gap between [0, 100] and [200, 500]; gap [500, 600] at end too.
            content: [(0, 100), (200, 500)]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        // Coverage ends at 500 but duration is 600.
        #expect(issues.contains { $0.kind == .partitionShortfall || $0.kind == .timelineGap })
    }

    @Test("Catches gap at start of timeline")
    func gapAtStart() {
        let ann = makeAnnotation(
            duration: 600,
            ads: [(100, 200)],
            content: [(200, 600)] // missing [0, 100]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .timelineGap })
    }

    @Test("Off-by-one boundary within epsilon does not trigger gap")
    func boundaryEpsilon() {
        let ann = makeAnnotation(
            duration: 600,
            ads: [(100, 200.01)], // 10ms past 200
            content: [(0, 100), (200, 600)]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        // 10ms slack is below the 50ms epsilon — must not flag overlap.
        #expect(!issues.contains { $0.kind == .adContentOverlap },
                "10ms slack should be tolerated; got \(issues)")
    }

    @Test("Catches partition overshoot")
    func partitionOvershoot() {
        // Duration is 100 but coverage extends to 200 — caught as
        // out-of-range first (fail-fast) which keeps the message focused.
        let ann = makeAnnotation(
            duration: 100,
            ads: [],
            content: [(0, 200)]
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .windowOutOfRange || $0.kind == .partitionOvershoot })
    }

    @Test("Catches variantOf self-reference")
    func variantSelfReference() {
        let ann = CorpusAnnotation(
            episodeId: "ep-1",
            showName: "Test",
            durationSeconds: 100,
            adWindows: [],
            contentWindows: [
                CorpusAnnotation.ContentWindow(startSeconds: 0, endSeconds: 100, notes: nil),
            ],
            variantOf: "ep-1",
            audioFingerprint: placeholderFingerprint
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .variantOfSelfReference })
    }

    @Test("Empty timeline (no windows) flagged as partition shortfall")
    func emptyTimeline() {
        let ann = CorpusAnnotation(
            episodeId: "ep-empty",
            showName: "Empty",
            durationSeconds: 100,
            adWindows: [],
            contentWindows: [],
            variantOf: nil,
            audioFingerprint: placeholderFingerprint
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .partitionShortfall })
    }

    @Test("Catches malformed (too-short) hex digest")
    func fingerprintShortHex() {
        let ann = makeAnnotation(
            duration: 100,
            ads: [],
            content: [(0, 100)],
            fingerprint: "sha256:abc123"
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .fingerprintMalformedHex })
    }

    @Test("Catches uppercase hex digest")
    func fingerprintUppercaseHex() {
        let ann = makeAnnotation(
            duration: 100,
            ads: [],
            content: [(0, 100)],
            fingerprint: "sha256:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef"
        )
        let issues = CorpusAnnotationLoader.validate(ann)
        #expect(issues.contains { $0.kind == .fingerprintMalformedHex })
    }
}

// MARK: - Fingerprint Helper

@Suite("CorpusAudioFingerprint")
struct CorpusAudioFingerprintTests {

    @Test("In-memory fingerprint matches CryptoKit SHA-256 reference")
    func inMemoryHash() {
        let data = Data("hello, world".utf8)
        let expected = "sha256:" + SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
        #expect(CorpusAudioFingerprint.fingerprint(of: data) == expected)
    }

    @Test("Streaming fingerprint matches in-memory fingerprint")
    func streamingMatchesInMemory() throws {
        let bytes = Data((0..<(CorpusAudioFingerprint.chunkSize * 3 + 17)).map { UInt8($0 % 251) })
        let expected = CorpusAudioFingerprint.fingerprint(of: bytes)

        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(
            "corpus-fp-\(UUID().uuidString).bin"
        )
        try bytes.write(to: tempURL)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let actual = try CorpusAudioFingerprint.fingerprint(of: tempURL)
        #expect(actual == expected)
    }

    @Test("Streaming fingerprint is invariant under chunk size")
    func chunkSizeInvariance() throws {
        let bytes = Data((0..<200_000).map { UInt8(($0 * 31) & 0xff) })
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(
            "corpus-fp-chunk-\(UUID().uuidString).bin"
        )
        try bytes.write(to: tempURL)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let small = try CorpusAudioFingerprint.fingerprint(of: tempURL, chunkSize: 1024)
        let medium = try CorpusAudioFingerprint.fingerprint(of: tempURL, chunkSize: 65_536)
        let large = try CorpusAudioFingerprint.fingerprint(of: tempURL, chunkSize: 1_048_576)
        #expect(small == medium)
        #expect(medium == large)
    }

    @Test("Empty file has the well-known empty SHA-256 digest")
    func emptyFile() throws {
        let tempURL = FileManager.default.temporaryDirectory.appendingPathComponent(
            "corpus-fp-empty-\(UUID().uuidString).bin"
        )
        try Data().write(to: tempURL)
        defer { try? FileManager.default.removeItem(at: tempURL) }

        let actual = try CorpusAudioFingerprint.fingerprint(of: tempURL)
        // Well-known SHA-256 of empty input.
        #expect(actual == "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    }

    @Test("Missing file throws fileNotFound")
    func missingFile() {
        let url = URL(fileURLWithPath: "/tmp/this-does-not-exist-\(UUID().uuidString).bin")
        #expect(throws: CorpusAudioFingerprintError.self) {
            _ = try CorpusAudioFingerprint.fingerprint(of: url)
        }
    }
}

// MARK: - Loader

@Suite("CorpusAnnotationLoader – on-disk loader")
struct CorpusAnnotationLoaderDiskTests {

    @Test("Annotations directory exists for current source tree")
    func annotationsDirExists() throws {
        let loader = CorpusAnnotationLoader()
        #expect(
            FileManager.default.fileExists(atPath: loader.annotationsDirectoryURL.path),
            "Expected annotations directory at \(loader.annotationsDirectoryURL.path)"
        )
    }

    @Test("Template files are excluded from annotation enumeration")
    func templateExclusion() {
        #expect(CorpusAnnotationLoader.isTemplate("_template.example.json"))
        #expect(CorpusAnnotationLoader.isTemplate("anything.example.json"))
        #expect(CorpusAnnotationLoader.isTemplate("_anything.json"))
        #expect(!CorpusAnnotationLoader.isTemplate("corpus-001.json"))
        #expect(!CorpusAnnotationLoader.isTemplate("show-name-ep-42.json"))
    }

    @Test("loadAll succeeds on the current annotations directory")
    func loadAllSucceeds() throws {
        // Regression guard: this test failed across review rounds R1–R8
        // because the on-disk corpus had drifted ahead of the schema.
        // The auto-promote pipeline (`scripts/l2f-auto-promote.py`
        // rules R1/R3) emits `"ad_type": "dai"` for rediff-confirmed
        // Dynamic Ad Insertion and `"transition_type": null` for every
        // triangulated promotion, but `CorpusAnnotation.AdType` had no
        // `.dai` case and `transitionType` was non-optional, so Codable
        // decoding failed on every R1/R3-promoted annotation.
        //
        // Fix (Option A): widened `AdType` with an additive `.dai`
        // case distinct from `.dynamicInsertion` to preserve the
        // rediff physical-provenance signal in the audit trail, and
        // made `transitionType` optional. Both `.dai` and
        // `.dynamicInsertion` fold into the same `.dynamicInsertion`
        // simulator delivery style and `.dynamic` fusion-lift bucket,
        // so the distinction stays inside the corpus.
        let loader = CorpusAnnotationLoader()
        let annotations = try loader.loadAll(verifyAudioFingerprints: false)
        for a in annotations {
            #expect(!a.episodeId.isEmpty)
        }
    }

    @Test("An empty attestation list does not require an artifact directory")
    func emptyReviewAttestationsNeedNoArtifactDirectory() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(
            CorpusAnnotationLoader.annotationsRelativePath
        )
        let original = makeWellFormedAnnotation()
        let annotation = CorpusAnnotation(
            episodeId: original.episodeId,
            showName: original.showName,
            durationSeconds: original.durationSeconds,
            adWindows: original.adWindows,
            contentWindows: original.contentWindows,
            variantOf: original.variantOf,
            audioFingerprint: original.audioFingerprint,
            provenance: ["human_first_pass"],
            reviewAttestations: []
        )
        let annotationURL = annotations.appendingPathComponent("corpus-001.json")
        try CorpusAnnotation.encoder.encode(annotation).write(to: annotationURL)
        try Data(
            "{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8
        ).write(
            to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
        )

        #expect(try CorpusAnnotationLoader(repoRoot: root).loadAll().count == 1)
    }

    @Test("Canonical manifest schema version rejects a floating JSON number")
    func manifestSchemaVersionRejectsFloatingJSONNumber() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(
            CorpusAnnotationLoader.annotationsRelativePath
        )
        try Data(
            "{\"schema_version\":1.0,\"annotations\":[\"corpus-001.json\"]}".utf8
        ).write(
            to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
        )

        #expect {
            try CorpusAnnotationLoader(repoRoot: root).annotationFileURLs()
        } throws: { error in
            guard case CorpusAnnotationLoaderError.manifestInvalid(_, let detail) = error else {
                return false
            }
            return detail.contains("JSON integer 1")
        }
    }

    @Test("Annotation audit priorities reject floating JSON numbers")
    func annotationAuditPrioritiesRejectFloatingJSONNumbers() throws {
        for (episodePriority, windowPriority, expectedDetail) in [
            ("1.0", "1", "audit_priority must be a JSON integer or null"),
            ("1", "1.0", "ad_windows[0].audit_priority must be a JSON integer or null"),
        ] {
            let root = try makeTemporaryCorpusRoot()
            defer { try? FileManager.default.removeItem(at: root) }
            let annotations = root.appendingPathComponent(
                CorpusAnnotationLoader.annotationsRelativePath
            )
            let annotation = """
            {
              "episode_id": "corpus-001",
              "show_name": "Example",
              "duration_seconds": 10,
              "ad_windows": [{
                "start_seconds": 2,
                "end_seconds": 4,
                "advertiser": null,
                "product": null,
                "ad_type": "dai",
                "transition_type": null,
                "confidence_notes": null,
                "audit_priority": \(windowPriority)
              }],
              "content_windows": [
                {"start_seconds": 0, "end_seconds": 2, "notes": null},
                {"start_seconds": 4, "end_seconds": 10, "notes": null}
              ],
              "variant_of": null,
              "audio_fingerprint": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
              "audit_priority": \(episodePriority)
            }
            """
            try Data(annotation.utf8).write(
                to: annotations.appendingPathComponent("corpus-001.json")
            )
            try Data(
                "{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8
            ).write(
                to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
            )

            #expect {
                try CorpusAnnotationLoader(
                    repoRoot: root,
                    verifyReviewArtifacts: false
                ).loadAll()
            } throws: { error in
                guard case CorpusAnnotationLoaderError.decodeFailed(_, let underlying) = error else {
                    return false
                }
                return underlying.localizedDescription == expectedDetail
            }
        }
    }

    @Test("Review attestations reject unknown JSON keys like the Python validator")
    func reviewAttestationsRequireExactKeys() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(
            CorpusAnnotationLoader.annotationsRelativePath
        )
        let annotation = """
        {
          "episode_id": "corpus-001",
          "show_name": "Example",
          "duration_seconds": 10,
          "ad_windows": [],
          "content_windows": [
            {"start_seconds": 0, "end_seconds": 10, "notes": null}
          ],
          "variant_of": null,
          "audio_fingerprint": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
          "provenance": ["human_first_pass"],
          "review_attestations": [{
            "reviewer": "Reviewer",
            "reviewed_at": "2026-07-10T12:00:00Z",
            "audio_fingerprint": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "review_artifact_id": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "asserted_gold": true
          }]
        }
        """
        let annotationURL = annotations.appendingPathComponent("corpus-001.json")
        try Data(annotation.utf8).write(to: annotationURL)

        #expect {
            try CorpusAnnotationLoader(
                repoRoot: root,
                verifyReviewArtifacts: false
            ).decode(at: annotationURL)
        } throws: { error in
            guard case CorpusAnnotationLoaderError.decodeFailed(_, let underlying) = error else {
                return false
            }
            return underlying.localizedDescription.contains(
                "review_attestations[0] must contain exactly"
            )
        }
    }

    @Test("Canonical Swift readers wait for the publication lock")
    func canonicalReadersWaitForPublicationLock() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(
            CorpusAnnotationLoader.annotationsRelativePath
        )
        let fingerprint = try stageBoundAudio(root: root, episodeId: "corpus-001")
        try CorpusAnnotation.encoder.encode(
            makeWellFormedAnnotation(fingerprint: fingerprint)
        ).write(
            to: annotations.appendingPathComponent("corpus-001.json")
        )
        try Data(
            "{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8
        ).write(
            to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
        )
        let urls = try CorpusAnnotationLoader(
            repoRoot: root,
            verifyReviewArtifacts: false
        ).annotationFileURLs()
        let lockURL = annotations.appendingPathComponent(
            CorpusAnnotationLoader.publicationLockFilename
        )
        let lockContended = DispatchSemaphore(value: 0)
        let loader = CorpusAnnotationLoader(
            repoRoot: root,
            verifyReviewArtifacts: false,
            publicationLockContentionObserver: { lockContended.signal() }
        )

        func openLockedDescriptor() throws -> Int32 {
            let descriptor = Darwin.open(
                lockURL.path,
                O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW,
                S_IRUSR | S_IWUSR
            )
            guard descriptor >= 0, flock(descriptor, LOCK_EX) == 0 else {
                if descriptor >= 0 { Darwin.close(descriptor) }
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            return descriptor
        }

        func whilePublicationLocked<T: Sendable>(
            _ operation: @escaping @Sendable () throws -> T
        ) throws -> T {
            let descriptor = try openLockedDescriptor()
            var isLocked = true
            defer {
                if isLocked { flock(descriptor, LOCK_UN) }
                Darwin.close(descriptor)
            }
            let started = DispatchSemaphore(value: 0)
            let finished = DispatchSemaphore(value: 0)
            let outcome = CorpusThreadOutcome<T>()
            let worker = Thread {
                started.signal()
                defer { finished.signal() }
                outcome.store(Result { try operation() })
            }
            worker.qualityOfService = .userInitiated
            worker.start()
            started.wait()
            try #require(lockContended.wait(timeout: .now() + 30) == .success)
            flock(descriptor, LOCK_UN)
            isLocked = false
            finished.wait()
            return try outcome.resolve()
        }

        let listed = try whilePublicationLocked {
            try loader.annotationFileURLs()
        }
        #expect(listed == urls)
        let one = try whilePublicationLocked {
            try loader.loadAndValidate(at: urls[0])
        }
        #expect(one.episodeId == "corpus-001")
        let loaded = try whilePublicationLocked {
            try loader.loadAll()
        }
        #expect(loaded.count == 1)
        try whilePublicationLocked {
            try loader.preflightGoldEvaluationInputs(annotationURLs: urls)
        }

        let annotationURL = annotations.appendingPathComponent("corpus-001.json")
        var replacement = try #require(
            try JSONSerialization.jsonObject(with: Data(contentsOf: annotationURL))
                as? [String: Any]
        )
        replacement["show_name"] = "Committed after preflight"
        try JSONSerialization.data(withJSONObject: replacement).write(to: annotationURL)
        let pinned = try loader.loadAndValidate(at: annotationURL)
        let fresh = try CorpusAnnotationLoader(
            repoRoot: root,
            verifyReviewArtifacts: false
        ).loadAndValidate(at: annotationURL)
        #expect(pinned.showName == "Test Show")
        #expect(fresh.showName == "Committed after preflight")

        #expect(
            try loader.annotationFileURLs().map(\.lastPathComponent)
                == ["corpus-001.json"]
        )
    }

    @Test("Publication locks for unrelated corpus roots are independent")
    func publicationLocksAreScopedToCanonicalRoot() throws {
        let firstRoot = try makeTemporaryCorpusRoot()
        let secondRoot = try makeTemporaryCorpusRoot()
        defer {
            try? FileManager.default.removeItem(at: firstRoot)
            try? FileManager.default.removeItem(at: secondRoot)
        }

        func stageCorpus(at root: URL) throws {
            let annotations = root.appendingPathComponent(
                CorpusAnnotationLoader.annotationsRelativePath
            )
            try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(
                to: annotations.appendingPathComponent("corpus-001.json")
            )
            try Data(
                "{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8
            ).write(
                to: annotations.appendingPathComponent(
                    CorpusAnnotationLoader.manifestFilename
                )
            )
        }

        try stageCorpus(at: firstRoot)
        try stageCorpus(at: secondRoot)
        let firstAnnotations = firstRoot.appendingPathComponent(
            CorpusAnnotationLoader.annotationsRelativePath
        )
        let descriptor = Darwin.open(
            firstAnnotations.appendingPathComponent(
                CorpusAnnotationLoader.publicationLockFilename
            ).path,
            O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW,
            S_IRUSR | S_IWUSR
        )
        guard descriptor >= 0, flock(descriptor, LOCK_EX) == 0 else {
            if descriptor >= 0 { Darwin.close(descriptor) }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        var firstLockHeld = true
        defer {
            if firstLockHeld { flock(descriptor, LOCK_UN) }
            Darwin.close(descriptor)
        }

        let firstContended = DispatchSemaphore(value: 0)
        let firstLoader = CorpusAnnotationLoader(
            repoRoot: firstRoot,
            verifyReviewArtifacts: false,
            publicationLockContentionObserver: { firstContended.signal() }
        )
        let firstStarted = DispatchSemaphore(value: 0)
        let firstFinished = DispatchSemaphore(value: 0)
        let firstOutcome = CorpusThreadOutcome<[URL]>()
        let firstWorker = Thread {
            firstStarted.signal()
            defer { firstFinished.signal() }
            firstOutcome.store(Result { try firstLoader.annotationFileURLs() })
        }
        firstWorker.qualityOfService = .userInitiated
        firstWorker.start()
        firstStarted.wait()
        try #require(firstContended.wait(timeout: .now() + 30) == .success)

        let secondLoader = CorpusAnnotationLoader(
            repoRoot: secondRoot,
            verifyReviewArtifacts: false
        )
        let secondURLs = try secondLoader.annotationFileURLs()

        flock(descriptor, LOCK_UN)
        firstLockHeld = false
        firstFinished.wait()
        let firstURLs = try firstOutcome.resolve()

        #expect(firstURLs.map(\.lastPathComponent) == ["corpus-001.json"])
        #expect(secondURLs.map(\.lastPathComponent) == ["corpus-001.json"])
    }

    @Test("Canonical manifest excludes unverifiable B labels and keeps first-pass silver")
    func canonicalManifestAndTiers() throws {
        let loader = CorpusAnnotationLoader()
        let urls = try loader.annotationFileURLs()
        #expect(urls.count == 12)

        let annotations = try loader.loadAll(verifyAudioFingerprints: false)
        #expect(annotations.filter(\.isEligibleForGoldEvaluation).isEmpty)
        let tiers = annotations.flatMap { annotation in
            annotation.adWindows.map { annotation.labelTier(for: $0) }
        }
        #expect(tiers.filter { $0 == .gold }.isEmpty)
        #expect(tiers.filter { $0 == .silver }.count == 24)
        #expect(tiers.filter { $0 == .boundaryProposal }.isEmpty)
        #expect(annotations.allSatisfy { $0.provenance == ["human_first_pass"] })
        #expect(annotations.allSatisfy { $0.reviewAttestations?.count == 1 })
        #expect(annotations.allSatisfy { CorpusAnnotationLoader.validate($0).isEmpty })
        for annotation in annotations {
            let intervals = (
                annotation.adWindows.map { ($0.startSeconds, $0.endSeconds) }
                    + annotation.contentWindows.map { ($0.startSeconds, $0.endSeconds) }
            ).sorted { $0.0 < $1.0 }
            #expect(intervals.first?.0 == 0, "\(annotation.episodeId) must start at zero")
            #expect(
                intervals.last?.1 == annotation.durationSeconds,
                "\(annotation.episodeId) must end at its exact duration"
            )
            for (left, right) in zip(intervals, intervals.dropFirst()) {
                #expect(
                    left.1 == right.0,
                    "\(annotation.episodeId) has a non-exact boundary at \(left.1)/\(right.0)"
                )
            }
        }
    }

    @Test("Canonical manifest is the only annotation membership source")
    func manifestPinsMembership() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let listed = annotations.appendingPathComponent("listed.json")
        let extra = annotations.appendingPathComponent("extra.json")
        let data = try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation())
        try data.write(to: listed)
        try data.write(to: extra)

        let manifest = annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
        try Data("{\"schema_version\":1,\"annotations\":[\"listed.json\"]}".utf8)
            .write(to: manifest)
        let pinned = try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).annotationFileURLs()
        #expect(pinned.map(\.lastPathComponent) == ["listed.json"])
    }

    @Test("Every corpus root requires the canonical manifest")
    func everyRootRequiresManifest() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation())
            .write(to: annotations.appendingPathComponent("listed.json"))
        do {
            _ = try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).annotationFileURLs()
            Issue.record("Expected a missing canonical manifest to fail")
        } catch CorpusAnnotationLoaderError.manifestMissing(let url) {
            #expect(url.lastPathComponent == CorpusAnnotationLoader.manifestFilename)
        } catch {
            Issue.record("Expected manifestMissing, got \(error)")
        }
    }

    @Test("Canonical manifest itself must not be a symbolic link")
    func canonicalManifestRejectsSymlink() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation())
            .write(to: annotations.appendingPathComponent("corpus-001.json"))
        let outside = root.appendingPathComponent("outside-manifest.json")
        try Data("{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8)
            .write(to: outside)
        try FileManager.default.createSymbolicLink(
            at: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename),
            withDestinationURL: outside
        )

        #expect(throws: CorpusAnnotationLoaderError.self) {
            _ = try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).annotationFileURLs()
        }
    }

    @Test("Canonical annotation filename must match its episode id")
    func canonicalFilenameMatchesEpisodeID() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation())
            .write(to: annotations.appendingPathComponent("wrong.json"))
        try Data("{\"schema_version\":1,\"annotations\":[\"wrong.json\"]}".utf8)
            .write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))

        #expect(throws: CorpusAnnotationLoaderError.self) {
            _ = try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).loadAll()
        }
    }

    @Test("Canonical episodes must reference distinct audio fingerprints")
    func canonicalAudioFingerprintsAreUnique() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let first = makeWellFormedAnnotation()
        let second = CorpusAnnotation(
            episodeId: "corpus-002",
            showName: first.showName,
            durationSeconds: first.durationSeconds,
            adWindows: first.adWindows,
            contentWindows: first.contentWindows,
            variantOf: nil,
            audioFingerprint: first.audioFingerprint
        )
        try CorpusAnnotation.encoder.encode(first)
            .write(to: annotations.appendingPathComponent("corpus-001.json"))
        try CorpusAnnotation.encoder.encode(second)
            .write(to: annotations.appendingPathComponent("corpus-002.json"))
        try Data(
            "{\"schema_version\":1,\"annotations\":[\"corpus-001.json\",\"corpus-002.json\"]}".utf8
        ).write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))

        #expect {
            try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).loadAll()
        } throws: { error in
            guard case let CorpusAnnotationLoaderError.manifestInvalid(_, detail) = error else {
                return false
            }
            return detail.contains("share audio_fingerprint")
        }
    }

    @Test("Canonical variants must reference a manifest episode")
    func canonicalVariantParentExists() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let base = makeWellFormedAnnotation()
        let variant = CorpusAnnotation(
            episodeId: base.episodeId,
            showName: base.showName,
            durationSeconds: base.durationSeconds,
            adWindows: base.adWindows,
            contentWindows: base.contentWindows,
            variantOf: "missing-parent",
            audioFingerprint: base.audioFingerprint,
            provenance: base.provenance,
            reviewAttestations: base.reviewAttestations
        )
        try CorpusAnnotation.encoder.encode(variant)
            .write(to: annotations.appendingPathComponent("corpus-001.json"))
        try Data("{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8)
            .write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))

        #expect {
            try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).loadAll()
        } throws: { error in
            guard case let CorpusAnnotationLoaderError.manifestInvalid(_, detail) = error else {
                return false
            }
            return detail.contains("non-canonical variant_of")
        }
    }

    @Test("Manifest rejects duplicate, missing, and unsafe entries")
    func invalidManifestEntries() throws {
        let invalidLists = [
            ["listed.json", "listed.json"],
            ["missing.json"],
            ["../listed.json"],
            ["/tmp/listed.json"],
            ["nested/listed.json"],
            ["nested\\listed.json"],
        ]

        for entries in invalidLists {
            let root = try makeTemporaryCorpusRoot()
            defer { try? FileManager.default.removeItem(at: root) }
            let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
            let data = try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation())
            try data.write(to: annotations.appendingPathComponent("listed.json"))
            let payload: [String: Any] = ["schema_version": 1, "annotations": entries]
            let manifestData = try JSONSerialization.data(withJSONObject: payload)
            try manifestData.write(
                to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
            )

            #expect(throws: CorpusAnnotationLoaderError.self) {
                _ = try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).annotationFileURLs()
            }
        }
    }

    @Test("Manifest rejects a symlink that escapes the annotations directory")
    func manifestRejectsEscapingSymlink() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let outside = root.appendingPathComponent("outside.json")
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(to: outside)
        try FileManager.default.createSymbolicLink(
            at: annotations.appendingPathComponent("linked.json"),
            withDestinationURL: outside
        )
        try Data("{\"schema_version\":1,\"annotations\":[\"linked.json\"]}".utf8)
            .write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))

        #expect(throws: CorpusAnnotationLoaderError.self) {
            _ = try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).annotationFileURLs()
        }
    }

    @Test("Canonical annotations root must not be a symbolic link")
    func annotationsRootRejectsSymbolicLink() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            "l2f-root-symlink-\(UUID().uuidString)"
        )
        defer { try? FileManager.default.removeItem(at: root) }
        let corpus = root.appendingPathComponent("TestFixtures/Corpus", isDirectory: true)
        let outside = root.appendingPathComponent("outside", isDirectory: true)
        try FileManager.default.createDirectory(at: corpus, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: outside, withIntermediateDirectories: true)
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(
            to: outside.appendingPathComponent("corpus-001.json")
        )
        try Data("{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8)
            .write(to: outside.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))
        try FileManager.default.createSymbolicLink(
            at: corpus.appendingPathComponent("Annotations", isDirectory: true),
            withDestinationURL: outside
        )

        #expect(throws: CorpusAnnotationLoaderError.self) {
            _ = try CorpusAnnotationLoader(
                repoRoot: root,
                verifyReviewArtifacts: false
            ).annotationFileURLs()
        }
    }

    @Test("Canonical annotations root rejects a symbolic-link parent")
    func annotationsRootRejectsSymbolicLinkParent() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(
            "l2f-parent-symlink-\(UUID().uuidString)"
        )
        defer { try? FileManager.default.removeItem(at: root) }
        let outside = root.appendingPathComponent("outside", isDirectory: true)
        let annotationsTarget = outside
            .appendingPathComponent("Corpus", isDirectory: true)
            .appendingPathComponent("Annotations", isDirectory: true)
        try FileManager.default.createDirectory(
            at: annotationsTarget,
            withIntermediateDirectories: true
        )
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(
            to: annotationsTarget.appendingPathComponent("corpus-001.json")
        )
        try Data("{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8)
            .write(
                to: annotationsTarget.appendingPathComponent(
                    CorpusAnnotationLoader.manifestFilename
                )
            )
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(
            at: root.appendingPathComponent("TestFixtures", isDirectory: true),
            withDestinationURL: outside
        )

        #expect(throws: CorpusAnnotationLoaderError.self) {
            _ = try CorpusAnnotationLoader(
                repoRoot: root,
                verifyReviewArtifacts: false
            ).annotationFileURLs()
        }
        #expect(
            !FileManager.default.fileExists(
                atPath: annotationsTarget.appendingPathComponent(
                    CorpusAnnotationLoader.publicationLockFilename
                ).path
            )
        )
    }

    @Test("Manifest rejects aliases of the same annotation")
    func manifestRejectsDuplicateSymlinkAlias() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let listed = annotations.appendingPathComponent("listed.json")
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(to: listed)
        try FileManager.default.createSymbolicLink(
            at: annotations.appendingPathComponent("alias.json"),
            withDestinationURL: listed
        )
        try Data(
            "{\"schema_version\":1,\"annotations\":[\"listed.json\",\"alias.json\"]}".utf8
        ).write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))

        #expect(throws: CorpusAnnotationLoaderError.self) {
            _ = try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).annotationFileURLs()
        }
    }

    @Test("Manifest rejects a sole in-directory symlink alias")
    func manifestRejectsSoleSymlinkAlias() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let listed = annotations.appendingPathComponent("listed.json")
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(to: listed)
        try FileManager.default.createSymbolicLink(
            at: annotations.appendingPathComponent("alias.json"),
            withDestinationURL: listed
        )
        try Data("{\"schema_version\":1,\"annotations\":[\"alias.json\"]}".utf8)
            .write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))

        #expect(throws: CorpusAnnotationLoaderError.self) {
            _ = try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).annotationFileURLs()
        }
    }

    @Test("Template file in annotations directory decodes successfully")
    func templateDecodes() throws {
        let loader = CorpusAnnotationLoader()
        let templateURL = loader.annotationsDirectoryURL
            .appendingPathComponent("_template.example.json")
        guard FileManager.default.fileExists(atPath: templateURL.path) else {
            // Template not yet committed in this snapshot — skip.
            return
        }
        // Decode raw (no validation) — the template uses placeholder values.
        let decoded = try loader.decode(at: templateURL)
        #expect(!decoded.episodeId.isEmpty)
    }

    @Test("loadAndValidate surfaces structured errors")
    func loadAndValidateErrors() throws {
        // Write a deliberately broken annotation to a tmp directory and
        // confirm the loader raises validationFailed with the expected
        // issue kinds.
        let tmp = FileManager.default.temporaryDirectory.appendingPathComponent(
            "l2f-bad-\(UUID().uuidString)"
        )
        try FileManager.default.createDirectory(at: tmp, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tmp) }

        let json = """
        {
          "episode_id": "bad-1",
          "show_name": "Bad",
          "duration_seconds": 100,
          "ad_windows": [
            {"start_seconds": 50, "end_seconds": 200, "advertiser": null, "product": null,
             "ad_type": "host_read", "transition_type": "explicit", "confidence_notes": null}
          ],
          "content_windows": [
            {"start_seconds": 0, "end_seconds": 50, "notes": null}
          ],
          "variant_of": null,
          "audio_fingerprint": "sha256:abc"
        }
        """
        let badURL = tmp.appendingPathComponent("bad-1.json")
        try json.write(to: badURL, atomically: true, encoding: .utf8)

        let loader = CorpusAnnotationLoader(repoRoot: tmp)
        #expect {
            try loader.loadAndValidate(at: badURL, verifyFingerprint: false)
        } throws: { error in
            guard case let CorpusAnnotationLoaderError.validationFailed(_, issues) = error else {
                return false
            }
            return issues.contains { $0.kind == .windowOutOfRange }
        }
    }

    @Test("Audio resolution refuses ambiguous episode cuts")
    func audioResolutionRejectsMultipleCuts() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let audio = root.appendingPathComponent(CorpusAnnotationLoader.audioRelativePath)
        try FileManager.default.createDirectory(at: audio, withIntermediateDirectories: true)
        try Data("first".utf8).write(to: audio.appendingPathComponent("corpus-001.mp3"))
        try Data("second".utf8).write(to: audio.appendingPathComponent("corpus-001.m4a"))

        #expect {
            try CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false).audioFileURL(
                for: makeWellFormedAnnotation()
            )
        } throws: { error in
            guard case let CorpusAnnotationLoaderError.audioFileAmbiguous(id, matches) = error else {
                return false
            }
            return id == "corpus-001" && matches.map(\.lastPathComponent) == [
                "corpus-001.m4a", "corpus-001.mp3",
            ]
        }
    }

    @Test("Audio resolution refuses leaf and parent symbolic links")
    func audioResolutionRejectsSymbolicLinks() throws {
        for aliasParent in [false, true] {
            let root = try makeTemporaryCorpusRoot()
            defer { try? FileManager.default.removeItem(at: root) }
            let audio = root.appendingPathComponent(CorpusAnnotationLoader.audioRelativePath)
            let external = root.appendingPathComponent("external-audio", isDirectory: true)
            try FileManager.default.createDirectory(at: external, withIntermediateDirectories: true)
            let target = external.appendingPathComponent("corpus-001.mp3")
            let original = Data("external retained audio".utf8)
            try original.write(to: target)
            if aliasParent {
                try FileManager.default.createSymbolicLink(
                    at: audio,
                    withDestinationURL: external
                )
            } else {
                try FileManager.default.createDirectory(at: audio, withIntermediateDirectories: true)
                try FileManager.default.createSymbolicLink(
                    at: audio.appendingPathComponent("corpus-001.mp3"),
                    withDestinationURL: target
                )
            }

            #expect(throws: CorpusAnnotationLoaderError.self) {
                try CorpusAnnotationLoader(
                    repoRoot: root,
                    verifyReviewArtifacts: false
                ).audioFileURL(for: makeWellFormedAnnotation())
            }
            #expect(try Data(contentsOf: target) == original)
        }
    }

    @Test("Gold evaluation preflight rejects hard input failures before scoring")
    func goldEvaluationPreflightRejectsAmbiguousCuts() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let annotationURL = annotations.appendingPathComponent("corpus-001.json")
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(to: annotationURL)
        try Data("{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8)
            .write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))
        let audio = root.appendingPathComponent(CorpusAnnotationLoader.audioRelativePath)
        try FileManager.default.createDirectory(at: audio, withIntermediateDirectories: true)
        try Data("first".utf8).write(to: audio.appendingPathComponent("corpus-001.mp3"))
        try Data("second".utf8).write(to: audio.appendingPathComponent("corpus-001.m4a"))

        let loader = CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false)
        #expect(throws: CorpusAnnotationLoaderError.self) {
            try loader.preflightGoldEvaluationInputs(
                annotationURLs: loader.annotationFileURLs()
            )
        }
    }

    @Test("Gold preflight rejects unresolved review artifact hashes")
    func goldEvaluationPreflightRejectsUnresolvedReviewArtifacts() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let annotationURL = annotations.appendingPathComponent("corpus-001.json")
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(to: annotationURL)
        try Data("{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8)
            .write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))
        try FileManager.default.createDirectory(
            at: root.appendingPathComponent("TestFixtures/Corpus/Reviews"),
            withIntermediateDirectories: true
        )

        let loader = CorpusAnnotationLoader(repoRoot: root)
        #expect {
            try loader.loadAndValidate(at: annotationURL)
        } throws: { error in
            guard case CorpusAnnotationLoaderError.evaluationCohortIncomplete(let detail) = error else {
                return false
            }
            return detail.contains("unresolved review evidence")
        }
        #expect {
            try loader.loadAll()
        } throws: { error in
            guard case CorpusAnnotationLoaderError.evaluationCohortIncomplete(let detail) = error else {
                return false
            }
            return detail.contains("unresolved review evidence")
        }
    }

    @Test("Review artifact roots must not be symbolic links")
    func reviewArtifactRootRejectsSymbolicLink() throws {
        let sourceLoader = CorpusAnnotationLoader()
        let sourceURL = try #require(try sourceLoader.annotationFileURLs().first { url in
            try sourceLoader.decode(at: url).reviewAttestations?.isEmpty == false
        })
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        try FileManager.default.copyItem(
            at: sourceURL,
            to: annotations.appendingPathComponent(sourceURL.lastPathComponent)
        )
        let manifest = [
            "schema_version": 1,
            "annotations": [sourceURL.lastPathComponent],
        ] as [String: Any]
        try JSONSerialization.data(withJSONObject: manifest).write(
            to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
        )
        let reviews = root.appendingPathComponent(
            "TestFixtures/Corpus/Reviews",
            isDirectory: true
        )
        let sourceReviews = sourceLoader.repoRoot.appendingPathComponent(
            "TestFixtures/Corpus/Reviews",
            isDirectory: true
        )
        try FileManager.default.createSymbolicLink(
            at: reviews,
            withDestinationURL: sourceReviews
        )

        #expect {
            try CorpusAnnotationLoader(repoRoot: root).loadAll()
        } throws: { error in
            guard case CorpusAnnotationLoaderError.evaluationCohortIncomplete(let detail) = error else {
                return false
            }
            return detail.contains("symbolic link")
        }
    }

    @Test("Review artifacts cannot be replayed after annotation decisions change")
    func reviewArtifactRejectsChangedAnnotationDecision() throws {
        let sourceLoader = CorpusAnnotationLoader()
        let sourceURL = try #require(try sourceLoader.annotationFileURLs().first { url in
            try sourceLoader.decode(at: url).adWindows.isEmpty == false
        })
        let sourceAnnotation = try sourceLoader.decode(at: sourceURL)
        let attestation = try #require(sourceAnnotation.reviewAttestations?.first)
        let artifactName = String(
            attestation.reviewArtifactId.dropFirst(CorpusAudioFingerprint.prefix.count)
        ) + ".json"
        let sourceArtifact = sourceLoader.repoRoot
            .appendingPathComponent("TestFixtures/Corpus/Reviews", isDirectory: true)
            .appendingPathComponent(artifactName)

        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let stagedAnnotation = annotations.appendingPathComponent(sourceURL.lastPathComponent)
        try FileManager.default.copyItem(at: sourceURL, to: stagedAnnotation)
        let manifest = [
            "schema_version": 1,
            "annotations": [sourceURL.lastPathComponent],
        ] as [String: Any]
        try JSONSerialization.data(withJSONObject: manifest).write(
            to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
        )
        let reviews = root.appendingPathComponent(
            "TestFixtures/Corpus/Reviews",
            isDirectory: true
        )
        try FileManager.default.createDirectory(at: reviews, withIntermediateDirectories: true)
        try FileManager.default.copyItem(
            at: sourceArtifact,
            to: reviews.appendingPathComponent(artifactName)
        )

        let loader = CorpusAnnotationLoader(repoRoot: root)
        #expect(try loader.loadAll().count == 1)

        var changed = try #require(
            try JSONSerialization.jsonObject(with: Data(contentsOf: stagedAnnotation))
                as? [String: Any]
        )
        var adWindows = try #require(changed["ad_windows"] as? [[String: Any]])
        adWindows[0]["advertiser"] = "Changed after review"
        changed["ad_windows"] = adWindows
        try JSONSerialization.data(withJSONObject: changed, options: [.sortedKeys]).write(
            to: stagedAnnotation
        )

        #expect {
            try loader.loadAll()
        } throws: { error in
            guard case CorpusAnnotationLoaderError.evaluationCohortIncomplete(let detail) = error else {
                return false
            }
            return detail.contains("unresolved review evidence")
        }
    }

    @Test("Review artifact integer fields reject floating JSON numbers")
    func reviewArtifactRejectsFloatingIntegerFields() throws {
        let sourceLoader = CorpusAnnotationLoader()
        let sourceURL = try #require(try sourceLoader.annotationFileURLs().first)
        let sourceAnnotation = try sourceLoader.decode(at: sourceURL)
        let attestation = try #require(sourceAnnotation.reviewAttestations?.first)
        let sourceArtifact = sourceLoader.repoRoot
            .appendingPathComponent("TestFixtures/Corpus/Reviews", isDirectory: true)
            .appendingPathComponent(
                String(attestation.reviewArtifactId.dropFirst("sha256:".count)) + ".json"
            )
        let artifactText = try String(contentsOf: sourceArtifact, encoding: .utf8)

        for (integer, floating) in [
            ("\"schema_version\": 1", "\"schema_version\": 1.0"),
            ("\"source_decision_count\": 33", "\"source_decision_count\": 33.0"),
        ] {
            let poisonedText = artifactText.replacingOccurrences(of: integer, with: floating)
            #expect(poisonedText != artifactText)
            let poisonedData = Data(poisonedText.utf8)
            let poisonedID = CorpusAudioFingerprint.fingerprint(of: poisonedData)

            let root = try makeTemporaryCorpusRoot()
            defer { try? FileManager.default.removeItem(at: root) }
            let annotations = root.appendingPathComponent(
                CorpusAnnotationLoader.annotationsRelativePath,
                isDirectory: true
            )
            var document = try #require(
                try JSONSerialization.jsonObject(with: Data(contentsOf: sourceURL))
                    as? [String: Any]
            )
            var attestations = try #require(
                document["review_attestations"] as? [[String: Any]]
            )
            attestations[0]["review_artifact_id"] = poisonedID
            document["review_attestations"] = attestations
            try JSONSerialization.data(withJSONObject: document).write(
                to: annotations.appendingPathComponent(sourceURL.lastPathComponent)
            )
            try JSONSerialization.data(withJSONObject: [
                "schema_version": 1,
                "annotations": [sourceURL.lastPathComponent],
            ]).write(
                to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
            )
            let reviews = root.appendingPathComponent(
                "TestFixtures/Corpus/Reviews",
                isDirectory: true
            )
            try FileManager.default.createDirectory(at: reviews, withIntermediateDirectories: true)
            try poisonedData.write(
                to: reviews.appendingPathComponent(
                    String(poisonedID.dropFirst("sha256:".count)) + ".json"
                )
            )

            #expect(throws: CorpusAnnotationLoaderError.self) {
                try CorpusAnnotationLoader(repoRoot: root).loadAll()
            }
        }
    }

    @Test("Gold preflight rejects corrupt staged transcripts even when audio is absent")
    func goldEvaluationPreflightRejectsCorruptTranscriptWithoutAudio() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let annotationURL = annotations.appendingPathComponent("corpus-001.json")
        try CorpusAnnotation.encoder.encode(makeWellFormedAnnotation()).write(to: annotationURL)
        try Data("{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8)
            .write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))
        let transcripts = root.appendingPathComponent(
            "TestFixtures/Corpus/Transcripts",
            isDirectory: true
        )
        try FileManager.default.createDirectory(at: transcripts, withIntermediateDirectories: true)
        try Data("{not-json}".utf8).write(
            to: transcripts.appendingPathComponent("corpus-001.json")
        )

        let loader = CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false)
        #expect(throws: DecodingError.self) {
            try loader.preflightGoldEvaluationInputs(
                annotationURLs: loader.annotationFileURLs()
            )
        }
    }

    @Test("Gold preflight rejects staged transcripts with no segments")
    func goldEvaluationPreflightRejectsEmptyTranscript() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let annotationURL = annotations.appendingPathComponent("corpus-001.json")
        let fingerprint = try stageBoundAudio(root: root, episodeId: "corpus-001")
        try CorpusAnnotation.encoder.encode(
            makeWellFormedAnnotation(fingerprint: fingerprint)
        ).write(to: annotationURL)
        try Data("{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8)
            .write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))
        let transcripts = root.appendingPathComponent(
            "TestFixtures/Corpus/Transcripts",
            isDirectory: true
        )
        try FileManager.default.createDirectory(at: transcripts, withIntermediateDirectories: true)
        try Data("{\"source_audio_fingerprint\":\"\(fingerprint)\",\"transcription\":[]}".utf8).write(
            to: transcripts.appendingPathComponent("corpus-001.json")
        )

        let loader = CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false)
        #expect {
            try loader.preflightGoldEvaluationInputs(
                annotationURLs: loader.annotationFileURLs()
            )
        } throws: { error in
            guard case CorpusAnnotationLoaderError.evaluationCohortIncomplete(let detail) = error else {
                return false
            }
            return detail.contains("staged transcript has no usable segments for corpus-001")
        }
    }

    @Test("Gold preflight rejects staged transcripts without a usable segment")
    func goldEvaluationPreflightRejectsSemanticallyEmptyTranscript() throws {
        let invalidTranscripts = [
            (
                """
                {"transcription":[{"text":"   ","offsets":{"from":0,"to":1000}}]}
                """,
                "invalid segment 0"
            ),
            (
                """
                {"transcription":[{"text":"recognized words","offsets":{"from":1000,"to":1000}}]}
                """,
                "invalid segment 0"
            ),
            (
                """
                {"transcription":[
                  {"text":"valid words","offsets":{"from":0,"to":1000}},
                  {"text":"negative words","offsets":{"from":-1000,"to":-500}}
                ]}
                """,
                "invalid segment 1"
            ),
            (
                """
                {"transcription":[
                  {"text":"first words","offsets":{"from":0,"to":1000}},
                  {"text":"overlap words","offsets":{"from":500,"to":1500}}
                ]}
                """,
                "segments overlap or are out of order at 1"
            ),
            (
                """
                {"transcription":[{"text":"late words","offsets":{"from":602000,"to":603000}}]}
                """,
                "segment 0 exceeds episode duration"
            ),
        ]

        for (transcript, expectedDetail) in invalidTranscripts {
            let root = try makeTemporaryCorpusRoot()
            defer { try? FileManager.default.removeItem(at: root) }
            let annotations = root.appendingPathComponent(
                CorpusAnnotationLoader.annotationsRelativePath
            )
            let fingerprint = try stageBoundAudio(root: root, episodeId: "corpus-001")
            try CorpusAnnotation.encoder.encode(
                makeWellFormedAnnotation(fingerprint: fingerprint)
            ).write(
                to: annotations.appendingPathComponent("corpus-001.json")
            )
            try Data(
                "{\"schema_version\":1,\"annotations\":[\"corpus-001.json\"]}".utf8
            ).write(
                to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename)
            )
            let transcripts = root.appendingPathComponent(
                "TestFixtures/Corpus/Transcripts",
                isDirectory: true
            )
            try FileManager.default.createDirectory(
                at: transcripts,
                withIntermediateDirectories: true
            )
            let boundTranscript = transcript.replacingOccurrences(
                of: "{",
                with: "{\"source_audio_fingerprint\":\"\(fingerprint)\",",
                options: [.anchored]
            )
            try Data(boundTranscript.utf8).write(
                to: transcripts.appendingPathComponent("corpus-001.json")
            )

            let loader = CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false)
            #expect {
                try loader.preflightGoldEvaluationInputs(
                    annotationURLs: loader.annotationFileURLs()
                )
            } throws: { error in
                guard case CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                    let detail
                ) = error else {
                    return false
                }
                return detail.contains(expectedDetail) && detail.contains("corpus-001")
            }
        }
    }

    @Test("Gold preflight rejects partial canonical and sidecar cohorts")
    func goldEvaluationPreflightRejectsPartialCohorts() throws {
        let root = try makeTemporaryCorpusRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let annotations = root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath)
        let firstFingerprint = try stageBoundAudio(root: root, episodeId: "corpus-001")
        let secondFingerprint = try stageBoundAudio(root: root, episodeId: "corpus-002")
        let first = makeWellFormedAnnotation(fingerprint: firstFingerprint)
        let second = makeWellFormedAnnotation(
            episodeId: "corpus-002",
            fingerprint: secondFingerprint
        )
        let firstURL = annotations.appendingPathComponent("corpus-001.json")
        let secondURL = annotations.appendingPathComponent("corpus-002.json")
        try CorpusAnnotation.encoder.encode(first).write(to: firstURL)
        try CorpusAnnotation.encoder.encode(second).write(to: secondURL)
        try Data(
            "{\"schema_version\":1,\"annotations\":[\"corpus-001.json\",\"corpus-002.json\"]}".utf8
        ).write(to: annotations.appendingPathComponent(CorpusAnnotationLoader.manifestFilename))

        let loader = CorpusAnnotationLoader(repoRoot: root, verifyReviewArtifacts: false)
        #expect(throws: CorpusAnnotationLoaderError.self) {
            try loader.preflightGoldEvaluationInputs(annotationURLs: [firstURL])
        }
        try loader.preflightGoldEvaluationInputs(
            annotationURLs: loader.annotationFileURLs()
        )

        let transcripts = root.appendingPathComponent(
            "TestFixtures/Corpus/Transcripts",
            isDirectory: true
        )
        try FileManager.default.createDirectory(at: transcripts, withIntermediateDirectories: true)
        let transcript = """
        {"source_audio_fingerprint":"\(firstFingerprint)","transcription":[{"text":"reviewed words","offsets":{"from":0,"to":1000}}]}
        """
        try Data(transcript.utf8).write(
            to: transcripts.appendingPathComponent("corpus-001.json")
        )
        #expect {
            try loader.preflightGoldEvaluationInputs(
                annotationURLs: loader.annotationFileURLs()
            )
        } throws: { error in
            guard case CorpusAnnotationLoaderError.evaluationCohortIncomplete(let detail) = error else {
                return false
            }
            return detail.contains("transcript inputs cover 1 of 2")
        }
    }
}

private func makeTemporaryCorpusRoot() throws -> URL {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(
        "l2f-manifest-\(UUID().uuidString)"
    )
    try FileManager.default.createDirectory(
        at: root.appendingPathComponent(CorpusAnnotationLoader.annotationsRelativePath),
        withIntermediateDirectories: true
    )
    return root
}

private func stageBoundAudio(root: URL, episodeId: String) throws -> String {
    let data = Data("retained audio for \(episodeId)".utf8)
    let directory = root.appendingPathComponent(
        CorpusAnnotationLoader.audioRelativePath,
        isDirectory: true
    )
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    try data.write(to: directory.appendingPathComponent("\(episodeId).mp3"))
    return CorpusAudioFingerprint.fingerprint(of: data)
}

// MARK: - Replay Adapter

@Suite("CorpusAnnotation – ReplaySimulator adapter")
struct CorpusAnnotationReplayAdapterTests {

    @Test("groundTruthSegments preserve start/end and metadata")
    func groundTruthMappingPreservesFields() {
        let ann = makeWellFormedAnnotation()
        let gt = ann.groundTruthSegments()
        #expect(gt.count == ann.adWindows.count)
        for (a, b) in zip(ann.adWindows, gt) {
            #expect(a.startSeconds == b.startTime)
            #expect(a.endSeconds == b.endTime)
            #expect(a.advertiser == b.advertiser)
            #expect(a.product == b.product)
        }
    }

    @Test("makeReplayConfiguration spans the full episode duration")
    func replayConfigCoversDuration() {
        let ann = makeWellFormedAnnotation()
        let cfg = ann.makeReplayConfiguration(
            condition: SimulationCondition(
                audioMode: .cached,
                playbackSpeed: 1.0,
                interactions: []
            )
        )
        #expect(cfg.episodeDuration == ann.durationSeconds)
        #expect(cfg.episodeId == ann.episodeId)
        if let last = cfg.transcriptChunks.last {
            #expect(last.endTime <= ann.durationSeconds + 0.0001)
            #expect(last.endTime >= ann.durationSeconds - 10.0001)
        }
        #expect(cfg.groundTruthSegments.count == ann.adWindows.count)
    }
}

// MARK: - Position-derived AdSegmentType classification

@Suite("CorpusAnnotation – Position-derived AdSegmentType")
struct CorpusAnnotationAdSegmentTypeTests {
    /// Small offset used to probe boundary behavior. Larger than the
    /// annotation epsilon (50 ms) used elsewhere in the validator so
    /// that "epsilon" tests genuinely sit on the open side of the
    /// strict-less-than comparison.
    private static let boundaryEpsilon: Double = 0.5

    // MARK: Pre-roll boundary

    @Test("start_seconds == 0 → preRoll")
    func startAtZeroIsPreRoll() {
        // 600 s episode → threshold = max(30, 0.01 * 600) = max(30, 6) = 30.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(0, 25)],
            content: [(25, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .preRoll)
    }

    @Test("start_seconds == threshold - epsilon → preRoll")
    func startJustBelowThresholdIsPreRoll() {
        // 600 s episode → threshold = 30.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(30 - Self.boundaryEpsilon, 60)],
            content: [(0, 30 - Self.boundaryEpsilon), (60, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .preRoll)
    }

    @Test("start_seconds == threshold → midRoll (strict-less-than boundary)")
    func startEqualThresholdIsMidRoll() {
        // 600 s episode → threshold = 30.
        // At exactly 30s the strict `<` comparison makes this midRoll.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(30, 60)],
            content: [(0, 30), (60, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .midRoll)
    }

    @Test("start_seconds == threshold + epsilon → midRoll")
    func startJustAboveThresholdIsMidRoll() {
        // 600 s episode → threshold = 30.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(30 + Self.boundaryEpsilon, 60)],
            content: [(0, 30 + Self.boundaryEpsilon), (60, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .midRoll)
    }

    // MARK: Post-roll boundary

    @Test("end_seconds == duration → postRoll")
    func endAtDurationIsPostRoll() {
        // 600 s episode → threshold = 30. Ad ends at 600 > 600 - 30 = 570.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(575, 600)],
            content: [(0, 575)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .postRoll)
    }

    @Test("end_seconds == duration - threshold + epsilon → postRoll")
    func endJustInsidePostRollWindowIsPostRoll() {
        // 600 s episode → threshold = 30. duration - threshold = 570.
        // end at 570 + epsilon is strictly greater than 570 → postRoll.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(560, 570 + Self.boundaryEpsilon)],
            content: [(0, 560), (570 + Self.boundaryEpsilon, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .postRoll)
    }

    @Test("end_seconds == duration - threshold → midRoll (strict-greater-than boundary)")
    func endEqualPostRollBoundaryIsMidRoll() {
        // 600 s episode → threshold = 30, post-roll boundary = 570.
        // At exactly 570 the strict `>` comparison makes this midRoll.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(540, 570)],
            content: [(0, 540), (570, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .midRoll)
    }

    @Test("end_seconds == duration - threshold - epsilon → midRoll (negative control)")
    func endJustBelowPostRollWindowIsMidRoll() {
        // 600 s episode → threshold = 30, post-roll boundary = 570.
        // end at 570 - epsilon is below the boundary → midRoll.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(540, 570 - Self.boundaryEpsilon)],
            content: [(0, 540), (570 - Self.boundaryEpsilon, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .midRoll)
    }

    // MARK: OR semantics across the absolute / relative threshold arms

    @Test("OR semantics: 30 s ad in 60-min episode is preRoll (1% arm dominates)")
    func orSemanticsLongEpisode() {
        // 3600 s episode → threshold = max(30, 0.01 * 3600) = max(30, 36) = 36.
        // An ad starting at exactly 30 s is below the 36 s effective
        // threshold and must classify as preRoll. This is the user's
        // explicit acceptance criterion: "should classify a 0:30 ad
        // in a 60-min episode as .preRoll even though 30s == [the
        // absolute] threshold".
        let ann = makeAnnotation(
            duration: 3600,
            ads: [(30, 90)],
            content: [(0, 30), (90, 3600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .preRoll)
    }

    @Test("OR semantics: 36 s ad start in 60-min episode falls to midRoll at the relative-arm boundary")
    func orSemanticsRelativeArmBoundary() {
        // 3600 s episode → threshold = max(30, 36) = 36. Strictly at
        // 36 the comparison fails, so the ad is mid-roll. This pins
        // down the boundary on the larger arm.
        let ann = makeAnnotation(
            duration: 3600,
            ads: [(36, 90)],
            content: [(0, 36), (90, 3600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .midRoll)
    }

    @Test("OR semantics: short episode falls back to absolute 30 s arm")
    func orSemanticsShortEpisode() {
        // 600 s episode → threshold = max(30, 6) = 30. The 1 % arm
        // would be 6 s but the absolute arm dominates.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(20, 50)],
            content: [(0, 20), (50, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .preRoll)
    }

    // MARK: Tiny-episode precedence

    @Test("Tiny episode where pre + post > duration: pre-roll wins")
    func tinyEpisodePrecedence() {
        // 30 s episode → threshold = 30. pre + post = 60 > 30, so an
        // ad starting at 0 simultaneously satisfies preRoll
        // (start < 30) AND postRoll (end > 30 - 30 = 0). Documented
        // precedence: pre-roll wins.
        let ann = makeAnnotation(
            duration: 30,
            ads: [(0, 30)],
            content: []
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .preRoll)
    }

    @Test("Episode-spanning ad in long episode: pre-roll wins")
    func episodeSpanningAdPrefersPreRoll() {
        // 600 s episode → threshold = 30. Ad covers entire episode:
        // start=0 (preRoll) AND end=600 > 570 (postRoll). Pre-roll wins.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(0, 600)],
            content: []
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .preRoll)
    }

    // MARK: Interior (negative-control) midRoll cases

    @Test("Interior ad far from both boundaries is midRoll")
    func interiorAdIsMidRoll() {
        // 600 s episode → threshold = 30. Ad at [180, 240]: well past
        // the 30 s pre-roll cutoff, well before the 570 s post-roll
        // cutoff. Belongs to the dominant midRoll bucket.
        let ann = makeAnnotation(
            duration: 600,
            ads: [(180, 240)],
            content: [(0, 180), (240, 600)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.first?.adType == .midRoll)
    }

    @Test("Multiple ads classify independently")
    func multipleAdsClassifyIndependently() {
        // 600 s episode → threshold = 30.
        // Window 1: [0, 25]      → preRoll  (start < 30).
        // Window 2: [180, 240]   → midRoll  (interior).
        // Window 3: [580, 600]   → postRoll (end > 570).
        let ann = makeAnnotation(
            duration: 600,
            ads: [(0, 25), (180, 240), (580, 600)],
            content: [(25, 180), (240, 580)]
        )
        let gt = ann.groundTruthSegments()
        #expect(gt.count == 3)
        #expect(gt[0].adType == .preRoll)
        #expect(gt[1].adType == .midRoll)
        #expect(gt[2].adType == .postRoll)
    }

    // MARK: Threshold accessor sanity

    @Test("rollBoundaryThresholdSeconds applies max(30, 1% * duration)")
    func thresholdAccessorReturnsMax() {
        // Short episode: absolute arm dominates.
        let short = makeAnnotation(duration: 600, ads: [], content: [(0, 600)])
        #expect(short.rollBoundaryThresholdSeconds == 30.0)

        // Long episode: relative arm dominates.
        let long = makeAnnotation(duration: 3600, ads: [], content: [(0, 3600)])
        #expect(long.rollBoundaryThresholdSeconds == 36.0)

        // Boundary case: 1% of 3000 == 30 → both arms tied, max == 30.
        let tied = makeAnnotation(duration: 3000, ads: [], content: [(0, 3000)])
        #expect(tied.rollBoundaryThresholdSeconds == 30.0)
    }
}

// MARK: - Test Fixtures

/// A valid-looking 64-char lowercase hex digest for placeholder
/// fingerprints in tests. Real digests are computed by
/// `CorpusAudioFingerprint`; tests that don't actually verify the
/// audio file just need something that passes the format check.
private let placeholderFingerprint =
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

private func makeWellFormedAnnotation(
    episodeId: String = "corpus-001",
    fingerprint: String = placeholderFingerprint
) -> CorpusAnnotation {
    CorpusAnnotation(
        episodeId: episodeId,
        showName: "Test Show",
        durationSeconds: 600,
        adWindows: [
            .init(
                startSeconds: 180,
                endSeconds: 240,
                advertiser: "Squarespace",
                product: "Website builder",
                adType: .hostRead,
                transitionType: .explicit,
                confidenceNotes: "Clear brought-to-you-by intro",
                provenance: ["human_reviewed"]
            ),
            .init(
                startSeconds: 400,
                endSeconds: 460,
                advertiser: "BetterHelp",
                product: "Therapy",
                adType: .blendedHostRead,
                transitionType: .blended,
                confidenceNotes: nil,
                provenance: ["human_reviewed"]
            ),
        ],
        contentWindows: [
            .init(startSeconds: 0, endSeconds: 180, notes: "Pre-ad content"),
            .init(startSeconds: 240, endSeconds: 400, notes: nil),
            .init(startSeconds: 460, endSeconds: 600, notes: "Closing"),
        ],
        variantOf: nil,
        audioFingerprint: fingerprint,
        provenance: ["human_reviewed"],
        reviewAttestations: [
            .init(
                reviewer: "Reviewer One",
                reviewedAt: "2026-05-12T03:06:35Z",
                audioFingerprint: fingerprint,
                reviewArtifactId: "sha256:" + String(repeating: "a", count: 64)
            ),
            .init(
                reviewer: "Reviewer Two",
                reviewedAt: "2026-07-10T12:00:00Z",
                audioFingerprint: fingerprint,
                reviewArtifactId: "sha256:" + String(repeating: "b", count: 64)
            ),
        ]
    )
}

private func makeAnnotation(
    duration: Double,
    ads: [(Double, Double)],
    content: [(Double, Double)],
    fingerprint: String = placeholderFingerprint
) -> CorpusAnnotation {
    CorpusAnnotation(
        episodeId: "ep-test",
        showName: "Test",
        durationSeconds: duration,
        adWindows: ads.map {
            .init(
                startSeconds: $0.0,
                endSeconds: $0.1,
                advertiser: nil,
                product: nil,
                adType: .hostRead,
                transitionType: .explicit,
                confidenceNotes: nil
            )
        },
        contentWindows: content.map {
            .init(startSeconds: $0.0, endSeconds: $0.1, notes: nil)
        },
        variantOf: nil,
        audioFingerprint: fingerprint
    )
}
