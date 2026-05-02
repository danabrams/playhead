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
import Foundation
import Testing
@testable import Playhead

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
}

// MARK: - Validator

@Suite("CorpusAnnotationLoader – Validator")
struct CorpusAnnotationValidatorTests {

    @Test("A well-formed annotation reports no issues")
    func wellFormed() {
        let issues = CorpusAnnotationLoader.validate(makeWellFormedAnnotation())
        #expect(issues.isEmpty, "unexpected issues: \(issues)")
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
        let loader = CorpusAnnotationLoader()
        // The corpus may be empty until Dan completes the labeling pass;
        // an empty result is acceptable here. The contract is "doesn't
        // crash and excludes templates".
        let annotations = try loader.loadAll(verifyAudioFingerprints: false)
        for a in annotations {
            #expect(!a.episodeId.isEmpty)
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

// MARK: - Test Fixtures

/// A valid-looking 64-char lowercase hex digest for placeholder
/// fingerprints in tests. Real digests are computed by
/// `CorpusAudioFingerprint`; tests that don't actually verify the
/// audio file just need something that passes the format check.
private let placeholderFingerprint =
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

private func makeWellFormedAnnotation() -> CorpusAnnotation {
    CorpusAnnotation(
        episodeId: "corpus-001",
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
                confidenceNotes: "Clear brought-to-you-by intro"
            ),
            .init(
                startSeconds: 400,
                endSeconds: 460,
                advertiser: "BetterHelp",
                product: "Therapy",
                adType: .blendedHostRead,
                transitionType: .blended,
                confidenceNotes: nil
            ),
        ],
        contentWindows: [
            .init(startSeconds: 0, endSeconds: 180, notes: "Pre-ad content"),
            .init(startSeconds: 240, endSeconds: 400, notes: nil),
            .init(startSeconds: 460, endSeconds: 600, notes: "Closing"),
        ],
        variantOf: nil,
        audioFingerprint: placeholderFingerprint
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
