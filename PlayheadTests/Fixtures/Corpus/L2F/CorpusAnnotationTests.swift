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
