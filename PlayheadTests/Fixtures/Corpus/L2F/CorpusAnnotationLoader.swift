// CorpusAnnotationLoader.swift
// Loads, decodes, and validates `CorpusAnnotation` JSON files from
// `TestFixtures/Corpus/Annotations/` for the playhead-l2f corpus.
//
// Validation is intentionally strict — annotations are ground truth for
// quality metrics, so silently accepting overlap/gap bugs would corrupt
// every downstream score. The loader fails loud with a precise error
// message rather than papering over malformed input.

import Foundation

// MARK: - Loader Errors

enum CorpusAnnotationLoaderError: Error, CustomStringConvertible {
    case directoryNotFound(URL)
    case enumerateFailed(URL, Error)
    case decodeFailed(URL, Error)
    case validationFailed(URL, [CorpusValidationIssue])
    case fingerprintMismatch(URL, expected: String, actual: String)
    case audioFileMissing(URL, expectedAt: URL)

    var description: String {
        switch self {
        case .directoryNotFound(let url):
            return "Annotations directory not found at \(url.path)"
        case .enumerateFailed(let url, let err):
            return "Failed to enumerate \(url.path): \(err.localizedDescription)"
        case .decodeFailed(let url, let err):
            return "Failed to decode \(url.lastPathComponent): \(err.localizedDescription)"
        case .validationFailed(let url, let issues):
            let joined = issues.map(\.description).joined(separator: "; ")
            return "Validation failed for \(url.lastPathComponent): \(joined)"
        case .fingerprintMismatch(let json, let expected, let actual):
            return "Fingerprint mismatch for \(json.lastPathComponent): expected \(expected), got \(actual)"
        case .audioFileMissing(let json, let expectedAt):
            return "Audio file referenced by \(json.lastPathComponent) is missing at \(expectedAt.path)"
        }
    }
}

// MARK: - Validation Issue

/// A single problem found while validating a `CorpusAnnotation`.
struct CorpusValidationIssue: Sendable, Equatable, CustomStringConvertible {
    let kind: Kind
    let detail: String

    enum Kind: String, Sendable, Equatable {
        case nonPositiveDuration
        case fingerprintMissingPrefix
        case fingerprintMalformedHex
        case windowStartNegative
        case windowOutOfRange
        case windowEndBeforeStart
        case adWindowsOverlap
        case contentWindowsOverlap
        case adContentOverlap
        case timelineGap
        case partitionShortfall
        case partitionOvershoot
        case variantOfSelfReference
    }

    var description: String {
        "\(kind.rawValue): \(detail)"
    }
}

// MARK: - Loader

/// Loads and validates `CorpusAnnotation` files from disk.
///
/// Use `loadAll(verifyAudioFingerprints:)` to pull every annotation
/// from `TestFixtures/Corpus/Annotations/`, validate its structure, and
/// optionally verify each `audio_fingerprint` against the referenced
/// audio file in `TestFixtures/Corpus/Audio/`.
struct CorpusAnnotationLoader {

    /// Filenames in the annotations directory that are NOT real
    /// annotations and should be skipped by `loadAll`. Templates and
    /// schema-style sidecars live alongside the real annotations so
    /// they're easy to find, but must not run through validation.
    static let templateFilenameSuffix = ".example.json"
    static let templatePrefix = "_"

    /// Repo-root-relative path to the corpus annotations directory.
    static let annotationsRelativePath = "TestFixtures/Corpus/Annotations"
    /// Repo-root-relative path to the corpus audio directory.
    static let audioRelativePath = "TestFixtures/Corpus/Audio"

    /// Repo root for the current source tree, derived from this file's
    /// `#filePath`. The corpus lives at the repo root, not inside the
    /// test bundle, because annotations may reference audio files too
    /// large to bundle into a UI test target.
    let repoRoot: URL

    init(repoRoot: URL? = nil, filePath: String = #filePath) {
        if let repoRoot {
            self.repoRoot = repoRoot
        } else {
            // PlayheadTests/Fixtures/Corpus/L2F/CorpusAnnotationLoader.swift
            //                                          ^ this file
            // Walk up four parents to reach the repo root.
            self.repoRoot = URL(fileURLWithPath: filePath)
                .deletingLastPathComponent()  // L2F/
                .deletingLastPathComponent()  // Corpus/
                .deletingLastPathComponent()  // Fixtures/
                .deletingLastPathComponent()  // PlayheadTests/
                .deletingLastPathComponent()  // <repo root>
        }
    }

    var annotationsDirectoryURL: URL {
        repoRoot.appendingPathComponent(Self.annotationsRelativePath)
    }

    var audioDirectoryURL: URL {
        repoRoot.appendingPathComponent(Self.audioRelativePath)
    }

    // MARK: - Loading

    /// List the URLs of every real annotation JSON file (excluding
    /// templates) in the corpus annotations directory.
    func annotationFileURLs() throws -> [URL] {
        let dir = annotationsDirectoryURL
        guard FileManager.default.fileExists(atPath: dir.path) else {
            throw CorpusAnnotationLoaderError.directoryNotFound(dir)
        }
        let contents: [URL]
        do {
            contents = try FileManager.default.contentsOfDirectory(
                at: dir,
                includingPropertiesForKeys: nil,
                options: [.skipsHiddenFiles]
            )
        } catch {
            throw CorpusAnnotationLoaderError.enumerateFailed(dir, error)
        }
        return contents
            .filter { $0.pathExtension == "json" }
            .filter { !Self.isTemplate($0.lastPathComponent) }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
    }

    /// Returns true if a filename matches the template-exclusion rules.
    /// Files starting with `_` (e.g. `_template.example.json`) or ending
    /// in `.example.json` are treated as templates, not real annotations.
    static func isTemplate(_ filename: String) -> Bool {
        filename.hasPrefix(templatePrefix) || filename.hasSuffix(templateFilenameSuffix)
    }

    /// Decode a single annotation JSON file. Does not run validation.
    func decode(at url: URL) throws -> CorpusAnnotation {
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw CorpusAnnotationLoaderError.decodeFailed(url, error)
        }
        do {
            return try CorpusAnnotation.decoder.decode(CorpusAnnotation.self, from: data)
        } catch {
            throw CorpusAnnotationLoaderError.decodeFailed(url, error)
        }
    }

    /// Decode and validate a single annotation. Throws if validation
    /// produces any issue. If `verifyFingerprint` is true, also asserts
    /// the audio file at `audio/<episode_id>.<ext>` matches the recorded
    /// fingerprint.
    @discardableResult
    func loadAndValidate(at url: URL, verifyFingerprint: Bool = false) throws -> CorpusAnnotation {
        let annotation = try decode(at: url)

        let issues = Self.validate(annotation)
        if !issues.isEmpty {
            throw CorpusAnnotationLoaderError.validationFailed(url, issues)
        }

        if verifyFingerprint {
            try verify(audioFingerprintFor: annotation, jsonURL: url)
        }
        return annotation
    }

    /// Load every annotation in `TestFixtures/Corpus/Annotations/`.
    /// - Parameter verifyAudioFingerprints: when `true`, also verifies
    ///   each `audio_fingerprint` against the referenced audio file.
    ///   Audio files live in `TestFixtures/Corpus/Audio/` and must
    ///   exist on disk.
    func loadAll(verifyAudioFingerprints: Bool = false) throws -> [CorpusAnnotation] {
        try annotationFileURLs().map {
            try loadAndValidate(at: $0, verifyFingerprint: verifyAudioFingerprints)
        }
    }

    // MARK: - Audio Fingerprint Verification

    /// Audio file extensions accepted as podcast media. The corpus
    /// auto-resolver only matches these so a stray text/JSON file with
    /// a colliding stem can never be mis-fingerprinted as audio.
    static let audioFileExtensions: Set<String> = ["m4a", "mp3", "mp4", "aac", "wav", "flac"]

    /// Resolve the audio file for an annotation. Walks `Audio/` looking
    /// for any file whose stem equals the episode id and whose extension
    /// is in `audioFileExtensions`. Returns the first match or throws if
    /// nothing is present.
    func audioFileURL(for annotation: CorpusAnnotation) throws -> URL {
        let dir = audioDirectoryURL
        guard FileManager.default.fileExists(atPath: dir.path) else {
            throw CorpusAnnotationLoaderError.audioFileMissing(
                URL(fileURLWithPath: annotation.episodeId),
                expectedAt: dir.appendingPathComponent(annotation.episodeId)
            )
        }
        let contents = try FileManager.default.contentsOfDirectory(
            at: dir,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        )
        if let match = contents.first(where: {
            $0.deletingPathExtension().lastPathComponent == annotation.episodeId
                && Self.audioFileExtensions.contains($0.pathExtension.lowercased())
        }) {
            return match
        }
        throw CorpusAnnotationLoaderError.audioFileMissing(
            URL(fileURLWithPath: annotation.episodeId),
            expectedAt: dir.appendingPathComponent(annotation.episodeId)
        )
    }

    func verify(audioFingerprintFor annotation: CorpusAnnotation, jsonURL: URL) throws {
        let audio: URL
        do {
            audio = try audioFileURL(for: annotation)
        } catch {
            throw CorpusAnnotationLoaderError.audioFileMissing(
                jsonURL,
                expectedAt: audioDirectoryURL.appendingPathComponent(annotation.episodeId)
            )
        }
        let actual = try CorpusAudioFingerprint.fingerprint(of: audio)
        guard actual == annotation.audioFingerprint else {
            throw CorpusAnnotationLoaderError.fingerprintMismatch(
                jsonURL,
                expected: annotation.audioFingerprint,
                actual: actual
            )
        }
    }

    // MARK: - Validation

    /// Validate an annotation. Returns every issue found; an empty
    /// array means the annotation is well-formed.
    ///
    /// Validation rules:
    /// 1. `duration_seconds` is positive.
    /// 2. `audio_fingerprint` starts with the `sha256:` prefix.
    /// 3. Every window has `start < end`, both within `[0, duration]`.
    /// 4. `ad_windows` do not overlap each other.
    /// 5. `content_windows` do not overlap each other.
    /// 6. `ad_windows` do not overlap any `content_windows`.
    /// 7. The union of ad+content windows partitions `[0, duration]`
    ///    with no gaps and no overshoot.
    /// 8. `variant_of`, if set, is not the annotation's own `episode_id`.
    static func validate(_ annotation: CorpusAnnotation) -> [CorpusValidationIssue] {
        var issues: [CorpusValidationIssue] = []

        if annotation.durationSeconds <= 0 {
            issues.append(.init(
                kind: .nonPositiveDuration,
                detail: "duration_seconds=\(annotation.durationSeconds) must be > 0"
            ))
            // Stop early — every other rule depends on a positive duration.
            return issues
        }

        if !annotation.audioFingerprint.hasPrefix(CorpusAudioFingerprint.prefix) {
            issues.append(.init(
                kind: .fingerprintMissingPrefix,
                detail: "audio_fingerprint must start with '\(CorpusAudioFingerprint.prefix)'"
            ))
        } else {
            let suffix = annotation.audioFingerprint.dropFirst(CorpusAudioFingerprint.prefix.count)
            // SHA-256 emits 32 bytes = 64 lowercase hex chars. Encoder
            // writes lowercase via String(format: "%02x", _); enforce the
            // same on input so a typo or accidental uppercase doesn't
            // silently miscompare against a freshly computed fingerprint.
            let isLowerHex = suffix.allSatisfy { c in
                ("0"..."9").contains(c) || ("a"..."f").contains(c)
            }
            if suffix.count != 64 || !isLowerHex {
                issues.append(.init(
                    kind: .fingerprintMalformedHex,
                    detail: "audio_fingerprint hex digest must be 64 lowercase hex chars; got '\(suffix)' (length \(suffix.count))"
                ))
            }
        }

        if let variantOf = annotation.variantOf, variantOf == annotation.episodeId {
            issues.append(.init(
                kind: .variantOfSelfReference,
                detail: "variant_of points at this same episode_id"
            ))
        }

        // Per-window range checks.
        for (i, w) in annotation.adWindows.enumerated() {
            issues.append(contentsOf: validateRange(
                start: w.startSeconds,
                end: w.endSeconds,
                duration: annotation.durationSeconds,
                label: "ad_windows[\(i)]"
            ))
        }
        for (i, w) in annotation.contentWindows.enumerated() {
            issues.append(contentsOf: validateRange(
                start: w.startSeconds,
                end: w.endSeconds,
                duration: annotation.durationSeconds,
                label: "content_windows[\(i)]"
            ))
        }

        // If any per-window range failed, partition checks would just
        // emit redundant noise — stop here and let the annotator fix
        // the underlying ranges first.
        if !issues.isEmpty {
            return issues
        }

        // Internal overlap within a single window list.
        // Treat overlaps within `epsilon` (50ms) as touching boundaries,
        // not real overlaps — the corpus is annotated at ±0.5s precision
        // so adjacent windows often share an endpoint.
        let sortedAds = annotation.adWindows.sorted { $0.startSeconds < $1.startSeconds }
        if sortedAds.count >= 2 {
            for i in 1..<sortedAds.count
                where sortedAds[i].startSeconds + epsilon < sortedAds[i - 1].endSeconds {
                issues.append(.init(
                    kind: .adWindowsOverlap,
                    detail: "ad_windows overlap between [\(sortedAds[i - 1].startSeconds), \(sortedAds[i - 1].endSeconds)] and [\(sortedAds[i].startSeconds), \(sortedAds[i].endSeconds)]"
                ))
            }
        }
        let sortedContent = annotation.contentWindows.sorted { $0.startSeconds < $1.startSeconds }
        if sortedContent.count >= 2 {
            for i in 1..<sortedContent.count
                where sortedContent[i].startSeconds + epsilon < sortedContent[i - 1].endSeconds {
                issues.append(.init(
                    kind: .contentWindowsOverlap,
                    detail: "content_windows overlap between [\(sortedContent[i - 1].startSeconds), \(sortedContent[i - 1].endSeconds)] and [\(sortedContent[i].startSeconds), \(sortedContent[i].endSeconds)]"
                ))
            }
        }

        // Cross-list overlap (ad vs content). Same epsilon tolerance.
        let merged: [(Double, Double, String)] =
            annotation.adWindows.map { ($0.startSeconds, $0.endSeconds, "ad") } +
            annotation.contentWindows.map { ($0.startSeconds, $0.endSeconds, "content") }
        let mergedSorted = merged.sorted { $0.0 < $1.0 }
        if mergedSorted.count >= 2 {
            for i in 1..<mergedSorted.count
                where mergedSorted[i].0 + epsilon < mergedSorted[i - 1].1 {
                let (a0, a1, ka) = mergedSorted[i - 1]
                let (b0, b1, kb) = mergedSorted[i]
                if ka != kb {
                    issues.append(.init(
                        kind: .adContentOverlap,
                        detail: "\(ka) [\(a0), \(a1)] overlaps \(kb) [\(b0), \(b1)]"
                    ))
                }
            }
        }

        // Partition check: ad ∪ content must cover [0, duration] exactly.
        // Bail if we already failed overlap — the messages would be redundant.
        if issues.isEmpty {
            issues.append(contentsOf: partitionIssues(
                duration: annotation.durationSeconds,
                merged: mergedSorted.map { ($0.0, $0.1) }
            ))
        }

        return issues
    }

    private static func validateRange(
        start: Double,
        end: Double,
        duration: Double,
        label: String
    ) -> [CorpusValidationIssue] {
        var issues: [CorpusValidationIssue] = []
        if start < 0 {
            issues.append(.init(
                kind: .windowStartNegative,
                detail: "\(label) start_seconds=\(start) is negative"
            ))
        }
        if end <= start {
            issues.append(.init(
                kind: .windowEndBeforeStart,
                detail: "\(label) end_seconds=\(end) <= start_seconds=\(start)"
            ))
        }
        if end > duration + Self.epsilon {
            issues.append(.init(
                kind: .windowOutOfRange,
                detail: "\(label) end_seconds=\(end) exceeds duration_seconds=\(duration)"
            ))
        }
        if start > duration + Self.epsilon {
            issues.append(.init(
                kind: .windowOutOfRange,
                detail: "\(label) start_seconds=\(start) exceeds duration_seconds=\(duration)"
            ))
        }
        return issues
    }

    /// Tolerance for partition arithmetic. Annotations are recorded at
    /// ±0.5s precision; we allow 0.05s slack so a [0, 60.0] / [60.0001, 120]
    /// pairing doesn't trigger a spurious "gap" report.
    static let epsilon: Double = 0.05

    /// Verify that the merged window list covers `[0, duration]` with no
    /// gap and no overshoot.
    private static func partitionIssues(
        duration: Double,
        merged: [(Double, Double)]
    ) -> [CorpusValidationIssue] {
        guard !merged.isEmpty else {
            return [.init(
                kind: .partitionShortfall,
                detail: "no ad_windows or content_windows defined; cannot cover [0, \(duration)]"
            )]
        }
        let sorted = merged.sorted { $0.0 < $1.0 }
        var issues: [CorpusValidationIssue] = []

        if sorted.first!.0 > epsilon {
            issues.append(.init(
                kind: .timelineGap,
                detail: "gap from 0 to \(sorted.first!.0): coverage must start at 0"
            ))
        }
        for i in 1..<sorted.count {
            let prevEnd = sorted[i - 1].1
            let currStart = sorted[i].0
            if currStart > prevEnd + epsilon {
                issues.append(.init(
                    kind: .timelineGap,
                    detail: "gap from \(prevEnd) to \(currStart)"
                ))
            }
        }
        if let last = sorted.last {
            if last.1 + epsilon < duration {
                issues.append(.init(
                    kind: .partitionShortfall,
                    detail: "coverage ends at \(last.1) but duration_seconds=\(duration)"
                ))
            }
            if last.1 > duration + epsilon {
                issues.append(.init(
                    kind: .partitionOvershoot,
                    detail: "coverage ends at \(last.1) but duration_seconds=\(duration)"
                ))
            }
        }
        return issues
    }
}
