// CorpusAnnotationLoader.swift
// Loads, decodes, and validates `CorpusAnnotation` JSON files from
// `TestFixtures/Corpus/Annotations/` for the playhead-l2f corpus.
//
// Validation is intentionally strict — annotations are ground truth for
// quality metrics, so silently accepting overlap/gap bugs would corrupt
// every downstream score. The loader fails loud with a precise error
// message rather than papering over malformed input.

import CoreFoundation
import Darwin
import Foundation
@testable import Playhead

// MARK: - Loader Errors

enum CorpusAnnotationLoaderError: Error, CustomStringConvertible {
    case directoryNotFound(URL)
    case manifestMissing(URL)
    case manifestDecodeFailed(URL, Error)
    case manifestInvalid(URL, String)
    case decodeFailed(URL, Error)
    case episodeIDMismatch(URL, actual: String)
    case validationFailed(URL, [CorpusValidationIssue])
    case fingerprintMismatch(URL, expected: String, actual: String)
    case audioFileMissing(URL, expectedAt: URL)
    case audioFileAmbiguous(String, matches: [URL])
    case evaluationCohortIncomplete(String)
    case publicationLockFailed(URL, String)

    var description: String {
        switch self {
        case .directoryNotFound(let url):
            return "Annotations directory not found at \(url.path)"
        case .manifestMissing(let url):
            return "Canonical annotation manifest not found at \(url.path)"
        case .manifestDecodeFailed(let url, let err):
            return "Failed to decode annotation manifest \(url.lastPathComponent): \(err.localizedDescription)"
        case .manifestInvalid(let url, let detail):
            return "Invalid annotation manifest \(url.lastPathComponent): \(detail)"
        case .decodeFailed(let url, let err):
            return "Failed to decode \(url.lastPathComponent): \(err.localizedDescription)"
        case .episodeIDMismatch(let url, let actual):
            return "Annotation \(url.lastPathComponent) contains episode_id '\(actual)'"
        case .validationFailed(let url, let issues):
            let joined = issues.map(\.description).joined(separator: "; ")
            return "Validation failed for \(url.lastPathComponent): \(joined)"
        case .fingerprintMismatch(let json, let expected, let actual):
            return "Fingerprint mismatch for \(json.lastPathComponent): expected \(expected), got \(actual)"
        case .audioFileMissing(let json, let expectedAt):
            return "Audio file referenced by \(json.lastPathComponent) is missing at \(expectedAt.path)"
        case .audioFileAmbiguous(let episodeID, let matches):
            let names = matches.map(\.lastPathComponent).joined(separator: ", ")
            return "Multiple audio files match episode_id '\(episodeID)': \(names)"
        case .evaluationCohortIncomplete(let detail):
            return "Gold evaluation cohort is incomplete: \(detail)"
        case .publicationLockFailed(let url, let detail):
            return "Cannot lock canonical corpus at \(url.path): \(detail)"
        }
    }
}

// MARK: - Validation Issue

/// A single problem found while validating a `CorpusAnnotation`.
struct CorpusValidationIssue: Sendable, Equatable, CustomStringConvertible {
    let kind: Kind
    let detail: String

    enum Kind: String, Sendable, Equatable {
        case episodeIDEmpty
        case showNameEmpty
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
        case reviewAttestationInvalid
        case humanReviewedWithoutTwoAttestations
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
struct CorpusAnnotationLoader: Sendable {

    private final class SnapshotCache: @unchecked Sendable {
        private let lock = NSLock()
        private var annotationsByURL: [URL: CorpusAnnotation] = [:]

        func annotation(at url: URL) -> CorpusAnnotation? {
            lock.lock()
            defer { lock.unlock() }
            return annotationsByURL[url]
        }

        func snapshot() -> [(url: URL, annotation: CorpusAnnotation)]? {
            lock.lock()
            defer { lock.unlock() }
            guard !annotationsByURL.isEmpty else { return nil }
            return annotationsByURL
                .map { (url: $0.key, annotation: $0.value) }
                .sorted { $0.url.path < $1.url.path }
        }

        func replace(with entries: [(url: URL, annotation: CorpusAnnotation)]) {
            lock.lock()
            defer { lock.unlock() }
            annotationsByURL = Dictionary(
                uniqueKeysWithValues: entries.map { ($0.url, $0.annotation) }
            )
        }
    }

    private final class PublicationProcessLockRegistry: @unchecked Sendable {
        private let registryLock = NSLock()
        private var locksByDirectory: [String: NSRecursiveLock] = [:]

        func processLock(for directory: URL) -> NSRecursiveLock {
            let key = directory
                .resolvingSymlinksInPath()
                .standardizedFileURL
                .path
            registryLock.lock()
            defer { registryLock.unlock() }
            if let existing = locksByDirectory[key] {
                return existing
            }
            let created = NSRecursiveLock()
            locksByDirectory[key] = created
            return created
        }
    }

    /// Filenames that are not permitted as canonical manifest entries.
    static let templateFilenameSuffix = ".example.json"
    static let templatePrefix = "_"
    static let manifestFilename = "_canonical-manifest.json"
    static let publicationLockFilename = ".canonical-manifest.lock"
    static let requiredGoldReviewArtifactKinds = [
        "corpus_review_attestation",
        "human_first_pass_attestation",
    ]
    private static let publicationProcessLocks = PublicationProcessLockRegistry()

    static func hasRequiredGoldReviewArtifactKinds(_ kinds: [String]) -> Bool {
        kinds.sorted() == requiredGoldReviewArtifactKinds
    }

    /// Repo-root-relative path to the corpus annotations directory.
    static let annotationsRelativePath = "TestFixtures/Corpus/Annotations"
    /// Repo-root-relative path to the corpus audio directory.
    static let audioRelativePath = "TestFixtures/Corpus/Audio"
    /// whisper.cpp segment endpoints may extend slightly past the probed audio
    /// duration because its final decoding window is quantized.
    static let transcriptTimelineToleranceSeconds = 2.0

    /// Repo root for the current source tree, derived from this file's
    /// `#filePath`. The corpus lives at the repo root, not inside the
    /// test bundle, because annotations may reference audio files too
    /// large to bundle into a UI test target.
    let repoRoot: URL
    let verifyReviewArtifacts: Bool
    private let snapshotCache: SnapshotCache
    private let publicationLockContentionObserver: (@Sendable () -> Void)?

    init(
        repoRoot: URL? = nil,
        verifyReviewArtifacts: Bool = true,
        publicationLockContentionObserver: (@Sendable () -> Void)? = nil,
        filePath: String = #filePath
    ) {
        self.verifyReviewArtifacts = verifyReviewArtifacts
        self.snapshotCache = SnapshotCache()
        self.publicationLockContentionObserver = publicationLockContentionObserver
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

    var manifestURL: URL {
        annotationsDirectoryURL.appendingPathComponent(Self.manifestFilename)
    }

    // MARK: - Loading

    /// List the canonical annotation URLs in manifest order.
    func annotationFileURLs() throws -> [URL] {
        try withCanonicalSnapshotLock {
            try annotationFileURLsUnlocked()
        }
    }

    private func annotationFileURLsUnlocked() throws -> [URL] {
        let dir = annotationsDirectoryURL
        guard FileManager.default.fileExists(atPath: dir.path) else {
            throw CorpusAnnotationLoaderError.directoryNotFound(dir)
        }
        guard FileManager.default.fileExists(atPath: manifestURL.path) else {
            throw CorpusAnnotationLoaderError.manifestMissing(manifestURL)
        }
        guard let manifestValues = try? manifestURL.resourceValues(
            forKeys: [.isRegularFileKey, .isSymbolicLinkKey]
        ), manifestValues.isRegularFile == true, manifestValues.isSymbolicLink != true else {
            throw CorpusAnnotationLoaderError.manifestInvalid(
                manifestURL,
                "canonical manifest must be a regular file, not a symbolic link"
            )
        }
        return try canonicalAnnotationFileURLs(from: manifestURL)
    }

    private struct CanonicalManifest: Decodable {
        let schemaVersion: Int
        let annotations: [String]
    }

    private struct RawJSONValidationError: LocalizedError {
        let detail: String

        var errorDescription: String? { detail }
    }

    private func canonicalAnnotationFileURLs(from url: URL) throws -> [URL] {
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw CorpusAnnotationLoaderError.manifestDecodeFailed(url, error)
        }
        guard let rawManifest = try? JSONSerialization.jsonObject(with: data)
                as? [String: Any],
              Self.strictInteger(rawManifest["schema_version"]) == 1
        else {
            throw CorpusAnnotationLoaderError.manifestInvalid(
                url,
                "schema_version must be the JSON integer 1"
            )
        }
        let manifest: CanonicalManifest
        do {
            manifest = try CorpusAnnotation.decoder.decode(CanonicalManifest.self, from: data)
        } catch {
            throw CorpusAnnotationLoaderError.manifestDecodeFailed(url, error)
        }
        guard manifest.schemaVersion == 1 else {
            throw CorpusAnnotationLoaderError.manifestInvalid(
                url,
                "unsupported schema_version \(manifest.schemaVersion)"
            )
        }
        guard !manifest.annotations.isEmpty else {
            throw CorpusAnnotationLoaderError.manifestInvalid(url, "annotations must not be empty")
        }

        let canonicalDirectory = annotationsDirectoryURL
            .resolvingSymlinksInPath()
            .standardizedFileURL
        var seen: Set<String> = []
        var seenCanonical: Set<URL> = []
        var resolved: [URL] = []
        for filename in manifest.annotations {
            let isBareFilename = !filename.isEmpty
                && !filename.hasPrefix("/")
                && !filename.contains("/")
                && !filename.contains("\\")
                && (filename as NSString).lastPathComponent == filename
            guard isBareFilename,
                  filename.hasSuffix(".json"),
                  !Self.isTemplate(filename)
            else {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    url,
                    "unsafe annotation entry '\(filename)'"
                )
            }
            guard seen.insert(filename).inserted else {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    url,
                    "duplicate annotation entry '\(filename)'"
                )
            }
            let annotationURL = annotationsDirectoryURL.appendingPathComponent(filename)
            if (try? annotationURL.resourceValues(forKeys: [.isSymbolicLinkKey]))?.isSymbolicLink == true {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    url,
                    "listed annotation must not be a symbolic link: '\(filename)'"
                )
            }
            var isDirectory: ObjCBool = false
            guard FileManager.default.fileExists(
                atPath: annotationURL.path,
                isDirectory: &isDirectory
            ), !isDirectory.boolValue else {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    url,
                    "listed annotation is missing: '\(filename)'"
                )
            }
            let canonicalAnnotation = annotationURL
                .resolvingSymlinksInPath()
                .standardizedFileURL
            guard canonicalAnnotation.deletingLastPathComponent() == canonicalDirectory else {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    url,
                    "listed annotation escapes the annotations directory: '\(filename)'"
                )
            }
            guard let values = try? canonicalAnnotation.resourceValues(forKeys: [.isRegularFileKey]),
                  values.isRegularFile == true
            else {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    url,
                    "listed annotation is not a regular file: '\(filename)'"
                )
            }
            guard seenCanonical.insert(canonicalAnnotation).inserted else {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    url,
                    "duplicate resolved annotation entry '\(filename)'"
                )
            }
            resolved.append(canonicalAnnotation)
        }
        return resolved
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
            try Self.validateRawAnnotationIntegerFields(data)
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
        if let pinned = snapshotCache.annotation(at: url) {
            if verifyFingerprint {
                try verify(audioFingerprintFor: pinned, jsonURL: url)
            }
            return pinned
        }
        let parent = url.deletingLastPathComponent()
            .resolvingSymlinksInPath()
            .standardizedFileURL
        let canonicalParent = annotationsDirectoryURL
            .resolvingSymlinksInPath()
            .standardizedFileURL
        guard parent == canonicalParent else {
            return try loadAndValidateUnlocked(
                at: url,
                verifyFingerprint: verifyFingerprint
            )
        }
        return try withCanonicalSnapshotLock {
            try loadAndValidateUnlocked(at: url, verifyFingerprint: verifyFingerprint)
        }
    }

    private func loadAndValidateUnlocked(
        at url: URL,
        verifyFingerprint: Bool = false,
        verifyArtifacts: Bool = true
    ) throws -> CorpusAnnotation {
        let annotation = try decode(at: url)
        let expectedEpisodeID = url.deletingPathExtension().lastPathComponent
        guard annotation.episodeId == expectedEpisodeID else {
            throw CorpusAnnotationLoaderError.episodeIDMismatch(url, actual: annotation.episodeId)
        }

        let issues = Self.validate(annotation)
        if !issues.isEmpty {
            throw CorpusAnnotationLoaderError.validationFailed(url, issues)
        }

        if verifyFingerprint {
            try verify(audioFingerprintFor: annotation, jsonURL: url)
        }
        if verifyArtifacts && verifyReviewArtifacts {
            try validateReviewArtifacts([annotation])
        }
        return annotation
    }

    /// Load every annotation in `TestFixtures/Corpus/Annotations/`.
    /// - Parameter verifyAudioFingerprints: when `true`, also verifies
    ///   each `audio_fingerprint` against the referenced audio file.
    ///   Audio files live in `TestFixtures/Corpus/Audio/` and must
    ///   exist on disk.
    func loadAll(verifyAudioFingerprints: Bool = false) throws -> [CorpusAnnotation] {
        try withCanonicalSnapshotLock {
            let urls = try annotationFileURLsUnlocked()
            let annotations = try urls.map {
                try loadAndValidateUnlocked(
                    at: $0,
                    verifyFingerprint: verifyAudioFingerprints,
                    verifyArtifacts: false
                )
            }
            try validateUniqueAudioFingerprints(annotations, urls: urls)
            try validateVariantReferences(annotations, urls: urls)
            if verifyReviewArtifacts {
                try validateReviewArtifacts(annotations)
            }
            snapshotCache.replace(
                with: zip(urls, annotations).map { (url: $0.0, annotation: $0.1) }
            )
            return annotations
        }
    }

    /// Validate every canonical annotation and every staged input for the
    /// all-gold cohort before an accuracy harness starts model work or writes a
    /// report. An entirely absent sidecar class remains a lightweight-checkout
    /// soft skip, but partial gold coverage, malformed annotations, ambiguous
    /// cuts, hash drift, and transcript decode failures propagate.
    func preflightGoldEvaluationInputs(annotationURLs: [URL]) throws {
        try withCanonicalSnapshotLock {
            try preflightGoldEvaluationInputsUnlocked(annotationURLs: annotationURLs)
        }
    }

    private func preflightGoldEvaluationInputsUnlocked(annotationURLs: [URL]) throws {
        let canonical: [(url: URL, annotation: CorpusAnnotation)]
        if let pinned = snapshotCache.snapshot() {
            let byURL = Dictionary(
                uniqueKeysWithValues: pinned.map { ($0.url, $0.annotation) }
            )
            let expectedURLs = try annotationFileURLsUnlocked()
            canonical = expectedURLs.compactMap { url in
                byURL[url].map { (url: url, annotation: $0) }
            }
            guard canonical.count == pinned.count else {
                throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                    "canonical manifest changed after the evaluation snapshot was pinned"
                )
            }
        } else {
            canonical = try annotationFileURLsUnlocked().map { url in
                (
                    url: url,
                    annotation: try loadAndValidateUnlocked(
                        at: url,
                        verifyArtifacts: false
                    )
                )
            }
        }
        let expectedURLs = canonical.map(\.url)
        guard annotationURLs == expectedURLs else {
            throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                "caller supplied \(annotationURLs.count) annotation URLs; "
                    + "canonical manifest requires \(expectedURLs.count) in manifest order"
            )
        }
        try validateUniqueAudioFingerprints(
            canonical.map(\.annotation),
            urls: canonical.map(\.url)
        )
        try validateVariantReferences(
            canonical.map(\.annotation),
            urls: canonical.map(\.url)
        )
        if verifyReviewArtifacts {
            try validateReviewArtifacts(canonical.map(\.annotation))
        }
        let gold = canonical.filter { $0.annotation.isEligibleForGoldEvaluation }
        guard !gold.isEmpty else {
            throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                "canonical corpus has no explicitly human-reviewed gold episodes"
            )
        }
        var audioCount = 0
        var transcriptCount = 0
        for entry in gold {
            do {
                _ = try audioFileURL(for: entry.annotation)
                try verify(audioFingerprintFor: entry.annotation, jsonURL: entry.url)
                audioCount += 1
            } catch CorpusAnnotationLoaderError.audioFileMissing {
                // Audio is optional in lightweight checkouts. Still validate a
                // staged transcript below so corruption cannot become a skip.
            }
            let transcriptURL = repoRoot
                .appendingPathComponent("TestFixtures/Corpus/Transcripts", isDirectory: true)
                .appendingPathComponent("\(entry.annotation.episodeId).json", isDirectory: false)
            let transcriptIsStaged = FileManager.default.fileExists(atPath: transcriptURL.path)
            let transcript = try CorpusTranscriptLoader.load(
                episodeId: entry.annotation.episodeId,
                repoRoot: repoRoot
            )
            if transcriptIsStaged {
                guard !transcript.isEmpty else {
                    throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                        "staged transcript has no usable segments for "
                            + entry.annotation.episodeId
                    )
                }
                var previousEnd: Double?
                for (index, chunk) in transcript.enumerated() {
                    let hasText = !chunk.normalizedText.trimmingCharacters(
                        in: .whitespacesAndNewlines
                    ).isEmpty
                    guard chunk.startTime.isFinite,
                          chunk.endTime.isFinite,
                          chunk.startTime >= 0,
                          chunk.endTime > chunk.startTime,
                          hasText
                    else {
                        throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                            "staged transcript has invalid segment \(index) for "
                                + entry.annotation.episodeId
                        )
                    }
                    if let previousEnd, chunk.startTime < previousEnd {
                        throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                            "staged transcript segments overlap or are out of order at "
                                + "\(index) for \(entry.annotation.episodeId)"
                        )
                    }
                    guard chunk.endTime <= entry.annotation.durationSeconds
                        + Self.transcriptTimelineToleranceSeconds
                    else {
                        throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                            "staged transcript segment \(index) exceeds episode duration for "
                                + entry.annotation.episodeId
                        )
                    }
                    previousEnd = chunk.endTime
                }
                transcriptCount += 1
            }
        }
        for (kind, count) in [("audio", audioCount), ("transcript", transcriptCount)]
            where count != 0 && count != gold.count {
            throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                "staged \(kind) inputs cover \(count) of \(gold.count) gold episodes"
            )
        }
        snapshotCache.replace(with: canonical)
    }

    private func withCanonicalSnapshotLock<T>(_ operation: () throws -> T) throws -> T {
        let directory = annotationsDirectoryURL
        guard !Self.hasSymbolicLinkComponent(directory, relativeTo: repoRoot),
              let directoryValues = try? directory.resourceValues(
            forKeys: [.isDirectoryKey, .isSymbolicLinkKey]
        ), directoryValues.isDirectory == true, directoryValues.isSymbolicLink != true else {
            throw CorpusAnnotationLoaderError.directoryNotFound(directory)
        }
        let lockURL = directory.appendingPathComponent(Self.publicationLockFilename)
        let processLock = Self.publicationProcessLocks.processLock(for: directory)
        processLock.lock()
        defer { processLock.unlock() }

        let descriptor = Darwin.open(
            lockURL.path,
            O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW,
            S_IRUSR | S_IWUSR
        )
        guard descriptor >= 0 else {
            throw CorpusAnnotationLoaderError.publicationLockFailed(
                lockURL,
                String(cString: strerror(errno))
            )
        }
        defer { Darwin.close(descriptor) }
        if flock(descriptor, LOCK_EX | LOCK_NB) != 0 {
            let nonblockingError = errno
            guard nonblockingError == EWOULDBLOCK || nonblockingError == EAGAIN else {
                throw CorpusAnnotationLoaderError.publicationLockFailed(
                    lockURL,
                    String(cString: strerror(nonblockingError))
                )
            }
            publicationLockContentionObserver?()
            guard flock(descriptor, LOCK_EX) == 0 else {
                throw CorpusAnnotationLoaderError.publicationLockFailed(
                    lockURL,
                    String(cString: strerror(errno))
                )
            }
        }
        defer { flock(descriptor, LOCK_UN) }
        return try operation()
    }

    private func validateReviewArtifacts(_ annotations: [CorpusAnnotation]) throws {
        let required: [(
            annotation: CorpusAnnotation,
            attestations: [CorpusAnnotation.ReviewAttestation]
        )] = annotations.compactMap { annotation in
            guard let attestations = annotation.reviewAttestations,
                  !attestations.isEmpty
            else { return nil }
            return (annotation, attestations)
        }
        guard !required.isEmpty else { return }
        let reviewsDirectory = repoRoot.appendingPathComponent(
            "TestFixtures/Corpus/Reviews",
            isDirectory: true
        )
        guard !Self.hasSymbolicLinkComponent(reviewsDirectory, relativeTo: repoRoot),
              let directoryValues = try? reviewsDirectory.resourceValues(
            forKeys: [.isDirectoryKey, .isSymbolicLinkKey]
        ), directoryValues.isDirectory == true, directoryValues.isSymbolicLink != true else {
            throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                "review artifacts directory must be a directory, not a symbolic link"
            )
        }
        let files = try FileManager.default.contentsOfDirectory(
            at: reviewsDirectory,
            includingPropertiesForKeys: [.isRegularFileKey, .isSymbolicLinkKey],
            options: [.skipsHiddenFiles]
        ).filter { $0.pathExtension == "json" }
        var artifacts: [String: [String: Any]] = [:]
        for file in files {
            let values = try file.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey])
            guard values.isRegularFile == true, values.isSymbolicLink != true else {
                throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                    "review artifact is not a regular file: \(file.lastPathComponent)"
                )
            }
            let data = try Data(contentsOf: file)
            let fingerprint = CorpusAudioFingerprint.fingerprint(of: data)
            guard file.deletingPathExtension().lastPathComponent
                    == String(fingerprint.dropFirst(CorpusAudioFingerprint.prefix.count)),
                  let object = try JSONSerialization.jsonObject(with: data)
                    as? [String: Any]
            else {
                throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                    "review artifact is not content-addressed: \(file.lastPathComponent)"
                )
            }
            artifacts[fingerprint] = object
        }
        for (annotation, attestations) in required {
            for attestation in attestations {
                guard let artifact = artifacts[attestation.reviewArtifactId],
                      artifact["reviewer"] as? String == attestation.reviewer,
                      artifact["reviewed_at"] as? String == attestation.reviewedAt,
                      artifactBinds(artifact, annotation: annotation, attestation: attestation)
                else {
                    throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                        "unresolved review evidence for \(annotation.episodeId)"
                    )
                }
            }
            if annotation.isEligibleForGoldEvaluation {
                let artifactKinds = attestations.compactMap {
                    artifacts[$0.reviewArtifactId]?["artifact_kind"] as? String
                }.sorted()
                guard Self.hasRequiredGoldReviewArtifactKinds(artifactKinds) else {
                    throw CorpusAnnotationLoaderError.evaluationCohortIncomplete(
                        "gold review evidence for \(annotation.episodeId) must contain exactly "
                            + "one first-pass and one second-pass artifact"
                    )
                }
            }
        }
    }

    static func hasSymbolicLinkComponent(_ url: URL, relativeTo repoRoot: URL) -> Bool {
        let trustRoot = repoRoot.standardizedFileURL
        let absolute = url.standardizedFileURL
        let rootComponents = trustRoot.pathComponents
        let targetComponents = absolute.pathComponents
        guard targetComponents.starts(with: rootComponents) else { return true }
        var cursor = trustRoot
        for component in targetComponents.dropFirst(rootComponents.count) {
            cursor = cursor.appendingPathComponent(component)
            if (try? cursor.resourceValues(forKeys: [.isSymbolicLinkKey]))?
                .isSymbolicLink == true {
                return true
            }
        }
        return false
    }

    private struct ArtifactAnnotationDecision: Codable, Equatable {
        struct Ad: Codable, Equatable {
            let startSeconds: Double
            let endSeconds: Double
            let advertiser: String?
            let product: String?
            let adType: CorpusAnnotation.AdType
            let transitionType: CorpusAnnotation.TransitionType?
            let confidenceNotes: String?
        }

        struct Content: Codable, Equatable {
            let startSeconds: Double
            let endSeconds: Double
            let notes: String?
        }

        let episodeId: String
        let audioFingerprint: String
        let showName: String
        let durationSeconds: Double
        let variantOf: String?
        let adWindows: [Ad]
        let contentWindows: [Content]

        init(annotation: CorpusAnnotation) {
            episodeId = annotation.episodeId
            audioFingerprint = annotation.audioFingerprint
            showName = annotation.showName
            durationSeconds = annotation.durationSeconds
            variantOf = annotation.variantOf
            adWindows = annotation.adWindows.map {
                Ad(
                    startSeconds: $0.startSeconds,
                    endSeconds: $0.endSeconds,
                    advertiser: $0.advertiser,
                    product: $0.product,
                    adType: $0.adType,
                    transitionType: $0.transitionType,
                    confidenceNotes: $0.confidenceNotes
                )
            }
            contentWindows = annotation.contentWindows.map {
                Content(
                    startSeconds: $0.startSeconds,
                    endSeconds: $0.endSeconds,
                    notes: $0.notes
                )
            }
        }
    }

    private func decodeArtifactDecision(_ value: Any?) -> ArtifactAnnotationDecision? {
        let rootKeys = Set([
            "episode_id", "audio_fingerprint", "show_name", "duration_seconds",
            "variant_of", "ad_windows", "content_windows",
        ])
        let adKeys = Set([
            "start_seconds", "end_seconds", "advertiser", "product", "ad_type",
            "transition_type", "confidence_notes",
        ])
        let contentKeys = Set(["start_seconds", "end_seconds", "notes"])
        guard let object = value as? [String: Any],
              Set(object.keys) == rootKeys,
              let ads = object["ad_windows"] as? [[String: Any]],
              ads.allSatisfy({ Set($0.keys) == adKeys }),
              let content = object["content_windows"] as? [[String: Any]],
              content.allSatisfy({ Set($0.keys) == contentKeys }),
              JSONSerialization.isValidJSONObject(object),
              let data = try? JSONSerialization.data(withJSONObject: object),
              let decision = try? CorpusAnnotation.decoder.decode(
                ArtifactAnnotationDecision.self,
                from: data
              )
        else { return nil }
        return decision
    }

    private static func strictInteger(_ value: Any?) -> Int? {
        guard let number = value as? NSNumber,
              CFGetTypeID(number) != CFBooleanGetTypeID(),
              !CFNumberIsFloatType(number)
        else { return nil }
        let integer = number.intValue
        return number.doubleValue == Double(integer) ? integer : nil
    }

    private static func validateRawAnnotationIntegerFields(_ data: Data) throws {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else { return }

        func validateOptionalInteger(_ container: [String: Any], field: String) throws {
            guard let value = container[field], !(value is NSNull) else { return }
            guard Self.strictInteger(value) != nil else {
                throw RawJSONValidationError(
                    detail: "\(field) must be a JSON integer or null"
                )
            }
        }

        try validateOptionalInteger(object, field: "audit_priority")
        if let rawAttestations = object["review_attestations"], !(rawAttestations is NSNull),
           let attestations = rawAttestations as? [Any] {
            let requiredKeys = Set([
                "reviewer", "reviewed_at", "audio_fingerprint", "review_artifact_id",
            ])
            for (index, value) in attestations.enumerated() {
                guard let attestation = value as? [String: Any],
                      Set(attestation.keys) == requiredKeys
                else {
                    throw RawJSONValidationError(
                        detail: "review_attestations[\(index)] must contain exactly the four canonical keys"
                    )
                }
            }
        }
        guard let windows = object["ad_windows"] as? [Any] else { return }
        for (index, value) in windows.enumerated() {
            guard let window = value as? [String: Any] else { continue }
            do {
                try validateOptionalInteger(window, field: "audit_priority")
            } catch {
                throw RawJSONValidationError(
                    detail: "ad_windows[\(index)].audit_priority must be a JSON integer or null"
                )
            }
        }
    }

    private func canonicalReviewTimestamp(_ value: Any?) -> Bool {
        guard let value = value as? String else { return false }
        return CorpusAnnotation.isCanonicalReviewTimestamp(value)
    }

    private func artifactBinds(
        _ artifact: [String: Any],
        annotation: CorpusAnnotation,
        attestation: CorpusAnnotation.ReviewAttestation
    ) -> Bool {
        guard Self.strictInteger(artifact["schema_version"]) == 1,
              artifact["reviewer"] as? String == attestation.reviewer,
              artifact["reviewed_at"] as? String == attestation.reviewedAt,
              canonicalReviewTimestamp(artifact["reviewed_at"])
        else { return false }
        let episodeId = annotation.episodeId
        let fingerprint = annotation.audioFingerprint
        let expectedDecision = ArtifactAnnotationDecision(annotation: annotation)
        if artifact["artifact_kind"] as? String == "human_first_pass_attestation" {
            guard let count = Self.strictInteger(artifact["source_decision_count"]), count > 0,
                  let bindings = artifact["audio_bindings"] as? [[String: Any]]
            else { return false }
            let matches = bindings.filter {
                $0["episode_id"] as? String == episodeId
                    && $0["audio_fingerprint"] as? String == fingerprint
            }
            return matches.count == 1
                && decodeArtifactDecision(matches[0]["annotation_decision"]) == expectedDecision
        }
        guard artifact["artifact_kind"] as? String == "corpus_review_attestation",
              let episodes = artifact["episodes"] as? [[String: Any]],
              let reviews = artifact["reviews"] as? [String: [String: Any]]
        else { return false }
        let matches = episodes.filter {
            $0["episode_id"] as? String == episodeId
                && $0["audio_fingerprint"] as? String == fingerprint
        }
        guard
              matches.count == 1,
              let episode = matches.first,
              let decisionIds = episode["decision_ids"] as? [String],
              !decisionIds.isEmpty,
              Set(decisionIds).count == decisionIds.count,
              decisionIds.allSatisfy({ !$0.isEmpty }),
              decodeArtifactDecision(episode["annotation_decision"]) == expectedDecision
        else { return false }
        return decisionIds.allSatisfy { decisionId in
            let owners = episodes.filter {
                ($0["decision_ids"] as? [String])?.contains(decisionId) == true
            }
            guard let decision = reviews[decisionId],
                  let status = decision["status"] as? String
            else { return false }
            return owners.count == 1
                && decision["episode_id"] as? String == episodeId
                && decision["reviewer"] as? String == attestation.reviewer
                && decision["audio_fingerprint"] as? String == fingerprint
                && decision["reviewed_at"] as? String == attestation.reviewedAt
                && canonicalReviewTimestamp(decision["reviewed_at"])
                && ["verified_ad", "false_positive", "zero_ad_confirmed"]
                    .contains(status)
        }
    }

    /// Different episode ids must never score the same audio cut as independent
    /// examples. Duplicate bytes would leak one recording across train/eval strata
    /// and make aggregate accuracy appear more stable than the corpus really is.
    private func validateUniqueAudioFingerprints(
        _ annotations: [CorpusAnnotation],
        urls: [URL]
    ) throws {
        var owners: [String: URL] = [:]
        for (annotation, url) in zip(annotations, urls) {
            if let owner = owners[annotation.audioFingerprint] {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    manifestURL,
                    "\(owner.lastPathComponent) and \(url.lastPathComponent) "
                        + "share audio_fingerprint \(annotation.audioFingerprint)"
                )
            }
            owners[annotation.audioFingerprint] = url
        }
    }

    /// A variant is meaningful only when its parent is part of the same
    /// canonical corpus. Otherwise a typo can silently defeat pair-based evals.
    private func validateVariantReferences(
        _ annotations: [CorpusAnnotation],
        urls: [URL]
    ) throws {
        let episodeIDs = Set(annotations.map(\.episodeId))
        for (annotation, url) in zip(annotations, urls) {
            guard let parent = annotation.variantOf else { continue }
            guard episodeIDs.contains(parent) else {
                throw CorpusAnnotationLoaderError.manifestInvalid(
                    manifestURL,
                    "\(url.lastPathComponent) references non-canonical variant_of episode '\(parent)'"
                )
            }
        }
    }

    // MARK: - Audio Fingerprint Verification

    /// Audio file extensions accepted as podcast media. The corpus
    /// auto-resolver only matches these so a stray text/JSON file with
    /// a colliding stem can never be mis-fingerprinted as audio.
    static let audioFileExtensions: Set<String> = ["m4a", "mp3", "mp4", "aac", "wav", "flac"]

    /// Resolve the audio file for an annotation. Walks `Audio/` looking
    /// for any file whose stem equals the episode id and whose extension
    /// is in `audioFileExtensions`. Exactly one matching media file is
    /// required; choosing an arbitrary cut would make scores nondeterministic.
    func audioFileURL(for annotation: CorpusAnnotation) throws -> URL {
        let dir = audioDirectoryURL
        guard !Self.hasSymbolicLinkComponent(dir, relativeTo: repoRoot),
              let directoryValues = try? dir.resourceValues(
                forKeys: [.isDirectoryKey, .isSymbolicLinkKey]
              ),
              directoryValues.isDirectory == true,
              directoryValues.isSymbolicLink != true
        else {
            throw CorpusAnnotationLoaderError.audioFileMissing(
                URL(fileURLWithPath: annotation.episodeId),
                expectedAt: dir.appendingPathComponent(annotation.episodeId)
            )
        }
        let contents = try FileManager.default.contentsOfDirectory(
            at: dir,
            includingPropertiesForKeys: [.isRegularFileKey, .isSymbolicLinkKey],
            options: [.skipsHiddenFiles]
        )
        let matches = contents.filter {
            let values = try? $0.resourceValues(
                forKeys: [.isRegularFileKey, .isSymbolicLinkKey]
            )
            return $0.deletingPathExtension().lastPathComponent == annotation.episodeId
                && Self.audioFileExtensions.contains($0.pathExtension.lowercased())
                && values?.isRegularFile == true
                && values?.isSymbolicLink != true
        }.sorted { $0.lastPathComponent < $1.lastPathComponent }
        if matches.count > 1 {
            throw CorpusAnnotationLoaderError.audioFileAmbiguous(
                annotation.episodeId,
                matches: matches
            )
        }
        if let match = matches.first {
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
        } catch CorpusAnnotationLoaderError.audioFileMissing {
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

        if annotation.episodeId.isEmpty {
            issues.append(.init(kind: .episodeIDEmpty, detail: "episode_id must not be empty"))
        }
        if annotation.showName.isEmpty {
            issues.append(.init(kind: .showNameEmpty, detail: "show_name must not be empty"))
        }

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
        if !annotation.reviewAttestationsAreWellFormed {
            issues.append(.init(
                kind: .reviewAttestationInvalid,
                detail: "every review attestation must be normalized and match audio_fingerprint"
            ))
        }
        if annotation.hasHumanOnlyProvenance && !annotation.hasVerifiedReviewAttestations {
            issues.append(.init(
                kind: .humanReviewedWithoutTwoAttestations,
                detail: "human_reviewed requires two distinct reviewers and review artifacts"
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
