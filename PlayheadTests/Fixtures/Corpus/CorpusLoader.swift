// CorpusLoader.swift
// Loads and validates test corpus annotations from JSON fixtures.
// Used by ReplaySimulator and other test suites to access labeled episodes.

import Foundation
@testable import Playhead

// MARK: - Corpus Loader Errors

enum CorpusLoaderError: Error, CustomStringConvertible {
    case manifestNotFound
    case annotationNotFound(String)
    case decodingFailed(String, Error)
    case validationFailed(String, [String])

    var description: String {
        switch self {
        case .manifestNotFound:
            "Corpus manifest.json not found in test fixtures"
        case .annotationNotFound(let file):
            "Annotation file not found: \(file)"
        case .decodingFailed(let file, let error):
            "Failed to decode \(file): \(error.localizedDescription)"
        case .validationFailed(let file, let errors):
            "Validation failed for \(file): \(errors.joined(separator: "; "))"
        }
    }
}

// MARK: - Corpus Loader

/// Reads the test corpus from the PlayheadTests bundle, validates annotations,
/// and provides typed access to labeled episodes.
struct CorpusLoader {

    /// The bundle containing test fixtures.
    private let bundle: Bundle

    /// Directory name within the bundle's resource path.
    private static let corpusDirectoryName = "Corpus"

    init(bundle: Bundle = Bundle(for: BundleToken.self)) {
        self.bundle = bundle
    }

    // MARK: - Loading

    /// Load the corpus manifest.
    func loadManifest() throws -> CorpusManifest {
        let data = try loadJSONData(filename: "manifest")
        do {
            return try JSONDecoder().decode(CorpusManifest.self, from: data)
        } catch {
            throw CorpusLoaderError.decodingFailed("manifest.json", error)
        }
    }

    /// Load a single episode annotation by filename (without extension).
    func loadAnnotation(filename: String) throws -> TestEpisodeAnnotation {
        let data = try loadJSONData(filename: filename)
        let annotation: TestEpisodeAnnotation
        do {
            annotation = try JSONDecoder().decode(TestEpisodeAnnotation.self, from: data)
        } catch {
            throw CorpusLoaderError.decodingFailed("\(filename).json", error)
        }

        let errors = validate(annotation)
        if !errors.isEmpty {
            throw CorpusLoaderError.validationFailed("\(filename).json", errors)
        }

        return annotation
    }

    /// Load all annotations listed in the manifest.
    func loadAllAnnotations() throws -> [TestEpisodeAnnotation] {
        let manifest = try loadManifest()
        return try manifest.episodes.map { entry in
            let name = entry.annotationFile.replacingOccurrences(of: ".json", with: "")
            return try loadAnnotation(filename: name)
        }
    }

    /// Load annotations matching specific tags.
    func loadAnnotations(withTag tag: String) throws -> [TestEpisodeAnnotation] {
        let manifest = try loadManifest()
        let matching = manifest.episodes.filter { $0.tags.contains(tag) }
        return try matching.map { entry in
            let name = entry.annotationFile.replacingOccurrences(of: ".json", with: "")
            return try loadAnnotation(filename: name)
        }
    }

    /// Load annotations for episodes from a specific podcast (for per-show priors testing).
    func loadAnnotations(forPodcastId podcastId: String) throws -> [TestEpisodeAnnotation] {
        try loadAllAnnotations().filter { $0.podcast.podcastId == podcastId }
    }

    // MARK: - Conversion to Replay Config

    /// Convert a TestEpisodeAnnotation into a ReplayConfiguration for the simulator.
    func makeReplayConfig(
        from annotation: TestEpisodeAnnotation,
        condition: SimulationCondition,
        timeStep: TimeInterval = ReplayConfiguration.defaultTimeStep
    ) -> ReplayConfiguration {
        let groundTruth = annotation.adSegments.map { seg in
            GroundTruthAdSegment(
                startTime: seg.startTime,
                endTime: seg.endTime,
                advertiser: seg.advertiser,
                product: seg.product,
                adType: mapAdType(seg.adType)
            )
        }

        // Generate synthetic transcript chunks covering the full duration.
        let chunkDuration = 10.0
        let chunks = stride(from: 0.0, to: annotation.episode.duration, by: chunkDuration).map { start in
            TranscriptChunk(
                id: "corpus-\(annotation.episode.episodeId)-\(Int(start))",
                analysisAssetId: annotation.episode.episodeId,
                segmentFingerprint: "corpus-fp-\(Int(start))",
                chunkIndex: Int(start / chunkDuration),
                startTime: start,
                endTime: min(start + chunkDuration, annotation.episode.duration),
                text: "Synthetic transcript chunk for corpus replay.",
                normalizedText: "synthetic transcript chunk for corpus replay",
                pass: "fast",
                modelVersion: "corpus-v1"
            )
        }

        return ReplayConfiguration(
            episodeId: annotation.episode.episodeId,
            episodeTitle: annotation.episode.title,
            episodeDuration: annotation.episode.duration,
            condition: condition,
            groundTruthSegments: groundTruth,
            transcriptChunks: chunks,
            featureWindows: [],
            dynamicAdVariants: [],
            timeStep: timeStep
        )
    }

    // MARK: - Validation

    /// Validate an annotation for consistency.
    func validate(_ annotation: TestEpisodeAnnotation) -> [String] {
        var errors: [String] = []

        if annotation.schemaVersion != TestEpisodeAnnotation.currentSchemaVersion {
            errors.append("Schema version \(annotation.schemaVersion) != expected \(TestEpisodeAnnotation.currentSchemaVersion)")
        }

        if annotation.episode.duration <= 0 {
            errors.append("Episode duration must be positive")
        }

        for (i, seg) in annotation.adSegments.enumerated() {
            if seg.startTime < 0 {
                errors.append("Segment \(i): startTime is negative")
            }
            if seg.endTime <= seg.startTime {
                errors.append("Segment \(i): endTime must be after startTime")
            }
            if seg.endTime > annotation.episode.duration {
                errors.append("Segment \(i): endTime exceeds episode duration")
            }
            if seg.duration < 1.0 {
                errors.append("Segment \(i): duration < 1s is suspicious")
            }
        }

        // Check for overlapping segments.
        let sorted = annotation.adSegments.sorted { $0.startTime < $1.startTime }
        for i in 1..<sorted.count {
            if sorted[i].startTime < sorted[i - 1].endTime {
                errors.append("Segments \(i - 1) and \(i) overlap")
            }
        }

        if annotation.isNoAdEpisode && !annotation.adSegments.isEmpty {
            errors.append("isNoAdEpisode is true but adSegments is non-empty")
        }

        return errors
    }

    // MARK: - Private

    private func loadJSONData(filename: String) throws -> Data {
        // Try bundle resource path first.
        if let url = bundle.url(forResource: filename, withExtension: "json") {
            return try Data(contentsOf: url)
        }

        // Fall back to in-memory embedded annotations.
        if let data = EmbeddedCorpusAnnotations.data(for: filename) {
            return data
        }

        throw CorpusLoaderError.annotationNotFound("\(filename).json")
    }

    private func mapAdType(_ type: TestAdSegment.AdType) -> GroundTruthAdSegment.AdSegmentType {
        switch type {
        case .preRoll: .preRoll
        case .midRoll: .midRoll
        case .postRoll: .postRoll
        }
    }
}

// MARK: - Bundle Token

/// Dummy class used to locate the test bundle.
private final class BundleToken {}
