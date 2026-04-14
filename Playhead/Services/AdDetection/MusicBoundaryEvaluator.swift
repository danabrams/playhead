// MusicBoundaryEvaluator.swift
// Evaluation structure for producing per-genre precision/recall reports
// when run against labeled data. Does not block any production path --
// this is for future corpus evaluation once labeled data is available.

import Foundation

// MARK: - Labeled Data Types

/// A ground-truth music boundary annotation in labeled corpus data.
struct LabeledMusicBoundary: Sendable, Equatable {
    /// Time in seconds where music starts or ends.
    let time: Double
    /// Whether this is a music onset or offset.
    let direction: MusicBoundaryDirection
    /// The music bed level at this boundary.
    let level: MusicBedLevel
    /// Genre of the podcast (for per-genre slicing).
    let genre: String
}

enum MusicBoundaryDirection: String, Sendable, Equatable {
    case onset
    case offset
}

// MARK: - Evaluation Results

/// Per-genre precision/recall report for music boundary detection.
struct MusicBoundaryGenreReport: Sendable, Equatable {
    let genre: String
    let truePositives: Int
    let falsePositives: Int
    let falseNegatives: Int

    var precision: Double {
        let denominator = truePositives + falsePositives
        guard denominator > 0 else { return 0 }
        return Double(truePositives) / Double(denominator)
    }

    var recall: Double {
        let denominator = truePositives + falseNegatives
        guard denominator > 0 else { return 0 }
        return Double(truePositives) / Double(denominator)
    }

    var f1: Double {
        let p = precision
        let r = recall
        guard p + r > 0 else { return 0 }
        return 2 * p * r / (p + r)
    }
}

/// Aggregate evaluation report across all genres.
struct MusicBoundaryEvaluationReport: Sendable, Equatable {
    let genreReports: [MusicBoundaryGenreReport]
    let toleranceSeconds: Double

    var aggregatePrecision: Double {
        let totalTP = genreReports.map(\.truePositives).reduce(0, +)
        let totalFP = genreReports.map(\.falsePositives).reduce(0, +)
        let denominator = totalTP + totalFP
        guard denominator > 0 else { return 0 }
        return Double(totalTP) / Double(denominator)
    }

    var aggregateRecall: Double {
        let totalTP = genreReports.map(\.truePositives).reduce(0, +)
        let totalFN = genreReports.map(\.falseNegatives).reduce(0, +)
        let denominator = totalTP + totalFN
        guard denominator > 0 else { return 0 }
        return Double(totalTP) / Double(denominator)
    }

    var aggregateF1: Double {
        let p = aggregatePrecision
        let r = aggregateRecall
        guard p + r > 0 else { return 0 }
        return 2 * p * r / (p + r)
    }
}

// MARK: - MusicBoundaryEvaluator

/// Evaluates detected music boundaries against labeled ground truth.
/// Produces per-genre precision/recall reports for tuning.
///
/// Usage (future, when labeled corpus is available):
/// ```swift
/// let evaluator = MusicBoundaryEvaluator(toleranceSeconds: 2.0)
/// let report = evaluator.evaluate(
///     detected: detectedBoundaries,
///     groundTruth: labeledBoundaries
/// )
/// for genre in report.genreReports {
///     print("\(genre.genre): P=\(genre.precision) R=\(genre.recall) F1=\(genre.f1)")
/// }
/// ```
struct MusicBoundaryEvaluator: Sendable {

    /// A detected music boundary from the classifier output.
    struct DetectedBoundary: Sendable, Equatable {
        let time: Double
        let direction: MusicBoundaryDirection
        let level: MusicBedLevel
        let score: Double
        let genre: String
    }

    /// Maximum time difference (seconds) between a detected boundary
    /// and a ground-truth boundary to count as a match.
    let toleranceSeconds: Double

    init(toleranceSeconds: Double = 2.0) {
        self.toleranceSeconds = toleranceSeconds
    }

    /// Evaluate detected boundaries against ground truth.
    ///
    /// Matching uses greedy nearest-neighbor within `toleranceSeconds`,
    /// direction-aware (onset matches onset, offset matches offset).
    func evaluate(
        detected: [DetectedBoundary],
        groundTruth: [LabeledMusicBoundary]
    ) -> MusicBoundaryEvaluationReport {
        let allGenres = Set(
            detected.map(\.genre) + groundTruth.map(\.genre)
        ).sorted()

        let genreReports = allGenres.map { genre in
            evaluateGenre(
                genre: genre,
                detected: detected.filter { $0.genre == genre },
                groundTruth: groundTruth.filter { $0.genre == genre }
            )
        }

        return MusicBoundaryEvaluationReport(
            genreReports: genreReports,
            toleranceSeconds: toleranceSeconds
        )
    }

    private func evaluateGenre(
        genre: String,
        detected: [DetectedBoundary],
        groundTruth: [LabeledMusicBoundary]
    ) -> MusicBoundaryGenreReport {
        var matchedGT = Set<Int>()
        var truePositives = 0
        var falsePositives = 0

        // Sort detected by score descending for greedy matching.
        let sortedDetected = detected.sorted { $0.score > $1.score }

        for det in sortedDetected {
            let bestMatch = groundTruth.enumerated()
                .filter { !matchedGT.contains($0.offset) }
                .filter { $0.element.direction == det.direction }
                .filter { abs($0.element.time - det.time) <= toleranceSeconds }
                .min { abs($0.element.time - det.time) < abs($1.element.time - det.time) }

            if let match = bestMatch {
                truePositives += 1
                matchedGT.insert(match.offset)
            } else {
                falsePositives += 1
            }
        }

        let falseNegatives = groundTruth.count - matchedGT.count

        return MusicBoundaryGenreReport(
            genre: genre,
            truePositives: truePositives,
            falsePositives: falsePositives,
            falseNegatives: falseNegatives
        )
    }
}
