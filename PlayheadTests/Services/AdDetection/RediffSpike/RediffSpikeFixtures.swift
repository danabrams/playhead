// RediffSpikeFixtures.swift
// SPIKE (playhead-xsdz.16): fixture loader for the fingerprint-rediff
// prototype's parity tests. TEST-TARGET-ONLY — nothing in production
// invokes anything in this directory.
//
// The JSON fixtures live at TestFixtures/RediffSpike/*.json in the repo
// tree and are deliberately NOT bundled into the test target (they embed
// multi-thousand-element fingerprint arrays). They are loaded from disk
// via the #filePath walk-up pattern, mirroring
// PlayheadTests/Fixtures/Corpus/L2F/CorpusAnnotationLoader.swift.
//
// Fixture schema (per file):
//   name, note                       — provenance strings
//   fingerprintA / fingerprintB     — raw chromaprint sequences ([UInt32])
//   secondsPerFpA / secondsPerFpB   — per-file fingerprint rate
//   algorithmParams                 — knobs the reference run used
//   pythonReference                 — exact expected mergedRuns / slots /
//                                     minGapFps from scripts/l2f-dai-rediff.py
//   groundTruth (optional)          — absolute-seconds tolerance windows
//                                     (empty array = must be empty)

import Foundation

struct RediffSpikeFixture: Decodable, Sendable {

    struct AlgorithmParams: Decodable, Sendable {
        let hammingTol: Int
        let minRunFps: Int
        let offsetSlack: Int
        let gapDiffSlack: Int
        let minAdSeconds: Double
    }

    struct ReferenceRun: Decodable, Sendable {
        let aStart: Int
        let bStart: Int
        let length: Int
        let errors: Int
    }

    struct ReferenceSlot: Decodable, Sendable {
        let startFp: Int
        let endFp: Int
        let startSeconds: Double
        let endSeconds: Double
        let leftRunFps: Int
        let rightRunFps: Int
    }

    struct PythonReference: Decodable, Sendable {
        let mergedRuns: [ReferenceRun]
        let slotsA: [ReferenceSlot]
        let slotsB: [ReferenceSlot]
        let minGapFpsA: Int
        let minGapFpsB: Int
    }

    struct GroundTruthWindow: Decodable, Sendable {
        /// [lo, hi] inclusive bounds for the slot's startSeconds.
        let startSecondsRange: [Double]
        /// [lo, hi] inclusive bounds for the slot's endSeconds.
        let endSecondsRange: [Double]
    }

    struct GroundTruth: Decodable, Sendable {
        let slotsA: [GroundTruthWindow]?
        let slotsB: [GroundTruthWindow]?
    }

    let name: String
    let note: String
    let fingerprintA: [UInt32]
    let fingerprintB: [UInt32]
    let secondsPerFpA: Double
    let secondsPerFpB: Double
    let algorithmParams: AlgorithmParams
    let pythonReference: PythonReference
    let groundTruth: GroundTruth?
}

enum RediffSpikeFixtureLoader {

    /// Repo-root-relative directory holding the rediff spike fixtures.
    static let fixturesRelativePath = "TestFixtures/RediffSpike"

    /// Repo root for the current source tree, derived from this file's
    /// `#filePath` (the fixtures live in the repo tree, not the bundle).
    static func repoRoot(filePath: String = #filePath) -> URL {
        // PlayheadTests/Services/AdDetection/RediffSpike/RediffSpikeFixtures.swift
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()  // RediffSpike/
            .deletingLastPathComponent()  // AdDetection/
            .deletingLastPathComponent()  // Services/
            .deletingLastPathComponent()  // PlayheadTests/
            .deletingLastPathComponent()  // <repo root>
    }

    /// Load and decode `TestFixtures/RediffSpike/<name>.json`.
    static func load(_ name: String) throws -> RediffSpikeFixture {
        let url = repoRoot()
            .appendingPathComponent(fixturesRelativePath)
            .appendingPathComponent("\(name).json")
        let data = try Data(contentsOf: url)
        return try JSONDecoder().decode(RediffSpikeFixture.self, from: data)
    }
}
