// TestFMRuntime.swift
// A deterministic, dependency-free FoundationModelClassifier.Runtime for unit
// tests. Mirrors the in-test `RuntimeRecorder` from FoundationModelClassifierTests
// but is shared across the BackfillJobRunner / shadow-mode harness suites.

import Foundation
@testable import Playhead

/// Builds a deterministic `FoundationModelClassifier.Runtime` that returns
/// pre-canned coarse and refinement responses with zero-latency, fake token
/// counts. Used by tests that exercise the runner / shadow path orchestration
/// without booting the real Foundation Models stack.
actor TestFMRuntime {
    private var coarseQueue: [CoarseScreeningSchema]
    private var refinementQueue: [RefinementWindowSchema]
    private let defaultCoarse: CoarseScreeningSchema
    private let defaultRefinement: RefinementWindowSchema
    private(set) var coarseCallCount = 0
    private(set) var refinementCallCount = 0

    init(
        coarseResponses: [CoarseScreeningSchema] = [],
        refinementResponses: [RefinementWindowSchema] = [],
        defaultCoarse: CoarseScreeningSchema = CoarseScreeningSchema(
            transcriptQuality: .good,
            disposition: .noAds,
            support: nil
        ),
        defaultRefinement: RefinementWindowSchema = RefinementWindowSchema(spans: [])
    ) {
        self.coarseQueue = coarseResponses
        self.refinementQueue = refinementResponses
        self.defaultCoarse = defaultCoarse
        self.defaultRefinement = defaultRefinement
    }

    nonisolated var runtime: FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { 4_096 },
            tokenCount: { prompt in
                max(1, prompt.split(whereSeparator: \.isWhitespace).count)
            },
            coarseSchemaTokenCount: { 16 },
            refinementSchemaTokenCount: { 32 },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { _ in await self.nextCoarse() },
                    respondRefinement: { _ in await self.nextRefinement() }
                )
            }
        )
    }

    private func nextCoarse() -> CoarseScreeningSchema {
        coarseCallCount += 1
        if coarseQueue.isEmpty {
            return defaultCoarse
        }
        return coarseQueue.removeFirst()
    }

    private func nextRefinement() -> RefinementWindowSchema {
        refinementCallCount += 1
        if refinementQueue.isEmpty {
            return defaultRefinement
        }
        return refinementQueue.removeFirst()
    }
}

// MARK: - Convenience builders

/// Build a tiny, deterministic transcript-segment fixture from raw text lines.
/// Each entry becomes one `TranscriptAtom` and one `AdTranscriptSegment` so
/// tests can assert against stable atom ordinals.
func makeFMSegments(
    analysisAssetId: String,
    transcriptVersion: String,
    lines: [(start: Double, end: Double, text: String)]
) -> [AdTranscriptSegment] {
    lines.enumerated().map { ordinal, line in
        let atom = TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: analysisAssetId,
                transcriptVersion: transcriptVersion,
                atomOrdinal: ordinal
            ),
            contentHash: String(format: "%08x", ordinal),
            startTime: line.start,
            endTime: line.end,
            text: line.text,
            chunkIndex: ordinal
        )
        return AdTranscriptSegment(
            atoms: [atom],
            segmentIndex: ordinal,
            boundaryReason: .startOfTranscript,
            boundaryConfidence: 1.0,
            segmentType: .speech
        )
    }
}

/// A `CapabilitySnapshot` that admits all jobs immediately: nominal thermals,
/// charging, FM available. Use unless you're testing the deferral paths.
func makePermissiveCapabilitySnapshot() -> CapabilitySnapshot {
    CapabilitySnapshot(
        foundationModelsAvailable: true,
        foundationModelsUsable: true,
        appleIntelligenceEnabled: true,
        foundationModelsLocaleSupported: true,
        thermalState: .nominal,
        isLowPowerMode: false,
        isCharging: true,
        backgroundProcessingSupported: true,
        availableDiskSpaceBytes: 1024 * 1024 * 1024,
        capturedAt: Date()
    )
}

/// A snapshot that should cause AdmissionController to defer with `thermalThrottled`.
func makeThermalThrottledSnapshot() -> CapabilitySnapshot {
    CapabilitySnapshot(
        foundationModelsAvailable: true,
        foundationModelsUsable: true,
        appleIntelligenceEnabled: true,
        foundationModelsLocaleSupported: true,
        thermalState: .critical,
        isLowPowerMode: false,
        isCharging: true,
        backgroundProcessingSupported: true,
        availableDiskSpaceBytes: 1024 * 1024 * 1024,
        capturedAt: Date()
    )
}

/// JSON-encodes a default ScanCohort for tests. Stable across runs so reuse
/// lookups behave deterministically.
func makeTestScanCohortJSON(promptLabel: String = "phase3-test") -> String {
    let cohort = ScanCohort(
        promptLabel: promptLabel,
        promptHash: "prompt-hash-test",
        schemaHash: "schema-hash-test",
        scanPlanHash: "plan-hash-test",
        normalizationHash: "norm-hash-test",
        osBuild: "26A123",
        locale: "en_US",
        appBuild: "test"
    )
    return String(decoding: try! JSONEncoder().encode(cohort), as: UTF8.self)
}
