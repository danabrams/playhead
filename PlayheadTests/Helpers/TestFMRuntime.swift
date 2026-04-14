// TestFMRuntime.swift
// A deterministic, dependency-free FoundationModelClassifier.Runtime for unit
// tests. Mirrors the in-test `RuntimeRecorder` from FoundationModelClassifierTests
// but is shared across the BackfillJobRunner / shadow-mode harness suites.

import Foundation
@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

/// Builds a deterministic `FoundationModelClassifier.Runtime` that returns
/// pre-canned coarse and refinement responses with zero-latency, fake token
/// counts. Used by tests that exercise the runner / shadow path orchestration
/// without booting the real Foundation Models stack.
actor TestFMRuntime {
    private var coarseQueue: [CoarseScreeningSchema]
    private var refinementQueue: [RefinementWindowSchema]
    private var boundaryExtractionQueue: [FMBoundarySchema]
    private var coarseFailureQueue: [TestFMRuntimeFailure?]
    private var refinementFailureQueue: [TestFMRuntimeFailure?]
    private var boundaryExtractionFailureQueue: [TestFMRuntimeFailure?]
    private var coarsePrompts: [String] = []
    private var refinementPrompts: [String] = []
    private var boundaryExtractionPrompts: [String] = []
    private let defaultCoarse: CoarseScreeningSchema
    private let defaultRefinement: RefinementWindowSchema
    private let defaultBoundaryExtraction: FMBoundarySchema
    private let contextSizeValue: Int
    private let coarseSchemaTokenCountValue: Int
    private let refinementSchemaTokenCountValue: Int
    private let boundarySchemaTokenCountValue: Int
    private let tokenCountRule: @Sendable (String) -> Int
    private(set) var coarseCallCount = 0
    private(set) var refinementCallCount = 0
    private(set) var boundaryExtractionCallCount = 0

    init(
        coarseResponses: [CoarseScreeningSchema] = [],
        refinementResponses: [RefinementWindowSchema] = [],
        boundaryExtractionResponses: [FMBoundarySchema] = [],
        coarseFailures: [TestFMRuntimeFailure?] = [],
        refinementFailures: [TestFMRuntimeFailure?] = [],
        boundaryExtractionFailures: [TestFMRuntimeFailure?] = [],
        contextSize: Int = 4_096,
        coarseSchemaTokenCount: Int = 16,
        refinementSchemaTokenCount: Int = 32,
        boundarySchemaTokenCount: Int = 32,
        tokenCountRule: @escaping @Sendable (String) -> Int = { prompt in
            max(1, prompt.split(whereSeparator: \.isWhitespace).count)
        },
        defaultCoarse: CoarseScreeningSchema = CoarseScreeningSchema(
            disposition: .noAds,
            support: nil
        ),
        defaultRefinement: RefinementWindowSchema = RefinementWindowSchema(spans: []),
        defaultBoundaryExtraction: FMBoundarySchema = FMBoundarySchema(spans: [], abstain: false)
    ) {
        self.coarseQueue = coarseResponses
        self.refinementQueue = refinementResponses
        self.boundaryExtractionQueue = boundaryExtractionResponses
        self.coarseFailureQueue = coarseFailures
        self.refinementFailureQueue = refinementFailures
        self.boundaryExtractionFailureQueue = boundaryExtractionFailures
        self.contextSizeValue = contextSize
        self.coarseSchemaTokenCountValue = coarseSchemaTokenCount
        self.refinementSchemaTokenCountValue = refinementSchemaTokenCount
        self.boundarySchemaTokenCountValue = boundarySchemaTokenCount
        self.tokenCountRule = tokenCountRule
        self.defaultCoarse = defaultCoarse
        self.defaultRefinement = defaultRefinement
        self.defaultBoundaryExtraction = defaultBoundaryExtraction
    }

    nonisolated var runtime: FoundationModelClassifier.Runtime {
        FoundationModelClassifier.Runtime(
            availabilityStatus: { _ in nil },
            contextSize: { self.contextSizeValue },
            tokenCount: { prompt in
                self.tokenCountRule(prompt)
            },
            coarseSchemaTokenCount: { self.coarseSchemaTokenCountValue },
            refinementSchemaTokenCount: { self.refinementSchemaTokenCountValue },
            boundarySchemaTokenCount: { self.boundarySchemaTokenCountValue },
            makeSession: {
                FoundationModelClassifier.Runtime.Session(
                    prewarm: { _ in },
                    respondCoarse: { prompt in try await self.nextCoarse(prompt: prompt) },
                    respondRefinement: { prompt in try await self.nextRefinement(prompt: prompt) },
                    respondBoundaryExtraction: { prompt in try await self.nextBoundaryExtraction(prompt: prompt) }
                )
            }
        )
    }

    func snapshotSubmittedCoarseLineRefs() -> [[Int]] {
        coarsePrompts.map(Self.submittedLineRefs(from:))
    }

    private func nextCoarse(prompt: String) throws -> CoarseScreeningSchema {
        coarsePrompts.append(prompt)
        coarseCallCount += 1
        if !coarseFailureQueue.isEmpty {
            let failure = coarseFailureQueue.removeFirst()
            if let failure {
                throw failure.error
            }
        }
        if coarseQueue.isEmpty {
            return defaultCoarse
        }
        return coarseQueue.removeFirst()
    }

    private func nextRefinement(prompt: String) throws -> RefinementWindowSchema {
        refinementPrompts.append(prompt)
        refinementCallCount += 1
        if !refinementFailureQueue.isEmpty {
            let failure = refinementFailureQueue.removeFirst()
            if let failure {
                throw failure.error
            }
        }
        if refinementQueue.isEmpty {
            return defaultRefinement
        }
        return refinementQueue.removeFirst()
    }

    private func nextBoundaryExtraction(prompt: String) throws -> FMBoundarySchema {
        boundaryExtractionPrompts.append(prompt)
        boundaryExtractionCallCount += 1
        if !boundaryExtractionFailureQueue.isEmpty {
            let failure = boundaryExtractionFailureQueue.removeFirst()
            if let failure {
                throw failure.error
            }
        }
        if boundaryExtractionQueue.isEmpty {
            return defaultBoundaryExtraction
        }
        return boundaryExtractionQueue.removeFirst()
    }

    /// Rev4-L2: exposed for adversarial-input regression tests.
    nonisolated static func submittedLineRefsForTesting(from prompt: String) -> [Int] {
        submittedLineRefs(from: prompt)
    }

    nonisolated private static func submittedLineRefs(from prompt: String) -> [Int] {
        // Rev4-L2: tightened from `L(\d+)>` to require either a
        // non-letter character before the `L` or the start of input.
        // The previous pattern matched things like `XYL12>` (a token
        // ending in `L` followed by digits and `>`) and reported `12`
        // as a submitted line ref, polluting the submission record. The
        // capture group on the digits is now group 1; the prefix is a
        // non-capturing assertion.
        guard let regex = try? NSRegularExpression(pattern: #"(?:^|[^A-Za-z])L(\d+)>"#) else {
            return []
        }
        let nsPrompt = prompt as NSString
        let range = NSRange(location: 0, length: nsPrompt.length)
        var refs: [Int] = []
        var seen = Set<Int>()
        for match in regex.matches(in: prompt, range: range) {
            guard match.numberOfRanges > 1 else { continue }
            let refString = nsPrompt.substring(with: match.range(at: 1))
            guard let ref = Int(refString), seen.insert(ref).inserted else { continue }
            refs.append(ref)
        }
        return refs.sorted()
    }

    // Cycle 4 H4: Rev4-L3 originally added a `reset()` method here, but
    // every call site constructs a fresh `TestFMRuntime` instance per test
    // (there is no shared/static runtime), so `reset()` had no production
    // users. Removed to kill the dead code path rather than leaving a
    // method whose contract was never enforced.
}

enum TestFMRuntimeFailure: Sendable {
    case exceededContextWindow
    case refusal
    case guardrailViolation
    case rateLimited

    var error: Error {
        #if canImport(FoundationModels)
        if #available(iOS 26.0, *) {
            let context = LanguageModelSession.GenerationError.Context(debugDescription: "test-fm-runtime")
            switch self {
            case .exceededContextWindow:
                return LanguageModelSession.GenerationError.exceededContextWindowSize(context)
            case .refusal:
                let refusal = LanguageModelSession.GenerationError.Refusal(transcriptEntries: [])
                return LanguageModelSession.GenerationError.refusal(refusal, context)
            case .guardrailViolation:
                return LanguageModelSession.GenerationError.guardrailViolation(context)
            case .rateLimited:
                return LanguageModelSession.GenerationError.rateLimited(context)
            }
        }
        #endif

        return NSError(
            domain: "TestFMRuntimeFailure",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "\(self)"]
        )
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
