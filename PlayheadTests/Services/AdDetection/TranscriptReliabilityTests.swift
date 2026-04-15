// TranscriptReliabilityTests.swift
// Tests for playhead-ef2.1.3: per-atom TranscriptReliability signals.

import Foundation
import XCTest
@testable import Playhead

// MARK: - Helpers

private func makeAtom(
    ordinal: Int = 0,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String = "hello world",
    reliability: TranscriptReliability = .default
) -> TranscriptAtom {
    TranscriptAtom(
        atomKey: TranscriptAtomKey(
            analysisAssetId: "asset-1",
            transcriptVersion: "v1",
            atomOrdinal: ordinal
        ),
        contentHash: "deadbeef",
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: ordinal,
        reliability: reliability
    )
}

private func makeChunk(
    chunkIndex: Int = 0,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String = "This is a test sentence. Another sentence here."
) -> TranscriptChunk {
    TranscriptChunk(
        id: UUID().uuidString,
        analysisAssetId: "asset-1",
        segmentFingerprint: "fp",
        chunkIndex: chunkIndex,
        startTime: startTime,
        endTime: endTime,
        text: text,
        normalizedText: text.lowercased(),
        pass: "final",
        modelVersion: "speech-v1",
        transcriptVersion: nil,
        atomOrdinal: nil,
        weakAnchorMetadata: nil
    )
}

// MARK: - TranscriptReliability struct

final class TranscriptReliabilityTests: XCTestCase {

    func testDefaultReliabilityHasGoodChunkQualityAndUnknownNormalization() {
        let r = TranscriptReliability.default
        XCTAssertEqual(r.chunkQuality, .good)
        XCTAssertEqual(r.chunkQualityScore, 1.0)
        XCTAssertEqual(r.normalizationQuality, .unknown)
        XCTAssertEqual(r.alternativeCount, 0)
    }

    func testReliabilityIsSendableAndCanBeStoredOnAtoms() {
        let r = TranscriptReliability(
            chunkQuality: .degraded,
            chunkQualityScore: 0.45,
            normalizationQuality: .partial,
            alternativeCount: 2
        )
        let atom = makeAtom(reliability: r)
        XCTAssertEqual(atom.reliability.chunkQuality, .degraded)
        XCTAssertEqual(atom.reliability.chunkQualityScore, 0.45)
        XCTAssertEqual(atom.reliability.normalizationQuality, .partial)
        XCTAssertEqual(atom.reliability.alternativeCount, 2)
    }

    func testReliabilityDefaultsWhenNotProvided() {
        let atom = TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "a",
                transcriptVersion: "v",
                atomOrdinal: 0
            ),
            contentHash: "h",
            startTime: 0,
            endTime: 1,
            text: "test",
            chunkIndex: 0
        )
        XCTAssertEqual(atom.reliability, TranscriptReliability.default)
    }
}

// MARK: - NormalizationQuality

final class NormalizationQualityTests: XCTestCase {

    func testAllCasesAreAccessible() {
        let cases: [NormalizationQuality] = [.good, .partial, .failed, .unknown]
        XCTAssertEqual(cases.count, 4)
    }
}

// MARK: - Chunk quality propagation through atomizer

final class ChunkQualityPropagationTests: XCTestCase {

    func testAtomizerPropagatesChunkQualityAssessmentToAtoms() {
        let goodChunk = makeChunk(
            chunkIndex: 0,
            startTime: 0,
            endTime: 5,
            text: "Welcome back to our show. Today we have a great guest. Let me introduce them."
        )
        let badChunk = makeChunk(
            chunkIndex: 1,
            startTime: 5,
            endTime: 10,
            text: "aaaa bbbb cccc dddd eeee ffff gggg hhhh iiii jjjj kkkk llll mmmm nnnn oooo pppp"
        )

        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: [goodChunk, badChunk],
            analysisAssetId: "asset-1",
            normalizationHash: "nh",
            sourceHash: "sh"
        )

        // First atom inherits quality from the good chunk
        let firstReliability = atoms[0].reliability
        XCTAssertNotEqual(firstReliability.chunkQuality, .unusable)
        XCTAssertGreaterThan(firstReliability.chunkQualityScore, 0.0)

        // Second atom inherits quality from the bad chunk
        let secondReliability = atoms[1].reliability
        XCTAssertGreaterThanOrEqual(secondReliability.chunkQualityScore, 0.0)
        XCTAssertLessThanOrEqual(secondReliability.chunkQualityScore, 1.0)
    }

    func testSingleChunkAtomizationCarriesReliability() {
        let chunk = makeChunk(
            text: "Visit squarespace.com slash podcast for a free trial. Use code HELLO at checkout."
        )
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: [chunk],
            analysisAssetId: "asset-1",
            normalizationHash: "nh",
            sourceHash: "sh"
        )

        XCTAssertEqual(atoms.count, 1)
        XCTAssertGreaterThan(atoms[0].reliability.chunkQualityScore, 0.0)
        XCTAssertEqual(atoms[0].reliability.normalizationQuality, .unknown)
    }
}

// MARK: - NormalizationQuality assessment

final class NormalizationQualityAssessorTests: XCTestCase {

    func testURLEvidenceGivesGoodQuality() {
        let quality = NormalizationQualityAssessor.assess(
            atomText: "visit squarespace.com slash podcast",
            evidenceCategories: [.url]
        )
        XCTAssertEqual(quality, .good)
    }

    func testPromoCodeGivesGoodQuality() {
        let quality = NormalizationQualityAssessor.assess(
            atomText: "use code HELLO at checkout",
            evidenceCategories: [.promoCode]
        )
        XCTAssertEqual(quality, .good)
    }

    func testNoEvidenceGivesUnknown() {
        let quality = NormalizationQualityAssessor.assess(
            atomText: "and that was a great conversation",
            evidenceCategories: []
        )
        XCTAssertEqual(quality, .unknown)
    }

    func testBrandSpanOnlyGivesPartial() {
        let quality = NormalizationQualityAssessor.assess(
            atomText: "brought to you by Squarespace",
            evidenceCategories: [.brandSpan]
        )
        XCTAssertEqual(quality, .partial)
    }

    func testMultipleStrongCategoriesYieldGoodQuality() {
        let quality = NormalizationQualityAssessor.assess(
            atomText: "visit squarespace.com and use code HELLO",
            evidenceCategories: [.url, .promoCode]
        )
        XCTAssertEqual(quality, .good)
    }
}

// MARK: - Reliability gating

final class ReliabilityGatingTests: XCTestCase {

    func testIsUsableForClassificationIsTrueForGoodQuality() {
        let r = TranscriptReliability(
            chunkQuality: .good,
            chunkQualityScore: 0.8,
            normalizationQuality: .unknown,
            alternativeCount: 0
        )
        XCTAssertTrue(r.isUsableForClassification)
    }

    func testIsUsableForClassificationIsTrueForDegradedQuality() {
        let r = TranscriptReliability(
            chunkQuality: .degraded,
            chunkQualityScore: 0.5,
            normalizationQuality: .unknown,
            alternativeCount: 0
        )
        XCTAssertTrue(r.isUsableForClassification)
    }

    func testIsUsableForClassificationIsFalseForUnusableQuality() {
        let r = TranscriptReliability(
            chunkQuality: .unusable,
            chunkQualityScore: 0.2,
            normalizationQuality: .unknown,
            alternativeCount: 0
        )
        XCTAssertFalse(r.isUsableForClassification)
    }

    func testIsHighConfidenceRequiresBothGoodChunkAndGoodNormalization() {
        let good = TranscriptReliability(
            chunkQuality: .good,
            chunkQualityScore: 0.8,
            normalizationQuality: .good,
            alternativeCount: 0
        )
        XCTAssertTrue(good.isHighConfidence)

        let degradedChunk = TranscriptReliability(
            chunkQuality: .degraded,
            chunkQualityScore: 0.5,
            normalizationQuality: .good,
            alternativeCount: 0
        )
        XCTAssertFalse(degradedChunk.isHighConfidence)

        let unknownNorm = TranscriptReliability(
            chunkQuality: .good,
            chunkQualityScore: 0.8,
            normalizationQuality: .unknown,
            alternativeCount: 0
        )
        XCTAssertFalse(unknownNorm.isHighConfidence)
    }
}

// MARK: - ReliabilityGate (Phase 0: all pass-through)

final class ReliabilityGateTests: XCTestCase {

    func testAllGatesReturnTrueInPhase0RegardlessOfQuality() {
        let unusable = TranscriptReliability(
            chunkQuality: .unusable,
            chunkQualityScore: 0.1,
            normalizationQuality: .failed,
            alternativeCount: 0
        )
        // Phase 0: even unusable atoms pass all gates
        XCTAssertTrue(ReliabilityGate.shouldIncludeInLexicalScan(unusable))
        XCTAssertTrue(ReliabilityGate.shouldIncludeInFingerprintMatch(unusable))
        XCTAssertTrue(ReliabilityGate.shouldIncludeInFMScheduling(unusable))
        XCTAssertTrue(ReliabilityGate.shouldIncludeInCorroboration(unusable))
    }

    func testGoodReliabilityPassesAllGates() {
        let good = TranscriptReliability(
            chunkQuality: .good,
            chunkQualityScore: 0.9,
            normalizationQuality: .good,
            alternativeCount: 0
        )
        XCTAssertTrue(ReliabilityGate.shouldIncludeInLexicalScan(good))
        XCTAssertTrue(ReliabilityGate.shouldIncludeInFingerprintMatch(good))
        XCTAssertTrue(ReliabilityGate.shouldIncludeInFMScheduling(good))
        XCTAssertTrue(ReliabilityGate.shouldIncludeInCorroboration(good))
    }
}

// MARK: - Equatable conformance

final class TranscriptReliabilityEquatableTests: XCTestCase {

    func testEqualReliabilitiesCompareEqual() {
        let a = TranscriptReliability(
            chunkQuality: .good,
            chunkQualityScore: 0.8,
            normalizationQuality: .good,
            alternativeCount: 1
        )
        let b = TranscriptReliability(
            chunkQuality: .good,
            chunkQualityScore: 0.8,
            normalizationQuality: .good,
            alternativeCount: 1
        )
        XCTAssertEqual(a, b)
    }

    func testDifferentReliabilitiesCompareNotEqual() {
        let a = TranscriptReliability.default
        let b = TranscriptReliability(
            chunkQuality: .degraded,
            chunkQualityScore: 0.5,
            normalizationQuality: .partial,
            alternativeCount: 3
        )
        XCTAssertNotEqual(a, b)
    }
}
