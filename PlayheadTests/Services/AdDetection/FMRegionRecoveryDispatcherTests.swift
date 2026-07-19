// FMRegionRecoveryDispatcherTests.swift
// playhead-r2vz (PR2): focused coverage for the live FM recovery dispatcher's
// disposition→verdict mapping and its coarse-call path against a STUB
// `FoundationModelClassifier.Runtime`. No live model is ever touched — the
// stub returns a deterministic `CoarseScreeningSchema`, exactly like the
// `LiveShadowFMDispatcherTests` precedent.
//
// Coverage:
//   - Pure mapping (`verdict(for:)`): containsAd→.ad, noAds→.content,
//     uncertain→.content (DECISION #3 conservative), abstain→.unavailable.
//   - classify happy paths: a stub whose `respondCoarse` returns each
//     disposition maps to the expected verdict.
//   - classify graceful degrade: a stub whose `respondCoarse` throws maps to
//     `.unavailable` (region stays suppressed).
//   - regionSegments: builds non-empty segments from the region atom range,
//     and returns [] when the region range has no atoms (→ classify returns
//     `.unavailable` without calling the model).

import Foundation
import Testing

@testable import Playhead

@Suite("FMRegionRecoveryDispatcher — mapping (playhead-r2vz)")
struct FMRegionRecoveryDispatcherMappingTests {

    @Test("containsAd → .ad")
    func mapsContainsAd() {
        #expect(LiveFMRegionRecoveryDispatcher.verdict(for: .containsAd) == .ad)
    }

    @Test("noAds → .content")
    func mapsNoAds() {
        #expect(LiveFMRegionRecoveryDispatcher.verdict(for: .noAds) == .content)
    }

    @Test("uncertain → .content (DECISION #3 conservative)")
    func mapsUncertain() {
        #expect(LiveFMRegionRecoveryDispatcher.verdict(for: .uncertain) == .content)
    }

    @Test("abstain → .unavailable")
    func mapsAbstain() {
        #expect(LiveFMRegionRecoveryDispatcher.verdict(for: .abstain) == .unavailable)
    }
}

@Suite("FMRegionRecoveryDispatcher — classify (playhead-r2vz)")
struct FMRegionRecoveryDispatcherClassifyTests {

    private let assetId = "r2vz-dispatcher"

    /// Six 10-second chunks → six atoms (one per chunk).
    private func makeChunks() -> [TranscriptChunk] {
        (0..<6).map { idx in
            TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 10,
                endTime: Double(idx + 1) * 10,
                text: "Line \(idx) of synthetic content for region recovery.",
                normalizedText: "line \(idx) of synthetic content for region recovery.",
                pass: "final",
                modelVersion: "v",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    private func makeAtoms() -> [TranscriptAtom] {
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: makeChunks(),
            analysisAssetId: assetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        return atoms
    }

    /// A `.sustainedMusic` region spanning the middle atom ordinals so the
    /// dispatcher's `regionSegments` has transcript text to segment.
    private func makeRegion(atoms: [TranscriptAtom]) -> ProposedRegion {
        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        let lo = sorted[1].atomKey.atomOrdinal
        let hi = sorted[sorted.count - 2].atomKey.atomOrdinal
        return ProposedRegion(
            analysisAssetId: assetId,
            transcriptVersion: sorted[0].atomKey.transcriptVersion,
            firstAtomOrdinal: lo,
            lastAtomOrdinal: hi,
            startTime: sorted[1].startTime,
            endTime: sorted[sorted.count - 2].endTime,
            origins: [.sustainedMusic],
            fmConsensusStrength: .none,
            lexicalCandidates: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            acousticBreaks: [],
            foundationModelSpans: [],
            resolvedEvidenceAnchors: [],
            fmEvidence: nil
        )
    }

    @Test("classify maps each coarse disposition to the expected verdict")
    func classifyMapsDispositions() async {
        let atoms = makeAtoms()
        let region = makeRegion(atoms: atoms)

        let cases: [(CoarseDisposition, FMRegionVerdict)] = [
            (.containsAd, .ad),
            (.noAds, .content),
            (.uncertain, .content),
            (.abstain, .unavailable),
        ]
        for (disposition, expected) in cases {
            let runtime = makeStubCoarseRuntime(
                respondCoarse: { _ in CoarseScreeningSchema(disposition: disposition, support: nil) }
            )
            let dispatcher = LiveFMRegionRecoveryDispatcher(runtime: runtime)
            let verdict = await dispatcher.classify(region: region, atoms: atoms)
            #expect(verdict == expected, "disposition \(disposition) should map to \(expected)")
        }
    }

    @Test("classify degrades to .unavailable when the coarse call throws")
    func classifyThrowIsUnavailable() async {
        let atoms = makeAtoms()
        let region = makeRegion(atoms: atoms)
        let runtime = makeStubCoarseRuntime(
            respondCoarse: { _ in throw StubCoarseError() }
        )
        let dispatcher = LiveFMRegionRecoveryDispatcher(runtime: runtime)
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .unavailable)
    }

    @Test("classify returns .unavailable without calling the model when the region has no atoms")
    func classifyEmptyRegionIsUnavailable() async {
        let atoms = makeAtoms()
        // Region whose ordinal range sits entirely past the atom stream, so
        // no atom falls inside it and the front-pad window is empty too.
        let region = ProposedRegion(
            analysisAssetId: assetId,
            transcriptVersion: atoms[0].atomKey.transcriptVersion,
            firstAtomOrdinal: 10_000,
            lastAtomOrdinal: 10_001,
            startTime: 100_000,
            endTime: 100_010,
            origins: [.sustainedMusic],
            fmConsensusStrength: .none,
            lexicalCandidates: [],
            sponsorMatches: [],
            fingerprintMatches: [],
            acousticBreaks: [],
            foundationModelSpans: [],
            resolvedEvidenceAnchors: [],
            fmEvidence: nil
        )
        // A stub that WOULD say containsAd — proving the empty-segment guard
        // short-circuits before the model is consulted.
        let calls = CoarseCallCounter()
        let runtime = makeStubCoarseRuntime(
            respondCoarse: { _ in
                await calls.bump()
                return CoarseScreeningSchema(disposition: .containsAd, support: nil)
            }
        )
        let dispatcher = LiveFMRegionRecoveryDispatcher(runtime: runtime)
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .unavailable)
        #expect(await calls.count == 0, "empty region must not consult the model")
    }

    @Test("regionSegments includes the music-tail AND the POST-EDGE onset window (the ad-read)")
    func regionSegmentsIncludePostEdgeAdRead() {
        // The music region is [ord lo..hi]; the atom right AFTER the edge
        // carries the (cue-less) ad-read. The FM window MUST include it — the
        // discriminating signal is post-edge, not the music play-out itself.
        let atoms = makeAtoms()
        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        let hi = sorted[sorted.count - 2].atomKey.atomOrdinal // last region atom
        let region = makeRegion(atoms: atoms)                 // spans [lo, hi]
        let postEdgeOrdinal = sorted[sorted.count - 1].atomKey.atomOrdinal
        #expect(postEdgeOrdinal > hi, "fixture must have an atom strictly after the region")

        let segments = LiveFMRegionRecoveryDispatcher.regionSegments(
            region: region,
            atoms: atoms,
            padLeadSeconds: MusicOffsetLexicalGate.onsetWindowLeadSeconds
        )
        #expect(!segments.isEmpty)
        let windowOrdinals = Set(segments.flatMap { $0.atoms }.map { $0.atomKey.atomOrdinal })
        // Music-tail context is present…
        #expect(windowOrdinals.contains(hi), "the region's music-tail atoms must be in the FM window")
        // …AND the post-edge ad-read atom the gate would have inspected.
        #expect(
            windowOrdinals.contains(postEdgeOrdinal),
            "the FM window MUST reach past the music→speech edge to the ad-read — otherwise recovery is inert"
        )
    }
}

// MARK: - Test helpers (file-scoped)

private struct StubCoarseError: Error {}

private actor CoarseCallCounter {
    private(set) var count = 0
    func bump() { count += 1 }
}

/// Build a `FoundationModelClassifier.Runtime` whose coarse closure is supplied
/// by the caller. All other legs return harmless defaults. Mirrors the
/// `makeStubRuntime` helper in `LiveShadowFMDispatcherTests`.
private func makeStubCoarseRuntime(
    respondCoarse: @escaping @Sendable (_ prompt: String) async throws -> CoarseScreeningSchema
) -> FoundationModelClassifier.Runtime {
    FoundationModelClassifier.Runtime(
        availabilityStatus: { _ in nil },
        contextSize: { 4_096 },
        tokenCount: { prompt in
            max(1, prompt.split(whereSeparator: \.isWhitespace).count)
        },
        coarseSchemaTokenCount: { 16 },
        refinementSchemaTokenCount: { 32 },
        boundarySchemaTokenCount: { 32 },
        makeSession: {
            FoundationModelClassifier.Runtime.Session(
                prewarm: { _ in },
                respondCoarse: respondCoarse,
                respondRefinement: { _ in RefinementWindowSchema(spans: []) }
            )
        }
    )
}
