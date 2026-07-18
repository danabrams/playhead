// SustainedMusicOffsetSeamIntegrationTests.swift
// playhead-t1py / playhead-xtpf: end-to-end proof of the propose-not-refine
// seam for the sustained-music-offset proposer.
//
// Two properties:
//   1. FLAG OFF ⇒ byte-identical no-op. With the proposer disabled, a strong
//      sustained-music run in the feature windows changes nothing — no
//      `.sustainedMusic` region appears and every pre-existing region is
//      untouched.
//   2. FLAG ON ⇒ an FM-missed post-roll music run PROPOSES a WIDE candidate
//      that anchors INDEPENDENTLY (Path 5) and, flowing
//      RegionShadowPhase.run → AtomEvidenceProjector →
//      MinimalContiguousSpanDecoder → DecisionMapper, survives to a
//      `.markOnly` decision (never auto-skip) on `.sustainedMusicOffset`
//      provenance ALONE (no classifier / FM co-firing).

import Foundation
import Testing

@testable import Playhead

@Suite("Sustained-music-offset seam integration (t1py / xtpf)")
struct SustainedMusicOffsetSeamIntegrationTests {

    private let assetId = "smo-integration"
    private let episodeDuration = 90.0

    private let chunkDuration = 3.0

    // Contiguous 3s chunks across the whole [0,90) episode — the atomizer makes
    // ONE atom per chunk, so short chunks give the post-roll music run several
    // atoms to span (proving the WIDE, multi-atom proposal). All text is
    // deliberately ad-free so the ONLY proposal source under test is the music
    // proposer (no lexical / sponsor / fingerprint / classifier co-firing).
    private func makeChunks() -> [TranscriptChunk] {
        let count = Int(episodeDuration / chunkDuration)  // 30
        return (0..<count).map { idx in
            let start = Double(idx) * chunkDuration
            let text = "Segment \(idx) of ordinary spoken conversation about coastal tide pools and slow patient observation."
            return TranscriptChunk(
                id: "c\(idx)-\(assetId)",
                analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)",
                chunkIndex: idx,
                startTime: start,
                endTime: start + chunkDuration,
                text: text,
                normalizedText: text.lowercased(),
                pass: "final",
                modelVersion: "v",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
    }

    /// Feature windows: flat RMS / flux / pause everywhere (so the acoustic
    /// break detector finds NOTHING and cannot confound the test), with a
    /// sustained high `musicProbability` play-out run in [60, 74).
    private func makeFeatureWindows() -> [FeatureWindow] {
        var windows: [FeatureWindow] = []
        var t: Double = 0
        while t < episodeDuration {
            let inMusicRun = t >= 60 && t < 74
            windows.append(
                FeatureWindow(
                    analysisAssetId: assetId,
                    startTime: t,
                    endTime: t + 2.0,
                    rms: 0.3,
                    spectralFlux: 0.05,
                    musicProbability: inMusicRun ? 0.9 : 0.0,
                    pauseProbability: 0.0,
                    speakerClusterId: 1,
                    jingleHash: nil,
                    featureVersion: 5
                )
            )
            t += 2.0
        }
        return windows
    }

    private func makeInput(
        enabled: Bool,
        lexicalCandidates: [LexicalCandidate] = []
    ) -> RegionShadowPhase.Input {
        RegionShadowPhase.Input(
            analysisAssetId: assetId,
            chunks: makeChunks(),
            lexicalCandidates: lexicalCandidates,
            featureWindows: makeFeatureWindows(),
            episodeDuration: episodeDuration,
            priors: ShowPriors.from(profile: nil),
            podcastProfile: nil,
            fmWindows: [],
            sustainedMusicProposerEnabled: enabled
        )
    }

    // MARK: - Property 1: flag-off equivalence

    @Test("FLAG OFF ⇒ byte-identical: a sustained-music run adds no region and perturbs no existing region")
    func flagOffIsByteIdenticalNoOp() async throws {
        // An early, disjoint lexical candidate produces a NON-music region that
        // must be identical between the OFF and ON runs (proving the seam is
        // purely additive and never perturbs existing regions).
        let lexical = LexicalCandidate(
            id: "lex-1",
            analysisAssetId: assetId,
            startTime: 8.0,
            endTime: 20.0,
            confidence: 0.8,
            hitCount: 2,
            categories: [.sponsor],
            evidenceText: "evidence",
            evidenceStartTime: 8.0,
            detectorVersion: "lexical-v1"
        )

        let off = try await RegionShadowPhase.run(makeInput(enabled: false, lexicalCandidates: [lexical]))
        let on = try await RegionShadowPhase.run(makeInput(enabled: true, lexicalCandidates: [lexical]))

        // OFF: the proposer never ran → no music origin, no music spans.
        #expect(off.allSatisfy { !$0.region.origins.contains(.sustainedMusic) })
        #expect(off.allSatisfy { $0.region.proposedMusicSpans.isEmpty })
        #expect(!off.isEmpty, "the disjoint lexical candidate must still produce a region")

        // ON: exactly one additional region, and it is the music region.
        let onMusic = on.filter { $0.region.origins.contains(.sustainedMusic) }
        #expect(onMusic.count == 1, "flag ON must add exactly one sustainedMusic region")
        #expect(on.count == off.count + 1, "the seam is purely additive")

        // Every non-music region in ON is byte-identical to its OFF counterpart.
        func key(_ b: RegionFeatureBundle) -> String {
            "\(b.region.firstAtomOrdinal)-\(b.region.lastAtomOrdinal)-\(b.region.origins.rawValue)"
        }
        let offByKey = Dictionary(uniqueKeysWithValues: off.map { (key($0), $0) })
        for onBundle in on where !onBundle.region.origins.contains(.sustainedMusic) {
            guard let offBundle = offByKey[key(onBundle)] else {
                Issue.record("ON produced a non-music region absent from OFF: \(key(onBundle))")
                continue
            }
            #expect(onBundle.region.firstAtomOrdinal == offBundle.region.firstAtomOrdinal)
            #expect(onBundle.region.lastAtomOrdinal == offBundle.region.lastAtomOrdinal)
            #expect(onBundle.region.origins.rawValue == offBundle.region.origins.rawValue)
            #expect(onBundle.lexicalScore == offBundle.lexicalScore)
            #expect(onBundle.lexicalHitCount == offBundle.lexicalHitCount)
        }
    }

    // MARK: - Property 2: flag-on → wide independent anchor → markOnly

    @Test("FLAG ON ⇒ FM-missed post-roll music run anchors independently and decodes to a .markOnly span on music provenance alone")
    func flagOnPostRollDecodesToMarkOnly() async throws {
        let bundles = try await RegionShadowPhase.run(makeInput(enabled: true))

        // A wide sustainedMusic-origin region exists (not a 1-atom anchor).
        let musicRegions = bundles.filter { $0.region.origins.contains(.sustainedMusic) }
        #expect(musicRegions.count == 1)
        let musicRegion = musicRegions[0].region
        #expect(
            musicRegion.lastAtomOrdinal > musicRegion.firstAtomOrdinal,
            "the music proposal must be atom-range WIDE, not a single atom"
        )

        // Re-atomize exactly as RegionShadowPhase.run does, so the projector sees
        // the identical atom ordinals/times.
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: makeChunks(),
            analysisAssetId: assetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )

        let projector = AtomEvidenceProjector()
        let evidence = await projector.project(
            regions: bundles,
            catalog: EvidenceCatalog(analysisAssetId: assetId, transcriptVersion: atoms[0].atomKey.transcriptVersion, entries: []),
            atoms: atoms,
            correctionMaskProvider: NoCorrectionMaskProvider()
        )

        // Independent anchoring (Path 5): at least one atom is anchored SOLELY by
        // the music offset, with NO FM co-firing anywhere in the evidence.
        let musicAnchoredAtoms = evidence.filter { ev in
            ev.isAnchored && ev.anchorProvenance.contains {
                if case .sustainedMusicOffset = $0 { return true }
                return false
            }
        }
        #expect(!musicAnchoredAtoms.isEmpty, "the music run must anchor its atoms independently of FM")
        let anyFM = evidence.contains { ev in
            ev.anchorProvenance.contains {
                switch $0 {
                case .fmConsensus, .fmAcousticCorroborated: return true
                default: return false
                }
            }
        }
        #expect(!anyFM, "FM must produce nothing for this region — survival is on music alone")

        // Decode to spans.
        let decoder = MinimalContiguousSpanDecoder()
        let spans = decoder.decode(atoms: evidence, assetId: assetId)

        // Exactly the music span survives, anchored ONLY by sustainedMusicOffset.
        #expect(spans.count == 1, "only the music-anchored run should decode to a span")
        let span = spans[0]
        #expect(span.anchorProvenance.contains {
            if case .sustainedMusicOffset = $0 { return true }
            return false
        })
        #expect(span.anchorProvenance.allSatisfy {
            if case .sustainedMusicOffset = $0 { return true }
            return false
        }, "the decoded span must be music-ONLY (no other anchor)")
        // Sits in the post-roll (past all [0,60) content), within the episode.
        #expect(span.startTime >= 59.0 && span.endTime <= episodeDuration, "span sits at the post-roll music run")

        // Fuse + map: a music-only span must NEVER auto-skip → .markOnly.
        let fusion = BackfillEvidenceFusion(
            span: span,
            classifierScore: 0.0,
            fmEntries: [],
            lexicalEntries: [],
            acousticEntries: [],
            catalogEntries: [],
            mode: .off,
            config: FusionWeightConfig()
        )
        let ledger = fusion.buildLedger()
        let decision = DecisionMapper(
            span: span,
            ledger: ledger,
            config: FusionWeightConfig(),
            transcriptQuality: .good
        ).map()

        #expect(decision.eligibilityGate == .markOnly, "music-only span must be markOnly, never auto-skip")
    }
}
