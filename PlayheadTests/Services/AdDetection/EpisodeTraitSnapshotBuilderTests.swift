// EpisodeTraitSnapshotBuilderTests.swift
// playhead-v7v8: tests for the production producer that derives
// `EpisodeTraitSnapshot` from live backfill signals (FeatureWindow,
// TranscriptChunk, AdWindow, episodeDuration).
//
// The producer is intentionally pure / static — it does not read the
// AnalysisStore. Inputs flow in from `runBackfill`'s in-scope variables
// after the fusion loop completes, and the resulting snapshot is folded
// into the persisted `ShowTraitProfile` inside `updatePriors`'
// `mutateProfile` closure (so the trait write happens atomically with
// the rest of the priors update).

import Foundation
import Testing
@testable import Playhead

@Suite("EpisodeTraitSnapshotBuilder (playhead-v7v8)")
struct EpisodeTraitSnapshotBuilderTests {

    // MARK: - musicDensity

    @Test("musicDensity equals mean musicProbability across feature windows")
    func musicDensityIsMeanMusicProbability() {
        let windows: [FeatureWindow] = [
            makeFeatureWindow(musicProbability: 0.0),
            makeFeatureWindow(musicProbability: 0.5),
            makeFeatureWindow(musicProbability: 1.0)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: windows,
            chunks: [],
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 600
        )
        #expect(abs(snap.musicDensity - 0.5) < 0.001)
    }

    @Test("musicDensity defaults to 0 when no feature windows are available")
    func musicDensityZeroWithNoWindows() {
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 600
        )
        #expect(snap.musicDensity == 0.0)
    }

    @Test("musicDensity clamps out-of-range musicProbability inputs")
    func musicDensityClampsRogueWindowValues() {
        // FeatureWindow's stored property is unconstrained, but the
        // snapshot's init clamps to [0,1]. The producer must not blow
        // past 1.0 even if a corrupt feature row carries a value > 1.
        let windows: [FeatureWindow] = [
            makeFeatureWindow(musicProbability: 2.0),
            makeFeatureWindow(musicProbability: 5.0)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: windows,
            chunks: [],
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 600
        )
        #expect(snap.musicDensity == 1.0)
    }

    // MARK: - speakerTurnRate

    @Test("speakerTurnRate is turns-per-minute across distinct speakerIds")
    func speakerTurnRateBasic() {
        // 6 chunks, 5 transitions over 60 seconds (1 minute) → 5 turns/min.
        let chunks: [TranscriptChunk] = [
            makeChunk(start: 0, end: 10, speakerId: 1),
            makeChunk(start: 10, end: 20, speakerId: 2),
            makeChunk(start: 20, end: 30, speakerId: 1),
            makeChunk(start: 30, end: 40, speakerId: 2),
            makeChunk(start: 40, end: 50, speakerId: 1),
            makeChunk(start: 50, end: 60, speakerId: 2)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: chunks,
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 60
        )
        #expect(abs(snap.speakerTurnRate - 5.0) < 0.001)
    }

    @Test("speakerTurnRate is zero when chunks lack speakerIds")
    func speakerTurnRateZeroWithoutSpeakerIds() {
        let chunks: [TranscriptChunk] = [
            makeChunk(start: 0, end: 10, speakerId: nil),
            makeChunk(start: 10, end: 20, speakerId: nil)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: chunks,
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 60
        )
        #expect(snap.speakerTurnRate == 0.0)
    }

    @Test("speakerTurnRate is zero when episodeDuration is non-positive")
    func speakerTurnRateZeroEpisodeDuration() {
        let chunks: [TranscriptChunk] = [
            makeChunk(start: 0, end: 10, speakerId: 1),
            makeChunk(start: 10, end: 20, speakerId: 2)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: chunks,
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 0
        )
        // Producer must not divide by zero.
        #expect(snap.speakerTurnRate == 0.0)
    }

    // MARK: - singleSpeakerDominance

    @Test("singleSpeakerDominance is fraction of chunks held by the dominant speaker")
    func singleSpeakerDominanceMonologue() {
        // 9/10 chunks are speaker 1.
        var chunks: [TranscriptChunk] = []
        for i in 0..<10 {
            let start: Double = Double(i) * 10
            let end: Double = Double(i + 1) * 10
            let sid: Int = i == 0 ? 2 : 1
            chunks.append(makeChunk(start: start, end: end, speakerId: sid))
        }
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: chunks,
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 100
        )
        #expect(abs(snap.singleSpeakerDominance - 0.9) < 0.001)
    }

    @Test("singleSpeakerDominance defaults to 0.5 with no speakerIds")
    func singleSpeakerDominanceUnknown() {
        let chunks: [TranscriptChunk] = [
            makeChunk(start: 0, end: 10, speakerId: nil),
            makeChunk(start: 10, end: 20, speakerId: nil)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: chunks,
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 60
        )
        // Maximum-uncertainty default.
        #expect(snap.singleSpeakerDominance == 0.5)
    }

    // MARK: - structureRegularity

    @Test("structureRegularity is high when ad slots align with prior slots")
    func structureRegularityHighWhenAligned() {
        // Existing profile has ad slots at 0.1 and 0.5; this episode's ads
        // land at the same positions → high regularity.
        let priorSlots = "[0.1, 0.5]"
        let existing = makeProfile(normalizedAdSlotPriors: priorSlots)
        let ads: [AdWindow] = [
            makeAdWindow(start: 60, end: 90),    // center 75 → 0.125
            makeAdWindow(start: 295, end: 305)   // center 300 → 0.5
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: ads,
            existingProfile: existing,
            episodeDuration: 600
        )
        #expect(snap.structureRegularity > 0.7)
    }

    @Test("structureRegularity is low when ad slots scatter from prior slots")
    func structureRegularityLowWhenScattered() {
        let priorSlots = "[0.05]"
        let existing = makeProfile(normalizedAdSlotPriors: priorSlots)
        let ads: [AdWindow] = [
            makeAdWindow(start: 540, end: 570)  // center 555 → 0.925, far from 0.05
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: ads,
            existingProfile: existing,
            episodeDuration: 600
        )
        #expect(snap.structureRegularity < 0.5)
    }

    @Test("structureRegularity defaults to 0.5 when no prior slots exist")
    func structureRegularityUnknownWithoutPriors() {
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: [makeAdWindow(start: 60, end: 90)],
            existingProfile: nil,
            episodeDuration: 600
        )
        #expect(snap.structureRegularity == 0.5)
    }

    @Test("structureRegularity is neutral 0.5 when normalizedAdSlotPriors JSON is malformed")
    func structureRegularityNeutralOnMalformedPriors() {
        // Cycle-2 M-T2: corrupt JSON in the persisted prior must not propagate
        // a misleading regularity score; `decodeSlotPriors` returning nil
        // should fall back to the documented neutral default rather than 0.
        let existing = makeProfile(normalizedAdSlotPriors: "{ not valid json")
        let ads: [AdWindow] = [
            makeAdWindow(start: 60, end: 90)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: ads,
            existingProfile: existing,
            episodeDuration: 600
        )
        #expect(snap.structureRegularity == 0.5)
    }

    // MARK: - sponsorRecurrence

    @Test("sponsorRecurrence is fraction of advertisers seen in existing lexicon")
    func sponsorRecurrenceFraction() {
        // existing lexicon has [acme, beta]; this episode's ads carry
        // [acme, gamma] → 1/2 = 0.5 recurrence.
        let existing = makeProfile(sponsorLexicon: "acme,beta")
        let ads: [AdWindow] = [
            makeAdWindow(start: 60, end: 90, advertiser: "ACME"),
            makeAdWindow(start: 200, end: 230, advertiser: "Gamma")
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: ads,
            existingProfile: existing,
            episodeDuration: 600
        )
        #expect(abs(snap.sponsorRecurrence - 0.5) < 0.001)
    }

    @Test("sponsorRecurrence defaults to 0 when no advertisers are tagged")
    func sponsorRecurrenceZeroWithoutAdvertisers() {
        let existing = makeProfile(sponsorLexicon: "acme,beta")
        let ads: [AdWindow] = [
            makeAdWindow(start: 60, end: 90, advertiser: nil)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: ads,
            existingProfile: existing,
            episodeDuration: 600
        )
        #expect(snap.sponsorRecurrence == 0.0)
    }

    // MARK: - insertionVolatility

    @Test("insertionVolatility is the inverse of structureRegularity")
    func insertionVolatilityInverseOfRegularity() {
        let priorSlots = "[0.1]"
        let existing = makeProfile(normalizedAdSlotPriors: priorSlots)
        let ads: [AdWindow] = [
            makeAdWindow(start: 60, end: 90)  // center 75 → 0.125 (close to 0.1)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: ads,
            existingProfile: existing,
            episodeDuration: 600
        )
        // Their sum must equal 1.0 by definition of volatility = 1 - regularity.
        #expect(abs((snap.structureRegularity + snap.insertionVolatility) - 1.0) < 0.001)
    }

    // MARK: - transcriptReliability

    @Test("transcriptReliability defaults to 0.7 when no chunks observed")
    func transcriptReliabilityDefault() {
        // Conservative neutral default until a richer signal is wired.
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [],
            chunks: [],
            confirmedAdWindows: [],
            existingProfile: nil,
            episodeDuration: 600
        )
        #expect(snap.transcriptReliability == 0.7)
    }

    @Test("transcriptReliability is the 0.7 placeholder regardless of inputs")
    func transcriptReliabilityIsPlaceholderConstant() {
        // Locks in M4 placeholder; remove this assertion once playhead-snat lands.
        // The producer currently hard-codes `transcriptReliability: Float = 0.7`,
        // so any combination of inputs must surface the same constant. When
        // playhead-snat replaces the placeholder with a real ASR-confidence
        // signal, this test should be deleted (not weakened to a tolerance).
        let chunks: [TranscriptChunk] = [
            makeChunk(start: 0, end: 10, speakerId: 1),
            makeChunk(start: 10, end: 20, speakerId: 2)
        ]
        let snap = EpisodeTraitSnapshotBuilder.build(
            featureWindows: [makeFeatureWindow(musicProbability: 0.42)],
            chunks: chunks,
            confirmedAdWindows: [makeAdWindow(start: 60, end: 90, advertiser: "ACME")],
            existingProfile: makeProfile(sponsorLexicon: "acme"),
            episodeDuration: 600
        )
        #expect(snap.transcriptReliability == 0.7)
    }

    // MARK: - End-to-end: producer feeds EMA merge into ShowTraitProfile.updated

    @Test("snapshot from real signal updates an unknown profile to a reliable one after 3 episodes")
    func snapshotPipelineProducesReliableProfile() {
        // Drive 3 backfills: each emits a snapshot that the ShowTraitProfile
        // EMA absorbs. After 3 the profile is reliable, gating the trait
        // tier in PriorHierarchyResolver.
        var profile = ShowTraitProfile.unknown
        for _ in 0..<3 {
            let windows: [FeatureWindow] = [
                makeFeatureWindow(musicProbability: 0.3)
            ]
            let snap = EpisodeTraitSnapshotBuilder.build(
                featureWindows: windows,
                chunks: [],
                confirmedAdWindows: [],
                existingProfile: nil,
                episodeDuration: 600
            )
            profile = profile.updated(from: snap)
        }
        #expect(profile.episodesObserved == 3)
        #expect(profile.isReliable)
        // EMA should converge toward the steady 0.3 musicDensity signal.
        #expect(abs(profile.musicDensity - 0.3) < 0.001)
    }

    // MARK: - Helpers

    private func makeFeatureWindow(musicProbability: Double) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "asset-1",
            startTime: 0,
            endTime: 1,
            rms: 0,
            spectralFlux: 0,
            musicProbability: musicProbability,
            speakerChangeProxyScore: 0,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: .none,
            pauseProbability: 0,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        )
    }

    private func makeChunk(
        start: Double,
        end: Double,
        speakerId: Int?
    ) -> TranscriptChunk {
        TranscriptChunk(
            id: "chunk-\(start)-\(end)",
            analysisAssetId: "asset-1",
            segmentFingerprint: "fp",
            chunkIndex: 0,
            startTime: start,
            endTime: end,
            text: "text",
            normalizedText: "text",
            pass: "final",
            modelVersion: "v1",
            transcriptVersion: "tv1",
            atomOrdinal: 0,
            weakAnchorMetadata: nil,
            speakerId: speakerId
        )
    }

    private func makeAdWindow(
        start: Double,
        end: Double,
        advertiser: String? = nil
    ) -> AdWindow {
        AdWindow(
            id: "win-\(start)-\(end)",
            analysisAssetId: "asset-1",
            startTime: start,
            endTime: end,
            confidence: 0.95,
            boundaryState: "confirmed",
            decisionState: AdDecisionState.applied.rawValue,
            detectorVersion: "detection-v1",
            advertiser: advertiser,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "test",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false
        )
    }

    private func makeProfile(
        sponsorLexicon: String? = nil,
        normalizedAdSlotPriors: String? = nil
    ) -> PodcastProfile {
        PodcastProfile(
            podcastId: "podcast-1",
            sponsorLexicon: sponsorLexicon,
            normalizedAdSlotPriors: normalizedAdSlotPriors,
            repeatedCTAFragments: nil,
            jingleFingerprints: nil,
            implicitFalsePositiveCount: 0,
            skipTrustScore: 0.5,
            observationCount: 1,
            mode: "shadow",
            recentFalseSkipSignals: 0
        )
    }
}
