// ShowTraitProfileTests.swift
// Tests for ShowTraitProfile and EpisodeTraitSnapshot.

import Foundation
import Testing
@testable import Playhead

@Suite("ShowTraitProfile")
struct ShowTraitProfileTests {

    // MARK: - Helpers

    private func makeSnapshot(
        musicDensity: Float = 0.3,
        speakerTurnRate: Float = 4.0,
        singleSpeakerDominance: Float = 0.6,
        structureRegularity: Float = 0.5,
        sponsorRecurrence: Float = 0.2,
        insertionVolatility: Float = 0.4,
        transcriptReliability: Float = 0.8
    ) -> EpisodeTraitSnapshot {
        EpisodeTraitSnapshot(
            musicDensity: musicDensity,
            speakerTurnRate: speakerTurnRate,
            singleSpeakerDominance: singleSpeakerDominance,
            structureRegularity: structureRegularity,
            sponsorRecurrence: sponsorRecurrence,
            insertionVolatility: insertionVolatility,
            transcriptReliability: transcriptReliability
        )
    }

    // MARK: - Unknown sentinel

    @Test("unknown sentinel has all traits at 0.5 and 0 episodes")
    func unknownSentinel() {
        let profile = ShowTraitProfile.unknown
        #expect(profile.musicDensity == 0.5)
        #expect(profile.speakerTurnRate == 0.5)
        #expect(profile.singleSpeakerDominance == 0.5)
        #expect(profile.structureRegularity == 0.5)
        #expect(profile.sponsorRecurrence == 0.5)
        #expect(profile.insertionVolatility == 0.5)
        #expect(profile.transcriptReliability == 0.5)
        #expect(profile.episodesObserved == 0)
    }

    // MARK: - Reliability gating

    @Test("isReliable is false with 0 episodes")
    func reliableZeroEpisodes() {
        #expect(!ShowTraitProfile.unknown.isReliable)
    }

    @Test("isReliable is false with 1 episode")
    func reliableOneEpisode() {
        let profile = ShowTraitProfile.unknown.updated(from: makeSnapshot())
        #expect(profile.episodesObserved == 1)
        #expect(!profile.isReliable)
    }

    @Test("isReliable is false with 2 episodes")
    func reliableTwoEpisodes() {
        var profile = ShowTraitProfile.unknown
        for _ in 0..<2 {
            profile = profile.updated(from: makeSnapshot())
        }
        #expect(profile.episodesObserved == 2)
        #expect(!profile.isReliable)
    }

    @Test("isReliable is true with 3 episodes")
    func reliableThreeEpisodes() {
        var profile = ShowTraitProfile.unknown
        for _ in 0..<3 {
            profile = profile.updated(from: makeSnapshot())
        }
        #expect(profile.episodesObserved == 3)
        #expect(profile.isReliable)
    }

    @Test("isReliable is true with many episodes")
    func reliableManyEpisodes() {
        var profile = ShowTraitProfile.unknown
        for _ in 0..<20 {
            profile = profile.updated(from: makeSnapshot())
        }
        #expect(profile.episodesObserved == 20)
        #expect(profile.isReliable)
    }

    // MARK: - First episode replaces sentinel

    @Test("first episode snapshot replaces unknown sentinel directly")
    func firstEpisodeReplacesUnknown() {
        let snapshot = makeSnapshot(
            musicDensity: 0.8,
            speakerTurnRate: 12.0,
            singleSpeakerDominance: 0.9,
            structureRegularity: 0.1,
            sponsorRecurrence: 0.0,
            insertionVolatility: 1.0,
            transcriptReliability: 0.3
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)

        #expect(profile.musicDensity == 0.8)
        #expect(profile.speakerTurnRate == 12.0)
        #expect(profile.singleSpeakerDominance == 0.9)
        #expect(profile.structureRegularity == 0.1)
        #expect(profile.sponsorRecurrence == 0.0)
        #expect(profile.insertionVolatility == 1.0)
        #expect(profile.transcriptReliability == 0.3)
        #expect(profile.episodesObserved == 1)
    }

    // MARK: - EMA incremental updates

    @Test("second episode applies EMA blending")
    func secondEpisodeEMA() {
        let first = makeSnapshot(musicDensity: 1.0, speakerTurnRate: 10.0)
        let second = makeSnapshot(musicDensity: 0.0, speakerTurnRate: 0.0)

        let profile = ShowTraitProfile.unknown
            .updated(from: first)
            .updated(from: second)

        // EMA: 0.3 * 0.0 + 0.7 * 1.0 = 0.7
        #expect(abs(profile.musicDensity - 0.7) < 0.001)
        // EMA: 0.3 * 0.0 + 0.7 * 10.0 = 7.0
        #expect(abs(profile.speakerTurnRate - 7.0) < 0.001)
        #expect(profile.episodesObserved == 2)
    }

    @Test("EMA converges toward repeated signal")
    func emaConvergence() {
        // Start with a snapshot at 0.0, then feed repeated snapshots at 1.0.
        // The profile should converge toward 1.0.
        let initial = makeSnapshot(musicDensity: 0.0)
        let repeated = makeSnapshot(musicDensity: 1.0)

        var profile = ShowTraitProfile.unknown.updated(from: initial)
        #expect(abs(profile.musicDensity - 0.0) < 0.001)

        for _ in 0..<20 {
            profile = profile.updated(from: repeated)
        }

        // After 20 episodes at 1.0, should be very close to 1.0
        #expect(profile.musicDensity > 0.99)
        #expect(profile.episodesObserved == 21)
    }

    @Test("EMA with constant input preserves value")
    func emaConstantInput() {
        let snapshot = makeSnapshot(musicDensity: 0.6, speakerTurnRate: 5.0)
        var profile = ShowTraitProfile.unknown.updated(from: snapshot)

        for _ in 0..<10 {
            profile = profile.updated(from: snapshot)
        }

        // Constant input through EMA should stay constant.
        #expect(abs(profile.musicDensity - 0.6) < 0.001)
        #expect(abs(profile.speakerTurnRate - 5.0) < 0.001)
    }

    // MARK: - Debug archetype labels

    @Test("debugArchetypeLabel returns nil for 0 episodes")
    func archetypeLabelNoEpisodes() {
        #expect(ShowTraitProfile.unknown.debugArchetypeLabel == nil)
    }

    @Test("debugArchetypeLabel returns general for unremarkable profile")
    func archetypeLabelGeneral() {
        let snapshot = makeSnapshot(
            musicDensity: 0.4,
            speakerTurnRate: 2.0,
            singleSpeakerDominance: 0.5,
            structureRegularity: 0.5
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        #expect(profile.debugArchetypeLabel == "general")
    }

    @Test("debugArchetypeLabel detects music-heavy show")
    func archetypeLabelMusicHeavy() {
        let snapshot = makeSnapshot(
            musicDensity: 0.8,
            speakerTurnRate: 2.0,
            singleSpeakerDominance: 0.5,
            structureRegularity: 0.5
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        let label = profile.debugArchetypeLabel!
        #expect(label.contains("music-heavy"))
    }

    @Test("debugArchetypeLabel detects interview format")
    func archetypeLabelInterview() {
        let snapshot = makeSnapshot(
            musicDensity: 0.1,
            speakerTurnRate: 5.0,
            singleSpeakerDominance: 0.3,
            structureRegularity: 0.5
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        let label = profile.debugArchetypeLabel!
        #expect(label.contains("interview"))
    }

    @Test("debugArchetypeLabel detects structured news")
    func archetypeLabelStructuredNews() {
        let snapshot = makeSnapshot(
            musicDensity: 0.2,
            speakerTurnRate: 1.5,
            singleSpeakerDominance: 0.8,
            structureRegularity: 0.9
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        let label = profile.debugArchetypeLabel!
        #expect(label.contains("structured"))
        #expect(label.contains("news"))
    }

    @Test("debugArchetypeLabel detects monologue")
    func archetypeLabelMonologue() {
        let snapshot = makeSnapshot(
            musicDensity: 0.1,
            speakerTurnRate: 1.0,
            singleSpeakerDominance: 0.9,
            structureRegularity: 0.4
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        let label = profile.debugArchetypeLabel!
        #expect(label.contains("monologue"))
    }

    @Test("debugArchetypeLabel detects freeform")
    func archetypeLabelFreeform() {
        let snapshot = makeSnapshot(
            musicDensity: 0.4,
            speakerTurnRate: 2.0,
            singleSpeakerDominance: 0.5,
            structureRegularity: 0.1
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        let label = profile.debugArchetypeLabel!
        #expect(label.contains("freeform"))
    }

    @Test("debugArchetypeLabel combines multiple labels")
    func archetypeLabelCombined() {
        let snapshot = makeSnapshot(
            musicDensity: 0.8,
            speakerTurnRate: 1.0,
            singleSpeakerDominance: 0.9,
            structureRegularity: 0.9
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        let label = profile.debugArchetypeLabel!
        #expect(label.contains("music-heavy"))
        #expect(label.contains("monologue"))
        #expect(label.contains("structured"))
    }

    // MARK: - Codable round-trip

    @Test("profile survives JSON encode/decode round-trip")
    func codableRoundTrip() throws {
        let snapshot = makeSnapshot(
            musicDensity: 0.7,
            speakerTurnRate: 6.5,
            singleSpeakerDominance: 0.3,
            structureRegularity: 0.8,
            sponsorRecurrence: 0.9,
            insertionVolatility: 0.1,
            transcriptReliability: 0.95
        )
        var profile = ShowTraitProfile.unknown
        for _ in 0..<5 {
            profile = profile.updated(from: snapshot)
        }

        let data = try JSONEncoder().encode(profile)
        let decoded = try JSONDecoder().decode(ShowTraitProfile.self, from: data)

        #expect(decoded == profile)
        #expect(decoded.episodesObserved == 5)
        #expect(decoded.isReliable)
    }

    // MARK: - EpisodeTraitSnapshot

    @Test("EpisodeTraitSnapshot survives JSON round-trip")
    func snapshotCodableRoundTrip() throws {
        let snapshot = makeSnapshot()
        let data = try JSONEncoder().encode(snapshot)
        let decoded = try JSONDecoder().decode(EpisodeTraitSnapshot.self, from: data)
        #expect(decoded == snapshot)
    }

    // MARK: - Edge cases

    @Test("all-zero snapshot produces valid profile")
    func allZeroSnapshot() {
        let snapshot = EpisodeTraitSnapshot(
            musicDensity: 0.0,
            speakerTurnRate: 0.0,
            singleSpeakerDominance: 0.0,
            structureRegularity: 0.0,
            sponsorRecurrence: 0.0,
            insertionVolatility: 0.0,
            transcriptReliability: 0.0
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        #expect(profile.musicDensity == 0.0)
        #expect(profile.episodesObserved == 1)
    }

    @Test("all-one snapshot produces valid profile")
    func allOneSnapshot() {
        let snapshot = EpisodeTraitSnapshot(
            musicDensity: 1.0,
            speakerTurnRate: 1.0,
            singleSpeakerDominance: 1.0,
            structureRegularity: 1.0,
            sponsorRecurrence: 1.0,
            insertionVolatility: 1.0,
            transcriptReliability: 1.0
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        #expect(profile.musicDensity == 1.0)
        #expect(profile.transcriptReliability == 1.0)
    }

    @Test("high speakerTurnRate works for rapid-exchange label")
    func rapidExchangeLabel() {
        let snapshot = makeSnapshot(
            speakerTurnRate: 12.0,
            singleSpeakerDominance: 0.5,
            structureRegularity: 0.5
        )
        let profile = ShowTraitProfile.unknown.updated(from: snapshot)
        let label = profile.debugArchetypeLabel!
        #expect(label.contains("rapid-exchange"))
    }

    @Test("EMA alpha is 0.3")
    func emaAlphaValue() {
        #expect(ShowTraitProfile.emaAlpha == 0.3)
    }

    @Test("out-of-range snapshot values are clamped")
    func snapshotClamping() {
        let snapshot = EpisodeTraitSnapshot(
            musicDensity: 1.5,
            speakerTurnRate: -2.0,
            singleSpeakerDominance: -0.1,
            structureRegularity: 2.0,
            sponsorRecurrence: -1.0,
            insertionVolatility: 5.0,
            transcriptReliability: -0.5
        )
        #expect(snapshot.musicDensity == 1.0)
        #expect(snapshot.speakerTurnRate == 0.0)
        #expect(snapshot.singleSpeakerDominance == 0.0)
        #expect(snapshot.structureRegularity == 1.0)
        #expect(snapshot.sponsorRecurrence == 0.0)
        #expect(snapshot.insertionVolatility == 1.0)
        #expect(snapshot.transcriptReliability == 0.0)
    }

    // MARK: - Codable decode clamping (cycle-1 L5)
    //
    // Mirrors the cycle-1 084j fix on `AdDurationStats`: a hand-edited or
    // version-skewed JSON payload must not slip past the `[0,1]` clamp.
    // `JSONDecoder`'s synthesized `init(from:)` writes raw values directly
    // to the stored `let` properties, bypassing the `min(max(..., 0), 1)`
    // clamp at the memberwise initializer. The custom `init(from:)` funnels
    // raw values through `init(...)` so the clamp is authoritative across
    // every construction path.

    @Test("EpisodeTraitSnapshot decode clamps out-of-range values")
    func episodeTraitSnapshotDecodeClampsOutOfRangeValues() throws {
        // Every clamped field set above 1.0 — must decode clamped to 1.0.
        let payload = #"""
        {
            "musicDensity": 1.5,
            "speakerTurnRate": 4.0,
            "singleSpeakerDominance": 1.2,
            "structureRegularity": 2.0,
            "sponsorRecurrence": 1.7,
            "insertionVolatility": 5.0,
            "transcriptReliability": 1.1
        }
        """#
        let snap = try JSONDecoder().decode(
            EpisodeTraitSnapshot.self,
            from: Data(payload.utf8)
        )
        #expect(snap.musicDensity == 1.0)
        #expect(snap.speakerTurnRate == 4.0)
        #expect(snap.singleSpeakerDominance == 1.0)
        #expect(snap.structureRegularity == 1.0)
        #expect(snap.sponsorRecurrence == 1.0)
        #expect(snap.insertionVolatility == 1.0)
        #expect(snap.transcriptReliability == 1.0)
    }

    @Test("EpisodeTraitSnapshot decode clamps negative values")
    func episodeTraitSnapshotDecodeClampsNegativeValues() throws {
        // Every clamped field set below 0 — must decode clamped to 0.
        // speakerTurnRate is also clamped non-negative.
        let payload = #"""
        {
            "musicDensity": -0.3,
            "speakerTurnRate": -1.5,
            "singleSpeakerDominance": -0.1,
            "structureRegularity": -2.0,
            "sponsorRecurrence": -0.5,
            "insertionVolatility": -0.7,
            "transcriptReliability": -0.9
        }
        """#
        let snap = try JSONDecoder().decode(
            EpisodeTraitSnapshot.self,
            from: Data(payload.utf8)
        )
        #expect(snap.musicDensity == 0.0)
        #expect(snap.speakerTurnRate == 0.0)
        #expect(snap.singleSpeakerDominance == 0.0)
        #expect(snap.structureRegularity == 0.0)
        #expect(snap.sponsorRecurrence == 0.0)
        #expect(snap.insertionVolatility == 0.0)
        #expect(snap.transcriptReliability == 0.0)
    }

    @Test("ShowTraitProfile decode clamps out-of-range values")
    func showTraitProfileDecodeClampsOutOfRangeValues() throws {
        let payload = #"""
        {
            "musicDensity": 1.8,
            "speakerTurnRate": 3.0,
            "singleSpeakerDominance": 1.4,
            "structureRegularity": 2.5,
            "sponsorRecurrence": 1.1,
            "insertionVolatility": 9.0,
            "transcriptReliability": 1.3,
            "episodesObserved": 7
        }
        """#
        let profile = try JSONDecoder().decode(
            ShowTraitProfile.self,
            from: Data(payload.utf8)
        )
        #expect(profile.musicDensity == 1.0)
        #expect(profile.speakerTurnRate == 3.0)
        #expect(profile.singleSpeakerDominance == 1.0)
        #expect(profile.structureRegularity == 1.0)
        #expect(profile.sponsorRecurrence == 1.0)
        #expect(profile.insertionVolatility == 1.0)
        #expect(profile.transcriptReliability == 1.0)
        #expect(profile.episodesObserved == 7)
    }

    @Test("ShowTraitProfile decode clamps negative values")
    func showTraitProfileDecodeClampsNegativeValues() throws {
        let payload = #"""
        {
            "musicDensity": -0.5,
            "speakerTurnRate": -2.0,
            "singleSpeakerDominance": -0.1,
            "structureRegularity": -1.5,
            "sponsorRecurrence": -0.3,
            "insertionVolatility": -3.0,
            "transcriptReliability": -0.4,
            "episodesObserved": 2
        }
        """#
        let profile = try JSONDecoder().decode(
            ShowTraitProfile.self,
            from: Data(payload.utf8)
        )
        #expect(profile.musicDensity == 0.0)
        #expect(profile.speakerTurnRate == 0.0)
        #expect(profile.singleSpeakerDominance == 0.0)
        #expect(profile.structureRegularity == 0.0)
        #expect(profile.sponsorRecurrence == 0.0)
        #expect(profile.insertionVolatility == 0.0)
        #expect(profile.transcriptReliability == 0.0)
        #expect(profile.episodesObserved == 2)
    }
}
