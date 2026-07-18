// MusicOffsetLexicalGateTests.swift
// playhead-eki3 (PR1): unit + integration coverage for the lexical ad-cue gate
// over the sustained-music-offset proposer's music-ONLY spans.
//
// Three layers:
//   1. Cue matcher (`hasAdCue`) — synthesized onset-window strings mirroring
//      the validated prototype's cases: underwriting / DTC / URL / promo → ad;
//      credits / host-intro / theme → not ad; first-party self-promo →
//      suppressed even with an ad-ish phrase (override).
//   2. Onset-window extraction (`onsetWindowText`) — the `>= edge - 2s`,
//      600-char, raw-text contract.
//   3. Seam integration through `RegionShadowPhase.run`: a music-only span
//      with content at the edge is suppressed with the flag ON and survives
//      with it OFF; a music-only span with a sponsor read at the edge survives
//      both; a music span corroborated by a lexical candidate is unaffected.

import Foundation
import Testing

@testable import Playhead

@Suite("MusicOffsetLexicalGate — cue matcher (playhead-eki3)")
struct MusicOffsetLexicalGateCueMatcherTests {

    // MARK: - AD cues → keep (true)

    @Test("underwriting cues are ad-cues")
    func underwritingIsAdCue() {
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "support for this program comes from the corporation for public broadcasting"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "this message comes from Capital One"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "this episode is brought to you by BetterHelp"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "sponsored by Squarespace, the all in one platform"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "today's sponsor is ExpressVPN"))
    }

    @Test("DTC / URL / promo cues are ad-cues")
    func dtcCtaIsAdCue() {
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "go to expressvpn.com to get three months free"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "sign up now at betterhelp.com slash pod"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "check out policygenius.com for a quote today"),
                "bare URL is an ad-cue")
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "use promo code POD at checkout"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "get twenty percent off, that's 20% off your first order"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "start your free trial today, no risk"))
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: "call 1-800-flowers to order"))
    }

    // MARK: - Content / credits / host-intro → suppress (false)

    @Test("credits / outro have no ad-cue")
    func creditsAreNotAdCue() {
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "thanks for listening everyone, we'll see you next week, take care"))
    }

    @Test("host intro / segment transition over music has no ad-cue")
    func hostIntroIsNotAdCue() {
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "welcome back to the program, today we are talking about coastal tide pools and the ocean"))
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "and now, our main story of the week"))
    }

    @Test("theme-music filler / empty text have no ad-cue")
    func themeAndEmptyAreNotAdCue() {
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "segment 12 of ordinary spoken conversation about slow patient observation"))
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: ""))
    }

    // MARK: - First-party self-promo → suppress (false), even overriding an ad-cue

    @Test("first-party house promo is NOT a third-party ad-cue")
    func firstPartyPromoIsSuppressed() {
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "listen ad-free by joining Wondery Plus"))
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "subscribe to the show wherever you get your podcasts"))
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "follow the show and check out our other podcasts"))
    }

    @Test("first-party marker OVERRIDES a co-occurring ad-cue")
    func firstPartyOverridesAdCue() {
        // Has a hard AD cue ("brought to you by" + a bare URL) AND a first-party
        // marker ("ad-free" / "join Wondery"). The first-party check runs first
        // and wins → suppress. Mirrors the prototype's FIRST_PARTY-before-AD order.
        #expect(!MusicOffsetLexicalGate.hasAdCue(inOnsetWindow:
            "brought to you by wondery.com — join Wondery to listen ad-free"))
    }
}

@Suite("MusicOffsetLexicalGate — onset window (playhead-eki3)")
struct MusicOffsetLexicalGateOnsetWindowTests {

    private func chunk(_ index: Int, _ start: Double, _ text: String) -> TranscriptChunk {
        TranscriptChunk(
            id: "c\(index)", analysisAssetId: "a", segmentFingerprint: "f\(index)",
            chunkIndex: index, startTime: start, endTime: start + 3.0,
            text: text, normalizedText: text.lowercased(),
            pass: "final", modelVersion: "v", transcriptVersion: nil, atomOrdinal: nil
        )
    }

    @Test("window starts at edge - 2s: chunks starting before the cutoff are excluded")
    func windowRespectsLeadCutoff() {
        let chunks = [
            chunk(0, 60.0, "PRE well before the edge"),
            chunk(1, 71.0, "PRE just before the cutoff"),   // 71 < 74 - 2 = 72 → excluded
            chunk(2, 72.0, "ON at the cutoff boundary"),     // 72 >= 72 → included
            chunk(3, 75.0, "ON after the edge"),
        ]
        let window = MusicOffsetLexicalGate.onsetWindowText(trailingEdge: 74.0, chunks: chunks)
        #expect(!window.contains("well before"))
        #expect(!window.contains("just before"))
        #expect(window.contains("at the cutoff boundary"))
        #expect(window.contains("after the edge"))
    }

    @Test("a mid-read brand cue a few seconds past the edge is still captured")
    func midReadCueIsCaptured() {
        // The host-read brand cue lands two chunks (~6s) after the edge — the
        // wide onset window (not a tight time bound) still catches it.
        let chunks = [
            chunk(0, 74.0, "so anyway that wraps our segment on tide pools"),
            chunk(1, 77.0, "before we go a quick word"),
            chunk(2, 80.0, "this episode is brought to you by BetterHelp"),
        ]
        let window = MusicOffsetLexicalGate.onsetWindowText(trailingEdge: 74.0, chunks: chunks)
        #expect(MusicOffsetLexicalGate.hasAdCue(inOnsetWindow: window))
    }

    @Test("window is capped at 600 characters")
    func windowIsCharCapped() {
        let long = String(repeating: "x", count: 2000)
        let chunks = [chunk(0, 74.0, long)]
        let window = MusicOffsetLexicalGate.onsetWindowText(trailingEdge: 74.0, chunks: chunks)
        #expect(window.count == 600)
    }
}

@Suite("MusicOffsetLexicalGate — scoping (playhead-eki3)")
struct MusicOffsetLexicalGateScopingTests {

    /// A minimal region carrying exactly `origins`; every other field is empty.
    /// `endTime` is the onset-window anchor.
    private func region(_ origins: ProposedRegionOrigins, endTime: Double = 75.0) -> ProposedRegion {
        ProposedRegion(
            analysisAssetId: "a", transcriptVersion: "t",
            firstAtomOrdinal: 20, lastAtomOrdinal: 24,
            startTime: 60.0, endTime: endTime,
            origins: origins, fmConsensusStrength: .none,
            lexicalCandidates: [], sponsorMatches: [], fingerprintMatches: [],
            acousticBreaks: [], foundationModelSpans: [],
            resolvedEvidenceAnchors: [], fmEvidence: nil
        )
    }

    private func chunk(_ index: Int, _ start: Double, _ text: String) -> TranscriptChunk {
        TranscriptChunk(
            id: "c\(index)", analysisAssetId: "a", segmentFingerprint: "f\(index)",
            chunkIndex: index, startTime: start, endTime: start + 3.0,
            text: text, normalizedText: text.lowercased(),
            pass: "final", modelVersion: "v", transcriptVersion: nil, atomOrdinal: nil
        )
    }

    /// Onset-window chunks (start >= 75 - 2 = 73) with NO ad-cue.
    private var fillerAfterEdge: [TranscriptChunk] {
        [chunk(0, 73.0, "ordinary conversation about tide pools and patient observation")]
    }

    /// Onset-window chunks with a genuine third-party ad-cue.
    private var cueAfterEdge: [TranscriptChunk] {
        [chunk(0, 73.0, "this episode is brought to you by BetterHelp, go to betterhelp.com")]
    }

    private let corroboratingOrigins: [ProposedRegionOrigins] =
        [.foundationModel, .lexical, .sponsor, .fingerprint, .classifier]

    @Test("isUncorroboratedMusicOnly: ONLY a music-only region (± .acoustic) is flagged")
    func onlyUncorroboratedMusicOnlyIsFlagged() {
        // Pure music-only → flagged.
        #expect(MusicOffsetLexicalGate.isUncorroboratedMusicOnly(region([.sustainedMusic])))
        // .acoustic is a bare hint, NOT corroboration → still flagged (the
        // load-bearing distinction that mirrors isMusicOnlyProvenance).
        #expect(MusicOffsetLexicalGate.isUncorroboratedMusicOnly(region([.sustainedMusic, .acoustic])))
        // Any real corroborating origin exempts the region.
        for corro in corroboratingOrigins {
            #expect(
                !MusicOffsetLexicalGate.isUncorroboratedMusicOnly(region([.sustainedMusic, corro])),
                "origin rawValue \(corro.rawValue) must exempt a music region from the gate"
            )
        }
        // Non-music regions are never flagged — the gate cannot touch them.
        #expect(!MusicOffsetLexicalGate.isUncorroboratedMusicOnly(region([.lexical])))
        #expect(!MusicOffsetLexicalGate.isUncorroboratedMusicOnly(region([.foundationModel, .acoustic])))
        #expect(!MusicOffsetLexicalGate.isUncorroboratedMusicOnly(region([.acoustic])))
    }

    @Test("shouldSuppress: a corroborated music region is NEVER suppressed, even with a cue-less onset")
    func corroboratedMusicNeverSuppressed() {
        for corro in corroboratingOrigins {
            #expect(
                !MusicOffsetLexicalGate.shouldSuppress(region([.sustainedMusic, corro]), chunks: fillerAfterEdge),
                "corroborated music (\(corro.rawValue)) must survive regardless of onset content"
            )
        }
    }

    @Test("shouldSuppress: an uncorroborated music-only region is suppressed IFF its onset has no cue")
    func uncorroboratedSuppressionTracksCue() {
        // No cue → suppress (both pure-music and music+acoustic).
        #expect(MusicOffsetLexicalGate.shouldSuppress(region([.sustainedMusic]), chunks: fillerAfterEdge))
        #expect(MusicOffsetLexicalGate.shouldSuppress(region([.sustainedMusic, .acoustic]), chunks: fillerAfterEdge))
        // Cue present → keep.
        #expect(!MusicOffsetLexicalGate.shouldSuppress(region([.sustainedMusic]), chunks: cueAfterEdge))
    }

    @Test("shouldSuppress: a non-music region is never suppressed")
    func nonMusicNeverSuppressed() {
        #expect(!MusicOffsetLexicalGate.shouldSuppress(region([.lexical]), chunks: fillerAfterEdge))
        #expect(!MusicOffsetLexicalGate.shouldSuppress(region([.foundationModel]), chunks: fillerAfterEdge))
        #expect(!MusicOffsetLexicalGate.shouldSuppress(region([.acoustic]), chunks: fillerAfterEdge))
    }

    @Test("filter drops ONLY the cue-less music-only regions and preserves input order")
    func filterDropsOnlyFlaggedPreservingOrder() {
        let regions = [
            region([.sustainedMusic], endTime: 75.0),                 // cue-less music-only → DROP
            region([.lexical], endTime: 75.0),                        // non-music → keep
            region([.sustainedMusic, .foundationModel], endTime: 75.0), // corroborated → keep
            region([.sustainedMusic, .acoustic], endTime: 75.0),      // cue-less music+acoustic → DROP
        ]
        let kept = MusicOffsetLexicalGate.filter(regions, chunks: fillerAfterEdge)
        // Order preserved; only the two flagged regions removed.
        #expect(kept.count == 2)
        #expect(kept[0].origins == [.lexical])
        #expect(kept[1].origins == [.sustainedMusic, .foundationModel])
    }
}

@Suite("MusicOffsetLexicalGate — seam integration (playhead-eki3)")
struct MusicOffsetLexicalGateSeamIntegrationTests {

    private let assetId = "eki3-integration"
    private let episodeDuration = 90.0
    private let chunkDuration = 3.0

    /// 30 contiguous 3s chunks over [0,90). `adCopyChunks` maps a chunk index to
    /// bespoke text (used to plant a sponsor read after the music edge); every
    /// other chunk is ad-free filler.
    private func makeChunks(adCopyChunks: [Int: String] = [:]) -> [TranscriptChunk] {
        let count = Int(episodeDuration / chunkDuration)  // 30
        return (0..<count).map { idx in
            let start = Double(idx) * chunkDuration
            let text = adCopyChunks[idx]
                ?? "Segment \(idx) of ordinary spoken conversation about coastal tide pools and slow patient observation."
            return TranscriptChunk(
                id: "c\(idx)-\(assetId)", analysisAssetId: assetId,
                segmentFingerprint: "fp-\(idx)", chunkIndex: idx,
                startTime: start, endTime: start + chunkDuration,
                text: text, normalizedText: text.lowercased(),
                pass: "final", modelVersion: "v", transcriptVersion: nil, atomOrdinal: nil
            )
        }
    }

    /// Flat features with a sustained high-`musicProbability` play-out in
    /// [60, 74) — the acoustic break detector finds nothing, so the ONLY
    /// proposal source is the music proposer.
    private func makeFeatureWindows() -> [FeatureWindow] {
        var windows: [FeatureWindow] = []
        var t: Double = 0
        while t < episodeDuration {
            let inMusicRun = t >= 60 && t < 74
            windows.append(FeatureWindow(
                analysisAssetId: assetId, startTime: t, endTime: t + 2.0,
                rms: 0.3, spectralFlux: 0.05,
                musicProbability: inMusicRun ? 0.9 : 0.0,
                pauseProbability: 0.0, speakerClusterId: 1, jingleHash: nil, featureVersion: 5
            ))
            t += 2.0
        }
        return windows
    }

    private func makeInput(
        gateEnabled: Bool,
        adCopyChunks: [Int: String] = [:],
        lexicalCandidates: [LexicalCandidate] = []
    ) -> RegionShadowPhase.Input {
        RegionShadowPhase.Input(
            analysisAssetId: assetId,
            chunks: makeChunks(adCopyChunks: adCopyChunks),
            lexicalCandidates: lexicalCandidates,
            featureWindows: makeFeatureWindows(),
            episodeDuration: episodeDuration,
            priors: ShowPriors.from(profile: nil),
            podcastProfile: nil,
            fmWindows: [],
            sustainedMusicProposerEnabled: true,     // proposer ON for every case here
            musicOffsetLexicalGateEnabled: gateEnabled
        )
    }

    private func hasMusicRegion(_ bundles: [RegionFeatureBundle]) -> Bool {
        bundles.contains { $0.region.origins.contains(.sustainedMusic) }
    }

    // MARK: - Content at the edge: suppressed ON, survives OFF

    @Test("content-at-edge music-only span: SUPPRESSED with gate ON, SURVIVES with gate OFF")
    func contentAtEdgeIsSuppressedOnlyWhenGateOn() async throws {
        // Onset window (chunks starting >= region.endTime - 2 = 73s) is all
        // ad-free filler → no ad-cue.
        let off = try await RegionShadowPhase.run(makeInput(gateEnabled: false))
        let on = try await RegionShadowPhase.run(makeInput(gateEnabled: true))

        #expect(hasMusicRegion(off), "gate OFF must be byte-identical to today: the music banner survives")
        #expect(!hasMusicRegion(on), "gate ON must suppress the cue-less content-at-edge music banner")
    }

    // MARK: - Sponsor read at the edge: survives both

    @Test("sponsor-read-at-edge music-only span: SURVIVES with gate ON and OFF")
    func sponsorReadAtEdgeSurvivesBoth() async throws {
        // Plant a third-party ad-read in the chunks just after the music edge
        // (region.endTime ≈ 75s, so onset window starts at chunk 25 = [75,78)).
        let adCopy: [Int: String] = [
            25: "This episode is brought to you by Acme Rest. Go to acmerest.com and use code POD.",
            26: "That's acmerest.com, use code POD for twenty percent off your first order.",
        ]
        let off = try await RegionShadowPhase.run(makeInput(gateEnabled: false, adCopyChunks: adCopy))
        let on = try await RegionShadowPhase.run(makeInput(gateEnabled: true, adCopyChunks: adCopy))

        #expect(hasMusicRegion(off), "gate OFF: the music banner survives")
        #expect(hasMusicRegion(on), "gate ON: a genuine sponsor read at the edge keeps the banner alive")
    }

    // MARK: - Corroborated by lexical: unaffected

    @Test("music span corroborated by a lexical candidate is UNAFFECTED by the gate (survives ON)")
    func lexicalCorroboratedMusicIsUnaffected() async throws {
        // A lexical candidate overlapping the music run [60,74) merges with the
        // music proposal in RegionProposalBuilder.build → origins gain .lexical,
        // so the region is NOT uncorroborated-music-only. The gate must leave it
        // alone even though the onset window is ad-free filler (no cue).
        let lexical = LexicalCandidate(
            id: "lex-corr", analysisAssetId: assetId,
            startTime: 62.0, endTime: 70.0, confidence: 0.8, hitCount: 2,
            categories: [.sponsor], evidenceText: "evidence", evidenceStartTime: 62.0,
            detectorVersion: "lexical-v1"
        )
        let on = try await RegionShadowPhase.run(
            makeInput(gateEnabled: true, lexicalCandidates: [lexical])
        )
        let musicRegions = on.filter { $0.region.origins.contains(.sustainedMusic) }
        #expect(musicRegions.count == 1, "the lexical-corroborated music region must survive the gate")
        #expect(musicRegions[0].region.origins.contains(.lexical),
                "the surviving region carries the corroborating lexical origin")
    }

    // MARK: - Flag-off equivalence

    @Test("gate OFF is byte-identical to a plain proposer run (equivalence)")
    func gateOffIsByteIdentical() async throws {
        // The gate flag defaults OFF, so an explicit gate-OFF run must produce
        // exactly the region set of a default (no-gate-param) run.
        let explicitOff = try await RegionShadowPhase.run(makeInput(gateEnabled: false))
        let defaulted = try await RegionShadowPhase.run(
            RegionShadowPhase.Input(
                analysisAssetId: assetId,
                chunks: makeChunks(),
                lexicalCandidates: [],
                featureWindows: makeFeatureWindows(),
                episodeDuration: episodeDuration,
                priors: ShowPriors.from(profile: nil),
                podcastProfile: nil,
                fmWindows: [],
                sustainedMusicProposerEnabled: true
                // musicOffsetLexicalGateEnabled defaulted (false)
            )
        )
        func keys(_ bundles: [RegionFeatureBundle]) -> [String] {
            bundles
                .map { "\($0.region.firstAtomOrdinal)-\($0.region.lastAtomOrdinal)-\($0.region.origins.rawValue)" }
                .sorted()
        }
        #expect(keys(explicitOff) == keys(defaulted))
        #expect(hasMusicRegion(explicitOff), "flag OFF leaves the music banner in place")
    }
}
