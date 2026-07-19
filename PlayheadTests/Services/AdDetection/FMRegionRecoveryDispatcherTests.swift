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

// MARK: - Sliding-window sweep (playhead-vlo1, Option B)

/// Coverage for the sliding-window FM recovery sweep — the escalation of the
/// single-window Option-A dispatcher. All tests drive a SCRIPTED stub
/// `Runtime` (`ScriptedCoarseRuntime`) that returns a disposition (or throws)
/// keyed on the 1-based `respondCoarse` call index, so each test asserts BOTH
/// the aggregated verdict AND the exact number of FM calls (proving the
/// admit-on-any short-circuit and the byte-identical single-window default).
/// No live model is ever touched — this proves the sweep WIRING, not real
/// recall (the recall/FP tradeoff is the deferred measurement).
@Suite("FMRegionRecoveryDispatcher — sliding-window sweep (playhead-vlo1)")
struct FMRegionRecoveryDispatcherSweepTests {

    private let assetId = "vlo1-sweep"

    private func makeChunks(count: Int) -> [TranscriptChunk] {
        (0..<count).map { idx in
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

    private func makeAtoms(count: Int) -> [TranscriptAtom] {
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: makeChunks(count: count),
            analysisAssetId: assetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
        return atoms
    }

    /// A `.sustainedMusic` region over `[atoms[1], atoms[hiIndex]]`, leaving
    /// atoms past `hiIndex` as the post-edge onset material the sweep steps
    /// through.
    private func makeRegion(atoms: [TranscriptAtom], hiIndex: Int) -> ProposedRegion {
        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        return ProposedRegion(
            analysisAssetId: assetId,
            transcriptVersion: sorted[0].atomKey.transcriptVersion,
            firstAtomOrdinal: sorted[1].atomKey.atomOrdinal,
            lastAtomOrdinal: sorted[hiIndex].atomKey.atomOrdinal,
            startTime: sorted[1].startTime,
            endTime: sorted[hiIndex].endTime,
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

    /// A fixture whose post-edge atoms are spread widely enough that the three
    /// `.sweep` windows (offsets 0 / 15 / 30 s) each select a DISTINCT atom set,
    /// so the dedup guard never collapses them — the multi-window call-count
    /// tests can therefore assert exactly K = 3 calls. Verified below.
    private func distinctSweepFixture() -> (atoms: [TranscriptAtom], region: ProposedRegion) {
        let atoms = makeAtoms(count: 12)
        // Region = [atoms[1], atoms[3]] ⇒ ~8 post-edge atoms at 10 s spacing,
        // spanning well past 3 × 15 s of forward stride.
        let region = makeRegion(atoms: atoms, hiIndex: 3)
        return (atoms, region)
    }

    // MARK: - Single-window default = byte-identical Option A

    @Test("windowCount == 1 (default) makes exactly ONE coarse call and returns the single-window verdict")
    func singleWindowIsOneCallSameVerdict() async {
        let atoms = makeAtoms(count: 6)
        let region = makeRegion(atoms: atoms, hiIndex: 4)

        let cases: [(CoarseDisposition, FMRegionVerdict)] = [
            (.containsAd, .ad),
            (.noAds, .content),
            (.uncertain, .content),
            (.abstain, .unavailable),
        ]
        for (disposition, expected) in cases {
            let scripted = ScriptedCoarseRuntime { _ in
                CoarseScreeningSchema(disposition: disposition, support: nil)
            }
            // Default init ⇒ `.single` ⇒ Option A.
            let dispatcher = LiveFMRegionRecoveryDispatcher(runtime: makeScriptedRuntime(scripted))
            let verdict = await dispatcher.classify(region: region, atoms: atoms)
            #expect(verdict == expected, "single-window \(disposition) must map to \(expected)")
            #expect(await scripted.callCount == 1, "single-window mode must make exactly one respondCoarse call")
        }
    }

    // MARK: - Admit-on-any + short-circuit

    @Test("sweep admits on the SECOND window's .containsAd (first .noAds) → .ad, and short-circuits before the third")
    func sweepAdmitsOnSecondWindowAndShortCircuits() async {
        let (atoms, region) = distinctSweepFixture()
        let scripted = ScriptedCoarseRuntime { call in
            switch call {
            case 1: return CoarseScreeningSchema(disposition: .noAds, support: nil)
            case 2: return CoarseScreeningSchema(disposition: .containsAd, support: nil)
            default: return CoarseScreeningSchema(disposition: .noAds, support: nil) // must not be reached
            }
        }
        let dispatcher = LiveFMRegionRecoveryDispatcher(
            runtime: makeScriptedRuntime(scripted),
            sweep: .sweep // windowCount 3
        )
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .ad, "admit-on-any: a later window's .containsAd restores the region")
        #expect(await scripted.callCount == 2, "the sweep must short-circuit right after the first .containsAd")
    }

    // MARK: - All-content → suppressed (every window queried)

    @Test("sweep with all-.noAds → .content (suppressed) and queries every one of the K windows")
    func sweepAllNoAdsIsContentAllWindows() async {
        let (atoms, region) = distinctSweepFixture()
        let scripted = ScriptedCoarseRuntime { _ in
            CoarseScreeningSchema(disposition: .noAds, support: nil)
        }
        let dispatcher = LiveFMRegionRecoveryDispatcher(
            runtime: makeScriptedRuntime(scripted),
            sweep: .sweep
        )
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .content, "no window said .containsAd ⇒ stay suppressed")
        #expect(await scripted.callCount == 3, "with no early admit the sweep queries all K distinct windows")
    }

    // MARK: - All-unavailable → graceful degrade

    @Test("sweep whose every window THROWS degrades to .unavailable (after querying all K)")
    func sweepAllThrowIsUnavailable() async {
        let (atoms, region) = distinctSweepFixture()
        let scripted = ScriptedCoarseRuntime { _ in throw StubCoarseError() }
        let dispatcher = LiveFMRegionRecoveryDispatcher(
            runtime: makeScriptedRuntime(scripted),
            sweep: .sweep
        )
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .unavailable, "all windows degraded ⇒ graceful .unavailable")
        #expect(await scripted.callCount == 3)
    }

    @Test("sweep whose every window ABSTAINS degrades to .unavailable")
    func sweepAllAbstainIsUnavailable() async {
        let (atoms, region) = distinctSweepFixture()
        let scripted = ScriptedCoarseRuntime { _ in
            CoarseScreeningSchema(disposition: .abstain, support: nil)
        }
        let dispatcher = LiveFMRegionRecoveryDispatcher(
            runtime: makeScriptedRuntime(scripted),
            sweep: .sweep
        )
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .unavailable, "abstain-only ⇒ .unavailable")
        #expect(await scripted.callCount == 3)
    }

    // MARK: - Mixed sweep: a degraded window must not clobber a later .content

    @Test("sweep where the first window THROWS and a later window reads .noAds → .content (a degrade does not lose a subsequent no-ad read)")
    func sweepDegradeThenContentIsContent() async {
        let (atoms, region) = distinctSweepFixture()
        let scripted = ScriptedCoarseRuntime { call in
            switch call {
            case 1: throw StubCoarseError()
            case 2: return CoarseScreeningSchema(disposition: .noAds, support: nil)
            default: return CoarseScreeningSchema(disposition: .abstain, support: nil)
            }
        }
        let dispatcher = LiveFMRegionRecoveryDispatcher(
            runtime: makeScriptedRuntime(scripted),
            sweep: .sweep
        )
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .content, "a window that read no-ads survives an earlier degraded window")
        #expect(await scripted.callCount == 3, "no early admit ⇒ every distinct window is queried")
    }

    // MARK: - Dedup: identical overlapping windows are queried at most once

    @Test("sweep dedups identical windows — a sparse region whose later windows collapse to the same atom set makes FEWER than K calls")
    func sweepDedupsIdenticalWindows() async {
        // The 6-atom fixture has ONE post-edge atom (at the edge). Window 0
        // (offset 0) selects region-tail + that atom; windows 1 and 2
        // (offsets 15 / 30 s) step past it and collapse to region-tail ONLY —
        // identical to each other. So the sweep queries 2 DISTINCT windows, not
        // 3, proving the dedup guard skips the repeat rather than re-rolling it.
        let atoms = makeAtoms(count: 6)
        let region = makeRegion(atoms: atoms, hiIndex: 4)
        let scripted = ScriptedCoarseRuntime { _ in
            CoarseScreeningSchema(disposition: .noAds, support: nil)
        }
        let dispatcher = LiveFMRegionRecoveryDispatcher(
            runtime: makeScriptedRuntime(scripted),
            sweep: .sweep
        )
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .content)
        #expect(await scripted.callCount == 2, "the two identical region-only windows must be queried only once")
    }

    // MARK: - Empty region → zero calls (unchanged)

    @Test("sweep over a region with no atoms makes ZERO coarse calls and returns .unavailable")
    func sweepEmptyRegionMakesZeroCalls() async {
        let atoms = makeAtoms(count: 6)
        // Ordinal range past the whole atom stream → no region atoms, no
        // post-edge atoms in any window.
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
        let scripted = ScriptedCoarseRuntime { _ in
            CoarseScreeningSchema(disposition: .containsAd, support: nil) // would admit if consulted
        }
        let dispatcher = LiveFMRegionRecoveryDispatcher(
            runtime: makeScriptedRuntime(scripted),
            sweep: .sweep
        )
        let verdict = await dispatcher.classify(region: region, atoms: atoms)
        #expect(verdict == .unavailable)
        #expect(await scripted.callCount == 0, "an empty region must not consult the model in any window")
    }

    // MARK: - Window geometry: stepped forward from region.endTime

    @Test("sweep windows step FORWARD from region.endTime: a later window drops the earliest post-edge atom, still reaches a later ad-read, and keeps the music tail")
    func sweepWindowsStepForwardFromEdge() {
        let atoms = makeAtoms(count: 8)
        let sorted = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        // Region = [atoms[1], atoms[2]] ⇒ atoms[3...] are post-edge onset material.
        let region = makeRegion(atoms: atoms, hiIndex: 2)
        let hi = sorted[2].atomKey.atomOrdinal
        let padLead = MusicOffsetLexicalGate.onsetWindowLeadSeconds

        let posts = sorted
            .filter { $0.atomKey.atomOrdinal > hi }
            .sorted { $0.startTime < $1.startTime }
        #expect(posts.count >= 3, "fixture must have several post-edge atoms to slide over")
        let early = posts.first!
        let late = posts.last!
        #expect(early.startTime < late.startTime)

        // Window at the edge (offset 0 — the single-window / Option-A window).
        let window0 = LiveFMRegionRecoveryDispatcher.regionSegments(
            region: region,
            atoms: atoms,
            padLeadSeconds: padLead,
            forwardOffsetSeconds: 0
        )
        // A forward-stepped window whose onset start lands mid-way through the
        // post-edge material: it must exclude the earliest post-edge atom but
        // still include the latest. Derived from actual atom times so the test
        // is robust to the atomizer's exact timing.
        let target = (early.startTime + late.startTime) / 2
        let offsetLate = target - (region.endTime - padLead)
        #expect(offsetLate > 0, "the later window must step strictly forward")
        let windowLate = LiveFMRegionRecoveryDispatcher.regionSegments(
            region: region,
            atoms: atoms,
            padLeadSeconds: padLead,
            forwardOffsetSeconds: offsetLate
        )

        let ords0 = Set(window0.flatMap { $0.atoms }.map { $0.atomKey.atomOrdinal })
        let ordsLate = Set(windowLate.flatMap { $0.atoms }.map { $0.atomKey.atomOrdinal })

        #expect(ords0.contains(early.atomKey.atomOrdinal), "the edge window includes the earliest post-edge ad-read")
        #expect(
            !ordsLate.contains(early.atomKey.atomOrdinal),
            "a forward-stepped window drops the earliest post-edge atom"
        )
        #expect(ordsLate.contains(late.atomKey.atomOrdinal), "the forward-stepped window still reaches a later ad-read")
        #expect(
            ords0.contains(hi) && ordsLate.contains(hi),
            "the music-tail region atoms are shared boundary context in every window"
        )
    }
}

// MARK: - Test helpers (file-scoped)

private struct StubCoarseError: Error {}

/// Records each `respondCoarse` call and returns a scripted disposition (or
/// throws) keyed on the 1-based call index. Lets the sweep tests assert both
/// the aggregated verdict AND the exact number of FM calls (short-circuit).
private actor ScriptedCoarseRuntime {
    private(set) var callCount = 0
    private let script: @Sendable (_ callIndex: Int) throws -> CoarseScreeningSchema

    init(_ script: @escaping @Sendable (_ callIndex: Int) throws -> CoarseScreeningSchema) {
        self.script = script
    }

    func next() throws -> CoarseScreeningSchema {
        callCount += 1
        return try script(callCount)
    }
}

/// A `Runtime` whose every fresh session routes `respondCoarse` through the
/// shared `ScriptedCoarseRuntime` (so counts accumulate across the sweep's
/// per-window sessions). Mirrors `makeStubCoarseRuntime`.
private func makeScriptedRuntime(_ scripted: ScriptedCoarseRuntime) -> FoundationModelClassifier.Runtime {
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
                respondCoarse: { _ in try await scripted.next() },
                respondRefinement: { _ in RefinementWindowSchema(spans: []) }
            )
        }
    )
}

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
