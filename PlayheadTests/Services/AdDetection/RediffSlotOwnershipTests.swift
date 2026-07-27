// RediffSlotOwnershipTests.swift
// playhead-xsdz.29: offline coverage for the rediff width-oracle integration
// engine — the double-gate (version + sourceAudioIdentity), the alignedFraction
// re-encode guard, fragment-merge + duration-cap, per-span synthesis + the
// coreCoverage gate, and the full flow through the REUSED-UNCHANGED
// `SpliceSlotDispositionEngine` → `SpliceSlotRewriter(provenance: .rediffSlot)`
// → the REUSED shadow row builder. Also pins the rediff-SOLE-SETTER contract:
// a span with no overlapping rediff slot falls to STATUS-QUO width (never the
// acoustic resolver).

import Foundation
import Testing
@testable import Playhead

@Suite("RediffSlotOwnership (playhead-xsdz.29 offline oracle)")
struct RediffSlotOwnershipTests {

    // MARK: - Deterministic white-noise PCM (varied, section-distinct fingerprints)

    private struct Noise {
        var state: UInt64
        init(seed: UInt64) { state = seed }
        mutating func next() -> UInt64 {
            state &+= 0x9E37_79B9_7F4A_7C15
            var z = state
            z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
            z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
            return z ^ (z >> 31)
        }
        mutating func sample() -> Float {
            // Uniform in [-1, 1).
            Float(Int64(bitPattern: next()) % 2_000_000) / 1_000_000.0
        }
    }

    private static func noisePCM(seconds: Double, seed: UInt64) -> [Float] {
        let n = Int(seconds * 16_000)  // 16 kHz analysis rate
        var rng = Noise(seed: seed)
        return (0..<n).map { _ in rng.sample() }
    }

    private static let currentVersion = ChromaFingerprinter.algorithmVersion
    private static let secPerFp = ChromaFingerprinter.secondsPerFingerprint

    private static func record(
        fingerprints: [UInt32],
        version: UInt32 = currentVersion,
        identity: String = "audio-1"
    ) -> EpisodeFingerprintRecord {
        EpisodeFingerprintRecord(
            analysisAssetId: "asset-1",
            algorithmVersion: version,
            secondsPerFingerprint: secPerFp,
            fingerprints: fingerprints,
            sourceAudioIdentity: identity,
            capturedAt: 0
        )
    }

    // MARK: - Double-gate: version

    @Test("stale algorithmVersion is rejected before any diff")
    func rejectsStaleVersion() {
        let stored = Self.record(fingerprints: [1, 2, 3], version: Self.currentVersion &+ 7)
        let outcome = RediffSlotOwnership.gateAndDiff(
            storedASide: stored,
            refetchedBSideSamples16kHz: Self.noisePCM(seconds: 1, seed: 1),
            currentAssetFingerprint: "audio-1"
        )
        #expect(outcome == .rejectedStaleVersion(stored: Self.currentVersion &+ 7, current: Self.currentVersion))
    }

    // MARK: - Double-gate: sourceAudioIdentity (the key adversarial case)

    @Test("version-matching but audio-MISMATCHED stored stream is NOT trusted")
    func rejectsAudioIdentityMismatch() {
        // The stored stream is the CURRENT version but was captured for DIFFERENT
        // audio (assetId reused for a re-download). It must be rejected even
        // though the version gate — which the store already enforces — passes.
        let stored = Self.record(fingerprints: [1, 2, 3], identity: "OLD-audio")
        let outcome = RediffSlotOwnership.gateAndDiff(
            storedASide: stored,
            refetchedBSideSamples16kHz: Self.noisePCM(seconds: 1, seed: 2),
            currentAssetFingerprint: "NEW-audio"
        )
        #expect(outcome == .rejectedAudioIdentityMismatch(stored: "OLD-audio", current: "NEW-audio"))
    }

    @Test("version gate is checked BEFORE the identity gate")
    func versionGatePrecedesIdentityGate() {
        // Both gates fail; the version reason must win (fired first).
        let stored = Self.record(fingerprints: [1], version: Self.currentVersion &+ 1, identity: "OLD")
        let outcome = RediffSlotOwnership.gateAndDiff(
            storedASide: stored,
            refetchedBSideSamples16kHz: Self.noisePCM(seconds: 1, seed: 3),
            currentAssetFingerprint: "NEW"
        )
        if case .rejectedStaleVersion = outcome {} else {
            Issue.record("expected version rejection to win, got \(outcome)")
        }
    }

    // MARK: - alignedFraction re-encode guard

    @Test("identical A/B passes both gates and the re-encode guard (near-full alignment, no slots)")
    func identicalCopiesAcceptedNoSlots() {
        let pcm = Self.noisePCM(seconds: 16, seed: 100)
        let fpA = EpisodeFingerprintCapture.fingerprints(mono16kHz: pcm)
        #expect(fpA.count > 40, "sanity: enough fingerprints to align")
        let stored = Self.record(fingerprints: fpA, identity: "audio-1")

        let outcome = RediffSlotOwnership.gateAndDiff(
            storedASide: stored,
            refetchedBSideSamples16kHz: pcm,
            currentAssetFingerprint: "audio-1"
        )
        guard case .accepted(let acc) = outcome else {
            Issue.record("expected accepted, got \(outcome)"); return
        }
        #expect(acc.alignedFractionB >= 0.99, "identical copies must align almost fully")
        #expect(acc.playedSlots.isEmpty, "identical copies have no removed A segment → no slots")
    }

    @Test("non-aligning stored A (re-encode/garbage) trips the alignedFraction guard")
    func lowAlignmentRejected() {
        let pcm = Self.noisePCM(seconds: 16, seed: 101)
        let fpB = EpisodeFingerprintCapture.fingerprints(mono16kHz: pcm)
        // A totally unrelated fingerprint stream of comparable length: it will
        // not form long runs against B → alignedFractionB ≈ 0.
        var rng = Noise(seed: 999)
        let fpAlien = (0..<fpB.count).map { _ in UInt32(truncatingIfNeeded: rng.next()) }
        let stored = Self.record(fingerprints: fpAlien, identity: "audio-1")

        let outcome = RediffSlotOwnership.gateAndDiff(
            storedASide: stored,
            refetchedBSideSamples16kHz: pcm,
            currentAssetFingerprint: "audio-1"
        )
        guard case .rejectedLowAlignedFraction(let frac) = outcome else {
            Issue.record("expected low-alignment rejection, got \(outcome)"); return
        }
        #expect(frac < 0.5)
    }

    @Test("A with an extra inserted segment yields a played-timeline slot (full pipeline)")
    func insertedSegmentProducesPlayedSlot() {
        // A (played) = content1 + adX + content2; B (fresh) = content1 + content2
        // (the played ad was rotated out). adX is REMOVED from A relative to B →
        // it surfaces as a slotA in the PLAYED timeline. Proves gate → B
        // fingerprinting via the xsdz.27 extractor → differ → guard → cleaned
        // slots end-to-end on real PCM. Geometry is asserted loosely (framing at
        // the splice); exact indices are pinned by the differ's own tests.
        let content1 = Self.noisePCM(seconds: 5, seed: 200)
        let adX = Self.noisePCM(seconds: 10, seed: 201)
        let content2 = Self.noisePCM(seconds: 5, seed: 202)
        let aPCM = content1 + adX + content2
        let bPCM = content1 + content2
        let fpA = EpisodeFingerprintCapture.fingerprints(mono16kHz: aPCM)
        let stored = Self.record(fingerprints: fpA, identity: "audio-1")

        let outcome = RediffSlotOwnership.gateAndDiff(
            storedASide: stored,
            refetchedBSideSamples16kHz: bPCM,
            currentAssetFingerprint: "audio-1"
        )
        guard case .accepted(let acc) = outcome else {
            Issue.record("expected accepted, got \(outcome)"); return
        }
        #expect(acc.alignedFractionB >= 0.5, "B (all shared content) should align well")
        #expect(!acc.playedSlots.isEmpty, "the removed adX must surface as a played slot")
        // adX occupies roughly [5s, 15s] in the played timeline. The dominant
        // slot should cover most of that band (generous framing tolerance).
        let dominant = acc.playedSlots.max { $0.durationSeconds < $1.durationSeconds }!
        #expect(dominant.startSeconds >= 2 && dominant.startSeconds <= 8,
                "slot start near adX onset (~5s), got \(dominant.startSeconds)")
        #expect(dominant.endSeconds >= 12 && dominant.endSeconds <= 18,
                "slot end near adX offset (~15s), got \(dominant.endSeconds)")
    }

    // MARK: - cleanedPlayedSlots: fragment merge + duration cap

    private static func differSlot(
        start: Double, end: Double, left: Double = 60, right: Double = 60
    ) -> RediffDiffer.Slot {
        RediffDiffer.Slot(
            startFp: 0, endFp: 0, leftRunFps: 0, rightRunFps: 0,
            startSeconds: start, endSeconds: end, durationSeconds: end - start,
            confidence: 0, leftRunSeconds: left, rightRunSeconds: right
        )
    }

    @Test("fragments ≤3s apart merge into one slot carrying the OUTER flanks")
    func fragmentMergeJoinsNearbySlots() {
        let slots = [
            Self.differSlot(start: 10, end: 20, left: 40, right: 5),
            Self.differSlot(start: 22, end: 30, left: 5, right: 70),   // 2s gap → merge
        ]
        let cleaned = RediffSlotOwnership.cleanedPlayedSlots(from: slots)
        #expect(cleaned.count == 1)
        #expect(cleaned[0].startSeconds == 10 && cleaned[0].endSeconds == 30)
        // Outer flanks: left from the first, right from the last.
        #expect(cleaned[0].leftRunSeconds == 40 && cleaned[0].rightRunSeconds == 70)
    }

    @Test("fragments >3s apart stay separate")
    func fragmentsFarApartStaySeparate() {
        let slots = [
            Self.differSlot(start: 10, end: 20),
            Self.differSlot(start: 24, end: 30),   // 4s gap → no merge
        ]
        let cleaned = RediffSlotOwnership.cleanedPlayedSlots(from: slots)
        #expect(cleaned.count == 2)
    }

    @Test("a slot longer than the 8-minute cap is dropped as an alignment breakdown")
    func durationCapDropsGiantSlot() {
        let slots = [
            Self.differSlot(start: 10, end: 40),          // 30s — keep
            Self.differSlot(start: 100, end: 100 + 600),  // 10min — drop
        ]
        let cleaned = RediffSlotOwnership.cleanedPlayedSlots(from: slots)
        #expect(cleaned.count == 1)
        #expect(cleaned[0].endSeconds == 40)
    }

    @Test("empty differ slots → empty played slots")
    func emptySlots() {
        #expect(RediffSlotOwnership.cleanedPlayedSlots(from: []).isEmpty)
    }

    // MARK: - k-way union (playhead-xsdz.36.2)

    @Test("unionedPlayedSlots: a single list (K=1) is returned UNCHANGED — the exact single-fetch behavior")
    func unionSingleListUnchanged() {
        let single = [Self.played(100, 160, left: 40, right: 50), Self.played(300, 340)]
        #expect(RediffSlotOwnership.unionedPlayedSlots([single]) == single)
        #expect(RediffSlotOwnership.unionedPlayedSlots([]).isEmpty)
        #expect(RediffSlotOwnership.unionedPlayedSlots([[]]).isEmpty)
    }

    @Test("unionedPlayedSlots recovers a pod one fetch-pair misses (B vs C misses / B+C+D recovers)")
    func unionRecoversCollisionMissedPod() {
        // Three ad pods exist in the played copy. Fetch B reveals pods 1 & 3 but
        // its stitch COLLIDED with A on pod 2 (identical fill → no byte
        // divergence → pod 2 MISSED). Fetch C (a distinct persona) reveals pod 2;
        // fetch D reveals pods 1 & 2. UNIONing the pairwise diffs recovers all
        // three — the collision-missed pod 2 included.
        let pod1 = Self.played(60, 90)
        let pod2 = Self.played(600, 640)
        let pod3 = Self.played(1800, 1830)
        let bDiff = [pod1, pod3]   // A vs B: pod2 collided → MISSED
        let cDiff = [pod2]         // A vs C: only pod2 diverged
        let dDiff = [pod1, pod2]   // A vs D

        // A single pair (B alone) MISSES pod 2.
        #expect(!RediffSlotOwnership.unionedPlayedSlots([bDiff]).contains { $0.startSeconds == 600 },
                "the single B-vs-A diff misses the low-entropy collision pod")

        // The union across the fetch set RECOVERS it — 100% of the pods.
        let union = RediffSlotOwnership.unionedPlayedSlots([bDiff, cDiff, dDiff])
        let intervals = union.map { ($0.startSeconds, $0.endSeconds) }
        #expect(union.count == 3, "all three pods recovered, got \(intervals)")
        #expect(intervals.contains { $0 == (60, 90) })
        #expect(intervals.contains { $0 == (600, 640) },
                "the collision-missed pod is recovered from another persona's divergence")
        #expect(intervals.contains { $0 == (1800, 1830) })
    }

    @Test("unionedPlayedSlots collapses overlapping detections of the SAME pod to one slot")
    func unionCollapsesOverlappingSamePod() {
        // Two personas detect the same pod at slightly different byte edges; the
        // union must merge them (not duplicate), widening to the outer edges.
        let b = [Self.played(100, 160, left: 200, right: 5)]
        let c = [Self.played(99, 161, left: 5, right: 300)]
        let union = RediffSlotOwnership.unionedPlayedSlots([b, c])
        #expect(union.count == 1, "same-pod overlap merges, not duplicated")
        #expect(union[0].startSeconds == 99 && union[0].endSeconds == 161)
    }

    // MARK: - day-0 mint UNION semantics (playhead-xsdz.36.4 / playhead-wybg)
    //
    // The day-0 byte-exact mint UNIONs the per-persona slot lists via
    // `unionedPlayedSlots` — quorum = 1: a slot mints if ANY persona's byte-exact
    // diff reveals it (the lagged-path primitive). A ≥2-AGREEMENT quorum
    // (`kWayRobustPlayedSlots`) was tried and REMOVED (playhead-wybg): a minutes-
    // apart measurement showed that on client-PINNED shows (Conan/AdsWizz) only
    // ONE persona diverges while the same-persona re-fetch COLLIDES (byte-
    // identical), so requiring cross-persona agreement dropped real ads and
    // defeated k-way collision recovery. These tests pin the pinned-show recovery
    // over the EXACT `perBSideSlots` shape `mintByteExactDayZeroMarks` builds (one
    // list per persona whose B passed `gateAndDiffBytes`; a collision is an
    // accepted-but-EMPTY list, a gate-reject contributes no list at all).

    @Test("day-0 union: a SINGLE diverging persona among collisions/gate-rejects still mints its slots (the Conan/AdsWizz pinned-show case)")
    func dayZeroUnionSingleDivergingPersonaMints() {
        // Client-pinned show: the same-persona re-fetch COLLIDES with A (byte-
        // identical → gate-accepted with ZERO divergent slots → an EMPTY list); a
        // gate-REJECTED persona (re-encode CDN) contributes NO list at all. Only
        // ONE persona (Overcast) diverges and reveals the real pods. Union mints
        // exactly those — NOT nothing. NON-tautological: the removed ≥2-agreement
        // quorum returned EMPTY here (the diverging pods have single-persona support).
        let collision: [RediffSlotOwnership.PlayedSlot] = []            // same-persona: byte-identical
        let overcast = [Self.played(0, 31, left: 8, right: 400),       // dynamic pre-roll
                        Self.played(1500, 1680, left: 400, right: 8)]  // mid-roll pod
        // The gate-rejected persona simply isn't in the list.
        let minted = RediffSlotOwnership.unionedPlayedSlots([collision, overcast])
        let intervals = minted.map { ($0.startSeconds, $0.endSeconds) }
        #expect(minted.count == 2, "the lone diverging persona's pods mint despite the collision, got \(intervals)")
        #expect(intervals.contains { $0 == (0, 31) }, "dynamic pre-roll minted")
        #expect(intervals.contains { $0 == (1500, 1680) }, "mid-roll pod minted")
    }

    @Test("day-0 union: ALL personas collide (every list empty) → nothing minted")
    func dayZeroUnionAllCollideMintsNothing() {
        // Every re-fetch landed the same stitch as A (byte-identical → empty
        // lists). No divergence anywhere → the union is empty → the mint returns 0.
        #expect(RediffSlotOwnership.unionedPlayedSlots([[], [], []]).isEmpty)
    }

    // MARK: - resolveSpan / synthesizeSlot + coreCoverage gate

    private static func played(_ start: Double, _ end: Double, left: Double = 120, right: Double = 120) -> RediffSlotOwnership.PlayedSlot {
        RediffSlotOwnership.PlayedSlot(startSeconds: start, endSeconds: end, leftRunSeconds: left, rightRunSeconds: right)
    }

    @Test("degenerate core → no slot, .degenerateCore")
    func resolveDegenerateCore() {
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 10, end: 10), playedSlots: [Self.played(5, 40)])
        #expect(slot == nil)
        #expect(diag.failureReason == .degenerateCore)
        #expect(diag.bestGeometryValidPair == nil)
    }

    @Test("no overlapping rediff slot → no slot, .noCandidatePairs (status-quo width)")
    func resolveNoOverlap() {
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 200, end: 210), playedSlots: [Self.played(5, 40)])
        #expect(slot == nil)
        #expect(diag.failureReason == .noCandidatePairs)
        #expect(diag.bestGeometryValidPair == nil)
    }

    @Test("overlap but core sticks out (coverage < 0.8) → no slot, .coreCoverageBelowMinimum")
    func resolveCoverageBelowMinimum() {
        // Core [10,100] len 90; slot [8,40] overlap 30 → coverage 0.33 < 0.8.
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 10, end: 100), playedSlots: [Self.played(8, 40)])
        #expect(slot == nil)
        #expect(diag.failureReason == .coreCoverageBelowMinimum)
        #expect(diag.bestGeometryValidPair != nil, "the below-coverage slot is surfaced for shadow visibility")
    }

    @Test("undersized core fully inside a wide rediff slot → qualifies (coverage ≈ 1.0), edges carry flank confidence")
    func resolveQualifyingUndersizedCore() {
        // Core [12,20] len 8 fully inside slot [8,40] → coverage 1.0.
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 12, end: 20), playedSlots: [Self.played(8, 40, left: 120, right: 30)])
        guard let s = slot else { Issue.record("expected a qualifying slot"); return }
        #expect(diag.failureReason == nil)
        #expect(s.startTime == 8 && s.endTime == 40)
        #expect(abs(s.coreCoverage - 1.0) < 1e-9)
        // Edge stepScores = 1 - exp(-flank/60).
        #expect(abs(s.startEdge.stepScore - (1 - exp(-120.0 / 60))) < 1e-9)
        #expect(abs(s.endEdge.stepScore - (1 - exp(-30.0 / 60))) < 1e-9)
        #expect(s.slotConfidence == min(s.startEdge.stepScore, s.endEdge.stepScore))
    }

    @Test("best-overlapping slot wins when several overlap the core")
    func resolvePicksMaxOverlap() {
        // Core [10,30]; slotA [9,15] overlap 5; slotB [12,32] overlap 18 → B wins.
        let (slot, _) = RediffSlotOwnership.resolveSpan(
            core: TimeRange(start: 10, end: 30),
            playedSlots: [Self.played(9, 15), Self.played(12, 32)])
        #expect(slot?.startTime == 12 && slot?.endTime == 32)
    }

    // MARK: - FIX A (playhead-xsdz.58): byte-slot containment clip

    @Test("FIX A: over-anchored core fully containing a sole byte slot → clips to the slot (conan [0,88.32]→[0,60.369])")
    func resolveContainedOverrunClipsToSlot() {
        // Conan repro: FM over-anchors the intro to [0,88.32]; the byte differ
        // held the true DAI ad [0,60.369]. coverage = 60.369/88.32 = 0.684 < 0.8,
        // but the slot is CONTAINED (0>=0 && 60.369<=88.32) and is the SOLE
        // overlapping slot → the coverage gate is bypassed and the span clips.
        let core = TimeRange(start: 0, end: 88.32)
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: core, playedSlots: [Self.played(0, 60.369, left: 300, right: 45)])
        guard let s = slot else { Issue.record("FIX A: expected the contained byte slot to qualify"); return }
        #expect(diag.failureReason == nil, "the contained clip QUALIFIES — no rejection")
        #expect(s.startTime == 0 && s.endTime == 60.369, "span clips to the byte-exact slot extent")
        // The slot's recorded coverage is still the true (sub-threshold) value —
        // FIX A bypasses the GATE, it does not fabricate coverage.
        #expect(abs(s.coreCoverage - (60.369 / 88.32)) < 1e-9)
        #expect(s.coreCoverage < 0.8, "coverage really is below the gate; FIX A is why it survived")
    }

    @Test("FIX A guard (over-fire): a slot that POKES OUT of the core stays .coreCoverageBelowMinimum")
    func resolveContainedGuardSlotPokesOut() {
        // Same over-anchored core, but the slot [40,100] runs PAST the core end
        // (100 > 88.32) — a genuine mis-association, NOT a containment. FIX A must
        // NOT fire: the existing coverage rejection stands unchanged.
        let core = TimeRange(start: 0, end: 88.32)
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: core, playedSlots: [Self.played(40, 100)])
        #expect(slot == nil, "a poke-out slot must NOT clip")
        #expect(diag.failureReason == .coreCoverageBelowMinimum)
        #expect(diag.bestGeometryValidPair != nil)
    }

    @Test("FIX A guard (baked-in-ad pin): a core with NO overlapping byte slot stays status-quo, untouched")
    func resolveNoByteSlotStaysStatusQuo() {
        // casefile-style baked-in / host-read ad [6.7,77.1] the byte differ CANNOT
        // see. Even with a real (distant) mid-roll byte slot present in the pass,
        // this core has NO overlapping slot → .noCandidatePairs → status-quo width.
        // This is the "don't break baked-in-ad shows" pin: FIX A NEVER fires here.
        let core = TimeRange(start: 6.7, end: 77.1)
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: core, playedSlots: [Self.played(150, 210)])
        #expect(slot == nil, "no byte slot ⇒ FIX A never fires; span stays as decoded")
        #expect(diag.failureReason == .noCandidatePairs)
        #expect(diag.bestGeometryValidPair == nil)
    }

    @Test("FIX A multi-slot: a core containing TWO byte slots does NOT clip → status-quo (neither ad dropped)")
    func resolveMultipleContainedSlotsStayStatusQuo() {
        // Core [0,200] strictly contains byte slots [10,60] and [100,160]. Clipping
        // to a single slot would DROP the other byte-confirmed ad, and the
        // one-slot-per-span machinery cannot split. So FIX A DELIBERATELY does not
        // fire: the span falls to status-quo (its full minted width, which still
        // ENCLOSES both slots → nothing dropped). The full-flow test
        // `multiContainedSlotsKeepFullWidthNeitherDropped` proves the enclosure.
        let core = TimeRange(start: 0, end: 200)
        let (slot, diag) = RediffSlotOwnership.resolveSpan(
            core: core, playedSlots: [Self.played(10, 60), Self.played(100, 160)])
        #expect(slot == nil, "2+ contained slots ⇒ no single-slot clip")
        #expect(diag.failureReason == .coreCoverageBelowMinimum)
    }

    // MARK: - candidates: index alignment + rediff-sole-setter

    private static func atom(_ ordinal: Int, _ start: Double, _ end: Double) -> AtomEvidence {
        AtomEvidence(
            atomOrdinal: ordinal, startTime: start, endTime: end,
            isAnchored: true, anchorProvenance: [], hasAcousticBreakHint: false, correctionMask: .none)
    }

    private static func span(_ id: String, _ start: Double, _ end: Double, _ first: Int, _ last: Int) -> DecodedSpan {
        DecodedSpan(
            id: id, assetId: "asset-1", firstAtomOrdinal: first, lastAtomOrdinal: last,
            startTime: start, endTime: end, anchorProvenance: [])
    }

    @Test("candidates are index-aligned; a non-covered span gets slot=nil (status-quo, not acoustic)")
    func candidatesRediffSoleSetter() {
        let spans = [
            Self.span("s0", 12, 20, 1, 2),   // inside a rediff slot
            Self.span("s1", 200, 210, 9, 10), // NO overlapping rediff slot
        ]
        let atoms = [Self.atom(1, 12, 16), Self.atom(2, 16, 20), Self.atom(9, 200, 205), Self.atom(10, 205, 210)]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans,
            atomEvidence: atoms,
            playedSlots: [Self.played(8, 40)],
            coreBankMatch: [false, false],
            slotBankMatch: [false, false]
        )
        #expect(bundle.candidates.count == 2)
        #expect(bundle.synthesizedSlots[0] != nil, "covered span gets a rediff slot")
        #expect(bundle.synthesizedSlots[1] == nil, "non-covered span falls to status-quo width")
        #expect(bundle.diagnostics[1].failureReason == .noCandidatePairs)
        // slotIntersectsAtoms is true for the covered span (its slot covers atoms).
        #expect(bundle.candidates[0].slotIntersectsAtoms)
    }

    @Test("bank verdict tables pass through to the candidates")
    func candidatesCarryBankVerdicts() {
        let spans = [Self.span("s0", 12, 20, 1, 2)]
        let atoms = [Self.atom(1, 12, 16), Self.atom(2, 16, 20)]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans, atomEvidence: atoms,
            playedSlots: [Self.played(8, 40)],
            coreBankMatch: [true], slotBankMatch: [true])
        #expect(bundle.candidates[0].coreBankMatch)
        #expect(bundle.candidates[0].slotBankMatch)
    }

    // MARK: - Full flow: dispositions → rewrite with .rediffSlot provenance

    @Test("rediff slots flow through the disposition engine to a .rediffSlot width rewrite")
    func fullFlowRewritesWidthWithRediffProvenance() {
        let spans = [
            Self.span("s0", 12, 20, 1, 2),    // widened by rediff slot [8,40]
            Self.span("s1", 200, 210, 9, 10), // status-quo (no rediff slot)
        ]
        let atoms = [
            Self.atom(1, 9, 15), Self.atom(2, 30, 39),        // atoms across [8,40]
            Self.atom(9, 200, 205), Self.atom(10, 205, 210),
        ]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans, atomEvidence: atoms,
            playedSlots: [Self.played(8, 40)],
            coreBankMatch: [false, false], slotBankMatch: [false, false])
        let result = SpliceSlotDispositionEngine.computeDispositions(bundle.candidates)

        // s0 keeps its slot; s1 has no slot.
        guard case .keepSlot = result.dispositions[0] else {
            Issue.record("expected s0 to keep its rediff slot, got \(result.dispositions[0])"); return
        }
        #expect(result.dispositions[1] == .noSlot)

        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: spans,
            dispositions: result.dispositions,
            atomEvidence: atoms,
            provenance: .rediffSlot)

        // s0 rewritten to the slot interval, carrying .rediffSlot (NOT .spliceSlot).
        let rewritten0 = rewrite.finalSpans.first { $0.startTime == 8 && $0.endTime == 40 }
        #expect(rewritten0 != nil, "s0 width rewritten to the rediff slot [8,40]")
        #expect(rewritten0?.anchorProvenance.contains(.rediffSlot) == true)
        #expect(rewritten0?.anchorProvenance.contains(.spliceSlot) == false,
                "rediff path must NOT stamp the acoustic .spliceSlot marker")

        // s1 carried through verbatim (status-quo), no provenance added.
        let unchanged1 = rewrite.finalSpans.first { $0.id == "s1" }
        #expect(unchanged1 != nil)
        #expect(unchanged1?.anchorProvenance.isEmpty == true)
        #expect(unchanged1?.startTime == 200 && unchanged1?.endTime == 210)
    }

    @Test("FIX A integration: over-anchored [0,88.32] + byte slot [0,60.369] → single ad [0,60.369], nothing in (60.4,100), mid-roll byte-exact")
    func containedOverrunClipsToSingleAdWindowEndToEnd() {
        // s0: FM over-anchored the conan intro to [0,88.32]; the byte slot is the
        // true DAI ad [0,60.369]. s1: a byte-exact mid-roll [100,160] (coverage
        // 1.0 → ordinary qualify) — the control that FIX A leaves mid-rolls alone.
        let spans = [
            Self.span("s0", 0, 88.32, 1, 3),
            Self.span("s1", 100, 160, 9, 10),
        ]
        let atoms = [
            Self.atom(1, 0, 30), Self.atom(2, 30, 58),   // inside the clipped [0,60.369]
            Self.atom(3, 62, 85),                        // FM over-anchor TAIL, outside the clip
            Self.atom(9, 100, 130), Self.atom(10, 130, 158),
        ]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans, atomEvidence: atoms,
            playedSlots: [Self.played(0, 60.369), Self.played(100, 160)],
            coreBankMatch: [false, false], slotBankMatch: [false, false])
        let result = SpliceSlotDispositionEngine.computeDispositions(bundle.candidates)
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: spans, dispositions: result.dispositions,
            atomEvidence: atoms, provenance: .rediffSlot)

        // Exactly two ad windows: the clipped intro ad and the untouched mid-roll.
        #expect(rewrite.finalSpans.count == 2)
        // s0 clipped to the byte-exact slot [0,60.369] (NOT the [0,88.32] overrun).
        let clipped = rewrite.finalSpans.first { $0.startTime == 0 }
        #expect(clipped?.endTime == 60.369, "clips at the real ad boundary, not the FM overrun")
        #expect(clipped?.anchorProvenance.contains(.rediffSlot) == true)
        // NO ad window remains in (60.369, 100) — the vacated FM over-anchor tail.
        for span in rewrite.finalSpans {
            let overlapsTail = span.startTime < 100 && span.endTime > 60.369
            #expect(!overlapsTail || span.startTime == 0,
                    "no span (other than the clip itself) survives in the vacated tail, got [\(span.startTime),\(span.endTime)]")
        }
        // Mid-roll width is byte-exact and unchanged.
        let midroll = rewrite.finalSpans.first { $0.startTime == 100 }
        #expect(midroll?.endTime == 160, "mid-roll width unchanged")
    }

    @Test("FIX A multi-slot integration: a core containing TWO byte slots keeps full width — neither byte-confirmed ad dropped")
    func multiContainedSlotsKeepFullWidthNeitherDropped() {
        // Core [0,200] strictly contains byte slots [10,60] and [100,160]. FIX A
        // does not clip (would drop one). Status-quo keeps the full minted width,
        // which ENCLOSES both slots → both ad regions remain covered.
        let spans = [Self.span("s0", 0, 200, 1, 2)]
        let atoms = [Self.atom(1, 10, 60), Self.atom(2, 100, 160)]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans, atomEvidence: atoms,
            playedSlots: [Self.played(10, 60), Self.played(100, 160)],
            coreBankMatch: [false], slotBankMatch: [false])
        #expect(bundle.synthesizedSlots[0] == nil, "no single-slot clip for a multi-slot core")
        #expect(bundle.diagnostics[0].failureReason == .coreCoverageBelowMinimum)

        let result = SpliceSlotDispositionEngine.computeDispositions(bundle.candidates)
        #expect(result.dispositions[0] == .noSlot, "status-quo — the span is carried through unchanged")
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: spans, dispositions: result.dispositions,
            atomEvidence: atoms, provenance: .rediffSlot)
        guard let s0 = rewrite.finalSpans.first else { Issue.record("expected the status-quo span"); return }
        #expect(rewrite.finalSpans.count == 1)
        #expect(s0.startTime == 0 && s0.endTime == 200, "full minted width retained")
        // Both byte-confirmed ad regions are still enclosed by the retained span.
        #expect(s0.startTime <= 10 && s0.endTime >= 60, "encloses byte slot [10,60]")
        #expect(s0.startTime <= 100 && s0.endTime >= 160, "encloses byte slot [100,160]")
        #expect(!s0.anchorProvenance.contains(.rediffSlot), "status-quo sets no rediff width")
    }

    @Test("the acoustic rewrite path is unchanged (default provenance stays .spliceSlot)")
    func acousticRewriteDefaultProvenanceUnchanged() {
        // Same slot, but call apply WITHOUT the provenance arg (acoustic default).
        let spans = [Self.span("s0", 12, 20, 1, 2)]
        let atoms = [Self.atom(1, 9, 15), Self.atom(2, 30, 39)]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans, atomEvidence: atoms,
            playedSlots: [Self.played(8, 40)],
            coreBankMatch: [false], slotBankMatch: [false])
        let result = SpliceSlotDispositionEngine.computeDispositions(bundle.candidates)
        let rewrite = SpliceSlotRewriter.apply(
            decodedSpans: spans, dispositions: result.dispositions, atomEvidence: atoms)
        let s0 = rewrite.finalSpans.first { $0.startTime == 8 }
        #expect(s0?.anchorProvenance.contains(.spliceSlot) == true)
        #expect(s0?.anchorProvenance.contains(.rediffSlot) == false)
    }

    // MARK: - Shadow rows (reused builder + rediff breadcrumb tag)

    @Test("shadow rows describe the rediff dispositions and format under the rediffslot.shadow tag")
    func shadowRowsRediffSourced() {
        let spans = [
            Self.span("s0", 12, 20, 1, 2),
            Self.span("s1", 200, 210, 9, 10),
        ]
        let atoms = [Self.atom(1, 9, 15), Self.atom(2, 30, 39), Self.atom(9, 200, 205), Self.atom(10, 205, 210)]
        let bundle = RediffSlotOwnership.candidates(
            decodedSpans: spans, atomEvidence: atoms,
            playedSlots: [Self.played(8, 40)],
            coreBankMatch: [false, false], slotBankMatch: [false, false])
        let result = SpliceSlotDispositionEngine.computeDispositions(bundle.candidates)

        let rows = SpliceSlotShadowRowBuilder.makeRows(
            assetId: "asset-1",
            spanIds: spans.map(\.id),
            candidates: bundle.candidates,
            diagnostics: bundle.diagnostics,
            dispositions: result.dispositions)

        #expect(rows.count == 2)
        // s0: qualifying rediff slot row.
        #expect(rows[0].qualified)
        #expect(rows[0].reason == .qualifying)
        #expect(rows[0].slotStart == 8 && rows[0].slotEnd == 40)
        #expect(rows[0].widthDeltaSec == (40 - 8) - (20 - 12))
        // s1: no rediff slot → sentinel no-pair row.
        #expect(!rows[1].qualified)
        #expect(rows[1].reason == .noCandidatePairs)
        #expect(rows[1].slotStart == SpliceSlotShadowRowBuilder.sentinelSlotStart)

        // Rediff breadcrumb uses a DISTINCT tag (vs spliceslot.shadow).
        let line = RediffSlotShadowBreadcrumb.format(rows[0])
        #expect(line.hasPrefix("rediffslot.shadow "))
        #expect(line.contains("slotStart=8"))
        #expect(line.contains("reason=qualifying"))
        #expect(!SpliceSlotShadowBreadcrumb.format(rows[0]).hasPrefix("rediffslot"),
                "the acoustic breadcrumb keeps its own spliceslot.shadow tag")
    }
}
