// RediffSegmentRecoveryBoundaryMeasurementTests.swift
// playhead-pyq7 — THE MEASUREMENT. Are playhead-9s6q segment-recovered rediff
// boundaries tight enough to auto-skip?
//
// STATUS (playhead-3zxd, 2026-08-11). This file was carried onto the 3zxd
// branch verbatim from pyq7's 93232b52, because it is the specification of the
// defect 3zxd fixes — the harness placed the gold, so it is the only thing that
// can say what the recovery arm actually did. Three expectations PINNED the
// broken behaviour and have been INVERTED in place, each keeping the pre-fix
// number in its comment as the record: family A's single > 100 s phantom,
// family B's 120–470 s accept / 540–720 s reject sweep against the 480 s cap,
// and family D's one slot carrying a real 30 s ad plus 299.99 s of show. What
// they measured is unchanged and still reported by `report()`; only the verdict
// they assert moved. The suite is therefore now a REGRESSION RAIL as well as a
// measurement: a return to the dropping accept reddens here and in
// `RediffSegmentRecoveryPhantomTests`, from two independent directions (gold
// overlap here, the gold-free aligned-seconds probe there).
//
// WHY SYNTHETIC AND NOT THE CORPUS. The corpus byte oracle
// (`playhead-dogfood-diagnostics-tier-a-rediff-byte.json`) records
// `monotonicClean` per episode and STRICT slots only — it predates 9s6q and its
// staged A/B audio is not on disk (`TestFixtures/Corpus/Audio` is a `.gitkeep`).
// The segmented slot set has therefore NEVER been computed for a real pair
// except the four windows on Dan's device. What IS reproducible is the
// mechanism: `RediffByteAligner` is a deterministic pure function of two byte
// strings, so a synthetic pair with EXACT gold ad spans measures where the
// recovery arm puts an edge, to the byte, with no oracle error at all.
//
// WHAT EACH QUANTITY MEASURES (the standing defect class is a value that names
// one thing being read as another, so these are spelled out):
//
//   goldAdSpans        A-timeline seconds occupied by bytes present in A and
//                      ABSENT from B by construction. This is ground truth, not
//                      an estimate: the harness placed them.
//   showEatenSeconds   Σ over an emitted slot of the A-seconds it covers that
//                      are NOT inside any gold ad span. Numerator = eaten
//                      seconds; denominator = that slot's own duration. This is
//                      show content the listener loses if the slot auto-skips.
//   adLeftSeconds      Σ over a gold ad span of the A-seconds NO emitted slot
//                      covers. The cheap direction (an under-skip).
//   alignedSecondsInSlot  Σ over an emitted slot of the A-seconds covered by a
//                      byte-verified common RUN. A run is bytes that occur
//                      identically in both copies, so those seconds are
//                      provably NOT divergent. This is the GOLD-FREE proxy for
//                      showEatenSeconds — it is computable on real audio, where
//                      gold does not exist, and the suite checks it against
//                      gold here so it can be trusted there.
//
// EDGE CLASS (Dan's per-edge ruling, 2026-07-29 "outer edges free, inner
// precious"): an edge is OUTER when its gold position is the episode start or
// the episode end — widening there costs nothing. Every other edge is INNER and
// an error there eats show.
//
// SIGN CONVENTION. Deltas are reported as (emitted − gold) in seconds, so for a
// START edge a NEGATIVE delta eats show and a positive one leaves ad; for an
// END edge a POSITIVE delta eats show and a negative one leaves ad. The
// direction-free `showEatenSeconds` / `adLeftSeconds` pair above is what the
// verdict is stated in, precisely so the sign convention cannot be misread.

import Foundation
import Testing

@testable import Playhead

// MARK: - Synthetic pair with exact gold

/// A synthetic A/B pair whose divergent regions are known to the byte.
///
/// A (the played copy) = content₀ + adA₀ + content₁ + adA₁ + … + contentₙ
/// B (the re-fetch)    = content₀ + adB₀ + content₁ + adB₁ + … + contentₙ
///
/// Content blocks are byte-identical between the copies; ad blocks are drawn
/// from different seeds and may differ in LENGTH, which is what makes a chain
/// go non-monotonic. `adAFrames == 0` models a break the fresh copy gained
/// (inserted_in_B); `adBFrames == 0` models one it lost (removed_in_B).
enum SegmentRecoveryFixture {

    struct Break: Sendable {
        let adAFrames: Int
        let adBFrames: Int

        static func removedInB(_ frames: Int) -> Break { Break(adAFrames: frames, adBFrames: 0) }
        static func insertedInB(_ frames: Int) -> Break { Break(adAFrames: 0, adBFrames: frames) }
        static func replaced(a: Int, b: Int) -> Break { Break(adAFrames: a, adBFrames: b) }
    }

    struct Span: Sendable {
        let start: Double
        let end: Double
        var duration: Double { end - start }
    }

    struct Pair: Sendable {
        let name: String
        let aData: Data
        let bData: Data
        /// A-timeline seconds present in A and absent from B, by construction.
        let goldAdSpans: [Span]
        let aDurationSeconds: Double
    }

    /// `contentFrames.count` must be `breaks.count + 1`.
    static func build(name: String, contentFrames: [Int], breaks: [Break]) -> Pair {
        precondition(contentFrames.count == breaks.count + 1, "\(name): need one more content block than breaks")
        let spf = SyntheticMP3.secondsPerFrame
        var aParts: [[UInt8]] = []
        var bParts: [[UInt8]] = []
        var gold: [Span] = []
        var aFrameCursor = 0
        var seed: UInt64 = 0x5EED_0000

        for (index, count) in contentFrames.enumerated() {
            seed &+= 0x1001
            let content = SyntheticMP3.frames(count: count, seed: seed)
            aParts.append(contentsOf: content)
            bParts.append(contentsOf: content)
            aFrameCursor += count

            guard index < breaks.count else { continue }
            let brk = breaks[index]
            if brk.adAFrames > 0 {
                seed &+= 0xA_D000
                aParts.append(contentsOf: SyntheticMP3.frames(count: brk.adAFrames, seed: seed))
                gold.append(Span(
                    start: Double(aFrameCursor) * spf,
                    end: Double(aFrameCursor + brk.adAFrames) * spf
                ))
                aFrameCursor += brk.adAFrames
            }
            if brk.adBFrames > 0 {
                seed &+= 0xB_D000
                bParts.append(contentsOf: SyntheticMP3.frames(count: brk.adBFrames, seed: seed))
            }
        }
        return Pair(
            name: name,
            aData: SyntheticMP3.file(aParts),
            bData: SyntheticMP3.file(bParts),
            goldAdSpans: gold,
            aDurationSeconds: Double(aFrameCursor) * spf
        )
    }

    static func frames(seconds: Double) -> Int {
        Int((seconds / SyntheticMP3.secondsPerFrame).rounded())
    }
}

// MARK: - Per-pair measurement

struct SegmentRecoveryMeasurement: Sendable {

    struct EmittedSlot: Sendable {
        let start: Double
        let end: Double
        var duration: Double { end - start }
        /// A-seconds inside this slot NOT covered by any gold ad span.
        let showEatenSeconds: Double
        /// A-seconds inside this slot covered by a byte-verified common run —
        /// the gold-free proxy for `showEatenSeconds`.
        let alignedSecondsInSlot: Double
        /// Index into `goldAdSpans` of the best-overlapping gold span, or nil
        /// when this slot overlaps NO gold ad at all (a wholly phantom slot).
        let matchedGoldIndex: Int?
    }

    let name: String
    let monotonicClean: Bool
    let runsFound: Int
    let runsChained: Int
    let segmentedRunsChained: Int
    let chainedFractionB: Double
    let segmentedChainedFractionB: Double
    let aDurationSeconds: Double
    let goldAdSpans: [SegmentRecoveryFixture.Span]
    /// Slots the STRICT gate emitted (empty when it rejected).
    let strictSlots: [EmittedSlot]
    let strictRejected: String?
    /// Slots the 9s6q recovery gate emitted (empty when it rejected).
    let recoveredSlots: [EmittedSlot]
    let recoveredRejected: String?

    /// Slots the recovery arm emitted that overlap NO gold ad second at all.
    var phantomSlots: [EmittedSlot] { recoveredSlots.filter { $0.matchedGoldIndex == nil } }

    var totalShowEatenSeconds: Double { recoveredSlots.reduce(0) { $0 + $1.showEatenSeconds } }
    var totalAlignedSecondsInSlots: Double { recoveredSlots.reduce(0) { $0 + $1.alignedSecondsInSlot } }
    /// A-seconds of gold ad NO emitted (recovered) slot covers.
    var totalAdLeftSeconds: Double {
        goldAdSpans.reduce(0) { running, span in
            let covered = Self.coveredSeconds(of: span, by: recoveredSlots.map { ($0.start, $0.end) })
            return running + max(0, span.duration - covered)
        }
    }

    static func overlap(_ lhs: (Double, Double), _ rhs: (Double, Double)) -> Double {
        max(0, min(lhs.1, rhs.1) - max(lhs.0, rhs.0))
    }

    static func coveredSeconds(of span: SegmentRecoveryFixture.Span, by ranges: [(Double, Double)]) -> Double {
        ranges.reduce(0) { $0 + overlap((span.start, span.end), $1) }
    }

    // MARK: Build

    static func measure(
        _ pair: SegmentRecoveryFixture.Pair,
        alignerConfig: RediffByteAligner.Configuration = .default,
        gateConfig: RediffSlotOwnership.Configuration = .default
    ) -> SegmentRecoveryMeasurement {
        let alignment = RediffByteAligner.align(aData: pair.aData, bData: pair.bData, config: alignerConfig)

        // Re-derive the FOUND runs (the `Alignment` keeps only a count) so the
        // gold-free proxy can be computed. Same inputs, same pure function, so
        // this is the identical run set `align` used.
        let runSpans: [(Double, Double)] = pair.aData.withUnsafeBytes { ma in
            pair.bData.withUnsafeBytes { mb in
                let parsedA = RediffByteAligner.parse(ma)
                let parsedB = RediffByteAligner.parse(mb)
                let runs = RediffByteAligner.byteRuns(
                    ma, mb, parsedA: parsedA, parsedB: parsedB, config: alignerConfig
                )
                return runs.map {
                    (RediffByteAligner.timeAt(parsedA, byteOffset: $0.aStart),
                     RediffByteAligner.timeAt(parsedA, byteOffset: $0.aStart + $0.bytes))
                }
            }
        }

        func classify(_ outcome: RediffSlotOwnership.ByteGateOutcome) -> ([EmittedSlot], String?) {
            guard case .accepted(let acceptance) = outcome else {
                return ([], describe(outcome))
            }
            let slots = acceptance.playedSlots.map { slot -> EmittedSlot in
                let range = (slot.startSeconds, slot.endSeconds)
                let goldCovered = pair.goldAdSpans.reduce(0.0) { $0 + overlap(range, ($1.start, $1.end)) }
                var bestIndex: Int?
                var bestOverlap = 0.0
                for (index, span) in pair.goldAdSpans.enumerated() {
                    let value = overlap(range, (span.start, span.end))
                    if value > bestOverlap { bestOverlap = value; bestIndex = index }
                }
                return EmittedSlot(
                    start: slot.startSeconds,
                    end: slot.endSeconds,
                    showEatenSeconds: max(0, (slot.endSeconds - slot.startSeconds) - goldCovered),
                    alignedSecondsInSlot: runSpans.reduce(0.0) { $0 + overlap(range, $1) },
                    matchedGoldIndex: bestIndex
                )
            }
            return (slots, nil)
        }

        let (strictSlots, strictRejected) = classify(
            RediffSlotOwnership.gateAndDiffBytes(
                alignment: alignment, config: gateConfig, recoverNonMonotonicSegments: false
            )
        )
        let (recoveredSlots, recoveredRejected) = classify(
            RediffSlotOwnership.gateAndDiffBytes(
                alignment: alignment, config: gateConfig, recoverNonMonotonicSegments: true
            )
        )

        return SegmentRecoveryMeasurement(
            name: pair.name,
            monotonicClean: alignment.monotonicClean,
            runsFound: alignment.runsFound,
            runsChained: alignment.chain.count,
            segmentedRunsChained: alignment.segmentedRunsChained,
            chainedFractionB: alignment.chainedFractionB,
            segmentedChainedFractionB: alignment.segmentedChainedFractionB,
            aDurationSeconds: pair.aDurationSeconds,
            goldAdSpans: pair.goldAdSpans,
            strictSlots: strictSlots,
            strictRejected: strictRejected,
            recoveredSlots: recoveredSlots,
            recoveredRejected: recoveredRejected
        )
    }

    static func describe(_ outcome: RediffSlotOwnership.ByteGateOutcome) -> String {
        switch outcome {
        case .rejectedNoChainedRuns: return "noChainedRuns"
        case .rejectedNonMonotonic(let dropped): return "nonMonotonic(\(dropped))"
        case .rejectedLowChainedFraction(let fraction): return String(format: "lowFraction(%.3f)", fraction)
        case .accepted: return "accepted"
        }
    }

    // MARK: Report

    /// One machine-greppable block per pair. `PYQ7` prefixes every line so the
    /// measurement can be lifted out of a full gate log.
    func report() -> String {
        var lines: [String] = []
        lines.append("PYQ7 PAIR \(name) monotonicClean=\(monotonicClean) runsFound=\(runsFound) chained=\(runsChained) segChained=\(segmentedRunsChained) "
            + String(format: "fracB=%.4f segFracB=%.4f aDur=%.2f", chainedFractionB, segmentedChainedFractionB, aDurationSeconds))
        for (index, span) in goldAdSpans.enumerated() {
            lines.append(String(format: "PYQ7   GOLD[%d] %.4f..%.4f (%.4f s)", index, span.start, span.end, span.duration))
        }
        lines.append("PYQ7   STRICT \(strictRejected ?? "accepted") slots=\(strictSlots.count)")
        for slot in strictSlots {
            lines.append(String(format: "PYQ7     S %.4f..%.4f dur=%.4f eaten=%.4f aligned=%.4f gold=%@",
                                slot.start, slot.end, slot.duration, slot.showEatenSeconds,
                                slot.alignedSecondsInSlot, slot.matchedGoldIndex.map(String.init) ?? "NONE"))
        }
        lines.append("PYQ7   RECOVERED \(recoveredRejected ?? "accepted") slots=\(recoveredSlots.count)")
        for slot in recoveredSlots {
            let goldIndex = slot.matchedGoldIndex
            let delta: String
            if let goldIndex {
                let span = goldAdSpans[goldIndex]
                delta = String(format: " dStart=%+.4f dEnd=%+.4f", slot.start - span.start, slot.end - span.end)
            } else {
                delta = " dStart=NA dEnd=NA"
            }
            lines.append(String(format: "PYQ7     R %.4f..%.4f dur=%.4f eaten=%.4f aligned=%.4f gold=%@",
                                slot.start, slot.end, slot.duration, slot.showEatenSeconds,
                                slot.alignedSecondsInSlot, goldIndex.map(String.init) ?? "NONE") + delta)
        }
        lines.append(String(format: "PYQ7   TOTALS eatenSec=%.4f adLeftSec=%.4f alignedInSlotsSec=%.4f",
                            totalShowEatenSeconds, totalAdLeftSeconds, totalAlignedSecondsInSlots))
        return lines.joined(separator: "\n")
    }
}

// MARK: - The measurement suite

@Suite("playhead-pyq7 — 9s6q segment-recovered boundary tightness (MEASUREMENT)")
struct RediffSegmentRecoveryBoundaryMeasurementTests {

    /// ≈30 s at 128 kbps CBR — a typical DAI unit.
    private static let ad30 = SegmentRecoveryFixture.frames(seconds: 30)
    private static let ad60 = SegmentRecoveryFixture.frames(seconds: 60)

    /// The measured edge budget: the ONLY imprecision on a genuine divergence
    /// is the shared 4-byte CBR frame header, which the earlier run's greedy
    /// byte extension carries across the splice. 4 bytes of a 417-byte frame is
    /// 0.00025 s, so 10 ms is two orders of magnitude of headroom and any
    /// failure here is a real change in edge placement, not float noise.
    private static let edgeBudgetSeconds = 0.010

    /// RAIL — the tightness half of the verdict, asserted over every family.
    ///
    /// A recovered slot that contains NO aligned seconds (no byte-verified
    /// common run intersects it) is a slot whose whole A-span really is absent
    /// from the fresh copy — and for those the recovery arm places BOTH edges
    /// inside `edgeBudgetSeconds` of the true splice, erring in the direction
    /// that leaves ad rather than eating show.
    ///
    /// RAIL — and the precision half. `alignedSecondsInSlot` must never
    /// UNDER-report `showEatenSeconds`. That inequality is the whole basis for
    /// proposing the aligned-seconds test as a promotion precondition: it is
    /// computable without gold, so if it could miss eaten seconds it would be
    /// worthless on real audio, where gold does not exist.
    private func assertTightnessInvariants(
        _ measurement: SegmentRecoveryMeasurement,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        for slot in measurement.recoveredSlots {
            #expect(
                slot.alignedSecondsInSlot >= slot.showEatenSeconds - 1e-6,
                "\(measurement.name): the gold-free proxy UNDER-reported eaten show — aligned=\(slot.alignedSecondsInSlot) eaten=\(slot.showEatenSeconds)",
                sourceLocation: sourceLocation
            )
            guard slot.alignedSecondsInSlot <= 1e-6, let goldIndex = slot.matchedGoldIndex else { continue }
            let gold = measurement.goldAdSpans[goldIndex]
            #expect(
                slot.showEatenSeconds <= 1e-6,
                "\(measurement.name): a slot with zero aligned seconds still ate \(slot.showEatenSeconds) s of show",
                sourceLocation: sourceLocation
            )
            #expect(
                abs(slot.start - gold.start) < Self.edgeBudgetSeconds,
                "\(measurement.name): start edge \(slot.start) vs gold \(gold.start)",
                sourceLocation: sourceLocation
            )
            #expect(
                abs(slot.end - gold.end) < Self.edgeBudgetSeconds,
                "\(measurement.name): end edge \(slot.end) vs gold \(gold.end)",
                sourceLocation: sourceLocation
            )
            // Direction: a start may only run LATE and an end only EARLY, so
            // the residual error leaves ad rather than clipping show.
            #expect(slot.start >= gold.start - 1e-9, "\(measurement.name): start edge ran EARLY into show", sourceLocation: sourceLocation)
            #expect(slot.end <= gold.end + 1e-9, "\(measurement.name): end edge ran LATE into show", sourceLocation: sourceLocation)
        }
    }

    /// FAMILY A — which break KIND drives a chain non-monotonic, and where the
    /// recovery arm puts the edges when it does. Content blocks are 240 s so
    /// every run clears the 64 KiB (~4.1 s) min-run comfortably.
    @Test("FAMILY A: break kind x count — edge placement vs exact gold")
    func familyABreakKinds() {
        let content = SegmentRecoveryFixture.frames(seconds: 240)
        var pairs: [SegmentRecoveryFixture.Pair] = []

        let kinds: [(String, SegmentRecoveryFixture.Break)] = [
            ("removedInB30", .removedInB(Self.ad30)),
            ("insertedInB30", .insertedInB(Self.ad30)),
            ("replacedEqual30", .replaced(a: Self.ad30, b: Self.ad30)),
            ("replacedShorterB", .replaced(a: Self.ad60, b: Self.ad30)),
            ("replacedLongerB", .replaced(a: Self.ad30, b: Self.ad60))
        ]
        for (label, brk) in kinds {
            for breakCount in 1...3 {
                pairs.append(SegmentRecoveryFixture.build(
                    name: "A/\(label)/x\(breakCount)",
                    contentFrames: Array(repeating: content, count: breakCount + 1),
                    breaks: Array(repeating: brk, count: breakCount)
                ))
            }
        }

        var out: [String] = ["PYQ7 FAMILY A begin"]
        var byName: [String: SegmentRecoveryMeasurement] = [:]
        for pair in pairs {
            let measurement = SegmentRecoveryMeasurement.measure(pair)
            out.append(measurement.report())
            byName[pair.name] = measurement
            assertTightnessInvariants(measurement)
        }
        out.append("PYQ7 FAMILY A end")
        print(out.joined(separator: "\n"))

        // MEASURED FACT — which break kinds reach the recovery arm at all. A
        // pure ROTATION (same break count, any lengths) chains monotonically:
        // both runs still advance in A and in B, so `chainRuns` keeps them and
        // 9s6q is a no-op. Only a break-COUNT disagreement goes non-monotonic.
        for count in 1...3 {
            #expect(byName["A/replacedEqual30/x\(count)"]?.monotonicClean == true)
            #expect(byName["A/replacedShorterB/x\(count)"]?.monotonicClean == true)
            #expect(byName["A/replacedLongerB/x\(count)"]?.monotonicClean == true)
            #expect(byName["A/removedInB30/x\(count)"]?.monotonicClean == false)
            #expect(byName["A/insertedInB30/x\(count)"]?.monotonicClean == false)
        }

        // MEASURED FACT — removed-in-B recovers EXACTLY: the strict arm rejects
        // the whole fetch, the recovery arm returns one slot per gold ad with
        // zero eaten show. This is the seconds 9s6q was built to unlock.
        for count in 1...3 {
            let recovered = try? #require(byName["A/removedInB30/x\(count)"])
            #expect(recovered?.strictRejected != nil, "strict must reject a removed-in-B multi-break pair")
            #expect(recovered?.recoveredSlots.count == count)
            #expect((recovered?.totalShowEatenSeconds ?? 1) <= 1e-6)
        }

        // MEASURED FACT — THE BLOCKER, AND ITS REMOVAL (playhead-3zxd).
        //
        // BEFORE: an inserted-in-B break makes the run AFTER it A-overlap the
        // run before it (the shared CBR frame header bleeds the earlier run
        // 4 bytes past the splice), so the greedy A-order accept in
        // `segmentDivergentSlots` DROPPED the whole following run and reported
        // its entire A-span as one divergent slot. `A/insertedInB30/x2` emitted
        // exactly 1 phantom, > 100 s wide, with no gold ad anywhere inside it.
        //
        // AFTER: the A-overlapper is CLIPPED to its uncovered tail instead of
        // dropped, so no run's A-span is ever reported as divergent and this
        // family emits no phantom at all. The instruction in the original
        // version of this note — "if this ever fails, re-run the measurement and
        // re-decide the promotion rather than deleting the expectation" — is
        // what happened; the expectation is INVERTED, not removed, so a
        // regression to the dropping accept reddens here as well as in
        // `RediffSegmentRecoveryPhantomTests`.
        let inserted = try? #require(byName["A/insertedInB30/x2"])
        #expect(inserted?.phantomSlots.isEmpty == true,
                "no slot may contain zero gold ad seconds — got \(inserted?.phantomSlots.count ?? -1)")
        #expect(inserted?.recoveredSlots.isEmpty == true,
                "an inserted-in-B break leaves NO ad in the played copy, so nothing may be emitted")
    }

    /// FAMILY B — the duration cap is the only thing between a phantom slot and
    /// a skip, so find where it stops protecting. An inserted-in-B break makes
    /// the run AFTER it A-overlap the run before it (shared CBR frame header
    /// bleeds the earlier run 4 bytes past the splice), and the greedy A-order
    /// accept in `segmentDivergentSlots` therefore DROPS the whole following
    /// run — turning the entire following content block into one "ad".
    @Test("FAMILY B: inserted-in-B phantom width vs the 480 s duration cap")
    func familyBPhantomWidthVsCap() {
        var out: [String] = ["PYQ7 FAMILY B begin"]
        var widest = 0.0
        var acceptedTails: [Double] = []
        var rejectedTails: [Double] = []
        for tailSeconds in [120.0, 240.0, 360.0, 420.0, 470.0, 540.0, 720.0] {
            let pair = SegmentRecoveryFixture.build(
                name: "B/insertedInB/tail\(Int(tailSeconds))s",
                contentFrames: [
                    SegmentRecoveryFixture.frames(seconds: 900),
                    SegmentRecoveryFixture.frames(seconds: tailSeconds)
                ],
                breaks: [.insertedInB(Self.ad30)]
            )
            let measurement = SegmentRecoveryMeasurement.measure(pair)
            out.append(measurement.report())
            assertTightnessInvariants(measurement)
            #expect(measurement.goldAdSpans.isEmpty, "control: an inserted-in-B break leaves NO ad in the played copy")
            if measurement.recoveredRejected == nil {
                acceptedTails.append(tailSeconds)
                widest = max(widest, measurement.recoveredSlots.map(\.duration).max() ?? 0)
                // Every accepted slot here is pure show — there is no gold at all.
                for slot in measurement.recoveredSlots {
                    #expect(slot.matchedGoldIndex == nil)
                    #expect(abs(slot.showEatenSeconds - slot.duration) < 1e-6)
                }
            } else {
                rejectedTails.append(tailSeconds)
            }
        }
        out.append("PYQ7 FAMILY B end")
        print(out.joined(separator: "\n"))

        // MEASURED FACT — BEFORE playhead-3zxd, `maxSlotSeconds` (480 s) was the
        // ONLY thing standing between this phantom and a skip, and it did not
        // engage until the phantom was already eight minutes wide: tails
        // 120/240/360/420/470 s were ACCEPTED (100 % show, every second), and
        // only 540/720 s were rejected. The boundary was exact.
        //
        // AFTER: the sweep is empty at every width. That is the point of fixing
        // the CAUSE rather than tightening the cap — a cap bounds the damage at
        // whatever number it is set to, and a longer content block walks
        // straight under any such number. `widest` staying 0 is the assertion
        // that no pure-show slot is emitted at all, not that it is emitted
        // smaller.
        #expect(widest == 0, "no pure-show phantom ships at ANY width — widest was \(widest) s")
        #expect(rejectedTails.count + acceptedTails.count == 7,
                "VACUITY: all seven widths must be exercised — got \(acceptedTails) + \(rejectedTails)")
    }

    /// FAMILY C — outer edges. A pre-roll starts at the episode start and a
    /// post-roll ends at the episode end; by Dan's per-edge ruling those edges
    /// are free to widen, so they must be counted separately from inner ones.
    @Test("FAMILY C: pre-roll / post-roll (outer) edges")
    func familyCOuterEdges() {
        let content = SegmentRecoveryFixture.frames(seconds: 300)
        var out: [String] = ["PYQ7 FAMILY C begin"]
        let cases: [(String, [Int], [SegmentRecoveryFixture.Break])] = [
            ("C/preroll-removed", [0, content], [.removedInB(Self.ad30)]),
            ("C/preroll-replaced", [0, content], [.replaced(a: Self.ad30, b: Self.ad60)]),
            ("C/postroll-removed", [content, 0], [.removedInB(Self.ad30)]),
            ("C/postroll-replaced", [content, 0], [.replaced(a: Self.ad30, b: Self.ad60)]),
            ("C/pre+mid+post", [0, content, content, 0],
             [.removedInB(Self.ad30), .removedInB(Self.ad30), .removedInB(Self.ad30)])
        ]
        var byName: [String: SegmentRecoveryMeasurement] = [:]
        for (name, contentFrames, breaks) in cases {
            let pair = SegmentRecoveryFixture.build(name: name, contentFrames: contentFrames, breaks: breaks)
            let measurement = SegmentRecoveryMeasurement.measure(pair)
            out.append(measurement.report())
            byName[name] = measurement
            assertTightnessInvariants(measurement)
        }
        out.append("PYQ7 FAMILY C end")
        print(out.joined(separator: "\n"))

        // MEASURED FACT — an OUTER edge is placed exactly, same as an inner
        // one. Nothing about the recovery arm treats the episode boundary
        // specially, so Dan's "outer edges are free" ruling buys no extra
        // safety here and no extra risk either.
        let mixed = try? #require(byName["C/pre+mid+post"])
        #expect(mixed?.monotonicClean == false)
        #expect(mixed?.recoveredSlots.count == 3)
        #expect((mixed?.totalShowEatenSeconds ?? 1) <= 1e-6)
        #expect((mixed?.recoveredSlots.first?.start ?? 1) == 0.0, "the pre-roll slot starts at the episode start")
    }

    /// FAMILY D — mixed structure, the realistic case: a played copy and a
    /// re-fetch that disagree about how many breaks there are AND how long each
    /// is. This is the population the day-0 mint actually sees.
    @Test("FAMILY D: mixed multi-break structures")
    func familyDMixed() {
        let content = SegmentRecoveryFixture.frames(seconds: 300)
        var out: [String] = ["PYQ7 FAMILY D begin"]
        let cases: [(String, [SegmentRecoveryFixture.Break])] = [
            ("D/removed+replaced", [.removedInB(Self.ad30), .replaced(a: Self.ad30, b: Self.ad60)]),
            ("D/replaced+inserted", [.replaced(a: Self.ad30, b: Self.ad60), .insertedInB(Self.ad30)]),
            ("D/inserted+removed", [.insertedInB(Self.ad30), .removedInB(Self.ad30)]),
            ("D/three-replaced-mixed", [
                .replaced(a: Self.ad30, b: Self.ad60),
                .replaced(a: Self.ad60, b: Self.ad30),
                .replaced(a: Self.ad30, b: Self.ad30)
            ])
        ]
        var byName: [String: SegmentRecoveryMeasurement] = [:]
        for (name, breaks) in cases {
            let pair = SegmentRecoveryFixture.build(
                name: name,
                contentFrames: Array(repeating: content, count: breaks.count + 1),
                breaks: breaks
            )
            let measurement = SegmentRecoveryMeasurement.measure(pair)
            out.append(measurement.report())
            byName[name] = measurement
            assertTightnessInvariants(measurement)
        }
        out.append("PYQ7 FAMILY D end")
        print(out.joined(separator: "\n"))

        // MEASURED FACT — the worst shape for the promotion question.
        //
        // BEFORE playhead-3zxd the phantom here was NOT a separate slot the eye
        // could catch: the dropped run's A-span was fused to a REAL 30 s ad, so
        // ONE emitted slot carried a genuine ad AND 299.99 s of show, with an
        // inner start edge 300 s early — indistinguishable at the mint from the
        // exact ones above unless the aligned-seconds test was applied.
        //
        // AFTER: still ONE slot, still overlapping the real ad, but it IS the
        // real ad. Clipping the A-overlapper reinstates the boundary the drop
        // had erased, so the fix is a recall GAIN here, not merely a rejection.
        // The gold-free proxy is what the promotion argument rests on, so it is
        // still asserted both directions: it may never under-report eaten show,
        // and with nothing eaten it must itself read zero.
        let fused = try? #require(byName["D/inserted+removed"])
        #expect(fused?.recoveredSlots.count == 1)
        let slot = try? #require(fused?.recoveredSlots.first)
        #expect(slot?.matchedGoldIndex != nil, "it still finds the real ad")
        #expect((slot?.showEatenSeconds ?? 1) <= 1e-6,
                "it no longer eats show — got \(slot?.showEatenSeconds ?? -1) s (was 299.99 s)")
        #expect((slot?.duration ?? 0) < 31.0,
                "the slot is the 30 s ad, not the ad fused to a content block — got \(slot?.duration ?? -1) s")
        #expect((slot?.alignedSecondsInSlot ?? 1) <= 1e-6,
                "the gold-free proxy agrees: no byte-verified run intersects the emitted slot")
    }
}
