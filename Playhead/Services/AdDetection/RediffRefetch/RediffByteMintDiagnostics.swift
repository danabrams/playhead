// RediffByteMintDiagnostics.swift
// playhead-3zxd — the durable, device-pullable record of what one day-0 mint's
// byte diffs did, so the phantom-slot fix can be VALIDATED on real audio rather
// than only on synthetic pairs.
//
// WHY THE PHONE IS THE HARNESS. `TestFixtures/Corpus/Audio` is empty, the
// checked-in byte oracle predates playhead-9s6q, and nobody is going to
// hand-fetch 66 MB A/B pairs per episode. But day-0 rediff already runs on every
// download, on the real CDN, with the real personas — so every episode Dan adds
// is a real observation if, and only if, the diff leaves something behind. This
// is that something.
//
// WHAT IT DELIBERATELY DOES NOT RECORD. No B-side byte offsets, no B-side
// timeline, no audio, no transcript — xsdz.28 never-persist-B and the
// on-device/no-cloud mandate both hold. Everything here is an A-timeline second
// or a count, it is written to the local `AnalysisStore` only, and nothing sends
// it anywhere. A run's LENGTH is identical in both files, so the A-span carries
// every quantity the aligned-seconds check needs and none that the contract
// excludes.

import Foundation

/// One day-0 mint's byte-diff instrumentation, aggregated across the k-way
/// B-side personas.
///
/// A persona the gate REJECTED contributes a zeroed entry, not a missing one.
/// Zeroed because it minted no slot, so it has no emitted slots to measure and
/// including its run counts would inflate the vacuity control into looking like
/// evidence. PRESENT because `alignedRunSpans` labels each group with the
/// persona's position in the k-way fetch, and a list handed over already
/// compacted turns that label into the ordinal among accepted personas instead —
/// the identity-that-is-not-an-identity this bead's own instrumentation shipped
/// with in R1. `AdDetectionService.mintByteExactDayZeroMarks` is what guarantees
/// the alignment; see the `defer` there.
struct RediffByteMintDiagnostics: Sendable, Equatable {
    /// Σ runs found across accepted personas. VACUITY CONTROL — `0` means the
    /// aligner found nothing and every other field says nothing.
    var runsFound: Int = 0
    /// Σ runs whose A-span partly overlapped already-accepted A-coverage.
    /// THE OPPORTUNITY COUNTER: `> 0` means a pre-playhead-3zxd build could have
    /// emitted a phantom on this episode. `0` across a corpus means the defect
    /// never got a chance there — which is weak evidence, not a confirmation.
    var runsAOverlapping: Int = 0
    /// Σ A-seconds of byte-verified matched audio retained by CLIPPING those
    /// runs instead of dropping them. THE AVERTED DAMAGE, as an upper bound: a
    /// pre-3zxd build would have left exactly these seconds uncovered by every
    /// accepted run and therefore reported them inside a divergent gap. Upper
    /// bound rather than realised, because the resulting gap still had to clear
    /// `minAdSeconds` and `maxSlotSeconds` to ship.
    var overlapSecondsRecovered: Double = 0
    /// Σ over EMITTED slots of the A-seconds a found run covers. THE INVARIANT
    /// WITNESS — zero by construction after playhead-3zxd. See
    /// `RediffSlotOwnership.ByteDiagnostics.alignedSecondsInSlots` for the one
    /// known non-phantom contributor (fragment-merge, ≤ 3 s per join) and why
    /// the magnitude discriminates.
    var alignedSecondsInSlots: Double = 0
    /// The worst SINGLE emitted slot across every persona, so a large value
    /// cannot hide inside a sum spread over many slots.
    var maxAlignedSecondsInSlot: Double = 0
    /// A-time run spans, capped and encoded — see `RediffAlignedRunSpanCodec`.
    /// `nil` when no accepted persona produced a run.
    var alignedRunSpans: String?

    static let empty = RediffByteMintDiagnostics()

    /// Fold the per-persona gate diagnostics into one per-attempt record.
    ///
    /// Sums where the quantity is additive across personas (runs, seconds) and
    /// takes the MAX where it is a worst case (`maxAlignedSecondsInSlot`) —
    /// summing a max is how a per-slot bound becomes an uninterpretable total.
    static func combining(_ perBSide: [RediffSlotOwnership.ByteDiagnostics]) -> RediffByteMintDiagnostics {
        var out = RediffByteMintDiagnostics()
        for one in perBSide {
            out.runsFound += one.runsFound
            out.runsAOverlapping += one.runsAOverlapping
            out.overlapSecondsRecovered += one.overlapSecondsRecovered
            out.alignedSecondsInSlots += one.alignedSecondsInSlots
            out.maxAlignedSecondsInSlot = max(out.maxAlignedSecondsInSlot, one.maxAlignedSecondsInSlot)
        }
        out.alignedRunSpans = RediffAlignedRunSpanCodec.encode(perBSide: perBSide.map(\.foundRunASpans))
        return out
    }
}

/// The bounded wire format for the persisted A-time run spans.
///
/// FORMAT (one line, no whitespace):
///
///     v1;<bIndex>:<start>-<end>[,<start>-<end>]*[;<bIndex>:…][;trunc=<n>]
///
/// Seconds are fixed 2 dp and non-negative (`RediffByteAligner.timeAt` never
/// returns a negative), so `-` is an unambiguous separator. `trunc=<n>` appears
/// only when the cap dropped spans and says how many, so a truncated payload can
/// never be mistaken for a complete one.
///
/// THE CAP, and what it costs. `maxSpans` (48) is a TOTAL across personas, spent
/// in persona order and then A order, so a truncated payload is biased toward
/// the START of the episode and toward the first persona. That bias is stated
/// rather than engineered away: a smarter selection (say, the runs adjacent to
/// emitted slots) would make the payload a function of the slots it is meant to
/// be checked against, which is exactly how a diagnostic comes to agree with
/// itself. 48 spans is ~18 characters each, so the column holds well under 1 KB;
/// a real episode has one run per break plus one, i.e. single digits, and the
/// cap only engages on a badly fragmented alignment — which the scalars above
/// already describe.
enum RediffAlignedRunSpanCodec {

    static let maxSpans = 48

    static func encode(perBSide: [[TimeRange]], maxSpans: Int = maxSpans) -> String? {
        let total = perBSide.reduce(0) { $0 + $1.count }
        guard total > 0 else { return nil }
        var remaining = max(0, maxSpans)
        var groups: [String] = []
        for (index, spans) in perBSide.enumerated() where !spans.isEmpty {
            guard remaining > 0 else { break }
            let kept = spans.prefix(remaining)
            remaining -= kept.count
            groups.append("\(index):" + kept.map {
                String(format: "%.2f-%.2f", $0.start, $0.end)
            }.joined(separator: ","))
        }
        guard !groups.isEmpty else { return nil }
        let dropped = total - (max(0, maxSpans) - remaining)
        if dropped > 0 { groups.append("trunc=\(dropped)") }
        return (["v1"] + groups).joined(separator: ";")
    }
}
