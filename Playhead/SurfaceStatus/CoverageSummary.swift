// CoverageSummary.swift
// Phase 2 coverage record (playhead-cthe). Persisted on `Episode` as a
// Codable attribute. `playbackReadiness` is a DERIVED view of the pair
// `(coverage, readinessAnchor)` — not an intrinsic episode field — so
// the Library, NowPlaying, and Activity surfaces all re-derive it from
// the same inputs and cannot fall out of sync.
//
// Scope: playhead-cthe (Phase 2 deliverable 2 — "Persist CoverageSummary
// + derive playbackReadiness from coverage + readinessAnchor").
//
// ----- Design notes -----
//
// * The spec schema uses `[Range<Duration>]` for `coverageRanges`. The
//   rest of this codebase already commits to `TimeInterval` (Double) for
//   every time value that crosses a persistence boundary (see
//   `Episode.playbackPosition`, the reducer's `readinessAnchor`,
//   `AnalysisAsset.*CoverageEndTime`). Using `Duration` here would force
//   a one-off Codable hand-roll for a type that has no other usage in
//   the repo and would diverge from every neighboring time field. The
//   ranges carry identical semantics as `ClosedRange<TimeInterval>` with
//   a Double backing (SQLite / JSON Lines / AnalysisStore all encode
//   them as Double anyway), so we represent the spec's `Range<Duration>`
//   here as `ClosedRange<TimeInterval>`. Documented as a deliberate
//   deviation; if a future bead switches the whole time taxonomy to
//   `Duration`, this file joins the flip.
//
// * `coverageRanges` must be sorted by `lowerBound` ascending and MUST
//   NOT overlap. The initializer normalizes the input by sorting and
//   merging adjacent/overlapping ranges so callers never need to do
//   this themselves. The derivation relies on the normalized form.
//
// * `PlaybackReadiness` is derived; never persist it. The reducer calls
//   `derivePlaybackReadiness(coverage:anchor:)` on every render so a
//   Library cell's badge always reflects the current anchor.

import Foundation

// MARK: - CoverageSummary

/// Persisted, versioned record of the on-device analysis coverage for a
/// single episode. Stored as a Codable attribute on `Episode`.
///
/// Invariants (enforced at construction AND checked centrally by
/// `SurfaceStatusInvariants.violations(of:)`):
///   * `coverageRanges` is sorted by `lowerBound` ascending and has no
///     overlapping (or touching) ranges. The initializer normalizes on
///     input; the normalized form is what gets persisted.
///   * `firstCoveredOffset == coverageRanges.first?.lowerBound` — a
///     cached convenience maintained by the initializer.
///   * `isComplete == true` implies `coverageRanges` is non-empty AND
///     `firstCoveredOffset != nil`. Violating this in a persisted blob
///     is an impossible state the invariant channel flags.
///
/// Cross-refs:
///   * playhead-ol05 — assertion channel this type's invariants piggy-back on.
///   * playhead-5bb3 — reducer that consumes the derived readiness.
///   * playhead-zp5y / playhead-quh7 — downstream Phase 2 beads that
///     populate the record from the analysis pipeline.
struct CoverageSummary: Sendable, Hashable, Codable {

    /// Sorted, non-overlapping closed ranges of time (seconds) that have
    /// been fully analyzed. A contiguous segment from 0..<900 covering
    /// the first 15 minutes appears as a single-element array whose one
    /// range is `0...900`. Empty when no analysis has confirmed any
    /// region (which derives to `PlaybackReadiness.none`).
    let coverageRanges: [ClosedRange<TimeInterval>]

    /// Earliest covered start time, or `nil` when `coverageRanges` is
    /// empty. Persisted as a convenience — equals
    /// `coverageRanges.first?.lowerBound` on every well-formed record.
    let firstCoveredOffset: TimeInterval?

    /// `true` when the full episode duration has been analyzed end-to-end.
    /// Whoever constructs the record is responsible for flipping this when
    /// the analysis pipeline reports completion; the derivation function
    /// treats it as an authoritative short-circuit to `.complete` regardless
    /// of the anchor.
    let isComplete: Bool

    /// Identifier of the on-device model that produced the coverage.
    /// Bumped by the analysis pipeline when the model weights change so
    /// downstream consumers can invalidate stale records.
    let modelVersion: String

    /// Integer version of the decision policy (classifier thresholds /
    /// post-processing rules) applied to produce this coverage. Bumped
    /// independently of `modelVersion` when only the decision policy
    /// moves.
    let policyVersion: Int

    /// Integer version of the feature schema (window sizes, mel bin
    /// counts, etc.). Bumped when a feature change invalidates prior
    /// coverage even if `modelVersion` and `policyVersion` are stable.
    let featureSchemaVersion: Int

    /// Wall-clock time at which this record was last written. Used by
    /// downstream telemetry and as a tiebreaker when two records claim
    /// the same coverage for the same episode.
    let updatedAt: Date

    // MARK: - Init

    /// Designated initializer. Normalizes `coverageRanges` by sorting
    /// (ascending `lowerBound`) and merging adjacent/overlapping ranges.
    /// `firstCoveredOffset` is derived from the normalized result.
    ///
    /// A consumer that has already-normalized input pays only the sort
    /// check; construction is O(n log n) on the input length.
    init(
        coverageRanges: [ClosedRange<TimeInterval>],
        isComplete: Bool,
        modelVersion: String,
        policyVersion: Int,
        featureSchemaVersion: Int,
        updatedAt: Date
    ) {
        let normalized = Self.normalize(coverageRanges)
        self.coverageRanges = normalized
        self.firstCoveredOffset = normalized.first?.lowerBound
        self.isComplete = isComplete
        self.modelVersion = modelVersion
        self.policyVersion = policyVersion
        self.featureSchemaVersion = featureSchemaVersion
        self.updatedAt = updatedAt
    }

    // MARK: - Codable
    //
    // The compiler-synthesized Decodable implementation would decode each
    // stored property directly, bypassing the designated initializer's
    // `normalize(coverageRanges)` step and its derivation of
    // `firstCoveredOffset`. A hand-edited or externally-written JSON blob
    // with unsorted/overlapping ranges (or a stale `firstCoveredOffset`)
    // would therefore decode into a non-canonical instance and violate the
    // invariants documented above. We override `init(from:)` so every
    // decode path flows through the designated init, producing the same
    // canonical form callers get from direct construction.
    //
    // `updatedAt` is preserved as decoded rather than overwritten — a
    // round-tripped record must retain its original write timestamp.

    enum CodingKeys: String, CodingKey {
        case coverageRanges
        case firstCoveredOffset
        case isComplete
        case modelVersion
        case policyVersion
        case featureSchemaVersion
        case updatedAt
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // Decode the raw (potentially non-canonical) ranges then delegate
        // through the designated initializer so normalization and
        // `firstCoveredOffset` derivation run exactly as they do for
        // in-process construction. The persisted `firstCoveredOffset`
        // field is intentionally ignored — the designated init derives
        // the canonical value from the normalized ranges.
        let rawRanges = try container.decode([ClosedRange<TimeInterval>].self, forKey: .coverageRanges)
        let isComplete = try container.decode(Bool.self, forKey: .isComplete)
        let modelVersion = try container.decode(String.self, forKey: .modelVersion)
        let policyVersion = try container.decode(Int.self, forKey: .policyVersion)
        let featureSchemaVersion = try container.decode(Int.self, forKey: .featureSchemaVersion)
        let updatedAt = try container.decode(Date.self, forKey: .updatedAt)
        self.init(
            coverageRanges: rawRanges,
            isComplete: isComplete,
            modelVersion: modelVersion,
            policyVersion: policyVersion,
            featureSchemaVersion: featureSchemaVersion,
            updatedAt: updatedAt
        )
    }

    /// Construct an empty summary (no coverage yet). Useful when the
    /// analysis pipeline enqueues an episode but has not produced any
    /// confirmed windows. Derives to `PlaybackReadiness.none`.
    static func empty(
        modelVersion: String,
        policyVersion: Int,
        featureSchemaVersion: Int,
        updatedAt: Date = Date()
    ) -> CoverageSummary {
        CoverageSummary(
            coverageRanges: [],
            isComplete: false,
            modelVersion: modelVersion,
            policyVersion: policyVersion,
            featureSchemaVersion: featureSchemaVersion,
            updatedAt: updatedAt
        )
    }

    // MARK: - Derivation convenience

    /// Delegate to the pure free function. Retained so existing reducer
    /// call-sites (`coverage?.readiness(anchor:)`) continue to compile
    /// unchanged.
    func readiness(anchor: TimeInterval?) -> PlaybackReadiness {
        derivePlaybackReadiness(coverage: self, anchor: anchor)
    }

    // MARK: - Normalization

    /// Sort then merge adjacent/overlapping ranges so every persisted
    /// `CoverageSummary` is in canonical form. Two ranges merge iff
    /// `a.upperBound >= b.lowerBound` (touching counts as overlapping —
    /// the coverage is continuous).
    ///
    /// Internal detail; callers go through the initializer.
    static func normalize(
        _ ranges: [ClosedRange<TimeInterval>]
    ) -> [ClosedRange<TimeInterval>] {
        guard !ranges.isEmpty else { return [] }
        let sorted = ranges.sorted { $0.lowerBound < $1.lowerBound }
        var merged: [ClosedRange<TimeInterval>] = []
        merged.reserveCapacity(sorted.count)
        for r in sorted {
            if let last = merged.last, last.upperBound >= r.lowerBound {
                // Overlapping or touching → merge upper bounds.
                let newUpper = max(last.upperBound, r.upperBound)
                merged[merged.count - 1] = last.lowerBound...newUpper
            } else {
                merged.append(r)
            }
        }
        return merged
    }
}

// MARK: - PlaybackReadiness

/// Derived readiness signal for a single episode at a single anchor.
/// Never persisted — always computed from the pair `(coverage, anchor)`
/// so UI surfaces that observe different anchors (Library, NowPlaying,
/// Activity) cannot diverge from each other.
///
/// Library cells must render the ✓ affordance ONLY for `.proximal` or
/// `.complete`. See `EpisodeListView` for the call-site.
enum PlaybackReadiness: String, Sendable, Hashable, Codable, CaseIterable {
    /// No confirmed coverage at all — analysis has not produced any
    /// ranges yet. Library cell renders no checkmark.
    case none
    /// Coverage exists but none of the ranges contains the readiness
    /// anchor's 15-minute look-ahead window. The episode is analyzed
    /// somewhere but NOT near the current playback point. Library cell
    /// renders no checkmark — the user would hit an analysis gap almost
    /// immediately.
    case deferredOnly
    /// Coverage spans `[anchor, anchor + 15min]` continuously. The user
    /// can start (or resume) playback at the anchor and the skip
    /// pipeline has ad-windows prepared for the next 15 minutes. Library
    /// cell renders the ✓ affordance.
    case proximal
    /// The entire episode has been analyzed end-to-end. Strongest
    /// possible signal; supersedes `.proximal` (and implies it at every
    /// anchor). Library cell renders the ✓ affordance.
    case complete
}

// MARK: - Derivation

/// Lookahead window, in seconds, that a range must cover past the
/// readiness anchor in order to qualify as `.proximal`. Matches the
/// plan §6 Phase 2 specification ("15 min").
///
/// Exposed as an internal constant so unit tests can reference the same
/// number rather than hardcoding `900` in multiple places.
let playbackReadinessProximalLookaheadSeconds: TimeInterval = 15 * 60

/// Pure function deriving playback-readiness from the supplied coverage
/// and anchor. Every Library / NowPlaying / Activity surface MUST route
/// through this function — persisting a pre-computed readiness would
/// break the "anchor-relative derived view" contract.
///
/// Rules (per plan §6 Phase 2 spec, playhead-cthe):
///   * `.complete` iff `coverage.isComplete`;
///   * `.proximal` iff some range covers `[anchor, anchor + 15min]`
///     continuously (and `.complete` did not fire);
///   * `.deferredOnly` iff coverage is non-empty but no range is
///     proximal at the anchor;
///   * `.none` iff coverage is nil OR coverage has no ranges.
///
/// A nil anchor cannot produce `.proximal` (there is no point to
/// look ahead from). It can still produce `.complete` (the whole
/// episode is analyzed regardless of anchor) or `.deferredOnly` (some
/// coverage exists but no anchor to evaluate proximity against).
///
/// **End-of-episode contract (for downstream producers — playhead-zp5y,
/// playhead-quh7):** this function does not know the episode duration,
/// so it cannot clamp the lookahead window when the anchor is within
/// `playbackReadinessProximalLookaheadSeconds` of the episode's end.
/// An episode analyzed up to `duration - 60s` with the user at anchor
/// `duration - 120s` will never derive `.proximal` unless the producer
/// sets `coverage.isComplete = true`. **Producers MUST set
/// `isComplete = true` when analysis has covered up to
/// `episodeDuration - playbackReadinessProximalLookaheadSeconds`** — i.e.
/// once the remaining unanalyzed tail is shorter than the lookahead
/// window — otherwise `.proximal` is unreachable near the end of the
/// episode. Out of scope for this function to clamp: adding an
/// `episodeDuration` parameter here would couple the derivation to a
/// field that is not part of the `(coverage, anchor)` pair the spec
/// mandates as the derivation's only inputs.
///
/// - Parameters:
///   - coverage: The persisted coverage record, or `nil` when the
///     episode has never been enqueued for analysis.
///   - anchor: The readiness anchor (seconds from episode start). `nil`
///     when the user has not yet played this episode.
/// - Returns: The derived readiness; see the case docs for UI semantics.
func derivePlaybackReadiness(
    coverage: CoverageSummary?,
    anchor: TimeInterval?
) -> PlaybackReadiness {
    guard let coverage else { return .none }

    if coverage.isComplete {
        return .complete
    }

    if coverage.coverageRanges.isEmpty {
        return .none
    }

    guard let anchor else {
        // Non-empty coverage + nil anchor → we have analyzed segments
        // but no playback target to evaluate proximity against. By
        // spec this is `.deferredOnly`.
        return .deferredOnly
    }

    let lookaheadEnd = anchor + playbackReadinessProximalLookaheadSeconds
    for range in coverage.coverageRanges {
        if range.lowerBound <= anchor && range.upperBound >= lookaheadEnd {
            return .proximal
        }
    }
    return .deferredOnly
}
