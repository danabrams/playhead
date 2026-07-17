// RediffRefetchPolicy.swift
// playhead-xsdz.28: the PURE, deterministic decision core for the rediff
// RE-FETCH policy — the production shadow mechanism that periodically re-fetches
// an episode's audio so the rediff width-oracle (xsdz.29 `RediffSlotOwnership`)
// can diff the played copy (A) against a fresh copy (B) and extract exact DAI
// slot boundaries. THIS file is offline/pure; the `RediffRefetchService` actor
// wires it to the BGTask scheduler and the network/file seams (default OFF).
//
// EVERYTHING here is derived from the xsdz.16 spike's MEASURED constraints
// (docs/xsdz16-rediff-spike.md); the numbers are NOT re-derived:
//
//   • ≥24h GATE (spike §4): rotation is 0/10 back-to-back, 2/10 an hour apart,
//     and only saturates (~85-90%) at a weeks gap. Re-fetching an episode
//     downloaded < ~24h ago is measured waste, so the FIRST attempt waits
//     `minimumAgeBeforeFirstAttempt` (24h) after download.
//   • ROTATION PRE-CHECK = ranged-GET head/tail sample compare (spike §5
//     Strategy C, §7): HEAD/ETag/conditional-GET are UNRELIABLE on real podcast
//     CDNs (podtrac length-flaps seconds apart; Acast HEAD returns
//     `content-length: 2`). So change-detection is a byte SAMPLE compare — head
//     64KB + tail 64KB + total length (the total read from a ranged GET's
//     `Content-Range`, NEVER from HEAD). Identical sample → very likely
//     unchanged → SKIP the full fetch. Any difference → full re-fetch. Sizes
//     changed in 10/10 observed rotations, so a mid-file-only rotation with an
//     identical head/tail/length is the (low) residual risk the periodic
//     unconditional retry budget bounds.
//   • BACKOFF + RETRY BUDGET (spike §6.2): on an UNCHANGED pre-check, back off
//     (1d → 2d → 4d) and give up after `maxUnchangedAttempts` (baked-in-only
//     shows never rotate, so re-fetching them nightly forever is pure waste).
//     A detected rotation is TERMINAL (`resolved`) — the width oracle has its
//     B-side; no further re-fetch is scheduled for that episode.
//
// PURITY: value types + `static` functions, `Foundation`/`CryptoKit` only,
// deterministic, no I/O, no time source (the caller passes `now`), no actor
// hops. The whole decision surface is unit-testable without a network or a
// device.

import CryptoKit
import Foundation

enum RediffRefetchPolicy {

    // MARK: - Configuration

    /// The measured, TUNABLE policy knobs. Every default is the spike's value.
    struct Configuration: Sendable, Equatable {
        /// Minimum age (seconds since download) before the FIRST re-fetch
        /// attempt. Spike §4: rotation is measured-zero back-to-back and only
        /// meaningful at ≥ ~24h, so re-fetching earlier is waste.
        var minimumAgeBeforeFirstAttempt: TimeInterval
        /// Wait AFTER the Nth unchanged pre-check before the (N+1)th, indexed by
        /// `unchangedAttempts - 1` and clamped to the last entry. Spike §6.2:
        /// 1 day → 2 days → 4 days. The trailing entry is the clamp for any
        /// raised `maxUnchangedAttempts`; with the default cap of 3 the
        /// effective gaps are 1d then 2d.
        var backoffSchedule: [TimeInterval]
        /// Give up after this many UNCHANGED pre-check attempts (baked-in-only
        /// shows never rotate). Spike §6.2: ~3.
        var maxUnchangedAttempts: Int
        /// Head sample size for the Strategy-C pre-check. Spike §5: 64 KB.
        var headSampleBytes: Int
        /// Tail sample size for the Strategy-C pre-check. Spike §5: 64 KB.
        var tailSampleBytes: Int

        static let secondsPerDay: TimeInterval = 24 * 60 * 60

        static let `default` = Configuration(
            minimumAgeBeforeFirstAttempt: secondsPerDay,
            backoffSchedule: [secondsPerDay, 2 * secondsPerDay, 4 * secondsPerDay],
            maxUnchangedAttempts: 3,
            headSampleBytes: 64 * 1024,
            tailSampleBytes: 64 * 1024
        )

        init(
            minimumAgeBeforeFirstAttempt: TimeInterval = secondsPerDay,
            backoffSchedule: [TimeInterval] = [secondsPerDay, 2 * secondsPerDay, 4 * secondsPerDay],
            maxUnchangedAttempts: Int = 3,
            headSampleBytes: Int = 64 * 1024,
            tailSampleBytes: Int = 64 * 1024
        ) {
            precondition(!backoffSchedule.isEmpty, "backoffSchedule must be non-empty")
            precondition(maxUnchangedAttempts >= 1, "maxUnchangedAttempts must be ≥ 1")
            precondition(headSampleBytes > 0 && tailSampleBytes > 0, "sample sizes must be > 0")
            self.minimumAgeBeforeFirstAttempt = minimumAgeBeforeFirstAttempt
            self.backoffSchedule = backoffSchedule
            self.maxUnchangedAttempts = maxUnchangedAttempts
            self.headSampleBytes = headSampleBytes
            self.tailSampleBytes = tailSampleBytes
        }
    }

    // MARK: - Per-episode bookkeeping

    /// The durable re-fetch state for one episode. The service loads this from
    /// its enumerator and writes the advanced state back through its recorder;
    /// the policy only computes the NEXT state (pure).
    struct AttemptState: Sendable, Equatable {
        /// Count of prior UNCHANGED pre-check attempts (rotation not yet found).
        var unchangedAttempts: Int
        /// Unix seconds of the last attempt, or `nil` if never attempted.
        var lastAttemptAt: Double?
        /// `true` once a rotation was detected AND the B-side fingerprinted —
        /// TERMINAL: no further re-fetch is scheduled for this episode.
        var resolved: Bool

        static let initial = AttemptState(unchangedAttempts: 0, lastAttemptAt: nil, resolved: false)

        init(unchangedAttempts: Int = 0, lastAttemptAt: Double? = nil, resolved: Bool = false) {
            self.unchangedAttempts = unchangedAttempts
            self.lastAttemptAt = lastAttemptAt
            self.resolved = resolved
        }
    }

    /// Why an episode is or is not due for a re-fetch attempt right now. Each
    /// non-eligible reason is distinct so the service breadcrumb / dogfood eval
    /// can size every skip class.
    enum Eligibility: Sendable, Equatable {
        /// Due now — run the pre-check.
        case eligible
        /// First attempt gated: downloaded < `minimumAgeBeforeFirstAttempt` ago.
        case tooSoonSinceDownload(ageSeconds: Double)
        /// A prior unchanged attempt's backoff has not elapsed yet.
        case backoffNotElapsed(nextEligibleAt: Double)
        /// `maxUnchangedAttempts` unchanged attempts made — give up.
        case retryBudgetExhausted
        /// A rotation was already found + fingerprinted — terminal.
        case alreadyResolved
    }

    /// Decide whether `downloadedAt`'s episode is due for a re-fetch at `now`.
    static func eligibility(
        now: Double,
        downloadedAt: Double,
        state: AttemptState,
        config: Configuration = .default
    ) -> Eligibility {
        if state.resolved { return .alreadyResolved }
        if state.unchangedAttempts >= config.maxUnchangedAttempts { return .retryBudgetExhausted }

        guard let lastAttemptAt = state.lastAttemptAt else {
            // First-ever attempt: the ≥24h download-age gate.
            let age = now - downloadedAt
            return age >= config.minimumAgeBeforeFirstAttempt
                ? .eligible
                : .tooSoonSinceDownload(ageSeconds: age)
        }

        // Subsequent attempt: honor the backoff since the last attempt.
        let index = min(state.unchangedAttempts - 1, config.backoffSchedule.count - 1)
        let backoff = config.backoffSchedule[max(0, index)]
        let nextEligibleAt = lastAttemptAt + backoff
        return now >= nextEligibleAt ? .eligible : .backoffNotElapsed(nextEligibleAt: nextEligibleAt)
    }

    /// Advance the state after an UNCHANGED pre-check (no rotation): bump the
    /// attempt count and stamp the attempt time so the next backoff is measured
    /// from here.
    static func advanceUnchanged(_ state: AttemptState, at now: Double) -> AttemptState {
        AttemptState(
            unchangedAttempts: state.unchangedAttempts + 1,
            lastAttemptAt: now,
            resolved: false
        )
    }

    /// Advance the state after a DETECTED rotation (B-side fingerprinted):
    /// terminal — no further re-fetch for this episode.
    static func markResolved(_ state: AttemptState, at now: Double) -> AttemptState {
        AttemptState(
            unchangedAttempts: state.unchangedAttempts,
            lastAttemptAt: now,
            resolved: true
        )
    }

    // MARK: - Rotation pre-check (Strategy C)

    /// The cheap change-detection sample for one audio copy: a hash of the head
    /// bytes, a hash of the tail bytes, and the total byte length. Two copies
    /// with an identical sample are very likely the SAME stitch (spike §5).
    ///
    /// NO ETag, NO Last-Modified, NO conditional validator lives here — the
    /// total length is read from a ranged GET's `Content-Range`, never HEAD.
    struct AudioSampleFingerprint: Sendable, Equatable {
        /// Lowercase-hex SHA-256 of the head sample bytes.
        let headHashHex: String
        /// Lowercase-hex SHA-256 of the tail sample bytes.
        let tailHashHex: String
        /// Total byte length of the copy (from `Content-Range` on the remote
        /// side; from the on-disk file length locally).
        let totalLength: Int64
    }

    /// Build a sample fingerprint from raw head/tail bytes + the total length.
    static func sampleFingerprint(head: Data, tail: Data, totalLength: Int64) -> AudioSampleFingerprint {
        AudioSampleFingerprint(
            headHashHex: sha256Hex(head),
            tailHashHex: sha256Hex(tail),
            totalLength: totalLength
        )
    }

    /// `true` when the remote copy differs from the played (local) copy — i.e.
    /// a full re-fetch is warranted. `false` (identical sample) → SKIP the full
    /// fetch (non-rotator).
    static func isRotated(
        local: AudioSampleFingerprint,
        remote: AudioSampleFingerprint
    ) -> Bool {
        local != remote
    }

    /// Lowercase-hex SHA-256 (matches `EpisodeIdHasher.sha256Hex`).
    static func sha256Hex(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    // MARK: - Bandwidth accounting

    /// Bytes spent on ONE candidate: the pre-check sample (~128 KB) plus the
    /// full re-fetch (~54 MB, spike §5) — `0` when the pre-check skipped it.
    struct BandwidthCost: Sendable, Equatable {
        let precheckBytes: Int
        let fullFetchBytes: Int
        var totalBytes: Int { precheckBytes + fullFetchBytes }

        static let zero = BandwidthCost(precheckBytes: 0, fullFetchBytes: 0)
    }

    // MARK: - Per-candidate outcome

    /// The terminal outcome of processing one candidate in a sweep. Emitted to
    /// the service's recorder so dogfood eval can size every path AND so the
    /// advanced `AttemptState` gets persisted.
    enum Outcome: Sendable, Equatable {
        /// Not due (see `Eligibility`) — no network touched.
        case skippedIneligible(assetId: String, reason: Eligibility)
        /// Pre-check ran; sample identical → full fetch SKIPPED (non-rotator).
        case unchanged(assetId: String, cost: BandwidthCost, newState: AttemptState)
        /// Pre-check differed → full re-fetch + B-side fingerprint + B-copy
        /// DELETED. `fingerprintCount` is the B-side subfingerprint count.
        case rotated(assetId: String, cost: BandwidthCost, fingerprintCount: Int, newState: AttemptState)
        /// A network/decode error aborted this candidate. Bytes already spent
        /// are still accounted; the B-copy (if any) was still deleted.
        case failed(assetId: String, cost: BandwidthCost, error: String)
    }
}
