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
        /// playhead-xsdz.36 (R2 failure-state policy): backoff applied AFTER a
        /// failure has been confirmed deterministic (see
        /// `deterministicConfirmationCount`), indexed by the number of
        /// deterministic retries already made and clamped to the last entry.
        /// Defaults 1d → 3d → 7d.
        var deterministicFailureBackoffSchedule: [TimeInterval]
        /// A failure becomes DETERMINISTIC when the SAME terminal
        /// `FailureClass` occurs this many times consecutively (default 2 —
        /// "same terminal error class twice consecutively"). Transient
        /// failures and successful pre-checks reset the streak.
        var deterministicConfirmationCount: Int
        /// After a failure is confirmed deterministic, at most this many
        /// backoff-gated retries are made (default 3 — one per entry of the
        /// default deterministic backoff schedule). If all of them fail with
        /// the same class, the episode is PARKED (terminal until the state
        /// row is cleared).
        var maxDeterministicAttempts: Int
        /// playhead-xsdz.36.2 (k-way): how many DISTINCT-persona B-side re-fetches
        /// the sweep performs per rotated candidate. `1` (the default) is the
        /// single-fetch status quo — byte-identical to xsdz.36. K≥3 fans out over
        /// the curated persona bank (iPhone → Mac → Overcast → empty) and UNIONS
        /// the pairwise byte diffs so a pod one fetch-pair misses (both draws
        /// landed the same low-entropy fill) is recovered from another persona's
        /// distinct stitch — at K× the re-fetch bandwidth. The PRODUCTION value
        /// is the single flippable `RediffActivation.productionKWayFetchCount`
        /// (conservatively 1); this knob lets tests exercise K≥2. Effectively
        /// capped at the curated bank size (see `RediffFetchPersona.kWayPersonas`).
        var kWayFetchCount: Int

        static let secondsPerDay: TimeInterval = 24 * 60 * 60

        static let `default` = Configuration(
            minimumAgeBeforeFirstAttempt: secondsPerDay,
            backoffSchedule: [secondsPerDay, 2 * secondsPerDay, 4 * secondsPerDay],
            maxUnchangedAttempts: 3,
            headSampleBytes: 64 * 1024,
            tailSampleBytes: 64 * 1024
        )

        /// playhead-xsdz.36 ACTIVATION preset: identical to `.default` except
        /// the first-attempt delay is ~3 DAYS, per the xsdz.30 days-gap
        /// measurement — rotation coverage saturates by ~3d (87% at BOTH ~3d
        /// and ~8d), so waiting 3d costs no coverage and skips the 1d/2d
        /// re-check waste the 24h default would spend on slow rotators.
        static let production = Configuration(
            minimumAgeBeforeFirstAttempt: 3 * secondsPerDay
        )

        init(
            minimumAgeBeforeFirstAttempt: TimeInterval = secondsPerDay,
            backoffSchedule: [TimeInterval] = [secondsPerDay, 2 * secondsPerDay, 4 * secondsPerDay],
            maxUnchangedAttempts: Int = 3,
            headSampleBytes: Int = 64 * 1024,
            tailSampleBytes: Int = 64 * 1024,
            deterministicFailureBackoffSchedule: [TimeInterval] = [secondsPerDay, 3 * secondsPerDay, 7 * secondsPerDay],
            deterministicConfirmationCount: Int = 2,
            maxDeterministicAttempts: Int = 3,
            kWayFetchCount: Int = 1
        ) {
            precondition(!backoffSchedule.isEmpty, "backoffSchedule must be non-empty")
            precondition(maxUnchangedAttempts >= 1, "maxUnchangedAttempts must be ≥ 1")
            precondition(headSampleBytes > 0 && tailSampleBytes > 0, "sample sizes must be > 0")
            precondition(!deterministicFailureBackoffSchedule.isEmpty, "deterministicFailureBackoffSchedule must be non-empty")
            precondition(deterministicConfirmationCount >= 1, "deterministicConfirmationCount must be ≥ 1")
            precondition(maxDeterministicAttempts >= 1, "maxDeterministicAttempts must be ≥ 1")
            precondition(kWayFetchCount >= 1, "kWayFetchCount must be ≥ 1")
            self.minimumAgeBeforeFirstAttempt = minimumAgeBeforeFirstAttempt
            self.backoffSchedule = backoffSchedule
            self.maxUnchangedAttempts = maxUnchangedAttempts
            self.headSampleBytes = headSampleBytes
            self.tailSampleBytes = tailSampleBytes
            self.deterministicFailureBackoffSchedule = deterministicFailureBackoffSchedule
            self.deterministicConfirmationCount = deterministicConfirmationCount
            self.maxDeterministicAttempts = maxDeterministicAttempts
            self.kWayFetchCount = kWayFetchCount
        }
    }

    // MARK: - Failure classification (playhead-xsdz.36, the xsdz.28 R2 decision)

    /// Terminal-vs-transient classification of a per-candidate failure.
    ///
    /// TRANSIENT (network drop, cancellation, non-410/404 HTTP, unknown
    /// pre-fetch errors) → retry at the next eligible sweep, unlimited — the
    /// common case; the sweep itself is already WiFi+charging gated.
    ///
    /// TERMINAL classes are the ones that, when they repeat, indicate the
    /// SAME failure will recur on every attempt (re-spending ~54 MB each
    /// time — the xsdz.28 R2 loop): decode failure, HTTP 404/410, a
    /// fingerprint-mismatch-class result, or invalid local state (asset row
    /// gone / unusable). Two consecutive same-class terminal failures confirm
    /// the failure as DETERMINISTIC → exponential backoff (1d → 3d → 7d),
    /// capped at `maxDeterministicAttempts` retries, then PARK.
    enum FailureClass: String, Sendable, Equatable, CaseIterable {
        /// Network error / cancellation / any unclassified pre-download error.
        case transient
        /// The enclosure is gone: HTTP 404 or 410 (on the pre-check OR fetch).
        case resourceGone = "resource_gone"
        /// The fetched B-copy could not be decoded/fingerprinted.
        case decodeFailure = "decode_failure"
        /// The B-side produced an empty/unusable fingerprint stream.
        case fingerprintMismatch = "fingerprint_mismatch"
        /// Local state required to consume the B-side is missing or invalid
        /// (e.g. the analysis-asset row disappeared under the candidate).
        case staleAsset = "stale_asset"

        var isTerminal: Bool { self != .transient }
    }

    /// Which stage of `processCandidate` the failure escaped from — the
    /// fallback discriminator when the error TYPE alone is ambiguous: an
    /// unknown error before the full fetch is cheap to retry (transient); an
    /// unknown error after the ~54 MB download is treated as a decode-class
    /// failure so a deterministic decoder loop cannot re-spend the fetch
    /// forever.
    enum FailureStage: Sendable, Equatable {
        /// Ranged pre-check / local sample.
        case precheck
        /// The full ~54 MB re-fetch.
        case fetch
        /// B-side fingerprint / rediff-pass consumption (download succeeded).
        case postDownload
    }

    /// Classify a thrown error into a `FailureClass`. Pure given
    /// (error, stage); precedence: explicit conformance
    /// (`RediffFailureClassifiable`) → cancellation → known network shapes →
    /// stage fallback.
    static func classifyFailure(_ error: any Error, stage: FailureStage) -> FailureClass {
        if let classified = error as? any RediffFailureClassifiable {
            return classified.rediffFailureClass
        }
        if error is CancellationError { return .transient }
        if error is URLError { return .transient }
        if case URLSessionFullEpisodeFetcher.FetchError.notOK(let status) = error {
            return (status == 404 || status == 410) ? .resourceGone : .transient
        }
        if case URLSessionRangedAudioSampler.SampleError.notPartialContent(let status) = error {
            return (status == 404 || status == 410) ? .resourceGone : .transient
        }
        switch stage {
        case .precheck, .fetch: return .transient
        case .postDownload: return .decodeFailure
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
        /// playhead-xsdz.36 (R2): class of the most recent FAILED attempt, or
        /// `nil` when the last attempt did not fail (never attempted /
        /// unchanged / rotated). `.transient` is stored too so eligibility can
        /// distinguish "last attempt failed transiently → retry next sweep"
        /// from "last attempt was an unchanged pre-check → unchanged backoff".
        var lastFailureClass: FailureClass?
        /// playhead-xsdz.36 (R2): count of CONSECUTIVE failures with the same
        /// terminal `lastFailureClass`. 0 for transient failures and after any
        /// non-failure attempt. Drives deterministic confirmation
        /// (`>= deterministicConfirmationCount`), the deterministic backoff
        /// index, and parking.
        var sameClassFailureStreak: Int

        static let initial = AttemptState(unchangedAttempts: 0, lastAttemptAt: nil, resolved: false)

        init(
            unchangedAttempts: Int = 0,
            lastAttemptAt: Double? = nil,
            resolved: Bool = false,
            lastFailureClass: FailureClass? = nil,
            sameClassFailureStreak: Int = 0
        ) {
            self.unchangedAttempts = unchangedAttempts
            self.lastAttemptAt = lastAttemptAt
            self.resolved = resolved
            self.lastFailureClass = lastFailureClass
            self.sameClassFailureStreak = sameClassFailureStreak
        }
    }

    /// PARKED: a deterministic failure exhausted its retry budget — the
    /// confirmation failures (`deterministicConfirmationCount`) plus all
    /// `maxDeterministicAttempts` backoff-gated retries failed with the SAME
    /// terminal class. Terminal for the sweep (surfaced as
    /// `.parkedDeterministicFailure`); clearing the persisted state row is the
    /// only un-park path.
    static func isParked(_ state: AttemptState, config: Configuration = .default) -> Bool {
        guard let cls = state.lastFailureClass, cls.isTerminal else { return false }
        return state.sameClassFailureStreak >= config.deterministicConfirmationCount + config.maxDeterministicAttempts
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
        /// playhead-xsdz.36 (R2): a CONFIRMED deterministic failure's
        /// exponential backoff (1d → 3d → 7d) has not elapsed yet.
        case deterministicBackoffNotElapsed(nextEligibleAt: Double)
        /// playhead-xsdz.36 (R2): deterministic-failure retry budget exhausted
        /// — the episode is parked (terminal; surfaced in diagnostics).
        case parkedDeterministicFailure
    }

    /// Decide whether `downloadedAt`'s episode is due for a re-fetch at `now`.
    static func eligibility(
        now: Double,
        downloadedAt: Double,
        state: AttemptState,
        config: Configuration = .default
    ) -> Eligibility {
        if state.resolved { return .alreadyResolved }
        if isParked(state, config: config) { return .parkedDeterministicFailure }
        if state.unchangedAttempts >= config.maxUnchangedAttempts { return .retryBudgetExhausted }

        // CONFIRMED deterministic failure → exponential backoff, indexed by
        // the number of post-confirmation retries already made.
        if let cls = state.lastFailureClass, cls.isTerminal,
           state.sameClassFailureStreak >= config.deterministicConfirmationCount,
           let lastAttemptAt = state.lastAttemptAt {
            let index = min(
                state.sameClassFailureStreak - config.deterministicConfirmationCount,
                config.deterministicFailureBackoffSchedule.count - 1
            )
            let nextEligibleAt = lastAttemptAt + config.deterministicFailureBackoffSchedule[max(0, index)]
            return now >= nextEligibleAt
                ? .eligible
                : .deterministicBackoffNotElapsed(nextEligibleAt: nextEligibleAt)
        }

        guard let lastAttemptAt = state.lastAttemptAt else {
            // First-ever attempt: the ≥24h download-age gate.
            let age = now - downloadedAt
            return age >= config.minimumAgeBeforeFirstAttempt
                ? .eligible
                : .tooSoonSinceDownload(ageSeconds: age)
        }

        // Last attempt FAILED but is not (yet) a confirmed deterministic
        // failure: transient failures and a first terminal-class failure both
        // retry at the very next eligible sweep — no backoff (the R2 policy's
        // "retry next eligible sweep, unlimited" arm).
        if state.lastFailureClass != nil { return .eligible }

        // Subsequent attempt after an UNCHANGED pre-check: honor the backoff
        // since the last attempt.
        let index = min(state.unchangedAttempts - 1, config.backoffSchedule.count - 1)
        let backoff = config.backoffSchedule[max(0, index)]
        let nextEligibleAt = lastAttemptAt + backoff
        return now >= nextEligibleAt ? .eligible : .backoffNotElapsed(nextEligibleAt: nextEligibleAt)
    }

    /// Advance the state after an UNCHANGED pre-check (no rotation): bump the
    /// attempt count and stamp the attempt time so the next backoff is measured
    /// from here. A successful attempt also RESETS the failure streak — the
    /// R2 policy's "twice consecutively" requirement.
    static func advanceUnchanged(_ state: AttemptState, at now: Double) -> AttemptState {
        AttemptState(
            unchangedAttempts: state.unchangedAttempts + 1,
            lastAttemptAt: now,
            resolved: false
        )
    }

    /// Advance the state after a DETECTED rotation (B-side fingerprinted):
    /// terminal — no further re-fetch for this episode. Failure fields reset.
    static func markResolved(_ state: AttemptState, at now: Double) -> AttemptState {
        AttemptState(
            unchangedAttempts: state.unchangedAttempts,
            lastAttemptAt: now,
            resolved: true
        )
    }

    /// playhead-xsdz.36 (R2): advance the state after a FAILED attempt.
    ///
    ///   * `.transient` → streak resets to 0 (also breaking any in-progress
    ///     terminal chain: "same class twice CONSECUTIVELY"), retried at the
    ///     next eligible sweep, unlimited.
    ///   * terminal class equal to the previous failure's class → streak + 1.
    ///   * terminal class different from the previous → streak restarts at 1.
    ///
    /// The streak drives everything downstream: `>= confirmationCount` puts
    /// the episode into the deterministic backoff regime;
    /// `>= confirmationCount + maxDeterministicAttempts` parks it.
    static func advanceFailed(
        _ state: AttemptState,
        failureClass: FailureClass,
        at now: Double
    ) -> AttemptState {
        let streak: Int
        if !failureClass.isTerminal {
            streak = 0
        } else if state.lastFailureClass == failureClass {
            streak = state.sameClassFailureStreak + 1
        } else {
            streak = 1
        }
        return AttemptState(
            unchangedAttempts: state.unchangedAttempts,
            lastAttemptAt: now,
            resolved: false,
            lastFailureClass: failureClass,
            sameClassFailureStreak: streak
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
        /// DELETED. `fingerprintCount` is the B-side subfingerprint count
        /// (0 when a `RediffBSideConsuming` handoff ran instead of the
        /// standalone fingerprint — the rediff pass consumed the bytes).
        case rotated(assetId: String, cost: BandwidthCost, fingerprintCount: Int, newState: AttemptState)
        /// A network/decode/consume error aborted this candidate. Bytes
        /// already spent are still accounted; the B-copy (if any) was still
        /// deleted. Carries the R2 `failureClass` and the ADVANCED state so
        /// the recorder persists the failure streak (previously `.failed`
        /// carried no state — the xsdz.28 R2 no-backoff loop).
        case failed(assetId: String, cost: BandwidthCost, failureClass: FailureClass, newState: AttemptState, error: String)
    }
}

// MARK: - Failure-class conformance seam

/// Errors that know their own rediff failure class (e.g. B-side consume
/// errors thrown by the production `RediffBSideConsuming` conformer) declare
/// it via this protocol; `RediffRefetchPolicy.classifyFailure` honors it
/// before any type/stage-based fallback.
protocol RediffFailureClassifiable: Error {
    var rediffFailureClass: RediffRefetchPolicy.FailureClass { get }
}
