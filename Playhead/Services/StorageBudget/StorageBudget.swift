// StorageBudget.swift
// playhead-h7r (Phase 0 Guardrails): storage-budget admission and
// eviction policy spanning three artifact classes.
//
// Responsibilities:
//   1. Enforce two independent caps:
//        media_cap     (user-configurable, default 10 GB) for `media`
//        analysis_cap  (fixed at 200 MB)                  for
//                      `warmResumeBundle` + `scratch`
//   2. Admission control: reject new writes that would exceed the
//      relevant cap, pre-start (not mid-flight). A transfer already in
//      progress is handled by the caller per URLSession delegate
//      checkpoints — this object only reports the decision.
//   3. Simultaneous-cap eviction order: when both caps are exceeded,
//      evict media first, then scratch. `warmResumeBundle` is NEVER
//      directly evicted; its footprint is constrained indirectly by the
//      ratio invariant.
//   4. Warm-resume ratio invariant: on media eviction, retained
//      `warmResumeBundle` bytes must be ≤ 1% of deleted media bytes.
//      When violated, admission of new warmResumeBundle writes is
//      paused until the next eviction cycle restores balance. Ratio
//      violations are reported via ``StorageBudgetAudit`` so future
//      instrumentation (secondary KPI `warm_resume_hit_rate` per
//      playhead-d99) can hook in.
//
// Purity / testability:
//   The enforcer is `actor`-isolated for concurrent safety but takes
//   its storage-size reader and eviction performer as closures, so
//   tests can drive the full decision matrix without a filesystem.
//   This matches the "inject a storage-size accessor and an eviction
//   callback rather than calling FileManager directly in tests" rule.

import Foundation
import OSLog

// MARK: - Caps

/// The default media cap in bytes. 10 GB.
///
/// User-configurable via ``StorageBudgetSettings``. The value is stored
/// in SI bytes (1 GB = 1_000_000_000 bytes), not binary bytes, to match
/// the unit users see in iOS Settings → General → iPhone Storage.
let defaultMediaCapBytes: Int64 = 10 * 1_000_000_000

/// The fixed analysis-artifact cap in bytes. 200 MB. Not user-configurable.
///
/// Covers `warmResumeBundle` + `scratch` together under a single cap;
/// eviction policy within that pool is nuanced (see ``StorageBudget``).
let analysisCapBytes: Int64 = 200 * 1_000_000

/// Maximum retained `warmResumeBundle` bytes expressed as a fraction of
/// deleted media bytes on the most recent eviction cycle. Per the spec:
/// "retained warmResumeBundle size ≤ 1% of deleted media bytes".
let warmResumeToMediaMaxRatio: Double = 0.01

// MARK: - Admission / Eviction decision types

/// Outcome of an admission check for a proposed write against the
/// storage budget. Named `StorageAdmissionDecision` rather than the
/// shorter `AdmissionDecision` to avoid collision with the distinct
/// `AdmissionDecision` type in `AdmissionController` (backfill-job
/// scheduling), which is an unrelated domain concept.
enum StorageAdmissionDecision: Equatable, Sendable {
    /// The write is admitted; caller may proceed.
    case accept

    /// The write is rejected because admitting it would exceed the cap
    /// that governs its artifact class, after eviction credits are
    /// exhausted. Callers should surface this to the user (for media)
    /// or drop the artifact (for scratch/warmResumeBundle).
    case rejectCapExceeded(class: ArtifactClass, cap: Int64, currentBytes: Int64, proposedBytes: Int64)

    /// The write is rejected because the warm-resume ratio invariant
    /// (retained warmResumeBundle ≤ 1% of deleted media) would be
    /// violated. This is a transient pause — the next eviction cycle
    /// may restore admission eligibility.
    case rejectWarmResumeRatioExceeded(retainedBytes: Int64, recentlyEvictedMediaBytes: Int64)
}

/// Audit record emitted on each eviction cycle. Shape is designed to
/// feed future instrumentation (playhead-d99 `warm_resume_hit_rate`
/// secondary KPI pairing) without this bead wiring the emitter itself.
struct StorageBudgetAudit: Equatable, Sendable {
    /// Bytes of media actually deleted this cycle (sum of reported
    /// eviction callback return values).
    let evictedMediaBytes: Int64

    /// Bytes of scratch actually deleted this cycle.
    let evictedScratchBytes: Int64

    /// Retained `warmResumeBundle` bytes AFTER eviction. Compared
    /// against ``evictedMediaBytes`` to compute the ratio.
    let retainedWarmResumeBytes: Int64

    /// Retained / evictedMedia ratio. Nil when evictedMediaBytes == 0
    /// (ratio is undefined — no media was evicted this cycle).
    let warmResumeToMediaRatio: Double?

    /// True iff THIS CYCLE actually evaluated the ratio AND the ratio
    /// exceeded ``warmResumeToMediaMaxRatio``. This is strictly a
    /// per-cycle signal: it is `false` whenever the ratio was not
    /// evaluated this cycle (i.e. `evictedMediaBytes == 0`), even if a
    /// latch from a prior cycle is still held. Telemetry consumers
    /// should treat ``ratioExceeded`` as a per-cycle event and
    /// ``latchHeld`` as the persistent admission-pause signal.
    let ratioExceeded: Bool

    /// The persistent state of the warm-resume admission-pause latch
    /// AT THE END OF THIS CYCLE. When `true`, new warmResumeBundle
    /// admissions are rejected until a future cycle clears the latch.
    /// Distinct from ``ratioExceeded``: a cycle that did not evict any
    /// media will report `ratioExceeded == false` even while
    /// `latchHeld == true` if a prior cycle's breach is still active.
    let latchHeld: Bool
}

// MARK: - StorageBudget

/// The storage-budget enforcer. See file-level docs for the full
/// responsibility contract.
///
/// Pluggable collaborators:
///   - `sizeProvider`: given an ``ArtifactClass``, return the current
///     total on-disk size (bytes) of that class. Called once per cap
///     check; do not cache across the call.
///   - `evictor`: given a class and a target byte budget, delete up to
///     that many bytes (LRU within the class) and return the ACTUAL
///     bytes freed. Return `0` if nothing was evictable.
actor StorageBudget {
    typealias SizeProvider = @Sendable (ArtifactClass) -> Int64
    typealias Evictor = @Sendable (ArtifactClass, Int64) -> Int64
    typealias AuditSink = @Sendable (StorageBudgetAudit) -> Void

    private let logger = Logger(subsystem: "com.playhead", category: "StorageBudget")

    private let mediaCap: Int64
    private let analysisCap: Int64
    private let sizeProvider: SizeProvider
    private let evictor: Evictor
    private let auditSink: AuditSink?

    /// Ratio-breach latch. When set, new `warmResumeBundle` admissions
    /// are rejected until the next ``enforceCaps()`` cycle clears the
    /// flag. Cleared automatically at the end of any cycle whose audit
    /// reports `ratioExceeded == false`.
    private var warmResumeAdmissionPaused: Bool = false

    init(
        mediaCap: Int64 = defaultMediaCapBytes,
        analysisCap: Int64 = analysisCapBytes,
        sizeProvider: @escaping SizeProvider,
        evictor: @escaping Evictor,
        auditSink: AuditSink? = nil
    ) {
        self.mediaCap = mediaCap
        self.analysisCap = analysisCap
        self.sizeProvider = sizeProvider
        self.evictor = evictor
        self.auditSink = auditSink
    }

    /// The cap that governs an artifact class. `media` is governed by
    /// ``mediaCap``; `warmResumeBundle` and `scratch` share
    /// ``analysisCap``.
    func cap(for cls: ArtifactClass) -> Int64 {
        switch cls {
        case .media: return mediaCap
        case .warmResumeBundle, .scratch: return analysisCap
        }
    }

    /// Current bytes attributed to an artifact class (via injected
    /// `sizeProvider`).
    func currentBytes(for cls: ArtifactClass) -> Int64 {
        sizeProvider(cls)
    }

    /// Current bytes attributed to the cap that governs `cls`. For
    /// `media`, this is just the media size. For the two analysis
    /// classes (`warmResumeBundle`, `scratch`), this is the SUM of
    /// both because they share the single `analysisCap`.
    ///
    /// Admission uses the governed-cap total (not per-class), so a
    /// scratch write against a full analysis pool (where the bulk is
    /// already warmResumeBundle) is correctly rejected.
    private func currentBytesGovernedByCap(of cls: ArtifactClass) -> Int64 {
        switch cls {
        case .media:
            return sizeProvider(.media)
        case .warmResumeBundle, .scratch:
            // Saturating add: if the analysis pool somehow already
            // exceeds Int64.max in aggregate, clamp to Int64.max so the
            // subsequent cap comparison correctly reports cap-exceeded
            // rather than wrapping to a negative value.
            let warm = sizeProvider(.warmResumeBundle)
            let scratch = sizeProvider(.scratch)
            let (sum, overflow) = warm.addingReportingOverflow(scratch)
            return overflow ? Int64.max : sum
        }
    }

    /// True iff the last eviction cycle observed a warm-resume-to-media
    /// ratio > ``warmResumeToMediaMaxRatio``. While set, new
    /// warmResumeBundle writes are rejected by ``admit(class:sizeBytes:)``.
    var isWarmResumeAdmissionPaused: Bool {
        warmResumeAdmissionPaused
    }

    // MARK: - Admission

    /// Admission check for a proposed write of `sizeBytes` into `cls`.
    /// See ``StorageAdmissionDecision``.
    ///
    /// Independence invariant: a media-class admission check considers
    /// ONLY the media cap, and an analysis-class admission check
    /// considers ONLY the analysis cap. The two caps do not interact at
    /// admission time — eviction is the only operation that can span
    /// both.
    func admit(class cls: ArtifactClass, sizeBytes: Int64) -> StorageAdmissionDecision {
        // Cycle-3 hardening: a negative `sizeBytes` is a caller bug.
        // Without this guard, `current + sizeBytes` would be smaller
        // than `current` and the admission check would silently accept
        // a write that the caller has clearly mis-described. Fail fast
        // so the bug surfaces at the call site rather than in a
        // confusing downstream cap-budget mystery.
        precondition(sizeBytes >= 0, "StorageBudget.admit: sizeBytes must be non-negative; got \(sizeBytes)")

        // warmResumeBundle admission is gated on the ratio latch first;
        // a cap-available bundle still gets rejected if the previous
        // eviction cycle reported a ratio breach.
        if cls == .warmResumeBundle, warmResumeAdmissionPaused {
            let retained = sizeProvider(.warmResumeBundle)
            return .rejectWarmResumeRatioExceeded(
                retainedBytes: retained,
                recentlyEvictedMediaBytes: 0  // audit history not persisted in this bead
            )
        }

        let cap = cap(for: cls)
        let current = currentBytesGovernedByCap(of: cls)
        // Checked addition: an overflow on `current + sizeBytes` means
        // the admission would (mathematically) blow past Int64.max,
        // which is by definition past any sane cap — treat overflow as
        // cap-exceeded rather than wrapping to a negative `projected`
        // that would silently accept the write.
        let (projected, overflow) = current.addingReportingOverflow(sizeBytes)
        if overflow || projected > cap {
            return .rejectCapExceeded(
                class: cls,
                cap: cap,
                currentBytes: current,
                proposedBytes: sizeBytes
            )
        }
        return .accept
    }

    // MARK: - Eviction

    /// Run one eviction cycle. Evicts enough bytes to bring each
    /// exceeded cap back under its limit, in order:
    ///   1. Media (LRU within media).
    ///   2. Scratch (LRU within scratch, only if analysis cap is over).
    ///   3. warmResumeBundle is NEVER directly evicted here.
    ///
    /// After evictions run, the warm-resume ratio is checked against
    /// the media bytes deleted this cycle; a breach latches
    /// ``warmResumeAdmissionPaused`` true until the next cycle clears it.
    ///
    /// Emits a ``StorageBudgetAudit`` to the `auditSink` (if provided)
    /// regardless of whether anything was evicted — the audit log is
    /// the authoritative record the ratio check reads from.
    @discardableResult
    func enforceCaps() -> StorageBudgetAudit {
        // Media-cap enforcement (class 1: direct LRU).
        let mediaBefore = sizeProvider(.media)
        var evictedMedia: Int64 = 0
        if mediaBefore > mediaCap {
            let target = mediaBefore - mediaCap
            let evictedRaw = evictor(.media, target)
            // Cycle-3 hardening: the evictor contract guarantees a
            // non-negative return (bytes actually freed). A negative
            // value would underflow the ratio math (Double conversion
            // gives a meaningless ratio) and could quietly hide a bug
            // in a real evictor. Clamp to 0 in release, assert in
            // DEBUG so tests catch a bad evictor immediately.
            if evictedRaw < 0 {
                logger.error("StorageBudget: media evictor returned negative \(evictedRaw, privacy: .public); clamping to 0")
                #if DEBUG
                assertionFailure("evictor must return non-negative bytes; got \(evictedRaw)")
                #endif
                evictedMedia = 0
            } else {
                evictedMedia = evictedRaw
            }
            // Do not assert evictor honored the full target — a partial
            // eviction is legal (e.g. in-use files refuse deletion). The
            // next cycle will retry.
        }

        // Analysis-cap enforcement (class 2/3: scratch is evictable,
        // warmResumeBundle is NOT). We evaluate the combined analysis
        // pool size; if it's over, we spill scratch only. Saturating
        // add on the pool sum so a hypothetical Int64-overflow ledger
        // still triggers cap enforcement instead of wrapping negative.
        let warmBefore = sizeProvider(.warmResumeBundle)
        let scratchBefore = sizeProvider(.scratch)
        let (poolSum, poolOverflow) = warmBefore.addingReportingOverflow(scratchBefore)
        let analysisBefore = poolOverflow ? Int64.max : poolSum
        var evictedScratch: Int64 = 0
        if analysisBefore > analysisCap {
            let target = analysisBefore - analysisCap
            let evictedRaw = evictor(.scratch, target)
            // Cycle-3 hardening (mirrors media branch): clamp negative
            // evictor returns to 0 in release, assert in DEBUG.
            if evictedRaw < 0 {
                logger.error("StorageBudget: scratch evictor returned negative \(evictedRaw, privacy: .public); clamping to 0")
                #if DEBUG
                assertionFailure("evictor must return non-negative bytes; got \(evictedRaw)")
                #endif
                evictedScratch = 0
            } else {
                evictedScratch = evictedRaw
            }
        }

        // Warm-resume ratio check. Use the fresh size — the evictor may
        // have removed warmResumeBundle bytes indirectly (unlikely but
        // not forbidden by the protocol).
        let warmAfter = sizeProvider(.warmResumeBundle)
        let ratio: Double?
        // `ratioExceeded` is strictly per-cycle: only `true` when this
        // cycle actually evaluated the ratio (i.e. evicted some media)
        // AND the ratio exceeded the cap. Persistent latch state is
        // tracked separately.
        let perCycleBreach: Bool
        if evictedMedia > 0 {
            let r = Double(warmAfter) / Double(evictedMedia)
            ratio = r
            perCycleBreach = r > warmResumeToMediaMaxRatio
        } else {
            // Ratio undefined when no media was evicted this cycle.
            ratio = nil
            perCycleBreach = false
        }

        // Latch transition rules:
        //   - A per-cycle breach SETS the latch.
        //   - A per-cycle non-breach (with a real ratio measurement)
        //     CLEARS the latch — this is the canonical "balance
        //     restored" signal.
        //   - A no-pressure cycle (mediaBefore <= mediaCap AND
        //     warmAfter == 0) CLEARS the latch — covers the idle
        //     recovery path where warm bundles drained externally
        //     (manual deletion, app reset, no new podcast downloads).
        //     Without this, a user whose system stops generating media
        //     pressure is permanently stuck with admission rejected.
        //   - Otherwise, the latch carries forward unchanged.
        if evictedMedia > 0 {
            warmResumeAdmissionPaused = perCycleBreach
        } else if mediaBefore <= mediaCap, warmAfter == 0 {
            warmResumeAdmissionPaused = false
        }
        // else: no eviction happened but warm bundles still exist or
        // media is still over cap — preserve prior latch state.

        let latchHeld = warmResumeAdmissionPaused

        let audit = StorageBudgetAudit(
            evictedMediaBytes: evictedMedia,
            evictedScratchBytes: evictedScratch,
            retainedWarmResumeBytes: warmAfter,
            warmResumeToMediaRatio: ratio,
            ratioExceeded: perCycleBreach,
            latchHeld: latchHeld
        )
        // L4: log BEFORE the audit sink so log-correlated telemetry
        // observes the same ordering as the audit pipeline.
        if perCycleBreach {
            logger.error(
                """
                Warm-resume ratio breach: retained=\(warmAfter, privacy: .public) bytes, \
                evictedMedia=\(evictedMedia, privacy: .public) bytes, \
                ratio=\(ratio ?? 0, privacy: .public), \
                cap=\(warmResumeToMediaMaxRatio, privacy: .public) — \
                pausing warmResumeBundle admission
                """
            )
        }
        if let sink = auditSink {
            sink(audit)
        }
        return audit
    }

    // MARK: - Test seams

    #if DEBUG
    /// Test-only: clear the ratio-breach latch to simplify test setup.
    func resetWarmResumeAdmissionPauseForTesting() {
        warmResumeAdmissionPaused = false
    }
    #endif
}
