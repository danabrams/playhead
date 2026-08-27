// SemanticScanResult.swift
// Persistence-facing FM scan result and evidence-event models.
//
// H16 resolved: `decisionCohortJSON` was removed from BackfillJob, the
// backfill_jobs column, and the reuse contract. Decision-time changes never
// invalidate FM scan output, so there is nothing to key on.

import Foundation
import OSLog

private let semanticScanLogger = Logger(
    subsystem: "com.playhead",
    category: "SemanticScanResult"
)

/// Rev3-M5: discriminator for `semantic_scan_results.phase` and
/// `evidence_events.phase`. Phase 3 shadow rows and Phase 5 targeted rows
/// are otherwise indistinguishable in those tables (only differ by
/// `reuseKeyHash`); the explicit phase tag lets queries filter without
/// reverse-engineering the hash inputs.
enum SemanticScanPhase: String, Sendable, Hashable, CaseIterable {
    case shadow
    case targeted
}

/// playhead-hx6n: the app state a semantic scan row was written in.
///
/// **The vocabulary is BORROWED, NOT INVENTED** (playhead-9v09). These four
/// raw values are byte-identical to the strings `BGTaskTelemetryScenePhase`
/// produces and `background_task_runs.scenePhase` already stores, because they
/// come from the same helper. That is what makes a phase split over
/// `semantic_scan_results` directly comparable with one over
/// `background_task_runs` instead of being a second, parallel vocabulary that
/// happens to use similar words.
///
/// Two absences are deliberately kept distinct, and BOTH are unattributed:
///
///   * a `nil` `SemanticScanResult.scenePhase` — no binary ever recorded a
///     phase for this row. Every row written before schema V42 is in this
///     state, permanently and correctly: they are genuinely unattributable and
///     nothing invents a value for them.
///   * `.unknown` — a phase *was* recorded and the platform declined to name
///     it (`@unknown default`, or a non-UIKit host).
///
/// They answer different questions ("is the writer wired up?" versus "is the
/// platform answering?"), so they are not collapsed at rest. They collapse only
/// at the point of ATTRIBUTION, in ``ScanAttributionBucket``, where both are
/// `.unattributed` — see ``attributionBucket``.
enum ScanScenePhase: String, Sendable, Hashable, CaseIterable {
    case active
    case inactive
    case background
    case unknown
}

/// playhead-iw7q (schema V61): WHO produced this row's verdict — the model under
/// its ordinary guardrails, or the permissive bypass whose grade the RUNNER
/// hardcodes.
///
/// # Why this is three-valued and not a `Bool`
///
/// `PermissiveAdGrammar.parse` writes a hardcoded `certainty: .strong` into
/// the `passA` payload and says so in its own header: *"the FM never inferred
/// these classification dimensions, the runner is hardcoding them."* The
/// REFINED half of that fabrication records itself —
/// `makeAnchorlessSpan` stamps `ownershipInferenceWasSuppressed: true` on every
/// span (playhead-92im) — and the COARSE half recorded nothing at all, because
/// `SemanticScanResult.usedPermissiveFallback` existed with **no column** in
/// `semantic_scan_results`. A permissive coarse row was byte-identical at rest
/// to one the model actually produced.
///
/// A two-valued column would have fixed that for rows written from now on and
/// broken it for every row already on disk. There is no way to recover the
/// provenance of a pre-V61 row — that indistinguishability IS the defect — so a
/// migration that seeded `false` would assert *"the model graded this"* about
/// **1,089 rows on the 2026-08-19 t4 pull and 1,406 on the 2026-08-21 t6 pull**
/// that nobody can speak for. That is this bead's own defect, inverted and
/// shipped as a backfill.
///
/// So: **UNKNOWN IS NOT ZERO.** The column is nullable with no default, NULL
/// decodes to ``unknown``, and ``unknown`` is not ``model``.
///
/// # What each case licenses
///
///   * ``model`` — the `@Generable` path answered under Apple's ordinary
///     guardrails. Its `CertaintyBand` is the model's own grade and is the ONLY
///     state that licenses a coarse payload's band
///     (`SemanticSweepMarkComposer.certaintyBand(of:)`).
///   * ``permissive`` — the permissive string path answered, so any band on the
///     payload was written by the runner. A POSITIVE claim, and the only state a
///     telemetry count of permissive rows may include.
///   * ``unknown`` — nothing recorded it. Every pre-V61 row, and any writer with
///     no observation to offer. It licenses nothing in either direction: it is
///     not evidence the model graded the row, and it is not evidence the bypass
///     did.
///
/// There is deliberately no `Bool` bridge OUT of this type and no
/// `init(rawValue:)` taking a boolean. The one way in from an observation is
/// ``init(observedPermissiveFallback:)``, which is named for the observation
/// precisely so that an ABSENCE cannot be spelled as one.
enum ScanVerdictProvenance: String, Sendable, Hashable, CaseIterable {
    case model
    case permissive
    case unknown

    /// From a writer that ACTUALLY OBSERVED which path answered.
    ///
    /// Never reachable from a stored absence — that is ``unknown``, and it is
    /// why this initialiser carries a label rather than being spelled
    /// `init(_:)`. A call site that has to name `observedPermissiveFallback:`
    /// has to have an observation to put in it.
    init(observedPermissiveFallback: Bool) {
        self = observedPermissiveFallback ? .permissive : .model
    }

    /// The persisted spelling: `nil` (SQL NULL) = ``unknown``, `false` =
    /// ``model``, `true` = ``permissive``.
    ///
    /// Optional rather than `Bool` for the same reason `prewarmHit` became
    /// optional at V52: a non-optional column forces a writer with no
    /// observation to claim one.
    var persistedFlag: Bool? {
        switch self {
        case .model: false
        case .permissive: true
        case .unknown: nil
        }
    }

    /// The read half. A NULL column is ``unknown`` — never ``model``.
    static func decoded(persistedFlag: Bool?) -> Self {
        switch persistedFlag {
        case .some(true): .permissive
        case .some(false): .model
        case .none: .unknown
        }
    }

    /// May a `CertaintyBand` carried on a COARSE (`CoarseSupportSchema`)
    /// payload be read as the MODEL'S OWN grade?
    ///
    /// Only for ``model``. The coarse payload has no per-span discriminator to
    /// fall back on — that is the asymmetry with the refined shape, where
    /// `ownershipInferenceWasSuppressed` travels with the span — so this
    /// property is the whole of the record, and silence is not a licence.
    var licensesCoarseCertaintyBand: Bool { self == .model }

    /// Is this row KNOWN to have come from the permissive bypass?
    ///
    /// A positive claim, so ``unknown`` is `false` here as well. The two
    /// predicates are deliberately not each other's negation: a reader that
    /// wanted "how many permissive rows" and got `!licensesCoarseCertaintyBand`
    /// would count every unattributed row as a bypass.
    var isKnownPermissive: Bool { self == .permissive }
}

/// playhead-6gcy (schema V56): the three `semantic_scan_results` columns that
/// hold a row's LATENCY history, and the ONE place an attempt is folded into it.
///
/// This is a value type rather than three lines inside
/// ``AnalysisStore/insertSemanticScanResult(_:now:)`` because the folding rule
/// has a case that is easy to get wrong and impossible to see from a device
/// pull afterwards — see ``folding(attemptLatencyMs:isIdempotentRewrite:)``.
/// A pure function can be driven directly by a test; a branch buried in a
/// 60-line upsert can only be driven through SQLite.
struct SemanticScanLatencyHistory: Sendable, Equatable {
    /// Sum of every MEASURED attempt's `latencyMs`. See
    /// ``SemanticScanResult/latencyMsTotal``.
    let total: Double?
    /// Largest single measured attempt. See ``SemanticScanResult/latencyMsMax``.
    let max: Double?
    /// How many attempts contributed. See
    /// ``SemanticScanResult/latencySampleCount``.
    let sampleCount: Int?

    /// No claim in any direction — a pre-V56 row, or a row none of whose
    /// attempts ever measured itself.
    static let unrecorded = SemanticScanLatencyHistory(total: nil, max: nil, sampleCount: nil)

    /// The history of a row being written for the FIRST time.
    ///
    /// A nil cost yields ``unrecorded`` rather than a zero total: 0 asserts the
    /// attempt was free, nil says nobody measured it, and that distinction is
    /// the one playhead-ejr7 spent a bead establishing for `latencyMs` itself.
    static func first(attemptLatencyMs: Double?) -> Self {
        guard let attemptLatencyMs else { return .unrecorded }
        return Self(total: attemptLatencyMs, max: attemptLatencyMs, sampleCount: 1)
    }

    /// Fold ONE further write of this row into the history.
    ///
    /// **`isIdempotentRewrite` is the case that matters, and it is not a
    /// micro-optimisation.** `BackfillJobRunner.checkpointCoarseProgress` writes
    /// each successfully screened window at the checkpoint AND again in the
    /// end-of-pass digest, carrying the SAME `latencyMs` both times. That is one
    /// attempt written twice, not two attempts — playhead-bg2n established it
    /// for `attemptCount`, and a SUM feels it harder: accumulating
    /// unconditionally would DOUBLE the recorded cost of every checkpointed
    /// success and make `latencySampleCount` say two attempts were timed when
    /// one was. The resulting row is structurally valid and its inflation is
    /// invisible from a pull, which is why the rule lives here with a name
    /// rather than inline with a comment.
    ///
    /// An UNMEASURED attempt (`attemptLatencyMs == nil`) carries the history
    /// forward untouched — exactly the contribution SQL's `SUM` gives a NULL,
    /// and the reason ``SemanticScanResult/latencySampleCount`` can legitimately
    /// trail `attemptCount`.
    func folding(attemptLatencyMs: Double?, isIdempotentRewrite: Bool) -> Self {
        if isIdempotentRewrite {
            // Not a new attempt, so not a new sample. Seed only if nothing is
            // recorded yet, so a pre-V56 row whose only further writes are
            // digests still gains a TRUE one-sample record instead of staying
            // silent forever.
            return sampleCount == nil ? Self.first(attemptLatencyMs: attemptLatencyMs) : self
        }
        guard let attemptLatencyMs else { return self }
        return Self(
            total: (total ?? 0) + attemptLatencyMs,
            max: Swift.max(max ?? attemptLatencyMs, attemptLatencyMs),
            sampleCount: (sampleCount ?? 0) + 1
        )
    }
}

struct SemanticScanResult: Sendable, Equatable {
    let id: String
    let analysisAssetId: String
    let windowFirstAtomOrdinal: Int
    let windowLastAtomOrdinal: Int
    let windowStartTime: Double
    let windowEndTime: Double
    let scanPass: String
    let transcriptQuality: TranscriptQuality
    let disposition: CoarseDisposition
    let spansJSON: String
    let status: SemanticScanStatus
    let attemptCount: Int
    /// playhead-6gcy: LAST-WRITE-WINS, deliberately, and this is the record of
    /// the measurement that decided to leave it that way.
    ///
    /// playhead-bg2n filed `latencyMs` and this column together, and its design
    /// note argued this was the WORSE of the two: "unbounded strings; a set is
    /// not available … where bg2n's rejected per-attempt journal would actually
    /// earn its cost". Measured over every preserved capture — 25 SQLite files
    /// carrying `semantic_scan_results`, collapsing to 7 distinct table states —
    /// all three clauses of that are wrong:
    ///
    ///   * **The strings are short and enumerable.** Every non-NULL value in the
    ///     whole preserved record is the single 20-byte literal
    ///     `noWork:emptySegments`. `min = max = 20` in all seven states. The
    ///     only other vocabulary any writer can produce is
    ///     ``CoarseWindowFailure/oversizeErrorContext``'s
    ///     `oversize:<case> budget=<N>`, whose longest spelling is ~48 bytes.
    ///     The 1 MB cap in `insertSemanticScanResult` bounds the TYPE, not the
    ///     practice.
    ///   * **Zero overwrites have ever been observed.** 18 rows carry a
    ///     non-NULL value in two or more states and not one of them CHANGES.
    ///   * **The cost playhead-hzpa paid was never an overwrite.** Its finding
    ///     was that the failing prompt is unrecoverable — and the reason is that
    ///     nothing ever WROTE this column for a failure. Zero rows with
    ///     `decodingFailure` / `exceededContextWindow` / `inferenceTimeout` /
    ///     `failedTransient` / `cancelled` carry an errorContext at all;
    ///     `inputTokenCount` is NULL on all 961 rows of the 2026-08-15 pull.
    ///     hzpa fixed the WRITER, and its `oversize:` values post-date every
    ///     capture on disk.
    ///
    /// **So "0 of 18 rows changed" is not evidence that overwrites are
    /// harmless — it is evidence that the only value ever written cannot
    /// change,** because one writer emits one constant. The population that
    /// COULD vary has zero rows anywhere. A container designed now would be
    /// designed for a population with no observations, which is the mistake this
    /// file spends most of its comments recording. The next device pull is the
    /// first that can produce one; **playhead-vj89** carries the re-measure and
    /// its trigger.
    let errorContext: String?
    let inputTokenCount: Int?
    let outputTokenCount: Int?
    let latencyMs: Double?
    /// playhead-rkfp (schema V45): `latencyMs`'s twin over the SAME span,
    /// measured on the SUSPENDING clock — device sleep excluded.
    ///
    /// `latencyMs` is a `ContinuousClock` span and includes every second the
    /// process spent frozen between background grants: the 2026-08-06 device
    /// pull's worst row read 1,955.6 s of which 1,504 s was a process freeze,
    /// and one number cannot say so. `latencyMs − suspendingLatencyMs` is the
    /// device-asleep share of the row; it reads 0 when the row never crossed
    /// a sleep, and `nil` means "not measured" (pre-V45 rows, and paths not
    /// yet threaded: shrink-split retries, subdivision, passB).
    let suspendingLatencyMs: Double?
    /// playhead-ezmv (schema V45): how many OTHER in-process Foundation Models
    /// daemon calls were in flight when this row's window attempt began
    /// (`FMDaemonCallCensus`, which counts abandoned-but-unreturned calls too).
    ///
    /// 0 means "no self-contention at this instant" — the honest reading for a
    /// stall that was really a frozen wait or another process's load. `nil`
    /// means "not measured". A lower bound: `prewarm` and non-ad-detection FM
    /// use are not censused.
    let daemonPeersAtStart: Int?
    /// playhead-exxc (schema V52): OPTIONAL, and the optionality is the whole
    /// point — `nil` means NOT MEASURED, exactly as it does for `latencyMs`,
    /// `suspendingLatencyMs` and `daemonPeersAtStart` above.
    ///
    /// **It was `Bool` until V52, and it was never a measurement.** The column
    /// read `0` on all 95 rows of the 2026-08-11 virgin-DB overnight pull and
    /// was quoted as "every FM call paid a cold start". It could not have read
    /// anything else: `prewarmHit` is a field of the PASS-level
    /// ``FMCoarseScanOutput`` / ``FMRefinementScanOutput``, while every row here
    /// is built from a WINDOW-level ``FMCoarseWindowOutput`` /
    /// ``FMRefinementWindowOutput``, which has no such field. So all four
    /// builders in `BackfillJobRunner` passed the literal `false` — the only
    /// value typeable at a site with nothing to read — and `prewarmHit: true`
    /// has never appeared anywhere in `Playhead/` in the repository's history.
    /// `{0: 95}` measured the instrument, not the model.
    ///
    /// A non-nil value therefore means a writer that ACTUALLY OBSERVED whether
    /// the model was warm said so. Nothing in the tree can say that yet, so
    /// every production row is `nil` and the honest reading of this column is
    /// "unestablished" rather than "cold". Do not restore a default: a default
    /// is what made this column claim ninety-five cold starts nobody measured.
    let prewarmHit: Bool?
    let scanCohortJSON: String
    let transcriptVersion: String
    /// Optional stable scope included in persistence reuse hashing so
    /// logically distinct jobs/phases that share the same window bounds do
    /// not collapse each other. Nil preserves legacy reuse semantics.
    ///
    /// **NOT PERSISTED AS ITSELF, and that is deliberate** (playhead-iw7q's
    /// enumeration). It is an INGREDIENT of `reuseKeyHash`, which IS a column,
    /// so it decides row identity without being recoverable from the row. The
    /// value the runner puts here is the `jobId`, and the same `jobId` reaches
    /// disk as `backfillJobId` on every path that goes through
    /// `BackfillJobRunner.attributed(_:jobId:)` — so "which job wrote this row"
    /// is answerable, just not from this property.
    let reuseScope: String?
    /// Rev3-M5 (C4): run-mode discriminator persisted as a real column.
    /// Defaults to `.shadow` so existing call sites stay byte-identical.
    /// Targeted-narrowed rows opt-in by passing `.targeted`. Cycle-8
    /// reconciliation: renamed from `phase` → `runMode` to disambiguate
    /// from B6's `jobPhase` field.
    let runMode: SemanticScanPhase
    /// Cycle 6 B6 Rev3-M6: originating backfill phase (BackfillJobPhase.rawValue)
    /// or the sentinel `"shadow"` for rows persisted by callers that do not yet
    /// attribute phase. Stored in a distinct `jobPhase` column post cycle-8
    /// reconciliation and used by Rev3-M6 tests to verify that harvester
    /// and lexical narrowing phases actually produce strict-subset coverage.
    let jobPhase: String
    // MARK: - The fields with NO COLUMN (playhead-iw7q enumerated them)
    //
    // `insertSemanticScanResult` binds 34 values and `semanticScanResultColumns`
    // reads 34. Of this type's 36 stored properties, THREE reach neither —
    // they exist in memory, travel from the producer to the store, and are
    // dropped at the write:
    //
    //   * `reuseScope`               — folded into `reuseKeyHash` and not
    //                                  recoverable from it. See below: it is a
    //                                  KEY INGREDIENT, not a dropped attribute,
    //                                  and it is the one of the three that is
    //                                  deliberate.
    //   * `refusalExplanation`       — dropped. playhead-807i.
    //   * `permissiveFallbackReason` — dropped. playhead-807i.
    //
    // `usedPermissiveFallback` was a fourth until V61 and was the only one a
    // CONSUMER read, which is why it is the one this bead fixed; see
    // ``ScanVerdictProvenance``. The two remaining are diagnostic strings that
    // no shipped consumer reads, so they are FILED rather than fixed here —
    // adding a column nobody reads is not an improvement, and the enumeration
    // is what makes the omission visible.

    /// playhead-36t: model-generated refusal explanation captured from
    /// `LanguageModelSession.GenerationError.Refusal.explanation` when
    /// the FM classifier refuses this window. Nil for successful scans,
    /// for permissive-path scans, and when the async explanation fetch
    /// fails. Diagnostic only — does not affect routing.
    ///
    /// **NOT PERSISTED** — there is no `refusalExplanation` column, so this
    /// survives only as long as the object does. playhead-807i.
    let refusalExplanation: String?
    /// playhead-eu1 / playhead-iw7q (schema V61): WHO produced this row's
    /// verdict — the `@Generable` path, the permissive string bypass, or a
    /// binary that did not record it.
    ///
    /// **It was `Bool` and had NO COLUMN until V61**, so the flag `eu1`
    /// introduced was computed by the runner, carried on this struct, and
    /// dropped at the write. Three consumers inherited a `.strong` the runner
    /// hardcoded and read it as the model's — see ``ScanVerdictProvenance`` for
    /// the whole account, and for why `nil`/``ScanVerdictProvenance/unknown``
    /// is a third state rather than a defaulted `false`.
    ///
    /// It DEFAULTS to ``ScanVerdictProvenance/unknown``, which is the
    /// under-claiming direction: a writer that says nothing withholds the
    /// licence instead of granting it.
    let verdictProvenance: ScanVerdictProvenance
    /// playhead-qjcf (schema V66): WHERE the lines this row's `supportLineRefs`
    /// name actually WERE, in seconds, in the segmentation the model was shown.
    ///
    /// **`nil` means the row RECORDS NO SECONDS, and on disk it means exactly
    /// one thing: the row predates V66.** There is no backfill and there cannot
    /// be one, and the argument is one line: **a row's segmentation is
    /// rebuildable iff today's chunks atomize to its version, i.e. iff it is at
    /// the CURRENT version, i.e. iff it already resolves.** Only 90 of the 301
    /// coarse `containsAd` rows on the 2026-08-19 t4 pull are. A default here
    /// would be a fabrication with a `[]`-shaped hole in it, which is the same
    /// trap ``verdictProvenance`` and ``prewarmHit`` document above.
    ///
    /// **DO NOT MAKE THAT ARGUMENT OUT OF playhead-kg6i's 280.** That figure
    /// counts a DIFFERENT predicate — rows whose `transcriptVersion` matches no
    /// surviving `transcript_chunks` ROW STAMP, with 30,125 of the 65,310 chunk
    /// rows carrying NULL by design — kg6i itself refuted it as a reach figure,
    /// and `CD2976E6`'s own CURRENT segmentation falls inside it. The V66 rung's
    /// header carries the whole correction.
    ///
    /// # What it is FOR
    ///
    /// `supportLineRefs` are SEGMENT INDICES, so they name a position in a
    /// coordinate system rather than a stretch of audio. Re-transcribe the
    /// episode and that system is replaced; `SupportLineIndex.resolve` then
    /// correctly refuses, and `SemanticSweepMarkComposer` keeps the row's whole
    /// ~95 s scan tile. **174 of the 301** coarse `containsAd` rows on that pull
    /// are in that state. This column is the same claim in a coordinate system
    /// nothing can supersede.
    ///
    /// # BOTH forms, never just the seconds
    ///
    /// Each entry carries its `lineRef` beside its seconds, and
    /// ``SemanticSweepMarkComposer/persistedSupportSpans(of:)`` requires the ref
    /// SET here to equal the set in ``spansJSON``. So a payload that has drifted
    /// from the verdict it claims to project is refused rather than believed,
    /// and a reader can always tell a projection of THIS row from bytes
    /// reconstructed later against some other segmentation. Seconds alone cannot
    /// support either check.
    ///
    /// Written for `passA` rows only. A `passB` row's geometry is its own
    /// window, which is already seconds — see
    /// ``SemanticSweepMarkComposer/supportLineRefs(of:)``, which excludes
    /// refinement rows for the same reason.
    let supportLineSpansJSON: String?
    /// Model-generated explanation from `Refusal.explanation` at the time the permissive
    /// fallback was triggered. `nil` if explanation was unavailable or the fallback was not used.
    ///
    /// **NOT PERSISTED** — there is no `permissiveFallbackReason` column.
    /// ``verdictProvenance`` now records THAT the bypass ran; this records WHY,
    /// and it is still dropped at the write. playhead-807i.
    let permissiveFallbackReason: String?
    /// playhead-hx6n (schema V42): UNIX seconds at which this row was written.
    ///
    /// `nil` means UNATTRIBUTED, and it means exactly one thing on disk: the row
    /// predates V42. Post-V42 writes cannot leave it nil —
    /// ``AnalysisStore/insertSemanticScanResult(_:now:)`` stamps its own clock
    /// when the caller supplies none, so a NULL `createdAt` is an unambiguous
    /// "written by a binary that did not record this", never "a writer forgot".
    ///
    /// It is NOT defaulted to 0 or to `Date()` on the struct. A default would
    /// make every historical row claim a timestamp it does not have, which is
    /// the precise defect this bead exists to end.
    let createdAt: Double?
    /// playhead-hx6n (schema V42): the app state this row was written in.
    ///
    /// `nil` is UNATTRIBUTED and stays unattributed. Nothing in this codebase
    /// may read `nil` as `.active` — see ``ScanAttributionBucket`` and
    /// ``SemanticScanThroughputSplit``.
    let scenePhase: ScanScenePhase?
    /// playhead-hx6n (schema V42): the `backfill_jobs.jobId` this row was
    /// produced under — the join key `semantic_scan_results` never had.
    ///
    /// `nil` for rows written before V42 and for any writer with no job in
    /// hand. A non-nil value that matches no job joins to nothing, which reads
    /// as "no job" rather than as a wrong one.
    ///
    /// **THIS IS A JOB ID AND IT IS NOT A RUN ID.** It was called
    /// `runCorrelationId` — property and column both — until schema V65
    /// (playhead-1gu0). A `backfill_jobs.jobId` is per `(asset, phase, offset)`,
    /// so it is ONE value for an asset's whole backfill history: measured on the
    /// 2026-08-19 device pull, 15 distinct ids for 15 distinct assets, with 176
    /// rows over four calendar days under a single id. It cannot tell two
    /// screenings of the same window apart, and anything that needs to should
    /// read `transcriptVersion`, which separated every replicate window on both
    /// measured pulls. `latencyMs` does too on the population that matters and
    /// NOT everywhere — two `cancelled` rows can both record 0 ms — so reach for
    /// the version first. ``SemanticSweepMarkComposer/corroboration(for:in:atTranscriptVersion:)``
    /// holds both measurements with the population each is over.
    let backfillJobId: String?
    /// playhead-bg2n (schema V55): the wall clock of this row's FIRST write, and
    /// the LICENCE that says ``createdAt`` and ``observedStatuses`` are complete.
    ///
    /// `nil` means the record does not reach back to the first attempt — either
    /// the row predates V55, or it predates V42 and has no timestamps at all.
    /// **It is never backfilled**, because nothing on disk can recover it: the
    /// value it would hold was overwritten by the upsert this bead is about, and
    /// `attemptCount` cannot stand in for it (a `.success` write replacing a
    /// failure resets the counter to the caller's value, so `attemptCount == 1`
    /// is not proof of one write). See ``AnalysisStore/migrateSemanticScanAttemptHistoryV55IfNeeded()``.
    let firstAttemptAt: Double?
    /// playhead-bg2n (schema V55): the wall clock of this row's MOST RECENT
    /// write — the quantity `createdAt` was silently carrying.
    ///
    /// Under `INSERT OR REPLACE` every upsert restamped `createdAt`, so a column
    /// whose name says creation held the last attempt's time. The V55 migration
    /// COPIES `createdAt` into this column, which is lossless precisely because
    /// that is what every stored value already was, and `createdAt` stops moving.
    ///
    /// Read THIS, not `createdAt`, for "when was this row last touched" —
    /// `AnalysisCoordinator.semanticScanRowsRecorded(since:)` counts windows a
    /// grant banked and was moved onto it in the same change.
    let lastAttemptAt: Double?
    /// playhead-bg2n (schema V55): every distinct ``SemanticScanStatus`` this row
    /// has been written with, as far as the record reaches — sorted raw values,
    /// comma-joined.
    ///
    /// **This is the column that makes `status` readable.** `status` describes ONE
    /// attempt and was read as describing all of them: playhead-hzpa was filed
    /// stating "eleven attempts on one 42.9 s window, every one
    /// `exceededContextWindow`", and the same row's 5th and 9th attempts ended in
    /// `decodingFailure` — a different status with a different `retryPolicy`
    /// (`.simplifySchemaAndRetryOnce` vs `.shrinkWindowAndRetryOnce`). With this
    /// column that sentence is untypeable.
    ///
    /// It is a SET, not a sequence, and that is deliberate: a set cannot be
    /// misread as an ordering. No bounded column can carry the per-attempt
    /// sequence, and this one is bounded by `SemanticScanStatus.allCases.count`
    /// however many times a window is retried — which is why it was preferred to
    /// a per-attempt journal whose growth a phone pays forever.
    ///
    /// COMPLETE only when ``firstAttemptAt`` is non-nil. A pre-V55 row's set is
    /// seeded from its stored `status` (a true statement — that status WAS
    /// observed) and grows from there, so it is a lower bound on a row whose
    /// earlier attempts are gone.
    let observedStatusesCSV: String?
    /// playhead-6gcy (schema V56): the SUM of every MEASURED attempt's
    /// ``latencyMs`` on this row.
    ///
    /// **``latencyMs`` is ONE attempt's cost and was read as the row's.** Under
    /// `INSERT OR REPLACE` every retry overwrites it, so a window that burned
    /// eleven attempts reports whichever one happened to be last. Measured
    /// across the preserved capture generations, `scan-24f9deacdb0e3ab6` read
    /// 6,747.4 → 19,413.8 → 8,213.7 ms on one 42.9 s window — a **2.88×** spread
    /// of which only the last survives a pull.
    ///
    /// **THE UNIT IS THE ONE ``latencyMs`` HAS, AND THE SUM INHERITS ITS
    /// CAVEAT.** Each addend is one FM window attempt's `ContinuousClock` span,
    /// taken from an ``FMClockPair`` before the call — NOT the pass's wall clock
    /// (playhead-ejr7 removed the parameter through which the pass total could
    /// reach a row), and NOT this row's lifetime. Device sleep is INSIDE it, as
    /// it is inside every addend, so this is summed wall clock and not compute.
    /// There is deliberately no `suspendingLatencyMsTotal` beside it, so the
    /// device-asleep share is not recoverable across attempts — a stated limit.
    ///
    /// `nil` means NOT RECORDED (the row predates V56, or no attempt of it has
    /// ever measured itself). It is never 0 for "no attempts": SQL's `SUM` skips
    /// NULL and so does this, which is the contribution an unmeasured attempt
    /// should make.
    let latencyMsTotal: Double?
    /// playhead-6gcy (schema V56): the LARGEST single measured attempt's
    /// ``latencyMs`` on this row. Same span, same caveat, same `nil` meaning as
    /// ``latencyMsTotal``.
    ///
    /// Compare it against ``latencyMsMean`` to see whether the attempts cost the
    /// same. There is deliberately **no boolean** for that comparison: telling
    /// "equal" from "nearly equal" over `Double`s needs a tolerance, and a
    /// tolerance chosen here would be a threshold nobody measured.
    let latencyMsMax: Double?
    /// playhead-6gcy (schema V56): HOW MANY attempts contributed to
    /// ``latencyMsTotal`` and ``latencyMsMax`` — the denominator, and half the
    /// licence that says the pair is exhaustive.
    ///
    /// It is NOT ``attemptCount``. An attempt that measured nothing writes NULL
    /// and contributes to neither, which is why the two can differ and why the
    /// gap is the interesting quantity: `attemptCount - latencySampleCount` is
    /// how many of this row's attempts left no cost behind.
    ///
    /// See ``latencyHistoryIsComplete`` before reading the pair as the row's
    /// whole cost.
    let latencySampleCount: Int?

    init(
        id: String,
        analysisAssetId: String,
        windowFirstAtomOrdinal: Int,
        windowLastAtomOrdinal: Int,
        windowStartTime: Double,
        windowEndTime: Double,
        scanPass: String,
        transcriptQuality: TranscriptQuality,
        disposition: CoarseDisposition,
        spansJSON: String,
        status: SemanticScanStatus,
        attemptCount: Int,
        errorContext: String?,
        inputTokenCount: Int?,
        outputTokenCount: Int?,
        latencyMs: Double?,
        suspendingLatencyMs: Double? = nil,
        daemonPeersAtStart: Int? = nil,
        prewarmHit: Bool? = nil,
        scanCohortJSON: String,
        transcriptVersion: String,
        reuseScope: String? = nil,
        runMode: SemanticScanPhase = .shadow,
        jobPhase: String = "shadow",
        refusalExplanation: String? = nil,
        verdictProvenance: ScanVerdictProvenance = .unknown,
        permissiveFallbackReason: String? = nil,
        createdAt: Double? = nil,
        scenePhase: ScanScenePhase? = nil,
        backfillJobId: String? = nil,
        firstAttemptAt: Double? = nil,
        lastAttemptAt: Double? = nil,
        observedStatusesCSV: String? = nil,
        latencyMsTotal: Double? = nil,
        latencyMsMax: Double? = nil,
        latencySampleCount: Int? = nil,
        supportLineSpansJSON: String? = nil
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.windowFirstAtomOrdinal = windowFirstAtomOrdinal
        self.windowLastAtomOrdinal = windowLastAtomOrdinal
        self.windowStartTime = windowStartTime
        self.windowEndTime = windowEndTime
        self.scanPass = scanPass
        self.transcriptQuality = transcriptQuality
        self.disposition = disposition
        self.spansJSON = spansJSON
        self.status = status
        self.attemptCount = attemptCount
        self.errorContext = errorContext
        self.inputTokenCount = inputTokenCount
        self.outputTokenCount = outputTokenCount
        self.latencyMs = latencyMs
        self.suspendingLatencyMs = suspendingLatencyMs
        self.daemonPeersAtStart = daemonPeersAtStart
        self.prewarmHit = prewarmHit
        self.scanCohortJSON = scanCohortJSON
        self.transcriptVersion = transcriptVersion
        self.reuseScope = reuseScope
        self.runMode = runMode
        self.jobPhase = jobPhase
        self.refusalExplanation = refusalExplanation
        self.verdictProvenance = verdictProvenance
        self.permissiveFallbackReason = permissiveFallbackReason
        self.createdAt = createdAt
        self.scenePhase = scenePhase
        self.backfillJobId = backfillJobId
        self.firstAttemptAt = firstAttemptAt
        self.lastAttemptAt = lastAttemptAt
        self.observedStatusesCSV = observedStatusesCSV
        self.latencyMsTotal = latencyMsTotal
        self.latencyMsMax = latencyMsMax
        self.latencySampleCount = latencySampleCount
        self.supportLineSpansJSON = supportLineSpansJSON
    }

    // MARK: - playhead-bg2n: reading a row's ATTEMPT HISTORY

    /// The canonical encoding of ``observedStatusesCSV``: sorted raw values,
    /// comma-joined, duplicates collapsed.
    ///
    /// Sorted so the column is a SET on disk as well as in meaning — two rows
    /// that have seen the same statuses in different orders must compare equal,
    /// or a diff over two device pulls reports churn that did not happen.
    static func encodeObservedStatuses(_ statuses: Set<SemanticScanStatus>) -> String {
        statuses.map(\.rawValue).sorted().joined(separator: ",")
    }

    /// Decode ``observedStatusesCSV`` into TYPED cases. Unrecognised raw values
    /// are DROPPED rather than throwing, matching `fetchSemanticScanStatuses`'s
    /// leniency: a status a newer binary invented must not abort a read of the
    /// whole row.
    ///
    /// **Do not count this set to decide whether the attempts differed.** Use
    /// ``rawObservedStatusTokens``. Dropping is not the harmless direction it
    /// looks like: on a row whose history is COMPLETE, dropping one of two
    /// tokens takes the count from 2 to 1 and turns ``attemptsDiffered`` from
    /// `true` into a confident `false` — "these attempts were all alike", which
    /// is playhead-hzpa's sentence, manufactured by a decoder. Found at review
    /// round 2 of playhead-bg2n, in the API written to prevent that sentence.
    static func decodeObservedStatuses(_ csv: String?) -> Set<SemanticScanStatus> {
        guard let csv, !csv.isEmpty else { return [] }
        return Set(csv.split(separator: ",").compactMap { SemanticScanStatus(rawValue: String($0)) })
    }

    /// The RAW tokens of ``observedStatusesCSV``, decoded by nobody.
    ///
    /// An unrecognised token is still a status this row was written with — it is
    /// evidence about the COUNT even when it is not evidence about the KIND — so
    /// every "did the attempts differ" question is asked here rather than of the
    /// typed set.
    static func rawObservedStatusTokens(_ csv: String?) -> Set<String> {
        guard let csv, !csv.isEmpty else { return [] }
        return Set(csv.split(separator: ",").map(String.init))
    }

    /// Every status this row has been written with, as far as the record reaches.
    ///
    /// Falls back to `[status]` when the column is absent, so a caller never has
    /// to special-case a pre-V55 row — but see ``historyIsComplete`` before
    /// reading the result as exhaustive.
    var observedStatuses: Set<SemanticScanStatus> {
        let decoded = Self.decodeObservedStatuses(observedStatusesCSV)
        return decoded.isEmpty ? [status] : decoded.union([status])
    }

    /// True when this row's attempt history is known from its FIRST attempt —
    /// i.e. when ``observedStatuses`` is exhaustive and ``createdAt`` really is
    /// the creation time.
    ///
    /// The single licence, deliberately: one marker guarding both claims is
    /// harder to read past than two independent nullables that can disagree.
    var historyIsComplete: Bool { firstAttemptAt != nil }

    /// Did this row's attempts differ from one another? THREE-VALUED, because
    /// "not established" is a distinct answer from "no".
    ///
    /// * `true`  — two or more distinct statuses are on the record. Whatever
    ///             else is unknown, the attempts were NOT all alike.
    /// * `false` — the history is complete and holds exactly one status.
    /// * `nil`   — the history is partial and only one status survives, so the
    ///             attempts before the record began cannot be spoken for.
    ///
    /// `nil` is the answer playhead-hzpa needed and could not get. Reading it as
    /// `false` reproduces this bead exactly.
    var attemptsDiffered: Bool? {
        // RAW tokens, not `observedStatuses`. The typed set drops a status a
        // newer binary invented, and on a COMPLETE row that takes the count from
        // 2 to 1 and answers `false` — "the attempts were all alike" — for a row
        // that demonstrably had two. See ``decodeObservedStatuses``.
        let distinct = Self.rawObservedStatusTokens(observedStatusesCSV).union([status.rawValue])
        if distinct.count > 1 { return true }
        return historyIsComplete ? false : nil
    }

    /// How long this row has been being attempted, in seconds — `nil` unless
    /// BOTH endpoints are known.
    ///
    /// Deliberately not `lastAttemptAt − createdAt`: on a pre-V55 row
    /// `createdAt` is frozen at the last pre-migration attempt, so that
    /// subtraction reads a stuck window as young — the exact inversion this bead
    /// exists to end ("the longer a window has been stuck, the newer it looks").
    var attemptSpanSeconds: Double? {
        guard let firstAttemptAt, let lastAttemptAt else { return nil }
        return lastAttemptAt - firstAttemptAt
    }

    // MARK: - playhead-6gcy: reading a row's LATENCY history

    /// The mean cost of this row's MEASURED attempts — `nil` when none were.
    ///
    /// The denominator is ``latencySampleCount``, never ``attemptCount``: an
    /// attempt that wrote no latency is not in the numerator either, and
    /// dividing by the larger number would report a cheap-looking mean for a
    /// window most of whose attempts were never timed.
    var latencyMsMean: Double? {
        guard let latencyMsTotal, let latencySampleCount, latencySampleCount > 0 else { return nil }
        return latencyMsTotal / Double(latencySampleCount)
    }

    /// Do ``latencyMsTotal`` / ``latencyMsMax`` account for EVERY attempt this
    /// row has ever had? THREE-VALUED, for the same reason
    /// ``attemptsDiffered`` is: "not established" is a distinct answer from "no".
    ///
    /// * `true`  — the record reaches the first attempt AND every recorded
    ///             attempt contributed a cost. The total IS the row's whole cost.
    /// * `false` — one of those two fails: attempts before the record began are
    ///             missing, or some recorded attempt measured nothing. The total
    ///             is a LOWER BOUND.
    /// * `nil`   — nothing was recorded at all (a pre-V56 row), so no claim is
    ///             made in either direction.
    ///
    /// Both clauses are needed and neither implies the other.
    /// ``historyIsComplete`` (playhead-bg2n's `firstAttemptAt` licence) says the
    /// record reaches attempt 1; it says nothing about whether those attempts
    /// were timed. ``latencySampleCount`` says every recorded attempt was timed;
    /// it says nothing about attempts that predate the record. A reader who
    /// checks one and not the other reads a lower bound as a total, which is
    /// this bead one level up.
    ///
    /// The count comparison is `==` rather than `>=` deliberately: more samples
    /// than attempts would mean a writer double-counted, and answering `false`
    /// under-claims rather than vouching for a total nobody can explain.
    var latencyHistoryIsComplete: Bool? {
        guard let latencySampleCount else { return nil }
        guard historyIsComplete else { return false }
        return latencySampleCount == attemptCount
    }

    /// How many of this row's attempts left NO cost behind — `nil` when the
    /// latency record does not exist.
    ///
    /// This is the quantity that makes a `nil` ``latencyHistoryIsComplete``
    /// actionable: it says how much of the row is missing, not merely that some
    /// of it is.
    var unmeasuredAttemptCount: Int? {
        guard let latencySampleCount else { return nil }
        return Swift.max(0, attemptCount - latencySampleCount)
    }

    /// playhead-hx6n: the ONE seam that stamps run attribution onto a scan row.
    ///
    /// Every `SemanticScanResult` factory in `BackfillJobRunner` already had the
    /// job id in hand — each one takes `jobId:` and passes it as `reuseScope`,
    /// where it was hashed into `reuseKeyHash` and then discarded. Rather than
    /// widen six factories, attribution is applied once, at the seam where the
    /// row is handed to the store, which is also the moment that makes
    /// `scenePhase` mean what it claims: the phase AT COMPLETION, not the phase
    /// the job happened to start in. A backfill job runs for minutes and can
    /// cross a phase boundary mid-run; stamping at job start would attribute
    /// every one of its rows to whichever side it began on — the exact
    /// misattribution the measurement is trying to avoid.
    ///
    /// Deliberately non-mutating and total: it returns a copy with the three
    /// attribution fields replaced and everything else byte-identical, so it
    /// cannot perturb geometry, status, or the reuse key.
    func attributed(
        createdAt: Double,
        scenePhase: ScanScenePhase?,
        backfillJobId: String?
    ) -> SemanticScanResult {
        SemanticScanResult(
            id: id,
            analysisAssetId: analysisAssetId,
            windowFirstAtomOrdinal: windowFirstAtomOrdinal,
            windowLastAtomOrdinal: windowLastAtomOrdinal,
            windowStartTime: windowStartTime,
            windowEndTime: windowEndTime,
            scanPass: scanPass,
            transcriptQuality: transcriptQuality,
            disposition: disposition,
            spansJSON: spansJSON,
            status: status,
            attemptCount: attemptCount,
            errorContext: errorContext,
            inputTokenCount: inputTokenCount,
            outputTokenCount: outputTokenCount,
            latencyMs: latencyMs,
            suspendingLatencyMs: suspendingLatencyMs,
            daemonPeersAtStart: daemonPeersAtStart,
            prewarmHit: prewarmHit,
            scanCohortJSON: scanCohortJSON,
            transcriptVersion: transcriptVersion,
            reuseScope: reuseScope,
            runMode: runMode,
            jobPhase: jobPhase,
            refusalExplanation: refusalExplanation,
            verdictProvenance: verdictProvenance,
            permissiveFallbackReason: permissiveFallbackReason,
            createdAt: createdAt,
            scenePhase: scenePhase,
            backfillJobId: backfillJobId,
            // playhead-bg2n: the three history fields are carried through
            // UNCHANGED, exactly like geometry and status. They are decided by
            // the STORE, which is the only place that can see the row already on
            // disk; a producer stamping them here would be asserting a history
            // it has not read.
            firstAttemptAt: firstAttemptAt,
            lastAttemptAt: lastAttemptAt,
            observedStatusesCSV: observedStatusesCSV,
            // playhead-6gcy: the three latency-history fields are carried
            // through UNCHANGED for bg2n's reason one line up — they are
            // decided by the STORE, which is the only place that can see what
            // the row on disk already accumulated. A producer stamping a total
            // here would be asserting a history it has not read.
            latencyMsTotal: latencyMsTotal,
            latencyMsMax: latencyMsMax,
            latencySampleCount: latencySampleCount,
            // playhead-qjcf: carried through UNCHANGED, and the reason is the
            // opposite of the two blocks above. Those are the STORE's to decide;
            // this is the PRODUCER's — it is a projection of the segmentation
            // the scan just ran, and nothing downstream of the scan can
            // reconstruct it. `attributed` is on the ONLY path from
            // `makeScanResult` to `insertSemanticScanResult`
            // (`BackfillJobRunner.attributed(_:jobId:)`), so omitting it here
            // would drop every projected second between the writer and the disk
            // while every rail on either side stayed green — a value silently
            // lost at a copy, which is this bead's own defect class one layer
            // over. Pinned by `attributedCarriesTheProjectionThrough`.
            supportLineSpansJSON: supportLineSpansJSON
        )
    }

    /// playhead-avbn: the pass whose ``disposition`` is a PRESENCE VERDICT — the
    /// answer to "is there an ad in this window". Deliberately defined AS
    /// ``SemanticScanCoverage/coverageScanPass`` rather than repeating the
    /// literal, because they are the same fact seen from two sides: `passA` is
    /// the pass that screens a window and says yes or no, which is exactly why
    /// it is also the pass coverage counts.
    ///
    /// `passB` is refinement. It runs only inside windows `passA` already called
    /// `containsAd` and it is asked WHERE the edges are, so its `.noAds` means
    /// "found no edges", not "there is no ad". Any consumer reading a
    /// `disposition` as evidence of ABSENCE must filter to this pass — see
    /// ``FMSuppressionWindow/votingWindows(spanStartTime:spanEndTime:scanResults:)``.
    static let presenceScanPass = SemanticScanCoverage.coverageScanPass

    /// playhead-pz32: `errorContext` prefix marking a NO-WORK SENTINEL row —
    /// see `BackfillJobRunner.makeNoWorkSentinelScanResult`. Such a row carries
    /// `status == .noAds` (whose ``SemanticScanStatus/didExamineWindow`` is
    /// `true`, because `.noAds` is normally the permissive path's genuine "I
    /// looked and there is nothing here" verdict) and spans the WHOLE attempted
    /// transcript range — while explicitly meaning **no work was performed**.
    ///
    /// Any coverage accounting that treats it as examined therefore reports an
    /// entire episode as screened off a job that made zero FM calls. The status
    /// alone cannot distinguish the two, so the `errorContext` marker is the
    /// discriminator, and it lives here so the writer, the pipeline breadcrumb
    /// and the user-facing readiness predicate all agree.
    static let noWorkSentinelErrorContextPrefix = "noWork:"

    /// playhead-pz32: true when this row is a no-work sentinel rather than a
    /// real examination. NEVER count it as scanned audio.
    var isNoWorkSentinel: Bool {
        Self.isNoWorkSentinel(errorContext: errorContext)
    }

    /// playhead-pz32: did this row screen its window and produce a verdict? The
    /// single definition of "we looked", combining the status semantics with the
    /// no-work-sentinel exclusion.
    var didExamineWindow: Bool {
        Self.didExamineWindow(status: status, errorContext: errorContext)
    }

    static func isNoWorkSentinel(errorContext: String?) -> Bool {
        errorContext?.hasPrefix(noWorkSentinelErrorContextPrefix) == true
    }

    /// playhead-pz32: THE definition of "this row screened its window", stated
    /// over the two raw column values so a narrow SQL projection can apply it
    /// without decoding a whole row.
    /// ``AnalysisStore/fetchCoverageSummariesByAssetIds(_:)`` calls this; do not
    /// re-implement the condition at a call site. A `nil` status is an
    /// unrecognised persisted string (forward-compat) and is NOT an examination —
    /// under-claim.
    static func didExamineWindow(status: SemanticScanStatus?, errorContext: String?) -> Bool {
        guard let status, status.didExamineWindow else { return false }
        return !isNoWorkSentinel(errorContext: errorContext)
    }

    func isReusable(
        scanCohortJSON: String,
        transcriptVersion: String
    ) -> Bool {
        // Decision cohort changes only affect downstream decisioning; the FM
        // scan remains reusable as long as the scan cohort and transcript match.
        Self.matchesScanCohortJSON(self.scanCohortJSON, scanCohortJSON) &&
        self.transcriptVersion == transcriptVersion
    }

    /// Internal hook so tests can observe decode-failure logging without
    /// scraping OSLog.
    nonisolated(unsafe) static var decodeFailureObserver: (@Sendable (String, String) -> Void)?

    static func matchesScanCohortJSON(_ lhs: String, _ rhs: String) -> Bool {
        if lhs == rhs {
            return true
        }

        let decoder = JSONDecoder()
        guard let lhsData = lhs.data(using: .utf8),
              let rhsData = rhs.data(using: .utf8) else {
            semanticScanLogger.debug("scan cohort comparison failed: invalid UTF-8")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        let lhsCohort: ScanCohort?
        let rhsCohort: ScanCohort?
        do {
            lhsCohort = try decoder.decode(ScanCohort.self, from: lhsData)
        } catch {
            semanticScanLogger.debug("scan cohort decode failed for stored value: \(String(describing: error), privacy: .public)")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        do {
            rhsCohort = try decoder.decode(ScanCohort.self, from: rhsData)
        } catch {
            semanticScanLogger.debug("scan cohort decode failed for query value: \(String(describing: error), privacy: .public)")
            decodeFailureObserver?(lhs, rhs)
            return false
        }
        return lhsCohort == rhsCohort
    }
}

/// playhead-qbib: honest scanned-duration accounting over persisted
/// `semantic_scan_results` rows.
///
/// Every downstream ratio — coverage %, precision measurement, the
/// playhead-0sro watermark invariants — SHOULD divide by "how much audio did
/// we actually screen". Before this type existed there was no way to ask that
/// question: a refused window and a window screened clean both persisted a
/// row, so a truncated scan that reported success was indistinguishable from
/// a complete one. `examinedSeconds` answers "we looked"; `unexaminedSeconds`
/// / `unexaminedRanges` answer "we could not look" — including audio no
/// window ever reached, when `episodeDuration` is supplied.
///
/// Scope note (playhead-qbib): today the only production consumer is the
/// run-completion breadcrumb in `BackfillJobRunner.logCoarseCoverage`. Wiring
/// it into the coverage/precision reporting and the 0sro watermark is
/// deliberately NOT part of this bead — those consumers still compute their
/// own denominators, and moving them is a measurement change, not a
/// robustness fix.
///
/// Ranges are half-open in spirit but modelled as `ClosedRange` because a
/// window's `[start, end]` is inclusive of both transcript boundaries. Zero-
/// and negative-width rows are ignored: they carry no coverage information
/// (and a `0.0 ... 0.0` row is the passB coordinate bug this bead fixed).
struct SemanticScanCoverage: Sendable, Equatable {
    /// Union of the time ranges of rows that actually obtained a verdict
    /// (`SemanticScanResult.didExamineWindow`).
    let examinedSeconds: Double
    /// Union of attempted-or-expected time that produced no verdict, with
    /// `examinedSeconds` subtracted out. A window that refused and was then
    /// recovered by a smaller retry is NOT a hole.
    let unexaminedSeconds: Double
    /// The individual holes behind `unexaminedSeconds`, in time order. These
    /// are the ranges a consumer must not describe as "no ads here".
    let unexaminedRanges: [ClosedRange<Double>]

    /// Total audio this accounting covers. With no `episodeDuration` that is
    /// "screened + tried to screen"; with one it also includes audio no
    /// window ever reached, i.e. "screened + should have screened".
    var accountedSeconds: Double { examinedSeconds + unexaminedSeconds }

    /// Fraction of `accountedSeconds` that produced a real verdict. `1.0`
    /// when nothing was accounted for, so an empty pass never reads as a hole.
    var examinedFraction: Double {
        let total = accountedSeconds
        guard total > 0 else { return 1 }
        return examinedSeconds / total
    }

    /// True when every second accounted for produced a verdict.
    var isComplete: Bool { unexaminedRanges.isEmpty }

    /// playhead-pz32: the canonical COVERAGE LANE. `passA` rows are the
    /// whole-episode screening sweep; `passB` rows are localized extent
    /// attempts INSIDE already-screened `passA` windows, so counting them
    /// would double-count.
    ///
    /// Shared with ``AnalysisStore/fetchCoverageSummariesByAssetIds(_:)``'s
    /// `adScanCoveredSec` derivation, so the pipeline breadcrumb and the
    /// user-facing readiness predicate agree about WHICH ROWS constitute
    /// coverage. They deliberately do NOT agree on the resulting SECONDS: this
    /// type unions the window bounds as persisted, while `adScanCoveredSec`
    /// additionally intersects them with the transcribed region, because a
    /// window's bounds can straddle audio its prompt never contained. Expect
    /// `examinedSeconds >= adScanCoveredSec`, sometimes by a lot — the log is
    /// answering "how much did the pass attempt to screen", the checkmark is
    /// answering "how much audio was read".
    static let coverageScanPass = "passA"

    /// Compute coverage for one scan pass.
    ///
    /// - Parameters:
    ///   - rows: persisted scan rows for a single asset. Rows from other
    ///     passes are filtered out — passB rows are localized extent
    ///     attempts inside already-screened passA windows, so mixing them in
    ///     would double-count.
    ///   - scanPass: the pass to account for. `passA` is the coverage lane.
    ///   - episodeDuration: when supplied, audio in `0 ... episodeDuration`
    ///     that no row covers at all is reported as unexamined. This is what
    ///     catches a pass that stopped at 1425.9s of a 3578s episode: the
    ///     rows it did write all look fine on their own. Note the window is
    ///     measured from zero, so an episode whose first scanned window starts
    ///     after t=0 reports that lead-in as a hole — which is correct: no
    ///     window looked there.
    static func compute(
        rows: [SemanticScanResult],
        scanPass: String = SemanticScanCoverage.coverageScanPass,
        episodeDuration: Double? = nil
    ) -> SemanticScanCoverage {
        let passRows = rows.filter { $0.scanPass == scanPass && $0.windowEndTime > $0.windowStartTime }
        // playhead-pz32: `didExamineWindow` is the row-level predicate (status
        // semantics MINUS no-work sentinels), not the bare status. A sentinel
        // spans the whole attempted range while meaning "no work performed", so
        // counting it as examined turned a job that made zero FM calls into a
        // fully-screened episode. It is correctly reported as a HOLE instead.
        let examined = merge(
            passRows
                .filter(\.didExamineWindow)
                .map { $0.windowStartTime ... $0.windowEndTime }
        )
        var unexamined = merge(
            passRows
                .filter { !$0.didExamineWindow }
                .map { $0.windowStartTime ... $0.windowEndTime }
        )
        if let episodeDuration, episodeDuration > 0 {
            let attempted = merge(passRows.map { $0.windowStartTime ... $0.windowEndTime })
            unexamined = merge(unexamined + subtract(cuts: attempted, from: [0 ... episodeDuration]))
        }
        let holes = subtract(cuts: examined, from: unexamined)
        return SemanticScanCoverage(
            examinedSeconds: total(of: examined),
            unexaminedSeconds: total(of: holes),
            unexaminedRanges: holes
        )
    }

    private static func total(of ranges: [ClosedRange<Double>]) -> Double {
        ranges.reduce(0) { $0 + ($1.upperBound - $1.lowerBound) }
    }

    /// Sort and coalesce overlapping/touching ranges into a disjoint union.
    private static func merge(_ ranges: [ClosedRange<Double>]) -> [ClosedRange<Double>] {
        let sorted = ranges.sorted { $0.lowerBound < $1.lowerBound }
        var merged: [ClosedRange<Double>] = []
        for range in sorted {
            guard let last = merged.last else {
                merged.append(range)
                continue
            }
            if range.lowerBound <= last.upperBound {
                merged[merged.count - 1] = last.lowerBound ... Swift.max(last.upperBound, range.upperBound)
            } else {
                merged.append(range)
            }
        }
        return merged
    }

    /// Remove every second covered by `cuts` from `ranges`. Both inputs are
    /// merged first so the walk only has to handle disjoint, ordered spans.
    private static func subtract(
        cuts: [ClosedRange<Double>],
        from ranges: [ClosedRange<Double>]
    ) -> [ClosedRange<Double>] {
        let orderedCuts = merge(cuts)
        var remaining: [ClosedRange<Double>] = []
        for range in merge(ranges) {
            var cursor = range.lowerBound
            for cut in orderedCuts where cut.upperBound > range.lowerBound && cut.lowerBound < range.upperBound {
                if cut.lowerBound > cursor {
                    remaining.append(cursor ... cut.lowerBound)
                }
                cursor = Swift.max(cursor, cut.upperBound)
            }
            if cursor < range.upperBound {
                remaining.append(cursor ... range.upperBound)
            }
        }
        return remaining
    }
}

enum EvidenceSourceType: String, Codable, Sendable, Hashable, CaseIterable {
    case fm
    case lexical
    case acoustic
    case catalog
    case classifier
    case fingerprint
    /// Calibration key for the fused aggregate score (proposalConfidence → skipConfidence).
    /// Distinct from `.classifier` to avoid conflating per-source classifier calibration
    /// with the post-fusion score mapping.
    case fusedScore
    /// playhead-z3ch: Pre-seeded evidence derived from RSS feed description /
    /// itunes:summary metadata. Capped at `FusionWeightConfig.metadataCap`
    /// (Plan §7.4 = 0.15) and gated by a corroboration check — metadata-only
    /// ledgers MUST resolve to `.blockedByEvidenceQuorum`.
    /// Persistence note: this enum is `Codable` and persisted via SQLite in
    /// `EvidenceEvent.sourceType`. The case is purely additive; no migration
    /// is required because no rows reference it pre-shipping.
    case metadata
    /// 2026-04-23 real-data eval Finding 4: music-bed coverage across a
    /// span's interior windows. Distinct from `.acoustic` so the quorum
    /// gate's `distinctKinds.count` increments when both an RMS-drop and
    /// a music-bed signal fire. Shares the acoustic evidence family
    /// (see `SourceEvidenceFamily.for`) for trust-update orthogonality —
    /// same underlying modality, different trigger geometry. Per-source
    /// budget is `FusionWeightConfig.musicBedCap` (NOT `acousticCap`):
    /// playhead-2hpn carved out a dedicated cap so the scoped-music-bed
    /// jingle boost (0.10 → 0.25) is not silently truncated to 0.20. See
    /// `BackfillEvidenceFusion.buildLedger()` for the dedicated branch
    /// and `MusicBedLedgerEvaluator.musicBedConfirmedJingleWeight` for
    /// the coupling invariant.
    /// Persistence note: additive case; no migration required.
    case musicBed
    /// playhead-fqc8: Acoustic-break alignment with a `.classifierSeed`-anchored
    /// span boundary. Treated as a DISTINCT evidence kind from `.acoustic`
    /// (RMS-drop) so the family budget is honest: each kind caps independently
    /// against its own per-source cap (`acousticCap` for RMS-drop,
    /// `breakAlignmentCap` for break-alignment). Without a separate kind, a
    /// classifier-seeded span could emit two `.acoustic` entries summing to
    /// 2 × `acousticCap` = 0.40, silently doubling the documented family
    /// budget. See `BackfillEvidenceFusion.buildLedger()` for the dedicated
    /// branch.
    /// Persistence note: additive case; no migration required. Forward-compatible
    /// only — a TestFlight downgrade to a build that lacks `.breakAlignment` would
    /// fail-loud at decode time (`AnalysisStore.readEvidenceEvent` throws
    /// `queryFailed("Unknown evidence source type 'breakAlignment'")`); acceptable
    /// for an additive enum and matches existing behavior for `.musicBed` etc.
    case breakAlignment
    /// playhead-xsdz.1: High-precision lexical auto-ad rule. Distinct from
    /// `.lexical` (the per-candidate confidence signal capped at the modest
    /// `lexicalCap = 0.20`) because this kind represents a strong, *vetted*
    /// co-occurrence of ad-copy signals (a sponsor disclosure PLUS a promo
    /// code and/or URL CTA) inside a tight time window, with negative-
    /// evidence guardrails already applied. It carries its own larger budget
    /// (`FusionWeightConfig.lexicalAutoAdCap`) and gates a dedicated
    /// `PromotionTrack.lexicalAutoAdQualified` so a confirmed combo can clear
    /// the auto-skip threshold on its own — which the structurally-capped
    /// `.lexical` family can never do. The separate kind also lets the
    /// quorum / corroboration gates count it as an independent in-audio
    /// evidence family without inflating the `.lexical` family cap.
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.breakAlignment` / `.musicBed`.
    case lexicalAutoAd
    /// playhead-xsdz.8: Composite audio-forensics boundary channel. ONE merged
    /// evidence kind carrying the PHYSICAL signature of audio insertion at a
    /// candidate ad boundary — discontinuities that ad TEXT cannot fake:
    /// loudness/RMS jump, spectral-character (flux) shift, noise-floor change,
    /// and recording-environment (production/music) change measured ACROSS the
    /// span's start and end edges relative to the span interior.
    ///
    /// Why ONE kind (not three caps): the cross-model idea duel explicitly
    /// recommended a single capped channel. Each sub-signal is sigma-normalized
    /// against the episode's own distribution and the strongest few are merged
    /// into one boundary-forensics score before it ever becomes a ledger entry,
    /// so the family budget is a single `FusionWeightConfig.audioForensicsCap`
    /// (mirrors the `.breakAlignment` / `.musicBed` carve-outs). It fires
    /// CONSERVATIVELY (corroborative only) and is OFF by default
    /// (`AdDetectionConfig.audioForensicsEnabled`).
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.breakAlignment` / `.musicBed` / `.lexicalAutoAd`.
    case audioForensics
    /// playhead-xsdz.9: Cross-episode "memory" POSITIVE boost. Emitted when a
    /// candidate's transcript tokens align strongly (Smith-Waterman local
    /// alignment) to a CONFIRMED-AD bank sequence — i.e. the same ad copy was
    /// already confirmed on a prior episode. A modest corroborator capped at
    /// `FusionWeightConfig.crossEpisodeMemoryCap`; it never drives a skip on its
    /// own (no qualified promotion track). Distinct from `.fingerprint` (MinHash
    /// Jaccard, order-insensitive) because this is order-SENSITIVE local
    /// alignment. The HARD-NEGATIVE half of the same feature is NOT a ledger
    /// entry — it is a post-fusion multiplicative suppression (a negative ledger
    /// weight would be clamped to 0 by the v0 identity calibrator), so only the
    /// positive boost rides this source kind. Gated OFF by default
    /// (`AdDetectionConfig.crossEpisodeMemoryEnabled`).
    /// Persistence note: additive case; no migration required. Forward-only.
    case crossEpisodeMemory
    /// playhead-xsdz.12: Rhetorical act-sequence grammar. Emitted when a
    /// candidate span's transcript prose exhibits the canonical persuasion
    /// PROGRAM — HOOK → PROBLEM → SOLUTION → EVIDENCE → OFFER → CTA — with
    /// THREE OR MORE distinct rhetorical roles co-occurring in (roughly) that
    /// order. No single role is ad-specific (content can ask questions, cite
    /// stats, give URLs); the ORDERED CO-OCCURRENCE of 3+ roles is what is
    /// almost exclusively an ad, and it fires even when the lexical
    /// sponsor/promo/URL cues do not. Text-derived, so it shares the `.textual`
    /// evidence family with `.lexical` / `.lexicalAutoAd`. A MODEST corroborator
    /// capped at `FusionWeightConfig.rhetoricalGrammarCap` with NO qualified
    /// promotion track — it never drives a skip on its own, only adds honest
    /// in-audio (transcript) mass and bumps `distinctKinds.count`. Gated OFF by
    /// default (`AdDetectionConfig.rhetoricalGrammarEnabled`).
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.crossEpisodeMemory` / `.audioForensics`.
    case rhetoricalGrammar
    /// playhead-xsdz.13: Cross-show syndication POSITIVE boost. Emitted when a
    /// candidate span's NORMALIZED sponsor entity (extracted by
    /// `EvidenceCatalogBuilder`) recurs across MANY of the user's UNRELATED
    /// subscribed shows AND has persisted across time — overwhelming evidence of
    /// a paid NETWORK ad campaign (ads are sold across show networks), as opposed
    /// to a show-specific editorial brand mention. Aggregated PURELY from the
    /// user's OWN local library by `CrossShowSyndicationStore` — no network, no
    /// cross-user data. A MODEST corroborator capped at
    /// `FusionWeightConfig.crossShowSyndicationCap` with NO qualified promotion
    /// track — it never drives a skip on its own, only adds honest mass and bumps
    /// `distinctKinds.count`. It is a cross-library REFERENCE-match signal (shares
    /// the `.reference` family with `.fingerprint` / `.catalog` /
    /// `.crossEpisodeMemory`), so — like `.crossEpisodeMemory` — it is
    /// deliberately NOT counted as in-audio corroboration. Gated OFF by default
    /// (`AdDetectionConfig.crossShowSyndicationEnabled`).
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.crossEpisodeMemory` / `.rhetoricalGrammar`.
    case crossShowSyndication
    /// playhead-xsdz.62: Byte-exact REDIFF-confirmed DAI insertion. Emitted by
    /// `BackfillEvidenceFusion.buildLedger()` (behind `rediffConfirmedKindEnabled`)
    /// for a span whose WIDTH is owned by the byte-exact rediff oracle
    /// (`.rediffSlot` in `anchorProvenance` == `DecodedSpan.carriesRediffByteExactWidth`).
    /// A byte-exact-rediff-confirmed rotating region IS a DAI-inserted ad by
    /// deterministic definition (the origin literally served different ad bytes
    /// on a re-fetch — no classification needed), so this is a DISTINCT
    /// corroborating KIND for the fusion corroboration quorum: it gives
    /// rediff-confirmed DAI a reproducible 2nd deterministic kind so FM's vote
    /// stops being load-bearing for their eligibility. It is a deterministic
    /// PRESENCE marker, NOT a score driver — the emitted entry carries weight 0
    /// (the kind itself is the whole signal), so it increments
    /// `distinctKinds.count` in the corroboration gates WITHOUT changing
    /// `proposalConfidence` / `skipConfidence`. It is a cross-fetch reference-match
    /// signal (shares the `.reference` family with `.fingerprint` / `.catalog` /
    /// `.crossEpisodeMemory` / `.crossShowSyndication`). Deliberately byte-exact
    /// ONLY: acoustic splice (`.spliceSlot`) is NOT deterministic and never
    /// triggers this kind (it fails `carriesRediffByteExactWidth`).
    /// Persistence note: additive case; no migration required. Forward-only,
    /// matching `.breakAlignment` / `.crossShowSyndication`.
    case rediffConfirmed
    /// Phase 11 random negative-audit marker. These rows are persisted in
    /// `evidence_events` for miss-rate estimation, but they are not positive
    /// FM evidence for training or fusion.
    case audit
    /// Phase 11 operational-health payloads for FM backfill jobs/runs.
    /// These rows use an empty atom ordinal array and are excluded from
    /// model-training evidence preparation.
    case operational

    var isObservabilityOnly: Bool {
        switch self {
        case .audit, .operational:
            return true
        case .fm, .lexical, .acoustic, .catalog, .classifier, .fingerprint,
             .fusedScore, .metadata, .musicBed, .breakAlignment, .lexicalAutoAd,
             .audioForensics, .crossEpisodeMemory, .rhetoricalGrammar,
             .crossShowSyndication, .rediffConfirmed:
            return false
        }
    }
}

struct EvidenceEvent: Sendable, Equatable {
    let id: String
    let analysisAssetId: String
    let eventType: String
    let sourceType: EvidenceSourceType
    let atomOrdinals: String
    let evidenceJSON: String
    let scanCohortJSON: String
    let createdAt: Double
    /// Rev3-M5 (C4): run-mode discriminator persisted as a real column.
    /// Defaults to `.shadow` so existing call sites stay byte-identical.
    /// Cycle-8 reconciliation: renamed from `phase` → `runMode`.
    let runMode: SemanticScanPhase
    /// Cycle 6 B6 Rev3-M6: originating backfill phase (BackfillJobPhase.rawValue)
    /// or the sentinel `"shadow"` for legacy rows. Stored in a distinct
    /// `jobPhase` column post cycle-8 reconciliation.
    let jobPhase: String

    init(
        id: String,
        analysisAssetId: String,
        eventType: String,
        sourceType: EvidenceSourceType,
        atomOrdinals: String,
        evidenceJSON: String,
        scanCohortJSON: String,
        createdAt: Double,
        runMode: SemanticScanPhase = .shadow,
        jobPhase: String = "shadow"
    ) {
        self.id = id
        self.analysisAssetId = analysisAssetId
        self.eventType = eventType
        self.sourceType = sourceType
        self.atomOrdinals = atomOrdinals
        self.evidenceJSON = evidenceJSON
        self.scanCohortJSON = scanCohortJSON
        self.createdAt = createdAt
        self.runMode = runMode
        self.jobPhase = jobPhase
    }
}
