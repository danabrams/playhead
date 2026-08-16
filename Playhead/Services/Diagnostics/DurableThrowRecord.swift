// DurableThrowRecord.swift
// playhead-3lc3: four production sites wrote a Swift value's DESCRIPTION into
// three durable columns. It is the defect `playhead-59c8` removed from
// `backfill_jobs.deferReason`'s generic arm, `playhead-sckv` removed from the
// same column's typed arm, and `playhead-v7q6` forbids by name — surviving in
// the three columns nobody had swept.
//
// ===== THE FOUR LINES =====
//
//   1. `AnalysisWorkScheduler` outer catch, TERMINAL arm -> analysis_jobs.lastErrorCode
//          lastErrorCode: "\(Self.maxAttemptsReachedPrefix)\(error.localizedDescription)"
//   2. `AnalysisWorkScheduler` outer catch, RETRY arm     -> analysis_jobs.lastErrorCode
//          lastErrorCode: error.localizedDescription
//   3. `AnalysisCoordinator.runPipeline` outer catch      -> analysis_sessions.failureReason
//          failureReason: String(describing: error)
//      (`resumeBackfillForTesting` is the same line on a DEBUG seam.)
//   4. `BackgroundProcessingService` pre-analysis recovery -> background_task_runs.lastErrorCode
//          lastErrorCode: String(describing: error)
//
// ===== `localizedDescription` IS THE WORSE OF THE TWO AND READS LIKE THE SAFER =====
//
// It is LOCALIZED. The same failure produces different bytes on a device whose
// language differs, so the column cannot be grouped ACROSS devices at all — and
// it is worse than that within one device, because neither `AnalysisStoreError`
// nor `AnalysisCoordinatorError` conforms to `LocalizedError`. Foundation then
// synthesises the description from the bridged `NSError`, and what lands in the
// column is a SENTENCE plus an ENUM ORDINAL. Measured by running it, because the
// prediction was wrong — see below:
//
//     AnalysisStoreError.notFound.localizedDescription
//         -> "The operation couldn’t be completed. (Playhead.AnalysisStoreError error 12.)"
//     AnalysisCoordinatorError.noAudioAvailable(episodeId:).localizedDescription
//         -> "The operation couldn’t be completed. (Playhead.AnalysisCoordinatorError error 1.)"
//
// Read what that column held: a localized apology and a number. Note the
// typographic apostrophe — U+2019, not `'` — so even a same-locale `GROUP BY` is
// one Unicode normalisation away from splitting the population.
//
// **AND THE NUMBER IS NOT THE DECLARATION INDEX, WHICH IS THE STRONGER FORM OF
// THE SAME COMPLAINT.** The first draft of `DurableThrowRecordTests` predicted
// `4`, because `notFound` is the fifth case declared. It is **12**. The bridge
// uses the enum's TAG, and a multi-payload enum lays its PAYLOAD cases out first
// in declaration order and its EMPTY cases after them — the same layout
// `UnclassifiedModelFailure`'s header derives for `ModelManagerError` ("29
// payload cases (tags 0…28) then 20 empty cases"). Twelve of these thirteen
// cases carry a payload, so the single empty one lands at 12. So the persisted
// number moves not only when a case is added or reordered, but when an
// ASSOCIATED VALUE IS ADDED TO ANY EXISTING CASE — no case added, none renamed,
// none reordered, and every historical row now names a different failure. That
// was learned by writing the prediction down and watching the rail go red, which
// is the only way this file's claims are allowed to be made.
//
// `String(describing:)` at least produces one spelling per build. But not the
// spelling anyone predicts: BOTH error types here conform to
// `CustomStringConvertible`, which `String(describing:)` prefers over
// reflection, so what the column got was the enum's PROSE and not its case —
// `.notFound` persisted as `Row not found`, which names no case at all. That is
// the trap `playhead-sckv` measured on `AnalysisStoreError` and it repeats here
// on `AnalysisCoordinatorError`. Verify by running, never by reading the type.
//
// ===== WHY THIS IS 59c8's SHAPE AND NOT sckv's, WHICH IS A REAL DIFFERENCE ====
//
// `playhead-sckv` records `case=<caseName>` because its site catches a TYPED
// `AnalysisStoreError` — a closed enum of thirteen cases with a compile-time
// name for each. **All four sites here catch `any Error`**: the scheduler's
// outer catch stands under a runner, a store and an asset resolver; the
// coordinator's stands under the whole pipeline; the recovery task's stands
// under `reconcile()` and a drain. There is no case name to carry, because there
// is no enum.
//
// That is exactly the situation `playhead-59c8` faced on the generic arm, and
// its answer is the one reused here verbatim: an error's stable machine identity
// is its bridged `(domain, code)` plus the DEEPEST `(domain, code)` its
// `NSUnderlyingErrorKey` chain reaches. `error as NSError` is total in Swift, so
// it never fails and never guesses. This file therefore delegates BOTH halves of
// the grammar to ``UnclassifiedModelFailure`` — `identity(of:)` and
// `sanitize(_:)` — rather than restating them, for the reason
// ``StoreFailureRecord/sanitize(_:)`` gives one column over: one implementation
// cannot drift from itself.
//
// ===== WHAT THE THREE COLUMNS HOLD TODAY, COUNTED RATHER THAN ASSUMED =====
//
// Five preserved device pulls, db-pull8 → db-pull12 (2026-08-13 09:46 →
// 2026-08-15 20:09).
//
// **COUNT ROW IDENTITIES ACROSS THE UNION, NOT PER-PULL TOTALS SUMMED.** The
// first draft of this paragraph added the five snapshots together and reported
// 26 non-null job causes, 44 background ones and 9 sessions. These are five
// photographs of three growing tables, so that arithmetic counts the same row up
// to five times — this repo's standing defect class committed inside the
// paragraph that measures it. The figures below are `SELECT DISTINCT` over the
// five files:
//
//   analysis_jobs.lastErrorCode       38 rows in the largest snapshot. NINE
//                                     distinct jobIds have EVER carried a
//                                     non-null cause, over FOUR distinct
//                                     spellings.
//   analysis_sessions.failureReason   FOUR distinct sessions have ever existed.
//                                     `failureReason` is NULL on every one, in
//                                     every pull.
//   background_task_runs.lastError…   268 rows in the largest snapshot.
//                                     SEVENTEEN distinct runIds have ever
//                                     carried a non-null cause, and all
//                                     seventeen are `orphan_at_launch`.
//
// **NO MIGRATION IS NEEDED, AND FOR THREE DIFFERENT REASONS — say which.**
//
//   * `analysis_sessions.failureReason`: the population is EMPTY. Four sessions,
//     NULL on all four, every pull. Nothing to repair.
//   * `background_task_runs.lastErrorCode`: seventeen rows have a cause and
//     every one is `orphan_at_launch`, a named token written by
//     `reapOrphanBackgroundTaskRuns` from a different arm. The arm this bead
//     changes has produced zero rows.
//   * `analysis_jobs.lastErrorCode`: the four spellings ever observed are
//     `coverageInsufficient:noProgress`, `backgroundWindowExpired`,
//     `transcription:cancelled` and
//     `assetResolution: Insert failed: UNIQUE constraint failed: …`. The first
//     three are named tokens from other arms. **The fourth IS prose, and FIVE of
//     the nine rows that ever carried a cause carried it** — and it is written
//     by NEITHER of the two sites this bead fixes. It comes from the
//     asset-resolution arm's `"assetResolution: \(error)"`, two lines the bead's
//     own enumeration missed; filed as `playhead-3c4k` and FIXED THERE — see the
//     next section, which is that bead's record.
//
// **AND A PULL IS A LOWER BOUND, NOT A CENSUS.** Traced per row across the five
// files, every one of those five prose rows was later CLEARED OR OVERWRITTEN by
// a named token from a different arm:
//
//   070B2272  p8=assetResolution  p9=assetResolution  p10=NULL   p11=NULL   p12=NULL
//   133D75FD  p8=assetResolution  p9=NULL             p10=bgWinExpired  p11=bgWinExpired  p12=NULL
//   26D9075B  p8=assetResolution  p9=assetResolution  p10=bgWinExpired  p11=NULL   p12=NULL
//   2BCAB736  p8=assetResolution  p9=assetResolution  p10=NULL   p11=NULL   p12=NULL
//   6966117E  p8=assetResolution  p9=assetResolution  p10=transcription:cancelled  p11=NULL  p12=NULL
//
// `lastErrorCode` is a LAST-WRITER column, not an append-only ledger: the next
// arm to touch the row overwrites it and `requeueOrphanedLease` NULLs it
// outright. So the number of times prose has been written here is strictly
// greater than the number of prose rows any pull can show, and the true count is
// unrecoverable from these files. Five is a floor.
//
// So the honest reading is the opposite of the reassuring one. This column is
// not a theoretical risk that has never fired: **it is the one of the three that
// has demonstrably taken a Swift error's description in the field, through a
// fifth site, on a majority of the rows that ever carried a cause.** The two
// arms this bead fixes have simply not been the ones to fire yet. Cheap now,
// expensive on the first field failure.
//
// ===== THE FIFTH SITE (playhead-3c4k), AND WHY IT NEEDED A FOURTH TOKEN =====
//
// The line above is the whole of `playhead-3c4k`'s reason to exist: this is the
// ONE site of the class with FIELD ROWS. The other five had produced nothing.
//
//     lastErrorCode: "\(Self.maxAttemptsReachedPrefix)assetResolution: \(error)"
//     lastErrorCode: "assetResolution: \(error)"
//
// **STRING INTERPOLATION OF AN `Error` IS `String(describing:)`, AND THAT IS WHY
// 3lc3's OWN SOURCE CANARY COULD NOT SEE IT.** `testTheSchedulerArmsNoLonger…`
// filtered every `lastErrorCode:` argument for the two SPELLINGS it had just
// removed — `localizedDescription` and `String(describing:` — and this argument
// contains neither. A rule written from the two defects in front of it was blind
// to the third spelling of the same defect, in the same file, twelve hundred
// lines up. The rail now bans an interpolated `\(error)` as well, which is the
// spelling that actually shipped, and the test asserts that a rule stated over
// only the first two would MISS it.
//
// ===== WHY A FOURTH PREFIX RATHER THAN REUSING `jobThrew` =====
//
// `jobThrewPrefix` records the condition "the job's RUN threw something this
// scheduler did not classify" — the outer catch, standing over `runTask.value`.
// This arm stands over `resolveAnalysisAssetId(for:localAudioURL:)` and fires
// BEFORE any runner exists. They are two conditions and, per ``FMDaemonRefusal``'s
// R2-Fix1 rule, two prefixes. Folding them into one would delete exactly the
// discrimination the prose already provided — the `assetResolution:` prefix is
// why this site was greppable at all, and why 3lc3's enumeration could name it.
//
// **THE STEM `assetResolution` IS DELIBERATELY PRESERVED, AND A RAIL DEPENDS ON
// IT.** `DownloadTimeAssetRegistrationTests` asserts
// `job.lastErrorCode?.contains("assetResolution") != true` and calls that "the
// precise signature of the defect" — it is the regression guard for the UNIQUE
// constraint failure that produced all five field rows. A token spelled without
// the stem would leave that rail GREEN while this arm fired: a check that reads
// as evidence of an absence it can no longer see. So the token is
// `assetResolutionThrew(…)` and the retired prose was `assetResolution: …`; they
// share fifteen characters and diverge at the sixteenth, `T` against `:`.
// Consequences, stated so a device pull can be written without guessing:
//
//     LIKE 'assetResolutionThrew(%'  -> the token, and only the token
//     LIKE 'assetResolution: %'      -> the retired prose, and only that
//     LIKE 'assetResolution%'        -> both, which is the UNION query and is
//                                       the one an operator asking "how often
//                                       has asset resolution ever failed?"
//                                       actually wants
//
// ===== NO MIGRATION, AND THE REASON IS A CENSUS RATHER THAN AN ABSENCE =====
//
// The other three columns needed no migration because their populations were
// empty. This one's is not, so the argument has to be different in kind.
// Re-counted per ROW IDENTITY over the union of the same five pulls:
//
//     38 distinct jobIds have ever existed across the five files
//      9 ever carried a non-null `lastErrorCode`
//      5 of those 9 ever carried the `assetResolution: ` prose  (56 %)
//      0 ever carried the TERMINAL spelling — every field row came from the
//        RETRY arm, so the attempt cap has never been reached here
//      0 hold it in db-pull12, the most recent pull
//
// The last line is the migration argument and it is a CENSUS, not a sample:
// db-pull12 is the whole `analysis_jobs` table at 2026-08-15 20:09 — 38 rows, of
// which exactly 2 carry any cause at all and both read
// `coverageInsufficient:noProgress`. There is nothing to migrate because
// last-writer-wins already did it: traced per row, every one of the five was
// NULLed or overwritten by a named token from a different arm within one or two
// pulls, and all five ended at `state=complete`. They are rows that failed
// transiently and then SUCCEEDED — `playhead-e6d3`'s question ("were those rows
// failed under a rule that no longer holds?") does not even arise, because they
// are not failed rows.
//
// **DO NOT READ THE FIVE AS A COUNT OF ANYTHING.** `lastErrorCode` is a
// LAST-WRITER column, so the number of times this arm has fired is strictly
// greater than the number of rows any pull can show and is unrecoverable from
// these files. Five is a floor on the FIRINGS. Zero, by contrast, is exact on
// the HOLDINGS, because a snapshot of a table is a complete enumeration of that
// table — the two numbers are different quantities and only one of them is a
// bound.
//
// ===== WHO READS THESE COLUMNS, ENUMERATED PER COLUMN =====
//
// `analysis_jobs.lastErrorCode` — ONE production consumer switches on content,
// and this bead writes into it: ``AnalysisWorkScheduler/isAttemptCapTerminal(_:)``
// tests `state == "superseded" && lastErrorCode.hasPrefix("maxAttemptsReached:")`,
// reached from `AnalysisJobReconciler`'s cap-out-retry rescue and from
// ``AnalysisWorkScheduler/isRescuableTerminal(_:)``. **Site 1 writes exactly the
// row that predicate matches**, so the `maxAttemptsReached:` prefix is
// load-bearing and is preserved verbatim; only the SUFFIX changes, from a
// localized apology to a token. The same holds for playhead-3c4k's terminal
// asset-resolution arm, which is the third writer of that prefix here.
// `isNoProgressTerminal(_:)` compares the whole
// string to `coverageInsufficient:noProgress` and cannot match either spelling.
// `PersistedStateInvariantEvaluator` tests PRESENCE (`!= nil`) and then
// sanitizes for a witness line; `ActivitySnapshotProvider` and
// `DogfoodDiagnosticsAnalysisHealth` only sanitize and print. No SQL predicate.
//
// `analysis_sessions.failureReason` — ONE production consumer switches on
// content, and it is the only SQL predicate over any of the three columns:
// `AnalysisStore.fetchFailedSessions(withFailureReasonPrefix:)`,
// `WHERE state = ? AND failureReason LIKE ?`, called from
// ``AnalysisCoordinator/recoverCoverageGuardFailures()`` with
// `coverageGuardFailureReasonPrefix = "transcript coverage "`, which then PARSES
// A DOUBLE out of the matched string and divides by it. **Nothing has written
// that prefix since `e9388f5f`** (`feat(gtt9.8): classifyBackfillTerminal +
// finalizeBackfill rewire`), which deleted the `String(format: "transcript
// coverage %.1f/%.1fs (ratio %.3f < %.3f)", …)` the sweep was built to read. So
// it is a stranded reader looking for a prefix no writer produces — filed
// separately, not repaired here. It is unaffected either way: neither the old
// spelling nor the new one can begin with `transcript coverage `, and the token
// this file emits contains no space at all.
//
// `analysis_sessions.failureReason` has two further reads, both harmless and
// both worth naming so the next sweep does not have to rediscover them:
// `DogfoodDiagnosticsAnalysisHealth` tests it for EMPTINESS to raise a
// `.missingFailureReason` staleness flag (a token is never empty, and neither
// was the prose), and interpolates it into a redacted, truncated `.retry`
// recommendation note. Neither branches on its content.
//
// `background_task_runs.lastErrorCode` — NOTHING switches on it, and **nothing
// exports it either**. `finishRun` writes it, `reapOrphanBackgroundTaskRuns`
// writes the literal `orphan_at_launch`, and the only SQL that mentions it
// besides those is `lastErrorCode = COALESCE(?, lastErrorCode)` preserving the
// old value on a NULL bind. An earlier draft of this file claimed
// `DiagnosticsExportService` exports it as `last_error_code`; that is FALSE and
// an independent enumeration caught it. That JSON key belongs to
// `DogfoodDiagnosticsAnalysisJobSnapshot`, i.e. to `analysis_jobs`; the only
// export path that touches background runs is `RediffBackgroundRunSummary`,
// which projects eight fields and drops this one.
//
// That correction makes the case for a token STRONGER rather than weaker. A
// column that no code reads and no bundle exports has exactly one consumer left
// — a human running `GROUP BY` on a device pull — which is precisely the reader
// a localized sentence defeats.
//
// ===== TWO PROPERTIES, STATED SO THEY CAN BE CHECKED =====
//
//   * **What each token reads when the thing never happened.** Each is written
//     from ONE arm, so an absent prefix is a positive claim that the arm never
//     fired. Within a token every field is present: `under=none` says the error
//     carried no underlying chain, and is never an empty value or a dropped
//     field.
//   * **Nothing here decides anything.** No retry is charged, no permanence is
//     concluded, no state is chosen. Each function turns a throw the site could
//     not classify into something a device pull can `GROUP BY`. Unlike
//     `StoreFailureRecord`, there is no second field carrying a CONCLUSION —
//     because these arms conclude nothing about the error itself. The scheduler's
//     terminal-vs-retry decision is already readable from the row (`state`,
//     `attemptCount`, and the `maxAttemptsReached:` prefix), and duplicating it
//     inside the token would be a second ruler for a quantity the row measures.
//
// LIMIT, named rather than hidden, and the same one both sibling types name:
// `PersistedStateInvariantEvaluator.sanitize` truncates to 80 characters for its
// witness line. The COLUMN holds the whole token and a device pull reads the
// column. Do not read a truncated witness as the record.
//
// ===== THE SIXTH SITE (playhead-luie), AND WHY IT NEEDED NO FIFTH CONDITION ===
//
// A FOURTH column joins the three: `rediff_day_zero_attempts.lastDetail`. TWO
// production writers, both `String(describing: error)`, both reaching it through
// the SAME field one struct later:
//
//     RediffRefetchService.swift:701   mint: .blocked(.fetchFailed, detail: String(describing: error))
//     AdDetectionService.swift:12328   failed.detail = String(describing: error)
//
// `RediffDayZeroMintOutcome.detail` → `DayZeroRediffAttemptPolicy.advance` →
// `RediffDayZeroAttemptRecord.lastDetail` → the column. Neither is a labelled
// argument named after the column, which is why playhead-3c4k's two schema-derived
// sweeps could not see either: **the description is bound to a FIELD and travels,
// and a per-call-site pattern only ever sees the first hop.** The second writer
// is not even a labelled argument at all — it is an assignment to a `var` local's
// property, so no argument-shaped rule of any spelling could have matched it.
//
// **ONE PREFIX FOR BOTH ARMS, and this is the one place in this file where that
// is the right answer rather than the lazy one.** `jobThrew` and
// `assetResolutionThrew` are two prefixes because `analysis_jobs` carries nothing
// that distinguishes the arms — the column IS the discriminator. Here the row
// carries `lastExit`, a closed `RediffDayZeroExit` rawValue, and the two arms are
// `fetch_failed` and `persist_failed`. It cannot drift from the detail it stands
// beside: `advance` writes `lastExit: outcome.exit` and
// `lastDetail: outcome.detail` unconditionally, from the SAME `outcome`, in the
// SAME initialiser call — and `noteRediffDayZeroSuppression`, the only other
// writer of the row, touches neither on conflict. So a second prefix would be a
// SECOND RULER for a quantity the row already measures exactly, which is the
// thing this file's closing section forbids. Read the arm off `lastExit`:
//
//     lastExit='fetch_failed'   + dayZeroThrew(…)  -> the k-way B-copy fetch threw
//     lastExit='persist_failed' + dayZeroThrew(…)  -> upsertHotPathAdWindows threw
//
// ===== IT HAS FIRED IN THE FIELD, AND THE TWO ROWS ARE THE ARGUMENT ==========
//
// **COUNT OVER EVERY PRESERVED DATABASE, NOT THE FIVE YOU WERE HANDED.** The
// first draft of this section counted db-pull8…12 alone, found ZERO rows with a
// non-null `lastDetail`, and concluded "the population is empty". That is this
// repo's standing defect class one level up — a census of the databases somebody
// happened to name, read as a census of the databases that exist. There are
// NINETEEN preserved device databases carrying `rediff_day_zero_attempts` under
// the session scratchpads, spanning 2026-08-02 → 2026-08-16, and db-pull8…12 are
// five of them. Re-counted per ROW IDENTITY over the union of all nineteen:
//
//     21  distinct `analysisAssetId`s have ever existed in the table
//      2  have ever carried a non-null `lastDetail` — BOTH `lastExit=fetch_failed`
//      0  hold one in the most recent pull (2026-08-16 01:04), or in ANY pull
//         from db-pull8 (2026-08-13 09:46) onward
//
// So the FIRINGS are ≥ 2 (a floor — `lastDetail` is last-writer-wins) and the
// HOLDINGS are 0 (exact — a snapshot enumerates the table). Both prose rows live
// in `3gzp/gt.sqlite`, a ground-truth snapshot of device state from before the
// wipe-and-reinstall; neither asset survives into any later pull, so **there is
// nothing to migrate**, and the reason is a wipe rather than a repair.
//
// **WHAT THOSE TWO ROWS ACTUALLY HELD IS THE WHOLE CASE, so it is quoted rather
// than characterised.** Both are 200 characters — i.e. BOTH were cut by
// `detailCharCap`, mid-word, with no marker:
//
//   F4CE7F47 (2026-08-05 21:02:28)
//     Error Domain=NSURLErrorDomain Code=-1001 "The request timed out." UserInfo=
//     {_kCFStreamErrorCodeKey=-2102, NSUnderlyingError=0x1502a0d20 {Error Domain=
//     kCFErrorDomainCFNetwork Code=-1001 "(null)" UserIn
//   FCDDB309 (2026-08-02 04:45:44)
//     …identical, except NSUnderlyingError=0x11cc169d0
//
// Four things at once, and the third is the one no earlier site in this series
// could demonstrate:
//
//   1. A LOCALIZED SENTENCE — `"The request timed out."` — so the same failure
//      does not group across devices whose language differs.
//   2. A `userInfo` DUMP. On this path `NSURLErrorFailingURLErrorKey` lives in
//      there, which is why three separate exporters refuse to project the column
//      at all (see below).
//   3. **A HEAP POINTER.** `0x1502a0d20` against `0x11cc169d0`. These two rows
//      are the SAME failure — same domain, same code, same underlying domain,
//      same underlying code — and they group as TWO DISTINCT VALUES, on one
//      device, because a per-process object address was serialised into a durable
//      column. `GROUP BY lastDetail` over this table can never return a count
//      above one. That is not "prose is hard to group": it is a column whose
//      cardinality equals its row count by construction.
//   4. The cut lands INSIDE the underlying chain, so the 200 characters do not
//      even carry the one thing a reader wants. The token carries it in 84:
//      `dayZeroThrew(domain=NSURLErrorDomain,code=-1001,under=<deepest>/…)`,
//      identical for both rows.
//
// **AND NOTHING READS IT — enumerated per consumer kind, because "nobody reads
// it" is a claim about a search.** Over the whole tree plus `scripts/`, `docs/`
// and every `.md`:
//
//     SQL PREDICATE       0   no WHERE, no LIKE, no GROUP BY, no ORDER BY, no index
//     EXPORT              0   the bundle, the dogfood payload and every JSON surface
//     UI STRING           0
//     LOG                 2   the two `.dayZeroUnmarked` recorder lines
//     PROJECTION          4   two in the SELECT/read path, one in the bind, one the
//                             mint→record hop that this token now feeds
//     DELIBERATE EXCLUSION 6  four production doc sites, two test ones, plus one
//                             documented device-pull query that omits it
//
// That is why this site is P2 rather than P1 — the risk is bounded by a cap and
// a redaction rather than open — and it is exactly why the token matters. A
// column no code reads and no bundle exports has ONE consumer left: a human
// running `GROUP BY` on a device pull. `playhead-3oyz`'s bead body is that human,
// transcribing this very field off a 2026-08-06 pull by hand. A heap pointer is
// what a `String(describing:)` gives them.
//
// The exclusions are KEPT and were not weakened by this change. Their stated
// justification ("it is `String(describing: error)`") is updated where it appears
// (`DiagnosticsBundleBuilder` twice, `DiagnosticsBundle`), because a rule whose
// reason has stopped being true is a rule the next reader deletes; the RULE
// stands on a different footing, which those comments now give: the field is
// still `String?`, so what makes it safe is a property of two call sites and not
// of the column, and an exclusion is what keeps a future producer's mistake from
// becoming a disclosure without anyone editing the exporter.
//
// **THE URL CANNOT REACH THE TOKEN, WHICH IS THE PROPERTY THE EXCLUSIONS WERE
// STANDING IN FOR.** `identityFields(of:)` reads `NSError.domain` and
// `NSError.code` and walks `NSUnderlyingErrorKey`. It never touches `userInfo`
// otherwise, and `NSURLErrorFailingURLErrorKey` / `NSURLErrorFailingURLStringErrorKey`
// live there. A timeout on a private enclosure used to persist as
// `Error Domain=NSURLErrorDomain Code=-1001 … NSErrorFailingURLKey=https://…mp3`;
// it now persists as `dayZeroThrew(domain=NSURLErrorDomain,code=-1001,under=none)`,
// 58 characters, no URL in it by construction.
//
// LIMIT, measured rather than asserted: `DayZeroRediffAttemptPolicy.detailCharCap`
// truncates at 200 and the token is NOT self-marking when cut. Worst realistic
// case — both domains at ``UnclassifiedModelFailure/maxDomainLength`` (64) and
// both codes eleven digits — is **187** characters, so the headroom is thirteen;
// a 20-digit code on BOTH levels would reach 203 and lose the closing paren. No
// `NSError` has a code of that magnitude (a native Swift error's code is its case
// index), so this is named rather than fixed, and `theTokenFitsUnderTheDetailCap`
// pins the 187 exactly rather than with a `<`. **The first prediction written
// here was 175 and the test measured 187** — a signed eleven-digit code is twelve
// characters and this token carries two of them — which is the same discipline
// this file's `localizedDescription` section records: write the number down,
// then run it.

// ===== THE SEVENTH SITE (playhead-q93o): THE SAME COLUMN, A THIRD PATH ======
//
// `analysis_jobs.lastErrorCode` for the third time, reached by a road neither
// playhead-3lc3 nor playhead-3c4k could see — and it walked past 3lc3's own
// rail, in production.
//
// **THE DATES, BECAUSE THE OBVIOUS SENTENCE IS THE WRONG ONE.** The first draft
// of this section said the rail "stayed green for four months". It did not: the
// five sites landed with the runner itself on **2026-04-03** (`94406761`) and
// `DurableThrowRecordSourceCanaryTests` landed on **2026-08-16** (`1d904708`),
// hours before this bead. The rail did not decay — **it was green against a live
// instance in its own column from the moment it was written**, which is the
// sharper complaint and the one worth keeping. A rule authored from the defects
// in front of it, in the file in front of it.
//
// FIVE producers in `AnalysisJobRunner` built a description into an
// `AnalysisOutcome.StopReason.failed` payload:
//
//     404   .failed("decode: \(error)")
//     541   .failed("features: \(error)")
//     976   .failed("fetchChunks: \(error)")
//     1010  .failed("hotPath: \(error)")
//     1073  .failed("backfill: \(error)")
//
// TWO consumers in `AnalysisWorkScheduler` put that payload in the column
// (`lastErrorCode: "\(maxAttemptsReachedPrefix)\(reason)"` terminal,
// `lastErrorCode: reason` retry), and a THIRD puts it in a second durable
// column (`"runner_reason": reason` -> `work_journal.metadata`).
//
// ===== WHY THE RAIL WAS GREEN, WHICH IS THE POINT OF THIS SITE =============
//
// `DurableThrowRecordSourceCanaryTests.testTheSchedulerArmsNoLongerPersistA…`
// filters every `lastErrorCode:` ARGUMENT in the scheduler for three spellings.
// The two arms above spell `reason` and `"\(Self.maxAttemptsReachedPrefix)\(reason)"`.
// Neither contains any of the three, so the check was green while the column
// took prose.
//
// That is mutation DT05's escape — "prose laundered through a local while every
// argument stays wholesome" — SHIPPED, at the exact site the rail was written to
// guard, surviving because the laundering happens in a DIFFERENT FILE two hops
// away. 3lc3's per-SITE fix for DT05 reads the binding of `throwRecord`; `reason`
// is not bound at that site at all, it arrives as a `case .failed(let reason)`
// pattern binding whose value was built by another module-mate entirely.
//
// **NO ARGUMENT-SHAPED RULE AT EITHER END CAN SEE BOTH HALVES.** At the writing
// end the argument is an identifier; at the building end there is no durable
// column in sight. The rail that replaces it is stated over the CARRIER — every
// construction of `StopReason.failed(code:)` / `.interrupted(code:)` in the whole
// production tree — and the payload LABEL exists so the compiler forces one
// spelling for it to enumerate. See `AnalysisOutcome.swift`.
//
// ===== IT HAS FIRED, TWICE, AND THE BEAD SAID IT HAD NOT =================
//
// The bead body reported "ZERO rows carry this bead's prose today … read that as
// 'not yet', not as 'safe'", counted over the nineteen databases
// `playhead-luie` had walked. Re-counted per ROW IDENTITY over the union of
// **every** preserved database on this box — 21 sqlite files, **13 distinct by
// content hash**, 2026-04-25 -> 2026-08-16 — this arm has produced field rows,
// and one of them is on the arm that has no witness anywhere else in this file:
//
//     107  distinct jobIds have ever existed
//      29  ever carried a non-null `lastErrorCode`
//       1  `decode: Decoding failed: Operation Interrupted`                  (RETRY arm)
//       1  `maxAttemptsReached:decode: Decoding failed: Operation Interrupted` (TERMINAL arm)
//
// Both rows are in `/Users/dabrams/playhead/.captures/2026-04-25/…/analysis.sqlite`,
// a real `.xcappdata` app-container capture — the DB carries the two live test
// feeds, so it is a record of genuine use — that carries no
// `rediff_day_zero_attempts` table at all — which is precisely why luie's census, filtered to the databases
// that DO carry that table, could not see it. **A census of the databases
// somebody happened to name, read as a census of the databases that exist**:
// this repo's standing defect class, one level up, in the paragraph that measures
// it — the same correction luie's own header records having made.
//
//   * `323D2A22` — `state='failed'`, `attemptCount=2`, retry arm.
//   * `5C185837` — `state='superseded'`, `attemptCount=6`, `nextEligibleAt=NULL`,
//     TERMINAL arm. That row is PERMANENTLY DEAD: `workKey` is UNIQUE and
//     `insertJob` is `INSERT OR IGNORE` over a key stable across launches, so
//     every later enqueue for that episode is a silent no-op. An episode was
//     killed by this arm and the durable record of why is a sentence.
//
// **THE TERMINAL ARM IS THE ONE NOTHING ELSE WITNESSES.** playhead-3c4k's census
// found 0 of its 5 field rows on the terminal arm ("the arm with no witness, and
// the one a rail has to carry"); here it is, in the field, on this site. And
// `CapOutRetryTests.poisonedAssetTerminatesWithNamedCause` seeded
// `"maxAttemptsReached:decode: Decoding failed: Operation Interrupted"`, which is
// BYTE-IDENTICAL to `5C185837`. How it got there is not recorded; that it
// matches a field row exactly is.
//
// FIRINGS >= 2 (a FLOOR — `lastErrorCode` is last-writer-wins and
// `requeueOrphanedLease` NULLs it outright). HOLDINGS = 0 (EXACT — the newest
// pull, 2026-08-16 01:04, is a complete enumeration of `analysis_jobs`: 38 rows,
// 36 NULL and 2 `coverageInsufficient:noProgress`). **No migration, and the
// reason is that the rows are UNREACHABLE rather than repaired**: neither jobId
// appears in any of the twelve later databases, which span a documented
// wipe-and-reinstall. Say which — an absence produced by a wipe is not evidence
// that anything was fixed.
//
// ===== WHAT THE PROSE ACTUALLY WAS, AND WHY IT IS WORSE THAN IT LOOKS ======
//
// `AnalysisAudioError` is `CustomStringConvertible`, so the interpolation took
// its `description` and NOT its case — sckv's trap, a third time. What landed was
// `Decoding failed: Operation Interrupted`, which names no case, and the three
// sibling cases nobody has been unlucky enough to hit yet are worse:
//
//     .fileNotFound(url)              -> "Analysis file not found: <FILENAME>"
//     .assetUnreadable(url, err)      -> "Cannot read asset <FILENAME>: <LOCALIZED SENTENCE>"
//     .truncatedFile(url, …)          -> "Truncated file <FILENAME>: expected …s, decoded …s"
//
// A user's episode FILENAME in a durable column, plus a localized sentence
// underneath it — luie's enclosure-URL disclosure in a different column, from a
// different producer, waiting on a decode failure of a different kind. The token
// cannot carry either: ``identityFields(of:)`` reads only `domain`, `code` and
// the `NSUnderlyingErrorKey` walk, and never touches `userInfo` or a payload.
//
// ===== A SIXTH PREFIX, AND THE STAGE IS A FIELD RATHER THAN FIVE PREFIXES ==
//
// **A SIXTH CONDITION, NOT A SIXTH SPELLING OF `jobThrew`.** `jobThrewPrefix` is
// "the job's RUN threw something this scheduler did not classify" — it is written
// from the scheduler's OUTER CATCH, which stands over the dispatch, the store
// writes and `runTask.value`, and fires when an error ESCAPED the runner
// entirely. These five fire when the runner's own body CAUGHT a throw, handled
// it, and RETURNED an outcome reporting it. Two conditions reached by two code
// paths that cannot both run for one throw, so per ``FMDaemonRefusal``'s R2-Fix1
// rule, two prefixes.
//
// **ONE PREFIX FOR THE FIVE, WITH THE STAGE AS A DISCRIMINATOR**, which is
// `sessionPipelineThrew-<resumeState>`'s grammar and for the same reason: the
// five are one condition differing in WHICH STAGE, and `analysis_jobs` carries no
// stage column, so the value the write destroys is the only place the stage can
// live. Five prefixes would make "did the runner's body throw?" a five-clause
// query for a question the row asks once.
//
//     LIKE 'runnerStageThrew-%'          -> every stage throw, and only those
//     LIKE 'runnerStageThrew-hotPath(%'  -> one stage, and only that stage
//     LIKE '%Threw%'                     -> the whole family, all six conditions
//
// **THE STEMS ARE LOAD-BEARING AND ARE PRESERVED AS THE STAGE RAW VALUES**, the
// same call `playhead-3c4k` made for `assetResolution`. Enumerated rather than
// assumed: `RunnerMaterializerRegressionTests` asserts
// `msg.contains("backfill")` and `msg.contains("hotPath")` on the outcome
// payload, and those are the only two greps of any of the five stems anywhere in
// the tree. `runnerStageThrew-backfill(…)` and `runnerStageThrew-hotPath(…)`
// satisfy both; a stage spelled any other way would leave them GREEN while the
// arm fired.
//
// The retired prose and the token separate exactly, so a device pull can be
// written without guessing:
//
//     LIKE 'decode: %'                   -> the retired prose, and only that
//     LIKE 'runnerStageThrew-decode(%'   -> the token, and only that
//
// ===== THE TWO CONTENT-SWITCHING READERS, RE-VERIFIED ======================
//
// `isAttemptCapTerminal(_:)` is `state == "superseded" && lastErrorCode.hasPrefix(
// "maxAttemptsReached:")`. The terminal arm is UNCHANGED — it still writes the
// prefix in front — so a capped row reads `maxAttemptsReached:runnerStageThrew-decode(…)`
// and stays rescuable. Field row `5C185837` is exactly that row with the old
// suffix.
//
// `isNoProgressTerminal(_:)` is `state == "complete" && lastErrorCode ==
// "coverageInsufficient:noProgress"` — an EXACT match, and the token contains a
// `-`, a `(` and three `=` and can never equal it. **3c4k's first test of this
// reader passed for the wrong reason** (its fixture was `state='failed'`, so the
// state clause carried the assertion and the token was never consulted), so this
// bead's version pins every clause independently and carries an anti-vacuity
// control that makes the predicate return TRUE.

import Foundation

/// The durable, countable record of a throw that reached one of three
/// catch-alls standing over an open set of error types.
///
/// **Deliberately decides nothing** — see this file's header. It is the sibling
/// of ``UnclassifiedModelFailure`` in both role and grammar: that one records an
/// unclassifiable throw in `backfill_jobs.deferReason`, this one records the
/// same class of event in `analysis_jobs.lastErrorCode`,
/// `analysis_sessions.failureReason` and `background_task_runs.lastErrorCode`.
/// It is NOT a sibling of ``StoreFailureRecord``, whose `case=` field only
/// exists because its one site catches a closed thirteen-case enum.
enum DurableThrowRecord {

    /// The greppable family for `analysis_jobs.lastErrorCode`.
    ///
    /// Its own prefix, sharing none with the other causes that column holds:
    /// `maxAttemptsReached:`, `staleFingerprint:`, `assetResolutionThrew(`,
    /// `backgroundWindowExpired`, `coverageInsufficient:`, `cancelMidRun`,
    /// `transcription:` or `reconciler_unavailable` — nor with the RETIRED
    /// `assetResolution: ` prose ``assetResolutionThrewPrefix`` replaced.
    ///
    /// The prefix is the CONDITION, per ``FMDaemonRefusal``'s R2-Fix1 rule, and
    /// the condition is "the job's run threw something this scheduler did not
    /// classify". It is deliberately NOT a phase or an arm: the terminal arm
    /// still carries ``AnalysisWorkScheduler/maxAttemptsReachedPrefix`` IN FRONT
    /// of this token, which is what `isAttemptCapTerminal(_:)` matches on, so a
    /// capped row reads `maxAttemptsReached:jobThrew(…)` and a retried one reads
    /// `jobThrew(…)`. One prefix per condition; the arm is spelled by the
    /// existing prefix and by `state`.
    static let jobThrewPrefix = "jobThrew"

    /// The SECOND greppable family for `analysis_jobs.lastErrorCode`
    /// (playhead-3c4k), and the only one of the four that has ever fired in the
    /// field.
    ///
    /// A separate condition from ``jobThrewPrefix``, therefore a separate
    /// prefix: this one is "resolving the analysis asset for this job threw",
    /// raised by ``AnalysisWorkScheduler``'s pre-runner
    /// `resolveAnalysisAssetId(for:localAudioURL:)` catch, which fires BEFORE
    /// any runner exists. `jobThrew` is "the job's run threw". A pull that
    /// cannot tell those apart has lost the discrimination the retired prose
    /// already gave it, which is why this is not folded into `jobThrew`.
    ///
    /// The stem is the retired spelling's, deliberately — see this file's
    /// header, and `DownloadTimeAssetRegistrationTests`, whose regression guard
    /// for the UNIQUE-constraint defect is `contains("assetResolution")` and
    /// would go blind against a token spelled any other way. `Threw(` is what
    /// separates the token from the prose, and the separation is exact: the
    /// prose's sixteenth character is `:` and the token's is `T`.
    ///
    /// Like ``jobThrewPrefix``, the TERMINAL arm still carries
    /// ``AnalysisWorkScheduler/maxAttemptsReachedPrefix`` IN FRONT of this
    /// token — `isAttemptCapTerminal(_:)` matches on that and drives the
    /// cap-out rescue. No field row has ever taken that arm (0 of 5), so it is
    /// the arm with no witness and the one a rail has to carry.
    static let assetResolutionThrewPrefix = "assetResolutionThrew"

    /// The greppable family for `analysis_sessions.failureReason`.
    ///
    /// Sharing none with that column's other writers — `transcript coverage `
    /// (the stranded coverage-guard prefix), `Unknown state in DB: `,
    /// `corrupt session state on resume: `, or the prose
    /// `classifyBackfillTerminal` verdict reasons.
    ///
    /// The condition is "the analysis pipeline threw and the session was failed
    /// for it".
    static let sessionPipelineThrewPrefix = "sessionPipelineThrew"

    /// The greppable family for `background_task_runs.lastErrorCode`.
    ///
    /// Sharing none with that column's only other value, `orphan_at_launch`, or
    /// with the `reconciler_unavailable` written by the sibling arm eight lines
    /// up in the same handler.
    ///
    /// The condition is "the pre-analysis recovery body threw". The row already
    /// carries `entryPoint`, so the prefix does not restate which task it was.
    static let recoveryThrewPrefix = "recoveryThrew"

    /// The greppable family for `rediff_day_zero_attempts.lastDetail`
    /// (playhead-luie), written by BOTH of that column's producers.
    ///
    /// Sharing none with anything that column has ever held — which is nothing
    /// at all: zero of the nine assets in db-pull8…12 carry a non-null
    /// `lastDetail`, so unlike ``assetResolutionThrewPrefix`` there is no
    /// retired spelling whose stem a rail depends on, and the name was chosen
    /// freely rather than inherited.
    ///
    /// The condition is "a day-0 mint attempt threw something the day-0 path
    /// could not classify". ONE prefix over two arms, deliberately: see this
    /// file's header. The arm is spelled by the row's own `lastExit`
    /// (`fetch_failed` / `persist_failed`), which is written from the same
    /// `RediffDayZeroMintOutcome`, in the same statement, and therefore cannot
    /// disagree with the detail beside it. A second prefix would restate it.
    static let dayZeroThrewPrefix = "dayZeroThrew"

    /// The stage of ``AnalysisJobRunner``'s own body that caught the throw
    /// (playhead-q93o).
    ///
    /// A CLOSED enum rather than five string literals, because the stage is the
    /// discriminator the row can never answer for itself and a misspelling of it
    /// is a perfectly well-formed token naming a stage that did not run — the
    /// `sckv` SF04/SF10 shape, which no value test can see.
    ///
    /// **The raw values are the RETIRED PROSE'S STEMS, deliberately.** They are
    /// what `RunnerMaterializerRegressionTests` greps (`msg.contains("backfill")`,
    /// `msg.contains("hotPath")`) — the only greps of any of the five anywhere in
    /// the tree — so a stage spelled otherwise would leave those rails green
    /// while the arm fired. Same call playhead-3c4k made for `assetResolution`.
    enum RunnerStage: String, CaseIterable, Sendable {
        /// `audioProvider.decode(fileURL:episodeID:shardDuration:)` threw. The
        /// one stage with FIELD ROWS — both of this site's, on both arms.
        case decode
        /// `featureService.extractAndPersist(…)` threw.
        case features
        /// `store.fetchTranscriptChunks(assetId:)` threw before ad detection.
        case fetchChunks
        /// `adDetection.runHotPath(…)` threw.
        case hotPath
        /// `adDetection.runBackfill(…)` threw.
        case backfill
    }

    /// The SIXTH greppable family for `analysis_jobs.lastErrorCode`
    /// (playhead-q93o), and the second of the six to have fired in the field.
    ///
    /// A separate condition from ``jobThrewPrefix``, and the distinction is
    /// which side of the runner boundary the throw was caught on: `jobThrew` is
    /// written by ``AnalysisWorkScheduler``'s OUTER CATCH, standing over the
    /// dispatch, the store writes and `runTask.value`, when an error ESCAPED the
    /// runner; this one is written when the runner's own body CAUGHT a throw,
    /// handled it, and RETURNED an outcome reporting it. One throw can only ever
    /// take one of those paths, and a pull that cannot tell them apart has lost
    /// the discrimination the retired prose already gave it.
    ///
    /// Sharing no prefix with anything else the column holds:
    /// `maxAttemptsReached:`, `staleFingerprint:`, `jobThrew(`,
    /// `assetResolutionThrew(`, `backgroundWindowExpired`,
    /// `coverageInsufficient:`, `cancelMidRun`, `transcription:`,
    /// `reconciler_unavailable` — nor with the RETIRED stems it replaced, which
    /// separate at the character after the stage name (`(` against `:`).
    ///
    /// Like the two above, the TERMINAL arm carries
    /// ``AnalysisWorkScheduler/maxAttemptsReachedPrefix`` IN FRONT of this token
    /// and the retry arm writes it bare. **Unlike them, BOTH arms have a field
    /// witness** — see this file's q93o section.
    static let runnerStageThrewPrefix = "runnerStageThrew"

    /// The identity fields shared by all three tokens:
    /// `domain=…,code=…,under=…`.
    ///
    /// Delegates wholly to ``UnclassifiedModelFailure/identity(of:)`` and
    /// ``UnclassifiedModelFailure/sanitize(_:)``. Not re-derived here, and not
    /// re-bounded here: that type's 64-character domain budget, its
    /// head-plus-tail truncation with a `~` marker, and its eight-level cap on
    /// the underlying walk are the measured values `playhead-59c8` established,
    /// and a second copy of them is a second ruler.
    ///
    /// `under=none` when the error carried no `NSUnderlyingErrorKey` /
    /// `NSMultipleUnderlyingErrorsKey` chain — a WORD, so a reader can tell "no
    /// underlying error" from "the field was dropped".
    static func identityFields(of error: Error) -> String {
        let identity = UnclassifiedModelFailure.identity(of: error)
        let under: String
        if let underlyingDomain = identity.underlyingDomain,
           let underlyingCode = identity.underlyingCode {
            under = "\(UnclassifiedModelFailure.sanitize(underlyingDomain))/\(underlyingCode)"
        } else {
            under = UnclassifiedModelFailure.noUnderlyingToken
        }
        return "domain=\(UnclassifiedModelFailure.sanitize(identity.domain))"
            + ",code=\(identity.code)"
            + ",under=\(under)"
    }

    /// The durable cause for `analysis_jobs.lastErrorCode`, written by
    /// ``AnalysisWorkScheduler``'s outer catch.
    ///
    /// `jobThrew(domain=…,code=…,under=…)`. The TERMINAL arm prepends
    /// ``AnalysisWorkScheduler/maxAttemptsReachedPrefix`` to this value — that
    /// prefix is what `isAttemptCapTerminal(_:)` matches, and it must stay in
    /// front. The retry arm writes this value alone.
    static func jobLastErrorCode(for error: Error) -> String {
        "\(jobThrewPrefix)(\(identityFields(of: error)))"
    }

    /// The durable cause for `analysis_jobs.lastErrorCode`, written by
    /// ``AnalysisWorkScheduler``'s pre-runner asset-resolution catch
    /// (playhead-3c4k).
    ///
    /// `assetResolutionThrew(domain=…,code=…,under=…)`. The TERMINAL arm
    /// prepends ``AnalysisWorkScheduler/maxAttemptsReachedPrefix``; the retry
    /// arm writes this value alone. Identical treatment to
    /// ``jobLastErrorCode(for:)`` and deliberately so — the two arms differ in
    /// the CONDITION they record, not in the grammar they record it with.
    static func assetResolutionLastErrorCode(for error: Error) -> String {
        "\(assetResolutionThrewPrefix)(\(identityFields(of: error)))"
    }

    /// The durable cause for `analysis_sessions.failureReason`, written by
    /// ``AnalysisCoordinator``'s pipeline catch-all.
    ///
    /// `sessionPipelineThrew-<resumeState>(domain=…,code=…,under=…)`.
    ///
    /// - Parameter resumeState: the state the pipeline was DRIVING when it
    ///   threw — the one field the row itself can never answer, because the
    ///   write that carries this reason overwrites `analysis_sessions.state`
    ///   with `failed`. A `queued` throw and a `backfill` throw are different
    ///   failures with the same identity, and without this they are one string.
    ///   This is the analogue of ``UnclassifiedModelFailure``'s `<phase>`, and
    ///   it is the only place any of the three tokens carries a second
    ///   discriminator, because it is the only place one is destroyed by the
    ///   write.
    static func sessionFailureReason(for error: Error, resumeState: SessionState) -> String {
        "\(sessionPipelineThrewPrefix)-\(resumeState.rawValue)(\(identityFields(of: error)))"
    }

    /// The durable cause for `background_task_runs.lastErrorCode`, written by
    /// the pre-analysis recovery task's failure arm.
    ///
    /// `recoveryThrew(domain=…,code=…,under=…)`.
    static func backgroundTaskLastErrorCode(for error: Error) -> String {
        "\(recoveryThrewPrefix)(\(identityFields(of: error)))"
    }

    /// The durable cause for `rediff_day_zero_attempts.lastDetail`
    /// (playhead-luie), written by BOTH day-0 arms that catch a throw:
    /// `RediffRefetchService`'s k-way fetch catch (`.fetchFailed`) and
    /// `AdDetectionService.mintByteExactDayZeroMarks`'s persist catch
    /// (`.persistFailed`).
    ///
    /// `dayZeroThrew(domain=…,code=…,under=…)`. No arm discriminator — the
    /// record's `lastExit` carries it and is written from the same outcome in
    /// the same statement. See ``dayZeroThrewPrefix``.
    static func dayZeroAttemptDetail(for error: Error) -> String {
        "\(dayZeroThrewPrefix)(\(identityFields(of: error)))"
    }

    /// The durable cause for `analysis_jobs.lastErrorCode` when
    /// ``AnalysisJobRunner``'s own body caught a throw at a named stage and
    /// reported it as an outcome (playhead-q93o).
    ///
    /// `runnerStageThrew-<stage>(domain=…,code=…,under=…)`.
    ///
    /// - Parameter stage: which of the runner's five catches this was. The one
    ///   field the row itself can never answer — `analysis_jobs` has no stage
    ///   column, and the write that carries this value overwrites nothing that
    ///   would imply one. This is the analogue of
    ///   ``sessionFailureReason(for:resumeState:)``'s `<resumeState>`, and the
    ///   reason the five sites share ONE prefix instead of minting five.
    ///
    /// The value travels as an `AnalysisOutcome.StopReason` associated value
    /// into another file before it reaches the column; see
    /// ``AnalysisOutcome/StopReason/failed(code:)`` for why that payload carries
    /// a label.
    static func runnerStageLastErrorCode(for error: Error, stage: RunnerStage) -> String {
        "\(runnerStageThrewPrefix)-\(stage.rawValue)(\(identityFields(of: error)))"
    }
}
