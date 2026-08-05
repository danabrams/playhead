#!/bin/bash
#
# mutation-battery.sh — playhead-i08e
#
# WHAT THIS IS FOR
# ----------------
# The correction seams in `SkipOrchestrator` are guarded by a set of invariants
# that are easy to break by accident and invisible when broken: a lifecycle
# guard in the wrong place, a `return` where a `break` belongs, an early exit
# keyed on an optional dependency, a calibration effect attributed to the show
# that is live NOW rather than the one captured at gesture time. Nine review
# rounds of playhead-i08e re-derived the same core mutations by hand every time,
# at roughly 14 minutes of build per round. This script encodes that battery so
# the re-derivation is a command instead of an exercise.
#
# Each entry names one mutation: the exact source edit that reproduces a real
# defect, and the test(s) that MUST go red when it is applied. The script
# applies the mutation, runs only the focused suites (`FOCUSED_SUITES` below —
# six of them now, not the three this line used to promise), checks the expected
# tests actually failed, and restores the tree with `git checkout --`.
#
#   KILLED   — the expected test(s) failed. The rail works.
#   SURVIVED — the mutation was applied, the suites ran, and the expected
#              test(s) still passed.
#
# A SURVIVOR IS A MISSING TEST, NOT A BROKEN SCRIPT. Do not "fix" a survivor by
# relaxing its expectation or deleting the entry. A survivor means the codebase
# accepts a defect silently; the fix is a test that rejects it. (The one honest
# exception: if the source moved on and the mutation no longer reproduces the
# defect it describes, rewrite the EDIT — never the expectation.)
#
# K2 SERIES STATUS — playhead-mptr, 2026-08-02
#   The artifact-backed shard ORDERING (unread audio before audio we already
#   hold). 10 entries K201-K210, 4 batches (200-203).
#   FINAL 10 KILLED / 0 SURVIVED / 0 ERROR, 5 builds — batches 200, 201, 202,
#   203, then K206 alone after its expectation string was corrected.
#
#   TWO THINGS THAT COST A BUILD EACH, WORTH KNOWING BEFORE ADDING A SERIES:
#
#   1. The 4th field is the EXACT `@Test` display name, matched verbatim against
#      the observed failures — it is not a prose description. Every K2 entry
#      first came back `expected test never ran` while the mutation had in fact
#      killed its test perfectly. Check the name with `grep '@Test("'`, and note
#      the field is SPLIT ON ';', so a test whose name contains a semicolon can
#      never be matched (one K2 test was renamed for exactly this).
#
#   2. `--dry-run` DOES NOT RELIABLY RESTORE. Running batches 200-203 back to
#      back in one shell loop left six mutations live on disk while every batch
#      printed "the tree was restored"; batch 203 then reported phantom "anchor
#      drift" because it was patching an already-mutated file. Real runs leave
#      the tree dirty too. Nothing reached HEAD only because commits staged
#      EXPLICIT PATHS — which is the same discipline the `git add -A` warning at
#      the top of this file demands, and this is the second mechanism that makes
#      it necessary. ALWAYS `git checkout -- .` and re-check `git status` between
#      invocations, and never trust the restore message alone.
#
# THE R AND P SERIES LIVE ELSEWHERE (playhead-voez, playhead-3nfa)
# ----------------------------------------------------------------
# The P series — the disk-headroom preflight, `scripts/disk_preflight.py` plus
# the preflight half of `scripts/fast-gate.sh` and `scripts/disk-cleanup.sh` —
# is in `scripts/mutation-battery-disk-preflight.py`, for the same reason the R
# series is, and it reuses the R series' engine outright rather than copying it.
#
# The rails for the gate baseline — `scripts/gate_baseline.py` and the baseline
# half of `scripts/fast-gate.sh` — are in
# `scripts/mutation-battery-gate-baseline.py`, not here. This script is
# structurally a SWIFT battery: MUTABLE_FILES are Swift sources, `apply_mutation`
# resolves a path from a Swift-file variable, and `run_focused` is an xcodebuild
# run whose verdicts are read out of Swift Testing console glyphs. The R series
# mutates Python and bash and is judged by `python3 -m unittest` in about a
# second, so hosting it here would mean a second runner, a second failure
# extractor and a second "did it run" extractor bolted onto the repo's
# certification tool for no gain. Same discipline, same vocabulary (KILLED /
# SURVIVED / a survivor is a coverage hole), same pre-flight check that every
# expectation names a test that actually runs — see that file's header.
#
# THE TY SERIES IS A DIFFERENT VERDICT, NOT A DIFFERENT RUNNER (playhead-x0lb)
# ---------------------------------------------------------------------------
# `scripts/mutation-battery-untypeable.py` mutates SWIFT, like this script, and
# still cannot live here — because its kill is `UNTYPEABLE`: the mutated source
# must FAIL TO COMPILE. This script reads its verdict out of Swift Testing
# console glyphs, and a build that never produced a test binary emits none, so a
# compile failure is indistinguishable here from an infrastructure error.
#
# It exists because the defect class it covers is invisible to any test: two
# quantities share a unit and one is read as the other, and they are EQUAL on
# every fixture a test author would write. playhead-fil5 R3 divided the
# transcript union by the duration and called it reach — nought of twelve field
# assets cleared the gate and every test passed, because every fixture was
# single-chunk. A compile failure is the strongest kill available; TY rails also
# check the diagnostic KIND, so a build that breaks for a syntax reason reports
# WRONG-ERROR rather than being credited.
#
# HOW TO ADD A MUTATION
# ---------------------
#   1. Add a record to MUTATIONS: "NAME|BATCH|FILE_KEY|Expected Test Name;..."
#      Expected names are Swift Testing DISPLAY names (the string in `@Test(…)`),
#      because that is what xcodebuild prints. Separator is `;`.
#   2. Add a `NAME)` case to `apply_mutation` that calls `patch` with an OLD
#      snippet that occurs EXACTLY ONCE in the file (the patcher aborts
#      otherwise) and its NEW replacement.
#   3. Pick a BATCH. Mutations in the same batch are applied together and
#      verified by their distinct failures, so a batch must contain no two
#      mutations that can redden the same test. When in doubt, give it a batch
#      of its own — correctness first, build count second.
#
# BASELINE
# --------
# Every run starts with one UNMUTATED build of the focused suites and refuses to
# continue if anything is already red — otherwise a pre-existing failure would
# be miscredited as a kill for every mutation that names that test. That costs
# one build; `PLAYHEAD_MB_SKIP_BASELINE=1` skips it when you have just run the
# suites green by hand.
#
# BATCHING
# --------
# Mutations with disjoint expected-failure sets are applied together and told
# apart by WHICH test failed, so N mutations cost 1 build instead of N. The
# limit is set by the most-contested test: seven distinct mutations must each
# redden `listenRevertSurvivesEpisodeReplacement`, so seven batches is the
# floor. Mutations that interact — two edits to the same loop, two edits whose
# blast radius overlaps — are kept in separate batches even when their
# *expected* tests differ, because a false KILL is worse than an extra build.
#
# USAGE
#   scripts/mutation-battery.sh              # whole battery
#   scripts/mutation-battery.sh --list       # print the battery, run nothing
#   scripts/mutation-battery.sh --only M07   # one mutation, alone
#   scripts/mutation-battery.sh --batch 3    # one batch
#   scripts/mutation-battery.sh --dry-run    # apply + diffstat + restore, no build
#
# `--dry-run` is how you check a NEW mutation's anchor before spending a build
# on it: it proves the anchor still matches exactly once and shows the edit.
#
# Exits non-zero if any mutation survives, if the tree is dirty at start, if a
# batch fails to build, or if restoration is not byte-exact.
#
# LAST WHOLE-BATTERY RUN — the single provenance line
#   2026-07-28 (playhead-auz3). 35 live entries — COUNTED, not carried over;
#   every earlier status block here quoted a figure ("26", "31", "33") that was
#   already wrong when written, and a reader naturally trusts it. If you change
#   the array, recount. 19 batches, run END TO END:
#   34 KILLED, 1 SURVIVED (M17, a known and deliberately unpinned rail — see
#   below), 0 unevaluated. 21 builds, 13m24s wall clock.
#
#   Composition, stated because it is not one invocation: the whole battery in
#   one pass (20 builds, 12m40s) came back 30 KILLED / 1 SURVIVED / 4 ERROR —
#   batch 8 failed to COMPILE on N04's stale EDIT and took N02, N03 and N06
#   down with it. The EDIT was re-cut (see the note above N04) and batch 8
#   re-run immediately, behind that same run's green baseline: 1 build, 44s,
#   4/4 KILLED. Nothing else changed between the two.
#
#   This block replaced three overlapping status blocks that each described a
#   DIFFERENT subset of batches as verified, so a reader had to intersect them
#   to discover that no whole-battery verdict existed at all. Keep it to ONE
#   provenance line. If you re-run part of the battery, say so here and say
#   which part — do not add a second status block.
#
#   Settled by this run, previously open:
#     • Batches never re-run after `FOCUSED_SUITES` grew the two
#       characterization @Suite structs (playhead-ugy4) — all now re-run.
#     • M14 and O04, whose EDITs playhead-1mq1.2.1 re-cut and dry-run verified
#       but never re-KILLED. Both KILLED here.
#     • S06 and S07 added (playhead-auz3). Both SURVIVED against the fixtures
#       as they stood — that was the point, it is what proved the two tests
#       vacuous — and both KILLED once the fixtures were repaired.
#
#   PARTIAL RE-RUN 2026-08-01 (playhead-djl0). Batches 41-47 only, added by
#   this bead: J01-J18, 18 entries, 8 builds. FINAL 18 KILLED / 0 SURVIVED /
#   0 ERROR. Batches 1-40 were NOT re-run and carry the 07-28 verdict above.
#   Recount: the array now holds 74 live entries.
#
#   PARTIAL RE-RUN 2026-08-01 (playhead-4dqe). Batches 48-52 only, added by
#   this bead: K01-K22, 22 entries, 5 batches. FINAL 22 KILLED / 0 SURVIVED /
#   0 ERROR (7 builds: 5 batches, then K15 and K10 re-run alone after the two
#   faults below were repaired). Batches 1-47 were NOT re-run and carry the
#   verdicts above. Recount: the array now holds 96 live entries.
#
#   PARTIAL RE-RUN 2026-08-01 (playhead-eks2). Batches 53-61 only, added by
#   this bead: L01-L09, 9 entries, 9 batches (one each — see the note above
#   L01). FINAL 9 KILLED / 0 SURVIVED / 0 ERROR, 10 builds, ~23m wall clock.
#   Batches 1-52 were NOT re-run and carry the verdicts above. Recount: the
#   array now holds 105 live entries.
#
#   PARTIAL RE-RUN 2026-08-01 (playhead-evc1). Batches 62-67 (V01-V06, 6 new
#   entries) plus batches 56, 57, 58 and 61 — L04, L05, L06 and L09, whose
#   expectations this bead changed. FINAL 10 KILLED / 0 SURVIVED / 0 ERROR, 11
#   builds (batch 62 and batch 64 each ERRORed once on a disk-exhaustion sim
#   crash — "Test crashed with signal kill while preparing to run tests" — and
#   KILLED on re-run after `simctl erase`; that is an environment fault, not a
#   mutation fault). Batches 1-55 and 59-60 were NOT re-run and carry the
#   verdicts above. Recount: the array now holds 111 live entries.
#
#   PARTIAL RE-RUN 2026-08-01 (playhead-y3ya). Batches 91-102 (Y01-Y18, 18 new
#   entries), 12 batches. FINAL 18 KILLED / 0 SURVIVED / 0 ERROR. Batches 1-90
#   were NOT re-run and carry the verdicts above. Recount: the array now holds
#   170 live entries.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-6qvf). Batches 160-169 (G01-G10, 10 new
#   entries, one batch each) plus batch 158 — F08, whose EDIT this bead re-cut
#   when it unified the host-read floor onto `carriesRediffByteExactWidth`.
#   FINAL 11 KILLED / 0 SURVIVED / 0 ERROR, 16 builds. Batches 1-157 and 159
#   were NOT re-run and carry the verdicts above. Recount: the array now holds
#   180 live entries.

#   R4 REVIEW ADDITIONS 2026-08-05 (playhead-e75l). Batches 507-510 — DR11
#   through DR14, one mutation each because DR11/DR12 both redden the wiring
#   canary and DR13/DR14 both redden the budget canary.
#
#   THE LESSON, and it is one lesson wearing four spellings: EVERY source
#   canary in this bead found its sites by literal substring over ONE LINE, and
#   no round had tried respelling one ACROSS lines. R4 planted eight and SEVEN
#   walked past — two hard-coded event names split by a `\` continuation or a
#   `+` join, one that SWAPPED the two event properties and used no literal at
#   all, and four ways to spell a third 30 s budget (`standard / 10`, a site
#   split over two lines, `Type . run(`, and a budget in a local with the
#   allowed name in a trailing comment). The single evasion that FAILED was
#   killed by SwiftLint's `no_space_in_method_call`, not by any rail here.
#
#   Two shapes are worth carrying forward. DR11 is the sharpest: R2's rule was
#   "both properties must be READ", which says nothing about WHICH SITE reads
#   which, so swapping them satisfied every assertion. A rail that checks a
#   name is present is not checking that the name is in the right place. And
#   DR14 is why a count FLOOR is only half a vacuity guard: a site that
#   DISAPPEARS is red, a site nobody can see is not, so adding an unreadable
#   one is free. The fix in both cases was to stop reading lines — the finders
#   now run over a collapsed / whitespace-free read of the file and judge the
#   ARGUMENT rather than the line it sits on.
#
#   R3 REVIEW ADDITIONS 2026-08-05 (playhead-e75l). Batch 506 — DR10 added and
#   DR02 MOVED here out of batch 500. Batch 506 run WITH a green baseline, then
#   500-505 re-run behind it with PLAYHEAD_MB_SKIP_BASELINE=1 on the same
#   commit. FINAL 10 KILLED / 0 SURVIVED / 0 ERROR, 8 builds, ~17m. Batches
#   1-499 were NOT re-run and carry the verdicts below.
#
#   TWO LESSONS, and the first is the same one DR08 taught wearing a different
#   hat. R2 fixed the log-EVENT canary to derive its forbidden set from
#   `FMDaemonRefusal.allCases` and left the TOKEN-FAMILY canary next to it as a
#   hand-written list of two families — the two it had just looked at.
#   `BackfillJobRunner` writes six. DR10 names the token into `underCoverage-`
#   (playhead-41mu), which the old list passes by arithmetic. When you fix one
#   list-shaped rail, the next one is usually in the same file.
#
#   The second is about BATCHES, not rails. DR02's expectation set grew to
#   include `$T_DR_STANDARD` — a variable DEFINED at R1 and referenced by no
#   mutation at all, so the runner-level "a 300 s timeout still fails and still
#   spends a retry" control had a test and no mutant. DR02 could not stay in
#   batch 500: DR01 reduces `classify` to the throttle-only ternary, under which
#   that control stays GREEN, so the batch would have reported SURVIVED for a
#   mutation DR02 kills on its own. Growing an expectation set can invalidate a
#   batch that was sound when it was written.
#
#   R2 REVIEW ADDITIONS 2026-08-05 (playhead-e75l). Batches 500-505, the DR
#   series: DR01-DR07 added at R1, DR08-DR09 added here. Batch 505 run WITH a
#   green baseline, batches 500-504 re-run behind it with
#   PLAYHEAD_MB_SKIP_BASELINE=1 on the same commit. FINAL 9 KILLED / 0 SURVIVED
#   / 0 ERROR, 7 builds, ~19m. Batches 1-499 were NOT re-run and carry the
#   verdicts above.
#
#   DR08 IS WHY THIS RE-RUN EXISTS, and it is the reusable lesson. R1 added a
#   SOURCE canary forbidding two log-event literals BY NAME — the two kvs8-era
#   spellings it had just watched go wrong. DR08 plants a literal of the event
#   THIS BEAD ADDED, one line up from DR06's anchor, and it SURVIVED: a
#   blacklist covers the mistakes already made and is blind by construction to
#   the next one. The canary now derives its forbidden set from
#   `FMDaemonRefusal.allCases`. When a rail is a list of known-bad strings, the
#   mutation worth planting is the string that is not on the list yet.
#
#   R2 REVIEW ADDITIONS 2026-08-04 (playhead-9y9e). Batch 420, RT14 — one entry,
#   KILLED on exactly ONE victim, baseline green. Its EDIT is the pre-review
#   implementation verbatim, the standard RT12 set: the broad mutant (deleting
#   the duration-sanity guard outright) also reddens
#   `overshootingNumeratorIsWithheldNotClamped`, a test the reviewed code already
#   passes, so that verdict would have said nothing about the widening.
#
#   R2 ALSO CLOSED THE FOUR RAILS R1 COULD NOT GET A VERDICT ON — RT11, SC25,
#   SC30, SC33, run ONE AT A TIME, each its own invocation: all four KILLED,
#   baseline green on each, 0 survivors / 0 errors. R1's four attempts were
#   destroyed by a concurrent battery in the same worktree, which reports as
#   "the baseline did not run tests (rc=65)". The check that costs nothing is
#   `ps -Ao command | grep -c "[x]codebuild test -scheme Playhead"` BEFORE each
#   invocation, and it is worth doing between entries, not only at the start:
#   this worktree acquired a second agent mid-session.
#
#   THE RUNNING "Recount: the array now holds N entries" FIGURE IS STALE and has
#   been for a while — measured 2026-08-04 the array holds 417 entries under 416
#   unique names (`L09` is registered twice at batch 61, both on main, which
#   predates this bead). Do not carry the old number forward by addition; count
#   it. The point of the recount line is to catch an entry that was dropped by a
#   bad merge, and a figure nobody re-derives cannot do that.

#   R1 REVIEW ADDITIONS 2026-08-04 (playhead-9y9e). Batches 418-419, RT12-RT13.
#   Both KILLED, each on exactly ONE victim. RT02 was also re-run and KILLED on
#   all three of its named victims, which is the vacuity audit doing its job.
#   Recount: the array now holds 337 live entries.
#
#   RT12 WAS RE-CUT ONCE, and the reason is the most reusable thing here. Its
#   first form was the obvious one-line revert — an unconditional
#   `fastIntervals + finalIntervals`. It KILLED, on TWENTY victims, because it
#   also deletes the watermark fallback for assets with no chunks of EITHER
#   pass, a shape half the coverage suite rests on. That verdict was worthless:
#   the code being reviewed would have failed those twenty too, so the mutant
#   proved nothing about the fix. RT12 is now the pre-review implementation
#   VERBATIM, branch and all, and reddens one test. **When a mutant's victim
#   list is much larger than the behaviour you changed, suspect that the mutant
#   is broader than the fix, not that the fix is well covered.**
#
#   THE OTHER FAULT WAS OPERATIONAL, and it is worth a line because it wastes
#   builds silently. Two batteries running against the same `.derivedData`
#   produce `unable to attach DB: … database is locked`, reported as
#   "the baseline did not run tests (rc=65)" — which looks exactly like a broken
#   anchor or a compile failure. Killing the loser then wedges the simulator,
#   and the battery's own recovery path shells out to a bare `xcrun simctl`,
#   which resolves against the GLOBAL xcode-select and dies with
#   `unable to find utility "simctl"` (the 2026-07-16 gotcha, from a new
#   direction). Export `DEVELOPER_DIR` for the whole invocation, and check
#   `ps -Ao command | grep -c "[x]codebuild test -scheme Playhead"` is 0 before
#   starting — CLAUDE.md's "ONE AT A TIME" is not only about memory.
#
#   PARTIAL RE-RUN 2026-08-04 (playhead-9y9e). Batches 413-417, RT01-RT11 — 11
#   entries in 5 batches — plus batches 404, 407, 409, 410 and 411, whose EDITs
#   this bead re-cut when it renamed `bridgedTranscriptCoveredSec(fastRanges:)`
#   to `(ranges:)` and moved the sweep onto `fetchTranscriptCoveredRanges`.
#   FINAL 11 KILLED / 0 SURVIVED / 0 ERROR for the RT series, and SC25, SC21,
#   SC30, SC31 and SC33 all KILLED again on their re-cut anchors. 12 builds
#   (1 baseline + 5 RT batches, one of them re-run, + 5 SC batches). Batches
#   1-412 other than those five were NOT re-run and carry the verdicts above.
#   Recount: the array now holds 335 live entries.
#
#   One fault, and it is the second time a mutation has been rejected by LINT
#   rather than by the compiler: RT10's first EDIT prepended an
#   `if SemanticScanClaim.isOwed(…)` arm to a chain that already ended in one,
#   which is a `duplicate_conditions` violation and therefore an ERROR under
#   `scripts/lint.sh --strict`. The whole chain had to become the anchor. A
#   mutation that cannot build is not a kill — check a new EDIT against the
#   linter, not only against `--dry-run`.
#
#   PARTIAL RE-RUN 2026-08-04 (playhead-26od). Batches 314-323, CK01-CK13 minus
#   the withdrawn CK08 — 12 entries in 9 batches. Batches 1-313 were NOT re-run
#   and carry the verdicts above. Recount: the array now holds 324 live entries.
#
#   One operational fault, recorded because it is not about a mutation and it
#   cost an hour. Batch 320 WEDGED — its `batch-<n>.log` froze, xcodebuild stayed
#   alive at 0.0 %% CPU and never exited. That is the disk-exhaustion wedge
#   `scripts/disk_preflight.py` exists to refuse, and it was reached because the
#   run set `PLAYHEAD_DISK_MIN_GIB=6`: the volume was at 10 GiB when the batch
#   started and a selective run of FOCUSED_SUITES draws ~7. Recovery is the one
#   in CLAUDE.md — TERM the script so its trap restores sources (it did, byte-
#   exact), kill xcodebuild by PID, `simctl erase`, clear `$TMPDIR/Deleting-*`
#   (3.0 GiB was stranded there), then re-run the outstanding batches at a floor
#   HIGH enough to refuse rather than wedge. Do not lower this floor to make a
#   battery fit; erase the sim between batches instead.
#
#   The second fault is worth as much as the first: the dirty-tree guard REFUSED
#   the re-run because an unrelated edit was in flight, which is exactly right
#   and is what stops `restore_sources` from eating uncommitted work. Commit
#   before re-running; do not touch a mutated file while a battery is live.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-bllt). Batches 240-247 only (BL01-BL10,
#   10 new entries, 8 batches — five of them edit ONE predicate in
#   `HotPathExtentGate.gatedLabel` and so cannot be batched together at all).
#   FINAL 10 KILLED / 0 SURVIVED / 0 ERROR, 9 builds (1 baseline + 8 batches),
#   ~18m wall clock across six invocations. Batches 1-239 were NOT re-run and
#   carry the verdicts above. Recount: the array now holds 312 live entries.
#
#   Two operational faults, both recorded because neither is about a mutation:
#     • The first attempt reported `BL01 ERROR — anchor did not apply`, which
#       reads like source drift and was not: a MUTABLE_FILES entry is TWO edits,
#       and `rec_file`'s case had not been given a `HOTGATE)` arm, so `file`
#       resolved to the empty string. Any new series adding a file will hit this.
#     • The batch loop ran `git checkout -- .` between invocations and thereby
#       discarded the UNCOMMITTED `rec_file` fix, making batches 242-247 fail for
#       a reason unrelated to their anchors. That is the header's own
#       "commit script edits before --dry-run" trap, hit from the other side:
#       the restore does not distinguish your script edit from a live mutant.
#       Restrict the between-batch restore to the SOURCE trees
#       (`git checkout -- Playhead PlayheadTests`) and the problem disappears.
#
#   One thing the run proved that is worth keeping: BL03 inverts the gate's
#   condition so that ANCHORED rows demote and invented ones auto-skip, and the
#   whole-domain MONOTONICITY sweep stays green under it — passing an
#   `"autoSkip"` through is not a rise. It is killed by the EXACT-transition and
#   anchored-row rails instead. That is the measured argument for why
#   monotonicity alone is not the safety property, stated in
#   `HotPathExtentGateMonotonicityTests`' header and now demonstrated.
#
#   Three faults found and fixed during the run, recorded because two of them
#   are traps any new series can hit:
#     • G03/G04 ERRORed on "expected test never ran". Their expected names
#       contained a SEMICOLON, which is this script's expected-test separator —
#       the name was silently split into two names that match nothing. Renamed
#       the test this bead owns; repointed G03 at a differently-named test in
#       the same suite rather than renaming one it does not own.
#     • G05 and G09 SURVIVED against expectations the mutation cannot REACH
#       (a both-markers fixture; a predicate-closure `contains(where:)`), not
#       against a coverage hole. Corrected the expectations — and G09's finding
#       also corrected a false claim in the SOURCE comment on the Equatable arm.
#     • G04's first EDIT survived at the extent tier and that WAS a real hole:
#       `SpanExtentSupport.derive` inlined its own `contains(.rediffSlot)`
#       instead of sharing the predicate, so widening the predicate left
#       auto-skip ADMISSION untouched. Fixed in source (one shared
#       `[AnchorRef].carriesRediffByteExactWidth`), not in the expectation.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-cgka). Batches 120-125 (Z01-Z12, 12 new
#   entries), 6 batches. FINAL 12 KILLED / 0 SURVIVED / 0 ERROR, 9 builds.
#   Batches 1-102 were NOT re-run and carry the verdicts above. Recount: the
#   array now holds 182 live entries.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-9v09). Batches 170-175 (H01-H10, 10 new
#   entries), 6 batches. FINAL 10 KILLED / 0 SURVIVED / 0 ERROR, 7 builds,
#   16m50s wall clock. Batches 1-169 were NOT re-run and carry the verdicts
#   above. Recount with `--list`: the array now holds 248 live entries.
#
#   Not one invocation: batch 170 ran with the baseline (2 builds), then
#   171-175 in a shell loop under `PLAYHEAD_MB_SKIP_BASELINE=1`, because
#   `--batch` is not repeatable and editing that parser to make it so is a
#   change to a shared tool for one bead's convenience. No batch was re-run and
#   no expectation was relaxed.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-gard). Batches 180-194 (I01-I21, 21 new
#   entries), 15 batches. FINAL 21 KILLED / 0 SURVIVED / 0 ERROR, 22 builds
#   (14 batches + baseline + 6 re-runs after the fixes below). Batches 1-179
#   were NOT re-run and carry the verdicts above. Recount with `--list`.
#
#   THE PRE-FLIGHT EARNED ITS KEEP AGAIN. The first attempt refused: the new
#   `revertAttributesToTheDrawingDetector` was RED under the focused set and
#   GREEN alone — `revertWindow` issues its trust write in an unstructured
#   `Task`, so the test raced it. Six mutations naming rails in that suite would
#   have been credited KILLED off a flake.
#
#   Five survivors on the first pass, and every one was a real finding:
#     • I02 — `migrationPreservesPosture` iterated `allCases where
#       consultsShowTrust`, so a mutation making EVERY class exempt emptied the
#       loop and the test passed describing nothing. The three classes are now
#       NAMED. A test whose iteration set is derived from the predicate under
#       test cannot fail when that predicate is wrong.
#     • I20 — an EQUIVALENT MUTANT: dropping the exempt class from the session
#       override's map is masked by `DetectorSkipModes.mode(for:)`'s fallback to
#       `showMode`, which the override just set to the same value. EDIT re-cut
#       to delete the assignment entirely (the stale `beginEpisode` map then
#       governs, which is a real defect).
#     • I15 — `userOverrideIsNotSilentlyUndone` read only the LEGACY counter and
#       the modes, neither of which the ledger's weight touches. Restated as
#       behaviour: one unanchored veto after an override must leave the class in
#       `auto`.
#     • I14 — materialization has TWO carriers (the veto path and the
#       correct-observation path) and removing one leaves the other's rail
#       green. The mutation now removes both.
#     • I08 and I18 — expectations naming tests the edits cannot REACH. I08's
#       demotion/escape rails are satisfied by the legacy triple when the ledger
#       does not persist (correctly so); repointed at the three claims that need
#       the ledger to hold state the scalar does not. I18's new-show rail exits
#       `resolveDetectorModes` one branch earlier; split out as I21 rather than
#       credited by association.
#
#   Every EDIT was `--dry-run` verified before any build was spent — ten
#   anchors, ten "applied exactly once and the tree was restored". That is the
#   cheap half of the F09 lesson recorded above and it caught nothing here; the
#   expensive half (a dry run proves the anchor matches, NOT that the result
#   compiles) also came back clean, all six batches building first time.
#
#   TWO ISSUE LISTS WERE READ RATHER THAN TRUSTED, per this file's standing
#   warning that a KILL proves only that SOME expectation failed. Both batched
#   pairs put a wiring mutation next to a value-type one, and both value-type
#   mutations have a blast radius that reaches the behavioural suite:
#     • H06 (`retiredCount` keyed on `isDelivered`) additionally reddens isp5's
#       golden-string test, because `armedSuggest` then counts as retired and
#       `retired=3` appears in a row that must render byte-identically. That is
#       a genuine consequence of the mutation, not of its batchmate H01 — H01
#       deletes only the stamp and leaves every row assertion green, which is
#       exactly the asymmetry that made the pair worth batching.
#     • H08 and H09 each redden `a retraction row renders retired= …` for
#       opposite reasons — H08 makes a DELIVERY row carry `retired=1`, H09 makes
#       the SWEEP row carry `delivered=2`. Their batchmates H03 and H04 are both
#       reason-carrying mutations whose expectations name the reason itself, so
#       neither could have been credited off the other's failure.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-sik9). Batches 140-144 (C01-C10, 10 new
#   entries), 5 batches. FINAL 10 KILLED / 0 SURVIVED / 0 ERROR, 5 builds
#   (1 baseline inside batch 140, then 141-144 with PLAYHEAD_MB_SKIP_BASELINE=1
#   — `--batch` is not repeatable, so this was a loop, not one invocation, and
#   no batch was re-run). Batches 1-134 were NOT re-run and carry the verdicts
#   below. The array now holds 219 live entries.
#
#   Nothing survived on the first pass, which is worth one sentence of
#   suspicion rather than celebration: five of the ten (C01-C05) mutate the
#   SAME `if`, so a positive-only test set would have let at least C03 and C04
#   through. They were killed by the `stillGuarded` table, which is the part of
#   the bead that exists for exactly that reason. The one thing this battery
#   deliberately does NOT claim: the CUE-surface pair builds its `AdWindow` at
#   the orchestrator's door, so no fusion-side mutation can reach it — those
#   two tests are an end-of-chain witness, not a rail, and are listed in no
#   expectation.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-nqey). Batches 150-158 (F01-F09, 9 new
#   entries), 9 batches — one per mutation, because F01-F05 all edit
#   `AdDetectionConfig.default` and cancel each other, and F06's blast radius
#   (arming the bare `FusionWeightConfig()`) would make any batchmate's verdict
#   noise. FINAL 9 KILLED / 0 SURVIVED / 0 ERROR. Batches 1-144 were NOT re-run
#   and carry the verdicts above. Recount with `--list`: the array now holds 228
#   live entries.
#
#   Composition, stated because it was not one invocation and because two of the
#   builds were faults of mine, not of the battery:
#     • First pass reported 8 KILLED / 1 ERROR. F09's EDIT wrote
#       `let x = episodeDuration ?? span.endTime`, which does not compile — `??`
#       yields a non-optional and `if let` needs an Optional. Re-cut with
#       `Optional(...)` and re-run alone: KILLED. `--dry-run` would NOT have
#       caught this; it proves the ANCHOR matches, not that the result builds.
#     • Batch 154 was run twice. The first attempt was killed mid-flight after a
#       `git add -A` in another window swept the live F05 mutant into a commit.
#       That is worth a line here rather than in a commit message alone, because
#       the failure is silent: the battery restores with `git checkout --`, so
#       once a mutant reaches HEAD the restore succeeds INTO the mutated state
#       and `git status` reads clean. Repaired in 35fd529c and re-run from a
#       verified-clean tree; the interrupted verdict was discarded, not reported.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-le02). Batches 230-231 (LE01-LE08, 8
#   new entries), 2 batches. FINAL 8 KILLED / 0 SURVIVED / 0 ERROR, 4 builds
#   (1 baseline + 1 batch, twice). Batches 1-217 were NOT re-run and carry the
#   verdicts below. Recount with `--list`: 302 live entries.
#
#   These are the A11 class generalised. avbn repaired ONE rail; le02 enumerated
#   the rest (`scripts/le02-negative-banner-audit.py`, 39 A11-blind test
#   functions of 75 that make a negative surfacing claim) and repaired 15.
#
#   Two results worth keeping:
#
#   LE01 is the cruellest form of the class and the reason the repairs assert
#   TWO witnesses. It arms a suggestion while leaving the census stamp intact,
#   so `lastAdWindowIngestOutcome` still reads `droppedInventorySanity`. A rail
#   that trusted the census alone survives it; only `activeSuggestWindowIDs()`
#   — the collection the suggest tier actually lands in — kills it. It also
#   demonstrated the OTHER half of the acceptance criterion: the inventory
#   suite's positive controls (pre-roll survives, post-roll survives, valid
#   spans survive, flag-OFF passes everything) all stayed GREEN under it, so
#   the new witness is targeted rather than a blanket assertion that fires on
#   every delivery.
#
#   LE08's blast radius is WIDER than its comment claims. It is described as
#   "narrowed to applied rows so its blast radius stays inside the rail it is
#   aimed at"; measured, it also reddens three other preload tests and two
#   revert rails. Harmless — nothing else in batch 231 claims those tests, so
#   attribution held — but the comment overstates the narrowing and the next
#   person should not batch anything near it on that basis.
#
#   Batch 231 first reported 4 ERROR / "batch did not build/run, rc=28". That
#   was NOT a build failure: 28 is the playhead-3nfa disk preflight refusing to
#   start. The battery reports any non-running batch as ERROR, which is the
#   right default, but rc=28 specifically means "reclaim disk and re-run", not
#   "fix the EDIT". Re-ran after erasing the destination simulator and sweeping
#   $TMPDIR/Deleting-*: 4/4 KILLED, nothing about the mutations changed.
#
#   PARTIAL RE-RUN 2026-08-02 (playhead-avbn). Batches 130-134 (A01-A11, 11 new
#   entries), 5 batches. FINAL 11 KILLED / 0 SURVIVED / 0 ERROR, 7 builds
#   (1 baseline + 5 batches + 1 re-run of batch 133). Batches 1-125 were NOT
#   re-run and carry the verdicts above. Recount: the array now holds 209 live
#   entries — the pre-avbn header said 182, which was already short by 16
#   before this bead added 11; recounted here with `--list`, not carried over.
#
#   Composition, stated because it was not one invocation. First pass reported
#   9 KILLED / 1 SURVIVED / 1 ERROR.
#
#   A11 — route `blockedByFMConsensus` to the suggest tier — SURVIVED, and the
#   survivor was right. `blockedGateValuesAreDroppedInReceiveAdWindows` made
#   five assertions and every one of them is about what did NOT happen. Since
#   playhead-d3g0 a suggest banner is ARMED at delivery and EMITTED only when
#   the playhead ENTERS the span, so routing a blocked gate to the suggest tier
#   arms a banner and emits nothing inside the test 100 ms window: all five stay
#   green while the span banners in the field the moment playback reaches it.
#   The rail gained a POSITIVE witness — playhead-isp5 census row, which names
#   the terminal disposition and its cause — plus an `armedSuggest` count of
#   zero. Batch 133 re-run: 3/3 KILLED.
#
#   A04 reported ERROR ("expected test never ran") with its expected failure
#   visibly listed two lines above. `extract_ran` matched the marker only at the
#   START of a line, and this run interleaved `XCTestOutputBarrier` into the
#   `◇ Test "…" started` line — word characters, so `^\W*` could not skip them.
#   `extract_failures` had the identical exposure and got lucky. Both now scan
#   for the marker anywhere in the line; see the note there for why that cannot
#   manufacture a KILL.
#
#   Composition, stated because it was not one invocation. First pass, batches
#   120-125: 11 KILLED, 1 SURVIVED. Z02 — delete the orphan-mark reset in
#   `adopt` — survived, and the survivor was RIGHT twice over. The sweep's
#   live-owner branch cleared the same mark, so the edit changed no observable
#   behaviour: an equivalent mutant. And the rail it was aimed at asked the
#   wrong question — it asserted a re-adopted directory survives while its
#   SECOND owner is alive, which the sweep guarantees by construction, since it
#   can only doom an entry whose owner is already nil. What a stale mark
#   actually destroys is the one-sweep DEFERRAL for the new owner. The
#   redundant reset was deleted (it was unreachable: a weak reference that has
#   gone nil never becomes non-nil again) and the rail re-cut to assert the
#   deferral, with deliberately no sweep between the re-adoption and the second
#   death. Batch 121 re-run: 3/3 KILLED.
#
#   Composition, stated because it was not one invocation. First pass, batches
#   91-100 (Y01-Y14): 13 KILLED, 1 SURVIVED. Y12 — the clip-radius bound —
#   survived, and the survivor was RIGHT: `aDistantAnchorDoesNotClip` used a
#   SINGLE mid-window anchor, which is a candidate for BOTH edges, so an
#   unbounded radius collapsed the extent to a point, the min-duration guard
#   refused the clip, and the mark came back unchanged. The assertion was green
#   for a reason unrelated to the radius it names. The FIXTURE was re-cut (one
#   anchor per half); the implementation was not touched. Second pass, batches
#   98-102, re-ran Y11/Y12 plus the re-pointed Y13/Y14 and the new Y15-Y18:
#   all KILLED.
#
#   Y13 and Y14 were RE-POINTED between the passes, not merely re-run: round 2
#   added the `canProposeNewRegions` mode gate to both compose sites, so both
#   `if` lines moved. Their claims are unchanged.
#
#   TWO ISSUE LISTS WERE READ RATHER THAN TRUSTED, per this file's standing
#   warning that a KILL proves only that SOME expectation failed. Y04 (drop a
#   coarse verdict no pass-B narrowed) and Y06 (require an anchor before
#   emitting) each redden ~20 tests, which is why each has its own batch — a
#   batched partner would have been credited off their blast radius. Their
#   lists differ in exactly the right place: Y06 reddens `a passB refinement
#   replaces the coarse window it lies inside` and Y04 does not, because Y04
#   leaves the refinement path alone.
#
#   KNOWN GAP, deliberately not encoded: there is no rail on
#   `AnalysisStore+CrossUserSharing.isLocalOnlyBoundaryState`. The registration
#   IS pinned by `sweepMarksAreAKnownExportDisposition`, but that suite is not
#   in FOCUSED_SUITES and adding the sharing suites to every batch is not worth
#   the wall clock for one string in a switch.
#
#   PARTIAL RE-RUN 2026-08-01 (playhead-b6r2). Batches 85-90 (B01-B07, 7 new
#   entries) plus batch 81 — W01/W02, whose expectation this bead re-pointed.
#   FINAL 9 KILLED / 0 SURVIVED / 0 ERROR, 8 builds, ~21m wall clock. Batches
#   1-80 and 82-84 were NOT re-run and carry the verdicts above. Recount: the
#   array now holds 152 live entries.
#
#   PARTIAL RE-RUN 2026-08-04 (playhead-fil5). Batches 400-407 only, added by
#   this bead: SC01-SC22 (SC13 was drafted and dropped, see SC01), 21 entries,
#   later 31 across 9 batches — see the R1/R2/R3 review-round notes below,
#   7 batches. FINAL 21 KILLED / 0 SURVIVED / 0 ERROR, 8 builds. Batches 1-323
#   were NOT re-run and carry the verdicts above. Recount: the array now holds
#   391 live entries. (Superseded by the R1 round below, which adds three.)
#
#   R1 REVIEW ROUND, same day. Batch 404 re-run with three entries ADDED —
#   SC23, SC24, SC25 — bringing the SC series to 24 entries in the same 7
#   batches. Batch 404 FINAL: 7 KILLED / 0 SURVIVED / 0 ERROR, 2 builds, 5m06s,
#   each of the seven with a distinct victim. Batches 400-403 and 405-407 were
#   NOT re-run and carry the verdicts above. Recount: 394 live entries.
#
#   All three additions were REVIEW FINDINGS, not padding, and two of them are
#   the same shape: a correct line no test could observe.
#     • SC23 — the mode-off guard INSIDE `runShadowFMPhase`. `runBackfill`
#       answers `.off` itself now, so the guard's only remaining caller in a
#       shipped build is the shadow-RETRY drain, which does not consult the mode
#       before dispatching. Nothing drove that path with a demoted cohort, so
#       deleting the recording call was invisible. A wire-in test through
#       `retryShadowFMPhaseForSession` closed it.
#     • SC24 — the mode-off gate in `runBackfill` had a test but no mutant while
#       its three siblings (SC10/SC11/SC12) had both. Registration only.
#     • SC25 — the sweep's transcript gate reads `fastTranscriptCoveredSec` (the
#       interval UNION) and its doc claimed to apply the floor `finalizeBackfill`
#       applies, which divides the `max(endTime)` WATERMARK. Same constant,
#       different numerator, identical on every contiguous fixture in the series.
#       A gappy-transcript test now pins the divergence across the floor.
#
#   R2 REVIEW ROUND, same day. A NEW batch 408 with three entries — SC26, SC27,
#   SC28 — bringing the SC series to 27 entries in 8 batches. Batch 408 FINAL:
#   3 KILLED / 0 SURVIVED / 0 ERROR, 2 builds, 5m16s, baseline green, each of
#   the three with a distinct victim. Batch 402 was RE-RUN because the R2 round
#   rewrote a test it names — FINAL 2 KILLED / 0 SURVIVED / 0 ERROR, 2 builds,
#   4m22s, with `the claim's id is the runner's fullCoverage job id` still
#   among SC05's observed victims after being rebuilt on top of
#   `CoveragePlanner.plan(for:)`. Batches 400-401 and 403-407 were NOT re-run
#   and carry the verdicts above; every one of their anchors was re-checked by
#   `--dry-run --batch` after the R2 edits and still applies exactly once.
#   Recount: 397 live entries.
#
#   ONE ERROR ON THE WAY, twice, and neither was a mutation problem. The first
#   batch-402 attempt failed to BUILD: a backslash line-continuation inside a
#   single-line Swift string literal in the rewritten test. The battery reports
#   that as ERROR rather than KILLED, which is the point — the edit gets fixed,
#   not the expectation. The second attempt came back rc=28: the DISK PREFLIGHT
#   refusing, the failure mode already documented above. `simctl erase` on the
#   destination plus clearing `$TMPDIR/Deleting-*` (`chmod -R u+rwx` first)
#   took the volume from 14.43 to 18.13 GiB and the batch then ran clean.
#
#   All three are the R1 round's second family — a correct line nothing could
#   observe — and this time all three are in ONE function, the reconciler sweep:
#     • SC26 / SC27 — the sweep's two per-pass bounds
#       (`maxSemanticScanClaimsPerReconcile` = 8,
#       `maxSemanticScanClaimCandidatesPerReconcile` = 24) had no test and no
#       mutant. Both constants carry load-bearing docs ("deliberately the same 8
#       as maxAdScanRedrivesPerReconcile", "24 covers several times the largest
#       library seen") and both could be deleted or widened with every suite
#       green. They need SEPARATE mutants because the mint cap stops the loop
#       before the window is ever reached — the window only bites behind a wall
#       of REJECTED candidates, which is what its test builds.
#     • SC28 — a genuine defect rather than a coverage hole, and the round's
#       one instance of the "value that names one thing" family. The candidate
#       query names "assets with no coverage-lane row"; the sweep read it as
#       "assets nobody will call `runBackfill` for again". `runPendingBackfill`
#       is step 6 of a pass, so every episode being analysed right now is a
#       zero-row asset from the moment its transcript crosses the finalize floor
#       — and `reconcile()` runs on a BGProcessingTask wake, which playhead-qk44
#       documents as happening inside the live app process while the foreground
#       runner drains. The sweep now applies the same `fetchActiveJobEpisodeIds`
#       guard `mintAdScanRedrives` applies one step later.
#
#   R3 REVIEW ROUND, 2026-08-04. Four new entries — SC30 and SC32 in a new
#   batch 409, SC31 alone in 410, SC33 alone in 411 — bringing the SC series to
#   31 entries in 11 batches. SC31 and SC33 mutate the SAME LINE so they cannot
#   share a batch, and SC31 cannot share with SC30 because both redden
#   `$T_FIL5_BREATH` (a mutation credited KILLED off a batchmate's victim is a
#   fabricated rail — the SC29 reasoning). SC21, SC25 and SC27 were RE-ANCHORED
#   (the sweep's transcript gate and its candidate read were both rewritten);
#   every other SC anchor was re-checked with `--dry-run --batch` and still
#   applies exactly once.
#
#   BATCH 409/410/411 FINAL: 4 KILLED / 0 SURVIVED / 0 ERROR, one build each,
#   2m06s + 2m09s + 2m04s. The three RE-ANCHORED entries were re-run alone to
#   prove the rewrite did not cost their rails — SC25 KILLED (2m33s), SC21
#   KILLED (1m56s), SC27 KILLED (1m58s). Batches 400-403 and 405-407 were NOT
#   re-run and carry their earlier verdicts; SC21's batch (407) is represented
#   by the isolated re-run above. Recount: 401 live entries.
#
#   THREE OPERATIONAL NOTES, all costing builds, none a mutation problem:
#     • The first four attempts died on a WEDGED SIM (`Mach error -308`, then
#       "Test crashed with signal kill before establishing connection"). The
#       battery does not carry `fast-gate.sh`'s -308 recovery, so do it by hand:
#       `simctl shutdown/erase/boot` with DEVELOPER_DIR set. The
#       `xcrun: error: unable to find utility "simctl"` line that accompanies it
#       is diagnostics collection failing through the GLOBAL xcode-select
#       (playhead-ekpn) and is NOT the cause — do not chase it.
#     • Then rc=28, the disk preflight, at 6.41 GiB free. What reclaimed it was
#       `simctl erase` on the destination (3.7 GiB of device data), NOT the repo
#       cleaner, which found 3.3 MB. `$TMPDIR/Deleting-*` was empty both before
#       and after, so the erase reaped cleanly this time — check it anyway.
#     • `PLAYHEAD_MB_SKIP_BASELINE=1` was used for all six runs on the strength
#       of a scoped gate over the same 14 suites (151 tests, green) taken
#       minutes earlier. That is the documented trade and it roughly halved both
#       the builds and the disk drawdown; each run held steady at 17-20 GiB free
#       where the un-skipped ones had been falling ~2 GiB per attempt.
#
#   ONE THING TO KNOW IF YOU EDIT SOURCE WHILE A BATTERY RUNS: it restores with
#   `git checkout -- .`, so an uncommitted edit made mid-run is DISCARDED. Commit
#   first. (Also: a working-tree read during a run shows MUTATED source. Read
#   `git show HEAD:<path>` if you need the truth while one is in flight.)
#
#   Batch 409 is NOT a coverage-hole batch. SC30/SC31 restore a defect that was
#   live on this branch: the sweep's transcript gate divided the RAW interval
#   union of `pass='fast'` chunks by the episode duration and compared it to
#   0.95. A `transcript_chunks` row spans FIRST WORD to LAST WORD, so that
#   quantity is speech density, not reach — measured on the 2026-08-03 device
#   pull it clears 0.95 for ZERO of 12 assets (max 93.8 %), with FCDDB309 at
#   87.3 % and 4FF3A238 at 89.2 % despite both being transcribed to ~100 %. The
#   P0's reconciler half therefore minted nothing at all in the field while
#   every test in the suite passed, because every fixture gave its asset ONE
#   chunk spanning the whole episode — the only transcript whose raw union
#   equals its reach. playhead-pz32 had already found and removed the identical
#   shape one layer down (`AnalysisCoverageMath.bridgingShortGaps`); the fix is
#   to reuse it, which also makes the transcript floor commensurable with the
#   ad-scan floor, since `adScanCoveredSec` is intersected with that same
#   bridged region. SC33 is the direction control — bridging wide enough to
#   close a real untranscribed BLOCK is the inverse failure.
#
#   SC32 is the round's second family (a bound that reads as a rate limit but
#   is a wall). The candidate query is oldest-first and a rejected candidate
#   writes nothing, so it stays a candidate in the same position forever: a full
#   window of never-claimable assets starves everything behind it permanently,
#   and "never" is a real state — two of the four zero-row assets on the pull
#   are half-transcribed with terminal analysis jobs. A rotating read cursor
#   makes the per-pass bound a rate limit again.
#
#   R4 REVIEW ROUND, 2026-08-04. ONE new entry — SC34 alone in a new batch 412,
#   bringing the SC series to 32 entries in 12 batches. Recount: 402 live
#   entries.
#
#   BATCH 412 FINAL: 1 KILLED / 0 SURVIVED / 0 ERROR, one build, 2m12s. Its
#   ONLY observed victim was `$T_FIL5_WRAP`, which is the point: nothing that
#   already existed could see the mutation, so the hole it fills was real rather
#   than notional. Batches 400-411 were NOT re-run — the R4 round changed only
#   doc comments in the three mutated files plus one added test — and every
#   anchor was re-checked with `--dry-run --only`/`--batch` and still applies
#   exactly once.
#
#   SC34 is the OTHER HALF of the cursor SC32 introduced. SC32 pins that
#   `semanticScanClaimSweepOffset` ADVANCES past a full window; nothing pinned
#   that it WRAPS when the read comes back empty at a non-zero offset, and the
#   two states are disjoint. `$T_FIL5_ROTATE`'s fixture always leaves a
#   claimable asset waiting at offset 24, so its read is never empty. The empty
#   read arrives when the population SHRINKS under the cursor — which the mint
#   cap manufactures unaided, since a window of 24 claimable assets is claimed
#   8 at a time and leaves the cursor at 24 with 16 rows behind it. The cost of
#   losing the wrap is one whole `reconcile()` spent idle with work
#   outstanding, and on a BGProcessingTask wake that is a launch: the per-pass
#   sequence goes [8, 8, 8] to [8, 0, 8]. Every bound test stays green through
#   it, because the claims are all eventually minted.
#
#   DISK, SAMPLED AGAIN because the box was tighter than R3's runs. Batch 412
#   with `PLAYHEAD_MB_SKIP_BASELINE=1`: 12.75 GiB free at start, 8.52 GiB
#   minimum, 11.61 GiB at exit — a drawdown of 4.23 GiB over 30 samples at 5 s.
#   That is close to R2's 5.43 GiB for a selective batch and nowhere near the
#   12.15 GiB a full plan draws, so `PLAYHEAD_DISK_MIN_GIB=11` was the lever
#   (never PLAYHEAD_SKIP_DISK_PREFLIGHT — the preflight stays ARMED, just at a
#   floor measured for this run's size). Worth knowing for the next round: the
#   largest reservoir on this box is now `~/Library/Developer/CoreDevice` at
#   38 GB, which is device-build state and OUTSIDE every rail in CLAUDE.md's
#   safety list — do not reach for it, `simctl erase` on the destination is
#   still the sanctioned 2-3 GiB.
#
#   ONE THING DELIBERATELY NOT REGISTERED, on the record like SC29: the
#   `max(0, offset)` clamp in `fetchAssetIdsMissingCoverageLaneJobs`. SQLite
#   specifies a negative OFFSET as equivalent to zero, so removing the clamp is
#   behaviourally identical and any mutant would SURVIVE by construction — a
#   fabricated coverage hole, not a rail. The clamp stays as a boundary
#   guarantee that does not depend on a dependency's coercion rule, and the test
#   that pins the behaviour stays with it; neither is offered as mutation
#   coverage.
#
#   R5 REVIEW ROUND, 2026-08-04. NO new entries — the series stays at 32 in 12
#   batches, 402 live. The round's two findings were both stale MEASUREMENTS
#   rather than behaviour (the zero-row population still stated as 3 in
#   `ReconciliationReport`, and a test argument list labelled "the three field
#   assets' measured shapes" whose 0.026 and 0.47 match nothing in the pull), so
#   there is nothing new to mutate: a doc comment has no mutant, and the
#   corrected arguments assert `isOwed` below the floor, which SC01 and SC02
#   already rail.
#
#   ANCHOR RE-CHECK: `--dry-run --batch` over all twelve batches (400-412) —
#   every anchor applied exactly once and the tree was restored, each time.
#   BATCH 401 RE-RUN for real because the round's test edit lives in the file
#   its victims do: 5 KILLED / 0 SURVIVED / 0 ERROR, two builds, 4m20s.
#
#   ONE THING THE RE-RUN REVEALED, worth having on the record: the corrected
#   argument list STRENGTHENED SC02. The old list's largest value was 0.9, which
#   is below BOTH the real floor (0.98) and SC02's mutated one (0.95), so "the
#   device's unscanned assets are all owed a scan" could not see that mutation
#   at all — it is not any entry's named victim, but the batch runs the focused
#   suite and reports every failure it observes. Expressing the near-floor bound
#   as `semanticBackfillSufficientAdScanFraction - 0.001` rather than a literal
#   makes it move with the constant it probes, and it now appears in SC02's
#   observed failures alongside `$T_FIL5_CLAIM_FLOOR`. A
#   literal that happens to sit below both the real and the mutated threshold is
#   a vacuous bound; deriving it from the constant is what makes it a rail.
#
#   DISK, SAMPLED A THIRD TIME. Batch 401 WITH its baseline (two builds):
#   11.87 GiB free at start, 6.73 GiB minimum, 10.73 GiB at exit — 5.14 GiB of
#   drawdown over 54 samples at 5 s. That sits between R2's 5.43 (two builds)
#   and R4's 4.23 (one build, `PLAYHEAD_MB_SKIP_BASELINE=1`), which is the shape
#   you would expect: the second build is most of the cost. The first attempt at
#   `PLAYHEAD_DISK_MIN_GIB=10` refused the MUTATED run at 7.48 GiB with five
#   ERRORs — the R4 note's rc=28 pattern exactly — and the retry used
#   `PLAYHEAD_DISK_MIN_GIB=8`, justified by that same failed attempt having
#   measured the baseline phase's own drawdown at 3.52 GiB (11.00 -> 7.48).
#   The preflight stayed ARMED throughout; it is a START gate, so the 6.73 GiB
#   minimum is expected and is not a breach of the 8.00 floor.
#   `~/Library/Developer/CoreDevice`, 38 GB in R4, measured 1.4 MB this round —
#   it drained on its own, so do not budget for reclaiming it.
#
#   ONE SURVIVOR ON THE WAY, and it was real rather than a mis-stated
#   expectation: SC09 ("" persisted as if it were a podcast id) survived
#   because `runBackfill`'s podcastId gate pre-converted "" to nil at the CALL
#   SITE, so the row never saw the empty string the normalization exists to
#   catch and no wire-in test could observe the constructor however it behaved.
#   The duplicate decision at the call site was deleted and a third rail added
#   (the value reaching SQLite as NULL); SC09 then KILLED with all three.
#
#   TWO OPERATIONAL NOTES, both cost builds:
#     • The first four batches came back ERROR / "batch did not build/run,
#       rc=28". That is the DISK PREFLIGHT refusing, not a compile failure —
#       the baseline build leaves the volume under the 13.50 GiB floor and the
#       mutated run is then refused. The floor is measured for a FULL plan; a
#       selective batch was sampled at 5.43 GiB of drawdown (16.11 start,
#       10.68 minimum), so `PLAYHEAD_DISK_MIN_GIB=9` is the deliberate,
#       measured lever here — never PLAYHEAD_SKIP_DISK_PREFLIGHT. Pair it with
#       `PLAYHEAD_MB_SKIP_BASELINE=1` once the baseline has been seen green,
#       which halves the builds and the drawdown with them.
#     • `--dry-run --only <NAME>` returns "nothing selected" with exit 2 when
#       the name does not match, and a loop that greps the output for the word
#       "anchor" reads that as success for every entry. Worse, the `git
#       checkout -- .` such a loop uses to tidy up will DISCARD uncommitted
#       edits to this script itself. Commit the battery entry before dry-running
#       it, and check the exit code rather than the prose.
#
#   THE PRE-FLIGHT EARNED ITS KEEP. The first attempt refused to run: the
#   focused set was already RED on two xr3t review-round tests
#   (`managedAdWindowReplacementCannotBypassInventoryFilter` and its
#   AdDecisionResult twin), both of which demonstrate "a same-ID geometry
#   change re-runs inventory validation" by replacing a span with `[0, 60]`.
#   That is a PRE-ROLL, which the corrected rule (b) admits. Without the
#   baseline check, six mutations would have been credited KILLED off two
#   failures that had nothing to do with them.
#
#   B01's issue list was READ, not trusted, and it says something the bead did
#   not: restoring the old head rule reddens FOUR tests belonging to other
#   beads — d3g0's `Pre-roll banners when the playhead is AT 0:00`, d3g0's
#   field-batch test, djl0's no-show banner trace, and the shadow-mode markOnly
#   banner. Those tests passed on main for eleven weeks while the field
#   behaviour they describe was broken, because `SkipOrchestrator.init`
#   defaulted the filter OFF. They are red under B01 only because this bead
#   bound that default to production. That is the bead's option 3 measured
#   rather than argued: the divergence, not the rule, is what made the loss
#   invisible — flipping the default alone would have gone red in April.
#
#   L06 was RE-CUT rather than re-verified. Its edit (admit `.candidate`
#   wholesale) used to be "the playhead-evc1 carve-out applied early"; since
#   evc1 landed it is the WRONG carve — it admits the segment aggregator's
#   coarse 30 s tiles, which is the population the original exclusion was for.
#   Its expectation moved from the eks2 characterization test to evc1's
#   exclusion sweep plus the negative-control arm of that same eks2 test.
#
#   THE TWO SAFETY RAILS WERE RE-APPLIED BY HAND AND THEIR ISSUE LISTS READ,
#   for the reason the eks2 note below gives — a KILL only proves SOME
#   expectation failed, and both of these mutations also trip a metadata
#   assertion in the evc1 suite. Under L04 (gate markOnly -> eligible), `2350: a
#   day-0 seeded continuation span never auto-skips` also failed
#   `trespassing.isEmpty`: a real skip cue was pushed over the day-0-seeded
#   span. Under L05 (anchors unanchored -> rediffByteExact), `ynmk: confirming a
#   day-0 seeded banner marks` also failed `pushedCues.isEmpty`,
#   `promoted.wasSkipped == false` and `promoted.decisionState == .confirmed` —
#   a tap really cut audio and the row really recorded `.applied`. The claim
#   that a byte-exact seed's certainty does not propagate along the chain is
#   load-bearing, not a restatement of the emitted literal.
#
#   V01's expectation list is deliberately the WHOLE admission half of the evc1
#   suite (11 names). That makes it the vacuity audit for the bead: with the
#   carve-out reverted, every test that claims to measure the admission must go
#   red, and the three exclusion tests must stay green. It came back KILLED with
#   all 11 red, including both vacuity CONTROLS — the "the control must seed"
#   arm of the exclusion sweep and the "a candidate day-0 row was refused" arm
#   of the veto test.
#
#   One thing that run learned, and it is the reason two of these were then
#   re-run BY HAND rather than trusted: a KILL only proves SOME expectation in
#   the named test failed, and a suite that also asserts the mutated field
#   directly can report a kill without ever exercising the behaviour the rail
#   is about. L04 (gate markOnly -> eligible) and L05 (anchors unanchored ->
#   rediffByteExact) both trip a metadata assertion in the eks2 suite, so each
#   was re-applied alone and its ISSUE LIST read: L04 also failed
#   `trespassing.isEmpty` (a real skip cue was pushed over a continuation span)
#   and L05 also failed `pushedCues.isEmpty` / `promoted.wasSkipped == false`
#   (a tap really did cut audio). The two safety claims are load-bearing, not
#   restatements of the literal.
#
#   Two things that run learned:
#     • K10 SURVIVED for real, and the survivor was a genuine coverage hole.
#       `an UNKNOWN publish date sorts LAST` asserted the ORDER OF A
#       TWO-ELEMENT SORT. Inverting `case (nil, .some)` makes the comparator
#       claim BOTH "a precedes b" and "b precedes a" — an invalid predicate,
#       whose `sorted(by:)` behaviour is undefined — and on two elements the
#       sort happened to reach the expected order through the other arm. The
#       test now asserts `isOrderedBefore` in BOTH directions. A list order is
#       not evidence about a comparator.
#     • K15 reported ERROR, not SURVIVED: one of its three named tests lives in
#       `RediffDayZeroTriggerIdempotencyTests`, which was missing from
#       FOCUSED_SUITES. That is the script's own documented failure mode and
#       the fix was the list, not the expectation.
#
#   Three things the 07-28/djl0 runs learned, all of them authoring faults
#   rather than coverage holes except the first:
#     • J02 SURVIVED for real. `a show with no profile yet is a new-show
#       default, not a failure` named the right resolution and then asserted a
#       DIFFERENT cause's counter, so reclassifying `newShowDefault` as a
#       failure left it green — every first listen would have written a coded
#       diagnostics incident. The test now asserts that cause's own counter and
#       the absence of both diagnostics codes.
#     • J08 and J14 each SURVIVED on ONE named test that the mutation provably
#       cannot reach (an orchestrator edit against a view-model-local
#       transition; a "route everything down the resolved branch" edit against
#       the resolved branch's own labels). Both expectations were narrowed and
#       the un-covered production values given their own entries, J17 and J18.
#       Neither was fixed by relaxing an assertion.
#     • J03's first EDIT was dry-run-clean and did not COMPILE (it left a
#       non-exhaustive switch) — the file's own warning about `--dry-run`,
#       collected again.
#
# THE ONE STANDING SURVIVOR — M17. Read this before "fixing" it.
#   Production attribution is correct by construction: both seams pass the
#   `sourceShowId` captured at gesture time into `makeManualCorrectionVetoEvent`
#   and never read `activePodcastId`. What changed is the OBSERVATION POINT.
#   Before playhead-o4qr the receipt was written by `persistManualCorrectionVeto`
#   AFTER the revert barrier, so parking there and swapping episodes made a
#   live-attributed receipt visible. o4qr mints the event and commits it inside
#   the atomic transaction BEFORE that barrier (SkipOrchestrator.swift: mint,
#   then `persistRevertedAdWindowsIfCurrent`, then the barrier), so by the time
#   the race test can replace the episode the receipt is already durable and
#   `activePodcastId` still equals `sourceShowId`. The mutation is inert.
#
#   Pinning it again needs a barrier at the suspension that CAN corrupt the
#   attribution — the `await revokeRecurrenceEvidence` that precedes the mint —
#   not the one that precedes the live-state guard. One barrier cannot pin both
#   under this structure, because the durable write now sits above the effects:
#   the current placement is what makes M07/M12/M14/M15/M16 killable. Adding a
#   second, pre-mint barrier is the fix. It is a new test seam, and it stays a
#   COVERAGE decision for a human rather than something a passing run absorbs.
#
# THE CONTRACT THIS BATTERY PINS (playhead-o4qr, Dan's decision)
#   ACCEPT THE RECEIPT, REFUSE THE LEARNING. A correction whose show identity is
#   unusable (nil, empty, non-canonical, or disagreeing with the live episode)
#   still commits its durable receipt and still returns true; what it withholds
#   is every show-KEYED effect — trust penalty, hard-negative bank, per-show
#   threshold controller, show-scoped recurrence revocation. O01-O05 pin the
#   decision itself; N07 covers the controller surface the others cannot see.
#   `T_ANON_SILENT` is the display name of its subject test — if that test is
#   retitled again, the constant must follow or N06, N07 and O01-O04 all go
#   silently un-creditable.
#
# FIVE THINGS THIS FILE LEARNED THE HARD WAY
#   • A survivor is a MISSING TEST. Every survivor found so far was fixed at the
#     source of the problem, never by touching an expectation.
#   • `--dry-run` proves an anchor still APPLIES. It does not prove the mutant
#     still COMPILES, and it does not prove the mutant still KILLS. N04 sat
#     dry-run-clean and build-broken for a whole bead (playhead-1mq1.2.1 →
#     playhead-auz3); M14 and O04 sat dry-run-clean and unverified beside it.
#   • A batch aborts on its first failed anchor, so one rotten entry costs the
#     whole batch. That is why the five unrepresentable o4qr mutations live in
#     the KNOWN GAP block instead of ERRORing in `MUTATIONS`.
#   • "No learning happened" assertions need a BARRIER, not a drain. P01
#     survived because "the hard-negative bank stayed empty" was read straight
#     after `drainOrchestratorEffects`, which orders only work enqueued on the
#     ORCHESTRATOR — the read beat the ingest's hop to the bank actor and passed
#     for the wrong reason. Its fixture now issues a CLEAN sentinel revert
#     second and asserts the resulting count. O02 survived for the sibling
#     reason and was rebuilt on the trust store (`awaitTrustFalseSkipSignals`).
#   • An assertion must be able to OBSERVE the outcome it forbids. Both
#     playhead-ugy4 and playhead-auz3 found tests whose only probe was
#     `confirmedWindows()`, which filters to `.confirmed`, while the duplicate
#     promotion they forbade lands as `.applied`. `activeWindowIDs()` is the
#     dictionary the promotion actually writes into. Fixtures that cannot reach
#     the seam (no durable row, or an asset row whose episodeId disagrees with
#     `beginEpisode`) fail the same way, one layer earlier.

set -uo pipefail

cd "$(dirname "$0")/.." || {
  echo "mutation-battery: cannot reach the repo root — refusing to run, because" >&2
  echo "restore_sources would then 'git checkout --' in whatever repo the caller" >&2
  echo "happens to be sitting in." >&2
  exit 2
}
REPO_ROOT="$(pwd)"

: "${DEVELOPER_DIR:=/Applications/Xcode-beta.app/Contents/Developer}"
export DEVELOPER_DIR

ORCH="Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift"
STORE="Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
CTRL="Playhead/Services/AdDetection/PerShowThresholdControllerStore.swift"
# playhead-d3g0: the suggest card's copy seam. The mark-only wording is the
# only thing standing between "confirming this MARKS" and a button that
# promises a skip it cannot perform, so it is mutable here.
VIEW="Playhead/Views/Components/AdBannerView.swift"
# playhead-96ot: the two rediff-side halves of the in-session delivery. The
# orchestrator owns the door; these own the DECISION to knock on it, and that
# decision is what was missing for the whole life of the day-0 path.
TRIG="Playhead/Services/AdDetection/RediffRefetch/DayZeroRediffTrigger.swift"
RSVC="Playhead/Services/AdDetection/RediffRefetch/RediffRefetchService.swift"
# playhead-djl0: the three layers that carry the CAUSE behind the active skip
# mode. `.shadow` used to name both "deliberately observing this show" and "I
# could not look the show up", and every one of these files is a place the two
# can be collapsed back together — the trust lookup that decides the cause, and
# the pill/view-model hop that shows it.
TRUST="Playhead/Services/TrustScoring/TrustScoringService.swift"
NPV="Playhead/Views/NowPlaying/NowPlayingView.swift"
NPVM="Playhead/Views/NowPlaying/NowPlayingViewModel.swift"
# playhead-4dqe: day-0 at DOWNLOAD time. Four more places the download-time
# path can silently stop working — the transport/budget policy that decides
# whether it may spend, the readiness wait + ordering that decide whether it
# ever reaches the trigger, the coordinator that counts and surfaces a give-up,
# and the fetch seams where the user's transport setting has to reach the
# SOCKET rather than only the gate.
BWPOL="Playhead/Services/AdDetection/RediffRefetch/RediffDayZeroBandwidthPolicy.swift"
KICK="Playhead/Services/AdDetection/RediffRefetch/RediffDayZeroKickoff.swift"
KCOORD="Playhead/Services/AdDetection/RediffRefetch/RediffDayZeroKickoffCoordinator.swift"
SEAMS="Playhead/Services/AdDetection/RediffRefetch/RediffRefetchSeams.swift"
ACT="Playhead/Services/AdDetection/RediffRefetch/RediffActivation.swift"
# playhead-eks2: the ad-pod continuation flip (L01-L09). ADSVC carries BOTH the
# shipped flag (`AdDetectionConfig.default`) and the Step 18b wire-in; PODC
# carries the mark literals and the seed predicate.
ADSVC="Playhead/Services/AdDetection/AdDetectionService.swift"
PODC="Playhead/Services/AdDetection/AdPodContinuation.swift"
# playhead-mptr: the artifact-backed shard skip (K2 series). MPTRIDX owns the
# merge/overlap/skip policy; the two SQL rails live in STORE.
MPTRIDX="Playhead/Services/TranscriptEngine/FastTranscriptCoverageIndex.swift"
# playhead-kvs8: the FM daemon throttle (Q01-Q08). THROT is the single
# definition + the named causes; RUNNER carries the defer branch that replaced
# the terminal `failed`; FMCLS the permissive status/counter mapping; PROBE the
# readiness cache guard.
THROT="Playhead/Services/AdDetection/FMDaemonThrottle.swift"
# playhead-e75l: the daemon-refusal CLASS — kvs8's throttle plus the metadata
# stall — and the per-kind tokens and log events that keep the two countable
# apart in a device pull. Added for the DR series.
FMREF="Playhead/Services/AdDetection/FMDaemonRefusal.swift"
RUNNER="Playhead/Services/AdDetection/BackfillJobRunner.swift"
FMCLS="Playhead/Services/AdDetection/FoundationModelClassifier.swift"
PROBE="Playhead/Services/Capabilities/FoundationModelsUsabilityProbe.swift"
# playhead-usn1: the per-show skip-mode CONTROL. Two more files join the
# djl0 trio because the field defect was not in the cause taxonomy at all — it
# was a surface that sampled the mode ONCE, before `beginEpisode` had resolved
# the show, and never looked again. RT owns the write (which used to be an
# `if let` with no `else`) and the identity the write targets; MODEL owns the
# recovery of that identity from the episode row itself.
RT="Playhead/App/PlayheadRuntime.swift"
MODEL="Playhead/Models/Podcast.swift"
# playhead-isp5: the ingest-outcome TAXONOMY. Its own file because two of the
# W rails are about the audit row's own arithmetic — a drop counted as a
# delivery, a reason that never renders — and those live in the value type, not
# in the orchestrator that stamps it.
INGO="Playhead/Services/SkipOrchestrator/AdWindowIngestOutcome.swift"
# playhead-b6r2: the inventory sanity filter. A pure value type, which is why
# the B rails split cleanly — the edge READING lives here and the DEFAULT that
# decides whether tests ever see it lives in the orchestrator's init.
INVF="Playhead/Services/AdDetection/InventorySanityFilter.swift"
# playhead-y3ya: the semantic-sweep mark composer. A PURE type — the whole
# extent policy is in it — so every Y rail but the wire-in ones is a one-line
# edit to a stage whose claim has its own named test.
SWEEP="Playhead/Services/AdDetection/SemanticSweepMarkComposer.swift"
# playhead-lxkq: the ad-likelihood SCAN ORDER (X01-X14). SCANORD is the pure
# permutation policy — every ranking, budget and degenerate-input claim is a
# one-line edit there. The two WIRES live elsewhere and are the defect a pure
# battery structurally cannot see: FMCLS holds the `planPassA` call and the
# `restoreOrder` that keeps the REPORTED plan list in episode order, RUNNER the
# seed derivation and its flag, ADSVC the shipped default.
SCANORD="Playhead/Services/AdDetection/AdLikelihoodScanOrder.swift"
# playhead-cgka: the per-test scratch lifetime. The ONLY entries in this battery
# whose mutable files are TEST sources, and deliberately so — the thing under
# test IS the test harness. A reaper that reclaims nothing leaves the gate as
# broken as it was and nothing goes red to say so; a reaper that reclaims too
# eagerly deletes a live suite's database and surfaces as an unrelated flake
# somewhere else entirely. Neither direction announces itself, which is exactly
# the shape this battery exists for.
SCRATCH="PlayheadTests/Helpers/TestScratch.swift"
SCRATCHH="PlayheadTests/Helpers/TestHelpers.swift"
# playhead-avbn: WHO MAY VOTE THAT THERE IS NO AD. FMSUP is the pure admission
# rule — every A rail but the two wires is a one-line edit there. GATE is the
# eligibility gate whose NAME and SEVERITY had to be brought into line with the
# behaviour, and it is a separate file for the same reason INGO is: the rails
# are about a value type's own arithmetic (a severity that must sort with the
# blocked cases, an alias that must decode one way only), not about the service
# that stamps it. The two WIRES are the defect a pure battery structurally
# cannot see: ADSVC holds the only caller of the admission rule, ORCH the
# routing decision that makes the gate a block rather than a banner.
FMSUP="Playhead/Services/AdDetection/FMSuppressionGuard.swift"
GATE="Playhead/Services/AdDetection/EvidenceLedgerEntry.swift"
# playhead-sik9: THE POST-ROLL GUARD'S BYTE-ANCHORED EXEMPTION (C01-C10).
# FUSION is where the guard and its exemption live — a one-line predicate, which
# is exactly why it needs a battery: every plausible mis-scoping (delete it,
# invert it, widen it to any width oracle, widen it to any provenance, implement
# it as a promotion) compiles and passes a positive-only test set. DSPAN holds
# the shared `carriesRediffByteExactWidth` DEFINITION that four other shipped
# carve-outs also key on, so a mutation there is a blast-radius test as much as
# a rail. EXTENT and RSLOT are the two REACHABILITY claims the exemption's
# narrowness rests on and that no test of the guard itself can see: that a
# geometry-rewritten span loses its edge claim (so playhead-2350 still catches
# it), and that the lagged byte path's default keeps every playhead-9s6q
# segment-recovered region out of `.rediffSlot` provenance in the first place.
FUSION="Playhead/Services/AdDetection/BackfillEvidenceFusion.swift"
DSPAN="Playhead/Services/AdDetection/DecodedSpan.swift"
EXTENT="Playhead/Services/AdDetection/SpanExtentSupport.swift"
RSLOT="Playhead/Services/AdDetection/RediffSlotOwnership.swift"
# playhead-6qvf: `AnchorRef` itself — the enum whose byte/chroma split the G
# series defends. Named ADSVC_ATOM rather than ATOM because the key is read out
# of a `|`-delimited record and a two-letter key invites collisions.
ATOMEV="Playhead/Services/AdDetection/AtomEvidence.swift"
# playhead-gard: PER-DETECTOR TRUST (I series). Four files because the claim is
# a chain and each link fails silently on its own. DETCLS is the pure
# classifier — "which detector drew this span, and does the show's history
# govern it" — where every mis-scoping of the exemption is a one-line edit that
# compiles. DETLED is the persisted state and, critically, the MIGRATION: the
# seed function is the only thing standing between an upgrading user and a
# silent posture change, and no behavioural test of a fresh store can see it.
# TRUST holds the weighting and the promotion/demotion arithmetic. ORCH is the
# WIRE — the gate that reads a mode, the four veto sites that name a detector,
# and the banner-confirm site that is the ONLY escape from `manual`; a battery
# over the pure types structurally cannot see any of them.
DETCLS="Playhead/Services/TrustScoring/SkipDetectorClass.swift"
DETLED="Playhead/Services/TrustScoring/DetectorTrustLedger.swift"
# playhead-hx6n: SCAN-ROW RUN ATTRIBUTION (T series). Three files, because the
# claim is a chain from the write to the read to the arithmetic and each link
# fails silently and separately. SPLIT is the CONSUMER — `bucket(for:)` is one
# line, and defaulting a nil phase to `.foreground` there still produces
# perfectly-shaped numbers that are simply wrong. STORE holds both halves of
# persistence: the write that must NOT invent a phase, and the read that must
# NOT default a NULL to one (a `?? .active` on the read line quietly re-attributes
# the entire pre-V42 corpus). RUNNER holds the single attribution seam, which is
# the only thing that can put a real value in the columns at all — a battery
# over the pure types structurally cannot see a seam that never fires.
SPLIT="Playhead/Services/AdDetection/SemanticScanThroughputSplit.swift"
# playhead-bllt: THE HOT PATH'S PRESENCE-vs-EXTENT GATE (BL series). Three
# files, because the claim is a chain and each link fails silently on its own.
# HOTGATE is the pure rule — one predicate, and every plausible mis-scoping of
# it (make it inert, invert it, read either edge instead of both, ignore the
# kill switch, turn the demotion into a drop) compiles and passes a
# positive-only test set. ADSVC holds the two EMISSION sites, which are the only
# things that can wire the rule to a persisted row at all: a battery over the
# pure type structurally cannot see a gate that is computed and then ignored,
# nor a site that persists an anchor pair it did not gate on. ORCH is where the
# demotion becomes VISIBLE — the isp5 census sub-cause that separates "demoted
# for want of a proven edge" from "absent for some reason nobody wrote down",
# which is the whole difference between this bead and the silence it replaced.
HOTGATE="Playhead/Services/AdDetection/HotPathExtentGate.swift"
# playhead-ug9m: THE DAY-0 RESCUE (UG series). Two files, because the claim is
# split between a pure rule and the two places that spend real bytes on it.
# UGCEN is the census + the supersede predicate — every plausible loosening of
# either (drop a conjunct, read one edge, ignore a veto, ignore the producer)
# compiles and passes a positive-only test set, and a loosened `isSupersedable`
# is how a 9s6q segment-recovered slot would get promoted. POLICY is the
# terminality arm and the rescue CEILING, which is the only thing bounding a
# ~108 MB re-fetch; ADSVC is the mint site, the only place able to observe a
# gate that is computed and then not applied to the retire set.
UGCEN="Playhead/Services/AdDetection/RediffRefetch/DayZeroMarkCensus.swift"
POLICY="Playhead/Services/AdDetection/RediffRefetch/RediffDayZeroOutcome.swift"
# playhead-pggn (PG series). SEGAGG is the pure aggregator — the only place
# able to observe a score that describes a different region than the geometry
# it ships with. ADSVC is already listed and carries the PG08 extent-support
# literal, which is the one mutation in this series that reaches the row.
SEGAGG="Playhead/Services/AdDetection/SegmentAggregator.swift"
# playhead-kkzu (KZ series). The download path is the ONLY writer of
# `analysis_jobs.podcastId`, and before this bead every background/auto
# download wrote a NULL there. DLMGR owns the context type, the durable
# attribution sidecar and the completion enqueue; FQSCAN is the force-quit
# resume, the one production caller that genuinely cannot derive the show and
# must recover it from disk; BGFEED and EPPREP are the two callers that always
# could and were dropping it.
DLMGR="Playhead/Services/Downloads/DownloadManager.swift"
FQSCAN="Playhead/Services/Downloads/ForceQuitResumeScan.swift"
BGFEED="Playhead/Services/PodcastFeed/BackgroundFeedRefreshService.swift"
EPPREP="Playhead/Services/Downloads/EpisodePreparationCoordinator.swift"
# playhead-kanf (KN series). SCHED owns the one-shot user-intent flag and the
# decision to promote; STORE (already listed) owns the guarded write. The two
# halves are genuinely separable — a store that promotes a leased row and a
# scheduler that burns the flag on nothing are different defects — so both are
# mutated.
SCHED="Playhead/Services/PreAnalysis/AnalysisWorkScheduler.swift"
# playhead-fil5 (SC series). The durable scan CLAIM. Four files, because the
# claim spans four separable defects and a mutation in one is invisible to the
# others: CLAIM owns the predicates, the identity and the row shape; ADSVC
# (already listed) owns whether the four gates record at all; RECON owns the
# sweep that reaches assets nobody will ever call `runBackfill` for again; STORE
# (already listed) owns the selector that finds them. ATOM is the shared
# transcript-version hash the claim's identity is derived from — a claim that
# hashes differently from the runner names a job that does not exist.
CLAIM="Playhead/Services/AdDetection/SemanticScanClaim.swift"
RECON="Playhead/Services/PreAnalysis/AnalysisJobReconciler.swift"
ATOM="Playhead/Services/AdDetection/TranscriptAtom.swift"
# playhead-9y9e: the PIPELINE runner (stages 1-5), not `$RUNNER`, which is the
# coverage-lane `BackfillJobRunner`. Two different files whose names differ by
# a prefix; the RT series mutates this one.
AJRUN="Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift"
# playhead-iu0t R1: the analysis COORDINATOR — the actor that drives the
# session lifecycle and pushes the evidence catalog onto the SkipOrchestrator.
# Added for CN06, the fourth instance of the pre-hc7e final-only collapse.
ACOORD="Playhead/Services/AnalysisCoordinator/AnalysisCoordinator.swift"
# playhead-iu0t R2: the episode-summary backfill coordinator — the FIFTH
# instance of the collapse, and the one that reached it through the persisted
# `transcript_chunks.transcriptVersion` column rather than through a call.
# Added for CN07.
ESUMBF="Playhead/Services/EpisodeSummaries/EpisodeSummaryBackfillCoordinator.swift"
MUTABLE_FILES=(
  "$ORCH" "$STORE" "$CTRL" "$VIEW" "$TRIG" "$RSVC" "$TRUST" "$NPV" "$NPVM"
  "$BWPOL" "$KICK" "$KCOORD" "$SEAMS" "$ACT" "$ADSVC" "$PODC"
  "$THROT" "$FMREF" "$RUNNER" "$FMCLS" "$PROBE" "$RT" "$MODEL" "$INGO" "$INVF"
  "$SWEEP" "$SCANORD" "$SCRATCH" "$SCRATCHH" "$FMSUP" "$GATE"
  "$FUSION" "$DSPAN" "$EXTENT" "$RSLOT" "$ATOMEV"
  "$DETCLS" "$DETLED" "$SPLIT" "$HOTGATE" "$UGCEN" "$POLICY" "$SEGAGG"
  "$DLMGR" "$FQSCAN" "$BGFEED" "$EPPREP" "$SCHED"
  "$CLAIM" "$RECON" "$ATOM" "$AJRUN" "$ACOORD" "$ESUMBF"
)

FOCUSED_SUITES=(
  # playhead-gard: the per-detector trust rails (I series). Seven suites,
  # because the claim spans the whole chain and no one layer can observe
  # another: classification and the exemption's scope; the persisted ledger and
  # its MIGRATION seed; the veto weighting; per-detector resolution; the
  # demotion that must still happen; the escape from `manual`; and the
  # orchestrator wiring that is the only thing able to see a gate reading the
  # wrong mode.
  -only-testing:PlayheadTests/SkipDetectorClassTests
  -only-testing:PlayheadTests/DetectorVetoWeightTests
  -only-testing:PlayheadTests/DetectorTrustLedgerTests
  -only-testing:PlayheadTests/DetectorModeResolutionTests
  -only-testing:PlayheadTests/PerDetectorDemotionTests
  -only-testing:PlayheadTests/ManualModeEscapabilityTests
  -only-testing:PlayheadTests/PerDetectorSkipGateTests
  # playhead-6qvf: the byte/chroma certainty split (G series). Four suites,
  # because the claim spans four layers and no one of them can see the others:
  # the pure invariants + the exhaustive "only .rediffSlot is deterministic"
  # sweep; the AnchorRef plumbing (equality, Codable, isWidthOwnership); and the
  # two service-level e2e harnesses that are the ONLY things able to observe
  # which differ arm stamped which marker — byte-first drives the byte aligner,
  # xsdz.29 drives the chroma fallback. The two live CONSUMERS (host-read floor,
  # post-roll guard) are already in scope via the nqey/sik9 suites below.
  -only-testing:PlayheadTests/RediffChromaWidthIsNotDeterministicTests
  -only-testing:PlayheadTests/AnchorRefRediffSlotTests
  -only-testing:PlayheadTests/RediffByteFirstEndToEndTests
  -only-testing:PlayheadTests/RediffSlotOwnershipEndToEndTests
  # playhead-e75l: the daemon-refusal rails (DR series). Three suites, because
  # the claim spans three layers and no one of them can see the others: the
  # enum's per-kind tokens and events (pure, instant); the runner's DISPOSITION
  # for a metadata-deadline timeout (defer, retryCount preserved, named cause,
  # one shared consecutive counter); and a SOURCE canary, because a log event
  # name is not observable from a test on this harness and the drain-stop line
  # shipped kvs8's literal for a condition that is not a throttle.
  -only-testing:PlayheadTests/FMDaemonRefusalDefinitionTests
  -only-testing:PlayheadTests/FMDaemonMetadataStallRunnerTests
  -only-testing:PlayheadTests/FMDaemonRefusalSourceCanaryTests
  # playhead-cgka: the scratch-reaper rails (Z series). 13 tests, ~0.06s — it
  # costs nothing to carry in every batch and the alternative is a second
  # focused set for one series.
  -only-testing:PlayheadTests/TestScratchReaperTests
  # playhead-mptr: the artifact-backed ordering rails (K2 series). Both suites
  # are pure value-type / small-store tests, well under a second.
  -only-testing:PlayheadTests/FastTranscriptCoverageIndexTests
  -only-testing:PlayheadTests/FastTranscriptCoveredRangesStoreTests
  -only-testing:PlayheadTests/SkipOrchestratorThresholdControlTests
  -only-testing:PlayheadTests/SkipOrchestratorRevertTests
  -only-testing:PlayheadTests/SkipOrchestratorRevertLifecycleRaceTests
  # playhead-ugy4: the two suggest-tier rails (S04/S05) live in
  # SkipOrchestratorCharacterizationTests.swift, whose @Suite structs are
  # named for their topic rather than the file.
  -only-testing:PlayheadTests/SkipOrchestratorAdDecisionContractTests
  -only-testing:PlayheadTests/SkipOrchestratorSuggestTierTests
  # playhead-1mq1.2.1: the mixed-width attribution rails (P01/P02).
  -only-testing:PlayheadTests/RevertMixedWidthAttributionTests
  # playhead-ug9m: the day-0 rescue rails (UG series). Five suites — the pure
  # census, the policy bound, the mint over real divergent MP3 bytes, the store
  # round-trip + surfacing query, and the trigger wiring. ~0.8 s for all thirty.
  -only-testing:PlayheadTests/DayZeroMarkCensusTests
  -only-testing:PlayheadTests/DayZeroRescuePolicyTests
  -only-testing:PlayheadTests/DayZeroRescueMintTests
  -only-testing:PlayheadTests/DayZeroRescuePersistenceTests
  -only-testing:PlayheadTests/DayZeroRescueTriggerTests
  # playhead-d3g0: the playhead-entry gate (D01-D06) and the skip affordance
  # (D07-D09).
  -only-testing:PlayheadTests/SuggestBannerEntryGateTests
  -only-testing:PlayheadTests/SuggestBannerSkipAffordanceTests
  -only-testing:PlayheadTests/SuggestBannerEntryGateSecondPassTests
  # playhead-ynmk's extent gate. D07 is a direct revert of it, so leaving it out
  # would have let a mutation that re-skips 150 s of show be judged solely on
  # d3g0's own card-side assertion.
  -only-testing:PlayheadTests/BannerConfirmationExtentGateTests
  # playhead-96ot: the in-session delivery of a day-0 mint (E01-E09). The two
  # d3g0 suites above stay in scope deliberately — the new door feeds
  # `receiveAdWindows`, which is where d3g0 ARMS, so a delivery mutation that
  # broke the entry gate would otherwise be judged only by its own suite.
  -only-testing:PlayheadTests/SkipOrchestratorMidSessionIngestTests
  -only-testing:PlayheadTests/MidSessionIngestSuggestArmingTests
  -only-testing:PlayheadTests/DayZeroTriggerMarkDeliveryTests
  -only-testing:PlayheadTests/DayZeroFirstListenInSessionSkipTests
  # playhead-djl0: the named/counted/surfaced cause behind the skip mode
  # (J01-J16). The two d3g0/96ot families above stay in scope: the field
  # regression asserts a mark-only day-0 window still banners in a session
  # with no show, which is a claim about the SUGGEST tier, not this bead's
  # own machinery.
  -only-testing:PlayheadTests/SkipModeResolutionNamingTests
  -only-testing:PlayheadTests/SkipModeResolutionCountingTests
  -only-testing:PlayheadTests/SkipModeResolutionDiagnosticsTests
  -only-testing:PlayheadTests/ShowIdentityRecoveryTests
  -only-testing:PlayheadTests/UnresolvedShowFieldRegressionTests
  -only-testing:PlayheadTests/SuggestTierIsNotGatedByTrustModeTests
  -only-testing:PlayheadTests/SkipModePillPresentationTests
  -only-testing:PlayheadTests/NowPlayingViewModelSkipModeResolutionTests
  # playhead-4dqe: day-0 at DOWNLOAD time (K01-K22). The 96ot delivery suites
  # above stay in scope deliberately — the download-time path ends in the same
  # `triggerIfEligible`, so a mutation that broke the mint delivery while
  # satisfying this bead's own suites would otherwise go unnoticed.
  -only-testing:PlayheadTests/DayZeroTransportPolicyTests
  -only-testing:PlayheadTests/RediffDayZeroDailyBudgetTests
  -only-testing:PlayheadTests/RediffDayZeroKickoffOutcomeTests
  -only-testing:PlayheadTests/DayZeroReadinessWaitTests
  -only-testing:PlayheadTests/RediffDayZeroKickoffOrderingTests
  -only-testing:PlayheadTests/RediffDayZeroKickoffCoordinatorTests
  -only-testing:PlayheadTests/DayZeroTransportSettingTests
  -only-testing:PlayheadTests/DayZeroFetchTransportTests
  -only-testing:PlayheadTests/DayZeroDownloadTimeStoreTests
  -only-testing:PlayheadTests/DayZeroTriggerTransportBudgetTests
  # The playhead-p70f trigger suite: K15 re-injects the silent gate refusal,
  # and the assertion that a refused transport still does not READ the attempt
  # record lives here rather than in this bead's own file. Without this line
  # K15 reported ERROR ("expected test never ran") rather than KILLED — the
  # script's own named failure mode for a suite missing from this list.
  -only-testing:PlayheadTests/RediffDayZeroTriggerIdempotencyTests
  # playhead-bllt: the hot path's presence-vs-extent gate (BL series). Two
  # suites, and BOTH are needed. The monotonicity suite is pure and exhaustive
  # — it is the only thing that can see a rule that widens somewhere it was not
  # looked at — but it cannot see whether the rule is WIRED. The integration
  # suite drives the real `runHotPath` through both producers and reads the
  # persisted row, which is the only place a computed-then-ignored gate shows
  # up. Neither alone is sufficient and the battery proves it: BL01 kills both,
  # BL06/BL07 kill only the integration suite, BL04 kills only the pure one.
  -only-testing:PlayheadTests/HotPathExtentGateMonotonicityTests
  -only-testing:PlayheadTests/AutoSkipPrecisionGateIntegrationTests
  # playhead-eks2: the pod-continuation flip (L01-L09). The d3g0/96ot suites
  # above stay in scope deliberately — a continuation window is delivered by
  # `ingestPersistedAdWindows` and presented by the entry gate, so a mutation
  # that broke either while satisfying this bead's own suite would go unseen.
  -only-testing:PlayheadTests/AdPodContinuationFlipTests
  -only-testing:PlayheadTests/AdPodContinuationWireInTests
  -only-testing:PlayheadTests/AdPodContinuationTests
  # playhead-evc1: the day-0 seed carve-out (V01-V06). The eks2 suites above
  # stay in scope: L06's re-cut expectation spans both, because the wholesale
  # `.candidate` admission is caught by the exclusion sweep here AND by the
  # negative-control arm of the eks2 field test.
  -only-testing:PlayheadTests/AdPodContinuationDayZeroSeedTests
  # playhead-kvs8: the FM daemon throttle (Q01-Q08).
  #
  # NOT LISTED, and it is not an oversight: `FMDaemonThrottleCanaryTests` is
  # XCTest, and `extract_ran`/`extract_failures` above parse only Swift
  # Testing's `◇ Test "…" started` lines. An expectation naming an XCTest case
  # can never be matched, so it would report ERROR rather than KILLED. The two
  # source canaries (the streaming sweep and the probe cache guard) are verified
  # by hand instead — see the kvs8 provenance note.
  -only-testing:PlayheadTests/FMDaemonThrottleClassificationTests
  -only-testing:PlayheadTests/FMThrottledPrologueRunnerTests
  -only-testing:PlayheadTests/FMThrottlePermissiveLaneTests
  -only-testing:PlayheadTests/FMThrottleUsabilityProbeTests
  # pmp9's window-level rate-limit defer stays in scope: M02 renames the
  # prologue cause to the WINDOW cause, and a mutation that quietly merged the
  # two would otherwise be judged only by this bead's own suite.
  -only-testing:PlayheadTests/BackfillRateLimitDeferTests
  # playhead-usn1: the per-show skip-mode control (U01-U16). The djl0 suites
  # above stay in scope deliberately — this bead's push replaces the one-shot
  # read djl0's pill consumes, so a mutation that broke the CAUSE taxonomy while
  # satisfying the push would otherwise be judged only by its own suites.
  -only-testing:PlayheadTests/EpisodeShowIdentityTests
  -only-testing:PlayheadTests/SkipModeStreamTests
  -only-testing:PlayheadTests/NowPlayingSkipModeSubscriptionTests
  -only-testing:PlayheadTests/ShowSkipModeWriteTests
  -only-testing:PlayheadTests/RefusedSkipModeSelectionTests
  -only-testing:PlayheadTests/RecoveredShowIdentityAdoptionTests
  -only-testing:PlayheadTests/RefusedShowSkipModeWriteDiagnosticsTests
  # playhead-isp5: the ingest audit trail (W01-W06). The 96ot delivery suites
  # above stay in scope deliberately — the census wraps the SAME door those
  # suites drive, so an instrumentation edit that also broke the delivery would
  # otherwise be judged only by its own assertions.
  -only-testing:PlayheadTests/AdWindowIngestFieldCaseTests
  -only-testing:PlayheadTests/AdWindowIngestOutcomeCountTests
  -only-testing:PlayheadTests/AdWindowIngestDoorOutcomeTests
  -only-testing:PlayheadTests/AdWindowIngestLifetimeTests
  -only-testing:PlayheadTests/AdWindowIngestTaxonomyTests
  # playhead-b6r2: the inventory filter's edge reading (B01-B07). The isp5
  # suites above stay in scope deliberately — the census is where the fix is
  # MEASURED, so an edge-rule mutation that satisfied the filter's own unit
  # tests while losing the window at the orchestrator boundary would otherwise
  # go unseen. The xr3t contract suites are here because this bead rewrote
  # them; a mutation that reverts the reading must redden the rewritten
  # contract, not only the new tests.
  -only-testing:PlayheadTests/InventorySanityInnerEdgeRuleTests
  -only-testing:PlayheadTests/FieldEdgeWindowsArmTests
  -only-testing:PlayheadTests/FieldPreRollBannerTests
  -only-testing:PlayheadTests/InventoryFilterDefaultDivergenceTests
  -only-testing:PlayheadTests/ImpossibleGeometryIsMaterialTests
  -only-testing:PlayheadTests/InventorySanityFilterEdgeTests
  -only-testing:PlayheadTests/InventorySanityFilterRejectionRateTests
  -only-testing:PlayheadTests/InventorySanityFilterRollbackTests
  -only-testing:PlayheadTests/SkipOrchestratorInventoryFilterIntegrationTests
  # playhead-y3ya: the semantic-sweep mark producer (Y01-Y12). The isp5 census
  # suites above stay in scope deliberately — `ingest_armed_suggest` is where
  # this fix is MEASURED, so a composer mutation that satisfied the pure
  # policy tests while losing the window at the door would otherwise go
  # unseen. The WIRE-IN suite is listed because a correct composer that is
  # never called is the one defect no pure test can see.
  -only-testing:PlayheadTests/SemanticSweepFieldCaseTests
  -only-testing:PlayheadTests/SemanticSweepDeclinedVerdictTests
  -only-testing:PlayheadTests/SemanticSweepExtentPolicyTests
  -only-testing:PlayheadTests/SemanticSweepAdditiveOnlyTests
  -only-testing:PlayheadTests/SemanticSweepArmsSuggestTests
  -only-testing:PlayheadTests/SemanticSweepDeclinedSurfacesNothingTests
  -only-testing:PlayheadTests/SemanticSweepWireInTests
  -only-testing:PlayheadTests/SemanticSweepRunnerTailTests
  # playhead-lxkq: the ad-likelihood scan ORDER (X01-X14). The pure suite holds
  # the permutation/ranking/budget claims; the wiring suite is listed because
  # the whole bead is a scheduling change whose only observable consequence is
  # WHICH FM CALL HAPPENS FIRST — a correct permutation that `planPassA` never
  # calls, or a runner that never derives a seed, is exactly the defect the
  # pure tests cannot see.
  -only-testing:PlayheadTests/AdLikelihoodScanOrderTests
  -only-testing:PlayheadTests/AdLikelihoodScanOrderWiringTests
  # playhead-avbn: who may vote that there is no ad (A01-A11). The first two
  # suites are the pure admission rule and the gate value type — both are
  # sub-second. SpanFinalizerTests is listed because the SEVERITY change is only
  # observable in a merge, and the blocked-gate guard suite because the whole
  # Half-1 decision is that this gate DROPS rather than banners: a mutation that
  # routed it to the suggest tier would otherwise satisfy every value-type rail
  # while re-opening the surface the bead closed.
  -only-testing:PlayheadTests/FMSuppressionVotingWindowTests
  -only-testing:PlayheadTests/BlockedByFMConsensusGateTests
  -only-testing:PlayheadTests/SpanFinalizerTests
  -only-testing:PlayheadTests/SkipOrchestratorBlockedGateGuardTests
  # The pass-B row's MEASURED transcript quality is a persistence claim, so its
  # only rail lives in the runner suite. It is the most expensive line in this
  # list; it is here because the alternative is an unpinned producer.
  -only-testing:PlayheadTests/BackfillJobRunnerTests
  # playhead-sik9: the post-roll guard's byte-anchored exemption (C01-C10). The
  # first four are the bead's own suites. `BackfillEvidenceFusionTests` is
  # listed because the two rails this bead INVERTED live there next to the rest
  # of the guard's mechanics, and a mutation that quietly restored the old
  # blanket demotion would otherwise be judged only by the new file.
  -only-testing:PlayheadTests/PostRollGuardByteAnchoredExemptionTests
  -only-testing:PlayheadTests/PostRollGuardSegmentRecoveredReachabilityTests
  -only-testing:PlayheadTests/PostRollGuardExemptionRespects2350Tests
  -only-testing:PlayheadTests/PostRollGuardExemptCueSurfacingTests
  -only-testing:PlayheadTests/BackfillEvidenceFusionTests
  # playhead-nqey: the ENABLEMENT (F01-F09). The sik9 suites above stay in scope
  # deliberately — they prove the guard's LOGIC against a config they build
  # themselves, so they pass whether the flag ships ON or OFF; the two suites
  # here are the only ones that read `AdDetectionConfig.default`. Both families
  # are needed: a mutation to the shipped VALUE is invisible to sik9's suites,
  # and a mutation to the guard's PREDICATE would be judged by nqey's four
  # shapes alone.
  -only-testing:PlayheadTests/CertaintyTieredSkipShipsOnTests
  -only-testing:PlayheadTests/CertaintyTieredSkipFlagsWireInTests
  # playhead-9v09: the ingest census's RETRACTION path (H01-H10). Two suites,
  # ~0.6s combined. The behavioural one drives the real orchestrator through the
  # preload door and then the retroactive sweep; the taxonomy one is a pure test
  # of the value type's three classifiers and its rendering. Both are needed and
  # neither substitutes: a classifier mutation is invisible to the behavioural
  # suite's row assertions when the row is not written at all, and a wiring
  # mutation is invisible to a pure test by construction.
  -only-testing:PlayheadTests/AdWindowIngestRetroactiveRetirementTests
  -only-testing:PlayheadTests/AdWindowIngestTaxonomyTests
  # playhead-hx6n: scan-row run attribution (T01-T15). Three suites, ~1.5s
  # combined. The pure/persistence suite owns the schema, the join and the
  # unknown-stays-unknown arithmetic; the wire-in suite is the ONLY thing that
  # can see whether the production write path stamps anything at all (every
  # other assertion in the series is satisfied by hand-built rows and would stay
  # green while the device wrote NULLs forever); and the migration ladder is the
  # only thing that notices a rung that stops climbing, because `createTables()`
  # builds the final shape unconditionally and masks it everywhere else.
  -only-testing:PlayheadTests/SemanticScanRunAttributionTests
  -only-testing:PlayheadTests/SemanticScanAttributionWireInTests
  -only-testing:PlayheadTests/MigrationLadderTests

  # playhead-le02: the cross-launch preload suite. It is the only place that
  # can see `endEpisode`'s suggest-tier reset and the preloaded `.applied`
  # row's tier, and it was not in this list — so LE07 and LE08 would have
  # reported SURVIVED for the honest but useless reason that nothing able to
  # judge them ever ran.
  -only-testing:PlayheadTests/SkipOrchestratorPreloadTests
  # playhead-pggn: the score/extent coherence rails (PG series). Two suites and
  # both are needed. SegmentAggregatorTests is the only thing that can see the
  # pure state machine's four exits from an open segment; SegmentAggregatorWiring
  # Tests is the only thing that can see what CONFIDENCE actually lands on the
  # persisted row, and a wiring mutation (PG08) is invisible to a pure test by
  # construction. ~0.3 s for both.
  -only-testing:PlayheadTests/SegmentAggregatorTests
  -only-testing:PlayheadTests/SegmentAggregatorWiringTests
  # playhead-kkzu: the download-attribution rails (KZ series). Four suites and
  # each sees something the others structurally cannot. DownloadShowAttribution
  # Tests drives the real DownloadManager, so it is the only thing that can
  # observe the podcastId that actually lands in `analysis_jobs` and the only
  # thing that can see the sidecar cross a process boundary. DownloadContext
  # Tests is pure and is the only thing that judges the type-level half — a
  # blank identity and a named absence. The other two are the callers: a
  # coordinator or feed-refresh mutation is invisible to the manager, because
  # the manager faithfully records whatever it is handed. ~0.5 s for all four.
  -only-testing:PlayheadTests/DownloadShowAttributionTests
  -only-testing:PlayheadTests/DownloadContextTests
  -only-testing:PlayheadTests/EpisodePreparationCoordinatorTests
  -only-testing:PlayheadTests/BackgroundFeedRefreshSharedEnqueuePathTests
  # playhead-kanf: the user-intent promotion rails (KN series). Two suites, and
  # neither can see what the other does. AnalysisStoreUserIntentPromotionTests
  # is the only thing that judges the guarded write in isolation — a `queued`
  # row carrying a stale lease, and a `running` row carrying none, are states
  # the scheduler cannot construct through its own API, so the two halves of
  # `state = 'queued' AND leaseOwner IS NULL` are separable only there.
  # AnalysisWorkSchedulerUserIntentTests is the only thing that can observe the
  # one-shot flag at all: it is private in-memory scheduler state with no
  # accessor, and its survival is visible ONLY as the priority a LATER enqueue
  # lands at. ~1.3 s for both.
  -only-testing:PlayheadTests/AnalysisStoreUserIntentPromotionTests
  -only-testing:PlayheadTests/AnalysisWorkSchedulerUserIntentTests
  # playhead-5n8k: the observer-scoping rails (TS series). 0.6 s.
  -only-testing:PlayheadTests/CoarsePassDiagnosticTaskScopingTests
  # ...plus ONE test from the 100-test FoundationModelClassifierTests suite,
  # selected by method rather than by suite. It is the consumer's-eye view — the
  # test whose fast, misleading failure was the tax this bead removes — and it
  # is the only thing that can see the funnel go inert while the scoping suite's
  # own fixtures still look self-consistent. Naming the whole suite would put
  # 25 s on EVERY batch of this script, for one test's worth of judgement.
  # The parentheses are quoted: bash parses a bare `()` inside an array literal
  # as a syntax error, which is a fast failure but a confusing one.
  '-only-testing:PlayheadTests/FoundationModelClassifierTests/coarsePassWindowGuardrailViolationEmitsDiagnostic()'
  # playhead-26od: the mid-flight coarse checkpoint (CK series). Three suites —
  # the end-to-end durability + lifetime claims, the box's own write-cost
  # arithmetic, and the pure coverage walk. 36 tests, ~0.6 s. All three are
  # needed: the walk cases hand-build their inputs so they pin the RULE and say
  # nothing about whether the checkpoint supplies it, and the end-to-end suite
  # cannot see a rule the store never rejects.
  -only-testing:PlayheadTests/BackfillCoarseCheckpointTests
  -only-testing:PlayheadTests/CoarseCheckpointBoxTests
  -only-testing:PlayheadTests/CoarseCoverageWalkTests
  # ...and the SOURCE canary that guards the lease refresh (CK13 names it as a
  # second expectation). Three grep-only XCTest cases, 0.03 s. It is here
  # because playhead-26od's own extraction is what proved a source canary can go
  # stale while the behaviour is intact — so it has to be under the mutant that
  # deletes the behaviour, not merely in the full gate.
  -only-testing:PlayheadTests/FMUnboundedCallCanaryTests
  # playhead-41mu: the coverage-lane terminal (UC series). One suite, and it has
  # to be its own rather than folded into the pmp9 file above: every fixture
  # there deliberately omits `episodeDurationSec`, which is precisely the input
  # that makes the measured terminal UNREACHABLE, so a suite built on those
  # assets could not observe this gate at all. The 16 tests split into pure
  # decision/cursor rails and five end-to-end runs that are the only things able
  # to see whether the terminal actually consults the measurement (UC01) and
  # whether it consults it with the RUN's own segment list (UC09, R2) — the
  # pure rails cannot see either, because both mutants leave the pure functions
  # correct and change only what is handed to them.
  -only-testing:PlayheadTests/BackfillCoverageTerminalTests
  # playhead-fil5: the durable scan claim (SC series). Five suites, and all five
  # are needed because the claim has four separable failure modes and no suite
  # can see more than one: the predicates + identity + row shape (pure); the
  # store round-trip and its idempotency; the four `runShadowFMPhase` gates
  # driven through the real `runBackfill`, which is the only place a claim that
  # is never WRITTEN shows up; the reconciler sweep, the only place a claim that
  # is never SELECTED shows up; and the selector's own exclusions. ~60 tests,
  # under 2 s for all five.
  -only-testing:PlayheadTests/SemanticScanClaimPredicateTests
  -only-testing:PlayheadTests/SemanticScanClaimPersistenceTests
  -only-testing:PlayheadTests/SemanticScanClaimGateWireInTests
  -only-testing:PlayheadTests/SemanticScanClaimReconcilerTests
  -only-testing:PlayheadTests/MissingCoverageLaneSelectorTests
  # ...and playhead-onn6's own re-drive suites. The SC series changes what the
  # re-drive sweep is HANDED, and a mutation that made the claim visible to
  # nothing (or to everything) would otherwise be judged only by this bead's own
  # fixtures. These are the tests that know what the sweep is supposed to refuse.
  -only-testing:PlayheadTests/AdScanRedriveDecisionTests
  -only-testing:PlayheadTests/AdScanRedriveReconcilerTests
  -only-testing:PlayheadTests/ResumableBackfillJobSelectorTests
  # playhead-9y9e: the path from "transcribed" to "read for ads" (RT series).
  # Four suites, and the split is not cosmetic — the bead has three separable
  # failure modes in three layers and no suite can see more than one:
  #
  #   * the RUNNER's short-circuit (does a pass over a finished transcript reach
  #     Stage 4 at all, and does the drop leave a row?) — only an end-to-end run
  #     through a real TranscriptEngineService can observe that;
  #   * the STORE's ad-scan ruler (is the bound the fast pass or the whole
  #     transcript?) — a pure interval question that the runner suite cannot see
  #     because its fixtures are single-pass;
  #   * the SCHEDULER's cap-out decision (is an unread episode outstanding
  #     work?) — a pure decision matrix.
  #
  # `AnalysisStoreCoverageRulerTests` is where the new ruler tests physically
  # live (the csbq suite that already owns the numerator/denominator fixtures);
  # `AnalysisStoreAdScanCoverageTests` is pz32's own suite and is listed because
  # a widened bound that stopped CLIPPING would satisfy this bead's tests while
  # breaking the intersection pz32 exists for. `CapOutRetryTests` is y8f3's.
  -only-testing:PlayheadTests/TranscriptionAlreadyCompleteTests
  -only-testing:PlayheadTests/AnalysisStoreCoverageRulerTests
  -only-testing:PlayheadTests/AnalysisStoreAdScanCoverageTests
  -only-testing:PlayheadTests/CapOutRetryTests
  # playhead-iu0t: the shadow-retry drain's replay set (CN series). Three
  # suites. The bead's own rails are the first; `ShadowRetryTests` is listed
  # because it is the suite that owns "the drain runs at all" and, until iu0t,
  # the suite that asserted the DEFECT as the contract — a mutant that broke the
  # drain outright must be judged by the tests that predate this bead, not only
  # by its own. `TranscriptChunkCanonicalizerTests` is hc7e's, and is what
  # separates "the drain picked the wrong set" from "canonicalize itself
  # changed"; without it a mutation to the canonicalizer would be credited to
  # the drain. ~50 tests, well under a second.
  -only-testing:PlayheadTests/ShadowRetryCanonicalReplayTests
  -only-testing:PlayheadTests/ShadowRetryTests
  -only-testing:PlayheadTests/TranscriptChunkCanonicalizerTests
  # playhead-iu0t R1: the RULE, enforced. A SOURCE canary — it reads the
  # working-tree `.swift` files rather than running the code — so it kills a
  # mutant at a call site with no behavioural test of its own, which is exactly
  # the situation `pushEvidenceCatalog` (CN06) was in: private, unseamed, and
  # covered by nothing. Two tests, no build cost beyond compiling them.
  -only-testing:PlayheadTests/TranscriptCanonicalizationRuleCanaryTests
)

# Named to match the `/private/tmp/playhead-*` pattern `scripts/disk-cleanup.sh`
# already sweeps at 3 days, so kept logs are reaped by the existing weekly cron
# instead of accumulating on a box with documented disk pressure.
WORK="$(mktemp -d /private/tmp/playhead-mutation-battery.XXXXXX)"
# Without this, a failed mktemp leaves WORK empty and every `>"$WORK/…"` fails
# quietly-ish while `rm -rf "$WORK"` runs against the empty string. Cheaper to
# refuse than to reason about.
[ -n "$WORK" ] && [ -d "$WORK" ] || {
  echo "mutation-battery: could not create a work directory under /private/tmp" >&2
  exit 2
}
KEEP_WORK=0
# Armed only once `require_clean_tree` has proved every mutable file is
# pristine; before that a `git checkout --` would destroy the caller's work.
TREE_OWNED=0

on_exit() {
  if [ "$TREE_OWNED" -eq 1 ]; then
    # INT/TERM land here too. Without this, Ctrl-C during a 2-minute
    # xcodebuild leaves an injected defect — e.g. `feedbackAssetMatches`
    # returning true, which neuters the episode-ownership check in all four
    # explicit-feedback transactions — sitting in the working tree.
    restore_sources
  fi
  if [ "$KEEP_WORK" -eq 1 ]; then
    echo "mutation-battery: per-batch xcodebuild logs kept in $WORK" >&2
  else
    rm -rf "$WORK"
  fi
}
trap on_exit EXIT
trap 'echo; echo "mutation-battery: interrupted — restoring sources" >&2; exit 130' INT TERM

# ---------------------------------------------------------------------------
# The battery.  NAME|BATCH|FILE_KEY|expected display names (';'-separated)
# ---------------------------------------------------------------------------
# playhead-avbn: the FM-suppression admission rule and the gate it feeds.
T_AVBN_REFINE_EMPTY="a pass-B refinement that found no spans does NOT vote"
T_AVBN_REFINE_TWO="two empty pass-B refinements cannot manufacture a noAds consensus"
T_AVBN_REFINE_FOUND="a pass-B refinement that DID find spans does not vote either"
T_AVBN_SENTINEL_ONE="a no-work sentinel does NOT vote"
T_AVBN_SENTINEL_TWO="two no-work sentinels cannot manufacture a noAds consensus"
T_AVBN_ABUTS="a row that merely abuts the span does not vote"
T_AVBN_BAND="a degraded transcript bands weak, a good transcript bands moderate"
T_AVBN_COARSE_VOTES="a coarse noAds row that examined its window votes"
T_AVBN_COARSE_PAIR_TRIGGERS="two genuine coarse noAds windows still trigger suppression"
T_AVBN_WIRE="applyFMSuppression builds its windows through votingWindows, not inline"
T_AVBN_MEASURED_QUALITY="playhead-avbn: a passB row reports the MEASURED transcript quality, never a hardcoded good"
T_AVBN_SEVERITY="blockedByFMConsensus sorts with the blocked cases, not with markOnly"
T_AVBN_MERGE_DEMOTES="playhead-avbn: merging markOnly with blockedByFMConsensus demotes to blockedByFMConsensus"
T_AVBN_LEGACY_DECODES="the pre-rename raw value still decodes to blockedByFMConsensus"
T_AVBN_LEGACY_CODABLE="the pre-rename raw value decodes through Codable too"
T_AVBN_LEGACY_REENCODE="re-encoding a legacy row writes the CANONICAL raw value, not the alias"
T_AVBN_ALIAS_ONE_WAY="the alias is one-way: no OTHER unknown raw value decodes"
T_AVBN_BLOCKED_DROPPED="blocked eligibilityGate values do NOT enter active managed-window set"

# playhead-le02 (LE01-LE08). The A11 CLASS, generalised past the one rail avbn
# repaired. A11 survived five negative assertions because it only ARMED a banner
# and playhead-d3g0 defers emission to playhead entry; le02 found the same hole
# on fourteen more rails and gave each a positive witness. These mutations are
# the proof: every one routes a must-drop span to the suggest tier, or corrupts
# the census row that names where a span went. A repaired rail dies; the rail as
# it stood before le02 would not have noticed.
T_LE02_INV_SHORT="Hot-path span < 2 s is filtered out before reaching active set"
T_LE02_INV_HEAD="Span lying inside the first 3 s is filtered out"
T_LE02_INV_TAIL="Span lying inside the last 3 s is filtered out when duration is known"
T_LE02_INV_CHAPTER="Chapter overlap filters the span — and requires the chapters push"
T_LE02_INV_SYNTH="Non-trailing open-ended chapter gets its end synthesized from the next chapter"
T_LE02_INV_FUSION="Filter also gates AdDecisionResult path (fusion → backfill push)"
T_LE02_INV_RETRO_CHAPTER="Chapters arriving AFTER preloaded spans retroactively reject overlapping windows"
T_LE02_INV_RETRO_DURATION="Episode duration arriving mid-episode retroactively retires late-edge spans"
T_LE02_BYPASS_ADWINDOW="same-ID AdWindow with changed bounds must pass inventory validation"
T_LE02_BYPASS_DECISION="same-ID decision with changed bounds must pass inventory validation"
T_LE02_DOOR_FOREIGN="ingest for an asset that is NOT the one playing delivers nothing"
T_LE02_DOOR_NOEPISODE="ingest with no active episode delivers nothing"
T_LE02_DOOR_VETOED="a user-vetoed row is not resurrected by the mid-session door"
# These two are XCTest, which this battery could not match until le02 taught
# `extract_ran`/`extract_failures` to read `Test Case '-[Suite method]'` lines.
# Written in the QUALIFIED `Suite/method` form the extractors also emit, so a
# same-named method in another suite cannot manufacture a kill.
T_LE02_PRELOAD_APPLIED="SkipOrchestratorPreloadTests/testPreloadedAppliedWindowDoesNotEmitBanner"
T_LE02_ENDEPISODE_RESET="SkipOrchestratorPreloadTests/testEndEpisodeResetsEmittedAutoSkipBannersSet"
T_LISTEN_RACE="A Listen revert whose episode is replaced mid-flight still calibrates the captured show"
T_MANAGED_RACE="A time-range revert whose episode is replaced mid-loop calibrates the captured show and leaves the replacement alone"
T_SUGGEST_RACE="A suggest-only revert whose episode is replaced mid-loop keeps its receipt and stops retiring banners"
T_ANON_RACE="An anonymous time-range revert replaced mid-loop keeps its receipt and still gates cue republication"
T_LISTEN_FP="Listen revert of a managed auto-skip window records a FALSE-POSITIVE signal (integral +1)"
T_REVERTWINDOW_FP="Manual 'not an ad' revertWindow records a FALSE-POSITIVE signal"
T_SUGGEST_NO_NOSTORE="Suggest-No persists its durable receipt with no correction store wired"
T_OWNERSHIP="explicit responses are refused when the card's episode does not own the asset"
T_AUTOFADE="declineSuggestedSkip auto-fade (isExplicitDenial:false) records NO correction and leaves userDismissedBanner=0"
T_CONFIRM_SILENT="Confirming an auto-skipped banner records no controller sample and no hard negative"
T_SUGGESTONLY_SILENT="A suggest-tier-only revertByTimeRange records no controller sample"
# playhead-o4qr renamed this test when it grew the other three learning
# clauses. The name here is the DISPLAY name xcodebuild prints, so a stale
# copy silently un-credits every mutation that names it (N06, N07, O01-O04).
T_ANON_SILENT="An anonymous revert (no podcastId, or an empty one) keeps its receipt and records NO show-keyed learning"
T_STALE_SHOW_NO="A banner No naming another show keeps its receipt and records NO show-keyed learning"
T_REVERTWINDOW_VETO="revertWindow records a public manual veto and generic decision log"
T_ACCEPT_RACE="A suggest Yes whose episode is replaced mid-flight calibrates the captured show"
T_DENY_RACE="A banner No whose episode is replaced mid-flight calibrates the captured show"

# playhead-ugy4. Seven tests that named a suggest-tier contract but could not
# observe it: their fixtures made `acceptSuggestedSkip` abort in
# `AnalysisStore.persistAcceptedSuggestionIfCurrent` (asset owned by a
# different episode, no durable suggestion row) long before the seam under
# test mattered, so "a stale Yes must not promote" was proved by the fixture.
# The fixtures are repaired; these five entries are what keeps them honest.
T_ADWINDOW_STALE_YES="blocked and inventory-rejected AdWindows retire stale suggestions"
T_DECISION_STALE_YES="blocked and inventory-rejected decisions retire stale suggestions"
T_EXPLICIT_RETIRE_STALE_YES="explicit window retirement also invalidates a suggestion"
T_LATE_INVENTORY_STALE_YES="late inventory context retires a newly-invalid suggestion"
T_STALE_IDENTITY="episode-bound suggest actions reject stale banner identities"
T_FUSION_CLEARS_SUGGEST="Fusion result with same id as an open suggest entry clears the suggest entry (playhead-rfu-sad)"
T_DECLINE_NO_CONFIRM="declineSuggestedSkip drops the window without confirming it"

# playhead-auz3. Two more of the same class, found while fixing ugy4 and
# deliberately left out of its stated seven. Both were PROVED vacuous by
# mutation before repair: S06 and S07 each SURVIVED against the fixtures as
# they stood.
T_GATE_FLIP_CLEARS_SUGGEST="Gate flip from markOnly clears suggest entry — accept after flip is a no-op (playhead-rfu-sad)"
T_DIRECT_REPLACEMENT_CLEARS_ACCEPTED="direct episode replacement clears accepted-suggestion race guards"

# playhead-1mq1.2.1. The mixed-width fixture: an auto window whose WIDTH was
# wrong, reverted in one tap. Both learning surfaces it can poison are asserted
# in this one test, which is why P01 and P02 need separate batches.
T_MIXED_WIDTH="THEMOVE: reverting 3493.02-3536.90 lands no negative label on the true ad 3505.74-3536.10"

# playhead-d3g0 — the suggest banner's playhead gate and skip affordance.
T_D3G0_FIELD="Field case: one batch of three spans emits NOTHING until the playhead reaches each"
T_D3G0_ENTRY="Fires on the first observation INSIDE the span, and not one tick earlier"
T_D3G0_BUDGET="Worst-case tick alignment still banners inside the budget"
T_D3G0_PREROLL="Pre-roll banners when the playhead is AT 0:00 — and not when the episode is resumed past it"
T_D3G0_PAST="A span already behind the playhead never banners"
T_D3G0_ONCE="Entry fires at most once per window, however many ticks land inside"
T_D3G0_SEEK="Seeking backwards into an already-fired span does not re-ask"
T_D3G0_REPLAY="A host that attaches late replays only spans the playhead has entered — exactly once"
T_D3G0_CATALOG="Moving the emit later means the banner carries the RICHER catalog"
T_D3G0_NOSKIP="A both-edges-unanchored span does not offer a skip it cannot perform"
T_D3G0_SKIP="A byte-exact span still offers a real skip"
T_D3G0_MATCH="The card's claim matches what the tap actually does"
# NOTE: no semicolon. `;` is the MUTATIONS record separator for expected test
# names, so a display name containing one is silently split into two names that
# match nothing — which is exactly how D09 first reported SURVIVED against a
# test that had failed correctly. The test was renamed rather than the parser
# taught to escape: one fewer thing to remember.
T_D3G0_COPY="Mark-only copy drops the skip promise and skippable copy is untouched"
T_D3G0_LATENCY="Entry latency budget is derived from the real transport tick, not chosen"
T_D3G0_REPLAY_EVENTS="The EVENT stream replays only entered spans too"
T_D3G0_SAME_TICK_ORDER="Two spans entered by one observation banner in playhead order"
T_D3G0_EXACT_REPLAY="An exact producer replay after retirement re-arms rather than re-asking"

# playhead-o4qr MERGE NOTE — READ BEFORE "FIXING" M01/M02/M03/M04/M06.
#
# Those five entries anchor on `revertByTimeRange`'s TWO mutating loops: a
# managed loop and a suggest loop, each writing one row at a time with an
# in-loop lifecycle guard between iterations, plus a work list built after the
# first suspension. playhead-o4qr replaced that shape wholesale. The merged
# seam now: snapshots BOTH work lists before any await, revokes recurrence
# evidence, commits every row in ONE `persistRevertedAdWindowsIfCurrent`
# transaction, and only then runs a single live pass gated on
# `sourceLifecycleIsCurrent` that ALSO re-validates each entry against live
# state and its producer revision.
#
# The defects these five describe are therefore not merely re-anchorable, they
# are UNREPRESENTABLE — verified against the merged source, not assumed:
#
#   M01 (delete the managed in-loop guard) and M02 (the suggest one): there is
#   no in-loop guard. Deleting the outer gate does not reproduce the defect
#   either, because the live pass looks each id up in the CURRENT dictionary
#   (`windows[id]` / `suggestWindows[id]`) and `continue`s when absent —
#   `beginEpisode` has cleared it — so no stale entry can be re-inserted. The
#   only observable effect left is `evaluateAndPush()`, which is M12's rail.
#
#   M03 / M04 (`break` -> `return`): both guards are gone, and a `return`
#   placed before the effects is byte-for-byte the mutation M07 now applies.
#   Keeping them would be two aliases of one rail, i.e. a fabricated rail.
#
#   M06 (drop the lifecycle gate on the suggest work list): the work list is
#   built before the first suspension, so there is no post-await gate to drop.
#
# They are moved to the KNOWN GAP block below rather than deleted: their
# `apply_mutation` arms are kept verbatim, so restoring any of them is a
# one-line edit if the analysis above is ever shown wrong. They are NOT left in
# `MUTATIONS` ERRORing, because a batch aborts on its first failed anchor —
# leaving them there took nine OTHER mutations (M05 M07 M08 M09 M11 M12 M14 M18
# M20) down with them in batches 1, 2 and 4, which is a strictly worse outcome
# than a documented gap: it turns the whole script into a no-op.
#
# Whether these five contracts are now structurally guaranteed (the analysis
# says yes) or merely unpinned is a COVERAGE decision for a human. The
# behavioural rails themselves — the four `SkipOrchestratorRevertLifecycle`
# race tests — are unchanged and pass; see the merge commit.
# playhead-96ot — a day-0 mint must reach the session that minted it.
T_96OT_INACTIVE="ingest for an asset that is NOT the one playing delivers nothing"
T_96OT_NOEPISODE="ingest with no active episode delivers nothing"
T_96OT_VETOED="a user-vetoed row is not resurrected by the mid-session door"
T_96OT_ARMS="mid-session ingest ARMS a suggestion and the next observation presents it"
T_96OT_UNMARKED="an UNMARKED day-0 run delivers nothing"
T_96OT_COVERED="a run whose slots were already covered delivers nothing"
T_96OT_ONCE="a marked day-0 run delivers its asset id EXACTLY once"
T_96OT_MARKCOUNT="a marked day-0 run reports its MARK count, distinct from rotatedCount"
T_96OT_FIRSTLISTEN="a first-listen day-0 mint produces a skip cue WITHOUT relaunching the episode"

# playhead-djl0 — the CAUSE behind the active skip mode. Every one of these
# names a way to collapse `.shadow` back into a single silent value.
T_DJL0_DISTINGUISH="a deliberate shadow and an unresolved identity are DISTINGUISHABLE"
T_DJL0_NEWSHOW="a show with no profile yet is a new-show default, not a failure"
T_DJL0_NOTRUST="a missing trust service is its own cause, not an unresolved identity"
T_DJL0_NONCANON="a non-canonical show id is an identity failure, not a new show"
T_DJL0_BADMODE="a profile whose stored mode does not decode is its own cause"
T_DJL0_STALE="a beginEpisode superseded mid-hydration leaves no stale cause"
T_DJL0_OVERRIDE="an explicit session override is its own cause"
T_DJL0_NOEPISODE="no episode is running before beginEpisode and after endEpisode"
T_DJL0_FAILURESET="exactly the lookup failures are classified as failures"
T_DJL0_SHOWLESS="only an unresolved identity leaves the session without a show"
T_DJL0_COUNTED="each episode begun without a resolvable identity increments its counter"
T_DJL0_UNAFFECTED="a resolvable show never increments any failure counter"
T_DJL0_NOBLEED="counters are per cause — a missing trust service does not inflate the identity count"
T_DJL0_LOGGED="an unresolved identity writes a coded entry to the session log"
T_DJL0_QUIET="a resolvable show writes no skip-mode failure entry"
T_DJL0_TRUSTCODE="a missing trust service records the trust-lookup code, not the identity code"
T_DJL0_RECOVER="a nil caller identity is recovered from the durable job row"
T_DJL0_RECOVER_ACTIVE="the recovered identity becomes the session's active show"
T_DJL0_CALLER_WINS="a supplied identity is never overridden by the job row"
T_DJL0_NULL_ROW="a job row with a NULL podcastId falls through to the named failure"
T_DJL0_STORED_NONCANON="a non-canonical stored identity is not recovered"
T_DJL0_NULL_MASK="a NULL newest row does not mask an older row that knows the show"
T_DJL0_NEWEST="the newest job row wins when an episode has several"
T_DJL0_NOT_BORROWED="another episode's job row is not borrowed"
T_DJL0_FIELD="day-0 marks delivered to a session with no show still banner, and leave a trace"
T_DJL0_PILL_DISTINCT="a deliberate shadow and an unresolved identity do not render the same pill"
T_DJL0_PILL_LABELS="a resolved show keeps its existing pill text exactly"
T_DJL0_PILL_LOCKED="an unresolved identity withholds the per-show control"
T_DJL0_PILL_LIVE="a trust-lookup failure keeps the control — the show is still known"
T_DJL0_PILL_VISIBLE="the pill is shown for an unresolved identity even with no podcast title"
T_DJL0_PILL_HIDDEN="the pill stays hidden when there is simply nothing playing"
T_DJL0_VM_LOAD="loadSkipMode carries the CAUSE across, not just the mode"
T_DJL0_VM_OVERRIDE="setting a mode marks the resolution as a session override"

# playhead-4dqe — day-0 at DOWNLOAD time. Display names, verbatim; the
# separator in a MUTATIONS record is `;`, so none of these may contain one.
T_4DQE_LDM_CELLULAR="LOW DATA MODE WINS ON CELLULAR too — it is an OS-level instruction, not a transport rule"
T_4DQE_LDM_OUTRANKS="Low Data Mode outranks the user setting: flipping the toggle cannot change the verdict"
T_4DQE_CELLULAR_ALLOWED="cellular WITH the setting is ALLOWED — this is what Dan moved out of code"
T_4DQE_DEFAULT_WIFI="THE DEFAULT IS WIFI ONLY — a metered user must never lose ~130 MB/episode before finding the toggle"
T_4DQE_UNTOUCHED_INSTALL="an untouched install reads WiFi only"
T_4DQE_WINDOW_ROLLS="the window ROLLS after 24 h from FIRST SPEND (not calendar midnight — a timezone crossing must not gain or lose a day)"
T_4DQE_SPEND_RESTARTS="a spend after the window elapsed RESTARTS it rather than accumulating forever"
T_4DQE_FULL_ESTIMATE="an attempt is admitted on the FULL estimate — never a partial fetch, which cannot mint at the k-way floor"
T_4DQE_ZERO_SPEND="a zero-byte spend does not start a window — an attempt that spent nothing has not opened a day"
T_4DQE_FURTHEST_PROGRESS="the wait reports the FURTHEST progress it ever saw, not the last (an LRU eviction must not rewrite history)"
T_4DQE_CANCELLED_OWN_OUTCOME="a cancelled wait is .cancelled — teardown is not a defect and must not be reported as one"
T_4DQE_DISTINCT_CODES="THE PRE-EWAG SIGNATURE and a download failure map to DIFFERENT invariant codes — the remedies differ"
T_4DQE_MISSING_FILE_CAUSE="a download whose bytes never land is a DIFFERENT counted cause with a DIFFERENT code"
T_4DQE_UNDATED_LAST="an UNKNOWN publish date sorts LAST — a missing date is not evidence of newness"
T_4DQE_TOTAL_ORDER="equal publish dates fall back to FIFO, then to episode id — the order is TOTAL"
T_4DQE_NEWEST_FIRST="A CONTENDED BATCH DRAINS NEWEST FIRST — Dan's ordering sub-decision, end to end"
T_4DQE_DEDUPE="a SECOND kickoff for an episode already in flight does not double-spend the wait"
T_4DQE_PRE_EWAG_SURFACED="PRE-EWAG REPRODUCTION: the file lands, the asset never does → counted, recorded, and SURFACED"
T_4DQE_ORPHAN_ROW="A KICKOFF FOR AN EPISODE WITH NO ASSET ROW IS RECORDABLE — the whole reason this table is episode-keyed"
T_4DQE_COUNTS_ACCUMULATE="counts ACCUMULATE per episode so \`kickoffCount\` large + \`firedCount\` zero reads as the pre-ewag failure"
T_4DQE_MIXED_HISTORY="a mixed history keeps BOTH numbers — a device that recovered is distinguishable from one that never worked"
T_4DQE_STORE_ROLLS="a spend more than 24 h after the window started ROLLS it — yesterday's bytes do not bind today"
T_4DQE_CELLULAR_RECORDED="A CELLULAR REFUSAL IS RECORDED — before playhead-4dqe the gate returned an empty summary and wrote NOTHING"
T_4DQE_LDM_RECORDED="LOW DATA MODE ON WIFI is recorded under its OWN exit — not folded into the cellular refusal"
T_4DQE_CELLULAR_FETCHES="CELLULAR WITH THE SETTING ON ACTUALLY FETCHES — the setting is not decorative"
T_4DQE_BUDGET_REFUSES="AN EXHAUSTED DAILY BUDGET REFUSES AND IS RECORDED — on WiFi, where nothing else would have stopped it"
T_4DQE_REAL_COST="an attempt that RAN folds the bytes it ACTUALLY spent into the window — not the pre-flight estimate"
T_4DQE_CELLULAR_NO_READ="the suppression check runs AFTER the WiFi gate — a cellular play never reads the store"
T_4DQE_REQUEST_CELLULAR="a day-0 request under the opted-in setting ACTUALLY permits cellular — a gate alone would just fail the fetch"
T_4DQE_SOCKET_LDM="the cellular-capable session opens cellular and expensive paths — but NEVER the constrained one"
T_4DQE_FETCHER_FOLLOWS_SETTING="a fetcher with a cellular session follows the SETTING, both ways"

# playhead-eks2 — ad-pod continuation is ON in production, and the two gates
# that bound its downside still hold with the flag at its shipped value.
T_EKS2_SHIPPED_ON="the production config ships pod continuation ON"
T_EKS2_INIT_DEFAULT="a config built without the argument matches the shipped value"
T_EKS2_RECOVERS="the shipped flag persists a continuation mark over the missed creative"
T_EKS2_NO_AUTOSKIP="2350: a persisted continuation span never auto-skips, and still banners"
T_EKS2_CONFIRM_MARKS="ynmk: confirming a continuation banner marks, it does not skip"
T_EKS2_ENTRY_ONCE="d3g0: a continuation window ingested mid-listen arms, fires on entry, once"
T_EKS2_DAY0_SEED="Field case: a day-0 rediff mark seeds where an aggregator tile does not"
T_EKS2_WIREIN_TIER="flag ON: every persisted continuation row is banner-tier"
T_EKS2_WIREIN_COMPOSES="flag ON: runBackfill persists at least one continuation mark"

# playhead-evc1 — a STRICT day-0 byte-exact rediff mark seeds a pod walk while
# still `.candidate`, and NOTHING else `.candidate` does. The carve-out is
# scoped on PROVENANCE, so every mutation below asks one of two questions: does
# the admission still fire for the row that found the ad, and does the refusal
# still hold for the coarse aggregator tile that cost 210 s of show?
T_EVC1_SEEDS="a strict day-0 byte-exact mark seeds while still candidate"
T_EVC1_FIELD="Field case: the day-0 mark that found ad 1 now marks ad 2"
T_EVC1_SURVIVES="the production-faithful day-0 seed survives its own backfill"
T_EVC1_OTHERS_REFUSED="every other candidate row is still refused as a seed"
T_EVC1_TILE_SILENT="an aggregator candidate tile composes nothing while a day-0 row composes a mark"
T_EVC1_SEGMENT_RECOVERED="a segment-recovered day-0 slot does not seed until its edges are validated"
T_EVC1_VETOED="a vetoed or suppressed day-0 row never seeds"
T_EVC1_GATE_BLIND="the day-0 carve-out does not read the eligibility gate"
T_EVC1_NO_AUTOSKIP="2350: a day-0 seeded continuation span never auto-skips, and still banners"
T_EVC1_CONFIRM_MARKS="ynmk: confirming a day-0 seeded banner marks, it does not skip"
T_EVC1_ENTRY_ONCE="d3g0: a day-0 seeded continuation window arms on ingest, fires on entry, once"
T_EVC1_SEED_UNTOUCHED="the day-0 seed row itself is never modified"
T_EVC1_DELIVERY="the carve-out raises a continuation row across the delivery floor"
# playhead-kvs8 — an FM daemon THROTTLE is not a model failure. The field row
# (DE0784D8, status=failed / retryCount=1 / raw daemon prose in deferReason) is
# three defects in three columns, and Q01-Q03 are one rail each.
T_KVS8_DEFERS="a daemon throttle in the coarse-pass PROLOGUE defers the job — it must not mark it failed"
T_KVS8_COVERAGE="a throttled prologue leaves coverage accounting untouched — no cursor, no scan rows"
T_KVS8_RETRY="a throttle does not spend a lifetime retry — the field row's retryCount=1"
T_KVS8_CAUSE="a throttled prologue records a NAMED cause, not the daemon's raw prose"
T_KVS8_VACUITY="VACUITY CONTROL: a non-throttle prologue throw still marks the job failed and burns a retry"
T_KVS8_BATCH="a throttled batch leaves no job stranded in queued and none marked failed"
T_KVS8_PERM="a daemon throttle on the permissive path persists as rateLimited, never as a permissive refusal"
T_KVS8_REASON="a throttle Reason maps to rateLimited and is charged to no permissive counter"
T_KVS8_PROBE="a throttle is not a usability verdict, so it must not be cached"
T_KVS8_NAMES="the pass-prologue defer cause is a NAMED token, distinct from pmp9's window token"
T_KVS8_CONSEC="the drain stops CONSECUTIVELY, not on a lifetime tally"
# playhead-usn1 — the per-show skip-mode CONTROL. The field symptom ("show
# unknown", no menu) was not a lost identity: it was a surface that read the
# mode ONCE, before `beginEpisode` had resolved the show, and never again.
T_USN1_LATE="a subscriber attached before beginEpisode receives the resolved mode"
T_USN1_NOTFINAL="the cleared pair is not the LAST thing a pre-attached subscriber sees"
T_USN1_REPLAY="the current pair is replayed the moment a subscriber attaches"
T_USN1_ENDCLEAR="endEpisode publishes the cleared pair"
T_USN1_OVERRIDE="an explicit choice publishes the session override"
T_USN1_PRECLEAR="beginEpisode publishes the cleared pair before it resolves the show"
T_USN1_VMLATE="the view model learns the mode resolved after it appeared"
T_USN1_VMMENU="the pill the tracked resolution drives offers the per-show menu"
T_USN1_RECOVER="a missing relationship still resolves the show from the episode key"
T_USN1_IDENTICAL="the recovered identity is byte-identical to the relationship's"
T_USN1_GUIDSEP="a guid containing the separator still recovers the feed URL"
T_USN1_IPV6="an IPv6 feed host still recovers the feed URL"
T_USN1_CANON="a non-canonical spelling is refused rather than trimmed"
T_USN1_REFUSE="a session with no show REFUSES the write instead of skipping it"
T_USN1_COUNT="a refused write is counted"
T_USN1_SESSION="a refused write does not change the session mode either"
T_USN1_ADOPT="the runtime adopts an identity only the orchestrator could recover"

# playhead-isp5 — the ingest audit trail (W01-W06).
T_ISP5_NOTPLAYING="an ingest for an episode that is not playing is counted, not just logged"
# playhead-b6r2 RE-POINTED this one. It used to name "the field pre-roll is
# dropped by the inventory filter as tooEarly", which was the only test
# asserting the drop's REASON — and that test now asserts the pre-roll ARMS,
# because the drop was the defect. W02 (stamp the outcome without its reason)
# would have SURVIVED silently against the renamed test. The detail assertion
# moved to the head-artifact test, so the rail follows it. This is the
# script's documented failure mode — an expectation naming a test that no
# longer makes the claim — caught by reading the rename rather than by a run.
T_ISP5_REASON="the production filter still rejects a head artifact in the same delivery"
T_ISP5_LIFETIME="endEpisode clears the per-window stamps and keeps the counts"
T_ISP5_DELIVERED="exactly three outcomes count as delivered"
T_ISP5_ROW="the delivery leaves ONE durable census row that names the cause"
T_ISP5_READ="no admissible rows records how many rows were read"
T_ISP5_SILENT="a preload that read nothing counts but writes no durable row"
T_USN1_NOOVERWRITE="adoption never overwrites an identity the runtime already has"
T_USN1_STALE="a superseded play request does not adopt"
T_USN1_VMREVERT="a refused write restores the pill to what it said before the tap"
T_USN1_TRACE="a refused write is recorded under its own code"
T_USN1_MISMATCH="a key that is not this episode's key recovers nothing"

# playhead-b6r2 — the inventory filter's edge reading.
T_B6R2_PREROLL="the field pre-roll at 0.0-45.1 passes"
T_B6R2_PREROLL_ARMS="the field pre-roll is armed by the inventory filter, not dropped"
T_B6R2_PREROLL_BANNER="the pre-roll's banner reaches the listener"
T_B6R2_PREROLL_XR3T="Span merely STARTING at the head edge passes — this is a pre-roll"
T_B6R2_PREROLL_ORCH="A pre-roll starting at 0.0 reaches the active set"
T_B6R2_FIELD_SLOTS="A pre-roll and a post-roll are VALID spans"
T_B6R2_POSTROLL="the field post-roll ending 1.1 s before the episode end passes"
T_B6R2_POSTROLL_ARMS="the field post-roll arms once the episode duration is known"
T_B6R2_POSTROLL_XR3T="Span merely ENDING at the episode end passes — this is a post-roll"
T_B6R2_POSTROLL_ORCH="A post-roll ending at the episode end reaches the active set"
T_B6R2_HEAD_BOUNDARY="the head boundary is the inner edge at exactly edgeMarginSeconds"
T_B6R2_TAIL_BOUNDARY="the tail boundary is the inner edge at exactly duration - edgeMarginSeconds"
T_B6R2_DEFAULT="the init default enforces the filter, with no argument passed"
T_B6R2_FRESH_INSTALL="the init default equals what production loads on a fresh install"
T_B6R2_MATERIAL="a negative start is refused by the material check, not the edge rule"

# playhead-y3ya — a semantic containsAd verdict has standing on its own.
T_Y3YA_FIELD="both DE0784D8 verdicts produce a candidate"
T_Y3YA_MARKONLY="every emitted mark is markOnly"
T_Y3YA_UNANCHORED="every emitted mark is unanchored on both edges"
T_Y3YA_PRELOAD="a mark clears the cross-launch preload confidence floor"
T_Y3YA_UNCERTAIN="an uncertain verdict produces nothing"
T_Y3YA_UNEXAMINED="a containsAd row whose scan never examined the window produces nothing"
T_Y3YA_SENTINEL="a no-work sentinel row produces nothing"
T_Y3YA_PASSB_DECLINED="a declined passB refinement leaves the coarse presence verdict standing"
T_Y3YA_ORPHAN_PASSB="a passB verdict with no coarse parent stands on its own"
T_Y3YA_NO_GATE="no anchor anywhere still emits the mark"
T_Y3YA_CLIP_UNANCHORED="a clipped mark still records both edges as unanchored"
T_Y3YA_MERGE_BOUND="verdicts separated by more than the merge gap stay separate"
T_Y3YA_CLIP_RADIUS="an anchor beyond the clip radius does not move an edge"
T_Y3YA_VETO="a verdict overlapping a user-reverted window produces nothing"
T_Y3YA_ARMS="both field verdicts arm the suggest tier"
T_Y3YA_ARMS_COUNT="the ingest census counts two armed suggestions"
T_Y3YA_NOT_MANAGED="a sweep mark never enters the managed auto-skip set"
T_Y3YA_RELAUNCH="a sweep mark also arms through the cross-launch preload"
T_Y3YA_WIRE="flag ON: the verdict is persisted as a sweep row"
T_Y3YA_TAIL="flag ON: a job run composes the persisted verdict into a mark"
T_Y3YA_SHADOW_SVC="shadow mode composes nothing at the service site"
T_Y3YA_SHADOW_RUN="shadow mode composes nothing at the runner tail"
T_Y3YA_CEILING="a verdict wider than the mark ceiling produces nothing"
T_Y3YA_MERGE_CEILING="the merge stops rather than growing past the mark ceiling"

# playhead-lxkq — the ad-likelihood scan order.
T_LXKQ_POD="lxkq: the 2828 seam pulls the missed pod into the first four FM windows"
T_LXKQ_BUDGET="lxkq: reaching the missed pod costs under fifteen minutes of FM budget, not two hours"
T_LXKQ_PREFIX="lxkq: nothing from the first half of the episode jumps the seeded prefix"
T_LXKQ_IDENTITY="lxkq: an episode with no seeds keeps the linear sweep exactly"
T_LXKQ_NOT_IDENTITY="lxkq control: with seeds the order is NOT the identity"
T_LXKQ_PERMUTATION="lxkq: seeding never starves a window — the result is a permutation"
T_LXKQ_FILLER="lxkq: the un-promoted remainder stays in episode order"
T_LXKQ_UNUSABLE="lxkq: an episode whose only seed is unusable falls back to the linear sweep"
T_LXKQ_RANK="lxkq: the higher-scoring neighbourhood is attempted first"
T_LXKQ_AGREEMENT="lxkq: two seeds agreeing on one region outrank a single stronger seed elsewhere"
T_LXKQ_WEIGHTS="lxkq: an acoustic seam outranks a lexical cue of equal strength"
T_LXKQ_TIES="lxkq: equal scores break to the earlier window, deterministically"
T_LXKQ_CAP="lxkq: the promoted prefix never exceeds the audio budget"
T_LXKQ_OVERSIZE="lxkq: a single window wider than the whole budget is still promoted"
T_LXKQ_ZERO="lxkq: a zero-strength seed promotes nothing"
T_LXKQ_NAN_SPAN="lxkq: a window with a non-finite span is never promoted but is never dropped either"
T_LXKQ_CLAMP="lxkq: strength outside [0,1] is clamped rather than trusted"
T_LXKQ_RADIUS="lxkq: a seam opens a three-minute neighbourhood around itself"
T_LXKQ_WIDTH="lxkq: a seed wider than the width ceiling opens no neighbourhood at all"
T_LXKQ_ANCHOR_POS="lxkq: an evidence anchor seeds its own position, not its episode-wide coverage span"
T_LXKQ_RESTORE="lxkq: restoring plan order sorts by the plan key"
T_LXKQ_RESTORE_STABLE="lxkq: restoring plan order is stable for rows sharing one plan"
T_LXKQ_W_FIRST="lxkq wiring: planPassA attempts the seeded window first"
T_LXKQ_W_EPISODE="lxkq wiring: planPassA returns episode order when no seed is supplied"
T_LXKQ_W_INDEX="lxkq wiring: windowIndex still names the EPISODE position after promotion"
T_LXKQ_W_CALL="lxkq wiring: the first FM call of the pass is the seeded neighbourhood"
T_LXKQ_W_CONTROL="lxkq wiring control: unseeded, the same neighbourhood is 37 FM calls in"
T_LXKQ_W_REPORTED="lxkq wiring: the REPORTED plan list is still in episode order"
T_LXKQ_W_ONCE="lxkq wiring: every window is still attempted exactly once"
T_LXKQ_W_RUNNER_ON="lxkq wiring: a runner with the flag ON scans the seam neighbourhood first"
T_LXKQ_W_RUNNER_OFF="lxkq wiring: a runner with the flag OFF sweeps front to back"
T_LXKQ_W_NO_SEAM="lxkq wiring: flag ON with no acoustic seam still sweeps front to back"
T_LXKQ_W_SHIPPED="lxkq wiring: the shipped config has the scan order ON"
# playhead-cgka — the per-test scratch lifetime.
T_CGKA_RECLAIM="an owned directory is reclaimed once its owner is deallocated"
T_CGKA_DEFER="the reclaim is deferred one sweep past the first nil observation"
T_CGKA_READOPT="re-adopting a directory re-arms the deferral for its new owner"
T_CGKA_UNOWNED="an unowned directory is never reclaimed by a sweep"
T_CGKA_AUTOSWEEP="registration drives sweeps without a timer or a thread"
T_CGKA_CLAMP="a non-positive sweep interval is clamped rather than trapping"
T_CGKA_UNREADABLE="an unreadable 0o300 directory is still removed"
T_CGKA_CONCURRENT="concurrent registration and sweeping keeps the ledger consistent"
T_CGKA_STORE_ADOPT="makeTestStoreWithDirectory hands its directory to the shared reaper"
T_CGKA_REGISTERS="makeTempDir registers every directory it hands out"
T_CGKA_OWNEDBY="makeTempDir(ownedBy:) attaches the owner it was given"
T_CGKA_WIPE="the process-boundary wipe removes a root the suite left unreadable"
# playhead-sik9 — the post-roll guard's byte-anchored exemption.
T_SIK9_BEAD="THE BEAD: the DE0784D8 byte-exact post-roll stays eligible with wraj ON"
T_SIK9_BELOW_FLOOR="the byte-exact post-roll stays eligible even below the host-read floor"
T_SIK9_SCORES="the exemption never modifies proposalConfidence or skipConfidence"
T_SIK9_GUARDED="STILL GUARDED: a tail whose width is not byte-derived demotes to markOnly"
T_SIK9_SPLICE="STILL GUARDED: acoustic .spliceSlot width is NOT byte-exact and stays demoted"
T_SIK9_MIXED="rediff width plus other provenance is still exempt (the width owner decides)"
T_SIK9_NO_PROMOTE="the exemption never PROMOTES a blocked rediff tail"
T_SIK9_MUSIC="the exemption does not disarm the unconditional music-only demotion"
T_SIK9_FLAG_OFF="flag OFF is byte-identical for both the exempt and the guarded shape"
T_SIK9_LAGGED="the lagged default REJECTS a tail slot the day-0 opt-in would recover"
T_SIK9_DAY0_STRICT="a day-0 slot no strict persona reproduced is classified non-strict"
T_SIK9_REWRITE="a geometry-REWRITTEN rediff span derives unanchored edges despite .rediffSlot"
T_SIK9_2350="and playhead-2350 demotes it downstream, so the exemption cannot leak a skip"
T_SIK9_INTACT="an intact rediff span keeps both byte-exact edges and survives 2350"
T_SIK9_CUE="an eligible byte-exact post-roll produces a skip cue that never precedes its inner edge"
T_SIK9_FUSION_BELOW="Post-roll guard: a byte-exact rediff span below the floor is EXEMPT (playhead-sik9)"
T_SIK9_FUSION_AT="Post-roll guard: a byte-exact rediff span at/above the floor is EXEMPT (playhead-sik9)"
T_SIK9_FUSION_SPLICE="Post-roll guard: an acoustic .spliceSlot span is NOT exempt (playhead-sik9)"
T_SIK9_UNKNOWN="unknown episode duration keeps the guard inert for the guarded shape too"

# playhead-6qvf (G series): the byte/chroma certainty split. The rails divide
# into (a) the STAMP SITE — only a behavioural run through `runBackfill` can see
# which differ arm left which marker; (b) the PREDICATE and its persistence; (c)
# the EXTENT tier and the padding lane; and (d) the two live CONSUMERS, whose
# discriminating negatives are the ones an "any width oracle" implementation
# fails. Each group is mutated separately because a single fix in the wrong
# place would otherwise be credited with every kill.
T_6QVF_PRED_CHROMA="a chroma-owned span FAILS carriesRediffByteExactWidth and PASSES carriesRediffChromaWidth"
T_6QVF_PRED_BYTE="a byte-owned span is unchanged — byte-exact true, chroma false"
T_6QVF_PRED_INDEP="the two predicates are independent — a span carrying BOTH answers both true"
T_6QVF_OWNS_WIDTH="chroma STILL owns width — isWidthOwnership is true"
T_6QVF_UNANCHORED="a chroma-owned span derives UNANCHORED on both edges — tier .none, not .deterministic"
T_6QVF_RAIL_SWEEP="RAIL — ONLY .rediffSlot derives the deterministic tier, and no other anchor alone does"
T_6QVF_RAIL_MIXED="RAIL — a chroma span mixed with ordinary presence anchors still does not reach deterministic"
T_6QVF_RAIL_LANE="RAIL — a chroma span's anchors do NOT open the qs0d targeted padding lane"
T_6QVF_RAIL_MARGIN="RAIL — a chroma span has NO late-safe start margin, so it cannot be auto-skipped"
T_6QVF_TYPESTRING="chroma encodes a STABLE bare 'rediffSlotChroma' type string, distinct from 'rediffSlot'"
T_6QVF_ROUNDTRIP="chroma round-trips, equals itself, and differs from every sibling (default:false trap closed)"
T_6QVF_OLDBINARY="what an OLDER binary sees: an unknown bare type is DROPPED, never mistaken for a known one"
T_6QVF_THREE_MARKERS="isWidthOwnership is true for ALL THREE bare slot markers, false for every presence anchor"
T_6QVF_NEQ_SIBLINGS="rediffSlot != every other AnchorRef case (incl. the sibling spliceSlot)"
T_6QVF_E2E_CHROMA="byte-fail (disjoint bytes, re-encode shape) falls back to chroma, which behaves exactly as pre-xsdz.57"
T_6QVF_E2E_REMOTE="non-file A-side sourceURL disables the byte path even with a staged B file (chroma fallback)"
T_6QVF_E2E_BYTE="byte-success: byte aligner sets width; chroma differ (PCM fetch) never invoked; no fingerprint stream needed"
T_6QVF_E2E_ANCHORS="hdgk: a byte-rediff width-owned span persists 'rediffByteExact' on BOTH ad_windows edges through the REAL runBackfill wiring"
T_6QVF_OWNERSHIP_E2E="flag ON + provider + aligned A/B: the ad span is widened to the rediff slot with .rediffSlotChroma"
T_6QVF_FLOOR_CHROMA="NOT EXEMPT: a rediff CHROMA span below the 0.9 floor demotes, where the byte arm is spared"
T_6QVF_POSTROLL_CHROMA="STILL GUARDED: a tail whose width is not byte-derived demotes to markOnly"

# playhead-nqey — the ENABLEMENT. These read `AdDetectionConfig.default`, which
# is what makes them sensitive to the shipped VALUES and not merely to the
# guard's mechanics (the distinction the sik9 names above cannot make).
T_NQEY_SHIPPED_ON="the shipped AdDetectionConfig.default has the certainty-tiered gate ON at 0.9 / 90.0"
T_NQEY_INIT_MATCHES="the AdDetectionConfig init default matches the shipped .default"
T_NQEY_FLOOR_DEMOTES="NEWLY DEMOTED: a 0.70 non-rediff host-read span is eligible OFF and markOnly at the shipped default"
T_NQEY_TAIL_DEMOTES="NEWLY DEMOTED: an unanchored tail inside the 90s window is eligible OFF and markOnly at the shipped default"
T_NQEY_AT_FLOOR_KEPT="STILL ELIGIBLE: a non-rediff span AT the 0.9 floor keeps auto-skipping at the shipped default"
T_NQEY_REDIFF_TAIL_KEPT="STILL ELIGIBLE: a rediff-anchored tail inside the 90s window is exempt at the shipped default"
T_NQEY_REDIFF_FLOOR_KEPT="STILL ELIGIBLE: a rediff-anchored span below the 0.9 floor is exempt at the shipped default"
T_NQEY_SUBSET="the shipped flip demotes a strict non-empty subset — neither inert nor total"
T_NQEY_BARE_INERT="a bare FusionWeightConfig stays OFF — the flip does not leak to the hot path or the aggregator"
T_NQEY_UNKNOWN_INERT="unknown episode duration keeps the shipped guard inert"
T_NQEY_WIREIN_DEFAULT="AdDetectionConfig.default ships the certainty-tiered gate ON at the calibrated floor + guard"
T_NQEY_WIREIN_BYTEID="Omitted wraj flags: runBackfill is byte-identical to explicit-default (true/0.9/90.0) flags"
T_NQEY_WIREIN_OMITTED="AdDetectionConfig.init defaults the three certainty-tiered fields when omitted"
T_NQEY_WIREIN_VERBATIM="AdDetectionConfig.init carries each certainty-tiered field through verbatim, one at a time"

# playhead-gard (I series): TRUST IS PER DETECTOR CLASS. The rails divide into
# (a) CLASSIFICATION — which detector drew a span, and which single class is
# exempt from the show's history; (b) the MIGRATION seed, the only thing
# between an upgrading user and a silent posture change; (c) the veto WEIGHT,
# keyed on the tier system 6qvf sharpened rather than a parallel scale; (d) the
# DEMOTION that must still happen, because per-detector trust that never
# demotes is as broken as one global scalar; (e) the ESCAPE from `manual`,
# which had no production caller at all before this bead; and (f) the WIRING —
# the gate, the veto attribution and the banner-confirm credit — which no test
# of the pure types can see.
T_GARD_EXEMPT_SINGULAR="Exactly one class is exempt from the show's trust history"
T_GARD_ONE_EDGE="ONE byte-exact edge is not the deterministic class — a span is worth its weaker edge"
T_GARD_STINGER="A stinger-snapped pair is corroborated, not deterministic — it is not the rediff class"
T_GARD_UNKNOWN_BOUNDARY="An unrecognised boundary state falls to fusion, never to a weaker gate"
T_GARD_ROW_CLASSIFY="AdWindow classifies from its own persisted columns"
T_GARD_WEIGHT_ORDER="Weights are ordered by the certainty of what was skipped"
T_GARD_FIELD_ARITHMETIC="Three unanchored vetoes weigh 1.5 — under the demotion threshold of 2"
T_GARD_MIGRATION_KEEPS="A pre-gard row seeds every show-governed class from the legacy scalar"
T_GARD_MIGRATION_FREES="The exempt class seeds CLEAN — it does not inherit other detectors' mistakes"
T_GARD_ROUNDTRIP="The column survives a store round-trip"
T_GARD_FIELD_ROW="THE BEAD: the device row still auto-skips byte-exact rediff while the aggregator stays manual"
T_GARD_STORED_WINS="A stored per-detector entry beats the legacy seed"
T_GARD_LOOKUP_FAILURE="A profile READ failure lands every class on shadow, exemption included"
T_GARD_NEW_SHOW="A show with no profile is the deliberate new-show default, and the exempt class still resolves"
T_GARD_DEMOTION_HAPPENS="THE NEGATIVE: per-detector trust that never demotes is as broken as one scalar"
T_GARD_BLAME_NOT_SHARED="The blamed detector is demoted and the others are NOT"
T_GARD_EXEMPT_DEMOTABLE="The EXEMPT class is exempt from the show's history, not from its own"
T_GARD_WEIGHTED_STAYS="THE FIELD CASE: three unanchored aggregator vetoes no longer demote"
T_GARD_LEGACY_ONCE="The LEGACY triple moves exactly once per gesture, however many classes were blamed"
T_GARD_STRONGEST_TIER="A duplicate class in one gesture is charged ONCE, at its strongest tier"
T_GARD_WEAK_HALVED="An inferred revert weighs half an explicit one (the fidelity ladder)"
T_GARD_DOOR_OPENS="THE DOOR OPENS: correct observations walk the device row back to auto"
T_GARD_DECAY_ONE="Each correct observation decays exactly one unit of false-signal evidence"

# playhead-mptr (K2 series): the artifact-backed shard skip. The whole point is
# that a skip needs TWO independent facts — a watermark that reached past the
# shard AND a persisted chunk that backs it — so the two rails are asserted
# separately and in both directions.
T_MPTR_WATERMARK_RAIL="a shard past the watermark never counts as transcribed, even where chunks exist"
T_MPTR_ARTIFACT_RAIL="H3 counterexample: watermark reaches past the shard but NO chunk backs it"
T_MPTR_TOUCHING_MERGE="exactly touching ranges merge — ASR segments abut all the time"
T_MPTR_DEGENERATE_DROPPED="degenerate and non-finite ranges are dropped, not merged"
T_MPTR_HALF_OPEN_END="touching at a boundary is not an overlap — the ranges are half-open"
T_MPTR_HALF_OPEN_START="touching at a boundary is not an overlap — the ranges are half-open"
T_MPTR_PASS_FILTER="a final-pass chunk is not reported as fast coverage"
T_MPTR_SQL_DEGENERATE="a degenerate chunk covers no time and is excluded"
T_MPTR_WATERMARK_INCLUSIVE="a shard ending exactly at the watermark counts, one second past does not"
T_MPTR_UNCOVERED_FIRST="the D9B513CD shape: the unread tail is ordered ahead of the covered prefix"
T_GARD_CREDIT_NOT_SHARED="Credit goes to the observed detector only"
T_GARD_OVERRIDE_CLEARS="An explicit user override clears the stale evidence against every detector"
T_GARD_BANNER_CREDITS="A confirmed banner IS a correct observation, credited to the detector that drew the span"
T_GARD_CALL_SITE="The banner-confirm path calls recordCorrectObservation in PRODUCTION source"
T_GARD_REDIFF_SKIPS="THE ACCEPTANCE: the demoted show auto-skips byte-exact rediff"
T_GARD_AGG_BLOCKED="…and the aggregator that earned the demotion still does NOT skip"
T_GARD_SESSION_OVERRIDE="A session override governs EVERY detector, exemption included"
T_GARD_REVERT_ATTRIB="A veto is attributed to the detector that DREW the span, through the real orchestrator seam"

# playhead-9v09 (H series): the census's SILENT RETRACTION PATH. The rails
# divide into (a) the two WRITES the retroactive sweep now performs — the
# per-window stamp and the durable row — which only a behavioural run through
# the preload door and then `setEpisodeDuration` can see; (b) the REASON, which
# travels on both and is what separates four different bugs; (c) the NEGATIVE,
# an outcome that fires when nothing was retired being exactly as useless as one
# that never fires; and (d) the three CLASSIFIERS the balance is derived from,
# which are pure and whose mis-keying is invisible to every behavioural
# assertion that does not read `retired=`.
T_9V09_BOTH="the census shows BOTH the arm and the retirement, with the reason"
T_9V09_NEGATIVE="a span that stays armed records NO retirement, and the sweep is still live"
T_9V09_MANAGED="a managed window swept by a declared chapter is named with THAT reason"
T_9V09_AGGREGATE="a sweep that retires two windows writes ONE row that aggregates them"
T_9V09_BALANCE="armed minus retired is what the listener could have seen"
T_9V09_LIFETIME="endEpisode clears the retirement stamp and keeps the count"
T_9V09_ONE_RETRACTION="exactly one outcome is a retraction"
T_9V09_NOT_BOTH="a retraction is neither a delivery nor a door outcome"
T_9V09_RENDER="a retraction row renders retired= and a delivery row does not"
T_9V09_THREE_DELIVERED="exactly three outcomes count as delivered"

# playhead-hx6n — scan-row run attribution (T series).
T_HX6N_NIL_BUCKET="NEGATIVE: a nil scene phase buckets as unattributed, never as a phase"
# SINGLE-quoted: the display name contains backticks, and in a double-quoted
# bash string those are command substitution — the shell ran `.unknown` at load
# time, printed "command not found", and substituted the empty string, leaving
# an expectation that could never match a real test name.
T_HX6N_UNKNOWN_BUCKET='NEGATIVE: a recorded `.unknown` phase buckets as unattributed'
T_HX6N_MIXED_CORPUS="NEGATIVE: unattributed rows are counted apart and never poison the foreground ratio"
T_HX6N_ALL_UNATTRIBUTED="NEGATIVE: an all-unattributed corpus yields no foreground measurement at all"
T_HX6N_V41_SURVIVES="V42: a v41 row survives the migration and stays unattributed forever"
T_HX6N_STORE_STAMPS="V42: the store stamps createdAt when a writer supplies none — and stamps nothing else"
T_HX6N_EMPTY_CORPUS="an empty corpus reports nil, not zero and not one"
T_HX6N_INELIGIBLE="throughput excludes no-work sentinels, failures and zero-width windows"
T_HX6N_SQL_AGREES="the SQL split and the Swift split agree on one fixture"
T_HX6N_RUNNER_STAMPS="every row the runner persists carries attribution, and the id resolves to a real job"
T_HX6N_FOREGROUND_RUN="a foreground run lands on the foreground side of the same split"
T_HX6N_BROKEN_PROVIDER="a provider that breaks the vocabulary yields unattributed rows, not guessed ones"
T_HX6N_LADDER_RAIL="Cycle 4 H1 RAIL: the isolated ladder does NOT run createTables()"

# playhead-bllt — THE HOT PATH'S UNANCHORED ROWS (BL series). The names divide
# into (a) the exhaustive monotonicity claims, which are the safety argument and
# are the only rails that can see a widening in a cell nobody wrote a test for;
# (b) the two SHIPPING-config integration rails, one per producer, which are the
# only rails that can see the gate being computed and then not used; and (c) the
# two PRESENCE rails, which run with the block held OFF and are what proves the
# kill switch still kills.
T_BLLT_MONOTONE="no input becomes more admissible — the whole domain, every cell"
T_BLLT_ONLY_TRANSITION="the ONLY transition is autoSkip → markOnly on an unanchored extent with the block on"
T_BLLT_COUNT="exactly 5 of the 180 cells change, and every one of them is a demotion"
T_BLLT_NOT_A_DROP="a non-nil label never becomes nil — the gate never DROPS a row"
T_BLLT_KILL_SWITCH="the block OFF is a no-op — the kill switch actually kills"
T_BLLT_DETAIL="the census detail fires only when it discriminates, and names the edges"

# playhead-ug9m — THE DAY-0 RESCUE. The dangerous direction is PROMOTION: a
# day-0 row persisted `eligible` auto-skips with nothing downstream to catch it
# (bllt gates `runHotPath`/`runSegmentAggregation`, 2350 gates the fusion path,
# and the day-0 mint passes through none of them). So the claims below are
# weighted toward the negatives, and the centrepiece — a 9s6q segment-recovered
# slot is never promoted — is asserted at three tiers so no single loosening can
# take all three out.
T_UG9M_RESCUABLE="an asset with an ANCHORED day-0 row is never rescued — its unanchored siblings are 9s6q-recovered"
T_UG9M_SUPERSEDE_NARROW="only a degraded, unsettled, day-0 row is supersedable"
T_UG9M_HALF_ANCHORED="a HALF-anchored row is degraded, not anchored"
T_UG9M_VETOED_ANCHORED="a VETOED but ANCHORED row still counts as anchored, and still blocks the rescue"
T_UG9M_VETO="a STRICT re-mint does not supersede a user-VETOED day-0 row"
T_UG9M_FOREIGN_PRODUCER="a STRICT re-mint does not supersede another producer's window"
T_UG9M_ANCHORED_ROW="a STRICT re-mint does not supersede an already-ANCHORED day-0 row"

# playhead-pggn — the score describes the span the segment reports.
T_PGGN_WORKED="playhead-pggn worked example: a 0.62 slot with a 0.20 lead-out reports 0.62 over 30 s, not 0.41"
T_PGGN_TAIL_VALUE="playhead-pggn: the reported score does not depend on the VALUE of a sub-continuation tail"
T_PGGN_TAIL_WIDTH="playhead-pggn: the reported score does not depend on the WIDTH of a sub-continuation tail"
T_PGGN_NEVER_LOWERS="playhead-pggn: a sub-continuation tail can never LOWER a promoted segment's score"
T_PGGN_GAP_CLOSE="playhead-pggn: closing a segment on an unbridgeable gap drops the pending tail rather than scoring it"
T_PGGN_FOLD_LATER="playhead-pggn: a below-continuation window the extent LATER covers is scored, not discarded"
T_PGGN_ALREADY_INSIDE="playhead-pggn: a below-continuation window already covered by the extent is scored immediately"
T_PGGN_PERSISTED="playhead-pggn: a quiet lead-out does not dilute the confidence persisted on the row it is outside of"
T_PGGN_BLLT="playhead-pggn × playhead-bllt: an undiluted 0.62 aggregator row still persists markOnly under the shipped extent gate"
T_UG9M_RECOVERED_NOT_PROMOTED="a SEGMENT-RECOVERED re-mint supersedes nothing and promotes nothing"
T_UG9M_STRICT_SUPERSEDES="a STRICT re-mint supersedes its own degraded day-0 row and persists anchors + eligible"
T_UG9M_TERMINAL_IN_GEN="a marked exit in the CURRENT generation is still terminal, degraded marks or not"
T_UG9M_EXACTLY_ONE="a trapped asset spends EXACTLY ONE rescue across repeated plays"
T_UG9M_NAMED="a spent rescue suppresses as rescueExhausted, not as silence"
T_UG9M_CEILING="the ceiling holds even when the rescue ended in a RETRYABLE exit"
T_UG9M_ADVANCE="advance counts a rescue only for a marked prior that spent bytes"
T_UG9M_CONTEXT="fetchDayZeroAttemptContext returns the record AND the census together"
T_UG9M_TRIGGER_REFETCH="a trapped asset's rescue reaches the k-way fetch"
# playhead-ug9m: added AFTER UG13 survived. The three rails above hand the
# trigger a hand-built context, so none of them can observe the STORE read at
# all — a `fetchDayZeroAttemptContext` returning an empty census left every one
# of them green while no trapped asset would ever be rescued on a device.
T_UG9M_STORE_WIRING="the rescue decision reads the census from the STORE, not from a fixture"
T_UG9M_TRIGGER_STOPS="an anchored sibling stops the trigger before any bytes are spent"
T_UG9M_TRIGGER_NAMED="a spent rescue refuses at the trigger and NAMES itself"
T_UG9M_FREEZE_REPORT="fetchDayZeroMarkFreezeReports names rescuable, frozen and anchored assets"
T_BLLT_ANCHORED="an anchored hot-path row keeps its autoSkip verdict — the gate reads the extent, not the producer (playhead-bllt)"
T_BLLT_SINGLE_SHIPPED="single-window fast path: the same 0.85 + strong-lexical window persists markOnly at the SHIPPING config — its edges are invented (playhead-bllt)"
T_BLLT_AGG_SHIPPED="hot path: the same slot+lexical segment persists markOnly at the SHIPPING config — its edges are invented (playhead-bllt)"
T_BLLT_SINGLE_PRESENCE="single-window fast path: 0.85 window with ≥1 safety signal reaches the autoSkip PRESENCE verdict and is auto-skipped (extent block off)"
T_BLLT_AGG_PRESENCE="hot path: ≥0.55 score in pre-roll WITH a non-slot signal (sponsor/promoCode/URL lexical) reaches the autoSkip PRESENCE verdict (extent block off)"

# playhead-kkzu — a background download carries the show it belongs to. NOTE:
# these strings are the @Test DISPLAY NAMES verbatim, not descriptions of the
# claim — the runner matches them against what actually ran, and a paraphrase
# reports "an expectation names a test that never ran" at the baseline. The
# dangerous direction here is SILENCE: a dropped identity does not fail, it
# writes NULL, and `AnalysisWorkScheduler` turns that into `?? ""` so every
# unattributed episode pools under one fake show. So the claims are weighted
# toward "the value that arrived is THIS show" rather than "a value arrived".
T_KKZU_ENQUEUE="A background download enqueues analysis against the show it was queued with"
T_KKZU_RESTART="A completion delivered to a NEW manager instance still carries the show"
T_KKZU_REAP="Queueing writes the attribution and a terminal completion reaps it"
T_KKZU_RESUME_KEEPS="Dropping the resume blob leaves the attribution for the resume path"
T_KKZU_FORCE_QUIT="A force-quit resume re-queues the fresh download against the recovered show"
T_KKZU_CANCEL="Cancelling a download reaps its attribution"
T_KKZU_NAMED="A null show identity always carries the reason it is null"
T_KKZU_BLANK="An empty or non-canonical identifier is a named absence, not a key"
T_KKZU_PLAYED="The played path's context is byte-identical under the new constructor"
T_KKZU_AUTO="An auto-download carries the show it belongs to"
T_KKZU_PREPARE="cellular + policy on: proceeds with download"
T_KKZU_CLEAR="Clearing the cache takes the attribution with it"

# playhead-kanf (KN series). The claim is a promotion AND a refusal, and the
# refusal is the one worth rails: "a leased job must not be promoted" is the
# correctness boundary the bead draws, and a fix that promotes everything
# passes every promotion rail. Note which direction is silent here — a
# promotion that does not happen writes nothing and fails nothing, which is
# exactly how the original defect survived being documented as intended.
# Verbatim @Test display names, not paraphrases of the claim.
T_KANF_PROMOTES="a queued, unleased row is promoted to the .now floor"
T_KANF_NUDGE="promotion touches priority and updatedAt only — never the lifecycle columns"
T_KANF_SOON="an explicit-download row at priority 10 is still promoted to the .now floor"
T_KANF_LEASED="a LEASED row is not promoted, while an unleased twin in the same store is"
T_KANF_RUNNING="a RUNNING row is not promoted, while a queued twin in the same store is"
T_KANF_PRODLEASE="a row leased through acquireLease — the production shape — is not promoted"
T_KANF_PAUSED="a paused row is not promoted (one promotable state, deliberately)"
T_KANF_TERMINAL="a complete row is not promoted"
T_KANF_ATFLOOR="a row already at the floor reports alreadyPromoted and is left untouched"
T_KANF_RUNATFLOOR="a RUNNING row already at the floor reports alreadyPromoted, not notPromotable"
T_KANF_NOROW="an unoccupied work key reports noRow and creates nothing"
T_KANF_SCOPED="promotion is keyed on workKey alone and moves no other row"
T_KANF_TAP="a tap on an ALREADY-queued episode promotes that job to the .now lane"
T_KANF_IDENT="promotion is a selection nudge: createdAt and the routing pair survive it"
T_KANF_TAP_LEASED="a LEASED job is not promoted, while a queued episode tapped in the same breath is"
T_KANF_TAP_RUNNING="a RUNNING job with no live lease is not promoted either"
T_KANF_FLAG_KEPT="a refused promotion RETAINS the flag, so the next enqueue still honours it"
T_KANF_FLAG_SPENT="a SERVED promotion consumes the flag — a later auto enqueue is not promoted"
T_KANF_RETAP="re-tapping an already-promoted job is idempotent and consumes the flag"
T_KANF_NO_INTENT="a plain auto enqueue over an existing queued row leaves its priority alone"

# playhead-5n8k (TS series). The coarse-pass diagnostic observer's scoping.
# All four live in CoarsePassDiagnosticTaskScopingTests, which is deliberately
# NOT `.serialized` — it creates the collision on purpose.
T_5N8K_FOREIGN="an unobserved coarse pass in another task tree cannot reach a bound capture box"
T_5N8K_CONCURRENT="two concurrent bindings each receive only their own windows"
T_5N8K_SCOPE="a binding does not outlive its scope"
T_5N8K_NESTED="a nested binding shadows the outer one and then restores it"
# The bd-34e test whose FAST failure was the tax this bead removes. It is the
# consumer's-eye view: if the observer stops delivering, this is what breaks.
T_5N8K_BD34E="coarse pass window submission and guardrail violation emit bd-34e diagnostics"

# playhead-26od (CK series). The mid-flight coarse checkpoint: the rails that
# make a killed pass's screened windows durable WITHOUT letting the resume
# cursor claim audio no persisted row covers.
T_26OD_PROD_SHAPE="the production runner shape checkpoints too — both coarse call sites carry the observer"
T_26OD_MIDLOOP_DEFUSE="a defuse that lands mid-loop stops the checkpoint at that window"
T_26OD_LATE_DEFUSE="a defuse that lands after the last row still withholds the cursor"
T_26OD_WRITE_COST="a pass writes one row per window per checkpoint, not the whole prefix"
T_26OD_EMPTY_REFS="an outcome with no line refs credits no plan"
T_26OD_AMBIGUOUS_REFS="an outcome matching two plans credits neither"
T_26OD_UNIQUE_REFS="a strict subset of exactly one plan still credits that plan"
T_26OD_LEASE_GUARD="a defused lease touch cannot resurrect a deferred job — and a live one still leases"
T_26OD_WIRED_PREFIX="the checkpoint's cursor is the contiguous prefix, not the furthest window banked"
T_26OD_NO_REGRESS="a checkpoint never lowers a cursor an earlier run already reached"
T_26OD_DISCRIMINATORS="a checkpointed row carries the job's own runMode and phase, not a constant"
T_26OD_FAILED_WRITE="a checkpoint whose write failed does not advance the resume cursor"
# The XCTest source canary. The battery reads `Test Case '-[Suite method]'` lines, so an
# XCTest expectation is written Suite/method.
# NO module prefix: `extract_failures` captures suite and method out of
# `Test Case '-[PlayheadTests.Suite method]'` and joins them as `Suite/method`.
T_26OD_LEASE_CANARY="FMUnboundedCallCanaryTests/testRunnerPassesAProgressObserverToEveryCoarsePass"

# ---- playhead-fil5: the durable semantic-scan claim (SC series) ----
# Verbatim `@Test(...)` display names. The field is split on ';', so none of
# these may contain one.
T_FIL5_UNMEASURED="an unmeasured ad scan is owed a scan"
T_FIL5_CLAIM_FLOOR="the claim floor is exactly the runner's skip floor"
T_FIL5_TX_FLOOR="the transcript gate uses the finalize floor, which admits 95%"
T_FIL5_TX_UNMEASURABLE="an unmeasurable transcript never clears the floor"
T_FIL5_ID="the claim's id is the runner's fullCoverage job id"
T_FIL5_CANONICAL="the persisted-chunk version matches what runBackfill atomizes"
T_FIL5_HASH="the standalone version hash equals atomize's"
T_FIL5_ROW="the claim row is resumable, unbudgeted, and names its gate"
T_FIL5_EMPTY_POD="an empty podcastId persists as nil, not as an empty-string podcast"
T_FIL5_MINT="a bail mints one durable, resumable, named row"
T_FIL5_IDEMPOTENT="repeated bails refresh one row and never charge a retry"
T_FIL5_NOT_OWED="no claim when the measured ad scan already clears the floor"
T_FIL5_SHORT_MINTS="a short measured ad scan still mints"
T_FIL5_COMPLETE="a complete row is left alone"
T_FIL5_IN_FLIGHT="running and failed rows keep their own state and reason"
T_FIL5_REFUSAL="a store refusal is reported, not thrown"
T_FIL5_GATE_FM="an FM-unavailable bail leaves a durable claim"
T_FIL5_GATE_OFF="an fm-mode-off bail leaves a durable claim"
T_FIL5_GATE_POD="a missing-podcastId bail leaves a claim with no podcast, not an empty one"
T_FIL5_GATE_FACTORY="a missing-runner-factory bail leaves a durable claim"
T_FIL5_OPEN_GATE="an open gate mints no claim — the runner's own row does the work"
T_FIL5_REACHED="a zero-row transcribed asset gets a claim AND a re-drive in one pass"
T_FIL5_ONCE="repeated reconciles mint exactly one claim, ever"
T_FIL5_SHORT_TX="an asset short of the transcript finalize floor gets no claim"
T_FIL5_SCANNED="an already-scanned asset gets no claim"
T_FIL5_EXISTING_ROW="an asset that already has a coverage-lane row gets no claim"
T_FIL5_LEDGER="claims are excluded from recoveredWorkCount"
T_FIL5_ANY_ROW="any coverage-lane row excludes the asset, whatever its status"
T_FIL5_NO_TX="an asset with no transcript is not a candidate"
T_FIL5_ORDER="candidates come back oldest-first and respect the limit"
T_FIL5_ONN6_LEDGER="minted re-drives count as recovered work in the background ledger"
T_FIL5_STORED_POD="an empty podcastId reaches the database as NULL"
# R1 review round. Two recording sites and one numerator that the first
# registration left unmutated.
T_FIL5_GATE_OFF_RETRY="a mode-off bail on the shadow-RETRY path leaves a durable claim"
T_FIL5_GAPPY="a GAPPY transcript whose watermark reads 100% still gets no claim"
# playhead-fil5 R2 — the two per-pass bounds and the in-flight guard.
T_FIL5_MINT_CAP="one pass mints at most the cap, and the rest drain next launch"
T_FIL5_WINDOW="a claimable asset beyond the candidate window waits for the next pass"
T_FIL5_IN_FLIGHT_EP="an episode with an analysis pass in flight gets no claim"

# playhead-fil5 R3 review round.
T_FIL5_BRIDGED="a complete transcript reads under the floor RAW and over it bridged"
T_FIL5_BLOCKS="bridging closes breaths, not blocks"
T_FIL5_BREATH="an asset transcribed in real speech runs — breaths, not blocks — is claimed"
T_FIL5_ROTATE="a claimable asset behind a full window of permanent rejects is reached next pass"

# playhead-fil5 R4 review round.
T_FIL5_WRAP="a full window of claimable assets drains without an idle pass"

# playhead-9y9e — the RT series.
T_9Y9E_REACHES="a pass that adds nothing to a complete transcript runs ad detection"
T_9Y9E_PARTIAL="a half-transcribed asset still fails, and still journals the failure"
T_9Y9E_GAPPY="a full watermark over a gappy transcript does not license the short-circuit"
T_9Y9E_NOCHUNKS="a watermark with no chunks behind it does not license the short-circuit"
T_9Y9E_INTERRUPT="an interrupted pass does not short-circuit, even on a complete transcript"
T_9Y9E_FINALONLY="a scan window inside FINAL-pass-only transcript is counted, not discarded"
T_9Y9E_CEILING="a fully scanned, mostly-final-pass episode can now reach the sufficiency floor"
T_9Y9E_CLIPS="widening the bound to both passes does not stop it clipping untranscribed audio"
T_9Y9E_RANGES="fetchTranscriptCoveredRanges spans both passes and drops degenerate rows"
T_9Y9E_UNSCANNED="a fully transcribed episode whose ad scan is short still gets a retry"
# R1 review additions.
T_9Y9E_MONO="the widened ad-scan bound never measures LESS than the watermark fallback did"
T_9Y9E_DEGEN="an all-degenerate final pass reports the watermark, not a zero-width row"
T_9Y9E_REACHGUARD="a final-pass reach past the duration withholds the fraction too"
# R3 review additions — the reconciler WIRE-IN, tested through reconcile().
T_9Y9E_WIRE_MINT="reconcile mints the cap-out retry for a transcribed but unscanned episode"
T_9Y9E_WIRE_DECLINE="reconcile mints nothing once the ad scan clears its floor"
T_9Y9E_FINALSRC="a final-pass WATERMARK with no chunks behind it never withholds the fraction"
T_9Y9E_LADDER="an outstanding transcript rung still wins over the ad-scan term"
T_9Y9E_GUARDS="the ad-scan term does not bypass the terminal, budget or cooldown guards"
# The vacuity control's victims. `pz32`'s own rails, which must stay green under
# RT12 and red under RT07 — see the batch note.
T_9Y9E_PZ32_SPRAWL="ad-scan fraction can never exceed 1.0 however far the windows sprawl"

# playhead-41mu — the UC series. The coverage-lane terminal.
T_41MU_DEFERS="THE 53FC53E3 CASE — a pass that swept every segment it was handed and still read 1/10th of the episode DEFERS, it does not complete"
T_41MU_NOCURSOR="the under-coverage defer does NOT publish a cursor over audio the job never held — the 2,525.82-on-a-2,528 s-episode shape"
T_41MU_CURSOR="THE AD5F3A0A CASE — when the run DID start at the head, the under-coverage defer publishes the honest cursor so the resume scans only the remainder"
T_41MU_REDRIVE="THE CONSEQUENCE — a non-completing terminal leaves resumable work, so the ad-scan re-drive can mint where the old \`complete\` left ZERO and blocked it"
T_41MU_VACUITY="VACUITY CONTROL — a pass that genuinely reads the episode still COMPLETES, with its full-coverage cursor"
T_41MU_BOUNDED="the deferral is BOUNDED — an episode that keeps coming back under-covered terminates with a named cause instead of re-driving forever"
T_41MU_FLOOR="the floor is the pipeline's floor, and the comparison is strict at the boundary"
T_41MU_SCOPE="ONLY the phase that claims the whole episode is judged against an episode-wide floor"
T_41MU_ABSENCE="not-measurable and un-readable are different facts with opposite answers"
T_41MU_BUDGET="the attempt budget is the shared one, and it terminates rather than deferring forever"
T_41MU_HOLE="a cursor is an assertion about the EPISODE, so a hole at the head freezes it"
T_41MU_PREFIX="a run that starts at the head publishes the honest bound, and leading silence is not a hole"
T_41MU_BRIDGE="the hole threshold is the coverage reader's own bridge tolerance, not a fresh constant"
T_41MU_NONFINITE="R1 — a non-finite MEASUREMENT is an absence, and under-claims like every other reader of it"
T_41MU_RESUMEHOLE="R2 — the head test measures the RESUME's own first plan, so a hole immediately above the prior cursor freezes it on attempt TWO as well as attempt one"

# playhead-iu0t — the CN series. What the shadow-retry drain REPLAYS.
T_IU0T_REACH="the drain screens the whole transcript, not just the candidate-local final tail"
T_IU0T_VERSION="scan rows are stamped with the canonical transcriptVersion, not a drain-private one"
T_IU0T_RAW="the replay is the canonical set, not the raw persisted rows"
T_IU0T_JOBID="the drain mints the job id the runBackfill path would mint"
T_IU0T_CLAIM="a mode-off bail on the drain claims under the canonical job id"
T_IU0T_ALLFAST="an all-fast transcript replays byte-identically to the pre-fix fallback"
T_IU0T_FIXTURE="control: the fixture's four candidate chunk sets are all genuinely different"
# The pre-iu0t suite's own drain rails. Listed as victims for the mutants that
# stop the drain running at all, so "did it replay the right set" is judged
# separately from "did it replay anything".
T_IU0T_FASTFALLBACK="Bug 9-A: shadow retry succeeds with only pass='fast' chunks"
T_IU0T_MIXEDDRAIN="Bug 9-A: shadow retry drains a mixed-pass asset (fully-overlapped final)"
T_IU0T_RETRYRUNS="Test B: retry path runs only the shadow phase and clears the flag"
# R1 — the rule canary. XCTest, so `Suite/method`.
T_IU0T_RULE="TranscriptCanonicalizationRuleCanaryTests/testEveryTranscriptVersionCallSiteCanonicalizes"
T_IU0T_SHAPE="TranscriptCanonicalizationRuleCanaryTests/testThePreHc7eFinalOnlyCollapseAppearsNowhereInProduction"
# R2 — the THIRD sink: the persisted `transcript_chunks.transcriptVersion`
# column, which no call-site walk can see because there is no call.
T_IU0T_COLUMN="TranscriptCanonicalizationRuleCanaryTests/testNoProductionConsumerReadsThePersistedChunkTranscriptVersion"
# R5 — the walk's own rails. Synthetic source, so they are the tests that go red
# when the SITE FINDER or the canonicalization predicate regresses, as opposed to
# when the tree does.
T_IU0T_SPLITLINE="TranscriptCanonicalizationRuleCanaryTests/testTheSiteFinderSeesACallSplitAcrossLines"
T_IU0T_LAUNDER="TranscriptCanonicalizationRuleCanaryTests/testCanonicalizingAndThenCollapsingIsNotCanonicalized"

# --- playhead-e75l: the daemon-refusal rails (DR series) -------------------
T_DR_DEFERS="a metadata-deadline timeout in the PROLOGUE defers the job — it must not mark it failed"
T_DR_COST="THE COST, DIRECTLY: three metadata stalls must not disqualify an episode forever"
T_DR_CAUSE="a metadata stall records a NAMED cause, not the error's Swift description"
# NOTE: no ';' in this display name. The MUTATIONS record separator is ';', so a
# test whose name contains one can never be matched and every mutation naming it
# reports ERROR while having killed it perfectly (observed here on DR02/DR03).
T_DR_BUDGET="a metadata-deadline timeout is a refusal — a standard-deadline one is not"
T_DR_STANDARD="DISCRIMINATOR: a STANDARD-deadline timeout through the same seam still fails and burns a retry"
T_DR_ONECOUNTER="ONE counter: a throttle then a metadata stall stops the drain, and the sibling names the stall"
T_DR_BATCH="a stalled batch leaves no job stranded in queued and none marked failed"
# The wiring canary. XCTest, so `Suite/method`.
T_DR_WIRING="FMDaemonRefusalSourceCanaryTests/testDaemonRefusalLogEventsAreNotHardCodedInTheRunner"
# R2 review: the durable token must not join a FOREIGN prefix family. XCTest —
# it is a source canary because the foreign token is a bare literal in the
# runner, so the rule has to be re-derived if playhead-8d5r's spelling moves.
T_DR_FAMILY="FMDaemonRefusalSourceCanaryTests/testDaemonRefusalCausesDoNotJoinAForeignTokenFamily"
# R4 review: the budget-enumeration canary. XCTest. It is the rail holding
# `isMetadataStall`'s soundness — "no other budget reaching
# `FMInferenceDeadline.run` is 30 s" — and until R4 it asked its question one
# LINE at a time, which four different spellings walked past.
T_DR_BUDGETSITES="FMDaemonRefusalSourceCanaryTests/testEveryProductionInferenceBudgetIsEnumerated"

MUTATIONS=(
  "M05|1|ORCH|$T_ANON_RACE"
  "M07|1|ORCH|$T_LISTEN_RACE"
  "M11|1|ORCH|$T_LISTEN_FP"

  "M12|2|ORCH|$T_LISTEN_RACE"
  "M09|2|ORCH|$T_REVERTWINDOW_FP"
  "M20|2|ORCH|$T_CONFIRM_SILENT"

  # M08 deliberately does NOT share a batch with M13: both rewrite the
  # `if revertedManagedAny { … }` block, so whichever lands first destroys the
  # other's anchor.
  "M13|3|ORCH|$T_LISTEN_RACE;$T_MANAGED_RACE"
  "M10|3|ORCH|$T_SUGGEST_NO_NOSTORE"

  "M14|4|ORCH|$T_LISTEN_RACE"
  "M08|4|ORCH|$T_SUGGEST_RACE;$T_SUGGESTONLY_SILENT"
  "M18|4|STORE|$T_OWNERSHIP"

  "M15|5|ORCH|$T_LISTEN_RACE"
  "M19|5|ORCH|$T_AUTOFADE"

  "M16|6|ORCH|$T_LISTEN_RACE"

  "M17|7|ORCH|$T_LISTEN_RACE;$T_MANAGED_RACE"

  # Tenth pass. Every one of these SURVIVED when first probed: the four race
  # tests above cover `recordListenRevert` and `revertByTimeRange`, and nothing
  # covered the other two calibrating seams under the same interleave, the
  # hard-negative bank's write-trigger census, or the empty-show-id clause.
  "N02|8|ORCH|$T_DENY_RACE"
  "N03|8|ORCH|$T_ACCEPT_RACE"
  "N04|8|ORCH|$T_CONFIRM_SILENT"
  "N06|8|ORCH|$T_ANON_SILENT"

  "N05|9|ORCH|$T_ACCEPT_RACE"
  "N07|9|ORCH|$T_ANON_SILENT"

  # playhead-ugy4. S01 gets a batch to itself: a stale Yes that can reach a
  # promotion reddens EVERY suggest-tier test whose id survives in
  # `lastSuggestRevisionByWindowId` — S04's, S05's and (measured 2026-07-28,
  # once playhead-auz3 repaired its fixture) S06's expectations. Sharing a
  # batch would credit those three off S01's blast radius.
  "S01|10|ORCH|$T_ADWINDOW_STALE_YES;$T_DECISION_STALE_YES;$T_EXPLICIT_RETIRE_STALE_YES;$T_LATE_INVENTORY_STALE_YES"

  # S02 is safe here: every accept in S04's and S05's tests goes through the
  # one-argument `acceptSuggestedSkip(windowId:)`, which forwards
  # `ifCurrentEpisodeId: activeEpisodeId`, so deleting that guard is inert for
  # them and only `episodeBoundSuggestActionsRejectStaleIdentity` moves. That
  # is a standing DEPENDENCY, not a proof: if either test is ever edited to
  # pass an episode id of its own, S02 gains a second victim and must move to
  # a batch of its own.
  "S02|11|ORCH|$T_STALE_IDENTITY"
  "S04|11|ORCH|$T_FUSION_CLEARS_SUGGEST"
  "S05|11|ORCH|$T_DECLINE_NO_CONFIRM"

  # S03 is the decline-side twin of S02 and names the same test, so it needs
  # its own batch.
  "S03|12|ORCH|$T_STALE_IDENTITY"

  # playhead-o4qr — ACCEPT THE RECEIPT, REFUSE THE LEARNING.
  #
  # Every one of these gets a batch to itself, and the reason is structural
  # rather than timid: the decided contract is ONE test's subject, so all four
  # redden `anonymousRevertRecordsNoControllerSample`. Two mutations that can
  # redden the same test in one batch is a FALSE KILL — the script would credit
  # both off a single failure — which the BATCHING note above rules out. Four
  # builds is the honest price of pinning four distinct learning surfaces
  # behind one contract.
  #
  # Note the division of labour with N07, which already mutates the CONTROLLER
  # attribution in this seam: N07 covers the threshold controller, so these
  # cover the three surfaces it cannot see.
  "O01|13|ORCH|$T_ANON_SILENT"

  # O05 rides in O02's batch for free: it edits a different seam
  # (`denyAutoSkippedBanner`, not `revertWindow`) and its only victim is the
  # deny-side test, which never calls `revertWindow`. Disjoint code, disjoint
  # victims — the two conditions the BATCHING note requires.
  "O02|14|ORCH|$T_ANON_SILENT;$T_REVERTWINDOW_VETO"
  "O05|14|ORCH|$T_STALE_SHOW_NO"

  "O03|15|ORCH|$T_ANON_SILENT"
  "O04|16|ORCH|$T_ANON_SILENT"

  # playhead-1mq1.2.1 — a revert says the BOUNDARIES were wrong, not that there
  # is no ad inside. Both mutations restore the pre-guard reading, and both
  # redden the same fixture (it asserts the bank clause and the catalog clause
  # together, because they are one defect observed twice), so they cannot share
  # a batch.
  "P01|17|ORCH|$T_MIXED_WIDTH"
  "P02|18|ORCH|$T_MIXED_WIDTH"

  # playhead-auz3. S06 and S07 share a batch, and the disjointness is checked
  # rather than assumed — both directions, because either one aliasing the
  # other's victim would print a false KILL:
  #   • S06 (delete the AdWindow-path gate-flip clear) is INERT in S07's test:
  #     that fixture's only same-id redelivery arrives in a NEW episode whose
  #     `beginEpisode` has already emptied `suggestWindows`, so the deleted
  #     call had nothing to remove.
  #   • S07 (stop clearing `recentlyAcceptedSuggestIds` at `beginEpisode`) is
  #     INERT in S06's test: it calls `beginEpisode` once, before any accept,
  #     so the set it stops clearing is empty at that point.
  # Distinct seams (`receiveAdWindows` vs `beginEpisode`), distinct victims.
  "S06|19|ORCH|$T_GATE_FLIP_CLEARS_SUGGEST"
  "S07|19|ORCH|$T_DIRECT_REPLACEMENT_CLEARS_ACCEPTED"

  # playhead-d3g0 — the suggest banner fires on PLAYHEAD ENTRY, and never
  # offers a Skip it cannot perform.
  #
  # D01 is the bead itself and reddens the whole entry suite, so it takes a
  # batch alone. Everything after it is a NARROWING of that gate, and each
  # narrowing is checked against a distinct victim.
  "D01|20|ORCH|$T_D3G0_FIELD;$T_D3G0_ENTRY;$T_D3G0_BUDGET;$T_D3G0_PREROLL;$T_D3G0_ONCE;$T_D3G0_SEEK;$T_D3G0_CATALOG"

  # D02 (inclusive→exclusive at the START edge), D03 (exclusive→inclusive at
  # the END edge) and D06 (a dwell) are the three ways to get the half-open
  # interval wrong. Their VICTIMS are disjoint — D02 kills the tests that enter
  # exactly on a boundary, D03 the only test that observes a span from outside
  # its end, D06 the worst-case-alignment test — but all three rewrite the SAME
  # `.filter` line, so whichever landed first would destroy the others'
  # anchors. A batch each, for the M08/M13 reason rather than a crediting one.
  # (Measured: sharing batch 21 made D03 ERROR "anchor did not apply".)
  "D02|21|ORCH|$T_D3G0_ENTRY;$T_D3G0_PREROLL"
  "D03|28|ORCH|$T_D3G0_PAST"

  # D04 (never disarm) and D05 (drop the replay gate) both touch the
  # once/replay contracts, and D05's victim is the replay test which D04 does
  # not reach — D04's window is disarmed-then-re-fired within one stream,
  # D05's is never disarmed at all. Separate batches anyway: D04 leaves every
  # armed window eligible forever, which is broad enough that crediting D05
  # off its blast radius is a real risk.
  "D04|22|ORCH|$T_D3G0_ONCE;$T_D3G0_SEEK"
  "D05|23|ORCH|$T_D3G0_REPLAY"

  # D06: require a DWELL before presenting. This is the latency budget as a
  # behaviour rather than a constant — the shape every "just wait a beat"
  # instinct produces, and the one Dan's decision rules out.
  "D06|24|ORCH|$T_D3G0_BUDGET"

  # D07/D08 are the affordance, and they need a batch each — MEASURED, not
  # assumed. Sharing batch 25 made D07 report SURVIVED: D08 replaces the card's
  # `confirmationWouldSkip(_:)` call with a literal, so it deletes D07's only
  # card-side reader and masks it entirely. Distinct victims were not enough;
  # one mutation ate the other's seam.
  #
  # D07 (the card always claims it will skip — the pre-affordance state) names
  # ONLY the absolute test. It cannot redden `affordanceMatchesWhatTheTapActuallyDoes`,
  # and that is the point rather than a gap: D07 flips the card AND the
  # transaction through the one shared helper, so the two agree and the anti-lie
  # test passes. A consistent lie is invisible to a consistency check. That is
  # why the suite carries both shapes.
  "D07|25|ORCH|$T_D3G0_NOSKIP"

  # D08 (the card never claims it will skip — the overshoot) leaves
  # `acceptSuggestedSkip` honest, so the card and the transaction now DISAGREE
  # on the byte-exact population and the anti-lie test does fire.
  "D08|29|ORCH|$T_D3G0_SKIP;$T_D3G0_MATCH"

  # D09 is the copy seam and lives in a different file, so it cannot collide.
  "D09|26|VIEW|$T_D3G0_COPY"

  # D10: widen the latency budget past the transport tick's meaning. The
  # constant is only honest while it is derived from the observer cadence.
  "D10|27|ORCH|$T_D3G0_LATENCY"

  # Second pass, hunting for what the first ten did NOT reach. Each of these
  # names a seam D01-D10 leave untouched: the EVENT-stream replay filter (D05
  # only mutates the item-stream twin), the same-tick emission ORDER, the
  # cross-episode clear, and the exact-replay re-arm arm of
  # `registerSuggestedWindow` (D01 only rewrites the revision-changed arm).
  # All three SURVIVED when first probed, with NOTHING in the focused set going
  # red — three real gaps, each closed by a test in
  # `SuggestBannerEntryGateSecondPassTests`. D13 is deliberately absent; see the
  # KNOWN GAP note below.
  "D11|30|ORCH|$T_D3G0_REPLAY_EVENTS"
  "D12|31|ORCH|$T_D3G0_SAME_TICK_ORDER"
  "D14|33|ORCH|$T_D3G0_EXACT_REPLAY"

  # playhead-96ot — THE DELIVERY. Three links in one chain, mutated at each
  # link: the mint reports how many rows it wrote (E09), the summary carries
  # that number rather than a neighbouring one (E08), and the trigger acts on
  # it (E05/E06/E07). Then the door itself: the asset guard (E01), the shared
  # admission rule (E02), and the promise NOT to emit at registration (E04).
  #
  # E01 and E02 share a batch — disjoint victims, and E02 edits
  # `preloadAdmissibleWindows` while E01 edits `ingestPersistedAdWindows`, so
  # neither can destroy the other's anchor. E04 also edits
  # `ingestPersistedAdWindows`, so it gets its own batch on the M08/M13 rule
  # (same function, overlapping blast radius) even though its victim is
  # distinct.
  "E01|34|ORCH|$T_96OT_INACTIVE;$T_96OT_NOEPISODE"
  "E02|34|ORCH|$T_96OT_VETOED"

  "E04|35|ORCH|$T_96OT_ARMS"

  # E05/E06/E07 all rewrite the same six-line delivery block in
  # `triggerIfEligible`, so a batch each is forced by anchor collision, not
  # only by crediting. E06 and E07 additionally name overlapping victims.
  "E05|36|TRIG|$T_96OT_UNMARKED;$T_96OT_COVERED"
  "E06|37|TRIG|$T_96OT_ONCE;$T_96OT_FIRSTLISTEN"
  "E07|38|TRIG|$T_96OT_ONCE"

  # E08 (the summary accumulates `rotated` instead of the mark count) and E09
  # (the mint's count never leaves `fetchMintAndRecord`) are the two ways to
  # break the number the delivery reads. They share a victim, so they cannot
  # share a batch.
  "E08|39|RSVC|$T_96OT_MARKCOUNT"
  "E09|40|RSVC|$T_96OT_MARKCOUNT;$T_96OT_ONCE;$T_96OT_FIRSTLISTEN"

  # playhead-djl0 — `.shadow` must never be a silent fallback for "I could not
  # look something up." Four layers, and the mutation set is organised by which
  # layer re-merges the causes: the CLASSIFIER (J02/J03), the ORCHESTRATOR's
  # branch and its bookkeeping (J01, J04-J09), the RECOVERY query (J10-J12) and
  # the SURFACE (J13-J16).
  #
  # Batch 41 is the broad one: J01 reddens most of this bead's suites at once,
  # so nothing that could be credited off its blast radius shares it. The two
  # classifier mutations join it because they live in a different file and name
  # victims J01 cannot reach — J02 flips a NON-failure into a failure (which
  # J01, being a failure-side edit, never touches) and J03 the reverse.
  "J01|41|ORCH|$T_DJL0_DISTINGUISH;$T_DJL0_COUNTED;$T_DJL0_LOGGED;$T_DJL0_FIELD"
  "J02|41|TRUST|$T_DJL0_FAILURESET;$T_DJL0_NEWSHOW"
  "J03|41|TRUST|$T_DJL0_SHOWLESS;$T_DJL0_PILL_LOCKED"

  # The remaining classifier seam: `resolveMode`'s two failure exits. Both are
  # `.shadow`, and before djl0 both were `.showTrustProfile`'s equal. Separate
  # batch from J02/J03 because all three edit the same enum/function region.
  "J04|42|TRUST|$T_DJL0_BADMODE"
  # J05-J07 are the orchestrator's bookkeeping. Victims are disjoint: the
  # counter (J05), the diagnostics record (J06), the code CHOICE (J07).
  "J05|42|ORCH|$T_DJL0_COUNTED;$T_DJL0_NOBLEED"
  "J06|42|ORCH|$T_DJL0_LOGGED;$T_DJL0_TRUSTCODE"

  # J07 rewrites the same ternary J06 deletes, so it cannot share J06's batch.
  "J07|43|ORCH|$T_DJL0_TRUSTCODE"
  # J08/J09 are the two lifecycle resets. `setActiveSkipMode` and `endEpisode`
  # are distinct functions with distinct victims.
  #
  # J08 names ONLY the orchestrator's test. It first named the view model's
  # too and SURVIVED on it — measured, and the measurement was right: the view
  # model sets its own `skipModeResolution` in `noteSkipModeSelection` and never
  # reads the orchestrator back on that path, so an orchestrator-side edit
  # cannot reach it. That is a mis-authored expectation, not a coverage hole —
  # the view-model half is a separate production value and is mutated as J17.
  "J08|43|ORCH|$T_DJL0_OVERRIDE"
  "J09|43|ORCH|$T_DJL0_NOEPISODE"

  # J10-J12: half one, the recovery. J10 and J11 both edit `recoverShowIdentity`
  # / the `beginEpisode` call around it, so they are split; J12 is the SQL and
  # cannot collide with either.
  "J10|44|ORCH|$T_DJL0_STORED_NONCANON"
  "J12|44|STORE|$T_DJL0_NULL_MASK"

  "J11|45|ORCH|$T_DJL0_CALLER_WINS"
  # J13 drops the recovery entirely — the "half one was never wired" mutant.
  # Its victims are the recovery suite's, which J11's caller-precedence victim
  # is not among.
  "J13|45|ORCH|$T_DJL0_RECOVER;$T_DJL0_RECOVER_ACTIVE;$T_DJL0_NEWEST"

  # J14-J16: the surface. Three files, three disjoint victims, no anchor
  # overlap — the cheapest honest batch in the bead.
  #
  # J14 names the two tests an "ignore the resolution" edit can actually
  # redden. It first also named the resolved-show label test and SURVIVED on
  # it — measured, and correct: J14 routes EVERY resolution down the resolved
  # branch, so a resolved show's label is exactly what it was. That constant is
  # a separate production value, mutated as J18.
  "J14|46|NPV|$T_DJL0_PILL_DISTINCT;$T_DJL0_PILL_LOCKED"
  "J15|46|NPV|$T_DJL0_PILL_VISIBLE"
  "J16|46|NPVM|$T_DJL0_VM_LOAD"
  "J18|46|NPV|$T_DJL0_PILL_LABELS"

  # J17: the view-model half of the override. Separate batch from J16 — both
  # edit `NowPlayingViewModel`'s skip-mode block, so they would eat each
  # other's anchors.
  "J17|47|NPVM|$T_DJL0_VM_OVERRIDE"

  # -------------------------------------------------------------------------
  # playhead-4dqe — day-0 rediff at DOWNLOAD time (K01-K22)
  #
  # Three claims, and one mutation family per claim: the download-time trigger
  # reaches the trigger and NAMES it when it does not (K07-K09, K13, K15);
  # background/auto downloads get the same entry point, drained serially and
  # newest-first (K10-K12, K14); and the transport is a USER SETTING that is
  # not the byte budget, that Low Data Mode outranks, and that has to reach the
  # SOCKET (K01-K06, K16-K22).
  # -------------------------------------------------------------------------

  # K01: Low Data Mode applies ONLY on WiFi. Dan's sub-decision is that it wins
  # on BOTH transports — it is the user's OS-level instruction, and an in-app
  # toggle is not consent to override it.
  "K01|48|BWPOL|$T_4DQE_LDM_CELLULAR;$T_4DQE_LDM_OUTRANKS"
  # K04: the rolling window never elapses, so yesterday's bytes bind forever.
  "K04|48|BWPOL|$T_4DQE_WINDOW_ROLLS;$T_4DQE_SPEND_RESTARTS"
  # K10: an unknown publish date sorts FIRST, so a date-less feed jumps ahead
  # of a genuine new drop when the budget is contended.
  "K10|48|KICK|$T_4DQE_UNDATED_LAST"
  # K18: a give-up is counted as a fired kickoff, which is precisely the
  # "kickoffCount large, firedCount zero" signature this ledger exists to make
  # readable.
  "K18|48|STORE|$T_4DQE_ORPHAN_ROW;$T_4DQE_COUNTS_ACCUMULATE;$T_4DQE_MIXED_HISTORY"
  # K20: the request's transport flag is hardcoded off, so an opted-in user's
  # day-0 fetch is refused by the REQUEST even though the gate allowed it.
  "K20|48|SEAMS|$T_4DQE_REQUEST_CELLULAR"

  # K02: the cellular leg ignores the setting — a hardcoded WiFi-only gate,
  # i.e. exactly what Dan moved out of code.
  "K02|49|BWPOL|$T_4DQE_CELLULAR_ALLOWED;$T_4DQE_CELLULAR_FETCHES"
  # K07: the wait blames the LAST probe rather than the furthest observed, so
  # an LRU eviction rewrites the diagnosis of a kickoff that got further.
  "K07|49|KICK|$T_4DQE_FURTHEST_PROGRESS"
  # K12: the drain is FIFO, losing Dan's newest-episode-first ordering.
  "K12|49|KCOORD|$T_4DQE_NEWEST_FIRST"
  # K21: Low Data Mode is not honored at the SOCKET, only at the gate — one
  # dropped `if` away from spending a metered user's data against an explicit
  # OS-level instruction.
  "K21|49|SEAMS|$T_4DQE_SOCKET_LDM"

  # K03: the shipping default flips to cellular-allowed, so a metered user
  # silently loses ~130 MB per episode before ever finding the toggle.
  "K03|50|ACT|$T_4DQE_DEFAULT_WIFI;$T_4DQE_UNTOUCHED_INSTALL"
  # K08: cancellation is reported as a give-up, so app teardown is
  # indistinguishable from a download that never landed.
  "K08|50|KICK|$T_4DQE_CANCELLED_OWN_OUTCOME"
  # K14: the in-flight dedupe is gone, so two play paths plus the tap each
  # spend a full k-way fetch for one episode.
  "K14|50|KCOORD|$T_4DQE_DEDUPE"
  # K16: the budget is admitted against a zero cost, so the ceiling never binds.
  "K16|50|TRIG|$T_4DQE_BUDGET_REFUSES"
  # K22: the fetcher ignores the setting and always reaches for cellular.
  "K22|50|SEAMS|$T_4DQE_FETCHER_FOLLOWS_SETTING"

  # K05: a PARTIAL attempt is admitted. Below the >=2 B-copy floor the mint
  # cannot diff at all, so this spends the last of the budget for nothing.
  "K05|51|BWPOL|$T_4DQE_FULL_ESTIMATE"
  # K09: the two give-up causes collapse onto one invariant code, which is the
  # same unattributable state the bare `return` left behind.
  "K09|51|KICK|$T_4DQE_DISTINCT_CODES;$T_4DQE_MISSING_FILE_CAUSE"
  # K15: THE BEAD'S CORE DEFECT, re-injected — the transport refusal writes
  # nothing, so "why has day-0 never fired on this phone?" is unanswerable.
  "K15|51|TRIG|$T_4DQE_CELLULAR_RECORDED;$T_4DQE_LDM_RECORDED;$T_4DQE_CELLULAR_NO_READ"
  # K19: the store's roll never fires, so the SQL and the pure policy disagree
  # about when a day ends.
  "K19|51|STORE|$T_4DQE_STORE_ROLLS"

  # K06: a zero-byte spend starts a window, so a device that only ever fails
  # silently shrinks its own daily allowance.
  "K06|52|BWPOL|$T_4DQE_ZERO_SPEND"
  # K11: the ordering loses its FIFO tiebreak and is no longer total, so the
  # drain order depends on sort stability.
  "K11|52|KICK|$T_4DQE_TOTAL_ORDER"
  # K13: the per-cause give-up counters stop counting — one indistinguishable
  # total is what playhead-djl0 established is not enough.
  "K13|52|KCOORD|$T_4DQE_PRE_EWAG_SURFACED"
  # K17: the ledger records the pre-flight ESTIMATE instead of what was
  # actually spent, so the budget bounds a number nobody measured.
  "K17|52|TRIG|$T_4DQE_REAL_COST"

  # playhead-eks2 — the pod-continuation FLIP (L01-L09).
  #
  # ONE MUTATION PER BATCH, deliberately. Six of the nine redden a test in the
  # same small suite, and the two that do not (L01, L03) have a blast radius
  # that covers the other six — a shared batch could not tell them apart.

  # L01 reverts the flip itself. It reddens the whole eks2 suite bar the
  # pure-compose day-0 test, which is why it is alone.
  "L01|53|ADSVC|$T_EKS2_SHIPPED_ON"

  # L02 desynchronises the init default from `.default`. The eks2 e2e tests
  # read `AdDetectionConfig.default.podContinuationEnabled` explicitly, so this
  # is invisible to them — which is exactly what the rail is for.
  "L02|54|ADSVC|$T_EKS2_INIT_DEFAULT"

  # L03 keeps the flag ON and kills the Step 18b call site. Distinct seam from
  # L01: a config that says yes and a pipeline that never asks.
  "L03|55|ADSVC|$T_EKS2_RECOVERS;$T_EKS2_WIREIN_COMPOSES"

  # L04 is the playhead-2350 rail: the emitted row claims auto-skip eligibility.
  # The evc1 name is here because a DAY-0 seed is the case where the mistake is
  # most tempting — the seed really is byte-exact and 1.00-confident — so the
  # "certainty does not propagate along the chain" claim needs its own witness.
  "L04|56|PODC|$T_EKS2_NO_AUTOSKIP;$T_EKS2_WIREIN_TIER;$T_EVC1_NO_AUTOSKIP"

  # L05 is the playhead-ynmk rail: the row claims byte-exact edges, so a
  # confirmation acquires an extent it never measured and cuts.
  "L05|57|PODC|$T_EKS2_CONFIRM_MARKS;$T_EKS2_NO_AUTOSKIP;$T_EKS2_WIREIN_TIER;$T_EVC1_NO_AUTOSKIP;$T_EVC1_CONFIRM_MARKS"

  # L06 widens the seed predicate to admit `.candidate` WHOLESALE. Since
  # playhead-evc1 this is no longer "the carve-out applied early" — it is the
  # WRONG carve: it admits the segment aggregator's coarse 30 s tiles, the
  # population the original exclusion was actually for and the one that went
  # 0-for-3 in the field. The three named tests are the exclusion half of evc1;
  # if this ever goes green again the carve-out has stopped being scoped on
  # provenance.
  "L06|58|PODC|$T_EVC1_OTHERS_REFUSED;$T_EVC1_TILE_SILENT;$T_EKS2_DAY0_SEED"

  # L07 turns d3g0's CONTAINMENT emit back into a start-edge TRANSITION. A
  # window armed while the playhead is already inside — which is every
  # continuation window, since the pass runs after the pod's first ad is found
  # — would then never banner at all.
  "L07|59|ORCH|$T_EKS2_ENTRY_ONCE;$T_D3G0_FIELD"

  # L08 stops disarming on emit, so a span asks again on every 0.25 s tick.
  "L08|60|ORCH|$T_EKS2_ENTRY_ONCE;$T_D3G0_ONCE"

  # L09 drops the mark confidence ceiling below `preloadConfidenceThreshold`.
  # `markConfidenceCeiling`'s doc asserts the two are the same number; without
  # that, 96ot's ingest filters every continuation row out and the banner the
  # whole bead exists for never reaches the session.
  "L09|61|PODC|$T_EKS2_ENTRY_ONCE;$T_EVC1_ENTRY_ONCE;$T_EVC1_DELIVERY"

  # playhead-evc1 — the DAY-0 SEED CARVE-OUT (V01-V06).
  #
  # ONE MUTATION PER BATCH again: they all edit the same six lines of
  # `isSeed` / `isDayZeroByteExactSeed`, so any two applied together would be
  # indistinguishable, and V03/V04 name the same test by construction.

  # V01 reverts the carve-out itself — `isSeed` back to `seedDecisionStates`
  # alone. Deliberately expects EVERY evc1 test that is about the ADMISSION,
  # which makes it the vacuity audit for this suite: if any of these stays green
  # with the carve-out gone, that test was never measuring it. The three
  # exclusion tests (V02/V03/V05's subjects) correctly stay green here.
  "V01|62|PODC|$T_EVC1_SEEDS;$T_EVC1_FIELD;$T_EVC1_SURVIVES;$T_EVC1_DELIVERY;$T_EVC1_TILE_SILENT;$T_EVC1_GATE_BLIND;$T_EVC1_SEED_UNTOUCHED;$T_EVC1_NO_AUTOSKIP;$T_EVC1_CONFIRM_MARKS;$T_EVC1_ENTRY_ONCE;$T_EKS2_DAY0_SEED"

  # V02 drops the `boundaryState` leg, so the byte-exact ANCHORS alone admit a
  # row. Anchors are a per-edge provenance tier that several producers can set;
  # the day-0 `boundaryState` has exactly one writer. Carving on the wrong one
  # would admit any refined row that happened to carry rediff edges.
  "V02|63|PODC|$T_EVC1_OTHERS_REFUSED"

  # V03 drops BOTH anchor legs, admitting a playhead-9s6q segment-recovered
  # slot — whose boundaries playhead-pyq7 has not validated — as a seed. A pod
  # walk starts AT the seed edge, so this is the mutation that can put the
  # walk's first step inside the show.
  "V03|64|PODC|$T_EVC1_SEGMENT_RECOVERED"

  # V04 is the subtler half of V03: require only ONE anchored edge. A
  # half-validated boundary is still unvalidated on the other side, and the walk
  # uses both.
  "V04|65|PODC|$T_EVC1_SEGMENT_RECOVERED"

  # V05 drops the visible-state leg, so a row the listener VETOED
  # (`decisionState = .reverted`) goes on seeding marks around the span it was
  # vetoed out of. A veto is the only durable "no" that exists for this row
  # type — every other retirement path exempts it.
  "V05|66|PODC|$T_EVC1_VETOED"

  # V06 keys the carve-out on the eligibility gate. It looks like hardening and
  # is the opposite: the gate moves with
  # `RediffActivation.dayZeroByteExactAutoSkipEnabled`, so pod recovery would
  # become a side-effect of an auto-skip flag, and turning auto-skip off would
  # silently turn first-listen pod recovery off with it.
  "V06|67|PODC|$T_EVC1_GATE_BLIND"
  "L09|61|PODC|$T_EKS2_ENTRY_ONCE"

  # playhead-kvs8 — the FM daemon THROTTLE (Q01-Q08).
  #
  # Q01-Q03 are one rail per COLUMN of the field row, deliberately separated:
  # `status=failed`, the burned `retryCount`, and the unattributable reason
  # string are three independent defects that a single mutation would conflate.
  # Each gets its own batch because all three redden the same test.

  # Q01 makes the throttle arm unreachable (its guard can never hold), so a
  # throttled prologue falls through to the generic catch-all and the job is
  # marked terminally `failed` — the field row, restored.
  "Q01|62|RUNNER|$T_KVS8_DEFERS;$T_KVS8_RETRY;$T_KVS8_CAUSE;$T_KVS8_BATCH"

  # Q02 spends a lifetime retry on the throttle. Everything else about the
  # defer is intact, so ONLY the retryCount assertion moves — which is the
  # point: three unlucky throttles must not disqualify an episode that was
  # never scanned once.
  "Q02|63|RUNNER|$T_KVS8_RETRY"

  # Q03 merges the prologue cause into pmp9's window cause. Both are honest
  # tokens, so nothing crashes and coverage looks fine; what is lost is the
  # operator's ability to tell "the daemon refused us outright, nothing was
  # scanned" from "a window lost its retries and we banked the rest".
  "Q03|64|RUNNER|$T_KVS8_CAUSE"

  # Q04 restores `.permissiveRefusal` as the status a throttled permissive
  # window persists — a `.persistFailure` status, so a momentary throttle
  # becomes a permanent coverage hole attributed to Apple's safety layer.
  "Q04|65|FMCLS|$T_KVS8_REASON;$T_KVS8_PERM"

  # Q05 charges a throttle to the permissive REFUSAL counter. The persisted row
  # is still honest, so only the telemetry lies — the model looks like it
  # refuses more windows the busier the device gets.
  "Q05|66|FMCLS|$T_KVS8_REASON"

  # Q06 re-hard-codes `.permissiveRefusal` in the coarse defensive arm, which is
  # the arm an iOS-27 `LanguageModelError` throttle actually reaches. Distinct
  # seam from Q04: the mapping is correct and simply not consulted.
  "Q06|67|FMCLS|$T_KVS8_PERM"

  # Q07 caches a throttled readiness probe as an unusable model. `probeIfNeeded`
  # gates the WHOLE lane, so this converts one momentary daemon refusal into 15
  # minutes with no ad scanning at all on a perfectly healthy device.
  "Q07|68|PROBE|$T_KVS8_PROBE"

  # Q08 drops the consecutive stop threshold to one, so a single throttled job
  # ends the whole drain. The counter still resets, so this is specifically the
  # "one event is not a device fact" rail.
  "Q08|69|THROT|$T_KVS8_CONSEC"

  # playhead-usn1 — the per-show skip-mode CONTROL (U01-U16).
  #
  # U01 is THE field defect, restored: drop the publish that carries
  # `beginEpisode`'s verdict to the surface. Everything still resolves — the
  # trust profile is read, the mode is correct inside the orchestrator — and the
  # screen keeps showing the cleared pair it sampled before the episode began,
  # which playhead-djl0's pill renders as "Show Unknown" with the menu withheld.
  # Its own batch: U15 reddens the same two view-model tests from the other end.
  "U01|70|ORCH|$T_USN1_LATE;$T_USN1_NOTFINAL;$T_USN1_VMLATE;$T_USN1_VMMENU"

  # U02-U04 are the other three transitions. Disjoint expectations, one batch:
  # U02 lets the PREVIOUS show's mode stand across the next lookup's
  # suspensions; U03 leaves a mounted screen describing a show that stopped
  # playing; U04 makes the listener's own choice invisible to the surface.
  "U02|71|ORCH|$T_USN1_PRECLEAR"
  "U03|71|ORCH|$T_USN1_ENDCLEAR"
  "U04|71|ORCH|$T_USN1_OVERRIDE"

  # U05 removes the replay-on-attach. Subtle, because every LATER transition
  # still arrives: what breaks is the screen opened from the mini player
  # mid-episode, which subscribes to a stream that has nothing left to say.
  "U05|72|ORCH|$T_USN1_REPLAY"

  # U06 restores the pre-bead read: the relationship or nothing. Its own batch
  # because U07 reddens two of the same tests from a different direction.
  "U06|73|MODEL|$T_USN1_RECOVER;$T_USN1_IDENTICAL;$T_USN1_GUIDSEP;$T_USN1_IPV6"

  # U07 splits the key on the FIRST `::`. It first shipped naming the
  # guid-with-separator test too, and SURVIVED on that one — correctly: the guid
  # is the SUFFIX, so a first-split still recovers the right feed URL when the
  # GUID contains `::`. What a first-split breaks is a feed URL with an IPv6
  # literal host, and any key that is not this episode's at all (it will happily
  # invent a show for a mismatched guid). Those are the two it names now. The
  # guid case gets its own mutation below rather than a relaxed expectation.
  # U08 drops the canonicalisation, admitting a spelling that resolves to a
  # different persisted namespace for the same show.
  "U07|74|MODEL|$T_USN1_IPV6;$T_USN1_MISMATCH"
  "U08|74|MODEL|$T_USN1_CANON"

  # U18 is the OTHER plausible way to write the derivation: split on the LAST
  # `::`. Correct for an IPv6 host and wrong for a guid containing the separator
  # — the exact complement of U07, which is why the pair exists. Separate batch
  # from U07: same lines, and both redden the mismatched-key rail.
  "U18|79|MODEL|$T_USN1_GUIDSEP;$T_USN1_MISMATCH"

  # U09-U11 are the three halves of "never silently skip". U09 answers a write
  # it did not perform with a success carrying a fabricated empty identity —
  # the `if let` with no `else`, wearing the new return type. U10 keeps the
  # refusal honest but stops counting it. U11 applies the mode for the session
  # only: the same lie in a shorter-lived form, and the one that looks most like
  # it worked.
  "U09|75|RT|$T_USN1_REFUSE"
  "U10|75|RT|$T_USN1_COUNT"
  "U11|75|RT|$T_USN1_SESSION"

  # U12-U16 are the identity handoff. U12 lets a late recovery overwrite the
  # identity the caller supplied; U13 lets a superseded play request write its
  # show onto the session that replaced it. They share a batch because neither
  # masks the other — both leave the adoption body reachable.
  "U12|76|RT|$T_USN1_NOOVERWRITE"
  "U13|76|RT|$T_USN1_STALE"

  # U16 drops the adoption entirely, which is the state playhead-djl0 shipped:
  # the orchestrator holds a recovered show and reports a resolved identity — so
  # the pill offers the menu — while the runtime's write target is still nil.
  #
  # ITS OWN BATCH, and that is the whole point. It first shipped batched with
  # U12/U13 and reported both of them SURVIVED — a FALSE survivor: an early
  # `return` makes every other edit in the same function unreachable, so their
  # rails had nothing to fail on. This is the "blast radius overlaps" case the
  # batching rule at the top of this file warns about, and it produced exactly
  # the misleading verdict that rule exists to prevent.
  "U16|80|RT|$T_USN1_ADOPT"

  # U14 leaves the optimistic `.sessionOverride` standing after a refusal, so
  # the pill reports the listener's choice back to them while nothing has
  # stored it. U15 subscribes and then never consumes, which is the one shape
  # that looks wired and is not.
  "U14|77|NPVM|$T_USN1_VMREVERT"
  "U15|77|NPVM|$T_USN1_VMLATE;$T_USN1_VMMENU"

  # U17 files the refusal under playhead-djl0's READ-side code. Both tokens are
  # honest and nothing crashes; what is lost is an operator's ability to tell
  # "we never knew the show" from "the listener tried to set a preference and it
  # went nowhere". The same collapse djl0 spent a bead undoing, one layer over.
  "U17|78|RT|$T_USN1_TRACE"

  # playhead-isp5 — the ingest audit trail (W01-W06). Batch ids 81-83: 61 is
  # unusable (a duplicate L09 record ERRORs a rerun) and 62-67 are shared by
  # evc1 and kvs8, so these start above the highest id in the file.
  #
  # W01 restores the pre-isp5 state of the door verbatim: the drop still writes
  # its `os_log` line, and nothing durable. That is precisely the state in which
  # "the ingest never fired" and "the ingest fired and refused" were the same
  # observation, which is what kept this bead open through two investigations.
  # W02 keeps the outcome and drops its REASON — the more tempting mistake,
  # because the row still looks informative while `tooEarly` (every pre-roll)
  # and `overlapsDeclaredChapter` (a chapter-metadata bug) collapse into one
  # bucket. Batched: different functions, disjoint rails, neither edit makes the
  # other's site unreachable.
  "W01|81|ORCH|$T_ISP5_NOTPLAYING"
  "W02|81|ORCH|$T_ISP5_REASON"

  # W03 inverts the lifetime rule — clearing the per-cause tally at episode end
  # turns a build-level measurement ("this build loses pre-rolls") into a
  # per-episode one that reads zero by the time anyone looks. W04 is the
  # audit's own arithmetic: count a DROP as a delivery and every future
  # `delivered=` figure in the field logs is inflated, which is the
  # "what would this number read if the thing never happened?" check applied to
  # the instrument rather than the product.
  "W03|82|ORCH|$T_ISP5_LIFETIME"
  "W04|82|INGO|$T_ISP5_DELIVERED"

  # W05 writes the census BEFORE `receiveAdWindows` returns, so every forwarded
  # row reads as unstamped and a total loss renders identically to a healthy
  # delivery. W06 drops the detail rendering, which is W02 one layer down: the
  # reason is captured and then never reaches the file. Batched because W05
  # touches only the forwarding order and W06 only the value type's rendering —
  # W06's rail runs through the DOOR path, which W05 does not reorder.
  "W05|83|ORCH|$T_ISP5_ROW"
  "W06|83|INGO|$T_ISP5_READ"

  # W07 is the OTHER direction of the same judgement: make the empty-store
  # preload durable again. It is the shape the first draft shipped, and it cost
  # a diagnostics session file per episode start — measured as ~18 extra
  # 60 s time-limit exceedances across the load-sensitive families in a full
  # gate run. Its own batch because it is the only rail whose defect is a COST
  # rather than a lost fact, and batching it with an edit that suppresses rows
  # would confuse the two.
  "W07|84|ORCH|$T_ISP5_SILENT"

  # playhead-b6r2. B01 and B02 restore the OUTER-edge reading of rule (b) — the
  # exact code that shipped for eleven weeks and dropped `d0-1` as `tooEarly`
  # and `d0-4` as `tooLate` on Dan's 2026-08-01 session. Their own batches: the
  # two edits live in the same `evaluate` and a batched failure could not
  # distinguish "the head revert reddened the post-roll test" from a genuine
  # kill. Each names the same claim at four altitudes — the pure function, the
  # xr3t contract, the orchestrator boundary, and the census/banner the
  # listener actually meets — so a mutation that satisfies one layer while
  # losing the window at another cannot pass.
  "B01|85|INVF|$T_B6R2_PREROLL;$T_B6R2_PREROLL_XR3T;$T_B6R2_PREROLL_ORCH;$T_B6R2_FIELD_SLOTS;$T_B6R2_PREROLL_ARMS;$T_B6R2_PREROLL_BANNER"

  "B02|86|INVF|$T_B6R2_POSTROLL;$T_B6R2_POSTROLL_XR3T;$T_B6R2_POSTROLL_ORCH;$T_B6R2_POSTROLL_ARMS"

  # B03 and B04 are the boundary itself: relax `<=` to `<` (and `>=` to `>`) so
  # a span that exactly FILLS the margin band escapes. This is the direction
  # the fix could plausibly be over-applied — one token away from a rule that
  # rejects nothing an artifact would actually produce. Batched together
  # because each is a single comparison in a different rule with a single,
  # distinct expected test; neither reaches the other's span.
  "B03|87|INVF|$T_B6R2_HEAD_BOUNDARY"
  "B04|87|INVF|$T_B6R2_TAIL_BOUNDARY"

  # B05 restores the divergence itself: the init default back to a disabled
  # no-op while production stays ON. Nothing about the FIX breaks — every
  # filter test that passes an explicit filter still passes — and that is the
  # point. This is the rail that would have gone red in April.
  "B05|88|ORCH|$T_B6R2_DEFAULT"

  # B06 is the same divergence one layer down: keep the init reading the shared
  # constant, and make the CONSTANT wrong. It proves the structural tripwire
  # has teeth rather than being a tautology over its own definition. Its own
  # batch because its expectation set contains B05's.
  "B06|89|INVF|$T_B6R2_DEFAULT;$T_B6R2_FRESH_INSTALL"

  # B07 is the guard that MOVED rather than died. xr3t's rule (b) rejected a
  # negative start as `tooEarly` — incidentally, by the same clause that
  # rejected every pre-roll — so this bead had to show impossible material is
  # still refused elsewhere. Delete `startTime >= 0` from the material check
  # and the claim is a lie.
  "B07|90|ORCH|$T_B6R2_MATERIAL"

  # playhead-y3ya. Batch ids 91-99, verified fresh against the whole array
  # before use (61 carries a duplicate L09 that ERRORs a rerun, 62-67 are shared
  # by evc1 and kvs8, isp5 took 81-84 and b6r2 85-90).
  #
  # Y01 admits `uncertain` alongside `containsAd`. It is the global threshold
  # lowering this bead is FORBIDDEN to do, wearing the smallest possible
  # disguise — one `||`. Y02 deletes the examined check, so a cancelled row
  # votes on whatever its disposition column happens to hold; the field sweep
  # ended `2581-2676 | abstain | cancelled`, so this is not hypothetical.
  # Batched: adjacent but distinct guards, disjoint rails, neither edit makes
  # the other's anchor unreachable.
  "Y01|91|SWEEP|$T_Y3YA_UNCERTAIN"
  "Y02|91|SWEEP|$T_Y3YA_UNEXAMINED"

  # Y03 is the pz32 defect at one remove: read `row.status.didExamineWindow`
  # instead of the row-level predicate, which drops the no-work-sentinel
  # exclusion. `.noAds` IS an examined status, so a sentinel spanning the whole
  # attempted range mints one enormous mark over an episode nothing scanned.
  # Its own batch because it rewrites the same line Y02 deletes.
  "Y03|92|SWEEP|$T_Y3YA_SENTINEL"

  # Y04 is THE BUG, rebuilt one layer down: drop a coarse verdict that no pass-B
  # refinement narrowed. That is "presence needs a seed to attach to" restated
  # inside the composer, and it is the tempting shape because it reads like
  # prudence. Own batch — its blast radius is every field-case test, so a
  # batched partner would be credited off it.
  "Y04|93|SWEEP|$T_Y3YA_FIELD;$T_Y3YA_PASSB_DECLINED;$T_Y3YA_WIRE"

  # Y05 drops a pass-B verdict that lies inside no coarse containsAd window —
  # the same "must have a host" rule as Y04, applied to the other direction.
  "Y05|94|SWEEP|$T_Y3YA_ORPHAN_PASSB"

  # Y06 IS THE INVERSION THE BEAD NAMES. Require an anchor before emitting, so
  # a hard boundary GATES eligibility instead of CLIPPING geometry. It is one
  # clause, it looks conservative, and it reproduces exactly the reasoning that
  # discarded both field verdicts. Own batch: it empties the composer, so every
  # other rail in a shared batch would have nothing to fail on.
  "Y06|95|SWEEP|$T_Y3YA_NO_GATE;$T_Y3YA_FIELD;$T_Y3YA_ARMS"

  # Y07 stamps `.eligible` instead of the hard-coded `markOnly`, which is the
  # one edit that turns a coarse FM guess into a silent skip. Named at three
  # altitudes — the pure literal, the census count, and the managed-set count —
  # so a mutation cannot satisfy the composer while losing the tier at the door.
  # Y08 claims `.stingerSnapped` on both edges, i.e. lets a CLIP become an
  # anchor CLAIM; that is what would hand playhead-2350's gate a boundary an FM
  # window merely happened to sit near. Batched: different lines in `makeMark`,
  # disjoint rails, neither reaches the other's assertion.
  "Y07|96|SWEEP|$T_Y3YA_MARKONLY;$T_Y3YA_ARMS_COUNT;$T_Y3YA_NOT_MANAGED"
  "Y08|96|SWEEP|$T_Y3YA_UNANCHORED;$T_Y3YA_CLIP_UNANCHORED"

  # Y09 narrows the dedupe to VISIBLE decision states — which is precisely what
  # `SpecialistMarkComposer` does, so it is the mistake a reader copies. It
  # resurfaces a span the listener already vetoed. Y10 drops the mark
  # confidence below `preloadAdmissibleWindows`' 0.70 floor, which loses the
  # window at BOTH doors while every pure-composer test stays green. Batched:
  # one constant and one filter, disjoint rails.
  "Y09|97|SWEEP|$T_Y3YA_VETO"
  "Y10|97|SWEEP|$T_Y3YA_PRELOAD;$T_Y3YA_RELAUNCH"

  # Y11 and Y12 unbound the two radii. An unbounded merge claims the show
  # between two real breaks; an unbounded clip snaps an edge to a boundary
  # belonging to something else, which is inventing extent — the failure
  # playhead-2350 documented. Batched: distinct constants, and each rail's
  # fixture is inert under the other edit (the merge fixture passes no anchors,
  # the clip fixture has one row).
  "Y11|98|SWEEP|$T_Y3YA_MERGE_BOUND"
  "Y12|98|SWEEP|$T_Y3YA_CLIP_RADIUS"

  # Y13 and Y14 are the two WIRES, and they are the defect a pure-composer
  # battery structurally cannot see: a correct composer that is never called.
  # Y13 inverts Step 18c's flag read; Y14 inverts the runner tail's. Separate
  # batches because each is the other's control — a single flag inversion must
  # redden ONE site, and a batched pair could not show that the two sites are
  # independently wired.
  "Y13|99|ADSVC|$T_Y3YA_WIRE"
  "Y14|100|RUNNER|$T_Y3YA_TAIL"

  # Y15 and Y16 delete the MODE gate at each site, leaving the feature flag
  # standing. `ApprovedCohortRegistry` collapses an unapproved prompt / schema /
  # scan-plan / locale / appBuild cohort to `.shadow` — PlayheadRuntime's
  # bootstrap calls that "exactly the protection the registry was designed to
  # provide" — so a composer that reads only the flag would put an UNAPPROVED
  # cohort's verdicts in front of the listener. Batched: different files,
  # different rails, and each gate is the other's independent site.
  "Y15|101|ADSVC|$T_Y3YA_SHADOW_SVC"
  "Y16|101|RUNNER|$T_Y3YA_SHADOW_RUN"

  # Y17 drops the mark-WIDTH ceiling, so a coarse verdict spanning nineteen
  # minutes (SpanExtentSupport's header records FM windows of 17.04-1183.62 s)
  # becomes a banner claiming show. Y18 drops the ceiling from the MERGE only,
  # which is the subtler half: the width filter still catches the fused extent,
  # but it drops it WHOLE, so every verdict in a run of adjacent windows is lost
  # instead of surviving as several bounded marks. Batched: one is a filter and
  # one a merge condition, with distinct fixtures.
  "Y17|102|SWEEP|$T_Y3YA_CEILING"
  "Y18|102|SWEEP|$T_Y3YA_MERGE_CEILING"

  # -------------------------------------------------------------------------
  # playhead-lxkq — the ad-likelihood scan ORDER (X01-X16)
  #
  # The bead's whole safety argument is that `AdLikelihoodScanOrder.order` is a
  # PERMUTATION: same windows, same prompts, different sequence. So the rails
  # come in three shapes — the ones that break the RANKING (a wrong window goes
  # first), the ones that break the PERMUTATION (a window is dropped, which is
  # starvation), and the two WIRES, which are the defect a pure battery
  # structurally cannot see: a correct permutation nothing calls.
  #
  # Batches are conservative here because the scoring is additive and global:
  # almost any edit to `order` moves more than one window, so two edits in one
  # batch can plausibly redden each other's victim. Where that was in doubt the
  # mutation got its own batch.

  # 103: three edits that cannot reach each other. X01 is the weights table,
  # X11 the seed's own clamp, X12 the order-RESTORATION — three functions, three
  # disjoint victims.
  "X01|103|SCANORD|$T_LXKQ_WEIGHTS"
  "X11|103|SCANORD|$T_LXKQ_CLAMP"
  "X12|103|SCANORD|$T_LXKQ_RESTORE;$T_LXKQ_RESTORE_STABLE;$T_LXKQ_W_REPORTED"

  # 104: X02 makes the score a MAX instead of a sum, so two independent signals
  # agreeing on one region stop reinforcing. X03 drops the seed-width ceiling,
  # so a recurring sponsor's episode-wide span names the whole episode. X02's
  # fixture uses point seeds (ceiling-immune) and X03's uses a 3,000 s seed
  # (dropped whatever the scoring), so neither can be credited off the other.
  "X02|104|SCANORD|$T_LXKQ_AGREEMENT"
  # X03 names ONLY the width test. It first also named the linear-sweep fallback
  # and SURVIVED on it — measured, and the measurement was right. With the
  # ceiling gone, that fixture's 3,000 s seed opens ONE neighbourhood covering
  # every window, so every window scores identically, the tie-break resolves to
  # episode order, and the 1,800 s cap promotes the first thirty windows in that
  # same order. The mutant's output is the identity permutation — indistinguishable
  # from the linear sweep it is supposed to have broken. That is a mis-authored
  # expectation, not a coverage hole: the ceiling's real consequence is caught by
  # the width test, which failed as intended.
  "X03|104|SCANORD|$T_LXKQ_WIDTH"

  # 105: the starvation mutant — promotion becomes a FILTER. This is the one
  # the bead's central claim is about, and its blast radius is every
  # count-preservation assertion in both suites, so it runs alone.
  "X04|105|SCANORD|$T_LXKQ_PERMUTATION;$T_LXKQ_FILLER;$T_LXKQ_NAN_SPAN;$T_LXKQ_W_ONCE"

  # 106: the tie-break inverted. Ties are what make the order a pure function of
  # its inputs, and inverting the start term moves the first window in three
  # separate fixtures — alone, because two of those victims are other X rails'.
  "X05|106|SCANORD|$T_LXKQ_TIES;$T_LXKQ_CAP;$T_LXKQ_AGREEMENT"

  # 107: X07 removes the promoted-prefix cap entirely; X10 seeds an evidence
  # anchor from its episode-wide COVERAGE span instead of its own position.
  # Different functions, different fixtures.
  "X07|107|SCANORD|$T_LXKQ_CAP"
  "X10|107|SCANORD|$T_LXKQ_ANCHOR_POS"

  # 108: X08 deletes the "a non-empty seeded set always promotes at least one
  # plan" carve-out, so a single 1,183 s device window is excluded by a 1,800 s
  # budget it fits inside. Separate batch from X07 because both rewrite the same
  # admission `if`. X09 relaxes BOTH zero-score guards (two sites — a
  # half-applied mutation here would fabricate a rail).
  "X08|108|SCANORD|$T_LXKQ_OVERSIZE"
  "X09|108|SCANORD|$T_LXKQ_ZERO"

  # 109: the neighbourhood collapses to the seed's own extent. The 2828-2836
  # seam then no longer reaches the 2838-2954 pod that starts AFTER it, which is
  # the exact geometry the field episode turned on.
  "X06|109|SCANORD|$T_LXKQ_RADIUS;$T_LXKQ_PREFIX"

  # 110: WIRE ONE. `planPassA` packs correctly and throws the ordering away.
  # Every pure test in the suite still passes — that is the point of listing the
  # wiring suite in FOCUSED_SUITES.
  "X13|110|FMCLS|$T_LXKQ_W_FIRST;$T_LXKQ_W_CALL;$T_LXKQ_W_RUNNER_ON"

  # 111: WIRE TWO. X14 scopes the runner's seed derivation to a phase a
  # cold-start show never reaches, so the seeds are computed and never used —
  # which is precisely the pre-lxkq defect (`AssetInputs.acousticBreaks` was
  # threaded in and dropped by `narrowedInputs`). X16 flips the SHIPPED default,
  # a different file and a different victim.
  "X14|111|RUNNER|$T_LXKQ_W_RUNNER_ON"
  "X16|111|ADSVC|$T_LXKQ_W_SHIPPED"

  # 112: the flag stops being a flag. Same anchor as X14, so it cannot share
  # 111 — and it is X14's control: OFF must restore the pre-lxkq sweep exactly.
  "X15|112|RUNNER|$T_LXKQ_W_RUNNER_OFF"
  # playhead-cgka — the per-test scratch lifetime. Batches 120+.
  #
  # The batching rule bites harder than usual here, because several of these
  # mutations disable the reaper wholesale and so redden more rails than they
  # are credited for. Z11 alone reddens RECLAIM, DEFER, AUTOSWEEP and OWNEDBY —
  # which is why Z01 (also DEFER), Z07 (also AUTOSWEEP) and Z10 (also OWNEDBY)
  # are each in a different batch from it. A kill credited to the wrong mutation
  # is worse than an extra build.
  "Z01|120|SCRATCH|$T_CGKA_DEFER"
  "Z09|120|SCRATCHH|$T_CGKA_REGISTERS"

  # Z02 SURVIVED when first run, correctly: the sweep's live-owner branch
  # cleared the mark too, so deleting the reset in `adopt` changed no
  # observable behaviour. That redundant path is gone and the rail now
  # asserts the property that is actually at stake — the deferral being
  # RE-ARMED for the new owner, not the file merely surviving.
  "Z02|121|SCRATCH|$T_CGKA_READOPT"
  "Z04|121|SCRATCH|$T_CGKA_UNREADABLE"
  "Z06|121|SCRATCH|$T_CGKA_CLAMP"

  # Z03 gets a batch to itself for BLAST RADIUS, not for expectation overlap:
  # it makes the SHARED reaper delete unowned directories, i.e. every bare
  # makeTempDir directory in every focused suite, mid-test. Anything sharing the
  # batch would be judged against a run whose failures are mostly collateral.
  "Z03|122|SCRATCH|$T_CGKA_UNOWNED;$T_CGKA_CONCURRENT"

  "Z05|123|SCRATCH|$T_CGKA_UNREADABLE"
  "Z07|123|SCRATCH|$T_CGKA_AUTOSWEEP"
  "Z08|123|SCRATCHH|$T_CGKA_STORE_ADOPT"

  "Z10|124|SCRATCHH|$T_CGKA_OWNEDBY"
  "Z12|124|SCRATCHH|$T_CGKA_WIPE"

  "Z11|125|SCRATCH|$T_CGKA_RECLAIM;$T_CGKA_DEFER"

  # playhead-avbn (A01-A11). Two claims, and they are opposite in sign, which
  # is why the negative controls matter as much as the rails: a `.noAds` scan
  # row may only vote that there is no ad if it ANSWERED that question
  # (A01/A02/A05), and the gate that records the resulting veto must be named
  # and sorted as the block it actually is (A07/A08/A09/A11).
  #
  # A01 and A05 are the same guard mutated in opposite directions and each
  # reddens most of the voting suite, so they are batched apart — a kill
  # credited to the wrong direction would be worse than the extra build.
  "A01|130|FMSUP|$T_AVBN_REFINE_EMPTY;$T_AVBN_REFINE_TWO;$T_AVBN_REFINE_FOUND"
  "A06|130|RUNNER|$T_AVBN_MEASURED_QUALITY"
  "A07|130|GATE|$T_AVBN_SEVERITY;$T_AVBN_MERGE_DEMOTES"

  "A02|131|FMSUP|$T_AVBN_SENTINEL_ONE;$T_AVBN_SENTINEL_TWO"
  "A08|131|GATE|$T_AVBN_LEGACY_DECODES;$T_AVBN_LEGACY_CODABLE;$T_AVBN_LEGACY_REENCODE"

  "A03|132|FMSUP|$T_AVBN_ABUTS"
  # A09 also reddens the orchestrator's malformed-gate rail (every unknown
  # string would decode), which is collateral, not its claim — batched away
  # from A11, whose claim IS an orchestrator gate rail.
  "A09|132|GATE|$T_AVBN_ALIAS_ONE_WAY"

  "A04|133|FMSUP|$T_AVBN_BAND"
  "A10|133|ADSVC|$T_AVBN_WIRE"
  "A11|133|ORCH|$T_AVBN_BLOCKED_DROPPED"

  "A05|134|FMSUP|$T_AVBN_COARSE_VOTES;$T_AVBN_COARSE_PAIR_TRIGGERS"

  # playhead-le02 (LE01-LE08). Two batches, and the split is by EXPECTATION
  # OVERLAP rather than by blast radius: within a batch no two mutants claim the
  # same test, so a kill is attributable without re-running.
  #
  # LE01 is the centrepiece and is deliberately the CRUELLEST version of the
  # A11 class: it arms a suggestion while LEAVING THE CENSUS STAMP INTACT. A
  # rail that trusted only `lastAdWindowIngestOutcome` would still read
  # `droppedInventorySanity` and pass. Only the `activeSuggestWindowIDs()`
  # claim — which reads the collection the suggest tier actually lands in —
  # can see it. That is why the repair asserts both and not either.
  "LE01|230|ORCH|$T_LE02_INV_SHORT;$T_LE02_INV_HEAD;$T_LE02_INV_TAIL;$T_LE02_INV_CHAPTER;$T_LE02_INV_SYNTH;$T_LE02_BYPASS_ADWINDOW"
  "LE04|230|ORCH|$T_LE02_DOOR_FOREIGN;$T_LE02_DOOR_NOEPISODE"
  "LE06|230|ORCH|$T_LE02_BYPASS_DECISION"
  "LE07|230|ORCH|$T_LE02_ENDEPISODE_RESET"

  # LE02/LE03 attack the census's PRECISION rather than its presence: the four
  # inventory rejection reasons are four unrelated defects (playhead-b6r2 was
  # exactly a tooEarly/tooLate mix-up), and a retroactive retirement is a
  # different fact from a hot-path drop. LE05 mis-attributes a door cause.
  # LE08 is the A11 shape at the tier-routing `if` itself, narrowed to applied
  # rows so its blast radius stays inside the rail it is aimed at.
  "LE02|231|ORCH|$T_LE02_INV_HEAD;$T_LE02_INV_TAIL;$T_LE02_INV_CHAPTER;$T_LE02_INV_SYNTH"
  "LE03|231|ORCH|$T_LE02_INV_RETRO_CHAPTER;$T_LE02_INV_RETRO_DURATION"
  "LE05|231|ORCH|$T_LE02_DOOR_VETOED"
  "LE08|231|ORCH|$T_LE02_PRELOAD_APPLIED"

  # playhead-sik9 (C01-C10). ONE predicate, five ways to get it wrong, and
  # every one of them compiles. C01 deletes the exemption (the old blanket
  # demotion — the thing that would delete Dan's post-roll); C02 inverts it;
  # C03 widens it to any WIDTH oracle (`.spliceSlot` is the trap: it also sets
  # both edges, it is just acoustic); C04 widens it to any provenance at all;
  # C05 implements it as a PROMOTION rather than a declined demotion. Those
  # five all touch the same `if`, so they interact when applied together and
  # each takes its own batch — C01 and C05 in particular CANCEL (delete the
  # exemption, then re-promote what it would have exempted), which would have
  # fabricated two survivors in one build.
  #
  # C06 attacks the shared DEFINITION rather than this bead's use of it.
  # C07/C08 are the composition claim: the exemption declines to demote at the
  # guard, it must never outrank playhead-2350's unanchored-edge block. C09/C10
  # are the two REACHABILITY facts that make the playhead-9s6q carve-out
  # structural rather than a fixture accident.
  # NOT expected of C01: the CUE-surface test. It builds its `AdWindow` directly
  # at the orchestrator's door, so no mutation of the fusion predicate can reach
  # it — it is an end-of-chain assertion that `.eligible` really becomes a skip,
  # not a rail on the exemption. Said out loud because listing it here would
  # have produced a fabricated kill.
  "C01|140|FUSION|$T_SIK9_BEAD;$T_SIK9_BELOW_FLOOR;$T_SIK9_SCORES;$T_SIK9_FUSION_BELOW;$T_SIK9_FUSION_AT"
  "C09|140|RSLOT|$T_SIK9_LAGGED"
  "C10|140|RSLOT|$T_SIK9_DAY0_STRICT"

  "C02|141|FUSION|$T_SIK9_GUARDED;$T_SIK9_SPLICE;$T_SIK9_FUSION_SPLICE"
  "C07|141|EXTENT|$T_SIK9_REWRITE;$T_SIK9_2350"

  "C03|142|FUSION|$T_SIK9_GUARDED;$T_SIK9_SPLICE;$T_SIK9_FUSION_SPLICE"
  "C08|142|FUSION|$T_SIK9_2350"

  "C04|143|FUSION|$T_SIK9_GUARDED"

  "C05|144|FUSION|$T_SIK9_NO_PROMOTE"
  "C06|144|DSPAN|$T_SIK9_SPLICE;$T_SIK9_FUSION_SPLICE"

  # playhead-nqey (F01-F09). The bead is an ENABLEMENT, so the battery attacks
  # the enablement rather than the mechanism sik9 already covers.
  #
  # Five of the nine (F01-F05) mutate the SHIPPED VALUES in
  # `AdDetectionConfig.default` and its init. Every one of them compiles, every
  # one of them leaves the sik9 suites entirely green — those build their own
  # `FusionWeightConfig(certaintyTieredEnabled: true)` and never ask what
  # production sends — and every one of them silently returns the pipeline to a
  # policy nobody chose. That gap is the whole reason
  # `CertaintyTieredSkipShipsOnTests` exists, and F01-F05 are what prove it is
  # closed rather than merely written.
  #
  # F01/F02 are the two ways to un-ship the flip (the static and the init
  # default; they must not diverge, which is why F02 has its own expectation).
  # F03/F04 disarm ONE HALF each by zeroing its parameter — the "inert"
  # failure mode. F05 raises the floor above 1.0 — the "total" failure mode,
  # where every host-read demotes and the gate stops being certainty-TIERED.
  # F03/F04/F05 are the reason `flipIsNeitherInertNorTotal` asserts a strict
  # non-empty subset instead of asserting only that something demoted.
  #
  # F06-F09 attack the SCOPE claims the PR makes in prose, so that the prose
  # cannot drift from the code: F06 leaks the flip into the bare
  # `FusionWeightConfig()` the hot path and the aggregator decision logs use;
  # F07 turns the floor into `<=` so an AT-floor span demotes; F08 removes the
  # rediff carve-out from the FLOOR branch (the guard keeps its own, so this is
  # the half sik9 did not touch); F09 makes the guard guess an unknown episode
  # end.
  #
  # BATCHING. F01-F05 all edit `AdDetectionConfig.default` (and F02 the init
  # beside it), and they CANCEL in the obvious ways — F01 turns the switch off,
  # which makes F03/F04/F05's parameter edits unobservable, fabricating three
  # survivors in one build. Each takes its own batch. F06-F09 edit four
  # distinct sites in BackfillEvidenceFusion.swift; they are also one per batch,
  # F07/F08 because they both concern the floor branch and F06 because its blast
  # radius is the widest in the series — arming the bare `FusionWeightConfig()`
  # reddens much of `BackfillEvidenceFusionTests` as collateral, and a batchmate
  # judged against that noise is a false KILL waiting to happen.
  #
  # NOT expected of any F entry: `PostRollGuardExemptCueSurfacingTests`. Same
  # reason the C series gives — it builds its `AdWindow` at the orchestrator's
  # door, so no config or fusion-side mutation reaches it.
  "F01|150|ADSVC|$T_NQEY_SHIPPED_ON;$T_NQEY_FLOOR_DEMOTES;$T_NQEY_TAIL_DEMOTES;$T_NQEY_SUBSET;$T_NQEY_WIREIN_DEFAULT"

  "F02|151|ADSVC|$T_NQEY_INIT_MATCHES;$T_NQEY_WIREIN_OMITTED;$T_NQEY_WIREIN_VERBATIM"

  "F03|152|ADSVC|$T_NQEY_SHIPPED_ON;$T_NQEY_FLOOR_DEMOTES;$T_NQEY_SUBSET"

  "F04|153|ADSVC|$T_NQEY_SHIPPED_ON;$T_NQEY_TAIL_DEMOTES;$T_NQEY_SUBSET"

  "F05|154|ADSVC|$T_NQEY_SHIPPED_ON;$T_NQEY_AT_FLOOR_KEPT;$T_NQEY_SUBSET"

  "F06|155|FUSION|$T_NQEY_BARE_INERT"

  "F09|156|FUSION|$T_NQEY_UNKNOWN_INERT;$T_SIK9_UNKNOWN"

  "F07|157|FUSION|$T_NQEY_AT_FLOOR_KEPT;$T_NQEY_SUBSET"

  "F08|158|FUSION|$T_NQEY_REDIFF_FLOOR_KEPT;$T_SIK9_BELOW_FLOOR;$T_SIK9_FUSION_BELOW"

  # --- playhead-6qvf (G series): the byte/chroma certainty split -------------
  #
  # G01 and G02 are the two halves of the ACTUAL shipped defect, and they are in
  # separate batches on purpose even though a naive reading says their expected
  # sets differ. They do not really: either one alone restores "both differ arms
  # stamp .rediffSlot", so batching them would let one fix be credited twice.
  #
  # G03 is the mirror — arms swapped — and exists because a rail that only
  # checks "chroma is not byte-exact" is satisfied by a build where NOTHING is
  # byte-exact. The byte arm has to be pinned in its own right or the whole
  # rediff auto-skip lane could be silently retired and the G series would still
  # come back green.
  "G01|160|ADSVC|$T_6QVF_E2E_CHROMA;$T_6QVF_E2E_REMOTE;$T_6QVF_OWNERSHIP_E2E"

  "G02|161|ADSVC|$T_6QVF_E2E_CHROMA;$T_6QVF_E2E_REMOTE;$T_6QVF_OWNERSHIP_E2E"

  # Judged by the persisted-anchor test alone. The obvious companion —
  # `byte-success: …` in the same suite — carries semicolons in its display
  # name, which this script uses as its expected-test separator, so naming it
  # here makes the mutation unevaluable rather than killed. The anchor test is
  # the stronger claim anyway: it reads the tier off the persisted ad_windows
  # row rather than off the in-memory span.
  "G03|162|ADSVC|$T_6QVF_E2E_ANCHORS"

  # The predicate layer. G04 is the collapse restated as a one-line
  # "simplification"; G05 is the copy-paste that makes the chroma accessor
  # report the byte marker (which would make every chroma diagnostic read as
  # empty and the rails read as passing for the wrong reason).
  "G04|163|DSPAN|$T_6QVF_PRED_CHROMA;$T_6QVF_UNANCHORED;$T_6QVF_RAIL_SWEEP;$T_6QVF_FLOOR_CHROMA"

  # Judged by the two SINGLE-marker tests. `predicatesAreIndependent` builds a
  # span carrying BOTH markers, so under this mutation both properties still
  # answer true and it stays green — it is a good test of a different claim and
  # a useless expectation here.
  "G05|164|DSPAN|$T_6QVF_PRED_CHROMA;$T_6QVF_PRED_BYTE"

  # Persistence. G06 is the UNSAFE direction specifically: encoding chroma under
  # the byte type string is the one migration mistake an older binary cannot
  # detect — it would decode the value as byte-exact rather than dropping it.
  "G06|165|DSPAN|$T_6QVF_TYPESTRING;$T_6QVF_ROUNDTRIP"

  # G07 is the extent tier, mutated at the derivation rather than at the
  # predicate: `isWidthOwnership` is the plausible wrong key, and it is exactly
  # the one that re-admits chroma to `deterministic` and to the qs0d padding
  # lane.
  "G07|166|EXTENT|$T_6QVF_RAIL_SWEEP;$T_6QVF_RAIL_MIXED;$T_6QVF_RAIL_LANE;$T_6QVF_RAIL_MARGIN;$T_6QVF_UNANCHORED"

  # Ownership must SURVIVE the certainty split. G08 is the over-correction: drop
  # chroma from `isWidthOwnership` and a chroma-owned span stops bypassing the
  # boundary refiners and stops being protected from the Phase-5 projector's
  # clobber guard — strictly worse than the bug being fixed.
  "G08|167|ADSVC_ATOM|$T_6QVF_OWNS_WIDTH;$T_6QVF_THREE_MARKERS"

  # G09 is the default:false trap that `.spliceSlot` and `.rediffSlot` each have
  # a comment about. With the arm gone, `.rediffSlotChroma != .rediffSlotChroma`
  # and every `contains(_:)` BY VALUE silently misses a chroma-owned span — in
  # the pure predicate and all the way out at the service boundary.
  #
  # Expected set corrected after the first run: it named `isWidthOwnership` and
  # the `!=` sweep, neither of which this mutation can reach. `isWidthOwnership`
  # is a predicate closure, and the sweep asserts `.rediffSlot != <other>`, which
  # a broken chroma-vs-chroma comparison leaves true. The battery is what
  # established that, and the source comment on the arm was corrected to match.
  "G09|168|ADSVC_ATOM|$T_6QVF_ROUNDTRIP;$T_6QVF_PRED_CHROMA;$T_6QVF_E2E_CHROMA"

  # G10 is the CONSUMER half of the "all six move together" constraint: re-grant
  # the host-read floor exemption to any width oracle. It is the mutation the
  # bead's hard constraint exists to forbid, and the only test that sees it is
  # the discriminating negative added by this bead.
  "G10|169|FUSION|$T_6QVF_FLOOR_CHROMA"

  # ---- playhead-9v09 (H series): the census's silent retraction path --------
  #
  # Batching note. Each batch pairs ONE orchestrator-wiring mutation with ONE
  # value-type mutation, because the two files cannot interact and their
  # expected sets are disjoint. Two wiring mutations are never batched together:
  # they all edit the same twenty lines of
  # `reapplyInventoryFilterToManagedWindows`, which is precisely the "two edits
  # to the same loop" case this file keeps in separate batches.
  #
  # H01 deletes the STAMP and H02 the ROW. They are separate defects and each is
  # separately survivable: the row is built from locals, so deleting the stamp
  # leaves every row assertion green, and deleting the row leaves every counter
  # assertion green. A battery that only mutated one of them would certify the
  # other by association.
  "H01|170|ORCH|$T_9V09_NEGATIVE;$T_9V09_MANAGED;$T_9V09_BALANCE;$T_9V09_LIFETIME"

  # H06 also reddens the two row tests, because a `retiredCount` keyed on the
  # wrong classifier renders no `retired=` at all. Both are named so the KILL
  # cannot be credited to its batchmate.
  "H06|170|INGO|$T_9V09_RENDER;$T_9V09_BOTH;$T_9V09_AGGREGATE"

  "H02|171|ORCH|$T_9V09_BOTH;$T_9V09_MANAGED;$T_9V09_AGGREGATE;$T_9V09_NEGATIVE"

  # The rendering asymmetry is deliberate and load-bearing: a delivery row is
  # written the instant the delivery concludes, so `retired=0` there would be a
  # claim it is in no position to make. Only the pure test sees this.
  "H07|171|INGO|$T_9V09_RENDER"

  # H03 drops the REASON from the row, H04 from the stamp. Same information,
  # two independent carriers — and a field investigation reads the row while a
  # test reads the stamp, so neither covers the other.
  "H03|172|ORCH|$T_9V09_BOTH;$T_9V09_MANAGED;$T_9V09_AGGREGATE"

  "H08|172|INGO|$T_9V09_ONE_RETRACTION;$T_9V09_NOT_BOTH"

  "H04|173|ORCH|$T_9V09_MANAGED"

  # The classification error that would let a retraction inflate `delivered=` —
  # the numerator check of playhead-aqo9's "what would this read if the thing
  # never happened?", applied to the audit row's own headline.
  "H09|173|INGO|$T_9V09_THREE_DELIVERED;$T_9V09_NOT_BOTH;$T_9V09_BOTH"

  # THE NEGATIVE, and the mutation the bead's second acceptance exists for: a
  # sweep that retires everything it looks at. Alone in its batch because its
  # blast radius covers most of the behavioural suite — it also reddens
  # T_9V09_AGGREGATE, which is read, not batched around.
  "H05|174|ORCH|$T_9V09_NEGATIVE;$T_9V09_BALANCE;$T_9V09_BOTH"

  # `forwarded` and the summed counts must agree on the retraction row too, or
  # the existing "a gap means a missing instrumentation site" reading of the
  # census silently stops holding for the new row shape.
  "H10|175|ORCH|$T_9V09_BOTH;$T_9V09_AGGREGATE"
  # ---- playhead-gard (I series): trust is per detector class ---------------
  #
  # Batching note. A batch pairs at most one PURE-TYPE mutation with one WIRING
  # mutation, and only where neither can redden the other's tests. Six of the
  # twenty run alone because their blast radius covers the acceptance suite —
  # anything that changes what `.rediffByteExact` seeds to, or whether the
  # ledger persists at all, reddens the orchestrator rails too, and a false
  # KILL is worse than an extra build.

  # THE DECISION, deleted. If the exempt class consults the show's history
  # again, Dan's three aggregator vetoes gate the byte differ once more — the
  # defect exactly as it shipped.
  "I01|180|DETCLS|$T_GARD_EXEMPT_SINGULAR;$T_GARD_FIELD_ROW;$T_GARD_MIGRATION_FREES"

  # The opposite mis-scoping: nobody consults show trust. Every show-governed
  # class would seed clean and an upgrading user would silently GAIN auto-skip
  # on three detectors — the migration failure this bead is obliged to state.
  "I02|181|DETCLS|$T_GARD_EXEMPT_SINGULAR;$T_GARD_MIGRATION_KEEPS"

  # A veto blames nobody. The gesture still moves the show scalar, so every
  # pre-gard assertion stays green and only the attribution rail sees it.
  "I13|181|ORCH|$T_GARD_REVERT_ATTRIB"

  # Deterministic on EITHER edge. A span with an invented end would be treated
  # as byte-exact — the playhead-2350 lesson ("a span is only as well-bounded
  # as its weaker edge") re-broken one layer up.
  "I03|182|DETCLS|$T_GARD_ONE_EDGE"

  # The escape from `manual`, unwired. This is the shipped state before gard:
  # the ladder is intact and nothing climbs it.
  "I12|182|ORCH|$T_GARD_CALL_SITE;$T_GARD_BANNER_CREDITS"

  # The conservative fallback inverted: an unrecognised producer is admitted to
  # the exempt class. A future boundary state nobody has taught this enum about
  # would skip on a show that trusts nothing.
  "I04|183|DETCLS|$T_GARD_UNKNOWN_BOUNDARY;$T_GARD_STINGER;$T_GARD_ROW_CLASSIFY"

  # A live instruction that governs only some detectors — "exempt from the
  # show's history" widened into "exempt from the user".
  "I20|183|ORCH|$T_GARD_SESSION_OVERRIDE"

  # The weighting removed: every veto weighs 1 again, and the field case
  # demotes on the third junk span exactly as it did.
  "I05|184|DETLED|$T_GARD_WEIGHT_ORDER;$T_GARD_FIELD_ARITHMETIC;$T_GARD_WEIGHTED_STAYS;$T_GARD_LEGACY_ONCE"

  # THE SHIPPED DEFECT, restored at the wire: one scalar gates every window.
  "I11|184|ORCH|$T_GARD_REDIFF_SKIPS"

  # The migration's exempt arm removed — the poisoned legacy scalar is
  # inherited by the one class it is not evidence about.
  "I06|185|DETLED|$T_GARD_MIGRATION_FREES;$T_GARD_FIELD_ROW;$T_GARD_NEW_SHOW;$T_GARD_REDIFF_SKIPS"

  # The migration's other arm: every class seeds clean, so an upgrading user
  # silently gains auto-skip on the detector that just cost them 210 s.
  "I07|186|DETLED|$T_GARD_MIGRATION_KEEPS;$T_GARD_FIELD_ROW;$T_GARD_AGG_BLOCKED"

  # The ledger never persists. Per-detector state exists in memory for one
  # transaction and is gone — indistinguishable from working, for one call.
  #
  # EXPECTATION CORRECTED after the first I run, which is what established what
  # this mutation can actually reach. It originally named the demotion and the
  # escape; both SURVIVE it, and correctly so. With no persisted ledger every
  # class re-seeds from the LEGACY triple on each read, and the legacy triple
  # demotes and escapes on exactly the same thresholds — so a single-detector
  # story still reads right. What the mutation cannot fake is anything that
  # requires the ledger to hold state the scalar does not: a round-trip, blame
  # that is NOT shared, and the per-class weights after one gesture.
  "I08|187|DETLED|$T_GARD_ROUNDTRIP;$T_GARD_BLAME_NOT_SHARED;$T_GARD_LEGACY_ONCE"

  # THE NEGATIVE the bead's third acceptance exists for: per-detector trust
  # that never demotes anything. Every positive rail stays green.
  "I09|188|TRUST|$T_GARD_DEMOTION_HAPPENS;$T_GARD_EXEMPT_DEMOTABLE;$T_GARD_WEIGHTED_STAYS"

  # THE ONE-WAY DOOR, restored. A correct observation stops decaying the
  # false-signal evidence, so `recentFalseSkipSignals` never reaches 0 and no
  # show can ever leave `manual` — the shipped behaviour this bead measured.
  "I10|189|TRUST|$T_GARD_DOOR_OPENS;$T_GARD_DECAY_ONE"

  # An explicit user instruction that leaves the stale counters standing, so
  # the very next veto undoes it.
  "I15|189|TRUST|$T_GARD_OVERRIDE_CLEARS"

  # Materialization removed. The subtle half: the seed is a lazy read off a
  # legacy scalar that this same gesture demotes, so blame leaks back into
  # every unwritten class one hop later.
  "I14|190|TRUST|$T_GARD_BLAME_NOT_SHARED;$T_GARD_CREDIT_NOT_SHARED"

  # Dedup keeps the WEAKEST tier, so one junk span in a range veto launders a
  # real miss down to its own weight.
  "I16|191|TRUST|$T_GARD_STRONGEST_TIER"

  # An inferred revert weighs as much as an explicit one — the fidelity ladder
  # flattened.
  "I17|191|TRUST|$T_GARD_WEAK_HALVED"

  # Per-detector resolution made inert: every class answers with the show mode.
  # Compiles, ships, and is the scalar again.
  #
  # EXPECTATION CORRECTED after the first I run: it also named the new-show
  # rail, which this edit cannot reach — a show with NO profile row exits
  # `resolveDetectorModes` one branch earlier, through `seededModes(from: nil)`.
  # That branch gets its own mutation (I21) rather than being credited to this
  # one by association.
  "I18|192|TRUST|$T_GARD_FIELD_ROW;$T_GARD_STORED_WINS"

  # The FIRST-LISTEN branch, which is the one that matters most for the exempt
  # class: day-0 byte-exact rediff is the only signal an unheard show has, and
  # an empty map sends it to the show mode, which is `.shadow` by definition
  # there. Separate from I18 because it is a separate `return`.
  "I21|194|TRUST|$T_GARD_NEW_SHOW"

  # A lookup FAILURE grants the exempt class auto. "Exempt from the show's
  # history" widened into "runs when persistence is broken" — playhead-djl0's
  # rule that every failure lands non-actioning.
  "I19|193|TRUST|$T_GARD_LOOKUP_FAILURE"

  # playhead-mptr — K2 series, the artifact-backed shard skip. Four batches;
  # K201 and K208 patch the SAME guard line and so can never share one.
  "K201|200|MPTRIDX|$T_MPTR_WATERMARK_RAIL"
  "K203|200|MPTRIDX|$T_MPTR_TOUCHING_MERGE"
  "K209|200|MPTRIDX|$T_MPTR_HALF_OPEN_START"
  "K202|201|MPTRIDX|$T_MPTR_ARTIFACT_RAIL"
  "K204|201|MPTRIDX|$T_MPTR_DEGENERATE_DROPPED"
  "K205|202|MPTRIDX|$T_MPTR_HALF_OPEN_END"
  "K210|202|MPTRIDX|$T_MPTR_UNCOVERED_FIRST"
  "K206|202|STORE|$T_MPTR_PASS_FILTER"
  "K207|203|STORE|$T_MPTR_SQL_DEGENERATE"
  "K208|203|MPTRIDX|$T_MPTR_WATERMARK_INCLUSIVE"

  # playhead-hx6n — SCAN-ROW RUN ATTRIBUTION. Fifteen entries, batches 210-216.
  #
  # WHAT SETS THE BATCH FLOOR HERE, since it is not the expectation lists. Four
  # of these mutations do not merely redden their own rail — they redden
  # `T_HX6N_SQL_AGREES` as a SIDE EFFECT, because the documented SQL and the
  # shipped Swift consumer are cross-checked on one fixture and any change to
  # the bucketing arithmetic moves both sides. Batching is therefore done on
  # each mutation's TOTAL blast radius, not on the tests it is declared to kill:
  # a batch-mate whose side effect reddens another's expected test would be
  # scored a false KILL, which is worse than an extra build. Seven batches is
  # what that constraint permits; four are pairs, two are triples, and T15 runs
  # alone because every other sqlAgrees mutation was already spoken for.
  "T01|210|SPLIT|$T_HX6N_NIL_BUCKET;$T_HX6N_MIXED_CORPUS;$T_HX6N_ALL_UNATTRIBUTED;$T_HX6N_V41_SURVIVES"
  "T10|210|RUNNER|$T_HX6N_BROKEN_PROVIDER"

  "T02|211|SPLIT|$T_HX6N_UNKNOWN_BUCKET;$T_HX6N_SQL_AGREES"
  "T05|211|STORE|$T_HX6N_STORE_STAMPS"

  "T03|212|STORE|$T_HX6N_V41_SURVIVES;$T_HX6N_STORE_STAMPS"
  "T13|212|SPLIT|$T_HX6N_EMPTY_CORPUS;$T_HX6N_ALL_UNATTRIBUTED"

  "T04|213|STORE|$T_HX6N_STORE_STAMPS"
  "T14|213|SPLIT|$T_HX6N_EMPTY_CORPUS"

  # T09 gets a batch of its own, MEASURED not assumed. It first shared 213 with
  # T04 and came back SURVIVED — falsely. T09 makes the runner stamp a nil
  # phase; T04 makes the store rewrite a nil phase to `.active` at the write.
  # Together they persist exactly the `.active` that
  # `T_HX6N_FOREGROUND_RUN` asserts, so one of T09's two expected tests stayed
  # green and the battery scored it a survivor. `T_HX6N_RUNNER_STAMPS` did fail,
  # which is what makes this diagnosable rather than mysterious.
  #
  # The lesson generalises past this entry: batching has to consider whether a
  # batch-mate can REPAIR another mutation's damage, not only whether it can
  # cause the same failure. A rescue is invisible to a disjoint-expectations
  # check, and it manufactures a coverage hole that does not exist.
  "T09|217|RUNNER|$T_HX6N_RUNNER_STAMPS;$T_HX6N_FOREGROUND_RUN"

  "T06|214|STORE|$T_HX6N_V41_SURVIVES"
  "T11|214|SPLIT|$T_HX6N_INELIGIBLE;$T_HX6N_SQL_AGREES"
  "T08|214|RUNNER|$T_HX6N_RUNNER_STAMPS"

  "T07|215|STORE|$T_HX6N_LADDER_RAIL"
  "T12|215|STORE|$T_HX6N_SQL_AGREES"

  "T15|216|STORE|$T_HX6N_SQL_AGREES"

  # playhead-bllt — THE HOT PATH'S PRESENCE-vs-EXTENT GATE. Ten entries,
  # batches 240-247.
  #
  # WHAT SETS THE BATCH FLOOR, since it is not the expectation lists. Five of
  # these mutations edit ONE predicate in `HotPathExtentGate.gatedLabel`, so no
  # two of them can share a batch at all: whichever lands first destroys the
  # other's anchor. That is the floor, and it is the same reason the D series
  # runs one-per-batch. The two that CAN pair (BL06+BL07, BL09+BL10) do, and
  # both pairings were checked for the rescue hazard the T09 note describes as
  # well as for overlapping expectations — BL06 and BL07 are different producers
  # whose rows are asserted separately, and BL10 edits a helper BL09's site does
  # not call on the path BL09 breaks.
  "BL01|240|HOTGATE|$T_BLLT_ONLY_TRANSITION;$T_BLLT_COUNT;$T_BLLT_SINGLE_SHIPPED;$T_BLLT_AGG_SHIPPED"
  "BL02|241|HOTGATE|$T_BLLT_NOT_A_DROP;$T_BLLT_ONLY_TRANSITION"
  "BL03|242|HOTGATE|$T_BLLT_ONLY_TRANSITION;$T_BLLT_ANCHORED;$T_BLLT_SINGLE_SHIPPED"
  "BL04|243|HOTGATE|$T_BLLT_ANCHORED;$T_BLLT_COUNT;$T_BLLT_ONLY_TRANSITION"
  "BL05|244|HOTGATE|$T_BLLT_KILL_SWITCH;$T_BLLT_SINGLE_PRESENCE;$T_BLLT_AGG_PRESENCE"
  "BL06|245|ADSVC|$T_BLLT_SINGLE_SHIPPED"
  "BL07|245|ADSVC|$T_BLLT_AGG_SHIPPED"
  "BL08|246|ADSVC|$T_BLLT_SINGLE_SHIPPED"
  "BL09|247|ORCH|$T_BLLT_SINGLE_SHIPPED;$T_BLLT_AGG_SHIPPED"
  "BL10|247|HOTGATE|$T_BLLT_DETAIL"

  # playhead-ug9m — THE DAY-0 RESCUE. Fourteen entries, batches 250-260.
  #
  # WHAT SETS THE BATCH FLOOR. UG01-UG05 all edit predicates inside
  # `DayZeroMarkCensus`, and UG02/UG03/UG04 edit the SAME expression
  # (`isSupersedable`), so those three cannot share a batch — whichever lands
  # first destroys the others' anchors. UG06-UG09 are four separate sites in
  # `DayZeroRediffAttemptPolicy`; UG06 and UG08 pair (different functions,
  # disjoint expectations, and neither can REPAIR the other's damage — the T09
  # hazard was checked). UG10/UG11 are the two mint sites and are deliberately
  # split, because UG10 makes the supersede over-fire and UG11 makes it
  # under-fire: batched together they would cancel and both report SURVIVED.
  "UG01|250|UGCEN|$T_UG9M_RESCUABLE;$T_UG9M_TRIGGER_STOPS"
  "UG05|250|UGCEN|$T_UG9M_HALF_ANCHORED"
  "UG02|251|UGCEN|$T_UG9M_SUPERSEDE_NARROW;$T_UG9M_ANCHORED_ROW"
  "UG03|252|UGCEN|$T_UG9M_SUPERSEDE_NARROW;$T_UG9M_VETO"
  "UG04|253|UGCEN|$T_UG9M_SUPERSEDE_NARROW;$T_UG9M_FOREIGN_PRODUCER"
  "UG06|254|POLICY|$T_UG9M_TERMINAL_IN_GEN"
  "UG08|254|POLICY|$T_UG9M_ADVANCE;$T_UG9M_EXACTLY_ONE"
  "UG07|255|POLICY|$T_UG9M_EXACTLY_ONE;$T_UG9M_NAMED;$T_UG9M_TRIGGER_NAMED"
  "UG09|256|POLICY|$T_UG9M_CEILING"
  "UG10|257|ADSVC|$T_UG9M_RECOVERED_NOT_PROMOTED"
  "UG11|258|ADSVC|$T_UG9M_STRICT_SUPERSEDES"
  "UG12|258|UGCEN|$T_UG9M_FREEZE_REPORT"
  "UG13|259|STORE|$T_UG9M_CONTEXT;$T_UG9M_STORE_WIRING"
  "UG14|260|UGCEN|$T_UG9M_VETOED_ANCHORED"

  # playhead-pggn — THE SCORE DESCRIBES THE SPAN IT REPORTS. Eight entries,
  # batches 270-276.
  #
  # WHAT SETS THE BATCH FLOOR. PG01-PG06 all live inside the same ~40 lines of
  # `ingestIntoOpenSegment` / `flushOpenSegment`, and the masking hazard is real
  # rather than theoretical: PG01 removes the defer entirely, so a PG02-PG06
  # mutation sharing its batch would be judged on a code path that no longer
  # runs and would be credited a KILL it did not earn. PG03 and PG05/PG06 are
  # the same hazard in the other direction — PG03 drops what PG05/PG06 fold, so
  # together they partially cancel.
  #
  # The two that CAN share: PG07 edits `dropPendingTail`, reached by every exit,
  # and PG08 is in a different FILE. Neither can repair the other and their
  # expectations are disjoint (PG07 moves every score, PG08 moves only the
  # persisted anchors), so they ride together in 276.
  #
  # RUN 2026-08-03: 8 KILLED / 0 SURVIVED / 0 ERROR, after a first pass that
  # reported PG02, PG05 and PG07 SURVIVED. Recorded because two of those three
  # were holes in the NEW rails and the shapes recur:
  #   • PG02 — the rail ended on a QUALIFYING window, which folds a wrongly
  #     deferred window straight back in, so correct and inverted behaviour were
  #     byte-identical. A containment rail has to CLOSE the segment right after
  #     the window it is arguing about.
  #   • PG05 — the witness was GLOBAL (`strictImprovements > 0`) and the
  #     short-tail cases satisfied it through the FLUSH exit while the COUNTDOWN
  #     exit was the one mutated. A witness any path can satisfy proves nothing
  #     about the path under test; it is now `== comparisons`.
  #   • PG07 — NOT a hole. The first edit put the extra term in `meanScore`,
  #     which every exit reaches only after the tail is settled, so it was inert
  #     and could not have been killed by anything. Re-aimed at
  #     `dropPendingTail`, where the tail still has a value.
  # PG08 was predicted vacuous and is not: `eligibilityGate` cannot see it (no
  # safety signal fires on that fixture, so the PRECISION gate withholds
  # autoSkip on its own) but the two edge-anchor witnesses can.
  "PG01|270|SEGAGG|$T_PGGN_WORKED;$T_PGGN_TAIL_VALUE;$T_PGGN_PERSISTED"
  "PG02|271|SEGAGG|$T_PGGN_WORKED;$T_PGGN_ALREADY_INSIDE"
  "PG03|272|SEGAGG|$T_PGGN_FOLD_LATER"
  "PG04|273|SEGAGG|$T_PGGN_TAIL_WIDTH"
  "PG05|274|SEGAGG|$T_PGGN_WORKED;$T_PGGN_TAIL_VALUE;$T_PGGN_NEVER_LOWERS"
  "PG06|275|SEGAGG|$T_PGGN_GAP_CLOSE"
  "PG07|276|SEGAGG|$T_PGGN_WORKED;$T_PGGN_TAIL_VALUE;$T_PGGN_TAIL_WIDTH"
  "PG08|276|ADSVC|$T_PGGN_BLLT"

  # ---- playhead-kkzu: the show a background download carries (KZ series) ----
  #
  # WHAT SETS THE BATCH FLOOR. KZ01/KZ02 both destroy the attribution that
  # reaches the enqueue — one at the write, one at the read — so together they
  # would be judged on a path where the identity is already gone and the second
  # would be credited a kill it did not earn. KZ04/KZ05 both edit the private
  # DownloadContext initializer and would collide textually. KZ06/KZ07 patch
  # the SAME line in the auto-download enqueue. Each of those pairs is split.
  #
  # RUN 2026-08-03: 11 KILLED / 0 SURVIVED / 0 ERROR, one mutation per batch.
  # Three things worth keeping:
  #   • The first attempt reported KZ01 as ERROR — "an expectation names a test
  #     that never ran" — because the T_KKZU_* constants were DESCRIPTIONS of the
  #     claim rather than the @Test display names. The guard caught it at the
  #     BASELINE, before a build was spent on a mutant. They are display names now.
  #   • KZ02 (never persist the record) failed SIX rails, not the three it names.
  #     Dropping the write breaks every attribution rail at once, which is the
  #     shape you want from the mutation that removes the mechanism itself.
  #   • KZ07 is the one a lazy rail would miss. It attributes every auto-download
  #     to the FIRST group's feed, so "podcastId is not nil" stays true for every
  #     row while real episodes are filed under the wrong show. It is killed only
  #     by the per-episode half of the auto-download rail.
  # Batch 285 also ERRORed once on rc=28 — the disk preflight refusing, not a
  # surviving mutant. `simctl erase` + clearing $TMPDIR/Deleting-* took the volume
  # from 14 GiB to 20 GiB and it passed on the re-run.
  "KZ01|280|DLMGR|$T_KKZU_ENQUEUE;$T_KKZU_RESTART"
  "KZ02|281|DLMGR|$T_KKZU_ENQUEUE;$T_KKZU_REAP;$T_KKZU_RESTART"
  "KZ03|282|FQSCAN|$T_KKZU_RESUME_KEEPS;$T_KKZU_FORCE_QUIT"
  "KZ04|283|DLMGR|$T_KKZU_BLANK"
  "KZ05|284|DLMGR|$T_KKZU_NAMED"
  "KZ06|285|BGFEED|$T_KKZU_AUTO"
  "KZ07|286|BGFEED|$T_KKZU_AUTO"
  "KZ08|287|EPPREP|$T_KKZU_PREPARE"
  "KZ09|288|FQSCAN|$T_KKZU_FORCE_QUIT"
  "KZ10|289|DLMGR|$T_KKZU_CANCEL"
  "KZ11|290|DLMGR|$T_KKZU_CLEAR"

  # ---- playhead-kanf: a user-intent tap promotes a queued job (KN series) ----
  #
  # ONE MUTATION PER BATCH, and here that is not caution but necessity: KN01,
  # KN02 and KN03 all rewrite the SAME guard, and KN06 and KN08 the same call.
  # More importantly the outcomes interfere — KN01 makes everything promotable
  # and KN04 makes nothing already-served, so run together each would be
  # credited with failures the other caused.
  #
  # WHAT IS ACTUALLY BEING DEFENDED. The bead's promise ("the tap promotes")
  # and its constraint ("never a leased or running row") pull in opposite
  # directions, and a fix that satisfies only the first passes every promotion
  # rail. So the centrepiece is KN01/KN02/KN03 — the refusals — and the
  # promotion rails exist mostly to keep those from being vacuous.
  #
  # The lease defence is deliberately DOUBLE: a Swift guard that classifies
  # (so the caller learns *why*) and a SQL predicate that is its own
  # compare-and-swap. Every mutation that attacks it therefore patches BOTH
  # sites — a single-site mutation survives on the other's back, which would be
  # a report about redundancy rather than about coverage.
  #
  # RUN 2026-08-03: 9 KILLED / 0 SURVIVED / 0 ERROR, one mutation per batch,
  # baseline green. Kill-set sizes, in order: 8, 1, 5, 3, 1, 5, 1, 5, 2.
  # Three things worth keeping:
  #   • "a refused promotion RETAINS the flag" failed under SIX of the nine
  #     (KN01/03/05/06/07/08), and is the SOLE killer of KN05 and KN07. That is
  #     the shape you want from a rail guarding a token spent exactly once — and
  #     it is the only rail that can see those two at all, because the flag is
  #     private in-memory state whose survival is observable only as the priority
  #     a LATER enqueue lands at.
  #   • KN02 has a kill set of ONE: the store-level rail that puts a stale lease
  #     on a still-`queued` row. That row is the only witness that
  #     `leaseOwner IS NULL` does work `state = 'queued'` does not — precisely
  #     the case the bead's own wording ("guard to state='queued'") would have
  #     left uncovered. It looks like a contrived state and is not: every lease
  #     expiry produces it.
  #   • KN01 fails eight rails including the flag one, because a guard that
  #     refuses nothing also makes every refusal "served". The blast radius is
  #     the tell that it removed the mechanism rather than bending it.
  "KN01|300|STORE|$T_KANF_LEASED;$T_KANF_RUNNING;$T_KANF_PAUSED;$T_KANF_TERMINAL;$T_KANF_PRODLEASE;$T_KANF_TAP_LEASED;$T_KANF_TAP_RUNNING"
  "KN02|301|STORE|$T_KANF_LEASED"
  "KN03|302|STORE|$T_KANF_RUNNING;$T_KANF_PAUSED;$T_KANF_TERMINAL;$T_KANF_TAP_RUNNING"
  "KN04|303|STORE|$T_KANF_ATFLOOR;$T_KANF_RUNATFLOOR;$T_KANF_RETAP"
  "KN05|304|SCHED|$T_KANF_FLAG_KEPT"
  "KN06|305|SCHED|$T_KANF_TAP;$T_KANF_IDENT;$T_KANF_TAP_LEASED;$T_KANF_FLAG_KEPT"
  "KN07|306|SCHED|$T_KANF_FLAG_KEPT"
  "KN08|307|SCHED|$T_KANF_TAP;$T_KANF_TAP_LEASED"
  "KN09|308|STORE|$T_KANF_NUDGE;$T_KANF_IDENT"

  # ---- playhead-5n8k: the coarse-pass diagnostic observer is per-test (TS) ----
  #
  # WHAT IS BEING DEFENDED. Not a product behaviour — the observer is nil in
  # every production build. What is defended is the INTEGRITY OF A VERDICT: as a
  # `nonisolated(unsafe) static var` the hook was a shared mailbox, so any other
  # suite's coarse pass landed in whichever capture box happened to be installed.
  # The resulting failure was FAST (0.03-0.27s), which is the shape gate triage
  # reserves for REAL defects, so it cost a full triage round every time it
  # appeared and looked like a regression in whatever was under test.
  #
  # ALL FOUR ATTACK THE SAME TWO FUNCTIONS — `withCoarsePassDiagnosticObserver`
  # (the only install) and `emitCoarsePassDiagnostic` (the only read). Those two
  # exist so that these mutations have one anchor apiece instead of five, and so
  # a regression cannot come back through whichever of the five emit sites a
  # mutation happened not to pick. One batch each, because the outcomes
  # interfere: TS03 makes every box empty and TS01 makes one box overfull, so
  # run together each would be credited with the other's failures.
  #
  # TS01 and TS02 are two DIFFERENT process-global regressions — assign-and-
  # restore, and cache-and-leak. The listed expectations are the rails each one
  # must fail on the mechanism alone, and they differ: TS01's save/restore is
  # CORRECT inside a single task, so the lifetime rails (scope, nesting) cannot
  # see it and only the concurrency ones can; TS02 is the mirror image.
  #
  # MEASURED 2026-08-03, and it did not match that prediction: each mutation
  # failed ALL FIVE tests, including the two the analysis said it could not
  # reach. The reason is the finding, not a flaw — once ANY process-global
  # exists, the scoping suite is no longer alone with it. It runs concurrently
  # with the bd-34e consumer test, which installs an observer of its own, so
  # every rail acquires cross-talk it cannot control. That is the whole bug
  # restated: a shared hook does not fail where you predict, it fails wherever
  # two things happen to overlap, and the verdict moves run to run.
  #
  # The expectations are kept NARROW rather than widened to the observed five.
  # A kill set produced by incidental overlap is not a claim this file should
  # make: the named rails are the ones that fail on the mutation's own
  # mechanism, and those are the ones whose passing would mean the defect
  # returned undetected.
  #
  # TS03 and TS04 are the vacuity controls. Every rail here is built as "my own
  # windows arrived AND nothing else did"; without a mutation that empties every
  # box, the second half would be passing on an observer that never fires.
  #
  # RUN 2026-08-03: 4 KILLED / 0 SURVIVED / 0 ERROR, one mutation per batch,
  # baseline green each time. All four failed the same five tests. One
  # operational note worth keeping, because it is not about a mutation: TS03 and
  # TS04 first came back ERROR ("batch did not build/run", rc=28) — that is the
  # DISK PREFLIGHT refusing, not an anchor that stopped applying. The baseline
  # started at 13.74 GiB free and the destination simulator had grown to 6.7 GiB
  # across the earlier batches, leaving 7.84. Erase the sim between batteries
  # (and clear `$TMPDIR/Deleting-*`, which is where erase actually puts it);
  # do not go looking for a broken EDIT.
  "TS01|310|FMCLS|$T_5N8K_FOREIGN;$T_5N8K_CONCURRENT"
  "TS02|311|FMCLS|$T_5N8K_FOREIGN;$T_5N8K_SCOPE"
  "TS03|312|FMCLS|$T_5N8K_FOREIGN;$T_5N8K_CONCURRENT;$T_5N8K_SCOPE;$T_5N8K_NESTED;$T_5N8K_BD34E"
  "TS04|313|FMCLS|$T_5N8K_FOREIGN;$T_5N8K_CONCURRENT;$T_5N8K_SCOPE;$T_5N8K_NESTED;$T_5N8K_BD34E"

  # ---- playhead-26od: the mid-flight coarse checkpoint (CK series) ----
  #
  # WHY THESE TWELVE. The bead's whole value is that a pass killed mid-flight
  # keeps its screened windows, and every rail that makes that SAFE is invisible
  # in the rows a healthy pass leaves: a full pass never defuses mid-flight,
  # never fails only SOME of its writes, and never sees an ambiguous or empty
  # line-ref list. Seven of the twelve were, at one review round or another,
  # deletable with the entire suite green — CK01 most starkly, because the
  # production factory always supplies a router, so the overload every test but
  # one exercised was the one a shipped build never calls.
  #
  # ONE MUTATION PER BATCH wherever two edits share a function: CK02/CK03 are
  # the two defuse guards inside `checkpointCoarseWindows`, and CK05/CK06 are two
  # edits to `planIndex`, so running them together would let each be credited
  # with the other's failures.
  # CK07 and CK13 are likewise split: CK07 removes the lease touch's lifetime
  # BOUND and CK13 removes the touch itself, and CK13 subsumes CK07's failure.
  # CK04+CK07, CK09+CK10 and CK11+CK12 share a batch each: disjoint statements,
  # disjoint expectations.
  #
  # CK05 IS ONLY KILLABLE WITH A ONE-PLAN FIXTURE, and that is not a fixture
  # detail — it is the interaction between two rails. The empty needle is a
  # subset of EVERY plan, so with two or more plans CK06's ambiguity guard
  # already returns nil and the empty guard is redundant. Exactly one plan is the
  # only shape where the empty set has a UNIQUE superset. The multi-plan fixture
  # this test shipped with in R3 would have let CK05 SURVIVE.
  #
  # NOT REGISTERED, and MEASURED rather than inferred: the vacuity control for
  # CK06. "Credit nobody when it is ambiguous" is one `guard` away from "credit
  # nobody, ever", so the obvious control is `planIndex` returning nil
  # unconditionally. That mutation DOES NOT TERMINATE, and the reason is now
  # named rather than guessed: with no plan ever credited the coverage cursor
  # never advances, and
  # `the attempt budget counts CONSECUTIVE barren windows — a window that covers
  # new audio RESETS it` (playhead-bkhc's re-drive bound) drives a runner until
  # the cursor advances. Under this mutant it never does.
  #
  # Three observations, at 10, 17 and 17 GiB free, each freezing its batch log at
  # the same ~1.159 MB with xcodebuild alive at 0.0 %% CPU — so NOT the disk wedge
  # the first one was read as. The hung test was identified by diffing `started`
  # against `passed|failed` in the frozen log; 15 cases were outstanding and that
  # one is the only re-drive loop among them.
  #
  # A mutation that hangs is worse than one that survives, because it takes the
  # tool down with it rather than reporting. The control itself is not lost —
  # `a strict subset of exactly one plan still credits that plan` is in the suite
  # and fails against a degrade-to-nothing implementation. What is lost is a
  # REGISTERED mutant that kills it. Reviving it needs a terminating variant
  # (bound the re-drive loop, or mutate only the SUCCESS attribution and leave
  # the failure fallback intact), which is a test-harness change rather than a
  # battery entry.
  #
  # NOT REGISTERED, deliberately: `defer { checkpointBox.defuse() }` itself.
  # Deleting it is observable only from a pass that outlives `runJob`, which
  # only `FMNoProgressWatchdog` produces and only on wall-clock, so the test
  # that would kill it is PerfGate'd out of the plans this script runs. Naming
  # it here would register a permanent SURVIVOR, which the KNOWN GAP note below
  # explains is worse than not naming it.
  "CK01|314|RUNNER|$T_26OD_PROD_SHAPE"
  "CK02|315|RUNNER|$T_26OD_MIDLOOP_DEFUSE"
  "CK03|316|RUNNER|$T_26OD_LATE_DEFUSE"
  "CK04|317|RUNNER|$T_26OD_WRITE_COST"
  "CK07|317|RUNNER|$T_26OD_LEASE_GUARD"
  "CK05|318|RUNNER|$T_26OD_EMPTY_REFS"
  "CK06|319|RUNNER|$T_26OD_AMBIGUOUS_REFS"
  "CK09|321|RUNNER|$T_26OD_WIRED_PREFIX"
  "CK10|321|RUNNER|$T_26OD_NO_REGRESS"
  "CK11|322|RUNNER|$T_26OD_DISCRIMINATORS"
  "CK12|322|RUNNER|$T_26OD_FAILED_WRITE"
  "CK13|323|RUNNER|$T_26OD_LEASE_GUARD;$T_26OD_LEASE_CANARY"
  # ---- playhead-fil5: the durable semantic-scan claim (SC series) ----
  #
  # The bead in one sentence: nothing durably requested a semantic ad scan when
  # a transcript completed, so FOUR of the twelve episodes on the 2026-08-03
  # device pull had no path to one at all — 48E903D7, FCDDB309, 4FF3A238 and
  # 2C5C3699, what the shipped candidate SQL returns verbatim against that pull.
  # The bead's own text says three; it counts only the assets it judged
  # transcribed, and R4 corrected the count in the four Swift files that state
  # it. This line was the fifth and was missed until R6. The rails split four
  # ways and the
  # batches follow that split, because a mutation in one half is invisible to
  # the other three:
  #   SC01-SC09  the claim itself — predicates, identity, row shape
  #   SC10-SC14  whether the four gates and the sweep RECORD
  #   SC15-SC17  whether the selector FINDS the assets nobody will call again
  #   SC18-SC22  the record/ledger contracts

  # SC01 — THE defect this bead is made of, in one word, and the series'
  # VACUITY AUDIT. `adScanFraction` is `nil` exactly when no coverage-lane scan
  # row exists for the asset — it is never a synthetic 0 — so reading `nil` as
  # sufficient silences the claim for precisely the never-scanned assets it
  # exists to rescue, and for nothing else.
  #
  # Its expectation list is deliberately EVERY positive claim test, which is
  # what makes it the vacuity control: with no claim ever written each of these
  # must go red, and one staying green would mean it was passing on something
  # other than a claim actually landing. The negatives must stay GREEN and are
  # named here so a future reader can check them rather than infer them:
  # `no claim when the measured ad scan already clears the floor`, `a short
  # measured ad scan still mints` (0.47 is finite, so this mutation cannot reach
  # it), `an open gate mints no claim`, and the three reconciler "gets no claim"
  # tests.
  #
  # An artificial "record() writes nothing" control was drafted and dropped: its
  # kill set is a subset of this one, and a registered mutant that only restates
  # a real defect's coverage costs a build for no judgement.
  "SC01|400|CLAIM|$T_FIL5_UNMEASURED;$T_FIL5_MINT;$T_FIL5_IDEMPOTENT;$T_FIL5_COMPLETE;$T_FIL5_IN_FLIGHT;$T_FIL5_REFUSAL;$T_FIL5_GATE_FM;$T_FIL5_GATE_OFF;$T_FIL5_GATE_POD;$T_FIL5_GATE_FACTORY;$T_FIL5_REACHED;$T_FIL5_ONCE;$T_FIL5_LEDGER"

  # Batch 401 — five floor/identity edits with disjoint blast radii. Each is a
  # value that reads plausibly and changes which episodes are claimed.
  #
  # SC02 — the claim floor drifts onto the TRANSCRIPT's number. 0.95 is
  # calibrated for a decoder chopping seconds off the end; applying it to the ad
  # scan stops claiming for every episode between 95% and 98% scanned.
  "SC02|401|CLAIM|$T_FIL5_CLAIM_FLOOR"
  # SC03 — the mirror image: the transcript gate borrows the ad-scan floor. 0.98
  # is `episodePreparationCompleteThreshold`, the floor a FINISHED AD SCAN is
  # measured against; borrowing it here demands a transcript be more complete
  # than the scan it is a prerequisite for, and 0.95 is the transcript number
  # precisely because a decoder chops seconds off the end.
  #
  # ITS FIELD COST IS A MARGIN, NOT AN EXCLUSION, and the earlier note here got
  # that wrong in a way worth recording. It read "0.98 excludes 48E903D7, one of
  # the three assets this bead exists for" — but 48E903D7 covers 36.9 % of its
  # duration as a bridged fast-transcript AREA (its 95 % is a WATERMARK; see the
  # batch-409 note), so this gate refuses it at 0.95 and at 0.98 alike and it is
  # not the asset that separates the two constants. Measured on the 2026-08-03
  # pull, NEITHER asset the sweep reaches changes verdict: FCDDB309 is 98.79 %
  # and 4FF3A238 98.85 %, so both clear 0.98 — by 0.008 and 0.005, which is
  # inside the decoder tail the 0.95 constant exists to absorb. So the mutant's
  # rail is the synthetic one (`$T_FIL5_TX_FLOOR` pins the constant and the
  # 0.95-of-2113 case) rather than a field episode, and that is the honest claim
  # for it — a wall this narrow is a defect the next episode finds, not this one.
  "SC03|401|CLAIM|$T_FIL5_TX_FLOOR"
  # SC04 — the transcript gate's unmeasurable direction flips permissive. It
  # SUPPRESSES a mint, so `true` on an unknown duration claims for assets whose
  # readiness was never established.
  "SC04|401|CLAIM|$T_FIL5_TX_UNMEASURABLE"
  # SC06 — the version is hashed from RAW persisted chunks. Correct on every
  # single-pass asset, wrong on every asset a final pass has touched, because
  # canonicalization REPLACES the fast coverage a final chunk overlaps. Every
  # other test computes its expectation through the same helper, so this is
  # visible ONLY to the test that compares against `atomize` directly — which is
  # the whole reason that test exists.
  "SC06|401|CLAIM|$T_FIL5_CANONICAL"
  # SC09 — "" is persisted as if it were a podcast id: the same
  # absence-rendered-as-a-value that closes the `podcastIdMissing` gate in the
  # first place.
  #
  # RUN 2026-08-04: SURVIVED on its first registration, and the survivor was
  # real. The gate call site pre-converted "" to nil ITSELF before handing the
  # claim over, so the row never saw the empty string the normalization exists
  # to catch and the wire-in test could not observe it however the constructor
  # behaved. Fixed by deleting the duplicate decision at the call site, not by
  # dropping the expectation; the third rail (`record` persisting "" as NULL
  # through the store) was added at the same time.
  "SC09|401|CLAIM|$T_FIL5_EMPTY_POD;$T_FIL5_GATE_POD;$T_FIL5_STORED_POD"

  # SC05 — the claim gets a private identity. Everything still persists and the
  # gate ledger still reads correctly; the row simply sits `deferred` forever
  # beside the one the runner later mints and completes. Only two tests can see
  # it, and one is the happy-path control — which is what makes that control
  # load-bearing rather than decorative.
  "SC05|402|CLAIM|$T_FIL5_ID;$T_FIL5_OPEN_GATE"
  # SC07 — the shared hash stops ORDERING its input, so it agrees with `atomize`
  # on already-sorted chunks and diverges on everything else. That is the shape
  # an "extraction" takes when it becomes a second implementation: right in the
  # fixture, wrong on the 27 of 30 device assets that are non-monotone in
  # chunkIndex order (playhead-r5um).
  "SC07|402|ATOM|$T_FIL5_HASH"

  # SC08 — charge a closed gate as a failed attempt. Every resumability query
  # also demands `retryCount < maxRetries`, so three closed gates would retire an
  # asset that has never once been scanned. Alone in its batch: the row is
  # written and correct in every visible field, and what breaks is every
  # downstream SELECTOR, so the blast radius spans three suites.
  "SC08|403|CLAIM|$T_FIL5_ROW;$T_FIL5_MINT;$T_FIL5_IDEMPOTENT;$T_FIL5_GATE_FM;$T_FIL5_REACHED;$T_FIL5_ONCE"

  # Batch 404 — the recording sites. Each is one deleted call visible to exactly
  # one test, which is what makes them a batch.
  #
  # SC10 — the FM-unavailable gate stops recording. THE field gate: the
  # usability probe caches a false for 15 minutes, and on the sessionless
  # callers the bd-3bz retry marker is skipped too, so the drop was permanent.
  "SC10|404|ADSVC|$T_FIL5_GATE_FM"
  # SC11 — the missing-podcastId gate stops recording.
  "SC11|404|ADSVC|$T_FIL5_GATE_POD"
  # SC12 — the missing-factory gate stops recording.
  "SC12|404|ADSVC|$T_FIL5_GATE_FACTORY"
  # SC14 — the sweep runs a step LATE. The claim is still written, still correct
  # and still counted; it simply cannot be acted on until the next launch. The
  # only thing that can see it is the assertion that both counters move in the
  # SAME pass, which is why that assertion is written as a conjunction.
  "SC14|404|RECON|$T_FIL5_REACHED"
  # SC24 — the mode-off gate in `runBackfill` stops recording. Registered in the
  # R1 review round: SC10/SC11/SC12 covered three of the four gates and this one
  # was left out, so the only line of the four that could be deleted with every
  # suite green was the one a `knownBad` cohort closes.
  "SC24|404|ADSVC|$T_FIL5_GATE_OFF"
  # SC23 — the OTHER mode-off site, the guard inside `runShadowFMPhase`. It is a
  # separate mutation from SC24 because it is a separate line with a separate
  # single caller: `runBackfill` now answers `.off` itself, so the only thing in
  # a shipped build that reaches this guard is the shadow-RETRY drain, which
  # does not consult the mode before dispatching. Before the R1 round no test
  # drove that path with a demoted cohort, so deleting this call survived.
  "SC23|404|ADSVC|$T_FIL5_GATE_OFF_RETRY"
  # SC25 — the sweep's transcript gate reads the WATERMARK instead of the
  # interval union. Same constant, different numerator, and on a contiguous
  # fixture the two are identical — which is why every earlier test in this
  # series is blind to it. A gappy transcript reads 100 % by watermark over
  # audio nobody transcribed (the playhead-sd71 antipattern), so this claims —
  # and spends a capped ad-scan re-drive on — episodes full of holes.
  "SC25|404|RECON|$T_FIL5_GAPPY"

  # Batch 406 — the selector. Three edits, three disjoint exclusions.
  #
  # SC15 — the selector drops the transcript requirement, so an asset with no
  # transcript gets a claim naming a job over audio nothing has read.
  "SC15|406|STORE|$T_FIL5_NO_TX"
  # SC16 — the selector's exclusion becomes status-scoped, so an asset whose scan
  # COMPLETED reads as missing its lane and is re-claimed on every launch.
  "SC16|406|STORE|$T_FIL5_ANY_ROW;$T_FIL5_EXISTING_ROW"
  # SC17 — the selector tiebreaks on `id`. `analysis_assets.createdAt` is whole
  # SECONDS, so assets registered in one launch share a timestamp and the sweep
  # drains them by UUID rather than by age.
  "SC17|406|STORE|$T_FIL5_ORDER"

  # Batch 407 — the record/ledger contracts. Five edits, five disjoint reads.
  #
  # SC18 — `record` stops asking whether a scan is owed, so a fully-scanned
  # episode acquires a claim that solicits a pass with nothing to do.
  "SC18|407|CLAIM|$T_FIL5_NOT_OWED;$T_FIL5_SCANNED"
  # SC19 — a `complete` row is treated as claimable. The scan this claim requests
  # has already run; re-deferring it would reopen finished work.
  "SC19|407|CLAIM|$T_FIL5_COMPLETE"
  # SC20 — a `running` row is rewritten to `deferred` under a pass that is
  # mid-flight and owns that row's state machine.
  "SC20|407|CLAIM|$T_FIL5_IN_FLIGHT"
  # SC21 — the reconciler's transcript-floor guard goes, so a half-transcribed
  # asset spends an ad-scan re-drive on a pass whose real job is transcription.
  "SC21|407|RECON|$T_FIL5_SHORT_TX"
  # SC22 — claims are counted as recovered work. The pass a claim unblocks is
  # already counted as `adScanRedrivesMinted` in the same sweep, so the
  # background-task ledger would report one repair twice. playhead-onn6's own
  # ledger test is named too: it loads every EXCLUDED counter and asserts the sum
  # is nothing, so it is the second thing this mutation breaks.
  "SC22|407|RECON|$T_FIL5_LEDGER;$T_FIL5_ONN6_LEDGER"

  # Batch 408 — the sweep's BOUNDS and the population it is allowed to act on.
  # Added in the R2 review round: all three lines could be deleted with every
  # suite green, and each is on a shipped path.
  #
  # SC26 — the per-pass MINT cap goes. One `reconcile()` claims every candidate
  # that clears the floors, into a queue whose very next step can only mint 8
  # (`maxAdScanRedrivesPerReconcile`); the surplus buys nothing and would have
  # been claimed on the next launch anyway. Nothing asserted the bound before,
  # so the constant's own doc ("deliberately the same 8") was unchecked.
  "SC26|408|RECON|$T_FIL5_MINT_CAP"
  # SC27 — the CANDIDATE window widens to the re-drive sweep's 64. A separate
  # bound with a separate cost: a candidate here is expensive to REJECT (a whole
  # transcript read and a SHA-256 for one that clears the floors), which is why
  # it is 24 and not 64. Unobservable through SC26's rail — the mint cap stops
  # the loop at 8 mints, so the window only bites behind a wall of rejects.
  "SC27|408|RECON|$T_FIL5_WINDOW"
  # SC28 — the sweep stops asking whether a pass is already in flight. The
  # candidate query names "assets with no coverage-lane row" and this step reads
  # it as "assets nobody will call `runBackfill` for again"; those differ for
  # every episode being analysed RIGHT NOW, because `runPendingBackfill` is step
  # 6 of a pass. `reconcile()` genuinely reaches that state — playhead-qk44
  # documents `BGProcessingTaskHandler` running in-process while the foreground
  # runner drains — so the mint budget goes to episodes that were about to ask
  # for themselves, stamped with a reason asserting nothing asked.
  "SC28|408|RECON|$T_FIL5_IN_FLIGHT_EP"

  # Batch 409 — the NUMERATOR, and the window's liveness. Added in the R3 review
  # round, and unlike batch 408 these are not coverage holes: SC30/SC31 restore a
  # defect that was live on this branch and made the reconciler sweep mint
  # NOTHING on the field data it was written for (0 of 12 assets over the floor,
  # 2026-08-03 pull), and SC32 restores a wall that made the candidate window
  # permanently starvable. Every existing fixture stayed green under both.
  #
  # SC30 — the sweep's numerator reverts to the RAW chunk union. A chunk spans
  # first-word to last-word, so the raw union of a COMPLETE transcript is its
  # speech density (87.3 % for FCDDB309, 89.2 % for 4FF3A238) and a 0.95
  # wall-clock floor is unreachable rather than strict. playhead-pz32 found and
  # removed the identical shape one layer down; this is it re-entering.
  "SC30|409|RECON|$T_FIL5_BREATH"
  # SC32 — the candidate cursor stops advancing, restoring the fixed oldest-first
  # prefix. Rejects write nothing and so keep their position forever, which makes
  # a full window of never-claimable assets a permanent wall. Invisible to SC26
  # and SC27, which both bound the window correctly while it stays stuck at 0.
  # Batched with SC30: different functions, disjoint victims (SC30's fixture is
  # one candidate, so the cursor never leaves 0; SC32's is contiguous-chunked, so
  # the numerator never matters).
  "SC32|409|RECON|$T_FIL5_ROTATE"

  # Batch 410 — SC31 ALONE, and 411 — SC33 alone. Both mutate the SAME LINE as
  # each other, so they cannot share a batch (a batch applies its members
  # simultaneously against one build). SC31 additionally cannot share with SC30:
  # both would redden `$T_FIL5_BREATH`, and a mutation credited KILLED off a
  # batchmate's victim is a fabricated rail — the reason SC29 is unregistered.
  #
  # SC31 — the SC30 defect where it reads as a constant rather than a choice: the
  # bridge width goes to 0, so the helper silently becomes the raw union for
  # every caller. Separate from SC30: one is the call site, one is the contract,
  # and `$T_FIL5_BRIDGED` observes only this one.
  "SC31|410|CLAIM|$T_FIL5_BRIDGED;$T_FIL5_BREATH"
  # SC33 — the direction control for SC30/SC31. Bridging is only defensible while
  # it is narrower than the shortest span any lane calls an ad; widen it and it
  # conceals genuine untranscribed BLOCKS, claiming assets whose transcript a
  # scan cannot read. Without this the suite would pin "bridge more" as free.
  "SC33|411|CLAIM|$T_FIL5_BLOCKS"

  # Batch 412 — SC34, added in the R4 review round. A coverage hole, not a live
  # defect: the OTHER half of the cursor. SC32 pins that it ADVANCES; nothing
  # pinned that it WRAPS, and the two are reached by different states. Removing
  # the wrap leaves `$T_FIL5_ROTATE` green because that fixture always finds its
  # 25th asset waiting at offset 24 — the empty read only happens when the
  # population SHRANK past the cursor, which the mint cap manufactures on its
  # own (a window of 24 claimable assets leaves the cursor at 24 and 16 rows
  # behind it). The cost is a whole `reconcile()` spent idle with work
  # outstanding, and on a BGProcessingTask wake that is a launch.
  #
  # Alone in its own batch only because 409 is already recorded; it is
  # line-disjoint from SC32 and their victims do not overlap in either
  # direction (SC32 leaves the cursor at 0, so `$T_FIL5_WRAP`'s reads are never
  # empty; SC34 never fires in `$T_FIL5_ROTATE`'s fixture).
  "SC34|412|RECON|$T_FIL5_WRAP"

  # ---- playhead-9y9e: transcribed -> read for ads (RT series) ----
  #
  # Batches 413-415 each carry ONE mutation per LAYER (runner / store /
  # scheduler). The three layers share no code and no victim, and the bead's
  # whole claim is that they are separable — a batch that mixed two mutations
  # inside one layer could not be told apart by which test went red.

  # Batch 413 — the three defects as they stood.
  #
  # RT01 — the short-circuit goes: a pass that adds nothing to a finished
  # transcript is a `transcription:*` failure again and Stage 4 never runs. This
  # is AD5F3A0A on the 2026-08-03 pull, which spent BOTH onn6 re-drive ordinals
  # without once reaching the semantic scan.
  "RT01|413|AJRUN|$T_9Y9E_REACHES"
  # RT06 — the ad-scan area is bounded by the FAST union again. Nothing throws
  # and every number still prints; an episode whose transcript came from the
  # final pass simply measures as unscanned forever. Six of the twelve field
  # assets move under this mutant, and under it ALL SIX sit below the 0.98
  # sufficiency floor — four of them only because the mutant put them there.
  # (R2 review: this line read "four of them below the sufficiency floor", which
  # is the number the FIX lifts over it, not the number the mutant caps. Same
  # confusion R1 corrected in the Swift and missed here.)
  "RT06|413|STORE|$T_9Y9E_FINALONLY;$T_9Y9E_CEILING"
  # RT09 — the cap-out rescue reverts to a transcript-only measure of
  # outstanding work, which is the one measure a fully transcribed, never
  # scanned episode can never satisfy.
  "RT09|413|SCHED|$T_9Y9E_UNSCANNED"

  # Batch 414 — the direction controls, and the runner's VACUITY AUDIT.
  #
  # RT02 — VACUITY AUDIT for the short-circuit's floor. The helper returns the
  # watermark unconditionally: no chunks required, no area, no floor. Every
  # NEGATIVE fixture in the runner suite must go red. One staying green means
  # that fixture never reached the gate it claims to test — which is exactly how
  # a "half-transcribed asset" test passes against an implementation that never
  # short-circuits at all.
  "RT02|414|AJRUN|$T_9Y9E_PARTIAL;$T_9Y9E_GAPPY;$T_9Y9E_NOCHUNKS"
  # RT07 — the direction control for RT06. Widening the bound is only defensible
  # while it still CLIPS: take the windows at face value and one prompt whose
  # bounds straddle an untranscribed block claims the whole episode. Named
  # against pz32's own sprawl rail as well as this bead's, because the mutation
  # is a revert of pz32 and must be caught by the suite that owns it.
  "RT07|414|STORE|$T_9Y9E_CLIPS;$T_9Y9E_PZ32_SPRAWL"
  # RT10 — the direction control for RT09. The ad-scan term is a FALLBACK, not
  # an override: while the transcript ladder still has a rung, the target must
  # stay the ladder's, or a half-transcribed episode is asked for the whole
  # thing and the cheapest-probe-first ordering is gone.
  "RT10|414|SCHED|$T_9Y9E_LADDER"

  # Batch 415 — the quantity each gate divides, and the guards it must not jump.
  #
  # RT03 — the family-1 revert: the gate divides the WATERMARK (a high-water
  # REACH) instead of the gap-bridged AREA. A gappy transcript reads 100 % over
  # audio nobody transcribed — the playhead-sd71 antipattern — and a genuinely
  # stalled transcription gets a licence to skip its own failure accounting.
  "RT03|415|AJRUN|$T_9Y9E_GAPPY"
  # RT08 — `fetchTranscriptCoveredRanges` becomes its fast-only sibling, so
  # every caller asking "what can a scan read?" is answered "what has the fast
  # pass reached?" again. Distinct from RT06: that is the summary's bound, this
  # is the standalone read fil5's sweep and the runner's gate both use.
  "RT08|415|STORE|$T_9Y9E_RANGES"
  # RT11 — the ad-scan term jumps the queue, evaluated before the terminal,
  # budget and cooldown guards. Without this rail the new term would be an
  # unbounded retry wearing a floor.
  "RT11|415|SCHED|$T_9Y9E_GUARDS"

  # Batch 416 — RT04 ALONE. The interruption carve-out. playhead-ngev pays a
  # deliberate cost to hand the scheduler's single running slot back the instant
  # a listener scrubs; continuing into a full detection pass there reintroduces
  # exactly what ngev bought. Its own batch because it edits the same `if` the
  # RT02/RT03 helper feeds.
  "RT04|416|AJRUN|$T_9Y9E_INTERRUPT"

  # Batch 417 — RT05 ALONE. The trace reverts to a `.failed`/`asrFailed` journal
  # row. The pass still reaches Stage 4, so every behavioural assertion stays
  # green and only the row is wrong — which is the whole misattribution this
  # bead removes (five `asr_failed` rows for an episode that is fully
  # transcribed). Cannot share 413 with RT01: both redden `$T_9Y9E_REACHES`, and
  # a mutation credited KILLED off a batchmate's victim is a fabricated rail.
  "RT05|417|AJRUN|$T_9Y9E_REACHES"

  # Batch 418 — RT12, the R1 review's finding. The widened bound is built from
  # the two CHUNK sets instead of from the bound it replaced, which silently
  # drops the watermark fallback. Monotonicity was ASSERTED in a comment
  # ("the intersection is taken against a superset") and was false for an asset
  # whose watermark outlives its chunks.
  "RT12|418|STORE|$T_9Y9E_MONO"

  # Batch 419 — RT13. The final-pass loop stops dropping degenerate rows, so a
  # zero-width chunk becomes the pass's reported reach. The FAST loop has always
  # dropped them; this is the rail on the two staying symmetric.
  "RT13|419|STORE|$T_9Y9E_DEGEN"

  # Batch 420 — RT14 (R2 review). `adScanFraction`'s duration-sanity guard goes
  # back to reading the FAST reach alone while the NUMERATOR it guards is bounded
  # by both passes. Cannot share 418/419: all three mutate
  # `AnalysisStore.swift` and the battery applies a batch simultaneously.
  #
  # THE NEW TEXT IS THE PRE-REVIEW IMPLEMENTATION VERBATIM, the RT12 standard.
  # The broad mutant here would be deleting the guard outright, which also kills
  # `overshootingNumeratorIsWithheldNotClamped` — a test the reviewed code
  # already passes, so that verdict would prove nothing about the widening.
  "RT14|420|STORE|$T_9Y9E_REACHGUARD"

  # Batch 421 — RT15 (R3 review). THE WIRE-IN. The reconciler stops measuring and
  # hands `capOutRetryDecision` a constant "fully scanned", so the ad-scan arm can
  # never fire in production while every pure decision-matrix test stays green —
  # RT01/RT09/RT10/RT11 all call the static decision directly and cannot see it.
  # This is the "a correct mechanism production never invokes" family: the fix is
  # right and nothing proves it is reached.
  "RT15|421|RECON|$T_9Y9E_WIRE_MINT"

  # Batch 422 — RT16 (R3 review). The both-pass duration guard admits a final
  # WATERMARK with no chunks behind it, so a stale `finalPassCoverageEndTime`
  # withholds a fraction that is fine — an episode made LESS ready, the one
  # direction this bead's widening promised never to move. Its own batch:
  # RT14/RT16 both mutate `AnalysisStore.swift` and a batch applies at once.
  "RT16|422|STORE|$T_9Y9E_FINALSRC"

  # Batch 423 — RT17. The wire-in's OTHER direction: the reconciler skips the
  # read entirely, so `adScanFraction` is always `nil`, `isOwed` is always true,
  # and the rescue mints on a finished episode forever. RT15 cannot see this
  # (a constant 1.0 declines correctly here); RT17 cannot see RT15 (a nil owes
  # correctly there). Two mutants because it is two directions of one wire.
  "RT17|423|RECON|$T_9Y9E_WIRE_DECLINE"

  # ---------------------------------------------------------------------------
  # playhead-41mu — the UC series. The `fullEpisodeScan` terminal is MEASURED.
  #
  # Every entry is its own batch. They all mutate `$RUNNER` and a batch applies
  # simultaneously, and several of them move the same two functions in opposite
  # directions (UC05/UC06 are the two directions of the cursor rule, UC03/UC07
  # of the refusal), so sharing a batch would let one mutation mask another.
  #
  # ON BREADTH, stated because the RT12 lesson says a broad kill is not evidence.
  # The six pure-decision mutants (UC02–UC07) name one or two victims each. UC01
  # is deliberately the exception: it is the WIRE-IN, and its victims are exactly
  # the five end-to-end tests that depend on the terminal actually consulting the
  # measurement. That is not broader than the fix — it IS the fix — and it is the
  # one mutant that can tell a correct decision function from a decision function
  # production never calls. UC09 (R2) is the same family one operand over: the
  # decision IS called, with the wrong list.
  # ---------------------------------------------------------------------------

  # Batch 430 — UC01. THE WIRE-IN. `coverageTerminalDecision(for:)` stops asking
  # the store and always answers `.complete`, which is the pre-bead behaviour
  # VERBATIM: the terminal fires whenever `runJob` returned without a rate-limit
  # hole. Every pure test in the suite still passes under it — that is the whole
  # point, and it is the "a correct mechanism production never invokes" family
  # this queue keeps paying out on.
  "UC01|430|RUNNER|$T_41MU_DEFERS;$T_41MU_NOCURSOR;$T_41MU_CURSOR;$T_41MU_REDRIVE;$T_41MU_BOUNDED;$T_41MU_RESUMEHOLE"

  # Batch 431 — UC02. The phase scope is dropped from the PURE decision, so a
  # caller that reaches it with a narrow `targetedWithAudit` phase judges that
  # phase against an episode-wide floor and defers forever having done exactly
  # what it was asked to do.
  #
  # R1 review, stated because the earlier note here overclaimed: this mutant is
  # NOT observable in production. `coverageTerminalDecision(for:)` short-circuits
  # every non-`fullEpisodeScan` phase before the pure function is called, so the
  # actor-side guard still saves the narrow phases under UC02 and its only victim
  # is the pure test. The duplication is deliberate belt-and-braces — UC01 kills
  # the actor-side guard's absence, this kills the pure one's — and the honest
  # claim for UC02 is "the pure rule is load-bearing if anything ever calls it
  # directly", not "narrow phases would defer forever on device".
  "UC02|431|RUNNER|$T_41MU_SCOPE"

  # Batch 432 — UC03. A coverage read that THREW is treated as evidence the
  # episode was read. The asymmetry is the point: deferring a finished job costs
  # one bounded pass, completing an unfinished one is permanent.
  "UC03|432|RUNNER|$T_41MU_ABSENCE"

  # Batch 433 — UC04. The attempt budget never terminates, so an episode whose
  # transcript cannot support the floor (2C5C3699 on the 2026-08-03 pull has a
  # CEILING of 0.130) re-drives forever. Names the pure budget test AND the
  # end-to-end one because the bound is only worth anything if the persistence
  # path honours it.
  "UC04|433|RUNNER|$T_41MU_BUDGET;$T_41MU_BOUNDED"

  # Batch 434 — UC05. The head-hole rule is dropped and the walk's contiguous
  # bound is published unconditionally. This reproduces 53FC53E3's device row
  # exactly — cursor 2,525.82 on a 2,528 s episode — so `narrowedForResume`
  # collapses the next attempt to nothing and the deferral buys no coverage:
  # the same permanent lockout, in a different costume.
  "UC05|434|RUNNER|$T_41MU_HOLE;$T_41MU_BRIDGE;$T_41MU_NOCURSOR"

  # Batch 435 — UC06. The OTHER direction, and the reason UC05 is not simply
  # "never write a cursor": the bridge tolerance is dropped, so AD5F3A0A's 2.8 s
  # of leading silence reads as a hole, the cursor freezes on a genuine prefix,
  # and every resume re-scans audio it already holds. UC05 cannot see this (a
  # frozen cursor is correct there); UC06 cannot see UC05 (an advancing cursor is
  # correct here). Two mutants because it is two directions of one rule.
  "UC06|435|RUNNER|$T_41MU_PREFIX;$T_41MU_BRIDGE;$T_41MU_CURSOR"

  # Batch 436 — UC07, THE VACUITY CONTROL. The refusal fires unconditionally, so
  # a job that genuinely read its episode is deferred too. Without this the suite
  # could be satisfied by a gate that refuses everything, which would strand the
  # whole library rather than the under-covered slice of it.
  "UC07|436|RUNNER|$T_41MU_VACUITY;$T_41MU_FLOOR;$T_41MU_ABSENCE"

  # Batch 437 — UC08 (R1 review). The non-finite arm goes back to `.complete`,
  # which is the pre-review code VERBATIM. Cut exactly that way rather than
  # "delete the isFinite test", which would be broader than the fix and would
  # also change the finite path. It makes the terminal the ONLY consumer in the
  # pipeline that reads a non-finite fraction as evidence the episode was read.
  "UC08|437|RUNNER|$T_41MU_NONFINITE"

  # Batch 438 — UC09 (R2 review). The cursor rule's head test goes back to
  # reading the pre-`narrowedForResume` segment list, which is the pre-review
  # implementation VERBATIM — one identifier, nothing else moved. It kills only
  # the resume rail: on a first attempt the two lists are the same value, which
  # is exactly why eight mutants and two review rounds could not see it. The
  # narrowest possible cut for a defect whose whole signature is "agrees with
  # the right answer until the second attempt".
  "UC09|438|RUNNER|$T_41MU_RESUMEHOLE"

  # ---------------------------------------------------------------------------
  # playhead-iu0t — WHAT THE SHADOW-RETRY DRAIN REPLAYS (CN01-CN04)
  #
  # One line, four ways to write it, and only one is right. The shipped defect
  # was `finalChunks.isEmpty ? chunks : finalChunks` — the pre-hc7e collapse,
  # which hc7e removed from `runBackfill` and left standing here. It is worth
  # four mutants rather than one because the three wrong answers fail
  # DIFFERENTLY and no single rail sees more than two of them: final-only loses
  # reach AND identity, fast-only loses only identity (its reach is fine), and
  # raw loses only identity by a third route (right reach, duplicated evidence,
  # drifted version). A suite that killed final-only alone would be satisfied by
  # either of the others.
  #
  # All four edit the same three lines, so each takes its own batch on the
  # M08/M13 anchor rule.
  # ---------------------------------------------------------------------------

  # Batch 439 — CN01. THE SHIPPED DEFECT, restored verbatim. Not "delete the
  # canonicalize call" — the exact expression that was on disk at d257a060,
  # including the `finalChunks` binding, so the mutant reproduces the field row
  # rather than merely something adjacent to it. On asset 53FC53E3 this is what
  # discarded 2,917 fast chunks over [0, 2490] and completed a `fullEpisodeScan`
  # at an adScanFraction of 0.0142.
  #
  # It does NOT name `$T_IU0T_ALLFAST` or the two pre-iu0t drain rails: with no
  # final chunks the restored ternary falls through to the raw array, which for
  # a single-pass asset is what canonicalize returns anyway. Those tests are
  # correct to stay green, and naming them would be an expectation the mutant
  # cannot meet.
  #
  # R1 re-run, after the rule canary joined FOCUSED_SUITES: CN01 also reddens
  # `$T_IU0T_SHAPE`, deterministically — it restores the literal collapse
  # expression, which is what that canary bans by name. Deliberately NOT added
  # as a named victim. CN01's claim is about the DRAIN, and pinning it to a
  # repo-wide source canary would make a future edit to the canary's regex
  # report as "CN01's expected test never ran". Observed, recorded, not
  # depended on.
  "CN01|439|ADSVC|$T_IU0T_REACH;$T_IU0T_VERSION;$T_IU0T_JOBID;$T_IU0T_CLAIM"

  # Batch 440 — CN02. Fast-only: keep the whole fast pass and throw the
  # higher-accuracy re-transcription away. The complement of CN01, and the
  # reason CN01 alone is not enough — its REACH is correct, so the reach rail
  # cannot see it and only the identity rails can. It is also the plausible
  # over-correction for CN01 ("the fast pass is the one with the coverage"),
  # which is exactly the kind of fix-introduced defect this queue keeps
  # producing.
  #
  # R1: the rule canary does NOT see CN02, and that is the canary being narrow
  # rather than broken. It audits the argument at `atomize` call sites;
  # `chunksForReplay` is handed to `runShadowFMPhase`, whose own atomize
  # argument is the parameter `chunks` — allow-listed, because the obligation
  # sits with the callers. Only the behavioural identity rails can see a wrong
  # chunk set one frame up, which is the whole reason CN02 exists.
  "CN02|440|ADSVC|$T_IU0T_VERSION;$T_IU0T_RAW;$T_IU0T_JOBID;$T_IU0T_CLAIM"

  # Batch 441 — CN03. No canonicalization at all: replay the raw persisted rows.
  # The subtlest of the three and the one the FIRST version of this bead's
  # fixture could not see — its reach is correct AND its chunk count is correct;
  # what is wrong is that the audio the final pass re-transcribed is present
  # twice (hc7e's duplicate evidence) and the version drifts from the one
  # `runBackfill` derives, putting the drain back in a private id space by a
  # third route. Killed only because the fixture's final region OVERLAPS the
  # fast coverage; with a disjoint final tail this mutant survives, which is why
  # the fixture control asserts `droppedFastCount == 2`.
  "CN03|441|ADSVC|$T_IU0T_VERSION;$T_IU0T_RAW;$T_IU0T_JOBID;$T_IU0T_CLAIM"

  # Batch 442 — CN04, THE VACUITY CONTROL. The replay set is empty, so the
  # `!chunksForReplay.isEmpty` guard bails and the drain never runs. Every
  # behavioural rail in the bead must go red, and the PURE fixture control must
  # stay green — that is the shape a vacuity control has to have here. Without
  # it, the identity rails above could in principle be satisfied by a store that
  # happened to contain no rows at all (`allSatisfy` over an empty array is
  # true), and the reach rail by ambient fixtures. It also names the two
  # pre-iu0t drain rails and Test B: those predate this bead, so their reddening
  # is what proves the mutant kills the DRAIN rather than merely this suite.
  "CN04|442|ADSVC|$T_IU0T_REACH;$T_IU0T_VERSION;$T_IU0T_RAW;$T_IU0T_JOBID;$T_IU0T_CLAIM;$T_IU0T_ALLFAST;$T_IU0T_FASTFALLBACK;$T_IU0T_MIXEDDRAIN;$T_IU0T_RETRYRUNS"

  # Batch 443 — CN06 (playhead-iu0t R1). THE FOURTH INSTANCE, restored verbatim
  # in `AnalysisCoordinator.pushEvidenceCatalog`, where it was still live after
  # iu0t declared "both are converted now". Different file, so it is not on the
  # CN01-CN04 anchor and takes its own batch for the ordinary reason.
  #
  # Its victims are the CANARY, not a behavioural rail, and that is the finding
  # rather than a shortcut: `pushEvidenceCatalog` is private, has no test seam,
  # and no test in the repo exercises it — which is precisely how the collapse
  # sat there. A source-level pin is what can see a defect at a call site that
  # nothing runs. Both canary tests are named because they check different
  # things (the ARGUMENT is uncanonicalized, and the COLLAPSE SHAPE is back)
  # and a mutant that only tripped one would mean the other is miswritten.
  #
  # There is no CN05: it is the KNOWN GAP entry below (`runPhase5ProjectorPhase`,
  # production-dead). The number is skipped rather than reused so the gap note
  # and the battery agree.
  "CN06|443|ACOORD|$T_IU0T_RULE;$T_IU0T_SHAPE"

  # Batch 444 — CN05 (playhead-iu0t R2). The THIRD instance, restored verbatim
  # in `runPhase5ProjectorPhase`. It was a KNOWN GAP in R1 and is no longer one,
  # and the reason it stopped being one is itself the R2 finding.
  #
  # R1 declined to encode it on the grounds that a mutant on production-dead
  # code "can only ever be red or meaningless". That was right about a
  # BEHAVIOURAL rail — the method's only two call sites are tests passing an
  # all-`final` fixture that canonicalize returns unchanged, so the mutation is
  # a literal no-op there — but it is wrong about a SOURCE canary, which reads
  # the call site rather than running it. The reason CN05 nevertheless could not
  # be encoded in R1 is that the canary's allow-list was keyed
  # `File.swift|expression`, so `AdDetectionService.swift|chunks` licensed the
  # collapse in every one of that file's 13,331 lines and the mutant SURVIVED.
  # R2 re-keyed it `File.swift|declaration|expression`; the entry now licenses
  # `runShadowFMPhase` and `recordSemanticScanClaim` only, and this mutant dies.
  #
  # So CN05 is the mutant that proves the scoping change bites, and it converts
  # a documented hole into a killed mutant. Both canary tests are named: the
  # restored expression is both an uncanonicalized ARGUMENT and the literal
  # COLLAPSE SHAPE.
  "CN05|444|ADSVC|$T_IU0T_RULE;$T_IU0T_SHAPE"

  # Batch 445 — CN07 (playhead-iu0t R2). THE FIFTH INSTANCE, restored verbatim:
  # the episode-summary invalidation key read back out of the persisted
  # `transcript_chunks.transcriptVersion` column, which
  # `backfillLegacyTranscriptChunksPhase1IfNeeded` fills `WHERE pass != 'fast'`
  # — the collapse spelled as a negation, in SQL, and therefore invisible to
  # both greps that found the first four AND to the call-site walk, because
  # reading a column is not a call.
  #
  # Its victim is the third canary test only. `hydrate` has no behavioural rail
  # that could see this: nothing reads `episode_summaries.transcriptVersion`
  # back (the candidate selector's only staleness test is `schemaVersion < ?`),
  # which is filed separately — and that absence is precisely why a source pin
  # is the instrument that can see it. Same lesson as CN06, by a different
  # route: the sites this defect family survives at are the ones no test runs.
  "CN07|445|ESUMBF|$T_IU0T_COLUMN"

  # Batch 446 — CN08 (playhead-iu0t R3). THE FIFTH INSTANCE AGAIN, RESPELLED.
  # Semantically identical to CN07 — the same column, read as the same
  # invalidation key, at the same line — but written with a closure instead of
  # a keypath. Before R3 this mutant SURVIVED while CN07 died, because the rule
  # it violates was policed by three literal substrings and the first of them
  # was the keypath spelling `\.transcriptVersion` matched with
  # `String.contains`, not a regex.
  #
  # That is why it is worth its own batch rather than being folded into CN07.
  # CN07 alone proves the canary recognises ONE WAY OF WRITING the defect; a
  # pair that differ only in spelling is what proves it recognises the DEFECT.
  # R3 verified the gap directly rather than by reading: it planted this
  # spelling plus `chunks.last?.transcriptVersion` and a plain-receiver form in
  # the tree and the whole canary exited 0.
  #
  # Same anchor line as CN07, so it takes its own batch on the M08/M13 rule.
  "CN08|446|ESUMBF|$T_IU0T_COLUMN"

  # Batch 447 — CN09 (playhead-iu0t R4). CN06 RESPELLED: the same collapse at
  # the same line of `pushEvidenceCatalog`, written `{ chunk in chunk.pass ==
  # "final" }` instead of `{ $0.pass == "final" }`. CN08 is to CN07 what this is
  # to CN06, and for the identical reason — R3 fixed rule 3's spelling-bound
  # needles and did not check rule 2, which was policed by TWO literal
  # substrings (`filter { $0.pass == "final" }` and the `TranscriptPassType`
  # form). This spelling matched neither.
  #
  # The victim that discriminates is `$T_IU0T_SHAPE`. `$T_IU0T_RULE` reddens
  # either way — rule 1 checks for the PRESENCE of `canonicalize(`, so it is
  # spelling-blind by construction and killed CN09 before R4 too. Naming both is
  # what makes this mutant a measurement rather than a duplicate of CN06: under
  # R3's literal half-one the SHAPE rail stayed green and the battery would
  # report CN09 SURVIVED on that victim; under R4's pattern both die.
  "CN09|447|ACOORD|$T_IU0T_RULE;$T_IU0T_SHAPE"

  # Batch 448 — CN10 (playhead-iu0t R5). CN06 RESPELLED A THIRD TIME, and this
  # one is not about the closure at all: the collapse is gone and the ONLY
  # change is that the call itself is split across two lines.
  #
  #     TranscriptAtomizer
  #         .atomize(chunks: chunks, …)
  #
  # Before R5 that made the call site VANISH — `guardedCalls` held two literal
  # substrings, so one line break removed it from the walk entirely: not
  # reported, not counted against the `audited` floor, indistinguishable from a
  # clean tree. Measured directly rather than reasoned about: the identical
  # uncanonicalized argument planted INLINE reddened rule 1 and named the site;
  # planted SPLIT it passed. CN09 is to CN06 what this is to CN01-style raw
  # replay — the pair differing only in spelling is what proves the rule is
  # about the defect rather than about one way of writing it.
  #
  # `$T_IU0T_RULE` is the discriminating victim. `$T_IU0T_SHAPE` is NOT named:
  # this mutant contains no final-pass filter, so rule 2 is correct to stay
  # green and naming it would be an expectation the mutant cannot meet.
  "CN10|448|ACOORD|$T_IU0T_RULE"

  # Batch 449 — CN11 (playhead-iu0t R5). The collapse spelled as a NEGATION:
  # `filter { $0.pass != "fast" }` with the same `.isEmpty ? chunks :` fallback.
  # Semantically CN06 exactly — "keep the final pass" and "drop the fast pass"
  # are one instruction — and it is not a hypothetical spelling: instance #5 of
  # this bead's five IS the negation (`AnalysisStore
  # .backfillLegacyTranscriptChunksPhase1IfNeeded`, `WHERE pass != 'fast'`), and
  # this file's own header already calls it "the collapse spelled as a
  # negation". R4's pattern was receiver-agnostic but still required `==`
  # against one of three final literals, so it did not see this.
  #
  # `$T_IU0T_SHAPE` is the discriminating victim — under R4's pattern it stayed
  # green. `$T_IU0T_RULE` reddens either way, because rule 1 judges the
  # ARGUMENT and `preferred` resolves to no canonicalizing binding whatever the
  # filter says. Naming both is what makes this a measurement rather than a
  # duplicate of CN06.
  "CN11|449|ACOORD|$T_IU0T_RULE;$T_IU0T_SHAPE"

  # Batch 450 — CN12 (playhead-iu0t R5). LAUNDERING: the site canonicalizes and
  # then collapses the result anyway.
  #
  #     chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
  #         .filter { $0.pass == "final" }
  #
  # This passed rule 1 before R5 because the check named `isCanonicalized`
  # actually tested whether the token `canonicalize(` APPEARS — the bead's own
  # defect class (a value that names one thing, read as though it named
  # another) committed inside the check named for it, failing OPEN. Rule 2
  # cannot cover it either: its conjunction needs the `X.isEmpty ? chunks : X`
  # fallback and a bare `.filter` has none.
  #
  # Worth its own batch rather than folding into CN06 because it is the mutant
  # that separates "the argument mentions canonicalize" from "the argument IS
  # the canonical set" — and because fixing `isCanonicalized` alone did NOT
  # kill it: a duplicated bare `argument.contains("canonicalize(")` one line
  # below still let it through, which is how the fix's own reintroduction was
  # caught.
  "CN12|450|ACOORD|$T_IU0T_RULE"

  # --- playhead-e75l: daemon-refusal disposition (DR series) ---------------
  #
  # Batches 500-506. The series is deliberately spread thin: four of the ten
  # mutants make a metadata stall stop being a refusal, so any two of them in
  # one batch would each be credited for the other's kill. Batch membership
  # here means "these two cannot mask each other", never "these fit". R3 review
  # moved DR02 out of batch 500 for exactly that reason once its expectation set
  # grew — see batch 506.
  #
  # DR01 is THE mutant: the pre-e75l production line, verbatim. Everything else
  # is one substitution inside the fix.

  # Batch 500. DR01 reverts the classification to kvs8's throttle-only
  # predicate — the shipped code this bead was filed against.
  #
  # R3 review MOVED DR02 out of this batch. Its expectation set grew to include
  # the RUNNER-level control (`$T_DR_STANDARD`), and DR01 masks that control
  # exactly: with `classify` reduced to the throttle-only ternary, a
  # standard-deadline timeout still lands in the genuine-failure arm and the
  # control stays GREEN, so DR02 would have reported SURVIVED for a mutation it
  # kills on its own. Batch membership means "these two cannot mask each other".
  "DR01|500|RUNNER|$T_DR_DEFERS;$T_DR_COST"

  # Batch 501. DR03 is the reviewer's probe made concrete: the predicate reads
  # the OTHER budget. It is in the same anchor as DR02, so it cannot share a
  # batch with it regardless of masking.
  "DR03|501|FMREF|$T_DR_BUDGET;$T_DR_DEFERS"

  # Batch 502. DR04 collapses the SIBLING token back to kvs8's literal — a rate
  # limit recorded for a batch the daemon stopped serving for another reason.
  # DR06 restores the hard-coded drain-stop event name, and only the source
  # canary can see it: no test on this harness reads a log line.
  "DR04|502|RUNNER|$T_DR_ONECOUNTER;$T_DR_BATCH"
  "DR06|502|RUNNER|$T_DR_WIRING"

  # Batch 503. DR05 collapses the PER-JOB token. Alone because it also breaks
  # the mixed-population test, which is DR04's and DR07's expectation.
  "DR05|503|RUNNER|$T_DR_CAUSE"

  # Batch 504. DR07 is the "two counters" defect the shared type exists to
  # prevent: only throttles advance the consecutive counter, so a drain the
  # daemon refused twice by two different means never stops.
  "DR07|504|RUNNER|$T_DR_ONECOUNTER"

  # Batch 505. R2 review. DR08 is DR06's twin one line up — the PER-JOB event
  # name as a literal. R1's canary forbade two kvs8-era spellings by name, so
  # it could not see a literal of the event this bead ADDED; DR08 SURVIVED on
  # first plant and is what turned the canary from a blacklist into a rule
  # derived from `FMDaemonRefusal.allCases`. DR09 is the durable TOKEN family:
  # the pre-review spelling verbatim, which joined playhead-8d5r's
  # `inferenceTimeout-` family — the prefix an operator greps to count "the
  # model is not answering on this device".
  "DR08|505|RUNNER|$T_DR_WIRING"
  "DR09|505|FMREF|$T_DR_FAMILY"

  # Batch 506. R3 review, and both entries are about a rail that was thinner
  # than it read.
  #
  # DR02 moved here from batch 500 (see above) and now names the RUNNER-level
  # control as well as the pure one. `$T_DR_STANDARD` was DEFINED at R1 and
  # referenced by no mutation at all, so the claim "a 300 s inference timeout
  # still fails and still spends a retry" had a test and no mutant — the
  # difference between a rail and a sentence.
  #
  # DR10 is DR09's sibling in a family the canary could not see. R2's foreign
  # set was the two spellings it had looked at; this runner writes six, so a
  # refusal token named into `underCoverage-` (playhead-41mu's cause for a
  # coverage-lane job that declines to certify itself) passed the rule. Neither
  # can mask the other: DR02 cannot reach a source canary and DR10 cannot reach
  # `classify`.
  "DR02|506|FMREF|$T_DR_BUDGET;$T_DR_STANDARD"
  "DR10|506|FMREF|$T_DR_FAMILY"

  # Batches 507-510. R4 review, and all four are the SAME defect wearing four
  # spellings: a source canary that finds its sites by literal substring over
  # ONE line cannot see a name written across two. R4 planted eight respellings
  # against these three canaries and SEVEN survived — the one that failed was
  # killed by SwiftLint's `no_space_in_method_call`, not by a rail here.
  #
  # One mutation per batch, because DR11/DR12 both redden the wiring canary and
  # DR13/DR14 both redden the budget canary, and a batch may contain no two
  # mutations that can redden the same test.
  #
  # NOT ENCODED, and stated rather than hidden: probe CN-B1 (the foreign family
  # written `"\(Self.underCoverageFamily)-…"` so the derivation loses it, plus a
  # refusal token named into that family) needs edits in TWO files, and a
  # mutation record names one. It was verified by hand at R4 — survives the R3
  # canary, killed by the R4 one — and the constant-resolution fix it bought is
  # exercised by nothing here.

  # DR11 — the SWAP, and the sharpest of the four because it uses no literal at
  # all. The per-job line emits `drainStoppedEvent`, the drain-stop line emits
  # `logEvent`. R2's rail asked whether both properties are READ, which says
  # nothing about which SITE reads which; both are read, so it passed. On a
  # device every daemon-refused JOB would then be counted as a drain stop by a
  # support-bundle grep, and every drain stop as a job. This bead's own defect
  # class committed inside the check written for it.
  "DR11|507|RUNNER|$T_DR_WIRING"

  # DR12 — DR06 respelled. The drain-stop event is hard-coded again, but split
  # across a `\` line continuation INSIDE the multiline string the runner
  # already writes its log lines in, so no single line holds the name; a decoy
  # `logger.debug` supplies the property read R2's half required.
  "DR12|508|RUNNER|$T_DR_WIRING"

  # DR13 — a third 30 s budget spelled as arithmetic on an allowed one.
  # `FMInferenceDeadline.standard / 10` IS thirty seconds, and the pre-R4 check
  # asked only whether the LINE mentions an allowed name. It does. Every
  # genuine 300 s inference timeout would classify as a daemon refusal and
  # retry unboundedly — the exact hazard R2-Fix5 built that canary for.
  "DR13|509|PROBE|$T_DR_BUDGETSITES"

  # DR14 — an ADDED call site written across two lines. Invisible to a per-line
  # finder, and free: the site count is a FLOOR, so a site that disappears is
  # red and a site nobody can see is not. The existing `standard` site is left
  # in place precisely so the count still clears 9.
  "DR14|510|PROBE|$T_DR_BUDGETSITES"
)

# KNOWN GAP, deliberately NOT encoded above (an entry here would make this
# script permanently red, which would train people to ignore it):
#
#   N01 — `revertWindow`: relocate its `recordThresholdControlSignal` and trust
#   penalty BELOW the lifecycle guard, so a replacement landing during
#   `store.persistRevertedAdWindow` discards feedback the CAPTURED show is
#   owed — the exact defect the seam's own comment says the ordering prevents.
#   Probed 2026-07-27; SURVIVED the focused set.
#
#   D13 (playhead-d3g0) — delete `endEpisode`'s
#   `armedSuggestWindowIds.removeAll()`. Probed 2026-07-31; SURVIVED, and it is
#   INERT rather than unpinned. Both readers of that set resolve through
#   `suggestWindows`, which `endEpisode` also clears, so a leaked armed id has
#   nothing to resolve to and cannot reach an emission or a replay. The clear is
#   bounded-growth hygiene across a long session, not behaviour. Writing a test
#   for it would be theatre: there is no observable difference to assert.
#
#   Every sibling seam now has this pinned (`recordListenRevert`,
#   `revertByTimeRange`, `acceptSuggestedSkip`, `denyAutoSkippedBanner`).
#   `revertWindow` is the one that cannot be interleaved today: it takes no
#   `…PersistenceBarrierForTesting` suspension, and its only await is the store
#   transaction. Closing it means one more production barrier take — worth
#   doing when `revertWindow` acquires a production caller, which per the
#   census above `declineSuggestedSkip` it does not have yet (the only
#   reference is a `NowPlayingView` closure PARAMETER of the same name, bound
#   to `denyAutoSkippedBanner`). Its show ATTRIBUTION is covered: see N07.
#
#   SC29 (playhead-fil5, R2) — `CoveragePlanner.plan(for:)`'s fullCoverage arm
#   gains a phase AHEAD of `.fullEpisodeScan`, e.g. `[.scanHarvesterProposals,
#   .fullEpisodeScan]`. That is the real latent break behind
#   `SemanticScanClaim.jobId`: the claim hardcodes `(.fullEpisodeScan, offset
#   0)` while the runner derives its id from `plan.phases.enumerated()`, so the
#   two agree only while that tuple IS the whole plan. Change the plan and every
#   claim row already on disk orphans silently — invisible to the runner, still
#   counted by both sweeps.
#
#   NOT ENCODED because of blast radius, the same reason the FusionWeightConfig
#   arming above is excluded: `CoveragePlanner` is upstream of
#   `BackfillJobRunnerTests` and `BackfillCoarseCheckpointTests`, both in
#   FOCUSED_SUITES, so the mutation turns suites red across the whole focused
#   set and any batchmate's verdict becomes noise.
#
#   Pinned by a TEST instead, which is the honest substitute here because the
#   coupling is an assertion about production code rather than a missing rail:
#   `the claim's id is the runner's fullCoverage job id` now DERIVES its
#   expectation from `CoveragePlanner().plan(for:)` — it asserts `plan.phases ==
#   [.fullEpisodeScan]` and then builds the id the way `runPendingBackfill`
#   does. Change the plan and it goes red. The claim's own side of the coupling
#   stays mutation-covered by SC05, which names this same test.
#
#   CN05 (playhead-iu0t) — RESOLVED in R2; it is encoded above as batch 444 and
#   this entry is kept only because the reasoning that closed it is worth more
#   than the hole was.
#
#   R1 left it out: restoring `filter { $0.pass == "final" }` inside
#   `AdDetectionService.runPhase5ProjectorPhase` changes nothing observable,
#   because the method has no production caller at all — its only two call sites
#   are `SpliceSlotOwnershipPhase5GuardTests`, both passing an all-`final`
#   fixture that canonicalize returns unchanged. That argument is sound for a
#   BEHAVIOURAL rail and it is what "a mutant on unreachable code can only ever
#   be red or meaningless" meant.
#
#   It stops being sound the moment the victim is a SOURCE canary, which reads
#   the call site instead of running it. The real reason CN05 could not be
#   encoded in R1 was narrower and fixable: the canary's allow-list was keyed
#   `File.swift|expression`, so one entry (`AdDetectionService.swift|chunks`)
#   licensed the collapse anywhere in a 13,331-line file. R2 re-keyed it to
#   `File.swift|declaration|expression` and the mutant dies. The dead-code
#   question is still open and still filed as **playhead-tqqu** — encoding CN05
#   pins the call site, it does not decide whether the method should exist.

# One-line description per mutation, for the report.
describe_mutation() {
  case "$1" in
    T01) echo "SemanticScanThroughputSplit.bucket(for:): a nil scene phase becomes .foreground" ;;
    T02) echo "ScanScenePhase.attributionBucket: a recorded .unknown becomes .foreground" ;;
    T03) echo "readSemanticScanResult: a NULL scenePhase column defaults to .active on the READ" ;;
    T04) echo "insertSemanticScanResult: the store invents .active for a caller that supplied no phase" ;;
    T05) echo "insertSemanticScanResult: drop the createdAt backstop clock" ;;
    T06) echo "readSemanticScanResult: read a NULL createdAt through sqlite3_column_double (1970)" ;;
    T07) echo "V42 rung stamps 41 instead of 42 — the ladder stops climbing to head" ;;
    T08) echo "BackfillJobRunner.attributed: stop stamping runCorrelationId" ;;
    T09) echo "BackfillJobRunner.attributed: stop stamping scenePhase" ;;
    T10) echo "BackfillJobRunner.attributed: guess .active when the provider breaks its vocabulary" ;;
    T11) echo "SemanticScanThroughputSplit.isEligible: admit no-work sentinels as throughput" ;;
    T12) echo "fetchSemanticScanThroughputSplit: SQL admits no-work sentinels as throughput" ;;
    T13) echo "ScanThroughputBucket.realtimeRatio: a zero denominator reports 1.0 instead of nil" ;;
    T14) echo "SemanticScanThroughputSplit.attributedFraction: an empty corpus reports 1.0" ;;
    T15) echo "fetchSemanticScanThroughputSplit: OVERWRITE the unattributed bucket instead of summing" ;;
    BL01) echo "bllt: gatedLabel is INERT — the hot path reaches auto-skip on invented edges again (THE shipped defect)" ;;
    BL02) echo "bllt: the demotion becomes a DROP — the ad is silently un-persisted instead of bannered" ;;
    BL03) echo "bllt: the condition is INVERTED — anchored rows demote, invented ones auto-skip" ;;
    BL04) echo "bllt: BOTH edges must be invented to demote, so a half-anchored span auto-skips on its invented edge" ;;
    BL05) echo "bllt: the blocking flag is ignored — the kill switch stops killing" ;;
    BL06) echo "bllt: the single-window site persists the UNGATED label (gate computed, then not used)" ;;
    BL07) echo "bllt: the aggregator site persists the UNGATED label (gate computed, then not used)" ;;
    BL08) echo "bllt: the single-window site persists an anchor it did NOT gate on — label and row disagree" ;;
    BL09) echo "bllt: the suggest-tier census loses the extent REASON — demotion is silent again" ;;
    BL10) echo "bllt: censusDetail fires on EVERY row, so the detail stops discriminating" ;;
    UG01) echo "ug9m: isRescuable drops the no-anchored-row conjunct — a 9s6q-withheld asset is rescued and re-promoted" ;;
    UG02) echo "ug9m: isSupersedable drops !isFullyAnchored — a re-mint replaces an already-proven row" ;;
    UG03) echo "ug9m: isSupersedable drops !isSettled — a re-mint deletes a USER VETO" ;;
    UG04) echo "ug9m: isSupersedable drops the producer check — a re-mint retires another detector's window" ;;
    UG05) echo "ug9m: isFullyAnchored accepts EITHER edge — a half-anchored row reads as healthy" ;;
    PG01) echo "pggn: the sub-continuation tail is scored unconditionally again — a 30 s row carries a 60 s number (THE shipped defect)" ;;
    PG02) echo "pggn: the containment test is INVERTED — inside the extent is deferred, outside it is scored" ;;
    PG03) echo "pggn: the deferred tail is DROPPED even once the extent covers it — the score describes LESS than the span" ;;
    PG04) echo "pggn: the end-of-stream flush scores its pending tail" ;;
    PG05) echo "pggn: the countdown close scores its pending tail" ;;
    PG06) echo "pggn: the unbridgeable-gap close scores its pending tail" ;;
    PG07) echo "pggn: the DENOMINATOR alone grows with the dropped tail — dilution with no numerator contribution" ;;
    PG08) echo "pggn/bllt: the aggregator claims byte-exact edges, so an undiluted 0.62 reaches auto-skip over invented boundaries" ;;
    UG06) echo "ug9m: the rescue arm ignores the generation — a marked exit becomes retryable within a generation" ;;
    UG07) echo "ug9m: the rescue CEILING is removed — the re-fetch is unbounded" ;;
    UG08) echo "ug9m: advance never increments rescueAttemptCount — the bound is never reached" ;;
    UG09) echo "ug9m: the ceiling reads lastExit instead of the mark census — a retryable rescue outcome escapes it" ;;
    UG10) echo "ug9m: the mint drops the strict conjunct — a SEGMENT-RECOVERED re-mint supersedes and promotes (THE negative)" ;;
    UG11) echo "ug9m: the mint retires nothing — the degraded row survives beside the anchored one" ;;
    UG12) echo "ug9m: freezeState reports frozen as rescuable — the permanent state stops being visible" ;;
    UG13) echo "ug9m: fetchDayZeroAttemptContext returns an EMPTY census — the rescue never fires" ;;
    UG14) echo "ug9m: classify buckets a settled row before an anchored one — a vetoed anchored mark stops proving the mint could stamp" ;;
    M01) echo "revertByTimeRange: delete the MANAGED loop's in-loop lifecycle guard" ;;
    M02) echo "revertByTimeRange: delete the SUGGEST loop's in-loop lifecycle guard" ;;
    M03) echo "revertByTimeRange: MANAGED in-loop guard 'break' -> 'return'" ;;
    M04) echo "revertByTimeRange: SUGGEST in-loop guard 'break' -> 'return'" ;;
    M05) echo "revertByTimeRange: re-nest the final guard inside the podcastId/trustService branch" ;;
    M06) echo "revertByTimeRange: drop the lifecycle gate on the suggest-tier work list" ;;
    M07) echo "recordListenRevert: hoist the lifecycle guard back above the fire-and-forget effects" ;;
    M08) echo "revertByTimeRange: delete the 'if revertedManagedAny' condition on the controller write" ;;
    M09) echo "makeManualCorrectionVetoEvent: restore the 'correctionStore != nil' precondition" ;;
    M10) echo "declineSuggestedSkip: restore the 'correctionStore != nil' precondition on the denial receipt" ;;
    M11) echo "recordListenRevert: early-exit on the optional trustService above the effects" ;;
    M12) echo "recordListenRevert: delete the final lifecycle guard before evaluateAndPush()" ;;
    M13) echo "controller sample attributed to activePodcastId instead of the captured podcastId (both seams)" ;;
    M14) echo "recordListenRevert: move ingestNegativeFingerprint below the lifecycle guard" ;;
    M15) echo "recordListenRevert: delete the trust penalty" ;;
    M16) echo "recordListenRevert: downgrade the trust penalty to recordWeakFalseSkipSignal" ;;
    M17) echo "correction receipt attributed to activePodcastId instead of the captured podcastId (both seams)" ;;
    M18) echo "AnalysisStore.feedbackAssetMatches: always accept (neuters all 4 explicit-feedback transactions)" ;;
    M19) echo "declineSuggestedSkip: treat a passive auto-fade as an explicit denial" ;;
    M20) echo "confirmAutoSkippedBanner: start writing a controller sample (agreement must not calibrate)" ;;
    N02) echo "denyAutoSkippedBanner: discard the calibration effects when the episode was replaced mid-flight" ;;
    N03) echo "acceptSuggestedSkip: discard the MISS sample when the episode was replaced mid-flight" ;;
    N04) echo "confirmAutoSkippedBanner: ingest a hard negative (agreement must not pollute the bank)" ;;
    N05) echo "acceptSuggestedSkip: MISS attributed to activePodcastId instead of the captured show" ;;
    N06) echo "drop the empty-show-id refusal at BOTH sites (orchestrator + controller store)" ;;
    N07) echo "revertWindow: controller sample attributed to activePodcastId instead of the captured show" ;;
    S01) echo "acceptSuggestedSkip: fall back to the last retired suggest revision, so a stale Yes promotes anyway" ;;
    S02) echo "acceptSuggestedSkip: delete the active-episode guard on the episode-bound form" ;;
    S03) echo "declineSuggestedSkip: delete the active-episode guard on the episode-bound form" ;;
    S04) echo "receiveAdDecisionResults: delete the symmetric suggest clear on a same-id eligible result" ;;
    S05) echo "declineSuggestedSkip: read the suggest entry instead of removing it" ;;
    S06) echo "receiveAdWindows: delete the gate-flip suggest clear on a same-id non-markOnly revision" ;;
    S07) echo "beginEpisode: stop clearing recentlyAcceptedSuggestIds on a direct episode replacement" ;;
    O01) echo "ingestNegativeFingerprint: drop the anonymous-show refusal (a NULL-show hard negative every show reads back)" ;;
    O02) echo "revertWindow: fall the trust penalty back to activePodcastId when the correction has no usable show" ;;
    O03) echo "revertWindow: restore the outright refusal, so an anonymous correction loses its durable receipt" ;;
    O04) echo "revertWindow: attribute recurrence REVOCATION to activePodcastId when the correction has no usable show" ;;
    P01) echo "ingestNegativeFingerprint: drop the mixed-width guard (a whole-span hard negative over a window that is mostly a real ad)" ;;
    P02) echo "revokeRecurrenceEvidence: fingerprint the WHOLE reverted span again instead of the attributable subspans" ;;
    D01) echo "registerSuggestedWindow: emit at DETECTION delivery again instead of arming (the bead)" ;;
    D02) echo "playhead entry: exclude the START edge, so entering the span is not entry" ;;
    D03) echo "playhead entry: include the END edge, so a span already behind the playhead still asks" ;;
    D04) echo "playhead entry: never disarm, so every observation inside a span re-asks" ;;
    D05) echo "replay: drop the armed filter, so a late host receives spans the playhead never reached" ;;
    D06) echo "playhead entry: require a dwell before presenting, so the card arrives inside the ad" ;;
    D07) echo "suggest card: always claim the confirmation will skip (the pre-affordance state)" ;;
    D08) echo "suggest card: never claim the confirmation will skip (the overshoot)" ;;
    D09) echo "banner copy: give the mark-only card the skipping card's confirm wording" ;;
    D10) echo "latency budget: raise it past any meaning the transport tick gives it" ;;
    D11) echo "EVENT-stream replay: drop the armed filter (D05's twin, a duplicated guard)" ;;
    D12) echo "same-tick emission: drop the ordering, so two spans banner in Set order" ;;
    D14) echo "registerSuggestedWindow: drop the re-arm on an exact replay after retirement" ;;
    E01) echo "ingestPersistedAdWindows: drop the active-asset guard, so a mint for another episode is 'delivered'" ;;
    E02) echo "the shared admission rule: admit every decision state, resurrecting a user's veto through the new door" ;;
    E04) echo "ingestPersistedAdWindows: emit at registration (d3g0's stale-playhead hazard, through the new door)" ;;
    E05) echo "triggerIfEligible: deliver on EVERY day-0 run, marked or not" ;;
    E06) echo "triggerIfEligible: never deliver (the playhead-96ot defect, restored verbatim)" ;;
    E07) echo "triggerIfEligible: deliver twice per marked run" ;;
    # NOTE: no backticks. These descriptions are inside a double-quoted `echo`,
    # so a backtick opens a command substitution — the first draft of this line
    # printed "accumulate  into the MARK count" plus a "rotated: command not
    # found" to stderr. Same class of defect as the ';' in a d3g0 test name.
    E08) echo "runDayZeroRefetch: accumulate the ROTATED flag into the MARK count (the neighbouring number)" ;;
    E09) echo "fetchMintAndRecord: report 0 marks on the marked branch, so nothing propagates" ;;
    O05) echo "denyAutoSkippedBanner: restore the outright refusal, so a banner No naming another show loses its receipt" ;;
    K01) echo "DayZeroTransportPolicy: Low Data Mode applies only on WiFi" ;;
    K02) echo "DayZeroTransportPolicy: cellular refused regardless of the user setting" ;;
    K03) echo "RediffActivation: the shipping transport default flips to cellular-allowed" ;;
    K04) echo "RediffDayZeroDailyBudget: the rolling window never elapses" ;;
    K05) echo "RediffDayZeroDailyBudget: admit an attempt that fits only ONE B-copy" ;;
    K06) echo "RediffDayZeroDailyBudget: a zero-byte spend starts a window" ;;
    K07) echo "DayZeroReadinessWait: blame the LAST probe instead of the furthest observed" ;;
    K08) echo "DayZeroReadinessWait: report cancellation as a missing-file give-up" ;;
    K09) echo "RediffDayZeroKickoffOutcome: both give-ups share one invariant code" ;;
    K10) echo "RediffDayZeroKickoffOrdering: an unknown publish date sorts FIRST" ;;
    K11) echo "RediffDayZeroKickoffOrdering: drop the FIFO tiebreak (the order is no longer total)" ;;
    K12) echo "RediffDayZeroKickoffCoordinator: drain FIFO instead of newest-episode-first" ;;
    K13) echo "RediffDayZeroKickoffCoordinator: count every give-up as .fired" ;;
    K14) echo "RediffDayZeroKickoffCoordinator: drop the in-flight dedupe" ;;
    K15) echo "DayZeroRediffTrigger: the transport refusal writes nothing (the playhead-4dqe defect)" ;;
    K16) echo "DayZeroRediffTrigger: admit the budget against a zero estimate" ;;
    K17) echo "DayZeroRediffTrigger: charge the window the ESTIMATE, not the real cost" ;;
    K18) echo "AnalysisStore: count a day-0 give-up as a fired kickoff" ;;
    K19) echo "AnalysisStore: the day-0 budget window never rolls in SQL" ;;
    K20) echo "RediffFetchRequest: the request refuses cellular whatever the setting says" ;;
    K21) echo "URLSessionRangedAudioSampler: the cellular session admits Low Data Mode" ;;
    K22) echo "URLSessionFullEpisodeFetcher: use the cellular session regardless of the setting" ;;
    W01) echo "ingestPersistedAdWindows: the not-playing drop writes no durable row (the pre-isp5 state)" ;;
    W02) echo "receiveAdWindows: the inventory-filter drop is stamped without its rejection REASON" ;;
    W03) echo "endEpisode: clear the per-cause ingest tally too, so the measurement never survives an episode" ;;
    W04) echo "AdWindowIngestOutcome.isDelivered: count an already-bannered DROP as a delivery" ;;
    W05) echo "forwardPersistedAdWindows: write the census BEFORE receiveAdWindows, so every row reads unstamped" ;;
    W06) echo "AdWindowIngestCensus.auditDescription: drop the detail rendering, so the reason never reaches the file" ;;
    W07) echo "forwardPersistedAdWindows: make the empty-store preload durable again (a session file per episode start)" ;;
    B01) echo "InventorySanityFilter rule (b) head: reject on the span's START again, so every pre-roll dies as tooEarly" ;;
    B02) echo "InventorySanityFilter rule (b) tail: reject on the span's END again, so every post-roll dies as tooLate" ;;
    B03) echo "InventorySanityFilter rule (b) head: relax <= to <, so a span exactly filling the margin band escapes" ;;
    B04) echo "InventorySanityFilter rule (b) tail: relax >= to >, so a span exactly filling the tail band escapes" ;;
    B05) echo "SkipOrchestrator.init: restore the disabled-filter default, so tests stop observing what the field runs" ;;
    B06) echo "InventorySanityFilter.productionDefaultConfiguration: pin it OFF, so the shared constant lies" ;;
    B07) echo "hasValidRuntimeWindowMaterial: drop the startTime >= 0 check, so impossible geometry is admitted" ;;
    X01) echo "AdLikelihoodScanOrder.weight: flatten every channel to 1.0, so a lexical cue ranks like a seam" ;;
    X02) echo "order: score by MAX instead of SUM, so independent signals agreeing on a region stop reinforcing" ;;
    X03) echo "neighbourhoods: drop the seed-width ceiling, so an episode-wide sponsor span names the episode" ;;
    X04) echo "order: drop the filler loop, so promotion becomes a FILTER and un-promoted windows are starved" ;;
    X05) echo "order: invert the start tie-break, so equally-scored windows resolve to the LAST one" ;;
    X06) echo "neighbourhoods: collapse the radius, so a seam no longer reaches the pod that starts after it" ;;
    X07) echo "order: remove the promoted-prefix audio cap, so a cue-dense episode reorders wholesale" ;;
    X08) echo "order: drop the always-promote-one carve-out, so an oversize seeded window is refused" ;;
    X09) echo "neighbourhoods + order: relax both zero-score guards, so an inert seed reorders the sweep" ;;
    X10) echo "seeds: build the evidence anchor from its episode-wide coverage span, not its own position" ;;
    X11) echo "AdLikelihoodSeed.init: trust the caller's strength instead of clamping it to [0,1]" ;;
    X12) echo "restoreOrder: return the items untouched, so the REPORTED plan list keeps attempt order" ;;
    X13) echo "planPassA: return the packed plans directly, so the ordering is computed and thrown away" ;;
    X14) echo "BackfillJobRunner: scope seed derivation to a phase cold start never reaches — the pre-lxkq defect" ;;
    X15) echo "BackfillJobRunner: drop the feature flag from the seed guard, so OFF no longer restores the sweep" ;;
    X16) echo "AdDetectionConfig.default: ship the scan order OFF" ;;
    Z01) echo "sweep: reclaim on the FIRST nil owner instead of deferring one sweep" ;;
    Z02) echo "adopt: keep the stale orphan mark, so the new owner gets no deferral" ;;
    Z03) echo "sweep: reclaim UNOWNED entries too, with nothing proving them idle" ;;
    Z04) echo "forceRemove: give up on the first failure instead of repairing permissions" ;;
    Z05) echo "makeReadableAndWritable: chmod the top directory only, do not recurse" ;;
    Z06) echo "init: drop the sweepEvery clamp, so 0 disables sweeping entirely" ;;
    Z07) echo "register: never trigger a sweep" ;;
    Z08) echo "makeTestStoreWithDirectory: register the directory but do not adopt it" ;;
    Z09) echo "makeTempDir: do not register a directory that has no owner" ;;
    Z10) echo "makeTempDir: ignore ownedBy: and merely register" ;;
    Z11) echo "adopt: no-op when the URL was never registered" ;;
    Z12) echo "wipeTestScratchRoot: plain removeItem again, so an unreadable leftover survives forever" ;;
    A01) echo "votingWindows: drop the coarse-pass filter, so a pass-B refinement votes on presence again" ;;
    A02) echo "votingWindows: drop the didExamineWindow filter, so a no-work sentinel votes there is no ad" ;;
    A03) echo "votingWindows: overlap test '>' -> '>=', so a row that merely abuts the span votes" ;;
    A04) echo "votingWindows: hardcode the band to .moderate, so a degraded transcript votes at full strength" ;;
    A05) echo "votingWindows: invert the pass filter, so ONLY refinements vote and no coarse verdict does" ;;
    A06) echo "makeRefinementScanResult: hardcode transcriptQuality .good again" ;;
    A07) echo "SkipEligibilityGate: blockedByFMConsensus severity back to 1 (ties with markOnly)" ;;
    A08) echo "SkipEligibilityGate: drop the legacy raw-value alias, orphaning every persisted row" ;;
    A09) echo "SkipEligibilityGate: widen the alias, so EVERY unknown raw value decodes to the FM-consensus block" ;;
    A10) echo "applyFMSuppression: build the windows inline again, bypassing the admission rule" ;;
    A11) echo "receiveAdWindows: route blockedByFMConsensus to the suggest tier (the surface the bead closed)" ;;
    LE01) echo "receiveAdWindows: an inventory-REJECTED span also arms a suggestion — census stamp left intact, so only the suggest-tier witness can see it" ;;
    LE02) echo "receiveAdWindows: the inventory census always names tooShort, so four unrelated rejection reasons become one" ;;
    LE03) echo "retroactive sweep: stamp droppedInventorySanity, erasing the difference between 'never admitted' and 'admitted then taken back'" ;;
    LE04) echo "ingestPersistedAdWindows: drop the doorDroppedNotPlaying stamp, so 'the door refused' and 'the door never fired' are one observation again" ;;
    LE05) echo "forwardPersistedAdWindows: mis-attribute an empty admission set to doorDroppedEpisodeReplaced" ;;
    LE06) echo "receiveAdDecisionResults: an inventory-rejected decision arms a suggestion instead of reaching no tier" ;;
    LE07) echo "endEpisode: stop clearing suggestWindows, so an arming leaks into the next episode" ;;
    LE08) echo "receiveAdWindows: route .applied rows to the suggest tier (the A11 shape at the tier-routing if)" ;;
    C01) echo "post-roll guard: DELETE the byte-anchored exemption — the old blanket demotion that would delete Dan's post-roll" ;;
    C02) echo "post-roll guard: INVERT the exemption, so only byte-exact tails are demoted and everything else is exempt" ;;
    C03) echo "post-roll guard: widen the exemption to ANY width oracle, so an acoustic .spliceSlot tail auto-skips" ;;
    C04) echo "post-roll guard: widen the exemption to ANY provenance, so only a bare span is still guarded" ;;
    C05) echo "post-roll guard: implement the exemption as a PROMOTION rather than a declined demotion" ;;
    C06) echo "carriesRediffByteExactWidth: count acoustic .spliceSlot as byte-exact (the shared key four carve-outs read)" ;;
    C07) echo "SpanExtentSupport.derive: keep the edge claim through a finalizer geometry rewrite" ;;
    C08) echo "withExtentSupport: invert the blocking flag, so an unanchored edge no longer blocks auto-skip" ;;
    C09) echo "gateAndDiffBytes: default recoverNonMonotonicSegments to true, admitting 9s6q slots on the LAGGED path" ;;
    C10) echo "strictByteExactMask: mark every unioned slot strict, so a segment-recovered day-0 slot earns an anchor" ;;
    F01) echo "un-ship the flip: AdDetectionConfig.default back to certaintyTieredSkipEnabled: false" ;;
    F02) echo "diverge the init default from .default: certaintyTieredSkipEnabled: Bool = false" ;;
    F03) echo "disarm the host-read half: shipped hostReadConfidenceFloor 0.9 -> 0.0 (nothing is below 0)" ;;
    F04) echo "disarm the post-roll half: shipped postRollGuardSeconds 90.0 -> 0.0 (no tail is within 0s)" ;;
    F05) echo "make the flip TOTAL: shipped hostReadConfidenceFloor 0.9 -> 2.0, above any skipConfidence" ;;
    F06) echo "leak the flip: FusionWeightConfig's own init default certaintyTieredEnabled -> true" ;;
    F07) echo "host-read floor: < becomes <=, so a span AT the calibrated floor demotes" ;;
    F08) echo "host-read floor: drop the rediff carve-out, so a byte-exact span below the floor demotes" ;;
    F09) echo "post-roll guard: guess the episode end — treat an unknown duration as span.endTime" ;;
    G01) echo "6qvf: the chroma differ arm stamps .rediffSlot again — THE shipped defect" ;;
    G02) echo "6qvf: the rewrite site hardcodes .rediffSlot instead of the differ's provenance" ;;
    G03) echo "6qvf: arms swapped — the BYTE differ stamps .rediffSlotChroma" ;;
    G04) echo "6qvf: carriesRediffByteExactWidth widened to accept the chroma marker too" ;;
    G05) echo "6qvf: carriesRediffChromaWidth reads the BYTE marker (copy-paste)" ;;
    G06) echo "6qvf: chroma encodes under the 'rediffSlot' type string — the unsafe migration direction" ;;
    G07) echo "6qvf: SpanExtentSupport.derive keys on isWidthOwnership, re-admitting chroma to deterministic" ;;
    G08) echo "6qvf: drop .rediffSlotChroma from isWidthOwnership — chroma loses width ownership" ;;
    G09) echo "6qvf: remove the (.rediffSlotChroma, .rediffSlotChroma) Equatable arm (default:false trap)" ;;
    G10) echo "6qvf: the host-read floor re-grants its exemption to ANY width oracle" ;;
    H01) echo "9v09: the retroactive sweep stops STAMPING — the exact silence the bead closed" ;;
    H02) echo "9v09: the retroactive sweep stops writing its durable census ROW" ;;
    H03) echo "9v09: the sweep's row loses the filter's REJECTION REASON" ;;
    H04) echo "9v09: the sweep's per-window stamp loses the rejection reason (detail: nil)" ;;
    H05) echo "9v09: a PASSED verdict retires too — an outcome that fires on every sweep" ;;
    H06) echo "9v09: AdWindowIngestCensus.retiredCount keys on isDelivered, not isRetraction" ;;
    H07) echo "9v09: retired= is rendered on EVERY row, so a delivery row claims retired=0" ;;
    H08) echo "9v09: armedSuggest is classified a retraction — the balance eats its own numerator" ;;
    H09) echo "9v09: the retirement is classified DELIVERED, inflating every delivered= figure" ;;
    H10) echo "9v09: the sweep row reports forwarded=0 while its counts say otherwise" ;;
    I01) echo "gard: byte-exact rediff consults the show trust history again — the shipped defect" ;;
    I02) echo "gard: NO class consults show trust — every show silently gains auto on upgrade" ;;
    I03) echo "gard: deterministic on EITHER edge, so an invented end reads as byte-exact" ;;
    I04) echo "gard: an unrecognised boundary state is admitted to the exempt class" ;;
    I05) echo "gard: every veto weighs 1 again — the certainty weighting deleted" ;;
    I06) echo "gard: the exempt class inherits the poisoned legacy scalar on migration" ;;
    I07) echo "gard: EVERY class seeds clean on migration — an upgrading user gains auto-skip" ;;
    I08) echo "gard: the per-detector ledger never persists" ;;
    I09) echo "gard: per-detector demotion is inert — trust that never demotes anything" ;;
    I10) echo "gard: a correct observation stops decaying the counter — the one-way door returns" ;;
    I11) echo "gard: the skip gate reads the show scalar again, not the detector's mode" ;;
    I12) echo "gard: the banner confirm records no correct observation — the escape unwired" ;;
    I13) echo "gard: a veto blames nobody — the show scalar still moves, attribution is lost" ;;
    I14) echo "gard: no materialization, so blame leaks back through the seed one hop later" ;;
    I15) echo "gard: a user override leaves the stale counters, so the next veto undoes it" ;;
    I16) echo "gard: a multi-class veto takes the WEAKEST tier, laundering a real miss" ;;
    I17) echo "gard: an inferred revert weighs as much as an explicit one" ;;
    I18) echo "gard: per-detector resolution is inert — every class answers with the show mode" ;;
    I19) echo "gard: a lookup FAILURE grants the exempt class auto" ;;
    I21) echo "gard: a show with NO profile row gets an empty per-detector map — first listen sends the exempt class to shadow" ;;
    I20) echo "gard: a session override never reaches the per-detector map — the stale episode map governs" ;;
    KN01) echo "kanf: the promotion guard deleted at BOTH sites — the tap re-ranks a running job" ;;
    KN02) echo "kanf: 'state is enough' — the lease half of the guard dropped at both sites" ;;
    KN03) echo "kanf: the mirror image — the state half dropped, so paused/terminal/crashed rows promote" ;;
    KN04) echo "kanf: already-at-the-floor collapses into refused, so the one-shot flag is never spent" ;;
    KN05) echo "kanf: a refusal is reported as served — the flag is burned on a leased row" ;;
    KN06) echo "kanf: THE defect restored — skip the promotion and call the intent served" ;;
    KN07) echo "kanf: the flag is consumed unconditionally again, so a refused tap cannot retry" ;;
    KN08) echo "kanf: promote to the SOON floor — the row keeps starving in a deferred lane" ;;
    KN09) echo "kanf: the promotion rotates generationID, breaking the orphan-recovery join" ;;
    TS01) echo "5n8k: THE historical defect restored — install assigns a process-global and restores it in a defer" ;;
    TS02) echo "5n8k: the leak variant — the funnel caches the last binding in a process-global and never restores" ;;
    TS03) echo "5n8k: the funnel drops every diagnostic — the vacuity control on all four positive witnesses" ;;
    TS04) echo "5n8k: the emit moves onto a detached task, where the task-local binding is out of scope" ;;
    CK01) echo "26od: the DISPATCHING coarse call site — the only one production takes — loses the checkpoint observer" ;;
    CK02) echo "26od: the defuse flag is checked on entry only, so an orphaned pass writes the rest of its bank" ;;
    CK03) echo "26od: the post-loop defuse guard goes, so a late checkpoint undoes t1kq's cursor rewind" ;;
    CK04) echo "26od: every checkpoint rewrites the whole banked prefix — O(W^2) writes across a pass" ;;
    CK05) echo "26od: a refless outcome is a subset of every plan, so it credits the episode's only plan" ;;
    CK06) echo "26od: attribution returns to the FIRST superset, so a shared line ref credits an unscreened plan" ;;
    CK07) echo "26od: the qk44 lease touch loses its lifetime bound and resurrects a deferred job" ;;
    CK13) echo "26od: the lease REFRESH itself goes — the observer ticks and updates nothing" ;;
    CK09) echo "26od: the mid-flight walk stops seeing failures, so a partially-recovered plan reads as covered" ;;
    CK10) echo "26od: the monotonic merge goes, so a resumed pass can overwrite a higher cursor with a lower one" ;;
    CK11) echo "26od: the checkpoint row hardcodes runMode, mislabelling the only row a killed pass leaves" ;;
    CK12) echo "26od: the snapshot splits at the OFFERED count, crediting a window whose row never landed" ;;
    SC01) echo "fil5: VACUITY AUDIT — an UNMEASURED ad scan reads as sufficient, so the never-scanned asset never claims" ;;
    SC02) echo "fil5: the claim floor drifts onto the transcript's 0.95" ;;
    SC03) echo "fil5: the transcript gate borrows the ad-scan 0.98, leaving the two field assets it reaches 0.005 of margin" ;;
    SC04) echo "fil5: an unmeasurable transcript reads as ready, so the sweep claims assets it cannot judge" ;;
    SC05) echo "fil5: the claim gets a private jobId, so the runner never re-drives it and it sits deferred forever" ;;
    SC06) echo "fil5: the version is hashed from RAW chunks, so any final-passed asset names a job that does not exist" ;;
    SC07) echo "fil5: the shared hash stops ordering its input — right on sorted fixtures, wrong on 27 of 30 device assets" ;;
    SC08) echo "fil5: a closed gate is charged as a failed attempt and spends the resumability budget" ;;
    SC09) echo "fil5: an absent podcast persists as a podcast whose id is the empty string" ;;
    SC10) echo "fil5: the FM-unavailable gate stops recording — the field gate, and the drop was permanent" ;;
    SC11) echo "fil5: the missing-podcastId gate stops recording" ;;
    SC12) echo "fil5: the missing-runner-factory gate stops recording" ;;
    SC14) echo "fil5: the sweep runs AFTER the re-drive step, so every claim waits a launch to become work" ;;
    SC15) echo "fil5: the selector drops the transcript requirement and claims over audio nobody has read" ;;
    SC16) echo "fil5: a COMPLETE coverage lane reads as missing, so a finished asset is re-claimed every launch" ;;
    SC17) echo "fil5: the selector tiebreaks on id, so same-second assets drain by UUID rather than by age" ;;
    SC18) echo "fil5: record() stops asking whether a scan is owed" ;;
    SC19) echo "fil5: a complete row is treated as claimable, reopening finished work" ;;
    SC20) echo "fil5: a running row is rewritten under a pass that is mid-flight and owns its state machine" ;;
    SC21) echo "fil5: the sweep's transcript-floor guard goes, spending an ad-scan re-drive on transcription" ;;
    SC22) echo "fil5: claims are counted as recovered work, reporting one repair twice to the BG ledger" ;;
    SC23) echo "fil5: the mode-off guard inside runShadowFMPhase stops recording — the shadow-retry drain's only gate" ;;
    SC24) echo "fil5: the mode-off gate in runBackfill stops recording" ;;
    SC25) echo "fil5: the sweep's transcript gate reads the watermark, so a gappy transcript claims at 100%" ;;
    SC30) echo "fil5: the sweep's numerator reverts to the RAW union — 0 of 12 field assets clear the floor" ;;
    SC31) echo "fil5: the bridge width goes to 0, making the shared helper the raw union for every caller" ;;
    SC32) echo "fil5: the candidate cursor stops advancing, so a window of permanent rejects walls off the rest" ;;
    SC33) echo "fil5: bridging widens until it conceals a genuine untranscribed block" ;;
    SC34) echo "fil5: the candidate cursor stops wrapping, so a shrunken population costs a whole idle pass" ;;
    SC26) echo "fil5: the sweep's per-pass mint cap goes, emptying the window into a queue that drains 8" ;;
    SC27) echo "fil5: the candidate window widens to 64, paying a transcript read per reject" ;;
    SC28) echo "fil5: the sweep claims episodes whose analysis pass is still in flight" ;;
    RT01) echo "9y9e: the short-circuit goes — a finished transcript is a transcription failure again and Stage 4 never runs" ;;
    RT02) echo "9y9e: VACUITY AUDIT — the short-circuit's floor goes, so every negative runner fixture must go red" ;;
    RT03) echo "9y9e: the gate divides the WATERMARK, not the bridged area — a gappy transcript reads as finished" ;;
    RT04) echo "9y9e: the interruption carve-out goes, so a scrub buys a full detection pass on the only slot" ;;
    RT05) echo "9y9e: the short-circuit journals a FAILED row again — the asr_failed misattribution, behaviour intact" ;;
    RT06) echo "9y9e: the ad-scan area is bounded by the FAST union again, so a final-passed episode is unscannable forever" ;;
    RT07) echo "9y9e: the ad-scan bound stops CLIPPING — one window's bounds claim audio nobody transcribed" ;;
    RT08) echo "9y9e: fetchTranscriptCoveredRanges becomes fast-only, answering the wrong question for every caller" ;;
    RT09) echo "9y9e: the cap-out rescue measures outstanding TRANSCRIPT only — the one thing an unscanned episode has none of" ;;
    RT10) echo "9y9e: the ad-scan term overrides the transcript ladder instead of falling back to it" ;;
    RT11) echo "9y9e: the ad-scan term jumps the terminal, budget and cooldown guards" ;;
    RT12) echo "9y9e R1: the widened bound drops the watermark fallback, so the ad-scan area can DECREASE" ;;
    RT13) echo "9y9e R1: the final-pass loop keeps degenerate rows, so a zero-width chunk becomes a pass reach" ;;
    RT14) echo "9y9e R2: the duration-sanity guard reads the FAST reach while the numerator spans both passes" ;;
    RT15) echo "9y9e R3: the reconciler stops MEASURING the ad scan and hands the decision a constant 1.0" ;;
    RT16) echo "9y9e R3: the both-pass reach guard trusts a final WATERMARK with no chunks behind it" ;;
    RT17) echo "9y9e R3: the reconciler skips the ad-scan read, so the rescue mints on a finished episode forever" ;;
    UC01) echo "41mu WIRE-IN: the terminal stops measuring and completes whatever runJob returned (the pre-bead behaviour verbatim)" ;;
    UC02) echo "41mu: the phase scope is dropped, so narrow targeted phases are judged against an episode-wide floor" ;;
    UC03) echo "41mu: a coverage read that THREW is treated as evidence the episode was read" ;;
    UC04) echo "41mu: the attempt budget never terminates, so an under-coverable episode re-drives forever" ;;
    UC05) echo "41mu: the cursor is published over the unscanned head — 53FC53E3's 2,525.82-on-2,528 s row, verbatim" ;;
    UC06) echo "41mu: the cursor's hole test drops the bridge tolerance, so leading silence freezes a genuine prefix" ;;
    UC07) echo "41mu VACUITY CONTROL: the terminal refuses unconditionally, deferring episodes that were genuinely read" ;;
    UC08) echo "41mu R1: a non-finite measured fraction completes the job again — the terminal alone reads an absence as 'read'" ;;
    UC09) echo "41mu R2: the cursor's head test reads the caller's PRE-narrowing segment list again, so no resume can ever detect a hole above its cursor" ;;
    CN01) echo "iu0t: THE SHIPPED DEFECT — the drain replays the final-only chunk set again, verbatim (53FC53E3's discarded 2,490 s)" ;;
    CN02) echo "iu0t: the drain replays fast-only, throwing the re-transcription away — right reach, wrong identity" ;;
    CN03) echo "iu0t: the drain replays the RAW rows, so overlapped audio is scanned twice and the version drifts from runBackfill's" ;;
    CN04) echo "iu0t VACUITY CONTROL: the replay set is empty, so the drain never runs at all" ;;
    CN06) echo "iu0t R1: the FOURTH collapse — pushEvidenceCatalog builds the banner evidence catalog from the final-only slice again" ;;
    CN05) echo "iu0t R2: the THIRD collapse restored at runPhase5ProjectorPhase — killable only once the allow-list is keyed per DECLARATION, not per file" ;;
    CN07) echo "iu0t R2: the FIFTH collapse — the episode-summary invalidation key read back out of the final-only persisted transcriptVersion column" ;;
    CN08) echo "iu0t R3: CN07 RESPELLED with a closure instead of a keypath — the same read of the same column, which survived while CN07 died" ;;
    CN09) echo "iu0t R4: CN06 RESPELLED with a named closure parameter — the collapse shape rule matched two literal spellings and not this one" ;;
    CN10) echo "iu0t R5: an uncanonicalized atomize call SPLIT ACROSS LINES — the site finder was a literal substring, so one line break removed the call site from the walk" ;;
    CN11) echo "iu0t R5: CN06 spelled as a NEGATION (pass != fast) — the same collapse as instance #5, which the collapse-shape pattern required == to see" ;;
    CN12) echo "iu0t R5: LAUNDERING — the site canonicalizes and then filters to final anyway, which passed because the check tested for the TOKEN canonicalize( rather than the value" ;;
    DR01) echo "e75l: the drain classifies only THROTTLES again — the pre-e75l line verbatim, so a wedged tokenizer is marked failed and spends a lifetime retry" ;;
    DR02) echo "e75l: the discriminator keys on the TYPE, not the budget — every FMInferenceTimeoutError becomes a daemon refusal, including the 300s inference one" ;;
    DR03) echo "e75l: the discriminator reads the OTHER budget — a standard-deadline timeout defers and a metadata stall fails" ;;
    DR04) echo "e75l: swept siblings carry kvs8's rateLimited-batchSibling again — a rate limit recorded for a batch stopped by a wedged tokenizer" ;;
    DR05) echo "e75l: the per-job cause is kvs8's rateLimited-prologue again — the stall answers to the prefix an operator counts rate limits with" ;;
    DR06) echo "e75l: the drain-stop log EVENT is the hard-coded ..._by_throttle again — the name fires for a condition that is not a throttle" ;;
    DR07) echo "e75l: only throttles advance the consecutive counter — two counters that can disagree about whether the daemon is serving us" ;;
    DR08) echo "e75l R2: the PER-JOB log event is a hard-coded literal — R1's canary named two kvs8-era spellings, so it could not see a literal of the event this bead added" ;;
    DR09) echo "e75l R2: the durable token joins playhead-8d5r's inferenceTimeout- family — the prefix an operator greps to count a model that is not answering" ;;
    DR10) echo "e75l R3: the durable token joins playhead-41mu's underCoverage- family — one of the four families R2's hand-written foreign list forgot" ;;
    DR11) echo "e75l R4: the two log-event properties are SWAPPED — no literal anywhere, both still read, and every refused JOB is counted as a drain stop" ;;
    DR12) echo "e75l R4: the drain-stop event is hard-coded again but SPLIT across a backslash continuation, so no one line holds the name" ;;
    DR13) echo "e75l R4: a third 30s budget as FMInferenceDeadline.standard / 10 — the line MENTIONS an allowed budget, which is all the old check asked" ;;
    DR14) echo "e75l R4: an ADDED 30s call site spelled across two lines — invisible to a per-line finder, and free because the site count is a floor" ;;
    *)   echo "(no description)" ;;
  esac
}

# ---------------------------------------------------------------------------
# Patch primitive: exact, single-occurrence string replacement.
# ---------------------------------------------------------------------------
patch() {
  local file="$1" old="$2" new="$3"
  MB_FILE="$file" MB_OLD="$old" MB_NEW="$new" python3 -c '
import os, sys
path = os.environ["MB_FILE"]
old = os.environ["MB_OLD"]
new = os.environ["MB_NEW"]
src = open(path, encoding="utf-8").read()
n = src.count(old)
if n != 1:
    sys.stderr.write(
        "mutation-battery: anchor matched %d times in %s (need exactly 1)\n"
        % (n, path)
    )
    sys.stderr.write("---- anchor ----\n%s\n----------------\n" % old)
    sys.exit(3)
open(path, "w", encoding="utf-8").write(src.replace(old, new, 1))
'
}

# Multi-line snippets are read with `read -r -d ''`, which always returns 1 at
# EOF; `|| true` keeps `set -e`-style habits from tripping over it.
snippet() { IFS= read -r -d '' "$1" || true; }

# A mutation with TWO sites (M13, M17, N06) must abort on the first failed
# anchor. Without this, `apply_mutation` returns only the LAST patch's status,
# so a drifted first anchor yields a HALF-APPLIED mutation that reports success
# — and a half-mutation that survives is a fabricated coverage hole, while one
# that kills is a fabricated rail. Use `patch … || return $?` for every patch
# but the last in a case arm.

apply_mutation() {
  local name="$1" file="$2" OLD NEW
  case "$name" in

  # ---- playhead-hx6n: scan-row run attribution (T series) ----

  # T01 — THE one-line mutation the whole bead reduces to: read a nil scene
  # phase as `.foreground`. Nothing crashes, every number still prints, and the
  # entire pre-V42 corpus silently becomes a foreground measurement.
  T01)
    snippet OLD <<'EOF'
        guard let scenePhase else { return .unattributed }
EOF
    snippet NEW <<'EOF'
        guard let scenePhase else { return .foreground }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T02 — `.unknown` is a RECORDED non-answer; calling it foreground invents an
  # attribution the row explicitly declines to make.
  T02)
    snippet OLD <<'EOF'
        case .unknown:           return .unattributed
EOF
    snippet NEW <<'EOF'
        case .unknown:           return .foreground
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T03 — the same defect one layer lower, on the READ. A `?? .active` here is
  # far more plausible-looking than T01 (it reads like defensive coding) and is
  # invisible to any test that only checks in-memory rows.
  T03)
    snippet OLD <<'EOF'
        let scenePhase = optionalText(stmt, 23).flatMap(ScanScenePhase.init(rawValue:))
EOF
    snippet NEW <<'EOF'
        let scenePhase = ScanScenePhase(rawValue: optionalText(stmt, 23) ?? ScanScenePhase.active.rawValue)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T04 — the store invents a phase at the WRITE. It cannot know one, so this
  # manufactures attribution rather than recording it.
  T04)
    snippet OLD <<'EOF'
        bind(stmt, 24, result.scenePhase?.rawValue)
EOF
    snippet NEW <<'EOF'
        bind(stmt, 24, result.scenePhase?.rawValue ?? ScanScenePhase.active.rawValue)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T05 — drop the createdAt backstop. Post-V42 rows written by a caller with no
  # clock become indistinguishable on disk from pre-V42 rows, and the
  # attributable corpus quietly shrinks with nothing to notice.
  T05)
    snippet OLD <<'EOF'
        bind(stmt, 23, result.createdAt ?? now)
EOF
    snippet NEW <<'EOF'
        bind(stmt, 23, result.createdAt)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T06 — read a NULL createdAt through `sqlite3_column_double`, which returns
  # 0.0. Every pre-V42 row then claims 1970 and sorts as the oldest thing in the
  # database — a fabricated timestamp that a stall-timeline reconstruction would
  # trust completely.
  T06)
    snippet OLD <<'EOF'
            createdAt: optionalDouble(stmt, 22),
EOF
    snippet NEW <<'EOF'
            createdAt: sqlite3_column_double(stmt, 22),
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T07 — the V42 rung stops stamping head. On a fresh install `createTables()`
  # masks it completely; only the isolated ladder can see the version stall.
  T07)
    snippet OLD <<'EOF'
        try setSchemaVersion(42)
EOF
    snippet NEW <<'EOF'
        try setSchemaVersion(41)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T08 — the seam stops stamping the join key. The columns still exist and the
  # rows still land; the join simply resolves to nothing, which is exactly as
  # useless as having no column.
  T08)
    snippet OLD <<'EOF'
            runCorrelationId: jobId
EOF
    snippet NEW <<'EOF'
            runCorrelationId: nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T09 — the seam stops stamping the phase. Every production row reads as
  # unattributed, indistinguishable from a pre-V42 row, and the bead ships as a
  # schema change that measures nothing.
  T09)
    snippet OLD <<'EOF'
            scenePhase: phase,
EOF
    snippet NEW <<'EOF'
            scenePhase: nil,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T10 — the seam guesses when the provider breaks its vocabulary contract. A
  # broken contract is precisely when a guess is least defensible.
  T10)
    snippet OLD <<'EOF'
        let phase = ScanScenePhase(rawValue: phaseRaw)
EOF
    snippet NEW <<'EOF'
        let phase = ScanScenePhase(rawValue: phaseRaw) ?? .active
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T11 — the Swift consumer admits no-work sentinels. A sentinel spans a range
  # it never examined, so its ~zero latency over a whole-episode window reports
  # a model of spectacular speed that never ran (playhead-pz32).
  T11)
    snippet OLD <<'EOF'
            && !row.isNoWorkSentinel
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T12 — the same admission on the SQL side. This is the one that makes the
  # documented query answer a different question from the shipped consumer while
  # both keep returning plausible numbers.
  T12)
    snippet OLD <<'EOF'
              AND (errorContext IS NULL OR errorContext NOT LIKE 'noWork:%')
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T13 — a zero denominator reports 1.0 instead of nil. A ratio of exactly 1.0
  # reads as "realtime" and is the single most dangerous value this type can
  # produce; see feedback_ask_what_the_quantity_measures_2026-07-29.
  T13)
    snippet OLD <<'EOF'
        guard audioSeconds > 0 else { return nil }
EOF
    snippet NEW <<'EOF'
        guard audioSeconds > 0 else { return 1 }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T14 — an empty corpus claims to be fully attributed. This is the number a
  # reader consults BEFORE believing the split, so a confident 1.0 here is what
  # makes every other number in the report look trustworthy.
  T14)
    snippet OLD <<'EOF'
        guard totalScanCount > 0 else { return nil }
EOF
    snippet NEW <<'EOF'
        guard totalScanCount > 0 else { return 1 }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # T15 — the SQL side OVERWRITES the unattributed bucket instead of summing
  # into it. `GROUP BY scenePhase` yields the NULL group and the 'unknown' group
  # separately, and both belong in the same bucket; an assignment silently drops
  # whichever arrives first.
  T15)
    snippet OLD <<'EOF'
            case .unattributed: split.unattributed.merge(group)
EOF
    snippet NEW <<'EOF'
            case .unattributed: split.unattributed = group
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-bllt (BL series) -------------------------------------------

  # BL01 — THE SHIPPED DEFECT, restored. The gate is present, its call sites are
  # wired, and it decides nothing. This is what the hot path did before this
  # bead: an `"autoSkip"` verdict over two invented edges went straight into the
  # managed tier. It must be killed by BOTH suites — the pure one (the
  # transition is gone) and the integration one (the persisted row is autoSkip
  # again) — because a gate that is present but inert is exactly the shape that
  # a single-suite battery reports as healthy.
  BL01)
    snippet OLD <<'EOF'
        guard blockingUnanchoredAutoSkip,
              !extent.isFullyAnchored,
              label == autoSkipLabel
        else { return label }
        return markOnlyLabel
EOF
    snippet NEW <<'EOF'
        return label
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # BL02 — the demotion becomes a DROP. Strictly fewer auto-skips, so
  # monotonicity alone is satisfied; the row simply never exists, and the
  # listener hears the ad with no banner. Trading a wrong skip for a missed ad
  # is a different decision than the one this bead was given.
  BL02)
    patch "$file" \
      "        return markOnlyLabel" \
      "        return nil" ;;

  # BL03 — the condition inverted. Note what this does NOT trip: the whole-domain
  # monotonicity sweep stays GREEN, because passing an `"autoSkip"` through is
  # not a rise. That is the entry that proves the "EXACT" and "anchored"
  # assertions are load-bearing rather than decoration.
  BL03)
    patch "$file" \
      "              !extent.isFullyAnchored," \
      "              extent.isFullyAnchored," ;;

  # ---- playhead-ug9m (UG series) -------------------------------------------

  # UG01 — THE CENTREPIECE NEGATIVE, at the policy tier. One anchored day-0 row
  # proves the mint could stamp anchors and did, which makes its unanchored
  # siblings 9s6q segment-recovered and DELIBERATELY withheld. Dropping the
  # conjunct rescues those assets, re-derives the same recovered slots, and
  # spends ~108 MB doing it.
  UG01)
    patch "$file" \
      "        anchoredCount == 0 && degradedCount > 0" \
      "        degradedCount > 0" ;;

  # UG02 — a re-mint replaces a row that is ALREADY proven. Strictly worse than
  # doing nothing: this fetch's geometry has no claim over the one already
  # anchored, and the swap is invisible in any count of rows.
  UG02)
    patch "$file" \
      "        isDayZeroByteExactMark(row) && !isSettled(row) && !isFullyAnchored(row)" \
      "        isDayZeroByteExactMark(row) && !isSettled(row)" ;;

  # UG03 — the fidelity ladder, inverted. A user veto is deleted by a re-fetch.
  UG03)
    patch "$file" \
      "        isDayZeroByteExactMark(row) && !isSettled(row) && !isFullyAnchored(row)" \
      "        isDayZeroByteExactMark(row) && !isFullyAnchored(row)" ;;

  # UG04 — the relaxation escapes its own producer and starts retiring other
  # detectors' windows.
  UG04)
    patch "$file" \
      "        isDayZeroByteExactMark(row) && !isSettled(row) && !isFullyAnchored(row)" \
      "        !isSettled(row) && !isFullyAnchored(row)" ;;

  # UG05 — one proven edge is read as two. The row then counts as `anchored`,
  # which BLOCKS its own rescue while its other edge is still invented.
  UG05)
    snippet OLD <<'EOF'
        return support.startAnchor == .rediffByteExact
            && support.endAnchor == .rediffByteExact
EOF
    snippet NEW <<'EOF'
        return support.startAnchor == .rediffByteExact
            || support.endAnchor == .rediffByteExact
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UG06 — the generation scope falls off the rescue arm. `.marked` becomes
  # retryable on every replay within a generation, which is the ~108 MB-per-play
  # bleed playhead-p70f exists to prevent, reintroduced through the new door.
  UG06)
    snippet OLD <<'EOF'
            if record.lastExit == .marked,
               record.policyGeneration != currentGeneration,
               markCensus.isRescuable {
EOF
    snippet NEW <<'EOF'
            if record.lastExit == .marked,
               markCensus.isRescuable {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UG07 — the CEILING is removed. Everything else still reads correctly; the
  # rescue simply never stops, and `rescueExhausted` becomes unreachable.
  UG07)
    snippet OLD <<'EOF'
        if markCensus.hasMarks, record.rescueAttemptCount >= maxRescueAttempts {
            return .suppress(reason: .rescueExhausted, nextEligibleAt: nil)
        }
EOF
    snippet NEW <<'EOF'
        if false, record.rescueAttemptCount >= maxRescueAttempts {
            return .suppress(reason: .rescueExhausted, nextEligibleAt: nil)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UG08 — the counter never rises, so the ceiling above is present and can
  # never fire. The bound looks enforced and is not — the exact shape of the
  # defect this bead is fixing one level up.
  UG08)
    patch "$file" \
      "        let isRescue = record?.lastExit == .marked && spentBandwidth" \
      "        let isRescue = record?.lastExit == .noDivergentSlot && spentBandwidth" ;;

  # UG09 — the ceiling keyed on the EXIT rather than on the marks. A rescue that
  # ended in `.noDivergentSlot` then falls through into the ordinary
  # three-attempt budget: ~324 MB where the policy promised ~108 MB.
  UG09)
    patch "$file" \
      "        if markCensus.hasMarks, record.rescueAttemptCount >= maxRescueAttempts {" \
      "        if record.lastExit == .marked, record.rescueAttemptCount >= maxRescueAttempts {" ;;

  # UG10 — THE CENTREPIECE NEGATIVE, at the mint tier and the one that matters
  # most. Without the `strict` conjunct a 9s6q SEGMENT-RECOVERED re-mint retires
  # the existing row and persists its own — and since the anchor stamp and the
  # eligibility gate are chosen from the same `strict` value, the replacement is
  # an unanchored, mark-only row that DELETED a banner. A second draw that
  # dropped runs is not evidence it is better than the first.
  UG10)
    snippet OLD <<'EOF'
                guard strict,
                      overlappingExisting.allSatisfy(DayZeroMarkCensus.isSupersedable)
                else { continue }
EOF
    snippet NEW <<'EOF'
                guard overlappingExisting.allSatisfy(DayZeroMarkCensus.isSupersedable)
                else { continue }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UG11 — the mint computes the supersede set and then does not apply it. Both
  # rows survive: the degraded one keeps bannering beside the new anchored one,
  # so the listener gets a banner AND a skip over the same seconds.
  UG11)
    patch "$file" \
      "                retiredIDs: Set(superseded.keys)," \
      "                retiredIDs: []," ;;

  # UG12 — `frozen` reports as `rescuable`. Nothing behaves differently; the
  # permanently-frozen state simply stops being visible in a support pull, which
  # is precisely the silence this bead replaced.
  UG12)
    patch "$file" \
      "        return rescueAttemptCount < DayZeroRediffAttemptPolicy.maxRescueAttempts" \
      "        return rescueAttemptCount >= 0" ;;

  # UG13 — the two legs of the decision stop being one snapshot: the context
  # read reports an empty census, so `isRescuable` is never true and every
  # trapped asset stays frozen with the whole policy suite green.
  UG13)
    snippet OLD <<'EOF'
            markCensus: DayZeroMarkCensus.classify(
                rows: try fetchAdWindows(assetId: assetId)
            )
EOF
    snippet NEW <<'EOF'
            markCensus: DayZeroMarkCensus.classify(rows: [])
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UG14 — the bucketing ORDER, reversed. Reads as pure tidiness (three disjoint
  # buckets either way, every count still sums to the same total) and quietly
  # removes the proof that withholds the rescue: an asset whose only anchored
  # day-0 mark the user vetoed reads back `rescuable`, and the rescue then
  # re-derives exactly the 9s6q slots playhead-qs0d refused to promote.
  UG14)
    snippet OLD <<'EOF'
            if isFullyAnchored(row) {
                anchored += 1
            } else if isSettled(row) {
                settled += 1
EOF
    snippet NEW <<'EOF'
            if isSettled(row) {
                settled += 1
            } else if isFullyAnchored(row) {
                anchored += 1
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-pggn (PG series) --------------------------------------------
  #
  # Every one of these restores a state in which `segmentScore` describes a
  # different region than `[startTime, endTime]` — the defect, reached through
  # each of the four exits from an open segment plus the two directions of the
  # containment test. PG08 is the only one that leaves the aggregator.

  # PG01 — THE SHIPPED DEFECT, restored verbatim. The sub-continuation branch
  # scores the tail unconditionally while the emitted endTime still snaps back,
  # so a 30 s row carries a number describing 60 s.
  PG01)
    snippet OLD <<'EOF'
            if w.endTime <= seg.lastQualifyingEndTime {
                seg.include(w)
            } else {
                seg.deferBeyondExtent(w)
            }
EOF
    snippet NEW <<'EOF'
            seg.include(w)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # PG02 — the containment test is INVERTED: a window inside the extent is
  # deferred and one outside it is scored. Both halves of the invariant break at
  # once, in opposite directions.
  PG02)
    patch "$file" \
      "            if w.endTime <= seg.lastQualifyingEndTime {" \
      "            if w.endTime > seg.lastQualifyingEndTime {" ;;

  # PG03 — the deferred tail is DROPPED even when a later qualifying window
  # carries the extent past it. The score then describes LESS than the span it
  # reports — the same defect mirrored, and the reason the fix defers rather
  # than discards.
  PG03)
    snippet OLD <<'EOF'
                if w.endTime >= seg.tailMaxEndTime {
                    seg.foldPendingTail()
                } else {
                    seg.dropPendingTail()
                }
EOF
    snippet NEW <<'EOF'
                seg.dropPendingTail()
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # PG04 — the end-of-stream flush SCORES its pending tail. Only reachable when
  # the tail is shorter than the 3 s countdown, which is exactly the Tier-2
  # width regime.
  PG04)
    snippet OLD <<'EOF'
            seg.dropPendingTail()
            return Self.materialize(segment: seg, config: config)
EOF
    snippet NEW <<'EOF'
            seg.foldPendingTail()
            return Self.materialize(segment: seg, config: config)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # PG05 — the countdown close SCORES its pending tail. This is the exit the
  # bead's own worked example takes: one 30 s sub-continuation slot both trips
  # the countdown and is the thing being wrongly scored.
  PG05)
    snippet OLD <<'EOF'
                // End condition met. Close the segment. The pending tail is by
                // construction beyond `lastQualifyingEndTime`, which is where
                // the segment ends, so it is dropped rather than scored.
                seg.dropPendingTail()
EOF
    snippet NEW <<'EOF'
                // End condition met. Close the segment. The pending tail is by
                // construction beyond `lastQualifyingEndTime`, which is where
                // the segment ends, so it is dropped rather than scored.
                seg.foldPendingTail()
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # PG06 — the unbridgeable-gap close SCORES its pending tail. A third exit, and
  # the one no other mutation in this series reaches.
  PG06)
    snippet OLD <<'EOF'
                    seg.dropPendingTail()
                    finishedSlot = Self.materialize(segment: seg, config: config)
                    open = nil
                    ingestWhileIdle(w)
EOF
    snippet NEW <<'EOF'
                    seg.foldPendingTail()
                    finishedSlot = Self.materialize(segment: seg, config: config)
                    open = nil
                    ingestWhileIdle(w)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # PG07 — the denominator alone grows with the tail: the dropped region is
  # added to `totalDuration` without crediting its score. The sharpest form of
  # the original defect, and the one a reader of `include()` alone cannot see.
  #
  # NOTE the first attempt at this mutation was `meanScore` dividing by
  # `totalDuration + tailDuration`. That is INERT: every exit settles the tail
  # before `materialize` reads the mean, so `tailDuration` is always 0 there and
  # the edit changes nothing. It reported SURVIVED and was not a coverage hole.
  # The mutation has to land where the tail still has a value — inside
  # `dropPendingTail` itself.
  PG07)
    snippet OLD <<'EOF'
            mutating func dropPendingTail() {
                tailWeightedScoreSum = 0
EOF
    snippet NEW <<'EOF'
            mutating func dropPendingTail() {
                totalDuration += tailDuration
                tailWeightedScoreSum = 0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # PG08 — the aggregator claims BOTH edges are byte-exact. Its edges are
  # wherever the scoring windows happened to fall; it never observes a boundary.
  # With the score no longer diluted, this is the mutation that would turn a
  # 0.62 marker into an auto-skip over invented edges.
  PG08)
    snippet OLD <<'EOF'
            // a fused verdict does under playhead-2350. Same variable persisted
            // and consulted; see the single-window site for the full argument.
            let extentSupport = SpanExtentSupport.unanchored
EOF
    snippet NEW <<'EOF'
            // a fused verdict does under playhead-2350. Same variable persisted
            // and consulted; see the single-window site for the full argument.
            let extentSupport = SpanExtentSupport(
                startAnchor: .rediffByteExact,
                endAnchor: .rediffByteExact
            )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # BL04 — "a span is only as well-bounded as its weaker edge" replaced by "both
  # edges must be bad". A byte-exact start with an invented end then auto-skips
  # at the invented edge, which is the edge that clips the show.
  BL04)
    patch "$file" \
      "              !extent.isFullyAnchored," \
      "              extent.startTier == .none && extent.endTier == .none," ;;

  # BL05 — the kill switch stops killing. The flag is the SAME one playhead-2350
  # uses, so a mutation that ignores it here half-fires the switch: turning
  # `unanchoredExtentBlocksAutoSkip` off would silence fusion and not the hot
  # path. Killed by the two PRESENCE rails, which are the only tests that run
  # with the block held off.
  BL05)
    patch "$file" \
      "        guard blockingUnanchoredAutoSkip," \
      "        guard true," ;;

  # BL06 — the single-window site computes the gated label, logs about it, and
  # then persists the ungated one. The pure suite cannot see this: the rule is
  # perfect and simply unused.
  BL06)
    snippet OLD <<'EOF'
            guard let eligibilityGate = gatedLabel else { continue }
            adWindows.append(buildAdWindow(
EOF
    snippet NEW <<'EOF'
            guard let eligibilityGate = gateResult.label else { continue }
            adWindows.append(buildAdWindow(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # BL07 — the same defect at the OTHER producer. Two sites, two entries: a
  # battery that pinned only one would certify a fix that closed half the hole,
  # which is precisely how the hot path came to be missed by playhead-2350.
  BL07)
    snippet OLD <<'EOF'
            guard let eligibilityGate = gatedLabel else { continue }
            newWindows.append(
EOF
    snippet NEW <<'EOF'
            guard let eligibilityGate = gateResult.label else { continue }
            newWindows.append(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # BL08 — the label and the persisted anchors come apart. The row claims a
  # byte-exact start it does not have, so every downstream consumer that reads
  # the anchors (AutoSkipEdgePadding, SkipDetectorClass, the census) is lied to.
  # This is the rail for "one variable, persisted AND consulted" — the
  # playhead-6qvf lesson — and it cannot be seen by the pure suite at all.
  BL08)
    snippet OLD <<'EOF'
            startEdgeAnchor: extentSupport.startAnchor.rawValue,
            endEdgeAnchor: extentSupport.endAnchor.rawValue
        )
    }

    // MARK: - Precision-gate wiring (playhead-gtt9.11)
EOF
    snippet NEW <<'EOF'
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: extentSupport.endAnchor.rawValue
        )
    }

    // MARK: - Precision-gate wiring (playhead-gtt9.11)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # BL09 — the demotion goes back to being invisible. The row still lands in the
  # suggest tier and the banner still fires, so every assertion about BEHAVIOUR
  # stays green; what is lost is the audit trail's ability to say WHY, which is
  # the difference between this bead and the silence playhead-isp5 was filed
  # about.
  BL09)
    snippet OLD <<'EOF'
                let extentDetail = HotPathExtentGate.censusDetail(
                    for: resolvedExtentSupport(for: adWindow)
                )
EOF
    snippet NEW <<'EOF'
                let extentDetail: String? = nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # BL10 — the detail fires on every row, anchored or not. A sub-cause that is
  # always present carries no information, which is the same rule
  # `AdWindowIngestOutcome`'s header states about `retired=`.
  BL10)
    patch "$file" \
      "        guard !extent.isFullyAnchored else { return nil }" \
      "        guard true else { return nil }" ;;


  M01)
    snippet OLD <<'EOF'
            // this is strictly better, not complete.
            guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
                break
            }
EOF
    snippet NEW <<'EOF'
            // this is strictly better, not complete.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M02)
    snippet OLD <<'EOF'
            // captured source show.
            guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
                break
            }
EOF
    snippet NEW <<'EOF'
            // captured source show.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M03)
    snippet OLD <<'EOF'
            // this is strictly better, not complete.
            guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
                break
            }
EOF
    snippet NEW <<'EOF'
            // this is strictly better, not complete.
            guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
                return
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M04)
    snippet OLD <<'EOF'
            // captured source show.
            guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
                break
            }
EOF
    snippet NEW <<'EOF'
            // captured source show.
            guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
                return
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M05)
    snippet OLD <<'EOF'
        let sourceLifecycleIsCurrent =
            activeAssetId == expectedAssetId
EOF
    snippet NEW <<'EOF'
        let sourceLifecycleIsCurrent =
            sourceShowId == nil || trustService == nil ||
            activeAssetId == expectedAssetId
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M06)
    snippet OLD <<'EOF'
        let suggestRevertTargets: [(id: String, window: AdWindow)] =
            episodeLifecycleGeneration == sourceLifecycleGeneration
            ? suggestWindows.compactMap { (id, suggested) in
                guard suggested.startTime < end, suggested.endTime > start else { return nil }
                return (id, suggested)
            }
            : []
EOF
    snippet NEW <<'EOF'
        let suggestRevertTargets: [(id: String, window: AdWindow)] =
            suggestWindows.compactMap { (id, suggested) in
                guard suggested.startTime < end, suggested.endTime > start else { return nil }
                return (id, suggested)
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M07)
    snippet OLD <<'EOF'
        // Retire live state only if the exact source lifecycle and producer
        // revision still own the window after the durable transaction.
EOF
    snippet NEW <<'EOF'
        guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
            return false
        }

        // Retire live state only if the exact source lifecycle and producer
        // revision still own the window after the durable transaction.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M08)
    snippet OLD <<'EOF'
        if revertedManagedAny {
            recordThresholdControlSignal(
                .falsePositive,
                podcastId: sourceShowId
            )
        }
EOF
    snippet NEW <<'EOF'
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: sourceShowId
        )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M09)
    snippet OLD <<'EOF'
    ) throws -> CorrectionEvent {
        guard startTime.isFinite, endTime.isFinite else {
EOF
    snippet NEW <<'EOF'
    ) throws -> CorrectionEvent {
        guard correctionStore != nil else {
            throw SkipOrchestratorFeedbackError.staleDurableMaterial
        }
        guard startTime.isFinite, endTime.isFinite else {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M10)
    snippet OLD <<'EOF'
            let correction = makeSuggestDenialCorrection(
EOF
    snippet NEW <<'EOF'
            guard correctionStore != nil else {
                throw SkipOrchestratorFeedbackError.staleDurableMaterial
            }
            let correction = makeSuggestDenialCorrection(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M11)
    snippet OLD <<'EOF'
        if let sourceShowId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: sourceShowId)
        }
EOF
    snippet NEW <<'EOF'
        guard trustService != nil else { return false }

        if let sourceShowId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: sourceShowId)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M12)
    snippet OLD <<'EOF'
            evaluateAndPush()
        }

        if let sourceShowId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: sourceShowId)
        }
EOF
    snippet NEW <<'EOF'
        }
        evaluateAndPush()

        if let sourceShowId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: sourceShowId)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M13)
    # Both calibrating revert seams at once: the sample must be attributed to
    # the show CAPTURED at gesture time, never to whoever is live at effect
    # time.  Batched as one mutation because it is one defect with two sites.
    snippet OLD <<'EOF'
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: sourceShowId
        )
        return true
EOF
    snippet NEW <<'EOF'
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: activePodcastId
        )
        return true
EOF
    patch "$file" "$OLD" "$NEW" || return $?
    snippet OLD <<'EOF'
        if revertedManagedAny {
            recordThresholdControlSignal(
                .falsePositive,
                podcastId: sourceShowId
            )
        }
EOF
    snippet NEW <<'EOF'
        if revertedManagedAny {
            recordThresholdControlSignal(
                .falsePositive,
                podcastId: activePodcastId
            )
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # Anchor re-cut for playhead-1mq1.2.1, which added the third argument. The
  # DEFECT is unchanged — the guard still moves the ingest below the lifecycle
  # check — which is the sanctioned reason to rewrite an EDIT.
  M14)
    snippet OLD <<'EOF'
        ingestNegativeFingerprint(
            text: requestedManaged.adWindow.evidenceText,
            podcastId: sourceShowId,
            negativeAttribution: sourceNegativeAttribution
        )
EOF
    snippet NEW <<'EOF'
        if episodeLifecycleGeneration == sourceLifecycleGeneration {
            ingestNegativeFingerprint(
                text: requestedManaged.adWindow.evidenceText,
                podcastId: sourceShowId,
                negativeAttribution: sourceNegativeAttribution
            )
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M15)
    snippet OLD <<'EOF'
        if let sourceShowId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: sourceShowId)
        }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M16)
    snippet OLD <<'EOF'
        if let sourceShowId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: sourceShowId)
        }
EOF
    snippet NEW <<'EOF'
        if let sourceShowId, let trustService {
            await trustService.recordWeakFalseSkipSignal(podcastId: sourceShowId)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M17)
    # Same defect as M13 for the durable receipt rather than the controller
    # sample; two sites, one mutation.
    snippet OLD <<'EOF'
                    podcastId: sourceShowId,
                    source: .listenRevert,
EOF
    snippet NEW <<'EOF'
                    podcastId: activePodcastId,
                    source: .listenRevert,
EOF
    patch "$file" "$OLD" "$NEW" || return $?
    snippet OLD <<'EOF'
                    podcastId: sourceShowId,
                    source: .manualVeto,
EOF
    snippet NEW <<'EOF'
                    podcastId: activePodcastId,
                    source: .manualVeto,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M18)
    snippet OLD <<'EOF'
        guard !expectedEpisodeId.isEmpty,
              let asset = try fetchAsset(id: analysisAssetId)
        else {
            return false
        }
        return asset.episodeId == expectedEpisodeId
EOF
    snippet NEW <<'EOF'
        return true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M19)
    snippet OLD <<'EOF'
        guard isExplicitDenial else {
            // Neutral x / auto-fade — no explicit feedback.
            logger.debug("Suggest banner exited without feedback")
            return true
        }
EOF
    snippet NEW <<'EOF'
        if !isExplicitDenial {
            // Neutral x / auto-fade — no explicit feedback.
            logger.debug("Suggest banner exited without feedback")
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M20)
    snippet OLD <<'EOF'
            scheduleConfirmedRecurrenceLearning(
                for: managed.adWindow,
                showId: sourcePodcastId,
                source: .confirmedAutoSkipBanner,
                lifecycle: .explicitConfirmation
            )
            return true
EOF
    snippet NEW <<'EOF'
            scheduleConfirmedRecurrenceLearning(
                for: managed.adWindow,
                showId: sourcePodcastId,
                source: .confirmedAutoSkipBanner,
                lifecycle: .explicitConfirmation
            )
            recordThresholdControlSignal(
                .falsePositive,
                podcastId: activePodcastId
            )
            return true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  N02)
    snippet OLD <<'EOF'
        } catch {
            logger.warning("Banner feedback persistence failed")
            return false
        }

        recordThresholdControlSignal(
EOF
    snippet NEW <<'EOF'
        } catch {
            logger.warning("Banner feedback persistence failed")
            return false
        }

        guard activeEpisodeId == sourceEpisodeId,
              episodeLifecycleGeneration == sourceLifecycleGeneration
        else {
            return true
        }
        recordThresholdControlSignal(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  N03)
    snippet OLD <<'EOF'
        recordThresholdControlMiss(podcastId: sourcePodcastId)

        guard sourceLifecycleIsCurrent else {
            return true
        }
EOF
    snippet NEW <<'EOF'
        guard sourceLifecycleIsCurrent else {
            return true
        }
        recordThresholdControlMiss(podcastId: sourcePodcastId)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # EDIT re-cut playhead-auz3 for `ingestNegativeFingerprint`'s
  # `negativeAttribution` argument (playhead-1mq1.2.1); the defect is unchanged,
  # which is the sanctioned reason to rewrite an EDIT. The CLEAN partition from
  # `revertNegativeAttribution(for:)` is what makes the ingest reach the bank —
  # a MIXED one would be refused by P01's guard and the mutation would be inert.
  #
  # Worth remembering: this rot was INVISIBLE to `--dry-run`. The anchor still
  # matched exactly once, so 1mq1.2.1's dry-run swept clean while the mutated
  # tree no longer COMPILED, and the first real run took the whole batch — N02,
  # N03 and N06 as well — down with it. Dry-run proves an anchor applies; only a
  # build proves the result is still a valid mutant.
  N04)
    snippet OLD <<'EOF'
            scheduleConfirmedRecurrenceLearning(
                for: managed.adWindow,
                showId: sourcePodcastId,
                source: .confirmedAutoSkipBanner,
                lifecycle: .explicitConfirmation
            )
            return true
EOF
    snippet NEW <<'EOF'
            scheduleConfirmedRecurrenceLearning(
                for: managed.adWindow,
                showId: sourcePodcastId,
                source: .confirmedAutoSkipBanner,
                lifecycle: .explicitConfirmation
            )
            ingestNegativeFingerprint(
                text: managed.adWindow.evidenceText,
                podcastId: activePodcastId,
                negativeAttribution: revertNegativeAttribution(
                    for: managed.adWindow
                )
            )
            return true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  N05)
    snippet OLD <<'EOF'
        recordThresholdControlMiss(podcastId: sourcePodcastId)
EOF
    snippet NEW <<'EOF'
        recordThresholdControlMiss(podcastId: activePodcastId)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  N06)
    # BOTH sites, because the unattributed-show contract is enforced twice —
    # `recordThresholdControlSignal` refuses to call, and
    # `PerShowThresholdControllerStore.record` refuses to write. Deleting
    # either one alone is an EQUIVALENT MUTANT that no test can kill (verified:
    # each half survives on its own). What a test CAN rail is the contract, so
    # that is what this mutation removes. `$file` is the orchestrator; the
    # store is patched by absolute key below.
    #
    # playhead-o4qr — THE EDIT WAS REWRITTEN, THE EXPECTATION WAS NOT. The
    # previous edit weakened the guard from `let podcastId, !podcastId.isEmpty`
    # to `let podcastId`, i.e. it removed only the EMPTY clause. That reproduced
    # a real defect while `revertWindow` passed the caller's raw show id
    # through; it no longer does. `exactFeedbackShowIdentity` canonicalizes an
    # empty request to nil upstream, so `""` can no longer reach this method at
    # all and the old edit became an equivalent mutant — it SURVIVED batch 8 on
    # 2026-07-27, correctly.
    #
    # The defect the entry has always DESCRIBED — "every unattributed
    # correction would pile into that one shared bucket" — is still live, and
    # this is the shape it now takes: fold the nil case into `""` and let the
    # store accept it. Same contract, same expectation, reachable edit.
    #
    # The `perShowThresholdControllerStore` line is here for UNIQUENESS: the
    # nil/empty refusal is written twice in this file now, because
    # `ingestNegativeFingerprint` grew a deliberately identical guard when the
    # hard-negative bank joined the per-show learning surfaces.
    snippet OLD <<'EOF'
        guard let store = perShowThresholdControllerStore else { return }
        guard let podcastId, !podcastId.isEmpty else { return }
EOF
    snippet NEW <<'EOF'
        guard let store = perShowThresholdControllerStore else { return }
        let podcastId = podcastId ?? ""
EOF
    patch "$file" "$OLD" "$NEW" || return $?
    snippet OLD <<'EOF'
        guard !podcastId.isEmpty else {
            throw PerShowThresholdControllerStoreError.writeFailed("empty podcastId")
        }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$CTRL" "$OLD" "$NEW" ;;

  N07)
    snippet OLD <<'EOF'
        // cannot silently discard valid old-episode feedback.
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: sourceShowId
        )
EOF
    snippet NEW <<'EOF'
        // cannot silently discard valid old-episode feedback.
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: activePodcastId
        )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  S01)
    snippet OLD <<'EOF'
        guard let suggested = suggestWindows.removeValue(forKey: windowId) else {
            return false
        }
        provisionallyResolvingSuggestWindowIds.insert(windowId)
EOF
    snippet NEW <<'EOF'
        guard let suggested = suggestWindows.removeValue(forKey: windowId)
                ?? lastSuggestRevisionByWindowId[windowId] else {
            return false
        }
        provisionallyResolvingSuggestWindowIds.insert(windowId)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  S02)
    snippet OLD <<'EOF'
        windowId: String,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64? = nil,
        ifSuggestionRevisionToken expectedRevisionToken: String? = nil
    ) async -> Bool {
        guard let expectedEpisodeId,
              activeEpisodeId == expectedEpisodeId,
EOF
    snippet NEW <<'EOF'
        windowId: String,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64? = nil,
        ifSuggestionRevisionToken expectedRevisionToken: String? = nil
    ) async -> Bool {
        guard let expectedEpisodeId,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  S03)
    snippet OLD <<'EOF'
        isExplicitDenial: Bool = false,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64? = nil,
        ifSuggestionRevisionToken expectedRevisionToken: String? = nil
    ) async -> Bool {
        guard let expectedEpisodeId,
              activeEpisodeId == expectedEpisodeId,
EOF
    snippet NEW <<'EOF'
        isExplicitDenial: Bool = false,
        ifCurrentEpisodeId expectedEpisodeId: String?,
        ifPlaybackLifecycleGeneration expectedPlaybackGeneration: UInt64? = nil,
        ifSuggestionRevisionToken expectedRevisionToken: String? = nil
    ) async -> Bool {
        guard let expectedEpisodeId,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  S04)
    snippet OLD <<'EOF'
            if retireSuggestedWindowIfPresent(windowId: result.id) {
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) gate flipped from markOnly — cleared suggest entry"
                )
            }
EOF
    snippet NEW <<'EOF'
            // mutation S04: symmetric suggest clear deleted.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  S05)
    snippet OLD <<'EOF'
        guard let suggested = suggestWindows.removeValue(forKey: windowId) else {
            return false
        }

        guard isExplicitDenial else {
EOF
    snippet NEW <<'EOF'
        guard let suggested = suggestWindows[windowId] else {
            return false
        }

        guard isExplicitDenial else {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-auz3. The AdWindow-path twin of S04. Same defect, other ingress:
  # a window first stamped `markOnly` re-arrives with the gate cleared, and
  # without this clear `suggestWindows[id]` survives alongside the new managed
  # entry, so a still-visible suggest banner can re-fire `acceptSuggestedSkip`
  # and mint a SECOND, UUID-keyed durable window for one span.
  S06)
    snippet OLD <<'EOF'
            if retireSuggestedWindowIfPresent(windowId: adWindow.id) {
                logger.debug(
                    "AdWindow \(adWindow.id, privacy: .public) gate flipped from markOnly — cleared suggest entry"
                )
            }
EOF
    snippet NEW <<'EOF'
            // mutation S06: symmetric suggest clear deleted.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-auz3. `recentlyAcceptedSuggestIds` is the tap-then-flip guard: an
  # id the user already promoted is terminal for the rest of that playback
  # lifecycle (`hasTerminalSuggestResolution`). A direct episode replacement —
  # `beginEpisode` with no `endEpisode` between — must reset it, or a producer
  # that reuses a stable window id in the NEXT episode is silently suppressed.
  # The trailing edge-padding comment is load-bearing for UNIQUENESS, not for
  # the defect: `endEpisode` clears the same four sets in the same order, so a
  # shorter anchor matches twice and the patcher (correctly) refuses.
  S07)
    snippet OLD <<'EOF'
        vetoedSuggestWindowIds.removeAll()
        recentlyAcceptedSuggestIds.removeAll()
        provisionallyResolvingSuggestWindowIds.removeAll()
        bufferedSuggestProducerUpdates.removeAll()
        // playhead-98co: per-episode edge-padding state.
EOF
    snippet NEW <<'EOF'
        vetoedSuggestWindowIds.removeAll()
        // mutation S07: beginEpisode no longer clears the accepted-suggest guard.
        provisionallyResolvingSuggestWindowIds.removeAll()
        bufferedSuggestProducerUpdates.removeAll()
        // playhead-98co: per-episode edge-padding state.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-o4qr. REFUSE THE LEARNING, surface 1 of 3 (the controller is
  # N07's). The bank is per-show, and a nil/empty show writes a NULL-show row
  # that `loadEntries(forShow:includeGlobal:)` hands back to EVERY show — so
  # deleting this one guard turns a single unattributable correction into a
  # library-wide suppression. Only `recordListenRevert` reaches the bank, which
  # is why the victim test drives that seam as well as `revertWindow`.
  O01)
    snippet OLD <<'EOF'
        guard let podcastId, !podcastId.isEmpty else { return }
        guard let text, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
EOF
    snippet NEW <<'EOF'
        guard let text, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-o4qr. REFUSE THE LEARNING, surface 2 of 3: the per-show trust
  # penalty. The fallback shape is the tempting one — "we know what show is
  # playing, use it" — and it is exactly the global/null-show contamination the
  # bead exists to prevent: the live show gets penalised on the strength of a
  # gesture that never named it. Anchored below N07's `recordThresholdControl`
  # site, so this must NOT share a batch with N07.
  O02)
    snippet OLD <<'EOF'
        // cannot silently discard valid old-episode feedback.
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: sourceShowId
        )
        if let sourceShowId {
EOF
    snippet NEW <<'EOF'
        // cannot silently discard valid old-episode feedback.
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: sourceShowId
        )
        if let sourceShowId = sourceShowId ?? activePodcastId {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-o4qr. ACCEPT THE RECEIPT — the other half of the decision, and the
  # one a reader is most likely to "restore" on the grounds that refusing looks
  # safer. It is not: it silently discards what the listener said. This is
  # byte-for-byte the pre-decision o4qr shape.
  O03)
    snippet OLD <<'EOF'
        let validatedShow = exactFeedbackShowIdentity(requested: podcastId)
        guard activeEpisodeId == expectedEpisodeId,
              expectedPlaybackGeneration == nil
                || activePlaybackLifecycleGeneration
                    == expectedPlaybackGeneration
        else {
            return false
        }
        let sourceEpisodeId = activeEpisodeId
EOF
    snippet NEW <<'EOF'
        let validatedShow = exactFeedbackShowIdentity(requested: podcastId)
        guard activeEpisodeId == expectedEpisodeId,
              expectedPlaybackGeneration == nil
                || activePlaybackLifecycleGeneration
                    == expectedPlaybackGeneration,
              validatedShow.isValid
        else {
            return false
        }
        let sourceEpisodeId = activeEpisodeId
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-o4qr. REFUSE THE LEARNING, surface 3 of 3: show-scoped recurrence
  # revocation. `revokeRecurrenceEvidence` is deliberately still CALLED for an
  # anonymous correction — its in-memory retraction is what stops a delayed
  # learner reopening the vetoed span — and what makes that safe is that a nil
  # show leaves both stores on their show-free exact-source branches. Handing
  # it `activePodcastId` re-arms the show-scoped branch and lets an
  # unattributable gesture retract this show's creative evidence.
  # Anchor re-cut for playhead-1mq1.2.1's extra argument; same defect.
  O04)
    snippet OLD <<'EOF'
                for: requestedManaged.adWindow,
                showId: sourceShowId,
                source: .manualVeto,
                negativeAttribution: sourceNegativeAttribution
EOF
    snippet NEW <<'EOF'
                for: requestedManaged.adWindow,
                showId: activePodcastId,
                source: .manualVeto,
                negativeAttribution: sourceNegativeAttribution
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-1mq1.2.1, surface 1 of 2: the hard-negative copy bank. The window's
  # `evidenceText` has no time index, so on a MIXED revert it is the REAL ad's
  # copy — banking it as a confirmed false positive suppresses that ad on this
  # show forever. Deleting the guard is the pre-bead behaviour verbatim.
  P01)
    snippet OLD <<'EOF'
        guard negativeAttribution.allowsWholeSpanNegativeLabel else { return }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-1mq1.2.1, surface 2 of 2: the FUZZY recurrence sweep. Restoring the
  # whole-span fingerprint re-arms `compatibleMatches` /
  # `RepeatedAdCacheService.revokeMatches` against a fingerprint taken over
  # audio that is mostly the ad, so the revert deletes the legitimately learned
  # row for the very ad the listener did not dispute.
  P02)
    snippet OLD <<'EOF'
            let featureWindows = allFeatureWindows.filter { feature in
                negativeAttribution.allowsNegativeAttribution(
                    startTime: feature.startTime,
                    endTime: feature.endTime
                )
            }
EOF
    snippet NEW <<'EOF'
            let featureWindows = allFeatureWindows
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # playhead-o4qr. The deny-side twin of O03: ACCEPT THE RECEIPT at the one
  # correction seam that has a shipped production caller (the banner No).
  # The `requestedManaged` line is load-bearing for UNIQUENESS, not for the
  # defect: `confirmAutoSkippedBanner` opens with a byte-identical guard prefix
  # and binds `managed` instead, so a shorter anchor matches twice and the
  # patcher (correctly) refuses.
  O05)
    snippet OLD <<'EOF'
              let expectedMaterialToken,
              activeEpisodeId == expectedEpisodeId,
              activePlaybackLifecycleGeneration
                == expectedPlaybackGeneration,
              let requestedManaged = windows[windowId],
EOF
    snippet NEW <<'EOF'
              let expectedMaterialToken,
              validatedShow.isValid,
              activeEpisodeId == expectedEpisodeId,
              activePlaybackLifecycleGeneration
                == expectedPlaybackGeneration,
              let requestedManaged = windows[windowId],
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # --- playhead-d3g0 ------------------------------------------------------

  D01)
    snippet OLD <<'EOF'
            suggestWindows[adWindow.id] = adWindow
            armedSuggestWindowIds.insert(adWindow.id)
            return
EOF
    snippet NEW <<'EOF'
            suggestWindows[adWindow.id] = adWindow
            emitSuggestBanner(for: adWindow)
            return
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D02)
    snippet OLD <<'EOF'
            .filter { time >= $0.startTime && time < $0.endTime }
EOF
    snippet NEW <<'EOF'
            .filter { time > $0.startTime && time < $0.endTime }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D03)
    snippet OLD <<'EOF'
            .filter { time >= $0.startTime && time < $0.endTime }
EOF
    snippet NEW <<'EOF'
            .filter { time >= $0.startTime }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D04)
    snippet OLD <<'EOF'
        for window in entered {
            armedSuggestWindowIds.remove(window.id)
            emitSuggestBanner(for: window)
        }
EOF
    snippet NEW <<'EOF'
        for window in entered {
            emitSuggestBanner(for: window)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D05)
    snippet OLD <<'EOF'
        let pending = suggestWindows.values
            .filter {
                guard !armedSuggestWindowIds.contains($0.id) else {
                    return false
                }
                guard let revisionToken =
                        suggestRevisionTokensByWindowId[$0.id]
                else {
                    return true
                }
                return !acknowledgedSuggestRevisionTokens
                    .contains(revisionToken)
                    && !banneredWindowIds.contains($0.id)
            }
            .sorted {
                if $0.startTime != $1.startTime {
                    return $0.startTime < $1.startTime
                }
                return $0.id < $1.id
            }

        for window in pending {
            switch continuation.yield(makeSuggestBannerItem(for: window)) {
EOF
    snippet NEW <<'EOF'
        let pending = suggestWindows.values
            .filter {
                guard let revisionToken =
                        suggestRevisionTokensByWindowId[$0.id]
                else {
                    return true
                }
                return !acknowledgedSuggestRevisionTokens
                    .contains(revisionToken)
                    && !banneredWindowIds.contains($0.id)
            }
            .sorted {
                if $0.startTime != $1.startTime {
                    return $0.startTime < $1.startTime
                }
                return $0.id < $1.id
            }

        for window in pending {
            switch continuation.yield(makeSuggestBannerItem(for: window)) {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D06)
    snippet OLD <<'EOF'
            .filter { time >= $0.startTime && time < $0.endTime }
EOF
    snippet NEW <<'EOF'
            .filter { time >= $0.startTime + 1.0 && time < $0.endTime }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D07)
    snippet OLD <<'EOF'
    private func confirmationWouldSkip(_ window: AdWindow) -> Bool {
        let anchors = resolvedEdgeAnchors(for: window)
        return AutoSkipEdgePadding.skipWindow(
EOF
    snippet NEW <<'EOF'
    private func confirmationWouldSkip(_ window: AdWindow) -> Bool {
        if true { return true }
        let anchors = resolvedEdgeAnchors(for: window)
        return AutoSkipEdgePadding.skipWindow(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D08)
    snippet OLD <<'EOF'
            confirmationSkipsPlayback: confirmationWouldSkip(adWindow)
EOF
    snippet NEW <<'EOF'
            confirmationSkipsPlayback: false
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D09)
    snippet OLD <<'EOF'
                    confirmLabel: markOnlyConfirmFeedbackLabel,
                    denyLabel: denyFeedbackLabel,
                    confirmAccessibilityLabel: "Yes, this is a sponsor break",
                    confirmAccessibilityHint:
                        "Marks this as an ad; playback continues",
EOF
    snippet NEW <<'EOF'
                    confirmLabel: confirmFeedbackLabel,
                    denyLabel: denyFeedbackLabel,
                    confirmAccessibilityLabel: "Yes, skip this sponsor break",
                    confirmAccessibilityHint:
                        "Confirms this is an ad and skips it",
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D10)
    snippet OLD <<'EOF'
    static let suggestEntryLatencyBudgetSeconds: TimeInterval = 0.5
EOF
    snippet NEW <<'EOF'
    static let suggestEntryLatencyBudgetSeconds: TimeInterval = 3.0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D11)
    snippet OLD <<'EOF'
        let pending = suggestWindows.values
            .filter {
                guard !armedSuggestWindowIds.contains($0.id) else {
                    return false
                }
                guard let revisionToken =
                        suggestRevisionTokensByWindowId[$0.id]
                else {
                    return true
                }
                return !acknowledgedSuggestRevisionTokens
                    .contains(revisionToken)
                    && !banneredWindowIds.contains($0.id)
            }
            .sorted {
                if $0.startTime != $1.startTime {
                    return $0.startTime < $1.startTime
                }
                return $0.id < $1.id
            }

        for window in pending {
            continuation.yield(.present(makeSuggestBannerItem(for: window)))
        }
EOF
    snippet NEW <<'EOF'
        let pending = suggestWindows.values
            .filter {
                guard let revisionToken =
                        suggestRevisionTokensByWindowId[$0.id]
                else {
                    return true
                }
                return !acknowledgedSuggestRevisionTokens
                    .contains(revisionToken)
                    && !banneredWindowIds.contains($0.id)
            }
            .sorted {
                if $0.startTime != $1.startTime {
                    return $0.startTime < $1.startTime
                }
                return $0.id < $1.id
            }

        for window in pending {
            continuation.yield(.present(makeSuggestBannerItem(for: window)))
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D12)
    snippet OLD <<'EOF'
            .filter { time >= $0.startTime && time < $0.endTime }
            .sorted {
                if $0.startTime != $1.startTime {
                    return $0.startTime < $1.startTime
                }
                return $0.id < $1.id
            }
        for window in entered {
EOF
    snippet NEW <<'EOF'
            .filter { time >= $0.startTime && time < $0.endTime }
            .sorted { $0.startTime > $1.startTime }
        for window in entered {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D13)
    snippet OLD <<'EOF'
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
        armedSuggestWindowIds.removeAll()
        suggestRevisionTokensByWindowId.removeAll()
        lastSuggestRevisionByWindowId.removeAll()
        acknowledgedSuggestRevisionTokens.removeAll()
        vetoedSuggestWindowIds.removeAll()
        recentlyAcceptedSuggestIds.removeAll()
        provisionallyResolvingSuggestWindowIds.removeAll()
        bufferedSuggestProducerUpdates.removeAll()
        // playhead-98co: clear per-episode edge-padding state here as well
EOF
    snippet NEW <<'EOF'
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
        suggestRevisionTokensByWindowId.removeAll()
        lastSuggestRevisionByWindowId.removeAll()
        acknowledgedSuggestRevisionTokens.removeAll()
        vetoedSuggestWindowIds.removeAll()
        recentlyAcceptedSuggestIds.removeAll()
        provisionallyResolvingSuggestWindowIds.removeAll()
        bufferedSuggestProducerUpdates.removeAll()
        // playhead-98co: clear per-episode edge-padding state here as well
EOF
    patch "$file" "$OLD" "$NEW" ;;

  D14)
    snippet OLD <<'EOF'
        let isNewActiveSuggestion = suggestWindows[adWindow.id] == nil
        suggestWindows[adWindow.id] = adWindow
        if isNewActiveSuggestion {
            armedSuggestWindowIds.insert(adWindow.id)
        }
EOF
    snippet NEW <<'EOF'
        suggestWindows[adWindow.id] = adWindow
EOF
    patch "$file" "$OLD" "$NEW" ;;

  E01)
    snippet OLD <<'EOF'
    func ingestPersistedAdWindows(analysisAssetId: String) async -> Int {
        guard activeAssetId == analysisAssetId else {
            logger.debug(
                "ingestPersistedAdWindows: dropping mismatched asset \(analysisAssetId, privacy: .public) (active=\(self.activeAssetId ?? "nil", privacy: .public))"
            )
            return 0
        }
        return await forwardPersistedAdWindows(
EOF
    snippet NEW <<'EOF'
    func ingestPersistedAdWindows(analysisAssetId: String) async -> Int {
        return await forwardPersistedAdWindows(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  E02)
    snippet OLD <<'EOF'
                || $0.userAssertion != nil)
                && $0.endTime > $0.startTime
                && preloadEligibleDecisionStates.contains($0.decisionState)
EOF
    snippet NEW <<'EOF'
                || $0.userAssertion != nil)
                && $0.endTime > $0.startTime
EOF
    patch "$file" "$OLD" "$NEW" ;;

  E04)
    snippet OLD <<'EOF'
        return await forwardPersistedAdWindows(
            analysisAssetId: analysisAssetId,
            lifecycleGeneration: episodeLifecycleGeneration
        )
    }
EOF
    snippet NEW <<'EOF'
        let forwarded = await forwardPersistedAdWindows(
            analysisAssetId: analysisAssetId,
            lifecycleGeneration: episodeLifecycleGeneration
        )
        emitSuggestBannersOnPlayheadEntry(at: currentPlayheadTime)
        return forwarded
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  E05)
    snippet OLD <<'EOF'
        if summary.dayZeroMarkCount > 0 {
            await mintedMarkDelivery(analysisAssetId)
        }
EOF
    snippet NEW <<'EOF'
        await mintedMarkDelivery(analysisAssetId)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  E06)
    snippet OLD <<'EOF'
        if summary.dayZeroMarkCount > 0 {
            await mintedMarkDelivery(analysisAssetId)
        }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  E07)
    snippet OLD <<'EOF'
        if summary.dayZeroMarkCount > 0 {
            await mintedMarkDelivery(analysisAssetId)
        }
EOF
    snippet NEW <<'EOF'
        if summary.dayZeroMarkCount > 0 {
            await mintedMarkDelivery(analysisAssetId)
            await mintedMarkDelivery(analysisAssetId)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  E08)
    snippet OLD <<'EOF'
        summary.dayZeroMarkCount += result.dayZeroMarkCount
EOF
    snippet NEW <<'EOF'
        summary.dayZeroMarkCount += result.rotated ? 1 : 0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  E09)
    snippet OLD <<'EOF'
                return CandidateResult(
                    cost: cost, rotated: true, failed: false,
                    dayZeroMarkCount: mint.markCount
                )
EOF
    snippet NEW <<'EOF'
                return CandidateResult(
                    cost: cost, rotated: true, failed: false,
                    dayZeroMarkCount: 0
                )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-djl0 ------------------------------------------------------

  # J01: the pre-djl0 shape. One silent `.shadow` for every reason.
  J01)
    snippet OLD <<'EOF'
        } else {
            activeSkipMode = .shadow
            noteSkipModeResolution(.unresolvedShowIdentity, episodeId: episodeId)
        }
EOF
    snippet NEW <<'EOF'
        } else {
            activeSkipMode = .shadow
            noteSkipModeResolution(.showTrustProfile, episodeId: episodeId)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J02: a brand-new show counted as a failure. This is the mistake that makes
  # the failure numbers read as "we lose the show on every first listen".
  J02)
    snippet OLD <<'EOF'
        case .noActiveEpisode, .showTrustProfile, .newShowDefault, .sessionOverride:
            return false
EOF
    snippet NEW <<'EOF'
        case .noActiveEpisode, .showTrustProfile, .sessionOverride:
            return false
        case .newShowDefault:
            return true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J03: an unresolved identity claiming to have a show. Widens the predicate
  # that decides both the diagnostics code and whether the pill offers a menu
  # whose selection cannot be stored.
  J03)
    snippet OLD <<'EOF'
        case .noActiveEpisode, .unresolvedShowIdentity:
            return false
        case .showTrustProfile, .newShowDefault, .sessionOverride,
EOF
    snippet NEW <<'EOF'
        case .noActiveEpisode:
            return false
        case .unresolvedShowIdentity,
             .showTrustProfile, .newShowDefault, .sessionOverride,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J04: an undecodable stored mode reported as the profile's verdict.
  J04)
    snippet OLD <<'EOF'
            return (.shadow, .unrecognizedTrustProfileMode)
EOF
    snippet NEW <<'EOF'
            return (.shadow, .showTrustProfile)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J05: named but not counted.
  J05)
    snippet OLD <<'EOF'
        skipModeResolutionFailureCounts[resolution, default: 0] += 1
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J06: counted but not recorded — no durable trace, which is exactly the
  # state the 2026-08-01 field investigation found.
  J06)
    snippet OLD <<'EOF'
        invariantLogger.invariantViolated(
            code: code,
            description: """
                skip mode fell back to \(activeSkipMode.rawValue) \
                because \(resolution.rawValue) (episode \(episodeIdHash))
                """
        )
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J07: one bucket for both failure classes. The audit could count them but
  # not tell "we lost the show" from "we could not read its profile".
  J07)
    snippet OLD <<'EOF'
        let code: InvariantViolation.Code = resolution.hasResolvedShowIdentity
            ? .skipModeTrustLookupFailed
            : .skipModeShowIdentityUnresolved
EOF
    snippet NEW <<'EOF'
        let code: InvariantViolation.Code = .skipModeShowIdentityUnresolved
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J08: the listener answers the question and the pill keeps saying their
  # show was not recognised.
  J08)
    snippet OLD <<'EOF'
        activeSkipModeResolution = .sessionOverride
        evaluateAndPush()
EOF
    snippet NEW <<'EOF'
        evaluateAndPush()
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J09: `endEpisode` leaves the finished episode's cause describing nothing.
  J09)
    snippet OLD <<'EOF'
        // process-lifetime tally, not per-episode state.
        activeSkipModeResolution = .noActiveEpisode
EOF
    snippet NEW <<'EOF'
        // process-lifetime tally, not per-episode state.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J10: recovery admits a non-canonical stored identity, retargeting
  # show-scoped recurrence evidence into a neighbouring namespace.
  J10)
    snippet OLD <<'EOF'
            return normalizedCatalogShowId(recorded)
EOF
    snippet NEW <<'EOF'
            return recorded
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J11: the lagging mirror overrides the live relationship.
  J11)
    snippet OLD <<'EOF'
        var resolvedShowId = normalizedPodcastId
        if resolvedShowId == nil {
EOF
    snippet NEW <<'EOF'
        var resolvedShowId = normalizedPodcastId
        if true {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J12: drop the NOT NULL filter, so an episode whose NEWEST enqueue was
  # context-free hides an older row that knows the show.
  J12)
    snippet OLD <<'EOF'
            WHERE episodeId = ? AND podcastId IS NOT NULL
EOF
    snippet NEW <<'EOF'
            WHERE episodeId = ?
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J13: half one never wired — the caller's nullable hop is the only source.
  J13)
    snippet OLD <<'EOF'
            resolvedShowId = await recoverShowIdentity(episodeId: episodeId)
EOF
    snippet NEW <<'EOF'
            resolvedShowId = nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J14: the pill reads the mode alone again, so a lost show impersonates a
  # show that is deliberately in shadow.
  J14)
    snippet OLD <<'EOF'
        if resolution.hasResolvedShowIdentity {
            label = Self.modeLabel(mode)
            accessibilityLabel = "Skip mode: \(Self.modeLabel(mode))"
            isModeSelectable = true
        } else {
EOF
    snippet NEW <<'EOF'
        if true {
            label = Self.modeLabel(mode)
            accessibilityLabel = "Skip mode: \(Self.modeLabel(mode))"
            isModeSelectable = true
        } else {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J15: back to "render only when there is a podcast title" — and the title
  # is missing for exactly the same reason the identity is.
  J15)
    snippet OLD <<'EOF'
        if resolution == .unresolvedShowIdentity { return true }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J16: every layer correct, and the cause dropped on the last hop.
  J16)
    snippet OLD <<'EOF'
        skipModeResolution = await orchestrator.currentSkipModeResolution()
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J18: the pill's own copy. A resolved show's three labels are the pre-djl0
  # strings and must stay byte-identical — this bead adds a state, it does not
  # rename the ones that were already right.
  J18)
    snippet OLD <<'EOF'
        case .manual: "Manual"
EOF
    snippet NEW <<'EOF'
        case .manual: "Shadow"
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # J17: the listener chooses a mode and the pill keeps reporting the lookup
  # failure that preceded it.
  J17)
    snippet OLD <<'EOF'
        activeSkipMode = mode
        skipModeResolution = .sessionOverride
EOF
    snippet NEW <<'EOF'
        activeSkipMode = mode
EOF
    patch "$file" "$OLD" "$NEW" ;;


  # ------------------------------------------------------------------
  # playhead-4dqe — day-0 rediff at DOWNLOAD time
  # ------------------------------------------------------------------

  K01)
    snippet OLD <<'EOF'
        guard !transport.isLowDataMode else { return .denyLowDataMode }
EOF
    snippet NEW <<'EOF'
        guard !(transport.isLowDataMode && transport.reachability == .wifi) else {
            return .denyLowDataMode
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K02)
    snippet OLD <<'EOF'
        if transport.reachability == .cellular, !transport.allowsCellular {
EOF
    snippet NEW <<'EOF'
        if transport.reachability == .cellular {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K03)
    snippet OLD <<'EOF'
    static let dayZeroAllowsCellularByDefault = false
EOF
    snippet NEW <<'EOF'
    static let dayZeroAllowsCellularByDefault = true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K04)
    snippet OLD <<'EOF'
        return now >= startedAt + windowSeconds
EOF
    snippet NEW <<'EOF'
        _ = startedAt
        return false
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K05)
    snippet OLD <<'EOF'
        estimatedCost <= remainingBytes(window, now: now)
EOF
    snippet NEW <<'EOF'
        _ = estimatedCost
        return estimatedBytesPerBCopy <= remainingBytes(window, now: now)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K06)
    snippet OLD <<'EOF'
        guard bytes > 0 else { return window }
EOF
    snippet NEW <<'EOF'
        guard bytes >= 0 else { return window }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K07)
    snippet OLD <<'EOF'
            if reached.readinessProgressRank > furthest.readinessProgressRank {
                furthest = reached
            }
EOF
    snippet NEW <<'EOF'
            furthest = reached
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K08)
    snippet OLD <<'EOF'
                    ready: nil, outcome: .cancelled, pollCount: polls
EOF
    snippet NEW <<'EOF'
                    ready: nil, outcome: .noPinnedFile, pollCount: polls
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K09)
    snippet OLD <<'EOF'
        case .noPinnedFile: return .rediffDayZeroKickoffNoPinnedFile
EOF
    snippet NEW <<'EOF'
        case .noPinnedFile: return .rediffDayZeroKickoffNoAnalysisAsset
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K10)
    snippet OLD <<'EOF'
        case (nil, .some):
            return false
EOF
    snippet NEW <<'EOF'
        case (nil, .some):
            return true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K11)
    snippet OLD <<'EOF'
        if lhs.enqueuedAt != rhs.enqueuedAt { return lhs.enqueuedAt < rhs.enqueuedAt }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K12)
    snippet OLD <<'EOF'
        pending = RediffDayZeroKickoffOrdering.drainOrder(pending)
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K13)
    snippet OLD <<'EOF'
            giveUps[outcome.outcome, default: 0] += 1
EOF
    snippet NEW <<'EOF'
            giveUps[.fired, default: 0] += 1
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K14)
    snippet OLD <<'EOF'
        guard !inFlight.contains(request.episodeId) else { return }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K15)
    snippet OLD <<'EOF'
            if let exit = transportDecision.deniedExit {
                await suppressionRecorder(analysisAssetId, exit, now)
            }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K16)
    snippet OLD <<'EOF'
        guard RediffDayZeroDailyBudget.allows(window, estimatedCost: estimate, now: now) else {
EOF
    snippet NEW <<'EOF'
        guard RediffDayZeroDailyBudget.allows(window, estimatedCost: 0, now: now) else {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K17)
    snippet OLD <<'EOF'
            await budgetSpendRecorder(summary.fullFetchBytes, now)
EOF
    snippet NEW <<'EOF'
            await budgetSpendRecorder(estimate, now)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K18)
    snippet OLD <<'EOF'
        let fired = outcome.isGiveUp ? 0 : 1
EOF
    snippet NEW <<'EOF'
        let fired = 1
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K19)
    snippet OLD <<'EOF'
        let window = RediffDayZeroDailyBudget.windowSeconds
EOF
    snippet NEW <<'EOF'
        let window = RediffDayZeroDailyBudget.windowSeconds * 1_000_000
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K20)
    snippet OLD <<'EOF'
        request.allowsCellularAccess = allowsCellular
EOF
    snippet NEW <<'EOF'
        request.allowsCellularAccess = false
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K21)
    snippet OLD <<'EOF'
        config.allowsConstrainedNetworkAccess = false
EOF
    snippet NEW <<'EOF'
        config.allowsConstrainedNetworkAccess = allowsCellular
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K22)
    snippet OLD <<'EOF'
        cellularSession != nil && allowsCellular()
EOF
    snippet NEW <<'EOF'
        cellularSession != nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-eks2: the ad-pod continuation flip -----------------------

  L01)
    snippet OLD <<'EOF'
        podContinuationEnabled: true,  // playhead-eks2: flipped ON 2026-08-01 (Dan) — the corpus A/B the xsdz.65 close gated on measures 0.0 newly-claimed seconds outside a byte-confirmed DAI slot at the shipping arm, and the output is mark-only/candidate/unanchored, so the worst case is a wrong BANNER (playhead-2350 + ynmk both hold, pinned by AdPodContinuationFlipTests)
EOF
    snippet NEW <<'EOF'
        podContinuationEnabled: false,  // playhead-eks2: flipped ON 2026-08-01 (Dan) — the corpus A/B the xsdz.65 close gated on measures 0.0 newly-claimed seconds outside a byte-confirmed DAI slot at the shipping arm, and the output is mark-only/candidate/unanchored, so the worst case is a wrong BANNER (playhead-2350 + ynmk both hold, pinned by AdPodContinuationFlipTests)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  L02)
    snippet OLD <<'EOF'
        podContinuationEnabled: Bool = true,
EOF
    snippet NEW <<'EOF'
        podContinuationEnabled: Bool = false,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  L03)
    snippet OLD <<'EOF'
        if config.podContinuationEnabled {
            do {
                let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
EOF
    snippet NEW <<'EOF'
        if config.podContinuationEnabled && analysisAssetId.isEmpty {
            do {
                let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  L04)
    snippet OLD <<'EOF'
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
EOF
    snippet NEW <<'EOF'
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
            catalogStoreMatchSimilarity: nil,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  L05)
    snippet OLD <<'EOF'
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
EOF
    snippet NEW <<'EOF'
            startEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.rediffByteExact.rawValue
EOF
    patch "$file" "$OLD" "$NEW" ;;

  L06)
    snippet OLD <<'EOF'
    static let seedDecisionStates: Set<String> = [
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue
    ]
EOF
    snippet NEW <<'EOF'
    static let seedDecisionStates: Set<String> = [
        AdDecisionState.confirmed.rawValue,
        AdDecisionState.applied.rawValue,
        AdDecisionState.candidate.rawValue
    ]
EOF
    patch "$file" "$OLD" "$NEW" ;;

  L07)
    snippet OLD <<'EOF'
            .filter { time >= $0.startTime && time < $0.endTime }
EOF
    snippet NEW <<'EOF'
            .filter {
                time >= $0.startTime
                    && time < $0.startTime
                        + PlaybackService.periodicTimeObserverIntervalSeconds
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  L08)
    snippet OLD <<'EOF'
        for window in entered {
            armedSuggestWindowIds.remove(window.id)
            emitSuggestBanner(for: window)
        }
EOF
    snippet NEW <<'EOF'
        for window in entered {
            emitSuggestBanner(for: window)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  L09)
    snippet OLD <<'EOF'
            markConfidenceCeiling: 0.70
        )

        init(
EOF
    snippet NEW <<'EOF'
            markConfidenceCeiling: 0.60
        )

        init(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  V01)
    snippet OLD <<'EOF'
        (seedDecisionStates.contains(window.decisionState)
            || isDayZeroByteExactSeed(window))
EOF
    snippet NEW <<'EOF'
        (seedDecisionStates.contains(window.decisionState))
EOF
    patch "$file" "$OLD" "$NEW" ;;

  V02)
    snippet OLD <<'EOF'
        window.boundaryState == AdDetectionService.dayZeroRediffByteExactBoundaryState
            && window.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue
EOF
    snippet NEW <<'EOF'
        window.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue
EOF
    patch "$file" "$OLD" "$NEW" ;;

  V03)
    snippet OLD <<'EOF'
            && window.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue
            && window.endEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue
EOF
    snippet NEW <<'EOF'
            && !window.boundaryState.isEmpty
EOF
    patch "$file" "$OLD" "$NEW" ;;

  V04)
    snippet OLD <<'EOF'
            && window.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue
            && window.endEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue
EOF
    snippet NEW <<'EOF'
            && (window.startEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue
                || window.endEdgeAnchor == AutoSkipEdgeAnchor.rediffByteExact.rawValue)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  V05)
    snippet OLD <<'EOF'
            && visibleDecisionStates.contains(window.decisionState)
EOF
    snippet NEW <<'EOF'
            && !window.decisionState.isEmpty
EOF
    patch "$file" "$OLD" "$NEW" ;;

  V06)
    snippet OLD <<'EOF'
    static func isDayZeroByteExactSeed(_ window: AdWindow) -> Bool {
        window.boundaryState == AdDetectionService.dayZeroRediffByteExactBoundaryState
EOF
    snippet NEW <<'EOF'
    static func isDayZeroByteExactSeed(_ window: AdWindow) -> Bool {
        window.eligibilityGate == SkipEligibilityGate.eligible.rawValue
            && window.boundaryState == AdDetectionService.dayZeroRediffByteExactBoundaryState
  # --- playhead-kvs8: the FM daemon throttle ---------------------------------

  Q01)
    # The guard can never hold: `CancellationError` is caught by an earlier arm,
    # so no error reaching here is both a throttle and a cancellation. The arm
    # stays syntactically live (nothing is deleted) and every throttle falls
    # through to the generic catch-all that produced the field row.
    snippet OLD <<'EOF'
            } catch let throttle where FMDaemonThrottle.isThrottle(throttle) {
EOF
    snippet NEW <<'EOF'
            } catch let throttle where FMDaemonThrottle.isThrottle(throttle) && throttle is CancellationError {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Q02)
    snippet OLD <<'EOF'
                throttledThisJob = true
                consecutiveThrottledJobs += 1
                do {
EOF
    snippet NEW <<'EOF'
                throttledThisJob = true
                consecutiveThrottledJobs += 1
                try? await store.checkpointBackfillJobProgress(
                    jobId: job.jobId,
                    progressCursor: job.progressCursor,
                    retryCount: job.retryCount + 1
                )
                do {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Q03)
    snippet OLD <<'EOF'
                        reason: FMDaemonThrottle.DeferCause.passPrologue.rawValue
EOF
    snippet NEW <<'EOF'
                        reason: FMDaemonThrottle.DeferCause.window.rawValue
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Q04)
    snippet OLD <<'EOF'
            // `.persistFailure` status that would strand it.
            return .rateLimited
EOF
    snippet NEW <<'EOF'
            // `.persistFailure` status that would strand it.
            return .permissiveRefusal
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Q05)
    snippet OLD <<'EOF'
            // carries it into `failedWindowStatuses` and onto the persisted row.
            break
EOF
    snippet NEW <<'EOF'
            // carries it into `failedWindowStatuses` and onto the persisted row.
            refusal += 1
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Q06)
    snippet OLD <<'EOF'
                        let unexpectedReason = Self.permissiveUnexpectedReason(for: error)
                        failedWindows.append(
EOF
    snippet NEW <<'EOF'
                        let unexpectedReason = PermissiveClassificationError.Reason.permissiveRefusal
                        failedWindows.append(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Q07)
    snippet OLD <<'EOF'
        !FMDaemonThrottle.isThrottle(error)
EOF
    snippet NEW <<'EOF'
        true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Q08)
    snippet OLD <<'EOF'
    static let consecutiveDeferStopThreshold = 2
EOF
    snippet NEW <<'EOF'
    static let consecutiveDeferStopThreshold = 1
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U01)
    snippet OLD <<'EOF'
        // playhead-usn1: the verdict reaches the surface HERE, not on the next
        // pull. This is the emission the Now Playing pill was missing.
        publishSkipMode()
EOF
    snippet NEW <<'EOF'
        // playhead-usn1: the verdict reaches the surface HERE, not on the next
        // pull. This is the emission the Now Playing pill was missing.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U02)
    snippet OLD <<'EOF'
        // playhead-usn1: the cleared pair is published too. A subscriber that
        // attached during the PREVIOUS episode must not keep rendering that
        // show's mode across the suspensions the lookup below is about to take.
        publishSkipMode()
EOF
    snippet NEW <<'EOF'
        // playhead-usn1: the cleared pair is published too. A subscriber that
        // attached during the PREVIOUS episode must not keep rendering that
        // show's mode across the suspensions the lookup below is about to take.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U03)
    snippet OLD <<'EOF'
        // playhead-usn1: publish the cleared pair so a mounted Now Playing
        // screen stops describing a show that is no longer playing.
        publishSkipMode()
EOF
    snippet NEW <<'EOF'
        // playhead-usn1: publish the cleared pair so a mounted Now Playing
        // screen stops describing a show that is no longer playing.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U04)
    snippet OLD <<'EOF'
        // playhead-usn1: an explicit choice is a transition like any other.
        publishSkipMode()
EOF
    snippet NEW <<'EOF'
        // playhead-usn1: an explicit choice is a transition like any other.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U05)
    snippet OLD <<'EOF'
            continuation.yield(self.currentSkipModeSnapshot())
EOF
    snippet NEW <<'EOF'
            _ = self.currentSkipModeSnapshot()
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U06)
    snippet OLD <<'EOF'
        return Self.showIdentity(
            fromCanonicalEpisodeKey: canonicalEpisodeKey,
            feedItemGUID: feedItemGUID
        )
EOF
    snippet NEW <<'EOF'
        return nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U07)
    snippet OLD <<'EOF'
        let suffix = "::\(feedItemGUID)"
        guard key.hasSuffix(suffix), key.count > suffix.count else { return nil }
        let feedURLString = String(key.dropLast(suffix.count))
EOF
    snippet NEW <<'EOF'
        guard let separator = key.range(of: "::") else { return nil }
        let feedURLString = String(key[key.startIndex..<separator.lowerBound])
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U08)
    snippet OLD <<'EOF'
        return RecurrenceMaterialIdentity.canonicalIdentifier(feedURLString)
EOF
    snippet NEW <<'EOF'
        return feedURLString.isEmpty ? nil : feedURLString
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U09)
    snippet OLD <<'EOF'
            return .refusedNoShowIdentity
EOF
    snippet NEW <<'EOF'
            return .persisted(podcastId: "")
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U10)
    snippet OLD <<'EOF'
            refusedShowSkipModeWriteCount += 1
EOF
    snippet NEW <<'EOF'
            _ = refusedShowSkipModeWriteCount
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U11)
    snippet OLD <<'EOF'
            logger.error(
                "setShowSkipMode: REFUSED \(mode.rawValue, privacy: .public) — the session has no show to attach it to; nothing was written"
            )
EOF
    snippet NEW <<'EOF'
            logger.error(
                "setShowSkipMode: REFUSED \(mode.rawValue, privacy: .public) — the session has no show to attach it to; nothing was written"
            )
            await orchestrator.setActiveSkipMode(mode)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U12)
    snippet OLD <<'EOF'
        guard currentPodcastId == nil else { return }
        let recovered = await skipOrchestrator.activeShowIdentity()
EOF
    snippet NEW <<'EOF'
        let recovered = await skipOrchestrator.activeShowIdentity()
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U13)
    snippet OLD <<'EOF'
        guard isCurrentPlayRequest(generation: generation, episodeId: episodeId),
              let recovered else { return }
EOF
    snippet NEW <<'EOF'
        guard let recovered else { return }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U16)
    snippet OLD <<'EOF'
    func adoptRecoveredShowIdentity(
        generation: UInt64,
        episodeId: String
    ) async {
EOF
    snippet NEW <<'EOF'
    func adoptRecoveredShowIdentity(
        generation: UInt64,
        episodeId: String
    ) async {
        if true { return }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U14)
    snippet OLD <<'EOF'
            if outcome == .refusedNoShowIdentity {
                self.activeSkipMode = previousMode
                self.skipModeResolution = previousResolution
            }
EOF
    snippet NEW <<'EOF'
            if outcome == .refusedNoShowIdentity {
                _ = (previousMode, previousResolution)
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U15)
    snippet OLD <<'EOF'
            let stream = await orchestrator.skipModeStream()
            for await snapshot in stream {
EOF
    snippet NEW <<'EOF'
            let stream = await orchestrator.skipModeStream()
            for await snapshot in AsyncStream<SkipModeSnapshot>.makeStream().stream {
                _ = stream
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U18)
    snippet OLD <<'EOF'
        let suffix = "::\(feedItemGUID)"
        guard key.hasSuffix(suffix), key.count > suffix.count else { return nil }
        let feedURLString = String(key.dropLast(suffix.count))
EOF
    snippet NEW <<'EOF'
        guard let separator = key.range(of: "::", options: .backwards) else {
            return nil
        }
        let feedURLString = String(key[key.startIndex..<separator.lowerBound])
EOF
    patch "$file" "$OLD" "$NEW" ;;

  U17)
    snippet OLD <<'EOF'
                code: .skipModeWriteRefusedNoShowIdentity,
EOF
    snippet NEW <<'EOF'
                code: .skipModeShowIdentityUnresolved,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  W01)
    snippet OLD <<'EOF'
            noteIngestDoorOutcome(
                .doorDroppedNotPlaying,
                door: .midSessionIngest,
                analysisAssetId: analysisAssetId,
                detail: "active=\(activeAssetId ?? "nil")"
            )
            return 0
EOF
    snippet NEW <<'EOF'
            return 0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  W02)
    snippet OLD <<'EOF'
                    noteIngestOutcome(
                        .droppedInventorySanity,
                        windowId: adWindow.id,
                        detail: reason.rawValue
                    )
EOF
    snippet NEW <<'EOF'
                    noteIngestOutcome(
                        .droppedInventorySanity,
                        windowId: adWindow.id
                    )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  W03)
    snippet OLD <<'EOF'
        // playhead-isp5: mirrors the `beginEpisode` clear. The per-cause
        // COUNTS survive on purpose (process-lifetime tally).
        lastIngestOutcomeByWindowId.removeAll()
EOF
    snippet NEW <<'EOF'
        lastIngestOutcomeByWindowId.removeAll()
        adWindowIngestOutcomeCounts.removeAll()
EOF
    patch "$file" "$OLD" "$NEW" ;;

  W04)
    snippet OLD <<'EOF'
    var isDelivered: Bool {
        switch self {
EOF
    snippet NEW <<'EOF'
    var isDelivered: Bool {
        if self == .droppedAlreadyBannered { return true }
        switch self {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  W05)
    snippet OLD <<'EOF'
        let forwardedWindowIds = eligible.map(\.id)
        await receiveAdWindows(eligible)
        recordIngestCensus(
            door: door,
            analysisAssetId: analysisAssetId,
            forwardedWindowIds: forwardedWindowIds
        )
EOF
    snippet NEW <<'EOF'
        let forwardedWindowIds = eligible.map(\.id)
        recordIngestCensus(
            door: door,
            analysisAssetId: analysisAssetId,
            forwardedWindowIds: forwardedWindowIds
        )
        await receiveAdWindows(eligible)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  W06)
    snippet OLD <<'EOF'
        for (detail, count) in details.sorted(by: { $0.key < $1.key })
        where count > 0 {
            parts.append("\(detail)=\(count)")
        }
EOF
    snippet NEW <<'EOF'
        _ = details
EOF
    patch "$file" "$OLD" "$NEW" ;;

  W07)
    snippet OLD <<'EOF'
                durable: !preWindows.isEmpty
EOF
    snippet NEW <<'EOF'
                durable: true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  B01)
    snippet OLD <<'EOF'
        if endTime <= edgeMarginSeconds {
            return .rejected(reason: .tooEarly)
        }
EOF
    snippet NEW <<'EOF'
        if startTime < edgeMarginSeconds {
            return .rejected(reason: .tooEarly)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  B02)
    snippet OLD <<'EOF'
            if startTime >= tailBoundary {
                return .rejected(reason: .tooLate)
            }
EOF
    snippet NEW <<'EOF'
            if endTime > tailBoundary {
                return .rejected(reason: .tooLate)
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  B03)
    snippet OLD <<'EOF'
        if endTime <= edgeMarginSeconds {
EOF
    snippet NEW <<'EOF'
        if endTime < edgeMarginSeconds {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  B04)
    snippet OLD <<'EOF'
            if startTime >= tailBoundary {
EOF
    snippet NEW <<'EOF'
            if startTime > tailBoundary {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  B05)
    snippet OLD <<'EOF'
        inventoryFilter: InventorySanityFilter = .productionDefaultConfiguration
EOF
    snippet NEW <<'EOF'
        inventoryFilter: InventorySanityFilter = InventorySanityFilter(isEnabled: false)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  B06)
    snippet OLD <<'EOF'
    static let productionDefaultConfiguration = InventorySanityFilter(
        isEnabled: LightweightInventoryChecksSettings.defaultEnabled
    )
EOF
    snippet NEW <<'EOF'
    static let productionDefaultConfiguration = InventorySanityFilter(
        isEnabled: false
    )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  B07)
    snippet OLD <<'EOF'
            && startTime >= 0
            && endTime > startTime
EOF
    snippet NEW <<'EOF'
            && endTime > startTime
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ── playhead-y3ya: the semantic-sweep mark composer ──────────────────────

  Y01)
    snippet OLD <<'EOF'
        guard row.disposition == .containsAd else { return false }
EOF
    snippet NEW <<'EOF'
        guard row.disposition == .containsAd
            || row.disposition == .uncertain else { return false }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y02)
    snippet OLD <<'EOF'
        guard row.didExamineWindow else { return false }
        guard row.windowStartTime.isFinite, row.windowEndTime.isFinite else { return false }
EOF
    snippet NEW <<'EOF'
        guard row.windowStartTime.isFinite, row.windowEndTime.isFinite else { return false }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y03)
    snippet OLD <<'EOF'
        guard row.didExamineWindow else { return false }
EOF
    snippet NEW <<'EOF'
        guard row.status.didExamineWindow else { return false }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y04)
    snippet OLD <<'EOF'
            result.append(contentsOf: narrowed.isEmpty ? [window] : narrowed)
EOF
    snippet NEW <<'EOF'
            result.append(contentsOf: narrowed)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y05)
    snippet OLD <<'EOF'
        for (index, refinement) in refinements.enumerated()
        where !claimedRefinements.contains(index) {
            result.append(refinement)
        }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y06)
    snippet OLD <<'EOF'
        guard !presence.isEmpty else { return [] }
EOF
    snippet NEW <<'EOF'
        guard !presence.isEmpty, !anchors.isEmpty else { return [] }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y07)
    snippet OLD <<'EOF'
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
EOF
    snippet NEW <<'EOF'
            eligibilityGate: SkipEligibilityGate.eligible.rawValue,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y08)
    snippet OLD <<'EOF'
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
EOF
    snippet NEW <<'EOF'
            startEdgeAnchor: AutoSkipEdgeAnchor.stingerSnapped.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.stingerSnapped.rawValue
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y09)
    snippet OLD <<'EOF'
        let blocking = existingWindows
            .filter { $0.detectorVersion != detectorVersion }
            .map { (start: $0.startTime, end: $0.endTime) }
EOF
    snippet NEW <<'EOF'
        let blocking = existingWindows
            .filter {
                $0.detectorVersion != detectorVersion
                    && SpecialistMarkComposer.visibleDecisionStates
                        .contains($0.decisionState)
            }
            .map { (start: $0.startTime, end: $0.endTime) }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y10)
    snippet OLD <<'EOF'
    static let markConfidence = 0.70
EOF
    snippet NEW <<'EOF'
    static let markConfidence = 0.50
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y11)
    snippet OLD <<'EOF'
    static let mergeGapSeconds = 2.0
EOF
    snippet NEW <<'EOF'
    static let mergeGapSeconds = 1_000.0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y12)
    snippet OLD <<'EOF'
    static let anchorClipRadiusSeconds = 20.0
EOF
    snippet NEW <<'EOF'
    static let anchorClipRadiusSeconds = 1_000.0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y13)
    snippet OLD <<'EOF'
        if config.semanticSweepMarkEnabled,
           effectiveFMBackfillMode.canProposeNewRegions,
EOF
    snippet NEW <<'EOF'
        if !config.semanticSweepMarkEnabled,
           effectiveFMBackfillMode.canProposeNewRegions,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y14)
    snippet OLD <<'EOF'
        if semanticSweepMarkEnabled, mode.canProposeNewRegions {
EOF
    snippet NEW <<'EOF'
        if !semanticSweepMarkEnabled, mode.canProposeNewRegions {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y15)
    snippet OLD <<'EOF'
        if config.semanticSweepMarkEnabled,
           effectiveFMBackfillMode.canProposeNewRegions,
EOF
    snippet NEW <<'EOF'
        if config.semanticSweepMarkEnabled,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y16)
    snippet OLD <<'EOF'
        if semanticSweepMarkEnabled, mode.canProposeNewRegions {
EOF
    snippet NEW <<'EOF'
        if semanticSweepMarkEnabled {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y17)
    snippet OLD <<'EOF'
            .filter { $0.duration <= maximumMarkDurationSeconds }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Y18)
    snippet OLD <<'EOF'
            if var last = result.last,
               extent.start <= last.end + mergeGapSeconds,
               max(last.end, extent.end) - last.start <= maximumMarkDurationSeconds {
EOF
    snippet NEW <<'EOF'
            if var last = result.last,
               extent.start <= last.end + mergeGapSeconds {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ── playhead-lxkq: the ad-likelihood scan order ──────────────────────────

  X01)
    snippet OLD <<'EOF'
        case .evidenceAnchor: 0.8
        case .lexicalCue: 0.6
EOF
    snippet NEW <<'EOF'
        case .evidenceAnchor: 1.0
        case .lexicalCue: 1.0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X02)
    snippet OLD <<'EOF'
                score += neighbourhood.score
EOF
    snippet NEW <<'EOF'
                score = max(score, neighbourhood.score)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X03)
    snippet OLD <<'EOF'
            guard hi - lo <= maxSeedWidthSeconds else { return nil }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X04)
    snippet OLD <<'EOF'
        for (index, plan) in plans.enumerated() where !promotedSet.contains(index) {
            result.append(plan)
        }
        return result
EOF
    snippet NEW <<'EOF'
        _ = promotedSet
        return result
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X05)
    snippet OLD <<'EOF'
            if lhs.start != rhs.start { return lhs.start < rhs.start }
EOF
    snippet NEW <<'EOF'
            if lhs.start != rhs.start { return lhs.start > rhs.start }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X06)
    snippet OLD <<'EOF'
            return Neighbourhood(lo: lo - radius, hi: hi + radius, score: score)
EOF
    snippet NEW <<'EOF'
            _ = radius
            return Neighbourhood(lo: lo, hi: hi, score: score)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X07)
    snippet OLD <<'EOF'
            if !promotedIndices.isEmpty,
               promotedSeconds + duration > maxPromotedAudioSeconds {
                break
            }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X08)
    snippet OLD <<'EOF'
            if !promotedIndices.isEmpty,
               promotedSeconds + duration > maxPromotedAudioSeconds {
EOF
    snippet NEW <<'EOF'
            if promotedSeconds + duration > maxPromotedAudioSeconds {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RE-CUT. The first version relaxed both `guard score > 0` sites to `>= 0`,
  # and it SURVIVED — measured, and the measurement was right. Relaxing them
  # admits EVERY window at score 0, not just the inert seed's neighbourhood, so
  # the whole episode ties, the tie-break resolves to episode order and the
  # output is the identity permutation. The mutant was indistinguishable from
  # the linear sweep it was supposed to have broken, so it reproduced no defect.
  #
  # A floor on the score is the honest version of "an inert seed is treated as a
  # real pointer": it promotes that seed's neighbourhood and nothing else.
  X09)
    snippet OLD <<'EOF'
            let score = weight(for: seed.kind) * seed.strength
EOF
    snippet NEW <<'EOF'
            let score = max(weight(for: seed.kind) * seed.strength, 0.01)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X10)
    snippet OLD <<'EOF'
                startTime: entry.startTime,
                endTime: entry.endTime,
                kind: .evidenceAnchor,
EOF
    snippet NEW <<'EOF'
                startTime: entry.firstTime,
                endTime: entry.lastTime,
                kind: .evidenceAnchor,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X11)
    snippet OLD <<'EOF'
        self.strength = strength.isFinite ? min(max(strength, 0), 1) : 0
EOF
    snippet NEW <<'EOF'
        self.strength = strength
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X12)
    snippet OLD <<'EOF'
        items.enumerated()
            .sorted { lhs, rhs in
                let lhsKey = key(lhs.element)
                let rhsKey = key(rhs.element)
                if lhsKey != rhsKey { return lhsKey < rhsKey }
                return lhs.offset < rhs.offset
            }
            .map(\.element)
EOF
    snippet NEW <<'EOF'
        _ = key
        return items
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X13)
    snippet OLD <<'EOF'
        return AdLikelihoodScanOrder.order(plans, seeds: seeds) { ($0.startTime, $0.endTime) }
EOF
    snippet NEW <<'EOF'
        _ = seeds
        return plans
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X14)
    snippet OLD <<'EOF'
        guard adLikelihoodScanOrderEnabled, job.phase == .fullEpisodeScan else { return [] }
EOF
    snippet NEW <<'EOF'
        guard adLikelihoodScanOrderEnabled, job.phase == .scanLikelyAdSlots else { return [] }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  X15)
    snippet OLD <<'EOF'
        guard adLikelihoodScanOrderEnabled, job.phase == .fullEpisodeScan else { return [] }
EOF
    snippet NEW <<'EOF'
        guard job.phase == .fullEpisodeScan else { return [] }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # The stored property, not the `.default` literal. `AdDetectionConfig.default`
  # passes the flag on one line whose trailing rationale comment runs past the
  # newline, and this patcher's anchors are whole lines. Pinning the property
  # instead is the stronger rail anyway: it fails the shipped-config test the
  # same way AND catches a config that accepts the flag and drops it.
  X16)
    snippet OLD <<'EOF'
        self.adLikelihoodScanOrderEnabled = adLikelihoodScanOrderEnabled
EOF
    snippet NEW <<'EOF'
        self.adLikelihoodScanOrderEnabled = false
  # ── playhead-cgka: the per-test scratch lifetime ─────────────────────────

  Z01)
    snippet OLD <<'EOF'
            if let seen = entry.orphanedAtSweep, seen < now {
                doomed.append(entry.url)
                continue
            }
            if entry.orphanedAtSweep == nil { entry.orphanedAtSweep = now }
            kept.append(entry)
EOF
    snippet NEW <<'EOF'
            if entry.orphanedAtSweep == nil { entry.orphanedAtSweep = now }
            doomed.append(entry.url)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z02)
    snippet OLD <<'EOF'
            entries[index].owner = owner
            entries[index].isOwned = true
            entries[index].orphanedAtSweep = nil
EOF
    snippet NEW <<'EOF'
            entries[index].owner = owner
            entries[index].isOwned = true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z03)
    snippet OLD <<'EOF'
            guard entry.isOwned else { kept.append(entry); continue }
EOF
    snippet NEW <<'EOF'
            guard entry.isOwned else { doomed.append(entry.url); continue }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z04)
    snippet OLD <<'EOF'
        do {
            try fileManager.removeItem(at: url)
        } catch {
            makeReadableAndWritable(url)
            try? fileManager.removeItem(at: url)
        }
EOF
    snippet NEW <<'EOF'
        try? fileManager.removeItem(at: url)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z05)
    snippet OLD <<'EOF'
        guard isDirectory.boolValue else { return }
        let children = (try? fileManager.contentsOfDirectory(atPath: url.path)) ?? []
        for child in children {
            makeReadableAndWritable(url.appendingPathComponent(child))
        }
EOF
    snippet NEW <<'EOF'
        _ = isDirectory
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z06)
    snippet OLD <<'EOF'
        self.sweepEvery = max(1, sweepEvery)
EOF
    snippet NEW <<'EOF'
        self.sweepEvery = sweepEvery == 0 ? 1_000_000 : sweepEvery
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z07)
    snippet OLD <<'EOF'
        let due = registered % sweepEvery == 0
EOF
    snippet NEW <<'EOF'
        let due = registered % sweepEvery == 0 && sweepEvery < 0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z08)
    # Anchored on the RETURN as well as the adopt: the bare adopt line occurs
    # twice in this file (the controller-store factory is the other), and the
    # patcher refuses an anchor that is not unique.
    snippet OLD <<'EOF'
    TestScratchReaper.shared.adopt(dir, owner: store)
    return (store, dir)
EOF
    snippet NEW <<'EOF'
    return (store, dir)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z09)
    snippet OLD <<'EOF'
    if let owner {
        TestScratchReaper.shared.adopt(url, owner: owner)
    } else {
        TestScratchReaper.shared.register(url)
    }
EOF
    snippet NEW <<'EOF'
    if let owner {
        TestScratchReaper.shared.adopt(url, owner: owner)
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z10)
    snippet OLD <<'EOF'
    if let owner {
        TestScratchReaper.shared.adopt(url, owner: owner)
    } else {
        TestScratchReaper.shared.register(url)
    }
EOF
    snippet NEW <<'EOF'
    _ = owner
    TestScratchReaper.shared.register(url)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z11)
    snippet OLD <<'EOF'
        } else {
            entries.append(Entry(url: url, owner: owner, isOwned: true, orphanedAtSweep: nil))
            registered += 1
        }
EOF
    snippet NEW <<'EOF'
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  Z12)
    snippet OLD <<'EOF'
func wipeTestScratchRoot(at url: URL) {
    TestScratchReaper.forceRemove(url)
}
EOF
    snippet NEW <<'EOF'
func wipeTestScratchRoot(at url: URL) {
    try? FileManager.default.removeItem(at: url)
}
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A01)
    snippet OLD <<'EOF'
            guard result.scanPass == SemanticScanResult.presenceScanPass else { return nil }
            guard result.didExamineWindow else { return nil }
EOF
    snippet NEW <<'EOF'
            guard result.didExamineWindow else { return nil }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A02)
    snippet OLD <<'EOF'
            guard result.scanPass == SemanticScanResult.presenceScanPass else { return nil }
            guard result.didExamineWindow else { return nil }
EOF
    snippet NEW <<'EOF'
            guard result.scanPass == SemanticScanResult.presenceScanPass else { return nil }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A03)
    snippet OLD <<'EOF'
            guard overlapEnd > overlapStart else { return nil }
EOF
    snippet NEW <<'EOF'
            guard overlapEnd >= overlapStart else { return nil }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A04)
    snippet OLD <<'EOF'
                band: result.transcriptQuality == .good ? .moderate : .weak
EOF
    snippet NEW <<'EOF'
                band: .moderate
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A05)
    snippet OLD <<'EOF'
            guard result.scanPass == SemanticScanResult.presenceScanPass else { return nil }
EOF
    snippet NEW <<'EOF'
            guard result.scanPass != SemanticScanResult.presenceScanPass else { return nil }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A06)
    snippet OLD <<'EOF'
            transcriptQuality: aggregateTranscriptQuality(for: windowSegments),
EOF
    snippet NEW <<'EOF'
            transcriptQuality: .good,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A07)
    snippet OLD <<'EOF'
        case .blockedByFMConsensus: return 2
EOF
    snippet NEW <<'EOF'
        case .blockedByFMConsensus: return 1
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A08)
    snippet OLD <<'EOF'
        if rawValue == Self.legacyFMConsensusRawValue {
            self = .blockedByFMConsensus
            return
        }
        return nil
EOF
    snippet NEW <<'EOF'
        return nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A09)
    snippet OLD <<'EOF'
        if rawValue == Self.legacyFMConsensusRawValue {
            self = .blockedByFMConsensus
            return
        }
        return nil
EOF
    snippet NEW <<'EOF'
        self = .blockedByFMConsensus
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A10)
    snippet OLD <<'EOF'
        let overlappingWindows = FMSuppressionWindow.votingWindows(
            spanStartTime: span.startTime,
            spanEndTime: span.endTime,
            scanResults: semanticScanResults
        )
EOF
    snippet NEW <<'EOF'
        let overlappingWindows: [FMSuppressionWindow] = semanticScanResults.compactMap { result in
            let overlapStart = max(span.startTime, result.windowStartTime)
            let overlapEnd = min(span.endTime, result.windowEndTime)
            guard overlapEnd > overlapStart else { return nil }
            return FMSuppressionWindow(
                disposition: result.disposition,
                band: result.transcriptQuality == .good ? .moderate : .weak
            )
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  A11)
    snippet OLD <<'EOF'
            if decodedGate == .markOnly
                || (
EOF
    snippet NEW <<'EOF'
            if decodedGate == .markOnly
                || decodedGate == .blockedByFMConsensus
                || (
EOF
    patch "$file" "$OLD" "$NEW" ;;


  # ---- playhead-le02: a negative banner assertion needs a positive witness ----

  # The centrepiece. Arms a suggestion for a span the inventory filter rejected
  # while LEAVING THE CENSUS STAMP INTACT, so the row still reads
  # `droppedInventorySanity`. A rail that trusted the census alone survives this;
  # only reading the collection the suggest tier lands in kills it.
  LE01)
    snippet OLD <<'EOF'
                    noteIngestOutcome(
                        .droppedInventorySanity,
                        windowId: adWindow.id,
                        detail: reason.rawValue
                    )
                    continue
EOF
    snippet NEW <<'EOF'
                    noteIngestOutcome(
                        .droppedInventorySanity,
                        windowId: adWindow.id,
                        detail: reason.rawValue
                    )
                    registerSuggestedWindow(adWindow)
                    continue
EOF
    patch "$file" "$OLD" "$NEW" ;;

  LE02)
    snippet OLD <<'EOF'
                        .droppedInventorySanity,
                        windowId: adWindow.id,
                        detail: reason.rawValue
EOF
    snippet NEW <<'EOF'
                        .droppedInventorySanity,
                        windowId: adWindow.id,
                        detail: InventorySanityRejectionReason.tooShort.rawValue
EOF
    patch "$file" "$OLD" "$NEW" ;;

  LE03)
    snippet OLD <<'EOF'
            noteIngestOutcome(
                .retiredReapplyInventoryFilter,
                windowId: id,
                detail: reason?.rawValue
            )
EOF
    snippet NEW <<'EOF'
            noteIngestOutcome(
                .droppedInventorySanity,
                windowId: id,
                detail: reason?.rawValue
            )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  LE04)
    snippet OLD <<'EOF'
            noteIngestDoorOutcome(
                .doorDroppedNotPlaying,
                door: .midSessionIngest,
                analysisAssetId: analysisAssetId,
                detail: "active=\(activeAssetId ?? "nil")"
            )
            return 0
EOF
    snippet NEW <<'EOF'
            return 0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  LE05)
    snippet OLD <<'EOF'
            noteIngestDoorOutcome(
                .doorDroppedNoAdmissibleRows,
                door: door,
EOF
    snippet NEW <<'EOF'
            noteIngestDoorOutcome(
                .doorDroppedEpisodeReplaced,
                door: door,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  LE06)
    snippet OLD <<'EOF'
                    retireAllNonRevertedWindowStateIfPresent(
                        windowId: result.id
                    )
                    continue
                }
            }
EOF
    snippet NEW <<'EOF'
                    retireAllNonRevertedWindowStateIfPresent(
                        windowId: result.id
                    )
                    if let revision = result.producerRevision {
                        registerSuggestedWindow(revision)
                    }
                    continue
                }
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # Anchored on the two lines ABOVE the reset block, because `endEpisode` and
  # the begin-side reset share a byte-identical six-line sequence.
  LE07)
    snippet OLD <<'EOF'
        inAdState = false
        latestUserSeekOperationGeneration = 0
        banneredWindowIds.removeAll()
        emittedAutoSkipBannerWindowIds.removeAll()
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
EOF
    snippet NEW <<'EOF'
        inAdState = false
        latestUserSeekOperationGeneration = 0
        banneredWindowIds.removeAll()
        emittedAutoSkipBannerWindowIds.removeAll()
        suggestBanneredWindowIds.removeAll()
EOF
    patch "$file" "$OLD" "$NEW" ;;

  LE08)
    snippet OLD <<'EOF'
            if decodedGate == .markOnly
                || (
EOF
    snippet NEW <<'EOF'
            if decodedGate == .markOnly
                || adWindow.decisionState == AdDecisionState.applied.rawValue
                || (
EOF
    patch "$file" "$OLD" "$NEW" ;;


  # ---- playhead-sik9: the post-roll guard's byte-anchored exemption ----

  C01)
    snippet OLD <<'EOF'
           gate == .eligible,
           !span.carriesRediffByteExactWidth,
           let episodeDuration,
EOF
    snippet NEW <<'EOF'
           gate == .eligible,
           let episodeDuration,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C02)
    snippet OLD <<'EOF'
           gate == .eligible,
           !span.carriesRediffByteExactWidth,
           let episodeDuration,
EOF
    snippet NEW <<'EOF'
           gate == .eligible,
           span.carriesRediffByteExactWidth,
           let episodeDuration,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C03)
    snippet OLD <<'EOF'
           gate == .eligible,
           !span.carriesRediffByteExactWidth,
           let episodeDuration,
EOF
    snippet NEW <<'EOF'
           gate == .eligible,
           !(span.carriesRediffByteExactWidth || span.anchorProvenance.contains(.spliceSlot)),
           let episodeDuration,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C04)
    snippet OLD <<'EOF'
           gate == .eligible,
           !span.carriesRediffByteExactWidth,
           let episodeDuration,
EOF
    snippet NEW <<'EOF'
           gate == .eligible,
           span.anchorProvenance.isEmpty,
           let episodeDuration,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C05)
    snippet OLD <<'EOF'
           episodeDuration - span.endTime <= config.postRollGuardSeconds {
            gate = .markOnly
        }
EOF
    snippet NEW <<'EOF'
           episodeDuration - span.endTime <= config.postRollGuardSeconds {
            gate = .markOnly
        }
        if config.certaintyTieredEnabled, span.carriesRediffByteExactWidth {
            gate = .eligible
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C06)
    snippet OLD <<'EOF'
    var carriesRediffByteExactWidth: Bool {
        anchorProvenance.contains(.rediffSlot)
    }
EOF
    snippet NEW <<'EOF'
    var carriesRediffByteExactWidth: Bool {
        anchorProvenance.contains(.rediffSlot) || anchorProvenance.contains(.spliceSlot)
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C07)
    snippet OLD <<'EOF'
        guard !geometryWasRewritten else { return .unanchored }
        let rediffOwnsWidth = anchorProvenance.contains(.rediffSlot)
EOF
    snippet NEW <<'EOF'
        let rediffOwnsWidth = anchorProvenance.contains(.rediffSlot)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C08)
    snippet OLD <<'EOF'
        if blockingUnanchoredAutoSkip,
           !support.isFullyAnchored,
EOF
    snippet NEW <<'EOF'
        if !blockingUnanchoredAutoSkip,
           !support.isFullyAnchored,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C09)
    snippet OLD <<'EOF'
        recoverNonMonotonicSegments: Bool = false
EOF
    snippet NEW <<'EOF'
        recoverNonMonotonicSegments: Bool = true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  C10)
    snippet OLD <<'EOF'
        let strictUnioned = unionedPlayedSlots(strictPerBSideSlots, config: config)
        return unioned.map { slot in
            strictUnioned.contains {
                $0.startSeconds == slot.startSeconds && $0.endSeconds == slot.endSeconds
            }
        }
EOF
    snippet NEW <<'EOF'
        _ = unionedPlayedSlots(strictPerBSideSlots, config: config)
        return unioned.map { _ in true }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # --- playhead-nqey: the enablement (F01-F09) --------------------------------
  #
  # F01-F05 anchor on the trailing `// playhead-...` comment rather than on the
  # bare `field: value,` so the anchor cannot collide with the init default of
  # the same name a few hundred lines above — which is exactly the collision F02
  # exists to detect.

  F01)
    patch "$file" \
      "certaintyTieredSkipEnabled: true,  // playhead-nqey" \
      "certaintyTieredSkipEnabled: false,  // playhead-nqey" ;;

  F02)
    patch "$file" \
      "certaintyTieredSkipEnabled: Bool = true," \
      "certaintyTieredSkipEnabled: Bool = false," ;;

  F03)
    patch "$file" \
      "hostReadConfidenceFloor: 0.9,  // playhead-wraj" \
      "hostReadConfidenceFloor: 0.0,  // playhead-wraj" ;;

  F04)
    patch "$file" \
      "postRollGuardSeconds: 90.0,  // playhead-wraj" \
      "postRollGuardSeconds: 0.0,  // playhead-wraj" ;;

  F05)
    patch "$file" \
      "hostReadConfidenceFloor: 0.9,  // playhead-wraj" \
      "hostReadConfidenceFloor: 2.0,  // playhead-wraj" ;;

  F06)
    patch "$file" \
      "        certaintyTieredEnabled: Bool = false," \
      "        certaintyTieredEnabled: Bool = true," ;;

  F07)
    patch "$file" \
      "           skipConfidence < config.hostReadConfidenceFloor {" \
      "           skipConfidence <= config.hostReadConfidenceFloor {" ;;

  # EDIT re-cut by playhead-6qvf: the floor site used to inline its own
  # `contains(where: { if case .rediffSlot ... })` pattern-match. 6qvf unified it
  # onto the shared `carriesRediffByteExactWidth` so all six consumers read ONE
  # expression; the mutation is unchanged in meaning (drop the carve-out).
  F08)
    snippet OLD <<'EOF'
           !span.carriesRediffByteExactWidth,
           skipConfidence < config.hostReadConfidenceFloor {
EOF
    snippet NEW <<'EOF'
           skipConfidence < config.hostReadConfidenceFloor {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # NOTE the `Optional(...)` wrapper, which looks redundant and is not: `??`
  # yields a NON-optional `Double`, and `if let x = <non-optional>` does not
  # compile ("initializer for conditional binding must have Optional type").
  # The first cut of this EDIT lost a build to that. The mutation must stay an
  # optional binding because that is the shape of the line it replaces.
  F09)
    snippet OLD <<'EOF'
           !span.carriesRediffByteExactWidth,
           let episodeDuration,
           episodeDuration > 0,
EOF
    snippet NEW <<'EOF'
           !span.carriesRediffByteExactWidth,
           let episodeDuration = Optional(episodeDuration ?? span.endTime),
           episodeDuration > 0,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  G01)
    patch "$file" \
      "            widthProvenance = .rediffSlotChroma" \
      "            widthProvenance = .rediffSlot" ;;

  G02)
    patch "$file" \
      "            provenance: computation.widthProvenance" \
      "            provenance: .rediffSlot" ;;

  G03)
    patch "$file" \
      "            widthProvenance = .rediffSlot
        } else {" \
      "            widthProvenance = .rediffSlotChroma
        } else {" ;;

  # EDIT re-cut after the first G run: G04 SURVIVED two of its four expectations
  # because `SpanExtentSupport.derive` inlined its own `contains(.rediffSlot)`
  # rather than sharing the predicate, so widening the span property left the
  # extent tier untouched. The two spellings were unified into
  # `[AnchorRef].carriesRediffByteExactWidth`; this mutation now hits the ONE
  # definition, which is why its expected set spans predicate, extent and
  # consumer.
  G04)
    patch "$file" \
      "    var carriesRediffByteExactWidth: Bool { contains(.rediffSlot) }" \
      "    var carriesRediffByteExactWidth: Bool { contains(.rediffSlot) || contains(.rediffSlotChroma) }" ;;

  G05)
    patch "$file" \
      "    var carriesRediffChromaWidth: Bool { contains(.rediffSlotChroma) }" \
      "    var carriesRediffChromaWidth: Bool { contains(.rediffSlot) }" ;;

  G06)
    patch "$file" \
      'try container.encode("rediffSlotChroma", forKey: .type)' \
      'try container.encode("rediffSlot", forKey: .type)' ;;

  G07)
    patch "$file" \
      "        let rediffOwnsWidth = anchorProvenance.carriesRediffByteExactWidth" \
      "        let rediffOwnsWidth = anchorProvenance.contains { \$0.isWidthOwnership }" ;;

  G08)
    patch "$file" \
      "        case .spliceSlot, .rediffSlot, .rediffSlotChroma:
            return true" \
      "        case .spliceSlot, .rediffSlot:
            return true
        case .rediffSlotChroma:
            return false" ;;

  # The Equatable arm plus its comment go together: leaving the comment behind
  # would read as a live claim about an arm that no longer exists.
  G09)
    snippet OLD <<'EOF'
        case (.rediffSlotChroma, .rediffSlotChroma):
            // Bare case (playhead-6qvf): the SAME default:false trap. REQUIRED,
            // and the mutation battery (G09) is what established which
            // consequence is real — an earlier version of this comment named
            // the wrong one.
            //
            // `isWidthOwnership`'s proxies are SAFE from it: they spell the test
            // `contains(where: { $0.isWidthOwnership })`, a predicate closure
            // that never invokes `==`. What breaks is every `contains(_:)` by
            // VALUE — `[AnchorRef].carriesRediffChromaWidth` returns false for a
            // span that plainly carries the marker, and `DecodedSpan`'s own
            // synthesized equality stops matching a span against itself.
            return true
        default:
EOF
    snippet NEW <<'EOF'
        default:
EOF
    patch "$file" "$OLD" "$NEW" ;;

  G10)
    snippet OLD <<'EOF'
           !span.carriesRediffByteExactWidth,
           skipConfidence < config.hostReadConfidenceFloor {
EOF
    snippet NEW <<'EOF'
           !span.anchorProvenance.contains(where: { $0.isWidthOwnership }),
           skipConfidence < config.hostReadConfidenceFloor {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-9v09 (H series) --------------------------------------------

  # THE SHIPPED DEFECT, restored. The window is still retired; nothing says so.
  H01)
    snippet OLD <<'EOF'
            let reason = rejectionReasonsById[id]
            noteIngestOutcome(
                .retiredReapplyInventoryFilter,
                windowId: id,
                detail: reason?.rawValue
            )
            retiredCount += 1
EOF
    snippet NEW <<'EOF'
            let reason = rejectionReasonsById[id]
            retiredCount += 1
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # The other half of the same silence: counted in memory, invisible to a device
  # pull. This is the half that decides whether a support ticket can be answered.
  H02)
    snippet OLD <<'EOF'
        if retiredCount > 0 {
            recordIngestCensus(
EOF
    snippet NEW <<'EOF'
        if retiredCount < 0 {
            recordIngestCensus(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  H03)
    snippet OLD <<'EOF'
            if let reason {
                let key = "\(AdWindowIngestOutcome.retiredReapplyInventoryFilter.rawValue)"
                    + ":\(reason.rawValue)"
                details[key, default: 0] += 1
            }
EOF
    snippet NEW <<'EOF'
            _ = reason
EOF
    patch "$file" "$OLD" "$NEW" ;;

  H04)
    patch "$file" \
      "                detail: reason?.rawValue" \
      "                detail: nil" ;;

  # The suggest loop keeps its `.rejected` binding for the reason, and then
  # retires whatever the filter said — including the spans it PASSED. The
  # managed loop is deliberately left alone, so the managed rail stays green and
  # the kill cannot be credited to a blanket retire-everything.
  H05)
    snippet OLD <<'EOF'
            guard case let .rejected(reason) = verdict else { continue }
EOF
    snippet NEW <<'EOF'
            let reason: InventorySanityRejectionReason
            if case let .rejected(rejected) = verdict {
                reason = rejected
            } else {
                reason = .tooLate
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  H06)
    patch "$file" \
      "        counts.reduce(0) { \$0 + (\$1.key.isRetraction ? \$1.value : 0) }" \
      "        counts.reduce(0) { \$0 + (\$1.key.isDelivered ? \$1.value : 0) }" ;;

  H07)
    snippet OLD <<'EOF'
        if retiredCount > 0 {
            parts.append("retired=\(retiredCount)")
        }
EOF
    snippet NEW <<'EOF'
        parts.append("retired=\(retiredCount)")
EOF
    patch "$file" "$OLD" "$NEW" ;;

  H08)
    snippet OLD <<'EOF'
        case .retiredReapplyInventoryFilter:
            return true
        case .admittedManaged, .armedSuggest, .retainedAppliedReceipt,
             .doorDroppedNotPlaying, .doorDroppedStoreReadFailed,
EOF
    snippet NEW <<'EOF'
        case .retiredReapplyInventoryFilter, .armedSuggest:
            return true
        case .admittedManaged, .retainedAppliedReceipt,
             .doorDroppedNotPlaying, .doorDroppedStoreReadFailed,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # The whole `isDelivered` switch is rewritten in one patch rather than two:
  # the case has to leave the false arm as it joins the true one, and the false
  # arm's tail lines are byte-identical to `isDoorOutcome`'s, so a two-anchor
  # edit would not be unique.
  H09)
    snippet OLD <<'EOF'
        case .admittedManaged, .armedSuggest, .retainedAppliedReceipt:
            return true
        case .doorDroppedNotPlaying, .doorDroppedStoreReadFailed,
             .doorDroppedEpisodeReplaced, .doorDroppedNoAdmissibleRows,
             .droppedNoActiveEpisode, .droppedForeignAsset,
             .droppedStaleProducerRevision, .droppedTerminalProducerReplay,
             .droppedEpisodeReplaced, .droppedInvalidMaterial,
             .droppedMalformedDecisionState, .droppedMalformedEligibilityGate,
             .droppedUserResolvedSuggestion, .droppedUserReverted,
             .droppedProducerTerminalState, .droppedInventorySanity,
             .droppedBlockedGate, .droppedAlreadyBannered,
             .suggestReplayNotRearmed, .bufferedProvisionalResolution,
             .retiredReapplyInventoryFilter:
            return false
EOF
    snippet NEW <<'EOF'
        case .admittedManaged, .armedSuggest, .retainedAppliedReceipt,
             .retiredReapplyInventoryFilter:
            return true
        case .doorDroppedNotPlaying, .doorDroppedStoreReadFailed,
             .doorDroppedEpisodeReplaced, .doorDroppedNoAdmissibleRows,
             .droppedNoActiveEpisode, .droppedForeignAsset,
             .droppedStaleProducerRevision, .droppedTerminalProducerReplay,
             .droppedEpisodeReplaced, .droppedInvalidMaterial,
             .droppedMalformedDecisionState, .droppedMalformedEligibilityGate,
             .droppedUserResolvedSuggestion, .droppedUserReverted,
             .droppedProducerTerminalState, .droppedInventorySanity,
             .droppedBlockedGate, .droppedAlreadyBannered,
             .suggestReplayNotRearmed, .bufferedProvisionalResolution:
            return false
EOF
    patch "$file" "$OLD" "$NEW" ;;

  H10)
    patch "$file" \
      "                    forwarded: retiredCount," \
      "                    forwarded: 0," ;;

  # ---- playhead-gard (I series) --------------------------------------------

  I01)
    snippet OLD <<'EOF'
        case .rediffByteExact:
            return false
        case .segmentAggregated, .userAsserted, .fusion:
            return true
EOF
    snippet NEW <<'EOF'
        case .rediffByteExact:
            return true
        case .segmentAggregated, .userAsserted, .fusion:
            return true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I02)
    snippet OLD <<'EOF'
        case .rediffByteExact:
            return false
        case .segmentAggregated, .userAsserted, .fusion:
            return true
EOF
    snippet NEW <<'EOF'
        case .rediffByteExact:
            return false
        case .segmentAggregated, .userAsserted, .fusion:
            return false
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I03)
    patch "$file" \
      "        if support.tier == .deterministic {" \
      "        if support.startTier == .deterministic || support.endTier == .deterministic {" ;;

  I04)
    snippet OLD <<'EOF'
        if AdBoundaryState(rawValue: boundaryState) == .segmentAggregated {
            return .segmentAggregated
        }
        return .fusion
EOF
    snippet NEW <<'EOF'
        if AdBoundaryState(rawValue: boundaryState) == .segmentAggregated {
            return .segmentAggregated
        }
        return .rediffByteExact
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I05)
    snippet OLD <<'EOF'
        case .none: return 0.5
        case .corroborated: return 1.0
        case .deterministic: return 1.5
EOF
    snippet NEW <<'EOF'
        case .none: return 1.0
        case .corroborated: return 1.0
        case .deterministic: return 1.0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I06)
    snippet OLD <<'EOF'
        guard detector.consultsShowTrust else {
            return DetectorTrustEntry(
                trustScore: 0.5,
                mode: SkipDetectorClass.showIndependentSeedMode.rawValue,
                falseSkipWeight: 0,
                observationCount: 0
            )
        }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I07)
    snippet OLD <<'EOF'
        return DetectorTrustEntry(
            trustScore: profile.skipTrustScore,
            mode: profile.mode,
            falseSkipWeight: Double(profile.recentFalseSkipSignals),
            observationCount: profile.observationCount
        )
EOF
    snippet NEW <<'EOF'
        return DetectorTrustEntry(
            trustScore: 0.5,
            mode: SkipDetectorClass.showIndependentSeedMode.rawValue,
            falseSkipWeight: 0,
            observationCount: 0
        )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I08)
    snippet OLD <<'EOF'
        guard !entries.isEmpty else { return nil }
        guard let data = try? JSONEncoder().encode(self) else { return nil }
        return String(data: data, encoding: .utf8)
EOF
    snippet NEW <<'EOF'
        guard !entries.isEmpty else { return nil }
        return nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I09)
    snippet OLD <<'EOF'
        switch currentMode {
        case .auto:
            if falseSkipWeight >= Double(config.autoToManualFalseSignals) {
                return .manual
            }
        case .manual:
            if falseSkipWeight >= Double(config.manualToShadowFalseSignals) {
                return .shadow
            }
        case .shadow:
            break
        }
        return currentMode
EOF
    snippet NEW <<'EOF'
        _ = config
        _ = falseSkipWeight
        return currentMode
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I10)
    snippet OLD <<'EOF'
        let newFalseSignals = max(0, profile.recentFalseSkipSignals - 1)
EOF
    snippet NEW <<'EOF'
        let newFalseSignals = profile.recentFalseSkipSignals
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I11)
    patch "$file" \
      "        let windowSkipMode = skipMode(for: managed.adWindow)" \
      "        let windowSkipMode = activeSkipMode" ;;

  I12)
    snippet OLD <<'EOF'
            if let handler = correctObservationHandlerForTesting {
                let detector = detectorClass(for: suggested)
                Task {
                    await handler(podcastId, detector)
                }
            } else if let trustService {
                let detector = detectorClass(for: suggested)
                Task {
                    await trustService.recordCorrectObservation(
                        podcastId: podcastId,
                        detector: detector
                    )
                }
            }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I13)
    snippet OLD <<'EOF'
                let attributions = [
                    vetoAttribution(for: requestedManaged.adWindow)
                ]
                Task {
                    await trustService.recordFalseSkipSignal(
                        podcastId: sourceShowId,
                        attributions: attributions
                    )
                }
EOF
    snippet NEW <<'EOF'
                Task {
                    await trustService.recordFalseSkipSignal(
                        podcastId: sourceShowId
                    )
                }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # BOTH sites, because they are one defect with two carriers and the first I
  # run proved each survives the other: removing it only from the veto path
  # leaves `creditIsNotShared` green, and vice versa.
  I14)
    snippet OLD <<'EOF'
        if !strongestTierByDetector.isEmpty {
            ledger = Self.materialized(ledger, from: profile)
        }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW"
    patch "$file" \
      "        var ledger = Self.materialized(profile.detectorTrustLedger, from: profile)" \
      "        var ledger = profile.detectorTrustLedger" ;;

  I15)
    snippet OLD <<'EOF'
                                mode: mode.rawValue,
                                falseSkipWeight: 0,
                                observationCount: entry.observationCount
EOF
    snippet NEW <<'EOF'
                                mode: mode.rawValue,
                                falseSkipWeight: entry.falseSkipWeight,
                                observationCount: entry.observationCount
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I16)
    snippet OLD <<'EOF'
            if existing == nil || attribution.tier > (existing ?? .none) {
EOF
    snippet NEW <<'EOF'
            if existing == nil || attribution.tier < (existing ?? .none) {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I17)
    patch "$file" \
      "                * (weak ? 0.5 : 1.0)" \
      "                * 1.0" ;;

  I18)
    snippet OLD <<'EOF'
            byDetector[detector] = ledger
                .entry(for: detector, seededFrom: profile)
                .skipMode
EOF
    snippet NEW <<'EOF'
            byDetector[detector] = showMode
EOF
    patch "$file" "$OLD" "$NEW" ;;

  I21)
    patch "$file" \
      "        var modes: [SkipDetectorClass: SkipMode] = [:]" \
      "        var modes: [SkipDetectorClass: SkipMode] = [:]
        if !modes.isEmpty || true { return modes }" ;;

  I19)
    snippet OLD <<'EOF'
            return DetectorSkipModes(
                showMode: .shadow,
                resolution: .unrecognizedTrustProfileMode,
                byDetector: [:]
            )
EOF
    snippet NEW <<'EOF'
            return DetectorSkipModes(
                showMode: .shadow,
                resolution: .unrecognizedTrustProfileMode,
                byDetector: [.rediffByteExact: .auto]
            )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # EDIT RE-CUT after the first I run. The original dropped the exempt class
  # from the override's map — an EQUIVALENT MUTANT, because
  # `DetectorSkipModes.mode(for:)` falls back to `showMode`, which the override
  # has just set to the same value. It is not a defect and no test should have
  # caught it. What IS a defect is the override never reaching the per-detector
  # map at all: the stale map from `beginEpisode` then governs, and a class
  # seeded `.auto` keeps skipping after the listener asked for shadow.
  I20)
    snippet OLD <<'EOF'
        activeDetectorSkipModes = DetectorSkipModes(
            showMode: mode,
            resolution: .sessionOverride,
            byDetector: Dictionary(
                uniqueKeysWithValues: SkipDetectorClass.allCases.map {
                    ($0, mode)
                }
            )
        )
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  K201)
    patch "$file" \
      "        guard let watermark, watermark.isFinite, watermark >= shardEnd else {" \
      "        guard let watermark, watermark.isFinite, watermark >= 0 else {" ;;

  K202)
    patch "$file" \
      "        return overlaps(start: shardStart, end: shardEnd)" \
      "        return true" ;;

  K203)
    patch "$file" \
      "            if var last = merged.last, range.start <= last.end {" \
      "            if var last = merged.last, range.start < last.end {" ;;

  K204)
    patch "$file" \
      "            .filter { \$0.start.isFinite && \$0.end.isFinite && \$0.end > \$0.start }" \
      "            .filter { \$0.start.isFinite && \$0.end.isFinite && \$0.end >= \$0.start }" ;;

  K205)
    patch "$file" \
      "        if candidate >= 0, intervals[candidate].end > start {" \
      "        if candidate >= 0, intervals[candidate].end >= start {" ;;

  K206)
    patch "$file" \
      "              AND pass = 'fast'" \
      "              AND pass IS NOT NULL" ;;

  K207)
    patch "$file" \
      "              AND endTime > startTime" \
      "              AND endTime >= startTime" ;;

  K208)
    patch "$file" \
      "        guard let watermark, watermark.isFinite, watermark >= shardEnd else {" \
      "        guard let watermark, watermark.isFinite, watermark > shardEnd else {" ;;

  K209)
    patch "$file" \
      "        return next < intervals.count && intervals[next].start < end" \
      "        return next < intervals.count && intervals[next].start <= end" ;;

  K210)
    patch "$file" \
      "        return uncovered + covered" \
      "        return covered + uncovered" ;;

  # ---- playhead-kkzu: the show a background download carries (KZ series) ----

  # KZ01 — THE defect, restored. `handleBackgroundDownloadComplete` is the only
  # thing that enqueues analysis for a background/auto download; before this
  # bead it passed `context: nil` unconditionally. The mutation is the modern
  # spelling of exactly that: a named absence in place of the recovered record.
  # It looks entirely reasonable — "the completion cannot know the show" — and
  # is silent, because a NULL podcastId fails nothing.
  KZ01)
    snippet OLD <<'EOF'
            await enqueueAnalysisIfNeeded(
                episodeId: episodeId,
                sourceFingerprint: strongHash,
                context: attribution
            )
EOF
    snippet NEW <<'EOF'
            await enqueueAnalysisIfNeeded(
                episodeId: episodeId,
                sourceFingerprint: strongHash,
                context: .unattributed(
                    reason: .resumeWithoutRecordedShow,
                    isExplicitDownload: false
                )
            )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ02 — the same loss one step earlier: never write the record. Plausible as
  # a "why persist what the caller already has" simplification, and it is the
  # mutation an in-memory implementation would be indistinguishable from.
  KZ02)
    snippet OLD <<'EOF'
        persistDownloadAttribution(episodeId: episodeId, context: context)
EOF
    snippet NEW <<'EOF'
        _ = context
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ03 — reap the attribution alongside the resume blob. This reads as
  # symmetry ("clean up everything for this episode") and is precisely wrong: a
  # SUSPENSION is not terminal, and the force-quit resume path has no other
  # route back to the show.
  KZ03)
    snippet OLD <<'EOF'
        // playhead-wrj8: symmetric cleanup of the freshness sidecar.
        try? fm.removeItem(at: validatorURL)
        reportedSuspendedTransfers.remove(episodeId)
EOF
    snippet NEW <<'EOF'
        // playhead-wrj8: symmetric cleanup of the freshness sidecar.
        try? fm.removeItem(at: validatorURL)
        deleteDownloadAttribution(episodeId: episodeId)
        reportedSuspendedTransfers.remove(episodeId)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ04 — admit the identity in whatever spelling arrived. `""` then becomes a
  # show key rather than a named absence, which is strictly worse than NULL: it
  # JOINS, pooling every unattributed episode into one fake show.
  KZ04)
    snippet OLD <<'EOF'
            canonicalPodcastId:
                RecurrenceMaterialIdentity.canonicalIdentifier(podcastId),
            unattributedReason: .showIdentityUnresolvable,
EOF
    snippet NEW <<'EOF'
            canonicalPodcastId: podcastId,
            unattributedReason: .showIdentityUnresolvable,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ05 — store the reason unconditionally. Every context then claims a reason
  # it has no absence for, and "carries a reason" stops distinguishing an
  # unknowable identity from a known one.
  KZ05)
    snippet OLD <<'EOF'
        self.unattributedReason =
            canonicalPodcastId == nil ? unattributedReason : nil
EOF
    snippet NEW <<'EOF'
        self.unattributedReason = unattributedReason
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ06 — the auto-download drops the show again. This is the original bug's
  # home: the feed URL is right there in `FeedRefreshNewEpisode`, and the whole
  # population is the one whose work should be done before the user presses play.
  KZ06)
    snippet OLD <<'EOF'
                context: .resolving(
                    podcastId: episode.feedURL.absoluteString,
EOF
    snippet NEW <<'EOF'
                context: .resolving(
                    podcastId: nil,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ07 — attribute every episode in the sweep to the FIRST group's feed. A
  # rail that only asserts "podcastId is not nil" passes this happily, and the
  # result is worse than a NULL: real episodes filed under the wrong show.
  KZ07)
    snippet OLD <<'EOF'
                    podcastId: episode.feedURL.absoluteString,
EOF
    snippet NEW <<'EOF'
                    podcastId: groups.first?.feedURL.absoluteString,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ08 — the sharpest case from the bead: the prepare coordinator receives a
  # podcastId, threads it into the analysis half, and drops it for the download
  # half. Restored verbatim.
  KZ08)
    snippet OLD <<'EOF'
            context: .resolving(
                podcastId: request.podcastId,
EOF
    snippet NEW <<'EOF'
            context: .resolving(
                podcastId: nil,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ09 — the force-quit resume stops recovering. It looks defensible ("this
  # path cannot know the show"), and it is the one call site where that
  # sentence is nearly true — but the sidecar written by the ORIGINAL queue is
  # exactly the thing that makes it false.
  KZ09)
    snippet OLD <<'EOF'
                let recovered = loadDownloadAttribution(episodeId: episodeId)
                    ?? .unattributed(
                        reason: .resumeWithoutRecordedShow,
                        isExplicitDownload: false
                    )
EOF
    snippet NEW <<'EOF'
                let recovered = DownloadContext.unattributed(
                    reason: .resumeWithoutRecordedShow,
                    isExplicitDownload: false
                )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ10 — a cancel stops reaping. Not a correctness defect in the enqueue, but
  # an unbounded on-disk leak: one record per cancelled episode, forever.
  KZ10)
    snippet OLD <<'EOF'
            deleteDownloadAttribution(episodeId: episodeId)
            logger.info("Cancelled download for \(episodeId)")
EOF
    snippet NEW <<'EOF'
            logger.info("Cancelled download for \(episodeId)")
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KZ11 — a cache clear stops taking the attribution. The enumeration stays
  # (so the fail-closed check still passes) and only the removal is dropped,
  # which is exactly how this kind of leak survives review.
  KZ11)
    snippet OLD <<'EOF'
        for contents in [partialContents, resumeContents, attributionContents] {
EOF
    snippet NEW <<'EOF'
        _ = attributionContents
        for contents in [partialContents, resumeContents] {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-kanf: a user-intent tap promotes a queued job (KN series) ----

  # KN01 — the tap always wins. Drop the promotion guard entirely, at both
  # sites. This is the most likely wrong fix for this bead and it reads as the
  # most user-friendly one: the user asked, so promote it. It re-ranks a row a
  # worker is executing, which races the lease/epoch stamping, and it is
  # completely silent — a promoted running job still finishes.
  KN01)
    snippet OLD <<'EOF'
        guard job.state == "queued", !leased else {
            return .notPromotable(
                jobId: job.jobId,
                state: job.state,
                priority: job.priority,
                leased: leased
            )
        }
EOF
    snippet NEW <<'EOF'
        // guard removed
EOF
    patch "$file" "$OLD" "$NEW" || return $?
    snippet OLD <<'EOF'
            WHERE workKey = ?
              AND state = 'queued'
              AND leaseOwner IS NULL
              AND priority < ?
EOF
    snippet NEW <<'EOF'
            WHERE workKey = ?
              AND priority < ?
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KN02 — "state is enough". Keep `state = 'queued'`, drop `leaseOwner IS
  # NULL`. This is the bead's own wording taken literally, and it is the
  # mutation with the narrowest kill set: only a QUEUED row still carrying a
  # stale lease can see it. That row is not hypothetical — `fetchNextEligibleJob`
  # selects `state IN ('queued','paused') AND (leaseOwner IS NULL OR
  # leaseExpiresAt < ?)`, so the schema produces it on every lease expiry.
  KN02)
    snippet OLD <<'EOF'
        guard job.state == "queued", !leased else {
EOF
    snippet NEW <<'EOF'
        guard job.state == "queued" else {
EOF
    patch "$file" "$OLD" "$NEW" || return $?
    snippet OLD <<'EOF'
              AND state = 'queued'
              AND leaseOwner IS NULL
              AND priority < ?
EOF
    snippet NEW <<'EOF'
              AND state = 'queued'
              AND priority < ?
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KN03 — the mirror image: keep the lease check, drop the state check. Reads
  # as the more sophisticated fix ("the lease is the real authority; state is
  # derived"), and it promotes every `running` row whose worker crashed without
  # clearing the lease, plus `paused` and terminal rows.
  KN03)
    snippet OLD <<'EOF'
        guard job.state == "queued", !leased else {
EOF
    snippet NEW <<'EOF'
        guard !leased else {
EOF
    patch "$file" "$OLD" "$NEW" || return $?
    snippet OLD <<'EOF'
              AND state = 'queued'
              AND leaseOwner IS NULL
EOF
    snippet NEW <<'EOF'
              AND leaseOwner IS NULL
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KN04 — collapse "already served" into "refused". Entirely reasonable on its
  # face — no row changed, so report no promotion — and it is what a Bool
  # return would have forced. The cost is invisible: the one-shot flag is then
  # never spent on a row that is already `.now`, so it survives forever and
  # every later enqueue for that episode inherits a lane nobody asked for.
  KN04)
    snippet OLD <<'EOF'
        if job.priority >= priority {
            return .alreadyPromoted(jobId: job.jobId, priority: job.priority)
        }
EOF
    snippet NEW <<'EOF'
        if job.priority >= priority {
            return .notPromotable(
                jobId: job.jobId,
                state: job.state,
                priority: job.priority,
                leased: leased
            )
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KN05 — report a refusal as served. The single most tempting simplification
  # on the scheduler side ("we asked, we're done"), and it restores exactly half
  # of the original defect: the tap still does nothing to a leased row, and now
  # the one-shot flag is burned for it too, so the retry that would have worked
  # never happens.
  KN05)
    snippet OLD <<'EOF'
                    (playhead-kanf: never re-rank a leased/running row; flag retained)
                    """
                )
                return false
EOF
    snippet NEW <<'EOF'
                    (playhead-kanf: never re-rank a leased/running row; flag retained)
                    """
                )
                return true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KN06 — THE defect, restored. Skip the promotion and call the intent served.
  # This is not a strawman: it is precisely what the code did before this bead,
  # and it was DOCUMENTED as intended ("Recording intent for an episode that
  # already has a queued job does NOT retroactively promote it"). It fails
  # nothing, logs nothing alarming, and the control still shows live status.
  KN06)
    snippet OLD <<'EOF'
                userIntentServed = await promoteExistingJobToUserIntentLane(
                    episodeId: episodeId,
                    workKey: workKey,
                    priority: priority
                )
EOF
    snippet NEW <<'EOF'
                userIntentServed = true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KN07 — consume the flag unconditionally, as the pre-kanf code did. The
  # promotion still works, so every promotion rail stays green; what breaks is
  # only the RETRY, which is visible nowhere except in the priority a later
  # enqueue lands at.
  KN07)
    snippet OLD <<'EOF'
        if userIntentServed {
            pendingUserIntentEpisodes.remove(episodeId)
            pendingUserIntentCoverage[episodeId] = nil
        }
EOF
    snippet NEW <<'EOF'
        logger.debug("user_intent_served=\(userIntentServed)")
        pendingUserIntentEpisodes.remove(episodeId)
        pendingUserIntentCoverage[episodeId] = nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KN08 — promote to the SOON floor instead of the NOW floor. The plausible
  # compromise ("a tap shouldn't outrank playback"), and it silently gives up
  # the entire point: `.soon` is deferred work, which is the lane the job was
  # already starving in. Only the exact floor distinguishes it.
  KN08)
    snippet OLD <<'EOF'
                    priority: priority
EOF
    snippet NEW <<'EOF'
                    priority: Self.soonLanePriorityFloor
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # KN09 — treat the promotion as a lifecycle transition rather than a
  # selection nudge: rotate the routing identity while re-ranking. Reads as
  # hygiene ("a re-ranked row is a new dispatch generation") and breaks
  # orphan-recovery routing, which joins `analysis_jobs` to `work_journal` on
  # {generationID, schedulerEpoch}. Nothing fails at promotion time.
  KN09)
    snippet OLD <<'EOF'
            SET priority = ?, updatedAt = ?
            WHERE workKey = ?
EOF
    snippet NEW <<'EOF'
            SET priority = ?, updatedAt = ?, generationID = ''
            WHERE workKey = ?
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-5n8k: coarse-pass diagnostic observer scoping (TS series) ----
  #
  # The hook's whole lifetime is two functions: `withCoarsePassDiagnosticObserver`
  # installs, `emitCoarsePassDiagnostic` reads. Both exist so these mutations
  # have one anchor apiece instead of five scattered reads, and so a regression
  # cannot come back through whichever emit site a mutation happened not to pick.

  # TS01 — THE historical defect, restored verbatim rather than approximated.
  # Install assigns a process-global and restores it in a `defer`; the funnel
  # reads that global. This is byte-for-byte what the code did before this bead,
  # down to the save/restore, which is why it is worth two edits instead of one:
  # a leak-style approximation (TS02) would have let the rails be judged against
  # a defect the repo never actually had.
  #
  # The defect was never about one test's own bookkeeping — save/restore is
  # correct inside one task — but about a second, concurrent installer
  # overwriting the first so the first's windows go somewhere else. Hence the
  # two named expectations. RUN 2026-08-03: KILLED, and it took all five tests
  # down including the bd-34e consumer, which is the ORIGINAL playhead-5n8k
  # failure reproduced on demand. See the series header for why the extra kills
  # are the finding rather than a wider claim.
  TS01)
    snippet OLD <<'EOF'
    static func withCoarsePassDiagnosticObserver<R>(
        _ observer: @escaping @Sendable (CoarsePassWindowDiagnostic) -> Void,
        operation: () async throws -> R
    ) async rethrows -> R {
        try await $coarsePassDiagnosticObserver.withValue(observer, operation: operation)
    }
EOF
    snippet NEW <<'EOF'
    static func withCoarsePassDiagnosticObserver<R>(
        _ observer: @escaping @Sendable (CoarsePassWindowDiagnostic) -> Void,
        operation: () async throws -> R
    ) async rethrows -> R {
        let previous = CoarsePassDiagnosticGlobal.observer
        CoarsePassDiagnosticGlobal.observer = observer
        defer { CoarsePassDiagnosticGlobal.observer = previous }
        return try await operation()
    }

    enum CoarsePassDiagnosticGlobal {
        nonisolated(unsafe) static var observer: (@Sendable (CoarsePassWindowDiagnostic) -> Void)?
    }
EOF
    patch "$file" "$OLD" "$NEW" || return $?
    snippet OLD <<'EOF'
    private static func emitCoarsePassDiagnostic(_ diagnostic: CoarsePassWindowDiagnostic) {
        coarsePassDiagnosticObserver?(diagnostic)
    }
EOF
    snippet NEW <<'EOF'
    private static func emitCoarsePassDiagnostic(_ diagnostic: CoarsePassWindowDiagnostic) {
        CoarsePassDiagnosticGlobal.observer?(diagnostic)
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # TS02 — the mirror image, and a regression that is easier to write by
  # accident than TS01 is: the funnel remembers the last binding it saw and
  # falls back to it, so a binding outlives its scope and an UNBOUND task
  # inherits it. Its own mechanism is visible to the lifetime rails, which is
  # the complement of TS01 and the argument for keeping both.
  # RUN 2026-08-03: KILLED, all five, for the same reason TS01 was.
  TS02)
    snippet OLD <<'EOF'
    private static func emitCoarsePassDiagnostic(_ diagnostic: CoarsePassWindowDiagnostic) {
        coarsePassDiagnosticObserver?(diagnostic)
    }
EOF
    snippet NEW <<'EOF'
    private static func emitCoarsePassDiagnostic(_ diagnostic: CoarsePassWindowDiagnostic) {
        if let bound = coarsePassDiagnosticObserver {
            CoarsePassDiagnosticLeak.observer = bound
        }
        (coarsePassDiagnosticObserver ?? CoarsePassDiagnosticLeak.observer)?(diagnostic)
    }

    private enum CoarsePassDiagnosticLeak {
        nonisolated(unsafe) static var observer: (@Sendable (CoarsePassWindowDiagnostic) -> Void)?
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # TS03 — the vacuity control. Nothing is ever emitted. Every rail in the
  # scoping suite is built as "my own windows arrived AND nothing else did", so
  # this must kill all four; if it did not, the negative half would be passing
  # on an observer that never fires. The bd-34e consumer test is in the kill set
  # too, as the check that the funnel is genuinely load-bearing.
  # RUN 2026-08-03: KILLED, all five.
  TS03)
    snippet OLD <<'EOF'
    private static func emitCoarsePassDiagnostic(_ diagnostic: CoarsePassWindowDiagnostic) {
        coarsePassDiagnosticObserver?(diagnostic)
    }
EOF
    snippet NEW <<'EOF'
    private static func emitCoarsePassDiagnostic(_ diagnostic: CoarsePassWindowDiagnostic) {
        _ = diagnostic
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # TS04 — the documented cost of choosing a task-local, made concrete. A
  # `Task.detached` does not inherit task-locals, so an emit moved onto one
  # reads nil and the diagnostic is lost. This is the regression the source
  # comment on `coarsePassDiagnosticObserver` warns about; encoding it is what
  # turns that warning from a note into something a test can enforce.
  # RUN 2026-08-03: KILLED, all five — so the one real cost of choosing a
  # task-local is now a test failure rather than diagnostics quietly stopping.
  TS04)
    snippet OLD <<'EOF'
    private static func emitCoarsePassDiagnostic(_ diagnostic: CoarsePassWindowDiagnostic) {
        coarsePassDiagnosticObserver?(diagnostic)
    }
EOF
    snippet NEW <<'EOF'
    private static func emitCoarsePassDiagnostic(_ diagnostic: CoarsePassWindowDiagnostic) {
        Task.detached { Self.coarsePassDiagnosticObserver?(diagnostic) }
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-26od: the mid-flight coarse checkpoint (CK series) ----

  # CK01 — delete the observer from the DISPATCHING call site. This is the one
  # that mattered: `PlayheadRuntime` supplies a router and a permissive
  # classifier unconditionally on iOS 26+, so this is the only overload a
  # shipped build ever reaches, and `onWindowsBanked:` defaults to nil — so the
  # edit compiles silently and the feature is simply dead on device.
  CK01)
    snippet OLD <<'EOF'
                seeds: scanOrderSeeds,
                onProgress: onCoarseProgress,
                onWindowsBanked: onCoarseWindowsBanked
            )
        } else {
EOF
    snippet NEW <<'EOF'
                seeds: scanOrderSeeds,
                onProgress: onCoarseProgress
            )
        } else {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK02 — check `isDefused` on ENTRY only, not per iteration. The row loop
  # suspends on every write, so a checkpoint that entered while the job was
  # alive resumes after `runJob` has exited and writes the whole remainder of
  # its bank onto a row whose run is over.
  CK02)
    snippet OLD <<'EOF'
                guard !box.isDefused else { return }
                let result = makeScanResult(
EOF
    snippet NEW <<'EOF'
                let result = makeScanResult(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK03 — delete the POST-LOOP guard. Separate rule from CK02's: it is the one
  # protecting playhead-t1kq's rewind, which nulls the cursor on a
  # fully-covered cancellation. A late checkpoint puts the partial cursor back
  # and strands the refinement the rewind exists to preserve.
  CK03)
    snippet OLD <<'EOF'
        guard !box.isDefused else { return }

        // Coverage is computed from what LANDED, not from what the pass
EOF
    snippet NEW <<'EOF'
        // Coverage is computed from what LANDED, not from what the pass
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK04 — rewrite the whole banked prefix on every checkpoint. Correct rows,
  # O(W^2) writes and O(W^2) JSON encodings across a pass, on a thermally
  # constrained phone, in the middle of the pass this bead keeps alive.
  CK04)
    snippet OLD <<'EOF'
            for index in alreadyProcessed..<banked.windows.count {
EOF
    snippet NEW <<'EOF'
            for index in 0..<banked.windows.count {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK05 — remove the EMPTY-needle guard. The empty set is a subset of every
  # set, so a refless outcome attributes to the first plan. Only observable with
  # exactly ONE plan, because CK06's ambiguity guard catches every larger case —
  # which is why the test carries a one-plan fixture.
  CK05)
    snippet OLD <<'EOF'
            guard !refs.isEmpty else { return nil }
EOF
    snippet NEW <<'EOF'
            guard refs.count >= 0 else { return nil }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK06 — revert attribution to the FIRST superset. Two plans can share a line
  # ref whenever `segmentIndex` repeats (which the classifier tolerates by
  # design), and a shrunk sub-window of the later plan is then also a subset of
  # the earlier one's refs — crediting a plan nobody screened.
  CK06)
    snippet OLD <<'EOF'
            var unique: Int?
            for candidate in planLineRefSets where needle.isSubset(of: candidate.lineRefs) {
                guard unique == nil else { return nil }
                unique = candidate.windowIndex
            }
            return unique
EOF
    snippet NEW <<'EOF'
            return planLineRefSets.first(where: { needle.isSubset(of: $0.lineRefs) })?.windowIndex
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK07 — delete the lease guard. An orphaned pass keeps refreshing
  # `updatedAt` on a jobId a LATER re-drive left `queued`/`deferred`, pulling it
  # into `running` and pinning it where `resetStrandedBackfillJobs` will not
  # sweep it. A `running` row nobody sweeps is the stall qk44 exists to prevent.
  CK07)
    snippet OLD <<'EOF'
    func touchCoarseLeaseIfLive(box: CoarseCheckpointBox, jobId: String) async {
        guard !box.isDefused else { return }
EOF
    snippet NEW <<'EOF'
    func touchCoarseLeaseIfLive(box: CoarseCheckpointBox, jobId: String) async {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK13 — delete the lease REFRESH itself, as opposed to CK07's lifetime bound
  # on it. Without this entry the only thing standing between a silent
  # regression and a 12-45 minute frozen `updatedAt` is a source-grep canary,
  # and playhead-26od's own extraction is what proved that canary can go stale
  # while the behaviour is intact.
  CK13)
    snippet OLD <<'EOF'
        // `try?` matches the other heartbeat sites: a lease touch that loses a
        // race with a terminal transition must not fail the pass.
        try? await store.markBackfillJobRunning(jobId: jobId)
EOF
    snippet NEW <<'EOF'
        logger.debug("coarse lease tick job=\(jobId, privacy: .public)")
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK09 — drop the failure list from the MID-FLIGHT walk. playhead-qbib: a plan
  # that produced both a recovered window and an un-recovered remainder is not
  # covered, and without this argument the checkpoint credits it from the half
  # that landed.
  CK09)
    snippet OLD <<'EOF'
            failedWindows: banked.failedWindows,
EOF
    snippet NEW <<'EOF'
            failedWindows: [],
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK10 — drop the monotonic merge. A resumed pass can plan a window that
  # BEGINS below the cursor it resumed from (a plan's `startTime` is a MIN over
  # its segments), the cap pulls the walk value down to that start, and the raw
  # value then overwrites a higher cursor already in the database.
  CK10)
    snippet OLD <<'EOF'
        let merged = priorCursor.map { honest.monotonic(from: $0) } ?? honest
EOF
    snippet NEW <<'EOF'
        let merged = honest
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK11 — hardcode the checkpoint row's `runMode`. On a killed pass the
  # checkpoint's row is the ONLY row that window will ever have, so a
  # discriminator it gets wrong is wrong permanently — and the end-of-pass
  # digest's re-write hides it on every pass that completes.
  CK11)
    snippet OLD <<'EOF'
                    status: .success,
                    runMode: runMode
                )
                var wrote = true
EOF
    snippet NEW <<'EOF'
                    status: .success,
                    runMode: .shadow
                )
                var wrote = true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CK12 — split the snapshot at the OFFERED count instead of the DURABLE one.
  # The cursor then credits a window whose row did not land, which tells the
  # next run to skip audio nothing covers: pmp9's hole, reintroduced by the fix
  # for a different bug.
  CK12)
    snippet OLD <<'EOF'
            durableWindowCount: box.durableWindowCount
EOF
    snippet NEW <<'EOF'
            durableWindowCount: box.processedWindowCount
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-fil5: the durable semantic-scan claim (SC series) ----

  # SC01 — read UNMEASURED as sufficient. `adScanFraction` is `nil` exactly when
  # no coverage-lane scan row exists, so this silences the claim for the
  # never-scanned assets it was built to rescue and for nothing else.
  SC01)
    snippet OLD <<'EOF'
        guard let adScanFraction, adScanFraction.isFinite else { return true }
        return adScanFraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
EOF
    snippet NEW <<'EOF'
        guard let adScanFraction, adScanFraction.isFinite else { return false }
        return adScanFraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC02 — the claim floor drifts onto the TRANSCRIPT's number.
  SC02)
    snippet OLD <<'EOF'
        return adScanFraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
EOF
    snippet NEW <<'EOF'
        return adScanFraction < AnalysisCoordinator.finalizeBackfillMinCoverageRatio
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC03 — and the mirror image: the transcript gate borrows the AD SCAN's.
  SC03)
    snippet OLD <<'EOF'
        return ratio + 1e-9 >= AnalysisCoordinator.finalizeBackfillMinCoverageRatio
EOF
    snippet NEW <<'EOF'
        return ratio + 1e-9 >= episodePreparationCompleteThreshold
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC04 — the suppressing gate's unmeasurable direction flips permissive.
  SC04)
    snippet OLD <<'EOF'
              let episodeDurationSec, episodeDurationSec.isFinite, episodeDurationSec > 0 else {
            return false
        }
EOF
    snippet NEW <<'EOF'
              let episodeDurationSec, episodeDurationSec.isFinite, episodeDurationSec > 0 else {
            return true
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC05 — a private identity. The row still persists and still names its gate;
  # it just never coincides with the job the runner derives, so it is an orphan.
  SC05)
    snippet OLD <<'EOF'
            transcriptVersion: transcriptVersion,
            phase: .fullEpisodeScan,
            offset: 0
        )
    }
EOF
    snippet NEW <<'EOF'
            transcriptVersion: transcriptVersion,
            phase: .scanLikelyAdSlots,
            offset: 0
        )
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC06 — hash the RAW persisted rows. Right on every single-pass asset, wrong
  # on every asset a final pass has touched.
  SC06)
    snippet OLD <<'EOF'
        TranscriptAtomizer.transcriptVersionHash(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
        )
EOF
    snippet NEW <<'EOF'
        TranscriptAtomizer.transcriptVersionHash(
            chunks: chunks
        )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC07 — the shared hash stops ORDERING its input. It then agrees with
  # `atomize` on already-sorted chunks and diverges on everything else.
  SC07)
    snippet OLD <<'EOF'
        versionHash(ofSorted: chunks.sorted(by: TranscriptChunkCanonicalizer.canonicalTimeOrder))
EOF
    snippet NEW <<'EOF'
        versionHash(ofSorted: chunks)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC08 — charge a closed gate as a failed attempt.
  SC08)
    snippet OLD <<'EOF'
            progressCursor: nil,
            retryCount: 0,
            deferReason: gate.deferReason,
EOF
    snippet NEW <<'EOF'
            progressCursor: nil,
            retryCount: AdmissionController.maxRetries,
            deferReason: gate.deferReason,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC09 — persist "" as if it were a podcast id.
  SC09)
    snippet OLD <<'EOF'
            podcastId: (podcastId?.isEmpty ?? true) ? nil : podcastId,
EOF
    snippet NEW <<'EOF'
            podcastId: podcastId,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC10 — the FM-unavailable gate stops recording. THE field gate.
  SC10)
    snippet OLD <<'EOF'
            await recordSemanticScanClaim(
                gate: .foundationModelsUnavailable,
                chunks: chunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
            return wrap(.requeued)
EOF
    snippet NEW <<'EOF'
            return wrap(.requeued)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC11 — the missing-podcastId gate stops recording.
  SC11)
    snippet OLD <<'EOF'
            await recordSemanticScanClaim(
                gate: .podcastIdMissing,
                chunks: canonicalChunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
EOF
    snippet NEW <<'EOF'
            _ = canonicalChunks
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC12 — the missing-factory gate stops recording.
  SC12)
    snippet OLD <<'EOF'
            await recordSemanticScanClaim(
                gate: .runnerFactoryMissing,
                chunks: chunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
            return .skipped
EOF
    snippet NEW <<'EOF'
            return .skipped
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC14 — the sweep runs a step LATE: after `mintAdScanRedrives` instead of
  # before it. The claim is still written, still correct and still counted; it
  # simply cannot be acted on until the next launch.
  SC14)
    snippet OLD <<'EOF'
        let stepScanClaims = await mintSemanticScanClaims()
        // playhead-onn6: AFTER the backfill reaper (rows it just rescued from
        // `running` are now resumable and must be counted) and after step 7 (an
        // episode receiving a fresh job is already excluded as active).
        let stepAdScanRedrive = await mintAdScanRedrives()
EOF
    snippet NEW <<'EOF'
        let stepAdScanRedrive = await mintAdScanRedrives()
        let stepScanClaims = await mintSemanticScanClaims()
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC24 — the mode-off gate in `runBackfill` stops recording. `_ =
  # canonicalChunks` keeps the branch non-empty so the edit is a deleted
  # RECORDING rather than a deleted branch.
  SC24)
    snippet OLD <<'EOF'
        if effectiveFMBackfillMode == .off {
            await recordSemanticScanClaim(
                gate: .fmModeOff,
                chunks: canonicalChunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
        } else if podcastId.isEmpty {
EOF
    snippet NEW <<'EOF'
        if effectiveFMBackfillMode == .off {
            _ = canonicalChunks
        } else if podcastId.isEmpty {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC23 — the mode-off guard INSIDE `runShadowFMPhase` stops recording. This is
  # the pre-fil5 line, restored verbatim.
  SC23)
    snippet OLD <<'EOF'
        guard resolvedMode != .off else {
            await recordSemanticScanClaim(
                gate: .fmModeOff,
                chunks: chunks,
                analysisAssetId: analysisAssetId,
                podcastId: podcastId
            )
            return .skipped
        }
EOF
    snippet NEW <<'EOF'
        guard resolvedMode != .off else { return .skipped }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC25 — the sweep's transcript gate divides the WATERMARK rather than the
  # interval union. Identical on a contiguous transcript; 100 % on a gappy one.
  SC25)
    snippet OLD <<'EOF'
                coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(
                    ranges: try await store.fetchTranscriptCoveredRanges(assetId: assetId)
                ),
EOF
    snippet NEW <<'EOF'
                coveredSec: asset.fastTranscriptCoverageEndTime,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC30 (R3) — THE defect this round found, restored in one edit: the sweep's
  # numerator goes back to the RAW chunk union. Nothing breaks, no gate widens,
  # every contiguous-chunk fixture in the suite still passes — and the sweep
  # mints NOTHING in the field, because a `transcript_chunks` row spans
  # first-word to last-word and a complete transcript's raw union is only its
  # speech density. Measured on the 2026-08-03 pull: 0 of 12 assets over 0.95,
  # maximum 93.8 %, with the two assets the bead is named after at 87.3 % and
  # 89.2 %. Killed only by a fixture with real inter-utterance gaps.
  SC30)
    snippet OLD <<'EOF'
                coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(
                    ranges: try await store.fetchTranscriptCoveredRanges(assetId: assetId)
                ),
EOF
    snippet NEW <<'EOF'
                coveredSec: AnalysisCoverageMath.unionedSeconds(
                    try await store.fetchTranscriptCoveredRanges(assetId: assetId)
                ),
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC31 (R3) — the same defect one layer down, where it looks like a constant
  # rather than a choice: the bridge width goes to zero, so
  # `bridgedTranscriptCoveredSec` silently becomes the raw union for every
  # caller. Distinct from SC30 because it survives SC30's rail being deleted and
  # vice versa — one is the call site, one is the helper's contract.
  SC31)
    snippet OLD <<'EOF'
                upTo: AnalysisCoverageMath.adScanBridgeableGapSec
EOF
    snippet NEW <<'EOF'
                upTo: 0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC32 (R3) — the candidate cursor stops advancing, i.e. the window becomes a
  # fixed oldest-first prefix again. A reject writes nothing and so stays a
  # candidate in the same position forever, which makes a full window of
  # never-claimable assets a permanent wall in front of everything behind it.
  # Invisible to every bound test: SC26's mint cap and SC27's window both still
  # hold, and the sweep still mints for any library that fits in one window.
  SC32)
    snippet OLD <<'EOF'
        semanticScanClaimSweepOffset =
            candidates.count < window ? 0 : semanticScanClaimSweepOffset + window
EOF
    snippet NEW <<'EOF'
        semanticScanClaimSweepOffset = 0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC34 (R4) — the cursor stops WRAPPING. `> 0` becomes `< 0`, so the recovery
  # block is dead while everything around it is untouched: an empty read at a
  # non-zero offset now returns nothing and skips the pass instead of restarting
  # at the oldest candidate. Deliberately a dead CONDITION rather than a deleted
  # block, so `candidates` stays a legitimately-mutated `var` and the mutant is
  # a behaviour change rather than a compiler warning.
  #
  # Distinct from SC32 (which pins the ADVANCE) because the two are reached by
  # different states: SC32's fixture keeps a claimable asset waiting at the
  # cursor, so the read is never empty; this one has the mint cap leave the
  # cursor past a population that shrank under it.
  SC34)
    snippet OLD <<'EOF'
        if candidates.isEmpty, semanticScanClaimSweepOffset > 0 {
EOF
    snippet NEW <<'EOF'
        if candidates.isEmpty, semanticScanClaimSweepOffset < 0 {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC33 (R3) — bridging widens until it conceals real holes. The direction
  # matters as much as the mechanism: a bridge wide enough to close an
  # untranscribed BLOCK claims assets whose transcript a scan cannot read, which
  # is the failure SC30 causes inverted. 5 s is defensible only because it is
  # shorter than the shortest span any lane calls an ad; 500 s is not.
  SC33)
    snippet OLD <<'EOF'
                ranges,
                upTo: AnalysisCoverageMath.adScanBridgeableGapSec
EOF
    snippet NEW <<'EOF'
                ranges,
                upTo: 500
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-9y9e: transcribed -> read for ads (RT series) ----

  # RT01 — the short-circuit goes. The condition can never hold, so a pass over
  # a finished transcript falls through to the zero-coverage failure arm exactly
  # as it did before this bead and Stage 4 is never reached. Deleting the block
  # outright would also delete the `else`, so the mutation makes the guard
  # unsatisfiable instead: same behaviour, one-line anchor.
  RT01)
    snippet OLD <<'EOF'
            if transcriptFailure?.termination != .interrupted,
               let alreadyTranscribed = await transcriptCoverageOfCompletedTranscript(
EOF
    snippet NEW <<'EOF'
            if transcriptFailure?.termination == .interrupted,
               let alreadyTranscribed = await transcriptCoverageOfCompletedTranscript(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT02 — VACUITY AUDIT. The helper stops measuring: any positive watermark
  # licenses the short-circuit, with no chunks required and no floor. Every
  # NEGATIVE fixture in the runner suite must go red; one staying green means it
  # never reached the gate it claims to test.
  RT02)
    snippet OLD <<'EOF'
        guard let ranges = try? await store.fetchTranscriptCoveredRanges(assetId: assetId),
              !ranges.isEmpty else {
            return nil
        }
        guard SemanticScanClaim.transcriptClearsFinalizeFloor(
            coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(ranges: ranges),
            episodeDurationSec: asset.episodeDurationSec
        ) else {
            return nil
        }
        return watermark
EOF
    snippet NEW <<'EOF'
        return watermark
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT03 — the gate divides the WATERMARK instead of the gap-bridged AREA. The
  # two agree on a contiguous transcript and diverge on a gappy one, where the
  # watermark reads 100 % over audio nobody transcribed.
  RT03)
    snippet OLD <<'EOF'
            coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(ranges: ranges),
EOF
    snippet NEW <<'EOF'
            coveredSec: watermark,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT04 — the interruption carve-out goes, so a listener's scrub over a
  # finished transcript buys a whole detection pass on the scheduler's only
  # running slot.
  RT04)
    snippet OLD <<'EOF'
            if transcriptFailure?.termination != .interrupted,
               let alreadyTranscribed = await transcriptCoverageOfCompletedTranscript(
EOF
    snippet NEW <<'EOF'
            if true,
               let alreadyTranscribed = await transcriptCoverageOfCompletedTranscript(
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT05 — the trace reverts to a failure row. Behaviour is untouched (the pass
  # still reaches Stage 4); only the account of it is wrong, which is the whole
  # misattribution — five `asr_failed` rows for a fully transcribed episode.
  RT05)
    snippet OLD <<'EOF'
            eventType: .checkpointed,
            cause: nil,
EOF
    snippet NEW <<'EOF'
            eventType: .failed,
            cause: .asrFailed,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT06 — the ad-scan area is bounded by the FAST union again. The final pass
  # is still queried, so nothing breaks and every display field is unchanged;
  # only the ruler shrinks.
  RT06)
    snippet OLD <<'EOF'
                    within: AnalysisCoverageMath.bridgingShortGaps(
                        transcribedIntervals,
EOF
    snippet NEW <<'EOF'
                    within: AnalysisCoverageMath.bridgingShortGaps(
                        transcriptIntervals,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT07 — the bound stops clipping. A window's persisted bounds are
  # `first.startTime ... last.endTime` over the segments that fit one prompt, so
  # taking them at face value lets one prompt claim every untranscribed block it
  # straddles. This is a revert of playhead-pz32 and must be caught by pz32's
  # own rail as well as this bead's.
  RT07)
    snippet OLD <<'EOF'
                adScanCoveredSec = AnalysisCoverageMath.unionedSecondsIntersecting(
                    adScanIntervals[id] ?? [],
                    within: AnalysisCoverageMath.bridgingShortGaps(
                        transcribedIntervals,
                        upTo: AnalysisCoverageMath.adScanBridgeableGapSec
                    )
                )
EOF
    snippet NEW <<'EOF'
                adScanCoveredSec = AnalysisCoverageMath.unionedSeconds(
                    adScanIntervals[id] ?? []
                )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT08 — the standalone read becomes its fast-only sibling, so every caller
  # asking "what can a scan read?" is answered "what has the fast pass
  # reached?". Distinct from RT06: that is the summary's internal bound, this is
  # the query fil5's sweep and the runner's own gate both call.
  RT08)
    snippet OLD <<'EOF'
            SELECT startTime, endTime FROM transcript_chunks
            WHERE analysisAssetId = ?
              AND endTime > startTime
            ORDER BY startTime
EOF
    snippet NEW <<'EOF'
            SELECT startTime, endTime FROM transcript_chunks
            WHERE analysisAssetId = ?
              AND pass = 'fast'
              AND endTime > startTime
            ORDER BY startTime
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT09 — the cap-out rescue reverts to a transcript-only measure of
  # outstanding work, which a fully transcribed and never-scanned episode can
  # never satisfy.
  RT09)
    snippet OLD <<'EOF'
        } else if SemanticScanClaim.isOwed(adScanFraction: adScanFraction), let deepest = ladder.last {
EOF
    snippet NEW <<'EOF'
        } else if false, let deepest = ladder.last {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT10 — the ad-scan term stops being a FALLBACK and becomes an override, so a
  # half-transcribed episode is asked for the whole thing and the ladder's
  # cheapest-probe-first ordering is gone.
  # The whole chain is the anchor, not its head: prepending a second
  # `isOwed(…)` arm to the existing chain is a `duplicate_conditions` lint
  # ERROR, and a mutation that cannot compile is not a kill.
  RT10)
    snippet OLD <<'EOF'
        let target: Double
        if let outstanding = outstandingTranscriptTarget(
            transcriptCoverageSec: transcriptCoverageSec,
            tiers: tiers,
            episodeDurationSec: episodeDurationSec
        ) {
            target = outstanding
        } else if SemanticScanClaim.isOwed(adScanFraction: adScanFraction), let deepest = ladder.last {
EOF
    snippet NEW <<'EOF'
        let target: Double
        if SemanticScanClaim.isOwed(adScanFraction: adScanFraction), let deepest = ladder.last {
            target = deepest
        } else if let outstanding = outstandingTranscriptTarget(
            transcriptCoverageSec: transcriptCoverageSec,
            tiers: tiers,
            episodeDurationSec: episodeDurationSec
        ), let deepest = Optional(outstanding) {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT11 — the ad-scan term jumps the terminal, budget and cooldown guards,
  # turning a bounded rescue into an unbounded retry wearing a floor.
  RT11)
    snippet OLD <<'EOF'
        guard isAttemptCapTerminal(chainTail) else { return .declined(.notACapOutTerminal) }
        guard let nextOrdinal else { return .declined(.budgetSpent) }
        guard capOutRetryCooldownElapsed(chainTail: chainTail, now: now) else {
            return .declined(.cooling)
        }
        let ladder = coverageTierLadder(tiers: tiers, episodeDurationSec: episodeDurationSec)
EOF
    snippet NEW <<'EOF'
        let ladder = coverageTierLadder(tiers: tiers, episodeDurationSec: episodeDurationSec)
        if SemanticScanClaim.isOwed(adScanFraction: adScanFraction), let deepest = ladder.last {
            return .mint(CapOutRetryPlan(
                workKey: capOutRetryWorkKey(baseWorkKey: baseWorkKey, ordinal: nextOrdinal ?? 1),
                ordinal: nextOrdinal ?? 1,
                desiredCoverageSec: deepest
            ))
        }
        guard isAttemptCapTerminal(chainTail) else { return .declined(.notACapOutTerminal) }
        guard let nextOrdinal else { return .declined(.budgetSpent) }
        guard capOutRetryCooldownElapsed(chainTail: chainTail, now: now) else {
            return .declined(.cooling)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT12 (R1 review) — the widened bound is rebuilt from the two CHUNK sets,
  # dropping the `transcriptIntervals` term that carries the watermark fallback.
  # On an asset whose watermark outlives its chunks the measured ad-scan area
  # goes DOWN, which is the exact claim the surrounding comment makes impossible.
  #
  # THE NEW TEXT IS THE PRE-REVIEW IMPLEMENTATION VERBATIM, branch and all, and
  # that is the whole point. The obvious one-line mutant — an UNCONDITIONAL
  # `fastIntervals + finalIntervals` — also kills, but it kills off twenty
  # unrelated victims, because it deletes the watermark fallback for assets with
  # no chunks of EITHER pass, a shape half the coverage suite is built on. A
  # mutant that the buggy code would ALSO have failed proves nothing about the
  # fix; this one is green under the unconditional revert and red only on the
  # asset the branch actually mishandled (watermark + final chunks + no fast).
  RT12)
    snippet OLD <<'EOF'
            let transcribedIntervals = transcriptIntervals + (finalIntervals[id] ?? [])
EOF
    snippet NEW <<'EOF'
            let transcribedIntervals: [(start: Double, end: Double)]
            if chunkMaxEnd != nil || finalMaxEnd[id] != nil {
                transcribedIntervals = (fastIntervals[id] ?? []) + (finalIntervals[id] ?? [])
            } else {
                transcribedIntervals = transcriptIntervals
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT13 (R1 review) — the final-pass loop keeps degenerate rows, so a zero-width
  # chunk supplies `MAX(endTime)` and an all-degenerate final pass reports a
  # reach it never had. The FAST loop's identical guard is left alone, which is
  # what makes this a rail on the two staying symmetric.
  RT13)
    snippet OLD <<'EOF'
                let endTime = sqlite3_column_double(finalStmt, 2)
                guard endTime > startTime else { continue }
EOF
    snippet NEW <<'EOF'
                let endTime = sqlite3_column_double(finalStmt, 2)
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT14 (R2 review) — the duration-sanity guard reverts to the FAST reach while
  # `adScanCoveredSec` keeps its widened both-pass bound. The two stop describing
  # the same region, so a final-pass transcript running far past a wrong declared
  # duration draws a proportionate-LOOKING numerator through a guard that cannot
  # see it, and the fraction reads 1.0 instead of `nil`.
  RT14)
    snippet OLD <<'EOF'
        let transcriptReach = [fastTranscriptCoverageEndSec, finalPassCoverageEndSec]
            .compactMap { $0 }
            .filter { $0.isFinite }
            .max()
        if let transcriptReach, transcriptReach > episodeDurationSec + tolerance {
            return nil
        }
EOF
    snippet NEW <<'EOF'
        if let reach = fastTranscriptCoverageEndSec, reach.isFinite,
           reach > episodeDurationSec + tolerance {
            return nil
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT15 (R3 review) — the reconciler stops measuring and asserts "fully scanned".
  # The read is the only thing that carries the field's ad-scan coverage into
  # `capOutRetryDecision`; a constant `1.0` makes `SemanticScanClaim.isOwed`
  # false forever, so the arm this bead added is unreachable in production while
  # every pure decision test still passes.
  RT15)
    snippet OLD <<'EOF'
        var adScanFraction: Double?
        if nextOrdinal != nil,
           AnalysisWorkScheduler.capOutRetryCooldownElapsed(chainTail: tail, now: decidedAt),
           let assetId = tail.analysisAssetId {
            adScanFraction = try await store
                .fetchCoverageSummariesByAssetIds([assetId])[assetId]?
                .adScanFraction
        }
EOF
    snippet NEW <<'EOF'
        let adScanFraction: Double? = 1.0
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT16 (R3 review) — the duration guard admits the final WATERMARK as well as
  # final CHUNKS. `finalPassCoverageEndSec` falls back to the
  # `analysis_assets.finalPassCoverageEndTime` column when no final chunk is on
  # disk (playhead-0sro's shape), so a stale column withholds a fraction that is
  # fine and the episode goes BACKWARDS in readiness.
  RT16)
    snippet OLD <<'EOF'
        let finalReach = finalPassCoverageEndSource == .finalPassChunks
            ? finalPassCoverageEndSec
            : nil
        let transcriptReach = [fastTranscriptCoverageEndSec, finalReach]
EOF
    snippet NEW <<'EOF'
        let finalReach = finalPassCoverageEndSec
        let transcriptReach = [fastTranscriptCoverageEndSec, finalReach]
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # RT17 (R3 review) — the reconciler skips the coverage read entirely, so
  # `adScanFraction` is always `nil`. `SemanticScanClaim.isOwed(nil)` is TRUE by
  # design, so the rescue mints on an episode whose scan is finished, on every
  # sweep, until the ordinal budget is gone. The mirror of RT15: a constant 1.0
  # never mints, a constant nil always does, and only one test sees each.
  RT17)
    snippet OLD <<'EOF'
        var adScanFraction: Double?
        if nextOrdinal != nil,
           AnalysisWorkScheduler.capOutRetryCooldownElapsed(chainTail: tail, now: decidedAt),
           let assetId = tail.analysisAssetId {
            adScanFraction = try await store
                .fetchCoverageSummariesByAssetIds([assetId])[assetId]?
                .adScanFraction
        }
EOF
    snippet NEW <<'EOF'
        let adScanFraction: Double? = nil
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-41mu, the UC series --------------------------------------

  # UC01 — THE WIRE-IN. The actor-side decision stops asking the store and
  # answers `.complete` unconditionally, which is the pre-bead terminal
  # VERBATIM. Every pure test in the suite still passes; only the end-to-end
  # tests can see it, which is exactly the point of registering it.
  UC01)
    snippet OLD <<'EOF'
        guard job.phase == .fullEpisodeScan else { return .complete }
        return Self.coverageTerminalDecision(
            phase: job.phase,
            measurement: await measuredAdScanFraction(assetId: job.analysisAssetId),
            retryCount: job.retryCount
        )
EOF
    snippet NEW <<'EOF'
        return .complete
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UC02 — the phase scope is dropped, so `scanHarvesterProposals`,
  # `scanLikelyAdSlots` and `scanRandomAuditWindows` are judged against a floor
  # that describes the whole episode and defer forever.
  UC02)
    snippet OLD <<'EOF'
        guard phase == .fullEpisodeScan else { return .complete }
        let underCovered: Bool
EOF
    snippet NEW <<'EOF'
        let underCovered: Bool
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UC03 — a store read that THREW is treated as evidence the episode was read.
  UC03)
    snippet OLD <<'EOF'
        case .unreadable:
            underCovered = true
        }
EOF
    snippet NEW <<'EOF'
        case .unreadable:
            underCovered = false
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UC04 — the budget never terminates, so an episode whose transcript cannot
  # support the floor re-drives until something else stops it.
  UC04)
    snippet OLD <<'EOF'
        return retryCount + 1 >= AdmissionController.maxRetries
            ? .failUnderCoverage
            : .deferUnderCoverage
EOF
    snippet NEW <<'EOF'
        _ = retryCount
        return .deferUnderCoverage
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UC05 — the head-hole rule is dropped and the walk's contiguous bound is
  # published as an episode prefix regardless of where the run's plans began.
  # This is 53FC53E3's device row reproduced exactly.
  UC05)
    snippet OLD <<'EOF'
        let priorUpper = prior?.lastProcessedUpperBoundSec ?? 0
        if let firstPlannedSegmentStartSec, firstPlannedSegmentStartSec.isFinite,
           firstPlannedSegmentStartSec - priorUpper > AnalysisCoverageMath.adScanBridgeableGapSec {
            return prior
        }
EOF
    snippet NEW <<'EOF'
        _ = firstPlannedSegmentStartSec
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UC06 — the OTHER direction: the hole test drops the coverage reader's bridge
  # tolerance, so AD5F3A0A's 2.8 s of leading silence reads as unscanned audio
  # and freezes a cursor that is a genuine prefix.
  UC06)
    snippet OLD <<'EOF'
           firstPlannedSegmentStartSec - priorUpper > AnalysisCoverageMath.adScanBridgeableGapSec {
EOF
    snippet NEW <<'EOF'
           firstPlannedSegmentStartSec - priorUpper > 0 {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UC07 — THE VACUITY CONTROL. The early return for a sufficiently-scanned
  # episode is removed, so the terminal refuses everything.
  UC07)
    snippet OLD <<'EOF'
        guard underCovered else { return .complete }
        return retryCount + 1 >= AdmissionController.maxRetries
EOF
    snippet NEW <<'EOF'
        _ = underCovered
        return retryCount + 1 >= AdmissionController.maxRetries
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UC08 (R1 review) — the non-finite arm returns `.complete` again. This is the
  # pre-review implementation verbatim, so the mutant is exactly the size of the
  # fix: the finite comparison below is untouched.
  UC08)
    snippet OLD <<'EOF'
            underCovered = !fraction.isFinite
                || fraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
EOF
    snippet NEW <<'EOF'
            guard fraction.isFinite else { return .complete }
            underCovered = fraction < AnalysisJobRunner.semanticBackfillSufficientAdScanFraction
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # UC09 (R2 review) — the head test is fed the pre-`narrowedForResume` segment
  # list again. Cut at the PRODUCER because that is where one identifier does it
  # (`rootInputs` is the caller's `jobInputs`, verbatim); cutting at the consumer
  # would have to re-add a parameter and would be broader than the fix. The pure
  # rule is untouched, `CoverageOutcome` still exists, and every first-attempt
  # fixture still passes — on a first attempt the two lists ARE the same value,
  # which is precisely why this survived eight mutants and two review rounds.
  # Only a RESUME can see it.
  UC09)
    snippet OLD <<'EOF'
            firstPlannedSegmentStartSec: inputs.segments.first?.startTime
EOF
    snippet NEW <<'EOF'
            firstPlannedSegmentStartSec: rootInputs.segments.first?.startTime
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-iu0t: the shadow-retry drain's replay set (CN series) ----
  #
  # All four cut the same statement in `retryShadowFMPhaseForSession`, so the
  # anchor is shared and each needs its own batch. The anchor is the STATEMENT,
  # not the surrounding comment: the comment above it is 30 lines of field
  # evidence that no mutation should have to carry, and including it would make
  # every one of these anchors drift the first time a word of it is edited.

  # CN01 — THE SHIPPED DEFECT, restored exactly as it stood on `main` at
  # d257a060, `finalChunks` binding and all. Anything less literal (say, just
  # `chunks.filter { ... }`) would be a different defect that happens to be
  # nearby; this one is the row on the device.
  CN01)
    snippet OLD <<'EOF'
        let chunksForReplay = TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
EOF
    snippet NEW <<'EOF'
        let finalChunks = chunks.filter { $0.pass == TranscriptPassType.final_.rawValue }
        let chunksForReplay = finalChunks.isEmpty ? chunks : finalChunks
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN02 — the over-correction: keep the fast pass, drop the final one. Reach is
  # correct, so only the identity rails can see it.
  CN02)
    snippet OLD <<'EOF'
        let chunksForReplay = TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
EOF
    snippet NEW <<'EOF'
        let chunksForReplay = chunks.filter { $0.pass != TranscriptPassType.final_.rawValue }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN03 — no canonicalization at all. The narrowest possible cut: the call is
  # removed and nothing else moves.
  CN03)
    snippet OLD <<'EOF'
        let chunksForReplay = TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
EOF
    snippet NEW <<'EOF'
        let chunksForReplay = chunks
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN04 — VACUITY CONTROL. Nothing is replayed, so the empty guard below bails
  # and the drain never runs. Every behavioural rail must die; the pure fixture
  # control must not.
  CN04)
    snippet OLD <<'EOF'
        let chunksForReplay = TranscriptChunkCanonicalizer.canonicalize(chunks).chunks
EOF
    snippet NEW <<'EOF'
        let chunksForReplay: [TranscriptChunk] = []
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN06 — the fourth instance, restored exactly as it stood in
  # `AnalysisCoordinator.pushEvidenceCatalog` before playhead-iu0t R1,
  # `preferred` binding and all. Restoring the BINDING rather than inlining a
  # closure into the argument matters twice: it is what was actually on disk, and
  # it exercises the canary's local-binding resolution rather than only its
  # inline-expression path — a mutant that only ever appeared inline would leave
  # the two-hop resolver unproven.
  #
  # The anchor is the two-line atomize call, not the 20-line comment above it:
  # the comment is the measurement of what this cost, and no mutation should
  # have to carry it.
  CN06)
    snippet OLD <<'EOF'
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks,
EOF
    snippet NEW <<'EOF'
        let preferred: [TranscriptChunk] = {
            let finals = chunks.filter { $0.pass == "final" }
            return finals.isEmpty ? chunks : finals
        }()
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: preferred,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN05 — the THIRD instance, restored exactly as it stood before playhead-iu0t
  # in `runPhase5ProjectorPhase`, ternary and all. The victim is the rule canary,
  # not a behavioural rail: nothing in production calls this method. It is
  # encodable at all only because the allow-list is keyed per DECLARATION —
  # under the file-scoped key this mutant survived. See the batch-444 note.
  CN05)
    snippet OLD <<'EOF'
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks,
EOF
    snippet NEW <<'EOF'
        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks.filter { $0.pass == "final" }.isEmpty ? chunks : chunks.filter { $0.pass == "final" },
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN07 — the FIFTH instance, restored exactly as it stood before
  # playhead-iu0t R2: the episode-summary invalidation key read out of the
  # persisted `transcript_chunks.transcriptVersion` column, which is populated
  # `WHERE pass != 'fast'` and therefore holds a final-only version.
  CN07)
    snippet OLD <<'EOF'
        let transcriptVersion = SemanticScanClaim.transcriptVersion(forPersistedChunks: chunks)
EOF
    snippet NEW <<'EOF'
        let transcriptVersion = chunks
            .compactMap(\.transcriptVersion)
            .last
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN08 — CN07's defect, respelled with a closure instead of a keypath. The
  # SAME read of the SAME column at the SAME line; only the syntax differs.
  # It survived until playhead-iu0t R3, because the rule was enforced by the
  # literal substring `\.transcriptVersion` rather than by a pattern that knows
  # what a read of the column looks like.
  CN08)
    snippet OLD <<'EOF'
        let transcriptVersion = SemanticScanClaim.transcriptVersion(forPersistedChunks: chunks)
EOF
    snippet NEW <<'EOF'
        let transcriptVersion = chunks
            .compactMap { $0.transcriptVersion }
            .last
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN09 — CN06's defect, respelled with a named closure parameter. The SAME
  # collapse at the SAME line; only the closure's binding differs. It escaped
  # the collapse-shape rule until playhead-iu0t R4, because that rule's first
  # half was two literal substrings rather than a pattern.
  CN09)
    snippet OLD <<'EOF'
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks,
EOF
    snippet NEW <<'EOF'
        let preferred: [TranscriptChunk] = {
            let finals = chunks.filter { chunk in chunk.pass == "final" }
            return finals.isEmpty ? chunks : finals
        }()
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: preferred,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN10 — playhead-iu0t R5. The NARROWEST possible cut for the site-finder
  # defect: the argument becomes the raw `chunks` AND the call is split across
  # two lines. No collapse, no ternary, no closure — the whole point is that
  # the line break alone is what used to make it invisible, so anything more
  # elaborate would be a different mutant that happens to also be split.
  # The anchor is the whole call rather than its first two lines, unlike CN06 /
  # CN09 / CN11 / CN12: splitting the receiver from the member re-indents every
  # argument, and leaving the tail at the old indent would make the mutant's
  # own formatting, rather than its semantics, the thing a reader notices.
  CN10)
    snippet OLD <<'EOF'
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks,
            analysisAssetId: assetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )
EOF
    snippet NEW <<'EOF'
        let (atoms, version) = TranscriptAtomizer
            .atomize(
                chunks: chunks,
                analysisAssetId: assetId,
                normalizationHash: "norm-v1",
                sourceHash: "asr-v1"
            )
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN11 — playhead-iu0t R5. CN06's collapse spelled as a negation against the
  # FAST pass instead of an equality against the FINAL one. Identical semantics,
  # identical line; only the operator and the literal differ. This is instance
  # #5's spelling, lifted out of SQL into Swift.
  CN11)
    snippet OLD <<'EOF'
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks,
EOF
    snippet NEW <<'EOF'
        let preferred: [TranscriptChunk] = {
            let finals = chunks.filter { $0.pass != "fast" }
            return finals.isEmpty ? chunks : finals
        }()
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: preferred,
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # CN12 — playhead-iu0t R5. The site canonicalizes and then collapses the
  # result anyway. The `canonicalize(` call is KEPT deliberately: a mutant that
  # removed it would be CN03's shape and would prove nothing about laundering.
  # What is added is the `.filter`, and it is the whole mutation.
  CN12)
    snippet OLD <<'EOF'
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks,
EOF
    snippet NEW <<'EOF'
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: TranscriptChunkCanonicalizer.canonicalize(chunks).chunks.filter { $0.pass == "final" },
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC15 — the selector drops the transcript requirement.
  SC15)
    snippet OLD <<'EOF'
              AND EXISTS (
                    SELECT 1 FROM transcript_chunks c WHERE c.analysisAssetId = a.id
                  )
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC16 — a COMPLETE coverage lane reads as missing.
  SC16)
    snippet OLD <<'EOF'
                    SELECT 1 FROM backfill_jobs b WHERE b.analysisAssetId = a.id
EOF
    snippet NEW <<'EOF'
                    SELECT 1 FROM backfill_jobs b WHERE b.analysisAssetId = a.id AND b.status <> 'complete'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC17 — tiebreak on `id`, a UUID, over a `createdAt` of whole seconds.
  SC17)
    snippet OLD <<'EOF'
            ORDER BY a.createdAt ASC, a.rowid ASC
EOF
    snippet NEW <<'EOF'
            ORDER BY a.createdAt ASC, a.id ASC
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC18 — stop asking whether a scan is owed.
  SC18)
    snippet OLD <<'EOF'
            guard isOwed(adScanFraction: fraction) else {
EOF
    snippet NEW <<'EOF'
            guard isOwed(adScanFraction: fraction) || fraction != nil else {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC19 — a `complete` row is treated as claimable.
  SC19)
    snippet OLD <<'EOF'
                case .complete:
                    return .alreadySatisfied
EOF
    snippet NEW <<'EOF'
                case .complete:
                    break
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC20 — a `running` row is rewritten under the pass that owns it.
  SC20)
    snippet OLD <<'EOF'
                case .running, .failed:
EOF
    snippet NEW <<'EOF'
                case .failed:
EOF
    patch "$file" "$OLD" "$NEW"
    snippet OLD <<'EOF'
                case .queued, .deferred:
                    break
EOF
    snippet NEW <<'EOF'
                case .queued, .deferred, .running:
                    break
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC21 — the sweep's transcript-floor guard goes.
  SC21)
    snippet OLD <<'EOF'
            guard SemanticScanClaim.transcriptClearsFinalizeFloor(
                coveredSec: SemanticScanClaim.bridgedTranscriptCoveredSec(
                    ranges: try await store.fetchTranscriptCoveredRanges(assetId: assetId)
                ),
                episodeDurationSec: asset.episodeDurationSec
            ) else { continue }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC22 — claims counted as recovered work, double-reporting one repair.
  SC22)
    snippet OLD <<'EOF'
            + adScanRedrivesMinted
            + capOutRetriesMinted
    }
EOF
    snippet NEW <<'EOF'
            + adScanRedrivesMinted
            + capOutRetriesMinted
            + semanticScanClaimsMinted
    }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC26 — the sweep's per-pass MINT cap goes. Every candidate that clears the
  # floors is claimed in one pass, into a queue the very next step drains 8 of.
  SC26)
    snippet OLD <<'EOF'
            guard minted < Self.maxSemanticScanClaimsPerReconcile else { break }
EOF
    snippet NEW <<'EOF'
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC27 — the CANDIDATE window widens to the re-drive sweep's 64. A different
  # bound from the mint cap, and invisible through it: the mint cap stops the
  # loop at 8 MINTS, so the window only bites once the assets ahead are rejects.
  SC27)
    snippet OLD <<'EOF'
        let window = Self.maxSemanticScanClaimCandidatesPerReconcile
EOF
    snippet NEW <<'EOF'
        let window = Self.maxAdScanRedriveCandidatesPerReconcile
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # SC28 — the sweep stops asking whether a pass is already in flight, so every
  # episode being analysed right now is claimed the moment its transcript
  # crosses the floor and before step 6 of its own pass has run.
  SC28)
    snippet OLD <<'EOF'
            guard !episodesWithPendingWork.contains(asset.episodeId) else { continue }
EOF
    snippet NEW <<'EOF'
            _ = episodesWithPendingWork
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # ---- playhead-e75l: daemon-refusal disposition (DR series) ----

  # DR01 — THE mutant: the shipped pre-e75l line, verbatim. Only a throttle is
  # a daemon condition, so a tokenizer round trip that outlived the 30 s
  # metadata bound falls to the genuine-failure disposition and spends one of
  # three LIFETIME retries. Three of those and the episode is never scanned.
  DR01)
    snippet OLD <<'EOF'
                if let refusal = FMDaemonRefusal.classify(error) {
EOF
    snippet NEW <<'EOF'
                if let refusal: FMDaemonRefusal = FMDaemonThrottle.isThrottle(error) ? .throttle : nil {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR02 — the discriminator keys on the TYPE rather than on which BUDGET
  # elapsed. The 300 s inference timeout is the same Swift type, so it becomes
  # a daemon refusal too: an unbounded retry at 300 s per window per drain, and
  # evidence about the MODEL read as evidence about the daemon.
  DR02)
    snippet OLD <<'EOF'
        return timeout.deadline == FMInferenceDeadline.metadata
EOF
    snippet NEW <<'EOF'
        return true
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR03 — the same predicate reading the OTHER budget. Two Durations, one
  # comparison, and nothing in the type system distinguishes them: this is the
  # substitution the whole `deadline ==` form is exposed to.
  DR03)
    snippet OLD <<'EOF'
        return timeout.deadline == FMInferenceDeadline.metadata
EOF
    snippet NEW <<'EOF'
        return timeout.deadline == FMInferenceDeadline.standard
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR04 — the swept siblings carry kvs8's literal again. Nothing about those
  # jobs was rate-limited and nothing about the drain that stopped was either;
  # an operator counting `rateLimited-` gets an event the daemon never
  # described that way.
  DR04)
    snippet OLD <<'EOF'
                        reason: refusalThatStoppedUs.batchSiblingCause
EOF
    snippet NEW <<'EOF'
                        reason: FMDaemonThrottle.DeferCause.batchSibling.rawValue
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR05 — the same substitution one level in: the REFUSED job's own cause.
  DR05)
    snippet OLD <<'EOF'
                            reason: refusal.passPrologueCause
EOF
    snippet NEW <<'EOF'
                            reason: FMDaemonThrottle.DeferCause.passPrologue.rawValue
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR06 — the drain-stop log EVENT reverts to the hard-coded name. This is the
  # defect R1 found: the durable tokens were split per kind but the event name
  # was not, so a drain stopped by two wedged tokenizer round trips emitted
  # `..._by_throttle` verbatim. No runtime test can see a log line, which is
  # why the killer is a source canary.
  DR06)
    snippet OLD <<'EOF'
                \(refusalThatStoppedUs.drainStoppedEvent, privacy: .public) \
EOF
    snippet NEW <<'EOF'
                fm.backfill.drain_stopped_by_throttle \
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR07 — two counters instead of one. Only throttles advance the consecutive
  # count, so a daemon refusing one job by rate limit and the next by a wedged
  # tokenizer never reaches the stop threshold and the drain keeps asking.
  DR07)
    snippet OLD <<'EOF'
                    consecutiveDaemonRefusals += 1
EOF
    snippet NEW <<'EOF'
                    if refusal == .throttle { consecutiveDaemonRefusals += 1 }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR08 — R2 review. DR06's twin ONE LINE UP: the PER-JOB event as a literal.
  # R1's canary forbade `fm.backfill.job_throttled` and
  # `fm.backfill.drain_stopped_by` BY NAME, so a literal of the event this bead
  # itself added was invisible to it — every throttle would log the stall's
  # event name and the rate-limit count in a support bundle would read zero.
  # This SURVIVED on first plant; the canary now derives its forbidden set from
  # `FMDaemonRefusal.allCases`.
  DR08)
    snippet OLD <<'EOF'
                        \(refusal.logEvent, privacy: .public) job=\(job.jobId, privacy: .public) \
EOF
    snippet NEW <<'EOF'
                        fm.backfill.job_daemon_metadata_stalled job=\(job.jobId, privacy: .public) \
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR09 — R2 review, and the pre-review spelling VERBATIM. The two new durable
  # tokens were `inferenceTimeout-metadata` / `inferenceTimeout-batchSibling`,
  # which put a wedged TOKENIZER round trip into the same greppable family as
  # playhead-8d5r's `inferenceTimeout-noProgress` — a run of 300 s inference
  # timeouts, whose documented operator reading is "the model is not answering
  # on this device". That is the exact substitution `FMDaemonRefusal`'s own doc
  # forbids for `rateLimited-`, one family over.
  DR09)
    snippet OLD <<'EOF'
            "metadataStall-refused"
EOF
    snippet NEW <<'EOF'
            "inferenceTimeout-metadata"
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR10 — R3 review, and the reason a blacklist is not a rail. R2's foreign set
  # was `["inferenceTimeout-noProgress", "cancelled-during-"]`: the two families
  # it had just looked at. `BackfillJobRunner` writes SIX, and this plants the
  # token in one of the four it forgot — playhead-41mu's `underCoverage-`, the
  # cause a coverage-lane job records when it declines to certify its own
  # completion. An operator counting that family to find jobs that stopped
  # converging would be counting wedged tokenizer round trips instead. Under
  # R2's list this SURVIVES by arithmetic (`underCoverage-` is not one of the two
  # named); the canary now derives the families from the runner's own source.
  DR10)
    snippet OLD <<'EOF'
            "metadataStall-refused"
EOF
    snippet NEW <<'EOF'
            "underCoverage-refused"
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR11 — R4 review. THE SWAP, and it uses no literal at all. Two patches in
  # one mutation because the defect is the PAIR: swapping only one side leaves
  # a property unread, which R2's half already caught. Swapping both leaves
  # every rail satisfied — both properties read, no forbidden name anywhere —
  # while the per-job line reports a drain stop and the drain-stop line reports
  # a job.
  DR11)
    snippet OLD <<'EOF'
                        \(refusal.logEvent, privacy: .public) job=\(job.jobId, privacy: .public) \
EOF
    snippet NEW <<'EOF'
                        \(refusal.drainStoppedEvent, privacy: .public) job=\(job.jobId, privacy: .public) \
EOF
    patch "$file" "$OLD" "$NEW"
    snippet OLD <<'EOF'
                \(refusalThatStoppedUs.drainStoppedEvent, privacy: .public) \
EOF
    snippet NEW <<'EOF'
                \(refusalThatStoppedUs.logEvent, privacy: .public) \
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR12 — R4 review. DR06 respelled: the drain-stop event hard-coded again,
  # split by a `\` continuation inside the multiline string, with a decoy
  # `logger.debug` supplying the property read.
  DR12)
    snippet OLD <<'EOF'
            logger.error(
                """
                \(refusalThatStoppedUs.drainStoppedEvent, privacy: .public) \
EOF
    snippet NEW <<'EOF'
            logger.debug("drain stop kind=\(refusalThatStoppedUs.drainStoppedEvent, privacy: .public)")
            logger.error(
                """
                fm.backfill.drain_stopped_\
                by_throttle \
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR13 — R4 review. The readiness probe's budget becomes thirty seconds,
  # written as arithmetic on an allowed one so the line still MENTIONS
  # `FMInferenceDeadline.standard`.
  DR13)
    snippet OLD <<'EOF'
            try await FMInferenceDeadline.run(FMInferenceDeadline.standard) {
EOF
    snippet NEW <<'EOF'
            try await FMInferenceDeadline.run(FMInferenceDeadline.standard / 10) {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  # DR14 — R4 review. An ADDED 30 s call site spelled across two lines. The
  # existing `standard` site stays, so the site count still clears its floor
  # and only judging the ARGUMENT can see this.
  DR14)
    snippet OLD <<'EOF'
            try await FMInferenceDeadline.run(FMInferenceDeadline.standard) {
EOF
    snippet NEW <<'EOF'
            try await FMInferenceDeadline
                .run(.seconds(30)) {
                logger.debug("Foundation Models readiness probe warm-up")
            }
            try await FMInferenceDeadline.run(FMInferenceDeadline.standard) {
EOF
    patch "$file" "$OLD" "$NEW" ;;

  *)
    echo "mutation-battery: unknown mutation '$name'" >&2
    return 3 ;;
  esac
}

# ---------------------------------------------------------------------------
# Record accessors
# ---------------------------------------------------------------------------
rec_name()   { printf '%s' "${1%%|*}"; }
rec_batch()  { printf '%s' "$1" | cut -d'|' -f2; }
rec_file()   {
  case "$(printf '%s' "$1" | cut -d'|' -f3)" in
    ORCH)  printf '%s' "$ORCH" ;;
    STORE) printf '%s' "$STORE" ;;
    CTRL)  printf '%s' "$CTRL" ;;
    VIEW)  printf '%s' "$VIEW" ;;
    TRIG)  printf '%s' "$TRIG" ;;
    RSVC)  printf '%s' "$RSVC" ;;
    TRUST) printf '%s' "$TRUST" ;;
    NPV)   printf '%s' "$NPV" ;;
    NPVM)  printf '%s' "$NPVM" ;;
    BWPOL) printf '%s' "$BWPOL" ;;
    KICK)  printf '%s' "$KICK" ;;
    KCOORD) printf '%s' "$KCOORD" ;;
    SEAMS) printf '%s' "$SEAMS" ;;
    ACT)   printf '%s' "$ACT" ;;
    ADSVC) printf '%s' "$ADSVC" ;;
    ACOORD) printf '%s' "$ACOORD" ;;
    ESUMBF) printf '%s' "$ESUMBF" ;;
    ADSVC_ATOM) printf '%s' "$ATOMEV" ;;
    PODC)  printf '%s' "$PODC" ;;
    MPTRIDX) printf '%s' "$MPTRIDX" ;;
    THROT) printf '%s' "$THROT" ;;
    FMREF) printf '%s' "$FMREF" ;;
    RUNNER) printf '%s' "$RUNNER" ;;
    FMCLS) printf '%s' "$FMCLS" ;;
    PROBE) printf '%s' "$PROBE" ;;
    RT)    printf '%s' "$RT" ;;
    MODEL) printf '%s' "$MODEL" ;;
    INGO)  printf '%s' "$INGO" ;;
    INVF)  printf '%s' "$INVF" ;;
    SWEEP) printf '%s' "$SWEEP" ;;
    SCANORD) printf '%s' "$SCANORD" ;;
    SCRATCH)  printf '%s' "$SCRATCH" ;;
    SCRATCHH) printf '%s' "$SCRATCHH" ;;
    FMSUP) printf '%s' "$FMSUP" ;;
    GATE)  printf '%s' "$GATE" ;;
    FUSION) printf '%s' "$FUSION" ;;
    DSPAN) printf '%s' "$DSPAN" ;;
    EXTENT) printf '%s' "$EXTENT" ;;
    RSLOT) printf '%s' "$RSLOT" ;;
    DETCLS) printf '%s' "$DETCLS" ;;
    DETLED) printf '%s' "$DETLED" ;;
    SPLIT) printf '%s' "$SPLIT" ;;
    HOTGATE) printf '%s' "$HOTGATE" ;;
    UGCEN) printf '%s' "$UGCEN" ;;
    POLICY) printf '%s' "$POLICY" ;;
    SEGAGG) printf '%s' "$SEGAGG" ;;
    DLMGR) printf '%s' "$DLMGR" ;;
    FQSCAN) printf '%s' "$FQSCAN" ;;
    BGFEED) printf '%s' "$BGFEED" ;;
    EPPREP) printf '%s' "$EPPREP" ;;
    SCHED) printf '%s' "$SCHED" ;;
    CLAIM) printf '%s' "$CLAIM" ;;
    RECON) printf '%s' "$RECON" ;;
    ATOM)  printf '%s' "$ATOM" ;;
    AJRUN) printf '%s' "$AJRUN" ;;
    *)     printf '%s' "" ;;
  esac
}
rec_expect() { printf '%s' "$1" | cut -d'|' -f4-; }

# ---------------------------------------------------------------------------
# Safety: clean tree in, clean tree out
# ---------------------------------------------------------------------------
require_clean_tree() {
  if [ -n "$(git status --porcelain -- "${MUTABLE_FILES[@]}")" ]; then
    cat >&2 <<MSG
mutation-battery: the tree has uncommitted changes to a file this script
mutates. It restores with \`git checkout --\`, which would DESTROY that work.

$(git status --porcelain -- "${MUTABLE_FILES[@]}")

If a previous battery run was INTERRUPTED, that diff is its injected mutation,
not your work — check with \`git diff -- ${MUTABLE_FILES[*]}\` and discard it
with \`git checkout -- ${MUTABLE_FILES[*]}\`. Do NOT commit or stash it: a
committed mutation is a fabricated bug on the branch, and a stashed one comes
back later.

Otherwise commit your work first.
MSG
    exit 2
  fi
}

hash_of() { shasum -a 256 "$1" | awk '{print $1}'; }

restore_sources() {
  git checkout -- "${MUTABLE_FILES[@]}"
}

# Verify the restore actually produced the ORIGINAL bytes, after EVERY batch —
# not only at the end.
#
# `git checkout --` is not guaranteed to succeed: an `index.lock` left by a
# concurrent git process, a file turned read-only, a checkout racing an editor.
# If it fails mid-run the tree keeps this batch's injected defect, and every
# LATER batch is then evaluated against a codebase that already contains a bug —
# reddening tests for reasons that have nothing to do with their own mutation,
# i.e. false KILLs. The end-of-run check cannot see this: the final restore
# repairs the tree, the hashes match, and the corrupted verdicts print as
# "All mutations killed". Checking here is what makes the table trustworthy.
#
# Returns 1 rather than exiting so the caller can `break` and still print the
# partial table — an aborted run that shows its work beats a silent one.
restore_and_verify() {
  local where="${1:-final}" now="" f
  restore_sources
  for f in "${MUTABLE_FILES[@]}"; do
    now="$now$(hash_of "$f")  $f
"
  done
  if [ "$now" != "$HASHES_BEFORE" ]; then
    echo >&2
    echo "mutation-battery: FATAL — restore after $where was NOT byte-exact." >&2
    printf 'before:\n%s\nafter:\n%s\n' "$HASHES_BEFORE" "$now" >&2
    echo "The working tree may still carry an injected defect. Run" >&2
    echo "  git status --porcelain -- ${MUTABLE_FILES[*]}" >&2
    echo "before doing anything else. Verdicts printed below this line are only" >&2
    echo "trustworthy up to $where." >&2
    RESTORE_OK=0
    FATAL=1
    return 1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# Focused run + failure extraction
# ---------------------------------------------------------------------------
# Swift Testing prints `✘ Test "<display name>" failed after …` through
# xcodebuild.  Function-name form (no display name) is handled too so a future
# undecorated @Test is not silently invisible here.
# `scripts/fast-gate.sh` retries once on a wedged simulator, and BOTH attempts
# land in the same log. Attempt 1's casualties (tests that were mid-flight when
# the sim died) would otherwise union with attempt 2's results and credit a
# mutation as KILLED off an infrastructure artefact. Cut everything before the
# retry banner so only the last attempt is read.
last_attempt() {
  python3 -c '
import sys
lines = open(sys.argv[1], encoding="utf-8", errors="replace").readlines()
cut = 0
for i, line in enumerate(lines):
    if "fast-gate: wedged simulator" in line:
        cut = i + 1
sys.stdout.writelines(lines[cut:])
' "$1"
}

extract_failures() {
  python3 -c '
import re, sys
# playhead-avbn: SEARCH, not match-from-start. xcodebuild interleaves its own
# output into a line often enough to matter — an observed run prefixed
# XCTestOutputBarrier onto a started-marker line, and those are WORD characters,
# so the old ^\W* could not skip them. Widening cannot manufacture a KILL: the
# anchor is the Swift Testing glyph plus the literal " Test \"", which nothing
# else in the log emits.
pat_named = re.compile(r"✘ Test \"(.+?)\" (?:failed|recorded an issue)")
pat_plain = re.compile(r"✘ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) failed")
# playhead-le02: XCTest, which this battery could not see at all. Until now an
# expectation naming an XCTest case reported ERROR ("expected test never ran"),
# so those rails were "verified by hand" (see the kvs8 note in FOCUSED_SUITES)
# — i.e. not verified by anything that fails when someone stops doing it.
#
# The duration heuristic INVERTS for XCTest and that matters here: a Swift
# Testing failure over ~97 s is a load flake, but an XCTest failure is an
# ASSERTION and is fast (0.025 s is typical). Nothing in this function times
# anything, which is correct — a mutation battery wants every failure, flake or
# not, because the baseline run is the control.
# `chr(39)` rather than a literal apostrophe: this whole program is inside a
# single-quoted `python3 -c` argument, so an apostrophe would end it.
Q = chr(39)
pat_xctest = re.compile(r"Test Case " + Q + r"-\[[A-Za-z0-9_.]*?([A-Za-z0-9_]+) ([A-Za-z0-9_]+)\]" + Q + r" failed")
seen = []
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    x = pat_xctest.search(line)
    if x:
        # Both forms, so an expectation may be written bare when the method
        # name is distinctive or qualified when it is not.
        for name in (x.group(2), x.group(1) + "/" + x.group(2)):
            if name not in seen:
                seen.append(name)
        continue
    m = pat_named.search(line) or pat_plain.search(line)
    if m and m.group(1) not in seen:
        seen.append(m.group(1))
# Deliberately not `print("\n".join(...))`: on an empty list that emits a bare
# newline, which reads downstream as one nameless failure.
sys.stdout.writelines(name + "\n" for name in seen)
' "$1"
}

# Every test that RAN in this attempt, by display name. Companion to
# `extract_failures`, and the input to the "expected test never ran" check
# below: an expectation that matches NOTHING in the run is a harness fault
# (typo, renamed test, suite missing from FOCUSED_SUITES, or a ';' inside a
# display name colliding with the record separator) — and without this it is
# indistinguishable from a genuine SURVIVED, which is the failure direction
# that reads as a coverage hole and sends the next person to write a test that
# already exists. Measured: playhead-96ot's E04 reported SURVIVED with its
# expected test visibly failing three lines above, one bead after playhead-d3g0
# documented the same collision.
extract_ran() {
  python3 -c '
import re, sys
# playhead-avbn: SEARCH, not match-from-start — see extract_failures. This is
# the function the interleaving actually defeated, turning a real KILL into a
# reported ERROR ("expected test never ran") with the failure printed two lines
# above it. Widening here can only move a verdict ERROR -> KILLED/SURVIVED; the
# KILL itself comes from extract_failures.
pat_named = re.compile(r"◇ Test \"(.+?)\" started")
pat_plain = re.compile(r"◇ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) started")
# playhead-le02: the XCTest half — see extract_failures for why. Must stay in
# lockstep with that function: a name this one cannot record is reported as a
# harness fault no matter what the other one saw, which is exactly the
# false-ERROR direction the "never ran" check exists to remove.
Q = chr(39)  # see extract_failures
pat_xctest = re.compile(r"Test Case " + Q + r"-\[[A-Za-z0-9_.]*?([A-Za-z0-9_]+) ([A-Za-z0-9_]+)\]" + Q + r" started")
seen = []
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    x = pat_xctest.search(line)
    if x:
        for name in (x.group(2), x.group(1) + "/" + x.group(2)):
            if name not in seen:
                seen.append(name)
        continue
    m = pat_named.search(line) or pat_plain.search(line)
    if m and m.group(1) not in seen:
        seen.append(m.group(1))
sys.stdout.writelines(name + "\n" for name in seen)
' "$1"
}

run_focused() {
  local log="$1"
  scripts/fast-gate.sh "${FOCUSED_SUITES[@]}" >"$log" 2>&1
  # fast-gate.sh already captures PIPESTATUS internally; its exit code is the
  # xcodebuild status.  Deliberately NOT piped here.
  return $?
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
ONLY=""
ONLY_BATCH=""
LIST_ONLY=0
DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --list)    LIST_ONLY=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --only)  ONLY="$2"; shift 2 ;;
    --batch) ONLY_BATCH="$2"; shift 2 ;;
    -h|--help)
      # Print the whole header block and stop at the first non-comment line.
      # A line range would silently truncate the moment the header grows — it
      # already had, cutting USAGE off the bottom of --help.
      awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0"
      exit 0 ;;
    *) echo "mutation-battery: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

if [ "$LIST_ONLY" -eq 1 ]; then
  printf '%-5s %-6s %s\n' "NAME" "BATCH" "MUTATION"
  for rec in "${MUTATIONS[@]}"; do
    printf '%-5s %-6s %s\n' \
      "$(rec_name "$rec")" "$(rec_batch "$rec")" "$(describe_mutation "$(rec_name "$rec")")"
    printf '%-12s expects: %s\n' "" "$(rec_expect "$rec")"
  done
  exit 0
fi

require_clean_tree
TREE_OWNED=1

HASHES_BEFORE=""
for f in "${MUTABLE_FILES[@]}"; do
  HASHES_BEFORE="$HASHES_BEFORE$(hash_of "$f")  $f
"
done

# Selected records
SELECTED=()
for rec in "${MUTATIONS[@]}"; do
  name="$(rec_name "$rec")"
  batch="$(rec_batch "$rec")"
  if [ -n "$ONLY" ] && [ "$name" != "$ONLY" ]; then continue; fi
  if [ -n "$ONLY_BATCH" ] && [ "$batch" != "$ONLY_BATCH" ]; then continue; fi
  SELECTED+=("$rec")
done
if [ "${#SELECTED[@]}" -eq 0 ]; then
  echo "mutation-battery: nothing selected" >&2
  exit 2
fi

# Batch ids, in first-seen order
BATCH_IDS=()
for rec in "${SELECTED[@]}"; do
  b="$(rec_batch "$rec")"
  found=0
  for existing in ${BATCH_IDS[@]+"${BATCH_IDS[@]}"}; do
    [ "$existing" = "$b" ] && found=1 && break
  done
  [ "$found" -eq 0 ] && BATCH_IDS+=("$b")
done

RESULTS="$WORK/results"
: >"$RESULTS"
BUILD_COUNT=0
FATAL=0
# Cleared by `restore_and_verify` and never re-set; see the note at the final
# restore for why a repaired tree must not un-fail the run.
RESTORE_OK=1
START_TS="$(date +%s)"

echo "mutation-battery: DEVELOPER_DIR=$DEVELOPER_DIR"
echo "mutation-battery: ${#SELECTED[@]} mutation(s) in ${#BATCH_IDS[@]} batch(es)"
echo

# ---------------------------------------------------------------------------
# Baseline. KILLED is decided by "the expected test appears in this batch's
# failure list", so a test that is ALREADY red — an unrelated regression, or a
# load flake expiring one of the polling budgets — would credit every mutation
# that names it. `listenRevertSurvivesEpisodeReplacement` alone is the sole
# expectation of five mutations and half of two more, so one such flake can
# print "All mutations killed" and exit 0. That is the exact false
# certification this tool exists to prevent, and it fails in the direction
# that reads as success. One unmutated run closes it.
#
# Set PLAYHEAD_MB_SKIP_BASELINE=1 ONLY when you have just run the focused
# suites green yourself; it trades a build for the guarantee above.
# ---------------------------------------------------------------------------
if [ "$DRY_RUN" -eq 0 ] && [ "${PLAYHEAD_MB_SKIP_BASELINE:-0}" != "1" ]; then
  echo "=== baseline: focused suites on UNMUTATED sources ==="
  BASE_LOG="$WORK/baseline.log"
  run_focused "$BASE_LOG"
  BASE_RC=$?
  BUILD_COUNT=$((BUILD_COUNT + 1))
  last_attempt "$BASE_LOG" >"$BASE_LOG.last"
  if ! grep -q "Test run with" "$BASE_LOG.last"; then
    echo "mutation-battery: the baseline did not run tests (rc=$BASE_RC)" >&2
    grep -m 20 -E "error:|BUILD FAILED|Killed: 9" "$BASE_LOG" >&2 || true
    KEEP_WORK=1
    exit 2
  fi
  extract_failures "$BASE_LOG.last" >"$WORK/baseline-failures.txt"
  if [ -s "$WORK/baseline-failures.txt" ]; then
    echo "mutation-battery: the focused suites are RED before any mutation." >&2
    sed 's/^/    ✘ /' "$WORK/baseline-failures.txt" >&2
    echo "Every mutation naming one of those tests would be credited KILLED for" >&2
    echo "a reason that has nothing to do with the mutation. Fix the tree first." >&2
    KEEP_WORK=1
    exit 2
  fi
  # Every expectation must NAME A TEST THAT ACTUALLY RAN. Checked here, on the
  # one build that is already being spent, so a mis-typed or mis-split
  # expectation costs zero mutation builds instead of printing SURVIVED.
  extract_ran "$BASE_LOG.last" >"$WORK/baseline-ran.txt"
  UNKNOWN=""
  for rec in "${SELECTED[@]}"; do
    exp="$(rec_expect "$rec")"
    OLDIFS="$IFS"; IFS=';'
    for want in $exp; do
      IFS="$OLDIFS"
      grep -Fxq "$want" "$WORK/baseline-ran.txt" || \
        UNKNOWN="${UNKNOWN}    $(rec_name "$rec") expects: ${want}
"
      IFS=';'
    done
    IFS="$OLDIFS"
  done
  if [ -n "$UNKNOWN" ]; then
    echo "mutation-battery: an expectation names a test that never ran." >&2
    printf '%s' "$UNKNOWN" >&2
    cat >&2 <<'MSG'
Causes, in order of how often they happen: the display name contains a ';'
(the MUTATIONS record separator splits it into fragments that match nothing);
the test was renamed; the suite is missing from FOCUSED_SUITES; a typo.
Every one of them would otherwise print SURVIVED against a working rail.
MSG
    KEEP_WORK=1
    exit 2
  fi
  echo "  baseline green"
  echo
fi

for b in "${BATCH_IDS[@]}"; do
  MEMBERS=()
  for rec in "${SELECTED[@]}"; do
    [ "$(rec_batch "$rec")" = "$b" ] && MEMBERS+=("$rec")
  done

  echo "=== batch $b: ${#MEMBERS[@]} mutation(s) ==="
  APPLY_FAILED=0
  for rec in "${MEMBERS[@]}"; do
    name="$(rec_name "$rec")"
    file="$(rec_file "$rec")"
    echo "  apply $name — $(describe_mutation "$name")"
    if ! apply_mutation "$name" "$file"; then
      echo "mutation-battery: FAILED to apply $name (anchor drift?)" >&2
      APPLY_FAILED=1
      break
    fi
  done

  if [ "$APPLY_FAILED" -eq 1 ]; then
    for rec in "${MEMBERS[@]}"; do
      echo "$(rec_name "$rec")|ERROR|anchor did not apply — source moved on" >>"$RESULTS"
    done
    FATAL=1
    restore_and_verify "batch $b" || break
    continue
  fi

  if [ "$DRY_RUN" -eq 1 ]; then
    git --no-pager diff --stat -- "${MUTABLE_FILES[@]}"
    git --no-pager diff -U1 -- "${MUTABLE_FILES[@]}"
    for rec in "${MEMBERS[@]}"; do
      echo "$(rec_name "$rec")|DRY-RUN|anchor applied" >>"$RESULTS"
    done
    restore_and_verify "batch $b" || break
    echo
    continue
  fi

  LOG="$WORK/batch-$b.log"
  echo "  running focused suites…"
  run_focused "$LOG"
  RC=$?
  BUILD_COUNT=$((BUILD_COUNT + 1))

  last_attempt "$LOG" >"$LOG.last"
  if ! grep -q "Test run with" "$LOG.last"; then
    echo "mutation-battery: batch $b did not run tests (build failure?), rc=$RC" >&2
    grep -m 20 -E "error:|BUILD FAILED|Killed: 9" "$LOG" >&2 || true
    for rec in "${MEMBERS[@]}"; do
      echo "$(rec_name "$rec")|ERROR|batch did not build/run" >>"$RESULTS"
    done
    FATAL=1
    restore_and_verify "batch $b" || break
    continue
  fi

  FAILED_LIST="$WORK/failed-$b.txt"
  RAN_LIST="$WORK/ran-$b.txt"
  extract_ran "$LOG.last" >"$RAN_LIST"
  extract_failures "$LOG.last" >"$FAILED_LIST"
  echo "  observed failures:"
  if [ -s "$FAILED_LIST" ]; then
    sed 's/^/    ✘ /' "$FAILED_LIST"
  else
    echo "    (none)"
  fi

  for rec in "${MEMBERS[@]}"; do
    name="$(rec_name "$rec")"
    expect="$(rec_expect "$rec")"
    missing=""
    never_ran=""
    OLDIFS="$IFS"; IFS=';'
    for want in $expect; do
      IFS="$OLDIFS"
      if ! grep -Fxq "$want" "$RAN_LIST"; then
        # Not a survivor — the harness never asked the question. Kept separate
        # from `missing` so the two are never conflated in the report.
        never_ran="${never_ran}${never_ran:+ | }${want}"
      elif ! grep -Fxq "$want" "$FAILED_LIST"; then
        missing="${missing}${missing:+ | }${want}"
      fi
      IFS=';'
    done
    IFS="$OLDIFS"
    if [ -n "$never_ran" ]; then
      echo "$name|ERROR|expected test never ran (';' in the name? renamed? suite not in FOCUSED_SUITES?): $never_ran" >>"$RESULTS"
      FATAL=1
    elif [ -z "$missing" ]; then
      echo "$name|KILLED|" >>"$RESULTS"
    else
      echo "$name|SURVIVED|$missing" >>"$RESULTS"
    fi
  done

  restore_and_verify "batch $b" || break
  echo
done

# ---------------------------------------------------------------------------
# Restoration must be byte-exact. Per-batch checks above already enforce this
# after every mutation; this is the belt for the final state (and the only
# check that runs at all when the loop was skipped entirely).
#
# `restore_and_verify` only ever clears RESTORE_OK — it is never re-set to 1
# here, so a mid-run mismatch that a later restore happened to repair still
# fails the run. The tree being clean NOW does not make the verdicts above it
# trustworthy.
# ---------------------------------------------------------------------------
restore_and_verify "the last batch" || true

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
ELAPSED=$(( $(date +%s) - START_TS ))
echo "================================ RESULTS ================================"
printf '%-6s %-9s %s\n' "NAME" "VERDICT" "MUTATION"
SURVIVORS=0
ERRORS=0
while IFS='|' read -r name verdict detail; do
  printf '%-6s %-9s %s\n' "$name" "$verdict" "$(describe_mutation "$name")"
  case "$verdict" in
    SURVIVED) SURVIVORS=$((SURVIVORS + 1))
              printf '%-16s still green: %s\n' "" "$detail" ;;
    ERROR)    ERRORS=$((ERRORS + 1))
              printf '%-16s %s\n' "" "$detail" ;;
  esac
done <"$RESULTS"
echo "-------------------------------------------------------------------------"
printf 'builds: %d   wall clock: %dm%02ds   survivors: %d   errors: %d\n' \
  "$BUILD_COUNT" "$((ELAPSED / 60))" "$((ELAPSED % 60))" "$SURVIVORS" "$ERRORS"

if [ "$SURVIVORS" -gt 0 ] || [ "$ERRORS" -gt 0 ] || [ "$FATAL" -eq 1 ]; then
  KEEP_WORK=1
fi

if [ "$RESTORE_OK" -eq 0 ]; then
  echo "TREE NOT RESTORED — inspect the worktree before doing anything else." >&2
  exit 4
fi
if [ "$ERRORS" -gt 0 ] || [ "$FATAL" -eq 1 ]; then
  echo "One or more mutations could not be evaluated. Fix the EDIT, not the expectation." >&2
  exit 3
fi
if [ "$SURVIVORS" -gt 0 ]; then
  cat >&2 <<'MSG'

A SURVIVOR IS A COVERAGE HOLE. The defect above can be introduced with the
focused suites still green. Write the test that rejects it — do not relax the
expectation and do not delete the entry.
MSG
  exit 1
fi
if [ "$DRY_RUN" -eq 1 ]; then
  echo "Dry run: every anchor applied exactly once and the tree was restored."
else
  echo "All mutations killed."
fi
