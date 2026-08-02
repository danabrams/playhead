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
# THE R SERIES LIVES ELSEWHERE (playhead-voez)
# --------------------------------------------
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
#   PARTIAL RE-RUN 2026-08-02 (playhead-cgka). Batches 120-125 (Z01-Z12, 12 new
#   entries), 6 batches. FINAL 12 KILLED / 0 SURVIVED / 0 ERROR, 9 builds.
#   Batches 1-102 were NOT re-run and carry the verdicts above. Recount: the
#   array now holds 182 live entries.
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
# playhead-kvs8: the FM daemon throttle (Q01-Q08). THROT is the single
# definition + the named causes; RUNNER carries the defer branch that replaced
# the terminal `failed`; FMCLS the permissive status/counter mapping; PROBE the
# readiness cache guard.
THROT="Playhead/Services/AdDetection/FMDaemonThrottle.swift"
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
MUTABLE_FILES=(
  "$ORCH" "$STORE" "$CTRL" "$VIEW" "$TRIG" "$RSVC" "$TRUST" "$NPV" "$NPVM"
  "$BWPOL" "$KICK" "$KCOORD" "$SEAMS" "$ACT" "$ADSVC" "$PODC"
  "$THROT" "$RUNNER" "$FMCLS" "$PROBE" "$RT" "$MODEL" "$INGO" "$INVF"
  "$SWEEP" "$SCANORD" "$SCRATCH" "$SCRATCHH"
)

FOCUSED_SUITES=(
  # playhead-cgka: the scratch-reaper rails (Z series). 13 tests, ~0.06s — it
  # costs nothing to carry in every batch and the alternative is a second
  # focused set for one series.
  -only-testing:PlayheadTests/TestScratchReaperTests
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

# One-line description per mutation, for the report.
describe_mutation() {
  case "$1" in
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
    PODC)  printf '%s' "$PODC" ;;
    THROT) printf '%s' "$THROT" ;;
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
pat_named = re.compile(r"^\W*✘ Test \"(.+?)\" (?:failed|recorded an issue)")
pat_plain = re.compile(r"^\W*✘ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) failed")
seen = []
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    m = pat_named.match(line) or pat_plain.match(line)
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
pat_named = re.compile(r"^\W*◇ Test \"(.+?)\" started")
pat_plain = re.compile(r"^\W*◇ Test ([A-Za-z_][A-Za-z0-9_]*\(\)) started")
seen = []
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    m = pat_named.match(line) or pat_plain.match(line)
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
