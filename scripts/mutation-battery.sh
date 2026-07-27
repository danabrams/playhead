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
# applies the mutation, runs only the three focused suites, checks the expected
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
# LAST GREEN END-TO-END RUN
#   2026-07-27 — 26/26 KILLED, 0 survivors, 0 errors, exit 0.
#   10 builds (1 baseline + 9 batches), 17m21s wall clock.
# That run predates the playhead-o4qr merge and NO LONGER DESCRIBES THIS FILE.
#
# STATUS AFTER THE playhead-o4qr MERGE (2026-07-27)
#   The battery is 26 mutations, not 31: five entries were relocated to the
#   KNOWN GAP block (see the MERGE NOTE above `MUTATIONS`). All 26 anchors are
#   dry-run verified to apply exactly once against the merged source.
#
#   Batches 1–7 were re-run: 15 mutations, 14 KILLED, 1 SURVIVED (M17).
#
#   M17 SURVIVED and the survivor is REAL, but it is an unpinned rail rather
#   than a live defect, so read the next paragraph before "fixing" anything.
#   Production attribution is correct by construction: both seams pass the
#   `sourceShowId` captured at gesture time into `makeManualCorrectionVetoEvent`
#   and never read `activePodcastId`. What changed is the OBSERVATION POINT. On
#   main the receipt was written by `persistManualCorrectionVeto` AFTER the
#   revert barrier, so parking there and swapping episodes made a
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
#   second, pre-mint barrier is the fix; it is a new test seam, deliberately not
#   done as part of a merge.
#
#   Batches 8–12 were NOT re-run, for two different reasons, and BOTH must be
#   cleared before this file can claim a whole-battery green again:
#     • batches 8 and 9 contain N06 and N07, whose only victim
#       (`anonymousRevertRecordsNoControllerSample`) is currently RED — o4qr's
#       `exactFeedbackShowIdentity` refuses a revert whose show id is nil or
#       empty while the episode has one, which is the contract collision the
#       merge report flags for decision. Running them now would credit both
#       mutations off a pre-existing failure, which is precisely the
#       miscrediting the baseline exists to prevent.
#     • batches 10–12 (S01–S05) are the suggest-tier rails; untouched by the
#       merge resolution and simply not re-run, for build budget.
#
#   The baseline is likewise BLOCKED until that red test is resolved: it refuses
#   to start while any focused-suite test is already failing. Batches 1–7 were
#   therefore run with PLAYHEAD_MB_SKIP_BASELINE=1, which is sound only because
#   none of them names the red test. Do not extend that shortcut to 8/9.
#
# THE CONTRACT COLLISION IS RESOLVED (playhead-o4qr, 2026-07-27)
#   Dan's decision: ACCEPT THE RECEIPT, REFUSE THE LEARNING. A correction whose
#   show identity is unusable (nil, empty, non-canonical, or disagreeing with
#   the live episode) still commits its durable receipt and still returns true;
#   what it withholds is every show-KEYED effect — trust penalty, hard-negative
#   bank, per-show threshold controller, show-scoped recurrence revocation.
#
#   Consequences for this file, all already applied below:
#     • `T_ANON_SILENT` was RETITLED. The test grew three more clauses, so its
#       @Test display name changed; the constant had to follow or N06/N07 and
#       the new O-series would all be silently un-creditable.
#     • Batches 8 and 9 are UNBLOCKED — their victim is green again, so N06 and
#       N07 can finally be credited honestly rather than off a red test.
#     • Five new entries, O01–O05, pin the decision itself: three learning
#       surfaces N07 cannot see (bank, trust, revocation scope) plus BOTH
#       seams' receipt half. See the note above them for the batching.
#     • Two pre-existing tests pinned the OLD refusal and were re-pointed:
#       `revertWindowRemovesCue` (now O02's second victim) and
#       `autoSkipNoWinsBlockedAppliedPersistenceRace`, whose show probe moved
#       to `staleShowBannerNoKeepsReceiptAndRecordsNoLearning` (O05's victim).

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
MUTABLE_FILES=("$ORCH" "$STORE" "$CTRL")

FOCUSED_SUITES=(
  -only-testing:PlayheadTests/SkipOrchestratorThresholdControlTests
  -only-testing:PlayheadTests/SkipOrchestratorRevertTests
  -only-testing:PlayheadTests/SkipOrchestratorRevertLifecycleRaceTests
  # playhead-ugy4: the two suggest-tier rails (S04/S05) live in
  # SkipOrchestratorCharacterizationTests.swift, whose @Suite structs are
  # named for their topic rather than the file.
  -only-testing:PlayheadTests/SkipOrchestratorAdDecisionContractTests
  -only-testing:PlayheadTests/SkipOrchestratorSuggestTierTests
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
  # `lastSuggestRevisionByWindowId`, including S04's and S05's expectations —
  # sharing a batch would credit those two off S01's blast radius.
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
    O01) echo "ingestNegativeFingerprint: drop the anonymous-show refusal (a NULL-show hard negative every show reads back)" ;;
    O02) echo "revertWindow: fall the trust penalty back to activePodcastId when the correction has no usable show" ;;
    O03) echo "revertWindow: restore the outright refusal, so an anonymous correction loses its durable receipt" ;;
    O04) echo "revertWindow: attribute recurrence REVOCATION to activePodcastId when the correction has no usable show" ;;
    O05) echo "denyAutoSkippedBanner: restore the outright refusal, so a banner No naming another show loses its receipt" ;;
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

  M14)
    snippet OLD <<'EOF'
        ingestNegativeFingerprint(
            text: requestedManaged.adWindow.evidenceText,
            podcastId: sourceShowId
        )
EOF
    snippet NEW <<'EOF'
        if episodeLifecycleGeneration == sourceLifecycleGeneration {
            ingestNegativeFingerprint(
                text: requestedManaged.adWindow.evidenceText,
                podcastId: sourceShowId
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
                podcastId: activePodcastId
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
  O04)
    snippet OLD <<'EOF'
                for: requestedManaged.adWindow,
                showId: sourceShowId,
                source: .manualVeto
EOF
    snippet NEW <<'EOF'
                for: requestedManaged.adWindow,
                showId: activePodcastId,
                source: .manualVeto
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
    OLDIFS="$IFS"; IFS=';'
    for want in $expect; do
      IFS="$OLDIFS"
      if ! grep -Fxq "$want" "$FAILED_LIST"; then
        missing="${missing}${missing:+ | }${want}"
      fi
      IFS=';'
    done
    IFS="$OLDIFS"
    if [ -z "$missing" ]; then
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
