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
)

# Named to match the `/private/tmp/playhead-*` pattern `scripts/disk-cleanup.sh`
# already sweeps at 3 days, so kept logs are reaped by the existing weekly cron
# instead of accumulating on a box with documented disk pressure.
WORK="$(mktemp -d /private/tmp/playhead-mutation-battery.XXXXXX)"
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
T_ANON_SILENT="An anonymous revert (no podcastId, or an empty one) records no controller sample"
T_ACCEPT_RACE="A suggest Yes whose episode is replaced mid-flight calibrates the captured show"
T_DENY_RACE="A banner No whose episode is replaced mid-flight calibrates the captured show"

MUTATIONS=(
  "M01|1|ORCH|$T_MANAGED_RACE"
  "M02|1|ORCH|$T_SUGGEST_RACE"
  "M05|1|ORCH|$T_ANON_RACE"
  "M07|1|ORCH|$T_LISTEN_RACE"
  "M11|1|ORCH|$T_LISTEN_FP"

  "M03|2|ORCH|$T_MANAGED_RACE"
  "M04|2|ORCH|$T_SUGGEST_RACE"
  "M12|2|ORCH|$T_LISTEN_RACE"
  "M09|2|ORCH|$T_REVERTWINDOW_FP"
  "M20|2|ORCH|$T_CONFIRM_SILENT"

  # M08 deliberately does NOT share a batch with M13: both rewrite the
  # `if revertedManagedAny { … }` block, so whichever lands first destroys the
  # other's anchor.
  "M13|3|ORCH|$T_LISTEN_RACE;$T_MANAGED_RACE"
  "M10|3|ORCH|$T_SUGGEST_NO_NOSTORE"

  "M14|4|ORCH|$T_LISTEN_RACE"
  "M06|4|ORCH|$T_MANAGED_RACE"
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
            guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
                return
            }

            evaluateAndPush()
EOF
    snippet NEW <<'EOF'
            if podcastId != nil, trustService != nil {
                guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
                    return
                }
            }

            evaluateAndPush()
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
        // Phase 7.2 / playhead-zskc: persist a listenRevert CorrectionEvent
EOF
    snippet NEW <<'EOF'
        guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
            return
        }

        // Phase 7.2 / playhead-zskc: persist a listenRevert CorrectionEvent
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M08)
    snippet OLD <<'EOF'
            if revertedManagedAny {
                recordThresholdControlSignal(.falsePositive, podcastId: podcastId)
            }
EOF
    snippet NEW <<'EOF'
            recordThresholdControlSignal(.falsePositive, podcastId: podcastId)
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
        // Phase 7.2 / playhead-zskc: persist a listenRevert CorrectionEvent
EOF
    snippet NEW <<'EOF'
        guard trustService != nil else { return }

        // Phase 7.2 / playhead-zskc: persist a listenRevert CorrectionEvent
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M12)
    snippet OLD <<'EOF'
        guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
            // A replacement episode owns the live cue state now; the durable
            // and calibration effects above were the old lifecycle's to write.
            return
        }

        // Remove the cue and re-push.
EOF
    snippet NEW <<'EOF'
        // Remove the cue and re-push.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M13)
    # Both calibrating revert seams at once: the sample must be attributed to
    # the show CAPTURED at gesture time, never to whoever is live at effect
    # time.  Batched as one mutation because it is one defect with two sites.
    snippet OLD <<'EOF'
        recordThresholdControlSignal(.falsePositive, podcastId: podcastId)

        guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
            // A replacement episode owns the live cue state now; the durable
EOF
    snippet NEW <<'EOF'
        recordThresholdControlSignal(.falsePositive, podcastId: activePodcastId)

        guard episodeLifecycleGeneration == sourceLifecycleGeneration else {
            // A replacement episode owns the live cue state now; the durable
EOF
    patch "$file" "$OLD" "$NEW"
    snippet OLD <<'EOF'
            if revertedManagedAny {
                recordThresholdControlSignal(.falsePositive, podcastId: podcastId)
            }
EOF
    snippet NEW <<'EOF'
            if revertedManagedAny {
                recordThresholdControlSignal(.falsePositive, podcastId: activePodcastId)
            }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M14)
    snippet OLD <<'EOF'
        ingestNegativeFingerprint(
            text: managed.adWindow.evidenceText,
            podcastId: podcastId
        )
EOF
    snippet NEW <<'EOF'
        if episodeLifecycleGeneration == sourceLifecycleGeneration {
            ingestNegativeFingerprint(
                text: managed.adWindow.evidenceText,
                podcastId: podcastId
            )
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M15)
    snippet OLD <<'EOF'
        // Signal the trust engine about the false skip.
        if let podcastId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: podcastId)
        }
EOF
    snippet NEW <<'EOF'
        // Signal the trust engine about the false skip.
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M16)
    snippet OLD <<'EOF'
        // Signal the trust engine about the false skip.
        if let podcastId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: podcastId)
        }
EOF
    snippet NEW <<'EOF'
        // Signal the trust engine about the false skip.
        if let podcastId, let trustService {
            await trustService.recordWeakFalseSkipSignal(podcastId: podcastId)
        }
EOF
    patch "$file" "$OLD" "$NEW" ;;

  M17)
    # Same defect as M13 for the durable receipt rather than the controller
    # sample; two sites, one mutation.
    snippet OLD <<'EOF'
            podcastId: podcastId,
            source: .listenRevert
EOF
    snippet NEW <<'EOF'
            podcastId: activePodcastId,
            source: .listenRevert
EOF
    patch "$file" "$OLD" "$NEW"
    snippet OLD <<'EOF'
                    podcastId: podcastId,
                    source: .manualVeto
                )
EOF
    snippet NEW <<'EOF'
                    podcastId: activePodcastId,
                    source: .manualVeto
                )
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
            schedulePostCommitCorrectionLearning(
                receipt,
                wasNewlyInserted: wasNewlyInserted
            )
            return true
EOF
    snippet NEW <<'EOF'
            schedulePostCommitCorrectionLearning(
                receipt,
                wasNewlyInserted: wasNewlyInserted
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
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: podcastId
        )
        if let podcastId {
EOF
    snippet NEW <<'EOF'
        guard activeEpisodeId == sourceEpisodeId,
              episodeLifecycleGeneration == sourceLifecycleGeneration
        else {
            return true
        }
        recordThresholdControlSignal(
            .falsePositive,
            podcastId: podcastId
        )
        if let podcastId {
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
            schedulePostCommitCorrectionLearning(
                receipt,
                wasNewlyInserted: wasNewlyInserted
            )
            return true
EOF
    snippet NEW <<'EOF'
            schedulePostCommitCorrectionLearning(
                receipt,
                wasNewlyInserted: wasNewlyInserted
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
    # BOTH sites, because the empty-show-id contract is enforced twice —
    # `recordThresholdControlSignal` refuses to call, and
    # `PerShowThresholdControllerStore.record` refuses to write. Deleting
    # either one alone is an EQUIVALENT MUTANT that no test can kill (verified:
    # each half survives on its own). What a test CAN rail is the contract, so
    # that is what this mutation removes. `$file` is the orchestrator; the
    # store is patched by absolute key below.
    snippet OLD <<'EOF'
        guard let podcastId, !podcastId.isEmpty else { return }
EOF
    snippet NEW <<'EOF'
        guard let podcastId else { return }
EOF
    patch "$file" "$OLD" "$NEW"
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
        recordThresholdControlSignal(.falsePositive, podcastId: podcastId)
EOF
    snippet NEW <<'EOF'
        // cannot silently discard valid old-episode feedback.
        recordThresholdControlSignal(.falsePositive, podcastId: activePodcastId)
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
      sed -n '2,60p' "$0" | sed 's/^# \{0,1\}//'
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
    restore_sources
    for rec in "${MEMBERS[@]}"; do
      echo "$(rec_name "$rec")|ERROR|anchor did not apply — source moved on" >>"$RESULTS"
    done
    FATAL=1
    continue
  fi

  if [ "$DRY_RUN" -eq 1 ]; then
    git --no-pager diff --stat -- "${MUTABLE_FILES[@]}"
    git --no-pager diff -U1 -- "${MUTABLE_FILES[@]}"
    restore_sources
    for rec in "${MEMBERS[@]}"; do
      echo "$(rec_name "$rec")|DRY-RUN|anchor applied" >>"$RESULTS"
    done
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
    restore_sources
    for rec in "${MEMBERS[@]}"; do
      echo "$(rec_name "$rec")|ERROR|batch did not build/run" >>"$RESULTS"
    done
    FATAL=1
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

  restore_sources
  echo
done

# ---------------------------------------------------------------------------
# Restoration must be byte-exact.
# ---------------------------------------------------------------------------
restore_sources
HASHES_AFTER=""
for f in "${MUTABLE_FILES[@]}"; do
  HASHES_AFTER="$HASHES_AFTER$(hash_of "$f")  $f
"
done
RESTORE_OK=1
if [ "$HASHES_BEFORE" != "$HASHES_AFTER" ]; then
  echo "mutation-battery: FATAL — a mutated file was NOT restored byte-exactly" >&2
  printf 'before:\n%s\nafter:\n%s\n' "$HASHES_BEFORE" "$HASHES_AFTER" >&2
  RESTORE_OK=0
fi

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
