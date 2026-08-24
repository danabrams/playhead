#!/usr/bin/env python3
"""playhead-voez — mutation battery for the gate baseline (R series).

WHY THIS IS NOT IN scripts/mutation-battery.sh
----------------------------------------------
It should have been, and the brief for this bead said so. It is not, because
that script is structurally a SWIFT battery: `MUTABLE_FILES` are Swift sources,
`apply_mutation` resolves a path from a Swift-file variable, and `run_focused`
is hard-wired to `scripts/fast-gate.sh -only-testing:…` — an xcodebuild run
whose verdict is read out of Swift Testing console glyphs. The unit under test
here is a Python module and a bash script, exercised by `python3 -m unittest` in
about a second. Bolting a second runner, a second failure extractor and a second
"did it run" extractor onto a 3,670-line script that is the repo's certification
tool, in order to host 23 rails that need none of its machinery, buys nothing and
risks the thing every other bead depends on.

`scripts/mutation-battery.sh` carries a pointer to this file so the R series is
discoverable from where the D/E/J/K/L/Q series live.

WHAT A RAIL IS
--------------
A mutation is a defect deliberately introduced into the source. KILLED means the
named test noticed. SURVIVED means the defect can be shipped with the suite
green — a coverage hole, and the entry stays until a test rejects it.

Every rail's expectation is verified to NAME A TEST THAT ACTUALLY RUNS AND
PASSES before anything is mutated. Two of playhead-djl0's rails named tests they
could not reach and reported SURVIVED against working code; that failure mode
reads as a coverage hole and sends the next person to write a test that already
exists, so it is checked on the free unmutated pass.

One rail (R99) is a VACUITY CONTROL: a cosmetic edit that must SURVIVE. If it is
killed, the battery is reddening on any edit at all and every other KILLED above
it is worthless.

    scripts/mutation-battery-gate-baseline.py            # run them all
    scripts/mutation-battery-gate-baseline.py --list
    scripts/mutation-battery-gate-baseline.py --only R01

Do not run this while a gate is running from the same worktree: it rewrites
scripts/fast-gate.sh in place, and bash reads a script incrementally.
"""

import argparse
import hashlib
import os
import pathlib
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
GB = "scripts/gate_baseline.py"
FG = "scripts/fast-gate.sh"
TF = "scripts/tests/test_gate_baseline.py"
SUITE = "scripts.tests.test_gate_baseline"

V = SUITE + ".VerdictTests."
P = SUITE + ".ParseSwiftTestingTests."
X = SUITE + ".ParseXCTestTests."
C = SUITE + ".CompletenessTests."
A = SUITE + ".AmbiguityTests."
K = SUITE + ".ClassificationTests."
M = SUITE + ".MergeTests."
CLI = SUITE + ".CLITests."
W = SUITE + ".FastGateWiringTests."
TC = SUITE + ".TierChangeTests."
T = SUITE + ".AcceptOutputTests."
CH = SUITE + ".CrashedHostVerdictTests."
AC = SUITE + ".ArmedCensusTests."
RC = SUITE + ".RealCrashedRunTests."
TR = SUITE + ".TruncatedOutcomeLineTests."
# playhead-phn3 — the fifth correction: a weld that reaches past line N+1,
# and a marker glyph severed mid-codepoint.
SS = SUITE + ".SpliceSpanningSeveralLinesTests."
SG = SUITE + ".SeveredMarkerGlyphTests."
GR = SUITE + ".GreenRunWithPhantomCasualtiesTests."
SP = SUITE + ".CrashedHostSafetyPropertyTests."
CM = SUITE + ".CensusMergeTests."
# playhead-t53a — the .xcresult as the verdict source.
XP = SUITE + ".XcresultParseTests."
XC = SUITE + ".CrashedCaseIsACasualtyTests."
XW = SUITE + ".BundleCannotWeakenTheCensusTests."
XM = SUITE + ".BundleMergeTests."
XB = SUITE + ".BlamedBlockAgainstTheBundleTests."
XR = SUITE + ".ResidualCommandTests."
XG = SUITE + ".FastGateBundleWiringTests."
# playhead-s34ux — a denied resource is not a failing test.
DC = SUITE + ".ResourceCauseTests."
DK = SUITE + ".ResourceConsoleParseTests."
DB = SUITE + ".ResourceBundleParseTests."
DV = SUITE + ".ResourceVerdictTests."
DA = SUITE + ".ResourceAcceptTests."


# name, file, description, old, new, expected-to-fail test ids
MUTATIONS = [
    (
        "R01", GB,
        "an unknown failing test is recorded as KNOWN instead of NEW "
        "(the whole point of the bead)",
        "            result.new_failures.append(key)\n            continue",
        "            result.known_failures.append(key)\n            continue",
        [V + "test_a_NEW_failure_fails_the_gate_and_is_NAMED",
         V + "test_a_NEW_xctest_failure_fails_the_gate"],
    ),
    (
        "R02", GB,
        "the failure KIND is ignored, so 'known to time out' licenses "
        "'known to fail its expectations' — the tolerance becomes a hole",
        "        unexpected = failure.kinds - known",
        "        unexpected = set()",
        [V + "test_a_load_sensitive_member_failing_a_DIFFERENT_WAY_is_NEW",
         V + "test_a_deterministic_member_failing_a_different_way_is_also_NEW"],
    ),
    (
        "R03", GB,
        "a deterministic member that PASSES is filed as a load-sensitive "
        "passer, so Dan's pass-direction arm never fires",
        "                result.deterministic_passed.append(key)",
        "                result.load_sensitive_passed.append(key)",
        [V + "test_a_DETERMINISTIC_baseline_member_that_PASSES_fails_the_gate"],
    ),
    (
        "R04", GB,
        "a baseline member that never ran is silently ignored",
        "            result.absent.append(key)",
        "            pass",
        [V + "test_a_baseline_member_that_did_not_RUN_fails_the_gate"],
    ),
    (
        "R05", GB,
        "a truncated log is judged anyway, so every test after the cut reads "
        "as 'did not run' or, worse, as green",
        "    if not run.complete:\n        result.cannot_evaluate = (",
        "    if False:\n        result.cannot_evaluate = (",
        [V + "test_an_INCOMPLETE_log_refuses_to_judge_rather_than_reporting_green"],
    ),
    (
        "R06", GB,
        "XCTest output stops being parsed at all — the exact shape that let "
        "playhead-ynmk (#313) merge unnoticed",
        # RE-ANCHORED at R2 review. tl6l added `skipped` as a third outcome and
        # made the duration optional, so this rail's anchor had been dead since
        # 40ac7dcc — and the battery REFUSES TO START on a drifted anchor, which
        # is how it is known that neither the implementer's round nor R1's ever
        # ran the committed battery. Both ran scratchpad ones instead.
        r'''    r"Test Case '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' (failed|passed|skipped)"''',
        r'''    r"Test Kase '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' (failed|passed|skipped)"''',
        [X + "test_xctest_failure_is_captured_fully_qualified",
         V + "test_a_NEW_xctest_failure_fails_the_gate"],
    ),
    (
        "R07", GB,
        "the slow-is-a-flake heuristic is applied to XCTest, where it inverts: "
        "a 3.5s assertion failure gets filed as a timeout",
        "                failure.kinds.add(KIND_ASSERTION)",
        "                failure.kinds.add(KIND_TIMEOUT if float(seconds) > 1.0 else KIND_ASSERTION)",
        [X + "test_a_SLOW_xctest_failure_is_still_an_assertion_never_a_flake"],
    ),
    (
        "R08", GB,
        "new failures no longer reach the exit code — the gate names them and "
        "exits 0 anyway",
        "        if (self.new_failures or self.kind_changed or self.deterministic_passed",
        "        if (self.kind_changed or self.deterministic_passed",
        [V + "test_a_NEW_failure_fails_the_gate_and_is_NAMED",
         W + "test_a_new_failure_makes_the_gate_exit_65_and_names_it"],
    ),
    (
        "R09", GB,
        "the promotion threshold is lowered, so a test starved once or twice "
        "becomes one whose passing fails the gate",
        "MIN_RUNS_FOR_DETERMINISTIC = 3",
        "MIN_RUNS_FOR_DETERMINISTIC = 1",
        [K + "test_a_single_observation_can_never_be_deterministic",
         M + "test_two_observations_are_NOT_enough_to_promote"],
    ),
    (
        "R09b", GB,
        "the threshold drops from 3 to 2 — the value the measured 0.46 Jaccard "
        "between two runs says is not enough evidence",
        "MIN_RUNS_FOR_DETERMINISTIC = 3",
        "MIN_RUNS_FOR_DETERMINISTIC = 2",
        [M + "test_two_observations_are_NOT_enough_to_promote"],
    ),
    (
        "R10", GB,
        "a colliding same-named PASS erases a real failure (Swift Testing's "
        "console line carries no suite, so keys genuinely collide)",
        "    run.passed -= set(failures)",
        "    run.passed = run.passed",
        [A + "test_a_name_that_both_passed_and_failed_counts_as_FAILED"],
    ),
    (
        "R11", GB,
        "both simulator attempts are read, so attempt 1's casualties union "
        "with attempt 2 and manufacture failures out of an artefact",
        '    return "".join(lines[cut:])',
        "    return text",
        [C + "test_only_the_last_attempt_is_read_after_a_sim_recovery"],
    ),
    (
        "R12", GB,
        "--accept-baseline records a truncated run, freezing a fragment as "
        "the definition of known-broken",
        '    if not run.complete:\n        raise CannotEvaluate(',
        '    if False:\n        raise CannotEvaluate(',
        [M + "test_merge_REFUSES_an_incomplete_log",
         CLI + "test_accept_refuses_an_incomplete_log_and_writes_nothing"],
    ),
    (
        "R13", GB,
        "a test that has failed in no observation is kept, so the baseline "
        "only ever grows",
        '        if entry["failed_runs"] == 0:\n            continue',
        '        if False:\n            continue',
        [M + "test_merge_DROPS_a_test_that_has_never_failed_in_any_observation"],
    ),
    (
        "R14", GB,
        "a renamed or deleted test is kept in the baseline, where it reads as "
        "ABSENT forever and the gate can never go green",
        # RE-ANCHORED at R2 review, same cause as R06: tl6l inserted the
        # carry-forward clause inside this branch and the anchor died with it.
        "        if not reached:\n            if key in protected:",
        "        if False:\n            if key in protected:",
        [M + "test_merge_DROPS_a_baseline_member_that_no_longer_exists"],
    ),
    (
        "R15", GB,
        "every Swift Testing failure is classed a timeout, so an assertion "
        "regression in a known-flaky test is absorbed",
        "    return KIND_TIMEOUT if _TIME_LIMIT in message else KIND_ASSERTION",
        "    return KIND_TIMEOUT",
        [P + "test_expectation_failure_is_an_assertion_not_a_timeout",
         V + "test_a_load_sensitive_member_failing_a_DIFFERENT_WAY_is_NEW"],
    ),
    (
        "R16", GB,
        "kinds are REPLACED rather than unioned on refresh, so accepting a "
        "quiet run narrows an entry that legitimately fails two ways",
        '            entry["kinds"] = sorted(set(previous.get("kinds", [])) | failure.kinds)',
        '            entry["kinds"] = sorted(failure.kinds)',
        [M + "test_merge_unions_kinds_rather_than_replacing_them"],
    ),
    (
        "R17", GB,
        "a fully green run against a non-empty baseline is accepted as normal",
        "    if entries and not run.failures:",
        "    if False:",
        [V + "test_a_fully_GREEN_run_against_a_nonempty_baseline_is_FICTION"],
    ),
    (
        "R18", GB,
        "the glyph patterns anchor at the line start, so \\r-overwritten "
        "prefixes hide a failure",
        "        m = _ST_FAIL_NAMED.search(line) or _ST_FAIL_FUNC.search(line)",
        "        m = _ST_FAIL_NAMED.match(line) or _ST_FAIL_FUNC.match(line)",
        # SURVIVED on first run against
        # test_carriage_return_junk_before_the_glyph_does_not_hide_a_failure,
        # because that fixture also carries an issue line and the issue line
        # alone creates the failure. The rail is unchanged; the test that had to
        # exist is the one where the fail line is the only evidence.
        [P + "test_a_failure_evidenced_ONLY_by_its_failed_after_line_survives_junk"],
    ),
    (
        "R24", GB,
        "the PASS patterns anchor at the line start, so a \\r-overwritten pass "
        "is invisible and every baseline member that passed reads as ABSENT",
        "        m = _ST_PASS_NAMED.search(line) or _ST_PASS_FUNC.search(line)",
        "        m = _ST_PASS_NAMED.match(line) or _ST_PASS_FUNC.match(line)",
        [P + "test_carriage_return_junk_does_not_hide_a_PASS_either"],
    ),
    (
        "R19", FG,
        "a verdict failure is laundered into exit 0 by the shell",
        "  1) finish 65 ;;",
        "  1) finish 0 ;;",
        [W + "test_a_new_failure_makes_the_gate_exit_65_and_names_it"],
    ),
    (
        "R20", FG,
        "a build failure the check could not evaluate is reported as a pass",
        '  *) [ "$RC" -eq 0 ] && finish 0; finish "$RC" ;;',
        "  *) finish 0 ;;",
        [W + "test_a_build_failure_is_not_laundered_into_a_pass"],
    ),
    (
        "R21", FG,
        "the baseline check is applied to selective runs too, so "
        "mutation-battery.sh's focused failures get absorbed as 'known'",
        'if [ "$SELECTIVE" -eq 1 ]; then\n  echo "fast-gate: baseline check SKIPPED',
        'if [ 0 -eq 1 ]; then\n  echo "fast-gate: baseline check SKIPPED',
        [W + "test_a_selective_run_passes_the_raw_exit_code_through"],
    ),
    (
        "R22", FG,
        "--accept-baseline is forwarded to xcodebuild, which rejects it",
        "    --accept-baseline) ACCEPT_BASELINE=1 ;;",
        '    --accept-baseline) ACCEPT_BASELINE=1; FORWARD+=("$arg") ;;',
        [W + "test_accept_baseline_is_never_forwarded_to_xcodebuild"],
    ),
    (
        "R23", FG,
        "--accept-baseline on a selective run is allowed, deleting every "
        "baseline entry the filter excluded",
        '  if [ "$SELECTIVE" -eq 1 ]; then\n    echo "fast-gate: --accept-baseline REFUSED',
        '  if [ 0 -eq 1 ]; then\n    echo "fast-gate: --accept-baseline REFUSED',
        [W + "test_accept_baseline_REFUSES_a_selective_run"],
    ),
    (
        "R25", GB,
        "--accept-baseline records a run that executed ZERO tests, deleting "
        "every entry as unreachable — the file destroyed by its own maintenance "
        "command (measured live: a sim erase sends xcodebuild to the clone "
        "helper, which cannot find simctl, and it reports TEST FAILED after "
        "nothing ran)",
        "    if not run.ran:\n        raise CannotEvaluate(",
        "    if False:\n        raise CannotEvaluate(",
        # SURVIVED first time against
        # test_merge_REFUSES_a_run_that_executed_no_tests_at_all: with a
        # populated baseline R26's too-few-reached guard refuses anyway, so the
        # rail was masked. Point it at the case only this guard can catch — the
        # first ever accept, when there is no baseline to compare against.
        [M + "test_merge_REFUSES_an_empty_run_even_with_NO_baseline_to_compare"],
    ),
    (
        "R26", GB,
        "--accept-baseline records a run that reached almost none of the "
        "baseline, silently dropping the rest",
        "        if reached * 2 < len(existing):",
        "        if False:",
        [M + "test_merge_REFUSES_a_run_that_reached_too_little_of_the_baseline"],
    ),
    (
        "R27", GB,
        "a run with zero failures is called GREEN even when it executed nothing "
        "— zero failures is not zero problems",
        # RE-ANCHORED at R2 review, same cause as R06 and R14: tl6l added
        # `and not self.crashed_host` to this condition.
        "        if self.ok and self.total_failures == 0 and not self.crashed_host:",
        "        if self.total_failures == 0:",
        [V + "test_a_run_that_executed_NOTHING_is_never_called_GREEN"],
    ),
    # ---- playhead-26od R5: an accept has to SAY what it accepted ----
    #
    # Both of these defects shipped once. The accept that carried this bead's
    # third observation added 28 entries and its commit message called them "all
    # timeouts" — three were assertion-only and a fourth mixed — and the same
    # accept crossed fifteen entries into `deterministic`, arming a hard failure
    # on each of them, in silence. Neither is a policy question; both were
    # unsayable because the tool printed only membership.
    (
        "R28", GB,
        "a tier PROMOTION goes unannounced, so an accept arms the "
        "pass-direction check on N entries and says nothing about any of them",
        '        if promoted:\n            print(\n                "  ARMED:',
        '        if False:\n            print(\n                "  ARMED:',
        [T + "test_a_promotion_is_ANNOUNCED_and_named"],
    ),
    (
        "R29", GB,
        "every deterministic entry is re-announced as a promotion, not just the "
        "ones that CHANGED — the loud line stops being read",
        "        if now == TIER_DETERMINISTIC and before != TIER_DETERMINISTIC:",
        "        if now == TIER_DETERMINISTIC:",
        [TC + "test_an_ALREADY_deterministic_entry_is_not_re_announced"],
    ),
    (
        "R30", GB,
        "the added entries lose their KIND, which is exactly the state in which "
        "28 entries were summarised as 'all timeouts'",
        '            print("  + [%s] %s" % (_kinds_label(merged["tests"][key]), key))',
        '            print("  + %s" % key)',
        [T + "test_every_added_entry_carries_its_KIND",
         W + "test_accept_baseline_writes_the_file"],
    ),
    (
        "R31", FG,
        "the disk preflight is dropped from the gate, so a run that should "
        "refuse at exit 28 goes on to wedge with no output instead",
        '  if ! python3 scripts/disk_preflight.py "${PREFLIGHT_ARGS[@]}"; then',
        '  if ! true "${PREFLIGHT_ARGS[@]}"; then',
        [W + "test_the_disk_preflight_runs_BEFORE_xcodebuild"],
    ),
    # ---- playhead-tl6l R2: the splice repair, and the armed census ----
    #
    # These rails are in THIS FILE rather than in a scratchpad script, which is
    # the point. tl6l's implementer ran a 17-rail battery and R1 review ran a
    # 37-rail one; neither is committed, so neither can be re-run and neither
    # protects the next edit. A rail that exists only in a session directory is
    # the same defect as a test that skips itself — see playhead-fer3.
    (
        "RC1", GB,
        "the app-log splice is not repaired, so a verdict line cut in half "
        "reads as silence — 18 passing tests on main become crashed-host "
        "casualties and the census reads 30 where the truth is 11",
        "    for line in rejoin_spliced_lines(last_attempt(text).splitlines()):",
        "    for line in last_attempt(text).splitlines():",
        [RC + "test_the_main_control_run",
         RC + "test_BOTH_runs_lost_THE_SAME_ELEVEN_TESTS",
         RC + "test_the_spliced_verdict_lines_are_read_as_PASSES"],
    ),
    (
        "RC2", GB,
        "the repair swallows a line that is already a whole record, gluing two "
        "severed lines for one test into a phantom named "
        "`victim\" recorded an iss<glyph> Test \"victim`",
        # RE-ANCHORED at playhead-phn3. The condition moved into
        # `_displaced_tail_span` when the weld learned to reach past line
        # N+1; the EDIT moved, the defect and the expectation did not. An
        # anchor that no longer applies is a LOST rail, not a passing one.
        "        if _parses_as_a_test_line(following):\n"
        "            return None\n",
        "        if False:\n"
        "            return None\n",
        [TR + "test_a_truncated_FAIL_line_is_still_a_FAILURE"],
    ),
    (
        "RC3", GB,
        "an INTACT verdict line is rewritten anyway, so it absorbs the next "
        "line — a pass followed by the restart banner is claimed by the restart "
        "handler and the test that passed becomes a casualty",
        # RE-ANCHORED at playhead-phn3: the condition now ends the `if`
        # rather than leading into a third clause, so the old text is gone
        # from the file. Same defect, same expectation.
        "                    and not _parses_as_a_test_line(head)):\n",
        "                    and True):\n",
        [TR + "test_an_INTACT_verdict_followed_by_app_output_is_NOT_rewritten"],
    ),
    (
        "RC4", GB,
        "the reconstruction is assembled tail-first, so the recovered name is "
        "garbage in a way no assertion about counts alone would notice",
        "                out.append(head + following)",
        "                out.append(following + head)",
        [TR + "test_the_reconstruction_is_HEAD_then_TAIL_and_not_the_other_way",
         RC + "test_BOTH_runs_lost_THE_SAME_ELEVEN_TESTS"],
    ),
    (
        "RC5", GB,
        "the displaced app output is dropped rather than kept, so anything that "
        "landed after the intrusion on that line — including xcodebuild's own "
        "restart banner — is lost with it",
        "                out.append(line[match.start():])",
        "                pass",
        [TR + "test_the_DISPLACED_app_output_is_kept_and_still_scanned"],
    ),
    (
        "RA1", GB,
        "NO VERDICT leaves the exit code again, so a change that CRASHES the "
        "test host exits 0 — its victims are healthy tests in nobody's "
        "baseline, and the crash destroys the evidence of itself",
        "                or (self.new_casualties and self.census_armed)",
        "                or False",
        [AC + "test_a_casualty_NOT_in_the_record_FAILS_the_gate",
         AC + "test_the_SAME_COUNT_with_a_DIFFERENT_NAME_still_fails",
         AC + "test_a_RECORDED_EMPTY_census_is_a_CLAIM_and_is_armed",
         AC + "test_the_THIRD_observation_ARMS_it"],
    ),
    (
        "RA2", GB,
        "an UNRECORDED census is read as a recorded ZERO, so the arm fires on "
        "main today for a pre-existing crash nobody in an unrelated bead can "
        "fix — the gate everyone learns to route around",
        "    if value is None:\n        return None",
        "    if value is None:\n        return Census(MIN_RUNS_FOR_DETERMINISTIC)",
        [CH + "test_it_is_REPORTED_but_does_NOT_by_itself_fail_the_gate",
         AC + "test_an_UNRECORDED_census_is_inert_even_for_a_LARGE_loss",
         AC + "test_a_FRESH_baseline_records_NOTHING_rather_than_a_ZERO"],
    ),
    # ---- playhead-tl6l R4: the record is a UNION WITH COUNTS ----
    #
    # RA3 used to assert the OPPOSITE — that the census is replaced, not
    # unioned — on the strength of two runs whose casualty sets were identical.
    # A third full-plan run lost fifteen where those two lost eleven, and under
    # replace an armed gate on such a night exits 65 for four healthy tests
    # nobody can do anything about. The rail is inverted, not deleted: what it
    # protects now is that an accept RECORDS an observation instead of
    # overwriting the record with a snapshot.
    (
        "RA3", GB,
        "the census is REPLACED by each run's snapshot instead of unioned with "
        "counts, so one loud night manufactures NEW casualties out of healthy "
        "tests and one quiet night forgets eleven measured ones",
        "        NO_VERDICT_KEY: merge_census(recorded_census(baseline), run).to_json(),",
        "        NO_VERDICT_KEY: Census(1, {key: {\"seen_runs\": 1, \"lost_runs\": 1}\n"
        "                                  for key in run.no_verdict}).to_json(),",
        [AC + "test_accept_UNIONS_the_census_and_COUNTS_the_observations",
         AC + "test_accept_on_a_CLEAN_run_CREDITS_an_observation_not_an_erasure",
         RC + "test_the_THREE_runs_PROMOTE_ELEVEN_and_leave_FOUR_load_sensitive"],
    ),
    (
        "RA8", GB,
        "the census tier is read off the FAILURE numerator, so `lost_runs` is "
        "never consulted and every entry with enough observations reads as "
        "deterministic — arming a hard failure on names that were only ever "
        "lost once",
        '    return _tier(entry["seen_runs"], entry["lost_runs"])',
        '    return _tier(entry["seen_runs"], entry["seen_runs"])',
        [AC + "test_a_LOAD_SENSITIVE_entry_that_reports_again_is_NOT_fatal",
         RC + "test_the_THREE_runs_PROMOTE_ELEVEN_and_leave_FOUR_load_sensitive"],
    ),
    (
        "RA9", GB,
        "a DETERMINISTIC census entry that reports again is no longer fatal, so "
        "a crash that is genuinely fixed stays recorded forever and the gate "
        "never speaks about those names again — R2's whole objection to a "
        "union, left unanswered",
        "                or self.census_now_reports):",
        "                or False):",
        [AC + "test_a_DETERMINISTIC_entry_that_REPORTS_AGAIN_fails_the_gate"],
    ),
    (
        "RA10", GB,
        "the pass-direction arm fires on a recorded name that never STARTED, so "
        "a rename — or a host that died before the test got there — reads as "
        "proof the crash is fixed and hard-fails the gate",
        "            if key in result.census_started",
        "            if True",
        [AC + "test_a_recorded_name_that_NEVER_STARTED_is_never_fatal"],
    ),
    (
        "RA11", GB,
        "a crashed run PRUNES the census entries it silenced, so the crash "
        "shrinks the record from inside `accept` — the same defect `protected` "
        "closes for `tests`, one layer down, and the shrinking diff reads as "
        "good news",
        "        elif crashed:\n            tests[key] = dict(previous)",
        "        elif False:\n            tests[key] = dict(previous)",
        [CM + "test_a_CRASHED_run_carries_an_unreached_census_entry_forward",
         CM + "test_a_crash_can_never_SHRINK_the_census_across_repeated_accepts"],
    ),
    (
        "RA12", GB,
        "a name that started and REPORTED is credited a LOST observation "
        "anyway, so nothing can ever demote and the union really does become a "
        "licence nobody can revoke",
        '        elif key in run.started:\n            tests[key] = {"seen_runs": previous["seen_runs"] + 1,\n'
        '                          "lost_runs": previous["lost_runs"]}',
        '        elif key in run.started:\n            tests[key] = {"seen_runs": previous["seen_runs"] + 1,\n'
        '                          "lost_runs": previous["lost_runs"] + 1}',
        [AC + "test_a_recorded_name_that_REPORTS_is_DEMOTED_not_deleted",
         RC + "test_the_THREE_runs_PROMOTE_ELEVEN_and_leave_FOUR_load_sensitive"],
    ),
    (
        "RA13", GB,
        "the census borrows the FILE's observation count instead of its own, so "
        "a record with one observation of the census arrives pre-armed off nine "
        "observations of something else",
        "    return Census(value.get(CENSUS_RUNS_KEY, 0), value.get(CENSUS_TESTS_KEY, {}))",
        "    return Census(baseline.get(\"runs_observed\", 0), "
        "value.get(CENSUS_TESTS_KEY, {}))",
        [CM + "test_the_census_OBSERVATION_COUNT_is_its_own_and_not_the_files"],
    ),
    (
        "RA14", GB,
        "a census tier PROMOTION goes unannounced, so an accept arms a hard "
        "failure on N names and says nothing about any of them — exactly the "
        "defect playhead-26od R5 closed for the failure tiers",
        "        if promoted_c:",
        "        if False:",
        [CM + "test_the_accept_ANNOUNCES_a_census_promotion_and_the_ARMING"],
    ),
    (
        # playhead-o89d R1 found and fixed this one; it had unit rails but no
        # mutation rail, so the battery could not tell you whether they bite.
        "RA15", GB,
        "a tier DEMOTION goes unannounced, so an accept REVOKES a hard-failure "
        "licence in silence — the same defect as RA14 pointed the other way, and "
        "the direction that makes the gate LOOSER",
        "        if demoted:",
        "        if False:",
        [T + "test_a_demotion_is_ANNOUNCED_and_named"],
    ),
    (
        # playhead-o89d R2. RA14's and RA15's twin, one layer down.
        "RA16", GB,
        "a CENSUS tier demotion goes unannounced, so the one census event that "
        "revokes a hard-failure licence is rendered in the same words as the one "
        "that revokes nothing (`~= reported again`), and the tier is left to the "
        "reader's arithmetic",
        "        if demoted_c:",
        "        if False:",
        [T + "test_a_CENSUS_demotion_is_ANNOUNCED_and_named"],
    ),
    (
        # The other half of RA16: a banner that fires for every returning
        # casualty says nothing, because most of them are good news.
        "RA17", GB,
        "the census demotion banner fires for LOAD-SENSITIVE entries too, so a "
        "casualty coming back — which is good news and costs nobody a licence — "
        "is announced as a revocation, and the loud line stops meaning anything",
        "        elif was_deterministic(key) and not now:",
        "        elif key in before and not now:",
        [T + "test_an_accept_that_demotes_NO_CENSUS_ENTRY_stays_quiet"],
    ),
    (
        # playhead-o89d R3. RA16/RA17 gave the two DEMOTIONS distinct spellings
        # and the module docstring wrote the rule down as "neither side can be
        # read as the other" — for the direction that LOOSENS only. The two
        # PROMOTIONS were both `ARMED:`, and this mutant proved they were
        # interchangeable: it survived the whole suite while telling the operator
        # that a crashed-host name's PASSING is what becomes fatal.
        "RA18", GB,
        "the CENSUS promotion banner is spelled exactly like the `tests` one, so "
        "the accept that arms a hard failure on a crashed-host name is indis"
        "tinguishable from the one that arms it on a recorded failure — and "
        "states the wrong consequence for the record it belongs to",
        '                "  CENSUS ARMED: %d census entr%s crossed into DETERMINISTIC — lost "\n'
        '                "their verdict in every one of their observations. Each of these "\n'
        '                "REPORTING AGAIN now fails the gate, which is what lets this record "\n'
        '                "shrink when the crash is fixed."',
        '                "  ARMED: %d entr%s crossed into DETERMINISTIC — failed in every one "\n'
        '                "of their observations. Each of these PASSING now fails the gate, so "\n'
        '                "say in the commit message why that is the right reading."',
        [CM + "test_the_accept_ANNOUNCES_a_census_promotion_and_the_ARMING",
         T + "test_a_promotion_is_ANNOUNCED_and_named"],
    ),
    (
        # The half of RA18 that survives a rename: keep the prefix, lie about
        # the event. For a census entry it is REPORTING AT ALL that is fatal —
        # pass OR fail — so an operator told `PASSES` concludes a failing report
        # is safe, which is the one thing the census arm exists to deny.
        "RA19", GB,
        "the census promotion banner names PASSING as the fatal event, when what "
        "is fatal for a crashed-host name is REPORTING AT ALL — the operator "
        "signs a commit message claiming a licence narrower than the one armed",
        '                "REPORTING AGAIN now fails the gate, which is what lets this record "',
        '                "PASSING now fails the gate, which is what lets this record "',
        [CM + "test_the_accept_ANNOUNCES_a_census_promotion_and_the_ARMING"],
    ),
    # ------------------------------------------------------------------------
    # playhead-o89d R4. R3 closed the two PROMOTION banners and wrote the rule
    # down as "the SPELLING and the EVENT it names". Every OTHER banner in the
    # accept path was then mutated one at a time against the whole suite: 28
    # mutants, 15 survivors. R3's own fix had been applied to the direction R3
    # was looking at, exactly as R2's had — the fourth round in a row on that
    # shape. The rails below are the survivors that misstate a FACT or a
    # CONSEQUENCE. Three survivors are deliberately NOT railed and are named in
    # the module docstring instead: they swap only the leading GLYPH between the
    # two records' detail lines, whose words and `[kind]` label still
    # discriminate, and pinning a glyph is taste rather than a defect.
    # ------------------------------------------------------------------------
    (
        # The sharpest one: this banner was RENAMED by R3, in the commit that
        # stated the spelling-and-event rule, and only its spelling was pinned.
        "RA20", GB,
        "the record-level census arm states the OPPOSITE rule — the operator "
        "signs a commit message saying it is a casualty IN the record that now "
        "fails the gate, which is the one thing the arm does not do",
        '"a test that loses its verdict and is NOT in the record fails the gate."',
        '"a test that loses its verdict and IS in the record fails the gate."',
        [CM + "test_the_accept_ANNOUNCES_a_census_promotion_and_the_ARMING"],
    ),
    (
        "RA21", GB,
        "the `tests` demotion says accepting KEEPS the licence and the next pass "
        "is STILL fatal — the gate got LOOSER and the line says it did not",
        '"revokes that licence: from here the entry is load-sensitive and its "\n'
        '                "next pass is NOT fatal. Say in the commit message why the RECORD was "',
        '"KEEPS that licence: from here the entry is deterministic and its "\n'
        '                "next pass is STILL fatal. Say in the commit message why the RECORD was "',
        [T + "test_a_demotion_is_ANNOUNCED_and_named"],
    ),
    (
        "RA22", GB,
        "the `tests` demotion names the CENSUS event (`REPORTS AGAIN`) as what "
        "hard-failed the gate, so the operator looks for a report where there "
        "was a pass",
        '"failing in every one of its observations and this run watched it "\n'
        '                "PASS, which is what hard-failed the gate (`NOW PASSES`). Accepting "',
        '"failing in every one of its observations and this run watched it "\n'
        '                "REPORT, which is what hard-failed the gate (`REPORTS AGAIN`). Accepting "',
        [T + "test_a_demotion_is_ANNOUNCED_and_named"],
    ),
    (
        "RA23", GB,
        "the CENSUS demotion says accepting KEEPS the licence and the next "
        "report is STILL fatal — on the only event that ever shrinks the record",
        '"gate (`REPORTS AGAIN`). Accepting revokes that licence: from here the "\n'
        '                "entry is load-sensitive and its next report is NOT fatal. That is also "',
        '"gate (`REPORTS AGAIN`). Accepting KEEPS that licence: from here the "\n'
        '                "entry is deterministic and its next report is STILL fatal. That is also "',
        [T + "test_a_CENSUS_demotion_is_ANNOUNCED_and_named"],
    ),
    (
        # R2's `CENSUS DISARMED:` defect one layer down, in the membership lines
        # under the tier banners: a name that has NEVER lost its verdict before
        # rendered in the words of one that has.
        "RA24", GB,
        "a name losing its verdict for the FIRST time is announced as a name "
        "that has lost it before — a new casualty read as a recurrence, which "
        "is the same 'one line, two opposite meanings' RA16 closed above it",
        'print("  ~+ NOW LOSES ITS VERDICT  %s" % key)',
        'print("  ~= reported again          %s" % key)',
        [CM + "test_the_accept_NAMES_a_census_entry_that_lost_its_verdict_for_"
              "the_FIRST_time"],
    ),
    (
        "RA25", GB,
        "the census size transition is printed backwards, so a record that GREW "
        "reads as one that shrank — and shrinking is the direction this record "
        "is not allowed to take quietly",
        "% (len(was.tests), len(now.tests), now.runs_observed)",
        "% (len(now.tests), len(was.tests), now.runs_observed)",
        [CM + "test_the_accept_NAMES_a_census_entry_that_lost_its_verdict_for_"
              "the_FIRST_time",
         CM + "test_the_accept_NAMES_a_census_entry_the_prune_DROPPED"],
    ),
    (
        "RA26", GB,
        "the PRUNE — a recorded name nobody could reach on a healthy run, i.e. "
        "renamed, deleted or newly skipped — is announced as a recovery",
        'print("  ~- dropped (never started this run — renamed, deleted or "\n'
        '                      "skipped)  %s" % key)',
        'print("  ~- recovered (started and reported this run)  %s" % key)',
        [CM + "test_the_accept_NAMES_a_census_entry_the_prune_DROPPED"],
    ),
    (
        # The fourth time this particular number has been wrong; the first three
        # are in CLAUDE.md and every one was the same defect class.
        "RA27", GB,
        "the accept's NO VERDICT headline counts the CARRIED-FORWARD entries "
        "instead of the casualties, so a run that lost tests can report that it "
        "lost none",
        '"observation says nothing about them."\n'
        '                % (len(no_verdict),',
        '"observation says nothing about them."\n'
        '                % (len(protected),',
        [T + "test_an_accept_over_a_crashed_run_SAYS_SO"],
    ),
    (
        "RA28", GB,
        "the first-ever census is announced as ALREADY fatal for an unrecorded "
        "casualty, when one observation is PROVISIONAL — the ladder CLAUDE.md "
        "records as what let the arming land without turning main red",
        '"not fatal. Say in the commit message why this loss is the "',
        '"already fatal. Say in the commit message why this loss is the "',
        [T + "test_an_accept_over_a_crashed_run_SAYS_SO"],
    ),
    (
        # `+ [kind]` prints only for names ENTERING the file, and a promotion is
        # by construction a name already in it — so this line is the only place
        # a promoted entry's kind is ever shown.
        "RA29", GB,
        "the promotion detail reports a constant KIND, so the tolerance the "
        "accept is justified by cannot be checked against the entry it arms",
        'print("  ! now deterministic [%s] %d/%d  %s" % (\n'
        '                    _kinds_label(entry), entry["failed_runs"], entry["seen_runs"], key,',
        'print("  ! now deterministic [%s] %d/%d  %s" % (\n'
        '                    "timeout", entry["failed_runs"], entry["seen_runs"], key,',
        [T + "test_a_promotion_detail_carries_the_ENTRYS_OWN_kind"],
    ),
    (
        "RA30", GB,
        "the accept header prints the entry count as the observation count and "
        "vice versa — the numerator and denominator of every tier decision in "
        "the file, interchangeable",
        '% (merged["plan"], merged["runs_observed"], len(merged["tests"])))',
        '% (merged["plan"], len(merged["tests"]), merged["runs_observed"]))',
        [T + "test_a_promotion_is_ANNOUNCED_and_named"],
    ),
    # ------------------------------------------------------------------------
    # playhead-o89d R5. The whole accept path was enumerated a SECOND time,
    # independently (69 mutants, not R4's 28), and the round found two things.
    #
    # First, the OMISSION: the accept had NO line at all for a KIND WIDENING —
    # the fifth event that makes the gate looser, and the only one no mutant of
    # an existing line can reach. RA31-RA35 pin it.
    #
    # Second, R4's own fixes were one-directional in exactly the way R1's, R2's
    # and R3's were. Three of its new rails have mirrors it did not write:
    # RA29 pinned the PROMOTION detail's kind and not the DEMOTION's (RA36);
    # RA22 pinned `DISARMED:`'s event and not `CENSUS DISARMED:`'s (RA37);
    # RA27 pinned the crash headline's count and not `CARRIED FORWARD:`'s
    # (RA38). RA39-RA44 are the remaining survivors that misstate a fact or a
    # consequence.
    # ------------------------------------------------------------------------
    (
        "RA31", GB,
        "a KIND WIDENING goes unannounced, so an accept doubles what an entry "
        "absorbs — `known to time out` becomes `known to fail at all` — and the "
        "transcript says `(membership unchanged; counts updated)`. The fifth "
        "loosening event, and the one four rounds could not see because it is an "
        "omission rather than a wrong line",
        "        if widened:\n            print(\n"
        '                "  TOLERANCE WIDENED:',
        "        if False:\n            print(\n"
        '                "  TOLERANCE WIDENED:',
        [T + "test_a_WIDENED_KIND_is_ANNOUNCED_with_both_kinds_and_the_consequence"],
    ),
    (
        "RA32", GB,
        "the widening detail prints the entry's NEW kind as its OLD one, so the "
        "line shows no change on the one event whose whole content is the change",
        'print("  ± [%s -> %s]  %s" % (before, after, key))',
        'print("  ± [%s -> %s]  %s" % (after, after, key))',
        [T + "test_a_WIDENED_KIND_is_ANNOUNCED_with_both_kinds_and_the_consequence"],
    ),
    (
        "RA33", GB,
        "the widening banner states the opposite consequence — that the new kind "
        "is STILL reported NEW — so the operator signs a commit message claiming "
        "the tolerance did not move",
        '"the gate; from here that kind is absorbed as KNOWN and is no longer "\n'
        '                "reported NEW.',
        '"the gate; from here that kind is still treated as UNKNOWN and is "\n'
        '                "reported NEW.',
        [T + "test_a_WIDENED_KIND_is_ANNOUNCED_with_both_kinds_and_the_consequence"],
    ),
    (
        "RA34", GB,
        "the widening banner fires for every recorded entry that failed at all, "
        "so a line that should mark the rare tolerance change fires on most of "
        "the file and stops being read — the failure mode RA17 closed for the "
        "census demotion",
        "        if after > before:",
        "        if after >= before:",
        [T + "test_an_accept_that_widens_NO_KIND_stays_quiet"],
    ),
    (
        "RA35", GB,
        "the accept claims `(membership unchanged; counts updated)` on a run that "
        "widened a KIND — a positive claim that only counts moved, made about the "
        "one change that is not a count",
        "        if not added and not removed and not widened:",
        "        if not added and not removed:",
        [T + "test_a_WIDENED_KIND_is_ANNOUNCED_with_both_kinds_and_the_consequence"],
    ),
    (
        "RA36", GB,
        "the DEMOTION detail reports a constant KIND — R4 closed exactly this on "
        "the promotion side (RA29) with the argument that a promoted entry is not "
        "in the `added` set, and every word of it is true of a demoted one",
        'print("  ~ no longer deterministic [%s] %d/%d  %s" % (\n'
        '                _kinds_label(entry), entry["failed_runs"], entry["seen_runs"], key,',
        'print("  ~ no longer deterministic [%s] %d/%d  %s" % (\n'
        '                "timeout", entry["failed_runs"], entry["seen_runs"], key,',
        [T + "test_a_demotion_detail_carries_the_ENTRYS_OWN_kind"],
    ),
    (
        "RA37", GB,
        "the CENSUS demotion names the `tests` event (`NOW PASSES`) as what "
        "hard-failed the gate — the exact mirror of RA22, which R4 wrote for the "
        "`tests` demotion naming the census event. For a crashed-host name it is "
        "REPORTING AT ALL that is fatal, so the operator is sent looking for a "
        "pass that never happened",
        '"this run watched it START AND REPORT, which is what hard-failed the "\n'
        '                "gate (`REPORTS AGAIN`).',
        '"this run watched it PASS, which is what hard-failed the "\n'
        '                "gate (`NOW PASSES`).',
        [T + "test_a_CENSUS_demotion_is_ANNOUNCED_and_named"],
    ),
    (
        "RA38", GB,
        "CARRIED FORWARD counts the whole casualty set instead of the recorded "
        "entries it protected, so an accept that carried one entry through a "
        "twelve-test crash reports twelve — RA27's defect pointed the other way, "
        "and the fifth time this number class has been wrong",
        '                    % (len(protected), "y was" if len(protected) == 1 else "ies were")',
        '                    % (len(no_verdict), "y was" if len(protected) == 1 else "ies were")',
        [T + "test_an_accept_ANNOUNCES_what_it_carried_forward"],
    ),
    (
        "RA39", GB,
        "the crash headline claims the observation CONFIRMS the lost tests are "
        "still broken, when a test whose host died was never judged at all — the "
        "one sentence that states why a lost verdict is not evidence",
        '"observation says nothing about them."',
        '"observation confirms they are still broken."',
        [T + "test_an_accept_over_a_crashed_run_SAYS_SO"],
    ),
    (
        "RA40", GB,
        "the parenthetical reports the CASUALTY count as the number of host "
        "restarts, so one restart that cost twelve verdicts reads as twelve "
        "restarts — two quantities that were interchangeable while both were 1",
        '" (xcodebuild restarted the test host %d time(s))" % run.host_restarts',
        '" (xcodebuild restarted the test host %d time(s))" % len(no_verdict)',
        [T + "test_an_accept_ANNOUNCES_what_it_carried_forward"],
    ),
    (
        "RA41", GB,
        "CARRIED FORWARD states the opposite reason — that a crash IS a rename "
        "and keeping the entries is what makes the file grow — recommending the "
        "prune the line exists to forbid",
        '"pruned — a crash is not a rename, and dropping them here is how "\n'
        '                    "the file would shrink without anyone deciding to shrink it."',
        '"pruned — a crash IS a rename, and keeping them here is how "\n'
        '                    "the file would grow without anyone deciding to grow it."',
        [T + "test_an_accept_ANNOUNCES_what_it_carried_forward"],
    ),
    (
        "RA42", GB,
        "the CENSUS promotion states its consequence backwards — that arming a "
        "name is why the record can NEVER shrink — which is R2's objection to a "
        "union stated as though the design agreed with it",
        '"REPORTING AGAIN now fails the gate, which is what lets this record "\n'
        '                "shrink when the crash is fixed."',
        '"REPORTING AGAIN now fails the gate, which is why this record can never "\n'
        '                "shrink once the crash is fixed."',
        [CM + "test_the_accept_ANNOUNCES_a_census_promotion_and_the_ARMING"],
    ),
    (
        "RA43", GB,
        "a REMOVAL is rendered as an ADDITION — opposite claims about the file "
        "on one line, the shape RA24 closed on the census side",
        '            print("  - %s" % key)',
        '            print("  + %s" % key)',
        [T + "test_a_REMOVAL_is_named_and_is_not_rendered_as_an_addition"],
    ),
    (
        "RA44", GB,
        "the removal lines are suppressed, so the only notice that a recorded "
        "name is no longer known-broken — i.e. that its next failure will be "
        "reported NEW — is gone",
        "        for key in removed:\n            print(",
        "        for key in []:\n            print(",
        [T + "test_a_REMOVAL_is_named_and_is_not_rendered_as_an_addition"],
    ),
    (
        "RA45", GB,
        "the CARRIED FORWARD listing's `… and N more` reports the WHOLE set "
        "instead of the remainder, so ten names are listed and the reader is "
        "told there are twelve more — the count class CLAUDE.md records being "
        "wrong four times, in the one place it cannot be checked without "
        "counting the lines above it",
        '                    print("  = … and %d more" % (len(protected) - _MAX_LISTED))',
        '                    print("  = … and %d more" % len(protected))',
        [T + "test_the_TRUNCATED_listings_report_the_REMAINDER_not_the_whole_set"],
    ),
    (
        "RA46", GB,
        "the same claim on the FIRST-CENSUS listing, which had no rail at all",
        '                print("  ~ … and %d more" % (len(now.tests) - _MAX_LISTED))',
        '                print("  ~ … and %d more" % len(now.tests))',
        [T + "test_the_FIRST_census_listing_reports_the_REMAINDER_too"],
    ),
    (
        "RA4", GB,
        "the census diff is computed in the wrong direction, so a recovery is "
        "reported as a new casualty and a genuine new casualty is reported as "
        "good news",
        "        result.new_casualties = sorted(no_verdict - recorded)",
        "        result.new_casualties = sorted(recorded - no_verdict)",
        [AC + "test_a_casualty_NOT_in_the_record_FAILS_the_gate",
         AC + "test_FEWER_casualties_than_recorded_reports_the_IMPROVEMENT"],
    ),
    (
        "RA5", GB,
        "a recorded casualty that REPORTED AGAIN is treated as fatal, so good "
        "news fails the gate and the record can never be shrunk without a red "
        "run first",
        "        result.recovered_casualties = sorted(recorded - no_verdict)",
        "        result.recovered_casualties = []\n"
        "        result.new_casualties += sorted(recorded - no_verdict)",
        [AC + "test_FEWER_casualties_than_recorded_reports_the_IMPROVEMENT",
         AC + "test_a_run_that_loses_NOTHING_still_reports_the_whole_record_as_recovered",
         AC + "test_a_casualty_that_IS_in_the_record_is_quiet"],
    ),
    (
        "RA6", GB,
        "the arm compares COUNTS rather than NAMES, so eleven tests dying while "
        "eleven different ones recover reads as no change at all",
        "        result.new_casualties = sorted(no_verdict - recorded)\n"
        "        result.recovered_casualties = sorted(recorded - no_verdict)",
        "        excess = len(no_verdict) - len(recorded)\n"
        "        result.new_casualties = sorted(no_verdict - recorded)[:max(excess, 0)]\n"
        "        result.recovered_casualties = sorted(recorded - no_verdict)",
        [AC + "test_the_SAME_COUNT_with_a_DIFFERENT_NAME_still_fails"],
    ),
    (
        "RA7", GB,
        "the `Failing tests:` block is allowed to arm the gate, but it spells a "
        "test `Suite.function()` where the console prints a display name — so "
        "every display-named test that reported perfectly well becomes a "
        "casualty (this is exactly how the bead came to be filed at 19)",
        # Anchor rewritten at playhead-t53a, which threaded the bundle's own
        # spellings into `_blamed_is_matched`. The EDIT moved; the expectation
        # did not.
        "    result.blamed_unmatched = [\n"
        "        name for name in blamed\n"
        "        if not _blamed_is_matched(name, identities, run.blamed_spellings)\n"
        "    ]",
        "    result.blamed_unmatched = [\n"
        "        name for name in blamed\n"
        "        if not _blamed_is_matched(name, identities, run.blamed_spellings)\n"
        "    ]\n"
        "    result.new_casualties = sorted(set(result.new_casualties)\n"
        "                                   | set(result.blamed_unmatched))",
        [AC + "test_a_BLAMED_name_alone_is_a_LEAD_and_never_a_NEW_CASUALTY"],
    ),
    # ---- the four safety properties tl6l ARGUES FROM, pinned durably ----
    #
    # R1 review found all four correct in the code and asserted nowhere, and
    # added CrashedHostSafetyPropertyTests. Those rails then lived only in a
    # scratchpad battery, so nothing stopped the next edit from removing them
    # together with the behaviour. They are rails here now.
    (
        "RB1", GB,
        "the census stops counting as crash evidence, so a run where tests "
        "started and said nothing — with no restart banner and no summary "
        "block, which is exactly what a hang produces — reads as GREEN",
        "        return bool(self.host_restarts or self.no_verdict or self.blamed_unmatched)",
        "        return bool(self.host_restarts or self.blamed_unmatched)",
        [SP + "test_NO_VERDICT_alone_forecloses_GREEN"],
    ),
    (
        "RB2", GB,
        "ABSENT leaves the exit code, so a baseline member the crash took down "
        "is reported and the gate exits 0 anyway",
        "                or self.absent or self.baseline_fiction\n",
        "                or self.baseline_fiction\n",
        [SP + "test_an_ABSENT_baseline_member_makes_the_gate_EXIT_NONZERO",
         SP + "test_a_baseline_member_lost_to_the_CRASH_is_still_fatal"],
    ),
    (
        "RB3", GB,
        "a test that lost its verdict is folded into NEW FAILURE, sending the "
        "reader to triage a regression in their own diff for a test that was "
        "never judged at all",
        "    result.no_verdict = sorted(no_verdict)",
        "    result.no_verdict = sorted(no_verdict)\n"
        "    result.new_failures = sorted(no_verdict)",
        [SP + "test_a_lost_test_is_NEVER_rendered_under_NEW_FAILURE"],
    ),
    (
        "RB4", GB,
        "`accept` stops carrying forward a recorded entry the crash silenced, "
        "so the crash shrinks the baseline from inside the one command whose "
        "job is to maintain it — and the shrinking diff reads as good news",
        '                merged["tests"][key] = dict(previous)',
        "                pass",
        [SP + "test_accept_CARRIES_FORWARD_across_a_SECOND_and_THIRD_crash"],
    ),
    (
        "RB5", GB,
        "a deliberate SKIP is no longer subtracted from the census, so the 30 "
        "XCTest PerfGate skips read as crashed-host casualties on every single "
        "full-plan run",
        "        return self.started - self.ran - self.skipped",
        "        return self.started - self.ran",
        [RC + "test_the_skips_are_subtracted_and_it_MATTERS",
         RC + "test_the_main_control_run"],
    ),
    (
        "RF1", TF,
        "the real-data rails point at a fixture that is not there — they must "
        "FAIL, never skip. A rail that reports OK without running is the defect "
        "playhead-fer3 was filed for and is the same shape as a gate reporting "
        "an all-clear for a run that lost part of the plan",
        'FIXTURES = ROOT / "scripts" / "tests" / "fixtures"',
        'FIXTURES = ROOT / "scripts" / "tests" / "fixtures-that-are-not-there"',
        [RC + "test_the_main_control_run",
         RC + "test_the_mn5e_branch_run",
         RC + "test_BOTH_runs_lost_THE_SAME_ELEVEN_TESTS",
         C + "test_a_real_log_cut_short_is_reported_incomplete",
         # playhead-phn3's green-run fixture is under the same rule.
         GR + "test_the_run_reports_NO_CASUALTIES_and_the_four_PASSED",
         GR + "test_the_INLINE_specimens_are_in_the_fixture_BYTE_FOR_BYTE"],
    ),
    # -----------------------------------------------------------------------
    # playhead-t53a — the .xcresult is the verdict source. Every rail below is
    # a way the change could REVERT to the console, or be read as a licence to
    # go quiet, without anything saying so.
    # -----------------------------------------------------------------------
    (
        "RB01", GB,
        "the framework discriminator reverts to `endswith(\"()\")`, which puts "
        "every PARAMETERISED Swift Testing test in the XCTest bucket — the bug "
        "the first draft shipped, worth 57 keys unmatched in EACH direction",
        'return url.rsplit("/", 1)[-1].endswith(")")',
        'return url.rsplit("/", 1)[-1].endswith("()")',
        [XP + "test_a_PARAMETERISED_swift_testing_test_is_not_mistaken_for_XCTest"],
    ),
    (
        "RB02", GB,
        "the bundle REPLACES the console's started roster instead of unioning "
        "with it, so a test the infrastructure never mentioned leaves the "
        "census by being forgotten — the quiet direction, and the one line that "
        "makes this change unable to weaken the gate",
        "    run.started = run.started | bundle.keys",
        "    run.started = set(bundle.keys)",
        [XW + "test_a_console_STARTED_test_the_bundle_never_mentions_is_STILL_a_casualty"],
    ),
    (
        "RB03", GB,
        "a crash-message failure is taken at face value as a FAILING TEST, so "
        "the census goes quiet and one `--accept-baseline` writes three healthy "
        "tests into the known-broken file, where their next GENUINE failure "
        "reads as known",
        "    crash = _crash_message(node)\n    if crash is not None:",
        "    crash = None\n    if crash is not None:",
        [XC + "test_a_crashed_case_is_a_CASUALTY_and_not_a_failure",
         XC + "test_accepting_a_crashed_run_does_NOT_write_the_victim_into_the_known_broken_file"],
    ),
    (
        "RB04", GB,
        "the crash pattern matches ANY failure message, so every ordinary "
        "assertion failure is laundered into a casualty — the mirror of RB03 "
        "and the LOOSENING direction: a real regression stops being a failure "
        "at all and becomes something to re-run",
        r'_XCRESULT_CRASHED = re.compile(r"^Test crashed\b", re.IGNORECASE)',
        '_XCRESULT_CRASHED = re.compile(r"", re.IGNORECASE)',
        [XC + "test_an_UNRECOGNISED_crash_wording_stays_a_FAILURE_which_is_the_loud_direction"],
    ),
    (
        "RB05", GB,
        "a result string this gate has never heard of counts as a verdict, so a "
        "future Xcode's new outcome silently empties the census instead of being "
        "reported — an absence read as a measurement, one more time",
        "XCRESULT_VERDICTS = frozenset(\n    (XCRESULT_PASSED, XCRESULT_FAILED, "
        "XCRESULT_SKIPPED, XCRESULT_EXPECTED_FAILURE)\n)",
        "XCRESULT_VERDICTS = None\n\n\nclass _Any(frozenset):\n"
        "    def __contains__(self, item):\n        return True\n\n\n"
        "XCRESULT_VERDICTS = _Any()",
        [XP + "test_a_result_string_this_gate_does_not_know_is_NOT_counted_as_a_verdict",
         XW + "test_a_bundle_key_with_no_recognised_verdict_is_a_casualty_and_is_NAMED"],
    ),
    (
        "RB06", GB,
        "a bundle that cannot be read falls back to the console SILENTLY, which "
        "reinstates the whole defect and makes the run indistinguishable from "
        "one that never asked for a bundle",
        '        raise XcresultUnreadable("no such result bundle: %s" % path)',
        "        return {}",
        [XM + "test_a_missing_bundle_REFUSES_rather_than_falling_back_to_the_console"],
    ),
    (
        "RB07", GB,
        "an XCTest failure takes its KIND from the bundle's message, so a slow "
        "XCTest failure whose message mentions a time limit is handed the "
        "starvation tolerance the console has always refused it",
        "    if framework == FRAMEWORK_XCTEST:\n        return {KIND_ASSERTION}",
        "    if framework == FRAMEWORK_XCTEST and False:\n        return {KIND_ASSERTION}",
        [XP + "test_an_XCTEST_failure_is_pinned_to_ASSERTION_whatever_its_message_says"],
    ),
    (
        "RB08", GB,
        "the crash resolution stops running after the collision loop, so NODE "
        "ORDER decides whether a passing twin launders a crashed namesake — the "
        "defect a rail caught in exactly one of the two orders",
        "    for key in run.crashed:\n        run.passed.discard(key)",
        "    for key in []:\n        run.passed.discard(key)",
        [XC + "test_a_passing_twin_cannot_launder_a_crashed_one_that_shares_its_key"],
    ),
    (
        "RB09", GB,
        "the second bundle only overrides keys it JUDGED, so a residual re-run "
        "can never clear a casualty — the whole point of running it — and the "
        "gate ends holding the hole it just spent a run trying to fill",
        "        for key in part.keys:\n            merged.passed.discard(key)",
        "        for key in part.judged:\n            merged.passed.discard(key)",
        [XM + "test_a_later_bundle_recording_a_CRASH_overrides_an_earlier_PASS",
         XM + "test_a_later_bundle_that_LOST_a_verdict_overrides_an_earlier_failure"],
    ),
    (
        "RB10", GB,
        "the console-sourced census announces itself as RELIABLE, telling the "
        "reader the opposite of the measurement — 80 of 87 reported casualties "
        "over 27 crash-free runs were verdicts the parser could not read",
        '            "  VERDICTS FROM    %s, which is NOT a reliable census '
        '(playhead-t53a). "',
        '            "  VERDICTS FROM    %s, which is a reliable census '
        '(playhead-t53a). "',
        [XW + "test_the_verdict_NAMES_which_instrument_produced_the_census"],
    ),
    (
        "RB11", GB,
        "the residual command prints the KEY rather than the bundle's "
        "identifier, so every re-run argument is a display name `-only-testing:` "
        "silently ignores — and a silently-ignored filter STILL REPORTS SUCCESS",
        '                print("-only-testing:%s" % target)',
        '                print("-only-testing:%s" % key)',
        [XR + "test_a_casualty_is_printed_as_an_only_testing_argument"],
    ),
    (
        "RB12", GB,
        "a casualty with no bundle identifier is printed as a BLANK LINE on "
        "stdout instead of named on stderr, so the hole becomes an empty "
        "argument the caller passes to xcodebuild",
        '                sys.stderr.write(\n'
        '                    "gate-baseline: cannot re-run %s — no bundle identifier for it. "',
        '                print("-only-testing:")\n'
        '                sys.stderr.write(\n'
        '                    "gate-baseline: %s\\n" % "" or (\n'
        '                    "gate-baseline: cannot re-run %s — no bundle identifier for it. "',
        [XR + "test_a_casualty_with_no_bundle_identifier_goes_to_STDERR_not_stdout"],
    ),
    (
        "RB13", GB,
        "every `Failing tests:` entry is matched once a bundle is present, so "
        "the one arm that can see a test the host killed BEFORE its start line "
        "reports nothing at all",
        "    if entry in blamed_spellings:\n        return True",
        "    if blamed_spellings:\n        return True",
        [XB + "test_a_blamed_name_NO_bundle_knows_is_still_reported_unmatched"],
    ),
    (
        "RB14", FG,
        "fast-gate stops asking xcodebuild for a bundle, so every full-plan run "
        "silently reverts to the console census this bead exists to replace",
        '    -resultBundlePath "$RESULT_BUNDLE" \\\n',
        "",
        [XG + "test_the_gate_asks_xcodebuild_for_a_bundle_and_the_CHECK_reads_it"],
    ),
    (
        "RB15", FG,
        "the bundle is written but never handed to the check, which is the same "
        "outcome as RB14 arrived at from the other end — and harder to see, "
        "because the bundle is right there on disk",
        'BUNDLE_ARGS=()\n[ -d "$RESULT_BUNDLE" ] && BUNDLE_ARGS+=(--xcresult "$RESULT_BUNDLE")',
        'BUNDLE_ARGS=()',
        [XG + "test_the_gate_asks_xcodebuild_for_a_bundle_and_the_CHECK_reads_it"],
    ),
    (
        "RB16", FG,
        "the residual re-run happens and its bundle is DISCARDED, so the gate "
        "pays for the re-run and still reports the casualties it just resolved",
        '      BUNDLE_ARGS+=(--xcresult "$RESIDUAL_BUNDLE")',
        "      :",
        [XG + "test_a_real_casualty_triggers_a_scoped_RERUN_and_the_second_bundle_is_read"],
    ),
    (
        "RB17", FG,
        "the residual re-run fires on a SELECTIVE run too, so a mutation "
        "battery's focused invocation drags a re-run of somebody else's "
        "population into its verdict",
        'if [ "$SELECTIVE" -eq 0 ] && [ "${#BUNDLE_ARGS[@]}" -gt 0 ] \\\n'
        '   && [ "${PLAYHEAD_SKIP_BASELINE:-0}" != "1" ]; then',
        'if [ "${#BUNDLE_ARGS[@]}" -gt 0 ] \\\n'
        '   && [ "${PLAYHEAD_SKIP_BASELINE:-0}" != "1" ]; then',
        [XG + "test_a_selective_run_does_NOT_trigger_a_residual_rerun"],
    ),
    (
        "RB18", GB,
        "the verdict source is printed only when something was lost, so the one "
        "reading that most needs qualifying — a census of ZERO taken off the "
        "console — is the reading that never names its instrument",
        "        out.extend(self._render_verdict_source())\n"
        "        out.extend(self._render_no_verdict())",
        "        out.extend(self._render_no_verdict())",
        [XG + "test_the_gate_asks_xcodebuild_for_a_bundle_and_the_CHECK_reads_it"],
    ),
    # -----------------------------------------------------------------------
    # playhead-phn3 — the weld that reaches past line N+1, and the severed glyph.
    # -----------------------------------------------------------------------
    (
        "RS01", GB,
        "the weld reaches only line N+1 again — the shipped behaviour, which "
        "recovered 90.2% of 2,563 measured welds and left 251 records reading "
        "as silence, four of them on an otherwise-green merge gate",
        "    for span in range(1, _MAX_SPLICE_SPAN + 1):",
        "    for span in range(1, 2):",
        [SS + "test_the_xul6_Corrupted_specimen_is_a_PASS",
         SS + "test_the_xul6_bracketRefined_specimen_is_a_PASS",
         SS + "test_the_xul6_Podcast_specimen_survives_FOUR_intervening_lines"],
    ),
    (
        "RS02", GB,
        "the lookahead is unbounded, so a head can walk an arbitrary run of app "
        "output looking for something that completes it — the fabrication "
        "direction the bound is a backstop against",
        "_MAX_SPLICE_SPAN = 8",
        "_MAX_SPLICE_SPAN = 10000",
        [SS + "test_the_lookahead_is_BOUNDED_rather_than_running_to_the_next_match"],
    ),
    (
        "RS03", GB,
        "the scan steps over ANY line rather than only over app output, so "
        "unattributable text between the halves is treated as evidence they "
        "belong together",
        "        if not _APP_LOG_INTRUSION.match(following):\n"
        "            return None",
        "        if False:\n"
        "            return None",
        [SS + "test_the_weld_STOPS_at_a_line_that_is_not_app_output"],
    ),
    (
        "RS04", GB,
        "the step-over test becomes `search`, so a line that merely CONTAINS a "
        "timestamp — i.e. a line that has itself been spliced — is stepped over "
        "and its wreckage ends up between a head and a stranger's tail",
        "        if not _APP_LOG_INTRUSION.match(following):",
        "        if not _APP_LOG_INTRUSION.search(following):",
        [SS + "test_a_line_whose_timestamp_is_NOT_at_COLUMN_ZERO_is_not_stepped_over"],
    ),
    (
        "RS05", GB,
        "the end-of-list guard goes, so a log truncated mid-splice walks off "
        "the end of its own line list",
        "        if index + span >= len(lines):\n"
        "            return None",
        "        if False:\n"
        "            return None",
        [SS + "test_a_head_at_the_very_END_of_a_truncated_log_is_left_alone"],
    ),
    (
        "RS06", GB,
        "the tail is taken from line N+1 whatever span the scan found, so the "
        "reconstruction is head + the first intruder — garbage under a name "
        "nobody can reconcile",
        "                following = lines[index + span]",
        "                following = lines[index + 1]",
        [SS + "test_the_xul6_Corrupted_specimen_is_a_PASS",
         SS + "test_the_xul6_Podcast_specimen_survives_FOUR_intervening_lines"],
    ),
    (
        "RS07", GB,
        "the intervening app-log lines are dropped rather than re-emitted, so "
        "everything the intrusion carried between the halves is lost with them",
        "                out.extend(lines[index + 1:index + span])",
        "                pass",
        [SS + "test_the_INTERVENING_lines_are_re_emitted_VERBATIM_and_none_is_lost"],
    ),
    (
        "RS08", GB,
        "the cursor advances by a fixed two lines rather than by the span "
        "consumed, so the lines between the halves are emitted twice",
        "                index += span + 1",
        "                index += 2",
        [SS + "test_the_INTERVENING_lines_are_re_emitted_VERBATIM_and_none_is_lost"],
    ),
    (
        "RS09", GB,
        "the marker anchor becomes OPTIONAL — `Test \"x\" passed after` matches "
        "anywhere, so an app-log line that merely quotes a verdict invents a "
        "test no verdict can ever answer for (CLAUDE.md's fifth splice shape, "
        "a permanent phantom casualty)",
        '    return re.compile("(?:" + glyph + "|" + _GLYPH_SHARD + ") " + rest)',
        '    return re.compile("(?:" + glyph + "|" + _GLYPH_SHARD + ")? ?" + rest)',
        [SG + "test_app_output_that_merely_QUOTES_a_verdict_does_not_FABRICATE_one"],
    ),
    (
        # NOT the obvious mutant. Dropping the `+` from `_GLYPH_SHARD` is a
        # PROVEN EQUIVALENT and survived when it was tried: every pattern here
        # SEARCHES, so `(X)+ Test "` and `X Test "` accept exactly the same
        # lines — a `+` match ends with one X immediately before ` Test "`, which
        # the single form finds at that same position, and n=1 gives the
        # converse. The `+` stays because it describes the shape; it is not
        # load-bearing and there is nothing here for a rail to hold.
        "RS10", GB,
        "the shard covers only a raw byte, so the octal escaping xcodebuild "
        "ACTUALLY DOES goes unread — 92 measured lines over 138 logs are octal "
        "and not one is a raw byte",
        '_GLYPH_SHARD = r"(?:\\\\[0-3][0-7][0-7]|\ufffd)+"',
        '_GLYPH_SHARD = r"(?:\ufffd)+"',
        [SG + "test_the_xul6_empty_chunks_specimen_is_a_PASS",
         SG + "test_the_TWO_ESCAPE_spelling_is_read_too",
         SG + "test_the_WHOLE_GLYPH_escaped_is_read_too"],
    ),
    (
        "RS11", GB,
        "the shard covers only the octal escaping xcodebuild happens to do "
        "today, so a genuinely raw continuation byte — which `_read` hands over "
        "as U+FFFD — goes back to reading as silence",
        '_GLYPH_SHARD = r"(?:\\\\[0-3][0-7][0-7]|\ufffd)+"',
        '_GLYPH_SHARD = r"(?:\\\\[0-3][0-7][0-7])+"',
        [SG + "test_a_RAW_replacement_character_is_read_too"],
    ),
    (
        "RS12", GB,
        "the shard is dropped from `_REPAIRABLE_HEAD`, so a record whose glyph "
        "AND whose name were both severed has no repairable head and stays a "
        "casualty — the two defects this bead fixes compose, and the rail that "
        "sees the composition is not either of the ones that sees them apart",
        '_REPAIRABLE_HEAD = re.compile(r"[◇✔✘➜]|" + _GLYPH_SHARD + r"|Test Case \'-\\[")',
        '_REPAIRABLE_HEAD = re.compile(r"[◇✔✘➜]|Test Case \'-\\[")',
        [SG + "test_a_severed_glyph_and_a_severed_NAME_compose"],
    ),
    (
        "RS13", GB,
        "the pass pattern stops requiring the word `Test`, so `\\224 Suite "
        "\"DownloadManager \u2013 Setup\" passed after 105.532 seconds.` — real, "
        "from the se0x merge gate — enters the run as a test that never existed",
        """_ST_PASS_NAMED = _marked('\u2714', r'Test "(.+?)" (?:with \\d+ test cases? )?passed after')""",
        """_ST_PASS_NAMED = _marked('\u2714', r'\\w+ "(.+?)" (?:with \\d+ test cases? )?passed after')""",
        [SG + "test_a_SUITE_line_with_a_severed_glyph_is_NOT_a_test"],
    ),
    (
        "RS14", GB,
        "only the PASS pattern learns the damaged glyph, so a severed FAIL "
        "stays invisible — the lost-failure direction, which has never been "
        "observed only because passes outnumber failures 124 to 1",
        """_ST_FAIL_NAMED = _marked('\u2718', r'Test "(.+?)" (?:with \\d+ test cases? )?failed after(?: ([\\d.]+) seconds)?')""",
        """_ST_FAIL_NAMED = re.compile(r'\u2718 Test "(.+?)" (?:with \\d+ test cases? )?failed after(?: ([\\d.]+) seconds)?')""",
        [SG + "test_a_severed_glyph_FAIL_is_still_a_FAILURE"],
    ),
    (
        "RS15", GB,
        "the SKIP pattern loses the damaged glyph, so a deliberate skip reads "
        "as silence — and a skip read as silence is a crashed-host casualty, "
        "the one category whose remedy is to re-run the whole plan",
        """_ST_SKIP_NAMED = _marked('\u279c', r'Test "(.+?)" skipped')""",
        """_ST_SKIP_NAMED = re.compile(r'\u279c Test "(.+?)" skipped')""",
        [SG + "test_a_severed_glyph_SKIP_is_an_OUTCOME"],
    ),
    (
        "RS16", GB,
        "the START pattern loses the damaged glyph, so a test whose start line "
        "was severed is in NO set at all — invisible in both directions, which "
        "is the one blind spot the console roster exists to close",
        """_ST_START_NAMED = _marked('\u25c7', r'Test "(.+?)" started')""",
        """_ST_START_NAMED = re.compile(r'\u25c7 Test "(.+?)" started')""",
        [SG + "test_a_severed_glyph_START_keeps_the_test_ON_the_roster"],
    ),
    (
        "RS17", TF,
        "an inline specimen is retyped by one character, so its rail becomes a "
        "test of the typo rather than of the log — the four specimens are only "
        "worth having while they are the real bytes",
        "'ed / absurdly-low value falls back to default\" passed after 118.958 seconds.\\n'",
        "'ed / absurdly-low value falls back to defau1t\" passed after 118.958 seconds.\\n'",
        [GR + "test_the_INLINE_specimens_are_in_the_fixture_BYTE_FOR_BYTE"],
    ),
    (
        "RD01", GB,
        "the SQLite prose leaves the table, so `unable to open database file` "
        "— the ONE shape that reaches the log with its errno already thrown "
        "away — reads as a regression again",
        '    r"unable to open database file"\n',
        '    r"unable to open NO SUCH PHRASE"\n',
        [DC + "test_sqlite_cantopen_prose_is_a_resource"],
    ),
    (
        "RD02", GB,
        "EBADF leaves the errno table, so the one witness no database "
        "explanation covers — a SOURCE FILE read failing mid-run — is a NEW "
        "failure again",
        '    "9": "EBADF — a descriptor that was obtained and then went bad",\n',
        "",
        [DC + "test_ebadf_is_a_resource_even_though_the_sentence_is_generic"],
    ),
    (
        "RD03", GB,
        "the unrecognised-errno VETO becomes a search for any recognised one, "
        "so a message carrying ENOENT *and* a recognised phrase is swallowed — "
        "the direction that hides somebody's bug",
        """        if any(name is None for name in named):
            return None
        return named[0]""",
        """        for name in named:
            if name is not None:
                return name""",
        [DC + "test_an_unrecognised_errno_VETOES_a_recognised_phrase"],
    ),
    (
        "RD05", GB,
        "the console UNANIMITY veto is dropped, so one CANTOPEN alongside a "
        "real assertion takes the whole test out of the NEW column",
        """        if key in resource_vetoed:
            continue
""",
        "",
        [DK + "test_UNANIMITY_one_real_assertion_keeps_the_whole_test_a_failure",
         DK + "test_the_veto_holds_whichever_ORDER_the_issues_arrive_in"],
    ),
    (
        "RD06", GB,
        "the BUNDLE unanimity rule becomes a search: an unrecognised message "
        "is skipped instead of vetoing, so a real assertion is swallowed",
        """        cause = resource_cause(message)
        if cause is None:
            return None
        if found is None:""",
        """        cause = resource_cause(message)
        if cause is None:
            continue
        if found is None:""",
        [DB + "test_a_failed_case_with_a_real_assertion_alongside_stays_a_failure"],
    ),
    (
        "RD07", GB,
        "a FAILED case with NO messages is classified from SILENCE — exactly "
        "the inference playhead-t53a removed one category along",
        """        if found is None:
            found = (cause, message)
    return found""",
        """        if found is None:
            found = (cause, message)
    return found or ("resource failure", "")""",
        [DB + "test_a_failed_case_with_NO_messages_stays_a_failure"],
    ),
    (
        "RD08", GB,
        "`no_verdict` stops subtracting the denied, so a test that REPORTED an "
        "error is booked as one that said nothing — and the accept writes it "
        "into the crashed-host census",
        "        return self.started - self.ran - self.skipped - set(self.resource)",
        "        return self.started - self.ran - self.skipped",
        [DK + "test_it_is_NOT_counted_as_a_crashed_host_casualty",
         DA + "test_a_denied_test_does_not_enter_the_crashed_host_census"],
    ),
    (
        "RD09", GB,
        "a host death stops outranking a denial, so one key sits in BOTH "
        "categories and is counted twice",
        """        run.resource.pop(key, None)
        run.resource_causes.pop(key, None)
    for key in run.resource:""",
        """    for key in run.resource:""",
        [DB + "test_a_crashed_twin_outranks_a_denied_twin_in_BOTH_orders"],
    ),
    (
        "RD10", GB,
        "the collision guard goes, so a same-named twin that ran fine promotes "
        "a denied test to PASSED — resolving toward the better news, which is "
        "the direction this module never resolves",
        """    if key in run.resource:
        # A same-named twin that ran fine is not evidence about the one that
        # was denied a descriptor. Resolve toward the worse news, exactly as
        # the crash rule above does.
        return
""",
        "",
        [DB + "test_a_passing_same_named_twin_cannot_launder_a_resource_casualty"],
    ),
    (
        "RD11", GB,
        "the headline count is a constant, so twelve denied tests and one read "
        "identically — a count that does not count",
        """            return " — %d test%s hit a RESOURCE FAILURE (re-run)" % (
                len(self.resource), "" if len(self.resource) == 1 else "s",
            )""",
        """            return " — %d test%s hit a RESOURCE FAILURE (re-run)" % (
                1, "" if len(self.resource) == 1 else "s",
            )""",
        [DV + "test_the_count_is_the_NUMBER_of_denied_tests"],
    ),
    (
        "RD12", GB,
        "the resource tail leaves the RED line entirely, so the reclassification "
        "happens SILENTLY — failures vanish from the NEW column with nothing "
        "printed, which is the one outcome worse than over-reporting",
        """        if self.resource:
            # Standing alone this is the whole reason the run is red, so it
            # opens the tail rather than trailing a NO VERDICT count.""",
        """        if False:
            # Standing alone this is the whole reason the run is red, so it
            # opens the tail rather than trailing a NO VERDICT count.""",
        [DV + "test_the_count_rides_on_the_RED_line",
         DV + "test_the_count_is_the_NUMBER_of_denied_tests"],
    ),
    (
        "RD13", GB,
        "a denied run exits ZERO, so the gate is QUIETER than before this bead "
        "— the exact failure this change must not introduce",
        "                or self.resource):\n            return EXIT_REGRESSION",
        "                or False):\n            return EXIT_REGRESSION",
        [DV + "test_it_is_NOT_quieter_than_before_the_exit_code_still_says_re_run"],
    ),
    (
        "RD14", GB,
        "GREEN becomes reachable while tests were denied a file — the mirror of "
        "RD13, and it needs its own rail because the two are independent",
        """        if (self.ok and self.total_failures == 0 and not self.crashed_host
                and not self.resource):""",
        """        if (self.ok and self.total_failures == 0 and not self.crashed_host
                and True):""",
        [DV + "test_GREEN_is_unreachable_while_a_resource_failure_stands"],
    ),
    (
        "RD15", GB,
        "the accept stops protecting denied entries, so the known-broken file "
        "SHRINKS because the box was short — from inside the one command whose "
        "job is to maintain it",
        "    protected = run.no_verdict | set(run.resource)",
        "    protected = run.no_verdict",
        [DA + "test_a_recorded_entry_denied_this_run_is_NOT_pruned"],
    ),
    (
        "RD16", GB,
        "BASELINE IS FICTION fires on a denied run, blaming the FILE for a "
        "short box and inviting an accept that empties it",
        "    if entries and not run.failures and not run.resource:",
        "    if entries and not run.failures:",
        [DV + "test_BASELINE_IS_FICTION_does_not_fire_on_a_denied_run"],
    ),
    (
        "RD17", GB,
        "an ABSENT baseline member denied a file is reported as a RENAME, "
        "sending the reader after a rename that did not happen",
        """            elif key in self.absent_resource:""",
        """            elif False:""",
        [DV + "test_an_ABSENT_baseline_member_names_the_denial_not_a_rename"],
    ),
    (
        "RD18", GB,
        "the console's reading survives a bundle that judged the test PASSED, "
        "so a name can leave the category without the bundle ever saying so",
        "    run.resource = dict(bundle.resource)\n    run.resource_causes = dict(bundle.resource_causes)",
        "    run.resource_causes = dict(bundle.resource_causes)",
        [DB + "test_the_bundle_REPLACES_the_consoles_reading"],
    ),
    (
        "RD19", GB,
        "the block names a generic cause instead of the observed one, so "
        "EMFILE and ENOSPC — different remedies — read identically",
        """            out.append("  RESOURCE         %s  (%s)" % (
                key, self.resource_causes.get(key, "resource failure"),
            ))""",
        """            out.append("  RESOURCE         %s  (%s)" % (
                key, "resource failure",
            ))""",
        [DV + "test_the_block_NAMES_which_resource_per_test"],
    ),
    (
        "RD20", GB,
        "the block drops the sentence saying what it does NOT know, and starts "
        "reading as a diagnosis rather than an observation",
        """        out.append(
            "  RESOURCE — this does NOT say whether the box was short or something "
            "leaked; it says the process asked for a file and did not get one. \"""",
        """        out.append(
            "  RESOURCE — re-run the plan. \"""",
        [DV + "test_the_block_states_what_it_does_NOT_know"],
    ),
    (
        "RD21", GB,
        "the block prints on EVERY run, so a healthy gate carries a RESOURCE "
        "heading claiming nothing — the mirror of RD12, and a printed zero is "
        "a claim",
        """        if not self.resource:
            return []
        out = [
            "  RESOURCE FAILURE""",
        """        if False:
            return []
        out = [
            "  RESOURCE FAILURE""",
        [DV + "test_nothing_is_printed_on_a_healthy_run"],
    ),
    (
        "R99", GB,
        "VACUITY CONTROL: a cosmetic wording change that must SURVIVE. If this "
        "is killed, the rails redden on any edit and every KILLED above is void",
        '        out.append("  NEW FAILURE      %s" % key)',
        '        out.append("  NEW FAILURE:     %s" % key)',
        [V + "test_a_NEW_failure_fails_the_gate_and_is_NAMED"],
    ),
]

EXPECT_SURVIVE = {"R99"}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_tests(test_ids):
    """Return (rc, output). Non-zero rc means at least one named test failed.

    BYTECODE CACHING WILL LIE TO YOU IF YOU LET IT. The suite loads the module
    under test through importlib, and SourceFileLoader validates its cached
    bytecode on (mtime, size) — both of which are second-granular and
    coincidence-prone here. R18 and R24 replace `.search(` with `.match(` twice
    each, so their mutated files are byte-for-byte the SAME SIZE; applied within
    the same second, R24 loaded R18's cached bytecode, ran against unmutated
    PASS patterns, and reported SURVIVED against a rail that actually works.
    A mutation battery that mis-reports a verdict is worse than none, so the
    cache is purged and disabled rather than trusted.
    """
    for cache in ROOT.rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.run(
        [sys.executable, "-B", "-m", "unittest"] + list(test_ids),
        cwd=str(ROOT), capture_output=True, text=True, env=env,
    )
    return proc.returncode, proc.stdout + proc.stderr


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--only", default=None)
    args = parser.parse_args(argv)

    selected = [m for m in MUTATIONS if args.only in (None, m[0])]
    if not selected:
        sys.stderr.write("no mutation named %r\n" % args.only)
        return 2

    # playhead-o89d R5. `--only` CANNOT DROP THE CONTROL. Measured on this file:
    # `--only RA32` printed `control not run` and then `All mutations killed, and
    # the vacuity control survived.` and exited 0 — a positive claim about a
    # control that had not run, from a run that had proved nothing about whether
    # the suite reddens on any edit at all. That is the same hole CLAUDE.md
    # records being closed in the untypeable battery, still open here, and it is
    # the standing defect class: a sentence naming one thing read as though it
    # named another. The disk-preflight battery reuses this engine, so it is
    # fixed there too.
    chosen = {m[0] for m in selected}
    for control in sorted(EXPECT_SURVIVE):
        if control not in chosen:
            selected += [m for m in MUTATIONS if m[0] == control]
            sys.stderr.write(
                "note: appending the vacuity control %s — a selection that omits "
                "it can report nothing about whether the rails redden on any "
                "edit\n" % control
            )

    if args.list:
        for name, path, desc, _, _, expect in selected:
            print("%-5s %-22s %s" % (name, path, desc))
            for test in expect:
                print("%-5s %s expects: %s" % ("", "", test.split(".")[-1]))
        return 0

    files = sorted({m[1] for m in selected})
    originals = {f: (ROOT / f).read_bytes() for f in files}
    before = {f: sha(ROOT / f) for f in files}

    # ---------------------------------------------------------------------
    # The free unmutated pass. Two things at once: the tree is green to begin
    # with (a test that is ALREADY red would credit every rail that names it),
    # and every expectation names a test that actually exists and runs.
    # ---------------------------------------------------------------------
    # Anchors first, ALL of them, before a single test is run. A drifted anchor
    # found halfway through is an ERROR you pay for after the battery has already
    # started rewriting files; found here it costs nothing. Two of this file's own
    # anchors were wrong on first authoring — the escaping in R06 and the
    # indentation in R16 — and both would have read as ERROR mid-run.
    drift = []
    for name, rel, _, old, _, _ in selected:
        found = (ROOT / rel).read_text(encoding="utf-8").count(old)
        if found != 1:
            drift.append("    %-5s %s: anchor matched %d times, expected 1"
                         % (name, rel, found))
    if drift:
        sys.stderr.write("mutation-battery: anchor drift — the source moved on.\n")
        sys.stderr.write("\n".join(drift) + "\n")
        sys.stderr.write("Rewrite the EDIT, never the expectation.\n")
        return 2
    print("=== anchors: %d/%d match exactly once ===" % (len(selected), len(selected)))

    every_test = sorted({t for m in selected for t in m[5]})
    print("=== baseline: %d expected test(s) on UNMUTATED sources ===" % len(every_test))
    rc, out = run_tests(every_test)
    if rc != 0:
        sys.stderr.write(out)
        sys.stderr.write(
            "\nmutation-battery: the rails are RED before any mutation. Either a "
            "named test does not exist (check the spelling — this is exactly how "
            "two of playhead-djl0's rails reported SURVIVED against working code) "
            "or the tree is broken. Fix that first; every verdict below would be "
            "meaningless.\n"
        )
        return 2
    ran = out.count(".") and True
    print("  green — %d test(s), every expectation reachable\n" % len(every_test))

    results = []
    try:
        for name, rel, desc, old, new, expect in selected:
            path = ROOT / rel
            text = path.read_text(encoding="utf-8")
            count = text.count(old)
            if count != 1:
                results.append((name, "ERROR",
                                "anchor matched %d times, expected 1" % count))
                print("%-5s ERROR   anchor matched %d times" % (name, count))
                continue
            path.write_text(text.replace(old, new), encoding="utf-8")
            rc, _ = run_tests(expect)
            path.write_bytes(originals[rel])
            if sha(path) != before[rel]:
                results.append((name, "ERROR", "restore was not byte-exact"))
                break
            wanted_survive = name in EXPECT_SURVIVE
            killed = rc != 0
            if wanted_survive:
                verdict = "OK-SURVIVED" if not killed else "CONTROL-KILLED"
            else:
                verdict = "KILLED" if killed else "SURVIVED"
            results.append((name, verdict, desc))
            print("%-5s %-14s %s" % (name, verdict, desc))
    finally:
        for rel in files:
            (ROOT / rel).write_bytes(originals[rel])

    for rel in files:
        if sha(ROOT / rel) != before[rel]:
            sys.stderr.write("TREE NOT RESTORED: %s — inspect before anything else\n" % rel)
            return 4

    bad = [r for r in results if r[1] in ("SURVIVED", "ERROR", "CONTROL-KILLED")]
    control_ran = [r for r in results if r[0] in EXPECT_SURVIVE]
    print("\n%d mutation(s): %d killed, %d survived, %d error, control %s"
          % (len(results),
             sum(1 for r in results if r[1] == "KILLED"),
             sum(1 for r in results if r[1] == "SURVIVED"),
             sum(1 for r in results if r[1] == "ERROR"),
             next((r[1] for r in results if r[0] in EXPECT_SURVIVE), "not run")))
    if bad:
        sys.stderr.write(
            "\nA SURVIVOR IS A COVERAGE HOLE. The defect above can be introduced "
            "with the suite green. Write the test that rejects it — do not relax "
            "the expectation and do not delete the entry.\n"
        )
        for name, verdict, detail in bad:
            sys.stderr.write("  %-5s %-14s %s\n" % (name, verdict, detail))
        return 1
    if not control_ran:
        # Unreachable while the append above stands; kept because the claim and
        # the evidence for it must be computed from the same thing.
        sys.stderr.write(
            "\nTHE VACUITY CONTROL DID NOT RUN — this run says nothing about "
            "whether the rails redden on any edit, so it cannot report an "
            "all-clear.\n"
        )
        return 3
    print("All mutations killed, and the vacuity control survived.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
