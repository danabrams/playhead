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
SP = SUITE + ".CrashedHostSafetyPropertyTests."
CM = SUITE + ".CensusMergeTests."


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
        "                    and not _parses_as_a_test_line(following)\n",
        "",
        [TR + "test_a_truncated_FAIL_line_is_still_a_FAILURE"],
    ),
    (
        "RC3", GB,
        "an INTACT verdict line is rewritten anyway, so it absorbs the next "
        "line — a pass followed by the restart banner is claimed by the restart "
        "handler and the test that passed becomes a casualty",
        "                    and not _parses_as_a_test_line(head)\n",
        "",
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
        "    result.blamed_unmatched = [name for name in blamed\n"
        "                               if not _blamed_is_matched(name, identities)]",
        "    result.blamed_unmatched = [name for name in blamed\n"
        "                               if not _blamed_is_matched(name, identities)]\n"
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
         C + "test_a_real_log_cut_short_is_reported_incomplete"],
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
    print("All mutations killed, and the vacuity control survived.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
