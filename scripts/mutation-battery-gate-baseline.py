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
        r'''    r"Test Case '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' (failed|passed) \(([\d.]+) seconds\)"''',
        r'''    r"Test Kase '-\[([A-Za-z0-9_.]+) ([A-Za-z0-9_:]+)\]' (failed|passed) \(([\d.]+) seconds\)"''',
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
        "one observation is enough to mint a DETERMINISTIC entry, so a test "
        "starved once becomes one whose passing fails the gate forever",
        "MIN_RUNS_FOR_DETERMINISTIC = 2",
        "MIN_RUNS_FOR_DETERMINISTIC = 1",
        [K + "test_a_single_observation_can_never_be_deterministic"],
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
        "        if not reached:\n            continue",
        "        if False:\n            continue",
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
