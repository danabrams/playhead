#!/usr/bin/env python3
"""playhead-3nfa — mutation battery for the disk preflight (P series).

WHY THERE IS NO SWIFT SERIES FOR THIS BEAD
------------------------------------------
`scripts/mutation-battery.sh` is structurally a SWIFT battery: its MUTABLE_FILES
are Swift sources and its verdict comes out of xcodebuild's console glyphs. This
bead changes no Swift at all — it is one Python module, two bash scripts and a
test file. Inventing a Swift series to have one would be fabricating evidence.

So this follows the precedent playhead-voez already set for exactly this case:
`scripts/mutation-battery-gate-baseline.py` (the R series) is a Python rail with
the same discipline — anchors verified, a free unmutated pass, a vacuity
control, byte-exact restore. This file reuses that engine verbatim rather than
copying it, and supplies only the rails.

    scripts/mutation-battery-disk-preflight.py            # run them all
    scripts/mutation-battery-disk-preflight.py --list
    scripts/mutation-battery-disk-preflight.py --only P01

Do not run this while a gate is running from the same worktree: it rewrites
scripts/fast-gate.sh in place, and bash reads a script incrementally.
"""

import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location(
    "mutation_battery_engine", ROOT / "scripts" / "mutation-battery-gate-baseline.py")
engine = importlib.util.module_from_spec(_spec)
sys.modules["mutation_battery_engine"] = engine
_spec.loader.exec_module(engine)

DP = "scripts/disk_preflight.py"
FG = "scripts/fast-gate.sh"
DC = "scripts/disk-cleanup.sh"
SUITE = "scripts.tests.test_disk_preflight"

T = SUITE + ".ThresholdTests."
R = SUITE + ".RefusalContentTests."
S = SUITE + ".SurveyTests."
I = SUITE + ".ResolveSimIdTests."
O = SUITE + ".ReclaimOptInTests."
W = SUITE + ".FastGateWiringTests."
C = SUITE + ".CleanerWiringTests."
D = SUITE + ".DefaultThresholdTests."


# name, file, description, old, new, expected-to-fail test ids
MUTATIONS = [
    (
        "P01", DP,
        "the threshold becomes exclusive — a run with EXACTLY the measured "
        "requirement is refused, which is how an accurate number starts "
        "over-refusing and people learn to set the override",
        "    if free_b >= min_b:\n        if not args.quiet:",
        "    if free_b > min_b:\n        if not args.quiet:",
        [T + "test_exactly_at_the_threshold_passes"],
    ),
    (
        "P02", DP,
        "the refusal prints but exits 0 — fast-gate proceeds into the wedge "
        "having just explained why it should not",
        "    sys.stderr.flush()\n    return 1",
        "    sys.stderr.flush()\n    return 0",
        [T + "test_one_byte_short_refuses",
         W + "test_a_short_disk_stops_the_run_BEFORE_xcodebuild"],
    ),
    (
        "P03", DP,
        "the cleaner runs on every short check whether or not anyone opted in "
        "— silent deletion, which the repo's rm rails exist to forbid",
        "    if args.reclaim:",
        "    if True:",
        [O + "test_nothing_is_deleted_without_the_flag"],
    ),
    (
        "P04", DP,
        "self-healing never re-checks: the cleaner is declared to have worked "
        "and the gate starts anyway, so a box that is genuinely full wedges",
        "        free_b = free_fn(args.path)\n        if free_b >= min_b:",
        "        free_b = free_fn(args.path)\n        if True:",
        [O + "test_reclaim_refuses_when_it_did_not_free_enough"],
    ),
    (
        "P05", DP,
        "the remedy says `chmod -R u+w` — the fix that looks right and does "
        "nothing, because 0o300 already grants write and it is READ that is "
        "missing. This exact mistake left 15 GiB stranded.",
        '        \'     chmod -R u+rwx "$TMPDIR"/Deleting-*  &&  rm -rf "$TMPDIR"/Deleting-*\',',
        '        \'     chmod -R u+w "$TMPDIR"/Deleting-*  &&  rm -rf "$TMPDIR"/Deleting-*\',',
        [R + "test_the_chmod_is_u_plus_rwx_and_says_why_u_plus_w_is_not_enough"],
    ),
    (
        "P06", DP,
        "the refusal goes to stdout, where fast-gate tees it into the log the "
        "baseline check parses",
        "    sys.stderr.write(format_refusal(",
        "    sys.stdout.write(format_refusal(",
        [T + "test_the_refusal_goes_to_stderr_not_stdout"],
    ),
    (
        "P07", DP,
        "the simulator name match loses its word boundary, so the remedy for "
        "'iPhone 17' tells you to erase 'iPhone 17 Pro' — a destructive "
        "instruction aimed at the wrong device",
        'r"\\s+\\(([0-9A-Fa-f-]{36})\\)"',
        'r".*\\(([0-9A-Fa-f-]{36})\\)"',
        [I + "test_resolves_a_udid_from_a_name_destination"],
    ),
    (
        "P08", DP,
        "a cleaner that errored is still believed, so the refusal lists "
        "reclaim candidates parsed out of an error message",
        '    rc, out = runner([cleaner, "--dry-run"])\n    if rc != 0:',
        '    rc, out = runner([cleaner, "--dry-run"])\n    if False:',
        [S + "test_a_cleaner_that_errored_is_not_believed_even_where_it_looks_parseable"],
    ),
    (
        "P09", DP,
        "SKIP lines are read as removals, so the refusal reports space as "
        "reclaimable that the cleaner just refused to touch",
        r'_DRY_LINE = re.compile(r"^\[DRY\]\s+REMOVE\s+\(([^,]+),\s*([^)]*)\):\s*(.+)$")',
        r'_DRY_LINE = re.compile(r"^\[DRY\]\s+\w+\s+\(([^,]+),\s*([^)]*)\):\s*(.+)$")',
        [S + "test_only_REMOVE_parses_even_when_another_verb_carries_the_same_payload"],
    ),
    (
        "P10", DP,
        "the threshold is quietly restored to a pre-measurement guess, "
        "parting company with the derivation recorded beside it",
        "DEFAULT_MIN_GIB = ",
        "DEFAULT_MIN_GIB = 2.0  # ",
        [D + "test_the_default_is_stated_in_gib_and_is_plausible"],
    ),
    (
        "P11", FG,
        "the preflight's refusal no longer stops the gate — the check runs, "
        "prints, and xcodebuild starts anyway. The whole bead, defeated by one "
        "negation.",
        '  if ! python3 scripts/disk_preflight.py "${PREFLIGHT_ARGS[@]}"; then\n'
        "    exit 28",
        '  if ! python3 scripts/disk_preflight.py "${PREFLIGHT_ARGS[@]}"; then\n'
        "    :",
        [W + "test_a_short_disk_stops_the_run_BEFORE_xcodebuild"],
    ),
    (
        "P12", FG,
        "--reclaim-disk falls through to the xcodebuild passthrough, so the "
        "flag that reclaims disk breaks every run that uses it",
        "    --reclaim-disk) RECLAIM_DISK=1 ;;\n",
        "",
        [W + "test_reclaim_disk_is_consumed_and_never_forwarded_to_xcodebuild"],
    ),
    (
        "P13", DC,
        "live builds stop being resolved at all — every guard downstream still "
        "reads as present but never fires, so .xcresult bundles are pruned from "
        "under a gate that is writing them",
        '  cwd="$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n \'s/^n//p\' | head -1)"',
        '  cwd=""',
        [C + "test_a_live_build_is_resolved_by_cwd_and_skipped"],
    ),
    (
        "P14", DC,
        "permissions are repaired with u+w instead of u+rwx, so `rm -rf` dies "
        "on exactly the 0o300 directories the sweep exists to clear",
        '    chmod -R u+rwx "$path" 2>/dev/null || true',
        '    chmod -R u+w "$path" 2>/dev/null || true',
        [C + "test_permissions_are_repaired_before_rm_and_with_read"],
    ),
    (
        "P15", DC,
        "the safe-prefix fence is opened, so any path a caller computes wrong "
        "is removed rather than refused",
        '    *) log "REFUSE (outside safe prefixes): $path"; return ;;',
        '    *) : ;;',
        [C + "test_removal_is_fenced_to_the_three_safe_prefixes"],
    ),
    (
        "P16", DC,
        "an unset or hostile TMPDIR is trusted, turning `$TRASH_ROOT/Deleting-*` "
        "into a glob rooted at /",
        'case "$TRASH_ROOT" in /var/folders/*) ;; *) TRASH_ROOT="/nonexistent" ;; esac',
        ':',
        [C + "test_the_coresim_trash_root_must_look_like_a_real_TMPDIR"],
    ),
    (
        "P99", DP,
        "VACUITY CONTROL — a cosmetic comment edit that must SURVIVE. If this "
        "is killed the suite is reddening on any edit at all and every KILLED "
        "above it is worthless.",
        "GIB = 1024.0 ** 3",
        "GIB = 1024.0 ** 3  # bytes",
        [T + "test_plenty_of_room_passes_quietly_enough",
         W + "test_a_normal_run_passes_through_and_says_so_in_one_line"],
    ),
]

engine.MUTATIONS = MUTATIONS
engine.EXPECT_SURVIVE = {"P99"}

if __name__ == "__main__":
    sys.exit(engine.main())
