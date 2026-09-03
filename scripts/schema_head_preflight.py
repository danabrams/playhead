#!/usr/bin/env python3
"""Fail when a test asserts a schema head the app no longer has.

playhead-x1lbr. `AnalysisStore.currentSchemaVersion` is a constant, and 18
places in the test tree compare a LITERAL against it or against the migrated
store's `schemaVersion()`. playhead-jra6 (#482) moved the constant 66 -> 67 and
updated none of them, so `main` shipped 18 deterministic failures that only a
full 46-minute plan could see. Its own gate was scoped, which is the standing
rule and was not wrong; nothing between a scoped gate and the next full plan
could observe the breakage.

This is the whole class, caught in about a fifth of a second with no build.

CLOSED IN BOTH DIRECTIONS, deliberately, because the failure mode this repo
keeps finding is a guard that names an ABSENCE and whose false branch makes no
claim. A version of this script that only compared the numbers it happened to
find would pass silently if the constant were renamed, if the assertion
convention changed, or if the regex rotted — reporting "clean" while checking
nothing. So:

  * the constant must be READABLE, or exit 2;
  * at least one assertion site must MATCH, or exit 2. Zero sites is not a
    clean tree, it is a scan that has stopped seeing its population.
"""

import re
import sys
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
STORE = REPO_ROOT / "Playhead/Persistence/AnalysisStore/AnalysisStore.swift"
TESTS = REPO_ROOT / "PlayheadTests"

CONSTANT_RE = re.compile(
    r"^\s*(?:nonisolated\s+)?static\s+let\s+currentSchemaVersion\s*=\s*(\d+)\s*$",
    re.MULTILINE,
)
# ONLY the constant. `store.schemaVersion() == N` is deliberately NOT matched,
# and the first draft of this script matched it: 35 sites fired, of which 17
# were correct. A migration rung seeds a v38 database and asserts
# `schemaVersion() == 38` BEFORE migrating — that names the store's version at a
# point in the test, not the head, and it must not move when the head moves.
# The two spellings are the standing defect class in miniature: one value read
# as though it named another, and nothing in the syntax tells them apart.
#
# `AnalysisStore.currentSchemaVersion == N` has no such ambiguity. It reads the
# constant, so a literal beside it can only be a claim about the head. That is
# 16 of the 18 sites playhead-jra6 left stale; the other two are
# `schemaVersion()` calls that do mean head, and this preflight cannot see them.
# Stating that limit is worth more than a check that cries wolf on 17 correct
# assertions until someone deletes it.
#
# `!=` is not matched either — a rung asserting the head is not some older
# number stays true as the head moves.
ASSERTION_RE = re.compile(
    r"AnalysisStore\.currentSchemaVersion\s*==\s*(\d+)"
)


def read_head(source):
    # type: (str) -> Optional[int]
    found = CONSTANT_RE.findall(source)
    return int(found[0]) if len(found) == 1 else None


def scan(tests_dir: Path, head: int):
    """Return (violations, total_sites). A violation is (path, line, claimed)."""
    violations = []
    total = 0
    for path in sorted(tests_dir.rglob("*.swift")):
        for number, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            for claimed in ASSERTION_RE.findall(line):
                total += 1
                if int(claimed) != head:
                    violations.append((path, number, int(claimed)))
    return violations, total


def main() -> int:
    if not STORE.is_file():
        print(
            f"schema-head: cannot read {STORE.relative_to(REPO_ROOT)} — "
            "this preflight is checking NOTHING",
            file=sys.stderr,
        )
        return 2
    head = read_head(STORE.read_text(encoding="utf-8"))
    if head is None:
        print(
            "schema-head: could not read a single `currentSchemaVersion = N` "
            "from AnalysisStore.swift. The constant was renamed or duplicated; "
            "this preflight is checking NOTHING until that is fixed.",
            file=sys.stderr,
        )
        return 2

    violations, total = scan(TESTS, head)

    if total == 0:
        print(
            "schema-head: found ZERO head assertions in PlayheadTests. That is "
            "not a clean tree — the convention changed or this scan has stopped "
            "seeing its population. Fix the pattern, do not delete the check.",
            file=sys.stderr,
        )
        return 2

    if violations:
        print(
            f"schema-head: {len(violations)} assertion(s) name a head the app "
            f"does not have (currentSchemaVersion is {head}):",
            file=sys.stderr,
        )
        for path, number, claimed in violations:
            print(
                f"  {path.relative_to(REPO_ROOT)}:{number}: asserts {claimed}",
                file=sys.stderr,
            )
        print(
            "schema-head: a bead that moves the head owes every one of these. "
            "They fail deterministically on every run, and only a full plan "
            "sees them.",
            file=sys.stderr,
        )
        return 2

    print(f"schema-head: clean ({total} head assertion(s) all name {head})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
