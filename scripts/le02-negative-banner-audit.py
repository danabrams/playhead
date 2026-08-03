#!/usr/bin/env python3
"""playhead-le02 — enumerate tests that cannot see a must-drop span routed to
the SUGGEST tier.

WHY THIS SCRIPT EXISTS
----------------------
playhead-d3g0 made a suggest-tier banner ARMED at registration and EMITTED only
when the playhead ENTERS the span. So in a test that never advances the
playhead, "no banner was emitted" is true by construction — it holds whether or
not the code under test did the right thing. avbn's A11 mutation proved it: it
routed a must-drop span to the suggest tier, and all five negative assertions of
SkipOrchestratorBlockedGateGuardTests stayed green.

The blind spot is WIDER than the banner stream, and that is the part worth
writing down. A suggest-tier window lands in `suggestWindows` and NOWHERE else:

    * it is not in `windows`            -> activeWindowIDs()             blind
    * it is not confirmed               -> confirmedWindows()            blind
    * it pushes no skip cue             -> pushedCues.isEmpty            blind
    * it emits no banner until entry    -> banner spool .isEmpty         blind
    * it is not an auto-skip emission   -> emittedAutoSkipBannersSnapshot blind
    * it logs no applied/confirmed      -> decision log filter           blind

Every one of those is a true statement about a span that was armed as a
suggestion. So the question this script asks is not "does the test watch the
banner stream" but the bead's actual acceptance criterion:

    would routing this span to the suggest tier FAIL this test?

Only three things answer yes, and they are the three witnesses the bead names:
`activeSuggestWindowIDs()`, the isp5 census (`lastAdWindowIngestOutcome` /
`adWindowIngestOutcomeCount`), and advancing the playhead into the span so
emission actually gets its chance.

CLASSIFICATION
--------------
A test function is reported A11-BLIND when all of:
  1. it delivers windows into a SkipOrchestrator, and
  2. it makes at least one NEGATIVE surfacing claim (something did not appear),
     and
  3. it makes NO positive landing claim (so its subject really is a span that
     must not surface, rather than a markOnly span that must arm), and
  4. it constrains neither the suggest tier nor the census, and does not
     advance the playhead.

Usage:  python3 scripts/le02-negative-banner-audit.py [--all] [--json] [PATH ...]
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_ROOT = REPO / "PlayheadTests"

# --- signal vocabularies -----------------------------------------------------
# Printed with --explain so the classification can be argued with rather than
# trusted. Deliberately over-broad on the WITNESS side: a false "protected" only
# costs a re-read, a false "blind" wastes a repair.

DELIVERS = [
    r"\breceiveAdWindows\(",
    r"\bingestPersistedAdWindows\(",
    r"\breceiveAdDecisionResults\(",
    r"\bbeginEpisode\(",
]

# A negative claim that a span did not surface. Any of these is blind to a
# suggest-tier arming — see the module docstring for why each one is.
NEGATIVE_SURFACING = [
    r"#expect\(\s*!\s*\w*[Cc]onfirmed\w*\.contains",
    r"#expect\(\s*!\s*\w*[Aa]ctive\w*\.contains",
    r"#expect\(\s*!\s*\w*[Ee]mitted\w*\.contains",
    r"#expect\(\s*!\s*\w*[Bb]anner\w*\.contains",
    r"#expect\(\s*!\s*\w*[Cc]ue\w*\.contains",
    r"#expect\(\s*!\(?\s*await\s+\w+\.activeWindowIDs\(\)\.contains",
    r"#expect\(\s*!\(?\s*await\s+\w+\.emittedAutoSkipBannersSnapshot\(\)\.contains",
    r"#expect\(\s*\w*(?:[Bb]anner|received|emitted|items|[Cc]ue|[Ss]uggest|auto)\w*\.isEmpty",
    r"#expect\(\s*await\s+\w+\.activeWindowIDs\(\)\.isEmpty",
    r"#expect\(\s*\w*(?:[Bb]anner|received|emitted|items|[Cc]ue)\w*\.count\s*==\s*0",
    r"XCTAssertFalse\(\s*\w*(?:[Ee]mitted|[Bb]anner|[Aa]ctive|[Cc]onfirmed)\w*\.contains",
    r"XCTAssertTrue\(\s*\w*(?:[Bb]anner|received|emitted|items|[Cc]ue)\w*\.isEmpty",
    r"XCTAssertEqual\(\s*\w*(?:[Bb]anner|received|emitted|items|[Cc]ue)\w*\.count,\s*0",
]

# A POSITIVE claim that the delivered span DID land somewhere. Its presence
# means the test's subject is not a must-drop span, so the blind spot does not
# apply in the same way.
POSITIVE_LANDING = [
    r"#expect\(\s*\w*[Cc]onfirmed\w*\.contains",
    r"#expect\(\s*(?:await\s+)?\w+\.activeWindowIDs\(\)\.contains",
    r"#expect\(\s*!\s*\w*(?:[Bb]anner|received|emitted|items|[Cc]ue)\w*\.isEmpty",
    r"XCTAssertTrue\(\s*\w*(?:[Cc]onfirmed|[Aa]ctive)\w*\.contains",
    r"precondition: valid material must be active",
]

# THE THREE WITNESSES that actually see a suggest-tier arming.
SUGGEST_WITNESS = [
    (r"\bactiveSuggestWindowIDs\(", "activeSuggestWindowIDs"),
    (r"\blastAdWindowIngestOutcome\(", "isp5 census row"),
    (r"\badWindowIngestOutcomeCount\(", "isp5 counter"),
    (r"\backnowledgedSuggestWindowIDs\(", "acknowledgedSuggestWindowIDs"),
    (r"AdWindowIngestOutcome\.", "names a disposition"),
]

ADVANCES_PLAYHEAD = [
    r"\bupdatePlayheadTime\(",
    r"\bhandlePlayheadTime\(",
    r"\bsetPlayheadTime\(",
]

# Does the scenario EXPECT the span to reach the suggest tier?
#
# This splits the blind set into two populations that need different repairs.
# A MUST-DEMOTE test hands in a markOnly span, and "did not auto-skip" is the
# whole claim — arming a suggestion is the CORRECT outcome, so the A11 mutation
# (route to suggest) does not describe a defect there. Its blind spot is the
# mirror one: nothing proves the demotion actually armed rather than vanishing.
# A MUST-DROP test hands in a span that should reach no tier at all, and that is
# the population the bead's acceptance criterion names.
EXPECTS_SUGGEST_TIER = [
    r"[Mm]arkOnly",
    r"\.suggest\b",
    r"[Ss]uggestWindow",
    r"[Ss]uggested",
    r"[Ss]uggestion",
]

TEST_DECL = re.compile(
    r"^\s*(?:private\s+|public\s+|internal\s+)?func\s+(\w+)\s*\(",
    re.MULTILINE,
)
SUITE_DECL = re.compile(
    r"^\s*(?:final\s+)?(?:struct|class)\s+(\w+)", re.MULTILINE)


def matched(patterns: list[str], text: str) -> list[str]:
    return [p for p in patterns if re.search(p, text)]


def witnesses(text: str) -> list[str]:
    return [label for pat, label in SUGGEST_WITNESS if re.search(pat, text)]


def split_functions(src: str) -> list[tuple[str, str, int]]:
    """(name, body, line) for every func, brace-balanced from its body brace."""
    out: list[tuple[str, str, int]] = []
    for m in TEST_DECL.finditer(src):
        name = m.group(1)
        i = src.find("{", m.end())
        if i < 0:
            continue
        depth, j = 0, i
        while j < len(src):
            if src[j] == "{":
                depth += 1
            elif src[j] == "}":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        out.append((name, src[m.start():j + 1], src.count("\n", 0, m.start()) + 1))
    return out


def classify(path: pathlib.Path) -> list[dict]:
    src = path.read_text(encoding="utf-8", errors="replace")
    suite_m = SUITE_DECL.search(src)
    suite = suite_m.group(1) if suite_m else path.stem
    rows = []
    for name, body, line in split_functions(src):
        neg = matched(NEGATIVE_SURFACING, body)
        if not neg or not matched(DELIVERS, body):
            continue
        pos = matched(POSITIVE_LANDING, body)
        wit = witnesses(body)
        adv = bool(matched(ADVANCES_PLAYHEAD, body))
        blind = (not wit) and (not adv) and (not pos)
        expects_suggest = bool(matched(EXPECTS_SUGGEST_TIER, body))
        rows.append({
            "file": str(path.relative_to(REPO)),
            "suite": suite,
            "test": name,
            "line": line,
            "negative_claims": len(neg),
            "has_positive_landing": bool(pos),
            "suggest_witnesses": wit,
            "advances_playhead": adv,
            "a11_blind": blind,
            "population": ("must-demote" if expects_suggest else "must-drop"),
        })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="*", default=[])
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    roots = [pathlib.Path(p) for p in args.paths] or [DEFAULT_ROOT]
    files: list[pathlib.Path] = []
    for r in roots:
        if r.is_dir():
            files.extend(sorted(r.rglob("*.swift")))
        elif r.exists():
            files.append(r)

    rows: list[dict] = []
    for f in files:
        rows.extend(classify(f))

    if args.json:
        print(json.dumps(rows, indent=2))
        return 0

    blind = [r for r in rows if r["a11_blind"]]
    seeing = [r for r in rows if not r["a11_blind"]]

    print(f"scanned {len(files)} files")
    print(f"{len(rows)} test functions deliver windows AND make a negative "
          f"surfacing claim")
    print(f"  A11-BLIND (no suggest witness, no advance, no positive landing): "
          f"{len(blind)}")
    print(f"  can see a suggest arming:                                       "
          f"{len(seeing)}")

    def dump(rs: list[dict], header: str, annotate) -> None:
        print(f"\n=== {header} ===")
        by_file: dict[str, list[dict]] = {}
        for r in rs:
            by_file.setdefault(r["file"], []).append(r)
        for f in sorted(by_file):
            print(f"\n{f}")
            for r in sorted(by_file[f], key=lambda x: x["line"]):
                print(f"    :{r['line']:<5} {r['test']}  {annotate(r)}")

    drop = [r for r in blind if r["population"] == "must-drop"]
    demote = [r for r in blind if r["population"] == "must-demote"]
    print(f"    of which must-drop  (the bead's A11 criterion): {len(drop)}")
    print(f"    of which must-demote (the mirror blind spot):   {len(demote)}")

    dump(drop, "A11-BLIND / MUST-DROP",
         lambda r: f"({r['negative_claims']} negative claim(s))")
    dump(demote, "A11-BLIND / MUST-DEMOTE (mirror: nothing proves it armed)",
         lambda r: f"({r['negative_claims']} negative claim(s))")
    if args.all:
        def why(r: dict) -> str:
            bits = []
            if r["suggest_witnesses"]:
                bits.append("witness: " + ", ".join(r["suggest_witnesses"]))
            if r["advances_playhead"]:
                bits.append("advances playhead")
            if r["has_positive_landing"]:
                bits.append("asserts a positive landing")
            return "[" + "; ".join(bits) + "]"
        dump(seeing, "CAN SEE A SUGGEST ARMING", why)

    return 0


if __name__ == "__main__":
    sys.exit(main())
