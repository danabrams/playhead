#!/usr/bin/env python3
"""Ban SHAPE 2 of the standing defect class: a singleton standing for a set.

WHY THIS EXISTS (playhead-mfeq)
-------------------------------
The standing defect class in this repo is *a value that names one thing, read
as though it named another*, and it has ~19 shipped instances. Vigilance has
the failure record; types have the success record. Dan, 2026-08-06: **"Compiler
guarding is best."** So the class is treated here as a MANUFACTURING problem
rather than a reading one — ban the shape that keeps producing it.

SHAPE 2 is an optional stored `var` holding "the current X" on a type where
more than one X can exist at a time. The slot names the MOST RECENT X and is
read as THE X, and the two are equal on every fixture anybody writes, which is
why review keeps missing it. One rule catches four shipped defects:

  * playhead-mk6z  `currentJobId` — the last DISPATCHED job read as the running
                   one, so the expired-lease sweep reclaimed a live job's lease.
  * playhead-lmrx F4  a single `currentRunningTask` slot: a job finishing
                   normally cancelled an unrelated job mid-run.
  * playhead-lmrx F9  a global `pendingCancelCause`, destructively read: the
                   second of two cancelled jobs fell back to `.pipelineError`,
                   took the attempt-SPENDING arm, and superseded an episode
                   permanently.
  * playhead-lmrx R3  `leaseRenewalTask`, same slot shape: one job's heartbeat
                   cancelled by another job's `defer`.

WHAT "MORE THAN ONE X" MEANS MECHANICALLY
-----------------------------------------
`actor`. An actor is REENTRANT: every `await` in a method is a point at which
another call can enter, so any actor with `async` methods can have several
logical operations in flight against one set of stored properties. That is
exactly the precondition — and it is decidable from the source text, which
"this type's concurrency cap exceeds one" is not.

Deliberately NOT included, and it is a judgement call rather than an oversight:
`class` and `struct`. Measured over `Playhead/` at the time this shipped, the
same name pattern matches 24 class properties and 19 struct properties, and the
overwhelming majority are genuinely singular — `PlayheadRuntime.currentEpisodeId`
describes the ONE loaded playback session, `FeedParser.currentEpisode` is an XML
parser's cursor over a document it reads once, `AttemptState.lastAttemptAt` is a
scalar reading about the type itself. Firing on those would put ~43 entries in
the allowlist, and an allowlist that large stops being read. If a
non-actor type acquires a genuine concurrent population, `--report-all` prints
the whole measured population so the next person can re-tier it with numbers
rather than taste.

THE ALLOWLIST IS CLOSED, WHICH IS THE HALF THAT USUALLY ROTS
------------------------------------------------------------
An entry that matches nothing is an ERROR, not a shrug. An allowlist whose
entries have been renamed out from under it is indistinguishable from one that
is doing its job, and it silently grants amnesty to whatever inherits the name.
This is the same rule `gate_baseline.py` applies to a baseline member that did
not run.

EXIT CODES
  0   no un-allowlisted violations, and every allowlist entry matched
  1   a violation, or a stale allowlist entry
  2   bad usage / unreadable allowlist
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

# The name prefixes that mark a slot as naming "the one that is current".
# `^(current|pending|last)[A-Z]` — the capital is what keeps `currently`,
# `pendingCount` (no capital after the prefix... `Count` has one, so that WOULD
# match; see `is_identity_shaped` for why that is fine) and `lastly` out.
NAME_RE = re.compile(r"^(current|pending|last)[A-Z]")

# A stored `var` with an explicit type annotation. Swift's grammar is not
# regular and this is not a parser: it is a line-shaped matcher run over source
# with comments and string literals blanked out, which is enough for a
# declaration that idiomatically lives on one line. A property whose
# declaration is split across lines is a KNOWN BLIND SPOT (limit L-1).
DECL_RE = re.compile(
    r"""^\s*
    (?:(?:@[\w.]+(?:\([^)]*\))?\s+)*)                       # attributes
    (?:(?:private|fileprivate|internal|public|open|package)
       (?:\(set\))?\s+)*                                    # access control
    (?:static\s+|class\s+|final\s+|lazy\s+|weak\s+
       |unowned(?:\([^)]*\))?\s+|nonisolated(?:\([^)]*\))?\s+)*
    var\s+(?P<name>[A-Za-z_]\w*)\s*:\s*(?P<type>.+?)$
    """,
    re.VERBOSE,
)

TYPE_RE = re.compile(
    r"""^\s*
    (?:(?:@[\w.]+(?:\([^)]*\))?\s+)*)
    (?:(?:private|fileprivate|internal|public|open|package)\s+)*
    (?:final\s+|indirect\s+)*
    (?P<kind>actor|class|struct|enum|extension|protocol)\s+
    (?P<name>[A-Za-z_]\w*)
    """,
    re.VERBOSE,
)

DEFAULT_ROOTS = ("Playhead",)
DEFAULT_ALLOWLIST = "scripts/singleton-slot-allowlist.json"
SKIP_DIRS = {".derivedData", ".build", "build", ".git", "DerivedData"}


def strip_noise(src: str) -> str:
    """Blank comments and string-literal bodies, preserving line structure.

    Without this the scanner reads its own documentation: every doc comment in
    `AnalysisWorkScheduler.swift` that quotes `private var currentJobId: String?`
    to explain why it was deleted would be reported as a live declaration. That
    is this repo's standing defect class inside the instrument — text that
    DESCRIBES a thing read as evidence the thing is there — and it is the
    identical bug `gate_baseline.py` hit when a header reading
    "no -only-testing" was matched as a selective run.
    """
    out: list[str] = []
    i, n = 0, len(src)
    block_depth = 0
    while i < n:
        ch = src[i]
        if block_depth:
            if src.startswith("/*", i):
                block_depth += 1
                out.append("  ")
                i += 2
                continue
            if src.startswith("*/", i):
                block_depth -= 1
                out.append("  ")
                i += 2
                continue
            out.append("\n" if ch == "\n" else " ")
            i += 1
            continue
        if src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
            continue
        if src.startswith("/*", i):
            block_depth = 1
            out.append("  ")
            i += 2
            continue
        if ch == '"':
            if src.startswith('"""', i):
                j = src.find('"""', i + 3)
                j = n if j < 0 else j + 3
                out.append("".join("\n" if c == "\n" else " " for c in src[i:j]))
                i = j
                continue
            j = i + 1
            while j < n and src[j] != '"':
                if src[j] == "\\":
                    j += 1
                if j < n and src[j] == "\n":
                    break
                j += 1
            j = min(j + 1, n)
            out.append(" " * (j - i))
            i = j
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _annotation(type_text: str) -> str:
    """The declared type, with any initialiser or accessor block cut off."""
    t = type_text
    for cut in ("=", "{"):
        p = t.find(cut)
        if p >= 0:
            t = t[:p]
    return t.strip().rstrip(",")


def is_optional(type_text: str) -> bool:
    t = _annotation(type_text)
    if not t:
        return False
    if t.startswith("Optional<") or t.startswith("Optional "):
        return True
    return t.endswith("?") or t.endswith("!")


def has_accessor_block(type_text: str) -> bool:
    """`var x: T? { ... }` is computed, not a stored slot — there is nothing to
    go stale, because it is recomputed on every read."""
    eq = type_text.find("=")
    brace = type_text.find("{")
    return brace >= 0 and (eq < 0 or brace < eq)


def scan_file(path: str, text: str) -> list[dict]:
    """Every optional stored `var` matching the prefix, with its owning type."""
    src = strip_noise(text)
    found: list[dict] = []
    stack: list[tuple[str, str, int]] = []
    depth = 0
    for lineno, line in enumerate(src.split("\n"), 1):
        pending_type = None
        m = TYPE_RE.match(line)
        if m and "{" in line:
            pending_type = (m.group("kind"), m.group("name"))
        d = DECL_RE.match(line)
        if d:
            type_text = d.group("type")
            name = d.group("name")
            # MEMBER DEPTH, and it is not a nicety. `depth` here is the brace
            # depth BEFORE this line's own braces, so a stored property sits at
            # exactly the depth the enclosing type was opened at; anything
            # deeper is a LOCAL `var` inside a method. A local is per-invocation
            # — reentrancy gives each call its own — so it is not a shared slot
            # and cannot be the defect this rule bans.
            #
            # The first version of this scanner had no such check and reported
            # `ShadowRetryObserver.lastSeen`, which is `var lastSeen: Bool? = nil`
            # on the first line of `consumeMergedEvents`. A rule that cannot tell
            # a shared slot from a local names one thing and is read as naming
            # another, which is the defect class it exists to catch, living in
            # the instrument. Found by reading all nine hits before writing nine
            # justifications, which is the only reason to write them by hand.
            is_member = bool(stack) and depth == stack[-1][2]
            if (
                is_member
                and NAME_RE.match(name)
                and not has_accessor_block(type_text)
                and is_optional(type_text)
            ):
                kind, owner = (stack[-1][0], stack[-1][1]) if stack else ("<file>", "<file>")
                found.append(
                    {
                        "file": path,
                        "line": lineno,
                        "kind": kind,
                        "type": owner,
                        "field": name,
                        "declared": _annotation(type_text),
                    }
                )
        for ch in line:
            if ch == "{":
                depth += 1
                if pending_type is not None:
                    stack.append((pending_type[0], pending_type[1], depth))
                    pending_type = None
            elif ch == "}":
                if stack and stack[-1][2] == depth:
                    stack.pop()
                depth -= 1
    return found


def scan_roots(repo_root: str, roots) -> list[dict]:
    rows: list[dict] = []
    for root in roots:
        abs_root = os.path.join(repo_root, root)
        for dirpath, dirnames, filenames in os.walk(abs_root):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
            for fn in sorted(filenames):
                if not fn.endswith(".swift"):
                    continue
                abs_path = os.path.join(dirpath, fn)
                rel = os.path.relpath(abs_path, repo_root)
                with open(abs_path, encoding="utf-8", errors="replace") as fh:
                    rows.extend(scan_file(rel, fh.read()))
    rows.sort(key=lambda r: (r["file"], r["line"]))
    return rows


def load_allowlist(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    entries = data.get("allow", [])
    for e in entries:
        missing = [k for k in ("type", "field", "why") if not e.get(k)]
        if missing:
            raise ValueError(f"allowlist entry {e!r} is missing {missing}")
    return entries


def key(row) -> tuple[str, str]:
    return (row["type"], row["field"])


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--repo-root", default=None)
    parser.add_argument("--roots", nargs="*", default=list(DEFAULT_ROOTS))
    parser.add_argument("--allowlist", default=None)
    parser.add_argument(
        "--report-all",
        action="store_true",
        help="print every match on every type kind, not just the enforced actor "
             "population — the measured tier, for re-deciding the scope later",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    repo_root = args.repo_root or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    allow_path = args.allowlist or os.path.join(repo_root, DEFAULT_ALLOWLIST)

    try:
        allow = load_allowlist(allow_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"singleton-slot: cannot read allowlist {allow_path}: {exc}", file=sys.stderr)
        return 2

    rows = scan_roots(repo_root, args.roots)
    enforced = [r for r in rows if r["kind"] == "actor"]

    if args.report_all:
        by_kind: dict[str, int] = {}
        for r in rows:
            by_kind[r["kind"]] = by_kind.get(r["kind"], 0) + 1
        print(f"singleton-slot: {len(rows)} match(es) of ^(current|pending|last)[A-Z] "
              f"on an optional stored var under {args.roots}")
        for k in sorted(by_kind, key=lambda k: -by_kind[k]):
            print(f"    {k:<10} {by_kind[k]}"
                  + ("   <- ENFORCED" if k == "actor" else ""))
        for r in rows:
            print(f"    {r['kind']:<9} {r['type']}.{r['field']}: {r['declared']}  "
                  f"({r['file']}:{r['line']})")

    allow_keys = {(e["type"], e["field"]): e for e in allow}
    seen_keys = {key(r) for r in enforced}

    violations = [r for r in enforced if key(r) not in allow_keys]
    stale = [e for k, e in allow_keys.items() if k not in seen_keys]

    if args.json:
        print(json.dumps({"violations": violations, "stale_allowlist": stale}, indent=2))

    rc = 0
    if violations:
        rc = 1
        print(
            f"singleton-slot: {len(violations)} SINGLETON SLOT(S) on a reentrant actor",
            file=sys.stderr,
        )
        for r in violations:
            print(f"  {r['file']}:{r['line']}: {r['type']}.{r['field']}: {r['declared']}",
                  file=sys.stderr)
        print(
            "\n  An optional `var` named current*/pending*/last* on an ACTOR names the\n"
            "  most recent one and gets read as THE one. An actor is reentrant, so more\n"
            "  than one operation can be in flight against it (playhead-mk6z, and\n"
            "  playhead-lmrx F4/F9/R3 — four shipped defects, one shape).\n"
            "\n  Fix it by keying the state on the identity it belongs to — a\n"
            "  `[String: State]` registry, as AnalysisWorkScheduler.runningJobs does —\n"
            "  or by deleting the slot if nothing reads it. If the population really is\n"
            f"  singular, add it to {os.path.relpath(allow_path, repo_root)} with a WHY\n"
            "  that says what bounds it to one.",
            file=sys.stderr,
        )
    if stale:
        rc = 1
        print(
            f"\nsingleton-slot: {len(stale)} STALE allowlist entr(ies) — nothing matches them",
            file=sys.stderr,
        )
        for e in stale:
            print(f"  {e['type']}.{e['field']}", file=sys.stderr)
        print(
            "\n  A licence for a field nobody can find is not evidence of anything. It\n"
            "  was renamed, moved off its actor or deleted — and whatever inherits the\n"
            "  name inherits the amnesty. Update the entry or remove it.",
            file=sys.stderr,
        )

    if rc == 0 and not args.report_all and not args.json:
        print(f"singleton-slot: clean ({len(enforced)} actor slot(s), all {len(allow)} allowlisted)")
    return rc


if __name__ == "__main__":
    sys.exit(main())
