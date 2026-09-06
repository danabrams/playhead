#!/usr/bin/env python3
"""playhead-awhs7 / playhead-35ohl: a buildless inventory of scripts/mutation-battery.sh.

Two things the battery's own preflights never checked:

  EXPECTATIONS  every `T_*` name a record cites must be a test that EXISTS in
                PlayheadTests/ — a Swift Testing @Test display name (single-line or
                triple-quoted) or an XCTest method (`func testX(`). The gjlp0 R2 rail
                resolves the SPELLING of every expectation against a synthetic
                console; it proves the naming systems parse, not that the names
                exist. `--series SU` and `--series Y` refused before scoring for
                weeks because one record each named a test my33/y3ya renamed.
  ANCHORS       every `snippet OLD` heredoc in apply_mutation must match its
                mutation's file EXACTLY ONCE. 79 of 1,072 matched nothing and 35
                matched more than once at 55dd7e6e (35ohl) — every one aborts with
                'anchor did not apply' and leaves its expectations unmeasured.

Exit codes: 0 clean; 2 unresolved expectations (the gate — see --check); anchors
are a REPORT (--anchors) until the known drift is repaired, so one red rule does
not make everyone route around the lint gate.
"""
import argparse
import glob
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BATTERY = os.path.join(ROOT, "scripts", "mutation-battery.sh")

CONST_RE = re.compile(r'^(T_[A-Z0-9_]+)=(?:"((?:[^"\\]|\\.)*)"|\'([^\']*)\')\s*$', re.M)
FILEVAR_RE = re.compile(r'^([A-Z][A-Z0-9_]*)="((?:Playhead|PlayheadTests|scripts)/[^"]+)"\s*$', re.M)
RECORD_RE = re.compile(r'^\s*"([A-Z]+[0-9]+)\|(\d+)\|([A-Z0-9_]+)\|([^"]*)"\s*$', re.M)
TEST_SINGLE_RE = re.compile(r'@Test\(\s*"((?:[^"\\]|\\.)+)"')
TEST_TRIPLE_RE = re.compile(r'@Test\(\s*"""\s*\n([\s\S]*?)\n\s*"""')
XCTEST_FUNC_RE = re.compile(r'\bfunc\s+(test[A-Za-z0-9_]*)\s*\(')


def _unescape_shell_double(value):
    """What bash makes of a double-quoted literal: \\` -> `, \\" -> ", \\$ -> $, \\\\ -> \\."""
    return re.sub(r'\\([`"$\\])', r'\1', value)


def parse_battery(text):
    consts = {}
    for m in CONST_RE.finditer(text):
        consts[m.group(1)] = _unescape_shell_double(m.group(2)) if m.group(2) is not None else m.group(3)
    filevars = {m.group(1): m.group(2) for m in FILEVAR_RE.finditer(text)}
    records = []
    for m in RECORD_RE.finditer(text):
        name, batch, filecode, exp = m.groups()
        refs = [r for r in exp.split(";") if r]
        records.append({"name": name, "batch": int(batch), "file": filecode, "refs": refs})
    return consts, filevars, records


def expand(ref, consts):
    """`$T_X` -> its value; a literal stays literal. None when the constant is unknown."""
    if ref.startswith("$"):
        return consts.get(ref[1:])
    return ref


def test_names(tests_root):
    display, funcs = set(), set()
    for f in glob.glob(os.path.join(tests_root, "**", "*.swift"), recursive=True):
        src = open(f, encoding="utf-8").read()
        for m in TEST_SINGLE_RE.finditer(src):
            display.add(m.group(1).replace('\\"', '"'))
        for m in TEST_TRIPLE_RE.finditer(src):
            display.add(" ".join(l.strip() for l in m.group(1).split("\n")))
        for m in XCTEST_FUNC_RE.finditer(src):
            funcs.add(m.group(1))
    return display, funcs


def unresolved_expectations(consts, records, display, funcs):
    """[(record, ref, value_or_None)] for every expectation naming no test."""
    out = []
    for rec in records:
        for ref in rec["refs"]:
            value = expand(ref, consts)
            if value is None:
                out.append((rec["name"], ref, None)); continue
            v = value
            # XCTest spellings the battery uses: `-[Class method]` and `Class/method`.
            if v.startswith("-[") and v.endswith("]"):
                v = v[2:-1].split(" ")[-1]
            elif "/" in v and " " not in v:
                v = v.split("/")[-1]
            if v in display or v in funcs:
                continue
            out.append((rec["name"], ref, value))
    return out


def anchors(text, filevars, records, root):
    """[(mutant, file, count)] for every snippet OLD whose count in its file is not 1."""
    rec_file = {r["name"]: r["file"] for r in records}
    start = text.find("apply_mutation()")
    body = text[start:] if start >= 0 else ""
    case_re = re.compile(r'^  ([A-Z]+[0-9]+)\)\n', re.M)
    cases = list(case_re.finditer(body))
    out = []
    for i, m in enumerate(cases):
        name = m.group(1)
        chunk = body[m.end(): cases[i + 1].start() if i + 1 < len(cases) else len(body)]
        olds = re.findall(r"snippet OLD <<'EOF'\n([\s\S]*?)\nEOF\n", chunk)
        code = rec_file.get(name)
        path = filevars.get(code) if code else None
        if not path:
            out.append((name, code or "?", -1)); continue
        full = os.path.join(root, path)
        try:
            src = open(full, encoding="utf-8").read()
        except OSError:
            out.append((name, path, -2)); continue
        for old in olds:
            c = src.count(old + "\n") if (old + "\n") in src else src.count(old)
            if c != 1:
                out.append((name, path, c))
    return out


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--battery", default=BATTERY)
    p.add_argument("--tests-root", default=os.path.join(ROOT, "PlayheadTests"))
    p.add_argument("--root", default=ROOT)
    p.add_argument("--check", action="store_true", help="exit 2 on any unresolved expectation")
    p.add_argument("--anchors", action="store_true", help="also report snippet OLD anchors that match != 1")
    a = p.parse_args(argv)
    text = open(a.battery, encoding="utf-8").read()
    consts, filevars, records = parse_battery(text)
    display, funcs = test_names(a.tests_root)
    bad = unresolved_expectations(consts, records, display, funcs)
    print("mutation-inventory: %d records, %d constants, %d file codes; %d display names + %d XCTest methods in the tree"
          % (len(records), len(consts), len(filevars), len(display), len(funcs)))
    if bad:
        print("mutation-inventory: %d expectation(s) name NO test:" % len(bad))
        for rec, ref, value in bad:
            print("  %-6s %-28s %s" % (rec, ref, "(unknown constant)" if value is None else repr(value)))
    else:
        print("mutation-inventory: every expectation names a test that exists")
    if a.anchors:
        drift = anchors(text, filevars, records, a.root)
        zero = [d for d in drift if d[2] == 0]; multi = [d for d in drift if d[2] > 1]; nofile = [d for d in drift if d[2] < 0]
        print("mutation-inventory: anchors — %d match nothing, %d match more than once, %d have no resolvable file"
              % (len(zero), len(multi), len(nofile)))
        for name, path, c in drift[:200]:
            print("  %-6s %-70s %s" % (name, path, {-1: "no file code", -2: "file missing"}.get(c, "x%d" % c)))
    if a.check and bad:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
