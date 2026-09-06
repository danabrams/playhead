#!/usr/bin/env python3
"""playhead-y27o — the LAST line fast-gate.sh prints, and the exit code that goes with it.

Two ways a gate run used to read as green while it did nothing: a toolchain that
died in a second (two lines that scrolled past), and `scripts/fast-gate.sh | tail`
returning TAIL's exit code. Neither is a logic bug; both are the absence of a line
that says the verdict. This prints one, always, and applies one rule of its own:
a run that EXECUTED ZERO TESTS is a FAIL whatever xcodebuild's exit code was —
'zero tests executed' has read as success three times on this box (playhead-y27o's
notes), and a run that runs nothing is not a pass.

    python3 scripts/gate_terminal_line.py --log <console log> --rc <rc>

Prints `fast-gate: PASS (N tests: S swift-testing + X xctest)` or
`fast-gate: FAIL rc=N ...` and exits with the FINAL code: `rc` itself, or 1 when
`rc` was 0 and nothing ran. Counts are taken from the LAST summary of each
framework in the log (the residual re-run overwrites the console's first pass).
"""
import argparse
import os
import re
import sys

SWIFT_TESTING = re.compile(r"Test run with (\d+) tests? in \d+ suites?")
XCTEST = re.compile(r"Executed (\d+) tests?, with (\d+) failures?")


def counts(log_text):
    """(swift_testing, xctest) executed counts from the LAST summary of each."""
    st = [int(m.group(1)) for m in SWIFT_TESTING.finditer(log_text)]
    xc = [int(m.group(1)) for m in XCTEST.finditer(log_text)]
    return (st[-1] if st else 0, xc[-1] if xc else 0)


def terminal_line(log_text, rc):
    """Return (line, final_rc)."""
    st, xc = counts(log_text)
    total = st + xc
    if rc == 0 and total == 0:
        return ("fast-gate: FAIL rc=0 — zero tests executed; a run that runs nothing is not a pass "
                "(a misspelled -only-testing: is silently skipped; a test file added after the last "
                "xcodegen generate is not in the target)", 1)
    if rc == 0:
        return ("fast-gate: PASS (%d tests: %d swift-testing + %d xctest)" % (total, st, xc), 0)
    return ("fast-gate: FAIL rc=%d (%d tests executed: %d swift-testing + %d xctest) — the verdict is above"
            % (rc, total, st, xc), rc)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("--rc", type=int, required=True)
    args = parser.parse_args(argv)
    text = ""
    if os.path.exists(args.log):
        with open(args.log, errors="replace") as handle:
            text = handle.read()
    line, final = terminal_line(text, args.rc)
    print(line)
    sys.stdout.flush()
    return final


if __name__ == "__main__":
    sys.exit(main())
