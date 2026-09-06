"""playhead-6yxms: a mutation NAME registered twice in MUTATIONS is refused.

Two rows under one key: `--only` selects both, the case runs twice against the
same source, and every reader of RESULTS resolves the name by FIRST match — so
the second row's verdict is never the one you read. L09 sat in the table twice
(batch 61, PODC) with different expectation sets; this rail keeps the table
unique, and proves the refusal by re-introducing a duplicate on a same-directory
copy of the script (relative paths must still resolve).
"""
import os
import shutil
import subprocess
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT = os.path.join(ROOT, "scripts", "mutation-battery.sh")


def _list(script):
    return subprocess.run(["bash", script, "--list"], cwd=ROOT, capture_output=True,
                          text=True, timeout=300)


class MutationNamesTests(unittest.TestCase):
    def test_the_real_table_has_no_duplicate_name_and_lists(self):
        proc = _list(SCRIPT)
        out = proc.stdout + proc.stderr
        self.assertEqual(proc.returncode, 0, out[-1200:])
        self.assertNotIn("registered twice", out)
        rows = [l for l in proc.stdout.splitlines() if l[:1].isalpha() and l.split()[0] != "NAME"]
        names = [l.split()[0] for l in rows]
        self.assertGreater(len(names), 1000, "the listing is the whole table")
        self.assertEqual(len(names), len(set(names)), "a name appears twice in --list")

    def test_a_duplicate_name_is_refused_with_exit_2_and_named(self):
        with open(SCRIPT) as f:
            src = f.read()
        marker = '  "V06|67|PODC|$T_EVC1_GATE_BLIND"\n'
        self.assertEqual(src.count(marker), 1, "the anchor this rail duplicates has moved")
        copy = os.path.join(ROOT, "scripts", "mutation-battery-dupname-rail-tmp.sh")
        try:
            with open(copy, "w") as f:
                f.write(src.replace(marker, marker + marker, 1))
            shutil.copymode(SCRIPT, copy)
            proc = _list(copy)
            out = proc.stdout + proc.stderr
            self.assertEqual(proc.returncode, 2, out[-1200:])
            self.assertIn("registered twice", out)
            self.assertIn("V06", out)
        finally:
            if os.path.exists(copy):
                os.remove(copy)


if __name__ == "__main__":
    unittest.main()
