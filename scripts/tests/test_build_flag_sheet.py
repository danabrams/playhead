"""Tests for the cohort flag sheet generator (scripts/build_flag_sheet.py).

playhead-i7kvl.4. The sheet exists so a tester report can be attributed to a
build. Its whole value is that it CANNOT drift from the code, so the cases here
are about the ways a derived document silently stops being derived.
"""

import importlib.util
import pathlib
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


bfs = _load("build_flag_sheet", "build_flag_sheet.py")


class BuildFlagSheetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def write(self, relative: str, body: str):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")

    def test_reads_a_static_let(self):
        self.write("A.swift", "struct S {\n    static let flag = true\n}\n")
        self.assertEqual(bfs.read_value(self.root, "flag", "A.swift"), "true")

    def test_reads_a_nonisolated_static_let(self):
        self.write("A.swift", "    nonisolated static let cap = 25\n")
        self.assertEqual(bfs.read_value(self.root, "cap", "A.swift"), "25")

    def test_reads_a_private_static_let_with_a_type(self):
        self.write("A.swift", "    private static let dwell: TimeInterval = 8.0\n")
        self.assertEqual(bfs.read_value(self.root, "dwell", "A.swift"), "8.0")

    def test_reads_a_FILE_SCOPE_let(self):
        """The case `--check` caught. A static-only pattern silently dropped
        `episodePreparationCompleteThreshold`, and a sheet missing a row still
        looks complete."""
        self.write("A.swift", "let threshold = ReachRatio(0.98)\n")
        self.assertEqual(
            bfs.read_value(self.root, "threshold", "A.swift"), "ReachRatio(0.98)"
        )

    def test_a_missing_constant_is_None_not_empty(self):
        self.write("A.swift", "let other = 1\n")
        self.assertIsNone(bfs.read_value(self.root, "flag", "A.swift"))

    def test_a_missing_FILE_is_None(self):
        self.assertIsNone(bfs.read_value(self.root, "flag", "nope.swift"))

    def test_a_similarly_named_constant_does_not_match(self):
        """`flagExtra` must not satisfy a lookup for `flag` — a near-miss that
        reports the wrong value is worse than one that reports none."""
        self.write("A.swift", "    static let flagExtra = 99\n")
        self.assertIsNone(bfs.read_value(self.root, "flag", "A.swift"))

    def test_every_listed_flag_resolves_against_the_real_tree(self):
        """THE ACCEPTANCE. This is the check that fails when somebody renames a
        constant, which is the only way the sheet can go stale."""
        self.assertEqual(bfs.main(["--check"]), 0)

    def test_check_fails_loudly_when_a_flag_cannot_be_read(self):
        original = bfs.FLAGS
        try:
            bfs.FLAGS = original + [("nonexistentFlag", "Playhead/App/PlayheadApp.swift", "x")]
            self.assertEqual(bfs.main(["--check"]), 1)
        finally:
            bfs.FLAGS = original

    def test_generated_sheet_names_its_commit_and_every_flag(self):
        out = self.root / "sheet.md"
        self.assertEqual(bfs.main(["-o", str(out)]), 0)
        text = out.read_text(encoding="utf-8")
        self.assertIn("Commit:", text)
        self.assertIn("do not edit by hand", text)
        for constant, _, _ in bfs.FLAGS:
            self.assertIn(f"`{constant}`", text)
        self.assertNotIn("NOT FOUND", text)


if __name__ == "__main__":
    unittest.main()
