"""Tests for playhead-mfeq's SHAPE 2 preflight (scripts/singleton_slot_preflight.py).

The unit under test answers one question of a Swift source tree: is there an
optional stored `var` named current*/pending*/last* on an ACTOR that nobody has
signed for? The interesting cases are the ones that made the scanner wrong at
least once — a declaration quoted inside a doc comment, a function-LOCAL var, a
computed property, a nested type, and an allowlist entry that has rotted.

A rule that fires zero times because its pattern never matches is
indistinguishable from a codebase with no violations, so every positive case
below is paired with the mutation that should make it fire, and the negatives
assert the scanner is NOT simply matching everything.

Conventions follow test_disk_preflight.py: importlib module loading, in-code
fixtures, stdlib unittest, no real repo reads except the two `LiveRepo` cases
that deliberately pin the shipped tree.
"""

import importlib.util
import io
import json
import os
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


sp = _load("singleton_slot_preflight", "singleton_slot_preflight.py")


def scan(source: str, path: str = "Fake.swift"):
    return sp.scan_file(path, source)


def fields(rows):
    return sorted((r["kind"], r["type"], r["field"]) for r in rows)


class ScannerFindsTheShape(unittest.TestCase):
    def test_optional_var_on_an_actor_is_reported(self):
        rows = scan("actor S {\n    private var currentJobId: String?\n}\n")
        self.assertEqual(fields(rows), [("actor", "S", "currentJobId")])

    def test_all_three_prefixes_match(self):
        src = (
            "actor S {\n"
            "    private var currentA: String?\n"
            "    private var pendingB: Int?\n"
            "    private var lastC: Date?\n"
            "}\n"
        )
        self.assertEqual(len(scan(src)), 3)

    def test_implicitly_unwrapped_optional_is_still_optional(self):
        rows = scan("actor S {\n    var currentThing: Thing!\n}\n")
        self.assertEqual(fields(rows), [("actor", "S", "currentThing")])

    def test_spelled_out_optional_is_matched(self):
        rows = scan("actor S {\n    var currentThing: Optional<Thing>\n}\n")
        self.assertEqual(fields(rows), [("actor", "S", "currentThing")])

    def test_initialised_optional_is_matched(self):
        rows = scan("actor S {\n    private var pendingCause: Cause? = nil\n}\n")
        self.assertEqual(fields(rows), [("actor", "S", "pendingCause")])

    def test_kind_is_reported_for_non_actors_too(self):
        # The scanner sees everything; `main` is what restricts enforcement to
        # actors. Keeping the two separate is what makes --report-all able to
        # print the measured tier.
        rows = scan("final class C {\n    var currentX: String?\n}\n")
        self.assertEqual(fields(rows), [("class", "C", "currentX")])


class ScannerRejectsWhatIsNotTheShape(unittest.TestCase):
    """The vacuity direction: a scanner that matched everything would pass every
    test above and be useless."""

    def test_non_optional_is_not_a_slot(self):
        self.assertEqual(scan("actor S {\n    var currentCount: Int = 0\n}\n"), [])

    def test_computed_property_is_not_stored(self):
        self.assertEqual(scan("actor S {\n    var currentX: String? { nil }\n}\n"), [])

    def test_let_is_not_a_slot(self):
        self.assertEqual(scan("actor S {\n    let currentX: String? = nil\n}\n"), [])

    def test_name_without_a_capital_after_the_prefix_is_not_matched(self):
        # `currently...` is a different word, not the prefix.
        self.assertEqual(scan("actor S {\n    var currentlyX: String?\n}\n"), [])

    def test_unrelated_name_is_not_matched(self):
        self.assertEqual(scan("actor S {\n    var activeJobId: String?\n}\n"), [])

    def test_function_local_var_is_not_a_shared_slot(self):
        # THE REGRESSION THIS TEST EXISTS FOR. The first version of the scanner
        # reported ShadowRetryObserver.lastSeen, which is the first line of
        # `consumeMergedEvents`. A local is per-invocation — reentrancy gives
        # each call its own — so it cannot be the shared slot this rule bans,
        # and reporting it is the defect class living in the instrument.
        src = (
            "actor S {\n"
            "    private func consume() async {\n"
            "        var lastSeen: Bool? = nil\n"
            "        _ = lastSeen\n"
            "    }\n"
            "}\n"
        )
        self.assertEqual(scan(src), [])

    def test_local_var_inside_a_closure_is_not_a_shared_slot(self):
        src = (
            "actor S {\n"
            "    private func go() {\n"
            "        Task {\n"
            "            var pendingThing: Int? = nil\n"
            "            _ = pendingThing\n"
            "        }\n"
            "    }\n"
            "}\n"
        )
        self.assertEqual(scan(src), [])

    def test_declaration_quoted_in_a_line_comment_is_not_a_declaration(self):
        # AnalysisWorkScheduler.swift's own doc comments quote
        # `private var currentJobId: String?` to explain why it was deleted.
        # A scanner that reads its own tombstones reports the field forever.
        src = (
            "actor S {\n"
            "    // private var currentJobId: String?\n"
            "    /// A `private var currentEpisodeId: String?` stood here.\n"
            "    var ok: Int = 0\n"
            "}\n"
        )
        self.assertEqual(scan(src), [])

    def test_declaration_quoted_in_a_block_comment_is_not_a_declaration(self):
        src = (
            "actor S {\n"
            "    /*\n"
            "      private var pendingCancelCause: Cause?\n"
            "    */\n"
            "    var ok: Int = 0\n"
            "}\n"
        )
        self.assertEqual(scan(src), [])

    def test_declaration_inside_a_string_literal_is_not_a_declaration(self):
        src = (
            "actor S {\n"
            '    let message = "private var currentJobId: String?"\n'
            "}\n"
        )
        self.assertEqual(scan(src), [])

    def test_declaration_inside_a_multiline_string_is_not_a_declaration(self):
        src = (
            "actor S {\n"
            '    let message = """\n'
            "    private var currentJobId: String?\n"
            '    """\n'
            "}\n"
        )
        self.assertEqual(scan(src), [])


class ScannerAttributesToTheRightType(unittest.TestCase):
    def test_nested_type_owns_its_own_property(self):
        src = (
            "actor Outer {\n"
            "    private struct Inner {\n"
            "        var currentX: String?\n"
            "    }\n"
            "    private var pendingY: Int?\n"
            "}\n"
        )
        self.assertEqual(
            fields(scan(src)),
            [("actor", "Outer", "pendingY"), ("struct", "Inner", "currentX")],
        )

    def test_property_after_a_nested_type_closes_belongs_to_the_outer_actor(self):
        src = (
            "actor Outer {\n"
            "    struct Inner { let a: Int }\n"
            "    private var currentZ: String?\n"
            "}\n"
        )
        self.assertEqual(fields(scan(src)), [("actor", "Outer", "currentZ")])

    def test_actor_extension_is_recognised_as_an_extension_not_an_actor(self):
        # `extension` cannot declare stored properties in Swift, so this is
        # about not mis-attributing; enforcement keys on kind == "actor".
        src = "extension Foo {\n    var currentX: String? { nil }\n}\n"
        self.assertEqual(scan(src), [])


class EndToEnd(unittest.TestCase):
    """`main` over a temporary tree: the enforcement, the allowlist and the
    two exit directions."""

    def build(self, files: dict, allow: list):
        tmp = tempfile.mkdtemp()
        for rel, text in files.items():
            path = os.path.join(tmp, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(text)
        allow_path = os.path.join(tmp, "allow.json")
        with open(allow_path, "w", encoding="utf-8") as fh:
            json.dump({"allow": allow}, fh)
        return tmp, allow_path

    def run_main(self, tmp, allow_path, extra=()):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            rc = sp.main(
                ["--repo-root", tmp, "--roots", "Src", "--allowlist", allow_path, *extra]
            )
        return rc, out.getvalue(), err.getvalue()

    def test_unallowlisted_actor_slot_fails(self):
        tmp, allow = self.build(
            {"Src/A.swift": "actor S {\n    private var currentJobId: String?\n}\n"}, []
        )
        rc, _, err = self.run_main(tmp, allow)
        self.assertEqual(rc, 1)
        self.assertIn("S.currentJobId", err)
        self.assertIn("SINGLETON SLOT", err)

    def test_allowlisted_actor_slot_passes(self):
        tmp, allow = self.build(
            {"Src/A.swift": "actor S {\n    private var currentJobId: String?\n}\n"},
            [{"type": "S", "field": "currentJobId", "why": "bounded because reasons"}],
        )
        rc, out, _ = self.run_main(tmp, allow)
        self.assertEqual(rc, 0)
        self.assertIn("clean", out)

    def test_class_slot_is_not_enforced(self):
        tmp, allow = self.build(
            {"Src/A.swift": "final class C {\n    var currentJobId: String?\n}\n"}, []
        )
        rc, _, _ = self.run_main(tmp, allow)
        self.assertEqual(rc, 0)

    def test_report_all_still_prints_the_unenforced_population(self):
        tmp, allow = self.build(
            {"Src/A.swift": "final class C {\n    var currentJobId: String?\n}\n"}, []
        )
        rc, out, _ = self.run_main(tmp, allow, extra=["--report-all"])
        self.assertEqual(rc, 0)
        self.assertIn("C.currentJobId", out)
        self.assertIn("class", out)

    def test_stale_allowlist_entry_fails(self):
        # A licence for a field nobody can find is not evidence. This is the
        # direction allowlists rot in, and it is the one nothing usually checks.
        tmp, allow = self.build(
            {"Src/A.swift": "actor S {\n    private var ok: Int = 0\n}\n"},
            [{"type": "S", "field": "currentGone", "why": "was fine once"}],
        )
        rc, _, err = self.run_main(tmp, allow)
        self.assertEqual(rc, 1)
        self.assertIn("STALE", err)
        self.assertIn("S.currentGone", err)

    def test_rename_reports_both_directions_at_once(self):
        tmp, allow = self.build(
            {"Src/A.swift": "actor S {\n    private var currentNew: String?\n}\n"},
            [{"type": "S", "field": "currentOld", "why": "renamed out from under it"}],
        )
        rc, _, err = self.run_main(tmp, allow)
        self.assertEqual(rc, 1)
        self.assertIn("S.currentNew", err)
        self.assertIn("S.currentOld", err)

    def test_allowlist_entry_without_a_why_is_rejected(self):
        # The `why` is the whole mechanism: an allowlist of bare names is a
        # list of things nobody has thought about.
        tmp, allow = self.build(
            {"Src/A.swift": "actor S {\n    private var currentX: String?\n}\n"},
            [{"type": "S", "field": "currentX"}],
        )
        rc, _, err = self.run_main(tmp, allow)
        self.assertEqual(rc, 2)
        self.assertIn("missing", err)

    def test_missing_allowlist_file_is_an_empty_allowlist_not_a_crash(self):
        tmp, _ = self.build({"Src/A.swift": "actor S {\n    var ok: Int = 0\n}\n"}, [])
        rc, _, _ = self.run_main(tmp, os.path.join(tmp, "nope.json"))
        self.assertEqual(rc, 0)


class LiveRepo(unittest.TestCase):
    """Two assertions about the shipped tree, because a preflight that is green
    on fixtures and unrun on the repo has proved nothing."""

    def test_the_repo_is_clean_against_its_committed_allowlist(self):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            rc = sp.main([])
        self.assertEqual(rc, 0, err.getvalue())

    def test_the_three_fields_playhead_mfeq_removed_are_gone(self):
        # Not a tautology against the allowlist: these names are absent from
        # BOTH the scan and the licences, so a reader who restores one gets a
        # violation rather than a quiet pass.
        rows = sp.scan_roots(str(ROOT), ["Playhead"])
        live = {(r["type"], r["field"]) for r in rows}
        for gone in ("currentJobId", "currentEpisodeId", "pendingCancelCause"):
            self.assertNotIn(
                ("AnalysisWorkScheduler", gone),
                live,
                f"{gone} is back on AnalysisWorkScheduler — see playhead-mfeq",
            )


if __name__ == "__main__":
    unittest.main()
