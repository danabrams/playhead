"""Rails for scripts/schema_head_preflight.py (playhead-x1lbr).

The point of these is the CLOSED-IN-BOTH-DIRECTIONS property. A preflight that
only compares numbers it happens to find passes silently once its population
disappears, which is the failure mode this repo names most often: a guard whose
false branch makes no claim.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import schema_head_preflight as preflight  # noqa: E402


class ReadHeadTests(unittest.TestCase):
    def test_reads_the_constant(self):
        source = "    nonisolated static let currentSchemaVersion = 67\n"
        self.assertEqual(preflight.read_head(source), 67)

    def test_reads_it_without_the_nonisolated_modifier(self):
        self.assertEqual(
            preflight.read_head("    static let currentSchemaVersion = 12\n"), 12
        )

    def test_a_renamed_constant_reads_as_unreadable_not_as_zero(self):
        source = "    static let schemaHead = 67\n"
        self.assertIsNone(preflight.read_head(source))

    def test_two_declarations_are_unreadable(self):
        source = (
            "    static let currentSchemaVersion = 66\n"
            "    static let currentSchemaVersion = 67\n"
        )
        self.assertIsNone(preflight.read_head(source))


class ScanTests(unittest.TestCase):
    def setUp(self):
        import tempfile

        self._dir = tempfile.TemporaryDirectory()
        self.root = Path(self._dir.name)

    def tearDown(self):
        self._dir.cleanup()

    def write(self, name, body):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        return path

    def test_a_matching_assertion_is_counted_and_clean(self):
        self.write("A.swift", "#expect(AnalysisStore.currentSchemaVersion == 67)\n")
        violations, total = preflight.scan(self.root, 67)
        self.assertEqual(violations, [])
        self.assertEqual(total, 1)

    def test_a_stale_assertion_is_a_violation_naming_its_line(self):
        self.write(
            "B.swift",
            "// leading\n#expect(AnalysisStore.currentSchemaVersion == 66)\n",
        )
        violations, total = preflight.scan(self.root, 67)
        self.assertEqual(total, 1)
        self.assertEqual(len(violations), 1)
        path, line, claimed = violations[0]
        self.assertEqual((line, claimed), (2, 66))
        self.assertEqual(path.name, "B.swift")

    def test_the_store_call_spelling_is_deliberately_invisible(self):
        # A rung seeds a v38 database and asserts its version BEFORE migrating.
        # That names the store at a point in the test, never the head, and must
        # not move when the head moves.
        self.write("C.swift", "#expect(try await store.schemaVersion() == 38)\n")
        violations, total = preflight.scan(self.root, 67)
        self.assertEqual((violations, total), ([], 0))

    def test_a_not_equal_assertion_is_not_matched(self):
        self.write("D.swift", "#expect(AnalysisStore.currentSchemaVersion != 3)\n")
        _, total = preflight.scan(self.root, 67)
        self.assertEqual(total, 0)

    def test_several_sites_in_one_file_are_all_reported(self):
        self.write(
            "E.swift",
            "#expect(AnalysisStore.currentSchemaVersion == 66)\n"
            "#expect(AnalysisStore.currentSchemaVersion == 67)\n"
            "#expect(AnalysisStore.currentSchemaVersion == 65)\n",
        )
        violations, total = preflight.scan(self.root, 67)
        self.assertEqual(total, 3)
        self.assertEqual([claimed for _, _, claimed in violations], [66, 65])

    def test_whitespace_around_the_operator_does_not_hide_a_site(self):
        self.write("F.swift", "#expect(AnalysisStore.currentSchemaVersion==66)\n")
        violations, _ = preflight.scan(self.root, 67)
        self.assertEqual(len(violations), 1)


class ClosedInBothDirectionsTests(unittest.TestCase):
    """The half that makes a green run mean something."""

    def test_the_real_tree_is_clean_and_its_population_is_not_empty(self):
        head = preflight.read_head(preflight.STORE.read_text(encoding="utf-8"))
        self.assertIsNotNone(head, "the head constant must stay readable")
        violations, total = preflight.scan(preflight.TESTS, head)
        self.assertEqual(violations, [], "the tree must name the current head")
        self.assertGreater(
            total,
            0,
            "zero head assertions means the scan has stopped seeing its "
            "population, which is a broken check reporting success",
        )


if __name__ == "__main__":
    unittest.main()
