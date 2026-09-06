"""playhead-awhs7: the inventory resolves every expectation against the tree, and says which do not."""
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import mutation_battery_inventory as inv  # noqa: E402

BATTERY = r'''
ORCH="Playhead/A.swift"
T_ONE="a display name"
T_TWO="a name with a \`backtick\` and a \"quote\""
T_X="SomeSuiteTests/testSomething"
T_Y="-[OtherTests testOther]"
T_GONE="a test nobody wrote"
MUTATIONS=(
  "A01|1|ORCH|$T_ONE;$T_TWO"
  "A02|2|ORCH|$T_X;$T_Y"
  "A03|3|ORCH|$T_GONE"
  "A04|4|ORCH|$T_UNDEFINED"
)
apply_mutation() {
  local name="$1" file="$2" OLD NEW
  case "$name" in
  A01)
    snippet OLD <<'EOF'
let x = 1
EOF
    snippet NEW <<'EOF'
let x = 2
EOF
    patch "$file" "$OLD" "$NEW" ;;
  A02)
    snippet OLD <<'EOF'
let y = 1
EOF
    snippet NEW <<'EOF'
let y = 2
EOF
    patch "$file" "$OLD" "$NEW" ;;
  esac
}
'''

SUITE = '''
@Test("a display name") func a() {}
@Test("a name with a `backtick` and a \\"quote\\"") func b() {}
@Test("""
    a triple-quoted
    name
    """) func c() {}
final class SomeSuiteTests: XCTestCase { func testSomething() {} }
final class OtherTests: XCTestCase { func testOther() {} }
'''


class InventoryTests(unittest.TestCase):
    def _fixture(self):
        tmp = tempfile.mkdtemp()
        os.makedirs(os.path.join(tmp, "PlayheadTests"))
        os.makedirs(os.path.join(tmp, "Playhead"))
        with open(os.path.join(tmp, "Playhead", "A.swift"), "w") as f:
            f.write("let x = 1\nlet y = 1\nlet y = 1\n")
        with open(os.path.join(tmp, "PlayheadTests", "Suite.swift"), "w") as f:
            f.write(SUITE)
        bat = os.path.join(tmp, "battery.sh")
        with open(bat, "w") as f:
            f.write(BATTERY)
        return tmp, bat

    def test_display_names_xctest_spellings_and_shell_escapes_resolve(self):
        tmp, bat = self._fixture()
        consts, filevars, records = inv.parse_battery(open(bat).read())
        display, funcs = inv.test_names(os.path.join(tmp, "PlayheadTests"))
        bad = inv.unresolved_expectations(consts, records, display, funcs)
        names = sorted(set(r for r, _, _ in bad))
        self.assertEqual(names, ["A03", "A04"], bad)
        self.assertIn("a triple-quoted name", display)

    def test_the_gate_exits_2_on_an_unresolved_expectation_and_names_it(self):
        tmp, bat = self._fixture()
        proc = subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "mutation_battery_inventory.py"),
                               "--battery", bat, "--tests-root", os.path.join(tmp, "PlayheadTests"), "--root", tmp, "--check"],
                              capture_output=True, text=True)
        self.assertEqual(proc.returncode, 2, proc.stdout)
        self.assertIn("A03", proc.stdout)
        self.assertIn("a test nobody wrote", proc.stdout)
        self.assertIn("A04", proc.stdout)
        self.assertIn("unknown constant", proc.stdout)

    def test_anchors_report_zero_and_multiple_matches(self):
        tmp, bat = self._fixture()
        text = open(bat).read()
        consts, filevars, records = inv.parse_battery(text)
        drift = inv.anchors(text, filevars, records, tmp)
        self.assertEqual(sorted((n, c) for n, _, c in drift), [("A02", 2)], drift)

    def test_the_real_battery_resolves_every_expectation(self):
        proc = subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "mutation_battery_inventory.py"), "--check"],
                              capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(proc.returncode, 0, proc.stdout[-1500:])


if __name__ == "__main__":
    unittest.main()
