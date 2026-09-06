"""playhead-hbejs: tools/9s1z/Deps.swift is re-verified before the harness builds.

`verify-deps.sh` proves the committed copy is a byte-identical extract of the
app target; it was wired to nothing. `build.sh` now refuses to build from a
stale copy. These rails run the verifier on the real tree, make it FIRE by
hand-editing the copy (restored afterwards), and pin that build.sh calls it
before swiftc — a green harness build must not read as "the copy is current".
"""
import os
import subprocess
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TOOLS = os.path.join(ROOT, "tools", "9s1z")
DEPS = os.path.join(TOOLS, "Deps.swift")


def _verify():
    return subprocess.run(["bash", os.path.join(TOOLS, "verify-deps.sh")], cwd=ROOT,
                          capture_output=True, text=True, timeout=120)


class DepsVerifiedTests(unittest.TestCase):
    def test_the_committed_copy_is_a_verbatim_extract_and_the_tree_stays_clean(self):
        proc = _verify()
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        # Scoped to the COPY: the verifier re-extracts in place and must leave it
        # byte-identical. A modified build.sh beside it (an uncommitted edit, or
        # this rail's own battery mutating the call) is not the verifier's doing.
        status = subprocess.run(["git", "status", "--porcelain", "--", "tools/9s1z/Deps.swift"], cwd=ROOT,
                                capture_output=True, text=True).stdout
        self.assertEqual(status.strip(), "", "verify-deps.sh must leave the copy as it found it:\n" + status)

    def test_a_hand_edited_copy_makes_the_verifier_refuse(self):
        with open(DEPS) as f:
            original = f.read()
        try:
            with open(DEPS, "w") as f:
                f.write(original + "\n// hand edit — playhead-hbejs rail\n")
            proc = _verify()
            self.assertNotEqual(proc.returncode, 0, "a hand edit must be refused")
            self.assertIn("DIFFERS", proc.stderr + proc.stdout)
        finally:
            with open(DEPS, "w") as f:
                f.write(original)

    def test_build_sh_verifies_before_it_compiles(self):
        with open(os.path.join(TOOLS, "build.sh")) as f:
            src = f.read()
        verify_at = src.find("verify-deps.sh")
        swiftc_at = src.find("swiftc ")
        self.assertGreater(verify_at, 0, "build.sh no longer calls verify-deps.sh")
        self.assertGreater(swiftc_at, verify_at, "verify-deps.sh must run BEFORE swiftc")
        self.assertIn("exit 1", src[verify_at:swiftc_at], "a failed verification must stop the build")


if __name__ == "__main__":
    unittest.main()
