"""Buildless rails for the simulator trim (playhead-blsh).

`scripts/sim-trim.sh` verifies itself AT RUNTIME by re-reading the device — that
is the important check and it cannot be replaced here. What these tests pin is
the part that has no runtime signal on the run that breaks it: **the disables
outlive `simctl erase`**, so a bad entry does not spoil the run that adds it, it
spoils the NEXT one, on somebody else's branch.

`python3 -m unittest scripts.tests.test_sim_trim` — under a second, no build.
"""

from __future__ import annotations

import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
TIER_A = ROOT / "scripts" / "sim-trim-jobs.txt"
TIER_B = ROOT / "scripts" / "sim-trim-jobs-tier-b.txt"
SCRIPT = ROOT / "scripts" / "sim-trim.sh"

LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def labels(path: pathlib.Path) -> list[str]:
    out = []
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            out.append(line)
    return out


def keep_list() -> list[str]:
    """The KEEP heredoc-ish block in sim-trim.sh, read the way the script reads it."""
    text = SCRIPT.read_text()
    m = re.search(r"^KEEP='\n(.*?)^'", text, re.S | re.M)
    assert m, "KEEP block not found in sim-trim.sh"
    return [ln.strip() for ln in m.group(1).splitlines() if ln.strip()]


class SimTrimJobFiles(unittest.TestCase):

    def test_every_entry_is_a_plausible_launchd_label(self):
        for path in (TIER_A, TIER_B):
            for label in labels(path):
                self.assertRegex(label, LABEL, f"{path.name}: {label!r}")
                self.assertTrue(
                    label.startswith("com.apple."),
                    f"{path.name}: {label!r} is not an Apple launchd label",
                )

    def test_no_duplicates_within_a_file(self):
        for path in (TIER_A, TIER_B):
            got = labels(path)
            self.assertEqual(len(got), len(set(got)), f"{path.name} repeats a label")

    def test_the_two_tiers_are_disjoint(self):
        # Tier B is opt-in and is DAN'S COVERAGE DECISION. A label in both files
        # would be applied by default and the opt-in would be a lie.
        self.assertEqual(set(labels(TIER_A)) & set(labels(TIER_B)), set())

    def test_no_job_is_also_on_the_keep_list(self):
        # sim-trim.sh refuses at runtime if this is violated, which fails the gate
        # LOUDLY. This test fails it in a second instead of after a boot.
        keep = set(keep_list())
        self.assertEqual(set(labels(TIER_A)) & keep, set())
        self.assertEqual(set(labels(TIER_B)) & keep, set())


class NanoRegistryDIsNotNegotiable(unittest.TestCase):
    """The one that cost eight boot probes to find.

    `com.apple.nanoregistryd` is the paired-device (Watch) registry. It backs no
    framework this app imports and passes every admission rule in
    sim-trim-jobs.txt, so the obvious edit is to put it back. Disabling it ALONE
    makes the NEXT BOOT UNUSABLE: `simctl bootstatus` prints nothing at all and
    hangs, and `simctl launch com.apple.Preferences` never returns — so xcodebuild
    could not install or start a test host either.

    There is no runtime signal on the run that adds it. The trim lands after boot,
    so that run is fine; the damage lands on the next one, which will be somebody
    else's branch and will look like their diff.
    """

    LABEL = "com.apple.nanoregistryd"

    def test_it_is_on_the_keep_list(self):
        self.assertIn(self.LABEL, keep_list())

    def test_it_is_in_neither_job_file(self):
        self.assertNotIn(self.LABEL, labels(TIER_A))
        self.assertNotIn(self.LABEL, labels(TIER_B))

    def test_the_reason_is_written_down_where_the_edit_happens(self):
        # A bare absence is indistinguishable from an oversight, and an oversight
        # invites the re-add. Both files must carry the reason in prose.
        self.assertIn(self.LABEL, TIER_A.read_text())
        self.assertIn("bisection", SCRIPT.read_text().lower())

    def test_a_close_relative_is_still_trimmed(self):
        # nanoregistrylaunchd is a DIFFERENT job and was never the problem. If a
        # future edit blanket-keeps everything matching "nanoregistry", this
        # notices — the amnesty must stay the width it was measured at.
        self.assertNotIn("com.apple.nanoregistrylaunchd", keep_list())


class KeepListCoversTheImportedFrameworks(unittest.TestCase):
    """The job files' safety argument is 'this framework is imported nowhere'.

    That half is checked by grep at authoring time. THIS half — that the daemons
    behind the frameworks which ARE imported are protected — is checkable here,
    and it is the half a future widening would get wrong.
    """

    REQUIRED = [
        "com.apple.SpringBoard",       # launching the test host; scene phase
        "com.apple.backboardd",
        "com.apple.runningboardd",
        "com.apple.dasd",              # BGTaskScheduler
        "com.apple.nsurlsessiond",     # background URLSession
        "com.apple.mediaremoted",      # MediaPlayer
        "com.apple.usernotificationsd",
        "com.apple.storekitd",
        "com.apple.mobileassetd",      # Speech / FoundationModels assets
        "com.apple.mobile.installd",
        "com.apple.tccd",
    ]

    def test_each_is_on_the_keep_list(self):
        keep = set(keep_list())
        for label in self.REQUIRED:
            self.assertIn(label, keep)


if __name__ == "__main__":
    unittest.main()
