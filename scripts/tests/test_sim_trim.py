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


FAST_GATE = ROOT / "scripts" / "fast-gate.sh"


class TheTrimMustNeverBeSilentlySkipped(unittest.TestCase):
    """playhead-81ig: the trim shipped INERT and no log said so.

    `fast-gate.sh` resolved the simulator UDID only from a `id=` destination, but
    the default destination is `platform=iOS Simulator,name=iPhone 17` — a NAME.
    So `SIM_ID` was empty on the default invocation, the `[ -n "$SIM_ID" ]` guard
    took its false branch in SILENCE, and a trimmed run and an untrimmed run
    produced byte-identical logs. Two full plans were accepted as evidence for a
    trim that was applied to neither.

    The house defect class, one layer up from where sim-trim.sh already fights it:
    a guard that names an ABSENCE, whose false branch makes no claim.
    """

    def test_a_name_destination_is_resolved_too(self):
        self.assertIn("*name=*)", FAST_GATE.read_text())

    def test_every_skip_path_prints_something(self):
        # The `if` that runs the trim must be followed by elif/else arms that SAY
        # the run is untrimmed. A bare `fi` is the bug.
        text = FAST_GATE.read_text()
        start = text.index('if [ "${PLAYHEAD_SIM_TRIM:-1}" != "0" ]')
        block = text[start:start + 3000]
        self.assertIn("RUNNING UNTRIMMED", block)
        for arm in ("elif [ \"${PLAYHEAD_SIM_TRIM:-1}\" = \"0\" ]",
                    "elif [ -z \"$SIM_ID\" ]",
                    "\nelse\n"):
            self.assertIn(arm, block, f"missing arm: {arm!r}")

    def test_a_trimmed_run_is_identifiable_from_its_log_alone(self):
        # The one line a reader can grep for. Without it, "was this trimmed?" is
        # unanswerable after the fact — which is how the inert version survived.
        self.assertIn("simulator processes after trim:", FAST_GATE.read_text())


class NameResolutionDoesNotMatchAPrefix(unittest.TestCase):
    """`iPhone 17` must not resolve to `iPhone 17 Pro`.

    Only one iPhone 17 exists on this box, so the confusable case cannot be
    observed live — it is pinned against a fixture instead, because the failure
    would be silent and wrong rather than loud.
    """

    LISTING = (
        "== Devices ==\n"
        "-- iOS 27.0 --\n"
        "    iPhone 17 Pro (AAAAAAAA-1111-2222-3333-444444444444) (Shutdown) \n"
        "    iPhone 17 (BBBBBBBB-1111-2222-3333-444444444444) (Booted) \n"
    )

    def _resolve(self, name: str, listing: str) -> str:
        import subprocess
        booted = subprocess.run(
            ["sed", "-n", rf"s/^ *{name} (\([0-9A-Fa-f-]\{{36\}}\)) (Booted).*/\1/p"],
            input=listing, capture_output=True, text=True, check=False).stdout.splitlines()
        if booted:
            return booted[0]
        any_state = subprocess.run(
            ["sed", "-n", rf"s/^ *{name} (\([0-9A-Fa-f-]\{{36\}}\)) (.*/\1/p"],
            input=listing, capture_output=True, text=True, check=False).stdout.splitlines()
        return any_state[0] if any_state else ""

    def test_exact_name_wins_over_a_longer_one(self):
        self.assertEqual(self._resolve("iPhone 17", self.LISTING),
                         "BBBBBBBB-1111-2222-3333-444444444444")

    def test_the_longer_name_still_resolves_to_itself(self):
        self.assertEqual(self._resolve("iPhone 17 Pro", self.LISTING),
                         "AAAAAAAA-1111-2222-3333-444444444444")

    def test_an_unknown_name_resolves_to_nothing_rather_than_to_anything(self):
        self.assertEqual(self._resolve("iPad Air", self.LISTING), "")


if __name__ == "__main__":
    unittest.main()
