"""Tests for playhead-pu7e's mutation-battery run lock (scripts/mutation-battery-lock.sh).

The unit under test answers three questions the battery got wrong:

1.  May this run mutate this worktree? Two batteries in one worktree revert each
    other's mutants and NOTHING detects it — each run's own restore succeeds, so
    each run's own hash check passes. The clean-tree guard cannot close this: the
    tree is genuinely clean in the window between run A's restore and its next
    apply, which is precisely when run B looks.
2.  When a run left a mutant behind, whose is it and may it be discarded? A lock
    that records what it broke before breaking it can answer; `require_clean_tree`
    cannot tell a mutant from your work.
3.  When the suites ran no tests, WHY? `rc=65` was printed as "the baseline did
    not run tests", which is a claim about the tree. Three real causes produce it
    and only one of them implicates the tree at all.

Everything here is hermetic except the rails at the bottom, which drive the real
`scripts/mutation-battery.sh`. Those are still BUILDLESS: a refused run stops at
the lock, before `require_clean_tree` and before any mutation is applied, and the
two that do run go through `--dry-run`. Two of them dirty a real mutable file to
prove which guard fires; both restore the original BYTES in a `finally`, never
via git.

`SAMPLE_MUTATION` and MUTABLE_FILES[0] are resolved out of the battery itself and
RAISE if they cannot be found, rather than skipping. A rail that quietly stops
testing is the failure mode this whole bead is about.

Conventions follow test_disk_preflight.py: stdlib unittest, fixtures built in
code, no network and no xcodebuild.
"""

import os
import pathlib
import re
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
LOCK_SH = ROOT / "scripts" / "mutation-battery-lock.sh"
BATTERY = ROOT / "scripts" / "mutation-battery.sh"

# A mutation that exists in the battery's table. Only used to give `--only` a
# selection; no mutation is ever applied by these tests except in the one
# explicitly-named control.
SAMPLE_MUTATION = "M05"


def bash(body, cwd, check=False, timeout=60):
    """Run `body` with the lock module sourced. Returns CompletedProcess."""
    script = 'set -uo pipefail\n. "%s" || exit 99\n%s\n' % (LOCK_SH, body)
    proc = subprocess.run(
        ["bash", "-c", script],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if check and proc.returncode != 0:
        raise AssertionError(
            "bash failed rc=%d\nstdout:\n%s\nstderr:\n%s"
            % (proc.returncode, proc.stdout, proc.stderr)
        )
    return proc


class TempRepo:
    """A throwaway git repo with one tracked file, so `git checkout --` is real."""

    def __init__(self):
        # Resolved: on macOS `mkdtemp` hands back `/var/folders/...` while bash's
        # `pwd` reports `/private/var/folders/...`, and the lock records the
        # latter. Comparing the two spellings would make the worktree-identity
        # check fire on every test rather than on a real mismatch.
        self.dir = pathlib.Path(tempfile.mkdtemp(prefix="playhead-mb-lock-test.")).resolve()
        self.file = "src/thing.swift"
        (self.dir / "src").mkdir(parents=True)
        (self.dir / self.file).write_text("let pristine = 1\n")
        env = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_SYSTEM="/dev/null")
        for cmd in (
            ["git", "init", "-q", "-b", "main"],
            ["git", "config", "user.email", "t@example.com"],
            ["git", "config", "user.name", "t"],
            ["git", "add", "-A"],
            ["git", "commit", "-qm", "base"],
        ):
            subprocess.run(cmd, cwd=str(self.dir), check=True, env=env,
                           capture_output=True, text=True)

    @property
    def lockdir(self):
        return self.dir / ".git" / "mutation-battery.lock"

    def write(self, text):
        (self.dir / self.file).write_text(text)

    def read(self):
        return (self.dir / self.file).read_text()

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class LockTestCase(unittest.TestCase):
    def setUp(self):
        self.repo = TempRepo()
        self.addCleanup(self.repo.cleanup)
        self.holders = []
        self.addCleanup(self._reap_holders)

    def _reap_holders(self):
        for proc in self.holders:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)

    def hold(self, argv="--only RT14", ready_timeout=15):
        return self.hold_files(self.repo.file, argv=argv, ready_timeout=ready_timeout)

    def hold_files(self, *files, argv="--only RT14", ready_timeout=15):
        """Start a process that ACQUIRES the lock and then sits on it.

        Returns the holder's pid. The holder writes a sentinel once it owns the
        lock, so nothing here races the mkdir.
        """
        sentinel = self.repo.dir / "held"
        body = (
            'mb_lock_acquire "%s" || exit $?\n'
            'mb_lock_record_pre %s\n'
            'echo ok > "%s"\n'
            "sleep 300\n" % (argv, " ".join('"%s"' % f for f in files), sentinel)
        )
        proc = subprocess.Popen(
            ["bash", "-c", 'set -uo pipefail\n. "%s" || exit 99\n%s' % (LOCK_SH, body)],
            cwd=str(self.repo.dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.holders.append(proc)
        deadline = time.time() + ready_timeout
        while time.time() < deadline:
            if sentinel.exists():
                return proc.pid
            if proc.poll() is not None:
                self.fail("holder exited early rc=%s: %s" % (proc.returncode, proc.stderr.read()))
            time.sleep(0.02)
        self.fail("holder never acquired the lock")


# ---------------------------------------------------------------------------
# 1. Mutual exclusion
# ---------------------------------------------------------------------------
class TestAcquireAndRelease(LockTestCase):
    def test_acquire_creates_the_lock_and_release_removes_it(self):
        out = bash('mb_lock_acquire "--batch 7" && echo OWNED=$MB_LOCK_OWNED && '
                   '[ -d "$MB_LOCK_DIR" ] && echo DIR_EXISTS && mb_lock_release && '
                   '[ -d "$MB_LOCK_DIR" ] || echo DIR_GONE', self.repo.dir)
        self.assertIn("OWNED=1", out.stdout)
        self.assertIn("DIR_EXISTS", out.stdout)
        self.assertIn("DIR_GONE", out.stdout)
        self.assertFalse(self.repo.lockdir.exists())

    def test_the_lock_lives_outside_the_working_tree(self):
        bash('mb_lock_acquire "" && echo "$MB_LOCK_DIR" > path.txt', self.repo.dir, check=True)
        # A lock inside the working tree would dirty `git status` and so would be
        # reported BY THE CLEAN-TREE GUARD as the operator's uncommitted work.
        status = subprocess.run(["git", "status", "--porcelain"], cwd=str(self.repo.dir),
                                capture_output=True, text=True).stdout
        self.assertNotIn("mutation-battery.lock", status)
        # Positive control on that observer: `git status` in this repo DOES
        # report a new file, so the absence above is an absence and not a query
        # that could never have seen anything.
        self.assertIn("path.txt", status)

    def test_lock_path_is_scoped_to_the_git_directory(self):
        out = bash("mb_lock_path", self.repo.dir, check=True)
        gitdir = subprocess.run(["git", "rev-parse", "--absolute-git-dir"],
                                cwd=str(self.repo.dir), capture_output=True,
                                text=True, check=True).stdout.strip()
        # Per WORKTREE, not per repo: `.git/worktrees/<slug>` for a linked one.
        # That is the scope of the corruption — each worktree has its own tree
        # and its own .derivedData.
        self.assertEqual(out.stdout.strip(), gitdir + "/mutation-battery.lock")


class TestContention(LockTestCase):
    def test_a_second_run_is_refused_and_the_holder_is_named(self):
        pid = self.hold(argv="--only RT14")
        out = bash('mb_lock_acquire "--batch 420"; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=75", out.stdout)
        self.assertIn("another mutation battery is already running", out.stderr.lower())
        self.assertIn(str(pid), out.stderr)
        self.assertIn("--only RT14", out.stderr)

    def test_the_refusal_does_not_blame_the_baseline_or_the_tree(self):
        self.hold()
        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        lowered = out.stderr.lower()
        self.assertNotIn("did not run tests", lowered)
        self.assertNotIn("commit your work first", lowered)
        # Positive control on this observer: the same stderr DOES carry the
        # message we do expect, so "absent" above is a real absence and not a
        # stream that was never read.
        self.assertIn("refusing", lowered)

    def test_a_refused_run_does_not_release_the_holders_lock(self):
        self.hold()
        before = (self.repo.lockdir / "info").read_bytes()
        bash('mb_lock_acquire ""; mb_lock_release', self.repo.dir)
        self.assertTrue(self.repo.lockdir.exists(), "the holder's lock was deleted by a refused run")
        self.assertEqual(before, (self.repo.lockdir / "info").read_bytes())

    def test_release_refuses_when_the_lock_is_now_held_by_a_different_pid(self):
        # The last line of defence if a reclaim ever misjudges: a process that
        # believes it owns the lock must still not delete one somebody else's
        # pid is written into.
        self.repo.lockdir.mkdir(parents=True)
        (self.repo.lockdir / "info").write_text("pid=999999\nargv=--only ZZ99\n")
        out = bash('MB_LOCK_DIR="%s"; MB_LOCK_OWNED=1; mb_lock_release; echo "RC=$?"'
                   % self.repo.lockdir, self.repo.dir)
        self.assertIn("RC=0", out.stdout)
        self.assertTrue(self.repo.lockdir.exists())
        self.assertIn("not releasing", out.stderr)
        # Positive control: with OUR pid in the file it IS released, so the
        # refusal above is the pid check and not a release that never works.
        (self.repo.lockdir / "info").write_text("pid=PIDPLACEHOLDER\n")
        out = bash('sed -i "" "s/PIDPLACEHOLDER/$$/" "%s/info"\n'
                   'MB_LOCK_DIR="%s"; MB_LOCK_OWNED=1; mb_lock_release; echo "RC=$?"'
                   % (self.repo.lockdir, self.repo.lockdir), self.repo.dir)
        self.assertIn("RC=0", out.stdout)
        self.assertFalse(self.repo.lockdir.exists(), out.stderr)

    def test_a_lock_directory_with_no_info_yet_is_not_stolen(self):
        # The holder wins `mkdir` and is descheduled before it writes `info`.
        # Reading an absent pid as "gone" would hand the worktree to a second
        # battery at the exact moment the first is starting to mutate it.
        self.repo.lockdir.mkdir(parents=True)
        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir, timeout=90)
        self.assertNotIn("RC=0", out.stdout)
        self.assertIn("RC=75", out.stdout)

    def test_a_lock_directory_abandoned_before_it_recorded_anything_is_reclaimed(self):
        # The complement of the test above, so "never stolen" does not become
        # "wedged forever". A dir with no `info` was abandoned between `mkdir`
        # and the very next line, which is long before any mutation is applied —
        # so there is provably nothing of its to restore.
        self.repo.lockdir.mkdir(parents=True)
        os.utime(self.repo.lockdir, (time.time() - 3600, time.time() - 3600))
        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir, timeout=90)
        self.assertIn("RC=0", out.stdout)
        self.assertIn("ABANDONED", out.stderr)


# ---------------------------------------------------------------------------
# 2. Staleness and recovery
# ---------------------------------------------------------------------------
class TestStaleLock(LockTestCase):
    def _kill9(self, pid):
        for proc in self.holders:
            if proc.pid == pid:
                proc.send_signal(signal.SIGKILL)
                # Reap it. A zombie still answers `kill -0` and `ps -o lstart=`,
                # so an unreaped holder would read as ALIVE and the lock would
                # never be seen as stale.
                proc.wait(timeout=10)
                return
        self.fail("no holder with pid %d" % pid)

    def test_a_lock_whose_holder_was_SIGKILLed_is_reclaimed(self):
        pid = self.hold()
        for proc in self.holders:
            proc.kill()
            proc.wait(timeout=10)
        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=0", out.stdout)
        self.assertIn("stale", out.stderr.lower())
        self.assertIn(str(pid), out.stderr)

    def test_a_recycled_pid_does_not_wedge_the_worktree_forever(self):
        # pid is alive (it is this test's own shell) but its start time is not
        # the recorded one, so the lock is stale. Without the start-time check a
        # recycled pid holds a worktree hostage until someone deletes the lock
        # by hand.
        self.repo.lockdir.mkdir(parents=True)
        (self.repo.lockdir / "info").write_text(
            "pid=%d\npid_start=Thu Jan  1 00:00:00 1970\nargv=--only ZZ99\n"
            "worktree=%s\n" % (os.getpid(), self.repo.dir)
        )
        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=0", out.stdout)

        # Positive control on the same observer: with the REAL start time the
        # very same pid reads as live, so the reclaim above is the start-time
        # check firing and not a lock that is always reclaimable.
        real_start = subprocess.run(["ps", "-o", "lstart=", "-p", str(os.getpid())],
                                    capture_output=True, text=True).stdout.strip()
        shutil.rmtree(self.repo.lockdir)
        self.repo.lockdir.mkdir(parents=True)
        (self.repo.lockdir / "info").write_text(
            "pid=%d\npid_start=%s\nargv=--only ZZ99\nworktree=%s\n"
            % (os.getpid(), real_start, self.repo.dir)
        )
        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=75", out.stdout)

    def test_the_dead_runs_mutant_is_restored_and_a_rescue_copy_is_kept(self):
        pid = self.hold()
        # The holder recorded the pristine hash. Now inject a mutant on its
        # behalf and record it, exactly as an applied batch would.
        self.repo.write("let pristine = 2  // MUTANT\n")
        bash('MB_LOCK_DIR="%s"; MB_LOCK_OWNED=1\n'
             'MB_LOCK_FILES_PRE="$(sed -n "/^file\t/p" "$MB_LOCK_DIR/state")\n"\n'
             'mb_lock_note_applied' % self.repo.lockdir, self.repo.dir, check=True)
        self._kill9(pid)

        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=0", out.stdout)
        self.assertEqual("let pristine = 1\n", self.repo.read())
        rescue = re.search(r"rescue copy: (\S+)", out.stderr)
        self.assertIsNotNone(rescue, "no rescue copy was named:\n" + out.stderr)
        saved = pathlib.Path(rescue.group(1))
        # `src/thing.swift` is two components, so parents[1] is the rescue root.
        self.addCleanup(shutil.rmtree, str(saved.parents[1]), True)
        self.assertEqual("let pristine = 2  // MUTANT\n", saved.read_text())

    def test_recovery_refuses_when_the_file_is_neither_pristine_nor_the_mutant(self):
        pid = self.hold()
        self.repo.write("let pristine = 2  // MUTANT\n")
        bash('MB_LOCK_DIR="%s"; MB_LOCK_OWNED=1\n'
             'MB_LOCK_FILES_PRE="$(sed -n "/^file\t/p" "$MB_LOCK_DIR/state")\n"\n'
             'mb_lock_note_applied' % self.repo.lockdir, self.repo.dir, check=True)
        self._kill9(pid)
        # Somebody edited it AFTER the crash. Discarding that is destroying work.
        self.repo.write("let pristine = 3  // a human's edit\n")

        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=2", out.stdout)
        self.assertIn("neither", out.stderr.lower())
        self.assertEqual("let pristine = 3  // a human's edit\n", self.repo.read())

    def test_an_edit_made_after_a_crash_that_never_mutated_is_NOT_discarded(self):
        # The dead run recorded pristine hashes and died before applying
        # anything, so its state is `clean` and every `post` is `?`. A later
        # difference is therefore SOMEBODY'S EDIT, not a half-applied mutation.
        # Reading `post=?` as "mid-apply" regardless of state would discard it.
        pid = self.hold()
        self._kill9(pid)
        self.assertIn("state=clean", (self.repo.lockdir / "state").read_text())
        self.repo.write("let pristine = 9  // a human's edit\n")

        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=2", out.stdout)
        self.assertEqual("let pristine = 9  // a human's edit\n", self.repo.read())
        self.assertIn("DID NOT WRITE this file", out.stderr)
        self.assertIn("state\n  'clean'", out.stderr.replace("mutation-battery: ", ""))

    def test_a_crash_MID_APPLY_is_still_restored(self):
        # Positive control for the test above: the very same `post=?` DOES mean
        # mid-apply once the state says `mutated`, and must be restored — a
        # half-applied batch is the one shape no hash can describe.
        pid = self.hold()
        bash('MB_LOCK_DIR="%s"; MB_LOCK_OWNED=1\n'
             'MB_LOCK_FILES_PRE="$(sed -n "/^file\t/p" "$MB_LOCK_DIR/state")\n"\n'
             'mb_lock_note_mutating 7 "AA01 AA02"' % self.repo.lockdir,
             self.repo.dir, check=True)
        self.repo.write("let pristine = 2  // half a batch\n")
        self._kill9(pid)

        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=0", out.stdout)
        self.assertEqual("let pristine = 1\n", self.repo.read())
        self.assertIn("MID-APPLY", out.stderr)

    def test_an_edit_to_a_file_the_batch_never_touched_is_not_called_a_mutant(self):
        # A batch mutates one or two files; the lock records `post` for ALL of
        # them, so an untouched file has post == pre. A later difference there
        # is unambiguously somebody's edit — the dead run provably did not write
        # that file — and saying "matches neither the pristine bytes nor the
        # mutant it injected" names a mutant that was never there.
        repo = self.repo
        second = "src/other.swift"
        (repo.dir / second).write_text("let other = 1\n")
        subprocess.run(["git", "add", "-A"], cwd=str(repo.dir), check=True, capture_output=True)
        subprocess.run(["git", "commit", "-qm", "two"], cwd=str(repo.dir), check=True,
                       capture_output=True)
        pid = self.hold_files(repo.file, second)
        repo.write("let pristine = 2  // MUTANT\n")   # only the FIRST file
        bash('MB_LOCK_DIR="%s"; MB_LOCK_OWNED=1\n'
             'MB_LOCK_FILES_PRE="$(sed -n "/^file\t/p" "$MB_LOCK_DIR/state")\n"\n'
             'mb_lock_note_mutating 3 "XX01"\nmb_lock_note_applied' % repo.lockdir,
             repo.dir, check=True)
        self._kill9(pid)
        (repo.dir / second).write_text("let other = 99  // a human's edit\n")

        out = bash('mb_lock_acquire ""; echo "RC=$?"', repo.dir)
        self.assertIn("RC=2", out.stdout)
        self.assertIn("DID NOT WRITE this file", out.stderr)
        self.assertIn("src/other.swift", out.stderr)
        # And it must NOT print a "mutant" hash for a file that carries none.
        self.assertNotIn("mutant   :", out.stderr)
        self.assertEqual("let other = 99  // a human's edit\n",
                         (repo.dir / second).read_text())
        # And nothing else was touched either: a refusal must not half-restore.
        self.assertEqual("let pristine = 2  // MUTANT\n", repo.read())
        self.assertIn("nothing was restored", out.stderr.lower())

    def test_a_stale_lock_from_another_worktree_is_never_acted_on(self):
        self.repo.lockdir.mkdir(parents=True)
        (self.repo.lockdir / "info").write_text(
            "pid=999999\npid_start=Thu Jan  1 00:00:00 1970\nworktree=/somewhere/else\n"
        )
        (self.repo.lockdir / "state").write_text("state=mutated\n")
        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=2", out.stdout)
        self.assertIn("/somewhere/else", out.stderr)


# ---------------------------------------------------------------------------
# 3. The rc=65 diagnosis
# ---------------------------------------------------------------------------
class TestDiagnosis(LockTestCase):
    def diagnose(self, log_text, rc=65, what="the baseline"):
        log = self.repo.dir / "run.log"
        log.write_text(log_text)
        out = bash('mb_diagnose_no_tests "%s" %d "%s"' % (log, rc, what), self.repo.dir)
        return out.stderr

    def test_contention_is_named_as_contention(self):
        err = self.diagnose(
            "Testing started\n"
            "error: unable to attach DB: error: accessing build database "
            "\"/x/.derivedData/XCBuildData/build.db\": database is locked\n"
            "** TEST FAILED **\n"
        )
        self.assertIn("CONTENTION", err)
        self.assertIn("database is locked", err)
        # The whole point: it must not send the operator to the tree.
        self.assertNotIn("does not compile", err)

    def test_a_wedged_simulator_is_named_and_the_recovery_is_given(self):
        for signature in (
            "blessSimulatorHub failed. Simulator service hub IS NOT still alive",
            "Test crashed with signal kill before establishing connection",
            "Failed to install or launch the test runner",
            "Mach error -308",
        ):
            with self.subTest(signature=signature):
                err = self.diagnose("Testing started\n%s\n** TEST FAILED **\n" % signature)
                self.assertIn("RUNNER NEVER LAUNCHED", err)
                self.assertIn(signature.split(".")[0][:24], err)
                self.assertIn("simctl", err)

    def test_the_simctl_utility_error_is_labelled_a_SECONDARY_symptom(self):
        # This line is xcodebuild's diagnostic collection shelling out through
        # the GLOBAL xcode-select. On its own it sends the reader to the
        # 2026-07-16 clone-parallelism gotcha, which is not what happened.
        err = self.diagnose(
            "Testing started\n"
            "blessSimulatorHub failed. Simulator service hub IS NOT still alive\n"
            'xcrun: error: unable to find utility "simctl", not a developer tool or in PATH\n'
        )
        self.assertIn("RUNNER NEVER LAUNCHED", err)
        self.assertIn("secondary", err.lower())

    def test_contention_wins_over_a_bare_error_line(self):
        # Contention sprays `error:` too. Classifying on `error:` first is how
        # four verdicts got told the tree was broken.
        err = self.diagnose(
            "error: something\n"
            "error: unable to attach DB: database is locked\n"
            "error: another thing\n"
        )
        self.assertIn("CONTENTION", err)
        self.assertNotIn("THE BUILD FAILED", err)

    def test_a_real_build_failure_is_still_called_a_build_failure(self):
        err = self.diagnose(
            "CompileSwift normal arm64 /x/Foo.swift\n"
            "/x/Foo.swift:12:5: error: cannot find 'nope' in scope\n"
            "** BUILD FAILED **\n"
        )
        self.assertIn("THE BUILD FAILED", err)
        self.assertIn("cannot find 'nope' in scope", err)

    def test_oom_and_disk_are_distinguished_from_a_build_failure(self):
        self.assertIn("OOM", self.diagnose("Killed: 9\n"))
        self.assertIn("DISK", self.diagnose("", rc=28))
        self.assertIn("DISK", self.diagnose("No space left on device\n"))

    def test_an_unrecognised_log_is_called_UNDIAGNOSED_not_a_broken_baseline(self):
        err = self.diagnose("Testing started\nnothing recognisable at all\n" * 3)
        self.assertIn("UNDIAGNOSED", err)
        self.assertIn("nothing recognisable at all", err)  # the tail is printed
        for claim in ("the baseline did not run tests", "does not compile",
                      "CONTENTION", "RUNNER NEVER LAUNCHED"):
            self.assertNotIn(claim, err)

    def test_the_undiagnosed_verdict_is_not_vacuous(self):
        # Positive control on the classifier used by the test above: it CAN
        # reach a non-UNDIAGNOSED verdict from the same call site.
        err = self.diagnose("Testing started\ndatabase is locked\n")
        self.assertNotIn("UNDIAGNOSED", err)

    def test_rc_is_reported_as_not_being_a_verdict(self):
        err = self.diagnose("nothing\n", rc=65, what="batch 420")
        self.assertIn("batch 420", err)
        self.assertIn("NO TESTS", err)


# ---------------------------------------------------------------------------
# 4. The battery itself. Buildless: a refused run stops at the lock.
# ---------------------------------------------------------------------------
def _first_mutable_file():
    """Resolve MUTABLE_FILES[0] out of the battery. Raises rather than skips."""
    text = BATTERY.read_text()
    array = re.search(r"^MUTABLE_FILES=\(\n(.*?)^\)", text, re.S | re.M)
    if not array:
        raise AssertionError("MUTABLE_FILES array not found in " + str(BATTERY))
    var = re.search(r'"\$(\w+)"', array.group(1)).group(1)
    decl = re.search(r'^%s="([^"]+)"' % var, text, re.M)
    if not decl:
        raise AssertionError("no declaration for $%s" % var)
    path = ROOT / decl.group(1)
    if not path.is_file():
        raise AssertionError("$%s -> %s does not exist" % (var, decl.group(1)))
    return decl.group(1), path


class TestBatteryRefusesAConcurrentRun(unittest.TestCase):
    """The rail the bead asks for: start one, start a second, the second must
    exit non-zero NAMING the first, and the first must be unaffected."""

    def setUp(self):
        lockdir = pathlib.Path(
            subprocess.run(["git", "rev-parse", "--absolute-git-dir"], cwd=str(ROOT),
                           capture_output=True, text=True, check=True).stdout.strip()
        ) / "mutation-battery.lock"
        if lockdir.exists():
            self.fail("a mutation battery is already running in %s — rerun when it is done" % ROOT)
        self.lockdir = lockdir
        self.holder = None
        self.addCleanup(self._reap)

    def _reap(self):
        if self.holder and self.holder.poll() is None:
            self.holder.kill()
            self.holder.wait(timeout=10)
        shutil.rmtree(self.lockdir, ignore_errors=True)

    def _hold(self, argv="--only RT14"):
        ready = pathlib.Path(tempfile.mkdtemp(prefix="playhead-mb-e2e.")) / "held"
        self.addCleanup(shutil.rmtree, str(ready.parent), True)
        body = ('. "%s" || exit 99\nmb_lock_acquire "%s" || exit $?\ntouch "%s"\nsleep 300\n'
                % (LOCK_SH, argv, ready))
        self.holder = subprocess.Popen(["bash", "-c", body], cwd=str(ROOT),
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + 20
        while time.time() < deadline:
            if ready.exists():
                return self.holder.pid
            if self.holder.poll() is not None:
                self.fail("holder exited rc=%s: %s" % (self.holder.returncode,
                                                       self.holder.stderr.read()))
            time.sleep(0.02)
        self.fail("holder never acquired the lock")

    def _battery(self, *args, timeout=300):
        return subprocess.run([str(BATTERY), *args], cwd=str(ROOT),
                              capture_output=True, text=True, timeout=timeout)

    def test_the_second_battery_exits_nonzero_naming_the_first(self):
        pid = self._hold(argv="--only RT14")
        before = (self.lockdir / "info").read_bytes()
        out = self._battery("--dry-run", "--only", SAMPLE_MUTATION, timeout=120)
        self.assertNotEqual(0, out.returncode)
        self.assertEqual(75, out.returncode, out.stderr)
        self.assertIn(str(pid), out.stderr)
        self.assertIn("--only RT14", out.stderr)
        # The first run is untouched: same lock, same holder, still alive.
        self.assertEqual(before, (self.lockdir / "info").read_bytes())
        self.assertIsNone(self.holder.poll())

    def test_contention_is_reported_as_contention_even_on_a_dirty_tree(self):
        # A live holder mid-batch has a mutant on disk. Reporting THAT as
        # "commit your work first" is the misattribution one layer over.
        rel, path = _first_mutable_file()
        original = path.read_bytes()
        self.addCleanup(path.write_bytes, original)
        pid = self._hold()
        path.write_bytes(original + b"\n// injected by a test\n")

        out = self._battery("--dry-run", "--only", SAMPLE_MUTATION, timeout=120)
        self.assertEqual(75, out.returncode, out.stderr)
        self.assertIn(str(pid), out.stderr)
        self.assertNotIn("commit your work first", out.stderr)

    def test_a_dirty_tree_with_NO_holder_still_says_commit_your_work_first(self):
        # Positive control for the assertion above. Without this, "did not say
        # commit your work first" could be true because the guard was deleted.
        rel, path = _first_mutable_file()
        original = path.read_bytes()
        self.addCleanup(path.write_bytes, original)
        path.write_bytes(original + b"\n// injected by a test\n")
        out = self._battery("--dry-run", "--only", SAMPLE_MUTATION, timeout=120)
        self.assertEqual(2, out.returncode, out.stderr)
        self.assertIn("commit your work first", out.stderr)
        self.assertIn(rel, out.stderr)

    def test_a_run_whose_output_pipe_closes_still_releases_the_lock(self):
        # Operators pipe this battery to `head`/`tail` constantly — CLAUDE.md
        # documents `fast-gate.sh | tail` as its own hazard. Bash dies on
        # SIGPIPE WITHOUT running an EXIT trap unless PIPE is trapped, so an
        # untrapped run would leave both its lock and, one line earlier, its
        # mutant behind. Observed doing exactly that while demonstrating crash
        # recovery for this bead.
        proc = subprocess.run(
            "'%s' --dry-run --only %s 2>&1 | head -3" % (BATTERY, SAMPLE_MUTATION),
            shell=True, cwd=str(ROOT), capture_output=True, text=True, timeout=300)
        self.assertIn("mutation-battery:", proc.stdout)
        self.assertFalse(self.lockdir.exists(),
                         "SIGPIPE left the lock behind: " + proc.stdout)
        status = subprocess.run(["git", "status", "--porcelain", "--", "Playhead"],
                                cwd=str(ROOT), capture_output=True, text=True).stdout
        self.assertEqual("", status.strip(), "SIGPIPE left a mutant on disk")

    def test_an_uncontended_dry_run_still_succeeds(self):
        # Positive control for the refusals above: they are contention, not a
        # battery that now refuses to start at all. This one really applies and
        # restores a mutation, so it also proves the lock is released on exit.
        out = self._battery("--dry-run", "--only", SAMPLE_MUTATION, timeout=300)
        self.assertEqual(0, out.returncode, out.stdout + out.stderr)
        self.assertFalse(self.lockdir.exists(), "the lock was not released")
        # Scoped to the app sources — the battery mutates only those, and the
        # caller's own in-flight work on scripts/ or tests is not evidence about
        # the restore. (`--` with a pathspec, so an untracked file elsewhere
        # cannot read as a leftover mutant.)
        status = subprocess.run(["git", "status", "--porcelain", "--", "Playhead"],
                                cwd=str(ROOT), capture_output=True, text=True).stdout
        self.assertEqual("", status.strip(), "the dry run left the tree dirty")
        # Positive control on that observer: it can see a dirty Playhead file.
        rel, path = _first_mutable_file()
        original = path.read_bytes()
        try:
            path.write_bytes(original + b"\n// injected by a test\n")
            dirtied = subprocess.run(["git", "status", "--porcelain", "--", "Playhead"],
                                     cwd=str(ROOT), capture_output=True, text=True).stdout
        finally:
            path.write_bytes(original)
        self.assertIn(rel, dirtied)


if __name__ == "__main__":
    unittest.main()
