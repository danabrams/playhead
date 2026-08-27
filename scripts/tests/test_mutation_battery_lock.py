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

    def spawn_named(self, basename, *args):
        """Start a live process whose `ps -o command=` contains `basename`.

        Two tests below turn on what a pid is RUNNING, so the argv is the point
        and `exec sleep` — the obvious way to make one killable — would erase it.
        Hence a process GROUP: the wrapper keeps its name and the group kill
        reaps the `sleep` with it. stdio goes to /dev/null because an orphaned
        `sleep` holding the caller's stdout makes `python3 -m unittest … | tee`
        hang for five minutes after an otherwise green run.
        """
        d = pathlib.Path(tempfile.mkdtemp(prefix="playhead-mb-fakeproc."))
        self.addCleanup(shutil.rmtree, str(d), True)
        exe = d / basename
        exe.write_text("#!/bin/bash\nsleep 300\n")
        exe.chmod(0o755)
        proc = subprocess.Popen(
            [str(exe), *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)

        def reap():
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            proc.wait(timeout=10)

        self.addCleanup(reap)
        # The wrapper must be visible to `ps` before anything asks about it.
        deadline = time.time() + 10
        while time.time() < deadline:
            cmd = subprocess.run(["ps", "-o", "command=", "-p", str(proc.pid)],
                                 capture_output=True, text=True).stdout
            if basename in cmd:
                return proc
            time.sleep(0.02)
        self.fail("fake %s never appeared in ps" % basename)

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
# 1b. Taking the lock is not the same as holding it (review round)
#
# Three planted states, each of which the shipped code judged wrong, and all
# three are the bead's own defect class one layer down: an ABSENCE read as
# somebody else's PRESENCE, and a directory read as an owner.
# ---------------------------------------------------------------------------
class TestTakingIsNotHolding(LockTestCase):
    def test_a_mkdir_that_fails_for_another_reason_is_not_called_contention(self):
        # `mkdir` failing is not evidence anybody holds this. ENOSPC — on a box
        # that hit 100 % capacity four times on 2026-08-01 — EACCES and EROFS all
        # land on the same non-zero exit as EEXIST, and every one of them used to
        # print "Another run took it while this one was waiting" and return 75:
        # EX_TEMPFAIL, i.e. wait for a process that does not exist.
        gitdir = self.repo.dir / ".git"
        gitdir.chmod(0o500)
        self.addCleanup(gitdir.chmod, 0o700)
        out = bash('mb_lock_acquire "--only M05"; echo "RC=$?"', self.repo.dir, timeout=90)
        self.assertIn("RC=2", out.stdout, out.stderr)
        self.assertIn("NOBODY holds it", out.stderr)
        self.assertNotIn("Another run took it", out.stderr)
        self.assertIn("Permission denied", out.stderr)  # mkdir's own words, quoted

    def test_a_lock_whose_owner_cannot_be_recorded_is_not_claimed(self):
        # The chain this closes: mkdir succeeds, the identity write fails, the
        # result is discarded, MB_LOCK_OWNED=1 and the run mutates the tree while
        # its lock sits ownerless — and 300 s later, INSIDE a 4-9 minute run, the
        # next battery reclaims that directory as ABANDONED. Two batteries, one
        # worktree, which is the entire bead.
        out = bash(
            'mkdir() { command mkdir "$@" && chmod 500 "${!#}"; }\n'
            'mb_lock_acquire "--only M05"; echo "RC=$?"; echo "OWNED=$MB_LOCK_OWNED"',
            self.repo.dir, timeout=90)
        self.assertIn("RC=2", out.stdout, out.stderr)
        self.assertIn("OWNED=0", out.stdout)
        self.assertIn("could NOT record who holds it", out.stderr)
        # And the directory is GONE rather than left ownerless — leaving it is
        # what arms the ABANDONED reclaim above.
        self.assertFalse(self.repo.lockdir.exists(), out.stderr)

    def test_an_uncontended_acquire_still_records_a_complete_identity(self):
        # Positive control for both tests above on the same observer: without it
        # they would pass against a lock that never manages to record anything.
        out = bash('mb_lock_acquire "--only M07"; echo "RC=$?"', self.repo.dir)
        self.assertIn("RC=0", out.stdout, out.stderr)
        keys = [line.split("=", 1)[0]
                for line in (self.repo.lockdir / "info").read_text().splitlines()]
        self.assertEqual(
            ["pid", "pid_start", "started", "started_human", "worktree", "host", "argv"],
            keys)
        # `info` is renamed into place, so no reader can ever see half of it and
        # no temp file is left inside the lock for one to trip over.
        self.assertEqual(sorted(p.name for p in self.repo.lockdir.iterdir()),
                         ["info", "state"])

    def test_record_pre_refuses_rather_than_mutating_with_nothing_recorded(self):
        # The same unchecked-write class one call later, and the last moment
        # before the first mutation. Without the pristine hashes on disk a crash
        # leaves a mutant no later run can tell from somebody's work — which
        # `mb_lock_recover` correctly REFUSES to act on, i.e. a wedged worktree
        # needing a human. Refusing here costs nothing: nothing is mutated yet.
        out = bash('mb_lock_acquire "" || exit 9\n'
                   'chmod 500 "$MB_LOCK_DIR"\n'
                   'mb_lock_record_pre "%s"; echo "RC=$?"\n'
                   'chmod 700 "$MB_LOCK_DIR"' % self.repo.file,
                   self.repo.dir)
        self.assertIn("RC=1", out.stdout, out.stderr)
        self.assertIn("could not record the pristine hashes", out.stderr)

        # Positive control on the same observer: a writable lock records them.
        shutil.rmtree(self.repo.lockdir, ignore_errors=True)
        out = bash('mb_lock_acquire "" || exit 9\n'
                   'mb_lock_record_pre "%s"; echo "RC=$?"\n'
                   'grep -c "^file" "$MB_LOCK_DIR/state"' % self.repo.file,
                   self.repo.dir)
        self.assertIn("RC=0", out.stdout, out.stderr)
        self.assertIn("1", out.stdout.split()[-1])

    def test_a_regular_file_at_the_lock_path_is_named_as_such(self):
        self.repo.lockdir.parent.mkdir(parents=True, exist_ok=True)
        self.repo.lockdir.write_text("not a directory\n")
        out = bash('mb_lock_acquire ""; echo "RC=$?"', self.repo.dir, timeout=90)
        self.assertIn("RC=2", out.stdout, out.stderr)
        self.assertIn("NOT a directory", out.stderr)


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

    def test_a_record_this_run_cannot_read_is_not_evidence_the_holder_is_dead(self):
        # The other reading of a start-time mismatch, and the dangerous one. A
        # HALF-WRITTEN `pid_start` mismatches exactly like a recycled pid does,
        # and the shipped code reclaimed on it: planted with `pid=<live>` plus a
        # truncated start time it returned 0 with MB_LOCK_OWNED=1 while the named
        # holder was still running — two batteries in one worktree.
        #
        # `mb__write_info` renames the record into place now, so this code can no
        # longer author a partial one; this is the second wall. Before believing a
        # recorded pid is dead, ask what that pid is running.
        holder = self.spawn_named("mutation-battery.sh", "--only", "M05")
        self.repo.lockdir.mkdir(parents=True)
        (self.repo.lockdir / "info").write_text("pid=%d\npid_start=Thu Aug" % holder.pid)
        out = bash('mb_lock_acquire ""; echo "RC=$?"; echo "OWNED=$MB_LOCK_OWNED"',
                   self.repo.dir, timeout=90)
        self.assertIn("RC=75", out.stdout, out.stderr)
        self.assertIn("OWNED=0", out.stdout)
        self.assertIn("IS RUNNING A MUTATION BATTERY", out.stderr)
        self.assertTrue(self.repo.lockdir.exists(), "the live holder's lock was removed")
        # The holder is untouched and still running.
        self.assertIsNone(holder.poll())

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

    def test_a_wedged_runner_names_the_live_xcodebuild_before_the_recovery(self):
        # playhead-zsqh, and the reason this belongs in the RUNNER branch and not
        # only under CONTENTION: a SIGKILLed battery leaves its `xcodebuild` child
        # alive (reproduced in this review round — pid 50621 outlived its parent
        # 44904) and that orphan holds the simulator. `simctl shutdown all` loses
        # to it, because the orphan boots the device again underneath, so the
        # recovery reads as "did not work" unless the process to kill is named.
        orphan = self.spawn_named(
            "xcodebuild", "test", "-scheme", "Playhead", "-testPlan", "PlayheadFastTests")
        err = self.diagnose(
            "Testing started\nblessSimulatorHub failed. Simulator service hub "
            "IS NOT still alive\n** TEST FAILED **\n")
        self.assertIn("RUNNER NEVER LAUNCHED", err)
        self.assertIn("STILL LIVE", err)
        self.assertIn(str(orphan.pid), err)
        self.assertIn("ORPHAN", err)
        # Named BEFORE the recovery command, or the recovery gets blamed.
        self.assertLess(err.index("KILL IT FIRST"), err.index("simctl shutdown all"))

    def test_a_wedged_runner_with_no_live_build_does_not_invent_an_orphan(self):
        # Negative control for the test above on the same observer: with nothing
        # running it must not print the orphan paragraph at all.
        err = self.diagnose(
            "Testing started\nblessSimulatorHub failed. Simulator service hub "
            "IS NOT still alive\n** TEST FAILED **\n")
        self.assertIn("RUNNER NEVER LAUNCHED", err)
        self.assertNotIn("STILL LIVE", err)
        self.assertNotIn("ORPHAN", err)

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

    def test_the_disk_refusal_names_the_work_dir_THIS_RUN_is_holding(self):
        """playhead-gjlp0 R4.

        Since playhead-gjlp0 the battery keeps an `.xcresult` per non-KILL batch
        inside `$WORK`, so a long series accumulates hundreds of megabytes DURING
        the run — the one way a `28` can be the run's own doing. Both remedies
        this arm printed are useless for it: `disk-cleanup.sh` sweeps
        `/private/tmp/playhead-*` at THREE DAYS and this directory is minutes
        old, so a reader who follows the advice reclaims nothing and concludes
        the box is short.
        """
        work = self.repo.dir / "work"
        (work / "batch-1.xcresult").mkdir(parents=True)
        (work / "batch-2.xcresult").mkdir(parents=True)
        (work / "batch-1.log").write_text("x" * 4096)
        log = self.repo.dir / "run.log"
        log.write_text("")
        err = bash('WORK="%s"; mb_diagnose_no_tests "%s" 28 "batch 7"' % (work, log),
                   self.repo.dir).stderr
        self.assertIn("THIS RUN is holding", err)
        self.assertIn(str(work), err)
        # TWO here against THREE in the mid-run rail above: see the note there
        # for why the two counts must differ.
        self.assertIn("2 .xcresult bundle(s)", err)
        self.assertNotIn("3 .xcresult bundle(s)", err)
        self.assertIn("3-day sweep", err)
        # AND WHAT TO DO, not only why the sweep will not do it. Mutant L4
        # survived the first cut of this rail by deleting the only sentence
        # that says the remedy is by hand — the SECOND time in this round a
        # rail pinned the fact and left the action unpinned (the first was
        # mutant R4-5 in test_mutation_verdict.py). Ask of every message rail:
        # does it pin what to DO, or only what happened?
        self.assertIn("remove it by hand", err)

    def test_the_MID_RUN_disk_arm_names_the_work_dir_too(self):
        """playhead-gjlp0 R5, and it is R4's own finding in the sibling arm.

        R4 put the `$WORK` clause on the `rc=28` arm — fast-gate's preflight
        REFUSING before a batch — and left `DISK EXHAUSTION mid-run` eight
        lines below it printing exactly the two remedies R4 had just measured
        as useless: `disk-cleanup.sh` at three days, and somebody else's
        `$TMPDIR/Deleting-*`. If anything this arm is MORE the run's own doing,
        because the preflight passed at this batch's start and the bundles this
        bead keeps accumulated after it.
        """
        # THREE bundles, where the `rc=28` rail below builds TWO — deliberately.
        # A rail that asserts the count its own fixture happens to have cannot
        # tell a `find` from a hard-coded literal: driven at R5, replacing the
        # `find` with the constant `2` SURVIVED the rail R4 shipped. Two rails
        # over one clause with DIFFERENT counts kill every constant between
        # them, which is the cheapest form the anti-vacuity half can take here.
        work = self.repo.dir / "work"
        for n in (1, 2, 3):
            (work / ("batch-%d.xcresult" % n)).mkdir(parents=True, exist_ok=True)
        (work / "batch-1.log").write_text("x" * 4096)
        log = self.repo.dir / "run.log"
        log.write_text("Testing started\nerror: No space left on device\n")
        err = bash('WORK="%s"; mb_diagnose_no_tests "%s" 65 "batch 7"' % (work, log),
                   self.repo.dir).stderr
        self.assertIn("DISK EXHAUSTION mid-run", err)
        self.assertIn("THIS RUN is holding", err)
        self.assertIn(str(work), err)
        self.assertIn("3 .xcresult bundle(s)", err)
        self.assertNotIn("2 .xcresult bundle(s)", err)
        # What to DO, not only what happened — the hole `L4` and `R4-5` both
        # went through at R4.
        self.assertIn("remove it by hand", err)

    def test_the_mid_run_disk_arm_claims_no_work_dir_when_there_is_none(self):
        """The anti-fabrication half of the rail above, on the same arm."""
        err = self.diagnose("Testing started\nerror: No space left on device\n")
        self.assertIn("DISK EXHAUSTION mid-run", err)
        self.assertNotIn("THIS RUN is holding", err)

    def test_both_disk_arms_say_it_out_of_ONE_function(self):
        """ANTI-DRIFT, and the reason this is a function rather than a copy.

        The two arms said the same thing for one round and then did not,
        because R4 edited one of them. A rail over the OUTPUT of each arm
        cannot see that they have drifted apart in wording; a rail over the
        SOURCE can, and it is the property that was actually violated.
        """
        text = LOCK_SH.read_text(encoding="utf-8")
        self.assertEqual(text.count("THIS RUN is holding"), 1, text.count("THIS RUN is holding"))
        self.assertEqual(text.count("mb__say_work_holdings\n"), 2)

    def test_the_disk_refusal_claims_no_work_dir_when_there_is_none(self):
        """The anti-fabrication half, and the reason the clause is guarded.

        `mb_diagnose_no_tests` is called from these rails and from the battery,
        and only the battery has a `$WORK`. A clause that printed `holding 0
        bundles` for a directory nobody created would be a measurement of an
        absence read as a measurement of a thing — which is the defect the bead
        this clause belongs to exists to remove.
        """
        err = self.diagnose("", rc=28)
        self.assertIn("DISK", err)
        self.assertNotIn("THIS RUN is holding", err)

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
