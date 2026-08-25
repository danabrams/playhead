"""Tests for playhead-vk68m's descriptor-path instrument (scripts/gate-fd-paths.py).

The unit under test answers "WHAT are the test host's open descriptors", and
the whole reason it exists is that the two obvious answers are wrong in
opposite directions: `lsof | wc -l` over-reports (it lists cwd/rtd/txt and every
mapped dylib) and `PROC_PIDLISTFDS` with a NULL buffer reports table CAPACITY,
which never falls.

So the rails come in two kinds and both are load-bearing:

  * PURE rails over the parsing and grouping, which need no kernel.
  * KERNEL rails that open a known number of files at known paths and require
    the instrument to report exactly those. A ctypes struct with a wrong offset
    does not throw — it returns a plausible string from the wrong bytes — so
    "did it return a string" is never the check; "is it THE string" is.

Conventions follow test_gate_memory_verdict.py: importlib module loading,
stdlib unittest, in-code fixtures.
"""

import ctypes
import importlib.util
import io
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stderr

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


gfp = _load("gate_fd_paths", "gate-fd-paths.py")


class SelfTestRails(unittest.TestCase):
    """The instrument must prove its own struct layout, and must be ABLE to fail."""

    def test_self_test_passes_on_this_box(self):
        ok, detail = gfp.self_test()
        self.assertTrue(ok, detail)

    def test_struct_is_the_size_the_kernel_writes(self):
        # vnode_fdinfowithpath = proc_fileinfo(24) + vnode_info_path(1176).
        # A layout that is the wrong SIZE is caught even when it would have
        # produced a readable string, which is the failure mode that matters.
        self.assertEqual(gfp._VNODE_PATH_SIZE, 1200)

    def test_self_test_FAILS_when_the_layout_is_wrong(self):
        """Anti-vacuity: a green self-test must be evidence, so make it red.

        A rail that can only ever pass is indistinguishable from no rail.
        """
        original = gfp._VNODE_PATH_SIZE
        try:
            gfp._VNODE_PATH_SIZE = original - 1
            ok, detail = gfp.self_test()
            self.assertFalse(ok)
            self.assertIn("no path", detail)
        finally:
            gfp._VNODE_PATH_SIZE = original
        self.assertTrue(gfp.self_test()[0], "the rail must restore the module")


class KernelRails(unittest.TestCase):
    """Open a known number of files at known paths; require exactly those."""

    def test_count_moves_by_exactly_the_number_opened_and_back(self):
        pid = os.getpid()
        before = gfp.snapshot(pid)
        self.assertIsNotNone(before)
        with tempfile.TemporaryDirectory(prefix="vk68m-rail-") as tmp:
            handles = [open(os.path.join(tmp, f"f{i}.probe"), "w") for i in range(37)]
            try:
                during = gfp.snapshot(pid)
                self.assertEqual(during["count"] - before["count"], 37)
                probes = [r["path"] for r in during["rows"]
                          if r["path"].endswith(".probe")]
                self.assertEqual(len(probes), 37)
                self.assertTrue(all(os.path.basename(p).startswith("f") for p in probes))
            finally:
                for handle in handles:
                    handle.close()
            after = gfp.snapshot(pid)
        self.assertEqual(after["count"], before["count"])

    def test_the_path_is_the_path_and_not_merely_a_string(self):
        with tempfile.NamedTemporaryFile(prefix="vk68m-rail-", suffix=".db") as probe:
            rows = gfp.snapshot(os.getpid())["rows"]
            paths = {os.path.realpath(r["path"]) for r in rows if not r["path"].startswith("<")}
        self.assertIn(os.path.realpath(probe.name), paths)

    def test_a_socket_has_no_path_and_is_named_rather_than_dropped(self):
        pid = os.getpid()
        left, right = socket.socketpair()
        try:
            snap = gfp.snapshot(pid)
            kinds = {r["fd"]: r["kind"] for r in snap["rows"]}
            self.assertEqual(kinds.get(left.fileno()), "socket")
            row = next(r for r in snap["rows"] if r["fd"] == left.fileno())
            self.assertEqual(row["path"], "<socket>")
            # and the path reader declines rather than inventing one
            self.assertEqual(gfp.fd_path(pid, left.fileno()), ("", False))
        finally:
            left.close()
            right.close()

    def test_an_unreadable_table_is_None_and_never_an_empty_list(self):
        """`None` is `not recorded`; `[]` would be a claim that nobody is open."""
        self.assertIsNone(gfp.list_fds(0))
        self.assertIsNone(gfp.list_fds(-1))
        self.assertIsNone(gfp.snapshot(0))
        # A pid that cannot exist on a 32-bit pid space.
        self.assertIsNone(gfp.list_fds(0x7FFFFFF0))

    def test_max_fd_is_the_highest_number_not_the_count(self):
        """The two must be SEPARATED by the fixture, or the rail is vacuous.

        In a quiet Python process the descriptor table is very nearly
        contiguous, so `max_fd` and `count - 1` are the same number and a
        `max_fd` computed as `len(rows) - 1` passes every assertion here —
        measured: mutation F01 survived this test as originally written. A
        deliberately high descriptor is what makes the two disagree, and it is
        also the shape that matters in the field, where the peak reading is
        `max_fd = 2559 = soft - 1` against a count of 2,539.
        """
        pid = os.getpid()
        with tempfile.NamedTemporaryFile(prefix="vk68m-highfd-") as probe:
            high = os.dup2(probe.fileno(), 900)
            try:
                snap = gfp.snapshot(pid)
                self.assertEqual(snap["max_fd"], 900)
                self.assertGreater(snap["max_fd"], snap["count"] - 1,
                                   "fixture failed to separate max_fd from the count")
                self.assertEqual(snap["max_fd"], max(r["fd"] for r in snap["rows"]))
            finally:
                os.close(high)


class LsofSplitRails(unittest.TestCase):
    """The FD column is a NUMBER for a descriptor and a WORD otherwise."""

    SAMPLE = (
        "p123\n"
        "f3\ntREG\nn/a/b.sqlite\n"
        "fcwd\ntDIR\nn/home\n"
        "ftxt\ntREG\nn/lib.dylib\n"
        "ftxt\ntREG\nn/lib2.dylib\n"
        "f12u\ntREG\nn/a/c.txt\n"
        "fNOFD\ntunknown\nn/x\n"
    )

    def test_only_numeric_fd_rows_count_as_descriptors(self):
        parsed = gfp.parse_lsof_fields(self.SAMPLE)
        self.assertEqual(parsed["descriptor_rows"], 2)
        self.assertEqual(parsed["distinct_fds"], 2)
        self.assertEqual(parsed["fds"], [3, 12])

    def test_the_excluded_rows_are_NAMED_rather_than_silently_dropped(self):
        parsed = gfp.parse_lsof_fields(self.SAMPLE)
        self.assertEqual(parsed["excluded_rows"], 4)
        self.assertEqual(parsed["excluded_by_kind"], {"cwd": 1, "txt": 2, "NOFD": 1})
        self.assertEqual(parsed["total_rows"], 6)

    def test_the_mode_suffix_is_stripped_from_the_fd_number(self):
        for spelling in ("f7", "f7r", "f7w", "f7u", "f7rW"):
            parsed = gfp.parse_lsof_fields(f"p1\n{spelling}\ntREG\nn/z\n")
            self.assertEqual(parsed["fds"], [7], spelling)

    def test_empty_output_is_zero_and_still_ok(self):
        parsed = gfp.parse_lsof_fields("")
        self.assertTrue(parsed["ok"])
        self.assertEqual(parsed["descriptor_rows"], 0)
        self.assertEqual(parsed["total_rows"], 0)


class GroupingRails(unittest.TestCase):

    def test_each_group_claims_its_own_shape(self):
        cases = {
            "/Users/dabrams/Library/Developer/CoreSimulator/Devices/ABC/data/x.sqlite":
                "simulator device data",
            "/Library/Developer/CoreSimulator/Volumes/iOS_27/x":
                "simulator runtime bundle",
            "/Users/dabrams/playhead/.worktrees/a/.derivedData/Build/x.o":
                "derived data / build products",
            "/Users/dabrams/playhead/Playhead/App/Foo.swift": "repo source tree",
            "/System/Library/Frameworks/Foundation.framework/Foundation":
                "system framework / dylib",
            "/private/tmp/whatever": "tmp",
            "/dev/null": "other",
        }
        for path, group in cases.items():
            self.assertEqual(gfp.group_of(path), group, path)

    def test_a_non_path_placeholder_passes_through_unchanged(self):
        for placeholder in ("<socket>", "<kqueue>", "<pipe>", "<gone>"):
            self.assertEqual(gfp.group_of(placeholder), placeholder)
            self.assertEqual(gfp.leaf_family(placeholder), placeholder)

    def test_leaf_family_collapses_the_identifiers_that_make_a_histogram_useless(self):
        uuid = "/a/11111111-2222-3333-4444-555555555555/store.sqlite-wal"
        self.assertEqual(gfp.leaf_family(uuid), "/a/<uuid>/*.sqlite-wal")
        hex32 = "/a/0123456789abcdef0123456789abcdef/x.db"
        self.assertEqual(gfp.leaf_family(hex32), "/a/<hex32>/*.db")

    def test_leaf_family_keeps_a_suffixless_name_distinguishable(self):
        self.assertEqual(gfp.leaf_family("/dev/null"), "/dev/*/<no-suffix>")

    def test_two_stores_differing_only_by_uuid_land_in_ONE_row(self):
        rows = [
            {"fd": 3, "kind": "vnode",
             "path": "/d/aaaaaaaa-1111-2222-3333-444444444444/analysis.sqlite"},
            {"fd": 4, "kind": "vnode",
             "path": "/d/bbbbbbbb-1111-2222-3333-444444444444/analysis.sqlite"},
        ]
        summary = gfp.summarise({"pid": 1, "epoch": 0.0, "count": 2,
                                 "max_fd": 4, "rows": rows})
        self.assertEqual(summary["by_family"], {"/d/<uuid>/*.sqlite": 2})
        self.assertEqual(summary["distinct_paths"], 2)


class SummaryRails(unittest.TestCase):

    ROWS = [
        {"fd": 3, "kind": "vnode", "path": "/a/x.sqlite"},
        {"fd": 4, "kind": "vnode", "path": "/a/x.sqlite"},
        {"fd": 5, "kind": "socket", "path": "<socket>"},
    ]

    def _summary(self):
        return gfp.summarise({"pid": 9, "epoch": 1.0, "count": 3,
                              "max_fd": 5, "rows": self.ROWS})

    def test_every_row_is_counted_exactly_once_in_each_breakdown(self):
        summary = self._summary()
        self.assertEqual(sum(summary["by_kind"].values()), 3)
        self.assertEqual(sum(summary["by_group"].values()), 3)
        self.assertEqual(sum(summary["by_family"].values()), 3)

    def test_a_repeated_path_is_reported_as_repeated(self):
        summary = self._summary()
        self.assertEqual(summary["distinct_paths"], 2)
        self.assertEqual(summary["duplicated_paths"], 2)


class AtomicWriteRails(unittest.TestCase):

    def test_the_dump_arrives_and_leaves_no_partial_behind(self):
        with tempfile.TemporaryDirectory(prefix="vk68m-atomic-") as tmp:
            target = os.path.join(tmp, "peak.json")
            gfp._atomic_json(target, {"count": 2539})
            self.assertFalse(os.path.exists(target + ".partial"))
            with open(target) as fh:
                self.assertEqual(json.load(fh)["count"], 2539)

    def test_a_FAILED_write_leaves_the_PREVIOUS_dump_intact(self):
        """Anti-vacuity: the test above passes against a plain `open(path,"w")`.

        Measured — mutation F02 replaced the `.partial` + `os.replace` pair with
        a direct write and the whole suite stayed green, so the rail named the
        property and did not test it. Atomicity is only observable when a write
        FAILS PART WAY: `json.dump` streams, so it emits the opening brace and
        the keys it can serialise before it hits one it cannot. A direct write
        truncates the reader's file and then abandons it half-written, which is
        exactly what `peak.json` must never do while a run is live.
        """
        class _Unserialisable:
            pass

        with tempfile.TemporaryDirectory(prefix="vk68m-atomic-") as tmp:
            target = os.path.join(tmp, "peak.json")
            gfp._atomic_json(target, {"count": 453, "rows": ["a" * 200]})
            with self.assertRaises(TypeError):
                gfp._atomic_json(target, {"count": 2539, "rows": [_Unserialisable()]})
            with open(target) as fh:
                survived = json.load(fh)
            self.assertEqual(survived["count"], 453,
                             "the failed write clobbered the dump it replaced")


class PinnedHostRails(unittest.TestCase):
    """`--last` must not be rewritten by a DIFFERENT process.

    `find_test_host()` picks the largest-RSS process under `/Playhead.app/`,
    which is right during a run and wrong the instant it ends — a stale
    simulator app satisfies the same predicate. On this bead's own first run
    that clobbered the tail dump with twelve descriptors belonging to something
    else, and the file that names the run's tail held another process's.
    """

    def test_the_pinned_host_writes_to_the_plain_path(self):
        self.assertEqual(gfp._scoped("/a/last.json", 7, 7), "/a/last.json")

    def test_an_interloper_gets_its_own_file_and_keeps_the_extension(self):
        self.assertEqual(gfp._scoped("/a/last.json", 9, 7), "/a/last.pid9.json")
        self.assertEqual(gfp._scoped("/a/peak.json", 42, 7), "/a/peak.pid42.json")

    def test_a_suffixless_path_is_still_distinguished(self):
        self.assertEqual(gfp._scoped("/a/last", 9, 7), "/a/last.pid9")

    def test_the_splice_lands_BEFORE_the_extension(self):
        """`last.json.pid9` reads as a partial file; `last.pid9.json` does not."""
        scoped = gfp._scoped("/a/last.json", 9, 7)
        self.assertTrue(scoped.endswith(".json"))
        self.assertNotIn(".json.", scoped)

    def test_a_DOT_IN_A_DIRECTORY_does_not_move_the_splice_up_the_path(self):
        """Splitting the whole path on its last dot renames a DIRECTORY."""
        self.assertEqual(gfp._scoped("/a/run.1/last", 9, 7), "/a/run.1/last.pid9")
        self.assertEqual(gfp._scoped("/a/run.1/last.json", 9, 7),
                         "/a/run.1/last.pid9.json")

    def test_a_bare_relative_name_keeps_its_shape(self):
        self.assertEqual(gfp._scoped("last.json", 9, 7), "last.pid9.json")
        self.assertEqual(gfp._scoped("/last.json", 9, 7), "/last.pid9.json")


class HostDiscoveryRails(unittest.TestCase):

    def test_discovery_returns_a_pid_or_zero_and_never_raises(self):
        found = gfp.find_test_host()
        self.assertIsInstance(found, int)
        self.assertGreaterEqual(found, 0)

    def test_the_ps_invocation_this_box_ACTUALLY_ACCEPTS(self):
        """The rail that would have caught `etimes`.

        A revision of this function asked macOS `ps` for `etimes`, a Linux
        keyword it rejects: `ps` printed a usage error, wrote nothing to stdout,
        and discovery returned 0 forever without saying so. Every other rail
        here passed, because "no host found" is indistinguishable from "no host
        exists". So the rail is on the COMMAND: run exactly what the function
        runs and require rc 0 and a populated stdout.
        """
        import subprocess
        result = subprocess.run(
            ["ps", "-Ao", "pid=,rss=,comm="],
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreater(len(result.stdout.splitlines()), 10)
        # and every line the parser accepts must really parse
        parsed = 0
        for line in result.stdout.splitlines():
            parts = line.strip().split(None, 2)
            if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
                parsed += 1
        self.assertGreater(parsed, 10, "the ps FORMAT changed under the parser")

    def test_a_ps_failure_is_reported_rather_than_returned_as_zero(self):
        """0 must mean `no match`, never `the instrument did not run`."""
        import io
        import subprocess
        from contextlib import redirect_stderr

        class _Failed:
            returncode = 1
            stdout = ""
            stderr = "ps: nope: keyword not found"

        original = subprocess.run
        captured = io.StringIO()
        try:
            subprocess.run = lambda *a, **k: _Failed()
            with redirect_stderr(captured):
                self.assertEqual(gfp.find_test_host(), 0)
        finally:
            subprocess.run = original
        self.assertIn("`ps` FAILED", captured.getvalue())
        self.assertIn("keyword not found", captured.getvalue())


class _StubFdInfoLibc:
    """A libc whose `proc_pidfdinfo` writes a path WE choose and returns a byte
    count WE choose — independently.

    That independence is the whole point: a kernel that writes FEWER bytes than
    the struct still leaves a perfectly readable string in the buffer, so "did
    a string come back" can never be the check. Only the returned length can
    tell a complete record from a partial one.
    """

    def __init__(self, path: bytes, written: int, terminate: bool = True):
        self.path = path
        self.written = written
        self.terminate = terminate

    def proc_pidfdinfo(self, _pid, _fd, _flavor, ref, _size):
        target = ctypes.cast(ref, ctypes.POINTER(gfp._VnodeFdInfoWithPath)).contents
        raw = self.path[:gfp.MAXPATHLEN]
        for i, byte in enumerate(raw):
            target.pvip.vip_path[i] = byte
        if self.terminate and len(raw) < gfp.MAXPATHLEN:
            target.pvip.vip_path[len(raw)] = 0
        return self.written


class _StubListLibc:
    """A libc whose `proc_pidinfo` reports a table size and a write length.

    `written` is a callable over the buffer size handed in, so a stub can say
    "I filled whatever you gave me" — the signature of a TRUNCATED read, which
    is otherwise indistinguishable from a complete one.
    """

    def __init__(self, sized, written):
        self.sized = sized
        self.written = written
        self.sizes_seen: list[int] = []

    def proc_pidinfo(self, _pid, _flavor, _arg, buffer, size):
        # `size` arrives as a `ctypes.c_int`, exactly as the real call receives
        # it; unwrapping it here is what keeps the stub honest about the
        # argument the production code actually passes.
        size = getattr(size, "value", size)
        if buffer is None:
            return self.sized
        self.sizes_seen.append(size)
        return self.written(size)


class PathDecodeRails(unittest.TestCase):
    """A record shorter than the struct is REFUSED, not decoded."""

    def _with_libc(self, stub):
        original = gfp._LIBC
        gfp._LIBC = stub
        self.addCleanup(lambda: setattr(gfp, "_LIBC", original))

    def test_a_full_write_is_decoded(self):
        self._with_libc(_StubFdInfoLibc(b"/a/analysis.sqlite", gfp._VNODE_PATH_SIZE))
        self.assertEqual(gfp.fd_path(1, 3), ("/a/analysis.sqlite", False))

    def test_a_SHORT_write_is_refused_even_though_the_buffer_READS_as_a_path(self):
        self._with_libc(
            _StubFdInfoLibc(b"/a/analysis.sqlite", gfp._VNODE_PATH_SIZE - 8))
        self.assertEqual(gfp.fd_path(1, 3), ("", False))

    def test_an_UNTERMINATED_path_is_marked_truncated_rather_than_silently_cut(self):
        self._with_libc(_StubFdInfoLibc(b"/x" * 600, gfp._VNODE_PATH_SIZE,
                                        terminate=False))
        path, truncated = gfp.fd_path(1, 3)
        self.assertTrue(truncated)
        self.assertEqual(len(path), gfp.MAXPATHLEN)


class TableTruncationRails(unittest.TestCase):
    """A buffer the kernel FILLED is a short count wearing a complete shape."""

    def _with_libc(self, stub):
        original = gfp._LIBC
        gfp._LIBC = stub
        self.addCleanup(lambda: setattr(gfp, "_LIBC", original))
        return stub

    def test_a_read_that_fills_the_buffer_EVERY_time_is_None_not_a_short_count(self):
        stub = self._with_libc(_StubListLibc(800, lambda size: size))
        self.assertIsNone(gfp.list_fds(4242))
        self.assertEqual(len(stub.sizes_seen), gfp._LIST_FDS_ATTEMPTS)
        self.assertEqual(stub.sizes_seen, sorted(stub.sizes_seen),
                         "the retry must GROW the buffer, not repeat it")
        self.assertGreater(stub.sizes_seen[-1], stub.sizes_seen[0])

    def test_a_read_that_fills_the_buffer_ONCE_is_retried_and_then_believed(self):
        first = {"done": False}

        def written(size):
            if not first["done"]:
                first["done"] = True
                return size
            return 40 * gfp._FDINFO_SIZE

        stub = self._with_libc(_StubListLibc(800, written))
        entries = gfp.list_fds(4242)
        self.assertEqual(len(entries), 40)
        self.assertEqual(len(stub.sizes_seen), 2)

    def test_a_short_read_is_believed_at_once(self):
        stub = self._with_libc(_StubListLibc(800, lambda size: 7 * gfp._FDINFO_SIZE))
        self.assertEqual(len(gfp.list_fds(4242)), 7)
        self.assertEqual(len(stub.sizes_seen), 1)


class GoneDescriptorRails(unittest.TestCase):
    """A descriptor that closed under us is COUNTED, never dropped."""

    def test_a_vnode_with_no_readable_path_is_kept_as_gone(self):
        original_list, original_path = gfp.list_fds, gfp.fd_path
        gfp.list_fds = lambda pid: [(3, gfp.PROX_FDTYPE_VNODE),
                                    (4, gfp.PROX_FDTYPE_VNODE)]
        gfp.fd_path = lambda pid, fd: ("/a/b.sqlite", False) if fd == 3 else ("", False)
        try:
            snap = gfp.snapshot(11)
        finally:
            gfp.list_fds, gfp.fd_path = original_list, original_path
        self.assertEqual(snap["count"], 2)
        self.assertEqual([r["path"] for r in snap["rows"]], ["/a/b.sqlite", "<gone>"])


class LsofFailureRails(unittest.TestCase):
    """A cross-check that could not RUN must not read as `zero descriptors`."""

    class _Result:
        def __init__(self, returncode, stdout, stderr=""):
            self.returncode, self.stdout, self.stderr = returncode, stdout, stderr

    def _with_run(self, result):
        original = subprocess.run
        subprocess.run = lambda *a, **k: result
        self.addCleanup(lambda: setattr(subprocess, "run", original))

    def test_a_failing_lsof_is_NOT_ok_and_says_why(self):
        self._with_run(self._Result(1, "", "lsof: no such process"))
        out = gfp.lsof_cross_check(999999)
        self.assertFalse(out["ok"])
        self.assertIn("no such process", out["error"])
        self.assertNotIn("descriptor_rows", out)

    def test_a_nonzero_exit_that_STILL_reported_descriptors_is_believed(self):
        """lsof exits 1 for a file it could not stat while listing the rest."""
        self._with_run(self._Result(1, "p1\nf3\ntREG\nn/a/b.sqlite\n", "lsof: warning"))
        out = gfp.lsof_cross_check(1)
        self.assertTrue(out["ok"])
        self.assertEqual(out["descriptor_rows"], 1)
        self.assertEqual(out["returncode"], 1)

    def test_an_lsof_that_cannot_be_executed_is_an_error_not_a_count(self):
        original = subprocess.run

        def _boom(*_a, **_k):
            raise OSError(2, "No such file or directory")

        subprocess.run = _boom
        try:
            out = gfp.lsof_cross_check(1)
        finally:
            subprocess.run = original
        self.assertFalse(out["ok"])
        self.assertIn("No such file", out["error"])


class PinDecisionRails(unittest.TestCase):
    """Which process owns the un-suffixed dumps, and why it is not first-seen."""

    def test_the_first_process_seen_is_pinned(self):
        self.assertEqual(gfp.pin_decision(0, {}, 58651, 20), "pin")

    def test_the_pinned_process_keeps_the_pin_however_it_moves(self):
        self.assertEqual(gfp.pin_decision(71372, {71372: 2402}, 71372, 453), "keep")
        self.assertEqual(gfp.pin_decision(71372, {71372: 2402}, 71372, 2500), "keep")

    def test_run1s_LEFTOVER_loses_the_pin_to_the_real_host(self):
        """The measured case: 62 samples of a 20-descriptor leftover first.

        `artifacts/run1/full/sample-0001-00020.json.gz` .. `-0062-00020.json.gz`
        are all pid 58651 holding exactly twenty descriptors; the test host
        (71372) appears at sample 63 and climbs to 2,402. First-seen-wins would
        have written the leftover's twenty into `peak.json`.
        """
        self.assertEqual(gfp.pin_decision(58651, {58651: 20}, 71372, 23), "promote")

    def test_the_TWELVE_descriptor_clobber_still_never_takes_the_pin(self):
        """The case the pin was introduced for, and it must stay excluded."""
        self.assertEqual(gfp.pin_decision(71372, {71372: 2402}, 85292, 12), "keep")
        self.assertEqual(gfp.pin_decision(71372, {71372: 2402}, 77131, 23), "keep")

    def test_a_tie_is_not_a_promotion(self):
        """Strictly greater: equality is what a re-launched identical app gives."""
        self.assertEqual(gfp.pin_decision(58651, {58651: 20}, 90001, 20), "keep")


class PerProcessPeakRails(unittest.TestCase):
    """`peak.pid<N>.json` must be a HIGH-WATER file, as its name claims."""

    @staticmethod
    def _snap(pid, count):
        return {"pid": pid, "count": count, "rows": [], "max_fd": count - 1,
                "epoch": 0.0}

    def test_a_higher_sample_replaces_the_peak_and_says_so(self):
        peaks: dict = {}
        self.assertTrue(gfp.record_peak(peaks, 7, self._snap(7, 20)))
        self.assertTrue(gfp.record_peak(peaks, 7, self._snap(7, 2402)))
        self.assertEqual(peaks[7]["count"], 2402)

    def test_a_LOWER_sample_does_not_overwrite_the_peak(self):
        """The tail of a run is 453 and its peak is 2,402; the file keeps 2,402."""
        peaks: dict = {}
        gfp.record_peak(peaks, 7, self._snap(7, 2402))
        self.assertFalse(gfp.record_peak(peaks, 7, self._snap(7, 453)))
        self.assertEqual(peaks[7]["count"], 2402)

    def test_an_EQUAL_sample_is_not_a_new_peak(self):
        peaks: dict = {}
        gfp.record_peak(peaks, 7, self._snap(7, 453))
        self.assertFalse(gfp.record_peak(peaks, 7, self._snap(7, 453)))

    def test_processes_do_not_share_a_peak(self):
        peaks: dict = {}
        gfp.record_peak(peaks, 71372, self._snap(71372, 2402))
        self.assertTrue(gfp.record_peak(peaks, 85292, self._snap(85292, 12)))
        self.assertEqual(peaks[71372]["count"], 2402)
        self.assertEqual(peaks[85292]["count"], 12)


class CensusRails(unittest.TestCase):

    def test_every_process_seen_is_named_with_its_peak_and_the_pin_is_marked(self):
        lines = gfp.census_lines(101, {58651: 62, 71372: 32},
                                 {58651: 20, 71372: 2402}, 71372)
        joined = "\n".join(lines)
        self.assertIn("101 sample(s) over 2 process(es)", joined)
        self.assertIn("pid 71372", joined)
        self.assertIn("2402", joined)
        self.assertIn("pid 58651", joined)
        self.assertIn("20", joined)
        # the PINNED row is marked, and it is the only one that is
        marked = [line for line in lines if "PINNED" in line]
        self.assertEqual(len(marked), 1)
        self.assertIn("71372", marked[0])

    def test_the_biggest_holder_is_listed_first_so_a_wrong_pin_is_visible(self):
        lines = gfp.census_lines(101, {58651: 62, 71372: 32},
                                 {58651: 20, 71372: 2402}, 58651)
        self.assertIn("71372", lines[1])
        self.assertIn("PINNED", lines[2])


class WatchStartupRails(unittest.TestCase):
    """`--watch` before the host exists must WAIT, not exit having seen nothing."""

    def _run_main(self, argv):
        handlers = {sig: signal.getsignal(sig)
                    for sig in (signal.SIGINT, signal.SIGTERM)}
        original = sys.argv
        captured = io.StringIO()
        try:
            sys.argv = ["gate-fd-paths.py"] + argv
            with redirect_stderr(captured):
                rc = gfp.main()
        finally:
            sys.argv = original
            for sig, handler in handlers.items():
                signal.signal(sig, handler)
        return rc, captured.getvalue()

    def test_a_watcher_started_before_the_host_keeps_sampling(self):
        """The default `--deadline 0` used to end the watch on cycle two.

        `gone_since` covered both "has not appeared yet" and "has gone away",
        so a watcher launched ahead of the gate — the only order in which it can
        see the ramp — measured nothing and said nothing about it.
        """
        began = time.monotonic()
        rc, err = self._run_main(
            ["--pid", "2147483632", "--watch", "--interval", "0.05",
             "--max-minutes", "0.006"])
        elapsed = time.monotonic() - began
        self.assertEqual(rc, 0)
        self.assertIn("no /Playhead.app/ process yet", err)
        self.assertGreater(elapsed, 0.15, "it gave up instead of waiting")
        self.assertIn("0 sample(s)", err)

    def test_a_one_shot_run_against_an_unreadable_pid_still_reports_and_exits(self):
        rc, err = self._run_main(["--pid", "2147483632"])
        self.assertEqual(rc, 4)
        self.assertIn("no readable fd table", err)

    def test_peak_json_holds_the_PEAK_and_last_json_holds_the_LAST(self):
        """The two files are different questions and the loop must not conflate them.

        Driven through `main()` on a scripted sequence, because the wiring —
        which snapshot reaches which path — is a property of the sampling loop
        and not of any function a rail can call directly. A version that wrote
        the peak file on every sample kept a correct peak in memory and still
        left `peak.json` holding the tail.
        """
        def _snap(count):
            return {"pid": 4242, "count": count, "max_fd": count + 100,
                    "epoch": 0.0,
                    "rows": [{"fd": i, "kind": "vnode", "path": "/a/x.sqlite"}
                             for i in range(count)]}

        sequence = [_snap(2402), _snap(453), None, None]
        original = gfp.snapshot
        gfp.snapshot = lambda pid: sequence.pop(0) if sequence else None
        try:
            with tempfile.TemporaryDirectory(prefix="vk68m-loop-") as tmp:
                peak = os.path.join(tmp, "peak.json")
                last = os.path.join(tmp, "last.json")
                rc, err = self._run_main(
                    ["--pid", "4242", "--watch", "--interval", "0.01",
                     "--max-minutes", "0.05", "--peak", peak, "--last", last])
                with open(peak) as fh:
                    self.assertEqual(json.load(fh)["count"], 2402)
                with open(last) as fh:
                    self.assertEqual(json.load(fh)["count"], 453)
        finally:
            gfp.snapshot = original
        self.assertEqual(rc, 0)
        self.assertIn("pinned host pid 4242 (holding 2402 descriptors)", err)
        self.assertIn("2 sample(s) over 1 process(es)", err)


if __name__ == "__main__":
    unittest.main()
