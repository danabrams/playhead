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

import importlib.util
import json
import os
import pathlib
import socket
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
        pid = os.getpid()
        snap = gfp.snapshot(pid)
        self.assertGreaterEqual(snap["max_fd"], snap["count"] - 1)
        self.assertEqual(snap["max_fd"], max(r["fd"] for r in snap["rows"]))


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

    def test_no_reader_can_see_a_half_written_dump(self):
        with tempfile.TemporaryDirectory(prefix="vk68m-atomic-") as tmp:
            target = os.path.join(tmp, "peak.json")
            gfp._atomic_json(target, {"count": 2539})
            self.assertFalse(os.path.exists(target + ".partial"))
            with open(target) as fh:
                self.assertEqual(json.load(fh)["count"], 2539)


class HostDiscoveryRails(unittest.TestCase):

    def test_discovery_returns_a_pid_or_zero_and_never_raises(self):
        found = gfp.find_test_host()
        self.assertIsInstance(found, int)
        self.assertGreaterEqual(found, 0)


if __name__ == "__main__":
    unittest.main()
