"""Tests for the playhead-xsdz.38 joint candidate-pair refinement mode of
scripts/l2f-boundary-stinger-prototype.py (`refine_edges_joint`, `--joint`).
Follows the test_l2f_stinger_bank.py conventions (importlib loading of
hyphenated modules, in-code fixture builders, stdlib unittest).

Two synthetic worlds:
  - PlantedEnvelopes: plants template patterns at exact NCC cosine values
    (signal = cos*u + sin*v around the centered/normalized template), so
    every peak height in a test is chosen, not approximated;
  - FakeEnvelopes: the bank-test world (identical stinger at every break
    edge) for end-to-end CLI runs through learn_show_model.
"""

import importlib.util
import json
import math
import pathlib
import sys
import tempfile
import unittest

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROTO = _load("l2f_boundary_stinger_prototype", "l2f-boundary-stinger-prototype.py")

HZ = PROTO.ENVELOPE_HZ
TIEBREAK = PROTO.JOINT_TIEBREAK_MOVE_RATE


def make_pattern(seed, seconds=2.0):
    rng = np.random.RandomState(seed)
    return rng.standard_normal(int(seconds * HZ)) * 0.5 + 2.0


def make_entry(template, confidence):
    return {
        "template": np.asarray(template, dtype=np.float64),
        "edge_sample": 0,
        "offset": 0.0,
        "confidence": confidence,
        "support": 3,
        "spread": 0.0,
    }


class PlantedEnvelopes:
    """Deterministic envelope world; `plant` writes a signal whose NCC
    against `template` at the plant position is EXACTLY `cosine`."""

    sample_rate = 8000

    def __init__(self, duration_s, seed=99):
        rng = np.random.RandomState(seed)
        self.duration = duration_s
        self.frames = rng.standard_normal(int(duration_s * HZ)) * 0.02 + 1.0

    def plant(self, template, at_seconds, cosine):
        t = np.asarray(template, dtype=np.float64)
        t = t - t.mean()
        u = t / np.linalg.norm(t)
        rng = np.random.RandomState(int(round(at_seconds * 100)) % (2**31))
        n = rng.standard_normal(t.size)
        n = n - n.mean()
        n = n - (n @ u) * u
        n = n / np.linalg.norm(n)
        signal = cosine * u + math.sqrt(1.0 - cosine**2) * n + 2.0
        i = int(round(at_seconds * HZ))
        self.frames[i:i + signal.size] = signal

    def get(self, episode_id, start_s, end_s):
        start_s = max(0.0, start_s)
        i = int(round(start_s * HZ))
        j = min(int(round(end_s * HZ)), self.frames.size)
        return self.frames[i:j].astype(np.float64)


class NccQualifyingMaximaTests(unittest.TestCase):
    def test_gate_and_plateau(self):
        curve = np.array([0.1, 0.6, 0.55, 0.7, 0.7, 0.4])
        self.assertEqual(
            PROTO.ncc_qualifying_maxima(curve, 0.5), [(1, 0.6), (4, 0.7)],
            "plateaus report their right edge",
        )
        self.assertEqual(PROTO.ncc_qualifying_maxima(curve, 0.65), [(4, 0.7)])

    def test_endpoints_qualify(self):
        self.assertEqual(
            PROTO.ncc_qualifying_maxima(np.array([0.8, 0.2]), 0.5), [(0, 0.8)]
        )
        self.assertEqual(
            PROTO.ncc_qualifying_maxima(np.array([0.2, 0.8]), 0.5), [(1, 0.8)]
        )


class JointPeakCandidateTests(unittest.TestCase):
    def test_candidates_honor_gate_and_move_cap(self):
        world = PlantedEnvelopes(400.0)
        pattern = make_pattern(1)
        world.plant(pattern, 100.0, 1.0)
        world.plant(pattern, 130.0, 0.9)
        world.plant(pattern, 190.0, 0.95)  # move +70 from center: within cap
        world.plant(pattern, 199.0, 0.95)  # move +79: beyond the 75s cap
        entry = make_entry(pattern, 0.9)  # gate max(0.5, 0.75) = 0.75
        candidates = PROTO.joint_peak_candidates(entry, world, "ep", 120.0, 400.0)
        times = sorted(round(c["time"], 1) for c in candidates)
        self.assertEqual(times, [100.0, 130.0, 190.0])
        by_time = {round(c["time"], 1): c["peak"] for c in candidates}
        self.assertAlmostEqual(by_time[100.0], 1.0, places=3)
        self.assertAlmostEqual(by_time[130.0], 0.9, places=3)
        self.assertTrue(all(c["kind"] == "peak" for c in candidates))


class JointRefineBase(unittest.TestCase):
    """Shared world builders. All scoring arithmetic in comments uses the
    exact planted cosines."""

    def refine(self, world, model, proposal, config, duration=None):
        return PROTO.refine_edges_joint(
            proposal, model, world,
            "ep", duration or world.duration, config,
        )


class ParityTests(JointRefineBase):
    def test_single_peak_no_grid_matches_shipped_recipe(self):
        world = PlantedEnvelopes(400.0)
        pre, post = make_pattern(2), make_pattern(3)
        world.plant(pre, 95.0, 0.9)
        world.plant(post, 155.0, 0.9)
        model = {"pre": make_entry(pre, 0.9), "post": make_entry(post, 0.9),
                 "grid": None}
        proposal = {"start": 100.0, "end": 150.0}
        js, je, jt = self.refine(world, model, proposal, PROTO.JointConfig())
        rs, re, rt = PROTO.refine_edges(proposal, model, world, "ep", 400.0)
        self.assertAlmostEqual(js, rs, places=9)
        self.assertAlmostEqual(je, re, places=9)
        self.assertEqual(
            (jt["start_snapped"], jt["end_snapped"]),
            (rt["start_snapped"], rt["end_snapped"]),
        )
        self.assertAlmostEqual(js, 95.0, delta=0.05)
        self.assertAlmostEqual(je, 155.0, delta=0.05)


class ProductPenaltyTests(JointRefineBase):
    """grid_inconsistency_rate * grid_distance * widening / move-cap.

    Peaks are planted at 0.6 — below derived_anchor_min_peak, like the
    live 0.525 defect instance — so no derived partner can rescue the pair
    and the penalty is exercised in isolation."""

    def _world(self, plant_at, cosine):
        world = PlantedEnvelopes(600.0)
        post = make_pattern(4)
        world.plant(post, plant_at, cosine)
        model = {"pre": None, "post": make_entry(post, 0.6), "grid": 30.0}
        return world, model

    def test_far_offgrid_widening_rejected(self):
        # width 76.5 -> distance 16.5 from 2*30; widening 66.5:
        # 0.6 - 0.08*16.5*66.5/75 = 0.6 - 1.17 < 0 -> no-snap wins.
        world, model = self._world(126.5, 0.6)
        s, e, t = self.refine(world, model, {"start": 50.0, "end": 60.0},
                              PROTO.JointConfig())
        self.assertEqual((s, e), (50.0, 60.0))
        self.assertFalse(t["end_snapped"])

    def test_far_offgrid_widening_kept_when_disabled(self):
        world, model = self._world(126.5, 0.6)
        s, e, t = self.refine(world, model, {"start": 50.0, "end": 60.0},
                              PROTO.JointConfig(grid_inconsistency_rate=0.0))
        self.assertAlmostEqual(e, 126.5, delta=0.05)
        self.assertTrue(t["end_snapped"])

    def test_near_grid_widening_survives(self):
        # width 64 -> distance 4; widening 54: 0.6 - 0.08*4*54/75 = 0.37 > 0.
        world, model = self._world(114.0, 0.6)
        s, e, t = self.refine(world, model, {"start": 50.0, "end": 60.0},
                              PROTO.JointConfig())
        self.assertAlmostEqual(e, 114.0, delta=0.05)
        self.assertTrue(t["end_snapped"])

    def test_sliver_small_widening_survives(self):
        # width 13.1 -> distance 16.9 from 1*30, but widening only 1.1:
        # 0.6 - 0.08*16.9*1.1/75 = 0.58 > 0 (the correct +1.16s-style snap).
        world = PlantedEnvelopes(4000.0)
        post = make_pattern(5)
        world.plant(post, 2843.1, 0.6)
        model = {"pre": None, "post": make_entry(post, 0.6), "grid": 30.0}
        s, e, t = self.refine(world, model, {"start": 2830.0, "end": 2842.0},
                              PROTO.JointConfig())
        self.assertAlmostEqual(e, 2843.1, delta=0.05)
        self.assertTrue(t["end_snapped"])


class MoveRatePenaltyTests(JointRefineBase):
    def test_rate_times_move_semantics(self):
        world = PlantedEnvelopes(600.0)
        # 0.6 sits below the derived-anchor gate: no derived partner can
        # turn this lone off-grid snap into an on-grid pair.
        post = make_pattern(6)
        world.plant(post, 100.4, 0.6)  # off-grid width 50.4, move 40.4
        model = {"pre": None, "post": make_entry(post, 0.6), "grid": 30.0}
        proposal = {"start": 50.0, "end": 60.0}
        # 0.6 - 0.02*40.4 < 0 -> rejected
        s, e, _ = self.refine(world, model, proposal, PROTO.JointConfig(
            grid_inconsistency_rate=0.0, offgrid_move_rate=0.02))
        self.assertEqual(e, 60.0)
        # 0.6 - 0.01*40.4 = 0.196 > 0 -> kept
        s, e, _ = self.refine(world, model, proposal, PROTO.JointConfig(
            grid_inconsistency_rate=0.0, offgrid_move_rate=0.01))
        self.assertAlmostEqual(e, 100.4, delta=0.05)


class GridBonusTests(JointRefineBase):
    def _world(self):
        world = PlantedEnvelopes(600.0)
        pre, post = make_pattern(7), make_pattern(8)
        world.plant(pre, 95.0, 0.8)
        world.plant(post, 143.0, 0.95)   # width 48 with the pre peak: off-grid
        world.plant(post, 155.2, 0.9)    # width 60.2: on-grid
        model = {"pre": make_entry(pre, 0.6), "post": make_entry(post, 0.6),
                 "grid": 30.0}
        return world, model

    def test_bonus_prefers_grid_consistent_pair(self):
        world, model = self._world()
        s, e, t = self.refine(world, model, {"start": 100.0, "end": 130.0},
                              PROTO.JointConfig())
        self.assertAlmostEqual(s, 95.0, delta=0.05)
        self.assertAlmostEqual(e, 155.2, delta=0.05)

    def test_without_grid_terms_stronger_peak_wins(self):
        world, model = self._world()
        s, e, t = self.refine(
            world, model, {"start": 100.0, "end": 130.0},
            PROTO.JointConfig(grid_bonus=0.0, grid_inconsistency_rate=0.0),
        )
        self.assertAlmostEqual(e, 143.0, delta=0.05)

    def test_scope_any_extends_bonus_to_no_snap_partner(self):
        world = PlantedEnvelopes(600.0)
        post = make_pattern(9)
        world.plant(post, 160.3, 0.6)   # on-grid with the UNSNAPPED start
        world.plant(post, 149.0, 0.85)  # stronger, off-grid (dist 11)
        model = {"pre": None, "post": make_entry(post, 0.6), "grid": 30.0}
        proposal = {"start": 100.0, "end": 130.0}
        # Derived candidates are disabled to isolate the scope term (the
        # 0.85 peak would otherwise anchor an on-grid derived start).
        # both: no bonus for (no-snap, peak); 0.85 - 0.08*11*19/75 = 0.63
        # beats 0.6.
        s, e, _ = self.refine(world, model, proposal,
                              PROTO.JointConfig(derived_candidates=False))
        self.assertAlmostEqual(e, 149.0, delta=0.05)
        # any: 0.6 + 0.5 beats it.
        s, e, _ = self.refine(
            world, model, proposal,
            PROTO.JointConfig(grid_bonus_scope="any", derived_candidates=False),
        )
        self.assertAlmostEqual(e, 160.3, delta=0.05)


class DerivedCandidateTests(JointRefineBase):
    def _world(self, cosine):
        world = PlantedEnvelopes(600.0)
        pre = make_pattern(10)
        world.plant(pre, 100.0, cosine)
        model = {"pre": make_entry(pre, 0.6), "post": None, "grid": 30.0}
        return world, model

    def test_confident_anchor_derives_nearest_grid_end(self):
        world, model = self._world(0.9)
        s, e, t = self.refine(world, model, {"start": 105.0, "end": 120.0},
                              PROTO.JointConfig())
        self.assertAlmostEqual(s, 100.0, delta=0.05)
        self.assertAlmostEqual(e, 130.0, delta=0.06)
        self.assertEqual(t["end_kind"], "derived")
        self.assertTrue(t["grid_applied"])

    def test_weak_anchor_derives_nothing(self):
        world, model = self._world(0.6)  # below derived_anchor_min_peak 0.65
        s, e, t = self.refine(world, model, {"start": 105.0, "end": 120.0},
                              PROTO.JointConfig())
        self.assertAlmostEqual(s, 100.0, delta=0.05)
        self.assertEqual(e, 120.0)
        self.assertFalse(t["grid_applied"])

    def test_max_multiple_caps_derivation(self):
        world, model = self._world(0.9)
        model["grid_max_multiple"] = 1
        candidates = PROTO._joint_derived_candidates(
            [{"time": 100.0, "peak": 0.9, "kind": "peak"}],
            30.0, 1, +1, 120.0, 600.0, 0.65,
        )
        self.assertEqual([c["time"] for c in candidates], [130.0])


class GridMaxMultipleTests(JointRefineBase):
    """The show's largest observed pod multiple stops the bonus from
    stitching a neighboring break's stinger into a super-window (the
    morbid-05-29 break-4 gobble found in the sweep)."""

    def _world(self):
        world = PlantedEnvelopes(6000.0)
        pre, post = make_pattern(11), make_pattern(12)
        world.plant(pre, 4036.0, 0.9)
        world.plant(post, 4153.3, 0.6)  # a NEIGHBOR break's stinger
        model = {"pre": make_entry(pre, 0.6), "post": make_entry(post, 0.6),
                 "grid": 30.0}
        return world, model

    def test_uncapped_bonus_gobbles(self):
        world, model = self._world()
        s, e, _ = self.refine(
            world, model, {"start": 4076.0, "end": 4096.4},
            PROTO.JointConfig(derived_candidates=False),
        )
        self.assertAlmostEqual(e, 4153.3, delta=0.05,
                               msg="width 117.3 ~= 4*30 earns the bonus without a cap")

    def test_capped_multiple_rejects_gobble(self):
        world, model = self._world()
        model["grid_max_multiple"] = 3
        s, e, t = self.refine(
            world, model, {"start": 4076.0, "end": 4096.4},
            PROTO.JointConfig(derived_candidates=False),
        )
        self.assertAlmostEqual(s, 4036.0, delta=0.05)
        self.assertEqual(e, 4096.4, "end must stay on the proposal")
        self.assertFalse(t["end_snapped"])

    def test_learner_records_max_multiple(self):
        breaks = [
            {"episode_id": "s-2026-01-01-e", "start_seconds": a, "end_seconds": b}
            for a, b in ((100.0, 130.5), (500.0, 589.5), (900.0, 960.0))
        ]

        class NoAudio:
            def get(self, *_):
                return np.zeros(1)

        model = PROTO.learn_show_model(breaks, NoAudio(), None)
        self.assertEqual(model["grid"], 30.0)
        self.assertEqual(model["grid_max_multiple"], 3)


class StructuralAnchorTests(JointRefineBase):
    def _world(self):
        world = PlantedEnvelopes(400.0)
        post = make_pattern(13)
        world.plant(post, 60.5, 0.9)
        model = {"pre": None, "post": make_entry(post, 0.6), "grid": 30.0}
        return world, model

    def test_episode_start_anchor_completes_grid_pair(self):
        # Derived candidates are disabled to isolate the structural term
        # (the 0.9 end peak would otherwise anchor a derived start whose
        # smaller movement out-tiebreaks the anchor at 0.0).
        world, model = self._world()
        s, e, t = self.refine(
            world, model, {"start": 20.0, "end": 40.0},
            PROTO.JointConfig(structural_anchors=True, derived_candidates=False),
        )
        self.assertEqual(s, 0.0)
        self.assertAlmostEqual(e, 60.5, delta=0.05)
        self.assertEqual(t["start_kind"], "structural")

    def test_disabled_by_default(self):
        world, model = self._world()
        s, e, t = self.refine(
            world, model, {"start": 20.0, "end": 40.0},
            PROTO.JointConfig(derived_candidates=False),
        )
        self.assertEqual(s, 20.0)
        self.assertAlmostEqual(e, 60.5, delta=0.05)


class PeakMarginTests(JointRefineBase):
    def _world(self):
        world = PlantedEnvelopes(400.0)
        pre = make_pattern(14)
        world.plant(pre, 95.0, 0.97)
        world.plant(pre, 118.0, 0.96)
        model = {"pre": make_entry(pre, 0.9), "post": None, "grid": None}
        return world, model

    def test_zero_margin_keeps_argmax(self):
        world, model = self._world()
        s, _, _ = self.refine(world, model, {"start": 120.0, "end": 150.0},
                              PROTO.JointConfig())
        self.assertAlmostEqual(s, 95.0, delta=0.05)

    def test_margin_flattens_near_equals_toward_proposal(self):
        world, model = self._world()
        s, _, _ = self.refine(world, model, {"start": 120.0, "end": 150.0},
                              PROTO.JointConfig(peak_margin=0.02))
        self.assertAlmostEqual(s, 118.0, delta=0.05)


class FeasibilityTests(JointRefineBase):
    def test_no_candidates_returns_proposal(self):
        world = PlantedEnvelopes(400.0)
        model = {"pre": make_entry(make_pattern(15), 0.9),
                 "post": make_entry(make_pattern(16), 0.9), "grid": 30.0}
        proposal = {"start": 100.0, "end": 150.0}
        s, e, t = self.refine(world, model, proposal, PROTO.JointConfig())
        self.assertEqual((s, e), (100.0, 150.0))
        self.assertTrue(t["joint"])
        self.assertFalse(t["start_snapped"] or t["end_snapped"] or t["grid_applied"])

    def test_candidate_before_start_is_infeasible(self):
        world = PlantedEnvelopes(400.0)
        post = make_pattern(17)
        world.plant(post, 90.0, 0.9)
        model = {"pre": None, "post": make_entry(post, 0.6), "grid": None}
        proposal = {"start": 100.0, "end": 150.0}
        s, e, t = self.refine(world, model, proposal, PROTO.JointConfig())
        self.assertEqual((s, e), (100.0, 150.0))
        # invariant: the refined window always overlaps the proposal
        self.assertGreater(min(e, 150.0) - max(s, 100.0), 0)

    def test_determinism(self):
        world = PlantedEnvelopes(600.0)
        pre, post = make_pattern(18), make_pattern(19)
        world.plant(pre, 95.0, 0.8)
        world.plant(post, 143.0, 0.95)
        world.plant(post, 155.2, 0.9)
        model = {"pre": make_entry(pre, 0.6), "post": make_entry(post, 0.6),
                 "grid": 30.0}
        proposal = {"start": 100.0, "end": 130.0}
        first = self.refine(world, model, proposal, PROTO.JointConfig())
        second = self.refine(world, model, proposal, PROTO.JointConfig())
        self.assertEqual(first, second)


class FakeEnvelopes:
    """The bank-test world: identical stinger audio at every break edge."""

    sample_rate = 8000

    def __init__(self, evaluation):
        self.pre_edges = {}
        self.post_edges = {}
        for asset in evaluation["assets"]:
            eid = asset["episode_id"]
            self.pre_edges[eid] = [b["start_seconds"] for b in asset["full_breaks"]]
            self.post_edges[eid] = [b["end_seconds"] for b in asset["full_breaks"]]

    def _value(self, episode_id, t):
        for edge in self.pre_edges[episode_id]:
            u = t - edge
            if -4.0 <= u < 0.5:
                return 1.5 + ((u * 7.3) % 1.0)
        for edge in self.post_edges[episode_id]:
            u = t - edge
            if -0.5 <= u < 4.0:
                return 2.5 + ((u * 4.7) % 0.8)
        return 0.1

    def get(self, episode_id, start_s, end_s):
        start_s = max(0.0, start_s)
        frames = int(round((end_s - start_s) * HZ))
        times = start_s + (np.arange(frames) + 0.5) / HZ
        return np.array(
            [self._value(episode_id, t) for t in times], dtype=np.float64
        )


def _break(start, end):
    return {
        "start_seconds": start,
        "end_seconds": end,
        "boundary_tolerance_seconds": 0.3,
        "source_ledger_ids": [],
        "source_review_ids": [],
    }


class JointCLITests(unittest.TestCase):
    """End-to-end --joint invocation through main() with a patched
    envelope extractor (no ffmpeg)."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)

        self.evaluation = {
            "artifact_kind": "oracle_earaudit_gold_boundary_evaluation",
            "schema_version": 1,
            "assets": [
                {
                    "episode_id": "show-a-2026-01-01-ep1",
                    "show_name": "Show A",
                    "duration_seconds": 3000.0,
                    "full_breaks": [
                        _break(500.0, 545.0),
                        _break(1000.0, 1045.0),
                        _break(1500.0, 1545.0),
                    ],
                    "presence_anchors": [],
                    "content_vetoes": [],
                },
                {
                    "episode_id": "show-a-2026-01-08-ep2",
                    "show_name": "Show A",
                    "duration_seconds": 3000.0,
                    "full_breaks": [_break(700.0, 745.0)],
                    "presence_anchors": [],
                    "content_vetoes": [],
                },
            ],
            "summary": {},
            "label_semantics": {},
            "sources": {},
        }
        self.evaluation_path = self.tmp / "gold.json"
        self.evaluation_path.write_text(json.dumps(self.evaluation), encoding="utf-8")

        dump = {
            "episodes": [
                {
                    "episodeId": "show-a-2026-01-01-ep1",
                    "adWindows": [
                        {"startTime": 510.0, "endTime": 540.0},
                        {"startTime": 1005.0, "endTime": 1050.0},
                        {"startTime": 1495.0, "endTime": 1552.0},
                    ],
                },
                {
                    "episodeId": "show-a-2026-01-08-ep2",
                    "adWindows": [{"startTime": 703.0, "endTime": 748.0}],
                },
            ]
        }
        self.dump_path = self.tmp / "dump.json"
        self.dump_path.write_text(json.dumps(dump), encoding="utf-8")

        self.audio_dir = self.tmp / "audio"
        self.audio_dir.mkdir()
        for asset in self.evaluation["assets"]:
            (self.audio_dir / f"{asset['episode_id']}.mp3").write_bytes(b"")

        fake = FakeEnvelopes(self.evaluation)

        class PatchedCache(PROTO.EnvelopeCache):
            def _extract(cache_self, episode_id, start_s, end_s):
                return fake.get(episode_id, start_s, end_s)

        self._original_cache = PROTO.EnvelopeCache
        PROTO.EnvelopeCache = PatchedCache
        self.addCleanup(setattr, PROTO, "EnvelopeCache", self._original_cache)

    def _run(self, extra=()):
        report_path = self.tmp / "report.json"
        rc = PROTO.main([
            "--evaluation", str(self.evaluation_path),
            "--dump", str(self.dump_path),
            "--audio-dir", str(self.audio_dir),
            "--report", str(report_path),
            *extra,
        ])
        self.assertEqual(rc, 0)
        return json.loads(report_path.read_text(encoding="utf-8"))

    def test_joint_flag_runs_joint_recipe(self):
        report = self._run(extra=["--joint"])
        self.assertIn("joint candidate-pair", report["recipe"])
        self.assertTrue(all(t.get("joint") for t in report["traces"]))
        self.assertGreaterEqual(report["breaks_snapped"], 3)
        self.assertGreaterEqual(
            report["after"]["within_tolerance"],
            report["before"]["within_tolerance"],
        )

    def test_default_path_is_the_shipped_recipe(self):
        report = self._run()
        self.assertIn("per-show stinger anchors", report["recipe"])
        self.assertNotIn("joint", report["recipe"])
        self.assertTrue(all("joint" not in t for t in report["traces"]))

    def test_joint_term_flags_are_recorded_in_recipe(self):
        report = self._run(extra=["--joint", "--joint-grid-bonus", "0.25",
                                  "--joint-structural-anchors"])
        self.assertIn("'grid_bonus': 0.25", report["recipe"])
        self.assertIn("'structural_anchors': True", report["recipe"])


if __name__ == "__main__":
    sys.exit(unittest.main())
