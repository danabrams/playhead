"""Tests for the cohort readout (scripts/cohort_readout.py), playhead-i7kvl.3.

The readout produces the one number the launch window is organised around:
manual skip-forward reaches per listening hour. Every case here is a way that
number could come out wrong and look right.

Conventions follow test_gate_memory_verdict.py: importlib module loading,
in-code fixture builders, stdlib unittest, nothing touching a real bundle.
"""

import importlib.util
import json
import pathlib
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


cr = _load("cohort_readout", "cohort_readout.py")


def bundle(*, recorded=True, reaches=None, seconds=None, shown=None,
           confirmed=None, denied=None, events=None, census=None):
    by_metric = {}
    def put(key, value, cohort="all"):
        if value is not None:
            by_metric[key] = {cohort: value}
    put(cr.REACHES, reaches)
    put(cr.LISTENING_SECONDS, seconds)
    put(cr.BANNERS_SHOWN, shown)
    put(cr.BANNERS_CONFIRMED, confirmed)
    put(cr.BANNERS_DENIED, denied)
    default = {
        "analytics_counters": {"by_metric": by_metric, "recorded": recorded},
        "scheduler_events": events or [],
    }
    if census is not None:
        default["scheduler_event_census"] = census
    return {"generated_at": 1, "default": default}


class CohortReadoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = pathlib.Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def write(self, name, payload):
        path = self.dir / name
        path.write_text(json.dumps(payload), encoding="utf-8")
        return path

    # --- the north star ---------------------------------------------------

    def test_the_north_star_is_computed(self):
        path = self.write("a.json", bundle(reaches=12, seconds=7200))
        reading = cr.read_bundle(path)
        self.assertEqual(reading.count(cr.REACHES), 12)
        self.assertAlmostEqual(reading.listening_hours, 2.0)
        self.assertAlmostEqual(reading.reaches_per_hour, 6.0)

    def test_a_missing_counter_reads_not_recorded_and_never_zero(self):
        """THE RULE. A fabricated zero in the numerator of the north-star
        metric is exactly the defect this repo finds most often."""
        path = self.write("legacy.json", bundle(recorded=False))
        reading = cr.read_bundle(path)
        self.assertIsNone(reading.count(cr.REACHES))
        self.assertIsNone(reading.count(cr.LISTENING_SECONDS))
        self.assertIsNone(reading.reaches_per_hour)
        self.assertIn(cr.NOT_RECORDED, cr.render([reading]))

    def test_a_bundle_predating_the_field_is_not_recorded(self):
        payload = bundle(reaches=5, seconds=3600)
        del payload["default"]["analytics_counters"]
        reading = cr.read_bundle(self.write("old.json", payload))
        self.assertFalse(reading.recorded)
        self.assertIsNone(reading.reaches_per_hour)

    def test_a_recorded_zero_is_a_measurement(self):
        """`recorded: true` with no reaches means the listener reached zero
        times — a real and good result, distinct from nobody counting."""
        reading = cr.read_bundle(self.write("z.json", bundle(seconds=3600)))
        self.assertEqual(reading.count(cr.REACHES), 0)
        self.assertAlmostEqual(reading.reaches_per_hour, 0.0)

    def test_zero_denominator_is_not_a_rate(self):
        reading = cr.read_bundle(self.write("nd.json", bundle(reaches=4, seconds=0)))
        self.assertEqual(reading.count(cr.REACHES), 4)
        self.assertIsNone(reading.reaches_per_hour, "4 reaches over no listening is not a rate")
        self.assertIn("—", cr.render([reading]))

    # --- populations ------------------------------------------------------

    def test_cohorts_are_summed_into_the_population(self):
        payload = bundle(reaches=1, seconds=1800)
        payload["default"]["analytics_counters"]["by_metric"][cr.REACHES] = {
            "under30m": 3, "between30and60m": 4
        }
        reading = cr.read_bundle(self.write("c.json", payload))
        self.assertEqual(reading.count(cr.REACHES), 7)

    def test_unrecorded_bundles_are_excluded_from_the_cohort_total(self):
        """A bundle that counted nothing must not dilute the cohort rate, and
        the output states the population it used."""
        good = cr.read_bundle(self.write("g.json", bundle(reaches=10, seconds=3600)))
        blank = cr.read_bundle(self.write("b.json", bundle(recorded=False)))
        out = cr.render([good, blank])
        self.assertIn("1 of 2 bundle(s) carried counters", out)
        self.assertIn("COHORT reaches/hour = 10 / 1.0 h = 10.00", out)

    def test_auto_skips_come_from_events_not_counters(self):
        events = [
            {"event_type": "auto_skip_fired"},
            {"event_type": "auto_skip_fired"},
            {"event_type": "ready_entered"},
        ]
        reading = cr.read_bundle(self.write("e.json", bundle(reaches=1, seconds=60, events=events)))
        self.assertEqual(reading.auto_skips, 2)

    # --- degradation ------------------------------------------------------

    def test_an_unreadable_bundle_is_named_not_skipped(self):
        path = self.dir / "broken.json"
        path.write_text("{not json", encoding="utf-8")
        reading = cr.read_bundle(path)
        self.assertIsNotNone(reading.error)
        self.assertIn("broken.json", cr.render([reading]))

    def test_a_non_playhead_json_is_named(self):
        reading = cr.read_bundle(self.write("other.json", {"hello": "world"}))
        self.assertIn("default", reading.error)

    def test_a_saturated_tail_is_noted_without_affecting_counts(self):
        reading = cr.read_bundle(self.write("s.json", bundle(
            reaches=6, seconds=3600,
            census={"total": 500, "exported": 200, "truncated": True},
        )))
        self.assertAlmostEqual(reading.reaches_per_hour, 6.0)
        out = cr.render([reading])
        self.assertIn("saturated", out)
        self.assertIn("unaffected", out)

    def test_render_states_both_terms_of_the_rate(self):
        out = cr.render([cr.read_bundle(self.write("h.json", bundle(reaches=1, seconds=3600)))])
        self.assertIn("numerator", out)
        self.assertIn("denominator", out)

    def test_main_runs_end_to_end(self):
        self.write("a.json", bundle(reaches=3, seconds=3600))
        self.write("b.json", bundle(recorded=False))
        self.assertEqual(cr.main([str(self.dir)]), 0)
        self.assertEqual(cr.main([str(self.dir), "--json"]), 0)
        self.assertEqual(cr.main([str(self.dir / "nope")]), 2)


if __name__ == "__main__":
    unittest.main()
