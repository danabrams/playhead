#!/usr/bin/env python3
"""playhead-9s1z — print the tables from tools/9s1z/out/t4-recompose.json.

What the SHIPPED composer produces over a device pull. The three-option comparison that
decided playhead-9s1z is a frozen record in out/t4-three-option-decision-record.json and
out/findings.md; it is not re-runnable from here — see README.md.
"""
import json
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
data = json.loads((HERE / "out" / "t4-recompose.json").read_text())

print("PULL:", data["pull"])
print(f"assets={data['assets']}  semantic_scan_results rows={data['semanticScanRows']}  "
      f"coarse containsAd presence rows={data['coarseContainsAdPresenceRows']}")
print(f"coarse x passB OVERLAP PAIRINGS: {data['coarseXpassBOverlapPairings_crossVersion']} of "
      f"{data['coarseXpassBOverlapPairings_total']} cross a transcriptVersion")
print("marks by number of transcriptVersions among the presence rows overlapping them:",
      data["markBackingVersionCountHistogram"])

per = data["perAsset"]
print(f"\n{'asset':10s} {'marks':>7s} {'seconds':>10s}")
for asset in sorted(per):
    print(f"{asset:10s} {per[asset]['marks']:7d} {per[asset]['seconds']:10.1f}")
print(f"{'TOTAL':10s} {data['marks']:7d} {data['seconds']:10.1f}")

val = data["validation"]
print(f"\nfidelity vs what the DEVICE wrote: {val['recomposedWithExactPersistedTwin']} of "
      f"{val['recomposedMarks']} recomposed marks have an exact persisted twin "
      f"({val['persistedSweepRows']} semantic-sweep-v1 rows on the pull).")
print("A GAP IS EXPECTED: the t4 device build predates playhead-shu5/my33/9s1z, so a recompose")
print("on today's geometry is supposed to differ. This number catches a BROKEN HARNESS, not a")
print("broken composer.")
