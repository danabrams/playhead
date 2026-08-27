#!/usr/bin/env python3
"""playhead-9s1z MEASUREMENT ONLY — print the tables from tools/9s1z/out/t4-recompose.json.

No option has been chosen and nothing in this directory changes shipped behaviour.
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
print("marks by number of transcriptVersions among the presence rows overlapping them:")
print("  stage 6 OFF (the bead's basis):", data["markBackingVersionCountHistogram_stage6Off"])
print("  stage 6 ON  (this tree):       ", data["markBackingVersionCountHistogram_stage6On"])

for cfg, label in [("stage6Off_theBeadsBasis", "STAGE 6 OFF — the basis playhead-kg6i's model measured on"),
                   ("stage6On_thisTree", "STAGE 6 ON — what the code in this tree does")]:
    c = data[cfg]
    print(f"\n{'=' * 78}\n{label}\n{'=' * 78}")
    per = {}
    for opt in ("i_shipped", "ii_sameVersion", "iii_sameVersionDropBoth"):
        for asset, v in c["options"][opt]["perAsset"].items():
            per.setdefault(asset, {})[opt] = v
    print(f"{'asset':10s} {'(i) m':>6s} {'(i) s':>9s} {'(ii) m':>7s} {'(ii) s':>9s} "
          f"{'(iii) m':>8s} {'(iii) s':>9s}")
    for asset in sorted(per):
        r = per[asset]
        print(f"{asset:10s} {r['i_shipped']['marks']:6d} {r['i_shipped']['seconds']:9.1f} "
              f"{r['ii_sameVersion']['marks']:7d} {r['ii_sameVersion']['seconds']:9.1f} "
              f"{r['iii_sameVersionDropBoth']['marks']:8d} {r['iii_sameVersionDropBoth']['seconds']:9.1f}")
    t = c["options"]
    print(f"{'TOTAL':10s} {t['i_shipped']['marks']:6d} {t['i_shipped']['seconds']:9.1f} "
          f"{t['ii_sameVersion']['marks']:7d} {t['ii_sameVersion']['seconds']:9.1f} "
          f"{t['iii_sameVersionDropBoth']['marks']:8d} {t['iii_sameVersionDropBoth']['seconds']:9.1f}")
    for opt in ("ii_sameVersion", "iii_sameVersionDropBoth"):
        v = c["diffVsShipped"][opt]
        print(f"\n  {opt} vs (i):")
        print(f"    marked seconds ADDED at an INNER edge : {v['addedSecondsInner']:8.1f}")
        print(f"    marked seconds ADDED at an OUTER edge : {v['addedSecondsOuter']:8.1f}")
        print(f"    marked seconds REMOVED               : {v['removedSeconds']:8.1f}")
        print(f"    net                                  : {v['netSeconds']:8.1f}")
        for run in v["addedRuns"]:
            print(f"      + {run['asset']} [{run['start']:.2f}-{run['end']:.2f}] "
                  f"{run['seconds']:.1f}s {run['edge'].upper()} "
                  f"(head {run['secondsFromEpisodeStart']:.0f}s / tail {run['secondsFromEpisodeEnd']:.0f}s)")
        for run in v["removedRuns"]:
            print(f"      - {run['asset']} [{run['start']:.2f}-{run['end']:.2f}] {run['seconds']:.1f}s")
        for m in v["marksWithNoIdenticalTwinInThisOption"]:
            print(f"      mark gone: {m['asset']} [{m['start']:.2f}-{m['end']:.2f}] {m['seconds']:.1f}s "
                  f"-> {m['secondsNoLongerUnderAnyMark']:.1f}s no longer under ANY mark")
        for m in v["marksThisOptionIntroduces"]:
            print(f"      mark new : {m['asset']} [{m['start']:.2f}-{m['end']:.2f}] {m['seconds']:.1f}s")
    val = c["validation"]
    print(f"\n  fidelity vs what the DEVICE wrote: {val['recomposedWithExactPersistedTwin']} of "
          f"{val['recomposedShippedMarks']} recomposed marks have an exact persisted twin "
          f"({val['persistedSweepRows']} semantic-sweep-v1 rows on the pull)")

print(f"\n{'=' * 78}\nREACHABILITY: all three options on synthetic rows, to prove (iii)'s")
print(f"suppressions are live rather than merely producing a zero\n{'=' * 78}")
for opt, v in data["reachabilitySelfTest"].items():
    marks = [f"{m['start']:.1f}-{m['end']:.1f}" for m in v["marks"]]
    print(f"  {opt:26s} marks={marks or '[]'}  suppressedCoarse={v['suppressedCoarseWindows']} "
          f"suppressedOrphans={v['suppressedOrphanedRefinements']}")

diverge = 0
for asset, t in data["stageTraces"].items():
    def ks(x):
        return sorted((round(e["start"], 6), round(e["end"], 6)) for e in x)
    if ks(t["i_shipped"]["presence"]) != ks(t["iii_sameVersionDropBoth"]["presence"]):
        diverge += 1
    for stage in ("merged", "afterClipAndWidth", "marks"):
        if ks(t["i_shipped"][stage]) != ks(t["iii_sameVersionDropBoth"][stage]):
            print(f"  !! (i) and (iii) DIFFER at {stage} on {asset}")
            sys.exit(1)
print(f"\n(iii) vs (i): diverges at STAGE 1-2 on {diverge} of {len(data['stageTraces'])} assets and")
print("re-converges at STAGE 3 (merge) on every one. Stage 6 is a pure function of the")
print("post-dedupe survivors, so the equality holds for ANY supportLines index, not just nil.")
