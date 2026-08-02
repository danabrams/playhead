#!/usr/bin/env python3
"""playhead-6qvf STEP 1: how often does the lagged rediff byte differ fall back to chroma?

Drives the reference aligner (scripts/l2f-mp3-forensics.py, the file
RediffByteAligner.swift is a pinned port of) over every real A/B pair in
TestFixtures/Corpus/Audio and applies RediffSlotOwnership.gateAndDiffBytes's
gates verbatim (Configuration.default: minAlignedFractionB 0.5, minRunBytes
65536, recoverNonMonotonicSegments = false -- the LAGGED path's value).
"""
import importlib.util, json, pathlib, sys, time

REPO = pathlib.Path("/Users/dabrams/playhead/.worktrees/6qvf")
AUDIO = pathlib.Path("/Users/dabrams/playhead/TestFixtures/Corpus/Audio")
spec = importlib.util.spec_from_file_location("f", REPO / "scripts" / "l2f-mp3-forensics.py")
f = importlib.util.module_from_spec(spec); spec.loader.exec_module(f)

MIN_RUN_BYTES = 65536
MIN_ALIGNED_FRACTION_B = 0.5

pairs = []
for b in sorted(AUDIO.glob("*.fresh.mp3")):
    a = AUDIO / (b.name[:-len(".fresh.mp3")] + ".mp3")
    if a.exists():
        pairs.append((a, b))

rows = []
for i, (a, b) in enumerate(pairs, 1):
    t0 = time.time()
    rec = {"pair": a.stem}
    try:
        pa = f.parse_mp3(str(a)); pb = f.parse_mp3(str(b))
        rec["aFrames"] = pa["stats"]["frames"]
        rec["bFrames"] = pb["stats"]["frames"]
    except Exception as e:
        rec["parseError"] = repr(e)
        rows.append(rec); print(json.dumps(rec), flush=True); continue
    try:
        runs, pa, pb = f.byte_runs(str(a), str(b), MIN_RUN_BYTES)
        chain, total, dropped = f.chain_runs(runs)
        b_audio = pb["size"] - pb["leading_id3_bytes"]
        frac = (total / b_audio) if b_audio else 0.0
        rec.update({
            "runsFound": len(runs), "runsChained": len(chain),
            "droppedNonMonotonic": dropped, "monotonicClean": dropped == 0,
            "chainedBytes": total, "bAudioBytes": b_audio,
            "chainedFractionB": round(frac, 4),
        })
        if not chain:
            rec["gate"] = "rejectedNoChainedRuns"
        elif dropped != 0:
            rec["gate"] = "rejectedNonMonotonic"
        elif frac < MIN_ALIGNED_FRACTION_B:
            rec["gate"] = "rejectedLowChainedFraction"
        else:
            rec["gate"] = "accepted"
    except Exception as e:
        rec["alignError"] = repr(e); rec["gate"] = "error"
    rec["secs"] = round(time.time() - t0, 1)
    rows.append(rec)
    print(f"[{i}/{len(pairs)}] " + json.dumps(rec), flush=True)

out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/6qvf.json")
out.write_text(json.dumps(rows, indent=1))
from collections import Counter
c = Counter(r.get("gate", "parseError") for r in rows)
print("\n=== SUMMARY ===", flush=True)
print(f"pairs: {len(rows)}")
for k, v in sorted(c.items()):
    print(f"  {k}: {v}")
accepted = c.get("accepted", 0)
print(f"BYTE PATH ACCEPTED: {accepted}/{len(rows)}")
print(f"WOULD FALL BACK TO CHROMA: {len(rows)-accepted}/{len(rows)}")
