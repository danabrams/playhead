#!/usr/bin/env python3
"""Build a byte-alignment tier-a truth file (playhead-xsdz.36.1 / xsdz.44 GO).

For every rotated episode in the fpcalc truth that has a staged B-side
(TestFixtures/Corpus/Audio/<id>.fresh.mp3), run the l2f-mp3-forensics byte
aligner and emit a truth JSON in the exact schema l2f-bd4xqf-compare.py
consumes (episodes[].episodeId / rotated / adSlots[{startSeconds,endSeconds}]).

Slots are the aligner's differing regions in the A (played/snapshot) timeline.
adSlots keeps slots >= MIN_SLOT_SECONDS; the raw list is preserved per episode
as byteSlotsRaw with a droppedTinySlots count (no silent filtering).

Cross-check: where the fpcalc truth had slots (the 16 aligned episodes), report
per-episode agreement (slot count byte vs fpcalc) to stderr for eyeballing.
"""
import json, pathlib, subprocess, sys, tempfile, datetime

REPO = pathlib.Path("/Users/dabrams/playhead")
AUDIO = REPO / "TestFixtures/Corpus/Audio"
FORENSICS = REPO / "scripts/l2f-mp3-forensics.py"
FPCALC_TRUTH = REPO / "playhead-dogfood-diagnostics-tier-a-rediff.json"
OUT = REPO / "playhead-dogfood-diagnostics-tier-a-rediff-byte.json"
MIN_SLOT_SECONDS = 5.0

fp = json.load(FPCALC_TRUTH.open())
episodes_out, cross = [], []
started = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"

for rep in fp["episodes"]:
    ep = rep["episodeId"]
    a = AUDIO / f"{ep}.mp3"
    b = AUDIO / f"{ep}.fresh.mp3"
    if not rep.get("rotated"):
        episodes_out.append({"episodeId": ep, "rotated": False, "adSlots": [],
                             "ok": True, "note": "no rotation (fpcalc sha match)"})
        continue
    if not (a.exists() and b.exists()):
        episodes_out.append({"episodeId": ep, "rotated": True, "adSlots": [],
                             "ok": False, "error": "missing A or B audio"})
        continue
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
        tmp = pathlib.Path(tf.name)
    r = subprocess.run([sys.executable, str(FORENSICS), "align",
                        "--a", str(a), "--b", str(b), "--out", str(tmp)],
                       capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        episodes_out.append({"episodeId": ep, "rotated": True, "adSlots": [],
                             "ok": False, "error": f"align failed: {r.stderr[-200:]}"})
        tmp.unlink(missing_ok=True)
        continue
    al = json.load(tmp.open()); tmp.unlink(missing_ok=True)
    raw = al.get("slots") or []
    kept = [{"startSeconds": s["aStartSec"], "endSeconds": s["aEndSec"]}
            for s in raw if (s["aEndSec"] - s["aStartSec"]) >= MIN_SLOT_SECONDS]
    dropped = len(raw) - len(kept)
    ok = bool(al.get("monotonic_clean")) and al.get("runs_chained", 0) > 0
    rec = {"episodeId": ep, "rotated": True, "adSlots": kept if ok else [],
           "ok": ok,
           "byteAlign": {"runsFound": al.get("runs_found"),
                          "runsChained": al.get("runs_chained"),
                          "droppedNonMonotonic": al.get("runs_dropped_nonmonotonic"),
                          "monotonicClean": al.get("monotonic_clean"),
                          "chainedSeconds": al.get("chained_seconds"),
                          "aDuration": al.get("a_duration"),
                          "bDuration": al.get("b_duration"),
                          "droppedTinySlots": dropped},
           "byteSlotsRaw": raw}
    if not ok:
        rec["error"] = "byte alignment non-monotonic or zero runs"
    episodes_out.append(rec)
    fslots = rep.get("adSlots") or []
    if fslots and ok:
        cross.append((ep, len(kept), len(fslots)))
    print(f"  {ep}: byte slots={len(kept)} (raw {len(raw)}) ok={ok}"
          + (f" fpcalc={len(fslots)}" if fslots else ""), file=sys.stderr)

n_ok = sum(1 for e in episodes_out if e.get("rotated") and e.get("ok") and e.get("adSlots"))
n_fail = sum(1 for e in episodes_out if e.get("rotated") and not e.get("ok"))
out = {"schemaVersion": 1,
       "tool": "scratchpad/build-byte-truth.py (l2f-mp3-forensics align; xsdz.44 GO)",
       "startedIso": started,
       "completedIso": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
       "baseTruth": str(FPCALC_TRUTH.name),
       "minSlotSeconds": MIN_SLOT_SECONDS,
       "totals": {"episodes": len(episodes_out),
                   "rotatedOkWithSlots": n_ok,
                   "rotatedByteFailed": n_fail},
       "episodes": episodes_out}
OUT.write_text(json.dumps(out, indent=1))
print(f"BYTE_TRUTH_DONE ok_with_slots={n_ok} byte_failed={n_fail} -> {OUT.name}", file=sys.stderr)
print("cross-check (byte vs fpcalc slot counts):", file=sys.stderr)
for ep, bl, fl in cross:
    flag = "" if bl == fl else "  <-- DIFFERS"
    print(f"  {ep}: byte={bl} fpcalc={fl}{flag}", file=sys.stderr)
