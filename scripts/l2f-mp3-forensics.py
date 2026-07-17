#!/usr/bin/env python3
"""
l2f-mp3-forensics.py — MP3 byte/frame forensics for the byte-substrate
width-detection kill tests (playhead-xsdz.43 / .44 / .51).

ONE mp3-frame parser, THREE offline questions:

  parse   Walk an MP3 robustly: skip leading ID3v2, parse frame headers
          (version/layer/bitrate/samplerate/padding -> frame length), extract
          side-info main_data_begin per frame (MPEG1 L3: 9 bits at the start
          of side info, which begins 4 bytes after the header, +2 with CRC),
          and detect structural anomalies: mid-file Xing/Info/LAME/VBRI tag
          frames, embedded mid-stream ID3v2 blocks, sync losses/resyncs,
          bitrate/samplerate/mode changes. Emits frame index -> byte offset
          -> time (cumulative samples/samplerate) plus an anomaly list.

  scars   xsdz.43: over corpus A-sides that have gold labels, measure per
          scar CLASS (reservoir resets, mid-file tags, mid-file ID3, resyncs,
          param changes): recall of gold full-break edges (scar within +-tol)
          and false-alarm rate per content-hour. HONESTY: main_data_begin==0
          occurs legitimately (encoder discretion, esp. silence) — the mdb==0
          base rate is reported so the verdict reflects discrimination, not
          co-occurrence. Edges within 2 s of file start/end are excluded from
          recall (trivially matched by file-boundary structure).

  align   xsdz.44: byte-run alignment of an A/B fetch pair. Anchors are
          per-frame content hashes unique in both files (the frame lattice is
          content-defined, so arbitrary byte shifts between files are
          handled); anchors sharing a byte delta are extended greedily to
          maximal BYTE-verified runs via chunked mmap compares; runs are
          chained by weighted longest strictly-monotonic subsequence
          (non-overlapping in both files); gaps between chained runs =
          inserted-in-A / inserted-in-B spans = ad slots with byte-exact
          edges. Caveat: a splice inside a frame still re-locks on the next
          shared frame boundary, then byte extension recovers the exact
          splice offset.

  dedup   xsdz.51: shared byte runs (>= min bytes) across DIFFERENT episodes
          of one show. Rung 1: byte-exact (same machinery as align). Rung 2
          (--payload): frame-PAYLOAD hashes (frame bytes minus the 4-byte
          header) — survives header-level (padding/bitrate-index) jitter but
          NOT bit-reservoir bleed, which is reported, not hidden.

Known limits (kept, deliberately, for spike honesty):
  * MPEG2/2.5 side info is parsed (8-bit main_data_begin) but the corpus is
    MPEG1 Layer III; other layers get frame length/time but mdb=None.
  * 'Free' bitrate (index 0) frames abort the walk into a resync — none seen
    in corpus.
  * Xing/Info/VBRI are only recognized at their canonical offsets to avoid
    payload-coincidence false positives; LAME/Lavc/Lavf strings are only
    checked immediately after an all-zero side info block.

Stdlib only. Read-only over TestFixtures/. All outputs go where --out says.
"""
import argparse
import bisect
import hashlib
import json
import mmap
import os
import struct
import sys

# ---------------------------------------------------------------- MP3 tables

BITRATE_V1_L3 = [None, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, None]
BITRATE_V2_L3 = [None, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160, None]
BITRATE_V1_L2 = [None, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, None]
BITRATE_V1_L1 = [None, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448, None]
SAMPLERATE = {3: [44100, 48000, 32000], 2: [22050, 24000, 16000], 0: [11025, 12000, 8000]}
# version bits: 00=MPEG2.5, 01=reserved, 10=MPEG2, 11=MPEG1


def _parse_header(b0, b1, b2, b3):
    """Return dict for a valid frame header or None."""
    if b0 != 0xFF or (b1 & 0xE0) != 0xE0:
        return None
    version = (b1 >> 3) & 0x3          # 0=2.5, 1=reserved, 2=MPEG2, 3=MPEG1
    layer = (b1 >> 1) & 0x3            # 1=III, 2=II, 3=I
    if version == 1 or layer == 0:
        return None
    protection = b1 & 0x1              # 0 => 16-bit CRC follows header
    br_idx = (b2 >> 4) & 0xF
    sr_idx = (b2 >> 2) & 0x3
    padding = (b2 >> 1) & 0x1
    if sr_idx == 3 or br_idx in (0, 15):
        return None                    # free/bad bitrate treated as invalid
    channel_mode = (b3 >> 6) & 0x3     # 3 = mono
    if version == 3:
        sr = SAMPLERATE[3][sr_idx]
        if layer == 1:
            kbps, spf = BITRATE_V1_L3[br_idx], 1152
            flen = 144000 * kbps // sr + padding
        elif layer == 2:
            kbps, spf = BITRATE_V1_L2[br_idx], 1152
            flen = 144000 * kbps // sr + padding
        else:
            kbps, spf = BITRATE_V1_L1[br_idx], 384
            flen = (12000 * kbps // sr + padding) * 4
    else:
        sr = SAMPLERATE[2 if version == 2 else 0][sr_idx]
        if layer == 1:
            kbps, spf = BITRATE_V2_L3[br_idx], 576
            flen = 72000 * kbps // sr + padding
        elif layer == 2:
            kbps, spf = BITRATE_V2_L3[br_idx], 1152
            flen = 144000 * kbps // sr + padding
        else:
            kbps, spf = BITRATE_V1_L1[br_idx], 384
            flen = (12000 * kbps // sr + padding) * 4
    if kbps is None or flen < 24:
        return None
    return {"version": version, "layer": layer, "crc": protection == 0,
            "kbps": kbps, "samplerate": sr, "padding": padding,
            "channel_mode": channel_mode, "frame_len": flen, "spf": spf}


def _side_info(mm, off, hdr):
    """(main_data_begin, side_info_all_zero) for Layer III, else (None, False)."""
    if hdr["layer"] != 1:
        return None, False
    p = off + 4 + (2 if hdr["crc"] else 0)
    mono = hdr["channel_mode"] == 3
    silen = (17 if mono else 32) if hdr["version"] == 3 else (9 if mono else 17)
    raw = mm[p:p + silen]
    if len(raw) < silen:
        return None, False
    if hdr["version"] == 3:
        mdb = (raw[0] << 1) | (raw[1] >> 7)     # 9 bits
    else:
        mdb = raw[0]                            # 8 bits
    return mdb, not any(raw)


def _id3v2_size(mm, off):
    """Byte length of an ID3v2 block at off, or 0."""
    if mm[off:off + 3] != b"ID3" or len(mm) - off < 10:
        return 0
    flags = mm[off + 5]
    sz = mm[off + 6:off + 10]
    if any(b & 0x80 for b in sz):
        return 0
    size = (sz[0] << 21) | (sz[1] << 14) | (sz[2] << 7) | sz[3]
    return 10 + size + (10 if flags & 0x10 else 0)


def parse_mp3(path, want_frames=True):
    """Walk an MP3. Returns dict with frames (offsets/lens/times), anomalies,
    stats. Frames arrays are parallel lists for compactness."""
    result = {"path": path, "size": os.path.getsize(path), "leading_id3_bytes": 0,
              "frames": {"offset": [], "length": [], "time": [], "mdb": []},
              "anomalies": [], "stats": {}}
    f = open(path, "rb")
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    n = len(mm)
    pos = _id3v2_size(mm, 0)
    result["leading_id3_bytes"] = pos
    t = 0.0
    fi = 0
    offs, lens, times, mdbs = (result["frames"][k] for k in ("offset", "length", "time", "mdb"))
    anomalies = result["anomalies"]
    prev = None
    mdb0 = 0
    l3 = 0
    while pos + 4 <= n:
        hdr = _parse_header(mm[pos], mm[pos + 1], mm[pos + 2], mm[pos + 3])
        if hdr is None:
            id3len = _id3v2_size(mm, pos)
            if id3len and pos + id3len <= n:
                anomalies.append({"kind": "midfile_id3", "offset": pos,
                                  "bytes": id3len, "time": round(t, 3)})
                pos += id3len
                continue
            if mm[pos:pos + 3] == b"TAG" and n - pos == 128:
                result["stats"]["trailing_id3v1"] = True
                break
            # resync: need two consecutive valid headers (or valid+EOF)
            scan = pos + 1
            found = None
            while scan + 4 <= n:
                h2 = _parse_header(mm[scan], mm[scan + 1], mm[scan + 2], mm[scan + 3])
                if h2 is not None:
                    nxt = scan + h2["frame_len"]
                    if nxt + 4 > n or _parse_header(mm[nxt], mm[nxt + 1], mm[nxt + 2], mm[nxt + 3]):
                        found = scan
                        break
                scan += 1
            junk = (found if found is not None else n) - pos
            anomalies.append({"kind": "resync", "offset": pos, "junk_bytes": junk,
                              "time": round(t, 3)})
            if found is None:
                break
            pos = found
            continue
        if pos + hdr["frame_len"] > n:
            anomalies.append({"kind": "truncated_final_frame", "offset": pos,
                              "time": round(t, 3)})
            break
        mdb, zero_si = _side_info(mm, pos, hdr)
        # canonical-offset tag detection
        tag = None
        sip = pos + 4 + (2 if hdr["crc"] else 0)
        mono = hdr["channel_mode"] == 3
        silen = ((17 if mono else 32) if hdr["version"] == 3 else (9 if mono else 17)) \
            if hdr["layer"] == 1 else 0
        magic = mm[sip + silen:sip + silen + 4]
        if magic in (b"Xing", b"Info"):
            tag = magic.decode()
        elif mm[pos + 36:pos + 40] == b"VBRI":
            tag = "VBRI"
        elif zero_si and mm[sip + silen:sip + silen + 48].count(b"LAME") + \
                mm[sip + silen:sip + silen + 48].count(b"Lavc") + \
                mm[sip + silen:sip + silen + 48].count(b"Lavf"):
            tag = "encoder_string_zero_si"
        if tag and fi > 0:
            anomalies.append({"kind": "midfile_tag", "tag": tag, "offset": pos,
                              "frame": fi, "time": round(t, 3)})
        if prev is not None:
            # stereo<->joint-stereo flips are routine LAME behavior; only a
            # change in channel COUNT (mono<->any stereo) is structural.
            mono_now = hdr["channel_mode"] == 3
            mono_prev = prev["channel_mode"] == 3
            if hdr["samplerate"] != prev["samplerate"] or hdr["version"] != prev["version"] \
                    or hdr["layer"] != prev["layer"] or mono_now != mono_prev:
                anomalies.append({"kind": "param_change", "offset": pos, "frame": fi,
                                  "time": round(t, 3),
                                  "from": [prev["samplerate"], prev["version"], prev["layer"], prev["channel_mode"]],
                                  "to": [hdr["samplerate"], hdr["version"], hdr["layer"], hdr["channel_mode"]]})
            elif hdr["kbps"] != prev["kbps"]:
                anomalies.append({"kind": "bitrate_change", "offset": pos, "frame": fi,
                                  "time": round(t, 3), "from": prev["kbps"], "to": hdr["kbps"]})
        if want_frames:
            offs.append(pos)
            lens.append(hdr["frame_len"])
            times.append(t)
            mdbs.append(mdb)
        if hdr["layer"] == 1:
            l3 += 1
            if mdb == 0:
                mdb0 += 1
        prev = hdr
        t += hdr["spf"] / hdr["samplerate"]
        pos += hdr["frame_len"]
        fi += 1
    mm.close()
    f.close()
    result["stats"].update({
        "frames": fi, "duration_seconds": round(t, 3),
        "layer3_frames": l3, "mdb0_frames": mdb0,
        "mdb0_rate": round(mdb0 / l3, 5) if l3 else None,
        "modal_kbps": prev["kbps"] if prev else None,
        "samplerate": prev["samplerate"] if prev else None,
        "channel_mode": prev["channel_mode"] if prev else None,
    })
    return result


# ------------------------------------------------------------- scar scoring

SCAR_CLASSES = ("mdb_reset", "midfile_tag", "midfile_id3", "resync",
                "param_change", "bitrate_change")


def scar_events(parsed):
    """Map parse output -> {class: [times]}. mdb_reset = mdb==0 with previous
    frame mdb>0 (transition), excluding frame 0."""
    ev = {k: [] for k in SCAR_CLASSES}
    mdbs = parsed["frames"]["mdb"]
    times = parsed["frames"]["time"]
    for i in range(1, len(mdbs)):
        if mdbs[i] == 0 and mdbs[i - 1] not in (0, None):
            ev["mdb_reset"].append(times[i])
    for a in parsed["anomalies"]:
        if a["kind"] in ev:
            ev[a["kind"]].append(a["time"])
    return ev


def cmd_scars(args):
    gold = json.load(open(args.gold))
    tol = args.tolerance
    per_file = []
    agg = {k: {"edges": 0, "edges_hit": 0, "content_events": 0} for k in SCAR_CLASSES}
    content_hours = 0.0
    for asset in gold["assets"]:
        eid = asset["episode_id"]
        path = os.path.join(args.audio_dir, eid + ".mp3")
        if not os.path.exists(path):
            print(f"SKIP (no A-side): {eid}", file=sys.stderr)
            continue
        parsed = parse_mp3(path)
        dur = parsed["stats"]["duration_seconds"]
        ev = scar_events(parsed)
        breaks = [(b["start_seconds"], b["end_seconds"]) for b in asset["full_breaks"]]
        edges = [e for b in breaks for e in b if tol < e < dur - tol]
        # content region = outside all breaks (each expanded by tol)
        break_secs = sum(e - s for s, e in breaks)
        content_h = max(dur - break_secs - 2 * tol * len(breaks), 0.0) / 3600.0
        content_hours += content_h
        row = {"episode_id": eid, "duration": dur, "edges": len(edges),
               "mdb0_rate": parsed["stats"]["mdb0_rate"],
               "modal_kbps": parsed["stats"]["modal_kbps"], "classes": {}}
        for cls in SCAR_CLASSES:
            evs = sorted(ev[cls])
            hit = sum(1 for e in edges if evs and _near(evs, e, tol))
            in_content = sum(1 for x in evs
                             if not any(s - tol <= x <= e + tol for s, e in breaks))
            agg[cls]["edges"] += len(edges)
            agg[cls]["edges_hit"] += hit
            agg[cls]["content_events"] += in_content
            row["classes"][cls] = {"events": len(evs), "edge_hits": hit,
                                   "content_events": in_content}
        per_file.append(row)
    out = {"tolerance_seconds": tol, "files": len(per_file),
           "content_hours": round(content_hours, 2), "per_file": per_file,
           "aggregate": {}}
    for cls in SCAR_CLASSES:
        a = agg[cls]
        out["aggregate"][cls] = {
            "edge_recall": round(a["edges_hit"] / a["edges"], 4) if a["edges"] else None,
            "edges": a["edges"], "edges_hit": a["edges_hit"],
            "false_alarms_per_content_hour":
                round(a["content_events"] / content_hours, 2) if content_hours else None,
            "content_events": a["content_events"]}
    mdb_rates = [r["mdb0_rate"] for r in per_file if r["mdb0_rate"] is not None]
    out["mdb0_base_rate_mean"] = round(sum(mdb_rates) / len(mdb_rates), 5) if mdb_rates else None
    _write(args.out, out)
    print(json.dumps(out["aggregate"], indent=2))
    print("mdb0 base rate (mean over files):", out["mdb0_base_rate_mean"])


def _near(sorted_evs, x, tol):
    i = bisect.bisect_left(sorted_evs, x)
    for j in (i - 1, i):
        if 0 <= j < len(sorted_evs) and abs(sorted_evs[j] - x) <= tol:
            return True
    return False


# --------------------------------------------------- byte-run A/B alignment

def _frame_hashes(parsed, mm, payload_only=False):
    offs = parsed["frames"]["offset"]
    lens = parsed["frames"]["length"]
    out = []
    skip = 4 if payload_only else 0
    for o, ln in zip(offs, lens):
        out.append(hashlib.blake2b(mm[o + skip:o + ln], digest_size=8).digest())
    return out


def _extend_run(ma, mb, a0, b0, ln, chunk=1 << 16):
    """Greedy byte extension of an already-equal region [a0,a0+ln)==[b0,b0+ln)."""
    # extend right
    ae, be = a0 + ln, b0 + ln
    na, nb = len(ma), len(mb)
    while ae < na and be < nb:
        c = min(chunk, na - ae, nb - be)
        if ma[ae:ae + c] == mb[be:be + c]:
            ae += c
            be += c
        else:
            lo, hi = 0, c
            while lo < hi:
                mid = (lo + hi) // 2 + 1
                if ma[ae:ae + mid] == mb[be:be + mid]:
                    lo = mid
                else:
                    hi = mid - 1
            ae += lo
            be += lo
            break
    # extend left
    while a0 > 0 and b0 > 0:
        c = min(chunk, a0, b0)
        if ma[a0 - c:a0] == mb[b0 - c:b0]:
            a0 -= c
            b0 -= c
        else:
            lo, hi = 0, c
            while lo < hi:
                mid = (lo + hi) // 2 + 1
                if ma[a0 - mid:a0] == mb[b0 - mid:b0]:
                    lo = mid
                else:
                    hi = mid - 1
            a0 -= lo
            b0 -= lo
            break
    return a0, b0, ae - a0


def byte_runs(path_a, path_b, min_run_bytes=65536, payload_only=False):
    """Maximal common byte runs between two MP3s via unique-frame anchors.
    Returns (runs, parsedA, parsedB); each run = dict(aStart,bStart,bytes)."""
    pa = parse_mp3(path_a)
    pb = parse_mp3(path_b)
    fa = open(path_a, "rb")
    fb = open(path_b, "rb")
    ma = mmap.mmap(fa.fileno(), 0, access=mmap.ACCESS_READ)
    mb = mmap.mmap(fb.fileno(), 0, access=mmap.ACCESS_READ)
    ha = _frame_hashes(pa, ma, payload_only)
    hb = _frame_hashes(pb, mb, payload_only)
    ca, cb = {}, {}
    for h in ha:
        ca[h] = ca.get(h, 0) + 1
    for h in hb:
        cb[h] = cb.get(h, 0) + 1
    pos_b = {}
    for i, h in enumerate(hb):
        if cb[h] == 1 and ca.get(h) == 1:
            pos_b[h] = i
    offs_a, lens_a = pa["frames"]["offset"], pa["frames"]["length"]
    offs_b, lens_b = pb["frames"]["offset"], pb["frames"]["length"]
    anchors = []          # (aOff, bOff, frameLen)
    for i, h in enumerate(ha):
        j = pos_b.get(h)
        if j is not None and ca[h] == 1:
            anchors.append((offs_a[i], offs_b[j], lens_a[i]))
    runs = []
    if payload_only:
        # frame-lattice runs (no byte extension: headers differ by definition)
        idx_b = {}
        for i, h in enumerate(hb):
            idx_b.setdefault(h, []).append(i)
        anchors_f = [(i, pos_b[h]) for i, h in enumerate(ha)
                     if ca.get(h) == 1 and h in pos_b]
        anchors_f.sort()
        used = -1
        for i, j in anchors_f:
            if i <= used:
                continue
            s, t = i, j
            while s > 0 and t > 0 and ha[s - 1] == hb[t - 1]:
                s -= 1
                t -= 1
            e, u = i, j
            while e + 1 < len(ha) and u + 1 < len(hb) and ha[e + 1] == hb[u + 1]:
                e += 1
                u += 1
            used = e
            nbytes = offs_a[e] + lens_a[e] - offs_a[s]
            if nbytes >= min_run_bytes:
                runs.append({"aStart": offs_a[s], "bStart": offs_b[t], "bytes": nbytes,
                             "frames": e - s + 1})
    else:
        by_delta = {}
        for aOff, bOff, fl in anchors:
            by_delta.setdefault(aOff - bOff, []).append((aOff, bOff, fl))
        for delta, lst in by_delta.items():
            lst.sort()
            covered_end = -1
            for aOff, bOff, fl in lst:
                if aOff < covered_end:
                    continue
                a0, b0, ln = _extend_run(ma, mb, aOff, bOff, fl)
                covered_end = a0 + ln
                if ln >= min_run_bytes:
                    runs.append({"aStart": a0, "bStart": b0, "bytes": ln})
    runs.sort(key=lambda r: r["aStart"])
    # dedupe identical/contained runs
    pruned = []
    for r in runs:
        if pruned and r["aStart"] >= pruned[-1]["aStart"] and \
                r["aStart"] + r["bytes"] <= pruned[-1]["aStart"] + pruned[-1]["bytes"] and \
                r["aStart"] - r["bStart"] == pruned[-1]["aStart"] - pruned[-1]["bStart"]:
            continue
        pruned.append(r)
    ma.close()
    mb.close()
    fa.close()
    fb.close()
    return pruned, pa, pb


def chain_runs(runs):
    """Max-bytes chain, strictly monotonic and non-overlapping in A and B."""
    runs = sorted(runs, key=lambda r: (r["aStart"], r["bStart"]))
    n = len(runs)
    best = [0] * n
    prev = [-1] * n
    for i, r in enumerate(runs):
        best[i] = r["bytes"]
        for j in range(i):
            q = runs[j]
            if q["aStart"] + q["bytes"] <= r["aStart"] and \
                    q["bStart"] + q["bytes"] <= r["bStart"]:
                cand = best[j] + r["bytes"]
                if cand > best[i]:
                    best[i] = cand
                    prev[i] = j
    if not runs:
        return [], 0, 0
    i = max(range(n), key=lambda k: best[k])
    total = best[i]
    chain = []
    while i != -1:
        chain.append(runs[i])
        i = prev[i]
    chain.reverse()
    return chain, total, len(runs) - len(chain)


def _time_at(parsed, byte_off):
    offs = parsed["frames"]["offset"]
    times = parsed["frames"]["time"]
    if not offs:
        return None
    i = bisect.bisect_right(offs, byte_off) - 1
    if i < 0:
        return 0.0
    # linear within frame (frames are ~26 ms; sub-frame precision is cosmetic)
    ln = parsed["frames"]["length"][i]
    frac = min(max((byte_off - offs[i]) / ln, 0.0), 1.0)
    spf_sec = (times[i + 1] - times[i]) if i + 1 < len(times) else 1152 / 44100
    return times[i] + frac * spf_sec


def cmd_align(args):
    runs, pa, pb = byte_runs(args.a, args.b, args.min_run_bytes)
    chain, total, dropped = chain_runs(runs)
    size_a = pa["size"]
    size_b = pb["size"]
    slots = []

    def add_gap(a0, a1, b0, b1, kind_hint=None):
        ga, gb = a1 - a0, b1 - b0
        if ga <= 0 and gb <= 0:
            return
        sa, ea = _time_at(pa, a0), _time_at(pa, a1)
        if kind_hint:
            kind = kind_hint
        elif ga > 0 and gb > 0:
            kind = "replaced"
        elif ga > 0:
            kind = "removed_in_B"
        else:
            kind = "inserted_in_B"
        slots.append({"kind": kind, "aStartByte": a0, "aEndByte": a1,
                      "aStartSec": round(sa, 3) if sa is not None else None,
                      "aEndSec": round(ea, 3) if ea is not None else None,
                      "aBytes": ga, "bBytes": gb,
                      "aSeconds": round((ea - sa), 3) if sa is not None and ea is not None else None})

    if chain:
        first = chain[0]
        aud_a0 = pa["leading_id3_bytes"]
        aud_b0 = pb["leading_id3_bytes"]
        add_gap(aud_a0, first["aStart"], aud_b0, first["bStart"], "head")
        for r1, r2 in zip(chain, chain[1:]):
            add_gap(r1["aStart"] + r1["bytes"], r2["aStart"],
                    r1["bStart"] + r1["bytes"], r2["bStart"])
        last = chain[-1]
        add_gap(last["aStart"] + last["bytes"], size_a,
                last["bStart"] + last["bytes"], size_b, "tail")
    out = {
        "a": args.a, "b": args.b, "min_run_bytes": args.min_run_bytes,
        "runs_found": len(runs), "runs_chained": len(chain),
        "runs_dropped_nonmonotonic": dropped,
        "monotonic_clean": dropped == 0,
        "chained_bytes": total,
        "chained_seconds": round(total / (size_a / pa["stats"]["duration_seconds"]), 1)
        if pa["stats"]["duration_seconds"] else None,
        "a_duration": pa["stats"]["duration_seconds"],
        "b_duration": pb["stats"]["duration_seconds"],
        "slots": slots,
    }
    _write(args.out, out)
    print(json.dumps({k: v for k, v in out.items() if k != "slots"}, indent=2))
    for s in slots:
        print(f"  slot {s['kind']:>12}: A {s['aStartSec']}s..{s['aEndSec']}s "
              f"({s['aSeconds']}s, {s['aBytes']}B) vs B {s['bBytes']}B")


def cmd_dedup(args):
    files = args.files
    pairs = [(files[i], files[j]) for i in range(len(files)) for j in range(i + 1, len(files))]
    out = {"payload_only": args.payload, "min_run_bytes": args.min_run_bytes, "pairs": []}
    for a, b in pairs:
        runs, pa, pb = byte_runs(a, b, args.min_run_bytes, payload_only=args.payload)
        rows = []
        for r in runs:
            t0 = _time_at(pa, r["aStart"])
            t1 = _time_at(pa, r["aStart"] + r["bytes"])
            u0 = _time_at(pb, r["bStart"])
            rows.append({"bytes": r["bytes"],
                         "aStartSec": round(t0, 2), "aEndSec": round(t1, 2),
                         "bStartSec": round(u0, 2),
                         "seconds": round(t1 - t0, 2)})
        out["pairs"].append({"a": os.path.basename(a), "b": os.path.basename(b),
                             "shared_runs": rows,
                             "aDuration": pa["stats"]["duration_seconds"],
                             "bDuration": pb["stats"]["duration_seconds"]})
        print(f"{os.path.basename(a)} vs {os.path.basename(b)}: "
              f"{len(rows)} shared runs "
              f"({sum(r['seconds'] for r in rows):.1f}s)")
    _write(args.out, out)


def cmd_parse(args):
    parsed = parse_mp3(args.file)
    slim = dict(parsed)
    if not args.frames:
        slim = {k: v for k, v in parsed.items() if k != "frames"}
    if args.out:
        _write(args.out, parsed if args.frames else slim)
    print(json.dumps(slim if not args.frames else
                     {k: v for k, v in parsed.items() if k != "frames"}, indent=2))


def _write(path, obj):
    if not path:
        return
    with open(path, "w") as fh:
        json.dump(obj, fh, indent=1)
    print(f"wrote {path}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("parse", help="parse one MP3, dump stats + anomalies")
    p.add_argument("file")
    p.add_argument("--frames", action="store_true", help="include per-frame arrays in --out")
    p.add_argument("--out")
    p.set_defaults(fn=cmd_parse)

    p = sub.add_parser("scars", help="xsdz.43 scar-class scoring vs gold edges")
    p.add_argument("--audio-dir", required=True)
    p.add_argument("--gold", required=True)
    p.add_argument("--tolerance", type=float, default=2.0)
    p.add_argument("--out")
    p.set_defaults(fn=cmd_scars)

    p = sub.add_parser("align", help="xsdz.44 byte-run A/B alignment -> slots")
    p.add_argument("--a", required=True)
    p.add_argument("--b", required=True)
    p.add_argument("--min-run-bytes", type=int, default=65536)
    p.add_argument("--out")
    p.set_defaults(fn=cmd_align)

    p = sub.add_parser("dedup", help="xsdz.51 cross-episode shared byte runs")
    p.add_argument("--files", nargs="+", required=True)
    p.add_argument("--min-run-bytes", type=int, default=100000)
    p.add_argument("--payload", action="store_true",
                   help="rung 2: match frame payload hashes (bytes minus 4-byte header)")
    p.add_argument("--out")
    p.set_defaults(fn=cmd_dedup)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
