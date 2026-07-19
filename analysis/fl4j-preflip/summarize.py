#!/usr/bin/env python3
"""Summarize fl4j pre-flip harness output into the verdict tables."""
import json
import re
import sys
from collections import defaultdict

EVENT_VOCAB_TOKENS = {
    "ticket", "tickets", "ticketmaster", "livenation", "stubhub", "seatgeek",
    "tour", "touring", "concert", "concerts", "festival", "venue", "venues",
    "presale", "arena", "stream", "streaming", "streamed",
}
EVENT_VOCAB_BIGRAMS = [
    ("box", "office"), ("live", "show"), ("live", "shows"), ("on", "tour"),
    ("get", "tickets"), ("live", "version"), ("live", "event"),
    ("live", "events"), ("on", "sale"),
]


def load(path):
    return json.load(open(path))


def vocab_hits(text):
    toks = text.split()
    hits = sorted({t for t in toks if t in EVENT_VOCAB_TOKENS})
    bi = sorted({" ".join(b) for b in EVENT_VOCAB_BIGRAMS
                 for i in range(len(toks) - 1) if (toks[i], toks[i + 1]) == b})
    return hits, bi


def summarize(path, label):
    d = load(path)
    spans = d["spans"]
    print(f"\n================ {label} ================")
    print(f"mismatches (real-vs-diagnostic): {d['mismatches']}")

    for lane in ("ad_refined", "ad_v6"):
        rows = [s for s in spans if s["cls"] == lane]
        fired = [s for s in rows if s["suppressed"]]
        old = [s for s in rows if s["oldFire"]]
        subset_ok = all(s["oldFire"] for s in fired)
        print(f"\n--- LANE A {lane}: {len(rows)} breaks | "
              f"REWORK false-fires: {len(fired)} | old-design (bare-match) fires: {len(old)} | "
              f"rework ⊆ old: {subset_ok}")
        # per-phrase attention/verification on ads
        att = defaultdict(set)
        ver = defaultdict(set)
        for s in rows:
            key = (s["episodeId"], s["start"])
            for c in s["candidates"]:
                att[c["phrase"]].add(key)
                if c["corroborated"]:
                    ver[c["phrase"]].add(key)
        print(f"{'phrase':32s} {'class':22s} {'att.breaks':>10s} {'VERIFIED-fires':>14s}")
        for ph, cls in sorted(d["bankPhrases"].items(), key=lambda kv: (kv[1], kv[0])):
            a, v = len(att.get(ph, ())), len(ver.get(ph, ()))
            if a or v:
                print(f"{ph:32s} {cls:22s} {a:10d} {v:14d}")
        for s in fired:
            why = [c for c in s["candidates"] if c["corroborated"]]
            for c in why:
                print(f"  FIRE {s['episodeId']} [{s['start']:.0f}-{s['end']:.0f}] "
                      f"phrase='{c['phrase']}' ({c['selfReference']}) "
                      f"fp={c['windowFirstPerson']} id={c['windowIdentity']}")
                print(f"       window: …{c['window']}…")
        # old-design detail for ambiguous phrases (the risk class)
        amb_old = defaultdict(set)
        for s in rows:
            for c in s["candidates"]:
                if c["selfReference"] == "requiresCorroboration":
                    amb_old[c["phrase"]].add((s["episodeId"], s["start"]))

    # Lane B: vetoes
    print("\n--- LANE B: Dan's 20 vetoes (recall = suppressed → banner)")
    for sub in ("veto_self_promo", "veto_guest_plug", "veto_sponsored_editorial",
                "veto_content_boundary"):
        rows = [s for s in spans if s["cls"] == sub]
        got = [s for s in rows if s["suppressed"]]
        dur = sum(s["end"] - s["start"] for s in rows)
        gdur = sum(s["end"] - s["start"] for s in got)
        print(f"{sub:28s} {len(got)}/{len(rows)} caught  ({gdur:.1f}s of {dur:.1f}s)")
        for s in rows:
            why = [c for c in s["candidates"] if c["corroborated"]]
            mark = "CAUGHT" if s["suppressed"] else "miss  "
            phr = ", ".join(sorted({f"{c['phrase']}({'sE' if c['selfReference']=='selfEvident' else 'rC'})" for c in why})) or "-"
            att = ", ".join(sorted({c["phrase"] for c in s["candidates"] if not c["corroborated"]})) or "-"
            print(f"   {mark} {s['episodeId'][:44]:44s} [{s['start']:.0f}-{s['end']:.0f}] via: {phr}"
                  + (f" | uncorroborated: {att}" if att != "-" else ""))

    # Lane C: neutral
    rows = [s for s in spans if s["cls"] == "neutral"]
    fired = [s for s in rows if s["suppressed"]]
    old = [s for s in rows if s["oldFire"]]
    print(f"\n--- LANE C neutral: {len(rows)} windows (60s) | rework fires: {len(fired)} | old fires: {len(old)}")
    byp = defaultdict(int)
    for s in fired:
        for c in s["candidates"]:
            if c["corroborated"]:
                byp[(c["phrase"], c["selfReference"])] += 1
    for (ph, cls), n in sorted(byp.items(), key=lambda kv: -kv[1]):
        print(f"   {ph:32s} {cls:22s} {n}")

    # Event/ticketing vocabulary scan on real-ad copy
    print("\n--- EVENT/TICKETING/STREAMING VOCAB in gold ad breaks (ad_refined)")
    n_vocab = 0
    for s in spans:
        if s["cls"] != "ad_refined" or not s.get("text"):
            continue
        hits, bi = vocab_hits(s["text"])
        if hits or bi:
            n_vocab += 1
            print(f"   {s['episodeId'][:44]:44s} [{s['start']:.0f}-{s['end']:.0f}] "
                  f"tokens={hits} bigrams={bi} suppressed={s['suppressed']} "
                  f"tag={s.get('tag','')[:40]}")
    print(f"   breaks with event vocab: {n_vocab}/150")


if __name__ == "__main__":
    for path, label in [(sys.argv[1], "TITLE-ONLY identity"),
                        (sys.argv[2], "TITLE+NETWORK identity (sensitivity)")]:
        summarize(path, label)
