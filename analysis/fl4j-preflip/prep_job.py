#!/usr/bin/env python3
"""playhead-fl4j pre-flip validation: build the measurement job JSON.

Assembles the three evaluation lanes the bead's PRE-FLIP GATE requires:
  A. third-party ad copy  = gold full_breaks (gold-v-next-refined, 44 eps /
     150 breaks) + the gold-v6 70-break subset (comparability with the
     bc9ae7d5 flip commit's "0/70" claim)
  B. self-promo positives = Dan's 20 content vetoes from the gold v6 eval
     (subtype decomposition per fl4j-negative-anchor-prototype-2026-07-16.md)
  C. neutral content      = 60 s non-ad windows excluding all breaks/vetoes

Emits one job per identity variant:
  job-title.json    — show identity from title only (production analogue when
                      no networkId is set on the profile)
  job-network.json  — title + plausible networkId (sensitivity probe: network
                      underwriting reads are the known contamination channel)
"""
import json
import os
import sys

SCRATCH = "/private/tmp/claude-501/-Users-dabrams-playhead/6ce9b37b-c84d-4ce3-a585-8e33b921ee5b/scratchpad"
GOLD_REFINED = os.path.join(SCRATCH, "rediff", "gold-v-next-refined.json")
GOLD_V6 = "/Users/dabrams/playhead/TestFixtures/Corpus/Evaluations/earaudit-oracle-gold-836b81885f6d279a84c1ef0dee83302e7df6ed28f0d20ec2db621b518f1ef220.json"
TRANSCRIPTS = "/Users/dabrams/playhead/TestFixtures/Corpus/Transcripts"
BANK = None  # filled from --bank
OUTDIR = os.path.join(SCRATCH, "fl4j-preflip")

# Titles for refined assets with empty show_name (derived from episode ids /
# sibling assets in the same gold file).
TITLE_FILL = {
    "american-scandal": "American Scandal",
    "smartless": "Smartless",
    "techcrunch-daily-crunch": "Techcrunch Daily Crunch",
    "the-ezra-klein-show": "The Ezra Klein Show",
    "the-mel-robbins-podcast": "The Mel Robbins Podcast",
    "the-nikki-glaser-podcast": "The Nikki Glaser Podcast",
    "up-first": "Up First",
    "why-is-this-happening-the-chris-hayes-po": "Why Is This Happening The Chris Hayes Po",
    "conan": "Conan O'Brien Needs A Friend",
    "doac": "The Diary Of A CEO",
    "on-the-media": "On The Media",
    "ted-business": "Ted Business",
    "themove": "THEMOVE",
    "unexplained": "Unexplained",
    "morbid": "Morbid",
    "radiolab": "Radiolab",
    "business-wars": "Business Wars",
    "casefile-true-crime": "Casefile True Crime",
    "fresh-air": "Fresh Air",
    "hard-fork": "Hard Fork",
    "planet-money": "Planet Money",
    "stuff-you-should-know": "Stuff You Should Know",
}

# Sensitivity variant: plausible network identity (the underwriting-read
# contamination channel the prototype's C3 flagged: "<Network> is supported
# by <brand>"). NOT authoritative production values — a probe.
NETWORK_FILL = {
    "on-the-media": "WNYC Studios",
    "radiolab": "WNYC Studios",
    "conan": "Team Coco",
    "business-wars": "Wondery",
    "american-scandal": "Wondery",
    "morbid": "Wondery",
    "planet-money": "NPR",
    "up-first": "NPR",
    "fresh-air": "NPR",
    "daily": "The New York Times",
    "hard-fork": "The New York Times",
    "the-ezra-klein-show": "The New York Times",
    "ted-business": "TED Audio Collective",
}

# Veto subtype decomposition, keyed (episode_id, round(start)) — from
# fl4j-negative-anchor-prototype-2026-07-16.md (audit-verifiable table).
VETO_SUBTYPE = {
    ("conan-2026-07-09-the-wedding-ringer", 64): "self_promo",
    ("conan-2026-07-09-the-wedding-ringer", 1377): "self_promo",
    ("unexplained-2026-06-26-season-09-episode-26-in-through-the-out-", 190): "self_promo",
    ("unexplained-2026-06-26-season-09-episode-26-in-through-the-out-", 2390): "self_promo",
    ("unexplained-2026-07-03-season-09-episode-27-death-in-vegas-the-", 230): "self_promo",
    ("unexplained-2026-07-03-season-09-episode-27-death-in-vegas-the-", 2765): "self_promo",
    ("unexplained-2026-07-10-season-09-episode-27-death-in-vegas-the-", 2477): "self_promo",
    ("doac-2026-07-13-openai-whistleblower-finally-speaks-ai-h", 7144): "guest_plug",
    ("themove-2026-07-14-rest-day-hotels-cause-a-stir-amp-poga-ar", 3103): "sponsored_editorial",
    ("themove-2026-07-14-rest-day-hotels-cause-a-stir-amp-poga-ar", 3455): "sponsored_editorial",
    ("themove-2026-07-15-how-an-ultra-fast-stage-led-to-a-shockin", 2434): "sponsored_editorial",
    ("themove-2026-07-15-how-an-ultra-fast-stage-led-to-a-shockin", 2626): "sponsored_editorial",
    ("themove-2026-07-15-uno-x-keeps-their-five-star-tour-rolling", 2952): "sponsored_editorial",
    ("on-the-media-2026-05-29-trump-sued-himself-and-settled-for-a-1-8", 1231): "content_boundary",
    ("techcrunch-daily-crunch-2026-05-29-google-engineer-charged-with-insider-tra", 224): "content_boundary",
    ("ted-business-2026-05-25-the-secret-to-making-the-right-career-de", 922): "content_boundary",
    ("ted-business-2026-05-25-the-secret-to-making-the-right-career-de", 1018): "content_boundary",
    ("ted-business-2026-05-25-the-secret-to-making-the-right-career-de", 1938): "content_boundary",
    ("ted-business-2026-05-25-the-secret-to-making-the-right-career-de", 1973): "content_boundary",
    ("unexplained-2026-07-10-season-09-episode-27-death-in-vegas-the-", 838): "content_boundary",
}

NEUTRAL_WINDOW = 60.0
NEUTRAL_MARGIN = 3.0


def show_prefix(episode_id):
    # everything before the YYYY-MM-DD date token
    parts = episode_id.split("-")
    for i in range(len(parts)):
        if len(parts[i]) == 4 and parts[i].isdigit() and i + 2 < len(parts):
            return "-".join(parts[:i])
    return episode_id


def transcript_duration(path):
    with open(path) as f:
        d = json.load(f)
    segs = d.get("transcription") or []
    if not segs:
        return 0.0
    return max(s["offsets"]["to"] for s in segs) / 1000.0


def main():
    refined = json.load(open(GOLD_REFINED))["assets"]
    v6 = json.load(open(GOLD_V6))["assets"]

    episodes = {}  # episode_id -> dict

    def ep(eid, title_hint=""):
        if eid in episodes:
            return episodes[eid]
        tpath = os.path.join(TRANSCRIPTS, eid + ".json")
        if not os.path.exists(tpath):
            raise SystemExit(f"missing transcript for {eid}")
        pref = show_prefix(eid)
        title = title_hint or TITLE_FILL.get(pref)
        if not title:
            raise SystemExit(f"no title for {eid} (prefix {pref})")
        episodes[eid] = {
            "episodeId": eid,
            "title": title,
            "networkId": None,
            "transcriptPath": tpath,
            "spans": [],
        }
        return episodes[eid]

    n_refined = 0
    for a in refined:
        e = ep(a["episode_id"], a.get("show_name", ""))
        for b in a.get("full_breaks") or []:
            e["spans"].append({
                "cls": "ad_refined",
                "start": b["start_seconds"],
                "end": b["end_seconds"],
                "tag": b.get("sponsor") or "",
            })
            n_refined += 1

    n_v6 = 0
    n_veto = 0
    for a in v6:
        eid = a["episode_id"]
        e = ep(eid)
        for b in a.get("full_breaks") or []:
            e["spans"].append({
                "cls": "ad_v6",
                "start": b["start_seconds"],
                "end": b["end_seconds"],
                "tag": "",
            })
            n_v6 += 1
        for v in a.get("content_vetoes") or []:
            key = (eid, round(v["start_seconds"]))
            sub = VETO_SUBTYPE.get(key)
            if sub is None:
                raise SystemExit(f"unmapped veto {key}")
            e["spans"].append({
                "cls": "veto_" + sub,
                "start": v["start_seconds"],
                "end": v["end_seconds"],
                "tag": "",
            })
            n_veto += 1

    # Neutral windows per episode: exclude overlap with ANY break/veto span
    # (margin ±3 s).
    n_neutral = 0
    for eid, e in episodes.items():
        dur = transcript_duration(e["transcriptPath"])
        occupied = [(s["start"] - NEUTRAL_MARGIN, s["end"] + NEUTRAL_MARGIN)
                    for s in e["spans"]]
        t = 0.0
        while t + NEUTRAL_WINDOW <= dur:
            w = (t, t + NEUTRAL_WINDOW)
            if not any(w[0] < o[1] and w[1] > o[0] for o in occupied):
                e["spans"].append({
                    "cls": "neutral", "start": w[0], "end": w[1], "tag": "",
                })
                n_neutral += 1
            t += NEUTRAL_WINDOW

    bank = sys.argv[sys.argv.index("--bank") + 1]
    eps = sorted(episodes.values(), key=lambda x: x["episodeId"])

    job_title = {"bankPath": bank, "episodes": eps}
    with open(os.path.join(OUTDIR, "job-title.json"), "w") as f:
        json.dump(job_title, f, indent=1)

    import copy
    eps_net = copy.deepcopy(eps)
    for e in eps_net:
        e["networkId"] = NETWORK_FILL.get(show_prefix(e["episodeId"]))
    with open(os.path.join(OUTDIR, "job-network.json"), "w") as f:
        json.dump({"bankPath": bank, "episodes": eps_net}, f, indent=1)

    print(f"episodes={len(eps)} ad_refined={n_refined} ad_v6={n_v6} "
          f"vetoes={n_veto} neutral={n_neutral}")


if __name__ == "__main__":
    main()
