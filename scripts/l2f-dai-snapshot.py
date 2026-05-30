#!/usr/bin/env python3
"""
l2f-dai-snapshot.py — Tier-A corpus growth: snapshot DAI episodes NOW so a
later (time-separated) re-download can be DIFFED against the snapshot. The
differing bytes between the two downloads are the dynamic ad fills, giving
exact ad boundaries with no human, no model, no cloud.

Usage:
    scripts/l2f-dai-snapshot.py                 # snapshot the default batch
    scripts/l2f-dai-snapshot.py --shows "SmartLess|Casefile True Crime"
    scripts/l2f-dai-snapshot.py --episode-index 0   # 0 = latest item in RSS

Writes audio to TestFixtures/Corpus/Audio/<show-slug>/<show>-<date>-<slug>.mp3
(so the existing l2f-local-transcribe + l2f-draft-annotation pipeline picks it
up for Tier-C immediately), and records {enclosureUrl, sha256, snapshotIso}
in TestFixtures/Corpus/Snapshots/manifest.json for the later rediff/diff step.
"""
import argparse, datetime as dt, hashlib, json, os, pathlib, re, subprocess, time, urllib.parse

REPO = pathlib.Path(__file__).resolve().parents[1]
AUDIO_ROOT = REPO / "TestFixtures/Corpus/Audio"
MANIFEST = REPO / "TestFixtures/Corpus/Snapshots/manifest.json"
UA = "Mozilla/5.0 (Macintosh) Podcast/1.0"
TIMEOUT_SEARCH = "20"
TIMEOUT_DOWNLOAD = "180"

DEFAULT_BATCH = [
    "TechCrunch Daily Crunch",       # mgln.ai (Megaphone)
    "The Big Interview with Dan Rather",  # omny
    "Casefile True Crime",           # acast
    "TED Business",                  # acast
    "Last Week in AI",               # art19
    "The Nikki Glaser Podcast",      # podtrac (+ transcripts)
    "The Charlie Kirk Show",         # pdst.fm (+ transcripts)
    "SmartLess",                     # podtrac
    "Morbid",                        # podtrac
    "Why Is This Happening? The Chris Hayes Podcast",  # podtrac
]

def curl(url, rng=None, mt=TIMEOUT_SEARCH):
    cmd = ["curl","-sL","-A",UA,"--max-time",mt]
    if rng: cmd += ["-r", rng]
    cmd += [url]
    try: return subprocess.run(cmd, capture_output=True, timeout=int(mt)+15).stdout
    except Exception: return b""

def feed_for(name):
    u = "https://itunes.apple.com/search?" + urllib.parse.urlencode(
        {"term": name, "entity": "podcast", "limit": 1, "country": "US"})
    try: return json.loads(curl(u).decode("utf-8","ignore"))["results"][0]["feedUrl"]
    except Exception: return None

def slug(s, maxlen=40):
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s[:maxlen]

def pick_episode(rss, idx):
    items = re.findall(r"<item[\s\S]*?</item>", rss)
    if idx >= len(items): idx = 0
    it = items[idx]
    title = re.search(r"<title>([\s\S]*?)</title>", it).group(1).strip()
    title = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", title, flags=re.S)
    pub   = re.search(r"<pubDate>([\s\S]*?)</pubDate>", it).group(1).strip() if "<pubDate>" in it else ""
    enc   = re.search(r'<enclosure[^>]*url="([^"]+)"', it).group(1)
    return title, pub, enc

def parse_pub_date(s):
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z","%a, %d %b %Y %H:%M:%S %z","%a, %d %b %Y %H:%M:%S GMT"):
        try: return dt.datetime.strptime(s.strip(), fmt).date().isoformat()
        except Exception: pass
    return dt.date.today().isoformat()

def load_manifest():
    if MANIFEST.exists():
        try: return json.loads(MANIFEST.read_text())
        except Exception: return []
    return []

def save_manifest(rows):
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(rows, indent=2))

def snapshot_one(name, episode_index=0):
    feed = feed_for(name)
    if not feed: return {"show": name, "ok": False, "error": "feed-lookup-failed"}
    rss = curl(feed, "0-300000").decode("utf-8","ignore")
    if "<item" not in rss: return {"show": name, "ok": False, "error": "no-items"}
    try:
        title, pubraw, enc = pick_episode(rss, episode_index)
    except Exception as e:
        return {"show": name, "ok": False, "error": f"item-parse: {e}"}
    pub = parse_pub_date(pubraw)
    show_slug = slug(name)
    ep_slug = slug(title)
    episode_id = f"{show_slug}-{pub}-{ep_slug}"
    # Flat layout matches the existing corpus (Audio/<episode-id>.mp3) so the
    # existing l2f-local-transcribe contentsOfDirectory scan picks them up.
    out_path = AUDIO_ROOT / f"{episode_id}.mp3"
    if out_path.exists():
        return {"show": name, "ok": True, "skipped": "exists", "path": str(out_path.relative_to(REPO))}
    print(f"  ↓ {name:34} ep='{title[:42]}' ({pub})", flush=True)
    data = curl(enc, mt=TIMEOUT_DOWNLOAD)
    if len(data) < 100_000:
        return {"show": name, "ok": False, "error": f"download-too-small ({len(data)}B)"}
    out_path.write_bytes(data)
    sha = hashlib.sha256(data).hexdigest()
    host = re.match(r"https?://([^/]+)", enc).group(1)
    return {
        "show": name, "showSlug": show_slug, "episodeId": episode_id, "title": title,
        "publishDate": pub, "snapshotIso": dt.datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "feedUrl": feed, "enclosureUrl": enc, "enclosureHost": host,
        "audioPath": str(out_path.relative_to(REPO)),
        "sizeBytes": len(data), "sha256": sha,
        "ok": True,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shows", help="| separated show names; default = built-in DAI batch")
    ap.add_argument("--episode-index", type=int, default=0, help="0=latest item")
    args = ap.parse_args()
    shows = args.shows.split("|") if args.shows else DEFAULT_BATCH
    AUDIO_ROOT.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    seen_ids = {r.get("episodeId") for r in manifest}
    results = []
    for s in shows:
        r = snapshot_one(s.strip(), episode_index=args.episode_index)
        results.append(r)
        if r.get("ok") and "episodeId" in r and r["episodeId"] not in seen_ids:
            manifest.append({k: r[k] for k in (
                "show","showSlug","episodeId","title","publishDate","snapshotIso",
                "feedUrl","enclosureUrl","enclosureHost","audioPath","sizeBytes","sha256")})
        save_manifest(manifest)
    ok = [r for r in results if r.get("ok")]
    fail = [r for r in results if not r.get("ok")]
    print(f"\n{'='*60}")
    print(f"snapshot complete: {len(ok)}/{len(results)} ok, {len(fail)} failed")
    total = sum(r.get("sizeBytes", 0) for r in ok)/1024/1024
    print(f"audio: TestFixtures/Corpus/Audio/<show-slug>/   ({total:.1f} MB)")
    print(f"manifest: TestFixtures/Corpus/Snapshots/manifest.json  ({len(manifest)} entries)")
    for r in fail: print(f"  FAIL: {r['show']:32} {r.get('error')}")

if __name__ == "__main__":
    main()
