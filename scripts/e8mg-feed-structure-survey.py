"""Measure what the three structural ownership signals actually resolve to,
across a large sample of REAL podcast feeds (playhead-e8mg).

`OwnershipGraph` documents three sources for `.showOwned` — the RSS channel
`<link>`, the `<itunes:owner>` email domain, and the feed URL — and `.showOwned`
injects NEGATIVE lexical evidence, so a domain wrongly admitted argues that
hearing it makes a segment LESS likely to be an ad. Deciding which of the three
to believe is therefore a measurement, not a preference, and two subscribed
shows are not a sample.

This is the script that produced the numbers quoted in
`OwnershipGraph.feedHostDomain` and `OwnershipGraph.ingestRSSFeed`. It hits the
network and is DEV TOOLING — nothing it does runs in the app, and nothing the
app does depends on it.

    python3 scripts/e8mg-feed-structure-survey.py --out /tmp/e8mg-survey

Stage 1 collects feed URLs from the public iTunes search API across a spread of
terms. Stage 2 fetches the first 250 KB of each feed — the channel head, which
precedes the first `<item>` on every feed observed — and extracts three domains.
Both stages cache on disk, so a re-run costs nothing and `--skip-fetch` reports
from whatever is already there.

WHAT IT MEASURES, and why each column is the one that matters:

  * SHAREDNESS, per route: the fraction of feeds whose domain by that route is
    also some OTHER show's domain by the same route. A domain thousands of
    shows resolve to cannot identify one of them. This is the number that
    settles `ingestFeedURL`.
  * link == feed host, owner == feed host: how often a route lands back on the
    hosting platform by a different name. This is why the exclusion lives in
    `OwnershipGraph` rather than at one call site.
  * link vs owner disagreement: how often the two surviving declarations name
    different parties, which is what the precedence rule adjudicates.
  * THE SELF-HOSTED CASE: feeds whose host is unique in the sample — the only
    population in which promoting the feed host could ever have been right —
    and whether `<link>` or `<itunes:owner>` already names the same domain.

Read the parsing as APPROXIMATE and the shape claims as exact. The extraction
here is regex over the channel head, not `FeedParser`; it exists to rank
populations, and the per-feed claims this bead relies on are pinned by
`FeedParserRealFeedTests` against verbatim publisher bytes instead.
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.parse
from urllib.parse import urlparse

SEARCH_TERMS = [
    "news", "comedy", "true crime", "business", "science", "history", "health",
    "technology", "sports", "music", "interview", "politics", "fiction", "daily",
    "culture", "education", "football", "money", "design", "parenting",
]

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"

# The same minimal list `DomainNormalizer.multiPartTLDs` carries. Kept in step
# with it deliberately: a survey that normalizes differently from the code it
# is advising would be measuring its own normalizer.
MULTI_PART_TLDS = {
    "co.uk", "co.jp", "co.kr", "co.nz", "co.za", "co.in",
    "com.au", "com.br", "com.cn", "com.mx", "com.tw",
    "org.uk", "net.au", "ac.uk", "gov.uk",
}

def etld1(value: str | None) -> str | None:
    """eTLD+1 of a URL, bare host, or email address."""
    if not value:
        return None
    value = value.strip()
    if "@" in value:
        value = value.split("@", 1)[1]
    if "://" not in value:
        value = "https://" + value
    try:
        host = urlparse(value).hostname or ""
    except ValueError:
        return None
    labels = host.lower().strip(".").split(".")
    if len(labels) < 2 or not labels[-1].isalpha():
        return None
    if len(labels) >= 3 and ".".join(labels[-2:]) in MULTI_PART_TLDS:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])

def collect_feed_urls(out_dir: str) -> dict[str, str]:
    path = os.path.join(out_dir, "feedlist.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    feeds: dict[str, str] = {}
    for term in SEARCH_TERMS:
        url = (
            "https://itunes.apple.com/search?term=%s&entity=podcast&limit=50&country=US"
            % urllib.parse.quote(term)
        )
        try:
            raw = subprocess.run(
                ["curl", "-sS", "--max-time", "30", url],
                capture_output=True, timeout=45,
            ).stdout
            payload = json.loads(raw)
        except Exception as error:  # noqa: BLE001 - a failed term is not fatal
            print(f"  term {term!r}: {error}", file=sys.stderr)
            continue
        for result in payload.get("results", []):
            feed_url = result.get("feedUrl")
            if feed_url and feed_url.startswith("http"):
                feeds[feed_url] = result.get("collectionName")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(feeds, handle)
    return feeds

def fetch_heads(feeds: dict[str, str], out_dir: str) -> dict[str, str]:
    heads_dir = os.path.join(out_dir, "heads")
    os.makedirs(heads_dir, exist_ok=True)

    def grab(feed_url: str) -> tuple[str, str]:
        key = hashlib.sha1(feed_url.encode()).hexdigest()[:16]
        path = os.path.join(heads_dir, key + ".xml")
        if os.path.exists(path) and os.path.getsize(path) > 500:
            return (feed_url, path)
        # `head -c` truncates rather than downloading the whole feed; several
        # of these are 5-10 MB and only the channel head is wanted.
        subprocess.run(
            "curl -sS -L --max-time 45 -A '%s' '%s' | head -c 250000 > %s"
            % (USER_AGENT, feed_url.replace("'", "%27"), path),
            shell=True, capture_output=True,
        )
        return (feed_url, path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        return dict(pool.map(grab, list(feeds)))

def extract(path: str) -> dict[str, str | None] | None:
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
    except OSError:
        return None
    if "<channel" not in text and "<feed" not in text:
        return None
    head = re.split(r"<item[\s>]", text, 1)[0]
    head = re.split(r"<entry[\s>]", head, 1)[0]
    # `<image>` carries its OWN `<link>`, and it is not the channel's — the
    # defect `FeedParserStructuralOwnershipTests` pins on the real Diary Of A
    # CEO feed, whose only `<link>` anywhere is inside `<image>`.
    without_image = re.sub(r"<image[\s>][\s\S]*?</image>", "", head, flags=re.I)
    without_image = re.sub(
        r"<itunes:image[\s>][\s\S]*?</itunes:image>", "", without_image, flags=re.I
    )
    link = re.search(
        r"<link(?![a-zA-Z:])[^>]*>\s*([^<\s][^<]*?)\s*</link>", without_image, re.I
    )
    image_link = re.search(
        r"<image[\s>][\s\S]*?<link[^>]*>\s*([^<\s][^<]*?)\s*</link>", head, re.I
    )
    owner = re.search(
        r"<itunes:owner[\s>][\s\S]*?<itunes:email[^>]*>\s*([^<\s][^<]*?)\s*</itunes:email>",
        head, re.I,
    )
    bare_email = None
    if not owner:
        without_owner = re.sub(
            r"<itunes:owner[\s>][\s\S]*?</itunes:owner>", "", head, flags=re.I
        )
        bare = re.search(
            r"<itunes:email[^>]*>\s*([^<\s][^<]*?)\s*</itunes:email>", without_owner, re.I
        )
        bare_email = bare.group(1) if bare else None
    return {
        "link": link.group(1) if link else None,
        "imageLink": image_link.group(1) if image_link else None,
        "owner": owner.group(1) if owner else None,
        "bareEmail": bare_email,
    }

def report(records: list[dict]) -> None:
    total = len(records)

    def pct(count: int) -> str:
        return f"{count} / {total} = {count / total:.1%}" if total else "0"

    print(f"parsed feeds: {total}")
    print("has channel <link>          :", pct(sum(1 for r in records if r["ld"])))
    print("has <itunes:owner> email    :", pct(sum(1 for r in records if r["od"])))
    print("has bare <itunes:email>     :", pct(sum(1 for r in records if r["bd"])))
    print("link == feed host           :",
          pct(sum(1 for r in records if r["ld"] and r["ld"] == r["fd"])))
    print("owner == feed host          :",
          pct(sum(1 for r in records if r["od"] and r["od"] == r["fd"])))
    both = [r for r in records if r["ld"] and r["od"]]
    agree = sum(1 for r in both if r["ld"] == r["od"])
    print(f"link vs owner               : {len(both)} carry both; "
          f"{agree} agree, {len(both) - agree} DISAGREE "
          f"({(len(both) - agree) / len(both):.1%})" if both else "")
    print("no channel <link>, only <image><link>:",
          pct(sum(1 for r in records if not r["ld"] and r["imgld"])))

    print("\n--- SHAREDNESS: how many DIFFERENT shows resolve to the same domain")
    counters = {
        "feed host": collections.Counter(r["fd"] for r in records if r["fd"]),
        "channel <link>": collections.Counter(r["ld"] for r in records if r["ld"]),
        "itunes owner": collections.Counter(r["od"] for r in records if r["od"]),
    }
    keys = {"feed host": "fd", "channel <link>": "ld", "itunes owner": "od"}
    for name, counter in counters.items():
        key = keys[name]
        carriers = [r for r in records if r[key]]
        shared = sum(1 for r in carriers if counter[r[key]] >= 2)
        share = shared / len(carriers) if carriers else 0
        print(f"  {name:16s}: {len(carriers):4d} feeds carry it; {shared:4d} ({share:.1%}) "
              f"land on a domain shared by >=2 shows; {len(counter)} distinct")
        print(f"      top: {counter.most_common(10)}")

    print("\n--- THE SELF-HOSTED CASE (feed host unique in this sample)")
    feed_counter = counters["feed host"]
    unique = [r for r in records if r["fd"] and feed_counter[r["fd"]] == 1]
    covered = [r for r in unique if r["fd"] in {r["ld"], r["od"], r["bd"]}]
    print(f"  {len(unique)} feeds ({len(unique) / total:.1%} of the sample)")
    if unique:
        print(f"  <link> or <itunes:owner> already yields the same domain: "
              f"{len(covered)} / {len(unique)} = {len(covered) / len(unique):.1%}")
        print("  NOT covered — inspect these by hand, they are the whole case for "
              "keeping the feed-URL route:")
        for record in unique:
            if record in covered:
                continue
            print(f"     feed={record['fd']:28s} link={record['ld']} owner={record['od']}")

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="cache + output directory")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="report from the cache without touching the network")
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)

    feeds = collect_feed_urls(args.out)
    print(f"feed URLs: {len(feeds)}")
    heads_path = os.path.join(args.out, "heads.json")
    if args.skip_fetch and os.path.exists(heads_path):
        with open(heads_path, encoding="utf-8") as handle:
            heads = json.load(handle)
    else:
        heads = fetch_heads(feeds, args.out)
        with open(heads_path, "w", encoding="utf-8") as handle:
            json.dump(heads, handle)

    records = []
    unparsable = 0
    for feed_url, path in heads.items():
        fields = extract(path)
        if fields is None:
            unparsable += 1
            continue
        records.append({
            "feed": feed_url,
            "fd": etld1(feed_url),
            "link": fields["link"],
            "ld": etld1(fields["link"]),
            "imgld": etld1(fields["imageLink"]),
            "owner": fields["owner"],
            "od": etld1(fields["owner"]),
            "bd": etld1(fields["bareEmail"]),
        })
    with open(os.path.join(args.out, "survey.json"), "w", encoding="utf-8") as handle:
        json.dump(records, handle, indent=1)
    print(f"unparsable / not a feed: {unparsable}\n")
    report(records)

if __name__ == "__main__":
    main()
