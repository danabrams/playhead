#!/usr/bin/env python3
"""Export a device pull into the JSON corpus that
`MetadataShowOwnedDomainDevicePullEvalTests` (playhead-kmw4) reads.

The eval measures what removing the show-notes-frequency -> `.showOwned`
promotion does to a real device's ad-detection pipeline. It needs two things
the repo does not carry:

  * every episode's RSS description / summary for BOTH subscribed shows, which
    is what the ownership graph is built from (`Playhead.store`, SwiftData —
    NOT `analysis.sqlite`; see the 2026-08-15 note that the library is a second
    database); and
  * the transcript chunks and decoded spans of the analysed assets, which is
    what the lexical scanner and the auto-ad rule actually run on
    (`analysis.sqlite`).

Usage:

    python3 scripts/kmw4-export-device-corpus.py \\
        --pull /Users/dabrams/playhead-gate-artifacts/device-pulls/2026-08-18-t3/work \\
        --out  /Users/dabrams/playhead-gate-artifacts/kmw4/corpus-2026-08-18-t3.json

Then:

    PLAYHEAD_KMW4_CORPUS=<out> scripts/fast-gate.sh \\
        -only-testing:PlayheadTests/MetadataShowOwnedDomainDevicePullEvalTests

ALWAYS point `--pull` at the `work/` copy of a pull, never `db/`: `db/` is the
read-only evidence copy, and the 2026-08-18-t3 one holds a deliberately
truncated `analysis.sqlite.TRUNCATED-20000000B-DO-NOT-USE`.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys


def _connect(path: str) -> sqlite3.Connection:
    if not os.path.exists(path):
        sys.exit(f"missing: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    # Device text is user content; never let a stray byte abort the export.
    conn.text_factory = lambda b: b.decode("utf-8", "replace")
    return conn


def export(pull_dir: str) -> dict:
    store = _connect(os.path.join(pull_dir, "Playhead.store"))
    analysis = _connect(os.path.join(pull_dir, "analysis.sqlite"))

    shows = []
    podcast_pk_to_index = {}
    for index, (pk, title, feed_url) in enumerate(
        store.execute("SELECT Z_PK, ZTITLE, ZFEEDURL FROM ZPODCAST ORDER BY Z_PK")
    ):
        podcast_pk_to_index[pk] = index
        episodes = []
        for key, desc, summ in store.execute(
            "SELECT ZCANONICALEPISODEKEY, ZFEEDDESCRIPTION, ZFEEDSUMMARY "
            "FROM ZEPISODE WHERE ZPODCAST = ? ORDER BY Z_PK",
            (pk,),
        ):
            # `compactMap(\\.feedMetadata)` drops episodes with no metadata at
            # all; a row with both text fields NULL contributes no domain
            # either way, so the two are equivalent for this measurement.
            episodes.append(
                {
                    "canonicalEpisodeKey": key or "",
                    "feedDescription": desc,
                    "feedSummary": summ,
                }
            )
        shows.append(
            {
                "title": title,
                "feedURL": feed_url,
                # `PlayheadApp` uses the feed URL string as the podcastId.
                "podcastId": feed_url,
                "episodes": episodes,
            }
        )

    # canonicalEpisodeKey -> podcast index, for attaching an asset to its show.
    key_to_show = {}
    for pk, index in podcast_pk_to_index.items():
        for (key,) in store.execute(
            "SELECT ZCANONICALEPISODEKEY FROM ZEPISODE WHERE ZPODCAST = ?", (pk,)
        ):
            if key:
                key_to_show[key] = index
    # And the episode's own metadata, which is what cue extraction reads.
    key_to_metadata = {
        key: {"feedDescription": desc, "feedSummary": summ}
        for key, desc, summ in store.execute(
            "SELECT ZCANONICALEPISODEKEY, ZFEEDDESCRIPTION, ZFEEDSUMMARY FROM ZEPISODE"
        )
        if key
    }

    assets = []
    for asset_id, episode_id, episode_title in analysis.execute(
        "SELECT id, episodeId, episodeTitle FROM analysis_assets ORDER BY id"
    ):
        metadata = key_to_metadata.get(episode_id)
        chunks = [
            {
                "id": cid,
                "chunkIndex": chunk_index,
                "startTime": start,
                "endTime": end,
                "text": text,
                "normalizedText": normalized,
                "pass": pass_,
                "atomOrdinal": atom_ordinal,
            }
            for cid, chunk_index, start, end, text, normalized, pass_, atom_ordinal in analysis.execute(
                "SELECT id, chunkIndex, startTime, endTime, text, normalizedText, pass, atomOrdinal "
                "FROM transcript_chunks WHERE analysisAssetId = ? ORDER BY startTime, chunkIndex",
                (asset_id,),
            )
        ]
        spans = [
            {
                "id": sid,
                "firstAtomOrdinal": first,
                "lastAtomOrdinal": last,
                "startTime": start,
                "endTime": end,
            }
            for sid, first, last, start, end in analysis.execute(
                "SELECT id, firstAtomOrdinal, lastAtomOrdinal, startTime, endTime "
                "FROM decoded_spans WHERE assetId = ? ORDER BY startTime",
                (asset_id,),
            )
        ]
        assets.append(
            {
                "id": asset_id,
                "episodeId": episode_id,
                "episodeTitle": episode_title,
                "showIndex": key_to_show.get(episode_id, -1),
                "feedDescription": (metadata or {}).get("feedDescription"),
                "feedSummary": (metadata or {}).get("feedSummary"),
                "chunks": chunks,
                "spans": spans,
            }
        )

    return {
        "source": os.path.abspath(pull_dir),
        "shows": shows,
        "assets": assets,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pull", required=True, help="a device pull's work/ directory")
    parser.add_argument("--out", required=True, help="JSON path to write")
    args = parser.parse_args()

    corpus = export(args.pull)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(corpus, handle)

    episodes = sum(len(show["episodes"]) for show in corpus["shows"])
    chunks = sum(len(asset["chunks"]) for asset in corpus["assets"])
    spans = sum(len(asset["spans"]) for asset in corpus["assets"])
    size = os.path.getsize(args.out)
    print(
        f"wrote {args.out}: {len(corpus['shows'])} shows / {episodes} episodes / "
        f"{len(corpus['assets'])} assets / {chunks} chunks / {spans} spans "
        f"({size / 1e6:.1f} MB)"
    )


if __name__ == "__main__":
    main()
