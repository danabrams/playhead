#!/usr/bin/env python3
"""playhead-04rx: export a device analysis.sqlite into the JSON the
RepeatedOccurrenceAnchoringCorpusEvalTests harness reads.

Exports, per analysis asset: every transcript_chunk row (all passes, RAW —
the harness canonicalizes with production TranscriptChunkCanonicalizer), plus
the persisted ad_windows and correction_events so an anchored occurrence can be
adjudicated against what the device itself believed.
"""
import argparse, json, sqlite3, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db")
    ap.add_argument("out")
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row

    assets = {}
    for r in con.execute("SELECT id, episodeTitle, episodeDurationSec, sourceURL FROM analysis_assets"):
        assets[r["id"]] = {
            "assetId": r["id"],
            "title": r["episodeTitle"],
            "durationSec": r["episodeDurationSec"],
            "sourceURL": r["sourceURL"],
            "chunks": [], "windows": [], "corrections": [],
        }

    nchunks = 0
    for r in con.execute(
        "SELECT id, analysisAssetId, segmentFingerprint, chunkIndex, startTime, endTime, "
        "text, normalizedText, pass, modelVersion, transcriptVersion, atomOrdinal, speakerId, avgConfidence "
        "FROM transcript_chunks"
    ):
        a = assets.get(r["analysisAssetId"])
        if a is None:
            a = assets.setdefault(r["analysisAssetId"], {
                "assetId": r["analysisAssetId"], "title": None, "durationSec": None,
                "sourceURL": None, "chunks": [], "windows": [], "corrections": []})
        a["chunks"].append({
            "id": r["id"],
            "segmentFingerprint": r["segmentFingerprint"],
            "chunkIndex": r["chunkIndex"],
            "startTime": r["startTime"],
            "endTime": r["endTime"],
            "text": r["text"],
            "normalizedText": r["normalizedText"],
            "pass": r["pass"],
            "modelVersion": r["modelVersion"],
            "transcriptVersion": r["transcriptVersion"],
            "atomOrdinal": r["atomOrdinal"],
            "speakerId": r["speakerId"],
            "avgConfidence": r["avgConfidence"],
        })
        nchunks += 1

    for r in con.execute(
        "SELECT analysisAssetId, startTime, endTime, confidence, boundaryState, decisionState, "
        "detectorVersion, advertiser, evidenceSources, eligibilityGate, startEdgeAnchor, endEdgeAnchor "
        "FROM ad_windows"
    ):
        a = assets.get(r["analysisAssetId"])
        if a is None: continue
        a["windows"].append({k: r[k] for k in r.keys() if k != "analysisAssetId"})

    cols = {c[1] for c in con.execute("PRAGMA table_info(correction_events)")}
    if "analysisAssetId" in cols:
        sel = ", ".join(sorted(cols))
        for r in con.execute(f"SELECT {sel} FROM correction_events"):
            a = assets.get(r["analysisAssetId"])
            if a is None: continue
            a["corrections"].append({k: r[k] for k in r.keys()})

    out = {"source": args.db,
           "assets": [a for a in assets.values() if a["chunks"]]}
    with open(args.out, "w") as f:
        json.dump(out, f)
    print(f"assets_with_chunks={len(out['assets'])} chunks={nchunks} -> {args.out}", file=sys.stderr)

main()
