#!/usr/bin/env bash
# Audit how far analysis actually got on a device's library.
#
# Usage: scripts/coverage-audit.sh <path-to-analysis.sqlite>
#
# Pull the database off a dogfood device first (the app must not be running --
# it holds the DB open):
#   xcrun devicectl device copy from --device <udid> \
#     --source Library/Application\ Support/analysis.sqlite --destination /tmp/analysis.sqlite
#
# Why this exists: on 2026-07-28 a dogfood audit found that 22 of 26 episodes
# over 15 minutes had less than 25% of their audio semantically scanned for
# ads, while three were marked completeFull having scanned 3-40%. Detection
# quality work is multiplied by this number, so measure it after every change
# rather than re-deriving it by hand. See playhead-gqx4 and playhead-i7qe.

set -euo pipefail

DB=${1:-}
if [[ -z $DB ]]; then
  echo "usage: $0 <path-to-analysis.sqlite>" >&2
  exit 64
fi
if [[ ! -r $DB ]]; then
  echo "cannot read database: $DB" >&2
  exit 66
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "sqlite3 not found on PATH" >&2
  exit 70
fi

# A short episode may legitimately have no ad break, and the tiny-duration rows
# are where the corrupt-watermark cases cluster, so hold the headline numbers to
# episodes long enough to carry a real ad load.
MIN_DURATION=${COVERAGE_AUDIT_MIN_DURATION:-900}

echo "=== per-episode reach (episodes over ${MIN_DURATION}s) ==="
sqlite3 -header -column "$DB" "
SELECT substr(a.id,1,8) AS asset,
       round(a.episodeDurationSec) AS dur,
       round(100.0*a.fastTranscriptCoverageEndTime/a.episodeDurationSec) AS txPct,
       round(100.0*(SELECT max(s.windowEndTime) FROM semantic_scan_results s
                    WHERE s.analysisAssetId=a.id)/a.episodeDurationSec) AS scanPct,
       (SELECT group_concat(DISTINCT s.scanPass) FROM semantic_scan_results s
        WHERE s.analysisAssetId=a.id) AS passes,
       a.analysisState AS state
FROM analysis_assets a
WHERE a.episodeDurationSec > $MIN_DURATION
ORDER BY scanPct IS NULL DESC, scanPct ASC;"

echo
echo "=== headline: how much of the library was actually scanned for ads ==="
sqlite3 -header -column "$DB" "
SELECT count(*) AS episodes,
       sum(scanPct >= 95) AS fullyScanned,
       sum(scanPct IS NULL OR scanPct < 25) AS under25pct,
       round(avg(coalesce(scanPct,0))) AS meanScanPct
FROM (SELECT (SELECT 100.0*max(s.windowEndTime)/a.episodeDurationSec
              FROM semantic_scan_results s WHERE s.analysisAssetId=a.id) AS scanPct
      FROM analysis_assets a WHERE a.episodeDurationSec > $MIN_DURATION);"

echo
echo "=== CLEAN terminals that under-scanned (playhead-gqx4) ==="
echo "An asset here claims a full analysis it did not perform."
echo "AFTER the gqx4 fix this set should be EMPTY for newly-analysed episodes:"
echo "the clean terminal now requires measured ad-scan coverage. Rows that"
echo "predate the fix are left terminal deliberately and still show up here."
sqlite3 -header -column "$DB" "
SELECT substr(a.id,1,8) AS asset,
       round(a.episodeDurationSec) AS dur,
       round(100.0*(SELECT max(s.windowEndTime) FROM semantic_scan_results s
                    WHERE s.analysisAssetId=a.id)/a.episodeDurationSec) AS scanPct,
       a.terminalReason
FROM analysis_assets a
WHERE a.analysisState IN ('completeFull','complete')
  AND coalesce((SELECT 100.0*max(s.windowEndTime)/a.episodeDurationSec
                FROM semantic_scan_results s WHERE s.analysisAssetId=a.id), 0) < 95
ORDER BY scanPct;"

echo
echo "=== HONEST degraded terminals (playhead-gqx4, post-fix) ==="
echo "The fix's own output. A growing set here is the fix working, not a"
echo "regression — these rows used to be indistinguishable from a full"
echo "analysis. terminalReason carries the AdScanLimit cause token."
sqlite3 -header -column "$DB" "
SELECT substr(a.id,1,8) AS asset,
       round(a.episodeDurationSec) AS dur,
       round(100.0*(SELECT max(s.windowEndTime) FROM semantic_scan_results s
                    WHERE s.analysisAssetId=a.id)/a.episodeDurationSec) AS scanPct,
       a.terminalReason
FROM analysis_assets a
WHERE a.analysisState = 'completeAdScanPartial'
ORDER BY scanPct;"

echo
echo "=== stalled: transcript complete, scan incomplete, NOT terminal (playhead-i7qe) ==="
echo "Non-terminal only — a degraded terminal is a different (honest) state"
echo "and is listed in its own section above."
sqlite3 -header -column "$DB" "
SELECT substr(a.id,1,8) AS asset,
       round(a.episodeDurationSec) AS dur,
       round(100.0*(SELECT max(s.windowEndTime) FROM semantic_scan_results s
                    WHERE s.analysisAssetId=a.id)/a.episodeDurationSec) AS scanPct,
       a.analysisState AS state
FROM analysis_assets a
WHERE a.fastTranscriptCoverageEndTime >= 0.98*a.episodeDurationSec
  AND a.analysisState NOT IN (
        'complete', 'completeFull', 'completeFeatureOnly',
        'completeTranscriptPartial', 'completeAdScanPartial'
      )
  AND coalesce((SELECT 100.0*max(s.windowEndTime)/a.episodeDurationSec
                FROM semantic_scan_results s WHERE s.analysisAssetId=a.id), 0) < 95
ORDER BY dur DESC;"

echo
echo "=== impossible coverage: watermark claims more than the episode holds (playhead-csbq) ==="
sqlite3 -header -column "$DB" "
SELECT substr(a.id,1,8) AS asset,
       round(a.episodeDurationSec) AS dur,
       round(a.fastTranscriptCoverageEndTime) AS txEnd,
       round(a.fastTranscriptCoverageEndTime/a.episodeDurationSec, 2) AS ratio,
       a.terminalReason
FROM analysis_assets a
WHERE a.episodeDurationSec > 0
  AND a.fastTranscriptCoverageEndTime > 1.05*a.episodeDurationSec
ORDER BY ratio DESC;"

echo
echo "=== FM throughput by scene phase (playhead-hx6n) ==="
if sqlite3 "$DB" "PRAGMA table_info(semantic_scan_results);" | grep -q '|scenePhase|'; then
  # The measurement playhead-kvs8 was asked for and could not make: the
  # 2.4x-slower-than-realtime figure split by the phase each scan COMPLETED in.
  # ratio > 1 is slower than realtime.
  #
  # `unattributed` is a real row, not a rounding error. It is every scan written
  # before schema V42 plus any whose phase the platform declined to name, and
  # they are genuinely unattributable — no backfill can honestly repair them.
  # READ IT FIRST: a split over a corpus that is 95% unattributed is not a
  # foreground-versus-background finding, it is a report that the corpus
  # predates the instrumentation.
  #
  # `noWork:` sentinels are excluded (playhead-pz32): a sentinel spans a range
  # it never examined, so its ~zero latency over a whole-episode window would
  # report a model of spectacular speed that never ran.
  sqlite3 -header -column "$DB" "
  SELECT CASE
           WHEN scenePhase IN ('active','inactive') THEN 'foreground'
           WHEN scenePhase = 'background'           THEN 'background'
           ELSE 'unattributed'
         END                                             AS phase,
         count(*)                                        AS scans,
         round(sum(windowEndTime - windowStartTime))      AS audioSec,
         round(sum(latencyMs)/1000.0)                     AS wallSec,
         round((sum(latencyMs)/1000.0)
               / nullif(sum(windowEndTime - windowStartTime),0), 2) AS ratio
  FROM semantic_scan_results
  WHERE status='success'
    AND latencyMs IS NOT NULL
    AND windowEndTime > windowStartTime
    AND (errorContext IS NULL OR errorContext NOT LIKE 'noWork:%')
  GROUP BY phase
  ORDER BY phase;"
  echo
  echo "  (raw scene phases, so the inactive-counts-as-foreground call stays auditable)"
  sqlite3 -header -column "$DB" "
  SELECT coalesce(scenePhase,'(null: pre-V42)') AS rawPhase, count(*) AS scans
  FROM semantic_scan_results
  WHERE status='success' AND latencyMs IS NOT NULL
    AND windowEndTime > windowStartTime
    AND (errorContext IS NULL OR errorContext NOT LIKE 'noWork:%')
  GROUP BY rawPhase ORDER BY scans DESC;"
else
  echo "  This database predates schema V42 — semantic_scan_results has no"
  echo "  scenePhase column, so NO row in it can be attributed to a phase."
  echo "  That is the honest answer, not a script failure: the attribution has"
  echo "  to be recorded at write time and this binary never recorded it."
fi

echo
echo "=== windows surfaced with no evidence at all (playhead-8x59) ==="
echo "Shard-quantized bounds plus empty evidence means no verifier ever ran."
sqlite3 -header -column "$DB" "
SELECT count(*) AS noEvidenceWindows,
       sum(CAST(startTime AS INTEGER) % 30 = 0
           AND CAST(endTime AS INTEGER) % 30 = 0) AS shardQuantized,
       round(min(confidence),2) AS minConf,
       round(max(confidence),2) AS maxConf
FROM ad_windows
WHERE coalesce(evidenceText,'')='' AND coalesce(metadataSource,'none')='none';"
