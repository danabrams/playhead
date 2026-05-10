#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$(mktemp -d "${TMPDIR:-/tmp}/playhead-l2f-promote-test.XXXXXX")"
trap 'rm -rf "$OUT"' EXIT

cd "$ROOT"

make_wav() {
  local path="$1"
  local seconds="$2"
  python3 - "$path" "$seconds" <<'PY'
import struct
import sys
import wave

path = sys.argv[1]
seconds = float(sys.argv[2])
rate = 8000
frames = int(rate * seconds)
with wave.open(path, "wb") as handle:
    handle.setnchannels(1)
    handle.setsampwidth(2)
    handle.setframerate(rate)
    silence = struct.pack("<h", 0)
    handle.writeframes(silence * frames)
PY
}

mkdir -p "$OUT/audio" "$OUT/annotations"
make_wav "$OUT/audio/episode-two-ads.wav" 100
make_wav "$OUT/audio/episode-zero.wav" 80

cat > "$OUT/two-ads-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-two-ads#1",
      "episode_id": "episode-two-ads",
      "candidate_index": 1,
      "start_seconds": 10,
      "end_seconds": 20,
      "ad_type": "host_read",
      "transition_type": "explicit",
      "advertiser_guess": "Acme",
      "product_guess": "Widgets",
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    },
    {
      "id": "episode-two-ads#2",
      "episode_id": "episode-two-ads",
      "candidate_index": 2,
      "start_seconds": 30,
      "end_seconds": 40,
      "ad_type": "dynamic_insertion",
      "transition_type": "hard_cut",
      "advertiser_guess": "Rorra",
      "product_guess": "Water",
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/two-ads-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/two-ads-queue.json",
  "reviews": {
    "episode-two-ads#1": {
      "status": "verified_ad",
      "show_name": "Fixture Show",
      "start_seconds": 10,
      "end_seconds": 20,
      "advertiser": "Acme",
      "product": "Widgets",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "boundary_confidence": "high",
      "notes": "Clear sponsor intro."
    },
    "episode-two-ads#2": {
      "status": "verified_ad",
      "show_name": "Fixture Show",
      "start_seconds": 30,
      "end_seconds": 40,
      "advertiser": "Rorra",
      "product": "Water",
      "ad_type": "dynamic_insertion",
      "transition_type": "hard_cut",
      "boundary_confidence": "medium",
      "notes": "Inserted mid-roll."
    }
  }
}
JSON

python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/two-ads-review.json" \
  --annotations-dir "$OUT/annotations" \
  --audio-dir "$OUT/audio" >/tmp/playhead-l2f-promote-two-ads.out

test "$(jq '.ad_windows | length' "$OUT/annotations/episode-two-ads.json")" = "2"
test "$(jq '.content_windows | length' "$OUT/annotations/episode-two-ads.json")" = "3"
jq -e '.content_windows[0].start_seconds == 0' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[0].end_seconds == 10' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[1].start_seconds == 20' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[1].end_seconds == 30' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[2].start_seconds == 40' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[2].end_seconds == 100' "$OUT/annotations/episode-two-ads.json" >/dev/null
expected_fp="$(python3 - "$OUT/audio/episode-two-ads.wav" <<'PY'
import hashlib
import sys
print("sha256:" + hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())
PY
)"
test "$(jq -r '.audio_fingerprint' "$OUT/annotations/episode-two-ads.json")" = "$expected_fp"

cat > "$OUT/strict-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-unreviewed#1",
      "episode_id": "episode-unreviewed",
      "candidate_index": 1,
      "start_seconds": 1,
      "end_seconds": 2,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    },
    {
      "id": "episode-unsure#1",
      "episode_id": "episode-unsure",
      "candidate_index": 1,
      "start_seconds": 1,
      "end_seconds": 2,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    },
    {
      "id": "episode-missing-metadata#1",
      "episode_id": "episode-missing-metadata",
      "candidate_index": 1,
      "start_seconds": 1,
      "end_seconds": 2,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    },
    {
      "id": "episode-missing-audio#1",
      "episode_id": "episode-missing-audio",
      "candidate_index": 1,
      "start_seconds": 1,
      "end_seconds": 2,
      "audio_path": "$OUT/audio/does-not-exist.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/strict-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/strict-queue.json",
  "reviews": {
    "episode-unsure#1": {
      "status": "unsure"
    },
    "episode-missing-metadata#1": {
      "status": "verified_ad",
      "start_seconds": 1,
      "end_seconds": 2,
      "advertiser": "Acme",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Missing product should block."
    },
    "episode-missing-audio#1": {
      "status": "verified_ad",
      "start_seconds": 1,
      "end_seconds": 2,
      "advertiser": "Acme",
      "product": "Widgets",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Audio missing should block."
    }
  }
}
JSON

python3 scripts/l2f-promote-reviewed-corpus.py \
  --review-file "$OUT/missing-review-file.json" \
  --queue "$OUT/strict-queue.json" \
  --annotations-dir "$OUT/strict-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/missing-review-report.out"
grep -q "review_file_status: missing" "$OUT/missing-review-report.out"
grep -q "unreviewed:" "$OUT/missing-review-report.out"

if python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/strict-review.json" \
  --annotations-dir "$OUT/strict-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/strict-report.out" 2>"$OUT/strict-report.err"; then
  echo "expected strict promotion to refuse blocked entries" >&2
  exit 1
fi
grep -q "unreviewed:" "$OUT/strict-report.out"
grep -q "unsure:" "$OUT/strict-report.out"
grep -q "missing_ad_metadata:" "$OUT/strict-report.out"
grep -q "missing_audio:" "$OUT/strict-report.out"
test ! -e "$OUT/strict-annotations/episode-missing-metadata.json"

cat > "$OUT/zero-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-zero#1",
      "episode_id": "episode-zero",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-zero.wav",
      "false_positive_trap": true
    }
  ]
}
JSON

cat > "$OUT/zero-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/zero-queue.json",
  "reviews": {
    "episode-zero#1": {
      "status": "zero_ad_confirmed",
      "show_name": "Zero Show",
      "notes": "Listened through the trap sample; no ad."
    }
  }
}
JSON

python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/zero-review.json" \
  --annotations-dir "$OUT/zero-annotations" \
  --audio-dir "$OUT/audio" >/tmp/playhead-l2f-promote-zero.out
test "$(jq '.ad_windows | length' "$OUT/zero-annotations/episode-zero.json")" = "0"
test "$(jq '.content_windows | length' "$OUT/zero-annotations/episode-zero.json")" = "1"
jq -e '.content_windows[0].start_seconds == 0' "$OUT/zero-annotations/episode-zero.json" >/dev/null
jq -e '.content_windows[0].end_seconds == 80' "$OUT/zero-annotations/episode-zero.json" >/dev/null

cat > "$OUT/overlap-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-overlap#1",
      "episode_id": "episode-overlap",
      "candidate_index": 1,
      "duration_seconds": 50,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    },
    {
      "id": "episode-overlap#2",
      "episode_id": "episode-overlap",
      "candidate_index": 2,
      "duration_seconds": 50,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    },
    {
      "id": "episode-invalid#1",
      "episode_id": "episode-invalid",
      "candidate_index": 1,
      "duration_seconds": 50,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/overlap-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/overlap-queue.json",
  "reviews": {
    "episode-overlap#1": {
      "status": "verified_ad",
      "start_seconds": 10,
      "end_seconds": 25,
      "advertiser": "A",
      "product": "A",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "First ad."
    },
    "episode-overlap#2": {
      "status": "verified_ad",
      "start_seconds": 20,
      "end_seconds": 30,
      "advertiser": "B",
      "product": "B",
      "ad_type": "promo",
      "transition_type": "hard_cut",
      "notes": "Overlapping ad."
    },
    "episode-invalid#1": {
      "status": "verified_ad",
      "start_seconds": 45,
      "end_seconds": 44,
      "advertiser": "C",
      "product": "C",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Invalid timing."
    }
  }
}
JSON

if python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/overlap-review.json" \
  --annotations-dir "$OUT/overlap-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/overlap-report.out" 2>"$OUT/overlap-report.err"; then
  echo "expected overlap/invalid timing promotion to fail" >&2
  exit 1
fi
grep -q "overlap:" "$OUT/overlap-report.out"
grep -q "invalid_timing:" "$OUT/overlap-report.out"
test ! -e "$OUT/overlap-annotations/episode-overlap.json"
test ! -e "$OUT/overlap-annotations/episode-invalid.json"

echo "l2f promote reviewed corpus tests passed"
