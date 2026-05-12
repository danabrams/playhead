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
make_wav "$OUT/audio/episode-short.wav" 50

cat > "$OUT/two-ads-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-two-ads#1",
      "episode_id": "episode-two-ads",
      "candidate_index": 1,
      "duration_seconds": 50,
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
      "duration_seconds": 50,
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
  --audio-dir "$OUT/audio" >"$OUT/two-ads-promote.out"

test "$(jq '.ad_windows | length' "$OUT/annotations/episode-two-ads.json")" = "2"
test "$(jq '.content_windows | length' "$OUT/annotations/episode-two-ads.json")" = "3"
jq -e 'keys == [
  "ad_windows",
  "audio_fingerprint",
  "content_windows",
  "duration_seconds",
  "episode_id",
  "show_name",
  "variant_of"
]' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[0].start_seconds == 0' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[0].end_seconds == 10' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[1].start_seconds == 20' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[1].end_seconds == 30' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[2].start_seconds == 40' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.content_windows[2].end_seconds == 100' "$OUT/annotations/episode-two-ads.json" >/dev/null
jq -e '.duration_seconds == 100' "$OUT/annotations/episode-two-ads.json" >/dev/null
expected_fp="$(python3 - "$OUT/audio/episode-two-ads.wav" <<'PY'
import hashlib
import sys
print("sha256:" + hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())
PY
)"
test "$(jq -r '.audio_fingerprint' "$OUT/annotations/episode-two-ads.json")" = "$expected_fp"

mkdir -p "$OUT/relative/audio" "$OUT/relative/annotations"
make_wav "$OUT/relative/audio/episode-relative.wav" 12
cat > "$OUT/relative/queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-relative#1",
      "episode_id": "episode-relative",
      "candidate_index": 1,
      "start_seconds": 2,
      "end_seconds": 4,
      "ad_type": "promo",
      "transition_type": "musical",
      "advertiser_guess": "Relco",
      "product_guess": "Paths",
      "audio_path": "audio/episode-relative.wav",
      "false_positive_trap": false
    }
  ]
}
JSON
cat > "$OUT/relative/review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "queue.json",
  "reviews": {
    "episode-relative#1": {
      "status": "verified_ad",
      "show_name": "Relative Path Show",
      "start_seconds": 2,
      "end_seconds": 4,
      "advertiser": "Relco",
      "product": "Paths",
      "ad_type": "promo",
      "transition_type": "musical",
      "notes": "Relative queue and audio paths should resolve from their JSON files."
    }
  }
}
JSON
python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/relative/review.json" \
  --annotations-dir "$OUT/relative/annotations" \
  --audio-dir "$OUT/audio" >"$OUT/relative/promote.out"
test "$(jq -r '.show_name' "$OUT/relative/annotations/episode-relative.json")" = "Relative Path Show"
jq -e '.duration_seconds == 12' "$OUT/relative/annotations/episode-relative.json" >/dev/null

python3 scripts/l2f-promote-reviewed-corpus.py \
  --review-file "$OUT/two-ads-review.json" \
  --annotations-dir "$OUT/annotations" \
  --audio-dir "$OUT/audio" >"$OUT/existing-annotation-dry-run.out"
grep -q "BLOCKED episode-two-ads" "$OUT/existing-annotation-dry-run.out"
grep -q "annotation_exists:" "$OUT/existing-annotation-dry-run.out"
grep -q "ready_episodes=0 skipped_episodes=0 blocked_episodes=1" "$OUT/existing-annotation-dry-run.out"

if python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/two-ads-review.json" \
  --annotations-dir "$OUT/annotations-unknown-episode" \
  --audio-dir "$OUT/audio" \
  --episode "does-not-exist" >"$OUT/unknown-episode.out" 2>"$OUT/unknown-episode.err"; then
  echo "expected unknown --episode promotion to fail" >&2
  exit 1
fi
grep -q "episode_not_found:" "$OUT/unknown-episode.err"

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
      "show_name": "Fixture Show",
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

cat > "$OUT/malformed-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-well-formed#1",
      "episode_id": "episode-well-formed",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    },
    "not an entry object"
  ]
}
JSON

if python3 scripts/l2f-promote-reviewed-corpus.py \
  --review-file "$OUT/missing-review-file.json" \
  --queue "$OUT/malformed-queue.json" \
  --annotations-dir "$OUT/malformed-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/malformed-queue.out" 2>"$OUT/malformed-queue.err"; then
  echo "expected malformed queue entry to fail instead of being silently skipped" >&2
  exit 1
fi
grep -q "entries\\[1\\] must be a JSON object" "$OUT/malformed-queue.err"

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

cat > "$OUT/boolean-timing-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-boolean-timing#1",
      "episode_id": "episode-boolean-timing",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/boolean-timing-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/boolean-timing-queue.json",
  "reviews": {
    "episode-boolean-timing#1": {
      "status": "verified_ad",
      "show_name": "Fixture Show",
      "start_seconds": true,
      "end_seconds": 2,
      "advertiser": "Acme",
      "product": "Widgets",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Boolean timing must not coerce to 1.0."
    }
  }
}
JSON

if python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/boolean-timing-review.json" \
  --annotations-dir "$OUT/boolean-timing-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/boolean-timing.out" 2>"$OUT/boolean-timing.err"; then
  echo "expected boolean timing promotion to fail" >&2
  exit 1
fi
grep -q "missing_ad_metadata: episode-boolean-timing#1 missing start_seconds" "$OUT/boolean-timing.out"
test ! -e "$OUT/boolean-timing-annotations/episode-boolean-timing.json"

cat > "$OUT/boolean-string-metadata-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-boolean-string-metadata#1",
      "episode_id": "episode-boolean-string-metadata",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/boolean-string-metadata-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/boolean-string-metadata-queue.json",
  "reviews": {
    "episode-boolean-string-metadata#1": {
      "status": "verified_ad",
      "show_name": "Fixture Show",
      "start_seconds": 1,
      "end_seconds": 2,
      "advertiser": true,
      "product": "Widgets",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Boolean string metadata must not stringify to True."
    }
  }
}
JSON

if python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/boolean-string-metadata-review.json" \
  --annotations-dir "$OUT/boolean-string-metadata-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/boolean-string-metadata.out" 2>"$OUT/boolean-string-metadata.err"; then
  echo "expected boolean string metadata promotion to fail" >&2
  exit 1
fi
grep -q "missing_ad_metadata: episode-boolean-string-metadata#1 missing advertiser" "$OUT/boolean-string-metadata.out"
test ! -e "$OUT/boolean-string-metadata-annotations/episode-boolean-string-metadata.json"

cat > "$OUT/missing-show-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-missing-show#1",
      "episode_id": "episode-missing-show",
      "candidate_index": 1,
      "start_seconds": 1,
      "end_seconds": 2,
      "ad_type": "host_read",
      "transition_type": "explicit",
      "advertiser_guess": "Acme",
      "product_guess": "Widgets",
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/missing-show-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/missing-show-queue.json",
  "reviews": {
    "episode-missing-show#1": {
      "status": "verified_ad",
      "start_seconds": 1,
      "end_seconds": 2,
      "advertiser": "Acme",
      "product": "Widgets",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Show name should be real corpus metadata."
    }
  }
}
JSON

if python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/missing-show-review.json" \
  --annotations-dir "$OUT/missing-show-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/missing-show.out" 2>"$OUT/missing-show.err"; then
  echo "expected missing show_name promotion to fail" >&2
  exit 1
fi
grep -q "missing_show_name:" "$OUT/missing-show.out"
test ! -e "$OUT/missing-show-annotations/episode-missing-show.json"

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
  --audio-dir "$OUT/audio" >"$OUT/zero-promote.out"
test "$(jq '.ad_windows | length' "$OUT/zero-annotations/episode-zero.json")" = "0"
test "$(jq '.content_windows | length' "$OUT/zero-annotations/episode-zero.json")" = "1"
jq -e '.content_windows[0].start_seconds == 0' "$OUT/zero-annotations/episode-zero.json" >/dev/null
jq -e '.content_windows[0].end_seconds == 80' "$OUT/zero-annotations/episode-zero.json" >/dev/null

cat > "$OUT/zero-false-positive-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/zero-queue.json",
  "reviews": {
    "episode-zero#1": {
      "status": "false_positive",
      "show_name": "Zero Show",
      "notes": "Trap candidate was reviewed and rejected as a false positive."
    }
  }
}
JSON

python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/zero-false-positive-review.json" \
  --annotations-dir "$OUT/zero-false-positive-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/zero-false-positive-promote.out"
test "$(jq '.ad_windows | length' "$OUT/zero-false-positive-annotations/episode-zero.json")" = "0"
test "$(jq '.content_windows | length' "$OUT/zero-false-positive-annotations/episode-zero.json")" = "1"

cat > "$OUT/string-false-trap-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-string-false-trap#1",
      "episode_id": "episode-string-false-trap",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-zero.wav",
      "false_positive_trap": "false"
    }
  ]
}
JSON

cat > "$OUT/string-false-trap-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/string-false-trap-queue.json",
  "reviews": {
    "episode-string-false-trap#1": {
      "status": "false_positive",
      "show_name": "Malformed Trap Flag Show",
      "notes": "String false must not count as a reviewed trap."
    }
  }
}
JSON

python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/string-false-trap-review.json" \
  --annotations-dir "$OUT/string-false-trap-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/string-false-trap.out" 2>"$OUT/string-false-trap.err"
grep -q "SKIPPED episode-string-false-trap" "$OUT/string-false-trap.out"
grep -q "false_positive_only:" "$OUT/string-false-trap.out"
test ! -e "$OUT/string-false-trap-annotations/episode-string-false-trap.json"

cat > "$OUT/non-trap-false-positive-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-non-trap-false-positive#1",
      "episode_id": "episode-non-trap-false-positive",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-zero.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/non-trap-false-positive-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/non-trap-false-positive-queue.json",
  "reviews": {
    "episode-non-trap-false-positive#1": {
      "status": "false_positive",
      "show_name": "Not A Trap Show",
      "notes": "A rejected ordinary candidate should not imply full-episode no-ad truth."
    }
  }
}
JSON

python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/non-trap-false-positive-review.json" \
  --annotations-dir "$OUT/non-trap-false-positive-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/non-trap-false-positive.out" 2>"$OUT/non-trap-false-positive.err"
grep -q "SKIPPED episode-non-trap-false-positive" "$OUT/non-trap-false-positive.out"
grep -q "false_positive_only:" "$OUT/non-trap-false-positive.out"
test ! -e "$OUT/non-trap-false-positive-annotations/episode-non-trap-false-positive.json"

cat > "$OUT/mixed-skipped-ready-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-non-trap-missing-audio-false-positive#1",
      "episode_id": "episode-non-trap-missing-audio-false-positive",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/does-not-exist.wav",
      "false_positive_trap": false
    },
    {
      "id": "episode-two-ads#1",
      "episode_id": "episode-two-ads",
      "candidate_index": 1,
      "duration_seconds": 50,
      "start_seconds": 10,
      "end_seconds": 20,
      "ad_type": "host_read",
      "transition_type": "explicit",
      "advertiser_guess": "Acme",
      "product_guess": "Widgets",
      "audio_path": "$OUT/audio/episode-two-ads.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/mixed-skipped-ready-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/mixed-skipped-ready-queue.json",
  "reviews": {
    "episode-non-trap-missing-audio-false-positive#1": {
      "status": "false_positive",
      "notes": "A rejected ordinary candidate should be skipped even if annotation metadata is unavailable."
    },
    "episode-two-ads#1": {
      "status": "verified_ad",
      "show_name": "Fixture Show",
      "start_seconds": 10,
      "end_seconds": 20,
      "advertiser": "Acme",
      "product": "Widgets",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Ready episode must still promote."
    }
  }
}
JSON

mkdir -p "$OUT/mixed-skipped-ready-annotations"
printf '{"stale": true}\n' \
  >"$OUT/mixed-skipped-ready-annotations/episode-non-trap-missing-audio-false-positive.json"

python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/mixed-skipped-ready-review.json" \
  --annotations-dir "$OUT/mixed-skipped-ready-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/mixed-skipped-ready.out" 2>"$OUT/mixed-skipped-ready.err"
grep -q "SKIPPED episode-non-trap-missing-audio-false-positive" "$OUT/mixed-skipped-ready.out"
test "$(grep -c "missing_audio:" "$OUT/mixed-skipped-ready.out")" = "0"
test "$(grep -c "annotation_exists:" "$OUT/mixed-skipped-ready.out")" = "0"
grep -q "READY episode-two-ads" "$OUT/mixed-skipped-ready.out"
test "$(jq -r '.stale' "$OUT/mixed-skipped-ready-annotations/episode-non-trap-missing-audio-false-positive.json")" = "true"
test -e "$OUT/mixed-skipped-ready-annotations/episode-two-ads.json"
jq -e '.ad_windows | length == 1' "$OUT/mixed-skipped-ready-annotations/episode-two-ads.json" >/dev/null

cat > "$OUT/manual-missed-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-zero#1",
      "episode_id": "episode-zero",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-zero.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/manual-missed-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/manual-missed-queue.json",
  "manual_entries": [
    {
      "id": "manual:episode-zero#1",
      "episode_id": "episode-zero",
      "candidate_index": "M1",
      "manual_index": 1,
      "manual_entry": true,
      "source": "manual_missed_ad",
      "audio_path": "$OUT/audio/episode-zero.wav",
      "false_positive_trap": false
    }
  ],
  "reviews": {
    "episode-zero#1": {
      "status": "false_positive",
      "notes": "Original queue candidate was not an ad."
    },
    "manual:episode-zero#1": {
      "status": "verified_ad",
      "show_name": "Fixture Show",
      "start_seconds": 20,
      "end_seconds": 30,
      "advertiser": "ManualCo",
      "product": "Missed Ad",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Missed ad added manually."
    }
  }
}
JSON

python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/manual-missed-review.json" \
  --annotations-dir "$OUT/manual-missed-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/manual-missed-promote.out"
jq -e '.ad_windows | length == 1' "$OUT/manual-missed-annotations/episode-zero.json" >/dev/null
jq -e '.ad_windows[0].start_seconds == 20' "$OUT/manual-missed-annotations/episode-zero.json" >/dev/null
jq -e '.ad_windows[0].advertiser == "ManualCo"' "$OUT/manual-missed-annotations/episode-zero.json" >/dev/null

cat > "$OUT/tiny-overshoot-queue.json" <<JSON
{
  "schema": "playhead-l2f-review-queue-v1",
  "entries": [
    {
      "id": "episode-tiny-overshoot#1",
      "episode_id": "episode-tiny-overshoot",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-short.wav",
      "false_positive_trap": false
    }
  ]
}
JSON

cat > "$OUT/tiny-overshoot-review.json" <<JSON
{
  "schema": "playhead-l2f-audio-review-v1",
  "queue_path": "$OUT/tiny-overshoot-queue.json",
  "reviews": {
    "episode-tiny-overshoot#1": {
      "status": "verified_ad",
      "show_name": "Fixture Show",
      "start_seconds": 45,
      "end_seconds": 50.04,
      "advertiser": "D",
      "product": "D",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Tiny endpoint drift should promote and clamp to duration."
    }
  }
}
JSON

python3 scripts/l2f-promote-reviewed-corpus.py \
  --promote \
  --review-file "$OUT/tiny-overshoot-review.json" \
  --annotations-dir "$OUT/tiny-overshoot-annotations" \
  --audio-dir "$OUT/audio" >"$OUT/tiny-overshoot-promote.out"
grep -q "clamped_timing:" "$OUT/tiny-overshoot-promote.out"
jq -e '.duration_seconds == 50' "$OUT/tiny-overshoot-annotations/episode-tiny-overshoot.json" >/dev/null
jq -e '.ad_windows[0].end_seconds == 50' "$OUT/tiny-overshoot-annotations/episode-tiny-overshoot.json" >/dev/null
jq -e '.content_windows[0].end_seconds == 45' "$OUT/tiny-overshoot-annotations/episode-tiny-overshoot.json" >/dev/null
jq -e '.content_windows | all(.end_seconds <= 50)' "$OUT/tiny-overshoot-annotations/episode-tiny-overshoot.json" >/dev/null

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
    },
    {
      "id": "episode-too-long#1",
      "episode_id": "episode-too-long",
      "candidate_index": 1,
      "audio_path": "$OUT/audio/episode-short.wav",
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
    },
    "episode-too-long#1": {
      "status": "verified_ad",
      "show_name": "Fixture Show",
      "start_seconds": 45,
      "end_seconds": 50.06,
      "advertiser": "D",
      "product": "D",
      "ad_type": "host_read",
      "transition_type": "explicit",
      "notes": "Ends just outside the endpoint tolerance."
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
grep -q "exceeds duration 50.0" "$OUT/overlap-report.out"
test ! -e "$OUT/overlap-annotations/episode-overlap.json"
test ! -e "$OUT/overlap-annotations/episode-invalid.json"
test ! -e "$OUT/overlap-annotations/episode-too-long.json"

echo "l2f promote reviewed corpus tests passed"
