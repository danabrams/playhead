#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$(mktemp -d "${TMPDIR:-/tmp}/playhead-l2f-test.XXXXXX")"
trap 'rm -rf "$OUT"' EXIT

cd "$ROOT"

mkdir -p "$OUT/audio" "$OUT/transcripts"
FIXTURES=(
  l2f_zero_ad_sample
  l2f_false_positive_sponsor_sample
  l2f_back_to_back_ads_sample
  l2f_multi_cta_pod_sample
  l2f_overlapping_expansion_sample
)
for fixture in "${FIXTURES[@]}"; do
  printf 'synthetic retained audio for %s\n' "$fixture" > "$OUT/audio/$fixture.mp3"
  fingerprint="sha256:$(shasum -a 256 "$OUT/audio/$fixture.mp3" | awk '{print $1}')"
  jq --arg fingerprint "$fingerprint" \
    '. + {source_audio_fingerprint: $fingerprint}' \
    "scripts/fixtures/$fixture.json" > "$OUT/transcripts/$fixture.json"
done

swift scripts/l2f-draft-annotation.swift \
  --audio-dir "$OUT/audio" \
  --duration 180 \
  --draft-dir "$OUT/synthetic" \
  --force \
  --write-review-queue \
  "$OUT/transcripts/l2f_zero_ad_sample.json" \
  "$OUT/transcripts/l2f_false_positive_sponsor_sample.json" \
  "$OUT/transcripts/l2f_back_to_back_ads_sample.json" \
  "$OUT/transcripts/l2f_multi_cta_pod_sample.json" \
  "$OUT/transcripts/l2f_overlapping_expansion_sample.json" >/dev/null

zero_count="$(jq '.ad_windows | length' "$OUT/synthetic/l2f_zero_ad_sample.draft.json")"
false_positive_count="$(jq '.ad_windows | length' "$OUT/synthetic/l2f_false_positive_sponsor_sample.draft.json")"
back_to_back_count="$(jq '.ad_windows | length' "$OUT/synthetic/l2f_back_to_back_ads_sample.draft.json")"
multi_cta_count="$(jq '.ad_windows | length' "$OUT/synthetic/l2f_multi_cta_pod_sample.draft.json")"
overlap_count="$(jq '.ad_windows | length' "$OUT/synthetic/l2f_overlapping_expansion_sample.draft.json")"
test "$zero_count" = "0"
test "$false_positive_count" = "0"
test "$back_to_back_count" = "1"
test "$multi_cta_count" = "1"
test "$overlap_count" = "1"

test "$(jq -r '.ad_windows[0].start_seconds' "$OUT/synthetic/l2f_back_to_back_ads_sample.draft.json")" = "18"
test "$(jq -r '.ad_windows[0].end_seconds' "$OUT/synthetic/l2f_back_to_back_ads_sample.draft.json")" = "92"
test "$(jq -r '.ad_windows[0].advertiser_guess' "$OUT/synthetic/l2f_back_to_back_ads_sample.draft.json")" = "Acme Notes"
test "$(jq -r '.ad_windows[0].product_guess' "$OUT/synthetic/l2f_back_to_back_ads_sample.draft.json")" = "wireless service"

test "$(jq -r '.ad_windows[0].start_seconds' "$OUT/synthetic/l2f_multi_cta_pod_sample.draft.json")" = "28"
test "$(jq -r '.ad_windows[0].end_seconds' "$OUT/synthetic/l2f_multi_cta_pod_sample.draft.json")" = "127"
test "$(jq -r '.ad_windows[0].advertiser_guess' "$OUT/synthetic/l2f_multi_cta_pod_sample.draft.json")" = "Rorra"
test "$(jq -r '.ad_windows[0].product_guess' "$OUT/synthetic/l2f_multi_cta_pod_sample.draft.json")" = "water filter"

test "$(jq -r '.ad_windows[0].start_seconds' "$OUT/synthetic/l2f_overlapping_expansion_sample.draft.json")" = "18"
test "$(jq -r '.ad_windows[0].end_seconds' "$OUT/synthetic/l2f_overlapping_expansion_sample.draft.json")" = "132"

test "$(jq '.entries | length' "$OUT/synthetic/review-queue.json")" = "5"
test "$(jq '[.entries[] | select(.false_positive_trap == true)] | length' "$OUT/synthetic/review-queue.json")" = "2"

swift scripts/l2f-draft-annotation.swift \
  --audio-dir "$OUT/audio" \
  --duration 180 \
  --draft-dir "$OUT/mixed" \
  --force \
  "$OUT/transcripts/l2f_zero_ad_sample.json" \
  "$OUT/transcripts/l2f_back_to_back_ads_sample.json" >/dev/null

swift scripts/l2f-draft-annotation.swift \
  --audio-dir "$OUT/audio" \
  --duration 180 \
  --draft-dir "$OUT/mixed" \
  --write-review-queue \
  "$OUT/transcripts/l2f_zero_ad_sample.json" \
  "$OUT/transcripts/l2f_back_to_back_ads_sample.json" \
  "$OUT/transcripts/l2f_multi_cta_pod_sample.json" >/dev/null

test "$(jq '.entries | length' "$OUT/mixed/review-queue.json")" = "3"

if swift scripts/l2f-draft-annotation.swift \
  --audio-dir "$OUT/audio" \
  --duration 180 \
  --draft-dir "$OUT/reject-unbound" \
  scripts/fixtures/l2f_zero_ad_sample.json >/dev/null 2>&1; then
  echo "expected an unbound transcript sidecar to fail" >&2
  exit 1
fi

jq '.source_audio_fingerprint = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' \
  "$OUT/transcripts/l2f_zero_ad_sample.json" > "$OUT/mismatched.json"
cp -f "$OUT/audio/l2f_zero_ad_sample.mp3" "$OUT/audio/mismatched.mp3"
if swift scripts/l2f-draft-annotation.swift \
  --audio-dir "$OUT/audio" \
  --duration 180 \
  --draft-dir "$OUT/reject-mismatch" \
  "$OUT/mismatched.json" >/dev/null 2>&1; then
  echo "expected a mismatched transcript source fingerprint to fail" >&2
  exit 1
fi

cp -f "$OUT/audio/l2f_zero_ad_sample.mp3" "$OUT/audio/malformed-segment.mp3"
malformed_fingerprint="sha256:$(shasum -a 256 "$OUT/audio/malformed-segment.mp3" | awk '{print $1}')"
jq --arg fingerprint "$malformed_fingerprint" \
  '.source_audio_fingerprint = $fingerprint | .transcription += [{text:"missing timestamps"}]' \
  "$OUT/transcripts/l2f_zero_ad_sample.json" > "$OUT/malformed-segment.json"
if swift scripts/l2f-draft-annotation.swift \
  --audio-dir "$OUT/audio" \
  --duration 180 \
  --draft-dir "$OUT/reject-malformed-segment" \
  "$OUT/malformed-segment.json" >/dev/null 2>&1; then
  echo "expected a partially malformed transcript to fail" >&2
  exit 1
fi

cat > "$OUT/missing-windows-review.json" <<'JSON'
{"episodes":[{"episode_id":"missing-windows"}]}
JSON
if swift scripts/l2f-draft-annotation.swift \
  --review-queue-only \
  --review-source "$OUT/missing-windows-review.json" \
  --draft-dir "$OUT/reject-missing-windows" >/dev/null 2>&1; then
  echo "expected missing codex_windows to fail closed" >&2
  exit 1
fi

mkdir -p "$OUT/reject-malformed-draft"
cat > "$OUT/reject-malformed-draft/bad.draft.json" <<'JSON'
{"episode_id":"bad","duration_seconds":10,"ad_windows":[{"start_seconds":2}]}
JSON
if swift scripts/l2f-draft-annotation.swift \
  --review-queue-only \
  --draft-dir "$OUT/reject-malformed-draft" >/dev/null 2>&1; then
  echo "expected malformed draft ad_windows to fail closed" >&2
  exit 1
fi

printf 'current B audio\n' > "$OUT/audio/stale-draft.mp3"
mkdir -p "$OUT/reject-stale-draft"
cat > "$OUT/reject-stale-draft/stale-draft.draft.json" <<'JSON'
{
  "episode_id": "stale-draft",
  "audio_fingerprint": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "duration_seconds": 30,
  "ad_windows": [{"start_seconds": 2, "end_seconds": 8}]
}
JSON
if swift scripts/l2f-draft-annotation.swift \
  --review-queue-only \
  --audio-dir "$OUT/audio" \
  --draft-dir "$OUT/reject-stale-draft" >/dev/null 2>&1; then
  echo "expected stale A-derived draft coordinates to reject staged B audio" >&2
  exit 1
fi

printf 'current codex audio\n' > "$OUT/audio/unbound-codex.mp3"
cat > "$OUT/unbound-codex-review.json" <<'JSON'
{"episodes":[{"episode_id":"unbound-codex","codex_windows":[{"start_seconds":2,"end_seconds":8}]}]}
JSON
if swift scripts/l2f-draft-annotation.swift \
  --review-queue-only \
  --review-source "$OUT/unbound-codex-review.json" \
  --audio-dir "$OUT/audio" \
  --draft-dir "$OUT/reject-unbound-codex" >/dev/null 2>&1; then
  echo "expected an unbound Codex review source to fail" >&2
  exit 1
fi

printf 'current Codex B audio\n' > "$OUT/audio/stale-codex.mp3"
cat > "$OUT/stale-codex-review.json" <<'JSON'
{
  "episodes": [{
    "episode_id": "stale-codex",
    "audio_fingerprint": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "codex_windows": [{"start_seconds":2,"end_seconds":8}]
  }]
}
JSON
if swift scripts/l2f-draft-annotation.swift \
  --review-queue-only \
  --review-source "$OUT/stale-codex-review.json" \
  --audio-dir "$OUT/audio" \
  --draft-dir "$OUT/reject-stale-codex" >/dev/null 2>&1; then
  echo "expected stale A-derived Codex coordinates to reject staged B audio" >&2
  exit 1
fi

if swift scripts/l2f-draft-annotation.swift \
  --review-queue-only \
  --review-queue-name '../outside' \
  --draft-dir "$OUT/reject-name" >/dev/null 2>&1; then
  echo "expected --review-queue-name with a path separator to fail" >&2
  exit 1
fi

if swift scripts/l2f-draft-annotation.swift \
  --allow-missing-audio \
  --episode-id '../outside' \
  --draft-dir "$OUT/reject-episode" \
  scripts/fixtures/l2f_zero_ad_sample.json >/dev/null 2>&1; then
  echo "expected --episode-id with a path separator to fail" >&2
  exit 1
fi

if swift scripts/l2f-draft-annotation.swift \
  --allow-missing-audio \
  --draft-dir TestFixtures/Corpus/Annotations \
  scripts/fixtures/l2f_zero_ad_sample.json >/dev/null 2>&1; then
  echo "expected --draft-dir outside Drafts or tmp to fail" >&2
  exit 1
fi

mkdir -p "$OUT/audio"
touch "$OUT/audio/quote'id.m4a"
cat > "$OUT/quoted-review-source.json" <<'JSON'
{
  "episodes": [
    {
      "episode_id": "quote'id",
      "audio_fingerprint": "REPLACE_FINGERPRINT",
      "codex_windows": [
        {
          "start_seconds": 12,
          "end_seconds": 24,
          "advertiser": "Quoted Co",
          "product": "review safety",
          "confidence_notes": "Synthetic quoting regression."
        }
      ]
    }
  ]
}
JSON
quoted_fingerprint="sha256:$(shasum -a 256 "$OUT/audio/quote'id.m4a" | awk '{print $1}')"
jq --arg fingerprint "$quoted_fingerprint" \
  '.episodes[0].audio_fingerprint = $fingerprint' \
  "$OUT/quoted-review-source.json" > "$OUT/quoted-review-source.bound.json"

swift scripts/l2f-draft-annotation.swift \
  --review-queue-only \
  --review-source "$OUT/quoted-review-source.bound.json" \
  --audio-dir "$OUT/audio" \
  --draft-dir "$OUT/quoted" >/dev/null

playback_command="$(jq -r '.entries[0].playback_command' "$OUT/quoted/review-queue.json")"
[[ "$playback_command" == *"ffplay -autoexit -nodisp -ss 0.0 -t 44.0"* ]]
[[ "$playback_command" == *"quote'\\''id.m4a"* ]]
test "$(jq -r '.entries[0].extraction_command | contains("/quote-id-12.0-24.0.review.m4a")' "$OUT/quoted/review-queue.json")" = "true"

CODEX_REVIEW_SOURCE="TestFixtures/Corpus/Drafts/codex-transcript-review.json"
if [[ -f "$CODEX_REVIEW_SOURCE" ]] && jq -e '
  (.episodes | type == "array") and
  all(.episodes[]; .audio_fingerprint | type == "string" and test("^sha256:[0-9a-f]{64}$"))
' "$CODEX_REVIEW_SOURCE" >/dev/null; then
  swift scripts/l2f-draft-annotation.swift \
    --review-queue-only \
    --review-source "$CODEX_REVIEW_SOURCE" \
    --draft-dir "$OUT/codex" \
    --review-queue-name codex-review-queue >/dev/null

  test "$(jq '.entries | length' "$OUT/codex/codex-review-queue.json")" = "33"
  test "$(jq '[.entries[].episode_id] | unique | length' "$OUT/codex/codex-review-queue.json")" = "15"
  test "$(jq '[.entries[] | select(.false_positive_trap == true)] | length' "$OUT/codex/codex-review-queue.json")" = "4"
  test "$(jq '[.entries[] | select(.false_positive_trap == false) | select((.playback_command | contains("ffplay") | not) or (.extraction_command | contains("ffmpeg") | not))] | length' "$OUT/codex/codex-review-queue.json")" = "0"
else
  echo "skipped local Codex review queue smoke; missing or unbound ignored $CODEX_REVIEW_SOURCE"
fi

echo "l2f-draft-annotation smoke tests passed"
