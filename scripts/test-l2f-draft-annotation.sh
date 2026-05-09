#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$(mktemp -d "${TMPDIR:-/tmp}/playhead-l2f-test.XXXXXX")"
trap 'rm -rf "$OUT"' EXIT

cd "$ROOT"

swift scripts/l2f-draft-annotation.swift \
  --allow-missing-audio \
  --duration 180 \
  --draft-dir "$OUT/synthetic" \
  --force \
  --write-review-queue \
  scripts/fixtures/l2f_zero_ad_sample.json \
  scripts/fixtures/l2f_false_positive_sponsor_sample.json \
  scripts/fixtures/l2f_back_to_back_ads_sample.json \
  scripts/fixtures/l2f_multi_cta_pod_sample.json \
  scripts/fixtures/l2f_overlapping_expansion_sample.json >/dev/null

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
  --allow-missing-audio \
  --duration 180 \
  --draft-dir "$OUT/mixed" \
  --force \
  scripts/fixtures/l2f_zero_ad_sample.json \
  scripts/fixtures/l2f_back_to_back_ads_sample.json >/dev/null

swift scripts/l2f-draft-annotation.swift \
  --allow-missing-audio \
  --duration 180 \
  --draft-dir "$OUT/mixed" \
  --write-review-queue \
  scripts/fixtures/l2f_zero_ad_sample.json \
  scripts/fixtures/l2f_back_to_back_ads_sample.json \
  scripts/fixtures/l2f_multi_cta_pod_sample.json >/dev/null

test "$(jq '.entries | length' "$OUT/mixed/review-queue.json")" = "3"

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

swift scripts/l2f-draft-annotation.swift \
  --review-queue-only \
  --review-source "$OUT/quoted-review-source.json" \
  --audio-dir "$OUT/audio" \
  --draft-dir "$OUT/quoted" >/dev/null

playback_command="$(jq -r '.entries[0].playback_command' "$OUT/quoted/review-queue.json")"
[[ "$playback_command" == *"ffplay -autoexit -nodisp -ss 0.0 -t 44.0"* ]]
[[ "$playback_command" == *"quote'\\''id.m4a"* ]]
test "$(jq -r '.entries[0].extraction_command | contains("/quote-id-12.0-24.0.review.m4a")' "$OUT/quoted/review-queue.json")" = "true"

CODEX_REVIEW_SOURCE="TestFixtures/Corpus/Drafts/codex-transcript-review.json"
if [[ -f "$CODEX_REVIEW_SOURCE" ]]; then
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
  echo "skipped local Codex review queue smoke; missing ignored $CODEX_REVIEW_SOURCE"
fi

echo "l2f-draft-annotation smoke tests passed"
