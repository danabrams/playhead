#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$(mktemp -d "${TMPDIR:-/tmp}/playhead-l2f-transcribe-test.XXXXXX")"
trap 'rm -rf "$OUT"' EXIT

mkdir -p "$OUT/audio" "$OUT/transcripts"

cat > "$OUT/fake-whisper" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
output_base=""
input=""
while (($#)); do
  if [[ "$1" == "-of" ]]; then
    output_base="$2"
    shift 2
  else
    input="$1"
    shift
  fi
done
printf '%s\n' "$output_base" >> "${FAKE_WHISPER_LOG:?}"
case "$(basename "$input")" in
  *malformed*)
    printf '{"transcription":[' > "$output_base.json"
    ;;
  *invalidutf8*)
    printf '{"source_audio_fingerprint":"sha256:bogus","transcription":[{"text":"bad \377 text","offsets":{"from":0,"to":1000}}]}\n' > "$output_base.json"
    ;;
  *episode-mutating*)
    printf '{"transcription":[{"text":"words","offsets":{"from":0,"to":1000}}]}\n' > "$output_base.json"
    printf 'mutation during transcription\n' >> "$input"
    ;;
  *publishrace*)
    printf '{"transcription":[{"text":"words","offsets":{"from":0,"to":1000}}]}\n' > "$output_base.json"
    : > "${RACE_READY:?}"
    while [[ ! -e "${RACE_RELEASE:?}" ]]; do sleep 0.01; done
    ;;
  *publicationmutating*)
    python3 - "$output_base.json" <<'PY'
import json
import sys

with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump({
        "transcription": [{
            "text": "x" * (128 * 1024 * 1024),
            "offsets": {"from": 0, "to": 1000},
        }],
    }, handle)
PY
    ;;
  *whisperfail*)
    exit 7
    ;;
  *nooutput*)
    :
    ;;
  *)
    printf '{"source_audio_fingerprint":"sha256:bogus","transcription":[{"text":"words","offsets":{"from":0,"to":1000}}]}\n' > "$output_base.json"
    ;;
esac
SH
chmod +x "$OUT/fake-whisper"
export FAKE_WHISPER_LOG="$OUT/whisper.log"
export RACE_READY="$OUT/race.ready"
export RACE_RELEASE="$OUT/race.release"
: > "$FAKE_WHISPER_LOG"

transcribe() {
  swift "$ROOT/scripts/l2f-local-transcribe.swift" \
    --model "$OUT/model.bin" \
    --whisper-bin "$OUT/fake-whisper" \
    --transcript-dir "$OUT/transcripts" \
    "$@"
}

transcribe_async() {
  exec swift "$ROOT/scripts/l2f-local-transcribe.swift" \
    --model "$OUT/model.bin" \
    --whisper-bin "$OUT/fake-whisper" \
    --transcript-dir "$OUT/transcripts" \
    "$@"
}

fingerprint() {
  printf 'sha256:%s' "$(shasum -a 256 "$1" | awk '{print $1}')"
}

# Non-regular explicit inputs fail before fingerprinting. In particular, a
# FIFO must not block forever while waiting for a writer that will never come.
python3 - "$ROOT" "$OUT" <<'PY'
import os
import pathlib
import subprocess
import sys

root = pathlib.Path(sys.argv[1])
out = pathlib.Path(sys.argv[2])
fifo = out / "audio" / "episode-fifo.mp3"
os.mkfifo(fifo)
command = [
    "swift",
    str(root / "scripts" / "l2f-local-transcribe.swift"),
    "--model", str(out / "model.bin"),
    "--whisper-bin", str(out / "fake-whisper"),
    "--transcript-dir", str(out / "transcripts"),
    str(fifo),
]
try:
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=10,
        check=False,
    )
except subprocess.TimeoutExpired as error:
    raise AssertionError("non-regular audio input blocked during fingerprinting") from error
assert result.returncode != 0
assert "not a regular file" in result.stderr
PY

# Happy path binds exact retained bytes, overwrites bogus generated provenance,
# and sends Whisper output to process-private staging rather than the final path.
printf 'retained source audio\n' > "$OUT/audio/episode-1.mp3"
transcribe "$OUT/audio/episode-1.mp3" >/dev/null
test "$(jq -r '.source_audio_fingerprint' "$OUT/transcripts/episode-1.json")" = \
  "$(fingerprint "$OUT/audio/episode-1.mp3")"
test "$(wc -l < "$FAKE_WHISPER_LOG" | tr -d ' ')" = 1
case "$(head -n 1 "$FAKE_WHISPER_LOG")" in
  "$OUT/transcripts"/*)
    echo "Whisper wrote at the final transcript path instead of staging" >&2
    exit 1
    ;;
esac

# A valid bound sidecar skips cleanly and does not invoke Whisper again.
transcribe "$OUT/audio/episode-1.mp3" >/dev/null
test "$(wc -l < "$FAKE_WHISPER_LOG" | tr -d ' ')" = 1

# Parsing a large valid sidecar leaves a deterministic window after the first
# audio hash. Atomically replacing the audio in that window must invalidate the
# skip instead of reporting success against stale provenance.
truncate -s 536870912 "$OUT/audio/episode-skip-replace.mp3"
skip_fingerprint="$(fingerprint "$OUT/audio/episode-skip-replace.mp3")"
python3 - "$OUT/transcripts/episode-skip-replace.json" "$skip_fingerprint" <<'PY'
import json
import sys

with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump({
        "source_audio_fingerprint": sys.argv[2],
        "padding": "x" * (512 * 1024 * 1024),
    }, handle)
PY
cp -f "$OUT/transcripts/episode-skip-replace.json" "$OUT/skip-replace.before"
transcribe_async "$OUT/audio/episode-skip-replace.mp3" \
  >"$OUT/skip-replace.stdout" 2>"$OUT/skip-replace.stderr" &
skip_replace_pid=$!
skip_reader_seen=0
for _ in {1..5000}; do
  if /usr/sbin/lsof -a -p "$skip_replace_pid" -- \
    "$OUT/transcripts/episode-skip-replace.json" >/dev/null 2>&1; then
    kill -STOP "$skip_replace_pid"
    skip_reader_seen=1
    break
  fi
  sleep 0.002
done
if [[ "$skip_reader_seen" != 1 ]]; then
  kill "$skip_replace_pid" 2>/dev/null || true
  wait "$skip_replace_pid" 2>/dev/null || true
  echo "could not synchronize existing-sidecar parse race" >&2
  exit 1
fi
printf 'atomically replaced skip audio\n' > "$OUT/skip-replacement.mp3"
mv -f "$OUT/skip-replacement.mp3" "$OUT/audio/episode-skip-replace.mp3"
kill -CONT "$skip_replace_pid"
if wait "$skip_replace_pid"; then
  echo "expected audio replacement during sidecar parse to fail" >&2
  exit 1
fi
cmp -s "$OUT/skip-replace.before" \
  "$OUT/transcripts/episode-skip-replace.json"

# Invalid UTF-8 is repaired only in fresh Whisper text. Canonical JSON remains
# valid UTF-8 and the replacement scalar is retained rather than dropping text.
printf 'invalid UTF-8 audio\n' > "$OUT/audio/episode-invalidutf8.mp3"
transcribe "$OUT/audio/episode-invalidutf8.mp3" >/dev/null
python3 - "$OUT/transcripts/episode-invalidutf8.json" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    transcript = json.load(handle)
assert "\ufffd" in transcript["transcription"][0]["text"]
assert transcript["source_audio_fingerprint"].startswith("sha256:")
assert transcript["source_audio_fingerprint"] != "sha256:bogus"
PY

# A malformed fresh result under --force must never damage the prior final.
printf 'malformed audio\n' > "$OUT/audio/episode-malformed.mp3"
printf '{"preserve":"malformed sentinel"}\n' > "$OUT/transcripts/episode-malformed.json"
cp -f "$OUT/transcripts/episode-malformed.json" "$OUT/malformed.before"
if transcribe --force "$OUT/audio/episode-malformed.mp3" >/dev/null 2>&1; then
  echo "expected malformed Whisper JSON to fail" >&2
  exit 1
fi
cmp -s "$OUT/malformed.before" "$OUT/transcripts/episode-malformed.json"

# Nonzero Whisper exit and success-without-output are also failed staged runs;
# neither is allowed to disturb a previous final under --force.
for stem in episode-whisperfail episode-nooutput; do
  printf '%s audio\n' "$stem" > "$OUT/audio/$stem.mp3"
  printf '{"preserve":"%s sentinel"}\n' "$stem" > "$OUT/transcripts/$stem.json"
  cp -f "$OUT/transcripts/$stem.json" "$OUT/$stem.before"
  if transcribe --force "$OUT/audio/$stem.mp3" >/dev/null 2>&1; then
    echo "expected $stem staged run to fail" >&2
    exit 1
  fi
  cmp -s "$OUT/$stem.before" "$OUT/transcripts/$stem.json"
done

# Retained audio mutation after Whisper starts invalidates provenance and also
# preserves the prior final byte-for-byte.
printf 'mutating audio\n' > "$OUT/audio/episode-mutating.mp3"
printf '{"preserve":"mutation sentinel"}\n' > "$OUT/transcripts/episode-mutating.json"
cp -f "$OUT/transcripts/episode-mutating.json" "$OUT/mutating.before"
if transcribe --force "$OUT/audio/episode-mutating.mp3" >/dev/null 2>&1; then
  echo "expected retained-audio mutation to fail" >&2
  exit 1
fi
cmp -s "$OUT/mutating.before" "$OUT/transcripts/episode-mutating.json"

# The final publication hash reads through an open descriptor. If a normal
# atomic save replaces the audio path while that read is in flight, descriptor
# identity must disagree with the path and the prior sidecar must survive.
truncate -s 536870912 "$OUT/audio/episode-atomic-replace.mp3"
printf '{"preserve":"atomic replacement sentinel"}\n' > \
  "$OUT/transcripts/episode-atomic-replace.json"
cp -f "$OUT/transcripts/episode-atomic-replace.json" \
  "$OUT/atomic-replace.before"
transcribe_async --force "$OUT/audio/episode-atomic-replace.mp3" \
  >"$OUT/atomic-replace.stdout" 2>"$OUT/atomic-replace.stderr" &
atomic_replace_pid=$!
temporary_seen=0
for _ in {1..5000}; do
  if compgen -G \
    "$OUT/transcripts/.episode-atomic-replace.json.*.tmp" >/dev/null; then
    temporary_seen=1
    break
  fi
  sleep 0.002
done
if [[ "$temporary_seen" != 1 ]]; then
  kill "$atomic_replace_pid" 2>/dev/null || true
  wait "$atomic_replace_pid" 2>/dev/null || true
  echo "could not synchronize final fingerprint race" >&2
  exit 1
fi
final_reader_seen=0
for _ in {1..5000}; do
  if /usr/sbin/lsof -a -p "$atomic_replace_pid" -- \
    "$OUT/audio/episode-atomic-replace.mp3" >/dev/null 2>&1; then
    kill -STOP "$atomic_replace_pid"
    final_reader_seen=1
    break
  fi
  sleep 0.002
done
if [[ "$final_reader_seen" != 1 ]]; then
  kill "$atomic_replace_pid" 2>/dev/null || true
  wait "$atomic_replace_pid" 2>/dev/null || true
  echo "could not observe final fingerprint descriptor" >&2
  exit 1
fi
printf 'atomically replaced publication audio\n' > "$OUT/atomic-replacement.mp3"
mv -f "$OUT/atomic-replacement.mp3" \
  "$OUT/audio/episode-atomic-replace.mp3"
kill -CONT "$atomic_replace_pid"
if wait "$atomic_replace_pid"; then
  echo "expected audio replacement during final fingerprint to fail" >&2
  exit 1
fi
cmp -s "$OUT/atomic-replace.before" \
  "$OUT/transcripts/episode-atomic-replace.json"

# Audio can also change after the post-Whisper fingerprint while a large
# canonical transcript is being staged for publication. The final recheck must
# reject that race without replacing a prior sidecar under --force.
printf 'publication mutating audio\n' > "$OUT/audio/episode-publicationmutating.mp3"
printf '{"preserve":"publication mutation sentinel"}\n' > \
  "$OUT/transcripts/episode-publicationmutating.json"
cp -f "$OUT/transcripts/episode-publicationmutating.json" \
  "$OUT/publicationmutating.before"
(
  for _ in {1..5000}; do
    if compgen -G \
      "$OUT/transcripts/.episode-publicationmutating.json.*.tmp" >/dev/null; then
      printf 'mutation during transcript publication\n' >> \
        "$OUT/audio/episode-publicationmutating.mp3"
      : > "$OUT/publicationmutating.seen"
      exit 0
    fi
    sleep 0.002
  done
  exit 1
) &
publication_watcher=$!
if transcribe --force "$OUT/audio/episode-publicationmutating.mp3" >/dev/null 2>&1; then
  echo "expected publication-time retained-audio mutation to fail" >&2
  exit 1
fi
wait "$publication_watcher"
test -e "$OUT/publicationmutating.seen"
cmp -s "$OUT/publicationmutating.before" \
  "$OUT/transcripts/episode-publicationmutating.json"

# A final created after the initial existence check must win. Non-force mode
# publishes with an atomic no-overwrite primitive, not a racy check-then-replace.
printf 'publish race audio\n' > "$OUT/audio/episode-publishrace.mp3"
transcribe "$OUT/audio/episode-publishrace.mp3" >/dev/null 2>&1 &
race_pid=$!
for _ in {1..500}; do
  [[ -e "$RACE_READY" ]] && break
  sleep 0.01
done
test -e "$RACE_READY"
printf '{"preserve":"concurrent writer"}\n' > \
  "$OUT/transcripts/episode-publishrace.json"
: > "$RACE_RELEASE"
if wait "$race_pid"; then
  echo "expected no-force publication race to fail" >&2
  exit 1
fi
test "$(jq -r '.preserve' "$OUT/transcripts/episode-publishrace.json")" = \
  "concurrent writer"

# A successful force run does replace stale bytes after every staged-output
# and retained-audio check has passed.
printf 'force replacement audio\n' > "$OUT/audio/episode-force.mp3"
printf '{"preserve":"stale"}\n' > "$OUT/transcripts/episode-force.json"
transcribe --force "$OUT/audio/episode-force.mp3" >/dev/null
test "$(jq -r '.source_audio_fingerprint' "$OUT/transcripts/episode-force.json")" = \
  "$(fingerprint "$OUT/audio/episode-force.mp3")"
test "$(jq -r '.transcription[0].text' "$OUT/transcripts/episode-force.json")" = "words"

# Invalid and unbound pre-existing sidecars are strict failures without
# --force. They are neither rewritten nor sent through lossy UTF-8 repair.
printf 'strict audio\n' > "$OUT/audio/episode-strict.mp3"
printf '{"transcription":[]}\n' > "$OUT/transcripts/episode-strict.json"
cp -f "$OUT/transcripts/episode-strict.json" "$OUT/strict.before"
calls_before="$(wc -l < "$FAKE_WHISPER_LOG" | tr -d ' ')"
if transcribe "$OUT/audio/episode-strict.mp3" >/dev/null 2>&1; then
  echo "expected unbound existing sidecar to fail" >&2
  exit 1
fi
cmp -s "$OUT/strict.before" "$OUT/transcripts/episode-strict.json"
test "$(wc -l < "$FAKE_WHISPER_LOG" | tr -d ' ')" = "$calls_before"

printf '\377not-json\n' > "$OUT/transcripts/episode-strict.json"
cp -f "$OUT/transcripts/episode-strict.json" "$OUT/strict-invalid.before"
if transcribe "$OUT/audio/episode-strict.mp3" >/dev/null 2>&1; then
  echo "expected invalid UTF-8 existing sidecar to fail" >&2
  exit 1
fi
cmp -s "$OUT/strict-invalid.before" "$OUT/transcripts/episode-strict.json"

# Per-file failure does not stop the batch: a later valid input is published,
# while the command exits non-zero and the failed input has no final output.
printf 'batch malformed audio\n' > "$OUT/audio/batch-malformed.mp3"
printf 'batch good audio\n' > "$OUT/audio/batch-good.mp3"
if transcribe \
  "$OUT/audio/batch-malformed.mp3" \
  "$OUT/audio/batch-good.mp3" >/dev/null 2>&1; then
  echo "expected mixed batch to report its malformed member" >&2
  exit 1
fi
test ! -e "$OUT/transcripts/batch-malformed.json"
test "$(jq -r '.source_audio_fingerprint' "$OUT/transcripts/batch-good.json")" = \
  "$(fingerprint "$OUT/audio/batch-good.mp3")"

echo "l2f-local-transcribe staging and binding tests passed"
