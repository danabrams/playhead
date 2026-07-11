#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$(mktemp -d "${TMPDIR:-/tmp}/playhead-l2f-transcribe-test.XXXXXX")"
trap 'rm -rf "$OUT"' EXIT

mkdir -p "$OUT/audio" "$OUT/transcripts"
printf 'retained source audio\n' > "$OUT/audio/episode-1.mp3"

cat > "$OUT/fake-whisper" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
output_base=""
while (($#)); do
  if [[ "$1" == "-of" ]]; then
    output_base="$2"
    shift 2
  else
    shift
  fi
done
printf '{"transcription":[{"text":"words","offsets":{"from":0,"to":1000}}]}\n' > "$output_base.json"
SH
chmod +x "$OUT/fake-whisper"

cd "$ROOT"
swift scripts/l2f-local-transcribe.swift \
  --model "$OUT/model.bin" \
  --whisper-bin "$OUT/fake-whisper" \
  --transcript-dir "$OUT/transcripts" \
  "$OUT/audio/episode-1.mp3" >/dev/null

expected="sha256:$(shasum -a 256 "$OUT/audio/episode-1.mp3" | awk '{print $1}')"
test "$(jq -r '.source_audio_fingerprint' "$OUT/transcripts/episode-1.json")" = "$expected"

printf 'different retained source audio\n' > "$OUT/audio/episode-1.mp3"
if swift scripts/l2f-local-transcribe.swift \
  --model "$OUT/model.bin" \
  --whisper-bin "$OUT/fake-whisper" \
  --transcript-dir "$OUT/transcripts" \
  "$OUT/audio/episode-1.mp3" >/dev/null 2>&1; then
  echo "expected an existing transcript bound to old audio to fail" >&2
  exit 1
fi

swift scripts/l2f-local-transcribe.swift \
  --force \
  --model "$OUT/model.bin" \
  --whisper-bin "$OUT/fake-whisper" \
  --transcript-dir "$OUT/transcripts" \
  "$OUT/audio/episode-1.mp3" >/dev/null

expected="sha256:$(shasum -a 256 "$OUT/audio/episode-1.mp3" | awk '{print $1}')"
test "$(jq -r '.source_audio_fingerprint' "$OUT/transcripts/episode-1.json")" = "$expected"

echo "l2f-local-transcribe binding tests passed"
