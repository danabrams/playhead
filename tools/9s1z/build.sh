#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# playhead-hbejs: refuse to build from a stale extract. Deps.swift is a verbatim
# copy of production that nothing else re-verifies; a green build of the harness
# is not evidence the copy is current, so the check belongs to whoever is about
# to trust the output.
"$ROOT/tools/9s1z/verify-deps.sh" || {
  echo "build.sh: REFUSING to build — Deps.swift is not a verbatim extract of the app target." >&2
  echo "build.sh: run tools/9s1z/extract-deps.sh and commit the result, then build again." >&2
  exit 1
}
SDK="$(xcrun --sdk macosx --show-sdk-path)"
P="$ROOT/Playhead"
swiftc -O -sdk "$SDK" -o "$ROOT/tools/9s1z/recompose" \
  "$ROOT/tools/9s1z/Deps.swift" \
  "$ROOT/tools/9s1z/main.swift" \
  "$P/Services/AdDetection/SemanticSweepMarkComposer.swift" \
  "$P/Services/AdDetection/SemanticScanResult.swift" \
  "$P/Services/AdDetection/SemanticScanStatus.swift" \
  "$P/Services/AdDetection/ScanCohort.swift" \
  "$P/Services/AdDetection/FMInferenceDeadline.swift" \
  "$P/Services/AdDetection/TranscriptAtom.swift" \
  "$P/Services/AdDetection/TranscriptQualityEstimator.swift" \
  "$P/Services/AdDetection/TranscriptChunkCanonicalizer.swift" \
  "$P/Services/AdDetection/SupportLineIndex.swift" \
  "$P/Services/AdDetection/ComposedMarkGate.swift" 2>&1 | grep -v "warning:" | grep -v "^ *|" | grep -v "^ *[0-9]* |" || true
echo "built $ROOT/tools/9s1z/recompose"
