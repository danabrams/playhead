#!/bin/bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
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
