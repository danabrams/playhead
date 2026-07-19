#!/bin/bash
# Build the fl4j pre-flip validation harness: compiles the PRODUCTION sources
# verbatim (see main.swift header) plus the driver, into a macOS CLI.
set -euo pipefail
WT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="${1:-/private/tmp/claude-501/-Users-dabrams-playhead/6ce9b37b-c84d-4ce3-a585-8e33b921ee5b/scratchpad/fl4j-preflip/fl4j-harness}"
xcrun swiftc -O \
  "$WT/Playhead/Services/AdDetection/LexicalAnchorBank.swift" \
  "$WT/Playhead/Services/AdDetection/LexicalAnchorRefiner.swift" \
  "$WT/Playhead/Services/AdDetection/SelfPromoBank.swift" \
  "$WT/Playhead/Services/AdDetection/SelfPromoVerifier.swift" \
  "$WT/Playhead/Services/AdDetection/PromoSuppressor.swift" \
  "$(dirname "$0")/main.swift" \
  -o "$OUT"
echo "built $OUT"
