#!/bin/bash
# Prove tools/9s1z/Deps.swift is exactly what extract-deps.sh produces from the
# app target right now — i.e. that no hand edit has crept into the "verbatim"
# extract. Exit 1 with a diff if not.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
cp "$ROOT/tools/9s1z/Deps.swift" "$TMP/committed.swift"
"$ROOT/tools/9s1z/extract-deps.sh" >/dev/null
if diff -u "$TMP/committed.swift" "$ROOT/tools/9s1z/Deps.swift"; then
  echo "Deps.swift is a verbatim extract of the app target. OK"
else
  echo "Deps.swift DIFFERS from a fresh extract — it has been hand-edited." >&2
  exit 1
fi
