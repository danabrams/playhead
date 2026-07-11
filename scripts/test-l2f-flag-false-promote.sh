#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec python3 -m unittest -v "$ROOT/scripts/tests/test_l2f_flag_false_promote.py"
