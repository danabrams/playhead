#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python3 -m unittest scripts.tests.test_l2f_capture_partial_silver_device
bash -n "$ROOT/scripts/l2f-capture-partial-silver-device.sh"
