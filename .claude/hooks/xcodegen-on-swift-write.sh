#!/bin/bash
# PostToolUse hook: regenerate Xcode project when a .swift file is created
# Input: JSON from stdin with tool_input.file_path

file_path=$(python3 -c "import json,sys;print(json.load(sys.stdin).get('tool_input',{}).get('file_path',''))")

if [[ "$file_path" == *.swift ]]; then
  cd "$(dirname "$0")/../.." && xcodegen generate --quiet 2>/dev/null
fi

exit 0
