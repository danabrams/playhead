#!/usr/bin/env python3
from typing import Optional, Tuple, List
"""
Add Swift source files to project.pbxproj.

Walks the PBXGroup tree from a named root, descending through children by
group `name` (or `path` if `name` is absent). Each --add takes a target,
a group path (root + descendants), and a file basename. New child groups
are created on demand; existing ones are reused.

Usage:
    add_files_to_pbxproj.py PROJECT_PBXPROJ \\
        --target Playhead --target-tests PlayheadTests \\
        --add "app:Playhead/Services/AdDetection/RepeatedAdCache:Foo.swift" \\
        --add "tests:PlayheadTests/Services/AdDetection/RepeatedAdCache:FooTests.swift"
"""
import argparse
import os
import re
import secrets


def _uuid24() -> str:
    return secrets.token_hex(12).upper()


def _all_groups(text: str) -> List[Tuple[int, int, str]]:
    """Return list of (start, end, block) for every PBXGroup."""
    pattern = re.compile(
        r"([0-9A-F]{24}) /\* [^*]+ \*/ = \{\s*isa = PBXGroup;[\s\S]*?\};"
    )
    return [(m.start(), m.end(), m.group(0)) for m in pattern.finditer(text)]


def _find_group_block_by_uuid(text: str, uuid: str) -> Tuple[int, int, str]:
    pattern = re.compile(
        r"%s(?: /\* [^*]+ \*/)? = \{\s*isa = PBXGroup;[\s\S]*?\};" % re.escape(uuid)
    )
    m = pattern.search(text)
    if not m:
        raise SystemExit(f"PBXGroup not found by uuid: {uuid}")
    return m.start(), m.end(), m.group(0)


def _block_uuid(block: str) -> str:
    m = re.match(r"([0-9A-F]{24})", block)
    if not m:
        raise SystemExit(f"could not extract uuid from block: {block[:80]}")
    return m.group(1)


def _block_label(block: str) -> str:
    """Return the label inside `/* ... */` after the uuid."""
    m = re.match(r"[0-9A-F]{24} /\* ([^*]+) \*/", block)
    if not m:
        return ""
    return m.group(1).strip()


def _block_get_path(block: str) -> Optional[str]:
    m = re.search(r"\bpath = ([^;]+);", block)
    if not m:
        return None
    return m.group(1).strip().strip('"')


def _block_get_name(block: str) -> Optional[str]:
    m = re.search(r"\bname = ([^;]+);", block)
    if not m:
        return None
    return m.group(1).strip().strip('"')


def _block_children(block: str) -> List[str]:
    """Returns the UUIDs of children referenced in the children = (...) list."""
    m = re.search(r"children = \(\s*([\s\S]*?)\s*\);", block)
    if not m:
        return []
    body = m.group(1)
    return re.findall(r"([0-9A-F]{24})", body)


def _find_root_group(text: str, label: str) -> str:
    """Find the top-level PBXGroup whose `path` (or `name` if path absent)
    matches `label`. There may be multiple groups with the same `path` in
    the file (the source side and the tests side); the root group has
    `path = label;` and is referenced from the project's mainGroup.
    """
    # Find the project mainGroup uuid.
    proj_pat = re.compile(
        r"isa = PBXProject;[\s\S]*?mainGroup = ([0-9A-F]{24})\s*[;/]"
    )
    m = proj_pat.search(text)
    if not m:
        raise SystemExit("PBXProject mainGroup not found")
    main_uuid = m.group(1)
    _, _, main_block = _find_group_block_by_uuid(text, main_uuid)
    # Walk children of mainGroup, find the one with matching label.
    for child_uuid in _block_children(main_block):
        try:
            _, _, child_block = _find_group_block_by_uuid(text, child_uuid)
        except SystemExit:
            continue  # not a group (file ref)
        path = _block_get_path(child_block) or _block_get_name(child_block) or ""
        if path == label:
            return child_uuid
    raise SystemExit(f"top-level PBXGroup with path/name '{label}' not found in mainGroup")


def _find_or_create_subgroup(text: str, parent_uuid: str, child_label: str) -> Tuple[str, str]:
    """Inside `parent_uuid`'s children, look for a PBXGroup whose path
    matches `child_label`. If none, create one and link it. Returns
    (updated_text, child_uuid)."""
    _, _, parent_block = _find_group_block_by_uuid(text, parent_uuid)
    for child_uuid in _block_children(parent_block):
        try:
            _, _, child_block = _find_group_block_by_uuid(text, child_uuid)
        except SystemExit:
            continue
        path = _block_get_path(child_block) or _block_get_name(child_block) or ""
        if path == child_label:
            return text, child_uuid
    # Not found — create.
    child_uuid = _uuid24()
    new_block = (
        f"\t\t{child_uuid} /* {child_label} */ = {{\n"
        f"\t\t\tisa = PBXGroup;\n"
        f"\t\t\tchildren = (\n"
        f"\t\t\t);\n"
        f"\t\t\tpath = {child_label};\n"
        f"\t\t\tsourceTree = \"<group>\";\n"
        f"\t\t}};\n"
    )
    end_marker = "/* End PBXGroup section */"
    end_idx = text.index(end_marker)
    text = text[:end_idx] + new_block + text[end_idx:]
    # Add child uuid to parent's children list.
    parent_start, parent_end, parent_block = _find_group_block_by_uuid(text, parent_uuid)
    new_parent_block = re.sub(
        r"(children = \(\s*[\s\S]*?)(\s*\);)",
        lambda m: m.group(1) + f"\n\t\t\t\t{child_uuid} /* {child_label} */," + m.group(2),
        parent_block,
        count=1,
    )
    text = text[:parent_start] + new_parent_block + text[parent_end:]
    return text, child_uuid


def _add_to_group_children(text: str, group_uuid: str, file_uuid: str, basename: str) -> str:
    start, end, block = _find_group_block_by_uuid(text, group_uuid)
    new_block = re.sub(
        r"(children = \(\s*[\s\S]*?)(\s*\);)",
        lambda m: m.group(1) + f"\n\t\t\t\t{file_uuid} /* {basename} */," + m.group(2),
        block,
        count=1,
    )
    return text[:start] + new_block + text[end:]


def _add_to_sources_phase(text: str, target_name: str, build_file_uuid: str, basename: str) -> str:
    nt_pat = re.compile(
        r"([0-9A-F]{24}) /\* %s \*/ = \{\s*isa = PBXNativeTarget;[\s\S]*?buildPhases = \(\s*([\s\S]*?)\);" % re.escape(target_name)
    )
    nt_m = nt_pat.search(text)
    if not nt_m:
        raise SystemExit(f"PBXNativeTarget not found: {target_name}")
    phases_block = nt_m.group(2)
    sources_uuid_match = re.search(r"([0-9A-F]{24}) /\* Sources \*/", phases_block)
    if not sources_uuid_match:
        raise SystemExit(f"Sources phase not found in target {target_name}")
    sources_uuid = sources_uuid_match.group(1)
    src_pat = re.compile(
        r"(%s /\* Sources \*/ = \{\s*isa = PBXSourcesBuildPhase;[\s\S]*?files = \(\s*)([\s\S]*?)(\s*\);)"
        % re.escape(sources_uuid)
    )
    def repl(m):
        head = m.group(1)
        body = m.group(2)
        tail = m.group(3)
        new_entry = f"\n\t\t\t\t{build_file_uuid} /* {basename} in Sources */,"
        return head + body + new_entry + tail
    return src_pat.sub(repl, text, count=1)


def _splice_before_marker(text: str, marker: str, new_line: str) -> str:
    idx = text.index(marker)
    return text[:idx] + new_line + "\n" + text[idx:]


def add_file(text: str, file_path: str, group_uuid: str, target_name: str) -> str:
    file_uuid = _uuid24()
    build_uuid = _uuid24()
    basename = os.path.basename(file_path)
    build_line = f"\t\t{build_uuid} /* {basename} in Sources */ = {{isa = PBXBuildFile; fileRef = {file_uuid} /* {basename} */; }};"
    text = _splice_before_marker(text, "/* End PBXBuildFile section */", build_line)
    ref_line = (
        f"\t\t{file_uuid} /* {basename} */ = {{isa = PBXFileReference; "
        f"lastKnownFileType = sourcecode.swift; path = {basename}; sourceTree = \"<group>\"; }};"
    )
    text = _splice_before_marker(text, "/* End PBXFileReference section */", ref_line)
    text = _add_to_group_children(text, group_uuid, file_uuid, basename)
    text = _add_to_sources_phase(text, target_name, build_uuid, basename)
    return text


def main():
    p = argparse.ArgumentParser()
    p.add_argument("pbxproj")
    p.add_argument("--target", required=True)
    p.add_argument("--target-tests", required=True)
    p.add_argument(
        "--add",
        action="append",
        default=[],
        help=(
            "Format: WHICH:GROUP_PATH:FILE_BASENAME  where WHICH is 'app' or 'tests', "
            "and GROUP_PATH is /-separated, e.g. 'Services/AdDetection/RepeatedAdCache'. "
            "The root group is implied by WHICH (Playhead vs PlayheadTests)."
        ),
    )
    args = p.parse_args()

    with open(args.pbxproj, "r", encoding="utf-8") as f:
        text = f.read()

    for spec in args.add:
        parts = spec.split(":", 2)
        if len(parts) != 3:
            raise SystemExit(f"Bad --add: {spec}")
        which, group_path_str, file_path = parts
        target = args.target if which == "app" else args.target_tests
        root_label = "Playhead" if which == "app" else "PlayheadTests"
        root_uuid = _find_root_group(text, root_label)

        parent_uuid = root_uuid
        segments = [p for p in group_path_str.split("/") if p]
        # Defensive: if the caller redundantly leads with the root label
        # (e.g. "Playhead/Services/..."), strip it. The root is already
        # implied by WHICH, and descending into a duplicate "Playhead"
        # child silently creates an orphan parallel tree.
        if segments and segments[0] == root_label:
            segments = segments[1:]
        for child in segments:
            text, parent_uuid = _find_or_create_subgroup(text, parent_uuid, child)

        text = add_file(text, file_path, parent_uuid, target)

    with open(args.pbxproj, "w", encoding="utf-8") as f:
        f.write(text)


if __name__ == "__main__":
    main()
