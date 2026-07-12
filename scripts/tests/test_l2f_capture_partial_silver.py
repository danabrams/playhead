#!/usr/bin/env python3
"""Black-box tests for the partial-silver capture wrapper."""

from __future__ import annotations

import hashlib
import os
import importlib.util
import pathlib
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/l2f-capture-partial-silver.sh"
CAPTURE_FS_SCRIPT = ROOT / "scripts/l2f_capture_fs.py"
SPEC = importlib.util.spec_from_file_location("l2f_capture_fs", CAPTURE_FS_SCRIPT)
assert SPEC and SPEC.loader
CAPTURE_FS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CAPTURE_FS)
REVISION = "a" * 40


class PartialSilverCaptureWrapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.bin_dir = self.root / "bin"
        self.output_dir = self.root / "output"
        self.bin_dir.mkdir()
        self.output_dir.mkdir()
        self.revision_file = self.root / "revision"
        self.status_file = self.root / "status"
        self.xcode_log = self.root / "xcode.log"
        self.staged_path_log = self.root / "staged-path.log"
        self.hash_log = self.root / "hash.log"
        self.git_call_log = self.root / "git-calls.log"
        self.revision_file.write_text(REVISION + "\n", encoding="utf-8")
        self.status_file.write_text("", encoding="utf-8")
        self._write_executable(
            "git",
            """#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  rev-parse)
    printf 'rev-parse\n' >> "${FAKE_GIT_CALL_LOG:?}"
    count="$(wc -l < "${FAKE_GIT_CALL_LOG:?}" | tr -d ' ')"
    if [[ "$count" == 3 && -n "${FAKE_POSTPUBLICATION_MUTATION_OUTPUT:-}" ]]; then
      printf '%s' "${FAKE_POSTPUBLICATION_MUTATION_BYTES:-mutated after publication}" > \
        "$FAKE_POSTPUBLICATION_MUTATION_OUTPUT"
    fi
    cat "${FAKE_GIT_REVISION_FILE:?}"
    ;;
  status) cat "${FAKE_GIT_STATUS_FILE:?}" ;;
  *) echo "unexpected fake git invocation: $*" >&2; exit 97 ;;
esac
""",
        )
        self._write_executable(
            "xcodebuild",
            """#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${FAKE_XCODE_LOG:?}"
printf '%s\n' "${PLAYHEAD_BASELINE_OUTPUT_PATH:?}" > "${FAKE_STAGED_PATH_LOG:?}"
printf '%s' "${FAKE_STAGED_BYTES:-raw capture}" > "${PLAYHEAD_BASELINE_OUTPUT_PATH:?}"
if [[ -n "${FAKE_FINAL_OUTPUT:-}" ]]; then
  printf '%s' "${FAKE_FINAL_BYTES:-concurrent capture}" > "$FAKE_FINAL_OUTPUT"
fi
if [[ -n "${FAKE_NEXT_REVISION:-}" ]]; then
  printf '%s\n' "$FAKE_NEXT_REVISION" > "${FAKE_GIT_REVISION_FILE:?}"
fi
if [[ -n "${FAKE_NEXT_STATUS:-}" ]]; then
  printf '%s\n' "$FAKE_NEXT_STATUS" > "${FAKE_GIT_STATUS_FILE:?}"
fi
""",
        )
        self._write_executable(
            "sw_vers",
            """#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-}" == "-productVersion" ]] || exit 97
printf '%s\n' "${FAKE_MACOS_VERSION:-27.0}"
""",
        )
        self._write_executable(
            "shasum",
            """#!/usr/bin/env bash
set -euo pipefail
printf 'hash\n' >> "${FAKE_HASH_LOG:?}"
count="$(wc -l < "${FAKE_HASH_LOG:?}" | tr -d ' ')"
if [[ "$count" == 2 && -n "${FAKE_HASH_MUTATION_OUTPUT:-}" ]]; then
  printf '%s' "${FAKE_HASH_MUTATION_BYTES:-mutated during publication}" > \
    "$FAKE_HASH_MUTATION_OUTPUT"
fi
exec /usr/bin/shasum "$@"
""",
        )

    def _write_executable(self, name: str, contents: str) -> None:
        path = self.bin_dir / name
        path.write_text(contents, encoding="utf-8")
        path.chmod(0o755)

    def _capture(
        self,
        output: pathlib.Path | None = None,
        **environment_changes: str,
    ) -> subprocess.CompletedProcess[str]:
        output = output or self.output_dir / (
            "playhead-partial-silver-baseline-baseline-run-1.json"
        )
        environment = os.environ.copy()
        environment.update(
            {
                "FAKE_GIT_REVISION_FILE": str(self.revision_file),
                "FAKE_GIT_STATUS_FILE": str(self.status_file),
                "FAKE_GIT_CALL_LOG": str(self.git_call_log),
                "FAKE_HASH_LOG": str(self.hash_log),
                "FAKE_STAGED_PATH_LOG": str(self.staged_path_log),
                "FAKE_XCODE_LOG": str(self.xcode_log),
                "PATH": f"{self.bin_dir}:{environment['PATH']}",
            }
        )
        environment.update(environment_changes)
        return subprocess.run(
            [
                "bash",
                str(SCRIPT),
                "--run-id",
                "baseline-run-1",
                "--output",
                str(output),
                "--corpus-root",
                str(self.root),
                "--derived-data",
                str(self.root / "derived"),
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    def test_success_publishes_exact_staged_bytes_and_only_the_live_test(self) -> None:
        output = self.output_dir / (
            "playhead-partial-silver-baseline-baseline-run-1.json"
        )
        result = self._capture(output, FAKE_STAGED_BYTES="exact raw bytes")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.read_bytes(), b"exact raw bytes")
        staged_path = pathlib.Path(
            self.staged_path_log.read_text(encoding="utf-8").strip()
        )
        self.assertEqual(staged_path.parent.parent, self.output_dir)
        invocation = self.xcode_log.read_text(encoding="utf-8")
        self.assertIn("-only-testing:PlayheadTests/PipelineDumpLiveTests/", invocation)
        self.assertIn("testProductionPipelineDumpOnNewEpisodes", invocation)

    def test_untracked_source_is_rejected_before_xcodebuild(self) -> None:
        self.status_file.write_text("?? unexpected.swift\n", encoding="utf-8")

        result = self._capture()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("clean tracked and untracked", result.stderr)
        self.assertFalse(self.xcode_log.exists())

    def test_host_below_catalyst_deployment_target_is_rejected_before_xcodebuild(self) -> None:
        result = self._capture(FAKE_MACOS_VERSION="26.5.2")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires macOS 27 or newer", result.stderr)
        self.assertIn("host is macOS 26.5.2", result.stderr)
        self.assertFalse(self.xcode_log.exists())

    def test_head_or_source_change_after_capture_prevents_publication(self) -> None:
        for changes in (
            {"FAKE_NEXT_REVISION": "b" * 40},
            {"FAKE_NEXT_STATUS": "?? generated.swift"},
        ):
            with self.subTest(changes=changes):
                self.revision_file.write_text(REVISION + "\n", encoding="utf-8")
                self.status_file.write_text("", encoding="utf-8")
                output = self.output_dir / (
                    "playhead-partial-silver-baseline-baseline-run-1.json"
                )
                output.unlink(missing_ok=True)

                result = self._capture(output, **changes)

                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(output.exists())

    def test_concurrent_or_preexisting_output_is_never_removed(self) -> None:
        output = self.output_dir / (
            "playhead-partial-silver-baseline-baseline-run-1.json"
        )
        result = self._capture(
            output,
            FAKE_FINAL_OUTPUT=str(output),
            FAKE_FINAL_BYTES="concurrent winner",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(output.read_bytes(), b"concurrent winner")

        output.write_bytes(b"preexisting winner")
        result = self._capture(output)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(output.read_bytes(), b"preexisting winner")

    def test_postpublication_mutation_is_detected_without_removing_output(self) -> None:
        output = self.output_dir / (
            "playhead-partial-silver-baseline-baseline-run-1.json"
        )

        result = self._capture(
            output,
            FAKE_POSTPUBLICATION_MUTATION_OUTPUT=str(output),
            FAKE_POSTPUBLICATION_MUTATION_BYTES="mutated final",
        )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("changed after publication", result.stderr)
        self.assertEqual(output.read_bytes(), b"mutated final")

    def test_descriptor_pinned_publication_never_redirects_on_parent_swap(self) -> None:
        output = self.output_dir
        hostile = self.root / "hostile"
        hostile.mkdir()
        stage_name, output_identity, stage_identity = CAPTURE_FS.create_stage(output)
        staged_name = "playhead-partial-silver-baseline-baseline-run-1.json"
        (output / stage_name / staged_name).write_bytes(b"exact staged bytes")
        original_link = CAPTURE_FS.os.link

        def swapping_link(source, destination, **kwargs):
            output.rename(self.root / "original-output")
            output.symlink_to(hostile, target_is_directory=True)
            return original_link(source, destination, **kwargs)

        with mock.patch.object(CAPTURE_FS.os, "link", side_effect=swapping_link):
            with self.assertRaisesRegex(CAPTURE_FS.CaptureFileError, "output directory"):
                CAPTURE_FS.publish(
                    output,
                    output_identity,
                    stage_name,
                    stage_identity,
                    staged_name,
                    staged_name,
                )

        self.assertEqual(list(hostile.iterdir()), [])
        published = self.root / "original-output" / staged_name
        self.assertEqual(published.read_bytes(), b"exact staged bytes")

    def test_publication_uses_opened_bytes_when_staged_name_is_replaced(self) -> None:
        output = self.output_dir
        stage_name, output_identity, stage_identity = CAPTURE_FS.create_stage(output)
        staged_name = "playhead-partial-silver-baseline-baseline-run-1.json"
        staged = output / stage_name / staged_name
        staged.write_bytes(b"verified staged bytes")
        original_read = CAPTURE_FS._read_stage_file

        def replacing_read(stage_descriptor, name):
            descriptor, data = original_read(stage_descriptor, name)
            staged.unlink()
            staged.write_bytes(b"substituted after read")
            return descriptor, data

        with mock.patch.object(
            CAPTURE_FS,
            "_read_stage_file",
            side_effect=replacing_read,
        ):
            digest = CAPTURE_FS.publish(
                output,
                output_identity,
                stage_name,
                stage_identity,
                staged_name,
                staged_name,
            )

        final = output / staged_name
        self.assertEqual(final.read_bytes(), b"verified staged bytes")
        self.assertEqual(
            digest,
            hashlib.sha256(b"verified staged bytes").hexdigest(),
        )

    def test_descriptor_helpers_close_every_open_descriptor_on_errors(self) -> None:
        output_descriptor = os.open(self.output_dir, os.O_RDONLY)
        self.addCleanup(os.close, output_descriptor)
        stage_name = ".playhead-l2f8-capture-" + "a" * 32
        (self.output_dir / stage_name).mkdir()
        stage_identity = CAPTURE_FS._identity(
            os.stat(stage_name, dir_fd=output_descriptor, follow_symlinks=False)
        )
        opened: list[int] = []
        original_open = os.open

        def recording_open(*args, **kwargs):
            descriptor = original_open(*args, **kwargs)
            opened.append(descriptor)
            return descriptor

        with mock.patch.object(CAPTURE_FS.os, "open", side_effect=recording_open), mock.patch.object(
            CAPTURE_FS.os,
            "fstat",
            side_effect=OSError("forced fstat failure"),
        ):
            with self.assertRaisesRegex(CAPTURE_FS.CaptureFileError, "cannot open"):
                CAPTURE_FS._open_stage(output_descriptor, stage_name, stage_identity)
        self.assertEqual(len(opened), 1)
        with self.assertRaises(OSError):
            os.fstat(opened[0])

        opened.clear()
        original_open_output = CAPTURE_FS._open_output

        def recording_output(path, expected=None):
            descriptor = original_open_output(path, expected)
            opened.append(descriptor)
            return descriptor

        with mock.patch.object(
            CAPTURE_FS,
            "_open_output",
            side_effect=recording_output,
        ), mock.patch.object(
            CAPTURE_FS,
            "_open_stage",
            side_effect=CAPTURE_FS.CaptureFileError("forced stage failure"),
        ):
            with self.assertRaisesRegex(CAPTURE_FS.CaptureFileError, "forced stage"):
                CAPTURE_FS.publish(
                    self.output_dir,
                    CAPTURE_FS._identity(os.stat(self.output_dir)),
                    stage_name,
                    stage_identity,
                    "staged.json",
                    "final.json",
                )
        self.assertEqual(len(opened), 1)
        with self.assertRaises(OSError):
            os.fstat(opened[0])

    def test_failed_stage_allocation_does_not_leave_private_directory(self) -> None:
        with mock.patch.object(
            CAPTURE_FS,
            "_open_stage",
            side_effect=CAPTURE_FS.CaptureFileError("forced stage open failure"),
        ):
            with self.assertRaisesRegex(CAPTURE_FS.CaptureFileError, "forced stage"):
                CAPTURE_FS.create_stage(self.output_dir)

        self.assertEqual(list(self.output_dir.iterdir()), [])

    def test_symlinked_output_directory_is_rejected(self) -> None:
        real_directory = self.root / "real-output"
        real_directory.mkdir()
        linked_directory = self.root / "linked-output"
        linked_directory.symlink_to(real_directory, target_is_directory=True)
        output = linked_directory / (
            "playhead-partial-silver-baseline-baseline-run-1.json"
        )

        result = self._capture(output)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("regular directory", result.stderr)
        self.assertFalse(self.xcode_log.exists())

    def test_output_inside_source_worktree_is_rejected(self) -> None:
        output = ROOT / "playhead-partial-silver-baseline-baseline-run-1.json"
        self.assertFalse(output.exists())

        result = self._capture(output)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("outside the source worktree", result.stderr)
        self.assertFalse(output.exists())

    def test_output_filename_must_exactly_match_run_id(self) -> None:
        output = self.output_dir / "mistyped-baseline.json"

        result = self._capture(output)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "playhead-partial-silver-baseline-baseline-run-1.json",
            result.stderr,
        )
        self.assertFalse(self.xcode_log.exists())
        self.assertFalse(output.exists())

    def test_real_git_allows_ignored_corpus_but_rejects_untracked_source(self) -> None:
        repository = self.root / "real-git-repository"
        scripts = repository / "scripts"
        scripts.mkdir(parents=True)
        capture_script = scripts / SCRIPT.name
        shutil.copy2(SCRIPT, capture_script)
        shutil.copy2(CAPTURE_FS_SCRIPT, scripts / CAPTURE_FS_SCRIPT.name)
        shutil.copy2(
            ROOT / "scripts/l2f-score-partial-silver.py",
            scripts / "l2f-score-partial-silver.py",
        )
        (repository / ".gitignore").write_text(
            "TestFixtures/Corpus/Audio/\nTestFixtures/Corpus/Transcripts/\n",
            encoding="utf-8",
        )
        git_environment = os.environ.copy()
        git_environment["HOME"] = str(self.root / "git-home")
        pathlib.Path(git_environment["HOME"]).mkdir()
        for arguments in (
            ["init", "-q"],
            ["config", "user.name", "L2F Test"],
            ["config", "user.email", "l2f@example.invalid"],
            ["add", "."],
            ["-c", "commit.gpgsign=false", "commit", "-q", "-m", "fixture"],
        ):
            subprocess.run(
                ["/usr/bin/git", *arguments],
                cwd=repository,
                env=git_environment,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

        ignored_audio = repository / "TestFixtures/Corpus/Audio/episode.mp3"
        ignored_transcript = repository / "TestFixtures/Corpus/Transcripts/episode.json"
        ignored_audio.parent.mkdir(parents=True)
        ignored_transcript.parent.mkdir(parents=True)
        ignored_audio.write_bytes(b"retained audio")
        ignored_transcript.write_text("{}\n", encoding="utf-8")

        real_bin = self.root / "real-git-bin"
        real_bin.mkdir()
        xcodebuild = real_bin / "xcodebuild"
        xcodebuild.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "printf '%s' 'real git capture' > \"${PLAYHEAD_BASELINE_OUTPUT_PATH:?}\"\n",
            encoding="utf-8",
        )
        xcodebuild.chmod(0o755)
        sw_vers = real_bin / "sw_vers"
        sw_vers.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "[[ \"${1:-}\" == \"-productVersion\" ]] || exit 97\n"
            "printf '27.0\\n'\n",
            encoding="utf-8",
        )
        sw_vers.chmod(0o755)
        environment = git_environment | {
            "PATH": f"{real_bin}:/usr/bin:/bin:/usr/sbin:/sbin",
        }
        output = self.output_dir / (
            "playhead-partial-silver-baseline-baseline-run-1.json"
        )

        def capture() -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [
                    "bash",
                    str(capture_script),
                    "--run-id",
                    "baseline-run-1",
                    "--output",
                    str(output),
                    "--corpus-root",
                    str(repository),
                    "--derived-data",
                    str(self.root / "real-git-derived"),
                ],
                cwd=repository,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )

        result = capture()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(output.read_bytes(), b"real git capture")

        output.unlink()
        (repository / "unexpected.swift").write_text("// untracked\n", encoding="utf-8")
        result = capture()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("?? unexpected.swift", result.stderr)
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
