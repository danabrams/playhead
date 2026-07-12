#!/usr/bin/env python3
"""Descriptor-pinned staging and publication for the L2F capture wrapper."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
import pathlib
import secrets
import stat
import sys


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
SCORER_PATH = SCRIPT_DIR / "l2f-score-partial-silver.py"
SPEC = importlib.util.spec_from_file_location("l2f_capture_scorer", SCORER_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load safe-file support from {SCORER_PATH}")
SCORER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCORER)


class CaptureFileError(ValueError):
    """Capture staging or publication could not preserve its identity contract."""


def _identity(metadata: os.stat_result) -> str:
    return f"{metadata.st_dev}:{metadata.st_ino}"


def _open_output(path: pathlib.Path, expected: str | None = None) -> int:
    try:
        descriptor = SCORER._open_directory_descriptor(path, "capture output directory")
    except SCORER.ScoringError as error:
        raise CaptureFileError(str(error)) from error
    try:
        actual = _identity(os.fstat(descriptor))
        if expected is not None and actual != expected:
            raise CaptureFileError(f"capture output directory identity changed: {path}")
        return descriptor
    except CaptureFileError:
        os.close(descriptor)
        raise
    except OSError as error:
        os.close(descriptor)
        raise CaptureFileError(f"cannot inspect capture output directory: {error}") from error
    except BaseException:
        os.close(descriptor)
        raise


def _open_stage(output_descriptor: int, name: str, expected: str) -> int:
    if not name.startswith(".playhead-l2f8-capture-") or "/" in name:
        raise CaptureFileError("invalid capture staging directory name")
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=output_descriptor, follow_symlinks=False)
        if not stat.S_ISDIR(before.st_mode) or _identity(before) != expected:
            raise CaptureFileError("capture staging directory identity changed")
        descriptor = os.open(name, flags, dir_fd=output_descriptor)
        after = os.fstat(descriptor)
        if _identity(after) != expected:
            raise CaptureFileError("capture staging directory changed while opening")
        result = descriptor
        descriptor = None
        return result
    except CaptureFileError:
        raise
    except OSError as error:
        raise CaptureFileError(f"cannot open capture staging directory: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def create_stage(output_dir: pathlib.Path) -> tuple[str, str, str]:
    output_descriptor = _open_output(output_dir)
    name: str | None = None
    created_identity: str | None = None
    try:
        for _ in range(128):
            candidate = f".playhead-l2f8-capture-{secrets.token_hex(16)}"
            try:
                os.mkdir(candidate, 0o700, dir_fd=output_descriptor)
                name = candidate
                break
            except FileExistsError:
                continue
        else:
            raise CaptureFileError("cannot allocate capture staging directory")
        created_metadata = os.stat(name, dir_fd=output_descriptor, follow_symlinks=False)
        created_identity = _identity(created_metadata)
        stage_descriptor = _open_stage(
            output_descriptor,
            name,
            created_identity,
        )
        try:
            os.fsync(output_descriptor)
            result = (
                name,
                _identity(os.fstat(output_descriptor)),
                _identity(os.fstat(stage_descriptor)),
            )
        finally:
            os.close(stage_descriptor)
        name = None
        return result
    finally:
        if name is not None:
            try:
                current = os.stat(name, dir_fd=output_descriptor, follow_symlinks=False)
                if (
                    stat.S_ISDIR(current.st_mode)
                    and (created_identity is None or _identity(current) == created_identity)
                ):
                    os.rmdir(name, dir_fd=output_descriptor)
            except OSError:
                pass
        os.close(output_descriptor)


def _read_stage_file(stage_descriptor: int, name: str) -> tuple[int, bytes]:
    if not name or name in {".", ".."} or pathlib.PurePath(name).name != name:
        raise CaptureFileError("invalid staged capture filename")
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        before = os.stat(name, dir_fd=stage_descriptor, follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise CaptureFileError("baseline harness did not publish a regular staged output")
        descriptor = os.open(name, flags, dir_fd=stage_descriptor)
        after = os.fstat(descriptor)
        if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise CaptureFileError("staged capture changed while opening")
        data = SCORER._read_open_regular_bytes(
            descriptor,
            pathlib.Path(name),
            "staged capture",
            SCORER.MAX_JSON_INPUT_BYTES,
        )
        result = descriptor
        descriptor = None
        return result, data
    except CaptureFileError:
        raise
    except (OSError, SCORER.ScoringError) as error:
        raise CaptureFileError(f"cannot read staged capture: {error}") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _publish_verified_bytes(
    output_descriptor: int,
    output_dir: pathlib.Path,
    final_name: str,
    data: bytes,
) -> str:
    """Publish bytes through a private inode, never through the harness filename."""
    digest = hashlib.sha256(data).hexdigest()
    temporary_name = ""
    temporary_descriptor: int | None = None
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        for _ in range(128):
            candidate = f".playhead-l2f8-publish-{secrets.token_hex(16)}"
            try:
                temporary_descriptor = os.open(
                    candidate,
                    flags,
                    0o600,
                    dir_fd=output_descriptor,
                )
                temporary_name = candidate
                break
            except FileExistsError:
                continue
        if temporary_descriptor is None:
            raise CaptureFileError("cannot allocate private capture publication file")

        view = memoryview(data)
        offset = 0
        while offset < len(view):
            try:
                written = os.write(temporary_descriptor, view[offset:])
            except InterruptedError:
                continue
            if written <= 0:
                raise CaptureFileError("cannot write private capture publication file")
            offset += written
        os.fsync(temporary_descriptor)
        try:
            os.link(
                temporary_name,
                final_name,
                src_dir_fd=output_descriptor,
                dst_dir_fd=output_descriptor,
                follow_symlinks=False,
            )
        except FileExistsError as error:
            raise CaptureFileError(
                f"refusing to replace output created during capture: {output_dir / final_name}"
            ) from error
        destination = SCORER._read_published_bytes(
            output_descriptor,
            final_name,
            output_dir,
            len(data),
        )
        if hashlib.sha256(destination).hexdigest() != digest or destination != data:
            raise CaptureFileError("published baseline output changed during publication")
        os.fsync(output_descriptor)
        SCORER._assert_output_directory_identity(output_dir, output_descriptor)
        return digest
    except SCORER.ScoringError as error:
        raise CaptureFileError(str(error)) from error
    finally:
        try:
            if temporary_name:
                try:
                    os.unlink(temporary_name, dir_fd=output_descriptor)
                except FileNotFoundError:
                    pass
        finally:
            if temporary_descriptor is not None:
                os.close(temporary_descriptor)


def publish(
    output_dir: pathlib.Path,
    output_identity: str,
    stage_name: str,
    stage_identity: str,
    staged_name: str,
    final_name: str,
) -> str:
    if not final_name or final_name in {".", ".."} or pathlib.PurePath(final_name).name != final_name:
        raise CaptureFileError("invalid final capture filename")
    output_descriptor = _open_output(output_dir, output_identity)
    stage_descriptor: int | None = None
    source_descriptor: int | None = None
    try:
        stage_descriptor = _open_stage(output_descriptor, stage_name, stage_identity)
        source_descriptor, data = _read_stage_file(stage_descriptor, staged_name)
        return _publish_verified_bytes(output_descriptor, output_dir, final_name, data)
    finally:
        try:
            if source_descriptor is not None:
                os.close(source_descriptor)
        finally:
            try:
                if stage_descriptor is not None:
                    try:
                        os.unlink(staged_name, dir_fd=stage_descriptor)
                    except FileNotFoundError:
                        pass
                    finally:
                        os.close(stage_descriptor)
                    try:
                        current = os.stat(
                            stage_name,
                            dir_fd=output_descriptor,
                            follow_symlinks=False,
                        )
                        if (
                            _identity(current) == stage_identity
                            and stat.S_ISDIR(current.st_mode)
                        ):
                            os.rmdir(stage_name, dir_fd=output_descriptor)
                    except OSError:
                        pass
            finally:
                os.close(output_descriptor)


def cleanup(
    output_dir: pathlib.Path,
    output_identity: str,
    stage_name: str,
    stage_identity: str,
) -> None:
    output_descriptor = _open_output(output_dir, output_identity)
    try:
        try:
            stage_descriptor = _open_stage(output_descriptor, stage_name, stage_identity)
        except CaptureFileError:
            try:
                os.stat(stage_name, dir_fd=output_descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return
            raise
        try:
            for name in os.listdir(stage_descriptor):
                try:
                    os.unlink(name, dir_fd=stage_descriptor)
                except IsADirectoryError:
                    raise CaptureFileError("capture staging directory contains a subdirectory")
        finally:
            os.close(stage_descriptor)
        current = os.stat(stage_name, dir_fd=output_descriptor, follow_symlinks=False)
        if _identity(current) != stage_identity or not stat.S_ISDIR(current.st_mode):
            raise CaptureFileError("capture staging directory identity changed during cleanup")
        os.rmdir(stage_name, dir_fd=output_descriptor)
        os.fsync(output_descriptor)
    finally:
        os.close(output_descriptor)


def verify(
    output_dir: pathlib.Path,
    output_identity: str,
    final_name: str,
    expected_digest: str,
) -> None:
    output_descriptor = _open_output(output_dir, output_identity)
    try:
        data = SCORER._read_published_bytes(
            output_descriptor,
            final_name,
            output_dir,
            SCORER.MAX_JSON_INPUT_BYTES,
        )
        if hashlib.sha256(data).hexdigest() != expected_digest:
            raise CaptureFileError("published baseline output changed after publication")
        SCORER._assert_output_directory_identity(output_dir, output_descriptor)
    except SCORER.ScoringError as error:
        raise CaptureFileError(str(error)) from error
    finally:
        os.close(output_descriptor)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    stage_parser = subparsers.add_parser("stage")
    stage_parser.add_argument("output_dir", type=pathlib.Path)
    for action in ("publish", "cleanup", "verify"):
        child = subparsers.add_parser(action)
        child.add_argument("output_dir", type=pathlib.Path)
        child.add_argument("output_identity")
        if action in {"publish", "cleanup"}:
            child.add_argument("stage_name")
            child.add_argument("stage_identity")
        if action == "publish":
            child.add_argument("staged_name")
            child.add_argument("final_name")
        elif action == "verify":
            child.add_argument("final_name")
            child.add_argument("expected_digest")
    args = parser.parse_args(argv)
    try:
        if args.action == "stage":
            print("\t".join(create_stage(args.output_dir)))
        elif args.action == "publish":
            print(
                publish(
                    args.output_dir,
                    args.output_identity,
                    args.stage_name,
                    args.stage_identity,
                    args.staged_name,
                    args.final_name,
                )
            )
        elif args.action == "cleanup":
            cleanup(
                args.output_dir,
                args.output_identity,
                args.stage_name,
                args.stage_identity,
            )
        else:
            verify(
                args.output_dir,
                args.output_identity,
                args.final_name,
                args.expected_digest,
            )
    except CaptureFileError as error:
        parser.exit(1, f"error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
