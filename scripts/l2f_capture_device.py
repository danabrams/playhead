#!/usr/bin/env python3
"""Structured host helpers for the physical-device partial-silver capture."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import plistlib
import shutil
import stat
import sys
from typing import Any


EVALUATION_SHA256 = "0d85a0ec8bfa30873bad63bbc4bb12a3f7613aca76d5b76149e25db2a0be226f"
EVALUATION_RELATIVE_PATH = pathlib.Path(
    "TestFixtures/Corpus/Evaluations"
) / f"earaudit-partial-silver-{EVALUATION_SHA256}.json"
EXPECTED_ASSET_COUNT = 27
AUDIO_EXTENSIONS = {".aac", ".caf", ".flac", ".m4a", ".mp3", ".wav"}


class DeviceCaptureError(RuntimeError):
    """A deterministic capture input or metadata validation failure."""


def _load_json(path: pathlib.Path, description: str) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DeviceCaptureError(f"cannot read {description}: {path}: {error}") from error


def _regular_file(path: pathlib.Path, description: str) -> pathlib.Path:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise DeviceCaptureError(f"missing {description}: {path}") from error
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise DeviceCaptureError(f"{description} is not a regular file: {path}")
    return path


def _sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _copy_regular(source: pathlib.Path, destination: pathlib.Path) -> None:
    _regular_file(source, "staging input")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as source_handle, destination.open("xb") as destination_handle:
        shutil.copyfileobj(source_handle, destination_handle, length=1024 * 1024)


def transcript_audio_fingerprint(path: pathlib.Path, episode_id: str) -> str:
    document = _load_json(_regular_file(path, f"transcript for {episode_id}"), "transcript")
    if not isinstance(document, dict):
        raise DeviceCaptureError(f"transcript JSON is not an object for {episode_id}")
    fingerprint = document.get("source_audio_fingerprint")
    if not isinstance(fingerprint, str):
        raise DeviceCaptureError(
            f"transcript lacks source_audio_fingerprint for {episode_id}"
        )
    transcription = document.get("transcription")
    if not isinstance(transcription, list) or not transcription:
        raise DeviceCaptureError(
            f"transcript has no non-empty transcription array for {episode_id}"
        )
    for index, segment in enumerate(transcription):
        offsets = segment.get("offsets") if isinstance(segment, dict) else None
        if (
            not isinstance(segment, dict)
            or not isinstance(segment.get("text"), str)
            or not isinstance(offsets, dict)
            or type(offsets.get("from")) is not int
            or type(offsets.get("to")) is not int
        ):
            raise DeviceCaptureError(
                f"transcript segment {index} is invalid for {episode_id}"
            )
    return fingerprint


def validate_transcript_audio_fingerprint(
    path: pathlib.Path,
    episode_id: str,
    expected: str,
) -> str:
    fingerprint = transcript_audio_fingerprint(path, episode_id)
    if fingerprint != expected:
        raise DeviceCaptureError(
            f"transcript source audio fingerprint differs for {episode_id}"
        )
    return fingerprint


def stage_inputs(
    source_root: pathlib.Path,
    corpus_root: pathlib.Path,
    staging_root: pathlib.Path,
    revision: str,
    run_id: str,
) -> dict[str, Any]:
    evaluation_path = _regular_file(
        source_root / EVALUATION_RELATIVE_PATH,
        "partial-silver evaluation",
    )
    if _sha256(evaluation_path) != EVALUATION_SHA256:
        raise DeviceCaptureError("partial-silver evaluation content address differs")
    evaluation = _load_json(evaluation_path, "partial-silver evaluation")
    if not isinstance(evaluation, dict) or evaluation.get("schema_version") != 1:
        raise DeviceCaptureError("partial-silver evaluation schema is invalid")
    assets = evaluation.get("assets")
    if not isinstance(assets, list) or len(assets) != EXPECTED_ASSET_COUNT:
        raise DeviceCaptureError("partial-silver evaluation must contain exactly 27 assets")
    episode_ids = [asset.get("episode_id") for asset in assets if isinstance(asset, dict)]
    if (
        len(episode_ids) != EXPECTED_ASSET_COUNT
        or any(not isinstance(value, str) or not value for value in episode_ids)
        or len(set(episode_ids)) != EXPECTED_ASSET_COUNT
    ):
        raise DeviceCaptureError("partial-silver evaluation episode identity is invalid")
    if staging_root.exists() and any(staging_root.iterdir()):
        raise DeviceCaptureError(f"staging root is not empty: {staging_root}")
    staging_root.mkdir(parents=True, exist_ok=True)
    _copy_regular(evaluation_path, staging_root / EVALUATION_RELATIVE_PATH)

    audio_directory = corpus_root / "TestFixtures/Corpus/Audio"
    transcript_directory = corpus_root / "TestFixtures/Corpus/Transcripts"
    bindings: list[dict[str, str]] = []
    for asset in sorted(assets, key=lambda value: value["episode_id"]):
        episode_id = asset["episode_id"]
        fingerprint = asset.get("audio_fingerprint")
        if (
            not isinstance(fingerprint, str)
            or not fingerprint.startswith("sha256:")
            or len(fingerprint) != 71
        ):
            raise DeviceCaptureError(f"invalid audio fingerprint for {episode_id}")
        audio_candidates = [
            path
            for path in audio_directory.iterdir()
            if path.stem == episode_id and path.suffix.lower() in AUDIO_EXTENSIONS
        ] if audio_directory.is_dir() else []
        if len(audio_candidates) != 1:
            raise DeviceCaptureError(
                f"expected exactly one retained audio file for {episode_id}, "
                f"found {len(audio_candidates)}"
            )
        audio = _regular_file(audio_candidates[0], f"retained audio for {episode_id}")
        audio_sha256 = _sha256(audio)
        if f"sha256:{audio_sha256}" != fingerprint:
            raise DeviceCaptureError(f"retained audio fingerprint differs for {episode_id}")
        transcript = _regular_file(
            transcript_directory / f"{episode_id}.json",
            f"transcript for {episode_id}",
        )
        transcript_fingerprint = validate_transcript_audio_fingerprint(
            transcript,
            episode_id,
            fingerprint,
        )
        transcript_sha256 = _sha256(transcript)
        audio_relative = pathlib.Path("TestFixtures/Corpus/Audio") / audio.name
        transcript_relative = (
            pathlib.Path("TestFixtures/Corpus/Transcripts") / transcript.name
        )
        _copy_regular(audio, staging_root / audio_relative)
        _copy_regular(transcript, staging_root / transcript_relative)
        bindings.append(
            {
                "episode_id": episode_id,
                "audio_path": audio_relative.as_posix(),
                "audio_sha256": audio_sha256,
                "transcript_path": transcript_relative.as_posix(),
                "transcript_sha256": transcript_sha256,
                "transcript_audio_fingerprint": transcript_fingerprint,
            }
        )

    (staging_root / "output").mkdir()
    manifest = {
        "artifact_kind": "physical_device_partial_silver_staging",
        "schema_version": 1,
        "source_revision": revision,
        "evaluation_sha256": EVALUATION_SHA256,
        "run_id": run_id,
        "assets": bindings,
    }
    manifest_path = staging_root / "playhead-l2f8-device-staging.json"
    manifest_path.write_bytes(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True).encode("utf-8")
        + b"\n"
    )
    return manifest


def select_device(document: Any, udid: str) -> dict[str, str]:
    try:
        devices = document["result"]["devices"]
    except (KeyError, TypeError) as error:
        raise DeviceCaptureError("devicectl JSON has no result.devices array") from error
    matches = [
        device for device in devices
        if device.get("hardwareProperties", {}).get("udid") == udid
    ]
    if len(matches) != 1:
        raise DeviceCaptureError(f"expected exactly one connected device for UDID {udid}")
    device = matches[0]
    hardware = device.get("hardwareProperties", {})
    properties = device.get("deviceProperties", {})
    connection = device.get("connectionProperties", {})
    required = {
        "reality": (hardware.get("reality"), "physical"),
        "platform": (hardware.get("platform"), "iOS"),
        "pairing": (connection.get("pairingState"), "paired"),
        "transport": (connection.get("transportType"), "wired"),
        "tunnel": (connection.get("tunnelState"), "connected"),
        "boot": (properties.get("bootState"), "booted"),
        "developer mode": (properties.get("developerModeStatus"), "enabled"),
    }
    for label, (actual, expected) in required.items():
        if actual != expected:
            raise DeviceCaptureError(
                f"device {label} must be {expected}, found {actual!r}"
            )
    if properties.get("ddiServicesAvailable") is not True:
        raise DeviceCaptureError("device developer services are unavailable")
    os_version = properties.get("osVersionNumber")
    os_build = properties.get("osBuildUpdate")
    try:
        os_major = int(os_version.split(".", 1)[0]) if isinstance(os_version, str) else 0
    except ValueError:
        os_major = 0
    if os_major < 27:
        raise DeviceCaptureError(f"device must run iOS 27 or newer, found {os_version!r}")
    if not isinstance(os_build, str) or not os_build:
        raise DeviceCaptureError("device OS build is unavailable")
    core_device_identifier = device.get("identifier")
    if not isinstance(core_device_identifier, str) or not core_device_identifier:
        raise DeviceCaptureError("CoreDevice identifier is unavailable")
    return {
        "core_device_identifier": core_device_identifier,
        "udid": udid,
        "marketing_name": str(hardware.get("marketingName", "")),
        "product_type": str(hardware.get("productType", "")),
        "os_version": os_version,
        "os_build": os_build,
    }


def validate_lock_state(document: Any, core_device_identifier: str) -> None:
    try:
        result = document["result"]
    except (KeyError, TypeError) as error:
        raise DeviceCaptureError("devicectl lock-state JSON has no result object") from error
    if result.get("deviceIdentifier") != core_device_identifier:
        raise DeviceCaptureError("lock state belongs to a different CoreDevice identity")
    if result.get("passcodeRequired") is not False:
        raise DeviceCaptureError("physical device is locked and requires its passcode")
    if result.get("unlockedSinceBoot") is not True:
        raise DeviceCaptureError("physical device has not been unlocked since boot")


def patch_xctestrun(
    source: pathlib.Path,
    destination: pathlib.Path,
    environment: dict[str, str],
) -> None:
    with source.open("rb") as handle:
        document = plistlib.load(handle)
    targets: list[dict[str, Any]] = []

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            bundle_path = value.get("TestBundlePath", "")
            if value.get("BlueprintName") == "PlayheadTests" or (
                isinstance(bundle_path, str) and "PlayheadTests.xctest" in bundle_path
            ):
                targets.append(value)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)
    unique_targets = {id(target): target for target in targets}
    if len(unique_targets) != 1:
        raise DeviceCaptureError(
            f"expected one PlayheadTests target in xctestrun, found {len(unique_targets)}"
        )
    target = next(iter(unique_targets.values()))
    existing = target.get("EnvironmentVariables", {})
    if not isinstance(existing, dict):
        raise DeviceCaptureError("PlayheadTests EnvironmentVariables is not a dictionary")
    target["EnvironmentVariables"] = {**existing, **environment}
    target["IsEnabled"] = True
    with destination.open("xb") as handle:
        plistlib.dump(document, handle, fmt=plistlib.FMT_XML, sort_keys=True)


def validate_preflight(
    path: pathlib.Path,
    run_id: str,
    revision: str,
    device: dict[str, str],
) -> None:
    document = _load_json(_regular_file(path, "retrieved preflight"), "preflight")
    expected = {
        "artifact_kind": "physical_device_partial_silver_preflight",
        "schema_version": 1,
        "run_id": run_id,
        "source_revision": revision,
        "evaluation_sha256": EVALUATION_SHA256,
        "device_udid": device["udid"],
        "expected_os_version": device["os_version"],
        "expected_os_build": device["os_build"],
        "staged_asset_count": EXPECTED_ASSET_COUNT,
        "output_transport_writable": True,
    }
    for key, value in expected.items():
        if document.get(key) != value:
            raise DeviceCaptureError(
                f"retrieved preflight {key} differs: {document.get(key)!r} != {value!r}"
            )
    runtime = document.get("runtime", {})
    if runtime.get("architecture") != "arm64" or (
        runtime.get("capture_lane") != "physical_ios"
    ):
        raise DeviceCaptureError("preflight did not execute on physical arm64 iOS")
    if runtime.get("device_udid") != device["udid"]:
        raise DeviceCaptureError("preflight runtime device differs from the pinned device")
    if runtime.get("device_os_build") != device["os_build"]:
        raise DeviceCaptureError("preflight runtime OS build differs from the pinned device")
    if runtime.get("foundation_models_availability") != "available":
        raise DeviceCaptureError("preflight Foundation Models runtime is unavailable")
    if device["os_version"] not in str(runtime.get("os_version", "")):
        raise DeviceCaptureError("preflight runtime OS differs from the connected device")


def validate_raw(
    path: pathlib.Path,
    run_id: str,
    revision: str,
    device: dict[str, str],
    staging_manifest_path: pathlib.Path,
) -> None:
    document = _load_json(_regular_file(path, "retrieved raw capture"), "raw capture")
    expected = {
        "artifact_kind": "unchanged_production_partial_silver_raw",
        "schema_version": 1,
        "run_id": run_id,
        "source_revision": revision,
        "evaluation_sha256": EVALUATION_SHA256,
    }
    for key, value in expected.items():
        if document.get(key) != value:
            raise DeviceCaptureError(f"retrieved raw capture {key} differs")
    episodes = document.get("episodes")
    if not isinstance(episodes, list) or len(episodes) != EXPECTED_ASSET_COUNT:
        raise DeviceCaptureError("retrieved raw capture does not contain exactly 27 episodes")
    episode_ids = [episode.get("episode_id") for episode in episodes if isinstance(episode, dict)]
    manifest = _load_json(
        _regular_file(staging_manifest_path, "host staging manifest"),
        "host staging manifest",
    )
    if not isinstance(manifest, dict):
        raise DeviceCaptureError("host staging manifest is not a JSON object")
    manifest_assets = manifest.get("assets")
    expected_episode_ids = {
        asset.get("episode_id")
        for asset in manifest_assets
        if isinstance(asset, dict) and isinstance(asset.get("episode_id"), str)
    } if isinstance(manifest_assets, list) else set()
    if (
        manifest.get("artifact_kind") != "physical_device_partial_silver_staging"
        or manifest.get("schema_version") != 1
        or manifest.get("run_id") != run_id
        or manifest.get("source_revision") != revision
        or manifest.get("evaluation_sha256") != EVALUATION_SHA256
        or not isinstance(manifest_assets, list)
        or len(manifest_assets) != EXPECTED_ASSET_COUNT
        or len(expected_episode_ids) != EXPECTED_ASSET_COUNT
        or len(episode_ids) != EXPECTED_ASSET_COUNT
        or any(not isinstance(episode_id, str) for episode_id in episode_ids)
        or set(episode_ids) != expected_episode_ids
    ):
        raise DeviceCaptureError("retrieved raw capture episode identity is invalid")
    runtime = document.get("runtime", {})
    if (
        runtime.get("architecture") != "arm64"
        or runtime.get("capture_lane") != "physical_ios"
        or runtime.get("device_udid") != device["udid"]
        or runtime.get("device_os_build") != device["os_build"]
        or runtime.get("foundation_models_availability") != "available"
    ):
        raise DeviceCaptureError("retrieved raw capture is not a live physical-iOS FM run")
    if device["os_version"] not in str(runtime.get("os_version", "")):
        raise DeviceCaptureError("retrieved raw runtime differs from the pinned device OS")


def _parse_key_values(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key:
            raise DeviceCaptureError(f"invalid environment assignment: {value}")
        result[key] = item
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    stage = subparsers.add_parser("stage")
    stage.add_argument("source_root", type=pathlib.Path)
    stage.add_argument("corpus_root", type=pathlib.Path)
    stage.add_argument("staging_root", type=pathlib.Path)
    stage.add_argument("revision")
    stage.add_argument("run_id")
    device = subparsers.add_parser("device-info")
    device.add_argument("json_path", type=pathlib.Path)
    device.add_argument("udid")
    patch = subparsers.add_parser("patch-xctestrun")
    patch.add_argument("source", type=pathlib.Path)
    patch.add_argument("destination", type=pathlib.Path)
    patch.add_argument("environment", nargs="*")
    lock_state = subparsers.add_parser("lock-state")
    lock_state.add_argument("json_path", type=pathlib.Path)
    lock_state.add_argument("core_device_identifier")
    preflight = subparsers.add_parser("validate-preflight")
    preflight.add_argument("path", type=pathlib.Path)
    preflight.add_argument("run_id")
    preflight.add_argument("revision")
    preflight.add_argument("device_json", type=pathlib.Path)
    raw = subparsers.add_parser("validate-raw")
    raw.add_argument("path", type=pathlib.Path)
    raw.add_argument("run_id")
    raw.add_argument("revision")
    raw.add_argument("device_json", type=pathlib.Path)
    raw.add_argument("staging_manifest", type=pathlib.Path)
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "stage":
            manifest = stage_inputs(
                arguments.source_root,
                arguments.corpus_root,
                arguments.staging_root,
                arguments.revision,
                arguments.run_id,
            )
            print(json.dumps({"assets": len(manifest["assets"])}, sort_keys=True))
        elif arguments.command == "device-info":
            document = _load_json(arguments.json_path, "device list")
            print(json.dumps(select_device(document, arguments.udid), sort_keys=True))
        elif arguments.command == "patch-xctestrun":
            patch_xctestrun(
                arguments.source,
                arguments.destination,
                _parse_key_values(arguments.environment),
            )
        elif arguments.command == "lock-state":
            validate_lock_state(
                _load_json(arguments.json_path, "device lock state"),
                arguments.core_device_identifier,
            )
        elif arguments.command in {"validate-preflight", "validate-raw"}:
            device_info = _load_json(arguments.device_json, "pinned device identity")
            validator = (
                validate_preflight
                if arguments.command == "validate-preflight"
                else validate_raw
            )
            if arguments.command == "validate-preflight":
                validator(arguments.path, arguments.run_id, arguments.revision, device_info)
            else:
                validator(
                    arguments.path,
                    arguments.run_id,
                    arguments.revision,
                    device_info,
                    arguments.staging_manifest,
                )
        return 0
    except DeviceCaptureError as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
