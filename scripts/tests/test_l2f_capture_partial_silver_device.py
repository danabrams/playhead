#!/usr/bin/env python3
"""Contract tests for the physical-iPhone partial-silver capture wrapper."""

from __future__ import annotations

import hashlib
import json
import pathlib
import plistlib
import subprocess
import tempfile
import unittest
from unittest import mock

from scripts import l2f_capture_device


ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/l2f-capture-partial-silver-device.sh"
DEVICE_ID = "00008140-001609A42660801C"


class PartialSilverDeviceCaptureContractTests(unittest.TestCase):
    def test_stage_rejects_transcript_bound_to_different_audio(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source = root / "source"
            corpus = root / "corpus"
            staging = root / "staging"
            evaluation_relative = pathlib.Path("Evaluations/evaluation.json")
            assets = []
            for index in range(27):
                episode_id = f"episode-{index:02d}"
                audio = f"audio-{index}".encode()
                fingerprint = "sha256:" + hashlib.sha256(audio).hexdigest()
                assets.append(
                    {
                        "episode_id": episode_id,
                        "audio_fingerprint": fingerprint,
                    }
                )
                audio_path = corpus / "TestFixtures/Corpus/Audio" / f"{episode_id}.mp3"
                audio_path.parent.mkdir(parents=True, exist_ok=True)
                audio_path.write_bytes(audio)
                transcript_path = (
                    corpus
                    / "TestFixtures/Corpus/Transcripts"
                    / f"{episode_id}.json"
                )
                transcript_path.parent.mkdir(parents=True, exist_ok=True)
                transcript_fingerprint = fingerprint
                if index == 0:
                    transcript_fingerprint = "sha256:" + "0" * 64
                transcript_path.write_text(
                    json.dumps(
                        {
                            "source_audio_fingerprint": transcript_fingerprint,
                            "transcription": [
                                {
                                    "text": "words",
                                    "offsets": {"from": 0, "to": 1000},
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
            evaluation = {"schema_version": 1, "assets": assets}
            evaluation_bytes = json.dumps(evaluation).encode()
            evaluation_path = source / evaluation_relative
            evaluation_path.parent.mkdir(parents=True)
            evaluation_path.write_bytes(evaluation_bytes)

            with mock.patch.object(
                l2f_capture_device,
                "EVALUATION_RELATIVE_PATH",
                evaluation_relative,
            ), mock.patch.object(
                l2f_capture_device,
                "EVALUATION_SHA256",
                hashlib.sha256(evaluation_bytes).hexdigest(),
            ):
                with self.assertRaisesRegex(
                    l2f_capture_device.DeviceCaptureError,
                    "transcript source audio fingerprint differs for episode-00",
                ):
                    l2f_capture_device.stage_inputs(
                        source,
                        corpus,
                        staging,
                        "a" * 40,
                        "baseline-run-1",
                    )

    def test_xctestrun_patch_targets_only_playhead_tests_and_preserves_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source = root / "source.xctestrun"
            destination = root / "destination.xctestrun"
            document = {
                "TestConfigurations": [
                    {
                        "TestTargets": [
                            {
                                "BlueprintName": "PlayheadTests",
                                "TestBundlePath": "__TESTHOST__/PlugIns/PlayheadTests.xctest",
                                "EnvironmentVariables": {"EXISTING": "kept"},
                            },
                            {
                                "BlueprintName": "OtherTests",
                                "EnvironmentVariables": {"UNCHANGED": "yes"},
                            },
                        ]
                    }
                ]
            }
            with source.open("wb") as handle:
                plistlib.dump(document, handle)

            l2f_capture_device.patch_xctestrun(
                source,
                destination,
                {"PLAYHEAD_BASELINE_DEVICE_MODE": "1"},
            )

            with destination.open("rb") as handle:
                patched = plistlib.load(handle)
            targets = patched["TestConfigurations"][0]["TestTargets"]
            self.assertEqual(
                targets[0]["EnvironmentVariables"],
                {"EXISTING": "kept", "PLAYHEAD_BASELINE_DEVICE_MODE": "1"},
            )
            self.assertEqual(targets[1]["EnvironmentVariables"], {"UNCHANGED": "yes"})
            with source.open("rb") as handle:
                self.assertEqual(plistlib.load(handle), document)

    def test_device_identity_requires_wired_physical_ios(self) -> None:
        device = {
            "identifier": "core-device-id",
            "hardwareProperties": {
                "udid": DEVICE_ID,
                "reality": "physical",
                "platform": "iOS",
                "marketingName": "iPhone 16 Pro Max",
                "productType": "iPhone17,2",
            },
            "deviceProperties": {
                "bootState": "booted",
                "developerModeStatus": "enabled",
                "ddiServicesAvailable": True,
                "osVersionNumber": "27.0",
                "osBuildUpdate": "24A5380h",
            },
            "connectionProperties": {
                "pairingState": "paired",
                "transportType": "wired",
                "tunnelState": "connected",
            },
        }
        document = {"result": {"devices": [device]}}

        identity = l2f_capture_device.select_device(document, DEVICE_ID)

        self.assertEqual(identity["core_device_identifier"], "core-device-id")
        device["hardwareProperties"]["reality"] = "simulated"
        with self.assertRaisesRegex(
            l2f_capture_device.DeviceCaptureError,
            "device reality must be physical",
        ):
            l2f_capture_device.select_device(document, DEVICE_ID)

    def test_lock_state_must_be_unlocked_and_match_device_identity(self) -> None:
        lock_state = {
            "result": {
                "deviceIdentifier": "core-device-id",
                "passcodeRequired": False,
                "unlockedSinceBoot": True,
            }
        }
        l2f_capture_device.validate_lock_state(lock_state, "core-device-id")

        lock_state["result"]["passcodeRequired"] = True
        with self.assertRaisesRegex(
            l2f_capture_device.DeviceCaptureError,
            "locked and requires its passcode",
        ):
            l2f_capture_device.validate_lock_state(lock_state, "core-device-id")

        lock_state["result"]["passcodeRequired"] = False
        lock_state["result"]["deviceIdentifier"] = "different-device"
        with self.assertRaisesRegex(
            l2f_capture_device.DeviceCaptureError,
            "different CoreDevice identity",
        ):
            l2f_capture_device.validate_lock_state(lock_state, "core-device-id")

    def test_transcript_lineage_must_bind_to_evaluation_audio(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            transcript = pathlib.Path(temporary) / "episode.json"
            transcript.write_text(
                '{"source_audio_fingerprint":"sha256:'
                + "b" * 64
                + '","transcription":[{"text":"words","offsets":{"from":0,"to":1000}}]}\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                l2f_capture_device.DeviceCaptureError,
                "source audio fingerprint differs",
            ):
                l2f_capture_device.validate_transcript_audio_fingerprint(
                    transcript,
                    "episode",
                    "sha256:" + "a" * 64,
                )
            transcript.write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(
                l2f_capture_device.DeviceCaptureError,
                "lacks source_audio_fingerprint",
            ):
                l2f_capture_device.transcript_audio_fingerprint(transcript, "episode")

    def test_transcript_structure_must_match_the_production_loader_contract(self) -> None:
        fingerprint = "sha256:" + "a" * 64
        invalid_documents = [
            {"source_audio_fingerprint": fingerprint},
            {"source_audio_fingerprint": fingerprint, "transcription": []},
            {
                "source_audio_fingerprint": fingerprint,
                "transcription": [{"text": "words", "offsets": {"from": 0}}],
            },
            {
                "source_audio_fingerprint": fingerprint,
                "transcription": [
                    {"text": "words", "offsets": {"from": False, "to": 1000}}
                ],
            },
        ]
        with tempfile.TemporaryDirectory() as temporary:
            transcript = pathlib.Path(temporary) / "episode.json"
            for document in invalid_documents:
                with self.subTest(document=document):
                    transcript.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(l2f_capture_device.DeviceCaptureError):
                        l2f_capture_device.transcript_audio_fingerprint(
                            transcript,
                            "episode",
                        )

    def test_raw_validation_requires_the_exact_staged_episode_membership(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            run_id = "baseline-run-1"
            revision = "c" * 40
            device = {
                "udid": DEVICE_ID,
                "os_version": "27.0",
                "os_build": "24A5380h",
            }
            expected_ids = [f"episode-{index:02d}" for index in range(27)]
            manifest = {
                "artifact_kind": "physical_device_partial_silver_staging",
                "schema_version": 1,
                "run_id": run_id,
                "source_revision": revision,
                "evaluation_sha256": l2f_capture_device.EVALUATION_SHA256,
                "assets": [{"episode_id": episode_id} for episode_id in expected_ids],
            }
            raw = {
                "artifact_kind": "unchanged_production_partial_silver_raw",
                "schema_version": 1,
                "run_id": run_id,
                "source_revision": revision,
                "evaluation_sha256": l2f_capture_device.EVALUATION_SHA256,
                "episodes": [{"episode_id": episode_id} for episode_id in expected_ids],
                "runtime": {
                    "architecture": "arm64",
                    "capture_lane": "physical_ios",
                    "device_udid": DEVICE_ID,
                    "device_os_build": "24A5380h",
                    "foundation_models_availability": "available",
                    "os_version": "Version 27.0",
                },
            }
            manifest_path = root / "manifest.json"
            raw_path = root / "raw.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            raw_path.write_text(json.dumps(raw), encoding="utf-8")

            l2f_capture_device.validate_raw(
                raw_path,
                run_id,
                revision,
                device,
                manifest_path,
            )
            raw["episodes"][-1]["episode_id"] = "wrong-episode"
            raw_path.write_text(json.dumps(raw), encoding="utf-8")
            with self.assertRaisesRegex(
                l2f_capture_device.DeviceCaptureError,
                "episode identity",
            ):
                l2f_capture_device.validate_raw(
                    raw_path,
                    run_id,
                    revision,
                    device,
                    manifest_path,
                )

    def test_wrapper_has_fixed_device_and_explicit_app_container_transport(self) -> None:
        self.assertTrue(SCRIPT.is_file(), "device capture wrapper is missing")
        source = SCRIPT.read_text(encoding="utf-8")

        self.assertIn(DEVICE_ID, source)
        self.assertIn("--domain-type appDataContainer", source)
        self.assertIn("--domain-identifier", source)
        self.assertIn("--destination Documents/l2f8/", source)
        self.assertNotIn("--destination Documents\n", source)
        self.assertIn("build-for-testing", source)
        self.assertIn("test-without-building", source)
        self.assertIn("plistlib", source)
        self.assertIn("json.load", source)
        self.assertIn("--preflight-only", source)
        self.assertIn("paste -sd ' ' -", source)
        self.assertNotIn("tr '\\n' ' '", source)
        self.assertIn("signing key is unavailable", source)
        self.assertNotIn("security unlock-keychain", source)
        self.assertIn('CODE_SIGN_IDENTITY="$SIGNING_IDENTITY"', source)
        self.assertIn("--extract-certificates", source)
        self.assertIn('verify_signing_leaf "$app_path"', source)
        self.assertIn('"$app_path/PlugIns/PlayheadTests.xctest"', source)
        self.assertIn('xctestrun_directory="$(dirname "$xctestrun")"', source)
        self.assertNotIn('$host_temp/preflight.xctestrun', source)

    def test_wrapper_rejects_unknown_run_before_invoking_xcode(self) -> None:
        self.assertTrue(SCRIPT.is_file(), "device capture wrapper is missing")
        result = subprocess.run(
            [
                "bash",
                str(SCRIPT),
                "--run-id",
                "unreviewed-run",
                "--output",
                "/tmp/playhead-partial-silver-baseline-unreviewed-run.json",
                "--preflight-only",
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("baseline-run-{1,2,3}", result.stderr)


if __name__ == "__main__":
    unittest.main()
