// PipelineVersionsTests.swift
// playhead-zx6i — Unit tests for the version-triple value type that
// drives the B4 revalidation-from-features short-circuit.

import Foundation
import Testing
@testable import Playhead

@Suite("PipelineVersions")
struct PipelineVersionsTests {

    @Test("current() reads the canonical source-of-truth for each axis")
    func currentReadsCanonicalSources() {
        let current = PipelineVersions.current()

        // Each axis must match the canonical source from
        // `SharedVersionConstants` doc — duplicated literals are a
        // code-review block. The test asserts the values match the
        // sources the production code reads from, which prevents a
        // future refactor from silently pointing `current()` at a
        // stale copy.
        #expect(current.modelVersion == AdDetectionConfig.default.detectorVersion)
        #expect(current.policyVersion == SkipPolicyConfig.default.policyVersion)
        #expect(current.featureSchemaVersion == SharedVersionConstants.featureSchemaVersion)
    }

    @Test("equality is field-wise across all three axes")
    func equalityFieldWise() {
        let a = PipelineVersions(
            modelVersion: "m1",
            policyVersion: "p1",
            featureSchemaVersion: 1
        )
        let b = PipelineVersions(
            modelVersion: "m1",
            policyVersion: "p1",
            featureSchemaVersion: 1
        )
        #expect(a == b)

        // Each axis differs in isolation → not equal.
        let differsOnModel = PipelineVersions(modelVersion: "m2", policyVersion: "p1", featureSchemaVersion: 1)
        let differsOnPolicy = PipelineVersions(modelVersion: "m1", policyVersion: "p2", featureSchemaVersion: 1)
        let differsOnFeatureSchema = PipelineVersions(modelVersion: "m1", policyVersion: "p1", featureSchemaVersion: 2)
        #expect(a != differsOnModel)
        #expect(a != differsOnPolicy)
        #expect(a != differsOnFeatureSchema)
    }

    @Test("Codable round-trips byte-stably")
    func codableRoundTrips() throws {
        let original = PipelineVersions(
            modelVersion: "detection-v2",
            policyVersion: "skip-policy-v3",
            featureSchemaVersion: 7
        )
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(PipelineVersions.self, from: data)
        #expect(decoded == original)
    }

    @Test("sevenMqSentinel matches the playhead-7mq schema defaults")
    func sevenMqSentinelMatchesSchemaDefaults() {
        // The 7mq migration writes these literal default values into
        // pre-existing rows. If those defaults ever change in the
        // schema migration, this test (and the consumer
        // documentation in `RevalidationStateStore`) need to be
        // updated together.
        #expect(PipelineVersions.sevenMqSentinel.modelVersion == "pre-instrumentation")
        #expect(PipelineVersions.sevenMqSentinel.policyVersion == "0")
        #expect(PipelineVersions.sevenMqSentinel.featureSchemaVersion == 0)
    }
}
