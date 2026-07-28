// AnalyticsEnvelopeAllowListTests.swift
// playhead-jw63.3 — the default-deny claim, made falsifiable.
//
// "Any field not explicitly listed is prohibited from leaving the device"
// (telemetry-envelope-v1 §3) is a claim about behavior, so it gets tests
// that fail if the behavior changes — including a frozen literal key set,
// so widening the allow-list cannot happen by accident. Widening it on
// purpose means updating the envelope addendum and getting counsel
// signoff; failing this test is the reminder.

import Foundation
import Testing

@testable import Playhead

@Suite("Telemetry envelope v1 — default-deny allow-list")
struct AnalyticsEnvelopeAllowListTests {

    /// The complete permitted key set, written out by hand. Compared
    /// against the code's derived set so neither can drift alone.
    private static let frozenKeys: Set<String> = [
        "envelope_version",
        "payload_schema",
        "cohort_duration_bucket",
        "banners_shown",
        "banners_confirmed",
        "banners_denied",
        "manual_skip_forward_reaches",
        "listening_seconds",
        "retention_installs",
        "retention_d1_returned",
        "retention_d7_returned",
        "retention_d30_returned",
    ]

    private static let frozenTokens: Set<String> = [
        "all",
        "under30m",
        "between30and60m",
        "between60and90m",
        "over90m",
        "playhead.analytics.increment.v1",
    ]

    private func validFields(
        cohort: String = "all",
        extra: [String: AnalyticsFieldValue] = [:]
    ) -> [String: AnalyticsFieldValue] {
        var fields: [String: AnalyticsFieldValue] = [
            "envelope_version": .integer(1),
            "payload_schema": .token("playhead.analytics.increment.v1"),
            "cohort_duration_bucket": .token(cohort),
            "banners_shown": .integer(3),
        ]
        for (key, value) in extra {
            fields[key] = value
        }
        return fields
    }

    @Test("The permitted key set is exactly the frozen list")
    func keySetIsFrozen() {
        #expect(TelemetryEnvelopeV1AllowList.permittedKeys == Self.frozenKeys)
    }

    @Test("The permitted string vocabulary is exactly the frozen list")
    func tokenVocabularyIsFrozen() {
        #expect(TelemetryEnvelopeV1AllowList.permittedTokens == Self.frozenTokens)
    }

    @Test("A well-formed record validates unchanged")
    func wellFormedRecordPasses() {
        let fields = validFields()
        #expect(TelemetryEnvelopeV1AllowList.validate(fields) == fields)
    }

    @Test("An unknown field key rejects the whole record")
    func unknownKeyRejects() {
        let fields = validFields(extra: ["episode_title": .integer(1)])
        #expect(TelemetryEnvelopeV1AllowList.validate(fields) == nil)
    }

    @Test("A key that merely resembles an allowed one still rejects")
    func nearMissKeyRejects() {
        for key in ["banners_shown_v2", "Banners_Shown", "listening_hours"] {
            let fields = validFields(extra: [key: .integer(1)])
            #expect(
                TelemetryEnvelopeV1AllowList.validate(fields) == nil,
                "\(key) must not be accepted by resemblance"
            )
        }
    }

    @Test("A string value outside the vocabulary rejects the record")
    func unknownTokenRejects() {
        let fields = validFields(cohort: "Ep. 127: The Kelly Ripa Interview")
        #expect(TelemetryEnvelopeV1AllowList.validate(fields) == nil)
    }

    @Test("A counter key may not carry a string value")
    func counterKeyRejectsToken() {
        let fields = validFields(extra: ["listening_seconds": .token("all")])
        #expect(TelemetryEnvelopeV1AllowList.validate(fields) == nil)
    }

    @Test("A token key may not carry an integer value")
    func tokenKeyRejectsInteger() {
        var fields = validFields()
        fields["cohort_duration_bucket"] = .integer(1)
        #expect(TelemetryEnvelopeV1AllowList.validate(fields) == nil)
    }

    @Test("A negative counter rejects the record")
    func negativeCounterRejects() {
        let fields = validFields(extra: ["banners_denied": .integer(-1)])
        #expect(TelemetryEnvelopeV1AllowList.validate(fields) == nil)
    }

    @Test("A record missing an envelope field rejects")
    func missingEnvelopeFieldRejects() {
        for missing in ["envelope_version", "payload_schema", "cohort_duration_bucket"] {
            var fields = validFields()
            fields[missing] = nil
            #expect(
                TelemetryEnvelopeV1AllowList.validate(fields) == nil,
                "a record without \(missing) must not be sent"
            )
        }
    }

    @Test("A record claiming a different envelope version rejects")
    func wrongEnvelopeVersionRejects() {
        var fields = validFields()
        fields["envelope_version"] = .integer(2)
        #expect(TelemetryEnvelopeV1AllowList.validate(fields) == nil)
    }

    @Test("A record claiming a different schema rejects")
    func wrongSchemaRejects() {
        var fields = validFields()
        fields["payload_schema"] = .token("all")
        #expect(TelemetryEnvelopeV1AllowList.validate(fields) == nil)
    }

    @Test("Every metric and cohort case is representable in the allow-list")
    func everyLiveCaseIsPermitted() {
        for metric in AnalyticsMetricKey.allCases {
            #expect(TelemetryEnvelopeV1AllowList.permittedKeys.contains(metric.rawValue))
        }
        for cohort in AnalyticsCohortKey.allCases {
            #expect(TelemetryEnvelopeV1AllowList.permittedTokens.contains(cohort.rawValue))
        }
    }

    @Test("Upload stays gated until counsel signs the envelope addendum")
    func uploadGateIsClosed() {
        // telemetry-envelope-v1 §7: non-local transport paths stay behind a
        // flag until signoff. Flipping this constant is a deliberate act
        // that must land with the signature recorded in the addendum.
        #expect(AnalyticsUploadGate.legalSignoffRecorded == false)
        #expect(AnalyticsUploadGate.isAutomaticUploadPermitted == false)
    }
}
