// AnalyticsIncrementPayload.swift
// playhead-jw63.3 — turns a not-yet-uploaded delta into outbound records.
//
// One record per cohort, carrying the increments accrued since the last
// accepted upload. The server's job is to sum them; it is never asked to
// diff, join, or de-duplicate, because doing any of those would require the
// records to be linkable, and they are deliberately not.
//
// Every record is passed through `TelemetryEnvelopeV1AllowList.validate`
// before it is returned. A record that fails validation is dropped whole —
// not repaired, not partially emitted.

import Foundation

enum AnalyticsIncrementPayload {

    /// Builds validated outbound field dictionaries for a delta.
    ///
    /// Returns an empty array when there is nothing to send. Ordering is
    /// deterministic (by cohort) so the same delta always produces the same
    /// records, which is what makes the sentinel and shape tests meaningful.
    static func records(
        for delta: AnalyticsCounterTotals
    ) -> [[String: AnalyticsFieldValue]] {
        var byCohort: [AnalyticsCohortKey: [String: AnalyticsFieldValue]] = [:]

        for pair in delta.populatedPairs {
            var fields = byCohort[pair.cohort] ?? envelopeFields(for: pair.cohort)
            fields[pair.metric.rawValue] = .integer(pair.count)
            byCohort[pair.cohort] = fields
        }

        return byCohort.keys.sorted { $0.rawValue < $1.rawValue }
            .compactMap { cohort in
                byCohort[cohort].flatMap(TelemetryEnvelopeV1AllowList.validate)
            }
    }

    /// The three envelope fields every record carries.
    private static func envelopeFields(
        for cohort: AnalyticsCohortKey
    ) -> [String: AnalyticsFieldValue] {
        [
            AnalyticsEnvelopeFieldKey.envelopeVersion.rawValue:
                .integer(TelemetryEnvelopeV1AllowList.envelopeVersion),
            AnalyticsEnvelopeFieldKey.payloadSchema.rawValue:
                .token(TelemetryEnvelopeV1AllowList.payloadSchema),
            AnalyticsEnvelopeFieldKey.cohort.rawValue: .token(cohort.rawValue),
        ]
    }

    /// Stable textual rendering of a record: every key and every value, in
    /// sorted order, with nothing elided. Used by the egress sentinel test
    /// to assert absence over the *whole* payload rather than over the
    /// fields the test happened to think of.
    static func canonicalDescription(
        of fields: [String: AnalyticsFieldValue]
    ) -> String {
        fields.keys.sorted()
            .map { key in
                switch fields[key] {
                case .integer(let number): return "\(key)=\(number)"
                case .token(let token):    return "\(key)=\(token)"
                case nil:                  return "\(key)=nil"
                }
            }
            .joined(separator: "\n")
    }
}
