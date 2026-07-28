// AnalyticsEnvelope.swift
// playhead-jw63.3 — the binding shape of anything this app may send to the
// analytics transport, and the default-deny gate that enforces it.
//
// The contract is `docs/legal/telemetry-envelope-v1.md` plus
// `docs/legal/telemetry-envelope-v1-addendum-a-product-counters.md`
// (the addendum that proposes the nine product counters below; PENDING
// LEGAL REVIEW — see `AnalyticsUploadGate`).
//
// Two structural defences, in this order:
//
//   1. **Closed types.** Every key is an enum case and every string value is
//      an enum raw value. There is no `[String: Any]` anywhere on the path
//      from a counter to a record, so a free-form string — an episode title,
//      a feed URL, a transcript line — has no representation it could travel
//      in. This is the defence that matters; the validator below is the
//      belt to its suspenders.
//   2. **Default-deny validation.** `TelemetryEnvelopeV1AllowList.validate`
//      re-checks a materialized field dictionary against a frozen key set
//      *and* a frozen token vocabulary immediately before it becomes an
//      outbound record. A key it does not recognize, or a string value that
//      is not one of the ~14 permitted tokens, rejects the whole record —
//      it never truncates, never redacts, never partially emits. A dropped
//      counter is a rounding error; a leaked title is a breach.
//
// Both are exercised by `PlayheadTests/Services/Analytics/`:
// `AnalyticsEnvelopeAllowListTests` (default-deny) and
// `AnalyticsEgressSentinelTests` (a fake episode title / feed URL /
// transcript line seeded at the top of the pipeline, asserted absent from
// the encoded bytes).

import Foundation

// MARK: - Cohort key

/// The single cohort axis v1 keys by: episode duration bucket, per
/// `SLICohortAxes.swift`, plus `all` for counters that are not
/// episode-scoped (banner feedback and retention).
///
/// The other three SLI axes (trigger × analysis mode × execution
/// condition) are deliberately absent in v1: none of the five product
/// counters can supply them at their measurement point without inventing
/// a parallel counting path, and an axis emitted as a constant is not a
/// cohort — it is a re-identification surface with no analytic value.
enum AnalyticsCohortKey: String, CaseIterable, Sendable, Hashable, Codable {
    case all
    case under30m
    case between30and60m
    case between60and90m
    case over90m

    /// Lifts an SLI duration bucket into the cohort vocabulary.
    init(durationBucket: SLIEpisodeDurationBucket) {
        switch durationBucket {
        case .under30m:        self = .under30m
        case .between30and60m: self = .between30and60m
        case .between60and90m: self = .between60and90m
        case .over90m:         self = .over90m
        }
    }
}

// MARK: - Metric keys

/// The complete set of counters v1 may increment. Adding a case here is a
/// change to what leaves the device and requires an envelope amendment
/// plus counsel signoff — `AnalyticsEnvelopeAllowListTests` pins the set
/// against a frozen literal so the requirement cannot be forgotten.
enum AnalyticsMetricKey: String, CaseIterable, Sendable, Hashable, Codable {
    /// A banner was shown to the listener.
    case bannersShown = "banners_shown"
    /// The listener answered "yes" to the banner's one-tap question.
    case bannersConfirmed = "banners_confirmed"
    /// The listener answered "no".
    case bannersDenied = "banners_denied"
    /// The listener reached for the +30s button. The north-star numerator.
    case manualSkipForwardReaches = "manual_skip_forward_reaches"
    /// Wall-clock seconds of actual playback. The north-star denominator.
    case listeningSeconds = "listening_seconds"
    /// First launch on this install.
    case retentionInstalls = "retention_installs"
    /// This install opened the app on/after day 1 since first launch.
    case retentionD1Returned = "retention_d1_returned"
    /// …on/after day 7.
    case retentionD7Returned = "retention_d7_returned"
    /// …on/after day 30.
    case retentionD30Returned = "retention_d30_returned"
}

// MARK: - Envelope (non-metric) field keys

/// Fields that describe the record itself rather than a measurement.
enum AnalyticsEnvelopeFieldKey: String, CaseIterable, Sendable, Hashable {
    /// Which envelope version's rules the sender believes it is honoring.
    case envelopeVersion = "envelope_version"
    /// Frozen schema token; lets the rollup reject records it cannot read.
    case payloadSchema = "payload_schema"
    /// The `AnalyticsCohortKey` raw value.
    case cohort = "cohort_duration_bucket"
}

// MARK: - Field value

/// The only two shapes a field may take. There is no `case string(String)`
/// — a free-form string is not representable, by construction.
enum AnalyticsFieldValue: Equatable, Sendable {
    /// A count, a duration in whole seconds, or the envelope version.
    case integer(Int)
    /// A member of the frozen token vocabulary (see `permittedTokens`).
    case token(String)
}

// MARK: - Default-deny allow-list

/// The gate every outbound field dictionary passes through.
enum TelemetryEnvelopeV1AllowList {
    /// The envelope version these records claim to honor.
    static let envelopeVersion = 1

    /// Frozen schema token. Bump alongside any allow-list change.
    static let payloadSchema = "playhead.analytics.increment.v1"

    /// Every key permitted to appear in an outbound record. Anything else
    /// rejects the record.
    static let permittedKeys: Set<String> = {
        var keys = Set(AnalyticsEnvelopeFieldKey.allCases.map(\.rawValue))
        keys.formUnion(AnalyticsMetricKey.allCases.map(\.rawValue))
        return keys
    }()

    /// Every string a field value may hold. A string outside this set — an
    /// episode title, a URL, a transcript line, a locale, a device name —
    /// rejects the record. The vocabulary is closed and tiny on purpose:
    /// it is auditable by reading it.
    static let permittedTokens: Set<String> = {
        var tokens = Set(AnalyticsCohortKey.allCases.map(\.rawValue))
        tokens.insert(payloadSchema)
        return tokens
    }()

    /// Keys whose value must be an integer, i.e. everything except the two
    /// token-valued envelope fields.
    static let integerValuedKeys: Set<String> = {
        var keys = Set(AnalyticsMetricKey.allCases.map(\.rawValue))
        keys.insert(AnalyticsEnvelopeFieldKey.envelopeVersion.rawValue)
        return keys
    }()

    /// Validates a materialized field dictionary.
    ///
    /// Returns the dictionary unchanged when every key is on the allow-list,
    /// every string value is in the vocabulary, every value's shape matches
    /// its key, no count is negative, and the two required envelope fields
    /// are present and correct. Returns `nil` otherwise — the caller must
    /// drop the record whole. There is no partial-emit path and no
    /// sanitize-and-keep path.
    static func validate(
        _ fields: [String: AnalyticsFieldValue]
    ) -> [String: AnalyticsFieldValue]? {
        for (key, value) in fields {
            guard permittedKeys.contains(key) else { return nil }
            switch value {
            case .integer(let number):
                guard integerValuedKeys.contains(key), number >= 0 else {
                    return nil
                }
            case .token(let token):
                guard !integerValuedKeys.contains(key),
                      permittedTokens.contains(token)
                else {
                    return nil
                }
            }
        }

        guard case .integer(let version)? =
                fields[AnalyticsEnvelopeFieldKey.envelopeVersion.rawValue],
              version == envelopeVersion
        else {
            return nil
        }
        guard case .token(let schema)? =
                fields[AnalyticsEnvelopeFieldKey.payloadSchema.rawValue],
              schema == payloadSchema
        else {
            return nil
        }
        guard case .token(let cohort)? =
                fields[AnalyticsEnvelopeFieldKey.cohort.rawValue],
              AnalyticsCohortKey(rawValue: cohort) != nil
        else {
            return nil
        }

        return fields
    }
}
