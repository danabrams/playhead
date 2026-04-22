// ShadowFMResponse.swift
// playhead-narl.2: Value type for one row of `shadow_fm_responses`.
//
// A shadow FM response captures what the `.allEnabled` config variant would
// have produced for a window that `.default` did not schedule FM on. The
// harness in `playhead-narl.1` replays fusion/gate/policy against these
// responses to evaluate `fmSchedulingEnabled` honestly.
//
// The `fmResponse` payload is opaque to this type — it's whatever the FM
// call path serialized at capture time. The wire shape is intentionally
// stable (see ``shadowSchemaVersion``) so the `shadow-decisions.jsonl`
// exporter and the harness's corpus builder can round-trip without a
// schema migration per run.

import Foundation

// MARK: - ShadowFMResponse

/// One row of `shadow_fm_responses`. Primary key is the composite
/// `(assetId, windowStart, windowEnd, configVariant)` — identical
/// re-captures for the same window dedupe via `INSERT OR REPLACE`.
struct ShadowFMResponse: Sendable, Equatable, Hashable {

    // MARK: - Stored fields

    /// ``AnalysisAsset.id`` the window belongs to.
    let assetId: String

    /// Window start time, in asset seconds from t=0.
    let windowStart: TimeInterval

    /// Window end time, in asset seconds from t=0. `windowEnd >= windowStart`
    /// is enforced by the persistence layer (inserts with `end < start`
    /// are rejected at bind time).
    let windowEnd: TimeInterval

    /// Which `.allEnabled` variant captured this response. Phase 1 only
    /// exercises `.allEnabledShadow`.
    let configVariant: ShadowConfigVariant

    /// Serialized FM response payload. Opaque BLOB — the wire format is owned
    /// by the dispatcher that produced it. Downstream consumers MUST version-
    /// gate at the per-row level (see ``fmModelVersion``) before decoding.
    let fmResponse: Data

    /// Wall-clock unix time (seconds) the row was written.
    let capturedAt: TimeInterval

    /// Which lane wrote the row. For diagnostics only — consumers treat
    /// laneA and laneB rows identically.
    let capturedBy: ShadowCapturedBy

    /// FM model identifier at capture time. Shadow responses captured under
    /// one FM model should not be consumed as gospel if the model changes;
    /// downstream consumers invalidate by comparing this field to the
    /// current model version. Nil only for legacy pre-versioning rows that
    /// might appear after a schema rollback/replay scenario.
    ///
    /// DESIGN NOTE (from bead spec "FM response format versioning"):
    /// We store this per-row rather than as a table-level pragma because
    /// (a) the model version can change across app launches, and
    /// (b) keeping it per-row lets the harness accept older rows by their
    /// own provenance rather than assuming a store-wide invariant.
    ///
    /// INVALIDATION CONTRACT:
    /// On app launch (or at any explicit sweep point), call
    /// `AnalysisStore.deleteShadowFMResponses(fmModelVersionOtherThan:)`
    /// with the current FM model version string. That helper removes
    /// every row whose `fmModelVersion` differs from the current value
    /// and every row with a NULL `fmModelVersion` (legacy sentinel). The
    /// coordinator's lanes then naturally re-capture the missing windows
    /// on subsequent ticks, producing a clean per-model corpus without
    /// mixing eras.
    let fmModelVersion: String?

    // MARK: - Validation

    /// Reject degenerate windows before binding. The persistence layer also
    /// rejects `windowEnd < windowStart`, NaN/infinite bounds, and negative
    /// starts, but front-loading the guard here keeps the error close to the
    /// caller.
    var isWellFormed: Bool {
        windowStart.isFinite &&
            windowEnd.isFinite &&
            windowStart >= 0 &&
            windowEnd >= windowStart
    }

    // MARK: - Canonicalization
    //
    // AC-6 review finding: the composite PK uses REAL columns for
    // `windowStart` and `windowEnd`. IEEE-754 equality is technically
    // unsafe for an upsert key — two arithmetic paths that produce the
    // "same" fractional seconds can differ in the last bit and land as
    // distinct rows. We defend against this by canonicalizing every bound
    // to integer-millisecond precision before binding. Integer values
    // below 2^53 round-trip exactly through Double, so REAL equality is
    // safe at ms resolution, and we don't need a schema migration to
    // INTEGER columns.
    //
    // Callers (the coordinator's lanes) MUST either pass already-canonical
    // values or let the store canonicalize via ``canonicalize(seconds:)``.
    // The store does this defensively at bind time; the coordinator and
    // its lookup path (``AnalysisStore/capturedShadowWindows``) apply the
    // same rounding so the upsert/lookup keys agree.

    /// Round a seconds value to the nearest integer millisecond. Used as
    /// the PK canonicalization function for shadow windows. Returns the
    /// same value for NaN/Infinity so the caller's well-formedness check
    /// still fires.
    static func canonicalize(seconds: TimeInterval) -> TimeInterval {
        guard seconds.isFinite else { return seconds }
        return (seconds * 1000.0).rounded() / 1000.0
    }
}

// MARK: - ShadowWindowKey

/// Uniqueness key for a shadow-captured window under a fixed config variant.
/// Used by ``AnalysisStore.capturedShadowWindows(assetId:configVariant:)`` to
/// let the lanes skip windows that already have a shadow response on disk.
struct ShadowWindowKey: Sendable, Hashable {
    let start: TimeInterval
    let end: TimeInterval

    /// Construct a key with both bounds canonicalized to integer milliseconds.
    /// This is the entry point callers (both lanes) should use whenever they
    /// compute a key from a ``ShadowWindow`` boundary — it guarantees the
    /// key they consult matches what `AnalysisStore.capturedShadowWindows`
    /// returns and what `upsertShadowFMResponse` wrote on the way in.
    static func canonical(start: TimeInterval, end: TimeInterval) -> ShadowWindowKey {
        ShadowWindowKey(
            start: ShadowFMResponse.canonicalize(seconds: start),
            end: ShadowFMResponse.canonicalize(seconds: end)
        )
    }
}

// MARK: - JSONL schema

/// JSONL schema version for `shadow-decisions.jsonl`. Bump when the per-line
/// shape changes in a non-backward-compatible way. The corpus builder in
/// `playhead-narl.1` version-gates per line before consuming.
///
/// SCHEMA v1 line shape:
/// ```
/// {
///   "schemaVersion": 1,
///   "type": "shadow_fm_response",
///   "assetId": "<uuid>",
///   "windowStart": <REAL seconds>,
///   "windowEnd":   <REAL seconds>,
///   "configVariant": "allEnabledShadow",
///   "fmResponseBase64": "<base64 string>",
///   "capturedAt": <unix seconds>,
///   "capturedBy": "laneA"|"laneB",
///   "fmModelVersion": "<string>" | null
/// }
/// ```
///
/// The `fmResponse` payload is base64-encoded in JSONL because the underlying
/// bytes are opaque to the exporter and may not be valid UTF-8. The harness's
/// corpus builder decodes `fmResponseBase64` → `Data` before passing it back
/// through the FM response deserializer.
let shadowSchemaVersion: Int = 1
