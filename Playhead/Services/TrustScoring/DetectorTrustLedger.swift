// DetectorTrustLedger.swift
// playhead-gard: per-(show, detector) trust state, persisted as JSON on the
// `podcast_profiles` row.
//
// The legacy columns (`skipTrustScore`, `mode`, `recentFalseSkipSignals`) are
// UNCHANGED and still written exactly as before. This ledger is an ADDITIVE
// column that a binary which does not know about it never selects; such a
// binary reads the legacy scalar and behaves precisely as it does today. That
// is the playhead-6qvf shape applied to persistence — a new value whose ABSENCE
// means "fall back to the conservative established answer", so an older reader
// degrades rather than misreads.

import Foundation

// MARK: - DetectorTrustEntry

/// One detector class's trust state on one show.
///
/// Mirrors the legacy triple field-for-field with ONE deliberate difference:
/// the false-skip counter is a `Double`, not an `Int`. See
/// `DetectorVetoWeight` for why.
struct DetectorTrustEntry: Codable, Sendable, Equatable {
    /// `[0, 1]`, same scale and same meaning as `PodcastProfile.skipTrustScore`.
    var trustScore: Double
    /// A `SkipMode` raw value. Kept as a `String` for the same reason
    /// `PodcastProfile.mode` is: an unrecognised value must decode to a row,
    /// not to a decode failure that discards the whole ledger.
    var mode: String
    /// WEIGHTED recent false-skip signals. Crosses the same integer thresholds
    /// (`autoToManualFalseSignals`, `manualToShadowFalseSignals`) the legacy
    /// counter does.
    var falseSkipWeight: Double
    /// Correct observations recorded for this class on this show. Gates
    /// promotion exactly as `PodcastProfile.observationCount` does.
    var observationCount: Int

    var skipMode: SkipMode { SkipMode(rawValue: mode) ?? .shadow }
}

// MARK: - DetectorVetoWeight

/// How much ONE veto counts against the detector that produced the vetoed span.
///
/// The bead's second question: a veto of an UNANCHORED 0.40-confidence window
/// carried the same `falseSignalPenalty` as a veto of an anchored
/// high-confidence one. Both moved the counter by exactly 1, and the counter —
/// not the trust score — is what demotes. So three vetoes of the weakest thing
/// the pipeline can emit demoted the show past `autoToManualFalseSignals` (2)
/// on the nose.
///
/// The weight is keyed on `ExtentAnchorTier`, the tier system that already
/// exists and that playhead-6qvf sharpened hours before this bead — NOT on a
/// parallel scale invented here. `SpanExtentSupport.tier` is a span's WEAKER
/// edge, is derived from two columns persisted on every `ad_windows` row, and
/// post-6qvf `.deterministic` means the byte differ and nothing else.
///
/// The ORDERING is the claim; the magnitudes are the minimal choice that makes
/// the ordering bite at the thresholds already in `TrustScoringConfig`:
///
/// | tier | weight | vetoes to demote from `auto` |
/// | --- | --- | --- |
/// | `.none` — the pipeline invented both edges | 0.5 | 4 |
/// | `.corroborated` — a stinger snap agreed | 1.0 | 2 (unchanged) |
/// | `.deterministic` — the byte differ proved it | 1.5 | 2, and faster |
///
/// Read the top and bottom rows together, because they are the point. A span
/// whose edges nobody observed is weak evidence that the detector is broken —
/// the user may be rejecting geometry rather than the ad claim. A span the byte
/// differ PROVED and the user rejected anyway is the strongest evidence
/// available that this instrument is wrong ON THIS SHOW, and it should demote
/// faster than average, not slower. Certainty cuts both ways; a weighting that
/// only ever softened penalties would be a licence, not a measurement.
///
/// Applied to the field case: Dan's three vetoes were `.none` — 1.5 weighted,
/// under the threshold of 2. **The show stays in `auto`.**
enum DetectorVetoWeight {
    static func weight(for tier: ExtentAnchorTier) -> Double {
        switch tier {
        case .none: return 0.5
        case .corroborated: return 1.0
        case .deterministic: return 1.5
        }
    }
}

// MARK: - DetectorTrustLedger

/// Every detector class's state on one show, as one persisted value.
///
/// Storage is `[String: DetectorTrustEntry]` keyed by
/// `SkipDetectorClass.rawValue` rather than a fixed struct with four fields,
/// for one reason: **a key this binary does not recognise survives a
/// round-trip.** A future class added by a newer build is decoded into the
/// dictionary, ignored by policy here, and re-encoded intact — so a user who
/// downgrades and re-upgrades does not silently lose that class's history.
struct DetectorTrustLedger: Codable, Sendable, Equatable {
    private(set) var entries: [String: DetectorTrustEntry]

    init(entries: [String: DetectorTrustEntry] = [:]) {
        self.entries = entries
    }

    // MARK: Codable — the whole value IS the dictionary

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        entries = try container.decode([String: DetectorTrustEntry].self)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(entries)
    }

    // MARK: Persistence

    /// Decode a persisted ledger. A `nil`, empty, or CORRUPT column yields an
    /// EMPTY ledger, not a failure: every class then falls back to its seed,
    /// which for the three show-governed classes is the legacy scalar. Losing
    /// the ledger therefore costs history, never posture — the same failure
    /// direction `PodcastProfile.traitProfile` takes.
    static func decode(_ json: String?) -> DetectorTrustLedger {
        guard let json, !json.isEmpty,
              let data = json.data(using: .utf8),
              let decoded = try? JSONDecoder().decode(
                  DetectorTrustLedger.self, from: data
              )
        else { return DetectorTrustLedger() }
        return decoded
    }

    /// JSON for the `detectorTrustJSON` column. `nil` for an empty ledger so a
    /// show that has never diverged from the legacy scalar keeps writing NULL
    /// and stays byte-identical to a pre-gard row.
    func encoded() -> String? {
        guard !entries.isEmpty else { return nil }
        guard let data = try? JSONEncoder().encode(self) else { return nil }
        return String(data: data, encoding: .utf8)
    }

    // MARK: Reads

    /// This class's state on this show: the stored entry, or the seed.
    ///
    /// The seed is a PURE function of the profile's legacy columns, so reading
    /// is idempotent — a class with no stored entry reads the same value every
    /// time until something writes one.
    func entry(
        for detector: SkipDetectorClass,
        seededFrom profile: PodcastProfile
    ) -> DetectorTrustEntry {
        entries[detector.rawValue]
            ?? Self.seed(for: detector, from: profile)
    }

    /// What a class's state is on a show that has never recorded one.
    ///
    /// **The migration story lives here.** For the three show-governed classes
    /// the seed is the legacy triple verbatim, so an existing row keeps exactly
    /// the posture it had: same mode, same trust, same signal count. Nothing is
    /// gained and nothing is lost.
    ///
    /// `.rediffByteExact` is the one exception and it is the bead: it seeds at
    /// `SkipDetectorClass.showIndependentSeedMode` with a clean history,
    /// because the legacy scalar is a record of other detectors' mistakes and
    /// carrying it over is the defect. Its trust starts at the same 0.5 a
    /// brand-new profile gets — neither credit nor blame inherited.
    static func seed(
        for detector: SkipDetectorClass,
        from profile: PodcastProfile
    ) -> DetectorTrustEntry {
        guard detector.consultsShowTrust else {
            return DetectorTrustEntry(
                trustScore: 0.5,
                mode: SkipDetectorClass.showIndependentSeedMode.rawValue,
                falseSkipWeight: 0,
                observationCount: 0
            )
        }
        return DetectorTrustEntry(
            trustScore: profile.skipTrustScore,
            mode: profile.mode,
            falseSkipWeight: Double(profile.recentFalseSkipSignals),
            observationCount: profile.observationCount
        )
    }

    // MARK: Restoration (playhead-u0vv)

    /// The mode a class holds once a decay has DISCHARGED the last of the veto
    /// evidence against it.
    ///
    /// Dan's ruling, recorded on playhead-mn5e: *"Rediff is incredibly high
    /// confidence, it should always be auto. But other evidence isn't as
    /// strong."* `showIndependentSeedMode` implements the first half, but as a
    /// SEED it stopped applying at the listener's first gesture of any kind —
    /// an attributed veto materializes every class's entry, and from then on
    /// the stored entry wins over the seed forever. Two vetoes then demoted the
    /// class to `manual` and playhead-lqcp had removed the only route back, so
    /// the exemption survived exactly until it was first tested.
    ///
    /// **This is a RESTORATION, and the three guards below are what keep it
    /// from being a promotion.** Read them in order; each one is somewhere a
    /// promotion would have said yes.
    ///
    ///  1. **The discharge must be a DECAY, not a state.** `weightBefore > 0`
    ///     — a weight that was already zero has not *decayed* to zero, and the
    ///     difference is load-bearing rather than pedantic:
    ///     `setUserOverride` writes every entry at weight 0 with an explicitly
    ///     chosen mode, so a state-based rule would let the next banner Yes
    ///     silently overwrite a live user instruction with the seed. It also
    ///     means the only gesture that can restore anything is a USER's
    ///     (`applyCorrectObservation` is the sole decayer — self-observation
    ///     carries the weight through untouched, deliberately), so a detector
    ///     can never restore itself on its own output.
    ///  2. **Somebody other than this show must have set the mode.**
    ///     `authority` is `SkipDetectorClass.modeAuthority`, `nil` for every
    ///     class whose eligibility the show's history genuinely governs. A
    ///     show-governed class returns `currentMode` unchanged.
    ///  3. **The mode is the AUTHORITY's, never this function's.** It is read
    ///     off `DetectorModeAuthority.declaredMode`; nothing here can name a
    ///     mode of its own, so a class can only ever return to what some other
    ///     authority already granted it.
    ///
    /// What this deliberately does NOT do is touch `trustScore`,
    /// `observationCount` or `falseSkipWeight` — it returns a MODE. The caller
    /// keeps the quantities the class actually earned, so a restored class
    /// carries its real record and a subsequent veto demotes it again on the
    /// ordinary `evaluateDemotion` arithmetic. Restoration is a return trip,
    /// not immunity.
    static func restoredMode(
        under authority: DetectorModeAuthority?,
        currentMode: SkipMode,
        weightBefore: Double,
        weightAfter: Double
    ) -> SkipMode {
        guard weightBefore > 0, weightAfter <= 0 else { return currentMode }
        guard let authority else { return currentMode }
        return authority.declaredMode
    }

    // MARK: Writes

    /// Replace one class's entry, leaving every other key — including keys this
    /// binary does not recognise — untouched.
    mutating func set(_ entry: DetectorTrustEntry, for detector: SkipDetectorClass) {
        entries[detector.rawValue] = entry
    }
}
