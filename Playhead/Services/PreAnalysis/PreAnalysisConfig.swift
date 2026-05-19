// PreAnalysisConfig.swift
// User-configurable settings for the pre-analysis pipeline.
// Persisted to UserDefaults as JSON.
//
// Also hosts the playhead-dh9b device-class profile loader
// (`loadDeviceProfiles(...)`), which reads the bundled
// `PreAnalysisConfig.json` and falls back to the hard-coded table in
// `DeviceClassProfile.fallback(for:)` when the bundle resource is
// missing or malformed.

import Foundation
import OSLog

struct PreAnalysisConfig: Codable, Sendable {
    var isEnabled: Bool = true
    var defaultT0DepthSeconds: Double = 90
    var t1DepthSeconds: Double = 300
    var t2DepthSeconds: Double = 900

    /// playhead-24cm feature flag: when `true`, the download manager
    /// splits background transfers across two new URLSession
    /// configurations (`interactive` + `maintenance`). Defaults to
    /// `false` so production keeps using the single legacy session
    /// until the flag is flipped per-beta-cohort.
    var useDualBackgroundSessions: Bool = false

    /// playhead-beh3 feature flag: when `true`, the scheduler consults
    /// the adaptive Welford+EWMA estimator (`LearnedDeviceProfile`
    /// table) instead of the Phase-1 static seed table when computing
    /// slice / grant-window values. Default ON as of 2026-05-14 (post
    /// epic-3bv landing). The flag-off path bypasses the estimator
    /// entirely (no fetch, no record); flipping OFF via Settings is
    /// the rollback path.
    var useAdaptiveDeviceProfile: Bool = true

    /// playhead-44h1: nominal shard duration (seconds) used by the Live
    /// Activity ETA formula to estimate `totalShardsEstimate =
    /// ceil(episode.durationSec / nominalShardDurationSec)`. This is an
    /// estimator input only — the actual shard boundaries are still
    /// produced by the audio decoder during analysis. Default 20 s.
    var nominalShardDurationSec: Double = 20

    /// playhead-c3pi: length (seconds) of the candidate ASR window for
    /// an unplayed episode — first 20 minutes from `episodeStart`. Bead
    /// spec §"Candidate-window selection" (Plan §3 detection cascade +
    /// §6 Phase 2 deliverable 1). Hoisted into the config so a future
    /// per-cohort experiment can move the boundary without touching
    /// `CandidateWindowSelector`.
    var unplayedCandidateWindowSeconds: TimeInterval = 20 * 60

    /// playhead-c3pi: length (seconds) of the candidate ASR window for
    /// a resumed episode — next 15 minutes from the readiness anchor
    /// (`Episode.playbackAnchor`). Matches
    /// `playbackReadinessProximalLookaheadSeconds` from playhead-cthe so
    /// the readiness derivation and the cascade scheduler agree on the
    /// proximal lookahead.
    var resumedCandidateWindowSeconds: TimeInterval = 15 * 60

    /// playhead-c3pi: minimum playhead delta (seconds) between two
    /// committed positions for the cascade to treat the move as a
    /// "seek" and re-latch the candidate-window selection on the new
    /// position. Routine ±15-s / ±30-s skip taps stay below this
    /// threshold (strict greater-than comparison) so they do not churn
    /// the scheduler queue.
    var seekRelatchThresholdSeconds: TimeInterval = 30

    /// playhead-2hpn feature flag: when `true`, the ad-detection
    /// pipeline reads/writes `ShowMusicBedProfile` rows and applies the
    /// per-show recurring-jingle boost to the `.musicBed` fusion entry.
    /// Default `false` so production keeps the byte-identical pre-2hpn
    /// behavior until the flag is flipped per-beta-cohort. The flag is
    /// scoped to BOTH the read path (profile lookup at fusion time) and
    /// the write path (profile mutation at the end of `runBackfill`);
    /// when off, neither path runs.
    var scopedMusicBedGeneralization: Bool = false

    /// playhead-zx6i feature flag: when `true`, the `AnalysisJobRunner`
    /// consults the per-asset `RevalidationStateStore` at the top of
    /// `run(_:)`. If the stored `PipelineVersions` snapshot differs
    /// from `PipelineVersions.current()` AND the asset has persisted
    /// `TranscriptChunk` rows, the runner short-circuits stages 1–3
    /// (decode, feature extraction, transcription) and delegates to
    /// `AdDetectionService.revalidateFromFeatures(...)`, which re-runs
    /// only classifier + fusion + boundary against the persisted
    /// rows. The success-stamp at the end of `runBackfill` is ALSO
    /// gated on this flag — when off, no stamp is written and the
    /// short-circuit is structurally unreachable. Default ON as of
    /// 2026-05-14 (post epic-3bv landing). Rollback latency for the
    /// zx6i flag is **instant** (next analysis run), not next-launch:
    /// `AnalysisJobRunner`'s default `b4RevalidationEnabledProvider`
    /// and `AdDetectionService.runBackfill`'s stamp-write gate BOTH
    /// re-read `PreAnalysisConfig.load()` on every call instead of
    /// snapshotting at init. This intentionally diverges from
    /// 2hpn `scopedMusicBedGeneralization` (snapshot-at-init via
    /// `preAnalysisConfig`), because the zx6i short-circuit gates a
    /// perf optimization with `false_ready_rate` risk, and minimizing
    /// blast-radius on flag-OFF matters more than caching the read.
    var b4RevalidationFromFeaturesEnabled: Bool = true

    /// playhead-h6a6 feature flag: when `true`, the ad-detection
    /// pipeline observes a `ShowCapabilityProfile` per show (after
    /// activation floor of ≥ 5 analysis-completed episodes AND
    /// Phase-2 SLIs within defended bounds per playhead-d99) and
    /// applies a per-show analysis-budget modulator. When `false`
    /// (the default), the observation path and the modulation path
    /// are both byte-identical no-ops — no profile row writes, no
    /// budget multiplier reads. The flag has the same "next
    /// `AdDetectionService` init takes effect" rollback contract as
    /// `2hpn` and `xr3t` (the service caches the config snapshot at
    /// init time). Settings → Diagnostics exposes the OBSERVED
    /// profile read-only; there is no user-facing setter for the
    /// profile kind itself.
    var showCapabilityProfilesEnabled: Bool = false

    /// playhead-rxuv feature flag: when `true`, the ad-detection
    /// fusion path activates creator-supplied chapter evidence
    /// (Podcasting 2.0 / RSS inline / ID3 CHAP) as a first-class
    /// proposal source. The wiring is twofold:
    ///   * Recall side — `ChapterMetadataEvidenceBuilder` stamps
    ///     emitted `.metadata` entries with
    ///     `EvidenceSubSource.creatorChapter` so the existing
    ///     publisher "Sponsor"/"Ad break" chapters carry a distinct
    ///     provenance tag through fusion + persistence + diagnostics.
    ///   * Precision side (primary value of the bead) — when a
    ///     candidate ad span lies inside a `.content` chapter
    ///     (interview/Q&A/discussion/etc. per `ChapterDispositionClassifier`),
    ///     the eligibility gate is demoted to `.blockedByPolicy`
    ///     so the proposal cannot auto-skip while honest fusion
    ///     scoring is preserved.
    ///
    /// When `false` (the default), both paths are byte-identical
    /// no-ops — entries are emitted without `creatorChapter` sub-source
    /// (matching pre-rxuv output), and no content-chapter suppression
    /// runs. Inferred (FM-labeled) chapters are out of scope for this
    /// bead; the follow-on `playhead-w7oi` bead will wire those.
    ///
    /// Rollback latency: same "next `AdDetectionService` init takes
    /// effect" contract as `2hpn` / `h6a6` — the service caches the
    /// config snapshot at init time, so flipping via Settings persists
    /// to `UserDefaults` immediately but the running detector keeps
    /// its current state until the next backfill run that constructs
    /// a fresh `AdDetectionService`.
    var creatorChapterFusionEnabled: Bool = false

    static let analysisVersion: Int = 1

    private static let key = "PreAnalysisConfig"

    init(
        isEnabled: Bool = true,
        defaultT0DepthSeconds: Double = 90,
        t1DepthSeconds: Double = 300,
        t2DepthSeconds: Double = 900,
        useDualBackgroundSessions: Bool = false,
        nominalShardDurationSec: Double = 20,
        unplayedCandidateWindowSeconds: TimeInterval = 20 * 60,
        resumedCandidateWindowSeconds: TimeInterval = 15 * 60,
        seekRelatchThresholdSeconds: TimeInterval = 30,
        scopedMusicBedGeneralization: Bool = false,
        useAdaptiveDeviceProfile: Bool = true,
        b4RevalidationFromFeaturesEnabled: Bool = true,
        showCapabilityProfilesEnabled: Bool = false,
        creatorChapterFusionEnabled: Bool = false
    ) {
        self.isEnabled = isEnabled
        self.defaultT0DepthSeconds = defaultT0DepthSeconds
        self.t1DepthSeconds = t1DepthSeconds
        self.t2DepthSeconds = t2DepthSeconds
        self.useDualBackgroundSessions = useDualBackgroundSessions
        self.nominalShardDurationSec = nominalShardDurationSec
        self.unplayedCandidateWindowSeconds = unplayedCandidateWindowSeconds
        self.resumedCandidateWindowSeconds = resumedCandidateWindowSeconds
        self.seekRelatchThresholdSeconds = seekRelatchThresholdSeconds
        self.scopedMusicBedGeneralization = scopedMusicBedGeneralization
        self.useAdaptiveDeviceProfile = useAdaptiveDeviceProfile
        self.b4RevalidationFromFeaturesEnabled = b4RevalidationFromFeaturesEnabled
        self.showCapabilityProfilesEnabled = showCapabilityProfilesEnabled
        self.creatorChapterFusionEnabled = creatorChapterFusionEnabled
    }

    // Custom decoder so configs persisted before 24cm (which lack the
    // `useDualBackgroundSessions` key) still decode — absent keys fall
    // back to `false`, matching the new default. The 44h1
    // `nominalShardDurationSec` follows the same pattern: if the stored
    // config predates this bead, fall back to the 20 s default.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.isEnabled = try container.decodeIfPresent(Bool.self, forKey: .isEnabled) ?? true
        self.defaultT0DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .defaultT0DepthSeconds) ?? 90
        self.t1DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .t1DepthSeconds) ?? 300
        self.t2DepthSeconds = try container.decodeIfPresent(Double.self, forKey: .t2DepthSeconds) ?? 900
        self.useDualBackgroundSessions = try container.decodeIfPresent(Bool.self, forKey: .useDualBackgroundSessions) ?? false
        self.nominalShardDurationSec = try container.decodeIfPresent(Double.self, forKey: .nominalShardDurationSec) ?? 20
        // playhead-c3pi: configs persisted before this bead omit these
        // keys; default to the bead-spec values so old blobs decode.
        self.unplayedCandidateWindowSeconds = try container.decodeIfPresent(TimeInterval.self, forKey: .unplayedCandidateWindowSeconds) ?? (20 * 60)
        self.resumedCandidateWindowSeconds = try container.decodeIfPresent(TimeInterval.self, forKey: .resumedCandidateWindowSeconds) ?? (15 * 60)
        self.seekRelatchThresholdSeconds = try container.decodeIfPresent(TimeInterval.self, forKey: .seekRelatchThresholdSeconds) ?? 30
        // playhead-2hpn: configs persisted before this bead omit the
        // key; default to `false` so the scoped-music-bed feature stays
        // OFF on upgrade (rollback-friendly default).
        self.scopedMusicBedGeneralization = try container.decodeIfPresent(Bool.self, forKey: .scopedMusicBedGeneralization) ?? false
        // playhead-beh3: configs persisted before this bead omit the
        // adaptive-estimator flag. As of 2026-05-14, the production
        // default is ON, so absent keys decode to `true` — existing
        // users upgrade onto the adaptive estimator silently. Opt-OUT
        // is via the Settings toggle.
        self.useAdaptiveDeviceProfile = try container.decodeIfPresent(Bool.self, forKey: .useAdaptiveDeviceProfile) ?? true
        // playhead-zx6i: configs persisted before this bead omit the
        // key. As of 2026-05-14, the production default is ON, so
        // absent keys decode to `true` — existing users get B4
        // revalidation on next analysis run. Rollback is instant via
        // the Settings toggle (no relaunch needed) per the live-read
        // contract on `PreAnalysisConfig.load()`.
        self.b4RevalidationFromFeaturesEnabled = try container.decodeIfPresent(Bool.self, forKey: .b4RevalidationFromFeaturesEnabled) ?? true
        // playhead-h6a6: configs persisted before this bead omit the
        // capability-profiles flag; default to `false` so the
        // observation + modulation paths stay OFF on upgrade
        // (rollback-friendly default; identical to 2hpn rationale).
        self.showCapabilityProfilesEnabled = try container.decodeIfPresent(Bool.self, forKey: .showCapabilityProfilesEnabled) ?? false
        // playhead-rxuv: configs persisted before this bead omit the
        // creator-chapter-fusion flag; default to `false` so the
        // provenance-tagging + content-chapter-suppression paths stay
        // OFF on upgrade (rollback-friendly default; identical to 2hpn /
        // h6a6 rationale).
        self.creatorChapterFusionEnabled = try container.decodeIfPresent(Bool.self, forKey: .creatorChapterFusionEnabled) ?? false
    }

    private enum CodingKeys: String, CodingKey {
        case isEnabled
        case defaultT0DepthSeconds
        case t1DepthSeconds
        case t2DepthSeconds
        case useDualBackgroundSessions
        case nominalShardDurationSec
        case unplayedCandidateWindowSeconds
        case resumedCandidateWindowSeconds
        case seekRelatchThresholdSeconds
        case scopedMusicBedGeneralization
        case useAdaptiveDeviceProfile
        case b4RevalidationFromFeaturesEnabled
        case showCapabilityProfilesEnabled
        case creatorChapterFusionEnabled
    }

    static func load() -> PreAnalysisConfig {
        guard let data = UserDefaults.standard.data(forKey: key),
              var config = try? JSONDecoder().decode(PreAnalysisConfig.self, from: data)
        else { return PreAnalysisConfig() }
        // Enforce ascending tier depths; reset to defaults if misconfigured.
        let defaults = PreAnalysisConfig()
        if !(config.defaultT0DepthSeconds < config.t1DepthSeconds
             && config.t1DepthSeconds < config.t2DepthSeconds) {
            config.defaultT0DepthSeconds = defaults.defaultT0DepthSeconds
            config.t1DepthSeconds = defaults.t1DepthSeconds
            config.t2DepthSeconds = defaults.t2DepthSeconds
        }
        return config
    }

    func save() {
        if let data = try? JSONEncoder().encode(self) {
            UserDefaults.standard.set(data, forKey: Self.key)
        }
    }

    // MARK: - playhead-dh9b: Device-Class Profile Loader

    /// The manifest schema version this binary understands. Must match
    /// the `version` field in `PreAnalysisConfig.json`. A mismatch is
    /// treated as a malformed bundle (fallback table used, loud log).
    static let deviceProfilesManifestVersion: Int = 1

    /// Bundle resource name for the device-class manifest (no extension).
    static let deviceProfilesResourceName: String = "PreAnalysisConfig"

    private static let deviceProfilesLogger = Logger(
        subsystem: "com.playhead",
        category: "PreAnalysisConfig"
    )

    /// Outcome of a `loadDeviceProfiles(...)` call, exposed for
    /// observability hooks and tests.
    enum DeviceProfilesLoadResult: Sendable, Equatable {
        /// Bundle JSON decoded cleanly and covered every DeviceClass
        /// case. No fallback values were used.
        case bundleJSON
        /// Bundle resource was missing. The hard-coded fallback table
        /// is returned; the caller should log a loud observability
        /// event (production expects the file to be present).
        case fallbackMissingResource
        /// Bundle resource was present but failed to decode, or the
        /// manifest version did not match `deviceProfilesManifestVersion`.
        case fallbackMalformedJSON(reason: String)
    }

    /// Loads the per-device-class profile table.
    ///
    /// Resolution order:
    ///   1. Bundled `PreAnalysisConfig.json` in `bundle` (default `.main`).
    ///   2. Hard-coded `DeviceClassProfile.fallback(for:)` for every
    ///      `DeviceClass` case.
    ///
    /// The return value always covers every `DeviceClass` case — even
    /// if the bundled JSON is incomplete, missing entries are patched
    /// in from the fallback table. This guarantees `result[someClass]`
    /// never returns nil at runtime.
    ///
    /// - Parameter bundle: Bundle to search. Tests pass a stripped
    ///   bundle (or `.init()`) to exercise the fallback path.
    /// - Returns: Tuple of (table, outcome). Callers that care about
    ///   which branch was taken (observability, tests) inspect
    ///   `outcome`; most callers just use `table`.
    static func loadDeviceProfiles(
        bundle: Bundle = .main
    ) -> (table: [DeviceClass: DeviceClassProfile], outcome: DeviceProfilesLoadResult) {
        guard let url = bundle.url(
            forResource: deviceProfilesResourceName,
            withExtension: "json"
        ) else {
            deviceProfilesLogger.error(
                "PreAnalysisConfig.json missing from bundle; using hard-coded fallback table"
            )
            return (DeviceClassProfile.fallbackTable(), .fallbackMissingResource)
        }

        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            let reason = "read failed: \(error.localizedDescription)"
            deviceProfilesLogger.error(
                "PreAnalysisConfig.json \(reason, privacy: .public); using fallback"
            )
            return (DeviceClassProfile.fallbackTable(), .fallbackMalformedJSON(reason: reason))
        }

        let manifest: DeviceClassProfilesManifest
        do {
            manifest = try JSONDecoder().decode(
                DeviceClassProfilesManifest.self,
                from: data
            )
        } catch {
            let reason = "decode failed: \(error.localizedDescription)"
            deviceProfilesLogger.error(
                "PreAnalysisConfig.json \(reason, privacy: .public); using fallback"
            )
            return (DeviceClassProfile.fallbackTable(), .fallbackMalformedJSON(reason: reason))
        }

        guard manifest.version == deviceProfilesManifestVersion else {
            let reason = "version mismatch (got \(manifest.version), expected \(deviceProfilesManifestVersion))"
            deviceProfilesLogger.error(
                "PreAnalysisConfig.json \(reason, privacy: .public); using fallback"
            )
            return (DeviceClassProfile.fallbackTable(), .fallbackMalformedJSON(reason: reason))
        }

        // Start from fallback so any DeviceClass not mentioned in the
        // JSON is still covered. Then overlay the JSON rows.
        var table = DeviceClassProfile.fallbackTable()
        for profile in manifest.profiles {
            guard let bucket = DeviceClass(rawValue: profile.deviceClass) else {
                // Unknown bucket in JSON (e.g., a row for a future
                // DeviceClass case this binary doesn't know about).
                // Ignored — the fallback row stays in place.
                deviceProfilesLogger.notice(
                    "PreAnalysisConfig.json contains unknown deviceClass=\(profile.deviceClass, privacy: .public); ignoring row"
                )
                continue
            }
            table[bucket] = profile
        }

        return (table, .bundleJSON)
    }
}
