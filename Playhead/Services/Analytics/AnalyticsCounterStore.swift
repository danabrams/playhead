// AnalyticsCounterStore.swift
// playhead-jw63.3 — durable local totals for the playback-side and
// retention counters, plus the upload watermark.
//
// Shape: a two-level `metric → cohort → count` map of integers. Nothing
// else is stored. There is no event stream, no timestamp per increment, no
// episode reference, no device identifier. A single JSON blob under one
// `UserDefaults` key, exactly as `BannerFeedbackCounterStore` (jw63.1)
// does for the three banner counters — this store deliberately does not
// duplicate those three; the service reads them from their owner.
//
// The map is **normalized on decode**: any metric or cohort key that is not
// a live enum case is dropped when the blob is read back, and negative
// counts are clamped to zero. That makes the allow-list an at-rest property
// too — a stored blob that has been corrupted, hand-edited, or written by a
// future build with a wider vocabulary cannot smuggle a key into an
// outbound record, because the key does not survive being loaded.
//
// Isolation: `OSAllocatedUnfairLock`, not an actor and not `@MainActor`.
// One of the two manual-skip call sites is `@PlaybackServiceActor`
// (`PlaybackService.skipForward`, the lock-screen / CarPlay / AirPods
// path) and the other is `@MainActor` (`PlayheadRuntime.skipForward`, the
// in-app button); a synchronous `Sendable` store lets both increment
// without a hop, which keeps analytics off the transport's critical path.

import Foundation
import os

// MARK: - Totals

/// Cumulative counts, keyed metric → cohort. Saturating, never negative.
struct AnalyticsCounterTotals: Codable, Equatable, Sendable {
    private var counts: [String: [String: Int]]

    init() {
        counts = [:]
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let raw = (try? container.decode([String: [String: Int]].self)) ?? [:]
        counts = Self.normalized(raw)
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(counts)
    }

    /// Drops keys outside the live vocabulary and clamps negative counts.
    private static func normalized(
        _ raw: [String: [String: Int]]
    ) -> [String: [String: Int]] {
        var result: [String: [String: Int]] = [:]
        for (metric, byCohort) in raw where AnalyticsMetricKey(rawValue: metric) != nil {
            var kept: [String: Int] = [:]
            for (cohort, value) in byCohort
            where AnalyticsCohortKey(rawValue: cohort) != nil && value > 0 {
                kept[cohort] = value
            }
            if !kept.isEmpty {
                result[metric] = kept
            }
        }
        return result
    }

    func count(_ metric: AnalyticsMetricKey, cohort: AnalyticsCohortKey) -> Int {
        counts[metric.rawValue]?[cohort.rawValue] ?? 0
    }

    /// Saturating add. Non-positive deltas are ignored — these counters only
    /// ever go up, so a negative delta can only be an upstream bug and must
    /// not be able to rewind the watermark arithmetic.
    mutating func add(
        _ delta: Int,
        to metric: AnalyticsMetricKey,
        cohort: AnalyticsCohortKey
    ) {
        guard delta > 0 else { return }
        let current = count(metric, cohort: cohort)
        let updated = current > Int.max - delta ? Int.max : current + delta
        counts[metric.rawValue, default: [:]][cohort.rawValue] = updated
    }

    mutating func set(
        _ value: Int,
        for metric: AnalyticsMetricKey,
        cohort: AnalyticsCohortKey
    ) {
        guard value > 0 else {
            counts[metric.rawValue]?[cohort.rawValue] = nil
            if counts[metric.rawValue]?.isEmpty == true {
                counts[metric.rawValue] = nil
            }
            return
        }
        counts[metric.rawValue, default: [:]][cohort.rawValue] = value
    }

    /// Every (metric, cohort) pair with a non-zero count.
    var populatedPairs: [(metric: AnalyticsMetricKey, cohort: AnalyticsCohortKey, count: Int)] {
        var pairs: [(AnalyticsMetricKey, AnalyticsCohortKey, Int)] = []
        for (metricRaw, byCohort) in counts {
            guard let metric = AnalyticsMetricKey(rawValue: metricRaw) else { continue }
            for (cohortRaw, value) in byCohort {
                guard let cohort = AnalyticsCohortKey(rawValue: cohortRaw), value > 0
                else {
                    continue
                }
                pairs.append((metric, cohort, value))
            }
        }
        return pairs.sorted {
            ($0.0.rawValue, $0.1.rawValue) < ($1.0.rawValue, $1.1.rawValue)
        }
        .map { (metric: $0.0, cohort: $0.1, count: $0.2) }
    }

    var isEmpty: Bool { populatedPairs.isEmpty }

    /// The not-yet-uploaded portion: `self - watermark`, floored at zero so a
    /// watermark ahead of the totals (a restored backup, a cleared store)
    /// can never produce a negative increment.
    func delta(since watermark: AnalyticsCounterTotals) -> AnalyticsCounterTotals {
        var result = AnalyticsCounterTotals()
        for pair in populatedPairs {
            let sent = watermark.count(pair.metric, cohort: pair.cohort)
            result.add(pair.count - sent, to: pair.metric, cohort: pair.cohort)
        }
        return result
    }

    /// Union with another set of totals, taking the larger of each pair.
    /// Used to fold the banner counters (owned by
    /// `BannerFeedbackCounterStore`) into the upload view without copying
    /// them into this store's durable state.
    func merging(_ other: AnalyticsCounterTotals) -> AnalyticsCounterTotals {
        var result = self
        for pair in other.populatedPairs
        where pair.count > result.count(pair.metric, cohort: pair.cohort) {
            result.set(pair.count, for: pair.metric, cohort: pair.cohort)
        }
        return result
    }
}

// MARK: - Persistent state

/// Everything this subsystem keeps on disk.
struct AnalyticsPersistentState: Codable, Equatable, Sendable {
    /// Locally-owned cumulative counters (playback + retention).
    var totals = AnalyticsCounterTotals()
    /// Cumulative totals already accepted by the server, including the
    /// banner counters this store does not own.
    var uploaded = AnalyticsCounterTotals()
    /// Days since the reference date on which this install first launched.
    /// A local integer; it never leaves the device.
    var installDayIndex: Int?
    /// Retention metrics already counted once for this install.
    var reportedRetentionMetrics: [String] = []
    var lastAttemptAt: Date?
    var lastSuccessAt: Date?
    var consecutiveFailures = 0
    /// When the currently-pending delta first became non-empty. Used to
    /// expire data rather than retry it forever.
    var oldestUnsentAt: Date?

    private enum CodingKeys: String, CodingKey {
        case totals, uploaded, installDayIndex, reportedRetentionMetrics
        case lastAttemptAt, lastSuccessAt, consecutiveFailures, oldestUnsentAt
    }

    init() {}

    /// Tolerant decode: a blob written by an older or newer build, or a
    /// partially-corrupt one, degrades to defaults instead of throwing. This
    /// store must never be able to fail a launch.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        if let value = try? container.decodeIfPresent(
            AnalyticsCounterTotals.self, forKey: .totals
        ) {
            totals = value ?? AnalyticsCounterTotals()
        }
        if let value = try? container.decodeIfPresent(
            AnalyticsCounterTotals.self, forKey: .uploaded
        ) {
            uploaded = value ?? AnalyticsCounterTotals()
        }
        if let value = try? container.decodeIfPresent(Int.self, forKey: .installDayIndex) {
            installDayIndex = value
        }
        if let value = try? container.decodeIfPresent(
            [String].self, forKey: .reportedRetentionMetrics
        ) {
            reportedRetentionMetrics = (value ?? [])
                .filter { AnalyticsMetricKey(rawValue: $0) != nil }
        }
        if let value = try? container.decodeIfPresent(Date.self, forKey: .lastAttemptAt) {
            lastAttemptAt = value
        }
        if let value = try? container.decodeIfPresent(Date.self, forKey: .lastSuccessAt) {
            lastSuccessAt = value
        }
        if let value = try? container.decodeIfPresent(
            Int.self, forKey: .consecutiveFailures
        ) {
            consecutiveFailures = max(0, value ?? 0)
        }
        if let value = try? container.decodeIfPresent(Date.self, forKey: .oldestUnsentAt) {
            oldestUnsentAt = value
        }
    }
}

// MARK: - Store

/// Thread-safe, `UserDefaults`-backed home for `AnalyticsPersistentState`.
///
/// `@unchecked` for one reason: `UserDefaults` is not marked `Sendable`
/// although it is documented thread-safe. Everything this type owns beyond
/// it is either immutable or lives inside the lock.
final class AnalyticsCounterStore: @unchecked Sendable {
    /// Process-wide instance. Under XCTest it binds to a volatile, unique
    /// suite so a test run can never accumulate into — or read — the real
    /// user's counters, mirroring how `PlayheadRuntime` swaps in
    /// `UnavailableCloudKitProvider` under test.
    static let shared = AnalyticsCounterStore(defaults: sharedDefaults)

    private static var sharedDefaults: UserDefaults {
        guard isRunningUnderXCTest else { return .standard }
        let suite = "playhead.analytics.volatile.\(UUID().uuidString)"
        return UserDefaults(suiteName: suite) ?? .standard
    }

    private static var isRunningUnderXCTest: Bool {
        let environment = ProcessInfo.processInfo.environment
        return environment["XCTestConfigurationFilePath"] != nil
            || environment["XCTestSessionIdentifier"] != nil
            || NSClassFromString("XCTestCase") != nil
    }

    private let defaults: UserDefaults
    private let storageKey: String
    private let cache: OSAllocatedUnfairLock<AnalyticsPersistentState?>

    init(
        defaults: UserDefaults,
        storageKey: String = "playhead.analytics.aggregate.v1"
    ) {
        self.defaults = defaults
        self.storageKey = storageKey
        self.cache = OSAllocatedUnfairLock(initialState: nil)
    }

    /// Current durable state.
    var state: AnalyticsPersistentState {
        cache.withLock { cached in
            if let cached { return cached }
            let loaded = loadFromDefaults()
            cached = loaded
            return loaded
        }
    }

    /// Read-modify-write under the lock. A failed encode leaves the on-disk
    /// blob untouched and the in-memory cache authoritative — analytics
    /// losing a count is never worth propagating an error to a caller.
    func mutate(_ body: @Sendable (inout AnalyticsPersistentState) -> Void) {
        cache.withLock { cached in
            var working = cached ?? loadFromDefaults()
            body(&working)
            cached = working
            if let data = try? JSONEncoder().encode(working) {
                defaults.set(data, forKey: storageKey)
            }
        }
    }

    // MARK: Counter entry points

    /// The listener reached for the +30s button. North-star numerator.
    func recordManualSkipForwardReach(cohort: AnalyticsCohortKey) {
        mutate { state in
            state.totals.add(1, to: .manualSkipForwardReaches, cohort: cohort)
        }
    }

    /// Wall-clock seconds actually spent listening. North-star denominator.
    func addListeningSeconds(_ seconds: Int, cohort: AnalyticsCohortKey) {
        guard seconds > 0 else { return }
        mutate { state in
            state.totals.add(seconds, to: .listeningSeconds, cohort: cohort)
        }
    }

    private func loadFromDefaults() -> AnalyticsPersistentState {
        guard let data = defaults.data(forKey: storageKey),
              let decoded = try? JSONDecoder().decode(
                  AnalyticsPersistentState.self, from: data
              )
        else {
            return AnalyticsPersistentState()
        }
        return decoded
    }
}
