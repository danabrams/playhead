// AnalyticsService.swift
// playhead-jw63.3 — the coordinator: five counters in, at most one tiny
// anonymous write a day out.
//
// North star: **manual +30s reaches per listening hour.** The thing
// Playhead replaces is the 30-second skip button, so the number that says
// whether it is working is how often a listener still reaches for it per
// hour of listening. Everything else here exists to make that number
// trustworthy and cheap.
//
// Scheduling, and why it is this boring:
//   * **No BGTask.** Five are already registered (analysis backfill ×2,
//     continued analysis, pre-analysis recovery, feed refresh) and they are
//     starved for windows as it is — playhead-i6oi / txq3 exist because
//     iOS grants roughly none of them overnight. Analytics taking a share
//     of that budget would trade the product's core promise for a counter.
//   * **No timer, no push, no wake.** The only trigger is the app already
//     being foreground-active, which by definition means the user woke the
//     device for their own reasons.
//   * **At most one attempt per 24 h**, at `Task(priority: .background)`,
//     sending at most five ~10-field records. Analysis work runs at higher
//     QoS and wins every scheduling contest by construction.
//
// Failure policy — analytics is the least important subsystem in the app:
//   * Offline or any CloudKit error: swallow it, do not advance the
//     watermark, count the failure, and back off (24 h → 48 h → … → 7 days).
//     Counters are cumulative, so an unsent delta simply rides along with
//     the next successful upload; nothing is queued and nothing retries in
//     a loop.
//   * A delta that has failed to land for 30 days is **dropped** by
//     advancing the watermark over it. Retrying forever to protect a
//     rounding error is how a background subsystem becomes a battery bug.
//   * Nothing in this file throws to a caller, blocks the main thread on
//     I/O, or participates in playback or analysis. The recording entry
//     points are synchronous lock-guarded integer increments.

import Foundation
import OSLog

@MainActor
final class AnalyticsService {
    static let shared = AnalyticsService.makeProduction()

    /// Minimum gap between upload attempts when the last one succeeded.
    static let minimumUploadInterval: TimeInterval = 24 * 60 * 60
    /// Ceiling on the failure backoff.
    static let maximumUploadInterval: TimeInterval = 7 * 24 * 60 * 60
    /// A pending delta older than this is dropped rather than retried.
    static let pendingDeltaExpiry: TimeInterval = 30 * 24 * 60 * 60

    /// Why an upload attempt did or did not happen. Returned so the policy
    /// is testable without a network, a clock, or a CloudKit container.
    enum UploadDecision: Equatable {
        case notPermitted
        case nothingPending
        case tooSoon
        case alreadyRunning
        case attempted
    }

    private static let logger = Logger(subsystem: "com.playhead", category: "Analytics")

    private let store: AnalyticsCounterStore
    private let writer: any AnalyticsRecordWriting
    private let isUploadPermitted: @Sendable () -> Bool
    private let bannerCounts: @MainActor () -> BannerFeedbackCounts
    private let now: @Sendable () -> Date
    private let calendar: Calendar

    private var accumulator = ListeningTimeAccumulator()
    private var quantizer = ListeningSecondsQuantizer()
    private var currentCohort: AnalyticsCohortKey = .all
    private var isUploading = false

    init(
        store: AnalyticsCounterStore = .shared,
        writer: any AnalyticsRecordWriting = DisabledAnalyticsRecordWriter(),
        isUploadPermitted: @escaping @Sendable () -> Bool = {
            AnalyticsUploadGate.isAutomaticUploadPermitted
        },
        bannerCounts: @escaping @MainActor () -> BannerFeedbackCounts = {
            BannerFeedbackCounterStore.shared.snapshot
        },
        now: @escaping @Sendable () -> Date = { Date() },
        calendar: Calendar = .current
    ) {
        self.store = store
        self.writer = writer
        self.isUploadPermitted = isUploadPermitted
        self.bannerCounts = bannerCounts
        self.now = now
        self.calendar = calendar
    }

    /// Production instance: the same store, wired to the real transport.
    /// The transport still refuses to send until the legal gate opens.
    static func makeProduction() -> AnalyticsService {
        AnalyticsService(
            store: .shared,
            writer: AnalyticsUploadGate.isAutomaticUploadPermitted
                ? CloudKitPublicAnalyticsWriter()
                : DisabledAnalyticsRecordWriter()
        )
    }

    // MARK: - Recording

    // The +30s counter is not recorded here. Its two call sites sit in
    // different isolation domains (`@MainActor` for the in-app button,
    // `@PlaybackServiceActor` for the lock-screen / CarPlay / AirPods
    // command), so both go through `AnalyticsRecorder`, which is
    // non-isolated and lands in the same store method. One counting path,
    // two adapters — not two counting paths.

    /// Feeds the playback status stream. Credits elapsed listening time on
    /// every transition out of `.playing`.
    func playbackStatusChanged(isPlaying: Bool, durationSeconds: TimeInterval) {
        let credited = accumulator.observe(isPlaying: isPlaying, at: ContinuousClock.now)
        commit(credited)
        if isPlaying {
            currentCohort = AnalyticsCohortResolver.cohort(
                forDurationSeconds: durationSeconds
            )
        }
    }

    /// Commits listening time accrued so far without ending the interval.
    /// Called when the app backgrounds: audio keeps playing, but the app may
    /// be killed without another transition, and unwritten seconds are lost
    /// seconds.
    func commitListeningInterval() {
        commit(accumulator.commit(at: ContinuousClock.now))
    }

    private func commit(_ seconds: TimeInterval) {
        let whole = quantizer.take(seconds)
        guard whole > 0 else { return }
        store.addListeningSeconds(whole, cohort: currentCohort)
    }

    // MARK: - Lifecycle

    /// Foreground entry point: update the local return-bucket ladder, then
    /// consider an upload. Never blocks; the upload runs detached at
    /// background priority.
    func applicationDidBecomeActive() {
        let today = RetentionBucketTracker.dayIndex(for: now(), calendar: calendar)
        store.mutate { state in
            RetentionBucketTracker.apply(to: &state, today: today)
        }
        scheduleUploadIfDue()
    }

    /// Background entry point: flush listening time. No network.
    func applicationDidEnterBackground() {
        commitListeningInterval()
    }

    // MARK: - Upload policy

    /// Decides whether an attempt is due and, if so, starts it.
    @discardableResult
    func scheduleUploadIfDue() -> UploadDecision {
        let decision = uploadDecision(at: now())
        guard decision == .attempted else { return decision }
        isUploading = true
        Task(priority: .background) { [weak self] in
            _ = await self?.performUpload()
        }
        return decision
    }

    /// Pure policy: would an attempt run right now?
    func uploadDecision(at instant: Date) -> UploadDecision {
        guard isUploadPermitted() else { return .notPermitted }
        guard !isUploading else { return .alreadyRunning }
        let state = store.state
        guard !pendingDelta(from: state).isEmpty else { return .nothingPending }
        guard let last = state.lastAttemptAt else { return .attempted }
        let wait = backoffInterval(consecutiveFailures: state.consecutiveFailures)
        return instant.timeIntervalSince(last) >= wait ? .attempted : .tooSoon
    }

    /// 24 h after a success; doubling after each failure, capped at 7 days.
    func backoffInterval(consecutiveFailures: Int) -> TimeInterval {
        guard consecutiveFailures > 0 else { return Self.minimumUploadInterval }
        let exponent = min(consecutiveFailures, 8)
        let scaled = Self.minimumUploadInterval * pow(2, Double(exponent))
        return min(scaled, Self.maximumUploadInterval)
    }

    /// Runs one attempt. Always returns; never throws; never rethrows a
    /// CloudKit error into a caller.
    @discardableResult
    func performUpload() async -> Bool {
        defer { isUploading = false }

        let instant = now()
        let totals = currentTotals()
        let delta = totals.delta(since: store.state.uploaded)
        guard !delta.isEmpty else {
            store.mutate { $0.oldestUnsentAt = nil }
            return true
        }

        let records = AnalyticsIncrementPayload.records(for: delta)
        guard !records.isEmpty else {
            // Every record failed envelope validation. Do not advance the
            // watermark — but do not retry in a tight loop either; the
            // normal backoff applies via `lastAttemptAt`.
            Self.logger.error("Analytics delta produced no envelope-valid records")
            store.mutate { state in
                state.lastAttemptAt = instant
                state.consecutiveFailures += 1
            }
            return false
        }

        do {
            try await writer.write(records)
            store.mutate { state in
                state.uploaded = state.uploaded.merging(totals)
                state.lastAttemptAt = instant
                state.lastSuccessAt = instant
                state.consecutiveFailures = 0
                state.oldestUnsentAt = nil
            }
            return true
        } catch {
            let expiry = Self.pendingDeltaExpiry
            store.mutate { state in
                state.lastAttemptAt = instant
                state.consecutiveFailures += 1
                let pendingSince = state.oldestUnsentAt ?? instant
                if instant.timeIntervalSince(pendingSince) >= expiry {
                    // Old enough that nobody will miss it. Drop rather than
                    // carry it forever.
                    state.uploaded = state.uploaded.merging(totals)
                    state.oldestUnsentAt = nil
                    state.consecutiveFailures = 0
                } else {
                    state.oldestUnsentAt = pendingSince
                }
            }
            return false
        }
    }

    /// Cumulative totals across both counter owners: this store (playback +
    /// retention) and `BannerFeedbackCounterStore` (jw63.1's three banner
    /// counters, which stay where they are — analytics reads them, it does
    /// not re-count them).
    func currentTotals() -> AnalyticsCounterTotals {
        var banners = AnalyticsCounterTotals()
        let counts = bannerCounts()
        banners.set(counts.bannersShown, for: .bannersShown, cohort: .all)
        banners.set(counts.bannersConfirmed, for: .bannersConfirmed, cohort: .all)
        banners.set(counts.bannersDenied, for: .bannersDenied, cohort: .all)
        return store.state.totals.merging(banners)
    }

    private func pendingDelta(from state: AnalyticsPersistentState) -> AnalyticsCounterTotals {
        currentTotals().delta(since: state.uploaded)
    }
}
