// DiagnosticsExportCoordinator.swift
// @MainActor orchestrator for the support-safe diagnostics bundle export
// flow: fetch → build → encode → present → apply opt-in reset policy.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Design shape:
//   * The coordinator is the ONLY place where the pure builder
//     (`DiagnosticsBundleBuilder`), the pure reset policy
//     (`DiagnosticsOptInResetPolicy`), and the UI-facing presenter
//     (`DiagnosticsExportPresenter`) meet. Everything touching UIKit,
//     SwiftData, or AnalysisStore is injected through a seam protocol so
//     this file can be unit-tested without a simulator.
//   * `exportAndPresent(from:)` runs the full flow; `buildAndEncode()`
//     is surfaced separately so tests can assert the encoded JSON shape
//     without driving the presenter stub.
//   * Opt-in reset is applied AFTER the presenter completes and only on
//     `.sent` / `.saved` per `DiagnosticsOptInResetPolicy`. Applying the
//     reset before presentation would leak state through a cancel path
//     and defeat the opt-in UX.
//
// Legal checklist alignment (per bead spec):
//   (a) builder enforces hashing for default-bundle episode references.
//   (b) builder enforces transcript excerpt ±30s / 1000-char truncation.
//   (c) hashing delegated to EpisodeIdHasher (salted by installID).
//   (d) feature_summaries type enforces coarse aggregates only.
// This file does not re-derive those invariants — it only wires them up.

import Foundation

#if canImport(UIKit)
import UIKit
#endif

@MainActor
final class DiagnosticsExportCoordinator {

    // MARK: - Dependencies

    private let environment: DiagnosticsExportEnvironment
    private let presenter: DiagnosticsExportPresenter
    private let journalFetch: DiagnosticsJournalFetch
    private let chapterPhaseEventsFetch: DiagnosticsChapterPhaseEventsFetch
    private let musicBedProfilesFetch: DiagnosticsMusicBedProfilesFetch
    private let learnedDeviceProfilesFetch: DiagnosticsLearnedDeviceProfilesFetch
    private let stabilityFetch: DiagnosticsStabilityFetch
    private let bannerTalliesFetch: DiagnosticsBannerTalliesFetch
    private let rediffFetch: DiagnosticsRediffFetch
    private let analysisStoreHealthFetch: DiagnosticsAnalysisStoreHealthFetch
    private let speechModelLoadFetch: DiagnosticsSpeechModelLoadFetch
    private let optInSink: DiagnosticsOptInSink
    private let optInEpisodes: [DiagnosticsEpisodeInput]

    // MARK: - Init

    /// - Parameters:
    ///   - environment: static build/eligibility/install inputs.
    ///   - presenter: UI adapter (UIKit composer or test fake).
    ///   - journalFetch: async fetch of the most-recent WorkJournal rows.
    ///   - chapterPhaseEventsFetch: async fetch of the chapter-phase
    ///     diagnostics events to embed in the bundle (playhead-au2v.1.3).
    ///     Defaults to "no events"; the live emit call sites land in
    ///     `playhead-au2v.1.10` and onwards, at which point the closure
    ///     will source rows from a persistence-backed store.
    ///   - musicBedProfilesFetch: playhead-2hpn — async fetch of every
    ///     show's recurring-jingle profile snapshot. Defaults to "no
    ///     profiles"; the live wiring sources rows from
    ///     `ShowMusicBedProfileStore`. Empty array yields an empty
    ///     `music_bed_profiles` field in the bundle (which still gets
    ///     emitted so the support engineer's grep cheat sheet can rely
    ///     on key presence).
    ///   - learnedDeviceProfilesFetch: playhead-beh3 — async fetch of
    ///     the adaptive Welford+EWMA estimator's per-device-class state.
    ///     Defaults to "no rows"; production wires this to the live
    ///     `SwiftDataLearnedDeviceProfileStore.snapshot()`. When the
    ///     adaptive feature flag is OFF the store is never queried so
    ///     the array is empty — but the JSON key is still emitted by
    ///     the builder for grep stability.
    ///   - stabilityFetch: playhead-jw63.4 — async read of the local
    ///     MetricKit crash + hang ring buffer, newest first. Defaults to
    ///     "no records"; production wires it to
    ///     `StabilityDiagnosticsStore.shared.recent()`. A device that has
    ///     never crashed yields an empty array, and the builder still
    ///     emits the `stability_diagnostics` key so key presence stays
    ///     grep-stable.
    ///   - bannerTalliesFetch: playhead-bfq7 — async read of the local
    ///     per-episode banner-card tally, oldest session first.
    ///     Defaults to "no rows"; production wires it to
    ///     `BannerTallyStore.shared.sessions`. A device that has been
    ///     shown no cards yields an empty array, and the builder still
    ///     emits the `banner_tallies` key so key presence stays
    ///     grep-stable.
    ///   - rediffFetch: playhead-p70f — async read of the rediff
    ///     re-fetch lane's telemetry (bandwidth ledger, lagged attempt
    ///     states, day-0 attempt records, and the lane's BGTask fires).
    ///     Defaults to `.empty`; production wires it to the live
    ///     `AnalysisStore`. The `rediff_diagnostics` key is always
    ///     emitted, so an empty snapshot still distinguishes "the lane
    ///     has done nothing" from "this bundle predates the lane".
    ///   - analysisStoreHealthFetch: playhead-wvdz — async read of the
    ///     durable record of whether the analysis database opened.
    ///     Defaults to `.healthy`; production wires it to
    ///     `AnalysisStoreHealthJournal.shared.load()`. The
    ///     `analysis_store_health` key is always emitted, so a healthy
    ///     device is distinguishable from a bundle that predates the
    ///     signal.
    ///   - speechModelLoadFetch: playhead-se2h — async read of the
    ///     durable record of whether the ASR model ever loaded, across
    ///     launches. Defaults to `.unknown`; production wires it to
    ///     `SpeechModelLoadJournal.shared.load()`. The default is
    ///     deliberately NOT a healthy value: dropping this argument must
    ///     read as "no evidence", never as "the speech stack is fine".
    ///   - optInSink: adapter that mutates `Episode.diagnosticsOptIn`.
    ///   - optInEpisodes: per-episode inputs for the OptIn bundle. Only
    ///     entries with `diagnosticsOptIn == true` ship; the builder
    ///     filters non-opted rows. The coordinator uses the same filter
    ///     to decide which episode IDs get reset after `.sent` / `.saved`.
    init(
        environment: DiagnosticsExportEnvironment,
        presenter: DiagnosticsExportPresenter,
        journalFetch: @escaping DiagnosticsJournalFetch,
        chapterPhaseEventsFetch: @escaping DiagnosticsChapterPhaseEventsFetch = { [] },
        musicBedProfilesFetch: @escaping DiagnosticsMusicBedProfilesFetch = { [] },
        learnedDeviceProfilesFetch: @escaping DiagnosticsLearnedDeviceProfilesFetch = { [] },
        stabilityFetch: @escaping DiagnosticsStabilityFetch = { [] },
        bannerTalliesFetch: @escaping DiagnosticsBannerTalliesFetch = { [] },
        rediffFetch: @escaping DiagnosticsRediffFetch = { .empty },
        analysisStoreHealthFetch: @escaping DiagnosticsAnalysisStoreHealthFetch = { .healthy },
        speechModelLoadFetch: @escaping DiagnosticsSpeechModelLoadFetch = { .unknown },
        optInSink: DiagnosticsOptInSink,
        optInEpisodes: [DiagnosticsEpisodeInput] = []
    ) {
        self.environment = environment
        self.presenter = presenter
        self.journalFetch = journalFetch
        self.chapterPhaseEventsFetch = chapterPhaseEventsFetch
        self.musicBedProfilesFetch = musicBedProfilesFetch
        self.learnedDeviceProfilesFetch = learnedDeviceProfilesFetch
        self.stabilityFetch = stabilityFetch
        self.bannerTalliesFetch = bannerTalliesFetch
        self.rediffFetch = rediffFetch
        self.analysisStoreHealthFetch = analysisStoreHealthFetch
        self.speechModelLoadFetch = speechModelLoadFetch
        self.optInSink = optInSink
        self.optInEpisodes = optInEpisodes
    }

    // MARK: - Entry points

    /// Build, present, and apply the opt-in reset policy.
    ///
    /// Returns the final composer result. Errors bubble from the fetch,
    /// encode, or presenter layers; `DiagnosticsExportError` is reserved
    /// for coordinator-level failures like a missing host view controller.
    @discardableResult
    func exportAndPresent() async throws -> DiagnosticsMailComposeResult {
        let (data, filename, subject) = try await buildAndEncode()

        let result = try await withCheckedThrowingContinuation { continuation in
            presenter.present(
                data: data,
                filename: filename,
                subject: subject
            ) { outcome in
                continuation.resume(with: outcome)
            }
        }

        applyOptInResetIfNeeded(for: result)
        return result
    }

    /// Fetch + build + encode the bundle. Surfaced for tests that need
    /// to assert encoded JSON shape without driving the presenter.
    func buildAndEncode() async throws -> (data: Data, filename: String, subject: String) {
        // playhead-wvdz: the three throwing fetches are guarded rather
        // than propagated, and each failure is NAMED.
        //
        // WHY. `journalFetch` reads the work journal out of
        // `AnalysisStore`, so `try await journalFetch()` sent the whole
        // export down whenever that store could not be opened — and
        // every UI caller wraps the export in `try?`, so the button
        // simply did nothing. The artifact that would explain why the
        // analysis database is broken could not be produced BECAUSE the
        // analysis database was broken. That is the exact hole
        // playhead-wvdz exists to close, so guarding these reads is part
        // of the observability fix rather than incidental hardening.
        //
        // Naming the failed read is what keeps the fix honest. A bare
        // `try?` would make an unreadable journal indistinguishable from
        // an empty one — "a quantity that names an absence" — which is
        // the same conflation `rediff_diagnostics.read_failures` was
        // added to remove. An empty `work_journal_tail` beside
        // `export_read_failures: ["work_journal"]` says something very
        // different from an empty one on its own.
        var exportReadFailures: [AnalysisStoreHealthState.ExportRead] = []

        let journal: [WorkJournalEntry]
        do {
            journal = try await journalFetch()
        } catch {
            journal = []
            exportReadFailures.append(.workJournal)
        }

        let chapterPhaseEvents: [ChapterPhaseEvent]
        do {
            chapterPhaseEvents = try await chapterPhaseEventsFetch()
        } catch {
            chapterPhaseEvents = []
            exportReadFailures.append(.chapterPhaseEvents)
        }

        let learnedDeviceProfiles: [LearnedDeviceProfileDiagnosticRecord]
        do {
            learnedDeviceProfiles = try await learnedDeviceProfilesFetch()
        } catch {
            learnedDeviceProfiles = []
            exportReadFailures.append(.learnedDeviceProfiles)
        }

        let musicBedProfileSnapshots = await musicBedProfilesFetch()
        let stabilityDiagnostics = await stabilityFetch()
        let bannerTallies = await bannerTalliesFetch()
        let rediff = await rediffFetch()
        let analysisStoreHealth = await analysisStoreHealthFetch()
            .withExportReadFailures(exportReadFailures)
        let speechModelLoad = await speechModelLoadFetch()

        let defaultBundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: environment.appVersion,
            osVersion: environment.osVersion,
            deviceClass: environment.deviceClass,
            buildType: environment.buildType,
            eligibility: environment.eligibility,
            workJournalEntries: journal,
            installID: environment.installID,
            chapterPhaseEvents: chapterPhaseEvents,
            musicBedProfileSnapshots: musicBedProfileSnapshots,
            learnedDeviceProfiles: learnedDeviceProfiles,
            stabilityDiagnostics: stabilityDiagnostics,
            bannerTallies: bannerTallies,
            rediff: rediff,
            analysisStoreHealth: analysisStoreHealth,
            speechModelLoad: speechModelLoad,
            // playhead-i7kvl.3: the north-star counters. Read from the shared
            // store at export time — they were recorded on-device and rode in
            // NO export, so manual reaches per listening hour was uncomputable
            // from anything a tester could send. `recorded: true` because a
            // store WAS consulted; a zero here is a measured zero, which is the
            // distinction `.unrecorded` exists to preserve.
            analyticsCounters: DefaultBundle.AnalyticsCounters(
                byMetric: AnalyticsCounterStore.shared.state.totals.exportable(),
                recorded: true
            )
        )
        let optInBundle = DiagnosticsBundleBuilder.buildOptIn(episodes: optInEpisodes)

        let file = DiagnosticsBundleFile(
            generatedAt: environment.now,
            default: defaultBundle,
            optIn: optInBundle
        )
        let data = try DiagnosticsExportService.encode(file)
        let filename = DiagnosticsExportService.filename(for: environment.now)
        let subject = DiagnosticsExportService.defaultSubject(buildType: environment.buildType)
        return (data, filename, subject)
    }

    /// playhead-jw63.5 — apply legal checklist item (d) for a bundle that
    /// this coordinator BUILT but did not itself present.
    ///
    /// The listener-feedback channel reuses `buildAndEncode()` for its
    /// optional attachment and presents through its own envelope, so the
    /// reset that `exportAndPresent()` performs inline would otherwise be
    /// skipped on that path — leaving `Episode.diagnosticsOptIn` set after
    /// the bundle had already left the device. Exposed as a named seam
    /// (rather than by widening `applyOptInResetIfNeeded`'s access) so the
    /// only two callers are both obvious in source.
    func applyOptInReset(for result: DiagnosticsMailComposeResult) {
        applyOptInResetIfNeeded(for: result)
    }

    // MARK: - Reset policy application

    /// Uses `DiagnosticsOptInResetPolicy` to decide whether the opted-in
    /// episodes should have their flag cleared. On `.sent` / `.saved` we
    /// pass the full set of opted-in episode IDs to the sink; on
    /// `.cancelled` / `.failed` we short-circuit and do not touch the sink
    /// (preserving the flag for the next retry).
    private func applyOptInResetIfNeeded(for result: DiagnosticsMailComposeResult) {
        // Ask the policy directly: "is this a delivery-confirming
        // result?" `shouldReset(result:)` is kept consistent with
        // `newValue(current: true, result:) == false` by a dedicated
        // unit test.
        guard DiagnosticsOptInResetPolicy.shouldReset(result: result) else { return }

        let includedIds = optInEpisodes
            .filter(\.diagnosticsOptIn)
            .map(\.episodeId)
        guard !includedIds.isEmpty else { return }
        optInSink.applyResetToEpisodes(
            matchingEpisodeIds: includedIds,
            newValue: false
        )
    }
}
