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
    private let optInSink: DiagnosticsOptInSink
    private let optInEpisodes: [DiagnosticsEpisodeInput]

    // MARK: - Init

    /// - Parameters:
    ///   - environment: static build/eligibility/install inputs.
    ///   - presenter: UI adapter (UIKit composer or test fake).
    ///   - journalFetch: async fetch of the most-recent WorkJournal rows.
    ///   - optInSink: adapter that mutates `Episode.diagnosticsOptIn`.
    ///   - optInEpisodes: per-episode inputs for the OptIn bundle. Only
    ///     entries with `diagnosticsOptIn == true` ship; the builder
    ///     filters non-opted rows. The coordinator uses the same filter
    ///     to decide which episode IDs get reset after `.sent` / `.saved`.
    init(
        environment: DiagnosticsExportEnvironment,
        presenter: DiagnosticsExportPresenter,
        journalFetch: @escaping DiagnosticsJournalFetch,
        optInSink: DiagnosticsOptInSink,
        optInEpisodes: [DiagnosticsEpisodeInput] = []
    ) {
        self.environment = environment
        self.presenter = presenter
        self.journalFetch = journalFetch
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
        let journal = try await journalFetch()

        let defaultBundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: environment.appVersion,
            osVersion: environment.osVersion,
            deviceClass: environment.deviceClass,
            buildType: environment.buildType,
            eligibility: environment.eligibility,
            workJournalEntries: journal,
            installID: environment.installID
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
