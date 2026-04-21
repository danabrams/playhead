// BatchNotificationService.swift
// User-notification surface for the "Download-Next-N" batch pipeline
// (playhead-zp0x). HARD CONTRACT: the only public emission method is
// `emit(eligibility:batch:)` — the service does NOT accept raw
// `SurfaceReason`, free-form text, dictionaries, or any other open-
// ended payload. Copy lookup is a private switch on
// `BatchNotificationEligibility` inside the service body.
//
// Rationale (spec §D5): if call sites can type their own notification
// copy then the snapshot tests in `BatchNotificationCopyTests` no
// longer pin every user-visible string, and a regression that misroutes
// an action-required notification can ship without anyone noticing.
// Forcing every call through the enum-typed surface means a typo or
// new call site fails to compile until it picks one of the five
// whitelisted cases.
//
// Permission ask is NOT performed here — it is requested at the
// `DownloadNextView` submit site where the user has just expressed
// intent. The service assumes authorization has already been
// requested (or denied) and silently no-ops on `.none` /
// authorization-denied at the OS level.

import Foundation
import OSLog
import UserNotifications

/// User-notification surface for batch-notification triggers. Wrapped
/// over `UNUserNotificationCenter` so tests can substitute a recording
/// scheduler via the `Scheduler` protocol.
///
/// One instance per `PlayheadRuntime`; called from
/// `BatchNotificationCoordinator` once the reducer + cap rules clear.
///
/// `@MainActor`-isolated because `emit(eligibility:batch:)` accepts a
/// SwiftData `@Model` (`DownloadBatch`) which is not `Sendable`. The
/// caller (`BatchNotificationCoordinator`) is also MainActor-bound, so
/// pinning the service here keeps the @Model-pass legal under Swift 6
/// strict concurrency without an extra hop.
@MainActor
final class BatchNotificationService {

    // MARK: - Scheduler protocol

    /// Minimal notification-scheduling surface so tests can record
    /// emitted requests without touching `UNUserNotificationCenter`.
    /// Production passes a wrapper around the live center
    /// (`SystemNotificationScheduler`).
    ///
    /// `@MainActor`-isolated because `UNNotificationRequest` is non-
    /// Sendable; pinning the protocol method to MainActor matches the
    /// service's own isolation and lets every conformer accept the
    /// request without crossing an isolation boundary.
    protocol Scheduler: Sendable {
        @MainActor
        func add(_ request: UNNotificationRequest) async throws
    }

    // MARK: - Dependencies

    private let scheduler: any Scheduler
    private let logger = Logger(
        subsystem: "com.playhead",
        category: "BatchNotificationService"
    )

    // MARK: - Init

    init(scheduler: any Scheduler = SystemNotificationScheduler()) {
        self.scheduler = scheduler
    }

    // MARK: - Public API (HARD CONTRACT)

    /// The ONLY public emission method. Every call site MUST route
    /// through this surface.
    ///
    /// The service does NOT accept raw `SurfaceReason`, arbitrary
    /// text, dictionaries, or any other open-ended payload — copy
    /// lookup is a private switch on the enum inside the service.
    /// Adding a `String`-accepting overload here would defeat the
    /// snapshot-test guarantee that every user-visible notification
    /// string is pinned in `BatchNotificationCopy`.
    ///
    /// `eligibility == .none` is a no-op (no notification scheduled).
    /// The coordinator never calls this with `.none` in production,
    /// but the no-op safety net keeps the contract honest.
    func emit(
        eligibility: BatchNotificationEligibility,
        batch: DownloadBatch
    ) async {
        guard let copy = renderCopy(for: eligibility, batch: batch) else {
            // .none falls here — silently no-op.
            return
        }

        let content = UNMutableNotificationContent()
        content.title = copy.title
        content.body = copy.body
        content.sound = .default
        // Stamp the batch id and trigger so a future deep-link / tap
        // handler can route the user to the right surface (e.g.
        // Settings → Storage for blockedStorage).
        content.userInfo = [
            "batchId": batch.id.uuidString,
            "trigger": eligibility.rawValue,
        ]

        let request = UNNotificationRequest(
            identifier: "batch-\(batch.id.uuidString)-\(eligibility.rawValue)",
            content: content,
            trigger: nil   // immediate
        )

        do {
            try await scheduler.add(request)
            logger.info(
                "emit \(eligibility.rawValue, privacy: .public) batch=\(batch.id.uuidString, privacy: .public)"
            )
        } catch {
            logger.error(
                "emit failed for \(eligibility.rawValue, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }
    }

    // MARK: - Private copy switch

    private struct RenderedCopy {
        let title: String
        let body: String
    }

    private func renderCopy(
        for eligibility: BatchNotificationEligibility,
        batch: DownloadBatch
    ) -> RenderedCopy? {
        // Decode the batch's trip context for trip-ready body
        // composition. Default to `.generic` if the rawValue is
        // somehow malformed — generic batches never reach this layer
        // (the coordinator filters them) so the worst case is a
        // generic-flavored body on a malformed row.
        let context = DownloadTripContext(rawValue: batch.tripContextRaw) ?? .generic

        switch eligibility {
        case .tripReady:
            return RenderedCopy(
                title: BatchNotificationCopy.tripReadyTitle,
                body: BatchNotificationCopy.tripReadyBody(context: context)
            )
        case .blockedStorage:
            return RenderedCopy(
                title: BatchNotificationCopy.blockedStorageTitle,
                body: BatchNotificationCopy.blockedStorageBody
            )
        case .blockedWifiPolicy:
            return RenderedCopy(
                title: BatchNotificationCopy.blockedWifiPolicyTitle,
                body: BatchNotificationCopy.blockedWifiPolicyBody
            )
        case .blockedAnalysisUnavailable:
            return RenderedCopy(
                title: BatchNotificationCopy.blockedAnalysisUnavailableTitle,
                body: BatchNotificationCopy.blockedAnalysisUnavailableBody
            )
        case .none:
            return nil
        }
    }
}

// MARK: - SystemNotificationScheduler

/// Production wrapper over `UNUserNotificationCenter.current()`. Lives
/// in this file so the protocol's only production conformer is
/// alongside the contract definition.
struct SystemNotificationScheduler: BatchNotificationService.Scheduler {
    func add(_ request: UNNotificationRequest) async throws {
        try await UNUserNotificationCenter.current().add(request)
    }
}
