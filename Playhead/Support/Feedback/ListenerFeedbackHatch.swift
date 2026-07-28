// ListenerFeedbackHatch.swift
// playhead-jw63.5 — production wiring for the in-app feedback channel.
//
// Sibling of `DebugDiagnosticsHatch` / `ReleaseDiagnosticsHatch` and shaped
// the same way: one free function is the entire view-visible surface, and the
// helpers live on an internal namespace so tests can drive each piece without
// standing up a presenter.
//
// Unlike those two, this hatch is NOT build-config-split — the feedback
// channel ships in Release, which is the whole point. What it does instead is
// alias whichever of the two diagnostics hatches exists in this
// configuration, so the environment construction and the export-coordinator
// graph are REUSED rather than copied a third time:
//
//     #if DEBUG  → DebugDiagnosticsHatch
//     #else      → ReleaseDiagnosticsHatch
//
// Placement note: lives under `Playhead/Support/Feedback/` (not
// `Playhead/Views/`) for the same reason the diagnostics hatches do — it
// touches `AnalysisStore` when building the journal-fetch adapter, and
// `SurfaceStatusUILintTests` (playhead-ol05) forbids that reference from any
// `Playhead/Views/` source file. `SettingsView` and `NowPlayingView` see only
// `runListenerFeedback(...)`.

#if canImport(UIKit) && canImport(MessageUI) && os(iOS)

import Foundation
import MessageUI
import SwiftData
import UIKit

#if DEBUG
private typealias DiagnosticsHatch = DebugDiagnosticsHatch
#else
private typealias DiagnosticsHatch = ReleaseDiagnosticsHatch
#endif

// MARK: - Entry point

/// Compose and present a listener's note.
///
/// Fire-and-forget from the caller's point of view: composer outcomes come
/// back as an `ListenerFeedbackOutcome`, and only a genuine
/// "nothing could be presented" condition throws.
///
/// - Parameters:
///   - context: `.general` (Settings) or `.moment(...)` (banner long-press).
///   - attachDiagnostics: opt-in per send; default `false`.
///   - canSendMail: injected so the mail-unavailable branch is testable
///     without a device. Production reads the live
///     `MFMailComposeViewController.canSendMail()`.
@MainActor
@discardableResult
func runListenerFeedback(
    runtime: PlayheadRuntime,
    modelContext: ModelContext,
    context: ListenerFeedbackContext = .general,
    attachDiagnostics: Bool = false,
    hostProvider: @MainActor @escaping () -> UIViewController? = ListenerFeedbackHatch.defaultHostProvider,
    canSendMail: @MainActor @escaping () -> Bool = ListenerFeedbackHatch.systemCanSendMail
) async throws -> ListenerFeedbackOutcome {
    let environment = try await DiagnosticsHatch.buildEnvironment(
        runtime: runtime,
        modelContext: modelContext
    )
    let presenter = UIKitDiagnosticsPresenter(hostProvider: hostProvider)
    let exportCoordinator = ListenerFeedbackHatch.makeExportCoordinator(
        runtime: runtime,
        modelContext: modelContext,
        presenter: presenter,
        environment: environment
    )

    let coordinator = ListenerFeedbackCoordinator(
        environment: ListenerFeedbackEnvironmentSummary(environment),
        presenter: presenter,
        canSendMail: canSendMail,
        attachmentBuilder: {
            let encoded = try await exportCoordinator.buildAndEncode()
            return ListenerFeedbackAttachment(
                data: encoded.data,
                filename: encoded.filename
            )
        },
        // Legal checklist (d): a bundle that actually shipped clears the
        // per-episode opt-in flag, on this delivery surface exactly as on
        // `DiagnosticsExportCoordinator.exportAndPresent()`.
        onAttachmentDelivered: { result in
            exportCoordinator.applyOptInReset(for: result)
        }
    )

    return try await coordinator.send(
        context: context,
        attachDiagnostics: attachDiagnostics
    )
}

/// Salted reference token for the banner-context entry. Returns `nil` when
/// the install identity cannot be provisioned — the note then simply carries
/// the moment without a reference, which is still worth sending.
@MainActor
func listenerFeedbackReference(
    modelContext: ModelContext,
    episodeId: String?
) -> String? {
    guard let episodeId, !episodeId.isEmpty else { return nil }
    guard let installID = try? InstallIDProvider(context: modelContext).installID() else {
        return nil
    }
    return ListenerFeedbackComposer.reference(installID: installID, episodeId: episodeId)
}

// MARK: - Hatch helpers (internal for tests)

@MainActor
enum ListenerFeedbackHatch {

    /// The live mail-availability reading. Wrapped as a stored closure so the
    /// production default and the injected test double have the same type,
    /// and so `MFMailComposeViewController` appears at exactly one call site.
    static let systemCanSendMail: @MainActor () -> Bool = {
        MFMailComposeViewController.canSendMail()
    }

    /// Reuses whichever diagnostics hatch this build configuration compiled.
    static let defaultHostProvider: @MainActor () -> UIViewController? = {
        DiagnosticsHatch.defaultHostProvider()
    }

    /// Assemble the diagnostics export coordinator used to produce the
    /// OPTIONAL attachment. Every dependency comes from the existing
    /// diagnostics hatch helpers, so the bundle this channel can attach is
    /// byte-identical to the one "Send diagnostics" produces.
    ///
    /// `optInEpisodes: []` matches both existing hatches — dogfood ships the
    /// default (non-opt-in) bundle.
    ///
    /// The `presenter` argument is required by the coordinator's initializer
    /// but is never exercised on this path: the feedback channel calls only
    /// `buildAndEncode()` and `applyOptInReset(for:)`, and presents through
    /// its own envelope. We hand it the SAME presenter instance rather than a
    /// second one so there is no ambiguity about which object owns the mail
    /// composer if a future caller does reach `exportAndPresent()`.
    static func makeExportCoordinator(
        runtime: PlayheadRuntime,
        modelContext: ModelContext,
        presenter: DiagnosticsExportPresenter,
        environment: DiagnosticsExportEnvironment
    ) -> DiagnosticsExportCoordinator {
        let musicBedStore = ShowMusicBedProfileStore(
            modelContainer: modelContext.container
        )
        let musicBedProfilesFetch: DiagnosticsMusicBedProfilesFetch = {
            await musicBedStore.allSnapshots()
        }
        return DiagnosticsExportCoordinator(
            environment: environment,
            presenter: presenter,
            journalFetch: DiagnosticsHatch.makeJournalFetch(store: runtime.analysisStore),
            musicBedProfilesFetch: musicBedProfilesFetch,
            learnedDeviceProfilesFetch: DiagnosticsHatch.makeLearnedDeviceProfilesFetch(
                modelContext: modelContext
            ),
            stabilityFetch: DiagnosticsHatch.stabilityFetch,
            bannerTalliesFetch: DiagnosticsHatch.bannerTalliesFetch,
            optInSink: SwiftDataDiagnosticsOptInSink(context: modelContext),
            optInEpisodes: []
        )
    }
}

#endif // canImport(UIKit) && canImport(MessageUI) && os(iOS)
