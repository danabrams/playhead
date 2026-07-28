// ListenerFeedbackCoordinator.swift
// playhead-jw63.5 — @MainActor orchestrator for the in-app feedback channel:
// decide delivery → (optionally) build the attachment → present → report.
//
// Deliberately shaped like `DiagnosticsExportCoordinator` (pure orchestration,
// every UIKit/SwiftData dependency behind an injected seam) so the two read
// side by side. It does NOT re-implement any of that coordinator's work:
//   * the attachment bytes come from
//     `DiagnosticsExportCoordinator.buildAndEncode()`, so the bundle's shape
//     and scrubbing are untouched by this bead;
//   * `applyOptInReset(for:)` on that same coordinator applies legal
//     checklist item (d) when a bundle actually rode along, so the second
//     delivery surface inherits the reset policy rather than inventing one.
//
// Two invariants this file exists to hold:
//
//   1. THE NOTE IS NEVER LOST. Every failure mode downgrades rather than
//      aborting. Mail unavailable → share sheet. Attachment build throws →
//      send the note without it. Only "we could not present anything at all"
//      escapes as an error, and the caller turns that into a notice that
//      names our address in plain text.
//
//   2. THE AVAILABILITY GATE IS INJECTED. `canSendMail` arrives as a closure
//      so the mail-unavailable branch is a real, asserting test on the
//      simulator (where `MFMailComposeViewController.canSendMail()` is
//      permanently false and the mail branch would otherwise be untestable
//      in the other direction).

import Foundation

// MARK: - Presenter seam

/// UI adapter for the feedback envelope. Production is
/// `UIKitDiagnosticsPresenter` (the same class that already presents the
/// diagnostics bundle — same composer, same delegate proxy, same tmp-file
/// lifecycle); tests inject a fake that records the envelope and completes
/// with a canned result.
@MainActor
protocol ListenerFeedbackPresenting {
    func present(
        envelope: ListenerFeedbackEnvelope,
        delivery: ListenerFeedbackDelivery,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    )
}

// MARK: - Outcome

/// What actually happened, in enough detail for the caller to say something
/// truthful about it. `delivery` is the branch we ended up on — not the one
/// we asked for — because the presenter is allowed to downgrade mail to the
/// share sheet if composer construction declines after the availability
/// check said yes.
struct ListenerFeedbackOutcome: Equatable, Sendable {
    let delivery: ListenerFeedbackDelivery
    let result: DiagnosticsMailComposeResult
    /// Whether a diagnostics bundle actually rode along. False when the
    /// listener did not opt in AND when they did but the build failed.
    let attachedDiagnostics: Bool
}

// MARK: - Coordinator

@MainActor
final class ListenerFeedbackCoordinator {

    /// Produces the encoded diagnostics bundle. `nil` when no diagnostics
    /// source is wired (previews, isolated hosts) — the opt-in toggle then
    /// simply yields a note with no attachment rather than an error.
    typealias AttachmentBuilder = @MainActor () async throws -> ListenerFeedbackAttachment

    /// Invoked with the composer result ONLY when a bundle actually shipped.
    /// Production wires this to `DiagnosticsExportCoordinator
    /// .applyOptInReset(for:)` so legal checklist item (d) holds on this
    /// delivery surface too.
    typealias DeliveryObserver = @MainActor (DiagnosticsMailComposeResult) -> Void

    private let environment: ListenerFeedbackEnvironmentSummary
    private let presenter: ListenerFeedbackPresenting
    private let canSendMail: @MainActor () -> Bool
    private let attachmentBuilder: AttachmentBuilder?
    private let onAttachmentDelivered: DeliveryObserver?

    init(
        environment: ListenerFeedbackEnvironmentSummary,
        presenter: ListenerFeedbackPresenting,
        canSendMail: @escaping @MainActor () -> Bool,
        attachmentBuilder: AttachmentBuilder? = nil,
        onAttachmentDelivered: DeliveryObserver? = nil
    ) {
        self.environment = environment
        self.presenter = presenter
        self.canSendMail = canSendMail
        self.attachmentBuilder = attachmentBuilder
        self.onAttachmentDelivered = onAttachmentDelivered
    }

    /// Compose and present the listener's note.
    ///
    /// - Parameters:
    ///   - context: `.general` for the Settings entry, `.moment(...)` for the
    ///     banner-context entry.
    ///   - attachDiagnostics: opt-in, per send. Default `false` — see the
    ///     bead report's BUNDLE ATTACHMENT note: the bundle is scrubbed, but
    ///     silently attaching it to a one-sentence note would undercut the
    ///     verbatim About-screen promise ("Your podcasts never leave your
    ///     device") that this channel is supposed to reinforce.
    /// - Throws: only when NOTHING could be presented (e.g. no host view
    ///   controller). Composer outcomes — including `.failed` — come back as
    ///   values.
    @discardableResult
    func send(
        context: ListenerFeedbackContext = .general,
        attachDiagnostics: Bool = false
    ) async throws -> ListenerFeedbackOutcome {
        let requestedDelivery = ListenerFeedbackComposer.delivery(canSendMail: canSendMail())
        let attachment = attachDiagnostics ? await buildAttachment() : nil
        let envelope = ListenerFeedbackComposer.envelope(
            context: context,
            environment: environment,
            attachment: attachment
        )

        let result = try await withCheckedThrowingContinuation { continuation in
            presenter.present(
                envelope: envelope,
                delivery: requestedDelivery
            ) { outcome in
                continuation.resume(with: outcome)
            }
        }

        if attachment != nil {
            onAttachmentDelivered?(result)
        }

        return ListenerFeedbackOutcome(
            delivery: requestedDelivery,
            result: result,
            attachedDiagnostics: attachment != nil
        )
    }

    /// Build the attachment, downgrading any failure to "no attachment".
    ///
    /// A diagnostics build can fail for reasons that have nothing to do with
    /// the listener (a SwiftData read error, an encoder failure). Throwing
    /// here would discard a note that was already written — strictly worse
    /// than sending it without the file, which support can request later.
    private func buildAttachment() async -> ListenerFeedbackAttachment? {
        guard let attachmentBuilder else { return nil }
        do {
            return try await attachmentBuilder()
        } catch {
            return nil
        }
    }
}
