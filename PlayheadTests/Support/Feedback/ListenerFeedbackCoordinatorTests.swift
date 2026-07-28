// ListenerFeedbackCoordinatorTests.swift
// playhead-jw63.5 — orchestration tests for the in-app feedback channel,
// with the MAIL-UNAVAILABLE path as the centrepiece.
//
// That path is the one most likely to dead-end in the field: no mail account
// configured, the simulator, some Mac Catalyst installs. It is also the path
// that a device-only test could never cover, because
// `MFMailComposeViewController.canSendMail()` cannot be forced true on the
// simulator NOR false on a normally-configured phone. The coordinator takes
// `canSendMail` as an injected closure precisely so both directions are real,
// asserting tests here — no `withKnownIssue`, no conditional skip.

import Foundation
import Testing

@testable import Playhead

// MARK: - Test doubles

/// Records what the coordinator asked to present and replies with a canned
/// outcome. Stands in for `UIKitDiagnosticsPresenter` (which needs a live
/// `UIViewController` host).
@MainActor
private final class RecordingFeedbackPresenter: ListenerFeedbackPresenting {
    private(set) var envelopes: [ListenerFeedbackEnvelope] = []
    private(set) var deliveries: [ListenerFeedbackDelivery] = []
    var reply: Result<DiagnosticsMailComposeResult, Error> = .success(.sent)

    var lastEnvelope: ListenerFeedbackEnvelope? { envelopes.last }

    func present(
        envelope: ListenerFeedbackEnvelope,
        delivery: ListenerFeedbackDelivery,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    ) {
        envelopes.append(envelope)
        deliveries.append(delivery)
        completion(reply)
    }
}

private enum StubError: Error { case bundleBuildFailed }

@Suite("ListenerFeedbackCoordinator (playhead-jw63.5)")
@MainActor
struct ListenerFeedbackCoordinatorTests {

    private static let environment = ListenerFeedbackEnvironmentSummary(
        appVersion: "1.4.2",
        osVersion: "27.0.1",
        deviceClass: "iPhone17Pro"
    )

    private static func attachment() -> ListenerFeedbackAttachment {
        ListenerFeedbackAttachment(
            data: Data(#"{"default":{}}"#.utf8),
            filename: "playhead-diagnostics-2026-07-28T00-00-00Z.json"
        )
    }

    private static func makeCoordinator(
        presenter: RecordingFeedbackPresenter,
        canSendMail: Bool,
        attachmentBuilder: ListenerFeedbackCoordinator.AttachmentBuilder? = nil,
        onAttachmentDelivered: ListenerFeedbackCoordinator.DeliveryObserver? = nil
    ) -> ListenerFeedbackCoordinator {
        ListenerFeedbackCoordinator(
            environment: environment,
            presenter: presenter,
            canSendMail: { canSendMail },
            attachmentBuilder: attachmentBuilder,
            onAttachmentDelivered: onAttachmentDelivered
        )
    }

    // MARK: - Mail available

    @Test("mail configured: presents the composer with an addressed, prefilled envelope")
    func mailPathPresentsComposer() async throws {
        let presenter = RecordingFeedbackPresenter()
        let coordinator = Self.makeCoordinator(presenter: presenter, canSendMail: true)

        let outcome = try await coordinator.send(context: .general)

        #expect(outcome.delivery == .mail)
        #expect(outcome.result == .sent)
        #expect(outcome.attachedDiagnostics == false)
        #expect(presenter.deliveries == [.mail])
        #expect(presenter.lastEnvelope?.recipients == [ListenerFeedbackCopy.recipient])
        #expect(presenter.lastEnvelope?.subject == ListenerFeedbackCopy.generalSubject)
        #expect(presenter.lastEnvelope?.body.contains("Playhead 1.4.2") == true)
    }

    // MARK: - Mail UNAVAILABLE (the path that must not dead-end)

    @Test("no mail account: falls back to the share sheet instead of failing")
    func mailUnavailableFallsBackToShareSheet() async throws {
        let presenter = RecordingFeedbackPresenter()
        let coordinator = Self.makeCoordinator(presenter: presenter, canSendMail: false)

        let outcome = try await coordinator.send(context: .general)

        #expect(outcome.delivery == .shareSheet)
        #expect(outcome.result == .sent)
        #expect(presenter.deliveries == [.shareSheet])
    }

    @Test("share-sheet fallback still carries the complete, self-sufficient note")
    func fallbackEnvelopeIsSelfSufficient() async throws {
        let presenter = RecordingFeedbackPresenter()
        let coordinator = Self.makeCoordinator(presenter: presenter, canSendMail: false)

        _ = try await coordinator.send(
            context: .moment(atSeconds: 754, reference: "a1b2c3d4")
        )

        let envelope = try #require(presenter.lastEnvelope)
        // A share item has no recipient/subject fields, so everything the
        // reader needs has to survive inside `shareText`. If any of these
        // is missing the fallback is a dead end in slow motion: the note
        // arrives somewhere with no way to tell what it is about.
        #expect(envelope.shareText.contains(ListenerFeedbackCopy.momentSubject))
        #expect(envelope.shareText.contains(ListenerFeedbackCopy.bodyPrompt))
        #expect(envelope.shareText.contains("Moment: 12:34 · ref a1b2c3d4"))
        #expect(envelope.shareText.contains("Playhead 1.4.2 · iOS 27.0.1 · iPhone17Pro"))
        #expect(envelope.recipients == [ListenerFeedbackCopy.recipient])
        #expect(!envelope.shareText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
    }

    @Test("cancelling the fallback still tells the listener where to reach us")
    func cancelledFallbackNamesAddress() async throws {
        let presenter = RecordingFeedbackPresenter()
        presenter.reply = .success(.cancelled)
        let coordinator = Self.makeCoordinator(presenter: presenter, canSendMail: false)

        let outcome = try await coordinator.send()

        #expect(outcome.delivery == .shareSheet)
        #expect(outcome.result == .cancelled)
        let notice = try #require(ListenerFeedbackComposer.notice(for: outcome))
        #expect(notice.contains(ListenerFeedbackCopy.recipient))
    }

    @Test("the fallback carries the opt-in bundle too")
    func fallbackCarriesAttachment() async throws {
        let presenter = RecordingFeedbackPresenter()
        let coordinator = Self.makeCoordinator(
            presenter: presenter,
            canSendMail: false,
            attachmentBuilder: { Self.attachment() }
        )

        let outcome = try await coordinator.send(attachDiagnostics: true)

        #expect(outcome.attachedDiagnostics)
        #expect(presenter.lastEnvelope?.attachment == Self.attachment())
    }

    // MARK: - Attachment opt-in

    @Test("attachment is opt-in: the builder is not even consulted when off")
    func attachmentIsOptIn() async throws {
        let presenter = RecordingFeedbackPresenter()
        let calls = Counter()
        let coordinator = Self.makeCoordinator(
            presenter: presenter,
            canSendMail: true,
            attachmentBuilder: {
                calls.increment()
                return Self.attachment()
            }
        )

        let outcome = try await coordinator.send(attachDiagnostics: false)

        #expect(calls.value == 0)
        #expect(outcome.attachedDiagnostics == false)
        #expect(presenter.lastEnvelope?.attachment == nil)
        #expect(presenter.lastEnvelope?.body.contains(ListenerFeedbackCopy.attachmentNote) == false)
    }

    @Test("opting in attaches the bundle and discloses it in the body")
    func attachmentOptedIn() async throws {
        let presenter = RecordingFeedbackPresenter()
        let coordinator = Self.makeCoordinator(
            presenter: presenter,
            canSendMail: true,
            attachmentBuilder: { Self.attachment() }
        )

        let outcome = try await coordinator.send(attachDiagnostics: true)

        #expect(outcome.attachedDiagnostics)
        #expect(presenter.lastEnvelope?.attachment == Self.attachment())
        #expect(presenter.lastEnvelope?.body.contains(ListenerFeedbackCopy.attachmentNote) == true)
    }

    @Test("a failing bundle build downgrades to a plain note — the message is never lost")
    func attachmentFailureDowngrades() async throws {
        let presenter = RecordingFeedbackPresenter()
        let coordinator = Self.makeCoordinator(
            presenter: presenter,
            canSendMail: true,
            attachmentBuilder: { throw StubError.bundleBuildFailed }
        )

        let outcome = try await coordinator.send(attachDiagnostics: true)

        #expect(outcome.result == .sent)
        #expect(outcome.attachedDiagnostics == false)
        #expect(presenter.lastEnvelope?.attachment == nil)
        // And the body must not promise an attachment that is not there.
        #expect(presenter.lastEnvelope?.body.contains(ListenerFeedbackCopy.attachmentNote) == false)
    }

    @Test("no diagnostics source wired: opting in is a no-op, not an error")
    func attachmentWithoutBuilder() async throws {
        let presenter = RecordingFeedbackPresenter()
        let coordinator = Self.makeCoordinator(presenter: presenter, canSendMail: true)

        let outcome = try await coordinator.send(attachDiagnostics: true)

        #expect(outcome.result == .sent)
        #expect(outcome.attachedDiagnostics == false)
    }

    // MARK: - Legal checklist (d) on the second delivery surface

    @Test("opt-in reset observer fires only when a bundle actually shipped")
    func resetObserverFiresOnlyWithAttachment() async throws {
        let presenter = RecordingFeedbackPresenter()
        let observed = ResultBox()

        let withoutAttachment = Self.makeCoordinator(
            presenter: presenter,
            canSendMail: true,
            attachmentBuilder: { Self.attachment() },
            onAttachmentDelivered: { observed.record($0) }
        )
        _ = try await withoutAttachment.send(attachDiagnostics: false)
        #expect(observed.values.isEmpty)

        let withAttachment = Self.makeCoordinator(
            presenter: presenter,
            canSendMail: true,
            attachmentBuilder: { Self.attachment() },
            onAttachmentDelivered: { observed.record($0) }
        )
        _ = try await withAttachment.send(attachDiagnostics: true)
        #expect(observed.values == [.sent])
    }

    @Test("the reset observer receives the real composer result, not an assumed one")
    func resetObserverForwardsResult() async throws {
        let presenter = RecordingFeedbackPresenter()
        presenter.reply = .success(.cancelled)
        let observed = ResultBox()
        let coordinator = Self.makeCoordinator(
            presenter: presenter,
            canSendMail: true,
            attachmentBuilder: { Self.attachment() },
            onAttachmentDelivered: { observed.record($0) }
        )

        _ = try await coordinator.send(attachDiagnostics: true)

        // `.cancelled` must reach the policy so it PRESERVES the opt-in flag
        // (legal checklist item (d)); swallowing it here would silently
        // clear opt-in on a note the listener backed out of.
        #expect(observed.values == [.cancelled])
    }

    // MARK: - Hard failure

    @Test("a presenter that cannot present at all surfaces the error to the caller")
    func presenterErrorPropagates() async {
        let presenter = RecordingFeedbackPresenter()
        presenter.reply = .failure(DiagnosticsExportError.missingHostViewController)
        let coordinator = Self.makeCoordinator(presenter: presenter, canSendMail: true)

        await #expect(throws: DiagnosticsExportError.missingHostViewController) {
            try await coordinator.send()
        }
    }
}

// MARK: - Small mutable boxes (MainActor-isolated, no concurrency needed)

@MainActor
private final class Counter {
    private(set) var value = 0
    func increment() { value += 1 }
}

@MainActor
private final class ResultBox {
    private(set) var values: [DiagnosticsMailComposeResult] = []
    func record(_ result: DiagnosticsMailComposeResult) { values.append(result) }
}
