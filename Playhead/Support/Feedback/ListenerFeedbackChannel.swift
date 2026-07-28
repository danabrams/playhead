// ListenerFeedbackChannel.swift
// playhead-jw63.5 — the pure, UI-free half of the in-app feedback channel:
// verbatim copy, the mail envelope, and the two decisions that govern the
// flow (mail vs share sheet; what the prefilled body says).
//
// Why this file has no UIKit import
// ---------------------------------
// `MFMailComposeViewController.canSendMail()` is the real-world gate and it
// is FALSE on the simulator and on a device with no configured account. If
// the availability check lived inside the presenter as a direct static call,
// the mail-unavailable path — the one most likely to dead-end in the field —
// could only be exercised on a physical device with the accounts removed. So
// the gate is modelled here as a pure function over a `Bool`, and the live
// `canSendMail()` reading is INJECTED at the coordinator seam. Both branches
// are then real assertions in the simulator gate, never a skipped test.
//
// Reuse note (bead constraint: extend, do not rebuild)
// ---------------------------------------------------
// Delivery reuses the existing diagnostics export plumbing wholesale:
//   * `DiagnosticsExportService.makeMailComposer` / `makeActivityFallback`
//     gain feedback-shaped siblings in the same file.
//   * `UIKitDiagnosticsPresenter` gains a `ListenerFeedbackPresenting`
//     conformance — same class, same `MailComposeDelegateProxy`, same
//     tmp-file lifecycle.
//   * The optional attachment is produced by
//     `DiagnosticsExportCoordinator.buildAndEncode()`. Nothing about the
//     bundle's shape, scrubbing, or legal checklist changes here — this bead
//     adds a second DELIVERY surface for bytes that already existed.
//
// On-device mandate
// -----------------
// The mail body is a new exfiltration surface, so it is held to the same bar
// as the bundle: no audio, no transcripts, no episode titles, no feed URLs.
// The only episode reference it can carry is the SALTED hash prefix that the
// bundle already emits as `episode_id_hash` (which is also what makes the
// note correlate with an attached bundle). `ListenerFeedbackRedactionTests`
// sweeps sentinels through the composed envelope to keep that honest.

import Foundation

// MARK: - Delivery decision

/// Which surface the listener's note travels on. Deliberately a two-case
/// enum rather than an optional composer so both branches are named,
/// exhaustive, and assertable.
enum ListenerFeedbackDelivery: String, Equatable, Sendable, CaseIterable {
    /// `MFMailComposeViewController` — prefilled recipient, subject, body,
    /// and (when opted in) the diagnostics attachment.
    case mail
    /// `UIActivityViewController` carrying the same text as a share item.
    /// Used when Mail is not configured (simulator, a device with no mail
    /// account, some Catalyst installs). NOT a dead end: the fallback keeps
    /// Copy and Messages available precisely so the listener always has a
    /// way to reach us.
    case shareSheet
}

// MARK: - Attachment

/// An already-encoded diagnostics bundle, ready to ride along. Produced by
/// `DiagnosticsExportCoordinator.buildAndEncode()`; this type deliberately
/// carries opaque bytes so the feedback layer can never reshape the bundle.
struct ListenerFeedbackAttachment: Equatable, Sendable {

    /// Kept in lock-step with `DiagnosticsExportService.attachmentMIMEType`
    /// and pinned equal to it by `ListenerFeedbackChannelTests`. Spelled out
    /// rather than referenced because that service is `@MainActor`-isolated,
    /// and this value type must be constructible from any isolation domain
    /// (including a default argument, which is evaluated at the call site).
    static let defaultMIMEType = "application/json"

    let data: Data
    let filename: String
    let mimeType: String

    init(
        data: Data,
        filename: String,
        mimeType: String = ListenerFeedbackAttachment.defaultMIMEType
    ) {
        self.data = data
        self.filename = filename
        self.mimeType = mimeType
    }
}

// MARK: - Context

/// Where the listener started from. `general` is the Settings entry;
/// `moment` is the banner-context entry and carries only a playback offset
/// plus an optional salted reference token — never a title, never text.
struct ListenerFeedbackContext: Equatable, Sendable {
    /// Playback offset, in episode seconds, of the moment that prompted the
    /// note. `nil` for the Settings entry.
    let momentSeconds: Double?
    /// Short salted-hash prefix identifying the episode, so a note and a
    /// (separately sent) diagnostics bundle can be correlated without the
    /// note ever naming the episode. `nil` when unavailable.
    let reference: String?

    init(momentSeconds: Double? = nil, reference: String? = nil) {
        self.momentSeconds = momentSeconds
        self.reference = reference
    }

    /// The Settings entry: no moment, no reference.
    static let general = ListenerFeedbackContext()

    /// The banner-context entry.
    static func moment(atSeconds seconds: Double, reference: String?) -> Self {
        ListenerFeedbackContext(momentSeconds: seconds, reference: reference)
    }
}

// MARK: - Environment summary

/// The build/device facts the note carries. A projection of
/// `DiagnosticsExportEnvironment` so there is exactly one source of truth
/// for "what version is this?" across both delivery surfaces.
struct ListenerFeedbackEnvironmentSummary: Equatable, Sendable {
    let appVersion: String
    let osVersion: String
    let deviceClass: String

    init(appVersion: String, osVersion: String, deviceClass: String) {
        self.appVersion = appVersion
        self.osVersion = osVersion
        self.deviceClass = deviceClass
    }

    init(_ environment: DiagnosticsExportEnvironment) {
        self.init(
            appVersion: environment.appVersion,
            osVersion: environment.osVersion,
            deviceClass: environment.deviceClass.rawValue
        )
    }
}

// MARK: - Envelope

/// Everything needed to present the note on either surface. Pure value —
/// tests assert on this instead of driving UIKit.
struct ListenerFeedbackEnvelope: Equatable, Sendable {
    let recipients: [String]
    let subject: String
    let body: String
    let attachment: ListenerFeedbackAttachment?

    /// What the share-sheet fallback hands to the activity controller. The
    /// subject would otherwise be lost (a share item has no subject field
    /// that every target honours), and losing it is how a fallback turns
    /// into a dead end for the person on the receiving side.
    var shareText: String {
        "\(subject)\n\(body)"
    }
}

// MARK: - Copy (verbatim, external-copy rule applies)

/// Every user-visible string in the feedback channel. Test-pinned
/// character-for-character in `ListenerFeedbackChannelTests`; do not inline
/// any of these literals into a SwiftUI body.
///
/// External-copy rule (binding): never "ad detection", never "AI", never a
/// quantified counter. The voice sells the felt outcome — the listener does
/// not have to think about this — and the promise is TEN SECONDS, so the
/// prefilled note asks exactly one question and prefills everything else.
enum ListenerFeedbackCopy {

    // MARK: Destination

    /// The single place the destination address is written. Swap here when a
    /// support domain exists; `ListenerFeedbackChannelTests` pins the value
    /// so the change is deliberate rather than incidental.
    static let recipient: String = "d.abrams@icloud.com"

    // MARK: Settings entry

    static let sectionHeader: String = "Feedback"
    static let sendRowLabel: String = "Tell us anything"
    static let attachDiagnosticsToggleLabel: String = "Attach a diagnostics file"
    static let attachDiagnosticsCaption: String =
        "Device and app state only. No audio, no transcripts, nothing about what you listen to."
    static let sectionFooter: String =
        "One line is plenty. Opens Mail — nothing leaves your device until you tap Send."

    // MARK: Banner-context entry

    /// Long-press label on a banner. Deliberately the listener's own words
    /// for the moment, not ours for the mechanism.
    static let bannerMenuLabel: String = "Something felt wrong"

    // MARK: Mail

    static let generalSubject: String = "Playhead feedback"
    static let momentSubject: String = "Playhead — something felt wrong"
    /// The one question. Everything else in the body is prefilled.
    static let bodyPrompt: String = "One line is plenty. What happened, and what you wanted instead?"
    /// Signature-block separator. The listener types ABOVE it; the cursor
    /// lands there by default in every mail client we care about.
    static let bodySeparator: String = "—"
    static let attachmentNote: String = "Attached: a diagnostics file (device and app state only)."

    // MARK: Outcome notices

    /// Shown when Mail is not configured and we routed to the share sheet
    /// instead. Names the address in plain text so a listener who backs out
    /// of the sheet still leaves knowing how to reach us.
    static let shareSheetNotice: String = "Mail isn't set up here. You can still reach us at \(recipient)."
    /// Last-resort notice when neither surface could be presented. Also
    /// names the address, for the same reason.
    static let unavailableNotice: String = "We couldn't open Mail. You can reach us at \(recipient)."
    /// Shown after the listener sends or saves the note.
    static let thanksNotice: String = "Thank you — that helps more than you'd think."
}

// MARK: - Persistence keys

/// The one persisted preference this channel owns: whether the Settings
/// entry attaches a diagnostics bundle. Default `false` (opt-in per send).
enum ListenerFeedbackDefaults {
    static let attachDiagnosticsKey = "Settings.feedback.attachDiagnostics"
    static let attachDiagnosticsDefault = false
}

// MARK: - Composer

/// Pure builders for the feedback flow. No UIKit, no I/O, no clock.
enum ListenerFeedbackComposer {

    /// The delivery branch for a given live `canSendMail()` reading.
    ///
    /// Split out (rather than inlined at the call site) so both branches are
    /// assertable on the simulator, where `canSendMail()` is always false.
    static func delivery(canSendMail: Bool) -> ListenerFeedbackDelivery {
        canSendMail ? .mail : .shareSheet
    }

    /// `m:ss` under an hour, `h:mm:ss` at or above it. Negative and
    /// non-finite inputs clamp to zero rather than rendering garbage into a
    /// user-visible string.
    static func momentStamp(seconds: Double) -> String {
        let clamped = seconds.isFinite ? max(0, seconds) : 0
        let total = Int(clamped.rounded(.down))
        let hours = total / 3600
        let minutes = (total % 3600) / 60
        let secs = total % 60
        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, secs)
        }
        return String(format: "%d:%02d", minutes, secs)
    }

    static func subject(for context: ListenerFeedbackContext) -> String {
        context.momentSeconds == nil
            ? ListenerFeedbackCopy.generalSubject
            : ListenerFeedbackCopy.momentSubject
    }

    /// The prefilled body.
    ///
    /// Shape (blank lines are load-bearing — the composer drops the cursor at
    /// the top, so the listener types into empty space and never has to
    /// delete placeholder text):
    ///
    /// ```
    /// <blank>
    /// <blank>
    /// —
    /// One line is plenty. What happened, and what you wanted instead?
    ///
    /// Moment: 12:34 · ref a1b2c3d4        (banner entry only)
    /// Playhead 1.0.0 · iOS 27.0 · iPhone17Pro
    /// Attached: a diagnostics file …      (opt-in only)
    /// ```
    static func body(
        context: ListenerFeedbackContext,
        environment: ListenerFeedbackEnvironmentSummary,
        hasAttachment: Bool
    ) -> String {
        var details: [String] = []
        if let seconds = context.momentSeconds {
            var line = "Moment: \(momentStamp(seconds: seconds))"
            if let reference = context.reference, !reference.isEmpty {
                line += " · ref \(reference)"
            }
            details.append(line)
        }
        details.append(
            "Playhead \(environment.appVersion) · iOS \(environment.osVersion) · \(environment.deviceClass)"
        )
        if hasAttachment {
            details.append(ListenerFeedbackCopy.attachmentNote)
        }
        return """


        \(ListenerFeedbackCopy.bodySeparator)
        \(ListenerFeedbackCopy.bodyPrompt)

        \(details.joined(separator: "\n"))
        """
    }

    /// Assemble the full envelope.
    static func envelope(
        context: ListenerFeedbackContext,
        environment: ListenerFeedbackEnvironmentSummary,
        attachment: ListenerFeedbackAttachment? = nil
    ) -> ListenerFeedbackEnvelope {
        ListenerFeedbackEnvelope(
            recipients: [ListenerFeedbackCopy.recipient],
            subject: subject(for: context),
            body: body(
                context: context,
                environment: environment,
                hasAttachment: attachment != nil
            ),
            attachment: attachment
        )
    }

    /// The user-visible line to show after a send attempt, or `nil` when the
    /// right thing to say is nothing.
    ///
    /// The two non-obvious cases are the ones that stop this flow dead-ending:
    /// a cancelled SHARE SHEET means Mail was never available, so we name the
    /// address; a `.failed` result on either surface does the same.
    static func notice(for outcome: ListenerFeedbackOutcome) -> String? {
        switch outcome.result {
        case .sent, .saved:
            return ListenerFeedbackCopy.thanksNotice
        case .cancelled:
            return outcome.delivery == .shareSheet ? ListenerFeedbackCopy.shareSheetNotice : nil
        case .failed:
            return ListenerFeedbackCopy.unavailableNotice
        }
    }

    /// Short salted reference token for the banner-context entry: the first
    /// eight hex characters of the SAME `SHA-256(installID || episodeId)`
    /// the diagnostics bundle emits as `episode_id_hash`. Eight characters
    /// is enough to grep a bundle and far too little to invert.
    static func reference(installID: UUID, episodeId: String) -> String {
        String(EpisodeIdHasher.hash(installID: installID, episodeId: episodeId).prefix(8))
    }
}
