// ListenerFeedbackChannelTests.swift
// playhead-jw63.5 — pins the copy and the two pure decisions behind the
// in-app feedback channel.
//
// Why these are pure functions: `MFMailComposeViewController.canSendMail()`
// is FALSE on the simulator and on any device without a configured account,
// so an availability check written as a direct static call could only ever be
// exercised in one direction here. Modelling the branch as
// `delivery(canSendMail:)` makes BOTH directions real assertions in the
// simulator gate — a test that silently skips is not a test that passes.

import Foundation
import Testing

@testable import Playhead

@Suite("ListenerFeedbackChannel — copy + pure decisions (playhead-jw63.5)")
struct ListenerFeedbackChannelTests {

    private static let environment = ListenerFeedbackEnvironmentSummary(
        appVersion: "1.4.2",
        osVersion: "27.0.1",
        deviceClass: "iPhone17Pro"
    )

    // MARK: - Delivery decision (both directions)

    @Test("canSendMail == true routes to the mail composer")
    func mailWhenAvailable() {
        #expect(ListenerFeedbackComposer.delivery(canSendMail: true) == .mail)
    }

    @Test("canSendMail == false routes to the share sheet, never to nothing")
    func shareSheetWhenUnavailable() {
        #expect(ListenerFeedbackComposer.delivery(canSendMail: false) == .shareSheet)
    }

    @Test("delivery is total — every case is a real surface")
    func deliveryIsTotal() {
        // Guards against a future "unavailable" case being added and left
        // unhandled, which is precisely the dead-end this bead removes.
        #expect(ListenerFeedbackDelivery.allCases.count == 2)
        #expect(ListenerFeedbackDelivery.allCases.contains(.mail))
        #expect(ListenerFeedbackDelivery.allCases.contains(.shareSheet))
    }

    // MARK: - Moment stamp

    @Test("moment stamp renders m:ss under an hour")
    func momentStampMinutes() {
        #expect(ListenerFeedbackComposer.momentStamp(seconds: 0) == "0:00")
        #expect(ListenerFeedbackComposer.momentStamp(seconds: 9) == "0:09")
        #expect(ListenerFeedbackComposer.momentStamp(seconds: 754) == "12:34")
        #expect(ListenerFeedbackComposer.momentStamp(seconds: 3_599) == "59:59")
    }

    @Test("moment stamp renders h:mm:ss at or above an hour")
    func momentStampHours() {
        #expect(ListenerFeedbackComposer.momentStamp(seconds: 3_600) == "1:00:00")
        #expect(ListenerFeedbackComposer.momentStamp(seconds: 3_723) == "1:02:03")
    }

    @Test("moment stamp clamps garbage rather than rendering it to the listener")
    func momentStampClamps() {
        #expect(ListenerFeedbackComposer.momentStamp(seconds: -12) == "0:00")
        #expect(ListenerFeedbackComposer.momentStamp(seconds: .nan) == "0:00")
        #expect(ListenerFeedbackComposer.momentStamp(seconds: .infinity) == "0:00")
    }

    // MARK: - Envelope

    @Test("general envelope is addressed, subject-ed, and version-stamped")
    func generalEnvelope() {
        let envelope = ListenerFeedbackComposer.envelope(
            context: .general,
            environment: Self.environment
        )
        #expect(envelope.recipients == [ListenerFeedbackCopy.recipient])
        #expect(envelope.subject == ListenerFeedbackCopy.generalSubject)
        #expect(envelope.body.contains("Playhead 1.4.2 · iOS 27.0.1 · iPhone17Pro"))
        #expect(envelope.body.contains(ListenerFeedbackCopy.bodyPrompt))
        #expect(envelope.attachment == nil)
        // No moment line on the Settings entry.
        #expect(!envelope.body.contains("Moment:"))
        // The attachment note only appears when a bundle actually rides.
        #expect(!envelope.body.contains(ListenerFeedbackCopy.attachmentNote))
    }

    @Test("body leaves the listener an empty first line to type into")
    func bodyStartsBlank() {
        let envelope = ListenerFeedbackComposer.envelope(
            context: .general,
            environment: Self.environment
        )
        // Composers drop the cursor at the top of the body. Anything other
        // than blank space there is text the listener has to delete first,
        // which is exactly the friction the ten-second promise forbids.
        #expect(envelope.body.hasPrefix("\n\n"))
        let firstLine = envelope.body.split(
            separator: "\n",
            omittingEmptySubsequences: false
        ).first
        #expect(firstLine?.isEmpty == true)
    }

    @Test("moment envelope carries offset and reference, and its own subject")
    func momentEnvelope() {
        let envelope = ListenerFeedbackComposer.envelope(
            context: .moment(atSeconds: 754, reference: "a1b2c3d4"),
            environment: Self.environment
        )
        #expect(envelope.subject == ListenerFeedbackCopy.momentSubject)
        #expect(envelope.body.contains("Moment: 12:34 · ref a1b2c3d4"))
    }

    @Test("moment envelope omits the ref fragment when no reference exists")
    func momentEnvelopeWithoutReference() {
        let envelope = ListenerFeedbackComposer.envelope(
            context: .moment(atSeconds: 30, reference: nil),
            environment: Self.environment
        )
        #expect(envelope.body.contains("Moment: 0:30"))
        #expect(!envelope.body.contains(" · ref "))
    }

    @Test("an attached bundle is disclosed in the body")
    func attachmentDisclosed() {
        let envelope = ListenerFeedbackComposer.envelope(
            context: .general,
            environment: Self.environment,
            attachment: ListenerFeedbackAttachment(
                data: Data("{}".utf8),
                filename: "playhead-diagnostics-x.json"
            )
        )
        #expect(envelope.attachment != nil)
        #expect(envelope.body.contains(ListenerFeedbackCopy.attachmentNote))
        #expect(envelope.attachment?.mimeType == ListenerFeedbackAttachment.defaultMIMEType)
    }

    /// The feedback attachment's MIME type is spelled out locally (the
    /// export service is `@MainActor`-isolated), so pin the two together or
    /// they will drift into a bundle that Mail refuses to render inline.
    @Test("feedback attachment MIME type matches the export service")
    @MainActor
    func mimeTypeMatchesExportService() {
        #expect(
            ListenerFeedbackAttachment.defaultMIMEType
                == DiagnosticsExportService.attachmentMIMEType
        )
    }

    @Test("shareText carries the subject the share sheet would otherwise drop")
    func shareTextCarriesSubject() {
        let envelope = ListenerFeedbackComposer.envelope(
            context: .general,
            environment: Self.environment
        )
        #expect(envelope.shareText.hasPrefix(ListenerFeedbackCopy.generalSubject))
        #expect(envelope.shareText.contains(ListenerFeedbackCopy.bodyPrompt))
        #expect(envelope.shareText.contains("Playhead 1.4.2"))
    }

    // MARK: - Reference token

    @Test("reference is the salted bundle hash prefix, not the raw episode id")
    func referenceIsSaltedHashPrefix() {
        let installID = UUID(uuidString: "8B3D7C4E-1111-2222-3333-444455556666")!
        let episodeId = "episode-guid-42"
        let reference = ListenerFeedbackComposer.reference(
            installID: installID,
            episodeId: episodeId
        )
        let fullHash = EpisodeIdHasher.hash(installID: installID, episodeId: episodeId)
        #expect(reference.count == 8)
        #expect(fullHash.hasPrefix(reference))
        #expect(!reference.contains(episodeId))
        #expect(reference.allSatisfy { $0.isHexDigit && !$0.isUppercase })
    }

    @Test("reference is per-install salted, so two installs never agree")
    func referenceIsPerInstall() {
        let episodeId = "episode-guid-42"
        let first = ListenerFeedbackComposer.reference(
            installID: UUID(uuidString: "00000000-0000-0000-0000-000000000001")!,
            episodeId: episodeId
        )
        let second = ListenerFeedbackComposer.reference(
            installID: UUID(uuidString: "00000000-0000-0000-0000-000000000002")!,
            episodeId: episodeId
        )
        #expect(first != second)
    }

    // MARK: - Notices (the anti-dead-end mapping)

    @Test("a sent or saved note is thanked")
    func sentIsThanked() {
        for result in [DiagnosticsMailComposeResult.sent, .saved] {
            for delivery in ListenerFeedbackDelivery.allCases {
                let outcome = ListenerFeedbackOutcome(
                    delivery: delivery,
                    result: result,
                    attachedDiagnostics: false
                )
                #expect(ListenerFeedbackComposer.notice(for: outcome) == ListenerFeedbackCopy.thanksNotice)
            }
        }
    }

    @Test("a cancelled MAIL composer says nothing — the listener changed their mind")
    func cancelledMailIsSilent() {
        let outcome = ListenerFeedbackOutcome(
            delivery: .mail,
            result: .cancelled,
            attachedDiagnostics: false
        )
        #expect(ListenerFeedbackComposer.notice(for: outcome) == nil)
    }

    @Test("a cancelled SHARE SHEET names the address — that path had no Mail to begin with")
    func cancelledShareSheetNamesAddress() {
        let outcome = ListenerFeedbackOutcome(
            delivery: .shareSheet,
            result: .cancelled,
            attachedDiagnostics: false
        )
        let notice = ListenerFeedbackComposer.notice(for: outcome)
        #expect(notice == ListenerFeedbackCopy.shareSheetNotice)
        #expect(notice?.contains(ListenerFeedbackCopy.recipient) == true)
    }

    @Test("a failed send always names the address on either surface")
    func failureNamesAddress() {
        for delivery in ListenerFeedbackDelivery.allCases {
            let outcome = ListenerFeedbackOutcome(
                delivery: delivery,
                result: .failed,
                attachedDiagnostics: false
            )
            let notice = ListenerFeedbackComposer.notice(for: outcome)
            #expect(notice == ListenerFeedbackCopy.unavailableNotice)
            #expect(notice?.contains(ListenerFeedbackCopy.recipient) == true)
        }
    }

    // MARK: - Copy pinning

    @Test("verbatim copy")
    func copyIsVerbatim() {
        #expect(ListenerFeedbackCopy.recipient == "d.abrams@icloud.com")
        #expect(ListenerFeedbackCopy.sectionHeader == "Feedback")
        #expect(ListenerFeedbackCopy.sendRowLabel == "Tell us anything")
        #expect(ListenerFeedbackCopy.attachDiagnosticsToggleLabel == "Attach a diagnostics file")
        #expect(
            ListenerFeedbackCopy.attachDiagnosticsCaption
                == "Device and app state only. No audio, no transcripts, nothing about what you listen to."
        )
        #expect(
            ListenerFeedbackCopy.sectionFooter
                == "One line is plenty. Opens Mail — nothing leaves your device until you tap Send."
        )
        #expect(ListenerFeedbackCopy.bannerMenuLabel == "Something felt wrong")
        #expect(ListenerFeedbackCopy.generalSubject == "Playhead feedback")
        #expect(ListenerFeedbackCopy.momentSubject == "Playhead — something felt wrong")
        #expect(
            ListenerFeedbackCopy.bodyPrompt
                == "One line is plenty. What happened, and what you wanted instead?"
        )
        #expect(ListenerFeedbackCopy.bodySeparator == "—")
        #expect(
            ListenerFeedbackCopy.attachmentNote
                == "Attached: a diagnostics file (device and app state only)."
        )
        #expect(
            ListenerFeedbackCopy.shareSheetNotice
                == "Mail isn't set up here. You can still reach us at d.abrams@icloud.com."
        )
        #expect(
            ListenerFeedbackCopy.unavailableNotice
                == "We couldn't open Mail. You can reach us at d.abrams@icloud.com."
        )
        #expect(ListenerFeedbackCopy.thanksNotice == "Thank you — that helps more than you'd think.")
    }

    /// Every user-visible string in this channel — including the composed
    /// mail body — is external copy, so the founder's copy rule is binding:
    /// never name the mechanism ("ad detection", "AI"), never quantify the
    /// benefit. Enforced mechanically here rather than by review vigilance.
    @Test("external-copy rule: no mechanism words, no quantified benefit")
    func externalCopyRule() {
        let banned = [
            "ad detection",
            "ad-detection",
            "detection",
            " ai ",
            "artificial intelligence",
            "machine learning",
            "algorithm",
            "time saved",
            "ads skipped",
            "accuracy",
            "confidence"
        ]
        var strings = [
            ListenerFeedbackCopy.sectionHeader,
            ListenerFeedbackCopy.sendRowLabel,
            ListenerFeedbackCopy.attachDiagnosticsToggleLabel,
            ListenerFeedbackCopy.attachDiagnosticsCaption,
            ListenerFeedbackCopy.sectionFooter,
            ListenerFeedbackCopy.bannerMenuLabel,
            ListenerFeedbackCopy.generalSubject,
            ListenerFeedbackCopy.momentSubject,
            ListenerFeedbackCopy.bodyPrompt,
            ListenerFeedbackCopy.attachmentNote,
            ListenerFeedbackCopy.shareSheetNotice,
            ListenerFeedbackCopy.unavailableNotice,
            ListenerFeedbackCopy.thanksNotice
        ]
        strings.append(
            ListenerFeedbackComposer.envelope(
                context: .moment(atSeconds: 754, reference: "a1b2c3d4"),
                environment: Self.environment,
                attachment: ListenerFeedbackAttachment(
                    data: Data(),
                    filename: "playhead-diagnostics-x.json"
                )
            ).shareText
        )

        for string in strings {
            // Pad so a word-boundary term like " ai " cannot be defeated by
            // sitting at the very start or end of the string.
            let haystack = " \(string.lowercased()) "
            for term in banned {
                #expect(
                    !haystack.contains(term),
                    "External copy must not contain \"\(term)\": \(string)"
                )
            }
        }
    }

    @Test("the diagnostics attachment is opt-in by default")
    func attachmentDefaultsOff() {
        #expect(ListenerFeedbackDefaults.attachDiagnosticsDefault == false)
        #expect(ListenerFeedbackDefaults.attachDiagnosticsKey == "Settings.feedback.attachDiagnostics")
    }
}
