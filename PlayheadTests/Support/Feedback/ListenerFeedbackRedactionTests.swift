// ListenerFeedbackRedactionTests.swift
// playhead-jw63.5 — the mail body is a NEW off-device surface, so it is held
// to the same bar as the diagnostics bundle: never audio, never transcripts,
// never episode content.
//
// The bundle itself is unchanged by this bead — `DiagnosticsBundle*` gains no
// field, so the existing checklist coverage (a)–(e) still holds verbatim.
// What is new is the prose the listener sends alongside it, and prose is
// exactly where a helpful-looking `\(episode.title)` interpolation would land
// if nobody were watching. This suite is the watcher: it seeds every sentinel
// the jw63.4 scrubbing tests use and sweeps the composed envelope for them.
//
// Companion to `PlayheadTests/Support/Diagnostics/StabilityDiagnosticScrubbingTests.swift`
// and `PlayheadTests/E2E/Privacy/DiagnosticExportRedactionTests.swift`.

import Foundation
import Testing

@testable import Playhead

@Suite("ListenerFeedback — the note carries no episode content (playhead-jw63.5)")
struct ListenerFeedbackRedactionTests {

    /// Everything the mandate forbids, in the shapes it would actually take.
    private static let sentinels = [
        "The Diary Of A CEO with Steven Bartlett",       // episode / show title
        "and that's when I realised the whole thing",    // transcript text
        "https://feeds.simplecast.com/54nAGcIl",         // feed URL
        "/var/mobile/Containers/Data/Application",       // container path
        "Squarespace",                                   // advertiser
        "episode-guid-42"                                // raw episode id
    ]

    private static let environment = ListenerFeedbackEnvironmentSummary(
        appVersion: "1.4.2",
        osVersion: "27.0.1",
        deviceClass: "iPhone17Pro"
    )

    @Test("no sentinel survives into a banner-context note")
    func momentNoteCarriesNoSentinels() {
        let installID = UUID(uuidString: "8B3D7C4E-1111-2222-3333-444455556666")!
        let reference = ListenerFeedbackComposer.reference(
            installID: installID,
            episodeId: "episode-guid-42"
        )
        let envelope = ListenerFeedbackComposer.envelope(
            context: .moment(atSeconds: 754, reference: reference),
            environment: Self.environment,
            attachment: ListenerFeedbackAttachment(
                data: Data(),
                filename: "playhead-diagnostics-2026-07-28T00-00-00Z.json"
            )
        )

        // Sweep the ENTIRE envelope, not just the body: subject, recipients,
        // filename and shareText are all things that leave the device.
        let surface = [
            envelope.subject,
            envelope.body,
            envelope.shareText,
            envelope.recipients.joined(separator: " "),
            envelope.attachment?.filename ?? ""
        ].joined(separator: "\n")

        for sentinel in Self.sentinels {
            #expect(
                !surface.contains(sentinel),
                "Listener feedback envelope leaked sentinel: \(sentinel)"
            )
        }
        // The test would be vacuous if the envelope were empty.
        #expect(surface.contains("Moment: 12:34"))
        #expect(surface.contains(reference))
    }

    @Test("the only episode-derived token is the salted hash prefix")
    func onlyReferenceIsEpisodeDerived() {
        let installID = UUID(uuidString: "8B3D7C4E-1111-2222-3333-444455556666")!
        let episodeId = "episode-guid-42"
        let reference = ListenerFeedbackComposer.reference(
            installID: installID,
            episodeId: episodeId
        )
        let body = ListenerFeedbackComposer.body(
            context: .moment(atSeconds: 754, reference: reference),
            environment: Self.environment,
            hasAttachment: false
        )

        #expect(!body.contains(episodeId))
        // The reference is a truncation of the SAME hash the bundle emits as
        // `episode_id_hash`, which is what lets a note and a bundle be
        // correlated without either naming the episode.
        #expect(EpisodeIdHasher.hash(installID: installID, episodeId: episodeId).hasPrefix(reference))
    }

    @Test("the body contains no locators — no URL, no path, no address")
    func bodyContainsNoLocators() {
        let body = ListenerFeedbackComposer.body(
            context: .moment(atSeconds: 3_723, reference: "a1b2c3d4"),
            environment: Self.environment,
            hasAttachment: true
        )
        for forbidden in ["://", "http", "/var/", "/Users/", "@"] {
            #expect(
                !body.contains(forbidden),
                "Prefilled body must not contain \"\(forbidden)\": \(body)"
            )
        }
    }

    @Test("the body carries only build/device facts the bundle already reports")
    func bodyCarriesOnlyKnownFacts() {
        let body = ListenerFeedbackComposer.body(
            context: .general,
            environment: Self.environment,
            hasAttachment: false
        )
        // The full set of interpolated values, spelled out. Anything else
        // appearing here later is a deliberate edit made against this list.
        #expect(body.contains("1.4.2"))
        #expect(body.contains("27.0.1"))
        #expect(body.contains("iPhone17Pro"))
        let lines = body
            .split(separator: "\n", omittingEmptySubsequences: true)
            .map(String.init)
        // Separator, prompt, and exactly one details line.
        #expect(lines.count == 3)
        #expect(lines[0] == ListenerFeedbackCopy.bodySeparator)
        #expect(lines[1] == ListenerFeedbackCopy.bodyPrompt)
        #expect(lines[2] == "Playhead 1.4.2 · iOS 27.0.1 · iPhone17Pro")
    }

    @Test("the diagnostics bundle shape is untouched by this bead")
    func bundleShapeUnchanged() {
        // The attachment is opaque bytes by type: the feedback layer cannot
        // add a field to the bundle even by accident, because it never
        // constructs one. `DiagnosticsBundleBuilder` remains the only
        // producer, and the checklist coverage for (a)–(e) still applies to
        // its output verbatim.
        let attachment = ListenerFeedbackAttachment(
            data: Data("opaque".utf8),
            filename: "playhead-diagnostics-x.json"
        )
        #expect(attachment.data == Data("opaque".utf8))
        #expect(attachment.mimeType == ListenerFeedbackAttachment.defaultMIMEType)
    }
}
