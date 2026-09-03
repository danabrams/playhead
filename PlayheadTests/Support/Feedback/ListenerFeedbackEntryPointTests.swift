// ListenerFeedbackEntryPointTests.swift
// playhead-jw63.5 — the two entry points, and the rule that keeps the
// banner-context one from competing with playhead-jw63.1's one-tap Yes/No.
//
// Also pins the share-sheet fallback's exclusion list, which is where the
// "must not dead-end" requirement becomes a concrete behaviour: the
// diagnostics export is mail-only by spec, but the FEEDBACK fallback keeps
// Copy / Messages / AirDrop available, because a listener with no mail
// account needs some route and any route beats none.

import Foundation
import Testing
import UIKit

@testable import Playhead

@Suite("ListenerFeedback entry points (playhead-jw63.5)")
@MainActor
struct ListenerFeedbackEntryPointTests {

    // MARK: - Banner-context entry vs. the one-tap Yes/No (playhead-jw63.1)

    @Test("the banner affordance attaches only when a handler is wired")
    func affordanceRequiresHandler() {
        #expect(AdBannerView.showsFeedbackChannelAffordance(hasHandler: true))
        #expect(!AdBannerView.showsFeedbackChannelAffordance(hasHandler: false))
    }

    @Test("the banner affordance is independent of tier and of the Yes/No copy")
    func affordanceDoesNotCompeteWithOneTapFeedback() {
        // Coexistence rule: the long-press entry depends ONLY on whether a
        // handler is wired. It cannot be gated by tier, by the feedback
        // claim flags, or by the presence of the Yes/No row — so it can
        // neither suppress nor be suppressed by the one-tap choice that
        // playhead-jw63.1 exists to collect.
        for tier in [AdBannerTier.autoSkipped, .suggest] {
            let content = AdBannerView.feedbackChoiceContent(for: tier)
            // The one-tap row keeps its own copy, untouched by this bead.
            //
            // playhead-1mq1.1 made that copy TIER-SPECIFIC: the suggest card
            // fires before its audio has played and cannot ask "was this
            // right?". This suite's claim is about coexistence with the
            // long-press affordance, not about which words each tier uses, so
            // it asserts each tier against its own constants.
            let expectedPrompt = tier == .suggest
                ? AdBannerView.suggestFeedbackPrompt
                : AdBannerView.feedbackPrompt
            let expectedConfirm = tier == .suggest
                ? AdBannerView.skipConfirmFeedbackLabel
                : AdBannerView.confirmFeedbackLabel
            #expect(content.prompt == expectedPrompt)
            #expect(content.confirmLabel == expectedConfirm)
            #expect(content.denyLabel == AdBannerView.denyFeedbackLabel)
            // And the feedback-channel label is a different string entirely,
            // so neither can be mistaken for the other on the card.
            #expect(content.confirmLabel != ListenerFeedbackCopy.bannerMenuLabel)
            #expect(content.denyLabel != ListenerFeedbackCopy.bannerMenuLabel)
            #expect(content.prompt != ListenerFeedbackCopy.bannerMenuLabel)
        }
    }

    @Test("the long-press label names the moment, not the mechanism")
    func bannerLabelVoice() {
        #expect(ListenerFeedbackCopy.bannerMenuLabel == "Something felt wrong")
    }

    // MARK: - Share-sheet fallback exclusions

    @Test("the feedback fallback keeps every route that can carry text")
    func feedbackFallbackKeepsUsableRoutes() {
        let excluded = Set(DiagnosticsExportService.feedbackFallbackExcludedActivities)
        // These are the escape hatches for a listener with no Mail account.
        // Excluding any of them is how this flow dead-ends.
        #expect(!excluded.contains(.copyToPasteboard))
        #expect(!excluded.contains(.message))
        #expect(!excluded.contains(.airDrop))
    }

    @Test("the feedback fallback drops only targets that cannot carry a note")
    func feedbackFallbackDropsNonsensicalTargets() {
        let excluded = Set(DiagnosticsExportService.feedbackFallbackExcludedActivities)
        #expect(excluded == Set([
            .addToReadingList,
            .assignToContact,
            .print,
            .saveToCameraRoll
        ]))
    }

    @Test("the diagnostics export fallback stays mail-only — this bead changed nothing there")
    func diagnosticsFallbackStillMailOnly() {
        let excluded = Set(DiagnosticsExportService.mailOnlyFallbackExcludedActivities)
        #expect(excluded.contains(.copyToPasteboard))
        #expect(excluded.contains(.message))
        #expect(excluded.contains(.airDrop))
    }

    @Test("the feedback fallback is strictly more permissive than the diagnostics one")
    func feedbackFallbackIsMorePermissive() {
        let feedback = Set(DiagnosticsExportService.feedbackFallbackExcludedActivities)
        let diagnostics = Set(DiagnosticsExportService.mailOnlyFallbackExcludedActivities)
        #expect(feedback.isSubset(of: diagnostics))
        #expect(feedback != diagnostics)
    }

    // MARK: - Fallback controller construction

    @Test("the fallback controller always carries the note, with or without a bundle")
    func fallbackControllerCarriesText() {
        let envelope = ListenerFeedbackComposer.envelope(
            context: .general,
            environment: ListenerFeedbackEnvironmentSummary(
                appVersion: "1.4.2",
                osVersion: "27.0.1",
                deviceClass: "iPhone17Pro"
            )
        )
        let withoutFile = DiagnosticsExportService.makeFeedbackFallback(
            text: envelope.shareText,
            fileURL: nil
        )
        let withFile = DiagnosticsExportService.makeFeedbackFallback(
            text: envelope.shareText,
            fileURL: URL(fileURLWithPath: "/tmp/playhead-diagnostics-x.json")
        )
        // Constructing both must succeed and must apply the permissive
        // exclusion list; a nil file URL is the common case (no opt-in) and
        // must not degrade the sheet.
        #expect(withoutFile.excludedActivityTypes.map(Set.init)
            == Set(DiagnosticsExportService.feedbackFallbackExcludedActivities))
        #expect(withFile.excludedActivityTypes.map(Set.init)
            == Set(DiagnosticsExportService.feedbackFallbackExcludedActivities))
    }
}
