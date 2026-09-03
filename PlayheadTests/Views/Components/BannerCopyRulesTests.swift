import Foundation
import Testing
@testable import Playhead

/// playhead-1mq1.1 — the banner's copy rules, enforced over every branch
/// rather than trusted to review.
///
/// The strings in `feedbackChoiceContent` are the only words most listeners
/// will ever read about what this app does. Three of the rules below are Dan's
/// standing product constraints and have been violated by drafts before: the
/// bead's own recommendation text proposed "Ad detected — [Skip]", which its
/// own copy principle bans in the paragraph above it.
///
/// The fourth rule is a correctness rule, not a style one. On a span whose
/// edges the detector drew and cannot prove late-safe, confirming MARKS and
/// playback does not move (playhead-ynmk). Copy that says "skip" there is a
/// button that does nothing at the moment the listener most wants it to act.
@MainActor
@Suite("playhead-1mq1.1: banner copy rules")
struct BannerCopyRulesTests {

    /// Every branch of the feedback row, with a name for failure messages.
    private static let branches: [(name: String, content: AdBannerView.FeedbackChoiceContent)] = [
        (
            "suggest/skippable",
            AdBannerView.feedbackChoiceContent(
                for: .suggest, confirmationSkipsPlayback: true
            )
        ),
        (
            "suggest/mark-only",
            AdBannerView.feedbackChoiceContent(
                for: .suggest, confirmationSkipsPlayback: false
            )
        ),
        (
            "autoSkipped",
            AdBannerView.feedbackChoiceContent(for: .autoSkipped)
        ),
    ]

    private static func strings(
        _ content: AdBannerView.FeedbackChoiceContent
    ) -> [(field: String, value: String)] {
        [
            ("prompt", content.prompt),
            ("confirmLabel", content.confirmLabel),
            ("denyLabel", content.denyLabel),
            ("confirmAccessibilityLabel", content.confirmAccessibilityLabel),
            ("confirmAccessibilityHint", content.confirmAccessibilityHint),
            ("denyAccessibilityLabel", content.denyAccessibilityLabel),
            ("denyAccessibilityHint", content.denyAccessibilityHint),
        ]
    }

    // MARK: - Anti-vacuity

    /// Every rule below scans `branches`. If that collection were empty, or if
    /// two entries collapsed to the same content, the whole suite would pass
    /// while checking almost nothing.
    @Test("the corpus this suite scans is real and distinct", .timeLimit(.minutes(1)))
    func corpusIsRealAndDistinct() {
        #expect(Self.branches.count == 3)
        var confirmLabels: Set<String> = []
        for (_, content) in Self.branches { confirmLabels.insert(content.confirmLabel) }
        #expect(
            confirmLabels.count == 3,
            """
            three branches must offer three different confirm labels, or the \
            per-branch rules below are testing one string three times
            """
        )
        for (name, content) in Self.branches {
            let fields = Self.strings(content)
            #expect(fields.count == 7, "\(name): the content shape changed")
            for (field, value) in fields {
                #expect(
                    !value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
                    "\(name).\(field) is empty"
                )
            }
        }
    }

    // MARK: - The product rules

    @Test("no jargon reaches a listener", .timeLimit(.minutes(1)))
    func noJargonReachesAListener() {
        // Dan 2026-07-24: external copy never says "ad detection" or "AI".
        // "detected"/"detection" name the MECHANISM; a listener is being told
        // what the app is doing to them rather than what is happening.
        let banned = [
            "ai ", " ai", "artificial intelligence",
            "detect", "algorithm", "model", "confidence", "probability",
        ]
        for (name, content) in Self.branches {
            for (field, value) in Self.strings(content) {
                let lowered = " " + value.lowercased() + " "
                for word in banned {
                    #expect(
                        !lowered.contains(word),
                        "\(name).\(field) says \"\(word)\": \(value)"
                    )
                }
            }
        }
    }

    @Test("no numbers, no counters", .timeLimit(.minutes(1)))
    func noNumbersNoCounters() {
        // feedback_peace_of_mind_not_metrics: the card should feel like the app
        // quietly handling something, not reporting a measurement.
        for (name, content) in Self.branches {
            for (field, value) in Self.strings(content) {
                var hasNumber = false
                for character in value where character.isNumber { hasNumber = true }
                #expect(
                    !hasNumber,
                    "\(name).\(field) carries a number: \(value)"
                )
            }
        }
    }

    // MARK: - The correctness rule

    @Test(
        "a card that cannot skip never says it will",
        .timeLimit(.minutes(1))
    )
    func markOnlyCardNeverPromisesASkip() {
        let markOnly = AdBannerView.feedbackChoiceContent(
            for: .suggest, confirmationSkipsPlayback: false
        )
        // Only the CONFIRM side is constrained: it is the tap that would move
        // playback if it could. The deny side may still speak of playback,
        // because "leaves playback unchanged" is true and is worth saying.
        for (field, value) in [
            ("confirmLabel", markOnly.confirmLabel),
            ("confirmAccessibilityLabel", markOnly.confirmAccessibilityLabel),
            ("confirmAccessibilityHint", markOnly.confirmAccessibilityHint),
        ] {
            #expect(
                !value.lowercased().contains("skip"),
                "mark-only \(field) promises a skip this branch cannot perform: \(value)"
            )
        }
    }

    @Test(
        "a card that CAN skip leads with the skip",
        .timeLimit(.minutes(1))
    )
    func skippableCardLeadsWithTheSkip() {
        let skippable = AdBannerView.feedbackChoiceContent(
            for: .suggest, confirmationSkipsPlayback: true
        )
        #expect(
            skippable.confirmLabel.lowercased().contains("skip"),
            "the visible button is the whole affordance; it must name the action"
        )
        #expect(
            skippable.confirmAccessibilityHint.lowercased().contains("skip"),
            "VoiceOver must hear the same promise the button makes"
        )
    }

    // MARK: - One gesture the listener learns once

    @Test("the negative is the same phrase everywhere", .timeLimit(.minutes(1)))
    func theNegativeIsOnePhrase() {
        var denials: Set<String> = []
        for (_, content) in Self.branches { denials.insert(content.denyLabel) }
        #expect(
            denials == [AdBannerView.denyFeedbackLabel],
            "every tier must spell the correction the same way; got \(denials)"
        )
        #expect(
            !AdBannerView.denyFeedbackLabel.lowercased().hasPrefix("no,"),
            """
            the negative must name what is being corrected, not answer a \
            question the prospective card no longer asks
            """
        )
    }

    // MARK: - Tense

    @Test(
        "only the retrospective card asks whether it was right",
        .timeLimit(.minutes(1))
    )
    func onlyTheRetrospectiveCardAsksAboutThePast() {
        // playhead-d3g0 moved the suggest card to fire on ENTRY, so its prompt
        // would be asking about audio that has not played.
        for confirmationSkipsPlayback in [true, false] {
            let suggest = AdBannerView.feedbackChoiceContent(
                for: .suggest,
                confirmationSkipsPlayback: confirmationSkipsPlayback
            )
            #expect(
                suggest.prompt != AdBannerView.feedbackPrompt,
                "the prospective card must not reuse the retrospective prompt"
            )
            #expect(
                !suggest.prompt.lowercased().contains("was "),
                "the suggest card asks about audio the listener has not heard"
            )
        }
        #expect(
            AdBannerView.feedbackChoiceContent(for: .autoSkipped).prompt
                == AdBannerView.feedbackPrompt,
            "a skip that already happened is a fair thing to ask about"
        )
    }
}
