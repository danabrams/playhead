// BannerHostDeliveryWiringSourceCanaryTests.swift
// playhead-8cjo: THE LINE THAT MAKES THE RAIL A RAIL.
//
// `AutoSkipCardDeliveryAgainstTheQueueTests` drives `BannerHostDelivery.forward`
// against a real `AdBannerQueue` and a real `SkipOrchestrator`. Every one of
// its assertions stays green if `NowPlayingViewModel.observeBanners` keeps its
// OWN copy of the forwarding rule and never calls that function — the suite
// would then be testing a type production does not use, which is the shape of
// playhead-o89d's six-week gate ("gated and deleted were the same thing") and
// of MS11 in this bead's predecessor (a provider that returns a constant, with
// every orchestrator rail green and the section unable to render).
//
// So this file pins the two lines no behavioural test can see:
//
//   1. the view model DELEGATES rather than duplicating, and
//   2. the forwarding rule acknowledges the AUTO tier, behind the queue's own
//      verdict.
//
// AND ONE ABSENCE, which is a claim as much as the presences are: the
// acknowledgement must not be reachable without `didAccept`. An unconditional
// acknowledgement compiles, passes every orchestrator test, and restores the
// exact defect this bead closes — "a host is attached" becomes "the host was
// handed it", which is still not "the queue took it".

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-8cjo — the queue's verdict reaches the orchestrator")
struct BannerHostDeliveryWiringSourceCanaryTests {

    /// `PlayheadTests/Services/SkipOrchestrator/<this file>` → repo root.
    private static func source(
        _ relative: String,
        filePath: String = #filePath
    ) throws -> String {
        let root = URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()   // SkipOrchestrator
            .deletingLastPathComponent()   // Services
            .deletingLastPathComponent()   // PlayheadTests
            .deletingLastPathComponent()   // repo root
        return try String(
            contentsOf: root.appendingPathComponent(relative),
            encoding: .utf8
        )
    }

    private static let viewModel =
        "Playhead/Views/NowPlaying/NowPlayingViewModel.swift"
    private static let delivery =
        "Playhead/Views/NowPlaying/BannerHostDelivery.swift"

    @Test("observeBanners routes every event through BannerHostDelivery")
    func theViewModelDelegatesToTheDeliveryRule() throws {
        let text = try Self.source(Self.viewModel)
        #expect(
            text.contains("BannerHostDelivery.forward("),
            """
            `observeBanners` does not call `BannerHostDelivery.forward`. \
            Everything `AutoSkipCardDeliveryAgainstTheQueueTests` proves is \
            then a property of a type production never runs.
            """
        )
    }

    /// AND IT KEEPS NO SECOND COPY. Delegating and duplicating are not
    /// mutually exclusive: a view model that calls `forward` for the retire
    /// case and still enqueues presents itself is exactly how the auto tier
    /// came to have no acknowledgement while the suggest tier had one.
    @Test("the view model does not enqueue or acknowledge on its own")
    func theViewModelKeepsNoSecondCopyOfTheRule() throws {
        let text = try Self.source(Self.viewModel)
        for forbidden in [
            "queue.enqueue(",
            "acknowledgeSuggestedBannerDelivery(",
            "acknowledgeAutoSkippedBannerDelivery(",
        ] {
            #expect(
                !text.contains(forbidden),
                """
                `NowPlayingViewModel` still spells `\(forbidden)` itself. There \
                must be exactly one forwarding rule; two call sites that agree \
                today is how one of them stops being maintained.
                """
            )
        }
    }

    @Test("the forwarding rule acknowledges the AUTO tier")
    func theRuleAcknowledgesTheAutoTier() throws {
        let text = try Self.source(Self.delivery)
        for required in [
            "acknowledgeAutoSkippedBannerDelivery(",
            "acknowledgeSuggestedBannerDelivery(",
        ] {
            #expect(
                text.contains(required),
                """
                the forwarding rule never calls `\(required)`. Without it the \
                orchestrator has no way to learn what the queue did, which is \
                the state playhead-8cjo found the auto tier in.
                """
            )
        }
    }

    /// THE ABSENCE. `guard didAccept else { return }` is the whole seam: an
    /// acknowledgement that fires regardless of the queue's answer is a
    /// rewording of the defect, not a fix.
    @Test("no acknowledgement is reachable without the queue's acceptance")
    func theAcknowledgementIsBehindTheQueuesVerdict() throws {
        let text = try Self.source(Self.delivery)
        let guardLine = "guard didAccept else { return }"
        #expect(
            text.contains(guardLine),
            "the forwarding rule does not guard on `didAccept`"
        )
        let acknowledgements = ["acknowledgeSuggestedBannerDelivery(",
                                "acknowledgeAutoSkippedBannerDelivery("]
        guard let guardIndex = text.range(of: guardLine)?.upperBound else {
            Issue.record("`\(guardLine)` is absent; nothing to order against")
            return
        }
        for call in acknowledgements {
            let before = text[text.startIndex..<guardIndex]
            #expect(
                !before.contains(call),
                """
                `\(call)` is reached BEFORE the `didAccept` guard, so an item \
                the queue threw away is still reported as delivered. That is \
                playhead-8cjo with an extra function in the middle.
                """
            )
        }
    }

    /// The tier switch is exhaustive on purpose: a third banner tier must be
    /// forced to choose an acknowledgement rather than silently inheriting
    /// whichever branch was written first.
    @Test("the tier switch is exhaustive — a new tier cannot inherit a seam")
    func theTierSwitchIsExhaustive() throws {
        let text = try Self.source(Self.delivery)
        #expect(text.contains("case .suggest:"))
        #expect(text.contains("case .autoSkipped:"))
        #expect(
            !text.contains("default:"),
            """
            the tier switch has a `default:` arm, so a tier added later takes \
            whichever acknowledgement happens to be first and nothing makes \
            anybody decide. The auto tier's missing seam is what this bead is.
            """
        )
    }
}
