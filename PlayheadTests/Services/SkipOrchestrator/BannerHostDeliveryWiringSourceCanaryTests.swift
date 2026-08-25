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

    /// `observeBanners`' OWN body, brace-balanced from its `func` keyword.
    ///
    /// SCOPED RATHER THAN FILE-WIDE, and both reasons were found by review with
    /// working code rather than argued for:
    ///
    ///   * a file-wide ban on `queue.enqueue(` is defeated by renaming the
    ///     parameter — `bannerQueue.enqueue(` contains `Queue.enqueue(` with a
    ///     CAPITAL Q, so the lowercase substring does not match. One character
    ///     of case is the entire bypass, and the auto tier is then never
    ///     acknowledged while every behavioural suite stays green;
    ///   * a file-wide REQUIREMENT of `BannerHostDelivery.forward(` is satisfied
    ///     by a doc comment naming it. This file's own doc comment does.
    ///
    /// Measured on the real file in both states before this was written: the
    /// scan extracts a ~625-character body at HEAD (containing the forward call
    /// and none of the forbidden spellings) and under mutation AK11 (containing
    /// the inline enqueue and no forward call). At HEAD the FILE contains both
    /// `.enqueue(` and `acknowledge` outside this function, so a file-wide ban
    /// would have been red on correct code from the first commit.
    ///
    /// LIMIT, stated: it counts braces, so a brace inside a string literal or a
    /// comment in that body would mis-scope it. There are none today, and the
    /// failure is loud — an unbalanced scan returns nil and the test fails.
    private static func observeBannersBody(_ text: String) -> Substring? {
        guard let start = text.range(of: "func observeBanners(")?.lowerBound
        else {
            return nil
        }
        var depth = 0
        var opened = false
        var index = start
        while index < text.endIndex {
            switch text[index] {
            case "{":
                depth += 1
                opened = true
            case "}":
                depth -= 1
                if opened, depth == 0 { return text[start...index] }
            default:
                break
            }
            index = text.index(after: index)
        }
        return nil
    }

    @Test("observeBanners routes every event through BannerHostDelivery")
    func theViewModelDelegatesToTheDeliveryRule() throws {
        let text = try Self.source(Self.viewModel)
        let body = try #require(
            Self.observeBannersBody(text),
            "could not find a brace-balanced `observeBanners` body to scope to"
        )
        #expect(
            body.contains("BannerHostDelivery.forward("),
            """
            `observeBanners`' BODY does not call `BannerHostDelivery.forward`. \
            Everything `AutoSkipCardDeliveryAgainstTheQueueTests` proves is then \
            a property of a type production never runs. Scoped to the body \
            because a doc comment naming the function satisfies a file-wide \
            check — this file has such a comment.
            """
        )
    }

    /// AND IT KEEPS NO SECOND COPY. Delegating and duplicating are not mutually
    /// exclusive: a view model that calls `forward` for retirements and still
    /// enqueues presentations itself is exactly how the auto tier came to have
    /// no acknowledgement while the suggest tier had one — and it passes a
    /// file-wide positive check.
    @Test("the view model does not enqueue or acknowledge on its own")
    func theViewModelKeepsNoSecondCopyOfTheRule() throws {
        let text = try Self.source(Self.viewModel)
        let body = try #require(Self.observeBannersBody(text))
        for forbidden in [".enqueue(", ".retireWindow(", "acknowledge"] {
            #expect(
                !body.contains(forbidden),
                """
                `observeBanners`' body spells `\(forbidden)` itself. There must \
                be exactly one forwarding rule; two call sites that agree today \
                is how one of them stops being maintained. The check is on the \
                SELECTOR rather than on `queue.…` because renaming the parameter \
                defeats the latter — that exact bypass was written out at review \
                and is re-created verbatim by mutation AK17.
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
        guard let guardIndex = text.range(of: guardLine)?.upperBound else {
            Issue.record("`\(guardLine)` is absent; nothing to order against")
            return
        }
        let before = text[text.startIndex..<guardIndex]
        for call in [
            "acknowledgeSuggestedBannerDelivery(",
            "acknowledgeAutoSkippedBannerDelivery(",
        ] {
            #expect(
                !before.contains(call),
                """
                `\(call)` is reached BEFORE the `didAccept` guard, so an item \
                the queue threw away is still reported as delivered. That is \
                playhead-8cjo with an extra function in the middle.
                """
            )
        }
        // AND IT IS DECIDED BEFORE THE TIER IS, which "no call above the
        // guard" does not say. Push the guard down into one arm of the tier
        // switch and the other arm acknowledges unconditionally while every
        // ordering check above still passes — the queue's verdict has to bind
        // both tiers or it binds whichever one the next reader forgets.
        guard let switchIndex = text.range(of: "switch item.tier {")?.lowerBound
        else {
            Issue.record("the tier switch is gone; nothing to order against")
            return
        }
        #expect(
            guardIndex < switchIndex,
            """
            the `didAccept` guard is inside the tier switch rather than ahead \
            of it, so at least one tier acknowledges an item the queue refused.
            """
        )
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
