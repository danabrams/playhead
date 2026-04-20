// DownloadNextViewTests.swift
// Verifies acceptance criteria for playhead-hkg8:
// - Verbatim copy for each of the 5 acceptance states
//   (default / over-cap / Flight / Commute / Workout)
// - Stepper default is 3, option set is {1, 3, 5, 10, 25}
// - Generic "for …" selection hides the trailing phrase entirely
// - Over-cap decision flips to amber + "Free up space →" CTA
// - Pure size estimator produces deterministic MB/GB numbers
//
// These tests operate on the pure copy/logic layer
// (`DownloadNextCopy`, `DownloadNextStepper`, `DownloadSizeEstimator`,
// `DownloadNextCapFit`) rather than instantiating a SwiftUI host —
// verbatim strings are the contract and the view body composes them.

import XCTest
@testable import Playhead

// MARK: - Verbatim copy (snapshot-style, 5 acceptance states)

/// Acceptance-gate snapshot tests for every user-visible string the
/// "Download Next N" affordance renders. Any drift here is a product
/// decision — update bd playhead-hkg8 and the UI design doc together.
final class DownloadNextCopyTests: XCTestCase {

    // MARK: State 1 — default (3 episodes, Generic, within cap)

    func testDefaultState_buttonLabel() {
        XCTAssertEqual(
            DownloadNextCopy.buttonLabel(count: 3),
            "Download next 3 episodes",
            "Default button label must read 'Download next 3 episodes'"
        )
    }

    func testDefaultState_withinCapSummary() {
        // Spec example: "Downloading 3 episodes (~640 MB). Will fit in your 10 GB cap."
        XCTAssertEqual(
            DownloadNextCopy.withinCapSummary(count: 3, estimatedMB: 640, capGB: 10),
            "Downloading 3 episodes (~640 MB). Will fit in your 10 GB cap."
        )
    }

    func testDefaultState_genericHidesTrailingPhrase() {
        // Acceptance: Generic is default and hides the phrase entirely.
        // No "for Generic" text must ever render.
        XCTAssertNil(
            DownloadNextCopy.forContextPhrase(.generic),
            "Generic must hide the 'for …' phrase entirely (no 'for Generic' text)"
        )
    }

    func testDefaultState_systemPromiseVerbatim() {
        XCTAssertEqual(
            DownloadNextCopy.systemPromise,
            "Media will be downloaded as fast as transport allows; analysis runs on each as capacity allows. No promise about full skip-readiness by any time.",
            "System-promise copy must match bd playhead-hkg8 verbatim"
        )
    }

    // MARK: State 2 — over-cap (amber + Free up space CTA)

    func testOverCapState_leadCopy() {
        XCTAssertEqual(
            DownloadNextCopy.overCapLead,
            "Not enough space",
            "Over-cap lead must be 'Not enough space' (no em-dash suffix; view renders ' —' separately)"
        )
    }

    func testOverCapState_freeUpSpaceCTAUsesRightwardsArrow() {
        // U+2192 RIGHTWARDS ARROW, not "->" or an em-dash.
        XCTAssertEqual(
            DownloadNextCopy.freeUpSpaceCTA,
            "Free up space \u{2192}"
        )
        XCTAssertTrue(
            DownloadNextCopy.freeUpSpaceCTA.hasSuffix("\u{2192}"),
            "CTA must end with the RIGHTWARDS ARROW glyph (U+2192)"
        )
    }

    // MARK: State 3 — "for Flight"

    func testForPickerState_flight() {
        XCTAssertEqual(
            DownloadNextCopy.forContextPhrase(.flight),
            "for Flight",
            "Flight trip context must render as 'for Flight'"
        )
    }

    // MARK: State 4 — "for Commute"

    func testForPickerState_commute() {
        XCTAssertEqual(
            DownloadNextCopy.forContextPhrase(.commute),
            "for Commute",
            "Commute trip context must render as 'for Commute'"
        )
    }

    // MARK: State 5 — "for Workout"

    func testForPickerState_workout() {
        XCTAssertEqual(
            DownloadNextCopy.forContextPhrase(.workout),
            "for Workout",
            "Workout trip context must render as 'for Workout'"
        )
    }
}

// MARK: - Stepper contract

final class DownloadNextStepperTests: XCTestCase {

    func testStepperDefaultIsThree() {
        XCTAssertEqual(
            DownloadNextStepper.defaultCount,
            3,
            "Default stepper value must be 3 per UI design §D"
        )
    }

    func testStepperOptionsMatchSpec() {
        XCTAssertEqual(
            DownloadNextStepper.options,
            [1, 3, 5, 10, 25],
            "Stepper must render 1 / 3 / 5 / 10 / 25 (no 'All unplayed' in v1)"
        )
    }

    func testStepperDefaultButtonLabelRendersWithPluralS() {
        // Belt-and-suspenders against a regression that'd render
        // "Download next 3 episode" (missing plural 's').
        XCTAssertEqual(
            DownloadNextCopy.buttonLabel(count: DownloadNextStepper.defaultCount),
            "Download next 3 episodes"
        )
    }

    func testStepperSingularCountUsesSingularNoun() {
        XCTAssertEqual(
            DownloadNextCopy.buttonLabel(count: 1),
            "Download next 1 episode",
            "Count of 1 must render without plural 's'"
        )
    }

    func testStepperEachOptionRendersCorrectButtonLabel() {
        // Every option in the stepper must produce a grammatically
        // correct button label. Prevents a future code change from
        // breaking, e.g., count=25 rendering as "25 episode".
        for option in DownloadNextStepper.options {
            let label = DownloadNextCopy.buttonLabel(count: option)
            if option == 1 {
                XCTAssertEqual(label, "Download next 1 episode")
            } else {
                XCTAssertEqual(label, "Download next \(option) episodes")
            }
        }
    }
}

// MARK: - Size estimator (pure)

final class DownloadSizeEstimatorTests: XCTestCase {

    private func makeEpisode(duration: TimeInterval?) -> Episode {
        Episode(
            feedItemGUID: UUID().uuidString,
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Test",
            audioURL: URL(string: "https://example.com/ep.mp3")!,
            duration: duration
        )
    }

    func testZeroCountReturnsZero() {
        let eps = [makeEpisode(duration: 3600)]
        XCTAssertEqual(DownloadSizeEstimator.estimatedBytes(for: eps, count: 0), 0)
    }

    func testNegativeCountReturnsZero() {
        let eps = [makeEpisode(duration: 3600)]
        XCTAssertEqual(DownloadSizeEstimator.estimatedBytes(for: eps, count: -5), 0)
    }

    func testOneHourEpisodeUses128kbpsHeuristic() {
        // 60 min × 16_000 B/s = 57_600_000 B = ~58 MB.
        let eps = [makeEpisode(duration: 60 * 60)]
        let bytes = DownloadSizeEstimator.estimatedBytes(for: eps, count: 1)
        XCTAssertEqual(bytes, 57_600_000)
        XCTAssertEqual(DownloadSizeEstimator.wholeMegabytes(bytes), 58)
    }

    func testMissingDurationFallsBackToSixtyMinutes() {
        // nil duration should yield the same estimate as a 60-minute
        // episode — 57.6 MB.
        let eps = [makeEpisode(duration: nil)]
        let bytes = DownloadSizeEstimator.estimatedBytes(for: eps, count: 1)
        XCTAssertEqual(bytes, 57_600_000)
    }

    func testCountIsClampedToAvailableEpisodes() {
        // Only 2 episodes available; asking for 10 must sum only 2.
        let eps = [
            makeEpisode(duration: 60 * 60),
            makeEpisode(duration: 60 * 60),
        ]
        let bytes = DownloadSizeEstimator.estimatedBytes(for: eps, count: 10)
        XCTAssertEqual(bytes, 2 * 57_600_000)
    }

    func testWholeGigabytesFromDefaultCap() {
        // 10 GB SI = 10 * 10^9 bytes → renders "10 GB" in the summary.
        XCTAssertEqual(
            DownloadSizeEstimator.wholeGigabytes(10 * 1_000_000_000),
            10
        )
    }

    func testWholeMegabytesRoundsToNearestInteger() {
        // 57.6 MB rounds to 58.
        XCTAssertEqual(DownloadSizeEstimator.wholeMegabytes(57_600_000), 58)
        // 57.4 MB rounds to 57.
        XCTAssertEqual(DownloadSizeEstimator.wholeMegabytes(57_400_000), 57)
    }
}

// MARK: - Cap-fit decision (pure)

final class DownloadNextCapFitTests: XCTestCase {

    func testWithinCap_noCurrentUsage() {
        let decision = DownloadNextCapFit.decide(
            estimatedBytes: 500_000_000,  // 500 MB
            capBytes: 10_000_000_000       // 10 GB
        )
        XCTAssertEqual(
            decision,
            .withinCap(estimatedBytes: 500_000_000, capBytes: 10_000_000_000)
        )
    }

    func testOverCap_estimatedAloneExceedsCap() {
        let decision = DownloadNextCapFit.decide(
            estimatedBytes: 11_000_000_000,  // 11 GB
            capBytes: 10_000_000_000          // 10 GB
        )
        XCTAssertEqual(
            decision,
            .overCap(estimatedBytes: 11_000_000_000, capBytes: 10_000_000_000)
        )
    }

    func testOverCap_currentUsagePushesOver() {
        // 9 GB already used + 2 GB proposed = 11 GB > 10 GB.
        let decision = DownloadNextCapFit.decide(
            estimatedBytes: 2_000_000_000,
            capBytes: 10_000_000_000,
            currentUsedBytes: 9_000_000_000
        )
        if case .overCap = decision {
            // expected
        } else {
            XCTFail("Expected over-cap when used + estimated exceeds cap")
        }
    }

    func testBoundary_exactlyAtCapIsWithinCap() {
        // Legal admission: estimated + used == cap exactly.
        let decision = DownloadNextCapFit.decide(
            estimatedBytes: 1_000_000_000,
            capBytes: 10_000_000_000,
            currentUsedBytes: 9_000_000_000
        )
        XCTAssertEqual(
            decision,
            .withinCap(estimatedBytes: 1_000_000_000, capBytes: 10_000_000_000)
        )
    }

    func testOverflow_treatedAsOverCap() {
        // used + estimated would overflow Int64 — must fall into overCap
        // branch rather than wrapping negative.
        let decision = DownloadNextCapFit.decide(
            estimatedBytes: Int64.max,
            capBytes: 10_000_000_000,
            currentUsedBytes: Int64.max - 1
        )
        if case .overCap = decision {
            // expected
        } else {
            XCTFail("Overflow must resolve to over-cap, not silent wrap")
        }
    }
}

// MARK: - Default cap alignment

/// Pins the MB/GB formatter output the view's inline summary uses
/// against the default media cap (10 GB) so a regression in the
/// `StorageBudget.defaultMediaCapBytes` constant or the unit formatter
/// immediately breaks the snapshot contract.
final class DownloadNextCapAlignmentTests: XCTestCase {

    func testDefaultMediaCapRendersAsTenGB() {
        XCTAssertEqual(
            DownloadSizeEstimator.wholeGigabytes(defaultMediaCapBytes),
            10,
            "defaultMediaCapBytes must render as '10 GB' in the summary copy"
        )
    }

    func testSummaryAtDefaultCapPinsExactString() {
        // Integration-style pin: feed the estimator+formatter into the
        // verbatim-copy producer with the production defaults. This is
        // the snapshot for the default acceptance state.
        let eps: [Episode] = (0..<3).map { _ in
            Episode(
                feedItemGUID: UUID().uuidString,
                feedURL: URL(string: "https://example.com/feed.xml")!,
                title: "t",
                audioURL: URL(string: "https://example.com/e.mp3")!,
                duration: 60 * 60
            )
        }
        let bytes = DownloadSizeEstimator.estimatedBytes(for: eps, count: 3)
        // 3 × 57.6 MB = 172.8 MB → rounds to 173 MB.
        XCTAssertEqual(DownloadSizeEstimator.wholeMegabytes(bytes), 173)

        XCTAssertEqual(
            DownloadNextCopy.withinCapSummary(
                count: 3,
                estimatedMB: DownloadSizeEstimator.wholeMegabytes(bytes),
                capGB: DownloadSizeEstimator.wholeGigabytes(defaultMediaCapBytes)
            ),
            "Downloading 3 episodes (~173 MB). Will fit in your 10 GB cap."
        )
    }
}
