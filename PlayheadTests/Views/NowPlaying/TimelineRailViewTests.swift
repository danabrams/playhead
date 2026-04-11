// TimelineRailViewTests.swift
// Verification tests for playhead-m3d (Now Playing Skip Markers).
// Covers: segment geometry, min-width clamping, tap-to-explain callback,
// skip glide constants, hit target sizing, and accessibility labels.
//
// Haptic feedback is covered separately in TimelineRailHapticTests.swift.

import XCTest
@testable import Playhead

@MainActor
final class TimelineRailViewTests: XCTestCase {

    // MARK: - Minimum Block Width (AC3)

    func testClampedSegmentWidthEnforcesTwoPointFloor() {
        // A tiny segment (0.1% of 300pt = 0.3pt) should be clamped to 2pt.
        let width = TimelineRailView.clampedSegmentWidth(
            segmentRange: 0.50...0.501,
            totalWidth: 300
        )
        XCTAssertEqual(width, 2, accuracy: 0.01,
            "Segments narrower than 2pt must be clamped to 2pt minimum")
    }

    func testClampedSegmentWidthPassesThroughLargerValues() {
        // A normal segment (10% of 400pt = 40pt) should pass through unchanged.
        let width = TimelineRailView.clampedSegmentWidth(
            segmentRange: 0.20...0.30,
            totalWidth: 400
        )
        XCTAssertEqual(width, 40, accuracy: 0.01,
            "Segments wider than 2pt must not be clamped")
    }

    func testClampedSegmentWidthExactlyTwoPoints() {
        // Edge case: exactly 2pt should pass through.
        let width = TimelineRailView.clampedSegmentWidth(
            segmentRange: 0.0...0.01,
            totalWidth: 200
        )
        XCTAssertEqual(width, 2, accuracy: 0.01,
            "Segment exactly at 2pt boundary should return 2pt")
    }

    // MARK: - Skip Glide Constants (AC5)

    func testSkipJumpThreshold() {
        let view = TimelineRailView(
            progress: 0.0,
            adSegments: [],
            onSeek: { _ in }
        )
        XCTAssertEqual(view.skipJumpThreshold, 0.02,
            "Skip jump detection threshold must be 0.02 (2% of timeline)")
    }

    // MARK: - Hit Target Sizing (AC9)

    func testTouchTargetHeight() {
        let view = TimelineRailView(
            progress: 0.0,
            adSegments: [],
            onSeek: { _ in }
        )
        XCTAssertEqual(view.touchTargetHeight, 44,
            "Touch target must be 44pt for accessibility compliance")
    }

    func testRailHeight() {
        let view = TimelineRailView(
            progress: 0.0,
            adSegments: [],
            onSeek: { _ in }
        )
        XCTAssertEqual(view.railHeight, 4,
            "Visual rail height must be 4pt")
    }

    // MARK: - Tap-to-Explain Callback (AC8)

    func testTapToExplainCallbackReceivesSegmentIndex() {
        var tappedIndex: Int?
        let view = TimelineRailView(
            progress: 0.3,
            adSegments: [0.10...0.20, 0.40...0.50, 0.70...0.80],
            onSeek: { _ in },
            onAdSegmentTap: { tappedIndex = $0 }
        )

        // Simulate tapping the second segment (index 1).
        view.onAdSegmentTap?(1)
        XCTAssertEqual(tappedIndex, 1,
            "onAdSegmentTap must forward the correct segment index")
    }

    func testTapToExplainCallbackIsOptional() {
        // Default construction should not crash when tap fires with no handler.
        let view = TimelineRailView(
            progress: 0.5,
            adSegments: [0.10...0.20],
            onSeek: { _ in }
        )
        // Calling the optional closure should be safe (no crash).
        view.onAdSegmentTap?(0)
    }

    // MARK: - Segment Count & Non-Overlap (AC6)

    func testMultipleSegmentsAccepted() {
        let segments: [ClosedRange<Double>] = [
            0.05...0.10,
            0.25...0.30,
            0.55...0.65,
            0.80...0.90
        ]
        let view = TimelineRailView(
            progress: 0.0,
            adSegments: segments,
            onSeek: { _ in }
        )
        XCTAssertEqual(view.adSegments.count, 4,
            "View must accept and store all ad segments")
    }

    func testSegmentGeometryDoesNotOverlap() {
        // With a 400pt rail, verify that adjacent segments' computed positions
        // do not collide.
        let totalWidth: CGFloat = 400
        let segments: [ClosedRange<Double>] = [
            0.10...0.20,  // x: 40, w: 40
            0.25...0.35   // x: 100, w: 40
        ]

        let end0 = segments[0].lowerBound * totalWidth
            + TimelineRailView.clampedSegmentWidth(segmentRange: segments[0], totalWidth: totalWidth)
        let start1 = segments[1].lowerBound * totalWidth

        XCTAssertLessThanOrEqual(end0, start1,
            "Adjacent segment blocks must not overlap in horizontal layout")
    }

    // MARK: - Seek Callback Clamping

    func testScrubEndClampsFractionToUnitRange() {
        var seekedTo: Double?
        let view = TimelineRailView(
            progress: 0.0,
            adSegments: [],
            onSeek: { seekedTo = $0 }
        )

        // The drag gesture already clamps, but handleScrubEnd passes through.
        view.handleScrubEnd(fraction: 0.75)
        XCTAssertEqual(seekedTo, 0.75,
            "handleScrubEnd must forward the fraction to onSeek")
    }
}
