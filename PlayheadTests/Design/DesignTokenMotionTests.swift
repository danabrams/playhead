// DesignTokenMotionTests.swift
// Verifies Motion tokens are all eased (no spring/bounce) and durations
// match the "Quiet Instrument" spec: quick=0.15, standard=0.25, deliberate=0.4.

import XCTest
@testable import Playhead

final class DesignTokenMotionTests: XCTestCase {

    func testQuickDurationIs150ms() {
        XCTAssertEqual(Motion.quickDescriptor.duration, 0.15, accuracy: 1e-9)
    }

    func testStandardDurationIs250ms() {
        XCTAssertEqual(Motion.standardDescriptor.duration, 0.25, accuracy: 1e-9)
    }

    func testDeliberateDurationIs400ms() {
        XCTAssertEqual(Motion.deliberateDescriptor.duration, 0.4, accuracy: 1e-9)
    }

    func testPreciseEaseIsTimingCurveNoBounce() {
        let d = Motion.preciseEaseDescriptor
        XCTAssertEqual(d.kind, .timingCurve, "preciseEase must use a cubic timing curve, not a spring")
        // Fast-settle curve: (0.2, 0.0, 0.0, 1.0) ≈ Material "decelerate"; no overshoot.
        XCTAssertEqual(d.controlPoints?.c1x ?? -1, 0.2, accuracy: 1e-9)
        XCTAssertEqual(d.controlPoints?.c1y ?? -1, 0.0, accuracy: 1e-9)
        XCTAssertEqual(d.controlPoints?.c2x ?? -1, 0.0, accuracy: 1e-9)
        XCTAssertEqual(d.controlPoints?.c2y ?? -1, 1.0, accuracy: 1e-9)
    }

    func testTransportIsLinear() {
        XCTAssertEqual(Motion.transportDescriptor.kind, .linear,
                       "Transport curve (scrubber) must be linear")
    }

    func testNoTokenUsesSpring() {
        let all: [MotionDescriptor] = [
            Motion.quickDescriptor,
            Motion.standardDescriptor,
            Motion.deliberateDescriptor,
            Motion.preciseEaseDescriptor,
            Motion.transportDescriptor
        ]
        for d in all {
            XCTAssertNotEqual(d.kind, .spring, "\(d.name) must not use spring physics")
            XCTAssertNotEqual(d.kind, .interpolatingSpring, "\(d.name) must not use interpolatingSpring")
            XCTAssertNotEqual(d.kind, .bouncy, "\(d.name) must not use bouncy")
        }
    }
}
