// EpisodeDurationBucketClassifierTests.swift
// Boundary tests for EpisodeDurationBucketClassifier.
//
// Boundary rule under test:
//   under30m        : duration <  30 * 60
//   between30and60m : 30 * 60 <= duration <= 60 * 60
//   between60and90m : 60 * 60 <  duration <= 90 * 60
//   over90m         : 90 * 60 <  duration

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeDurationBucketClassifier")
struct EpisodeDurationBucketClassifierTests {

    private func bucket(_ minutes: Double) -> SLIEpisodeDurationBucket {
        EpisodeDurationBucketClassifier.bucket(forDurationSeconds: minutes * 60)
    }

    // MARK: - 30-minute boundary

    @Test("Just below 30 min -> under30m")
    func justBelow30() {
        #expect(bucket(29.99) == .under30m)
    }

    @Test("Exactly 30 min -> between30and60m (lower bucket owns the boundary)")
    func exactly30() {
        #expect(bucket(30.0) == .between30and60m)
    }

    @Test("Just above 30 min -> between30and60m")
    func justAbove30() {
        #expect(bucket(30.01) == .between30and60m)
    }

    // MARK: - 60-minute boundary

    @Test("Just below 60 min -> between30and60m")
    func justBelow60() {
        #expect(bucket(59.99) == .between30and60m)
    }

    @Test("Exactly 60 min -> between30and60m (inclusive upper — lower bucket owns boundary)")
    func exactly60() {
        #expect(bucket(60.0) == .between30and60m)
    }

    @Test("Just above 60 min -> between60and90m")
    func justAbove60() {
        #expect(bucket(60.01) == .between60and90m)
    }

    // MARK: - 90-minute boundary

    @Test("Just below 90 min -> between60and90m")
    func justBelow90() {
        #expect(bucket(89.99) == .between60and90m)
    }

    @Test("Exactly 90 min -> between60and90m (inclusive upper — lower bucket owns boundary)")
    func exactly90() {
        #expect(bucket(90.0) == .between60and90m)
    }

    @Test("Just above 90 min -> over90m")
    func justAbove90() {
        #expect(bucket(90.01) == .over90m)
    }

    // MARK: - Far-from-boundary points

    @Test("Typical short: 5 min -> under30m")
    func typicalShort() {
        #expect(bucket(5.0) == .under30m)
    }

    @Test("Typical hour-long: 45 min -> between30and60m")
    func typicalMedium() {
        #expect(bucket(45.0) == .between30and60m)
    }

    @Test("Typical longer: 75 min -> between60and90m")
    func typicalLong() {
        #expect(bucket(75.0) == .between60and90m)
    }

    @Test("Long-form: 120 min -> over90m")
    func longForm() {
        #expect(bucket(120.0) == .over90m)
    }

    // MARK: - Edge cases

    @Test("Zero duration -> under30m")
    func zeroDuration() {
        #expect(bucket(0.0) == .under30m)
    }

    @Test("Negative duration (unknown sentinel) -> under30m")
    func negativeDuration() {
        // Classifier must be total; negative is treated as "< 30 * 60".
        #expect(EpisodeDurationBucketClassifier.bucket(forDurationSeconds: -1) == .under30m)
    }

    @Test("Very long duration -> over90m")
    func veryLongDuration() {
        // 10 hours.
        #expect(EpisodeDurationBucketClassifier.bucket(forDurationSeconds: 10 * 60 * 60) == .over90m)
    }

    // MARK: - Threshold constants sanity

    @Test("Threshold constants match the documented minute values")
    func thresholdsMatchDocumentation() {
        #expect(EpisodeDurationBucketThresholds.thirtyMinutesSeconds == 30 * 60)
        #expect(EpisodeDurationBucketThresholds.sixtyMinutesSeconds == 60 * 60)
        #expect(EpisodeDurationBucketThresholds.ninetyMinutesSeconds == 90 * 60)
    }
}
