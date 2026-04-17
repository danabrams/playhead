// LiveActivityCopyTests.swift
// playhead-44h1: snapshot-equivalence tests for the provisional Live
// Activity copy templates. Each state (downloading / analyzing /
// paused) has a fixed string contract that the WidgetKit consumer
// will consume in a later bead; these tests pin the contract against
// known baselines.
//
// The test suite is also where the "no `Now`/`Soon`/`Background`
// literal" acceptance is pinned against the provider itself: every
// branch of every `*Text(for:)` entry point is exercised and the
// resulting string is scanned for the forbidden tokens. This is
// belt-and-braces on top of the `SchedulerLaneUILintTests` grep lint
// (which scans source files, not computed strings).

import Foundation
import Testing
@testable import Playhead

@Suite("LiveActivityCopy — downloading")
struct LiveActivityCopyDownloadingTests {

    @Test("Single episode uses bare-singular phrasing")
    func singleEpisode() {
        let state = LiveActivityDownloadingState(
            queuedCount: 1,
            totalBytesWritten: 500 * 1024 * 1024,
            totalBytesExpectedToWrite: 1024 * 1024 * 1024
        )
        let copy = LiveActivityCopy.downloadingText(for: state)
        // Binary ByteCountFormatter: 500 MiB / 1 GiB (actual suffix
        // varies by locale / system formatter, so we only pin the
        // prefix and the byte separator).
        #expect(copy.hasPrefix("Downloading 1 episode · "))
        #expect(copy.contains(" / "))
        // Must not contain the plural ".. episodes" form.
        #expect(!copy.contains("1 episodes"))
    }

    @Test("Multiple episodes pluralize correctly")
    func multipleEpisodes() {
        let state = LiveActivityDownloadingState(
            queuedCount: 3,
            totalBytesWritten: 100 * 1024 * 1024,
            totalBytesExpectedToWrite: 500 * 1024 * 1024
        )
        let copy = LiveActivityCopy.downloadingText(for: state)
        #expect(copy.hasPrefix("Downloading 3 episodes · "))
    }

    @Test("Unknown expected-total omits byte suffix")
    func unknownTotalOmitsBytes() {
        let state = LiveActivityDownloadingState(
            queuedCount: 2,
            totalBytesWritten: 100_000_000,
            totalBytesExpectedToWrite: 0
        )
        let copy = LiveActivityCopy.downloadingText(for: state)
        #expect(copy == "Downloading 2 episodes")
    }

    @Test("Copy contains no forbidden lane-label literals")
    func noForbiddenLaneLabels() {
        for count in [1, 2, 5] {
            let state = LiveActivityDownloadingState(
                queuedCount: count,
                totalBytesWritten: 100_000,
                totalBytesExpectedToWrite: 1_000_000
            )
            let copy = LiveActivityCopy.downloadingText(for: state)
            assertNoLaneLiterals(copy)
        }
    }
}

@Suite("LiveActivityCopy — analyzing")
struct LiveActivityCopyAnalyzingTests {

    @Test("Running episode uses ETA formula")
    func runningEpisodeComputesETA() {
        // 600 s duration / 20 s nominal = 30 shards total. 10 completed
        // → 20 remaining. avgShardDurationMs = 3_000 → 60_000 ms → 1 min.
        let state = LiveActivityAnalyzingState(
            episodeDurationSec: 600,
            shardsCompleted: 10,
            nominalShardDurationSec: 20,
            avgShardDurationMs: 3_000,
            queuedRemaining: 0
        )
        #expect(LiveActivityCopy.analyzingText(for: state) == "Analyzing · ~1 min remaining")
    }

    @Test("Missing shards_completed falls back to 0")
    func missingShardsCompletedFallsBackToZero() {
        // 400 s / 20 s = 20 shards. nil → 0 completed → 20 remaining.
        // avg 4500 ms → 90_000 ms → 1.5 min → rounded up to 2 min.
        let state = LiveActivityAnalyzingState(
            episodeDurationSec: 400,
            shardsCompleted: nil,
            nominalShardDurationSec: 20,
            avgShardDurationMs: 4_500,
            queuedRemaining: 0
        )
        let text = LiveActivityCopy.analyzingText(for: state)
        #expect(text == "Analyzing · ~2 min remaining",
                "nil shards_completed MUST fall back to 0 and still render an ETA; got \(text)")
    }

    @Test("Unknown device class uses fallback avgShardDurationMs")
    func unknownDeviceClassFallback() {
        // avgShardDurationMs = 0 should trigger the 4500 ms fallback.
        // Duration 200 s / 20 = 10 shards. 0 completed → 10 remaining.
        // 10 * 4500 = 45000 ms = 0.75 min → ceil → 1 min.
        let state = LiveActivityAnalyzingState(
            episodeDurationSec: 200,
            shardsCompleted: 0,
            nominalShardDurationSec: 20,
            avgShardDurationMs: 0,
            queuedRemaining: 0
        )
        #expect(LiveActivityCopy.analyzingText(for: state) == "Analyzing · ~1 min remaining")
    }

    @Test("No running job uses Queued · N to go template")
    func noRunningJobShowsQueued() {
        let state = LiveActivityAnalyzingState.queuedOnly(queuedRemaining: 3)
        #expect(LiveActivityCopy.analyzingText(for: state) == "Queued · 3 to go")
    }

    @Test("No running job and empty queue renders plain Queued")
    func emptyQueueRendersQueued() {
        let state = LiveActivityAnalyzingState.queuedOnly(queuedRemaining: 0)
        #expect(LiveActivityCopy.analyzingText(for: state) == "Queued")
    }

    @Test("Overshoot of completed shards never goes negative")
    func overshootNeverNegative() {
        // 60 s / 20 s = 3 shards but caller reports 10 completed — the
        // formula must clamp to 0 remaining, not render a negative min.
        let state = LiveActivityAnalyzingState(
            episodeDurationSec: 60,
            shardsCompleted: 10,
            nominalShardDurationSec: 20,
            avgShardDurationMs: 3_000,
            queuedRemaining: 0
        )
        #expect(LiveActivityCopy.analyzingText(for: state) == "Analyzing · ~0 min remaining")
    }

    @Test("ETA resolves avgShardDurationMs from device-class profile")
    func etaUsesDeviceClassProfileAvgShardMs() {
        // Mocked iPhone 17 Pro profile: avgShardDurationMs = 2500.
        // 600 s / 20 s = 30 shards. 10 completed → 20 remaining.
        // 20 * 2500 = 50000 ms → ceil(50/60) = 1 min.
        let pro = DeviceClassProfile(
            deviceClass: DeviceClass.iPhone17Pro.rawValue,
            grantWindowMedianSeconds: 45,
            grantWindowP95Seconds: 90,
            nominalSliceSizeBytes: 25_000_000,
            cpuWindowSeconds: 40,
            bytesPerCpuSecond: 625_000,
            avgShardDurationMs: 2500
        )
        #expect(LiveActivityCopy.resolveAvgShardDurationMs(from: pro) == 2500)

        let text = LiveActivityCopy.analyzingText(
            episodeDurationSec: 600,
            shardsCompleted: 10,
            nominalShardDurationSec: 20,
            queuedRemaining: 0,
            deviceProfile: pro
        )
        #expect(text == "Analyzing · ~1 min remaining",
                "Device-class profile avgShardDurationMs must drive the ETA; got \(text)")
    }

    @Test("Unknown device-class profile falls back to 4500 ms")
    func etaFallsBackWhenProfileMissing() {
        // nil profile → resolver returns fallbackAvgShardDurationMs
        // (4500 ms). 200 s / 20 = 10 shards. 0 completed → 10
        // remaining. 10 * 4500 = 45000 ms = 0.75 min → ceil → 1 min.
        #expect(LiveActivityCopy.resolveAvgShardDurationMs(from: nil)
                == LiveActivityCopy.fallbackAvgShardDurationMs)
        #expect(LiveActivityCopy.fallbackAvgShardDurationMs == 4500)

        let text = LiveActivityCopy.analyzingText(
            episodeDurationSec: 200,
            shardsCompleted: 0,
            nominalShardDurationSec: 20,
            queuedRemaining: 0,
            deviceProfile: nil
        )
        #expect(text == "Analyzing · ~1 min remaining",
                "nil device-class profile must fall back to 4500 ms; got \(text)")
    }

    @Test("Zero avgShardDurationMs in profile uses fallback")
    func etaFallsBackWhenProfileReportsZero() {
        // A malformed/seed profile with zero avgShardDurationMs must
        // route through the same fallback so the ETA never divides by
        // zero downstream. Constructed off iPhone SE3's seed values
        // but with avgShardDurationMs clobbered to 0.
        let bad = DeviceClassProfile(
            deviceClass: DeviceClass.iPhoneSE3.rawValue,
            grantWindowMedianSeconds: 25,
            grantWindowP95Seconds: 55,
            nominalSliceSizeBytes: 10_000_000,
            cpuWindowSeconds: 20,
            bytesPerCpuSecond: 500_000,
            avgShardDurationMs: 0
        )
        #expect(LiveActivityCopy.resolveAvgShardDurationMs(from: bad)
                == LiveActivityCopy.fallbackAvgShardDurationMs)
    }

    @Test("Analyzing copy contains no forbidden lane-label literals")
    func analyzingHasNoLaneLiterals() {
        let running = LiveActivityAnalyzingState(
            episodeDurationSec: 600,
            shardsCompleted: 5,
            nominalShardDurationSec: 20,
            avgShardDurationMs: 3_000,
            queuedRemaining: 2
        )
        assertNoLaneLiterals(LiveActivityCopy.analyzingText(for: running))
        assertNoLaneLiterals(LiveActivityCopy.analyzingText(for: .queuedOnly(queuedRemaining: 1)))
        assertNoLaneLiterals(LiveActivityCopy.analyzingText(for: .queuedOnly(queuedRemaining: 0)))
    }
}

@Suite("LiveActivityCopy — paused")
struct LiveActivityCopyPausedTests {

    @Test("Every SurfaceReason renders plain-English copy")
    func everyReasonRenders() {
        // Exhaustive coverage pinning that no SurfaceReason case
        // leaks a raw rawValue (e.g. "couldnt_analyze") into the UI.
        // This is the Phase 1 provisional mapping; playhead-dfem
        // replaces it with the canonical localized table.
        for reason in SurfaceReason.allCases {
            let state = LiveActivityPausedState(reason: reason)
            let copy = LiveActivityCopy.pausedText(for: state)
            #expect(copy.hasPrefix("Paused — "), "unexpected paused copy: \(copy)")
            // The enum raw-value format is `snake_case`; no paused
            // template may contain an underscore (catches the
            // "raw name leaked through" regression).
            #expect(!copy.contains("_"),
                    "Paused copy for \(reason) contains a raw enum rawValue: \(copy)")
            assertNoLaneLiterals(copy)
        }
    }

    @Test("Paused copy format pins known baseline strings")
    func pausedBaselineStrings() {
        // Pin a few representative mappings as an equatable snapshot.
        #expect(LiveActivityCopy.pausedText(for: .init(reason: .phoneIsHot))
                == "Paused — the phone is warm")
        #expect(LiveActivityCopy.pausedText(for: .init(reason: .waitingForNetwork))
                == "Paused — waiting for Wi-Fi")
        #expect(LiveActivityCopy.pausedText(for: .init(reason: .analysisUnavailable))
                == "Paused — analysis is unavailable on this device")
    }
}

// MARK: - Helpers

/// The bead's acceptance says no UI element may contain the literal
/// words "Now" / "Soon" / "Background". This helper checks each
/// generated copy string against those tokens. Word-boundary checks
/// are used so we don't flag words like "Downloading" that happen
/// to CONTAIN "Now" as a substring (they don't, but future tokens
/// might).
func assertNoLaneLiterals(
    _ copy: String,
    sourceLocation: Testing.SourceLocation = #_sourceLocation
) {
    let forbidden = ["Now", "Soon", "Background"]
    for token in forbidden {
        // Word-boundary check via a simple scan: ensure each whole-word
        // occurrence is absent. Accept substrings like "Downloaded" etc.
        let words = copy.split(whereSeparator: { !$0.isLetter })
        let hit = words.contains(where: { $0 == Substring(token) })
        #expect(!hit,
                "Copy string leaked forbidden lane literal \"\(token)\": \(copy)",
                sourceLocation: sourceLocation)
    }
}
