// SkipOrchestratorPreloadTests.swift
//
// Bug 5 (skip-cues-deletion): the orchestrator now preloads
// confirmed-confidence rows directly from `ad_windows` rather than
// from the (deleted) `skip_cues` table. These tests pin the new
// preload path: high-confidence ad_windows are synthesized into the
// orchestrator's `confirmed` set, low-confidence and zero-length rows
// are filtered out, and live ingestion still dedups by window ID.

import CoreMedia
import os
import XCTest
@testable import Playhead

final class SkipOrchestratorPreloadTests: XCTestCase {

    private var store: AnalysisStore!
    private var orchestrator: SkipOrchestrator!

    override func setUp() async throws {
        try await super.setUp()
        let dir = try makeTempDir(prefix: "SkipOrchestratorPreloadTests")
        store = try await AnalysisStore.open(directory: dir)

        // Insert a dummy analysis asset so store lookups work.
        try await store.insertAsset(AnalysisAsset(
            id: "asset-1",
            episodeId: "ep-1",
            assetFingerprint: "fp",
            weakFingerprint: nil,
            sourceURL: "file:///test.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "complete",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        orchestrator = SkipOrchestrator(store: store)
    }

    // MARK: - Helpers

    /// Build a representative AdWindow row for the preload tests.
    /// `decisionState` defaults to `confirmed` so the seeded row is
    /// indistinguishable from one promoted by the live detection path.
    /// `analysisAssetId` defaults to `"asset-1"` (cycle-26 M-1b);
    /// override for cross-asset tests like
    /// `testEmittedAutoSkipBannersDoesNotLeakAcrossEpisodes`.
    private func makeAdWindow(
        id: String,
        start: Double,
        end: Double,
        confidence: Double,
        decisionState: String = "confirmed",
        analysisAssetId: String = "asset-1"
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: analysisAssetId,
            startTime: start,
            endTime: end,
            confidence: confidence,
            boundaryState: "confirmed",
            decisionState: decisionState,
            detectorVersion: "test-v1",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        )
    }

    // MARK: - Tests

    func testBeginEpisodeLoadsHighConfidenceAdWindows() async throws {
        // Seed two high-confidence ad_windows (≥ 0.7) directly into the store.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-1", start: 10.0, end: 40.0, confidence: 0.85)
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "win-2", start: 60.0, end: 90.0, confidence: 0.9)
        )

        // Track skip cues pushed via the handler. Cycle-26 M-2: use a
        // `OSAllocatedUnfairLock`-backed cell rather than a
        // `nonisolated(unsafe) var` so concurrent invocations of the
        // handler from any actor's isolation context cannot race the
        // test's read.
        let pushedCues = OSAllocatedUnfairLock<[CMTimeRange]>(initialState: [])
        await orchestrator.setSkipCueHandler { ranges in
            pushedCues.withLock { $0 = ranges }
        }

        // beginEpisode should load the windows and push them through the pipeline.
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // The orchestrator should have processed the pre-loaded windows.
        // In default shadow mode, windows are confirmed (not applied), so the
        // decision log should have entries for the preloaded windows.
        let log = await orchestrator.getDecisionLog()
        XCTAssertFalse(log.isEmpty, "Decision log should contain entries from preloaded windows")

        // Confirmed windows should be available.
        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertEqual(confirmed.count, 2, "Both preloaded windows should appear as confirmed windows")

        // The handler observation isn't asserted here — shadow-mode runs do
        // not push cues through to the playback service, but the lock cell
        // is referenced so the compiler keeps the closure active.
        _ = pushedCues.withLock { $0 }
    }

    func testBeginEpisodeWithNoAdWindows() async throws {
        // No ad_windows in store -- beginEpisode should succeed without error.
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let log = await orchestrator.getDecisionLog()
        XCTAssertTrue(log.isEmpty, "No decisions should be logged when store has no ad_windows")

        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertTrue(confirmed.isEmpty, "No confirmed windows when store has no ad_windows")
    }

    func testLowConfidenceAdWindowsAreFilteredFromPreload() async throws {
        // Mix of high- and low-confidence rows. Only the high-confidence
        // row (≥ 0.7) should be picked up by the preload.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-high", start: 10.0, end: 40.0, confidence: 0.85)
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "win-low", start: 60.0, end: 90.0, confidence: 0.5)
        )

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertEqual(confirmed.count, 1, "Only the ≥0.7 window should preload")
        XCTAssertEqual(confirmed.first?.id, "win-high")
    }

    func testZeroLengthAdWindowFilteredFromPreload() async throws {
        // Zero-length window: endTime == startTime → must be filtered even
        // if confidence clears the threshold. This mirrors the
        // (deleted) materializer's `endTime > startTime` guard.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-zero", start: 60.0, end: 60.0, confidence: 0.95)
        )

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertTrue(confirmed.isEmpty, "Zero-length window must not preload")
    }

    /// Cycle-21 H-1: only `.suppressed` and `.reverted` are filtered
    /// from the preload. `.applied` rows MUST survive so the skip cue
    /// re-pushes on the next app launch (cross-launch auto-skip
    /// continuity). Banner re-emission for those `.applied` rows is
    /// suppressed separately by `beginEpisode` pre-populating
    /// `banneredWindowIds` (covered by
    /// `testPreloadedAppliedWindowDoesNotEmitBanner` and
    /// `testPreloadedAppliedWindowEmitsCueForCrossLaunchSkip`).
    ///
    /// Pre-cycle-21 (cycle-20 M-1) excluded `.applied` entirely; that
    /// closed the banner-re-fire bug but introduced a quiet regression
    /// of the cross-launch auto-skip path. This test pins the cycle-21
    /// shape: filter ONLY `.suppressed` and `.reverted`.
    func testTerminalDecisionStatesAreFilteredFromPreload() async throws {
        try await store.insertAdWindow(
            makeAdWindow(id: "win-applied", start: 10.0, end: 40.0, confidence: 0.9, decisionState: "applied")
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "win-suppressed", start: 60.0, end: 90.0, confidence: 0.9, decisionState: "suppressed")
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "win-reverted", start: 120.0, end: 150.0, confidence: 0.9, decisionState: "reverted")
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "win-confirmed", start: 200.0, end: 230.0, confidence: 0.9, decisionState: "confirmed")
        )

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // .confirmed and .applied survive; .suppressed and .reverted are filtered.
        let active = await orchestrator.activeWindowIDs()
        XCTAssertEqual(
            active,
            ["win-confirmed", "win-applied"],
            "Preload must drop only `.suppressed` and `.reverted`; `.applied` must survive so the skip cue re-pushes on the next app launch (cross-launch auto-skip continuity)."
        )
    }

    /// Cycle-21 H-1 (banner-emission angle): a preloaded `.applied`
    /// window MUST NOT re-fire its banner, even though the row IS
    /// forwarded so the cue can re-push. Deterministic positive
    /// control: a sibling `.confirmed` row at a later `startTime` IS
    /// expected to emit a banner — `evaluateAndPush` iterates windows
    /// in `snappedStart`-ascending order, so if the `.applied`
    /// suppression is broken the FIRST banner the collector receives
    /// would be for `win-applied`. We assert the first (and only)
    /// banner is for `win-confirmed`, which proves both:
    ///   1. the banner stream is wired up and would have caught a
    ///      `.applied` re-emission if it had happened, and
    ///   2. no `.applied` banner came first.
    ///
    /// Cycle-21 M-2: replaces a fixed `Task.sleep` with a deterministic
    /// "wait for the confirmed banner to arrive" pattern so the test
    /// can't false-pass on a slow CI host where the regression's
    /// banner emission would still be in-flight when the sleep ends.
    ///
    /// Cycle-22 M-2 / Cycle-23 H-1: hardens the test against a future
    /// iteration-order change in `evaluateAndPush`. The "first banner
    /// is win-confirmed" check by itself is iteration-order-coupled —
    /// if the production sort flipped to descending or `snappedEnd`,
    /// the .confirmed banner could land first even with a broken
    /// `.applied` suppression. Cycle-22 added a gate-snapshot check
    /// over `banneredWindowIds` (the helper was deleted in cycle-27
    /// as dead code, since cycle-23 found the gate is also written
    /// from `evaluateAndPush`'s terminal-state branch — so the gate
    /// snapshot cannot distinguish "pre-populated correctly" from
    /// "pre-population missing but eval-loop emitted then inserted").
    /// Cycle-23 replaces it with `emittedAutoSkipBannersSnapshot()`,
    /// which is populated ONLY by `emitBannerItem` — so a
    /// `.contains(id) == false` assertion is genuinely emission-
    /// specific and iteration-order-independent.
    func testPreloadedAppliedWindowDoesNotEmitBanner() async throws {
        // .applied at start=10, .confirmed at start=200 → evaluateAndPush
        // visits .applied FIRST. If banner suppression is broken, the
        // first item the collector sees is from win-applied.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-applied", start: 10.0, end: 40.0, confidence: 0.95, decisionState: "applied")
        )
        try await store.insertAdWindow(
            makeAdWindow(id: "win-confirmed", start: 200.0, end: 230.0, confidence: 0.95, decisionState: "confirmed")
        )

        let bannerStream = await orchestrator.bannerItemStream()
        let collector = Task<[AdSkipBannerItem], Never> {
            var items: [AdSkipBannerItem] = []
            for await item in bannerStream {
                items.append(item)
                if items.count >= 1 { break }
            }
            return items
        }

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // Cycle-23 H-1 (iteration-order-independent + pre-population-
        // specific): observe the EMISSION snapshot, not the suppression
        // gate. The cycle-22 gate-snapshot check was structurally
        // compromised because `banneredWindowIds` is also inserted into
        // by `evaluateAndPush`'s terminal-state emission branch — so a
        // post-`beginEpisode` snapshot of the gate cannot distinguish
        // "pre-populated correctly" from "pre-population missing but
        // eval-loop emitted then inserted". The new
        // `emittedAutoSkipBannersSnapshot()` is populated ONLY by
        // `emitBannerItem`; asserting that `win-applied` is absent
        // proves no banner was emitted for the preloaded `.applied`
        // window, regardless of `evaluateAndPush` iteration order.
        let emittedBanners = await orchestrator.emittedAutoSkipBannersSnapshot()
        XCTAssertFalse(
            emittedBanners.contains("win-applied"),
            """
            Cycle-21 H-1 regression: `beginEpisode` did NOT pre-populate \
            `banneredWindowIds` for the preloaded `.applied` window, so \
            `evaluateAndPush` emitted a fresh banner for it. The terminal-\
            state branch only suppresses a banner when \
            `banneredWindowIds.contains(id)`; without the pre-population, \
            the `.applied` row re-fires its banner on every app launch.
            """
        )

        // Deterministic: wait for the .confirmed banner to arrive (it
        // WILL arrive — the orchestrator emits a banner the first time
        // it sees a .confirmed window). The collector breaks after 1
        // item, so we get exactly the FIRST banner emitted. This
        // check proves the gate is wired through `evaluateAndPush`
        // (i.e., the suppression actually fires), complementing the
        // gate-snapshot assertion above.
        let received = await collector.value
        XCTAssertEqual(received.count, 1, "Expected exactly one banner — the .confirmed window's.")
        XCTAssertEqual(
            received.first?.windowId,
            "win-confirmed",
            """
            Cycle-21 H-1 regression: a preloaded `.applied` window emitted \
            a banner on app restart. Because evaluateAndPush iterates \
            windows in snappedStart-ascending order, an `.applied` row at \
            start=10 is visited before the `.confirmed` row at start=200; \
            if the suppression were broken, the collector's first item \
            would be for `win-applied` instead of `win-confirmed`. \
            beginEpisode must pre-populate `banneredWindowIds` for every \
            preloaded `.applied` row so the terminal-state branch in \
            `evaluateAndPush` skips its banner emission.
            """
        )
    }

    /// Cycle-21 H-1 (cue-push angle): a preloaded `.applied` window
    /// MUST push its skip cue through the handler so playback auto-
    /// skips the ad on the next app launch. This is the cross-launch
    /// auto-skip contract that pre-pivot's `skip_cues` table provided
    /// implicitly (every confidence-passing row was re-cued at episode
    /// start). Cycle-20 M-1 broke this by filtering `.applied` from
    /// the preload; cycle-21 H-1 restores it by allowing `.applied`
    /// through and pre-suppressing only the banner.
    func testPreloadedAppliedWindowEmitsCueForCrossLaunchSkip() async throws {
        try await store.insertAdWindow(
            makeAdWindow(id: "win-applied", start: 10.0, end: 40.0, confidence: 0.95, decisionState: "applied")
        )

        // Cycle-26 M-2: lock-backed cell instead of `nonisolated(unsafe) var`.
        let pushedCuesCell = OSAllocatedUnfairLock<[CMTimeRange]>(initialState: [])
        await orchestrator.setSkipCueHandler { ranges in
            pushedCuesCell.withLock { $0 = ranges }
        }

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        let pushedCues = pushedCuesCell.withLock { $0 }
        XCTAssertFalse(
            pushedCues.isEmpty,
            """
            Cycle-21 H-1 regression: a preloaded `.applied` window did \
            NOT push a skip cue through the handler. Pre-pivot, the \
            `skip_cues` table re-cued every confidence-passing row at \
            episode start; post-pivot, the `ad_windows` preload must do \
            the same job for `.applied` rows so playback auto-skips an \
            already-skipped ad on the next app launch.
            """
        )
        XCTAssertEqual(pushedCues.count, 1, "Expected exactly one cue range for the single .applied window.")
        XCTAssertEqual(
            pushedCues.first?.start.seconds ?? -1.0,
            10.0,
            accuracy: 0.001,
            "Cue range must start at the .applied window's startTime."
        )
        // Cycle-22 L-3 / Cycle-23 L-3: pin the cue end-time as well.
        // The default `adTrailingCushionSeconds` subtracts a fixed
        // amount from the trailing edge of the merged pod. Derive the
        // expected end from `SkipPolicyConfig.default` rather than
        // hardcoding `39.0`, so a deliberate cushion change at the
        // policy level updates this assertion automatically — a
        // hardcoded literal would mask such a change as a test failure
        // here even when behavior is correct.
        let expectedEnd = 40.0 - SkipPolicyConfig.default.adTrailingCushionSeconds
        XCTAssertEqual(
            pushedCues.first?.end.seconds ?? -1.0,
            expectedEnd,
            accuracy: 0.001,
            """
            Cue range must end at the .applied window endTime (40.0) \
            minus `SkipPolicyConfig.default.adTrailingCushionSeconds` \
            (\(SkipPolicyConfig.default.adTrailingCushionSeconds)) = \
            \(expectedEnd).
            """
        )
    }

    /// Cycle-22 M-1 / Cycle-23 M-2 (exhaustiveness pin): the production
    /// preload filter is derived from
    /// `SkipDecisionState.allCases.filter(isPreloadEligible)`.
    /// `isPreloadEligible` is an exhaustive `switch`, so adding a new
    /// case to `SkipDecisionState` will fail to compile until an author
    /// explicitly classifies the new state. This test additionally pins
    /// the *current* partition so a careless edit that flips a case's
    /// eligibility (e.g., adding `.suppressed` to the `true` arm) fails
    /// loudly with a behavioral check, not just at compile time.
    ///
    /// Cycle-23 M-2: the seed loop iterates over `SkipDecisionState.allCases`
    /// rather than hardcoding 5 raw strings, so adding a 6th case
    /// inserts a 6th window automatically — and any classification
    /// disagreement with `expectedActive` (which still has to enumerate
    /// the eligible cases explicitly, since `isPreloadEligible` is
    /// private) fires the assertion. The author is forced to update
    /// both the production partition and `expectedActive` together.
    func testPreloadEligibilityPartitionMatchesAllEnumCases() async throws {
        // Seed exactly one window per `SkipDecisionState` case at unique
        // startTimes so all cases are present in the store. Iteration
        // order over `allCases` is the declaration order, but we don't
        // depend on it — we sort `active` and `expectedActive` below.
        for (index, state) in SkipDecisionState.allCases.enumerated() {
            let start = 10.0 + Double(index) * 50.0
            try await store.insertAdWindow(
                makeAdWindow(
                    id: "win-\(state.rawValue)",
                    start: start,
                    end: start + 30.0,
                    confidence: 0.95,
                    decisionState: state.rawValue
                )
            )
        }

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // Behavioral pin: the expected eligible set MUST stay in sync
        // with `SkipOrchestrator.isPreloadEligible`. Listing it
        // explicitly here means a flipped classification (e.g., adding
        // `.reverted` to the `true` arm) fails loudly with a real
        // active-set diff, not a silent compile-time pass.
        let expectedActive: [String] = [
            "win-\(SkipDecisionState.candidate.rawValue)",
            "win-\(SkipDecisionState.confirmed.rawValue)",
            "win-\(SkipDecisionState.applied.rawValue)"
        ].sorted()

        let active = await orchestrator.activeWindowIDs().sorted()
        XCTAssertEqual(
            active,
            expectedActive,
            """
            Cycle-22 M-1 / Cycle-23 M-2 regression: the preload \
            partition over `SkipDecisionState` cases drifted. Currently \
            only `.candidate`, `.confirmed`, and `.applied` should \
            survive — `.suppressed` and `.reverted` are terminal "no \
            skip" decisions and must be filtered. If you added a new \
            case to `SkipDecisionState`, the seed loop above already \
            covers it; update both `SkipOrchestrator.isPreloadEligible` \
            and `expectedActive` here to classify the new state \
            deliberately.
            """
        )
    }

    func testLiveDedupWithPreloaded() async throws {
        // Pre-seed an ad_window in the store.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-pre", start: 20.0, end: 50.0, confidence: 0.8)
        )

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")

        // Now send a live AdWindow with the SAME ID covering the same region.
        let liveWindow = AdWindow(
            id: "win-pre",  // Same ID as the preloaded row.
            analysisAssetId: "asset-1",
            startTime: 20.0,
            endTime: 50.0,
            confidence: 0.8,
            boundaryState: "confirmed",
            decisionState: "confirmed",
            detectorVersion: "live-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "live",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )

        await orchestrator.receiveAdWindows([liveWindow])

        // The orchestrator must NOT create a duplicate -- the same window ID
        // means the existing managed window is updated (not duplicated).
        let confirmed = await orchestrator.confirmedWindows()
        XCTAssertEqual(confirmed.count, 1, "Duplicate window must not create a second confirmed entry")
    }

    /// Cycle-24 missing-test: pin that `endEpisode` clears
    /// `emittedAutoSkipBannerWindowIds` so a new episode does not
    /// inherit emission state from the previous one. Cycle-23 H-1
    /// added the reset in `endEpisode` but coverage was implicit; an
    /// explicit assertion ensures a future code change that forgets
    /// the reset fails loudly here, rather than surfacing as a
    /// downstream "second episode wrongly suppresses banner" bug
    /// that would be much harder to root-cause.
    ///
    /// Cycle-25 L-2 setup note: the test subscribes to
    /// `bannerItemStream()` BEFORE `beginEpisode` because
    /// `emitBannerItem` early-returns on empty `bannerContinuations`.
    /// Without an active subscriber the emission never reaches the
    /// `emittedAutoSkipBannerWindowIds.insert` line, the precondition
    /// assertion fails with "Setup precondition: emission set must
    /// contain `win-confirmed`", and the test signals "setup broken"
    /// rather than "endEpisode reset broken".
    func testEndEpisodeResetsEmittedAutoSkipBannersSet() async throws {
        // Seed a confirmed window high enough to fire a banner.
        try await store.insertAdWindow(
            makeAdWindow(id: "win-confirmed", start: 10.0, end: 40.0, confidence: 0.95, decisionState: "confirmed")
        )

        let bannerStream = await orchestrator.bannerItemStream()
        let collector = Task<[AdSkipBannerItem], Never> {
            var items: [AdSkipBannerItem] = []
            for await item in bannerStream {
                items.append(item)
                if items.count >= 1 { break }
            }
            return items
        }

        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")
        let received = await collector.value
        XCTAssertEqual(received.count, 1, "Setup precondition: confirmed window must emit one banner.")

        let emittedDuringEpisode = await orchestrator.emittedAutoSkipBannersSnapshot()
        XCTAssertTrue(
            emittedDuringEpisode.contains("win-confirmed"),
            "Setup precondition: emission set must contain `win-confirmed` after the banner is emitted."
        )

        await orchestrator.endEpisode()

        let emittedAfterEnd = await orchestrator.emittedAutoSkipBannersSnapshot()
        XCTAssertTrue(
            emittedAfterEnd.isEmpty,
            """
            Cycle-24 missing-test regression: `endEpisode` did NOT \
            clear `emittedAutoSkipBannerWindowIds`. A subsequent \
            `beginEpisode` would inherit the prior episode's emission \
            state, suppressing banners for windows that should fire \
            fresh in the new episode. Cycle-23 H-1 added the reset at \
            `endEpisode` (and `beginEpisode`); restore it. Got: \
            \(emittedAfterEnd).
            """
        )
    }

    /// Cycle-25 L-1: behavioral pin for cross-episode banner-emission
    /// isolation. The reset assertion in
    /// `testEndEpisodeResetsEmittedAutoSkipBannersSet` proves the set
    /// is empty after `endEpisode`, but it does NOT prove that the
    /// next episode's banners actually fire fresh. If a future change
    /// dropped the `endEpisode` reset and instead left
    /// `emittedAutoSkipBannerWindowIds` populated across episodes,
    /// observability tests downstream of the snapshot accessor would
    /// silently include stale window IDs from the prior episode.
    /// This test runs the full sequence — beginEpisode(asset-1) →
    /// banner emission → endEpisode → beginEpisode(asset-2) — and
    /// asserts the second-episode snapshot contains ONLY the
    /// second-episode window's ID, with no carry-over from episode 1.
    ///
    /// Subscriber-gating note (cycle-25 L-2): each episode subscribes
    /// to `bannerItemStream()` BEFORE its `beginEpisode`, because
    /// `emitBannerItem` early-returns on empty `bannerContinuations`.
    /// Without an active subscriber the emission set never gains the
    /// window ID, and the setup precondition would fail rather than
    /// the cross-episode invariant.
    func testEmittedAutoSkipBannersDoesNotLeakAcrossEpisodes() async throws {
        // Episode 2 needs its own asset row so beginEpisode(asset-2)
        // resolves a real `analysis_assets` record and its preload
        // pulls the asset-2 ad_window.
        try await store.insertAsset(AnalysisAsset(
            id: "asset-2",
            episodeId: "ep-2",
            assetFingerprint: "fp2",
            weakFingerprint: nil,
            sourceURL: "file:///test2.mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "complete",
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))

        // win-ep1 lives on asset-1 (seeded in setUp); win-ep2 lives on
        // asset-2. Cycle-26 M-1b extended `makeAdWindow` to accept
        // `analysisAssetId` so this test no longer constructs AdWindow
        // inline (which would compile-break on every new field).
        try await store.insertAdWindow(
            makeAdWindow(id: "win-ep1", start: 10.0, end: 40.0, confidence: 0.95, decisionState: "confirmed")
        )
        try await store.insertAdWindow(
            makeAdWindow(
                id: "win-ep2",
                start: 10.0,
                end: 40.0,
                confidence: 0.95,
                decisionState: "confirmed",
                analysisAssetId: "asset-2"
            )
        )

        // --- Episode 1 ---
        let stream1 = await orchestrator.bannerItemStream()
        let collector1 = Task<[AdSkipBannerItem], Never> {
            var items: [AdSkipBannerItem] = []
            for await item in stream1 {
                items.append(item)
                if items.count >= 1 { break }
            }
            return items
        }
        await orchestrator.beginEpisode(analysisAssetId: "asset-1", episodeId: "asset-1")
        let ep1Banners = await collector1.value
        XCTAssertEqual(
            ep1Banners.first?.windowId,
            "win-ep1",
            "Setup precondition: episode 1's confirmed window must emit a banner before we test cross-episode isolation."
        )

        let snapshotDuringEp1 = await orchestrator.emittedAutoSkipBannersSnapshot()
        XCTAssertTrue(
            snapshotDuringEp1.contains("win-ep1"),
            "Setup precondition: episode 1's emission snapshot must contain `win-ep1`."
        )

        await orchestrator.endEpisode()

        // --- Episode 2 ---
        let stream2 = await orchestrator.bannerItemStream()
        let collector2 = Task<[AdSkipBannerItem], Never> {
            var items: [AdSkipBannerItem] = []
            for await item in stream2 {
                items.append(item)
                if items.count >= 1 { break }
            }
            return items
        }
        await orchestrator.beginEpisode(analysisAssetId: "asset-2", episodeId: "asset-2")
        let ep2Banners = await collector2.value
        XCTAssertEqual(
            ep2Banners.count,
            1,
            "Episode 2's confirmed window must emit a fresh banner — emittedAutoSkipBannerWindowIds carry-over would not cause suppression here (the set is observability-only), but absence of a banner indicates a deeper regression in the new-episode emission path."
        )
        XCTAssertEqual(
            ep2Banners.first?.windowId,
            "win-ep2",
            "Episode 2's first banner must be for `win-ep2` (the only confirmed window on asset-2)."
        )

        let snapshotDuringEp2 = await orchestrator.emittedAutoSkipBannersSnapshot()
        XCTAssertFalse(
            snapshotDuringEp2.contains("win-ep1"),
            """
            Cycle-25 L-1 regression: emittedAutoSkipBannerWindowIds \
            leaked across an episode boundary. Episode 2's snapshot \
            still contains `win-ep1` from episode 1, even after \
            `endEpisode` and a fresh `beginEpisode` for a different \
            asset. `endEpisode` (and `beginEpisode`) MUST clear \
            `emittedAutoSkipBannerWindowIds` — see cycle-23 H-1. \
            Got: \(snapshotDuringEp2).
            """
        )
        XCTAssertTrue(
            snapshotDuringEp2.contains("win-ep2"),
            "Episode 2's emission snapshot must contain `win-ep2` after its banner fires."
        )
        // Cycle-26 M-1a: also pin the EXACT size. Without this, a
        // future regression that emits a phantom `win-ep1` AND `win-ep2`
        // (e.g. preload re-arming on the wrong asset filter) would slip
        // past the contains/!contains checks above. Size-1 is the only
        // correct outcome here.
        XCTAssertEqual(
            snapshotDuringEp2.count,
            1,
            """
            Cycle-26 M-1a regression: episode 2's emission snapshot \
            contains an unexpected number of window IDs. Exactly one \
            (`win-ep2`) is correct; any other count signals stale \
            episode-1 state, a phantom emission, or a preload reading \
            from the wrong asset filter. Got: \(snapshotDuringEp2).
            """
        )
    }
}
