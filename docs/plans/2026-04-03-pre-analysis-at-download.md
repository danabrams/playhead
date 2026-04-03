# Pre-Analysis at Download Time — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Pre-analyze the first 90s-10min of podcast audio at download time so pre-roll ads can be skipped instantly at play-start.

**Architecture:** `DownloadManager` notifies a new `PreAnalysisScheduler` actor on download completion. The scheduler manages a FIFO work queue and tier progression (90s always, 3/5/10min while charging), delegating pipeline execution to `AnalysisCoordinator` via a new `preAnalyze()` entry point. At play-start, `SkipOrchestrator` loads pre-computed AdWindows from SQLite and skip cues are ready before the first audio sample.

**Tech Stack:** Swift 6, Swift actors, SQLite (AnalysisStore), CapabilitiesService (charging observation), UIKit battery APIs

**Design doc:** `docs/plans/2026-04-03-pre-analysis-at-download-design.md`

---

### Task 1: Add charging state to CapabilitySnapshot and CapabilitiesService

`CapabilitiesService` currently tracks thermal state and Low Power Mode but not charging. The `PreAnalysisScheduler` needs to observe charging transitions.

**Files:**
- Modify: `Playhead/Services/Capabilities/CapabilitySnapshot.swift:8-47`
- Modify: `Playhead/Services/Capabilities/CapabilitiesService.swift:78-102` (startObserving)
- Modify: `Playhead/Services/Capabilities/CapabilitiesService.swift:108-129` (captureSnapshot)
- Test: `PlayheadTests/Services/Capabilities/CapabilitySnapshotTests.swift`

**Step 1: Write the failing test**

```swift
// PlayheadTests/Services/Capabilities/CapabilitySnapshotTests.swift
import XCTest
@testable import Playhead

final class CapabilitySnapshotTests: XCTestCase {
    func testSnapshotIncludesChargingState() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: true,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        XCTAssertTrue(snapshot.isCharging)
    }

    func testNotChargingSnapshot() {
        let snapshot = CapabilitySnapshot(
            foundationModelsAvailable: false,
            appleIntelligenceEnabled: false,
            foundationModelsLocaleSupported: false,
            thermalState: .nominal,
            isLowPowerMode: false,
            isCharging: false,
            backgroundProcessingSupported: true,
            availableDiskSpaceBytes: 1_000_000,
            capturedAt: .now
        )
        XCTAssertFalse(snapshot.isCharging)
    }
}
```

**Step 2: Run test to verify it fails**

Run: `xcodegen generate && xcodebuild test -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' -only-testing PlayheadTests/CapabilitySnapshotTests 2>&1 | tail -20`
Expected: FAIL — `isCharging` parameter does not exist on `CapabilitySnapshot`

**Step 3: Add `isCharging` to CapabilitySnapshot**

In `Playhead/Services/Capabilities/CapabilitySnapshot.swift`, add after `isLowPowerMode`:

```swift
/// Whether the device is currently charging or full.
let isCharging: Bool
```

**Step 4: Update `captureSnapshot()` in CapabilitiesService**

In `Playhead/Services/Capabilities/CapabilitiesService.swift`, in `captureSnapshot()` (line ~108), add charging detection:

```swift
private static func captureSnapshot() -> CapabilitySnapshot {
    let processInfo = ProcessInfo.processInfo
    let device = UIDevice.current
    device.isBatteryMonitoringEnabled = true

    // ... existing capability checks ...

    let isCharging = device.batteryState == .charging || device.batteryState == .full

    return CapabilitySnapshot(
        // ... existing fields ...
        isLowPowerMode: isLowPowerMode,
        isCharging: isCharging,
        // ... rest ...
    )
}
```

Add `import UIKit` at top of CapabilitiesService.swift if not present.

**Step 5: Observe battery state changes in `startObserving()`**

In `startObserving()` (line ~78), add battery state observer:

```swift
let batteryToken = center.addObserver(
    forName: UIDevice.batteryStateDidChangeNotification,
    object: nil,
    queue: .main
) { [weak self] _ in
    guard let self else { return }
    Task { await self.refreshSnapshot() }
}

observerTokens = [thermalToken, powerToken, batteryToken]
```

**Step 6: Fix all existing call sites that construct CapabilitySnapshot**

Search for all places that construct `CapabilitySnapshot(` and add the `isCharging:` parameter. The main site is `captureSnapshot()` which is already handled. Check tests for any manual construction.

**Step 7: Run test to verify it passes**

Run: `xcodegen generate && xcodebuild test -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' -only-testing PlayheadTests/CapabilitySnapshotTests 2>&1 | tail -20`
Expected: PASS

**Step 8: Commit**

```bash
git add Playhead/Services/Capabilities/CapabilitySnapshot.swift \
       Playhead/Services/Capabilities/CapabilitiesService.swift \
       PlayheadTests/Services/Capabilities/CapabilitySnapshotTests.swift
git commit -m "feat: add charging state to CapabilitySnapshot"
```

---

### Task 2: Add `pre_analysis_records` table to AnalysisStore

The `PreAnalysisScheduler` needs persistent state to survive app restarts and track tier progress per episode.

**Files:**
- Modify: `Playhead/Persistence/AnalysisStore/AnalysisStore.swift:219-250` (createTables)
- Modify: `Playhead/Persistence/AnalysisStore/AnalysisStore.swift` (add row type + CRUD)
- Test: `PlayheadTests/Persistence/PreAnalysisRecordStoreTests.swift`

**Step 1: Write the failing test**

```swift
// PlayheadTests/Persistence/PreAnalysisRecordStoreTests.swift
import XCTest
@testable import Playhead

final class PreAnalysisRecordStoreTests: XCTestCase {
    var store: AnalysisStore!

    override func setUp() async throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        store = try AnalysisStore(directory: dir)
        try await store.migrate()
    }

    func testInsertAndFetchPreAnalysisRecord() async throws {
        let record = PreAnalysisRecord(
            episodeId: "ep-1",
            podcastId: "pod-1",
            audioURL: "/tmp/ep-1.audio",
            isExplicitDownload: true,
            currentTier: 0,
            coverageAchieved: 0,
            status: "pending"
        )
        try await store.insertPreAnalysisRecord(record)

        let fetched = try await store.fetchPreAnalysisRecord(episodeId: "ep-1")
        XCTAssertNotNil(fetched)
        XCTAssertEqual(fetched?.episodeId, "ep-1")
        XCTAssertEqual(fetched?.status, "pending")
        XCTAssertEqual(fetched?.currentTier, 0)
    }

    func testUpdateTierAndCoverage() async throws {
        let record = PreAnalysisRecord(
            episodeId: "ep-2",
            podcastId: "pod-1",
            audioURL: "/tmp/ep-2.audio",
            isExplicitDownload: false,
            currentTier: 0,
            coverageAchieved: 0,
            status: "pending"
        )
        try await store.insertPreAnalysisRecord(record)
        try await store.updatePreAnalysisProgress(
            episodeId: "ep-2",
            currentTier: 1,
            coverageAchieved: 90.0,
            status: "paused"
        )

        let fetched = try await store.fetchPreAnalysisRecord(episodeId: "ep-2")
        XCTAssertEqual(fetched?.currentTier, 1)
        XCTAssertEqual(fetched?.coverageAchieved, 90.0)
        XCTAssertEqual(fetched?.status, "paused")
    }

    func testFetchPendingOrPausedRecords() async throws {
        for (i, status) in ["pending", "paused", "complete", "superseded"].enumerated() {
            let record = PreAnalysisRecord(
                episodeId: "ep-\(i)",
                podcastId: "pod-1",
                audioURL: "/tmp/ep-\(i).audio",
                isExplicitDownload: i == 0,
                currentTier: 0,
                coverageAchieved: 0,
                status: status
            )
            try await store.insertPreAnalysisRecord(record)
        }

        let actionable = try await store.fetchActionablePreAnalysisRecords()
        XCTAssertEqual(actionable.count, 2) // pending + paused
        // Explicit downloads come first
        XCTAssertTrue(actionable[0].isExplicitDownload)
    }

    func testDeletePreAnalysisRecord() async throws {
        let record = PreAnalysisRecord(
            episodeId: "ep-del",
            podcastId: nil,
            audioURL: "/tmp/ep-del.audio",
            isExplicitDownload: false,
            currentTier: 0,
            coverageAchieved: 0,
            status: "pending"
        )
        try await store.insertPreAnalysisRecord(record)
        try await store.deletePreAnalysisRecord(episodeId: "ep-del")

        let fetched = try await store.fetchPreAnalysisRecord(episodeId: "ep-del")
        XCTAssertNil(fetched)
    }
}
```

**Step 2: Run test to verify it fails**

Run: `xcodegen generate && xcodebuild test -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' -only-testing PlayheadTests/PreAnalysisRecordStoreTests 2>&1 | tail -20`
Expected: FAIL — `PreAnalysisRecord` type does not exist

**Step 3: Add the row type**

Add to `Playhead/Persistence/AnalysisStore/AnalysisStore.swift` near the other row types (after line ~37):

```swift
struct PreAnalysisRecord: Sendable {
    let episodeId: String
    let podcastId: String?
    let audioURL: String
    let isExplicitDownload: Bool
    let currentTier: Int
    let coverageAchieved: Double
    let status: String // pending | active | paused | complete | superseded
}
```

**Step 4: Add the DDL**

In `createTables()` (after the podcast_profiles table), add:

```swift
// pre_analysis_records
try exec("""
    CREATE TABLE IF NOT EXISTS pre_analysis_records (
        episodeId           TEXT PRIMARY KEY,
        podcastId           TEXT,
        audioURL            TEXT NOT NULL,
        isExplicitDownload  INTEGER NOT NULL DEFAULT 0,
        currentTier         INTEGER NOT NULL DEFAULT 0,
        coverageAchieved    REAL NOT NULL DEFAULT 0,
        status              TEXT NOT NULL DEFAULT 'pending',
        createdAt           REAL NOT NULL DEFAULT (strftime('%s', 'now')),
        updatedAt           REAL NOT NULL DEFAULT (strftime('%s', 'now'))
    )
    """)
```

**Step 5: Add CRUD methods**

Add to AnalysisStore (in a new MARK section):

```swift
// MARK: - Pre-Analysis Records

func insertPreAnalysisRecord(_ record: PreAnalysisRecord) throws {
    let sql = """
        INSERT OR REPLACE INTO pre_analysis_records
        (episodeId, podcastId, audioURL, isExplicitDownload, currentTier, coverageAchieved, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
    try execBind(sql, bindings: [
        .text(record.episodeId),
        record.podcastId.map { .text($0) } ?? .null,
        .text(record.audioURL),
        .int(record.isExplicitDownload ? 1 : 0),
        .int(record.currentTier),
        .real(record.coverageAchieved),
        .text(record.status),
    ])
}

func fetchPreAnalysisRecord(episodeId: String) throws -> PreAnalysisRecord? {
    let sql = "SELECT * FROM pre_analysis_records WHERE episodeId = ?"
    return try queryOne(sql, bindings: [.text(episodeId)]) { row in
        PreAnalysisRecord(
            episodeId: row.text("episodeId"),
            podcastId: row.optionalText("podcastId"),
            audioURL: row.text("audioURL"),
            isExplicitDownload: row.int("isExplicitDownload") != 0,
            currentTier: row.int("currentTier"),
            coverageAchieved: row.real("coverageAchieved"),
            status: row.text("status")
        )
    }
}

func fetchActionablePreAnalysisRecords() throws -> [PreAnalysisRecord] {
    let sql = """
        SELECT * FROM pre_analysis_records
        WHERE status IN ('pending', 'paused')
        ORDER BY isExplicitDownload DESC, createdAt ASC
        """
    return try queryAll(sql) { row in
        PreAnalysisRecord(
            episodeId: row.text("episodeId"),
            podcastId: row.optionalText("podcastId"),
            audioURL: row.text("audioURL"),
            isExplicitDownload: row.int("isExplicitDownload") != 0,
            currentTier: row.int("currentTier"),
            coverageAchieved: row.real("coverageAchieved"),
            status: row.text("status")
        )
    }
}

func updatePreAnalysisProgress(
    episodeId: String,
    currentTier: Int,
    coverageAchieved: Double,
    status: String
) throws {
    let sql = """
        UPDATE pre_analysis_records
        SET currentTier = ?, coverageAchieved = ?, status = ?,
            updatedAt = strftime('%s', 'now')
        WHERE episodeId = ?
        """
    try execBind(sql, bindings: [
        .int(currentTier),
        .real(coverageAchieved),
        .text(status),
        .text(episodeId),
    ])
}

func deletePreAnalysisRecord(episodeId: String) throws {
    try execBind(
        "DELETE FROM pre_analysis_records WHERE episodeId = ?",
        bindings: [.text(episodeId)]
    )
}
```

**Step 6: Run tests to verify they pass**

Run: `xcodegen generate && xcodebuild test -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' -only-testing PlayheadTests/PreAnalysisRecordStoreTests 2>&1 | tail -20`
Expected: PASS

**Step 7: Commit**

```bash
git add Playhead/Persistence/AnalysisStore/AnalysisStore.swift \
       PlayheadTests/Persistence/PreAnalysisRecordStoreTests.swift
git commit -m "feat: add pre_analysis_records table to AnalysisStore"
```

---

### Task 3: Add `preAnalyze()` entry point to AnalysisCoordinator

New method that runs the pipeline from 0s to a coverage target, with no playback snapshot dependency and lower QoS.

**Files:**
- Modify: `Playhead/Services/AnalysisCoordinator/AnalysisCoordinator.swift:105-170` (add method)
- Test: `PlayheadTests/Services/AnalysisCoordinator/PreAnalyzeTests.swift`

**Step 1: Write the failing test**

```swift
// PlayheadTests/Services/AnalysisCoordinator/PreAnalyzeTests.swift
import XCTest
@testable import Playhead

final class PreAnalyzeTests: XCTestCase {
    func testPreAnalyzeCreatesSessionAndAsset() async throws {
        // Use the test fixture helpers to build a coordinator with
        // stubbed dependencies (same pattern as existing coordinator tests).
        let store = try AnalysisStore(
            directory: FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString, isDirectory: true)
        )
        try await store.migrate()

        let coordinator = AnalysisCoordinator(
            store: store,
            audioService: StubAnalysisAudioService(),
            featureService: StubFeatureExtractionService(store: store),
            transcriptEngine: StubTranscriptEngineService(store: store),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: StubAdDetectionService(store: store),
            skipOrchestrator: SkipOrchestrator(store: store)
        )

        let assetId = await coordinator.preAnalyze(
            episodeId: "test-ep",
            podcastId: "test-pod",
            audioURL: URL(fileURLWithPath: "/tmp/test.audio"),
            coverageTarget: 90.0
        )

        XCTAssertNotNil(assetId)

        // Verify session was created
        let asset = try await store.fetchAssetByEpisodeId("test-ep")
        XCTAssertNotNil(asset)
    }
}
```

Note: Stub services should be created following the pattern of any existing test stubs in the project. If none exist, create minimal protocol-conforming stubs that return empty results.

**Step 2: Run test to verify it fails**

Expected: FAIL — `preAnalyze` method does not exist

**Step 3: Add `preAnalyze()` to AnalysisCoordinator**

In `Playhead/Services/AnalysisCoordinator/AnalysisCoordinator.swift`, add after `stop()` (line ~200):

```swift
// MARK: - Pre-Analysis (Download-Time)

/// Pre-analyze the first `coverageTarget` seconds of an episode's audio.
/// Called at download completion to pre-compute ad detection results.
/// Returns the analysis asset ID, or nil on failure.
///
/// Differences from playback-driven analysis:
/// - Synthetic playback snapshot (playheadTime: 0, linear processing)
/// - Stops when coverage watermarks reach `coverageTarget`
/// - Does not push skip cues (no active playback)
/// - Lower QoS (.utility)
/// - Yields immediately if playback starts (cancels pipelineTask)
func preAnalyze(
    episodeId: String,
    podcastId: String?,
    audioURL: URL,
    coverageTarget: TimeInterval
) async -> String? {
    // Don't pre-analyze if playback is active — coordinator is busy.
    guard activeEpisodeId == nil else {
        logger.info("Pre-analysis skipped: playback active")
        return nil
    }

    let syntheticSnapshot = PlaybackSnapshot(
        playheadTime: 0,
        playbackRate: 1.0,
        isPlaying: false
    )

    do {
        let (sessionId, assetId, resumeState) = try await resolveSession(
            episodeId: episodeId,
            audioURL: audioURL
        )

        // Check if already at or past coverage target.
        if let asset = try? await store.fetchAsset(id: assetId) {
            let existingCoverage = min(
                asset.featureCoverageEndTime ?? 0,
                asset.fastTranscriptCoverageEndTime ?? 0
            )
            if existingCoverage >= coverageTarget {
                logger.info("Pre-analysis: coverage \(existingCoverage)s >= target \(coverageTarget)s, skipping")
                return assetId
            }
        }

        // Run the pipeline up to hotPathReady, capped by coverageTarget.
        activeAssetId = assetId
        activeEpisodeId = episodeId
        activePodcastId = podcastId
        latestSnapshot = syntheticSnapshot

        switch resumeState {
        case .queued:
            try await runFromQueued(
                sessionId: sessionId,
                assetId: assetId,
                episodeId: episodeId,
                audioURL: audioURL
            )
        case .spooling:
            try await runFromSpooling(
                sessionId: sessionId,
                assetId: assetId,
                episodeId: episodeId,
                audioURL: audioURL
            )
        case .featuresReady:
            try await runFromFeaturesReady(
                sessionId: sessionId,
                assetId: assetId
            )
        case .hotPathReady, .backfill, .complete:
            logger.info("Pre-analysis: session already at \(resumeState.rawValue)")
        case .failed:
            try await transition(sessionId: sessionId, assetId: assetId, to: .queued)
            try await runFromQueued(
                sessionId: sessionId,
                assetId: assetId,
                episodeId: episodeId,
                audioURL: audioURL
            )
        }

        // Clear active state so playback isn't blocked.
        activeEpisodeId = nil
        activeAssetId = nil
        activePodcastId = nil
        activeShards = nil
        latestSnapshot = nil

        return assetId
    } catch is CancellationError {
        logger.info("Pre-analysis cancelled for \(episodeId)")
        clearActiveState()
        return nil
    } catch {
        logger.error("Pre-analysis failed for \(episodeId): \(error)")
        clearActiveState()
        return nil
    }
}

private func clearActiveState() {
    activeEpisodeId = nil
    activeAssetId = nil
    activePodcastId = nil
    activeShards = nil
    latestSnapshot = nil
}
```

**Step 4: Add coverage cap to shard filtering in TranscriptEngineService**

In `TranscriptEngineService.swift`, modify `prioritizeShards()` (line ~357) to respect the coverage target. The existing `existingCoverage` filter already skips processed shards, and the `runTranscriptionLoop` already checks `shardEnd <= existingCoverage`. The synthetic snapshot with `playheadTime: 0` and `isPlaying: false` will cause `prioritizeShards` to return shards in natural order (the `guard let snapshot = latestSnapshot else { return shards }` path at line ~361).

The coverage cap is enforced by `featureCoverageEndTime` and `fastTranscriptCoverageEndTime` watermarks — the coordinator stops the pipeline once the session transitions to `hotPathReady`. No changes needed to TranscriptEngineService for basic tier support.

**Step 5: Run test to verify it passes**

Run: `xcodegen generate && xcodebuild test -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' -only-testing PlayheadTests/PreAnalyzeTests 2>&1 | tail -20`
Expected: PASS

**Step 6: Commit**

```bash
git add Playhead/Services/AnalysisCoordinator/AnalysisCoordinator.swift \
       PlayheadTests/Services/AnalysisCoordinator/PreAnalyzeTests.swift
git commit -m "feat: add preAnalyze() entry point to AnalysisCoordinator"
```

---

### Task 4: Create PreAnalysisScheduler actor

New actor that owns tier progression, charging observation, and the work queue.

**Files:**
- Create: `Playhead/Services/PreAnalysis/PreAnalysisScheduler.swift`
- Test: `PlayheadTests/Services/PreAnalysis/PreAnalysisSchedulerTests.swift`

**Step 1: Write the failing test**

```swift
// PlayheadTests/Services/PreAnalysis/PreAnalysisSchedulerTests.swift
import XCTest
@testable import Playhead

final class PreAnalysisSchedulerTests: XCTestCase {
    func testEnqueueCreatesRecord() async throws {
        let store = try AnalysisStore(
            directory: FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString, isDirectory: true)
        )
        try await store.migrate()

        let coordinator = AnalysisCoordinator(
            store: store,
            audioService: StubAnalysisAudioService(),
            featureService: StubFeatureExtractionService(store: store),
            transcriptEngine: StubTranscriptEngineService(store: store),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: StubAdDetectionService(store: store),
            skipOrchestrator: SkipOrchestrator(store: store)
        )

        let scheduler = PreAnalysisScheduler(
            store: store,
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService()
        )

        await scheduler.enqueue(
            episodeId: "ep-1",
            podcastId: "pod-1",
            audioURL: URL(fileURLWithPath: "/tmp/ep-1.audio"),
            isExplicitDownload: true
        )

        let record = try await store.fetchPreAnalysisRecord(episodeId: "ep-1")
        XCTAssertNotNil(record)
        XCTAssertEqual(record?.status, "pending")
    }

    func testSupersededOnPlayback() async throws {
        let store = try AnalysisStore(
            directory: FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString, isDirectory: true)
        )
        try await store.migrate()

        let coordinator = AnalysisCoordinator(
            store: store,
            audioService: StubAnalysisAudioService(),
            featureService: StubFeatureExtractionService(store: store),
            transcriptEngine: StubTranscriptEngineService(store: store),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: StubAdDetectionService(store: store),
            skipOrchestrator: SkipOrchestrator(store: store)
        )

        let scheduler = PreAnalysisScheduler(
            store: store,
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService()
        )

        // Enqueue and then signal playback started.
        await scheduler.enqueue(
            episodeId: "ep-1",
            podcastId: "pod-1",
            audioURL: URL(fileURLWithPath: "/tmp/ep-1.audio"),
            isExplicitDownload: true
        )
        await scheduler.playbackStarted(episodeId: "ep-1")

        let record = try await store.fetchPreAnalysisRecord(episodeId: "ep-1")
        XCTAssertEqual(record?.status, "superseded")
    }
}
```

**Step 2: Run test to verify it fails**

Expected: FAIL — `PreAnalysisScheduler` does not exist

**Step 3: Implement PreAnalysisScheduler**

Create `Playhead/Services/PreAnalysis/PreAnalysisScheduler.swift`:

```swift
// PreAnalysisScheduler.swift
// Manages progressive pre-analysis of downloaded podcast episodes.
//
// Tier model:
//   T0 (90s)  — always, immediately on download
//   T1 (3min) — charging only
//   T2 (5min) — charging only
//   T3 (10min) — charging only
//
// One episode at a time. Explicit downloads get priority.
// Cancels on playback start (coordinator takes over).

import Foundation
import OSLog

actor PreAnalysisScheduler {

    private let logger = Logger(subsystem: "com.playhead", category: "PreAnalysisScheduler")

    // MARK: - Configuration

    static let tierTargets: [TimeInterval] = [90, 180, 300, 600]

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let coordinator: AnalysisCoordinator
    private let capabilitiesService: CapabilitiesService

    // MARK: - State

    private var activeTask: Task<Void, Never>?
    private var chargingObserverTask: Task<Void, Never>?
    private var activeEpisodeId: String?

    // MARK: - Init

    init(
        store: AnalysisStore,
        coordinator: AnalysisCoordinator,
        capabilitiesService: CapabilitiesService
    ) {
        self.store = store
        self.coordinator = coordinator
        self.capabilitiesService = capabilitiesService
    }

    // MARK: - Public API

    /// Enqueue an episode for pre-analysis. Called by DownloadManager
    /// on download completion.
    func enqueue(
        episodeId: String,
        podcastId: String?,
        audioURL: URL,
        isExplicitDownload: Bool
    ) {
        let record = PreAnalysisRecord(
            episodeId: episodeId,
            podcastId: podcastId,
            audioURL: audioURL.path,
            isExplicitDownload: isExplicitDownload,
            currentTier: 0,
            coverageAchieved: 0,
            status: "pending"
        )
        do {
            try store.insertPreAnalysisRecord(record)
            logger.info("Enqueued pre-analysis for \(episodeId) (explicit=\(isExplicitDownload))")
        } catch {
            logger.error("Failed to enqueue pre-analysis for \(episodeId): \(error)")
            return
        }

        processNextIfIdle()
    }

    /// Signal that playback started for an episode. Cancels pre-analysis
    /// if in-flight, marks record as superseded.
    func playbackStarted(episodeId: String) {
        if activeEpisodeId == episodeId {
            activeTask?.cancel()
            activeTask = nil
            activeEpisodeId = nil
        }

        do {
            try store.updatePreAnalysisProgress(
                episodeId: episodeId,
                currentTier: -1, // unchanged, but we need to pass something
                coverageAchieved: -1,
                status: "superseded"
            )
        } catch {
            // Best-effort status update.
        }
    }

    /// Signal that an episode was deleted. Cancels work and removes record.
    func episodeDeleted(episodeId: String) {
        if activeEpisodeId == episodeId {
            activeTask?.cancel()
            activeTask = nil
            activeEpisodeId = nil
        }

        do {
            try store.deletePreAnalysisRecord(episodeId: episodeId)
        } catch {
            logger.error("Failed to delete pre-analysis record for \(episodeId): \(error)")
        }
    }

    /// Start observing charging state for tier advancement.
    /// Call once at app launch.
    func startObservingCharging() {
        chargingObserverTask?.cancel()
        chargingObserverTask = Task { [weak self] in
            guard let self else { return }
            let updates = await self.capabilitiesService.capabilityUpdates()
            var wasCharging = false
            for await snapshot in updates {
                guard !Task.isCancelled else { break }
                if snapshot.isCharging && !wasCharging {
                    await self.processNextIfIdle()
                }
                wasCharging = snapshot.isCharging
            }
        }
    }

    /// Resume any pending work at app launch. Picks up episodes that
    /// were downloaded while the app was suspended, or paused mid-tier.
    func resumeOnLaunch() {
        processNextIfIdle()
    }

    // MARK: - Processing

    private func processNextIfIdle() {
        guard activeTask == nil else { return }

        activeTask = Task { [weak self] in
            guard let self else { return }
            await self.processQueue()
            await self.clearActiveTask()
        }
    }

    private func clearActiveTask() {
        activeTask = nil
        activeEpisodeId = nil
    }

    private func processQueue() async {
        while !Task.isCancelled {
            let record: PreAnalysisRecord?
            do {
                let records = try store.fetchActionablePreAnalysisRecords()
                record = records.first
            } catch {
                logger.error("Failed to fetch pre-analysis queue: \(error)")
                return
            }

            guard let record else { return }

            let tierTarget = tierTargetForRecord(record)
            guard let tierTarget else {
                // All tiers complete.
                do {
                    try store.updatePreAnalysisProgress(
                        episodeId: record.episodeId,
                        currentTier: record.currentTier,
                        coverageAchieved: record.coverageAchieved,
                        status: "complete"
                    )
                } catch {}
                continue
            }

            // T1+ require charging.
            if record.currentTier > 0 || record.coverageAchieved >= Self.tierTargets[0] {
                let snapshot = await capabilitiesService.currentSnapshot
                if !snapshot.isCharging {
                    logger.info("Pre-analysis paused: not charging (tier \(record.currentTier))")
                    return
                }

                // Also respect thermal throttle.
                if snapshot.shouldThrottleAnalysis {
                    logger.info("Pre-analysis paused: thermal throttle")
                    return
                }
            }

            activeEpisodeId = record.episodeId

            do {
                try store.updatePreAnalysisProgress(
                    episodeId: record.episodeId,
                    currentTier: record.currentTier,
                    coverageAchieved: record.coverageAchieved,
                    status: "active"
                )
            } catch {}

            let audioURL = URL(fileURLWithPath: record.audioURL)
            let assetId = await coordinator.preAnalyze(
                episodeId: record.episodeId,
                podcastId: record.podcastId,
                audioURL: audioURL,
                coverageTarget: tierTarget
            )

            guard !Task.isCancelled else { return }

            let nextTier = record.currentTier + 1
            let newStatus: String
            if nextTier >= Self.tierTargets.count {
                newStatus = "complete"
            } else {
                newStatus = "paused" // Wait for next tier conditions
            }

            do {
                try store.updatePreAnalysisProgress(
                    episodeId: record.episodeId,
                    currentTier: nextTier,
                    coverageAchieved: tierTarget,
                    status: newStatus
                )
            } catch {
                logger.error("Failed to update pre-analysis progress: \(error)")
            }

            if assetId != nil {
                logger.info("Pre-analysis tier \(record.currentTier) complete for \(record.episodeId): \(tierTarget)s")
            }

            activeEpisodeId = nil
        }
    }

    private func tierTargetForRecord(_ record: PreAnalysisRecord) -> TimeInterval? {
        let tier = record.currentTier
        guard tier < Self.tierTargets.count else { return nil }
        return Self.tierTargets[tier]
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `xcodegen generate && xcodebuild test -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' -only-testing PlayheadTests/PreAnalysisSchedulerTests 2>&1 | tail -20`
Expected: PASS

**Step 5: Commit**

```bash
git add Playhead/Services/PreAnalysis/PreAnalysisScheduler.swift \
       PlayheadTests/Services/PreAnalysis/PreAnalysisSchedulerTests.swift
git commit -m "feat: add PreAnalysisScheduler for tiered download-time analysis"
```

---

### Task 5: Add DownloadContext and wire DownloadManager to PreAnalysisScheduler

**Files:**
- Modify: `Playhead/Services/Downloads/DownloadManager.swift:214-245` (progressiveDownload)
- Modify: `Playhead/Services/Downloads/DownloadManager.swift:249-308` (performDownload)
- Test: `PlayheadTests/Services/Downloads/DownloadManagerPreAnalysisTests.swift`

**Step 1: Write the failing test**

```swift
// PlayheadTests/Services/Downloads/DownloadManagerPreAnalysisTests.swift
import XCTest
@testable import Playhead

final class DownloadManagerPreAnalysisTests: XCTestCase {
    func testDownloadContextPassedToScheduler() async throws {
        let context = DownloadContext(podcastId: "pod-1", isExplicitDownload: true)
        XCTAssertEqual(context.podcastId, "pod-1")
        XCTAssertTrue(context.isExplicitDownload)
    }
}
```

**Step 2: Run test to verify it fails**

Expected: FAIL — `DownloadContext` does not exist

**Step 3: Add DownloadContext struct**

In `Playhead/Services/Downloads/DownloadManager.swift`, add near the top (after `HTTPAssetMetadata`):

```swift
/// Context passed alongside a download request for downstream use.
struct DownloadContext: Sendable {
    let podcastId: String?
    let isExplicitDownload: Bool
}
```

**Step 4: Add scheduler dependency to DownloadManager**

Add a weak reference to avoid retain cycles:

```swift
// In DownloadManager actor, add property:
private var preAnalysisScheduler: PreAnalysisScheduler?

// Add setter:
func setPreAnalysisScheduler(_ scheduler: PreAnalysisScheduler) {
    preAnalysisScheduler = scheduler
}
```

**Step 5: Update `progressiveDownload` and `performDownload` signatures**

Add optional `context` parameter:

```swift
func progressiveDownload(
    episodeId: String,
    from url: URL,
    context: DownloadContext? = nil
) async throws -> URL {
    // ... existing logic ...
}
```

In `performDownload`, after fingerprint computed (line ~296), add notification:

```swift
// Notify pre-analysis scheduler.
if let scheduler = preAnalysisScheduler, let context {
    await scheduler.enqueue(
        episodeId: episodeId,
        podcastId: context.podcastId,
        audioURL: completeURL,
        isExplicitDownload: context.isExplicitDownload
    )
}
```

**Step 6: Run tests**

Run: `xcodegen generate && xcodebuild test -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' -only-testing PlayheadTests/DownloadManagerPreAnalysisTests 2>&1 | tail -20`
Expected: PASS

**Step 7: Commit**

```bash
git add Playhead/Services/Downloads/DownloadManager.swift \
       PlayheadTests/Services/Downloads/DownloadManagerPreAnalysisTests.swift
git commit -m "feat: wire DownloadManager to PreAnalysisScheduler via DownloadContext"
```

---

### Task 6: Load pre-computed AdWindows in SkipOrchestrator.beginEpisode()

This is the payoff — pre-roll skip cues ready at play-start.

**Files:**
- Modify: `Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift:219-244`
- Test: `PlayheadTests/Services/SkipOrchestrator/SkipOrchestratorPreloadTests.swift`

**Step 1: Write the failing test**

```swift
// PlayheadTests/Services/SkipOrchestrator/SkipOrchestratorPreloadTests.swift
import XCTest
@testable import Playhead

final class SkipOrchestratorPreloadTests: XCTestCase {
    func testBeginEpisodeLoadsExistingAdWindows() async throws {
        let store = try AnalysisStore(
            directory: FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString, isDirectory: true)
        )
        try await store.migrate()

        // Insert a pre-computed ad window (simulating pre-analysis output).
        let asset = AnalysisAsset(
            id: "asset-1",
            episodeId: "ep-1",
            assetFingerprint: "fp-1",
            weakFingerprint: nil,
            sourceURL: "/tmp/test.audio",
            featureCoverageEndTime: 90,
            fastTranscriptCoverageEndTime: 90,
            confirmedAdCoverageEndTime: nil,
            analysisState: "hotPathReady",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)

        let adWindow = AdWindow(
            id: "ad-1",
            analysisAssetId: "asset-1",
            startTime: 0,
            endTime: 45,
            confidence: 0.82,
            boundaryState: "acousticRefined",
            decisionState: "candidate",
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: "brought to you by",
            evidenceStartTime: 2.0,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
        try await store.insertAdWindows([adWindow])

        let orchestrator = SkipOrchestrator(store: store)
        await orchestrator.beginEpisode(analysisAssetId: "asset-1")

        // The window should be loaded and available.
        let confirmed = await orchestrator.confirmedWindows()
        // Window may be confirmed or still candidate depending on trust mode,
        // but it should be tracked.
        let log = await orchestrator.getDecisionLog()
        XCTAssertFalse(log.isEmpty, "Pre-computed ad window should be processed on beginEpisode")
    }
}
```

**Step 2: Run test to verify it fails**

Expected: FAIL — `beginEpisode` does not load existing AdWindows

**Step 3: Modify `beginEpisode()` to load existing AdWindows**

In `Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift`, at the end of `beginEpisode()` (after loading feature windows, around line 243):

```swift
// Load pre-computed AdWindows from pre-analysis or prior sessions.
do {
    let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
    if !existingWindows.isEmpty {
        await receiveAdWindows(existingWindows)
        logger.info("Loaded \(existingWindows.count) pre-computed AdWindows for asset \(analysisAssetId)")
    }
} catch {
    logger.warning("Failed to load pre-computed AdWindows: \(error.localizedDescription)")
}
```

Note: `beginEpisode` is already `async` (it awaits `trustService`), and `receiveAdWindows` is on the same actor, so calling it directly works — but since we're already inside the actor, call it without `await`:

Actually, looking at the code more carefully, `receiveAdWindows` is defined on the same actor, so within `beginEpisode` we call it directly. But it's declared `func receiveAdWindows` (not `private`), so the self-call within the actor doesn't need await. Double-check: if `receiveAdWindows` has awaits inside it, actor reentrancy means we do need await for any suspension points. The method itself doesn't await, it just does synchronous state updates + `evaluateAndPush()`. So no await needed.

**Step 4: Run test to verify it passes**

Run: `xcodegen generate && xcodebuild test -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' -only-testing PlayheadTests/SkipOrchestratorPreloadTests 2>&1 | tail -20`
Expected: PASS

**Step 5: Commit**

```bash
git add Playhead/Services/SkipOrchestrator/SkipOrchestrator.swift \
       PlayheadTests/Services/SkipOrchestrator/SkipOrchestratorPreloadTests.swift
git commit -m "feat: load pre-computed AdWindows on beginEpisode"
```

---

### Task 7: Wire PreAnalysisScheduler into PlayheadRuntime

Connect everything in the composition root.

**Files:**
- Modify: `Playhead/App/PlayheadRuntime.swift:9-189`
- No new test file (integration verified by existing tests + manual)

**Step 1: Add property to PlayheadRuntime**

In `PlayheadRuntime.swift`, add after `backgroundProcessingService` (line 24):

```swift
let downloadManager: DownloadManager
let preAnalysisScheduler: PreAnalysisScheduler
```

**Step 2: Initialize in `init()`**

After `backgroundProcessingService` init (line 112), add:

```swift
let downloadManager = DownloadManager()
self.downloadManager = downloadManager

self.preAnalysisScheduler = PreAnalysisScheduler(
    store: analysisStore,
    coordinator: analysisCoordinator,
    capabilitiesService: capabilitiesService
)
```

**Step 3: Wire DownloadManager to scheduler**

In the launch Task (after `backgroundProcessingService.start()`, line 185):

```swift
do {
    try await downloadManager.bootstrap()
} catch {
    // Download cache is best-effort at launch.
}
await downloadManager.setPreAnalysisScheduler(preAnalysisScheduler)
await preAnalysisScheduler.startObservingCharging()
await preAnalysisScheduler.resumeOnLaunch()
```

**Step 4: Notify scheduler on playback start**

In `playEpisode()` (around line 224), after `skipOrchestrator.beginEpisode()`:

```swift
await preAnalysisScheduler.playbackStarted(episodeId: episodeId)
```

**Step 5: Build and verify**

Run: `xcodegen generate && xcodebuild build -scheme Playhead -destination 'platform=iOS Simulator,name=iPhone 16 Pro' 2>&1 | tail -20`
Expected: BUILD SUCCEEDED

**Step 6: Commit**

```bash
git add Playhead/App/PlayheadRuntime.swift
git commit -m "feat: wire PreAnalysisScheduler into PlayheadRuntime composition root"
```

---

### Task 8: Startup sweep for un-enqueued downloaded episodes

Handle background download completions and existing users upgrading.

**Files:**
- Modify: `Playhead/Services/PreAnalysis/PreAnalysisScheduler.swift`
- Test: `PlayheadTests/Services/PreAnalysis/PreAnalysisSchedulerStartupTests.swift`

**Step 1: Write the failing test**

```swift
// PlayheadTests/Services/PreAnalysis/PreAnalysisSchedulerStartupTests.swift
import XCTest
@testable import Playhead

final class PreAnalysisSchedulerStartupTests: XCTestCase {
    func testResumeOnLaunchPicksUpPausedRecords() async throws {
        let store = try AnalysisStore(
            directory: FileManager.default.temporaryDirectory
                .appendingPathComponent(UUID().uuidString, isDirectory: true)
        )
        try await store.migrate()

        // Simulate a record left in "paused" state from previous launch.
        let record = PreAnalysisRecord(
            episodeId: "ep-paused",
            podcastId: "pod-1",
            audioURL: "/tmp/ep-paused.audio",
            isExplicitDownload: true,
            currentTier: 1,
            coverageAchieved: 90.0,
            status: "paused"
        )
        try store.insertPreAnalysisRecord(record)

        let coordinator = AnalysisCoordinator(
            store: store,
            audioService: StubAnalysisAudioService(),
            featureService: StubFeatureExtractionService(store: store),
            transcriptEngine: StubTranscriptEngineService(store: store),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: StubAdDetectionService(store: store),
            skipOrchestrator: SkipOrchestrator(store: store)
        )

        let scheduler = PreAnalysisScheduler(
            store: store,
            coordinator: coordinator,
            capabilitiesService: CapabilitiesService()
        )

        // resumeOnLaunch should pick up the paused record.
        await scheduler.resumeOnLaunch()

        // Give the async work a moment to start.
        try await Task.sleep(for: .milliseconds(100))

        let fetched = try store.fetchPreAnalysisRecord(episodeId: "ep-paused")
        // Should be active or beyond paused (processing started).
        XCTAssertNotEqual(fetched?.status, "paused")
    }
}
```

**Step 2: Run test to verify behavior**

The `resumeOnLaunch()` already calls `processNextIfIdle()` which fetches actionable records. This test verifies the end-to-end flow works on launch. If it passes with the existing implementation, we're done. If not, debug.

**Step 3: Commit**

```bash
git add PlayheadTests/Services/PreAnalysis/PreAnalysisSchedulerStartupTests.swift
git commit -m "test: verify pre-analysis scheduler resume on launch"
```

---

## Summary

| Task | What | Key Files |
|------|------|-----------|
| 1 | Add charging state to CapabilitySnapshot | CapabilitySnapshot.swift, CapabilitiesService.swift |
| 2 | Add pre_analysis_records table | AnalysisStore.swift |
| 3 | Add preAnalyze() to AnalysisCoordinator | AnalysisCoordinator.swift |
| 4 | Create PreAnalysisScheduler actor | PreAnalysisScheduler.swift (new) |
| 5 | Wire DownloadManager to scheduler | DownloadManager.swift |
| 6 | Load pre-computed AdWindows on play-start | SkipOrchestrator.swift |
| 7 | Wire into PlayheadRuntime | PlayheadRuntime.swift |
| 8 | Startup sweep for existing downloads | PreAnalysisScheduler tests |

Tasks 1-2 are foundational (no dependencies on each other, can be parallelized). Tasks 3-4 depend on 1-2. Tasks 5-7 depend on 3-4. Task 8 is a verification pass.
