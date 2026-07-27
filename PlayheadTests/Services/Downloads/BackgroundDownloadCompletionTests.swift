// BackgroundDownloadCompletionTests.swift
// playhead-24cm.1: regression coverage for the
// `EpisodeDownloadDelegate` → `DownloadManager` handoff.
//
// I3: file placement must honor `DownloadManager.cacheDirectory` (the
//     pre-fix delegate hardcoded `defaultCacheDirectory()`, which broke
//     custom cache dirs in tests and any future multi-profile host).
// I4: the delegate must populate a non-empty weak fingerprint —
//     previously it injected `AudioFingerprint(weak: "", strong: ...)`
//     which polluted downstream weak-fingerprint dedup.
//
// We drive `handleBackgroundDownloadComplete` directly because the real
// background-session callback requires a live `URLSessionDownloadTask`
// that we cannot construct without a session. The actor method is the
// load-bearing part of both fixes.

import Foundation
import Testing
@testable import Playhead

private actor BackgroundCompletionJournal: WorkJournalRecording {
    private(set) var finalized: [String] = []
    private(set) var failed: [String] = []

    func recordFinalized(episodeId: String) async {
        finalized.append(episodeId)
    }

    func recordFailed(
        episodeId: String,
        cause: InternalMissCause
    ) async {
        failed.append(episodeId)
    }

    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {
        failed.append(episodeId)
    }

    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {}
}

private actor GatedBackgroundCompletionJournal: WorkJournalRecording {
    private var finalized: [String] = []
    private var didStart = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func recordFinalized(episodeId: String) async {
        didStart = true
        let waiters = startWaiters
        startWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
        guard !Task.isCancelled else { return }
        finalized.append(episodeId)
    }

    func recordFailed(
        episodeId: String,
        cause: InternalMissCause
    ) async {}

    func recordFailed(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {}

    func recordPreempted(
        episodeId: String,
        cause: InternalMissCause,
        metadataJSON: String
    ) async {}

    func waitUntilFinalizationStarted() async {
        if didStart { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func releaseFinalization() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }

    func finalizedSnapshot() -> [String] {
        finalized
    }
}

private actor BackgroundEnqueueGate {
    private var didStart = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func wait() async {
        didStart = true
        let waiters = startWaiters
        startWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
    }

    func waitUntilStarted() async {
        if didStart { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

@Suite("DownloadManager – background completion (playhead-24cm.1)")
struct BackgroundDownloadCompletionTests {
    private func makeScheduler(
        store: AnalysisStore
    ) -> AnalysisWorkScheduler {
        let speechService = SpeechService(
            recognizer: StubSpeechRecognizer()
        )
        let runner = AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: speechService,
                store: store
            ),
            adDetection: StubAdDetectionProvider()
        )
        let battery = StubBatteryProvider()
        battery.level = 0.9
        battery.charging = true
        return AnalysisWorkScheduler(
            store: store,
            jobRunner: runner,
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: StubDownloadProvider(),
            batteryProvider: battery,
            transportStatusProvider:
                StubTransportStatusProvider(),
            config: PreAnalysisConfig()
        )
    }

    @Test("identity-qualified staging isolates a retired task from its replacement")
    func identityQualifiedStagingIsIsolated() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let journal = BackgroundCompletionJournal()
        let manager = DownloadManager(
            cacheDirectory: dir,
            workJournalRecorder: journal
        )
        try await manager.bootstrap()
        let delegate = await manager.sessionDelegateForTesting()
        let episodeId = "same-episode-distinct-background-identities"
        let oldIdentity = BackgroundTransferIdentity(
            sessionIdentifier: "legacy-session",
            taskIdentifier: 41
        )
        let newIdentity = BackgroundTransferIdentity(
            sessionIdentifier: "maintenance-session",
            taskIdentifier: 42
        )

        struct Staged {
            let identity: BackgroundTransferIdentity
            let url: URL
            let originalURL: URL?
            let metadata: HTTPAssetMetadata?
        }
        var callbacks: [Staged] = []
        let productionStaged = delegate.onBackgroundDownloadStaged
        delegate.onBackgroundDownloadStaged = {
            identity, _, stagedURL, originalURL, metadata in
            callbacks.append(
                Staged(
                    identity: identity,
                    url: stagedURL,
                    originalURL: originalURL,
                    metadata: metadata
                )
            )
        }

        let oldSource = dir.appendingPathComponent("old-os-location")
        let newSource = dir.appendingPathComponent("new-os-location")
        let oldBytes = Data(repeating: 0xA1, count: 128)
        let newBytes = Data(repeating: 0xB2, count: 128)
        try oldBytes.write(to: oldSource)
        try newBytes.write(to: newSource)
        let originalURL = URL(
            string: "https://example.com/episode.mp3"
        )!
        delegate._stageBackgroundDownloadForTesting(
            identity: oldIdentity,
            episodeId: episodeId,
            location: oldSource,
            originalURL: originalURL
        )
        delegate._stageBackgroundDownloadForTesting(
            identity: newIdentity,
            episodeId: episodeId,
            location: newSource,
            originalURL: originalURL
        )

        #expect(callbacks.count == 2)
        let old = try #require(
            callbacks.first { $0.identity == oldIdentity }
        )
        let new = try #require(
            callbacks.first { $0.identity == newIdentity }
        )
        #expect(old.url != new.url)
        #expect(FileManager.default.fileExists(atPath: old.url.path))
        #expect(FileManager.default.fileExists(atPath: new.url.path))

        await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId,
            identity: oldIdentity
        )
        try await manager.removeCache(for: episodeId)
        await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId,
            identity: newIdentity
        )

        productionStaged?(
            old.identity,
            episodeId,
            old.url,
            old.originalURL,
            old.metadata
        )
        #expect(
            await pollUntil {
                !FileManager.default.fileExists(atPath: old.url.path)
            }
        )
        #expect(
            FileManager.default.fileExists(atPath: new.url.path),
            "retiring the older identity must not delete replacement bytes"
        )

        productionStaged?(
            new.identity,
            episodeId,
            new.url,
            new.originalURL,
            new.metadata
        )
        #expect(
            await pollUntil {
                await manager.cachedFileURL(for: episodeId) != nil
            }
        )
        let placed = try #require(
            await manager.cachedFileURL(for: episodeId)
        )
        #expect(try Data(contentsOf: placed) == newBytes)
        #expect(!FileManager.default.fileExists(atPath: new.url.path))
        #expect(
            await pollUntil {
                await journal.finalized == [episodeId]
            },
            "the staged callback finishes asynchronously after placement"
        )
    }

    @Test("placement and pin failures never emit WorkJournal finalized")
    func failedPlacementAndPinDoNotFinalize() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let journal = BackgroundCompletionJournal()
        let manager = DownloadManager(
            cacheDirectory: dir,
            workJournalRecorder: journal
        )
        try await manager.bootstrap()

        let placementEpisode = "background-placement-failure"
        let placementIdentity = BackgroundTransferIdentity(
            sessionIdentifier: "placement-session",
            taskIdentifier: 51
        )
        await manager._registerBackgroundTransferForTesting(
            episodeId: placementEpisode,
            identity: placementIdentity
        )
        await manager.handleBackgroundDownloadComplete(
            episodeId: placementEpisode,
            stagedURL: dir.appendingPathComponent("missing-staged-file"),
            originalURL: URL(
                string: "https://example.com/placement.mp3"
            ),
            metadata: nil,
            transferIdentity: placementIdentity
        )

        let pinEpisode = "background-pin-failure"
        let pinIdentity = BackgroundTransferIdentity(
            sessionIdentifier: "pin-session",
            taskIdentifier: 52
        )
        let pinStaged = dir.appendingPathComponent("pin-staged.mp3")
        try Data(repeating: 0xC3, count: 96).write(to: pinStaged)
        await manager._registerBackgroundTransferForTesting(
            episodeId: pinEpisode,
            identity: pinIdentity
        )
        await manager._setForcePinWriteFailureForTesting(true)
        await manager.handleBackgroundDownloadComplete(
            episodeId: pinEpisode,
            stagedURL: pinStaged,
            originalURL: URL(
                string: "https://example.com/pin.mp3"
            ),
            metadata: nil,
            transferIdentity: pinIdentity
        )
        await manager._setForcePinWriteFailureForTesting(false)

        #expect(await journal.finalized.isEmpty)
        #expect(
            Set(await journal.failed)
                == Set([placementEpisode, pinEpisode])
        )
    }

    @Test("cache deletion during journal finalization revokes the stale finalized tail")
    func cacheDeletionRacingFinalizationDoesNotJournalSuccess() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let journal = GatedBackgroundCompletionJournal()
        let manager = DownloadManager(
            cacheDirectory: dir,
            workJournalRecorder: journal
        )
        try await manager.bootstrap()
        let episodeId = "background-finalization-delete-race"
        let identity = BackgroundTransferIdentity(
            sessionIdentifier: "finalization-race-session",
            taskIdentifier: 61
        )
        _ = await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId,
            identity: identity
        )
        let staged = dir.appendingPathComponent(
            "finalization-race-staged.mp3"
        )
        try Data(repeating: 0xD4, count: 192).write(to: staged)

        let completion = Task {
            await manager.handleBackgroundDownloadComplete(
                episodeId: episodeId,
                stagedURL: staged,
                originalURL: URL(
                    string: "https://example.com/finalization-race.mp3"
                ),
                metadata: nil,
                transferIdentity: identity
            )
        }
        await journal.waitUntilFinalizationStarted()

        try await manager.removeCache(for: episodeId)
        let cachedAfterDelete =
            await manager.cachedFileURL(for: episodeId)
        let pinAfterDelete = await manager.loadPin(for: episodeId)
        let backgroundStillOwned =
            await manager._isBackgroundDownloadInFlightForTesting(
                episodeId: episodeId
            )
        #expect(cachedAfterDelete == nil)
        #expect(pinAfterDelete == nil)
        #expect(!backgroundStillOwned)

        await journal.releaseFinalization()
        await completion.value

        let finalized = await journal.finalizedSnapshot()
        let finalCached = await manager.cachedFileURL(for: episodeId)
        let finalPin = await manager.loadPin(for: episodeId)
        let finalFingerprint = await manager.fingerprint(for: episodeId)
        #expect(finalized.isEmpty)
        #expect(finalCached == nil)
        #expect(finalPin == nil)
        #expect(finalFingerprint == nil)
    }

    @Test(
        "cache deletion while background analysis enqueue is blocked leaves no resurrected artifacts",
        .timeLimit(.minutes(1))
    )
    func cacheDeletionRacingAnalysisEnqueueLeavesNoArtifacts()
        async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = try await makeTestStore()
        let journal = BackgroundCompletionJournal()
        let manager = DownloadManager(
            cacheDirectory: dir,
            workJournalRecorder: journal
        )
        let scheduler = makeScheduler(store: store)
        let enqueueGate = BackgroundEnqueueGate()
        await scheduler._setEnqueueBarrierForTesting {
            await enqueueGate.wait()
        }
        await manager.setAnalysisWorkScheduler(scheduler)
        try await manager.bootstrap()

        let episodeId = "background-enqueue-delete-race"
        let identity = BackgroundTransferIdentity(
            sessionIdentifier: "enqueue-race-session",
            taskIdentifier: 71
        )
        await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId,
            identity: identity
        )
        let staged = dir.appendingPathComponent(
            "enqueue-race-staged.mp3"
        )
        try Data(repeating: 0xE5, count: 256).write(to: staged)
        let completion = Task {
            await manager.handleBackgroundDownloadComplete(
                episodeId: episodeId,
                stagedURL: staged,
                originalURL: URL(
                    string: "https://example.com/enqueue-race.mp3"
                ),
                metadata: nil,
                transferIdentity: identity
            )
        }
        await enqueueGate.waitUntilStarted()

        try await manager.removeCache(for: episodeId)
        await enqueueGate.release()
        await completion.value

        #expect(await manager.cachedFileURL(for: episodeId) == nil)
        #expect(await manager.loadPin(for: episodeId) == nil)
        #expect(await manager.fingerprint(for: episodeId) == nil)
        #expect(
            !(await manager._hasAccessIndexForTesting(
                episodeId: episodeId
            ))
        )
        #expect(
            await manager._analysisProtectionCountForTesting(
                episodeId: episodeId
            ) == 0
        )
        #expect(
            try await store.fetchLatestJobForEpisode(episodeId) == nil
        )
        #expect(
            try await store.fetchRecentWorkJournalEntries(limit: 100)
                .allSatisfy { $0.episodeId != episodeId }
        )
        #expect(await journal.finalized.isEmpty)
        #expect(
            !(await manager._isBackgroundDownloadInFlightForTesting(
                episodeId: episodeId
            ))
        )
    }

    /// I3: the actor must place the final file inside the injected
    /// `cacheDirectory`, not the process-wide default cache dir.
    @Test("File placement honors custom cacheDirectory")
    func filePlacementHonorsCacheDirectory() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-24cm-1-i3"
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStagingI3", isDirectory: true)
        try FileManager.default.createDirectory(
            at: stagingDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: stagingDir) }

        let stagedFile = stagingDir.appendingPathComponent(
            "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        )
        try Data("fake mp3 bytes".utf8).write(to: stagedFile)

        let originalURL = URL(string: "https://example.com/episode-i3.mp3")
        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: stagedFile,
            originalURL: originalURL,
            metadata: HTTPAssetMetadata(
                etag: "\"i3-etag\"",
                contentLength: 13,
                lastModified: "Wed, 17 Apr 2026 00:00:00 GMT"
            )
        )

        // Final file lives in the custom cache dir, not the default.
        let expectedURL = await manager.completeFileURL(for: episodeId)
        let fm = FileManager.default
        #expect(fm.fileExists(atPath: expectedURL.path))
        #expect(expectedURL.path.hasPrefix(dir.path),
                "Expected final URL \(expectedURL.path) to live under custom cache dir \(dir.path)")

        let defaultDir = DownloadManager.defaultCacheDirectory()
        #expect(!expectedURL.path.hasPrefix(defaultDir.path),
                "Final URL must NOT land in defaultCacheDirectory() when a custom one is injected")

        // Staged file consumed.
        #expect(!fm.fileExists(atPath: stagedFile.path))

        // Cached lookup succeeds via the actor-side accessor.
        let cached = await manager.cachedFileURL(for: episodeId)
        #expect(cached == expectedURL)
    }

    /// I4: the resulting fingerprint must carry a non-empty weak,
    /// synthesized from URL + HTTP metadata exactly as the progressive
    /// path does.
    @Test("Weak fingerprint matches AudioFingerprint.makeWeak from URL + HTTP metadata")
    func weakFingerprintIsNonEmptyAndMatchesProgressivePath() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-24cm-1-i4"
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStagingI4", isDirectory: true)
        try FileManager.default.createDirectory(
            at: stagingDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: stagingDir) }

        let stagedFile = stagingDir.appendingPathComponent(
            "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        )
        try Data("ep i4 audio".utf8).write(to: stagedFile)

        let originalURL = URL(string: "https://example.com/episode-i4.mp3")!
        let metadata = HTTPAssetMetadata(
            etag: "\"i4-etag\"",
            contentLength: 11,
            lastModified: "Thu, 18 Apr 2026 00:00:00 GMT"
        )

        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: stagedFile,
            originalURL: originalURL,
            metadata: metadata
        )

        let fingerprint = await manager.fingerprint(for: episodeId)
        let unwrapped = try #require(fingerprint)

        // Must be non-empty — the pre-fix delegate stamped "" here.
        #expect(!unwrapped.weak.isEmpty,
                "Weak fingerprint must not be the empty sentinel after a background completion")

        // Must match the synthesis the progressive path uses.
        let expectedWeak = AudioFingerprint.makeWeak(url: originalURL, metadata: metadata)
        #expect(unwrapped.weak == expectedWeak)

        // Strong fingerprint is also computed against the placed file.
        #expect(unwrapped.strong != nil)
        #expect(unwrapped.strong?.isEmpty == false)
    }

    /// I4 corollary: a prior progressive pass that already cached a real
    /// weak fingerprint must NOT be regressed when the background-session
    /// callback later fires without HTTP response metadata. The progressive
    /// pass remains the source of truth in that case.
    @Test("Background completion preserves prior weak fingerprint when delegate has no URL")
    func priorWeakFingerprintPreservedWhenNoURL() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let manager = DownloadManager(cacheDirectory: dir)
        try await manager.bootstrap()

        let episodeId = "ep-24cm-1-i4-preserve"

        // Simulate a prior progressive pass: write a file directly into
        // the cache and pre-compute the strong fingerprint via the
        // actor's public seam, which also carries a real weak fp.
        let progressiveURL = await manager.completeFileURL(for: episodeId)
        try Data("pre-existing audio".utf8).write(to: progressiveURL)
        let priorURL = URL(string: "https://example.com/preserve.mp3")!
        _ = try await manager.computeStrongFingerprint(
            episodeId: episodeId, url: priorURL
        )

        let priorFp = try #require(await manager.fingerprint(for: episodeId))
        #expect(!priorFp.weak.isEmpty)
        let priorWeak = priorFp.weak

        // Now fire the background completion path WITHOUT a URL — same
        // file content, but the delegate could not harvest metadata.
        let stagingDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("PlayheadBGStagingI4Preserve", isDirectory: true)
        try FileManager.default.createDirectory(
            at: stagingDir, withIntermediateDirectories: true
        )
        defer { try? FileManager.default.removeItem(at: stagingDir) }
        let stagedFile = stagingDir.appendingPathComponent(
            "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        )
        try Data("pre-existing audio".utf8).write(to: stagedFile)

        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: stagedFile,
            originalURL: nil,
            metadata: nil
        )

        let after = try #require(await manager.fingerprint(for: episodeId))
        #expect(after.weak == priorWeak,
                "Prior weak fingerprint must survive a metadata-less background completion")
        #expect(after.strong != nil)
    }
}
