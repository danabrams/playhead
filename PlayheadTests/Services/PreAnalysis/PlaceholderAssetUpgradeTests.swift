// PlaceholderAssetUpgradeTests.swift
// playhead-0hi9 part 2 — make the EXISTING canonical-SHA upgrade reachable.
//
// A reconciliation path was already built for the two-row case:
// `AnalysisWorkScheduler.canUpgradeWeakAssetToCanonicalSHA`. Its final gate is
//
//     asset.assetFingerprint == currentWeak || asset.weakFingerprint == currentWeak
//
// and it could never be satisfied for a Pipeline A placeholder, because that
// row's `assetFingerprint` is its own UUID and its `weakFingerprint` was
// always NULL. The column's only writer, `AnalysisStore.updateAssetFingerprint`,
// is called exclusively from inside the upgrade path that requires it to
// already be populated — a closed loop.
//
// A second, independent blocker: `currentAudioFingerprint` comes from
// `DownloadManager.fingerprint(for:)`, which was a pure in-memory dictionary
// read. The cache is never persisted, so after any relaunch it was empty and
// the upgrade was unreachable regardless of what the database held.
//
// Both are fixed by *populating existing machinery*, not by adding new
// machinery: `weakFingerprint` is written at both insert sites, and
// `fingerprint(for:)` rehydrates from the `.pin` sidecar that already carried
// the strong hash durably.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-0hi9 — placeholder rows upgrade instead of duplicating", .serialized)
struct PlaceholderAssetUpgradeTests {

    private static let tempDirs = TestTempDirTracker()

    // MARK: - Harness

    private func makeRunner(store: AnalysisStore) -> AnalysisJobRunner {
        AnalysisJobRunner(
            store: store,
            audioProvider: StubAnalysisAudioProvider(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: SpeechService(recognizer: StubSpeechRecognizer()),
                store: store
            ),
            adDetection: StubAdDetectionProvider()
        )
    }

    private func makeScheduler(
        store: AnalysisStore,
        downloadProvider: StubDownloadProvider
    ) -> AnalysisWorkScheduler {
        AnalysisWorkScheduler(
            store: store,
            jobRunner: makeRunner(store: store),
            capabilitiesService: StubCapabilitiesProvider(),
            downloadManager: downloadProvider,
            batteryProvider: {
                let b = StubBatteryProvider()
                b.level = 0.9
                b.charging = true
                return b
            }(),
            transportStatusProvider: StubTransportStatusProvider(),
            config: PreAnalysisConfig()
        )
    }

    private func writeSynthAudio(seconds: TimeInterval) throws -> URL {
        let dir = try makeTempDir(prefix: "Bd0hi9Upgrade")
        Self.tempDirs.track(dir)
        let fileURL = dir.appendingPathComponent("synth-\(UUID().uuidString).caf")
        guard let format = AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 44_100,
            channels: 1,
            interleaved: false
        ) else {
            throw NSError(domain: "PlaceholderAssetUpgradeTests", code: -1)
        }
        let file = try AVAudioFile(
            forWriting: fileURL,
            settings: format.settings,
            commonFormat: .pcmFormatFloat32,
            interleaved: false
        )
        let totalFrames = AVAudioFramePosition(seconds * 44_100)
        var written = AVAudioFramePosition(0)
        while written < totalFrames {
            let frames = AVAudioFrameCount(min(AVAudioFramePosition(44_100), totalFrames - written))
            guard let buffer = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: frames) else {
                throw NSError(domain: "PlaceholderAssetUpgradeTests", code: -2)
            }
            buffer.frameLength = frames
            try file.write(from: buffer)
            written += AVAudioFramePosition(frames)
        }
        return fileURL
    }

    /// Every `analysis_assets` row for one episode, via the production
    /// keyset-paginated reader (`fetchAllAssets` is DEBUG-only).
    private func allAssets(
        store: AnalysisStore,
        episodeId: String
    ) async throws -> [AnalysisAsset] {
        var out: [AnalysisAsset] = []
        var cursor: Int64 = 0
        while true {
            let page = try await store.fetchAssetsKeysetByRowId(afterRowId: cursor, limit: 100)
            if page.isEmpty { break }
            for (rowId, asset) in page {
                cursor = max(cursor, rowId)
                if asset.episodeId == episodeId { out.append(asset) }
            }
            if page.count < 100 { break }
        }
        return out
    }

    /// Exactly the row Pipeline A mints in `AnalysisCoordinator.resolveSession`:
    /// `assetFingerprint` is the row's own id, state `queued`.
    private func insertPlaceholder(
        store: AnalysisStore,
        assetId: String,
        episodeId: String,
        weakFingerprint: String?
    ) async throws {
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: assetId,
            weakFingerprint: weakFingerprint,
            sourceURL: "file:///partial/\(episodeId).mp3",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.queued.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil
        ))
    }

    // MARK: - The upgrade actually fires now

    @Test("placeholder carrying a weakFingerprint is UPGRADED to the canonical SHA, not duplicated")
    func placeholderWithWeakIsUpgraded() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 4.0)
        let sha = try FileHasher.sha256(fileURL: url)
        let weak = "https://cdn.example.com/ep-upgrade.mp3|\"etag-1\"|8388608|Mon, 01 Jan 2026 00:00:00 GMT"
        provider.cachedURLs["ep-upgrade"] = url
        provider.fingerprints["ep-upgrade"] = AudioFingerprint(weak: weak, strong: sha)

        let placeholderId = UUID().uuidString
        try await insertPlaceholder(
            store: store,
            assetId: placeholderId,
            episodeId: "ep-upgrade",
            weakFingerprint: weak
        )

        await scheduler.enqueue(
            episodeId: "ep-upgrade",
            podcastId: "pod-upgrade",
            downloadId: "dl-upgrade",
            sourceFingerprint: sha,
            isExplicitDownload: true
        )
        _ = await scheduler.processNextDispatchableJobForTesting()

        let rows = try await allAssets(store: store, episodeId: "ep-upgrade")
        #expect(rows.count == 1,
                "one episode must own one asset row (got \(rows.count): \(rows.map(\.assetFingerprint)))")
        let survivor = try #require(rows.first)
        #expect(survivor.id == placeholderId,
                "the placeholder row must be upgraded in place, keeping every child row already pointed at it")
        #expect(survivor.assetFingerprint == sha,
                "the upgraded row must be keyed by the canonical full-file SHA")
        #expect(survivor.weakFingerprint == weak,
                "updateAssetFingerprint must preserve the weak identity it matched on")
    }

    @Test("placeholder with NO weak identity still splits — the pre-fix shape part 3 reconciles")
    func placeholderWithoutWeakStillSplits() async throws {
        let store = try await makeTestStore()
        let provider = StubDownloadProvider()
        let scheduler = makeScheduler(store: store, downloadProvider: provider)

        let url = try writeSynthAudio(seconds: 4.0)
        let sha = try FileHasher.sha256(fileURL: url)
        provider.cachedURLs["ep-legacy"] = url
        provider.fingerprints["ep-legacy"] = AudioFingerprint(
            weak: "https://cdn.example.com/ep-legacy.mp3|||",
            strong: sha
        )

        try await insertPlaceholder(
            store: store,
            assetId: UUID().uuidString,
            episodeId: "ep-legacy",
            weakFingerprint: nil
        )

        await scheduler.enqueue(
            episodeId: "ep-legacy",
            podcastId: "pod-legacy",
            downloadId: "dl-legacy",
            sourceFingerprint: sha,
            isExplicitDownload: true
        )
        _ = await scheduler.processNextDispatchableJobForTesting()

        let rows = try await allAssets(store: store, episodeId: "ep-legacy")
        #expect(rows.count == 2,
                "a NULL-weak placeholder carries no identity the upgrade can match — this is exactly the shape already on disk, and only the part-3 sweep can repair it")
        // The row the SCHEDULER minted must itself carry the weak identity, so
        // the pair is symmetric and no third row can ever be created for the
        // same audio.
        let schedulerRow = try #require(rows.first { $0.assetFingerprint == sha })
        #expect(schedulerRow.weakFingerprint == "https://cdn.example.com/ep-legacy.mp3|||",
                "AnalysisWorkScheduler.resolveAnalysisAssetId must persist the observed weak fingerprint at insert")
    }

    // MARK: - Pipeline A's insert site

    @Test("resolveSession writes the weak fingerprint the download manager already knows")
    func resolveSessionPersistsWeakFingerprint() async throws {
        let store = try await makeTestStore()
        let dir = try makeTempDir(prefix: "Bd0hi9Resolve")
        Self.tempDirs.track(dir)
        let downloadManager = DownloadManager(cacheDirectory: dir)

        let episodeId = "ep-resolve"
        let weak = "https://cdn.example.com/resolve.mp3|\"etag-r\"|8388608|Wed, 03 Jan 2026 00:00:00 GMT"
        // Only the durable pin exists — no in-memory state — which is exactly
        // the post-relaunch condition the old code could not survive.
        _ = await downloadManager.writePin(
            AudioAssetPin(
                expectedBytes: 8_388_608,
                sha256: nil,
                sourceURL: "https://cdn.example.com/resolve.mp3",
                etag: "\"etag-r\"",
                weakFingerprint: weak
            ),
            for: episodeId
        )

        let coordinator = AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: SpeechService(recognizer: StubSpeechRecognizer()),
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store),
            downloadManager: downloadManager
        )

        let audioURL = try #require(
            LocalAudioURL(dir.appendingPathComponent("partial-\(episodeId).mp3"))
        )
        let (_, assetId, _) = try await coordinator.resolveSessionForTesting(
            episodeId: episodeId,
            audioURL: audioURL
        )

        let asset = try #require(try await store.fetchAsset(id: assetId))
        #expect(asset.assetFingerprint == assetId,
                "the placeholder is still keyed by its own id — no content hash exists yet")
        #expect(asset.weakFingerprint == weak,
                "the placeholder must carry the weak identity so canUpgradeWeakAssetToCanonicalSHA can match it later")
        #expect(asset.sourceURL == audioURL.absoluteString,
                "a path outside the audio cache has no portable form and is stored verbatim")
    }

    /// playhead-b8hj: Pipeline A's placeholder is minted the moment playback
    /// starts and its `sourceURL` is never rewritten (no `UPDATE ... SET
    /// sourceURL` exists), so an absolute path baked in here outlives the
    /// container it names. iOS re-creates the Data container under a new UUID
    /// on reinstall and restore.
    @Test("playhead-b8hj: resolveSession persists a container-portable path for cached audio")
    func resolveSessionPersistsContainerPortableSourceURL() async throws {
        let store = try await makeTestStore()
        let dir = try makeTempDir(prefix: "Bdb8hjResolve")
        Self.tempDirs.track(dir)

        let coordinator = AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(
                speechService: SpeechService(recognizer: StubSpeechRecognizer()),
                store: store
            ),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store),
            downloadManager: DownloadManager(cacheDirectory: dir)
        )

        let episodeId = "ep-b8hj-portable"
        let name = "\(DownloadManager.safeFilename(for: episodeId)).mp3"
        // Inside the LIVE audio cache — the production shape. The file is
        // deliberately not created: what is under test is the persisted string.
        let audioURL = try #require(LocalAudioURL(
            DownloadManager.defaultCacheDirectory()
                .appendingPathComponent("complete", isDirectory: true)
                .appendingPathComponent(name)
        ))
        let (_, assetId, _) = try await coordinator.resolveSessionForTesting(
            episodeId: episodeId, audioURL: audioURL
        )

        let asset = try #require(try await store.fetchAsset(id: assetId))
        #expect(asset.sourceURL == "complete/\(name)")
        #expect(!asset.sourceURL.contains("Containers"),
                "a container segment in a write-once column is a permanent dead reference")
    }

    // MARK: - The upgrade survives a relaunch

    @Test("fingerprint(for:) rehydrates weak AND strong from the .pin sidecar after a relaunch")
    func fingerprintSurvivesRelaunch() async throws {
        let dir = try makeTempDir(prefix: "Bd0hi9Pin")
        Self.tempDirs.track(dir)
        let episodeId = "ep-relaunch"
        let weak = "https://cdn.example.com/relaunch.mp3|\"etag-9\"|4096|Tue, 02 Jan 2026 00:00:00 GMT"
        let sha = String(repeating: "a", count: 64)

        // Session 1: the download completes and pins the artifact.
        let first = DownloadManager(cacheDirectory: dir)
        let wrote = await first.writePin(
            AudioAssetPin(
                expectedBytes: 4096,
                sha256: sha,
                sourceURL: "https://cdn.example.com/relaunch.mp3",
                etag: "\"etag-9\"",
                weakFingerprint: weak
            ),
            for: episodeId
        )
        #expect(wrote)

        // Session 2: a brand-new actor over the same directory. This is the
        // relaunch — `fingerprintCache` starts empty.
        let second = DownloadManager(cacheDirectory: dir)
        let rehydrated = try #require(
            await second.fingerprint(for: episodeId),
            "a relaunched DownloadManager must recover the fingerprint from the durable pin"
        )
        #expect(rehydrated.weak == weak)
        #expect(rehydrated.strong == sha)
    }

    @Test("writePin stamps the live weak fingerprint so the rehydration is exact")
    func writePinStampsWeakFingerprint() async throws {
        let dir = try makeTempDir(prefix: "Bd0hi9PinStamp")
        Self.tempDirs.track(dir)
        let manager = DownloadManager(cacheDirectory: dir)
        let episodeId = "ep-stamp"

        // Put real bytes at the canonical path and let the manager compute its
        // own fingerprint, which is what populates `fingerprintCache`.
        let completeURL = await manager.completeFileURL(for: episodeId)
        try FileManager.default.createDirectory(
            at: completeURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        try Data("playhead-0hi9 stamp fixture".utf8).write(to: completeURL)
        let sourceURL = try #require(URL(string: "https://cdn.example.com/stamp.mp3"))
        let computed = try #require(
            await manager.computeStrongFingerprint(episodeId: episodeId, url: sourceURL)
        )
        #expect(!computed.weak.isEmpty)

        // A pin written WITHOUT an explicit weak must pick it up.
        let wrote = await manager.writePin(
            AudioAssetPin(
                expectedBytes: Int64(try Data(contentsOf: completeURL).count),
                sha256: computed.strong,
                sourceURL: sourceURL.absoluteString,
                etag: nil
            ),
            for: episodeId
        )
        #expect(wrote)

        let pin = try #require(await manager.loadPin(for: episodeId))
        #expect(pin.weakFingerprint == computed.weak,
                "the pin must carry the exact weak string that was live, so a later relaunch rehydrates an identical fingerprint rather than a reconstruction")
    }

    @Test("a caller-supplied weak fingerprint is never overwritten by the cache")
    func writePinKeepsExplicitWeakFingerprint() async throws {
        let dir = try makeTempDir(prefix: "Bd0hi9PinExplicit")
        Self.tempDirs.track(dir)
        let manager = DownloadManager(cacheDirectory: dir)
        let episodeId = "ep-explicit"

        let wrote = await manager.writePin(
            AudioAssetPin(
                expectedBytes: 10,
                sha256: nil,
                sourceURL: "https://cdn.example.com/explicit.mp3",
                etag: nil,
                weakFingerprint: "explicit-weak"
            ),
            for: episodeId
        )
        #expect(wrote)
        let pin = try #require(await manager.loadPin(for: episodeId))
        #expect(pin.weakFingerprint == "explicit-weak")
    }

    @Test("a legacy pin with no weakFingerprint field still rehydrates the strong hash")
    func legacyPinRehydratesStrongHash() async throws {
        let dir = try makeTempDir(prefix: "Bd0hi9PinLegacy")
        Self.tempDirs.track(dir)
        let episodeId = "ep-legacy-pin"
        let sha = String(repeating: "b", count: 64)

        // Hand-write the pre-0hi9 JSON shape: no `weakFingerprint` key at all.
        let first = DownloadManager(cacheDirectory: dir)
        let pinURL = await first.pinFileURL(for: episodeId)
        try FileManager.default.createDirectory(
            at: pinURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        let legacyJSON = """
            {"expectedBytes":4096,"sha256":"\(sha)","sourceURL":"https://cdn.example.com/legacy.mp3","etag":"\\"etag-legacy\\""}
            """
        try Data(legacyJSON.utf8).write(to: pinURL)

        let second = DownloadManager(cacheDirectory: dir)
        let rehydrated = try #require(await second.fingerprint(for: episodeId))
        #expect(rehydrated.strong == sha)
        // Best-effort reconstruction from what a legacy pin does carry.
        #expect(rehydrated.weak.contains("https://cdn.example.com/legacy.mp3"))
        #expect(rehydrated.weak.contains("4096"))
    }

    /// R1: `fingerprint(for:)` now WRITES its rehydration back into
    /// `fingerprintCache`, and `computeStrongFingerprint` short-circuits on a
    /// cache hit. A pin that carries neither a weak nor a strong identity must
    /// therefore report a miss rather than seed an empty entry — an empty
    /// entry would be returned for the rest of the process and would shadow
    /// the real fingerprint once the download completes.
    @Test("a pin carrying no identity reports a miss instead of poisoning the cache")
    func emptyPinDoesNotPoisonTheFingerprintCache() async throws {
        let dir = try makeTempDir(prefix: "Bd0hi9PinEmpty")
        Self.tempDirs.track(dir)
        let episodeId = "ep-empty-pin"
        let manager = DownloadManager(cacheDirectory: dir)
        let pinURL = await manager.pinFileURL(for: episodeId)
        try FileManager.default.createDirectory(
            at: pinURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        // No sourceURL, no etag, no sha256, no weakFingerprint.
        try Data(#"{"expectedBytes":1024}"#.utf8).write(to: pinURL)

        #expect(await manager.fingerprint(for: episodeId) == nil,
                "an identity-free pin carries nothing a caller can act on")

        // The download finishes and pins for real. The earlier miss must not
        // have left anything behind that shadows it.
        let weak = "https://cdn.example.com/late.mp3|\"etag-late\"|1024|"
        let sha = String(repeating: "c", count: 64)
        _ = await manager.writePin(
            AudioAssetPin(
                expectedBytes: 1024,
                sha256: sha,
                sourceURL: "https://cdn.example.com/late.mp3",
                etag: "\"etag-late\"",
                weakFingerprint: weak
            ),
            for: episodeId
        )
        let recovered = try #require(await manager.fingerprint(for: episodeId))
        #expect(recovered.strong == sha)
        #expect(recovered.weak == weak)
    }

    /// R2, found by a SURVIVING mutation. `fingerprintFromPin` rebuilds a
    /// legacy pin's weak from `sourceURL`/`etag`/`expectedBytes`, and guards
    /// `expectedBytes != Int64.max`. Deleting that guard survived every test —
    /// yet `Int64.max` is not an edge case: it is what FIVE pin write sites
    /// record (`totalContentLength ?? Int64.max` when the server omits
    /// Content-Length, plus the always-incomplete streaming seeds). Baking the
    /// sentinel into the weak yields `…|9223372036854775807|`, which can never
    /// equal the live weak, so `canUpgradeWeakAssetToCanonicalSHA` never
    /// matches and the episode splits into two rows again — the exact failure
    /// this bead exists to close, for the whole no-Content-Length cohort.
    @Test("the unknown-length sentinel is never baked into a rehydrated weak")
    func unknownLengthSentinelIsNotBakedIntoTheWeak() async throws {
        let dir = try makeTempDir(prefix: "Bd0hi9PinSentinel")
        Self.tempDirs.track(dir)
        let episodeId = "ep-sentinel"
        let sourceURL = try #require(URL(string: "https://cdn.example.com/sentinel.mp3"))
        let manager = DownloadManager(cacheDirectory: dir)
        let pinURL = await manager.pinFileURL(for: episodeId)
        try FileManager.default.createDirectory(
            at: pinURL.deletingLastPathComponent(),
            withIntermediateDirectories: true
        )
        // Pre-0hi9 shape: no `weakFingerprint` field, length unknown.
        let legacyJSON = """
            {"expectedBytes":9223372036854775807,"sourceURL":"\(sourceURL.absoluteString)","etag":"\\"etag-s\\""}
            """
        try Data(legacyJSON.utf8).write(to: pinURL)

        let rehydrated = try #require(await manager.fingerprint(for: episodeId))
        let liveWeak = AudioFingerprint.makeWeak(
            url: sourceURL,
            metadata: HTTPAssetMetadata(
                etag: "\"etag-s\"", contentLength: nil, lastModified: nil
            )
        )
        #expect(rehydrated.weak == liveWeak,
                "the reconstruction has to match what makeWeak produces for a response with no Content-Length, or the upgrade can never fire")
        #expect(!rehydrated.weak.contains("9223372036854775807"))
    }

    @Test("nonEmptyWeak rejects the empty sentinel that no-metadata paths write")
    func nonEmptyWeakRejectsSentinels() {
        #expect(AudioFingerprint.nonEmptyWeak(nil) == nil)
        #expect(AudioFingerprint.nonEmptyWeak("") == nil)
        #expect(AudioFingerprint.nonEmptyWeak("   ") == nil)
        #expect(AudioFingerprint.nonEmptyWeak("weak|x|1|y") == "weak|x|1|y")
    }
}
