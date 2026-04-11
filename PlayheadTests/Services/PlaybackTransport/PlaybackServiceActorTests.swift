// PlaybackServiceActorTests.swift
// Regression tests for PlaybackServiceActor executor conflicts.
//
// The "Incorrect actor executor assumption" crash happens when
// PlaybackServiceActor-isolated methods are called from the wrong
// context — typically during Siri or phone call interruptions when
// AVFoundation fires KVO/delegate callbacks on arbitrary threads.

@preconcurrency import AVFoundation
import Foundation
import MediaPlayer
import Testing
@testable import Playhead

private actor LoaderDriver {
    let loader: ProgressiveResourceLoader

    init(loader: ProgressiveResourceLoader) {
        self.loader = loader
    }

    func toggleSuspendResume(times: Int) {
        for _ in 0..<times {
            loader.suspend()
            loader.resume()
        }
    }
}

// MARK: - Actor Isolation

@Suite("PlaybackServiceActor – Isolation")
struct PlaybackServiceActorIsolationTests {

    @Test("PlaybackService methods are callable from PlaybackServiceActor")
    func basicActorAccess() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        let snapshot = await service.snapshot()
        #expect(snapshot.status == .idle)
    }

    @Test("State observation stream yields from actor without assertion")
    func stateObservation() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        let stream = await service.observeStates()

        // Consume the initial yield (immediate hydration).
        var received = false
        for await state in stream {
            #expect(state.status == .idle)
            received = true
            break
        }
        #expect(received)
    }

    @Test("Concurrent snapshot calls don't trigger executor assertion")
    func concurrentSnapshots() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )

        // Simulate rapid concurrent access like KVO callbacks during Siri.
        await withTaskGroup(of: PlaybackState.self) { group in
            for _ in 0..<20 {
                group.addTask {
                    await service.snapshot()
                }
            }
            for await state in group {
                #expect(state.status == .idle)
            }
        }
    }
}

// MARK: - Progressive Loader Decoupling

@Suite("PlaybackService – Progressive Loader Decoupling")
struct ProgressiveLoaderDecouplingTests {

    @Test("loadItem accepts externally-created player item")
    func loadItemAcceptsExternalItem() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )

        // Create a player item outside the actor — same pattern as the runtime.
        let asset = AVURLAsset(url: URL(string: "playhead-progressive://audio/test.mp3")!)
        let item = AVPlayerItem(asset: asset)

        // This should not trigger any actor assertion.
        await service.loadItem(item)
        let snapshot = await service.snapshot()
        // Status will be loading or failed (no real delegate), but no crash.
        #expect(snapshot.status != .idle)
    }

    @Test("ProgressiveResourceLoader operates independently of PlaybackServiceActor")
    func loaderIndependentOfActor() async throws {
        let dir = try makeTempDir(prefix: "LoaderDecouple")
        defer { try? FileManager.default.removeItem(at: dir) }

        let file = dir.appendingPathComponent("test.mp3")
        try Data(repeating: 0x42, count: 4096).write(to: file)

        // Create loader outside any actor — same as PlayheadRuntime does.
        let loader = ProgressiveResourceLoader(
            fileURL: file,
            totalBytes: 4096,
            contentType: "public.mp3"
        )

        // Simulate suspend/resume from a non-actor context (like an
        // interruption handler would if the loader were still on the actor).
        loader.suspend()
        loader.resume()

        // Create asset + item using the loader.
        var components = URLComponents()
        components.scheme = "playhead-progressive"
        components.host = "audio"
        components.path = "/test.mp3"
        let asset = AVURLAsset(url: components.url!)
        asset.resourceLoader.setDelegate(loader, queue: loader.queue)

        let item = AVPlayerItem(asset: asset)

        // Hand the item to PlaybackService — loader stays outside the actor.
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        await service.loadItem(item)

        // No actor assertion should fire. The loader's delegate callbacks
        // run on loader.queue, completely decoupled from PlaybackServiceActor.
        let snapshot = await service.snapshot()
        #expect(snapshot.status != .idle)
        _ = loader // Keep alive
    }

    @Test("Simultaneous actor access and loader callbacks don't conflict")
    func simultaneousAccessAndCallbacks() async throws {
        let dir = try makeTempDir(prefix: "SimultaneousAccess")
        defer { try? FileManager.default.removeItem(at: dir) }

        let file = dir.appendingPathComponent("test.mp3")
        try Data(repeating: 0x42, count: 8192).write(to: file)

        let loader = ProgressiveResourceLoader(
            fileURL: file,
            totalBytes: 8192,
            contentType: "public.mp3"
        )
        let loaderDriver = LoaderDriver(loader: loader)

        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )

        // Run loader operations and actor operations concurrently.
        // This simulates the Siri scenario: AVFoundation fires delegate
        // callbacks on loader.queue while the interruption handler runs
        // on PlaybackServiceActor.
        await withTaskGroup(of: Void.self) { group in
            // Actor-side: rapid snapshot reads (simulating KVO storm).
            group.addTask {
                for _ in 0..<50 {
                    _ = await service.snapshot()
                }
            }

            group.addTask {
                await loaderDriver.toggleSuspendResume(times: 50)
            }
        }

        // If we get here without crashing, the decoupling works.
        _ = loader
    }
}

// MARK: - Callback Isolation

@Suite("PlaybackService – Callback Isolation")
struct PlaybackServiceCallbackIsolationTests {

    /// 1×1 pixel test image, cheap to create and sufficient for artwork tests.
    private static var testImage: UIImage {
        UIGraphicsImageRenderer(size: CGSize(width: 1, height: 1))
            .image { $0.fill(CGRect(x: 0, y: 0, width: 1, height: 1)) }
    }

    @Test("setNowPlayingMetadata with artwork doesn't crash from actor")
    func setNowPlayingMetadataWithArtwork() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )
        let image = Self.testImage

        // The MPMediaItemArtwork closure must be non-isolated; if it were
        // tainted with @PlaybackServiceActor the runtime would crash when
        // MediaPlayer invokes the provider from the main thread.
        await service.setNowPlayingMetadata(
            title: "Test Episode",
            artist: "Test Podcast",
            albumTitle: "Test Album",
            artworkImage: image
        )

        // Verify the title was written to the injected fake, not the real
        // MPNowPlayingInfoCenter.default().
        let title = nowPlaying.info?[MPMediaItemPropertyTitle] as? String
        #expect(title == "Test Episode")
    }

    @Test("MPMediaItemArtwork provider callable from main thread")
    func artworkProviderCallableFromMain() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )
        let image = Self.testImage

        // Set metadata so the artwork provider closure is installed.
        await service.setNowPlayingMetadata(
            title: "Artwork Test",
            artworkImage: image
        )

        // Read back the artwork from the injected fake and invoke its
        // image(at:) from MainActor, exactly as MediaPlayer does when
        // rendering the lock screen. This is the exact scenario that
        // crashed before the fix.
        let rendered: UIImage? = await MainActor.run {
            let artwork = nowPlaying.info?[MPMediaItemPropertyArtwork] as? MPMediaItemArtwork
            return artwork?.image(at: CGSize(width: 1, height: 1))
        }
        #expect(rendered != nil)
        _ = service
    }

    @Test("loadItem triggers KVO without executor assertion")
    func loadItemKVOSafe() async throws {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )

        // Create a player item from a progressive URL. The asset won't load
        // real audio, but AVPlayerItem will still fire status KVO (to .unknown
        // or .failed) which exercises the nonisolated KVO callback path.
        let asset = AVURLAsset(url: URL(string: "playhead-progressive://audio/kvo-test.mp3")!)
        let item = AVPlayerItem(asset: asset)

        await service.loadItem(item)

        // Give KVO callbacks time to fire on their arbitrary queue.
        try await Task.sleep(for: .milliseconds(100))

        // If the KVO closure were actor-tainted, we would have crashed
        // before reaching this snapshot.
        let snapshot = await service.snapshot()
        #expect(snapshot.status != .idle)
    }

    @Test("State updates after play don't crash")
    func stateUpdatesAfterPlay() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )

        // Inject a playing state so play() has a playerItem-like path to
        // exercise. _testingInjectState bypasses AVPlayer so we can test
        // the updateNowPlayingInfo codepath in isolation.
        let playingState = PlaybackState(
            status: .playing,
            currentTime: 30,
            duration: 3600,
            rate: 1.0,
            playbackSpeed: 1.0
        )
        await service._testingInjectState(playingState)

        // play() calls updateNowPlayingInfo which writes to the now-playing
        // seam — if any closure in that path is actor-tainted and called from
        // the wrong executor, we crash.
        await service.play()

        let snapshot = await service.snapshot()
        #expect(snapshot.status == .playing)
    }

    @Test("Concurrent metadata and snapshot access")
    func concurrentMetadataAndSnapshot() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        let image = Self.testImage

        await withTaskGroup(of: Void.self) { group in
            // Task A: set metadata with artwork (exercises makeArtwork).
            group.addTask {
                await service.setNowPlayingMetadata(
                    title: "Concurrent Test",
                    artist: "Podcast Host",
                    artworkImage: image
                )
            }

            // Task B: rapid snapshot reads, simulating KVO storm.
            group.addTask {
                for _ in 0..<20 {
                    let snap = await service.snapshot()
                    // Status should be consistent (idle since no media loaded).
                    #expect(snap.status == .idle)
                }
            }
        }

        // If we reach here, concurrent artwork closure creation didn't
        // interfere with actor-serialized snapshot access.
    }
}
