// PlaybackServiceActorTests.swift
// Regression tests for PlaybackServiceActor executor conflicts.
//
// The "Incorrect actor executor assumption" crash happens when
// PlaybackServiceActor-isolated methods are called from the wrong
// context — typically during Siri or phone call interruptions when
// AVFoundation fires KVO/delegate callbacks on arbitrary threads.

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

// MARK: - Actor Isolation

@Suite("PlaybackServiceActor – Isolation")
struct PlaybackServiceActorIsolationTests {

    @Test("PlaybackService methods are callable from PlaybackServiceActor")
    func basicActorAccess() async {
        let service = await PlaybackService()
        let snapshot = await service.snapshot()
        #expect(snapshot.status == .idle)
    }

    @Test("State observation stream yields from actor without assertion")
    func stateObservation() async {
        let service = await PlaybackService()
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
        let service = await PlaybackService()

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
        let service = await PlaybackService()

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
        let service = await PlaybackService()
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

        let service = await PlaybackService()

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

            // Loader-side: suspend/resume (simulating interruption).
            nonisolated(unsafe) let unsafeLoader = loader
            group.addTask {
                for _ in 0..<50 {
                    unsafeLoader.suspend()
                    unsafeLoader.resume()
                }
            }
        }

        // If we get here without crashing, the decoupling works.
        _ = loader
    }
}
