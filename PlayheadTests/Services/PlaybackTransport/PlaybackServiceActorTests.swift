// PlaybackServiceActorTests.swift
// Regression tests for PlaybackServiceActor executor conflicts.
//
// The "Incorrect actor executor assumption" crash happens when
// PlaybackServiceActor-isolated methods are called from the wrong
// context — typically during Siri or phone call interruptions when
// AVFoundation fires KVO/delegate callbacks on arbitrary threads.
//
// playhead-xc6b — HANG BACKSTOPS ARE MANDATORY IN EVERY SUITE HERE.
//
// These tests are BEHAVIOURAL, not measurement: not one of them asserts a
// latency, and every assertion (a status observed, a volume restored, a
// generation rejected) stays valid on a saturated machine. They belong in
// the default gate and must never be migrated behind `PerfGate` — doing so
// would delete real coverage to fix a problem PerfGate does not solve.
//
// What they lacked was a finite deadline. Before playhead-xc6b, 30 of the
// 32 tests in this file had no `.timeLimit` at all, while awaiting
// `ControlledItemSeek` / `ControlledSeekSequence` continuation gates and
// `AsyncStream` iterators that carry no deadline of their own. A regression
// on those paths HUNG THE GATE HOST indefinitely instead of failing a test
// — strictly worse than a flake, since a hang burns the whole run and
// reports nothing.
//
// Every suite below carries `.timeLimit(.minutes(3))` so tests added later
// inherit a backstop, and every test carries it explicitly so the coverage
// is greppable rather than inherited-and-assumed. Three minutes is chosen
// deliberately: the documented executor-starvation signature on this box
// reaches ~98-131 s of queueing delay under a saturated gate (the same
// reasoning already recorded on
// `RuntimeShutdownLifecycleTests.lifecycleInvalidationDoesNotJoinStalledCacheTask`),
// so 3 minutes clears contention — a trip means a real hang, not a busy
// machine — while staying far below the gate's outer timeout.

@preconcurrency import AVFoundation
import Foundation
import MediaPlayer
import Testing
@testable import Playhead

private actor LoaderDriver {
    let loader: ProgressiveResourceLoader
    /// playhead-xc6b: counts completed toggles so the concurrency test can
    /// assert the loader side actually ran its full workload. Without it a
    /// `toggleSuspendResume` that silently did nothing still "passed".
    private(set) var completedToggles = 0

    init(loader: ProgressiveResourceLoader) {
        self.loader = loader
    }

    func toggleSuspendResume(times: Int) {
        for _ in 0..<times {
            loader.suspend()
            loader.resume()
            completedToggles += 1
        }
    }
}

private actor ControlledItemSeek {
    private var started = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseContinuation: CheckedContinuation<Void, Never>?

    func perform() async -> Bool {
        started = true
        let waiters = startWaiters
        startWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
        await withCheckedContinuation { continuation in
            releaseContinuation = continuation
        }
        return true
    }

    func waitUntilStarted() async {
        if started { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        releaseContinuation?.resume()
        releaseContinuation = nil
    }
}

private actor ControlledSeekSequence {
    private var nextOperation = 0
    private var startedOperations: Set<Int> = []
    private var startWaiters:
        [Int: [CheckedContinuation<Void, Never>]] = [:]
    private var releaseContinuations:
        [Int: CheckedContinuation<Bool, Never>] = [:]

    func perform() async -> Bool {
        let operation = nextOperation
        nextOperation += 1
        startedOperations.insert(operation)
        let waiters = startWaiters.removeValue(forKey: operation) ?? []
        for waiter in waiters {
            waiter.resume()
        }
        return await withCheckedContinuation { continuation in
            releaseContinuations[operation] = continuation
        }
    }

    func waitUntilStarted(_ operation: Int) async {
        if startedOperations.contains(operation) { return }
        await withCheckedContinuation { continuation in
            startWaiters[operation, default: []].append(continuation)
        }
    }

    func release(_ operation: Int, completed: Bool) {
        releaseContinuations.removeValue(forKey: operation)?.resume(
            returning: completed
        )
    }
}

@Suite("PlaybackService seek validation", .timeLimit(.minutes(3)))
struct PlaybackServiceSeekValidationTests {
    @Test(
        "Invalid absolute seeks fail closed without publishing state",
        .timeLimit(.minutes(3)),
        arguments: [Double.nan, .infinity, -.infinity, -0.001]
    )
    func invalidAbsoluteSeek(_ target: Double) async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        await service._testingInjectState(
            PlaybackState(
                status: .playing,
                currentTime: 17,
                duration: 100,
                rate: 1,
                playbackSpeed: 1
            )
        )

        #expect(!(await service.seek(to: target)))
        #expect(await service.snapshot().currentTime == 17)
        await service.tearDown()
    }
}

/// playhead-xc6b: POSITIVE CONTROL for the audio-session interruption
/// observer, used by the two tests whose real subject is that observer.
///
/// `PlaybackService` installs the observer from a retained setup hop
/// (`observeInterruptionsAsync()` inside `setupTask`), and it only becomes a
/// live subscriber once its `for await` has actually registered with the
/// NotificationCenter. Both steps are asynchronous and neither is exposed as
/// a flag, so the only honest proof that the observer exists is to DRIVE it:
/// put the service in `.playing`, post a `.began` interruption, and wait for
/// the resulting `pause()` to land.
///
/// The post is repeated on every poll iteration on purpose:
/// `NotificationCenter.notifications(named:)` only delivers notifications
/// posted AFTER its registration, so a single pre-registration post would be
/// dropped and never redelivered.
///
/// Fails closed. If the observer was never installed, or is subscribed to a
/// different center, the poll expires and the caller's `#expect` reddens
/// instead of the test quietly proceeding on an assumption. Call this BEFORE
/// `loadItem`, so item-status KVO cannot race the observed `.paused`.
private func driveInterruptionObserverUntilLive(
    _ service: PlaybackService,
    center: NotificationCenter
) async -> Bool {
    await service._testingInjectState(
        PlaybackState(
            status: .playing,
            currentTime: 1,
            duration: 100,
            rate: 1,
            playbackSpeed: 1
        )
    )
    return await pollUntil {
        center.post(
            name: AVAudioSession.interruptionNotification,
            object: nil,
            userInfo: [
                AVAudioSessionInterruptionTypeKey:
                    AVAudioSession.InterruptionType.began.rawValue,
            ]
        )
        return await service.snapshot().status == .paused
    }
}

/// playhead-xc6b: a subscriber that mirrors the one `PlaybackService`
/// installs — an async notification sequence consumed on
/// `@PlaybackServiceActor` — used purely as a delivery barrier.
private final class InterruptionWitness: @unchecked Sendable {
    private let lock = NSLock()
    private var received = false
    private var task: Task<Void, Never>?

    var hasReceived: Bool {
        lock.lock()
        defer { lock.unlock() }
        return received
    }

    func start(center: NotificationCenter) {
        task = Task { @PlaybackServiceActor [self] in
            for await _ in center.notifications(
                named: AVAudioSession.interruptionNotification
            ) {
                markReceived()
                break
            }
        }
    }

    func stop() {
        task?.cancel()
        task = nil
    }

    private func markReceived() {
        lock.lock()
        received = true
        lock.unlock()
    }
}

/// playhead-xc6b: post an interruption and return only once a WITNESS
/// subscriber has demonstrably received it.
///
/// This is the barrier half of the prove-a-negative idiom, and it exists
/// because the obvious cheaper version does not work. `center.post` is
/// synchronous, so it is tempting to argue that a following actor read is
/// automatically queued behind whatever a live observer was resumed to do.
/// MEASURED, THAT ARGUMENT IS FALSE: with a live observer and an installed
/// item, a single post followed immediately by `snapshot()` still read the
/// PRE-resume state. Resumption of the notification sequence is not
/// completed inside `post`.
///
/// So we enqueue a subscriber of our own, of the same kind and on the same
/// executor, and wait for IT to be resumed and run. `PlaybackService`'s
/// observer registered earlier, so for any given post its continuation is
/// resumed first and its actor job is enqueued first; observing the
/// witness's job therefore proves the service observer's job already ran,
/// and a read issued afterwards is behind both.
///
/// Posts are repeated because `notifications(named:)` only delivers
/// notifications posted after its own registration — a single pre-
/// registration post would be dropped and never redelivered.
///
/// Honest limit, the same one carried by `drainOrchestratorEffects`:
/// same-priority FIFO on an actor's default executor is an implementation
/// property, not a language guarantee.
private func postInterruptionUntilWitnessed(
    _ center: NotificationCenter,
    type: AVAudioSession.InterruptionType,
    options: AVAudioSession.InterruptionOptions = []
) async -> Bool {
    let witness = InterruptionWitness()
    witness.start(center: center)
    defer { witness.stop() }
    let rawType = type.rawValue
    let rawOptions = options.rawValue
    return await pollUntil {
        var userInfo: [AnyHashable: Any] = [
            AVAudioSessionInterruptionTypeKey: rawType,
        ]
        if rawOptions != 0 {
            userInfo[AVAudioSessionInterruptionOptionKey] = rawOptions
        }
        center.post(
            name: AVAudioSession.interruptionNotification,
            object: nil,
            userInfo: userInfo
        )
        return witness.hasReceived
    }
}

private actor ItemSeekRecorder {
    private var callCount = 0

    func record() {
        callCount += 1
    }

    func count() -> Int {
        callCount
    }
}

// MARK: - Actor Isolation

@Suite("PlaybackServiceActor – Isolation", .timeLimit(.minutes(3)))
struct PlaybackServiceActorIsolationTests {

    @Test("PlaybackService methods are callable from PlaybackServiceActor",
          .timeLimit(.minutes(3)))
    func basicActorAccess() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        let snapshot = await service.snapshot()
        #expect(snapshot.status == .idle)
    }

    @Test("State observation stream yields from actor without assertion",
          .timeLimit(.minutes(3)))
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

    @Test("Concurrent snapshot calls don't trigger executor assertion",
          .timeLimit(.minutes(3)))
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

    @Test("A detach token rejects a stale queued item installation",
          .timeLimit(.minutes(3)))
    func detachTokenRejectsStaleLoad() async throws {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        let staleAdmission = try #require(
            await service.pauseAndDetachCurrentItem(force: false)
        )
        #expect(
            await service.snapshot().status == .idle,
            "The first no-item admission must not synthesize a pause edge"
        )

        let currentAdmission = try #require(
            await service.pauseAndDetachCurrentItem()
        )
        #expect(currentAdmission != staleAdmission)

        let staleItem = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string: "playhead-progressive://audio/stale-load.mp3"
                )!
            )
        )
        #expect(
            !(await service.loadItem(
                staleItem,
                ifCurrentItemGeneration: staleAdmission
            ))
        )
        #expect(!(await service._testingHasPlayerItem))
        #expect(!(await service._testingHasCurrentPlayerItem))

        let currentItem = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string: "playhead-progressive://audio/current-load.mp3"
                )!
            )
        )
        #expect(
            await service.loadItem(
                currentItem,
                ifCurrentItemGeneration: currentAdmission
            )
        )
        #expect(await service._testingHasPlayerItem)
        #expect(await service._testingHasCurrentPlayerItem)
        await service.tearDown()
    }

    @Test("A detached item cannot resurrect Now Playing through stale rate delivery",
          .timeLimit(.minutes(3)))
    func detachedItemRejectsStaleRateNowPlayingUpdate() async {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string: "playhead-progressive://audio/rate-stale.mp3"
                )!
            )
        )
        await service.loadItem(item)
        await service.setNowPlayingMetadata(title: "Old episode")
        let installed = await service.snapshotWithItemGeneration()
        #expect(nowPlaying.info != nil)

        _ = await service.pauseAndDetachCurrentItem()
        #expect(nowPlaying.info == nil)

        await service._testingApplyObservedRate(
            0,
            expectedGeneration: installed.itemGeneration
        )
        #expect(
            nowPlaying.info == nil,
            "A queued rate callback from the detached generation must not recreate Now Playing"
        )
        await service.pause()
        #expect(
            nowPlaying.info == nil,
            "A later Pause command without an item must not recreate Now Playing"
        )
        await service.tearDown()
    }

    @Test("Replacement metadata survives controls before its item installs",
          .timeLimit(.minutes(3)))
    func replacementMetadataSurvivesItemlessLoadingGap() async throws {
        let nowPlaying = FakeNowPlayingInfoProvider()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: nowPlaying,
            notificationCenter: NotificationCenter()
        )
        let oldItem = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string: "playhead-progressive://audio/metadata-old.mp3"
                )!
            )
        )
        await service.loadItem(oldItem)
        await service.setNowPlayingMetadata(title: "Old episode")

        let admission = try #require(
            await service.pauseAndDetachCurrentItem()
        )
        #expect(nowPlaying.info == nil)

        await service.setNowPlayingMetadata(title: "Replacement episode")
        await service.pause()
        #expect(
            nowPlaying.info?[MPMediaItemPropertyTitle] as? String
                == "Replacement episode",
            "A control event in the loading gap must preserve pending replacement metadata"
        )

        let replacement = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string: "playhead-progressive://audio/metadata-new.mp3"
                )!
            )
        )
        let installed = await service.loadItem(
            replacement,
            ifCurrentItemGeneration: admission
        )
        #expect(installed)
        await service.pause()
        #expect(
            nowPlaying.info?[MPMediaItemPropertyTitle] as? String
                == "Replacement episode",
            "Installing the admitted item must retain the metadata published before loading"
        )
        await service.tearDown()
    }

    @Test("A replaced item cannot publish an old seek after actor reentrancy",
          .timeLimit(.minutes(3)))
    func replacedItemRejectsStaleSeekCompletion() async {
        let controlledSeek = ControlledItemSeek()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await controlledSeek.perform()
            }
        )
        let itemA = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/a.mp3")!
            )
        )
        let itemB = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/b.mp3")!
            )
        )

        await service.loadItem(itemA)
        let context = await service.snapshotWithItemGeneration()
        let seekTask = Task {
            await service.seek(
                to: 42,
                ifCurrentItemGeneration: context.itemGeneration
            )
        }

        await controlledSeek.waitUntilStarted()
        await service.loadItem(itemB)
        await controlledSeek.release()

        #expect(await seekTask.value == false)
        #expect(
            await service.snapshot().currentTime != 42,
            "The old item's completion must not overwrite the replacement item's state"
        )
    }

    @Test("Newest overlapping user seek wins on the same item",
          .timeLimit(.minutes(3)))
    func newestSameItemSeekWinsWhenOlderCompletionArrivesLast() async {
        let controlledSeeks = ControlledSeekSequence()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await controlledSeeks.perform()
            }
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string:
                        "playhead-progressive://audio/latest-seek-wins.mp3"
                )!
            )
        )
        await service.loadItem(item)
        let context = await service.snapshotWithItemGeneration()

        let olderSeek = Task {
            await service.seek(
                to: 40,
                ifCurrentItemGeneration: context.itemGeneration
            )
        }
        await controlledSeeks.waitUntilStarted(0)
        let newerSeek = Task {
            await service.seek(
                to: 90,
                ifCurrentItemGeneration: context.itemGeneration
            )
        }
        await controlledSeeks.waitUntilStarted(1)

        await controlledSeeks.release(1, completed: true)
        #expect(await newerSeek.value)
        #expect(await service.snapshot().currentTime == 90)

        await controlledSeeks.release(0, completed: true)
        #expect(
            !(await olderSeek.value),
            "The superseded operation must reject even if its injected seek reports success"
        )
        #expect(
            await service.snapshot().currentTime == 90,
            "An older overlapping completion must not publish over the newest target"
        )
        await service.tearDown()
    }

    @Test("A replaced item cannot receive stale skip-transition completion",
          .timeLimit(.minutes(3)))
    func replacedItemRejectsStaleSkipTransitionCompletion() async {
        let controlledSeek = ControlledItemSeek()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await controlledSeek.perform()
            }
        )
        let itemA = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/skip-a.mp3")!
            )
        )
        let itemB = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/skip-b.mp3")!
            )
        )

        await service.loadItem(itemA)
        let originalVolume = await service._testingPlayerVolume
        let transition = Task {
            await service._testingPerformSkipTransition(to: 120)
        }
        await controlledSeek.waitUntilStarted()
        #expect(
            await service._testingPlayerVolume
                == PlaybackService._testingDuckVolume
        )

        await service.loadItem(itemB)
        #expect(
            await service._testingPlayerVolume == originalVolume,
            "Installing the replacement must clear the old item's duck"
        )
        await controlledSeek.release()
        await transition.value

        #expect(
            await service.snapshot().currentTime != 120,
            "The old cue completion must not publish into the replacement"
        )
        #expect(await service._testingPlayerVolume == originalVolume)
    }

    @Test("A failed item seek releases the active transition duck",
          .timeLimit(.minutes(3)))
    func failedItemSeekRestoresVolume() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in false }
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string:
                        "playhead-progressive://audio/seek-failure.mp3"
                )!
            )
        )
        await service.loadItem(item)
        let originalVolume = await service._testingPlayerVolume

        await service._testingPerformSkipTransition(to: 120)

        #expect(
            await service._testingPlayerVolume == originalVolume,
            "A false seek completion must not strand playback at duck volume"
        )
        #expect(
            await service.snapshot().currentTime != 120,
            "A failed seek must not publish the cue target"
        )
    }

    @Test("Listen disarm invalidates an in-flight transition for the same cue",
          .timeLimit(.minutes(3)))
    func listenDisarmInvalidatesInFlightTransition() async {
        let controlledSeek = ControlledItemSeek()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await controlledSeek.perform()
            }
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/listen.mp3")!
            )
        )
        await service.loadItem(item)
        let originalVolume = await service._testingPlayerVolume
        let transition = Task {
            await service._testingPerformSkipTransition(to: 119)
        }
        await controlledSeek.waitUntilStarted()
        #expect(
            await service._testingPlayerVolume
                == PlaybackService._testingDuckVolume
        )

        await service.disarmSkipCues(
            overlappingStart: 60,
            end: 120
        )
        #expect(await service._testingPlayerVolume == originalVolume)
        await controlledSeek.release()
        await transition.value

        #expect(
            await service.snapshot().currentTime != 119,
            "The invalidated cue transition must not defeat Listen"
        )
    }

    @Test("Cue removal cannot cancel an overlapping user seek",
          .timeLimit(.minutes(3)))
    func cueRemovalDoesNotCancelOverlappingUserSeek() async {
        let controlledSeeks = ControlledSeekSequence()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await controlledSeeks.perform()
            }
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/user-seek-wins.mp3")!
            )
        )
        await service.loadItem(item)
        await service.setSkipCues([
            CMTimeRange(
                start: CMTime(seconds: 60, preferredTimescale: 600),
                end: CMTime(seconds: 120, preferredTimescale: 600)
            ),
        ])

        // Park the automatic skip inside AVFoundation's seek, then start a
        // user seek while the actor is re-entrant. The user seek must retire
        // the automatic transition before it begins.
        let transition = Task {
            await service._testingPerformSkipTransition(
                cueStart: 60,
                cueEnd: 120
            )
        }
        await controlledSeeks.waitUntilStarted(0)
        let context = await service.snapshotWithItemGeneration()
        let userSeek = Task {
            await service.seek(
                to: 42,
                ifCurrentItemGeneration: context.itemGeneration
            )
        }
        await controlledSeeks.waitUntilStarted(1)

        // A gate flip after the user seek starts must not cancel all pending
        // seeks on the item. Model AVFoundation reporting the old automatic
        // seek as cancelled and the newer user seek as completed.
        await service.setSkipCues([])
        await controlledSeeks.release(1, completed: true)
        #expect(await userSeek.value)
        #expect(await service.snapshot().currentTime == 42)

        await controlledSeeks.release(0, completed: false)
        await transition.value
        #expect(
            await service.snapshot().currentTime == 42,
            "The cancelled automatic skip must not overwrite the user seek"
        )
    }

    @Test("Listen disarm invalidates an in-flight merged-pod transition",
          .timeLimit(.minutes(3)))
    func listenDisarmInvalidatesMergedPodTransition() async {
        let controlledSeek = ControlledItemSeek()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await controlledSeek.perform()
            }
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/merged-pod.mp3")!
            )
        )
        await service.loadItem(item)
        await service.setSkipCues([
            CMTimeRange(
                start: CMTime(seconds: 60, preferredTimescale: 600),
                end: CMTime(seconds: 150, preferredTimescale: 600)
            ),
        ])
        let originalVolume = await service._testingPlayerVolume
        let transition = Task {
            await service._testingPerformSkipTransition(
                cueStart: 60,
                cueEnd: 150
            )
        }
        await controlledSeek.waitUntilStarted()

        // The banner represents only the first ad in a cue merged across an
        // adjacent second ad. Its restored span does not contain the pod-end
        // seek target, but it does overlap the transition's owning cue.
        await service.disarmSkipCues(
            overlappingStart: 60,
            end: 90
        )
        #expect(await service._testingPlayerVolume == originalVolume)
        #expect(await service._testingSkipCues.isEmpty)
        await controlledSeek.release()
        await transition.value

        #expect(
            await service.snapshot().currentTime != 150,
            "The merged-pod transition must not skip past the restored first ad"
        )
    }

    @Test("Listen disarm invalidates a detected transition before queued execution",
          .timeLimit(.minutes(3)))
    func listenDisarmInvalidatesReservedTransition() async throws {
        let seekRecorder = ItemSeekRecorder()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await seekRecorder.record()
                return true
            }
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/reserved.mp3")!
            )
        )
        await service.loadItem(item)
        let originalVolume = await service._testingPlayerVolume

        // Model checkSkipCues detecting and synchronously claiming the cue,
        // followed by Listen winning the actor queue before the transition's
        // unstructured async execution begins.
        let transitionToken = try #require(
            await service._testingReserveSkipTransition(
                cueStart: 60,
                cueEnd: 120
            )
        )
        await service.disarmSkipCues(
            overlappingStart: 60,
            end: 90
        )
        await service._testingExecuteReservedSkipTransition(
            transitionToken: transitionToken
        )

        #expect(await seekRecorder.count() == 0)
        #expect(await service.snapshot().currentTime != 120)
        #expect(await service._testingPlayerVolume == originalVolume)
    }

    @Test("Removing a cue invalidates its detected transition before queued execution",
          .timeLimit(.minutes(3)))
    func cueRemovalInvalidatesReservedTransition() async throws {
        let seekRecorder = ItemSeekRecorder()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await seekRecorder.record()
                return true
            }
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/gate-flip.mp3")!
            )
        )
        await service.loadItem(item)
        await service.setSkipCues([
            CMTimeRange(
                start: CMTime(seconds: 60, preferredTimescale: 600),
                end: CMTime(seconds: 120, preferredTimescale: 600)
            ),
        ])
        let originalVolume = await service._testingPlayerVolume

        // Model the periodic observer claiming a cue immediately before an
        // eligibility/gate update removes that cue from the active set.
        let transitionToken = try #require(
            await service._testingReserveSkipTransition(
                cueStart: 60,
                cueEnd: 120
            )
        )
        await service.setSkipCues([])
        await service._testingExecuteReservedSkipTransition(
            transitionToken: transitionToken
        )

        #expect(await seekRecorder.count() == 0)
        #expect(await service.snapshot().currentTime != 120)
        #expect(await service._testingPlayerVolume == originalVolume)
    }

    @Test("Tear down invalidates a detected transition before queued execution",
          .timeLimit(.minutes(3)))
    func tearDownInvalidatesReservedTransition() async throws {
        let seekRecorder = ItemSeekRecorder()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter(),
            itemSeekOperation: { _, _ in
                await seekRecorder.record()
                return true
            }
        )
        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/teardown.mp3")!
            )
        )
        await service.loadItem(item)
        let originalVolume = await service._testingPlayerVolume
        let transitionToken = try #require(
            await service._testingReserveSkipTransition(
                cueStart: 60,
                cueEnd: 120
            )
        )

        await service.tearDown()
        await service._testingExecuteReservedSkipTransition(
            transitionToken: transitionToken
        )

        #expect(await seekRecorder.count() == 0)
        #expect(await service.snapshot().currentTime != 120)
        #expect(await service._testingPlayerVolume == originalVolume)
    }

    @Test("Tear down joins setup, releases the current item, and ignores later interruptions",
          .timeLimit(.minutes(3)))
    func tearDownOwnsEveryLongLivedResource() async {
        let center = NotificationCenter()
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: center
        )

        // playhead-xc6b: prove the interruption observer is INSTALLED AND
        // SUBSCRIBED before we tear it down. Without this the test could tear
        // down before `setupTask` ever reached `observeInterruptionsAsync()`,
        // in which case "a joined interruption observer cannot resume a
        // torn-down service" was asserted about an observer that never
        // existed. Runs before `loadItem` so item-status KVO cannot race the
        // observed `.paused`.
        #expect(
            await driveInterruptionObserverUntilLive(service, center: center),
            "the interruption observer must be live before tear down, or the post-teardown assertion is vacuous"
        )

        let item = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string: "playhead-progressive://audio/owned-teardown.mp3"
                )!
            )
        )
        await service.loadItem(item)
        await service._testingInjectState(
            PlaybackState(
                status: .paused,
                currentTime: 20,
                duration: 100,
                rate: 0,
                playbackSpeed: 1
            )
        )
        #expect(await service._testingHasPlayerItem)
        #expect(await service._testingHasCurrentPlayerItem)

        await service.tearDown()

        #expect(await service._testingIsTornDown)
        #expect(!(await service._testingHasPlayerItem))
        #expect(!(await service._testingHasCurrentPlayerItem))
        let terminalSnapshot = await service.snapshot()
        #expect(!(await service.seek(to: 42)))
        #expect(
            await service.snapshot() == terminalSnapshot,
            "post-teardown seek must not mutate the terminal state"
        )
        let stalePostShutdownItem = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(
                    string:
                        "playhead-progressive://audio/stale-after-teardown.mp3"
                )!
            )
        )
        await service.loadItem(stalePostShutdownItem)
        #expect(
            !(await service._testingHasPlayerItem),
            "a cancelled runtime prefetch must not resurrect a torn-down item"
        )
        #expect(!(await service._testingHasCurrentPlayerItem))

        // playhead-xc6b — FAIL-OPEN FIX. This used to be a bare `post` plus
        // `for _ in 0..<5 { await Task.yield() }`, a fixed budget guarding a
        // NEGATIVE assertion. Under load that fails OPEN: "the service did
        // not resume" could be true simply because the notification had not
        // been processed YET, so the assertion passed vacuously and the suite
        // got WEAKER under load rather than merely flakier.
        //
        // Replaced with the repo's prove-a-negative idiom (see
        // `drainOrchestratorEffects` / `awaitTrustFalseSkipSignals` in
        // TestHelpers.swift): a positive control first, then a barrier.
        //   * The control is at the top of this test — the observer was
        //     driven and demonstrably reacted to a post on THIS center, so
        //     the stimulus is known to be well formed and an observer is
        //     known to have existed for `tearDown()` to join.
        //   * The barrier is `postInterruptionUntilWitnessed`, which returns
        //     only after a subscriber of the same kind, on the same executor,
        //     has been resumed and run. This is NOT belt-and-braces: a plain
        //     post followed by an immediate `snapshot()` was measured reading
        //     the pre-resume state even with a live observer, so without the
        //     witness this assertion would still be racing rather than
        //     asserting.
        #expect(
            await postInterruptionUntilWitnessed(
                center,
                type: .ended,
                options: .shouldResume
            ),
            "the resume stimulus must be delivered, or the assertion below is vacuous"
        )
        #expect(
            await service.snapshot().status == .paused,
            "a joined interruption observer cannot resume a torn-down service"
        )
    }

    @Test("A state subscriber added after tear down hydrates once and finishes",
          .timeLimit(.minutes(3)))
    func postTearDownStateSubscriberFinishes() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        await service.tearDown()

        let stream = await service.observeStates()
        var iterator = stream.makeAsyncIterator()
        let terminalSnapshot = await iterator.next()

        #expect(terminalSnapshot?.status == .paused)
        #expect(await iterator.next() == nil)
    }

    @Test(
        "Interruption observation does not retain a released playback service",
        .timeLimit(.minutes(3))
    )
    func interruptionObserverDoesNotCreateRetainCycle() async {
        // playhead-xc6b: this test deliberately does NOT call `tearDown()`.
        // Tear-down cancels and joins the interruption observer, which is the
        // very reference graph under test — tearing down first would make the
        // service release for a reason that has nothing to do with the
        // observer, and the test would pass while proving nothing.
        let center = NotificationCenter()
        weak var weakService: PlaybackService?

        // playhead-xc6b — FAIL-OPEN FIX + ASSERTIONS. Previously this was
        // `for _ in 0..<5 { await Task.yield() }` with the comment "let the
        // retained setup hop install the interruption sequence", and the test
        // carried ZERO `#expect`s. Both defects compound: under load the
        // yield budget can expire before `setupTask` reaches
        // `observeInterruptionsAsync()`, so the latch fires trivially because
        // THE OBSERVER WAS NEVER INSTALLED — no observer, no possible cycle,
        // vacuous pass, and nothing in the test could say so.
        //
        // The positive control drives the observer until it demonstrably
        // reacts, so the reference graph under test is known to exist before
        // the service is dropped. Then the negative — "nothing the observer
        // owns retains the service" — is asserted twice: the latch resumes at
        // the exact moment of deallocation (a real cycle keeps it silent and
        // the `.timeLimit` trips), and the weak reference is checked after.
        func makeObservedServiceLatch() async -> DeallocLatch {
            let service = await PlaybackService(
                audioSession: FakeAudioSessionProvider(),
                nowPlayingInfo: FakeNowPlayingInfoProvider(),
                notificationCenter: center
            )
            weakService = service
            #expect(
                await driveInterruptionObserverUntilLive(service, center: center),
                "the interruption observer must be installed and subscribed, or this test proves nothing about a cycle through it"
            )
            return attachDeallocLatch(to: service)
        }

        let deallocated = await makeObservedServiceLatch()
        // Event-driven and unbounded by design: resumes exactly when the
        // service deallocates. A genuine cycle through the interruption
        // observer leaves the latch silent and the test fails on its
        // `.timeLimit` backstop instead of hanging the gate host.
        await deallocated.wait()
        #expect(
            weakService == nil,
            "a live interruption observer must not retain the playback service it observes for"
        )
    }

    @Test("A replaced item rejects a queued periodic-time callback",
          .timeLimit(.minutes(3)))
    func replacedItemRejectsStalePeriodicTimeDelivery() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        let itemA = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/time-a.mp3")!
            )
        )
        let itemB = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/time-b.mp3")!
            )
        )

        await service.loadItem(itemA)
        let itemAContext = await service.snapshotWithItemGeneration()
        await service.loadItem(itemB)
        await service._testingDeliverPeriodicTime(
            CMTime(seconds: 9_999, preferredTimescale: 600),
            for: itemA,
            expectedGeneration: itemAContext.itemGeneration
        )

        #expect(
            await service.snapshot().currentTime != 9_999,
            "A queued periodic tick from A must not publish into B"
        )
    }

    @Test("Listen disarm removes every cue overlapping the banner span",
          .timeLimit(.minutes(3)))
    func listenDisarmRemovesEveryOverlappingCue() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        await service.setSkipCues([
            CMTimeRange(
                start: CMTime(seconds: 65, preferredTimescale: 600),
                end: CMTime(seconds: 95, preferredTimescale: 600)
            ),
            CMTimeRange(
                start: CMTime(seconds: 110, preferredTimescale: 600),
                end: CMTime(seconds: 130, preferredTimescale: 600)
            ),
            CMTimeRange(
                start: CMTime(seconds: 200, preferredTimescale: 600),
                end: CMTime(seconds: 230, preferredTimescale: 600)
            ),
        ])

        await service.disarmSkipCues(
            overlappingStart: 60,
            end: 120
        )

        let remaining = await service._testingSkipCues
        #expect(remaining.count == 1)
        #expect(CMTimeGetSeconds(remaining[0].start) == 200)
    }

    @Test("A stale item generation cannot disarm replacement cues",
          .timeLimit(.minutes(3)))
    func staleItemGenerationCannotDisarmReplacementCues() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        let itemA = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/disarm-a.mp3")!
            )
        )
        let itemB = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/disarm-b.mp3")!
            )
        )
        await service.loadItem(itemA)
        let itemAContext = await service.snapshotWithItemGeneration()
        await service.loadItem(itemB)
        await service.setSkipCues([
            CMTimeRange(
                start: CMTime(seconds: 60, preferredTimescale: 600),
                end: CMTime(seconds: 120, preferredTimescale: 600)
            ),
        ])

        #expect(
            !(await service.disarmSkipCues(
                overlappingStart: 60,
                end: 120,
                ifCurrentItemGeneration: itemAContext.itemGeneration
            ))
        )
        #expect(
            await service._testingSkipCues.count == 1,
            "The replacement item's cue must survive a stale Listen actor hop"
        )
    }

    @Test("A replaced item cannot publish an already-queued status callback",
          .timeLimit(.minutes(3)))
    func replacedItemRejectsStaleStatusDelivery() async {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )
        let itemA = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/status-a.mp3")!
            )
        )
        let itemB = AVPlayerItem(
            asset: AVURLAsset(
                url: URL(string: "playhead-progressive://audio/status-b.mp3")!
            )
        )

        await service.loadItem(itemA)
        let itemAContext = await service.snapshotWithItemGeneration()

        // Model a ready-to-play KVO delivery captured for A, queued outside
        // the actor, and released only after B becomes the current item.
        await service.loadItem(itemB)
        await service._testingDeliverItemStatus(
            .readyToPlay,
            duration: 9_999,
            for: itemA,
            expectedGeneration: itemAContext.itemGeneration
        )

        #expect(
            await service.snapshot().duration != 9_999,
            "A queued callback from the old item must not publish its duration as B's"
        )
    }
}

// MARK: - Progressive Loader Decoupling

@Suite("PlaybackService – Progressive Loader Decoupling", .timeLimit(.minutes(3)))
struct ProgressiveLoaderDecouplingTests {

    @Test("loadItem accepts externally-created player item",
          .timeLimit(.minutes(3)))
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

    @Test("ProgressiveResourceLoader operates independently of PlaybackServiceActor",
          .timeLimit(.minutes(3)))
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

    @Test("Simultaneous actor access and loader callbacks don't conflict",
          .timeLimit(.minutes(3)))
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
        //
        // playhead-xc6b: the snapshots are COUNTED and CHECKED rather than
        // discarded. This test previously ended `_ = loader` with no `#expect`
        // at all, so it could only fail by crashing or timing out — a version
        // of `toggleSuspendResume` that did nothing, or a task group that
        // exited before either side ran, passed identically.
        let idleSnapshots = await withTaskGroup(of: Int.self) { group in
            // Actor-side: rapid snapshot reads (simulating KVO storm).
            group.addTask {
                var idleCount = 0
                for _ in 0..<50 {
                    if await service.snapshot().status == .idle {
                        idleCount += 1
                    }
                }
                return idleCount
            }

            group.addTask {
                await loaderDriver.toggleSuspendResume(times: 50)
                return 0
            }
            return await group.reduce(0, +)
        }

        // Both workloads must have run to completion, and every actor-side
        // read must have observed a consistent state: no media was ever
        // loaded, so `.idle` is the only legal status. A torn read or an
        // executor conflict shows up here rather than only as a crash.
        #expect(
            idleSnapshots == 50,
            "every concurrent snapshot must observe the consistent .idle state"
        )
        #expect(
            await loaderDriver.completedToggles == 50,
            "the loader side must complete its full suspend/resume workload alongside the actor reads"
        )
        await service.tearDown()
    }
}

// MARK: - Callback Isolation

@Suite("PlaybackService – Callback Isolation", .timeLimit(.minutes(3)))
struct PlaybackServiceCallbackIsolationTests {

    /// 1×1 pixel test image, cheap to create and sufficient for artwork tests.
    private static var testImage: UIImage {
        UIGraphicsImageRenderer(size: CGSize(width: 1, height: 1))
            .image { $0.fill(CGRect(x: 0, y: 0, width: 1, height: 1)) }
    }

    @Test("setNowPlayingMetadata with artwork doesn't crash from actor",
          .timeLimit(.minutes(3)))
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

    @Test("MPMediaItemArtwork provider callable from main thread",
          .timeLimit(.minutes(3)))
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

    @Test("loadItem triggers KVO without executor assertion", .timeLimit(.minutes(3)))
    func loadItemKVOSafe() async throws {
        let service = await PlaybackService(
            audioSession: FakeAudioSessionProvider(),
            nowPlayingInfo: FakeNowPlayingInfoProvider(),
            notificationCenter: NotificationCenter()
        )

        // Subscribe BEFORE loadItem so we never miss the KVO-driven
        // transition. observeStates() yields the current snapshot (.idle)
        // immediately on subscribe.
        let stream = await service.observeStates()

        // Create a player item from a progressive URL. The asset won't load
        // real audio, but AVPlayerItem will still fire status KVO (to .unknown
        // or .failed) which exercises the nonisolated KVO callback path.
        let asset = AVURLAsset(url: URL(string: "playhead-progressive://audio/kvo-test.mp3")!)
        let item = AVPlayerItem(asset: asset)

        await service.loadItem(item)

        // playhead-vsot round 3: drain the state stream on the test's own
        // task until the KVO-driven status change lands, instead of a
        // fixed 100 ms Task.sleep that could expire before the callback
        // fires under the parallel gate. The stream IS the signal — the
        // nonisolated KVO closure hops to PlaybackServiceActor and yields
        // the new state to every observer. Unbounded; the `.timeLimit`
        // trait is the hang backstop. (If the KVO closure were
        // actor-tainted we would have crashed before reaching here — the
        // original isolation-safety contract still holds.)
        var sawNonIdle = false
        for await state in stream {
            if state.status != .idle {
                sawNonIdle = true
                break
            }
        }
        #expect(sawNonIdle,
                "loadItem must drive a KVO status change off .idle")
    }

    @Test("State updates after play don't crash",
          .timeLimit(.minutes(3)))
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

    @Test("Concurrent metadata and snapshot access",
          .timeLimit(.minutes(3)))
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
