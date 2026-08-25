import Foundation
import Testing
@testable import Playhead

/// playhead-882eg PROBE (temporary measurement scaffolding).
///
/// The test host holds a descriptor FLOOR of ~453 for the whole tail of every
/// full-plan run — 163 on the PRODUCTION `analysis.sqlite`, 153 on the
/// production `ad_catalog.sqlite`, 81 on distinct `surface-status-*.jsonl`
/// files. The fd dump names the FILES; it cannot say which OBJECT holds them.
/// `PlayheadRuntime` and its stores have different lifetimes — the stores are
/// captured strongly by an uncancellable bootstrap `Task` and installed into
/// long-lived service actors — so "the runtime is retained" and "the store is
/// retained" are two different claims. This separates them.
///
/// Descriptor COUNTS are deliberately NOT taken here: Swift cannot call the
/// variadic `fcntl`, and the by-path census belongs to `scripts/gate-fd-paths.py`
/// running against the whole plan. What this file measures is OBJECT LIFETIME.
private final class WeakBox<T: AnyObject> {
    weak var value: T?
    let label: String
    init(_ value: T, _ label: String) {
        self.value = value
        self.label = label
    }
    var state: String { value == nil ? "released" : "RETAINED" }
}

@MainActor
@Suite("playhead-882eg probe: what survives a dropped PlayheadRuntime", .serialized)
struct RuntimeGraphRetentionProbeTests {
    /// Suspend for real (so the main queue can idle and drain) rather than
    /// spinning `Task.yield()`, which keeps the MainActor busy and starves the
    /// very drain being waited on — the mistake `playhead-vsot` documents in
    /// `RuntimeShutdownLifecycleTests`.
    private func drain(_ seconds: Double) async {
        let deadline = Date().addingTimeInterval(seconds)
        while Date() < deadline {
            try? await Task.sleep(for: .milliseconds(100))
        }
    }

    private func report(_ name: String, _ boxes: [WeakBox<AnyObject>]) {
        let live = boxes.filter { $0.value != nil }.map(\.label)
        let dead = boxes.filter { $0.value == nil }.map(\.label)
        print("[882eg-probe] \(name) RETAINED[\(live.count)]: \(live.joined(separator: " "))")
        print("[882eg-probe] \(name) released[\(dead.count)]: \(dead.joined(separator: " "))")
    }

    @Test("preview runtime, no shutdown", .timeLimit(.minutes(2)))
    func previewNoShutdown() async throws {
        var boxes: [WeakBox<AnyObject>] = []
        await {
            let runtime = PlayheadRuntime(isPreviewRuntime: true)
            boxes = [
                WeakBox<AnyObject>(runtime, "runtime"),
                WeakBox<AnyObject>(runtime.analysisStore, "analysisStore"),
                WeakBox<AnyObject>(runtime.surfaceStatusLogger, "surfaceLogger"),
                WeakBox<AnyObject>(runtime.analysisWorkScheduler, "workScheduler"),
                WeakBox<AnyObject>(runtime.analysisCoordinator, "analysisCoordinator"),
                WeakBox<AnyObject>(runtime.entitlementManager, "entitlementManager"),
                WeakBox<AnyObject>(runtime.downloadManager, "downloadManager"),
                WeakBox<AnyObject>(runtime.backgroundProcessingService, "bgProcessing"),
                WeakBox<AnyObject>(runtime.analysisJobRunner, "jobRunner"),
                WeakBox<AnyObject>(runtime.skipOrchestrator, "skipOrchestrator"),
                WeakBox<AnyObject>(runtime.adDetectionService, "adDetection"),
                WeakBox<AnyObject>(runtime.capabilitiesService, "capabilities"),
                WeakBox<AnyObject>(runtime.playbackService, "playbackService"),
                WeakBox<AnyObject>(runtime.trustService, "trustService"),
                WeakBox<AnyObject>(runtime.surfaceStatusObserver, "surfaceObserver"),
                WeakBox<AnyObject>(runtime.lanePreemptionCoordinator, "lanePreemption"),
                WeakBox<AnyObject>(runtime.analysisJobReconciler, "jobReconciler"),
                WeakBox<AnyObject>(runtime.transcriptEngine, "transcriptEngine"),
                WeakBox<AnyObject>(runtime.speechService, "speechService"),
                WeakBox<AnyObject>(runtime.audioService, "audioService"),
                WeakBox<AnyObject>(runtime.silenceCompressionCoordinator, "silenceCompression"),
                WeakBox<AnyObject>(runtime.analysisStoreRecovery, "storeRecovery")
            ]
        }()
        await drain(8)
        report("previewNoShutdown", boxes)
    }

    @Test("preview runtime, WITH shutdown", .timeLimit(.minutes(2)))
    func previewWithShutdown() async throws {
        var boxes: [WeakBox<AnyObject>] = []
        await {
            let runtime = PlayheadRuntime(isPreviewRuntime: true)
            boxes = [
                WeakBox<AnyObject>(runtime, "runtime"),
                WeakBox<AnyObject>(runtime.analysisStore, "analysisStore"),
                WeakBox<AnyObject>(runtime.surfaceStatusLogger, "surfaceLogger"),
                WeakBox<AnyObject>(runtime.analysisWorkScheduler, "workScheduler"),
                WeakBox<AnyObject>(runtime.analysisCoordinator, "analysisCoordinator"),
                WeakBox<AnyObject>(runtime.entitlementManager, "entitlementManager"),
                WeakBox<AnyObject>(runtime.downloadManager, "downloadManager"),
                WeakBox<AnyObject>(runtime.backgroundProcessingService, "bgProcessing"),
                WeakBox<AnyObject>(runtime.analysisJobRunner, "jobRunner"),
                WeakBox<AnyObject>(runtime.skipOrchestrator, "skipOrchestrator"),
                WeakBox<AnyObject>(runtime.adDetectionService, "adDetection"),
                WeakBox<AnyObject>(runtime.capabilitiesService, "capabilities"),
                WeakBox<AnyObject>(runtime.playbackService, "playbackService"),
                WeakBox<AnyObject>(runtime.trustService, "trustService"),
                WeakBox<AnyObject>(runtime.surfaceStatusObserver, "surfaceObserver"),
                WeakBox<AnyObject>(runtime.lanePreemptionCoordinator, "lanePreemption"),
                WeakBox<AnyObject>(runtime.analysisJobReconciler, "jobReconciler"),
                WeakBox<AnyObject>(runtime.transcriptEngine, "transcriptEngine"),
                WeakBox<AnyObject>(runtime.speechService, "speechService"),
                WeakBox<AnyObject>(runtime.audioService, "audioService"),
                WeakBox<AnyObject>(runtime.silenceCompressionCoordinator, "silenceCompression"),
                WeakBox<AnyObject>(runtime.analysisStoreRecovery, "storeRecovery")
            ]
            await runtime.shutdown()
        }()
        await drain(8)
        report("previewWithShutdown", boxes)
    }

    @Test("non-preview runtime, WITH shutdown", .timeLimit(.minutes(2)))
    func nonPreviewWithShutdown() async throws {
        var boxes: [WeakBox<AnyObject>] = []
        await {
            let runtime = PlayheadRuntime(isPreviewRuntime: false)
            boxes = [
                WeakBox<AnyObject>(runtime, "runtime"),
                WeakBox<AnyObject>(runtime.analysisStore, "analysisStore"),
                WeakBox<AnyObject>(runtime.surfaceStatusLogger, "surfaceLogger"),
                WeakBox<AnyObject>(runtime.analysisWorkScheduler, "workScheduler"),
                WeakBox<AnyObject>(runtime.analysisCoordinator, "analysisCoordinator"),
                WeakBox<AnyObject>(runtime.entitlementManager, "entitlementManager"),
                WeakBox<AnyObject>(runtime.downloadManager, "downloadManager"),
                WeakBox<AnyObject>(runtime.backgroundProcessingService, "bgProcessing"),
                WeakBox<AnyObject>(runtime.analysisJobRunner, "jobRunner"),
                WeakBox<AnyObject>(runtime.skipOrchestrator, "skipOrchestrator"),
                WeakBox<AnyObject>(runtime.adDetectionService, "adDetection"),
                WeakBox<AnyObject>(runtime.capabilitiesService, "capabilities"),
                WeakBox<AnyObject>(runtime.playbackService, "playbackService"),
                WeakBox<AnyObject>(runtime.trustService, "trustService"),
                WeakBox<AnyObject>(runtime.surfaceStatusObserver, "surfaceObserver"),
                WeakBox<AnyObject>(runtime.lanePreemptionCoordinator, "lanePreemption"),
                WeakBox<AnyObject>(runtime.analysisJobReconciler, "jobReconciler"),
                WeakBox<AnyObject>(runtime.transcriptEngine, "transcriptEngine"),
                WeakBox<AnyObject>(runtime.speechService, "speechService"),
                WeakBox<AnyObject>(runtime.audioService, "audioService"),
                WeakBox<AnyObject>(runtime.silenceCompressionCoordinator, "silenceCompression"),
                WeakBox<AnyObject>(runtime.analysisStoreRecovery, "storeRecovery")
            ]
            await runtime.shutdown()
        }()
        await drain(8)
        report("nonPreviewWithShutdown", boxes)
    }

    @Test("five preview runtimes, none shut down", .timeLimit(.minutes(3)))
    func fivePreviewRuntimes() async throws {
        var runtimes: [WeakBox<AnyObject>] = []
        var stores: [WeakBox<AnyObject>] = []
        for index in 0..<5 {
            await {
                let runtime = PlayheadRuntime(isPreviewRuntime: true)
                runtimes.append(WeakBox<AnyObject>(runtime, "r\(index)"))
                stores.append(WeakBox<AnyObject>(runtime.analysisStore, "s\(index)"))
            }()
        }
        await drain(10)
        let liveRuntimes = runtimes.filter { $0.value != nil }.count
        let liveStores = stores.filter { $0.value != nil }.count
        print("[882eg-probe] fivePreviewRuntimes liveRuntimes=\(liveRuntimes)/5 liveStores=\(liveStores)/5")
    }

    /// Which services leak WITHOUT a `PlayheadRuntime` at all.
    ///
    /// The wide probe reports 19 of 22 runtime-owned objects retained, and they
    /// are mutually referencing — so one immortal member holds the whole
    /// cluster and the cluster cannot say which member that is. Constructing
    /// each in isolation can: an object that fails to release with nothing but
    /// the test holding it is a ROOT, and an object that releases fine here is
    /// a passenger.
    @Test("standalone services, constructed with no runtime", .timeLimit(.minutes(2)))
    func standaloneServices() async throws {
        var boxes: [WeakBox<AnyObject>] = []
        await {
            let playback = PlaybackService()
            boxes.append(WeakBox<AnyObject>(playback, "PlaybackService"))
        }()
        await {
            let capabilities = CapabilitiesService()
            boxes.append(WeakBox<AnyObject>(capabilities, "CapabilitiesService"))
        }()
        await {
            let audio = AnalysisAudioService()
            boxes.append(WeakBox<AnyObject>(audio, "AnalysisAudioService"))
        }()
        await {
            let logger = SurfaceStatusInvariantLogger()
            boxes.append(WeakBox<AnyObject>(logger, "SurfaceStatusInvariantLogger"))
        }()
        await {
            let downloads = DownloadManager()
            boxes.append(WeakBox<AnyObject>(downloads, "DownloadManager"))
        }()
        if let store = try? AnalysisStore() {
            await {
                boxes.append(WeakBox<AnyObject>(store, "AnalysisStore"))
            }()
        }
        await drain(8)
        report("standaloneServices", boxes)
    }
}
