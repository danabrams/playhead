// EpisodePreparationCoordinatorTests.swift
// playhead-3xtw: tests for the user-intent "prepare this episode" trigger.
//
// The coordinator talks only to injected seams, so we assert its
// behaviour with recording doubles: it enqueues at the user-intent lane,
// is idempotent-safe, respects `cellularPolicy`, and — by construction —
// never starts playback (there is NO playback seam; the recording doubles
// capture every side effect the coordinator can perform, and none of them
// is a play call).

import Foundation
import Testing
@testable import Playhead

@Suite("EpisodePreparationCoordinator — trigger")
struct EpisodePreparationCoordinatorTests {

    // MARK: - Recording seams

    /// Records every operation the coordinator can invoke, in order, so a
    /// test can assert the FULL side-effect list (proving, e.g., that no
    /// unexpected effect — there is no playback effect to record — fired).
    actor Recorder {
        enum Op: Equatable {
            case markUserIntent(episodeId: String, coverage: Double?)
            case enqueueUserIntent(episodeId: String, fingerprint: String, coverage: Double?)
            case startDownload(episodeId: String, url: URL)
        }
        private(set) var ops: [Op] = []
        func record(_ op: Op) { ops.append(op) }
    }

    struct RecordingDownloads: EpisodePreparationDownloads {
        let recorder: Recorder
        let cached: Bool
        let fingerprint: String?

        func isCached(episodeId: String) async -> Bool { cached }
        func strongFingerprint(episodeId: String, audioURL: URL) async -> String? { fingerprint }
        func startDownload(episodeId: String, from url: URL) async {
            await recorder.record(.startDownload(episodeId: episodeId, url: url))
        }
    }

    struct RecordingAnalysis: EpisodePreparationAnalysis {
        let recorder: Recorder

        func markUserIntent(episodeId: String, desiredCoverageSec: Double?) async {
            await recorder.record(.markUserIntent(episodeId: episodeId, coverage: desiredCoverageSec))
        }
        func enqueueUserIntent(
            episodeId: String,
            podcastId: String?,
            sourceFingerprint: String,
            desiredCoverageSec: Double?,
            podcastTitle: String?,
            episodeTitle: String?
        ) async {
            await recorder.record(.enqueueUserIntent(
                episodeId: episodeId,
                fingerprint: sourceFingerprint,
                coverage: desiredCoverageSec
            ))
        }
    }

    // MARK: - Fixture

    private static let audioURL = URL(string: "https://example.com/ep1.mp3")!

    private func makeCoordinator(
        recorder: Recorder,
        cached: Bool,
        fingerprint: String?,
        reachability: TransportSnapshot.Reachability,
        policy: CellularPolicy
    ) -> EpisodePreparationCoordinator {
        EpisodePreparationCoordinator(
            downloads: RecordingDownloads(recorder: recorder, cached: cached, fingerprint: fingerprint),
            analysis: RecordingAnalysis(recorder: recorder),
            reachability: StubTransportStatusProvider(reachability: reachability, allowsCellular: true),
            cellularPolicy: { policy }
        )
    }

    private func request(coverage: Double? = 3600) -> EpisodePreparationCoordinator.Request {
        EpisodePreparationCoordinator.Request(
            episodeId: "ep-1",
            podcastId: "pod-1",
            audioURL: Self.audioURL,
            durationSec: coverage,
            podcastTitle: "Pod",
            episodeTitle: "Ep 1"
        )
    }

    // MARK: - Not downloaded → download + mark user intent (no playback)

    @Test("un-prepared episode on Wi‑Fi: starts download and marks user intent, no direct analysis, no playback")
    func testUnpreparedOnWifi() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: false, fingerprint: nil,
            reachability: .wifi, policy: .off
        )
        let outcome = await c.prepare(request(coverage: 3600))
        #expect(outcome == .startedDownload)

        let ops = await recorder.ops
        // Intent is recorded BEFORE the download so the completion enqueue
        // inherits the user-intent lane.
        #expect(ops == [
            .markUserIntent(episodeId: "ep-1", coverage: 3600),
            .startDownload(episodeId: "ep-1", url: Self.audioURL),
        ])
        // No direct analysis enqueue (there is no fingerprint yet), and —
        // critically — no playback: the op list is exactly the two
        // preparation effects, nothing else.
    }

    // MARK: - Cellular gate

    @Test("cellular + policy off: does NOT download or mark (waitingForWifi is the view's job)")
    func testCellularPolicyOffBlocks() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: false, fingerprint: nil,
            reachability: .cellular, policy: .off
        )
        #expect(await c.prepare(request()) == .waitingForWifi)

        let ops = await recorder.ops
        #expect(ops.isEmpty)
    }

    @Test("cellular + askEachTime: treated conservatively as blocked")
    func testCellularAskEachTimeBlocks() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: false, fingerprint: nil,
            reachability: .cellular, policy: .askEachTime
        )
        await c.prepare(request())
        #expect(await recorder.ops.isEmpty)
    }

    @Test("cellular + policy on: proceeds with download")
    func testCellularPolicyOnProceeds() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: false, fingerprint: nil,
            reachability: .cellular, policy: .on
        )
        await c.prepare(request())

        let ops = await recorder.ops
        #expect(ops.contains(.startDownload(episodeId: "ep-1", url: Self.audioURL)))
        #expect(ops.contains(.markUserIntent(episodeId: "ep-1", coverage: 3600)))
    }

    @Test("Wi‑Fi + policy off still proceeds (policy only gates cellular)")
    func testWifiIgnoresPolicyOff() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: false, fingerprint: nil,
            reachability: .wifi, policy: .off
        )
        await c.prepare(request())
        #expect(await recorder.ops.contains(.startDownload(episodeId: "ep-1", url: Self.audioURL)))
    }

    @Test("unreachable: does not download")
    func testUnreachableBlocks() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: false, fingerprint: nil,
            reachability: .unreachable, policy: .on
        )
        await c.prepare(request())
        #expect(await recorder.ops.isEmpty)
    }

    // MARK: - Already downloaded → direct user-intent analysis enqueue

    @Test("already-downloaded episode enqueues full analysis at the user-intent lane, no download")
    func testAlreadyDownloadedEnqueuesAnalysis() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: true, fingerprint: "sha-123",
            reachability: .cellular, policy: .off // cellular is irrelevant once cached
        )
        #expect(await c.prepare(request(coverage: 1800)) == .enqueuedAnalysis)

        let ops = await recorder.ops
        #expect(ops == [
            .enqueueUserIntent(episodeId: "ep-1", fingerprint: "sha-123", coverage: 1800),
        ])
    }

    @Test("cached but no fingerprint yet: records intent, does not block on hashing")
    func testCachedNoFingerprintMarksIntent() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: true, fingerprint: nil,
            reachability: .wifi, policy: .on
        )
        #expect(await c.prepare(request(coverage: 1200)) == .markedIntentOnly)

        let ops = await recorder.ops
        #expect(ops == [.markUserIntent(episodeId: "ep-1", coverage: 1200)])
    }

    // MARK: - Idempotence & no-playback

    @Test("re-preparing a cached episode is safe (delegates each time; scheduler dedups)")
    func testIdempotentDoubleTapCached() async {
        let recorder = Recorder()
        let c = makeCoordinator(
            recorder: recorder, cached: true, fingerprint: "sha-1",
            reachability: .wifi, policy: .on
        )
        await c.prepare(request(coverage: 900))
        await c.prepare(request(coverage: 900))

        let ops = await recorder.ops
        // Only ever the enqueue effect — never a play call — even on a
        // double tap. Work-key dedup at the scheduler makes the second a
        // no-op there (covered by AnalysisWorkSchedulerUserIntentTests).
        #expect(ops == [
            .enqueueUserIntent(episodeId: "ep-1", fingerprint: "sha-1", coverage: 900),
            .enqueueUserIntent(episodeId: "ep-1", fingerprint: "sha-1", coverage: 900),
        ])
    }

    @Test("prepare performs only download/analysis effects — never playback")
    func testNoPlaybackEffect() async {
        // The coordinator has no playback seam by construction. This test
        // exercises both branches (not-cached and cached) and asserts the
        // ONLY effects recorded are preparation effects — there is no play
        // op in `Recorder.Op`, so a playback side effect is unrepresentable.
        let notCached = Recorder()
        await makeCoordinator(
            recorder: notCached, cached: false, fingerprint: nil,
            reachability: .wifi, policy: .on
        ).prepare(request())
        for op in await notCached.ops {
            switch op {
            case .markUserIntent, .startDownload: break // preparation only
            case .enqueueUserIntent: break
            }
        }
        #expect(await notCached.ops.isEmpty == false)

        let cached = Recorder()
        await makeCoordinator(
            recorder: cached, cached: true, fingerprint: "s",
            reachability: .wifi, policy: .on
        ).prepare(request())
        #expect(await cached.ops == [
            .enqueueUserIntent(episodeId: "ep-1", fingerprint: "s", coverage: 3600),
        ])
    }
}
