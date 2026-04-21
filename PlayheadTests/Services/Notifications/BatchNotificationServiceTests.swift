// BatchNotificationServiceTests.swift
// Contract tests for `BatchNotificationService`. playhead-zp0x.
//
// Negative-API contract: the service exposes ONLY
// `emit(eligibility:batch:)`. The tests below pin per-case copy
// (snapshot) and assert that each enum case routes to the verbatim
// `BatchNotificationCopy` strings.
//
// IF YOU ADD A NEW EMISSION OVERLOAD (e.g. `emit(reason: SurfaceReason, ...)`,
// `emit(text: String, ...)`, etc.) the snapshot lock here must be
// updated; the design intent is that the only public emission method
// is the enum-typed one. Adding overloads defeats the snapshot
// guarantee that every user-visible string is pinned in
// `BatchNotificationCopy`.

import Foundation
import Testing
import UserNotifications

@testable import Playhead

// MARK: - Recording scheduler

/// Records every `UNNotificationRequest` for assertion. Implements the
/// service's `Scheduler` protocol so production wiring is unchanged
/// outside tests.
///
/// `@MainActor`-isolated because the production
/// `BatchNotificationService` is also MainActor (its `emit(...)` accepts
/// a SwiftData `@Model` and so cannot cross into a different actor
/// domain). The protocol method is itself `@MainActor`, so this
/// conformer is a plain MainActor class.
@MainActor
private final class RecordingScheduler: BatchNotificationService.Scheduler {
    private(set) var requests: [UNNotificationRequest] = []

    func add(_ request: UNNotificationRequest) async throws {
        requests.append(request)
    }

    func snapshot() -> [UNNotificationRequest] { requests }
}

@Suite("BatchNotificationService — emit-only contract + copy snapshot (playhead-zp0x)")
@MainActor
struct BatchNotificationServiceTests {

    // MARK: - Helpers

    private static func makeBatch(context: DownloadTripContext) -> DownloadBatch {
        DownloadBatch(
            tripContextRaw: context.rawValue,
            episodeKeys: ["ep-1"]
        )
    }

    // MARK: - Each enum case → matching copy

    @Test("tripReady emits BatchNotificationCopy.tripReadyTitle + body(context:)")
    func tripReadyCopy() async {
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = Self.makeBatch(context: .flight)

        await service.emit(eligibility: .tripReady, batch: batch)

        let requests = await scheduler.snapshot()
        #expect(requests.count == 1)
        #expect(requests[0].content.title == BatchNotificationCopy.tripReadyTitle)
        #expect(
            requests[0].content.body
                == BatchNotificationCopy.tripReadyBody(context: .flight)
        )
    }

    @Test("blockedStorage emits the storage copy strings verbatim")
    func blockedStorageCopy() async {
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = Self.makeBatch(context: .commute)

        await service.emit(eligibility: .blockedStorage, batch: batch)

        let requests = await scheduler.snapshot()
        #expect(requests.count == 1)
        #expect(requests[0].content.title == BatchNotificationCopy.blockedStorageTitle)
        #expect(requests[0].content.body == BatchNotificationCopy.blockedStorageBody)
    }

    @Test("blockedWifiPolicy emits the wifi-policy copy strings verbatim")
    func blockedWifiPolicyCopy() async {
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = Self.makeBatch(context: .commute)

        await service.emit(eligibility: .blockedWifiPolicy, batch: batch)

        let requests = await scheduler.snapshot()
        #expect(requests.count == 1)
        #expect(requests[0].content.title == BatchNotificationCopy.blockedWifiPolicyTitle)
        #expect(requests[0].content.body == BatchNotificationCopy.blockedWifiPolicyBody)
    }

    @Test("blockedAnalysisUnavailable emits the analysis-unavailable copy")
    func blockedAnalysisUnavailableCopy() async {
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = Self.makeBatch(context: .workout)

        await service.emit(eligibility: .blockedAnalysisUnavailable, batch: batch)

        let requests = await scheduler.snapshot()
        #expect(requests.count == 1)
        #expect(
            requests[0].content.title
                == BatchNotificationCopy.blockedAnalysisUnavailableTitle
        )
        #expect(
            requests[0].content.body
                == BatchNotificationCopy.blockedAnalysisUnavailableBody
        )
    }

    @Test(".none is a no-op (no notification scheduled)")
    func noneIsNoOp() async {
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = Self.makeBatch(context: .flight)

        await service.emit(eligibility: .none, batch: batch)

        let requests = await scheduler.snapshot()
        #expect(requests.isEmpty)
    }

    // MARK: - Snapshot lock for trip-context phrasing

    @Test("Trip-ready body bakes in the trip-context phrase")
    func tripReadyBodyEmbedsContextPhrase() async {
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let flightBatch = Self.makeBatch(context: .flight)
        let commuteBatch = Self.makeBatch(context: .commute)
        let workoutBatch = Self.makeBatch(context: .workout)

        await service.emit(eligibility: .tripReady, batch: flightBatch)
        await service.emit(eligibility: .tripReady, batch: commuteBatch)
        await service.emit(eligibility: .tripReady, batch: workoutBatch)

        let requests = await scheduler.snapshot()
        #expect(requests.count == 3)
        #expect(requests[0].content.body.contains("for your flight"))
        #expect(requests[1].content.body.contains("for your commute"))
        #expect(requests[2].content.body.contains("for your workout"))
    }

    // MARK: - userInfo routing

    @Test("Notification userInfo carries batchId + trigger for deep link")
    func userInfoStampsBatchAndTrigger() async {
        let scheduler = RecordingScheduler()
        let service = BatchNotificationService(scheduler: scheduler)
        let batch = Self.makeBatch(context: .flight)
        let batchIdString = batch.id.uuidString

        await service.emit(eligibility: .tripReady, batch: batch)

        let requests = await scheduler.snapshot()
        #expect(requests.count == 1)
        let userInfo = requests[0].content.userInfo
        #expect((userInfo["batchId"] as? String) == batchIdString)
        #expect((userInfo["trigger"] as? String) == "tripReady")
    }
}

// MARK: - BatchNotificationCopy snapshot tests
//
// Pinned verbatim so any change to product copy must update this file.

@Suite("BatchNotificationCopy — verbatim snapshot (playhead-zp0x)")
struct BatchNotificationCopyTests {

    @Test("tripReadyTitle verbatim")
    func tripReadyTitleSnapshot() {
        #expect(BatchNotificationCopy.tripReadyTitle == "Your downloads are ready")
    }

    @Test("tripReadyBody verbatim per context")
    func tripReadyBodySnapshot() {
        #expect(
            BatchNotificationCopy.tripReadyBody(context: .generic)
                == "Episodes you queued are downloaded and analyzed."
        )
        #expect(
            BatchNotificationCopy.tripReadyBody(context: .flight)
                == "Episodes you queued for your flight are downloaded and analyzed."
        )
        #expect(
            BatchNotificationCopy.tripReadyBody(context: .commute)
                == "Episodes you queued for your commute are downloaded and analyzed."
        )
        #expect(
            BatchNotificationCopy.tripReadyBody(context: .workout)
                == "Episodes you queued for your workout are downloaded and analyzed."
        )
    }

    @Test("blockedStorage verbatim")
    func blockedStorageSnapshot() {
        #expect(BatchNotificationCopy.blockedStorageTitle == "Downloads need more space")
        #expect(
            BatchNotificationCopy.blockedStorageBody
                == "Tap to free up space so your queued episodes can finish."
        )
    }

    @Test("blockedWifiPolicy verbatim")
    func blockedWifiPolicySnapshot() {
        #expect(BatchNotificationCopy.blockedWifiPolicyTitle == "Downloads waiting for Wi\u{2011}Fi")
        #expect(
            BatchNotificationCopy.blockedWifiPolicyBody
                == "Connect to Wi\u{2011}Fi, or allow cellular for Playhead in Settings."
        )
    }

    @Test("blockedAnalysisUnavailable verbatim")
    func blockedAnalysisUnavailableSnapshot() {
        #expect(BatchNotificationCopy.blockedAnalysisUnavailableTitle == "Analysis is paused")
        #expect(
            BatchNotificationCopy.blockedAnalysisUnavailableBody
                == "Open Settings to re-enable Apple Intelligence or change language."
        )
    }

    @Test("Generic returns nil phrase")
    func genericPhraseIsNil() {
        #expect(BatchNotificationCopy.tripContextPhrase(.generic) == nil)
    }
}
