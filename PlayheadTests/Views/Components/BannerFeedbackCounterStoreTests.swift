// BannerFeedbackCounterStoreTests.swift
// Durable, aggregate-only banner feedback counts for playhead-jw63.1.

import Foundation
import XCTest

@testable import Playhead

@MainActor
final class BannerFeedbackCounterStoreTests: XCTestCase {

    private func makeDefaults() -> (UserDefaults, String) {
        let suiteName = "BannerFeedbackCounterStoreTests.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)
        return (defaults, suiteName)
    }

    func testStartsAtZeroAndAggregatesExactlyThreeCounters() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = BannerFeedbackCounterStore(defaults: defaults)

        XCTAssertEqual(
            store.snapshot,
            BannerFeedbackCounts(
                bannersShown: 0,
                bannersConfirmed: 0,
                bannersDenied: 0
            )
        )

        store.recordBannerShown()
        store.recordBannerShown()
        store.recordConfirmed()
        store.recordDenied()

        XCTAssertEqual(
            store.snapshot,
            BannerFeedbackCounts(
                bannersShown: 2,
                bannersConfirmed: 1,
                bannersDenied: 1
            )
        )
    }

    func testCountsPersistAcrossStoreInstances() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let first = BannerFeedbackCounterStore(defaults: defaults)
        first.recordBannerShown()
        first.recordConfirmed()

        let reopened = BannerFeedbackCounterStore(defaults: defaults)
        XCTAssertEqual(reopened.snapshot.bannersShown, 1)
        XCTAssertEqual(reopened.snapshot.bannersConfirmed, 1)
        XCTAssertEqual(reopened.snapshot.bannersDenied, 0)
    }

    func testStorageIsOneAggregateValueWithoutEventMetadata() throws {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let storageKey = "test.bannerFeedback.aggregate"
        let store = BannerFeedbackCounterStore(
            defaults: defaults,
            storageKey: storageKey
        )

        store.recordBannerShown()
        store.recordDenied()

        let storedKeys = defaults.persistentDomain(forName: suiteName)
            .map { Set($0.keys) } ?? []
        XCTAssertEqual(storedKeys, [storageKey])
        let data = try XCTUnwrap(defaults.data(forKey: storageKey))
        let object = try XCTUnwrap(
            JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        XCTAssertEqual(
            Set(object.keys),
            ["bannersShown", "bannersConfirmed", "bannersDenied"]
        )
        XCTAssertFalse(object.keys.contains("timestamp"))
        XCTAssertFalse(object.keys.contains("episodeId"))
        XCTAssertFalse(object.keys.contains("windowId"))
    }

    func testMalformedStorageRecoversToZeroBeforeNextMutation() {
        let (defaults, suiteName) = makeDefaults()
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let storageKey = "test.bannerFeedback.aggregate"
        defaults.set(Data("not-json".utf8), forKey: storageKey)
        let store = BannerFeedbackCounterStore(
            defaults: defaults,
            storageKey: storageKey
        )

        XCTAssertEqual(store.snapshot, .zero)
        store.recordConfirmed()
        XCTAssertEqual(
            store.snapshot,
            BannerFeedbackCounts(
                bannersShown: 0,
                bannersConfirmed: 1,
                bannersDenied: 0
            )
        )
    }

    func testAggregateFeedbackHasNoEncoderOrTelemetryEgressConsumer()
        throws
    {
        var repoRoot = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
        for _ in 0..<3 {
            repoRoot.deleteLastPathComponent()
        }
        let productionRoot = repoRoot.appendingPathComponent(
            "Playhead", isDirectory: true
        )
        let egressPathMarkers = [
            "export", "telemetry", "diagnostic", "logger", "sync",
            "sharing", "share", "upload", "replay",
        ]
        let forbiddenAggregateTokens = [
            "BannerFeedbackCounterStore",
            "BannerFeedbackCounts",
            "playhead.bannerFeedback.aggregate.v1",
            "bannersShown",
            "bannersConfirmed",
            "bannersDenied",
        ]
        let enumerator = try XCTUnwrap(
            FileManager.default.enumerator(
                at: productionRoot,
                includingPropertiesForKeys: nil
            )
        )

        for case let fileURL as URL in enumerator
        where fileURL.pathExtension == "swift" {
            let relativePath = fileURL.path
                .replacingOccurrences(
                    of: productionRoot.path + "/",
                    with: ""
                )
            let lowerPath = relativePath.lowercased()
            guard egressPathMarkers.contains(where: lowerPath.contains)
            else {
                continue
            }
            let source = try String(contentsOf: fileURL, encoding: .utf8)
            for token in forbiddenAggregateTokens {
                XCTAssertFalse(
                    source.contains(token),
                    "\(relativePath) must not encode, upload, log, sync, "
                        + "share, or export aggregate banner feedback "
                        + "token \(token)"
                )
            }
        }
    }
}
