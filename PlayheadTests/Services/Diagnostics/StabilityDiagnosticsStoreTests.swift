// StabilityDiagnosticsStoreTests.swift
// Ring-buffer behaviour of the local crash + hang store.
//
// Scope: playhead-jw63.4.
//
// Every test constructs its own store against a unique temp directory —
// the same isolation rule `SurfaceStatusInvariantLogger` follows.
// `StabilityDiagnosticsStore.shared` is production-only and is never
// touched here.

import Foundation
import Testing

@testable import Playhead

@Suite("StabilityDiagnosticsStore — bounded local ring buffer (playhead-jw63.4)")
struct StabilityDiagnosticsStoreTests {

    // MARK: - Fixtures

    private func temporaryDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("stability-store-\(UUID().uuidString)", isDirectory: true)
    }

    private func record(
        kind: StabilityDiagnosticKind = .crash,
        at seconds: Double
    ) -> StabilityDiagnosticRecord {
        StabilityDiagnosticRecord(
            kind: kind,
            receivedAt: seconds,
            appVersion: "1.0.0",
            appBuildVersion: "\(Int(seconds))",
            frameCount: 1,
            frames: [
                StabilityCallStackFrame(
                    binaryName: "Playhead",
                    binaryUUID: MetricKitPayloadFixture.appBinaryUUID,
                    offsetIntoBinaryTextSegment: Int(seconds),
                    depth: 0
                )
            ]
        )
    }

    // MARK: - Round trip

    @Test("appended records survive a fresh store instance — the buffer outlives the process")
    func persistsAcrossInstances() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        let writer = StabilityDiagnosticsStore(directory: directory)
        await writer.append([record(at: 1), record(kind: .hang, at: 2)])

        // A brand-new instance stands in for the next app launch, which
        // is exactly when MetricKit-captured crashes get read.
        let reader = StabilityDiagnosticsStore(directory: directory)
        let all = await reader.all()
        #expect(all.count == 2)
        #expect(all.map(\.kind) == [.crash, .hang])
        #expect(all.map(\.receivedAt) == [1, 2])
        #expect(all.last?.frames.first?.binaryUUID == MetricKitPayloadFixture.appBinaryUUID)
    }

    @Test("append accumulates across calls rather than replacing")
    func appendAccumulates() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)

        await store.append([record(at: 1)])
        await store.append([record(at: 2)])
        await store.append([record(at: 3)])

        let all = await store.all()
        #expect(all.map(\.receivedAt) == [1, 2, 3])
    }

    @Test("appending nothing does not create a file — a healthy device writes no crash log")
    func emptyAppendIsInert() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)

        await store.append([])
        let fileURL = directory.appendingPathComponent(StabilityDiagnosticsStore.filename)
        #expect(!FileManager.default.fileExists(atPath: fileURL.path))
        #expect(await store.all().isEmpty)
    }

    @Test("reading a store that was never written yields no records rather than throwing")
    func missingFileReadsEmpty() async {
        let store = StabilityDiagnosticsStore(directory: temporaryDirectory())
        #expect(await store.all().isEmpty)
        #expect(await store.recent().isEmpty)
    }

    // MARK: - Cap

    @Test("the buffer keeps the NEWEST maxRecords and drops the oldest")
    func capKeepsNewest() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)

        let overflow = StabilityDiagnosticsStore.maxRecords + 10
        for index in 1...overflow {
            await store.append([record(at: Double(index))])
        }

        let all = await store.all()
        #expect(all.count == StabilityDiagnosticsStore.maxRecords)
        // Oldest survivor is `overflow - maxRecords + 1`; anything
        // earlier was evicted. Keeping the OLDEST would be the classic
        // `.prefix` inversion and would mean the buffer stops recording
        // the moment it fills.
        #expect(all.first?.receivedAt == Double(overflow - StabilityDiagnosticsStore.maxRecords + 1))
        #expect(all.last?.receivedAt == Double(overflow))
    }

    @Test("a single oversized append is capped too")
    func capAppliesWithinOneAppend() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)

        let batch = (1...(StabilityDiagnosticsStore.maxRecords + 25)).map { record(at: Double($0)) }
        await store.append(batch)

        let all = await store.all()
        #expect(all.count == StabilityDiagnosticsStore.maxRecords)
        #expect(all.last?.receivedAt == Double(StabilityDiagnosticsStore.maxRecords + 25))
    }

    // MARK: - Ordering

    @Test("recent() returns newest first, and honours its limit")
    func recentIsNewestFirst() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)

        await store.append((1...5).map { record(at: Double($0)) })

        let recent = await store.recent()
        #expect(recent.map(\.receivedAt) == [5, 4, 3, 2, 1])

        let limited = await store.recent(limit: 2)
        #expect(limited.map(\.receivedAt) == [5, 4])

        #expect(await store.recent(limit: 0).isEmpty)
    }

    // MARK: - Corruption tolerance

    @Test("a torn or unreadable line costs one record, not the whole buffer")
    func corruptLineIsSkipped() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)
        await store.append([record(at: 1), record(at: 2)])

        // Simulate a power loss mid-write: append a half-written line.
        let fileURL = directory.appendingPathComponent(StabilityDiagnosticsStore.filename)
        var data = try Data(contentsOf: fileURL)
        data.append(Data("{\"kind\":\"cra".utf8))
        try data.write(to: fileURL)

        let all = await StabilityDiagnosticsStore(directory: directory).all()
        #expect(all.count == 2, "the two intact records must still be readable")
        #expect(all.map(\.receivedAt) == [1, 2])
    }

    @Test("a record missing newer optional fields still decodes — the buffer survives app updates")
    func toleratesOlderSchemaRows() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let fileURL = directory.appendingPathComponent(StabilityDiagnosticsStore.filename)
        // The minimum a row can be: kind + received_at.
        try Data("{\"kind\":\"hang\",\"received_at\":7}\n".utf8).write(to: fileURL)

        let all = await StabilityDiagnosticsStore(directory: directory).all()
        #expect(all.count == 1)
        #expect(all.first?.kind == .hang)
        #expect(all.first?.frames.isEmpty == true)
        #expect(all.first?.schemaVersion == StabilityDiagnosticRecord.currentSchemaVersion)
    }

    @Test("removeAll empties the buffer")
    func removeAllClears() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)
        await store.append([record(at: 1)])
        await store.removeAll()
        #expect(await store.all().isEmpty)
    }

    // MARK: - Storage discipline

    @Test("the buffer is one JSON object per line, so a torn tail is recoverable by construction")
    func storageIsJSONLines() async throws {
        let directory = temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = StabilityDiagnosticsStore(directory: directory)
        await store.append([record(at: 1), record(at: 2)])

        let fileURL = directory.appendingPathComponent(StabilityDiagnosticsStore.filename)
        let text = try String(contentsOf: fileURL, encoding: .utf8)
        let lines = text.split(separator: "\n")
        #expect(lines.count == 2)
        for line in lines {
            #expect(line.hasPrefix("{") && line.hasSuffix("}"))
        }
    }

    @Test("the buffer lives in Application Support, not Caches — the OS must not evict crash records")
    func defaultLocationIsApplicationSupport() throws {
        // The default (nil-directory) store resolves its own path; the
        // assertion is on the CONSTANT, because instantiating the
        // default store in a test would write into the test host's real
        // container.
        #expect(StabilityDiagnosticsStore.directoryName == "Diagnostics")
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Services/Diagnostics/StabilityDiagnosticsStore.swift"
        )
        let code = SwiftSourceInspector.strippingComments(source)
        #expect(code.contains("applicationSupportDirectory"))
        #expect(
            !code.contains("cachesDirectory"),
            "crash records must not live in Caches — the OS evicts it under the disk pressure that makes crashes interesting"
        )
    }
}
