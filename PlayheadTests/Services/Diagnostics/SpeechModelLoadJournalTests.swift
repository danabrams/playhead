// SpeechModelLoadJournalTests.swift
// playhead-se2h — the durable half of "did the ASR model ever load".
//
// The journal exists because a model-load failure had no durable trace at
// all. Its counter is the field a support engineer reads first, so the
// tests here are about the counter being TRUSTWORTHY:
//
//   * a missing document reads `unknown`, never a healthy value — a
//     default that reads "fine" makes an unwired signal indistinguishable
//     from a working device;
//   * an UNREADABLE document is not an empty one, so a write during a
//     protected-data window must not reset a real escalation history;
//   * a success clears the counter but KEEPS the failure history, because
//     "failed twice then recovered" is the evidence the retry worked.

import Foundation
import Testing

@testable import Playhead

@Suite("playhead-se2h — SpeechModelLoadJournal")
struct SpeechModelLoadJournalTests {

    private func makeTempDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("se2h-journal-\(UUID().uuidString)", isDirectory: true)
    }

    private func loadError(_ message: String = "assets unavailable") -> any Error {
        TranscriptEngineError.transcriptionFailed(message)
    }

    // MARK: - Defaults

    /// A default that reads "healthy" is how a diagnostics field ends up
    /// reporting health no matter how broken the device is. This one has to
    /// read as an absence.
    @Test("A device with no document reads `unknown`, not a healthy value")
    func missingDocumentReadsUnknown() async {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)

        let state = await journal.load()
        #expect(state == .unknown)
        #expect(state.status == .unknown)
        #expect(state.lastSuccessAt == nil)
        #expect(state.lastSuccessfulRole == nil)
        #expect(
            state.status != .loaded,
            "an empty document must never present as a working speech stack"
        )
    }

    // MARK: - Escalation

    @Test("Consecutive failures escalate to persistentlyFailing at the threshold")
    func consecutiveFailuresEscalate() async {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)

        var observed: [SpeechModelLoadStatus] = []
        for attempt in 1...SpeechModelLoadState.failuresBeforeConcern {
            let state = await journal.recordFailure(error: loadError(), attemptNumber: attempt)
            observed.append(state.status)
            #expect(state.consecutiveFailureCount == attempt)
        }

        #expect(
            observed.last == .persistentlyFailing,
            "\(SpeechModelLoadState.failuresBeforeConcern) failures in a row must escalate"
        )
        #expect(
            observed.dropLast().allSatisfy { $0 == .retrying },
            "and everything below the threshold must read as still-retrying: got \(observed)"
        )
    }

    /// The counter is DURABLE — that is the whole point of a file. A second
    /// journal instance over the same directory is what a second launch
    /// looks like.
    @Test("The failure counter survives across journal instances (i.e. across launches)")
    func counterSurvivesAcrossInstances() async {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }

        await SpeechModelLoadJournal(directory: directory)
            .recordFailure(error: loadError(), attemptNumber: 1)
        await SpeechModelLoadJournal(directory: directory)
            .recordFailure(error: loadError(), attemptNumber: 1)

        let reread = await SpeechModelLoadJournal(directory: directory).load()
        #expect(
            reread.consecutiveFailureCount == 2,
            """
            got \(reread.consecutiveFailureCount). One failure in each of two launches must \
            accumulate — a per-process counter would report 1 forever and never escalate.
            """
        )
        #expect(reread.recentFailures.count == 2)
    }

    @Test("Each record carries the counter as it stood after that failure")
    func recordsCarryTheirOwnCount() async {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)

        for attempt in 1...3 {
            await journal.recordFailure(error: loadError(), attemptNumber: attempt)
        }

        let state = await journal.load()
        #expect(state.recentFailures.map(\.consecutiveFailureCount) == [1, 2, 3])
        #expect(state.recentFailures.map(\.attemptNumber) == [1, 2, 3])
    }

    // MARK: - Recovery

    @Test("A success clears the counter but keeps the failure history")
    func successClearsTheCounterAndKeepsHistory() async {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)

        await journal.recordFailure(error: loadError(), attemptNumber: 1)
        await journal.recordFailure(error: loadError(), attemptNumber: 2)
        let recovered = await journal.recordSuccess(role: .asrFast)

        #expect(recovered.status == .loaded)
        #expect(recovered.consecutiveFailureCount == 0)
        #expect(recovered.firstFailureAt == nil, "the failure WINDOW is over")
        #expect(recovered.lastFailureAt != nil, "but when it last failed still matters")
        #expect(recovered.lastSuccessfulRole == .asrFast)
        #expect(
            recovered.recentFailures.count == 2,
            """
            the history must survive: "failed twice, then recovered" is the only evidence \
            that the retry did its job, and dropping it makes a recovered device \
            indistinguishable from one that never had a problem
            """
        )
    }

    @Test("The recorded role reflects which model actually loaded")
    func roleReflectsWhatLoaded() async {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)

        await journal.recordSuccess(role: .asrFast)
        #expect(await journal.load().lastSuccessfulRole == .asrFast)
        await journal.recordSuccess(role: .asrFinal)
        #expect(await journal.load().lastSuccessfulRole == .asrFinal)
    }

    // MARK: - Caps

    @Test("The failure list is capped, keeping the newest records")
    func failureListIsCapped() async {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)

        let overflow = SpeechModelLoadState.maxFailureRecords + 7
        for attempt in 1...overflow {
            await journal.recordFailure(error: loadError(), attemptNumber: attempt)
        }

        let state = await journal.load()
        #expect(state.recentFailures.count == SpeechModelLoadState.maxFailureRecords)
        #expect(
            state.recentFailures.last?.consecutiveFailureCount == overflow,
            "the cap must drop the OLDEST records — the newest are the diagnostic"
        )
        #expect(state.consecutiveFailureCount == overflow, "the counter itself is not capped")
    }

    // MARK: - Durability rules

    /// The refuse-to-clobber rule, inherited from
    /// `AnalysisStoreHealthJournal`. On iOS a background launch before the
    /// first unlock can find the container under Data Protection; treating
    /// that unreadable read as an empty document would replace a real
    /// escalation history with a fresh one and reset the counter forever.
    @Test("An unreadable document is not overwritten")
    func unreadableDocumentIsNotClobbered() async throws {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)

        // Build a real history first.
        for attempt in 1...3 {
            await journal.recordFailure(error: loadError(), attemptNumber: attempt)
        }
        let url = directory.appendingPathComponent(SpeechModelLoadJournal.filename)
        let good = try Data(contentsOf: url)

        // UNREADABLE-BUT-WRITABLE is the fixture that actually discriminates.
        //
        // The first version of this test put a DIRECTORY at the path. That
        // makes `Data(contentsOf:)` fail — but it also makes the subsequent
        // `.atomic` write fail, so deleting the guard under test changed
        // nothing and every assertion still held. The test could not fail.
        //
        // A mode-000 file is the honest shape: reads fail EACCES while an
        // `.atomic` write still succeeds, because `.atomic` writes a temp
        // file and `rename()`s over the target — the permissions of the file
        // being replaced are irrelevant. So without the refuse-to-clobber
        // guard the history IS destroyed here, and the final assertion
        // catches it.
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o000], ofItemAtPath: url.path
        )
        defer {
            try? FileManager.default.setAttributes(
                [.posixPermissions: 0o600], ofItemAtPath: url.path
            )
        }
        try #require(
            (try? Data(contentsOf: url)) == nil,
            "fixture premise: the document must be unreadable for this test to mean anything"
        )

        let returned = await journal.recordFailure(error: loadError(), attemptNumber: 4)
        #expect(
            returned.consecutiveFailureCount == 1,
            "the caller is still told the failure happened, from a fresh base"
        )

        // Restore readability: the real history must be intact.
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o600], ofItemAtPath: url.path
        )
        let restored = await journal.load()
        #expect(
            restored.consecutiveFailureCount == 3,
            """
            got \(restored.consecutiveFailureCount) — the pre-existing history must survive an \
            unreadable-read window. A 1 here means the write went ahead and reset a real \
            escalation counter, which on a device is how the listener stops ever being told.
            """
        )
        #expect(try Data(contentsOf: url) == good, "the bytes must be untouched, not merely equivalent")
    }

    @Test("A document that is not JSON at all reads unknown and is then replaced")
    func undecodableDocumentIsReplaced() async throws {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let url = directory.appendingPathComponent(SpeechModelLoadJournal.filename)
        try Data("not json at all".utf8).write(to: url)

        let journal = SpeechModelLoadJournal(directory: directory)
        #expect(await journal.load() == .unknown)

        let next = await journal.recordSuccess(role: .asrFast)
        #expect(next.status == .loaded)
        #expect(await journal.load().status == .loaded, "the garbage must be replaced, not preserved")
    }

    /// Field-by-field degrading decode: a single unusable field must not
    /// cost the counters, which are the part that matters most.
    @Test("A document with a corrupt field still yields its counters")
    func partiallyCorruptDocumentStillYieldsCounters() async throws {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let url = directory.appendingPathComponent(SpeechModelLoadJournal.filename)
        // `status` is an enum this build does not know, and
        // `last_successful_role` is the wrong type.
        try Data("""
        {
          "status": "invented_by_a_newer_build",
          "consecutive_failure_count": 5,
          "last_successful_role": 17,
          "recent_failures": []
        }
        """.utf8).write(to: url)

        let state = await SpeechModelLoadJournal(directory: directory).load()
        #expect(state.consecutiveFailureCount == 5, "the counter must survive its neighbours")
        #expect(state.status == .unknown, "an unrecognised status degrades to unknown, not to loaded")
        #expect(state.lastSuccessfulRole == nil)
    }

    // MARK: - Wire shape

    /// The document is also the exported shape, so its keys are a
    /// contract. Snake case throughout, matching every other bundle
    /// section.
    @Test("The persisted document uses the exported snake_case key names")
    func persistedDocumentUsesSnakeCaseKeys() async throws {
        let directory = makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let journal = SpeechModelLoadJournal(directory: directory)
        await journal.recordFailure(error: loadError(), attemptNumber: 2)

        let url = directory.appendingPathComponent(SpeechModelLoadJournal.filename)
        let object = try JSONSerialization.jsonObject(with: try Data(contentsOf: url))
        let dict = try #require(object as? [String: Any])

        for key in ["status", "consecutive_failure_count", "first_failure_at",
                    "last_failure_at", "recent_failures"] {
            #expect(dict[key] != nil, "missing `\(key)` in the persisted document")
        }
        let failures = try #require(dict["recent_failures"] as? [[String: Any]])
        let record = try #require(failures.first)
        for key in ["occurred_at", "failure_class", "attempt_number", "consecutive_failure_count"] {
            #expect(record[key] != nil, "missing `\(key)` in a failure record")
        }
    }

    @Test("The state round-trips through Codable")
    func stateRoundTrips() throws {
        let original = SpeechModelLoadState(
            status: .retrying,
            consecutiveFailureCount: 2,
            firstFailureAt: Date(timeIntervalSince1970: 1_000),
            lastFailureAt: Date(timeIntervalSince1970: 2_000),
            lastSuccessAt: Date(timeIntervalSince1970: 500),
            lastSuccessfulRole: .asrFinal,
            recentFailures: [
                SpeechModelLoadFailureRecord(
                    occurredAt: Date(timeIntervalSince1970: 2_000),
                    failureClass: .speechAssetsUnsupported,
                    attemptNumber: 2,
                    consecutiveFailureCount: 2
                )
            ]
        )

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let decoded = try decoder.decode(
            SpeechModelLoadState.self, from: try encoder.encode(original)
        )
        #expect(decoded == original)
    }

    /// The cap applies on the way IN, so no construction path — including
    /// a decode of a document written by a build with a larger cap — can
    /// produce an oversized document.
    @Test("Construction caps an oversized failure list")
    func constructionCapsOversizedList() {
        let tooMany = (0..<(SpeechModelLoadState.maxFailureRecords + 5)).map { index in
            SpeechModelLoadFailureRecord(
                occurredAt: Date(timeIntervalSince1970: TimeInterval(index)),
                failureClass: .modelNotLoaded,
                attemptNumber: 1,
                consecutiveFailureCount: index + 1
            )
        }
        let state = SpeechModelLoadState(
            status: .retrying,
            consecutiveFailureCount: tooMany.count,
            firstFailureAt: nil,
            lastFailureAt: nil,
            lastSuccessAt: nil,
            lastSuccessfulRole: nil,
            recentFailures: tooMany
        )
        #expect(state.recentFailures.count == SpeechModelLoadState.maxFailureRecords)
        #expect(state.recentFailures.last?.consecutiveFailureCount == tooMany.count)
    }
}
