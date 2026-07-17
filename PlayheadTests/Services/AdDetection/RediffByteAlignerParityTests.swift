// RediffByteAlignerParityTests.swift
// playhead-xsdz.57: pin the Swift byte-run aligner against the CHECKED-IN
// reference output of the validated python implementation
// (`scripts/l2f-mp3-forensics.py align`) on a REAL staged corpus A/B pair.
//
// The expectation fixture lives at
// `TestFixtures/ByteAlign/<episodeId>.align.json` (small, git-tracked; its
// `_regenerate` field documents the exact python invocation). The AUDIO pair
// (`TestFixtures/Corpus/Audio/<episodeId>.mp3` + `.fresh.mp3`) is the staged
// corpus — large, NOT git-tracked — so this suite is `.enabled(if:)`-gated on
// its presence: it runs for real wherever the corpus is staged (the primary
// checkout / the Catalyst capture machine) and reports SKIPPED elsewhere,
// never a silent pass.
//
// Tolerances: byte offsets and run/chain counts must match EXACTLY (the port
// is byte-deterministic); seconds are compared within 0.002 s — the python
// reference rounds to 3 decimals, so ±0.0005 of true rounding plus FP
// accumulation headroom.

import Foundation
import Testing

@testable import Playhead

enum ByteAlignParityFixture {

    static let episodeId = "smartless-2026-05-18-quot-sting-quot"

    /// Repo root via the #filePath walk-up (fixture + corpus live in the repo
    /// tree, not the test bundle — same pattern as `RediffSpikeFixtureLoader`).
    static func repoRoot(filePath: String = #filePath) -> URL {
        // PlayheadTests/Services/AdDetection/RediffByteAlignerParityTests.swift
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent()  // AdDetection/
            .deletingLastPathComponent()  // Services/
            .deletingLastPathComponent()  // PlayheadTests/
            .deletingLastPathComponent()  // <repo root>
    }

    static var audioURL: URL {
        repoRoot().appendingPathComponent("TestFixtures/Corpus/Audio/\(episodeId).mp3")
    }

    static var freshURL: URL {
        repoRoot().appendingPathComponent("TestFixtures/Corpus/Audio/\(episodeId).fresh.mp3")
    }

    static var expectationURL: URL {
        repoRoot().appendingPathComponent("TestFixtures/ByteAlign/\(episodeId).align.json")
    }

    /// Gate condition for `.enabled(if:)` — both staged files present.
    static var stagedPairIsPresent: Bool {
        FileManager.default.fileExists(atPath: audioURL.path)
            && FileManager.default.fileExists(atPath: freshURL.path)
    }

    struct Expectation: Decodable {
        struct Slot: Decodable {
            let kind: String
            let aStartByte: Int
            let aEndByte: Int
            let aStartSec: Double
            let aEndSec: Double
            let aBytes: Int
            let bBytes: Int
        }

        let min_run_bytes: Int
        let runs_found: Int
        let runs_chained: Int
        let runs_dropped_nonmonotonic: Int
        let monotonic_clean: Bool
        let chained_bytes: Int
        let a_duration: Double
        let b_duration: Double
        let slots: [Slot]
    }

    static func loadExpectation() throws -> Expectation {
        try JSONDecoder().decode(Expectation.self, from: Data(contentsOf: expectationURL))
    }
}

@Suite("RediffByteAligner ↔ python reference parity on a real corpus pair (playhead-xsdz.57)")
struct RediffByteAlignerParityTests {

    @Test(
        "Swift aligner reproduces the python reference on the staged smartless A/B pair",
        .enabled(if: ByteAlignParityFixture.stagedPairIsPresent)
    )
    func realPairParity() throws {
        let expectation = try ByteAlignParityFixture.loadExpectation()
        #expect(expectation.min_run_bytes == RediffByteAligner.Configuration.default.minRunBytes,
                "fixture was generated at a different min-run-bytes than the production default")

        let aData = try Data(contentsOf: ByteAlignParityFixture.audioURL, options: .mappedIfSafe)
        let bData = try Data(contentsOf: ByteAlignParityFixture.freshURL, options: .mappedIfSafe)
        let alignment = RediffByteAligner.align(aData: aData, bData: bData)

        // Run/chain accounting: exact.
        #expect(alignment.runsFound == expectation.runs_found)
        #expect(alignment.chain.count == expectation.runs_chained)
        #expect(alignment.runsDroppedNonMonotonic == expectation.runs_dropped_nonmonotonic)
        #expect(alignment.monotonicClean == expectation.monotonic_clean)
        #expect(alignment.chainedBytes == expectation.chained_bytes)

        // Durations: python rounds to 3 decimals.
        #expect(abs(alignment.aDurationSeconds - expectation.a_duration) < 0.002)
        #expect(abs(alignment.bDurationSeconds - expectation.b_duration) < 0.002)

        // Slots: byte-exact edges, kind parity, seconds within rounding.
        try #require(alignment.slots.count == expectation.slots.count,
                     "slot count \(alignment.slots.count) != reference \(expectation.slots.count)")
        for (got, want) in zip(alignment.slots, expectation.slots) {
            #expect(got.kind.rawValue == want.kind)
            #expect(got.aStartByte == want.aStartByte)
            #expect(got.aEndByte == want.aEndByte)
            #expect(got.aBytes == want.aBytes)
            #expect(got.bBytes == want.bBytes)
            #expect(abs(got.aStartSeconds - want.aStartSec) < 0.002)
            #expect(abs(got.aEndSeconds - want.aEndSec) < 0.002)
        }

        // The production gate accepts this alignment (byte PRIMARY engages on
        // the real pair) and the played slots carry the mid-roll DAI breaks.
        guard case .accepted(let acceptance) =
            RediffSlotOwnership.gateAndDiffBytes(alignment: alignment) else {
            Issue.record("byte gate rejected the real-pair alignment")
            return
        }
        #expect(acceptance.runsChained == expectation.runs_chained)
        // Every ≥ minAdSeconds A-width reference slot survives as a played slot
        // (modulo the shared fragment-merge, which requires a > 3 s inter-slot
        // gap to keep slots separate — true for this pair's slots).
        let expectedPlayed = expectation.slots.filter {
            ($0.aEndSec - $0.aStartSec) >= RediffSlotOwnership.Configuration.default.minAdSeconds
        }
        try #require(acceptance.playedSlots.count == expectedPlayed.count)
        for (got, want) in zip(acceptance.playedSlots, expectedPlayed) {
            #expect(abs(got.startSeconds - want.aStartSec) < 0.002)
            #expect(abs(got.endSeconds - want.aEndSec) < 0.002)
        }
    }
}
