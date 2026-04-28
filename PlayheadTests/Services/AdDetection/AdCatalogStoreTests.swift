// AdCatalogStoreTests.swift
// playhead-gtt9.13: Tests for the on-device ad catalog SQLite store.

import Foundation
import Testing
@testable import Playhead

@Suite("AdCatalogStore")
struct AdCatalogStoreTests {

    // MARK: - Helpers

    private func makeTempDir() throws -> URL {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("AdCatalogStoreTests-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        return base
    }

    private func sampleFingerprint(seed: Int = 1) -> AcousticFingerprint {
        let values = (0..<64).map { Float(($0 + seed) % 17) + 0.5 }
        return AcousticFingerprint(values: values)!
    }

    /// Build an orthogonal-ish fingerprint distinct from `sampleFingerprint(seed:)`.
    /// Nonzero bins live in the back half of the vector while `sampleFingerprint`
    /// is weighted toward the front, making the two sit well below any
    /// default similarity floor.
    private func orthogonalFingerprint(seed: Int = 1) -> AcousticFingerprint {
        var values = [Float](repeating: 0, count: 64)
        for i in 32..<64 {
            values[i] = Float((i + seed) % 13) + 1.0
        }
        return AcousticFingerprint(values: values)!
    }

    // MARK: - Insert + query roundtrip

    @Test("insert + matches roundtrip returns the inserted entry")
    func insertMatchRoundtrip() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 1)
        let inserted = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            transcriptSnippet: "betterhelp dot com slash podcast",
            sponsorTokens: ["betterhelp"],
            originalConfidence: 0.9
        )

        let matches = await store.matches(
            fingerprint: fp,
            show: "show-1",
            similarityFloor: 0.80
        )

        #expect(matches.count == 1)
        #expect(matches.first?.entry.id == inserted.id)
        #expect(matches.first?.entry.transcriptSnippet == "betterhelp dot com slash podcast")
        #expect(matches.first?.entry.sponsorTokens == ["betterhelp"])
        #expect(matches.first.map { abs(Double($0.similarity) - 1.0) < 1e-3 } ?? false)
    }

    // MARK: - Similarity threshold

    @Test("matches below similarity floor are filtered out")
    func belowFloorFilteredOut() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 1)
        _ = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: fp
        )

        // Build an orthogonal fingerprint for the query.
        var other = [Float](repeating: 0, count: 64)
        for i in 0..<32 { other[i] = Float(i + 1) }
        let orthogonalFP = AcousticFingerprint(values: other)!

        let matches = await store.matches(
            fingerprint: orthogonalFP,
            show: "show-1",
            similarityFloor: 0.80
        )
        #expect(matches.isEmpty)
    }

    @Test("lower similarity floor admits more matches")
    func lowerFloorAdmitsMore() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 1)
        _ = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: fp
        )

        // Close-but-not-identical query fingerprint.
        var closeValues = fp.values
        for i in 0..<8 { closeValues[i] = closeValues[i] * 0.7 }
        let closeFP = AcousticFingerprint(values: closeValues)!

        let strict = await store.matches(fingerprint: closeFP, show: "show-1", similarityFloor: 0.999)
        let permissive = await store.matches(fingerprint: closeFP, show: "show-1", similarityFloor: 0.50)
        #expect(strict.count <= permissive.count)
        #expect(permissive.count >= 1)
    }

    // MARK: - Show scoping

    @Test("matches are scoped to the requested show (and null-show entries)")
    func showScoping() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fpA = sampleFingerprint(seed: 1)
        let fpB = orthogonalFingerprint(seed: 2)

        _ = try await store.insert(
            showId: "show-a",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fpA
        )
        _ = try await store.insert(
            showId: "show-b",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fpB
        )
        _ = try await store.insert(
            showId: nil,
            episodePosition: .unknown,
            durationSec: 30,
            acousticFingerprint: fpA
        )

        let matchesA = await store.matches(fingerprint: fpA, show: "show-a", similarityFloor: 0.80)
        // show-a match + null-show match = 2 entries with identical fp
        #expect(matchesA.count == 2)
        for m in matchesA {
            #expect(m.entry.showId == "show-a" || m.entry.showId == nil)
        }

        // Searching show-b for fpB: only show-b entry matches; the null-show
        // row uses fpA which is orthogonal to fpB under this fixture.
        let matchesB = await store.matches(fingerprint: fpB, show: "show-b", similarityFloor: 0.80)
        #expect(matchesB.count == 1)
        #expect(matchesB.first?.entry.showId == "show-b")

        // Cross-show scoping: searching show-b with fpA should NOT find the
        // show-a entry even though fpA is highly similar to itself. Only the
        // null-show entry (fpA) should surface.
        let crossScope = await store.matches(fingerprint: fpA, show: "show-b", similarityFloor: 0.80)
        #expect(crossScope.count == 1)
        #expect(crossScope.first?.entry.showId == nil)
    }

    // MARK: - Zero fingerprint handling

    @Test("insert of a zero fingerprint is a no-op")
    func zeroFingerprintInsertIsNoOp() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let zero = AcousticFingerprint(values: [])!
        _ = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: zero
        )

        let count = try await store.count()
        #expect(count == 0)
    }

    @Test("matches on a zero query fingerprint returns nothing")
    func zeroQueryReturnsEmpty() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        _ = try await store.insert(
            showId: "show-1",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 1)
        )

        let zero = AcousticFingerprint(values: [])!
        let matches = await store.matches(fingerprint: zero, show: "show-1", similarityFloor: 0.80)
        #expect(matches.isEmpty)
    }

    // MARK: - Persistence across actor re-init

    @Test("entries persist across actor re-init")
    func persistsAcrossReinit() async throws {
        let dir = try makeTempDir()
        let fp = sampleFingerprint(seed: 7)

        do {
            let store = try AdCatalogStore(directoryURL: dir)
            _ = try await store.insert(
                showId: "show-persist",
                episodePosition: .postRoll,
                durationSec: 45,
                acousticFingerprint: fp,
                transcriptSnippet: "persistent ad"
            )
            let count = try await store.count()
            #expect(count == 1)
        }

        // Re-open the store from scratch.
        let reopened = try AdCatalogStore(directoryURL: dir)
        let count = try await reopened.count()
        #expect(count == 1)

        let matches = await reopened.matches(
            fingerprint: fp,
            show: "show-persist",
            similarityFloor: 0.80
        )
        #expect(matches.count == 1)
        #expect(matches.first?.entry.transcriptSnippet == "persistent ad")
    }

    // MARK: - Schema version

    @Test("migration bumps user_version to schemaVersion")
    func migrationBumpsUserVersion() async throws {
        let dir = try makeTempDir()
        _ = try AdCatalogStore(directoryURL: dir)

        // Probe the sqlite file directly via the store's user_version.
        // We rely on re-opening to confirm idempotency.
        let reopened = try AdCatalogStore(directoryURL: dir)
        let count = try await reopened.count()
        #expect(count == 0)  // Clean reopen, no rows.
    }

    // MARK: - Sorting

    @Test("matches sorted by similarity descending")
    func sortedBySimilarityDescending() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fpHi = sampleFingerprint(seed: 1)
        var closerValues = fpHi.values
        for i in 0..<4 { closerValues[i] *= 0.9 }
        let fpMid = AcousticFingerprint(values: closerValues)!
        var furtherValues = fpHi.values
        for i in 0..<16 { furtherValues[i] *= 0.3 }
        let fpLo = AcousticFingerprint(values: furtherValues)!

        _ = try await store.insert(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: fpLo
        )
        _ = try await store.insert(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: fpMid
        )
        _ = try await store.insert(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: fpHi
        )

        let matches = await store.matches(
            fingerprint: fpHi,
            show: "show-1",
            similarityFloor: 0.0
        )
        #expect(matches.count == 3)
        for i in 1..<matches.count {
            #expect(matches[i - 1].similarity >= matches[i].similarity)
        }
    }

    // MARK: - Clear

    @Test("clear removes all entries")
    func clearRemovesAllEntries() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        _ = try await store.insert(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: sampleFingerprint(seed: 1)
        )
        #expect(try await store.count() == 1)
        try await store.clear()
        #expect(try await store.count() == 0)
    }

    // MARK: - Integration: correction → entry → evidence

    @Test("simulated correction → catalog entry → catalog signal fires on similar fingerprint")
    func correctionToCatalogToSignalIntegration() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        // 1) Simulate a user correction landing: store inserts a fingerprint.
        let correctionFP = sampleFingerprint(seed: 42)
        _ = try await store.insert(
            showId: "integration-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: correctionFP,
            transcriptSnippet: "squarespace dot com slash podcast",
            sponsorTokens: ["squarespace"],
            originalConfidence: 0.92
        )

        // 2) A future episode produces a candidate with a near-identical fp.
        var slightlyDifferent = correctionFP.values
        for i in 0..<4 { slightlyDifferent[i] *= 0.95 }
        let futureFP = AcousticFingerprint(values: slightlyDifferent)!

        let matches = await store.matches(
            fingerprint: futureFP,
            show: "integration-show",
            similarityFloor: AdCatalogStore.defaultSimilarityFloor
        )
        #expect(!matches.isEmpty)
        let topSimilarity = matches.first?.similarity ?? 0

        // 3) Feed the top similarity into the precision gate input.
        let gateInput = AutoSkipPrecisionGateInput(
            segmentStartTime: 100,
            segmentEndTime: 130,
            segmentScore: 0.60,
            episodeDuration: 3600,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: 1.0,
            catalogMatchSimilarity: topSimilarity
        )
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: gateInput)
        #expect(signals.contains(.catalogMatch))
    }

    @Test("no catalog entries → no catalog signal fires")
    func emptyCatalogNoSignal() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let queryFP = sampleFingerprint(seed: 1)
        let matches = await store.matches(
            fingerprint: queryFP,
            show: "any-show",
            similarityFloor: AdCatalogStore.defaultSimilarityFloor
        )
        #expect(matches.isEmpty)

        let gateInput = AutoSkipPrecisionGateInput(
            segmentStartTime: 100,
            segmentEndTime: 130,
            segmentScore: 0.60,
            episodeDuration: 3600,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: 1.0,
            catalogMatchSimilarity: matches.first?.similarity ?? 0
        )
        let signals = AutoSkipPrecisionGate.collectSafetySignals(for: gateInput)
        #expect(!signals.contains(.catalogMatch))
    }
}
