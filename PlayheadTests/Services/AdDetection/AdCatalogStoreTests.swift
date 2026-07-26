// AdCatalogStoreTests.swift
// playhead-gtt9.13: Tests for the on-device ad catalog SQLite store.

import Foundation
import SQLite3
import Testing
@testable import Playhead

@Suite("AdCatalogStore")
struct AdCatalogStoreTests {

    // MARK: - Helpers

    private struct CalibrationFixture: Decodable {
        struct Observations: Decodable {
            let catalogRowCount: Int
            let legacyPairCount: Int
            let legacyCosineMinimum: Float
            let legacyCosineMedian: Float
            let legacyCosineP90: Float
            let legacyPairsAtPointEight: Int
            let mixedLegacyMatch: Float
            let v2MaximumNegative: Float
            let v2MixedMaximum: Float
            let catalogOnMixedConfidence: Double
            let catalogOffMixedConfidence: Double
        }

        struct THEMOVESpan: Decodable {
            let spanId: String
            let startTime: Double
            let endTime: Double
            let classification: String
            let startEdgeAnchor: String
            let endEdgeAnchor: String
            let fingerprintBase64: String
        }

        struct PositivePair: Decodable {
            let name: String
            let expectedSimilarity: Float
            let lhsBase64: String
            let rhsBase64: String
        }

        let schemaVersion: Int
        let observations: Observations
        let themove: THEMOVESpan
        let legacyCatalogFingerprints: [String]
        let positivePairs: [PositivePair]
    }

    private func calibrationFixture() throws -> CalibrationFixture {
        let testsDirectory = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let url = testsDirectory
            .appendingPathComponent("Fixtures", isDirectory: true)
            .appendingPathComponent("AdCatalog", isDirectory: true)
            .appendingPathComponent("themove-catalog-calibration.json")
        return try JSONDecoder().decode(
            CalibrationFixture.self,
            from: Data(contentsOf: url)
        )
    }

    private func fixtureFingerprint(
        _ base64: String,
        version: CatalogFingerprintVersion
    ) throws -> AcousticFingerprint {
        let data = try #require(Data(base64Encoded: base64))
        return try #require(AcousticFingerprint(data: data, version: version))
    }

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

    // MARK: - Real-device calibration (playhead-o4qr)

    @Test("legacy cosine reproduces the saturated 50-row THEMOVE catalog distribution")
    func legacyCosineDistributionIsNonSelective() throws {
        let fixture = try calibrationFixture()
        #expect(fixture.schemaVersion == 1)
        #expect(fixture.observations.catalogRowCount == 50)
        #expect(fixture.observations.legacyPairCount == 1_225)
        #expect(fixture.observations.legacyPairsAtPointEight == 1_225)
        #expect(fixture.legacyCatalogFingerprints.count == fixture.observations.catalogRowCount)

        let fingerprints = try fixture.legacyCatalogFingerprints.map {
            try fixtureFingerprint($0, version: .legacyCosineV1)
        }
        var similarities: [Float] = []
        for lhsIndex in fingerprints.indices {
            for rhsIndex in fingerprints.indices where rhsIndex > lhsIndex {
                similarities.append(
                    AcousticFingerprint.legacyCosineSimilarity(
                        fingerprints[lhsIndex],
                        fingerprints[rhsIndex]
                    )
                )
            }
        }
        similarities.sort()

        #expect(similarities.count == 1_225)
        #expect(similarities.count == fixture.observations.legacyPairCount)
        #expect(similarities.filter { $0 >= 0.80 }.count == fixture.observations.legacyPairsAtPointEight)
        #expect(abs((similarities.first ?? 0) - fixture.observations.legacyCosineMinimum) < 0.000_01)
        #expect(abs(similarities[(similarities.count - 1) / 2] - fixture.observations.legacyCosineMedian) < 0.000_01)
        #expect(abs(similarities[Int(Double(similarities.count - 1) * 0.90)] - fixture.observations.legacyCosineP90) < 0.000_01)
    }

    @Test("v2 relative fingerprint rejects real negatives and retains boundary-shifted positives")
    func v2CalibrationSeparatesPositiveAndNegativePairs() throws {
        let fixture = try calibrationFixture()
        #expect(fixture.legacyCatalogFingerprints.count == 50)
        #expect(fixture.positivePairs.count >= 2)
        let realNegatives = try fixture.legacyCatalogFingerprints.map {
            let legacy = try fixtureFingerprint($0, version: .legacyCosineV1)
            return try #require(
                AcousticFingerprint(
                    values: legacy.values,
                    version: .relativeFeatureSummaryV2
                )
            )
        }

        var evaluatedNegativeCount = 0
        var admittedNegativeCount = 0
        var maximumNegative: Float = 0
        for lhsIndex in realNegatives.indices {
            for rhsIndex in realNegatives.indices where rhsIndex > lhsIndex {
                evaluatedNegativeCount += 1
                let score = AcousticFingerprint.similarity(
                    realNegatives[lhsIndex],
                    realNegatives[rhsIndex]
                )
                maximumNegative = max(maximumNegative, score)
                if score >= AdCatalogStore.defaultSimilarityFloor {
                    admittedNegativeCount += 1
                }
            }
        }
        #expect(evaluatedNegativeCount == 1_225)
        #expect(AdCatalogStore.defaultSimilarityFloor == 0.90)
        #expect(
            abs(maximumNegative - fixture.observations.v2MaximumNegative)
                < 0.000_01
        )
        #expect(maximumNegative < AdCatalogStore.defaultSimilarityFloor)
        #expect(
            maximumNegative
                <= AdCatalogStore.defaultSimilarityFloor - 0.03,
            "known negatives need real headroom below the admission floor"
        )
        #expect(admittedNegativeCount == 0)

        for pair in fixture.positivePairs {
            let lhs = try fixtureFingerprint(
                pair.lhsBase64,
                version: .relativeFeatureSummaryV2
            )
            let rhs = try fixtureFingerprint(
                pair.rhsBase64,
                version: .relativeFeatureSummaryV2
            )
            let similarity = AcousticFingerprint.similarity(lhs, rhs)
            #expect(abs(similarity - pair.expectedSimilarity) < 0.000_01)
            #expect(
                similarity >= AdCatalogStore.defaultSimilarityFloor,
                "real positive pair rejected: \(pair.name)"
            )
        }
    }

    @Test("fingerprint layout versions never compare across compatibility cohorts")
    func incompatibleFingerprintVersionsFailClosed() throws {
        let values = (0..<AcousticFingerprint.vectorLength).map {
            Float($0 + 1)
        }
        let legacy = try #require(
            AcousticFingerprint(values: values, version: .legacyCosineV1)
        )
        let feature = try #require(
            AcousticFingerprint(
                values: values,
                version: .relativeFeatureSummaryV2
            )
        )
        let pcm = try #require(
            AcousticFingerprint(
                values: values,
                version: .relativePCMSummaryV1
            )
        )

        #expect(AcousticFingerprint.similarity(legacy, feature) == 0)
        #expect(AcousticFingerprint.similarity(feature, pcm) == 0)
        #expect(AcousticFingerprint.similarity(legacy, pcm) == 0)
        #expect(
            AcousticFingerprint.fromPCM(
                [],
                sampleRate: 16_000
            ).version == .relativePCMSummaryV1
        )
    }

    @Test("THEMOVE mixed unanchored span is rejected by v2 catalog evidence")
    func themoveMixedSpanDoesNotMatchCatalog() async throws {
        let fixture = try calibrationFixture()
        #expect(fixture.themove.spanId == "326ba036ba8cc7d7c6c173e036315998")
        #expect(fixture.themove.startTime == 3493.02)
        #expect(fixture.themove.endTime == 3537.95)
        #expect(fixture.themove.classification == "mixed")
        #expect(fixture.themove.startEdgeAnchor == "unanchored")
        #expect(fixture.themove.endEdgeAnchor == "unanchored")
        #expect(fixture.observations.catalogOnMixedConfidence == 1.0)
        #expect(fixture.observations.catalogOffMixedConfidence == 0.6988968699323662)

        let mixed = try fixtureFingerprint(
            fixture.themove.fingerprintBase64,
            version: .relativeFeatureSummaryV2
        )
        let realCatalog = try fixture.legacyCatalogFingerprints.map {
            let legacy = try fixtureFingerprint($0, version: .legacyCosineV1)
            return try #require(
                AcousticFingerprint(
                    values: legacy.values,
                    version: .relativeFeatureSummaryV2
                )
            )
        }
        let maximumV2Score = realCatalog
            .map { AcousticFingerprint.similarity(mixed, $0) }
            .max() ?? 0
        #expect(maximumV2Score < AdCatalogStore.defaultSimilarityFloor)
        #expect(
            abs(maximumV2Score - fixture.observations.v2MixedMaximum)
                < 0.000_01
        )

        let maximumLegacyScore = realCatalog
            .map { AcousticFingerprint.legacyCosineSimilarity(mixed, $0) }
            .max() ?? 0
        #expect(abs(maximumLegacyScore - fixture.observations.mixedLegacyMatch) < 0.000_01)

        let catalogStore = try AdCatalogStore(directoryURL: makeTempDir())
        for (index, fingerprint) in realCatalog.enumerated() {
            _ = try await catalogStore.insert(
                showId: "themove",
                episodePosition: .unknown,
                durationSec: 30,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.99,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "fixture-asset-\(index)",
                sourceWindowId: "fixture-window-\(index)",
                sourceStartTime: 10,
                sourceEndTime: 40
            )
        }
        let admittedMatches = await catalogStore.matches(
            fingerprint: mixed,
            show: "themove"
        )
        #expect(
            admittedMatches.isEmpty,
            "none of the 50 real rows may admit the mixed THEMOVE span"
        )

        let gateInput = AutoSkipPrecisionGateInput(
            analysisAssetId: "themove-mixed-fixture",
            segmentStartTime: fixture.themove.startTime,
            segmentEndTime: fixture.themove.endTime,
            segmentScore: fixture.observations.catalogOffMixedConfidence,
            episodeDuration: 7_200,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: 1,
            catalogMatchSimilarity: admittedMatches.first?.similarity ?? 0
        )
        switch AutoSkipPrecisionGate.classify(input: gateInput) {
        case .uiCandidate:
            break
        case .detectionOnly, .autoSkipEligible:
            Issue.record(
                "mixed/unanchored THEMOVE span must remain UI-only; catalog evidence alone cannot promote it"
            )
        }

        // Exercise the stronger counterfactual too: even if this exact mixed
        // material had previously been confirmed and therefore produced a
        // perfect learned-catalog hit, that learned evidence remains
        // diagnostic and cannot independently authorize the span.
        _ = try await catalogStore.insert(
            showId: "themove",
            episodePosition: .postRoll,
            durationSec: fixture.themove.endTime - fixture.themove.startTime,
            acousticFingerprint: mixed,
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "themove-mixed-confirmed-source",
            sourceWindowId: fixture.themove.spanId,
            sourceStartTime: fixture.themove.startTime,
            sourceEndTime: fixture.themove.endTime
        )
        let exactLearnedMatches = await catalogStore.matches(
            fingerprint: mixed,
            show: "themove"
        )
        let exactSimilarity = try #require(
            exactLearnedMatches.first?.similarity
        )
        #expect(exactSimilarity == 1)

        let exactCatalogOnlyInput = AutoSkipPrecisionGateInput(
            analysisAssetId: "themove-mixed-fixture",
            segmentStartTime: fixture.themove.startTime,
            segmentEndTime: fixture.themove.endTime,
            segmentScore: fixture.observations.catalogOffMixedConfidence,
            episodeDuration: 7_200,
            overlappingFeatureWindows: [],
            lexicalCategories: [],
            userCorrectionBoostFactor: 1,
            catalogMatchSimilarity: exactSimilarity
        )
        #expect(
            AutoSkipPrecisionGate.collectSafetySignals(
                for: exactCatalogOnlyInput
            ) == [.catalogMatch]
        )
        #expect(
            AutoSkipPrecisionGate.classify(input: exactCatalogOnlyInput)
                == .uiCandidate(reason: .noSafetySignals)
        )
    }

    // MARK: - Insert + query roundtrip

    @Test("insert + matches roundtrip returns the inserted entry")
    func insertMatchRoundtrip() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 1)
        let inserted = try #require(
            try await store.insertConfirmedForTest(
                showId: "show-1",
                episodePosition: .preRoll,
                durationSec: 30,
                acousticFingerprint: fp,
                transcriptSnippet: "betterhelp dot com slash podcast",
                sponsorTokens: ["betterhelp"],
                originalConfidence: 0.9
            )
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
        _ = try await store.insertConfirmedForTest(
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
        _ = try await store.insertConfirmedForTest(
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

    @Test("invalid similarity floors fail closed")
    func invalidSimilarityFloorsFailClosed() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let fingerprint = sampleFingerprint(seed: 2)
        _ = try await store.insertConfirmedForTest(
            showId: "show-floor",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint
        )

        for floor in [
            -Float.ulpOfOne,
            Float.nan,
            Float.infinity,
            1 + Float.ulpOfOne
        ] {
            #expect(
                await store.matches(
                    fingerprint: fingerprint,
                    show: "show-floor",
                    similarityFloor: floor
                ).isEmpty
            )
        }
    }

    // MARK: - Show scoping

    @Test("matches are scoped to the exact requested show")
    func showScoping() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fpA = sampleFingerprint(seed: 1)
        let fpB = orthogonalFingerprint(seed: 2)

        _ = try await store.insertConfirmedForTest(
            showId: "show-a",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fpA
        )
        _ = try await store.insertConfirmedForTest(
            showId: "show-b",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fpB
        )
        let matchesA = await store.matches(fingerprint: fpA, show: "show-a", similarityFloor: 0.80)
        #expect(matchesA.count == 1)
        #expect(matchesA.first?.entry.showId == "show-a")

        // Searching show-b for fpB: only show-b entry matches; the null-show
        // row uses fpA which is orthogonal to fpB under this fixture.
        let matchesB = await store.matches(fingerprint: fpB, show: "show-b", similarityFloor: 0.80)
        #expect(matchesB.count == 1)
        #expect(matchesB.first?.entry.showId == "show-b")

        // Cross-show scoping: searching show-b with fpA must not find show-a.
        let crossScope = await store.matches(fingerprint: fpA, show: "show-b", similarityFloor: 0.80)
        #expect(crossScope.isEmpty)
    }

    @Test("missing and noncanonical show identifiers fail closed for match and learning")
    func missingShowFailsClosed() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        let fp = sampleFingerprint()

        for invalidShow in [
            nil,
            "",
            "   ",
            "\n\t",
            " show-noncanonical ",
            "show-nul-prefix\u{0}other",
        ] as [String?] {
            do {
                _ = try await store.insert(
                    showId: invalidShow,
                    episodePosition: .unknown,
                    durationSec: 30,
                    acousticFingerprint: fp,
                    learningSource: .userMarkedAd,
                    learningLifecycle: .explicitConfirmation,
                    sourceAssetId: "asset-invalid-show",
                    sourceWindowId: "window-invalid-show",
                    sourceStartTime: 10,
                    sourceEndTime: 40
                )
                Issue.record("learning must reject show \(String(describing: invalidShow))")
            } catch AdCatalogStoreError.invalidShowIdentity {
                // Expected fail-closed result.
            }

            let matches = await store.matches(
                fingerprint: fp,
                show: invalidShow
            )
            #expect(matches.isEmpty)
        }
        #expect(try await store.count() == 0)

        _ = try await store.insertConfirmedForTest(
            showId: "show-valid-prefix",
            episodePosition: .unknown,
            durationSec: 30,
            acousticFingerprint: fp
        )
        #expect(
            await store.matches(
                fingerprint: fp,
                show: "show-valid-prefix\u{0}other"
            ).isEmpty,
            "a malformed query must not alias its valid SQLite prefix"
        )
    }

    @Test("learning rejects noncanonical exact source provenance")
    func noncanonicalSourceProvenanceFailsClosed() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let invalidSources = [
            (" asset-source ", "window-source"),
            ("asset-source", " window-source "),
            ("asset-source\u{0}other", "window-source"),
            ("asset-source", "window-source\u{0}other"),
        ]

        for (sourceAssetId, sourceWindowId) in invalidSources {
            do {
                _ = try await store.insert(
                    showId: "show-source-validation",
                    episodePosition: .midRoll,
                    durationSec: 30,
                    acousticFingerprint: sampleFingerprint(seed: 71),
                    learningSource: .userMarkedAd,
                    learningLifecycle: .explicitConfirmation,
                    sourceAssetId: sourceAssetId,
                    sourceWindowId: sourceWindowId,
                    sourceStartTime: 10,
                    sourceEndTime: 40
                )
                Issue.record(
                    "noncanonical source provenance must not be sanitized"
                )
            } catch AdCatalogStoreError.invalidLearningProvenance {
                // Expected fail-closed result.
            }
        }
        #expect(try await store.count() == 0)
    }

    // MARK: - Zero fingerprint handling

    @Test("insert of a zero fingerprint is a no-op")
    func zeroFingerprintInsertIsNoOp() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let zero = AcousticFingerprint(values: [])!
        _ = try await store.insertConfirmedForTest(
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

        _ = try await store.insertConfirmedForTest(
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
            _ = try await store.insertConfirmedForTest(
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

    @Test("source snippet preserves embedded NUL across persistence")
    func sourceSnippetPreservesEmbeddedNUL() async throws {
        let dir = try makeTempDir()
        let fp = sampleFingerprint(seed: 71)
        let snippet = "verbatim-before\u{0}verbatim-after"

        let initial = try AdCatalogStore(directoryURL: dir)
        _ = try await initial.insertConfirmedForTest(
            showId: "show-persist-nul-snippet",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            transcriptSnippet: snippet
        )
        await initial.close()

        let reopened = try AdCatalogStore(directoryURL: dir)
        let match = await reopened.matches(
            fingerprint: fp,
            show: "show-persist-nul-snippet"
        ).first
        #expect(match?.entry.transcriptSnippet == snippet)
    }

    // MARK: - Schema version

    @Test("migration bumps user_version to schemaVersion")
    func migrationBumpsUserVersion() async throws {
        let dir = try makeTempDir()
        let initial = try AdCatalogStore(directoryURL: dir)
        #expect(try await initial.count() == 0)
        await initial.close()

        var handle: OpaquePointer?
        let dbURL = dir.appendingPathComponent("ad_catalog.sqlite")
        guard sqlite3_open_v2(
            dbURL.path,
            &handle,
            SQLITE_OPEN_READONLY,
            nil
        ) == SQLITE_OK, let db = handle else {
            Issue.record("could not open catalog schema fixture")
            return
        }
        var statement: OpaquePointer?
        #expect(
            sqlite3_prepare_v2(
                db,
                "PRAGMA user_version",
                -1,
                &statement,
                nil
            ) == SQLITE_OK
        )
        if let statement {
            #expect(sqlite3_step(statement) == SQLITE_ROW)
            #expect(
                sqlite3_column_int(statement, 0)
                    == AdCatalogStore.schemaVersion
            )
            sqlite3_finalize(statement)
        }
        sqlite3_close(db)

        // Re-opening confirms that the current migration is idempotent.
        let reopened = try AdCatalogStore(directoryURL: dir)
        let count = try await reopened.count()
        #expect(count == 0)  // Clean reopen, no rows.
    }

    @Test("current-schema reopen repairs additive revocation objects idempotently")
    func currentSchemaRepairsAdditiveObjects() async throws {
        let dir = try makeTempDir()
        let initial = try AdCatalogStore(directoryURL: dir)
        #expect(try await initial.count() == 0)
        await initial.close()

        var handle: OpaquePointer?
        let dbURL = dir.appendingPathComponent("ad_catalog.sqlite")
        guard sqlite3_open_v2(
            dbURL.path,
            &handle,
            SQLITE_OPEN_READWRITE,
            nil
        ) == SQLITE_OK, let db = handle else {
            Issue.record("could not reopen catalog fixture")
            return
        }
        #expect(sqlite3_exec(
            db,
            """
            DROP TABLE ad_catalog_revocations;
            DROP INDEX idx_catalog_active_show_version;
            ALTER TABLE ad_catalog_entries DROP COLUMN confirmed_at;
            """,
            nil,
            nil,
            nil
        ) == SQLITE_OK)
        sqlite3_close(db)

        let repaired = try AdCatalogStore(directoryURL: dir)
        #expect(try await repaired.revoke(
            matchedEntryId: nil,
            sourceAssetId: "repair-asset",
            sourceWindowId: "repair-window",
            source: .manualVeto
        ) == 0)
        #expect(try await repaired.insert(
            showId: "repair-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 12),
            originalConfidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "repair-asset",
            sourceWindowId: "repair-window",
            sourceStartTime: 10,
            sourceEndTime: 40
        ) == nil)
        #expect(try await repaired.insert(
            showId: "repair-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 13),
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "independent-repair-asset",
            sourceWindowId: "independent-repair-window",
            sourceStartTime: 10,
            sourceEndTime: 40
        ) != nil)
        await repaired.close()

        // A second reopen exercises the already-repaired idempotent path.
        let reopened = try AdCatalogStore(directoryURL: dir)
        #expect(try await reopened.count() == 1)
    }

    @Test("current-schema repair deterministically deduplicates creative rows")
    func currentSchemaRepairDeduplicatesBeforeUniqueIndex() async throws {
        let dir = try makeTempDir()
        let initial = try AdCatalogStore(directoryURL: dir)
        let fingerprint = sampleFingerprint(seed: 121)
        let original = try #require(try await initial.insert(
            showId: "repair-duplicate-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.9,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "repair-duplicate-asset",
            sourceWindowId: "repair-duplicate-window",
            sourceStartTime: 10,
            sourceEndTime: 40
        ))
        await initial.close()

        var handle: OpaquePointer?
        let dbURL = dir.appendingPathComponent("ad_catalog.sqlite")
        guard sqlite3_open_v2(
            dbURL.path,
            &handle,
            SQLITE_OPEN_READWRITE,
            nil
        ) == SQLITE_OK, let db = handle else {
            Issue.record("could not reopen duplicate repair fixture")
            return
        }
        let duplicateId = UUID()
        #expect(sqlite3_exec(
            db,
            """
            DROP INDEX idx_catalog_show_version_fingerprint;
            INSERT INTO ad_catalog_entries
            SELECT '\(duplicateId.uuidString)', created_at + 1, show_id,
                   episode_position, duration_sec, fingerprint_blob,
                   transcript_snippet, sponsor_tokens_json, 0.8,
                   fingerprint_version, learning_source,
                   learning_lifecycle, source_asset_id, source_window_id,
                   source_start_time, source_end_time, confirmed_at,
                   200, 'manualVeto'
            FROM ad_catalog_entries
            WHERE id = '\(original.id.uuidString)';
            """,
            nil,
            nil,
            nil
        ) == SQLITE_OK)
        sqlite3_close(db)

        let repaired = try AdCatalogStore(directoryURL: dir)
        let entries = try await repaired.allEntries()
        #expect(entries.map(\.id) == [duplicateId])
        #expect(entries.first?.revokedAt == Date(timeIntervalSince1970: 200))
        #expect(entries.first?.revocationSource == .manualVeto)
        await repaired.close()

        let reopened = try AdCatalogStore(directoryURL: dir)
        #expect(try await reopened.allEntries().map(\.id) == [duplicateId])
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

        _ = try await store.insertConfirmedForTest(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: fpLo
        )
        _ = try await store.insertConfirmedForTest(
            showId: "show-1", episodePosition: .preRoll,
            durationSec: 30, acousticFingerprint: fpMid
        )
        _ = try await store.insertConfirmedForTest(
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
        _ = try await store.insertConfirmedForTest(
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
        _ = try await store.insertConfirmedForTest(
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
            analysisAssetId: "asset-catalog-integration",
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
            analysisAssetId: "asset-empty-catalog",
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

    // MARK: - Per-show growth bound (V2 schema)

    @Test("active per-show rows are bounded without evicting revocations")
    func perShowRowCountBounded() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        var revokedValues = [Float](repeating: 0, count: 64)
        revokedValues[62] = 1
        revokedValues[63] = 2
        let revokedFingerprint = try #require(
            AcousticFingerprint(values: revokedValues)
        )
        let revokedEntry = try #require(
            try await store.insertConfirmedForTest(
                showId: "show-bounded",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: revokedFingerprint,
                originalConfidence: 0.9
            )
        )
        _ = try await store.revoke(
            matchedEntryId: revokedEntry.id,
            sourceAssetId: try #require(revokedEntry.sourceAssetId),
            sourceWindowId: try #require(revokedEntry.sourceWindowId),
            source: .manualVeto,
            at: Date(timeIntervalSince1970: 200)
        )

        // Exercise the production ceiling so the test covers the exact
        // eviction predicate used in shipped builds.
        let cap = AdCatalogStore.maxEntriesPerShow
        let surplus = 25
        for seed in 0..<(cap + surplus) {
            // Each iteration uses a distinct fingerprint so the
            // (show_id, fingerprint_blob) UNIQUE constraint does not
            // collapse rows — eviction is the only mechanism that can
            // keep the row count bounded. Encode `seed` directly into
            // the first slots so each fingerprint is unique across the
            // full test range (avoid modular cycles).
            var values = [Float](repeating: 0, count: 64)
            values[0] = Float(seed) + 1.0
            values[1] = Float(seed >> 8) + 1.0
            for i in 2..<64 {
                values[i] = Float(i) + 1.0
            }
            let fp = AcousticFingerprint(values: values)!
            _ = try await store.insertConfirmedForTest(
                showId: "show-bounded",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: fp,
                originalConfidence: 0.5
            )
        }

        let entriesForShow = try await store.allEntries()
            .filter { $0.showId == "show-bounded" }
        #expect(entriesForShow.filter { $0.revokedAt == nil }.count == cap)
        #expect(
            entriesForShow.contains {
                $0.id == revokedEntry.id && $0.revokedAt != nil
            },
            "capacity eviction must retain the creative tombstone"
        )

        // Force the next capacity eviction to fail. Insert and eviction are
        // one transaction, so the newly inserted overflow row must roll back.
        var db: OpaquePointer?
        let dbPath = dir.appendingPathComponent("ad_catalog.sqlite").path
        guard sqlite3_open_v2(
            dbPath,
            &db,
            SQLITE_OPEN_READWRITE,
            nil
        ) == SQLITE_OK, let db else {
            Issue.record("could not open catalog database for fault injection")
            return
        }
        #expect(
            sqlite3_exec(
                db,
                """
                CREATE TRIGGER reject_catalog_eviction
                BEFORE DELETE ON ad_catalog_entries
                BEGIN
                    SELECT RAISE(ABORT, 'forced eviction failure');
                END;
                """,
                nil,
                nil,
                nil
            ) == SQLITE_OK
        )
        sqlite3_close(db)

        var overflowValues = [Float](repeating: 1, count: 64)
        overflowValues[0] = 10_000
        let overflowFingerprint = try #require(
            AcousticFingerprint(values: overflowValues)
        )
        await #expect(throws: AdCatalogStoreError.self) {
            _ = try await store.insert(
                showId: "show-bounded",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: overflowFingerprint,
                originalConfidence: 1,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "overflow-asset",
                sourceWindowId: "overflow-window",
                sourceStartTime: 10,
                sourceEndTime: 40,
                confirmedAt: Date(timeIntervalSince1970: 300)
            )
        }
        let afterFailedEviction = try await store.allEntries()
            .filter { $0.showId == "show-bounded" }
        #expect(
            afterFailedEviction.filter { $0.revokedAt == nil }.count == cap
        )
        #expect(
            !afterFailedEviction.contains {
                $0.sourceWindowId == "overflow-window"
            },
            "failed capacity maintenance must not leave a partial insert"
        )

        let delayedRelearning = try await store.insert(
            showId: "show-bounded",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: revokedFingerprint,
            originalConfidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "later-consumed-asset",
            sourceWindowId: "later-consumed-window",
            sourceStartTime: 50,
            sourceEndTime: 80,
            confirmedAt: Date(timeIntervalSince1970: 300)
        )
        #expect(
            delayedRelearning == nil,
            "eviction pressure must not let consumed learning resurrect a correction"
        )
    }

    // MARK: - UPSERT confidence-MAX + identity (V3)

    @Test("UPSERT cannot retarget an existing catalog UUID")
    func upsertCannotRetargetStableIdentity() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let stableId = UUID()
        let originalFingerprint = sampleFingerprint(seed: 41)
        let otherFingerprint = sampleFingerprint(seed: 42)

        func entry(
            showId: String,
            fingerprint: AcousticFingerprint,
            sourceWindowId: String
        ) -> CatalogEntry {
            CatalogEntry(
                id: stableId,
                createdAt: Date(timeIntervalSince1970: 100),
                showId: showId,
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.9,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "stable-id-asset",
                sourceWindowId: sourceWindowId,
                sourceStartTime: 10,
                sourceEndTime: 40,
                confirmedAt: Date(timeIntervalSince1970: 100)
            )
        }

        let original = entry(
            showId: "stable-id-show",
            fingerprint: originalFingerprint,
            sourceWindowId: "stable-id-original"
        )
        #expect(try await store.insert(entry: original)?.id == stableId)
        #expect(try await store.insert(entry: entry(
            showId: "stable-id-show",
            fingerprint: otherFingerprint,
            sourceWindowId: "stable-id-other-fingerprint"
        )) == nil)
        #expect(try await store.insert(entry: entry(
            showId: "different-show",
            fingerprint: originalFingerprint,
            sourceWindowId: "stable-id-other-show"
        )) == nil)

        let persisted = try #require(try await store.allEntries().first)
        #expect(try await store.count() == 1)
        #expect(persisted.id == stableId)
        #expect(persisted.showId == "stable-id-show")
        #expect(persisted.acousticFingerprint == originalFingerprint)
        #expect(persisted.sourceWindowId == "stable-id-original")
    }

    @Test("UPSERT on (show, fingerprint) lifts original_confidence to the higher value")
    func upsertConfidenceMaxOnShowFingerprint() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 7)

        // First insert at moderate confidence.
        let first = try #require(
            try await store.insertConfirmedForTest(
                showId: "merge-show",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: fp,
                originalConfidence: 0.5
            )
        )

        // Re-insert with a higher confidence. The (show, fingerprint)
        // UNIQUE collision should lift original_confidence to 0.9.
        let second = try #require(
            try await store.insertConfirmedForTest(
                showId: "merge-show",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: fp,
                originalConfidence: 0.9
            )
        )

        let entries = (try await store.allEntries()).filter { $0.showId == "merge-show" }
        #expect(entries.count == 1)
        #expect(second.id == first.id)
        #expect(entries.first?.id == first.id)
        #expect(second.sourceWindowId == entries.first?.sourceWindowId)
        #expect(entries.first?.originalConfidence == 0.9)

        // Re-insert again with a LOWER confidence. The MAX semantics
        // must keep the existing 0.9 — confidence must never regress.
        _ = try await store.insertConfirmedForTest(
            showId: "merge-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            originalConfidence: 0.3
        )

        let after = (try await store.allEntries()).filter { $0.showId == "merge-show" }
        #expect(after.count == 1)
        #expect(after.first?.originalConfidence == 0.9)
    }

    @Test("learning source and lifecycle must be an authoritative pair")
    func invalidLearningSourceLifecyclePairIsRejected() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())

        do {
            _ = try await store.insert(
                showId: "show-invalid-lifecycle",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: sampleFingerprint(seed: 8),
                learningSource: .manualSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "asset-invalid-lifecycle",
                sourceWindowId: "window-invalid-lifecycle",
                sourceStartTime: 10,
                sourceEndTime: 40
            )
            Issue.record(
                "manual skip must not be persisted as delayed consumption"
            )
        } catch AdCatalogStoreError.invalidLearningProvenance {
            // Expected.
        }

        #expect(try await store.count() == 0)
    }

    @Test("non-finite learning timestamps are rejected")
    func nonFiniteLearningTimestampIsRejected() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let entry = CatalogEntry(
            createdAt: Date(timeIntervalSince1970: .infinity),
            showId: "show-invalid-timestamp",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 81),
            originalConfidence: 0.9,
            learningSource: .manualSkip,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-invalid-timestamp",
            sourceWindowId: "window-invalid-timestamp",
            sourceStartTime: 10,
            sourceEndTime: 40,
            confirmedAt: Date(timeIntervalSince1970: 1_800_000_000)
        )

        do {
            _ = try await store.insert(entry: entry)
            Issue.record("non-finite creation timestamps must fail closed")
        } catch AdCatalogStoreError.invalidLearningProvenance {
            // Expected.
        }

        #expect(try await store.count() == 0)
    }

    @Test("delayed consumption cannot downgrade explicit confirmation provenance")
    func consumedLearningCannotDowngradeExplicitConfirmation() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let fingerprint = sampleFingerprint(seed: 9)
        let explicit = try #require(
            try await store.insert(
                showId: "show-lifecycle-precedence",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.90,
                learningSource: .confirmedAutoSkipBanner,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "explicit-asset",
                sourceWindowId: "explicit-window",
                sourceStartTime: 10,
                sourceEndTime: 40
            )
        )

        let delayed = try await store.insert(
            showId: "show-lifecycle-precedence",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.99,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "consumed-asset",
            sourceWindowId: "consumed-window",
            sourceStartTime: 50,
            sourceEndTime: 80
        )

        #expect(delayed == nil)
        let persisted = try #require(try await store.allEntries().first)
        #expect(persisted.id == explicit.id)
        #expect(persisted.learningSource == .confirmedAutoSkipBanner)
        #expect(persisted.learningLifecycle == .explicitConfirmation)
        #expect(persisted.sourceAssetId == "explicit-asset")
        #expect(persisted.sourceWindowId == "explicit-window")
        #expect(persisted.originalConfidence == 0.90)
    }

    @Test("stronger lifecycle cannot regress catalog recency clocks")
    func strongerLifecyclePreservesMonotonicClocks() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let fingerprint = sampleFingerprint(seed: 901)
        let durableTime = Date(timeIntervalSince1970: 200)
        let original = try #require(try await store.insert(entry: CatalogEntry(
            createdAt: durableTime,
            showId: "show-lifecycle-clock",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.9,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "consumed-clock-asset",
            sourceWindowId: "consumed-clock-window",
            sourceStartTime: 10,
            sourceEndTime: 40,
            confirmedAt: durableTime
        )))

        let staleTime = Date(timeIntervalSince1970: 100)
        let upgraded = try #require(try await store.insert(entry: CatalogEntry(
            createdAt: staleTime,
            showId: "show-lifecycle-clock",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "explicit-clock-asset",
            sourceWindowId: "explicit-clock-window",
            sourceStartTime: 50,
            sourceEndTime: 80,
            confirmedAt: staleTime
        )))

        #expect(upgraded.id == original.id)
        #expect(upgraded.learningLifecycle == .explicitConfirmation)
        #expect(upgraded.sourceAssetId == "explicit-clock-asset")
        #expect(upgraded.createdAt == durableTime)
        #expect(upgraded.confirmedAt == durableTime)
    }

    @Test("older same-lifecycle learning cannot replace newer provenance")
    func sameLifecycleLearningTimestampIsMonotonic() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let fingerprint = sampleFingerprint(seed: 91)
        let newer = try #require(try await store.insert(
            showId: "show-catalog-recency",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.9,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "newer-catalog-asset",
            sourceWindowId: "newer-catalog-window",
            sourceStartTime: 10,
            sourceEndTime: 40,
            confirmedAt: Date(timeIntervalSince1970: 200)
        ))

        #expect(try await store.insert(
            showId: "show-catalog-recency",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "older-catalog-asset",
            sourceWindowId: "older-catalog-window",
            sourceStartTime: 50,
            sourceEndTime: 80,
            confirmedAt: Date(timeIntervalSince1970: 100)
        ) == nil)

        let persisted = try #require(try await store.allEntries().first)
        #expect(persisted.id == newer.id)
        #expect(persisted.confirmedAt == Date(timeIntervalSince1970: 200))
        #expect(persisted.sourceAssetId == "newer-catalog-asset")
        #expect(persisted.sourceWindowId == "newer-catalog-window")
        #expect(persisted.sourceStartTime == 10)
        #expect(persisted.sourceEndTime == 40)
    }

    @Test("UPSERT preserves NULL original_confidence when both old and new are NULL")
    func upsertConfidenceNullPreservedWhenBothNull() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)

        let fp = sampleFingerprint(seed: 11)

        // First insert with NULL confidence (omit the parameter).
        _ = try await store.insertConfirmedForTest(
            showId: "null-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp
        )
        // Re-insert with NULL confidence — collision on (show, fingerprint)
        // path. The merged column must remain NULL ("unknown"), not silently
        // become 0.0 ("we measured zero").
        _ = try await store.insertConfirmedForTest(
            showId: "null-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp
        )

        let entries = (try await store.allEntries()).filter { $0.showId == "null-show" }
        #expect(entries.count == 1)
        #expect(entries.first?.originalConfidence == nil,
                "NULL × NULL UPSERT must preserve NULL — distinguishing 'unknown' from 'measured zero' is contractually meaningful.")
    }

    // MARK: - Provenance + revocation

    @Test("learning provenance persists and a veto soft-revokes the matching entry")
    func provenancePersistsAndRevocationRemovesAdmission() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        let fp = sampleFingerprint(seed: 14)
        let confirmedAt = Date(timeIntervalSince1970: 1_700_000_000)
        let inserted = try #require(
            try await store.insert(
                showId: "show-audit",
                episodePosition: .midRoll,
                durationSec: 44.93,
                acousticFingerprint: fp,
                originalConfidence: 0.97,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "asset-audit",
                sourceWindowId: "window-audit",
                sourceStartTime: 3493.02,
                sourceEndTime: 3537.95,
                confirmedAt: confirmedAt
            )
        )

        let reopened = try AdCatalogStore(directoryURL: dir)
        let persisted = try #require(try await reopened.allEntries().first)
        #expect(persisted.id == inserted.id)
        #expect(persisted.acousticFingerprint.version == .relativeFeatureSummaryV2)
        #expect(persisted.learningSource == .consumedAutoSkip)
        #expect(persisted.learningLifecycle == .consumed)
        #expect(persisted.sourceAssetId == "asset-audit")
        #expect(persisted.sourceWindowId == "window-audit")
        #expect(persisted.sourceStartTime == 3493.02)
        #expect(persisted.sourceEndTime == 3537.95)
        #expect(persisted.confirmedAt == confirmedAt)
        #expect(persisted.revokedAt == nil)

        let revoked = try await reopened.revoke(
            matchedEntryId: inserted.id,
            sourceAssetId: "asset-correction",
            sourceWindowId: "window-correction",
            source: .manualVeto,
            showId: "show-audit",
            at: Date(timeIntervalSince1970: 1_700_000_100)
        )
        #expect(revoked == 1)
        #expect(
            await reopened.matches(fingerprint: fp, show: "show-audit")
                .isEmpty
        )
        let audited = try #require(try await reopened.allEntries().first)
        #expect(audited.revokedAt != nil)
        #expect(audited.revocationSource == .manualVeto)
    }

    @Test("revocation tombstone beats delayed learning for the same source window")
    func revocationTombstonePreventsLateInsert() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        _ = try await store.revoke(
            matchedEntryId: nil,
            sourceAssetId: "asset-race",
            sourceWindowId: "window-race",
            source: .listenRevert
        )

        let inserted = try await store.insert(
            showId: "show-race",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 15),
            originalConfidence: 0.99,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-race",
            sourceWindowId: "window-race",
            sourceStartTime: 10,
            sourceEndTime: 40
        )
        #expect(inserted == nil)
        let explicitRetry = try await store.insert(
            showId: "show-race",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 15),
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-race",
            sourceWindowId: "window-race",
            sourceStartTime: 10,
            sourceEndTime: 40
        )
        #expect(
            explicitRetry == nil,
            "the exact source tombstone is terminal even for a later explicit writer"
        )
        #expect(try await store.count() == 0)
    }

    @Test("exact revocation geometry does not tombstone a same-ID replacement")
    func revocationTombstoneIsExactGeometryScoped() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let oldFingerprint = sampleFingerprint(seed: 150)
        let replacementFingerprint = orthogonalFingerprint(seed: 151)
        let old = try #require(try await store.insert(
            showId: "show-exact-geometry",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: oldFingerprint,
            originalConfidence: 0.95,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id",
            sourceStartTime: 10,
            sourceEndTime: 40
        ))
        let replacement = try #require(try await store.insert(
            showId: "show-exact-geometry",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: replacementFingerprint,
            originalConfidence: 0.96,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id",
            sourceStartTime: 50,
            sourceEndTime: 80
        ))

        #expect(try await store.revoke(
            matchedEntryId: nil,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id",
            sourceStartTime: 10,
            sourceEndTime: 40,
            source: .manualVeto
        ) == 1)

        let rows = try await store.allEntries()
        #expect(rows.first { $0.id == old.id }?.revokedAt != nil)
        #expect(rows.first { $0.id == replacement.id }?.revokedAt == nil)
        #expect(
            await store.matches(
                fingerprint: replacementFingerprint,
                show: "show-exact-geometry"
            ).map(\.entry.id) == [replacement.id]
        )

        #expect(try await store.insert(
            showId: "show-exact-geometry",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 152),
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id",
            sourceStartTime: 10,
            sourceEndTime: 40
        ) == nil)
        #expect(try await store.insert(
            showId: "show-exact-geometry",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 153),
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-exact-geometry",
            sourceWindowId: "reused-window-id",
            sourceStartTime: 90,
            sourceEndTime: 120
        ) != nil)
    }

    @Test("signed-zero geometry shares the SQLite revocation identity")
    func signedZeroRevocationCannotResurrectSource() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        let fingerprint = sampleFingerprint(seed: 180)
        let original = try #require(try await store.insert(
            showId: "show-signed-zero",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "asset-signed-zero",
            sourceWindowId: "window-signed-zero",
            sourceStartTime: 0.0,
            sourceEndTime: 30
        ))

        #expect(try await store.revoke(
            matchedEntryId: nil,
            sourceAssetId: "asset-signed-zero",
            sourceWindowId: "window-signed-zero",
            sourceStartTime: -0.0,
            sourceEndTime: 30,
            source: .manualVeto
        ) == 1)
        let rows = try await store.allEntries()
        #expect(
            rows.first { $0.id == original.id }?.revokedAt != nil
        )
        #expect(try await store.insert(
            showId: "show-signed-zero",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 181),
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-signed-zero",
            sourceWindowId: "window-signed-zero",
            sourceStartTime: 0.0,
            sourceEndTime: 30
        ) == nil)

        await store.close()
        let legacyKey = try #require(
            RecurrenceMaterialIdentity
                .legacyNegativeZeroTombstoneWindowKey(
                    sourceWindowId: "legacy-window-signed-zero",
                    sourceStartTime: -0.0,
                    sourceEndTime: 30
                )
        )
        var handle: OpaquePointer?
        #expect(sqlite3_open_v2(
            dir.appendingPathComponent("ad_catalog.sqlite").path,
            &handle,
            SQLITE_OPEN_READWRITE,
            nil
        ) == SQLITE_OK)
        let db = try #require(handle)
        var statement: OpaquePointer?
        #expect(sqlite3_prepare_v2(
            db,
            """
            INSERT INTO ad_catalog_revocations
                (source_asset_id, source_window_id, revoked_at,
                 revocation_source)
            VALUES (?, ?, ?, ?)
            """,
            -1,
            &statement,
            nil
        ) == SQLITE_OK)
        let insert = try #require(statement)
        sqlite3_bind_text(
            insert,
            1,
            "legacy-asset-signed-zero",
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_text(
            insert,
            2,
            legacyKey,
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_double(insert, 3, 10)
        sqlite3_bind_text(
            insert,
            4,
            CatalogRevocationSource.manualVeto.rawValue,
            -1,
            Self.SQLITE_TRANSIENT
        )
        #expect(sqlite3_step(insert) == SQLITE_DONE)
        sqlite3_finalize(insert)
        sqlite3_close(db)

        let reopened = try AdCatalogStore(directoryURL: dir)
        #expect(try await reopened.insert(
            showId: "show-legacy-signed-zero",
            episodePosition: .preRoll,
            durationSec: 30,
            acousticFingerprint: sampleFingerprint(seed: 183),
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "legacy-asset-signed-zero",
            sourceWindowId: "legacy-window-signed-zero",
            sourceStartTime: 0.0,
            sourceEndTime: 30
        ) == nil)
    }

    @Test("independent explicit confirmation may rehabilitate a revoked fingerprint")
    func independentExplicitConfirmationRehabilitatesFingerprint() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let fingerprint = sampleFingerprint(seed: 151)
        let original = try #require(try await store.insert(
            showId: "show-rehabilitation",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.95,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "original-source-asset",
            sourceWindowId: "original-source-window",
            sourceStartTime: 10,
            sourceEndTime: 40,
            confirmedAt: Date(timeIntervalSince1970: 100)
        ))
        #expect(try await store.revoke(
            matchedEntryId: original.id,
            sourceAssetId: "correction-receipt-asset",
            sourceWindowId: "correction-receipt-window",
            source: .manualVeto,
            showId: "show-rehabilitation",
            at: Date(timeIntervalSince1970: 200)
        ) == 1)

        #expect(try await store.insert(
            showId: "show-rehabilitation",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "delayed-explicit-asset",
            sourceWindowId: "delayed-explicit-window",
            sourceStartTime: 50,
            sourceEndTime: 80,
            confirmedAt: Date(timeIntervalSince1970: 150)
        ) == nil)
        #expect(
            await store.matches(
                fingerprint: fingerprint,
                show: "show-rehabilitation"
            ).isEmpty,
            "an explicit event older than the correction must not rehabilitate"
        )

        let rehabilitated = try #require(try await store.insert(
            showId: "show-rehabilitation",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "independent-explicit-asset",
            sourceWindowId: "independent-explicit-window",
            sourceStartTime: 50,
            sourceEndTime: 80,
            confirmedAt: Date(timeIntervalSince1970: 300)
        ))

        #expect(rehabilitated.id == original.id)
        #expect(rehabilitated.revokedAt == nil)
        #expect(rehabilitated.learningLifecycle == .explicitConfirmation)
        #expect(
            await store.matches(
                fingerprint: fingerprint,
                show: "show-rehabilitation"
            ).map(\.entry.id) == [original.id]
        )
    }

    @Test("exact source revocation is terminal despite a future confirmation timestamp")
    func exactSourceRevocationIgnoresConfirmationClockSkew() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let fingerprint = sampleFingerprint(seed: 154)
        _ = try #require(try await store.insert(
            showId: "show-exact-source-clock-skew",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "clock-skew-asset",
            sourceWindowId: "clock-skew-window",
            sourceStartTime: 10,
            sourceEndTime: 40,
            confirmedAt: Date(timeIntervalSince1970: 300)
        ))

        #expect(try await store.revoke(
            matchedEntryId: nil,
            sourceAssetId: "clock-skew-asset",
            sourceWindowId: "clock-skew-window",
            source: .manualVeto,
            at: Date(timeIntervalSince1970: 200)
        ) == 1)
        #expect(
            await store.matches(
                fingerprint: fingerprint,
                show: "show-exact-source-clock-skew"
            ).isEmpty
        )
        let audited = try #require(try await store.allEntries().first)
        #expect(audited.revokedAt == Date(timeIntervalSince1970: 200))
        #expect(audited.revocationSource == .manualVeto)
    }

    @Test("older revocation retries cannot regress the durable timestamp")
    func revocationTimestampIsMonotonic() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        let fingerprint = sampleFingerprint(seed: 153)
        let original = try #require(try await store.insert(
            showId: "timestamp-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "initial-timestamp-asset",
            sourceWindowId: "initial-timestamp-window",
            sourceStartTime: 10,
            sourceEndTime: 40,
            confirmedAt: Date(timeIntervalSince1970: 50)
        ))
        _ = try await store.revoke(
            matchedEntryId: original.id,
            sourceAssetId: "timestamp-asset",
            sourceWindowId: "timestamp-window",
            source: .manualVeto,
            showId: "timestamp-show",
            at: Date(timeIntervalSince1970: 200)
        )
        _ = try #require(try await store.insert(
            showId: "timestamp-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "later-timestamp-asset",
            sourceWindowId: "later-timestamp-window",
            sourceStartTime: 50,
            sourceEndTime: 80,
            confirmedAt: Date(timeIntervalSince1970: 300)
        ))
        _ = try await store.revoke(
            matchedEntryId: original.id,
            sourceAssetId: "timestamp-asset",
            sourceWindowId: "timestamp-window",
            source: .listenRevert,
            showId: "timestamp-show",
            at: Date(timeIntervalSince1970: 100)
        )
        #expect(
            await store.matches(
                fingerprint: fingerprint,
                show: "timestamp-show"
            ).map(\.entry.id) == [original.id],
            "an older correction retry must not undo a newer explicit confirmation"
        )
        await store.close()

        var handle: OpaquePointer?
        let path = dir.appendingPathComponent("ad_catalog.sqlite").path
        guard sqlite3_open_v2(
            path,
            &handle,
            SQLITE_OPEN_READONLY,
            nil
        ) == SQLITE_OK, let db = handle else {
            Issue.record("could not open revocation timestamp fixture")
            return
        }
        defer { sqlite3_close(db) }
        var statement: OpaquePointer?
        #expect(sqlite3_prepare_v2(
            db,
            """
            SELECT revoked_at, revocation_source
            FROM ad_catalog_revocations
            WHERE source_asset_id = 'timestamp-asset'
              AND source_window_id = 'timestamp-window'
            """,
            -1,
            &statement,
            nil
        ) == SQLITE_OK)
        if let statement {
            defer { sqlite3_finalize(statement) }
            #expect(sqlite3_step(statement) == SQLITE_ROW)
            #expect(sqlite3_column_double(statement, 0) == 200)
            let rawSource = try #require(sqlite3_column_text(statement, 1))
            #expect(String(cString: rawSource) == CatalogRevocationSource.manualVeto.rawValue)
        }
    }

    @Test("older retry that newly resolves a row uses the durable tombstone time")
    func resolvedRetryUsesDurableTombstoneTime() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let fingerprint = sampleFingerprint(seed: 159)
        let original = try #require(try await store.insert(
            showId: "resolved-retry-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "original-resolved-retry-asset",
            sourceWindowId: "original-resolved-retry-window",
            sourceStartTime: 10,
            sourceEndTime: 40,
            confirmedAt: Date(timeIntervalSince1970: 50)
        ))

        #expect(try await store.revoke(
            matchedEntryId: nil,
            sourceAssetId: "correction-resolved-retry-asset",
            sourceWindowId: "correction-resolved-retry-window",
            source: .manualVeto,
            at: Date(timeIntervalSince1970: 200)
        ) == 0)
        #expect(try await store.revoke(
            matchedEntryId: original.id,
            sourceAssetId: "correction-resolved-retry-asset",
            sourceWindowId: "correction-resolved-retry-window",
            source: .listenRevert,
            showId: "resolved-retry-show",
            at: Date(timeIntervalSince1970: 100)
        ) == 1)

        let revoked = try #require(try await store.allEntries().first)
        #expect(revoked.revokedAt == Date(timeIntervalSince1970: 200))
        #expect(revoked.revocationSource == .manualVeto)

        #expect(try await store.insert(
            showId: "resolved-retry-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "premature-rehabilitation-asset",
            sourceWindowId: "premature-rehabilitation-window",
            sourceStartTime: 50,
            sourceEndTime: 80,
            confirmedAt: Date(timeIntervalSince1970: 150)
        ) == nil)
        #expect(
            await store.matches(
                fingerprint: fingerprint,
                show: "resolved-retry-show"
            ).isEmpty
        )

        let rehabilitated = try #require(try await store.insert(
            showId: "resolved-retry-show",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "later-rehabilitation-asset",
            sourceWindowId: "later-rehabilitation-window",
            sourceStartTime: 80,
            sourceEndTime: 110,
            confirmedAt: Date(timeIntervalSince1970: 300)
        ))
        #expect(rehabilitated.id == original.id)
        #expect(rehabilitated.revokedAt == nil)
    }

    @Test("corrupted current rows with invalid numeric provenance never match")
    func invalidPersistedNumericsFailClosed() async throws {
        let dir = try makeTempDir()
        let fingerprint = sampleFingerprint(seed: 152)
        let store = try AdCatalogStore(directoryURL: dir)
        _ = try #require(try await store.insert(
            showId: "show-corrupt-numeric",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "corrupt-asset",
            sourceWindowId: "corrupt-window",
            sourceStartTime: 10,
            sourceEndTime: 40
        ))
        await store.close()

        var handle: OpaquePointer?
        let path = dir.appendingPathComponent("ad_catalog.sqlite").path
        guard sqlite3_open_v2(
            path,
            &handle,
            SQLITE_OPEN_READWRITE,
            nil
        ) == SQLITE_OK, let db = handle else {
            Issue.record("could not open corrupt numeric fixture")
            return
        }
        #expect(sqlite3_exec(
            db,
            """
            UPDATE ad_catalog_entries
            SET source_start_time = -1,
                original_confidence = 2
            WHERE show_id = 'show-corrupt-numeric';
            """,
            nil,
            nil,
            nil
        ) == SQLITE_OK)
        sqlite3_close(db)

        let reopened = try AdCatalogStore(directoryURL: dir)
        #expect(
            await reopened.matches(
                fingerprint: fingerprint,
                show: "show-corrupt-numeric"
            ).isEmpty
        )
        #expect(
            try await reopened.allEntries().count == 1,
            "corrupt evidence remains available for audit while quarantined"
        )
    }

    @Test("an unknown persisted revocation source quarantines an otherwise active row")
    func malformedPersistedRevocationSourceFailsClosed() async throws {
        let dir = try makeTempDir()
        let fingerprint = sampleFingerprint(seed: 153)
        let store = try AdCatalogStore(directoryURL: dir)
        _ = try #require(try await store.insert(
            showId: "show-corrupt-revocation",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "corrupt-revocation-asset",
            sourceWindowId: "corrupt-revocation-window",
            sourceStartTime: 10,
            sourceEndTime: 40
        ))
        await store.close()

        var handle: OpaquePointer?
        let path = dir.appendingPathComponent("ad_catalog.sqlite").path
        guard sqlite3_open_v2(
            path,
            &handle,
            SQLITE_OPEN_READWRITE,
            nil
        ) == SQLITE_OK, let db = handle else {
            Issue.record("could not open corrupt revocation fixture")
            return
        }
        #expect(sqlite3_exec(
            db,
            """
            UPDATE ad_catalog_entries
            SET revocation_source = 'unknownFutureRevocation'
            WHERE show_id = 'show-corrupt-revocation';
            """,
            nil,
            nil,
            nil
        ) == SQLITE_OK)
        sqlite3_close(db)

        let reopened = try AdCatalogStore(directoryURL: dir)
        #expect(
            await reopened.matches(
                fingerprint: fingerprint,
                show: "show-corrupt-revocation"
            ).isEmpty
        )
        #expect(
            try await reopened.allEntries().count == 1,
            "malformed evidence remains auditable while quarantined"
        )
    }

    @Test("a malformed persisted source tombstone blocks matching and final revalidation")
    func malformedPersistedSourceTombstoneFailsClosed() async throws {
        let dir = try makeTempDir()
        let fingerprint = sampleFingerprint(seed: 154)
        let initial = try AdCatalogStore(directoryURL: dir)
        let original = try #require(try await initial.insert(
            showId: "show-corrupt-source-tombstone",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "corrupt-source-tombstone-asset",
            sourceWindowId: "corrupt-source-tombstone-window",
            sourceStartTime: 10,
            sourceEndTime: 40
        ))

        // Close the store before introducing out-of-band corruption.
        await initial.close()
        var handle: OpaquePointer?
        let path = dir.appendingPathComponent("ad_catalog.sqlite").path
        guard sqlite3_open_v2(
            path,
            &handle,
            SQLITE_OPEN_READWRITE,
            nil
        ) == SQLITE_OK, let db = handle else {
            Issue.record("could not open corrupt source tombstone fixture")
            return
        }
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(
            db,
            """
            INSERT INTO ad_catalog_revocations
                (source_asset_id, source_window_id, revoked_at, revocation_source)
            VALUES (?, ?, ?, ?)
            """,
            -1,
            &statement,
            nil
        ) == SQLITE_OK, let statement else {
            sqlite3_close(db)
            Issue.record("could not prepare corrupt source tombstone fixture")
            return
        }
        sqlite3_bind_text(
            statement,
            1,
            "corrupt-source-tombstone-asset",
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_text(
            statement,
            2,
            "corrupt-source-tombstone-window",
            -1,
            Self.SQLITE_TRANSIENT
        )
        sqlite3_bind_double(statement, 3, 1_800_000_000)
        sqlite3_bind_text(
            statement,
            4,
            "unknownFutureRevocation",
            -1,
            Self.SQLITE_TRANSIENT
        )
        #expect(sqlite3_step(statement) == SQLITE_DONE)
        sqlite3_finalize(statement)
        sqlite3_close(db)

        let reopened = try AdCatalogStore(directoryURL: dir)
        #expect(
            await reopened.matchesIfAvailable(
                fingerprint: fingerprint,
                show: "show-corrupt-source-tombstone"
            ) == nil
        )
        #expect(
            await reopened.isActiveMatch(
                id: original.id,
                showId: "show-corrupt-source-tombstone",
                fingerprintVersion: fingerprint.version,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                candidateFingerprint: fingerprint
            ) == false
        )
    }

    @Test("embedded-NUL persisted provenance remains malformed after decoding")
    func persistedNULSourceIdentityFailsClosed() async throws {
        let dir = try makeTempDir()
        let fingerprint = sampleFingerprint(seed: 182)
        let initial = try AdCatalogStore(directoryURL: dir)
        let original = try #require(try await initial.insert(
            showId: "show-corrupt-nul",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 1,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "asset-corrupt-nul",
            sourceWindowId: "window-corrupt-nul",
            sourceStartTime: 10,
            sourceEndTime: 40
        ))

        await initial.close()
        var handle: OpaquePointer?
        let path = dir.appendingPathComponent("ad_catalog.sqlite").path
        #expect(
            sqlite3_open_v2(
                path,
                &handle,
                SQLITE_OPEN_READWRITE,
                nil
            ) == SQLITE_OK
        )
        let db = try #require(handle)
        var statement: OpaquePointer?
        #expect(sqlite3_prepare_v2(
            db,
            "UPDATE ad_catalog_entries SET source_asset_id = ? WHERE id = ?",
            -1,
            &statement,
            nil
        ) == SQLITE_OK)
        let update = try #require(statement)
        let malformed = "asset-corrupt-nul\u{0}other"
        _ = malformed.withCString { bytes in
            sqlite3_bind_text(
                update,
                1,
                bytes,
                Int32(malformed.utf8.count),
                Self.SQLITE_TRANSIENT
            )
        }
        sqlite3_bind_text(
            update,
            2,
            original.id.uuidString,
            -1,
            Self.SQLITE_TRANSIENT
        )
        #expect(sqlite3_step(update) == SQLITE_DONE)
        sqlite3_finalize(update)
        sqlite3_close(db)

        let reopened = try AdCatalogStore(directoryURL: dir)
        #expect(
            await reopened.matches(
                fingerprint: fingerprint,
                show: "show-corrupt-nul"
            ).isEmpty
        )
        #expect(
            await reopened.isActiveMatch(
                id: original.id,
                showId: "show-corrupt-nul",
                fingerprintVersion: fingerprint.version,
                learningSource: .userMarkedAd,
                learningLifecycle: .explicitConfirmation,
                candidateFingerprint: fingerprint
            ) == false
        )
        #expect(try await reopened.allEntries().count == 1)
    }

    @Test("delayed learning cannot rewrite a revoked row through its primary key")
    func consumedLearningCannotRewriteRevokedIdentity() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let fingerprint = sampleFingerprint(seed: 16)
        let original = try #require(
            try await store.insert(
                showId: "show-revoked-id",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: fingerprint,
                originalConfidence: 0.95,
                learningSource: .confirmedSuggestion,
                learningLifecycle: .explicitConfirmation,
                sourceAssetId: "original-asset",
                sourceWindowId: "original-window",
                sourceStartTime: 10,
                sourceEndTime: 40
            )
        )
        _ = try await store.revoke(
            matchedEntryId: original.id,
            sourceAssetId: "correction-asset",
            sourceWindowId: "correction-window",
            source: .manualVeto,
            showId: "show-revoked-id"
        )

        let delayed = CatalogEntry(
            id: original.id,
            createdAt: Date(timeIntervalSince1970: 1_800_000_000),
            showId: "show-revoked-id",
            episodePosition: .postRoll,
            durationSec: 30,
            acousticFingerprint: fingerprint,
            originalConfidence: 0.99,
            learningSource: .consumedAutoSkip,
            learningLifecycle: .consumed,
            sourceAssetId: "late-asset",
            sourceWindowId: "late-window",
            sourceStartTime: 50,
            sourceEndTime: 80,
            confirmedAt: Date(timeIntervalSince1970: 1_800_000_000)
        )
        #expect(try await store.insert(entry: delayed) == nil)

        let audited = try #require(try await store.allEntries().first)
        #expect(audited.id == original.id)
        #expect(audited.sourceAssetId == "original-asset")
        #expect(audited.sourceWindowId == "original-window")
        #expect(audited.learningSource == .confirmedSuggestion)
        #expect(audited.revocationSource == .manualVeto)
        #expect(audited.revokedAt != nil)
    }

    @Test("a veto revokes every same-show row matching the corrected span")
    func revocationRemovesAllMatchingRows() async throws {
        let dir = try makeTempDir()
        let store = try AdCatalogStore(directoryURL: dir)
        let query = sampleFingerprint(seed: 22)
        var nearbyValues = query.values
        nearbyValues[0] *= 0.95
        nearbyValues[1] *= 1.05
        let nearby = try #require(
            AcousticFingerprint(values: nearbyValues)
        )
        let exact = try #require(
            try await store.insert(
                showId: "show-multiple",
                episodePosition: .midRoll,
                durationSec: 30,
                acousticFingerprint: query,
                originalConfidence: 0.95,
                learningSource: .consumedAutoSkip,
                learningLifecycle: .consumed,
                sourceAssetId: "older-asset-1",
                sourceWindowId: "older-window-1",
                sourceStartTime: 10,
                sourceEndTime: 40
            )
        )
        _ = try await store.insert(
            showId: "show-multiple",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: nearby,
            originalConfidence: 0.94,
            learningSource: .confirmedSuggestion,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "older-asset-2",
            sourceWindowId: "older-window-2",
            sourceStartTime: 10,
            sourceEndTime: 40
        )
        #expect(
            await store.matches(
                fingerprint: query,
                show: "show-multiple"
            ).count == 2
        )

        let revoked = try await store.revoke(
            matchedEntryId: exact.id,
            sourceAssetId: "corrected-asset",
            sourceWindowId: "corrected-window",
            source: .manualVeto,
            matchingFingerprint: query,
            showId: "show-multiple"
        )

        #expect(revoked == 2)
        #expect(
            await store.matches(
                fingerprint: query,
                show: "show-multiple"
            ).isEmpty
        )
        #expect(
            try await store.allEntries().allSatisfy {
                $0.revocationSource == .manualVeto
            }
        )
    }

    @Test("private matched-row UUID revocation requires the exact canonical show")
    func matchedEntryRevocationFailsClosedAcrossShows() async throws {
        let store = try AdCatalogStore(directoryURL: makeTempDir())
        let showAFingerprint = sampleFingerprint(seed: 61)
        let showBFingerprint = orthogonalFingerprint(seed: 62)
        let showA = try #require(try await store.insert(
            showId: "show-revocation-a",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: showAFingerprint,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "show-a-asset",
            sourceWindowId: "show-a-window",
            sourceStartTime: 10,
            sourceEndTime: 40
        ))
        let showB = try #require(try await store.insert(
            showId: "show-revocation-b",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: showBFingerprint,
            originalConfidence: 0.95,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "show-b-asset",
            sourceWindowId: "show-b-window",
            sourceStartTime: 10,
            sourceEndTime: 40
        ))

        #expect(try await store.revoke(
            matchedEntryId: showB.id,
            sourceAssetId: "mismatched-correction-asset",
            sourceWindowId: "mismatched-correction-window",
            source: .manualVeto,
            showId: "show-revocation-a"
        ) == 0)
        #expect(try await store.revoke(
            matchedEntryId: showB.id,
            sourceAssetId: "missing-show-correction-asset",
            sourceWindowId: "missing-show-correction-window",
            source: .manualVeto,
            showId: nil
        ) == 0)
        #expect(try await store.revoke(
            matchedEntryId: showB.id,
            sourceAssetId: "noncanonical-show-correction-asset",
            sourceWindowId: "noncanonical-show-correction-window",
            source: .manualVeto,
            matchingFingerprint: showBFingerprint,
            showId: " show-revocation-b "
        ) == 0)

        #expect(
            await store.matches(
                fingerprint: showAFingerprint,
                show: "show-revocation-a"
            ).map(\.entry.id) == [showA.id]
        )
        #expect(
            await store.matches(
                fingerprint: showBFingerprint,
                show: "show-revocation-b"
            ).map(\.entry.id) == [showB.id]
        )

        #expect(try await store.revoke(
            matchedEntryId: showB.id,
            sourceAssetId: "exact-show-correction-asset",
            sourceWindowId: "exact-show-correction-window",
            source: .manualVeto,
            showId: "show-revocation-b"
        ) == 1)
        #expect(
            await store.matches(
                fingerprint: showBFingerprint,
                show: "show-revocation-b"
            ).isEmpty
        )
    }

    // MARK: - V1 → V3 migration

    /// Build a legacy-schema SQLite file at the canonical store location.
    /// Version 2 has the same columns as V1 plus the historical unique index.
    private func seedLegacyDatabase(
        at dir: URL,
        userVersion: Int32,
        populate: (OpaquePointer) -> Void
    ) throws {
        let dbURL = dir.appendingPathComponent("ad_catalog.sqlite")
        var handle: OpaquePointer?
        let path = dbURL.path
        guard sqlite3_open_v2(path, &handle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nil) == SQLITE_OK,
              let db = handle else {
            throw AdCatalogStoreError.openFailed("test seed: open failed")
        }
        defer { sqlite3_close(db) }

        let createSQL = """
        CREATE TABLE IF NOT EXISTS ad_catalog_entries (
            id TEXT PRIMARY KEY NOT NULL,
            created_at REAL NOT NULL,
            show_id TEXT,
            episode_position TEXT NOT NULL,
            duration_sec REAL NOT NULL,
            fingerprint_blob BLOB NOT NULL,
            transcript_snippet TEXT,
            sponsor_tokens_json TEXT,
            original_confidence REAL
        );
        CREATE INDEX IF NOT EXISTS idx_catalog_show_id ON ad_catalog_entries(show_id);
        CREATE INDEX IF NOT EXISTS idx_catalog_created_at ON ad_catalog_entries(created_at);
        \(userVersion >= 2
            ? "CREATE UNIQUE INDEX idx_catalog_show_fingerprint ON ad_catalog_entries(show_id, fingerprint_blob) WHERE show_id IS NOT NULL;"
            : "")
        PRAGMA user_version = \(userVersion);
        """
        guard sqlite3_exec(db, createSQL, nil, nil, nil) == SQLITE_OK else {
            throw AdCatalogStoreError.migrationFailed("test seed: schema exec failed")
        }
        populate(db)
    }

    private static let SQLITE_TRANSIENT = unsafeBitCast(
        -1, to: sqlite3_destructor_type.self
    )

    private func insertV1Row(
        db: OpaquePointer,
        id: UUID,
        showId: String?,
        fingerprint: AcousticFingerprint,
        confidence: Double?,
        createdAt: Date
    ) {
        let sql = """
        INSERT INTO ad_catalog_entries
            (id, created_at, show_id, episode_position, duration_sec,
             fingerprint_blob, transcript_snippet, sponsor_tokens_json,
             original_confidence)
        VALUES (?, ?, ?, 'midRoll', 30, ?, NULL, NULL, ?)
        """
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(db, sql, -1, &stmt, nil) == SQLITE_OK else { return }
        defer { sqlite3_finalize(stmt) }
        sqlite3_bind_text(stmt, 1, id.uuidString, -1, Self.SQLITE_TRANSIENT)
        sqlite3_bind_double(stmt, 2, createdAt.timeIntervalSince1970)
        if let showId {
            sqlite3_bind_text(stmt, 3, showId, -1, Self.SQLITE_TRANSIENT)
        } else {
            sqlite3_bind_null(stmt, 3)
        }
        let blob = fingerprint.data
        _ = blob.withUnsafeBytes { raw in
            sqlite3_bind_blob(stmt, 4, raw.baseAddress, Int32(blob.count), Self.SQLITE_TRANSIENT)
        }
        if let confidence {
            sqlite3_bind_double(stmt, 5, confidence)
        } else {
            sqlite3_bind_null(stmt, 5)
        }
        _ = sqlite3_step(stmt)
    }

    @Test("legacy migration preserves rows but quarantines them behind fingerprint v1")
    func migrationDedupKeepsHighestConfidence() async throws {
        let dir = try makeTempDir()
        let fp = sampleFingerprint(seed: 99)

        try seedLegacyDatabase(at: dir, userVersion: 1) { db in
            // Three duplicates of (show=A, fp). Confidences: nil, 0.4, 0.9.
            // The 0.9 row must survive; the others must be deleted.
            insertV1Row(db: db, id: UUID(), showId: "show-A", fingerprint: fp,
                        confidence: nil, createdAt: Date(timeIntervalSince1970: 1))
            insertV1Row(db: db, id: UUID(), showId: "show-A", fingerprint: fp,
                        confidence: 0.4, createdAt: Date(timeIntervalSince1970: 2))
            insertV1Row(db: db, id: UUID(), showId: "show-A", fingerprint: fp,
                        confidence: 0.9, createdAt: Date(timeIntervalSince1970: 3))
            // A separate (show=B, fp) row should pass through untouched.
            insertV1Row(db: db, id: UUID(), showId: "show-B", fingerprint: fp,
                        confidence: 0.6, createdAt: Date(timeIntervalSince1970: 4))
        }

        // Open the store — migration runs.
        let store = try AdCatalogStore(directoryURL: dir)
        let all = try await store.allEntries()

        let aRows = all.filter { $0.showId == "show-A" }
        let bRows = all.filter { $0.showId == "show-B" }
        #expect(aRows.count == 1, "Duplicates collapsed to a single row.")
        #expect(aRows.first?.originalConfidence == 0.9,
                "Highest-confidence row must survive (NULL sorts as lowest).")
        #expect(bRows.count == 1, "Distinct (show, fp) groups untouched.")
        #expect(aRows.first?.acousticFingerprint.version == .legacyCosineV1)
        #expect(aRows.first?.learningSource == .legacyUnconfirmed)
        #expect(aRows.first?.learningLifecycle == .legacyUnconfirmed)
        #expect(
            await store.matches(
                fingerprint: fp,
                show: "show-A",
                similarityFloor: 0
            ).isEmpty,
            "current v2 queries must never compare against legacy rows"
        )

        // A newly confirmed v2 fingerprint is a distinct compatibility
        // cohort even when its raw bytes happen to equal a legacy row.
        _ = try await store.insertConfirmedForTest(
            showId: "show-A",
            episodePosition: .midRoll,
            durationSec: 30,
            acousticFingerprint: fp,
            originalConfidence: 0.7
        )
        let afterReinsert = (try await store.allEntries()).filter { $0.showId == "show-A" }
        #expect(afterReinsert.count == 2)
        #expect(
            await store.matches(fingerprint: fp, show: "show-A").count == 1
        )
    }

    @Test("V1→V3 migration: NULL show_id duplicates are NOT collapsed")
    func migrationPreservesNullShowDuplicates() async throws {
        let dir = try makeTempDir()
        let fp = sampleFingerprint(seed: 77)

        try seedLegacyDatabase(at: dir, userVersion: 1) { db in
            // Two rows with NULL show_id and the same fingerprint. SQLite
            // treats NULLs in UNIQUE indexes as distinct, AND the V2 index
            // is partial (`WHERE show_id IS NOT NULL`), so NULL-show rows
            // must survive unchanged.
            insertV1Row(db: db, id: UUID(), showId: nil, fingerprint: fp,
                        confidence: 0.5, createdAt: Date(timeIntervalSince1970: 1))
            insertV1Row(db: db, id: UUID(), showId: nil, fingerprint: fp,
                        confidence: 0.7, createdAt: Date(timeIntervalSince1970: 2))
        }

        let store = try AdCatalogStore(directoryURL: dir)
        let nullShow = try await store.allEntries().filter { $0.showId == nil }
        #expect(nullShow.count == 2,
                "NULL-show duplicates must survive — partial UNIQUE index excludes them.")
    }

    @Test("real 50-row V2 catalog migrates byte-exact into quarantined V1 semantics")
    func catalystV2CatalogMigrationPreservesAndQuarantinesRows() async throws {
        let fixture = try calibrationFixture()
        let fingerprints = try fixture.legacyCatalogFingerprints.map {
            try fixtureFingerprint($0, version: .legacyCosineV1)
        }
        let dir = try makeTempDir()
        try seedLegacyDatabase(at: dir, userVersion: 2) { db in
            for (index, fingerprint) in fingerprints.enumerated() {
                insertV1Row(
                    db: db,
                    id: UUID(),
                    showId: index < 45 ? "themove" : nil,
                    fingerprint: fingerprint,
                    confidence: 0.95,
                    createdAt: Date(timeIntervalSince1970: Double(index + 1))
                )
            }
        }

        let store = try AdCatalogStore(directoryURL: dir)
        let migrated = try await store.allEntries()
        #expect(migrated.count == fixture.observations.catalogRowCount)
        #expect(
            migrated.allSatisfy {
                $0.acousticFingerprint.version == .legacyCosineV1
                    && $0.learningSource == .legacyUnconfirmed
                    && $0.learningLifecycle == .legacyUnconfirmed
            }
        )
        #expect(
            Set(migrated.map { $0.acousticFingerprint.data.base64EncodedString() })
                == Set(fixture.legacyCatalogFingerprints),
            "migration must preserve every raw fingerprint blob byte-for-byte"
        )

        let mixed = try fixtureFingerprint(
            fixture.themove.fingerprintBase64,
            version: .relativeFeatureSummaryV2
        )
        #expect(
            await store.matches(
                fingerprint: mixed,
                show: "themove",
                similarityFloor: 0
            ).isEmpty,
            "legacy rows remain auditable but never enter current admission"
        )
    }
}

private extension AdCatalogStore {
    @discardableResult
    func insertConfirmedForTest(
        showId: String?,
        episodePosition: CatalogEpisodePosition,
        durationSec: Double,
        acousticFingerprint: AcousticFingerprint,
        transcriptSnippet: String? = nil,
        sponsorTokens: [String]? = nil,
        originalConfidence: Double? = nil
    ) throws -> CatalogEntry? {
        let sourceWindowId = "test-\(UUID().uuidString)"
        return try insert(
            showId: showId,
            episodePosition: episodePosition,
            durationSec: durationSec,
            acousticFingerprint: acousticFingerprint,
            transcriptSnippet: transcriptSnippet,
            sponsorTokens: sponsorTokens,
            originalConfidence: originalConfidence,
            learningSource: .userMarkedAd,
            learningLifecycle: .explicitConfirmation,
            sourceAssetId: "test-asset",
            sourceWindowId: sourceWindowId,
            sourceStartTime: 10,
            sourceEndTime: max(10.001, 10 + durationSec)
        )
    }
}
