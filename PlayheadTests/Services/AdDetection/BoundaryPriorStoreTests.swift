// BoundaryPriorStoreTests.swift
// ef2.3.5: Tests for BoundaryPriorStore — cue-conditional prior storage
// for boundary corrections.

import XCTest
@testable import Playhead

final class BoundaryPriorStoreTests: XCTestCase {

    // MARK: - EdgeDirection serialization

    func testEdgeDirectionRawValues() {
        XCTAssertEqual(EdgeDirection.start.rawValue, "start")
        XCTAssertEqual(EdgeDirection.end.rawValue, "end")
        XCTAssertEqual(EdgeDirection(rawValue: "start"), .start)
        XCTAssertEqual(EdgeDirection(rawValue: "end"), .end)
        XCTAssertNil(EdgeDirection(rawValue: "middle"))
    }

    // MARK: - BoundaryPriorKey equality

    func testBoundaryPriorKeyEquality() {
        let a = BoundaryPriorKey(showId: "s1", edgeDirection: .start, bracketTemplate: nil)
        let b = BoundaryPriorKey(showId: "s1", edgeDirection: .start, bracketTemplate: nil)
        let c = BoundaryPriorKey(showId: "s1", edgeDirection: .end, bracketTemplate: nil)
        let d = BoundaryPriorKey(showId: "s1", edgeDirection: .start, bracketTemplate: "tmpl")
        XCTAssertEqual(a, b)
        XCTAssertNotEqual(a, c)
        XCTAssertNotEqual(a, d)
    }

    func testBoundaryPriorKeyHashable() {
        let a = BoundaryPriorKey(showId: "s1", edgeDirection: .start, bracketTemplate: nil)
        let b = BoundaryPriorKey(showId: "s1", edgeDirection: .start, bracketTemplate: nil)
        var set = Set<BoundaryPriorKey>()
        set.insert(a)
        set.insert(b)
        XCTAssertEqual(set.count, 1)
    }

    // MARK: - BoundaryPriorDistribution

    func testDistributionSnapRadiusGuidanceTight() {
        // Small spread → aggressive snap (small radius multiplier).
        let tight = BoundaryPriorDistribution(
            median: 1.5, spread: 0.3, sampleCount: 20, lastUpdatedAt: 0
        )
        let guidance = tight.snapRadiusGuidance(baseRadius: 8.0)
        // With spread 0.3, factor = clamp(0.3 / 2.0, 0.25, 1.0) = 0.25
        // guidance = 8.0 * 0.25 = 2.0
        XCTAssertEqual(guidance, 2.0, accuracy: 0.01)
    }

    func testDistributionSnapRadiusGuidanceWide() {
        // Large spread → wider search, capped at base radius.
        let wide = BoundaryPriorDistribution(
            median: 0.0, spread: 5.0, sampleCount: 3, lastUpdatedAt: 0
        )
        let guidance = wide.snapRadiusGuidance(baseRadius: 8.0)
        // factor = clamp(5.0 / 2.0, 0.25, 1.0) = 1.0
        // guidance = 8.0 * 1.0 = 8.0
        XCTAssertEqual(guidance, 8.0, accuracy: 0.01)
    }

    func testDistributionSnapRadiusGuidanceMedium() {
        let med = BoundaryPriorDistribution(
            median: -0.5, spread: 1.0, sampleCount: 10, lastUpdatedAt: 0
        )
        let guidance = med.snapRadiusGuidance(baseRadius: 8.0)
        // factor = clamp(1.0 / 2.0, 0.25, 1.0) = 0.5
        // guidance = 8.0 * 0.5 = 4.0
        XCTAssertEqual(guidance, 4.0, accuracy: 0.01)
    }

    // MARK: - Record + retrieve single prior

    func testRecordAndRetrievePrior() async throws {
        let store = try await makeBoundaryPriorStore()
        let key = BoundaryPriorKey(showId: "show-1", edgeDirection: .start, bracketTemplate: nil)

        await store.recordBoundaryCorrection(key: key, signedOffset: 2.0)

        let priorResult = await store.prior(for: key)
        let p = try XCTUnwrap(priorResult)
        XCTAssertEqual(p.median, 2.0, accuracy: 0.01)
        XCTAssertEqual(p.sampleCount, 1)
        // Single sample → spread is the initial spread constant, not 0.
        XCTAssertGreaterThan(p.spread, 0)
    }

    // MARK: - Multiple corrections update distribution

    func testMultipleCorrectionsBuildDistribution() async throws {
        let store = try await makeBoundaryPriorStore()
        let key = BoundaryPriorKey(showId: "show-2", edgeDirection: .end, bracketTemplate: nil)

        await store.recordBoundaryCorrection(key: key, signedOffset: 1.0)
        await store.recordBoundaryCorrection(key: key, signedOffset: 3.0)
        await store.recordBoundaryCorrection(key: key, signedOffset: 2.0)

        let priorResult = await store.prior(for: key)
        let p = try XCTUnwrap(priorResult)
        XCTAssertEqual(p.sampleCount, 3)
        // Mean of [1, 2, 3] = 2.0
        XCTAssertEqual(p.median, 2.0, accuracy: 0.01)
        // Sample stddev of [1, 2, 3] = 1.0
        XCTAssertEqual(p.spread, 1.0, accuracy: 0.01)
    }

    // MARK: - Welford correctness: sample stddev matches batch computation

    func testWelfordProducesCorrectSampleStddev() async throws {
        let store = try await makeBoundaryPriorStore()
        let key = BoundaryPriorKey(showId: "welford-check", edgeDirection: .start, bracketTemplate: nil)
        let samples: [Double] = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
        for s in samples {
            await store.recordBoundaryCorrection(key: key, signedOffset: s)
        }
        let priorResult = await store.prior(for: key)
        let p = try XCTUnwrap(priorResult)
        XCTAssertEqual(p.sampleCount, 8)
        // Batch mean = 5.0
        XCTAssertEqual(p.median, 5.0, accuracy: 0.01)
        // Batch sample stddev = sqrt(32/7) ~= 2.138
        let expectedStddev = sqrt(32.0 / 7.0)
        XCTAssertEqual(p.spread, expectedStddev, accuracy: 0.01)
    }

    // MARK: - Two samples: initial spread=2.0 should NOT inflate variance

    func testTwoSamplesNoPhantomVariance() async throws {
        let store = try await makeBoundaryPriorStore()
        let key = BoundaryPriorKey(showId: "phantom-check", edgeDirection: .end, bracketTemplate: nil)
        // Two identical samples → stddev should be floor (0.1), not inflated by initial spread.
        await store.recordBoundaryCorrection(key: key, signedOffset: 3.0)
        await store.recordBoundaryCorrection(key: key, signedOffset: 3.0)
        let priorResult = await store.prior(for: key)
        let p = try XCTUnwrap(priorResult)
        XCTAssertEqual(p.sampleCount, 2)
        XCTAssertEqual(p.median, 3.0, accuracy: 0.01)
        // Two identical values → sample stddev = 0, floored to 0.1
        XCTAssertEqual(p.spread, 0.1, accuracy: 0.01)
    }

    // MARK: - allPriors(forShow:)

    func testAllPriorsForShow() async throws {
        let store = try await makeBoundaryPriorStore()
        let keyStart = BoundaryPriorKey(showId: "show-3", edgeDirection: .start, bracketTemplate: nil)
        let keyEnd = BoundaryPriorKey(showId: "show-3", edgeDirection: .end, bracketTemplate: nil)
        let keyOther = BoundaryPriorKey(showId: "show-other", edgeDirection: .start, bracketTemplate: nil)

        await store.recordBoundaryCorrection(key: keyStart, signedOffset: 1.0)
        await store.recordBoundaryCorrection(key: keyEnd, signedOffset: -1.0)
        await store.recordBoundaryCorrection(key: keyOther, signedOffset: 5.0)

        let priors = await store.allPriors(forShow: "show-3")
        XCTAssertEqual(priors.count, 2)
        XCTAssertNotNil(priors[keyStart])
        XCTAssertNotNil(priors[keyEnd])
        XCTAssertNil(priors[keyOther])
    }

    // MARK: - Prior for missing key returns nil

    func testPriorForMissingKeyReturnsNil() async throws {
        let store = try await makeBoundaryPriorStore()
        let key = BoundaryPriorKey(showId: "nonexistent", edgeDirection: .start, bracketTemplate: nil)
        let prior = await store.prior(for: key)
        XCTAssertNil(prior)
    }

    // MARK: - bracketTemplate discrimination

    func testBracketTemplateDiscriminatesPriors() async throws {
        let store = try await makeBoundaryPriorStore()
        let keyNil = BoundaryPriorKey(showId: "show-bt", edgeDirection: .start, bracketTemplate: nil)
        let keyTmpl = BoundaryPriorKey(showId: "show-bt", edgeDirection: .start, bracketTemplate: "music-bed")

        await store.recordBoundaryCorrection(key: keyNil, signedOffset: 1.0)
        await store.recordBoundaryCorrection(key: keyTmpl, signedOffset: 5.0)

        let priorNilResult = await store.prior(for: keyNil)
        let priorTmplResult = await store.prior(for: keyTmpl)
        let priorNil = try XCTUnwrap(priorNilResult)
        let priorTmpl = try XCTUnwrap(priorTmplResult)
        XCTAssertEqual(priorNil.median, 1.0, accuracy: 0.01)
        XCTAssertEqual(priorTmpl.median, 5.0, accuracy: 0.01)
    }

    // MARK: - Persistence round-trip

    func testPriorSurvivesStoreRecreation() async throws {
        let dir = try makeTempDir(prefix: "BoundaryPriorStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        let key = BoundaryPriorKey(showId: "persist-show", edgeDirection: .end, bracketTemplate: nil)

        // Write with first store instance.
        do {
            AnalysisStore.resetMigratedPathsForTesting()
            let analysisStore = try AnalysisStore(directory: dir)
            try await analysisStore.migrate()
            let store = BoundaryPriorStore(store: analysisStore)
            await store.recordBoundaryCorrection(key: key, signedOffset: 3.0)
            await store.recordBoundaryCorrection(key: key, signedOffset: 5.0)
        }

        // Read with fresh store instance.
        do {
            AnalysisStore.resetMigratedPathsForTesting()
            let analysisStore = try AnalysisStore(directory: dir)
            try await analysisStore.migrate()
            let store = BoundaryPriorStore(store: analysisStore)
            let priorResult = await store.prior(for: key)
            let prior = try XCTUnwrap(priorResult)
            XCTAssertEqual(prior.sampleCount, 2)
            // Mean of [3, 5] = 4.0
            XCTAssertEqual(prior.median, 4.0, accuracy: 0.5)
        }
    }

    // MARK: - Decay: entries older than 90 days lose weight

    func testDecayWeight90Days() {
        let weight = boundaryPriorDecayWeight(ageDays: 0)
        XCTAssertEqual(weight, 1.0, accuracy: 1e-9)

        let weight45 = boundaryPriorDecayWeight(ageDays: 45)
        XCTAssertEqual(weight45, 0.5, accuracy: 1e-9)

        let weight90 = boundaryPriorDecayWeight(ageDays: 90)
        XCTAssertEqual(weight90, 0.1, accuracy: 1e-9)

        let weight180 = boundaryPriorDecayWeight(ageDays: 180)
        XCTAssertEqual(weight180, 0.1, accuracy: 1e-9)
    }

    // MARK: - Schema migration creates boundary_priors table

    func testMigrationCreatesBoundaryPriorsTable() async throws {
        let dir = try makeTempDir(prefix: "BoundaryPriorStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        XCTAssertTrue(try probeTableExists(in: dir, table: "boundary_priors"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "boundary_priors", column: "showId"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "boundary_priors", column: "edgeDirection"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "boundary_priors", column: "bracketTemplate"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "boundary_priors", column: "median"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "boundary_priors", column: "spread"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "boundary_priors", column: "sampleCount"))
        XCTAssertTrue(try probeColumnExists(in: dir, table: "boundary_priors", column: "lastUpdatedAt"))
    }

    // MARK: - Schema version reaches 9

    func testSchemaVersionReaches9() async throws {
        let dir = try makeTempDir(prefix: "BoundaryPriorStoreTests")
        defer { try? FileManager.default.removeItem(at: dir) }

        AnalysisStore.resetMigratedPathsForTesting()
        let store = try AnalysisStore(directory: dir)
        try await store.migrate()

        let version = try await store.schemaVersion()
        XCTAssertGreaterThanOrEqual(version ?? 0, 9)
    }
}

// MARK: - Test Helpers

private func makeBoundaryPriorStore() async throws -> BoundaryPriorStore {
    let analysisStore = try await makeTestStore()
    return BoundaryPriorStore(store: analysisStore)
}
