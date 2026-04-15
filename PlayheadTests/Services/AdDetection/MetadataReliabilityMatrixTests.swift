// MetadataReliabilityMatrixTests.swift
// ef2.2.3: Tests for MetadataReliabilityMatrix — per-cue Bayesian trust
// with orthogonal update rule, recency weighting, and persistence.

import Foundation
import Testing
@testable import Playhead

// MARK: - BetaDistribution Math

@Suite("BetaDistribution — Math")
struct BetaDistributionMathTests {

    @Test("Mean of Beta(2,8) is 0.20")
    func meanBeta2_8() {
        let dist = BetaDistribution(alpha: 2, beta: 8)
        #expect(abs(dist.mean - 0.20) < 0.001)
    }

    @Test("Mean of Beta(1,9) is 0.10")
    func meanBeta1_9() {
        let dist = BetaDistribution(alpha: 1, beta: 9)
        #expect(abs(dist.mean - 0.10) < 0.001)
    }

    @Test("Mean of Beta(1,4) is 0.20")
    func meanBeta1_4() {
        let dist = BetaDistribution(alpha: 1, beta: 4)
        #expect(abs(dist.mean - 0.20) < 0.001)
    }

    @Test("Mean of Beta(1,1) is 0.50 (uniform)")
    func meanUniform() {
        let dist = BetaDistribution(alpha: 1, beta: 1)
        #expect(abs(dist.mean - 0.50) < 0.001)
    }

    @Test("Variance of Beta(2,8) matches formula")
    func varianceBeta2_8() {
        let dist = BetaDistribution(alpha: 2, beta: 8)
        // Var = αβ / ((α+β)²(α+β+1)) = 16 / (100 * 11) = 0.01454...
        let expected: Float = (2.0 * 8.0) / (10.0 * 10.0 * 11.0)
        #expect(abs(dist.variance - expected) < 0.0001)
    }

    @Test("Update with success increments alpha")
    func updateSuccess() {
        let dist = BetaDistribution(alpha: 2, beta: 8)
        let updated = dist.updated(success: true)
        #expect(updated.alpha == 3)
        #expect(updated.beta == 8)
    }

    @Test("Update with failure increments beta")
    func updateFailure() {
        let dist = BetaDistribution(alpha: 2, beta: 8)
        let updated = dist.updated(success: false)
        #expect(updated.alpha == 2)
        #expect(updated.beta == 9)
    }

    @Test("Weighted update applies fractional increment")
    func weightedUpdate() {
        let dist = BetaDistribution(alpha: 2, beta: 8)
        let updated = dist.updated(success: true, weight: 0.5)
        #expect(abs(updated.alpha - 2.5) < 0.001)
        #expect(updated.beta == 8)
    }

    @Test("BetaDistribution is Equatable")
    func equatable() {
        let a = BetaDistribution(alpha: 2, beta: 8)
        let b = BetaDistribution(alpha: 2, beta: 8)
        #expect(a == b)
    }
}

// MARK: - Prior Initialization

@Suite("MetadataReliabilityMatrix — Prior Initialization")
struct PriorInitializationTests {

    @Test("externalDomain prior is Beta(2,8) → 0.20")
    func externalDomainPrior() async {
        let matrix = MetadataReliabilityMatrix()
        let trust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(abs(trust - 0.20) < 0.001)
    }

    @Test("promoCode prior is Beta(2,8) → 0.20")
    func promoCodePrior() async {
        let matrix = MetadataReliabilityMatrix()
        let trust = await matrix.trust(showId: "show1", for: .promoCode, sourceField: .description)
        #expect(abs(trust - 0.20) < 0.001)
    }

    @Test("disclosure prior is Beta(1,9) → 0.10")
    func disclosurePrior() async {
        let matrix = MetadataReliabilityMatrix()
        let trust = await matrix.trust(showId: "show1", for: .disclosure, sourceField: .description)
        #expect(abs(trust - 0.10) < 0.001)
    }

    @Test("sponsorAlias prior is Beta(1,9) → 0.10")
    func sponsorAliasPrior() async {
        let matrix = MetadataReliabilityMatrix()
        let trust = await matrix.trust(showId: "show1", for: .sponsorAlias, sourceField: .summary)
        #expect(abs(trust - 0.10) < 0.001)
    }

    @Test("showOwnedDomain prior is Beta(1,4) → 0.20")
    func showOwnedDomainPrior() async {
        let matrix = MetadataReliabilityMatrix()
        let trust = await matrix.trust(showId: "show1", for: .showOwnedDomain, sourceField: .description)
        #expect(abs(trust - 0.20) < 0.001)
    }

    @Test("networkOwnedDomain prior is Beta(1,4) → 0.20")
    func networkOwnedDomainPrior() async {
        let matrix = MetadataReliabilityMatrix()
        let trust = await matrix.trust(showId: "show1", for: .networkOwnedDomain, sourceField: .summary)
        #expect(abs(trust - 0.20) < 0.001)
    }

    @Test("Different shows have independent matrices")
    func independentShows() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,
            normalizedValue: "squarespace.com",
            sourceField: .description,
            confidence: 0.9,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )
        await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .textual
        )

        let trust1 = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        let trust2 = await matrix.trust(showId: "show2", for: .externalDomain, sourceField: .description)

        // show1 should have higher trust after positive observation
        #expect(trust1 > trust2)
        // show2 should still be at default prior
        #expect(abs(trust2 - 0.20) < 0.001)
    }
}

// MARK: - Observation Updates

@Suite("MetadataReliabilityMatrix — Observation Updates")
struct ObservationUpdateTests {

    @Test("Positive observation increases trust")
    func positiveObservation() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,
            normalizedValue: "betterhelp.com",
            sourceField: .description,
            confidence: 0.8,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )
        let priorTrust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)

        await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .textual
        )

        let posteriorTrust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(posteriorTrust > priorTrust)
    }

    @Test("Negative observation decreases trust")
    func negativeObservation() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,
            normalizedValue: "example.com",
            sourceField: .description,
            confidence: 0.8,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )
        let priorTrust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)

        await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: false,
            date: Date(), corroboratingFamily: .textual
        )

        let posteriorTrust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(posteriorTrust < priorTrust)
    }

    @Test("Multiple observations accumulate correctly")
    func multipleObservations() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .disclosure,
            normalizedValue: "sponsored by acme",
            sourceField: .summary,
            confidence: 0.95,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        // 5 positive observations from domain family
        for _ in 0..<5 {
            await matrix.observe(
                showId: "show1", cue: cue, wasCorrect: true,
                date: Date(), corroboratingFamily: .domain
            )
        }

        let trust = await matrix.trust(showId: "show1", for: .disclosure, sourceField: .summary)
        // Prior Beta(1,9) + 5 successes = Beta(6,9), mean = 6/15 = 0.40
        #expect(abs(trust - 0.40) < 0.001)
    }
}

// MARK: - Orthogonal Update Rule

@Suite("MetadataReliabilityMatrix — Orthogonal Update Rule")
struct OrthogonalUpdateTests {

    @Test("Rejects same-family corroboration for domain cue")
    func rejectsSameFamilyDomain() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,  // domain family
            normalizedValue: "squarespace.com",
            sourceField: .description,
            confidence: 0.9,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        let applied = await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .domain  // same family!
        )

        #expect(applied == false)

        // Trust should remain at prior
        let trust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(abs(trust - 0.20) < 0.001)
    }

    @Test("Rejects same-family corroboration for textual cue")
    func rejectsSameFamilyTextual() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .disclosure,  // textual family
            normalizedValue: "sponsored by",
            sourceField: .description,
            confidence: 0.9,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        let applied = await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .textual  // same family!
        )

        #expect(applied == false)
    }

    @Test("Accepts cross-family corroboration: textual cue + domain evidence")
    func acceptsCrossFamilyTextualDomain() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .disclosure,  // textual family
            normalizedValue: "sponsored by acme",
            sourceField: .description,
            confidence: 0.95,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        let applied = await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .domain  // different family
        )

        #expect(applied == true)

        let trust = await matrix.trust(showId: "show1", for: .disclosure, sourceField: .description)
        #expect(trust > 0.10)  // higher than prior
    }

    @Test("Accepts cross-family corroboration: domain cue + textual evidence")
    func acceptsCrossFamilyDomainTextual() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,  // domain family
            normalizedValue: "betterhelp.com",
            sourceField: .description,
            confidence: 0.8,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        let applied = await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .textual  // different family
        )

        #expect(applied == true)
    }

    @Test("Evidence family mapping covers all cue types")
    func evidenceFamilyMapping() {
        // Domain family
        #expect(MetadataCueType.externalDomain.evidenceFamily == .domain)
        #expect(MetadataCueType.showOwnedDomain.evidenceFamily == .domain)
        #expect(MetadataCueType.networkOwnedDomain.evidenceFamily == .domain)

        // Textual family
        #expect(MetadataCueType.disclosure.evidenceFamily == .textual)
        #expect(MetadataCueType.promoCode.evidenceFamily == .textual)
        #expect(MetadataCueType.sponsorAlias.evidenceFamily == .textual)
    }
}

// MARK: - Recency Weighting

@Suite("MetadataReliabilityMatrix — Recency Weighting")
struct RecencyWeightingTests {

    @Test("Recent observation (today) gets full weight")
    func recentObservationFullWeight() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,
            normalizedValue: "squarespace.com",
            sourceField: .description,
            confidence: 0.9,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .textual
        )

        // Prior Beta(2,8) + 1.0 success = Beta(3,8), mean = 3/11 ≈ 0.2727
        let trust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(abs(trust - 3.0 / 11.0) < 0.001)
    }

    @Test("Old observation (>90 days) gets 0.5× weight")
    func oldObservationHalfWeight() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,
            normalizedValue: "squarespace.com",
            sourceField: .description,
            confidence: 0.9,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        let oldDate = Calendar.current.date(byAdding: .day, value: -100, to: Date())!

        await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: oldDate, corroboratingFamily: .textual
        )

        // Prior Beta(2,8) + 0.5 success = Beta(2.5,8), mean = 2.5/10.5 ≈ 0.2381
        let trust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(abs(trust - 2.5 / 10.5) < 0.001)
    }

    @Test("Observation at exactly 90 days gets full weight")
    func boundaryObservationFullWeight() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,
            normalizedValue: "squarespace.com",
            sourceField: .description,
            confidence: 0.9,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        let boundaryDate = Calendar.current.date(byAdding: .day, value: -90, to: Date())!

        await matrix.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: boundaryDate, corroboratingFamily: .textual
        )

        // Should get full weight: Beta(2,8) + 1.0 = Beta(3,8)
        let trust = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(abs(trust - 3.0 / 11.0) < 0.001)
    }

    @Test("Old vs recent observations produce different trust levels")
    func oldVsRecentDifference() async {
        let matrixRecent = MetadataReliabilityMatrix()
        let matrixOld = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .disclosure,
            normalizedValue: "sponsored by",
            sourceField: .description,
            confidence: 0.9,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        await matrixRecent.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .domain
        )

        let oldDate = Calendar.current.date(byAdding: .day, value: -180, to: Date())!
        await matrixOld.observe(
            showId: "show1", cue: cue, wasCorrect: true,
            date: oldDate, corroboratingFamily: .domain
        )

        let trustRecent = await matrixRecent.trust(showId: "show1", for: .disclosure, sourceField: .description)
        let trustOld = await matrixOld.trust(showId: "show1", for: .disclosure, sourceField: .description)

        // Recent observation should produce higher trust than old one
        #expect(trustRecent > trustOld)
    }
}

// MARK: - Persistence Roundtrip

@Suite("MetadataReliabilityMatrix — Persistence")
struct PersistenceTests {

    @Test("JSON encode/decode roundtrip preserves all cells")
    func jsonRoundtrip() async throws {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,
            normalizedValue: "betterhelp.com",
            sourceField: .description,
            confidence: 0.8,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        await matrix.observe(
            showId: "podcast-123", cue: cue, wasCorrect: true,
            date: Date(), corroboratingFamily: .textual
        )

        // Encode
        let data = try await matrix.encodeToJSON()

        // Decode
        let restored = try MetadataReliabilityMatrix.decodeFromJSON(data)

        // Verify trust matches
        let originalTrust = await matrix.trust(
            showId: "podcast-123", for: .externalDomain, sourceField: .description
        )
        let restoredTrust = await restored.trust(
            showId: "podcast-123", for: .externalDomain, sourceField: .description
        )
        #expect(abs(originalTrust - restoredTrust) < 0.0001)
    }

    @Test("Roundtrip preserves multiple shows")
    func multipleShowsRoundtrip() async throws {
        let matrix = MetadataReliabilityMatrix()
        let cue1 = EpisodeMetadataCue(
            cueType: .disclosure,
            normalizedValue: "sponsored by",
            sourceField: .description,
            confidence: 0.95,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )
        let cue2 = EpisodeMetadataCue(
            cueType: .promoCode,
            normalizedValue: "PODCAST20",
            sourceField: .summary,
            confidence: 0.9,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        await matrix.observe(showId: "show-A", cue: cue1, wasCorrect: true,
                             date: Date(), corroboratingFamily: .domain)
        await matrix.observe(showId: "show-B", cue: cue2, wasCorrect: false,
                             date: Date(), corroboratingFamily: .domain)

        let data = try await matrix.encodeToJSON()
        let restored = try MetadataReliabilityMatrix.decodeFromJSON(data)

        let trustA = await restored.trust(showId: "show-A", for: .disclosure, sourceField: .description)
        let trustB = await restored.trust(showId: "show-B", for: .promoCode, sourceField: .summary)

        // show-A disclosure got a positive observation
        #expect(trustA > 0.10)
        // show-B promoCode got a negative observation
        #expect(trustB < 0.20)
    }

    @Test("ShowReliabilityMatrix is Codable")
    func showMatrixCodable() throws {
        let matrix = ShowReliabilityMatrix()
        let data = try JSONEncoder().encode(matrix)
        let decoded = try JSONDecoder().decode(ShowReliabilityMatrix.self, from: data)
        #expect(matrix == decoded)
    }

    @Test("BetaDistribution is Codable")
    func betaDistributionCodable() throws {
        let dist = BetaDistribution(alpha: 3.5, beta: 7.5)
        let data = try JSONEncoder().encode(dist)
        let decoded = try JSONDecoder().decode(BetaDistribution.self, from: data)
        #expect(decoded.alpha == 3.5)
        #expect(decoded.beta == 7.5)
    }
}

// MARK: - Reset

@Suite("MetadataReliabilityMatrix — Reset")
struct ResetTests {

    @Test("Reset clears show matrix to default priors")
    func resetClearsToDefaults() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .externalDomain,
            normalizedValue: "betterhelp.com",
            sourceField: .description,
            confidence: 0.8,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        // Accumulate some observations
        for _ in 0..<10 {
            await matrix.observe(
                showId: "show1", cue: cue, wasCorrect: true,
                date: Date(), corroboratingFamily: .textual
            )
        }

        let trustBeforeReset = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(trustBeforeReset > 0.20)  // should be well above prior

        // Reset
        await matrix.reset(showId: "show1")

        let trustAfterReset = await matrix.trust(showId: "show1", for: .externalDomain, sourceField: .description)
        #expect(abs(trustAfterReset - 0.20) < 0.001)  // back to prior
    }

    @Test("Reset one show does not affect another")
    func resetIsolated() async {
        let matrix = MetadataReliabilityMatrix()
        let cue = EpisodeMetadataCue(
            cueType: .disclosure,
            normalizedValue: "sponsored by",
            sourceField: .description,
            confidence: 0.95,
            canonicalSponsorId: nil,
            canonicalOwnerId: nil
        )

        await matrix.observe(showId: "show1", cue: cue, wasCorrect: true,
                             date: Date(), corroboratingFamily: .domain)
        await matrix.observe(showId: "show2", cue: cue, wasCorrect: true,
                             date: Date(), corroboratingFamily: .domain)

        await matrix.reset(showId: "show1")

        let trust1 = await matrix.trust(showId: "show1", for: .disclosure, sourceField: .description)
        let trust2 = await matrix.trust(showId: "show2", for: .disclosure, sourceField: .description)

        #expect(abs(trust1 - 0.10) < 0.001)  // reset to prior
        #expect(trust2 > 0.10)  // still updated
    }
}
