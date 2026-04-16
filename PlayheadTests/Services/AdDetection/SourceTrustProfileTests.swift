// SourceTrustProfileTests.swift
//
// Tests for BetaPosterior, SourceEvidenceFamily, OrthogonalUpdateRule,
// and SourceTrustProfile (playhead-ef2.4.4).

import Foundation
import Testing
@testable import Playhead

@Suite("BetaPosterior")
struct BetaPosteriorTests {

    @Test("mean is alpha / (alpha + beta)")
    func meanIsCorrect() {
        let posterior = BetaPosterior(alpha: 8, beta: 2)
        #expect(posterior.mean == 0.8)
    }

    @Test("variance matches Beta distribution formula")
    func varianceIsCorrect() {
        let posterior = BetaPosterior(alpha: 8, beta: 2)
        // Var = ab / ((a+b)^2 * (a+b+1))
        let expected = (8.0 * 2.0) / (100.0 * 11.0)
        #expect(abs(posterior.variance - expected) < 1e-10)
    }

    @Test("update with success increments alpha")
    func updateSuccessIncrementsAlpha() {
        let prior = BetaPosterior(alpha: 5, beta: 5)
        let updated = prior.updated(successes: 1, failures: 0)
        #expect(updated.alpha == 6)
        #expect(updated.beta == 5)
        #expect(updated.mean > prior.mean)
    }

    @Test("update with failure increments beta")
    func updateFailureIncrementsBeta() {
        let prior = BetaPosterior(alpha: 5, beta: 5)
        let updated = prior.updated(successes: 0, failures: 1)
        #expect(updated.alpha == 5)
        #expect(updated.beta == 6)
        #expect(updated.mean < prior.mean)
    }

    @Test("degenerate (0,0) posterior: mean is 0.5, variance is 0 (not NaN)")
    func degeneratePosterior() {
        let p = BetaPosterior(alpha: 0, beta: 0)
        #expect(p.mean == 0.5)
        #expect(p.variance == 0.0)
        #expect(!p.variance.isNaN)
    }

    @Test("Codable round-trip preserves BetaPosterior")
    func codableRoundTrip() throws {
        let original = BetaPosterior(alpha: 8, beta: 2)
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(BetaPosterior.self, from: data)
        #expect(decoded == original)
    }

    @Test("updated() clamps negative inputs to zero")
    func updatedClampsNegatives() {
        let prior = BetaPosterior(alpha: 5, beta: 5)
        let updated = prior.updated(successes: -3, failures: -2)
        #expect(updated.alpha == 5, "Negative successes should be clamped to 0")
        #expect(updated.beta == 5, "Negative failures should be clamped to 0")
    }

    @Test("Codable round-trip preserves UpdateTrace")
    func updateTraceCodableRoundTrip() throws {
        let trace = UpdateTrace(
            sourceToUpdate: .fm,
            corroboratingSource: .lexical,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-2",
            success: true,
            timestamp: Date(timeIntervalSince1970: 1000)
        )
        let data = try JSONEncoder().encode(trace)
        let decoded = try JSONDecoder().decode(UpdateTrace.self, from: data)
        #expect(decoded == trace)
    }

    @Test("observation count is alpha + beta")
    func observationCountIsCorrect() {
        let posterior = BetaPosterior(alpha: 17, beta: 3)
        #expect(posterior.observationCount == 20)
    }

    @Test("effectiveTrust is posteriorMean * confidence")
    func effectiveTrustDampensLowObservation() {
        // High observation count: confidence close to mean
        let high = BetaPosterior(alpha: 80, beta: 20)
        let highET = high.effectiveTrust(confidence: 0.9)
        #expect(abs(highET - 0.8 * 0.9) < 1e-10)

        // Low observation count: same mean but lower effective trust
        // because confidence factor is lower
        let low = BetaPosterior(alpha: 8, beta: 2)
        let lowET = low.effectiveTrust(confidence: 0.5)
        #expect(abs(lowET - 0.8 * 0.5) < 1e-10)
        #expect(lowET < highET)
    }
}

@Suite("SourceEvidenceFamily")
struct SourceEvidenceFamilyTests {

    @Test("lexical and classifier are textual family")
    func textualFamily() {
        #expect(SourceEvidenceFamily.for(.lexical) == .textual)
        #expect(SourceEvidenceFamily.for(.classifier) == .textual)
    }

    @Test("acoustic is acoustic family")
    func acousticFamily() {
        #expect(SourceEvidenceFamily.for(.acoustic) == .acoustic)
    }

    @Test("fm is model family")
    func modelFamily() {
        #expect(SourceEvidenceFamily.for(.fm) == .model)
    }

    @Test("fingerprint and catalog are reference family")
    func referenceFamily() {
        #expect(SourceEvidenceFamily.for(.fingerprint) == .reference)
        #expect(SourceEvidenceFamily.for(.catalog) == .reference)
    }

    @Test("every EvidenceSourceType has a family")
    func allSourceTypesHaveFamily() {
        for source in EvidenceSourceType.allCases {
            // Should not crash — all cases are covered.
            _ = SourceEvidenceFamily.for(source)
        }
    }
}

@Suite("OrthogonalUpdateRule")
struct OrthogonalUpdateRuleTests {

    @Test("cross-family corroboration is allowed")
    func crossFamilyAllowed() {
        let result = OrthogonalUpdateRule.validate(
            sourceToUpdate: .fm,
            corroboratingSource: .lexical,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-2"
        )
        #expect(result == .allowed)
    }

    @Test("same-family corroboration is blocked")
    func sameFamilyBlocked() {
        let result = OrthogonalUpdateRule.validate(
            sourceToUpdate: .lexical,
            corroboratingSource: .classifier,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-2"
        )
        #expect(result == .blockedSameFamily)
    }

    @Test("same-episode corroboration is blocked")
    func sameEpisodeBlocked() {
        let result = OrthogonalUpdateRule.validate(
            sourceToUpdate: .fm,
            corroboratingSource: .lexical,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-1"
        )
        #expect(result == .blockedSameEpisode)
    }

    @Test("same-family AND same-episode returns blockedSameFamily")
    func sameFamilySameEpisode() {
        let result = OrthogonalUpdateRule.validate(
            sourceToUpdate: .fingerprint,
            corroboratingSource: .catalog,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-1"
        )
        #expect(result == .blockedSameFamily)
    }
}

@Suite("SourceTrustProfile")
struct SourceTrustProfileTests {

    @Test("initial prior for FM matches spec: Beta(8,2) -> 0.80")
    func fmPriorMatchesSpec() {
        let profile = SourceTrustProfile()
        let fm = profile.posterior(for: .fm)
        #expect(fm.alpha == 8)
        #expect(fm.beta == 2)
        #expect(abs(fm.mean - 0.80) < 1e-10)
    }

    @Test("initial prior for lexical matches spec: Beta(17,3) -> 0.85")
    func lexicalPriorMatchesSpec() {
        let profile = SourceTrustProfile()
        let lexical = profile.posterior(for: .lexical)
        #expect(lexical.alpha == 17)
        #expect(lexical.beta == 3)
        #expect(abs(lexical.mean - 0.85) < 1e-10)
    }

    @Test("initial prior for acoustic matches spec: Beta(5,5) -> 0.50")
    func acousticPriorMatchesSpec() {
        let profile = SourceTrustProfile()
        let acoustic = profile.posterior(for: .acoustic)
        #expect(acoustic.alpha == 5)
        #expect(acoustic.beta == 5)
        #expect(abs(acoustic.mean - 0.50) < 1e-10)
    }

    @Test("initial prior for fingerprint matches spec: Beta(7,3) -> 0.70")
    func fingerprintPriorMatchesSpec() {
        let profile = SourceTrustProfile()
        let fp = profile.posterior(for: .fingerprint)
        #expect(fp.alpha == 7)
        #expect(fp.beta == 3)
        #expect(abs(fp.mean - 0.70) < 1e-10)
    }

    @Test("initial prior for catalog matches metadata spec: Beta(6,4) -> 0.60")
    func catalogPriorMatchesSpec() {
        let profile = SourceTrustProfile()
        let catalog = profile.posterior(for: .catalog)
        #expect(catalog.alpha == 6)
        #expect(catalog.beta == 4)
        #expect(abs(catalog.mean - 0.60) < 1e-10)
    }

    @Test("initial prior for classifier matches metadata spec: Beta(6,4) -> 0.60")
    func classifierPriorMatchesSpec() {
        let profile = SourceTrustProfile()
        let classifier = profile.posterior(for: .classifier)
        #expect(classifier.alpha == 6)
        #expect(classifier.beta == 4)
        #expect(abs(classifier.mean - 0.60) < 1e-10)
    }

    @Test("cross-family update increases trust")
    func crossFamilyUpdateIncreasesTrust() {
        var profile = SourceTrustProfile()
        let beforeMean = profile.posterior(for: .fm).mean

        let result = profile.recordCorroboration(
            sourceToUpdate: .fm,
            corroboratingSource: .lexical,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-2",
            success: true
        )
        #expect(result == .allowed)

        let afterMean = profile.posterior(for: .fm).mean
        #expect(afterMean > beforeMean)
    }

    @Test("same-family update is rejected and does not change posterior")
    func sameFamilyUpdateIsRejected() {
        var profile = SourceTrustProfile()
        let before = profile.posterior(for: .lexical)

        let result = profile.recordCorroboration(
            sourceToUpdate: .lexical,
            corroboratingSource: .classifier,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-2",
            success: true
        )
        #expect(result == .blockedSameFamily)

        let after = profile.posterior(for: .lexical)
        #expect(after.alpha == before.alpha)
        #expect(after.beta == before.beta)
    }

    @Test("same-episode update is rejected and does not change posterior")
    func sameEpisodeUpdateIsRejected() {
        var profile = SourceTrustProfile()
        let before = profile.posterior(for: .fm)

        let result = profile.recordCorroboration(
            sourceToUpdate: .fm,
            corroboratingSource: .acoustic,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-1",
            success: true
        )
        #expect(result == .blockedSameEpisode)

        let after = profile.posterior(for: .fm)
        #expect(after.alpha == before.alpha)
        #expect(after.beta == before.beta)
    }

    @Test("effectiveTrust = posteriorMean * confidence per spec")
    func effectiveTrustMatchesSpec() {
        let profile = SourceTrustProfile()
        // FM: Beta(8,2) = 0.80 mean; with confidence 0.9 -> 0.72
        let fmET = profile.effectiveTrust(for: .fm, confidence: 0.9)
        #expect(abs(fmET - 0.80 * 0.9) < 1e-10)

        // acoustic: Beta(5,5) = 0.50 mean; with confidence 0.5 -> 0.25
        let acousticET = profile.effectiveTrust(for: .acoustic, confidence: 0.5)
        #expect(abs(acousticET - 0.50 * 0.5) < 1e-10)

        // Same confidence -> ordering follows posterior mean.
        let fmSame = profile.effectiveTrust(for: .fm, confidence: 0.7)
        let acousticSame = profile.effectiveTrust(for: .acoustic, confidence: 0.7)
        #expect(fmSame > acousticSame)
    }

    @Test("every EvidenceSourceType has a default prior")
    func allSourceTypesHavePrior() {
        let profile = SourceTrustProfile()
        for source in EvidenceSourceType.allCases {
            let posterior = profile.posterior(for: source)
            #expect(posterior.observationCount > 0,
                    "Missing default prior for \(source)")
        }
    }

    @Test("blocked corroboration does not append trace")
    func blockedCorroborationNoTrace() {
        var profile = SourceTrustProfile()
        _ = profile.recordCorroboration(
            sourceToUpdate: .lexical,
            corroboratingSource: .classifier,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-2",
            success: true
        )
        #expect(profile.updateTraces.isEmpty, "Same-family block should not record a trace")
    }

    @Test("fusedScore has an uninformative prior: Beta(1,1) -> 0.50")
    func fusedScorePrior() {
        let profile = SourceTrustProfile()
        let fused = profile.posterior(for: .fusedScore)
        #expect(fused.alpha == 1)
        #expect(fused.beta == 1)
        #expect(abs(fused.mean - 0.50) < 1e-10)
    }

    @Test("update trace is recorded for holdout validation")
    func updateTraceIsRecorded() {
        var profile = SourceTrustProfile()
        _ = profile.recordCorroboration(
            sourceToUpdate: .fm,
            corroboratingSource: .acoustic,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-2",
            success: true
        )
        _ = profile.recordCorroboration(
            sourceToUpdate: .fm,
            corroboratingSource: .lexical,
            sourceEpisodeId: "ep-1",
            corroboratingEpisodeId: "ep-3",
            success: false
        )

        let traces = profile.updateTraces
        #expect(traces.count == 2)
        #expect(traces[0].sourceToUpdate == .fm)
        #expect(traces[0].corroboratingSource == .acoustic)
        #expect(traces[0].success == true)
        #expect(traces[1].success == false)
    }
}
