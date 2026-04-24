// MetadataActivationTests.swift
// ef2.4.7: Tests for MetadataActivationConfig, MetadataLexiconInjector,
// MetadataPriorShift, and MetadataSeededRegion (BackfillJobPhase).

import Foundation
import Testing
@testable import Playhead

// MARK: - MetadataActivationConfig

@Suite("MetadataActivationConfig — Gating")
struct MetadataActivationConfigTests {

    @Test("Default config has all consumption points disabled")
    func defaultDisabled() {
        let config = MetadataActivationConfig.default
        #expect(!config.isLexicalInjectionActive)
        #expect(!config.isClassifierPriorShiftActive)
        #expect(!config.isFMSchedulingActive)
    }

    @Test("allEnabled config has all consumption points active")
    func allEnabled() {
        let config = MetadataActivationConfig.allEnabled
        #expect(config.isLexicalInjectionActive)
        #expect(config.isClassifierPriorShiftActive)
        #expect(config.isFMSchedulingActive)
    }

    @Test("Counterfactual gate blocks all consumption points")
    func counterfactualGateBlocks() {
        let config = MetadataActivationConfig(
            lexicalInjectionEnabled: true,
            lexicalInjectionMinTrust: 0.0,
            lexicalInjectionDiscount: 0.75,
            classifierPriorShiftEnabled: true,
            classifierPriorShiftMinTrust: 0.08,
            classifierShiftedMidpoint: 0.33,
            classifierBaselineMidpoint: 0.37,
            fmSchedulingEnabled: true,
            fmSchedulingMinTrust: 0.0,
            counterfactualGateOpen: false  // gate closed
        )
        #expect(!config.isLexicalInjectionActive)
        #expect(!config.isClassifierPriorShiftActive)
        #expect(!config.isFMSchedulingActive)
    }

    @Test("Individual flags can be toggled independently")
    func independentFlags() {
        let lexicalOnly = MetadataActivationConfig(
            lexicalInjectionEnabled: true,
            lexicalInjectionMinTrust: 0.0,
            lexicalInjectionDiscount: 0.75,
            classifierPriorShiftEnabled: false,
            classifierPriorShiftMinTrust: 0.08,
            classifierShiftedMidpoint: 0.33,
            classifierBaselineMidpoint: 0.37,
            fmSchedulingEnabled: false,
            fmSchedulingMinTrust: 0.0,
            counterfactualGateOpen: true
        )
        #expect(lexicalOnly.isLexicalInjectionActive)
        #expect(!lexicalOnly.isClassifierPriorShiftActive)
        #expect(!lexicalOnly.isFMSchedulingActive)
    }

    @Test("Default discount is 0.75")
    func discountValue() {
        let config = MetadataActivationConfig.default
        #expect(config.lexicalInjectionDiscount == 0.75)
    }

    @Test("Default classifier prior shift min trust is 0.08")
    func priorShiftMinTrust() {
        let config = MetadataActivationConfig.default
        #expect(config.classifierPriorShiftMinTrust == 0.08)
    }

    @Test("Default shifted midpoint is 0.33 (playhead-gtt9.3 retune)")
    func shiftedMidpoint() {
        let config = MetadataActivationConfig.default
        #expect(config.classifierShiftedMidpoint == 0.33)
    }

    @Test("Default baseline midpoint is 0.37 (playhead-gtt9.3 retune)")
    func baselineMidpoint() {
        let config = MetadataActivationConfig.default
        #expect(config.classifierBaselineMidpoint == 0.37)
    }
}

// MARK: - MetadataLexiconInjector

@Suite("MetadataLexiconInjector — Injection")
struct MetadataLexiconInjectorTests {

    static let enabledConfig = MetadataActivationConfig.allEnabled

    @Test("Empty cues produce no entries")
    func emptyCues() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let entries = injector.inject(cues: [], metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("Disabled config produces no entries even with valid cues")
    func disabledConfig() {
        let injector = MetadataLexiconInjector(config: .default)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("External domain cue produces URL CTA entry with correct weight")
    func externalDomainWeight() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let metadataTrust: Float = 0.4
        let entries = injector.inject(cues: cues, metadataTrust: metadataTrust)
        #expect(entries.count == 1)

        let entry = entries[0]
        #expect(entry.category == .urlCTA)
        #expect(!entry.isNegativePattern)
        #expect(entry.isMetadataOrigin)
        // Weight = baseCategoryWeight(urlCTA=0.8) * metadataTrust(0.4) * 0.75
        let expectedWeight = 0.8 * 0.4 * 0.75
        #expect(abs(entry.weight - expectedWeight) < 0.001)
    }

    @Test("Sponsor alias cue produces sponsor entry with correct weight")
    func sponsorAliasWeight() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "squarespace",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let metadataTrust: Float = 0.6
        let entries = injector.inject(cues: cues, metadataTrust: metadataTrust)
        #expect(entries.count == 1)

        let entry = entries[0]
        #expect(entry.category == .sponsor)
        #expect(!entry.isNegativePattern)
        #expect(entry.isMetadataOrigin)
        // Weight = baseCategoryWeight(sponsor=1.0) * metadataTrust(0.6) * 0.75
        let expectedWeight = 1.0 * 0.6 * 0.75
        #expect(abs(entry.weight - expectedWeight) < 0.001)
    }

    @Test("Show-owned domain produces negative pattern")
    func showOwnedDomainNegative() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "teamcoco.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.count == 1)

        let entry = entries[0]
        #expect(entry.isNegativePattern)
        #expect(entry.isMetadataOrigin)
        #expect(entry.weight < 0.0, "Negative patterns should have negative weight")
    }

    @Test("Disclosure cues are skipped (covered by built-in patterns)")
    func disclosureSkipped() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .disclosure,
                normalizedValue: "brought to you by acme",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("PromoCode cues are skipped (covered by built-in patterns)")
    func promoCodeSkipped() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .promoCode,
                normalizedValue: "SAVE20",
                sourceField: .description,
                confidence: 0.9,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("NetworkOwnedDomain cues are skipped")
    func networkOwnedSkipped() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .networkOwnedDomain,
                normalizedValue: "earwolf.com",
                sourceField: .description,
                confidence: 0.9,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.isEmpty)
    }

    @Test("All metadata entries have isMetadataOrigin set to true")
    func metadataOriginFlag() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "squarespace",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        for entry in entries {
            #expect(entry.isMetadataOrigin, "All metadata entries must have isMetadataOrigin = true")
        }
    }

    @Test("Trust below minimum produces no entries")
    func trustBelowMinimum() {
        let config = MetadataActivationConfig(
            lexicalInjectionEnabled: true,
            lexicalInjectionMinTrust: 0.3,
            lexicalInjectionDiscount: 0.75,
            classifierPriorShiftEnabled: false,
            classifierPriorShiftMinTrust: 0.08,
            classifierShiftedMidpoint: 0.33,
            classifierBaselineMidpoint: 0.37,
            fmSchedulingEnabled: false,
            fmSchedulingMinTrust: 0.0,
            counterfactualGateOpen: true
        )
        let injector = MetadataLexiconInjector(config: config)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.2)
        #expect(entries.isEmpty)
    }

    @Test("Domain entry pattern matches spoken form in transcript text")
    func domainPatternMatches() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.count == 1)

        let pattern = entries[0].pattern
        let text = "go to betterhelp com for a free trial" as NSString
        let range = NSRange(location: 0, length: text.length)
        let matches = pattern.matches(in: text as String, range: range)
        #expect(matches.count == 1, "Pattern should match spoken domain form")
    }

    @Test("Multiple cue types produce correct entry mix")
    func multipleCueTypes() {
        let injector = MetadataLexiconInjector(config: Self.enabledConfig)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "squarespace",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "teamcoco.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.count == 3)

        let positives = entries.filter { !$0.isNegativePattern }
        let negatives = entries.filter { $0.isNegativePattern }
        #expect(positives.count == 2)
        #expect(negatives.count == 1)
    }
}

// MARK: - MetadataLexiconInjector — 2-Hit Rule

@Suite("MetadataLexiconInjector — 2-Hit Rule Enforcement")
struct MetadataLexiconTwoHitRuleTests {

    @Test("Metadata-only hit group is not promoted to candidate")
    func metadataOnlyGroupNotPromoted() {
        // Simulate the 2-hit rule: a group where ALL hits have isMetadataOrigin
        // should not be promoted. This is enforced by the flag, not by the injector
        // itself, but we verify the flag is set correctly.
        let injector = MetadataLexiconInjector(config: .allEnabled)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "betterhelp.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "betterhelp",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)

        // All entries must be marked as metadata origin
        let allMetadata = entries.allSatisfy { $0.isMetadataOrigin }
        #expect(allMetadata, "All injected entries must have isMetadataOrigin=true for 2-hit rule enforcement")
    }
}

// MARK: - MetadataPriorShift

@Suite("MetadataPriorShift — Sigmoid Midpoint")
struct MetadataPriorShiftTests {

    @Test("Baseline midpoint returned when gate is closed")
    func gateClosed() {
        let shift = MetadataPriorShift(config: .default)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.5)
        #expect(mid == 0.37)
    }

    @Test("Baseline midpoint returned when trust is below threshold")
    func trustBelowThreshold() {
        let shift = MetadataPriorShift(config: .allEnabled)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.07)
        #expect(mid == 0.37, "Trust 0.07 < 0.08 threshold should return baseline")
    }

    @Test("Shifted midpoint returned when trust meets threshold")
    func trustMeetsThreshold() {
        let shift = MetadataPriorShift(config: .allEnabled)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.08)
        #expect(mid == 0.33, "Trust 0.08 >= 0.08 threshold should return shifted midpoint")
    }

    @Test("Shifted midpoint returned when trust exceeds threshold")
    func trustExceedsThreshold() {
        let shift = MetadataPriorShift(config: .allEnabled)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.5)
        #expect(mid == 0.33)
    }

    @Test("isShiftActive returns false when gate is closed")
    func shiftActiveGateClosed() {
        let shift = MetadataPriorShift(config: .default)
        #expect(!shift.isShiftActive(metadataTrust: 0.5))
    }

    @Test("isShiftActive returns false when trust below threshold")
    func shiftActiveTrustBelow() {
        let shift = MetadataPriorShift(config: .allEnabled)
        #expect(!shift.isShiftActive(metadataTrust: 0.07))
    }

    @Test("isShiftActive returns true when trust meets threshold")
    func shiftActiveTrustMeets() {
        let shift = MetadataPriorShift(config: .allEnabled)
        #expect(shift.isShiftActive(metadataTrust: 0.08))
    }

    @Test("Shifted midpoint is strictly less than baseline")
    func shiftedLessThanBaseline() {
        let config = MetadataActivationConfig.allEnabled
        #expect(config.classifierShiftedMidpoint < config.classifierBaselineMidpoint)
    }

    @Test("Prior shift with zero trust returns baseline")
    func zeroTrust() {
        let shift = MetadataPriorShift(config: .allEnabled)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.0)
        #expect(mid == 0.37, "Zero trust should return baseline midpoint")
    }

    @Test("Classifier prior shift only with gate open and individual flag on")
    func classifierPriorShiftGating() {
        // Gate open but individual flag off
        let noShift = MetadataActivationConfig(
            lexicalInjectionEnabled: true,
            lexicalInjectionMinTrust: 0.0,
            lexicalInjectionDiscount: 0.75,
            classifierPriorShiftEnabled: false,
            classifierPriorShiftMinTrust: 0.08,
            classifierShiftedMidpoint: 0.33,
            classifierBaselineMidpoint: 0.37,
            fmSchedulingEnabled: true,
            fmSchedulingMinTrust: 0.0,
            counterfactualGateOpen: true
        )
        let shift = MetadataPriorShift(config: noShift)
        let mid = shift.effectiveMidpoint(metadataTrust: 0.5)
        #expect(mid == 0.37, "Individual flag disabled should return baseline")
    }
}

// MARK: - MetadataPriorShift real-data band (playhead-gtt9.3)

/// playhead-gtt9.3: the default midpoint band must overlap the real-data
/// confidence distribution measured on 2026-04-23. Histogram mode was
/// [0.30, 0.40) (53% of 147 scored windows); the old 0.22 / 0.25 band
/// contained zero windows, making PriorShift inert. The new default
/// band `(0.33, 0.37]` sits mid-mode and captures ~30 real windows.
///
/// These tests lock in that behavior at the defaults level so a future
/// rebase does not silently revert the retune.
@Suite("MetadataPriorShift — real-data band (gtt9.3)")
struct MetadataPriorShiftRealDataBandTests {

    /// A confidence in the mode of the real-data histogram. Chosen as the
    /// center of the new band so a single value exercises both sides of the
    /// half-open interval `(shifted, baseline]`.
    private static let realDataBandCenter: Double = 0.35

    @Test("Band center (0.35) sits in (shifted, baseline] under defaults — counterfactual flips decision")
    func bandCenterFlipsUnderCounterfactual() {
        // For a window whose fused classifier confidence is the band
        // center, baseline classification says "not ad" (confidence ≤
        // baseline midpoint) while shifted classification says "ad"
        // (confidence > shifted midpoint). That is exactly the flip the
        // priorShift counterfactual is supposed to produce.
        let baseline = MetadataActivationConfig.default.classifierBaselineMidpoint
        let shifted = MetadataActivationConfig.default.classifierShiftedMidpoint
        let c = Self.realDataBandCenter

        #expect(c > shifted,
                "Band-center confidence must exceed shifted midpoint; otherwise priorShift cannot flip it.")
        #expect(c <= baseline,
                "Band-center confidence must sit at or below baseline midpoint; otherwise the window was already an ad.")
    }

    @Test("Band invariants: baseline > shifted, both inside (0, 1)")
    func bandInvariants() {
        let config = MetadataActivationConfig.default
        #expect(config.classifierBaselineMidpoint > config.classifierShiftedMidpoint,
                "Baseline must exceed shifted; otherwise the band inverts and priorShift becomes a no-op.")
        #expect(config.classifierShiftedMidpoint > 0.0)
        #expect(config.classifierBaselineMidpoint < 1.0)
    }

    @Test("Midpoints stay inside the candidate/confirmation envelope")
    func midpointsInsideDetectionEnvelope() {
        // The classifier midpoints are sigmoid-level thresholds; they must
        // sit strictly below AdDetectionConfig.candidateThreshold so
        // priorShift-flipped windows can still be filtered by the candidate
        // stage without the midpoint itself being above the emit threshold,
        // and strictly above suppressionThreshold so no flipped window
        // triggers suppression. Regression guard against future retunes
        // that cross either rail.
        let config = MetadataActivationConfig.default
        let detection = AdDetectionConfig.default
        #expect(config.classifierBaselineMidpoint <= detection.candidateThreshold,
                "Baseline midpoint must not exceed candidateThreshold — flipped windows would be pre-filtered.")
        #expect(config.classifierShiftedMidpoint >= detection.suppressionThreshold,
                "Shifted midpoint must stay at or above suppressionThreshold — flipped windows would be suppressed.")
        #expect(config.classifierBaselineMidpoint < detection.confirmationThreshold,
                "Baseline midpoint must sit well below confirmationThreshold.")
    }
}

// MARK: - MetadataSeededRegion (BackfillJobPhase)

@Suite("MetadataSeededRegion — FM Scheduling Phase")
struct MetadataSeededRegionTests {

    @Test("metadataSeededRegion is a valid BackfillJobPhase case")
    func phaseExists() {
        let phase = BackfillJobPhase.metadataSeededRegion
        #expect(phase.rawValue == "metadataSeededRegion")
    }

    @Test("metadataSeededRegion round-trips via Codable")
    func codableRoundTrip() throws {
        let phase = BackfillJobPhase.metadataSeededRegion
        let encoder = JSONEncoder()
        let data = try encoder.encode(phase)
        let decoder = JSONDecoder()
        let decoded = try decoder.decode(BackfillJobPhase.self, from: data)
        #expect(decoded == phase)
    }

    @Test("metadataSeededRegion is included in allCases")
    func inAllCases() {
        #expect(BackfillJobPhase.allCases.contains(.metadataSeededRegion))
    }

    @Test("BackfillJob can be created with metadataSeededRegion phase")
    func backfillJobWithPhase() {
        let job = BackfillJob(
            jobId: "test-job",
            analysisAssetId: "asset-1",
            podcastId: "podcast-1",
            phase: .metadataSeededRegion,
            coveragePolicy: .targetedWithAudit,
            priority: 5,
            progressCursor: nil,
            retryCount: 0,
            deferReason: nil,
            status: .queued,
            scanCohortJSON: nil,
            createdAt: 1000.0
        )
        #expect(job.phase == .metadataSeededRegion)
    }
}

// MARK: - LexicalScannerCategoryWeights

@Suite("LexicalScannerCategoryWeights — Consistency")
struct LexicalScannerCategoryWeightsTests {

    @Test("Weights match LexicalScanner.categoryWeight for all categories")
    func weightsMatchScanner() {
        // These weights must match the private categoryWeight in LexicalScanner.
        // If they drift, metadata injection weights will be wrong.
        #expect(LexicalScannerCategoryWeights.weight(for: .sponsor) == 1.0)
        #expect(LexicalScannerCategoryWeights.weight(for: .promoCode) == 1.2)
        #expect(LexicalScannerCategoryWeights.weight(for: .urlCTA) == 0.8)
        #expect(LexicalScannerCategoryWeights.weight(for: .purchaseLanguage) == 0.9)
        #expect(LexicalScannerCategoryWeights.weight(for: .transitionMarker) == 0.3)
    }
}

// MARK: - Weight Formula Verification

@Suite("Weight Formula — baseCategoryWeight x metadataTrust x 0.75")
struct WeightFormulaTests {

    @Test("Weight formula: sponsor at trust 1.0")
    func sponsorFullTrust() {
        let injector = MetadataLexiconInjector(config: .allEnabled)
        let cues = [
            EpisodeMetadataCue(
                cueType: .sponsorAlias,
                normalizedValue: "acme",
                sourceField: .description,
                confidence: 0.85,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 1.0)
        #expect(entries.count == 1)
        // 1.0 * 1.0 * 0.75 = 0.75
        #expect(abs(entries[0].weight - 0.75) < 0.001)
    }

    @Test("Weight formula: external domain at trust 0.2")
    func externalDomainLowTrust() {
        let injector = MetadataLexiconInjector(config: .allEnabled)
        let cues = [
            EpisodeMetadataCue(
                cueType: .externalDomain,
                normalizedValue: "example.com",
                sourceField: .description,
                confidence: 0.8,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.2)
        #expect(entries.count == 1)
        // 0.8 * 0.2 * 0.75 = 0.12
        #expect(abs(entries[0].weight - 0.12) < 0.001)
    }

    @Test("Weight formula: show-owned domain produces negative weight")
    func showOwnedNegativeWeight() {
        let injector = MetadataLexiconInjector(config: .allEnabled)
        let cues = [
            EpisodeMetadataCue(
                cueType: .showOwnedDomain,
                normalizedValue: "mypodcast.com",
                sourceField: .description,
                confidence: 0.95,
                canonicalSponsorId: nil,
                canonicalOwnerId: nil
            ),
        ]
        let entries = injector.inject(cues: cues, metadataTrust: 0.5)
        #expect(entries.count == 1)
        // -(0.8 * 0.5 * 0.75) = -0.30
        let expectedWeight = -(0.8 * 0.5 * 0.75)
        #expect(abs(entries[0].weight - expectedWeight) < 0.001)
    }
}
