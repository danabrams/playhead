// DecisionExplanationTests.swift
// playhead-ef2.1.4: Explanation trace tests for DecisionExplanation.
//
// Covers:
//   - Codable round-trip (JSON encode/decode)
//   - Structure validation (per-source breakdown, authority, rationale)
//   - Builder from ledger + decision
//   - Persistence round-trip via DecisionEvent.explanationJSON

import Foundation
import XCTest
@testable import Playhead

// MARK: - DecisionExplanation Codable Round-Trip

final class DecisionExplanationCodableTests: XCTestCase {

    func testEmptyRoundTrip() throws {
        let explanation = DecisionExplanation(
            evidenceBreakdown: [],
            contributingFamilies: [],
            actionRationale: ActionRationale(
                threshold: 0.65,
                gate: "eligible",
                policyAction: "autoSkipEligible",
                skipEligible: true
            )
        )
        let data = try JSONEncoder().encode(explanation)
        let decoded = try JSONDecoder().decode(DecisionExplanation.self, from: data)
        XCTAssertEqual(decoded, explanation)
    }

    func testFullRoundTrip() throws {
        let explanation = DecisionExplanation(
            evidenceBreakdown: [
                SourceEvidence(
                    source: "classifier",
                    weight: 0.24,
                    capApplied: 0.30,
                    authority: .strong
                ),
                SourceEvidence(
                    source: "fm",
                    weight: 0.35,
                    capApplied: 0.40,
                    authority: .strong
                ),
                SourceEvidence(
                    source: "lexical",
                    weight: 0.12,
                    capApplied: 0.20,
                    authority: .weak
                ),
            ],
            contributingFamilies: ["classifier", "fm", "lexical"],
            actionRationale: ActionRationale(
                threshold: 0.65,
                gate: "eligible",
                policyAction: "autoSkipEligible",
                skipEligible: true
            )
        )
        let data = try JSONEncoder().encode(explanation)
        let decoded = try JSONDecoder().decode(DecisionExplanation.self, from: data)
        XCTAssertEqual(decoded, explanation)
        XCTAssertEqual(decoded.evidenceBreakdown.count, 3)
        XCTAssertEqual(decoded.contributingFamilies, ["classifier", "fm", "lexical"])
    }

    func testCompactJSON() throws {
        let explanation = DecisionExplanation(
            evidenceBreakdown: [
                SourceEvidence(source: "classifier", weight: 0.24, capApplied: 0.30, authority: .strong)
            ],
            contributingFamilies: ["classifier"],
            actionRationale: ActionRationale(
                threshold: 0.65,
                gate: "eligible",
                policyAction: "logOnly",
                skipEligible: false
            )
        )
        let data = try JSONEncoder().encode(explanation)
        let json = String(data: data, encoding: .utf8)!
        // Compact JSON should not contain newlines
        XCTAssertFalse(json.contains("\n"))
    }
}

// MARK: - SourceEvidence Authority

final class SourceEvidenceAuthorityTests: XCTestCase {

    func testAuthorityCodable() throws {
        for authority in [ProposalAuthority.strong, .weak] {
            let data = try JSONEncoder().encode(authority)
            let decoded = try JSONDecoder().decode(ProposalAuthority.self, from: data)
            XCTAssertEqual(decoded, authority)
        }
    }
}

// MARK: - DecisionExplanation Builder

final class DecisionExplanationBuilderTests: XCTestCase {

    func testClassifierOnly() {
        let ledger = [
            EvidenceLedgerEntry(
                source: .classifier,
                weight: 0.24,
                detail: .classifier(score: 0.80)
            )
        ]
        let decision = DecisionResult(
            proposalConfidence: 0.24,
            skipConfidence: 0.24,
            eligibilityGate: .eligible
        )
        let explanation = DecisionExplanation.build(
            ledger: ledger,
            decision: decision,
            policyAction: .logOnly,
            config: FusionWeightConfig(),
            skipThreshold: 0.65
        )
        XCTAssertEqual(explanation.evidenceBreakdown.count, 1)
        XCTAssertEqual(explanation.evidenceBreakdown[0].source, "classifier")
        XCTAssertEqual(explanation.evidenceBreakdown[0].weight, 0.24)
        XCTAssertEqual(explanation.evidenceBreakdown[0].capApplied, 0.30)
        XCTAssertEqual(explanation.contributingFamilies, ["classifier"])
        XCTAssertFalse(explanation.actionRationale.skipEligible)
    }

    func testMultiSource() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.24, detail: .classifier(score: 0.80)),
            EvidenceLedgerEntry(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(source: .lexical, weight: 0.10, detail: .lexical(matchedCategories: ["sponsor"])),
            EvidenceLedgerEntry(source: .lexical, weight: 0.08, detail: .lexical(matchedCategories: ["promo"])),
        ]
        let decision = DecisionResult(
            proposalConfidence: 0.77,
            skipConfidence: 0.77,
            eligibilityGate: .eligible
        )
        let explanation = DecisionExplanation.build(
            ledger: ledger,
            decision: decision,
            policyAction: .autoSkipEligible,
            config: FusionWeightConfig(),
            skipThreshold: 0.65
        )
        // classifier, fm, lexical — three distinct sources
        XCTAssertEqual(explanation.evidenceBreakdown.count, 3)
        // Lexical entries should be aggregated: 0.10 + 0.08 = 0.18
        let lexicalEntry = explanation.evidenceBreakdown.first { $0.source == "lexical" }
        XCTAssertNotNil(lexicalEntry)
        XCTAssertEqual(lexicalEntry?.weight ?? 0, 0.18, accuracy: 0.001)
        XCTAssertTrue(explanation.contributingFamilies.contains("classifier"))
        XCTAssertTrue(explanation.contributingFamilies.contains("fm"))
        XCTAssertTrue(explanation.contributingFamilies.contains("lexical"))
        XCTAssertTrue(explanation.actionRationale.skipEligible)
    }

    func testStrongAuthority() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.24, detail: .classifier(score: 0.80)),
        ]
        let decision = DecisionResult(
            proposalConfidence: 0.24,
            skipConfidence: 0.24,
            eligibilityGate: .eligible
        )
        let explanation = DecisionExplanation.build(
            ledger: ledger,
            decision: decision,
            policyAction: .logOnly,
            config: FusionWeightConfig(),
            skipThreshold: 0.65
        )
        // 0.24 > 0.30 * 0.5 = 0.15 → strong
        XCTAssertEqual(explanation.evidenceBreakdown[0].authority, .strong)
    }

    func testWeakAuthority() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.05, detail: .classifier(score: 0.17)),
        ]
        let decision = DecisionResult(
            proposalConfidence: 0.05,
            skipConfidence: 0.05,
            eligibilityGate: .eligible
        )
        let explanation = DecisionExplanation.build(
            ledger: ledger,
            decision: decision,
            policyAction: .logOnly,
            config: FusionWeightConfig(),
            skipThreshold: 0.65
        )
        // 0.05 < 0.30 * 0.5 = 0.15 → weak
        XCTAssertEqual(explanation.evidenceBreakdown[0].authority, .weak)
    }

    func testBlockedGate() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.24, detail: .classifier(score: 0.80)),
        ]
        let decision = DecisionResult(
            proposalConfidence: 0.24,
            skipConfidence: 0.24,
            eligibilityGate: .blockedByEvidenceQuorum
        )
        let explanation = DecisionExplanation.build(
            ledger: ledger,
            decision: decision,
            policyAction: .detectOnly,
            config: FusionWeightConfig(),
            skipThreshold: 0.65
        )
        XCTAssertEqual(explanation.actionRationale.gate, "blockedByEvidenceQuorum")
        XCTAssertFalse(explanation.actionRationale.skipEligible)
    }

    func testAllSources() {
        let ledger = [
            EvidenceLedgerEntry(source: .classifier, weight: 0.20, detail: .classifier(score: 0.67)),
            EvidenceLedgerEntry(source: .fm, weight: 0.35, detail: .fm(disposition: .containsAd, band: .strong, cohortPromptLabel: "v1")),
            EvidenceLedgerEntry(source: .lexical, weight: 0.10, detail: .lexical(matchedCategories: ["sponsor"])),
            EvidenceLedgerEntry(source: .acoustic, weight: 0.15, detail: .acoustic(breakStrength: 0.9)),
            EvidenceLedgerEntry(source: .catalog, weight: 0.10, detail: .catalog(entryCount: 2)),
            EvidenceLedgerEntry(source: .fingerprint, weight: 0.20, detail: .fingerprint(matchCount: 1, averageSimilarity: 0.95)),
        ]
        let decision = DecisionResult(
            proposalConfidence: 1.0,
            skipConfidence: 1.0,
            eligibilityGate: .eligible
        )
        let explanation = DecisionExplanation.build(
            ledger: ledger,
            decision: decision,
            policyAction: .autoSkipEligible,
            config: FusionWeightConfig(),
            skipThreshold: 0.65
        )
        XCTAssertEqual(explanation.evidenceBreakdown.count, 6)
        XCTAssertEqual(explanation.contributingFamilies.count, 6)
    }
}

// MARK: - DecisionEvent explanationJSON Field

final class DecisionEventExplanationTests: XCTestCase {

    private func makeStore() async throws -> AnalysisStore {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()
        return store
    }

    func testNilExplanationRoundTrip() async throws {
        let store = try await makeStore()
        let event = DecisionEvent(
            id: "e-nil",
            analysisAssetId: "asset1",
            eventType: "backfill_fusion",
            windowId: "w1",
            proposalConfidence: 0.72,
            skipConfidence: 0.72,
            eligibilityGate: "eligible",
            policyAction: "logOnly",
            decisionCohortJSON: "{}",
            createdAt: 1_000.0,
            explanationJSON: nil
        )
        try await store.appendDecisionEvent(event)
        let loaded = try await store.loadDecisionEvents(for: "asset1")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertNil(loaded[0].explanationJSON)
    }

    func testPopulatedExplanationRoundTrip() async throws {
        let store = try await makeStore()
        let explanation = DecisionExplanation(
            evidenceBreakdown: [
                SourceEvidence(source: "classifier", weight: 0.24, capApplied: 0.30, authority: .strong)
            ],
            contributingFamilies: ["classifier"],
            actionRationale: ActionRationale(
                threshold: 0.65,
                gate: "eligible",
                policyAction: "logOnly",
                skipEligible: false
            )
        )
        let json = String(data: try JSONEncoder().encode(explanation), encoding: .utf8)!

        let event = DecisionEvent(
            id: "e-pop",
            analysisAssetId: "asset1",
            eventType: "backfill_fusion",
            windowId: "w1",
            proposalConfidence: 0.72,
            skipConfidence: 0.72,
            eligibilityGate: "eligible",
            policyAction: "logOnly",
            decisionCohortJSON: "{}",
            createdAt: 1_000.0,
            explanationJSON: json
        )
        try await store.appendDecisionEvent(event)
        let loaded = try await store.loadDecisionEvents(for: "asset1")
        XCTAssertEqual(loaded.count, 1)
        XCTAssertNotNil(loaded[0].explanationJSON)
        // Verify the JSON decodes back to the original struct
        let decoded = try JSONDecoder().decode(
            DecisionExplanation.self,
            from: loaded[0].explanationJSON!.data(using: .utf8)!
        )
        XCTAssertEqual(decoded, explanation)
    }

    func testBackwardsCompatDefault() {
        // This test verifies that the default parameter works
        let event = DecisionEvent(
            id: "e-compat",
            analysisAssetId: "asset1",
            eventType: "backfill_fusion",
            windowId: "w1",
            proposalConfidence: 0.72,
            skipConfidence: 0.72,
            eligibilityGate: "eligible",
            policyAction: "logOnly",
            decisionCohortJSON: "{}",
            createdAt: 1_000.0
        )
        XCTAssertNil(event.explanationJSON)
    }
}
