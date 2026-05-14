// ShowCapabilityProfileEvaluatorTests.swift
// playhead-h6a6: pure-evaluator tests for per-show capability profile
// classification. Exercises the activation floor (≥ 5 episodes + SLI
// gate), each predicate's threshold, and the predicate priority order
// when multiple fire on the same show.
//
// The store-side persistence round-trip is covered by
// `ShowCapabilityProfileStorePersistenceTests`. The downstream budget
// modulator is covered by `ShowCapabilityBudgetModulatorTests`.

import Foundation
import Testing

@testable import Playhead

@Suite("ShowCapabilityProfileEvaluator")
struct ShowCapabilityProfileEvaluatorTests {

    /// Permissive SLI gate used when the test isn't exercising the
    /// gate axis. Returns `true` for every show.
    private static let openGate: ShowCapabilitySLIGate = { _ in true }

    /// Restrictive SLI gate used to assert the floor's SLI half. Returns
    /// `false` for every show.
    private static let closedGate: ShowCapabilitySLIGate = { _ in false }

    // MARK: - Activation floor

    @Test("less than 5 episodes pins the profile to .unknown")
    func subFloorIsUnknown() {
        // Even with overwhelming chapter coverage and music-bed
        // confirmation, the kind must stay `.unknown` until the
        // floor is met.
        let kind = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 4,
            chapterMatchedEpisodeCount: 4,
            hostVoicedEpisodeCount: 4,
            sponsorDeclaredEpisodeCount: 4,
            dynamicInsertionEpisodeCount: 4,
            musicBedConfirmed: true,
            sliGate: Self.openGate
        )
        #expect(kind == .unknown,
                "Sub-floor episodes must pin the profile to .unknown regardless of signals")
    }

    @Test("exactly 5 episodes meets the floor (not 6)")
    func floorIsInclusive() {
        // Tighten the boundary: at floor count == 5, the profile is
        // permitted to transition; at 4 it isn't. Catches a fence-
        // post regression that flips `>=` to `>`.
        let belowFloor = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 4,
            chapterMatchedEpisodeCount: 4,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        let atFloor = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 5,
            chapterMatchedEpisodeCount: 5,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(belowFloor == .unknown)
        #expect(atFloor == .chapterRich, "5 episodes with all chapter-matched should observe chapter-rich")
    }

    @Test("closed SLI gate pins the profile to .unknown")
    func sliGateClosed() {
        // Phase-2 SLI gate is the second half of the activation
        // floor. Even with floor count + a confirmed signal, a
        // closed gate must keep the kind at `.unknown`.
        let kind = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 10,
            hostVoicedEpisodeCount: 10,
            sponsorDeclaredEpisodeCount: 10,
            dynamicInsertionEpisodeCount: 10,
            musicBedConfirmed: true,
            sliGate: Self.closedGate
        )
        #expect(kind == .unknown)
    }

    // MARK: - Predicate thresholds

    @Test("chapter-rich fires at exactly 80%")
    func chapterRichThreshold() {
        // 4/5 = 80%, exact boundary. Strict `>=` so this should
        // fire; `8/10 = 80%` likewise. `7/10 = 70%` should NOT.
        let atBoundary = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 5,
            chapterMatchedEpisodeCount: 4,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        let belowBoundary = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 7,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(atBoundary == .chapterRich)
        #expect(belowBoundary == .unknown,
                "70% chapter coverage should NOT fire — strict >= 80% is the contract")
    }

    @Test("host-read-only fires at exactly 70%")
    func hostReadOnlyThreshold() {
        let atBoundary = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 0,
            hostVoicedEpisodeCount: 7,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        let belowBoundary = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 0,
            hostVoicedEpisodeCount: 6,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(atBoundary == .hostReadOnly)
        #expect(belowBoundary == .unknown)
    }

    @Test("sponsor-declared fires at exactly 50%")
    func sponsorDeclaredThreshold() {
        let atBoundary = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 0,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 5,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(atBoundary == .sponsorDeclared)
    }

    @Test("dynamic-insertion-heavy fires at exactly 50%")
    func dynamicInsertionThreshold() {
        let atBoundary = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 0,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 5,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(atBoundary == .dynamicInsertionHeavy)
    }

    @Test("music-bed-reliable fires from the 2hpn signal regardless of other counters")
    func musicBedReliableFires() {
        // Music-bed-reliable consumes the 2hpn confirmation signal
        // directly — no episode-share threshold. Floor + SLI gate
        // still required.
        let kind = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 0,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: true,
            sliGate: Self.openGate
        )
        #expect(kind == .musicBedReliable)
    }

    // MARK: - Priority order

    @Test("music-bed-reliable wins over chapter-rich when both fire")
    func priorityMusicBedOverChapter() {
        // Multiple predicates fire: music-bed + chapter-rich. The
        // priority order (documented at `classify`) puts the
        // 2hpn-derived signal first.
        let kind = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 10,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: true,
            sliGate: Self.openGate
        )
        #expect(kind == .musicBedReliable)
    }

    @Test("chapter-rich wins over host-read-only when both fire")
    func priorityChapterOverHostRead() {
        let kind = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 10,
            hostVoicedEpisodeCount: 10,
            sponsorDeclaredEpisodeCount: 0,
            dynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(kind == .chapterRich)
    }

    @Test("host-read-only wins over sponsor-declared and dynamic-insertion")
    func priorityHostReadOverSponsorAndDynamic() {
        let kind = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 0,
            hostVoicedEpisodeCount: 8,
            sponsorDeclaredEpisodeCount: 6,
            dynamicInsertionEpisodeCount: 6,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(kind == .hostReadOnly)
    }

    @Test("sponsor-declared wins over dynamic-insertion-heavy when both fire at the 50% boundary")
    func prioritySponsorOverDynamic() {
        // h6a6 R1 review gap: both predicates share a 50% threshold;
        // priority order says sponsor wins. Pin the tie-break so a
        // future edit that reorders the predicates fails CI rather
        // than silently flipping observed classifications.
        let kind = ShowCapabilityProfileEvaluator.classify(
            showIdentifier: "show",
            completedEpisodeCount: 10,
            chapterMatchedEpisodeCount: 0,
            hostVoicedEpisodeCount: 0,
            sponsorDeclaredEpisodeCount: 5,
            dynamicInsertionEpisodeCount: 5,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(kind == .sponsorDeclared,
                "Sponsor-declared must win over dynamic-insertion-heavy at the shared 50% boundary")
    }

    // MARK: - apply(...) mutation

    @Test("apply increments completed count and counter for true outcome")
    func applyAdvancesCounters() {
        let outcome = ShowCapabilityEpisodeOutcome(
            chapterMatched: true,
            hostVoiced: false,
            sponsorDeclared: true,
            dynamicInsertionShift: false
        )
        let mutation = ShowCapabilityProfileEvaluator.apply(
            outcome: outcome,
            showIdentifier: "show",
            priorCompletedEpisodeCount: 2,
            priorChapterMatchedEpisodeCount: 1,
            priorHostVoicedEpisodeCount: 1,
            priorSponsorDeclaredEpisodeCount: 0,
            priorDynamicInsertionEpisodeCount: 0,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(mutation.completedEpisodeCount == 3)
        #expect(mutation.chapterMatchedEpisodeCount == 2)
        #expect(mutation.hostVoicedEpisodeCount == 1)
        #expect(mutation.sponsorDeclaredEpisodeCount == 1)
        #expect(mutation.dynamicInsertionEpisodeCount == 0)
        // Sub-floor — even with 100% sponsor coverage so far the
        // kind stays `.unknown`.
        #expect(mutation.kind == .unknown)
    }

    @Test("apply respects nothingObserved sentinel")
    func applyNothingObserved() {
        // The all-false sentinel should advance only the completed
        // count, leaving every per-predicate counter unchanged.
        let mutation = ShowCapabilityProfileEvaluator.apply(
            outcome: .nothingObserved,
            showIdentifier: "show",
            priorCompletedEpisodeCount: 9,
            priorChapterMatchedEpisodeCount: 3,
            priorHostVoicedEpisodeCount: 3,
            priorSponsorDeclaredEpisodeCount: 3,
            priorDynamicInsertionEpisodeCount: 3,
            musicBedConfirmed: false,
            sliGate: Self.openGate
        )
        #expect(mutation.completedEpisodeCount == 10)
        #expect(mutation.chapterMatchedEpisodeCount == 3)
        #expect(mutation.hostVoicedEpisodeCount == 3)
        #expect(mutation.sponsorDeclaredEpisodeCount == 3)
        #expect(mutation.dynamicInsertionEpisodeCount == 3)
        #expect(mutation.kind == .unknown,
                "3/10 = 30% — below every predicate threshold")
    }
}
