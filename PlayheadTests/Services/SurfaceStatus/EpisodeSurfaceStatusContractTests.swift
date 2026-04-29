// EpisodeSurfaceStatusContractTests.swift
// Contract test: asserts that the cartesian-product fixture matrix
// covers every `(SurfaceDisposition × SurfaceReason)` pair the reducer
// actually emits — and, conversely, that the reducer does not emit a
// pair that no fixture covers. The intent is not to enumerate every
// mathematically-possible pairing (most would never be reducible from a
// real input combo); it is to pin the actual emitted set so any future
// change to the reducer ladder is caught by this test.
//
// See `EpisodeSurfaceStatusSnapshotTests` for the cartesian-product
// rendering and the golden JSON fixture it writes.

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeSurfaceStatusReducer — contract coverage (playhead-5bb3)")
struct EpisodeSurfaceStatusContractTests {

    /// The set of `(disposition, reason)` pairs the reducer is known to
    /// emit for the Phase 1.5 input space. When a future bead extends
    /// the reducer (e.g. Phase 2 playhead-cthe adds coverage-driven
    /// rows), add the new pairs to this set and the snapshot fixtures.
    ///
    /// Encoded as a struct rather than a tuple so the Hashable /
    /// Equatable requirements are explicit.
    struct Pair: Hashable {
        let disposition: SurfaceDisposition
        let reason: SurfaceReason
    }

    static let expectedPairs: Set<Pair> = [
        // Rule 1 — eligibility-blocks (also `.unsupportedEpisodeLanguage`
        // via the policy delegation, which routes to `.unavailable`).
        .init(disposition: .unavailable, reason: .analysisUnavailable),
        // Rule 2 — user-paused. After playhead-own9 the cause→triple
        // mapping for `.userCancelled` / `.userPreempted` delegates to
        // `CauseAttributionPolicy`, which emits (paused, cancelled).
        // `.appForceQuitRequiresRelaunch` keeps its (paused, resumeInApp)
        // triple.
        .init(disposition: .paused, reason: .cancelled),
        .init(disposition: .paused, reason: .resumeInApp),
        // Rule 3 — resource-blocks. `.mediaCap` keeps storageFull;
        // `.analysisCap` and `.taskExpired` (retries exhausted) both
        // surface (failed, couldntAnalyze) via the policy.
        .init(disposition: .failed, reason: .storageFull),
        .init(disposition: .failed, reason: .couldntAnalyze),
        // Rule 4 — transient-waits
        .init(disposition: .paused, reason: .phoneIsHot),
        .init(disposition: .paused, reason: .powerLimited),
        .init(disposition: .paused, reason: .waitingForNetwork),
        // Rule 4 — transient taskExpired (retries remaining) + Rule 5
        .init(disposition: .queued, reason: .waitingForTime),
    ]

    @Test("Every cartesian-fixture pair is covered by ≥1 matrix row")
    func cartesianFixtureCoversExpectedPairs() {
        // Iterate over the full fixture matrix and collect the pairs
        // the reducer actually emits. Compare against the expected set.
        let emitted = Self.emittedPairsFromMatrix()

        for pair in Self.expectedPairs {
            let msg = "Expected pair (\(pair.disposition), \(pair.reason)) is not emitted by any matrix row; update the fixture matrix or the expectedPairs set."
            #expect(emitted.contains(pair), Comment(rawValue: msg))
        }
    }

    @Test("Reducer does not emit an unexpected (disposition, reason) pair")
    func reducerDoesNotEmitUnexpectedPairs() {
        let emitted = Self.emittedPairsFromMatrix()
        let unexpected = emitted.subtracting(Self.expectedPairs)
        let msg = "Reducer emitted unexpected pairs \(unexpected); either add them to expectedPairs (after reviewing the new behavior) or fix the reducer."
        #expect(unexpected.isEmpty, Comment(rawValue: msg))
    }

    // MARK: - Matrix generation

    /// Compute every `(disposition, reason)` pair the reducer emits
    /// across the cartesian product used by the snapshot tests.
    static func emittedPairsFromMatrix() -> Set<Pair> {
        var set: Set<Pair> = []
        for row in EpisodeSurfaceStatusMatrix.rows() {
            let out = episodeSurfaceStatus(
                state: row.state,
                cause: row.cause,
                eligibility: row.eligibility,
                coverage: row.coverage,
                readinessAnchor: row.readinessAnchor
            )
            set.insert(.init(disposition: out.disposition, reason: out.reason))
        }
        return set
    }
}
