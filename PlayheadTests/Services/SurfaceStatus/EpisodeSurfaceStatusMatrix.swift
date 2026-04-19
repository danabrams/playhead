// EpisodeSurfaceStatusMatrix.swift
// Shared cartesian-product matrix used by the snapshot and contract
// test suites. Factored out so both files can iterate the identical
// row set — any drift between the two would defeat the contract
// coverage guarantee.

import Foundation
@testable import Playhead

/// One row of the cartesian-product matrix: a named combination of the
/// reducer's five inputs. The `label` is stable across runs so golden
/// fixtures key rows by their label rather than by array index — that
/// way adding a new row in the middle doesn't invalidate every prior
/// snapshot.
struct EpisodeSurfaceStatusMatrixRow {
    let label: String
    let state: AnalysisState
    let cause: InternalMissCause?
    let eligibility: AnalysisEligibility
    let coverage: CoverageSummary?
    let readinessAnchor: TimeInterval?
}

/// Cartesian-product fixture generator. The matrix is the deliberate
/// product of:
///   * eligibility  × { fully-eligible, ai-disabled }
///   * cause        × { nil, representative-per-tier × 9 }
///   * state        × { queued, failed }  (failed only matters for
///                                          `taskExpired`, but we run
///                                          the product across both to
///                                          catch ladder bugs)
///   * coverage     × { nil, stub-present }
///   * readiness    × { nil, 42.5 }
///
/// 2 × 10 × 2 × 2 × 2 = 160 rows. Small enough to render a stable JSON
/// fixture in-memory at test time.
///
/// Why only 9 representative causes (not every `InternalMissCause`
/// variant)? The 9 entries cover all 9 distinct `(disposition, reason)`
/// pairs the reducer emits. Additional cause variants that route to
/// the same pair (e.g. `.asrFailed` / `.pipelineError` / `.noRuntimeGrant`
/// / `.unsupportedEpisodeLanguage` all share the default-branch
/// `(failed, couldntAnalyze)`, and `.batteryLowUnplugged` / `.noNetwork`
/// / `.modelTemporarilyUnavailable` join the transient-wait tier) are
/// intentionally NOT duplicated here — they are pinned by targeted
/// unit tests in `EpisodeSurfaceStatusReducerTests`. Keeping the matrix
/// representative (rather than exhaustive) keeps the golden fixture
/// readable without sacrificing reducer coverage.
enum EpisodeSurfaceStatusMatrix {

    // MARK: - Base values

    private static let t0 = Date(timeIntervalSince1970: 1_700_000_000)

    private static let eligible = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: true,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: t0
    )

    private static let aiDisabled = AnalysisEligibility(
        hardwareSupported: true,
        appleIntelligenceEnabled: false,
        regionSupported: true,
        languageSupported: true,
        modelAvailableNow: true,
        capturedAt: t0
    )

    private static let queuedState = AnalysisState(
        persistedStatus: .queued,
        hasUserPreemptedJob: false,
        hasAppForceQuitFlag: false,
        pendingSinceEnqueuedAt: t0,
        hasAnyConfirmedAnalysis: false
    )

    private static let failedState = AnalysisState(
        persistedStatus: .failed,
        hasUserPreemptedJob: false,
        hasAppForceQuitFlag: true,
        pendingSinceEnqueuedAt: nil,
        hasAnyConfirmedAnalysis: false
    )

    /// Representative causes — one per ladder tier, plus a few that
    /// exercise the context-dependent rows. The 10 entries (9 causes +
    /// `nil`) keep the row count manageable while still reducing to
    /// every disposition/reason the reducer emits.
    private static let causes: [(String, InternalMissCause?)] = [
        ("none", nil),
        ("userPreempted", .userPreempted),
        ("userCancelled", .userCancelled),
        ("appForceQuit", .appForceQuitRequiresRelaunch),
        ("mediaCap", .mediaCap),
        ("analysisCap", .analysisCap),
        ("taskExpired", .taskExpired),
        ("thermal", .thermal),
        ("lowPowerMode", .lowPowerMode),
        ("wifiRequired", .wifiRequired),
    ]

    private static let eligibilityCases: [(String, AnalysisEligibility)] = [
        ("eligible", eligible),
        ("aiDisabled", aiDisabled),
    ]

    private static let stateCases: [(String, AnalysisState)] = [
        ("queued", queuedState),
        ("failed", failedState),
    ]

    private static let coverageCases: [(String, CoverageSummary?)] = [
        ("coverageNil", nil),
        ("coveragePresent", CoverageSummary(hasAnyCoverage: true)),
    ]

    private static let anchorCases: [(String, TimeInterval?)] = [
        ("anchorNil", nil),
        ("anchor42_5", 42.5),
    ]

    // MARK: - Rows

    /// The full cartesian product, ordered deterministically by the
    /// concatenated labels so the golden JSON is stable across runs.
    static func rows() -> [EpisodeSurfaceStatusMatrixRow] {
        var out: [EpisodeSurfaceStatusMatrixRow] = []
        for (eLabel, eligibility) in eligibilityCases {
            for (cLabel, cause) in causes {
                for (sLabel, state) in stateCases {
                    for (covLabel, coverage) in coverageCases {
                        for (aLabel, anchor) in anchorCases {
                            let label = [eLabel, cLabel, sLabel, covLabel, aLabel]
                                .joined(separator: "/")
                            out.append(
                                EpisodeSurfaceStatusMatrixRow(
                                    label: label,
                                    state: state,
                                    cause: cause,
                                    eligibility: eligibility,
                                    coverage: coverage,
                                    readinessAnchor: anchor
                                )
                            )
                        }
                    }
                }
            }
        }
        return out
    }
}
