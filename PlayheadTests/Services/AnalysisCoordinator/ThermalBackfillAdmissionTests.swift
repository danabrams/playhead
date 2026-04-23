// ThermalBackfillAdmissionTests.swift
// playhead-gtt9.8: `hotPathReady` → `backfill` must branch through
// `.waitingForBackfill` when the device is under critical thermal
// pressure rather than silently parking on `.hotPathReady`. The red
// integration test exercises the pure helper
// `AnalysisCoordinator.thermalBackfillAdmission(thermalState:)` that
// encapsulates the branching rule so the `runFromHotPathReady`
// implementation stays a thin dispatch layer.
//
// The helper returns:
//   - `.proceed` when the device can drive backfill immediately
//     (thermal ≤ serious).
//   - `.wait`    when the device is throttled to `.critical`; the
//     coordinator must transition the session into
//     `.waitingForBackfill` instead of returning early.
//
// Tests are intentionally exhaustive across all four `ThermalState`
// cases so a regression that flips the threshold (e.g. treating
// `.serious` as `.wait`) is caught immediately.

import Foundation
import Testing

@testable import Playhead

@Suite("AnalysisCoordinator.thermalBackfillAdmission — gtt9.8")
struct ThermalBackfillAdmissionTests {

    @Test("nominal thermal proceeds to backfill")
    func nominalProceeds() {
        #expect(
            AnalysisCoordinator.thermalBackfillAdmission(thermalState: .nominal)
                == .proceed
        )
    }

    @Test("fair thermal proceeds to backfill")
    func fairProceeds() {
        #expect(
            AnalysisCoordinator.thermalBackfillAdmission(thermalState: .fair)
                == .proceed
        )
    }

    @Test("serious thermal still proceeds to backfill (throttled, not blocked)")
    func seriousProceeds() {
        // .serious is a warning signal, not a hard stop. The coordinator
        // already reduces aggressiveness elsewhere; we don't want to
        // leave the session parked on .hotPathReady under mere warmth.
        #expect(
            AnalysisCoordinator.thermalBackfillAdmission(thermalState: .serious)
                == .proceed
        )
    }

    @Test("critical thermal parks the session in waitingForBackfill")
    func criticalParksInWaiting() {
        // Under critical pressure the coordinator must route through
        // the explicit `.waitingForBackfill` state so the UI and NARL
        // harness can reason about "paused by thermal" vs "stuck at
        // hotPathReady for unknown reasons".
        #expect(
            AnalysisCoordinator.thermalBackfillAdmission(thermalState: .critical)
                == .wait
        )
    }
}
