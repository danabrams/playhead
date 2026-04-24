// AcousticFeatureFunnelTests.swift
// playhead-gtt9.12 acceptance #1: verifies funnel counters thread correctly
// through each lifecycle stage.

import Foundation
import Testing

@testable import Playhead

@Suite("AcousticFeatureFunnel")
struct AcousticFeatureFunnelTests {

    @Test("fresh funnel is all-zero across features and stages")
    func freshIsZero() {
        let funnel = AcousticFeatureFunnel()
        for feature in AcousticFeatureKind.allCases {
            for stage in AcousticFeatureFunnelStage.allCases {
                #expect(funnel.count(stage, feature) == 0)
            }
        }
        #expect(funnel.total(.computed) == 0)
    }

    @Test("record increments stage / feature independently")
    func recordsIndependently() {
        var funnel = AcousticFeatureFunnel()
        funnel.record(.computed, .lufsShift)
        funnel.record(.computed, .lufsShift)
        funnel.record(.producedSignal, .lufsShift)
        funnel.record(.computed, .dynamicRange)
        #expect(funnel.count(.computed, .lufsShift) == 2)
        #expect(funnel.count(.producedSignal, .lufsShift) == 1)
        #expect(funnel.count(.computed, .dynamicRange) == 1)
        #expect(funnel.count(.computed, .musicBed) == 0)
        #expect(funnel.total(.computed) == 3)
    }

    @Test("convenience record advances monotonically through stages when flags are true")
    func convenienceRecord() {
        var funnel = AcousticFeatureFunnel()
        funnel.record(feature: .silenceBoundary, producedSignal: true, passedGate: true, includedInFusion: true)
        #expect(funnel.count(.computed, .silenceBoundary) == 1)
        #expect(funnel.count(.producedSignal, .silenceBoundary) == 1)
        #expect(funnel.count(.passedGate, .silenceBoundary) == 1)
        #expect(funnel.count(.includedInFusion, .silenceBoundary) == 1)
    }

    @Test("convenience record skips downstream stages when flags are false")
    func convenienceRecordRespectsFlags() {
        var funnel = AcousticFeatureFunnel()
        funnel.record(feature: .tempoOnset, producedSignal: true, passedGate: false, includedInFusion: false)
        #expect(funnel.count(.computed, .tempoOnset) == 1)
        #expect(funnel.count(.producedSignal, .tempoOnset) == 1)
        #expect(funnel.count(.passedGate, .tempoOnset) == 0)
        #expect(funnel.count(.includedInFusion, .tempoOnset) == 0)
    }

    @Test("rows flatten every (feature, stage) cell in stable order")
    func rowsFlattenEverything() {
        var funnel = AcousticFeatureFunnel()
        funnel.record(.computed, .musicBed)
        let rows = funnel.rows()
        let expected = AcousticFeatureKind.allCases.count * AcousticFeatureFunnelStage.allCases.count
        #expect(rows.count == expected)
        let musicBedComputed = rows.first { $0.feature == .musicBed && $0.stage == .computed }
        #expect(musicBedComputed?.count == 1)
    }
}
