// ExecutionConditionClassifierTests.swift
// Table-driven boundary tests for ExecutionConditionClassifier.
// Every threshold is exercised at the exact boundary value, plus one
// step above and one step below, to catch off-by-one errors in the
// comparison operators.

import Foundation
import Testing

@testable import Playhead

@Suite("ExecutionConditionClassifier")
struct ExecutionConditionClassifierTests {

    // MARK: - Helper

    private func make(
        reachability: SLIReachability = .wifi,
        batteryLevel: Float = 0.80,
        batteryState: SLIBatteryState = .charging,
        thermalState: ThermalState = .nominal,
        isLowPowerMode: Bool = false
    ) -> ExecutionConditionInput {
        ExecutionConditionInput(
            reachability: reachability,
            batteryLevel: batteryLevel,
            batteryState: batteryState,
            thermalState: thermalState,
            isLowPowerMode: isLowPowerMode
        )
    }

    // MARK: - Favorable path (all conditions met)

    @Test("Favorable: Wi-Fi + charging + nominal thermal")
    func favorableCharging() {
        let input = make(
            reachability: .wifi,
            batteryLevel: 0.10, // irrelevant when charging
            batteryState: .charging,
            thermalState: .nominal
        )
        #expect(ExecutionConditionClassifier.classify(input) == .favorable)
    }

    @Test("Favorable: Wi-Fi + battery exactly 50% not charging + fair thermal")
    func favorableBatteryExactly50NotCharging() {
        // 0.50 is the inclusive floor — exactly-50% qualifies as favorable.
        let input = make(
            reachability: .wifi,
            batteryLevel: 0.50,
            batteryState: .notCharging,
            thermalState: .fair
        )
        #expect(ExecutionConditionClassifier.classify(input) == .favorable)
    }

    @Test("Not favorable: Wi-Fi + battery 49.9% not charging + nominal -> mixed")
    func mixedBatteryJustBelow50NotCharging() {
        // One step below the inclusive floor — no longer favorable, but
        // not constrained either (20% is the constrained ceiling).
        let input = make(
            reachability: .wifi,
            batteryLevel: 0.499,
            batteryState: .notCharging,
            thermalState: .nominal
        )
        #expect(ExecutionConditionClassifier.classify(input) == .mixed)
    }

    // MARK: - Constrained: cellular wins even with otherwise-favorable state

    @Test("Constrained: cellular overrides charging + nominal thermal")
    func constrainedCellularBeatsOtherwiseFavorable() {
        let input = make(
            reachability: .cellular,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .nominal
        )
        #expect(ExecutionConditionClassifier.classify(input) == .constrained)
    }

    // MARK: - Constrained: battery < 20% and not charging

    @Test("Constrained: battery 19% not charging -> constrained")
    func constrainedBatteryJustBelow20NotCharging() {
        let input = make(
            reachability: .wifi,
            batteryLevel: 0.19,
            batteryState: .notCharging,
            thermalState: .nominal
        )
        #expect(ExecutionConditionClassifier.classify(input) == .constrained)
    }

    @Test("Not constrained: battery exactly 20% not charging -> mixed")
    func batteryExactly20NotChargingIsNotConstrained() {
        // The rule is `< 0.20` (strict), so exactly 20% does NOT trip
        // constrained. It can't be favorable (< 50% and not charging),
        // so it lands in mixed.
        let input = make(
            reachability: .wifi,
            batteryLevel: 0.20,
            batteryState: .notCharging,
            thermalState: .nominal
        )
        #expect(ExecutionConditionClassifier.classify(input) == .mixed)
    }

    @Test("Not constrained: battery 19% but charging -> not battery-constrained")
    func lowBatteryChargingIsNotBatteryConstrained() {
        // Charging trumps the low-battery check (matches the documented rule
        // "battery < 20% AND not charging").
        let input = make(
            reachability: .wifi,
            batteryLevel: 0.19,
            batteryState: .charging,
            thermalState: .nominal
        )
        // Charging + Wi-Fi + nominal thermal = favorable.
        #expect(ExecutionConditionClassifier.classify(input) == .favorable)
    }

    // MARK: - Constrained: thermal >= serious

    @Test("Constrained: thermal serious overrides charging + Wi-Fi")
    func constrainedThermalSerious() {
        let input = make(
            reachability: .wifi,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .serious
        )
        #expect(ExecutionConditionClassifier.classify(input) == .constrained)
    }

    @Test("Constrained: thermal critical")
    func constrainedThermalCritical() {
        let input = make(
            reachability: .wifi,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .critical
        )
        #expect(ExecutionConditionClassifier.classify(input) == .constrained)
    }

    @Test("Not constrained: thermal fair is still favorable territory")
    func thermalFairIsFavorable() {
        let input = make(
            reachability: .wifi,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .fair
        )
        #expect(ExecutionConditionClassifier.classify(input) == .favorable)
    }

    @Test("Not constrained: thermal nominal is favorable territory")
    func thermalNominalIsFavorable() {
        let input = make(
            reachability: .wifi,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .nominal
        )
        #expect(ExecutionConditionClassifier.classify(input) == .favorable)
    }

    // MARK: - Mixed: the "everything else" fallback

    @Test("Mixed: Wi-Fi + not charging + battery 40% + fair thermal")
    func mixedMiddleBattery() {
        let input = make(
            reachability: .wifi,
            batteryLevel: 0.40,
            batteryState: .notCharging,
            thermalState: .fair
        )
        // Battery 40% fails favorable (< 50%, not charging), but 40% > 20%
        // so it doesn't trip constrained either.
        #expect(ExecutionConditionClassifier.classify(input) == .mixed)
    }

    @Test("Mixed: unknown reachability is neither favorable nor constrained on the network axis")
    func mixedUnknownReachability() {
        let input = make(
            reachability: .unknown,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .nominal
        )
        // Unknown reachability is not Wi-Fi, so favorable's network
        // predicate fails. It's also not cellular, so constrained's
        // network predicate doesn't fire. => mixed.
        #expect(ExecutionConditionClassifier.classify(input) == .mixed)
    }

    @Test("Mixed: unknown battery state + Wi-Fi + fair thermal + 80% battery")
    func mixedUnknownBatteryStateHighBattery() {
        // Battery level is 80%, which satisfies the favorable floor even
        // when the state is unknown (we read level directly). So this is
        // actually favorable — documents that unknown state does NOT
        // block the favorable path if the level says so.
        let input = make(
            reachability: .wifi,
            batteryLevel: 0.80,
            batteryState: .unknown,
            thermalState: .nominal
        )
        #expect(ExecutionConditionClassifier.classify(input) == .favorable)
    }

    @Test("Unknown battery level + Wi-Fi + not charging -> mixed (cannot claim >= 50%)")
    func unknownBatteryLevelNotCharging() {
        let input = make(
            reachability: .wifi,
            batteryLevel: -1.0, // sentinel: unknown
            batteryState: .notCharging,
            thermalState: .nominal
        )
        // Unknown level cannot satisfy the favorable floor; but it's also
        // not "< 20% AND known", so constrained doesn't fire either.
        #expect(ExecutionConditionClassifier.classify(input) == .mixed)
    }

    @Test("Unknown battery level + Wi-Fi + charging -> favorable")
    func unknownBatteryLevelChargingIsFavorable() {
        let input = make(
            reachability: .wifi,
            batteryLevel: -1.0,
            batteryState: .charging,
            thermalState: .nominal
        )
        // Charging alone satisfies the power predicate regardless of level.
        #expect(ExecutionConditionClassifier.classify(input) == .favorable)
    }

    // MARK: - Precedence: constrained wins over favorable predicates

    @Test("Precedence: cellular + charging + nominal -> constrained, not favorable")
    func cellularBeatsOtherFavorablePredicates() {
        let input = make(
            reachability: .cellular,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .nominal
        )
        #expect(ExecutionConditionClassifier.classify(input) == .constrained)
    }

    // MARK: - Full boundary sweep (table-driven)

    @Test("Table: exact boundary values across all axes")
    func fullBoundarySweep() {
        struct Case {
            let name: String
            let input: ExecutionConditionInput
            let expected: SLIExecutionCondition
        }

        let cases: [Case] = [
            Case(
                name: "battery exactly 0.50, not charging, Wi-Fi, nominal",
                input: ExecutionConditionInput(
                    reachability: .wifi,
                    batteryLevel: 0.50,
                    batteryState: .notCharging,
                    thermalState: .nominal,
                    isLowPowerMode: false
                ),
                expected: .favorable
            ),
            Case(
                name: "battery exactly 0.50, not charging, Wi-Fi, fair",
                input: ExecutionConditionInput(
                    reachability: .wifi,
                    batteryLevel: 0.50,
                    batteryState: .notCharging,
                    thermalState: .fair,
                    isLowPowerMode: false
                ),
                expected: .favorable
            ),
            Case(
                name: "battery exactly 0.50, not charging, Wi-Fi, serious",
                input: ExecutionConditionInput(
                    reachability: .wifi,
                    batteryLevel: 0.50,
                    batteryState: .notCharging,
                    thermalState: .serious,
                    isLowPowerMode: false
                ),
                expected: .constrained
            ),
            Case(
                name: "battery exactly 0.20, not charging, Wi-Fi, nominal",
                input: ExecutionConditionInput(
                    reachability: .wifi,
                    batteryLevel: 0.20,
                    batteryState: .notCharging,
                    thermalState: .nominal,
                    isLowPowerMode: false
                ),
                expected: .mixed
            ),
            Case(
                name: "battery just below 0.20, not charging, Wi-Fi, nominal",
                input: ExecutionConditionInput(
                    reachability: .wifi,
                    batteryLevel: 0.1999,
                    batteryState: .notCharging,
                    thermalState: .nominal,
                    isLowPowerMode: false
                ),
                expected: .constrained
            ),
            Case(
                name: "battery exactly 0.00, not charging, Wi-Fi, nominal",
                input: ExecutionConditionInput(
                    reachability: .wifi,
                    batteryLevel: 0.00,
                    batteryState: .notCharging,
                    thermalState: .nominal,
                    isLowPowerMode: false
                ),
                expected: .constrained
            ),
            Case(
                name: "battery 0.0, charging (dead but plugged in), Wi-Fi, nominal",
                input: ExecutionConditionInput(
                    reachability: .wifi,
                    batteryLevel: 0.00,
                    batteryState: .charging,
                    thermalState: .nominal,
                    isLowPowerMode: false
                ),
                expected: .favorable
            ),
        ]

        for c in cases {
            let actual = ExecutionConditionClassifier.classify(c.input)
            #expect(actual == c.expected, "case '\(c.name)': expected \(c.expected), got \(actual)")
        }
    }

    // MARK: - Low Power Mode (H2)

    @Test("Constrained: LPM on overrides Wi-Fi + charging + nominal thermal")
    func lowPowerModeForcesConstrainedEvenWhenOtherwiseFavorable() {
        // Without LPM this would be the canonical favorable case. The LPM
        // axis must override (parallel to "battery < 20% and not charging")
        // so an OS-throttled device doesn't bias the cohort to favorable.
        let input = make(
            reachability: .wifi,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .nominal,
            isLowPowerMode: true
        )
        #expect(ExecutionConditionClassifier.classify(input) == .constrained)
    }

    @Test("Favorable: LPM off + Wi-Fi + charging + nominal thermal")
    func lpmOffPreservesFavorablePath() {
        // The mirror of the above: with LPM explicitly off the input is
        // unchanged from the canonical favorable case.
        let input = make(
            reachability: .wifi,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .nominal,
            isLowPowerMode: false
        )
        #expect(ExecutionConditionClassifier.classify(input) == .favorable)
    }

    @Test("Unknown reachability + thermal serious -> constrained (constrained always wins)")
    func testUnknownReachabilityCompoundsWithConstrained() {
        // Unknown reachability falls to mixed on its own, but a constrained
        // predicate (here: thermal serious) must still force `.constrained`.
        let input = make(
            reachability: .unknown,
            batteryLevel: 1.00,
            batteryState: .charging,
            thermalState: .serious
        )
        #expect(ExecutionConditionClassifier.classify(input) == .constrained)
    }

    @Test("LPM still constrained even when reachability/thermal/battery are also constrained")
    func lpmCompoundsWithOtherConstrainedPredicates() {
        // Defensive check: LPM should not change the answer in cases where
        // some other constrained predicate already fires. The bucket stays
        // `constrained` (not e.g. a hypothetical `verConstrained`).
        let input = make(
            reachability: .cellular,
            batteryLevel: 0.05,
            batteryState: .notCharging,
            thermalState: .serious,
            isLowPowerMode: true
        )
        #expect(ExecutionConditionClassifier.classify(input) == .constrained)
    }
}
