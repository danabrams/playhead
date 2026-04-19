// DiagnosticsOptInResetPolicyTests.swift
// Pure-logic verification of when `Episode.diagnosticsOptIn` resets after
// the mail composer dismisses.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Spec contract:
//   * `.sent`      → reset to false (the artifact left the device).
//   * `.saved`     → reset to false (Drafts also counts as "in user's hands").
//   * `.cancelled` → preserve current value (user backed out).
//   * `.failed`    → preserve current value (system error, not user intent).
//
// Why a pure function: `MFMailComposeViewController` is simulator-hostile
// to drive end-to-end. Splitting the reset rule into a pure
// `(current, result) -> Bool` mapping lets the unit suite cover every
// transition without spinning up a UIKit composer.

import Foundation
import Testing

@testable import Playhead

@Suite("DiagnosticsOptInResetPolicy — pure (current, result) → newValue (playhead-ghon)")
struct DiagnosticsOptInResetPolicyTests {

    // MARK: - Reset cases (`.sent` / `.saved`)

    @Test("(true, .sent) → false")
    func sentClearsTrue() {
        #expect(DiagnosticsOptInResetPolicy.newValue(current: true, result: .sent) == false)
    }

    @Test("(true, .saved) → false")
    func savedClearsTrue() {
        #expect(DiagnosticsOptInResetPolicy.newValue(current: true, result: .saved) == false)
    }

    @Test("(false, .sent) stays false (idempotent)")
    func sentIdempotent() {
        #expect(DiagnosticsOptInResetPolicy.newValue(current: false, result: .sent) == false)
    }

    @Test("(false, .saved) stays false (idempotent)")
    func savedIdempotent() {
        #expect(DiagnosticsOptInResetPolicy.newValue(current: false, result: .saved) == false)
    }

    // MARK: - Preserve cases (`.cancelled` / `.failed`)

    @Test("(true, .cancelled) → true")
    func cancelledPreservesTrue() {
        #expect(DiagnosticsOptInResetPolicy.newValue(current: true, result: .cancelled) == true)
    }

    @Test("(true, .failed) → true")
    func failedPreservesTrue() {
        #expect(DiagnosticsOptInResetPolicy.newValue(current: true, result: .failed) == true)
    }

    @Test("(false, .cancelled) stays false")
    func cancelledFromFalseStaysFalse() {
        #expect(DiagnosticsOptInResetPolicy.newValue(current: false, result: .cancelled) == false)
    }

    @Test("(false, .failed) stays false")
    func failedFromFalseStaysFalse() {
        #expect(DiagnosticsOptInResetPolicy.newValue(current: false, result: .failed) == false)
    }

    // MARK: - Exhaustiveness — every case is covered above.
    //
    // The enum has exactly 4 cases; if a future case is added, this test
    // will fail to compile (because of the @CaseIterable check below) —
    // which is the alarm we want.

    @Test("DiagnosticsMailComposeResult has exactly 4 cases (canary)")
    func enumExhaustivenessCanary() {
        #expect(DiagnosticsMailComposeResult.allCases.count == 4)
    }

    // MARK: - shouldReset(result:) — delivery-confirming predicate
    //
    // `shouldReset(result:)` MUST match `!newValue(current: true, result:)`
    // for every case. The coordinator relies on the two being equivalent.

    @Test("shouldReset(.sent) == true")
    func shouldResetSent() {
        #expect(DiagnosticsOptInResetPolicy.shouldReset(result: .sent) == true)
    }

    @Test("shouldReset(.saved) == true")
    func shouldResetSaved() {
        #expect(DiagnosticsOptInResetPolicy.shouldReset(result: .saved) == true)
    }

    @Test("shouldReset(.cancelled) == false")
    func shouldResetCancelled() {
        #expect(DiagnosticsOptInResetPolicy.shouldReset(result: .cancelled) == false)
    }

    @Test("shouldReset(.failed) == false")
    func shouldResetFailed() {
        #expect(DiagnosticsOptInResetPolicy.shouldReset(result: .failed) == false)
    }

    @Test("shouldReset(result:) is consistent with !newValue(current: true, result:) for all cases")
    func shouldResetConsistentWithNewValue() {
        for result in DiagnosticsMailComposeResult.allCases {
            let expected = !DiagnosticsOptInResetPolicy.newValue(current: true, result: result)
            let actual = DiagnosticsOptInResetPolicy.shouldReset(result: result)
            #expect(actual == expected, "Inconsistent for \(result): shouldReset=\(actual), !newValue(current:true)=\(expected)")
        }
    }
}
