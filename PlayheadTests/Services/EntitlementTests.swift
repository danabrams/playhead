// EntitlementTests.swift
// Unit tests for EntitlementManager and PreviewBudgetStore (playhead-4ae).
//
// EntitlementManager wraps StoreKit 2 (Transaction.currentEntitlements,
// Transaction.updates, AppStore.sync) which cannot be unit-tested without
// a StoreKit configuration file running in Xcode's test host. The tests
// below cover the testable surface — state management, stream plumbing,
// and the actor's internal logic — by exercising the public API where
// StoreKit calls are not required.
//
// PreviewBudgetStore is fully testable via in-memory AnalysisStore and
// is covered both here (episode-key isolation, cumulative consumption)
// and in CoreServiceTests.swift (budget enforcement, grace window).

import Foundation
import Testing
@testable import Playhead

// MARK: - EntitlementManager

@Suite("EntitlementManager - State & Stream")
struct EntitlementManagerTests {

    @Test("Default state is not premium")
    func defaultStateNotPremium() async {
        let manager = EntitlementManager()
        let isPremium = await manager.isPremium
        #expect(isPremium == false,
                "Fresh EntitlementManager must default to non-premium")
    }

    @Test("premiumUpdates stream emits current value immediately")
    func streamEmitsCurrentValue() async {
        let manager = EntitlementManager()

        // The stream should yield the current value (false) as its first element.
        var received: Bool?
        for await value in manager.premiumUpdates {
            received = value
            break  // Take only the first emission.
        }

        #expect(received == false,
                "premiumUpdates must emit the current isPremium value immediately")
    }

    @Test("Multiple subscribers each receive the initial value")
    func multipleSubscribers() async {
        let manager = EntitlementManager()

        // Create two independent streams and verify both get the initial value.
        var firstValue: Bool?
        for await value in manager.premiumUpdates {
            firstValue = value
            break
        }

        var secondValue: Bool?
        for await value in manager.premiumUpdates {
            secondValue = value
            break
        }

        #expect(firstValue == false)
        #expect(secondValue == false)
    }

    // NOTE: Testing premium = true via premiumUpdates requires either:
    // - A StoreKit configuration file (.storekit) in the test plan, or
    // - Making updatePremiumState internal/package for test seeding.
    //
    // The following behaviors require StoreKit sandbox and cannot be
    // unit-tested without a .storekit config:
    //   - restorePurchases() calls AppStore.sync then re-checks entitlements
    //   - purchasePremium() handles .success, .userCancelled, .pending
    //   - checkCurrentEntitlements() iterates Transaction.currentEntitlements
    //   - startTransactionListener() responds to Transaction.updates
    //   - Revocation (revocationDate != nil) revokes premium
    //
    // These are exercised by the PreviewBudgetExhaustedTests integration
    // suite using Xcode's StoreKit testing infrastructure.
}

// MARK: - PreviewBudgetStore: Episode Key Isolation

@Suite("PreviewBudgetStore - Episode Key Isolation")
struct PreviewBudgetKeyIsolationTests {

    @Test("Different episode keys have independent budgets")
    func independentBudgets() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Consume budget on episode A.
        _ = await budgetStore.consumeBudget(for: "ep-A", seconds: 300)

        // Episode B should still have full budget.
        let remainingB = await budgetStore.remainingBudget(for: "ep-B")
        #expect(remainingB == 720.0,
                "Unrelated episode must retain full budget")

        // Episode A should reflect consumption.
        let remainingA = await budgetStore.remainingBudget(for: "ep-A")
        #expect(remainingA == 420.0)
    }

    @Test("Dynamic ad variants sharing canonical key share one budget")
    func canonicalKeySharing() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Two different playback URLs that resolve to the same canonical key
        // would pass the same key here. Verify cumulative tracking.
        let sharedKey = "podcast-feed://show/ep-42"
        _ = await budgetStore.consumeBudget(for: sharedKey, seconds: 200)
        _ = await budgetStore.consumeBudget(for: sharedKey, seconds: 200)

        let remaining = await budgetStore.remainingBudget(for: sharedKey)
        #expect(remaining == 320.0,
                "Same canonical key must accumulate consumption")

        let total = await budgetStore.totalConsumed(for: sharedKey)
        #expect(total == 400.0)
    }

    @Test("Exhausting one episode does not affect another")
    func exhaustionIsolation() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Exhaust episode A.
        _ = await budgetStore.consumeBudget(for: "ep-exhausted", seconds: 720)
        let hasA = await budgetStore.hasBudget(for: "ep-exhausted")
        #expect(hasA == false)

        // Episode B is untouched.
        let hasB = await budgetStore.hasBudget(for: "ep-fresh")
        #expect(hasB == true)
        let remainingB = await budgetStore.remainingBudget(for: "ep-fresh")
        #expect(remainingB == 720.0)
    }
}

// MARK: - PreviewBudgetStore: Consumption Tracking

@Suite("PreviewBudgetStore - Consumption Tracking")
struct PreviewBudgetConsumptionTests {

    @Test("Multiple small consumptions accumulate correctly")
    func incrementalConsumption() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Simulate shard-by-shard consumption (30s shards).
        for _ in 0..<10 {
            _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 30)
        }

        let total = await budgetStore.totalConsumed(for: "ep-1")
        #expect(total == 300.0)

        let remaining = await budgetStore.remainingBudget(for: "ep-1")
        #expect(remaining == 420.0)
    }

    @Test("consumeBudget return value matches subsequent remainingBudget query")
    func returnValueConsistency() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        let returned = await budgetStore.consumeBudget(for: "ep-1", seconds: 500)
        let queried = await budgetStore.remainingBudget(for: "ep-1")
        #expect(returned == queried,
                "consumeBudget return must equal subsequent remainingBudget")
        #expect(returned == 220.0)
    }

    @Test("Consuming beyond base budget returns negative remaining")
    func overConsumption() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        let remaining = await budgetStore.consumeBudget(for: "ep-1", seconds: 800)
        #expect(remaining == -80.0,
                "consumeBudget may return negative when overdrawn")

        // But remainingBudget clamps to zero.
        let clamped = await budgetStore.remainingBudget(for: "ep-1")
        #expect(clamped == 0,
                "remainingBudget must clamp to zero, never negative")
    }
}

// MARK: - PreviewBudgetStore: Grace Window Edge Cases

@Suite("PreviewBudgetStore - Grace Window Edge Cases")
struct PreviewBudgetGraceEdgeCaseTests {

    @Test("Grace at exactly one second below base budget")
    func graceAtOneSecondBelow() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 719)

        // 1s remaining in base budget. Headroom = 1200 - 719 = 481.
        let grace = await budgetStore.graceAllowance(
            for: "ep-1", adBreakDuration: 120
        )
        #expect(grace == 120.0,
                "Full ad break should be allowed when headroom exceeds duration")
    }

    @Test("Grace window with very large ad break is capped at headroom")
    func graceCapToHeadroom() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Consume 1190s (10s from absolute cap).
        // But wait — 1190 > 720 base budget, so grace should be denied.
        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 1190)
        let grace = await budgetStore.graceAllowance(
            for: "ep-1", adBreakDuration: 600
        )
        #expect(grace == 0,
                "No grace when consumed >= baseBudgetSeconds")
    }

    @Test("Grace for zero-duration ad break returns zero")
    func zeroDurationAdBreak() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 600)
        let grace = await budgetStore.graceAllowance(
            for: "ep-1", adBreakDuration: 0
        )
        #expect(grace == 0,
                "Zero-duration ad break needs no grace")
    }

    @Test("Grace on fresh episode (no consumption)")
    func graceOnFreshEpisode() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Fresh episode: consumed = 0, which is < baseBudgetSeconds.
        // Headroom = 1200 - 0 = 1200.
        let grace = await budgetStore.graceAllowance(
            for: "ep-fresh", adBreakDuration: 60
        )
        #expect(grace == 60.0,
                "Grace should be granted on fresh episode (consumed < base)")
    }

    @Test("Grace headroom capped at maxBudgetWithGraceSeconds minus consumed")
    func graceHeadroomCap() async throws {
        let store = try await makeTestStore()
        let budgetStore = PreviewBudgetStore(analysisStore: store)

        // Consume 710s. Headroom = 1200 - 710 = 490.
        // Request grace for 500s ad break — should be capped at 490.
        _ = await budgetStore.consumeBudget(for: "ep-1", seconds: 710)
        let grace = await budgetStore.graceAllowance(
            for: "ep-1", adBreakDuration: 500
        )
        #expect(grace == 490.0,
                "Grace must be capped at remaining headroom under absolute cap")
    }
}

// MARK: - PreviewBudgetStore: Constants

@Suite("PreviewBudgetStore - Constants")
struct PreviewBudgetConstantsTests {

    @Test("Base budget is 12 minutes (720 seconds)")
    func baseBudgetConstant() {
        #expect(PreviewBudgetStore.baseBudgetSeconds == 720.0)
    }

    @Test("Max budget with grace is 20 minutes (1200 seconds)")
    func maxBudgetConstant() {
        #expect(PreviewBudgetStore.maxBudgetWithGraceSeconds == 1200.0)
    }
}
