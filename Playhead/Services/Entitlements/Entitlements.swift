// Entitlements.swift
// In-app purchase and feature entitlement management.
// Free preview + one-time purchase model.

import Foundation
import StoreKit
import OSLog

// MARK: - Product identifiers

enum PlayheadProduct {
    /// Non-consumable: unlocks premium (ad-skip + ad banners) forever.
    static let premiumUnlock = "com.playhead.premium"
}

// MARK: - EntitlementManager

/// Observes StoreKit 2 transactions to determine premium entitlement.
///
/// At launch, iterates `Transaction.currentEntitlements` for a silent unlock.
/// Stays subscribed to `Transaction.updates` so purchases made on other
/// devices (Family Sharing, App Store restores) propagate immediately.
///
/// Consumers subscribe to ``premiumUpdates`` for reactive state changes.
actor EntitlementManager {
    private let logger = Logger(subsystem: "com.playhead", category: "Entitlements")

    /// Current premium state. Read from the actor or subscribe to ``premiumUpdates``.
    private(set) var isPremium: Bool = false

    // AsyncStream plumbing
    private var continuations: [UUID: AsyncStream<Bool>.Continuation] = [:]

    /// Ongoing Task for Transaction.updates listener.
    private var updateListenerTask: Task<Void, Never>?

    // MARK: - Public API

    /// Returns an AsyncStream that emits the current value immediately,
    /// then every subsequent change to premium status.
    nonisolated var premiumUpdates: AsyncStream<Bool> {
        AsyncStream { continuation in
            let id = UUID()
            Task {
                await self.addContinuation(id: id, continuation: continuation)
                continuation.yield(await self.isPremium)
            }
            continuation.onTermination = { _ in
                Task { await self.removeContinuation(id: id) }
            }
        }
    }

    /// Call once at app launch. Checks current entitlements and starts
    /// listening for real-time transaction updates.
    func start() async {
        await checkCurrentEntitlements()
        startTransactionListener()
        logger.info("EntitlementManager started, isPremium=\(self.isPremium)")
    }

    /// Explicit restore triggered by a "Restore Purchases" button.
    /// Forces a sync with the App Store then re-checks entitlements.
    func restorePurchases() async throws {
        logger.info("User-initiated restore purchases")
        try await AppStore.sync()
        await checkCurrentEntitlements()
    }

    /// Request purchase of the premium unlock product.
    @discardableResult
    func purchasePremium() async throws -> Bool {
        let products = try await Product.products(for: [PlayheadProduct.premiumUnlock])
        guard let product = products.first else {
            logger.error("Premium product not found in App Store")
            return false
        }

        let result = try await product.purchase()

        switch result {
        case .success(let verification):
            let transaction = try checkVerification(verification)
            await transaction.finish()
            await updatePremiumState(true)
            logger.info("Purchase succeeded")
            return true
        case .userCancelled:
            logger.info("Purchase cancelled by user")
            return false
        case .pending:
            logger.info("Purchase pending (Ask to Buy or deferred)")
            return false
        @unknown default:
            logger.warning("Unknown purchase result")
            return false
        }
    }

    // MARK: - Internal

    private func addContinuation(
        id: UUID, continuation: AsyncStream<Bool>.Continuation
    ) {
        continuations[id] = continuation
    }

    private func removeContinuation(id: UUID) {
        continuations.removeValue(forKey: id)
    }

    /// Iterate all current entitlements for a silent unlock at launch.
    private func checkCurrentEntitlements() async {
        var foundPremium = false
        for await result in Transaction.currentEntitlements {
            if let transaction = try? checkVerification(result),
               transaction.productID == PlayheadProduct.premiumUnlock,
               transaction.revocationDate == nil
            {
                foundPremium = true
            }
        }
        await updatePremiumState(foundPremium)
    }

    /// Listen for real-time transaction updates (purchases on other devices,
    /// refunds, revocations, Family Sharing changes).
    private func startTransactionListener() {
        updateListenerTask?.cancel()
        updateListenerTask = Task(priority: .utility) { [weak self] in
            for await result in Transaction.updates {
                guard let self, !Task.isCancelled else { return }
                if let transaction = try? await self.checkVerification(result) {
                    await transaction.finish()
                    if transaction.productID == PlayheadProduct.premiumUnlock {
                        let granted = transaction.revocationDate == nil
                        await self.updatePremiumState(granted)
                    }
                }
            }
        }
    }

    private func updatePremiumState(_ newValue: Bool) async {
        guard isPremium != newValue else { return }
        isPremium = newValue
        logger.info("Premium state changed to \(newValue)")
        for (_, continuation) in continuations {
            continuation.yield(newValue)
        }
    }

    private func checkVerification<T>(
        _ result: VerificationResult<T>
    ) throws -> T {
        switch result {
        case .unverified(_, let error):
            logger.error("Transaction verification failed: \(error.localizedDescription)")
            throw error
        case .verified(let value):
            return value
        }
    }
}
