// UserDefaultsNewEpisodeLedger.swift
// playhead-snp — Persistent dedup ledger for the new-episode notifier.
// Backed by UserDefaults so the dedup decision survives app restart
// (the SwiftData Episode-existence check already does fine-grained dedup
// at discovery time, but a process-pinned set is what stops a same-
// session re-fetch loop or a partial-failure retry from announcing the
// same episode twice).
//
// Bounded by `capacity` (default 500 entries) with LRU-style eviction
// of the oldest insertion. We use an ordered array as the backing store
// — Set lookups for `contains` are still O(N) at this size (≤ 500), but
// the linear scan is dwarfed by the IPC cost of UserDefaults itself.
//
// Storage layout: a single key
// `playhead.newEpisodeNotifications.ledgerKeys` holds an `[String]`.
// Reading and writing are atomic from the consumer's POV because
// UserDefaults serializes its access internally.

import Foundation
import OSLog

/// MainActor-isolated bounded LRU dedup ledger persisted in UserDefaults.
/// Conforms to `NewEpisodeNotificationScheduler.DedupLedger`.
@MainActor
final class UserDefaultsNewEpisodeLedger: NewEpisodeNotificationScheduler.DedupLedger {

    private let logger = Logger(subsystem: "com.playhead", category: "NewEpisodeLedger")
    private let defaults: UserDefaults
    private let capacity: Int
    private let storageKey: String

    init(
        defaults: UserDefaults = .standard,
        capacity: Int = 500,
        storageKey: String = "playhead.newEpisodeNotifications.ledgerKeys"
    ) {
        self.defaults = defaults
        self.capacity = max(1, capacity)
        self.storageKey = storageKey
    }

    // MARK: - Read

    func contains(_ key: String) -> Bool {
        currentKeys().contains(key)
    }

    var count: Int { currentKeys().count }

    // MARK: - Write

    func record(_ key: String) {
        var keys = currentKeys()
        // Idempotent: if already present, leave the existing (older)
        // position alone. The user-visible behavior is "we have already
        // announced this episode", and refreshing the position would
        // make the LRU eviction kick the wrong neighbor out.
        guard !keys.contains(key) else { return }

        keys.append(key)
        // Evict the oldest until we are within capacity.
        while keys.count > capacity {
            keys.removeFirst()
        }
        defaults.set(keys, forKey: storageKey)
    }

    func clear() {
        defaults.removeObject(forKey: storageKey)
    }

    // MARK: - Helpers

    private func currentKeys() -> [String] {
        guard let raw = defaults.object(forKey: storageKey) else { return [] }
        guard let keys = raw as? [String] else {
            // Stored under the same key but the wrong shape — most
            // likely a future-format migration we don't recognize, or
            // user-driven defaults corruption. Log so the failure is
            // visible in Console; reset the slot so we don't keep
            // tripping the cast on every call. Behavior on corruption:
            // any episode that flows through a fresh discovery pass
            // before its key is re-recorded MAY re-announce. The
            // outer SwiftData Episode-existence check at the discovery
            // boundary still suppresses already-known episodes, so
            // the practical exposure is bounded to the (rare)
            // discovery-without-existence-check path.
            logger.error("Ledger value at \(self.storageKey, privacy: .public) is not [String]; resetting.")
            defaults.removeObject(forKey: storageKey)
            return []
        }
        return keys
    }
}
