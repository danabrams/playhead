// LiveEnvironmentSignalProvider.swift
// playhead-narl.2: Live `ShadowEnvironmentSignalProvider` backed by
// `CapabilitiesService`.
//
// The coordinator's protocol requires SYNCHRONOUS getters
// (`thermalStateIsNominal()` and `deviceIsCharging()`). `CapabilitiesService`
// is an actor whose `currentSnapshot` is isolated, so the live provider
// maintains a lock-protected snapshot refreshed by a long-running consumer
// task subscribed to `capabilityUpdates()`.
//
// Q2 decision (narl.2 continuation): the provider wraps
// `CapabilitiesService`. The protocol's docstring on
// `thermalStateIsNominal()` says "true iff ProcessInfo.processInfo.thermalState
// == .nominal"; narl.2 spec widens this to ".nominal or .fair" so Lane B
// still fires on lightly-loaded devices (as opposed to only on genuinely
// cold ones). The coordinator's unit tests do not constrain the thermal
// threshold — they flip the leg via a stub — so broadening the live
// threshold here does not regress any coverage.
//
// `deviceIsCharging()` is true when `isCharging` is true in the snapshot;
// `CapabilitySnapshot.isCharging` already folds `.charging || .full` per
// its existing definition, so the semantics match the protocol contract
// ("unplugged-but-full is accepted").

import Foundation
import os

/// Live `ShadowEnvironmentSignalProvider` backed by `CapabilitiesService`.
final class LiveEnvironmentSignalProvider: ShadowEnvironmentSignalProvider,
    @unchecked Sendable
{
    private struct Snapshot: Sendable {
        /// `true` when `thermalState` is `.nominal` or `.fair`. Broadened
        /// from the coordinator protocol's original `.nominal`-only
        /// threshold per narl.2 Q2 guidance — Lane B should still fire on
        /// lightly-loaded devices, not only on genuinely cold ones.
        var thermalNominal: Bool = false
        /// `true` when the device is `.charging` or `.full` (the snapshot's
        /// `isCharging` already folds both states together).
        var charging: Bool = false
    }

    private let state: OSAllocatedUnfairLock<Snapshot>
    private let observerTask: Task<Void, Never>

    /// - Parameter capabilitiesService: Source of the reactive capability
    ///   snapshot stream. The provider subscribes to
    ///   `capabilityUpdates()`, which yields the current snapshot
    ///   immediately on subscription, so the first synchronous read after
    ///   construction reflects a real reading.
    init(capabilitiesService: CapabilitiesService) {
        let initial = Snapshot()
        let box = OSAllocatedUnfairLock(initialState: initial)
        self.state = box
        self.observerTask = Task { [box, capabilitiesService] in
            let stream = await capabilitiesService.capabilityUpdates()
            for await snapshot in stream {
                let thermalNominal = Self.thermalIsNominal(snapshot.thermalState)
                box.withLock { snap in
                    snap.thermalNominal = thermalNominal
                    snap.charging = snapshot.isCharging
                }
                if Task.isCancelled { break }
            }
        }
    }

    // MARK: - Thermal mapping (Q2=B)

    /// Pure mapping from the capability snapshot's `ThermalState` to the
    /// `thermalStateIsNominal()` boolean leg.
    ///
    /// `.nominal` and `.fair` are both treated as "nominal" per narl.2
    /// Q2=B — Lane B should still fire on lightly-loaded devices, not
    /// only genuinely cold ones. `.serious` and `.critical` are hard
    /// refusals. Extracted as a static helper so this mapping — which
    /// encodes the locked Q2=B decision — is unit-testable without
    /// driving the full `capabilityUpdates()` stream.
    static func thermalIsNominal(_ state: ThermalState) -> Bool {
        switch state {
        case .nominal, .fair: return true
        case .serious, .critical: return false
        }
    }

    deinit {
        observerTask.cancel()
    }

    // MARK: - ShadowEnvironmentSignalProvider

    func thermalStateIsNominal() -> Bool {
        state.withLock { $0.thermalNominal }
    }

    func deviceIsCharging() -> Bool {
        state.withLock { $0.charging }
    }

    // MARK: - Test hooks

    #if DEBUG
    /// Test-only: override the snapshot directly.
    func _testingSetSnapshot(thermalNominal: Bool, charging: Bool) {
        state.withLock { snap in
            snap.thermalNominal = thermalNominal
            snap.charging = charging
        }
    }
    #endif
}
