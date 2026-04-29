// TransportStatusProviding.swift
// playhead-bnrs: transport-status surface consumed by AdmissionGate
// at admission time. Extracted from AnalysisWorkScheduler.swift so the
// scheduler file only contains the actor and its internal types.
//
// playhead-ml96: introduce `LiveTransportStatusProvider` — an
// `NWPathMonitor`-backed provider that reports actual reachability and
// reads the user's cellular preference via `UserPreferencesSnapshot`.
// `WifiTransportStatusProvider` is retained as a deterministic stub for
// preview builds and unit-test fixtures.

import Foundation
import Network
import os.lock

/// Point-in-time transport-status query consumed by
/// `AdmissionGate.transportRejection` at admission time. The scheduler
/// synthesizes a `TransportSnapshot` per job from the reachability axis
/// returned here, the job's lane (which maps to interactive vs
/// maintenance), and the user's cellular preference.
///
/// playhead-bnrs shipped the default `WifiTransportStatusProvider` — it
/// always reports `.wifi` / `allowsCellular = true`. playhead-ml96
/// promotes `LiveTransportStatusProvider` to the production default so
/// the cellular and unreachable paths actually fire.
protocol TransportStatusProviding: Sendable {
    /// The current reachability of the active network path.
    func currentReachability() async -> TransportSnapshot.Reachability
    /// Whether the user has opted into cellular transfers for
    /// interactive jobs. Maintenance jobs ignore this value — they are
    /// Wi-Fi-only regardless.
    func userAllowsCellular() async -> Bool
}

/// Deterministic stub: assume Wi-Fi + user allows cellular. Retained
/// post-ml96 for SwiftUI previews and as a unit-test fixture default.
/// Production callers use `LiveTransportStatusProvider`.
struct WifiTransportStatusProvider: TransportStatusProviding {
    func currentReachability() async -> TransportSnapshot.Reachability { .wifi }
    func userAllowsCellular() async -> Bool { true }
}

// MARK: - LiveTransportStatusProvider

/// `NWPathMonitor`-backed transport status. Wraps a long-lived monitor
/// on a private background queue, caches the most recent path under a
/// lock, and reads the user's `allowsCellular` preference via an
/// injected closure (defaulting to `UserPreferencesSnapshot.current`,
/// which mirrors `UserPreferences.allowsCellular` into UserDefaults).
///
/// Path → Reachability mapping:
/// - `.satisfied` && !isExpensive && !cellular interface → `.wifi`
/// - `.satisfied` && (isExpensive || cellular interface)  → `.cellular`
/// - otherwise                                            → `.unreachable`
///
/// `isExpensive == true` covers personal hotspots and other
/// metered transports that are reported as Wi-Fi by interface type but
/// are billed as cellular by the carrier; treating them as `.cellular`
/// honors the spirit of `allowsCellular` (avoid surprise data charges).
///
/// Sendable: state is guarded by an `OSAllocatedUnfairLock` (async-
/// safe; usable from `async` methods unlike `NSLock`). The monitor
/// itself is held privately and never escapes; `NWPathMonitor` isn't
/// formally `Sendable` so the class is `@unchecked Sendable` — access
/// to the monitor object is constrained to `init`/`deinit` and to its
/// own dispatch queue inside the path update handler.
final class LiveTransportStatusProvider: TransportStatusProviding, @unchecked Sendable {
    private let monitor: NWPathMonitor
    private let queue: DispatchQueue
    /// We store the mapped `Reachability` (a Sendable enum) rather than
    /// the raw `NWPath` so the lock's wrapped state is unambiguously
    /// Sendable under Swift 6 concurrency. The path → reachability
    /// mapping happens inside the update handler on `queue`.
    private let latestReachability: OSAllocatedUnfairLock<TransportSnapshot.Reachability>
    private let allowsCellularProvider: @Sendable () -> Bool

    /// - Parameters:
    ///   - monitor: NWPathMonitor instance (defaults to a fresh one).
    ///     Injectable for tests that want to assert the type composes.
    ///   - allowsCellularProvider: synchronous closure that returns the
    ///     user's current `allowsCellular` preference. Defaults to
    ///     `UserPreferencesSnapshot.current.allowsCellular`, which
    ///     reads UserDefaults (cheap, thread-safe).
    init(
        monitor: NWPathMonitor = NWPathMonitor(),
        allowsCellularProvider: @escaping @Sendable () -> Bool = {
            UserPreferencesSnapshot.current.allowsCellular
        }
    ) {
        self.monitor = monitor
        self.queue = DispatchQueue(
            label: "com.playhead.LiveTransportStatusProvider",
            qos: .utility
        )
        self.allowsCellularProvider = allowsCellularProvider
        // Seed reachability synchronously from `monitor.currentPath` so
        // the first admission pass after init doesn't fall through to
        // `.unreachable` during the boot-time race.
        // NWPathMonitor's `currentPath` is safe to read before
        // `start(...)` returns.
        let seed = Self.reachability(from: monitor.currentPath)
        let lock = OSAllocatedUnfairLock<TransportSnapshot.Reachability>(initialState: seed)
        self.latestReachability = lock
        monitor.pathUpdateHandler = { path in
            let mapped = Self.reachability(from: path)
            lock.withLock { $0 = mapped }
        }
        monitor.start(queue: queue)
    }

    deinit {
        monitor.cancel()
    }

    func currentReachability() async -> TransportSnapshot.Reachability {
        latestReachability.withLock { $0 }
    }

    func userAllowsCellular() async -> Bool {
        allowsCellularProvider()
    }

    /// Pure mapping function — extracted so unit tests can probe the
    /// truth table without driving `NWPathMonitor`.
    static func reachability(from path: NWPath?) -> TransportSnapshot.Reachability {
        guard let path else { return .unreachable }
        guard path.status == .satisfied else { return .unreachable }
        if path.isExpensive || path.usesInterfaceType(.cellular) {
            return .cellular
        }
        return .wifi
    }
}
