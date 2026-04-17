// TransportStatusProviding.swift
// playhead-bnrs: transport-status surface consumed by AdmissionGate
// at admission time. Extracted from AnalysisWorkScheduler.swift so the
// scheduler file only contains the actor and its internal types.

import Foundation

/// Point-in-time transport-status query consumed by
/// `AdmissionGate.transportRejection` at admission time. The scheduler
/// synthesizes a `TransportSnapshot` per job from the reachability axis
/// returned here, the job's lane (which maps to interactive vs
/// maintenance), and the user's cellular preference.
///
/// playhead-bnrs ships the default `WifiTransportStatusProvider` — it
/// always reports `.wifi` / `allowsCellular = true` so production
/// behavior is unchanged while the admission gate is wired through.
/// playhead-ml96 will introduce a real `NWPathMonitor`-backed provider
/// and a user-preferences adapter; widening `CapabilitySnapshot` to
/// carry the network axis was explicitly out of scope for this bead
/// (see the spec's "scope creep" section).
protocol TransportStatusProviding: Sendable {
    /// The current reachability of the active network path.
    func currentReachability() async -> TransportSnapshot.Reachability
    /// Whether the user has opted into cellular transfers for
    /// interactive jobs. Maintenance jobs ignore this value — they are
    /// Wi-Fi-only regardless.
    func userAllowsCellular() async -> Bool
}

/// Conservative default: assume Wi-Fi + user allows cellular. Matches
/// the pre-bnrs behavior where no transport gate was consulted at all,
/// so the admission gate wire does not regress production scheduling
/// until a real reachability provider lands (playhead-ml96).
struct WifiTransportStatusProvider: TransportStatusProviding {
    func currentReachability() async -> TransportSnapshot.Reachability { .wifi }
    func userAllowsCellular() async -> Bool { true }
}
