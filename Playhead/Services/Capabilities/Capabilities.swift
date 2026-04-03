// Capabilities.swift
// First-launch self-test and convenience extensions for CapabilitiesService.

import Foundation
import OSLog

extension CapabilitiesService {
    /// Runs a first-launch self-test: captures a snapshot and logs the result.
    /// Call from app initialization to verify capabilities detection works.
    func runSelfTest() async {
        let logger = Logger(subsystem: "com.playhead", category: "Capabilities")

        refreshSnapshot()
        let snapshot = currentSnapshot

        logger.notice("""
        [CapabilitiesService Self-Test] \
        Foundation Models: \(snapshot.canUseFoundationModels ? "available" : "unavailable") | \
        Thermal: \(snapshot.thermalState.rawValue) | \
        Low Power: \(snapshot.isLowPowerMode) | \
        Disk: \(snapshot.availableDiskSpaceBytes / (1024 * 1024))MB | \
        Throttle: \(snapshot.shouldThrottleAnalysis) | \
        Reduce Hot Path: \(snapshot.shouldReduceHotPath)
        """)

        // Verify round-trip encode/decode
        do {
            let encoder = JSONEncoder()
            encoder.dateEncodingStrategy = .iso8601
            let data = try encoder.encode(snapshot)

            let decoder = JSONDecoder()
            decoder.dateDecodingStrategy = .iso8601
            let decoded = try decoder.decode(CapabilitySnapshot.self, from: data)

            guard decoded == snapshot else {
                logger.error("[CapabilitiesService Self-Test] Round-trip mismatch!")
                return
            }
            logger.notice("[CapabilitiesService Self-Test] JSON round-trip OK (\(data.count) bytes)")
        } catch {
            logger.error("[CapabilitiesService Self-Test] Encode/decode failed: \(error)")
        }
    }
}
