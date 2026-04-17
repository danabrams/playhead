// AdmissionGateTests.swift
// Unit tests for the playhead-bnrs multi-resource admission gate.
//
// Coverage:
//   1. AND-gate truth table across (QualityProfile × network × storage × cpu)
//      — 16 (2×2×2×2) combinations.
//   2. Slice-sizing formula per device class from the playhead-dh9b table,
//      thermal derating (serious → 0.5×, critical → reject).
//   3. Cellular + maintenance → .wifiRequired; cellular + interactive →
//      admit with sliceBytes ≤ cellularSliceCapBytes.
//   4. Multi-class rejection → correct .mediaCap / .analysisCap cause.
//   5. Multi-cause simultaneous rejection → CauseAttributionPolicy picks
//      the primary (thermal > noNetwork per the policy's precedence ladder).

import Foundation
import Testing
@testable import Playhead

@Suite("AdmissionGate — playhead-bnrs multi-resource admission")
struct AdmissionGateTests {

    // MARK: - Fixtures

    /// A freshly-computed device profile for the default iPhone 17 Pro
    /// bucket. Uses the playhead-dh9b hard-coded fallback table so the
    /// tests remain stable if the JSON manifest drifts.
    private static let nominalDevice: DeviceClassProfile =
        DeviceClassProfile.fallback(for: .iPhone17Pro)

    /// A job that writes only to the media class, with a modest slice
    /// estimate. Used as the default in the truth-table suite.
    private static func mediaJob(bytes: Int64 = 1_000_000) -> AdmissionJob {
        AdmissionJob(artifactClasses: [.media], estimatedWriteBytes: bytes)
    }

    /// Transport snapshot: Wi-Fi, interactive. The "easy admit" case.
    private static let wifiTransport = TransportSnapshot(
        reachability: .wifi,
        session: .interactive,
        userAllowsCellular: true
    )

    /// Transport snapshot: cellular + user allows + interactive.
    private static let cellInteractiveTransport = TransportSnapshot(
        reachability: .cellular,
        session: .interactive,
        userAllowsCellular: true
    )

    /// Transport snapshot: cellular + maintenance (always rejected).
    private static let cellMaintenanceTransport = TransportSnapshot(
        reachability: .cellular,
        session: .maintenance,
        userAllowsCellular: true
    )

    /// Transport snapshot: unreachable (no network).
    private static let unreachableTransport = TransportSnapshot(
        reachability: .unreachable,
        session: .interactive,
        userAllowsCellular: true
    )

    /// Build a storage snapshot where every class admits with
    /// `remainingBytes` generous headroom. Mirrors the "plentiful
    /// storage" default.
    private static func plentifulStorage(
        remaining: Int64 = 5_000_000_000  // 5 GB
    ) -> StorageSnapshot {
        StorageSnapshot(
            canAdmit: [.media: true, .warmResumeBundle: true, .scratch: true],
            remainingBytes: [.media: remaining, .warmResumeBundle: remaining,
                             .scratch: remaining]
        )
    }

    /// Storage snapshot with the given class hitting its cap.
    private static func capReached(_ cls: ArtifactClass) -> StorageSnapshot {
        var canAdmit: [ArtifactClass: Bool] = [
            .media: true, .warmResumeBundle: true, .scratch: true,
        ]
        var remaining: [ArtifactClass: Int64] = [
            .media: 5_000_000_000, .warmResumeBundle: 100_000_000,
            .scratch: 100_000_000,
        ]
        canAdmit[cls] = false
        remaining[cls] = 0
        return StorageSnapshot(canAdmit: canAdmit, remainingBytes: remaining)
    }

    // MARK: - 1. AND-gate truth table (16 cells)

    /// A single truth-table cell. `ok` means the axis does NOT reject;
    /// `bad` means that axis fails admission.
    private struct Cell {
        let thermalOK: Bool  // true => QualityProfile.nominal (admit)
        let networkOK: Bool  // true => Wi-Fi (admit); false => unreachable
        let storageOK: Bool  // true => plentiful; false => media cap reached
        let cpuOK: Bool      // true => real profile; false => forced-0 device profile
        let expectAdmit: Bool
    }

    @Test("AND-gate truth table: 16 combinations of 4 axes (cpu axis is soft)")
    func testAndGateTruthTable() {
        // 16 cells: expectAdmit is true ONLY when every hard gate
        // admits. The CPU axis in Phase 1 is soft-only (it narrows
        // slice size) so CPU=bad does not produce a rejection; CPU=bad
        // cells still admit unless another gate rejects.
        var cells: [Cell] = []
        for thermalOK in [true, false] {
            for networkOK in [true, false] {
                for storageOK in [true, false] {
                    for cpuOK in [true, false] {
                        let anyHardRejection = !thermalOK || !networkOK || !storageOK
                        cells.append(Cell(
                            thermalOK: thermalOK,
                            networkOK: networkOK,
                            storageOK: storageOK,
                            cpuOK: cpuOK,
                            expectAdmit: !anyHardRejection
                        ))
                    }
                }
            }
        }

        #expect(cells.count == 16)

        for cell in cells {
            let profile: QualityProfile = cell.thermalOK ? .nominal : .critical
            let transport: TransportSnapshot = cell.networkOK
                ? Self.wifiTransport
                : Self.unreachableTransport
            let storage: StorageSnapshot = cell.storageOK
                ? Self.plentifulStorage()
                : Self.capReached(.media)
            // CPU axis: "bad" = a pathological device profile with
            // zero throughput. This narrows slice size to zero on
            // admit but does not itself reject in Phase 1.
            let deviceProfile: DeviceClassProfile = cell.cpuOK
                ? Self.nominalDevice
                : DeviceClassProfile(
                    deviceClass: DeviceClass.iPhone14andOlder.rawValue,
                    grantWindowMedianSeconds: 0,
                    grantWindowP95Seconds: 0,
                    nominalSliceSizeBytes: 0,
                    cpuWindowSeconds: 0,
                    bytesPerCpuSecond: 0,
                    avgShardDurationMs: 0
                )

            let decision = AdmissionGate.admit(
                job: Self.mediaJob(),
                profile: profile,
                deviceClass: .iPhone17Pro,
                deviceProfile: deviceProfile,
                storage: storage,
                transport: transport
            )

            switch decision {
            case .admit:
                #expect(cell.expectAdmit,
                        "cell thermal=\(cell.thermalOK) net=\(cell.networkOK) storage=\(cell.storageOK) cpu=\(cell.cpuOK) expected reject but admitted")
            case .reject:
                #expect(!cell.expectAdmit,
                        "cell thermal=\(cell.thermalOK) net=\(cell.networkOK) storage=\(cell.storageOK) cpu=\(cell.cpuOK) expected admit but rejected")
            }
        }
    }

    // MARK: - 2. Slice-sizing formula per device class

    @Test("Nominal profile + plentiful storage + Wi-Fi → sliceBytes == min(nominal, cpuCeiling) for every device class")
    func testSliceBytesEqualsNominalAcrossDeviceClasses() {
        // Under nominal QualityProfile (thermal derating = 1.0) and
        // plentiful storage on Wi-Fi, the binding constraint is
        // `min(nominalSliceSizeBytes, bytesPerCpuSecond × cpuWindowSeconds)`.
        // For some device classes the CPU ceiling is slightly below
        // the nominal size (e.g. iPhone 17: 628_000 × 35 = 21_980_000 vs
        // nominal 22_000_000). This is the documented behavior of the
        // playhead-dh9b profile table; the gate must honor the CPU
        // ceiling where it binds.
        for cls in DeviceClass.allCases {
            let profile = DeviceClassProfile.fallback(for: cls)
            // Storage headroom must NOT be the binding constraint; pick
            // a remaining size that is > 2 × nominal so `remaining / 2`
            // is well above the nominal.
            let headroom = Int64(profile.nominalSliceSizeBytes) * 4
            let storage = Self.plentifulStorage(remaining: headroom)
            let decision = AdmissionGate.admit(
                job: Self.mediaJob(),
                profile: .nominal,
                deviceClass: cls,
                deviceProfile: profile,
                storage: storage,
                transport: Self.wifiTransport
            )
            let cpuCeiling =
                Int64(profile.bytesPerCpuSecond) * Int64(profile.cpuWindowSeconds)
            let expected = min(Int64(profile.nominalSliceSizeBytes), cpuCeiling)
            switch decision {
            case .admit(let sliceBytes):
                #expect(
                    sliceBytes == expected,
                    "device class \(cls.rawValue): expected min(nominal=\(profile.nominalSliceSizeBytes), cpuCeiling=\(cpuCeiling)) = \(expected), got \(sliceBytes)"
                )
            case .reject:
                Issue.record("device class \(cls.rawValue): expected admit, got reject")
            }
        }
    }

    @Test("Serious QualityProfile halves slice size (sliceFraction=0.5)")
    func testSeriousProfileHalvesSlice() {
        let profile = Self.nominalDevice
        let headroom = Int64(profile.nominalSliceSizeBytes) * 4
        let storage = Self.plentifulStorage(remaining: headroom)
        let decision = AdmissionGate.admit(
            job: Self.mediaJob(),
            profile: .serious,
            deviceClass: .iPhone17Pro,
            deviceProfile: profile,
            storage: storage,
            transport: Self.wifiTransport
        )
        switch decision {
        case .admit(let sliceBytes):
            // Thermal ceiling = nominal * 0.5 (QualityProfile.serious.sliceFraction).
            let expected = Int64(Double(profile.nominalSliceSizeBytes) * 0.5)
            #expect(sliceBytes == expected,
                    "serious profile should halve slice: expected \(expected), got \(sliceBytes)")
        case .reject(let cause):
            Issue.record("serious profile should admit with halved slice, got reject(\(cause))")
        }
    }

    @Test("Critical QualityProfile rejects (pauseAllWork)")
    func testCriticalProfileRejects() {
        let decision = AdmissionGate.admit(
            job: Self.mediaJob(),
            profile: .critical,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.plentifulStorage(),
            transport: Self.wifiTransport
        )
        switch decision {
        case .admit:
            Issue.record("critical profile must reject")
        case .reject(let cause):
            #expect(cause == .thermal)
        }
    }

    // MARK: - 3. Cellular transport behavior

    @Test("Cellular + maintenance session → reject with .wifiRequired")
    func testCellularMaintenanceRejects() {
        let decision = AdmissionGate.admit(
            job: Self.mediaJob(),
            profile: .nominal,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.plentifulStorage(),
            transport: Self.cellMaintenanceTransport
        )
        switch decision {
        case .admit:
            Issue.record("cellular maintenance must reject")
        case .reject(let cause):
            #expect(cause == .wifiRequired)
        }
    }

    @Test("Cellular + interactive + user allows → admit, slice clamped to cellularSliceCapBytes")
    func testCellularInteractiveClampsSlice() {
        let decision = AdmissionGate.admit(
            job: Self.mediaJob(),
            profile: .nominal,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.plentifulStorage(),
            transport: Self.cellInteractiveTransport
        )
        switch decision {
        case .admit(let sliceBytes):
            #expect(sliceBytes <= AdmissionGate.cellularSliceCapBytes,
                    "cellular-interactive slice must be <= cellularSliceCapBytes (\(AdmissionGate.cellularSliceCapBytes)), got \(sliceBytes)")
            // And nominal device slice is much larger than 10 MiB, so
            // the cellular cap is the binding constraint here.
            #expect(sliceBytes == AdmissionGate.cellularSliceCapBytes,
                    "nominal iPhone 17 Pro slice (\(Self.nominalDevice.nominalSliceSizeBytes)) should be clamped to cellular cap (\(AdmissionGate.cellularSliceCapBytes)) — got \(sliceBytes)")
        case .reject(let cause):
            Issue.record("cellular interactive with userAllowsCellular must admit, got reject(\(cause))")
        }
    }

    @Test("Cellular + interactive + user disallows → reject with .wifiRequired")
    func testCellularInteractiveUserDisallowsRejects() {
        let transport = TransportSnapshot(
            reachability: .cellular,
            session: .interactive,
            userAllowsCellular: false
        )
        let decision = AdmissionGate.admit(
            job: Self.mediaJob(),
            profile: .nominal,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.plentifulStorage(),
            transport: transport
        )
        switch decision {
        case .admit:
            Issue.record("cellular + user disallows must reject")
        case .reject(let cause):
            #expect(cause == .wifiRequired)
        }
    }

    // MARK: - 4. Multi-class rejection surfaces the correct cause

    @Test("Media-class job with media cap reached → .mediaCap")
    func testMediaCapReachedCause() {
        let job = AdmissionJob(artifactClasses: [.media], estimatedWriteBytes: 1_000_000)
        let decision = AdmissionGate.admit(
            job: job,
            profile: .nominal,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.capReached(.media),
            transport: Self.wifiTransport
        )
        switch decision {
        case .admit:
            Issue.record("media cap reached must reject")
        case .reject(let cause):
            #expect(cause == .mediaCap)
        }
    }

    @Test("Analysis-class job with analysis cap reached → .analysisCap (via scratch)")
    func testAnalysisCapReachedCause() {
        let job = AdmissionJob(artifactClasses: [.scratch], estimatedWriteBytes: 1_000_000)
        let decision = AdmissionGate.admit(
            job: job,
            profile: .nominal,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.capReached(.scratch),
            transport: Self.wifiTransport
        )
        switch decision {
        case .admit:
            Issue.record("scratch cap reached must reject")
        case .reject(let cause):
            #expect(cause == .analysisCap)
        }
    }

    @Test("Analysis-class job with warmResumeBundle cap reached → .analysisCap")
    func testWarmResumeCapReachedCause() {
        let job = AdmissionJob(artifactClasses: [.warmResumeBundle], estimatedWriteBytes: 1_000_000)
        let decision = AdmissionGate.admit(
            job: job,
            profile: .nominal,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.capReached(.warmResumeBundle),
            transport: Self.wifiTransport
        )
        switch decision {
        case .admit:
            Issue.record("warmResumeBundle cap reached must reject")
        case .reject(let cause):
            #expect(cause == .analysisCap)
        }
    }

    @Test("Multi-class job (media + scratch) with ONLY analysis cap reached → .analysisCap")
    func testMultiClassAnalysisCapReachedCause() {
        let job = AdmissionJob(
            artifactClasses: [.media, .scratch],
            estimatedWriteBytes: 1_000_000
        )
        // Media admits, scratch does not.
        let storage = StorageSnapshot(
            canAdmit: [.media: true, .scratch: false, .warmResumeBundle: true],
            remainingBytes: [.media: 5_000_000_000, .scratch: 0,
                             .warmResumeBundle: 100_000_000]
        )
        let decision = AdmissionGate.admit(
            job: job,
            profile: .nominal,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: storage,
            transport: Self.wifiTransport
        )
        switch decision {
        case .admit:
            Issue.record("multi-class job with analysis cap reached must reject")
        case .reject(let cause):
            #expect(cause == .analysisCap)
        }
    }

    // MARK: - 5. Multi-cause simultaneous rejection uses CauseAttributionPolicy

    @Test("Thermal critical + no network → CauseAttributionPolicy resolves via precedence")
    func testMultiCauseResolvedViaPolicy() {
        // thermal is in `environmentalTransient` tier; noNetwork is
        // also `environmentalTransient`. Same-tier tie-break falls to
        // declaration order in `InternalMissCause.allCases`. `thermal`
        // is declared at index 2 in the enum ordering, `noNetwork` at
        // index 5 — so `thermal` wins the tie-break. We do NOT hardcode
        // precedence in this test; we compute the expected answer
        // directly from the policy so the assertion remains correct if
        // the ladder changes.
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 0
        )
        let expected = CauseAttributionPolicy.primaryCause(
            among: [.thermal, .noNetwork],
            context: context
        )
        #expect(expected != nil)

        let decision = AdmissionGate.admit(
            job: Self.mediaJob(),
            profile: .critical,  // thermal rejects
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.plentifulStorage(),
            transport: Self.unreachableTransport  // network rejects
        )
        switch decision {
        case .admit:
            Issue.record("thermal + noNetwork must reject")
        case .reject(let cause):
            #expect(cause == expected,
                    "CauseAttributionPolicy expected \(String(describing: expected)), AdmissionGate returned \(cause)")
        }
    }

    @Test("Thermal critical + media cap reached + no network → primary cause matches CauseAttributionPolicy")
    func testTripleCauseResolvedViaPolicy() {
        let context = CauseAttributionContext(
            modelAvailableNow: true,
            retryBudgetRemaining: 0
        )
        // The order of candidate causes mirrors the order AdmissionGate
        // assembles them: thermal, transport, storage.
        let expected = CauseAttributionPolicy.primaryCause(
            among: [.thermal, .noNetwork, .mediaCap],
            context: context
        )
        #expect(expected != nil)

        let decision = AdmissionGate.admit(
            job: Self.mediaJob(),
            profile: .critical,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: Self.capReached(.media),
            transport: Self.unreachableTransport
        )
        switch decision {
        case .admit:
            Issue.record("triple-cause admission must reject")
        case .reject(let cause):
            #expect(cause == expected)
        }
    }

    // MARK: - Slice-sizing: storage headroom is binding when remaining is small

    @Test("When storage headroom/2 is the smallest gate, slice equals remaining/2")
    func testStorageHeadroomIsBinding() {
        // Remaining is 4 MiB; /2 = 2 MiB, which is smaller than the
        // nominal iPhone 17 Pro slice (25 MB) and the cellular cap
        // (10 MiB). So the storage gate should be binding.
        let remaining: Int64 = 4 * 1024 * 1024
        let storage = StorageSnapshot(
            canAdmit: [.media: true, .scratch: true, .warmResumeBundle: true],
            remainingBytes: [.media: remaining, .scratch: remaining,
                             .warmResumeBundle: remaining]
        )
        let decision = AdmissionGate.admit(
            job: Self.mediaJob(),
            profile: .nominal,
            deviceClass: .iPhone17Pro,
            deviceProfile: Self.nominalDevice,
            storage: storage,
            transport: Self.wifiTransport
        )
        switch decision {
        case .admit(let sliceBytes):
            #expect(sliceBytes == remaining / 2,
                    "storage half-headroom (\(remaining / 2)) should be binding, got \(sliceBytes)")
        case .reject(let cause):
            Issue.record("expected admit under headroom path, got reject(\(cause))")
        }
    }
}
