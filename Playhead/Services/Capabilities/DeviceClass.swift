// DeviceClass.swift
// Coarse device-class buckets used by playhead-dh9b for per-device
// slice-sizing and grant-window prediction. The bucket list matches
// Phase 0 sign-off (plan §7 decision #6). Unknown hardware strings
// fall through to `.iPhone14andOlder` (conservative).
//
// The enum is intentionally a plain Sendable, Codable, CaseIterable
// value type so tests can round-trip it through JSON and inject a
// fake `utsname.machine` reader via `detect(machineProvider:)`.
//
// Consumers:
//   - playhead-bnrs  (cpuWindowSeconds / bytesPerCpuSecond / nominalSliceSizeBytes)
//   - playhead-44h1  (avgShardDurationMs for Live Activity ETA)
//   - playhead-uzdq  (emits `device_class` in WorkJournal metadata)
//   - playhead-beh3  (Phase 3 adaptive estimator; out of scope for dh9b)

import Foundation

enum DeviceClass: String, Sendable, Codable, CaseIterable, Equatable {
    case iPhone17Pro
    case iPhone17
    case iPhone16Pro
    case iPhone16
    case iPhone15Pro
    case iPhoneSE3
    /// Catch-all for A15-era and older hardware, simulator, and any
    /// unrecognized `utsname.machine` string.
    case iPhone14andOlder

    // MARK: - Detection

    /// Detect the current device's class from `utsname.machine`.
    /// Uses the real syscall by default; tests pass a custom provider
    /// to exercise arbitrary hardware strings without mocking
    /// Foundation.
    static func detect(machineProvider: () -> String = Self.currentMachineIdentifier) -> DeviceClass {
        let identifier = machineProvider()
        return classify(machineIdentifier: identifier)
    }

    /// Pure mapping from `utsname.machine` to `DeviceClass`. Split out
    /// from `detect()` so tests can exercise the decision table
    /// directly with no syscall.
    static func classify(machineIdentifier: String) -> DeviceClass {
        // iPhone hardware generations follow "iPhoneN,M" where N is the
        // chassis generation. The mapping below is conservative: any
        // unrecognized string falls through to `.iPhone14andOlder`.
        switch machineIdentifier {
        // iPhone 17 Pro / Pro Max — chassis iPhone18,*
        case "iPhone18,1", "iPhone18,2":
            return .iPhone17Pro
        // iPhone 17 / 17 Plus / iPhone Air — chassis iPhone18,*
        case "iPhone18,3", "iPhone18,4", "iPhone18,5":
            return .iPhone17
        // iPhone 16 Pro / Pro Max — chassis iPhone17,1 / iPhone17,2
        case "iPhone17,1", "iPhone17,2":
            return .iPhone16Pro
        // iPhone 16 / 16 Plus — chassis iPhone17,3 / iPhone17,4
        case "iPhone17,3", "iPhone17,4":
            return .iPhone16
        // iPhone 15 Pro / Pro Max — chassis iPhone16,1 / iPhone16,2
        case "iPhone16,1", "iPhone16,2":
            return .iPhone15Pro
        // iPhone SE (3rd generation) — chassis iPhone14,6
        case "iPhone14,6":
            return .iPhoneSE3
        default:
            return .iPhone14andOlder
        }
    }

    /// Reads `utsname.machine` via the Foundation `uname()` syscall.
    /// Exposed as `internal` so the fallback path in `detect` has a
    /// stable target, and so tests can verify it returns a non-empty
    /// string on simulator / device.
    static func currentMachineIdentifier() -> String {
        var systemInfo = utsname()
        uname(&systemInfo)
        let machineMirror = Mirror(reflecting: systemInfo.machine)
        let identifier = machineMirror.children.reduce(into: "") { partialResult, element in
            guard let value = element.value as? Int8, value != 0 else { return }
            partialResult.append(Character(UnicodeScalar(UInt8(value))))
        }
        return identifier
    }
}
