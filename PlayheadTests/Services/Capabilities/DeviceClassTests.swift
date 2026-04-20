// DeviceClassTests.swift
// playhead-dh9b landing-gate tests for DeviceClass detection.

import Foundation
import Testing

@testable import Playhead

@Suite("DeviceClass")
struct DeviceClassTests {

    // MARK: - classify: known utsname.machine strings

    @Test("iPhone 17 Pro / Pro Max map to .iPhone17Pro")
    func testClassifyiPhone17Pro() {
        #expect(DeviceClass.classify(machineIdentifier: "iPhone18,1") == .iPhone17Pro)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone18,2") == .iPhone17Pro)
    }

    @Test("iPhone 17 / 17 Plus / Air map to .iPhone17")
    func testClassifyiPhone17() {
        #expect(DeviceClass.classify(machineIdentifier: "iPhone18,3") == .iPhone17)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone18,4") == .iPhone17)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone18,5") == .iPhone17)
    }

    @Test("iPhone 16 Pro / Pro Max map to .iPhone16Pro")
    func testClassifyiPhone16Pro() {
        #expect(DeviceClass.classify(machineIdentifier: "iPhone17,1") == .iPhone16Pro)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone17,2") == .iPhone16Pro)
    }

    @Test("iPhone 16 / 16 Plus map to .iPhone16")
    func testClassifyiPhone16() {
        #expect(DeviceClass.classify(machineIdentifier: "iPhone17,3") == .iPhone16)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone17,4") == .iPhone16)
    }

    @Test("iPhone 15 Pro / Pro Max map to .iPhone15Pro")
    func testClassifyiPhone15Pro() {
        #expect(DeviceClass.classify(machineIdentifier: "iPhone16,1") == .iPhone15Pro)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone16,2") == .iPhone15Pro)
    }

    @Test("A16-era devices (iPhone 14 Pro + iPhone 15 non-Pro) map to .iPhone15")
    func testClassifyiPhone15A16Bucket() {
        // iPhone 14 Pro / Pro Max
        #expect(DeviceClass.classify(machineIdentifier: "iPhone15,2") == .iPhone15)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone15,3") == .iPhone15)
        // iPhone 15 / 15 Plus
        #expect(DeviceClass.classify(machineIdentifier: "iPhone15,4") == .iPhone15)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone15,5") == .iPhone15)
    }

    @Test("iPhone SE (3rd gen) maps to .iPhoneSE3")
    func testClassifyiPhoneSE3() {
        #expect(DeviceClass.classify(machineIdentifier: "iPhone14,6") == .iPhoneSE3)
    }

    // MARK: - classify: catch-all fallback

    @Test("Unknown iPhone identifiers fall back to .iPhone14andOlder")
    func testClassifyUnknownFallsBack() {
        // Real older devices (iPhone 14 generation and below).
        #expect(DeviceClass.classify(machineIdentifier: "iPhone14,7") == .iPhone14andOlder) // iPhone 14
        #expect(DeviceClass.classify(machineIdentifier: "iPhone14,8") == .iPhone14andOlder) // iPhone 14 Plus
        #expect(DeviceClass.classify(machineIdentifier: "iPhone13,2") == .iPhone14andOlder) // iPhone 12
        #expect(DeviceClass.classify(machineIdentifier: "iPhone11,8") == .iPhone14andOlder) // iPhone XR
        // Simulator identifiers.
        #expect(DeviceClass.classify(machineIdentifier: "x86_64") == .iPhone14andOlder)
        #expect(DeviceClass.classify(machineIdentifier: "arm64") == .iPhone14andOlder)
        // Garbage / empty strings.
        #expect(DeviceClass.classify(machineIdentifier: "") == .iPhone14andOlder)
        #expect(DeviceClass.classify(machineIdentifier: "not-a-device") == .iPhone14andOlder)
        #expect(DeviceClass.classify(machineIdentifier: "iPhone99,99") == .iPhone14andOlder)
    }

    // MARK: - detect() with injected provider

    @Test("detect() honors the injected machineProvider")
    func testDetectUsesProvider() {
        let detected = DeviceClass.detect(machineProvider: { "iPhone18,1" })
        #expect(detected == .iPhone17Pro)

        let fallbackDetected = DeviceClass.detect(machineProvider: { "unknown" })
        #expect(fallbackDetected == .iPhone14andOlder)
    }

    @Test("detect() default path returns a valid case on this host")
    func testDetectDefaultPathReturnsKnownCase() {
        // On simulator the default provider returns "arm64" or
        // "x86_64" and we should land in `.iPhone14andOlder`. On a
        // real device we should land on a real bucket. Either way the
        // returned value must be a valid case.
        let detected = DeviceClass.detect()
        #expect(DeviceClass.allCases.contains(detected))
    }

    @Test("currentMachineIdentifier returns a non-empty string")
    func testCurrentMachineIdentifierNonEmpty() {
        let machine = DeviceClass.currentMachineIdentifier()
        #expect(!machine.isEmpty)
    }

    // MARK: - Codable round-trip

    @Test("DeviceClass round-trips through JSON via rawValue")
    func testDeviceClassJSONRoundTrip() throws {
        for bucket in DeviceClass.allCases {
            let data = try JSONEncoder().encode(bucket)
            let decoded = try JSONDecoder().decode(DeviceClass.self, from: data)
            #expect(decoded == bucket)
        }
    }

    @Test("DeviceClass.allCases covers all eight documented buckets")
    func testAllCasesCount() {
        #expect(DeviceClass.allCases.count == 8)
    }
}
