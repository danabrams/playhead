// DiagnosticTextSanitizerTests.swift
// Unit coverage for the character allowlist and the unit-bearing
// measurement parsers that bound what a MetricKit diagnostic can
// contribute to the diagnostics bundle.
//
// Scope: playhead-jw63.4.

import Foundation
import Testing

@testable import Playhead

@Suite("DiagnosticTextSanitizer — allowlist + measurement parsing (playhead-jw63.4)")
struct DiagnosticTextSanitizerTests {

    // MARK: - identifier / versionToken

    @Test("symbol-shaped strings pass unchanged")
    func identifierAccepts() {
        #expect(DiagnosticTextSanitizer.identifier("Playhead") == "Playhead")
        #expect(DiagnosticTextSanitizer.identifier("libswift_Concurrency.dylib")
            == "libswift_Concurrency.dylib")
        #expect(DiagnosticTextSanitizer.identifier("NSInvalidArgumentException")
            == "NSInvalidArgumentException")
        #expect(DiagnosticTextSanitizer.identifier("$s8Playhead4mainyyF") == "$s8Playhead4mainyyF")
    }

    @Test("nil, empty, over-long, and out-of-allowlist inputs are rejected")
    func identifierRejects() {
        #expect(DiagnosticTextSanitizer.identifier(nil) == nil)
        #expect(DiagnosticTextSanitizer.identifier("") == nil)
        #expect(DiagnosticTextSanitizer.identifier("has/slash") == nil)
        #expect(DiagnosticTextSanitizer.identifier("has:colon") == nil)
        #expect(DiagnosticTextSanitizer.identifier("has\"quote") == nil)
        #expect(DiagnosticTextSanitizer.identifier("has,comma") == nil)
        #expect(
            DiagnosticTextSanitizer.identifier(
                String(repeating: "x", count: DiagnosticTextSanitizer.identifierMaxLength + 1)
            ) == nil
        )
        #expect(
            DiagnosticTextSanitizer.identifier(
                String(repeating: "x", count: DiagnosticTextSanitizer.identifierMaxLength)
            ) != nil,
            "the cap is inclusive"
        )
    }

    @Test("OS build strings pass versionToken; long prose does not")
    func versionTokenBounds() {
        #expect(DiagnosticTextSanitizer.versionToken("iPhone OS 27.0 (25A123)")
            == "iPhone OS 27.0 (25A123)")
        #expect(DiagnosticTextSanitizer.versionToken("1.0.0-rc1") == "1.0.0-rc1")
        #expect(
            DiagnosticTextSanitizer.versionToken(
                String(repeating: "a", count: DiagnosticTextSanitizer.versionMaxLength + 1)
            ) == nil
        )
    }

    // MARK: - deviceModel

    @Test("the Apple model grammar admits the comma; nothing else does")
    func deviceModelGrammar() {
        #expect(DiagnosticTextSanitizer.deviceModel("iPhone17,1") == "iPhone17,1")
        #expect(DiagnosticTextSanitizer.deviceModel("iPad14,10") == "iPad14,10")
        #expect(DiagnosticTextSanitizer.deviceModel("arm64") == "arm64")

        // Prose that merely contains a comma must NOT slip through the
        // one field allowed to carry one.
        #expect(DiagnosticTextSanitizer.deviceModel("Diary of a CEO, Episode 431") == nil)
        #expect(DiagnosticTextSanitizer.deviceModel("a,b,c") == nil)
        #expect(DiagnosticTextSanitizer.deviceModel("iPhone,17") == nil)
        #expect(DiagnosticTextSanitizer.deviceModel("iPhone17,10000") == nil)
        #expect(DiagnosticTextSanitizer.deviceModel(nil) == nil)
    }

    // MARK: - binaryUUID

    @Test("only a canonical UUID survives, normalised to uppercase")
    func binaryUUIDNormalisation() {
        #expect(DiagnosticTextSanitizer.binaryUUID("a1b2c3d4-e5f6-4708-9a0b-1c2d3e4f5061")
            == "A1B2C3D4-E5F6-4708-9A0B-1C2D3E4F5061")
        #expect(DiagnosticTextSanitizer.binaryUUID("not-a-uuid") == nil)
        #expect(DiagnosticTextSanitizer.binaryUUID(nil) == nil)
        #expect(DiagnosticTextSanitizer.binaryUUID("") == nil)
    }

    // MARK: - terminationReason decomposition

    @Test("the namespace token is extracted from the allowlist and nothing else is kept")
    func terminationNamespaceExtraction() {
        #expect(DiagnosticTextSanitizer.terminationNamespace(from: "Namespace SIGNAL, Code 0xb")
            == "SIGNAL")
        #expect(
            DiagnosticTextSanitizer.terminationNamespace(
                from: "<RBSTerminateContext| domain:10 code:0x8BADF00D explanation: RUNNINGBOARD>"
            ) == "RUNNINGBOARD"
        )
        // An unknown namespace is dropped, not passed through — that is
        // what stops a future iOS widening the channel with prose.
        #expect(DiagnosticTextSanitizer.terminationNamespace(from: "Namespace WHATEVER") == nil)
        #expect(DiagnosticTextSanitizer.terminationNamespace(from: nil) == nil)
    }

    @Test("the hex termination code is extracted, lowercased, and bounded")
    func terminationCodeExtraction() {
        #expect(DiagnosticTextSanitizer.terminationCode(from: "Namespace SIGNAL, Code 0xb") == "0xb")
        #expect(
            DiagnosticTextSanitizer.terminationCode(from: "code:0x8BADF00D explanation")
                == "0x8badf00d"
        )
        #expect(DiagnosticTextSanitizer.terminationCode(from: "no code here") == nil)
        #expect(DiagnosticTextSanitizer.terminationCode(from: "0x") == nil)
        // Bounded: a pathological run stops at 16 hex digits.
        let long = "0x" + String(repeating: "a", count: 40)
        #expect(DiagnosticTextSanitizer.terminationCode(from: long)?.count == 18)
    }

    @Test("a container path sharing the terminationReason string contributes nothing")
    func terminationDecompositionDropsPaths() {
        let reason = "Namespace SIGNAL, Code 0xb /private/var/mobile/Containers/ep-431.mp3"
        let namespace = DiagnosticTextSanitizer.terminationNamespace(from: reason)
        let code = DiagnosticTextSanitizer.terminationCode(from: reason)
        #expect(namespace == "SIGNAL")
        #expect(code == "0xb")
        #expect(namespace?.contains("/") != true)
        #expect(code?.contains("/") != true)
    }

    // MARK: - Measurements

    @Test("durations parse from ms, s, and bare numbers; anything else is nil")
    func durationParsing() {
        #expect(DiagnosticTextSanitizer.milliseconds(from: "3300 ms") == 3_300)
        #expect(DiagnosticTextSanitizer.milliseconds(from: "2.75 s") == 2_750)
        #expect(DiagnosticTextSanitizer.milliseconds(from: "500") == 500)
        #expect(DiagnosticTextSanitizer.milliseconds(from: NSNumber(value: 120)) == 120)
        #expect(DiagnosticTextSanitizer.milliseconds(from: "3300 fortnights") == nil)
        #expect(DiagnosticTextSanitizer.milliseconds(from: "not a number") == nil)
        #expect(DiagnosticTextSanitizer.milliseconds(from: nil) == nil)
        #expect(DiagnosticTextSanitizer.milliseconds(from: "-5 ms") == nil)
    }

    @Test("storage measurements normalise to MB")
    func storageParsing() {
        #expect(DiagnosticTextSanitizer.megabytes(from: "1024 MB") == 1_024)
        #expect(DiagnosticTextSanitizer.megabytes(from: "2 GB") == 2_048)
        #expect(DiagnosticTextSanitizer.megabytes(from: "512 kB") == 0.5)
        #expect(DiagnosticTextSanitizer.megabytes(from: "7 lightyears") == nil)
    }

    @Test("integers and booleans are read from numbers, not coerced from text")
    func scalarParsing() {
        #expect(DiagnosticTextSanitizer.integer(from: NSNumber(value: 11)) == 11)
        #expect(DiagnosticTextSanitizer.integer(from: "11") == 11)
        #expect(DiagnosticTextSanitizer.integer(from: "eleven") == nil)
        #expect(DiagnosticTextSanitizer.integer(from: nil) == nil)
        #expect(DiagnosticTextSanitizer.boolean(from: NSNumber(value: true)) == true)
        #expect(DiagnosticTextSanitizer.boolean(from: "false") == false)
        #expect(DiagnosticTextSanitizer.boolean(from: "maybe") == nil)
    }
}
