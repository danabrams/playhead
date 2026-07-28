// StabilityDiagnosticScrubbingTests.swift
// The privacy proof for the crash + hang pipeline.
//
// Scope: playhead-jw63.4, legal checklist item (e).
//
// Playhead's on-device mandate says no transcript text, no ad-candidate
// text, no episode titles and no feed URLs leave the device. A crash
// pipeline is the one subsystem that can breach that without anyone
// writing code to do it, because iOS hands us free-text fields
// (`exceptionReason.composedMessage`, `terminationReason`,
// `virtualMemoryRegionInfo`) that routinely contain interpolated
// application strings and absolute container paths.
//
// The claim under test is four-part, and each part is a separate test
// because each could regress on its own:
//
//   1. SENTINEL SWEEP — a payload deliberately stuffed with transcript
//      text, an episode title, a feed URL, and a container path
//      projects to records whose encoded bytes contain none of them.
//   2. STRUCTURAL INVARIANT — every string in an encoded record stays
//      inside the sanitiser's character allowlist, so no URL, no POSIX
//      path, and no non-ASCII text can be represented at all.
//   3. CLOSED SHAPE — an encoded record's key set is exactly the
//      declared `CodingKeys`. A future free-text field cannot be added
//      without turning this red.
//   4. ALLOWLIST CANARY — the projector's source reads only the
//      metadata keys it declares, so the leaky keys stay unread.
//
// Test 1 alone would be satisfiable by a lucky denylist. Tests 2-4 are
// what make the claim structural rather than anecdotal.

import Foundation
import Testing

@testable import Playhead

@Suite("Stability diagnostics — scrubbing (playhead-jw63.4, legal item e)")
struct StabilityDiagnosticScrubbingTests {

    private static let receivedAt = Date(timeIntervalSince1970: 1_800_000_000)

    /// Project the hostile fixture: every free-text field seeded with a
    /// sentinel shaped like forbidden content.
    private func leakyRecords() throws -> [StabilityDiagnosticRecord] {
        let payload = MetricKitPayloadFixture.payload(
            crashes: [MetricKitPayloadFixture.crashDiagnostic(leaky: true)],
            hangs: [MetricKitPayloadFixture.hangDiagnostic()]
        )
        let data = try MetricKitPayloadFixture.payloadData(payload)
        return MetricKitDiagnosticProjector.records(
            fromPayloadJSON: data,
            receivedAt: Self.receivedAt
        )
    }

    private func encode(_ records: [StabilityDiagnosticRecord]) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(records)
    }

    // MARK: - 1. Sentinel sweep

    @Test("transcript text, episode title, feed URL and container path never survive projection")
    func sentinelsAreScrubbed() throws {
        let records = try leakyRecords()
        #expect(records.count == 2, "the hostile fixture must still be CAPTURED, not dropped")

        let encoded = String(decoding: try encode(records), as: UTF8.self)
        for sentinel in MetricKitPayloadFixture.allSentinels {
            #expect(
                !encoded.contains(sentinel),
                "sentinel leaked into encoded stability records: \(sentinel)"
            )
        }
        // Also sweep on distinctive fragments, in case a future
        // implementation truncates rather than drops — half a
        // transcript is still a transcript.
        for fragment in ["SENTINELTRANSCRIPT", "SENTINELEPISODE", "sentinelfeed", "SENTINELPATH", "mattress"] {
            #expect(
                !encoded.contains(fragment),
                "sentinel fragment '\(fragment)' leaked into encoded stability records"
            )
        }
    }

    @Test("the useful half of terminationReason survives: namespace + hex code, nothing else")
    func terminationReasonIsDecomposedNotDropped() throws {
        let record = try #require(try leakyRecords().first)
        // The leaky fixture's terminationReason is
        // "Namespace SIGNAL, Code 0x8badf00d <container path>".
        #expect(record.terminationNamespace == "SIGNAL")
        #expect(record.terminationCode == "0x8badf00d")
        // The container path that shared that string is gone — assert it
        // against the encoded bytes rather than gesturing at it.
        let encoded = String(decoding: try encode([record]), as: UTF8.self)
        #expect(!encoded.contains("SENTINELPATH"))
        #expect(!encoded.contains("/private/var"))
        // And the record is still CAPTURED, with a usable stack.
        #expect(!record.frames.isEmpty)
    }

    @Test("the ObjC exception NAME survives; the reason MESSAGE does not")
    func exceptionNameSurvivesReasonDoesNot() throws {
        let record = try #require(try leakyRecords().first)
        #expect(record.objcExceptionName == "NSInternalInconsistencyException")
        #expect(record.objcExceptionClassName == "PlayheadAnalysisCoordinator")
        let encoded = String(decoding: try encode([record]), as: UTF8.self)
        #expect(!encoded.contains("no ad span"), "the exception format string must not ship")
    }

    // MARK: - 2. Structural invariant

    @Test("every string in an encoded record stays inside the sanitiser allowlist")
    func encodedStringsObeyAllowlist() throws {
        let records = try leakyRecords()
        let object = try JSONSerialization.jsonObject(with: try encode(records), options: [])
        let strings = Self.collectStrings(in: object)
        #expect(!strings.isEmpty, "vacuous if the records carried no strings at all")

        // `device_type` is the single field allowed a comma, behind the
        // Apple model grammar (`iPhone17,1`) — see
        // `DiagnosticTextSanitizer.deviceModel(_:)`.
        var allowed = DiagnosticTextSanitizer.allowedCharacters
        allowed.insert(",")

        for value in strings {
            #expect(
                value.count <= DiagnosticTextSanitizer.identifierMaxLength,
                "over-long string in a stability record: \(value.prefix(40))…"
            )
            let offenders = value.filter { !allowed.contains($0) }
            #expect(
                offenders.isEmpty,
                "disallowed character(s) \(Array(Set(offenders))) in stability record string '\(value)'"
            )
        }
    }

    @Test("a URL or POSIX path cannot be represented: `/` and `:` are outside the allowlist")
    func locatorCharactersAreUnrepresentable() {
        let forbidden: [Character] = ["/", ":", "?", "&", "@", "%", "\"", "'", "<", ">",
                                      "{", "}", "[", "]", "|", "^", "~", ";", ",", "!", "\\"]
        for character in forbidden {
            #expect(
                !DiagnosticTextSanitizer.allowedCharacters.contains(character),
                "'\(character)' must stay outside the allowlist — it enables paths/URLs/prose"
            )
        }
        #expect(DiagnosticTextSanitizer.identifier(MetricKitPayloadFixture.sentinelFeedURL) == nil)
        #expect(DiagnosticTextSanitizer.identifier(MetricKitPayloadFixture.sentinelPath) == nil)
        #expect(DiagnosticTextSanitizer.versionToken(MetricKitPayloadFixture.sentinelTranscript) == nil)
        // Non-ASCII (any non-Latin transcript, smart quotes, emoji).
        #expect(DiagnosticTextSanitizer.identifier("Épisode") == nil)
        #expect(DiagnosticTextSanitizer.identifier("エピソード") == nil)
        #expect(DiagnosticTextSanitizer.identifier("🎧") == nil)
    }

    @Test("rejection drops the field rather than truncating it — a truncated leak is still a leak")
    func rejectionDropsRatherThanTruncates() {
        let tooLong = String(repeating: "a", count: DiagnosticTextSanitizer.identifierMaxLength + 1)
        #expect(DiagnosticTextSanitizer.identifier(tooLong) == nil)
        let mostlyClean = "Playhead" + MetricKitPayloadFixture.sentinelFeedURL
        #expect(DiagnosticTextSanitizer.identifier(mostlyClean) == nil)
    }

    // MARK: - 3. Closed shape

    @Test("an encoded record's keys are exactly the declared CodingKeys — no surprise field")
    func encodedRecordShapeIsClosed() throws {
        let record = try #require(try leakyRecords().first)
        let data = try encode([record])
        let array = try #require(
            try JSONSerialization.jsonObject(with: data, options: []) as? [[String: Any]]
        )
        let keys = Set(try #require(array.first).keys)

        // FROZEN literal set, not `CodingKeys.allCases`. Comparing
        // against the type's own keys is circular — adding a
        // `reason: String` field with a CodingKey would satisfy it. The
        // point of this test is that adding ANY field is a deliberate,
        // reviewed edit to a privacy-audited list.
        let frozen: Set<String> = [
            "schema_version", "kind", "received_at",
            "app_version", "app_build_version", "os_version",
            "device_type", "platform_architecture", "is_test_flight",
            "signal", "exception_type", "exception_code",
            "termination_namespace", "termination_code",
            "objc_exception_name", "objc_exception_class_name",
            "hang_duration_ms", "writes_caused_mb", "launch_duration_ms",
            "frame_count", "frames_truncated", "frames"
        ]
        let declared = Set(
            StabilityDiagnosticRecord.CodingKeys.allCases.map(\.rawValue)
        )
        #expect(
            declared == frozen,
            """
            StabilityDiagnosticRecord.CodingKeys changed: \
            added \(declared.subtracting(frozen)), removed \(frozen.subtracting(declared)). \
            Every field on this type is privacy-audited (legal checklist item e) — update the \
            checklist and this frozen set together, deliberately.
            """
        )
        #expect(
            keys.isSubset(of: frozen),
            "encoded record carries key(s) outside the frozen set: \(keys.subtracting(frozen))"
        )

        // Frames too.
        let frames = try #require(array.first?["frames"] as? [[String: Any]])
        let frameKeys = Set(try #require(frames.first).keys)
        let declaredFrameKeys = Set(StabilityCallStackFrame.CodingKeys.allCases.map(\.rawValue))
        #expect(
            frameKeys.isSubset(of: declaredFrameKeys),
            "encoded frame carries key(s) outside StabilityCallStackFrame.CodingKeys: \(frameKeys.subtracting(declaredFrameKeys))"
        )

        // The known-leaky MetricKit key names must never become record
        // keys, whatever else changes.
        for forbidden in [
            "composed_message", "composedMessage",
            "format_string", "formatString",
            "arguments",
            "termination_reason", "terminationReason",
            "virtual_memory_region_info", "virtualMemoryRegionInfo",
            "address", "raw_payload", "message", "reason"
        ] {
            #expect(!keys.contains(forbidden), "leaky key '\(forbidden)' appeared on a record")
            #expect(!frameKeys.contains(forbidden), "leaky key '\(forbidden)' appeared on a frame")
        }
    }

    // MARK: - 4. Allowlist canary

    @Test("the projector reads only the metadata keys it declares in metadataKeys")
    func metadataAllowlistIsExhaustive() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Support/Diagnostics/MetricKitDiagnosticProjector.swift"
        )
        let code = SwiftSourceInspector.strippingComments(source)

        // Every `metadata["…"]` subscript in the (comment-stripped)
        // source must name a declared key. This is what stops a future
        // edit reaching for `metadata["terminationReason"]` verbatim or
        // `metadata["virtualMemoryRegionInfo"]` without declaring it.
        let read = Self.subscriptKeys(in: code, receiver: "metadata")
        #expect(!read.isEmpty, "canary is vacuous — the metadata subscript pattern drifted")
        let undeclared = read.subtracting(MetricKitDiagnosticProjector.metadataKeys)
        #expect(
            undeclared.isEmpty,
            "projector reads undeclared metadata key(s): \(undeclared.sorted())"
        )

        let reasonKeys = Self.subscriptKeys(in: code, receiver: "exceptionReason?")
        let undeclaredReason = reasonKeys.subtracting(MetricKitDiagnosticProjector.exceptionReasonKeys)
        #expect(
            undeclaredReason.isEmpty,
            "projector reads undeclared exceptionReason key(s): \(undeclaredReason.sorted())"
        )
    }

    @Test("the leaky MetricKit key names appear nowhere in the projector's code")
    func leakyKeysAreAbsentFromProjectorCode() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/Support/Diagnostics/MetricKitDiagnosticProjector.swift"
        )
        // Strip comments only — string literals MUST survive. MetricKit
        // keys are read via `metadata["…"]` subscripts, so the leaky
        // names would appear as string literals, not as identifiers.
        //
        // This test previously used `strippingCommentsAndStrings`, which
        // blanks literal contents and made it structurally incapable of
        // failing. A mutation that made the projector read
        // `exceptionReason?["composedMessage"]` was caught by five other
        // tests and sailed past this one — which is how the hole was
        // found. Do not "tighten" this back to the strings-stripped
        // variant.
        //
        // The file documents these names in prose (deliberately), which
        // is why comments still have to go.
        let code = SwiftSourceInspector.strippingComments(source)
        for leaky in ["composedMessage", "formatString", "virtualMemoryRegionInfo"] {
            #expect(
                !code.contains(leaky),
                "projector code references the leaky MetricKit key '\(leaky)'"
            )
        }
        // Vacuity guard: the same read against a name the projector DOES
        // use must find it, proving the source text really is being
        // searched.
        #expect(
            code.contains("exceptionName"),
            "canary is vacuous — the projector source was not actually loaded"
        )
    }

    // MARK: - Helpers

    /// Every string VALUE (not key) anywhere in a decoded JSON tree.
    private static func collectStrings(in object: Any) -> [String] {
        switch object {
        case let value as String:
            return [value]
        case let array as [Any]:
            return array.flatMap { collectStrings(in: $0) }
        case let dictionary as [String: Any]:
            return dictionary.values.flatMap { collectStrings(in: $0) }
        default:
            return []
        }
    }

    /// Literal keys used in `receiver["…"]` subscripts within `code`.
    private static func subscriptKeys(in code: String, receiver: String) -> Set<String> {
        var out = Set<String>()
        let needle = "\(receiver)[\""
        var searchRange = code.startIndex..<code.endIndex
        while let start = code.range(of: needle, range: searchRange) {
            guard let end = code.range(
                of: "\"]", range: start.upperBound..<code.endIndex
            ) else { break }
            out.insert(String(code[start.upperBound..<end.lowerBound]))
            searchRange = end.upperBound..<code.endIndex
        }
        return out
    }
}
