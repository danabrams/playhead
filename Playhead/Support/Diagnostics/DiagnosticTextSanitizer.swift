// DiagnosticTextSanitizer.swift
// Character-allowlist sanitiser for every String that a MetricKit
// stability diagnostic is allowed to contribute to the diagnostics
// bundle.
//
// Scope: playhead-jw63.4 (crash + hang pipeline).
//
// ----- Why an allowlist, not a denylist -----
//
// Playhead's on-device mandate says no transcript text, no ad-candidate
// text, no episode titles, and no feed URLs may leave the device.
// MetricKit payloads are the one channel that could smuggle them out
// without anybody writing code to do it: an `NSException` reason, a
// `terminationReason`, or a `virtualMemoryRegionInfo` blob routinely
// contains interpolated application strings and absolute file paths.
//
// A denylist ("strip anything that looks like a transcript") cannot be
// right — you cannot enumerate what a transcript looks like. So the
// pipeline inverts the burden of proof twice:
//
//   1. `MetricKitDiagnosticProjector` reads only keys it names. A field
//      that is never read cannot leak. (That is the primary defence and
//      it is what removes the free-text fields entirely.)
//   2. Everything that IS read passes through this file, which rejects
//      any string containing a character outside `allowedCharacters` or
//      longer than the per-kind cap. Rejection means the field is
//      dropped (`nil`), never truncated-and-kept — a truncated leak is
//      still a leak.
//
// The allowlist is hostile to locators and to non-Latin text:
//   * No `/`, `:`, `?`, `#`, `%`, `@` → a URL or a POSIX path cannot
//     survive intact, and neither can a `file://` locator.
//   * No `,`, `'`, `"`, `!`, `;` → English sentence punctuation is gone.
//   * ASCII only → any non-Latin transcript text, smart quote, or emoji
//     is rejected outright.
//
// ----- What the character allowlist does NOT do, stated plainly -----
//
// A run of unpunctuated ASCII words is allowlist-clean. `isAllowed`
// admits the space, because real OS build strings need it
// (`iPhone OS 27.0 (25A123)`). So the character class alone would NOT
// reject `SENTINELEPISODE Diary of a CEO 431`. Claiming otherwise would
// be the worst kind of privacy documentation — a guarantee that reads
// stronger than it is.
//
// What actually closes that gap is the SHAPE bound per field, and the
// fact that only three of the strings this file sanitises come from an
// app-influenced source at all:
//
//   * `exceptionReason.exceptionName` / `.className` → `identifier(_:)`,
//     which rejects ANY string containing a space. A class name and an
//     exception name are single tokens; a sentence is not. This is the
//     one field an `assertionFailure("… \(episode.title)")` could
//     plausibly reach, and a space-free token cannot carry a title.
//   * `terminationReason` → never stored; decomposed into an
//     allowlisted namespace token and a hex literal.
//
// Everything else (`appVersion`, `osVersion`, `deviceType`,
// `platformArchitecture`, `binaryName`) is supplied by the OS or the
// bundle, not by application code, and is additionally shape-bounded:
// `versionToken` caps at four whitespace-separated tokens, which every
// real OS build string satisfies and a sentence does not.
//
// The primary defence remains `MetricKitDiagnosticProjector`'s key
// allowlist: the free-text fields are never read at all. This file is
// the second line, and it is honest about being a second line.
//
// This type is pure and has no dependencies beyond Foundation so it can
// be exercised directly from the fast test gate.

import Foundation

enum DiagnosticTextSanitizer {

    // MARK: - Allowlist

    /// The only characters a sanitised diagnostic string may contain.
    ///
    /// Rationale for each admitted class:
    ///   * `A-Z a-z 0-9` — symbol and identifier bodies.
    ///   * `_` `.` `$` — Swift/ObjC mangled-name and framework-name
    ///     punctuation (`libswift_Concurrency.dylib`, `$s4Play…`).
    ///   * `+` `-` — architecture and version separators (`arm64-v8`,
    ///     `1.0.0-rc1`), and the `-` inside a canonical UUID.
    ///   * `(` `)` and space — OS build strings (`iPhone OS 27.0 (25A123)`).
    ///
    /// Everything else — path separators, colons, quotes, commas,
    /// non-ASCII — is a rejection.
    static let allowedCharacters: Set<Character> = {
        var set = Set<Character>()
        set.formUnion("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        set.formUnion("abcdefghijklmnopqrstuvwxyz")
        set.formUnion("0123456789")
        set.formUnion("_.$+-() ")
        return set
    }()

    /// Cap for symbol-shaped fields (binary names, exception class
    /// names). Long enough for a mangled Swift symbol prefix, far too
    /// short for a sentence of prose.
    static let identifierMaxLength = 128

    /// Cap for version / device / architecture tokens.
    static let versionMaxLength = 64

    /// Longest termination code we will render, in characters
    /// (`0x` + 16 hex digits).
    static let terminationCodeMaxLength = 18

    /// Maximum whitespace-separated tokens in a version / OS / device
    /// token. `iPhone OS 27.0 (25A123)` is exactly four; a sentence is
    /// not. This is the shape bound that the character allowlist
    /// deliberately does not provide (see the file header).
    static let versionMaxTokens = 4

    // MARK: - Core predicate

    /// True when every character of `value` is in ``allowedCharacters``.
    /// Empty strings are NOT allowed — an empty field carries no
    /// diagnostic value and `nil` (key omitted) is the honest encoding.
    static func isAllowed(_ value: String) -> Bool {
        guard !value.isEmpty else { return false }
        return value.allSatisfy { allowedCharacters.contains($0) }
    }

    // MARK: - Field sanitisers

    /// Sanitise a symbol-shaped field (binary name, ObjC class name,
    /// exception name). Returns `nil` — meaning "omit this field" — when
    /// the input is absent, too long, contains a disallowed character,
    /// or contains a SPACE.
    ///
    /// The space rejection is the load-bearing part. A Mach-O image
    /// name, an ObjC class name and an `NSException` name are all single
    /// tokens; an interpolated assertion message ("no ad span for
    /// Diary of a CEO 431") is not. Since `exceptionName` / `className`
    /// are the only strings here that application code can influence,
    /// "single token" is what keeps a sentence out of a record.
    static func identifier(_ raw: String?) -> String? {
        guard let raw,
              raw.count <= identifierMaxLength,
              !raw.contains(" "),
              isAllowed(raw) else {
            return nil
        }
        return raw
    }

    /// Sanitise a version / device / architecture token — an OS- or
    /// bundle-supplied string like `iPhone OS 27.0 (25A123)`.
    ///
    /// Spaces ARE permitted (real OS build strings need them), so the
    /// shape bound here is the token count: at most
    /// ``versionMaxTokens``. That is what a build string satisfies and a
    /// sentence does not.
    static func versionToken(_ raw: String?) -> String? {
        guard let raw, raw.count <= versionMaxLength, isAllowed(raw) else {
            return nil
        }
        let tokens = raw.split(separator: " ", omittingEmptySubsequences: true)
        guard tokens.count <= versionMaxTokens else { return nil }
        return raw
    }

    /// Sanitise an Apple device-model identifier (`iPhone17,1`).
    ///
    /// This is the one field whose real values need a character the
    /// allowlist rejects — the comma. Rather than admitting `,`
    /// generally (which would let a comma-separated sentence through
    /// every other field), it is admitted here behind the exact Apple
    /// model grammar: `<letters><digits>,<digits>`. Prose cannot occupy
    /// that shape, so the channel stays shut.
    ///
    /// Every predicate below is ASCII-qualified. `Character.isLetter`
    /// and `.isNumber` are UNICODE-wide — `É` is a letter and `𝟠` is a
    /// number — so an unqualified grammar check would have let up to 26
    /// arbitrary non-ASCII characters through on the comma path, which
    /// never reaches `isAllowed`. That would falsify the "ASCII only"
    /// guarantee this file and the legal checklist both assert.
    static func deviceModel(_ raw: String?) -> String? {
        guard let raw, raw.count <= 32 else { return nil }
        let parts = raw.split(separator: ",", omittingEmptySubsequences: false)
        guard parts.count == 2 else {
            // No comma at all (simulator reports e.g. `arm64`) — fall
            // back to the ordinary allowlist.
            return parts.count == 1 ? versionToken(raw) : nil
        }
        let family = parts[0]
        let variant = parts[1]
        guard (1...3).contains(variant.count),
              variant.allSatisfy(isASCIIDigit) else { return nil }
        let letters = family.prefix { $0.isASCII && $0.isLetter }
        let digits = family.dropFirst(letters.count)
        guard (1...20).contains(letters.count),
              (1...3).contains(digits.count),
              digits.allSatisfy(isASCIIDigit) else { return nil }
        return raw
    }

    private static func isASCIIDigit(_ character: Character) -> Bool {
        character.isASCII && character.isNumber
    }

    /// Validate a MetricKit `binaryUUID` by round-tripping it through
    /// `UUID`. Anything that is not a canonical UUID is dropped, which
    /// means a malformed or spoofed value can never become a free-text
    /// smuggling slot. Output is uppercased so it compares directly
    /// against `dwarfdump --uuid` output.
    static func binaryUUID(_ raw: String?) -> String? {
        guard let raw, let uuid = UUID(uuidString: raw) else { return nil }
        return uuid.uuidString
    }

    // MARK: - terminationReason decomposition

    /// Termination namespaces iOS is known to emit. `terminationReason`
    /// itself is FREE TEXT and is never stored — we only report whether
    /// its leading token is one of these known values.
    ///
    /// Sourced from the strings iOS puts in front of the code in
    /// `Namespace <NAME>, Code 0x…` and in RunningBoard / SpringBoard
    /// termination contexts.
    static let knownTerminationNamespaces: Set<String> = [
        "SIGNAL",
        "RUNNINGBOARD",
        "SPRINGBOARD",
        "FRONTBOARD",
        "ASSERTIOND",
        "CODESIGNING",
        "DYLD",
        "JETSAM",
        "SWIFT",
        "TCC",
        "WATCHDOG",
        "CARRIER",
        "APPLICATIONSPECIFIC"
    ]

    /// Extract the termination NAMESPACE from a raw `terminationReason`
    /// string, e.g. `"Namespace SIGNAL, Code 0xb"` → `"SIGNAL"`.
    ///
    /// The raw string is scanned for allowlisted namespace tokens and
    /// nothing else is retained — an unrecognised namespace yields
    /// `nil` rather than passing the unknown token through, so a future
    /// iOS that puts prose where the namespace used to be cannot widen
    /// the channel.
    static func terminationNamespace(from raw: String?) -> String? {
        guard let raw else { return nil }
        // Cheap bound: a legitimate terminationReason is short. A
        // pathological multi-kilobyte blob is not worth scanning.
        let scanned = raw.count > 512 ? String(raw.prefix(512)) : raw
        let upper = scanned.uppercased()
        // Split on non-letters so `Namespace SIGNAL, Code 0xb` and
        // `<RBSTerminateContext| domain:10 …>` both tokenise sanely.
        let tokens = upper.split(whereSeparator: { !$0.isLetter })
        for token in tokens where knownTerminationNamespaces.contains(String(token)) {
            return String(token)
        }
        return nil
    }

    /// Extract the first hexadecimal termination CODE from a raw
    /// `terminationReason` string, e.g.
    /// `"<RBSTerminateContext| … code:0x8BADF00D …>"` → `"0x8badf00d"`.
    ///
    /// Only the `0x`-prefixed hex run is retained; every other byte of
    /// the raw string is discarded. This is what makes the well-known
    /// watchdog / jetsam codes (`0x8badf00d`, `0xdead10cc`, `0xc00010ff`)
    /// triageable without carrying the surrounding free text.
    static func terminationCode(from raw: String?) -> String? {
        guard let raw else { return nil }
        let scanned = raw.count > 512 ? String(raw.prefix(512)) : raw
        let lower = scanned.lowercased()
        guard let prefixRange = lower.range(of: "0x") else { return nil }
        var digits = ""
        var index = prefixRange.upperBound
        // ASCII-qualified: `Character.isHexDigit` also matches fullwidth
        // compatibility forms (`Ａ`), and `lowercased()` keeps them
        // non-ASCII — which would put non-ASCII characters into
        // `termination_code` out of the one app-influenced free-text
        // field we read.
        while index < lower.endIndex,
              lower[index].isASCII,
              lower[index].isHexDigit,
              digits.count < 16 {
            digits.append(lower[index])
            index = lower.index(after: index)
        }
        guard !digits.isEmpty else { return nil }
        let code = "0x\(digits)"
        // Belt and braces: the constructed code must itself satisfy the
        // character allowlist, so this function cannot be the one place
        // that emits something the invariant forbids.
        guard code.count <= terminationCodeMaxLength, isAllowed(code) else { return nil }
        return code
    }

    // MARK: - Unit-bearing measurements

    /// Parse a MetricKit duration into whole milliseconds.
    ///
    /// `MXDiagnostic` JSON renders `Measurement<UnitDuration>` values as
    /// a unit-suffixed string (`"3300 ms"`, `"2.75 s"`). Some payload
    /// versions hand back a bare number instead, so both shapes are
    /// accepted. Anything else yields `nil`.
    ///
    /// Only the numeric magnitude survives — the raw string never
    /// reaches a record field.
    static func milliseconds(from raw: Any?) -> Int? {
        guard let raw else { return nil }
        if let number = raw as? NSNumber, !(raw is String) {
            return roundedMilliseconds(number.doubleValue)
        }
        guard let text = raw as? String else { return nil }
        guard let (magnitude, unit) = magnitudeAndUnit(in: text) else { return nil }
        switch unit {
        case "ms": return roundedMilliseconds(magnitude)
        case "s":  return roundedMilliseconds(magnitude * 1_000)
        case "":   return roundedMilliseconds(magnitude)
        default:   return nil
        }
    }

    /// Parse a MetricKit storage measurement into megabytes.
    ///
    /// `MXDiskWriteExceptionDiagnostic.totalWritesCaused` renders as
    /// `"1024 MB"` (occasionally `"2 GB"` / `"512 kB"`). As with
    /// ``milliseconds(from:)`` only the magnitude survives.
    static func megabytes(from raw: Any?) -> Double? {
        guard let raw else { return nil }
        if let number = raw as? NSNumber, !(raw is String) {
            return finiteOrNil(number.doubleValue)
        }
        guard let text = raw as? String else { return nil }
        guard let (magnitude, unit) = magnitudeAndUnit(in: text) else { return nil }
        switch unit {
        case "mb", "":  return finiteOrNil(magnitude)
        case "gb":      return finiteOrNil(magnitude * 1_024)
        case "kb":      return finiteOrNil(magnitude / 1_024)
        case "bytes":   return finiteOrNil(magnitude / (1_024 * 1_024))
        default:        return nil
        }
    }

    // MARK: - Numeric helpers

    /// Read an integer out of a JSON value that may be an `NSNumber` or
    /// a numeric string. Returns `nil` for anything non-numeric — a
    /// string field that happens to sit where a number was expected is
    /// dropped, never coerced into the record as text.
    static func integer(from raw: Any?) -> Int? {
        guard let raw else { return nil }
        if let number = raw as? NSNumber, !(raw is String) {
            let value = number.doubleValue
            guard value.isFinite,
                  value >= Double(Int.min),
                  value <= Double(Int.max) else { return nil }
            return Int(value)
        }
        if let text = raw as? String { return Int(text) }
        return nil
    }

    /// Read a boolean out of a JSON value. `NSNumber`-backed booleans
    /// (what `JSONSerialization` produces) and the literal strings
    /// `"true"` / `"false"` are accepted; anything else is `nil`.
    static func boolean(from raw: Any?) -> Bool? {
        guard let raw else { return nil }
        if let number = raw as? NSNumber, !(raw is String) { return number.boolValue }
        if let text = raw as? String {
            switch text.lowercased() {
            case "true", "yes", "1": return true
            case "false", "no", "0": return false
            default: return nil
            }
        }
        return nil
    }

    // MARK: - Private

    /// Split a unit-bearing measurement string into its magnitude and a
    /// lowercased unit token. Returns `nil` when the leading token is
    /// not a finite number.
    private static func magnitudeAndUnit(in text: String) -> (Double, String)? {
        let trimmed = text.trimmingCharacters(in: .whitespaces)
        guard trimmed.count <= 64 else { return nil }
        let parts = trimmed.split(separator: " ", maxSplits: 1, omittingEmptySubsequences: true)
        guard let first = parts.first, let magnitude = Double(first), magnitude.isFinite else {
            return nil
        }
        let unit = parts.count > 1 ? String(parts[1]).lowercased() : ""
        return (magnitude, unit)
    }

    private static func roundedMilliseconds(_ value: Double) -> Int? {
        guard value.isFinite, value >= 0, value <= 1_000_000_000 else { return nil }
        return Int(value.rounded())
    }

    private static func finiteOrNil(_ value: Double) -> Double? {
        guard value.isFinite, value >= 0 else { return nil }
        return value
    }
}
