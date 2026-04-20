// EpisodeStatusLineCopyTests.swift
// Pin every golden string the status-line resolver produces, across
// the seven cases the bead spec (playhead-zp5y) enumerates plus the
// first/next branching for `.proximal` and the optional secondary
// "analyzing remainder" line.
//
// Discipline: tests resolve copy through the public
// `EpisodeSurfaceStatus` API. Nothing here imports or references
// `InternalMissCause`; the test file's source is scanned by a
// companion regression test to enforce this boundary (per
// playhead-5bb3's lint contract).

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeStatusLineCopy — golden strings (playhead-zp5y)")
struct EpisodeStatusLineCopyTests {

    // MARK: - Punctuation constants
    //
    // Pinned here (not imported from the resolver) so a regression in
    // the resolver's private constants cannot silently hide a copy
    // change from these tests. The test file is the canonical golden
    // reference.
    private static let middot = "\u{00B7}"   // "·"
    private static let emdash = "\u{2014}"   // "—"

    // MARK: - Helpers

    /// Build an `EpisodeSurfaceStatus` for a disposition-dominant case
    /// (unavailable / failed / paused / cancelled). Readiness is
    /// irrelevant to the primary string in those cases.
    private static func dispositionStatus(
        disposition: SurfaceDisposition,
        reason: SurfaceReason,
        hint: ResolutionHint,
        analysisUnavailableReason: AnalysisUnavailableReason? = nil
    ) -> EpisodeSurfaceStatus {
        EpisodeSurfaceStatus(
            disposition: disposition,
            reason: reason,
            hint: hint,
            analysisUnavailableReason: analysisUnavailableReason,
            playbackReadiness: .none,
            readinessAnchor: nil
        )
    }

    /// Build an `EpisodeSurfaceStatus` for a queued + readiness case.
    private static func queuedStatus(
        readiness: PlaybackReadiness,
        anchor: TimeInterval? = nil
    ) -> EpisodeSurfaceStatus {
        EpisodeSurfaceStatus(
            disposition: .queued,
            reason: .waitingForTime,
            hint: .wait,
            analysisUnavailableReason: nil,
            playbackReadiness: readiness,
            readinessAnchor: anchor
        )
    }

    // MARK: - 1. complete

    @Test("complete renders \"Skip-ready · full episode\"")
    func completeGolden() {
        let status = Self.queuedStatus(readiness: .complete)
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil
        )
        #expect(line.primary == "Skip-ready \(Self.middot) full episode")
        #expect(line.secondary == nil)
    }

    // MARK: - 2. proximal — first X min

    @Test("proximal from 0 renders \"Skip-ready · first 15 min\"")
    func proximalFromStartGolden() {
        let status = Self.queuedStatus(readiness: .proximal, anchor: 0)
        let coverage = CoverageSummary(
            coverageRanges: [0...900],
            isComplete: false,
            modelVersion: "test",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date()
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: coverage,
            anchor: 0
        )
        #expect(line.primary == "Skip-ready \(Self.middot) first 15 min")
        #expect(line.secondary == nil)
    }

    // MARK: - 2b. proximal — next X min (anchor mid-episode)

    @Test("proximal mid-episode renders \"Skip-ready · next X min\"")
    func proximalMidEpisodeGolden() {
        // Coverage starts at 600s (10 minutes in) and extends to 1500s
        // (25 minutes in). Anchor is 600s — the covered region extends
        // 15 minutes past the anchor. Lead word must be "next".
        let status = Self.queuedStatus(readiness: .proximal, anchor: 600)
        let coverage = CoverageSummary(
            coverageRanges: [600...1500],
            isComplete: false,
            modelVersion: "test",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date()
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: coverage,
            anchor: 600
        )
        #expect(line.primary == "Skip-ready \(Self.middot) next 15 min")
        #expect(line.secondary == nil)
    }

    // MARK: - 3. deferredOnly

    @Test("deferredOnly renders \"Downloaded · queued for analysis\"")
    func deferredOnlyGolden() {
        let status = Self.queuedStatus(readiness: .deferredOnly)
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil
        )
        #expect(line.primary == "Downloaded \(Self.middot) queued for analysis")
        #expect(line.secondary == nil)
    }

    // MARK: - 4. none + queued

    @Test("none + queued renders \"Queued · [hint]\"")
    func noneQueuedGolden() {
        let status = Self.queuedStatus(readiness: .none)
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil
        )
        // `queuedStatus` uses `.wait`; `.wait` copy is "waiting".
        #expect(line.primary == "Queued \(Self.middot) waiting")
        #expect(line.secondary == nil)
    }

    // MARK: - 5. paused

    @Test("paused renders \"Paused — [reason] · [hint]\"")
    func pausedGolden() {
        // Thermal pause from the reducer: reason = phoneIsHot, hint = wait.
        let status = Self.dispositionStatus(
            disposition: .paused,
            reason: .phoneIsHot,
            hint: .wait
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil
        )
        // SurfaceReasonCopyTemplates.template(for: .phoneIsHot) prefixes
        // "Paused — phone is too hot"; our resolver strips the lead so
        // the final string has exactly one "Paused —".
        #expect(line.primary == "Paused \(Self.emdash) phone is too hot \(Self.middot) waiting")
        #expect(line.secondary == nil)
    }

    @Test("paused with a reason that is not already prefixed preserves the shape")
    func pausedWithoutPrefixGolden() {
        // `resumeInApp` copy is "Open Playhead to resume" — no "Paused —"
        // lead — so the resolver MUST add one.
        let status = Self.dispositionStatus(
            disposition: .paused,
            reason: .resumeInApp,
            hint: .openAppToResume
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil
        )
        #expect(line.primary == "Paused \(Self.emdash) Open Playhead to resume \(Self.middot) open Playhead to resume")
    }

    // MARK: - 6. unavailable

    @Test("unavailable renders \"Analysis unavailable — [reason]\"")
    func unavailableGolden() {
        let status = Self.dispositionStatus(
            disposition: .unavailable,
            reason: .analysisUnavailable,
            hint: .enableAppleIntelligence,
            analysisUnavailableReason: .appleIntelligenceDisabled
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil
        )
        #expect(line.primary == "Analysis unavailable \(Self.emdash) turn on Apple Intelligence")
        #expect(line.secondary == nil)
    }

    @Test("unavailable with no reason falls back to generic SurfaceReason copy")
    func unavailableWithoutReasonGolden() {
        let status = Self.dispositionStatus(
            disposition: .unavailable,
            reason: .analysisUnavailable,
            hint: .enableAppleIntelligence,
            analysisUnavailableReason: nil
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil
        )
        #expect(line.primary == "Analysis unavailable \(Self.emdash) Analysis unavailable on this device")
    }

    // MARK: - 7. failed

    @Test("failed renders \"Couldn't analyze · Retry\"")
    func failedGolden() {
        let status = Self.dispositionStatus(
            disposition: .failed,
            reason: .couldntAnalyze,
            hint: .retry
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil
        )
        #expect(line.primary == "Couldn't analyze \(Self.middot) Retry")
        #expect(line.secondary == nil)
    }

    // MARK: - Optional secondary: "analyzing remainder"

    @Test("proximal with backfillActive emits \"analyzing remainder\" secondary")
    func backfillSecondaryPresentOnProximal() {
        let status = Self.queuedStatus(readiness: .proximal, anchor: 0)
        let coverage = CoverageSummary(
            coverageRanges: [0...900],
            isComplete: false,
            modelVersion: "test",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date()
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: coverage,
            anchor: 0,
            backfillActive: true
        )
        #expect(line.primary == "Skip-ready \(Self.middot) first 15 min")
        #expect(line.secondary == "analyzing remainder")
    }

    @Test("proximal without backfillActive omits the secondary line")
    func backfillSecondaryAbsentByDefault() {
        let status = Self.queuedStatus(readiness: .proximal, anchor: 0)
        let coverage = CoverageSummary(
            coverageRanges: [0...900],
            isComplete: false,
            modelVersion: "test",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date()
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: coverage,
            anchor: 0,
            backfillActive: false
        )
        #expect(line.secondary == nil)
    }

    @Test("failed ignores backfillActive (secondary line is suppressed)")
    func backfillSecondaryIgnoredWhenFailed() {
        // Backfill is only meaningful when a playable-state primary is
        // rendered; for the three "something is wrong" dispositions the
        // secondary MUST stay nil even if a caller passes `true`.
        let status = Self.dispositionStatus(
            disposition: .failed,
            reason: .couldntAnalyze,
            hint: .retry
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: nil,
            anchor: nil,
            backfillActive: true
        )
        #expect(line.secondary == nil)
    }

    // MARK: - First vs Next branching (exhaustive)

    @Test("firstCoveredOffset exactly 0 picks \"first\"")
    func firstBranchAtExactZero() {
        let status = Self.queuedStatus(readiness: .proximal, anchor: 0)
        let coverage = CoverageSummary(
            coverageRanges: [0...900],
            isComplete: false,
            modelVersion: "test",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date()
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: coverage,
            anchor: 0
        )
        #expect(line.primary.contains("first"))
        #expect(!line.primary.contains("next"))
    }

    @Test("firstCoveredOffset > 0 picks \"next\"")
    func nextBranchWhenAnchorMidEpisode() {
        let status = Self.queuedStatus(readiness: .proximal, anchor: 300)
        let coverage = CoverageSummary(
            coverageRanges: [300...1200],
            isComplete: false,
            modelVersion: "test",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date()
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: coverage,
            anchor: 300
        )
        #expect(line.primary.contains("next"))
        #expect(!line.primary.contains("first"))
    }

    // MARK: - Precedence: disposition beats readiness

    @Test("unavailable + proximal readiness still renders unavailable copy")
    func unavailableBeatsProximal() {
        // Contrived: some reducer paths could in principle produce
        // `.unavailable` disposition with a stale `.proximal` readiness
        // (e.g. cached coverage from before eligibility flipped off).
        // The resolver MUST still surface the unavailable primary so
        // the user is not told "Skip-ready" on an ineligible device.
        let status = EpisodeSurfaceStatus(
            disposition: .unavailable,
            reason: .analysisUnavailable,
            hint: .enableAppleIntelligence,
            analysisUnavailableReason: .hardwareUnsupported,
            playbackReadiness: .proximal,
            readinessAnchor: 0
        )
        let coverage = CoverageSummary(
            coverageRanges: [0...900],
            isComplete: false,
            modelVersion: "test",
            policyVersion: 1,
            featureSchemaVersion: 1,
            updatedAt: Date()
        )
        let line = EpisodeStatusLineCopy.resolve(
            status: status,
            coverage: coverage,
            anchor: 0
        )
        #expect(line.primary.hasPrefix("Analysis unavailable"))
    }

    // MARK: - Exhaustive hint copy

    @Test("every ResolutionHint has a non-empty copy string")
    func hintCopyIsExhaustive() {
        for hint in ResolutionHint.allCases {
            let copy = EpisodeStatusLineCopy.hintCopy(hint)
            #expect(!copy.isEmpty, "hintCopy(\(hint)) must not be empty")
        }
    }

    // MARK: - Exhaustive unavailable-reason copy

    @Test("every AnalysisUnavailableReason has a non-empty copy string")
    func unavailableReasonCopyIsExhaustive() {
        for reason in AnalysisUnavailableReason.allCases {
            let copy = EpisodeStatusLineCopy.unavailableReasonCopy(reason)
            #expect(!copy.isEmpty, "unavailableReasonCopy(\(reason)) must not be empty")
        }
    }

    // MARK: - CI grep guard: this bead's test file must not reference
    // the raw scheduler cause taxonomy
    //
    // The bead spec (playhead-zp5y) states: no test accesses the raw
    // cause taxonomy directly. The existing SurfaceStatusUILintTests
    // enforces the same discipline for UI-layer source files; this
    // test extends the discipline to playhead-zp5y's own test module.
    //
    // Scope is narrow by design: only this specific test file is
    // scanned. Other SurfaceStatus test files (fuzz suites, matrix
    // generators, the UI lint itself) legitimately reference the
    // cause taxonomy to build fixtures or enforce boundaries, and
    // are out of scope for this bead's guard.
    //
    // The forbidden token is assembled from two halves at runtime
    // so the full word never appears as a string literal, comment
    // token, or identifier in this source file — which would trip
    // the scan against itself.
    @Test("this test file does not reference the raw cause taxonomy")
    func thisFileDoesNotReferenceRawCauseTaxonomy() throws {
        let forbidden = "Internal" + "MissCause"
        let selfURL = URL(fileURLWithPath: String(describing: #filePath))
        let source = try String(contentsOf: selfURL, encoding: .utf8)

        var violations: [Int] = []
        var lineNumber = 0
        source.enumerateLines { line, _ in
            lineNumber += 1
            // Strip `//` line comments and `*` block-comment continuation
            // lines so doc comments that mention the token don't trip
            // the scan.
            let trimmed = line.drop(while: { $0 == " " || $0 == "\t" })
            if trimmed.first == "*" { return }
            let code: Substring
            if let slashRange = line.range(of: "//") {
                code = line[..<slashRange.lowerBound]
            } else {
                code = Substring(line)
            }
            // Strip quoted string literals too — a literal that
            // happens to contain the token (e.g. an error message)
            // is not a reference to the type.
            let withoutStrings = Self.stripStringLiterals(String(code))
            if withoutStrings.range(of: forbidden) != nil {
                violations.append(lineNumber)
            }
        }

        #expect(
            violations.isEmpty,
            "\(selfURL.lastPathComponent) must not reference the raw scheduler cause type; offending lines: \(violations)"
        )
    }

    /// Remove double-quoted substrings from a line so a string literal
    /// that contains the forbidden token (e.g. an error message) does
    /// not trip the lint. Raw strings and multiline strings are not
    /// handled — this suite does not use them, and the test file
    /// itself is part of the scan.
    private static func stripStringLiterals(_ line: String) -> String {
        var out = ""
        var inString = false
        var i = line.startIndex
        while i < line.endIndex {
            let c = line[i]
            if c == "\"" {
                inString.toggle()
                i = line.index(after: i)
                continue
            }
            if !inString {
                out.append(c)
            }
            i = line.index(after: i)
        }
        return out
    }
}
