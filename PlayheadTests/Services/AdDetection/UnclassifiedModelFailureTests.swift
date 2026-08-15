// UnclassifiedModelFailureTests.swift
// playhead-59c8 — `ModelManagerError 1001` is NOT a daemon condition, and the
// row it killed died with a stringly-typed reason nobody could count.
//
// THE FIELD ROW. 2026-08-14 device pull (`db-pull10`), `backfill_jobs`, asset
// A9F6DF05, phase `fullEpisodeScan`:
//
//     status      = failed
//     retryCount  = 3
//     deferReason = Error Domain=FoundationModels.LanguageModelError Code=-1
//                   "... (ModelManagerServices.ModelManagerError error 1001.)"
//
// A9F6DF05 was the one row of the nine with a reachable ceiling — measured
// `adScanFraction` 0.8866 against a transcript ceiling of 0.9965 — and it is
// invisible to every re-drive at `retryCount == AdmissionController.maxRetries`.
//
// WHAT THIS FILE PINS, AND WHAT IT DELIBERATELY DOES NOT. It pins that the
// error is NOT admitted to `FMDaemonRefusal` (the refusal, with its argument in
// `UnclassifiedModelFailure`'s header), and that the failing arm now writes a
// NAMED, COUNTABLE cause carrying the discriminator a future bead would need to
// promote one specific `(domain, code)` out of the bucket. It does NOT pin that
// 1001 is permanent — nobody has established that either, which is the whole
// finding: 1001 is `ModelManagerError.inferenceError`, a category wrapper over
// a 25-case enum holding both transient and permanent members, so the code
// cannot answer the question in either direction.

import Foundation
import Testing
import XCTest

@testable import Playhead

@Suite("playhead-59c8: an unclassifiable model error is named, counted, and NOT excused")
struct UnclassifiedModelFailureTests {

    /// The 2026-08-14 field row, rebuilt from its own columns.
    private var fieldRowError: Error { TestFMRuntimeFailure.modelManagerInferenceError.error }

    // MARK: - The classification verdict

    @Test("THE VERDICT: the field row is NOT a daemon refusal")
    func theFieldRowIsNotADaemonRefusal() {
        #expect(FMDaemonRefusal.classify(fieldRowError) == nil)
        #expect(!FMDaemonThrottle.isThrottle(fieldRowError))
        #expect(!FMDaemonRefusal.isMetadataStall(fieldRowError))
    }

    @Test("and `nil` here is a FALLTHROUGH, not a classification — the finding, pinned")
    func theFieldRowFallsThroughRatherThanBeingClassified() {
        // `FMDaemonThrottle.isThrottle` routes through
        // `SemanticScanStatus.from(error:)`, every arm of which is a cast to a
        // Swift type. The field row is a bridged `NSError`, so no arm can
        // match and the status is the function's final `return`. That reads
        // identically to a deliberate "this is not a throttle", which is why
        // the durable record has to say which one it was.
        #expect(SemanticScanStatus.from(error: fieldRowError) == .failedTransient)
        // The control: a throw the funnel CAN classify does not land there.
        #expect(SemanticScanStatus.from(error: CancellationError()) == .cancelled)
    }

    @Test("DISCRIMINATOR: the two conditions this enum DOES know are still refusals")
    func theKnownRefusalsAreUnaffected() {
        // Without this the verdict test above would also pass against an
        // implementation that had stopped classifying anything at all.
        let stall = TestFMRuntimeFailure.metadataTimeout.error
        #expect(FMDaemonRefusal.classify(stall) == .metadataStall)
        let throttle = TestFMRuntimeFailure.rateLimited.error
        // `.rateLimited` is a FoundationModels type; on a host without the
        // framework the fixture degrades to a plain NSError, so this arm is
        // asserted only where the classification is actually reachable.
        if SemanticScanStatus.from(error: throttle) == .rateLimited {
            #expect(FMDaemonRefusal.classify(throttle) == .throttle)
        }
    }

    // MARK: - The identity it records

    @Test("the identity reads BOTH levels of the field row")
    func identityReadsBothLevelsOfTheFieldRow() {
        let identity = UnclassifiedModelFailure.identity(of: fieldRowError)
        #expect(identity.domain == TestFMRuntimeFailure.fieldRowOuterDomain)
        #expect(identity.code == TestFMRuntimeFailure.fieldRowOuterCode)
        #expect(identity.underlyingDomain == TestFMRuntimeFailure.fieldRowUnderlyingDomain)
        #expect(identity.underlyingCode == TestFMRuntimeFailure.fieldRowUnderlyingCode)
        // The two levels are DIFFERENT, so a reader that confused them would
        // be caught. `-1` is the framework declining to classify; `1001` is the
        // only value in the row that names a subsystem.
        #expect(identity.code != identity.underlyingCode)
    }

    @Test("NULL READING: `under` is a word, not an empty string, when there is no chain")
    func underReadsNoneWhenThereIsNoChain() {
        let bare = NSError(domain: "Solo.Domain", code: 7, userInfo: [:])
        let identity = UnclassifiedModelFailure.identity(of: bare)
        #expect(identity.underlyingDomain == nil)
        #expect(identity.underlyingCode == nil)
        let reason = UnclassifiedModelFailure.deferReason(for: bare, phase: .fullEpisodeScan)
        #expect(reason.contains("under=\(UnclassifiedModelFailure.noUnderlyingToken)"))
        #expect(!reason.contains("under=)"))
        #expect(!reason.contains("under=,"))
    }

    @Test("the DEEPEST underlying wins — the first one is the wrapper")
    func theDeepestUnderlyingWins() {
        let deepest = NSError(domain: "Level.Three", code: 3, userInfo: [:])
        let middle = NSError(domain: "Level.Two", code: 2, userInfo: [NSUnderlyingErrorKey: deepest])
        let outer = NSError(domain: "Level.One", code: 1, userInfo: [NSUnderlyingErrorKey: middle])
        let identity = UnclassifiedModelFailure.identity(of: outer)
        #expect(identity.domain == "Level.One")
        #expect(identity.underlyingDomain == "Level.Three")
        #expect(identity.underlyingCode == 3)
    }

    @Test("the walk is BOUNDED — a pathological chain terminates and says where it stopped")
    func theWalkIsBounded() {
        let depth = UnclassifiedModelFailure.maxUnderlyingDepth + 12
        var error = NSError(domain: "Chain.\(depth)", code: depth, userInfo: [:])
        for level in stride(from: depth - 1, through: 0, by: -1) {
            error = NSError(
                domain: "Chain.\(level)",
                code: level,
                userInfo: [NSUnderlyingErrorKey: error]
            )
        }
        let identity = UnclassifiedModelFailure.identity(of: error)
        #expect(identity.domain == "Chain.0")
        // Stopped at the bound, not at the bottom. Reading the bottom would
        // mean the bound is not enforced; reading anything else would mean it
        // is off by one against its own constant.
        #expect(identity.underlyingCode == UnclassifiedModelFailure.maxUnderlyingDepth)
        #expect(identity.underlyingCode != depth)
    }

    @Test("`NSMultipleUnderlyingErrorsKey` is followed too — it is how a CustomNSError bridges")
    func multipleUnderlyingErrorsAreFollowed() {
        let inner = NSError(domain: "Multi.Inner", code: 42, userInfo: [:])
        let outer = NSError(
            domain: "Multi.Outer",
            code: 1,
            userInfo: [NSMultipleUnderlyingErrorsKey: [inner]]
        )
        let identity = UnclassifiedModelFailure.identity(of: outer)
        #expect(identity.underlyingDomain == "Multi.Inner")
        #expect(identity.underlyingCode == 42)
    }

    @Test("a NATIVE Swift error still gets an identity — the read is total")
    func aNativeSwiftErrorStillGetsAnIdentity() {
        let identity = UnclassifiedModelFailure.identity(of: CancellationError())
        #expect(!identity.domain.isEmpty)
        #expect(identity.domain.contains("CancellationError"))
        #expect(identity.underlyingDomain == nil)
    }

    // MARK: - The token

    @Test("the cause names the condition, the phase, and the discriminator")
    func theCauseCarriesEverythingAPullNeeds() {
        let reason = UnclassifiedModelFailure.deferReason(
            for: fieldRowError,
            phase: .fullEpisodeScan
        )
        #expect(reason.hasPrefix("\(UnclassifiedModelFailure.causePrefix)-"))
        #expect(reason.contains(BackfillJobPhase.fullEpisodeScan.rawValue))
        #expect(reason.contains("domain=\(TestFMRuntimeFailure.fieldRowOuterDomain)"))
        #expect(reason.contains("code=\(TestFMRuntimeFailure.fieldRowOuterCode)"))
        #expect(
            reason.contains(
                "under=\(TestFMRuntimeFailure.fieldRowUnderlyingDomain)"
                    + "/\(TestFMRuntimeFailure.fieldRowUnderlyingCode)"
            )
        )
        // playhead-v7q6: NOT the framework's own prose. The whole point is that
        // the column becomes groupable.
        #expect(!reason.contains("Error Domain="))
    }

    @Test("the PHASE travels — two phases do not share a token")
    func thePhaseTravels() {
        let tokens = Set(
            BackfillJobPhase.allCases.map {
                UnclassifiedModelFailure.deferReason(for: fieldRowError, phase: $0)
            }
        )
        #expect(tokens.count == BackfillJobPhase.allCases.count)
    }

    @Test("the token joins NO existing cause family")
    func theTokenJoinsNoExistingFamily() {
        let reason = UnclassifiedModelFailure.deferReason(
            for: fieldRowError,
            phase: .fullEpisodeScan
        )
        // Every durable cause prefix this runner already writes. A new token
        // answering to one of them would inflate a count that means something
        // specific — `FMDaemonRefusal`'s R2-Fix1 rule.
        let existingFamilies = [
            "rateLimited-",
            "metadataStall-",
            "inferenceTimeout-",
            "expiredWithoutProgress-",
            "cancelled-during-",
            "underCoverageBudgetSpent-",
            "underCoverage-"
        ]
        for family in existingFamilies {
            #expect(!reason.hasPrefix(family), "token must not answer to \(family)")
        }
        // And the reverse direction: nothing existing answers to ours.
        let existingTokens = [
            FMDaemonThrottle.DeferCause.window.rawValue,
            FMDaemonThrottle.DeferCause.passPrologue.rawValue,
            FMDaemonThrottle.DeferCause.batchSibling.rawValue,
            FMDaemonRefusal.metadataStall.passPrologueCause,
            FMDaemonRefusal.metadataStall.batchSiblingCause,
            BackfillJobRunner.noProgressExpiryReason(phase: .fullEpisodeScan),
            BackfillJobRunner.underCoverageDeferReason(phase: .fullEpisodeScan),
            BackfillJobRunner.underCoverageExpiryReason(phase: .fullEpisodeScan)
        ]
        for token in existingTokens {
            #expect(!token.hasPrefix("\(UnclassifiedModelFailure.causePrefix)-"))
        }
    }

    @Test("COUNTABLE: a prefix grep over a mixed population counts exactly this cause")
    func theTokenIsCountableByPrefix() {
        let population = [
            UnclassifiedModelFailure.deferReason(for: fieldRowError, phase: .fullEpisodeScan),
            UnclassifiedModelFailure.deferReason(for: fieldRowError, phase: .scanLikelyAdSlots),
            FMDaemonRefusal.metadataStall.passPrologueCause,
            FMDaemonThrottle.DeferCause.passPrologue.rawValue,
            BackfillJobRunner.underCoverageExpiryReason(phase: .fullEpisodeScan),
            BackfillJobRunner.noProgressExpiryReason(phase: .fullEpisodeScan)
        ]
        let counted = population.filter {
            $0.hasPrefix("\(UnclassifiedModelFailure.causePrefix)-")
        }
        #expect(counted.count == 2)
        // The denominator is non-empty and the other four are real causes, so
        // the 2 is a measured 2-of-6, not a 2-of-2.
        #expect(population.count == 6)
    }

    @Test("the log event is its own name — it does not widen an existing grep")
    func theLogEventIsItsOwn() {
        let existing = [
            FMDaemonRefusal.throttle.logEvent,
            FMDaemonRefusal.metadataStall.logEvent,
            FMDaemonRefusal.throttle.drainStoppedEvent,
            FMDaemonRefusal.metadataStall.drainStoppedEvent
        ]
        #expect(!existing.contains(UnclassifiedModelFailure.failureEvent))
        for event in existing {
            #expect(!event.hasPrefix(UnclassifiedModelFailure.failureEvent))
            #expect(!UnclassifiedModelFailure.failureEvent.hasPrefix(event))
        }
    }

    // MARK: - Sanitization

    @Test("a domain cannot break the record it lives in")
    func sanitizeStripsWhatWouldBreakTheRecord() {
        let hostile = UnclassifiedModelFailure.sanitize("Bad Domain(with),chars=here")
        #expect(!hostile.contains(" "))
        #expect(!hostile.contains("("))
        #expect(!hostile.contains(")"))
        #expect(!hostile.contains(","))
        #expect(!hostile.contains("="))
        // And the surviving text is still the domain, not a placeholder.
        #expect(hostile.contains("Bad_Domain"))
    }

    @Test("TRUNCATION KEEPS THE TAIL AND SAYS SO — a fully-qualified name discriminates at the end")
    func truncationKeepsTheDiscriminatingEnd() {
        // The measured case, verbatim: `BackfillJobRunnerTests`' function-local
        // `CoarseFailure` reflects to this, and a plain `prefix(64)` recorded
        // the compiler's private context and not one character of the type.
        let reflected = "PlayheadTests.BackfillJobRunnerTests."
            + "(unknown context at $706420b8).CoarseFailure"
        let sanitized = UnclassifiedModelFailure.sanitize(reflected)
        #expect(sanitized.count == UnclassifiedModelFailure.maxDomainLength)
        #expect(sanitized.contains("CoarseFailure"), "the tail is what identifies it: \(sanitized)")
        #expect(sanitized.hasPrefix("PlayheadTests."), "the module is worth keeping too")
        // The cut is MARKED. A truncated identity that reads as a complete one
        // is the defect, not the truncation.
        #expect(sanitized.contains(UnclassifiedModelFailure.truncationMarker))
        // Vacuity: the input must actually exceed the bound, or this proves
        // nothing about truncation.
        #expect(reflected.count > UnclassifiedModelFailure.maxDomainLength)
        // A domain that FITS is passed through untouched — no marker, no loss.
        let short = UnclassifiedModelFailure.sanitize(TestFMRuntimeFailure.fieldRowUnderlyingDomain)
        #expect(!short.contains(UnclassifiedModelFailure.truncationMarker))
    }

    @Test("a domain is bounded and never empty")
    func sanitizeIsBoundedAndNeverEmpty() {
        let long = String(repeating: "x", count: UnclassifiedModelFailure.maxDomainLength * 3)
        #expect(
            UnclassifiedModelFailure.sanitize(long).count
                == UnclassifiedModelFailure.maxDomainLength
        )
        #expect(
            UnclassifiedModelFailure.sanitize("   ") == UnclassifiedModelFailure.unknownDomainToken
        )
        #expect(UnclassifiedModelFailure.sanitize("") == UnclassifiedModelFailure.unknownDomainToken)
        // The real domains clear the bound, so the cap is not silently
        // truncating the two identities this bead exists to record.
        #expect(
            UnclassifiedModelFailure.sanitize(TestFMRuntimeFailure.fieldRowOuterDomain)
                == TestFMRuntimeFailure.fieldRowOuterDomain
        )
        #expect(
            UnclassifiedModelFailure.sanitize(TestFMRuntimeFailure.fieldRowUnderlyingDomain)
                == TestFMRuntimeFailure.fieldRowUnderlyingDomain
        )
    }
}

// MARK: - Source canary

/// The two claims no runtime assertion on this type can observe, because both
/// are about the CALL SITE rather than about the value.
///
///  1. **The raw description is gone from the durable column.** A test can
///     assert that `deferReason(for:phase:)` returns a named token and say
///     nothing at all about whether the runner still passes
///     `String(describing: error)` to `markBackfillJobFailed`. That literal is
///     the entire defect this bead removes, and it is invisible from here.
///  2. **The token and the event reach the record through the constants.** A
///     literal is how a name and the condition it names drift apart — the
///     argument `FMDaemonRefusalSourceCanaryTests` makes for the two daemon
///     events, one family over.
///
/// Same technique, and the normalizations are reused rather than re-derived —
/// BOTH of them, because they normalize different things and neither subsumes
/// the other. `collapsedCode` trims each line and folds `"a" + "b"`, so it sees
/// a name split across a `\` continuation or a literal join but NOT one written
/// with interior spaces; `denseCode` removes every whitespace character, so it
/// sees `Type` / newline / `. run(` but not a `+` join. The sibling class needed
/// four review rounds and eight planted probes to arrive at that pair; the first
/// draft of this file asked `collapsedCode` for a substring containing no spaces
/// (`reason:String(describing:error)`) against source that has them, which could
/// only ever have returned false — a rail that reports on formatting.
final class UnclassifiedModelFailureSourceCanaryTests: XCTestCase {

    private func runnerSource() throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        let url = root
            .appendingPathComponent("Playhead/Services/AdDetection/BackfillJobRunner.swift")
        return FMDaemonRefusalSourceCanaryTests.codeLines(of: try String(contentsOf: url, encoding: .utf8))
    }

    func testTheGenericArmNoLongerPersistsTheFrameworkDescription() throws {
        let lines = try runnerSource()
        XCTAssertGreaterThan(lines.count, 1_000, "source read found only \(lines.count) code lines")
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)

        // VACUITY FIRST: the arm this canary is about must still exist and must
        // still route through the named cause. Without this the check below
        // passes trivially against a runner that stopped failing jobs at all.
        XCTAssertTrue(
            dense.contains("UnclassifiedModelFailure.deferReason("),
            "BackfillJobRunner no longer builds the unclassified cause; move this canary with the code."
        )
        XCTAssertTrue(
            dense.contains("markBackfillJobFailed("),
            "vacuity: the failing arm is gone from the runner"
        )
        // And the vacuity guard has to be able to FAIL: the forbidden spelling
        // must be findable by the same reader, or "not found" says nothing.
        // `String(describing:` is in the log line one call below, deliberately.
        XCTAssertTrue(
            dense.contains("String(describing:error)"),
            "vacuity: this reader cannot see the spelling it forbids, so its absence proves nothing"
        )

        // THE RULE. `playhead-v7q6`: a durable cause is a named token, never
        // `String(describing:)` of a framework error — it cannot be grouped,
        // cannot be counted, and changes shape with the OS. On the 2026-08-14
        // pull this produced 300 characters of `NSError` prose in the one
        // column a device pull groups by. The LOG may carry prose; the COLUMN
        // may not, which is why the finder is anchored on `reason:`.
        XCTAssertFalse(
            dense.contains("reason:String(describing:error)"),
            """
            The generic failure arm is persisting `String(describing: error)` again. That is the \
            playhead-59c8 defect: the column stops being groupable, and the next unclassified \
            framework condition costs somebody a hand-read of 300 characters to identify.
            """
        )
    }

    func testTheCauseAndEventReachTheRecordThroughTheConstants() throws {
        let lines = try runnerSource()
        // Both normalizations, because a literal can hide from either one alone:
        // interior spaces defeat `collapsedCode`, a `+` join defeats `denseCode`.
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)
        let collapsed = FMDaemonRefusalSourceCanaryTests.collapsedCode(lines)

        let forbidden = [
            UnclassifiedModelFailure.causePrefix,
            UnclassifiedModelFailure.failureEvent
        ]
        XCTAssertEqual(forbidden.count, Set(forbidden).count, "vacuity: the forbidden set is degenerate")
        XCTAssertFalse(forbidden.contains { $0.isEmpty }, "vacuity: an empty needle matches everything")

        let respelled = forbidden.filter { needle in
            [dense, collapsed].contains { $0.contains("\"\(needle)") || $0.contains("\(needle)\"") }
        }
        XCTAssertTrue(
            respelled.isEmpty,
            """
            An unclassified-failure token or event NAME is hard-coded in BackfillJobRunner. Both \
            must come from `UnclassifiedModelFailure`, because the token is the unit a device pull \
            counts and the event is the unit a support-bundle grep counts. Found: \(respelled.sorted())
            """
        )
        XCTAssertTrue(
            dense.contains("UnclassifiedModelFailure.failureEvent"),
            "the runner must emit the event through the constant"
        )
    }
}
