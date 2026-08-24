// StoreFailureRecordTests.swift
// playhead-sckv — the typed `AnalysisStoreError` arm wrote `String(describing:)`
// into the durable `backfill_jobs.deferReason` column.
//
// THE DEFECT, one line of `BackfillJobRunner`:
//
//     reason: String(describing: storeError)
//
// It is the same defect `playhead-59c8` removed from the arm one line below, and
// `playhead-v7q6` forbids by name.
//
// AND IT IS NOT THE STRING THE BEAD PREDICTED — measured by running it. The bead
// expected the reflected form, `insertFailed("UNIQUE constraint failed: …")`.
// `AnalysisStoreError` conforms to `CustomStringConvertible`, which
// `String(describing:)` prefers over reflection, so the column really got the
// enum's PROSE: `Insert failed: UNIQUE constraint failed: semantic_scan_results.id`,
// `SQLite open failed (14): unable to open database file`, and — for `.notFound`
// — `Row not found`, which names no case whatsoever. The payload is still
// unbounded and per-row, and the case name is gone as well. `theRealDefect…`
// below pins that spelling from the production type rather than from this
// comment, so a reader is never trusting either.
//
// WHAT MADE IT WORSE HERE. The runner already computed both halves of the honest
// answer and threw them away: `caseName(of:)` two lines BELOW the write, and
// `isPermanent(_:)` twice, neither time reaching the column. A row's PERMANENCE
// is what separates a charge of one attempt from a short-circuit straight to
// `AdmissionController.maxRetries` — the decision that retires the job — and it
// was unreadable from a device pull while sitting in scope.
//
// WHAT THIS FILE PINS. That the column now carries
// `storeFailure-<phase>(case=…,permanent=…)`; that the prefix collides with
// nothing else the column holds; that `permanent=` is a real second field
// (`insertFailed` classifies BOTH ways, so the case name alone cannot answer it);
// that the payload prose is GONE; and — the half no runtime assertion on the type
// can see — that the runner's write actually consumes it, with the same
// `isPermanent` local the retry arithmetic used.
//
// WHAT IT DELIBERATELY DOES NOT PIN. Any change to the disposition or the
// arithmetic. The row is still `failed`, `FMDaemonRefusal` gains no case, and
// `storeFailureRetryCount` is untouched — `BackfillFailedAttemptRetryChargeTests`
// owns that claim (playhead-ronl) and this bead does not re-open it.

import Foundation
import Testing
import XCTest

@testable import Playhead

// MARK: - The token

@Suite("playhead-sckv: a typed store failure is named, countable, and carries its permanence")
struct StoreFailureRecordTests {

    /// Every `AnalysisStoreError` case paired with the two things the token must
    /// carry about it. The switch below forces exhaustiveness, so a case added
    /// later fails COMPILATION here rather than shipping with an untested token.
    ///
    /// `insertFailed` appears twice on purpose: it is the only case that
    /// classifies both ways, and it is therefore the whole argument for
    /// `permanent=` being a field rather than something a reader derives.
    ///
    /// `payloadWitness` is a distinctive fragment of what the OLD write put in
    /// the column — the per-row half nobody can group by. `nil` for the one case
    /// that carries no associated value.
    struct Case {
        let error: AnalysisStoreError
        let caseName: String
        let permanent: Bool
        let payloadWitness: String?
    }

    static let allCases: [Case] = [
        Case(
            error: .openFailed(code: 14, message: "unable to open database file"),
            caseName: "openFailed", permanent: false,
            payloadWitness: "unable to open database file"
        ),
        Case(
            error: .migrationFailed("V54 stopped halfway"),
            caseName: "migrationFailed", permanent: false,
            payloadWitness: "V54 stopped halfway"
        ),
        Case(
            error: .queryFailed("no such column: deferReason"),
            caseName: "queryFailed", permanent: false,
            payloadWitness: "no such column: deferReason"
        ),
        Case(
            error: .insertFailed("UNIQUE constraint failed: semantic_scan_results.id"),
            caseName: "insertFailed", permanent: false,
            payloadWitness: "UNIQUE constraint failed: semantic_scan_results.id"
        ),
        Case(
            error: .insertFailed("payloadTooLarge: 1048577 bytes"),
            caseName: "insertFailed", permanent: true,
            payloadWitness: "1048577 bytes"
        ),
        Case(error: .notFound, caseName: "notFound", permanent: false, payloadWitness: nil),
        Case(
            error: .duplicateJobId("fm-abc123"),
            caseName: "duplicateJobId", permanent: false,
            payloadWitness: "fm-abc123"
        ),
        Case(
            error: .invalidRow(column: 7),
            caseName: "invalidRow", permanent: true,
            payloadWitness: "column 7"
        ),
        Case(
            error: .invalidEvidenceEvent("atomOrdinals must be a JSON array"),
            caseName: "invalidEvidenceEvent", permanent: true,
            payloadWitness: "atomOrdinals must be a JSON array"
        ),
        Case(
            error: .invalidScanCohortJSON("not a decodable ScanCohort"),
            caseName: "invalidScanCohortJSON", permanent: true,
            payloadWitness: "not a decodable ScanCohort"
        ),
        Case(
            error: .invalidStateTransition(jobId: "fm-abc123", fromStatus: "complete", toStatus: "running"),
            caseName: "invalidStateTransition", permanent: false,
            payloadWitness: "complete -> running"
        ),
        Case(
            error: .evidenceEventBodyMismatch(id: "ev-1"),
            caseName: "evidenceEventBodyMismatch", permanent: true,
            payloadWitness: "ev-1"
        ),
        Case(
            error: .staleAdWindowRevision(id: "window-1"),
            caseName: "staleAdWindowRevision", permanent: false,
            payloadWitness: "window-1"
        ),
        Case(
            error: .encodingFailure("JSONEncoder produced non-UTF8 bytes"),
            caseName: "encodingFailure", permanent: true,
            payloadWitness: "JSONEncoder produced non-UTF8 bytes"
        )
    ]

    private func token(for entry: Case, phase: BackfillJobPhase = .fullEpisodeScan) -> String {
        StoreFailureRecord.deferReason(
            caseName: entry.caseName,
            isPermanent: entry.permanent,
            phase: phase
        )
    }

    // MARK: THE DEFECT

    /// WHAT THE OLD WRITE ACTUALLY PUT IN THE COLUMN, taken from the production
    /// type rather than from a comment.
    ///
    /// The bead predicted the REFLECTED spelling — `insertFailed("…")` — and it
    /// is not what `String(describing:)` produces here, because
    /// `AnalysisStoreError` conforms to `CustomStringConvertible` and that
    /// conformance wins. The distinction is not pedantic: it is the difference
    /// between a column that at least leads with the case name and one that, for
    /// `.notFound`, reads `Row not found` and names no case at all.
    ///
    /// This test is also the anti-vacuity control for the one below it. "The
    /// token does not contain the payload" proves nothing unless the thing it
    /// replaced demonstrably did.
    @Test("THE REAL DEFECT: the old write was the enum's PROSE, and it swallowed the case name")
    func theRealDefectIsTheProseNotTheReflection() {
        let insert = AnalysisStoreError.insertFailed("UNIQUE constraint failed: semantic_scan_results.id")
        // `String(describing:)` resolves to `description`, not to reflection.
        #expect(String(describing: insert) == insert.description)
        #expect(String(describing: insert).hasPrefix("Insert failed: "))
        #expect(!String(describing: insert).hasPrefix("insertFailed"))
        // The unbounded per-row half really is in there.
        #expect(String(describing: insert).contains("UNIQUE constraint failed: semantic_scan_results.id"))
        // And the worst of the thirteen: a persisted row that names no case.
        #expect(String(describing: AnalysisStoreError.notFound) == "Row not found")
        #expect(!String(describing: AnalysisStoreError.notFound).contains("notFound"))
        // Which the token repairs — this is the one case where the token carries
        // strictly MORE identity than the string it replaced, not less.
        let repaired = StoreFailureRecord.deferReason(
            caseName: BackfillJobRunner.caseName(of: .notFound),
            isPermanent: BackfillJobRunner.isPermanentForTesting(.notFound),
            phase: .fullEpisodeScan
        )
        #expect(repaired.contains("case=notFound"))
    }

    /// THE HEADLINE. The payload is gone from the durable value, for every case
    /// that has one.
    ///
    /// Written as an ABSENCE of the old value rather than a presence of the new
    /// one: "the token is present" would still pass against an implementation
    /// that APPENDED the description to it, which is exactly the hole 59c8 found
    /// in the generic arm's own rail.
    @Test("THE DEFECT: no case's payload reaches the durable value")
    func noPayloadReachesTheColumn() {
        var casesWithPayload = 0
        for entry in Self.allCases {
            let reason = token(for: entry)
            #expect(
                reason != String(describing: entry.error),
                "the column is still the description for \(entry.caseName)"
            )
            guard let witness = entry.payloadWitness else { continue }
            casesWithPayload += 1
            // The witness has to be REAL, or its absence from the token says
            // nothing at all about the fix.
            #expect(
                String(describing: entry.error).contains(witness),
                "the witness \(witness) is not in what the old write persisted for \(entry.caseName)"
            )
            #expect(
                !reason.contains(witness),
                "\(entry.caseName) leaked its payload into the column: \(reason)"
            )
        }
        #expect(casesWithPayload == 13, "only \(casesWithPayload) cases carry a payload — the table has drifted")
    }

    // MARK: Countability

    @Test("the token is prefixed by the CONDITION and names the phase")
    func theTokenIsPrefixedByTheCondition() {
        for phase in BackfillJobPhase.allCases {
            for entry in Self.allCases {
                let reason = token(for: entry, phase: phase)
                #expect(reason.hasPrefix("\(StoreFailureRecord.causePrefix)-"))
                #expect(reason.contains(phase.rawValue), "the phase is missing from \(reason)")
            }
        }
        #expect(BackfillJobPhase.allCases.count > 1, "vacuity: one phase cannot show the phase is carried")
    }

    /// The prefix must answer to nothing else that lands in this column, in
    /// BOTH directions — a new token that is a prefix of an existing one poisons
    /// the existing count just as badly as the reverse.
    @Test("the prefix collides with no other cause this column holds")
    func thePrefixCollidesWithNothingElse() {
        let mine = "\(StoreFailureRecord.causePrefix)-"
        var others: [String] = [
            "\(UnclassifiedModelFailure.causePrefix)-",
            SemanticScanClaim.deferReasonPrefix,
            "cancelled-during-",
            "underCoverage-",
            "underCoverageBudgetSpent-",
            "transcriptCeilingBelowFloor-",
            "thermalThrottled",
            "retranscribeFailed"
        ]
        others += FMDaemonRefusal.allCases.flatMap { [$0.passPrologueCause, $0.batchSiblingCause] }
        for phase in BackfillJobPhase.allCases {
            others.append(BackfillJobRunner.noProgressExpiryReason(phase: phase))
        }
        #expect(others.count >= 12, "vacuity: the comparison set is too small to prove disjointness")
        #expect(!others.contains { $0.isEmpty }, "vacuity: an empty needle is a prefix of everything")
        for other in others {
            #expect(!other.hasPrefix(mine), "\(other) would be counted as a store failure")
            #expect(!mine.hasPrefix(other), "a store failure would be counted as \(other)")
        }
    }

    /// The property a device pull actually uses: one `LIKE 'storeFailure-%'`
    /// returns the whole population and nothing else.
    @Test("a prefix query returns every store failure and nothing else")
    func aPrefixQueryPartitionsTheColumn() {
        let mine = Set(
            BackfillJobPhase.allCases.flatMap { phase in
                Self.allCases.map { token(for: $0, phase: phase) }
            }
        )
        let foreign = [
            UnclassifiedModelFailure.deferReason(for: CancellationError(), phase: .fullEpisodeScan),
            BackfillJobRunner.noProgressExpiryReason(phase: .fullEpisodeScan),
            "cancelled-during-fullEpisodeScan",
            "underCoverageBudgetSpent-fullEpisodeScan",
            FMDaemonRefusal.metadataStall.passPrologueCause + "(peers=0)"
        ]
        let predicate = { (value: String) in value.hasPrefix("\(StoreFailureRecord.causePrefix)-") }
        #expect(mine.allSatisfy(predicate), "a store failure escaped its own prefix")
        #expect(!foreign.contains(where: predicate), "a foreign cause was captured by the prefix")
        #expect(mine.count >= BackfillJobPhase.allCases.count, "vacuity: the population is degenerate")
    }

    // MARK: The permanence field

    /// `permanent=` is not a second ruler for `case=` — it is a quantity the case
    /// name cannot answer, and `insertFailed` is the proof.
    @Test("permanence is NOT derivable from the case name: insertFailed classifies both ways")
    func permanenceIsNotDerivableFromTheCaseName() {
        let recoverable = AnalysisStoreError.insertFailed("UNIQUE constraint failed: x")
        let permanent = AnalysisStoreError.insertFailed("payloadTooLarge: 1048577 bytes")
        // The production classifier, not a table in this file.
        #expect(BackfillJobRunner.isPermanentForTesting(recoverable) == false)
        #expect(BackfillJobRunner.isPermanentForTesting(permanent) == true)
        #expect(BackfillJobRunner.caseName(of: recoverable) == BackfillJobRunner.caseName(of: permanent))

        let recoverableToken = StoreFailureRecord.deferReason(
            caseName: BackfillJobRunner.caseName(of: recoverable),
            isPermanent: BackfillJobRunner.isPermanentForTesting(recoverable),
            phase: .fullEpisodeScan
        )
        let permanentToken = StoreFailureRecord.deferReason(
            caseName: BackfillJobRunner.caseName(of: permanent),
            isPermanent: BackfillJobRunner.isPermanentForTesting(permanent),
            phase: .fullEpisodeScan
        )
        #expect(
            recoverableToken != permanentToken,
            "two rows with different fates would be indistinguishable in the column"
        )
        #expect(recoverableToken.contains("permanent=false"))
        #expect(permanentToken.contains("permanent=true"))
    }

    /// Both values are written as a POSITIVE claim. `permanent=false` is a field
    /// with a value, never an omission a reader has to notice.
    @Test("both permanence values are stated, neither is an absence")
    func bothPermanenceValuesAreStated() {
        for entry in Self.allCases {
            let reason = token(for: entry)
            #expect(reason.contains("permanent=\(entry.permanent)"))
            #expect(
                reason.contains("permanent=true") != reason.contains("permanent=false"),
                "the field must state exactly one value: \(reason)"
            )
        }
    }

    @Test("the case name is carried verbatim for every case")
    func theCaseNameIsCarriedVerbatim() {
        // Exhaustiveness: a new `AnalysisStoreError` case fails compilation here
        // before it can ship with an untested token.
        for entry in Self.allCases {
            switch entry.error {
            case .openFailed, .migrationFailed, .queryFailed, .insertFailed, .notFound,
                 .duplicateJobId, .invalidRow, .invalidEvidenceEvent, .invalidScanCohortJSON,
                 .invalidStateTransition, .evidenceEventBodyMismatch, .staleAdWindowRevision,
                 .encodingFailure:
                break
            }
            // The table's own case name must be the PRODUCTION one, or every
            // assertion in this file is about a string this file made up.
            #expect(BackfillJobRunner.caseName(of: entry.error) == entry.caseName)
            #expect(BackfillJobRunner.isPermanentForTesting(entry.error) == entry.permanent)
            #expect(token(for: entry).contains("case=\(entry.caseName)"))
        }
        #expect(Set(Self.allCases.map(\.caseName)).count == 13, "the table no longer covers all thirteen cases")
    }

    // MARK: Grammar

    /// The token has to survive being read out of a space-separated witness line
    /// and a `key=value` parenthetical, so its own grammar must be closed.
    @Test("the token's grammar is closed: no whitespace, one balanced parenthetical")
    func theTokenGrammarIsClosed() {
        for phase in BackfillJobPhase.allCases {
            for entry in Self.allCases {
                let reason = token(for: entry, phase: phase)
                let whitespace = reason.filter(\.isWhitespace).count
                let opens = reason.filter { $0 == "(" }.count
                let closes = reason.filter { $0 == ")" }.count
                let equals = reason.filter { $0 == "=" }.count
                #expect(whitespace == 0, "whitespace in \(reason)")
                #expect(opens == 1, "unbalanced parens in \(reason)")
                #expect(closes == 1, "unbalanced parens in \(reason)")
                #expect(reason.hasSuffix(")"), "the parenthetical must close the token: \(reason)")
                #expect(equals == 2, "expected exactly two fields in \(reason)")
            }
        }
    }

    /// The limit named in `StoreFailureRecord`'s header, asserted rather than
    /// asserted-about: `PersistedStateInvariantEvaluator.sanitize` truncates a
    /// `deferReason` to 80 characters for its WITNESS line, and on the only phase
    /// any field row has ever carried the token fits inside it.
    @Test("on fullEpisodeScan the whole token survives the 80-character witness")
    func theTokenFitsTheWitnessOnTheFieldPhase() {
        let witnessBudget = 80
        let onFieldPhase = Self.allCases.map { token(for: $0, phase: .fullEpisodeScan).count }.max() ?? 0
        #expect(onFieldPhase > 0, "vacuity: no token was measured")
        #expect(
            onFieldPhase <= witnessBudget,
            "the widest fullEpisodeScan token is \(onFieldPhase) characters and the witness holds \(witnessBudget)"
        )
        // The honest other half, so the LIMIT in `StoreFailureRecord`'s header is
        // a measurement and not a worry: on the longest phase names the token
        // DOES outrun the witness. The COLUMN is what a device pull reads and it
        // holds the whole thing; the witness is a summary. This is asserted so
        // that a future phase rename cannot quietly turn the header's caveat into
        // either a lie or a redundancy.
        let widestOverall = BackfillJobPhase.allCases
            .flatMap { phase in Self.allCases.map { token(for: $0, phase: phase).count } }
            .max() ?? 0
        #expect(
            widestOverall > witnessBudget,
            "no phase now outruns the witness (widest \(widestOverall)); the header's LIMIT is stale"
        )
    }

    // MARK: Sanitization

    /// THE COUPLING RAIL. `StoreFailureRecord.sanitize` delegates to
    /// `UnclassifiedModelFailure.sanitize`, whose length budget is sized for a
    /// fully-qualified type name. A future bead that tightens that budget below
    /// 25 would silently truncate `evidenceEventBodyMismatch` in a durable
    /// column; this makes the coupling fail loudly instead.
    @Test("every real case name survives sanitization UNCHANGED")
    func everyCaseNameSurvivesSanitization() {
        for entry in Self.allCases {
            let name = BackfillJobRunner.caseName(of: entry.error)
            #expect(
                StoreFailureRecord.sanitize(name) == name,
                "sanitizing \(name) changed it — the shared budget no longer clears a case name"
            )
        }
    }

    /// The anti-vacuity control for the rail above: the sanitizer must be able to
    /// CHANGE something, or "it changed nothing" proves only that it does
    /// nothing.
    @Test("and the sanitizer really fires on a name that would break the grammar")
    func theSanitizerFires() {
        let hostile = [
            "two words",
            "has(parens)",
            "has,comma",
            "has=equals",
            String(repeating: "z", count: 200)
        ]
        for input in hostile {
            let cleaned = StoreFailureRecord.sanitize(input)
            #expect(cleaned != input, "\(input) passed through unchanged")
            for forbidden in [" ", "(", ")", ",", "="] {
                #expect(!cleaned.contains(forbidden), "\(cleaned) still contains \(forbidden)")
            }
            #expect(!cleaned.isEmpty)
        }
        // An empty name reads as a WORD, so the field is present and a reader can
        // tell "nothing was there" from "the field was dropped".
        #expect(StoreFailureRecord.sanitize("") == UnclassifiedModelFailure.unknownDomainToken)
        #expect(!StoreFailureRecord.sanitize("").isEmpty)
    }
}

// MARK: - End to end

/// The claim the unit tests above cannot make: that the RUNNER's typed arm
/// writes this token, against a real store and a real drain loop.
///
/// The seam is a malformed `scanCohortJSON`. `insertSemanticScanResult`
/// validates it before anything else, so a runner configured with one cannot
/// persist a scan row at all and the pass ends in a real
/// `AnalysisStoreError.invalidScanCohortJSON` — a case classified PERMANENT, so
/// the row is short-circuited straight to `AdmissionController.maxRetries`. That
/// pairing is what makes this test worth its wall clock: it pins that the
/// `permanent=true` in the column and the retirement in `retryCount` are the same
/// decision, which is the property no unit test of either function can see.
/// `BackfillCoarseCheckpointTests` uses the same injection for its own claim.
@Suite("playhead-sckv: the typed store arm, end to end")
struct StoreFailureRecordWireInTests {

    private static let segmentSeconds = 30.0

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: nil
        )
    }

    private func makeInputs(assetId: String, lineCount: Int) -> BackfillJobRunner.AssetInputs {
        let transcriptVersion = "tx-sckv-v1"
        var lines: [(Double, Double, String)] = []
        for idx in 0..<lineCount {
            let start = Double(idx) * Self.segmentSeconds
            lines.append((start, start + Self.segmentSeconds, "Editorial line \(idx) about the topic of the day."))
        }
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: lines
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-sckv",
            segments: segments,
            evidenceCatalog: EvidenceCatalogBuilder.build(
                atoms: segments.flatMap(\.atoms),
                analysisAssetId: assetId,
                transcriptVersion: transcriptVersion
            ),
            transcriptVersion: transcriptVersion,
            plannerContext: CoveragePlannerContext(
                observedEpisodeCount: 0,
                stableRecall: false,
                isFirstEpisodeAfterCohortInvalidation: false,
                recallDegrading: false,
                sponsorDriftDetected: false,
                auditMissDetected: false,
                episodesSinceLastFullRescan: 0,
                periodicFullRescanIntervalEpisodes: 10
            )
        )
    }

    @Test("a real AnalysisStoreError lands in the column as a token, and it names the fate the row was given")
    func aRealStoreErrorLandsAsAToken() async throws {
        let assetId = "asset-sckv-cohort"
        let store = try await makeTestStore()
        try await store.insertAsset(makeAsset(id: assetId))
        let fmRuntime = TestFMRuntime(tokenCountRule: { $0.count })
        let runner = BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: "{ this is not a decodable ScanCohort"
        )

        _ = try? await runner.runPendingBackfill(for: makeInputs(assetId: assetId, lineCount: 40))

        // The premise: the pass really did try to persist rows and really did
        // fail to. Without both halves the assertions below could be satisfied
        // by a run that planned nothing.
        #expect(
            await fmRuntime.coarseCallCount > 0,
            "the pass screened nothing, so no store write was ever attempted"
        )
        let rows = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        #expect(rows.isEmpty, "the injected failure did not actually stop the writes")

        let jobId = BackfillJobRunner.makeJobId(
            analysisAssetId: assetId,
            phase: .fullEpisodeScan,
            offset: 0
        )
        let row = try #require(await store.fetchBackfillJob(byId: jobId))
        #expect(row.status == .failed, "the DISPOSITION is unchanged — this arm fails the row")
        let reason = try #require(row.deferReason)

        // COUNTABLE: one prefix, and it names the phase.
        #expect(reason.hasPrefix("\(StoreFailureRecord.causePrefix)-"), "got \(reason)")
        #expect(reason.contains(BackfillJobPhase.fullEpisodeScan.rawValue))
        // DIAGNOSABLE: the case the store actually threw.
        #expect(reason.contains("case=invalidScanCohortJSON"), "got \(reason)")
        // AND THE PROSE IS GONE. Without this, appending the description to the
        // token would still satisfy everything above.
        #expect(
            !reason.contains(AnalysisStoreError.invalidScanCohortJSON("x").description.prefix(20)),
            "the enum's prose is back in the column: \(reason)"
        )

        // THE PAIRING. `invalidScanCohortJSON` is permanent, so the arm
        // short-circuits the budget — and the column says so in the same breath.
        // A row that reads `permanent=true` while carrying a charge of one, or
        // the reverse, is the defect this pairing exists to make visible.
        #expect(reason.contains("permanent=true"), "got \(reason)")
        #expect(
            row.retryCount == AdmissionController.maxRetries,
            "a permanent store error must retire the row: retryCount=\(row.retryCount)"
        )
    }
}

// MARK: - Source canary

/// The two claims no runtime assertion on ``StoreFailureRecord`` can observe,
/// because both are about the CALL SITE rather than about the value.
///
///  1. **The description is gone from the durable write.** A test can prove
///     `deferReason(caseName:isPermanent:phase:)` returns a named token and say
///     nothing at all about whether the runner still passes
///     `String(describing: storeError)` to `markBackfillJobFailed`. That literal
///     IS the defect, and it is invisible from anywhere else.
///  2. **The token names the decision the row was actually retired under.** The
///     runner could build a perfectly well-formed token from a hard-coded
///     `isPermanent: true` while charging the row on the real value, and every
///     runtime assertion above would still pass. So the rule is DERIVED: the
///     `isPermanent:` argument the token is built from must be the SAME
///     expression the retry arithmetic was given, and neither may be a literal.
///
/// Same technique and the same two normalizations as
/// `UnclassifiedModelFailureSourceCanaryTests`, reused rather than re-derived.
final class StoreFailureRecordSourceCanaryTests: XCTestCase {

    private func runnerSource() throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        let url = root
            .appendingPathComponent("Playhead/Services/AdDetection/BackfillJobRunner.swift")
        return FMDaemonRefusalSourceCanaryTests.codeLines(of: try String(contentsOf: url, encoding: .utf8))
    }

    /// The balanced-paren argument list of the FIRST call spelled `marker`,
    /// split into `label: value` pairs at depth 1.
    ///
    /// Written here rather than reused because `firstArguments(after:in:)`
    /// returns only the FIRST argument of each call, and every claim below is
    /// about arguments two and three.
    private func arguments(ofCall marker: String, in dense: String) throws -> [String: String] {
        let call = try XCTUnwrap(dense.range(of: marker), "no call to \(marker) in the runner")
        var depth = 1
        var index = call.upperBound
        var current = ""
        var pieces: [String] = []
        while index < dense.endIndex, depth > 0 {
            let character = dense[index]
            if character == "(" || character == "[" {
                depth += 1
            } else if character == ")" || character == "]" {
                depth -= 1
                if depth == 0 { break }
            } else if character == "," && depth == 1 {
                pieces.append(current)
                current = ""
                index = dense.index(after: index)
                continue
            }
            current.append(character)
            index = dense.index(after: index)
        }
        if !current.isEmpty { pieces.append(current) }
        var labelled: [String: String] = [:]
        for piece in pieces {
            guard let colon = piece.firstIndex(of: ":") else { continue }
            labelled[String(piece[piece.startIndex..<colon])] = String(piece[piece.index(after: colon)...])
        }
        return labelled
    }

    func testTheTypedStoreArmNoLongerPersistsTheDescription() throws {
        let lines = try runnerSource()
        XCTAssertGreaterThan(lines.count, 1_000, "source read found only \(lines.count) code lines")
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)

        // VACUITY FIRST: the arm this canary is about must still exist and must
        // still route through the named record. Without this the rule below
        // passes trivially against a runner that stopped failing jobs at all.
        XCTAssertTrue(
            dense.contains("StoreFailureRecord.deferReason("),
            "BackfillJobRunner no longer builds the store-failure record; move this canary with the code."
        )
        XCTAssertTrue(
            dense.contains("catchletstoreErrorasAnalysisStoreError"),
            "vacuity: the typed AnalysisStoreError arm is gone from the runner"
        )
        // And the vacuity guard must be able to FAIL: the forbidden spelling has
        // to be findable by this same reader, or its absence proves nothing.
        // `String(describing: storeError)` survives in the LOG line one call
        // below, deliberately — a log may carry prose, a column may not.
        XCTAssertTrue(
            dense.contains("String(describing:storeError)"),
            "vacuity: this reader cannot see the spelling it forbids, so its absence proves nothing"
        )

        // THE RULE.
        XCTAssertFalse(
            dense.contains("reason:String(describing:storeError)"),
            """
            The typed AnalysisStoreError arm is persisting `String(describing: storeError)` again. \
            That is playhead-sckv: the column stops being groupable — the payload of \
            `insertFailed(_:)` is a per-row SQLite message — and the spelling changes with any \
            rename or added associated value, with nothing in the type system noticing.
            """
        )
        // And no other durable write in the runner may reintroduce it either;
        // 59c8's rail covers `error`, this one covers every spelling that
        // reaches `reason:`.
        let reasonArguments = FMDaemonRefusalSourceCanaryTests.firstArguments(after: "reason:", in: dense)
        XCTAssertGreaterThanOrEqual(reasonArguments.count, 4, "vacuity: the durable writes were not found")
        let describing = reasonArguments.filter { $0.contains("String(describing:") }
        XCTAssertTrue(
            describing.isEmpty,
            "a durable `reason:` is a framework/Swift description again: \(describing)"
        )
    }

    func testTheTokenNamesTheDecisionTheRowWasRetiredUnder() throws {
        let lines = try runnerSource()
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)

        let record = try arguments(ofCall: "StoreFailureRecord.deferReason(", in: dense)
        let charge = try arguments(ofCall: "Self.storeFailureRetryCount(", in: dense)

        // Vacuity: both calls must have been parsed into real argument lists.
        XCTAssertEqual(Set(record.keys), ["caseName", "isPermanent", "phase"], "record args: \(record)")
        XCTAssertEqual(
            Set(charge.keys),
            ["priorRetryCount", "isPermanent", "bankedNewAudio"],
            "charge args: \(charge)"
        )

        let recordPermanence = try XCTUnwrap(record["isPermanent"])
        let chargePermanence = try XCTUnwrap(charge["isPermanent"])
        XCTAssertEqual(
            recordPermanence,
            chargePermanence,
            """
            The token's `isPermanent` (\(recordPermanence)) is not the value the retry charge used \
            (\(chargePermanence)). The column would then name a fate the row was not given — a row \
            short-circuited to maxRetries reading `permanent=false`, or the reverse. No runtime \
            assertion on either function can see this; both would be individually correct.
            """
        )
        for literal in ["true", "false"] {
            XCTAssertNotEqual(recordPermanence, literal, "the token hard-codes its permanence")
            XCTAssertNotEqual(chargePermanence, literal, "the retry charge hard-codes its permanence")
        }
        let caseName = try XCTUnwrap(record["caseName"])
        XCTAssertFalse(caseName.contains("\""), "the token hard-codes a case name: \(caseName)")
        XCTAssertTrue(
            dense.contains("let\(caseName)=Self.caseName(of:storeError)"),
            "`\(caseName)` is not the runner's own case-name read of `storeError`"
        )
    }

    /// The other half of the call site, split out so a mutation of the PHASE and
    /// a mutation of the PERMANENCE do not answer to one rail.
    ///
    /// A constant phase is the same defect one field over — every phase's rows
    /// counted under one name — and the write itself is what makes any of this
    /// durable at all: a token built and never passed to `markBackfillJobFailed`
    /// leaves the column exactly as broken as it was, with every value test in
    /// this file still green.
    func testTheTokenNamesTheJobsOwnPhaseAndReachesTheWrite() throws {
        let lines = try runnerSource()
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)
        let record = try arguments(ofCall: "StoreFailureRecord.deferReason(", in: dense)

        XCTAssertEqual(
            record["phase"],
            "job.phase",
            "the token must name THIS job's phase, not a constant — every phase would share one count"
        )

        // The token must actually reach the durable write, by the name it was
        // bound to — DERIVED, so a rename moves the rule with the code rather
        // than emptying it.
        let binding = "=StoreFailureRecord.deferReason("
        let prefix = try XCTUnwrap(dense.range(of: binding), "the record is never bound to a local").lowerBound
        let declaration = try XCTUnwrap(
            dense.range(of: "let", options: .backwards, range: dense.startIndex..<prefix),
            "no `let` precedes the record"
        )
        let local = String(dense[declaration.upperBound..<prefix])
        XCTAssertFalse(local.isEmpty, "could not read the local the record is bound to")
        XCTAssertTrue(
            local.allSatisfy { $0.isLetter || $0.isNumber || $0 == "_" },
            "`\(local)` is not an identifier — the backwards `let` search found a substring of a word"
        )
        XCTAssertTrue(
            dense.contains("reason:\(local),"),
            "`\(local)` is built but never written to the durable column"
        )
    }
}

// MARK: - The descriptor ceiling this host actually runs under (playhead-s34ux)

/// Prints the test host's own `RLIMIT_NOFILE` into every gate log, once.
///
/// THIS EXISTS BECAUSE THE NUMBER WAS MISSING AND ITS ABSENCE COST A WEEK.
/// `playhead-s34ux` was filed on `SQLITE_CANTOPEN` in a rotating set of suites.
/// The descriptor count was measured from OUTSIDE the process — 2,539 peak —
/// compared against `kern.maxfilesperproc` (61,440), read as 4.1 % of the
/// ceiling, and the hypothesis was written up as REFUTED. It was not.
/// **`kern.maxfilesperproc` is not the limit that binds.** This host's
/// `RLIMIT_NOFILE` soft limit is 2,560, so the true reading was 99.2 % — at the
/// ceiling — and the same run's `maxfd` of 2,559 (soft − 1) says the table was
/// completely full, because `open()` hands out the lowest free descriptor.
///
/// **macOS cannot report another process's `RLIMIT_NOFILE`**, so no external
/// sampler can ever supply this and `scripts/gate-memory-sample.py` deliberately
/// does not pretend to. A test inside the host is the only instrument that can,
/// which is why this is a test and not a script.
///
/// ## Why it is one shot and not a series
///
/// The bead's own probe looped for 300 s at a 2 s interval, scanning 70,000
/// descriptor numbers each time — ~35,000 syscalls a second, on every gate,
/// forever, and it extended every run to its own 300 s floor. The SERIES is
/// expensive and is already collected from outside by the memory sampler. The
/// LIMIT is free, it never changes within a run, and it is the half that was
/// missing. So this prints once and returns.
///
/// `libproc.h` is absent from the iPhoneSimulator SDK, so
/// `proc_pidinfo(PROC_PIDLISTFDS)` is unavailable here and the live count is
/// taken by probing descriptor numbers with `fcntl(F_GETFD)`. That scan is
/// CAPPED, and it reports whether it hit the cap, because a count that silently
/// stops at its own bound is a value that names one thing read as though it
/// named another — the defect class this whole bead is made of.
@Suite("the test host's descriptor ceiling is on the record (playhead-s34ux)")
struct TestHostDescriptorCeilingTests {

    /// Bounded so a host with a huge soft limit cannot turn one reading into a
    /// million syscalls. Above every soft limit observed on this box (2,560)
    /// and above `kern.maxfilesperproc` (61,440), so on this box it cannot
    /// truncate — and it says so when it does.
    private static let scanCap: Int32 = 70_000

    private static func openDescriptors() -> (count: Int, maxFD: Int32, hitCap: Bool) {
        var count = 0
        var maxFD: Int32 = -1
        var fd: Int32 = 0
        while fd < scanCap {
            if fcntl(fd, F_GETFD) != -1 {
                count += 1
                maxFD = fd
            }
            fd += 1
        }
        return (count, maxFD, maxFD >= scanCap - 1)
    }

    @Test("the host's RLIMIT_NOFILE is printed into the log")
    func theDescriptorCeilingIsOnTheRecord() throws {
        var limit = rlimit()
        let code = getrlimit(RLIMIT_NOFILE, &limit)
        // A FAILED read must not be printed as a limit. `getrlimit` returning
        // non-zero leaves `limit` untouched, and printing whatever it happened
        // to contain would put a fabricated ceiling in the log — which is
        // exactly the reading that started this bead.
        guard code == 0 else {
            print("[s34ux-fd] pid=\(getpid()) RLIMIT_NOFILE UNREADABLE "
                  + "(getrlimit rc=\(code), errno=\(errno)) — this run says "
                  + "NOTHING about the descriptor ceiling")
            #expect(Bool(true), "an unreadable limit is reported, never invented")
            return
        }
        let sample = Self.openDescriptors()
        print("[s34ux-fd] pid=\(getpid()) RLIMIT_NOFILE soft=\(limit.rlim_cur) "
              + "hard=\(limit.rlim_max) fds=\(sample.count) maxfd=\(sample.maxFD) "
              + "hitCap=\(sample.hitCap) scanCap=\(Self.scanCap)")
        // The reading is the point; the assertion only pins that a limit was
        // obtained at all. A soft limit of zero would mean the host cannot open
        // anything, which is not a state any test could have reached.
        #expect(limit.rlim_cur > 0, "a soft limit of 0 is not a limit, it is a failed read")
        #expect(sample.count > 0, "a host running a test holds at least stdin/stdout/stderr")
    }
}
