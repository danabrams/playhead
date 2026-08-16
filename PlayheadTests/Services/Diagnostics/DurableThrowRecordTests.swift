// DurableThrowRecordTests.swift
// playhead-3lc3 — four production sites wrote a Swift value's DESCRIPTION into
// three durable columns.
//
// THE FOUR LINES, as they stood:
//
//   AnalysisWorkScheduler   lastErrorCode: "\(maxAttemptsReachedPrefix)\(error.localizedDescription)"
//   AnalysisWorkScheduler   lastErrorCode: error.localizedDescription
//   AnalysisCoordinator     failureReason: String(describing: error)      (+ the DEBUG seam)
//   BackgroundProcessing…   lastErrorCode: String(describing: error)
//
// WHAT THEY ACTUALLY WROTE IS MEASURED HERE, NOT PREDICTED. `playhead-sckv`
// established that reading the defect off a bead rather than off the running
// program produces a test asserting on a string the app never wrote: it
// expected `String(describing:)`'s REFLECTED form and got the enum's PROSE,
// because `AnalysisStoreError` conforms to `CustomStringConvertible` and
// `String(describing:)` prefers that. `AnalysisCoordinatorError` conforms too.
// So every claim below about the OLD spelling is derived from the production
// type at run time — `theRealDefect…` prints and asserts it — and never from
// this comment.
//
// AND `localizedDescription` IS THE WORSE OF THE TWO. Neither error type
// conforms to `LocalizedError`, so Foundation synthesises it from the bridged
// `NSError`: a LOCALIZED sentence plus the enum's DECLARATION INDEX. Two
// separate ways for one failure to answer to two strings — a device in another
// language, and a case inserted above it in the enum.
//
// WHAT THIS FILE PINS.
//
//   * The three tokens are prefixed by their own CONDITION and collide with
//     nothing else the three columns hold.
//   * No case's PAYLOAD reaches any durable value.
//   * `analysis_jobs`'s ONE content-reading consumer still works:
//     `isAttemptCapTerminal(_:)` matches the terminal arm's row and not the
//     retry arm's. That predicate drives the cap-out-retry rescue, so it is
//     load-bearing and is asserted from BOTH directions.
//   * `analysis_sessions`'s ONE content-reading consumer cannot be confused:
//     `coverageGuardRecoveryVerdict` reads `.skipUnrelated` for every token this
//     type can emit, for every `SessionState`.
//   * The session token names the `resumeState` the write DESTROYS.
//   * The grammar is closed: no whitespace, one balanced parenthetical, and the
//     `under=` field is a positive claim rather than an absence.
//   * WIRE-IN: a real `AnalysisCoordinatorError` thrown by the real pipeline
//     lands in a real SQLite column as a token, read back through the store.
//
// WHICH SITES HAVE A RUNTIME WITNESS AND WHICH DO NOT, stated rather than
// implied. Only the coordinator's does, and the reason is the population that
// reaches each catch:
//
//   * `AnalysisWorkScheduler`'s outer catch stands over `runTask.value`, and
//     `AnalysisJobRunner.run(_:)` is NON-throwing — it returns an
//     `AnalysisOutcome`. The only throw the inner task can raise is
//     `CancellationError`, which a nearer `catch is CancellationError` consumes.
//     What is left is a handful of bare `try await store.…` calls in the same
//     `do`, over a CONCRETE `AnalysisStore` with no protocol seam. Reaching it
//     from a test would mean adding a throw-injection hook to production, which
//     is a bigger change than the one this bead is making.
//   * `BackgroundProcessingService`'s recovery arm stands over
//     `AnalysisJobReconciler.reconcile()`, and `AnalysisJobReconciler` is a
//     concrete final type, not a protocol. Same conclusion.
//
// Both are therefore carried by the SOURCE CANARY at the bottom of this file,
// which is the only instrument that can see the defect class those two sites are
// most exposed to anyway: a call whose VALUE is a perfect token and whose
// ARGUMENT is wrong.

import Foundation
import Testing
import XCTest
@testable import Playhead

// MARK: - The token

@Suite("DurableThrowRecord — the durable cause is a named token")
struct DurableThrowRecordTests {

    /// Every `AnalysisCoordinatorError` case, with a payload where one exists,
    /// so a payload that leaked into a token would be visible.
    private static let coordinatorErrors: [AnalysisCoordinatorError] = [
        .invalidTransition(from: .backfill, to: .completeFull),
        .noAudioAvailable(episodeId: "EP-8F3D-42C6-A205-F49CACF66517"),
        .sessionNotFound(id: "SESSION-072FBAF8-95C2-4535-A780-359012FA10C6"),
        .storeError(underlying: AnalysisStoreError.insertFailed(
            "UNIQUE constraint failed: analysis_assets.episodeId, analysis_assets.assetFingerprint"
        )),
    ]

    /// The payload substrings that must never reach a durable value.
    private static let payloads = [
        "EP-8F3D-42C6-A205-F49CACF66517",
        "SESSION-072FBAF8-95C2-4535-A780-359012FA10C6",
        "UNIQUE constraint failed",
        "analysis_assets.episodeId",
    ]

    // MARK: The defect, measured from the production types

    @Test("THE REAL DEFECT: localizedDescription wrote a LOCALIZED sentence and an enum ORDINAL")
    func theRealDefectTheLocalizedSites() throws {
        // The two sites this covers wrote `error.localizedDescription` into
        // `analysis_jobs.lastErrorCode`. Neither error type conforms to
        // `LocalizedError`, so what Foundation synthesises is the bridged
        // NSError's description. Derived here, not quoted.
        for error in Self.coordinatorErrors {
            let bridged = error as NSError
            let old = error.localizedDescription

            // It IS the bridged form — i.e. localization is in play, and the
            // only identifying content is the domain and the code.
            #expect(old == bridged.localizedDescription)
            #expect(old.contains("\(bridged.code)"),
                    "the old value's only identifier was the enum's declaration index: \(old)")

            // And it names NO case. `noAudioAvailable` does not appear in
            // "The operation couldn't be completed. (Playhead.AnalysisCoordinatorError error 1.)"
            #expect(!old.contains("noAudioAvailable"))
            #expect(!old.contains("invalidTransition"))
            #expect(!old.contains("sessionNotFound"))

            // The token, by contrast, names the domain and the code as FIELDS a
            // `GROUP BY` can split on.
            let token = DurableThrowRecord.jobLastErrorCode(for: error)
            #expect(token.contains("domain=\(bridged.domain)"))
            #expect(token.contains("code=\(bridged.code)"))
        }
    }

    @Test("THE REAL DEFECT: the exact spellings, pinned from the running program")
    func theExactOldSpellings() throws {
        // The header comments of `DurableThrowRecord.swift` and this file quote
        // two strings. They are QUOTED there and MEASURED here, so a reader is
        // never trusting either comment — and so that a future Foundation or
        // Swift change to the bridging shows up as a red rail rather than as a
        // paragraph that quietly became false.
        let notFound = AnalysisStoreError.notFound
        let bridgedStore = notFound as NSError
        print("[3lc3] AnalysisStoreError.notFound"
            + " domain=\(bridgedStore.domain) code=\(bridgedStore.code)")
        print("[3lc3]   localizedDescription = \(notFound.localizedDescription)")
        print("[3lc3]   String(describing:)  = \(String(describing: notFound))")

        #expect(bridgedStore.domain == "Playhead.AnalysisStoreError")
        // **12, AND THE FIRST DRAFT OF THIS TEST PREDICTED 4.** `notFound` is
        // the FIFTH case declared, so a reader who assumes "declaration index"
        // gets 4. What the bridge actually uses is the enum's TAG, and a
        // multi-payload enum lays its payload cases out first (declaration
        // order) and its EMPTY cases after them — exactly the layout
        // `UnclassifiedModelFailure`'s header derives for `ModelManagerError`
        // ("29 payload cases (tags 0…28) then 20 empty cases"). Twelve of these
        // thirteen cases carry a payload, so the one empty case lands at 12.
        //
        // Which makes the defect WORSE than "an ordinal, not a name": ADDING AN
        // ASSOCIATED VALUE TO ANY EXISTING CASE renumbers every empty case
        // beneath it. The persisted number moves without a single case being
        // added, renamed or reordered — and this was measured only because the
        // prediction was written down first and turned red.
        #expect(bridgedStore.code == 12)
        #expect(notFound.localizedDescription.contains("Playhead.AnalysisStoreError error 12"))
        // U+2019, not an ASCII apostrophe — so even a same-locale GROUP BY is
        // one Unicode normalisation away from splitting the population.
        #expect(notFound.localizedDescription.contains("\u{2019}"))
        // And `String(describing:)` is the enum's own prose, naming no case.
        #expect(String(describing: notFound) == "Row not found")

        let noAudio = AnalysisCoordinatorError.noAudioAvailable(episodeId: "EP-1")
        let bridgedCoordinator = noAudio as NSError
        print("[3lc3] AnalysisCoordinatorError.noAudioAvailable"
            + " domain=\(bridgedCoordinator.domain) code=\(bridgedCoordinator.code)")
        print("[3lc3]   localizedDescription = \(noAudio.localizedDescription)")
        print("[3lc3]   String(describing:)  = \(String(describing: noAudio))")
        #expect(bridgedCoordinator.domain == "Playhead.AnalysisCoordinatorError")
        #expect(bridgedCoordinator.code == 1)
        #expect(noAudio.localizedDescription.contains("Playhead.AnalysisCoordinatorError error 1"))
    }

    @Test("THE REAL DEFECT: String(describing:) wrote the enum's PROSE, not its case")
    func theRealDefectTheDescribingSites() throws {
        // `AnalysisCoordinatorError` conforms to `CustomStringConvertible`, and
        // `String(describing:)` prefers that conformance over reflection. So the
        // two `String(describing: error)` sites persisted a SENTENCE, carrying
        // the payload and naming no case — the same trap playhead-sckv measured
        // on `AnalysisStoreError`.
        for error in Self.coordinatorErrors {
            let old = String(describing: error)
            #expect(old == error.description,
                    "String(describing:) must resolve to the CustomStringConvertible prose")
            #expect(!old.hasPrefix("noAudioAvailable("),
                    "the reflected spelling is NOT what this site wrote: \(old)")
        }

        // The concrete demonstration, one case, spelled out.
        let noAudio = AnalysisCoordinatorError.noAudioAvailable(episodeId: "EP-1")
        #expect(String(describing: noAudio) == "No cached audio available for episode EP-1")
    }

    @Test("THE DEFECT: no case's payload reaches any of the three durable values")
    func noPayloadReachesTheDurableValue() throws {
        for error in Self.coordinatorErrors {
            let tokens = [
                DurableThrowRecord.jobLastErrorCode(for: error),
                DurableThrowRecord.sessionFailureReason(for: error, resumeState: .backfill),
                DurableThrowRecord.backgroundTaskLastErrorCode(for: error),
            ]
            for token in tokens {
                for payload in Self.payloads {
                    #expect(!token.contains(payload),
                            "payload '\(payload)' leaked into the durable value: \(token)")
                }
            }
        }
    }

    // MARK: The family

    @Test("each token is prefixed by the CONDITION it records")
    func eachTokenNamesItsCondition() throws {
        let error = AnalysisStoreError.notFound
        #expect(DurableThrowRecord.jobLastErrorCode(for: error)
            .hasPrefix(DurableThrowRecord.jobThrewPrefix + "("))
        #expect(DurableThrowRecord.sessionFailureReason(for: error, resumeState: .queued)
            .hasPrefix(DurableThrowRecord.sessionPipelineThrewPrefix + "-"))
        #expect(DurableThrowRecord.backgroundTaskLastErrorCode(for: error)
            .hasPrefix(DurableThrowRecord.recoveryThrewPrefix + "("))
    }

    @Test("the three prefixes collide with nothing else these columns hold")
    func thePrefixesAreDisjointFromEveryOtherCause() throws {
        // Every other value observed in, or writable to, the three columns.
        // The first four are the ONLY distinct values ever seen in a device
        // pull (db-pull8 through db-pull12).
        let incumbents = [
            "coverageInsufficient:noProgress",
            "backgroundWindowExpired",
            "transcription:cancelled",
            "assetResolution: Insert failed: UNIQUE constraint failed: analysis_assets.episodeId",
            "orphan_at_launch",
            "reconciler_unavailable",
            "staleFingerprint:cachedAudioMismatch",
            AnalysisWorkScheduler.maxAttemptsReachedPrefix + "cancelMidRun",
            AnalysisWorkScheduler.maxAttemptsReachedPrefix + "coverageInsufficient",
            AnalysisCoordinator.coverageGuardFailureReasonPrefix + "689.8/3600.0s (ratio 0.192 < 0.950)",
            "Unknown state in DB: bogus",
            "corrupt session state on resume: bogus",
        ]
        let prefixes = [
            DurableThrowRecord.jobThrewPrefix,
            DurableThrowRecord.sessionPipelineThrewPrefix,
            DurableThrowRecord.recoveryThrewPrefix,
        ]
        #expect(Set(prefixes).count == prefixes.count, "two conditions share one prefix")
        for incumbent in incumbents {
            for prefix in prefixes {
                #expect(!incumbent.hasPrefix(prefix),
                        "'\(incumbent)' would be counted as \(prefix)")
            }
        }
        // And the reverse direction: no token answers to an incumbent's prefix.
        let error = AnalysisStoreError.notFound
        let tokens = [
            DurableThrowRecord.jobLastErrorCode(for: error),
            DurableThrowRecord.sessionFailureReason(for: error, resumeState: .spooling),
            DurableThrowRecord.backgroundTaskLastErrorCode(for: error),
        ]
        for token in tokens {
            for incumbent in incumbents {
                #expect(!token.hasPrefix(String(incumbent.prefix(12))),
                        "\(token) collides with \(incumbent)")
            }
        }
    }

    @Test("a prefix query returns every throw of one kind and nothing else")
    func aPrefixQueryCountsThePopulation() throws {
        let rows = [
            DurableThrowRecord.jobLastErrorCode(for: AnalysisStoreError.notFound),
            AnalysisWorkScheduler.maxAttemptsReachedPrefix
                + DurableThrowRecord.jobLastErrorCode(for: AnalysisStoreError.notFound),
            "coverageInsufficient:noProgress",
            "backgroundWindowExpired",
        ]
        // The retry arm alone.
        #expect(rows.filter { $0.hasPrefix(DurableThrowRecord.jobThrewPrefix) }.count == 1)
        // Both arms, which is what an operator counting "unclassified job
        // throws" wants — and is exactly why the token is a SUFFIX of the
        // attempt-cap prefix rather than a competing family.
        #expect(rows.filter { $0.contains(DurableThrowRecord.jobThrewPrefix) }.count == 2)
    }

    // MARK: The readers

    @Test("the terminal arm still satisfies isAttemptCapTerminal, and the retry arm still does not")
    func theCapOutRescueStillReadsTheTerminalRow() throws {
        let token = DurableThrowRecord.jobLastErrorCode(for: AnalysisStoreError.notFound)

        // The terminal arm writes `maxAttemptsReached:` + the token, at
        // state `superseded`. That is the row the cap-out-retry rescue looks
        // for; if the prefix stopped leading, the rescue would silently stop
        // finding capped rows and every capped episode would be abandoned.
        let terminal = Self.job(
            state: "superseded",
            lastErrorCode: AnalysisWorkScheduler.maxAttemptsReachedPrefix + token
        )
        #expect(AnalysisWorkScheduler.isAttemptCapTerminal(terminal))
        #expect(AnalysisWorkScheduler.isRescuableTerminal(terminal))

        // The retry arm writes the bare token at state `failed`. It is neither.
        let retried = Self.job(state: "failed", lastErrorCode: token)
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(retried))
        #expect(!AnalysisWorkScheduler.isRescuableTerminal(retried))

        // And the OTHER terminal predicate, an exact match, is untouched by
        // either spelling.
        #expect(!AnalysisWorkScheduler.isNoProgressTerminal(
            Self.job(state: "complete", lastErrorCode: token)
        ))
    }

    @Test("no session token can be mistaken for a coverage-guard failure")
    func theCoverageGuardSweepSkipsEveryToken() throws {
        // `analysis_sessions.failureReason` has the only SQL predicate over any
        // of the three columns — `fetchFailedSessions(withFailureReasonPrefix:)`
        // with `"transcript coverage "` — and its verdict PARSES A DOUBLE out of
        // the matched string and divides by it. A token that matched would be
        // fed to a parser expecting `<cov>/<dur>s`.
        for state in SessionState.allCases {
            for error in Self.coordinatorErrors {
                let token = DurableThrowRecord.sessionFailureReason(for: error, resumeState: state)
                #expect(!token.hasPrefix(AnalysisCoordinator.coverageGuardFailureReasonPrefix))
                #expect(AnalysisCoordinator.coverageGuardRecoveryVerdict(
                    failureReason: token,
                    coverageEnd: 3_500
                ) == .skipUnrelated)
                #expect(AnalysisCoordinator.parseCoverageGuardEpisodeDuration(from: token) == nil)
            }
        }
    }

    @Test("the session token names the resumeState the write destroys")
    func theSessionTokenNamesTheResumeState() throws {
        // `transition(to: .failed)` overwrites `analysis_sessions.state`, so the
        // state the pipeline was DRIVING is unreadable from the row afterwards.
        // Every one must produce a distinct token, or two different failures
        // answer to one string.
        let error = AnalysisStoreError.notFound
        var seen: Set<String> = []
        for state in SessionState.allCases {
            let token = DurableThrowRecord.sessionFailureReason(for: error, resumeState: state)
            #expect(token.contains("-\(state.rawValue)("),
                    "\(token) does not name \(state.rawValue)")
            seen.insert(token)
        }
        #expect(seen.count == SessionState.allCases.count,
                "two resume states produced the same durable reason")
    }

    // MARK: The grammar

    @Test("the token's grammar is closed: no whitespace, one balanced parenthetical")
    func theGrammarIsClosed() throws {
        // A domain that would break a `key=value` record if it were passed
        // through: whitespace, parentheses, a comma and an equals sign.
        let hostile = NSError(
            domain: "Some Module.Weird (Type), key=value",
            code: 7,
            userInfo: nil
        )
        let tokens = [
            DurableThrowRecord.jobLastErrorCode(for: hostile),
            DurableThrowRecord.sessionFailureReason(for: hostile, resumeState: .hotPathReady),
            DurableThrowRecord.backgroundTaskLastErrorCode(for: hostile),
        ]
        for token in tokens {
            #expect(!token.contains(" "), "whitespace in \(token)")
            #expect(token.filter { $0 == "(" }.count == 1, "not one parenthetical: \(token)")
            #expect(token.filter { $0 == ")" }.count == 1, "not one parenthetical: \(token)")
            #expect(token.hasSuffix(")"))
            // Exactly three fields, so a dropped one is visible.
            #expect(token.filter { $0 == "=" }.count == 3, "field count changed: \(token)")
        }
    }

    @Test("and the sanitizer really fires on a domain that would break the grammar")
    func theSanitizerFires() throws {
        // Vacuity: without this, the closure test above could pass because the
        // fixture was harmless rather than because the sanitizer works.
        let hostile = NSError(domain: "A B,C=D(E)", code: 1, userInfo: nil)
        let token = DurableThrowRecord.jobLastErrorCode(for: hostile)
        #expect(token.contains("domain=A_BCDE"), "sanitizer did not fire: \(token)")
    }

    @Test("under= is a positive claim, never an absence")
    func underIsStated() throws {
        let bare = NSError(domain: "Bare.Domain", code: 3, userInfo: nil)
        #expect(DurableThrowRecord.jobLastErrorCode(for: bare)
            .contains("under=\(UnclassifiedModelFailure.noUnderlyingToken)"))

        let inner = NSError(domain: "Inner.Domain", code: 1001, userInfo: nil)
        let outer = NSError(
            domain: "Outer.Domain",
            code: -1,
            userInfo: [NSUnderlyingErrorKey: inner]
        )
        let token = DurableThrowRecord.jobLastErrorCode(for: outer)
        #expect(token.contains("domain=Outer.Domain"))
        #expect(token.contains("code=-1"))
        #expect(token.contains("under=Inner.Domain/1001"))
        #expect(!token.contains("under=\(UnclassifiedModelFailure.noUnderlyingToken)"))
    }

    @Test("the deepest underlying link is the one carried, not the first")
    func theDeepestLinkWins() throws {
        let deepest = NSError(domain: "Deepest.Domain", code: 42, userInfo: nil)
        let middle = NSError(
            domain: "Middle.Domain",
            code: 2,
            userInfo: [NSUnderlyingErrorKey: deepest]
        )
        let outer = NSError(
            domain: "Outer.Domain",
            code: 1,
            userInfo: [NSUnderlyingErrorKey: middle]
        )
        let token = DurableThrowRecord.backgroundTaskLastErrorCode(for: outer)
        #expect(token.contains("under=Deepest.Domain/42"))
        #expect(!token.contains("Middle.Domain"))
    }

    @Test("the three factories share ONE identity grammar")
    func theIdentityGrammarIsShared() throws {
        // Not three copies of `domain=…,code=…,under=…`: one, delegated to
        // `UnclassifiedModelFailure`, so a future bead that tightens the domain
        // budget cannot move two of the three columns and leave the third.
        let error = AnalysisStoreError.openFailed(code: 14, message: "unable to open database file")
        let fields = DurableThrowRecord.identityFields(of: error)
        #expect(DurableThrowRecord.jobLastErrorCode(for: error).contains(fields))
        #expect(DurableThrowRecord.sessionFailureReason(for: error, resumeState: .queued).contains(fields))
        #expect(DurableThrowRecord.backgroundTaskLastErrorCode(for: error).contains(fields))
    }

    // MARK: -

    private static func job(state: String, lastErrorCode: String?) -> AnalysisJob {
        AnalysisJob(
            jobId: "JOB-1",
            jobType: "preAnalysis",
            episodeId: "EP-1",
            podcastId: nil,
            analysisAssetId: "ASSET-1",
            workKey: "wk-1",
            sourceFingerprint: "fp-1",
            downloadId: "dl-1",
            priority: 0,
            desiredCoverageSec: 600,
            featureCoverageSec: 0,
            transcriptCoverageSec: 0,
            cueCoverageSec: 0,
            state: state,
            attemptCount: 5,
            nextEligibleAt: nil,
            leaseOwner: nil,
            leaseExpiresAt: nil,
            lastErrorCode: lastErrorCode,
            createdAt: 0,
            updatedAt: 0
        )
    }
}

// MARK: - Wire-in: a real throw, through the real pipeline, into a real column

@Suite("DurableThrowRecord — a real AnalysisCoordinatorError lands in the column as a token")
struct DurableThrowRecordWireInTests {

    @Test("the pipeline's catch-all persists the token, and the coverage-guard sweep skips it")
    func aRealThrowLandsAsAToken() async throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("DurableThrowRecordWireIn-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let store = try AnalysisStore(directory: dir)

        let assetId = "ASSET-\(UUID().uuidString)"
        let sessionId = "SESSION-\(UUID().uuidString)"
        let episodeId = "EP-\(UUID().uuidString)"

        // A session parked in `.backfill` with ZERO transcript chunks. The
        // resume branch of `runFromBackfill` decides `.restart` and throws
        // `AnalysisCoordinatorError.noAudioAvailable` — which is the production
        // road into the catch-all this bead changed.
        try await store.insertAsset(AnalysisAsset(
            id: assetId,
            episodeId: episodeId,
            assetFingerprint: "fp-\(assetId)",
            weakFingerprint: nil,
            sourceURL: "file:///test/\(assetId).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: SessionState.backfill.rawValue,
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 600
        ))
        try await store.insertSession(AnalysisSession(
            id: sessionId,
            analysisAssetId: assetId,
            state: SessionState.backfill.rawValue,
            startedAt: Date().timeIntervalSince1970,
            updatedAt: Date().timeIntervalSince1970,
            failureReason: nil
        ))

        let speechService = SpeechService(vocabularyProvider: ASRVocabularyProvider(store: store))
        let coordinator = AnalysisCoordinator(
            store: store,
            audioService: AnalysisAudioService(),
            featureService: FeatureExtractionService(store: store),
            transcriptEngine: TranscriptEngineService(speechService: speechService, store: store),
            capabilitiesService: CapabilitiesService(),
            adDetectionService: AdDetectionService(
                store: store,
                metadataExtractor: FallbackExtractor(),
                backfillJobRunnerFactory: nil,
                canUseFoundationModelsProvider: { false }
            ),
            skipOrchestrator: SkipOrchestrator(store: store)
        )
        await coordinator.resumeBackfillForTesting(
            sessionId: sessionId,
            assetId: assetId,
            episodeId: episodeId
        )
        await coordinator.stop()

        let session = try #require(try await store.fetchSession(id: sessionId))
        #expect(session.state == SessionState.failed.rawValue)
        let reason = try #require(session.failureReason)

        // It is the token, and it names the state the pipeline was driving.
        let expected = DurableThrowRecord.sessionFailureReason(
            for: AnalysisCoordinatorError.noAudioAvailable(episodeId: episodeId),
            resumeState: .backfill
        )
        #expect(reason == expected, "column holds '\(reason)', expected '\(expected)'")
        #expect(reason.hasPrefix(DurableThrowRecord.sessionPipelineThrewPrefix + "-backfill("))

        // It is NOT the prose the site used to write, and the episodeId the
        // thrown case carries did not reach the column.
        let oldSpelling = String(
            describing: AnalysisCoordinatorError.noAudioAvailable(episodeId: episodeId)
        )
        #expect(reason != oldSpelling)
        #expect(!reason.contains(episodeId))

        // And the one SQL consumer of this column leaves it alone.
        let sweepCandidates = try await store.fetchFailedSessions(
            withFailureReasonPrefix: AnalysisCoordinator.coverageGuardFailureReasonPrefix
        )
        #expect(sweepCandidates.isEmpty,
                "the coverage-guard sweep selected a row it cannot parse: \(sweepCandidates.map(\.id))")
    }
}

// MARK: - Source canary: a token can be a perfect VALUE and a wrong ARGUMENT

/// The only instrument that can see the defect class the four call sites are
/// exposed to once the value tests are green.
///
/// `playhead-sckv`'s SF04/SF10 pair is the worked example: hard-code ONE
/// argument of a two-field token and the token stays well-formed, every pure
/// test stays green, and the column names a fate the row was not given. Here the
/// same shape is `resumeState:` — a literal `.queued` at the `runPipeline` site
/// produces a perfectly grammatical `sessionPipelineThrew-queued(…)` for a
/// session that died in `backfill`, and nothing that reads a value can tell.
final class DurableThrowRecordSourceCanaryTests: XCTestCase {

    private func source(_ relativePath: String) throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        let url = root.appendingPathComponent(relativePath)
        return FMDaemonRefusalSourceCanaryTests.codeLines(of: try String(contentsOf: url, encoding: .utf8))
    }

    private func schedulerSource() throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        try source("Playhead/Services/PreAnalysis/AnalysisWorkScheduler.swift")
    }

    private func coordinatorSource() throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        try source("Playhead/Services/AnalysisCoordinator/AnalysisCoordinator.swift")
    }

    private func backgroundSource() throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        try source("Playhead/Services/AnalysisCoordinator/BackgroundProcessingService.swift")
    }

    /// Every `label:` argument of the FIRST call spelled `marker`, at depth 1.
    private func arguments(ofCall marker: String, in dense: String) throws -> [String: String] {
        let call = try XCTUnwrap(dense.range(of: marker), "no call to \(marker)")
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

    // MARK: The rule, per column

    func testTheSchedulerArmsNoLongerPersistADescription() throws {
        let lines = try schedulerSource()
        XCTAssertGreaterThan(lines.count, 3_000, "source read found only \(lines.count) code lines")
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)

        // VACUITY: the arms must still exist and must route through the record.
        XCTAssertTrue(
            dense.contains("DurableThrowRecord.jobLastErrorCode(for:error)"),
            "AnalysisWorkScheduler no longer builds the durable throw record; move this canary with the code."
        )
        XCTAssertTrue(dense.contains("\"outerCatch.supersede\""), "vacuity: the terminal arm is gone")
        XCTAssertTrue(dense.contains("\"outerCatch.requeue\""), "vacuity: the retry arm is gone")
        // And this reader must be ABLE to see the spelling it forbids —
        // `error.localizedDescription` survives in the journal `extras`
        // deliberately (a different column, filed as playhead-0tss), so its
        // presence proves the finder works.
        XCTAssertTrue(
            dense.contains("error.localizedDescription"),
            "vacuity: this reader cannot see the spelling it forbids, so its absence proves nothing"
        )

        // THE RULE: nothing reaching `lastErrorCode:` may be a description.
        let written = FMDaemonRefusalSourceCanaryTests.firstArguments(after: "lastErrorCode:", in: dense)
        XCTAssertGreaterThanOrEqual(written.count, 10, "vacuity: the durable writes were not found")
        let offenders = written.filter {
            $0.contains("localizedDescription") || $0.contains("String(describing:")
        }
        XCTAssertTrue(
            offenders.isEmpty,
            """
            An `analysis_jobs.lastErrorCode` write is persisting a Swift value's DESCRIPTION again. \
            `localizedDescription` is LOCALIZED — the column cannot be grouped across devices — and \
            for the error types that reach these arms it is Foundation's bridged apology plus the \
            enum's declaration index. Offending arguments: \(offenders)
            """
        )
    }

    func testTheTerminalArmStillCarriesTheAttemptCapPrefixInFront() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try schedulerSource())
        // `isAttemptCapTerminal(_:)` matches `hasPrefix(maxAttemptsReachedPrefix)`
        // and drives the cap-out-retry rescue. If the token displaced the prefix
        // instead of following it, every capped episode would stop being
        // rescuable and no value test in this file could tell.
        XCTAssertTrue(
            dense.contains("lastErrorCode:\"\\(Self.maxAttemptsReachedPrefix)\\(throwRecord)\""),
            """
            The outer-catch TERMINAL arm no longer writes `maxAttemptsReached:` in front of the \
            token. `AnalysisWorkScheduler.isAttemptCapTerminal(_:)` matches on that prefix and the \
            cap-out-retry rescue reads it, so a capped row that loses it is abandoned permanently.
            """
        )
    }

    func testTheCoordinatorNoLongerPersistsADescription() throws {
        let lines = try coordinatorSource()
        XCTAssertGreaterThan(lines.count, 2_000, "source read found only \(lines.count) code lines")
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)

        // VACUITY.
        XCTAssertTrue(
            dense.contains("DurableThrowRecord.sessionFailureReason("),
            "AnalysisCoordinator no longer builds the durable throw record; move this canary."
        )
        XCTAssertTrue(dense.contains("to:.failed,failureReason:"), "vacuity: the failure write is gone")
        XCTAssertTrue(
            dense.contains("String(describing:error)"),
            "vacuity: this reader cannot see the spelling it forbids (it survives in log lines)"
        )

        // THE RULE, PART ONE: the value handed to the catch-all's write must be
        // BOUND FROM THE RECORD. An argument-only rule is defeated by one
        // intermediate local — `let throwRecord = String(describing: error)`
        // leaves every `failureReason:` argument spelling a wholesome
        // `throwRecord` while the column goes back to prose. Found by writing
        // that mutant (DT05) before running it.
        XCTAssertTrue(
            dense.contains("letthrowRecord=DurableThrowRecord.sessionFailureReason("),
            """
            `runPipeline`'s catch-all no longer binds its durable reason from `DurableThrowRecord`. \
            A local rebound to a description satisfies every rule stated over the ARGUMENT and \
            puts the enum's prose straight back in the column.
            """
        )

        // THE RULE, PART TWO: and no spelling reaching `failureReason:` may be a
        // description either.
        let written = FMDaemonRefusalSourceCanaryTests.firstArguments(after: "failureReason:", in: dense)
        XCTAssertGreaterThanOrEqual(written.count, 5, "vacuity: the durable writes were not found")
        let offenders = written.filter {
            $0.contains("String(describing:") || $0.contains("localizedDescription")
        }
        XCTAssertTrue(
            offenders.isEmpty,
            """
            An `analysis_sessions.failureReason` write is persisting a Swift value's DESCRIPTION \
            again. `AnalysisCoordinatorError` is `CustomStringConvertible`, so that is the enum's \
            PROSE — a sentence carrying the case's payload and naming no case. Offenders: \(offenders)
            """
        )
    }

    func testTheSessionTokenIsGivenTheStateThePipelineWasActuallyDriving() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try coordinatorSource())

        // THE MUTATION THIS TEST EXISTS FOR. A literal here builds a perfectly
        // well-formed token naming a phase the session was not in, every value
        // test stays green, and the wire-in stays green too when the literal
        // happens to match the state the seam drives. Only a rail reading the
        // ARGUMENT can see it.
        let production = try arguments(
            ofCall: "DurableThrowRecord.sessionFailureReason(",
            in: dense
        )
        XCTAssertEqual(
            production["resumeState"],
            "resumeState",
            """
            `runPipeline`'s catch-all is not passing its own `resumeState` parameter to the durable \
            reason. A literal produces `sessionPipelineThrew-<something>(…)` for a session that \
            failed somewhere else — a well-formed value and a wrong argument, which is the one \
            defect shape no value test in this file can see. Got: \(production["resumeState"] ?? "<absent>")
            """
        )
        XCTAssertEqual(production["for"], "error", "the reason must describe the error that was caught")
    }

    func testTheDebugSeamRecordsTheStateItStandsInFor() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try coordinatorSource())

        // Deliberately a SEPARATE method from the production-argument rule
        // above, and not a second assertion inside it. The two failures are
        // different defects — production ignoring its own parameter, versus the
        // seam drifting from the path it stands in for — and a single method
        // covering both means one mutant can be credited for reddening the
        // other's claim. `playhead-sckv`'s SF03/SF10 pairing cost a round to
        // exactly this.
        let calls = FMDaemonRefusalSourceCanaryTests.firstArguments(
            after: "DurableThrowRecord.sessionFailureReason(",
            in: dense
        )
        XCTAssertEqual(calls.count, 2, "expected exactly two session-failure sites (production + seam)")
        XCTAssertTrue(
            dense.contains("resumeState:.backfill"),
            """
            The DEBUG resume seam no longer records `.backfill`. Its whole purpose is to mirror \
            `runPipeline(resumeState: .backfill)`; a seam that persists a different phase from the \
            path it stands in for cannot witness that path, and every wire-in built on it is \
            asserting about a road production never takes.
            """
        )
    }

    func testTheRecoveryTaskNoLongerPersistsADescription() throws {
        let lines = try backgroundSource()
        XCTAssertGreaterThan(lines.count, 1_000, "source read found only \(lines.count) code lines")
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)

        // VACUITY.
        XCTAssertTrue(
            dense.contains("DurableThrowRecord.backgroundTaskLastErrorCode(for:error)"),
            "BackgroundProcessingService no longer builds the durable throw record; move this canary."
        )
        XCTAssertTrue(
            dense.contains("lastErrorCode:\"reconciler_unavailable\""),
            "vacuity: the sibling named-token arm is gone, so this file is not the one under test"
        )

        // THE RULE.
        let written = FMDaemonRefusalSourceCanaryTests.firstArguments(after: "lastErrorCode:", in: dense)
        XCTAssertGreaterThanOrEqual(written.count, 2, "vacuity: the durable writes were not found")
        let offenders = written.filter {
            $0.contains("String(describing:") || $0.contains("localizedDescription")
        }
        XCTAssertTrue(
            offenders.isEmpty,
            """
            A `background_task_runs.lastErrorCode` write is persisting a Swift value's DESCRIPTION \
            again. Offenders: \(offenders)
            """
        )
    }

    func testEverySiteBindsTheRecordOnceAboveItsWrite() throws {
        // playhead-sckv's discipline: the arithmetic, the token and the log all
        // consume ONE local, so no two of them can disagree about the throw in
        // front of them. Two calls in one arm is two measurements of one
        // quantity — harmless while the function is pure, and exactly how the
        // scheduler came to compute `caseName` below the write that needed it.
        let scheduler = FMDaemonRefusalSourceCanaryTests.denseCode(try schedulerSource())
        XCTAssertEqual(
            FMDaemonRefusalSourceCanaryTests
                .firstArguments(after: "DurableThrowRecord.jobLastErrorCode(", in: scheduler).count,
            1,
            "the scheduler's two arms must share ONE binding of the record"
        )
        XCTAssertTrue(
            scheduler.contains("letthrowRecord=DurableThrowRecord.jobLastErrorCode(for:error)"),
            "the record must be bound to a local above both arms"
        )
        XCTAssertTrue(
            scheduler.contains("token=\\(throwRecord,privacy:.public)"),
            "the log line must consume the SAME local the column was given"
        )

        let background = FMDaemonRefusalSourceCanaryTests.denseCode(try backgroundSource())
        XCTAssertEqual(
            FMDaemonRefusalSourceCanaryTests
                .firstArguments(after: "DurableThrowRecord.backgroundTaskLastErrorCode(", in: background).count,
            1,
            "the recovery arm must bind the record exactly once"
        )
        XCTAssertTrue(
            background.contains("token=\\(throwRecord,privacy:.public)"),
            "the recovery log line must consume the SAME local the column was given"
        )
    }
}
