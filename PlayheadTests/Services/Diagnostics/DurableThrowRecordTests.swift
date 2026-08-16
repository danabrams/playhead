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
//
// ===== playhead-3c4k: THE FIFTH SITE, AND THE ONE WITH FIELD ROWS =====
//
//   AnalysisWorkScheduler   lastErrorCode: "\(maxAttemptsReachedPrefix)assetResolution: \(error)"
//   AnalysisWorkScheduler   lastErrorCode: "assetResolution: \(error)"
//
// String INTERPOLATION of an `Error` IS `String(describing:)` — measured in
// `interpolationIsTheSameDefectInAThirdSpelling`, not asserted — so this is the
// same defect in a THIRD spelling, and the third spelling is exactly the one
// 3lc3's own canary could not see: `testTheSchedulerArmsNoLongerPersistADescription`
// filtered every `lastErrorCode:` argument for `localizedDescription` and
// `String(describing:` and read straight past this line in the same file. That
// blindness is now itself a test — `testTheOldTwoSpellingRuleWouldHaveMissedTheShippedLine`.
//
// **THIS SITE HAS A POPULATION, WHICH IS WHY IT IS A P1 AND THE OTHER FIVE WERE
// NOT.** Per row identity over db-pull8…12: 38 distinct jobIds, 9 that ever
// carried a cause, and 5 of those 9 carried this arm's prose. Zero carried the
// TERMINAL arm's spelling, so the arm with the load-bearing
// `maxAttemptsReached:` prefix is the one with no witness in the field either —
// `theAssetResolutionTerminalArmIsStillRescuable` and
// `testTheAssetResolutionTerminalArmStillCarriesTheAttemptCapPrefixInFront` are
// the only things standing under it.
//
// AND IT HAS NO RUNTIME WITNESS EITHER, for a THIRD reason that is worth
// stating because it is not the two above. `resolveAnalysisAssetId`'s throws all
// come from a concrete `AnalysisStore` with no protocol seam (the same
// conclusion as the two sites above) — but on top of that, the one production
// failure that produced ALL FIVE field rows can no longer reach this catch at
// all: `playhead-1216`'s `insertAssetAdoptingIdentity` turned the
// `UNIQUE constraint failed: analysis_assets.episodeId,
// analysis_assets.assetFingerprint` collision into an ADOPTION, and
// `DownloadTimeAssetRegistrationTests` asserts exactly that the arm does not
// fire. So the site is carried by the SOURCE CANARY, per site rather than per
// file — this scheduler now has TWO arms that build a `DurableThrowRecord` and
// write `lastErrorCode:`, which is precisely the shape that let mutant DT05
// survive a file-wide rule one bead ago.

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

    // MARK: playhead-3c4k — the fifth site, the only one with field rows

    /// The exact prose the asset-resolution arm wrote, as it stands in five
    /// device pulls. Quoted from `db-pull8`…`db-pull12`, not invented.
    static let retiredAssetResolutionProse =
        "assetResolution: Insert failed: UNIQUE constraint failed: "
        + "analysis_assets.episodeId, analysis_assets.assetFingerprint"

    @Test("THE REAL DEFECT: interpolating an Error IS String(describing:), which is why 3lc3's rule missed it")
    func interpolationIsTheSameDefectInAThirdSpelling() throws {
        // The shipped line was `lastErrorCode: "assetResolution: \(error)"`.
        // Neither `localizedDescription` nor `String(describing:` appears in
        // it, so 3lc3's source canary — which filtered every `lastErrorCode:`
        // argument for exactly those two spellings — was blind to a third
        // spelling of its own defect, in the same file, twelve hundred lines
        // below the two arms it did fix.
        //
        // The equivalence is the point, so it is MEASURED rather than asserted
        // in prose: interpolation and `String(describing:)` produce the same
        // bytes for every error type that reaches this arm.
        for error in Self.coordinatorErrors {
            #expect("\(error)" == String(describing: error),
                    "interpolation and String(describing:) disagree for \(error)")
        }
        let storeError = AnalysisStoreError.insertFailed(
            "UNIQUE constraint failed: analysis_assets.episodeId, analysis_assets.assetFingerprint"
        )
        #expect("\(storeError)" == String(describing: storeError))

        // And the shipped spelling reproduces the field rows exactly. This is
        // the string five of the nine `analysis_jobs` rows that ever carried a
        // cause actually held, rebuilt from the production error type.
        #expect("assetResolution: \(storeError)" == Self.retiredAssetResolutionProse,
                "the reconstruction of the field value drifted: assetResolution: \(storeError)")
    }

    @Test("the asset-resolution token replaces the prose and keeps the greppable STEM")
    func theAssetResolutionTokenKeepsTheStem() throws {
        let error = AnalysisStoreError.insertFailed(
            "UNIQUE constraint failed: analysis_assets.episodeId, analysis_assets.assetFingerprint"
        )
        let token = DurableThrowRecord.assetResolutionLastErrorCode(for: error)

        // It is a token, not prose: the SQLite sentence that produced every
        // field row does not reach the column.
        #expect(token != Self.retiredAssetResolutionProse)
        #expect(!token.contains("UNIQUE constraint failed"))
        #expect(!token.contains("analysis_assets.episodeId"))
        #expect(!token.contains(" "))

        // THE STEM IS LOAD-BEARING. `DownloadTimeAssetRegistrationTests` guards
        // the UNIQUE-constraint regression with
        // `lastErrorCode?.contains("assetResolution") != true`. A token spelled
        // without the stem leaves that guard green while this arm fires.
        #expect(token.contains("assetResolution"))
        #expect(Self.retiredAssetResolutionProse.contains("assetResolution"))

        // And the separation from the prose is EXACT rather than approximate:
        // fifteen shared characters, then `T` against `:`.
        let stem = "assetResolution"
        #expect(token.hasPrefix(stem) && Self.retiredAssetResolutionProse.hasPrefix(stem))
        #expect(String(token.dropFirst(stem.count).prefix(1)) == "T")
        #expect(String(Self.retiredAssetResolutionProse.dropFirst(stem.count).prefix(1)) == ":")

        // Which is what makes all three device-pull queries mean what the
        // header says they mean.
        let population = [token, Self.retiredAssetResolutionProse, "coverageInsufficient:noProgress"]
        #expect(population.filter { $0.hasPrefix("assetResolutionThrew(") } == [token])
        #expect(population.filter { $0.hasPrefix("assetResolution: ") }
            == [Self.retiredAssetResolutionProse])
        #expect(population.filter { $0.hasPrefix(stem) }.count == 2)
    }

    @Test("the asset-resolution condition is NOT the job-run condition")
    func theTwoJobConditionsAreDistinguishable() throws {
        // `jobThrew` stands over `runTask.value`; `assetResolutionThrew` stands
        // over `resolveAnalysisAssetId`, one stage earlier and before any
        // runner exists. Folding them into one prefix would delete exactly the
        // discrimination the retired prose already gave a device pull.
        let error = AnalysisStoreError.notFound
        let run = DurableThrowRecord.jobLastErrorCode(for: error)
        let resolution = DurableThrowRecord.assetResolutionLastErrorCode(for: error)
        #expect(run != resolution)
        #expect(!run.hasPrefix(DurableThrowRecord.assetResolutionThrewPrefix))
        #expect(!resolution.hasPrefix(DurableThrowRecord.jobThrewPrefix))

        // Both still carry the SAME identity grammar, so a pull can group the
        // two populations separately and still compare them field for field.
        let fields = DurableThrowRecord.identityFields(of: error)
        #expect(run.contains(fields))
        #expect(resolution.contains(fields))
    }

    @Test("the asset-resolution TERMINAL arm still satisfies isAttemptCapTerminal")
    func theAssetResolutionTerminalArmIsStillRescuable() throws {
        // NO FIELD ROW HAS EVER TAKEN THIS ARM. Counted per row identity over
        // db-pull8…12: five rows carried the retry arm's prose and ZERO carried
        // `maxAttemptsReached:assetResolution: …`, so the arm with the
        // load-bearing prefix is precisely the one with no witness — which is
        // what a rail is for.
        let token = DurableThrowRecord.assetResolutionLastErrorCode(for: AnalysisStoreError.notFound)
        let terminal = Self.job(
            state: "superseded",
            lastErrorCode: AnalysisWorkScheduler.maxAttemptsReachedPrefix + token
        )
        #expect(AnalysisWorkScheduler.isAttemptCapTerminal(terminal))
        #expect(AnalysisWorkScheduler.isRescuableTerminal(terminal))

        // The retry arm writes the bare token at state `failed` — the row all
        // five field rows actually were. Neither predicate matches it.
        let retried = Self.job(state: "failed", lastErrorCode: token)
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(retried))
        #expect(!AnalysisWorkScheduler.isRescuableTerminal(retried))

        // THE OTHER READER, ASKED SO THE ANSWER IS ABOUT THE CODE. Asserting
        // `!isNoProgressTerminal(retried)` alone would pass for the wrong
        // reason: that predicate is `state == "complete" && lastErrorCode ==
        // coverageInsufficient:noProgress`, and `retried` is `failed`, so the
        // STATE clause carries it and the token is never consulted — a value
        // that names one thing read as though it named another, inside the
        // check written to rule that out. Hold the state at the one value the
        // predicate accepts and let the code be the only variable.
        for spelling in [token,
                         AnalysisWorkScheduler.maxAttemptsReachedPrefix + token,
                         Self.retiredAssetResolutionProse] {
            #expect(!AnalysisWorkScheduler.isNoProgressTerminal(
                Self.job(state: "complete", lastErrorCode: spelling)
            ), "\(spelling) was read as the no-progress terminal")
        }
        // Anti-vacuity: at that same state the real no-progress code DOES
        // match, so the loop above is measuring the code and not the state.
        // Through the shared constant, not a literal — `AnalysisWorkScheduler`
        // documents that reader and writer share it precisely so a copy cannot
        // drift, and an anti-vacuity control spelled by hand is a copy.
        #expect(AnalysisWorkScheduler.isNoProgressTerminal(
            Self.job(
                state: "complete",
                lastErrorCode: AnalysisWorkScheduler.noProgressTerminalErrorCode
            )
        ))

        // And the RETIRED prose was rescuable on the same terms, so nothing
        // about the cap-out rescue changed shape when the suffix did.
        #expect(AnalysisWorkScheduler.isAttemptCapTerminal(Self.job(
            state: "superseded",
            lastErrorCode: AnalysisWorkScheduler.maxAttemptsReachedPrefix
                + Self.retiredAssetResolutionProse
        )))
    }

    @Test("THE DEFECT: no case's payload reaches any of the four durable values")
    func noPayloadReachesTheDurableValue() throws {
        for error in Self.coordinatorErrors {
            let tokens = [
                DurableThrowRecord.jobLastErrorCode(for: error),
                DurableThrowRecord.assetResolutionLastErrorCode(for: error),
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
        #expect(DurableThrowRecord.assetResolutionLastErrorCode(for: error)
            .hasPrefix(DurableThrowRecord.assetResolutionThrewPrefix + "("))
        #expect(DurableThrowRecord.sessionFailureReason(for: error, resumeState: .queued)
            .hasPrefix(DurableThrowRecord.sessionPipelineThrewPrefix + "-"))
        #expect(DurableThrowRecord.backgroundTaskLastErrorCode(for: error)
            .hasPrefix(DurableThrowRecord.recoveryThrewPrefix + "("))
    }

    /// Values a writer can produce TODAY. `assetResolution: …` is deliberately
    /// NOT here — playhead-3c4k retired it — and is handled below as a retired
    /// spelling, because the two claims are different: a live incumbent must be
    /// unconfusable with a token, while a retired one only has to be
    /// DISTINGUISHABLE from it in a pull that still holds both.
    private static let liveIncumbents = [
        "coverageInsufficient:noProgress",
        "backgroundWindowExpired",
        "transcription:cancelled",
        "orphan_at_launch",
        "reconciler_unavailable",
        "staleFingerprint:cachedAudioMismatch",
        AnalysisWorkScheduler.maxAttemptsReachedPrefix + "cancelMidRun",
        AnalysisWorkScheduler.maxAttemptsReachedPrefix + "coverageInsufficient",
        AnalysisCoordinator.coverageGuardFailureReasonPrefix + "689.8/3600.0s (ratio 0.192 < 0.950)",
        "Unknown state in DB: bogus",
        "corrupt session state on resume: bogus",
    ]

    private static func allTokens(for error: Error, resumeState: SessionState) -> [String] {
        [
            DurableThrowRecord.jobLastErrorCode(for: error),
            DurableThrowRecord.assetResolutionLastErrorCode(for: error),
            DurableThrowRecord.sessionFailureReason(for: error, resumeState: resumeState),
            DurableThrowRecord.backgroundTaskLastErrorCode(for: error),
        ]
    }

    @Test("the four prefixes collide with nothing else these columns hold")
    func thePrefixesAreDisjointFromEveryOtherCause() throws {
        let prefixes = [
            DurableThrowRecord.jobThrewPrefix,
            DurableThrowRecord.assetResolutionThrewPrefix,
            DurableThrowRecord.sessionPipelineThrewPrefix,
            DurableThrowRecord.recoveryThrewPrefix,
        ]
        #expect(Set(prefixes).count == prefixes.count, "two conditions share one prefix")
        for incumbent in Self.liveIncumbents {
            for prefix in prefixes {
                #expect(!incumbent.hasPrefix(prefix),
                        "'\(incumbent)' would be counted as \(prefix)")
            }
        }
        // And the reverse direction: no token answers to an incumbent's prefix.
        let error = AnalysisStoreError.notFound
        for token in Self.allTokens(for: error, resumeState: .spooling) {
            for incumbent in Self.liveIncumbents {
                #expect(!token.hasPrefix(String(incumbent.prefix(12))),
                        "\(token) collides with \(incumbent)")
            }
        }
    }

    @Test("the RETIRED asset-resolution prose is still separable from every token")
    func theRetiredProseIsSeparable() throws {
        // A device pull can hold both — five rows carried the prose, and the
        // column is last-writer-wins, so a future row can carry the token while
        // an old file still carries the sentence. The claim here is weaker than
        // for a live incumbent (they SHARE a stem, on purpose) and it is stated
        // exactly: the discriminating prefixes are disjoint.
        let error = AnalysisStoreError.notFound
        let prose = Self.retiredAssetResolutionProse
        for token in Self.allTokens(for: error, resumeState: .spooling) {
            #expect(token != prose)
            #expect(!token.hasPrefix("assetResolution: "),
                    "\(token) would be selected by a query for the retired prose")
        }
        #expect(!prose.hasPrefix(DurableThrowRecord.assetResolutionThrewPrefix + "("),
                "the retired prose would be counted as a token")
        for prefix in [DurableThrowRecord.jobThrewPrefix,
                       DurableThrowRecord.sessionPipelineThrewPrefix,
                       DurableThrowRecord.recoveryThrewPrefix] {
            #expect(!prose.hasPrefix(prefix))
        }
    }

    @Test("a prefix query returns every throw of one kind and nothing else")
    func aPrefixQueryCountsThePopulation() throws {
        let error = AnalysisStoreError.notFound
        let rows = [
            DurableThrowRecord.jobLastErrorCode(for: error),
            AnalysisWorkScheduler.maxAttemptsReachedPrefix
                + DurableThrowRecord.jobLastErrorCode(for: error),
            DurableThrowRecord.assetResolutionLastErrorCode(for: error),
            AnalysisWorkScheduler.maxAttemptsReachedPrefix
                + DurableThrowRecord.assetResolutionLastErrorCode(for: error),
            "coverageInsufficient:noProgress",
            "backgroundWindowExpired",
        ]
        // The retry arm alone.
        #expect(rows.filter { $0.hasPrefix(DurableThrowRecord.jobThrewPrefix) }.count == 1)
        // Both arms, which is what an operator counting "unclassified job
        // throws" wants — and is exactly why the token is a SUFFIX of the
        // attempt-cap prefix rather than a competing family.
        #expect(rows.filter { $0.contains(DurableThrowRecord.jobThrewPrefix) }.count == 2)

        // playhead-3c4k: the SAME two readings for the asset-resolution
        // condition, and each counts only its own condition. `jobThrew` is not
        // a substring of `assetResolutionThrew`, in either direction, so
        // neither query can absorb the other's population.
        #expect(rows.filter { $0.hasPrefix(DurableThrowRecord.assetResolutionThrewPrefix) }.count == 1)
        #expect(rows.filter { $0.contains(DurableThrowRecord.assetResolutionThrewPrefix) }.count == 2)
        #expect(!DurableThrowRecord.assetResolutionThrewPrefix
            .contains(DurableThrowRecord.jobThrewPrefix))
        #expect(!DurableThrowRecord.jobThrewPrefix
            .contains(DurableThrowRecord.assetResolutionThrewPrefix))
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
        let tokens = Self.allTokens(for: hostile, resumeState: .hotPathReady)
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

    @Test("the four factories share ONE identity grammar")
    func theIdentityGrammarIsShared() throws {
        // Not four copies of `domain=…,code=…,under=…`: one, delegated to
        // `UnclassifiedModelFailure`, so a future bead that tightens the domain
        // budget cannot move three of the four columns and leave the fourth.
        let error = AnalysisStoreError.openFailed(code: 14, message: "unable to open database file")
        let fields = DurableThrowRecord.identityFields(of: error)
        for token in Self.allTokens(for: error, resumeState: .queued) {
            #expect(token.contains(fields), "\(token) does not carry the shared identity grammar")
        }
    }

    @Test("under= and the deepest-link walk hold for the asset-resolution token too")
    func theAssetResolutionTokenCarriesTheSameUnderlyingWalk() throws {
        // The three older tokens each have their own rail for this; a fourth
        // factory that forgot to delegate would still LOOK right in every test
        // above, because a hand-rolled `domain=…,code=…` reads identically
        // until the error carries a chain.
        let bare = NSError(domain: "Bare.Domain", code: 3, userInfo: nil)
        #expect(DurableThrowRecord.assetResolutionLastErrorCode(for: bare)
            .contains("under=\(UnclassifiedModelFailure.noUnderlyingToken)"))

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
        let token = DurableThrowRecord.assetResolutionLastErrorCode(for: outer)
        #expect(token.contains("under=Deepest.Domain/42"))
        #expect(!token.contains("Middle.Domain"))

        // And the sanitizer really fires on this factory, not just on the
        // first one written.
        let hostile = NSError(domain: "A B,C=D(E)", code: 1, userInfo: nil)
        #expect(DurableThrowRecord.assetResolutionLastErrorCode(for: hostile)
            .contains("domain=A_BCDE"))
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

// MARK: - playhead-luie: the SIXTH site, and the first with a WITNESS ON DISK

/// `rediff_day_zero_attempts.lastDetail` — the fourth durable column, reached
/// through a struct FIELD rather than a labelled argument, which is why two
/// schema-derived sweeps could not find it.
///
/// **This is the only site in the series whose retired prose survives on disk**,
/// in `3gzp/gt.sqlite`, two rows from 2026-08-02 and 2026-08-05. The tests below
/// reconstruct that error's SHAPE rather than quoting the strings, because the
/// point is not what those two bytes were: it is that two constructions of one
/// failure produce two different strings, and that no test could have been
/// written against either of them.
@Suite("DurableThrowRecord — the day-0 attempt detail is a named token (playhead-luie)")
struct DurableThrowRecordDayZeroTests {

    /// The shape the two field rows carried: `NSURLErrorDomain` / `-1001`, a
    /// localized sentence, a `userInfo` with the enclosure URL in it, and a
    /// `kCFErrorDomainCFNetwork` link underneath. Built fresh on each call so
    /// two calls are two OBJECTS — which is the whole of the first test.
    private static func timedOutFetchError() -> NSError {
        let inner = NSError(
            domain: "kCFErrorDomainCFNetwork",
            code: -1001,
            userInfo: ["_kCFStreamErrorCodeKey": -2102]
        )
        return NSError(
            domain: NSURLErrorDomain,
            code: -1001,
            userInfo: [
                NSLocalizedDescriptionKey: "The request timed out.",
                "_kCFStreamErrorCodeKey": -2102,
                NSUnderlyingErrorKey: inner,
                "NSErrorFailingURLStringKey":
                    "https://traffic.megaphone.fm/PRIVATE-EPISODE-9137.mp3",
            ]
        )
    }

    @Test("THE REAL DEFECT: two constructions of ONE failure describe as two different strings")
    func theRealDefectDescriptionsDoNotGroup() {
        let first = String(describing: Self.timedOutFetchError())
        let second = String(describing: Self.timedOutFetchError())

        // The field evidence, reproduced rather than quoted. `3gzp/gt.sqlite`
        // holds two `lastExit=fetch_failed` rows whose 200 characters differ in
        // exactly one place: `NSUnderlyingError=0x1502a0d20` against
        // `0x11cc169d0`. A per-process HEAP ADDRESS was serialised into a
        // durable column, so `GROUP BY lastDetail` over that table can never
        // return a count above one.
        #expect(first != second, """
            The premise of this bead is false on this OS: String(describing:) is stable across two \
            constructions of the same NSError. Re-measure before trusting the rest of this suite.
            """)

        // …and it is not only the pointer. `userInfo` is a dictionary, so its
        // KEY ORDER varies too — a second, independent reason the same failure
        // answers to more than one string.
        #expect(first.contains("NSUnderlyingError=0x"), "the heap pointer is what the field rows carried")

        // The token is the same string both times, which is the property a
        // device pull actually needs.
        #expect(
            DurableThrowRecord.dayZeroAttemptDetail(for: Self.timedOutFetchError())
                == DurableThrowRecord.dayZeroAttemptDetail(for: Self.timedOutFetchError())
        )
    }

    @Test("THE REAL DEFECT: the description carried the enclosure URL, and the token cannot")
    func theRealDefectTheURLIsGone() {
        let error = Self.timedOutFetchError()
        let described = String(describing: error)
        // Why three exporters refuse to project this column — measured on the
        // real shape, not assumed.
        #expect(described.contains("traffic.megaphone.fm"))
        #expect(described.contains("PRIVATE-EPISODE-9137"))

        let token = DurableThrowRecord.dayZeroAttemptDetail(for: error)
        #expect(!token.contains("traffic.megaphone.fm"))
        #expect(!token.contains("PRIVATE-EPISODE-9137"))
        #expect(!token.contains("megaphone"))
        // The localized sentence goes with it. A device in another language
        // wrote different bytes for this failure.
        #expect(described.contains("The request timed out."))
        #expect(!token.contains("timed out"))
    }

    @Test("the token names the condition and carries the identity, including the deepest link")
    func theTokenNamesTheConditionAndTheIdentity() {
        let token = DurableThrowRecord.dayZeroAttemptDetail(for: Self.timedOutFetchError())
        #expect(token.hasPrefix(DurableThrowRecord.dayZeroThrewPrefix + "("))
        #expect(token.contains("domain=NSURLErrorDomain"))
        #expect(token.contains("code=-1001"))
        // `identityFields` walks to the DEEPEST link, not the first — the
        // wrapper is the framework saying it did not classify either.
        #expect(token.contains("under=kCFErrorDomainCFNetwork/-1001"))
        #expect(token.hasSuffix(")"))
    }

    @Test("under= is a positive claim on this token too, never an absence")
    func underIsAPositiveClaim() {
        enum Bare: Error { case fetchGaveUp }
        let token = DurableThrowRecord.dayZeroAttemptDetail(for: Bare.fetchGaveUp)
        #expect(token.contains("under=\(UnclassifiedModelFailure.noUnderlyingToken)"))
        #expect(!token.contains("under=,"))
        #expect(!token.contains("under=)"))
    }

    @Test("the day-0 prefix collides with none of the other four, in either direction")
    func thePrefixCollidesWithNothing() {
        let others = [
            DurableThrowRecord.jobThrewPrefix,
            DurableThrowRecord.assetResolutionThrewPrefix,
            DurableThrowRecord.sessionPipelineThrewPrefix,
            DurableThrowRecord.recoveryThrewPrefix,
        ]
        let mine = DurableThrowRecord.dayZeroThrewPrefix
        for other in others {
            #expect(!mine.hasPrefix(other), "\(mine) answers to a \(other) query")
            #expect(!other.hasPrefix(mine), "\(other) answers to a \(mine) query")
        }
        // And the column itself: `lastDetail` has never held any other value in
        // any of the nineteen preserved device databases, so there is no retired
        // spelling to stay separable from — unlike `assetResolutionThrew`, whose
        // STEM is load-bearing. Nothing greps this column for a substring.
        #expect(mine == "dayZeroThrew")
    }

    @Test("the token's grammar is closed, even against a hostile domain")
    func theGrammarIsClosed() {
        let hostile = NSError(
            domain: "Some Domain(with),spaces=and punctuation",
            code: 7,
            userInfo: [:]
        )
        let token = DurableThrowRecord.dayZeroAttemptDetail(for: hostile)
        #expect(!token.contains(" "))
        #expect(token.filter { $0 == "(" }.count == 1)
        #expect(token.filter { $0 == ")" }.count == 1)
        #expect(token.hasSuffix(")"))
        // The sanitizer really fired, rather than the fixture being harmless.
        #expect(!token.contains("=and"))
    }

    // MARK: The hop into the column

    @Test("the token reaches lastDetail through advance() UNCUT, and the exit rides beside it")
    func theTokenReachesTheColumnUncut() {
        let token = DurableThrowRecord.dayZeroAttemptDetail(for: Self.timedOutFetchError())
        let folded = DayZeroRediffAttemptPolicy.advance(
            record: nil,
            assetId: "A",
            outcome: .blocked(.fetchFailed, detail: token),
            fullFetchBytes: 54_000_000,
            at: 1_000
        )
        #expect(folded.lastDetail == token, "the 200-char cap must not bite a token")
        // THE ARM IS SPELLED BY `lastExit`, WHICH IS WHY THE TOKEN DOES NOT
        // RESTATE IT. Both come from the same outcome in the same call, so they
        // cannot drift — that is the argument for one prefix over two arms, and
        // this is where it is checked rather than asserted in a comment.
        #expect(folded.lastExit == .fetchFailed)

        let persist = DayZeroRediffAttemptPolicy.advance(
            record: nil,
            assetId: "A",
            outcome: RediffDayZeroMintOutcome(exit: .persistFailed, detail: token),
            fullFetchBytes: 54_000_000,
            at: 1_000
        )
        #expect(persist.lastDetail == token)
        #expect(persist.lastExit == .persistFailed)
        #expect(folded.lastDetail == persist.lastDetail,
                "the two arms share one token — the row's exit is what separates them")
    }

    @Test("a later attempt CLEARS the detail, so lastExit and lastDetail can never drift")
    func aLaterAttemptClearsTheDetail() {
        // THE INVARIANT THE ONE-PREFIX DECISION RESTS ON, and the only test in
        // this file that can see it fail. The token deliberately does not name
        // which arm produced it, because `lastExit` does — and that argument is
        // only sound while the two are written from the SAME outcome. If
        // `advance` ever carried `lastDetail` forward the way it carries
        // suppression history, a row would read `lastExit=marked` beside a
        // throw's token, and a pull would attribute a fetch failure to a
        // successful mint. Every other test here passes `record: nil`, which is
        // exactly the shape that cannot observe a carry-forward.
        let thrown = DurableThrowRecord.dayZeroAttemptDetail(
            for: NSError(domain: "Earlier", code: 3)
        )
        let failedAttempt = DayZeroRediffAttemptPolicy.advance(
            record: nil, assetId: "A",
            outcome: .blocked(.fetchFailed, detail: thrown),
            fullFetchBytes: 54_000_000, at: 1_000
        )
        #expect(failedAttempt.lastDetail == thrown, "vacuity: the first attempt recorded nothing")

        let laterSuccess = DayZeroRediffAttemptPolicy.advance(
            record: failedAttempt, assetId: "A",
            outcome: RediffDayZeroMintOutcome(markCount: 2, exit: .marked),
            fullFetchBytes: 54_000_000, at: 2_000
        )
        #expect(laterSuccess.lastExit == .marked)
        #expect(laterSuccess.lastDetail == nil, """
            `lastDetail` survived an attempt that caught no throw. The row now reads \
            lastExit=\(laterSuccess.lastExit.rawValue) beside a token from an earlier failure, and \
            the whole reason this token omits an arm discriminator is that `lastExit` carries it \
            and cannot disagree.
            """)
        // The other half of the same claim: history that IS meant to accumulate
        // still does, so this is not "advance forgets everything".
        #expect(laterSuccess.totalFullFetchBytes == 108_000_000)
        #expect(laterSuccess.attemptCount == 2)
    }

    @Test("the token fits under detailCharCap at the worst REALISTIC identity")
    func theTokenFitsUnderTheDetailCap() {
        // Both domains at the sanitizer's ceiling and both codes eleven digits —
        // the widest identity any real `NSError` can present. `maxDomainLength`
        // is read rather than restated so this measurement follows the bound.
        let wide = String(repeating: "D", count: UnclassifiedModelFailure.maxDomainLength * 3)
        let inner = NSError(domain: wide, code: -99_999_999_999, userInfo: [:])
        let outer = NSError(
            domain: wide,
            code: -99_999_999_999,
            userInfo: [NSUnderlyingErrorKey: inner]
        )
        let token = DurableThrowRecord.dayZeroAttemptDetail(for: outer)

        // MEASURED, and the first prediction written here was 175. It is 187 —
        // an eleven-digit code is twelve characters once it carries a sign, and
        // the token carries two of them. The number is asserted exactly rather
        // than with `<`, so a grammar change that eats the remaining thirteen
        // characters of headroom is a red test and not a silent truncation.
        #expect(token.count == 187, "the worst realistic token measures \(token.count), not 187")
        #expect(token.count < DayZeroRediffAttemptPolicy.detailCharCap)
        // ANTI-VACUITY: the cap is a real bound that a long enough string DOES
        // hit, so the expectation above is not passing because nothing is
        // measured. This is the direction that the retired prose took — both
        // field rows measured exactly 200.
        let folded = DayZeroRediffAttemptPolicy.advance(
            record: nil, assetId: "A",
            outcome: .blocked(.fetchFailed, detail: String(repeating: "x", count: 5_000)),
            fullFetchBytes: 0, at: 1
        )
        #expect(folded.lastDetail?.count == DayZeroRediffAttemptPolicy.detailCharCap)
    }

    @Test("detail stays nil on every exit that caught no throw")
    func detailIsNilWhereNothingThrew() {
        // The token's meaning depends on this: a non-nil `lastDetail` is a
        // positive claim that a throw was caught, so an exit that merely
        // declined must not put commentary there.
        for exit in RediffDayZeroExit.allCases {
            #expect(RediffDayZeroMintOutcome.blocked(exit).detail == nil,
                    "\(exit.rawValue) invents a detail")
        }
    }
}

// MARK: - playhead-q93o: the runner's own stage catches

@Suite("DurableThrowRecord — the runner's stage catches are a named token (playhead-q93o)")
struct DurableThrowRecordRunnerStageTests {

    /// Every `AnalysisAudioError` case, with a payload where one exists — the
    /// error family the `decode` stage actually catches, and the family that
    /// produced this site's two field rows.
    private static let audioErrors: [AnalysisAudioError] = [
        .fileNotFound(URL(fileURLWithPath: "/tmp/PRIVATE-EPISODE-NAME.mp3")),
        .assetUnreadable(URL(fileURLWithPath: "/tmp/PRIVATE-EPISODE-NAME.mp3"), underlying: nil),
        .readerSetupFailed("AVAssetReader could not start"),
        .converterSetupFailed,
        .decodingFailed("Operation Interrupted"),
        .truncatedFile(
            URL(fileURLWithPath: "/tmp/PRIVATE-EPISODE-NAME.mp3"),
            expectedDuration: 3_600,
            decodedDuration: 12
        ),
        .cancelled,
    ]

    // MARK: The defect, measured from the production type

    @Test("THE REAL DEFECT: the field rows' exact spelling, pinned from the running program")
    func theExactFieldSpelling() {
        // `analysis_jobs` on the 2026-04-25 device capture holds
        //   323D2A22  failed      attempts=2  decode: Decoding failed: Operation Interrupted
        //   5C185837  superseded  attempts=6  maxAttemptsReached:decode: Decoding failed: …
        // Reproduced here from the type rather than quoted, so the claim in
        // `DurableThrowRecord.swift`'s header cannot become quietly false.
        let field = AnalysisAudioError.decodingFailed("Operation Interrupted")
        let retired = "decode: \(field)"
        print("[q93o] retired decode payload = \(retired)")
        #expect(retired == "decode: Decoding failed: Operation Interrupted",
                "the field row's spelling is no longer what this arm produces: \(retired)")

        // And it names NO CASE — `AnalysisAudioError` is CustomStringConvertible,
        // so the interpolation took its PROSE. sckv's trap, a third time.
        #expect(!retired.contains("decodingFailed"))

        // The token names the domain and the code as fields a GROUP BY can split
        // on. The code is the enum's TAG, MEASURED rather than predicted: the
        // first draft of this line predicted 4 (declaration index of
        // `decodingFailed` counting from zero) and the tag is 3, because the five
        // payload cases are laid out first in declaration order and the two empty
        // ones after them — `fileNotFound` 0, `assetUnreadable` 1,
        // `readerSetupFailed` 2, `decodingFailed` 3, `truncatedFile` 4, then
        // `converterSetupFailed` 5 and `cancelled` 6.
        let bridged = field as NSError
        print("[q93o] bridged domain=\(bridged.domain) code=\(bridged.code)")
        #expect(bridged.domain == "Playhead.AnalysisAudioError")
        #expect(bridged.code == 3)
        let token = DurableThrowRecord.runnerStageLastErrorCode(for: field, stage: .decode)
        #expect(token == "runnerStageThrew-decode(domain=Playhead.AnalysisAudioError,code=3,under=none)",
                "token is \(token)")
    }

    @Test("THE REAL DEFECT: three sibling cases would have put the EPISODE FILENAME in the column")
    func theRealDefectTheFilenameCases() {
        // The case that fired in the field carried only a phrase. The three that
        // carry a `URL` carry the user's episode FILENAME, and one of them also
        // carries a LOCALIZED sentence underneath it — luie's enclosure-URL
        // disclosure, in a different column, from a different producer, waiting
        // on a decode failure of a different kind.
        let secret = "PRIVATE-EPISODE-NAME"
        let filenameCases: [AnalysisAudioError] = [
            .fileNotFound(URL(fileURLWithPath: "/tmp/\(secret).mp3")),
            .assetUnreadable(URL(fileURLWithPath: "/tmp/\(secret).mp3"), underlying: nil),
            .truncatedFile(
                URL(fileURLWithPath: "/tmp/\(secret).mp3"),
                expectedDuration: 3_600,
                decodedDuration: 12
            ),
        ]
        for error in filenameCases {
            #expect("\(error)".contains(secret),
                    "the premise is wrong — this case does not carry the filename: \(error)")
            let token = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .decode)
            #expect(!token.contains(secret), "the token carries the filename: \(token)")
        }
    }

    @Test("no case's payload reaches the token, for any stage")
    func noPayloadReachesTheToken() {
        for error in Self.audioErrors {
            for stage in DurableThrowRecord.RunnerStage.allCases {
                let token = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: stage)
                for leak in ["PRIVATE-EPISODE-NAME", "Operation Interrupted", "AVAssetReader"] {
                    #expect(!token.contains(leak), "\(stage.rawValue) leaked \(leak): \(token)")
                }
            }
        }
    }

    // MARK: The grammar

    @Test("the stage is a FIELD of one prefix, not five prefixes")
    func oneFamilyFiveStages() {
        let error = AnalysisAudioError.decodingFailed("x")
        for stage in DurableThrowRecord.RunnerStage.allCases {
            let token = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: stage)
            #expect(token.hasPrefix("\(DurableThrowRecord.runnerStageThrewPrefix)-\(stage.rawValue)("),
                    "\(stage.rawValue) is outside the family: \(token)")
            // The family query returns every stage…
            #expect(token.hasPrefix("\(DurableThrowRecord.runnerStageThrewPrefix)-"))
        }
        // …and the per-stage query returns exactly one of them. Stated as a
        // partition rather than as five separate `hasPrefix` checks, because the
        // failure this guards is two stages that answer to one query.
        let tokens = DurableThrowRecord.RunnerStage.allCases.map {
            DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: $0)
        }
        for stage in DurableThrowRecord.RunnerStage.allCases {
            let query = "\(DurableThrowRecord.runnerStageThrewPrefix)-\(stage.rawValue)("
            #expect(tokens.filter { $0.hasPrefix(query) }.count == 1,
                    "`LIKE '\(query)%'` does not select exactly one stage")
        }
    }

    @Test("the five stage raw values ARE the retired stems, which two rails grep")
    func theStemsArePreserved() {
        // `RunnerMaterializerRegressionTests` asserts `msg.contains("backfill")`
        // and `msg.contains("hotPath")` on the outcome payload — the only greps
        // of any of the five stems anywhere in the tree. A stage spelled
        // otherwise leaves those rails GREEN while this arm fires: a check that
        // reads as evidence of an absence it can no longer see.
        #expect(Set(DurableThrowRecord.RunnerStage.allCases.map(\.rawValue))
            == ["decode", "features", "fetchChunks", "hotPath", "backfill"])
        // AND THE RAW VALUE IS THE CASE NAME, which is a second claim and not a
        // restatement. Mutant Q06 gave `hotPath` the raw value `hot_path`: the
        // stem rail above caught it, and so did the per-site source rail below —
        // but that one caught it for the WRONG REASON and said so, because it
        // was keyed on the raw value while the SOURCE spells the case name. A
        // value that names one thing read as though it named another, in the
        // rail written to catch that. The source rail is keyed on the case name
        // now, and this line is what keeps the two coupled.
        for stage in DurableThrowRecord.RunnerStage.allCases {
            #expect(stage.rawValue == String(describing: stage),
                    "\(stage) persists as \(stage.rawValue); the source rail reads the CASE NAME")
        }
        let error = AnalysisAudioError.decodingFailed("x")
        #expect(DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .backfill)
            .contains("backfill"))
        #expect(DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .hotPath)
            .contains("hotPath"))
    }

    @Test("the token separates EXACTLY from the retired prose it replaced")
    func tokenAndProseSeparate() {
        // So a device pull can be written without guessing. The two diverge at
        // the character after the stem: `(` against `:`.
        let error = AnalysisAudioError.decodingFailed("Operation Interrupted")
        for stage in DurableThrowRecord.RunnerStage.allCases {
            let token = DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: stage)
            let prose = "\(stage.rawValue): \(error)"
            #expect(!token.hasPrefix("\(stage.rawValue): "), "the token answers the prose query")
            #expect(!prose.hasPrefix(DurableThrowRecord.runnerStageThrewPrefix),
                    "the prose answers the token query")
        }
    }

    @Test("the sixth prefix collides with nothing else this column holds, in either direction")
    func theSixthPrefixCollidesWithNothing() {
        let token = DurableThrowRecord.runnerStageLastErrorCode(
            for: AnalysisAudioError.decodingFailed("x"), stage: .decode
        )
        let others = [
            AnalysisWorkScheduler.maxAttemptsReachedPrefix,
            AnalysisWorkScheduler.noProgressTerminalErrorCode,
            "staleFingerprint:cachedAudioMismatch",
            "backgroundWindowExpired",
            "cancelMidRun",
            "transcription:",
            "reconciler_unavailable",
            "orphan_at_launch",
            DurableThrowRecord.jobThrewPrefix,
            DurableThrowRecord.assetResolutionThrewPrefix,
            DurableThrowRecord.sessionPipelineThrewPrefix,
            DurableThrowRecord.recoveryThrewPrefix,
            DurableThrowRecord.dayZeroThrewPrefix,
        ]
        for other in others {
            #expect(!token.hasPrefix(other), "the runner-stage token answers to \(other)")
            #expect(!other.hasPrefix(DurableThrowRecord.runnerStageThrewPrefix),
                    "\(other) answers to the runner-stage query")
        }
    }

    @Test("the token's grammar is closed: no whitespace, one balanced parenthetical")
    func grammarIsClosed() {
        // A device pull's `GROUP BY` and every LIKE query in this file's header
        // depend on it. The retired prose failed both — a space after the colon,
        // and an arbitrary sentence after that.
        for stage in DurableThrowRecord.RunnerStage.allCases {
            let token = DurableThrowRecord.runnerStageLastErrorCode(
                for: AnalysisAudioError.readerSetupFailed("a b c"), stage: stage
            )
            #expect(!token.contains(" "), "\(token) carries whitespace")
            #expect(token.filter { $0 == "(" }.count == 1, "\(token) is not one parenthetical")
            #expect(token.hasSuffix(")"), "\(token) is unbalanced")
        }
    }

    @Test("under= is a positive claim on this token too, never an absence")
    func underIsPositive() {
        let bare = DurableThrowRecord.runnerStageLastErrorCode(
            for: AnalysisAudioError.decodingFailed("x"), stage: .features
        )
        #expect(bare.contains("under=none"), "\(bare)")

        let chained = NSError(
            domain: "Outer", code: 7,
            userInfo: [NSUnderlyingErrorKey: NSError(domain: "Inner", code: 9)]
        )
        let token = DurableThrowRecord.runnerStageLastErrorCode(for: chained, stage: .features)
        #expect(token.contains("under=Inner/9"), "\(token)")
    }

    @Test("the six factories share ONE identity grammar")
    func oneIdentityGrammar() {
        // A second copy of `domain=…,code=…,under=…` is a second ruler. Every
        // factory must produce the same fields for the same error, differing
        // only in the prefix.
        let error = NSError(domain: "D", code: 4)
        let identity = DurableThrowRecord.identityFields(of: error)
        #expect(DurableThrowRecord.runnerStageLastErrorCode(for: error, stage: .decode)
            == "runnerStageThrew-decode(\(identity))")
        #expect(DurableThrowRecord.jobLastErrorCode(for: error) == "jobThrew(\(identity))")
        #expect(DurableThrowRecord.assetResolutionLastErrorCode(for: error)
            == "assetResolutionThrew(\(identity))")
    }

    // MARK: The two content-switching readers, EVERY CLAUSE PINNED

    /// playhead-3c4k's first test of `isNoProgressTerminal` PASSED FOR THE WRONG
    /// REASON: the predicate is `state == "complete" && code == …` and its
    /// fixture was `state = "failed"`, so the state clause carried the assertion
    /// and the token under test was never consulted. Both clauses are pinned
    /// here, in both directions, with a control that makes the predicate TRUE.
    private func job(state: String, code: String?) -> AnalysisJob {
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
            lastErrorCode: code,
            createdAt: 0,
            updatedAt: 0
        )
    }

    @Test("the TERMINAL arm's row still satisfies isAttemptCapTerminal, and every clause is load-bearing")
    func terminalArmIsStillRescuable() {
        let token = DurableThrowRecord.runnerStageLastErrorCode(
            for: AnalysisAudioError.decodingFailed("Operation Interrupted"), stage: .decode
        )
        // What the scheduler's terminal arm actually writes: the prefix IN FRONT.
        let written = AnalysisWorkScheduler.maxAttemptsReachedPrefix + token
        #expect(AnalysisWorkScheduler.isAttemptCapTerminal(job(state: "superseded", code: written)))
        #expect(AnalysisWorkScheduler.isRescuableTerminal(job(state: "superseded", code: written)))

        // CLAUSE 1 (state) is load-bearing: the same value on a non-superseded
        // row must NOT match, or the predicate is really just a prefix test.
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(job(state: "failed", code: written)))
        // CLAUSE 2 (prefix) is load-bearing: the RETRY arm writes the bare token
        // onto a `failed` row and must never be read as a cap-out.
        #expect(!AnalysisWorkScheduler.isAttemptCapTerminal(job(state: "superseded", code: token)))
        // ANTI-VACUITY: the predicate is capable of returning true for a value
        // that is not this token at all, so the expectations above are not
        // passing because the function is broken.
        #expect(AnalysisWorkScheduler.isAttemptCapTerminal(
            job(state: "superseded", code: "maxAttemptsReached:cancelMidRun")
        ))
    }

    @Test("no runner-stage token can be mistaken for a no-progress terminal, and every clause is pinned")
    func noProgressTerminalIsUnreachableFromThisToken() {
        let exact = AnalysisWorkScheduler.noProgressTerminalErrorCode
        for stage in DurableThrowRecord.RunnerStage.allCases {
            let token = DurableThrowRecord.runnerStageLastErrorCode(
                for: AnalysisAudioError.decodingFailed("x"), stage: stage
            )
            #expect(token != exact)
            // The reader is an EXACT match, so the fixture is put on the ONE
            // state that satisfies its other clause. 3c4k's version used
            // `failed` here and the state clause answered for it.
            #expect(!AnalysisWorkScheduler.isNoProgressTerminal(job(state: "complete", code: token)))
            #expect(!AnalysisWorkScheduler.isNoProgressTerminal(
                job(state: "complete", code: AnalysisWorkScheduler.maxAttemptsReachedPrefix + token)
            ))
        }
        // ANTI-VACUITY, both directions. The predicate DOES fire on `complete` +
        // the exact code (so the code clause above was genuinely consulted)…
        #expect(AnalysisWorkScheduler.isNoProgressTerminal(job(state: "complete", code: exact)))
        // …and does NOT on any other state with the same code (so the state
        // clause is real and the expectation above is not the state's doing).
        #expect(!AnalysisWorkScheduler.isNoProgressTerminal(job(state: "failed", code: exact)))
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

    /// The `span` characters of dense code IMMEDIATELY BEFORE `marker`.
    ///
    /// Exists because a file-wide `contains` cannot answer a question about ONE
    /// SITE, and this file has two sites that build the same value. Anchor on
    /// something only the site under test carries — a log message, not a type
    /// name — and read the window that must hold its binding. `span` is set from
    /// the real distance (the production binding plus `logger.error(` is ~100
    /// dense characters), wide enough to hold the whole statement and narrow
    /// enough that a neighbouring site cannot satisfy it.
    private func codeImmediatelyBefore(
        _ marker: String,
        span: Int,
        in dense: String
    ) throws -> String {
        let hit = try XCTUnwrap(dense.range(of: marker), "no \(marker) in the source under test")
        let start = dense.index(hit.lowerBound, offsetBy: -span, limitedBy: dense.startIndex)
            ?? dense.startIndex
        return String(dense[start..<hit.lowerBound])
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

    /// The spellings that put a Swift value's DESCRIPTION in a durable column.
    ///
    /// **The third entry is playhead-3c4k, and it is the reason this is a
    /// shared constant rather than a filter written inline at each site.**
    /// 3lc3's rule named the two spellings of the two defects in front of it,
    /// and string INTERPOLATION of an `Error` — which is `String(describing:)`
    /// by another name — walked straight through it, in the same file, in a
    /// `lastErrorCode:` argument this very finder was already reading.
    ///
    /// `\(error` rather than `\(error)` on purpose: it also catches
    /// `\(error.localizedDescription)` and `\(error as NSError)`. The cost is
    /// that a future `\(errorClass)` would trip it falsely — accepted, because
    /// a false RED is a five-second read of a named offender and a false GREEN
    /// is this bead.
    static let descriptionSpellings = ["localizedDescription", "String(describing:", "\\(error"]

    private func descriptionOffenders(in arguments: [String]) -> [String] {
        arguments.filter { argument in
            Self.descriptionSpellings.contains { argument.contains($0) }
        }
    }

    /// playhead-3c4k, stated as a test rather than as a paragraph: the rule
    /// 3lc3 shipped could not see the line 3lc3 left behind.
    func testTheOldTwoSpellingRuleWouldHaveMissedTheShippedLine() throws {
        // The retired argument, in the dense spelling this finder reads. Built
        // by concatenation so the test file does not itself interpolate an
        // error and become its own subject.
        let retired = "\"assetResolution:" + "\\(error)\""
        let old = ["localizedDescription", "String(describing:"]

        XCTAssertFalse(
            old.contains { retired.contains($0) },
            "the premise of this bead is false: 3lc3's rule DOES see \(retired)"
        )
        XCTAssertTrue(
            Self.descriptionSpellings.contains { retired.contains($0) },
            "the strengthened rule still cannot see \(retired), so it fixes nothing"
        )
        // And the terminal arm's spelling, which carries the prefix in front.
        let retiredTerminal = "\"\\(Self.maxAttemptsReachedPrefix)assetResolution:" + "\\(error)\""
        XCTAssertFalse(old.contains { retiredTerminal.contains($0) })
        XCTAssertTrue(Self.descriptionSpellings.contains { retiredTerminal.contains($0) })
    }

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
        XCTAssertTrue(
            dense.contains("DurableThrowRecord.assetResolutionLastErrorCode(for:error)"),
            "AnalysisWorkScheduler no longer builds the asset-resolution record; move this canary."
        )
        XCTAssertTrue(dense.contains("\"assetResolution.supersede\""), "vacuity: the terminal arm is gone")
        XCTAssertTrue(dense.contains("\"assetResolution.requeue\""), "vacuity: the retry arm is gone")
        // And this reader must be ABLE to see the spellings it forbids —
        // `error.localizedDescription` and an interpolated `\(error)` both
        // survive in log lines deliberately (that is 59c8's split: a log line
        // can afford prose, a column cannot), so their presence proves the
        // finder works.
        for spelling in Self.descriptionSpellings {
            XCTAssertTrue(
                dense.contains(spelling),
                "vacuity: this reader cannot see \(spelling), so its absence from an argument proves nothing"
            )
        }

        // THE RULE: nothing reaching `lastErrorCode:` may be a description —
        // in ANY of its spellings.
        let written = FMDaemonRefusalSourceCanaryTests.firstArguments(after: "lastErrorCode:", in: dense)
        XCTAssertGreaterThanOrEqual(written.count, 10, "vacuity: the durable writes were not found")
        let offenders = descriptionOffenders(in: written)
        XCTAssertTrue(
            offenders.isEmpty,
            """
            An `analysis_jobs.lastErrorCode` write is persisting a Swift value's DESCRIPTION again. \
            `localizedDescription` is LOCALIZED — the column cannot be grouped across devices — and \
            for the error types that reach these arms it is Foundation's bridged apology plus the \
            enum's declaration index. An interpolated `\\(error)` is `String(describing:)` spelled \
            differently, and is the spelling that actually shipped and produced field rows \
            (playhead-3c4k). Offending arguments: \(offenders)
            """
        )
    }

    // MARK: playhead-3c4k — per SITE, because the file has two job-cause arms

    /// The dense code IMMEDIATELY BEFORE the arm label, wide enough to hold the
    /// binding and far too narrow to reach the other arm.
    ///
    /// MEASURED, not guessed. The binding sits 113 dense characters before
    /// `"assetResolution.supersede"` and 101 before `"outerCatch.supersede"`;
    /// the two arms are ~17,000 dense characters apart. 300 is the smallest
    /// round number that still holds the window after a mutation INSERTS a
    /// laundering line ahead of the binding (AR05 adds ~55), and the
    /// asset-resolution window is measured clean of every forbidden spelling
    /// out to 420, so the margin is not bought with a false green.
    ///
    /// **The outer-catch window is NOT clean at this width** — a log line
    /// `…recovery(error: \(error))` sits 60 characters before it — which is
    /// exactly why the description rule below is asserted on the
    /// asset-resolution window only, and the outer-catch window carries the
    /// FACTORY-IDENTITY claim alone. A rule copied symmetrically here would be
    /// red on untouched `main`.
    private static let armWindow = 300

    func testTheAssetResolutionArmBindsItsOwnRecordAtItsOwnSite() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try schedulerSource())

        // THE MUTATION THIS EXISTS FOR, and it is 3lc3's DT05 one file over: a
        // file-wide `contains` is satisfied by the OTHER arm's binding while
        // this one goes back to prose. Both arms build a `DurableThrowRecord`
        // and both write `lastErrorCode:`, so only a per-site read can tell
        // which of them is which.
        let site = try codeImmediatelyBefore(
            "\"assetResolution.supersede\"",
            span: Self.armWindow,
            in: dense
        )
        XCTAssertTrue(
            site.contains("DurableThrowRecord.assetResolutionLastErrorCode(for:error)"),
            """
            The asset-resolution TERMINAL arm no longer binds its durable cause from \
            `DurableThrowRecord`. This is the ONE site of this defect class with field rows — five \
            of the nine `analysis_jobs` rows that ever carried a cause carried this arm's prose. \
            Site text: \(site)
            """
        )
        XCTAssertFalse(
            Self.descriptionSpellings.contains { site.contains($0) },
            "the asset-resolution arm binds a DESCRIPTION into its durable cause: \(site)"
        )

        // AND THE CONDITIONS MUST NOT CROSS. `jobThrew` names "the job's run
        // threw"; this arm fires before any runner exists. Either factory
        // produces a perfectly well-formed token, so nothing that reads a VALUE
        // can tell that the row is labelled with a condition it never met —
        // the same shape as sckv's SF04/SF10, one argument over.
        XCTAssertFalse(
            site.contains("DurableThrowRecord.jobLastErrorCode("),
            "the asset-resolution arm is recording the JOB-RUN condition: \(site)"
        )
        let outerCatch = try codeImmediatelyBefore(
            "\"outerCatch.supersede\"",
            span: Self.armWindow,
            in: dense
        )
        XCTAssertTrue(
            outerCatch.contains("DurableThrowRecord.jobLastErrorCode(for:error)"),
            "the outer catch no longer binds the job-run record: \(outerCatch)"
        )
        XCTAssertFalse(
            outerCatch.contains("DurableThrowRecord.assetResolutionLastErrorCode("),
            "the outer catch is recording the ASSET-RESOLUTION condition: \(outerCatch)"
        )
    }

    func testTheAssetResolutionTerminalArmStillCarriesTheAttemptCapPrefixInFront() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try schedulerSource())
        // The mirror of `testTheTerminalArmStillCarriesTheAttemptCapPrefixInFront`
        // for this arm. It needs its own rail rather than a second assertion in
        // that one: two claims in one method means one mutant can be credited
        // for reddening the other's claim (sckv's SF03/SF10).
        //
        // NO FIELD ROW HAS EVER TAKEN THIS ARM — zero of the five carried
        // `maxAttemptsReached:`, all five were the retry arm — so this is
        // exactly the direction with no witness anywhere else.
        XCTAssertTrue(
            dense.contains(
                "lastErrorCode:\"\\(Self.maxAttemptsReachedPrefix)\\(assetResolutionThrowRecord)\""
            ),
            """
            The asset-resolution TERMINAL arm no longer writes `maxAttemptsReached:` in front of \
            the token. `AnalysisWorkScheduler.isAttemptCapTerminal(_:)` matches on that prefix and \
            the cap-out-retry rescue reads it, so a capped row that loses it is abandoned \
            permanently.
            """
        )
    }

    func testTheAssetResolutionRetryArmWritesTheBareToken() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try schedulerSource())
        // The other direction, and the one all five field rows actually took.
        // The likely accident is a copy-paste: the retry arm becomes a second
        // copy of the terminal arm, and then the `maxAttemptsReached:` prefix
        // means nothing at all, because a row that spent one attempt of five
        // and a row that exhausted the cap answer to it alike. Every value test
        // in this file stays green either way.
        XCTAssertTrue(
            dense.contains("lastErrorCode:assetResolutionThrowRecord"),
            "the asset-resolution RETRY arm no longer writes the bare token"
        )
        let terminalSpelling =
            "lastErrorCode:\"\\(Self.maxAttemptsReachedPrefix)\\(assetResolutionThrowRecord)\""
        XCTAssertEqual(
            dense.components(separatedBy: terminalSpelling).count - 1,
            1,
            """
            The attempt-cap prefix is written by more than one asset-resolution arm. \
            `isAttemptCapTerminal(_:)` reads that prefix, so a retry row carrying it makes the \
            prefix stop discriminating between a job that exhausted five attempts and one that \
            spent its first.
            """
        )
    }

    func testTheAssetResolutionArmBindsTheRecordOnceAndTheLogConsumesIt() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try schedulerSource())
        XCTAssertEqual(
            FMDaemonRefusalSourceCanaryTests
                .firstArguments(after: "DurableThrowRecord.assetResolutionLastErrorCode(", in: dense)
                .count,
            1,
            "the asset-resolution arms must share ONE binding of the record"
        )
        XCTAssertTrue(
            dense.contains(
                "letassetResolutionThrowRecord=DurableThrowRecord.assetResolutionLastErrorCode(for:error)"
            ),
            "the record must be bound to a local above both arms"
        )
        XCTAssertTrue(
            dense.contains("token=\\(assetResolutionThrowRecord,privacy:.public)"),
            "the abandonment log line must consume the SAME local the column was given"
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

        // THE RULE, PART ONE: the PRODUCTION catch-all's value must be BOUND
        // FROM THE RECORD, read AT THAT SITE.
        //
        // TWO ROUNDS OF THIS, AND THE SECOND IS THE ONE THAT MATTERS. An
        // argument-only rule is defeated by one intermediate local —
        // `let throwRecord = String(describing: error)` leaves every
        // `failureReason:` argument spelling a wholesome `throwRecord` while the
        // column goes back to prose. That much was reasoned out while writing
        // mutant DT05, and the fix was a file-wide
        // `dense.contains("letthrowRecord=DurableThrowRecord.sessionFailureReason(")`.
        //
        // **DT05 THEN SURVIVED AGAINST IT.** This file has TWO
        // `sessionFailureReason` sites — the production catch-all and the DEBUG
        // seam — and DT05 only touches the first, so the seam's own binding kept
        // the file-wide `contains` true while production wrote prose. The rule
        // asserted a property of the FILE where the claim is about a SITE: the
        // standing defect class, inside the check written to catch it. Predicting
        // the mutant was not the same as running it.
        //
        // So the read is per-site now — anchored on the production catch-all's
        // own log message, which no other site carries.
        let productionCatchAll = try codeImmediatelyBefore(
            "\"Pipelinefailedforepisode",
            span: 200,
            in: dense
        )
        XCTAssertTrue(
            productionCatchAll.contains("DurableThrowRecord.sessionFailureReason("),
            """
            `runPipeline`'s catch-all no longer binds its durable reason from `DurableThrowRecord`. \
            A local rebound to a description satisfies every rule stated over the ARGUMENT — and a \
            file-wide check is satisfied by the DEBUG seam's binding while production writes prose, \
            which is exactly how mutation DT05 survived its first rail. Site text: \(productionCatchAll)
            """
        )
        XCTAssertFalse(
            productionCatchAll.contains("String(describing:")
                || productionCatchAll.contains("localizedDescription"),
            "the production catch-all binds a DESCRIPTION into its durable reason: \(productionCatchAll)"
        )

        // THE RULE, PART TWO: and no spelling reaching `failureReason:` may be a
        // description either.
        let written = FMDaemonRefusalSourceCanaryTests.firstArguments(after: "failureReason:", in: dense)
        XCTAssertGreaterThanOrEqual(written.count, 5, "vacuity: the durable writes were not found")
        // playhead-3c4k widened this from the two spellings 3lc3 named to the
        // three that exist, the third being the interpolation that shipped.
        let offenders = descriptionOffenders(in: written)
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
        // playhead-3c4k: three spellings, not two — see `descriptionSpellings`.
        let offenders = descriptionOffenders(in: written)
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

    // MARK: playhead-luie — the day-0 attempt detail, in TWO files and TWO shapes

    private func refetchServiceSource() throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        try source("Playhead/Services/AdDetection/RediffRefetch/RediffRefetchService.swift")
    }

    private func adDetectionSource() throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        try source("Playhead/Services/AdDetection/AdDetectionService.swift")
    }

    /// ARM 1 — the k-way fetch catch, an ARGUMENT, and therefore the only one of
    /// the two that any argument-shaped rule could ever have seen.
    func testTheDayZeroFetchArmNoLongerPersistsADescription() throws {
        let lines = try refetchServiceSource()
        XCTAssertGreaterThan(lines.count, 400, "source read found only \(lines.count) code lines")
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)

        // VACUITY: the arm must still exist and still be the day-0 fetch catch.
        XCTAssertTrue(
            dense.contains("DurableThrowRecord.dayZeroAttemptDetail(for:error)"),
            "RediffRefetchService no longer builds the day-0 detail record; move this canary with the code."
        )
        XCTAssertTrue(dense.contains("dayZeroExit:.fetchFailed"), "vacuity: the fetch-failure arm is gone")
        // And this reader must be ABLE to see what it forbids.
        //
        // **NOT all three spellings — this file carries exactly one, and the
        // first draft of this rail asserted all three and went red.** The lagged
        // sweep's `.failed(… error:)` arm one function up still writes
        // `String(describing: error)` deliberately, because it reaches a
        // `logger.error` and no column (59c8's split), so ONE spelling is
        // genuinely present and its visibility is what proves the reader is not
        // blind here. Asserting three would have been a vacuity guard that
        // fails for a reason unrelated to the property it guards, which is worse
        // than none.
        XCTAssertTrue(
            dense.contains("String(describing:"),
            "vacuity: this reader cannot see the spelling it forbids, so an absence proves nothing"
        )
        // The FILTER's coverage of the other two is a property of the filter,
        // not of this file, so it is checked against synthetic text instead.
        for spelling in ["error.localizedDescription", "\\(error)"] {
            XCTAssertFalse(
                descriptionOffenders(in: ["detail:\(spelling)"]).isEmpty,
                "the filter cannot see \(spelling)"
            )
        }

        // THE RULE: nothing reaching `detail:` in this file may be a description.
        let written = FMDaemonRefusalSourceCanaryTests.firstArguments(after: "detail:", in: dense)
        XCTAssertGreaterThanOrEqual(written.count, 1, "vacuity: the durable write was not found")
        let offenders = descriptionOffenders(in: written)
        XCTAssertTrue(
            offenders.isEmpty,
            """
            A `rediff_day_zero_attempts.lastDetail` write is persisting a Swift value's DESCRIPTION \
            again. This arm catches a `URLSession` download, so the description is an \
            `NSURLErrorDomain` dump carrying the ENCLOSURE URL, a LOCALIZED sentence and a HEAP \
            POINTER — the two rows preserved in `3gzp/gt.sqlite` differ from each other only in \
            that pointer, so the column cannot group even two instances of one failure. \
            Offenders: \(offenders)
            """
        )
    }

    /// ARM 1, per SITE. The `detail:` rule above is a property of the FILE, and
    /// 3lc3's DT05 is the record of what that is worth: one intermediate local
    /// (`let d = String(describing: error)`) leaves every argument spelling a
    /// wholesome name while the column goes back to prose.
    func testTheDayZeroFetchArmBindsItsRecordAtItsOwnSite() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try refetchServiceSource())
        // Anchored on the arm's own return, which no other site in the file
        // carries. 240 dense characters holds the `recordOutcome` call and its
        // three arguments and reaches nothing else — the nearest neighbouring
        // `recordOutcome` is ~700 characters up.
        let site = try codeImmediatelyBefore("dayZeroExit:.fetchFailed", span: 240, in: dense)
        XCTAssertTrue(
            site.contains("DurableThrowRecord.dayZeroAttemptDetail(for:error)"),
            """
            The day-0 FETCH arm no longer builds its durable detail from `DurableThrowRecord` at its \
            own site. Site text: \(site)
            """
        )
        XCTAssertFalse(
            Self.descriptionSpellings.contains { site.contains($0) },
            "the day-0 fetch arm binds a DESCRIPTION into its durable detail: \(site)"
        )
    }

    /// ARM 2 — and this is the shape the whole bead turns on.
    ///
    /// `failed.detail = String(describing: error)` is an ASSIGNMENT TO A
    /// PROPERTY, not a labelled argument. Every sweep this defect class has been
    /// hunted with — 3c4k's pass A (curated cause labels), pass B (schema-derived
    /// labels), pass C (any labelled argument carrying a description) — reads
    /// ARGUMENTS, and so does `descriptionOffenders` above. None of them could
    /// have found this line, and neither could a rule written over `detail:`.
    /// It needs a finder of its own.
    func testTheDayZeroPersistArmNoLongerPersistsADescription() throws {
        let lines = try adDetectionSource()
        XCTAssertGreaterThan(lines.count, 5_000, "source read found only \(lines.count) code lines")
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(lines)

        // VACUITY: the arm exists, and this reader can see the spelling it bans.
        XCTAssertTrue(
            dense.contains("outcome(.persistFailed,divergentSlotCount:"),
            "the day-0 persist-failure arm is gone; move this canary with the code."
        )
        XCTAssertTrue(
            dense.contains("String(describing:"),
            "vacuity: this reader cannot see the spelling it forbids (it survives in log lines here)"
        )

        // THE RULE, over the ASSIGNMENT rather than over an argument: every
        // right-hand side given to a `.detail` property in this file, read to the
        // end of the statement.
        let assigned = FMDaemonRefusalSourceCanaryTests.firstArguments(after: ".detail=", in: dense)
        XCTAssertEqual(assigned.count, 1, "expected exactly one `.detail =` assignment in this file")
        XCTAssertTrue(
            assigned[0].hasPrefix("DurableThrowRecord.dayZeroAttemptDetail(for:error"),
            """
            The day-0 PERSIST arm assigns something other than the durable throw record to \
            `RediffDayZeroMintOutcome.detail`, which is carried into \
            `rediff_day_zero_attempts.lastDetail` one struct later. Assigned: \(assigned[0])
            """
        )
        XCTAssertTrue(
            descriptionOffenders(in: assigned).isEmpty,
            "the day-0 persist arm assigns a DESCRIPTION to a durable field: \(assigned)"
        )
    }

    /// The finder above must be able to FAIL, and the shape it is aimed at is
    /// not one any other rail in this class covers.
    ///
    /// Stated as a test rather than as a comment for the same reason
    /// `testTheOldTwoSpellingRuleWouldHaveMissedTheShippedLine` is: a claim that
    /// an old rule was blind is checkable, and checking it is what stops the new
    /// rule from being the old one with a longer name.
    func testAnArgumentShapedRuleWouldHaveMissedThePersistArm() throws {
        // The retired line, in the dense spelling these finders read. Built by
        // concatenation so this file does not itself describe an error.
        let retired = "failed.detail=String(describing:" + "error)"

        // Every labelled-argument finder in this class, run over it: the line
        // contains no `label:` at depth 0 at all, so all of them return nothing
        // and report clean.
        for label in ["detail:", "lastErrorCode:", "failureReason:", "deferReason:"] {
            XCTAssertTrue(
                FMDaemonRefusalSourceCanaryTests.firstArguments(after: label, in: retired).isEmpty,
                "\(label) unexpectedly matched the assignment shape; the premise of this rail is wrong"
            )
        }
        // The assignment finder sees it, and sees it as an offender.
        let seen = FMDaemonRefusalSourceCanaryTests.firstArguments(after: ".detail=", in: retired)
        XCTAssertEqual(seen.count, 1, "the assignment finder cannot see the shape it exists for")
        XCTAssertFalse(
            descriptionOffenders(in: seen).isEmpty,
            "the assignment finder sees the line but does not call it an offender"
        )
    }

    /// Neither arm may restate the exit inside the token.
    ///
    /// One prefix covers both arms deliberately — `lastExit` carries the
    /// discrimination and is written from the same outcome in the same
    /// statement. The failure this guards is the opposite of the scheduler's:
    /// there, folding two conditions into one prefix would DELETE a
    /// discrimination; here, splitting one condition into two prefixes would
    /// ADD a second ruler for a quantity the row already measures, and the first
    /// symptom would be a pull whose `lastExit` and `lastDetail` disagree.
    func testNeitherDayZeroArmInventsASecondArmDiscriminator() throws {
        let service = FMDaemonRefusalSourceCanaryTests.denseCode(try refetchServiceSource())
        let detection = FMDaemonRefusalSourceCanaryTests.denseCode(try adDetectionSource())
        for (name, dense) in [("RediffRefetchService", service), ("AdDetectionService", detection)] {
            let calls = FMDaemonRefusalSourceCanaryTests.firstArguments(
                after: "DurableThrowRecord.dayZeroAttemptDetail(",
                in: dense
            )
            XCTAssertEqual(calls.count, 1, "\(name) must build the day-0 detail exactly once")
            XCTAssertEqual(
                calls[0],
                "for:error",
                """
                \(name) is passing something other than the caught error to the day-0 detail record. \
                Got: \(calls[0])
                """
            )
        }
        // And the factory takes no arm parameter, so neither site can be given
        // one without this rail's `for:error` equality failing first.
        XCTAssertFalse(
            service.contains("dayZeroAttemptDetail(for:error,")
                || detection.contains("dayZeroAttemptDetail(for:error,"),
            "a second discriminator has been threaded into the day-0 token"
        )
    }

    // MARK: playhead-q93o — the rule that had to stop being about a FILE
    //
    // Everything above this line is stated over ONE FILE: the file that writes
    // the column, or the file that assigns the field. That is what let the
    // seventh site ship. `AnalysisJobRunner` builds a description, an
    // `AnalysisOutcome.StopReason` associated value CARRIES it, a
    // `case .failed(let reason)` in `AnalysisWorkScheduler` unwraps it, and two
    // arms interpolate it into `analysis_jobs.lastErrorCode` — while
    // `testTheSchedulerArmsNoLongerPersistADescription` read every
    // `lastErrorCode:` argument in the scheduler, found the identifier `reason`,
    // and reported clean. FOR FOUR MONTHS, WITH FIELD ROWS.
    //
    // The rules below are stated over the CARRIER instead: every construction of
    // the enum case whose payload reaches the column, in EVERY production file.

    private func runnerSource() throws -> [FMDaemonRefusalSourceCanaryTests.SourceLine] {
        try source("Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift")
    }

    /// Every production Swift file. The walk is the whole point — a rule that
    /// names the files it reads can only ever cover the sites somebody already
    /// knew about.
    private func productionSwiftFiles() throws -> [URL] {
        var root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        for _ in 0..<3 {
            root.deleteLastPathComponent()
        }
        root.appendPathComponent("Playhead", isDirectory: true)
        let walker = try XCTUnwrap(
            FileManager.default.enumerator(at: root, includingPropertiesForKeys: nil),
            "could not walk \(root.path)"
        )
        let files = walker.compactMap { $0 as? URL }.filter { $0.pathExtension == "swift" }
        XCTAssertGreaterThan(files.count, 100, "source walk found only \(files.count) Swift files")
        return files
    }

    /// The two markers that construct the carrier whose payload reaches
    /// `analysis_jobs.lastErrorCode` and `work_journal.metadata`.
    ///
    /// **The LABELS are why this list can be short.** `AnalysisOutcome.StopReason`
    /// declares `failed(code:)` and `interrupted(code:)`, so the compiler refuses
    /// every other construction spelling — `.failed(x)` does not build. Before
    /// playhead-q93o the case was `failed(String)` and a rule like this one would
    /// have had to guess at spellings; now it enumerates from one marker per
    /// case. The whitespace-stripped `denseCode` covers `. failed( code : x )`.
    static let stopReasonConstructionMarkers = [".failed(code:", ".interrupted(code:"]

    /// EVERY production construction of the carrier, with the file it is in.
    private func stopReasonPayloads() throws -> [(file: String, argument: String)] {
        var found: [(file: String, argument: String)] = []
        for url in try productionSwiftFiles() {
            let dense = FMDaemonRefusalSourceCanaryTests.denseCode(
                FMDaemonRefusalSourceCanaryTests.codeLines(of: try String(contentsOf: url, encoding: .utf8))
            )
            for marker in Self.stopReasonConstructionMarkers {
                for argument in FMDaemonRefusalSourceCanaryTests.firstArguments(after: marker, in: dense) {
                    found.append((file: url.lastPathComponent, argument: argument))
                }
            }
        }
        return found
    }

    /// THE RULE THIS BEAD EXISTS FOR: no production construction of the carrier,
    /// in any file, may build a description.
    func testNoProductionSiteBuildsTheStopReasonPayloadFromADescription() throws {
        let payloads = try stopReasonPayloads()

        // VACUITY, THREE WAYS. The walk found the carrier at all…
        XCTAssertGreaterThanOrEqual(
            payloads.count, 8,
            "the carrier's constructions were not found — \(payloads.count) sites; move this canary with the code"
        )
        // …it found them in the file that actually builds them…
        XCTAssertTrue(
            payloads.contains { $0.file == "AnalysisJobRunner.swift" },
            "no construction found in AnalysisJobRunner.swift, so this rule is reading nothing"
        )
        // …and the filter it applies demonstrably fires. Checked against
        // synthetic text rather than against the tree, because "is the filter
        // able to see a description" is a property of the filter.
        for spelling in ["\\(error)", "String(describing:error)", "error.localizedDescription"] {
            XCTAssertFalse(
                descriptionOffenders(in: ["\"decode:\(spelling)\""]).isEmpty,
                "the filter cannot see \(spelling)"
            )
        }

        let offenders = payloads.filter { !descriptionOffenders(in: [$0.argument]).isEmpty }
        XCTAssertTrue(
            offenders.isEmpty,
            """
            An `AnalysisOutcome.StopReason` payload is being built from a Swift value's DESCRIPTION. \
            That payload is carried into `analysis_jobs.lastErrorCode` and into `work_journal.metadata` \
            by `AnalysisWorkScheduler`'s `.failed` / `.interrupted` arms, TWO HOPS AWAY IN ANOTHER FILE — \
            which is why no rule stated over `lastErrorCode:` arguments can see this, and why five such \
            lines shipped for four months past a green canary (playhead-q93o). Two field rows carried \
            one of them, one on the arm that permanently retires the job. \
            Offenders: \(offenders.map { "\($0.file): \($0.argument)" })
            """
        )
    }

    /// The premise, checked rather than asserted: 3lc3's rule, run over the
    /// shipped lines, reports CLEAN.
    ///
    /// Same discipline as `testTheOldTwoSpellingRuleWouldHaveMissedTheShippedLine`
    /// one bead over — a claim that the old rule was blind is checkable, and
    /// checking it is what stops the new rule from being the old one relocated.
    func testTheSchedulerArgumentRuleCannotSeeTheRunnerSitesAtAll() throws {
        // The two scheduler arguments, in the dense spelling that finder reads.
        // These are the REAL production spellings, and neither is a description.
        let schedulerArguments = ["reason", "\"\\(Self.maxAttemptsReachedPrefix)\\(reason)\""]
        XCTAssertTrue(
            descriptionOffenders(in: schedulerArguments).isEmpty,
            "the premise of this bead is false: 3lc3's rule DOES flag the scheduler's arms"
        )
        // And the value they carry, built two files away, IS one. Assembled by
        // concatenation so this test file does not itself interpolate an error.
        let retired = "\"decode:" + "\\(error)\""
        XCTAssertFalse(
            descriptionOffenders(in: [retired]).isEmpty,
            "the description filter cannot see the line that shipped"
        )
        // So the rule is sound and the SCOPE was the defect: a description that
        // never appears in the writing file is invisible to a rule about that
        // file, no matter how many spellings the rule knows.
        XCTAssertTrue(
            FMDaemonRefusalSourceCanaryTests
                .firstArguments(after: "lastErrorCode:", in: "lastErrorCode:reason,state:\"failed\"")
                .allSatisfy { descriptionOffenders(in: [$0]).isEmpty },
            "the argument finder is not reproducing the scheduler's shape"
        )
    }

    /// The rule above is stated over a MARKER SET, so it is only as complete as
    /// that set. This closes it in both directions.
    ///
    /// A construction the compiler accepts must spell one of the two markers —
    /// `AnalysisOutcome.StopReason` declares its payloads with labels precisely
    /// so. What this rail adds is the CONVERSE: if a new production file starts
    /// building the carrier, the rule's scope has changed and a human has to
    /// look. Same discipline as `scripts/singleton-slot-allowlist.json` — an
    /// entry that matches nothing fails too, because a licence for a site nobody
    /// can find has been inherited by whatever took its name.
    func testTheCarrierIsConstructedOnlyWhereThisRailExpects() throws {
        let files = Set(try stopReasonPayloads().map(\.file))
        XCTAssertEqual(
            files, ["AnalysisJobRunner.swift"],
            """
            The set of production files constructing `AnalysisOutcome.StopReason`'s payload has \
            changed. That is not necessarily wrong — but this canary's per-site stage rails below are \
            written for AnalysisJobRunner alone, so the new site is currently covered ONLY by the \
            tree-wide description rule. Extend the per-site rails, then update this expectation. \
            Found: \(files.sorted())
            """
        )
    }

    /// **THE ARGUMENT-SHAPED RULE ABOVE HAS EXACTLY ONE HOLE AND THIS IS IT.**
    ///
    /// Found by running mutant Q05, not by reasoning: a SIXTH catch added to
    /// `AnalysisJobRunner`, laundering a description through a local
    /// (`let poison = "sixthStage: \(error)"; … .failed(code: poison)`) and
    /// SURVIVED every rail in this class. The tree-wide rule sees the wholesome
    /// identifier `poison`; the per-site rails below are anchored on the five
    /// log messages that exist and never read the new site; the exact-count
    /// rails still find their five; and the closed-world file set is unchanged
    /// because the new site is in the same file. The whole gate returned rc=0.
    ///
    /// The remedy is the one `scripts/singleton-slot-allowlist.json` uses: an
    /// inventory that is CLOSED IN BOTH DIRECTIONS. Every argument this carrier
    /// is ever given is enumerated, so a new construction — of any spelling, in
    /// this file or another — fails here and has to be signed for.
    ///
    /// Three kinds are allowed, and nothing else:
    ///   * `throwRecord`, the local bound from `DurableThrowRecord` (five stage
    ///     catches, each pinned to its own site below);
    ///   * `code`, the local `zeroCoverageDisposition` builds from the closed
    ///     `TranscriptFailureReason.failureClass` vocabulary;
    ///   * one compile-time LITERAL, which is identical on every device and in
    ///     every locale and therefore already groups.
    func testTheCarrierConstructionsAreAnExactInventory() throws {
        let payloads = try stopReasonPayloads()
        let counted = Dictionary(grouping: payloads, by: \.argument).mapValues(\.count)
        XCTAssertEqual(
            counted,
            [
                "throwRecord": DurableThrowRecord.RunnerStage.allCases.count,
                "code": 2,
                "\"noshardswithindesiredcoverage\"": 1,
            ],
            """
            The set of values given to `AnalysisOutcome.StopReason`'s payload has changed. This is \
            the inventory that closes the one hole the argument-shaped rule above cannot see: a NEW \
            construction laundering a description through a local of any other name (mutant Q05, \
            which survived everything else in this class). If the new site is legitimate, give it a \
            per-site rail below and then extend this expectation — do not widen it alone. Found: \
            \(counted)
            """
        )
    }

    /// The hop itself, pinned. The rule above is scoped to the carrier because
    /// the carrier is what feeds the column; if that stops being true the rule
    /// is guarding nothing and must be moved rather than left looking green.
    func testTheColumnIsStillFedByThatCarrierAndByThoseTwoArms() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try schedulerSource())

        // The unwrap.
        XCTAssertTrue(dense.contains("case.failed(letreason):"), "the `.failed` arm's binding is gone")
        XCTAssertTrue(
            dense.contains("case.interrupted(letreason):"),
            "the `.interrupted` arm's binding is gone"
        )
        // The three durable writes that value reaches, verbatim.
        XCTAssertTrue(
            dense.contains("lastErrorCode:\"\\(Self.maxAttemptsReachedPrefix)\\(reason)\""),
            "the `.failed` TERMINAL arm no longer carries the payload into the column"
        )
        XCTAssertEqual(
            FMDaemonRefusalSourceCanaryTests
                .firstArguments(after: "lastErrorCode:", in: dense)
                .filter { $0 == "reason" }
                .count,
            2,
            "expected exactly two bare-`reason` durable writes (the `.failed` retry arm and `.interrupted`)"
        )
        // And the SECOND durable column the same value reaches, which is easy to
        // forget because it is spelled as a dictionary value rather than as a
        // labelled argument.
        XCTAssertEqual(
            dense.components(separatedBy: "\"runner_reason\":reason").count - 1,
            3,
            "the `runner_reason` -> work_journal.metadata writes have changed shape"
        )
    }

    // MARK: playhead-q93o — per SITE, because a right token with a wrong STAGE
    // is a well-formed value and no value test can see it

    /// Each stage catch, anchored on its OWN log message — the one thing no
    /// other site in the file carries. A file-wide `contains` is satisfied by
    /// any one of the five while the other four go back to prose, which is 3lc3's
    /// DT05 with five doors instead of two.
    ///
    /// Span measured, not guessed: the binding plus the `logger.error(` line is
    /// ~210 dense characters at the widest site (`fetchChunks`), and the nearest
    /// two sites are ~2,000 dense characters apart. 320 holds the whole statement
    /// after a mutation inserts a laundering line and still cannot reach a
    /// neighbour.
    private static let stageWindow = 320

    /// (stage, the log message that anchors its site).
    private static let stageSites: [(DurableThrowRecord.RunnerStage, String)] = [
        (.decode, "\"Decodefailedforjob"),
        (.features, "\"Featureextractionfailedforjob"),
        (.fetchChunks, "\"Failedtofetchtranscriptchunksforjob"),
        (.hotPath, "\"Hot-pathdetectionfailedforjob"),
        (.backfill, "\"Backfilldetectionfailedforjob"),
    ]

    func testEveryRunnerStageCatchBindsTheRecordAtItsOwnSite() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try runnerSource())
        // Vacuity: this reader must be able to SEE a description, because all
        // five log lines still carry the raw error deliberately (59c8's split).
        XCTAssertTrue(
            dense.contains("\\(error)"),
            "vacuity: this reader cannot see the spelling it forbids, so an absence proves nothing"
        )
        for (stage, anchor) in Self.stageSites {
            let site = try codeImmediatelyBefore(anchor, span: Self.stageWindow, in: dense)
            XCTAssertTrue(
                site.contains("DurableThrowRecord.runnerStageLastErrorCode(for:error,"),
                """
                The `\(stage.rawValue)` stage catch no longer builds its durable cause from \
                `DurableThrowRecord` at its own site. Site text: \(site)
                """
            )
            XCTAssertFalse(
                Self.descriptionSpellings.contains { site.contains($0) },
                "the `\(stage.rawValue)` catch binds a DESCRIPTION into its durable cause: \(site)"
            )
        }
    }

    /// Deliberately a SEPARATE method from the binding rule above, not a second
    /// assertion inside it: "this site stopped using the record" and "this site
    /// records the wrong stage" are different defects, and one method covering
    /// both lets one mutant be credited for reddening the other's claim
    /// (`playhead-sckv`'s SF03/SF10, which cost a round).
    func testEveryRunnerStageCatchRecordsITSOWNStage() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try runnerSource())
        // KEYED ON THE CASE NAME, not the raw value: the source spells
        // `stage: .hotPath`, which is the case. Mutant Q06 changed a raw value
        // and this rail reported "the hot_path catch is not recording its own
        // stage" — a true failure with a false reason, because it was reading
        // the greppable stem as though it were the Swift identifier. The two are
        // pinned equal by `theStemsArePreserved`; here the case name is used.
        for (stage, anchor) in Self.stageSites {
            let caseName = String(describing: stage)
            let site = try codeImmediatelyBefore(anchor, span: Self.stageWindow, in: dense)
            XCTAssertTrue(
                site.contains("stage:.\(caseName))"),
                """
                The `\(stage.rawValue)` catch is not recording its own stage. A different case here \
                produces a perfectly well-formed `runnerStageThrew-<other>(…)` naming a stage that did \
                not run — `analysis_jobs` has no stage column, so nothing else in the row can \
                contradict it, and no value test in this file can see it. Site text: \(site)
                """
            )
            for other in DurableThrowRecord.RunnerStage.allCases where other != stage {
                XCTAssertFalse(
                    site.contains("stage:.\(String(describing: other)))"),
                    "the `\(caseName)` catch also names `\(String(describing: other))`: \(site)"
                )
            }
        }
        // And every stage is used exactly once, so a mutation that points two
        // catches at one stage — leaving one stage unreachable and one row lying
        // — fails here rather than passing every per-site read above.
        for stage in DurableThrowRecord.RunnerStage.allCases {
            let marker = "stage:.\(String(describing: stage)))"
            XCTAssertEqual(
                dense.components(separatedBy: marker).count - 1,
                1,
                "`\(String(describing: stage))` is recorded by "
                    + "\(dense.components(separatedBy: marker).count - 1) sites, not one"
            )
        }
    }

    func testTheRunnerBindsEachRecordOnceAndTheLogConsumesIt() throws {
        let dense = FMDaemonRefusalSourceCanaryTests.denseCode(try runnerSource())
        // `playhead-sckv`'s discipline: the column and the log consume ONE local,
        // so they cannot disagree about the throw in front of them.
        XCTAssertEqual(
            FMDaemonRefusalSourceCanaryTests
                .firstArguments(after: "DurableThrowRecord.runnerStageLastErrorCode(", in: dense).count,
            DurableThrowRecord.RunnerStage.allCases.count,
            "expected exactly one record binding per stage"
        )
        XCTAssertEqual(
            dense.components(separatedBy: "letthrowRecord=DurableThrowRecord.runnerStageLastErrorCode(")
                .count - 1,
            DurableThrowRecord.RunnerStage.allCases.count,
            "each stage catch must bind the record to a local above its write"
        )
        XCTAssertEqual(
            dense.components(separatedBy: "token=\\(throwRecord,privacy:.public)").count - 1,
            DurableThrowRecord.RunnerStage.allCases.count,
            "each stage's log line must consume the SAME local the outcome was given"
        )
        XCTAssertEqual(
            dense.components(separatedBy: ".failed(code:throwRecord)").count - 1,
            DurableThrowRecord.RunnerStage.allCases.count,
            "each stage catch must hand the bound record to the outcome"
        )
    }
}
