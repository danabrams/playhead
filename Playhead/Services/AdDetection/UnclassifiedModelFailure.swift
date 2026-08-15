// UnclassifiedModelFailure.swift
// playhead-59c8: `ModelManagerError 1001` is NOT a daemon condition, and the
// reason is that it is not a CONDITION at all. This file is the decision site
// and carries the measurement the decision rests on.
//
// THE FIELD ROW. The 2026-08-14 device pull (`db-pull10`), `backfill_jobs`,
// asset A9F6DF05, phase `fullEpisodeScan`:
//
//     status      = failed
//     retryCount  = 3
//     deferReason = Error Domain=FoundationModels.LanguageModelError Code=-1
//                   "... (ModelManagerServices.ModelManagerError error 1001.)"
//
// `FMDaemonRefusal.classify(_:)` recognises two daemon conditions and this is
// neither, so the throw reached `BackfillJobRunner`'s generic arm and was
// recorded with `retryCount: job.retryCount + 1`. Three of those and the row
// is at `AdmissionController.maxRetries`, where both coarse candidate queries
// and the ad-scan re-drive stop seeing it. A9F6DF05 is the one row on that pull
// with a reachable ceiling — measured `adScanFraction` 0.8866 against a
// transcript ceiling of 0.9965, with the 0.98 floor still in reach.
//
// ===== WHAT 1001 ACTUALLY IS, MEASURED FROM THE SHIPPED FRAMEWORK =====
//
// `ModelManagerServices.ModelManagerError` is a 49-case Swift enum conforming
// to `CustomNSError`, so its NSError code is its own `errorCode`, and
// `errorCode` is a 49-entry lookup table indexed by the enum tag. Read out of
// the loaded framework on this machine (macOS 27 / Xcode-beta, the same SDK
// generation as the device's iOS 27):
//
//   * `rawCode.getter` is a single `b` to `errorCode.getter` — the two are the
//     same function, so `errorCode == rawCode`.
//   * the table's 49 entries run 1001…1046. **Entry 0 is 1001.**
//   * the Swift reflection field descriptor (`__TEXT,__swift5_fieldmd`) gives
//     the enum's declaration order: 29 payload cases (tags 0…28) then 20 empty
//     cases (tags 29…48), which is exactly the tag order a multi-payload enum
//     gets. **Declaration index 0 is `inferenceError(InferenceError)`.**
//   * cross-checked on twenty-odd anchors against the framework's own
//     `descriptionWithoutUnderlying` strings — tag 46 = `rateLimited` = "Client
//     rate limit exceeded, try again later" = code 1043; tag 40 =
//     `deviceNotEligible` = 1030; tag 34 = `insufficientSystemResources` = 1012
//     and tag 8 = `insufficientSystemResourcesWithJetsamReason` = 1012 (a pair
//     sharing one code, which is what makes the alignment falsifiable).
//
// **So 1001 is `ModelManagerError.inferenceError`, a CATEGORY WRAPPER over a
// 25-case `ModelManagerServices.InferenceError`** — enumerated from the same
// `.tbd`, every case carrying an `InferenceError.Context`: `rateLimited`,
// `resourcesBusy`, `networkError`, `hostFailed`, `hostError`, `loadFailed`,
// `notLoaded`, `alreadyLoaded`, `operationCancelled`, `internalError`,
// `inferenceFailed`, `streamNotFound`, `deviceConnectionError`, `xpcError`,
// `unspecified`, `unspecifiedUnderlyingError`, `unrecognizedUnderlyingError`,
// `notImplemented`, `unsupportedRequestType`, `invalidClientData`,
// `operationNotAllowed`, `responseEncodingFailed`, `conversionNotSupported`,
// `versionNotSupported`, `assetVersionMismatch`.
//
// Some of those could only ever be momentary and some could only ever be
// permanent, and **1001 is every one of them**. The code names the LAYER the
// error came from; it does not name what went wrong. That is this repo's
// standing defect class stated in Apple's numbering: a value that names one
// thing (which subsystem answered) read as though it named another (whether
// the condition will clear).
//
// ===== APPLE AGREES, AND NAMES THE DISCRIMINATOR WE CANNOT REACH =====
//
// The bead asks, if the condition is CONDITIONAL, what distinguishes the two
// and where that fact is available at throw time. It has an exact answer.
// `InferenceError` carries **`retryAfterDate: Foundation.Date?`**, built from
// `InferenceError.Context`, whose initialiser is
//
//     init(additionalDescription: String, domain: String, code: Int,
//          userInfo: [String: String], fallbackAllowed: Bool,
//          underlyingError: Error?, retryAfter: Date?)
//
// So "temporary, try again later" is a FIELD in this framework, populated per
// CONDITION rather than implied by a code — and `fallbackAllowed` is a second
// one. Grepped across the whole framework, `retryAfter` occurs on
// `InferenceError` and `InferenceError.Context` and **nowhere else**:
// `ModelManagerError`, the level whose code is 1001, has none. So nothing at
// the 1001 level can answer the question, the type that can is private and not
// ours to import, and the `NSError` the row actually carried did not bring it
// across.
//
// That is why the outcome here is a NAMED, COUNTABLE RECORD rather than a
// verdict. Admitting 1001 to `FMDaemonRefusal` would give "it will heal on its
// own, retry forever, never spend the budget" to a population that contains
// permanent members, on the strength of a number that Apple's own API treats as
// insufficient to decide it.
//
// ===== THE OUTER `Code=-1` IS THE FRAMEWORK DECLINING TO CLASSIFY =====
//
// `FoundationModels.LanguageModelError` has exactly nine cases and conforms to
// `Error`, `LocalizedError` and `CustomDebugStringConvertible` — **not** to
// `CustomNSError` (checked in the iOS 27 SDK's own `.tbd`: there is no
// `errorCode` / `errorDomain` / `errorUserInfo` symbol for it). A Swift error
// enum bridged without `CustomNSError` cannot produce a negative code, so `-1`
// is not one of the nine cases; it is a sentinel meaning "none of them".
//
// And the shape of the persisted string says the same thing from the other
// side. `String(describing:)` of a Swift `LanguageModelError` value renders as
// `rateLimited(FoundationModels.LanguageModelError.RateLimited(…))`. What is in
// the column is `Error Domain=… Code=… "…"`, which is `NSError.description`.
// **The thrown value was an `NSError`, not a Swift `LanguageModelError`** — so
// every typed cast in `SemanticScanStatus.from(error:)` missed it and the
// status was `.failedTransient` BY FALLTHROUGH, not by classification. There is
// no cast that would have caught it, and adding one for a private framework's
// type is not available to us.
//
// ===== WHY THE COST BOUND THAT LICENSES kvs8/e75l DOES NOT REACH HERE =====
//
// `FMDaemonThrottle`'s "there is deliberately no terminal cause in this file"
// is licensed by a measured cost: a refused or wedged METADATA round trip costs
// at most `FMInferenceDeadline.metadata` (30 s) of daemon time per drain, and
// it happens before a single window is planned, so retrying it forever is
// cheap. `inferenceError` is thrown by the inference layer and can arrive after
// real generation has been paid for. Retrying a permanent one without limit is
// not better than a dead row — it is the same dead row plus an unbounded FM
// bill, which is `playhead-ejr7`'s "41 % of FM compute produces nothing"
// arriving through a different door.
//
// ===== AND THE DAEMON HAS A VOCABULARY FOR "TRY AGAIN" AND DID NOT USE IT ====
//
// Four `ModelManagerError` cases say so in their own description text —
// `rateLimited` ("Client rate limit exceeded, try again later"),
// `deniedDueToSystemState` ("…try again later"), `deniedDueToSpecifiedSystemState`
// and `cancelledByPreemption` ("Canceled due to preemption, try again"). Those
// carry codes 1043, 1013, 1013 and 1039. The row does not carry any of them.
//
// **VERDICT: do not reclassify. There is therefore also no repair to make** —
// the rows retired on this cause were retired under a rule this bead does not
// change, so `playhead-e6d3`'s "a repair is principled exactly when the rows
// were failed under a rule that no longer holds" does not license one.
//
// ===== WHAT THE DEFECT ACTUALLY IS: THE RECORD, NOT THE CLASSIFICATION =====
//
// The generic arm wrote `String(describing: error)` into the durable column.
// `playhead-v7q6` forbids exactly that — "never `String(describing:)` of a
// framework error, which cannot be grouped, cannot be counted, and changes
// shape with the OS" — and the generic arm was the one site still doing it. On
// this pull it produced 300 characters of `NSError` prose, and the only reason
// this bead exists is that somebody read all 300 of them by hand.
//
// So the cause gets a NAMED token on the FAILING side, and the token carries
// the DISCRIMINATOR that a future bead would need in order to promote one
// specific `(domain, code)` out of this bucket: the outer NSError identity and
// the DEEPEST underlying one. Next pull, `grep -c 'unclassifiedModelError-'`
// counts the population and every row names which framework condition it was.
//
// Two properties, stated so they can be checked:
//
//   * **What it reads when the thing never happened.** The token is written
//     only from the catch-all, so no rows means no unclassified throws. Within
//     a row, `under=none` is a positive claim that the error carried no
//     underlying chain — never an empty string and never a missing field.
//   * **It is not a second `retryCount`.** Nothing here counts anything. The
//     token records the IDENTITY of a throw the app could not classify; it does
//     not claim the throw was transient, durable, this job's fault or the
//     daemon's.
//
// LIMIT, named rather than hidden: `PersistedStateInvariantEvaluator.sanitize`
// truncates a `deferReason` to 80 characters for its witness line, which is
// shorter than a full token. The COLUMN holds the whole thing and a device pull
// reads the column; the reporter's witness is a summary and will show the
// prefix plus the outer domain. Do not read a truncated witness as the record.

import Foundation

/// The identity of a throw `BackfillJobRunner` could not classify, and the
/// named cause it writes to `backfill_jobs.deferReason` for it.
///
/// **This type deliberately does not decide anything.** It is not a sibling of
/// ``FMDaemonRefusal`` — that enum answers "did the daemon decline to serve
/// this job", and the whole argument in this file's header is that
/// `ModelManagerError 1001` cannot answer that question either way. What this
/// type does is make the unanswered question COUNTABLE, so the next device pull
/// can bound the population instead of re-reading prose.
enum UnclassifiedModelFailure {

    /// The greppable family. Its own prefix, sharing none with
    /// `rateLimited-`, `metadataStall-`, `inferenceTimeout-`,
    /// `expiredWithoutProgress-`, `cancelled-during-` or
    /// `underCoverageBudgetSpent-`.
    ///
    /// The prefix is the CONDITION, per `FMDaemonRefusal`'s R2-Fix1 rule, and
    /// the condition here is precisely "the app could not classify this". A
    /// token that joined any existing family would put an unclassified throw
    /// into a count that means something specific.
    static let causePrefix = "unclassifiedModelError"

    /// The log event for the generic arm.
    ///
    /// A NEW name rather than a field on the existing
    /// `FM backfill job … failed: case=untyped` line, for the reason
    /// ``FMDaemonRefusal/logEvent`` gives: an event NAME is the unit a support
    /// -bundle grep counts, and widening an existing one silently changes what
    /// that grep counts.
    ///
    /// **Spelled `failureEvent`, not `logEvent`, and the reason is a measured
    /// one.** `FMDaemonRefusalSourceCanaryTests` pins that `BackfillJobRunner`
    /// reads `logEvent` EXACTLY ONCE — its finder is a bare substring, because
    /// no textual reader can tell which type a property access belongs to — so
    /// a second property of that name anywhere in the runner reddens a rail
    /// that is about a different family entirely. Observed, not reasoned: the
    /// first draft of this file called it `logEvent` and turned three of that
    /// class's assertions red. The daemon-refusal vocabulary (`logEvent`,
    /// `drainStoppedEvent`, any `reason:` argument ending in `Cause`, the
    /// `cause=` and `siblingCause=` log fields) is RESERVED in this runner, and
    /// that is the correct outcome rather than a workaround: this type is
    /// deliberately not a daemon refusal, so it should not answer to a daemon
    /// refusal's names any more than its token should answer to `rateLimited-`.
    static let failureEvent = "fm.backfill.job_failed_unclassified_model_error"

    /// How deep to walk `NSUnderlyingErrorKey`.
    ///
    /// Bounded because the chain is built by frameworks we do not control and a
    /// cycle would otherwise hang the drain's failure path — the one path that
    /// must never be the reason a batch is lost. Eight is far past any observed
    /// chain (the field row's is two).
    static let maxUnderlyingDepth = 8

    /// The longest domain fragment a token will carry.
    ///
    /// Domains are fully-qualified Swift type names, so 64 clears every one
    /// observed (`ModelManagerServices.ModelManagerError` is 38,
    /// `FoundationModels.LanguageModelError` is 35) while bounding what a
    /// hostile or generated domain can do to the column.
    static let maxDomainLength = 64

    /// What is written when an error carries no underlying chain at all.
    ///
    /// A WORD, not an empty string: the field is always present, so a reader
    /// can tell "there was no underlying error" from "the field was dropped".
    static let noUnderlyingToken = "none"

    /// Written when a domain sanitizes away to nothing. Cannot be produced by
    /// any real `NSError`, and says which of the two it is.
    static let unknownDomainToken = "unknown"

    /// The `(domain, code)` of a throw, plus the DEEPEST `(domain, code)` its
    /// underlying chain reaches.
    ///
    /// The deepest rather than the first because the first is the wrapper —
    /// `FoundationModels.LanguageModelError` / `-1` on the field row, which is
    /// the framework saying it did not classify either. The specific condition
    /// is always further in.
    struct Identity: Sendable, Equatable {
        let domain: String
        let code: Int
        /// `nil` when the error carried no `NSUnderlyingErrorKey` /
        /// `NSMultipleUnderlyingErrorsKey` chain — NOT when the chain was empty
        /// of information, which cannot happen: an `NSError` always has a
        /// domain and a code.
        let underlyingDomain: String?
        let underlyingCode: Int?
    }

    /// Read the identity of any `Error`.
    ///
    /// `error as NSError` is total in Swift — a native Swift error bridges to
    /// its reflected type name and its case index — so this never fails and
    /// never guesses. For the field row it reads
    /// `FoundationModels.LanguageModelError` / `-1` with
    /// `ModelManagerServices.ModelManagerError` / `1001` underneath.
    static func identity(of error: Error) -> Identity {
        let outer = error as NSError
        var deepest: NSError?
        var cursor = outer
        var depth = 0
        while depth < maxUnderlyingDepth, let next = underlying(of: cursor) {
            deepest = next
            cursor = next
            depth += 1
        }
        return Identity(
            domain: outer.domain,
            code: outer.code,
            underlyingDomain: deepest?.domain,
            underlyingCode: deepest?.code
        )
    }

    /// The durable cause. `<prefix>-<phase>(domain=…,code=…,under=…)`.
    ///
    /// Shaped after `playhead-ezmv`'s `metadataStall-refused(peers=0)`: the
    /// greppable token is a prefix nothing else answers to, and the
    /// parenthetical carries what a reader needs NEXT without changing what the
    /// grep counts.
    static func deferReason(for error: Error, phase: BackfillJobPhase) -> String {
        let identity = identity(of: error)
        let under: String
        if let underlyingDomain = identity.underlyingDomain, let underlyingCode = identity.underlyingCode {
            under = "\(sanitize(underlyingDomain))/\(underlyingCode)"
        } else {
            under = noUnderlyingToken
        }
        return "\(causePrefix)-\(phase.rawValue)"
            + "(domain=\(sanitize(identity.domain)),code=\(identity.code),under=\(under))"
    }

    /// One level of the underlying chain.
    ///
    /// `NSUnderlyingErrorKey` first, then the FIRST of
    /// `NSMultipleUnderlyingErrorsKey` — which is the key a `CustomNSError`
    /// with an `underlyingErrors` array bridges through, and
    /// `ModelManagerError` has exactly such a property. Taking the first of the
    /// multiple is a deliberate narrowing: the token records one path through
    /// the chain, not the whole tree, and it says so by carrying a single
    /// `under=`.
    private static func underlying(of error: NSError) -> NSError? {
        if let single = error.userInfo[NSUnderlyingErrorKey] as? NSError {
            return single
        }
        if let multiple = error.userInfo[NSMultipleUnderlyingErrorsKey] as? [Error] {
            return multiple.first.map { $0 as NSError }
        }
        return nil
    }

    /// Make a domain safe to live in a space-separated `key=value` record.
    ///
    /// Whitespace becomes `_` and the four characters that would end a field or
    /// a parenthetical are dropped, so a reader parsing the token cannot be
    /// fooled by a domain that contains them. Capped at ``maxDomainLength``.
    static func sanitize(_ domain: String) -> String {
        let collapsed = domain
            .split(whereSeparator: { $0.isWhitespace || $0.isNewline })
            .joined(separator: "_")
            .replacingOccurrences(of: "(", with: "")
            .replacingOccurrences(of: ")", with: "")
            .replacingOccurrences(of: ",", with: "")
            .replacingOccurrences(of: "=", with: "")
        let trimmed = String(collapsed.prefix(maxDomainLength))
        return trimmed.isEmpty ? unknownDomainToken : trimmed
    }
}
