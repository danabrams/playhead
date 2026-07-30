// AnalysisStoreHealthJournal.swift
// Durable, SQLite-free record of whether the analysis database opened.
//
// Scope: playhead-wvdz (a failed migration must never silently destroy
//        the user's analysis database).
//
// ----- Local-only, by decision -----
//
// Nothing in this file transmits. It writes one JSON document to the
// device and the diagnostics bundle the user already chooses to send is
// the only way it leaves. Same posture, and the same directory, as
// `StabilityDiagnosticsStore` — see that file's header for the full
// argument.
//
// ----- Storage shape, and why it is not JSONL -----
//
// A single JSON document at
// `Application Support/Diagnostics/analysis-store-health.json`, written
// atomically, stamped `.completeUntilFirstUserAuthentication`.
//
// The crash ring buffer next door is JSONL because it is a LOG: records
// are independent, and a torn tail costs one incident. This is not a
// log. Its most important field is a COUNTER whose whole purpose is to
// be authoritative — "how many launches in a row has this failed" has
// exactly one correct answer, and reconstructing it by replaying an
// append-only file would make a torn tail change the answer. A single
// atomically-replaced document has one value or the previous one, never
// a partial sum.
//
// The bounded failure list inside the document gives back the forensic
// history JSONL would have provided, capped so the file stays small.
//
// ----- The refuse-to-write rule -----
//
// `load()` cannot distinguish "no document yet" from "document present
// but unreadable" by its return value alone, and on iOS the second is
// real: a `BGProcessingTask` can wake the app after a reboot with the
// container still under Data Protection. If an unreadable read were
// treated as an empty state, the very next write would replace a genuine
// escalation history with a fresh one — and the counter that decides
// whether to ask the listener anything would silently reset forever.
//
// So every mutation goes through `mutate(_:)`, which refuses to write
// when the file exists but cannot be read. The consequence is honest and
// in the safe direction: during a protected-data window the journal
// stops advancing, and it resumes from the real value once the device is
// unlocked. It never invents history and never loses it.

import Foundation
import OSLog

actor AnalysisStoreHealthJournal {

    // MARK: - Configuration

    /// Directory under `Application Support/`. Deliberately the same
    /// `Diagnostics` directory the crash ring buffer uses — the two are
    /// siblings and support tooling looks in one place.
    static let directoryName = StabilityDiagnosticsStore.directoryName

    static let filename = "analysis-store-health.json"

    /// Consecutive escalation-counting failures before the app stops
    /// retrying quietly and asks the listener what to do.
    ///
    /// Three, because each retry costs a whole launch. One failure is
    /// very often a one-off (a locked container that unlocked a moment
    /// later, a write that lost a race with a backup, a device that ran
    /// out of disk and then did not). Three consecutive failures across
    /// three separate launches is no longer a one-off, and waiting
    /// longer only means more launches during which analysis is quietly
    /// unavailable and the listener has not been told why.
    static let failuresBeforeAskingListener = 3

    /// Production handle. Constructed lazily; touches no file until
    /// something is read or written, so referencing it from the launch
    /// path costs nothing.
    ///
    /// Tests MUST NOT use this — every test constructs its own instance
    /// against a temp directory, the same isolation rule
    /// `StabilityDiagnosticsStore` follows.
    static let shared = AnalysisStoreHealthJournal()

    // MARK: - State

    private let explicitDirectory: URL?
    private let fileManager: FileManager
    private let logger = Logger(subsystem: "com.playhead", category: "AnalysisStoreHealth")
    private var resolvedDirectory: URL?

    // MARK: - Init

    /// - Parameter directory: when nil, resolves to
    ///   `Application Support/Diagnostics/` on first use. Tests pass a
    ///   unique temp directory.
    init(directory: URL? = nil, fileManager: FileManager = .default) {
        self.explicitDirectory = directory
        self.fileManager = fileManager
    }

    // MARK: - Read

    /// The persisted state, or ``AnalysisStoreHealthState/healthy`` when
    /// no document exists yet.
    ///
    /// An unreadable document also yields `.healthy`. That is the
    /// correct read-side answer — there is nothing else to return — and
    /// it is safe because the write side (`mutate`) independently
    /// refuses to overwrite an unreadable document, so the real history
    /// is not lost by the optimistic read.
    func load() -> AnalysisStoreHealthState {
        guard let url = fileURL(), let data = try? Data(contentsOf: url) else {
            return .healthy
        }
        guard let state = try? Self.decoder.decode(AnalysisStoreHealthState.self, from: data) else {
            // `AnalysisStoreHealthState.init(from:)` degrades every
            // individual field rather than throwing, so reaching here
            // means the bytes are not JSON at all. The next `mutate`
            // will overwrite them. That is deliberate: refusing forever
            // would wedge the escalation counter permanently, which
            // costs exactly as much as the reset does (the listener is
            // never asked either way) while also never self-healing.
            logger.fault("analysis-store-health document present but undecodable — it will be replaced")
            return .healthy
        }
        return state
    }

    // MARK: - Write

    /// Record a failed open/migrate and return the resulting state.
    ///
    /// - Parameters:
    ///   - error: the thrown error. `AnalysisStoreError` is classified
    ///     precisely; anything else lands in `.unknown`.
    ///   - now: injected for tests.
    @discardableResult
    func recordFailure(
        error: Error,
        now: Date = Date()
    ) -> AnalysisStoreHealthState {
        let (phase, failureClass, detail) = Self.classify(error)
        return mutate { current in
            // Only classes that count advance the counter. `accessDenied`
            // is excluded because on iOS it is usually Data Protection
            // doing its job, not a broken database — see
            // `AnalysisStoreFailureClass.countsTowardEscalation`.
            //
            // The exemption expires. `SQLITE_CANTOPEN` is also what a
            // permanently broken container reports, and a failure run
            // that has already lasted longer than Data Protection ever
            // could is no longer explained by it. Without this the app
            // would sit with analysis dead and never ask the listener
            // anything — the same silent-forever failure this bead
            // exists to remove, wearing a different hat.
            let failureRunAge = now.timeIntervalSince(current.firstFailureAt ?? now)
            let counts = failureClass.countsTowardEscalation
                || failureRunAge >= AnalysisStoreHealthState.accessDeniedGracePeriod
            let newCount = counts ? current.consecutiveFailureCount + 1 : current.consecutiveFailureCount
            let record = AnalysisStoreFailureRecord(
                occurredAt: now,
                phase: phase,
                failureClass: failureClass,
                // The counter as it stands AFTER this failure, whether or
                // not this one advanced it. Recording 0 for a
                // non-counting failure made the record list contradict
                // the top-level counter during a mixed run (three
                // counting failures then one in-grace `accessDenied`
                // appended a `0` beside a top-level `3`), which broke the
                // one thing the field is for: reconstructing the
                // escalation history from the list alone.
                consecutiveFailureCount: newCount,
                expectedSchemaVersion: AnalysisStore.currentSchemaVersion,
                detail: detail
            )
            return AnalysisStoreHealthState(
                status: newCount >= Self.failuresBeforeAskingListener
                    ? .awaitingUserDecision
                    : .retrying,
                consecutiveFailureCount: newCount,
                firstFailureAt: current.firstFailureAt ?? now,
                lastFailureAt: now,
                lastSuccessAt: current.lastSuccessAt,
                recentFailures: current.recentFailures + [record],
                quarantines: current.quarantines
            )
        }
    }

    /// Record a successful open/migrate. Clears the escalation counter
    /// and the failure window, but KEEPS the failure records and the
    /// quarantine list: a store that failed twice and then recovered is
    /// exactly the history a support engineer needs, and a quarantine
    /// that still occupies disk must stay reportable after the fresh
    /// store starts working.
    @discardableResult
    func recordSuccess(now: Date = Date()) -> AnalysisStoreHealthState {
        mutate { current in
            AnalysisStoreHealthState(
                status: .healthy,
                consecutiveFailureCount: 0,
                firstFailureAt: nil,
                lastFailureAt: current.lastFailureAt,
                lastSuccessAt: now,
                recentFailures: current.recentFailures,
                quarantines: current.quarantines
            )
        }
    }

    /// Clear the escalation counter because the LISTENER asked to try
    /// again. Distinct from `recordSuccess` — nothing has succeeded yet;
    /// the app is merely being told to stop asking and retry from zero.
    ///
    /// `firstFailureAt` IS DELIBERATELY PRESERVED. It anchors the
    /// grace-period clock in `recordFailure`, and that clock measures how
    /// long the failure run has lasted — a question a retry does not
    /// answer. Clearing it here made the non-destructive option reset the
    /// gate on the destructive one, which produced a loop with no exit:
    ///
    ///   a permanently unreadable container reports `accessDenied`, so
    ///   the listener waits out the whole grace period before the choice
    ///   appears; they then do the obviously correct thing and tap "Try
    ///   again" first; it fails; the counter AND the clock reset, the
    ///   rows disappear, and the offer is another full grace period away.
    ///   Repeat forever.
    ///
    /// Only a SUCCESS clears it, because a success is the only evidence
    /// the failure run actually ended.
    @discardableResult
    func recordListenerRequestedRetry() -> AnalysisStoreHealthState {
        mutate { current in
            AnalysisStoreHealthState(
                status: .retrying,
                consecutiveFailureCount: 0,
                firstFailureAt: current.firstFailureAt,
                lastFailureAt: current.lastFailureAt,
                lastSuccessAt: current.lastSuccessAt,
                recentFailures: current.recentFailures,
                quarantines: current.quarantines
            )
        }
    }

    /// Append a quarantine record. Only ever called after an explicit
    /// listener choice — nothing in the app produces one on its own.
    @discardableResult
    func recordQuarantine(_ record: AnalysisStoreQuarantineRecord) -> AnalysisStoreHealthState {
        mutate { current in
            AnalysisStoreHealthState(
                status: .retrying,
                consecutiveFailureCount: 0,
                firstFailureAt: nil,
                lastFailureAt: current.lastFailureAt,
                lastSuccessAt: current.lastSuccessAt,
                recentFailures: current.recentFailures,
                quarantines: current.quarantines + [record]
            )
        }
    }

    // MARK: - Classification

    /// Map a thrown error onto the closed `(phase, class, detail)`
    /// vocabulary. `nonisolated` and pure so tests can exercise it
    /// without an instance.
    nonisolated static func classify(
        _ error: Error
    ) -> (AnalysisStoreFailurePhase, AnalysisStoreFailureClass, String?) {
        guard let storeError = error as? AnalysisStoreError else {
            return (.unknown, .unknown, nil)
        }
        switch storeError {
        case .openFailed(let code, let message):
            // Prefer the message when it is decisive: `sqlite3_open_v2`
            // reports SQLITE_CANTOPEN for several distinct conditions,
            // and the message is what separates "not a database" from
            // "could not open the file".
            let byMessage = AnalysisStoreFailureClass.classify(message: message)
            let resolved = byMessage == .unknown
                ? AnalysisStoreFailureClass.classify(openResultCode: code)
                : byMessage
            return (.open, resolved, message)
        case .migrationFailed(let message):
            return (.migration, .classify(message: message), message)
        case .queryFailed(let message):
            return (.query, .classify(message: message), message)
        case .insertFailed(let message):
            return (.write, .classify(message: message), message)
        default:
            // Every other case is a domain-logic error raised by a
            // specific query, not an open/migrate failure. Recorded
            // rather than mapped, so a future caller that funnels one
            // here shows up as `.unknown` instead of being silently
            // filed under a class it does not belong to.
            return (.unknown, .unknown, nil)
        }
    }

    // MARK: - Paths

    private func directoryURL() -> URL? {
        if let resolvedDirectory { return resolvedDirectory }
        let url: URL?
        if let explicitDirectory {
            url = explicitDirectory
        } else {
            url = try? fileManager.url(
                for: .applicationSupportDirectory,
                in: .userDomainMask,
                appropriateFor: nil,
                create: false
            ).appendingPathComponent(Self.directoryName, isDirectory: true)
        }
        resolvedDirectory = url
        return url
    }

    private func fileURL() -> URL? {
        directoryURL()?.appendingPathComponent(Self.filename, isDirectory: false)
    }

    // MARK: - Serialisation

    private static let decoder: JSONDecoder = {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return decoder
    }()

    private static let encoder: JSONEncoder = {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        encoder.dateEncodingStrategy = .iso8601
        return encoder
    }()

    /// Read-modify-write with the refuse-to-clobber rule described in
    /// the file header. Returns the state the caller should act on,
    /// which is the transformed state whether or not the write landed —
    /// a failed write must not make the caller believe the failure did
    /// not happen.
    private func mutate(
        _ transform: (AnalysisStoreHealthState) -> AnalysisStoreHealthState
    ) -> AnalysisStoreHealthState {
        // A read failure is NOT an empty document. Without this guard a
        // single unreadable read — the file still under Data Protection
        // during a locked background launch — would replace the real
        // escalation history with a one-entry document, permanently
        // resetting the counter that decides whether the listener is
        // ever asked.
        if let url = fileURL(),
           fileManager.fileExists(atPath: url.path),
           (try? Data(contentsOf: url)) == nil {
            logger.error("analysis-store-health document unreadable — skipping write rather than clobbering it")
            return transform(.healthy)
        }

        let next = transform(load())
        write(next)
        return next
    }

    private func write(_ state: AnalysisStoreHealthState) {
        guard let directory = directoryURL(), let url = fileURL() else { return }
        guard let data = try? Self.encoder.encode(state) else {
            logger.error("analysis-store-health document failed to encode")
            return
        }
        do {
            try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
            try data.write(to: url, options: [.atomic])
            // Re-apply protection after every write: `.atomic` replaces
            // the inode, so an attribute set once at create time would
            // silently stop applying on the next write.
            try fileManager.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: url.path
            )
        } catch {
            // FAULT, not error: this is the one failure in this file with
            // a consequence beyond itself. The escalation counter lives
            // only in this document, so a write that keeps failing pins
            // it — `load()` returns `.healthy` on the next launch and the
            // count never accumulates, which means the listener is never
            // asked. The natural trigger is a full disk, and `diskFull`
            // is itself a first-class counting failure class, so the two
            // correlate exactly when it matters most.
            //
            // Not repaired here. An in-memory fallback counter would not
            // help — `recordFailure` runs once per launch, so a
            // process-local count never reaches the threshold either —
            // and anything durable enough to help is a second store with
            // the same failure mode. What this fault buys is that the
            // condition is visible in the device log instead of silent.
            logger.fault(
                "analysis-store-health write FAILED — the escalation counter cannot advance while this persists, so the listener will not be asked: \(error.localizedDescription, privacy: .public)"
            )
        }
    }
}
