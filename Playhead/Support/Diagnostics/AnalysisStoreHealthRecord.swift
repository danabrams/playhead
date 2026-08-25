// AnalysisStoreHealthRecord.swift
// Wire shape for the health of the analysis database across launches, as
// it is persisted on device and projected into the diagnostics bundle.
//
// Scope: playhead-wvdz (a failed migration must never silently destroy
//        the user's analysis database).
//
// ----- Why this file exists at all -----
//
// Before playhead-wvdz a failed `AnalysisStore.migrate()` was invisible.
// `PlayheadRuntime` logged to Console, deleted the store directory, and
// retried on the empty directory — which succeeded, because the
// directory was empty. Nothing durable recorded that anything had
// happened, so from the next launch onward the app was indistinguishable
// from a fresh install. A fleet-wide wipe caused by one bad migration
// rung would have presented as an unexplained drop in analysis coverage
// with no error signal anywhere.
//
// The observability half is deliberately independent of the recovery
// half and of any UI: a signal that only exists once someone has built a
// screen for it is a signal that does not exist during the incident that
// motivates the screen.
//
// ----- Why it cannot live in the analysis database -----
//
// The event being recorded is "the analysis database could not be
// opened". Persisting it there would be a record that exists only when
// it is not needed. It lives in a JSON document under
// `Application Support/Diagnostics/`, a sibling of the crash/hang ring
// buffer (`StabilityDiagnosticsStore`), which is likewise SQLite-free.
//
// ----- The closed-shape argument -----
//
// Same discipline as `StabilityDiagnosticRecord`: every field is a
// number, a date, a boolean, or an enum rawValue this repo defines. The
// one free-text field — `detail` — carries a SQLite error message and is
// admitted ONLY if it passes `DiagnosticTextSanitizer`'s character
// allowlist, which rejects path separators, quotes, commas and any
// non-ASCII byte. `AnalysisStoreHealthRecordPrivacyTests` walks the
// encoded tree and asserts it.
//
// A SQLite message is a poor smuggling vector to begin with (the strings
// are emitted by SQLite itself, e.g. `FOREIGN KEY constraint failed`),
// but `AnalysisStore.exec` appends `(SQL: …)` to its messages, and that
// SQL is Playhead's own DDL rather than user data. The sanitiser rejects
// it anyway on punctuation, so `failureClass` — a closed vocabulary — is
// the field that actually carries the diagnosis, and `detail` is a
// best-effort extra.

import Foundation

// MARK: - Failure phase

/// WHICH LAYER of `AnalysisStore` raised the error, mapped 1:1 from the
/// `AnalysisStoreError` case. Coarse on purpose: it separates "the file
/// could not be opened at all" from "the file opened and a migration
/// statement failed", which are the two failures with genuinely
/// different remedies.
enum AnalysisStoreFailurePhase: String, Codable, Sendable, Hashable, CaseIterable {
    /// `sqlite3_open_v2` itself refused. The file may be absent,
    /// unreadable under Data Protection, or not a database.
    case open = "open"
    /// The handle opened and a DDL/DML statement inside the migration
    /// transaction failed. The transaction rolls back, so `schema_version`
    /// is unchanged and the next launch retries from the same rung.
    case migration = "migration"
    /// A `prepare` failed outside the migration ladder.
    case query = "query"
    /// A `step` failed outside the migration ladder.
    case write = "write"
    /// Anything that is not an `AnalysisStoreError`.
    case unknown = "unknown"
}

// MARK: - Failure class

/// WHAT WENT WRONG, as a closed vocabulary derived from the SQLite
/// result code (for open failures, which carry one) or from the SQLite
/// error message (for everything else, which does not).
///
/// The message matching is substring-based and therefore inexact by
/// construction. That is acceptable because of where the value is USED:
/// this classification is diagnostic only. It never decides whether to
/// escalate, never decides whether to quarantine, and never decides
/// whether to surface — with the single, documented exception of
/// ``countsTowardEscalation``, whose failure direction is "do not
/// escalate", i.e. never destroy. A misclassification therefore costs
/// triage clarity, never data.
enum AnalysisStoreFailureClass: String, Codable, Sendable, Hashable, CaseIterable {

    /// The file is not a valid SQLite database, or its pages are
    /// damaged. `SQLITE_CORRUPT` / `SQLITE_NOTADB`, or a message
    /// containing `malformed` / `not a database` / `file is encrypted`.
    /// This is the condition the pre-wvdz delete-and-retry existed for.
    case databaseCorrupt = "database_corrupt"

    /// A constraint rejected a migration statement — the failure mode
    /// that produced this bead. playhead-0hi9's v39 rung issued a bare
    /// `DELETE FROM analysis_assets` against rows that a child table
    /// referenced with `ON DELETE RESTRICT`, raising
    /// `FOREIGN KEY constraint failed`. Nothing about the stored data is
    /// damaged; the rung was wrong.
    ///
    /// (The child table is deliberately not named here, not even in
    /// prose. It is one of the local-learning tables, and a privacy
    /// canary in `PlayheadTests/Services/AdDetection/` enumerates every
    /// production file that so much as mentions it, asserting the
    /// consumer set is a closed allowlist. It caught this file on its
    /// first run — correctly: a diagnostics type has no business
    /// appearing in that set, and the canary is worth more than the
    /// extra word.)
    case constraintViolation = "constraint_violation"

    /// A statement referenced a table or column that does not exist on
    /// this database — a ladder ordering bug, or a downgrade.
    case schemaMismatch = "schema_mismatch"

    /// `SQLITE_IOERR` or a `disk I/O error` message.
    case diskIOError = "disk_io_error"

    /// `SQLITE_FULL`, or `database or disk is full`.
    case diskFull = "disk_full"

    /// The database is readable but not writable.
    case readOnly = "read_only"

    /// Another connection holds the write lock. Ordinary contention.
    case contention = "contention"

    /// The file could not be opened for permission reasons.
    /// `SQLITE_CANTOPEN` / `SQLITE_PERM` / `SQLITE_AUTH`.
    ///
    /// ON iOS THIS IS ROUTINELY NOT AN ERROR AT ALL. The store is
    /// stamped `.completeUntilFirstUserAuthentication`, so a
    /// `BGProcessingTask` that wakes the app after a reboot but before
    /// the owner has unlocked once cannot read the file. That is the
    /// expected, healthy behaviour of Data Protection — see
    /// ``countsTowardEscalation``.
    case accessDenied = "access_denied"

    /// Classified as nothing else. Recorded rather than guessed.
    case unknown = "unknown"

    // MARK: Escalation

    /// Whether a failure of this class should advance the
    /// consecutive-failure counter that eventually asks the listener what
    /// to do.
    ///
    /// Only ``accessDenied`` is excluded, and the reason is specific
    /// rather than general caution: on a device that reboots and is not
    /// unlocked promptly, iOS can launch the app for background work
    /// several times in a row with the container still protected. Every
    /// one of those launches fails to open the store through no fault of
    /// the data. Counting them would walk a perfectly healthy library up
    /// to "we could not open your analysis history" after three reboots.
    ///
    /// Excluding a class can only ever DELAY the prompt, never bring it
    /// forward, and the prompt is the only route to any destructive
    /// action. So the failure direction of this predicate is "keep the
    /// data".
    ///
    /// It is NOT an indefinite exemption — see
    /// ``AnalysisStoreHealthState/accessDeniedGracePeriod``. Measured
    /// against a real SQLite build, `unable to open database file`
    /// (`SQLITE_CANTOPEN`) is ALSO what you get for a permanently broken
    /// container — a directory sitting where the database file belongs,
    /// or a permission problem that never resolves. Left as a permanent
    /// exemption, those would leave analysis silently dead forever with
    /// the listener never asked, which is the same class of bug this
    /// whole bead removes. (Genuine corruption is a different code:
    /// a damaged header or a non-database file reports `SQLITE_NOTADB`
    /// with `file is not a database`, which counts immediately.)
    var countsTowardEscalation: Bool {
        self != .accessDenied
    }

    // MARK: Derivation

    /// Classify a raw SQLite result code from an open failure.
    static func classify(openResultCode code: Int32) -> AnalysisStoreFailureClass {
        // Raw values rather than the SQLITE_* macros so this type stays
        // importable from targets that do not link SQLite3, and so the
        // mapping is readable without cross-referencing a header.
        switch code {
        case 11, 26: return .databaseCorrupt   // SQLITE_CORRUPT, SQLITE_NOTADB
        case 10:     return .diskIOError       // SQLITE_IOERR
        case 13:     return .diskFull          // SQLITE_FULL
        case 8:      return .readOnly          // SQLITE_READONLY
        case 5, 6:   return .contention        // SQLITE_BUSY, SQLITE_LOCKED
        case 3, 14, 23: return .accessDenied   // SQLITE_PERM, SQLITE_CANTOPEN, SQLITE_AUTH
        default:     return .unknown
        }
    }

    /// Classify from a SQLite error message.
    ///
    /// Ordered most-specific-first. `constraint failed` is tested before
    /// the corruption family because a `FOREIGN KEY constraint failed`
    /// message is unambiguous and is the exact failure this bead exists
    /// to stop escalating into a wipe.
    static func classify(message: String) -> AnalysisStoreFailureClass {
        let text = message.lowercased()
        if text.contains("constraint failed") { return .constraintViolation }
        if text.contains("malformed")
            || text.contains("not a database")
            || text.contains("file is encrypted") { return .databaseCorrupt }
        if text.contains("no such table")
            || text.contains("no such column")
            || text.contains("has no column")
            || text.contains("duplicate column") { return .schemaMismatch }
        if text.contains("disk i/o error") { return .diskIOError }
        if text.contains("disk is full") { return .diskFull }
        if text.contains("readonly") || text.contains("read-only") { return .readOnly }
        if text.contains("locked") || text.contains("is busy") { return .contention }
        if text.contains("unable to open database")
            || text.contains("not authorized")
            || text.contains("authorization denied") { return .accessDenied }
        return .unknown
    }
}

// MARK: - Lifecycle state

/// Where the store stands as of the most recent launch. This is the
/// single field a support engineer reads first.
enum AnalysisStoreHealthStatus: String, Codable, Sendable, Hashable, CaseIterable {
    /// The last `migrate()` succeeded. The steady state.
    case healthy = "healthy"
    /// The last `migrate()` failed and the app has NOT yet asked the
    /// listener anything. Analysis is unavailable this launch; the next
    /// launch retries against the untouched store.
    case retrying = "retrying"
    /// Retries are exhausted. The app is waiting for the listener to
    /// choose, and will not act on its own. Analysis stays unavailable
    /// and playback is unaffected.
    case awaitingUserDecision = "awaiting_user_decision"
}

// MARK: - Failure record

/// One observed failure to open or migrate the analysis database.
struct AnalysisStoreFailureRecord: Codable, Sendable, Equatable {

    /// Bumped when the meaning of an existing field changes. Additive
    /// fields do not require a bump — readers use `decodeIfPresent`.
    static let currentSchemaVersion = 1

    let schemaVersion: Int
    let occurredAt: Date
    let phase: AnalysisStoreFailurePhase
    let failureClass: AnalysisStoreFailureClass
    /// The value of the consecutive-failure counter AFTER this failure
    /// was recorded — whether or not this particular failure advanced it.
    /// Lets a reader reconstruct the escalation history from the record
    /// list alone, and a run of records whose count does not move is
    /// exactly how a grace-exempt class shows up.
    let consecutiveFailureCount: Int
    /// The schema version this BINARY expects (`AnalysisStore.currentSchemaVersion`).
    /// Recorded rather than the version observed on disk, because when
    /// the database cannot be opened there is no on-disk version to read.
    let expectedSchemaVersion: Int
    /// Sanitised SQLite message, or nil when it failed the allowlist.
    /// Never load-bearing — `failureClass` is.
    let detail: String?

    /// `CaseIterable` so the privacy test can assert the encoded key set
    /// equals the declared one.
    enum CodingKeys: String, CodingKey, CaseIterable {
        case schemaVersion = "schema_version"
        case occurredAt = "occurred_at"
        case phase
        case failureClass = "failure_class"
        case consecutiveFailureCount = "consecutive_failure_count"
        case expectedSchemaVersion = "expected_schema_version"
        case detail
    }

    init(
        occurredAt: Date,
        phase: AnalysisStoreFailurePhase,
        failureClass: AnalysisStoreFailureClass,
        consecutiveFailureCount: Int,
        expectedSchemaVersion: Int,
        detail: String?
    ) {
        self.schemaVersion = Self.currentSchemaVersion
        self.occurredAt = occurredAt
        self.phase = phase
        self.failureClass = failureClass
        self.consecutiveFailureCount = consecutiveFailureCount
        self.expectedSchemaVersion = expectedSchemaVersion
        self.detail = AnalysisStoreHealthDetail.sanitize(detail)
    }

    /// Re-sanitises on decode for the same reason
    /// ``StabilityDiagnosticRecord/init(from:)`` does: the document
    /// outlives app versions, so the exporter must not trust bytes
    /// written by an older binary.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.schemaVersion = try container.decodeIfPresent(Int.self, forKey: .schemaVersion) ?? 0
        self.occurredAt = try container.decode(Date.self, forKey: .occurredAt)
        // `try?`, not `decodeIfPresent`: for a RawRepresentable enum the
        // synthesised decode THROWS on an unrecognised rawValue rather
        // than returning nil, so `decodeIfPresent` would take the whole
        // document down when a newer binary had written a case this one
        // does not know. Downgrades happen (a TestFlight build rolled
        // back), and the document must degrade to `.unknown` rather than
        // become undecodable — see `AnalysisStoreHealthJournal.mutate`,
        // which would otherwise overwrite the real escalation history.
        self.phase = (try? container.decodeIfPresent(
            AnalysisStoreFailurePhase.self, forKey: .phase
        )) ?? .unknown
        self.failureClass = (try? container.decodeIfPresent(
            AnalysisStoreFailureClass.self, forKey: .failureClass
        )) ?? .unknown
        self.consecutiveFailureCount = try container.decodeIfPresent(
            Int.self, forKey: .consecutiveFailureCount
        ) ?? 0
        self.expectedSchemaVersion = try container.decodeIfPresent(
            Int.self, forKey: .expectedSchemaVersion
        ) ?? 0
        self.detail = AnalysisStoreHealthDetail.sanitize(
            try container.decodeIfPresent(String.self, forKey: .detail)
        )
    }
}

// MARK: - Quarantine record

/// A store directory that was MOVED ASIDE rather than deleted, together
/// with enough information to find it again.
///
/// Only ever produced by an explicit listener choice. The app has no
/// path that creates one on its own — see
/// `AnalysisStoreRecoveryCoordinator`.
struct AnalysisStoreQuarantineRecord: Codable, Sendable, Equatable {
    let quarantinedAt: Date
    /// Directory NAME only, never the absolute path — a container path
    /// carries the install UUID and the user's home directory, and the
    /// name is what a support engineer needs to talk the owner through
    /// retrieving it.
    let directoryName: String
    /// Size of the quarantined tree at the moment it was moved. This is
    /// what makes "your data is still there" checkable rather than
    /// asserted.
    let byteCount: Int64

    enum CodingKeys: String, CodingKey, CaseIterable {
        case quarantinedAt = "quarantined_at"
        case directoryName = "directory_name"
        case byteCount = "byte_count"
    }
}

// MARK: - Health document

/// The whole persisted document, and — unchanged — the shape that ships
/// in the diagnostics bundle under `analysis_store_health`.
///
/// One type for both roles on purpose. A separate projection would be a
/// second place for the two to drift, and there is nothing in the
/// persisted shape that must not be exported: it is counters, dates,
/// enum rawValues and one allowlisted string.
struct AnalysisStoreHealthState: Codable, Sendable, Equatable {

    /// Failure records retained. Enough to see a repeating pattern
    /// across several weeks of launches; small enough that the document
    /// stays a few kilobytes.
    static let maxFailureRecords = 20

    /// How long a class that normally does not escalate
    /// (``AnalysisStoreFailureClass/countsTowardEscalation``) may keep
    /// failing before it starts counting anyway.
    ///
    /// The exemption exists for Data Protection, and Data Protection
    /// resolves the first time the owner unlocks the device after a
    /// boot — minutes or hours, not days. A container that has refused
    /// to open for a week straight with no successful open in between is
    /// not locked, it is broken, and leaving it exempt forever would
    /// mean analysis stays silently dead and the listener is never asked
    /// anything. Seven days is long enough to cover a phone left in a
    /// drawer over a holiday and short enough that "silently dead
    /// forever" is not a state this app can reach.
    ///
    /// TWO KNOWN LIMITS, both bounded by the fact that crossing this
    /// threshold only ever ASKS a question — it never destroys anything,
    /// and the answer is still the listener's:
    ///
    ///   * It is wall-clock against a persisted timestamp, so a restore
    ///     from backup or a clock correction can make a fresh failure run
    ///     look old and bring the prompt forward. Reaching it still
    ///     requires three real consecutive failures with no successful
    ///     open in between, so the prompt is legitimate when it appears;
    ///     it just appears sooner than intended.
    ///   * The Data-Protection window is self-defending rather than
    ///     merely exempt, and that is by design, not by luck: this
    ///     document carries `.completeUntilFirstUserAuthentication`, the
    ///     SAME class as the store it describes. During a pre-first-unlock
    ///     background launch the journal is therefore unreadable too, so
    ///     `AnalysisStoreHealthJournal.mutate` refuses to write and the
    ///     failure never reaches the counter at all. Do not "fix" the
    ///     journal's protection class to something weaker — the symmetry
    ///     is load-bearing.
    static let accessDeniedGracePeriod: TimeInterval = 7 * 24 * 60 * 60

    /// Quarantines retained.
    ///
    /// DELIBERATELY UNBOUNDED IN PRACTICE. Every other list here is
    /// capped because it is forensics and old entries stop mattering. A
    /// quarantine record is not forensics — it is the only pointer to a
    /// directory of the listener's data that is still on the device,
    /// possibly tens of megabytes of it. Dropping the oldest record would
    /// leave that directory orphaned: occupying disk, unreportable, and
    /// unfindable by anyone who did not already know the name. Each
    /// record is ~120 bytes and producing one requires an explicit
    /// listener action, so the list is bounded by human effort. The cap
    /// exists only as a runaway guard and is set far above any plausible
    /// count.
    static let maxQuarantineRecords = 200

    let status: AnalysisStoreHealthStatus
    /// Consecutive escalation-counting failures since the last success.
    /// Reset to zero on any successful migrate, and on an explicit
    /// listener "try again".
    let consecutiveFailureCount: Int
    let firstFailureAt: Date?
    let lastFailureAt: Date?
    let lastSuccessAt: Date?
    /// Newest last, capped at ``maxFailureRecords``.
    let recentFailures: [AnalysisStoreFailureRecord]
    /// Newest last, capped at ``maxQuarantineRecords``. Non-empty means
    /// data was moved aside and is still on the device.
    let quarantines: [AnalysisStoreQuarantineRecord]
    /// Which diagnostics-bundle reads THREW while this bundle was being
    /// assembled, by a closed vocabulary
    /// (``AnalysisStoreHealthState/ExportRead``).
    ///
    /// WHY IT SHIPS: the bundle's work-journal read goes through the
    /// analysis database. When that database is the thing that is
    /// broken, an unguarded read took the entire export down — the
    /// artifact that would have explained the failure could not be built
    /// BECAUSE of the failure. Guarding it makes the export survive, and
    /// this field is what stops a survived-but-empty read from looking
    /// like a genuinely empty journal.
    ///
    /// Populated at export time only; always empty in the persisted
    /// document.
    let exportReadFailures: [String]

    /// Closed vocabulary for ``exportReadFailures``. Names the READ, not
    /// the error, so no message text can reach the field.
    enum ExportRead: String, Sendable, Hashable, CaseIterable {
        case workJournal = "work_journal"
        case chapterPhaseEvents = "chapter_phase_events"
        case learnedDeviceProfiles = "learned_device_profiles"
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case status
        case consecutiveFailureCount = "consecutive_failure_count"
        case firstFailureAt = "first_failure_at"
        case lastFailureAt = "last_failure_at"
        case lastSuccessAt = "last_success_at"
        case recentFailures = "recent_failures"
        case quarantines
        case exportReadFailures = "export_read_failures"
    }

    /// A store that has never failed and never succeeded — the shape a
    /// first launch sees, and the shape a bundle carries when the
    /// document does not exist yet.
    static let healthy = AnalysisStoreHealthState(
        status: .healthy,
        consecutiveFailureCount: 0,
        firstFailureAt: nil,
        lastFailureAt: nil,
        lastSuccessAt: nil,
        recentFailures: [],
        quarantines: [],
        exportReadFailures: []
    )

    init(
        status: AnalysisStoreHealthStatus,
        consecutiveFailureCount: Int,
        firstFailureAt: Date?,
        lastFailureAt: Date?,
        lastSuccessAt: Date?,
        recentFailures: [AnalysisStoreFailureRecord],
        quarantines: [AnalysisStoreQuarantineRecord],
        exportReadFailures: [String] = []
    ) {
        self.status = status
        self.consecutiveFailureCount = consecutiveFailureCount
        self.firstFailureAt = firstFailureAt
        self.lastFailureAt = lastFailureAt
        self.lastSuccessAt = lastSuccessAt
        self.recentFailures = Array(recentFailures.suffix(Self.maxFailureRecords))
        self.quarantines = Array(quarantines.suffix(Self.maxQuarantineRecords))
        self.exportReadFailures = exportReadFailures
    }

    /// Tolerant decode: an older document, or one written by a build
    /// that predates a field, must not cost the whole record.
    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        // `try?` for the same reason `AnalysisStoreFailureRecord` uses
        // it: an unrecognised enum rawValue written by a newer binary
        // must degrade this field, not the document.
        self.status = (try? container.decodeIfPresent(
            AnalysisStoreHealthStatus.self, forKey: .status
        )) ?? .healthy
        self.consecutiveFailureCount = try container.decodeIfPresent(
            Int.self, forKey: .consecutiveFailureCount
        ) ?? 0
        self.firstFailureAt = try container.decodeIfPresent(Date.self, forKey: .firstFailureAt)
        self.lastFailureAt = try container.decodeIfPresent(Date.self, forKey: .lastFailureAt)
        self.lastSuccessAt = try container.decodeIfPresent(Date.self, forKey: .lastSuccessAt)
        // A malformed entry costs its LIST, never the document. The
        // counter and the status are what the recovery path reads; the
        // lists are forensics. Losing forensics to keep the counter is
        // the right trade, and losing both to keep neither is not.
        self.recentFailures = Array(
            ((try? container.decodeIfPresent(
                [AnalysisStoreFailureRecord].self, forKey: .recentFailures
            )) ?? []).suffix(Self.maxFailureRecords)
        )
        self.quarantines = Array(
            ((try? container.decodeIfPresent(
                [AnalysisStoreQuarantineRecord].self, forKey: .quarantines
            )) ?? []).suffix(Self.maxQuarantineRecords)
        )
        self.exportReadFailures = try container.decodeIfPresent(
            [String].self, forKey: .exportReadFailures
        ) ?? []
    }

    /// Copy carrying the export-time read failures. Used by the bundle
    /// builder so the persisted document never stores them.
    func withExportReadFailures(_ failures: [ExportRead]) -> AnalysisStoreHealthState {
        AnalysisStoreHealthState(
            status: status,
            consecutiveFailureCount: consecutiveFailureCount,
            firstFailureAt: firstFailureAt,
            lastFailureAt: lastFailureAt,
            lastSuccessAt: lastSuccessAt,
            recentFailures: recentFailures,
            quarantines: quarantines,
            exportReadFailures: failures.map(\.rawValue)
        )
    }
}

// MARK: - Detail sanitiser

/// The one place a SQLite message is narrowed before it is stored.
enum AnalysisStoreHealthDetail {

    /// Longest message we retain. Comfortably fits every SQLite error
    /// string; far too short for prose.
    static let maxLength = 96

    /// Drop `AnalysisStore.exec`'s ` (SQL: …)` suffix and
    /// `SQLiteSystemErrno`'s ` [sqlite3_system_errno…]` clause, trim, and admit
    /// the remainder only if it passes the shared character allowlist.
    ///
    /// Rejection yields `nil` — the field is omitted, never
    /// truncated-and-kept. A truncated leak is still a leak, and
    /// `failureClass` already carries the diagnosis, so there is nothing
    /// to trade away by being strict here.
    ///
    /// THE ERRNO CLAUSE IS STRIPPED BECAUSE IT WOULD OTHERWISE DELETE THIS
    /// FIELD ENTIRELY (playhead-vk68m review). `sqlite3_system_errno`'s
    /// rendering contains `[`, `]` and `=` or `:`, none of which is in
    /// ``DiagnosticTextSanitizer/allowedCharacters`` — so adding it to the
    /// message turned `sanitize` from "admit `unable to open database file`"
    /// into "return nil", silently, for **every** store open and migration
    /// failure, which is the whole population `playhead-s34ux` and
    /// `playhead-vk68m` exist to make readable from a device pull. It is also
    /// ~38 characters against a ``maxLength`` of 96, so even an allowlist-clean
    /// spelling would push a long SQLite message over the cap and drop the
    /// field for a DIFFERENT reason. Stripping is the only remedy that is
    /// provably behaviour-preserving in both directions: the durable record is
    /// byte-identical to what it held before the errno existed, and the errno
    /// lives where it was always going to be read — the log line.
    ///
    /// Note the ORDER. `AnalysisStore.exec` builds
    /// `msg + errnoClause + " (SQL: …)"`, so the SQL marker is found first and
    /// the errno clause is what remains to be removed. Both strips run
    /// unconditionally and the length check happens after, so a message that
    /// fitted before still fits.
    static func sanitize(_ raw: String?) -> String? {
        guard let raw else { return nil }
        var text = raw
        if let sqlMarker = text.range(of: " (SQL: ") {
            text = String(text[text.startIndex..<sqlMarker.lowerBound])
        }
        if let errnoMarker = text.range(of: SQLiteSystemErrno.detailMarker) {
            text = String(text[text.startIndex..<errnoMarker.lowerBound])
        }
        text = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard text.count <= maxLength, DiagnosticTextSanitizer.isAllowed(text) else {
            return nil
        }
        return text
    }
}
