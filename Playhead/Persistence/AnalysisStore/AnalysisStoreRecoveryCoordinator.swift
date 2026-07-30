// AnalysisStoreRecoveryCoordinator.swift
// What happens when the analysis database will not open.
//
// Scope: playhead-wvdz.
//
// ----- What this replaces -----
//
// `PlayheadRuntime` used to answer a thrown `AnalysisStore.migrate()`
// with:
//
//     try? FileManager.default.removeItem(at: AnalysisStore.defaultDirectory())
//
// and then retry. The retry succeeded BECAUSE the directory was now
// empty. Every transcript chunk, ad window, semantic scan result,
// fingerprint, learned threshold — and every correction the listener had
// made by hand — was destroyed, with nothing surfaced and nothing logged.
// On the product owner's own device 61 of 195 ad windows carry
// `detectorVersion = 'userCorrection'`: a third of them, each produced by
// a human tapping a button, and there is no cloud copy of any of it.
//
// ----- The decision this implements -----
//
// Dan, 2026-07-29, verbatim: "retry then surface to the user and let them
// decide." Three settled points, in the order they happen at runtime:
//
//   1. A FIRST FAILURE NEVER DESTROYS ANYTHING. Most migration failures
//      are transient or are a bug in one rung, and both resolve by
//      trying again. `AnalysisStore.runSchemaMigration` already wraps its
//      whole ladder in `BEGIN IMMEDIATE … COMMIT` with `ROLLBACK` on
//      error, so a thrown rung leaves `_meta.schema_version` exactly
//      where it was — the retry-next-launch property is already true at
//      the storage layer and was only ever defeated by the delete above.
//      This type's contribution is to stop defeating it.
//
//   2. WHEN RETRYING IS NOT ENOUGH, ASK. After
//      `AnalysisStoreHealthJournal.failuresBeforeAskingListener`
//      consecutive counting failures the state becomes
//      `awaitingUserDecision` and the app stops deciding.
//
//   3. THE APP NEVER CHOOSES DESTRUCTION. `quarantineAndRebuild` is the
//      only path that moves the store, it is only ever called from an
//      explicit listener action, and it MOVES rather than deletes — so
//      even the chosen path is reversible.
//
// ----- Launch is never blocked -----
//
// Every entry point returns an outcome; none of them throw at the
// launch site and none of them wait for a person. A failed open disables
// the pre-analysis pipeline for that launch and nothing else:
// `PlaybackService` is constructed in `PlayheadRuntime.init` before the
// deferred Task that calls this, and the play path loads and starts the
// transport before it touches the store. Someone who opens the app to
// play a podcast can play a podcast.
//
// ----- The property the delete was protecting -----
//
// The delete existed for a genuinely corrupt, unopenable database: with
// no way to get a working store, the pre-analysis pipeline would be dead
// forever. That property is preserved, by a different route. A corrupt
// database still fails every launch, still costs nothing but the
// analysis features, and after the third failure the listener is offered
// "start fresh" — which quarantines and rebuilds, giving exactly the
// working store the delete produced. The difference is that a person
// chose it and the old bytes are still on the device.

import Foundation
import OSLog

/// What a launch-time open produced. Total: there is no error case,
/// because a failed open is not an error the launch path can act on — it
/// is a state the app runs in.
enum AnalysisStoreLaunchOutcome: Sendable, Equatable {

    /// The store is open and the pre-analysis pipeline may start.
    case opened

    /// The open failed and the app will simply try again next launch.
    /// Analysis is unavailable this launch; playback is unaffected.
    case willRetryOnNextLaunch(attempt: Int)

    /// Retries are exhausted. The app is waiting for the listener and
    /// will not act on its own. Analysis stays unavailable; playback is
    /// still unaffected.
    case awaitingListenerDecision(attempt: Int)

    /// Whether the pre-analysis pipeline may start.
    var isOpen: Bool { self == .opened }
}

/// Raised only by the listener-chosen recovery path, where a caller
/// genuinely needs to know whether the move happened.
enum AnalysisStoreRecoveryError: Error, Equatable {
    /// The store directory could not be moved aside. The original is
    /// untouched — this is reported rather than escalated precisely so
    /// that a failed quarantine never degrades into a delete.
    case quarantineFailed(String)
}

actor AnalysisStoreRecoveryCoordinator {

    // MARK: - Configuration

    /// Prefix for a quarantined store directory, a SIBLING of the live
    /// one. A sibling rather than a child, because a child would be
    /// moved into itself.
    static let quarantineDirectoryPrefix = "AnalysisStore-quarantined-"

    // MARK: - State

    private let journal: AnalysisStoreHealthJournal
    private let storeDirectory: @Sendable () -> URL
    private let fileManager: FileManager
    private let logger = Logger(subsystem: "com.playhead", category: "AnalysisStoreRecovery")

    // MARK: - Init

    /// - Parameters:
    ///   - journal: the durable failure record. Production passes
    ///     `AnalysisStoreHealthJournal.shared`.
    ///   - storeDirectory: resolves the live store directory. A closure
    ///     rather than a `URL` so tests can point at a temp directory
    ///     without `AnalysisStore.defaultDirectory()`'s container
    ///     lookup, and so production stays a single source of truth.
    init(
        journal: AnalysisStoreHealthJournal = .shared,
        storeDirectory: @escaping @Sendable () -> URL = { AnalysisStore.defaultDirectory() },
        fileManager: FileManager = .default
    ) {
        self.journal = journal
        self.storeDirectory = storeDirectory
        self.fileManager = fileManager
    }

    // MARK: - Launch

    /// Run the launch-time open of `store` and record what happened.
    ///
    /// The entry point production uses. It exists alongside the closure
    /// form below because the launch site should not have to spell out
    /// what "open the analysis store" means — and because a `try` there
    /// would sit inside `PlayheadRuntime`'s bootstrap `Task`, where an
    /// unhandled throw is a real hazard rather than a stylistic one.
    func openAtLaunch(
        _ store: AnalysisStore,
        now: Date = Date()
    ) async -> AnalysisStoreLaunchOutcome {
        await openAtLaunch({ try await store.migrate() }, now: now)
    }

    /// Run the launch-time open and record what happened.
    ///
    /// NOTHING HERE DELETES OR MOVES ANYTHING, on any branch. That is
    /// the whole point of the type, and it is the invariant
    /// `AnalysisStoreRecoveryCoordinatorTests` exists to pin.
    ///
    /// - Parameter migrate: the open. Production passes
    ///   `{ try await analysisStore.migrate() }`.
    func openAtLaunch(
        _ migrate: @Sendable () async throws -> Void,
        now: Date = Date()
    ) async -> AnalysisStoreLaunchOutcome {
        do {
            try await migrate()
            await journal.recordSuccess(now: now)
            return .opened
        } catch {
            let state = await journal.recordFailure(error: error, now: now)
            switch state.status {
            case .awaitingUserDecision:
                logger.fault(
                    """
                    Analysis store could not be opened after \
                    \(state.consecutiveFailureCount, privacy: .public) consecutive attempts — \
                    the store is INTACT and untouched, analysis is unavailable this launch, \
                    playback is unaffected, and the listener is being asked what to do: \
                    \(error.localizedDescription, privacy: .public)
                    """
                )
                return .awaitingListenerDecision(attempt: state.consecutiveFailureCount)
            case .retrying, .healthy:
                logger.warning(
                    """
                    Analysis store open failed (attempt \
                    \(state.consecutiveFailureCount, privacy: .public)) — the store is INTACT, \
                    the schema version is unchanged, and the next launch retries: \
                    \(error.localizedDescription, privacy: .public)
                    """
                )
                return .willRetryOnNextLaunch(attempt: state.consecutiveFailureCount)
            }
        }
    }

    /// The current durable state, for a surface that wants to show it.
    func currentState() async -> AnalysisStoreHealthState {
        await journal.load()
    }

    // MARK: - Listener-chosen recovery

    /// "Try again." Clears the escalation counter and attempts the open
    /// immediately, so the listener sees an answer now rather than on a
    /// relaunch.
    ///
    /// Non-destructive in every branch, including failure.
    func retryAtListenerRequest(
        _ store: AnalysisStore,
        now: Date = Date()
    ) async -> AnalysisStoreLaunchOutcome {
        await retryAtListenerRequest({ try await store.migrate() }, now: now)
    }

    /// "Start fresh." Moves the live store aside and rebuilds an empty
    /// one. See the closure form below for the full contract.
    ///
    /// THIS OVERLOAD CARRIES A GUARD THE CLOSURE FORM CANNOT, and it is
    /// the reason production must never call the closure form directly.
    ///
    /// The listener reaches this from a Settings row that was rendered
    /// from a snapshot of the health document. That snapshot can be
    /// minutes stale, and the store can open successfully in the
    /// meantime — the launch-time attempt may still have been in flight
    /// when the row appeared, and separately EVERY `AnalysisStore`
    /// method re-enters `ensureOpen()` after a failure, so any other
    /// caller can bring the store up mid-session without anything
    /// writing to the journal.
    ///
    /// Moving the directory in that window is the bad outcome: POSIX
    /// `rename` leaves open descriptors valid, so the live handle would
    /// keep serving the MOVED database; `migrate()` would then
    /// short-circuit on `didOpen == true` and report success; and every
    /// write for the rest of the session would land in the quarantine,
    /// to be stranded when the next launch mints an empty store at the
    /// live path. Nothing is destroyed, but the listener would be told
    /// "started fresh" while using the old data.
    ///
    /// So: if the store is open, there is nothing wrong to recover from
    /// and this is a no-op that reports the truth. Checked as late as
    /// possible, immediately before the move.
    @discardableResult
    func quarantineAndRebuild(
        _ store: AnalysisStore,
        now: Date = Date()
    ) async throws -> AnalysisStoreLaunchOutcome {
        if await store.isOpen {
            logger.notice(
                "Start-fresh declined: the analysis store is open, so the request was made against a stale view. Nothing moved."
            )
            await journal.recordSuccess(now: now)
            return .opened
        }
        return try await quarantineAndRebuild({ try await store.migrate() }, now: now)
    }

    func retryAtListenerRequest(
        _ migrate: @Sendable () async throws -> Void,
        now: Date = Date()
    ) async -> AnalysisStoreLaunchOutcome {
        await journal.recordListenerRequestedRetry()
        return await openAtLaunch(migrate, now: now)
    }

    /// "Start fresh." THE ONLY PATH THAT TOUCHES THE STORE DIRECTORY,
    /// and it MOVES it.
    ///
    /// The listener is choosing to stop using the old data, not to
    /// destroy it. Moving rather than deleting costs disk until they
    /// clear it and makes the choice reversible — which matters because
    /// the most expensive rows in that database were produced by a human
    /// one tap at a time and there is no cloud copy.
    ///
    /// Ordering is load-bearing: the move happens FIRST and the retry
    /// second, and a failed move throws WITHOUT falling back to anything.
    /// A quarantine that degrades into a delete when the disk is full
    /// would reintroduce the exact bug this bead exists to remove.
    ///
    /// - Returns: the outcome of the open against the fresh directory.
    ///   A store that still will not open on an empty directory is a
    ///   genuinely broken container rather than a data problem, and it
    ///   is reported as an ordinary failure rather than retried further.
    @discardableResult
    func quarantineAndRebuild(
        _ migrate: @Sendable () async throws -> Void,
        now: Date = Date()
    ) async throws -> AnalysisStoreLaunchOutcome {
        let live = storeDirectory()

        // Nothing to move. A store directory that does not exist is not
        // an error — `sqlite3_open_v2` creates it — so proceed straight
        // to the open rather than reporting a failure the listener
        // cannot act on.
        guard fileManager.fileExists(atPath: live.path) else {
            return await openAtLaunch(migrate, now: now)
        }

        let byteCount = directoryByteCount(live)
        let destination = quarantineDestination(for: live, now: now)

        do {
            try fileManager.moveItem(at: live, to: destination)
        } catch {
            // Deliberately NOT followed by a delete, a retry against the
            // live directory, or any other fallback. The listener asked
            // to set the old store aside; if that cannot be done, the
            // honest outcome is to say so and change nothing.
            logger.error(
                "Analysis store quarantine failed; the store is UNCHANGED: \(error.localizedDescription, privacy: .public)"
            )
            throw AnalysisStoreRecoveryError.quarantineFailed(error.localizedDescription)
        }

        await journal.recordQuarantine(
            AnalysisStoreQuarantineRecord(
                quarantinedAt: now,
                // Name only — the absolute path embeds the install UUID
                // and the user's home directory.
                directoryName: destination.lastPathComponent,
                byteCount: byteCount
            )
        )
        logger.notice(
            """
            Analysis store moved aside at the listener's request \
            (\(byteCount, privacy: .public) bytes retained, nothing deleted); \
            rebuilding an empty store.
            """
        )

        return await openAtLaunch(migrate, now: now)
    }

    // MARK: - Paths

    /// A sibling directory whose name is unique per attempt. The
    /// timestamp is seconds-resolution and a UUID suffix guarantees
    /// uniqueness, because a name collision would make `moveItem` throw
    /// and turn a recoverable choice into a reported failure.
    private func quarantineDestination(for live: URL, now: Date) -> URL {
        let stamp = Self.timestampFormatter.string(from: now)
        let suffix = UUID().uuidString.prefix(8)
        return live
            .deletingLastPathComponent()
            .appendingPathComponent(
                "\(Self.quarantineDirectoryPrefix)\(stamp)-\(suffix)",
                isDirectory: true
            )
    }

    private static let timestampFormatter: DateFormatter = {
        let formatter = DateFormatter()
        // Filename-safe and sortable. Fixed locale + UTC so the name does
        // not change shape with the device's region settings.
        formatter.dateFormat = "yyyyMMdd-HHmmss"
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter
    }()

    /// Total size of the tree, so the record can state how much was kept.
    /// Best-effort: an unreadable entry contributes 0 rather than
    /// aborting, because a slightly wrong byte count is not worth
    /// failing a recovery over.
    private func directoryByteCount(_ url: URL) -> Int64 {
        guard let enumerator = fileManager.enumerator(
            at: url,
            includingPropertiesForKeys: [.totalFileAllocatedSizeKey, .fileSizeKey],
            options: []
        ) else {
            return 0
        }
        var total: Int64 = 0
        for case let fileURL as URL in enumerator {
            let values = try? fileURL.resourceValues(
                forKeys: [.totalFileAllocatedSizeKey, .fileSizeKey]
            )
            let size = values?.totalFileAllocatedSize ?? values?.fileSize ?? 0
            total += Int64(size)
        }
        return total
    }
}
