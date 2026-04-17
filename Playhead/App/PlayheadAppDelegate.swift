// PlayheadAppDelegate.swift
// Minimal UIKit-style delegate attached via `@UIApplicationDelegateAdaptor`
// from the pure-SwiftUI `PlayheadApp`. Its job is to relay the
// background URLSession wake callback
// (`application(_:handleEventsForBackgroundURLSession:completionHandler:)`)
// into the `DownloadManager` so pending transfers can finish reporting
// their state before the app is suspended again, AND to kick off the
// cold-launch force-quit manual-resume scan within the 2 s SLA
// required by playhead-hyht.
//
// playhead-24cm + playhead-hyht.

import Foundation
import OSLog
import UIKit

@MainActor
final class PlayheadAppDelegate: NSObject, UIApplicationDelegate {

    private let logger = Logger(subsystem: "com.playhead", category: "AppDelegate")

    // MARK: - State

    /// Pending completion handlers keyed by background session identifier.
    /// The OS hands us a handler when it wakes the app to relay URLSession
    /// events; we store it here and invoke it once the session's delegate
    /// reports `urlSessionDidFinishEvents(forBackgroundURLSession:)`. The
    /// dictionary layout guarantees at most one outstanding handler per
    /// identifier — if the OS wakes the app twice for the same session
    /// before the first wave drains, the newer handler replaces the older
    /// one, which matches Apple's documented contract (the older handler
    /// is no longer valid after replacement).
    private var pendingHandlers: [String: () -> Void] = [:]

    // MARK: - Public API (callable from tests and DownloadManager)

    /// Test hook: number of outstanding handlers currently stored.
    var pendingBackgroundCompletionHandlerCount: Int {
        pendingHandlers.count
    }

    /// Stores a completion handler for the given background session
    /// identifier. Replaces any existing handler for that identifier.
    func storePendingBackgroundCompletionHandler(
        _ handler: @escaping () -> Void,
        forIdentifier identifier: String
    ) {
        pendingHandlers[identifier] = handler
    }

    /// Invokes and removes the pending handler for `identifier`. Safe to
    /// call repeatedly — the second call is a no-op, which is exactly
    /// the no-leak / no-double-fire invariant the bead requires.
    func invokePendingBackgroundCompletionHandler(forIdentifier identifier: String) {
        guard let handler = pendingHandlers.removeValue(forKey: identifier) else {
            return
        }
        handler()
    }

    // MARK: - UIApplicationDelegate

    /// Kicks off the force-quit manual-resume scan (playhead-hyht) on
    /// cold launch. The scan must complete within 2 s of this callback
    /// per the bead spec; we dispatch it on the DownloadManager actor
    /// and log if the wall-clock elapsed time exceeds the SLA (we do
    /// NOT block launch on the result — the Activity UI renders the
    /// `paused` state off of persisted WorkJournal rows).
    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?
    ) -> Bool {
        if let manager = DownloadManager.shared {
            let logger = self.logger
            Task {
                let started = ContinuousClock.now
                do {
                    let outcome = try await manager.scanForSuspendedTransfers()
                    let elapsed = ContinuousClock.now - started
                    if elapsed > .seconds(2) {
                        logger.error("scanForSuspendedTransfers exceeded 2s SLA: elapsed=\(String(describing: elapsed), privacy: .public), resumable=\(outcome.resumableTransferIds.count), corrupted=\(outcome.corruptedTransferIds.count)")
                    } else {
                        logger.info("scanForSuspendedTransfers: resumable=\(outcome.resumableTransferIds.count), corrupted=\(outcome.corruptedTransferIds.count), elapsed=\(String(describing: elapsed), privacy: .public)")
                    }
                } catch {
                    logger.error("scanForSuspendedTransfers failed: \(error.localizedDescription, privacy: .public)")
                }
            }
        } else {
            logger.info("scanForSuspendedTransfers skipped — no DownloadManager registered")
        }
        return true
    }

    /// iOS calls this when it has relaunched the app (or brought it back
    /// from suspension) to deliver pending background URLSession events.
    /// We stash the completion handler keyed by `identifier` and ask the
    /// download manager to re-instantiate the matching URLSession so its
    /// delegate callbacks fire. Once the session is drained the
    /// DownloadManager's `urlSessionDidFinishEvents(forBackgroundURLSession:)`
    /// calls back into `invokePendingBackgroundCompletionHandler`.
    func application(
        _ application: UIApplication,
        handleEventsForBackgroundURLSession identifier: String,
        completionHandler: @escaping () -> Void
    ) {
        storePendingBackgroundCompletionHandler(completionHandler, forIdentifier: identifier)

        // Route to the registered DownloadManager, if any. `shared` is
        // `nil` in test contexts that don't install a manager.
        if let manager = DownloadManager.shared {
            Task { await manager.resumeSession(identifier: identifier) }
        }
    }
}
