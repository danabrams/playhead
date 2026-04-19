// UIKitDiagnosticsPresenter.swift
// Production adapter implementing `DiagnosticsExportPresenter` on top of
// `MFMailComposeViewController` (iPhone + mail-capable iPad) with a
// `UIActivityViewController` fallback (iPad without Mail configured or
// `canSendMail() == false` environments).
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Design shape:
//   * The presenter is intentionally thin. Mail-vs-activity selection
//     lives here (rather than in the coordinator) so that the coordinator
//     stays pure orchestration and can be tested without UIKit.
//   * `MailComposeDelegateProxy` bridges the Objective-C delegate
//     callback into a Swift completion handler. The proxy retains itself
//     until the callback fires so it survives composer dismissal even
//     when the host presenter is deallocated mid-flow.
//   * The presenter host is provided via a `() -> UIViewController?` closure
//     rather than a stored weak reference so test doubles can be vended
//     without dragging a real UIWindow graph through the tests.

import Foundation

#if canImport(UIKit) && os(iOS)
import UIKit
#endif

#if canImport(MessageUI) && os(iOS)
import MessageUI
#endif

#if canImport(UIKit) && canImport(MessageUI) && os(iOS)

// MARK: - Presenter

@MainActor
final class UIKitDiagnosticsPresenter: DiagnosticsExportPresenter {

    /// Resolves the host view controller each time the presenter is
    /// invoked. Returning `nil` surfaces a
    /// `DiagnosticsExportError.missingHostViewController` to the caller.
    private let hostProvider: @MainActor () -> UIViewController?

    /// Retains the active delegate proxy for the duration of the mail
    /// flow. Cleared when the completion fires. Optional because
    /// `presentActivityFallback(...)` does not use it.
    private var activeDelegate: MailComposeDelegateProxy?

    init(hostProvider: @escaping @MainActor () -> UIViewController?) {
        self.hostProvider = hostProvider
    }

    // MARK: - DiagnosticsExportPresenter

    func present(
        data: Data,
        filename: String,
        subject: String,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    ) {
        guard let host = hostProvider() else {
            completion(.failure(DiagnosticsExportError.missingHostViewController))
            return
        }

        // Prefer the mail composer when the device can send mail. Applies
        // to both iPhone and a mail-configured iPad. If Mail is not
        // configured (`canSendMail() == false`, e.g. iPad without a
        // signed-in Mail account), fall through to the activity fallback
        // so support still gets an email artifact from the user's other
        // mail client.
        if MFMailComposeViewController.canSendMail() {
            presentMailComposer(
                data: data,
                filename: filename,
                subject: subject,
                host: host,
                completion: completion
            )
            return
        }

        presentActivityFallback(
            data: data,
            filename: filename,
            host: host,
            completion: completion
        )
    }

    // MARK: - Mail composer path

    // The mail composer path writes no disk artifact: the attachment is
    // handed to `MFMailComposeViewController.addAttachmentData(_:mimeType:fileName:)`
    // in-memory (see `DiagnosticsExportService.makeMailComposer`). No
    // temp file is created so no cleanup is required on this branch.
    private func presentMailComposer(
        data: Data,
        filename: String,
        subject: String,
        host: UIViewController,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    ) {
        // The proxy is held both by the composer's `mailComposeDelegate`
        // (weak) and by `self.activeDelegate` (strong). Clearing the
        // strong reference in the callback releases the proxy after the
        // composer dismisses.
        let proxy = MailComposeDelegateProxy { [weak self] result in
            guard let self else {
                completion(.success(result))
                return
            }
            self.activeDelegate = nil
            completion(.success(result))
        }
        self.activeDelegate = proxy

        guard let composer = DiagnosticsExportService.makeMailComposer(
            data: data,
            filename: filename,
            subject: subject,
            delegate: proxy
        ) else {
            // `canSendMail()` said yes a moment ago but composer
            // construction declined — treat as failed so the coordinator
            // preserves the opt-in flag for retry.
            self.activeDelegate = nil
            completion(.success(.failed))
            return
        }
        host.present(composer, animated: true)
    }

    // MARK: - Activity fallback path

    private func presentActivityFallback(
        data: Data,
        filename: String,
        host: UIViewController,
        completion: @escaping @MainActor (Result<DiagnosticsMailComposeResult, Error>) -> Void
    ) {
        // Activity fallback needs a file URL — the presenter writes the
        // bundle into a per-export UUID subdirectory of the system tmp
        // directory. The subdirectory is removed in the activity
        // completion handler regardless of success/cancel/error. A
        // dedicated subdir (rather than a bare file under tmp) makes the
        // cleanup atomic and PII-safe: the diagnostics JSON contains
        // opted-in episode titles + transcript excerpts, and iOS does not
        // reliably reap tmp files — they can persist for days. Owning the
        // write + cleanup in one place keeps the pair symmetric.
        //
        // On any write error surface as a `.failed` result so the opt-in
        // flag persists; the user can retry.
        let subdir: URL
        let fileURL: URL
        do {
            (subdir, fileURL) = try Self.writeBundleToFreshSubdirectory(
                data: data,
                filename: filename
            )
        } catch {
            completion(.success(.failed))
            return
        }

        let activity = DiagnosticsExportService.makeActivityFallback(fileURL: fileURL)
        activity.completionWithItemsHandler = { _, completed, _, _ in
            // `completed == true` when the user successfully routes the
            // artifact; we treat that as `.sent` so the opt-in flag
            // clears. A dismissed sheet (`completed == false`) is
            // `.cancelled`.
            //
            // Clean up the per-export subdirectory before firing the
            // outer completion. `try?` swallows any unlikely removal
            // failure — the worst case is leftover tmp bytes, which is
            // exactly the state we were trying to avoid; we do not want
            // a cleanup failure to mask a successful export.
            let mapped: DiagnosticsMailComposeResult = completed ? .sent : .cancelled
            Task { @MainActor in
                Self.removeSubdirectory(subdir)
                completion(.success(mapped))
            }
        }

        // iPad popover positioning is driven by the host. We attach to
        // the host's view so presentation always lands somewhere
        // reasonable; real placement is the job of playhead-l274.
        if let popover = activity.popoverPresentationController {
            popover.sourceView = host.view
            popover.sourceRect = CGRect(
                x: host.view.bounds.midX,
                y: host.view.bounds.midY,
                width: 0, height: 0
            )
            popover.permittedArrowDirections = []
        }
        host.present(activity, animated: true)
    }

    // MARK: - Temp-file lifecycle (internal for tests)

    /// Creates a fresh UUID-named subdirectory under the system tmp
    /// directory, writes the bundle inside it, and returns both the
    /// subdir URL (for cleanup) and the file URL (for the activity
    /// controller). Exposed `internal static` so tests can drive the
    /// write + cleanup pair without presenting a real
    /// `UIActivityViewController` (simulator-hostile).
    static func writeBundleToFreshSubdirectory(
        data: Data,
        filename: String,
        parentDirectory: URL = FileManager.default.temporaryDirectory,
        subdirectoryName: String = UUID().uuidString
    ) throws -> (subdirectory: URL, fileURL: URL) {
        let subdir = parentDirectory.appendingPathComponent(
            subdirectoryName,
            isDirectory: true
        )
        try FileManager.default.createDirectory(
            at: subdir,
            withIntermediateDirectories: true
        )
        let fileURL = try DiagnosticsExportService.writeBundle(
            data: data,
            filename: filename,
            directory: subdir
        )
        return (subdir, fileURL)
    }

    /// Best-effort removal of the per-export subdirectory. Swallows
    /// errors by design — see `presentActivityFallback` for rationale.
    /// Exposed `internal static` so tests can call directly.
    static func removeSubdirectory(_ url: URL) {
        try? FileManager.default.removeItem(at: url)
    }
}

// MARK: - Mail compose delegate proxy

/// NSObject bridge that adapts the Objective-C
/// `MFMailComposeViewControllerDelegate` callback into a Swift
/// completion handler. Retained by the presenter until the mail
/// composer dismisses.
@MainActor
final class MailComposeDelegateProxy: NSObject, MFMailComposeViewControllerDelegate {

    private let completion: @MainActor (DiagnosticsMailComposeResult) -> Void

    init(completion: @escaping @MainActor (DiagnosticsMailComposeResult) -> Void) {
        self.completion = completion
    }

    nonisolated func mailComposeController(
        _ controller: MFMailComposeViewController,
        didFinishWith result: MFMailComposeResult,
        error: Error?
    ) {
        // Dismiss on the main actor, then fire the completion. Apple
        // guarantees `MFMailComposeViewController` dismissal happens
        // from the main thread, but routing through `Task { @MainActor
        // in ... }` makes the contract explicit and lets the closure
        // hop actor if called from a non-main context. `map(...)` is
        // also main-actor-isolated, so the hop happens first.
        Task { @MainActor [weak controller] in
            let mapped = DiagnosticsExportService.map(result)
            controller?.dismiss(animated: true)
            self.completion(mapped)
        }
    }
}

#endif // canImport(UIKit) && canImport(MessageUI) && os(iOS)
