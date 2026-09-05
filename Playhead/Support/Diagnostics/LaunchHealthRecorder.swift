import Foundation
import OSLog

/// playhead-h9y6: what happened to the launch-path work nobody could see fail.
///
/// `PlayheadRuntime`'s launch task wrapped `downloadManager.bootstrap()` in a
/// catch with an EMPTY body and a comment asserting "downloads will fail but
/// playback still works" — playhead-se2h's exact shape, one call later.
/// Established from source before this was written:
///
///   * `bootstrap()` can throw from exactly two places: creating a cache
///     directory, and `rebuildAccessLog()`. Everything else in it is `try?`.
///   * It is called ONCE per process, inside the launch task, and nothing
///     retries it. A transient failure — disk full at launch, or a directory
///     under `completeUntilFirstUserAuthentication` on a BACKGROUND launch
///     before first unlock — leaves the manager un-bootstrapped for the whole
///     process lifetime. That second case is the sceneless-launch class.
///   * The comment is true for the first throw site and false for the second:
///     when `rebuildAccessLog()` throws, downloads do NOT fail; eviction runs
///     blind against an empty access log.
///   * It was unobservable. No log, no counter, nothing in a bundle.
///
/// This type is the observability half. The retry is a separate decision and
/// is filed with these facts rather than guessed at here.
final class LaunchHealthRecorder: @unchecked Sendable {
    struct Snapshot: Equatable, Sendable {
        let downloadBootstrapFailures: Int
        let lastDownloadBootstrapError: String?
        let lastDownloadBootstrapFailureAt: Date?
    }

    static let shared = LaunchHealthRecorder(defaults: .standard)

    private let defaults: UserDefaults
    private let lock = NSLock()
    private static let countKey = "launchHealth.downloadBootstrapFailures"
    private static let errorKey = "launchHealth.lastDownloadBootstrapError"
    private static let atKey = "launchHealth.lastDownloadBootstrapFailureAt"

    init(defaults: UserDefaults) {
        self.defaults = defaults
    }

    func recordDownloadBootstrapFailure(_ error: Error, at date: Date = Date()) {
        lock.lock(); defer { lock.unlock() }
        let count = defaults.integer(forKey: Self.countKey) + 1
        defaults.set(count, forKey: Self.countKey)
        // The description, not the error: a bundle is read by a person, and an
        // NSError's domain/code pair is what distinguishes "no space" from
        // "not permitted" — which is the whole diagnostic question here.
        defaults.set(String(describing: error), forKey: Self.errorKey)
        defaults.set(date, forKey: Self.atKey)
    }

    func snapshot() -> Snapshot {
        lock.lock(); defer { lock.unlock() }
        return Snapshot(
            downloadBootstrapFailures: defaults.integer(forKey: Self.countKey),
            lastDownloadBootstrapError: defaults.string(forKey: Self.errorKey),
            lastDownloadBootstrapFailureAt: defaults.object(forKey: Self.atKey) as? Date
        )
    }
}
