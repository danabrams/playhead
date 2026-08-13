// BackgroundSessionCreationTests.swift
// playhead-gpdb: rails for the LAST unbounded crossing in the download path
// — constructing the background `URLSession` itself — and for the four
// refusal branches bounding it makes reachable.
//
// The defect is one layer ABOVE the two playhead-nsjn and playhead-rouw
// closed. Both of the calls they bounded (`downloadTask(with:)` and
// `allTasks`) are reached THROUGH `URLSession(configuration:
// .background(withIdentifier:))`, which attaches to `nsurlsessiond` and ran
// inline on the actor — i.e. on the fixed-width Swift Concurrency
// cooperative pool — with no bound at all. A construction that parks
// therefore bypasses both of their bounds before either can apply.
//
// EXPOSURE, measured before the fix rather than assumed: construction is
// MEMOIZED (`_sessionsByRole`) and `useDualBackgroundSessions` defaults
// false, so exactly ONE role is ever constructed and the crossing happens
// ONCE per process. That lowers the frequency a great deal and lowers the
// consequence not at all — the one crossing is at the first download, and if
// it parks, the download subsystem is dead for that whole process launch with
// no error anywhere. The worst launch for it is the relaunch iOS makes with
// no scene to deliver background events, which is exactly the launch nobody
// is watching.
//
// Dan's decision (2026-08-13) was BOUND IT AND FAIL THE CALL. A foreground
// fallback — return a plain `URLSession` so the signature could stay
// non-optional — was rejected by name: a session that still calls itself
// `background` while silently having stopped being one takes the overnight
// "wake up to analyzed episodes" behaviour away with nothing saying so.

import Foundation
import Testing
import os
@testable import Playhead

// MARK: - The four refusal branches

@Suite("DownloadManager – the daemon will not open a session (playhead-gpdb)")
struct BackgroundSessionCreationRefusalTests {

    /// Refuses ONLY the construction crossing, by the label production
    /// stamps on it. Everything else — `downloadTask(with:)`, `resume()`,
    /// `allTasks` — is answered normally, so each rail below fails at
    /// construction and nowhere else. A blanket `.neverAnswers` would reach
    /// the same outcomes through the branches nsjn already covers, and could
    /// not tell the two apart.
    private static func creationRefusingIO() -> BackgroundSessionIO {
        BackgroundSessionIO(
            behavior: .refusesCallsLabelled(
                DownloadManager.sessionCreationLabelPrefix
            ),
            timeout: 0.1,
            queueLabel: "gpdb.test.creation-refused.\(UUID().uuidString)"
        )
    }

    /// Collects what the manager wrote to the surface-status invariant
    /// stream. In production this closure is
    /// `SurfaceStatusInvariantLogger.invariantViolated`.
    private final class RecordedViolations: @unchecked Sendable {
        private let entries = OSAllocatedUnfairLock<
            [(code: InvariantViolation.Code, description: String)]
        >(initialState: [])

        var recorder: @Sendable (InvariantViolation.Code, String) -> Void {
            { [entries] code, description in
                entries.withLock { $0.append((code, description)) }
            }
        }

        var refusals: [String] {
            entries.withLock { list in
                list.filter { $0.code == .backgroundSessionCreationRefused }
                    .map(\.description)
            }
        }

        var allCodes: [InvariantViolation.Code] {
            entries.withLock { $0.map(\.code) }
        }
    }

    private static func manager(
        cacheDirectory: URL,
        recording: RecordedViolations
    ) -> DownloadManager {
        DownloadManager(
            cacheDirectory: cacheDirectory,
            sessionIO: creationRefusingIO(),
            invariantRecorder: recording.recorder
        )
    }

    // MARK: backgroundDownload

    /// THE behavioural probe the bead asks for: what does a caller OBSERVE
    /// when the session cannot be created?
    ///
    /// Before this bead the answer was "nothing, forever" — the actor method
    /// never returned, and on the cooperative pool that is the process-wide
    /// deadlock nsjn sampled. After it, the caller returns, the episode is
    /// still retryable, nothing was admitted to the OS, and no attribution
    /// sidecar outlives a transfer that never existed.
    @Test("A download whose session the daemon refuses reports failure and stays retryable")
    func refusedSessionLeavesTheDownloadRetryable() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let recording = RecordedViolations()
        let manager = Self.manager(cacheDirectory: dir, recording: recording)
        try await manager.bootstrap()

        let episodeId = "gpdb-refused-download"
        await manager.backgroundDownload(
            episodeId: episodeId,
            from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
            context: DownloadContext(
                podcastId: "show-gpdb",
                isExplicitDownload: false,
                podcastTitle: "Show",
                episodeTitle: "Episode"
            )
        )

        // Nothing reached the OS. Without this the rail below could pass on
        // an implementation that started a transfer and then lost it.
        #expect(await manager._backgroundDownloadAdmissionCountForTesting() == 0)
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false,
            "a download that was never started must not hold the in-flight slot for the life of the process"
        )
        #expect(
            await manager.loadDownloadAttribution(episodeId: episodeId) == nil,
            "the sidecar describes a transfer; with no transfer it must not outlive the attempt"
        )
        // And no session was memoized, so the next attempt gets a fresh
        // crossing rather than a cached refusal.
        #expect(await manager.instantiatedSessionIdentifiersForTesting().isEmpty)
    }

    /// The refusal has to be RECORDED where a device pull can read it, not
    /// only in `os_log`. This is playhead-oa82's lesson applied one subsystem
    /// over: until this line existed, "the daemon refused a session" and "no
    /// download was requested" left byte-identical evidence — no transfer, no
    /// completion, no row, no error.
    @Test("A refused session is recorded on the surface-status stream, naming the site")
    func refusedSessionIsRecorded() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let recording = RecordedViolations()
        let manager = Self.manager(cacheDirectory: dir, recording: recording)
        try await manager.bootstrap()

        await manager.backgroundDownload(
            episodeId: "gpdb-recorded",
            from: URL(string: "https://cdn.example.com/gpdb-recorded.mp3")!,
            context: DownloadContext(
                podcastId: "show-gpdb",
                isExplicitDownload: false,
                podcastTitle: "Show",
                episodeTitle: "Episode"
            )
        )

        let refusals = recording.refusals
        #expect(
            refusals.count == 1,
            "exactly one refusal, or the record cannot be counted; got \(recording.allCodes)"
        )
        let description = refusals.first ?? ""
        // The SITE is the load-bearing field: the four sites have unrelated
        // consequences and unrelated remedies, and a record that only says
        // "a session was refused" cannot tell a lost download from a
        // background relaunch that will now deliver nothing.
        #expect(
            description.contains(
                "site=\(DownloadManager.BackgroundSessionRequestSite.backgroundDownload.rawValue)"
            ),
            "the record must name which caller was refused: \(description)"
        )
        #expect(
            description.contains("role=\(BackgroundSessionIdentifier.legacy)"),
            "the record must name the session identifier: \(description)"
        )
    }

    // MARK: resumeSuspendedTransfer

    /// The blob is the ONLY copy of the bytes already fetched, and a refused
    /// CONSTRUCTION has exactly as little claim on it as a refused
    /// `downloadTask(withResumeData:)` — nothing owns the transfer either
    /// way. Deleting it would turn a recoverable daemon stall into a full
    /// re-download of an episode the user had nearly finished fetching.
    @Test("A resume whose session the daemon refuses reports .daemonUnavailable and KEEPS the blob")
    func refusedSessionRetainsTheResumeBlob() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let recording = RecordedViolations()
        let manager = Self.manager(cacheDirectory: dir, recording: recording)
        try await manager.bootstrap()

        let episodeId = "gpdb-refused-resume"
        let source = URL(string: "https://cdn.example.com/\(episodeId).mp3")!
        try await manager.persistResumeData(
            episodeId: episodeId,
            data: Data([0xAB, 0xCD]),
            sourceURL: source,
            validator: HTTPAssetMetadata(
                etag: "\"A\"", contentLength: 100, lastModified: nil
            )
        )
        // Freshness gate passes, so the run reaches the session lookup rather
        // than diverting to the re-download branch.
        await manager.setResumeValidatorProviderForTesting { _ in
            HTTPAssetMetadata(etag: "\"A\"", contentLength: 100, lastModified: nil)
        }

        let outcome = try await manager.resumeSuspendedTransfer(episodeId: episodeId)
        #expect(outcome == .daemonUnavailable)
        #expect(
            try await manager.loadResumeData(episodeId: episodeId) != nil,
            "a refused session must not cost the user the bytes already fetched"
        )
        #expect(
            recording.refusals.contains {
                $0.contains(
                    "site=\(DownloadManager.BackgroundSessionRequestSite.forceQuitResume.rawValue)"
                )
            },
            "the force-quit resume site must name itself: \(recording.refusals)"
        )
    }

    // MARK: retireBackgroundTransfers

    /// Cache deletion FAILS OPEN on a refused session, and that is the same
    /// trade playhead-rouw made one layer in for a lost enumeration. What is
    /// lost is a SOURCE of transfer identities; what is not lost is the
    /// deletion of the bytes or the retirement that keeps a late completion
    /// from resurrecting them. An implementation that returned early — or
    /// threw — on the refusal would leave the user's delete undone while
    /// reporting success.
    @Test("removeCache still deletes and still retires when no session can be opened")
    func cacheDeletionSurvivesARefusedSession() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let recording = RecordedViolations()
        let manager = Self.manager(cacheDirectory: dir, recording: recording)
        try await manager.bootstrap()

        let episodeId = "gpdb-refused-retire"
        let identity = await manager._registerBackgroundTransferForTesting(
            episodeId: episodeId
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId),
            "the rail is vacuous unless there was an admitted transfer to retire"
        )
        let completeURL = await manager.completeFileURL(for: episodeId)
        try Data(repeating: 0x7E, count: 96).write(to: completeURL)

        try await manager.removeCache(for: episodeId)

        #expect(!FileManager.default.fileExists(atPath: completeURL.path))
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false
        )

        // The retired identity's own late completion must still be discarded
        // rather than re-placing the bytes the user just deleted. The staged
        // deposit is REAL and lives outside the cache, so a manager that
        // failed to retire would place it.
        let staged = dir.appendingPathComponent("gpdb-staged-deposit")
        try Data(repeating: 0x3D, count: 96).write(to: staged)
        await manager.handleBackgroundDownloadComplete(
            episodeId: episodeId,
            stagedURL: staged,
            originalURL: URL(string: "https://cdn.example.com/gpdb.mp3"),
            metadata: HTTPAssetMetadata(
                etag: "\"gpdb\"", contentLength: 96, lastModified: nil
            ),
            transferIdentity: identity
        )
        #expect(
            !FileManager.default.fileExists(atPath: completeURL.path),
            "a refused session must not cost the retirement — a late completion for a deleted episode has to stay discarded"
        )
        #expect(await manager.cachedFileURL(for: episodeId) == nil)
        #expect(
            recording.refusals.contains {
                $0.contains(
                    "site=\(DownloadManager.BackgroundSessionRequestSite.transferRetirement.rawValue)"
                )
            },
            "the retirement site must name itself: \(recording.refusals)"
        )
    }

    // MARK: resumeSession — the warmup that is not one

    /// `resumeSession(identifier:)` USED to be `_ = backgroundSession(for:)`,
    /// which reads like a warmup whose failure another call would retry. It is
    /// not one, and the bead that filed this defect guessed the other way.
    /// `PlayheadRuntime` documents it as the only production caller that
    /// re-instantiates the background session in a relaunched process: without
    /// it the delegate never fires, `handleBackgroundDownloadComplete` never
    /// runs, and the OS completion handler is never invoked. Nothing retries
    /// any of that, so the failure is reported and recorded rather than
    /// swallowed.
    @Test("A background relaunch the daemon refuses reports failure rather than swallowing it")
    func refusedRelaunchWakeIsReportedAndRecorded() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let recording = RecordedViolations()
        let manager = Self.manager(cacheDirectory: dir, recording: recording)
        try await manager.bootstrap()

        let woke = await manager.resumeSession(
            identifier: BackgroundSessionIdentifier.legacy
        )
        #expect(
            woke == false,
            "a wake that opened no session must not report that it did"
        )
        #expect(await manager.instantiatedSessionIdentifiersForTesting().isEmpty)
        #expect(
            recording.refusals.contains {
                $0.contains(
                    "site=\(DownloadManager.BackgroundSessionRequestSite.backgroundRelaunchWake.rawValue)"
                )
            },
            """
            the relaunch wake is the launch nobody is watching — a refusal \
            there that records nothing is the silence this bead exists to \
            remove: \(recording.refusals)
            """
        )
    }
}

// MARK: - Recovery, and the shape of the memo

@Suite("DownloadManager – a refused session is not a permanent one (playhead-gpdb)")
struct BackgroundSessionCreationRecoveryTests {

    /// A daemon that refuses construction while the gate is up and answers
    /// once it is lowered.
    private final class Daemon: Sendable {
        private let refusing: OSAllocatedUnfairLock<Bool>

        /// Stored, not computed: every `BackgroundSessionIO` owns its own
        /// dispatch queue, so a computed property would hand the manager one
        /// instance and this test another.
        let io: BackgroundSessionIO

        init() {
            let gate = OSAllocatedUnfairLock<Bool>(initialState: true)
            self.refusing = gate
            self.io = BackgroundSessionIO(
                behavior: .intermittentlyRefusesCallsLabelled(
                    DownloadManager.sessionCreationLabelPrefix,
                    whileRefusing: { gate.withLock { $0 } }
                ),
                // The PRODUCTION bound, because the second half of this rail
                // needs a real construction to genuinely SUCCEED and that is
                // the blocking XPC round-trip the whole bead is about. A short
                // bound would make the rail a latency measurement of
                // `nsurlsessiond` — the load-flake family nsjn's header
                // disowns. It costs no test time: the refusal is synchronous,
                // so nothing here ever waits a bound out.
                timeout: BackgroundSessionIO.defaultTimeout,
                queueLabel: "gpdb.test.recovering.\(UUID().uuidString)"
            )
        }

        func startAnswering() { refusing.withLock { $0 = false } }
    }

    /// THE anti-caching rail, and the one the bead's implementation note asks
    /// for by name: *"A failed construction must leave `_sessionsByRole` empty
    /// so the NEXT call retries naturally. Do not cache the failure."*
    ///
    /// Cache the refusal — memoize the `nil`, or leave the in-flight entry
    /// behind for later callers to join — and the FIRST rail here still
    /// passes, because a caching implementation reports the first refusal
    /// exactly the same way. Only a second attempt against a recovered daemon
    /// separates "a transient stall" from "the download subsystem is dead for
    /// this process launch", which is the whole consequence this bead is
    /// about.
    @Test("A session refused once is constructed on the next attempt")
    func sessionCreationRetriesAfterARefusal() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let daemon = Daemon()
        let manager = DownloadManager(cacheDirectory: dir, sessionIO: daemon.io)
        try await manager.bootstrap()

        #expect(
            await manager.backgroundSessionForTesting(role: .legacy) == nil,
            "the rail is vacuous unless the first attempt was actually refused"
        )
        #expect(
            await manager.instantiatedSessionIdentifiersForTesting().isEmpty,
            "a refusal must leave the role map EMPTY — a memoized nil is a dead download subsystem for the life of the process"
        )

        daemon.startAnswering()

        let recovered = try #require(
            await manager.backgroundSessionForTesting(role: .legacy),
            "the next attempt must retry the crossing, not replay a cached refusal"
        )
        #expect(recovered.configuration.identifier == BackgroundSessionIdentifier.legacy)
        #expect(
            await manager.instantiatedSessionIdentifiersForTesting()
                .contains(BackgroundSessionIdentifier.legacy)
        )

        // The real background session this rail opened must not outlive it:
        // an orphan stays registered with the simulator's `nsurlsessiond` and
        // leaks into whatever runs next (playhead-ornc).
        await manager.invalidateBackgroundSessionsForTesting()
    }

    /// Counts every crossing a manager submits and admits only the first.
    ///
    /// playhead-gpdb R1 review. Two things no other seam here provides.
    ///
    /// COUNTING. `whileRefusing` is invoked exactly once per submission whose
    /// label contains the marker (`perform`'s `where` clause short-circuits on
    /// `label.contains`), so calling it and returning `false` is a call
    /// counter that needs no production hook. The marker is `" for "` because
    /// BOTH crossing labels carry it — `URLSession(background:) for <id>` and
    /// `downloadTask(with:) for <episode>`. The count is the only thing that
    /// can see a duplicated crossing: two `URLSession`s on one background
    /// identifier both report that identifier, so
    /// `instantiatedSessionIdentifiersForTesting()` reads `1` either way, and
    /// the role map is keyed by role so the second write merely overwrites the
    /// first.
    ///
    /// REFUSING AFTER THE FIRST. Everything past the admitted crossing is
    /// refused, so no rail below hands a real transfer to the simulator's
    /// `nsurlsessiond` (playhead-ornc) merely to observe an ordering.
    private final class CrossingCounter: Sendable {
        private let counter: OSAllocatedUnfairLock<Int>

        /// Stored, not computed: every `BackgroundSessionIO` owns its own
        /// dispatch queue, so a computed property would hand the manager one
        /// instance and this test another.
        let io: BackgroundSessionIO

        init(admittingFirst admitted: Int) {
            let counter = OSAllocatedUnfairLock<Int>(initialState: 0)
            self.counter = counter
            self.io = BackgroundSessionIO(
                behavior: .intermittentlyRefusesCallsLabelled(
                    " for ",
                    whileRefusing: {
                        counter.withLock { seen in
                            seen += 1
                            return seen > admitted
                        }
                    }
                ),
                // The PRODUCTION bound: the admitted crossing has to genuinely
                // succeed, and a short bound would turn that into a latency
                // measurement of `nsurlsessiond`.
                timeout: BackgroundSessionIO.defaultTimeout,
                queueLabel: "gpdb.test.crossings.\(UUID().uuidString)"
            )
        }

        var crossings: Int { counter.withLock { $0 } }
    }

    /// Occupies `io`'s serial work queue and returns the semaphore that frees
    /// it. Returns only once the queue is provably held.
    ///
    /// playhead-gpdb R1 review, and this is the load-bearing part of the rail
    /// below. Without it the three callers DO NOT OVERLAP: caller one's
    /// crossing completes in microseconds against a healthy simulator daemon,
    /// so callers two and three find the memo already populated and return it
    /// — which is exactly what a correct implementation does, and exactly what
    /// an implementation with NO in-flight join does too. Measured: deleting
    /// the join outright left the rail GREEN. Holding the queue is what makes
    /// the crossing slow enough for the re-entrancy this bead introduces to
    /// actually happen.
    private static func holdingQueue(
        of io: BackgroundSessionIO
    ) async -> DispatchSemaphore {
        let release = DispatchSemaphore(value: 0)
        await withCheckedContinuation { (held: CheckedContinuation<Void, Never>) in
            Task {
                // The label deliberately carries no `" for "`, so this
                // occupancy is neither counted nor refused by `CrossingCounter`.
                _ = await io.perform(
                    label: "gpdb.test.occupancy",
                    running: {
                        held.resume()
                        release.wait()
                    }
                )
            }
        }
        return release
    }

    /// How long a rail waits for a SECOND crossing to show up before
    /// concluding there is not going to be one. Only a broken implementation
    /// ever spends it: the duplicate appears within microseconds of the
    /// callers arriving, so this is the budget the GREEN path pays and the
    /// RED path exits early from.
    private static let duplicateCrossingWindow = 200

    /// Making construction failable made it a SUSPENSION POINT, and
    /// `DownloadManager` is a re-entrant actor — so two callers that both find
    /// the memo empty would both cross into the daemon and open TWO
    /// `URLSession`s on the SAME background identifier, which URLSession does
    /// not support. That interleaving was impossible before this bead (the
    /// whole function ran synchronously on the actor), so it is a hazard this
    /// change introduces and has to close.
    ///
    /// THE CROSSING COUNT, not object identity, is the assertion that can see
    /// it. Identity is necessary but not sufficient — see `CrossingCounter`
    /// for why every other observable reads `1` on a manager that opened three
    /// sessions — and neither assertion means anything at all unless the three
    /// callers genuinely overlap, which is what `holdingQueue` arranges and
    /// what this rail did not do before the R1 review.
    @Test("Concurrent requests for one role share a single construction")
    func concurrentRequestsDoNotOpenTwoSessions() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let daemon = CrossingCounter(admittingFirst: 1)
        let manager = DownloadManager(cacheDirectory: dir, sessionIO: daemon.io)
        try await manager.bootstrap()

        let release = await Self.holdingQueue(of: await manager.sessionCreationIO)

        async let first = manager.backgroundSessionForTesting(role: .legacy)
        async let second = manager.backgroundSessionForTesting(role: .legacy)
        async let third = manager.backgroundSessionForTesting(role: .legacy)

        // The first crossing is a deterministic wait: it cannot complete while
        // the queue is held, so it is observable for as long as we care to
        // look.
        var ticks = 0
        while daemon.crossings < 1, ticks < 2_000 {
            try await Task.sleep(for: .milliseconds(1))
            ticks += 1
        }
        #expect(
            daemon.crossings >= 1,
            "the rail is vacuous unless a crossing was actually submitted"
        )
        // A duplicate, if there is going to be one, is submitted by a caller
        // that has already found the memo empty — i.e. within microseconds.
        ticks = 0
        while daemon.crossings == 1, ticks < Self.duplicateCrossingWindow {
            try await Task.sleep(for: .milliseconds(1))
            ticks += 1
        }
        release.signal()

        // Asserted BEFORE the results are unwrapped: a duplicated crossing is
        // refused by `CrossingCounter`, so an implementation without the join
        // also fails the `#require` below — and `Expectation failed: await
        // second` names the symptom rather than the defect.
        #expect(
            daemon.crossings == 1,
            """
            three concurrent callers must produce ONE crossing into \
            `nsurlsessiond`; \(daemon.crossings) means each caller opened its \
            own `URLSession` on one background identifier, which is a state \
            the OS does not support
            """
        )

        let one = try #require(await first)
        let two = try #require(await second)
        let three = try #require(await third)

        #expect(
            one === two && two === three,
            "three callers must share ONE session; two URLSessions on one background identifier is a state the OS does not support"
        )
        #expect(
            await manager.instantiatedSessionIdentifiersForTesting().count == 1
        )

        await manager.invalidateBackgroundSessionsForTesting()
    }

    /// `backgroundDownload` reserves its per-episode in-flight slot BEFORE the
    /// session lookup, and this rail is what makes that ordering a property
    /// rather than a comment.
    ///
    /// playhead-gpdb R1 review: the implementation moved the reservation up
    /// for exactly this reason and nothing failed when it was moved back —
    /// measured. The window it closes is the FIRST download of a process, the
    /// one that has to construct the session: with the reservation below the
    /// lookup, caller one suspends inside the daemon holding nothing, caller
    /// two sails through the `bgInFlightEpisodes` guard, and one episode gets
    /// two transfers. That is the same defect playhead-nsjn closed one
    /// suspension point later, re-opened by adding an earlier one.
    ///
    /// The queue hold is what makes the window observable: while it is held
    /// the crossing cannot answer, so "caller one is suspended inside
    /// `backgroundSession`" is a state the rail can stand in and look at.
    @Test("backgroundDownload holds its in-flight slot across the session crossing")
    func reservationCoversTheSessionCrossing() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let daemon = CrossingCounter(admittingFirst: 1)
        let manager = DownloadManager(cacheDirectory: dir, sessionIO: daemon.io)
        try await manager.bootstrap()

        let release = await Self.holdingQueue(of: await manager.sessionCreationIO)

        let episodeId = "gpdb-reservation-across-the-crossing"
        let download = Task {
            await manager.backgroundDownload(
                episodeId: episodeId,
                from: URL(string: "https://cdn.example.com/\(episodeId).mp3")!,
                context: DownloadContext(
                    podcastId: "show-gpdb",
                    isExplicitDownload: false,
                    podcastTitle: "Show",
                    episodeTitle: "Episode"
                )
            )
        }

        var ticks = 0
        while daemon.crossings < 1, ticks < 2_000 {
            try await Task.sleep(for: .milliseconds(1))
            ticks += 1
        }
        #expect(
            daemon.crossings >= 1,
            "the rail is vacuous unless the download actually reached the session crossing"
        )
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId),
            """
            the episode must already be reserved while its caller is suspended \
            inside the session crossing — otherwise a second caller for the \
            same episode passes the in-flight guard and one episode gets two \
            transfers
            """
        )

        release.signal()
        await download.value

        // The `downloadTask(with:)` crossing is the second one, so it is
        // refused: nothing was admitted to the OS, and the reservation the
        // rail just observed is released again rather than wedging the
        // episode for the life of the process.
        #expect(await manager._backgroundDownloadAdmissionCountForTesting() == 0)
        #expect(
            await manager._isBackgroundDownloadInFlightForTesting(episodeId: episodeId) == false
        )

        await manager.invalidateBackgroundSessionsForTesting()
    }
}

// MARK: - The structural rail

/// playhead-gpdb: no background `URLSession` may be constructed unbounded.
///
/// A SOURCE canary for the same reason playhead-rouw's is: the property is not
/// observable from a test. Every behavioural rail above passes on the
/// unbounded implementation too — the simulator's `nsurlsessiond` opens a
/// session in milliseconds when the box is quiet, and no test can arrange the
/// wedged daemon a full-plan run produces. The difference between the two
/// implementations is visible only in the source: whether the crossing goes
/// through the bound.
///
/// The token is the API NAME — `background(withIdentifier:` is the only way
/// the language offers to make a background configuration — so this is "one
/// identifier, every spelling" rather than "the spellings the last reviewer
/// thought of". The escape that remains open is the same one rouw's canary
/// documents: a factory declared in another file and reached through a
/// typealias. It is a limit of a lexical rail, not of this one in particular.
@Suite("playhead-gpdb: background session construction is bounded")
struct BackgroundSessionCreationSourceCanaryTests {

    private static let crossingFile =
        "Playhead/Services/Downloads/DownloadManager.swift"
    private static let crossingSignature =
        "private func createBackgroundSession("

    /// One per role. The count is pinned so that deleting a role's
    /// configuration — or adding a fourth one outside the bound — is a
    /// failure rather than a silent drift.
    private static let permittedConfigurations = 3

    /// WHITESPACE-TOLERANT on purpose, and it caught the author on the first
    /// run: production spells the call across three lines
    /// (`.background(\n    withIdentifier: …\n)`), so the literal
    /// `background(withIdentifier:` matched ZERO times and the rail reported
    /// an allowance of 0 for code that was right there. A literal token would
    /// also have been silently satisfied by any reformatting — the same
    /// "the spellings the last reviewer thought of" failure playhead-9y9e's
    /// R7 review found three ways past.
    private static let configurationPattern =
        #"background\s*\(\s*withIdentifier\s*:"#

    /// Cheap raw-text prefilter before the expensive strip. Deliberately
    /// broader than the pattern: it only has to avoid stripping files that
    /// cannot possibly match.
    private static let configurationPrefilter = "withIdentifier"

    private static func appTargetSources() throws -> [String] {
        guard let root = SwiftSourceInspector.repositoryRoot(from: #filePath) else {
            throw NSError(
                domain: "BackgroundSessionCreationSourceCanaryTests",
                code: 1,
                userInfo: [NSLocalizedDescriptionKey: "could not locate the repo root"]
            )
        }
        let appRoot = root.appendingPathComponent("Playhead", isDirectory: true)
        guard let walker = FileManager.default.enumerator(
            at: appRoot,
            includingPropertiesForKeys: [.isRegularFileKey]
        ) else { return [] }
        var found: [String] = []
        // Deliberately NOT `resolvingSymlinksInPath()` — see the note on
        // `BackgroundSessionEnumerationSourceCanaryTests.appTargetSources`.
        let prefix = root.path.hasSuffix("/") ? root.path : root.path + "/"
        for case let url as URL in walker where url.pathExtension == "swift" {
            guard url.path.hasPrefix(prefix) else { continue }
            found.append(String(url.path.dropFirst(prefix.count)))
        }
        return found.sorted()
    }

    @Test("The app target builds a background session configuration in exactly one place")
    func configurationConstructionIsSingular() throws {
        let sources = try Self.appTargetSources()
        // Anti-vacuity: a sweep that reaches nothing reports zero hazards.
        #expect(
            sources.count > 400,
            "the sweep found \(sources.count) sources — it is not reaching the app target, so a zero-hit result would be vacuous"
        )
        #expect(sources.contains(Self.crossingFile))

        let crossingSource = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.crossingFile
        )
        let crossingCode = SwiftSourceInspector.strippingCommentsAndStrings(
            crossingSource
        )
        let crossingBody = SwiftSourceInspector.firstBody(
            in: crossingCode, after: Self.crossingSignature
        )
        #expect(
            crossingBody != nil,
            "`\(Self.crossingSignature)` is gone from \(Self.crossingFile) — the rail cannot know what to permit"
        )
        let allowance = SwiftSourceInspector.regexOccurrences(
            of: Self.configurationPattern, in: crossingBody ?? ""
        )
        #expect(
            allowance == Self.permittedConfigurations,
            "`createBackgroundSession` should build \(Self.permittedConfigurations) configurations, one per role; it builds \(allowance)"
        )

        for path in sources {
            let source = try SwiftSourceInspector.loadSource(repoRelativePath: path)
            guard source.contains(Self.configurationPrefilter) else { continue }
            let code = SwiftSourceInspector.strippingCommentsAndStrings(source)
            let found = SwiftSourceInspector.regexOccurrences(
                of: Self.configurationPattern, in: code
            )
            let permitted = (path == Self.crossingFile) ? allowance : 0
            #expect(
                found == permitted,
                """
                \(path) builds a BACKGROUND URLSession configuration \(found) \
                time(s); \(permitted) is permitted. Constructing the session \
                that follows attaches to `nsurlsessiond` — the daemon \
                playhead-nsjn sampled with all ten cooperative threads parked \
                behind it — and `DownloadManager` is an actor, so an \
                unbounded construction is a process-wide Swift Concurrency \
                deadlock with no spinner, no timeout and no crash report. It \
                is also the crossing that BOTH already-bounded calls are \
                reached through. Route it through \
                `DownloadManager.createBackgroundSession(role:)`, which \
                submits the whole construction to `sessionCreationIO`.
                """
            )
        }
    }

    /// The positive half. Without it, deleting the crossing outright — or
    /// keeping the function's name while calling `URLSession(configuration:)`
    /// straight from the actor — would satisfy the rail above.
    @Test("The bounded construction exists and runs through BackgroundSessionIO")
    func theBoundedConstructionIsTheOneThatSurvives() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: Self.crossingFile
        )
        // Strings stripped as well as comments: every token below is a CODE
        // token, and the label prefix this rail pins is itself a string
        // literal spelling `URLSession(` — counting it would make the
        // construction check pass on a body that constructs nothing.
        let code = SwiftSourceInspector.strippingCommentsAndStrings(source)
        #expect(code.contains(Self.crossingSignature))
        let body = SwiftSourceInspector.firstBody(
            in: code, after: Self.crossingSignature
        )
        #expect(body != nil, "`createBackgroundSession` has no locatable body")
        let crossing = body ?? ""
        #expect(
            SwiftSourceInspector.regexOccurrences(
                of: #"URLSession\s*\("#, in: crossing
            ) == 1,
            "the session construction itself has to be INSIDE `createBackgroundSession`"
        )
        #expect(
            crossing.contains("sessionCreationIO.perform("),
            """
            the construction has to be submitted to `BackgroundSessionIO` — \
            and to `sessionCreationIO`, not `sessionIO` or `enumerationIO`. \
            Those two serial queues carry every call that makes a download \
            happen and every task enumeration; a construction that wedges one \
            of them strands it for the life of the process, which is the \
            outcome this bead converts into a reported refusal.
            """
        )
        // The label check needs the OTHER stripping, and the difference is not
        // cosmetic: the label is built by INTERPOLATION
        // (`"\(Self.sessionCreationLabelPrefix)\(role.identifier)"`), and
        // `strippingCommentsAndStrings` blanks a literal's whole contents —
        // interpolated code included. Read against the stripped-code body this
        // assertion looks for a token that has just been erased, and reddens
        // on correct source. (It did, on the first run.)
        let withStrings = SwiftSourceInspector.firstBody(
            in: SwiftSourceInspector.strippingComments(source),
            after: Self.crossingSignature
        ) ?? ""
        #expect(
            withStrings.contains("Self.sessionCreationLabelPrefix"),
            "the crossing must carry the shared label prefix, or a seam that refuses by label silently stops refusing it"
        )
        // The refusal must reach the surface-status stream. A `nil` that only
        // reaches `os_log` is not distinguishable from "no download was
        // requested" on a device pull, which is the whole lesson of
        // playhead-oa82.
        #expect(
            code.contains(".backgroundSessionCreationRefused"),
            "a refused construction must be RECORDED, not only logged"
        )
    }
}
