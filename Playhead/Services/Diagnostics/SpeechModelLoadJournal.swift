// SpeechModelLoadJournal.swift
// Durable record of whether the ASR model ever loaded, across launches.
//
// Scope: playhead-se2h (a swallowed launch-time model-load failure
//        permanently disabled transcription, with no retry).
//
// ----- Local-only, by decision -----
//
// Nothing in this file transmits. It writes one JSON document to the
// device, and the diagnostics bundle the user already chooses to send is
// the only way it leaves. Same posture, same directory, and the same
// storage shape as `AnalysisStoreHealthJournal` — see that file's header
// for the full argument, which applies here verbatim:
//
//   * a single atomically-replaced JSON document, not JSONL, because the
//     most important field is a COUNTER that must have exactly one
//     correct value rather than one reconstructed from a possibly-torn
//     append log;
//   * `.completeUntilFirstUserAuthentication`, re-applied after every
//     write because `.atomic` replaces the inode;
//   * and the refuse-to-write rule: an unreadable document is NOT an
//     empty one, so `mutate` declines to clobber it rather than resetting
//     a real escalation history to zero.
//
// Reusing that shape is deliberate. Two journals that describe two
// subsystems in the same way is one thing to learn; two journals with
// different durability rules is two.

import Foundation
import OSLog

actor SpeechModelLoadJournal {

    // MARK: - Configuration

    /// Same `Diagnostics` directory the crash ring buffer and the
    /// analysis-store health journal use — support tooling looks in one
    /// place.
    static let directoryName = StabilityDiagnosticsStore.directoryName

    static let filename = "speech-model-load.json"

    /// Production handle. Constructed lazily; touches no file until
    /// something is read or written.
    ///
    /// Tests MUST NOT use this — every test constructs its own instance
    /// against a temp directory, the same isolation rule
    /// `AnalysisStoreHealthJournal` and `StabilityDiagnosticsStore`
    /// follow. `PlayheadRuntime` is what injects it, and only outside the
    /// XCTest host; see `PlayheadRuntime.speechModelLoadJournal(underTest:)`.
    static let shared = SpeechModelLoadJournal()

    // MARK: - State

    private let explicitDirectory: URL?
    private let fileManager: FileManager
    private let logger = Logger(subsystem: "com.playhead", category: "SpeechModelLoad")
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

    /// The persisted state, or ``SpeechModelLoadState/unknown`` when no
    /// document exists yet.
    ///
    /// `.unknown` — NOT a healthy value — is the right answer for both
    /// "nothing recorded" and "recorded but unreadable", and the write
    /// side independently refuses to overwrite an unreadable document, so
    /// the optimistic read cannot lose real history.
    func load() -> SpeechModelLoadState {
        guard let url = fileURL(), let data = try? Data(contentsOf: url) else {
            return .unknown
        }
        guard let state = try? Self.decoder.decode(SpeechModelLoadState.self, from: data) else {
            // `SpeechModelLoadState.init(from:)` degrades every individual
            // field rather than throwing, so reaching here means the bytes
            // are not JSON at all. The next `mutate` replaces them —
            // refusing forever would wedge the counter permanently for no
            // gain, since a wedged counter reports nothing either.
            logger.fault("speech-model-load document present but undecodable — it will be replaced")
            return .unknown
        }
        return state
    }

    // MARK: - Write

    /// Record a failed model load and return the resulting state.
    ///
    /// - Parameters:
    ///   - error: the thrown error, classified through
    ///     `TranscriptFailureClass` — the same closed vocabulary the
    ///     transcription loop's failures use.
    ///   - attemptNumber: 1-based position within the process's retry
    ///     budget, so a reader can tell a first failure from the one that
    ///     exhausted the budget.
    ///   - now: injected for tests.
    @discardableResult
    func recordFailure(
        error: any Error,
        attemptNumber: Int,
        now: Date = Date()
    ) -> SpeechModelLoadState {
        let failureClass = TranscriptFailureClass.classify(error)
        return mutate { current in
            let newCount = current.consecutiveFailureCount + 1
            let record = SpeechModelLoadFailureRecord(
                occurredAt: now,
                failureClass: failureClass,
                attemptNumber: attemptNumber,
                consecutiveFailureCount: newCount
            )
            return SpeechModelLoadState(
                status: newCount >= SpeechModelLoadState.failuresBeforeConcern
                    ? .persistentlyFailing
                    : .retrying,
                consecutiveFailureCount: newCount,
                firstFailureAt: current.firstFailureAt ?? now,
                lastFailureAt: now,
                lastSuccessAt: current.lastSuccessAt,
                lastSuccessfulRole: current.lastSuccessfulRole,
                recentFailures: current.recentFailures + [record]
            )
        }
    }

    /// Record a successful model load. Clears the escalation counter and
    /// the failure window but KEEPS the failure records: a device that
    /// failed twice and then recovered is exactly the history that
    /// distinguishes a transient asset hiccup from a broken install, and
    /// it is the only evidence the retry did its job.
    @discardableResult
    func recordSuccess(role: ModelRole, now: Date = Date()) -> SpeechModelLoadState {
        mutate { current in
            SpeechModelLoadState(
                status: .loaded,
                consecutiveFailureCount: 0,
                firstFailureAt: nil,
                lastFailureAt: current.lastFailureAt,
                lastSuccessAt: now,
                lastSuccessfulRole: role,
                recentFailures: current.recentFailures
            )
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

    /// Read-modify-write with the refuse-to-clobber rule. Returns the
    /// transformed state whether or not the write landed — a failed write
    /// must not make the caller believe the event did not happen.
    private func mutate(
        _ transform: (SpeechModelLoadState) -> SpeechModelLoadState
    ) -> SpeechModelLoadState {
        if let url = fileURL(),
           fileManager.fileExists(atPath: url.path),
           (try? Data(contentsOf: url)) == nil {
            logger.error("speech-model-load document unreadable — skipping write rather than clobbering it")
            return transform(.unknown)
        }

        let next = transform(load())
        write(next)
        return next
    }

    private func write(_ state: SpeechModelLoadState) {
        guard let directory = directoryURL(), let url = fileURL() else { return }
        guard let data = try? Self.encoder.encode(state) else {
            logger.error("speech-model-load document failed to encode")
            return
        }
        do {
            try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
            try data.write(to: url, options: [.atomic])
            // Re-apply protection after every write: `.atomic` replaces the
            // inode, so an attribute set once at create time silently stops
            // applying.
            try fileManager.setAttributes(
                [.protectionKey: FileProtectionType.completeUntilFirstUserAuthentication],
                ofItemAtPath: url.path
            )
        } catch {
            // FAULT, not error. The consequence reaches beyond this file:
            // this document is the only durable record that the ASR model
            // is not loading, so a write that keeps failing leaves the
            // bundle reporting `unknown` forever — indistinguishable from
            // an unwired signal, which is the exact ambiguity this journal
            // was added to remove.
            logger.fault(
                "speech-model-load write FAILED — the diagnostics bundle will keep reporting 'unknown': \(error.localizedDescription, privacy: .public)"
            )
        }
    }
}
