// ShowMusicBedProfileStore.swift
// playhead-2hpn (Plan §6 Phase 3 deliverable 4): persistence seam for
// the `ShowMusicBedProfile` SwiftData model.
//
// The store hops to the MainActor to touch the `ModelContext`
// (SwiftData's contract) and exposes Sendable async methods to the
// off-main `AdDetectionService` consumer. The pattern mirrors
// `SwiftDataNewEpisodeAnnouncer` so a reader who has seen one
// SwiftData seam has seen them all.
//
// Why a wrapper (not direct `ModelContext` access from the consumer):
//   * The consumer (`AdDetectionService`) is a serial executor — actor
//     state is its own per-instance state. Pulling SwiftData reads
//     across actor boundaries forces a hop; centralising the hop here
//     keeps each call site one `await` instead of a `MainActor.run`
//     closure with captured locals.
//   * Tests can inject a `Resolving` conformer that returns synthetic
//     profiles without standing up a SwiftData container.
//
// Thread/actor model:
//   * `ShowMusicBedProfileResolving` is the abstract Sendable seam.
//   * `ShowMusicBedProfileStore` is the SwiftData-backed conformer.
//     The struct itself is Sendable (only stored field is a
//     ModelContainer which is Sendable). Each method jumps to
//     MainActor internally.
//
// Read-mostly: the consumer asks "is show X confirmed and which
// hashes does it know about?" on every span; the mutation path is
// once-per-episode at the end of `runBackfill`.

import Foundation
import OSLog
import SwiftData

// MARK: - Abstract seam

/// Read-side contract consumed by `AdDetectionService` and the
/// `MusicBedLedgerEvaluator` boost path. Sendable so it can cross
/// actor boundaries without copying any mutable state.
protocol ShowMusicBedProfileResolving: Sendable {
    /// Returns the current profile snapshot for `showIdentifier`, or
    /// `nil` when the show has never been observed. The snapshot is a
    /// value type — mutating it has no effect on storage.
    func snapshot(showIdentifier: String) async -> ShowMusicBedProfileSnapshot?

    /// Applies the result of one episode's jingle-extraction pass to
    /// the profile for `showIdentifier`. Creates the profile if
    /// missing, advances `confirmationCount` / `consecutiveMissCount`,
    /// records new matching hashes, and evicts on the bead-spec
    /// 30-consecutive-miss threshold. Returns the post-update snapshot
    /// so the caller can log / instrument without a second read.
    @discardableResult
    func recordEpisodeOutcome(
        showIdentifier: String,
        outcome: ShowMusicBedEpisodeOutcome,
        now: Date
    ) async -> ShowMusicBedProfileSnapshot

    /// Returns snapshots for ALL stored profiles. Used by the diagnostics
    /// bundle and tests. Empty when no profile has ever been written.
    func allSnapshots() async -> [ShowMusicBedProfileSnapshot]
}

// MARK: - Value snapshot

/// Immutable value-type view of a `ShowMusicBedProfile`. Crossing the
/// actor boundary as a value avoids any SwiftData-row aliasing pitfalls
/// (the @Model is `final class`, not Sendable).
struct ShowMusicBedProfileSnapshot: Sendable, Equatable {
    let showIdentifier: String
    let confirmedJingleHashes: [RepeatedAdFingerprint]
    let confirmationCount: Int
    let consecutiveMissCount: Int
    let versionStamp: Int
    let createdAt: Date
    let updatedAt: Date

    var isConfirmed: Bool {
        confirmationCount >= ShowMusicBedProfile.confirmationThreshold
            && !confirmedJingleHashes.isEmpty
    }
}

// MARK: - Outcome (input to recordEpisodeOutcome)

/// The minimum information one episode's pre-analysis pass produces
/// about its intro/outro jingle slice. Pre-extracted so the
/// MainActor-bound store doesn't have to know how to compute hashes.
struct ShowMusicBedEpisodeOutcome: Sendable, Equatable {
    /// The episode-start (first ~10 s) and episode-end (last ~10 s)
    /// hashes the evaluator extracted. Either may be `.zero` if the
    /// audio at that edge produced no meaningful signal. Zero entries
    /// are skipped by the matching logic (they would otherwise collide
    /// with every other zero).
    let startHash: RepeatedAdFingerprint
    let endHash: RepeatedAdFingerprint
}

// MARK: - SwiftData-backed conformer

/// Production conformer. Holds a `ModelContainer` and hops to the
/// MainActor for every method.
struct ShowMusicBedProfileStore: ShowMusicBedProfileResolving {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "ShowMusicBedProfileStore"
    )

    private let modelContainer: ModelContainer

    init(modelContainer: ModelContainer) {
        self.modelContainer = modelContainer
    }

    func snapshot(showIdentifier: String) async -> ShowMusicBedProfileSnapshot? {
        await MainActor.run { [modelContainer] in
            Self.fetchSnapshot(
                showIdentifier: showIdentifier,
                context: modelContainer.mainContext
            )
        }
    }

    @discardableResult
    func recordEpisodeOutcome(
        showIdentifier: String,
        outcome: ShowMusicBedEpisodeOutcome,
        now: Date
    ) async -> ShowMusicBedProfileSnapshot {
        await MainActor.run { [modelContainer] in
            Self.recordOutcome(
                showIdentifier: showIdentifier,
                outcome: outcome,
                now: now,
                context: modelContainer.mainContext
            )
        }
    }

    func allSnapshots() async -> [ShowMusicBedProfileSnapshot] {
        await MainActor.run { [modelContainer] in
            Self.fetchAllSnapshots(context: modelContainer.mainContext)
        }
    }

    // MARK: - MainActor implementation

    @MainActor
    private static func fetchSnapshot(
        showIdentifier: String,
        context: ModelContext
    ) -> ShowMusicBedProfileSnapshot? {
        guard let row = fetchProfile(showIdentifier: showIdentifier, context: context) else {
            return nil
        }
        return snapshot(from: row)
    }

    @MainActor
    private static func fetchAllSnapshots(
        context: ModelContext
    ) -> [ShowMusicBedProfileSnapshot] {
        let descriptor = FetchDescriptor<ShowMusicBedProfile>()
        do {
            let rows = try context.fetch(descriptor)
            return rows.map(snapshot(from:))
        } catch {
            logger.error("fetchAllSnapshots failed: \(error.localizedDescription, privacy: .public)")
            return []
        }
    }

    @MainActor
    private static func recordOutcome(
        showIdentifier: String,
        outcome: ShowMusicBedEpisodeOutcome,
        now: Date,
        context: ModelContext
    ) -> ShowMusicBedProfileSnapshot {
        let existing = fetchProfile(showIdentifier: showIdentifier, context: context)
        let profile = existing ?? ShowMusicBedProfile(
            showIdentifier: showIdentifier,
            createdAt: now,
            updatedAt: now
        )
        if existing == nil {
            context.insert(profile)
        }

        let mutated = ShowMusicBedProfileEvaluator.apply(
            outcome: outcome,
            toShowIdentifier: showIdentifier,
            confirmedHashes: profile.confirmedJingleHashBits.map { RepeatedAdFingerprint(bits: UInt64(bitPattern: $0)) },
            confirmationCount: profile.confirmationCount,
            consecutiveMissCount: profile.consecutiveMissCount
        )

        profile.confirmedJingleHashBits = mutated.confirmedHashes.map { Int64(bitPattern: $0.bits) }
        profile.confirmationCount = mutated.confirmationCount
        profile.consecutiveMissCount = mutated.consecutiveMissCount
        profile.versionStamp = ShowMusicBedProfile.currentVersionStamp
        profile.updatedAt = now

        do {
            try context.save()
        } catch {
            logger.error("recordEpisodeOutcome save failed for show=\(showIdentifier, privacy: .public): \(error.localizedDescription, privacy: .public)")
        }

        return snapshot(from: profile)
    }

    @MainActor
    private static func fetchProfile(
        showIdentifier: String,
        context: ModelContext
    ) -> ShowMusicBedProfile? {
        // Keep `id` as a String (not Set/Optional) — the same predicate
        // translation rationale as `SwiftDataNewEpisodeAnnouncer.resolveCandidates`.
        let id = showIdentifier
        let descriptor = FetchDescriptor<ShowMusicBedProfile>(
            predicate: #Predicate { $0.showIdentifier == id }
        )
        do {
            return try context.fetch(descriptor).first
        } catch {
            logger.error("fetchProfile failed for show=\(showIdentifier, privacy: .public): \(error.localizedDescription, privacy: .public)")
            return nil
        }
    }

    @MainActor
    private static func snapshot(from row: ShowMusicBedProfile) -> ShowMusicBedProfileSnapshot {
        ShowMusicBedProfileSnapshot(
            showIdentifier: row.showIdentifier,
            confirmedJingleHashes: row.confirmedJingleHashBits.map { RepeatedAdFingerprint(bits: UInt64(bitPattern: $0)) },
            confirmationCount: row.confirmationCount,
            consecutiveMissCount: row.consecutiveMissCount,
            versionStamp: row.versionStamp,
            createdAt: row.createdAt,
            updatedAt: row.updatedAt
        )
    }
}
