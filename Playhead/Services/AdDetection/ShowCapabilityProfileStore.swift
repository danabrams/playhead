// ShowCapabilityProfileStore.swift
// playhead-h6a6: SwiftData persistence seam for `ShowCapabilityProfile`.
//
// Mirrors `ShowMusicBedProfileStore` (playhead-2hpn) so a reader who
// has seen the music-bed seam has seen this one. Hops to the MainActor
// to touch `ModelContext`; exposes Sendable async methods to the
// off-main `AdDetectionService` consumer.
//
// Why a wrapper (not direct ModelContext access from the consumer):
//   * The consumer (`AdDetectionService`) is an actor — pulling
//     SwiftData reads across actor boundaries forces a hop.
//     Centralising the hop here keeps each call site one `await`.
//   * Tests inject a synthetic `ShowCapabilityProfileResolving` to
//     exercise the budget-modulation path without spinning up
//     SwiftData.
//
// Thread/actor model:
//   * `ShowCapabilityProfileResolving` is the abstract Sendable seam.
//   * `ShowCapabilityProfileStore` is the SwiftData-backed conformer.
//     The struct itself is Sendable (only stored field is a
//     ModelContainer which is Sendable). Each method jumps to
//     MainActor internally.

import Foundation
import OSLog
import SwiftData

// MARK: - Abstract seam

/// Read/write contract consumed by `AdDetectionService`. Sendable so
/// it can cross actor boundaries without copying any mutable state.
protocol ShowCapabilityProfileResolving: Sendable {

    /// Returns the current profile snapshot for `showIdentifier`, or
    /// `nil` when the show has never been observed. The snapshot is a
    /// value type — mutating it has no effect on storage.
    func snapshot(showIdentifier: String) async -> ShowCapabilityProfileSnapshot?

    /// Apply this episode's `outcome` to the show's counters and
    /// derive the new profile kind. Creates the row if missing. The
    /// caller supplies:
    ///   * `musicBedConfirmed` — 2hpn's `isConfirmed` for this show.
    ///   * `sliGate` — Phase-2 SLI bounds predicate.
    /// Returns the post-update snapshot so the caller can log /
    /// instrument without a second read.
    @discardableResult
    func recordEpisodeOutcome(
        showIdentifier: String,
        outcome: ShowCapabilityEpisodeOutcome,
        musicBedConfirmed: Bool,
        sliGate: ShowCapabilitySLIGate,
        now: Date
    ) async -> ShowCapabilityProfileSnapshot

    /// Returns snapshots for ALL stored profiles. Used by the
    /// diagnostics bundle and tests. Empty when no profile has ever
    /// been written.
    func allSnapshots() async -> [ShowCapabilityProfileSnapshot]
}

// MARK: - Value snapshot

/// Immutable value-type view of a `ShowCapabilityProfile`. Crossing
/// the actor boundary as a value avoids any SwiftData-row aliasing
/// pitfalls (the @Model is `final class`, not Sendable).
struct ShowCapabilityProfileSnapshot: Sendable, Equatable {
    let showIdentifier: String
    let completedEpisodeCount: Int
    let chapterMatchedEpisodeCount: Int
    let hostVoicedEpisodeCount: Int
    let sponsorDeclaredEpisodeCount: Int
    let dynamicInsertionEpisodeCount: Int
    let kind: ShowCapabilityProfileKind
    let schemaVersion: Int
    let createdAt: Date
    let updatedAt: Date

    /// True iff the persisted row currently records a non-`.unknown`
    /// kind. The activation floor + SLI gate were already enforced at
    /// the most recent write — this is a cheap read-side check used
    /// by the Diagnostics row and the budget modulator.
    var isObserved: Bool {
        kind != .unknown
    }
}

// MARK: - SwiftData-backed conformer

/// Production conformer. Holds a `ModelContainer` and hops to the
/// MainActor for every method.
struct ShowCapabilityProfileStore: ShowCapabilityProfileResolving {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "ShowCapabilityProfileStore"
    )

    private let modelContainer: ModelContainer

    init(modelContainer: ModelContainer) {
        self.modelContainer = modelContainer
    }

    func snapshot(showIdentifier: String) async -> ShowCapabilityProfileSnapshot? {
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
        outcome: ShowCapabilityEpisodeOutcome,
        musicBedConfirmed: Bool,
        sliGate: ShowCapabilitySLIGate,
        now: Date
    ) async -> ShowCapabilityProfileSnapshot {
        await MainActor.run { [modelContainer] in
            Self.recordOutcome(
                showIdentifier: showIdentifier,
                outcome: outcome,
                musicBedConfirmed: musicBedConfirmed,
                sliGate: sliGate,
                now: now,
                context: modelContainer.mainContext
            )
        }
    }

    func allSnapshots() async -> [ShowCapabilityProfileSnapshot] {
        await MainActor.run { [modelContainer] in
            Self.fetchAllSnapshots(context: modelContainer.mainContext)
        }
    }

    // MARK: - MainActor implementation

    @MainActor
    private static func fetchSnapshot(
        showIdentifier: String,
        context: ModelContext
    ) -> ShowCapabilityProfileSnapshot? {
        guard let row = fetchProfile(showIdentifier: showIdentifier, context: context) else {
            return nil
        }
        return snapshot(from: row)
    }

    @MainActor
    private static func fetchAllSnapshots(
        context: ModelContext
    ) -> [ShowCapabilityProfileSnapshot] {
        let descriptor = FetchDescriptor<ShowCapabilityProfile>()
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
        outcome: ShowCapabilityEpisodeOutcome,
        musicBedConfirmed: Bool,
        sliGate: ShowCapabilitySLIGate,
        now: Date,
        context: ModelContext
    ) -> ShowCapabilityProfileSnapshot {
        let existing = fetchProfile(showIdentifier: showIdentifier, context: context)
        let profile = existing ?? ShowCapabilityProfile(
            showIdentifier: showIdentifier,
            createdAt: now,
            updatedAt: now
        )
        if existing == nil {
            context.insert(profile)
        }

        let mutation = ShowCapabilityProfileEvaluator.apply(
            outcome: outcome,
            showIdentifier: showIdentifier,
            priorCompletedEpisodeCount: profile.completedEpisodeCount,
            priorChapterMatchedEpisodeCount: profile.chapterMatchedEpisodeCount,
            priorHostVoicedEpisodeCount: profile.hostVoicedEpisodeCount,
            priorSponsorDeclaredEpisodeCount: profile.sponsorDeclaredEpisodeCount,
            priorDynamicInsertionEpisodeCount: profile.dynamicInsertionEpisodeCount,
            musicBedConfirmed: musicBedConfirmed,
            sliGate: sliGate
        )

        profile.completedEpisodeCount = mutation.completedEpisodeCount
        profile.chapterMatchedEpisodeCount = mutation.chapterMatchedEpisodeCount
        profile.hostVoicedEpisodeCount = mutation.hostVoicedEpisodeCount
        profile.sponsorDeclaredEpisodeCount = mutation.sponsorDeclaredEpisodeCount
        profile.dynamicInsertionEpisodeCount = mutation.dynamicInsertionEpisodeCount
        profile.kindRawValue = mutation.kind.rawValue
        profile.schemaVersion = ShowCapabilityProfile.currentSchemaVersion
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
    ) -> ShowCapabilityProfile? {
        // Same `id`-as-local rationale as `ShowMusicBedProfileStore`
        // and `SwiftDataNewEpisodeAnnouncer`: the predicate translator
        // is happier with a String binding than a closure capture.
        let id = showIdentifier
        let descriptor = FetchDescriptor<ShowCapabilityProfile>(
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
    private static func snapshot(from row: ShowCapabilityProfile) -> ShowCapabilityProfileSnapshot {
        let kind = ShowCapabilityProfileKind(rawValue: row.kindRawValue) ?? .unknown
        return ShowCapabilityProfileSnapshot(
            showIdentifier: row.showIdentifier,
            completedEpisodeCount: row.completedEpisodeCount,
            chapterMatchedEpisodeCount: row.chapterMatchedEpisodeCount,
            hostVoicedEpisodeCount: row.hostVoicedEpisodeCount,
            sponsorDeclaredEpisodeCount: row.sponsorDeclaredEpisodeCount,
            dynamicInsertionEpisodeCount: row.dynamicInsertionEpisodeCount,
            kind: kind,
            schemaVersion: row.schemaVersion,
            createdAt: row.createdAt,
            updatedAt: row.updatedAt
        )
    }
}
