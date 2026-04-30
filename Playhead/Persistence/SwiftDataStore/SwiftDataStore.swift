// SwiftDataStore.swift
// SwiftData model container and persistence configuration for podcasts,
// episodes, and user preferences.

import Foundation
import SwiftData

enum SwiftDataStore {
    static var schema: Schema {
        Schema([
            Podcast.self,
            Episode.self,
            UserPreferences.self,
            InstallIdentity.self,
            // playhead-zp0x: persisted batch state for the trip-ready /
            // action-required notification reducer. Survives app
            // suspension/restart so an overnight-completed download
            // still fires `tripReady`.
            DownloadBatch.self,
            // playhead-05i: persisted playback queue. Distinct from
            // `Episode.queuePosition` (analysis-priority ordering on the
            // Activity screen, playhead-cjqq) — this is the user-facing
            // "Up Next" playback queue that drives auto-advance.
            QueueEntry.self,
        ])
    }

    @MainActor
    static func makeContainer() throws -> ModelContainer {
        let schema = Self.schema
        // playhead-5c1t: when the app's iCloud capability is enabled
        // (entitlements file + `com.apple.developer.icloud-services
        // = CloudKit`), SwiftData attempts to opt the entire schema
        // into Core Data + CloudKit auto-mirroring at container-load
        // time. That mirror has stricter constraints than our schema
        // satisfies (every attribute must be optional or have a
        // default; relationships must be optional; unique constraints
        // are forbidden) — and would bake on-device-only content
        // (transcripts, analysis, episode positions) into a CloudKit
        // private DB without our consent, violating the on-device
        // mandate.
        //
        // Setting `cloudKitDatabase: .none` opts out and keeps SwiftData
        // purely on-device. iCloud sync for SUBSCRIPTIONS + ENTITLEMENT
        // lives behind a separate hand-written CloudKit pipeline
        // (`ICloudSyncCoordinator` in Services/iCloudSync) so we control
        // exactly what crosses the device boundary.
        let modelConfiguration = ModelConfiguration(
            "Playhead",
            schema: schema,
            isStoredInMemoryOnly: false,
            cloudKitDatabase: .none
        )
        return try ModelContainer(
            for: schema,
            migrationPlan: PlayheadMigrationPlan.self,
            configurations: [modelConfiguration]
        )
    }
}

// MARK: - Migration

// MIGRATION WARNING: PlayheadSchemaV1.models currently references LIVE types
// (Podcast.self, Episode.self, UserPreferences.self). Before creating a V2
// schema, these MUST be replaced with frozen type snapshots so that V1's
// definition never changes when the live models evolve. For example:
//
//     typealias PodcastV1 = Podcast   // snapshot of Podcast at V1
//     static var models: [any PersistentModel.Type] { [PodcastV1.self, ...] }
//
// Failing to freeze will silently corrupt lightweight migration diffs.
enum PlayheadSchemaV1: VersionedSchema {
    static var versionIdentifier: Schema.Version { Schema.Version(1, 0, 0) }

    static var models: [any PersistentModel.Type] {
        [
            Podcast.self,
            Episode.self,
            UserPreferences.self,
            InstallIdentity.self,
            // playhead-zp0x: schema is still V1 (zero migration stages
            // declared below). Adding to V1 is acceptable here because
            // there is nothing to migrate FROM. Once V2 is introduced
            // we MUST replace these with frozen type snapshots — see
            // the MIGRATION WARNING above.
            //
            // playhead-cjqq: `Episode.queuePosition: Int?` was added as
            // an additive optional field on the live `Episode` type.
            // No schema list change required — existing rows decode
            // with `queuePosition == nil` and inherit the provider's
            // natural ordering until a drag-reorder writes through.
            DownloadBatch.self,
            // playhead-05i: additive new entity for the user-facing
            // playback queue ("Up Next"). Adding to V1 is acceptable
            // because there is nothing to migrate FROM — existing
            // installs simply observe an empty queue table on first
            // launch. Once V2 is introduced these references must be
            // replaced with frozen type snapshots per the warning above.
            QueueEntry.self,
        ]
    }
}

enum PlayheadMigrationPlan: SchemaMigrationPlan {
    static var schemas: [any VersionedSchema.Type] {
        [PlayheadSchemaV1.self]
    }

    static var stages: [MigrationStage] {
        []
    }
}
