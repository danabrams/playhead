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
        ])
    }

    @MainActor
    static func makeContainer() throws -> ModelContainer {
        let schema = Self.schema
        let modelConfiguration = ModelConfiguration(
            "Playhead",
            schema: schema,
            isStoredInMemoryOnly: false
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
        [Podcast.self, Episode.self, UserPreferences.self, InstallIdentity.self]
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
