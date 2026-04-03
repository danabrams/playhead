// SwiftDataStore.swift
// SwiftData model container and persistence configuration for podcasts,
// episodes, and user preferences.

import Foundation
import SwiftData

enum SwiftDataStore {
    static let schema = Schema([
        Podcast.self,
        Episode.self,
        UserPreferences.self,
    ])

    static let modelConfiguration = ModelConfiguration(
        "Playhead",
        schema: schema,
        isStoredInMemoryOnly: false
    )

    @MainActor
    static func makeContainer() throws -> ModelContainer {
        try ModelContainer(
            for: schema,
            migrationPlan: PlayheadMigrationPlan.self,
            configurations: [modelConfiguration]
        )
    }
}

// MARK: - Migration

enum PlayheadSchemaV1: VersionedSchema {
    static var versionIdentifier: Schema.Version { Schema.Version(1, 0, 0) }

    static var models: [any PersistentModel.Type] {
        [Podcast.self, Episode.self, UserPreferences.self]
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
