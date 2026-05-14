// PerPodcastAutoDownloadOverrideTests.swift
// playhead-5w4 — Per-podcast auto-download override (Downloads polish).
//
// Pin the four contracts the bead acceptance criteria require:
//   1. Migration default: new + previously-stored `Podcast` rows decode
//      with `autoDownloadOverride == nil` (inherit the global setting).
//   2. Effective policy resolver returns `override ?? global` on both
//      branches.
//   3. Round-trip persistence: a non-nil override survives a
//      ModelContainer save/fetch cycle (the persistence half of the
//      "persists across app launches" acceptance criterion).
//   4. UI helpers — `AutoDownloadOverrideSelection` and
//      `AutoDownloadOverrideMenu.inheritLabel(global:)` — render the
//      verbatim copy and bridge nil <-> .inherit losslessly so the
//      picker's tag round-trips without dropping the inherit state.
//
// The wiring tests (refresh handler resolves override per-feed) live in
// `BackgroundFeedRefreshServiceTests.swift` because they exercise the
// auto-download path actor and would duplicate setup if split out.

import Foundation
import SwiftData
import Testing

@testable import Playhead

// MARK: - Model field + migration default

@Suite("Podcast.autoDownloadOverride schema (playhead-5w4)")
@MainActor
struct PodcastAutoDownloadOverrideSchemaTests {

    @Test("Podcast.autoDownloadOverride defaults to nil on a freshly-constructed row")
    func defaultIsNil() {
        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "T",
            author: "A"
        )
        #expect(podcast.autoDownloadOverride == nil,
                "Default must be nil so existing subscriptions keep inheriting the global setting")
    }

    @Test("Existing in-memory row (no explicit override) decodes with nil after a save/fetch round-trip")
    func roundTripDefaultIsNil() throws {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(
            "PodcastAutoDownloadOverride.defaultRoundTrip",
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(for: schema, configurations: [config])
        let context = container.mainContext

        // Construct WITHOUT supplying autoDownloadOverride — this is
        // the migration shape: existing rows in a pre-5w4 store had
        // no such column and SwiftData lightweight migration fills
        // in the optional as nil.
        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "T",
            author: "A"
        )
        context.insert(podcast)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<Podcast>()).first
        #expect(fetched?.autoDownloadOverride == nil,
                "Migration default for pre-existing rows must be nil")
    }

    @Test("A non-nil override survives a SwiftData round-trip")
    func nonNilOverrideRoundTrips() throws {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(
            "PodcastAutoDownloadOverride.nonNilRoundTrip",
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(for: schema, configurations: [config])
        let context = container.mainContext

        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "T",
            author: "A"
        )
        podcast.autoDownloadOverride = .last1
        context.insert(podcast)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<Podcast>()).first
        #expect(fetched?.autoDownloadOverride == .last1,
                "A user's per-show pick must survive a save/relaunch cycle")
    }

    @Test("Setting override back to nil after a non-nil value re-engages inherit")
    func clearingOverrideRoundTrips() throws {
        let schema = Schema([Podcast.self, Episode.self, UserPreferences.self])
        let config = ModelConfiguration(
            "PodcastAutoDownloadOverride.clearRoundTrip",
            schema: schema,
            isStoredInMemoryOnly: true,
            cloudKitDatabase: .none
        )
        let container = try ModelContainer(for: schema, configurations: [config])
        let context = container.mainContext

        let podcast = Podcast(
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "T",
            author: "A"
        )
        podcast.autoDownloadOverride = .all
        context.insert(podcast)
        try context.save()

        podcast.autoDownloadOverride = nil
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<Podcast>()).first
        #expect(fetched?.autoDownloadOverride == nil,
                "Clearing the override must return the row to the inherit-global default")
    }
}

// MARK: - Effective policy resolver

@Suite("AutoDownloadOnSubscribe.effective(override:global:) (playhead-5w4)")
struct AutoDownloadEffectiveResolverTests {

    @Test("Nil override falls back to the global setting")
    func nilOverrideUsesGlobal() {
        for global in AutoDownloadOnSubscribe.allCases {
            let effective = AutoDownloadOnSubscribe.effective(
                override: nil,
                global: global
            )
            #expect(effective == global,
                    "Nil override must mirror the global value (\(global.rawValue))")
        }
    }

    @Test("Non-nil override ignores the global setting")
    func nonNilOverrideIgnoresGlobal() {
        // Cartesian product: every override paired with every global.
        // The override always wins.
        for override in AutoDownloadOnSubscribe.allCases {
            for global in AutoDownloadOnSubscribe.allCases {
                let effective = AutoDownloadOnSubscribe.effective(
                    override: override,
                    global: global
                )
                #expect(effective == override,
                        "Override .\(override.rawValue) must win over global .\(global.rawValue)")
            }
        }
    }

    @Test("Override .off with global .all still resolves to .off (canonical user story)")
    func userStoryOffOverridesAll() {
        // The bead's motivating example: user has global "All" but
        // wants this noisy show to never auto-download.
        let effective = AutoDownloadOnSubscribe.effective(
            override: .off,
            global: .all
        )
        #expect(effective == .off)
    }

    @Test("Override .all with global .off still resolves to .all (canonical favorite story)")
    func userStoryAllOverridesOff() {
        // The bead's other motivating example: user has global "Off"
        // but wants this favorite to auto-download every new episode.
        let effective = AutoDownloadOnSubscribe.effective(
            override: .all,
            global: .off
        )
        #expect(effective == .all)
    }
}

// MARK: - UI helpers (picker selection + inherit label)

@Suite("AutoDownloadOverrideSelection bridge (playhead-5w4)")
struct AutoDownloadOverrideSelectionTests {

    @Test("init(nil) maps to .inherit")
    func nilMapsToInherit() {
        let selection = AutoDownloadOverrideSelection(nil)
        #expect(selection == .inherit)
        #expect(selection.override == nil)
    }

    @Test("init(.last3) maps to .override(.last3) and round-trips back")
    func nonNilRoundTrips() {
        for option in AutoDownloadOnSubscribe.allCases {
            let selection = AutoDownloadOverrideSelection(option)
            #expect(selection == .override(option))
            #expect(selection.override == option,
                    "Round-trip must preserve \(option.rawValue)")
        }
    }

    @Test(".inherit.override is nil")
    func inheritOverrideIsNil() {
        #expect(AutoDownloadOverrideSelection.inherit.override == nil)
    }
}

@Suite("AutoDownloadOverrideMenu copy (playhead-5w4)")
struct AutoDownloadOverrideMenuCopyTests {

    @Test("Menu title is verbatim")
    func menuTitleVerbatim() {
        #expect(AutoDownloadOverrideMenu.title == "Auto-Download")
    }

    @Test("Inherit label substitutes the user's current global setting")
    func inheritLabelSubstitutesGlobal() {
        #expect(
            AutoDownloadOverrideMenu.inheritLabel(global: .off)
                == "Inherit Global (Off)"
        )
        #expect(
            AutoDownloadOverrideMenu.inheritLabel(global: .last1)
                == "Inherit Global (Last 1)"
        )
        #expect(
            AutoDownloadOverrideMenu.inheritLabel(global: .last3)
                == "Inherit Global (Last 3)"
        )
        #expect(
            AutoDownloadOverrideMenu.inheritLabel(global: .all)
                == "Inherit Global (All)"
        )
    }
}
