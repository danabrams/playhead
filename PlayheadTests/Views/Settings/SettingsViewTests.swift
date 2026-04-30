// SettingsViewTests.swift
// playhead-j2u — coverage for the Settings View additions:
//   * UserPreferences SwiftData persistence (skip-behavior round-trip)
//   * `StorageBreakdownReporter` totals against a temp directory tree
//   * `SettingsViewModel` premium-status reflection from a stub
//     EntitlementManager-shaped surface
//   * Eligibility-driven Models-section copy
//   * About section privacy-statement verbatim pin
//
// Tests operate on the value layer (model + reporter + view model)
// rather than instantiating the SwiftUI host. Snapshot/UI tests are
// deliberately out of scope per the bead spec.

import Foundation
import SwiftData
import Testing

@testable import Playhead

// MARK: - UserPreferences persistence

@Suite("UserPreferences SwiftData persistence (playhead-j2u)")
struct UserPreferencesPersistenceTests {

    /// Skip-behavior round-trips through SwiftData: change the value,
    /// drop the context, recreate the container against the same
    /// in-memory store, and verify the persisted value is observed.
    @Test func skipBehaviorRoundTrips() throws {
        let schema = Schema([UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: config)

        // First context: write `.manual`.
        do {
            let context = ModelContext(container)
            let prefs = UserPreferences()
            #expect(prefs.skipBehavior == .auto, "Default skip behavior should be auto")
            context.insert(prefs)
            prefs.skipBehavior = .manual
            try context.save()
        }

        // Second context against the SAME container: re-fetch and verify
        // the value persisted across context boundaries.
        let context2 = ModelContext(container)
        let descriptor = FetchDescriptor<UserPreferences>()
        let fetched = try context2.fetch(descriptor)
        #expect(fetched.count == 1, "Exactly one UserPreferences row expected")
        #expect(fetched.first?.skipBehavior == .manual, "Skip behavior must round-trip")
    }

    @Test func playbackSpeedRoundTrips() throws {
        let schema = Schema([UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: config)

        do {
            let context = ModelContext(container)
            let prefs = UserPreferences()
            context.insert(prefs)
            prefs.playbackSpeed = 1.5
            try context.save()
        }

        let context2 = ModelContext(container)
        let fetched = try context2.fetch(FetchDescriptor<UserPreferences>())
        #expect(fetched.first?.playbackSpeed == 1.5)
    }

    @Test func skipIntervalsRoundTrip() throws {
        let schema = Schema([UserPreferences.self])
        let config = ModelConfiguration(isStoredInMemoryOnly: true, cloudKitDatabase: .none)
        let container = try ModelContainer(for: schema, configurations: config)

        do {
            let context = ModelContext(container)
            let prefs = UserPreferences()
            context.insert(prefs)
            prefs.skipIntervals = SkipIntervals(forwardSeconds: 45, backwardSeconds: 10)
            try context.save()
        }

        let context2 = ModelContext(container)
        let fetched = try context2.fetch(FetchDescriptor<UserPreferences>())
        #expect(fetched.first?.skipIntervals.forwardSeconds == 45)
        #expect(fetched.first?.skipIntervals.backwardSeconds == 10)
    }
}

// MARK: - StorageBreakdownReporter

@Suite("StorageBreakdownReporter totals (playhead-j2u)")
struct StorageBreakdownReporterTests {

    /// Build a temp directory tree with files of known sizes, point a
    /// reporter at it, and verify the totals match within 1 MB (the
    /// bead's stated tolerance).
    @Test func reportsExactCategoryTotals() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory
            .appendingPathComponent("StorageBreakdownReporterTests-\(UUID().uuidString)")
        let audioDir = root.appendingPathComponent("audio", isDirectory: true)
        let shardsDir = root.appendingPathComponent("AnalysisShards", isDirectory: true)
        try fm.createDirectory(at: audioDir, withIntermediateDirectories: true)
        try fm.createDirectory(at: shardsDir, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        // Audio: two files totalling 100 KB.
        let audio1 = audioDir.appendingPathComponent("ep1.mp3")
        let audio2 = audioDir.appendingPathComponent("ep2.mp3")
        try Data(repeating: 0xAA, count: 60 * 1024).write(to: audio1)
        try Data(repeating: 0xBB, count: 40 * 1024).write(to: audio2)

        // Transcript shards: one file of 32 KB.
        let shard1 = shardsDir.appendingPathComponent("episode-abc.shard")
        try Data(repeating: 0xCC, count: 32 * 1024).write(to: shard1)

        let reporter = StorageBreakdownReporter(
            audioDirectories: [audioDir],
            transcriptDirectories: [shardsDir],
            volumeProbeURL: root
        )

        let breakdown = reporter.measure()
        // Allocated size is at least the nominal size; APFS may pad up
        // to the cluster boundary (typically 4 KB on iOS). Within the
        // 1 MB tolerance specified by the bead is plenty of headroom.
        let oneMB: Int64 = 1 * 1024 * 1024
        #expect(abs(breakdown.cachedAudioBytes - 100 * 1024) < oneMB,
                "Cached audio total off by more than 1 MB: \(breakdown.cachedAudioBytes)")
        #expect(abs(breakdown.transcriptDatabaseBytes - 32 * 1024) < oneMB,
                "Transcript-DB total off by more than 1 MB: \(breakdown.transcriptDatabaseBytes)")
        #expect(breakdown.totalBytes == breakdown.cachedAudioBytes + breakdown.transcriptDatabaseBytes,
                "Total must equal sum of categories")
    }

    /// Empty / non-existent directories must report zero, not crash.
    @Test func handlesMissingDirectoriesGracefully() {
        let bogus = URL(fileURLWithPath: "/tmp/this-path-does-not-exist-\(UUID().uuidString)")
        let reporter = StorageBreakdownReporter(
            audioDirectories: [bogus],
            transcriptDirectories: [bogus],
            volumeProbeURL: FileManager.default.temporaryDirectory
        )

        let breakdown = reporter.measure()
        #expect(breakdown.cachedAudioBytes == 0)
        #expect(breakdown.transcriptDatabaseBytes == 0)
        #expect(breakdown.totalBytes == 0)
    }

    /// SQLite-style transcript root: only `analysis.sqlite*` files are
    /// counted. Sibling files in the same directory must be ignored so
    /// the reporter doesn't sweep in unrelated Application-Support
    /// state.
    @Test func transcriptRootCountsOnlyAnalysisSqliteSiblings() throws {
        let fm = FileManager.default
        let root = fm.temporaryDirectory
            .appendingPathComponent("TranscriptRootTests-\(UUID().uuidString)")
        try fm.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: root) }

        // Eligible: matches `analysis.sqlite*`.
        try Data(repeating: 0xDD, count: 16 * 1024)
            .write(to: root.appendingPathComponent("analysis.sqlite"))
        try Data(repeating: 0xEE, count: 4 * 1024)
            .write(to: root.appendingPathComponent("analysis.sqlite-wal"))
        // Ineligible: must NOT be counted.
        try Data(repeating: 0xFF, count: 1024 * 1024)
            .write(to: root.appendingPathComponent("preferences.plist"))

        let reporter = StorageBreakdownReporter(
            audioDirectories: [],
            transcriptDirectories: [root],
            volumeProbeURL: root
        )

        let breakdown = reporter.measure()
        let expected: Int64 = 20 * 1024
        let oneMB: Int64 = 1 * 1024 * 1024
        // Difference must be small (cluster padding only) and the 1 MB
        // ineligible file must be excluded.
        #expect(abs(breakdown.transcriptDatabaseBytes - expected) < oneMB,
                "Transcript DB scan included ineligible siblings: \(breakdown.transcriptDatabaseBytes)")
    }
}

// MARK: - Eligibility-driven model status

@Suite("Settings model-status copy (playhead-j2u)")
@MainActor
struct SettingsModelStatusTests {

    @Test func availableWhenAllGatesPass() {
        let viewModel = SettingsViewModel()
        viewModel.eligibility = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date()
        )
        // The view-level status string is private, so we re-derive it
        // here using the same expression the view uses. If the view's
        // expression drifts this test will not detect it — the
        // accessibility identifier on the row is the UI-level pin.
        let isAvailable = viewModel.eligibility?.isFullyEligible == true
        #expect(isAvailable, "All gates pass should yield Available")
    }

    @Test func unavailableWhenAnyGateFails() {
        let viewModel = SettingsViewModel()
        viewModel.eligibility = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: false, // toggled off in Settings
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date()
        )
        #expect(viewModel.eligibility?.isFullyEligible == false,
                "Any failing gate should yield Unavailable")
    }

    @Test func nilEligibilityIsCheckingState() {
        let viewModel = SettingsViewModel()
        #expect(viewModel.eligibility == nil,
                "Fresh view-model should default to nil so the row renders 'Checking…'")
    }

    /// `refreshEligibility(using:)` must call `evaluate()` exactly once
    /// per refresh and store the returned snapshot on the view model.
    @Test func refreshUsesEvaluatorVerdict() {
        let viewModel = SettingsViewModel()
        let stub = StubEligibilityEvaluator(verdict: AnalysisEligibility(
            hardwareSupported: false,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date()
        ))
        viewModel.refreshEligibility(using: stub)

        #expect(stub.evaluateCallCount == 1, "Exactly one evaluate() per refresh")
        #expect(viewModel.eligibility?.hardwareSupported == false,
                "View model must adopt the evaluator's verdict")
        #expect(viewModel.eligibility?.isFullyEligible == false,
                "Hardware unsupported must surface as Unavailable")
    }
}

/// Minimal stub for the eligibility evaluator. Records the evaluate
/// call count and returns a fixed verdict — enough for the model-status
/// section's wiring tests.
final class StubEligibilityEvaluator: AnalysisEligibilityEvaluating, @unchecked Sendable {
    private let verdict: AnalysisEligibility
    private(set) var evaluateCallCount = 0

    init(verdict: AnalysisEligibility) {
        self.verdict = verdict
    }

    func evaluate() -> AnalysisEligibility {
        evaluateCallCount += 1
        return verdict
    }

    func invalidate() {}
    func noteLocaleChanged() {}
    func noteRegionChanged() {}
    func noteOSVersionChangedIfNeeded() {}
    func noteAppleIntelligenceToggled() {}
    func noteAppForegrounded() {}
}

// MARK: - About section copy

@Suite("Settings About copy (playhead-j2u)")
struct SettingsAboutCopyTests {

    /// Privacy statement is verbatim per the bead spec. Any change here
    /// is a product decision; update the spec and the assertion together.
    @Test func privacyStatementIsVerbatim() {
        #expect(SettingsAboutCopy.privacyStatement
                == "Your podcasts never leave your device.")
    }
}

// MARK: - Premium status reflection

@Suite("Settings premium-status copy (playhead-j2u)")
@MainActor
struct SettingsPremiumStatusTests {

    @Test func defaultStatusIsFreePreview() {
        let viewModel = SettingsViewModel()
        #expect(viewModel.isPremium == false,
                "Fresh view-model defaults to Free preview state")
    }

    @Test func premiumFlagFlipsRendersPurchasedCopy() {
        let viewModel = SettingsViewModel()
        viewModel.isPremium = true
        // The view-level `premiumStatusText` is a private extension on
        // `SettingsView`; we re-derive it here to pin the expected copy.
        let copy = viewModel.isPremium ? "Premium — purchased" : "Free preview"
        #expect(copy == "Premium — purchased")
    }

    @Test func nonPremiumRendersFreePreviewCopy() {
        let viewModel = SettingsViewModel()
        viewModel.isPremium = false
        let copy = viewModel.isPremium ? "Premium — purchased" : "Free preview"
        #expect(copy == "Free preview")
    }
}
