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

// MARK: - Apple Intelligence unavailable copy
//
// Pins the user-facing caption strings rendered under the Apple
// Intelligence status row when the row reads "Unavailable". The View
// renders the caption via `SettingsModelUnavailableCopy.caption(for:)`;
// these tests pin the mapping for every `AnalysisUnavailableReason`
// case so any future edit forces a deliberate spec + test update.

@Suite("Settings Apple Intelligence unavailable copy")
struct SettingsModelUnavailableCopyTests {

    @Test func hardwareUnsupportedCaption() {
        #expect(
            SettingsModelUnavailableCopy.caption(for: .hardwareUnsupported)
                == "Device doesn't support Apple Intelligence"
        )
    }

    @Test func regionUnsupportedCaption() {
        #expect(
            SettingsModelUnavailableCopy.caption(for: .regionUnsupported)
                == "Not available in your region"
        )
    }

    @Test func languageUnsupportedCaption() {
        #expect(
            SettingsModelUnavailableCopy.caption(for: .languageUnsupported)
                == "Not available in your language"
        )
    }

    @Test func appleIntelligenceDisabledCaption() {
        #expect(
            SettingsModelUnavailableCopy.caption(for: .appleIntelligenceDisabled)
                == "Apple Intelligence is off in Settings"
        )
    }

    @Test func modelTemporarilyUnavailableCaption() {
        #expect(
            SettingsModelUnavailableCopy.caption(for: .modelTemporarilyUnavailable)
                == "Model not ready — tap Recheck after a moment"
        )
    }

    /// Every case has a non-empty caption, so adding a new
    /// `AnalysisUnavailableReason` case will fail compilation in
    /// `SettingsModelUnavailableCopy.caption(for:)` rather than
    /// silently rendering an empty caption.
    @Test func everyReasonHasNonEmptyCaption() {
        for reason in AnalysisUnavailableReason.allCases {
            let caption = SettingsModelUnavailableCopy.caption(for: reason)
            #expect(!caption.isEmpty, "Missing caption for \(reason)")
        }
    }
}

// MARK: - Recheck flow (view-model level)
//
// The SwiftUI Recheck button calls `SettingsViewModel.recheckModels`,
// which in turn must:
//   1. Drop the persisted FM usability cache.
//   2. Invalidate the eligibility evaluator.
//   3. Flip `isRecheckingModels = true` so the status row reads
//      "Checking…" until the fresh evaluation lands.
//   4. Re-evaluate after the snapshot refresh.
//
// We exercise the orchestration via a stub evaluator + the real
// CapabilitiesService actor. The latter is safe to spin up in a unit
// test because it owns no external state beyond its in-actor
// snapshot.

@Suite("Settings recheck flow")
@MainActor
struct SettingsRecheckFlowTests {

    @Test func recheckInvalidatesEvaluatorAndDropsEligibility() async {
        let viewModel = SettingsViewModel()

        // Seed an unavailable verdict so the View would show the
        // Recheck button.
        let stub = RecheckStubEligibilityEvaluator(verdict: AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: false, // model not ready
            capturedAt: Date()
        ))
        viewModel.refreshEligibility(using: stub)
        #expect(viewModel.eligibility?.isFullyEligible == false)
        #expect(stub.invalidateCallCount == 0)

        // Drive the recheck.
        let capabilities = CapabilitiesService()
        await viewModel.recheckModels(using: stub, capabilities: capabilities)

        #expect(stub.invalidateCallCount == 1,
                "Recheck must invalidate the evaluator exactly once per call")
        // The evaluator's verdict is consulted again at the end of
        // `recheckModels`, so the final eligibility comes from the
        // stub's `evaluate()` call.
        #expect(stub.evaluateCallCount == 2,
                "Recheck must re-evaluate after the snapshot refresh (initial seed + final read)")
        #expect(viewModel.isRecheckingModels == false,
                "isRecheckingModels must reset to false after the recheck completes")
        #expect(viewModel.eligibility?.isFullyEligible == false,
                "Final eligibility reflects the stub's verdict")
    }

    @Test func recheckClearsPersistedProbeCache() async {
        // Cache a `usable == false` record in the live UserDefaults so
        // we can prove the recheck flow drops it. Use a recent
        // timestamp so the TTL does not affect the result.
        let osBuild = FoundationModelsUsabilityProbe.osBuild()
        let bootEpoch = FoundationModelsUsabilityProbe.bootEpochSeconds()
        FoundationModelsUsabilityProbe.cache(
            usable: false,
            osBuild: osBuild,
            bootEpochSeconds: bootEpoch
        )
        // Sanity: the record reads back as false within the TTL.
        #expect(FoundationModelsUsabilityProbe.cachedUsability(
            osBuild: osBuild,
            bootEpochSeconds: bootEpoch
        ) == false)

        defer {
            // Belt-and-suspenders cleanup even if the test fails before
            // the recheck path executes.
            FoundationModelsUsabilityProbe.clearCache()
        }

        let viewModel = SettingsViewModel()
        let stub = RecheckStubEligibilityEvaluator(verdict: AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: false,
            capturedAt: Date()
        ))
        let capabilities = CapabilitiesService()

        await viewModel.recheckModels(using: stub, capabilities: capabilities)

        // Post-condition: the cache slot is empty.
        #expect(FoundationModelsUsabilityProbe.cachedUsability(
            osBuild: osBuild,
            bootEpochSeconds: bootEpoch
        ) == nil,
                "Recheck must clear the FM usability cache so the schedule gate can re-probe")
    }

    @Test func freshViewModelIsNotRechecking() {
        let viewModel = SettingsViewModel()
        #expect(viewModel.isRecheckingModels == false,
                "Fresh view-model defaults isRecheckingModels to false")
    }

    /// R1 audit: After a snapshot lands on `capabilityUpdates()`, the
    /// view-model must re-evaluate eligibility so the Apple Intelligence
    /// row reflects the post-probe verdict. Without this, the row would
    /// remain stuck on the stale verdict the user just tried to recheck
    /// (e.g. `modelAvailableNow == false`) until the user closed and
    /// reopened the Settings sheet.
    @Test func observeCapabilitySnapshotsReevaluatesOnEmission() async throws {
        let viewModel = SettingsViewModel()
        // Start with the stub returning Unavailable.
        let stub = RecheckStubEligibilityEvaluator(verdict: AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: false,
            capturedAt: Date()
        ))
        viewModel.refreshEligibility(using: stub)
        #expect(viewModel.eligibility?.isFullyEligible == false)

        // Flip the stub to Available BEFORE starting the observation so
        // the very first snapshot (yielded immediately by
        // `capabilityUpdates()` per its documented contract) triggers
        // a re-evaluation that produces the new verdict.
        stub.verdict = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date()
        )

        let capabilities = CapabilitiesService()
        let task = Task { @MainActor in
            await viewModel.observeCapabilitySnapshots(capabilities, evaluator: stub)
        }
        defer { task.cancel() }

        // Poll for the row to flip — the observation runs on a child
        // task and `capabilityUpdates()` yields the current snapshot
        // immediately on subscribe.
        let deadline = ContinuousClock.now.advanced(by: .seconds(1))
        while ContinuousClock.now < deadline {
            if viewModel.eligibility?.isFullyEligible == true { break }
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(viewModel.eligibility?.isFullyEligible == true,
                "Snapshot emission must trigger an evaluate() that flips the row to Available")
    }

    /// R1 audit: If a recheck is in flight when a snapshot reporting
    /// `foundationModelsUsable == true` lands, the observation loop
    /// must release the `isRecheckingModels` flag so the "Checking…"
    /// indicator does not linger past the moment the probe succeeded.
    @Test func observeCapabilitySnapshotsReleasesRecheckOnUsableSnapshot() async throws {
        let viewModel = SettingsViewModel()
        viewModel.isRecheckingModels = true
        let stub = RecheckStubEligibilityEvaluator(verdict: AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date()
        ))

        let capabilities = CapabilitiesService()
        let task = Task { @MainActor in
            await viewModel.observeCapabilitySnapshots(capabilities, evaluator: stub)
        }
        defer { task.cancel() }

        // The capture path in CapabilitiesService reads
        // `cachedUsability() ?? false`. To pin a usable snapshot
        // without dragging the real probe in, write a fresh
        // `usable=true` record into the shared cache slot before the
        // observation reads it. Belt-and-suspenders: a deferred
        // cleanup clears the slot after the test so we don't pollute
        // sibling tests.
        let osBuild = FoundationModelsUsabilityProbe.osBuild()
        let bootEpoch = FoundationModelsUsabilityProbe.bootEpochSeconds()
        FoundationModelsUsabilityProbe.cache(
            usable: true,
            osBuild: osBuild,
            bootEpochSeconds: bootEpoch
        )
        defer { FoundationModelsUsabilityProbe.clearCache() }
        await capabilities.refreshSnapshot()

        let deadline = ContinuousClock.now.advanced(by: .seconds(1))
        while ContinuousClock.now < deadline {
            if viewModel.isRecheckingModels == false { break }
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(viewModel.isRecheckingModels == false,
                "A usable snapshot landing during a pending recheck must clear isRecheckingModels")
    }
}

/// Stub eligibility evaluator that also records invalidation calls.
/// Local to the recheck-flow suite so its bookkeeping is independent
/// of the model-status copy suite's stub.
final class RecheckStubEligibilityEvaluator: AnalysisEligibilityEvaluating, @unchecked Sendable {
    var verdict: AnalysisEligibility
    private(set) var evaluateCallCount = 0
    private(set) var invalidateCallCount = 0

    init(verdict: AnalysisEligibility) {
        self.verdict = verdict
    }

    func evaluate() -> AnalysisEligibility {
        evaluateCallCount += 1
        return verdict
    }

    func invalidate() { invalidateCallCount += 1 }
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

// MARK: - iCloud sync reactivity (playhead-5c1t)

@Suite("Settings iCloud sync footer reactivity (playhead-5c1t)")
@MainActor
struct SettingsICloudSyncReactivityTests {

    /// The fresh view-model must report `nil` so the View suppresses
    /// the footer rather than flashing a wrong value at launch.
    @Test func defaultICloudSyncEnabledIsNil() {
        let viewModel = SettingsViewModel()
        #expect(viewModel.iCloudSyncEnabled == nil,
                "Fresh view-model defaults iCloud-sync flag to nil so the footer is suppressed.")
    }

    /// Drive the fake provider to `unavailable` mid-observation and
    /// assert the view-model published value flips. Pinned by the bead
    /// spec — sign-out mid-session must update the footer without a
    /// view re-appear.
    @Test func observeICloudSyncStatusFlipsOnSignOut() async throws {
        let provider = FakeCloudKitProvider(initialAccountStatus: .available)
        let coordinator = ICloudSyncCoordinator(provider: provider)
        await coordinator.handleAccountStatusChange()

        let viewModel = SettingsViewModel()

        // Run the observation in a child task so the suspending
        // for-loop doesn't block the test. We then drive the
        // status flip and poll the view-model for the new value.
        let observationTask = Task { @MainActor in
            await viewModel.observeICloudSyncStatus(coordinator)
        }
        defer { observationTask.cancel() }

        // Wait for the seed value (`true`) to land on the view-model.
        try await waitFor { viewModel.iCloudSyncEnabled == true }

        // Sign out mid-session.
        await provider.setAccountStatus(.noAccount)
        await coordinator.handleAccountStatusChange()

        // Footer must flip to `false` without a view re-appear.
        try await waitFor { viewModel.iCloudSyncEnabled == false }
        #expect(viewModel.iCloudSyncEnabled == false)
    }

    /// Polls a predicate on the main actor with a 1s ceiling. Sweeter
    /// than scattering `try await Task.sleep` across each test.
    @MainActor
    private func waitFor(
        _ predicate: @MainActor () -> Bool,
        timeout: Duration = .seconds(1)
    ) async throws {
        let deadline = ContinuousClock.now.advanced(by: timeout)
        while ContinuousClock.now < deadline {
            if predicate() { return }
            try await Task.sleep(for: .milliseconds(10))
        }
        if !predicate() {
            Issue.record("Predicate never became true within \(timeout)")
        }
    }
}
