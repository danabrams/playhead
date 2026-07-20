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

// `.serialized` because several cases here both write to and read
// from the shared `.standard` UserDefaults slot used by
// `FoundationModelsUsabilityProbe`. Without serialization, the
// `observeCapabilitySnapshotsReleasesRecheckOnUsableSnapshot` case
// can write `usable=true` while `recheckClearsPersistedProbeCache`
// is asserting the slot has been emptied, producing a non-
// deterministic failure. Routing through `.standard` is dictated by
// `CapabilitiesService.captureSnapshot()` (it reads `cachedUsability()`
// with the default UserDefaults); the cleanest reader for the
// snapshot flag in a unit test is the same writer, so serialization
// is the right tradeoff over plumbing a UserDefaults parameter
// through the production service for test-only use.
@Suite("Settings recheck flow", .serialized)
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
    ///
    /// playhead-zqhz: stream-driven, not timeout-driven. The scripted
    /// provider yields exactly one snapshot and finishes, so awaiting
    /// `observeCapabilitySnapshots` directly processes the emission on
    /// THIS task and returns — no child task, no polling deadline, no
    /// dependence on MainActor scheduling latency under suite load.
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

        // Flip the stub to Available BEFORE the observation runs so the
        // single scripted snapshot triggers a re-evaluation that
        // produces the new verdict.
        stub.verdict = AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date()
        )

        let provider = ScriptedCapabilitiesProvider(snapshots: [
            makeCapabilitySnapshot(foundationModelsAvailable: true)
        ])
        await viewModel.observeCapabilitySnapshots(provider, evaluator: stub)

        #expect(viewModel.eligibility?.isFullyEligible == true,
                "Snapshot emission must trigger an evaluate() that flips the row to Available")
    }

    /// R2 audit: The observation loop must invalidate the evaluator
    /// before calling `evaluate()` on every emission. Without this, the
    /// evaluator's 4-hour verdict cache races with the runtime-level
    /// subscription in `PlayheadRuntime`: if the SettingsView's
    /// observation wins, `evaluate()` returns the STALE cached verdict
    /// computed against the previous snapshot, and the row stays
    /// stuck on the pre-probe value indefinitely (until something
    /// else triggers a snapshot AND that race goes the other way).
    /// The fix is for the local observer to invalidate before
    /// evaluating, guaranteeing a fresh provider sweep against the
    /// snapshot we just received.
    ///
    /// R3 audit: this test pins the ORDER of calls, not just the
    /// counts. A refactor that swapped the order to
    /// `evaluate(); invalidate()` would still satisfy a count-only
    /// assertion (each iteration increments both counters), silently
    /// breaking the race fix.
    ///
    /// playhead-zqhz: stream-driven. The scripted provider yields
    /// exactly TWO snapshots and finishes, so after awaiting the
    /// observation the stub's event log is complete and can be pinned
    /// EXACTLY — a strictly stronger assertion than the previous
    /// ">= 1 then scan" form, with no polling deadline.
    @Test func observeCapabilitySnapshotsInvalidatesEvaluatorBeforeEvaluate() async throws {
        let viewModel = SettingsViewModel()
        let stub = RecheckStubEligibilityEvaluator(verdict: AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: Date()
        ))

        let provider = ScriptedCapabilitiesProvider(snapshots: [
            makeCapabilitySnapshot(foundationModelsAvailable: true),
            makeCapabilitySnapshot(foundationModelsAvailable: false),
        ])
        await viewModel.observeCapabilitySnapshots(provider, evaluator: stub)

        #expect(stub.invalidateCallCount == 2,
                "Observer must invalidate the evaluator exactly once per snapshot. Invalidate count: \(stub.invalidateCallCount)")
        #expect(stub.evaluateCallCount == 2,
                "Observer must re-evaluate exactly once per snapshot. Evaluate count: \(stub.evaluateCallCount)")

        // Pin the FULL sequence: for EACH of the two emissions the
        // observer must call invalidate before evaluate. An exact match
        // also catches a refactor that paired the first iteration
        // correctly but reversed subsequent ones, and any stray extra
        // calls per emission.
        #expect(stub.events == [.invalidate, .evaluate, .invalidate, .evaluate],
                "Each emission must be handled as invalidate-then-evaluate (race fix). Events: \(stub.events)")
    }

    /// R1 audit: If a recheck is in flight when a snapshot reporting
    /// `foundationModelsUsable == true` lands, the observation loop
    /// must release the `isRecheckingModels` flag so the "Checking…"
    /// indicator does not linger past the moment the probe succeeded.
    ///
    /// playhead-zqhz: stream-driven. The scripted provider yields a
    /// NON-usable snapshot first (the flag must survive it — a
    /// non-usable verdict leaves "Checking…" up) and then a usable one
    /// (the flag must clear). Because the stream is finite and the
    /// observation is awaited directly, there is no polling deadline
    /// and no UserDefaults probe-cache plumbing through the real
    /// `CapabilitiesService`.
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

        let provider = ScriptedCapabilitiesProvider(snapshots: [
            makeCapabilitySnapshot(
                foundationModelsAvailable: true, foundationModelsUsable: false
            ),
            makeCapabilitySnapshot(
                foundationModelsAvailable: true, foundationModelsUsable: true
            ),
        ])
        await viewModel.observeCapabilitySnapshots(provider, evaluator: stub)

        #expect(viewModel.isRecheckingModels == false,
                "A usable snapshot landing during a pending recheck must clear isRecheckingModels")
    }

    /// Per-emission flag semantics, pinned directly on the extracted
    /// snapshot handler (playhead-zqhz): a NON-usable snapshot must
    /// leave a pending recheck's "Checking…" flag alone — only
    /// `recheckModels` returning (or a usable snapshot) may clear it.
    @Test func handleCapabilitySnapshotLeavesRecheckPendingOnNonUsableSnapshot() {
        let viewModel = SettingsViewModel()
        viewModel.isRecheckingModels = true
        let stub = RecheckStubEligibilityEvaluator(verdict: AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: false,
            capturedAt: Date()
        ))

        viewModel.handleCapabilitySnapshot(
            makeCapabilitySnapshot(
                foundationModelsAvailable: true, foundationModelsUsable: false
            ),
            evaluator: stub
        )

        #expect(viewModel.isRecheckingModels == true,
                "A non-usable snapshot must NOT clear a pending recheck — the row keeps reading Checking…")
        #expect(stub.events == [.invalidate, .evaluate],
                "The handler must still invalidate-then-evaluate on a non-usable snapshot")
    }
}

/// playhead-zqhz: finite, test-controlled capability stream. Yields the
/// scripted snapshots in order and then finishes, so a test can await
/// `observeCapabilitySnapshots` DIRECTLY — the loop processes exactly
/// these emissions on the test's own task and returns. This removes the
/// child-task + wall-clock-deadline pattern that flaked under full-suite
/// MainActor contention (the seed snapshot was not processed within 1 s).
final class ScriptedCapabilitiesProvider: CapabilitiesProviding, @unchecked Sendable {
    private let snapshots: [CapabilitySnapshot]

    init(snapshots: [CapabilitySnapshot]) {
        self.snapshots = snapshots
    }

    var currentSnapshot: CapabilitySnapshot {
        snapshots.first ?? makeCapabilitySnapshot()
    }

    func capabilityUpdates() -> AsyncStream<CapabilitySnapshot> {
        AsyncStream { continuation in
            for snapshot in snapshots {
                continuation.yield(snapshot)
            }
            continuation.finish()
        }
    }
}

/// Stub eligibility evaluator that records both invalidation and
/// evaluation calls AND the order they arrived in. The ordered event
/// log lets R2/R3 audits assert the invalidate-before-evaluate
/// contract (a refactor swapping the two would otherwise still
/// satisfy a pure count-based assertion). Local to the recheck-flow
/// suite so its bookkeeping is independent of the model-status copy
/// suite's stub.
final class RecheckStubEligibilityEvaluator: AnalysisEligibilityEvaluating, @unchecked Sendable {
    enum Event: Equatable, Sendable {
        case invalidate
        case evaluate
    }

    var verdict: AnalysisEligibility
    private(set) var evaluateCallCount = 0
    private(set) var invalidateCallCount = 0
    /// Append-only log of every recorded call, in arrival order. Used
    /// by the R3 ordering assertion to verify that invalidate
    /// precedes evaluate within each observer iteration — a
    /// count-only check passes even when the order is reversed.
    private let eventsLock = NSLock()
    private var _events: [Event] = []
    var events: [Event] {
        eventsLock.lock()
        defer { eventsLock.unlock() }
        return _events
    }

    init(verdict: AnalysisEligibility) {
        self.verdict = verdict
    }

    func evaluate() -> AnalysisEligibility {
        eventsLock.lock()
        evaluateCallCount += 1
        _events.append(.evaluate)
        eventsLock.unlock()
        return verdict
    }

    func invalidate() {
        eventsLock.lock()
        invalidateCallCount += 1
        _events.append(.invalidate)
        eventsLock.unlock()
    }
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
