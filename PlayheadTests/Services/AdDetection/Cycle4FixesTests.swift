// Cycle4FixesTests.swift
//
// Targeted unit tests for the Cycle 4 (Agent A4) fix list on top of
// the Cycle 2 Agent A branch:
//
//   - H-1: new SemanticScanStatus.permissive* cases wired through
//          FoundationModelClassifier.permissiveStatus(for:) and
//          persisted via BackfillJobRunner's failure-scan-result path.
//   - H-2: end-to-end integration — a fake permissive classifier
//          throws each PermissiveClassificationError reason and the
//          runner persists the matching permissive SemanticScanStatus
//          row plus bumps the matching permissiveFailureCount counter.
//   - H-3: per-run counter reset on runPendingBackfill.
//   - M-1: PermissiveAdClassifier rethrows CancellationError instead
//          of collapsing it to .permissiveDecodingFailure.
//   - M-3: smart-shrink retry loop (`smartShrinkClassify`) happy path,
//          terminal overflow path, iteration cap, and halving math.
//   - M-5: PlayheadRuntime-constructed FoundationModelClassifier
//          actually receives a non-noop redactor (regression rail).
//   - M-6: legacy spansJSON / EvidencePayload without the
//          ownershipInferenceWasSuppressed key decodes to `false`
//          (covered in Cycle2FixesTests.swift alongside the H4 rail —
//          this file does not duplicate).

import Foundation
import Testing

@testable import Playhead

#if canImport(FoundationModels)
import FoundationModels
#endif

/// Thread-safe integer box used to observe a `@Sendable` closure's
/// invocation count from the outer test scope without tripping the
/// "mutation of captured var" check.
private final class CallCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var _value = 0
    func increment() {
        lock.lock(); defer { lock.unlock() }
        _value += 1
    }
    var value: Int {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
}

private final class IntArrayBox: @unchecked Sendable {
    private let lock = NSLock()
    private var _value: [Int] = []
    func append(_ v: Int) {
        lock.lock(); defer { lock.unlock() }
        _value.append(v)
    }
    var value: [Int] {
        lock.lock(); defer { lock.unlock() }
        return _value
    }
}

// MARK: - H-1 / H-2 integration

@Suite("Cycle 4 fix-list — permissive end-to-end")
struct Cycle4PermissiveEndToEndTests {

    // MARK: - Fixtures

    private func makeAsset(id: String) -> AnalysisAsset {
        AnalysisAsset(
            id: id,
            episodeId: "ep-\(id)",
            assetFingerprint: "fp-\(id)",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/\(id).m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
    }

    /// Build inputs with a transcript line that contains the literal
    /// token "TRIGGERWORD" so the custom router below classifies the
    /// window as `.sensitive`.
    private func makeTriggeringInputs(
        assetId: String,
        transcriptVersion: String = "tx-cycle4-v1"
    ) -> BackfillJobRunner.AssetInputs {
        let segments = makeFMSegments(
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion,
            lines: [
                (0, 10, "Welcome to the show — today's sponsor mention."),
                (10, 20, "Ask your doctor about TRIGGERWORD for your condition."),
                (20, 30, "Back to the show.")
            ]
        )
        let evidenceCatalog = EvidenceCatalogBuilder.build(
            atoms: segments.flatMap(\.atoms),
            analysisAssetId: assetId,
            transcriptVersion: transcriptVersion
        )
        let plannerContext = CoveragePlannerContext(
            observedEpisodeCount: 0,
            stablePrecision: false,
            isFirstEpisodeAfterCohortInvalidation: false,
            recallDegrading: false,
            sponsorDriftDetected: false,
            auditMissDetected: false,
            episodesSinceLastFullRescan: 0,
            periodicFullRescanIntervalEpisodes: 10
        )
        return BackfillJobRunner.AssetInputs(
            analysisAssetId: assetId,
            podcastId: "podcast-cycle4",
            segments: segments,
            evidenceCatalog: evidenceCatalog,
            transcriptVersion: transcriptVersion,
            plannerContext: plannerContext
        )
    }

    private func makeTriggerRouter() -> SensitiveWindowRouter {
        SensitiveWindowRouter(
            triggerRules: [
                PromptRedactor.RedactionRule(pattern: "TRIGGERWORD", isRegex: true)
            ]
        )
    }

    @available(iOS 26.0, *)
    private func makeRunner(
        store: AnalysisStore,
        permissiveClassifier: PermissiveAdClassifier,
        router: SensitiveWindowRouter
    ) -> BackfillJobRunner {
        let fmRuntime = TestFMRuntime()
        return BackfillJobRunner(
            store: store,
            admissionController: AdmissionController(),
            classifier: FoundationModelClassifier(runtime: fmRuntime.runtime),
            coveragePlanner: CoveragePlanner(),
            mode: .shadow,
            capabilitySnapshotProvider: { makePermissiveCapabilitySnapshot() },
            batteryLevelProvider: { 1.0 },
            scanCohortJSON: makeTestScanCohortJSON(),
            sensitiveRouter: router,
            permissiveClassifier: BackfillJobRunner.PermissiveClassifierBox(permissiveClassifier)
        )
    }

    // MARK: - H-2 / H-1 end-to-end

    @available(iOS 26.0, *)
    private func runPermissiveFailureCase(
        reason: PermissiveClassificationError.Reason,
        expectedStatus: SemanticScanStatus,
        assetSuffix: String
    ) async throws {
        let store = try await makeTestStore()
        let assetId = "asset-cycle4-\(assetSuffix)"
        try await store.insertAsset(makeAsset(id: assetId))

        let permissive = PermissiveAdClassifier()
        await permissive.installFaultInjectionForTesting { _ in
            PermissiveClassificationError.failed(
                reason: reason,
                underlyingDescription: "fault-injection"
            )
        }
        let router = makeTriggerRouter()
        let runner = makeRunner(
            store: store,
            permissiveClassifier: permissive,
            router: router
        )

        let inputs = makeTriggeringInputs(assetId: assetId)
        _ = try await runner.runPendingBackfill(for: inputs)

        // Every window in this fixture contains TRIGGERWORD (line 1)
        // so at least one window is routed as .sensitive and the
        // injected fault throws. Verify the persisted scan row carries
        // the permissive-specific status.
        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        let permissiveRows = scans.filter { $0.status == expectedStatus }
        #expect(
            !permissiveRows.isEmpty,
            "expected at least one persisted semantic_scan_results row with status=\(expectedStatus.rawValue), got \(scans.map(\.status.rawValue))"
        )
        // No row for this sensitive window should be .success — the
        // fault injection throws before the FM response is parsed.
        #expect(
            !scans.contains { $0.status == .success && $0.scanPass == "passA" },
            "no coarse pass-A .success row should be written when the permissive path throws"
        )

        // The in-memory per-reason counter must also be bumped.
        let snapshot = await runner.snapshotPermissiveTelemetry()
        switch reason {
        case .permissiveRefusal:
            #expect(snapshot.refusal >= 1)
            #expect(snapshot.decodingFailure == 0)
            #expect(snapshot.contextOverflow == 0)
        case .permissiveDecodingFailure:
            #expect(snapshot.decodingFailure >= 1)
            #expect(snapshot.refusal == 0)
            #expect(snapshot.contextOverflow == 0)
        case .permissiveContextOverflow:
            #expect(snapshot.contextOverflow >= 1)
            #expect(snapshot.refusal == 0)
            #expect(snapshot.decodingFailure == 0)
        }
    }

    @available(iOS 26.0, *)
    @Test("Cycle 4 H-1/H-2: permissiveRefusal persists as .permissiveRefusal row")
    func permissiveRefusalEndToEnd() async throws {
        try await runPermissiveFailureCase(
            reason: .permissiveRefusal,
            expectedStatus: .permissiveRefusal,
            assetSuffix: "refusal"
        )
    }

    @available(iOS 26.0, *)
    @Test("Cycle 4 H-1/H-2: permissiveDecodingFailure persists as .permissiveDecodingFailure row")
    func permissiveDecodingFailureEndToEnd() async throws {
        try await runPermissiveFailureCase(
            reason: .permissiveDecodingFailure,
            expectedStatus: .permissiveDecodingFailure,
            assetSuffix: "decoding"
        )
    }

    @available(iOS 26.0, *)
    @Test("Cycle 4 H-1/H-2: permissiveContextOverflow persists as .permissiveContextOverflow row")
    func permissiveContextOverflowEndToEnd() async throws {
        try await runPermissiveFailureCase(
            reason: .permissiveContextOverflow,
            expectedStatus: .permissiveContextOverflow,
            assetSuffix: "overflow"
        )
    }

    // MARK: - H-3: counter reset between runs

    @available(iOS 26.0, *)
    @Test("Cycle 4/6 H-3: runPendingBackfill resets permissive counters between runs")
    func counterResetBetweenRuns() async throws {
        // Cycle 5 finding: the previous version of this test called
        // `runPendingBackfill` twice on the SAME asset. The second
        // call saw the jobId as terminal and dispatched nothing,
        // so the assertion `secondRunCount < firstRunCount * 2` was
        // satisfied whether or not the reset block was present
        // (0 < 2N both holds and `N < 2N` also holds). Bogus rail.
        //
        // Cycle 6 fix: use a DIFFERENT asset on the second call so
        // the runner actually re-drives the permissive failure path
        // and accumulates per-reason counts. With the reset present,
        // `secondRunCount == firstRunCount`. Without it, the first
        // run's counts carry over so `secondRunCount == 2 *
        // firstRunCount`. The `==` assertion discriminates both
        // directions.
        let store = try await makeTestStore()
        let assetIdA = "asset-cycle4-reset-a"
        let assetIdB = "asset-cycle4-reset-b"
        try await store.insertAsset(makeAsset(id: assetIdA))
        try await store.insertAsset(makeAsset(id: assetIdB))

        let permissive = PermissiveAdClassifier()
        await permissive.installFaultInjectionForTesting { _ in
            PermissiveClassificationError.failed(
                reason: .permissiveRefusal,
                underlyingDescription: "fault-injection"
            )
        }
        let runner = makeRunner(
            store: store,
            permissiveClassifier: permissive,
            router: makeTriggerRouter()
        )

        let inputsA = makeTriggeringInputs(assetId: assetIdA)
        _ = try await runner.runPendingBackfill(for: inputsA)
        let firstRunCount = await runner.snapshotPermissiveTelemetry().refusal
        #expect(firstRunCount >= 1, "first run should accumulate at least one permissive refusal")

        // Second invocation on a fresh asset — same runner instance.
        // With the reset block the counter starts at 0 and accumulates
        // the second asset's refusals; without it, the first run's
        // counts carry over before new refusals pile on top.
        let inputsB = makeTriggeringInputs(assetId: assetIdB)
        _ = try await runner.runPendingBackfill(for: inputsB)
        let secondRunCount = await runner.snapshotPermissiveTelemetry().refusal

        #expect(
            secondRunCount == firstRunCount,
            "runPendingBackfill must reset permissive counters per run: first=\(firstRunCount) second=\(secondRunCount) (expected equal; non-equal means either the reset is missing, in which case second==2*first, or fixture refusals diverged between assets)"
        )
    }

    // MARK: - Cycle 6: defensive catch-all status/counter agreement

    /// Exercises the "should not happen" defensive catch-all in
    /// `FoundationModelClassifier.coarsePassA`'s permissive arm. When
    /// `PermissiveAdClassifier.classify` throws a non-
    /// `PermissiveClassificationError`, the prior version appended
    /// `.refusal` (standard-path status) to `failedWindowStatuses`
    /// while bumping `permissiveCounts.refusal`, leaving the persisted
    /// row and the in-memory counter disagreeing on which category the
    /// failure belonged to.
    ///
    /// Cycle 6 fix: the defensive arm now appends `.permissiveRefusal`.
    /// This test injects a `TestUnexpectedError` (a vanilla
    /// `Error`, not a `PermissiveClassificationError`) via the new
    /// `arbitraryFaultInjectionForTesting` hook and asserts the
    /// persisted semantic-scan row carries `.permissiveRefusal`.
    @available(iOS 26.0, *)
    @Test("Cycle 6: defensive catch-all persists .permissiveRefusal (not .refusal) to match the counter")
    func defensiveCatchAllUsesPermissiveRefusalStatus() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-cycle6-defensive"
        try await store.insertAsset(makeAsset(id: assetId))

        let permissive = PermissiveAdClassifier()
        // Install the arbitrary-error hook so classify/refine throw a
        // non-PermissiveClassificationError and the
        // FoundationModelClassifier defensive arm fires.
        await permissive.installArbitraryFaultInjectionForTesting { _ in
            TestUnexpectedError()
        }
        let runner = makeRunner(
            store: store,
            permissiveClassifier: permissive,
            router: makeTriggerRouter()
        )

        let inputs = makeTriggeringInputs(assetId: assetId)
        _ = try await runner.runPendingBackfill(for: inputs)

        let scans = try await store.fetchSemanticScanResults(analysisAssetId: assetId)
        let permissiveRefusalRows = scans.filter { $0.status == .permissiveRefusal }
        let standardRefusalRows = scans.filter { $0.status == .refusal }

        #expect(
            !permissiveRefusalRows.isEmpty,
            "defensive catch-all must persist .permissiveRefusal; got statuses=\(scans.map(\.status.rawValue))"
        )
        #expect(
            standardRefusalRows.isEmpty,
            "defensive catch-all must NOT append .refusal (standard-path status); got \(standardRefusalRows.count) .refusal rows alongside the permissive path"
        )

        // In-memory per-reason counter must agree with the persisted
        // status. If the counter says `refusal >= 1` but the row is
        // `.refusal` instead of `.permissiveRefusal`, telemetry
        // sources are out of sync.
        let snapshot = await runner.snapshotPermissiveTelemetry()
        #expect(
            snapshot.refusal >= 1,
            "defensive catch-all must bump permissiveCounts.refusal; got snapshot=\(snapshot)"
        )
    }
}

/// Cycle 6: an opaque `Error` used only to exercise the
/// `FoundationModelClassifier` permissive defensive catch-all arms.
/// Deliberately not a `PermissiveClassificationError` so the catch
/// ladder falls through to `catch { ... }`.
private struct TestUnexpectedError: Error {}

// MARK: - M-1: CancellationError propagation

@Suite("Cycle 4 M-1: CancellationError propagates")
struct Cycle4CancellationTests {
    // The actor's fault-injection hook throws a
    // PermissiveClassificationError; to exercise CancellationError we
    // use the extracted smartShrinkClassify static helper with a
    // closure that throws CancellationError. That directly pins the
    // M-1 fix in the shrink loop; the classify/refine body-level
    // catches are structurally identical.
    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    @Test("smartShrinkClassify rethrows CancellationError untouched")
    func smartShrinkPropagatesCancellation() async throws {
        let segments = makeFMSegments(
            analysisAssetId: "asset-cancel",
            transcriptVersion: "tx-cancel",
            lines: [
                (0, 1, "line zero"),
                (1, 2, "line one"),
                (2, 3, "line two"),
                (3, 4, "line three")
            ]
        )
        let context = LanguageModelSession.GenerationError.Context(debugDescription: "test")
        do {
            _ = try await PermissiveAdClassifier.smartShrinkClassify(
                initialSegments: segments,
                initialError: .exceededContextWindowSize(context),
                maxIterations: 3,
                respond: { _ in throw CancellationError() }
            )
            Issue.record("expected CancellationError to propagate")
        } catch is CancellationError {
            // Expected — the loop did not collapse cancellation to
            // .permissiveDecodingFailure.
        } catch {
            Issue.record("unexpected error type: \(error) — cancellation must propagate untouched")
        }
    }
    #endif
}

// MARK: - M-3: smart-shrink coverage

@Suite("Cycle 4 M-3: smart-shrink retry loop")
struct Cycle4SmartShrinkTests {
    #if canImport(FoundationModels)
    @available(iOS 26.0, *)
    private func makeSegments(_ count: Int) -> [AdTranscriptSegment] {
        makeFMSegments(
            analysisAssetId: "asset-shrink",
            transcriptVersion: "tx-shrink",
            lines: (0..<count).map { i in
                (Double(i), Double(i + 1), "line \(i)")
            }
        )
    }

    @available(iOS 26.0, *)
    @Test("M-3 iteration 1 succeeds after halving — returns parsed schema")
    func firstIterationSucceedsAfterHalving() async throws {
        let segments = makeSegments(8)
        let context = LanguageModelSession.GenerationError.Context(debugDescription: "test")

        // Respond closure: always returns NO_AD. The shrink loop
        // halves from 8 → 4 on first attempt, calls respond, parses
        // "NO_AD" → .noAds and returns.
        let callCount = CallCounter()
        let result = try await PermissiveAdClassifier.smartShrinkClassify(
            initialSegments: segments,
            initialError: .exceededContextWindowSize(context),
            maxIterations: 3,
            respond: { prompt in
                callCount.increment()
                // Verify halving math: first attempt uses 8/2 == 4
                // segments. Each segment is rendered as a single
                // `L<n>> "line <n>"` line; count the L<digit>> tokens.
                let regex = try! NSRegularExpression(pattern: #"L\d+>"#)
                let ns = prompt as NSString
                let matches = regex.matches(in: prompt, range: NSRange(location: 0, length: ns.length))
                #expect(matches.count == 4, "first shrink iteration should render exactly 4 L-prefixed lines, got \(matches.count)")
                return "NO_AD"
            }
        )
        #expect(result.disposition == .noAds)
        #expect(callCount.value == 1)
    }

    @available(iOS 26.0, *)
    @Test("M-3 all iterations overflow — throws .permissiveContextOverflow")
    func allIterationsOverflowThrowsTerminal() async throws {
        let segments = makeSegments(16)
        let context = LanguageModelSession.GenerationError.Context(debugDescription: "test")
        let callCount = CallCounter()

        do {
            _ = try await PermissiveAdClassifier.smartShrinkClassify(
                initialSegments: segments,
                initialError: .exceededContextWindowSize(context),
                maxIterations: 3,
                respond: { _ in
                    callCount.increment()
                    throw LanguageModelSession.GenerationError.exceededContextWindowSize(context)
                }
            )
            Issue.record("expected throw")
        } catch let error as PermissiveClassificationError {
            #expect(error.reason == .permissiveContextOverflow)
        } catch {
            Issue.record("unexpected error type: \(error)")
        }
        // 3 iterations exhausted → exactly 3 respond calls.
        #expect(callCount.value == 3)
    }

    @available(iOS 26.0, *)
    @Test("M-3 halving math: 16 → 8 → 4 → 2 across 3 iterations")
    func halvingMathAcross3Iterations() async throws {
        let segments = makeSegments(16)
        let context = LanguageModelSession.GenerationError.Context(debugDescription: "test")
        let promptLineCounts = IntArrayBox()

        do {
            _ = try await PermissiveAdClassifier.smartShrinkClassify(
                initialSegments: segments,
                initialError: .exceededContextWindowSize(context),
                maxIterations: 3,
                respond: { prompt in
                    // Cheap way to count L-prefixed lines: split on
                    // the literal L<n>> tokens. Use a regex for
                    // robustness.
                    let regex = try! NSRegularExpression(pattern: #"L\d+>"#)
                    let ns = prompt as NSString
                    let matches = regex.matches(in: prompt, range: NSRange(location: 0, length: ns.length))
                    promptLineCounts.append(matches.count)
                    throw LanguageModelSession.GenerationError.exceededContextWindowSize(context)
                }
            )
            Issue.record("expected throw")
        } catch is PermissiveClassificationError {
            // Expected.
        }
        // 16 → 8 → 4 → 2
        #expect(promptLineCounts.value == [8, 4, 2], "got \(promptLineCounts.value)")
    }

    @available(iOS 26.0, *)
    @Test("M-3 maxIterations cap is honored — 1 iteration max means 1 call")
    func iterationCapHonored() async throws {
        let segments = makeSegments(16)
        let context = LanguageModelSession.GenerationError.Context(debugDescription: "test")
        let callCount = CallCounter()
        do {
            _ = try await PermissiveAdClassifier.smartShrinkClassify(
                initialSegments: segments,
                initialError: .exceededContextWindowSize(context),
                maxIterations: 1,
                respond: { _ in
                    callCount.increment()
                    throw LanguageModelSession.GenerationError.exceededContextWindowSize(context)
                }
            )
            Issue.record("expected throw")
        } catch is PermissiveClassificationError {
            // Expected.
        }
        #expect(callCount.value == 1)
    }
    #endif
}

// MARK: - M-5: PlayheadRuntime-constructed classifier has non-noop redactor

@Suite("Cycle 4/6 M-5: redactor wiring regression rail")
struct Cycle4RedactorWiringTests {
    // Cycle 5 finding: the previous rail constructed
    // `FoundationModelClassifier` directly with a freshly-loaded
    // production redactor, which did NOT exercise PlayheadRuntime's
    // wiring at all. A regression that swapped `.noop` into
    // PlayheadRuntime's classifier-construction site would have left
    // that test green. Cycle 6 fix: route BOTH the production site
    // and the test through `PlayheadRuntime.makeFoundationModelClassifier`
    // so a single factory is the source of truth for the wiring.

    @Test("Cycle 6 M-5: runtime factory produces a classifier whose redactor is active (positive rail)")
    func runtimeFactoryProducesActiveRedactor() throws {
        // Positive rail: feed the factory the same redactor
        // `PlayheadRuntime.init` loads (via `PromptRedactor.loadDefault()`)
        // and assert the returned classifier exposes the same active
        // redactor. If someone changes the factory body to ignore
        // the `redactor` parameter or substitute `.noop`, this rail
        // fails at the `isActive` assertion.
        let redactor = try PromptRedactor.loadDefault()
        #expect(redactor.isActive, "production RedactionRules.json should load a non-empty redactor")

        let classifier = PlayheadRuntime.makeFoundationModelClassifier(
            redactor: redactor,
            feedbackStore: nil
        )
        #if DEBUG
        #expect(
            classifier.redactorForTesting.isActive,
            "PlayheadRuntime.makeFoundationModelClassifier(redactor: <active>) must retain the non-noop redactor"
        )
        #endif
    }

    @Test("Cycle 6 M-5: runtime factory with .noop produces an inactive redactor (negative rail)")
    func runtimeFactoryWithNoopProducesNoopRedactor() {
        // Negative rail: calling the factory with `.noop` must return
        // a classifier whose `redactorForTesting.isActive == false`.
        // This proves the positive rail's `isActive == true`
        // assertion actually discriminates between the two cases —
        // without this, a buggy accessor that always returned `true`
        // would still pass the positive rail.
        let classifier = PlayheadRuntime.makeFoundationModelClassifier(
            redactor: .noop,
            feedbackStore: nil
        )
        #if DEBUG
        #expect(
            !classifier.redactorForTesting.isActive,
            "PlayheadRuntime.makeFoundationModelClassifier(redactor: .noop) must expose an inactive redactor"
        )
        #endif
    }
}
