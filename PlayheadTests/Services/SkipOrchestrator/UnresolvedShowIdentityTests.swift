// UnresolvedShowIdentityTests.swift
// playhead-djl0: `.shadow` must never be a silent fallback for "I could not
// look something up."
//
// THE DEFECT. `SkipOrchestrator.beginEpisode` resolved the per-show trust mode
// like this:
//
//     if let podcastId = normalizedPodcastId, let trustService {
//         activeSkipMode = await trustService.effectiveMode(podcastId: podcastId)
//     } else {
//         activeSkipMode = .shadow
//     }
//
// One `else` branch, reached by TWO different facts that are not the same fact:
// "this episode carried no canonical show identifier" and "no trust service was
// wired". Neither is `SkipMode.shadow`, which names a DELIBERATE posture —
// detection runs, nothing fires, we are watching the show earn trust. And
// `TrustScoringService.effectiveMode` folds two more failures into the same
// value: a profile read that THREW, and a stored `mode` string that did not
// decode.
//
// Every one of those produced a byte-identical `activeSkipMode`, a
// byte-identical decision-log line ("Shadow mode -- detection logged, no skip
// fired"), a byte-identical pill, and no log entry at all. There was no way,
// from inside or outside the app, to tell a working pipeline that had lost the
// show's identity from a show that was deliberately being observed.
//
// THE STANDING CHECK (`feedback_ask_what_the_quantity_measures_2026-07-29`):
// what would this read if the lookup had SUCCEEDED and the show genuinely sat
// in shadow? Before this bead: exactly the same. That is the whole bug.
//
// WHAT IS PINNED HERE
//
//   1. Each cause has its OWN name, and two causes that produce the same
//      `SkipMode` are still distinguishable. `resolutionDistinguishesA
//      DeliberateShadowFromAnUnresolvedOne` is the standing check in test form.
//   2. Each failure cause is COUNTED, per cause, and a resolvable show
//      increments nothing (the "proof a resolvable show is unaffected" half).
//   3. Each failure is RECORDED to the durable diagnostics log the device pull
//      reads, with its own `InvariantViolation.Code` — the witness the field
//      investigation went looking for and did not find.
//   4. Half one: an identity the CALLER could not supply is recovered from the
//      durable `analysis_jobs` row before the failure branch is taken, so a
//      show that is knowable is not lost to a nullable in-memory hop.
//   5. THE FIELD REGRESSION (2026-08-01, episode D9B513CD): day-0 windows
//      delivered mid-session to a session with no resolvable show identity must
//      not produce zero user-facing output with no trace.
//
// A NOTE ON THE FIELD CASE, because it corrects the bead's leading hypothesis.
// The bead proposed that an unresolved `podcastId` forced `.shadow` and that
// shadow is why no banner appeared. The first half is a real defect and is what
// this file fixes. The second half is FALSE, and
// `SuggestTierIsNotGatedByTrustModeTests` below proves it: `activeSkipMode` is
// read at exactly five sites in `SkipOrchestrator`, all of them on the MANAGED
// (auto-skip) tier. The suggest tier — `receiveAdWindows`' `.markOnly` routing,
// `registerSuggestedWindow`, and `emitSuggestBannersOnPlayheadEntry` — never
// consults it. A mark-only window banners identically in `.shadow` and in
// `.auto`. Shadow cannot explain a missing banner, and that test exists so the
// hypothesis cannot be re-adopted without a red test.

import AVFoundation
import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum ShowIdentityFixture {

    static let assetId = "asset-1"
    static let episodeId = "ep-1"
    /// The ONLY show `makeSkipTestTrustService` seeds a profile for. A different
    /// string resolves through the "no profile yet" arm, which is a DIFFERENT
    /// cause and would make a mode assertion vacuous.
    static let seededPodcastId = "podcast-1"

    /// An orchestrator whose seeded show is in `auto`. `auto` (not `shadow`) is
    /// deliberate: it makes every "the lookup succeeded" assertion below
    /// non-vacuous, because a fixture that silently failed to resolve would
    /// land on `.shadow` and be visibly wrong rather than accidentally right.
    static func makeOrchestrator(
        store: AnalysisStore,
        trustMode: String = "auto",
        withTrustService: Bool = true,
        invariantLogger: SurfaceStatusInvariantLogger = SurfaceStatusInvariantLogger()
    ) async throws -> SkipOrchestrator {
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(id: assetId, episodeId: episodeId)
        )
        let trustService = withTrustService
            ? try await makeSkipTestTrustService(
                mode: trustMode, trustScore: 0.9, observations: 10
            )
            : nil
        return SkipOrchestrator(
            store: store,
            trustService: trustService,
            correctionStore: PersistentUserCorrectionStore(store: store),
            invariantLogger: invariantLogger
        )
    }

    /// An `analysis_jobs` row is the one durable table that carries a
    /// `podcastId` for an episode (`analysis_assets` was never given the
    /// column). Half one of the fix reads it when the caller has nothing.
    static func seedJobRow(
        in store: AnalysisStore,
        episodeId: String = episodeId,
        podcastId: String?,
        createdAt: Double = 1_000
    ) async throws {
        _ = try await store.insertJob(
            AnalysisJob(
                jobId: UUID().uuidString,
                jobType: "preAnalysis",
                episodeId: episodeId,
                podcastId: podcastId,
                analysisAssetId: assetId,
                workKey: "wk-\(UUID().uuidString)",
                sourceFingerprint: "fp-1",
                downloadId: episodeId,
                priority: 0,
                desiredCoverageSec: 600,
                featureCoverageSec: 0,
                transcriptCoverageSec: 0,
                cueCoverageSec: 0,
                state: "queued",
                attemptCount: 0,
                nextEligibleAt: nil,
                leaseOwner: nil,
                leaseExpiresAt: nil,
                lastErrorCode: nil,
                createdAt: createdAt,
                updatedAt: createdAt
            )
        )
    }

    /// A row shaped like what `mintByteExactDayZeroMarks` persisted for the
    /// four slots on episode D9B513CD: confidence 1.00, byte-exact provenance,
    /// `markOnly`, unanchored on both edges, `candidate`.
    static func makeFieldDayZeroWindow(
        id: String,
        start: Double,
        end: Double
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: AdDetectionService.dayZeroRediffByteExactBoundaryState,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: "detection-v1",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: start,
            metadataSource: AdDetectionService.dayZeroRediffByteExactMetadataSource,
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: nil,
            eligibilityGate: SkipEligibilityGate.markOnly.rawValue,
            catalogStoreMatchSimilarity: nil,
            startEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue,
            endEdgeAnchor: AutoSkipEdgeAnchor.unanchored.rawValue
        )
    }

    static func persist(_ windows: [AdWindow], in store: AnalysisStore) async throws {
        try await store.upsertHotPathAdWindows(windows, existingIDs: [], retiredIDs: [])
    }
}

/// Drain the logger's JSON Lines session file. Mirrors the retry-drain the
/// existing `SurfaceStatus` suites use — the write queue is serial and
/// `flushForTesting` is synchronous, but the file handle is opened lazily.
private func drainInvariantCodes(
    _ logger: SurfaceStatusInvariantLogger,
    untilSentinel sentinel: String
) throws -> [InvariantViolation.Code] {
    // The sentinel is enqueued AFTER the writes under test, on the same serial
    // queue, so its arrival proves those writes have landed. "No skip-mode code
    // arrived" then becomes a positive observation instead of a race.
    logger.invariantViolated(code: .unknown, description: sentinel)
    var entries: [SurfaceStateTransitionEntry] = []
    for _ in 0..<10 {
        logger.flushForTesting()
        guard let sessionURL = logger.currentSessionFileURL,
              let data = try? Data(contentsOf: sessionURL) else { continue }
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        entries = String(decoding: data, as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
            .compactMap {
                try? decoder.decode(
                    SurfaceStateTransitionEntry.self, from: Data($0.utf8)
                )
            }
        if entries.contains(where: {
            $0.invariantViolation?.description == sentinel
        }) { break }
    }
    return entries
        .filter { $0.invariantViolation?.description != sentinel }
        .compactMap(\.invariantViolation?.code)
}

/// A logger writing into a per-test temporary directory, plus its cleanup.
private func makeScopedInvariantLogger() -> (SurfaceStatusInvariantLogger, URL) {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("djl0-\(UUID().uuidString)", isDirectory: true)
    return (SurfaceStatusInvariantLogger(directory: directory), directory)
}

/// Single-consumer banner reader (playhead-d3g0's pattern, reused verbatim):
/// drain until a sentinel window arrives, so "nothing was emitted" is a
/// positive observation rather than a timeout.
private struct BannerReader {
    private var iterator: AsyncStream<AdSkipBannerItem>.AsyncIterator

    init(_ stream: AsyncStream<AdSkipBannerItem>) {
        iterator = stream.makeAsyncIterator()
    }

    mutating func drain(until sentinel: String) async -> [AdSkipBannerItem] {
        var collected: [AdSkipBannerItem] = []
        while let item = await iterator.next() {
            if item.windowId == sentinel { return collected }
            collected.append(item)
        }
        return collected
    }
}

// MARK: - 1. Each cause has its own name

@Suite("An unresolved show identity is its own named cause (playhead-djl0)",
       .timeLimit(.minutes(1)))
struct SkipModeResolutionNamingTests {

    private typealias Fx = ShowIdentityFixture

    /// THE STANDING CHECK, in test form. Two sessions, both landing on
    /// `SkipMode.shadow`, for two facts that are not the same fact. Before this
    /// bead every observable about them was identical.
    @Test("a deliberate shadow and an unresolved identity are DISTINGUISHABLE")
    func resolutionDistinguishesADeliberateShadowFromAnUnresolvedOne() async throws {
        // A. the lookup SUCCEEDED and the show genuinely sits in shadow.
        let deliberateStore = try await makeTestStore()
        let deliberate = try await Fx.makeOrchestrator(
            store: deliberateStore, trustMode: "shadow"
        )
        await deliberate.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )

        // B. the lookup could not be attempted — no show identity at all.
        let unresolvedStore = try await makeTestStore()
        let unresolved = try await Fx.makeOrchestrator(store: unresolvedStore)
        await unresolved.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: nil
        )

        // The MODE is the same in both, and must stay that way — this bead
        // changes no skip behaviour.
        #expect(await deliberate.currentSkipMode() == .shadow)
        #expect(await unresolved.currentSkipMode() == .shadow)

        // The CAUSE is not.
        #expect(await deliberate.currentSkipModeResolution() == .showTrustProfile)
        #expect(await unresolved.currentSkipModeResolution() == .unresolvedShowIdentity)
        #expect(
            await deliberate.currentSkipModeResolution()
                != unresolved.currentSkipModeResolution(),
            "if these ever compare equal the defect is back"
        )
    }

    @Test("a resolvable show in auto reports its profile as the cause")
    func aResolvedShowNamesItsProfile() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )
        #expect(await orchestrator.currentSkipMode() == .auto)
        #expect(await orchestrator.currentSkipModeResolution() == .showTrustProfile)
    }

    /// A show with no profile yet IS legitimately shadow — `SkipMode.shadow`'s
    /// own doc calls it "Default for new shows". It gets a distinct name so it
    /// is not counted as a failure, and so it is not confused with an identity
    /// that could not be resolved at all.
    /// "Not a failure" is a claim about the COUNTERS and the LOG, not only
    /// about the name. Asserting the name alone let a mutation that reclassified
    /// `newShowDefault` as a failure pass: every first listen would then emit a
    /// coded diagnostics line and increment a failure counter, and the audit
    /// bucket would read as "we lose the show constantly" while nothing was
    /// wrong. Found by mutation J02.
    @Test("a show with no profile yet is a new-show default, not a failure")
    func anUnseededShowIsANewShowDefault() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, invariantLogger: logger
        )
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: "podcast-never-seen"
        )
        #expect(await orchestrator.currentSkipMode() == .shadow)
        #expect(await orchestrator.currentSkipModeResolution() == .newShowDefault)
        #expect(!SkipModeResolution.newShowDefault.isLookupFailure)
        #expect(
            await orchestrator.skipModeResolutionFailureCount(.newShowDefault) == 0,
            "a first listen is not an incident — counting it makes the tally unreadable"
        )
        #expect(
            await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 0,
            "a brand-new show is not an identity failure"
        )
        let codes = try drainInvariantCodes(logger, untilSentinel: "djl0-newshow")
        #expect(!codes.contains(.skipModeShowIdentityUnresolved))
        #expect(!codes.contains(.skipModeTrustLookupFailed),
                "a first listen must not write a diagnostics incident")
    }

    /// A show identity WAS supplied; there was simply nothing to ask. Production
    /// always wires a trust service, so reaching this in the field would be a
    /// wiring regression — which is exactly why it must not hide inside the
    /// same branch as a missing identifier.
    @Test("a missing trust service is its own cause, not an unresolved identity")
    func aMissingTrustServiceIsItsOwnCause() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, withTrustService: false
        )
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )
        #expect(await orchestrator.currentSkipMode() == .shadow)
        #expect(await orchestrator.currentSkipModeResolution() == .trustServiceUnavailable)
        #expect(
            await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 0,
            "the identity was fine — attributing this to the identifier would be a new lie"
        )
    }

    /// A show id that is not in its canonical spelling is not an identity we
    /// may key show-scoped evidence on (`RecurrenceMaterialIdentity`), so it
    /// resolves to nothing — and that is an identity failure, not a new show.
    @Test("a non-canonical show id is an identity failure, not a new show")
    func aNonCanonicalShowIdIsAnIdentityFailure() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: "  \(Fx.seededPodcastId)  "
        )
        #expect(await orchestrator.currentSkipModeResolution() == .unresolvedShowIdentity)
    }

    /// A stored `mode` string that does not decode is a corruption or a
    /// forward-compatibility signal, not a verdict. Pre-djl0 it silently became
    /// `.shadow`, indistinguishable from the show having chosen shadow.
    @Test("a profile whose stored mode does not decode is its own cause")
    func anUnrecognizedStoredModeIsItsOwnCause() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, trustMode: "supersonic"
        )
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )
        #expect(await orchestrator.currentSkipMode() == .shadow)
        #expect(await orchestrator.currentSkipModeResolution() == .unrecognizedTrustProfileMode)
        #expect(
            await orchestrator.skipModeResolutionFailureCount(.unrecognizedTrustProfileMode) == 1
        )
        #expect(
            await orchestrator.skipModeResolutionFailureCount(.newShowDefault) == 0,
            "an undecodable mode is not the same as having no profile"
        )
    }

    /// A `beginEpisode` that is superseded mid-hydration returns early. Its
    /// partial state must not leave the PRIOR episode's cause installed under
    /// the new episode's mode — the mode is reset before the first suspension,
    /// so the cause has to be too or the two describe different episodes.
    @Test("a beginEpisode superseded mid-hydration leaves no stale cause")
    func aSupersededBeginEpisodeLeavesNoStaleCause() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )
        #expect(await orchestrator.currentSkipModeResolution() == .showTrustProfile,
                "control: the first episode really did resolve")

        // The replacement bumps the lifecycle generation from inside the
        // barrier, so the outer call returns at the guard immediately after it.
        await orchestrator._setBeginEpisodeHydrationBarrierForTesting {
            await orchestrator._setBeginEpisodeHydrationBarrierForTesting(nil)
            await orchestrator.beginEpisode(
                analysisAssetId: Fx.assetId, episodeId: "ep-replacement", podcastId: nil
            )
        }
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: "ep-superseded", podcastId: nil
        )

        #expect(await orchestrator.currentSkipMode() == .shadow)
        #expect(await orchestrator.currentSkipModeResolution() != .showTrustProfile,
                "the superseded start must not still report the PRIOR episode's show")
    }

    @Test("an explicit session override is its own cause")
    func aSessionOverrideIsItsOwnCause() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: nil
        )
        #expect(await orchestrator.currentSkipModeResolution() == .unresolvedShowIdentity)

        await orchestrator.setActiveSkipMode(.manual)

        #expect(await orchestrator.currentSkipMode() == .manual)
        #expect(await orchestrator.currentSkipModeResolution() == .sessionOverride,
                "the user's choice is not the orchestrator's lookup failure")
    }

    @Test("no episode is running before beginEpisode and after endEpisode")
    func noEpisodeIsItsOwnCause() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        #expect(await orchestrator.currentSkipModeResolution() == .noActiveEpisode)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )
        #expect(await orchestrator.currentSkipModeResolution() == .showTrustProfile)

        await orchestrator.endEpisode()
        #expect(await orchestrator.currentSkipModeResolution() == .noActiveEpisode)
        #expect(await orchestrator.currentSkipMode() == .shadow,
                "endEpisode's non-actioning default is unchanged")
    }

    /// The failure set is one definition, consumed by the counter, the
    /// diagnostics record and the pill. If it drifts, all three drift together
    /// or the tests below stop meaning what they say.
    @Test("exactly the lookup failures are classified as failures")
    func theFailureSetIsExactlyTheLookupFailures() {
        let failures = SkipModeResolution.allCases.filter(\.isLookupFailure)
        #expect(Set(failures) == [
            .unresolvedShowIdentity,
            .trustServiceUnavailable,
            .trustProfileUnreadable,
            .unrecognizedTrustProfileMode,
        ])
        #expect(!SkipModeResolution.noActiveEpisode.isLookupFailure)
        #expect(!SkipModeResolution.showTrustProfile.isLookupFailure)
        #expect(!SkipModeResolution.newShowDefault.isLookupFailure)
        #expect(!SkipModeResolution.sessionOverride.isLookupFailure)
    }

    /// The show-identity predicate is narrower than the failure predicate: a
    /// profile that failed to READ still belongs to a show whose per-show
    /// preference can be stored, so only the identity failure withholds the
    /// per-show control.
    @Test("only an unresolved identity leaves the session without a show")
    func onlyTheIdentityFailureLosesTheShow() {
        let showless = SkipModeResolution.allCases.filter { !$0.hasResolvedShowIdentity }
        #expect(Set(showless) == [.noActiveEpisode, .unresolvedShowIdentity])
    }
}

// MARK: - 2. Counted

@Suite("Lookup failures are counted per cause (playhead-djl0)",
       .timeLimit(.minutes(1)))
struct SkipModeResolutionCountingTests {

    private typealias Fx = ShowIdentityFixture

    @Test("each episode begun without a resolvable identity increments its counter")
    func unresolvedEpisodesAreCounted() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)

        #expect(await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 0)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )
        #expect(await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 1)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )
        #expect(await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 2,
                "the count is per EPISODE START, not a latched flag")
    }

    /// The other half of the same claim, and the one that makes the counter
    /// mean something: a show that resolves contributes nothing.
    @Test("a resolvable show never increments any failure counter")
    func aResolvableShowIsUnaffected() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)

        for _ in 0..<3 {
            await orchestrator.beginEpisode(
                analysisAssetId: Fx.assetId,
                episodeId: Fx.episodeId,
                podcastId: Fx.seededPodcastId
            )
        }

        #expect(await orchestrator.currentSkipMode() == .auto,
                "control: the fixture really did resolve — otherwise this suite is vacuous")
        for cause in SkipModeResolution.allCases where cause.isLookupFailure {
            #expect(await orchestrator.skipModeResolutionFailureCount(cause) == 0,
                    "a resolvable show incremented \(cause.rawValue)")
        }
    }

    @Test("counters are per cause — a missing trust service does not inflate the identity count")
    func countersDoNotBleedAcrossCauses() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, withTrustService: false
        )
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )
        #expect(await orchestrator.skipModeResolutionFailureCount(.trustServiceUnavailable) == 1)
        #expect(await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 0)
    }

    @Test("a non-failure cause is never counted")
    func nonFailureCausesAreNotCounted() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )
        await orchestrator.setActiveSkipMode(.manual)
        #expect(await orchestrator.skipModeResolutionFailureCount(.sessionOverride) == 0)
        #expect(await orchestrator.skipModeResolutionFailureCount(.showTrustProfile) == 0)
        #expect(await orchestrator.skipModeResolutionFailureCount(.newShowDefault) == 0)
    }
}

// MARK: - 3. Surfaced to the durable diagnostics log

@Suite("Lookup failures reach the diagnostics log (playhead-djl0)",
       .timeLimit(.minutes(1)))
struct SkipModeResolutionDiagnosticsTests {

    private typealias Fx = ShowIdentityFixture

    /// The field investigation went looking for a log line and found nothing.
    /// This is that line — in the same JSON Lines session file the device pull
    /// already reads, tagged with its own code so an audit can group by it.
    @Test("an unresolved identity writes a coded entry to the session log")
    func unresolvedIdentityIsRecorded() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, invariantLogger: logger
        )

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )

        let codes = try drainInvariantCodes(logger, untilSentinel: "djl0-a")
        #expect(codes.contains(.skipModeShowIdentityUnresolved),
                "the failure must leave a durable trace, not just an in-memory count")
    }

    /// The half that makes the entry mean something. A show that resolves must
    /// write NO such entry — otherwise the audit bucket counts every episode
    /// and measures nothing.
    @Test("a resolvable show writes no skip-mode failure entry")
    func aResolvableShowRecordsNothing() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, invariantLogger: logger
        )

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )

        #expect(await orchestrator.currentSkipMode() == .auto, "control: really resolved")
        let codes = try drainInvariantCodes(logger, untilSentinel: "djl0-b")
        #expect(!codes.contains(.skipModeShowIdentityUnresolved))
        #expect(!codes.contains(.skipModeTrustLookupFailed))
    }

    @Test("a missing trust service records the trust-lookup code, not the identity code")
    func missingTrustServiceRecordsItsOwnCode() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, withTrustService: false, invariantLogger: logger
        )

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )

        let codes = try drainInvariantCodes(logger, untilSentinel: "djl0-c")
        #expect(codes.contains(.skipModeTrustLookupFailed))
        #expect(!codes.contains(.skipModeShowIdentityUnresolved))
    }
}

// MARK: - 4. Half one — recovering an identity the caller could not supply

@Suite("A knowable show identity is recovered before the failure branch (playhead-djl0)",
       .timeLimit(.minutes(1)))
struct ShowIdentityRecoveryTests {

    private typealias Fx = ShowIdentityFixture

    /// `beginEpisode`'s `podcastId` comes from ONE nullable in-memory hop:
    /// `episode.podcast?.feedURL.absoluteString`, read on the MainActor in
    /// `PlayheadRuntime.performPlayEpisode`. When that hop yields nothing the
    /// durable `analysis_jobs` row may still know the answer, and losing the
    /// show's trust mode because an in-memory relationship was unavailable is
    /// not a decision anyone made.
    @Test("a nil caller identity is recovered from the durable job row")
    func aNilCallerIdentityIsRecoveredFromTheJobRow() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(in: store, podcastId: Fx.seededPodcastId)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )

        #expect(await orchestrator.currentSkipMode() == .auto,
                "the recovered identity resolved the show's real trust mode")
        #expect(await orchestrator.currentSkipModeResolution() == .showTrustProfile)
        #expect(await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 0)
    }

    /// The recovered identity has to become the session's show for real —
    /// show-scoped recurrence learning keys on it. A recovery that fixed only
    /// the mode would leave a second, quieter version of the same defect.
    @Test("the recovered identity becomes the session's active show")
    func theRecoveredIdentityBecomesTheActiveShow() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(in: store, podcastId: Fx.seededPodcastId)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )

        #expect(await orchestrator.activePodcastIdForTesting() == Fx.seededPodcastId)
    }

    /// The caller's value is authoritative — it is read from the live SwiftData
    /// relationship, the job row is a lagging mirror. Recovery must never
    /// override a supplied identity.
    @Test("a supplied identity is never overridden by the job row")
    func aSuppliedIdentityWins() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(in: store, podcastId: "podcast-stale")

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )

        #expect(await orchestrator.activePodcastIdForTesting() == Fx.seededPodcastId)
        #expect(await orchestrator.currentSkipMode() == .auto)
    }

    /// playhead-kkzu asked, and this answers it by OBSERVATION rather than by
    /// reading the code: does a NULL `analysis_jobs.podcastId` — what every
    /// background/auto download used to write — put an episode into shadow
    /// mode? It does not. `beginEpisode`'s identity comes from the caller,
    /// which in production is `Episode.resolvedShowIdentity` (SwiftData), and
    /// recovery from the job row is only consulted when that is already nil.
    ///
    /// The two witnesses are the point: the mode is the show's REAL `.auto`
    /// (so the trust lookup genuinely ran against the supplied identity), and
    /// the `.unresolvedShowIdentity` counter is zero (so nothing fell through
    /// the failure branch on the way there).
    @Test("a NULL job row does not shadow an episode whose caller knows the show")
    func aNullJobRowDoesNotShadowASuppliedIdentity() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(in: store, podcastId: nil)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )

        #expect(await orchestrator.activePodcastIdForTesting() == Fx.seededPodcastId)
        #expect(await orchestrator.currentSkipMode() == .auto)
        #expect(await orchestrator.currentSkipModeResolution() == .showTrustProfile)
        #expect(
            await orchestrator.skipModeResolutionFailureCount(
                .unresolvedShowIdentity
            ) == 0
        )
    }

    /// The field asset's actual shape: a job row exists but its `podcastId`
    /// column is NULL. Recovery cannot invent an answer, and must fall through
    /// to the NAMED failure rather than to silence.
    ///
    /// playhead-kkzu changed WHY such a row exists, not what happens to it.
    /// Every background/auto download used to enqueue with no `DownloadContext`
    /// at all; the download path now carries the show, so a NULL job row means
    /// the show was genuinely unresolvable. Note also what this rail does NOT
    /// say: reaching `.shadow` needs the CALLER's `podcastId` to be nil too
    /// (here it is passed explicitly). In production that value comes from
    /// `Episode.resolvedShowIdentity` — SwiftData, not `analysis_jobs` — so a
    /// NULL job row alone never put an episode into shadow mode. It removed the
    /// recovery net underneath a SwiftData failure.
    @Test("a job row with a NULL podcastId falls through to the named failure")
    func aNullJobRowStillFailsButNamed() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(in: store, podcastId: nil)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )

        #expect(await orchestrator.currentSkipMode() == .shadow)
        #expect(await orchestrator.currentSkipModeResolution() == .unresolvedShowIdentity)
        #expect(await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 1)
    }

    /// A stored value that is not in canonical spelling is not an identity we
    /// may key show-scoped evidence on. Recovery must apply the same rule the
    /// caller path applies, not a looser one.
    @Test("a non-canonical stored identity is not recovered")
    func aNonCanonicalStoredIdentityIsNotRecovered() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(in: store, podcastId: " \(Fx.seededPodcastId) ")

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )

        #expect(await orchestrator.currentSkipModeResolution() == .unresolvedShowIdentity)
        #expect(await orchestrator.activePodcastIdForTesting() == nil)
    }

    /// The realistic mixed history: an episode enqueued once through a
    /// context-free background download (NULL) and once through a path that
    /// knew the show. "Newest row" alone would return the NULL and give up, so
    /// the query filters to rows that actually carry an answer.
    @Test("a NULL newest row does not mask an older row that knows the show")
    func aNullNewestRowDoesNotMaskAnOlderAnswer() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(
            in: store, podcastId: Fx.seededPodcastId, createdAt: 1_000
        )
        try await Fx.seedJobRow(in: store, podcastId: nil, createdAt: 2_000)

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )

        #expect(await orchestrator.activePodcastIdForTesting() == Fx.seededPodcastId)
        #expect(await orchestrator.currentSkipMode() == .auto)
    }

    @Test("the newest job row wins when an episode has several")
    func theNewestJobRowWins() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(in: store, podcastId: "podcast-old", createdAt: 1_000)
        try await Fx.seedJobRow(
            in: store, podcastId: Fx.seededPodcastId, createdAt: 2_000
        )

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )

        #expect(await orchestrator.activePodcastIdForTesting() == Fx.seededPodcastId)
        #expect(await orchestrator.currentSkipMode() == .auto)
    }

    /// Recovery is scoped to the episode being started. A different episode's
    /// row must not lend its show to this session.
    @Test("another episode's job row is not borrowed")
    func anotherEpisodesJobRowIsNotBorrowed() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        try await Fx.seedJobRow(
            in: store, episodeId: "ep-other", podcastId: Fx.seededPodcastId
        )

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )

        #expect(await orchestrator.currentSkipModeResolution() == .unresolvedShowIdentity)
        #expect(await orchestrator.activePodcastIdForTesting() == nil)
    }
}

// MARK: - 5. The field regression

@Suite("The 2026-08-01 field case: detection without a show is not silent (playhead-djl0)",
       .timeLimit(.minutes(1)))
struct UnresolvedShowFieldRegressionTests {

    private typealias Fx = ShowIdentityFixture

    private static let sentinelId = "djl0-sentinel"

    private static func fireSentinel(
        _ orchestrator: SkipOrchestrator, at time: Double
    ) async {
        await orchestrator.receiveAdWindows([
            Fx.makeFieldDayZeroWindow(id: sentinelId, start: time, end: time + 4)
        ])
        await orchestrator.updatePlayheadTime(time)
    }

    /// Episode D9B513CD, reproduced: play starts, day-0 rediff mints four
    /// mark-only byte-exact windows ~14 s in with the playhead already inside
    /// the first one, and the session has no resolvable show identity.
    ///
    /// The bead's acceptance condition is "must NOT silently produce zero
    /// user-facing output with no trace", so this asserts BOTH halves:
    /// the banner is emitted (detection is not silenced by the missing
    /// identity), and the missing identity left a trace.
    @Test("day-0 marks delivered to a session with no show still banner, and leave a trace")
    func dayZeroMarksInAShowlessSessionAreNotSilent() async throws {
        let (logger, directory) = makeScopedInvariantLogger()
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, invariantLogger: logger
        )
        var reader = BannerReader(await orchestrator.bannerItemStream())

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId, episodeId: Fx.episodeId, podcastId: nil
        )
        // 14 s in, inside the 0.0–45.1 s pre-roll — Dan's exact position.
        await orchestrator.updatePlayheadTime(14)

        try await Fx.persist([
            Fx.makeFieldDayZeroWindow(id: "d0-1", start: 0, end: 45.1),
            Fx.makeFieldDayZeroWindow(id: "d0-2", start: 1_436.4, end: 1_508.1),
            Fx.makeFieldDayZeroWindow(id: "d0-3", start: 3_194.5, end: 3_371.2),
            Fx.makeFieldDayZeroWindow(id: "d0-4", start: 3_899.8, end: 3_929.9),
        ], in: store)
        let delivered = await orchestrator.ingestPersistedAdWindows(
            analysisAssetId: Fx.assetId
        )
        #expect(delivered == 4, "all four rows reach the live session")

        // d3g0 owns the trigger: the next position observation presents.
        await orchestrator.updatePlayheadTime(14.25)
        await Self.fireSentinel(orchestrator, at: 5_000)
        let banners = await reader.drain(until: Self.sentinelId)

        #expect(banners.map(\.windowId) == ["d0-1"],
                "the span the listener is INSIDE must ask — this is the visible output")

        // ...and the lost identity is no longer invisible.
        #expect(await orchestrator.currentSkipModeResolution() == .unresolvedShowIdentity)
        #expect(await orchestrator.skipModeResolutionFailureCount(.unresolvedShowIdentity) == 1)
        let codes = try drainInvariantCodes(logger, untilSentinel: "djl0-field")
        #expect(codes.contains(.skipModeShowIdentityUnresolved))
    }
}

// MARK: - 6. The refutation the bead's hypothesis needs

@Suite("The suggest tier is not gated by trust mode (playhead-djl0 refutation)",
       .timeLimit(.minutes(1)))
struct SuggestTierIsNotGatedByTrustModeTests {

    private typealias Fx = ShowIdentityFixture

    private static let sentinelId = "djl0-refute-sentinel"

    private static func fireSentinel(
        _ orchestrator: SkipOrchestrator, at time: Double
    ) async {
        await orchestrator.receiveAdWindows([
            Fx.makeFieldDayZeroWindow(id: sentinelId, start: time, end: time + 4)
        ])
        await orchestrator.updatePlayheadTime(time)
    }

    private static func bannersForMarkOnlyWindow(
        skipMode: SkipMode
    ) async throws -> [String] {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(store: store)
        var reader = BannerReader(await orchestrator.bannerItemStream())
        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.seededPodcastId
        )
        await orchestrator.setActiveSkipMode(skipMode)
        #expect(await orchestrator.currentSkipMode() == skipMode,
                "control: the mode under test is actually installed")

        await orchestrator.receiveAdWindows([
            Fx.makeFieldDayZeroWindow(id: "markonly-1", start: 0, end: 45.1)
        ])
        await orchestrator.updatePlayheadTime(14)
        await fireSentinel(orchestrator, at: 5_000)
        return await reader.drain(until: sentinelId).map(\.windowId)
    }

    /// The bead's leading hypothesis was that `.shadow` explained the missing
    /// banner. It cannot: `activeSkipMode` gates only the auto-skip tier.
    /// A mark-only window banners identically in every mode.
    @Test("a markOnly window banners in shadow exactly as it does in auto",
          arguments: [SkipMode.shadow, .manual, .auto])
    func markOnlyBannersInEveryMode(mode: SkipMode) async throws {
        let banners = try await Self.bannersForMarkOnlyWindow(skipMode: mode)
        #expect(banners == ["markonly-1"],
                "shadow does not suppress the suggest tier — \(mode.rawValue) emitted \(banners)")
    }
}
