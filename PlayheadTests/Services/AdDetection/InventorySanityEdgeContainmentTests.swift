// InventorySanityEdgeContainmentTests.swift
// playhead-b6r2 — the inventory filter's edge rules read the INNER edge.
//
// THE DEFECT. playhead-xr3t's rule (b) was specified as "reject spans IN the
// first / last 3 s of the episode" and implemented as:
//
//     if startTime < edgeMarginSeconds { return .rejected(reason: .tooEarly) }
//     if endTime > episodeDuration - edgeMarginSeconds { .tooLate }
//
// A pre-roll starts at 0.0 and a post-roll ends at the episode end, so the
// implementation rejects 100 % of the population it was aimed at. The 2026-08-01
// field session (episode D9B513CD) is the measurement: day-0 rediff minted a
// byte-exact pre-roll at 0.0–45.1 s at confidence 1.00 and the listener got no
// banner and heard the ad.
//
// THE READING THIS BEAD LANDS. Dan's 2026-07-29 ruling — "outer edges are free
// to widen, inner edges eat the show" — makes the correct question obvious. A
// pre-roll's OUTER edge is its start, pinned to 0 by the episode boundary; it
// cannot cost a second of show and there is nothing to guard. Its INNER edge is
// its end. A post-roll's outer edge is its end; its inner edge is its start. So
// rule (b) now measures the inner edge and ignores the free one:
//
//     tooEarly iff endTime   <= edgeMargin
//     tooLate  iff startTime >= duration - edgeMargin
//
// which, given rule (a)'s duration floor and the orchestrator's `startTime >= 0`
// material check, is exactly the spec's own words: the span lies WITHIN the
// margin band. A 45-second pre-roll passes. A 2-second head artifact does not.
//
// WHAT THESE TESTS PIN, and the one thing they deliberately pin as a COST:
// `theEdgeRulesNoLongerRejectASpanThatSwallowsTheWholeEpisode` records what the
// change gives up. It is not an oversight — see its own note.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixtures

private enum EdgeFixture {

    static let assetId = "asset-b6r2"
    static let episodeId = "ep-b6r2"
    static let podcastId = "podcast-b6r2"

    /// The four spans day-0 minted for D9B513CD on 2026-08-01, byte-identical
    /// to playhead-isp5's fixture.
    static let preRoll = (id: "d0-1", start: 0.0, end: 45.1)
    static let midRollA = (id: "d0-2", start: 1_436.4, end: 1_508.1)
    static let midRollB = (id: "d0-3", start: 3_194.5, end: 3_371.2)
    static let postRoll = (id: "d0-4", start: 3_899.8, end: 3_929.9)

    /// The field record fixes the post-roll's END (3929.9 s) and that it is the
    /// LAST slot; it does not carry the episode's own duration. Any duration
    /// within `edgeMarginSeconds` of that end reproduces the tail rule's reach,
    /// and this is the smallest such choice that still contains the span.
    static let episodeDuration = 3_931.0

    static func window(
        id: String,
        start: Double,
        end: Double,
        gate: String = SkipEligibilityGate.markOnly.rawValue
    ) -> AdWindow {
        makeSkipTestAdWindow(
            id: id,
            assetId: assetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            decisionState: AdDecisionState.candidate.rawValue,
            eligibilityGate: gate
        )
    }

    static var fieldWindows: [AdWindow] {
        [
            window(id: preRoll.id, start: preRoll.start, end: preRoll.end),
            window(id: midRollA.id, start: midRollA.start, end: midRollA.end),
            window(id: midRollB.id, start: midRollB.start, end: midRollB.end),
            window(id: postRoll.id, start: postRoll.start, end: postRoll.end),
        ]
    }

    /// - Parameters:
    ///   - inventoryFilterEnabled: `nil` uses the `init` DEFAULT — the thing
    ///     playhead-b6r2 is binding to production. A `Bool` passes an explicit
    ///     filter, which is how a suite reproduces a configuration on purpose.
    ///   - episodeDuration: written to the asset row, so `beginEpisode` hydrates
    ///     `activeEpisodeDuration` exactly as it does in the field.
    static func makeOrchestrator(
        store: AnalysisStore,
        inventoryFilterEnabled: Bool?,
        episodeDuration: Double? = nil,
        invariantLogger: SurfaceStatusInvariantLogger = SurfaceStatusInvariantLogger()
    ) async throws -> SkipOrchestrator {
        try await store.insertAsset(
            makeSkipTestAnalysisAsset(
                id: assetId,
                episodeId: episodeId,
                episodeDurationSec: episodeDuration
            )
        )
        if let inventoryFilterEnabled {
            return SkipOrchestrator(
                store: store,
                invariantLogger: invariantLogger,
                inventoryFilter: InventorySanityFilter(
                    isEnabled: inventoryFilterEnabled
                )
            )
        }
        return SkipOrchestrator(store: store, invariantLogger: invariantLogger)
    }

    static func deliver(
        _ orchestrator: SkipOrchestrator,
        windows: [AdWindow]
    ) async {
        await orchestrator.beginEpisode(
            analysisAssetId: assetId, episodeId: episodeId, podcastId: podcastId
        )
        await orchestrator.updatePlayheadTime(14)
        await orchestrator.receiveAdWindows(windows)
    }
}

// MARK: - 1. The rule itself

@Suite("The edge rules measure the inner edge (playhead-b6r2)")
struct InventorySanityInnerEdgeRuleTests {

    private let filter = InventorySanityFilter(isEnabled: true)

    /// THE FIELD SPAN. 45.1 s of pre-roll pinned to the episode start.
    @Test("the field pre-roll at 0.0-45.1 passes")
    func theFieldPreRollPasses() {
        #expect(filter.evaluate(
            startTime: EdgeFixture.preRoll.start,
            endTime: EdgeFixture.preRoll.end,
            episodeDuration: EdgeFixture.episodeDuration,
            declaredChapters: []
        ) == .passed)
    }

    /// The same defect mirrored. A post-roll ends at the episode end by
    /// definition, so the old tail rule rejected the whole class.
    @Test("the field post-roll ending 1.1 s before the episode end passes")
    func theFieldPostRollPasses() {
        #expect(filter.evaluate(
            startTime: EdgeFixture.postRoll.start,
            endTime: EdgeFixture.postRoll.end,
            episodeDuration: EdgeFixture.episodeDuration,
            declaredChapters: []
        ) == .passed)
    }

    /// The head rule keeps a real job: a span whose INNER edge is still inside
    /// the margin is a head artifact, not a pre-roll. 2.0 s exactly, so rule (a)
    /// (`duration >= 2.0` passes) provably did NOT make this call — the reason
    /// must read `.tooEarly`.
    @Test("a two-second head artifact is still rejected, and by rule (b)")
    func aTwoSecondHeadArtifactIsStillTooEarly() {
        #expect(filter.evaluate(
            startTime: 0,
            endTime: 2.0,
            episodeDuration: 600,
            declaredChapters: []
        ) == .rejected(reason: .tooEarly))
    }

    /// The tail rule keeps the mirrored job, with the same rule-attribution
    /// control: 2.0 s exactly, so a `.tooLate` verdict cannot have come from
    /// the duration floor.
    @Test("a two-second tail artifact is still rejected, and by rule (b)")
    func aTwoSecondTailArtifactIsStillTooLate() {
        #expect(filter.evaluate(
            startTime: 598,
            endTime: 600,
            episodeDuration: 600,
            declaredChapters: []
        ) == .rejected(reason: .tooLate))
    }

    /// The head boundary. A span that exactly fills the margin band is IN the
    /// first three seconds; one that pokes out of it by any amount is not.
    @Test("the head boundary is the inner edge at exactly edgeMarginSeconds")
    func theHeadBoundaryIsTheInnerEdge() {
        let atBoundary = filter.evaluate(
            startTime: 0, endTime: 3.0,
            episodeDuration: 600, declaredChapters: []
        )
        let justPast = filter.evaluate(
            startTime: 0, endTime: 3.001,
            episodeDuration: 600, declaredChapters: []
        )
        #expect(atBoundary == .rejected(reason: .tooEarly))
        #expect(justPast == .passed)
    }

    /// The tail boundary, mirrored.
    @Test("the tail boundary is the inner edge at exactly duration - edgeMarginSeconds")
    func theTailBoundaryIsTheInnerEdge() {
        let atBoundary = filter.evaluate(
            startTime: 597.0, endTime: 600,
            episodeDuration: 600, declaredChapters: []
        )
        let justBefore = filter.evaluate(
            startTime: 596.999, endTime: 600,
            episodeDuration: 600, declaredChapters: []
        )
        #expect(atBoundary == .rejected(reason: .tooLate))
        #expect(justBefore == .passed)
    }

    /// The head rule never needed the duration and still does not — the tail
    /// rule is the only one that degrades to a no-op on an unknown denominator.
    /// The second expectation is the vacuity control: without it, a head rule
    /// that rejected EVERYTHING on a nil duration would satisfy the first.
    @Test("the head rule survives an unknown episode duration")
    func theHeadRuleSurvivesAnUnknownDuration() {
        #expect(filter.evaluate(
            startTime: 0, endTime: 2.5,
            episodeDuration: nil, declaredChapters: []
        ) == .rejected(reason: .tooEarly))
        #expect(filter.evaluate(
            startTime: EdgeFixture.preRoll.start,
            endTime: EdgeFixture.preRoll.end,
            episodeDuration: nil, declaredChapters: []
        ) == .passed, "vacuity control: the head rule is not rejecting everything")
    }

    /// THE COST OF THIS CHANGE, STATED. The old tail rule rejected every span
    /// ending in the last three seconds, so it incidentally caught a span that
    /// swallowed the whole episode. The new rule does not.
    ///
    /// That protection was never coherent and this test exists so nobody
    /// rediscovers its loss as a surprise:
    ///   * it keyed on the OUTER edge, the one Dan ruled free, so it rejected
    ///     `[3928, 3930]` and `[0, 3930]` on identical grounds;
    ///   * it admitted `[500, 3920]` — 57 minutes of show — because that span
    ///     happens to stop ten seconds early. A guard whose verdict turns on
    ///     where a span ENDS is not a width guard.
    /// Separating a 30-second post-roll from a 57-minute one needs a WIDTH
    /// test. Rule (a) is a floor by design and this filter has no ceiling;
    /// adding one is a policy decision (what is the widest legitimate ad
    /// break?) that belongs to Dan, not a silent side effect of this fix.
    @Test("the edge rules no longer reject a span that swallows the whole episode")
    func theEdgeRulesNoLongerRejectASpanThatSwallowsTheWholeEpisode() {
        #expect(filter.evaluate(
            startTime: 0,
            endTime: EdgeFixture.episodeDuration,
            episodeDuration: EdgeFixture.episodeDuration,
            declaredChapters: []
        ) == .passed)
    }
}

// MARK: - 2. The field delivery, measured through the census

@Suite("The field pre-roll and post-roll reach the suggest tier (playhead-b6r2)",
       .timeLimit(.minutes(1)))
struct FieldEdgeWindowsArmTests {

    private typealias Fx = EdgeFixture

    /// The bead's verification, run through playhead-isp5's census rather than
    /// through the absence of a banner: `ingest_dropped_inventory_sanity:
    /// tooEarly` must read zero for a pre-roll, and the window must appear as
    /// `ingest_armed_suggest`.
    @Test("the field pre-roll arms as a suggestion")
    func theFieldPreRollArms() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store,
            inventoryFilterEnabled: true,
            episodeDuration: Fx.episodeDuration
        )
        await Fx.deliver(orchestrator, windows: Fx.fieldWindows)

        let preRoll = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.preRoll.id
        )
        #expect(preRoll?.outcome == .armedSuggest)
    }

    /// The mirrored half, and the reason the asset row carries a duration: the
    /// tail rule is dormant on a nil denominator, so a suite built on the
    /// duration-less test asset cannot see this loss at all. Production pushes
    /// a duration before every `receiveAdWindows`
    /// (`AnalysisCoordinator`, both the hot path and the final pass).
    @Test("the field post-roll arms once the episode duration is known")
    func theFieldPostRollArms() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store,
            inventoryFilterEnabled: true,
            episodeDuration: Fx.episodeDuration
        )
        await Fx.deliver(orchestrator, windows: Fx.fieldWindows)

        let postRoll = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: Fx.postRoll.id
        )
        #expect(postRoll?.outcome == .armedSuggest)
    }

    /// The whole delivery, counted, WITH THE DURATION KNOWN — which is what
    /// makes this distinct from `AdWindowIngestOutcomeCountTests`' version of
    /// the same count. There the asset row has no duration, so the tail rule
    /// is dormant and only the pre-roll was ever at risk; here both edge rules
    /// are armed and both edge-anchored slots have to survive.
    @Test("with the duration known, the production filter drops no field window")
    func theProductionFilterDropsNoFieldWindowWithDurationKnown() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store,
            inventoryFilterEnabled: true,
            episodeDuration: Fx.episodeDuration
        )
        await Fx.deliver(orchestrator, windows: Fx.fieldWindows)

        #expect(await orchestrator.adWindowIngestOutcomeCount(.armedSuggest) == 4)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.droppedInventorySanity) == 0)
    }

    /// The absence above, made non-vacuous. The SAME delivery carries one
    /// genuinely-invalid row, so the census proves it can still say "dropped"
    /// while saying nothing about the pre-roll — rather than proving only that
    /// the filter went quiet.
    @Test("a filter that still rejects is what makes the pre-roll's survival evidence")
    func theFilterStillRejectsInTheSameDelivery() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store,
            inventoryFilterEnabled: true,
            episodeDuration: Fx.episodeDuration
        )
        await Fx.deliver(
            orchestrator,
            windows: Fx.fieldWindows
                + [Fx.window(id: "head-artifact", start: 0, end: 2.0)]
        )

        let artifact = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: "head-artifact"
        )
        #expect(artifact?.outcome == .droppedInventorySanity)
        #expect(artifact?.detail == InventorySanityRejectionReason.tooEarly.rawValue)
        #expect(await orchestrator.adWindowIngestOutcomeCount(.armedSuggest) == 4,
                "vacuity control: the four field windows still armed alongside it")
    }
}

// MARK: - 2b. The outcome Dan reported missing

/// Single-consumer reader over the banner stream, the playhead-d3g0 pattern.
/// Owns the iterator so a pull returns already-buffered items without
/// depending on any other task being scheduled.
private struct EdgeBannerReader {
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

@Suite("The field pre-roll banners with the listener inside it (playhead-b6r2)",
       .timeLimit(.minutes(1)))
struct FieldPreRollBannerTests {

    private typealias Fx = EdgeFixture

    /// `armedSuggest` is an internal disposition; this is the sentence Dan
    /// wrote. On 2026-08-01 the playhead sat at 14 s INSIDE a 0.0–45.1 s
    /// pre-roll and no card appeared. playhead-d3g0's entry gate tests
    /// `time >= start && time < end` on each observation rather than an
    /// outside->inside transition, so a listener already inside the span is
    /// asked on the very next tick — once the window survives ingest at all.
    @Test("the pre-roll's banner reaches the listener")
    func thePreRollBanners() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store,
            inventoryFilterEnabled: true,
            episodeDuration: Fx.episodeDuration
        )
        var reader = EdgeBannerReader(await orchestrator.bannerItemStream())

        await orchestrator.beginEpisode(
            analysisAssetId: Fx.assetId,
            episodeId: Fx.episodeId,
            podcastId: Fx.podcastId
        )
        await orchestrator.receiveAdWindows(Fx.fieldWindows)
        await orchestrator.updatePlayheadTime(14)

        // A span the playhead reaches AFTER the pre-roll, used only to bound
        // the read: its arrival proves the pre-roll's chance to emit is over.
        await orchestrator.receiveAdWindows([
            Fx.window(id: "b6r2-sentinel", start: 200, end: 240),
        ])
        await orchestrator.updatePlayheadTime(210)

        let emitted = await reader.drain(until: "b6r2-sentinel")
        #expect(emitted.map(\.windowId) == [Fx.preRoll.id])
        #expect(emitted.allSatisfy { $0.tier == .suggest })
    }
}

// MARK: - 3. The init default no longer diverges from production

@Suite("A default-constructed orchestrator runs the field's filter (playhead-b6r2)",
       .timeLimit(.minutes(1)))
struct InventoryFilterDefaultDivergenceTests {

    private typealias Fx = EdgeFixture

    /// THE ROOT CAUSE OF TWO LOST INVESTIGATIONS. `SkipOrchestrator.init`
    /// defaulted `inventoryFilter` to `InventorySanityFilter(isEnabled: false)`
    /// while production wired `.production()`, which is ON. Every orchestrator
    /// suite inherited a live production guard turned OFF — which is why
    /// playhead-djl0's own reproduction of the 2026-08-01 field case asserted
    /// the banner IS emitted and PASSED.
    ///
    /// This asserts the divergence is gone the only way that matters: by
    /// observing a default-constructed orchestrator enforce a rule it used to
    /// ignore. The second expectation is the vacuity control — a default that
    /// rejected everything would satisfy the first.
    @Test("the init default enforces the filter, with no argument passed")
    func theInitDefaultEnforcesTheFilter() async throws {
        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: nil
        )
        await Fx.deliver(orchestrator, windows: [
            Fx.window(id: "sliver", start: 60, end: 60.5),
            Fx.window(id: "genuine", start: 100, end: 160),
        ])

        let sliver = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: "sliver"
        )
        #expect(sliver?.outcome == .droppedInventorySanity)

        let genuine = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: "genuine"
        )
        #expect(genuine?.outcome == .armedSuggest,
                "vacuity control: the default is a filter, not a wall")
    }

    /// The structural half, and it is not a tautology: it runs the real
    /// `UserDefaults` load path with the key ABSENT, which is the state Dan's
    /// 2026-07-31 wipe-and-reinstall left the device in and therefore the
    /// configuration the field defect was observed under. Equatable compares
    /// all three fields, so a future edit to the margins diverges here too.
    @Test("the init default equals what production loads on a fresh install")
    func theInitDefaultEqualsProductionOnAFreshInstall() {
        let suiteName = "b6r2.freshinstall.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defaults.removePersistentDomain(forName: suiteName)

        let freshInstall = InventorySanityFilter.production(
            settings: LightweightInventoryChecksSettings.load(from: defaults)
        )
        #expect(freshInstall == InventorySanityFilter.productionDefaultConfiguration)
        #expect(freshInstall.isEnabled,
                "vacuity control: a fresh install runs the filter, so equality is a real claim")
    }
}

// MARK: - 4. The guard that moved rather than died

@Suite("Impossible geometry is refused as material, not as an edge (playhead-b6r2)",
       .timeLimit(.minutes(1)))
struct ImpossibleGeometryIsMaterialTests {

    private typealias Fx = EdgeFixture

    /// xr3t's `negativeStartButLongSpanReachingEpisodeStartHandledSafely`
    /// asserted that `[-5, 30]` is `.tooEarly`. Under the inner-edge reading it
    /// is not: its inner edge is 30 s, well clear of the margin. That is
    /// correct, and the guard did not die — it lives where it belongs.
    ///
    /// A negative start is IMPOSSIBLE MATERIAL, not an edge-policy question,
    /// and `SkipOrchestrator.hasValidRuntimeWindowMaterial` requires
    /// `startTime >= 0` at both admission doors. Conflating the two is how
    /// rule (b) came to reject pre-rolls: a rule asked to be a material check
    /// as well as an edge policy answers neither question honestly.
    @Test("a negative start is refused by the material check, not the edge rule")
    func aNegativeStartIsRefusedAsMaterial() async throws {
        let filter = InventorySanityFilter(isEnabled: true)
        #expect(filter.evaluate(
            startTime: -5, endTime: 30,
            episodeDuration: 600, declaredChapters: []
        ) == .passed, "the edge rule has no opinion on impossible material")

        let store = try await makeTestStore()
        let orchestrator = try await Fx.makeOrchestrator(
            store: store, inventoryFilterEnabled: true, episodeDuration: 600
        )
        await Fx.deliver(orchestrator, windows: [
            Fx.window(id: "negative-start", start: -5, end: 30),
        ])
        let refused = await orchestrator.lastAdWindowIngestOutcome(
            forWindowId: "negative-start"
        )
        #expect(refused?.outcome == .droppedInvalidMaterial)
    }
}
