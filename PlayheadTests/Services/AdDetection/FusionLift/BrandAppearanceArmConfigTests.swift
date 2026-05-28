// BrandAppearanceArmConfigTests.swift
// playhead-brandab live A/B — hermetic, SYNTHETIC unit tests for the
// brand-appearance precision-signal A/B support in
// `FusionLiftHarnessSupport.swift` plus the fire-instrumentation observer. No
// audio, no Foundation Models, no live pipeline — every input is a hand-built
// value, so these run on the simulator in the default `PlayheadFastTests` plan
// (they do NOT need `PLAYHEAD_BRANDAPPEARANCE_AB=1`; that env var only gates the
// SLOW live harness in `BrandAppearanceLiveABTests`).
//
// They pin the LOAD-BEARING correctness properties before the (expensive,
// Catalyst-only) live A/B ever runs:
//   1. ARM ISOLATION: the four arms differ ONLY in the two brand-appearance
//      flags (`rhetoricalGrammarEnabled` / `crossShowSyndicationEnabled`); every
//      other field is byte-identical and equal to `AdDetectionConfig.default`. A
//      drift on any other field would attribute a precision change to a signal
//      that some OTHER flag caused.
//   2. STORE GATING PARITY: only the two flag-on arms require a syndication
//      store (the flag's production consequence).
//   3. PUBLISH-DATE PARSING: the corpus id → publish date derivation is correct
//      and ordered, since the xsdz.13 persistence gate depends on it.
//   4. FIRE INSTRUMENTATION: the nil-default channel-tap observer records the
//      per-channel fire counts so a null live result is interpretable.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("Brand-appearance A/B arm config (playhead-brandab)")
struct BrandAppearanceArmConfigTests {

    // MARK: - Arm enumeration

    @Test("A/B enumerates exactly four arms, baseline first")
    func arm_fourArmsBaselineFirst() {
        #expect(BrandAppearanceArm.allCases == [.baseline, .xsdz12Only, .xsdz13Only, .bothOn])
    }

    @Test("each arm's two toggles match its intent")
    func arm_togglesPerArm() {
        #expect(BrandAppearanceArm.baseline.xsdz12On == false)
        #expect(BrandAppearanceArm.baseline.xsdz13On == false)
        #expect(BrandAppearanceArm.xsdz12Only.xsdz12On == true)
        #expect(BrandAppearanceArm.xsdz12Only.xsdz13On == false)
        #expect(BrandAppearanceArm.xsdz13Only.xsdz12On == false)
        #expect(BrandAppearanceArm.xsdz13Only.xsdz13On == true)
        #expect(BrandAppearanceArm.bothOn.xsdz12On == true)
        #expect(BrandAppearanceArm.bothOn.xsdz13On == true)
    }

    @Test("only the two flag-ON arms require a syndication store (gating parity)")
    func arm_storeGatingParity() {
        #expect(BrandAppearanceArm.baseline.requiresSyndicationStore == false)
        #expect(BrandAppearanceArm.xsdz12Only.requiresSyndicationStore == false)
        #expect(BrandAppearanceArm.xsdz13Only.requiresSyndicationStore == true)
        #expect(BrandAppearanceArm.bothOn.requiresSyndicationStore == true)
        // The store requirement IS the xsdz.13 flag — never an independent axis.
        for arm in BrandAppearanceArm.allCases {
            #expect(arm.requiresSyndicationStore == arm.xsdz13On,
                    "arm \(arm.rawValue): store presence must equal the xsdz.13 flag")
        }
    }

    // MARK: - Per-arm config flags

    @Test("each arm's config carries exactly its two flag toggles")
    func config_flagsPerArm() {
        for arm in BrandAppearanceArm.allCases {
            let c = BrandAppearanceArmConfig.adDetectionConfig(for: arm)
            #expect(c.rhetoricalGrammarEnabled == arm.xsdz12On, "arm \(arm.rawValue) rhetoricalGrammarEnabled")
            #expect(c.crossShowSyndicationEnabled == arm.xsdz13On, "arm \(arm.rawValue) crossShowSyndicationEnabled")
        }
    }

    @Test("baseline arm equals the production default (both signals OFF)")
    func config_baselineIsProductionDefault() {
        let baseline = BrandAppearanceArmConfig.adDetectionConfig(for: .baseline)
        #expect(baseline.rhetoricalGrammarEnabled == false)
        #expect(baseline.crossShowSyndicationEnabled == false)
        #expect(baseline.rhetoricalGrammarEnabled == AdDetectionConfig.default.rhetoricalGrammarEnabled)
        #expect(baseline.crossShowSyndicationEnabled == AdDetectionConfig.default.crossShowSyndicationEnabled)
    }

    // MARK: - The load-bearing isolation property

    @Test("every arm differs from .default ONLY in the two brand-appearance flags")
    func config_isolation_onlyTwoFlagsVary() {
        let prod = AdDetectionConfig.default
        for arm in BrandAppearanceArm.allCases {
            let config = BrandAppearanceArmConfig.adDetectionConfig(for: arm)
            // The two fields the A/B is allowed to vary land at the arm's toggles.
            #expect(config.rhetoricalGrammarEnabled == arm.xsdz12On)
            #expect(config.crossShowSyndicationEnabled == arm.xsdz13On)
            // EVERY other field equals the production default.
            for field in BrandAppearanceArmConfig.comparableFields {
                #expect(
                    field.value(config) == field.value(prod),
                    "arm \(arm.rawValue): field \(field.name) drifted from .default: arm=\(field.value(config)) default=\(field.value(prod))"
                )
            }
        }
    }

    @Test("any two arms agree on every non-flag field")
    func config_isolation_pairwiseEqualOffFlags() {
        let configs = BrandAppearanceArm.allCases.map {
            (arm: $0, config: BrandAppearanceArmConfig.adDetectionConfig(for: $0))
        }
        for i in configs.indices {
            for j in configs.indices where j > i {
                for field in BrandAppearanceArmConfig.comparableFields {
                    #expect(
                        field.value(configs[i].config) == field.value(configs[j].config),
                        "arms \(configs[i].arm.rawValue)/\(configs[j].arm.rawValue): field \(field.name) drifted"
                    )
                }
                // `comparableFields` derives from FragilityGateArmConfig.comparableFields,
                // which intentionally OMITS `evidenceFragilityPenaltyEnabled` (it is the
                // fragility A/B's varying field). This A/B does NOT vary it, so close the
                // gap explicitly: it must stay equal across arms AND equal to .default.
                #expect(
                    configs[i].config.evidenceFragilityPenaltyEnabled
                        == configs[j].config.evidenceFragilityPenaltyEnabled,
                    "evidenceFragilityPenaltyEnabled drifted between \(configs[i].arm.rawValue)/\(configs[j].arm.rawValue)"
                )
            }
        }
        for c in configs {
            #expect(
                c.config.evidenceFragilityPenaltyEnabled == AdDetectionConfig.default.evidenceFragilityPenaltyEnabled,
                "arm \(c.arm.rawValue): evidenceFragilityPenaltyEnabled must equal .default"
            )
        }
    }

    @Test("comparableFields EXCLUDES exactly the two varying flags but keeps representative fields")
    func config_comparableFieldsExcludesFlags() {
        let names = Set(BrandAppearanceArmConfig.comparableFields.map(\.name))
        #expect(!names.contains("rhetoricalGrammarEnabled"))
        #expect(!names.contains("crossShowSyndicationEnabled"))
        // Still non-vacuous: it must include representative non-flag fields so the
        // isolation check is not empty.
        #expect(names.contains("fmBackfillMode"))
        #expect(names.contains("autoSkipConfidenceThreshold"))
        #expect(names.contains("chapterSignalMode"))
    }

    @Test("the four arms span exactly four distinct (xsdz12, xsdz13) flag combinations")
    func config_armsSpanFullCross() {
        let combos = Set(BrandAppearanceArm.allCases.map { "\($0.xsdz12On)-\($0.xsdz13On)" })
        #expect(combos == ["false-false", "true-false", "false-true", "true-true"])
    }

    @Test("baseline pins the explicit production flag/mode invariants the bead names")
    func config_baselineNamedInvariants() {
        let baseline = BrandAppearanceArmConfig.adDetectionConfig(for: .baseline)
        // fmBackfillMode .full → real FM scan feeds the fusion ledger.
        #expect(baseline.fmBackfillMode == .full)
        #expect(baseline.chapterSignalMode == .off)
        // ALL off-by-default evidence-channel flags FALSE on the baseline.
        #expect(baseline.rhetoricalGrammarEnabled == false)
        #expect(baseline.crossShowSyndicationEnabled == false)
        #expect(baseline.evidenceFragilityPenaltyEnabled == false)
        #expect(baseline.audioForensicsEnabled == false)
        #expect(baseline.crossEpisodeMemoryEnabled == false)
        #expect(baseline.temporalRegularizationEnabled == false)
        #expect(baseline.lexicalAutoAdEnabled == false)
    }

    // MARK: - NarrowingConfig invariant (snap ON for every arm)

    @Test("every arm uses NarrowingConfig.default (snap ON) and the config never varies")
    func narrowing_everyArmDefaultSnapOn() {
        for arm in BrandAppearanceArm.allCases {
            let narrowing = BrandAppearanceArmConfig.narrowingConfig(for: arm)
            #expect(narrowing == NarrowingConfig.default, "arm \(arm.rawValue): narrowing must be .default")
            #expect(narrowing.lexicalClusterSnapEnabled == true,
                    "arm \(arm.rawValue): snap must be on (production state)")
        }
        #expect(
            BrandAppearanceArmConfig.narrowingConfig(for: .baseline)
                == BrandAppearanceArmConfig.narrowingConfig(for: .bothOn),
            "narrowing must be identical across arms — only the two flags vary"
        )
    }
}

// MARK: - Publish-date parsing

@Suite("Brand-appearance publish date parsing (playhead-brandab)")
struct BrandAppearancePublishDateTests {

    private func ymd(_ date: Date) -> (Int, Int, Int) {
        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = TimeZone(identifier: "UTC") ?? .current
        let c = cal.dateComponents([.year, .month, .day], from: date)
        return (c.year ?? 0, c.month ?? 0, c.day ?? 0)
    }

    @Test("parses the YYYY-MM-DD token out of a real corpus episode id")
    func parse_realCorpusId() throws {
        let date = try #require(BrandAppearancePublishDate.parse(
            fromEpisodeId: "doac-2026-05-07-ww3-expert-this-could-trigger-global-starvation"
        ))
        #expect(ymd(date) == (2026, 5, 7))
    }

    @Test("parses an older corpus id (different year)")
    func parse_olderYear() throws {
        let date = try #require(BrandAppearancePublishDate.parse(
            fromEpisodeId: "ghost-machine-2024-06-28-episode-7-we-will-catch-you"
        ))
        #expect(ymd(date) == (2024, 6, 28))
    }

    @Test("ignores numeric-looking show prefixes and finds the real date token")
    func parse_numericShowPrefix() throws {
        // "99pi" contains digits but is not a 4-digit year token; the parser must
        // skip it and land on the real date.
        let date = try #require(BrandAppearancePublishDate.parse(
            fromEpisodeId: "99pi-2026-05-05-enshittification"
        ))
        #expect(ymd(date) == (2026, 5, 5))
    }

    @Test("returns nil when no date token is present")
    func parse_noDate() {
        #expect(BrandAppearancePublishDate.parse(fromEpisodeId: "some-show-no-date-here") == nil)
        #expect(BrandAppearancePublishDate.parse(fromEpisodeId: "short") == nil)
    }

    @Test("rejects an out-of-range month/day so a coincidental numeric triple is not a date")
    func parse_rejectsBadRanges() {
        // 2026-13-40 is not a valid date → no token matches → nil.
        #expect(BrandAppearancePublishDate.parse(fromEpisodeId: "show-2026-13-40-bad") == nil)
    }

    @Test("publish dates sort the corpus chronologically (spread spans >14 days)")
    func parse_chronologicalSpread() throws {
        let ids = [
            "ghost-machine-2024-06-28-ep",
            "doac-2026-05-08-newer",
            "99pi-2026-05-05-mid",
        ]
        let sorted = try ids
            .map { (id: $0, date: try #require(BrandAppearancePublishDate.parse(fromEpisodeId: $0))) }
            .sorted { $0.date < $1.date }
        #expect(sorted.map(\.id) == [
            "ghost-machine-2024-06-28-ep",
            "99pi-2026-05-05-mid",
            "doac-2026-05-08-newer",
        ])
        // The earliest→latest span is far more than 14 days — so on a corpus that
        // shares a sponsor across these shows, the persistence gate CAN be met.
        let span = sorted.last!.date.timeIntervalSince(sorted.first!.date) / 86_400.0
        #expect(span > 14.0)
    }
}

// MARK: - Fire instrumentation observer

@Suite("Brand-appearance channel-tap observer (playhead-brandab)")
struct BrandAppearanceChannelTapObserverTests {

    private func entry(_ source: EvidenceSourceType, weight: Double) -> EvidenceLedgerEntry {
        EvidenceLedgerEntry(source: source, weight: weight, detail: .catalog(entryCount: 1))
    }

    @Test("tallies a span only when a channel emitted a positive entry")
    func tap_countsPositiveEntriesOnly() async {
        let tap = BrandAppearanceChannelTapObserver()
        // Span 1: both channels fired.
        await tap.record(assetId: "ep1", ledger: [
            entry(.rhetoricalGrammar, weight: 0.1),
            entry(.crossShowSyndication, weight: 0.2),
            entry(.acoustic, weight: 0.3),
        ])
        // Span 2: only grammar fired.
        await tap.record(assetId: "ep1", ledger: [entry(.rhetoricalGrammar, weight: 0.05)])
        // Span 3: neither fired (a zero-weight syndication entry must NOT count).
        await tap.record(assetId: "ep1", ledger: [
            entry(.crossShowSyndication, weight: 0.0),
            entry(.lexical, weight: 0.2),
        ])

        let counts = await tap.fireCounts(for: "ep1")
        #expect(counts.observedSpans == 3)
        #expect(counts.rhetoricalGrammarFiredSpans == 2)
        #expect(counts.crossShowSyndicationFiredSpans == 1)
    }

    @Test("counts accumulate per asset and are isolated across assets")
    func tap_perAssetIsolation() async {
        let tap = BrandAppearanceChannelTapObserver()
        await tap.record(assetId: "epA", ledger: [entry(.rhetoricalGrammar, weight: 0.1)])
        await tap.record(assetId: "epB", ledger: [entry(.crossShowSyndication, weight: 0.1)])

        let a = await tap.fireCounts(for: "epA")
        let b = await tap.fireCounts(for: "epB")
        #expect(a.rhetoricalGrammarFiredSpans == 1)
        #expect(a.crossShowSyndicationFiredSpans == 0)
        #expect(b.rhetoricalGrammarFiredSpans == 0)
        #expect(b.crossShowSyndicationFiredSpans == 1)
        // An unseen asset returns zeroed defaults (never a crash / nil ambiguity).
        let empty = await tap.fireCounts(for: "never-seen")
        #expect(empty == BrandAppearanceChannelFireCounts())
    }

    @Test("the per-arm fire tally folds per-episode counts additively")
    func fireTally_foldsAdditively() {
        var tally = BrandAppearanceFireTally()
        tally.add(BrandAppearanceChannelFireCounts(
            rhetoricalGrammarFiredSpans: 2,
            crossShowSyndicationFiredSpans: 1,
            observedSpans: 5
        ))
        tally.add(BrandAppearanceChannelFireCounts(
            rhetoricalGrammarFiredSpans: 3,
            crossShowSyndicationFiredSpans: 0,
            observedSpans: 4
        ))
        #expect(tally.rhetoricalGrammarFiredSpans == 5)
        #expect(tally.crossShowSyndicationFiredSpans == 1)
        #expect(tally.observedSpans == 9)
        // gatedEntities is set separately (post-pass) — defaults to 0.
        #expect(tally.syndicationGatedEntities == 0)
    }
}

// MARK: - xsdz.13 shared-store cross-episode accumulation (the #1 correctness risk)

/// These pin that ONE store SHARED across episodes accumulates cross-show spread
/// + temporal persistence (publish-date stamped) and reaches the production gate,
/// whereas a PER-EPISODE store cannot. This is the load-bearing xsdz.13 property
/// the live harness depends on; pinning it hermetically (no audio/FM) guards
/// against a regression that would silently zero out the syndication signal.
@Suite("Brand-appearance shared syndication store accumulation (playhead-brandab)")
struct BrandAppearanceSharedStoreAccumulationTests {

    private let day: Double = 86_400

    @Test("a SHARED store accumulates a syndicated entity across shows over time and reaches the gate")
    func shared_storeReachesGate() async throws {
        let dir = try makeTempDir(prefix: "brandab-shared-accum")
        let store = try CrossShowSyndicationStore(directoryURL: dir)
        try await store.migrate()

        // Same sponsor entity observed across 3 DISTINCT shows, stamped at publish
        // dates spanning ~30 days (mirrors processing episodes in publish-date
        // order, where each episode's observations are stamped at its real date).
        let now = Date().timeIntervalSince1970
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "show-A", confidence: 0.9, now: now - 30 * day)
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "show-B", confidence: 0.9, now: now - 16 * day)
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "show-C", confidence: 0.9, now: now)

        let totalShows = await store.totalObservedShowCount()
        let profile = try #require(await store.spreadProfile(forEntity: "betterhelp", totalObservedShows: totalShows))

        // Spread: 3 of 3 distinct shows = 1.0 ≥ 0.40; distinct = 3 ≥ 3;
        // persistence = 30 days ≥ 14. The production evaluator must qualify it.
        #expect(profile.distinctShowCount == 3)
        #expect(profile.spreadRatio >= 0.40)
        #expect(profile.persistenceDays >= 14.0)
        #expect(CrossShowSyndicationEvaluator().qualifies(profile))

        await store.close()
    }

    @Test("a PER-EPISODE (single-show, single-moment) store does NOT reach the gate")
    func perEpisode_storeDoesNotReachGate() async throws {
        // Simulates the WRONG design: a fresh store per episode only ever sees ONE
        // show at ONE moment, so distinct-shows = 1 (< 3) and persistence = 0.
        let dir = try makeTempDir(prefix: "brandab-perepisode")
        let store = try CrossShowSyndicationStore(directoryURL: dir)
        try await store.migrate()
        let now = Date().timeIntervalSince1970
        try await store.recordObservation(normalizedEntity: "betterhelp", podcastId: "show-A", confidence: 0.9, now: now)

        let totalShows = await store.totalObservedShowCount()
        let profile = try #require(await store.spreadProfile(forEntity: "betterhelp", totalObservedShows: totalShows))
        #expect(profile.distinctShowCount == 1)
        #expect(profile.persistenceDays == 0.0)
        #expect(CrossShowSyndicationEvaluator().qualifies(profile) == false,
                "a single-show, single-moment observation must NOT clear the syndication gate")

        await store.close()
    }

    @Test("an entity within a <14-day burst across shows does NOT clear the persistence gate")
    func sub14DayBurst_doesNotReachGate() async throws {
        // 3 shows but all within a 7-day window (e.g. the May-2026 corpus cluster):
        // spread + distinct-shows clear, but persistence (7d) < 14d fails — exactly
        // the editorial-burst case the gate is designed to reject.
        let dir = try makeTempDir(prefix: "brandab-burst")
        let store = try CrossShowSyndicationStore(directoryURL: dir)
        try await store.migrate()
        let now = Date().timeIntervalSince1970
        try await store.recordObservation(normalizedEntity: "apple", podcastId: "show-A", confidence: 0.9, now: now - 7 * day)
        try await store.recordObservation(normalizedEntity: "apple", podcastId: "show-B", confidence: 0.9, now: now - 3 * day)
        try await store.recordObservation(normalizedEntity: "apple", podcastId: "show-C", confidence: 0.9, now: now)

        let totalShows = await store.totalObservedShowCount()
        let profile = try #require(await store.spreadProfile(forEntity: "apple", totalObservedShows: totalShows))
        #expect(profile.distinctShowCount == 3)
        #expect(profile.spreadRatio >= 0.40)
        #expect(profile.persistenceDays < 14.0)
        #expect(CrossShowSyndicationEvaluator().qualifies(profile) == false,
                "a <14-day multi-show burst must NOT clear the persistence gate")

        await store.close()
    }
}

#endif
