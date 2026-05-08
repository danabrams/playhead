// ChapterSignalCaseStudyTests.swift
// playhead-au2v.1.20: Per-case before/after assertions for the curated
// chapter-signal case-study corpus.
//
// Each case file in `PlayheadTests/Fixtures/ChapterSignalCaseStudies/` is
// a tiny anonymized behavioural pin: it carries enough state to construct
// a `FrozenTrace` and a deterministic `ChapterSignalGate.Config`, plus
// the documented before/after counters for `.off` vs `.enabled`. This
// file decodes those cases and runs the gate twice per case, asserting
// the documented counters.
//
// In addition to the per-case before/after assertions, this file ships
// directory-level invariants:
//   * Every case JSON in the directory decodes under the v1 schema.
//   * Case ids are unique and match their filename stem.
//   * The case mix satisfies the bead-spec selection contract
//     (≥3 conversational misses, ≥2 false-positive removals, ≥1
//     pre/post-roll edge, ≥1 monologue/short-episode edge, ≥1 sanity
//     case).
//   * The bytes of every case JSON pass a forbidden-token scrub audit
//     (no "advertiser" substring, no 32-hex identifier shape, etc.).
//
// SCAFFOLDING NOTE: Until the real `ChapterBoundaryDetector` /
// `ChapterLabelingService` land (beads 4 / 12 / 13), the gate uses a
// deterministic stub for boundary detection and labelling. The case
// fixtures pin behaviour against the stub; when the real services land
// and the gate's `runShadowOrEnabled` is rewired, the expected counters
// here will need to be regenerated against the real implementation.

import Foundation
import Testing
@testable import Playhead

// MARK: - Case-study fixture model

/// One case-study fixture. Decoded from
/// `PlayheadTests/Fixtures/ChapterSignalCaseStudies/case-NN-…json`.
///
/// Visibility: this is `internal` (the default) — Swift Testing
/// parameterized tests require their argument type to be at least as
/// accessible as the test method itself. `CaseStudyPaths` and the
/// extension `makeTrace`/`makeConfig` helpers stay `fileprivate` so the
/// rest of the test target can't accidentally couple to them.
struct ChapterSignalCaseStudy: Decodable, Equatable, Sendable, CustomStringConvertible {

    let schemaVersion: Int
    let caseId: String
    let archetype: Archetype
    let category: Category
    let expectedBehavior: String
    let synthesisNotes: String
    let trace: TraceTemplate
    let gateInputs: GateInputs
    let expectedBeforeOff: ExpectedCounters
    let expectedAfterEnabled: ExpectedCounters

    /// The `caseId` is what surfaces in Swift Testing's parameterized
    /// test argument labels (`Test case passing 1 argument study →
    /// case-NN-…`). Implementing `CustomStringConvertible` instead of
    /// relying on the auto-derived dump keeps the per-case names short
    /// and stable across schema additions.
    var description: String { caseId }

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case caseId = "case_id"
        case archetype
        case category
        case expectedBehavior = "expected_behavior"
        case synthesisNotes = "synthesis_notes"
        case trace
        case gateInputs = "gate_inputs"
        case expectedBeforeOff = "expected_before_off"
        case expectedAfterEnabled = "expected_after_enabled"
    }

    enum Archetype: String, Decodable, Equatable, Sendable {
        case conversational
        case narrative
        case comedy
        case news
    }

    enum Category: String, Decodable, Equatable, Sendable {
        case conversationalMiss = "conversational_miss"
        case falsePositiveRemoval = "false_positive_removal"
        case prePostRollEdge = "pre_post_roll_edge"
        case monologueShortEdge = "monologue_short_edge"
        case sanitySignalInert = "sanity_signal_inert"
    }

    struct TraceTemplate: Decodable, Equatable, Sendable {
        let episodeIdAnon: String
        let podcastIdArchetype: String
        let episodeDurationSec: Double
        let atomCount: Int
        let atomSpacingSec: Double

        enum CodingKeys: String, CodingKey {
            case episodeIdAnon = "episode_id_anon"
            case podcastIdArchetype = "podcast_id_archetype"
            case episodeDurationSec = "episode_duration_sec"
            case atomCount = "atom_count"
            case atomSpacingSec = "atom_spacing_sec"
        }
    }

    struct GateInputs: Decodable, Equatable, Sendable {
        let creatorChaptersPresent: Bool
        let stubChapterCount: Int?

        enum CodingKeys: String, CodingKey {
            case creatorChaptersPresent = "creator_chapters_present"
            case stubChapterCount = "stub_chapter_count"
        }
    }

    struct ExpectedCounters: Decodable, Equatable, Sendable {
        let planGeneratedCount: Int
        let skippedByCreatorChapters: Int
        let totalFMCallsForChapterLabeling: Int
        let aggregateLatencyMs: Double

        enum CodingKeys: String, CodingKey {
            case planGeneratedCount = "plan_generated_count"
            case skippedByCreatorChapters = "skipped_by_creator_chapters"
            case totalFMCallsForChapterLabeling = "total_fm_calls_for_chapter_labeling"
            case aggregateLatencyMs = "aggregate_latency_ms"
        }
    }
}

// MARK: - Fixture loader

fileprivate enum CaseStudyPaths {

    /// Resolve the directory of `ChapterSignalCaseStudies/` via `#filePath`.
    /// This file lives at
    /// `PlayheadTests/Services/ReplaySimulator/NarlEval/ChapterSignalCaseStudyTests.swift`,
    /// so we ascend four path components — file → NarlEval → ReplaySimulator
    /// → Services → PlayheadTests — and then descend into
    /// `Fixtures/ChapterSignalCaseStudies/`. We deliberately do NOT depend
    /// on the test bundle's resource lookup — these fixtures are read at
    /// runtime via `#filePath`, the same convention `FrozenTrace`
    /// fixtures and `DogfoodAnalysisHealthFixtureLoader` use.
    static func fixturesDir(_ filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
            .deletingLastPathComponent() // NarlEval
            .deletingLastPathComponent() // ReplaySimulator
            .deletingLastPathComponent() // Services
            .deletingLastPathComponent() // PlayheadTests
            .appendingPathComponent("Fixtures", isDirectory: true)
            .appendingPathComponent("ChapterSignalCaseStudies", isDirectory: true)
    }

    /// Enumerate every `case-*.json` file in the case-study fixtures
    /// directory, sorted by filename for determinism.
    static func caseFiles(_ filePath: String = #filePath) throws -> [URL] {
        let dir = fixturesDir(filePath)
        let entries = try FileManager.default.contentsOfDirectory(
            at: dir,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        )
        return entries
            .filter { $0.lastPathComponent.hasPrefix("case-") && $0.pathExtension == "json" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
    }

    static func loadAllCases(_ filePath: String = #filePath) throws -> [(URL, ChapterSignalCaseStudy)] {
        let decoder = JSONDecoder()
        return try caseFiles(filePath).map { url in
            let data = try Data(contentsOf: url)
            return (url, try decoder.decode(ChapterSignalCaseStudy.self, from: data))
        }
    }

    /// Best-effort loader for use as the `arguments:` of a parameterized
    /// test. If the directory or any file is unreadable / undecodable
    /// we return `[]`; the dedicated `everyCaseDecodes` test in the
    /// directory-integrity suite will surface the failure with a clear
    /// message instead of relying on a stray `try!` to crash the suite.
    static func loadAllCasesOrEmpty(_ filePath: String = #filePath) -> [ChapterSignalCaseStudy] {
        do {
            return try loadAllCases(filePath).map(\.1)
        } catch {
            return []
        }
    }
}

// MARK: - Case → gate inputs

fileprivate extension ChapterSignalCaseStudy {

    /// Synthesize a deterministic `FrozenTrace` from the case template.
    /// The atoms have empty text — the gate does not read atom text, and
    /// keeping text empty is part of the anonymization contract (no
    /// transcript content lives in the fixture; nothing is conjured at
    /// load time either).
    func makeTrace() -> FrozenTrace {
        let atoms: [FrozenTrace.FrozenAtom] = (0..<trace.atomCount).map { i in
            let start = Double(i) * trace.atomSpacingSec
            return FrozenTrace.FrozenAtom(
                startTime: start,
                endTime: start + trace.atomSpacingSec,
                text: ""
            )
        }
        return FrozenTrace(
            episodeId: trace.episodeIdAnon,
            podcastId: trace.podcastIdArchetype,
            episodeDuration: trace.episodeDurationSec,
            traceVersion: FrozenTrace.currentTraceVersion,
            // Frozen reference epoch — every case is replay-deterministic.
            // No wall-clock timestamps from any source diagnostic appear
            // here; see README.md anonymization §5.
            capturedAt: Date(timeIntervalSince1970: 1_700_000_000),
            featureWindows: [],
            atoms: atoms,
            evidenceCatalog: [],
            corrections: [],
            decisionEvents: [],
            baselineReplaySpanDecisions: [],
            holdoutDesignation: .training
        )
    }

    /// Build a gate `Config` from this case's `gate_inputs`. When
    /// `stub_chapter_count` is `null` the default stub is left in place
    /// (`Config.defaultStubChapterCount`); when it carries an integer,
    /// a fixed-value stub is wired in. `creator_chapters_present` is a
    /// per-case boolean — the closure simply returns the value.
    func makeConfig() -> ChapterSignalGate.Config {
        let creatorPresent = gateInputs.creatorChaptersPresent
        if let stub = gateInputs.stubChapterCount {
            return ChapterSignalGate.Config(
                stubChapterCount: { _ in stub },
                creatorChaptersPresent: { _ in creatorPresent }
            )
        } else {
            return ChapterSignalGate.Config(
                creatorChaptersPresent: { _ in creatorPresent }
            )
        }
    }
}

// MARK: - Per-case before/after suite

@Suite("ChapterSignalCaseStudy — per-case before/after")
struct ChapterSignalCaseStudyTests {

    @Test(
        "Each case asserts the documented before (.off) and after (.enabled) counters",
        arguments: CaseStudyPaths.loadAllCasesOrEmpty()
    )
    func caseAssertsBeforeAndAfter(study: ChapterSignalCaseStudy) {
        let trace = study.makeTrace()
        let config = study.makeConfig()

        // .off: must match expected_before_off byte-for-byte.
        let off = ChapterSignalGate.replay(trace: trace, mode: .off, config: config)
        #expect(off.mode == .off,
                "[\(study.caseId)] mode mismatch on .off result")
        #expect(off.episodesProcessed == 1,
                "[\(study.caseId)] .off must process exactly the one input trace.")
        #expect(off.planGeneratedCount == study.expectedBeforeOff.planGeneratedCount,
                "[\(study.caseId)] .off planGeneratedCount: expected \(study.expectedBeforeOff.planGeneratedCount), got \(off.planGeneratedCount)")
        #expect(off.skippedByCreatorChapters == study.expectedBeforeOff.skippedByCreatorChapters,
                "[\(study.caseId)] .off skippedByCreatorChapters mismatch.")
        #expect(off.totalFMCallsForChapterLabeling == study.expectedBeforeOff.totalFMCallsForChapterLabeling,
                "[\(study.caseId)] .off totalFMCallsForChapterLabeling mismatch.")
        #expect(off.aggregateLatencyMs == study.expectedBeforeOff.aggregateLatencyMs,
                "[\(study.caseId)] .off aggregateLatencyMs mismatch.")

        // .enabled: must match expected_after_enabled byte-for-byte.
        let enabled = ChapterSignalGate.replay(trace: trace, mode: .enabled, config: config)
        #expect(enabled.mode == .enabled,
                "[\(study.caseId)] mode mismatch on .enabled result.")
        #expect(enabled.episodesProcessed == 1,
                "[\(study.caseId)] .enabled must process exactly the one input trace.")
        #expect(enabled.planGeneratedCount == study.expectedAfterEnabled.planGeneratedCount,
                "[\(study.caseId)] .enabled planGeneratedCount: expected \(study.expectedAfterEnabled.planGeneratedCount), got \(enabled.planGeneratedCount)")
        #expect(enabled.skippedByCreatorChapters == study.expectedAfterEnabled.skippedByCreatorChapters,
                "[\(study.caseId)] .enabled skippedByCreatorChapters mismatch.")
        #expect(enabled.totalFMCallsForChapterLabeling == study.expectedAfterEnabled.totalFMCallsForChapterLabeling,
                "[\(study.caseId)] .enabled totalFMCallsForChapterLabeling mismatch.")
        #expect(enabled.aggregateLatencyMs == study.expectedAfterEnabled.aggregateLatencyMs,
                "[\(study.caseId)] .enabled aggregateLatencyMs mismatch.")

        // Per-episode outcome carries the right ids. This pins that
        // `episode_id_anon` and `podcast_id_archetype` flow through the
        // gate correctly — a regression that lost the ids would
        // mis-attribute case-study outcomes when bead 19 sums across
        // cases.
        #expect(off.perEpisodeOutcomes.count == 1)
        #expect(off.perEpisodeOutcomes[0].episodeId == study.trace.episodeIdAnon)
        #expect(off.perEpisodeOutcomes[0].podcastId == study.trace.podcastIdArchetype)
        #expect(enabled.perEpisodeOutcomes.count == 1)
        #expect(enabled.perEpisodeOutcomes[0].episodeId == study.trace.episodeIdAnon)
        #expect(enabled.perEpisodeOutcomes[0].podcastId == study.trace.podcastIdArchetype)

        // Phase-side parity: `.shadow` mirrors `.enabled` on every
        // additive counter. The gate's contract (au2v.1.18 doc comment)
        // is that consumer-side divergence between shadow and enabled
        // lives in `ChapterSignalMode.consumersReadChapterPlan`, NOT in
        // the gate's per-episode outputs. Pin it through the case-study
        // substrate so a regression in the gate's mode dispatch
        // surfaces here as well as in `ChapterSignalGateTests`.
        let shadow = ChapterSignalGate.replay(trace: trace, mode: .shadow, config: config)
        #expect(shadow.mode == .shadow,
                "[\(study.caseId)] mode mismatch on .shadow result.")
        #expect(shadow.episodesProcessed == 1,
                "[\(study.caseId)] .shadow must process exactly the one input trace.")
        #expect(shadow.planGeneratedCount == enabled.planGeneratedCount,
                "[\(study.caseId)] .shadow planGeneratedCount must equal .enabled.")
        #expect(shadow.skippedByCreatorChapters == enabled.skippedByCreatorChapters,
                "[\(study.caseId)] .shadow skippedByCreatorChapters must equal .enabled.")
        #expect(shadow.totalFMCallsForChapterLabeling == enabled.totalFMCallsForChapterLabeling,
                "[\(study.caseId)] .shadow totalFMCallsForChapterLabeling must equal .enabled.")
        #expect(shadow.aggregateLatencyMs == enabled.aggregateLatencyMs,
                "[\(study.caseId)] .shadow aggregateLatencyMs must equal .enabled.")
        #expect(shadow.perEpisodeOutcomes.count == 1)
        #expect(shadow.perEpisodeOutcomes[0].episodeId == study.trace.episodeIdAnon)
        #expect(shadow.perEpisodeOutcomes[0].podcastId == study.trace.podcastIdArchetype)

        // Determinism: replaying the same case twice in each mode must
        // yield Equatable-equal results. This catches regressions that
        // would introduce non-determinism in the trace synthesis (e.g.,
        // a future change to `makeTrace` that accidentally read a
        // wall-clock value) before they corrupt the harness's
        // byte-for-byte parity contract. Cover all three modes so a
        // future regression that touches only one mode's dispatch path
        // still surfaces here.
        let off2 = ChapterSignalGate.replay(trace: trace, mode: .off, config: config)
        let enabled2 = ChapterSignalGate.replay(trace: trace, mode: .enabled, config: config)
        let shadow2 = ChapterSignalGate.replay(trace: trace, mode: .shadow, config: config)
        #expect(off == off2,
                "[\(study.caseId)] .off result is non-deterministic across replays.")
        #expect(enabled == enabled2,
                "[\(study.caseId)] .enabled result is non-deterministic across replays.")
        #expect(shadow == shadow2,
                "[\(study.caseId)] .shadow result is non-deterministic across replays.")
    }

    @Test("Per-case loader produced at least one case (parameterized suite is non-vacuous)")
    func perCaseSuiteIsNonVacuous() throws {
        // The parameterized `caseAssertsBeforeAndAfter` test silently
        // runs zero iterations if `loadAllCasesOrEmpty()` returned [].
        // The directory-integrity suite catches that on the loader path,
        // but THIS suite should also pin its own non-vacuity so a future
        // refactor that disables the directory suite can't accidentally
        // ship empty case coverage in this suite too.
        let cases = CaseStudyPaths.loadAllCasesOrEmpty()
        #expect(!cases.isEmpty,
                "loadAllCasesOrEmpty() returned empty — the parameterized per-case suite would otherwise pass vacuously.")
    }
}

// MARK: - Directory-level integrity suite

@Suite("ChapterSignalCaseStudy — directory integrity")
struct ChapterSignalCaseStudyDirectoryTests {

    @Test("README.md is present in the case-studies fixture directory")
    func readmePresent() throws {
        let readme = CaseStudyPaths.fixturesDir().appendingPathComponent("README.md")
        #expect(FileManager.default.fileExists(atPath: readme.path),
                "README.md must ship in the case-studies fixture directory.")
    }

    @Test("Every case file decodes under the v1 schema and uses schema_version=1")
    func everyCaseDecodes() throws {
        let cases = try CaseStudyPaths.loadAllCases()
        #expect(!cases.isEmpty, "at least one case must ship")
        for (url, study) in cases {
            #expect(study.schemaVersion == 1,
                    "[\(study.caseId)] expected schema_version=1, got \(study.schemaVersion) (file: \(url.lastPathComponent))")
        }
    }

    @Test("Case ids are unique and match their filename stem")
    func caseIdsUniqueAndMatchFilename() throws {
        let cases = try CaseStudyPaths.loadAllCases()
        var seen = Set<String>()
        for (url, study) in cases {
            let stem = url.deletingPathExtension().lastPathComponent
            #expect(study.caseId == stem,
                    "[\(study.caseId)] case_id must equal filename stem; filename was \(stem).")
            let inserted = seen.insert(study.caseId).inserted
            #expect(inserted, "duplicate case_id: \(study.caseId)")
        }
    }

    @Test("Total case count is in the bead-spec range [5, 10]")
    func caseCountWithinSpec() throws {
        let cases = try CaseStudyPaths.loadAllCases()
        #expect((5...10).contains(cases.count),
                "case count must be in [5, 10]; got \(cases.count).")
    }

    @Test("Selection covers ≥3 conversational misses, ≥2 false-positive removals, ≥1 pre/post-roll, ≥1 monologue/short, ≥1 sanity")
    func selectionCoverageMatchesBeadSpec() throws {
        let cases = try CaseStudyPaths.loadAllCases().map(\.1)

        let conversationalMisses = cases.filter { $0.category == .conversationalMiss }.count
        let falsePositiveRemovals = cases.filter { $0.category == .falsePositiveRemoval }.count
        let prePostRollEdges = cases.filter { $0.category == .prePostRollEdge }.count
        let monologueEdges = cases.filter { $0.category == .monologueShortEdge }.count
        let sanity = cases.filter { $0.category == .sanitySignalInert }.count

        #expect(conversationalMisses >= 3,
                "Need ≥3 conversational misses; got \(conversationalMisses).")
        #expect(falsePositiveRemovals >= 2,
                "Need ≥2 false-positive removal cases; got \(falsePositiveRemovals).")
        #expect(prePostRollEdges >= 1,
                "Need ≥1 pre/post-roll edge case; got \(prePostRollEdges).")
        #expect(monologueEdges >= 1,
                "Need ≥1 monologue/short-episode edge case; got \(monologueEdges).")
        #expect(sanity >= 1,
                "Need ≥1 sanity case where the signal correctly does nothing; got \(sanity).")
    }

    @Test("No case JSON contains forbidden tokens (advertiser names, hex identifiers, feed URLs, transcript snippets)")
    func forbiddenTokenScrubAudit() throws {
        // Belt-and-suspenders scrub audit: read the raw bytes of every
        // CASE JSON file in the directory and assert that none of the
        // forbidden token shapes appears. The README is exempt — its job
        // is to *describe* the anonymization pipeline, so concept words
        // like "advertiser" appear there in legitimate documentation
        // context. Case JSONs are the load-bearing data surface; they
        // must be PII-free.
        let dir = CaseStudyPaths.fixturesDir()
        let entries = try FileManager.default.contentsOfDirectory(
            at: dir,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        )
        let caseJsons = entries.filter {
            $0.lastPathComponent.hasPrefix("case-") && $0.pathExtension == "json"
        }
        #expect(!caseJsons.isEmpty, "scrub audit must run against at least one case file")

        // 32 lowercase hex chars is the shape of the dogfood
        // `episode_id_hash` SHA-256 prefix; 64 lowercase hex is the
        // full SHA. Either is forbidden in case fixtures.
        let hex32 = try NSRegularExpression(pattern: "[0-9a-f]{32}", options: [])
        let hex64 = try NSRegularExpression(pattern: "[0-9a-f]{64}", options: [])
        // Any URL shape (with `://`) is forbidden in case JSONs.
        let urlShape = try NSRegularExpression(pattern: "https?://[^\"\\s]+", options: [])

        // Tokens forbidden as a case-insensitive substring match in
        // case JSONs. Concepts the case data surface must never carry.
        let forbiddenSubstrings = [
            "advertiser",
            "sponsor",
            "brand",
            "flightcast",
            "simplecast",
            "doac",
            "conan",
            "kelly ripa"
        ]

        for url in caseJsons {
            let bytes = try Data(contentsOf: url)
            guard let text = String(data: bytes, encoding: .utf8) else {
                Issue.record("non-UTF-8 bytes in fixture \(url.lastPathComponent)")
                continue
            }
            let lowered = text.lowercased()

            for substring in forbiddenSubstrings {
                #expect(!lowered.contains(substring.lowercased()),
                        "forbidden substring \"\(substring)\" found in \(url.lastPathComponent)")
            }

            let range = NSRange(text.startIndex..<text.endIndex, in: text)
            #expect(hex64.firstMatch(in: text, range: range) == nil,
                    "64-char hex identifier shape found in \(url.lastPathComponent) — looks like a SHA-256.")
            #expect(hex32.firstMatch(in: text, range: range) == nil,
                    "32-char hex identifier shape found in \(url.lastPathComponent) — looks like an MD5/UUID hash prefix.")
            #expect(urlShape.firstMatch(in: text, range: range) == nil,
                    "URL shape found in fixture \(url.lastPathComponent).")
        }
    }

    @Test("Every case carries a synthetic episode_id_anon (matches anonymization shape)")
    func everyCaseHasSyntheticEpisodeId() throws {
        // README anonymization §1 documents the canonical shape:
        //   `episode_anon_<short hex>`. Pin the regex so a future case
        // can't smuggle in an unredacted episode title or RSS GUID by
        // forgetting to run the anonymization step. The current corpus
        // uses six-char lowercase hex (e.g. "episode_anon_a1c01"); the
        // regex permits 4-16 hex chars to give future cases a little
        // headroom without weakening the redaction guarantee.
        let pattern = try NSRegularExpression(
            pattern: "^episode_anon_[0-9a-f]{4,16}$",
            options: []
        )
        let cases = try CaseStudyPaths.loadAllCases().map(\.1)
        for study in cases {
            let id = study.trace.episodeIdAnon
            let range = NSRange(id.startIndex..<id.endIndex, in: id)
            #expect(pattern.firstMatch(in: id, range: range) != nil,
                    "[\(study.caseId)] episode_id_anon \"\(id)\" must match ^episode_anon_<4-16 hex>$.")
        }
    }

    @Test("Every case carries a podcast_id_archetype matching the four allowed archetypes")
    func everyCaseHasArchetypePodcastId() throws {
        // README anonymization §2 documents the canonical shape:
        //   `show_archetype_<one of the four allowed archetypes>`. Pin
        // it so a future case can't smuggle in a real show title or RSS
        // URL by forgetting to redact. The archetype enum is decoded
        // separately, so we don't need to re-validate the set here —
        // we only need to check the wire-shape of `podcast_id_archetype`
        // and that it agrees with the decoded `archetype`.
        let cases = try CaseStudyPaths.loadAllCases().map(\.1)
        for study in cases {
            let expected = "show_archetype_\(study.archetype.rawValue)"
            #expect(study.trace.podcastIdArchetype == expected,
                    "[\(study.caseId)] podcast_id_archetype must equal \"\(expected)\"; got \"\(study.trace.podcastIdArchetype)\".")
        }
    }

    @Test("Every case exercises observable lift: .off and .enabled differ on at least one counter")
    func everyCaseExercisesObservableLift() throws {
        // A case-study corpus that documents identical .off and .enabled
        // counters for some case is a fixture that exercises no actual
        // gate behaviour — the test would tautologically pass on that
        // case under any gate implementation. Pin per-case lift here:
        // for every case, at least one of the four documented counters
        // (plan_generated_count, skipped_by_creator_chapters,
        // total_fm_calls_for_chapter_labeling, aggregate_latency_ms) must
        // differ between expected_before_off and expected_after_enabled.
        // Cases 04/08 have latency-only lift (the gate ran but produced
        // no plan); case 05 has skippedByCreatorChapters-only lift; the
        // rest carry plan+FM+latency lift. Any future case that
        // accidentally drops the lift will fail here loudly.
        let cases = try CaseStudyPaths.loadAllCases().map(\.1)
        for study in cases {
            let before = study.expectedBeforeOff
            let after = study.expectedAfterEnabled
            let differs =
                before.planGeneratedCount != after.planGeneratedCount
                || before.skippedByCreatorChapters != after.skippedByCreatorChapters
                || before.totalFMCallsForChapterLabeling != after.totalFMCallsForChapterLabeling
                || before.aggregateLatencyMs != after.aggregateLatencyMs
            #expect(differs,
                    "[\(study.caseId)] expected_before_off and expected_after_enabled are identical — case exercises no observable gate lift.")
        }
    }

    @Test("Every case has non-empty expected_behavior and synthesis_notes")
    func everyCaseHasDocumentationStrings() throws {
        // The fixtures pull double duty as the documentation surface for
        // *why* each behaviour is pinned. A case shipping with empty
        // `expected_behavior` or `synthesis_notes` strings is a
        // documentation regression: future readers (humans and the
        // replay-tooling diff harness) lose the context that explains
        // what each case is meant to demonstrate. Both strings must be
        // non-empty after trimming whitespace.
        let cases = try CaseStudyPaths.loadAllCases().map(\.1)
        for study in cases {
            let behaviorTrimmed = study.expectedBehavior
                .trimmingCharacters(in: .whitespacesAndNewlines)
            let notesTrimmed = study.synthesisNotes
                .trimmingCharacters(in: .whitespacesAndNewlines)
            #expect(!behaviorTrimmed.isEmpty,
                    "[\(study.caseId)] expected_behavior must be non-empty.")
            #expect(!notesTrimmed.isEmpty,
                    "[\(study.caseId)] synthesis_notes must be non-empty.")
        }
    }

    @Test("Every case's expected counters are self-consistent")
    func expectedCountersSelfConsistent() throws {
        // Every well-formed case must have non-negative counter values
        // (Int Int.signum check — a fixture with -1 latency would still
        // decode, but is meaningless), and `.off` must always be a
        // structural zero EXCEPT for the perEpisodeOutcomes carrying
        // the input ids.
        let cases = try CaseStudyPaths.loadAllCases().map(\.1)
        for study in cases {
            #expect(study.expectedBeforeOff.planGeneratedCount == 0,
                    "[\(study.caseId)] .off planGeneratedCount must be 0.")
            #expect(study.expectedBeforeOff.skippedByCreatorChapters == 0,
                    "[\(study.caseId)] .off skippedByCreatorChapters must be 0.")
            #expect(study.expectedBeforeOff.totalFMCallsForChapterLabeling == 0,
                    "[\(study.caseId)] .off totalFMCallsForChapterLabeling must be 0.")
            #expect(study.expectedBeforeOff.aggregateLatencyMs == 0.0,
                    "[\(study.caseId)] .off aggregateLatencyMs must be 0.")

            // .enabled counters must be non-negative. (Specific
            // expected values are pinned by the per-case test above;
            // here we only enforce the structural invariant.)
            #expect(study.expectedAfterEnabled.planGeneratedCount >= 0,
                    "[\(study.caseId)] .enabled planGeneratedCount must be ≥0.")
            #expect(study.expectedAfterEnabled.skippedByCreatorChapters >= 0,
                    "[\(study.caseId)] .enabled skippedByCreatorChapters must be ≥0.")
            #expect(study.expectedAfterEnabled.totalFMCallsForChapterLabeling >= 0,
                    "[\(study.caseId)] .enabled totalFMCallsForChapterLabeling must be ≥0.")
            #expect(study.expectedAfterEnabled.aggregateLatencyMs >= 0,
                    "[\(study.caseId)] .enabled aggregateLatencyMs must be ≥0.")

            // gate_inputs.stubChapterCount, when present, must be ≥0
            // (the gate clamps internally, but a negative literal in
            // the fixture is meaningless and a strong signal of a
            // hand-edit error).
            if let stub = study.gateInputs.stubChapterCount {
                #expect(stub >= 0,
                        "[\(study.caseId)] gate_inputs.stub_chapter_count must be ≥0; got \(stub).")
            }

            // trace.atom_count must be ≥0 and atom_spacing_sec ≥0.
            #expect(study.trace.atomCount >= 0,
                    "[\(study.caseId)] trace.atom_count must be ≥0.")
            #expect(study.trace.atomSpacingSec >= 0,
                    "[\(study.caseId)] trace.atom_spacing_sec must be ≥0.")
            #expect(study.trace.episodeDurationSec >= 0,
                    "[\(study.caseId)] trace.episode_duration_sec must be ≥0.")
        }
    }
}
