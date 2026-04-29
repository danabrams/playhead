// DiagnosticsBundleShapeTests.swift
// playhead-fsy3 Scope 2 — legal-checklist audit (a) + (b) shape tests
// against checked-in sample bundle JSON fixtures.
//
// Why fixtures (not just live builder output):
//   The legal checklist is a contract on the wire format the user
//   actually emails to support. Freezing canonical JSON shapes as
//   fixtures means any future schema drift (a renamed coding key, a
//   new top-level field, an accidentally added `episodeId` somewhere
//   deep in the tree) will trip these tests against the frozen
//   reference, not just whatever the builder happens to produce on
//   the current commit. The fixture-regeneration test below regenerates
//   the fixtures from the live builder when they are missing, so the
//   first run after deletion (or after schema-revision approval) is a
//   one-step refresh.
//
// Coverage map (this file pairs with `docs/plans/diagnostics-bundle-legal-checklist.md`):
//   (a) Default bundle: `defaultBundleHasOnlyAllowedKeys` walks the entire
//       JSON tree and asserts NO `episodeId`-shaped key appears anywhere.
//       Top-level + nested keys are bounded to the documented set.
//   (b) Opt-in isolation: `optInBundleEpisodeIdIsHashed64HexChars` is paired
//       with `defaultBundleNeverContainsTranscriptOrFeatureSummaryKeys` to
//       prove `transcript_excerpts` / `feature_summaries` never appear in
//       the default subtree.

import Foundation
import Testing

@testable import Playhead

// MARK: - Fixture provisioning

/// Helper namespace that owns the canonical bundle inputs and the
/// fixture-on-disk read/write contract. Inputs are deterministic so the
/// fixture is byte-stable across machines (sortedKeys + ISO8601 dates
/// + fixed UUIDs + fixed timestamps).
@MainActor
private enum BundleShapeFixtures {

    static let installID = UUID(uuidString: "11111111-1111-4111-8111-111111111111")!
    static let now = Date(timeIntervalSince1970: 1_700_000_000)

    static let rawDefaultEpisodeIds = [
        "ep-fsy3-default-1",
        "ep-fsy3-default-2"
    ]
    static let rawOptInEpisodeId = "ep-fsy3-optin-1"

    /// Produce the canonical default-bundle inputs. The two journal
    /// entries reference real episode IDs so the fixture exercises the
    /// hashing path; the fixture's `episode_id_hash` field is the
    /// SHA-256 of `installID || rawId` per the spec.
    static func defaultBundleEntries() -> [WorkJournalEntry] {
        rawDefaultEpisodeIds.enumerated().map { idx, id in
            WorkJournalEntry(
                id: "row-\(idx)",
                episodeId: id,
                generationID: UUID(
                    uuidString: "00000000-0000-4000-8000-00000000000\(idx)"
                )!,
                schedulerEpoch: 1,
                timestamp: now.timeIntervalSince1970 + Double(idx),
                eventType: .acquired,
                cause: nil,
                metadata: "{}",
                artifactClass: .scratch
            )
        }
    }

    /// One opt-in episode with a transcript excerpt and a feature
    /// summary so the OptInBundle fixture covers both fields.
    static func optInEpisode() -> DiagnosticsEpisodeInput {
        DiagnosticsEpisodeInput(
            episodeId: rawOptInEpisodeId,
            episodeTitle: "OptIn Episode (fsy3)",
            diagnosticsOptIn: true,
            adBoundaryTimes: [60.0],
            transcriptChunks: [
                DiagnosticsTranscriptChunk(
                    startTime: 30, endTime: 90,
                    text: "fsy3 sample opt-in transcript excerpt for legal checklist audit"
                )
            ],
            featureSummary: OptInBundle.FeatureSummary(
                rmsMean: 0.1,
                rmsMax: 0.5,
                spectralFluxMean: 0.2,
                musicProbabilityMean: 0.3,
                pauseProbabilityMean: 0.1
            )
        )
    }

    static func eligibility() -> AnalysisEligibility {
        AnalysisEligibility(
            hardwareSupported: true,
            appleIntelligenceEnabled: true,
            regionSupported: true,
            languageSupported: true,
            modelAvailableNow: true,
            capturedAt: now
        )
    }

    /// Build + encode the default-bundle fixture file. No opt-in
    /// episodes — the produced JSON has the `opt_in` field omitted, the
    /// shape that legal item (a) audits.
    static func encodedDefaultBundle() throws -> Data {
        let defaultBundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibility: eligibility(),
            workJournalEntries: defaultBundleEntries(),
            installID: installID
        )
        let file = DiagnosticsBundleFile(
            generatedAt: now,
            default: defaultBundle,
            optIn: nil
        )
        return try DiagnosticsExportService.encode(file)
    }

    /// Build + encode the opt-in fixture file. Same default bundle
    /// inputs, plus one opted-in episode — produced JSON contains the
    /// `opt_in` subtree, the shape that legal item (b) audits.
    static func encodedOptInBundle() throws -> Data {
        let defaultBundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibility: eligibility(),
            workJournalEntries: defaultBundleEntries(),
            installID: installID
        )
        let optIn = DiagnosticsBundleBuilder.buildOptIn(episodes: [optInEpisode()])
        let file = DiagnosticsBundleFile(
            generatedAt: now,
            default: defaultBundle,
            optIn: optIn
        )
        return try DiagnosticsExportService.encode(file)
    }

    /// Anchor the fixture directory at `#filePath` so the location is
    /// stable regardless of build-products layout. Walks up from the
    /// test file (`PlayheadTests/Support/Diagnostics/`) to the
    /// `PlayheadTests/` root, then descends into `Fixtures/Diagnostics/`.
    /// The fixture files are checked into git; if missing on disk we
    /// regenerate.
    static func fixtureDirectoryURL(file: StaticString = #filePath) -> URL {
        let thisFile = URL(fileURLWithPath: String(describing: file))
        // .../PlayheadTests/Support/Diagnostics/<this>.swift
        //   → .../PlayheadTests/Fixtures/Diagnostics/
        return thisFile
            .deletingLastPathComponent()       // strip filename → Diagnostics/
            .deletingLastPathComponent()       // strip Diagnostics → Support/
            .deletingLastPathComponent()       // strip Support → PlayheadTests/
            .appendingPathComponent("Fixtures")
            .appendingPathComponent("Diagnostics")
    }

    static let defaultFixtureFilename = "sample-default-bundle.json"
    static let optInFixtureFilename = "sample-opt-in-bundle.json"

    /// Read the on-disk fixture if present; regenerate-and-write
    /// (idempotent — same inputs always produce the same bytes) when
    /// missing. Either path returns the same canonical bytes.
    static func loadOrGenerate(filename: String, generator: () throws -> Data) throws -> Data {
        let directory = fixtureDirectoryURL()
        let url = directory.appendingPathComponent(filename, isDirectory: false)
        if let data = try? Data(contentsOf: url) {
            return data
        }
        // Regenerate — try to write to disk so the next run is a plain
        // read. The write may fail if the test binary runs inside a
        // sandboxed simulator process (no access to the developer's
        // source tree); in that case we fall back to in-memory bytes
        // and the assertion still runs against the freshly-built JSON.
        // The intent of the on-disk fixture is to give legal review a
        // grep-able artifact in the repo, not to gate the assertions.
        do {
            try FileManager.default.createDirectory(
                at: directory,
                withIntermediateDirectories: true
            )
            let data = try generator()
            try data.write(to: url, options: [.atomic])
            return data
        } catch {
            return try generator()
        }
    }
}

// MARK: - Suite

@Suite("Diagnostics bundle shape — legal checklist (a)+(b) (playhead-fsy3)", .serialized)
@MainActor
struct DiagnosticsBundleShapeTests {

    /// Top-level CodingKeys that may appear at the bundle-file root.
    /// Mirrors `DiagnosticsBundleFile.CodingKeys` exactly. The bead
    /// audit phrases the allowed default-bundle set as
    /// `{app_version, build_type, device_class, analysis_eligibility, summaries}`;
    /// the live schema's bundle-file root is a thinner wrapper
    /// (`generated_at`, `default`, `opt_in?`) and the actual
    /// per-domain fields live one level down inside `default`.
    /// Both layers are audited below.
    private static let allowedTopLevelKeys: Set<String> = [
        "generated_at",
        "default",
        "opt_in"
    ]

    /// CodingKeys allowed inside the `default` subtree. Mirrors
    /// `DefaultBundle.CodingKeys` exactly. `analysis_unavailable_reason`
    /// is optional and omitted when nil; `analysis_eligibility` from
    /// the bead audit maps to `eligibility_snapshot` in the live
    /// schema.
    private static let allowedDefaultSubtreeKeys: Set<String> = [
        "app_version",
        "os_version",
        "device_class",
        "build_type",
        "eligibility_snapshot",
        "analysis_unavailable_reason",
        "scheduler_events",
        "work_journal_tail"
    ]

    /// Substrings that — if present anywhere in the encoded JSON's
    /// keys — would indicate a raw episode ID has leaked back into the
    /// default bundle. This is the (a) contract: NO `episodeId`-shaped
    /// field, regardless of casing or snake-vs-camel, anywhere in the
    /// default subtree. We allow `episode_id_hash` (the salted hex)
    /// explicitly.
    private static let forbiddenEpisodeIdKeyTokens: [String] = [
        "episodeid",
        "episode_id"
    ]

    /// Field names that belong exclusively to the OptInBundle. Their
    /// presence in the default subtree would breach legal item (b).
    private static let forbiddenInDefaultSubtree: Set<String> = [
        "transcript_excerpts",
        "feature_summaries",
        "episode_title"
    ]

    // MARK: - (a) Default bundle: key presence ⊆ allowed, no episodeId anywhere

    @Test("Default-bundle JSON: top-level keys are within {generated_at, default, opt_in} and `default` keys are within the documented set")
    func defaultBundleHasOnlyAllowedKeys() throws {
        let data = try BundleShapeFixtures.loadOrGenerate(
            filename: BundleShapeFixtures.defaultFixtureFilename,
            generator: BundleShapeFixtures.encodedDefaultBundle
        )
        let object = try JSONSerialization.jsonObject(with: data, options: [])
        let dict = try #require(object as? [String: Any], "Default bundle root must be a JSON object")

        // Top-level shape: only documented keys, and `opt_in` MUST NOT
        // be present in the default-only fixture.
        let topKeys = Set(dict.keys)
        #expect(topKeys.isSubset(of: Self.allowedTopLevelKeys),
                "Top-level keys \(topKeys) escape allowed set \(Self.allowedTopLevelKeys)")
        #expect(!dict.keys.contains("opt_in"),
                "Default-only fixture must not carry opt_in subtree")

        // Default subtree keys: bounded to the documented per-domain set.
        let defaultSubtree = try #require(dict["default"] as? [String: Any],
                                          "`default` subtree must be a JSON object")
        let defaultKeys = Set(defaultSubtree.keys)
        #expect(defaultKeys.isSubset(of: Self.allowedDefaultSubtreeKeys),
                "Default subtree keys \(defaultKeys) escape allowed set \(Self.allowedDefaultSubtreeKeys)")
    }

    @Test("Default-bundle JSON: no `episodeId` (or `episode_id`) appears anywhere in the JSON tree — only `episode_id_hash` is permitted")
    func defaultBundleHasNoRawEpisodeIdKeyAnywhere() throws {
        let data = try BundleShapeFixtures.loadOrGenerate(
            filename: BundleShapeFixtures.defaultFixtureFilename,
            generator: BundleShapeFixtures.encodedDefaultBundle
        )
        let object = try JSONSerialization.jsonObject(with: data, options: [])

        let walk = collectAllKeys(in: object)
        for key in walk {
            let lower = key.lowercased()
            for token in Self.forbiddenEpisodeIdKeyTokens {
                if lower == token {
                    Issue.record("Forbidden raw episode-ID key '\(key)' present in default bundle JSON tree")
                }
                if lower.contains(token) && lower != "episode_id_hash" {
                    // The only permitted episodeId-adjacent key shape is
                    // exactly `episode_id_hash` (the salted hex). Anything
                    // else (e.g. `episode_id`, `episodeid_raw`) is a
                    // legal-checklist violation.
                    Issue.record(
                        "Suspicious episodeId-adjacent key '\(key)' (token '\(token)') in default bundle — only 'episode_id_hash' is permitted"
                    )
                }
            }
        }

        // Belt-and-suspenders: the seeded raw episode IDs must not
        // appear as VALUES anywhere either (rules out a future schema
        // change that names the field something innocuous but stuffs
        // the raw id into it).
        let jsonString = String(decoding: data, as: UTF8.self)
        for raw in BundleShapeFixtures.rawDefaultEpisodeIds {
            #expect(
                !jsonString.contains(raw),
                "Raw episode ID '\(raw)' leaked into default-bundle JSON values"
            )
        }
    }

    @Test("Default-bundle JSON: opt-in-only field names (transcript_excerpts, feature_summaries, episode_title) never appear")
    func defaultBundleNeverContainsTranscriptOrFeatureSummaryKeys() throws {
        let data = try BundleShapeFixtures.loadOrGenerate(
            filename: BundleShapeFixtures.defaultFixtureFilename,
            generator: BundleShapeFixtures.encodedDefaultBundle
        )
        let object = try JSONSerialization.jsonObject(with: data, options: [])
        let allKeys = collectAllKeys(in: object)
        for forbidden in Self.forbiddenInDefaultSubtree {
            #expect(
                !allKeys.contains(forbidden),
                "Forbidden opt-in field '\(forbidden)' present in default bundle JSON"
            )
        }
    }

    // MARK: - (b) Opt-in isolation: hashed episode_id is 64 lowercase hex chars

    @Test("Opt-in-bundle JSON: every transcript_excerpts owner episode carries an episode_id and the parent default subtree's episode_id_hash matches the SHA-256 hex shape")
    func optInBundleEpisodeIdIsHashed64HexChars() throws {
        let data = try BundleShapeFixtures.loadOrGenerate(
            filename: BundleShapeFixtures.optInFixtureFilename,
            generator: BundleShapeFixtures.encodedOptInBundle
        )
        let object = try JSONSerialization.jsonObject(with: data, options: [])
        let dict = try #require(object as? [String: Any])

        // Hash regex: SHA-256 hex is exactly 64 lowercase hex chars.
        let hashRegex = #/^[0-9a-f]{64}$/#

        // (b1) The opt_in subtree carries the cleartext episode_id —
        // that is the user-consented payload. Confirm it's present and
        // matches the seeded raw value.
        let optIn = try #require(dict["opt_in"] as? [String: Any],
                                 "opt_in subtree must be present in the opt-in fixture")
        let episodes = try #require(optIn["episodes"] as? [[String: Any]],
                                    "opt_in.episodes must be an array of objects")
        #expect(!episodes.isEmpty)
        for episode in episodes {
            let episodeId = try #require(episode["episode_id"] as? String,
                                         "Opt-in episode must carry episode_id")
            // The opt-in surface IS where a cleartext id is allowed.
            #expect(!episodeId.isEmpty)
            // And `transcript_excerpts` are scoped to this opt-in path.
            #expect(episode["transcript_excerpts"] as? [Any] != nil,
                    "Opt-in episode must carry transcript_excerpts array")
        }

        // (b2) Every episode_id_hash that DOES appear in the default
        // subtree of the same bundle file matches the SHA-256 hex
        // shape. This is the hash assertion the bead lists for
        // deliverable 2.
        let defaultSubtree = try #require(dict["default"] as? [String: Any])
        let allDefaultKeysAndValues = collectAllStringValuesByKey(in: defaultSubtree)
        let hashes = allDefaultKeysAndValues
            .filter { $0.key == "episode_id_hash" }
            .map(\.value)
        #expect(!hashes.isEmpty, "Default subtree must carry at least one episode_id_hash to audit")
        for hash in hashes {
            #expect(
                (try? hashRegex.wholeMatch(in: hash)) != nil,
                "episode_id_hash '\(hash)' does not match SHA-256 hex shape ^[0-9a-f]{64}$"
            )
        }
    }

    // MARK: - Tree walkers (private helpers)

    /// Collect every dictionary key reachable from `root` (recursive
    /// over nested dicts and arrays). Used for "no episodeId anywhere"
    /// and similar tree-wide audits.
    private func collectAllKeys(in root: Any) -> [String] {
        var keys: [String] = []
        var stack: [Any] = [root]
        while let next = stack.popLast() {
            if let dict = next as? [String: Any] {
                keys.append(contentsOf: dict.keys)
                stack.append(contentsOf: dict.values)
            } else if let array = next as? [Any] {
                stack.append(contentsOf: array)
            }
        }
        return keys
    }

    /// Collect every `(key, value)` pair where the value is a String.
    /// Used to walk for `episode_id_hash` values regardless of nesting
    /// depth.
    private func collectAllStringValuesByKey(in root: Any) -> [(key: String, value: String)] {
        var result: [(key: String, value: String)] = []
        var stack: [Any] = [root]
        while let next = stack.popLast() {
            if let dict = next as? [String: Any] {
                for (k, v) in dict {
                    if let s = v as? String {
                        result.append((key: k, value: s))
                    }
                    stack.append(v)
                }
            } else if let array = next as? [Any] {
                stack.append(contentsOf: array)
            }
        }
        return result
    }
}
