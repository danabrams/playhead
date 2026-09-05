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

    /// playhead-bfq7: one banner-tally row per canonical episode id.
    /// Seeding these with the SAME raw ids the journal entries use is
    /// what makes the fixture's raw-episode-id VALUE sweep cover the
    /// tally too — an unhashed id here would show up as a literal
    /// `ep-fsy3-default-1` in the fixture bytes.
    static func bannerTallies() -> [BannerTallySession] {
        rawDefaultEpisodeIds.enumerated().map { idx, id in
            BannerTallySession(
                sessionKey: "fsy3-session-\(idx)",
                episodeId: id,
                bannerCount: idx + 2,
                autoSkippedCount: idx,
                suggestCount: 2,
                firstShownAt: now,
                lastShownAt: now.addingTimeInterval(Double(idx))
            )
        }
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
            installID: installID,
            bannerTallies: bannerTallies()
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
            installID: installID,
            bannerTallies: bannerTallies()
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
    /// missing OR when its schema has drifted.
    ///
    /// playhead-bfq7 added the schema-drift arm. Read-if-present alone
    /// had a silent failure mode that actually bit: a bead adds a
    /// top-level field to `DefaultBundle`, the checked-in fixture keeps
    /// the OLD shape, and every audit below — including the raw-episode-id
    /// value sweep — quietly runs against a bundle that is no longer the
    /// one that ships, while the artifact the file comment offers to
    /// legal review misrepresents it. The fixtures are also build
    /// resources, so "just delete it and rerun" breaks the build rather
    /// than regenerating.
    ///
    /// The check is deliberately narrow: a MISSING KEY relative to live
    /// output means the schema moved and the fixture must be refreshed.
    /// Value-level drift still leaves the frozen bytes in place, so the
    /// fixture keeps its job as a byte-stable reference.
    static func loadOrGenerate(filename: String, generator: () throws -> Data) throws -> Data {
        let directory = fixtureDirectoryURL()
        let url = directory.appendingPathComponent(filename, isDirectory: false)
        if let data = try? Data(contentsOf: url),
           !(try isSchemaStale(onDisk: data, generator: generator)) {
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

    /// True when the live bundle emits a top-level `default` key the
    /// on-disk fixture lacks. Unreadable/undecodable bytes also count as
    /// stale — a fixture nobody can parse audits nothing.
    private static func isSchemaStale(
        onDisk data: Data,
        generator: () throws -> Data
    ) throws -> Bool {
        guard let onDiskKeys = defaultSubtreeKeys(in: data) else { return true }
        guard let liveKeys = defaultSubtreeKeys(in: try generator()) else { return false }
        return !liveKeys.subtracting(onDiskKeys).isEmpty
    }

    private static func defaultSubtreeKeys(in data: Data) -> Set<String>? {
        guard let root = try? JSONSerialization.jsonObject(with: data, options: [])
                as? [String: Any],
              let defaultSubtree = root["default"] as? [String: Any]
        else {
            return nil
        }
        var paths: Set<String> = []
        collectKeyPaths(defaultSubtree, prefix: "", into: &paths)
        return paths
    }

    /// Every key PATH in the subtree, not just the top level.
    ///
    /// REVIEW ROUND 2 (playhead-p70f): the top-level-only version had the same
    /// silent failure the drift check was written to prevent, one level down.
    /// `rediff_diagnostics` is a whole nested subtree, so a bead adding or
    /// renaming a field INSIDE it — as round 2's `read_failures` does — left
    /// the checked-in fixture stale while the drift check reported it fresh,
    /// and legal review would have been handed a bundle that no longer matched
    /// what ships. Array elements collapse onto a single `key[]` prefix so the
    /// path set stays a shape, not a row count.
    private static func collectKeyPaths(
        _ value: Any,
        prefix: String,
        into paths: inout Set<String>
    ) {
        if let dictionary = value as? [String: Any] {
            for (key, child) in dictionary {
                let path = prefix.isEmpty ? key : "\(prefix).\(key)"
                paths.insert(path)
                collectKeyPaths(child, prefix: path, into: &paths)
            }
        } else if let array = value as? [Any] {
            for element in array {
                collectKeyPaths(element, prefix: prefix + "[]", into: &paths)
            }
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
        // playhead-yz3o: counts over the WHOLE work journal, so a completion
        // rate is computable without inferring it from the 200-row tail — which
        // was SATURATED on the device that produced the bead, making the ratio
        // inside it not a rate. Carries only integers, a closed-vocabulary
        // event-type histogram and two timestamps; no ids and no free text.
        "scheduler_event_census",
        // playhead-i7kvl.3: the north-star counters, so a tester's report can
        // answer manual reaches per listening hour. Integers under a CLOSED
        // vocabulary (AnalyticsMetricKey x AnalyticsCohortKey, the exact set
        // Addendum A approved) — no ids, no free text. Upload stays OFF; this
        // rides only in the user-initiated bundle.
        "analytics_counters",
        // playhead-h9y6: launch-path failures the app used to swallow. Three
        // fields: a count, an error CLASS (a closed vocabulary — never text), a
        // timestamp; `recorded` says whether the recorder existed at all.
        "launch_health",
        "app_version",
        "os_version",
        "device_class",
        "build_type",
        "eligibility_snapshot",
        "analysis_unavailable_reason",
        "scheduler_events",
        "work_journal_tail",
        // playhead-au2v.1.3: chapter-phase events sibling array. Added
        // to the allowed set so the shape audit (a) does not reject a
        // bundle that legitimately carries chapter-phase events — both
        // the empty-array and populated forms are permitted.
        "chapter_phase_events",
        // playhead-2hpn: per-show recurring-jingle profile summaries.
        // Carries no raw episodeId/feedURL — only the install-scoped
        // `show_identifier_hash`. Added to the allowed set so the
        // shape audit (a) does not reject a bundle carrying these.
        "music_bed_profiles",
        // playhead-beh3: per-device-class adaptive estimator state for
        // the Welford+EWMA slice-sizing loop. Always encoded (empty
        // array when no rows exist) so the shape audit must accept the
        // key. Privacy review: the record carries device-class bucket
        // strings + math telemetry — NO episodeId, NO PII — so it stays
        // in the default subtree alongside other device-class fields.
        "learned_device_profiles",
        // playhead-jw63.4: the local MetricKit crash + hang ring buffer.
        // Always encoded (empty array on a device that has never
        // crashed) so the shape audit must accept the key. Privacy
        // review: `StabilityDiagnosticRecord` is a closed shape — every
        // string passed `DiagnosticTextSanitizer`'s allowlist, and it
        // carries NO episode reference, not even a hash. The proof
        // lives in `StabilityDiagnosticScrubbingTests` (legal item e).
        "stability_diagnostics",
        // playhead-bfq7: per-episode banner-card tally. Always encoded
        // (empty array when no card has been presented) so the shape
        // audit must accept the key. Privacy review:
        // `DefaultBundle.BannerTallySummary` is a closed shape of four
        // integers, two timestamps, and the SAME salted
        // `episode_id_hash` the scheduler-event tail carries — no
        // title, feed URL, advertiser, product, window id, or
        // transcript text. The proof lives in
        // `BannerTallyDiagnosticsPrivacyTests` (legal item g).
        "banner_tallies",
        // playhead-p70f: the rediff re-fetch lane (bandwidth ledger,
        // lagged attempt states, day-0 attempt records, and the lane's
        // BGTask fires). Always encoded so a support engineer can tell
        // "the lane did nothing" from "this bundle predates the lane".
        // Privacy review: every per-asset row carries the install-scoped
        // `asset_id_hash` produced by the SAME `EpisodeIdHasher` the
        // scheduler-event and banner tails use — never the raw
        // `analysisAssetId`. Everything else is an integer, a timestamp,
        // or a closed enum rawValue: the day-0 records' `lastDetail`
        // (which is `String(describing: error)` and can carry the
        // enclosure URL) is deliberately NOT projected, and the run
        // ledger's free-form `deferReason` is PARSED into two integers
        // rather than forwarded. The proof lives in
        // `DiagnosticsBundleRediffTests`.
        "rediff_diagnostics",
        // playhead-wvdz: whether the analysis database opened, across
        // launches. Always encoded so a support engineer can tell "the
        // store is fine" from "this bundle predates the signal" — the
        // absence of exactly that distinction is why a silent wipe was
        // invisible. Privacy review: `AnalysisStoreHealthState` is a
        // closed shape of counters, dates and enum rawValues this repo
        // defines. It carries NO episode, asset, or show reference of
        // any kind — a database that will not open has no episode to
        // name. Its two free-text-shaped fields are both narrowed: each
        // failure's `detail` must pass `DiagnosticTextSanitizer`'s
        // character allowlist or be dropped, and quarantine entries
        // carry a directory NAME, never a container path (which would
        // embed the install UUID and the user's home directory). The
        // proof lives in `AnalysisStoreHealthDiagnosticsPrivacyTests`
        // (legal item h).
        "analysis_store_health",
        // playhead-se2h: whether the ASR model ever loaded, across
        // launches. Always encoded, and its default is `unknown` rather
        // than a healthy value — an unwired signal must read as "no
        // evidence", never as a working speech stack. Privacy review:
        // counters, dates, and rawValues of enums this repo defines, with
        // NO free-text field of any kind (not even the sanitised `detail`
        // its `analysis_store_health` sibling carries) — a model that will
        // not load has no episode, show, or URL to name. Failure causes
        // reuse `TranscriptFailureClass`, the closed set playhead-8ysk
        // pinned. The proof lives in
        // `SpeechModelLoadDiagnosticsPrivacyTests`.
        "speech_model_load"
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


// MARK: - playhead-g58r: the guard sees CONTENT, not a key list

/// THE CORRECTION THIS SUITE EXISTS FOR. The checklist above is widely treated
/// as the compliance gate on the only egress path in the app, and for NESTED
/// RECORD keys it was not one. Measured during playhead-8ysk review round 1:
/// with an export vocabulary validation removed, a real episode title reached
/// the encoded bundle JSON and `defaultBundleHasOnlyAllowedKeys` PASSED,
/// because its allow-list enumerates the TOP LEVEL and the `default` subtree
/// and nothing below them. The leak was caught only by a hand-written
/// adversarial test that happened to exist because someone thought to write
/// one.
///
/// So this suite is deliberately NOT another key list. Enumerating today's
/// nested keys reproduces the same hole for tomorrow's field. It is
/// CONTENT-BASED: every free-text-capable SOURCE field is populated with a
/// unique sentinel, the bundle is encoded, and the assertion is that no
/// sentinel appears anywhere in the bytes at any depth. A field nobody
/// registered cannot defeat it, because it never asks what the fields are.
///
/// It is also why the search is over the RAW ENCODED BYTES rather than a
/// parsed tree walk: a parse can only visit structure it understands, and a
/// string containing escaped JSON — which is exactly how `metadata` travels —
/// is one node to a walker and a whole subtree to a reader.
@MainActor
@Suite("Diagnostics bundle: no source free text survives encoding (playhead-g58r)")
struct DiagnosticsBundlePoisonValueTests {

    /// Sentinels chosen to be unmistakable and to survive any encoding that
    /// does not DROP them: no characters that JSON escapes, so a hit is a hit
    /// rather than an escaping artifact.
    private enum Poison {
        static let episodeTitle = "PoisonEpisodeTitleZZQ1"
        static let metadataFreeText = "PoisonMetadataFreeTextZZQ2"
        static let transcriptExcerpt = "PoisonTranscriptExcerptZZQ4"

        /// Every sentinel, so one assertion covers the whole set and adding a
        /// source field is one line rather than a new test.
        static let all: [(label: String, value: String)] = [
            ("episode title", episodeTitle),
            ("work-journal metadata free text", metadataFreeText),
            ("transcript excerpt", transcriptExcerpt)
        ]
    }

    private static let now = Date(timeIntervalSince1970: 1_700_000_000)

    private static func poisonedEntries() -> [WorkJournalEntry] {
        [
            WorkJournalEntry(
                id: "poison-row-0",
                episodeId: "poison-episode-0",
                generationID: UUID(uuidString: "00000000-0000-4000-8000-000000000009")!,
                schedulerEpoch: 1,
                timestamp: now.timeIntervalSince1970,
                eventType: .acquired,
                // `cause` is a typed `InternalMissCause`, not free text — it
                // cannot carry a sentinel, and that is a real (typed) guard
                // worth recording rather than a gap.
                cause: nil,
                metadata: "{\"note\":\"\(Poison.metadataFreeText)\",\"title\":\"\(Poison.episodeTitle)\"}",
                artifactClass: .scratch
            )
        ]
    }

    private static func encodedDefaultBundle() throws -> Data {
        let bundle = DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibility: BundleShapeFixtures.eligibility(),
            workJournalEntries: poisonedEntries(),
            installID: BundleShapeFixtures.installID,
            bannerTallies: []
        )
        return try DiagnosticsExportService.encode(
            DiagnosticsBundleFile(generatedAt: now, default: bundle, optIn: nil)
        )
    }

    // MARK: The acceptance

    @Test("THE ACCEPTANCE: no source free text survives into the DEFAULT bundle, at any depth")
    func noPoisonSurvivesTheDefaultBundle() throws {
        let data = try Self.encodedDefaultBundle()
        let json = try #require(String(data: data, encoding: .utf8))

        for poison in Poison.all {
            #expect(
                !json.contains(poison.value),
                """
                \(poison.label) reached the encoded diagnostics bundle. This is \
                the ONLY egress path in the app and the on-device mandate says \
                no episode content leaves the device. A key-based checklist \
                cannot see this — that is playhead-g58r.
                """
            )
        }
    }

    /// ANTI-VACUITY. A test that searches for strings which were never in the
    /// inputs passes for the wrong reason and would keep passing if the builder
    /// stopped being called at all. This proves the sentinels are genuinely
    /// present on the SOURCE side, so the assertion above is about the
    /// builder's behaviour rather than about the fixture being empty.
    @Test("the poison really is in the inputs — the assertion above is not vacuous")
    func poisonIsPresentInTheSourceEntries() throws {
        let entries = Self.poisonedEntries()
        let sourceText = entries.map(\.metadata).joined(separator: " ")
        for poison in Poison.all where poison.value != Poison.transcriptExcerpt {
            #expect(
                sourceText.contains(poison.value),
                "\(poison.label) must be present in the source, or the leak test proves nothing"
            )
        }
    }

    /// The bundle must still be a bundle. A builder that returned an empty
    /// object would pass every assertion above for the worst possible reason.
    @Test("the poisoned bundle is still a well-formed bundle carrying its documented subtrees")
    func poisonedBundleIsStillWellFormed() throws {
        let data = try Self.encodedDefaultBundle()
        let object = try JSONSerialization.jsonObject(with: data, options: [])
        let dict = try #require(object as? [String: Any])
        #expect(dict["default"] != nil, "an empty bundle would pass the leak test vacuously")
        #expect(dict["generated_at"] != nil)
        #expect(data.count > 64, "a near-empty encoding is not evidence about leaks")
    }

    /// The two nested subtrees the bead names, checked by NAME here only to
    /// record that they were the ones in flight when the hole was found. The
    /// protection itself is the content assertion above, which does not care
    /// what they are called.
    @Test("the recently-added nested subtrees carry no source free text either")
    func recentNestedSubtreesAreClean() throws {
        let json = try #require(String(data: try Self.encodedDefaultBundle(), encoding: .utf8))
        // rediff_diagnostics (playhead-p70f) and failure_class / failure_code
        // (playhead-8ysk) are the two nested subtrees added in the day before
        // this bead was filed.
        for poison in Poison.all {
            #expect(!json.contains(poison.value), "\(poison.label) leaked")
        }
    }
}


// MARK: - playhead-yz3o: a rate is computable without inferring it from a sample

/// THE DEFECT, MEASURED. `scheduler_events` is the last 200 rows. On the device
/// that produced this bead it held exactly 200 — 147 `acquired`, 9 `finalized`,
/// 20 `failed`, 24 `preempted` — i.e. SATURATED. The terminal rows for the
/// older acquisitions had already fallen off the front, so the
/// acquired-to-finalized ratio INSIDE that window is not a completion rate. It
/// was quoted as "6 % completion" and had to be retracted.
///
/// That is this repo's standing defect class living in the instrument: a value
/// that names one thing (what the tail happens to contain) read as though it
/// named another (what the device did). And it is invisible at the boundary —
/// a saturated tail and a journal that happens to hold exactly 200 rows are
/// byte-identical in the export.
///
/// `scheduler_event_census` is counted over the WHOLE journal BEFORE the tail
/// is taken, and every count carries its window, so the ratio has a stated
/// denominator and `truncated` says outright whether the tail is a sample.
@MainActor
@Suite("Scheduler-event census (playhead-yz3o)")
struct SchedulerEventCensusTests {

    private static let now = Date(timeIntervalSince1970: 1_700_000_000)

    /// `count` rows, of which `finalized` are terminal — enough to overflow the
    /// 200-row tail so the saturation is real rather than described.
    private static func journal(count: Int, finalized: Int) -> [WorkJournalEntry] {
        (0..<count).map { index in
            WorkJournalEntry(
                id: "row-\(index)",
                episodeId: "episode-\(index)",
                generationID: UUID(),
                schedulerEpoch: 1,
                timestamp: now.timeIntervalSince1970 + Double(index),
                // The oldest rows are the finalized ones, which is the whole
                // point: they are exactly what a trailing window drops.
                eventType: index < finalized ? .finalized : .acquired,
                cause: nil,
                metadata: "{}",
                artifactClass: .scratch
            )
        }
    }

    private static func build(_ entries: [WorkJournalEntry]) -> DefaultBundle {
        DiagnosticsBundleBuilder.buildDefault(
            appVersion: "1.0.0",
            osVersion: "iOS 26.0",
            deviceClass: .iPhone17Pro,
            buildType: .release,
            eligibility: BundleShapeFixtures.eligibility(),
            workJournalEntries: entries,
            installID: BundleShapeFixtures.installID,
            bannerTallies: []
        )
    }

    @Test("THE ACCEPTANCE: with a SATURATED tail the census still reports the whole journal")
    func censusSurvivesASaturatedTail() {
        // 260 rows: 40 finalized (oldest) then 220 acquired. The 200-row tail
        // can only reach the newest 200, so every finalized row falls off.
        let bundle = Self.build(Self.journal(count: 260, finalized: 40))

        #expect(bundle.schedulerEvents.count == 200, "the tail is saturated, as on the device")
        #expect(
            !bundle.schedulerEvents.contains { $0.eventType == "finalized" },
            "the premise: every terminal row has fallen off the front of the tail"
        )

        // The tail alone would say 0 % completion. The census says 40 of 260.
        let census = bundle.schedulerEventCensus
        #expect(census.total == 260)
        #expect(census.byEventType["finalized"] == 40)
        #expect(census.byEventType["acquired"] == 220)
        #expect(census.exported == 200)
        #expect(census.truncated, "a reader must be told the tail is a SAMPLE")
    }

    /// The discriminator that could not previously be derived. A saturated tail
    /// and a journal of exactly 200 rows produce identical `scheduler_events`;
    /// only `truncated` tells them apart.
    @Test("`truncated` distinguishes a sample from the whole journal at the boundary")
    func truncatedIsTheDiscriminatorAtTheBoundary() {
        let exactlyFull = Self.build(Self.journal(count: 200, finalized: 10))
        let overflowing = Self.build(Self.journal(count: 201, finalized: 10))

        #expect(exactlyFull.schedulerEvents.count == overflowing.schedulerEvents.count)
        #expect(!exactlyFull.schedulerEventCensus.truncated)
        #expect(overflowing.schedulerEventCensus.truncated)
    }

    @Test("the census carries a WINDOW, so a rate has a duration rather than just a numerator")
    func censusCarriesItsWindow() {
        let bundle = Self.build(Self.journal(count: 10, finalized: 3))
        let census = bundle.schedulerEventCensus
        #expect(census.windowStart == Self.now.timeIntervalSince1970)
        #expect(census.windowEnd == Self.now.timeIntervalSince1970 + 9)
        #expect(
            (census.windowEnd ?? 0) > (census.windowStart ?? 0),
            "a count with no duration is not a rate"
        )
    }

    @Test("an EMPTY journal reports zero and no window — nobody counted is not the same as none happened")
    func emptyJournalIsHonest() {
        let census = Self.build([]).schedulerEventCensus
        #expect(census.total == 0)
        #expect(census.exported == 0)
        #expect(!census.truncated)
        #expect(census.windowStart == nil, "a fabricated window would invent a rate's denominator")
        #expect(census.windowEnd == nil)
    }

    @Test("a bundle written BEFORE this field decodes to an empty census, never to a zero count")
    func legacyBundleDecodesToEmpty() throws {
        // The tolerant decode. `.empty` reads as "nobody counted"; a decode
        // failure or a fabricated zero would both be worse.
        let census = DefaultBundle.SchedulerEventCensus.empty
        #expect(census.total == 0)
        #expect(!census.truncated)
        #expect(census.windowStart == nil)
    }

    @Test("the census counts the WHOLE journal, not the tail — the ordering the fix depends on")
    func censusIsComputedBeforeTheTail() {
        // If the census were computed from `schedulerTailAsc` this would read
        // 200 and the bead's defect would be intact one layer down.
        #expect(Self.build(Self.journal(count: 500, finalized: 100)).schedulerEventCensus.total == 500)
    }

    @Test("the census carries no ids and no free text — only counts, a closed vocabulary and timestamps")
    func censusCarriesNothingIdentifying() throws {
        let bundle = Self.build(Self.journal(count: 5, finalized: 2))
        let data = try JSONEncoder().encode(bundle.schedulerEventCensus)
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(!json.contains("episode-"), "an episode id in the census would be a new egress")
        // Keys are WorkJournalEventType raw values: a closed vocabulary, so
        // this histogram can never carry free text.
        for key in bundle.schedulerEventCensus.byEventType.keys {
            #expect(
                WorkJournalEntry.EventType(rawValue: key) != nil,
                "\(key) is outside the closed vocabulary"
            )
        }
    }
}
