// CorpusExporter.swift
// narE (playhead-dgzw): Debug-menu corpus export.
//
// Reads CorrectionEvent + DecodedSpan rows from analysis.sqlite and writes a
// JSONL corpus file to Documents/ so it is visible via the Files app /
// Finder sharing (UIFileSharingEnabled + LSSupportsOpeningDocumentsInPlace
// are already set for dev builds by narL's Info.plist entries).
//
// Design:
//   - Debug-only: wrapped in #if DEBUG. The release build strips the type.
//   - Streaming: uses FileHandle.write per record; does not materialize an
//     Array of all rows in memory. Per-asset fetches keep the working set
//     proportional to the largest single asset's span count.
//   - Schema version: every emitted record carries `schemaVersion: 1` so
//     downstream tooling can drift-check per record.
//   - Join keys: records use the analysisAssetId and atom-ordinal keys
//     exactly as they appear on DecodedSpan / CorrectionEvent, matching
//     narL's decision-log.jsonl by construction.
//   - Corrupt-scope tolerance: a correction row whose `scope` does not
//     deserialize via CorrectionScope.deserialize(_:) is logged and
//     skipped; it does NOT abort the export.
//   - Optional metadata (podcastId, coverage times, etc.) is serialized
//     as explicit null, not omitted.

#if DEBUG

import CryptoKit
import Foundation
import OSLog

// MARK: - CorpusExportResult

/// Result of a corpus export, returned to the caller so the UI can show
/// a share sheet or a confirmation summary.
struct CorpusExportResult: Sendable, Equatable {
    /// Absolute path of the written JSONL file.
    let fileURL: URL
    /// Number of asset rows emitted.
    let assetCount: Int
    /// Number of DecodedSpan rows emitted.
    let spanCount: Int
    /// Number of CorrectionEvent rows emitted.
    let correctionCount: Int
    /// Number of CorrectionEvent rows skipped due to unparseable `scope` strings.
    let skippedCorrectionCount: Int
    /// playhead-epfk: Number of `ad_window` rows emitted (one JSONL line
    /// per persisted `AdWindow`, carrying `catalogStoreMatchSimilarity`).
    let adWindowCount: Int
    /// Absolute path of the sibling `decision-log.jsonl` written by narL,
    /// if it exists in the same Documents directory — exposed so the
    /// caller can build a combined bundle.
    let decisionLogManifestURL: URL?
    /// Absolute path of the `shadow-decisions.jsonl` written alongside the
    /// corpus export by ``ShadowDecisionsExporter``. `nil` only if the
    /// shadow sidecar write threw (logged); a shadow-empty store still
    /// produces a zero-row file so downstream tooling can distinguish
    /// "shadow capture never ran" from "shadow capture ran, no rows".
    let shadowManifestURL: URL?
    /// Number of shadow rows written to `shadow-decisions.jsonl`. Zero
    /// when the store has no shadow rows yet.
    let shadowRowCount: Int
}

// MARK: - CorpusExportSource

/// Narrow test seam the exporter queries against. `AnalysisStore` conforms
/// below; tests can supply a mock that throws on specific methods to exercise
/// the SQL-error path without corrupting a real sqlite file.
///
/// Also requires `ShadowDecisionsExportSource` because a corpus export
/// writes `shadow-decisions.jsonl` as a sibling so the harness in
/// `playhead-narl.1` can consume both files from a single Files.app pull.
protocol CorpusExportSource: ShadowDecisionsExportSource {
    func fetchAllAssets() async throws -> [AnalysisAsset]
    func fetchDecodedSpans(assetId: String) async throws -> [DecodedSpan]
    /// playhead-epfk: pull the persisted ad-window rows so the exporter
    /// can emit a per-window record carrying `catalogStoreMatchSimilarity`
    /// (the cross-episode `AdCatalogStore` top-match similarity recorded
    /// at fusion time). NARL eval needs this column to measure the
    /// fingerprint-store firing rate independently from the in-pipeline
    /// transcript sponsor catalog (both share the `.catalog` evidence
    /// source label until the `subSource` disambiguator lands in
    /// readers).
    func fetchAdWindows(assetId: String) async throws -> [AdWindow]
    func loadCorrectionEvents(analysisAssetId: String) async throws -> [CorrectionEvent]
    /// Look up the `podcastId` recorded for an episode in the
    /// `analysis_jobs` table, or `nil` when no job row exists for the
    /// episode. Used by the exporter so asset records carry the podcastId
    /// needed for show-level aggregation downstream (playhead-narl.1
    /// HIGH-3). The mock can return `nil` for all episodes to exercise the
    /// "podcastId absent" JSONL path.
    func fetchPodcastId(forEpisodeId episodeId: String) async throws -> String?
    /// playhead-i9dj: look up the `podcast_profiles` row for `podcastId`
    /// so the exporter can emit the human-readable show title alongside
    /// the existing podcastId. Returns `nil` when no profile row exists
    /// (cold-start before trust-scoring has materialized one). Mocks can
    /// return `nil` to exercise the missing-title JSONL path.
    func fetchPodcastProfile(podcastId: String) async throws -> PodcastProfile?
}

extension AnalysisStore: CorpusExportSource {
    /// playhead-i9dj: protocol bridge for `fetchPodcastProfile(podcastId:)` —
    /// the canonical method on `AnalysisStore` is `fetchProfile(podcastId:)`.
    /// Renaming the actor method in place would ripple through every trust-
    /// scoring call site, so the protocol uses the longer name and the
    /// extension trampolines into the existing one. `fetchProfile` is
    /// actor-isolated and synchronous internally; the protocol method
    /// is `async` so non-isolated callers can hop onto the actor.
    nonisolated func fetchPodcastProfile(podcastId: String) async throws -> PodcastProfile? {
        try await fetchProfile(podcastId: podcastId)
    }
}

// MARK: - CorpusExportDedupMemo

/// playhead-vnni: per-`documentsURL` memo of the most recent successful
/// export's content digest, so a rapid-fire trigger storm (debug Button
/// repeat-tapped before the first Task body sets `corpusExportInProgress
/// = true`) collapses into a single physical file instead of N
/// byte-identical artifacts. The 2026-04-25 device snapshot recorded 4
/// duplicates within 1.8s — see `CorpusExporterTests.exportDeduplicates...`.
///
/// Keyed by the `documentsURL.path` so that two separate sinks (a debug
/// export to Documents/ and a hypothetical eval-export to a sibling dir)
/// don't cross-suppress. Default lifetime is process-shared via `.shared`;
/// tests construct an isolated memo so they neither leak into nor observe
/// state from each other.
actor CorpusExportDedupMemo {
    private struct Entry {
        let digestHex: String
        let fileURL: URL
        let writtenAt: Date
    }

    private var entries: [String: Entry] = [:]

    /// Process-shared memo for production callers. Tests create an isolated
    /// instance via `CorpusExportDedupMemo()` to avoid cross-test pollution.
    static let shared = CorpusExportDedupMemo()

    init() {}

    /// Returns the prior file URL if `digestHex` was last recorded for
    /// `documentsURL` within `window` seconds of `now`. Otherwise `nil`.
    func recentMatch(
        documentsURL: URL,
        digestHex: String,
        window: TimeInterval,
        now: Date
    ) -> URL? {
        let key = documentsURL.path
        guard let entry = entries[key] else { return nil }
        guard entry.digestHex == digestHex else { return nil }
        let age = now.timeIntervalSince(entry.writtenAt)
        guard age >= 0, age <= window else { return nil }
        // Verify the prior file still exists; a user who manually deleted
        // the file via Files.app should be able to re-export.
        guard FileManager.default.fileExists(atPath: entry.fileURL.path) else {
            entries.removeValue(forKey: key)
            return nil
        }
        return entry.fileURL
    }

    /// Record a successful export so the next near-simultaneous identical
    /// trigger can be deduplicated.
    func record(
        documentsURL: URL,
        digestHex: String,
        fileURL: URL,
        now: Date
    ) {
        let key = documentsURL.path
        entries[key] = Entry(digestHex: digestHex, fileURL: fileURL, writtenAt: now)
    }
}

// MARK: - CorpusExporter

enum CorpusExporter {

    /// JSONL schema version stamped on every emitted record. Bump when
    /// the record shape changes in a non-backward-compatible way; downstream
    /// tooling is expected to version-gate at the per-record level.
    static let schemaVersion: Int = 1

    private static let logger = Logger(subsystem: "com.playhead", category: "CorpusExporter")

    // MARK: - Filename

    /// Build the filename used for an export written at `date`.
    ///
    /// ISO-8601 with millisecond fractional seconds in UTC, with colons replaced
    /// by dashes so the file is Finder / Files-app friendly. Milliseconds defeat
    /// the same-second collision case: two back-to-back exports (e.g. a user
    /// double-tapping the action) land in distinct files rather than silently
    /// overwriting each other.
    static func filename(for date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(identifier: "UTC")
        let iso = formatter.string(from: date)
        let safe = iso.replacingOccurrences(of: ":", with: "-")
        return "corpus-export.\(safe).jsonl"
    }

    // MARK: - Per-record serializers

    /// Serialize an `AnalysisAsset` row as a single JSONL line body (no trailing newline).
    /// Throws if JSONSerialization fails, which in practice requires a non-UTF-8 field
    /// that shouldn't appear in real data.
    ///
    /// `podcastId` is looked up by the caller via `CorpusExportSource.fetchPodcastId(forEpisodeId:)`
    /// and threaded in here so show-level grouping works downstream (playhead-narl.1 HIGH-3).
    /// Emitted as explicit null when the lookup returns nil.
    ///
    /// `detectorVersion` and `buildCommitSHA` (playhead-gtt9.21) stamp
    /// the device binary's identity onto each asset row so downstream
    /// FrozenTrace fixtures carry capture provenance. Defaults read
    /// from `AdDetectionConfig.default.detectorVersion` and
    /// `BuildInfo.commitSHA`. Tests that need to assert exact JSON
    /// shape pass explicit values; production callers (the debug
    /// menu's CorpusExporter.export path) rely on the defaults.
    static func assetLine(
        _ asset: AnalysisAsset,
        podcastId: String? = nil,
        podcastTitle: String? = nil,
        detectorVersion: String = AdDetectionConfig.default.detectorVersion,
        buildCommitSHA: String = BuildInfo.commitSHA
    ) throws -> Data {
        // playhead-i9dj: emit the human-readable identifiers so the
        // exported corpus is legible standalone. Both fields are
        // explicit JSON `null` when missing — old exports without
        // titles still parse on the read side because consumers should
        // treat them as optional. `episodeTitle` comes off the
        // `analysis_assets` row directly; `podcastTitle` is looked up
        // by the caller via `fetchProfile(podcastId:)?.title`.
        let obj: [String: Any] = [
            "type": "asset",
            "schemaVersion": schemaVersion,
            "analysisAssetId": asset.id,
            "episodeId": asset.episodeId,
            "podcastId": podcastId as Any? ?? NSNull(),
            "podcastTitle": podcastTitle as Any? ?? NSNull(),
            "episodeTitle": asset.episodeTitle as Any? ?? NSNull(),
            "assetFingerprint": asset.assetFingerprint,
            "weakFingerprint": asset.weakFingerprint as Any? ?? NSNull(),
            "sourceURL": asset.sourceURL,
            "analysisState": asset.analysisState,
            "analysisVersion": asset.analysisVersion,
            "artifactClass": asset.artifactClass.rawValue,
            "featureCoverageEndTime": asset.featureCoverageEndTime as Any? ?? NSNull(),
            "fastTranscriptCoverageEndTime": asset.fastTranscriptCoverageEndTime as Any? ?? NSNull(),
            "confirmedAdCoverageEndTime": asset.confirmedAdCoverageEndTime as Any? ?? NSNull(),
            "terminalReason": asset.terminalReason as Any? ?? NSNull(),
            // gtt9.21: provenance — empty string is NOT used here (we
            // always have a real value from AdDetectionConfig.default
            // and BuildInfo.commitSHA's "unknown" fallback). The
            // FrozenTrace builder coerces a missing JSONL key to "" on
            // the read side so old corpus exports still decode.
            "detectorVersion": detectorVersion,
            "buildCommitSHA": buildCommitSHA,
        ]
        return try jsonLineData(from: obj)
    }

    /// Serialize a `DecodedSpan` as a single JSONL line body.
    static func spanLine(_ span: DecodedSpan) throws -> Data {
        // Encode anchorProvenance via the type's own Codable adapter so the
        // JSON shape matches what the on-disk `decoded_spans.anchorProvenanceJSON`
        // column stores — downstream tooling can reuse the same decoder.
        let provenanceData = try JSONEncoder().encode(span.anchorProvenance)
        let provenanceAny = try JSONSerialization.jsonObject(with: provenanceData)

        let obj: [String: Any] = [
            "type": "decision",
            "schemaVersion": schemaVersion,
            "spanId": span.id,
            "analysisAssetId": span.assetId,
            "firstAtomOrdinal": span.firstAtomOrdinal,
            "lastAtomOrdinal": span.lastAtomOrdinal,
            "startTime": span.startTime,
            "endTime": span.endTime,
            "anchorProvenance": provenanceAny,
        ]
        return try jsonLineData(from: obj)
    }

    /// Serialize an `AdWindow` row as a single JSONL line body.
    ///
    /// playhead-epfk: emits the per-window `catalogStoreMatchSimilarity`
    /// (top match similarity from `AdCatalogStore.matches` recorded at
    /// fusion time) as an explicit JSON value or `null`. NARL eval reads
    /// this column to compute the fingerprint-store firing rate
    /// (`catalogStoreMatchSimilarity >= 0.80 / total ad_windows with non-
    /// null value`) — the only way to measure how tightly the
    /// correction loop closes on real corpora today.
    static func adWindowLine(_ window: AdWindow) throws -> Data {
        let obj: [String: Any] = [
            "type": "ad_window",
            "schemaVersion": schemaVersion,
            "id": window.id,
            "analysisAssetId": window.analysisAssetId,
            "startTime": window.startTime,
            "endTime": window.endTime,
            "confidence": window.confidence,
            "boundaryState": window.boundaryState,
            "decisionState": window.decisionState,
            "detectorVersion": window.detectorVersion,
            "metadataSource": window.metadataSource,
            "metadataConfidence": window.metadataConfidence as Any? ?? NSNull(),
            "evidenceSources": window.evidenceSources as Any? ?? NSNull(),
            "eligibilityGate": window.eligibilityGate as Any? ?? NSNull(),
            "wasSkipped": window.wasSkipped,
            "userDismissedBanner": window.userDismissedBanner,
            // playhead-epfk: the field this whole bead exists to surface.
            // Explicit JSON `null` when the catalog store wasn't wired or
            // no fingerprint was queryable; `0.0` when the store ran but
            // produced no positive match (distinct from null — eval can
            // distinguish "loop is wired but inert" from "loop is off").
            "catalogStoreMatchSimilarity": window.catalogStoreMatchSimilarity as Any? ?? NSNull(),
        ]
        return try jsonLineData(from: obj)
    }

    /// Serialize a `CorrectionEvent` as a single JSONL line body. Returns nil if
    /// the row has an unparseable `scope` string, signalling the caller to skip it.
    static func correctionLine(_ event: CorrectionEvent) throws -> Data? {
        // Reject rows whose scope cannot be deserialized — corrupt persisted
        // input that would confuse downstream replay tools. We still let the
        // rest of the export proceed.
        guard CorrectionScope.deserialize(event.scope) != nil else {
            return nil
        }

        var targetRefsAny: Any = NSNull()
        if let refs = event.targetRefs {
            let data = try JSONEncoder().encode(refs)
            targetRefsAny = try JSONSerialization.jsonObject(with: data)
        }

        let obj: [String: Any] = [
            "type": "correction",
            "schemaVersion": schemaVersion,
            "id": event.id,
            "analysisAssetId": event.analysisAssetId,
            "scope": event.scope,
            "createdAt": event.createdAt,
            "source": event.source?.rawValue as Any? ?? NSNull(),
            "podcastId": event.podcastId as Any? ?? NSNull(),
            "correctionType": event.correctionType?.rawValue as Any? ?? NSNull(),
            "causalSource": event.causalSource?.rawValue as Any? ?? NSNull(),
            "targetRefs": targetRefsAny,
        ]
        return try jsonLineData(from: obj)
    }

    // MARK: - Export

    /// Default suppression window for `dedupMemo`. 10 seconds comfortably
    /// covers the 1.8 s storm captured in the 2026-04-25 snapshot while
    /// staying short enough that a deliberate "export, inspect Files.app,
    /// re-export" loop is not throttled.
    static let defaultDedupWindow: TimeInterval = 10.0

    /// Perform the full export against `store`, writing into `documentsURL`
    /// as `corpus-export.<timestamp>.jsonl`. Returns a summary result.
    ///
    /// Streams records to disk via `FileHandle.write` — no Array-in-memory
    /// accumulation of the full corpus. Memory use is bounded by the largest
    /// single asset's span or correction count.
    ///
    /// playhead-vnni: a SHA-256 digest of the streamed bytes is computed
    /// inline. After the file is closed, the exporter consults
    /// `dedupMemo`: if an identical-content export to the same
    /// `documentsURL` was recorded within `dedupWindow`, the
    /// just-written file is removed and the prior file's URL is
    /// returned. Counts in the result still reflect the rows the
    /// current call observed; only the on-disk artifact is collapsed.
    static func export(
        store: some CorpusExportSource,
        documentsURL: URL,
        now: Date = Date(),
        dedupMemo: CorpusExportDedupMemo = .shared,
        dedupWindow: TimeInterval = defaultDedupWindow
    ) async throws -> CorpusExportResult {
        let fm = FileManager.default
        if !fm.fileExists(atPath: documentsURL.path) {
            try fm.createDirectory(at: documentsURL, withIntermediateDirectories: true)
        }

        let fileURL = documentsURL.appendingPathComponent(filename(for: now))

        // Create (or truncate) the output file, then stream records with
        // FileHandle so we never hold all rows in memory simultaneously.
        fm.createFile(atPath: fileURL.path, contents: nil, attributes: nil)
        let handle = try FileHandle(forWritingTo: fileURL)
        // Unlink partially-written file on any throw between here and the
        // successful return, so the Documents/ directory the user sees in
        // Files.app never accumulates empty or partial export artifacts.
        var didSucceed = false
        defer {
            try? handle.close()
            if !didSucceed {
                try? fm.removeItem(at: fileURL)
            }
        }

        // playhead-vnni: tee the streamed bytes into a SHA-256 digest so we
        // can compare this export's content to the previous one in the same
        // sink. The hasher is updated by `write(line:to:hasher:)` for every
        // record body and trailing newline, so the final digest matches the
        // file's contents exactly without a re-read.
        var hasher = SHA256()

        var assetCount = 0
        var spanCount = 0
        var correctionCount = 0
        var skippedCorrectionCount = 0
        var adWindowCount = 0

        let assets = try await store.fetchAllAssets()
        // playhead-i9dj: cache resolved podcast titles by podcastId so
        // we don't re-query `podcast_profiles` for every asset that
        // shares a show. The cache is per-export (function scope) so it
        // never grows unbounded across runs.
        var podcastTitleCache: [String: String?] = [:]
        for asset in assets {
            // Look up podcastId by episodeId. A failing lookup is non-fatal:
            // downstream tooling reads podcastId as optional (HIGH-3 allows
            // explicit null) so we log and emit null rather than abort.
            let podcastId: String?
            do {
                podcastId = try await store.fetchPodcastId(forEpisodeId: asset.episodeId)
            } catch {
                logger.warning(
                    "export: fetchPodcastId failed for asset=\(asset.id, privacy: .public) episode=\(asset.episodeId, privacy: .public) error=\(String(describing: error), privacy: .public) — emitting null"
                )
                podcastId = nil
            }

            // playhead-i9dj: look up the podcast (show) title via
            // `podcast_profiles.title`. Cache hits avoid the extra
            // SQL on shows that span many episodes; misses go through
            // a single fetchProfile and are memoized for this export.
            // Failures emit null rather than aborting (parity with the
            // podcastId lookup above).
            let podcastTitle: String?
            if let pid = podcastId {
                if let cached = podcastTitleCache[pid] {
                    podcastTitle = cached
                } else {
                    do {
                        podcastTitle = try await store.fetchPodcastProfile(podcastId: pid)?.title
                    } catch {
                        logger.warning(
                            "export: fetchPodcastProfile failed for asset=\(asset.id, privacy: .public) podcast=\(pid, privacy: .public) error=\(String(describing: error), privacy: .public) — emitting null"
                        )
                        podcastTitle = nil
                    }
                    podcastTitleCache[pid] = podcastTitle
                }
            } else {
                podcastTitle = nil
            }

            // asset record
            let assetData = try assetLine(asset, podcastId: podcastId, podcastTitle: podcastTitle)
            try write(line: assetData, to: handle, hasher: &hasher)
            assetCount += 1

            // decision (DecodedSpan) records for this asset
            let spans: [DecodedSpan]
            do {
                spans = try await store.fetchDecodedSpans(assetId: asset.id)
            } catch {
                // Log and continue: a transient SQL failure on one asset must
                // not abort the whole export. Downstream tooling can detect
                // the gap because other assets' records still serialize.
                logger.warning(
                    "export: fetchDecodedSpans failed for asset=\(asset.id, privacy: .public) error=\(String(describing: error), privacy: .public) — emitting 0 decisions for this asset"
                )
                spans = []
            }
            for span in spans {
                let spanData = try spanLine(span)
                try write(line: spanData, to: handle, hasher: &hasher)
                spanCount += 1
            }

            // playhead-epfk: ad_window records for this asset. Carries
            // the `catalogStoreMatchSimilarity` column NARL eval needs to
            // measure the AdCatalogStore correction loop. Lookup failures
            // are non-fatal (parity with fetchDecodedSpans above): one
            // bad asset must not abort the entire export. Empty arrays
            // simply emit zero ad_window lines for that asset, which
            // downstream tooling treats as "no fusion windows persisted."
            let adWindows: [AdWindow]
            do {
                adWindows = try await store.fetchAdWindows(assetId: asset.id)
            } catch {
                logger.warning(
                    "export: fetchAdWindows failed for asset=\(asset.id, privacy: .public) error=\(String(describing: error), privacy: .public) — emitting 0 ad_window rows for this asset"
                )
                adWindows = []
            }
            for window in adWindows {
                let windowData = try adWindowLine(window)
                try write(line: windowData, to: handle, hasher: &hasher)
                adWindowCount += 1
            }

            // correction records for this asset
            let events: [CorrectionEvent]
            do {
                events = try await store.loadCorrectionEvents(analysisAssetId: asset.id)
            } catch {
                logger.warning(
                    "export: loadCorrectionEvents failed for asset=\(asset.id, privacy: .public) error=\(String(describing: error), privacy: .public) — emitting 0 corrections for this asset"
                )
                events = []
            }
            for event in events {
                guard let data = try correctionLine(event) else {
                    skippedCorrectionCount += 1
                    // Scope strings can contain user-authored substrings (e.g.
                    // correction text) — keep at .private so a corrupt value
                    // doesn't leak to a public log reader.
                    logger.warning(
                        "export: skipping correction row id=\(event.id, privacy: .public) asset=\(event.analysisAssetId, privacy: .public) — unparseable scope \(event.scope, privacy: .private)"
                    )
                    continue
                }
                try write(line: data, to: handle, hasher: &hasher)
                correctionCount += 1
            }
        }

        // playhead-vnni: finalize the streamed-bytes digest and consult the
        // dedup memo. If a same-content export landed in the same sink
        // within `dedupWindow`, remove the just-written file and redirect
        // the result's `fileURL` at the prior file. Close the handle now so
        // the file's bytes are flushed before the FileManager.removeItem
        // call. The `defer` block above will additionally try to close the
        // handle on exit; closing twice is a harmless no-op via `try?`.
        try? handle.close()
        let digestHex = hasher.finalize().map { String(format: "%02x", $0) }.joined()
        let resolvedFileURL: URL
        if let priorURL = await dedupMemo.recentMatch(
            documentsURL: documentsURL,
            digestHex: digestHex,
            window: dedupWindow,
            now: now
        ) {
            // Suppress the duplicate. The defer would otherwise leave it on
            // disk because `didSucceed = true` skips the cleanup branch.
            try? fm.removeItem(at: fileURL)
            logger.info(
                "export: dedup-suppressed identical-content corpus export at \(documentsURL.path, privacy: .public) — reusing \(priorURL.lastPathComponent, privacy: .public)"
            )
            resolvedFileURL = priorURL
        } else {
            await dedupMemo.record(
                documentsURL: documentsURL,
                digestHex: digestHex,
                fileURL: fileURL,
                now: now
            )
            resolvedFileURL = fileURL
        }

        // Optional narL pairing: if decision-log.jsonl exists in the same
        // Documents directory, surface it so the caller can drag both files
        // out in a single Finder operation. We do NOT copy or concatenate —
        // two files, one timestamped corpus bundle.
        let siblingDecisionLog = documentsURL.appendingPathComponent("decision-log.jsonl")
        let decisionLogManifestURL = fm.fileExists(atPath: siblingDecisionLog.path)
            ? siblingDecisionLog
            : nil

        // playhead-narl.2 shadow sidecar: always write
        // `shadow-decisions.jsonl` alongside the corpus bundle so the
        // harness's corpus builder can consume both files from the same
        // Files.app pull. A shadow-export failure is logged but does NOT
        // abort the corpus export — the core corpus file has already been
        // written at this point, and losing the shadow sidecar for a
        // single debug export is strictly better than losing the whole
        // bundle.
        var shadowManifestURL: URL? = nil
        var shadowRowCount: Int = 0
        do {
            let shadow = try await ShadowDecisionsExporter.export(
                source: store,
                documentsURL: documentsURL
            )
            shadowManifestURL = shadow.fileURL
            shadowRowCount = shadow.rowCount
        } catch {
            logger.warning(
                "export: shadow sidecar write failed error=\(String(describing: error), privacy: .public) — corpus file still valid"
            )
        }

        didSucceed = true
        return CorpusExportResult(
            fileURL: resolvedFileURL,
            assetCount: assetCount,
            spanCount: spanCount,
            correctionCount: correctionCount,
            skippedCorrectionCount: skippedCorrectionCount,
            adWindowCount: adWindowCount,
            decisionLogManifestURL: decisionLogManifestURL,
            shadowManifestURL: shadowManifestURL,
            shadowRowCount: shadowRowCount
        )
    }

    // MARK: - Private helpers

    /// Encode a dictionary as compact JSON. playhead-vnni: `.sortedKeys`
    /// makes the byte output deterministic for a given input — Swift's
    /// `[String: Any]` literals do not preserve insertion order and use a
    /// per-instance randomized hash seed, so JSONSerialization without
    /// `.sortedKeys` produces non-deterministic byte sequences across calls
    /// in the same process. Determinism is required for the streamed-bytes
    /// digest the dedup memo compares.
    private static func jsonLineData(from obj: [String: Any]) throws -> Data {
        return try JSONSerialization.data(
            withJSONObject: obj,
            options: [.withoutEscapingSlashes, .sortedKeys]
        )
    }

    /// Write a single JSONL line (record body + LF) to the handle. The same
    /// bytes are folded into `hasher` so the caller can fingerprint the
    /// stream without re-reading the file (playhead-vnni dedup).
    private static func write(line data: Data, to handle: FileHandle, hasher: inout SHA256) throws {
        try handle.write(contentsOf: data)
        hasher.update(data: data)
        let lf = Data([0x0A])  // '\n'
        try handle.write(contentsOf: lf)
        hasher.update(data: lf)
    }
}

#endif
