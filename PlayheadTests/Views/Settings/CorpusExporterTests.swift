// CorpusExporterTests.swift
// Tests for narE (playhead-dgzw): debug-only corpus export.
//
// CorpusExporter reads CorrectionEvent and DecodedSpan rows from
// analysis.sqlite and writes a JSONL corpus file to Documents/.
// Tests cover per-record serialization, join logic, empty-store,
// corrupt-scope handling, file naming, and streaming-to-disk.

#if DEBUG

import Foundation
import Testing
@testable import Playhead

@Suite("CorpusExporter — narE")
struct CorpusExporterTests {

    // MARK: - Filename

    @Test("filename(for:) uses ISO-8601 with millisecond fractional seconds, filesystem-safe colons replaced")
    func filenameFormat() {
        // 2026-04-21T15:30:45.000Z → "corpus-export.2026-04-21T15-30-45.000Z.jsonl"
        // (colons replaced with dashes; millisecond fraction preserved so
        // two exports in the same second land in distinct files).
        var comps = DateComponents()
        comps.year = 2026
        comps.month = 4
        comps.day = 21
        comps.hour = 15
        comps.minute = 30
        comps.second = 45
        comps.timeZone = TimeZone(identifier: "UTC")
        let date = Calendar(identifier: .iso8601).date(from: comps)!

        let name = CorpusExporter.filename(for: date)
        #expect(name == "corpus-export.2026-04-21T15-30-45.000Z.jsonl",
                "got \(name)")
        // No colon allowed in filename (Files app / Finder hostility).
        #expect(!name.contains(":"))
    }

    @Test("filename(for:) resolves dates in the same UTC second to distinct filenames via milliseconds")
    func filenameMillisecondDisambiguation() {
        // Two exports 500ms apart within the same UTC second must produce
        // different filenames, otherwise M1 (second-clobber) returns.
        let whole = Date(timeIntervalSince1970: 1_700_000_000)
        let half = whole.addingTimeInterval(0.5)
        let a = CorpusExporter.filename(for: whole)
        let b = CorpusExporter.filename(for: half)
        #expect(a != b, "same-second exports collapsed: \(a) == \(b)")
        #expect(a.contains(".000Z."))
        #expect(b.contains(".500Z."))
    }

    // MARK: - schemaVersion constant

    @Test("schemaVersion is 1 and every emitted record carries it")
    func schemaVersionConstant() throws {
        #expect(CorpusExporter.schemaVersion == 1)
        // Each record serializer stamps schemaVersion: 1.
        let asset = makeTestAsset(id: "asset-X")
        let assetJSON = try decodeJSONObject(from: CorpusExporter.assetLine(asset))
        #expect(assetJSON["schemaVersion"] as? Int == 1)
        #expect(assetJSON["type"] as? String == "asset")

        let span = makeSpan(assetId: "asset-X")
        let spanJSON = try decodeJSONObject(from: CorpusExporter.spanLine(span))
        #expect(spanJSON["schemaVersion"] as? Int == 1)
        #expect(spanJSON["type"] as? String == "decision")

        let scope = CorrectionScope.exactSpan(assetId: "asset-X", ordinalRange: 10...20)
        let event = CorrectionEvent(
            analysisAssetId: "asset-X",
            scope: scope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        guard let correctionData = try CorpusExporter.correctionLine(event) else {
            Issue.record("correctionLine returned nil for a valid event")
            return
        }
        let correctionJSON = try decodeJSONObject(from: correctionData)
        #expect(correctionJSON["schemaVersion"] as? Int == 1)
        #expect(correctionJSON["type"] as? String == "correction")
    }

    @Test("additive catalog provenance remains decodable by a legacy v1 reader")
    func catalogProvenanceIsBackwardCompatibleV1Extension() throws {
        struct LegacyV1AdWindow: Decodable {
            let type: String
            let schemaVersion: Int
            let id: String
            let analysisAssetId: String
            let startTime: Double
            let endTime: Double
            let confidence: Double
        }
        let window = AdWindow(
            id: "legacy-reader-window",
            analysisAssetId: "legacy-reader-asset",
            startTime: 10,
            endTime: 40,
            confidence: 0.95,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "test",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false,
            catalogStoreMatchSimilarity: 0.93,
            catalogFingerprintVersion:
                CatalogFingerprintVersion.currentCatalog.rawValue,
            catalogMatchedEntryId:
                "00000000-0000-0000-0000-000000000001",
            catalogMatchedShowId: "show-legacy-reader",
            catalogMatchedLearningSource:
                CatalogLearningSource.userMarkedAd.rawValue,
            catalogMatchedLearningLifecycle:
                CatalogLearningLifecycle.explicitConfirmation.rawValue
        )

        let decoded = try JSONDecoder().decode(
            LegacyV1AdWindow.self,
            from: CorpusExporter.adWindowLine(window)
        )

        #expect(decoded.type == "ad_window")
        #expect(decoded.schemaVersion == 1)
        #expect(decoded.id == window.id)
        #expect(decoded.analysisAssetId == window.analysisAssetId)
        #expect(decoded.startTime == window.startTime)
        #expect(decoded.endTime == window.endTime)
        #expect(decoded.confidence == window.confidence)

        let json = try decodeJSONObject(
            from: CorpusExporter.adWindowLine(window)
        )
        #expect(
            json["catalogFingerprintVersion"] as? Int
                == CatalogFingerprintVersion.currentCatalog.rawValue
        )
        #expect(json["catalogMatchedEntryId"] is NSNull)
        let encodedLine = try #require(
            String(
                data: CorpusExporter.adWindowLine(window),
                encoding: .utf8
            )
        )
        #expect(
            !encodedLine.contains(try #require(window.catalogMatchedEntryId)),
            "a private catalog-row UUID must not cross the corpus boundary"
        )
        // playhead-ar60 R1 review: the named residual gap. A one-number
        // producer emits `null`, which is what says "there is no separate
        // actuation number" rather than "it happened to equal `confidence`".
        #expect(json["skipConfidence"] is NSNull)
        #expect(
            json["catalogMatchedShowId"] as? String
                == window.catalogMatchedShowId
        )
        #expect(
            json["catalogMatchedLearningSource"] as? String
                == window.catalogMatchedLearningSource
        )
        #expect(
            json["catalogMatchedLearningLifecycle"] as? String
                == window.catalogMatchedLearningLifecycle
        )

        // Catalog payloads are deliberately device-local. The corpus receives
        // match audit metadata only—not a replayable fingerprint or lexical
        // material copied from AdCatalogStore.
        for forbiddenKey in [
            "fingerprintBlob",
            "acousticFingerprint",
            "transcriptSnippet",
            "sponsorTokens",
        ] {
            #expect(!json.keys.contains(forbiddenKey))
        }
    }

    @Test("ad_window export carries the ACTUATION number in its own key (playhead-ar60)")
    func adWindowExportCarriesSkipConfidence() throws {
        let fusion = AdWindow(
            id: "ar60-export-window",
            analysisAssetId: "ar60-export-asset",
            startTime: 2828.4,
            endTime: 2836.44,
            confidence: 0.456,
            skipConfidence: 0.00115,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "test",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: "fusion-v1",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
        let json = try decodeJSONObject(
            from: CorpusExporter.adWindowLine(fusion)
        )
        // Both, and distinguishable — the whole point of the V47 split is that
        // a corpus reader can no longer mistake one for the other.
        #expect(json["confidence"] as? Double == 0.456)
        #expect(json["skipConfidence"] as? Double == 0.00115)
    }

    @Test("feedback-targeted ad-window export redacts only response-derived state")
    func adWindowExportRedactsFeedbackState() throws {
        let window = AdWindow(
            id: "window-denied",
            analysisAssetId: "asset-X",
            startTime: 10,
            endTime: 40,
            confidence: 0.8,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.reverted.rawValue,
            detectorVersion: "test",
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: nil,
            evidenceStartTime: 10,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: true
        )

        let json = try decodeJSONObject(
            from: CorpusExporter.adWindowLine(
                window,
                redactingExplicitBannerFeedback: true
            )
        )

        #expect(
            !json.keys.contains("userDismissedBanner"),
            "Diagnostics must not expose a per-window Yes/No response"
        )
        #expect(json["boundaryState"] is NSNull)
        #expect(json["decisionState"] is NSNull)
        #expect(json["wasSkipped"] is NSNull)
        #expect(json["id"] as? String == window.id)
        #expect(json["startTime"] as? Double == window.startTime)
        #expect(json["endTime"] as? Double == window.endTime)
        #expect(json["confidence"] as? Double == window.confidence)
        #expect(
            json["metadataSource"] as? String == window.metadataSource
        )
    }

    // MARK: - Asset record

    @Test("asset record carries analysisAssetId, episodeId, sourceURL; missing optional metadata serialized as null")
    func assetRecordShape() throws {
        let asset = makeTestAsset(id: "asset-A")
        let data = try CorpusExporter.assetLine(asset)
        let json = try decodeJSONObject(from: data)
        #expect(json["type"] as? String == "asset")
        #expect(json["analysisAssetId"] as? String == "asset-A")
        #expect(json["episodeId"] as? String == "ep-asset-A")
        #expect(json["sourceURL"] as? String == "file:///tmp/asset-A.m4a")
        #expect(json["analysisState"] as? String == "new")
        // Every nullable asset field must be present as null, not missing — downstream
        // tooling needs the key set stable across records so it can coerce columns.
        for key in [
            "weakFingerprint",
            "podcastId",
            "episodeDurationSec",
            "featureCoverageEndTime",
            "fastTranscriptCoverageEndTime",
            "confirmedAdCoverageEndTime",
            // playhead-gtt9.8: `terminalReason` is the richer-terminal
            // diagnostic the classifier persisted into
            // `analysis_assets.terminalReason`. Nullable on pre-gtt9.8
            // rows and on sessions still in flight.
            "terminalReason",
            // playhead-i9dj: human-readable identifiers so the exported
            // corpus is legible standalone. Nullable on rows that
            // pre-date the i9dj migration and on rows whose first
            // observation hasn't yet supplied the title.
            "podcastTitle",
            "episodeTitle",
        ] {
            #expect(json.keys.contains(key), "\(key) must be present as a key")
            #expect(json[key] is NSNull, "\(key) must serialize as null for a minimal asset, not omitted or empty-string")
        }
    }

    @Test("listen_rewind record carries assetId, windowId, podcastId, time, createdAt — playhead-q45f.1")
    func listenRewindRecordShape() throws {
        let row = AdListenRewindRow(
            analysisAssetId: "asset-Z",
            windowId: "win-Z",
            podcastId: "pod-Z",
            time: 87.5,
            createdAt: Date(timeIntervalSince1970: 1_700_000_000)
        )
        let data = try CorpusExporter.listenRewindLine(row)
        let json = try decodeJSONObject(from: data)
        #expect(json["type"] as? String == "listen_rewind")
        #expect(json["schemaVersion"] as? Int == 1)
        #expect(json["analysisAssetId"] as? String == "asset-Z")
        #expect(json["windowId"] as? String == "win-Z")
        #expect(json["podcastId"] as? String == "pod-Z")
        #expect(json["time"] as? Double == 87.5)
        #expect(json["createdAt"] as? Double == 1_700_000_000.0)
    }

    @Test("export emits one listen_rewind line per persisted row, with non-zero count — playhead-q45f.1")
    func exportEmitsListenRewindLines() async throws {
        let docs = try makeTempDir(prefix: "CorpusExport-listen-rewind")
        let asset = makeTestAsset(id: "asset-q45f-1")
        let source = ListenRewindSource(
            assets: [asset],
            listenRewinds: [
                "asset-q45f-1": [
                    AdListenRewindRow(analysisAssetId: "asset-q45f-1", windowId: "win-A", podcastId: "pod-1", time: 60, createdAt: Date(timeIntervalSince1970: 1_700_000_100)),
                    AdListenRewindRow(analysisAssetId: "asset-q45f-1", windowId: "win-A", podcastId: "pod-1", time: 60, createdAt: Date(timeIntervalSince1970: 1_700_000_200)),
                    AdListenRewindRow(analysisAssetId: "asset-q45f-1", windowId: "win-B", podcastId: "pod-1", time: 120, createdAt: Date(timeIntervalSince1970: 1_700_000_300)),
                ]
            ]
        )

        let result = try await CorpusExporter.export(store: source, documentsURL: docs)
        #expect(result.listenRewindCount == 3)

        let records = try parseJSONL(at: result.fileURL)
        let rewinds = records.filter { ($0["type"] as? String) == "listen_rewind" }
        #expect(rewinds.count == 3)
        #expect(rewinds.allSatisfy { ($0["analysisAssetId"] as? String) == "asset-q45f-1" })
        let windowIds = Set(rewinds.compactMap { $0["windowId"] as? String })
        #expect(windowIds == ["win-A", "win-B"])
    }

    @Test("asset record carries podcastId when threaded through from the store (HIGH-3)")
    func assetRecordPodcastIdPassthrough() throws {
        let asset = makeTestAsset(id: "asset-H3")
        let data = try CorpusExporter.assetLine(asset, podcastId: "pod-abc-123")
        let json = try decodeJSONObject(from: data)
        #expect(json["podcastId"] as? String == "pod-abc-123")
    }

    @Test("asset record carries detectorVersion + buildCommitSHA capture provenance (gtt9.21)")
    func assetRecordCaptureProvenancePassthrough() throws {
        let asset = makeTestAsset(id: "asset-prov")
        let data = try CorpusExporter.assetLine(
            asset,
            podcastId: nil,
            detectorVersion: "detection-v9",
            buildCommitSHA: "abc1234"
        )
        let json = try decodeJSONObject(from: data)
        #expect(json["detectorVersion"] as? String == "detection-v9",
                "detectorVersion must be persisted on each asset row so the harness can attribute fixtures to a specific detector build")
        #expect(json["buildCommitSHA"] as? String == "abc1234",
                "buildCommitSHA must be persisted on each asset row so the harness can attribute fixtures to a specific binary")
    }

    @Test("asset record default-stamps BuildInfo.commitSHA when caller omits it (gtt9.21)")
    func assetRecordDefaultsToBuildInfoCommitSHA() throws {
        let asset = makeTestAsset(id: "asset-prov-default")
        // No detectorVersion / buildCommitSHA passed → exporter should
        // stamp the runtime defaults so live device captures never emit a
        // missing-provenance asset row by accident.
        let data = try CorpusExporter.assetLine(asset)
        let json = try decodeJSONObject(from: data)
        let stampedSHA = json["buildCommitSHA"] as? String
        #expect(stampedSHA == BuildInfo.commitSHA,
                "When the caller does not pass buildCommitSHA, exporter must stamp BuildInfo.commitSHA")
        let stampedVersion = json["detectorVersion"] as? String
        #expect(stampedVersion == AdDetectionConfig.default.detectorVersion,
                "When the caller does not pass detectorVersion, exporter must stamp AdDetectionConfig.default.detectorVersion")
    }

    @Test("asset record carries podcastTitle + episodeTitle when threaded through (i9dj)")
    func assetRecordTitlePassthrough() throws {
        // playhead-i9dj: `episodeTitle` lives on the AnalysisAsset row;
        // `podcastTitle` is supplied by the caller (looked up by
        // CorpusExporter via `fetchProfile(podcastId:)?.title` on the
        // export path). Both must serialize as their string values
        // when present, and as explicit JSON null when absent.
        let asset = AnalysisAsset(
            id: "asset-i9dj",
            episodeId: "ep-i9dj",
            assetFingerprint: "fp-i9dj",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/i9dj.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "queued",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeTitle: "How to escape burnout"
        )
        let data = try CorpusExporter.assetLine(
            asset,
            podcastId: "pod-i9dj",
            podcastTitle: "Diary of a CEO"
        )
        let json = try decodeJSONObject(from: data)
        #expect(json["episodeTitle"] as? String == "How to escape burnout")
        #expect(json["podcastTitle"] as? String == "Diary of a CEO")
    }

    @Test("asset record carries terminalReason when the classifier set one (gtt9.8)")
    func assetRecordTerminalReasonPassthrough() throws {
        let asset = AnalysisAsset(
            id: "asset-term",
            episodeId: "ep-term",
            assetFingerprint: "fp-term",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/term.m4a",
            featureCoverageEndTime: 3575.0,
            fastTranscriptCoverageEndTime: 3540.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "completeFull",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            terminalReason: "full coverage: transcript 0.981, feature 0.992"
        )
        let json = try decodeJSONObject(from: CorpusExporter.assetLine(asset))
        #expect(json["terminalReason"] as? String
                == "full coverage: transcript 0.981, feature 0.992")
        #expect(json["analysisState"] as? String == "completeFull")
    }

    @Test("asset record carries episodeDurationSec for NARL lifecycle fallback")
    func assetRecordEpisodeDurationPassthrough() throws {
        let asset = AnalysisAsset(
            id: "asset-duration",
            episodeId: "ep-duration",
            assetFingerprint: "fp-duration",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/duration.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "completeFull",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 3576.5
        )
        let json = try decodeJSONObject(from: CorpusExporter.assetLine(asset))
        #expect(json["episodeDurationSec"] as? Double == 3576.5)
    }

    @Test("export writes persisted episodeDurationSec on asset rows for NARL lifecycle fallback")
    func exportWritesEpisodeDurationOnAssetRows() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-duration")
        let asset = AnalysisAsset(
            id: "asset-export-duration",
            episodeId: "ep-export-duration",
            assetFingerprint: "fp-export-duration",
            weakFingerprint: nil,
            sourceURL: "file:///tmp/export-duration.m4a",
            featureCoverageEndTime: 3575.0,
            fastTranscriptCoverageEndTime: 3540.0,
            confirmedAdCoverageEndTime: nil,
            analysisState: "completeFull",
            analysisVersion: 1,
            capabilitySnapshot: nil,
            episodeDurationSec: 3576.5,
            terminalReason: "full coverage: transcript 0.990, feature 0.999"
        )
        try await store.insertAsset(asset)
        try await store.updateAssetState(
            id: asset.id,
            state: asset.analysisState,
            terminalReason: asset.terminalReason
        )

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)

        let records = try parseJSONL(at: result.fileURL)
        let assetJSON = try #require(records.first { ($0["type"] as? String) == "asset" })
        #expect(assetJSON["episodeDurationSec"] as? Double == 3576.5)
        #expect(assetJSON["analysisState"] as? String == "completeFull")
        #expect(assetJSON["terminalReason"] as? String == "full coverage: transcript 0.990, feature 0.999")
        #expect(assetJSON["fastTranscriptCoverageEndTime"] as? Double == 3540.0)
        #expect(assetJSON["featureCoverageEndTime"] as? Double == 3575.0)
    }

    // MARK: - Decision record (DecodedSpan)

    @Test("decision record carries assetId, atom ordinal range, start/end, anchorProvenance")
    func decisionRecordShape() throws {
        let span = makeSpan(assetId: "asset-A", startTime: 120.5, endTime: 180.25,
                            firstOrdinal: 42, lastOrdinal: 67)
        let data = try CorpusExporter.spanLine(span)
        let json = try decodeJSONObject(from: data)
        #expect(json["type"] as? String == "decision")
        #expect(json["analysisAssetId"] as? String == "asset-A")
        #expect(json["spanId"] as? String == span.id)
        #expect(json["firstAtomOrdinal"] as? Int == 42)
        #expect(json["lastAtomOrdinal"] as? Int == 67)
        #expect(json["startTime"] as? Double == 120.5)
        #expect(json["endTime"] as? Double == 180.25)
        // anchorProvenance is always an array (possibly empty).
        #expect(json["anchorProvenance"] is [Any])
    }

    // MARK: - Correction record

    @Test("correction record carries scope, source, correctionType, causalSource, targetRefs, createdAt, analysisAssetId; missing podcastId serialized as null")
    func correctionRecordShape() throws {
        let scope = CorrectionScope.exactSpan(assetId: "asset-A", ordinalRange: 5...12)
        let event = CorrectionEvent(
            id: "corr-1",
            analysisAssetId: "asset-A",
            scope: scope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto,
            podcastId: nil,
            correctionType: .falsePositive,
            causalSource: .foundationModel,
            targetRefs: CorrectionTargetRefs(sponsorEntity: "Squarespace")
        )
        guard let data = try CorpusExporter.correctionLine(event) else {
            Issue.record("correctionLine returned nil for a well-formed event")
            return
        }
        let json = try decodeJSONObject(from: data)
        #expect(json["type"] as? String == "correction")
        #expect(json["id"] as? String == "corr-1")
        #expect(json["analysisAssetId"] as? String == "asset-A")
        #expect(json["scope"] as? String == scope.serialized)
        #expect(json["createdAt"] as? Double == 1_700_000_000)
        #expect(json["source"] as? String == "manualVeto")
        #expect(json["correctionType"] as? String == "falsePositive")
        #expect(json["causalSource"] as? String == "foundationModel")
        // podcastId must be present as null, not omitted.
        #expect(json.keys.contains("podcastId"))
        #expect(json["podcastId"] is NSNull)
        // targetRefs survives as a nested object.
        let targetRefs = json["targetRefs"] as? [String: Any]
        #expect(targetRefs?["sponsorEntity"] as? String == "Squarespace")
    }

    @Test("correction record tolerates missing optional fields — only analysisAssetId + scope are required")
    func correctionMinimalRecord() throws {
        let event = CorrectionEvent(
            id: "corr-2",
            analysisAssetId: "asset-B",
            scope: CorrectionScope.exactSpan(assetId: "asset-B", ordinalRange: 1...1).serialized,
            createdAt: 0,
            source: nil,
            podcastId: nil,
            correctionType: nil,
            causalSource: nil,
            targetRefs: nil
        )
        guard let data = try CorpusExporter.correctionLine(event) else {
            Issue.record("correctionLine returned nil for a minimal-but-valid event")
            return
        }
        let json = try decodeJSONObject(from: data)
        // All optionals null, not omitted.
        for key in ["source", "podcastId", "correctionType", "causalSource", "targetRefs"] {
            #expect(json.keys.contains(key), "\(key) must be present")
            #expect(json[key] is NSNull, "\(key) must be null for a minimal record")
        }
    }

    @Test("all explicit banner receipt serializers fail closed")
    func explicitBannerReceiptLinesAreWithheld() throws {
        let sources: [CorrectionSource] = [
            .bannerAutoSkipConfirmed,
            .bannerAutoSkipDenied,
            .bannerSuggestionConfirmed,
            .bannerSuggestionDenied,
        ]
        for (index, source) in sources.enumerated() {
            let event = CorrectionEvent(
                id: "private-receipt-\(index)",
                analysisAssetId: "asset-private",
                scope: CorrectionScope.exactTimeSpan(
                    assetId: "asset-private",
                    startTime: Double(index * 40),
                    endTime: Double(index * 40 + 30)
                ).serialized,
                createdAt: 1_700_000_000 + Double(index),
                source: source,
                correctionType: source.kind.correctionType,
                targetRefs: CorrectionTargetRefs(
                    adWindowId: "window-\(index)"
                )
            )
            #expect(
                try CorpusExporter.correctionLine(event) == nil,
                "Explicit \(source.rawValue) receipt must never serialize"
            )
        }
    }

    // MARK: - Corrupt scope handling

    @Test("correctionLine returns nil for an unparseable scope string — caller logs and skips")
    func corruptScopeSkipped() throws {
        let event = CorrectionEvent(
            id: "corr-bad",
            analysisAssetId: "asset-A",
            scope: "!!!garbage_not_a_valid_scope",
            createdAt: 0,
            source: .manualVeto
        )
        let data = try CorpusExporter.correctionLine(event)
        #expect(data == nil, "Unparseable scope must make correctionLine return nil so the caller skips the row")
    }

    // MARK: - Join logic + end-to-end export against AnalysisStore

    @Test("export against empty store produces an empty file — valid (zero records), not a crash")
    func exportEmptyStore() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-empty")

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.assetCount == 0)
        #expect(result.spanCount == 0)
        #expect(result.correctionCount == 0)
        #expect(result.skippedCorrectionCount == 0)
        #expect(FileManager.default.fileExists(atPath: result.fileURL.path))

        let contents = try String(contentsOf: result.fileURL, encoding: .utf8)
        // Empty file or only trailing newline is acceptable.
        #expect(contents.isEmpty || contents == "\n")
    }

    @Test("export writes one line per asset, plus one line per DecodedSpan, plus one line per CorrectionEvent, each with the expected type discriminator")
    func exportJoinsAllThreeTypes() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-join")

        // Seed two assets, one span on each, one correction on the first.
        let a1 = makeTestAsset(id: "asset-1")
        let a2 = makeTestAsset(id: "asset-2")
        try await store.insertAsset(a1)
        try await store.insertAsset(a2)

        let span1 = makeSpan(assetId: "asset-1", firstOrdinal: 10, lastOrdinal: 20)
        let span2 = makeSpan(assetId: "asset-2", firstOrdinal: 30, lastOrdinal: 40)
        try await store.upsertDecodedSpans([span1, span2])

        let scope = CorrectionScope.exactSpan(assetId: "asset-1", ordinalRange: 10...20)
        let event = CorrectionEvent(
            analysisAssetId: "asset-1",
            scope: scope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto,
            correctionType: .falsePositive
        )
        let correctionStore = PersistentUserCorrectionStore(store: store)
        try await correctionStore.record(event)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.assetCount == 2)
        #expect(result.spanCount == 2)
        #expect(result.correctionCount == 1)
        #expect(result.skippedCorrectionCount == 0)

        let records = try parseJSONL(at: result.fileURL)
        #expect(records.count == 2 + 2 + 1,
                "expected 2 asset + 2 decision + 1 correction = 5 records, got \(records.count)")
        let typeCounts = Dictionary(grouping: records) { $0["type"] as? String ?? "?" }
            .mapValues { $0.count }
        #expect(typeCounts["asset"] == 2)
        #expect(typeCounts["decision"] == 2)
        #expect(typeCounts["correction"] == 1)
    }

    @Test("export withholds all four explicit routes while preserving unrelated diagnostics")
    func exportRedactsExplicitBannerFeedbackNarrowly() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-private-feedback")
        let assetId = "asset-private-feedback"
        try await store.insertAsset(makeTestAsset(id: assetId))

        func makeWindow(
            id: String,
            start: Double,
            boundaryState: String,
            decisionState: String,
            wasSkipped: Bool,
            dismissed: Bool = false
        ) -> AdWindow {
            AdWindow(
                id: id,
                analysisAssetId: assetId,
                startTime: start,
                endTime: start + 30,
                confidence: 0.81,
                boundaryState: boundaryState,
                decisionState: decisionState,
                detectorVersion: "privacy-test",
                advertiser: "Unrelated diagnostic sponsor",
                product: "Unrelated product",
                adDescription: "Unrelated detector description",
                evidenceText: "Unrelated detector evidence",
                evidenceStartTime: start,
                metadataSource: "privacy-test-source",
                metadataConfidence: 0.72,
                metadataPromptVersion: "privacy-test-prompt",
                wasSkipped: wasSkipped,
                userDismissedBanner: dismissed
            )
        }

        let routeWindows = [
            makeWindow(
                id: "window-auto-yes",
                start: 10,
                boundaryState: "lexical",
                decisionState: AdDecisionState.applied.rawValue,
                wasSkipped: true
            ),
            makeWindow(
                id: "window-auto-no",
                start: 50,
                boundaryState: "lexical",
                decisionState: AdDecisionState.reverted.rawValue,
                wasSkipped: true
            ),
            makeWindow(
                id: "window-suggest-yes",
                start: 90,
                boundaryState: "userConfirmedSuggested",
                decisionState: AdDecisionState.applied.rawValue,
                wasSkipped: true
            ),
            makeWindow(
                id: "window-suggest-no",
                start: 130,
                boundaryState: AdBoundaryState.segmentAggregated.rawValue,
                decisionState: AdDecisionState.reverted.rawValue,
                wasSkipped: false,
                dismissed: true
            ),
        ]
        let unrelated = makeWindow(
            id: "window-unrelated",
            start: 170,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            wasSkipped: false
        )
        let suggestYesOriginal = makeWindow(
            id: "window-suggest-yes-original",
            start: 90,
            boundaryState: "suggestion-producer",
            decisionState: AdDecisionState.suppressed.rawValue,
            wasSkipped: false
        )
        let sources: [CorrectionSource] = [
            .bannerAutoSkipConfirmed,
            .bannerAutoSkipDenied,
            .bannerSuggestionConfirmed,
            .bannerSuggestionDenied,
        ]
        let unansweredRouteWindows = sources.enumerated().map {
            index,
            source in
            let responseWindow = routeWindows[index]
            let producer =
                source == .bannerSuggestionConfirmed
                ? suggestYesOriginal
                : responseWindow
            return AdWindow(
                id: producer.id,
                analysisAssetId: producer.analysisAssetId,
                startTime: producer.startTime,
                endTime: producer.endTime,
                confidence: producer.confidence,
                boundaryState: producer.boundaryState,
                decisionState:
                    source == .bannerAutoSkipConfirmed
                    || source == .bannerAutoSkipDenied
                    ? AdDecisionState.confirmed.rawValue
                    : AdDecisionState.candidate.rawValue,
                detectorVersion: producer.detectorVersion,
                advertiser: producer.advertiser,
                product: producer.product,
                adDescription: producer.adDescription,
                evidenceText: producer.evidenceText,
                evidenceStartTime: producer.evidenceStartTime,
                metadataSource: producer.metadataSource,
                metadataConfidence: producer.metadataConfidence,
                metadataPromptVersion: producer.metadataPromptVersion,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: producer.evidenceSources,
                eligibilityGate: producer.eligibilityGate,
                catalogStoreMatchSimilarity:
                    producer.catalogStoreMatchSimilarity,
                startEdgeAnchor: producer.startEdgeAnchor,
                endEdgeAnchor: producer.endEdgeAnchor
            )
        }
        try await store.seedExplicitFeedbackEgressBaselineForTesting(
            analysisAssetId: assetId,
            unansweredWindows: unansweredRouteWindows + [unrelated]
        )
        for window in routeWindows + [suggestYesOriginal, unrelated] {
            try await store.insertAdWindow(window)
        }

        var expectedProjectionByID: [String: AdWindow] = [:]
        for (index, source) in sources.enumerated() {
            let window = routeWindows[index]
            let producer =
                source == .bannerSuggestionConfirmed
                ? suggestYesOriginal
                : window
            let preResponse = AdWindow(
                id: producer.id,
                analysisAssetId: producer.analysisAssetId,
                startTime: producer.startTime,
                endTime: producer.endTime,
                confidence: producer.confidence,
                boundaryState: producer.boundaryState,
                decisionState:
                    source == .bannerAutoSkipConfirmed
                    || source == .bannerAutoSkipDenied
                    ? AdDecisionState.confirmed.rawValue
                    : AdDecisionState.candidate.rawValue,
                detectorVersion: producer.detectorVersion,
                advertiser: producer.advertiser,
                product: producer.product,
                adDescription: producer.adDescription,
                evidenceText: producer.evidenceText,
                evidenceStartTime: producer.evidenceStartTime,
                metadataSource: producer.metadataSource,
                metadataConfidence: producer.metadataConfidence,
                metadataPromptVersion: window.metadataPromptVersion,
                wasSkipped: false,
                userDismissedBanner: false,
                evidenceSources: producer.evidenceSources,
                eligibilityGate: producer.eligibilityGate,
                catalogStoreMatchSimilarity:
                    producer.catalogStoreMatchSimilarity,
                startEdgeAnchor: producer.startEdgeAnchor,
                endEdgeAnchor: producer.endEdgeAnchor
            )
            expectedProjectionByID[preResponse.id] = preResponse
            let targetIDs =
                source == .bannerSuggestionConfirmed
                ? [suggestYesOriginal.id, window.id]
                : [window.id]
            try await store.appendCorrectionEvent(
                CorrectionEvent(
                    id: "private-route-receipt-\(index)",
                    analysisAssetId: assetId,
                    scope: CorrectionScope.exactTimeSpan(
                        assetId: assetId,
                        startTime: window.startTime,
                        endTime: window.endTime
                    ).serialized,
                    createdAt: 1_700_000_100 + Double(index),
                    source: source,
                    podcastId: "private-podcast",
                    correctionType: source.kind.correctionType,
                    causalSource: .specialist,
                    targetRefs: CorrectionTargetRefs(
                        adWindowId: window.id,
                        adWindowIds: targetIDs,
                        explicitFeedbackDetectionProjection:
                            ExplicitFeedbackDetectionProjection(preResponse),
                        evidenceRefs: ["private-evidence-\(index)"],
                        sponsorEntity: "private-target-\(index)"
                    )
                )
            )
        }

        let result = try await CorpusExporter.export(
            store: store,
            documentsURL: docs
        )
        #expect(result.correctionCount == 0)
        #expect(result.skippedCorrectionCount == 0)
        #expect(result.adWindowCount == 5)
        let records = try parseJSONL(at: result.fileURL)
        #expect(
            !records.contains { $0["type"] as? String == "correction" }
        )

        let windowRecords = records.filter {
            $0["type"] as? String == "ad_window"
        }
        for preResponse in expectedProjectionByID.values {
            let record = try #require(
                windowRecords.first {
                    $0["id"] as? String == preResponse.id
                }
            )
            #expect(
                record["boundaryState"] as? String
                    == preResponse.boundaryState
            )
            #expect(
                record["decisionState"] as? String
                    == preResponse.decisionState
            )
            #expect(record["wasSkipped"] as? Bool == false)
            #expect(record["startTime"] as? Double == preResponse.startTime)
            #expect(record["endTime"] as? Double == preResponse.endTime)
            #expect(record["confidence"] as? Double == 0.81)
            #expect(
                record["metadataSource"] as? String
                    == "privacy-test-source"
            )
        }
        let unrelatedRecord = try #require(
            windowRecords.first {
                $0["id"] as? String == unrelated.id
            }
        )
        #expect(
            unrelatedRecord["boundaryState"] as? String
                == unrelated.boundaryState
        )
        #expect(
            unrelatedRecord["decisionState"] as? String
                == unrelated.decisionState
        )
        #expect(unrelatedRecord["wasSkipped"] as? Bool == false)

        let bytes = try String(
            contentsOf: result.fileURL,
            encoding: .utf8
        )
        for source in sources {
            #expect(!bytes.contains(source.rawValue))
        }
        for index in sources.indices {
            #expect(!bytes.contains("private-route-receipt-\(index)"))
            #expect(!bytes.contains("private-evidence-\(index)"))
            #expect(!bytes.contains("private-target-\(index)"))
        }
        for aggregateKey in [
            "bannersShown",
            "bannersConfirmed",
            "bannersDenied",
        ] {
            #expect(!bytes.contains(aggregateKey))
        }
    }

    @Test("debug episode and library exports redact all explicit routes narrowly")
    func debugExportsRedactExplicitBannerFeedback() async throws {
        let store = try await makeTestStore()
        let assetId = "asset-debug-private-feedback"
        let asset = makeTestAsset(id: assetId)
        try await store.insertAsset(asset)
        let sources: [CorrectionSource] = [
            .bannerAutoSkipConfirmed,
            .bannerAutoSkipDenied,
            .bannerSuggestionConfirmed,
            .bannerSuggestionDenied,
        ]
        var privateStateSentinels: [String] = []
        var privateAdvertiserSentinels: [String] = []
        let unrelatedBoundary = "public-unrelated-boundary"
        let unrelatedDecision = "public-unrelated-decision"
        let unrelatedWindow = AdWindow(
            id: "debug-unrelated-window",
            analysisAssetId: assetId,
            startTime: 180,
            endTime: 210,
            confidence: 0.91,
            boundaryState: unrelatedBoundary,
            decisionState: unrelatedDecision,
            detectorVersion: "debug-privacy",
            advertiser: "Unrelated Diagnostic Sponsor",
            product: nil,
            adDescription: nil,
            evidenceText: "Unrelated Diagnostic Evidence",
            evidenceStartTime: 180,
            metadataSource: "debug-privacy",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: false
        )
        let preResponseWindows: [AdWindow] =
            sources.enumerated().map { index, source in
                let start = 10.0 + Double(index * 40)
                return AdWindow(
                    id: "debug-private-window-\(index)",
                    analysisAssetId: assetId,
                    startTime: start,
                    endTime: start + 30,
                    confidence: 0.83,
                    boundaryState:
                        "public-pre-response-boundary-\(index)",
                    decisionState:
                        source == .bannerAutoSkipConfirmed
                        || source == .bannerAutoSkipDenied
                        ? AdDecisionState.applied.rawValue
                        : AdDecisionState.candidate.rawValue,
                    detectorVersion: "debug-privacy",
                    advertiser: "Diagnostic Sponsor \(index)",
                    product: "Diagnostic Product \(index)",
                    adDescription: nil,
                    evidenceText: "Diagnostic Evidence \(index)",
                    evidenceStartTime: start,
                    metadataSource: "debug-privacy",
                    metadataConfidence: 0.7,
                    metadataPromptVersion: nil,
                    wasSkipped:
                        source == .bannerAutoSkipConfirmed
                        || source == .bannerAutoSkipDenied,
                    userDismissedBanner: false
                )
            }
        try await store.seedExplicitFeedbackEgressBaselineForTesting(
            analysisAssetId: assetId,
            unansweredWindows: preResponseWindows + [unrelatedWindow]
        )

        for (index, source) in sources.enumerated() {
            let start = 10.0 + Double(index * 40)
            let windowId = "debug-private-window-\(index)"
            let advertiser = "Diagnostic Sponsor \(index)"
            privateAdvertiserSentinels.append(advertiser)
            func routeWindow(
                id: String,
                boundary: String,
                decision: String,
                skipped: Bool,
                dismissed: Bool
            ) -> AdWindow {
                AdWindow(
                    id: id,
                    analysisAssetId: assetId,
                    startTime: start,
                    endTime: start + 30,
                    confidence: 0.83,
                    boundaryState: boundary,
                    decisionState: decision,
                    detectorVersion: "debug-privacy",
                    advertiser: advertiser,
                    product: "Diagnostic Product \(index)",
                    adDescription: nil,
                    evidenceText: "Diagnostic Evidence \(index)",
                    evidenceStartTime: start,
                    metadataSource: "debug-privacy",
                    metadataConfidence: 0.7,
                    metadataPromptVersion: nil,
                    wasSkipped: skipped,
                    userDismissedBanner: dismissed
                )
            }
            let preResponse = preResponseWindows[index]
            let responseRows: [AdWindow]
            let singularTarget: String
            let allTargets: [String]
            switch source {
            case .bannerAutoSkipConfirmed:
                responseRows = [preResponse]
                singularTarget = windowId
                allTargets = [windowId]
            case .bannerAutoSkipDenied:
                responseRows = [
                    routeWindow(
                        id: windowId,
                        boundary: preResponse.boundaryState,
                        decision: AdDecisionState.reverted.rawValue,
                        skipped: true,
                        dismissed: false
                    ),
                ]
                singularTarget = windowId
                allTargets = [windowId]
            case .bannerSuggestionDenied:
                responseRows = [
                    routeWindow(
                        id: windowId,
                        boundary: preResponse.boundaryState,
                        decision: AdDecisionState.reverted.rawValue,
                        skipped: false,
                        dismissed: true
                    ),
                ]
                singularTarget = windowId
                allTargets = [windowId]
            case .bannerSuggestionConfirmed:
                let promotedID = "\(windowId)-promoted"
                responseRows = [
                    routeWindow(
                        id: windowId,
                        boundary: preResponse.boundaryState,
                        decision: AdDecisionState.suppressed.rawValue,
                        skipped: false,
                        dismissed: false
                    ),
                    routeWindow(
                        id: promotedID,
                        boundary: "userConfirmedSuggested",
                        decision: AdDecisionState.applied.rawValue,
                        skipped: true,
                        dismissed: false
                    ),
                ]
                singularTarget = promotedID
                allTargets = [windowId, promotedID]
                privateStateSentinels.append("userConfirmedSuggested")
            case .listenRevert, .manualVeto, .falseNegative:
                Issue.record("Unexpected non-explicit source")
                continue
            }
            for responseRow in responseRows {
                try await store.insertAdWindow(responseRow)
            }
            try await store.appendCorrectionEvent(
                CorrectionEvent(
                    id: "debug-private-receipt-\(index)",
                    analysisAssetId: assetId,
                    scope: CorrectionScope.exactTimeSpan(
                        assetId: assetId,
                        startTime: start,
                        endTime: start + 30
                    ).serialized,
                    createdAt: 1_700_000_500 + Double(index),
                    source: source,
                    correctionType: source.kind.correctionType,
                    targetRefs: CorrectionTargetRefs(
                        adWindowId: singularTarget,
                        adWindowIds: allTargets,
                        explicitFeedbackDetectionProjection:
                            ExplicitFeedbackDetectionProjection(preResponse),
                        sponsorEntity: "debug-private-target-\(index)"
                    )
                )
            )
        }

        try await store.insertAdWindow(unrelatedWindow)

        let episodeExport = try #require(
            await DebugEpisodeExportService.build(
                episodeTitle: "Privacy Test Episode",
                podcastTitle: "Privacy Test Podcast",
                analysisAssetId: assetId,
                episodeId: asset.episodeId,
                store: store
            )
        )
        let libraryExport = try #require(
            await DebugEpisodeExportService.buildLibraryExport(store: store)
        )
        for content in [episodeExport.content, libraryExport.content] {
            for sentinel in privateStateSentinels {
                #expect(!content.contains(sentinel))
            }
            for (index, source) in sources.enumerated() {
                #expect(!content.contains(source.rawValue))
                #expect(!content.contains("debug-private-receipt-\(index)"))
                #expect(!content.contains("debug-private-target-\(index)"))
                #expect(
                    !content.contains(
                        String(1_700_000_500 + index)
                    )
                )
            }
            for aggregateKey in [
                "bannersShown",
                "bannersConfirmed",
                "bannersDenied",
            ] {
                #expect(!content.contains(aggregateKey))
            }
            #expect(content.contains(unrelatedDecision))
            #expect(content.contains("Unrelated Diagnostic Sponsor"))
            #expect(content.contains("Unrelated Diagnostic Evidence"))
            for advertiser in privateAdvertiserSentinels {
                #expect(
                    content.contains(advertiser),
                    "Detection diagnostics unrelated to the response must remain available"
                )
            }
        }
        #expect(episodeExport.content.contains(unrelatedBoundary))
        #expect(!episodeExport.content.contains("(was skipped)"))
    }

    @Test(
        "all explicit routes are byte-equivalent to unanswered Corpus and Debug exports"
    )
    func explicitRoutesMatchUnansweredExportBytes() async throws {
        let exportedAt = Date(timeIntervalSince1970: 1_800_000_000)
        let sources: [CorrectionSource] = [
            .bannerAutoSkipConfirmed,
            .bannerAutoSkipDenied,
            .bannerSuggestionConfirmed,
            .bannerSuggestionDenied,
        ]

        func window(
            id: String,
            assetId: String,
            start: Double,
            boundary: String =
                AdBoundaryState.acousticRefined.rawValue,
            decision: String,
            skipped: Bool = false,
            dismissed: Bool = false
        ) -> AdWindow {
            AdWindow(
                id: id,
                analysisAssetId: assetId,
                startTime: start,
                endTime: start + 35,
                confidence: 0.864,
                boundaryState: boundary,
                decisionState: decision,
                detectorVersion: "equivalence-detector",
                advertiser: "Equivalence Sponsor",
                product: "Equivalence Product",
                adDescription: "Equivalence Description",
                evidenceText: "Equivalence Evidence",
                evidenceStartTime: start + 1,
                metadataSource: "equivalence-source",
                metadataConfidence: 0.753,
                metadataPromptVersion: "equivalence-prompt",
                wasSkipped: skipped,
                userDismissedBanner: dismissed,
                evidenceSources: "semantic,acoustic",
                eligibilityGate: SkipEligibilityGate.eligible.rawValue,
                catalogStoreMatchSimilarity: 0.642,
                startEdgeAnchor: "equivalence-start",
                endEdgeAnchor: "equivalence-end"
            )
        }

        for (index, source) in sources.enumerated() {
            let assetId = "asset-export-equivalence-\(index)"
            let asset = AnalysisAsset(
                id: assetId,
                episodeId: "episode-export-equivalence-\(index)",
                assetFingerprint: String(repeating: "\(index)", count: 64),
                weakFingerprint: nil,
                sourceURL: "file:///test/\(assetId).m4a",
                featureCoverageEndTime: nil,
                fastTranscriptCoverageEndTime: nil,
                confirmedAdCoverageEndTime: 180,
                analysisState: "new",
                analysisVersion: 1,
                capabilitySnapshot: nil,
                episodeDurationSec: 300
            )
            let original = window(
                id: "original-export-row-\(index)",
                assetId: assetId,
                start: 45,
                decision:
                    index < 2
                    ? AdDecisionState.confirmed.rawValue
                    : AdDecisionState.candidate.rawValue
            )
            let sameSpanDeniedPeer = window(
                id: "same-span-denied-peer-\(index)",
                assetId: assetId,
                start: original.startTime,
                decision: AdDecisionState.candidate.rawValue
            )
            let retiredByBackfill = window(
                id: "retired-by-backfill-\(index)",
                assetId: assetId,
                start: 90,
                decision: AdDecisionState.candidate.rawValue
            )
            let modifiedByBackfill = window(
                id: "modified-by-backfill-\(index)",
                assetId: assetId,
                start: 140,
                decision: AdDecisionState.candidate.rawValue
            )
            let sharedConfirmed = window(
                id: "public-confirmed-\(index)",
                assetId: assetId,
                start: 1,
                decision: AdDecisionState.confirmed.rawValue
            )

            let baselineStore = try await makeTestStore()
            try await baselineStore.insertAsset(asset)
            let routeBaselineRows =
                source == .bannerSuggestionDenied
                ? [original, sameSpanDeniedPeer]
                : [original]
            let baselineRows =
                [sharedConfirmed]
                + routeBaselineRows
                + [retiredByBackfill, modifiedByBackfill]
            let baselineSpan = DecodedSpan(
                id: DecodedSpan.makeId(
                    assetId: assetId,
                    firstAtomOrdinal: 0,
                    lastAtomOrdinal: 9
                ),
                assetId: assetId,
                firstAtomOrdinal: 0,
                lastAtomOrdinal: 9,
                startTime: 0,
                endTime: 100,
                anchorProvenance: [
                    .classifierSeed(
                        regionId: "pre-response-\(index)",
                        score: 0.91
                    ),
                ]
            )
            let rewindCreatedAt = Date(
                timeIntervalSince1970: 1_700_100_000 + Double(index)
            )
            for row in baselineRows {
                try await baselineStore.insertAdWindow(row)
            }
            try await baselineStore.upsertDecodedSpans([baselineSpan])
            try await baselineStore.insertListenRewind(
                windowId: retiredByBackfill.id,
                analysisAssetId: assetId,
                podcastId: "equivalence-podcast",
                time: retiredByBackfill.startTime,
                createdAt: rewindCreatedAt
            )

            let respondedStore = try await makeTestStore()
            try await respondedStore.insertAsset(asset)
            for row in baselineRows {
                try await respondedStore.insertAdWindow(row)
            }
            try await respondedStore.upsertDecodedSpans([baselineSpan])
            try await respondedStore.insertListenRewind(
                windowId: retiredByBackfill.id,
                analysisAssetId: assetId,
                podcastId: "equivalence-podcast",
                time: retiredByBackfill.startTime,
                createdAt: rewindCreatedAt
            )
            let promoted = window(
                id: "promoted-export-row-\(index)",
                assetId: assetId,
                start: original.startTime,
                boundary: "userConfirmedSuggested",
                decision: AdDecisionState.applied.rawValue,
                skipped: true
            )
            func receipt(
                id: String,
                producer: AdWindow,
                singularID: String,
                targetIDs: [String]
            ) -> CorrectionEvent {
                CorrectionEvent(
                    id: id,
                    analysisAssetId: assetId,
                    scope: CorrectionScope.exactTimeSpan(
                        assetId: assetId,
                        startTime: producer.startTime,
                        endTime: producer.endTime
                    ).serialized,
                    createdAt: 1_700_200_000 + Double(index),
                    source: source,
                    correctionType: source.kind.correctionType,
                    targetRefs: CorrectionTargetRefs(
                        adWindowId: singularID,
                        adWindowIds: targetIDs,
                        explicitFeedbackDetectionProjection:
                            ExplicitFeedbackDetectionProjection(producer),
                        exactFeedbackSpan: ExactFeedbackSpan(
                            startTime: producer.startTime,
                            endTime: producer.endTime
                        )
                    )
                )
            }

            switch source {
            case .bannerAutoSkipConfirmed:
                #expect(
                    try await respondedStore.persistConfirmedAutoSkip(
                        windowId: original.id,
                        analysisAssetId: assetId,
                        expectedEpisodeId: asset.episodeId,
                        expectedStartTime: original.startTime,
                        expectedEndTime: original.endTime,
                        expectedProducerRevision: original,
                        expectedMaterialToken:
                            AdWindowMaterialIdentity.autoSkipToken(
                                window: original,
                                displayedStart: original.startTime,
                                displayedEnd: original.endTime
                            ),
                        correction: receipt(
                            id: "auto-yes-\(index)",
                            producer: original,
                            singularID: original.id,
                            targetIDs: [original.id]
                        )
                    ) == true
                )
            case .bannerAutoSkipDenied:
                #expect(
                    try await respondedStore.persistDeniedAutoSkip(
                        windowId: original.id,
                        analysisAssetId: assetId,
                        expectedEpisodeId: asset.episodeId,
                        expectedStartTime: original.startTime,
                        expectedEndTime: original.endTime,
                        expectedProducerRevision: original,
                        expectedMaterialToken:
                            AdWindowMaterialIdentity.autoSkipToken(
                                window: original,
                                displayedStart: original.startTime,
                                displayedEnd: original.endTime
                            ),
                        correction: receipt(
                            id: "auto-no-\(index)",
                            producer: original,
                            singularID: original.id,
                            targetIDs: [original.id]
                        )
                    ) == true
                )
            case .bannerSuggestionConfirmed:
                #expect(
                    try await respondedStore
                        .persistAcceptedSuggestionIfCurrent(
                            originalWindowId: original.id,
                            originalAnalysisAssetId: assetId,
                            expectedEpisodeId: asset.episodeId,
                            expectedStartTime: original.startTime,
                            expectedEndTime: original.endTime,
                            expectedProducerRevision: original,
                            expectedMaterialToken:
                                AdWindowMaterialIdentity
                                .suggestionToken(original),
                            promotedWindow: promoted,
                            correction: receipt(
                                id: "suggest-yes-\(index)",
                                producer: original,
                                singularID: promoted.id,
                                targetIDs: [original.id, promoted.id]
                            )
                        ) == true
                )
            case .bannerSuggestionDenied:
                for (receiptIndex, producer) in
                    [original, sameSpanDeniedPeer].enumerated()
                {
                    #expect(
                        try await respondedStore
                            .persistDeclinedSuggestionIfCurrent(
                                windowId: producer.id,
                                analysisAssetId: assetId,
                                expectedEpisodeId: asset.episodeId,
                                expectedStartTime: producer.startTime,
                                expectedEndTime: producer.endTime,
                                expectedProducerRevision: producer,
                                expectedMaterialToken:
                                    AdWindowMaterialIdentity
                                    .suggestionToken(producer),
                                correction: receipt(
                                    id:
                                        "suggest-no-\(index)-\(receiptIndex)",
                                    producer: producer,
                                    singularID: producer.id,
                                    targetIDs: [producer.id]
                                )
                            ) == true
                    )
                }
            case .listenRevert, .manualVeto, .falseNegative:
                Issue.record("Unexpected non-explicit source")
                continue
            }

            // A Listen arriving after the atomic explicit-response marker is
            // private post-answer state. Even though it targets a window ID
            // present in the frozen baseline, it must not change Corpus bytes
            // or counts for any of the four explicit routes.
            try await respondedStore.insertListenRewind(
                windowId: original.id,
                analysisAssetId: assetId,
                podcastId: "post-marker-private-podcast",
                time: original.startTime,
                createdAt: Date(
                    timeIntervalSince1970:
                        1_700_300_000 + Double(index)
                )
            )

            // Exercise the real atomic backfill reconciliation after private
            // feedback: one unrelated row is created, one retired, and one
            // producer revision is replaced near the decision threshold.
            let createdByBackfill = window(
                id: "created-by-backfill-\(index)",
                assetId: assetId,
                start: 190,
                decision: AdDecisionState.candidate.rawValue
            )
            let modifiedReplacement = window(
                id: modifiedByBackfill.id,
                assetId: assetId,
                start: modifiedByBackfill.startTime + 0.125,
                decision: AdDecisionState.confirmed.rawValue
            )
            try await respondedStore.reconcileBackfillAdWindows(
                [createdByBackfill, modifiedReplacement],
                retiredIDs: [retiredByBackfill.id]
            )
            try await respondedStore.updateConfirmedAdCoverage(
                id: assetId,
                endTime: 299
            )

            // Drive the real correction-mask/projector/decoder read side after
            // the explicit receipt lands, then persist its learned live span
            // shape. Yes routes force-anchor the answered time range; No
            // routes veto it. Either result diverges from the pre-answer span,
            // while outward Corpus bytes must continue to use the baseline.
            let atoms: [TranscriptAtom] = (0..<20).map { ordinal in
                TranscriptAtom(
                    atomKey: TranscriptAtomKey(
                        analysisAssetId: assetId,
                        transcriptVersion: "equivalence-tv",
                        atomOrdinal: ordinal
                    ),
                    contentHash: "equivalence-\(ordinal)",
                    startTime: Double(ordinal) * 10,
                    endTime: Double(ordinal + 1) * 10,
                    text: "equivalence atom \(ordinal)",
                    chunkIndex: ordinal
                )
            }
            let correctionStore = PersistentUserCorrectionStore(
                store: respondedStore
            )
            let maskProvider =
                await AdDetectionService.makeCorrectionMaskProvider(
                    enabled: true,
                    store: correctionStore,
                    analysisAssetId: assetId,
                    atoms: atoms
                )
            let learnedEvidence = await AtomEvidenceProjector().project(
                regions: [],
                catalog: EvidenceCatalog(
                    analysisAssetId: assetId,
                    transcriptVersion: "equivalence-tv",
                    entries: []
                ),
                atoms: atoms,
                correctionMaskProvider: maskProvider
            )
            let learnedSpans = MinimalContiguousSpanDecoder().decode(
                atoms: learnedEvidence,
                assetId: assetId
            )
            try await respondedStore.deleteDecodedSpans(
                assetId: assetId
            )
            try await respondedStore.upsertDecodedSpans(learnedSpans)
            #expect(
                learnedSpans != [baselineSpan],
                "The real correction projector must make live decoded spans diverge"
            )
            let privatelyLearnedRows = try await respondedStore
                .fetchAdWindows(assetId: assetId)
            #expect(
                Set(privatelyLearnedRows.map(\.id))
                    != Set(baselineRows.map(\.id)),
                "Local backfill rows must genuinely diverge"
            )
            if source == .bannerSuggestionDenied {
                let receipts =
                    try await respondedStore.loadCorrectionEvents(
                        analysisAssetId: assetId
                    )
                #expect(receipts.count == 2)
                #expect(
                    Set(receipts.compactMap {
                        $0.targetRefs?.adWindowId
                    }) == Set([original.id, sameSpanDeniedPeer.id])
                )
            }

            let baselineDocs = try makeTempDir(
                prefix: "CorpusExport-equivalence-before-\(index)"
            )
            let respondedDocs = try makeTempDir(
                prefix: "CorpusExport-equivalence-after-\(index)"
            )
            let beforeCorpus = try await CorpusExporter.export(
                store: baselineStore,
                documentsURL: baselineDocs,
                now: exportedAt,
                dedupMemo: CorpusExportDedupMemo()
            )
            let afterCorpus = try await CorpusExporter.export(
                store: respondedStore,
                documentsURL: respondedDocs,
                now: exportedAt,
                dedupMemo: CorpusExportDedupMemo()
            )
            #expect(
                try Data(contentsOf: afterCorpus.fileURL)
                    == Data(contentsOf: beforeCorpus.fileURL),
                "\(source) changed Corpus rows, count, identity, span, or bytes"
            )
            #expect(beforeCorpus.spanCount == 1)
            #expect(afterCorpus.spanCount == 1)
            #expect(beforeCorpus.listenRewindCount == 1)
            #expect(afterCorpus.listenRewindCount == 1)

            let beforeEpisode = try #require(
                await DebugEpisodeExportService.build(
                    episodeTitle: "Equivalence Episode",
                    podcastTitle: "Equivalence Podcast",
                    analysisAssetId: assetId,
                    episodeId: asset.episodeId,
                    store: baselineStore,
                    exportedAt: exportedAt
                )
            )
            let afterEpisode = try #require(
                await DebugEpisodeExportService.build(
                    episodeTitle: "Equivalence Episode",
                    podcastTitle: "Equivalence Podcast",
                    analysisAssetId: assetId,
                    episodeId: asset.episodeId,
                    store: respondedStore,
                    exportedAt: exportedAt
                )
            )
            #expect(afterEpisode.content == beforeEpisode.content)
            #expect(afterEpisode.filename == beforeEpisode.filename)

            let beforeLibrary = try #require(
                await DebugEpisodeExportService.buildLibraryExport(
                    store: baselineStore,
                    exportedAt: exportedAt
                )
            )
            let afterLibrary = try #require(
                await DebugEpisodeExportService.buildLibraryExport(
                    store: respondedStore,
                    exportedAt: exportedAt
                )
            )
            #expect(afterLibrary.content == beforeLibrary.content)
            #expect(afterLibrary.filename == beforeLibrary.filename)

            let baselineShareProjection = try #require(
                try await baselineStore
                    .responseIndependentAdWindows(
                        analysisAssetId: assetId
                    )
            )
            #expect(
                baselineShareProjection.allSatisfy(
                    CrossUserAnalysisSnapshot.Window
                        .hasKnownExportDisposition
                )
            )
            let directlyExportableShareWindows =
                baselineShareProjection.compactMap(
                    CrossUserAnalysisSnapshot.Window.exported
                )
            for projectedRow in baselineShareProjection {
                #expect(
                    CrossUserAnalysisSnapshot.Window.exported(
                        from: projectedRow
                    ) != nil,
                    "CrossUser fixture row is not exportable: \(projectedRow.id), decision=\(projectedRow.decisionState), boundary=\(projectedRow.boundaryState), gate=\(projectedRow.eligibilityGate ?? "nil")"
                )
            }
            #expect(
                directlyExportableShareWindows.count
                    == baselineShareProjection.count
            )
            #expect(
                CrossUserAnalysisShareKey.make(
                    podcastId: "equivalence-podcast",
                    fileSHA: asset.assetFingerprint,
                    analysisVersion: asset.analysisVersion
                ) != nil
            )
            let beforeShare = try #require(
                try await baselineStore
                .exportCrossUserAnalysisSnapshot(
                    assetId: assetId,
                    podcastId: "equivalence-podcast",
                    exportedAt: exportedAt,
                    sourceAppBuild: "equivalence-build"
                )
            )
            let afterShare = try #require(
                try await respondedStore
                .exportCrossUserAnalysisSnapshot(
                    assetId: assetId,
                    podcastId: "equivalence-podcast",
                    exportedAt: exportedAt,
                    sourceAppBuild: "equivalence-build"
                )
            )
            #expect(
                beforeShare.windows.map(\.sourceWindowId)
                    == afterShare.windows.map(\.sourceWindowId)
            )
            #expect(
                beforeShare.analysisCoverageEndSec
                    == afterShare.analysisCoverageEndSec
            )
            #expect(beforeShare.windows.count == afterShare.windows.count)
            #expect(
                afterShare == beforeShare,
                "\(source) changed shared IDs, count, coverage, or bytes after real backfill reconciliation"
            )

            try await respondedStore.execForTesting(
                "DELETE FROM correction_events WHERE analysisAssetId = '\(assetId)'"
            )
            let markerOnlyProjection =
                try await respondedStore.responseIndependentAdWindows(
                    analysisAssetId: assetId
                )
            let unansweredProjection =
                try await baselineStore.responseIndependentAdWindows(
                    analysisAssetId: assetId
                )
            #expect(
                markerOnlyProjection?
                    .map(ExplicitFeedbackDetectionProjection.init)
                    == unansweredProjection?
                    .map(ExplicitFeedbackDetectionProjection.init),
                "The durable baseline marker must prevent live-row fallback after receipt loss"
            )
        }
    }

    @Test("Suggest-Yes original and promoted rows are jointly private in every export")
    func suggestYesPairIsRedactedAcrossExports() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-suggest-pair")
        let assetId = "asset-suggest-pair"
        let asset = makeTestAsset(id: assetId)
        try await store.insertAsset(asset)

        func window(
            id: String,
            start: Double,
            boundary: String,
            decision: String,
            skipped: Bool,
            advertiser: String
        ) -> AdWindow {
            AdWindow(
                id: id,
                analysisAssetId: assetId,
                startTime: start,
                endTime: start + 30,
                confidence: 0.87,
                boundaryState: boundary,
                decisionState: decision,
                detectorVersion: "pair-diagnostics",
                advertiser: advertiser,
                product: "Pair diagnostic product",
                adDescription: nil,
                evidenceText: "Pair diagnostic evidence",
                evidenceStartTime: start,
                metadataSource: "pair-diagnostics",
                metadataConfidence: 0.76,
                metadataPromptVersion: nil,
                wasSkipped: skipped,
                userDismissedBanner: false
            )
        }

        let original = window(
            id: "suggest-original-row",
            start: 20,
            boundary: "private-original-boundary",
            decision: AdDecisionState.suppressed.rawValue,
            skipped: false,
            advertiser: "Original Diagnostic Sponsor"
        )
        let promoted = window(
            id: "suggest-promoted-row",
            start: 20,
            boundary: "userConfirmedSuggested",
            decision: AdDecisionState.applied.rawValue,
            skipped: true,
            advertiser: "Promoted Diagnostic Sponsor"
        )
        let unrelated = window(
            id: "pair-unrelated-row",
            start: 100,
            boundary: "public-pair-boundary",
            decision: "public-pair-decision",
            skipped: true,
            advertiser: "Unrelated Pair Sponsor"
        )
        let preResponseOriginal = AdWindow(
            id: original.id,
            analysisAssetId: original.analysisAssetId,
            startTime: original.startTime,
            endTime: original.endTime,
            confidence: original.confidence,
            boundaryState: original.boundaryState,
            decisionState: AdDecisionState.candidate.rawValue,
            detectorVersion: original.detectorVersion,
            advertiser: original.advertiser,
            product: original.product,
            adDescription: original.adDescription,
            evidenceText: original.evidenceText,
            evidenceStartTime: original.evidenceStartTime,
            metadataSource: original.metadataSource,
            metadataConfidence: original.metadataConfidence,
            metadataPromptVersion: original.metadataPromptVersion,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: original.evidenceSources,
            eligibilityGate: original.eligibilityGate,
            catalogStoreMatchSimilarity:
                original.catalogStoreMatchSimilarity,
            startEdgeAnchor: original.startEdgeAnchor,
            endEdgeAnchor: original.endEdgeAnchor
        )
        try await store.seedExplicitFeedbackEgressBaselineForTesting(
            analysisAssetId: assetId,
            unansweredWindows: [preResponseOriginal, unrelated]
        )
        for value in [original, promoted, unrelated] {
            try await store.insertAdWindow(value)
        }
        try await store.appendCorrectionEvent(
            CorrectionEvent(
                id: "private-suggest-pair-receipt",
                analysisAssetId: assetId,
                scope: CorrectionScope.exactTimeSpan(
                    assetId: assetId,
                    startTime: 20,
                    endTime: 50
                ).serialized,
                createdAt: 1_700_001_000,
                source: .bannerSuggestionConfirmed,
                correctionType: .falseNegative,
                targetRefs: CorrectionTargetRefs(
                    adWindowId: promoted.id,
                    adWindowIds: [original.id, promoted.id],
                    explicitFeedbackDetectionProjection:
                        ExplicitFeedbackDetectionProjection(
                            preResponseOriginal
                        )
                )
            )
        )

        let corpus = try await CorpusExporter.export(
            store: store,
            documentsURL: docs
        )
        let records = try parseJSONL(at: corpus.fileURL)
        let windowRecords = records.filter {
            $0["type"] as? String == "ad_window"
        }
        let restored = try #require(
            windowRecords.first { $0["id"] as? String == original.id }
        )
        #expect(
            restored["boundaryState"] as? String
                == original.boundaryState
        )
        #expect(
            restored["decisionState"] as? String
                == AdDecisionState.candidate.rawValue
        )
        #expect(restored["wasSkipped"] as? Bool == false)
        #expect(
            restored["advertiser"] as? String == original.advertiser
        )
        #expect(
            !windowRecords.contains {
                $0["id"] as? String == promoted.id
            }
        )
        let publicRecord = try #require(
            windowRecords.first {
                $0["id"] as? String == unrelated.id
            }
        )
        #expect(
            publicRecord["decisionState"] as? String
                == unrelated.decisionState
        )
        #expect(publicRecord["wasSkipped"] as? Bool == false)
        #expect(corpus.correctionCount == 0)

        let episode = try #require(
            await DebugEpisodeExportService.build(
                episodeTitle: "Suggest Pair Episode",
                podcastTitle: "Suggest Pair Podcast",
                analysisAssetId: assetId,
                episodeId: asset.episodeId,
                store: store
            )
        )
        let library = try #require(
            await DebugEpisodeExportService.buildLibraryExport(
                store: store
            )
        )
        for content in [episode.content, library.content] {
            #expect(
                content.contains(AdDecisionState.candidate.rawValue)
            )
            #expect(!content.contains(promoted.boundaryState))
            #expect(!content.contains(promoted.decisionState))
            #expect(!content.contains("private-suggest-pair-receipt"))
            #expect(content.contains("Original Diagnostic Sponsor"))
            #expect(!content.contains("Promoted Diagnostic Sponsor"))
            #expect(content.contains(unrelated.decisionState))
        }
        #expect(episode.content.contains(original.boundaryState))
        #expect(!episode.content.contains("(was skipped)"))
    }

    @Test("correction-query failure fails closed with no detection rows")
    func correctionQueryFailureFailsClosedForWindowState() async throws {
        let docs = try makeTempDir(prefix: "CorpusExport-feedback-failure")
        let asset = makeTestAsset(id: "asset-feedback-query-failure")
        let privateWindow = AdWindow(
            id: "query-failure-private-row",
            analysisAssetId: asset.id,
            startTime: 30,
            endTime: 60,
            confidence: 0.88,
            boundaryState: "query-failure-private-boundary",
            decisionState: "query-failure-private-decision",
            detectorVersion: "query-failure-diagnostics",
            advertiser: "Query Failure Diagnostic Sponsor",
            product: nil,
            adDescription: nil,
            evidenceText: "Query Failure Diagnostic Evidence",
            evidenceStartTime: 30,
            metadataSource: "query-failure-diagnostics",
            metadataConfidence: 0.7,
            metadataPromptVersion: nil,
            wasSkipped: true,
            userDismissedBanner: true
        )
        let source = FailingSource(
            assets: [asset],
            spans: [:],
            events: [:],
            windows: [asset.id: [privateWindow]],
            failSpansFor: [],
            failEventsFor: [asset.id]
        )

        let result = try await CorpusExporter.export(
            store: source,
            documentsURL: docs
        )
        let records = try parseJSONL(at: result.fileURL)
        #expect(
            !records.contains {
                $0["type"] as? String == "ad_window"
            }
        )
        #expect(result.adWindowCount == 0)
    }

    @Test("export skips corrupt-scope correction rows without aborting the overall export")
    func exportSkipsCorruptScopes() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-corrupt")

        let a1 = makeTestAsset(id: "asset-1")
        try await store.insertAsset(a1)

        // Insert a correction event with a deliberately corrupt scope string.
        // We bypass PersistentUserCorrectionStore because it always writes valid
        // scopes; AnalysisStore.appendCorrectionEvent takes the raw string.
        let goodScope = CorrectionScope.exactSpan(assetId: "asset-1", ordinalRange: 5...10)
        let goodEvent = CorrectionEvent(
            id: "good",
            analysisAssetId: "asset-1",
            scope: goodScope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto
        )
        let badEvent = CorrectionEvent(
            id: "bad",
            analysisAssetId: "asset-1",
            scope: "!!!not_a_scope_at_all",
            createdAt: 1_700_000_001,
            source: .manualVeto
        )
        try await store.appendCorrectionEvent(goodEvent)
        try await store.appendCorrectionEvent(badEvent)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.correctionCount == 1,
                "Only the good correction should be exported")
        #expect(result.skippedCorrectionCount == 1,
                "One corrupt-scope row should be logged as skipped")

        let records = try parseJSONL(at: result.fileURL)
        let correctionRecords = records.filter { ($0["type"] as? String) == "correction" }
        #expect(correctionRecords.count == 1)
        let ids = correctionRecords.compactMap { $0["id"] as? String }
        #expect(ids == ["good"])
    }

    @Test("export writes streaming via FileHandle — file opens and closes cleanly even with many rows")
    func exportStreamingWorksAtScale() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-stream")

        // Seed 50 assets with 5 spans each (250 decision rows) to exercise
        // the streaming path without depending on Array-in-memory accumulation.
        var spans: [DecodedSpan] = []
        for i in 0..<50 {
            let assetId = "asset-\(i)"
            try await store.insertAsset(makeTestAsset(id: assetId))
            for j in 0..<5 {
                let first = j * 10
                let last = first + 5
                spans.append(makeSpan(assetId: assetId, firstOrdinal: first, lastOrdinal: last))
            }
        }
        try await store.upsertDecodedSpans(spans)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.assetCount == 50)
        #expect(result.spanCount == 250)

        // File exists, line count matches, and the last line parses cleanly
        // (proves the FileHandle was flushed + closed).
        let records = try parseJSONL(at: result.fileURL)
        #expect(records.count == 50 + 250)
        #expect(records.last?["type"] as? String != nil)
    }

    @Test("filename is produced from current timestamp in the Documents directory — file lives where Files.app can see it")
    func filePathLocation() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-path")
        try await store.insertAsset(makeTestAsset(id: "asset-z"))

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.fileURL.deletingLastPathComponent().path == docs.path,
                "file must be in the provided documents URL")
        #expect(result.fileURL.lastPathComponent.hasPrefix("corpus-export."))
        #expect(result.fileURL.lastPathComponent.hasSuffix(".jsonl"))
    }

    // MARK: - G1: Filename collision (millisecond timestamps disambiguate)

    @Test("back-to-back export() calls produce two distinct files — no same-second clobber")
    func backToBackExportsProduceTwoFiles() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-collide")
        try await store.insertAsset(makeTestAsset(id: "asset-k"))

        // Inject explicit `now:` values 1ms apart so the test is deterministic
        // across machines. Milliseconds resolve the collision; pre-fix the
        // filename used second precision and the second file overwrote the
        // first.
        //
        // playhead-vnni: this test's intent is filename-precision, NOT
        // identical-content dedup. We mutate the store between calls so the
        // two exports have different content and both files are retained;
        // dedup behavior is covered by `exportDeduplicatesRapidBackToBack...`
        // below. Use an isolated memo to avoid cross-test state.
        let memo = CorpusExportDedupMemo()
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let a = try await CorpusExporter.export(
            store: store, documentsURL: docs, now: base, dedupMemo: memo
        )
        try await store.insertAsset(makeTestAsset(id: "asset-k2"))
        let b = try await CorpusExporter.export(
            store: store, documentsURL: docs,
            now: base.addingTimeInterval(0.001), dedupMemo: memo
        )

        #expect(a.fileURL != b.fileURL, "two exports produced the same filename: \(a.fileURL.lastPathComponent)")
        #expect(FileManager.default.fileExists(atPath: a.fileURL.path))
        #expect(FileManager.default.fileExists(atPath: b.fileURL.path))

        // Enumerate the docs dir and confirm two corpus-export.*.jsonl files.
        let contents = try FileManager.default.contentsOfDirectory(atPath: docs.path)
        let exports = contents.filter { $0.hasPrefix("corpus-export.") && $0.hasSuffix(".jsonl") }
        #expect(exports.count == 2, "expected 2 corpus-export files, got \(exports)")
    }

    // MARK: - vnni: idempotent emission across rapid back-to-back triggers

    /// Regression for playhead-vnni. The 2026-04-25 device snapshot recorded
    /// four byte-identical (md5 `432e1d7b362000175baa10802ba4e759`)
    /// `corpus-export*.jsonl` files written within 1.8 seconds. Cause: the
    /// debug-menu Button schedules a fresh `Task { … }` per tap, and the
    /// `corpusExportInProgress` guard is set inside the function body — so
    /// taps that arrive before the first Task body has run all enqueue and
    /// each writes its own file with identical content.
    ///
    /// Fix layer: idempotency at the export sink. The exporter hashes the
    /// streamed bytes inline; if the resulting digest matches a recent
    /// successful export from the same `documentsURL`, the just-written
    /// duplicate is removed and the prior file's URL is returned. Counts
    /// reflect the current run (downstream summary UI still shows what
    /// "would have been" exported), but only one physical file exists on
    /// disk per content-identical trigger storm.
    @Test("export: 4 rapid back-to-back triggers with identical store state produce exactly 1 corpus-export file")
    func exportDeduplicatesRapidBackToBackTriggers() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-vnni-dedupe")
        try await store.insertAsset(makeTestAsset(id: "asset-storm"))

        // Use an isolated dedup memo so this test doesn't leak state across
        // suite runs and doesn't observe state from other tests.
        let memo = CorpusExportDedupMemo()
        let base = Date(timeIntervalSince1970: 1_700_000_000)

        var results: [CorpusExportResult] = []
        for tick in 0..<4 {
            let stamp = base.addingTimeInterval(0.6 * Double(tick))  // ~600ms apart, mirrors the device snapshot cadence
            let r = try await CorpusExporter.export(
                store: store,
                documentsURL: docs,
                now: stamp,
                dedupMemo: memo
            )
            results.append(r)
        }

        // Only one physical file on disk — the storm collapsed.
        let contents = try FileManager.default.contentsOfDirectory(atPath: docs.path)
        let exportFiles = contents.filter { $0.hasPrefix("corpus-export.") && $0.hasSuffix(".jsonl") }
        #expect(exportFiles.count == 1,
                "expected 1 corpus-export file after 4 identical-content triggers, got \(exportFiles)")

        // All 4 results must point at the same file URL — the first export's
        // file is preserved, subsequent calls redirect to it.
        let urls = Set(results.map { $0.fileURL })
        #expect(urls.count == 1,
                "all results must share one fileURL, got \(urls.map { $0.lastPathComponent })")
        #expect(results.first?.fileURL == results.last?.fileURL)
    }

    /// Sibling proof: when the underlying content has *changed* between
    /// triggers (a real user action that should produce a new export), the
    /// dedup memo does NOT collapse the new file. This guards against the
    /// "fix overshoots and now legit re-exports vanish" failure mode.
    @Test("export: a second trigger with new content writes a distinct file (dedup must not eat real changes)")
    func exportDoesNotDeduplicateWhenContentChanged() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-vnni-distinct")
        try await store.insertAsset(makeTestAsset(id: "asset-first"))

        let memo = CorpusExportDedupMemo()
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let first = try await CorpusExporter.export(
            store: store,
            documentsURL: docs,
            now: base,
            dedupMemo: memo
        )

        // Mutate the store between calls so the export bytes differ.
        try await store.insertAsset(makeTestAsset(id: "asset-second"))
        let second = try await CorpusExporter.export(
            store: store,
            documentsURL: docs,
            now: base.addingTimeInterval(0.5),
            dedupMemo: memo
        )

        #expect(first.fileURL != second.fileURL,
                "different content within the dedup window must still produce a new file")
        let contents = try FileManager.default.contentsOfDirectory(atPath: docs.path)
        let exportFiles = contents.filter { $0.hasPrefix("corpus-export.") && $0.hasSuffix(".jsonl") }
        #expect(exportFiles.count == 2)
    }

    /// Sibling proof: the dedup window is bounded. After it lapses, an
    /// identical-content export still writes a fresh file — the memo is
    /// for "near-simultaneous storm collapse," not "permanent suppression."
    @Test("export: identical content past the dedup window writes a new file")
    func exportRewritesIdenticalContentPastWindow() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-vnni-window")
        try await store.insertAsset(makeTestAsset(id: "asset-w"))

        let memo = CorpusExportDedupMemo()
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let first = try await CorpusExporter.export(
            store: store,
            documentsURL: docs,
            now: base,
            dedupMemo: memo,
            dedupWindow: 5.0
        )
        // 6s later (past 5s window) — must NOT collapse onto the prior file.
        let second = try await CorpusExporter.export(
            store: store,
            documentsURL: docs,
            now: base.addingTimeInterval(6.0),
            dedupMemo: memo,
            dedupWindow: 5.0
        )

        #expect(first.fileURL != second.fileURL,
                "identical content past the dedup window must produce a new file")
    }

    // MARK: - G3: anchorProvenance round-trip

    @Test("export: anchorProvenance round-trips through spanLine with fmConsensus + evidenceCatalog entries")
    func anchorProvenanceRoundTrip() throws {
        // Build a span with two distinct anchor types so the Codable adapter
        // contract is exercised — not just an empty []. Locks in the on-disk
        // JSON shape that downstream tooling parses.
        let entry = EvidenceEntry(
            evidenceRef: 7,
            category: .promoCode,
            matchedText: "CODE42",
            normalizedText: "code42",
            atomOrdinal: 15,
            startTime: 21.0,
            endTime: 22.5,
            count: 2,
            firstTime: 21.0,
            lastTime: 45.0
        )
        let provenance: [AnchorRef] = [
            .fmConsensus(regionId: "region-alpha", consensusStrength: 0.82),
            .evidenceCatalog(entry: entry),
        ]
        let span = DecodedSpan(
            id: DecodedSpan.makeId(assetId: "asset-P", firstAtomOrdinal: 10, lastAtomOrdinal: 20),
            assetId: "asset-P",
            firstAtomOrdinal: 10,
            lastAtomOrdinal: 20,
            startTime: 20.0,
            endTime: 50.0,
            anchorProvenance: provenance
        )

        let data = try CorpusExporter.spanLine(span)
        let json = try decodeJSONObject(from: data)
        guard let provArray = json["anchorProvenance"] as? [[String: Any]] else {
            Issue.record("anchorProvenance not serialized as an array of objects")
            return
        }
        #expect(provArray.count == 2)

        // First entry: fmConsensus.
        #expect(provArray[0]["type"] as? String == "fmConsensus")
        #expect(provArray[0]["regionId"] as? String == "region-alpha")
        #expect(provArray[0]["consensusStrength"] as? Double == 0.82)

        // Second entry: evidenceCatalog wrapping an EvidenceEntry dictionary.
        #expect(provArray[1]["type"] as? String == "evidenceCatalog")
        guard let entryJSON = provArray[1]["entry"] as? [String: Any] else {
            Issue.record("evidenceCatalog.entry not serialized as a dictionary")
            return
        }
        #expect(entryJSON["evidenceRef"] as? Int == 7)
        #expect(entryJSON["category"] as? String == "promoCode")
        #expect(entryJSON["matchedText"] as? String == "CODE42")
        #expect(entryJSON["atomOrdinal"] as? Int == 15)
        #expect(entryJSON["count"] as? Int == 2)

        // Full round-trip: re-decode the serialized JSON into [AnchorRef] via
        // the same Codable adapter the persistence layer uses. If this fails,
        // downstream tooling would be broken.
        let re = try JSONEncoder().encode(provenance)
        let decoded = try JSONDecoder().decode([AnchorRef].self, from: re)
        #expect(decoded == provenance, "AnchorRef Codable adapter did not round-trip")
    }

    // MARK: - Decision-log pairing quarantine

    @Test("export never pairs a mutable sibling decision-log.jsonl")
    func decisionLogManifestURLNilWhenSiblingPresent() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-sibling-yes")
        let sibling = docs.appendingPathComponent("decision-log.jsonl")
        let siblingBytes = Data("{\"fake\":true}\n".utf8)
        try siblingBytes.write(to: sibling)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(
            result.decisionLogManifestURL == nil,
            "A live mutable decision log has no response-independent frozen projection and must never be paired"
        )
        #expect(
            try Data(contentsOf: sibling) == siblingBytes,
            "Export must leave the local decision log untouched"
        )
    }

    @Test("export: decisionLogManifestURL is nil when no sibling decision-log.jsonl exists")
    func decisionLogManifestURLNilWhenAbsent() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-sibling-no")
        // Deliberately no decision-log.jsonl written.
        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.decisionLogManifestURL == nil)
    }

    @Test(
        "durable explicit identity survives damaged source and suppresses corrections and live egress"
    )
    func damagedExplicitReceiptRemainsPrivate() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(
            prefix: "CorpusExport-damaged-private-receipt"
        )
        let sibling = docs.appendingPathComponent("decision-log.jsonl")
        let decisionBytes = Data("{\"private-learned\":true}\n".utf8)
        try decisionBytes.write(to: sibling)

        let assetId = "asset-damaged-private-receipt"
        let asset = makeTestAsset(id: assetId)
        let window = AdWindow(
            id: "window-damaged-private-receipt",
            analysisAssetId: assetId,
            startTime: 10,
            endTime: 40,
            confidence: 0.88,
            boundaryState: AdBoundaryState.acousticRefined.rawValue,
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "damaged-private",
            advertiser: "Private Durable Sponsor",
            product: nil,
            adDescription: nil,
            evidenceText: "Private durable evidence",
            evidenceStartTime: 10,
            metadataSource: "damaged-private",
            metadataConfidence: 0.7,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
        try await store.insertAsset(asset)
        try await store.insertAdWindow(window)
        let source = CorrectionSource.bannerAutoSkipDenied
        _ = try await store.appendCorrectionEvent(
            CorrectionEvent(
                id: "damaged-private-receipt",
                analysisAssetId: assetId,
                scope: CorrectionScope.exactTimeSpan(
                    assetId: assetId,
                    startTime: window.startTime,
                    endTime: window.endTime
                ).serialized,
                source: source,
                correctionType: source.kind.correctionType,
                targetRefs: CorrectionTargetRefs(
                    adWindowId: window.id,
                    adWindowIds: [window.id],
                    explicitFeedbackDetectionProjection:
                        ExplicitFeedbackDetectionProjection(window),
                    exactFeedbackSpan: ExactFeedbackSpan(
                        startTime: window.startTime,
                        endTime: window.endTime
                    )
                )
            )
        )
        try await store.execForTesting("""
            UPDATE correction_events
            SET source = 'future-unknown-private-route',
                targetRefsJSON = '{malformed'
            WHERE id = 'damaged-private-receipt'
            """)

        let loaded = try #require(
            try await store.loadCorrectionEvents(
                analysisAssetId: assetId
            ).first
        )
        #expect(loaded.source == nil)
        #expect(loaded.targetRefs == nil)
        #expect(loaded.persistedCorrectionIdentityKey?.isEmpty == false)
        #expect(loaded.isPrivateExplicitFeedbackReceipt)
        #expect(
            try await store.responseIndependentAdWindows(
                analysisAssetId: assetId
            ) == nil
        )

        let result = try await CorpusExporter.export(
            store: store,
            documentsURL: docs
        )
        #expect(result.correctionCount == 0)
        #expect(result.skippedCorrectionCount == 0)
        #expect(result.adWindowCount == 0)
        #expect(result.decisionLogManifestURL == nil)
        #expect(
            try Data(contentsOf: sibling) == decisionBytes,
            "The local decision log remains untouched; only pairing is withheld"
        )
        let content = try String(
            contentsOf: result.fileURL,
            encoding: .utf8
        )
        #expect(
            !content.contains(
                "\"id\":\"damaged-private-receipt\""
            )
        )
        #expect(!content.contains(window.id))
    }

    // MARK: - narl.2: shadow sidecar write

    /// End-to-end proof that `CorpusExporter.export` writes the sibling
    /// `shadow-decisions.jsonl` — and that the resulting file round-trips
    /// every row through `ShadowDecisionsExporter.parse`. Without this
    /// wiring the harness's corpus builder cannot replay `.allEnabled`
    /// FM evidence.
    @Test("export: writes shadow-decisions.jsonl sibling round-trippable via parser")
    func exportWritesShadowSidecarAndRoundTrips() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-shadow")

        // Seed a couple of shadow rows under a realistic config variant.
        let rowA = ShadowFMResponse(
            assetId: "asset-shadow-1",
            windowStart: 0, windowEnd: 10,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0xAA, 0xBB]),
            capturedAt: 1_700_000_000,
            capturedBy: .laneA,
            fmModelVersion: "fm-1.0"
        )
        let rowB = ShadowFMResponse(
            assetId: "asset-shadow-1",
            windowStart: 10, windowEnd: 20,
            configVariant: .allEnabledShadow,
            fmResponse: Data([0xCC]),
            capturedAt: 1_700_000_050,
            capturedBy: .laneB,
            fmModelVersion: "fm-1.0"
        )
        try await store.upsertShadowFMResponse(rowA)
        try await store.upsertShadowFMResponse(rowB)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)

        let shadow = try #require(result.shadowManifestURL)
        #expect(result.shadowRowCount == 2)
        #expect(shadow.lastPathComponent == "shadow-decisions.jsonl")

        // Round-trip every row through the exporter's parser.
        let parsed = try ShadowDecisionsExporter.parseAll(fileURL: shadow)
        #expect(parsed.count == 2)
        #expect(Set(parsed) == Set([rowA, rowB]))
    }

    @Test("export: shadow sidecar with no rows is a zero-row file (not missing)")
    func exportShadowSidecarIsEmptyFileWhenStoreHasNoRows() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-shadow-empty")

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)

        let shadow = try #require(result.shadowManifestURL)
        #expect(result.shadowRowCount == 0)
        // File exists even though it's empty.
        let data = try Data(contentsOf: shadow)
        #expect(data.isEmpty)
    }

    // MARK: - G5: SQL-error path tolerated via test seam

    @Test("export: a throwing fetchDecodedSpans for one asset is logged; other assets' records still serialize")
    func exportToleratesFetchDecodedSpansFailure() async throws {
        // Arrange a mock source with two assets. The second asset's span fetch
        // throws; the first asset must still emit its records and the export
        // must return successfully.
        let docs = try makeTempDir(prefix: "CorpusExport-sqlerr-spans")
        let a1 = makeTestAsset(id: "asset-ok")
        let a2 = makeTestAsset(id: "asset-sqlerr")
        let span1 = makeSpan(assetId: "asset-ok", firstOrdinal: 0, lastOrdinal: 10)
        let source = FailingSource(
            assets: [a1, a2],
            spans: ["asset-ok": [span1]],
            events: ["asset-ok": [], "asset-sqlerr": []],
            failSpansFor: ["asset-sqlerr"],
            failEventsFor: []
        )

        let result = try await CorpusExporter.export(store: source, documentsURL: docs)
        #expect(result.assetCount == 2, "both asset rows must serialize even though one span-fetch failed")
        #expect(result.spanCount == 1, "only asset-ok's single span survives; asset-sqlerr's fetch threw")

        let records = try parseJSONL(at: result.fileURL)
        let assets = records.filter { ($0["type"] as? String) == "asset" }.compactMap { $0["analysisAssetId"] as? String }
        #expect(Set(assets) == ["asset-ok", "asset-sqlerr"])
        let decisions = records.filter { ($0["type"] as? String) == "decision" }
        #expect(decisions.count == 1)
        #expect(decisions.first?["analysisAssetId"] as? String == "asset-ok")
    }

    @Test("export: a throwing loadCorrectionEvents for one asset is logged; other assets' records still serialize")
    func exportToleratesLoadCorrectionEventsFailure() async throws {
        let docs = try makeTempDir(prefix: "CorpusExport-sqlerr-events")
        let a1 = makeTestAsset(id: "asset-ok")
        let a2 = makeTestAsset(id: "asset-corr-err")
        let scope = CorrectionScope.exactSpan(assetId: "asset-ok", ordinalRange: 1...5)
        let goodEvent = CorrectionEvent(
            id: "good",
            analysisAssetId: "asset-ok",
            scope: scope.serialized,
            createdAt: 1_700_000_000,
            source: .manualVeto
        )
        let source = FailingSource(
            assets: [a1, a2],
            spans: ["asset-ok": [], "asset-corr-err": []],
            events: ["asset-ok": [goodEvent], "asset-corr-err": []],
            failSpansFor: [],
            failEventsFor: ["asset-corr-err"]
        )

        let result = try await CorpusExporter.export(store: source, documentsURL: docs)
        #expect(result.assetCount == 2)
        #expect(result.correctionCount == 1, "asset-ok's correction must survive the sibling's load failure")

        let records = try parseJSONL(at: result.fileURL)
        let corrections = records.filter { ($0["type"] as? String) == "correction" }
        #expect(corrections.count == 1)
        #expect(corrections.first?["analysisAssetId"] as? String == "asset-ok")
    }

    @Test("export: if fetchAllAssets throws, no partial corpus-export file remains in Documents/")
    func exportCleansUpOnEarlyThrow() async throws {
        let docs = try makeTempDir(prefix: "CorpusExport-cleanup")
        let source = FailingSource(
            assets: [],
            spans: [:],
            events: [:],
            failSpansFor: [],
            failEventsFor: [],
            failAllAssets: true
        )
        do {
            _ = try await CorpusExporter.export(store: source, documentsURL: docs)
            Issue.record("export should have thrown")
        } catch {
            // Expected: SimulatedSQLError propagated from fetchAllAssets.
        }
        let contents = try FileManager.default.contentsOfDirectory(atPath: docs.path)
        let orphans = contents.filter { $0.hasPrefix("corpus-export.") }
        #expect(orphans.isEmpty, "Documents/ must not accumulate partial exports; found: \(orphans)")
    }

    // MARK: - Test helpers

    private func makeSpan(
        assetId: String,
        startTime: Double = 10.0,
        endTime: Double = 40.0,
        firstOrdinal: Int = 100,
        lastOrdinal: Int = 200
    ) -> DecodedSpan {
        DecodedSpan(
            id: DecodedSpan.makeId(assetId: assetId, firstAtomOrdinal: firstOrdinal, lastAtomOrdinal: lastOrdinal),
            assetId: assetId,
            firstAtomOrdinal: firstOrdinal,
            lastAtomOrdinal: lastOrdinal,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: []
        )
    }

    private func decodeJSONObject(from data: Data) throws -> [String: Any] {
        guard let obj = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw NSError(domain: "CorpusExporterTests", code: 1,
                          userInfo: [NSLocalizedDescriptionKey: "not a JSON object"])
        }
        return obj
    }

    private func parseJSONL(at url: URL) throws -> [[String: Any]] {
        let text = try String(contentsOf: url, encoding: .utf8)
        var out: [[String: Any]] = []
        for line in text.split(whereSeparator: { $0.isNewline }) {
            let lineStr = String(line)
            guard !lineStr.isEmpty else { continue }
            guard let data = lineStr.data(using: .utf8) else {
                throw NSError(domain: "CorpusExporterTests", code: 2,
                              userInfo: [NSLocalizedDescriptionKey: "bad utf8: \(lineStr)"])
            }
            guard let json = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                throw NSError(domain: "CorpusExporterTests", code: 3,
                              userInfo: [NSLocalizedDescriptionKey: "not a JSON object: \(lineStr)"])
            }
            out.append(json)
        }
        return out
    }
}

// MARK: - FailingSource (G5 test seam)

/// In-memory `CorpusExportSource` that can be configured to throw from
/// `fetchDecodedSpans` or `loadCorrectionEvents` for specific asset IDs.
/// Used to exercise the exporter's SQL-error tolerance without corrupting
/// a real sqlite file.
private struct FailingSource: CorpusExportSource {
    struct SimulatedSQLError: Error, CustomStringConvertible {
        let method: String
        let assetId: String
        var description: String { "SimulatedSQLError(\(method), asset=\(assetId))" }
    }

    let assets: [AnalysisAsset]
    let spans: [String: [DecodedSpan]]
    let events: [String: [CorrectionEvent]]
    let windows: [String: [AdWindow]]
    let failSpansFor: Set<String>
    let failEventsFor: Set<String>
    let failAllAssets: Bool

    init(
        assets: [AnalysisAsset],
        spans: [String: [DecodedSpan]],
        events: [String: [CorrectionEvent]],
        windows: [String: [AdWindow]] = [:],
        failSpansFor: Set<String>,
        failEventsFor: Set<String>,
        failAllAssets: Bool = false
    ) {
        self.assets = assets
        self.spans = spans
        self.events = events
        self.windows = windows
        self.failSpansFor = failSpansFor
        self.failEventsFor = failEventsFor
        self.failAllAssets = failAllAssets
    }

    func fetchAllAssets() async throws -> [AnalysisAsset] {
        if failAllAssets {
            throw SimulatedSQLError(method: "fetchAllAssets", assetId: "")
        }
        return assets
    }

    func fetchDecodedSpans(assetId: String) async throws -> [DecodedSpan] {
        if failSpansFor.contains(assetId) {
            throw SimulatedSQLError(method: "fetchDecodedSpans", assetId: assetId)
        }
        return spans[assetId] ?? []
    }

    /// playhead-epfk: `FailingSource` doesn't model ad-window fixtures —
    /// the existing exporter tests only cover the asset / span / correction
    /// paths. Returning an empty array exercises the "no ad_windows for
    /// this asset" branch of the per-asset loop without changing any
    /// existing test's expectations. The dedicated end-to-end coverage
    /// for the new `ad_window` record lives in
    /// `AdCatalogStoreMatchTelemetryTests`.
    func fetchAdWindows(assetId: String) async throws -> [AdWindow] {
        windows[assetId] ?? []
    }

    func loadCorrectionEvents(analysisAssetId: String) async throws -> [CorrectionEvent] {
        if failEventsFor.contains(analysisAssetId) {
            throw SimulatedSQLError(method: "loadCorrectionEvents", assetId: analysisAssetId)
        }
        return events[analysisAssetId] ?? []
    }

    /// `FailingSource` doesn't model podcastId lookups — the exporter
    /// tolerates `nil` (emits JSON null) so returning nil here exercises
    /// the "podcastId absent" JSONL path.
    func fetchPodcastId(forEpisodeId episodeId: String) async throws -> String? {
        return nil
    }

    /// playhead-i9dj: no profile rows in the failing-source fixtures.
    /// The exporter tolerates `nil` (emits explicit JSON null for
    /// `podcastTitle`) so returning nil here exercises the
    /// "podcastTitle absent" JSONL path.
    func fetchPodcastProfile(podcastId: String) async throws -> PodcastProfile? {
        return nil
    }

    /// playhead-narl.2: no shadow rows in the failing-source fixtures. The
    /// sidecar exporter still writes a zero-row `shadow-decisions.jsonl`
    /// so the corpus-export path exercises end-to-end.
    func allShadowFMResponses() async throws -> [ShadowFMResponse] {
        return []
    }

    /// playhead-q45f.1: no listen-rewind rows in the failing-source
    /// fixtures. Returns empty so the exporter exercises the
    /// "no listen_rewind events for this asset" branch without any
    /// SQL setup.
    func fetchListenRewinds(forAssetId assetId: String) async throws -> [AdListenRewindRow] {
        return []
    }
}

// MARK: - ListenRewindSource (q45f.1 test seam)

/// In-memory `CorpusExportSource` that returns a configured map of
/// `[assetId: [AdListenRewindRow]]` for `fetchListenRewinds(forAssetId:)`.
/// All other methods return empty/nil so the exporter exercises only the
/// listen-rewind emission path.
private struct ListenRewindSource: CorpusExportSource {
    let assets: [AnalysisAsset]
    let listenRewinds: [String: [AdListenRewindRow]]

    func fetchAllAssets() async throws -> [AnalysisAsset] { assets }
    func fetchDecodedSpans(assetId: String) async throws -> [DecodedSpan] { [] }
    func fetchAdWindows(assetId: String) async throws -> [AdWindow] { [] }
    func loadCorrectionEvents(analysisAssetId: String) async throws -> [CorrectionEvent] { [] }
    func fetchPodcastId(forEpisodeId episodeId: String) async throws -> String? { nil }
    func fetchPodcastProfile(podcastId: String) async throws -> PodcastProfile? { nil }
    func allShadowFMResponses() async throws -> [ShadowFMResponse] { [] }
    func fetchListenRewinds(forAssetId assetId: String) async throws -> [AdListenRewindRow] {
        listenRewinds[assetId] ?? []
    }
    func fetchResponseIndependentAssetEgressProjection(
        analysisAssetId: String
    ) async throws -> ResponseIndependentAssetEgressProjection? {
        guard let asset = assets.first(where: {
            $0.id == analysisAssetId
        }) else {
            return nil
        }
        return ResponseIndependentAssetEgressProjection(
            windows: [],
            decodedSpans: [],
            listenRewinds: listenRewinds[analysisAssetId] ?? [],
            confirmedAdCoverageEndTime:
                asset.confirmedAdCoverageEndTime
        )
    }
}

#endif
