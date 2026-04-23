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
            "featureCoverageEndTime",
            "fastTranscriptCoverageEndTime",
            "confirmedAdCoverageEndTime",
            // playhead-gtt9.8: `terminalReason` is the richer-terminal
            // diagnostic the classifier persisted into
            // `analysis_assets.terminalReason`. Nullable on pre-gtt9.8
            // rows and on sessions still in flight.
            "terminalReason",
        ] {
            #expect(json.keys.contains(key), "\(key) must be present as a key")
            #expect(json[key] is NSNull, "\(key) must serialize as null for a minimal asset, not omitted or empty-string")
        }
    }

    @Test("asset record carries podcastId when threaded through from the store (HIGH-3)")
    func assetRecordPodcastIdPassthrough() throws {
        let asset = makeTestAsset(id: "asset-H3")
        let data = try CorpusExporter.assetLine(asset, podcastId: "pod-abc-123")
        let json = try decodeJSONObject(from: data)
        #expect(json["podcastId"] as? String == "pod-abc-123")
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
        let base = Date(timeIntervalSince1970: 1_700_000_000)
        let a = try await CorpusExporter.export(store: store, documentsURL: docs, now: base)
        let b = try await CorpusExporter.export(store: store, documentsURL: docs, now: base.addingTimeInterval(0.001))

        #expect(a.fileURL != b.fileURL, "two exports produced the same filename: \(a.fileURL.lastPathComponent)")
        #expect(FileManager.default.fileExists(atPath: a.fileURL.path))
        #expect(FileManager.default.fileExists(atPath: b.fileURL.path))

        // Enumerate the docs dir and confirm two corpus-export.*.jsonl files.
        let contents = try FileManager.default.contentsOfDirectory(atPath: docs.path)
        let exports = contents.filter { $0.hasPrefix("corpus-export.") && $0.hasSuffix(".jsonl") }
        #expect(exports.count == 2, "expected 2 corpus-export files, got \(exports)")
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

    // MARK: - G4: decisionLogManifestURL pairing

    @Test("export: decisionLogManifestURL is surfaced when decision-log.jsonl exists as a sibling")
    func decisionLogManifestURLSurfacedWhenPresent() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-sibling-yes")
        let sibling = docs.appendingPathComponent("decision-log.jsonl")
        try Data("{\"fake\":true}\n".utf8).write(to: sibling)

        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.decisionLogManifestURL == sibling,
                "expected \(sibling.path), got \(String(describing: result.decisionLogManifestURL?.path))")
    }

    @Test("export: decisionLogManifestURL is nil when no sibling decision-log.jsonl exists")
    func decisionLogManifestURLNilWhenAbsent() async throws {
        let store = try await makeTestStore()
        let docs = try makeTempDir(prefix: "CorpusExport-sibling-no")
        // Deliberately no decision-log.jsonl written.
        let result = try await CorpusExporter.export(store: store, documentsURL: docs)
        #expect(result.decisionLogManifestURL == nil)
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
    let failSpansFor: Set<String>
    let failEventsFor: Set<String>
    let failAllAssets: Bool

    init(
        assets: [AnalysisAsset],
        spans: [String: [DecodedSpan]],
        events: [String: [CorrectionEvent]],
        failSpansFor: Set<String>,
        failEventsFor: Set<String>,
        failAllAssets: Bool = false
    ) {
        self.assets = assets
        self.spans = spans
        self.events = events
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

    /// playhead-narl.2: no shadow rows in the failing-source fixtures. The
    /// sidecar exporter still writes a zero-row `shadow-decisions.jsonl`
    /// so the corpus-export path exercises end-to-end.
    func allShadowFMResponses() async throws -> [ShadowFMResponse] {
        return []
    }
}

#endif
