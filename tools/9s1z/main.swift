// playhead-9s1z MEASUREMENT ONLY — recompose one device pull under options
// (i) shipped / (ii) sameVersion / (iii) sameVersionDropBoth, using the REAL
// SemanticSweepMarkComposer source. Emits JSON on stdout.
//
// Dan has chosen no option. Nothing here changes shipped behaviour.
import Foundation
import SQLite3

func die(_ message: String) -> Never {
    FileHandle.standardError.write(Data((message + "\n").utf8))
    exit(1)
}

/// STRICT decode: an unrecognised persisted raw value stops the run rather than
/// resolving to a default. A default here would silently re-label rows, which
/// is the defect class this bead is about.
func strict<T: RawRepresentable>(_ value: T?, _ what: String, _ raw: String?) -> T {
    guard let value else { die("unknown \(what): \(raw ?? "nil")") }
    return value
}

// MARK: - SQLite

final class DB {
    private var handle: OpaquePointer?
    init(path: String) {
        guard sqlite3_open_v2(path, &handle, SQLITE_OPEN_READONLY, nil) == SQLITE_OK else {
            die("cannot open \(path)")
        }
    }
    func rows(_ sql: String) -> [[String: Any]] {
        var stmt: OpaquePointer?
        guard sqlite3_prepare_v2(handle, sql, -1, &stmt, nil) == SQLITE_OK else {
            die("prepare failed: \(String(cString: sqlite3_errmsg(handle)))")
        }
        defer { sqlite3_finalize(stmt) }
        var out: [[String: Any]] = []
        let n = sqlite3_column_count(stmt)
        while sqlite3_step(stmt) == SQLITE_ROW {
            var row: [String: Any] = [:]
            for i in 0..<n {
                let name = String(cString: sqlite3_column_name(stmt, i))
                switch sqlite3_column_type(stmt, i) {
                case SQLITE_NULL: break
                case SQLITE_INTEGER: row[name] = Int(sqlite3_column_int64(stmt, i))
                case SQLITE_FLOAT: row[name] = sqlite3_column_double(stmt, i)
                default:
                    if let c = sqlite3_column_text(stmt, i) { row[name] = String(cString: c) }
                }
            }
            out.append(row)
        }
        return out
    }
}

// MARK: - Load

let pull = CommandLine.arguments.count > 1
    ? CommandLine.arguments[1]
    : "/Users/dabrams/playhead-gate-artifacts/device-pulls/2026-08-19-t4-correction/work/analysis.sqlite"
let db = DB(path: pull)

func str(_ r: [String: Any], _ k: String) -> String? { r[k] as? String }
func dbl(_ r: [String: Any], _ k: String) -> Double? {
    if let d = r[k] as? Double { return d }
    if let i = r[k] as? Int { return Double(i) }
    return nil
}
func int(_ r: [String: Any], _ k: String) -> Int? {
    if let i = r[k] as? Int { return i }
    if let d = r[k] as? Double { return Int(d) }
    return nil
}

let scanRowsRaw = db.rows("SELECT * FROM semantic_scan_results")
var scanRowsByAsset: [String: [SemanticScanResult]] = [:]
for r in scanRowsRaw {
    guard let assetId = str(r, "analysisAssetId") else { continue }
    let row = SemanticScanResult(
        id: str(r, "id") ?? "",
        analysisAssetId: assetId,
        windowFirstAtomOrdinal: int(r, "windowFirstAtomOrdinal") ?? 0,
        windowLastAtomOrdinal: int(r, "windowLastAtomOrdinal") ?? 0,
        windowStartTime: dbl(r, "windowStartTime") ?? 0,
        windowEndTime: dbl(r, "windowEndTime") ?? 0,
        scanPass: str(r, "scanPass") ?? "",
        // STRICT: an unrecognised persisted value is a measurement fault, not a
        // default. Inventing `.abstain` or `.degraded` here would be exactly the
        // defect class this bead is about — a value that names one thing read as
        // though it named another.
        transcriptQuality: strict(TranscriptQuality(rawValue: str(r, "transcriptQuality") ?? ""),
                                  "transcriptQuality", str(r, "transcriptQuality")),
        disposition: strict(CoarseDisposition(rawValue: str(r, "disposition") ?? ""),
                            "disposition", str(r, "disposition")),
        spansJSON: str(r, "spansJSON") ?? "[]",
        status: strict(SemanticScanStatus(rawValue: str(r, "status") ?? ""),
                       "status", str(r, "status")),
        attemptCount: int(r, "attemptCount") ?? 0,
        errorContext: str(r, "errorContext"),
        inputTokenCount: int(r, "inputTokenCount"),
        outputTokenCount: int(r, "outputTokenCount"),
        latencyMs: dbl(r, "latencyMs"),
        suspendingLatencyMs: dbl(r, "suspendingLatencyMs"),
        daemonPeersAtStart: int(r, "daemonPeersAtStart"),
        prewarmHit: int(r, "prewarmHit").map { $0 != 0 },
        scanCohortJSON: str(r, "scanCohortJSON") ?? "{}",
        transcriptVersion: str(r, "transcriptVersion") ?? "",
        runMode: strict(SemanticScanPhase(rawValue: str(r, "runMode") ?? ""),
                        "runMode", str(r, "runMode")),
        jobPhase: str(r, "jobPhase") ?? "shadow",
        createdAt: dbl(r, "createdAt"),
        scenePhase: str(r, "scenePhase").flatMap { ScanScenePhase(rawValue: $0) },
        firstAttemptAt: dbl(r, "firstAttemptAt"),
        lastAttemptAt: dbl(r, "lastAttemptAt"),
        observedStatusesCSV: str(r, "observedStatuses"),
        latencyMsTotal: dbl(r, "latencyMsTotal"),
        latencyMsMax: dbl(r, "latencyMsMax"),
        latencySampleCount: int(r, "latencySampleCount")
    )
    scanRowsByAsset[assetId, default: []].append(row)
}

let windowRowsRaw = db.rows("SELECT * FROM ad_windows")
var windowsByAsset: [String: [AdWindow]] = [:]
for r in windowRowsRaw {
    guard let assetId = str(r, "analysisAssetId") else { continue }
    let w = AdWindow(
        id: str(r, "id") ?? "",
        analysisAssetId: assetId,
        startTime: dbl(r, "startTime") ?? 0,
        endTime: dbl(r, "endTime") ?? 0,
        confidence: dbl(r, "confidence") ?? 0,
        skipConfidence: dbl(r, "skipConfidence"),
        boundaryState: str(r, "boundaryState") ?? "lexical",
        decisionState: str(r, "decisionState") ?? "candidate",
        detectorVersion: str(r, "detectorVersion") ?? "",
        advertiser: str(r, "advertiser"),
        product: str(r, "product"),
        adDescription: str(r, "adDescription"),
        evidenceText: str(r, "evidenceText"),
        evidenceStartTime: dbl(r, "evidenceStartTime"),
        metadataSource: str(r, "metadataSource") ?? "none",
        metadataConfidence: dbl(r, "metadataConfidence"),
        metadataPromptVersion: str(r, "metadataPromptVersion"),
        wasSkipped: (int(r, "wasSkipped") ?? 0) != 0,
        userDismissedBanner: (int(r, "userDismissedBanner") ?? 0) != 0,
        evidenceSources: str(r, "evidenceSources"),
        eligibilityGate: str(r, "eligibilityGate"),
        catalogStoreMatchSimilarity: dbl(r, "catalogStoreMatchSimilarity"),
        catalogFingerprintVersion: int(r, "catalogFingerprintVersion"),
        catalogMatchedEntryId: str(r, "catalogMatchedEntryId"),
        catalogMatchedShowId: str(r, "catalogMatchedShowId"),
        catalogMatchedLearningSource: str(r, "catalogMatchedLearningSource"),
        catalogMatchedLearningLifecycle: str(r, "catalogMatchedLearningLifecycle"),
        startEdgeAnchor: str(r, "startEdgeAnchor") ?? "unanchored",
        endEdgeAnchor: str(r, "endEdgeAnchor") ?? "unanchored"
    )
    windowsByAsset[assetId, default: []].append(w)
}

var durationByAsset: [String: Double] = [:]
for r in db.rows("SELECT id, episodeDurationSec FROM analysis_assets") {
    if let id = str(r, "id"), let d = dbl(r, "episodeDurationSec") { durationByAsset[id] = d }
}

// MARK: - Cross-version census (re-derivation of the bead's own figures)

var crossVersionPairings = 0
var pairingsTotal = 0
for (_, rows) in scanRowsByAsset {
    let admissible = rows.filter(SemanticSweepMarkComposer.isPresenceVerdict)
    let refinements = admissible.filter { $0.scanPass == SemanticSweepMarkComposer.refinementScanPass }
    let coarse = admissible.filter { $0.scanPass != SemanticSweepMarkComposer.refinementScanPass }
    for window in coarse {
        for refinement in refinements
        where refinement.windowStartTime < window.windowEndTime
            && refinement.windowEndTime > window.windowStartTime {
            pairingsTotal += 1
            if refinement.transcriptVersion != window.transcriptVersion { crossVersionPairings += 1 }
        }
    }
}

// MARK: - Recompose

struct Mark: Codable {
    var assetId: String
    var start: Double
    var end: Double
    var confidence: Double
    var duration: Double { end - start }
}

func recompose(policy: SemanticSweepMarkComposer.VersionScopePolicy9s1z,
               skipLocalise: Bool = false) -> ([Mark], Int, Int) {
    SemanticSweepMarkComposer.versionScopePolicy9s1z = policy
    SemanticSweepMarkComposer.skipLocalise9s1z = skipLocalise
    SemanticSweepMarkComposer.measurement9s1zSuppressedCoarseWindows = 0
    SemanticSweepMarkComposer.measurement9s1zSuppressedOrphanedRefinements = 0
    var marks: [Mark] = []
    for assetId in scanRowsByAsset.keys.sorted() {
        let produced = SemanticSweepMarkComposer.compose(
            scanRows: scanRowsByAsset[assetId] ?? [],
            existingWindows: windowsByAsset[assetId] ?? [],
            supportLines: nil,
            analysisAssetId: assetId
        )
        for w in produced {
            marks.append(Mark(assetId: assetId, start: w.startTime, end: w.endTime, confidence: w.confidence))
        }
    }
    SemanticSweepMarkComposer.versionScopePolicy9s1z = .shipped
    SemanticSweepMarkComposer.skipLocalise9s1z = false
    return (marks.sorted { ($0.assetId, $0.start) < ($1.assetId, $1.start) },
            SemanticSweepMarkComposer.measurement9s1zSuppressedCoarseWindows,
            SemanticSweepMarkComposer.measurement9s1zSuppressedOrphanedRefinements)
}

// MARK: - How many marks rest on presence rows from more than one version

func backingVersionSpread(skipLocalise: Bool) -> [String: Int] {
    // For each shipped mark, the set of transcriptVersions of the presence rows
    // whose window OVERLAPS it. Same predicate the bead used: "rest on presence
    // rows from more than one version".
    var histogram: [String: Int] = [:]
    let (shipped, _, _) = recompose(policy: .shipped, skipLocalise: skipLocalise)
    for mark in shipped {
        let rows = (scanRowsByAsset[mark.assetId] ?? []).filter(SemanticSweepMarkComposer.isPresenceVerdict)
        var versions = Set<String>()
        for row in rows where row.windowStartTime < mark.end && row.windowEndTime > mark.start {
            versions.insert(row.transcriptVersion)
        }
        histogram[String(versions.count), default: 0] += 1
    }
    return histogram
}

// MARK: - Coverage arithmetic

struct Interval { var start: Double; var end: Double }

func normalise(_ ivs: [Interval]) -> [Interval] {
    let sorted = ivs.sorted { $0.start < $1.start }
    var out: [Interval] = []
    for iv in sorted {
        if var last = out.last, iv.start <= last.end {
            last.end = max(last.end, iv.end)
            out[out.count - 1] = last
        } else { out.append(iv) }
    }
    return out
}

func subtract(_ a: [Interval], _ b: [Interval]) -> [Interval] {
    var out: [Interval] = []
    let bb = normalise(b)
    for iv in normalise(a) {
        var cursor = iv.start
        for cut in bb where cut.end > iv.start && cut.start < iv.end {
            if cut.start > cursor { out.append(Interval(start: cursor, end: min(cut.start, iv.end))) }
            cursor = max(cursor, cut.end)
        }
        if cursor < iv.end { out.append(Interval(start: cursor, end: iv.end)) }
    }
    return out.filter { $0.end > $0.start }
}

func total(_ ivs: [Interval]) -> Double { ivs.reduce(0) { $0 + ($1.end - $1.start) } }

// MARK: - Report

var report: [String: Any] = [:]
report["pull"] = pull
report["assets"] = scanRowsByAsset.keys.count
report["semanticScanRows"] = scanRowsRaw.count
report["coarseContainsAdPresenceRows"] = scanRowsRaw.filter {
    str($0, "scanPass") == "passA" && str($0, "disposition") == "containsAd"
}.count
report["coarseXpassBOverlapPairings_total"] = pairingsTotal
report["coarseXpassBOverlapPairings_crossVersion"] = crossVersionPairings
report["markBackingVersionCountHistogram_stage6On"] = backingVersionSpread(skipLocalise: false)
report["markBackingVersionCountHistogram_stage6Off"] = backingVersionSpread(skipLocalise: true)

let boundaryToleranceSeconds = 30.0

/// Everything the three options produce, under ONE stage-6 configuration.
///
/// Two configurations are reported because they answer different questions.
/// `stage6On` is what the code in this tree does. `stage6Off` is what
/// playhead-kg6i's model did — its quoted 79 marks / 8048.8 s reproduces there
/// to the digit and does NOT reproduce with stage 6 on — so it is the basis the
/// bead's own figures are stated over.
func measure(skipLocalise: Bool) -> [String: Any] {
    var optionOut: [String: Any] = [:]
    var marksByOption: [String: [Mark]] = [:]
    for (name, policy) in [
        ("i_shipped", SemanticSweepMarkComposer.VersionScopePolicy9s1z.shipped),
        ("ii_sameVersion", .sameVersion),
        ("iii_sameVersionDropBoth", .sameVersionDropBoth)
    ] {
        let (marks, suppressedCoarse, suppressedOrphans) = recompose(policy: policy, skipLocalise: skipLocalise)
        marksByOption[name] = marks
        var perAsset: [String: Any] = [:]
        for assetId in scanRowsByAsset.keys.sorted() {
            let m = marks.filter { $0.assetId == assetId }
            perAsset[String(assetId.prefix(8))] = ["marks": m.count, "seconds": m.reduce(0) { $0 + $1.duration }]
        }
        optionOut[name] = [
            "marks": marks.count,
            "seconds": marks.reduce(0) { $0 + $1.duration },
            "suppressedCoarseWindows": suppressedCoarse,
            "suppressedOrphanedRefinements": suppressedOrphans,
            "perAsset": perAsset,
            "extents": marks.map { ["asset": String($0.assetId.prefix(8)), "start": $0.start,
                                    "end": $0.end, "conf": $0.confidence] }
        ]
    }

    var diffs: [String: Any] = [:]
    for name in ["ii_sameVersion", "iii_sameVersionDropBoth"] {
        var addedInner = 0.0, addedOuter = 0.0, removed = 0.0
        var addedRuns: [[String: Any]] = [], removedRuns: [[String: Any]] = []
        var marksGone: [[String: Any]] = [], marksNew: [[String: Any]] = []
        for assetId in scanRowsByAsset.keys.sorted() {
            let baseMarks = (marksByOption["i_shipped"] ?? []).filter { $0.assetId == assetId }
            let altMarks = (marksByOption[name] ?? []).filter { $0.assetId == assetId }
            let base = baseMarks.map { Interval(start: $0.start, end: $0.end) }
            let alt = altMarks.map { Interval(start: $0.start, end: $0.end) }
            let duration = durationByAsset[assetId] ?? .infinity
            for run in subtract(alt, base) {
                // OUTER = the newly-claimed audio abuts the episode head or tail,
                // where there is no show on the far side to lose. Everything else
                // is INNER: show sits on the other side of that edge, and that is
                // the audio a listener loses inside a banner.
                let isOuter = run.start <= boundaryToleranceSeconds
                    || run.end >= duration - boundaryToleranceSeconds
                if isOuter { addedOuter += run.end - run.start } else { addedInner += run.end - run.start }
                addedRuns.append([
                    "asset": String(assetId.prefix(8)), "start": run.start, "end": run.end,
                    "seconds": run.end - run.start, "edge": isOuter ? "outer" : "inner",
                    "secondsFromEpisodeStart": run.start,
                    "secondsFromEpisodeEnd": duration.isFinite ? duration - run.end : -1
                ])
            }
            for run in subtract(base, alt) {
                removed += run.end - run.start
                removedRuns.append([
                    "asset": String(assetId.prefix(8)), "start": run.start, "end": run.end,
                    "seconds": run.end - run.start
                ])
            }
            // Mark-level identity, so "which marks disappear entirely" is answered
            // by name and not inferred from a seconds total.
            for m in baseMarks where !altMarks.contains(where: {
                abs($0.start - m.start) < 1e-6 && abs($0.end - m.end) < 1e-6 }) {
                // Say how much of this mark's audio no longer sits under ANY
                // mark in this option, rather than a boolean: "the mark is gone"
                // and "the audio is gone" are different claims and only the
                // second is reach lost.
                let orphanedSeconds = total(subtract([Interval(start: m.start, end: m.end)], alt))
                marksGone.append(["asset": String(assetId.prefix(8)), "start": m.start, "end": m.end,
                                  "seconds": m.duration,
                                  "secondsNoLongerUnderAnyMark": orphanedSeconds])
            }
            for m in altMarks where !baseMarks.contains(where: {
                abs($0.start - m.start) < 1e-6 && abs($0.end - m.end) < 1e-6 }) {
                marksNew.append(["asset": String(assetId.prefix(8)), "start": m.start, "end": m.end,
                                 "seconds": m.duration])
            }
        }
        diffs[name] = [
            "addedSecondsInner": addedInner,
            "addedSecondsOuter": addedOuter,
            "removedSeconds": removed,
            "netSeconds": addedInner + addedOuter - removed,
            "addedRuns": addedRuns,
            "removedRuns": removedRuns,
            "marksWithNoIdenticalTwinInThisOption": marksGone,
            "marksThisOptionIntroduces": marksNew
        ]
    }

    // Fidelity: does this configuration reproduce what the DEVICE actually wrote?
    var persisted: [Mark] = []
    for (assetId, ws) in windowsByAsset {
        for w in ws where w.detectorVersion == SemanticSweepMarkComposer.detectorVersion {
            persisted.append(Mark(assetId: assetId, start: w.startTime, end: w.endTime, confidence: w.confidence))
        }
    }
    let shippedMarks = marksByOption["i_shipped"] ?? []
    func matches(_ a: Mark, _ b: Mark) -> Bool {
        a.assetId == b.assetId && abs(a.start - b.start) < 1e-6 && abs(a.end - b.end) < 1e-6
    }
    let validation: [String: Any] = [
        "persistedSweepRows": persisted.count,
        "recomposedShippedMarks": shippedMarks.count,
        "recomposedWithExactPersistedTwin": shippedMarks.filter { m in persisted.contains { matches(m, $0) } }.count,
        "persistedWithNoRecomposedTwin": persisted.filter { p in !shippedMarks.contains { matches(p, $0) } }
            .sorted { ($0.assetId, $0.start) < ($1.assetId, $1.start) }
            .map { ["asset": String($0.assetId.prefix(8)), "start": $0.start, "end": $0.end] }
    ]
    return ["options": optionOut, "diffVsShipped": diffs, "validation": validation]
}

report["stage6On_thisTree"] = measure(skipLocalise: false)
report["stage6Off_theBeadsBasis"] = measure(skipLocalise: true)

// MARK: - Stage 1-2 divergence, and the mechanism behind each option's diff

// The composer's own stages, called directly, so a difference can be attributed
// to the stage that made it rather than inferred from the final mark set.
func stageTrace(policy: SemanticSweepMarkComposer.VersionScopePolicy9s1z, assetId: String)
    -> (presence: [[String: Any]], merged: [[String: Any]], afterWidth: [[String: Any]], marks: [[String: Any]]) {
    SemanticSweepMarkComposer.versionScopePolicy9s1z = policy
    let rows = scanRowsByAsset[assetId] ?? []
    let presence = SemanticSweepMarkComposer.presenceExtents(rows)
    let merged = SemanticSweepMarkComposer.mergeExtents(
        presence, barredBy: SemanticSweepMarkComposer.clearedSpans(in: rows))
    let anchors = SemanticSweepMarkComposer.provenAnchorEdges(in: windowsByAsset[assetId] ?? [])
    let afterWidth = merged
        .map { SemanticSweepMarkComposer.clip($0, toAnchors: anchors) }
        .filter { $0.duration <= SemanticSweepMarkComposer.maximumMarkDurationSeconds }
    let produced = SemanticSweepMarkComposer.compose(
        scanRows: rows, existingWindows: windowsByAsset[assetId] ?? [],
        supportLines: nil, analysisAssetId: assetId)
    SemanticSweepMarkComposer.versionScopePolicy9s1z = .shipped
    func f(_ e: [SemanticSweepMarkComposer.Extent]) -> [[String: Any]] {
        e.sorted { $0.start < $1.start }.map { ["start": $0.start, "end": $0.end] }
    }
    return (f(presence), f(merged), f(afterWidth),
            produced.sorted { $0.startTime < $1.startTime }.map { ["start": $0.startTime, "end": $0.endTime] })
}

var stageTraces: [String: Any] = [:]
for assetId in scanRowsByAsset.keys.sorted() {
    guard scanRowsByAsset[assetId] != nil else { continue }
    var byPolicy: [String: Any] = [:]
    for (name, policy) in [
        ("i_shipped", SemanticSweepMarkComposer.VersionScopePolicy9s1z.shipped),
        ("ii_sameVersion", .sameVersion),
        ("iii_sameVersionDropBoth", .sameVersionDropBoth)
    ] {
        let t = stageTrace(policy: policy, assetId: assetId)
        byPolicy[name] = ["presence": t.presence, "merged": t.merged,
                          "afterClipAndWidth": t.afterWidth, "marks": t.marks]
    }
    byPolicy["blockingWindows"] = (windowsByAsset[assetId] ?? [])
        .filter { $0.detectorVersion != SemanticSweepMarkComposer.detectorVersion }
        .sorted { $0.startTime < $1.startTime }
        .map { ["start": $0.startTime, "end": $0.endTime, "detector": $0.detectorVersion] }
    stageTraces[String(assetId.prefix(8))] = byPolicy
}
report["stageTraces"] = stageTraces



// MARK: - The coarse windows (iii) suppresses, and why suppressing them costs nothing here

var suppressedDetail: [[String: Any]] = []
for assetId in scanRowsByAsset.keys.sorted() {
    let rows = scanRowsByAsset[assetId] ?? []
    let admissible = rows.filter(SemanticSweepMarkComposer.isPresenceVerdict)
    let refinements = admissible.filter { $0.scanPass == SemanticSweepMarkComposer.refinementScanPass }
    let coarse = admissible.filter { $0.scanPass != SemanticSweepMarkComposer.refinementScanPass }
    for window in coarse {
        let overlapping = refinements.filter {
            $0.windowStartTime < window.windowEndTime && $0.windowEndTime > window.windowStartTime
        }
        guard !overlapping.isEmpty,
              overlapping.allSatisfy({ $0.transcriptVersion != window.transcriptVersion })
        else { continue }
        // This window is exactly what (ii)/(iii) refuse to narrow. (ii) widens it
        // to its full tile; (iii) drops it. What does dropping it cost? Only
        // whatever narrowed extent it alone contributed.
        let refinementVersions = Set(overlapping.map(\.transcriptVersion)).sorted()
        // Is there ANOTHER coarse presence row with the same bounds at one of
        // those versions? If so, that sibling produces the identical narrowing
        // and (iii) loses nothing.
        let siblingAtRefinementVersion = coarse.contains { other in
            other.id != window.id
                && refinementVersions.contains(other.transcriptVersion)
                && abs(other.windowStartTime - window.windowStartTime) < 1e-6
                && abs(other.windowEndTime - window.windowEndTime) < 1e-6
        }
        suppressedDetail.append([
            "asset": String(assetId.prefix(8)),
            "coarseWindow": [window.windowStartTime, window.windowEndTime],
            "coarseVersion": String(window.transcriptVersion.prefix(8)),
            "refinementVersions": refinementVersions.map { String($0.prefix(8)) },
            "refinementSpans": overlapping.map { [$0.windowStartTime, $0.windowEndTime] },
            "sameBoundsCoarseSiblingAtRefinementVersion": siblingAtRefinementVersion
        ])
    }
}
report["coarseWindowsWithOnlyCrossVersionRefinements"] = suppressedDetail



// MARK: - Reachability proof for option (iii)'s two suppressions
//
// (iii) changes NOTHING on this pull. A zero is only a measurement if the rule
// that produced it can be shown to fire — otherwise "costs nothing" and "never
// runs" are the same reading. `suppressedCoarseWindows` already proves the
// first branch runs on real data (16 times). The ORPHAN branch fires zero times
// on the pull, so it is exercised here on synthetic rows, and all three options
// are made to disagree.

func syntheticRow(
    id: String, pass: String, disposition: CoarseDisposition,
    start: Double, end: Double, version: String, spans: String
) -> SemanticScanResult {
    SemanticScanResult(
        id: id, analysisAssetId: "SELFTEST", windowFirstAtomOrdinal: 0, windowLastAtomOrdinal: 0,
        windowStartTime: start, windowEndTime: end, scanPass: pass,
        transcriptQuality: .good, disposition: disposition, spansJSON: spans,
        status: .success, attemptCount: 1, errorContext: nil,
        inputTokenCount: nil, outputTokenCount: nil, latencyMs: nil,
        scanCohortJSON: "{}", transcriptVersion: version)
}

// One coarse tile at V1, one refinement at V2, nothing else. The refinement is
// overlapped ONLY by a cross-version coarse window, which is the shape the pull
// never produces.
let selftestRows = [
    syntheticRow(id: "coarse", pass: "passA", disposition: .containsAd,
                 start: 100, end: 200, version: "V1",
                 spans: "{\"supportLineRefs\":[7],\"certainty\":\"strong\"}"),
    syntheticRow(id: "refine", pass: "passB", disposition: .containsAd,
                 start: 120, end: 140, version: "V2",
                 spans: "[{\"commercialIntent\":\"paid\",\"ownership\":\"thirdParty\",\"firstLineRef\":7,\"lastLineRef\":7,\"certainty\":\"strong\",\"boundaryPrecision\":\"usable\",\"evidenceAnchors\":[]}]")
]
var selftest: [String: Any] = [:]
for (name, policy) in [
    ("i_shipped", SemanticSweepMarkComposer.VersionScopePolicy9s1z.shipped),
    ("ii_sameVersion", .sameVersion),
    ("iii_sameVersionDropBoth", .sameVersionDropBoth)
] {
    SemanticSweepMarkComposer.versionScopePolicy9s1z = policy
    SemanticSweepMarkComposer.measurement9s1zSuppressedCoarseWindows = 0
    SemanticSweepMarkComposer.measurement9s1zSuppressedOrphanedRefinements = 0
    let produced = SemanticSweepMarkComposer.compose(
        scanRows: selftestRows, existingWindows: [], supportLines: nil,
        analysisAssetId: "SELFTEST")
    selftest[name] = [
        "marks": produced.map { ["start": $0.startTime, "end": $0.endTime] },
        "suppressedCoarseWindows": SemanticSweepMarkComposer.measurement9s1zSuppressedCoarseWindows,
        "suppressedOrphanedRefinements": SemanticSweepMarkComposer.measurement9s1zSuppressedOrphanedRefinements
    ]
}
SemanticSweepMarkComposer.versionScopePolicy9s1z = .shipped
report["reachabilitySelfTest"] = selftest

let data = try! JSONSerialization.data(withJSONObject: report, options: [.prettyPrinted, .sortedKeys])
FileHandle.standardOutput.write(data)
