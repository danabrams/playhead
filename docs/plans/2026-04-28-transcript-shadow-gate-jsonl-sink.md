# Transcript Shadow-Gate JSONL Sink Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace `NoOpTranscriptShadowGateLogger` in DEBUG runtime with a real actor-backed JSONL writer at `Documents/transcript-shadow-gate.jsonl` so the gtt9.1 shadow-mode acoustic transcript gate's per-shard decisions land on disk and downstream eval tooling can compute would-skip-rate against host-read ground truth.

**Architecture:** Mechanical clone of `DecisionLogger` (actor body, lazy `migrate()`, 10 MB rotation with crash-safe `replaceItemAt` swap, livelock guard, idempotent rotation-index scan). Schema bumped to v2 with a new `buildCommitSHA: String?` field stamped from the existing `BuildInfo.commitSHA` (gtt9.21). DEBUG-only construction in `PlayheadRuntime` mirroring `preBuiltDecisionLogger`; release builds keep the no-op. Source canary extended to pin the new `migrate()` call site and DEBUG construction.

**Tech Stack:** Swift 6.0, Foundation, OSLog, Swift Testing (`@Suite`/`@Test`/`#expect`), XCTest (canary), `xcodebuild` against the iPhone 17 Pro simulator.

**Design doc:** `docs/plans/2026-04-28-transcript-shadow-gate-jsonl-sink-design.md`

**Worktree:** `/Users/dabrams/playhead/.worktrees/playhead-b58j` (branch `bead/playhead-b58j`)

---

## Pre-flight

You are operating in the worktree above. Verify before each step:

```bash
pwd                              # /Users/dabrams/playhead/.worktrees/playhead-b58j
git rev-parse --abbrev-ref HEAD  # bead/playhead-b58j
```

The Xcode project is regenerated from `project.yml` by `xcodegen`. If `Playhead.xcodeproj` is missing or stale (you've added new source files), regenerate first:

```bash
xcodegen generate
```

Test command throughout this plan (per `CLAUDE.md`):

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData
```

For targeted test runs, use `-only-testing:'PlayheadTests/<SuiteName>/<methodName>()'` (Swift Testing identifiers must use the command-line flag, not test plans — see `CLAUDE.md`).

---

## Task 1: Bump `TranscriptShadowGateEntry` to schema v2 with `buildCommitSHA`

Schema-versioned change. The auto-synthesized Codable on a non-optional new field would refuse pre-bump rows, so we hand-roll `init(from:)` / `encode(to:)` (mirrors the `DecisionLogEntry.LedgerEntry` pattern from playhead-epfk at `Playhead/Services/AdDetection/DecisionLogger.swift:182`).

**Files:**
- Modify: `Playhead/Services/AnalysisJobRunner/TranscriptShadowGateLogger.swift` (schema struct)
- Create: `PlayheadTests/Services/AnalysisJobRunner/TranscriptShadowGateLoggerTests.swift` (new file — schema-coverage section only in this task)

### Step 1.1: Write the failing schema tests

Create `PlayheadTests/Services/AnalysisJobRunner/TranscriptShadowGateLoggerTests.swift` with the schema suite. The actor suite is added in Task 2; do not pre-write Task 2's tests here.

```swift
// TranscriptShadowGateLoggerTests.swift
// playhead-b58j: coverage for the schema-v2 round-trip and the
// actor-backed JSONL writer.

import Foundation
import Testing
@testable import Playhead

// MARK: - Schema

@Suite("TranscriptShadowGateEntry — schema v2 round-trip")
struct TranscriptShadowGateEntryCodableTests {

    private func makeEntry(buildCommitSHA: String? = "abc1234") -> TranscriptShadowGateEntry {
        TranscriptShadowGateEntry(
            schemaVersion: TranscriptShadowGateEntry.currentSchemaVersion,
            timestamp: 1_745_000_000.0,
            analysisAssetID: "asset-a",
            episodeID: "ep-a",
            shardID: 3,
            shardStart: 30.0,
            shardEnd: 60.0,
            likelihood: 0.42,
            threshold: 0.55,
            decision: .wouldSkip,
            wouldGate: true,
            transcribed: true,
            buildCommitSHA: buildCommitSHA
        )
    }

    @Test("currentSchemaVersion is 2")
    func currentSchemaVersionIsTwo() {
        #expect(TranscriptShadowGateEntry.currentSchemaVersion == 2)
    }

    @Test("Encoded v2 entry is compact, newline-free, and round-trips")
    func encodesCompactJSON() throws {
        let entry = makeEntry()
        let data = try JSONEncoder().encode(entry)
        let json = String(decoding: data, as: UTF8.self)
        #expect(!json.contains("\n"),
                "Encoded entry must not embed newlines (JSONL requires one record per line)")
        let decoded = try JSONDecoder().decode(TranscriptShadowGateEntry.self, from: data)
        #expect(decoded == entry)
    }

    @Test("v2 always emits buildCommitSHA key (even when nil)")
    func encodeAlwaysEmitsBuildCommitSHAKey() throws {
        let entry = makeEntry(buildCommitSHA: nil)
        let data = try JSONEncoder().encode(entry)
        let json = String(decoding: data, as: UTF8.self)
        #expect(json.contains("\"buildCommitSHA\""),
                "v2 wire shape must always carry the key so consumers can self-identify the cohort")
    }

    @Test("v1 row decodes with nil buildCommitSHA")
    func v1RowDecodesWithNilBuildCommitSHA() throws {
        // Hand-crafted pre-bump row — no buildCommitSHA key, schemaVersion=1.
        let v1Json = """
        {"schemaVersion":1,"timestamp":1745000000.0,"analysisAssetID":"asset-a",\
        "episodeID":"ep-a","shardID":3,"shardStart":30.0,"shardEnd":60.0,\
        "likelihood":0.42,"threshold":0.55,"decision":"wouldSkip",\
        "wouldGate":true,"transcribed":true}
        """
        let decoded = try JSONDecoder().decode(
            TranscriptShadowGateEntry.self, from: Data(v1Json.utf8)
        )
        #expect(decoded.schemaVersion == 1)
        #expect(decoded.buildCommitSHA == nil)
        #expect(decoded.decision == .wouldSkip)
    }
}
```

**Step 1.2: Run tests to verify they fail**

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/TranscriptShadowGateEntryCodableTests'
```

Expected: FAIL — compile error on the missing `buildCommitSHA:` initializer argument and `currentSchemaVersion == 2` mismatch.

If you added a new file, regenerate the project first:

```bash
xcodegen generate
```

### Step 1.3: Bump the schema

Edit `Playhead/Services/AnalysisJobRunner/TranscriptShadowGateLogger.swift`. In the `TranscriptShadowGateEntry` struct (around line 46-124):

1. Change the `schemaVersion` doc comment from `Current: 1.` to `Current: 2.`.
2. Add a stored property `let buildCommitSHA: String?` after `transcribed` (around line 94). Add a doc comment:

   ```swift
   /// Short git SHA stamped at logger init from `BuildInfo.commitSHA`.
   /// Always set on v2 rows (falls back to `"unknown"` outside a git
   /// context per the `BuildInfo` contract). Decodes as `nil` on v1
   /// rows so pre-bump captures round-trip cleanly.
   let buildCommitSHA: String?
   ```

3. Bump `currentSchemaVersion` from `1` to `2`.
4. Add a private `CodingKeys` enum and explicit `init(from:)` / `encode(to:)` so v1 rows (key absent) decode with `buildCommitSHA = nil` and v2 always emits the key. Insert immediately after the `currentSchemaVersion` line, before the nested `Decision` enum:

   ```swift
   // playhead-b58j: explicit Codable so v1 rows (no buildCommitSHA key)
   // decode cleanly with `buildCommitSHA = nil`. v2 always emits the
   // key (even when nil → JSON null) so consumers self-identify the
   // capture cohort. Mirrors the DecisionLogEntry.LedgerEntry pattern
   // from playhead-epfk.
   private enum CodingKeys: String, CodingKey {
       case schemaVersion, timestamp, analysisAssetID, episodeID,
            shardID, shardStart, shardEnd, likelihood, threshold,
            decision, wouldGate, transcribed, buildCommitSHA
   }

   init(
       schemaVersion: Int,
       timestamp: Double,
       analysisAssetID: String,
       episodeID: String,
       shardID: Int,
       shardStart: Double,
       shardEnd: Double,
       likelihood: Double?,
       threshold: Double,
       decision: Decision,
       wouldGate: Bool,
       transcribed: Bool,
       buildCommitSHA: String?
   ) {
       self.schemaVersion = schemaVersion
       self.timestamp = timestamp
       self.analysisAssetID = analysisAssetID
       self.episodeID = episodeID
       self.shardID = shardID
       self.shardStart = shardStart
       self.shardEnd = shardEnd
       self.likelihood = likelihood
       self.threshold = threshold
       self.decision = decision
       self.wouldGate = wouldGate
       self.transcribed = transcribed
       self.buildCommitSHA = buildCommitSHA
   }

   init(from decoder: Decoder) throws {
       let c = try decoder.container(keyedBy: CodingKeys.self)
       self.schemaVersion = try c.decode(Int.self, forKey: .schemaVersion)
       self.timestamp = try c.decode(Double.self, forKey: .timestamp)
       self.analysisAssetID = try c.decode(String.self, forKey: .analysisAssetID)
       self.episodeID = try c.decode(String.self, forKey: .episodeID)
       self.shardID = try c.decode(Int.self, forKey: .shardID)
       self.shardStart = try c.decode(Double.self, forKey: .shardStart)
       self.shardEnd = try c.decode(Double.self, forKey: .shardEnd)
       self.likelihood = try c.decodeIfPresent(Double.self, forKey: .likelihood)
       self.threshold = try c.decode(Double.self, forKey: .threshold)
       self.decision = try c.decode(Decision.self, forKey: .decision)
       self.wouldGate = try c.decode(Bool.self, forKey: .wouldGate)
       self.transcribed = try c.decode(Bool.self, forKey: .transcribed)
       // playhead-b58j: pre-bump (v1) rows omit the key → nil.
       self.buildCommitSHA = try c.decodeIfPresent(String.self, forKey: .buildCommitSHA)
   }

   func encode(to encoder: Encoder) throws {
       var c = encoder.container(keyedBy: CodingKeys.self)
       try c.encode(schemaVersion, forKey: .schemaVersion)
       try c.encode(timestamp, forKey: .timestamp)
       try c.encode(analysisAssetID, forKey: .analysisAssetID)
       try c.encode(episodeID, forKey: .episodeID)
       try c.encode(shardID, forKey: .shardID)
       try c.encode(shardStart, forKey: .shardStart)
       try c.encode(shardEnd, forKey: .shardEnd)
       try c.encode(likelihood, forKey: .likelihood)
       try c.encode(threshold, forKey: .threshold)
       try c.encode(decision, forKey: .decision)
       try c.encode(wouldGate, forKey: .wouldGate)
       try c.encode(transcribed, forKey: .transcribed)
       // Always emit (even when nil → JSON null) so v2 rows are wire-
       // distinguishable from v1.
       try c.encode(buildCommitSHA, forKey: .buildCommitSHA)
   }

   static let currentSchemaVersion: Int = 2
   ```

   Remove the old `static let currentSchemaVersion: Int = 1` line that sat above the `Decision` enum.

5. Update the two existing emit sites in `Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift` (around lines 691 and 750 — search for `TranscriptShadowGateEntry(`). Each constructs an entry; add `buildCommitSHA: nil` as the new trailing argument so the runner-side compile passes. The actor wired in Task 2 will overwrite the stamp at logger-side; the runner-side value is `nil` (the runner doesn't know about the build SHA — that concern lives in the logger).

   Actually, simpler and more correct: the **logger** is the single point where `BuildInfo.commitSHA` is read, NOT the call site. The call site passes `nil` and the logger overwrites in `appendEntry` before encoding. We document this in Task 2 step 2.3. For Task 1 step 1.3, just add `buildCommitSHA: nil` to both runner-side constructors so they compile.

### Step 1.4: Run tests to verify they pass

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/TranscriptShadowGateEntryCodableTests'
```

Expected: PASS — three tests green (`currentSchemaVersionIsTwo`, `encodesCompactJSON`, `encodeAlwaysEmitsBuildCommitSHAKey`, `v1RowDecodesWithNilBuildCommitSHA`).

Also run the existing acoustic-gate suite to confirm the runner-side `buildCommitSHA: nil` add didn't break it:

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/AcousticTranscriptGateTests'
```

Expected: PASS.

### Step 1.5: Commit

```bash
git add Playhead/Services/AnalysisJobRunner/TranscriptShadowGateLogger.swift \
        Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift \
        PlayheadTests/Services/AnalysisJobRunner/TranscriptShadowGateLoggerTests.swift
git commit -m "$(cat <<'EOF'
feat(transcript-shadow-gate): bump schema to v2 with buildCommitSHA (playhead-b58j)

v2 adds an optional `buildCommitSHA` stamp. Custom Codable so pre-bump
v1 rows (key absent) decode with `buildCommitSHA = nil`; v2 always
emits the key so the wire shape self-identifies the capture cohort.
Existing emit sites pass `nil` — the actor-backed logger (next commit)
is the single point that reads `BuildInfo.commitSHA` and stamps every
encoded row.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Add `TranscriptShadowGateLogger` actor (the JSONL writer)

Mechanical clone of `Playhead/Services/AdDetection/DecisionLogger.swift`. Lives in the **same file** as the schema + protocol + no-op (`Playhead/Services/AnalysisJobRunner/TranscriptShadowGateLogger.swift`) — the file already takes its name from the actor we're adding.

**Files:**
- Modify: `Playhead/Services/AnalysisJobRunner/TranscriptShadowGateLogger.swift` (append actor body)
- Modify: `PlayheadTests/Services/AnalysisJobRunner/TranscriptShadowGateLoggerTests.swift` (append actor suite)

### Step 2.1: Write the failing actor tests

Append to `PlayheadTests/Services/AnalysisJobRunner/TranscriptShadowGateLoggerTests.swift`:

```swift
// MARK: - Logger (file I/O)

@Suite("TranscriptShadowGateLogger — append + rotation", .serialized)
struct TranscriptShadowGateLoggerFileIOTests {

    private func makeTempDir(function: String = #function) throws -> URL {
        let base = FileManager.default.temporaryDirectory
            .appendingPathComponent("transcript-shadow-gate-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: base, withIntermediateDirectories: true)
        return base
    }

    private func sampleEntry(asset: String = "asset-a",
                             timestamp: Double = 1_745_000_000.0) -> TranscriptShadowGateEntry {
        TranscriptShadowGateEntry(
            schemaVersion: TranscriptShadowGateEntry.currentSchemaVersion,
            timestamp: timestamp,
            analysisAssetID: asset,
            episodeID: "ep-\(asset)",
            shardID: 1,
            shardStart: 0.0,
            shardEnd: 30.0,
            likelihood: 0.42,
            threshold: 0.55,
            decision: .wouldSkip,
            wouldGate: true,
            transcribed: true,
            buildCommitSHA: nil  // logger overwrites with BuildInfo.commitSHA
        )
    }

    @Test("record(_:) appends one JSON line per call to transcript-shadow-gate.jsonl")
    func appendsJSONL() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try TranscriptShadowGateLogger(directory: dir)
        await logger.record(sampleEntry(asset: "a"))
        await logger.record(sampleEntry(asset: "b"))
        await logger.flushAndClose()

        let url = dir.appendingPathComponent(TranscriptShadowGateLogger.activeLogFilename)
        let data = try Data(contentsOf: url)
        let lines = String(decoding: data, as: UTF8.self)
            .split(separator: "\n", omittingEmptySubsequences: true)
        #expect(lines.count == 2)

        let decoder = JSONDecoder()
        let first = try decoder.decode(TranscriptShadowGateEntry.self, from: Data(lines[0].utf8))
        let second = try decoder.decode(TranscriptShadowGateEntry.self, from: Data(lines[1].utf8))
        #expect(first.analysisAssetID == "a")
        #expect(second.analysisAssetID == "b")
    }

    @Test("Every encoded row carries the logger's buildCommitSHA stamp")
    func everyEntryStampedWithBuildCommitSHA() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try TranscriptShadowGateLogger(directory: dir)
        // Caller passes nil; logger must overwrite with BuildInfo.commitSHA.
        await logger.record(sampleEntry(asset: "a"))
        await logger.flushAndClose()

        let url = dir.appendingPathComponent(TranscriptShadowGateLogger.activeLogFilename)
        let data = try Data(contentsOf: url)
        let line = String(decoding: data, as: UTF8.self)
            .split(separator: "\n").first.map(String.init) ?? ""
        let decoded = try JSONDecoder().decode(
            TranscriptShadowGateEntry.self, from: Data(line.utf8)
        )
        #expect(decoded.buildCommitSHA == BuildInfo.commitSHA)
        #expect(decoded.buildCommitSHA?.isEmpty == false,
                "BuildInfo.commitSHA contract: never empty (falls back to 'unknown')")
    }

    @Test("Exceeding threshold rotates active file to transcript-shadow-gate.1.jsonl")
    func rotatesOnThreshold() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Threshold of 1 byte triggers rotation as soon as the active file
        // has >= 2 lines (livelock guard requires >1 line before rotating).
        let logger = try TranscriptShadowGateLogger(directory: dir, rotationThresholdBytes: 1)
        await logger.record(sampleEntry(asset: "a"))
        await logger.record(sampleEntry(asset: "b"))
        await logger.flushAndClose()

        let rotated = dir.appendingPathComponent("transcript-shadow-gate.1.jsonl")
        #expect(FileManager.default.fileExists(atPath: rotated.path))
    }

    @Test("Warm start seeds next rotation index from highest existing rotated file")
    func warmStartSeedsFromDisk() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        // Pre-seed a synthetic rotated file at index 5.
        let preExisting = dir.appendingPathComponent("transcript-shadow-gate.5.jsonl")
        try "pre-seeded\n".data(using: .utf8)!.write(to: preExisting)

        let logger = try TranscriptShadowGateLogger(directory: dir, rotationThresholdBytes: 1)
        let seed = await logger.currentNextRotationIndex()
        #expect(seed == 6)

        await logger.record(sampleEntry(asset: "warm-a"))
        await logger.record(sampleEntry(asset: "warm-b"))
        await logger.flushAndClose()

        let r6 = dir.appendingPathComponent("transcript-shadow-gate.6.jsonl")
        #expect(FileManager.default.fileExists(atPath: r6.path))
        #expect(FileManager.default.fileExists(atPath: preExisting.path),
                "Pre-existing rotated file must be preserved across warm start")
    }

    @Test("Livelock guard skips rotation when active file has only one line")
    func livelockGuardSkipsRotation() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let logger = try TranscriptShadowGateLogger(directory: dir, rotationThresholdBytes: 1)
        await logger.record(sampleEntry(asset: "lone"))
        await logger.flushAndClose()

        let rotated = dir.appendingPathComponent("transcript-shadow-gate.1.jsonl")
        #expect(!FileManager.default.fileExists(atPath: rotated.path),
                "Single-line file must NOT rotate — would loop forever on a >threshold record")
        let active = dir.appendingPathComponent(TranscriptShadowGateLogger.activeLogFilename)
        #expect(FileManager.default.fileExists(atPath: active.path))
    }

    @Test("No-op TranscriptShadowGateLogger writes no files")
    func noOpWritesNothing() async throws {
        let dir = try makeTempDir()
        defer { try? FileManager.default.removeItem(at: dir) }

        let noop: TranscriptShadowGateLogging = NoOpTranscriptShadowGateLogger()
        await noop.record(sampleEntry())

        let contents = try FileManager.default.contentsOfDirectory(atPath: dir.path)
        #expect(contents.isEmpty, "NoOp logger must not write any files")
    }
}
```

**Step 2.2: Run tests to verify they fail**

```bash
xcodegen generate  # so the new test references compile
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/TranscriptShadowGateLoggerFileIOTests'
```

Expected: FAIL — `Cannot find 'TranscriptShadowGateLogger' in scope` (the actor doesn't exist yet).

### Step 2.3: Implement the actor

Append the following to `Playhead/Services/AnalysisJobRunner/TranscriptShadowGateLogger.swift`, after the existing `NoOpTranscriptShadowGateLogger` struct. This is a mechanical clone of `DecisionLogger`; all design rationale lives in that file's header comments. Cross-reference comments are kept short.

```swift
// MARK: - TranscriptShadowGateLogger (actor-backed JSONL writer)

/// Actor-backed JSONL writer for shadow-mode transcript-gate decisions.
/// DEBUG-only by convention — `PlayheadRuntime` gates construction
/// behind `#if DEBUG` so release builds never write to disk.
///
/// Mechanical clone of `DecisionLogger`: lazy `migrate()` bootstrap,
/// 10 MB rotation with crash-safe `replaceItemAt` swap, livelock guard
/// for >threshold single-line records, idempotent rotation-index scan.
/// See `DecisionLogger.swift` for the canonical design notes.
///
/// Every encoded row is stamped with `BuildInfo.commitSHA` so eval
/// tooling can correlate captures with the exact binary that produced
/// them. Callers pass `buildCommitSHA: nil`; the actor overwrites in
/// `appendEntry` before encoding.
actor TranscriptShadowGateLogger: TranscriptShadowGateLogging {

    static let defaultRotationThresholdBytes: Int = 10 * 1024 * 1024
    static let activeLogFilename: String = "transcript-shadow-gate.jsonl"
    static let rotatedPrefix: String = "transcript-shadow-gate"
    static let rotatedSuffix: String = ".jsonl"

    private let directoryOverride: URL?
    private var resolvedDirectory: URL?

    private let rotationThresholdBytes: Int
    private let buildCommitSHA: String
    private let encoder: JSONEncoder
    private let logger = Logger(subsystem: "com.playhead", category: "TranscriptShadowGateLogger")

    private var nextRotationIndex: Int?
    private var fileHandle: FileHandle?

    // MARK: - Init

    init(rotationThresholdBytes: Int = TranscriptShadowGateLogger.defaultRotationThresholdBytes) throws {
        self.directoryOverride = nil
        self.rotationThresholdBytes = rotationThresholdBytes
        self.buildCommitSHA = BuildInfo.commitSHA
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder
        self.resolvedDirectory = nil
        self.nextRotationIndex = nil
    }

    init(
        directory: URL,
        rotationThresholdBytes: Int = TranscriptShadowGateLogger.defaultRotationThresholdBytes
    ) throws {
        self.directoryOverride = directory
        self.rotationThresholdBytes = rotationThresholdBytes
        self.buildCommitSHA = BuildInfo.commitSHA
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        self.encoder = encoder
        self.resolvedDirectory = nil
        self.nextRotationIndex = nil
    }

    /// Lazy first-use bootstrap. Resolves the directory (Documents lookup
    /// for the convenience init), creates it, seeds `nextRotationIndex`
    /// from disk. Idempotent.
    func migrate() throws {
        try ensureBootstrapped()
    }

    private func ensureBootstrapped() throws {
        if resolvedDirectory == nil {
            let dir: URL
            if let override = directoryOverride {
                dir = override
            } else {
                dir = try FileManager.default.url(
                    for: .documentDirectory,
                    in: .userDomainMask,
                    appropriateFor: nil,
                    create: true
                )
            }
            try FileManager.default.createDirectory(
                at: dir, withIntermediateDirectories: true
            )
            self.resolvedDirectory = dir
        }
        if nextRotationIndex == nil, let dir = resolvedDirectory {
            self.nextRotationIndex = Self.scanNextRotationIndex(in: dir)
        }
    }

    // MARK: - Public API

    func record(_ entry: TranscriptShadowGateEntry) async {
        do {
            try ensureBootstrapped()
            try appendEntry(entry)
            try rotateIfNeeded()
        } catch {
            logger.warning("TranscriptShadowGateLogger.record failed: \(error.localizedDescription, privacy: .public)")
        }
    }

    // MARK: - Test hooks

    func currentNextRotationIndex() -> Int {
        try? ensureBootstrapped()
        return nextRotationIndex ?? 1
    }

    func flushAndClose() {
        closeHandle()
    }

    var activeLogURL: URL {
        try? ensureBootstrapped()
        let dir = resolvedDirectory ?? directoryOverride ?? URL(fileURLWithPath: NSTemporaryDirectory())
        return dir.appendingPathComponent(Self.activeLogFilename)
    }

    func rotatedLogURLs() -> [URL] {
        try? ensureBootstrapped()
        guard let dir = resolvedDirectory else { return [] }
        return Self.listRotatedLogs(in: dir)
    }

    // MARK: - Internal

    private func appendEntry(_ entry: TranscriptShadowGateEntry) throws {
        // Stamp every encoded row with the build SHA captured at init.
        // Callers pass `nil`; we own the stamp here so the eval pipeline
        // can correlate captures to the binary that produced them.
        let stamped = TranscriptShadowGateEntry(
            schemaVersion: entry.schemaVersion,
            timestamp: entry.timestamp,
            analysisAssetID: entry.analysisAssetID,
            episodeID: entry.episodeID,
            shardID: entry.shardID,
            shardStart: entry.shardStart,
            shardEnd: entry.shardEnd,
            likelihood: entry.likelihood,
            threshold: entry.threshold,
            decision: entry.decision,
            wouldGate: entry.wouldGate,
            transcribed: entry.transcribed,
            buildCommitSHA: buildCommitSHA
        )
        let data = try encoder.encode(stamped)
        var line = Data()
        line.reserveCapacity(data.count + 1)
        line.append(data)
        line.append(0x0A)
        try write(line)
    }

    private func write(_ data: Data) throws {
        let url = activeLogURL
        if fileHandle == nil {
            if !FileManager.default.fileExists(atPath: url.path) {
                FileManager.default.createFile(atPath: url.path, contents: nil)
            }
            fileHandle = try FileHandle(forWritingTo: url)
            try fileHandle?.seekToEnd()
        }
        try fileHandle?.write(contentsOf: data)
    }

    private func rotateIfNeeded() throws {
        let url = activeLogURL
        let attrs = try FileManager.default.attributesOfItem(atPath: url.path)
        let size = (attrs[.size] as? NSNumber)?.intValue ?? 0
        guard size >= rotationThresholdBytes else { return }
        if try lineCount(at: url) <= 1 {
            logger.warning(
                "TranscriptShadowGateLogger: active log exceeds threshold but has \u{2264}1 line; skipping rotation to avoid livelock"
            )
            return
        }
        try rotateNow()
    }

    private func lineCount(at url: URL) throws -> Int {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var count = 0
        while let chunk = try handle.read(upToCount: 64 * 1024), !chunk.isEmpty {
            for byte in chunk where byte == 0x0A {
                count += 1
                if count >= 2 { return count }
            }
        }
        return count
    }

    private func rotateNow() throws {
        try ensureBootstrapped()
        guard let dir = resolvedDirectory, let idx = nextRotationIndex else {
            return
        }
        let src = activeLogURL
        let dstName = "\(Self.rotatedPrefix).\(idx)\(Self.rotatedSuffix)"
        let dst = dir.appendingPathComponent(dstName)

        closeHandle()

        let fm = FileManager.default
        if fm.fileExists(atPath: dst.path) {
            _ = try fm.replaceItemAt(dst, withItemAt: src)
        } else {
            try fm.moveItem(at: src, to: dst)
        }
        nextRotationIndex = idx + 1
        logger.info("TranscriptShadowGateLogger: rotated active log to \(dstName, privacy: .public)")
    }

    private func closeHandle() {
        if let handle = fileHandle {
            try? handle.close()
            fileHandle = nil
        }
    }

    // MARK: - Static helpers

    fileprivate static func scanNextRotationIndex(in directory: URL) -> Int {
        listRotatedLogs(in: directory)
            .compactMap { extractRotationIndex(from: $0.lastPathComponent) }
            .max()
            .map { $0 + 1 } ?? 1
    }

    fileprivate static func listRotatedLogs(in directory: URL) -> [URL] {
        guard let items = try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else { return [] }
        let matches = items.filter { url in
            extractRotationIndex(from: url.lastPathComponent) != nil
        }
        return matches.sorted { lhs, rhs in
            let li = extractRotationIndex(from: lhs.lastPathComponent) ?? 0
            let ri = extractRotationIndex(from: rhs.lastPathComponent) ?? 0
            return li < ri
        }
    }

    fileprivate static func extractRotationIndex(from name: String) -> Int? {
        let prefix = rotatedPrefix + "."
        let suffix = rotatedSuffix
        guard name.hasPrefix(prefix), name.hasSuffix(suffix) else { return nil }
        let middle = name.dropFirst(prefix.count).dropLast(suffix.count)
        return Int(middle)
    }
}
```

### Step 2.4: Run tests to verify they pass

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/TranscriptShadowGateLoggerFileIOTests'
```

Expected: PASS — six tests green (`appendsJSONL`, `everyEntryStampedWithBuildCommitSHA`, `rotatesOnThreshold`, `warmStartSeedsFromDisk`, `livelockGuardSkipsRotation`, `noOpWritesNothing`).

### Step 2.5: Commit

```bash
git add Playhead/Services/AnalysisJobRunner/TranscriptShadowGateLogger.swift \
        PlayheadTests/Services/AnalysisJobRunner/TranscriptShadowGateLoggerTests.swift
git commit -m "$(cat <<'EOF'
feat(transcript-shadow-gate): actor-backed JSONL logger (playhead-b58j)

Mechanical clone of DecisionLogger: lazy migrate() bootstrap, 10 MB
rotation with crash-safe replaceItemAt swap, livelock guard for
>threshold single-line records, idempotent rotation-index scan. Stamps
BuildInfo.commitSHA on every encoded row so eval tooling correlates
captures to the producing binary.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Wire `TranscriptShadowGateLogger` into `PlayheadRuntime`

DEBUG-only construction parallel to `preBuiltDecisionLogger`. Three edits to `Playhead/App/PlayheadRuntime.swift`, all directly mirroring the existing decision-logger pattern.

**Files:**
- Modify: `Playhead/App/PlayheadRuntime.swift`

### Step 3.1: Add the DEBUG-gated construction block

In `Playhead/App/PlayheadRuntime.swift`, immediately after the existing `preBuiltDecisionLogger` block (which ends at line 677 with `#endif` after `preBuiltDecisionLogger = nil`), insert:

```swift
        // playhead-b58j (gtt9.1.1): DEBUG-only TranscriptShadowGateLogger.
        // Mirrors the preBuiltDecisionLogger pattern above — constructed
        // synchronously before AnalysisJobRunner so the very first
        // shadow-gate evaluation observes the installed writer. Release
        // builds never compile this branch and keep the no-op default
        // (zero disk I/O on shipping binaries).
        let preBuiltShadowGateLogger: TranscriptShadowGateLogging?
        #if DEBUG
        do {
            preBuiltShadowGateLogger = try TranscriptShadowGateLogger()
        } catch {
            Logger(subsystem: "com.playhead", category: "Runtime")
                .warning("TranscriptShadowGateLogger init failed — shadow logging disabled: \(error.localizedDescription, privacy: .public)")
            preBuiltShadowGateLogger = nil
        }
        #else
        preBuiltShadowGateLogger = nil
        #endif
```

### Step 3.2: Pass the logger into `AnalysisJobRunner`

Find the `AnalysisJobRunner(...)` construction (around line 795). Add a new trailing argument before the closing paren:

```swift
        self.analysisJobRunner = AnalysisJobRunner(
            store: analysisStore,
            audioProvider: audioService,
            featureService: featureService,
            transcriptEngine: transcriptEngine,
            adDetection: adDetectionService,
            cueMaterializer: cueMaterializer,
            preemptionCoordinator: lanePreemptionCoordinator,
            transcriptShadowGateLogger: preBuiltShadowGateLogger ?? NoOpTranscriptShadowGateLogger()
        )
```

(`AnalysisJobRunner.init` already declares `transcriptShadowGateLogger: TranscriptShadowGateLogging = NoOpTranscriptShadowGateLogger()` — see `Playhead/Services/AnalysisJobRunner/AnalysisJobRunner.swift:78`.)

### Step 3.3: Add the deferred `migrate()` call

Find the deferred init `Task` closure capture list (around line 948). Append `preBuiltShadowGateLogger` to the capture list so it's available inside the closure:

```swift
        Task { [analysisStore, downloadManager, analysisWorkScheduler, analysisJobReconciler, backgroundProcessingService, lanePreemptionCoordinator, analysisCoordinator, shadowCaptureCoordinator, adCatalogStore, feedbackStore, surfaceStatusLogger, preBuiltDecisionLogger, preBuiltShadowGateLogger, lifecycleLogger, bgTaskTelemetry] in
```

Then, after the existing `decisionLogger.migrate()` block (ending around line 1046), add a sibling block:

```swift
            if let shadowGateLogger = preBuiltShadowGateLogger as? TranscriptShadowGateLogger {
                do {
                    try await shadowGateLogger.migrate()
                } catch {
                    Logger(subsystem: "com.playhead", category: "Runtime")
                        .warning("TranscriptShadowGateLogger deferred migrate failed — first record will retry: \(error.localizedDescription, privacy: .public)")
                }
            }
```

### Step 3.4: Compile-and-test

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/TranscriptShadowGateLoggerFileIOTests' \
  -only-testing:'PlayheadTests/TranscriptShadowGateEntryCodableTests' \
  -only-testing:'PlayheadTests/AcousticTranscriptGateTests'
```

Expected: PASS — schema + actor + existing acoustic-gate suite all green; `PlayheadRuntime` compile clean.

### Step 3.5: Commit

```bash
git add Playhead/App/PlayheadRuntime.swift
git commit -m "$(cat <<'EOF'
feat(runtime): wire TranscriptShadowGateLogger in DEBUG (playhead-b58j)

Mirrors the preBuiltDecisionLogger pattern: synchronous DEBUG-only
construction so the first shadow-gate evaluation observes the writer,
deferred migrate() in the same Task that warms the other lazy loggers.
Release builds keep the no-op default — zero disk I/O on shipping
binaries.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Source canary updates

Two edits to `PlayheadTests/App/PlayheadRuntimeWiringSourceCanaryTests.swift`. Both protect the wiring from silent regressions during future refactors.

**Files:**
- Modify: `PlayheadTests/App/PlayheadRuntimeWiringSourceCanaryTests.swift`

### Step 4.1: Extend the migrate-call canary

In `testFiveLazyLoggersHaveMigrateCalls`, extend the `expected` table (currently 5 entries at lines 108-114) to 6 entries. Also rename the test method and update the doc comment to reflect "six" instead of "five".

Rename method: `testFiveLazyLoggersHaveMigrateCalls` → `testSixLazyLoggersHaveMigrateCalls`. Update the corresponding doc-comment phrase ("All five lazy-init loggers" → "All six lazy-init loggers") and append the new logger to the parenthetical list (`...BGTaskTelemetryLogger — playhead-jncn audit items #4/#8/#10/#15/#17, plus TranscriptShadowGateLogger — playhead-b58j`).

Add the new entry to the table:

```swift
        let expected: [(needle: String, name: String)] = [
            ("optionalFeedbackStore",            "FoundationModelsFeedbackStore"),
            ("surfaceStatusLogger.migrate()",    "SurfaceStatusInvariantLogger"),
            ("decisionLogger.migrate()",         "DecisionLogger"),
            ("assetLifecycleLogger.migrate()",   "AssetLifecycleLogger"),
            ("bgLogger.migrate()",               "BGTaskTelemetryLogger"),
            ("shadowGateLogger.migrate()",       "TranscriptShadowGateLogger"),
        ]
```

### Step 4.2: Add the DEBUG-construction canary

Append a new test method to the same `PlayheadRuntimeWiringSourceCanaryTests` class:

```swift
    /// `TranscriptShadowGateLogger` must be constructed inside a `#if DEBUG`
    /// block in `PlayheadRuntime.init` so release builds never write to
    /// disk. The per-component laziness canaries can't catch a regression
    /// that drops the `#if DEBUG` guard or replaces the constructor with
    /// a no-op — only the source shape proves the right thing happens.
    ///
    /// Anchors:
    ///   • `try TranscriptShadowGateLogger()` appears in the file
    ///   • it lives between `#if DEBUG` and `#else`
    /// Mirrors the wiring shape used for `preBuiltDecisionLogger` so a
    /// future refactor that removes the DEBUG gate fails loudly here.
    func testShadowGateLoggerIsConstructedInDebug() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadRuntime.swift"
        )

        guard let ctorRange = source.range(of: "try TranscriptShadowGateLogger()") else {
            XCTFail(
                """
                `try TranscriptShadowGateLogger()` is missing from PlayheadRuntime.swift. \
                The DEBUG-only shadow-gate logger construction was either dropped or \
                renamed; release builds would silently install the no-op and the gtt9.1 \
                shadow-gate eval would lose its data sink. Re-add the constructor inside \
                a `#if DEBUG` block parallel to `preBuiltDecisionLogger`, or update this \
                canary if the call site was intentionally moved.
                """
            )
            return
        }

        // Walk backwards from the constructor for the nearest `#if DEBUG`,
        // then forwards for the matching `#else` / `#endif`. The
        // constructor must lie strictly between them.
        guard let ifDebugRange = source.range(
            of: "#if DEBUG",
            options: .backwards,
            range: source.startIndex..<ctorRange.lowerBound
        ) else {
            XCTFail("`try TranscriptShadowGateLogger()` is not preceded by a `#if DEBUG` guard")
            return
        }
        guard let elseRange = source.range(
            of: "#else",
            range: ctorRange.upperBound..<source.endIndex
        ) else {
            XCTFail("`try TranscriptShadowGateLogger()` is not followed by a `#else` arm")
            return
        }

        XCTAssertLessThan(
            ifDebugRange.upperBound, ctorRange.lowerBound,
            "Expected #if DEBUG to precede the constructor"
        )
        XCTAssertLessThan(
            ctorRange.upperBound, elseRange.lowerBound,
            "Expected #else to follow the constructor"
        )
    }
```

### Step 4.3: Run the canary tests

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData \
  -only-testing:'PlayheadTests/PlayheadRuntimeWiringSourceCanaryTests'
```

Expected: PASS — three canaries (`testPostMigrateActivityRefreshIsWired`, `testSixLazyLoggersHaveMigrateCalls`, `testShadowGateLoggerIsConstructedInDebug`) all green.

### Step 4.4: Commit

```bash
git add PlayheadTests/App/PlayheadRuntimeWiringSourceCanaryTests.swift
git commit -m "$(cat <<'EOF'
test(runtime-canary): pin TranscriptShadowGateLogger wiring (playhead-b58j)

Extend the migrate-call canary to six loggers (adds shadowGateLogger),
add a new canary that pins the DEBUG-only construction shape so a
future refactor that drops the `#if DEBUG` guard or removes the
constructor fails loudly instead of silently shipping disk writes on
release.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Full PlayheadFastTests + push + PR

### Step 5.1: Run the full fast plan

```bash
xcodebuild test -scheme Playhead -testPlan PlayheadFastTests \
  -destination 'platform=iOS Simulator,name=iPhone 17 Pro' \
  -derivedDataPath .derivedData
```

Expected: PASS. Full plan must be green before pushing.

If anything fails: do not push. Fix the issue (likely a missed Codable arg in another call site or a stale source canary), commit the fix, re-run.

### Step 5.2: Push + open PR

```bash
git push -u origin bead/playhead-b58j
gh pr create --title "feat(transcript-shadow-gate): JSONL sink (playhead-b58j)" --body "$(cat <<'EOF'
## Summary
- Replaces `NoOpTranscriptShadowGateLogger` in DEBUG runtime with an actor-backed JSONL writer at `Documents/transcript-shadow-gate.jsonl` (10 MB rotation, crash-safe `replaceItemAt` swap, lazy `migrate()`).
- Bumps `TranscriptShadowGateEntry` schema to v2 with a new `buildCommitSHA: String?` field stamped from `BuildInfo.commitSHA` so eval tooling correlates captures to the binary that produced them.
- Custom Codable so v1 rows (key absent) decode with `buildCommitSHA = nil`; v2 always emits the key for cohort self-identification.
- Source canary extended to pin the new `migrate()` call site and DEBUG-only construction shape.

Release builds keep the no-op — zero disk I/O on shipping binaries.

## Test plan
- [x] `PlayheadFastTests` plan green on iPhone 17 Pro simulator
- [x] Schema v1 round-trips with nil `buildCommitSHA`
- [x] v2 always emits the `buildCommitSHA` key (even when nil)
- [x] Active log appends, rotates at threshold, livelock-guards single-line files, warm-start seeds rotation index
- [x] DEBUG-construction + deferred `migrate()` source canaries pass
- [ ] Manual: dogfood DEBUG run produces `transcript-shadow-gate.jsonl` in Documents with one row per gate-eligible shard

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

### Step 5.3: Wait for CI, then merge

After CI green:

```bash
gh pr view --json number,state,mergeable
gh pr merge --squash
```

If the worktree blocks `--delete-branch` (known consequence of `main` already being checked out elsewhere), recover manually after the squash-merge:

```bash
cd /Users/dabrams/playhead && git checkout main && git pull --ff-only
git branch --merged main | grep -q 'bead/playhead-b58j' && git branch -D bead/playhead-b58j
```

### Step 5.4: Bead close + worktree cleanup

```bash
bd close playhead-b58j
WT=/Users/dabrams/playhead/.worktrees/playhead-b58j
git worktree remove "$WT"
[ -d "$WT/.derivedData" ] && rm -rf "$WT/.derivedData" && echo "removed $WT/.derivedData"
git worktree prune -v
```

(Pre-flight check from `CLAUDE.md`: branch must be merged on origin, worktree must be clean, `rm -rf` path must start with `/Users/dabrams/playhead/.worktrees/` and must NOT appear in `git worktree list --porcelain`.)

---

## Acceptance

- [ ] `transcript-shadow-gate.jsonl` appears in `Documents` on a DEBUG dogfood run with one row per gate-eligible shard.
- [ ] File rotates at 10 MB.
- [ ] Each row carries `schemaVersion=2` and a non-empty `buildCommitSHA` (or `"unknown"` when no git context).
- [ ] Eval tooling can join `transcript-shadow-gate.jsonl × decision-log.jsonl` on `(analysisAssetID, shardStart, shardEnd)`.
- [ ] Source canary fails loudly if a future refactor drops the construction or the `migrate()` call.
- [ ] Full `PlayheadFastTests` plan green on simulator.

## Out of scope

- Eval tooling that consumes the JSONL (separate sibling bead if it grows).
- Flipping `AcousticTranscriptGateConfig.skipEnabled = true` (separate bead once eval data exists).
- Backfilling `DecisionLogger` to also stamp `BuildInfo.commitSHA` (small follow-up bead — `BuildInfo` is reusable).
