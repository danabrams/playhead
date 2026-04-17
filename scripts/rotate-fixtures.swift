#!/usr/bin/env swift

// rotate-fixtures.swift
// Stratified rotation picker for the playhead-ym57 device-lab fixture
// substrate. Given:
//   * a seed file (PlayheadTests/Fixtures/Corpus/fixtures-rotation-seed.txt)
//   * a candidate pool (initially empty; to be populated as fixtures land)
//   * a target pick count (default: 4)
// this script produces a deterministic stratified selection and writes the
// picks into fixtures-manifest.json as the "rotating" set. The same seed
// always produces the same selection, so a release branch remains stable.
//
// Usage:
//   swift scripts/rotate-fixtures.swift [--dry-run] [--pool <path>] [--count N]
//
// The pool file is JSON of the form:
//   { "candidates": [ { "id": ..., "taxonomy": {...}, ... }, ... ] }
// where each candidate is a FixtureDescriptor-compatible record minus
// `slot`/`locked` (rotating entries always have locked=false, slot=0).
//
// If the pool is empty, the script prints a warning and exits 0 without
// modifying the manifest. This is the expected posture until fixtures-v1
// is populated (blocked on licensing sign-off).

import Foundation

// MARK: - CLI parsing

struct Options {
    var dryRun: Bool = false
    var poolPath: String? = nil
    var count: Int = 4
    var seedPath: String? = nil
    var manifestPath: String? = nil
}

func parseArgs(_ argv: [String]) -> Options {
    var opts = Options()
    var i = 1
    while i < argv.count {
        let a = argv[i]
        switch a {
        case "--dry-run": opts.dryRun = true
        case "--pool":   i += 1; opts.poolPath = i < argv.count ? argv[i] : nil
        case "--count":  i += 1; opts.count = (i < argv.count ? Int(argv[i]) : nil) ?? 4
        case "--seed":   i += 1; opts.seedPath = i < argv.count ? argv[i] : nil
        case "--manifest": i += 1; opts.manifestPath = i < argv.count ? argv[i] : nil
        case "-h", "--help":
            print("""
            rotate-fixtures.swift [--dry-run] [--pool path] [--count N] [--seed path] [--manifest path]
              --dry-run         Print selection without writing manifest.
              --pool path       Path to rotating-pool JSON (default: scripts/rotation-pool.json).
              --count N         Number of rotating fixtures to pick (default: 4).
              --seed path       Seed file path (default: fixtures-rotation-seed.txt).
              --manifest path   Manifest to update (default: fixtures-manifest.json).
            """)
            exit(0)
        default:
            FileHandle.standardError.write(Data("unknown arg: \(a)\n".utf8))
            exit(2)
        }
        i += 1
    }
    return opts
}

// MARK: - Paths

let scriptURL = URL(fileURLWithPath: CommandLine.arguments[0]).standardizedFileURL
let repoRoot = scriptURL.deletingLastPathComponent().deletingLastPathComponent()
let corpusDir = repoRoot
    .appendingPathComponent("PlayheadTests", isDirectory: true)
    .appendingPathComponent("Fixtures", isDirectory: true)
    .appendingPathComponent("Corpus", isDirectory: true)

let options = parseArgs(CommandLine.arguments)
let poolURL = URL(fileURLWithPath: options.poolPath
    ?? repoRoot.appendingPathComponent("scripts/rotation-pool.json").path)
let seedURL = URL(fileURLWithPath: options.seedPath
    ?? corpusDir.appendingPathComponent("fixtures-rotation-seed.txt").path)
let manifestURL = URL(fileURLWithPath: options.manifestPath
    ?? corpusDir.appendingPathComponent("fixtures-manifest.json").path)

// MARK: - Load seed

guard let seedText = try? String(contentsOf: seedURL, encoding: .utf8) else {
    FileHandle.standardError.write(Data("error: could not read seed at \(seedURL.path)\n".utf8))
    exit(1)
}
let seedTrim = seedText.trimmingCharacters(in: .whitespacesAndNewlines)
guard !seedTrim.isEmpty else {
    FileHandle.standardError.write(Data("error: seed file is empty\n".utf8))
    exit(1)
}

// MARK: - Load pool (optional)

struct Pool: Decodable {
    let candidates: [Candidate]
}
struct Candidate: Decodable {
    let id: String
    let file: String
    let sha256: String
    let durationSec: Double
    let taxonomy: [String: AnyCodable]
    let licensingRef: String
    let synthetic: Bool?
    let syntheticDurationSec: Double?
}
// AnyCodable shim so we don't have to model the full taxonomy type in this
// single-file script. We just pass the dict through to the manifest writer.
struct AnyCodable: Decodable, Encodable {
    let raw: Any
    init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if let v = try? c.decode(String.self) { raw = v; return }
        if let v = try? c.decode(Bool.self)   { raw = v; return }
        if let v = try? c.decode(Double.self) { raw = v; return }
        if let v = try? c.decode(Int.self)    { raw = v; return }
        raw = NSNull()
    }
    func encode(to encoder: Encoder) throws {
        var c = encoder.singleValueContainer()
        switch raw {
        case let v as String: try c.encode(v)
        case let v as Bool:   try c.encode(v)
        case let v as Int:    try c.encode(v)
        case let v as Double: try c.encode(v)
        default:              try c.encodeNil()
        }
    }
}

var candidates: [Candidate] = []
if FileManager.default.fileExists(atPath: poolURL.path) {
    if let data = try? Data(contentsOf: poolURL) {
        candidates = (try? JSONDecoder().decode(Pool.self, from: data).candidates) ?? []
    }
}

// MARK: - Empty pool posture

if candidates.isEmpty {
    print("rotate-fixtures: candidate pool empty at \(poolURL.path).")
    print("rotate-fixtures: blocked on licensing sign-off; locked-core 8 remains authoritative.")
    print("rotate-fixtures: seed = \(seedTrim) (no-op).")
    exit(0)
}

// MARK: - Stratified pick
//
// Deterministic sort by (seed || candidate.id) SHA-256, then take the top
// `count`. Because the seed is pinned per release branch, a release always
// picks the same N fixtures even if the pool changes order. The seed being
// part of the input means different release branches diverge cleanly.

import CryptoKit

func hashKey(_ seed: String, _ id: String) -> String {
    let s = "\(seed)||\(id)"
    let digest = SHA256.hash(data: Data(s.utf8))
    return digest.map { String(format: "%02x", $0) }.joined()
}

let ranked = candidates
    .map { (c: $0, k: hashKey(seedTrim, $0.id)) }
    .sorted { $0.k < $1.k }
let picks = ranked.prefix(options.count).map(\.c)

print("rotate-fixtures: seed=\(seedTrim) pool=\(candidates.count) pick=\(picks.count)")
for p in picks {
    print("  - \(p.id)")
}

if options.dryRun {
    print("rotate-fixtures: --dry-run, not writing manifest.")
    exit(0)
}

// MARK: - Merge into manifest

// We re-read the manifest as a raw JSON object so we can splice the rotating
// set in without round-tripping the full FixtureDescriptor schema (the
// release script and tests remain the authoritative schema owners).

guard
    let manifestData = try? Data(contentsOf: manifestURL),
    var manifestObj = (try? JSONSerialization.jsonObject(with: manifestData)) as? [String: Any],
    var fixtures = manifestObj["fixtures"] as? [[String: Any]]
else {
    FileHandle.standardError.write(Data("error: could not parse manifest at \(manifestURL.path)\n".utf8))
    exit(1)
}

// Drop any existing rotating entries (locked=false) before writing the new set.
fixtures = fixtures.filter { ($0["locked"] as? Bool) == true }

for p in picks {
    var entry: [String: Any] = [
        "id": p.id,
        "file": p.file,
        "sha256": p.sha256,
        "durationSec": p.durationSec,
        "licensingRef": p.licensingRef,
        "locked": false,
        "slot": 0,
    ]
    // taxonomy passthrough
    var tax: [String: Any] = [:]
    for (k, v) in p.taxonomy { tax[k] = v.raw }
    entry["taxonomy"] = tax
    if let s = p.synthetic { entry["synthetic"] = s }
    if let d = p.syntheticDurationSec { entry["syntheticDurationSec"] = d }
    fixtures.append(entry)
}

manifestObj["fixtures"] = fixtures

let out = try JSONSerialization.data(
    withJSONObject: manifestObj,
    options: [.prettyPrinted, .sortedKeys]
)
try out.write(to: manifestURL)
print("rotate-fixtures: wrote \(picks.count) rotating entries to \(manifestURL.path)")
