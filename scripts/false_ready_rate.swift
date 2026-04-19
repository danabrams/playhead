#!/usr/bin/env swift

// false_ready_rate.swift
// Reference aggregation helper for playhead-o45p / Wave 4 dogfood
// pass criterion 3.
//
// Reads one or more JSON Lines session files produced by
// SurfaceStatusInvariantLogger, filters to the two event kinds
// introduced by playhead-o45p (ready_entered and auto_skip_fired),
// and prints the false_ready_rate ratio:
//
//   denominator = count(ready_entered) for a given episode_id_hash
//   numerator   = count(ready_entered for episode X where NO
//                       auto_skip_fired for X fired within the
//                       configured window AFTER the ready event)
//   false_ready_rate = numerator / denominator
//
// Usage:
//   swift scripts/false_ready_rate.swift <path> [--window-seconds N]
//
// `<path>` is a directory (all *.jsonl files recursively) or a single
// JSONL file. `--window-seconds` defaults to 60.
//
// This is a REFERENCE implementation. It does not pretend to be the
// dogfood audit's production tooling — its job is to prove the metric
// is mechanically computable from the JSONL schema the o45p bead
// introduced. The 10-day audit can port this logic into whatever shell
// the engineer running the audit prefers.

import Foundation

// MARK: - CLI parsing

struct Options {
    var paths: [String] = []
    var windowSeconds: Double = 60.0
}

func parseArgs(_ argv: [String]) -> Options {
    var opts = Options()
    var i = 1
    while i < argv.count {
        let arg = argv[i]
        switch arg {
        case "--window-seconds":
            if i + 1 < argv.count, let n = Double(argv[i + 1]) {
                opts.windowSeconds = n
                i += 2
            } else {
                fatalError("--window-seconds requires a numeric value")
            }
        case "--help", "-h":
            printUsage()
            exit(0)
        default:
            opts.paths.append(arg)
            i += 1
        }
    }
    if opts.paths.isEmpty {
        printUsage()
        exit(2)
    }
    return opts
}

func printUsage() {
    let msg = """
    Usage: swift scripts/false_ready_rate.swift <path> [<path> ...] [--window-seconds N]

      <path>             Either a directory (recursively scanned for *.jsonl
                         files) or a single JSONL session file.
      --window-seconds N Match window after a ready_entered event in which
                         an auto_skip_fired on the same episode counts as
                         "fired". Default: 60.

    Output: one line per input path with the total denominator, numerator,
            and false_ready_rate percentage, plus a rollup across all inputs.
    """
    FileHandle.standardError.write(Data(msg.utf8))
    FileHandle.standardError.write(Data("\n".utf8))
}

// MARK: - Minimal JSONL event model
//
// We deliberately do NOT share code with the app's
// SurfaceStateTransitionEntry — that would force this script to compile
// against the full Playhead target. Parsing only the fields the metric
// needs keeps the script self-contained and readable.

struct Event {
    let timestamp: Date
    let episodeIdHash: String?
    let eventType: String
    let windowStartMs: Int?
    let windowEndMs: Int?
}

// MARK: - IO

func collectJsonlPaths(_ roots: [String]) -> [URL] {
    let fm = FileManager.default
    var result: [URL] = []
    for root in roots {
        let url = URL(fileURLWithPath: root)
        var isDir: ObjCBool = false
        guard fm.fileExists(atPath: url.path, isDirectory: &isDir) else {
            FileHandle.standardError.write(Data("skip: \(root) does not exist\n".utf8))
            continue
        }
        if isDir.boolValue {
            if let enumerator = fm.enumerator(
                at: url,
                includingPropertiesForKeys: nil
            ) {
                for case let child as URL in enumerator {
                    if child.pathExtension.lowercased() == "jsonl" {
                        result.append(child)
                    }
                }
            }
        } else {
            result.append(url)
        }
    }
    return result
}

func readEvents(from url: URL) -> [Event] {
    guard let data = try? Data(contentsOf: url) else { return [] }
    let iso = ISO8601DateFormatter()
    iso.formatOptions = [.withInternetDateTime]
    var events: [Event] = []
    for line in String(decoding: data, as: UTF8.self)
        .split(separator: "\n", omittingEmptySubsequences: true)
    {
        guard let json = try? JSONSerialization.jsonObject(with: Data(line.utf8))
                as? [String: Any] else { continue }
        let tsStr = json["timestamp"] as? String ?? ""
        guard let ts = iso.date(from: tsStr) else { continue }
        let episodeIdHash = json["episode_id_hash"] as? String
        // Default: legacy entries with no event_type are invariant_violation.
        let eventType = (json["event_type"] as? String) ?? "invariant_violation"
        let windowStartMs = json["window_start_ms"] as? Int
        let windowEndMs = json["window_end_ms"] as? Int
        events.append(
            Event(
                timestamp: ts,
                episodeIdHash: episodeIdHash,
                eventType: eventType,
                windowStartMs: windowStartMs,
                windowEndMs: windowEndMs
            )
        )
    }
    return events
}

// MARK: - Metric

struct MetricResult {
    let denominator: Int
    let numerator: Int
    var rate: Double {
        guard denominator > 0 else { return 0.0 }
        return Double(numerator) / Double(denominator)
    }
}

/// Compute false_ready_rate over a flat event stream.
///
/// Every `ready_entered` event counts once in the denominator. A given
/// `ready_entered` event counts in the numerator ("false ready") when
/// no `auto_skip_fired` event on the same `episode_id_hash` arrives in
/// [readyTimestamp, readyTimestamp + windowSeconds].
func computeFalseReadyRate(
    events: [Event],
    windowSeconds: Double
) -> MetricResult {
    // Group auto_skip_fired events by episode hash, keeping a sorted
    // timestamp list so the lookup per ready event is O(log n).
    var skipsByEpisode: [String: [Date]] = [:]
    for event in events where event.eventType == "auto_skip_fired" {
        guard let hash = event.episodeIdHash else { continue }
        skipsByEpisode[hash, default: []].append(event.timestamp)
    }
    for key in skipsByEpisode.keys {
        skipsByEpisode[key]?.sort()
    }

    var denominator = 0
    var numerator = 0
    for event in events where event.eventType == "ready_entered" {
        denominator += 1
        guard let hash = event.episodeIdHash,
              let skips = skipsByEpisode[hash]
        else {
            numerator += 1
            continue
        }
        let lo = event.timestamp
        let hi = event.timestamp.addingTimeInterval(windowSeconds)
        let matched = skips.contains { $0 >= lo && $0 <= hi }
        if !matched {
            numerator += 1
        }
    }
    return MetricResult(denominator: denominator, numerator: numerator)
}

// MARK: - main

let options = parseArgs(CommandLine.arguments)
let urls = collectJsonlPaths(options.paths)
if urls.isEmpty {
    FileHandle.standardError.write(Data("no JSONL files found\n".utf8))
    exit(1)
}

print("window_seconds=\(options.windowSeconds)")
print("file\tready_entered\tfalse_ready\tfalse_ready_rate")

var allEvents: [Event] = []
for url in urls {
    let events = readEvents(from: url)
    allEvents.append(contentsOf: events)
    let metric = computeFalseReadyRate(events: events, windowSeconds: options.windowSeconds)
    let pct = String(format: "%.2f%%", metric.rate * 100.0)
    print("\(url.lastPathComponent)\t\(metric.denominator)\t\(metric.numerator)\t\(pct)")
}

let rollup = computeFalseReadyRate(events: allEvents, windowSeconds: options.windowSeconds)
let rollupPct = String(format: "%.2f%%", rollup.rate * 100.0)
print("---")
print("ROLLUP\t\(rollup.denominator)\t\(rollup.numerator)\t\(rollupPct)")
