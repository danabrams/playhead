#!/usr/bin/env swift

// false_ready_rate.swift
// Reference aggregation helper for playhead-o45p / Wave 4 dogfood
// pass criterion 3.
//
// Reads one or more JSON Lines session files produced by
// SurfaceStatusInvariantLogger, or the single-file JSON archive produced by
// Settings → Diagnostics → Export dogfood logs. It filters to the two event
// kinds introduced by playhead-o45p (ready_entered and auto_skip_fired), and
// prints the false_ready_rate ratio:
//
//   denominator = count(ready_entered) for a given episode_id_hash
//   numerator   = count(ready_entered for episode X where NO
//                       auto_skip_fired for X fired within the
//                       configured window AFTER the ready event)
//   false_ready_rate = numerator / denominator
//
// Usage:
//   swift scripts/false_ready_rate.swift <path> [--window-seconds N] [--audit]
//
// `<path>` is a directory (all *.jsonl files and dogfood archive *.json
// files recursively), a single JSONL file, or a single dogfood archive.
// `--window-seconds` defaults to 60.
//
// This is a REFERENCE implementation. It does not pretend to be the
// dogfood audit's production tooling — its job is to prove the metric
// is mechanically computable from the JSONL schema the o45p bead
// introduced. The 10-day audit can port this logic into whatever shell
// the engineer running the audit prefers.
//
// Smoke-test fixture:
//   swift scripts/false_ready_rate.swift scripts/fixtures/false_ready_rate_sample.jsonl
//
// The fixture covers the four behaviourally-distinct scenarios:
//   1. ready_entered without any matching auto_skip_fired → "false ready"
//   2. ready_entered followed by auto_skip_fired within the window → "true ready"
//   3. auto_skip_fired with no preceding ready_entered (neither denom nor numer)
//   4. ready_entered for one episode, auto_skip_fired for a DIFFERENT episode
//      within the window → "false ready" (skips do not cross episode hashes)
//
// Expected strict output: denominator=3, numerator=2, rate=66.67%.
// The script also splits rollups by ready_entered trigger so cold-start
// observations do not get mistaken for analysis-completed dogfood failures.
//
// Audit mode:
//   swift scripts/false_ready_rate.swift <path> --audit
//
// Audit mode keeps the raw reference rollup but adds a session-aware
// dogfood gate classification. Its playback proxy is a unique
// (session_id, episode_id_hash) pair that emitted auto_skip_fired.
// ready_entered rows without same-session playback evidence are
// unscored rather than treated as confirmed false-ready failures.

import Foundation

// MARK: - CLI parsing

struct Options {
    var paths: [String] = []
    var windowSeconds: Double = 60.0
    var auditMode = false
    var minPlaybackProxies = 20
}

func parseArgs(_ argv: [String]) -> Options {
    var opts = Options()
    var i = 1
    while i < argv.count {
        let arg = argv[i]
        switch arg {
        case "--audit":
            opts.auditMode = true
            i += 1
        case "--window-seconds":
            if i + 1 < argv.count, let n = Double(argv[i + 1]) {
                opts.windowSeconds = n
                i += 2
            } else {
                fatalError("--window-seconds requires a numeric value")
            }
        case "--min-playback-proxies":
            if i + 1 < argv.count, let n = Int(argv[i + 1]), n >= 0 {
                opts.minPlaybackProxies = n
                i += 2
            } else {
                fatalError("--min-playback-proxies requires a non-negative integer")
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
    Usage: swift scripts/false_ready_rate.swift <path> [<path> ...] [--window-seconds N] [--audit]

      <path>             Either a directory (recursively scanned for *.jsonl
                         files and dogfood archive *.json files), a single
                         JSONL session file, or a dogfood archive exported
                         from Settings > Diagnostics.
      --window-seconds N Match window after a ready_entered event in which
                         an auto_skip_fired on the same episode counts as
                         "fired". Default: 60.
      --audit            Add session-aware dogfood gate classification.
      --min-playback-proxies N
                         Minimum unique (session_id, episode_id_hash)
                         auto_skip_fired pairs required for audit PASS.
                         Default: 20.

    Output: one line per input path with the total denominator, numerator,
            and false_ready_rate percentage, plus a rollup across all inputs.
            With --audit, also prints playback-proxy and ready-transition
            classification tables plus DOGFOOD_GATE_AUDIT.
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
    let source: String
    let timestamp: Date
    let sessionId: String?
    let episodeIdHash: String?
    let eventType: String
    let entryTrigger: String?
    let windowStartMs: Int?
    let windowEndMs: Int?
}

struct EventSource {
    let label: String
    let events: [Event]
}

// MARK: - IO

func collectEventSources(_ roots: [String]) -> [EventSource] {
    let fm = FileManager.default
    var result: [EventSource] = []
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
                        result.append(readJsonlSource(from: child))
                    } else if child.pathExtension.lowercased() == "json" {
                        result.append(contentsOf: readDogfoodArchiveSources(from: child))
                    }
                }
            }
        } else {
            if url.pathExtension.lowercased() == "json" {
                result.append(contentsOf: readDogfoodArchiveSources(from: url))
            } else {
                result.append(readJsonlSource(from: url))
            }
        }
    }
    return result
}

func readJsonlSource(from url: URL) -> EventSource {
    let label = url.lastPathComponent
    guard let data = try? Data(contentsOf: url) else {
        return EventSource(label: label, events: [])
    }
    let text = String(decoding: data, as: UTF8.self)
    return EventSource(label: label, events: readEvents(fromJSONL: text, source: label))
}

func readDogfoodArchiveSources(from url: URL) -> [EventSource] {
    guard let data = try? Data(contentsOf: url),
          let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
          let schema = json["schema_version"] as? Int,
          schema == 1,
          let files = json["files"] as? [[String: Any]]
    else {
        return []
    }

    return files.compactMap { file in
        guard let filename = file["filename"] as? String,
              filename.hasSuffix(".jsonl"),
              let content = file["content"] as? String
        else {
            return nil
        }
        let label = "\(url.lastPathComponent):\(filename)"
        return EventSource(
            label: label,
            events: readEvents(fromJSONL: content, source: label)
        )
    }
}

func readEvents(fromJSONL text: String, source: String) -> [Event] {
    let iso = ISO8601DateFormatter()
    iso.formatOptions = [.withInternetDateTime]
    var events: [Event] = []
    for line in text.split(separator: "\n", omittingEmptySubsequences: true) {
        guard let json = try? JSONSerialization.jsonObject(with: Data(line.utf8))
                as? [String: Any] else { continue }
        let tsStr = json["timestamp"] as? String ?? ""
        guard let ts = iso.date(from: tsStr) else { continue }
        let sessionId = json["session_id"] as? String
        let episodeIdHash = json["episode_id_hash"] as? String
        // Default: legacy entries with no event_type are invariant_violation.
        let eventType = (json["event_type"] as? String) ?? "invariant_violation"
        let entryTrigger = json["entry_trigger"] as? String
        let windowStartMs = json["window_start_ms"] as? Int
        let windowEndMs = json["window_end_ms"] as? Int
        events.append(
            Event(
                source: source,
                timestamp: ts,
                sessionId: sessionId,
                episodeIdHash: episodeIdHash,
                eventType: eventType,
                entryTrigger: entryTrigger,
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
    windowSeconds: Double,
    triggerFilter: String? = nil
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
        if let triggerFilter,
           (event.entryTrigger ?? "none") != triggerFilter {
            continue
        }
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

func readyTriggers(in events: [Event]) -> [String] {
    Array(Set(events
        .filter { $0.eventType == "ready_entered" }
        .map { $0.entryTrigger ?? "none" }))
        .sorted()
}

// MARK: - Dogfood audit classification

struct SessionEpisodeKey: Hashable {
    let sessionId: String
    let episodeIdHash: String
}

struct AuditResult {
    let autoSkipFired: Int
    let sessionsWithAutoSkip: Int
    let uniqueSessionEpisodePairs: Int
    let uniqueEpisodeHashesWithAutoSkip: Int
    let readyEntered: Int
    let classificationCounts: [String: Int]
    let triggerClassificationCounts: [String: [String: Int]]

    var confirmedFalseReady: Int {
        classificationCounts["confirmed_false_ready", default: 0]
    }
}

let auditClassificationOrder = [
    "same_session_skip_after_ready",
    "same_session_skip_after_ready_within_window",
    "same_session_skip_after_ready_late",
    "skip_before_ready_only",
    "no_playback_evidence",
    "confirmed_false_ready",
]

func computeAuditResult(events: [Event], windowSeconds: Double) -> AuditResult {
    var skipsByKey: [SessionEpisodeKey: [Event]] = [:]
    var sessionsWithAutoSkip = Set<String>()
    var episodeHashesWithAutoSkip = Set<String>()
    var autoSkipFired = 0

    for event in events where event.eventType == "auto_skip_fired" {
        autoSkipFired += 1
        guard let sessionId = event.sessionId,
              let episodeIdHash = event.episodeIdHash
        else { continue }
        sessionsWithAutoSkip.insert(sessionId)
        episodeHashesWithAutoSkip.insert(episodeIdHash)
        let key = SessionEpisodeKey(
            sessionId: sessionId,
            episodeIdHash: episodeIdHash
        )
        skipsByKey[key, default: []].append(event)
    }

    for key in Array(skipsByKey.keys) {
        skipsByKey[key]?.sort { $0.timestamp < $1.timestamp }
    }

    var classificationCounts: [String: Int] = [:]
    var triggerClassificationCounts: [String: [String: Int]] = [:]
    var readyEntered = 0

    func record(_ classification: String, trigger: String) {
        classificationCounts[classification, default: 0] += 1
        triggerClassificationCounts[trigger, default: [:]][classification, default: 0] += 1
    }

    for ready in events where ready.eventType == "ready_entered" {
        readyEntered += 1
        let trigger = ready.entryTrigger ?? "none"
        guard let sessionId = ready.sessionId,
              let episodeIdHash = ready.episodeIdHash
        else {
            record("no_playback_evidence", trigger: trigger)
            continue
        }

        let key = SessionEpisodeKey(
            sessionId: sessionId,
            episodeIdHash: episodeIdHash
        )
        guard let skips = skipsByKey[key], !skips.isEmpty else {
            record("no_playback_evidence", trigger: trigger)
            continue
        }

        if let nextSkip = skips.first(where: { $0.timestamp >= ready.timestamp }) {
            record("same_session_skip_after_ready", trigger: trigger)
            let delta = nextSkip.timestamp.timeIntervalSince(ready.timestamp)
            if delta <= windowSeconds {
                record("same_session_skip_after_ready_within_window", trigger: trigger)
            } else {
                record("same_session_skip_after_ready_late", trigger: trigger)
            }
        } else if skips.contains(where: { $0.timestamp < ready.timestamp }) {
            record("skip_before_ready_only", trigger: trigger)
        } else {
            record("confirmed_false_ready", trigger: trigger)
        }
    }

    return AuditResult(
        autoSkipFired: autoSkipFired,
        sessionsWithAutoSkip: sessionsWithAutoSkip.count,
        uniqueSessionEpisodePairs: skipsByKey.keys.count,
        uniqueEpisodeHashesWithAutoSkip: episodeHashesWithAutoSkip.count,
        readyEntered: readyEntered,
        classificationCounts: classificationCounts,
        triggerClassificationCounts: triggerClassificationCounts
    )
}

func printAuditResult(
    _ result: AuditResult,
    windowSeconds: Double,
    minPlaybackProxies: Int
) {
    print("---")
    print("audit_mode=playback_window")
    print("audit_window_seconds=\(windowSeconds)")
    print("audit_note\tPlayback proxy is a unique session_id + episode_id_hash pair with auto_skip_fired. ready_entered rows without same-session playback evidence are unscored, not confirmed false-ready failures.")
    print("audit_playback_proxies\tsessions_with_auto_skip\tunique_session_episode_pairs\tunique_episode_hashes\tauto_skip_fired")
    print("audit_playback_proxies\t\(result.sessionsWithAutoSkip)\t\(result.uniqueSessionEpisodePairs)\t\(result.uniqueEpisodeHashesWithAutoSkip)\t\(result.autoSkipFired)")
    print("---")
    print("audit_ready_classification\tclassification\tcount")
    print("audit_ready_classification\tready_entered\t\(result.readyEntered)")
    for classification in auditClassificationOrder {
        print("audit_ready_classification\t\(classification)\t\(result.classificationCounts[classification, default: 0])")
    }

    let triggers = result.triggerClassificationCounts.keys.sorted()
    if !triggers.isEmpty {
        print("---")
        print("audit_by_trigger\ttrigger\tclassification\tcount")
        for trigger in triggers {
            let counts = result.triggerClassificationCounts[trigger, default: [:]]
            for classification in auditClassificationOrder {
                let count = counts[classification, default: 0]
                if count > 0 {
                    print("audit_by_trigger\t\(trigger)\t\(classification)\t\(count)")
                }
            }
        }
    }

    let conclusion: String
    if result.confirmedFalseReady > 0 {
        conclusion = "FAIL"
    } else if result.uniqueSessionEpisodePairs < minPlaybackProxies {
        conclusion = "INSUFFICIENT_PLAYBACK_PROXIES"
    } else {
        conclusion = "PASS"
    }

    print("---")
    print("DOGFOOD_GATE_AUDIT\t\(conclusion)\tconfirmed_false_ready\t\(result.confirmedFalseReady)\tplayback_proxy_pairs\t\(result.uniqueSessionEpisodePairs)\tmin_playback_proxy_pairs\t\(minPlaybackProxies)")
}

// MARK: - main

let options = parseArgs(CommandLine.arguments)
let sources = collectEventSources(options.paths)
if sources.isEmpty {
    FileHandle.standardError.write(Data("no JSONL files or dogfood archives found\n".utf8))
    exit(1)
}

print("window_seconds=\(options.windowSeconds)")
print("file\tready_entered\tfalse_ready\tfalse_ready_rate")

var allEvents: [Event] = []
for source in sources {
    let events = source.events
    allEvents.append(contentsOf: events)
    let metric = computeFalseReadyRate(events: events, windowSeconds: options.windowSeconds)
    let pct = String(format: "%.2f%%", metric.rate * 100.0)
    print("\(source.label)\t\(metric.denominator)\t\(metric.numerator)\t\(pct)")
}

let rollup = computeFalseReadyRate(events: allEvents, windowSeconds: options.windowSeconds)
let rollupPct = String(format: "%.2f%%", rollup.rate * 100.0)
print("---")
print("ROLLUP\t\(rollup.denominator)\t\(rollup.numerator)\t\(rollupPct)")

let triggers = readyTriggers(in: allEvents)
if !triggers.isEmpty {
    print("---")
    print("rollup_by_trigger\tready_entered\tfalse_ready\tfalse_ready_rate")
    for trigger in triggers {
        let metric = computeFalseReadyRate(
            events: allEvents,
            windowSeconds: options.windowSeconds,
            triggerFilter: trigger
        )
        let pct = String(format: "%.2f%%", metric.rate * 100.0)
        print("\(trigger)\t\(metric.denominator)\t\(metric.numerator)\t\(pct)")
    }
    if triggers.contains("analysis_completed") {
        let metric = computeFalseReadyRate(
            events: allEvents,
            windowSeconds: options.windowSeconds,
            triggerFilter: "analysis_completed"
        )
        let pct = String(format: "%.2f%%", metric.rate * 100.0)
        print("---")
        print("DOGFOOD_GATE_CANDIDATE\tanalysis_completed\t\(metric.denominator)\t\(metric.numerator)\t\(pct)")
    }
    if triggers.contains("cold_start") {
        print("note\tcold_start rows are initial observed ready states; inspect separately before treating them as false-ready failures.")
    }
}

if options.auditMode {
    let audit = computeAuditResult(
        events: allEvents,
        windowSeconds: options.windowSeconds
    )
    printAuditResult(
        audit,
        windowSeconds: options.windowSeconds,
        minPlaybackProxies: options.minPlaybackProxies
    )
}
