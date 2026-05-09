#!/usr/bin/env swift

// l2f-draft-annotation.swift
// Builds review-only playhead-l2f annotation drafts from timestamped local
// ASR transcripts. The output goes to TestFixtures/Corpus/Drafts, not the
// committed Annotations directory. A human should review and promote the
// draft before it becomes corpus ground truth.

import CryptoKit
import Foundation

struct Options {
    var transcriptDir = "TestFixtures/Corpus/Transcripts"
    var audioDir = "TestFixtures/Corpus/Audio"
    var draftDir = "TestFixtures/Corpus/Drafts"
    var showName: String?
    var episodeId: String?
    var force = false
    var allowMissingAudio = false
    var durationOverride: Double?
    var paddingSeconds = 2.0
    var mergeGapSeconds = 20.0
    var inputs: [String] = []
}

struct TranscriptSegment {
    let start: Double
    let end: Double
    let text: String
}

struct Candidate {
    var start: Double
    var end: Double
    var phrases: Set<String>
    var segmentIndexes: [Int]
}

func printUsage() {
    let msg = """
    Usage: swift scripts/l2f-draft-annotation.swift [options] [transcript-json ...]

      --transcript-dir PATH  Directory scanned when no explicit transcripts are passed.
                             Default: TestFixtures/Corpus/Transcripts
      --audio-dir PATH       Audio directory for matching <episode_id>.<ext> files.
                             Default: TestFixtures/Corpus/Audio
      --draft-dir PATH       Output directory for *.draft.json and *.review.md.
                             Default: TestFixtures/Corpus/Drafts
      --show-name NAME       Show name to put into generated drafts.
      --episode-id ID        Episode id override. Only valid with one transcript input.
      --force                Overwrite existing drafts.
      --allow-missing-audio  Permit draft generation without an audio file. Uses a
                             placeholder fingerprint and transcript-derived duration.
      --duration SECONDS     Duration override. Useful only with --allow-missing-audio.
      --padding-seconds N    Pad heuristic ad windows by N seconds. Default: 2.
      --merge-gap-seconds N  Merge candidate hits separated by up to N seconds. Default: 20.

    Supported transcript JSON shapes include whisper.cpp 'transcription',
    OpenAI/Whisper 'segments', and arrays of {start,end,text}.
    """
    FileHandle.standardError.write(Data(msg.utf8))
    FileHandle.standardError.write(Data("\n".utf8))
}

func parseArgs(_ argv: [String]) -> Options {
    var opts = Options()
    var i = 1
    while i < argv.count {
        let arg = argv[i]
        switch arg {
        case "--transcript-dir":
            i += 1
            guard i < argv.count else { fatal("--transcript-dir requires a path") }
            opts.transcriptDir = argv[i]
        case "--audio-dir":
            i += 1
            guard i < argv.count else { fatal("--audio-dir requires a path") }
            opts.audioDir = argv[i]
        case "--draft-dir":
            i += 1
            guard i < argv.count else { fatal("--draft-dir requires a path") }
            opts.draftDir = argv[i]
        case "--show-name":
            i += 1
            guard i < argv.count else { fatal("--show-name requires a value") }
            opts.showName = argv[i]
        case "--episode-id":
            i += 1
            guard i < argv.count else { fatal("--episode-id requires a value") }
            opts.episodeId = argv[i]
        case "--force":
            opts.force = true
        case "--allow-missing-audio":
            opts.allowMissingAudio = true
        case "--duration":
            i += 1
            guard i < argv.count, let seconds = Double(argv[i]), seconds > 0 else {
                fatal("--duration requires a positive number")
            }
            opts.durationOverride = seconds
        case "--padding-seconds":
            i += 1
            guard i < argv.count, let seconds = Double(argv[i]), seconds >= 0 else {
                fatal("--padding-seconds requires a non-negative number")
            }
            opts.paddingSeconds = seconds
        case "--merge-gap-seconds":
            i += 1
            guard i < argv.count, let seconds = Double(argv[i]), seconds >= 0 else {
                fatal("--merge-gap-seconds requires a non-negative number")
            }
            opts.mergeGapSeconds = seconds
        case "-h", "--help":
            printUsage()
            exit(0)
        default:
            opts.inputs.append(arg)
        }
        i += 1
    }
    if opts.episodeId != nil && opts.inputs.count > 1 {
        fatal("--episode-id can only be used with a single transcript")
    }
    return opts
}

func fatal(_ message: String) -> Never {
    FileHandle.standardError.write(Data("error: \(message)\n".utf8))
    exit(2)
}

let scriptURL = URL(fileURLWithPath: CommandLine.arguments[0]).standardizedFileURL
let repoRoot = scriptURL.deletingLastPathComponent().deletingLastPathComponent()

func resolve(_ path: String) -> URL {
    if path.hasPrefix("/") {
        return URL(fileURLWithPath: path)
    }
    return repoRoot.appendingPathComponent(path)
}

let options = parseArgs(CommandLine.arguments)
let transcriptDir = resolve(options.transcriptDir)
let audioDir = resolve(options.audioDir)
let draftDir = resolve(options.draftDir)
try FileManager.default.createDirectory(at: draftDir, withIntermediateDirectories: true)

func collectTranscripts() -> [URL] {
    if !options.inputs.isEmpty {
        return options.inputs.map(resolve)
    }
    guard let files = try? FileManager.default.contentsOfDirectory(
        at: transcriptDir,
        includingPropertiesForKeys: nil,
        options: [.skipsHiddenFiles]
    ) else {
        fatal("could not read transcript directory at \(transcriptDir.path)")
    }
    return files
        .filter { $0.pathExtension.lowercased() == "json" }
        .sorted { $0.lastPathComponent < $1.lastPathComponent }
}

func number(_ value: Any?) -> Double? {
    switch value {
    case let v as Double: return v
    case let v as Int: return Double(v)
    case let v as NSNumber: return v.doubleValue
    case let v as String: return Double(v)
    default: return nil
    }
}

func timecodeSeconds(_ raw: String) -> Double? {
    let cleaned = raw.replacingOccurrences(of: ",", with: ".")
    let parts = cleaned.split(separator: ":").map(String.init)
    guard !parts.isEmpty else { return nil }
    if parts.count == 3,
       let h = Double(parts[0]),
       let m = Double(parts[1]),
       let s = Double(parts[2]) {
        return h * 3600 + m * 60 + s
    }
    if parts.count == 2,
       let m = Double(parts[0]),
       let s = Double(parts[1]) {
        return m * 60 + s
    }
    if parts.count == 1 {
        return Double(parts[0])
    }
    return nil
}

func parseSegment(_ item: Any) -> TranscriptSegment? {
    guard let obj = item as? [String: Any] else { return nil }
    let text = (obj["text"] as? String ?? obj["sentence"] as? String ?? "")
        .trimmingCharacters(in: .whitespacesAndNewlines)
    guard !text.isEmpty else { return nil }

    var start = number(obj["start"] ?? obj["start_seconds"] ?? obj["startSeconds"])
    var end = number(obj["end"] ?? obj["end_seconds"] ?? obj["endSeconds"])

    if (start == nil || end == nil), let offsets = obj["offsets"] as? [String: Any] {
        if let from = number(offsets["from"]), let to = number(offsets["to"]) {
            start = from / 1000.0
            end = to / 1000.0
        }
    }

    if (start == nil || end == nil), let ts = obj["timestamps"] as? [String: Any] {
        if let from = ts["from"] as? String, let to = ts["to"] as? String {
            start = timecodeSeconds(from)
            end = timecodeSeconds(to)
        }
    }

    guard let s = start, let e = end, e > s else { return nil }
    return TranscriptSegment(start: s, end: e, text: text)
}

func parseTranscript(_ url: URL, fallbackDuration: Double?) throws -> [TranscriptSegment] {
    let data = try Data(contentsOf: url)
    let json = try JSONSerialization.jsonObject(with: data)

    if let array = json as? [Any] {
        return array.compactMap(parseSegment).sorted { $0.start < $1.start }
    }

    guard let obj = json as? [String: Any] else {
        throw NSError(domain: "L2FDraft", code: 1, userInfo: [NSLocalizedDescriptionKey: "root JSON is not an object or array"])
    }

    if let segments = obj["segments"] as? [Any] {
        return segments.compactMap(parseSegment).sorted { $0.start < $1.start }
    }
    if let transcription = obj["transcription"] as? [Any] {
        return transcription.compactMap(parseSegment).sorted { $0.start < $1.start }
    }
    if let text = obj["text"] as? String, let duration = fallbackDuration {
        return [TranscriptSegment(start: 0, end: duration, text: text)]
    }
    throw NSError(domain: "L2FDraft", code: 2, userInfo: [NSLocalizedDescriptionKey: "no supported transcript segments found"])
}

let audioExtensions: Set<String> = ["m4a", "mp3", "mp4", "aac", "wav", "flac"]

func matchingAudio(for episodeId: String) -> URL? {
    guard let files = try? FileManager.default.contentsOfDirectory(
        at: audioDir,
        includingPropertiesForKeys: nil,
        options: [.skipsHiddenFiles]
    ) else {
        return nil
    }
    return files.first {
        $0.deletingPathExtension().lastPathComponent == episodeId
            && audioExtensions.contains($0.pathExtension.lowercased())
    }
}

func runCapture(_ executable: String, _ args: [String]) throws -> String {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
    process.arguments = [executable] + args
    let pipe = Pipe()
    process.standardOutput = pipe
    process.standardError = Pipe()
    try process.run()
    process.waitUntilExit()
    guard process.terminationStatus == 0 else {
        throw NSError(domain: "L2FDraft", code: Int(process.terminationStatus), userInfo: [
            NSLocalizedDescriptionKey: "\(executable) exited \(process.terminationStatus)",
        ])
    }
    let data = pipe.fileHandleForReading.readDataToEndOfFile()
    return String(decoding: data, as: UTF8.self)
}

func durationSeconds(of audio: URL) -> Double? {
    let out = try? runCapture("ffprobe", [
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        audio.path,
    ])
    return out.flatMap { Double($0.trimmingCharacters(in: .whitespacesAndNewlines)) }
}

func fingerprint(of url: URL) throws -> String {
    let handle = try FileHandle(forReadingFrom: url)
    defer { try? handle.close() }

    var hasher = SHA256()
    while true {
        let data = try handle.read(upToCount: 1024 * 1024) ?? Data()
        if data.isEmpty { break }
        hasher.update(data: data)
    }
    let hex = hasher.finalize().map { String(format: "%02x", $0) }.joined()
    return "sha256:\(hex)"
}

let literalPhrases = [
    "brought to you by",
    "sponsored by",
    "sponsor of",
    "our sponsor",
    "today's sponsor",
    "this episode is sponsored",
    "this episode is brought",
    "support for this podcast",
    "support for the show",
    "promo code",
    "offer code",
    "use code",
    "free trial",
    "limited time offer",
    "percent off",
    "% off",
    ".com/",
]

func matchedPhrases(in text: String) -> Set<String> {
    let lower = text.lowercased()
    var result = Set<String>()
    for phrase in literalPhrases where lower.contains(phrase) {
        result.insert(phrase)
    }

    let regexPatterns = [
        #"promo code\s+[a-z0-9_-]+"#,
        #"use code\s+[a-z0-9_-]+"#,
        #"\b(go to|visit)\s+[a-z0-9.-]+\.(com|fm|io|co|net|org)\b"#,
        #"[a-z0-9.-]+\.(com|fm|io|co)/[a-z0-9_-]+"#,
        #"[0-9]{1,2}\s*%\s*off"#,
    ]
    for pattern in regexPatterns {
        if lower.range(of: pattern, options: .regularExpression) != nil {
            result.insert(pattern)
        }
    }
    return result
}

func classifyAdType(_ text: String) -> String {
    let lower = text.lowercased()
    if lower.contains("patreon")
        || lower.contains("merch")
        || lower.contains("subscribe to")
        || lower.contains("wherever you get your podcasts") {
        return "promo"
    }
    if lower.contains("sponsor")
        || lower.contains("promo code")
        || lower.contains("offer code")
        || lower.contains("brought to you") {
        return "host_read"
    }
    return "blended_host_read"
}

func classifyTransition(_ text: String) -> String {
    let lower = text.lowercased()
    if lower.contains("brought to you")
        || lower.contains("sponsored by")
        || lower.contains("our sponsor")
        || lower.contains("today's sponsor") {
        return "explicit"
    }
    return "blended"
}

func candidates(from segments: [TranscriptSegment], duration: Double) -> [Candidate] {
    var hits: [Candidate] = []
    for (index, segment) in segments.enumerated() {
        let phrases = matchedPhrases(in: segment.text)
        if !phrases.isEmpty {
            hits.append(Candidate(
                start: max(0, segment.start - options.paddingSeconds),
                end: min(duration, segment.end + options.paddingSeconds),
                phrases: phrases,
                segmentIndexes: [index]
            ))
        }
    }
    guard !hits.isEmpty else { return [] }

    var merged: [Candidate] = []
    for hit in hits {
        if var last = merged.last, hit.start - last.end <= options.mergeGapSeconds {
            last.end = max(last.end, hit.end)
            last.phrases.formUnion(hit.phrases)
            last.segmentIndexes.append(contentsOf: hit.segmentIndexes)
            merged[merged.count - 1] = last
        } else {
            merged.append(hit)
        }
    }
    return merged
}

func contentWindows(duration: Double, ads: [[String: Any]]) -> [[String: Any]] {
    var windows: [[String: Any]] = []
    var cursor = 0.0
    for ad in ads {
        let start = ad["start_seconds"] as? Double ?? 0
        let end = ad["end_seconds"] as? Double ?? start
        if start > cursor {
            windows.append([
                "start_seconds": rounded(cursor),
                "end_seconds": rounded(start),
                "notes": "DRAFT content window; verify against audio before promotion",
            ])
        }
        cursor = max(cursor, end)
    }
    if duration > cursor {
        windows.append([
            "start_seconds": rounded(cursor),
            "end_seconds": rounded(duration),
            "notes": "DRAFT content window; verify against audio before promotion",
        ])
    }
    return windows
}

func rounded(_ value: Double) -> Double {
    (value * 10).rounded() / 10
}

func titleFromEpisodeId(_ episodeId: String) -> String {
    episodeId
        .split { $0 == "-" || $0 == "_" }
        .map { part in
            part.prefix(1).uppercased() + part.dropFirst()
        }
        .joined(separator: " ")
}

func makeReviewReport(
    episodeId: String,
    transcript: URL,
    audio: URL?,
    segments: [TranscriptSegment],
    candidates: [Candidate],
    duration: Double
) -> String {
    var lines: [String] = []
    lines.append("# L2F Draft Review: \(episodeId)")
    lines.append("")
    lines.append("- Transcript: \(transcript.path)")
    lines.append("- Audio: \(audio?.path ?? "missing")")
    lines.append("- Duration seconds: \(String(format: "%.1f", duration))")
    lines.append("- Candidate ad windows: \(candidates.count)")
    lines.append("")
    if candidates.isEmpty {
        lines.append("No transcript heuristic ad candidates were found. Review this as a zero-ad / false-positive-trap candidate before promotion.")
        lines.append("")
        return lines.joined(separator: "\n")
    }

    for (candidateIndex, candidate) in candidates.enumerated() {
        lines.append("## Candidate \(candidateIndex + 1): \(String(format: "%.1f", candidate.start))-\(String(format: "%.1f", candidate.end))")
        lines.append("")
        lines.append("Matched phrases: \(candidate.phrases.sorted().joined(separator: ", "))")
        lines.append("")
        let lowerBound = max(0, (candidate.segmentIndexes.min() ?? 0) - 1)
        let upperBound = min(segments.count - 1, (candidate.segmentIndexes.max() ?? 0) + 1)
        for idx in lowerBound...upperBound {
            let marker = candidate.segmentIndexes.contains(idx) ? "*" : " "
            let seg = segments[idx]
            lines.append("\(marker) [\(String(format: "%.1f", seg.start))-\(String(format: "%.1f", seg.end))] \(seg.text)")
        }
        lines.append("")
    }
    lines.append("Promote only after checking audio boundaries to +/-0.5s and filling advertiser/product when identifiable.")
    lines.append("")
    return lines.joined(separator: "\n")
}

let transcripts = collectTranscripts()
guard !transcripts.isEmpty else {
    print("l2f-draft-annotation: no transcript JSON files found in \(transcriptDir.path)")
    exit(0)
}

var failures = 0
let placeholderFingerprint = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

for transcript in transcripts {
    let episodeId = options.episodeId ?? transcript.deletingPathExtension().lastPathComponent
    let draftURL = draftDir.appendingPathComponent("\(episodeId).draft.json")
    let reviewURL = draftDir.appendingPathComponent("\(episodeId).review.md")

    if FileManager.default.fileExists(atPath: draftURL.path), !options.force {
        print("skip: \(draftURL.path) already exists; pass --force to rebuild")
        continue
    }

    let audio = matchingAudio(for: episodeId)
    if audio == nil && !options.allowMissingAudio {
        FileHandle.standardError.write(Data("missing audio for \(episodeId); pass --allow-missing-audio for a placeholder draft\n".utf8))
        failures += 1
        continue
    }

    let audioDuration = audio.flatMap(durationSeconds)
    let provisionalDuration = options.durationOverride ?? audioDuration

    do {
        let segments = try parseTranscript(transcript, fallbackDuration: provisionalDuration)
        guard !segments.isEmpty else {
            throw NSError(domain: "L2FDraft", code: 3, userInfo: [NSLocalizedDescriptionKey: "transcript has no timestamped segments"])
        }
        let duration = options.durationOverride ?? audioDuration ?? segments.map(\.end).max()!
        let fingerprintValue = try audio.map(fingerprint) ?? placeholderFingerprint
        let foundCandidates = candidates(from: segments, duration: duration)

        let adWindows: [[String: Any]] = foundCandidates.map { candidate in
            let text = candidate.segmentIndexes.map { segments[$0].text }.joined(separator: " ")
            return [
                "start_seconds": rounded(candidate.start),
                "end_seconds": rounded(candidate.end),
                "advertiser": NSNull(),
                "product": NSNull(),
                "ad_type": classifyAdType(text),
                "transition_type": classifyTransition(text),
                "confidence_notes": "DRAFT transcript heuristic; matched: \(candidate.phrases.sorted().joined(separator: ", ")). Review audio boundaries and advertiser/product before promotion.",
            ]
        }

        let annotation: [String: Any] = [
            "episode_id": episodeId,
            "show_name": options.showName ?? titleFromEpisodeId(episodeId),
            "duration_seconds": rounded(duration),
            "ad_windows": adWindows,
            "content_windows": contentWindows(duration: duration, ads: adWindows),
            "variant_of": NSNull(),
            "audio_fingerprint": fingerprintValue,
        ]

        let data = try JSONSerialization.data(
            withJSONObject: annotation,
            options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
        )
        try data.write(to: draftURL)
        try Data(makeReviewReport(
            episodeId: episodeId,
            transcript: transcript,
            audio: audio,
            segments: segments,
            candidates: foundCandidates,
            duration: duration
        ).utf8).write(to: reviewURL)
        print("wrote: \(draftURL.path)")
        print("wrote: \(reviewURL.path)")
    } catch {
        FileHandle.standardError.write(Data("failed \(transcript.path): \(error.localizedDescription)\n".utf8))
        failures += 1
    }
}

if failures > 0 {
    FileHandle.standardError.write(Data("l2f-draft-annotation: \(failures) failure(s)\n".utf8))
    exit(1)
}
