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
    var mergeGapSeconds = 75.0
    var expandBeforeSeconds = 60.0
    var expandAfterSeconds = 45.0
    var maxWindowSeconds = 240.0
    var reviewContextSeconds = 20.0
    var writeReviewQueue = false
    var reviewQueueOnly = false
    var reviewSource: String?
    var reviewQueueName = "review-queue"
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
    var cueSegmentIndexes: [Int]
    var advertiserGuess: String?
    var productGuess: String?
}

struct ReviewQueueCandidate {
    var episodeId: String
    var start: Double?
    var end: Double?
    var duration: Double?
    var advertiserGuess: String?
    var productGuess: String?
    var adType: String?
    var transitionType: String?
    var notes: String
    var source: String
    var isFalsePositiveTrap: Bool
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
      --padding-seconds N    Pad final expanded pod candidates by N seconds. Default: 2.
      --merge-gap-seconds N  Merge cue hits separated by up to N seconds before pod
                             expansion. Default: 75.
      --expand-before-seconds N
                             Expand each merged cue cluster backward across likely ad-copy
                             transcript segments by up to N seconds. Default: 60.
      --expand-after-seconds N
                             Expand each merged cue cluster forward across likely ad-copy
                             transcript segments by up to N seconds. Default: 45.
      --max-window-seconds N Cap any single review-only candidate pod. Default: 240.
      --write-review-queue   Also write <name>.json and <name>.md review queue artifacts
                             under --draft-dir after draft generation.
      --review-queue-only    Do not generate drafts; only write the review queue from
                             existing drafts or --review-source.
      --review-source PATH   Optional Codex transcript-review JSON to consume instead of
                             draft ad_windows for queue candidates.
      --review-context-seconds N
                             Pre/post-roll context around each queue entry. Default: 20.
      --review-queue-name NAME
                             Basename for review queue artifacts. Default: review-queue.

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
            guard isSafeArtifactBasename(argv[i]) else {
                fatal("--episode-id must be a simple filename-safe id without path separators")
            }
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
        case "--expand-before-seconds":
            i += 1
            guard i < argv.count, let seconds = Double(argv[i]), seconds >= 0 else {
                fatal("--expand-before-seconds requires a non-negative number")
            }
            opts.expandBeforeSeconds = seconds
        case "--expand-after-seconds":
            i += 1
            guard i < argv.count, let seconds = Double(argv[i]), seconds >= 0 else {
                fatal("--expand-after-seconds requires a non-negative number")
            }
            opts.expandAfterSeconds = seconds
        case "--max-window-seconds":
            i += 1
            guard i < argv.count, let seconds = Double(argv[i]), seconds > 0 else {
                fatal("--max-window-seconds requires a positive number")
            }
            opts.maxWindowSeconds = seconds
        case "--write-review-queue":
            opts.writeReviewQueue = true
        case "--review-queue-only":
            opts.reviewQueueOnly = true
        case "--review-source":
            i += 1
            guard i < argv.count else { fatal("--review-source requires a path") }
            opts.reviewSource = argv[i]
        case "--review-context-seconds":
            i += 1
            guard i < argv.count, let seconds = Double(argv[i]), seconds >= 0 else {
                fatal("--review-context-seconds requires a non-negative number")
            }
            opts.reviewContextSeconds = seconds
        case "--review-queue-name":
            i += 1
            guard i < argv.count, !argv[i].isEmpty else {
                fatal("--review-queue-name requires a non-empty value")
            }
            guard isSafeArtifactBasename(i < argv.count ? argv[i] : "") else {
                fatal("--review-queue-name must be a simple filename without path separators")
            }
            opts.reviewQueueName = argv[i]
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
    if opts.reviewQueueOnly && !opts.inputs.isEmpty {
        fatal("--review-queue-only does not accept transcript inputs")
    }
    if opts.reviewQueueOnly {
        opts.writeReviewQueue = true
    }
    return opts
}

func isSafeArtifactBasename(_ value: String) -> Bool {
    !value.isEmpty
        && !value.contains("/")
        && !value.contains("\\")
        && value != "."
        && value != ".."
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
guard isAllowedDraftOutputDir(draftDir) else {
    fatal("--draft-dir must be TestFixtures/Corpus/Drafts or a system temporary directory")
}
try FileManager.default.createDirectory(at: draftDir, withIntermediateDirectories: true)

func isAllowedDraftOutputDir(_ url: URL) -> Bool {
    let standardized = url.standardizedFileURL.path
    let corpusDrafts = repoRoot
        .appendingPathComponent("TestFixtures/Corpus/Drafts")
        .standardizedFileURL
        .path
    let temporaryRoots = [
        URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true).standardizedFileURL.path,
        URL(fileURLWithPath: "/tmp", isDirectory: true).standardizedFileURL.path,
    ]
    if standardized == corpusDrafts || standardized.hasPrefix(corpusDrafts + "/") {
        return true
    }
    return temporaryRoots.contains { root in
        standardized == root || standardized.hasPrefix(root + "/")
    }
}

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
    let rawData = try Data(contentsOf: url)
    let data: Data
    if String(data: rawData, encoding: .utf8) == nil {
        data = Data(String(decoding: rawData, as: UTF8.self).utf8)
    } else {
        data = rawData
    }
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
    "our sponsor",
    "today's sponsor",
    "this episode is sponsored",
    "this episode is brought",
    "support for this podcast",
    "support for the show",
    "support for this show",
    "quick break",
    "word from our sponsor",
    "promo code",
    "offer code",
    "use code",
    "use my link",
    "free trial",
    "limited time offer",
    "percent off",
    "% off",
    "dot com slash",
    ".com/",
]

let adCopyExpansionTerms = [
    " sponsor",
    "sponsored",
    "commercial",
    "quick break",
    "brought to you",
    "promo code",
    "offer code",
    "use code",
    "use my link",
    "my link",
    "free trial",
    "trial",
    "limited time",
    "percent off",
    "% off",
    "visit ",
    "go to ",
    "head to ",
    "sign up",
    "get started",
    "learn more",
    "dot com",
    ".com",
    ".fm",
    ".io",
    "membership",
    "credit",
    "discount",
    "shipping",
    "subscribe",
    "subscription",
    "download",
    "insurance",
    "wireless",
    "business class",
    "selected routes",
    "privacy",
    "data removal",
    "water filter",
    "crm",
    "sales",
    "checkout",
    "dashboard",
    "deal",
    "lab test",
    "testing",
    "biomarker",
    "app",
]

func matchedPhrases(in text: String) -> Set<String> {
    let lower = text.lowercased()
    var result = Set<String>()
    for phrase in literalPhrases where lower.contains(phrase) {
        result.insert(phrase)
    }

    let regexPatterns: [(label: String, pattern: String)] = [
        ("promo code", #"promo code\s+[a-z0-9_-]+"#),
        ("use code", #"use code\s+[a-z0-9_-]+"#),
        ("show sponsor", #"\b(sponsor|sponsors|sponsored)\s+(this|the|our)\s+(show|podcast|episode)\b"#),
        ("show sponsor", #"\b(the|this|our)\s+(show|podcast|episode)\s+(sponsor|sponsors|is sponsored)\b"#),
        ("domain cta", #"\b(go to|visit|head to|sign up at)\s+[a-z0-9.-]+\.(com|fm|io|co|net|org)\b"#),
        ("domain cta", #"\b[a-z0-9.-]+\.(com|fm|io|co|net|org)/[a-z0-9_-]+"#),
        ("domain cta", #"\b[a-z0-9.-]+\s+dot\s+(com|fm|io|co|net|org)\s+(slash|/)\s+[a-z0-9_-]+"#),
        ("percent off", #"[0-9]{1,2}\s*%\s*off"#),
        ("percent off", #"[0-9]{1,2}\s*percent\s*off"#),
    ]
    for (label, pattern) in regexPatterns {
        if lower.range(of: pattern, options: .regularExpression) != nil {
            result.insert(label)
        }
    }
    if isLikelyContentSponsorReference(lower) && !hasCommercialSponsorIntent(result) {
        result = result.filter { !$0.contains("sponsor") && $0 != "sponsored by" }
    }
    return result
}

func hasCommercialSponsorIntent(_ phrases: Set<String>) -> Bool {
    phrases.contains("our sponsor")
        || phrases.contains("today's sponsor")
        || phrases.contains("this episode is sponsored")
        || phrases.contains("this episode is brought")
        || phrases.contains("support for this podcast")
        || phrases.contains("support for the show")
        || phrases.contains("support for this show")
        || phrases.contains("word from our sponsor")
        || phrases.contains("show sponsor")
        || phrases.contains("domain cta")
        || phrases.contains("promo code")
        || phrases.contains("offer code")
        || phrases.contains("use code")
}

func isLikelyContentSponsorReference(_ lower: String) -> Bool {
    guard lower.contains("sponsor") else { return false }
    let contentTerms = [
        " bill",
        " legislation",
        " law",
        " act ",
        " senator",
        " congress",
        " parliament",
        " article",
        " journal",
        " research",
        " study",
        " grant",
        " foundation",
        " cycling",
        " team",
        " athlete",
        " league",
        " tournament",
        " sponsorship deal",
    ]
    return contentTerms.contains { lower.contains($0) }
}

func isContextualSponsorFalsePositive(segments: [TranscriptSegment], index: Int, phrases: Set<String>) -> Bool {
    guard phrases.contains("sponsored by") || phrases.contains("show sponsor") else {
        return false
    }
    if hasCommercialSponsorIntent(phrases) && !phrases.subtracting(["sponsored by", "show sponsor"]).isEmpty {
        return false
    }

    let lowerBound = max(0, index - 2)
    let upperBound = min(segments.count - 1, index + 2)
    let context = segments[lowerBound...upperBound]
        .map { $0.text.lowercased() }
        .joined(separator: " ")
    let contentTerms = [
        " bill",
        " legislation",
        " house of representatives",
        " senate",
        " senator",
        " bipartisan",
        " republicans",
        " democrats",
        " law",
        " policy",
        " committee",
        " article",
        " study",
        " grant",
    ]
    return contentTerms.contains { context.contains($0) }
}

func isLikelyAdCopy(_ segment: TranscriptSegment) -> Bool {
    let lower = segment.text.lowercased()
    let phrases = matchedPhrases(in: segment.text)
    if !phrases.isEmpty {
        return true
    }
    if isLikelyContentSponsorReference(lower) {
        return false
    }
    if adCopyExpansionTerms.contains(where: { containsExpansionTerm(lower, term: $0) }) {
        return true
    }
    if lower.range(of: #"\$[0-9]+|[0-9]+\s*dollars?"#, options: .regularExpression) != nil {
        return true
    }
    return lower.range(
        of: #"\b[a-z0-9.-]+\s+dot\s+(com|fm|io|co|net|org)\b"#,
        options: .regularExpression
    ) != nil
}

func containsExpansionTerm(_ lower: String, term: String) -> Bool {
    if term.hasPrefix(" ") || term.hasSuffix(" ") || term.contains(".") || term.contains("%") {
        return lower.contains(term)
    }
    let pattern = #"(?<![a-z0-9])"# + NSRegularExpression.escapedPattern(for: term) + #"(?![a-z0-9])"#
    return lower.range(of: pattern, options: .regularExpression) != nil
}

func isBridgeableAdCopySegment(_ segment: TranscriptSegment) -> Bool {
    let words = segment.text.split { !$0.isLetter && !$0.isNumber }
    return segment.end - segment.start <= 5.0 && words.count <= 12
}

func isStrongContentBoundary(_ text: String) -> Bool {
    let lower = text.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    let prefixes = [
        "now back",
        "back to ",
        "let's get back",
        "let us get back",
        "returning to",
        "we're back",
        "welcome back",
        "today we are",
        "today we're",
        "okay, so",
        "yes, and",
        "where were we",
        "maya,",
    ]
    return prefixes.contains { lower.hasPrefix($0) }
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

func firstCapture(in text: String, pattern: String) -> String? {
    guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
        return nil
    }
    let nsRange = NSRange(text.startIndex..<text.endIndex, in: text)
    guard let match = regex.firstMatch(in: text, options: [], range: nsRange),
          match.numberOfRanges > 1,
          let range = Range(match.range(at: 1), in: text) else {
        return nil
    }
    let value = String(text[range])
        .trimmingCharacters(in: CharacterSet(charactersIn: " .,:;!?\n\t"))
    return value.isEmpty ? nil : value
}

func advertiserFromDomain(_ raw: String) -> String? {
    var domain = raw.lowercased()
        .replacingOccurrences(of: " dot ", with: ".")
        .replacingOccurrences(of: " slash ", with: "/")
        .replacingOccurrences(of: "www.", with: "")
    if let slash = domain.firstIndex(of: "/") {
        domain = String(domain[..<slash])
    }
    let base = domain.split(separator: ".").first.map(String.init) ?? domain
    let cleaned = base.replacingOccurrences(of: "-", with: " ")
    guard !cleaned.isEmpty else { return nil }
    return cleaned
        .split(separator: " ")
        .map { $0.prefix(1).uppercased() + $0.dropFirst() }
        .joined(separator: " ")
}

func cleanAdvertiserGuess(_ raw: String) -> String {
    let delimiters = [
        " makes ",
        " offers ",
        " gives ",
        " lets ",
        " helps ",
        " provides ",
        " has ",
        " is ",
        " are ",
    ]
    let lower = raw.lowercased()
    var cleaned = raw
    for delimiter in delimiters {
        if let range = lower.range(of: delimiter) {
            cleaned = String(raw[..<range.lowerBound])
            break
        }
    }
    return cleaned.trimmingCharacters(in: CharacterSet(charactersIn: " .,:;!?\n\t"))
}

func guessAdvertiser(from text: String) -> String? {
    let patterns = [
        #"brought to you by\s+([A-Z][A-Za-z0-9&' -]{1,60})"#,
        #"sponsored by\s+([A-Z][A-Za-z0-9&' -]{1,60})"#,
        #"today's sponsor[, ]+\s*([A-Z][A-Za-z0-9&' -]{1,60})"#,
        #"our sponsor[, ]+\s*([A-Z][A-Za-z0-9&' -]{1,60})"#,
        #"acknowledge our sponsor[, ]+\s*([A-Z][A-Za-z0-9&' -]{1,60})"#,
    ]
    for pattern in patterns {
        if let value = firstCapture(in: text, pattern: pattern) {
            let cleaned = cleanAdvertiserGuess(value)
            return cleaned.isEmpty ? value : cleaned
        }
    }
    if let domain = firstCapture(
        in: text,
        pattern: #"(?:go to|visit|head to|sign up at)\s+([A-Za-z0-9.-]+\.(?:com|fm|io|co|net|org)(?:/[A-Za-z0-9_-]+)?)"#
    ) {
        return advertiserFromDomain(domain)
    }
    if let domain = firstCapture(
        in: text,
        pattern: #"([A-Za-z0-9.-]+\s+dot\s+(?:com|fm|io|co|net|org)(?:\s+slash\s+[A-Za-z0-9_-]+)?)"#
    ) {
        return advertiserFromDomain(domain)
    }
    return nil
}

func guessProduct(from text: String) -> String? {
    let lower = text.lowercased()
    let productHints: [(needle: String, value: String)] = [
        ("water filter", "water filter"),
        ("business class", "business class travel"),
        ("crm", "CRM"),
        ("sales", "sales software"),
        ("checkout", "commerce"),
        ("membership", "membership"),
        ("lab test", "health testing"),
        ("biomarker", "health testing"),
        ("insurance", "insurance"),
        ("wireless", "wireless service"),
        ("credit card", "credit card"),
        ("subscription", "subscription"),
        ("free trial", "free trial"),
        ("privacy", "privacy service"),
        ("data removal", "data removal service"),
    ]
    return productHints.first { lower.contains($0.needle) }?.value
}

func candidates(from segments: [TranscriptSegment], duration: Double) -> [Candidate] {
    var hits: [Candidate] = []
    for (index, segment) in segments.enumerated() {
        let phrases = matchedPhrases(in: segment.text)
        if !phrases.isEmpty {
            if isContextualSponsorFalsePositive(segments: segments, index: index, phrases: phrases) {
                continue
            }
            hits.append(Candidate(
                start: segment.start,
                end: segment.end,
                phrases: phrases,
                segmentIndexes: [index],
                cueSegmentIndexes: [index],
                advertiserGuess: nil,
                productGuess: nil
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
            last.cueSegmentIndexes.append(contentsOf: hit.cueSegmentIndexes)
            merged[merged.count - 1] = last
        } else {
            merged.append(hit)
        }
    }
    let expanded = merged.map { expandCandidate($0, segments: segments, duration: duration) }
    return mergeExpandedCandidates(expanded, segments: segments)
}

func mergeExpandedCandidates(_ candidates: [Candidate], segments: [TranscriptSegment]) -> [Candidate] {
    var merged: [Candidate] = []
    for candidate in candidates.sorted(by: { $0.start < $1.start }) {
        guard var last = merged.last, candidate.start <= last.end else {
            merged.append(candidate)
            continue
        }
        last.end = max(last.end, candidate.end)
        last.phrases.formUnion(candidate.phrases)
        last.segmentIndexes = Array(Set(last.segmentIndexes + candidate.segmentIndexes)).sorted()
        last.cueSegmentIndexes = Array(Set(last.cueSegmentIndexes + candidate.cueSegmentIndexes)).sorted()
        let text = last.segmentIndexes.map { segments[$0].text }.joined(separator: " ")
        last.advertiserGuess = last.advertiserGuess ?? candidate.advertiserGuess ?? guessAdvertiser(from: text)
        last.productGuess = last.productGuess ?? candidate.productGuess ?? guessProduct(from: text)
        merged[merged.count - 1] = last
    }
    return merged
}

func expandCandidate(_ candidate: Candidate, segments: [TranscriptSegment], duration: Double) -> Candidate {
    guard let firstCueIndex = candidate.cueSegmentIndexes.min(),
          let lastCueIndex = candidate.cueSegmentIndexes.max() else {
        return candidate
    }

    var included = Set(firstCueIndex...lastCueIndex)
    var left = firstCueIndex - 1
    var bridgedLeft = 0
    while left >= 0 {
        let segment = segments[left]
        if candidate.start - segment.end > options.expandBeforeSeconds {
            break
        }
        if isStrongContentBoundary(segment.text) && !isLikelyAdCopy(segment) {
            break
        }
        if isLikelyAdCopy(segment) {
            included.insert(left)
            bridgedLeft = 0
        } else if bridgedLeft < 2 && isBridgeableAdCopySegment(segment) {
            included.insert(left)
            bridgedLeft += 1
        } else {
            break
        }
        left -= 1
    }

    var right = lastCueIndex + 1
    var bridgedRight = 0
    while right < segments.count {
        let segment = segments[right]
        if segment.start - candidate.end > options.expandAfterSeconds {
            break
        }
        if isStrongContentBoundary(segment.text) && !isLikelyAdCopy(segment) {
            break
        }
        if isLikelyAdCopy(segment) {
            included.insert(right)
            bridgedRight = 0
        } else if bridgedRight < 2 && isBridgeableAdCopySegment(segment) {
            included.insert(right)
            bridgedRight += 1
        } else {
            break
        }
        right += 1
    }

    var segmentIndexes = included.sorted()
    var start = max(0, (segmentIndexes.first.map { segments[$0].start } ?? candidate.start) - options.paddingSeconds)
    var end = min(duration, (segmentIndexes.last.map { segments[$0].end } ?? candidate.end) + options.paddingSeconds)

    if end - start > options.maxWindowSeconds {
        let cueStart = segments[firstCueIndex].start
        let cueEnd = segments[lastCueIndex].end
        start = max(0, cueStart - min(options.expandBeforeSeconds, options.maxWindowSeconds / 2))
        end = min(duration, max(cueEnd, start + options.maxWindowSeconds))
        if end - start > options.maxWindowSeconds {
            end = min(duration, start + options.maxWindowSeconds)
        }
        if cueEnd > end {
            end = min(duration, cueEnd)
            start = max(0, end - options.maxWindowSeconds)
        }
        segmentIndexes = segmentIndexes.filter { segments[$0].end >= start && segments[$0].start <= end }
    }

    let expandedText = segmentIndexes.map { segments[$0].text }.joined(separator: " ")
    return Candidate(
        start: start,
        end: end,
        phrases: candidate.phrases,
        segmentIndexes: segmentIndexes,
        cueSegmentIndexes: candidate.cueSegmentIndexes.sorted(),
        advertiserGuess: guessAdvertiser(from: expandedText),
        productGuess: guessProduct(from: expandedText)
    )
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

func formatSeconds(_ value: Double) -> String {
    String(format: "%.1f", rounded(value))
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
        lines.append("Advertiser guess: \(candidate.advertiserGuess ?? "review_needed")")
        lines.append("Product guess: \(candidate.productGuess ?? "review_needed")")
        lines.append("")
        let lowerBound = max(0, (candidate.segmentIndexes.min() ?? 0) - 1)
        let upperBound = min(segments.count - 1, (candidate.segmentIndexes.max() ?? 0) + 1)
        for idx in lowerBound...upperBound {
            let marker: String
            if candidate.cueSegmentIndexes.contains(idx) {
                marker = "*"
            } else if candidate.segmentIndexes.contains(idx) {
                marker = "+"
            } else {
                marker = " "
            }
            let seg = segments[idx]
            lines.append("\(marker) [\(String(format: "%.1f", seg.start))-\(String(format: "%.1f", seg.end))] \(seg.text)")
        }
        lines.append("")
    }
    lines.append("Promote only after checking audio boundaries to +/-0.5s and filling advertiser/product when identifiable.")
    lines.append("")
    return lines.joined(separator: "\n")
}

func stringValue(_ value: Any?) -> String? {
    switch value {
    case let value as String:
        return value.isEmpty ? nil : value
    case is NSNull:
        return nil
    default:
        return nil
    }
}

func jsonObject(from url: URL) throws -> [String: Any] {
    let data = try Data(contentsOf: url)
    let root = try JSONSerialization.jsonObject(with: data)
    guard let object = root as? [String: Any] else {
        throw NSError(domain: "L2FDraft", code: 20, userInfo: [
            NSLocalizedDescriptionKey: "\(url.path) is not a JSON object",
        ])
    }
    return object
}

func reviewCandidatesFromCodexReview(_ url: URL) throws -> [ReviewQueueCandidate] {
    let object = try jsonObject(from: url)
    guard let episodes = object["episodes"] as? [[String: Any]] else {
        throw NSError(domain: "L2FDraft", code: 21, userInfo: [
            NSLocalizedDescriptionKey: "\(url.path) does not contain an episodes array",
        ])
    }

    return episodes.flatMap { episode -> [ReviewQueueCandidate] in
        let episodeId = stringValue(episode["episode_id"]) ?? "unknown-episode"
        let episodeNotes = stringValue(episode["notes"]) ?? "Review transcript-derived candidate against local audio."
        let windows = episode["codex_windows"] as? [[String: Any]] ?? []
        if windows.isEmpty {
            return [ReviewQueueCandidate(
                episodeId: episodeId,
                start: nil,
                end: nil,
                duration: nil,
                advertiserGuess: nil,
                productGuess: nil,
                adType: "false_positive_trap",
                transitionType: nil,
                notes: episodeNotes,
                source: url.lastPathComponent,
                isFalsePositiveTrap: true
            )]
        }
        return windows.map { window in
            ReviewQueueCandidate(
                episodeId: episodeId,
                start: number(window["start_seconds"]),
                end: number(window["end_seconds"]),
                duration: nil,
                advertiserGuess: stringValue(window["advertiser"]),
                productGuess: stringValue(window["product"]),
                adType: stringValue(window["ad_type"]),
                transitionType: stringValue(window["transition_type"]),
                notes: stringValue(window["confidence_notes"]) ?? episodeNotes,
                source: url.lastPathComponent,
                isFalsePositiveTrap: false
            )
        }
    }
}

func reviewCandidatesFromDraft(_ url: URL) throws -> [ReviewQueueCandidate] {
    let object = try jsonObject(from: url)
    let episodeId = stringValue(object["episode_id"]) ?? url.deletingPathExtension().deletingPathExtension().lastPathComponent
    let duration = number(object["duration_seconds"])
    let windows = object["ad_windows"] as? [[String: Any]] ?? []
    if windows.isEmpty {
        return [ReviewQueueCandidate(
            episodeId: episodeId,
            start: nil,
            end: nil,
            duration: duration,
            advertiserGuess: nil,
            productGuess: nil,
            adType: "false_positive_trap",
            transitionType: nil,
            notes: "No transcript heuristic ad candidates were found. Listen with normal sampling before marking this episode as a zero-ad false-positive trap.",
            source: url.lastPathComponent,
            isFalsePositiveTrap: true
        )]
    }
    return windows.map { window in
        ReviewQueueCandidate(
            episodeId: episodeId,
            start: number(window["start_seconds"]),
            end: number(window["end_seconds"]),
            duration: duration,
            advertiserGuess: stringValue(window["advertiser_guess"]) ?? stringValue(window["advertiser"]),
            productGuess: stringValue(window["product_guess"]) ?? stringValue(window["product"]),
            adType: stringValue(window["ad_type"]),
            transitionType: stringValue(window["transition_type"]),
            notes: stringValue(window["confidence_notes"]) ?? "Draft heuristic candidate; verify with local audio.",
            source: url.lastPathComponent,
            isFalsePositiveTrap: false
        )
    }
}

func collectDraftsForReviewQueue() -> [URL] {
    guard let files = try? FileManager.default.contentsOfDirectory(
        at: draftDir,
        includingPropertiesForKeys: nil,
        options: [.skipsHiddenFiles]
    ) else {
        return []
    }
    return files
        .filter { $0.lastPathComponent.hasSuffix(".draft.json") }
        .sorted { $0.lastPathComponent < $1.lastPathComponent }
}

func shellQuoted(_ value: String) -> String {
    "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
}

func safeArtifactBasename(_ value: String) -> String {
    let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "._-"))
    var result = ""
    for scalar in value.unicodeScalars {
        result.append(allowed.contains(scalar) ? Character(scalar) : "-")
    }
    let trimmed = result.trimmingCharacters(in: CharacterSet(charactersIn: ".-_\n\t "))
    return trimmed.isEmpty ? "episode" : trimmed
}

func reviewAudio(for episodeId: String) -> URL? {
    matchingAudio(for: episodeId)
}

func reviewCommand(for candidate: ReviewQueueCandidate, audio: URL?) -> (playback: String, extraction: String) {
    guard let audio else {
        let episode = shellQuoted(candidate.episodeId)
        return (
            "Review local episode \(episode) manually; no candidate time range or audio file was available.",
            "No extraction command: add matching audio under \(audioDir.path)/\(candidate.episodeId).<ext> first."
        )
    }
    guard let start = candidate.start,
          let end = candidate.end else {
        let sampleLength = min(candidate.duration ?? 120.0, max(60.0, options.reviewContextSeconds * 3.0))
        let audioPath = shellQuoted(audio.path)
        let episodeArtifact = safeArtifactBasename(candidate.episodeId)
        let clipPath = shellQuoted(draftDir.appendingPathComponent("\(episodeArtifact)-false-positive-trap-sample.review.m4a").path)
        return (
            "ffplay -autoexit -nodisp -ss 0.0 -t \(formatSeconds(sampleLength)) \(audioPath)",
            "ffmpeg -y -ss 0.0 -t \(formatSeconds(sampleLength)) -i \(audioPath) -c copy \(clipPath)"
        )
    }
    let contextStart = max(0, start - options.reviewContextSeconds)
    let contextEnd = min(candidate.duration ?? end + options.reviewContextSeconds, end + options.reviewContextSeconds)
    let length = max(0.1, contextEnd - contextStart)
    let audioPath = shellQuoted(audio.path)
    let episodeArtifact = safeArtifactBasename(candidate.episodeId)
    let clipPath = shellQuoted(draftDir.appendingPathComponent("\(episodeArtifact)-\(formatSeconds(start))-\(formatSeconds(end)).review.m4a").path)
    let playback = "ffplay -autoexit -nodisp -ss \(formatSeconds(contextStart)) -t \(formatSeconds(length)) \(audioPath)"
    let extraction = "ffmpeg -y -ss \(formatSeconds(contextStart)) -t \(formatSeconds(length)) -i \(audioPath) -c copy \(clipPath)"
    return (playback, extraction)
}

func queueEntryJSON(
    candidate: ReviewQueueCandidate,
    index: Int,
    audio: URL?,
    commands: (playback: String, extraction: String)
) -> [String: Any] {
    let contextStart = candidate.start.map { max(0, $0 - options.reviewContextSeconds) }
    let contextEnd = candidate.end.map {
        min(candidate.duration ?? $0 + options.reviewContextSeconds, $0 + options.reviewContextSeconds)
    }
    return [
        "id": "\(candidate.episodeId)#\(index + 1)",
        "episode_id": candidate.episodeId,
        "candidate_index": index + 1,
        "start_seconds": candidate.start.map(rounded) as Any? ?? NSNull(),
        "end_seconds": candidate.end.map(rounded) as Any? ?? NSNull(),
        "context_start_seconds": contextStart.map(rounded) as Any? ?? NSNull(),
        "context_end_seconds": contextEnd.map(rounded) as Any? ?? NSNull(),
        "context_padding_seconds": rounded(options.reviewContextSeconds),
        "advertiser_guess": candidate.advertiserGuess as Any? ?? NSNull(),
        "product_guess": candidate.productGuess as Any? ?? NSNull(),
        "ad_type": candidate.adType as Any? ?? NSNull(),
        "transition_type": candidate.transitionType as Any? ?? NSNull(),
        "false_positive_trap": candidate.isFalsePositiveTrap,
        "source": candidate.source,
        "audio_path": audio?.path as Any? ?? NSNull(),
        "playback_command": commands.playback,
        "extraction_command": commands.extraction,
        "checklist": [
            "listen_with_pre_post_context",
            "mark_false_positive_or_verified_ad",
            "adjust_start_end_to_plus_minus_0_5s",
            "fill_advertiser_and_product",
            "write_boundary_confidence_notes",
        ],
        "notes": candidate.notes,
    ]
}

func markdownSeconds(_ value: Any?) -> String {
    guard let seconds = value as? Double else {
        return "n/a"
    }
    return formatSeconds(seconds)
}

func makeReviewQueueMarkdown(entries: [[String: Any]]) -> String {
    var lines: [String] = []
    lines.append("# L2F Local Audio Review Queue")
    lines.append("")
    lines.append("Review-only queue. These entries are not ground truth; promote only after local audio verification for playhead-l2f.3/.4.")
    lines.append("")
    lines.append("- Context padding seconds: \(formatSeconds(options.reviewContextSeconds))")
    lines.append("- Entries: \(entries.count)")
    lines.append("")
    for entry in entries {
        let episodeId = entry["episode_id"] as? String ?? "unknown-episode"
        let index = entry["candidate_index"] as? Int ?? 0
        let isTrap = entry["false_positive_trap"] as? Bool ?? false
        let start = entry["start_seconds"] as? Double
        let end = entry["end_seconds"] as? Double
        let titleRange = isTrap ? "false-positive trap / zero-ad check" : "\(formatSeconds(start ?? 0))-\(formatSeconds(end ?? 0))"
        lines.append("## [ ] \(episodeId) #\(index): \(titleRange)")
        lines.append("")
        lines.append("- Source: \(entry["source"] as? String ?? "unknown")")
        lines.append("- Advertiser guess: \(entry["advertiser_guess"] as? String ?? "review_needed")")
        lines.append("- Product guess: \(entry["product_guess"] as? String ?? "review_needed")")
        lines.append("- Context: \(markdownSeconds(entry["context_start_seconds"]))-\(markdownSeconds(entry["context_end_seconds"]))")
        lines.append("- Playback: `\(entry["playback_command"] as? String ?? "")`")
        lines.append("- Extract: `\(entry["extraction_command"] as? String ?? "")`")
        lines.append("- Checklist: [ ] listened with context [ ] false-positive/ad decision [ ] +/-0.5s boundaries [ ] advertiser/product [ ] notes")
        lines.append("- Notes: \(entry["notes"] as? String ?? "")")
        lines.append("")
    }
    return lines.joined(separator: "\n")
}

func writeReviewQueue(from candidates: [ReviewQueueCandidate]) throws {
    var perEpisodeIndex: [String: Int] = [:]
    let entries: [[String: Any]] = candidates.map { candidate in
        let index = perEpisodeIndex[candidate.episodeId, default: 0]
        perEpisodeIndex[candidate.episodeId] = index + 1
        let audio = reviewAudio(for: candidate.episodeId)
        let commands = reviewCommand(for: candidate, audio: audio)
        return queueEntryJSON(candidate: candidate, index: index, audio: audio, commands: commands)
    }
    let json: [String: Any] = [
        "schema": "playhead-l2f-review-queue-v1",
        "created_by": "scripts/l2f-draft-annotation.swift",
        "review_basis": options.reviewSource == nil ? "drafts" : "codex_transcript_review",
        "human_audio_verification_required": true,
        "context_padding_seconds": rounded(options.reviewContextSeconds),
        "entries": entries,
    ]
    let jsonURL = draftDir.appendingPathComponent("\(options.reviewQueueName).json")
    let markdownURL = draftDir.appendingPathComponent("\(options.reviewQueueName).md")
    let data = try JSONSerialization.data(
        withJSONObject: json,
        options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes]
    )
    try data.write(to: jsonURL)
    try Data(makeReviewQueueMarkdown(entries: entries).utf8).write(to: markdownURL)
    print("wrote: \(jsonURL.path)")
    print("wrote: \(markdownURL.path)")
}

func loadReviewQueueCandidates() throws -> [ReviewQueueCandidate] {
    if let reviewSource = options.reviewSource {
        return try reviewCandidatesFromCodexReview(resolve(reviewSource))
    }
    let drafts = collectDraftsForReviewQueue()
    guard !drafts.isEmpty else {
        fatal("no *.draft.json files found in \(draftDir.path); generate drafts first or pass --review-source")
    }
    return try drafts.flatMap(reviewCandidatesFromDraft)
}

var generatedReviewQueueCandidates: [ReviewQueueCandidate] = []
var failures = 0

if !options.reviewQueueOnly {
    let transcripts = collectTranscripts()
    guard !transcripts.isEmpty else {
        print("l2f-draft-annotation: no transcript JSON files found in \(transcriptDir.path)")
        exit(0)
    }

    let placeholderFingerprint = "sha256:0000000000000000000000000000000000000000000000000000000000000000"

    for transcript in transcripts {
        let episodeId = options.episodeId ?? transcript.deletingPathExtension().lastPathComponent
        let draftURL = draftDir.appendingPathComponent("\(episodeId).draft.json")
        let reviewURL = draftDir.appendingPathComponent("\(episodeId).review.md")

        if FileManager.default.fileExists(atPath: draftURL.path), !options.force {
            print("skip: \(draftURL.path) already exists; pass --force to rebuild")
            if options.writeReviewQueue && options.reviewSource == nil {
                do {
                    generatedReviewQueueCandidates.append(contentsOf: try reviewCandidatesFromDraft(draftURL))
                } catch {
                    FileHandle.standardError.write(Data("failed \(draftURL.path): \(error.localizedDescription)\n".utf8))
                    failures += 1
                }
            }
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
                    "advertiser_guess": candidate.advertiserGuess as Any? ?? NSNull(),
                    "product_guess": candidate.productGuess as Any? ?? NSNull(),
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
            generatedReviewQueueCandidates.append(contentsOf: try reviewCandidatesFromDraft(draftURL))
            print("wrote: \(draftURL.path)")
            print("wrote: \(reviewURL.path)")
        } catch {
            FileHandle.standardError.write(Data("failed \(transcript.path): \(error.localizedDescription)\n".utf8))
            failures += 1
        }
    }
}

if failures == 0 && options.writeReviewQueue {
    do {
        let queueCandidates = options.reviewSource == nil && !generatedReviewQueueCandidates.isEmpty
            ? generatedReviewQueueCandidates
            : try loadReviewQueueCandidates()
        try writeReviewQueue(from: queueCandidates)
    } catch {
        FileHandle.standardError.write(Data("failed review queue: \(error.localizedDescription)\n".utf8))
        failures += 1
    }
}

if failures > 0 {
    FileHandle.standardError.write(Data("l2f-draft-annotation: \(failures) failure(s)\n".utf8))
    exit(1)
}
