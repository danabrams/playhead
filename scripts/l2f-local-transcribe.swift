#!/usr/bin/env swift

// l2f-local-transcribe.swift
// Local ASR wrapper for the playhead-l2f corpus bootstrap.
//
// Takes audio files from TestFixtures/Corpus/Audio, runs whisper.cpp, and
// writes timestamped transcript JSON to TestFixtures/Corpus/Transcripts.
// Transcript files are intentionally ignored by git because they are derived
// from copyrighted audio and can be regenerated locally.

import Foundation

struct Options {
    var audioDir = "TestFixtures/Corpus/Audio"
    var transcriptDir = "TestFixtures/Corpus/Transcripts"
    var modelPath: String?
    var whisperBin = "whisper-cli"
    var language = "en"
    var threads: Int?
    var noGPU = false
    var force = false
    var dryRun = false
    var inputs: [String] = []
}

func printUsage() {
    let msg = """
    Usage: swift scripts/l2f-local-transcribe.swift [options] [audio-file ...]

      --model PATH          Required whisper.cpp GGML model, e.g. models/ggml-large-v3-turbo.bin.
      --audio-dir PATH      Audio directory when no explicit files are passed.
                            Default: TestFixtures/Corpus/Audio
      --transcript-dir PATH Output directory for <episode_id>.json transcripts.
                            Default: TestFixtures/Corpus/Transcripts
      --whisper-bin PATH    whisper.cpp binary. Default: whisper-cli
      --language LANG       Spoken language. Default: en
      --threads N           whisper.cpp thread count.
      --no-gpu              Pass -ng to whisper.cpp.
      --force               Rebuild transcripts that already exist.
      --dry-run             Print commands without running them. Does not require --model.

    Audio names should be <episode_id>.<ext>. The generated transcript is
    <transcript-dir>/<episode_id>.json.
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
        case "--audio-dir":
            i += 1
            guard i < argv.count else { fatal("--audio-dir requires a path") }
            opts.audioDir = argv[i]
        case "--transcript-dir":
            i += 1
            guard i < argv.count else { fatal("--transcript-dir requires a path") }
            opts.transcriptDir = argv[i]
        case "--model":
            i += 1
            guard i < argv.count else { fatal("--model requires a path") }
            opts.modelPath = argv[i]
        case "--whisper-bin":
            i += 1
            guard i < argv.count else { fatal("--whisper-bin requires a path") }
            opts.whisperBin = argv[i]
        case "--language":
            i += 1
            guard i < argv.count else { fatal("--language requires a value") }
            opts.language = argv[i]
        case "--threads":
            i += 1
            guard i < argv.count, let n = Int(argv[i]), n > 0 else {
                fatal("--threads requires a positive integer")
            }
            opts.threads = n
        case "--no-gpu":
            opts.noGPU = true
        case "--force":
            opts.force = true
        case "--dry-run":
            opts.dryRun = true
        case "-h", "--help":
            printUsage()
            exit(0)
        default:
            opts.inputs.append(arg)
        }
        i += 1
    }
    if opts.modelPath == nil && !opts.dryRun {
        fatal("--model is required unless --dry-run is set")
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
let audioDir = resolve(options.audioDir)
let transcriptDir = resolve(options.transcriptDir)
try FileManager.default.createDirectory(at: transcriptDir, withIntermediateDirectories: true)

let corpusAudioExtensions: Set<String> = ["aac", "flac", "m4a", "mp3", "mp4", "ogg", "wav"]
let whisperNativeExtensions: Set<String> = ["flac", "mp3", "ogg", "wav"]

func collectAudioFiles() -> [URL] {
    if !options.inputs.isEmpty {
        return options.inputs.map(resolve)
    }
    guard let files = try? FileManager.default.contentsOfDirectory(
        at: audioDir,
        includingPropertiesForKeys: nil,
        options: [.skipsHiddenFiles]
    ) else {
        fatal("could not read audio directory at \(audioDir.path)")
    }
    return files
        .filter { corpusAudioExtensions.contains($0.pathExtension.lowercased()) }
        .sorted { $0.lastPathComponent < $1.lastPathComponent }
}

func shellQuoted(_ args: [String]) -> String {
    args.map { arg in
        "'" + arg.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }.joined(separator: " ")
}

@discardableResult
func run(_ executable: String, _ args: [String], dryRun: Bool = false) throws -> Int32 {
    let printable = shellQuoted([executable] + args)
    if dryRun {
        print(printable)
        return 0
    }

    let process = Process()
    if executable.hasPrefix("/") {
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = args
    } else {
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [executable] + args
    }
    if let devNull = FileHandle(forWritingAtPath: "/dev/null") {
        process.standardOutput = devNull
        process.standardError = devNull
    }
    try process.run()
    process.waitUntilExit()
    return process.terminationStatus
}

let audioFiles = collectAudioFiles()
guard !audioFiles.isEmpty else {
    print("l2f-local-transcribe: no audio files found in \(audioDir.path)")
    exit(0)
}

let tempRoot = URL(fileURLWithPath: NSTemporaryDirectory())
    .appendingPathComponent("playhead-l2f-transcribe-\(getpid())", isDirectory: true)
if !options.dryRun {
    try FileManager.default.createDirectory(at: tempRoot, withIntermediateDirectories: true)
}
defer { try? FileManager.default.removeItem(at: tempRoot) }

var failures = 0

for audio in audioFiles {
    let stem = audio.deletingPathExtension().lastPathComponent
    let outputBase = transcriptDir.appendingPathComponent(stem)
    let outputJSON = outputBase.appendingPathExtension("json")

    if FileManager.default.fileExists(atPath: outputJSON.path), !options.force {
        print("skip: \(outputJSON.path) already exists; pass --force to rebuild")
        continue
    }

    guard FileManager.default.fileExists(atPath: audio.path) else {
        FileHandle.standardError.write(Data("missing audio: \(audio.path)\n".utf8))
        failures += 1
        continue
    }

    var whisperInput = audio
    let ext = audio.pathExtension.lowercased()
    if !whisperNativeExtensions.contains(ext) {
        let wav = tempRoot.appendingPathComponent(stem).appendingPathExtension("wav")
        let ffmpegArgs = [
            "-y", "-v", "error",
            "-i", audio.path,
            "-ar", "16000",
            "-ac", "1",
            "-c:a", "pcm_s16le",
            wav.path,
        ]
        do {
            let status = try run("ffmpeg", ffmpegArgs, dryRun: options.dryRun)
            if status != 0 {
                FileHandle.standardError.write(Data("ffmpeg failed for \(audio.path)\n".utf8))
                failures += 1
                continue
            }
            whisperInput = wav
        } catch {
            FileHandle.standardError.write(Data("ffmpeg launch failed: \(error)\n".utf8))
            failures += 1
            continue
        }
    }

    var whisperArgs = [
        "-m", resolve(options.modelPath ?? "missing-model").path,
        "-l", options.language,
        "-oj",
        "-ojf",
        "-np",
        "-of", outputBase.path,
    ]
    if let threads = options.threads {
        whisperArgs.append(contentsOf: ["-t", String(threads)])
    }
    if options.noGPU {
        whisperArgs.append("-ng")
    }
    whisperArgs.append(whisperInput.path)

    do {
        let status = try run(options.whisperBin, whisperArgs, dryRun: options.dryRun)
        if status != 0 {
            FileHandle.standardError.write(Data("whisper-cpp failed for \(audio.path)\n".utf8))
            failures += 1
            continue
        }
        if options.dryRun {
            print("would write: \(outputJSON.path)")
            continue
        }
        if !options.dryRun && !FileManager.default.fileExists(atPath: outputJSON.path) {
            FileHandle.standardError.write(Data("expected transcript missing: \(outputJSON.path)\n".utf8))
            failures += 1
            continue
        }
        print("wrote: \(outputJSON.path)")
    } catch {
        FileHandle.standardError.write(Data("whisper launch failed: \(error)\n".utf8))
        failures += 1
    }
}

if failures > 0 {
    FileHandle.standardError.write(Data("l2f-local-transcribe: \(failures) failure(s)\n".utf8))
    exit(1)
}
