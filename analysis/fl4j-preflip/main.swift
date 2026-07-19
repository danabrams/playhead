// main.swift — playhead-fl4j pre-flip validation harness (measurement only).
//
// Drives the REAL shipped Swift logic: this file is compiled TOGETHER with the
// production sources, verbatim and unmodified:
//   Playhead/Services/AdDetection/LexicalAnchorBank.swift   (LexicalAnchorNormalizer)
//   Playhead/Services/AdDetection/LexicalAnchorRefiner.swift (LexicalWord + buildWordStream)
//   Playhead/Services/AdDetection/SelfPromoBank.swift        (bank decode/validate)
//   Playhead/Services/AdDetection/SelfPromoVerifier.swift    (SelfReferenceVerifier et al.)
//   Playhead/Services/AdDetection/PromoSuppressor.swift      (shouldSuppress)
//
// Only two INERT data carriers are shimmed below (their production definitions
// drag persistence/crypto dependency chains the logic under test never reads):
//   * TranscriptChunk — buildWordStream reads text/normalizedText/startTime/endTime only
//   * DecodedSpan     — shouldSuppress reads startTime/endTime only
// The shims carry identical field names/types for those read fields, so the
// compiled behaviour of the code under test is byte-for-byte the shipped logic.
//
// Modes:
//   selftest <bankPath>          — replays parity cases from the shipped
//                                  77-test suite (PromoSuppressorTests) and the
//                                  wire-in semantics; exits non-zero on any FAIL.
//   measure  <jobPath> <outPath> — runs the three-lane corpus job. For each
//                                  span: the REAL end-to-end verdict
//                                  (PromoSuppressor.shouldSuppress), plus a
//                                  per-candidate diagnostic walk that uses the
//                                  REAL SelfReferenceVerifier per candidate.
//                                  The two are cross-checked; any divergence is
//                                  recorded as a harness bug (none expected —
//                                  the diagnostic walk mirrors the attention
//                                  loop exactly).

import Foundation

// MARK: - Shims (inert data carriers; see header)

struct TranscriptChunk: Sendable {
    let id: String
    let analysisAssetId: String
    let segmentFingerprint: String
    let chunkIndex: Int
    let startTime: Double
    let endTime: Double
    let text: String
    let normalizedText: String
    let pass: String
    let modelVersion: String
    let transcriptVersion: String?
    let atomOrdinal: Int?
}

struct DecodedSpan: Sendable, Equatable {
    let startTime: Double
    let endTime: Double
}

// MARK: - Job / output schema

struct JobSpan: Codable {
    let cls: String
    let start: Double
    let end: Double
    let tag: String
}

struct JobEpisode: Codable {
    let episodeId: String
    let title: String
    let networkId: String?
    let transcriptPath: String
    let spans: [JobSpan]
}

struct Job: Codable {
    let bankPath: String
    let episodes: [JobEpisode]
}

struct CandidateOut: Codable {
    let phrase: String
    let selfReference: String
    let matchStart: Int
    let corroborated: Bool
    /// First-person markers found in the ±window (outside the match) — the
    /// corroboration channel for AMBIGUOUS phrases.
    let windowFirstPerson: [String]
    /// Show-identity tokens found in the ±window (outside the match).
    let windowIdentity: [String]
    /// ±window context snippet (normalised tokens) for audit.
    let window: String
}

struct SpanOut: Codable {
    let episodeId: String
    let cls: String
    let start: Double
    let end: Double
    let tag: String
    let tokenCount: Int
    /// REAL end-to-end verdict from PromoSuppressor.shouldSuppress.
    let suppressed: Bool
    /// Old (pre-rework, 092d8a39) design: bare lexical match = demotion.
    let oldFire: Bool
    /// Diagnostic-vs-real divergence (harness bug canary; must stay false).
    let mismatch: Bool
    let candidates: [CandidateOut]
    /// Joined normalised tokens (full text for ad/veto lanes; only when a
    /// candidate matched for the neutral lane, to bound output size).
    let text: String?
}

struct MeasureOut: Codable {
    let bankPhrases: [String: String]
    let identityTokensByEpisode: [String: [String]]
    let spans: [SpanOut]
    let mismatches: Int
}

// MARK: - Transcript loading (whisper corpus JSON)

struct WhisperFile: Decodable {
    struct Segment: Decodable {
        struct Offsets: Decodable { let from: Double; let to: Double }
        let offsets: Offsets
        let text: String
    }
    let transcription: [Segment]
}

func loadWords(transcriptPath: String) throws -> [LexicalWord] {
    let data = try Data(contentsOf: URL(fileURLWithPath: transcriptPath))
    let file = try JSONDecoder().decode(WhisperFile.self, from: data)
    let chunks = file.transcription.enumerated().map { (i, s) in
        TranscriptChunk(
            id: "c\(i)", analysisAssetId: "asset", segmentFingerprint: "fp",
            chunkIndex: i,
            startTime: s.offsets.from / 1000.0,
            endTime: s.offsets.to / 1000.0,
            text: s.text, normalizedText: s.text.lowercased(),
            pass: "final", modelVersion: "corpus", transcriptVersion: nil,
            atomOrdinal: nil
        )
    }
    // REAL production word-stream construction.
    return LexicalAnchorRefiner.buildWordStream(chunks: chunks)
}

// MARK: - Diagnostic walk (mirrors PromoSuppressor's attention loop; verifier
// calls are the REAL SelfReferenceVerifier)

func diagnose(
    spanTokens: [String],
    bank: SelfPromoBank,
    identity: SelfPromoShowIdentity
) -> [CandidateOut] {
    let context = SelfPromoContext(spanTokens: spanTokens, showIdentity: identity)
    let verifier = SelfReferenceVerifier()
    var out: [CandidateOut] = []
    for phrase in bank.phrases {
        let n = phrase.tokens.count
        guard n >= 1, spanTokens.count >= n else { continue }
        let lastStart = spanTokens.count - n
        var i = 0
        while i <= lastStart {
            var k = 0
            var ok = true
            while k < n {
                if spanTokens[i + k] != phrase.tokens[k] { ok = false; break }
                k += 1
            }
            if ok {
                let candidate = SelfPromoCandidate(phrase: phrase, matchRange: i..<(i + n))
                let corroborated = verifier.corroborates(candidate, in: context)
                let lower = max(0, i - SelfReferenceVerifier.windowRadius)
                let upper = min(spanTokens.count, i + n + SelfReferenceVerifier.windowRadius)
                var fp: [String] = []
                var idt: [String] = []
                for j in lower..<upper where !(i..<(i + n)).contains(j) {
                    let t = spanTokens[j]
                    if SelfReferenceVerifier.firstPersonMarkers.contains(t) { fp.append(t) }
                    if identity.identityTokens.contains(t) { idt.append(t) }
                }
                out.append(CandidateOut(
                    phrase: phrase.phrase,
                    selfReference: phrase.selfReference.rawValue,
                    matchStart: i,
                    corroborated: corroborated,
                    windowFirstPerson: fp,
                    windowIdentity: idt,
                    window: spanTokens[lower..<upper].joined(separator: " ")
                ))
            }
            i += 1
        }
    }
    return out
}

// MARK: - measure

func runMeasure(jobPath: String, outPath: String) throws {
    let job = try JSONDecoder().decode(Job.self, from: Data(contentsOf: URL(fileURLWithPath: jobPath)))
    let bank = try SelfPromoBank.decode(Data(contentsOf: URL(fileURLWithPath: job.bankPath)))

    var bankPhrases: [String: String] = [:]
    for p in bank.phrases { bankPhrases[p.phrase] = p.selfReference.rawValue }

    var spansOut: [SpanOut] = []
    var identityByEpisode: [String: [String]] = [:]
    var mismatches = 0

    for episode in job.episodes {
        let words = try loadWords(transcriptPath: episode.transcriptPath)
        let identity = SelfPromoShowIdentity(
            title: episode.title, networkId: episode.networkId
        )
        identityByEpisode[episode.episodeId] = identity.identityTokens.sorted()

        for s in episode.spans {
            let span = DecodedSpan(startTime: s.start, endTime: s.end)
            // REAL end-to-end path (default verifiers = shipped list).
            let suppressed = PromoSuppressor.shouldSuppress(
                span: span,
                transcriptWords: words,
                bank: bank,
                showIdentity: identity
            )
            // Same slice predicate the suppressor uses, for diagnostics.
            let spanTokens = words
                .filter { $0.startSeconds < s.end && $0.endSeconds > s.start }
                .map(\.norm)
            let candidates = diagnose(spanTokens: spanTokens, bank: bank, identity: identity)
            let diagSuppressed = candidates.contains { $0.corroborated }
            let mismatch = (suppressed != diagSuppressed)
            if mismatch { mismatches += 1 }
            let isAdOrVeto = s.cls != "neutral"
            let keepText = isAdOrVeto || !candidates.isEmpty
            spansOut.append(SpanOut(
                episodeId: episode.episodeId,
                cls: s.cls,
                start: s.start,
                end: s.end,
                tag: s.tag,
                tokenCount: spanTokens.count,
                suppressed: suppressed,
                oldFire: !candidates.isEmpty,
                mismatch: mismatch,
                candidates: candidates,
                text: keepText ? spanTokens.joined(separator: " ") : nil
            ))
        }
        FileHandle.standardError.write("done \(episode.episodeId) (\(episode.spans.count) spans)\n".data(using: .utf8)!)
    }

    let out = MeasureOut(
        bankPhrases: bankPhrases,
        identityTokensByEpisode: identityByEpisode,
        spans: spansOut,
        mismatches: mismatches
    )
    let enc = JSONEncoder()
    enc.outputFormatting = [.sortedKeys]
    try enc.encode(out).write(to: URL(fileURLWithPath: outPath))
    print("spans=\(spansOut.count) mismatches=\(mismatches)")
    if mismatches != 0 {
        print("ERROR: diagnostic walk diverged from PromoSuppressor.shouldSuppress")
        exit(2)
    }
}

// MARK: - selftest (parity cases from the shipped PromoSuppressorTests suite)

func runSelftest(bankPath: String) throws {
    let bank = try SelfPromoBank.decode(Data(contentsOf: URL(fileURLWithPath: bankPath)))

    func words(_ text: String, start: Double = 0, end: Double = 30) -> [LexicalWord] {
        LexicalAnchorRefiner.buildWordStream(chunks: [TranscriptChunk(
            id: "c0", analysisAssetId: "asset", segmentFingerprint: "fp",
            chunkIndex: 0, startTime: start, endTime: end,
            text: text, normalizedText: text.lowercased(),
            pass: "final", modelVersion: "test", transcriptVersion: nil, atomOrdinal: nil
        )])
    }
    func makeBank(_ phrases: [(String, String)]) throws -> SelfPromoBank {
        let obj: [String: Any] = [
            "schemaVersion": 2,
            "phrases": phrases.map { ["phrase": $0.0, "selfReference": $0.1] },
        ]
        return try SelfPromoBank.decode(JSONSerialization.data(withJSONObject: obj))
    }
    let span = DecodedSpan(startTime: 0, endTime: 30)

    var failures = 0
    func check(_ name: String, _ got: Bool, _ want: Bool) {
        let ok = got == want
        print("\(ok ? "PASS" : "FAIL") \(name) (got \(got), want \(want))")
        if !ok { failures += 1 }
    }

    // — PromoSuppressorTests.actionPhraseFires
    check("actionPhraseFires",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Thanks so much for listening. Please rate review and subscribe wherever you get your podcasts."), bank: bank),
          true)
    // — PromoSuppressorTests.liveShowPlugFires (first-person corroborates)
    check("liveShowPlugFires",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("We are going on tour this fall, get tickets at the box office."), bank: bank),
          true)
    // — PromoSuppressorTests.beAGuestFires
    check("beAGuestFires",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Want to be a guest on the show? Follow us for details."), bank: bank),
          true)
    // — PromoSuppressorTests.bareSponsorDoesNotFire
    check("bareSponsorDoesNotFire",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("This episode is brought to you by Squarespace. Use code SHOW for 10 percent off at squarespace dot com slash show."), bank: bank),
          false)
    // — PromoSuppressorTests.bareShowNameDoesNotFire
    check("bareShowNameDoesNotFire",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("WNYC Studios is supported by Proof on Broadway. Welcome back to On The Media with Brooke Gladstone."), bank: bank),
          false)
    // — PromoSuppressorTests.ambiguousPhraseRequiresSelfReference
    let b1 = try makeBank([("get tickets", "requiresCorroboration"), ("on tour", "requiresCorroboration")])
    check("ambiguous.thirdPartyEventAdDoesNotFire",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Get tickets to see Taylor Swift on tour at Ticketmaster dot com."), bank: b1),
          false)
    check("ambiguous.firstPersonCorroborates",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Get tickets to our live show this weekend."), bank: b1),
          true)
    // — PromoSuppressorTests.strongPhraseSelfCorroborates
    let b2 = try makeBank([("rate review and subscribe", "selfEvident")])
    check("strongPhraseSelfCorroborates",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Rate review and subscribe for more episodes."), bank: b2),
          true)
    // — PromoSuppressorTests.ambiguousCorroboratedByShowIdentity
    let b3 = try makeBank([("on tour", "requiresCorroboration")])
    let conan = SelfPromoShowIdentity(title: "Conan O'Brien Needs a Friend")
    check("ambiguous.showIdentityCorroborates",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Conan is going on tour this fall."), bank: b3, showIdentity: conan),
          true)
    check("ambiguous.noIdentityNoFirstPersonDoesNotFire",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Conan is going on tour this fall."), bank: b3),
          false)
    // — PromoSuppressorTests.selfReferenceOutsideWindowDoesNotCorroborate
    let filler = Array(repeating: "and", count: 20).joined(separator: " ")
    check("selfRefOutsideWindowDoesNotCorroborate",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("we \(filler) going on tour"), bank: b3),
          false)
    // — PromoSuppressorTests.normalisationParity
    let b4 = try makeBank([("we're on tour", "selfEvident")])
    check("normalisation.curlyApostrophe",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("We\u{2019}re ON TOUR!"), bank: b4),
          true)
    check("normalisation.straightApostrophe",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("we're on tour"), bank: b4),
          true)
    // — PromoSuppressorTests no-op cases
    check("emptyWordsNoOp",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: [], bank: bank),
          false)
    check("noMatchNoOp",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Today we discuss the history of aviation and the future of flight."), bank: bank),
          false)
    check("outOfSpanGeometryExcluded",
          PromoSuppressor.shouldSuppress(span: DecodedSpan(startTime: 40, endTime: 50), transcriptWords: words("Please rate review and subscribe.", start: 0, end: 30), bank: bank),
          false)
    check("inSpanGeometryFires",
          PromoSuppressor.shouldSuppress(span: span, transcriptWords: words("Please rate review and subscribe.", start: 0, end: 30), bank: bank),
          true)
    check("zeroDurationNoOp",
          PromoSuppressor.shouldSuppress(span: DecodedSpan(startTime: 10, endTime: 10), transcriptWords: words("Please rate review and subscribe."), bank: bank),
          false)
    // Shipped-bank sanity: schema v2, 19 phrases, 5 ambiguous.
    let ambiguous = bank.phrases.filter { $0.selfReference == .requiresCorroboration }.map(\.phrase)
    check("shippedBank.phraseCount19", bank.phrases.count == 19, true)
    check("shippedBank.ambiguousCount5", ambiguous.count == 5, true)
    print("ambiguous phrases: \(ambiguous.sorted().joined(separator: ", "))")

    if failures > 0 {
        print("SELFTEST FAILED: \(failures) case(s)")
        exit(1)
    }
    print("SELFTEST PASSED (\(18) parity cases + 2 bank pins)")
}

// MARK: - entry

let args = CommandLine.arguments
guard args.count >= 2 else {
    print("usage: fl4j-harness selftest <bankPath> | measure <jobPath> <outPath>")
    exit(64)
}
do {
    switch args[1] {
    case "selftest":
        try runSelftest(bankPath: args[2])
    case "measure":
        try runMeasure(jobPath: args[2], outPath: args[3])
    default:
        print("unknown mode \(args[1])")
        exit(64)
    }
} catch {
    print("ERROR: \(error)")
    exit(1)
}
