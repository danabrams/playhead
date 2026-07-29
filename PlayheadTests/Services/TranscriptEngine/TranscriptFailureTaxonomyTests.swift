// TranscriptFailureTaxonomyTests.swift
// playhead-8ysk part 2 — name the failure.
//
// THE DEFECT, in three independent drops:
//
//   1. THE ERROR WAS DESTROYED. `runTranscriptionLoop`'s catch received a
//      fully-formed error carrying the Speech domain, code and reason, wrote
//      it to `logger.error`, and `continue`d. Not accumulated, not counted,
//      not returned, not stored. A second, identical drop sat in the drain
//      loop.
//   2. THERE WAS NO CHANNEL. `TranscriptEngineEvent` had exactly two cases —
//      `chunksPersisted` and `completed`. Even a catch that wanted to forward
//      the error could not express it. Worse: after EVERY shard had failed,
//      the loop fell through and emitted `.completed`. A total failure was
//      reported to the runner as success.
//   3. WHAT SURVIVED WAS NOT EXPORTED. `runner_reason` reaches SQLite, but
//      `DiagnosticsBundle` drops `metadata` wholesale on PII grounds.
//
// So `asr_failed` was never an ASR error report. Its only emitter fires on
// `transcriptCoverage == 0` with NO `Error` value in scope anywhere in the
// block — it labels an ABSENCE. Nine distinct causes were indistinguishable
// behind it across six days of the owner's device data.
//
// What is under test here: the closed vocabulary, its classification of the
// REAL errors the engine throws, and the redaction rule for the numeric code.

@preconcurrency import AVFoundation
import Foundation
import Testing

@testable import Playhead

@Suite("playhead-8ysk — the transcription failure taxonomy")
struct TranscriptFailureTaxonomyTests {

    // MARK: - The vocabulary is closed and stable

    /// Raw values are a wire format: they are written into `work_journal`
    /// metadata on one build and read out of a diagnostics bundle by a
    /// support engineer on another. Renaming one silently re-buckets history.
    @Test("TranscriptFailureClass covers the 16 documented variants with stable raw values")
    func vocabularyIsClosedAndStable() {
        let expected: Set<String> = [
            "silent_shard",
            "empty_shard",
            "non_finite_samples",
            "model_not_loaded",
            "speech_assets_unsupported",
            "analyzer_format_unavailable",
            "audio_bridge_failure",
            "invalid_analyzer_timeline",
            "analyzer_session_failure",
            "transcription_failed",
            "vad_failed",
            "unsupported_sample_rate",
            "no_shards",
            "speech_engine_not_ready",
            "persistence_failed",
            "unknown",
        ]
        let actual = Set(TranscriptFailureClass.allCases.map(\.rawValue))
        #expect(actual == expected)
        #expect(TranscriptFailureClass.allCases.count == 16)
    }

    /// The whole point of a closed vocabulary is that it cannot carry text.
    /// Every raw value must be a fixed identifier — no spaces, no
    /// punctuation, nothing that could have come from a feed, a title, a
    /// file path or an error message.
    @Test("every raw value is a bare snake_case identifier — nothing free-form can hide in one")
    func rawValuesAreIdentifierShaped() {
        let shape = #/^[a-z][a-z0-9_]*$/#
        for failureClass in TranscriptFailureClass.allCases {
            #expect(
                (try? shape.wholeMatch(in: failureClass.rawValue)) != nil,
                "'\(failureClass.rawValue)' is not identifier-shaped"
            )
        }
    }

    // MARK: - Classification of the REAL errors

    @Test("engine errors classify to their own variants, not to a catch-all")
    func engineErrorsClassify() {
        #expect(TranscriptFailureClass.classify(TranscriptEngineError.modelNotLoaded)
                == .modelNotLoaded)
        #expect(TranscriptFailureClass.classify(TranscriptEngineError.vadFailed("boom"))
                == .vadFailed)
        #expect(TranscriptFailureClass.classify(
            TranscriptEngineError.unsupportedSampleRate(expected: 16_000, actual: 44_100)
        ) == .unsupportedSampleRate)
        #expect(TranscriptFailureClass.classify(TranscriptEngineError.transcriptionFailed("x"))
                == .transcriptionFailed)
    }

#if canImport(Speech)
    @Test("Apple Speech boundary errors classify to their own variants")
    func boundaryErrorsClassify() {
        #expect(TranscriptFailureClass.classify(
            AppleSpeechBoundaryError.speechAssetsUnsupported(localeIdentifier: "en_US")
        ) == .speechAssetsUnsupported)
        #expect(TranscriptFailureClass.classify(
            AppleSpeechBoundaryError.analyzerFormatUnavailable(localeIdentifier: "en_US")
        ) == .analyzerFormatUnavailable)
        #expect(TranscriptFailureClass.classify(
            AppleSpeechBoundaryError.invalidAnalyzerInputTimeline("t")
        ) == .invalidAnalyzerTimeline)
        #expect(TranscriptFailureClass.classify(
            AppleSpeechBoundaryError.analyzerSessionFailure("s")
        ) == .analyzerSessionFailure)
        // An audioBridgeFailure that is NOT one of the three sample guards
        // keeps the general class rather than being mis-attributed.
        #expect(TranscriptFailureClass.classify(
            AppleSpeechBoundaryError.audioBridgeFailure("converter refused the format")
        ) == .audioBridgeFailure)
    }

    /// THE LOAD-BEARING ONE. The three sample guards all throw
    /// `.audioBridgeFailure(String)`, so the classifier recovers the sub-case
    /// by matching the message — a coupling to text in another file that
    /// would otherwise rot silently, degrading every future bundle to
    /// `audio_bridge_failure` and re-hiding the single most common failure.
    ///
    /// So the errors here are not hand-written: they are thrown by the REAL
    /// guards inside `AppleSpeechAudioBridge`, reached through the same
    /// entry point production uses (`makeAnalyzerBuffer`) and driven by the
    /// exact sample arrays that trigger them. A reworded message fails this
    /// test.
    ///
    /// The silent-shard case is the one that matters most: it is a pure CPU
    /// scan that rejects a shard in well under a millisecond, which is how a
    /// whole episode can fail instantly and how the tail of a partial decode
    /// (playhead-8ysk part 1) destroys a job.
    @Test(
        "the three real sample guards classify to three distinct variants",
        arguments: [
            ([Float](), TranscriptFailureClass.emptyShard),
            ([Float](repeating: 0, count: 800), TranscriptFailureClass.silentShard),
            ([Float.nan, 0.2, 0.3], TranscriptFailureClass.nonFiniteSamples),
        ]
    )
    func realSampleGuardsClassify(samples: [Float], expected: TranscriptFailureClass) throws {
        let shard = AnalysisShard(
            id: 7,
            episodeID: "ep-8ysk",
            startTime: 0,
            duration: Double(samples.count) / 16_000,
            samples: samples
        )
        let targetFormat = try #require(AVAudioFormat(
            commonFormat: .pcmFormatFloat32,
            sampleRate: 16_000,
            channels: 1,
            interleaved: false
        ))
        var thrown: Error?
        do {
            _ = try AppleSpeechAudioBridge.makeAnalyzerBuffer(
                from: shard, targetFormat: targetFormat
            )
        } catch {
            thrown = error
        }
        let error = try #require(
            thrown,
            "the guard must actually fire — a fixture that decodes cleanly proves nothing"
        )
        #expect(TranscriptFailureClass.classify(error) == expected,
                "got \(TranscriptFailureClass.classify(error).rawValue) for \(error)")
    }
#endif

    /// Round-1 review: `.persistenceFailed` was declared and never produced.
    /// Sixteen cases were pinned by `vocabularyIsClosedAndStable` and only
    /// fifteen were reachable, so a store failure — the one that made
    /// `appendShardsAfterCompletion` an accidental demo of this bead's defect
    /// — classified as `.unknown`, which is the same unnamed-absence problem
    /// `asr_failed` had, just moved one layer down.
    ///
    /// Only the error TYPE decides the class. `AnalysisStoreError`'s payloads
    /// are raw SQLite messages and are never read, which is what keeps the
    /// vocabulary closed on this branch too.
    @Test(
        "store errors classify to persistence_failed rather than to the catch-all",
        arguments: [
            AnalysisStoreError.insertFailed("FOREIGN KEY constraint failed"),
            AnalysisStoreError.queryFailed("no such table: transcript_chunks"),
            AnalysisStoreError.notFound,
        ]
    )
    func storeErrorsClassifyAsPersistenceFailure(error: AnalysisStoreError) {
        #expect(TranscriptFailureClass.classify(error) == .persistenceFailed)
        // And it exports no code: the bridged domain is ours, so the value
        // would only ever be a synthesised case ordinal.
        #expect(TranscriptFailureReason.classify(error).code == nil)
    }

    /// Every case in the closed vocabulary must be REACHABLE. A pinned set is
    /// only a contract if each member can actually be produced; a case no
    /// emitter can ever assign is a promise to a support engineer that the
    /// build cannot keep.
    @Test("no case in the vocabulary is unreachable")
    func everyVariantHasAProducer() {
        // Classes raised directly by the loop rather than by classifying an
        // error — they have no error to route through `classify`.
        let raisedDirectly: Set<TranscriptFailureClass> = [.noShards, .speechEngineNotReady]
        // The residual bucket, produced by construction for anything
        // unrecognised (`unrecognisedBecomesUnknown` pins it).
        let residual: Set<TranscriptFailureClass> = [.unknown]

        var producible: Set<TranscriptFailureClass> = raisedDirectly.union(residual)
        var errors: [Error] = [
            AnalysisStoreError.insertFailed("x"),
            TranscriptEngineError.modelNotLoaded,
            TranscriptEngineError.vadFailed("x"),
            TranscriptEngineError.unsupportedSampleRate(expected: 16_000, actual: 44_100),
            TranscriptEngineError.transcriptionFailed("x"),
        ]
#if canImport(Speech)
        errors.append(contentsOf: [
            AppleSpeechBoundaryError.speechAssetsUnsupported(localeIdentifier: "en_US"),
            AppleSpeechBoundaryError.analyzerFormatUnavailable(localeIdentifier: "en_US"),
            AppleSpeechBoundaryError.invalidAnalyzerInputTimeline("x"),
            AppleSpeechBoundaryError.analyzerSessionFailure("x"),
            AppleSpeechBoundaryError.audioBridgeFailure("converter refused the format"),
            AppleSpeechBoundaryError.audioBridgeFailure("empty audio shard"),
            AppleSpeechBoundaryError.audioBridgeFailure("shard 3 is entirely silent"),
            AppleSpeechBoundaryError.audioBridgeFailure("shard 3 contains NaN and Inf samples"),
        ] as [Error])
#endif
        for error in errors {
            producible.insert(TranscriptFailureClass.classify(error))
        }

        let unreachable = Set(TranscriptFailureClass.allCases).subtracting(producible)
        #expect(
            unreachable.isEmpty,
            """
            no emitter can ever produce \(unreachable.map(\.rawValue).sorted()) — \
            either wire it up or drop it from the vocabulary
            """
        )
    }

    @Test("anything unrecognised becomes .unknown rather than leaking a description")
    func unrecognisedBecomesUnknown() {
        struct Exotic: Error { let secret = "podcast-title-that-must-not-ship" }
        #expect(TranscriptFailureClass.classify(Exotic()) == .unknown)
    }

    // MARK: - The numeric code is redacted, not forwarded

    /// Every Swift error bridges to an `NSError`, so `error as NSError` always
    /// succeeds — a Swift enum gets the synthesised domain "<Module>.<Type>"
    /// and its case ORDINAL as the code. Exporting that would be noise
    /// dressed as data: `.modelNotLoaded` would ship `code: 0` forever.
    @Test("a Swift-native error exports no code")
    func swiftNativeErrorsCarryNoCode() {
        let reason = TranscriptFailureReason.classify(TranscriptEngineError.modelNotLoaded)
        #expect(reason.failureClass == .modelNotLoaded)
        #expect(reason.code == nil,
                "a synthesised case ordinal is not a diagnostic (got \(String(describing: reason.code)))")
    }

    /// A genuine framework error's code IS the thing that separates one
    /// Speech failure from another once the class is known — and an integer
    /// cannot carry PII.
    @Test("a framework NSError exports its code")
    func frameworkErrorsCarryTheirCode() {
        let speechish = NSError(domain: "kAFAssistantErrorDomain", code: 1101, userInfo: nil)
        let reason = TranscriptFailureReason.classify(speechish)
        #expect(reason.failureClass == .unknown)
        #expect(reason.code == 1101)
    }

    // MARK: - Reduction to one exportable reason

    @Test("the dominant failure class wins, and the shard count is the total")
    func dominantFailureIsTheMostFrequent() {
        let failures = [
            TranscriptFailureReason(failureClass: .modelNotLoaded),
            TranscriptFailureReason(failureClass: .silentShard, code: 9),
            TranscriptFailureReason(failureClass: .silentShard),
            TranscriptFailureReason(failureClass: .silentShard),
        ]
        let reduced = TranscriptEngineService.dominantFailure(failures)
        #expect(reduced.failureClass == .silentShard)
        #expect(reduced.failedShardCount == 4, "the count is every failure, not just the dominant class")
        #expect(reduced.code == 9, "the code comes from the first exemplar of the dominant class")
    }

    /// Ties must resolve deterministically. Reducing over a `Dictionary` would
    /// pick whichever key the hash seed happened to order first, so the same
    /// failure would report different classes on different launches — which is
    /// precisely the kind of non-reproducibility that makes device data
    /// unusable.
    @Test("a tie resolves to the first class seen, not to dictionary order")
    func tiesResolveByFirstOccurrence() {
        let failures = [
            TranscriptFailureReason(failureClass: .vadFailed),
            TranscriptFailureReason(failureClass: .silentShard),
        ]
        for _ in 0..<50 {
            #expect(TranscriptEngineService.dominantFailure(failures).failureClass == .vadFailed)
        }
        // And reversing the input reverses the answer — proving the order of
        // occurrence is what decides it, not an accident of the enum's
        // declaration order.
        #expect(TranscriptEngineService.dominantFailure(failures.reversed())
            .failureClass == .silentShard)
    }

    @Test("an empty failure list reduces to .unknown rather than trapping")
    func emptyFailureListIsSafe() {
        #expect(TranscriptEngineService.dominantFailure([]).failureClass == .unknown)
    }
}
