// DE0784D8MidRollPodFixture.swift
// playhead-j4wi: the measured mid-roll hole, frozen as a regression fixture.
//
// Episode: Diary of a CEO (Ray Dalio interview), 92 min (5522.65 s), device
// asset DE0784D8-BE2F-4BEB-8BA1-3D9EF51AD659. Every number in this file was
// RE-DERIVED from the 2026-08-02 device pull (`analysis.sqlite`), not copied
// from a bead summary table:
//   • ground truth   -> `correction_events` (un-retracted rows only; the three
//     2026-07-31 banner-confirm marks at 210-240 / 2670-2700 / 4800-4950 were
//     each vetoed ~20 min later — mark-then-retract churn pairs — and are
//     deliberately NOT ground truth here),
//   • pipeline output -> `ad_windows` (all 12 rows, frozen below),
//   • transcript      -> `transcript_chunks` (fast pass; the final pass never
//     covered this region because final re-transcription is candidate-local
//     and no candidate existed here).
//
// What the fixture pins (the CURRENT, broken behaviour):
//   • a 116 s two-creative host-read pod at 2838.18-2953.68 that detection
//     produced NOTHING for (Dan marked both creatives by hand), and
//   • the 8.04 s false positive at 2828.40-2836.44 immediately before it —
//     persisted with boundaryState "acousticRefined" but actually LEXICAL: the
//     disclosure regex `\bin partnership with\b` matched show content ("work
//     in partnership with the artificial intelligence"), and the span's 8.04 s
//     geometry is exactly the width of that one ASR chunk, and
//   • the two excellent day-0 rediff outer slots (pre-roll 0-60.13, post-roll
//     5462.57-5522.68) plus Dan's 2.04 s overshoot veto at 60.06-62.10.
//
// The canonical L2F ground-truth annotation for this episode is
// `TestFixtures/Corpus/Annotations/doac-2026-07-31-ray-dalio-de0784d8.json`;
// `DE0784D8MidRollPodRegressionTests` asserts the two stay in agreement.

import Foundation

@testable import Playhead

enum DE0784D8MidRollPodFixture {

    static let analysisAssetId = "DE0784D8-BE2F-4BEB-8BA1-3D9EF51AD659"
    static let corpusEpisodeId = "doac-2026-07-31-ray-dalio-de0784d8"
    /// `analysis_assets.episodeDurationSec` on the device pull.
    static let episodeDurationSeconds = 5522.6514375

    // MARK: - Ground truth (un-retracted correction_events, 2026-08-01)

    /// Creative 1 (WhisperFlow, host-read): Dan's falseNegative mark at
    /// 15:25:22. "I've got 60 seconds ... because of our sponsor called
    /// Whisper Flow" ... "head to whisperflow.ai slash Stephen".
    static let creative1: ClosedRange<Double> = 2838.18 ... 2897.94

    /// Creative 2 (Ketone-IQ, host-read): Dan's falseNegative mark at
    /// 15:26:04. "Just like John Jones ..." ... "go to ketone.com slash
    /// Stephen ... exclusive keto and IQ merch".
    static let creative2: ClosedRange<Double> = 2898.66 ... 2953.68

    /// The 8.04 s false positive Dan vetoed at 15:24:33 (manualVeto,
    /// falsePositive, causalSource "lexical", atom 2551).
    static let acousticFalsePositive: ClosedRange<Double> = 2828.4 ... 2836.44

    /// Dan's 2.04 s pre-roll overshoot veto at 15:23:34 (manualVeto,
    /// falsePositive). The day-0 rediff window ends at 60.133; the veto says
    /// everything from 60.06 on is show.
    static let preRollOvershootVeto: ClosedRange<Double> = 60.06 ... 62.1

    /// Total pod width the pipeline missed (both creatives, excluding the
    /// 0.72 s seam between them).
    static var podWidthSeconds: Double {
        (creative1.upperBound - creative1.lowerBound)
            + (creative2.upperBound - creative2.lowerBound)
    }

    // MARK: - Frozen pipeline output (`ad_windows`, device pull 2026-08-02)

    struct FrozenWindow: Sendable {
        let id: String
        let startTime: Double
        let endTime: Double
        let confidence: Double
        let boundaryState: String
        let decisionState: String
        let eligibilityGate: String?
        let detectorVersion: String

        /// Seconds of overlap between this window and `range`.
        func overlap(with range: ClosedRange<Double>) -> Double {
            max(0, min(endTime, range.upperBound) - max(startTime, range.lowerBound))
        }

        /// True when this row was produced by detection rather than replayed
        /// back from one of the listener's own corrections. Rows the user
        /// created (marks) or confirmed (banner taps) must never be counted
        /// as detection reach.
        var isDetectionProduced: Bool {
            detectorVersion != "userCorrection" && boundaryState != "userConfirmedSuggested"
        }
    }

    /// All 12 `ad_windows` rows for the asset, verbatim.
    static let devicePullAdWindows: [FrozenWindow] = [
        FrozenWindow(
            id: "0FCE1F75-1610-4261-8ED0-51712E8606A5", startTime: 0.0, endTime: 60.13346025950383,
            confidence: 1.0, boundaryState: "dayZeroRediffByteExact", decisionState: "reverted",
            eligibilityGate: "markOnly", detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "B63DFC91-F709-4BB5-80F2-12F6CA31A74D", startTime: 210.0, endTime: 240.0,
            confidence: 1.0, boundaryState: "userConfirmedSuggested", decisionState: "reverted",
            eligibilityGate: nil, detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "fusion-fdb0b42c16139351", startTime: 210.12, endTime: 239.82,
            confidence: 0.0015781934036755733, boundaryState: "acousticRefined", decisionState: "reverted",
            eligibilityGate: "blockedByUserCorrection", detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "fusion-7892299324c9e90f", startTime: 2667.3, endTime: 2702.82,
            confidence: 0.0034568565990255808, boundaryState: "acousticRefined", decisionState: "confirmed",
            eligibilityGate: "blockedByUserCorrection", detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "FFC30187-A119-4C63-8A71-6B2EC063B0D7", startTime: 2670.0, endTime: 2700.0,
            confidence: 1.0, boundaryState: "userConfirmedSuggested", decisionState: "reverted",
            eligibilityGate: nil, detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "fusion-f8d4c169288e9a37", startTime: 2828.4, endTime: 2836.44,
            confidence: 0.001150758771374174, boundaryState: "acousticRefined", decisionState: "reverted",
            eligibilityGate: "blockedByUserCorrection", detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "5E78EBB1-EFAC-4368-BCB9-EDECEDBCBF50", startTime: 2838.18, endTime: 2897.94,
            confidence: 1.0, boundaryState: "userMarked", decisionState: "confirmed",
            eligibilityGate: "eligible", detectorVersion: "userCorrection"
        ),
        FrozenWindow(
            id: "FA190555-A9D8-45BC-93D9-4F6C245649D2", startTime: 2898.66, endTime: 2953.68,
            confidence: 1.0, boundaryState: "userMarked", decisionState: "confirmed",
            eligibilityGate: "eligible", detectorVersion: "userCorrection"
        ),
        FrozenWindow(
            id: "fusion-d4e332f1a5d9221c", startTime: 4329.96, endTime: 4342.2,
            confidence: 0.003915335846121139, boundaryState: "acousticRefined", decisionState: "confirmed",
            eligibilityGate: "blockedByUserCorrection", detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "B6DFB026-8A30-4071-832C-F3CD218FFA5C", startTime: 4800.0, endTime: 4950.0,
            confidence: 1.0, boundaryState: "userConfirmedSuggested", decisionState: "reverted",
            eligibilityGate: nil, detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "fusion-05f4ddfceef0e09d", startTime: 4800.48, endTime: 4949.82,
            confidence: 6.7274513060489565e-06, boundaryState: "acousticRefined", decisionState: "reverted",
            eligibilityGate: "blockedByUserCorrection", detectorVersion: "detection-v1"
        ),
        FrozenWindow(
            id: "FCD34BBF-66C2-4A9D-8566-DE61A25EFDC4", startTime: 5462.570296660244, endTime: 5522.677551012367,
            confidence: 1.0, boundaryState: "dayZeroRediffByteExact", decisionState: "candidate",
            eligibilityGate: "markOnly", detectorVersion: "detection-v1"
        ),
    ]

    /// The frozen false-positive row, by id (fails loudly if the table above
    /// is ever edited out from under the tests).
    static var frozenFalsePositiveWindow: FrozenWindow? {
        devicePullAdWindows.first { $0.id == "fusion-f8d4c169288e9a37" }
    }

    // MARK: - Frozen transcript (fast pass, 2820.24-2961.18)

    struct Chunk: Sendable {
        let startTime: Double
        let endTime: Double
        let text: String

        init(_ startTime: Double, _ endTime: Double, _ text: String) {
            self.startTime = startTime
            self.endTime = endTime
            self.text = text
        }
    }

    /// Every fast-pass `transcript_chunks` row intersecting (2820, 2962),
    /// verbatim: show content into the seam, the seam chunk the disclosure
    /// regex false-fires on, both creatives, and show content resuming.
    static let seamAndPodChunks: [Chunk] = [
        Chunk(2820.24, 2822.22, "future. Those who"),
        Chunk(2822.22, 2826.84, "can work very well where they have an exceptional human"),
        Chunk(2826.84, 2828.4, "intelligence and"),
        Chunk(2828.4, 2836.44, "work in partnership with the artificial intelligence, that they are going to be at the cutting edge of all of"),
        Chunk(2836.44, 2836.62, "this."),
        Chunk(2838.18, 2839.26, "I've got 60"),
        Chunk(2839.26, 2839.68, "seconds and"),
        Chunk(2839.68, 2841.0, "I'm going to show you how much I can get"),
        Chunk(2841.0, 2841.66, "done because"),
        Chunk(2841.66, 2843.88, "of our sponsor called Whisper"),
        Chunk(2843.88, 2844.48, "Flow."),
        Chunk(2844.54, 2848.92, "And for those of you that don't know what it is, it's a business I invested in that turns your speech into"),
        Chunk(2848.92, 2849.52, "text in"),
        Chunk(2849.52, 2850.0, "any."),
        Chunk(2850.84, 2851.26, "I'm"),
        Chunk(2851.26, 2852.04, "going to post into our"),
        Chunk(2852.04, 2852.28, "Slack"),
        Chunk(2852.28, 2852.64, "Channel,"),
        Chunk(2852.64, 2855.88, "which rewards whichever team member conducted the most experiments this week."),
        Chunk(2856.0, 2856.3, "Hi"),
        Chunk(2856.3, 2856.9, "everyone."),
        Chunk(2856.96, 2857.26, "Here"),
        Chunk(2857.26, 2858.4, "is this week's"),
        Chunk(2858.4, 2859.06, "experimenter"),
        Chunk(2859.06, 2859.3, "of the"),
        Chunk(2859.3, 2859.66, "week."),
        Chunk(2859.72, 2861.34, "Congratulations to the dire of a"),
        Chunk(2861.34, 2861.7, "CEO"),
        Chunk(2861.7, 2863.02, "trailer team, which is"),
        Chunk(2863.02, 2864.46, "Ant, Liv,"),
        Chunk(2864.46, 2865.48, "Dom, and Cam."),
        Chunk(2865.54, 2865.78, "You"),
        Chunk(2865.78, 2866.38, "guys have"),
        Chunk(2866.38, 2866.56, "one."),
        Chunk(2867.28, 2867.82, "Okay, now"),
        Chunk(2867.82, 2868.06, "I'm"),
        Chunk(2868.06, 2868.12, "going"),
        Chunk(2868.12, 2868.3, "to open"),
        Chunk(2868.3, 2868.84, "Gmail."),
        Chunk(2868.9, 2869.38, "So here"),
        Chunk(2869.38, 2871.12, "is one of our founders on an email chain that I"),
        Chunk(2871.12, 2871.24, "want"),
        Chunk(2871.24, 2871.42, "to connect"),
        Chunk(2871.42, 2871.66, "my team"),
        Chunk(2871.66, 2872.14, "with."),
        Chunk(2872.2, 2872.74, "All I have to say"),
        Chunk(2872.74, 2873.76, "is add"),
        Chunk(2873.76, 2874.72, "my team's"),
        Chunk(2874.72, 2876.1, "emails and"),
        Chunk(2876.1, 2876.52, "Whisper"),
        Chunk(2876.52, 2877.9, "will do exactly that."),
        Chunk(2877.96, 2879.1, "Now a quick message to"),
        Chunk(2879.1, 2879.82, "Juan,"),
        Chunk(2879.82, 2879.94, "who"),
        Chunk(2879.94, 2880.0, "does."),
        Chunk(2880.0, 2880.42, "my schedule"),
        Chunk(2880.42, 2880.78, "every single"),
        Chunk(2880.78, 2881.2, "week."),
        Chunk(2881.26, 2882.04, "Hey, Juan,"),
        Chunk(2882.04, 2883.96, "can I record on Wednesday at 2"),
        Chunk(2883.96, 2884.32, "p?"),
        Chunk(2884.38, 2885.16, "Actually, no, do you know what?"),
        Chunk(2885.22, 2885.46, "lets record"),
        Chunk(2885.46, 2885.82, "it 3"),
        Chunk(2885.82, 2886.3, "PM on"),
        Chunk(2886.3, 2886.96, "Wednesday."),
        Chunk(2887.62, 2888.52, "Whisperflow"),
        Chunk(2888.52, 2889.84, "is 4 times faster than"),
        Chunk(2889.84, 2890.62, "typing and"),
        Chunk(2890.62, 2891.88, "it is incredibly easy to use."),
        Chunk(2891.94, 2894.22, "So if you want to give it a go, all you have to do is head to"),
        Chunk(2894.22, 2896.08, "whisperflow.ai"),
        Chunk(2896.08, 2896.62, "slash"),
        Chunk(2896.62, 2896.86, "Stephen"),
        Chunk(2896.86, 2897.94, "to download it today."),
        Chunk(2898.66, 2905.08, "Just like John Jones, where marginal improvements in your cognitive performance can have a massive impact, sometimes I podcast for 10 hours a"),
        Chunk(2905.08, 2905.26, "day,"),
        Chunk(2905.26, 2905.5, "over the"),
        Chunk(2905.5, 2907.9, "last couple of weeks, I've been in filming for a TV"),
        Chunk(2907.9, 2908.44, "show, and"),
        Chunk(2908.44, 2909.82, "then I have like one or 2 days off to"),
        Chunk(2909.82, 2910.0, "get..."),
        Chunk(2910.0, 2910.42, "All of"),
        Chunk(2910.42, 2913.0, "my work done, which means there's lots of cognitive load."),
        Chunk(2913.06, 2913.24, "And"),
        Chunk(2913.24, 2913.54, "so I"),
        Chunk(2913.54, 2913.72, "turn"),
        Chunk(2913.72, 2915.46, "to ketones because I find myself more"),
        Chunk(2915.46, 2916.54, "articulate, able"),
        Chunk(2916.54, 2921.28, "to think more clearly, able to work out better when I'm fueled by ketones."),
        Chunk(2921.34, 2922.96, "And so the reason I became a co-oner of this"),
        Chunk(2922.96, 2923.32, "company, and"),
        Chunk(2923.32, 2924.16, "the reason why they now are"),
        Chunk(2924.16, 2924.58, "responsible"),
        Chunk(2924.58, 2924.64, "for"),
        Chunk(2924.64, 2924.76, "this"),
        Chunk(2924.76, 2925.48, "podcast, is"),
        Chunk(2925.48, 2927.34, "because I remember one of my team members called"),
        Chunk(2927.34, 2927.82, "Cristiana."),
        Chunk(2927.88, 2928.0, "She"),
        Chunk(2928.0, 2929.32, "tried it once and came up to my desk and she"),
        Chunk(2929.32, 2929.92, "goes, this"),
        Chunk(2929.92, 2931.42, "is the best product ever made."),
        Chunk(2931.96, 2932.62, "And I think in"),
        Chunk(2932.62, 2933.22, "part that's"),
        Chunk(2933.22, 2935.08, "because she really cares about those cognitive"),
        Chunk(2935.08, 2935.56, "benefits, as"),
        Chunk(2935.56, 2936.64, "I do, as John Jones"),
        Chunk(2936.64, 2937.18, "does."),
        Chunk(2937.24, 2937.54, "And as"),
        Chunk(2937.54, 2937.66, "I"),
        Chunk(2937.66, 2937.96, "think most"),
        Chunk(2937.96, 2938.68, "of my listeners probably"),
        Chunk(2938.68, 2939.04, "will."),
        Chunk(2939.1, 2939.58, "So if you"),
        Chunk(2939.58, 2939.88, "haven't"),
        Chunk(2940.84, 2944.44, "All you have to do is go to ketone.com slash"),
        Chunk(2944.44, 2945.7, "Stephen, and"),
        Chunk(2945.7, 2947.86, "you'll also get 30% off your 1st subscription order."),
        Chunk(2947.92, 2948.58, "You'll get exclusive"),
        Chunk(2948.58, 2948.94, "keto"),
        Chunk(2948.94, 2949.36, "and IQ"),
        Chunk(2949.36, 2949.9, "merch."),
        Chunk(2949.96, 2950.14, "And"),
        Chunk(2950.14, 2950.32, "of"),
        Chunk(2950.32, 2951.88, "course, cognitive"),
        Chunk(2951.88, 2953.68, "benefits that might just change your life."),
        Chunk(2954.88, 2958.96, "So if you had kids that were 16 years old now, Ray, and they said,"),
        Chunk(2958.96, 2959.26, "dad,"),
        Chunk(2959.26, 2959.62, "what do you"),
        Chunk(2959.62, 2959.98, "think based"),
        Chunk(2959.98, 2961.18, "on everything you know about the future?"),
    ]

    /// Build `TranscriptAtom`s from the frozen chunks the same way production
    /// does (one chunk = one atom, positional ordinals in time order — see
    /// `TranscriptAtomizer.atomize`). Ordinals are excerpt-local; every
    /// assertion in the regression tests is by TIME, not absolute ordinal.
    static func atoms() -> [TranscriptAtom] {
        seamAndPodChunks.enumerated().map { ordinal, chunk in
            TranscriptAtom(
                atomKey: TranscriptAtomKey(
                    analysisAssetId: analysisAssetId,
                    transcriptVersion: "device-pull-2026-08-02",
                    atomOrdinal: ordinal
                ),
                contentHash: "j4wi-\(ordinal)",
                startTime: chunk.startTime,
                endTime: chunk.endTime,
                text: chunk.text,
                chunkIndex: ordinal
            )
        }
    }
}
