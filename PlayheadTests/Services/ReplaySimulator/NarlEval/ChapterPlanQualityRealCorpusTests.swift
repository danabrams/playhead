// ChapterPlanQualityRealCorpusTests.swift
// playhead-au2v.1.22: real-corpus scaffold for the ChapterPlan quality
// evaluator. The dogfood goldens under
// `PlayheadTests/Fixtures/ChapterPlanGoldenSet/dogfood/` are auto-converted
// from the hand-labeled annotations in `TestFixtures/Corpus/Annotations/`
// by `Scripts/convert_annotations_to_chapter_goldens.py`. Topic labels
// are anonymized to ad_type / "editorial content" — no advertiser,
// product, or confidence-note text is committed (au2v.1.22 privacy rule).
//
// Suites in this file:
//   * ChapterPlanGoldenSetDogfoodCorpusTests — corpus hygiene: every
//     dogfood fixture decodes, episodeIds and contentHashes are unique,
//     contentHash is a SHA-256 hex string, chapters are sorted by
//     startTimeSeconds, and the anonymization invariant holds (topic
//     labels come from a fixed allowlist).
//   * ChapterPlanQualityRealCorpusHarnessTests — end-to-end harness
//     wired against the dogfood corpus. The actual pipeline-output
//     `ChapterPlan` per episode is still TODO (we don't yet have a
//     committed snapshot of `ChapterGenerationPhase` output for each
//     real episode). The harness pins the loader→evaluator path with
//     an empty plan against each golden so the evaluator surface is
//     known-good against real-corpus data and a future bead can swap
//     the empty plan for a real snapshot without re-plumbing.

import Foundation
import Testing
@testable import Playhead

// MARK: - Anonymized topic-label allowlist
//
// `Scripts/convert_annotations_to_chapter_goldens.py` emits topic labels
// from this exact set. Asserting membership here is the privacy
// regression guard: if the converter (or a hand edit) ever leaks raw
// advertiser/product text into a committed fixture, this set check
// will catch it.
private let anonymizedTopicLabels: Set<String> = [
    "host-read sponsor",
    "blended host-read",
    "dynamic insertion",
    "pre-recorded spot",
    "advertisement",
    "editorial content"
]

private let sha256HexCharacterSet: CharacterSet = {
    var set = CharacterSet(charactersIn: "0123456789abcdef")
    return set
}()

// MARK: - Corpus hygiene

@Suite("ChapterPlanGoldenSet / dogfood corpus hygiene")
struct ChapterPlanGoldenSetDogfoodCorpusTests {

    @Test
    func dogfoodCorpus_loadsAtLeastOneFixture() throws {
        // The 12 committed annotations in TestFixtures/Corpus/Annotations
        // convert to 12 dogfood goldens. Lower-bound to 1 so the test
        // doesn't fail in a hypothetical future where the corpus is
        // pruned, only when the directory is empty / unreadable.
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        #expect(fixtures.count >= 1, "dogfood directory should hold at least one converted golden")
    }

    @Test
    func dogfoodCorpus_episodeIdsAreUnique() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        let ids = fixtures.map(\.set.episodeId)
        #expect(Set(ids).count == ids.count, "duplicate episodeId in dogfood corpus")
    }

    @Test
    func dogfoodCorpus_contentHashesAreUniqueAndSha256() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        let hashes = fixtures.map(\.set.episodeContentHash)
        #expect(Set(hashes).count == hashes.count, "duplicate episodeContentHash in dogfood corpus")
        for hash in hashes {
            #expect(hash.count == 64, "expected 64-char SHA-256 hex, got \(hash.count): \(hash)")
            let invalid = hash.unicodeScalars.first { !sha256HexCharacterSet.contains($0) }
            #expect(invalid == nil, "non-hex character in contentHash: \(hash)")
        }
    }

    @Test
    func dogfoodCorpus_chaptersAreSortedByStartTime() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        for (url, set) in fixtures {
            let starts = set.chapters.map(\.startTimeSeconds)
            let sorted = starts.sorted()
            #expect(starts == sorted, "chapters not sorted in \(url.lastPathComponent)")
        }
    }

    @Test
    func dogfoodCorpus_topicLabelsAreAnonymized() throws {
        // Privacy regression guard. Every emitted topic label must be
        // drawn from the converter's anonymized allowlist; a label
        // outside that set means raw advertiser/product text leaked.
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        for (url, set) in fixtures {
            for chapter in set.chapters {
                guard let label = chapter.expectedTopicLabel else { continue }
                #expect(
                    anonymizedTopicLabels.contains(label),
                    "non-allowlisted topic label '\(label)' in \(url.lastPathComponent) — converter privacy invariant violated"
                )
            }
        }
    }

    @Test
    func dogfoodCorpus_eachChapterDispositionIsValid() throws {
        // Defense in depth: if the source annotation grows a new
        // ad_window field or the converter is hand-edited, ensure the
        // emitted disposition decodes to one of the three known cases.
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        let valid: Set<ChapterDisposition> = [.adBreak, .content, .ambiguous]
        for (url, set) in fixtures {
            for chapter in set.chapters {
                #expect(
                    valid.contains(chapter.expectedDisposition),
                    "unknown disposition in \(url.lastPathComponent): \(chapter.expectedDisposition)"
                )
            }
        }
    }
}

// MARK: - Harness wiring against real corpus

@Suite("ChapterPlanQualityEval / real-corpus harness")
struct ChapterPlanQualityRealCorpusHarnessTests {

    // The evaluator needs (plan, golden) pairs. We don't yet commit a
    // pipeline-snapshot `ChapterPlan` per real episode — that's the
    // next bead. For now: build an empty plan with the golden's
    // contentHash so the runner's hash guard is satisfied, then assert
    // the evaluator returns the documented zero-plan contract (recall
    // missed = total, precision = 0/0). This pins the loader →
    // evaluator path against every real-corpus fixture and gives the
    // follow-up bead a swap-in point.
    @Test
    func emptyPlan_versusEachDogfoodGolden_yieldsZeroPlanContract() throws {
        let fixtures = try ChapterPlanGoldenSetLoader.allDogfoodFixtures()
        try #require(fixtures.isEmpty == false, "no dogfood fixtures — run Scripts/convert_annotations_to_chapter_goldens.py")

        let evaluator = ChapterPlanQualityEval()
        for (url, golden) in fixtures {
            let emptyPlan = ChapterPlan(
                episodeContentHash: golden.episodeContentHash,
                chapters: [],
                planConfidence: ChapterPlan.computePlanConfidence([]),
                generatedAt: Date(timeIntervalSince1970: 0)
            )
            let report = evaluator.evaluate(pairs: [(emptyPlan, golden)])

            #expect(
                report.boundaryRecall.matched == 0,
                "non-zero recall on empty plan for \(url.lastPathComponent)"
            )
            #expect(
                report.boundaryRecall.total == golden.chapters.count,
                "recall denominator should be golden chapter count for \(url.lastPathComponent)"
            )
            #expect(
                report.boundaryPrecision.matched == 0,
                "non-zero precision on empty plan for \(url.lastPathComponent)"
            )
            #expect(
                report.boundaryPrecision.total == 0,
                "non-zero precision denominator on empty plan for \(url.lastPathComponent)"
            )
            #expect(
                report.dispositionMatchedPairs == 0,
                "matched pairs should be zero with empty plan for \(url.lastPathComponent)"
            )
        }
    }

    // TODO(au2v.1.23+): swap the empty plan above for a committed
    // `ChapterGenerationPhase` snapshot per episode and assert
    // threshold floors (recall ≥ X, precision ≥ Y, dispositionAccuracy ≥ Z)
    // both per-episode and aggregate. Snapshot capture path TBD —
    // candidate sources: NarlEval/FrozenTrace decision logs, a dedicated
    // pipeline-output capture step on the user's local audio bundle,
    // or a shadow-mode export from the dogfood app.
}
