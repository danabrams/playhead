// ChapterEvidenceTests.swift
// ef2.2.4: Tests for chapter-marker parsing, disposition classification,
// quality scoring, and edge cases.

import Foundation
import Testing
@testable import Playhead

// MARK: - Disposition Classifier Tests

@Suite("ChapterDispositionClassifier")
struct ChapterDispositionClassifierTests {

    private let classifier = ChapterDispositionClassifier()

    // MARK: - Ad Break Detection

    @Test("classifies explicit 'Ad' title as adBreak")
    func adExplicit() {
        #expect(classifier.classify("Ad") == .adBreak)
    }

    @Test("classifies 'Advertisement' title as adBreak")
    func advertisement() {
        #expect(classifier.classify("Advertisement") == .adBreak)
    }

    @Test("classifies 'Ad Break' as adBreak")
    func adBreak() {
        #expect(classifier.classify("Ad Break") == .adBreak)
    }

    @Test("classifies 'Ads' as adBreak")
    func ads() {
        #expect(classifier.classify("Ads") == .adBreak)
    }

    @Test("classifies 'Sponsored by BetterHelp' as adBreak")
    func sponsoredBy() {
        #expect(classifier.classify("Sponsored by BetterHelp") == .adBreak)
    }

    @Test("classifies 'Brought to you by Squarespace' as adBreak")
    func broughtToYouBy() {
        #expect(classifier.classify("Brought to you by Squarespace") == .adBreak)
    }

    @Test("classifies 'Presented by Athletic Greens' as adBreak")
    func presentedBy() {
        #expect(classifier.classify("Presented by Athletic Greens") == .adBreak)
    }

    @Test("classifies 'Mid-roll' as adBreak")
    func midRoll() {
        #expect(classifier.classify("Mid-roll") == .adBreak)
    }

    @Test("classifies 'Midroll' as adBreak")
    func midrollNoHyphen() {
        #expect(classifier.classify("Midroll") == .adBreak)
    }

    @Test("classifies 'Pre-roll' as adBreak")
    func preRoll() {
        #expect(classifier.classify("Pre-roll") == .adBreak)
    }

    @Test("classifies 'Post-roll' as adBreak")
    func postRoll() {
        #expect(classifier.classify("Post-roll") == .adBreak)
    }

    @Test("classifies 'Commercial Break' as adBreak")
    func commercialBreak() {
        #expect(classifier.classify("Commercial Break") == .adBreak)
    }

    @Test("classifies 'Sponsor' as adBreak")
    func sponsor() {
        #expect(classifier.classify("Sponsor") == .adBreak)
    }

    @Test("classifies 'Sponsorship Message' as adBreak")
    func sponsorshipMessage() {
        #expect(classifier.classify("Sponsorship Message") == .adBreak)
    }

    @Test("classifies 'Promo' as adBreak")
    func promo() {
        #expect(classifier.classify("Promo") == .adBreak)
    }

    @Test("classifies 'Promotion' as adBreak")
    func promotion() {
        #expect(classifier.classify("Promotion") == .adBreak)
    }

    @Test("classifies 'Supported by ExpressVPN' as adBreak")
    func supportedBy() {
        #expect(classifier.classify("Supported by ExpressVPN") == .adBreak)
    }

    @Test("classifies 'Special Offer from HelloFresh' as adBreak")
    func specialOffer() {
        #expect(classifier.classify("Special Offer from HelloFresh") == .adBreak)
    }

    @Test("case-insensitive ad detection")
    func caseInsensitive() {
        #expect(classifier.classify("ADVERTISEMENT") == .adBreak)
        #expect(classifier.classify("sponsored BY betterhelp") == .adBreak)
        #expect(classifier.classify("BROUGHT TO YOU BY") == .adBreak)
    }

    // MARK: - Content Detection

    @Test("classifies 'Interview with Dr. Smith' as content")
    func interviewContent() {
        #expect(classifier.classify("Interview with Dr. Smith") == .content)
    }

    @Test("classifies 'Introduction' as content")
    func introduction() {
        #expect(classifier.classify("Introduction") == .content)
    }

    @Test("classifies 'Intro' as content")
    func intro() {
        #expect(classifier.classify("Intro") == .content)
    }

    @Test("classifies 'Outro' as content")
    func outro() {
        #expect(classifier.classify("Outro") == .content)
    }

    @Test("classifies 'Conclusion' as content")
    func conclusion() {
        #expect(classifier.classify("Conclusion") == .content)
    }

    @Test("classifies 'Discussion about climate change' as content")
    func discussion() {
        #expect(classifier.classify("Discussion about climate change") == .content)
    }

    @Test("classifies 'Q&A with listeners' as content")
    func qAndA() {
        #expect(classifier.classify("Q&A with listeners") == .content)
    }

    @Test("classifies 'Part 1' as content")
    func partNumbered() {
        #expect(classifier.classify("Part 1") == .content)
    }

    @Test("classifies 'News Update' as content")
    func newsUpdate() {
        #expect(classifier.classify("News Update") == .content)
    }

    @Test("classifies 'Wrap-up and final thoughts' as content")
    func wrapUp() {
        #expect(classifier.classify("Wrap-up and final thoughts") == .content)
    }

    @Test("classifies long descriptive title as content via word count")
    func longDescriptiveTitle() {
        #expect(classifier.classify("The history of medieval castle architecture") == .content)
    }

    // MARK: - Ambiguous Detection

    @Test("classifies nil title as ambiguous")
    func nilTitle() {
        #expect(classifier.classify(nil) == .ambiguous)
    }

    @Test("classifies empty title as ambiguous")
    func emptyTitle() {
        #expect(classifier.classify("") == .ambiguous)
    }

    @Test("classifies whitespace-only title as ambiguous")
    func whitespaceTitle() {
        #expect(classifier.classify("   ") == .ambiguous)
    }

    @Test("classifies short generic title as ambiguous")
    func shortGenericTitle() {
        // "Break" alone doesn't match ad patterns (no "ad" prefix) and isn't
        // a content keyword, and is only 1 word.
        #expect(classifier.classify("Hello") == .ambiguous)
    }

    @Test("classifies numbered marker as ambiguous")
    func numberedMarker() {
        #expect(classifier.classify("03") == .ambiguous)
    }

    // MARK: - Priority: Ad > Content

    @Test("ad pattern wins when title contains both ad and content markers")
    func adPriorityOverContent() {
        // "Sponsor Interview" has both "sponsor" (ad) and "interview" (content)
        #expect(classifier.classify("Sponsor Interview") == .adBreak)
    }

    // MARK: - Word boundary safety

    @Test("'Madam' does not match ad pattern — classifies as ambiguous")
    func madamNoFalsePositive() {
        // "ad" is inside "Madam" but word boundary should prevent match
        #expect(classifier.classify("Madam") == .ambiguous)
    }

    @Test("'iPad' does not match ad pattern — classifies as ambiguous")
    func iPadNoFalsePositive() {
        // "ad" is at the end of "iPad" but word boundary should prevent match
        #expect(classifier.classify("iPad") == .ambiguous)
    }
}

// MARK: - Quality Scorer Tests

@Suite("ChapterQualityScorer")
struct ChapterQualityScorerTests {

    private let scorer = ChapterQualityScorer()

    @Test("untitled chapter with no end time scores lowest")
    func untitledNoEndTime() {
        let score = scorer.score(title: nil, disposition: .ambiguous, hasEndTime: false, source: .id3)
        #expect(score < 0.1)
    }

    @Test("titled adBreak with end time from PC20 scores highest")
    func highQualityAdBreak() {
        let score = scorer.score(
            title: "Sponsored by BetterHelp - Mental Health Support",
            disposition: .adBreak,
            hasEndTime: true,
            source: .pc20
        )
        #expect(score >= 0.9)
    }

    @Test("titled content chapter scores moderately")
    func titledContent() {
        let score = scorer.score(
            title: "Interview",
            disposition: .content,
            hasEndTime: true,
            source: .id3
        )
        #expect(score > 0.5)
        #expect(score < 0.9)
    }

    @Test("PC20 source scores higher than ID3")
    func pc20HigherThanID3() {
        let pc20Score = scorer.score(title: "Ad", disposition: .adBreak, hasEndTime: true, source: .pc20)
        let id3Score = scorer.score(title: "Ad", disposition: .adBreak, hasEndTime: true, source: .id3)
        #expect(pc20Score > id3Score)
    }

    @Test("having end time adds quality")
    func endTimeBoost() {
        let withEnd = scorer.score(title: "Ad", disposition: .adBreak, hasEndTime: true, source: .pc20)
        let withoutEnd = scorer.score(title: "Ad", disposition: .adBreak, hasEndTime: false, source: .pc20)
        #expect(withEnd > withoutEnd)
    }

    @Test("longer titles score higher than short titles")
    func longerTitleBoost() {
        let longScore = scorer.score(
            title: "Sponsored by BetterHelp Mental Health",
            disposition: .adBreak,
            hasEndTime: false,
            source: .id3
        )
        let shortScore = scorer.score(
            title: "Ad",
            disposition: .adBreak,
            hasEndTime: false,
            source: .id3
        )
        #expect(longScore > shortScore)
    }

    @Test("score never exceeds 1.0")
    func scoreCapped() {
        let score = scorer.score(
            title: "A very long and descriptive sponsored content chapter title",
            disposition: .adBreak,
            hasEndTime: true,
            source: .pc20
        )
        #expect(score <= 1.0)
    }
}

// MARK: - Podcasting 2.0 JSON Parsing Tests

@Suite("PC20 JSON Parsing")
struct PC20JSONParsingTests {

    @Test("parses valid chapters JSON")
    func validJSON() {
        let json = """
        {
            "version": "1.2.0",
            "chapters": [
                {"startTime": 0, "title": "Introduction"},
                {"startTime": 120, "endTime": 180, "title": "Sponsored by BetterHelp"},
                {"startTime": 180, "title": "Main Discussion"}
            ]
        }
        """
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data(json.utf8))

        #expect(evidence.count == 3)
        #expect(evidence[0].title == "Introduction")
        #expect(evidence[0].disposition == .content)
        #expect(evidence[0].source == .pc20)

        #expect(evidence[1].title == "Sponsored by BetterHelp")
        #expect(evidence[1].disposition == .adBreak)
        #expect(evidence[1].startTime == 120)
        #expect(evidence[1].endTime == 180)

        #expect(evidence[2].title == "Main Discussion")
        #expect(evidence[2].disposition == .content)
    }

    @Test("handles missing endTime gracefully")
    func missingEndTime() {
        let json = """
        {"version": "1.0.0", "chapters": [{"startTime": 0, "title": "Only Chapter"}]}
        """
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data(json.utf8))
        #expect(evidence.count == 1)
        #expect(evidence[0].endTime == nil)
    }

    @Test("skips chapters with toc=false")
    func tocFalseSkipped() {
        let json = """
        {
            "version": "1.0.0",
            "chapters": [
                {"startTime": 0, "title": "Visible Chapter"},
                {"startTime": 60, "title": "Hidden Chapter", "toc": false},
                {"startTime": 120, "title": "Another Visible"}
            ]
        }
        """
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data(json.utf8))
        #expect(evidence.count == 2)
        #expect(evidence[0].title == "Visible Chapter")
        #expect(evidence[1].title == "Another Visible")
    }

    @Test("returns empty for invalid JSON")
    func invalidJSON() {
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data("not json".utf8))
        #expect(evidence.isEmpty)
    }

    @Test("returns empty for empty chapters array")
    func emptyChapters() {
        let json = """
        {"version": "1.0.0", "chapters": []}
        """
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data(json.utf8))
        #expect(evidence.isEmpty)
    }

    @Test("handles chapters without titles")
    func untitledChapters() {
        let json = """
        {"version": "1.0.0", "chapters": [{"startTime": 0}, {"startTime": 60}]}
        """
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data(json.utf8))
        #expect(evidence.count == 2)
        #expect(evidence[0].title == nil)
        #expect(evidence[0].disposition == .ambiguous)
    }

    @Test("chapters are sorted by startTime")
    func sortedOutput() {
        let json = """
        {
            "version": "1.0.0",
            "chapters": [
                {"startTime": 300, "title": "End"},
                {"startTime": 0, "title": "Start"},
                {"startTime": 150, "title": "Middle"}
            ]
        }
        """
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data(json.utf8))
        #expect(evidence[0].startTime == 0)
        #expect(evidence[1].startTime == 150)
        #expect(evidence[2].startTime == 300)
    }
}

// MARK: - RSS Inline Chapter Conversion Tests

@Suite("RSS Inline Chapter Conversion")
struct RSSInlineChapterTests {

    @Test("converts parsed chapters with inferred end times")
    func inferredEndTimes() {
        let chapters = [
            ParsedChapter(startTime: 0, title: "Intro", url: nil, imageURL: nil),
            ParsedChapter(startTime: 60, title: "Ad Break", url: nil, imageURL: nil),
            ParsedChapter(startTime: 120, title: "Main Content", url: nil, imageURL: nil),
        ]
        let evidence = ChapterEvidenceParser.fromParsedChapters(chapters, episodeDuration: 3600)

        #expect(evidence.count == 3)
        #expect(evidence[0].endTime == 60)   // next chapter's start
        #expect(evidence[1].endTime == 120)  // next chapter's start
        #expect(evidence[2].endTime == 3600) // episode duration
    }

    @Test("last chapter has nil endTime when episode duration unknown")
    func unknownDuration() {
        let chapters = [
            ParsedChapter(startTime: 0, title: "Only", url: nil, imageURL: nil),
        ]
        let evidence = ChapterEvidenceParser.fromParsedChapters(chapters)
        #expect(evidence.count == 1)
        #expect(evidence[0].endTime == nil)
    }

    @Test("empty chapters produces empty evidence")
    func emptyInput() {
        let evidence = ChapterEvidenceParser.fromParsedChapters([])
        #expect(evidence.isEmpty)
    }

    @Test("classifies RSS chapters correctly with rssInline source")
    func classifiesCorrectly() {
        let chapters = [
            ParsedChapter(startTime: 0, title: "Intro", url: nil, imageURL: nil),
            ParsedChapter(startTime: 60, title: "Sponsored by Casper", url: nil, imageURL: nil),
        ]
        let evidence = ChapterEvidenceParser.fromParsedChapters(chapters)
        #expect(evidence[0].disposition == .content)
        #expect(evidence[0].source == .rssInline)
        #expect(evidence[1].disposition == .adBreak)
        #expect(evidence[1].source == .rssInline)
    }

    @Test("unsorted input is sorted by startTime")
    func sortsInput() {
        let chapters = [
            ParsedChapter(startTime: 120, title: "End", url: nil, imageURL: nil),
            ParsedChapter(startTime: 0, title: "Start", url: nil, imageURL: nil),
        ]
        let evidence = ChapterEvidenceParser.fromParsedChapters(chapters)
        #expect(evidence[0].startTime == 0)
        #expect(evidence[1].startTime == 120)
    }
}

// MARK: - ChapterEvidence Codable Tests

@Suite("ChapterEvidence Codable")
struct ChapterEvidenceCodableTests {

    @Test("round-trips through JSON encoding/decoding")
    func roundTrip() throws {
        let original = ChapterEvidence(
            startTime: 120.5,
            endTime: 180.0,
            title: "Sponsored by BetterHelp",
            source: .pc20,
            disposition: .adBreak,
            qualityScore: 0.85
        )

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(ChapterEvidence.self, from: data)

        #expect(decoded == original)
    }

    @Test("handles nil endTime in round-trip")
    func nilEndTimeRoundTrip() throws {
        let original = ChapterEvidence(
            startTime: 0,
            endTime: nil,
            title: nil,
            source: .id3,
            disposition: .ambiguous,
            qualityScore: 0.1
        )

        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(ChapterEvidence.self, from: data)

        #expect(decoded == original)
    }
}

// MARK: - Time Validation Tests

@Suite("PC20 Chapter Time Validation")
struct PC20TimeValidationTests {

    @Test("skips chapters with negative startTime")
    func negativeStartTime() {
        let json = """
        {"version": "1.0.0", "chapters": [
            {"startTime": -5, "title": "Bad Chapter"},
            {"startTime": 0, "title": "Good Chapter"}
        ]}
        """
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data(json.utf8))
        #expect(evidence.count == 1)
        #expect(evidence[0].title == "Good Chapter")
    }

    @Test("skips chapters with inverted time range (endTime < startTime)")
    func invertedTimeRange() {
        let json = """
        {"version": "1.0.0", "chapters": [
            {"startTime": 100, "endTime": 50, "title": "Inverted"},
            {"startTime": 0, "endTime": 60, "title": "Valid"}
        ]}
        """
        let evidence = ChapterEvidenceParser.decodePC20ChaptersJSON(Data(json.utf8))
        #expect(evidence.count == 1)
        #expect(evidence[0].title == "Valid")
    }

    @Test("skips chapters with NaN startTime")
    func nanStartTime() {
        // NaN/Inf can't be expressed in JSON directly, but test the RSS inline path.
        let chapters = [
            ParsedChapter(startTime: .nan, title: "NaN", url: nil, imageURL: nil),
            ParsedChapter(startTime: 0, title: "Valid", url: nil, imageURL: nil),
        ]
        let evidence = ChapterEvidenceParser.fromParsedChapters(chapters)
        #expect(evidence.count == 1)
        #expect(evidence[0].title == "Valid")
    }

    @Test("skips chapters with infinite startTime")
    func infiniteStartTime() {
        let chapters = [
            ParsedChapter(startTime: .infinity, title: "Inf", url: nil, imageURL: nil),
            ParsedChapter(startTime: 60, title: "Valid", url: nil, imageURL: nil),
        ]
        let evidence = ChapterEvidenceParser.fromParsedChapters(chapters)
        #expect(evidence.count == 1)
        #expect(evidence[0].title == "Valid")
    }
}
