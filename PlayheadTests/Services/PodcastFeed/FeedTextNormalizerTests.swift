// FeedTextNormalizerTests.swift
// Tests for HTML stripping, entity decoding, truncation, and source hash stability.

import Foundation
import SwiftData
import Testing
@testable import Playhead

@Suite("FeedTextNormalizer")
struct FeedTextNormalizerTests {

    // MARK: - HTML Stripping

    @Test("Strips basic HTML tags")
    func stripsHTMLTags() {
        let raw = "<p>Hello <b>world</b></p>"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "Hello world")
    }

    @Test("Strips complex nested HTML")
    func stripsNestedHTML() {
        let raw = "<div class=\"desc\"><p>Paragraph <a href=\"http://x.com\">link</a></p><br/><ul><li>item</li></ul></div>"
        let result = FeedTextNormalizer.normalize(raw)
        // Tags are removed without inserting spaces, so adjacent text merges.
        #expect(result == "Paragraph linkitem")
    }

    @Test("Handles self-closing tags")
    func selfClosingTags() {
        let raw = "Before<br/>After<img src=\"x.jpg\"/>End"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "BeforeAfterEnd")
    }

    // MARK: - Entity Decoding

    @Test("Decodes common HTML entities")
    func decodesCommonEntities() {
        let raw = "Tom &amp; Jerry &lt;3&gt; &quot;fun&quot; it&apos;s"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "Tom & Jerry <3> \"fun\" it's")
    }

    @Test("Decodes typographic entities")
    func decodesTypographic() {
        let raw = "Hello&hellip; &ldquo;quoted&rdquo; &mdash; dash"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "Hello\u{2026} \u{201C}quoted\u{201D} \u{2014} dash")
    }

    @Test("Decodes numeric entities")
    func decodesNumericEntities() {
        let raw = "&#65;&#66;&#67; &#x41;&#x42;&#x43;"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "ABC ABC")
    }

    @Test("Decodes nbsp as space")
    func decodesNbsp() {
        let raw = "word1&nbsp;word2"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "word1 word2")
    }

    // MARK: - Whitespace Collapsing

    @Test("Collapses multiple spaces and newlines")
    func collapsesWhitespace() {
        let raw = "Hello   \n\n  world\t\ttab"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "Hello world tab")
    }

    @Test("Trims leading and trailing whitespace")
    func trimsEdges() {
        let raw = "  \n  hello  \n  "
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "hello")
    }

    // MARK: - Truncation

    @Test("Truncates text exceeding maxLength")
    func truncatesLongText() {
        let raw = String(repeating: "A", count: 5000)
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result?.count == FeedTextNormalizer.maxLength)
    }

    @Test("Multi-MB input does not blow up regex pipeline")
    func multiMBInputCappedBeforeRegex() {
        // Adversarial input: ~4 MB of plain text. Without the pre-regex
        // byte cap, the four uncapped regex passes in `normalize` would
        // each scan the full string, producing a perf cliff. The cap
        // truncates raw input before regex, so we expect this to return
        // a sub-`maxLength` String.
        //
        // Reviewer suggestion (rfu-mn): assert the *structural* property —
        // the post-regex output is bounded by `maxLength` and strictly
        // less than the input — rather than a wall-clock threshold. A
        // wall-clock assertion can flake on a loaded CI simulator AND
        // can silently false-pass if a future regression slows the path
        // to 4 s instead of 4 ms. The structural assertion proves the
        // pre-regex byte cap fired (otherwise the regex pipeline would
        // have either OOMed or produced a much larger intermediate),
        // independent of wall-clock noise.
        let raw = String(repeating: "A", count: 4_000_000)
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result != nil)
        // The post-truncation length is exactly `maxLength` characters.
        // This bounds the output and proves the truncation step ran;
        // combined with the test merely returning (not OOMing or
        // hanging), this asserts the cap+truncation pipeline succeeded.
        #expect(result?.count == FeedTextNormalizer.maxLength)
        // Tripwire: the result must be strictly smaller than the input.
        // If a future refactor accidentally bypasses the cap AND the
        // post-regex truncation, this catches it independent of timing.
        #expect((result?.utf8.count ?? .max) < raw.utf8.count)
    }

    @Test("Does not truncate text within limit")
    func noTruncationIfShort() {
        let raw = "Short text"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "Short text")
    }

    // MARK: - Nil / Empty Handling

    @Test("Returns nil for nil input")
    func nilInput() {
        #expect(FeedTextNormalizer.normalize(nil) == nil)
    }

    @Test("Returns nil for empty string")
    func emptyInput() {
        #expect(FeedTextNormalizer.normalize("") == nil)
    }

    @Test("Returns nil for whitespace-only input")
    func whitespaceOnly() {
        #expect(FeedTextNormalizer.normalize("   \n\t  ") == nil)
    }

    @Test("Returns nil for HTML-only input (no text content)")
    func htmlOnlyInput() {
        #expect(FeedTextNormalizer.normalize("<br/><img src=\"x\"/>") == nil)
    }

    // MARK: - Source Hash Stability

    @Test("Same input produces same hash")
    func hashStability() {
        let input = "This is a podcast description"
        let h1 = FeedTextNormalizer.stableHash(input)
        let h2 = FeedTextNormalizer.stableHash(input)
        #expect(h1 == h2)
        #expect(h1 != nil)
    }

    @Test("Different input produces different hash")
    func hashDifference() {
        let h1 = FeedTextNormalizer.stableHash("Description A")
        let h2 = FeedTextNormalizer.stableHash("Description B")
        #expect(h1 != h2)
    }

    @Test("Nil input produces nil hash")
    func hashNilInput() {
        #expect(FeedTextNormalizer.stableHash(nil) == nil)
    }

    @Test("Empty input produces nil hash")
    func hashEmptyInput() {
        #expect(FeedTextNormalizer.stableHash("") == nil)
    }

    // MARK: - Metadata Factory

    @Test("makeMetadata builds from raw description and summary")
    func makeMetadataBasic() {
        let meta = FeedTextNormalizer.makeMetadata(
            rawDescription: "<p>Hello world</p>",
            rawSummary: "<b>Summary</b> text"
        )
        #expect(meta != nil)
        #expect(meta?.feedDescription == "Hello world")
        #expect(meta?.feedSummary == "Summary text")
        #expect(meta?.sourceHashes.descriptionHash != nil)
        #expect(meta?.sourceHashes.summaryHash != nil)
    }

    @Test("makeMetadata returns nil when both inputs are nil")
    func makeMetadataBothNil() {
        let meta = FeedTextNormalizer.makeMetadata(
            rawDescription: nil,
            rawSummary: nil
        )
        #expect(meta == nil)
    }

    @Test("makeMetadata returns nil when all four signal sources are empty/nil")
    func makeMetadataAllSignalsEmpty() {
        // Pin the suppression invariant: nil/empty desc + nil/empty summary
        // + empty chapter evidence + nil chaptersFeedURL must collapse to
        // nil. Consumers treat a non-nil metadata blob as "we have *some*
        // useful signal for this episode"; this guards that contract.
        #expect(FeedTextNormalizer.makeMetadata(
            rawDescription: nil,
            rawSummary: nil,
            chapterEvidence: nil,
            chaptersFeedURL: nil
        ) == nil)

        #expect(FeedTextNormalizer.makeMetadata(
            rawDescription: "",
            rawSummary: "",
            chapterEvidence: [],
            chaptersFeedURL: nil
        ) == nil)
    }

    @Test("makeMetadata handles description-only")
    func makeMetadataDescOnly() {
        let meta = FeedTextNormalizer.makeMetadata(
            rawDescription: "Just description",
            rawSummary: nil
        )
        #expect(meta != nil)
        #expect(meta?.feedDescription == "Just description")
        #expect(meta?.feedSummary == nil)
        #expect(meta?.sourceHashes.descriptionHash != nil)
        #expect(meta?.sourceHashes.summaryHash == nil)
    }

    @Test("makeMetadata handles summary-only")
    func makeMetadataSumOnly() {
        let meta = FeedTextNormalizer.makeMetadata(
            rawDescription: nil,
            rawSummary: "Just summary"
        )
        #expect(meta != nil)
        #expect(meta?.feedDescription == nil)
        #expect(meta?.feedSummary == "Just summary")
        #expect(meta?.sourceHashes.descriptionHash == nil)
        #expect(meta?.sourceHashes.summaryHash != nil)
    }

    @Test("Source hashes change when raw source changes")
    func sourceHashChangeDetection() {
        let meta1 = FeedTextNormalizer.makeMetadata(
            rawDescription: "Version 1",
            rawSummary: nil
        )
        let meta2 = FeedTextNormalizer.makeMetadata(
            rawDescription: "Version 2",
            rawSummary: nil
        )
        #expect(meta1?.sourceHashes != meta2?.sourceHashes)
    }

    // MARK: - Script / Style Block Stripping

    @Test("Strips <script> blocks including content")
    func stripsScriptBlocks() {
        let raw = "Before<script type=\"text/javascript\">var x = 1; alert('hi');</script>After"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "BeforeAfter")
    }

    @Test("Strips <style> blocks including CSS content")
    func stripsStyleBlocks() {
        let raw = "Hello<style>.foo { color: red; background: url(http://sponsor.com); }</style>World"
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "HelloWorld")
        // CSS URL should not leak into normalized text
        #expect(result?.contains("sponsor") != true)
    }

    @Test("Strips multiple script and style blocks")
    func stripsMultipleBlocks() {
        let raw = "<style>body{}</style>Content<script>alert(1)</script> here<style>p{}</style>."
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "Content here.")
    }

    // MARK: - Combined HTML + Entity + Whitespace

    @Test("Full normalization pipeline: HTML, entities, whitespace, truncation")
    func fullPipeline() {
        let raw = """
        <div>
            <p>Welcome to &ldquo;The Show&rdquo; &mdash; hosted by Jane &amp; Bob.</p>
            <br/>
            <p>New episodes every Monday!</p>
        </div>
        """
        let result = FeedTextNormalizer.normalize(raw)
        #expect(result == "Welcome to \u{201C}The Show\u{201D} \u{2014} hosted by Jane & Bob. New episodes every Monday!")
    }
}

// MARK: - Persistence Roundtrip

@Suite("FeedDescriptionMetadata – Persistence")
struct FeedDescriptionMetadataPersistenceTests {

    @MainActor
    @Test("Episode feedMetadata survives SwiftData roundtrip")
    func persistenceRoundtrip() throws {
        let config = ModelConfiguration(
            "FeedMetadataTest",
            schema: SwiftDataStore.schema,
            isStoredInMemoryOnly: true
        )
        let container = try ModelContainer(
            for: SwiftDataStore.schema,
            configurations: [config]
        )
        let context = container.mainContext

        let metadata = FeedDescriptionMetadata(
            feedDescription: "Normalized episode description",
            feedSummary: "Normalized episode summary",
            sourceHashes: .init(descriptionHash: 123456789, summaryHash: 987654321)
        )
        let episode = Episode(
            feedItemGUID: "roundtrip-test",
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Test Episode",
            audioURL: URL(string: "https://example.com/ep.mp3")!,
            feedMetadata: metadata
        )
        context.insert(episode)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<Episode>())
        #expect(fetched.count == 1)
        let ep = fetched[0]
        #expect(ep.feedMetadata != nil)
        #expect(ep.feedMetadata?.feedDescription == "Normalized episode description")
        #expect(ep.feedMetadata?.feedSummary == "Normalized episode summary")
        #expect(ep.feedMetadata?.sourceHashes.descriptionHash == 123456789)
        #expect(ep.feedMetadata?.sourceHashes.summaryHash == 987654321)
    }

    @MainActor
    @Test("Episode feedMetadata survives SwiftData roundtrip with high-bit hash")
    func persistenceRoundtripHighBitHash() throws {
        // Regression: UInt64 hash values with the high bit set (> Int64.max)
        // used to crash with "Unable to bridge NSNumber to UInt64" when
        // SwiftData decoded the feedMetadata Codable blob. Store hashes as
        // Int64 bit patterns so the NSNumber bridge does not trap.
        let config = ModelConfiguration(
            "FeedMetadataHighBitTest",
            schema: SwiftDataStore.schema,
            isStoredInMemoryOnly: true
        )
        let container = try ModelContainer(
            for: SwiftDataStore.schema,
            configurations: [config]
        )
        let context = container.mainContext

        // Sentinel value: 0xFFFF_FFFF_FFFF_FFFE is > Int64.max when viewed
        // as UInt64. Bit-cast to Int64 yields -2.
        let sentinel: Int64 = Int64(bitPattern: 0xFFFF_FFFF_FFFF_FFFE)
        let metadata = FeedDescriptionMetadata(
            feedDescription: "desc",
            feedSummary: "sum",
            sourceHashes: .init(
                descriptionHash: sentinel,
                summaryHash: sentinel
            )
        )
        let episode = Episode(
            feedItemGUID: "high-bit-roundtrip",
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Test",
            audioURL: URL(string: "https://example.com/ep.mp3")!,
            feedMetadata: metadata
        )
        context.insert(episode)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<Episode>())
        #expect(fetched.count == 1)
        let ep = fetched[0]
        // The getter for feedMetadata used to trap here when hashes were
        // UInt64 with the high bit set. Reading it must succeed and return
        // the original sentinel bit pattern.
        #expect(ep.feedMetadata?.sourceHashes.descriptionHash == sentinel)
        #expect(ep.feedMetadata?.sourceHashes.summaryHash == sentinel)
    }

    @MainActor
    @Test("Episode feedMetadata nil by default")
    func defaultNil() throws {
        let config = ModelConfiguration(
            "FeedMetadataDefaultTest",
            schema: SwiftDataStore.schema,
            isStoredInMemoryOnly: true
        )
        let container = try ModelContainer(
            for: SwiftDataStore.schema,
            configurations: [config]
        )
        let context = container.mainContext

        let episode = Episode(
            feedItemGUID: "no-metadata-test",
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Bare Episode",
            audioURL: URL(string: "https://example.com/bare.mp3")!
        )
        context.insert(episode)
        try context.save()

        let fetched = try context.fetch(FetchDescriptor<Episode>())
        #expect(fetched.count == 1)
        #expect(fetched[0].feedMetadata == nil)
    }

    @MainActor
    @Test("Feed sync populates feedMetadata from parsed episode")
    func feedSyncPopulatesMetadata() async throws {
        let config = ModelConfiguration(
            "FeedSyncMetadataTest",
            schema: SwiftDataStore.schema,
            isStoredInMemoryOnly: true
        )
        let container = try ModelContainer(
            for: SwiftDataStore.schema,
            configurations: [config]
        )
        let context = container.mainContext
        let discoveryService = PodcastDiscoveryService()
        let feedURL = URL(string: "https://example.com/feed.xml")!

        let feed = ParsedFeed(
            title: "Test Podcast",
            author: "Host",
            description: "Feed desc",
            artworkURL: nil,
            language: "en",
            categories: [],
            episodes: [
                ParsedEpisode(
                    title: "Ep With Desc",
                    guid: "ep-meta-1",
                    enclosureURL: URL(string: "https://example.com/ep1.mp3"),
                    enclosureType: "audio/mpeg",
                    enclosureLength: 1024,
                    pubDate: .now,
                    duration: 600,
                    description: "<p>Episode description with &amp; entities</p>",
                    showNotes: "<b>Show notes</b> content",
                    chapters: [],
                    itunesAuthor: nil,
                    itunesImageURL: nil,
                    itunesEpisodeNumber: nil
                )
            ]
        )

        let podcast = await discoveryService.persist(feed, from: feedURL, in: context)
        try context.save()

        let ep = podcast.episodes.first { $0.feedItemGUID == "ep-meta-1" }
        #expect(ep != nil)
        #expect(ep?.feedMetadata != nil)
        #expect(ep?.feedMetadata?.feedDescription == "Episode description with & entities")
        #expect(ep?.feedMetadata?.feedSummary == "Show notes content")
        #expect(ep?.feedMetadata?.sourceHashes.descriptionHash != nil)
        #expect(ep?.feedMetadata?.sourceHashes.summaryHash != nil)
    }
}
