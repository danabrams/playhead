// MetadataCueExtractorTests.swift
// ef2.2.2: Tests for MetadataCueExtractor — deterministic metadata cue
// extraction from episode RSS description/summary text.

import Foundation
import Testing
@testable import Playhead

// MARK: - HTML Normalization

@Suite("MetadataCueExtractor — HTML Normalization")
struct HTMLNormalizationTests {

    @Test("Strips HTML tags")
    func stripsHTMLTags() {
        let input = "<p>Hello <b>world</b></p>"
        let result = MetadataCueExtractor.normalizeText(input)
        #expect(result == "Hello world")
    }

    @Test("Decodes common HTML entities")
    func decodesHTMLEntities() {
        let input = "Ben &amp; Jerry&apos;s &mdash; ice cream"
        let result = MetadataCueExtractor.normalizeText(input)
        #expect(result.contains("Ben & Jerry's"))
        #expect(result.contains("\u{2014}"))
    }

    @Test("Decodes numeric entities")
    func decodesNumericEntities() {
        let input = "&#169; 2026 Podcast Corp"
        let result = MetadataCueExtractor.normalizeText(input)
        #expect(result.contains("\u{00A9}"))
    }

    @Test("Decodes hex entities")
    func decodesHexEntities() {
        let input = "Hello&#x21; World"
        let result = MetadataCueExtractor.normalizeText(input)
        #expect(result.contains("Hello!"))
    }

    @Test("Collapses whitespace")
    func collapsesWhitespace() {
        let input = "hello    world\n\n  foo"
        let result = MetadataCueExtractor.normalizeText(input)
        #expect(result == "hello world foo")
    }

    @Test("Handles complex podcast HTML")
    func handlesComplexPodcastHTML() {
        let input = """
        <p>This episode is sponsored by <a href="https://squarespace.com/conan">Squarespace</a>.</p>
        <p>Use code <b>CONAN</b> for 10% off.</p>
        """
        let result = MetadataCueExtractor.normalizeText(input)
        #expect(result.contains("sponsored by"))
        #expect(result.contains("Squarespace"))
        #expect(result.contains("code"))
        #expect(result.contains("CONAN"))
    }

    @Test("Empty string returns empty")
    func emptyString() {
        let result = MetadataCueExtractor.normalizeText("")
        #expect(result.isEmpty)
    }
}

// MARK: - URL Extraction

@Suite("MetadataCueExtractor — URL Extraction")
struct URLExtractionTests {

    @Test("Extracts HTTP URLs")
    func extractsHTTPURLs() {
        let text = "Visit https://squarespace.com/conan for more info"
        let urls = MetadataCueExtractor.extractURLs(from: text)
        #expect(urls.count == 1)
        #expect(urls[0].domain == "squarespace.com")
    }

    @Test("Extracts bare domain URLs")
    func extractsBareDomains() {
        let text = "Go to betterhelp.com/conan to get started"
        let urls = MetadataCueExtractor.extractURLs(from: text)
        #expect(urls.count == 1)
        #expect(urls[0].domain == "betterhelp.com")
    }

    @Test("Strips www prefix")
    func stripsWWWPrefix() {
        let text = "Visit www.example.com/podcast"
        let urls = MetadataCueExtractor.extractURLs(from: text)
        #expect(urls.count == 1)
        #expect(urls[0].domain == "example.com")
    }

    @Test("Handles multiple URLs")
    func handlesMultipleURLs() {
        let text = "Check out squarespace.com and betterhelp.com/podcast"
        let urls = MetadataCueExtractor.extractURLs(from: text)
        #expect(urls.count == 2)
        let domains = Set(urls.map(\.domain))
        #expect(domains.contains("squarespace.com"))
        #expect(domains.contains("betterhelp.com"))
    }

    @Test("No URLs in plain text")
    func noURLsInPlainText() {
        let text = "This is a plain text podcast description with no links"
        let urls = MetadataCueExtractor.extractURLs(from: text)
        #expect(urls.isEmpty)
    }

    @Test("Handles various TLDs")
    func handlesVariousTLDs() {
        let text = "Visit podcast.fm and myapp.io and show.tv"
        let urls = MetadataCueExtractor.extractURLs(from: text)
        #expect(urls.count == 3)
        let domains = Set(urls.map(\.domain))
        #expect(domains.contains("podcast.fm"))
        #expect(domains.contains("myapp.io"))
        #expect(domains.contains("show.tv"))
    }
}

// MARK: - Domain Normalization

@Suite("MetadataCueExtractor — Domain Normalization")
struct DomainNormalizationTests {

    @Test("Normalizes to eTLD+1")
    func normalizesToETLD1() {
        let domain = MetadataCueExtractor.normalizeDomain(from: "https://www.subdomain.example.com/path")
        #expect(domain == "example.com")
    }

    @Test("Handles two-part TLDs")
    func handlesTwoPartTLDs() {
        let domain = MetadataCueExtractor.normalizeDomain(from: "https://example.co.uk/path")
        #expect(domain == "example.co.uk")
    }

    @Test("Strips www prefix")
    func stripsWWW() {
        let domain = MetadataCueExtractor.normalizeDomain(from: "www.example.com")
        #expect(domain == "example.com")
    }

    @Test("Lowercases domain")
    func lowercasesDomain() {
        let domain = MetadataCueExtractor.normalizeDomain(from: "EXAMPLE.COM")
        #expect(domain == "example.com")
    }

    @Test("Returns nil for invalid input")
    func returnsNilForInvalid() {
        let domain = MetadataCueExtractor.normalizeDomain(from: "not a url")
        #expect(domain == nil)
    }
}

// MARK: - Tracking Param Stripping

@Suite("MetadataCueExtractor — Tracking Params")
struct TrackingParamTests {

    @Test("Strips UTM parameters")
    func stripsUTMParams() {
        let url = "https://example.com/page?utm_source=podcast&utm_medium=audio&real_param=value"
        let result = MetadataCueExtractor.stripTrackingParams(from: url)
        #expect(!result.contains("utm_source"))
        #expect(!result.contains("utm_medium"))
        #expect(result.contains("real_param=value"))
    }

    @Test("Strips affiliate/click params")
    func stripsAffiliateParams() {
        let url = "https://example.com/page?ref=podcast123&fbclid=abc&gclid=def"
        let result = MetadataCueExtractor.stripTrackingParams(from: url)
        #expect(!result.contains("ref="))
        #expect(!result.contains("fbclid"))
        #expect(!result.contains("gclid"))
    }

    @Test("Preserves non-tracking params")
    func preservesNonTrackingParams() {
        let url = "https://example.com/page?product=widget&size=large"
        let result = MetadataCueExtractor.stripTrackingParams(from: url)
        #expect(result.contains("product=widget"))
        #expect(result.contains("size=large"))
    }

    @Test("Handles URL with no params")
    func handlesNoParams() {
        let url = "https://example.com/page"
        let result = MetadataCueExtractor.stripTrackingParams(from: url)
        #expect(result.contains("example.com/page"))
    }
}

// MARK: - Disclosure Extraction

@Suite("MetadataCueExtractor — Disclosure Cues")
struct DisclosureExtractionTests {

    @Test("Detects 'sponsored by' with sponsor name")
    func detectsSponsoredBy() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "This episode is sponsored by Squarespace. Build your website today.",
            summary: nil
        )
        let disclosures = cues.filter { $0.cueType == .disclosure }
        #expect(!disclosures.isEmpty)
        #expect(disclosures.contains { $0.normalizedValue.contains("squarespace") })
        #expect(disclosures[0].confidence >= 0.90)
    }

    @Test("Detects 'brought to you by'")
    func detectsBroughtToYouBy() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "This podcast is brought to you by BetterHelp online therapy.",
            summary: nil
        )
        let disclosures = cues.filter { $0.cueType == .disclosure }
        #expect(!disclosures.isEmpty)
        #expect(disclosures.contains { $0.normalizedValue.contains("betterhelp") })
    }

    @Test("Detects 'thanks to our sponsors'")
    func detectsThanksToSponsors() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Thanks to our sponsor, ZipRecruiter, for supporting the show.",
            summary: nil
        )
        let disclosures = cues.filter { $0.cueType == .disclosure }
        #expect(!disclosures.isEmpty)
    }

    @Test("Detects 'in partnership with'")
    func detectsPartnership() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "This series was produced in partnership with Audible Studios.",
            summary: nil
        )
        let disclosures = cues.filter { $0.cueType == .disclosure }
        #expect(!disclosures.isEmpty)
    }

    @Test("Low confidence for bare 'ad' keyword")
    func lowConfidenceForBareAd() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Contains an ad in the middle of the episode.",
            summary: nil
        )
        let disclosures = cues.filter { $0.cueType == .disclosure }
        #expect(!disclosures.isEmpty)
        #expect(disclosures[0].confidence <= 0.35)
    }

    @Test("Disclosure from HTML-wrapped text")
    func disclosureFromHTML() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "<p>This episode is <em>sponsored by</em> <a href='https://example.com'>Acme Corp</a>.</p>",
            summary: nil
        )
        let disclosures = cues.filter { $0.cueType == .disclosure }
        #expect(!disclosures.isEmpty)
    }
}

// MARK: - Promo Code Extraction

@Suite("MetadataCueExtractor — Promo Code Cues")
struct PromoCodeExtractionTests {

    @Test("Detects 'use code X'")
    func detectsUseCode() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Use code CONAN for 15% off your first order.",
            summary: nil
        )
        let codes = cues.filter { $0.cueType == .promoCode }
        #expect(codes.count == 1)
        #expect(codes[0].normalizedValue == "CONAN")
    }

    @Test("Detects 'promo code X'")
    func detectsPromoCode() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Enter promo code SAVE20 at checkout.",
            summary: nil
        )
        let codes = cues.filter { $0.cueType == .promoCode }
        #expect(codes.count == 1)
        #expect(codes[0].normalizedValue == "SAVE20")
    }

    @Test("Detects 'discount code'")
    func detectsDiscountCode() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Use the discount code PODCAST for free shipping.",
            summary: nil
        )
        let codes = cues.filter { $0.cueType == .promoCode }
        #expect(codes.count == 1)
        #expect(codes[0].normalizedValue == "PODCAST")
    }

    @Test("Detects 'code X at checkout'")
    func detectsCodeAtCheckout() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Just enter code DEAL10 at checkout to save 10%.",
            summary: nil
        )
        let codes = cues.filter { $0.cueType == .promoCode }
        #expect(!codes.isEmpty)
        #expect(codes[0].normalizedValue == "DEAL10")
    }

    @Test("Uppercases promo codes")
    func uppercasesPromoCodes() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Use code mycode for a discount.",
            summary: nil
        )
        let codes = cues.filter { $0.cueType == .promoCode }
        #expect(codes.count == 1)
        #expect(codes[0].normalizedValue == "MYCODE")
    }

    @Test("Filters false positive codes")
    func filtersFalsePositives() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Use code the for savings. Apply code your at checkout.",
            summary: nil
        )
        let codes = cues.filter { $0.cueType == .promoCode }
        #expect(codes.isEmpty)
    }
}

// MARK: - Domain Classification

@Suite("MetadataCueExtractor — Domain Classification")
struct DomainClassificationTests {

    @Test("Classifies external domains")
    func classifiesExternalDomains() {
        let extractor = MetadataCueExtractor(
            showOwnedDomains: ["teamcoco.com"],
            networkOwnedDomains: ["earwolf.com"]
        )
        let cues = extractor.extractCues(
            description: "Visit squarespace.com/conan for a free trial.",
            summary: nil
        )
        let domains = cues.filter { $0.cueType == .externalDomain }
        #expect(domains.count == 1)
        #expect(domains[0].normalizedValue == "squarespace.com")
        #expect(domains[0].confidence == 0.80)
    }

    @Test("Classifies show-owned domains")
    func classifiesShowOwnedDomains() {
        let extractor = MetadataCueExtractor(
            showOwnedDomains: ["teamcoco.com"],
            networkOwnedDomains: ["earwolf.com"]
        )
        let cues = extractor.extractCues(
            description: "Visit teamcoco.com for full episodes.",
            summary: nil
        )
        let showDomains = cues.filter { $0.cueType == .showOwnedDomain }
        #expect(showDomains.count == 1)
        #expect(showDomains[0].normalizedValue == "teamcoco.com")
        #expect(showDomains[0].confidence == 0.95)
    }

    @Test("Classifies network-owned domains")
    func classifiesNetworkOwnedDomains() {
        let extractor = MetadataCueExtractor(
            showOwnedDomains: ["teamcoco.com"],
            networkOwnedDomains: ["earwolf.com"]
        )
        let cues = extractor.extractCues(
            description: "More podcasts at earwolf.com",
            summary: nil
        )
        let networkDomains = cues.filter { $0.cueType == .networkOwnedDomain }
        #expect(networkDomains.count == 1)
        #expect(networkDomains[0].normalizedValue == "earwolf.com")
    }

    @Test("Deduplicates domains within same source")
    func deduplicatesDomains() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Visit squarespace.com/conan and squarespace.com/offer today.",
            summary: nil
        )
        let domains = cues.filter { $0.cueType == .externalDomain }
        #expect(domains.count == 1)
    }
}

// MARK: - Sponsor Alias Detection

@Suite("MetadataCueExtractor — Sponsor Alias Cues")
struct SponsorAliasTests {

    @Test("Detects known sponsor names")
    func detectsKnownSponsors() {
        let extractor = MetadataCueExtractor(
            knownSponsors: ["Squarespace", "BetterHelp"]
        )
        let cues = extractor.extractCues(
            description: "Today we talk about building websites. Squarespace makes it easy.",
            summary: nil
        )
        let aliases = cues.filter { $0.cueType == .sponsorAlias }
        #expect(aliases.count == 1)
        #expect(aliases[0].normalizedValue == "squarespace")
    }

    @Test("Case-insensitive matching")
    func caseInsensitiveMatching() {
        let extractor = MetadataCueExtractor(
            knownSponsors: ["BetterHelp"]
        )
        let cues = extractor.extractCues(
            description: "Get therapy from betterhelp online.",
            summary: nil
        )
        let aliases = cues.filter { $0.cueType == .sponsorAlias }
        #expect(aliases.count == 1)
    }

    @Test("Word boundary matching prevents partial matches")
    func wordBoundaryMatching() {
        let extractor = MetadataCueExtractor(
            knownSponsors: ["Help"]
        )
        let cues = extractor.extractCues(
            description: "BetterHelp is great for therapy.",
            summary: nil
        )
        // "Help" as a word boundary match should NOT match inside "BetterHelp"
        let aliases = cues.filter { $0.cueType == .sponsorAlias }
        #expect(aliases.isEmpty)
    }

    @Test("No false positives without known sponsors")
    func noFalsePositivesWithoutSponsors() {
        let extractor = MetadataCueExtractor(knownSponsors: [])
        let cues = extractor.extractCues(
            description: "This episode is sponsored by Squarespace.",
            summary: nil
        )
        let aliases = cues.filter { $0.cueType == .sponsorAlias }
        #expect(aliases.isEmpty)
    }
}

// MARK: - Source Field Tracking

@Suite("MetadataCueExtractor — Source Field")
struct SourceFieldTests {

    @Test("Cues from description tagged as .description")
    func descriptionSourceField() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Sponsored by Acme Corp.",
            summary: nil
        )
        #expect(cues.allSatisfy { $0.sourceField == .description })
    }

    @Test("Cues from summary tagged as .summary")
    func summarySourceField() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: nil,
            summary: "Sponsored by Acme Corp."
        )
        #expect(cues.allSatisfy { $0.sourceField == .summary })
    }

    @Test("Cues from both sources have correct tags")
    func bothSourceFields() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "Sponsored by Acme Corp.",
            summary: "Brought to you by Widget Co."
        )
        let descCues = cues.filter { $0.sourceField == .description }
        let sumCues = cues.filter { $0.sourceField == .summary }
        #expect(!descCues.isEmpty)
        #expect(!sumCues.isEmpty)
    }
}

// MARK: - Edge Cases

@Suite("MetadataCueExtractor — Edge Cases")
struct EdgeCaseTests {

    @Test("Empty text produces no cues")
    func emptyText() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(description: "", summary: "")
        #expect(cues.isEmpty)
    }

    @Test("Nil text produces no cues")
    func nilText() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(description: nil, summary: nil)
        #expect(cues.isEmpty)
    }

    @Test("Text with no cues produces empty array")
    func noCues() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: "This is a regular podcast episode about cooking pasta.",
            summary: nil
        )
        #expect(cues.isEmpty)
    }

    @Test("Deduplicates same cue from description and summary")
    func deduplicatesCrossSource() {
        let extractor = MetadataCueExtractor()
        let sponsorText = "This episode is sponsored by Acme Corp."
        let cues = extractor.extractCues(
            description: sponsorText,
            summary: sponsorText
        )
        // Same disclosure appears in both sources — dedup keeps both because
        // source field differs (description vs summary).
        let disclosures = cues.filter { $0.cueType == .disclosure }
        #expect(disclosures.count == 2)
    }

    @Test("Multiple cue types from single description")
    func multipleCueTypes() {
        let extractor = MetadataCueExtractor(
            knownSponsors: ["Squarespace"]
        )
        let cues = extractor.extractCues(
            description: """
            This episode is sponsored by Squarespace. Visit squarespace.com/conan
            and use code CONAN for 10% off your first purchase.
            """,
            summary: nil
        )
        let types = Set(cues.map(\.cueType))
        #expect(types.contains(.disclosure))
        #expect(types.contains(.externalDomain))
        #expect(types.contains(.promoCode))
        #expect(types.contains(.sponsorAlias))
    }

    @Test("Deterministic output ordering")
    func deterministicOrdering() {
        let extractor = MetadataCueExtractor()
        let text = "Sponsored by Acme. Visit example.com. Use code SAVE10."

        let cues1 = extractor.extractCues(description: text, summary: nil)
        let cues2 = extractor.extractCues(description: text, summary: nil)

        #expect(cues1.count == cues2.count)
        for (a, b) in zip(cues1, cues2) {
            #expect(a == b)
        }
    }

    @Test("Confidence values are within valid range")
    func confidenceRange() {
        let extractor = MetadataCueExtractor(knownSponsors: ["Squarespace"])
        let cues = extractor.extractCues(
            description: """
            Sponsored by Squarespace. Visit squarespace.com/podcast.
            Use promo code PODCAST for a discount. Brought to you by our partners.
            """,
            summary: nil
        )
        for cue in cues {
            #expect(cue.confidence >= 0.0 && cue.confidence <= 1.0,
                    "Confidence \(cue.confidence) out of range for \(cue.cueType)")
        }
    }
}

// MARK: - Real-world Podcast Descriptions

@Suite("MetadataCueExtractor — Real-world Descriptions")
struct RealWorldTests {

    @Test("Typical podcast ad description")
    func typicalAdDescription() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: """
            <p>Conan and the gang discuss their favorite holiday traditions.</p>
            <p>This episode is brought to you by BetterHelp. Visit
            <a href="https://betterhelp.com/conan?utm_source=podcast&utm_medium=audio">
            betterhelp.com/conan</a> and get 10% off your first month.
            Use code CONAN at checkout.</p>
            <p>Also sponsored by Squarespace. Go to squarespace.com/conan for a free trial.</p>
            """,
            summary: nil
        )
        // Should find: 2 disclosures, 2 external domains, 1 promo code
        let disclosures = cues.filter { $0.cueType == .disclosure }
        let domains = cues.filter { $0.cueType == .externalDomain }
        let codes = cues.filter { $0.cueType == .promoCode }

        #expect(disclosures.count >= 2)
        #expect(domains.count >= 2)
        #expect(codes.count >= 1)
    }

    @Test("Description with no ads")
    func descriptionWithNoAds() {
        let extractor = MetadataCueExtractor()
        let cues = extractor.extractCues(
            description: """
            <p>In this episode, we explore the history of jazz music from
            New Orleans to Chicago. Featuring interviews with renowned musicians
            and rare archival recordings.</p>
            """,
            summary: nil
        )
        #expect(cues.isEmpty)
    }

    @Test("Description with only show links")
    func descriptionWithOnlyShowLinks() {
        let extractor = MetadataCueExtractor(
            showOwnedDomains: ["mypodcast.com"]
        )
        let cues = extractor.extractCues(
            description: """
            Visit mypodcast.com for show notes and transcripts.
            Follow us on social media.
            """,
            summary: nil
        )
        let showDomains = cues.filter { $0.cueType == .showOwnedDomain }
        let external = cues.filter { $0.cueType == .externalDomain }
        #expect(showDomains.count == 1)
        #expect(external.isEmpty)
    }
}
