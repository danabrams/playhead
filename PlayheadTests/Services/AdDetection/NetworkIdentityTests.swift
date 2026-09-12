import Foundation
import Testing

@testable import Playhead

@Suite("NetworkIdentity")
struct NetworkIdentityTests {

    // MARK: - Basic Extraction

    @Test("extracts identity from iTunes author alone")
    func itunesAuthorOnly() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            itunesAuthor: "Gimlet Media"
        ))
        #expect(identity.networkId == "gimlet")
        #expect(identity.networkName == "Gimlet Media")
        #expect(identity.derivedFrom == [.itunesAuthor])
        #expect(identity.confidence == 0.4)
    }

    @Test("extracts identity from publisher alone")
    func publisherOnly() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            publisher: "NPR"
        ))
        #expect(identity.networkId == "npr")
        #expect(identity.derivedFrom == [.publisher])
    }

    @Test("extracts identity from feed URL domain")
    func feedDomainOnly() throws {
        let url = URL(string: "https://feeds.npr.org/podcast.xml")!
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(feedURL: url))
        #expect(identity.networkId == "npr")
        #expect(identity.derivedFrom == [.feedDomain])
    }

    @Test("extracts identity from title prefix with colon")
    func titlePrefixColon() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            title: "NPR: Fresh Air"
        ))
        #expect(identity.networkId == "npr")
        #expect(identity.derivedFrom == [.titlePrefix])
    }

    @Test("extracts identity from title prefix with pipe")
    func titlePrefixPipe() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            title: "Vox | The Weeds"
        ))
        #expect(identity.networkId == "vox")
        #expect(identity.derivedFrom == [.titlePrefix])
    }

    @Test("extracts identity from title prefix with spaced hyphen")
    func titlePrefixHyphen() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            title: "Gimlet - Reply All"
        ))
        #expect(identity.networkId == "gimlet")
        #expect(identity.derivedFrom == [.titlePrefix])
    }

    @Test("extracts identity from managing editor email")
    func managingEditorEmail() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            managingEditor: "editor@npr.org"
        ))
        #expect(identity.networkId == "npr")
        #expect(identity.derivedFrom == [.managingEditor])
    }

    @Test("extracts identity from managing editor with parenthesized name")
    func managingEditorParenthesized() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            managingEditor: "editor@example.com (NPR)"
        ))
        #expect(identity.networkId == "npr")
        #expect(identity.networkName == "NPR")
    }

    @Test("extracts identity from managing editor with angle bracket format")
    func managingEditorAngleBracket() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            managingEditor: "Gimlet Media <podcasts@gimlet.com>"
        ))
        #expect(identity.networkName == "Gimlet Media")
        #expect(identity.networkId == "gimlet")
    }

    // MARK: - Multi-Source Agreement

    @Test("confidence increases with multiple agreeing sources")
    func multiSourceAgreement() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            itunesAuthor: "NPR",
            feedURL: URL(string: "https://feeds.npr.org/show.xml")!,
            publisher: "NPR"
        ))
        #expect(identity.derivedFrom.count == 3)
        #expect(identity.confidence == 0.8)
        #expect(identity.networkId == "npr")
    }

    @Test("two agreeing sources yield 0.6 confidence")
    func twoSourcesAgreeing() throws {
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            itunesAuthor: "Gimlet Media",
            title: "Gimlet: Reply All"
        ))
        #expect(identity.derivedFrom.count == 2)
        #expect(identity.confidence == 0.6)
    }

    @Test("picks the largest agreement cluster when sources disagree")
    func disagreeingSourcesPicksMajority() throws {
        // iTunes author says "NPR", feed domain says "npr", title says "Vox".
        // NPR cluster has 2 sources, Vox has 1 — NPR wins.
        let identity = try #require(NetworkIdentityExtractor.extractIdentity(
            itunesAuthor: "NPR",
            feedURL: URL(string: "https://feeds.npr.org/show.xml")!,
            title: "Vox: Some Show"
        ))
        #expect(identity.networkId == "npr")
        #expect(identity.derivedFrom.contains(.itunesAuthor))
        #expect(identity.derivedFrom.contains(.feedDomain))
    }

    // MARK: - Edge Cases

    @Test("returns nil when no metadata provided")
    func noMetadata() {
        let identity = NetworkIdentityExtractor.extractIdentity()
        #expect(identity == nil)
    }

    @Test("returns nil when all fields are empty strings")
    func emptyStrings() {
        let identity = NetworkIdentityExtractor.extractIdentity(
            itunesAuthor: "",
            managingEditor: "  ",
            publisher: ""
        )
        #expect(identity == nil)
    }

    @Test("skips generic hosting domains")
    func skipHostingDomains() {
        let url = URL(string: "https://feeds.libsyn.com/12345/rss")!
        let identity = NetworkIdentityExtractor.extractIdentity(feedURL: url)
        #expect(identity == nil)
    }

    @Test("skips megaphone hosting domain")
    func skipMegaphone() {
        let url = URL(string: "https://feeds.megaphone.fm/something")!
        let identity = NetworkIdentityExtractor.extractIdentity(feedURL: url)
        #expect(identity == nil)
    }

    @Test("title without separator does not extract prefix")
    func titleWithoutSeparator() {
        let identity = NetworkIdentityExtractor.extractIdentity(
            title: "Just A Regular Podcast Title"
        )
        #expect(identity == nil)
    }

    @Test("single character title prefix is ignored")
    func singleCharPrefix() {
        let identity = NetworkIdentityExtractor.extractIdentity(
            title: "X: Some Show"
        )
        // "X" has count 1 which is < 2, so no prefix extracted.
        #expect(identity == nil)
    }

    @Test("returns nil when metadata normalizes to empty string")
    func allPunctuationMetadata() {
        // Input that passes the non-empty check but normalizes to empty string
        // (all punctuation, no alphanumeric content).
        let identity = NetworkIdentityExtractor.extractIdentity(
            itunesAuthor: "!!!"
        )
        #expect(identity == nil)
    }

    // MARK: - Normalization

    @Test("normalize strips common suffixes")
    func normalizeSuffixes() {
        #expect(NetworkIdentityExtractor.normalize("Gimlet Media") == "gimlet")
        #expect(NetworkIdentityExtractor.normalize("NPR Podcasts") == "npr")
        #expect(NetworkIdentityExtractor.normalize("iHeart Network") == "iheart")
        #expect(NetworkIdentityExtractor.normalize("Wondery Studios") == "wondery")
    }

    @Test("normalize strips multiple chained suffixes")
    func normalizeMultipleSuffixes() {
        #expect(NetworkIdentityExtractor.normalize("Gimlet Podcast Network") == "gimlet")
        #expect(NetworkIdentityExtractor.normalize("Acme Media Podcasts") == "acme")
        #expect(NetworkIdentityExtractor.normalize("Big Studios Entertainment") == "big")
    }

    @Test("normalize lowercases and strips punctuation")
    func normalizeCasing() {
        #expect(NetworkIdentityExtractor.normalize("The New York Times") == "the new york times")
        #expect(NetworkIdentityExtractor.normalize("BBC World Service") == "bbc world service")
    }

    // MARK: - Domain Extraction

    @Test("extractDomainLabel finds meaningful label")
    func domainLabelExtraction() {
        #expect(NetworkIdentityExtractor.extractDomainLabel("feeds.npr.org") == "npr")
        #expect(NetworkIdentityExtractor.extractDomainLabel("podcasts.vox.com") == "vox")
        #expect(NetworkIdentityExtractor.extractDomainLabel("www.bbc.co.uk") == "bbc")
    }

    // MARK: - Sendable Conformance

    @Test("NetworkIdentity is Sendable")
    func sendableConformance() {
        let identity = NetworkIdentity(
            networkId: "test",
            networkName: "Test",
            derivedFrom: [.itunesAuthor],
            confidence: 0.5
        )
        let _: any Sendable = identity
        #expect(identity.networkId == "test")
    }

    @Test("NetworkIdentity survives JSON encode/decode round-trip")
    func codableRoundTrip() throws {
        let identity = NetworkIdentity(
            networkId: "npr",
            networkName: "NPR",
            derivedFrom: [.itunesAuthor, .feedDomain],
            confidence: 0.6
        )
        let data = try JSONEncoder().encode(identity)
        let decoded = try JSONDecoder().decode(NetworkIdentity.self, from: data)
        #expect(decoded == identity)
    }
}
