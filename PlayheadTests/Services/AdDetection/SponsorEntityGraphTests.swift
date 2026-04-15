// SponsorEntityGraphTests.swift
// playhead-ef2.1.1: Tests for eTLD+1 normalization, cross-entity linking,
// path-shape features, co-occurrence alias discovery, and canonical
// sponsor identity resolution.

import XCTest
@testable import Playhead

// MARK: - DomainNormalizer: eTLD+1

final class DomainNormalizerETLD1Tests: XCTestCase {

    func testBareDomain() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "squarespace.com"), "squarespace.com")
    }

    func testStripsWWWSubdomain() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "www.squarespace.com"), "squarespace.com")
    }

    func testStripsDeepSubdomain() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "promo.offers.betterhelp.com"), "betterhelp.com")
    }

    func testMultiPartTLD_coUK() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "www.example.co.uk"), "example.co.uk")
    }

    func testMultiPartTLD_comAU() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "shop.example.com.au"), "example.com.au")
    }

    func testFullURLWithPath() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "https://www.squarespace.com/podcast"), "squarespace.com")
    }

    func testNoScheme() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "squarespace.com/offer/myshow"), "squarespace.com")
    }

    func testLowercasesDomain() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "WWW.Squarespace.COM"), "squarespace.com")
    }

    func testEmptyStringReturnsNil() {
        XCTAssertNil(DomainNormalizer.etld1(from: ""))
    }

    func testSingleLabelReturnsNil() {
        XCTAssertNil(DomainNormalizer.etld1(from: "localhost"))
    }

    func testWithPort() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "https://www.example.com:8080/path"), "example.com")
    }

    func testWithQueryParams() {
        XCTAssertEqual(DomainNormalizer.etld1(from: "betterhelp.com/podcast?utm_source=foo"), "betterhelp.com")
    }

    func testIPAddressReturnsNil() {
        XCTAssertNil(DomainNormalizer.etld1(from: "192.168.1.1"))
    }

    func testIPAddressWithSchemeReturnsNil() {
        XCTAssertNil(DomainNormalizer.etld1(from: "https://10.0.0.1/path"))
    }

    func testBareMultiPartTLD_coUK() {
        // "example.co.uk" has exactly 3 labels — must still resolve correctly.
        XCTAssertEqual(DomainNormalizer.etld1(from: "example.co.uk"), "example.co.uk")
    }

    func testUnicodeDomain() {
        // Punycode / unicode domains should still extract something reasonable.
        let result = DomainNormalizer.etld1(from: "https://www.例え.jp")
        // URLComponents may percent-encode; either way we should get a two-label result or nil, not crash.
        XCTAssertNotNil(result)
    }
}

// MARK: - DomainNormalizer: Tracking Parameter Stripping

final class DomainNormalizerTrackingTests: XCTestCase {

    func testStripsUTMParams() {
        let result = DomainNormalizer.stripTrackingParams(
            from: "squarespace.com/podcast?utm_source=myshow&utm_medium=podcast"
        )
        XCTAssertEqual(result, "squarespace.com/podcast")
    }

    func testPreservesNonTrackingParams() {
        let result = DomainNormalizer.stripTrackingParams(
            from: "example.com/offer?code=SAVE20&utm_source=pod"
        )
        XCTAssertEqual(result, "example.com/offer?code=SAVE20")
    }

    func testNoParams() {
        let result = DomainNormalizer.stripTrackingParams(from: "example.com/podcast")
        XCTAssertEqual(result, "example.com/podcast")
    }

    func testStripsFbclidAndGclid() {
        let result = DomainNormalizer.stripTrackingParams(
            from: "example.com/offer?fbclid=abc123&gclid=xyz789"
        )
        XCTAssertEqual(result, "example.com/offer")
    }

    func testFullHTTPSURL() {
        let result = DomainNormalizer.stripTrackingParams(
            from: "https://example.com/offer?utm_campaign=test&valid=1"
        )
        XCTAssertEqual(result, "https://example.com/offer?valid=1")
    }

    func testStripsAffiliateAndRefParams() {
        let result = DomainNormalizer.stripTrackingParams(
            from: "example.com/deal?affiliate=pod123&ref=myshow"
        )
        XCTAssertEqual(result, "example.com/deal")
    }
}

// MARK: - DomainNormalizer: URL Normalization

final class DomainNormalizerURLNormalizationTests: XCTestCase {

    func testFullNormalization() {
        let result = DomainNormalizer.normalizeURL(
            "WWW.Example.COM/Podcast/?utm_source=test"
        )
        XCTAssertEqual(result, "www.example.com/Podcast")
    }

    func testRemovesDefaultPort443() {
        let result = DomainNormalizer.normalizeURL("https://example.com:443/offer")
        XCTAssertEqual(result, "https://example.com/offer")
    }

    func testRemovesDefaultPort80() {
        let result = DomainNormalizer.normalizeURL("http://example.com:80/offer")
        XCTAssertEqual(result, "http://example.com/offer")
    }

    func testPreservesNonDefaultPort() {
        let result = DomainNormalizer.normalizeURL("https://example.com:8080/offer")
        XCTAssertEqual(result, "https://example.com:8080/offer")
    }
}

// MARK: - DomainNormalizer: Path-Shape Features

final class DomainNormalizerPathShapeTests: XCTestCase {

    func testStaticSegmentPreserved() {
        let shape = DomainNormalizer.pathShape(from: "squarespace.com/podcast")
        XCTAssertEqual(shape, "/podcast")
    }

    func testShowSlugBecomesWildcard() {
        let shape = DomainNormalizer.pathShape(from: "squarespace.com/podcast/my-show-name")
        XCTAssertEqual(shape, "/podcast/*")
    }

    func testMultipleDynamicSegments() {
        let shape = DomainNormalizer.pathShape(from: "example.com/offer/code/ABC123")
        XCTAssertEqual(shape, "/offer/code/*")
    }

    func testRootPath() {
        let shape = DomainNormalizer.pathShape(from: "example.com/")
        XCTAssertEqual(shape, "/")
    }

    func testNoPath() {
        let shape = DomainNormalizer.pathShape(from: "example.com")
        XCTAssertEqual(shape, "/")
    }

    func testDealPath() {
        let shape = DomainNormalizer.pathShape(from: "example.com/deals/holiday-special")
        XCTAssertEqual(shape, "/deals/*")
    }

    func testTryPath() {
        let shape = DomainNormalizer.pathShape(from: "example.com/try/my-product")
        XCTAssertEqual(shape, "/try/*")
    }
}

// MARK: - SponsorEntityGraph: Cross-Entity Linking

final class SponsorEntityGraphLinkingTests: XCTestCase {

    func testDomainLinking() {
        let sponsor = SponsorKnowledgeEntry(
            id: "s1", podcastId: "pod", entityType: .sponsor,
            entityValue: "Squarespace", firstSeenAt: 100
        )
        let url1 = SponsorKnowledgeEntry(
            id: "u1", podcastId: "pod", entityType: .url,
            entityValue: "squarespace.com/podcast", firstSeenAt: 101
        )
        let url2 = SponsorKnowledgeEntry(
            id: "u2", podcastId: "pod", entityType: .url,
            entityValue: "www.squarespace.com/offer", firstSeenAt: 102
        )

        let graph = SponsorEntityGraph(entries: [sponsor, url1, url2])

        let id1 = graph.canonicalSponsorId(forEntryId: "u1")
        let id2 = graph.canonicalSponsorId(forEntryId: "u2")
        XCTAssertNotNil(id1)
        XCTAssertEqual(id1, id2, "URLs sharing eTLD+1 should be in the same identity group")
    }

    func testAliasOverlapLinking() {
        let entry1 = SponsorKnowledgeEntry(
            id: "s1", podcastId: "pod", entityType: .sponsor,
            entityValue: "AG1", firstSeenAt: 100,
            aliases: ["Athletic Greens"]
        )
        let entry2 = SponsorKnowledgeEntry(
            id: "s2", podcastId: "pod", entityType: .sponsor,
            entityValue: "Athletic Greens", firstSeenAt: 200
        )

        let graph = SponsorEntityGraph(entries: [entry1, entry2])

        let id1 = graph.canonicalSponsorId(forEntryId: "s1")
        let id2 = graph.canonicalSponsorId(forEntryId: "s2")
        XCTAssertEqual(id1, id2, "Entries with overlapping aliases should be linked")
    }

    func testUnrelatedEntriesRemainSeparate() {
        let sponsor1 = SponsorKnowledgeEntry(
            id: "s1", podcastId: "pod", entityType: .sponsor,
            entityValue: "Squarespace", firstSeenAt: 100
        )
        let sponsor2 = SponsorKnowledgeEntry(
            id: "s2", podcastId: "pod", entityType: .sponsor,
            entityValue: "BetterHelp", firstSeenAt: 200
        )

        let graph = SponsorEntityGraph(entries: [sponsor1, sponsor2])

        let id1 = graph.canonicalSponsorId(forEntryId: "s1")
        let id2 = graph.canonicalSponsorId(forEntryId: "s2")
        XCTAssertNotEqual(id1, id2, "Unrelated sponsors should have different canonical IDs")
    }

    func testNodeAggregation() {
        let sponsor = SponsorKnowledgeEntry(
            id: "s1", podcastId: "pod", entityType: .sponsor,
            entityValue: "Squarespace", firstSeenAt: 100,
            aliases: ["squarespace"]
        )
        let url = SponsorKnowledgeEntry(
            id: "u1", podcastId: "pod", entityType: .url,
            entityValue: "squarespace.com/podcast", firstSeenAt: 101
        )
        let cta = SponsorKnowledgeEntry(
            id: "c1", podcastId: "pod", entityType: .cta,
            entityValue: "use code PODCAST", firstSeenAt: 102
        )

        let coOccurrences = [
            CoOccurrenceRecord(valueA: "squarespace", valueB: "squarespace.com/podcast", count: 3),
            CoOccurrenceRecord(valueA: "squarespace", valueB: "use code podcast", count: 3),
        ]

        let graph = SponsorEntityGraph(
            entries: [sponsor, url, cta],
            coOccurrences: coOccurrences
        )

        let node = graph.canonicalNode(forEntryId: "s1")
        XCTAssertNotNil(node)
        XCTAssertTrue(node?.names.contains("squarespace") == true)
        XCTAssertTrue(node?.domains.contains("squarespace.com") == true)
        XCTAssertTrue(node?.promoCodes.contains("use code podcast") == true)
        XCTAssertTrue(node?.pathShapes.contains("/podcast") == true)
        XCTAssertEqual(node?.entryIds.count, 3)
    }

    func testCanonicalIdIsEarliestSponsor() {
        let later = SponsorKnowledgeEntry(
            id: "s-later", podcastId: "pod", entityType: .sponsor,
            entityValue: "AG1", firstSeenAt: 200,
            aliases: ["Athletic Greens"]
        )
        let earlier = SponsorKnowledgeEntry(
            id: "s-earlier", podcastId: "pod", entityType: .sponsor,
            entityValue: "Athletic Greens", firstSeenAt: 100
        )

        let graph = SponsorEntityGraph(entries: [later, earlier])

        let canonId = graph.canonicalSponsorId(forEntryId: "s-later")
        XCTAssertEqual(canonId, "s-earlier", "Canonical ID should be from the earliest-seen sponsor")
    }

    func testLookupByName() {
        let entry = SponsorKnowledgeEntry(
            id: "s1", podcastId: "pod", entityType: .sponsor,
            entityValue: "Squarespace", firstSeenAt: 100
        )
        let graph = SponsorEntityGraph(entries: [entry])

        XCTAssertEqual(graph.canonicalSponsorId(forName: "squarespace"), "s1")
        XCTAssertEqual(graph.canonicalSponsorId(forName: "SQUARESPACE"), "s1")
    }

    func testLookupByDomain() {
        let url = SponsorKnowledgeEntry(
            id: "u1", podcastId: "pod", entityType: .url,
            entityValue: "squarespace.com/podcast", firstSeenAt: 100
        )
        let graph = SponsorEntityGraph(entries: [url])

        XCTAssertEqual(graph.canonicalSponsorId(forDomain: "squarespace.com"), "u1")
    }

    func testEmptyGraph() {
        let graph = SponsorEntityGraph(entries: [])
        XCTAssertTrue(graph.nodes.isEmpty)
        XCTAssertNil(graph.canonicalSponsorId(forEntryId: "any"))
    }
}

// MARK: - CoOccurrenceTracker

final class CoOccurrenceTrackerTests: XCTestCase {

    func testBasicRecording() {
        var tracker = CoOccurrenceTracker()
        tracker.record(valueA: "AG1", valueB: "Athletic Greens")
        tracker.record(valueA: "AG1", valueB: "Athletic Greens")
        tracker.record(valueA: "AG1", valueB: "Athletic Greens")

        let records = tracker.records(minCount: 2)
        XCTAssertEqual(records.count, 1)
        XCTAssertEqual(records[0].count, 3)
    }

    func testPairKeyIsOrderIndependent() {
        var tracker = CoOccurrenceTracker()
        tracker.record(valueA: "AG1", valueB: "Athletic Greens")
        tracker.record(valueA: "Athletic Greens", valueB: "AG1")

        let records = tracker.records(minCount: 1)
        XCTAssertEqual(records.count, 1)
        XCTAssertEqual(records[0].count, 2)
    }

    func testSelfCoOccurrenceIsIgnored() {
        var tracker = CoOccurrenceTracker()
        tracker.record(valueA: "AG1", valueB: "AG1")

        let records = tracker.records()
        XCTAssertTrue(records.isEmpty)
    }

    func testMinCountFilter() {
        var tracker = CoOccurrenceTracker()
        tracker.record(valueA: "A", valueB: "B")
        tracker.record(valueA: "C", valueB: "D")
        tracker.record(valueA: "C", valueB: "D")

        let records = tracker.records(minCount: 2)
        XCTAssertEqual(records.count, 1)
        XCTAssertEqual(records[0].count, 2)
    }

    func testIngestEventsOverlappingOrdinals() {
        var tracker = CoOccurrenceTracker()

        let events = [
            KnowledgeCandidateEvent(
                analysisAssetId: "asset-1",
                entityType: .sponsor,
                entityValue: "AG1",
                sourceAtomOrdinals: [10, 11, 12],
                transcriptVersion: "tv-1",
                confidence: 0.9
            ),
            KnowledgeCandidateEvent(
                analysisAssetId: "asset-1",
                entityType: .sponsor,
                entityValue: "Athletic Greens",
                sourceAtomOrdinals: [13, 14, 15],
                transcriptVersion: "tv-1",
                confidence: 0.85
            ),
        ]

        tracker.ingestEvents(events)

        let records = tracker.records()
        XCTAssertEqual(records.count, 1, "Overlapping/adjacent ordinals should create co-occurrence")
    }

    func testIngestEventsIgnoresDifferentAssets() {
        var tracker = CoOccurrenceTracker()

        let events = [
            KnowledgeCandidateEvent(
                analysisAssetId: "asset-1",
                entityType: .sponsor,
                entityValue: "AG1",
                sourceAtomOrdinals: [10, 11],
                transcriptVersion: "tv-1",
                confidence: 0.9
            ),
            KnowledgeCandidateEvent(
                analysisAssetId: "asset-2",
                entityType: .sponsor,
                entityValue: "Athletic Greens",
                sourceAtomOrdinals: [10, 11],
                transcriptVersion: "tv-1",
                confidence: 0.85
            ),
        ]

        tracker.ingestEvents(events)

        let records = tracker.records()
        XCTAssertTrue(records.isEmpty, "Events from different assets should not co-occur")
    }

    func testIngestEventsIgnoresDistantOrdinals() {
        var tracker = CoOccurrenceTracker()

        let events = [
            KnowledgeCandidateEvent(
                analysisAssetId: "asset-1",
                entityType: .sponsor,
                entityValue: "Sponsor A",
                sourceAtomOrdinals: [10, 11],
                transcriptVersion: "tv-1",
                confidence: 0.9
            ),
            KnowledgeCandidateEvent(
                analysisAssetId: "asset-1",
                entityType: .sponsor,
                entityValue: "Sponsor B",
                sourceAtomOrdinals: [100, 101],
                transcriptVersion: "tv-1",
                confidence: 0.85
            ),
        ]

        tracker.ingestEvents(events)

        let records = tracker.records()
        XCTAssertTrue(records.isEmpty, "Distant ordinals should not create co-occurrence")
    }
}

// MARK: - Co-occurrence + Graph Integration

final class SponsorEntityGraphCoOccurrenceTests: XCTestCase {

    func testCoOccurrenceLinking() {
        let entry1 = SponsorKnowledgeEntry(
            id: "s1", podcastId: "pod", entityType: .sponsor,
            entityValue: "AG1", firstSeenAt: 100
        )
        let entry2 = SponsorKnowledgeEntry(
            id: "s2", podcastId: "pod", entityType: .sponsor,
            entityValue: "Athletic Greens", firstSeenAt: 200
        )

        let coOccurrences = [
            CoOccurrenceRecord(valueA: "ag1", valueB: "athletic greens", count: 3)
        ]

        let graph = SponsorEntityGraph(
            entries: [entry1, entry2],
            coOccurrences: coOccurrences,
            coOccurrenceThreshold: 2
        )

        let id1 = graph.canonicalSponsorId(forEntryId: "s1")
        let id2 = graph.canonicalSponsorId(forEntryId: "s2")
        XCTAssertEqual(id1, id2, "Co-occurring entries should be linked")
    }

    func testBelowThresholdDoesNotLink() {
        let entry1 = SponsorKnowledgeEntry(
            id: "s1", podcastId: "pod", entityType: .sponsor,
            entityValue: "AG1", firstSeenAt: 100
        )
        let entry2 = SponsorKnowledgeEntry(
            id: "s2", podcastId: "pod", entityType: .sponsor,
            entityValue: "Athletic Greens", firstSeenAt: 200
        )

        let coOccurrences = [
            CoOccurrenceRecord(valueA: "ag1", valueB: "athletic greens", count: 1)
        ]

        let graph = SponsorEntityGraph(
            entries: [entry1, entry2],
            coOccurrences: coOccurrences,
            coOccurrenceThreshold: 2
        )

        let id1 = graph.canonicalSponsorId(forEntryId: "s1")
        let id2 = graph.canonicalSponsorId(forEntryId: "s2")
        XCTAssertNotEqual(id1, id2, "Below-threshold co-occurrence should not link entries")
    }
}
