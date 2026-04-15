// OwnershipGraphTests.swift
// Tests for OwnershipGraph domain-level ownership resolution (ef2.1.2).

import XCTest
@testable import Playhead

// MARK: - DomainOwnershipLabel -> AdOwnership

final class DomainOwnershipLabelMappingTests: XCTestCase {

    func testShowOwnedMapsToShow() {
        XCTAssertEqual(DomainOwnershipLabel.showOwned.toAdOwnership, .show)
    }

    func testSponsorOwnedMapsToThirdParty() {
        XCTAssertEqual(DomainOwnershipLabel.sponsorOwned.toAdOwnership, .thirdParty)
    }

    func testNetworkOwnedMapsToNetwork() {
        XCTAssertEqual(DomainOwnershipLabel.networkOwned.toAdOwnership, .network)
    }

    func testUnknownMapsToUnknown() {
        XCTAssertEqual(DomainOwnershipLabel.unknown.toAdOwnership, .unknown)
    }

    func testAllCasesMapped() {
        // Every DomainOwnershipLabel must map to a valid AdOwnership
        for label in DomainOwnershipLabel.allCases {
            let ownership = label.toAdOwnership
            XCTAssertTrue(AdOwnership.allCases.contains(ownership),
                          "\(label) maps to invalid AdOwnership: \(ownership)")
        }
    }
}

// MARK: - OwnershipGraph RSS Ingest

final class OwnershipGraphRSSTests: XCTestCase {

    func testIngestRSSLinkMarksShowOwned() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSLink("https://www.myshow.com")

        XCTAssertEqual(graph.ownership(for: "myshow.com"), .show)
    }

    func testIngestFeedURLMarksShowOwned() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestFeedURL("https://feeds.megaphone.fm/myshow")

        // eTLD+1 of feeds.megaphone.fm is megaphone.fm
        XCTAssertEqual(graph.ownership(for: "megaphone.fm"), .show)
    }

    func testIngestITunesOwnerEmail() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestITunesOwner(email: "host@myshow.com")

        XCTAssertEqual(graph.ownership(for: "myshow.com"), .show)
    }

    func testBulkRSSIngest() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSFeed(
            feedURL: "https://feeds.simplecast.com/abc",
            linkURL: "https://www.myshow.com",
            itunesOwnerEmail: "host@myshow.com"
        )

        // eTLD+1 of feeds.simplecast.com is simplecast.com
        XCTAssertEqual(graph.ownership(for: "simplecast.com"), .show)
        XCTAssertEqual(graph.ownership(for: "myshow.com"), .show)
    }

    func testNilRSSFieldsSkipped() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSFeed(feedURL: nil, linkURL: nil, itunesOwnerEmail: nil)
        XCTAssertTrue(graph.entries.isEmpty)
    }

    func testInvalidRSSLinkIgnored() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSLink("")
        XCTAssertTrue(graph.entries.isEmpty)
    }

    func testInvalidEmailIgnored() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestITunesOwner(email: "noatsign")
        XCTAssertTrue(graph.entries.isEmpty)
    }
}

// MARK: - OwnershipGraph Show Notes Frequency

final class OwnershipGraphShowNotesTests: XCTestCase {

    func testSingleAppearanceStaysUnknown() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.recordShowNotesDomain("https://somelink.com")
        // Default threshold is 3, so 1 appearance = unknown
        XCTAssertEqual(graph.ownership(for: "somelink.com"), .unknown)
    }

    func testFrequencyThresholdPromotesToShowOwned() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.recordShowNotesDomain("https://myshow.com")
        graph.recordShowNotesDomain("https://myshow.com")
        graph.recordShowNotesDomain("https://myshow.com")

        XCTAssertEqual(graph.ownership(for: "myshow.com"), .show)
    }

    func testFrequencyBelowThresholdStaysUnknown() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.recordShowNotesDomain("https://rare.com")
        graph.recordShowNotesDomain("https://rare.com")
        // 2 < default threshold of 3
        XCTAssertEqual(graph.ownership(for: "rare.com"), .unknown)
    }

    func testFrequencyDoesNotOverrideRSSSignal() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSLink("https://myshow.com")
        graph.recordShowNotesDomain("https://myshow.com")

        // Should still be showOwned from RSS, frequency just bumps count
        let entry = graph.entries["myshow.com"]
        XCTAssertEqual(entry?.source, .rssLink)
        XCTAssertEqual(entry?.frequency, 1)
        XCTAssertEqual(entry?.label, .showOwned)
    }

    func testBatchShowNotesDeduplicatesPerEpisode() {
        var graph = OwnershipGraph(podcastId: "pod1")
        // Same domain appears 3 times in one episode's show notes
        graph.ingestShowNotesDomains([
            "https://sponsor.com/ep1",
            "https://sponsor.com/ep1?utm=x",
            "https://sponsor.com"
        ])
        // Should count as 1 appearance, not 3 (all normalize to sponsor.com)
        let entry = graph.entries["sponsor.com"]
        XCTAssertEqual(entry?.frequency, 1)
    }

    func testCustomThreshold() {
        let config = OwnershipGraphConfig(
            showOwnedFrequencyThreshold: 1
        )
        var graph = OwnershipGraph(podcastId: "pod1", config: config)
        graph.recordShowNotesDomain("https://instant.com")
        XCTAssertEqual(graph.ownership(for: "instant.com"), .show)
    }
}

// MARK: - OwnershipGraph Sponsor Integration

final class OwnershipGraphSponsorTests: XCTestCase {

    func testRegisterSponsorDomain() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.registerSponsorDomain("https://betterhelp.com/podcast")

        XCTAssertEqual(graph.ownership(for: "betterhelp.com"), .thirdParty)
    }

    func testSponsorDomainWithCanonicalId() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.registerSponsorDomain(
            "https://betterhelp.com/podcast",
            canonicalSponsorId: "entry-42"
        )

        XCTAssertEqual(graph.ownership(for: "betterhelp.com"), .thirdParty)
        XCTAssertEqual(graph.sponsorId(for: "betterhelp.com"), "entry-42")
    }

    func testRegisterNetworkDomain() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.registerNetworkDomain("https://wondery.com")

        XCTAssertEqual(graph.ownership(for: "wondery.com"), .network)
    }

    func testSponsorOwnedDomainsList() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.registerSponsorDomain("https://sponsor1.com")
        graph.registerSponsorDomain("https://sponsor2.com")
        graph.ingestRSSLink("https://myshow.com")

        XCTAssertEqual(Set(graph.sponsorOwnedDomains), Set(["sponsor1.com", "sponsor2.com"]))
    }

    func testShowOwnedDomainsList() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSLink("https://myshow.com")
        graph.ingestFeedURL("https://feeds.example.com/myshow")

        XCTAssertEqual(Set(graph.showOwnedDomains), Set(["myshow.com", "example.com"]))
    }

    func testSponsorDoesNotOverrideUserOverride() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.applyUserOverride("https://example.com", label: .showOwned)
        graph.registerSponsorDomain("https://example.com", canonicalSponsorId: "sp1")

        XCTAssertEqual(graph.ownership(for: "example.com"), .show)
        let entry = graph.entries["example.com"]
        XCTAssertEqual(entry?.source, .userOverride)
    }
}

// MARK: - OwnershipGraph User Override

final class OwnershipGraphUserOverrideTests: XCTestCase {

    func testUserOverrideSetsDomainLabel() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.applyUserOverride("https://example.com", label: .showOwned)

        XCTAssertEqual(graph.ownership(for: "example.com"), .show)
    }

    func testUserOverridePreventsAutomaticOverwrite() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.applyUserOverride("https://example.com", label: .showOwned)

        // Sponsor registration should not override user override
        graph.registerSponsorDomain("https://example.com")

        XCTAssertEqual(graph.ownership(for: "example.com"), .show)
        let entry = graph.entries["example.com"]
        XCTAssertEqual(entry?.source, .userOverride)
    }

    func testUserOverrideOverridesRSS() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSLink("https://example.com")
        XCTAssertEqual(graph.ownership(for: "example.com"), .show)

        // User says this is actually a sponsor
        graph.applyUserOverride("https://example.com", label: .sponsorOwned)
        XCTAssertEqual(graph.ownership(for: "example.com"), .thirdParty)
    }

    func testUserOverrideOverridesNetwork() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.registerNetworkDomain("https://wondery.com")
        XCTAssertEqual(graph.ownership(for: "wondery.com"), .network)

        graph.applyUserOverride("https://wondery.com", label: .showOwned)
        XCTAssertEqual(graph.ownership(for: "wondery.com"), .show)
    }
}

// MARK: - OwnershipGraph Domain Lookup

final class OwnershipGraphDomainLookupTests: XCTestCase {

    func testSubdomainResolvesToSameETLD1() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.registerSponsorDomain("https://betterhelp.com")

        // podcast.betterhelp.com has eTLD+1 = betterhelp.com
        XCTAssertEqual(graph.ownership(for: "podcast.betterhelp.com"), .thirdParty)
    }

    func testUnknownDomainReturnsNil() {
        let graph = OwnershipGraph(podcastId: "pod1")
        XCTAssertNil(graph.ownership(for: "nowhere.com"))
    }

    func testInvalidDomainReturnsNil() {
        let graph = OwnershipGraph(podcastId: "pod1")
        XCTAssertNil(graph.ownership(for: ""))
    }

    func testURLWithTrackingParams() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.registerSponsorDomain("https://betterhelp.com")

        // eTLD+1 extraction ignores path/query
        XCTAssertEqual(
            graph.ownership(for: "https://betterhelp.com/podcast?utm_source=foo"),
            .thirdParty
        )
    }

    func testCompoundTLD() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSLink("https://www.bbc.co.uk")

        XCTAssertEqual(graph.ownership(for: "bbc.co.uk"), .show)
    }
}

// MARK: - OwnershipGraph End-to-End

final class OwnershipGraphEndToEndTests: XCTestCase {

    func testRealWorldShowSetup() {
        var graph = OwnershipGraph(podcastId: "conan")

        // RSS signals
        graph.ingestRSSFeed(
            feedURL: "https://feeds.simplecast.com/dHoohVNH",
            linkURL: "https://www.teamcoco.com",
            itunesOwnerEmail: "podcasts@teamcoco.com"
        )

        // Show notes domains across episodes
        for _ in 0..<5 {
            graph.ingestShowNotesDomains([
                "https://teamcoco.com/podcast",
                "https://www.teamcoco.com/tickets"
            ])
        }

        // Sponsor domains from knowledge store
        graph.registerSponsorDomain(
            "https://betterhelp.com/conan",
            canonicalSponsorId: "entry-betterhelp"
        )
        graph.registerSponsorDomain(
            "https://squarespace.com",
            canonicalSponsorId: "entry-squarespace"
        )

        // Verify classifications
        XCTAssertEqual(graph.ownership(for: "teamcoco.com"), .show)
        XCTAssertEqual(graph.ownership(for: "simplecast.com"), .show)
        XCTAssertEqual(graph.ownership(for: "betterhelp.com"), .thirdParty)
        XCTAssertEqual(graph.ownership(for: "squarespace.com"), .thirdParty)
        XCTAssertEqual(graph.sponsorId(for: "betterhelp.com"), "entry-betterhelp")
    }

    func testConfigDefaultValues() {
        let config = OwnershipGraphConfig.default
        XCTAssertEqual(config.showOwnedFrequencyThreshold, 3)
    }

    func testConfigDefaultHasNoUbiquitousPresenceRatio() {
        // Verify OwnershipGraphConfig.default only has showOwnedFrequencyThreshold.
        // The old ubiquitousPresenceRatio field was removed; this test ensures
        // the struct contains only the expected property.
        let config = OwnershipGraphConfig.default
        let mirror = Mirror(reflecting: config)
        let propertyNames = mirror.children.compactMap(\.label)
        XCTAssertTrue(propertyNames.contains("showOwnedFrequencyThreshold"))
        XCTAssertFalse(propertyNames.contains("ubiquitousPresenceRatio"),
                       "ubiquitousPresenceRatio should have been removed from OwnershipGraphConfig")
    }

    func testSignalPriorityOrder() {
        // user override > RSS/sponsor > frequency
        var graph = OwnershipGraph(podcastId: "pod1")

        // 1. Frequency classifies as showOwned
        for _ in 0..<5 {
            graph.recordShowNotesDomain("https://example.com")
        }
        XCTAssertEqual(graph.ownership(for: "example.com"), .show)

        // 2. Sponsor registration overrides frequency
        graph.registerSponsorDomain("https://example.com")
        XCTAssertEqual(graph.ownership(for: "example.com"), .thirdParty)

        // 3. User override overrides sponsor
        graph.applyUserOverride("https://example.com", label: .showOwned)
        XCTAssertEqual(graph.ownership(for: "example.com"), .show)

        // 4. Nothing can override user override
        graph.registerSponsorDomain("https://example.com")
        graph.registerNetworkDomain("https://example.com")
        XCTAssertEqual(graph.ownership(for: "example.com"), .show)
        XCTAssertEqual(graph.entries["example.com"]?.source, .userOverride)
    }
}
