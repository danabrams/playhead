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

    /// playhead-e8mg: the feed host is the one domain a structural signal may
    /// not claim, and the route that used to claim it is gone. Both surviving
    /// routes name `megaphone.fm` here and the graph still refuses it.
    func testFeedHostIsRefusedByBothStructuralRoutes() {
        var graph = OwnershipGraph(podcastId: "pod1", feedHostDomain: "megaphone.fm")
        graph.ingestRSSLink("https://feeds.megaphone.fm/myshow")
        graph.ingestITunesOwner(email: "shows@megaphone.fm")

        XCTAssertNil(graph.ownership(for: "megaphone.fm"))
        XCTAssertTrue(graph.entries.isEmpty)
    }

    /// The refusal is scoped to that one domain, not to the routes.
    func testFeedHostExclusionDoesNotSuppressOtherDomains() {
        var graph = OwnershipGraph(podcastId: "pod1", feedHostDomain: "megaphone.fm")
        graph.ingestRSSLink("https://www.myshow.com")
        graph.ingestITunesOwner(email: "host@myshow.co.uk")

        XCTAssertEqual(graph.ownership(for: "myshow.com"), .show)
        XCTAssertEqual(graph.ownership(for: "myshow.co.uk"), .show)
        XCTAssertNil(graph.ownership(for: "megaphone.fm"))
    }

    /// An unknown feed host admits everything. That is an ABSENCE of a
    /// constraint, not a licence — a graph built without one behaves exactly
    /// as it did before the exclusion existed.
    func testNilFeedHostRefusesNothing() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSLink("https://feeds.megaphone.fm/myshow")

        XCTAssertEqual(graph.ownership(for: "megaphone.fm"), .show)
    }

    /// The exclusion is on the eTLD+1, so the SUBDOMAIN spelling a real feed
    /// URL actually uses is refused too — `rss2.flightcast.com` against a
    /// host domain of `flightcast.com`.
    func testFeedHostExclusionComparesRegistrableDomains() {
        var graph = OwnershipGraph(podcastId: "pod1", feedHostDomain: "flightcast.com")
        graph.ingestRSSLink("https://rss2.flightcast.com/xmsftuzjjykcmqwolaqn6mdn.xml")

        XCTAssertTrue(graph.entries.isEmpty)
    }

    func testIngestITunesOwnerEmail() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestITunesOwner(email: "host@myshow.com")

        XCTAssertEqual(graph.ownership(for: "myshow.com"), .show)
    }

    func testBulkRSSIngest() {
        var graph = OwnershipGraph(podcastId: "pod1", feedHostDomain: "simplecast.com")
        graph.ingestRSSFeed(
            linkURL: "https://www.myshow.com",
            itunesOwnerEmail: "host@myshow.com"
        )

        XCTAssertNil(graph.ownership(for: "simplecast.com"))
        XCTAssertEqual(graph.ownership(for: "myshow.com"), .show)
    }

    func testNilRSSFieldsSkipped() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSFeed(linkURL: nil, itunesOwnerEmail: nil)
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

    /// playhead-kmw4: crossing the recurrence threshold used to promote the
    /// domain to `.showOwned`. It does not any more — a recurring SPONSOR
    /// clears the same bar — so the domain stays `.unknown` and surfaces only
    /// as "ownership undetermined".
    func testFrequencyThresholdDoesNotPromoteToShowOwned() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.recordShowNotesDomain("https://myshow.com")
        graph.recordShowNotesDomain("https://myshow.com")
        graph.recordShowNotesDomain("https://myshow.com")

        XCTAssertEqual(graph.ownership(for: "myshow.com"), .unknown)
        XCTAssertTrue(graph.showOwnedDomains.isEmpty)
        XCTAssertEqual(graph.recurringShowNotesDomains, ["myshow.com"])
        XCTAssertEqual(graph.entries["myshow.com"]?.frequency, 3)
    }

    /// A domain BELOW the recurrence threshold is not "undetermined" either —
    /// it is simply unremarkable, and the caller's fall-through (external
    /// domain) still applies to it. The threshold is what separates the two.
    func testBelowThresholdIsNotReportedAsRecurring() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.recordShowNotesDomain("https://rare.com")
        graph.recordShowNotesDomain("https://rare.com")

        XCTAssertEqual(graph.ownership(for: "rare.com"), .unknown)
        XCTAssertTrue(graph.recurringShowNotesDomains.isEmpty)
    }

    /// The sponsor case the bead was filed for, in miniature: a domain that
    /// appears in 69 of a show's episodes is still not evidence the show owns
    /// it. Nothing about a large count changes the answer.
    func testHighFrequencySponsorDomainNeverBecomesShowOwned() {
        var graph = OwnershipGraph(podcastId: "pod1")
        for _ in 0..<69 {
            graph.recordShowNotesDomain("https://linkedin.com")
        }

        XCTAssertEqual(graph.ownership(for: "linkedin.com"), .unknown)
        XCTAssertFalse(graph.showOwnedDomains.contains("linkedin.com"))
        XCTAssertEqual(graph.recurringShowNotesDomains, ["linkedin.com"])
    }

    /// A structurally classified domain keeps its label and stays OUT of the
    /// undetermined set no matter how often the notes mention it — the
    /// undetermined set is "no signal", not "lots of mentions".
    func testStructuralDomainAccruesCountButIsNotUndetermined() {
        var graph = OwnershipGraph(podcastId: "pod1")
        graph.ingestRSSLink("https://www.myshow.com")
        for _ in 0..<5 {
            graph.recordShowNotesDomain("https://myshow.com/show")
        }

        XCTAssertEqual(graph.ownership(for: "myshow.com"), .show)
        XCTAssertEqual(graph.entries["myshow.com"]?.source, .rssLink)
        XCTAssertEqual(graph.entries["myshow.com"]?.frequency, 5)
        XCTAssertTrue(graph.recurringShowNotesDomains.isEmpty)
    }

    /// The mirror, and the case playhead-e8mg creates: a REFUSED feed host
    /// that also recurs in the notes is not structurally classified any more,
    /// so it falls through to the undetermined set — "say nothing", never
    /// `.showOwned`. It must not come back as ownership by another door.
    func testRefusedFeedHostThatRecursInNotesIsUndeterminedNotShowOwned() {
        var graph = OwnershipGraph(podcastId: "pod1", feedHostDomain: "simplecast.com")
        graph.ingestRSSLink("https://feeds.simplecast.com/abc")
        for _ in 0..<5 {
            graph.recordShowNotesDomain("https://simplecast.com/show")
        }

        XCTAssertEqual(graph.ownership(for: "simplecast.com"), .unknown)
        XCTAssertEqual(graph.entries["simplecast.com"]?.source, .showNotesFrequency)
        XCTAssertEqual(graph.recurringShowNotesDomains, ["simplecast.com"])
        XCTAssertTrue(graph.showOwnedDomains.isEmpty)
    }

    /// A sponsor registration lifts the domain out of the undetermined set:
    /// it now has a real classification, so the "say nothing" treatment ends.
    func testSponsorRegistrationRemovesDomainFromUndeterminedSet() {
        var graph = OwnershipGraph(podcastId: "pod1")
        for _ in 0..<4 {
            graph.recordShowNotesDomain("https://betterhelp.com")
        }
        XCTAssertEqual(graph.recurringShowNotesDomains, ["betterhelp.com"])

        graph.registerSponsorDomain("https://betterhelp.com")

        XCTAssertEqual(graph.ownership(for: "betterhelp.com"), .thirdParty)
        XCTAssertTrue(graph.recurringShowNotesDomains.isEmpty)
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
            showNotesRecurrenceThreshold: 1
        )
        var graph = OwnershipGraph(podcastId: "pod1", config: config)
        graph.recordShowNotesDomain("https://instant.com")

        // The knob still governs WHEN a domain counts as recurring; it no
        // longer governs whether it is called show-owned (playhead-kmw4).
        XCTAssertEqual(graph.ownership(for: "instant.com"), .unknown)
        XCTAssertEqual(graph.recurringShowNotesDomains, ["instant.com"])
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
        graph.ingestITunesOwner(email: "host@myshow.fm")

        XCTAssertEqual(Set(graph.showOwnedDomains), Set(["myshow.com", "myshow.fm"]))
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
        var graph = OwnershipGraph(
            podcastId: "conan",
            feedHostDomain: DomainNormalizer.etld1(from: "https://feeds.simplecast.com/dHoohVNH")
        )

        // RSS signals
        graph.ingestRSSFeed(
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
        // playhead-e8mg: the hosting platform is NOT the show.
        XCTAssertNil(graph.ownership(for: "simplecast.com"))
        XCTAssertEqual(graph.ownership(for: "betterhelp.com"), .thirdParty)
        XCTAssertEqual(graph.ownership(for: "squarespace.com"), .thirdParty)
        XCTAssertEqual(graph.sponsorId(for: "betterhelp.com"), "entry-betterhelp")
    }

    func testConfigDefaultValues() {
        let config = OwnershipGraphConfig.default
        // playhead-kmw4 renamed the knob and changed what it licenses; the
        // VALUE is deliberately unchanged.
        XCTAssertEqual(config.showNotesRecurrenceThreshold, 3)
    }

    func testConfigDefaultHasNoUbiquitousPresenceRatio() {
        // Verify OwnershipGraphConfig.default only has showNotesRecurrenceThreshold.
        // The old ubiquitousPresenceRatio field was removed; this test ensures
        // the struct contains only the expected property.
        let config = OwnershipGraphConfig.default
        let mirror = Mirror(reflecting: config)
        let propertyNames = mirror.children.compactMap(\.label)
        XCTAssertTrue(propertyNames.contains("showNotesRecurrenceThreshold"))
        XCTAssertFalse(propertyNames.contains("ubiquitousPresenceRatio"),
                       "ubiquitousPresenceRatio should have been removed from OwnershipGraphConfig")
        // The promotion knob is GONE, not renamed-and-kept: a reader who
        // greps for the old name must find nothing (playhead-kmw4).
        XCTAssertFalse(propertyNames.contains("showOwnedFrequencyThreshold"),
                       "the frequency->showOwned promotion knob was removed by playhead-kmw4")
    }

    func testSignalPriorityOrder() {
        // user override > RSS/sponsor > frequency
        var graph = OwnershipGraph(podcastId: "pod1")

        // 1. Frequency classifies NOTHING (playhead-kmw4) — it only reports
        //    the domain as recurring with ownership undetermined.
        for _ in 0..<5 {
            graph.recordShowNotesDomain("https://example.com")
        }
        XCTAssertEqual(graph.ownership(for: "example.com"), .unknown)
        XCTAssertEqual(graph.recurringShowNotesDomains, ["example.com"])

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
