// OwnershipCacheWiringSourceCanaryTests.swift
//
// playhead-dyvy. The cache is only a saving if `metadataLookup`'s episode
// walk lives INSIDE the cache's `compute` closure: a walk performed before the
// cache is consulted is paid on every lookup, hit or miss, and the cache then
// caches nothing but a result nobody needed to wait for. No behavioural test
// constructs `PlayheadApp`'s provider closure, so the wiring is pinned here
// with explicit anti-vacuity assertions.

import Foundation
import XCTest
@testable import Playhead

final class OwnershipCacheWiringSourceCanaryTests: XCTestCase {

    func testTheEpisodeWalkLivesInsideTheCacheCompute() throws {
        let source = try SwiftSourceInspector.loadSource(
            repoRelativePath: "Playhead/App/PlayheadApp.swift"
        )
        let stripped = SwiftSourceInspector.strippingComments(source)
        guard let lookup = stripped.range(of: "metadataLookup: { episodeId in") else {
            return XCTFail("could not locate the metadataLookup closure")
        }
        let body = String(stripped[lookup.upperBound...].prefix(2_400))

        // Anti-vacuity: the region must contain all three parts of the claim.
        XCTAssertTrue(body.contains("ownershipCache.ownership("), "the lookup does not consult the cache")
        XCTAssertTrue(body.contains("domainOwnership("), "vacuous region: the graph is not built here")
        XCTAssertTrue(body.contains("compactMap(\\.feedMetadata)"), "vacuous region: the episode walk is not here")

        let cacheCall = try XCTUnwrap(body.range(of: "ownershipCache.ownership("))
        let walk = try XCTUnwrap(body.range(of: "compactMap(\\.feedMetadata)"))
        XCTAssertTrue(
            walk.lowerBound > cacheCall.lowerBound,
            "the episode walk runs BEFORE the cache is consulted, so every lookup pays it — the cache saves nothing"
        )
        XCTAssertTrue(
            source.contains("let ownershipCache = EpisodeMetadataSnapshot.ShowDomainOwnershipCache()"),
            "one cache per provider, created beside it"
        )
    }
}
