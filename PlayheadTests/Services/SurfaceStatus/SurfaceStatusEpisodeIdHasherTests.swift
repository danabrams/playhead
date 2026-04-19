// SurfaceStatusEpisodeIdHasherTests.swift
// Determinism + format tests for `SurfaceStatusEpisodeIdHasher`.
//
// Scope: playhead-ol05 (Phase 1.5 — local hasher implementation, will
// reconcile with playhead-ghon's canonical hasher when ghon lands).

import Foundation
import Testing

@testable import Playhead

@Suite("SurfaceStatusEpisodeIdHasher (playhead-ol05)")
struct SurfaceStatusEpisodeIdHasherTests {

    @Test("Same (installID, episodeId) inputs produce the same hash")
    func deterministic() {
        let a = SurfaceStatusEpisodeIdHasher.hash(
            installId: "install-A",
            episodeId: "episode-1"
        )
        let b = SurfaceStatusEpisodeIdHasher.hash(
            installId: "install-A",
            episodeId: "episode-1"
        )
        #expect(a == b)
    }

    @Test("Different installIDs produce different hashes for the same episodeId")
    func saltDifferentiates() {
        let a = SurfaceStatusEpisodeIdHasher.hash(
            installId: "install-A",
            episodeId: "episode-1"
        )
        let b = SurfaceStatusEpisodeIdHasher.hash(
            installId: "install-B",
            episodeId: "episode-1"
        )
        #expect(a != b)
    }

    @Test("Different episodeIds produce different hashes for the same installID")
    func episodeDifferentiates() {
        let a = SurfaceStatusEpisodeIdHasher.hash(
            installId: "install-A",
            episodeId: "episode-1"
        )
        let b = SurfaceStatusEpisodeIdHasher.hash(
            installId: "install-A",
            episodeId: "episode-2"
        )
        #expect(a != b)
    }

    @Test("Hash output is exactly 64 lowercase hex characters")
    func hexFormat() {
        let h = SurfaceStatusEpisodeIdHasher.hash(
            installId: "install-A",
            episodeId: "episode-1"
        )
        #expect(h.count == 64)
        #expect(h.allSatisfy { $0.isHexDigit && (!$0.isLetter || $0.isLowercase) })
    }

    @Test("Concatenation order matters — installID || episodeId is not the same as episodeId || installID")
    func orderMatters() {
        let normal = SurfaceStatusEpisodeIdHasher.hash(
            installId: "AAA",
            episodeId: "BBB"
        )
        let swapped = SurfaceStatusEpisodeIdHasher.hash(
            installId: "BBB",
            episodeId: "AAA"
        )
        #expect(normal != swapped, "Hash of (AAA, BBB) must differ from (BBB, AAA)")
    }
}
