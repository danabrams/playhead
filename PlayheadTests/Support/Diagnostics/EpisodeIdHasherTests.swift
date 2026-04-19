// EpisodeIdHasherTests.swift
// Verifies the episodeId-hashing scheme used by the diagnostics bundle.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Spec contract:
//   episodeId_hash = SHA-256(installID || episodeId), hex-encoded.
//   Different installIDs produce different hashes for the same episodeId
//   (per-install salting prevents cross-install correlation).
//
// Legal-safe property under test (checklist item c): the per-install salt
// is the ONLY entropy that links a hash back to a raw episode id, and the
// salt never leaves the device unless the user explicitly opts in.

import Foundation
import Testing

@testable import Playhead

@Suite("EpisodeIdHasher — SHA-256(installID || episodeId), hex (playhead-ghon)")
struct EpisodeIdHasherTests {

    // MARK: - Output shape

    @Test("hex output is 64 chars and all lowercase hex")
    func hexShape() {
        let installID = UUID()
        let hash = EpisodeIdHasher.hash(installID: installID, episodeId: "any-episode")
        #expect(hash.count == 64)
        let allowed = Set("0123456789abcdef")
        #expect(hash.allSatisfy { allowed.contains($0) })
    }

    // MARK: - Determinism

    @Test("same inputs always produce the same hash")
    func deterministic() {
        let installID = UUID()
        let h1 = EpisodeIdHasher.hash(installID: installID, episodeId: "ep-1")
        let h2 = EpisodeIdHasher.hash(installID: installID, episodeId: "ep-1")
        #expect(h1 == h2)
    }

    // MARK: - Per-install salting (the legal-relevant property)

    @Test("different installIDs produce different hashes for same episodeId")
    func perInstallSalt() {
        let install1 = UUID()
        let install2 = UUID()
        #expect(install1 != install2)
        let h1 = EpisodeIdHasher.hash(installID: install1, episodeId: "ep-1")
        let h2 = EpisodeIdHasher.hash(installID: install2, episodeId: "ep-1")
        #expect(h1 != h2)
    }

    @Test("different episodeIds produce different hashes for same install")
    func differentEpisodes() {
        let installID = UUID()
        let h1 = EpisodeIdHasher.hash(installID: installID, episodeId: "ep-1")
        let h2 = EpisodeIdHasher.hash(installID: installID, episodeId: "ep-2")
        #expect(h1 != h2)
    }

    // MARK: - Concatenation order

    @Test("salt is prepended (installID || episodeId), not appended")
    func saltPrepended() {
        // Two installIDs constructed so that swapping the concatenation
        // order would yield colliding hashes — which would only happen
        // accidentally. We instead lock down the order by checking the
        // hash matches the straight Foundation/CryptoKit computation
        // applied to `installID.uuidString.utf8 + episodeId.utf8`.
        let installID = UUID()
        let episodeId = "ep-1"
        let expectedInputBytes =
            Array(installID.uuidString.utf8) + Array(episodeId.utf8)
        let expected = EpisodeIdHasher._sha256Hex(expectedInputBytes)
        let actual = EpisodeIdHasher.hash(installID: installID, episodeId: episodeId)
        #expect(actual == expected)
    }

    // MARK: - Empty / unicode episodeIds

    @Test("empty episodeId still produces a stable, valid hash")
    func emptyEpisodeId() {
        let installID = UUID()
        let h = EpisodeIdHasher.hash(installID: installID, episodeId: "")
        #expect(h.count == 64)
    }

    @Test("non-ASCII episodeId is handled via UTF-8 bytes")
    func unicodeEpisodeId() {
        let installID = UUID()
        let asciiId = "ep-emoji"
        let unicodeId = "ep-\u{1F4FB}"
        let h1 = EpisodeIdHasher.hash(installID: installID, episodeId: asciiId)
        let h2 = EpisodeIdHasher.hash(installID: installID, episodeId: unicodeId)
        #expect(h1.count == 64)
        #expect(h2.count == 64)
        #expect(h1 != h2)
    }
}
