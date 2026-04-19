// SurfaceStatusEpisodeIdHasher.swift
// SHA-256 episode-ID hasher used by `SurfaceStatusInvariantLogger`.
//
// Scope: playhead-ol05 (Phase 1.5 — implemented locally because
// playhead-ghon, which owns the canonical hasher, is being landed
// concurrently and its hasher is not yet on this branch).
//
// Contract:
//   * Input:  (installID: String, episodeId: String)
//   * Output: hex string of SHA-256(installID || episodeId)
//   * Concatenation is a literal byte append: `installID.utf8 || episodeId.utf8`
//     with no separator. ghon's hasher (when it lands) MUST match.
//   * Hex encoding is lowercase, no separators, length 64.
//
// Legal-safe: the salt is per-install and never leaves the device. An
// attacker without access to the installID file cannot reverse the
// hash to recover the episode ID — the entropy of installID (UUID, 122
// bits) dominates the attack surface.

import Foundation
import CryptoKit

// MARK: - SurfaceStatusEpisodeIdHasher

/// Stateless static helper that hashes an episode ID with the per-install
/// salt to produce the opaque token persisted in the
/// `episode_id_hash` field of every JSON Lines audit entry.
enum SurfaceStatusEpisodeIdHasher {

    /// Hex-encoded SHA-256 of `installId.utf8 || episodeId.utf8`.
    /// Stable across launches as long as the installID file is intact.
    static func hash(installId: String, episodeId: String) -> String {
        var hasher = SHA256()
        hasher.update(data: Data(installId.utf8))
        hasher.update(data: Data(episodeId.utf8))
        let digest = hasher.finalize()
        // Lowercase hex, no separators, length 64.
        return digest.map { String(format: "%02x", $0) }.joined()
    }
}
