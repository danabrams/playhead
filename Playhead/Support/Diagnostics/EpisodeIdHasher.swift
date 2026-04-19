// EpisodeIdHasher.swift
// Pure SHA-256(installID || episodeId) hashing used by the support-safe
// diagnostics bundle. Per-install salt prevents cross-install correlation
// of episode identifiers in support artifacts.
//
// Scope: playhead-ghon (Phase 1.5 — support-safe diagnostics bundle classes).
//
// Legal checklist alignment:
//   (a) The default bundle never carries a raw episode id — every emitted
//       reference is the hex output of this function.
//   (c) Hashing scheme = SHA-256(installID || episodeId), hex-encoded.
//       The salt is the per-install UUID provisioned by ``InstallIdentity``;
//       see that file for storage / rotation policy. If legal requests a
//       stronger scheme (e.g. HMAC with rotating key), the swap happens
//       here and only here.
//
// Pure / side-effect free; safe to call from any actor / thread.

import CryptoKit
import Foundation

enum EpisodeIdHasher {

    /// Returns the lowercase hex SHA-256 of `installID.uuidString || episodeId`.
    ///
    /// `installID.uuidString` is used (not the raw 16-byte UUID) so that
    /// the input bytes are deterministic + human-auditable and exactly
    /// match what's stored on-device. The hex output is 64 lowercase
    /// hex characters, suitable for direct inclusion in the JSON bundle.
    static func hash(installID: UUID, episodeId: String) -> String {
        var bytes: [UInt8] = []
        bytes.append(contentsOf: installID.uuidString.utf8)
        bytes.append(contentsOf: episodeId.utf8)
        return _sha256Hex(bytes)
    }

    /// Internal helper exposed for direct verification in tests.
    /// Lowercase hex per UInt8.
    static func _sha256Hex(_ bytes: [UInt8]) -> String {
        let digest = SHA256.hash(data: bytes)
        var out = ""
        out.reserveCapacity(SHA256.Digest.byteCount * 2)
        for byte in digest {
            out.append(Self.hexNibble(byte >> 4))
            out.append(Self.hexNibble(byte & 0x0F))
        }
        return out
    }

    private static func hexNibble(_ value: UInt8) -> Character {
        switch value & 0x0F {
        case 0...9: return Character(UnicodeScalar(UInt8(0x30) + value))
        default:    return Character(UnicodeScalar(UInt8(0x57) + value)) // 'a' = 0x61, value=10
        }
    }
}
