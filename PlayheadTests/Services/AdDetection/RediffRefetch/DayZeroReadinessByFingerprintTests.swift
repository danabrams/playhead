import Foundation
import Testing
@testable import Playhead

/// playhead-66cn — the day-0 readiness probe resolves the asset row by the
/// pinned file's fingerprint, so a stale registration cannot win after a
/// re-download with different bytes.
@Suite("playhead-66cn: readiness resolves by the file, not the episode")
struct DayZeroReadinessByFingerprintTests {

    private func asset(id: String, episodeId: String, fingerprint: String, createdAt: Date) -> AnalysisAsset {
        var a = makeSkipTestAnalysisAsset(id: id, episodeId: episodeId)
        a = AnalysisAsset(
            id: a.id, episodeId: a.episodeId, assetFingerprint: fingerprint, weakFingerprint: a.weakFingerprint,
            sourceURL: a.sourceURL, featureCoverageEndTime: a.featureCoverageEndTime,
            fastTranscriptCoverageEndTime: a.fastTranscriptCoverageEndTime,
            confirmedAdCoverageEndTime: a.confirmedAdCoverageEndTime, analysisState: a.analysisState,
            analysisVersion: a.analysisVersion, capabilitySnapshot: a.capabilitySnapshot, createdAt: createdAt,
            episodeTitle: a.episodeTitle, finalPassCoverageEndTime: a.finalPassCoverageEndTime,
            terminalReason: a.terminalReason, artifactClass: a.artifactClass, episodeDurationSec: a.episodeDurationSec
        )
        return a
    }

    @Test("the fingerprint fetch returns the matching row, never a newer row for other bytes")
    func fetchByFingerprintIgnoresNewerStaleRows() async throws {
        let store = try await makeTestStore()
        let t0 = Date(timeIntervalSince1970: 1_700_000_000)
        try await store.insertAsset(asset(id: "row-sha1", episodeId: "ep", fingerprint: "SHA1", createdAt: t0))
        try await store.insertAsset(asset(id: "row-sha2", episodeId: "ep", fingerprint: "SHA2", createdAt: t0.addingTimeInterval(60)))

        // The bare episode fetch is the defect: newest wins.
        #expect(try await store.fetchAssetByEpisodeId("ep")?.id == "row-sha2")
        // The fingerprint fetch is the fix.
        #expect(try await store.fetchAssetByEpisodeId("ep", assetFingerprint: "SHA1")?.id == "row-sha1")
        #expect(try await store.fetchAssetByEpisodeId("ep", assetFingerprint: "SHA2")?.id == "row-sha2")
        #expect(try await store.fetchAssetByEpisodeId("ep", assetFingerprint: "SHA3") == nil,
                "a row that exists only for other bytes is not this file's row")
    }

    @Test("the pinned-file cache hashes once per unchanged file, and again when the bytes change in place")
    func fingerprintCacheRehashesOnlyWhenTheFileChanges() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("66cn-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let file = dir.appendingPathComponent("ep.mp3")
        try Data(repeating: 0x11, count: 8192).write(to: file)

        let cache = PinnedFileFingerprintCache()
        let first = try await cache.canonicalFingerprint(of: file)
        let again = try await cache.canonicalFingerprint(of: file)
        #expect(first == again)
        #expect(await cache.hashCount == 1, "an unchanged file is hashed once across polls")
        #expect(first == (try FileHasher.sha256(fileURL: file)), "the same derivation registration stores")

        // A re-download replaces the bytes at the SAME path.
        try Data(repeating: 0x22, count: 8192).write(to: file)
        try FileManager.default.setAttributes([.modificationDate: Date().addingTimeInterval(5)], ofItemAtPath: file.path)
        let replaced = try await cache.canonicalFingerprint(of: file)
        #expect(replaced != first, "new bytes at the old path must produce the new fingerprint")
        #expect(await cache.hashCount == 2)
    }
}
