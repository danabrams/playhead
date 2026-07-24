// DAIStitchRecorder.swift
// playhead-xsdz.71 (Signal 1): the recording seam that turns an observed
// enclosure redirect chain into a persisted, show-level DAI-EXPECTED prior.
//
// The `DownloadManager` observes the redirect-chain hop hosts at the enclosure
// download (via `RedirectChainRecordingDelegate`) and hands them here. The
// production conformer classifies the chain (`DAIStitchClassifier`) and persists
// the result on the show's `podcast_profiles` row via the column-specific
// `AnalysisStore.updateProfileDAIStitch` setter.
//
// ADDITIVE-ONLY: this OBSERVES and RECORDS. Nothing here (or downstream) yet
// consumes the persisted prior to change detection/rediff/banner behavior — that
// is a follow-on bead.
//
// ONCE-PER-SHOW / IDEMPOTENT: the persistence setter guards on
// `daiStitchNetwork IS NULL`, so the first observation for a show wins and every
// subsequent download is a cheap no-op write. A brand-new show whose
// `podcast_profiles` row does not exist yet (analysis has not materialized one)
// is a benign no-op — a later download, once the row exists, records it. This
// matches the conservative, additive posture of the signal.

import Foundation
import os
import OSLog

// MARK: - Recording seam

/// Receives the ordered redirect-chain hop hosts observed during an enclosure
/// download for a known show. `nil`-injected in `DownloadManager` by default, so
/// the download path is byte-identical until `PlayheadRuntime` wires the
/// production conformer.
protocol DAIStitchChainRecording: Sendable {
    /// Record the redirect-chain hop hosts observed while downloading an
    /// episode of the show identified by `podcastId`. Best-effort and
    /// observational — implementations must never throw into the download path.
    func recordRedirectChain(podcastId: String, hopHosts: [String]) async
}

// MARK: - Production conformer

/// Classifies the observed redirect chain and persists the show-level DAI-stitch
/// classification (`daiStitchNetwork` + `daiExpected`) on the `podcast_profiles`
/// row. A plain `Sendable` value type over the `AnalysisStore` actor.
struct AnalysisStoreDAIStitchRecorder: DAIStitchChainRecording {
    let store: AnalysisStore

    private static let logger = Logger(subsystem: "com.playhead", category: "DAIStitch")

    func recordRedirectChain(podcastId: String, hopHosts: [String]) async {
        guard !podcastId.isEmpty, !hopHosts.isEmpty else { return }

        // Classification is pure/cheap, so we run it unconditionally; the
        // once-per-show gate lives in SQL (`WHERE daiStitchNetwork IS NULL`),
        // making the whole record atomic and free of a read-modify-write window.
        let classification = DAIStitchClassifier.classify(redirectChainHosts: hopHosts)
        do {
            try await store.updateProfileDAIStitch(
                podcastId: podcastId,
                daiStitchNetwork: classification.stitchNetwork.rawValue,
                daiExpected: classification.daiExpected
            )
            Self.logger.info(
                "recorded DAI-stitch classification podcast=\(podcastId, privacy: .public) network=\(classification.stitchNetwork.rawValue, privacy: .public) daiExpected=\(classification.daiExpected, privacy: .public)"
            )
        } catch {
            // Best-effort, observational — a persist failure costs one
            // observation (a later download retries) and never disturbs the
            // download.
            Self.logger.warning(
                "DAI-stitch persist failed podcast=\(podcastId, privacy: .public): \(String(describing: error), privacy: .public)"
            )
        }
    }
}
