// AdPodContinuationDeviceLaneTests.swift
// playhead-xsdz.65: the HELD-OUT precision probe for ad-pod continuation, run
// against the product owner's real device database.
//
// WHY A SECOND LANE
// -----------------
// `AdPodContinuationCorpusEvalTests` calibrated the pass — the link bar and the
// chain gap were both chosen by sweeping that corpus — so its headline numbers
// are IN-SAMPLE and cannot certify precision. This lane is a different corpus:
// the real `analysis.sqlite` from the device, exported to JSON (windows +
// transcript chunks per asset). Nothing here was used to pick a parameter.
//
// WHAT IT ASSERTS, and what it cannot
// -----------------------------------
// The device DB has no rediff slots to score against, so "is this recovered
// second an ad?" cannot be answered mechanically. What it CAN answer, and does:
//
//   1. A LISTENER'S MARK IS NEVER ENGULFED. The DB carries the product owner's
//      own hand-marked spans (`boundaryState == "userMarked"`), including a
//      242 s pod he marked as four consecutive marks while the detector found
//      ~60 s in fragments. Every one is passed as a protected region and NO
//      emitted mark may overlap ANY of them. This is playhead-lc4c on real data
//      rather than a synthetic fixture.
//   2. RECOVERY IS BOUNDED. Total recovered seconds and the widest single mark
//      are reported and capped, so a regression that starts claiming minutes of
//      audio fails here instead of on someone's phone.
//   3. NOTHING IS EVER AUTO-SKIPPABLE. Every emitted row is checked mark-only
//      with unanchored edges — the playhead-2350 property, asserted on the real
//      window population (which includes `eligible`, `autoSkip` and
//      `blockedByUserCorrection` rows the synthetic fixtures do not have).
//
// The report prints every mark so the out-of-mark claims can be read against the
// transcript by hand; that audit is what actually establishes precision, and its
// conclusions belong in the bead, not in an assertion.
//
// STAGING: `PLAYHEAD_POD_DEVICE_LANE` (or `TEST_RUNNER_PLAYHEAD_POD_DEVICE_LANE`)
// points at the exported JSON. The export is a copy of a device pull and is NOT
// in the repo, so with the variable unset this SKIPS.

import Foundation
import XCTest
@testable import Playhead

final class AdPodContinuationDeviceLaneTests: XCTestCase {

    /// Ceiling on total recovered seconds per episode. Generous — the widest
    /// rediff-confirmed pod in the corpus is ~212 s and an episode can carry
    /// several — but low enough that "the chain ran away" fails rather than ships.
    private static let perEpisodeRecoveryCeiling = 400.0

    /// Ceiling on any single mark. A DAI pod longer than this is not a pod.
    private static let singleMarkCeiling = 240.0

    private struct DeviceLane: Decodable {
        struct Asset: Decodable {
            struct Window: Decodable {
                let startTime: Double
                let endTime: Double
                let confidence: Double
                let decisionState: String
                let boundaryState: String
                let detectorVersion: String
                let eligibilityGate: String?
                let startEdgeAnchor: String
                let endEdgeAnchor: String
            }
            struct Chunk: Decodable {
                let startTime: Double
                let endTime: Double
                let text: String
                let normalizedText: String
                let pass: String
            }
            let assetId: String
            let title: String?
            let durationSec: Double?
            let windows: [Window]
            let chunks: [Chunk]
        }
        let assets: [Asset]
    }

    func testPodContinuationOnDeviceDatabaseNeverEngulfsAUserMark() throws {
        let environment = ProcessInfo.processInfo.environment
        guard let path = environment["PLAYHEAD_POD_DEVICE_LANE"],
              FileManager.default.fileExists(atPath: path) else {
            throw XCTSkip(
                "device held-out lane needs PLAYHEAD_POD_DEVICE_LANE pointing at the exported analysis.sqlite JSON"
            )
        }
        let lane = try JSONDecoder().decode(
            DeviceLane.self,
            from: try Data(contentsOf: URL(fileURLWithPath: path))
        )
        try XCTSkipIf(lane.assets.isEmpty, "device lane export has no assets")

        let scanner = LexicalScanner()
        // Arms, mirroring the calibration lane so a held-out finding can be
        // ATTRIBUTED to a link source or a gap rather than just observed.
        struct Arm { let name: String; let lexical: Bool; let rhetorical: Bool; let gap: Double }
        let shippingGap = AdPodContinuation.Configuration.default.maxLinkGapSeconds
        let arms = [
            Arm(name: "lexical only, gap=30", lexical: true, rhetorical: false, gap: 30),
            Arm(name: "rhetorical only, gap=30", lexical: false, rhetorical: true, gap: 30),
            Arm(name: "shipping default", lexical: true, rhetorical: true, gap: shippingGap),
            Arm(name: "shipping links, gap=35", lexical: true, rhetorical: true, gap: 35),
            Arm(name: "shipping links, gap=60", lexical: true, rhetorical: true, gap: 60),
            Arm(name: "shipping links, gap=45", lexical: true, rhetorical: true, gap: 45)
        ]
        var armLines: [String: [String]] = [:]
        var armRecovered: [String: Double] = [:]
        var lines: [String] = []
        var totalMarks = 0
        var totalRecovered = 0.0
        var totalProtectedRegions = 0
        var overlaps: [String] = []
        var gateViolations: [String] = []
        var assetsEvaluated = 0

        for asset in lane.assets where !asset.chunks.isEmpty && !asset.windows.isEmpty {
            assetsEvaluated += 1
            let rawChunks = asset.chunks.enumerated().map { index, chunk in
                TranscriptChunk(
                    id: "\(asset.assetId)-\(index)",
                    analysisAssetId: asset.assetId,
                    segmentFingerprint: "device-\(index)",
                    chunkIndex: index,
                    startTime: chunk.startTime,
                    endTime: chunk.endTime,
                    text: chunk.text,
                    normalizedText: chunk.normalizedText,
                    pass: chunk.pass,
                    modelVersion: "device",
                    transcriptVersion: nil,
                    atomOrdinal: nil
                )
            }
            let windows = asset.windows.map { window in
                AdWindow(
                    id: "\(asset.assetId)-\(window.startTime)-\(window.endTime)-\(window.boundaryState)",
                    analysisAssetId: asset.assetId,
                    startTime: window.startTime,
                    endTime: window.endTime,
                    confidence: window.confidence,
                    boundaryState: window.boundaryState,
                    decisionState: window.decisionState,
                    detectorVersion: window.detectorVersion,
                    advertiser: nil,
                    product: nil,
                    adDescription: nil,
                    evidenceText: nil,
                    evidenceStartTime: nil,
                    metadataSource: "device",
                    metadataConfidence: nil,
                    metadataPromptVersion: nil,
                    wasSkipped: false,
                    userDismissedBanner: false,
                    eligibilityGate: window.eligibilityGate,
                    startEdgeAnchor: window.startEdgeAnchor,
                    endEdgeAnchor: window.endEdgeAnchor
                )
            }
            // The listener's own spans — exactly what the production wire-in reads.
            let protectedRegions = windows
                .filter { $0.boundaryState == "userMarked" }
                .map { (start: $0.startTime, end: $0.endTime) }
            totalProtectedRegions += protectedRegions.count

            // Production canonicalizes the mixed fast/final chunk array BEFORE any
            // consumer reads it (playhead-hc7e), and a real device transcript
            // genuinely carries both passes over the same seconds — the raw export
            // shows every overlapped segment twice. Reading the raw array here
            // would double-count the text and make this lane measure something
            // `runBackfill` never sees.
            let chunks = TranscriptChunkCanonicalizer.canonicalize(rawChunks).chunks
            let hits = scanner.collectHits(chunks: chunks)
            let lexicalLinks = AdPodContinuation.adCopyLinks(chunks: chunks, hits: hits)
            let rhetorical = AdPodContinuation.rhetoricalLinks(chunks: chunks)
            let armBarriers = AdPodContinuation.contentBarriers(
                semanticScanResults: [],
                lexicalHits: hits,
                chunks: chunks
            )
            for arm in arms {
                var armLinks: [AdPodContinuation.AdCopyLink] = []
                if arm.lexical { armLinks += lexicalLinks }
                if arm.rhetorical { armLinks += rhetorical }
                let armMarks = AdPodContinuation.compose(
                    existingWindows: windows,
                    adCopyLinks: AdPodContinuation.mergeLinks(armLinks),
                    contentBarriers: armBarriers,
                    protectedRegions: protectedRegions,
                    episodeDuration: asset.durationSec ?? 0,
                    analysisAssetId: asset.assetId,
                    config: AdPodContinuation.Configuration(maxLinkGapSeconds: arm.gap)
                )
                armRecovered[arm.name, default: 0] += armMarks.reduce(0.0) {
                    $0 + ($1.endTime - $1.startTime)
                }
                armLines[arm.name, default: []] += armMarks.map { mark in
                    String(
                        format: "      %-46@ %8.1f-%8.1f (%5.1fs)",
                        String((asset.title ?? asset.assetId).prefix(46)) as NSString,
                        mark.startTime,
                        mark.endTime,
                        mark.endTime - mark.startTime
                    )
                }
            }
            let marks = AdPodContinuation.compose(
                existingWindows: windows,
                adCopyLinks: AdPodContinuation.mergeLinks(
                    AdPodContinuation.adCopyLinks(chunks: chunks, hits: hits)
                        + AdPodContinuation.rhetoricalLinks(chunks: chunks)
                ),
                // No FM scan rows in the export, so the FM barrier is absent here
                // too: fewer barriers ⇒ MORE recovery ⇒ this is the worst case.
                contentBarriers: AdPodContinuation.contentBarriers(
                    semanticScanResults: [],
                    lexicalHits: hits,
                    chunks: chunks
                ),
                protectedRegions: protectedRegions,
                episodeDuration: asset.durationSec ?? 0,
                analysisAssetId: asset.assetId
            )
            totalMarks += marks.count
            let recovered = marks.reduce(0.0) { $0 + ($1.endTime - $1.startTime) }
            totalRecovered += recovered

            for mark in marks {
                if mark.eligibilityGate != SkipEligibilityGate.markOnly.rawValue
                    || mark.startEdgeAnchor != AutoSkipEdgeAnchor.unanchored.rawValue
                    || mark.endEdgeAnchor != AutoSkipEdgeAnchor.unanchored.rawValue
                    || mark.decisionState != AdDecisionState.candidate.rawValue {
                    gateViolations.append(
                        "\(asset.title ?? asset.assetId) \(mark.startTime)-\(mark.endTime)"
                    )
                }
                for region in protectedRegions
                where mark.startTime < region.end && mark.endTime > region.start {
                    overlaps.append(
                        String(
                            format: "%@: mark %.1f-%.1f overlaps user mark %.1f-%.1f",
                            asset.title ?? asset.assetId,
                            mark.startTime, mark.endTime, region.start, region.end
                        )
                    )
                }
                lines.append(
                    String(
                        format: "    %-52@ %8.1f-%8.1f (%5.1fs)",
                        String((asset.title ?? asset.assetId).prefix(52)) as NSString,
                        mark.startTime,
                        mark.endTime,
                        mark.endTime - mark.startTime
                    )
                )
            }
            XCTAssertLessThanOrEqual(
                recovered,
                Self.perEpisodeRecoveryCeiling,
                "runaway recovery on \(asset.title ?? asset.assetId): \(recovered)s"
            )
            for mark in marks {
                XCTAssertLessThanOrEqual(
                    mark.endTime - mark.startTime,
                    Self.singleMarkCeiling,
                    "single mark too wide on \(asset.title ?? asset.assetId)"
                )
            }
        }

        let armAttribution = arms.map { arm in
            let header = "  ARM \(arm.name): "
                + String(format: "%.0f", armRecovered[arm.name] ?? 0)
                + "s in \(armLines[arm.name]?.count ?? 0) marks"
            return ([header] + (armLines[arm.name] ?? [])).joined(separator: "\n")
        }.joined(separator: "\n")

        print(
            """

            == playhead-xsdz.65 device HELD-OUT lane =============================
            assets evaluated              \(assetsEvaluated)
            user-marked protected regions  \(totalProtectedRegions)
            marks emitted                  \(totalMarks)
            seconds recovered              \(String(format: "%.0f", totalRecovered))
            marks overlapping a user mark  \(overlaps.count)
            banner-tier violations         \(gateViolations.count)
            marks (for hand-audit against the transcript):
            \(lines.joined(separator: "\n"))

            per-arm attribution:
            \(armAttribution)
            =====================================================================

            """
        )

        XCTAssertTrue(overlaps.isEmpty, "a listener's mark was engulfed: \(overlaps)")
        XCTAssertTrue(gateViolations.isEmpty, "a mark was not banner-tier: \(gateViolations)")
    }
}
