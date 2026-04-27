// DebugEpisodeExporter.swift
// Debug-only helper that builds a text export of the current episode's
// analysis state: transcript, detected ads, evidence catalog, feature
// summary, and acoustic breaks. Used from the Settings debug section to
// validate Phase 1/Phase 2 components against real audio.

#if DEBUG

import Foundation
import CoreTransferable
import UniformTypeIdentifiers

// MARK: - DebugEpisodeExport

/// A Transferable document containing a text-format episode analysis report.
/// Used with ShareLink to export from the app.
struct DebugEpisodeExport: Transferable, Sendable {
    let content: String
    let filename: String

    static var transferRepresentation: some TransferRepresentation {
        DataRepresentation(exportedContentType: .plainText) { export in
            Data(export.content.utf8)
        }
        .suggestedFileName { $0.filename }
    }
}

// MARK: - DebugEpisodeExporter

enum DebugEpisodeExporter {

    /// Build an export for the current episode. Returns nil if no episode is loaded.
    static func build(
        episodeTitle: String,
        podcastTitle: String,
        analysisAssetId: String,
        episodeId: String,
        store: AnalysisStore
    ) async -> DebugEpisodeExport? {
        do {
            let asset = try await store.fetchAsset(id: analysisAssetId)
            let chunks = try await store.fetchTranscriptChunks(assetId: analysisAssetId)
            let adWindows = try await store.fetchAdWindows(assetId: analysisAssetId)

            let maxTime = max(
                chunks.last?.endTime ?? 0,
                adWindows.last?.endTime ?? 0,
                asset?.featureCoverageEndTime ?? 0
            )

            let featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: 0,
                to: maxTime + 1
            )

            let content = formatExport(
                episodeTitle: episodeTitle,
                podcastTitle: podcastTitle,
                analysisAssetId: analysisAssetId,
                episodeId: episodeId,
                asset: asset,
                chunks: chunks,
                adWindows: adWindows,
                featureWindows: featureWindows
            )

            let safeTitle = episodeTitle
                .components(separatedBy: CharacterSet.alphanumerics.inverted)
                .filter { !$0.isEmpty }
                .joined(separator: "_")
                .prefix(40)

            let timestamp = ISO8601DateFormatter().string(from: Date())
                .replacingOccurrences(of: ":", with: "-")

            return DebugEpisodeExport(
                content: content,
                filename: "playhead_export_\(safeTitle)_\(timestamp).txt"
            )
        } catch {
            return nil
        }
    }

    // MARK: - Formatting

    private static func formatExport(
        episodeTitle: String,
        podcastTitle: String,
        analysisAssetId: String,
        episodeId: String,
        asset: AnalysisAsset?,
        chunks: [TranscriptChunk],
        adWindows: [AdWindow],
        featureWindows: [FeatureWindow]
    ) -> String {
        var out = ""

        // Header
        out += "PLAYHEAD DEBUG EXPORT\n"
        out += String(repeating: "=", count: 60) + "\n"
        out += "Exported:      \(Date())\n"
        out += "Podcast:       \(podcastTitle)\n"
        out += "Episode:       \(episodeTitle)\n"
        out += "Episode ID:    \(episodeId)\n"
        out += "Analysis ID:   \(analysisAssetId)\n"
        if let asset = asset {
            out += "Asset state:   \(asset.analysisState)\n"
            out += "Asset version: \(asset.analysisVersion)\n"
            if let cov = asset.featureCoverageEndTime {
                out += "Feature cov:   \(formatTime(cov))\n"
            }
            if let cov = asset.fastTranscriptCoverageEndTime {
                out += "Fast cov:      \(formatTime(cov))\n"
            }
            if let cov = asset.confirmedAdCoverageEndTime {
                out += "Confirmed cov: \(formatTime(cov))\n"
            }
        }
        out += "\n"

        // Transcript
        out += sectionHeader("TRANSCRIPT (\(chunks.count) chunks)")
        if chunks.isEmpty {
            out += "  (no transcript chunks)\n"
        } else {
            let sorted = chunks.sorted { $0.chunkIndex < $1.chunkIndex }
            for chunk in sorted {
                let start = formatTime(chunk.startTime)
                let end = formatTime(chunk.endTime)
                out += "[\(start)-\(end)] (\(chunk.pass)) \(chunk.text)\n"
            }
        }
        out += "\n"

        // Detected ads
        out += sectionHeader("DETECTED AD WINDOWS (\(adWindows.count))")
        if adWindows.isEmpty {
            out += "  (no ad windows detected)\n"
        } else {
            for (i, w) in adWindows.enumerated() {
                out += "Ad #\(i + 1):\n"
                out += "  Time:       \(formatTime(w.startTime)) - \(formatTime(w.endTime)) (\(formatDuration(w.endTime - w.startTime)))\n"
                out += "  Confidence: \(String(format: "%.2f", w.confidence))\n"
                out += "  Decision:   \(w.decisionState)\n"
                out += "  Boundary:   \(w.boundaryState)\n"
                if let advertiser = w.advertiser {
                    out += "  Advertiser: \(advertiser)\n"
                }
                if let product = w.product {
                    out += "  Product:    \(product)\n"
                }
                if let evidence = w.evidenceText, !evidence.isEmpty {
                    out += "  Evidence:   \(evidence)\n"
                }
                if w.wasSkipped { out += "  (was skipped)\n" }
                if w.userDismissedBanner { out += "  (user dismissed banner)\n" }
                out += "\n"
            }
        }

        // Evidence catalog (computed from transcript atoms)
        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: analysisAssetId,
            normalizationHash: "debug-export",
            sourceHash: "debug-export"
        )
        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: analysisAssetId,
            transcriptVersion: version.transcriptVersion
        )
        out += sectionHeader("EVIDENCE CATALOG (\(catalog.entries.count) entries)")
        if catalog.entries.isEmpty {
            out += "  (no evidence extracted)\n"
        } else {
            for entry in catalog.entries {
                let time = formatTime(entry.startTime)
                out += "[E\(entry.evidenceRef)] \(entry.category.rawValue) @ \(time) (atom \(entry.atomOrdinal))\n"
                out += "    \"\(entry.matchedText)\"\n"
            }
        }
        out += "\n"

        // Feature window summary
        out += sectionHeader("FEATURE WINDOWS (\(featureWindows.count) total)")
        if !featureWindows.isEmpty {
            let rmsValues = featureWindows.map(\.rms)
            let fluxValues = featureWindows.map(\.spectralFlux)
            let pauseValues = featureWindows.map(\.pauseProbability)

            out += "  RMS:             min=\(fmt(rmsValues.min() ?? 0)) max=\(fmt(rmsValues.max() ?? 0)) mean=\(fmt(mean(rmsValues)))\n"
            out += "  Spectral flux:   min=\(fmt(fluxValues.min() ?? 0)) max=\(fmt(fluxValues.max() ?? 0)) mean=\(fmt(mean(fluxValues)))\n"
            out += "  Pause prob:      min=\(fmt(pauseValues.min() ?? 0)) max=\(fmt(pauseValues.max() ?? 0)) mean=\(fmt(mean(pauseValues)))\n"

            let highPauseCount = featureWindows.filter { $0.pauseProbability > 0.6 }.count
            out += "  High-pause windows: \(highPauseCount)\n"
        }
        out += "\n"

        // Acoustic breaks (from Phase 2 detector)
        let breaks = AcousticBreakDetector.detectBreaks(in: featureWindows)
        out += sectionHeader("ACOUSTIC BREAKS (\(breaks.count) detected)")
        if breaks.isEmpty {
            out += "  (no breaks detected)\n"
        } else {
            for b in breaks {
                let signals = b.signals.map(\.rawValue).sorted().joined(separator: "+")
                out += "  \(formatTime(b.time)) strength=\(fmt(b.breakStrength)) signals=[\(signals)]\n"
            }
        }
        out += "\n"

        // Ground truth helper section
        out += sectionHeader("MANUAL VERIFICATION WORKSHEET")
        out += "Fill in known ad positions here, then compare to DETECTED AD WINDOWS above:\n"
        out += "\n"
        out += "  Ad 1:  __:__ - __:__  advertiser: _______________\n"
        out += "  Ad 2:  __:__ - __:__  advertiser: _______________\n"
        out += "  Ad 3:  __:__ - __:__  advertiser: _______________\n"
        out += "  Ad 4:  __:__ - __:__  advertiser: _______________\n"
        out += "\n"
        out += "Missed ads (false negatives): _______________\n"
        out += "False positive detections:    _______________\n"
        out += "Boundary drift (sec):         _______________\n"

        return out
    }

    // MARK: - Library-wide export

    /// Build a summary export across every analyzed episode in the store.
    /// Intended for batch eyeballing precision/recall across the user's library.
    static func buildLibraryExport(store: AnalysisStore) async -> DebugEpisodeExport? {
        do {
            let assets = try await store.fetchAllAssets()
            guard !assets.isEmpty else {
                return DebugEpisodeExport(
                    content: "PLAYHEAD LIBRARY EXPORT\n\nNo analyzed episodes found in the store.\n",
                    filename: "playhead_library_export_empty.txt"
                )
            }

            var perAsset: [(AnalysisAsset, [TranscriptChunk], [AdWindow])] = []
            for asset in assets {
                let chunks = (try? await store.fetchTranscriptChunks(assetId: asset.id)) ?? []
                let ads = (try? await store.fetchAdWindows(assetId: asset.id)) ?? []
                perAsset.append((asset, chunks, ads))
            }

            let content = formatLibraryExport(perAsset: perAsset)

            let timestamp = ISO8601DateFormatter().string(from: Date())
                .replacingOccurrences(of: ":", with: "-")

            return DebugEpisodeExport(
                content: content,
                filename: "playhead_library_export_\(timestamp).txt"
            )
        } catch {
            return nil
        }
    }

    private static func formatLibraryExport(
        perAsset: [(AnalysisAsset, [TranscriptChunk], [AdWindow])]
    ) -> String {
        var out = ""

        // Header
        out += "PLAYHEAD LIBRARY EXPORT\n"
        out += String(repeating: "=", count: 80) + "\n"
        out += "Exported:         \(Date())\n"
        out += "Total episodes:   \(perAsset.count)\n"

        let totalChunks = perAsset.reduce(0) { $0 + $1.1.count }
        let totalAds = perAsset.reduce(0) { $0 + $1.2.count }
        let episodesWithAds = perAsset.filter { !$0.2.isEmpty }.count
        let episodesFullyAnalyzed = perAsset.filter { $0.0.analysisState == "ready" || $0.0.analysisState == "confirmed" }.count

        out += "Total chunks:     \(totalChunks)\n"
        out += "Total ads:        \(totalAds)\n"
        out += "Episodes with ads: \(episodesWithAds)\n"
        out += "Fully analyzed:   \(episodesFullyAnalyzed)\n"
        out += "\n"

        // Per-episode summary table
        out += sectionHeader("EPISODE SUMMARY")
        out += pad("Asset ID", 36) + "  " + pad("State", 10) + "  " + pad("Chunks", 7) + "  " + pad("Ads", 7) + "  Coverage\n"
        out += String(repeating: "-", count: 80) + "\n"
        for (asset, chunks, ads) in perAsset {
            let shortId = String(asset.id.prefix(36))
            let cov = asset.fastTranscriptCoverageEndTime.map { formatTime($0) } ?? "-"
            out += pad(shortId, 36) + "  "
                + pad(String(asset.analysisState.prefix(10)), 10) + "  "
                + pad("\(chunks.count)", 7) + "  "
                + pad("\(ads.count)", 7) + "  "
                + cov + "\n"
        }
        out += "\n"

        // Flat list of all detected ads (CSV-friendly)
        out += sectionHeader("ALL DETECTED ADS (CSV)")
        out += "asset_id,ad_index,start_sec,end_sec,duration_sec,confidence,decision,advertiser,product\n"
        for (asset, _, ads) in perAsset {
            for (i, w) in ads.enumerated() {
                let advertiser = (w.advertiser ?? "").replacingOccurrences(of: ",", with: " ")
                let product = (w.product ?? "").replacingOccurrences(of: ",", with: " ")
                out += String(format: "%@,%d,%.1f,%.1f,%.1f,%.3f,%@,%@,%@\n",
                              asset.id,
                              i + 1,
                              w.startTime,
                              w.endTime,
                              w.endTime - w.startTime,
                              w.confidence,
                              w.decisionState,
                              advertiser,
                              product)
            }
        }
        out += "\n"

        // Per-episode detailed detections (human-readable)
        out += sectionHeader("PER-EPISODE DETECTIONS")
        for (asset, chunks, ads) in perAsset {
            out += "\n### Asset \(asset.id) (episode: \(asset.episodeId))\n"
            out += "    State: \(asset.analysisState)  Chunks: \(chunks.count)  Ads: \(ads.count)\n"
            if ads.isEmpty {
                out += "    (no ads detected)\n"
                continue
            }
            for (i, w) in ads.enumerated() {
                let advertiser = w.advertiser ?? "?"
                out += "    Ad #\(i + 1): \(formatTime(w.startTime))-\(formatTime(w.endTime)) "
                out += "conf=\(fmt(w.confidence)) \(w.decisionState) \(advertiser)\n"
                if let evidence = w.evidenceText, !evidence.isEmpty {
                    out += "        evidence: \"\(evidence.prefix(80))\"\n"
                }
            }
        }
        out += "\n"

        out += sectionHeader("LIBRARY VERIFICATION WORKSHEET")
        out += "Use this to track precision/recall across the library:\n"
        out += "\n"
        out += "  True positives  (detected + confirmed ad):  _____\n"
        out += "  False positives (detected + not an ad):     _____\n"
        out += "  False negatives (missed ad):                _____\n"
        out += "  Precision: TP / (TP + FP) = _____\n"
        out += "  Recall:    TP / (TP + FN) = _____\n"
        out += "\n"
        out += "Notes on misses / false positives:\n"
        out += "  _______________________________________________\n"
        out += "  _______________________________________________\n"

        return out
    }

    // MARK: - Formatting helpers

    private static func sectionHeader(_ title: String) -> String {
        "\n" + String(repeating: "-", count: 60) + "\n" + title + "\n" + String(repeating: "-", count: 60) + "\n"
    }

    private static func formatTime(_ seconds: Double) -> String {
        let total = Int(seconds)
        let m = total / 60
        let s = total % 60
        return String(format: "%d:%02d", m, s)
    }

    private static func pad(_ s: String, _ width: Int) -> String {
        s.count >= width ? s : s + String(repeating: " ", count: width - s.count)
    }

    private static func formatDuration(_ seconds: Double) -> String {
        String(format: "%.1fs", seconds)
    }

    private static func fmt(_ value: Double) -> String {
        String(format: "%.3f", value)
    }

    private static func mean(_ values: [Double]) -> Double {
        guard !values.isEmpty else { return 0 }
        return values.reduce(0, +) / Double(values.count)
    }
}

#endif
