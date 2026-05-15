// AnalysisStore+CrossUserSharing.swift
// Phase A cross-user sharing for derived ad-window analysis keyed by
// (podcastId, episodeId, full-file SHA).

import CryptoKit
import Foundation

struct CrossUserAnalysisShareKey: Codable, Equatable, Hashable, Sendable {
    let podcastId: String
    let episodeId: String
    let fileSHA: String
}

struct CrossUserAnalysisProvenance: Codable, Equatable, Sendable {
    let exportedAt: Double
    let sourceAnalysisVersion: Int
    let sourceAppBuild: String?
    let pipelineVersions: PipelineVersions

    init(
        exportedAt: Double,
        sourceAnalysisVersion: Int,
        sourceAppBuild: String?,
        pipelineVersions: PipelineVersions = PipelineVersions.current()
    ) {
        self.exportedAt = exportedAt
        self.sourceAnalysisVersion = sourceAnalysisVersion
        self.sourceAppBuild = sourceAppBuild
        self.pipelineVersions = pipelineVersions
    }
}

struct CrossUserAnalysisMeasurements: Codable, Equatable, Sendable {
    let fmMinutesSaved: Double?
    let queueToReadyLatencySec: Double?
    let batteryDeltaPercent: Double?

    init(
        fmMinutesSaved: Double? = nil,
        queueToReadyLatencySec: Double? = nil,
        batteryDeltaPercent: Double? = nil
    ) {
        self.fmMinutesSaved = fmMinutesSaved
        self.queueToReadyLatencySec = queueToReadyLatencySec
        self.batteryDeltaPercent = batteryDeltaPercent
    }
}

struct CrossUserAnalysisSnapshot: Codable, Equatable, Sendable {
    static let currentSchemaVersion = 2

    let schemaVersion: Int
    let key: CrossUserAnalysisShareKey
    let provenance: CrossUserAnalysisProvenance
    let analysisCoverageEndSec: Double
    let measurements: CrossUserAnalysisMeasurements
    let windows: [Window]

    init(
        schemaVersion: Int = CrossUserAnalysisSnapshot.currentSchemaVersion,
        key: CrossUserAnalysisShareKey,
        provenance: CrossUserAnalysisProvenance,
        analysisCoverageEndSec: Double,
        measurements: CrossUserAnalysisMeasurements,
        windows: [Window]
    ) {
        self.schemaVersion = schemaVersion
        self.key = key
        self.provenance = provenance
        self.analysisCoverageEndSec = analysisCoverageEndSec
        self.measurements = measurements
        self.windows = windows
    }

    struct Window: Codable, Equatable, Sendable {
        let sourceWindowId: String
        let startTime: Double
        let endTime: Double
        let confidence: Double
        let boundaryState: String
        let decisionState: String
        let isAd: Bool
        let detectorVersion: String
        let advertiser: String?
        let product: String?
        let adDescription: String?
        let metadataSource: String
        let metadataConfidence: Double?
        let metadataPromptVersion: String?
        let evidenceSources: String?
        let eligibilityGate: String?
        let catalogStoreMatchSimilarity: Double?

        init(
            sourceWindowId: String,
            startTime: Double,
            endTime: Double,
            confidence: Double,
            boundaryState: String,
            decisionState: String,
            isAd: Bool = true,
            detectorVersion: String,
            advertiser: String?,
            product: String?,
            adDescription: String?,
            metadataSource: String,
            metadataConfidence: Double?,
            metadataPromptVersion: String?,
            evidenceSources: String?,
            eligibilityGate: String?,
            catalogStoreMatchSimilarity: Double?
        ) {
            self.sourceWindowId = sourceWindowId
            self.startTime = startTime
            self.endTime = endTime
            self.confidence = confidence
            self.boundaryState = boundaryState
            self.decisionState = decisionState
            self.isAd = isAd
            self.detectorVersion = detectorVersion
            self.advertiser = advertiser
            self.product = product
            self.adDescription = adDescription
            self.metadataSource = metadataSource
            self.metadataConfidence = metadataConfidence
            self.metadataPromptVersion = metadataPromptVersion
            self.evidenceSources = evidenceSources
            self.eligibilityGate = eligibilityGate
            self.catalogStoreMatchSimilarity = catalogStoreMatchSimilarity
        }

        init(adWindow: AdWindow) {
            self.init(
                sourceWindowId: adWindow.id,
                startTime: adWindow.startTime,
                endTime: adWindow.endTime,
                confidence: adWindow.confidence,
                boundaryState: adWindow.boundaryState,
                decisionState: adWindow.decisionState,
                isAd: Self.isAdDecision(adWindow.decisionState),
                detectorVersion: adWindow.detectorVersion,
                advertiser: adWindow.advertiser,
                product: adWindow.product,
                adDescription: adWindow.adDescription,
                metadataSource: adWindow.metadataSource,
                metadataConfidence: adWindow.metadataConfidence,
                metadataPromptVersion: adWindow.metadataPromptVersion,
                evidenceSources: adWindow.evidenceSources,
                eligibilityGate: adWindow.eligibilityGate,
                catalogStoreMatchSimilarity: adWindow.catalogStoreMatchSimilarity
            )
        }

        static func exported(from adWindow: AdWindow) -> Window? {
            guard let decisionState = normalizedExportDecisionState(adWindow.decisionState) else {
                return nil
            }
            return Window(
                sourceWindowId: adWindow.id,
                startTime: adWindow.startTime,
                endTime: adWindow.endTime,
                confidence: adWindow.confidence,
                boundaryState: adWindow.boundaryState,
                decisionState: decisionState,
                isAd: isAdDecision(decisionState),
                detectorVersion: adWindow.detectorVersion,
                advertiser: adWindow.advertiser,
                product: adWindow.product,
                adDescription: adWindow.adDescription,
                metadataSource: adWindow.metadataSource,
                metadataConfidence: adWindow.metadataConfidence,
                metadataPromptVersion: adWindow.metadataPromptVersion,
                evidenceSources: adWindow.evidenceSources,
                eligibilityGate: adWindow.eligibilityGate,
                catalogStoreMatchSimilarity: adWindow.catalogStoreMatchSimilarity
            )
        }

        static func normalizedImportDecisionState(_ decisionState: String, isAd: Bool) -> String? {
            if !isAd, decisionState == AdDecisionState.suppressed.rawValue {
                return AdDecisionState.suppressed.rawValue
            }
            switch decisionState {
            case AdDecisionState.candidate.rawValue,
                 AdDecisionState.confirmed.rawValue:
                return isAd ? decisionState : nil
            default:
                return nil
            }
        }

        static func isValidSharedDecisionState(_ decisionState: String, isAd: Bool) -> Bool {
            if isAd {
                return decisionState == AdDecisionState.candidate.rawValue
                    || decisionState == AdDecisionState.confirmed.rawValue
            }
            return decisionState == AdDecisionState.suppressed.rawValue
        }

        static func isKnownExportDecisionState(_ decisionState: String) -> Bool {
            normalizedExportDecisionState(decisionState) != nil
                || decisionState == AdDecisionState.reverted.rawValue
        }

        private static func normalizedExportDecisionState(_ decisionState: String) -> String? {
            switch decisionState {
            case AdDecisionState.candidate.rawValue,
                 AdDecisionState.confirmed.rawValue,
                 AdDecisionState.suppressed.rawValue:
                return decisionState
            case AdDecisionState.applied.rawValue:
                return AdDecisionState.confirmed.rawValue
            default:
                return nil
            }
        }

        private static func isAdDecision(_ decisionState: String) -> Bool {
            switch decisionState {
            case AdDecisionState.candidate.rawValue,
                 AdDecisionState.confirmed.rawValue,
                 AdDecisionState.applied.rawValue:
                return true
            default:
                return false
            }
        }
    }
}

struct CrossUserAnalysisImportReceipt: Codable, Equatable, Sendable {
    let key: CrossUserAnalysisShareKey
    let targetAssetId: String
    let analysisCoverageEndSec: Double
    let insertedWindowIds: [String]
    let bannerEligibleWindowIds: [String]
    let insertedWindowCount: Int
    let insertedCueCount: Int
    let totalWindowCount: Int
    let cueCoverageSec: Double
    let fmMinutesSaved: Double?
    let queueToReadyLatencySec: Double?
    let batteryDeltaPercent: Double?
}

enum CrossUserAnalysisImportResult: Equatable, Sendable {
    case imported(CrossUserAnalysisImportReceipt)
    case mismatchedKey(expected: CrossUserAnalysisShareKey, actual: CrossUserAnalysisShareKey)
    case incompatibleSnapshot(reason: String)
    case localAssetMissing(targetAssetId: String)
}

protocol CrossUserAnalysisSharingProviding: Sendable {
    var isEnabled: Bool { get }
    func matchingSnapshot(for key: CrossUserAnalysisShareKey) async -> CrossUserAnalysisSnapshot?
    func publish(_ snapshot: CrossUserAnalysisSnapshot) async throws
    func didImportSharedAdWindows(_ windows: [AdWindow]) async
}

extension CrossUserAnalysisSharingProviding {
    func publish(_ snapshot: CrossUserAnalysisSnapshot) async throws {}
    func didImportSharedAdWindows(_ windows: [AdWindow]) async {}
}

struct NoOpCrossUserAnalysisSharingProvider: CrossUserAnalysisSharingProviding {
    let isEnabled = false

    func matchingSnapshot(for key: CrossUserAnalysisShareKey) async -> CrossUserAnalysisSnapshot? {
        nil
    }
}

struct FileBackedCrossUserAnalysisSharingProvider: CrossUserAnalysisSharingProviding {
    let directory: URL
    let isEnabled: Bool

    init(directory: URL, isEnabled: Bool = true) {
        self.directory = directory
        self.isEnabled = isEnabled
    }

    func matchingSnapshot(for key: CrossUserAnalysisShareKey) async -> CrossUserAnalysisSnapshot? {
        guard isEnabled else { return nil }
        do {
            let data = try Data(contentsOf: Self.fileURL(for: key, directory: directory))
            return try JSONDecoder().decode(CrossUserAnalysisSnapshot.self, from: data)
        } catch {
            return nil
        }
    }

    func publish(_ snapshot: CrossUserAnalysisSnapshot) async throws {
        guard isEnabled else { return }
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(snapshot)
        try data.write(
            to: Self.fileURL(for: snapshot.key, directory: directory),
            options: [.atomic]
        )
    }

    static func fileURL(for key: CrossUserAnalysisShareKey, directory: URL) -> URL {
        let seed = [key.podcastId, key.episodeId, key.fileSHA].joined(separator: "|")
        let digest = SHA256.hash(data: Data(seed.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return directory.appendingPathComponent("cross-user-analysis-\(hex).json")
    }
}

enum CrossUserAnalysisSharingConstants {
    static let cueConfidenceThreshold: Double = 0.7
    static let spanDedupToleranceSec: Double = 0.25
    static let coverageToleranceSec: Double = 0.000_001
}

extension AnalysisStore {
    func exportCrossUserAnalysisSnapshot(
        assetId: String,
        podcastId: String,
        exportedAt: Date = Date(),
        sourceAppBuild: String? = nil,
        measurements: CrossUserAnalysisMeasurements = CrossUserAnalysisMeasurements()
    ) throws -> CrossUserAnalysisSnapshot? {
        guard let asset = try fetchAsset(id: assetId) else { return nil }
        let key = CrossUserAnalysisShareKey(
            podcastId: podcastId,
            episodeId: asset.episodeId,
            fileSHA: asset.assetFingerprint
        )
        let adWindows = try fetchAdWindows(assetId: assetId)
        guard adWindows.allSatisfy({
            CrossUserAnalysisSnapshot.Window.isKnownExportDecisionState($0.decisionState)
        }) else {
            return nil
        }
        let windows = adWindows
            .compactMap(CrossUserAnalysisSnapshot.Window.exported(from:))
        guard windows.allSatisfy(Self.isValidSharedWindow) else {
            return nil
        }
        let windowCoverageEnd = Self.coverageEnd(from: windows)

        return CrossUserAnalysisSnapshot(
            key: key,
            provenance: CrossUserAnalysisProvenance(
                exportedAt: exportedAt.timeIntervalSince1970,
                sourceAnalysisVersion: asset.analysisVersion,
                sourceAppBuild: sourceAppBuild
            ),
            analysisCoverageEndSec: windowCoverageEnd,
            measurements: measurements,
            windows: windows
        )
    }

    func importCrossUserAnalysisSnapshot(
        _ snapshot: CrossUserAnalysisSnapshot,
        targetAssetId: String,
        podcastId: String
    ) throws -> CrossUserAnalysisImportResult {
        guard let asset = try fetchAsset(id: targetAssetId) else {
            return .localAssetMissing(targetAssetId: targetAssetId)
        }

        let expectedKey = CrossUserAnalysisShareKey(
            podcastId: podcastId,
            episodeId: asset.episodeId,
            fileSHA: asset.assetFingerprint
        )
        guard expectedKey == snapshot.key else {
            return .mismatchedKey(expected: expectedKey, actual: snapshot.key)
        }
        guard snapshot.schemaVersion == CrossUserAnalysisSnapshot.currentSchemaVersion else {
            return .incompatibleSnapshot(reason: "schemaVersion")
        }
        guard snapshot.provenance.sourceAnalysisVersion == asset.analysisVersion else {
            return .incompatibleSnapshot(reason: "analysisVersion")
        }
        guard snapshot.provenance.pipelineVersions == PipelineVersions.current() else {
            return .incompatibleSnapshot(reason: "pipelineVersions")
        }
        guard snapshot.analysisCoverageEndSec.isFinite, snapshot.analysisCoverageEndSec >= 0 else {
            return .incompatibleSnapshot(reason: "analysisCoverageEndSec")
        }
        if let invalidWindowIndex = snapshot.windows.firstIndex(where: { !Self.isValidSharedWindow($0) }) {
            return .incompatibleSnapshot(reason: "window[\(invalidWindowIndex)]")
        }
        let exportedWindowCoverageEnd = Self.coverageEnd(from: snapshot.windows)
        guard snapshot.analysisCoverageEndSec <= exportedWindowCoverageEnd + CrossUserAnalysisSharingConstants.coverageToleranceSec else {
            return .incompatibleSnapshot(reason: "analysisCoverageEndSec")
        }

        var existingWindows = try fetchAdWindows(assetId: targetAssetId)
        var existingIds = Set(existingWindows.map(\.id))
        var windowsToInsert: [AdWindow] = []
        var bannerEligibleWindowIds: [String] = []
        var seenBannerEligibleWindowIds = Set<String>()

        func rememberBannerEligibleWindowId(_ id: String) {
            guard seenBannerEligibleWindowIds.insert(id).inserted else { return }
            bannerEligibleWindowIds.append(id)
        }

        for window in snapshot.windows {
            guard let decisionState = CrossUserAnalysisSnapshot.Window.normalizedImportDecisionState(
                window.decisionState,
                isAd: window.isAd
            ) else {
                continue
            }
            let sharedWindowIsCueEligible = Self.isCueWindow(
                window,
                normalizedDecisionState: decisionState
            )
            let id = Self.importedAdWindowId(
                key: snapshot.key,
                window: window,
                targetAssetId: targetAssetId
            )
            if existingIds.contains(id) {
                if sharedWindowIsCueEligible,
                   let existing = existingWindows.first(where: { $0.id == id }),
                   existing.decisionState == AdDecisionState.suppressed.rawValue {
                    let supersedingId = Self.supersedingImportedAdWindowId(
                        key: snapshot.key,
                        window: window,
                        targetAssetId: targetAssetId
                    )
                    if existingIds.contains(supersedingId) {
                        rememberBannerEligibleWindowId(supersedingId)
                        continue
                    }
                    guard !Self.hasEquivalentCueSpan(window, in: existingWindows) else {
                        continue
                    }
                    let adWindow = Self.importedAdWindow(
                        id: supersedingId,
                        window: window,
                        targetAssetId: targetAssetId,
                        decisionState: decisionState
                    )
                    windowsToInsert.append(adWindow)
                    existingWindows.append(adWindow)
                    existingIds.insert(supersedingId)
                    rememberBannerEligibleWindowId(supersedingId)
                } else if sharedWindowIsCueEligible {
                    rememberBannerEligibleWindowId(id)
                }
                continue
            }
            if sharedWindowIsCueEligible {
                guard !Self.hasEquivalentCueSpan(window, in: existingWindows) else {
                    continue
                }
            } else {
                guard !Self.hasEquivalentSpan(window, in: existingWindows) else {
                    continue
                }
            }

            let adWindow = Self.importedAdWindow(
                id: id,
                window: window,
                targetAssetId: targetAssetId,
                decisionState: decisionState
            )
            windowsToInsert.append(adWindow)
            existingWindows.append(adWindow)
            existingIds.insert(id)
            if sharedWindowIsCueEligible {
                rememberBannerEligibleWindowId(id)
            }
        }

        if !windowsToInsert.isEmpty {
            try insertAdWindows(windowsToInsert)
        }

        let finalWindows = try fetchAdWindows(assetId: targetAssetId)
        let cueCoverage = Self.cueCoverage(from: finalWindows)
        if cueCoverage > (asset.confirmedAdCoverageEndTime ?? 0) {
            try updateConfirmedAdCoverage(id: targetAssetId, endTime: cueCoverage)
        }

        return .imported(CrossUserAnalysisImportReceipt(
            key: snapshot.key,
            targetAssetId: targetAssetId,
            analysisCoverageEndSec: snapshot.analysisCoverageEndSec,
            insertedWindowIds: windowsToInsert.map(\.id),
            bannerEligibleWindowIds: bannerEligibleWindowIds,
            insertedWindowCount: windowsToInsert.count,
            insertedCueCount: windowsToInsert.filter(Self.isCueWindow).count,
            totalWindowCount: finalWindows.count,
            cueCoverageSec: cueCoverage,
            fmMinutesSaved: snapshot.measurements.fmMinutesSaved,
            queueToReadyLatencySec: snapshot.measurements.queueToReadyLatencySec,
            batteryDeltaPercent: snapshot.measurements.batteryDeltaPercent
        ))
    }

    private static func coverageEnd(from windows: [CrossUserAnalysisSnapshot.Window]) -> Double {
        windows
            .map(\.endTime)
            .filter(\.isFinite)
            .max() ?? 0
    }

    private static func importedAdWindowId(
        key: CrossUserAnalysisShareKey,
        window: CrossUserAnalysisSnapshot.Window,
        targetAssetId: String
    ) -> String {
        let seed = [
            key.podcastId,
            key.episodeId,
            key.fileSHA,
            targetAssetId,
            window.sourceWindowId,
            String(format: "%.6f", window.startTime),
            String(format: "%.6f", window.endTime),
        ].joined(separator: "|")
        let digest = SHA256.hash(data: Data(seed.utf8))
        let hex = digest.prefix(16).map { String(format: "%02x", $0) }.joined()
        return "shared-\(hex)"
    }

    private static func supersedingImportedAdWindowId(
        key: CrossUserAnalysisShareKey,
        window: CrossUserAnalysisSnapshot.Window,
        targetAssetId: String
    ) -> String {
        let seed = [
            key.podcastId,
            key.episodeId,
            key.fileSHA,
            targetAssetId,
            window.sourceWindowId,
            String(format: "%.6f", window.startTime),
            String(format: "%.6f", window.endTime),
            "cue-supersedes-non-ad",
        ].joined(separator: "|")
        let digest = SHA256.hash(data: Data(seed.utf8))
        let hex = digest.prefix(16).map { String(format: "%02x", $0) }.joined()
        return "shared-\(hex)"
    }

    private static func importedAdWindow(
        id: String,
        window: CrossUserAnalysisSnapshot.Window,
        targetAssetId: String,
        decisionState: String
    ) -> AdWindow {
        AdWindow(
            id: id,
            analysisAssetId: targetAssetId,
            startTime: window.startTime,
            endTime: window.endTime,
            confidence: min(max(window.confidence, 0), 1),
            boundaryState: window.boundaryState,
            decisionState: decisionState,
            detectorVersion: window.detectorVersion,
            advertiser: window.advertiser,
            product: window.product,
            adDescription: window.adDescription,
            evidenceText: nil,
            evidenceStartTime: nil,
            metadataSource: window.metadataSource,
            metadataConfidence: window.metadataConfidence,
            metadataPromptVersion: window.metadataPromptVersion,
            wasSkipped: false,
            userDismissedBanner: false,
            evidenceSources: window.evidenceSources,
            eligibilityGate: window.eligibilityGate,
            catalogStoreMatchSimilarity: window.catalogStoreMatchSimilarity
        )
    }

    private static func cueCoverage(from windows: [AdWindow]) -> Double {
        windows
            .filter(isCueWindow)
            .map(\.endTime)
            .max() ?? 0
    }

    private static func isValidSharedWindow(_ window: CrossUserAnalysisSnapshot.Window) -> Bool {
        window.startTime.isFinite
            && window.endTime.isFinite
            && window.confidence.isFinite
            && window.startTime >= 0
            && window.endTime > window.startTime
            && (0...1).contains(window.confidence)
            && window.metadataConfidence.map { $0.isFinite && (0...1).contains($0) } ?? true
            && window.catalogStoreMatchSimilarity.map { $0.isFinite && (0...1).contains($0) } ?? true
            && CrossUserAnalysisSnapshot.Window.isValidSharedDecisionState(
                window.decisionState,
                isAd: window.isAd
            )
    }

    private static func hasEquivalentSpan(
        _ window: CrossUserAnalysisSnapshot.Window,
        in existingWindows: [AdWindow]
    ) -> Bool {
        existingWindows.contains { existing in
            abs(existing.startTime - window.startTime) <= CrossUserAnalysisSharingConstants.spanDedupToleranceSec
                && abs(existing.endTime - window.endTime) <= CrossUserAnalysisSharingConstants.spanDedupToleranceSec
        }
    }

    private static func hasEquivalentCueSpan(
        _ window: CrossUserAnalysisSnapshot.Window,
        in existingWindows: [AdWindow]
    ) -> Bool {
        existingWindows.contains { existing in
            isCueWindow(existing)
                && abs(existing.startTime - window.startTime) <= CrossUserAnalysisSharingConstants.spanDedupToleranceSec
                && abs(existing.endTime - window.endTime) <= CrossUserAnalysisSharingConstants.spanDedupToleranceSec
        }
    }

    private static func isCueWindow(
        _ window: CrossUserAnalysisSnapshot.Window,
        normalizedDecisionState: String
    ) -> Bool {
        window.confidence >= CrossUserAnalysisSharingConstants.cueConfidenceThreshold
            && window.endTime > window.startTime
            && (
                normalizedDecisionState == AdDecisionState.candidate.rawValue
                    || normalizedDecisionState == AdDecisionState.confirmed.rawValue
                    || normalizedDecisionState == AdDecisionState.applied.rawValue
            )
    }

    private static func isCueWindow(_ window: AdWindow) -> Bool {
        window.confidence >= CrossUserAnalysisSharingConstants.cueConfidenceThreshold
            && window.endTime > window.startTime
            && (
                window.decisionState == AdDecisionState.candidate.rawValue
                    || window.decisionState == AdDecisionState.confirmed.rawValue
                    || window.decisionState == AdDecisionState.applied.rawValue
            )
    }
}
