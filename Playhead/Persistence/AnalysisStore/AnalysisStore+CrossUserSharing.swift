// AnalysisStore+CrossUserSharing.swift
// Phase A cross-user sharing for derived ad-window analysis keyed by
// (podcastId, full-file SHA, analysisVersion).

import CryptoKit
import Foundation

struct CrossUserAnalysisShareKey: Codable, Equatable, Hashable, Sendable {
    let podcastId: String
    let fileSHA: String
    let analysisVersion: Int

    var isCanonicalShareKey: Bool {
        Self.validationFailureReason(
            podcastId: podcastId,
            fileSHA: fileSHA,
            analysisVersion: analysisVersion
        ) == nil
    }

    static func make(
        podcastId: String,
        fileSHA: String,
        analysisVersion: Int
    ) -> CrossUserAnalysisShareKey? {
        guard validationFailureReason(
            podcastId: podcastId,
            fileSHA: fileSHA,
            analysisVersion: analysisVersion
        ) == nil else {
            return nil
        }
        guard let normalizedFileSHA = normalizedFullFileSHA(fileSHA) else {
            return nil
        }
        return CrossUserAnalysisShareKey(
            podcastId: podcastId,
            fileSHA: normalizedFileSHA,
            analysisVersion: analysisVersion
        )
    }

    static func validationFailureReason(
        podcastId: String,
        fileSHA: String,
        analysisVersion: Int
    ) -> String? {
        guard isCanonicalTupleComponent(podcastId) else { return "podcastId" }
        guard normalizedFullFileSHA(fileSHA) == fileSHA else { return "fileSHA" }
        guard analysisVersion > 0 else { return "analysisVersion" }
        return nil
    }

    static func isCanonicalFullFileSHA(_ value: String) -> Bool {
        normalizedFullFileSHA(value) == value
    }

    private static func isCanonicalTupleComponent(_ value: String) -> Bool {
        guard !value.utf8.contains(0) else { return false }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return !trimmed.isEmpty && trimmed == value
    }

    private static func normalizedFullFileSHA(_ value: String) -> String? {
        let normalized = value.lowercased()
        guard normalized.count == 64 else { return nil }
        guard normalized.unicodeScalars.allSatisfy({ scalar in
            (48...57).contains(scalar.value) || (97...102).contains(scalar.value)
        }) else {
            return nil
        }
        return normalized
    }
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
    /// V4 adds catalog match diagnostics to the wire contract. Entry UUIDs are
    /// device-local and are never exported as transferable authority; all
    /// catalog-assisted gates are demoted and stripped again on import. V3
    /// remains importable because the added fields are optional.
    ///
    /// V5 (playhead-ar60) adds `Window.skipConfidence`, splitting the
    /// ACTUATION number out of `Window.confidence`. Unlike V4 this bump is NOT
    /// merely bookkeeping for an optional field — it is what makes `nil`
    /// READABLE. In a V5 snapshot `skipConfidence == nil` means "the producer
    /// had one number and it is in `confidence`". In a V3/V4 snapshot the key
    /// is absent for BOTH that case and for a fusion row whose actuation
    /// number the exporter had put in `confidence`, and nothing on the wire
    /// distinguishes them. So the version is the only way an importer can tell
    /// whether `confidence` is unambiguous, and shipping the field without the
    /// bump would have reproduced, across the sharing boundary, exactly the
    /// two-quantities-one-column ambiguity this bead removed.
    ///
    /// V3/V4 stay importable and are treated conservatively: see
    /// `importedAdWindow`.
    static let currentSchemaVersion = 5
    static let supportedImportSchemaVersions: ClosedRange<Int> = 3...5

    /// The first wire version in which `Window.confidence` is unambiguously
    /// the DETECTION number and `Window.skipConfidence` carries actuation.
    static let firstSchemaVersionWithSplitConfidence = 5

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
        /// playhead-ar60: the ACTUATION number, split out of `confidence` in
        /// schema V47. Optional, so a snapshot written before the split
        /// decodes it as `nil` — which is exactly what those rows carried: one
        /// number, in `confidence`. Without it a shared fusion row would import
        /// with the exporter's DETECTION score standing in for its actuation
        /// number, i.e. more skip-eager on the importing device than on the one
        /// that produced it.
        ///
        /// Adding it DOES bump the wire version (4 → 5) even though the field
        /// is optional, because the version is the only thing that makes `nil`
        /// readable — see `CrossUserAnalysisSnapshot.currentSchemaVersion`.
        let skipConfidence: Double?
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
        let catalogFingerprintVersion: Int?
        let catalogMatchedEntryId: String?
        let catalogMatchedShowId: String?
        let catalogMatchedLearningSource: String?
        let catalogMatchedLearningLifecycle: String?

        init(
            sourceWindowId: String,
            startTime: Double,
            endTime: Double,
            confidence: Double,
            skipConfidence: Double? = nil,
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
            catalogStoreMatchSimilarity: Double?,
            catalogFingerprintVersion: Int? = nil,
            catalogMatchedEntryId: String? = nil,
            catalogMatchedShowId: String? = nil,
            catalogMatchedLearningSource: String? = nil,
            catalogMatchedLearningLifecycle: String? = nil
        ) {
            self.sourceWindowId = sourceWindowId
            self.startTime = startTime
            self.endTime = endTime
            self.confidence = confidence
            self.skipConfidence = skipConfidence
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
            self.catalogFingerprintVersion = catalogFingerprintVersion
            self.catalogMatchedEntryId = catalogMatchedEntryId
            self.catalogMatchedShowId = catalogMatchedShowId
            self.catalogMatchedLearningSource = catalogMatchedLearningSource
            self.catalogMatchedLearningLifecycle =
                catalogMatchedLearningLifecycle
        }

        init(adWindow: AdWindow) {
            self.init(
                sourceWindowId: adWindow.id,
                startTime: adWindow.startTime,
                endTime: adWindow.endTime,
                confidence: adWindow.confidence,
                skipConfidence: adWindow.skipConfidence,
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
                catalogStoreMatchSimilarity:
                    adWindow.catalogStoreMatchSimilarity,
                catalogFingerprintVersion:
                    adWindow.catalogFingerprintVersion,
                // This UUID names a row in the exporting device's private
                // catalog. It has no identity on another device and must not
                // cross the sharing boundary.
                catalogMatchedEntryId: nil,
                catalogMatchedShowId: adWindow.catalogMatchedShowId,
                catalogMatchedLearningSource:
                    adWindow.catalogMatchedLearningSource,
                catalogMatchedLearningLifecycle:
                    adWindow.catalogMatchedLearningLifecycle
            )
        }

        static func exported(from adWindow: AdWindow) -> Window? {
            guard !AnalysisStore.isImportedSharedAdWindow(adWindow) else {
                return nil
            }
            guard let decisionState = normalizedExportDecisionState(adWindow.decisionState) else {
                return nil
            }
            guard let boundaryState = normalizedExportBoundaryState(adWindow.boundaryState) else {
                return nil
            }
            guard AnalysisStore.isShareableCueEligibilityGate(
                adWindow.eligibilityGate,
                isAd: isAdDecision(decisionState)
            ) else {
                return nil
            }
            return Window(
                sourceWindowId: adWindow.id,
                startTime: adWindow.startTime,
                endTime: adWindow.endTime,
                confidence: adWindow.confidence,
                skipConfidence: adWindow.skipConfidence,
                boundaryState: boundaryState,
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
                catalogStoreMatchSimilarity:
                    adWindow.catalogStoreMatchSimilarity,
                catalogFingerprintVersion:
                    adWindow.catalogFingerprintVersion,
                // This UUID names a row in the exporting device's private
                // catalog. It has no identity on another device and must not
                // cross the sharing boundary.
                catalogMatchedEntryId: nil,
                catalogMatchedShowId: adWindow.catalogMatchedShowId,
                catalogMatchedLearningSource:
                    adWindow.catalogMatchedLearningSource,
                catalogMatchedLearningLifecycle:
                    adWindow.catalogMatchedLearningLifecycle
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
                || decisionState == AdDecisionState.suppressed.rawValue
                || decisionState == AdDecisionState.reverted.rawValue
        }

        static func isKnownExportBoundaryState(_ boundaryState: String) -> Bool {
            normalizedExportBoundaryState(boundaryState) != nil
                || isLocalOnlyBoundaryState(boundaryState)
        }

        static func hasKnownExportDisposition(_ adWindow: AdWindow) -> Bool {
            if AnalysisStore.isImportedSharedAdWindow(adWindow) {
                return true
            }
            switch adWindow.decisionState {
            case AdDecisionState.suppressed.rawValue,
                 AdDecisionState.reverted.rawValue:
                return true
            default:
                return isKnownExportDecisionState(adWindow.decisionState)
                    && isKnownExportBoundaryState(adWindow.boundaryState)
            }
        }

        private static func normalizedExportDecisionState(_ decisionState: String) -> String? {
            switch decisionState {
            case AdDecisionState.candidate.rawValue,
                 AdDecisionState.confirmed.rawValue:
                return decisionState
            case AdDecisionState.applied.rawValue:
                return AdDecisionState.confirmed.rawValue
            default:
                return nil
            }
        }

        private static func normalizedExportBoundaryState(_ boundaryState: String) -> String? {
            AdBoundaryState(rawValue: boundaryState) == nil ? nil : boundaryState
        }

        private static func isLocalOnlyBoundaryState(_ boundaryState: String) -> Bool {
            switch boundaryState {
            case "userMarked",
                 "userConfirmedSuggested",
                 "correctionReplay",
                 // playhead-xsdz.36.4: day-0 byte-exact rediff marks are LOCAL
                 // ground truth for THIS user's DAI stitch (another user's stitch
                 // has different byte boundaries), so they are never exported —
                 // but they ARE a recognized disposition, so an asset carrying one
                 // does not abort its cross-user snapshot via
                 // `hasKnownExportDisposition`. Mirrors
                 // `AdDetectionService.dayZeroRediffByteExactBoundaryState`.
                 "dayZeroRediffByteExact",
                 // playhead-b6jq PR5 / playhead-xsdz.65: additive MARK-ONLY
                 // producers (`SpecialistMarkComposer`, `AdPodContinuation`) stamp
                 // boundary states that are not `AdBoundaryState` cases. They are
                 // local derived guesses and are never exported — but they must be
                 // RECOGNIZED, because `hasKnownExportDisposition` is an
                 // ALL-or-nothing gate: one unrecognized `candidate` row aborts the
                 // whole asset's cross-user snapshot silently, with no log and no
                 // error. Both producers ship flag-off, which is the only reason
                 // this never bit.
                 SpecialistMarkComposer.boundaryState,
                 AdPodContinuation.boundaryState,
                 // playhead-y3ya: a semantic-sweep mark is the same shape — an
                 // additive MARK-ONLY guess derived from THIS device's FM run,
                 // with a boundary state that is not an `AdBoundaryState` case.
                 // Listed here for the ALL-or-nothing reason above, and it is
                 // NOT covered by the flag-off caveat: this producer ships ON,
                 // so an omission would abort snapshots in the field.
                 SemanticSweepMarkComposer.boundaryState:
                return true
            default:
                return false
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
        guard isEnabled, key.isCanonicalShareKey else { return nil }
        do {
            let data = try Data(contentsOf: Self.fileURL(for: key, directory: directory))
            let snapshot = try JSONDecoder().decode(CrossUserAnalysisSnapshot.self, from: data)
            guard snapshot.key == key else { return nil }
            return snapshot
        } catch {
            return nil
        }
    }

    func publish(_ snapshot: CrossUserAnalysisSnapshot) async throws {
        guard isEnabled, snapshot.key.isCanonicalShareKey else { return }
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
        let seed = CrossUserAnalysisStableHash.seed([
            key.podcastId,
            key.fileSHA,
            String(key.analysisVersion),
        ])
        let digest = SHA256.hash(data: Data(seed.utf8))
        let hex = digest.map { String(format: "%02x", $0) }.joined()
        return directory.appendingPathComponent("cross-user-analysis-\(hex).json")
    }
}

private enum CrossUserAnalysisStableHash {
    static func seed(_ components: [String]) -> String {
        components
            .map { "\($0.utf8.count):\($0)" }
            .joined()
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
        guard let key = CrossUserAnalysisShareKey.make(
            podcastId: podcastId,
            fileSHA: asset.assetFingerprint,
            analysisVersion: asset.analysisVersion
        ) else { return nil }
        guard exportedAt.timeIntervalSince1970.isFinite,
              exportedAt.timeIntervalSince1970 >= 0,
              Self.isCanonicalOptionalString(sourceAppBuild),
              Self.isValidSharedMeasurements(measurements) else {
            return nil
        }
        guard let projectedWindows = try responseIndependentAdWindows(
            analysisAssetId: assetId
        )
        else {
            return nil
        }
        guard projectedWindows.allSatisfy(
            CrossUserAnalysisSnapshot.Window.hasKnownExportDisposition
        ) else {
            return nil
        }
        let windows = Self.exportableSnapshotPrefix(
            from: projectedWindows
        )
        guard !windows.isEmpty else {
            return nil
        }
        guard windows.allSatisfy(Self.isValidSharedWindow) else {
            return nil
        }
        let windowCoverageEnd = Self.coverageEnd(from: windows)
        guard Self.isSharedSnapshotCoverage(
            analysisCoverageEndSec: windowCoverageEnd,
            windows: windows,
            withinLocalDuration: asset.episodeDurationSec
        ) else {
            return nil
        }

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

        if let keyFailureReason = CrossUserAnalysisShareKey.validationFailureReason(
            podcastId: podcastId,
            fileSHA: asset.assetFingerprint,
            analysisVersion: asset.analysisVersion
        ) {
            return .incompatibleSnapshot(reason: keyFailureReason)
        }
        guard let expectedKey = CrossUserAnalysisShareKey.make(
            podcastId: podcastId,
            fileSHA: asset.assetFingerprint,
            analysisVersion: asset.analysisVersion
        ) else {
            return .incompatibleSnapshot(reason: "fileSHA")
        }
        guard expectedKey == snapshot.key else {
            return .mismatchedKey(expected: expectedKey, actual: snapshot.key)
        }
        guard CrossUserAnalysisSnapshot.supportedImportSchemaVersions
            .contains(snapshot.schemaVersion) else {
            return .incompatibleSnapshot(reason: "schemaVersion")
        }
        guard snapshot.provenance.exportedAt.isFinite,
              snapshot.provenance.exportedAt >= 0 else {
            return .incompatibleSnapshot(reason: "provenance.exportedAt")
        }
        guard Self.isCanonicalOptionalString(snapshot.provenance.sourceAppBuild) else {
            return .incompatibleSnapshot(reason: "provenance.sourceAppBuild")
        }
        guard snapshot.provenance.sourceAnalysisVersion == asset.analysisVersion else {
            return .incompatibleSnapshot(reason: "analysisVersion")
        }
        guard snapshot.provenance.pipelineVersions == PipelineVersions.current() else {
            return .incompatibleSnapshot(reason: "pipelineVersions")
        }
        guard Self.isValidSharedMeasurements(snapshot.measurements) else {
            return .incompatibleSnapshot(reason: "measurements")
        }
        guard snapshot.analysisCoverageEndSec.isFinite, snapshot.analysisCoverageEndSec >= 0 else {
            return .incompatibleSnapshot(reason: "analysisCoverageEndSec")
        }
        guard !snapshot.windows.isEmpty else {
            return .incompatibleSnapshot(reason: "windows")
        }
        if let invalidWindowIndex = snapshot.windows.firstIndex(where: { !Self.isValidSharedWindow($0) }) {
            return .incompatibleSnapshot(reason: "window[\(invalidWindowIndex)]")
        }
        if let duplicateWindowIndex = Self.duplicateSourceWindowIdIndex(in: snapshot.windows) {
            return .incompatibleSnapshot(reason: "window[\(duplicateWindowIndex)].sourceWindowId")
        }
        let exportedWindowCoverageEnd = Self.coverageEnd(from: snapshot.windows)
        guard snapshot.analysisCoverageEndSec <= exportedWindowCoverageEnd + CrossUserAnalysisSharingConstants.coverageToleranceSec else {
            return .incompatibleSnapshot(reason: "analysisCoverageEndSec")
        }
        guard exportedWindowCoverageEnd <= snapshot.analysisCoverageEndSec + CrossUserAnalysisSharingConstants.coverageToleranceSec else {
            return .incompatibleSnapshot(reason: "analysisCoverageEndSec")
        }
        guard Self.isSharedSnapshotCoverage(
            analysisCoverageEndSec: snapshot.analysisCoverageEndSec,
            windows: snapshot.windows,
            withinLocalDuration: asset.episodeDurationSec
        ) else {
            return .incompatibleSnapshot(reason: "episodeDurationSec")
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
            ) && !Self.requiresCatalogAuthorityDemotion(
                window,
                expectedShowId: snapshot.key.podcastId
            )
            let id = Self.importedAdWindowId(
                key: snapshot.key,
                window: window,
                targetAssetId: targetAssetId
            )
            if existingIds.contains(id) {
                if sharedWindowIsCueEligible,
                   let existing = existingWindows.first(where: { $0.id == id }),
                   Self.isImportedNonAdVerdict(existing) {
                    let supersedingId = Self.supersedingImportedAdWindowId(
                        key: snapshot.key,
                        window: window,
                        targetAssetId: targetAssetId
                    )
                    if existingIds.contains(supersedingId) {
                        rememberBannerEligibleWindowId(supersedingId)
                        continue
                    }
                    guard !Self.hasEquivalentCueSpan(
                        window,
                        in: existingWindows.filter { $0.id != id }
                    ) else {
                        continue
                    }
                    let adWindow = Self.importedAdWindow(
                        id: supersedingId,
                        window: window,
                        targetAssetId: targetAssetId,
                        expectedShowId: snapshot.key.podcastId,
                        decisionState: decisionState
                    )
                    windowsToInsert.append(adWindow)
                    existingWindows.append(adWindow)
                    existingIds.insert(supersedingId)
                    rememberBannerEligibleWindowId(supersedingId)
                } else if sharedWindowIsCueEligible,
                          let existing = existingWindows.first(where: { $0.id == id }),
                          Self.isCueWindow(existing) {
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
                expectedShowId: snapshot.key.podcastId,
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

    private static func exportableSnapshotPrefix(
        from adWindows: [AdWindow]
    ) -> [CrossUserAnalysisSnapshot.Window] {
        var windows: [CrossUserAnalysisSnapshot.Window] = []
        for adWindow in adWindows {
            guard let window = CrossUserAnalysisSnapshot.Window.exported(from: adWindow) else {
                break
            }
            windows.append(window)
        }
        return windows
    }

    private static func isSharedSnapshotCoverage(
        analysisCoverageEndSec: Double,
        windows: [CrossUserAnalysisSnapshot.Window],
        withinLocalDuration localDuration: Double?
    ) -> Bool {
        guard let localDuration, localDuration.isFinite, localDuration > 0 else { return false }
        let tolerance = CrossUserAnalysisSharingConstants.coverageToleranceSec
        guard analysisCoverageEndSec <= localDuration + tolerance else { return false }
        return windows.allSatisfy { window in
            window.endTime <= localDuration + tolerance
        }
    }

    private static func importedAdWindowId(
        key: CrossUserAnalysisShareKey,
        window: CrossUserAnalysisSnapshot.Window,
        targetAssetId: String
    ) -> String {
        let seed = CrossUserAnalysisStableHash.seed([
            key.podcastId,
            key.fileSHA,
            String(key.analysisVersion),
            targetAssetId,
            window.sourceWindowId,
            String(format: "%.6f", window.startTime),
            String(format: "%.6f", window.endTime),
        ])
        let digest = SHA256.hash(data: Data(seed.utf8))
        let hex = digest.prefix(16).map { String(format: "%02x", $0) }.joined()
        return "shared-\(hex)"
    }

    private static func supersedingImportedAdWindowId(
        key: CrossUserAnalysisShareKey,
        window: CrossUserAnalysisSnapshot.Window,
        targetAssetId: String
    ) -> String {
        let seed = CrossUserAnalysisStableHash.seed([
            key.podcastId,
            key.fileSHA,
            String(key.analysisVersion),
            targetAssetId,
            window.sourceWindowId,
            String(format: "%.6f", window.startTime),
            String(format: "%.6f", window.endTime),
            "cue-supersedes-non-ad",
        ])
        let digest = SHA256.hash(data: Data(seed.utf8))
        let hex = digest.prefix(16).map { String(format: "%02x", $0) }.joined()
        return "shared-\(hex)"
    }

    private static func importedAdWindow(
        id: String,
        window: CrossUserAnalysisSnapshot.Window,
        targetAssetId: String,
        expectedShowId: String,
        decisionState: String
    ) -> AdWindow {
        // A remote catalog row cannot be resolved against this device's
        // private catalog, even when its claimed show matches the snapshot
        // key. A mismatched claimed show is an additional fail-closed reason.
        let mustDemoteCatalogAuthority =
            requiresCatalogAuthorityDemotion(
                window,
                expectedShowId: expectedShowId
        )
        let importedEligibilityGate: String?
        if mustDemoteCatalogAuthority {
            // A legacy nil gate means "admit through the managed path" when
            // the row is later reloaded. It is therefore just as automatic as
            // an explicit eligible gate and must be replaced, not preserved.
            importedEligibilityGate = SkipEligibilityGate.markOnly.rawValue
        } else {
            importedEligibilityGate = window.eligibilityGate
        }
        return AdWindow(
            id: id,
            analysisAssetId: targetAssetId,
            startTime: window.startTime,
            endTime: window.endTime,
            confidence: min(max(window.confidence, 0), 1),
            // playhead-ar60: clamped like `confidence` above — this is the
            // one write path that clamps, and an untrusted peer's actuation
            // number gets exactly the same treatment as its detection score.
            //
            // A V3/V4 snapshot has no such key, so this is `nil` and
            // `actuationConfidence` falls back to `confidence`. That is the
            // CONSERVATIVE reading in both directions and needs no version
            // plumbing: if the exporter was pre-ar60 and the row was a fusion
            // row, `confidence` held its ACTUATION number — so the imported
            // row's actuation is exactly what the exporter meant, and its
            // detection reading is that same (lower) number, which can only
            // make the 0.7 cue floors stricter, never looser. The version bump
            // to 5 is what lets a FUTURE reader tell the two apart at all; see
            // `CrossUserAnalysisSnapshot.firstSchemaVersionWithSplitConfidence`.
            skipConfidence: window.skipConfidence.map { min(max($0, 0), 1) },
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
            eligibilityGate: importedEligibilityGate,
            catalogStoreMatchSimilarity:
                window.catalogStoreMatchSimilarity == 0 ? 0 : nil,
            catalogFingerprintVersion: nil,
            catalogMatchedEntryId: nil,
            catalogMatchedShowId: nil,
            catalogMatchedLearningSource: nil,
            catalogMatchedLearningLifecycle: nil
        )
    }

    private static func requiresCatalogAuthorityDemotion(
        _ window: CrossUserAnalysisSnapshot.Window,
        expectedShowId: String
    ) -> Bool {
        let catalogScopeMismatch = window.catalogMatchedShowId.map {
            $0 != expectedShowId
        } ?? false
        let hasCatalogEvidence =
            window.catalogStoreMatchSimilarity.map { $0 != 0 } == true
            || window.catalogFingerprintVersion != nil
            || window.catalogMatchedEntryId != nil
            || window.catalogMatchedShowId != nil
            || window.catalogMatchedLearningSource != nil
            || window.catalogMatchedLearningLifecycle != nil
            || evidenceSourcesClaimCatalog(window.evidenceSources)
        return hasCatalogEvidence || catalogScopeMismatch
    }

    /// Evidence sources have existed as both comma-separated text and a JSON
    /// string array. Tokenizing on non-alphanumerics recognizes either form
    /// (and conservative malformed variants) without treating a source such
    /// as `transcriptCatalog` as the device-local `catalog` channel.
    private static func evidenceSourcesClaimCatalog(_ rawValue: String?) -> Bool {
        guard let rawValue else { return false }
        return rawValue
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .contains { $0.caseInsensitiveCompare("catalog") == .orderedSame }
    }

    private static func cueCoverage(from windows: [AdWindow]) -> Double {
        windows
            .filter(isCueWindow)
            .map(\.endTime)
            .max() ?? 0
    }

    private static func isValidSharedWindow(_ window: CrossUserAnalysisSnapshot.Window) -> Bool {
        hasCanonicalRequiredString(window.sourceWindowId)
            && hasCanonicalRequiredString(window.detectorVersion)
            && hasCanonicalRequiredString(window.metadataSource)
            && isCanonicalOptionalString(window.advertiser)
            && isCanonicalOptionalString(window.product)
            && isCanonicalOptionalString(window.adDescription)
            && isCanonicalOptionalString(window.metadataPromptVersion)
            && isCanonicalOptionalString(window.evidenceSources)
            && isCanonicalOptionalString(window.eligibilityGate)
            && isCanonicalOptionalString(window.catalogMatchedEntryId)
            && isCanonicalOptionalString(window.catalogMatchedShowId)
            && isCanonicalOptionalString(
                window.catalogMatchedLearningSource
            )
            && isCanonicalOptionalString(
                window.catalogMatchedLearningLifecycle
            )
            && window.startTime.isFinite
            && window.endTime.isFinite
            && window.confidence.isFinite
            && window.startTime >= 0
            && window.endTime > window.startTime
            && (0...1).contains(window.confidence)
            && AdBoundaryState(rawValue: window.boundaryState) != nil
            && window.skipConfidence.map { $0.isFinite && (0...1).contains($0) } ?? true
            && window.metadataConfidence.map { $0.isFinite && (0...1).contains($0) } ?? true
            && window.catalogStoreMatchSimilarity.map { $0.isFinite && (0...1).contains($0) } ?? true
            && window.catalogFingerprintVersion.map {
                CatalogFingerprintVersion(rawValue: $0) != nil
            } ?? true
            && CrossUserAnalysisSnapshot.Window.isValidSharedDecisionState(
                window.decisionState,
                isAd: window.isAd
            )
            && isShareableCueEligibilityGate(window.eligibilityGate, isAd: window.isAd)
            && (window.isAd || !hasAdMetadata(window))
    }

    fileprivate static func isShareableCueEligibilityGate(
        _ eligibilityGate: String?,
        isAd: Bool
    ) -> Bool {
        guard let eligibilityGate else { return true }
        if eligibilityGate == "autoSkip" {
            return isAd
        }
        guard let decoded = SkipEligibilityGate(rawValue: eligibilityGate) else {
            return false
        }
        return isAd && decoded == .eligible
    }

    private static func hasCanonicalRequiredString(_ value: String) -> Bool {
        guard !value.utf8.contains(0) else { return false }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return !trimmed.isEmpty && trimmed == value
    }

    private static func isCanonicalOptionalString(_ value: String?) -> Bool {
        guard let value else { return true }
        return hasCanonicalRequiredString(value)
    }

    private static func duplicateSourceWindowIdIndex(
        in windows: [CrossUserAnalysisSnapshot.Window]
    ) -> Int? {
        var seen = Set<String>()
        for (index, window) in windows.enumerated() {
            guard seen.insert(window.sourceWindowId).inserted else {
                return index
            }
        }
        return nil
    }

    private static func isValidSharedMeasurements(
        _ measurements: CrossUserAnalysisMeasurements
    ) -> Bool {
        measurements.fmMinutesSaved.map { $0.isFinite && $0 >= 0 } ?? true
            && measurements.queueToReadyLatencySec.map { $0.isFinite && $0 >= 0 } ?? true
            && measurements.batteryDeltaPercent.map(\.isFinite) ?? true
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
            protectsEquivalentCueSpan(existing)
                && abs(existing.startTime - window.startTime) <= CrossUserAnalysisSharingConstants.spanDedupToleranceSec
                && abs(existing.endTime - window.endTime) <= CrossUserAnalysisSharingConstants.spanDedupToleranceSec
        }
    }

    private static func protectsEquivalentCueSpan(_ window: AdWindow) -> Bool {
        isCueWindow(window)
            || window.decisionState == AdDecisionState.reverted.rawValue
            || (
                window.decisionState == AdDecisionState.suppressed.rawValue
                    && !isImportedNonAdVerdict(window)
            )
    }

    private static func hasAdMetadata(_ window: CrossUserAnalysisSnapshot.Window) -> Bool {
        window.advertiser != nil
            || window.product != nil
            || window.adDescription != nil
    }

    private static func isImportedNonAdVerdict(_ window: AdWindow) -> Bool {
        isImportedSharedAdWindow(window)
            && window.decisionState == AdDecisionState.suppressed.rawValue
            && window.advertiser == nil
            && window.product == nil
            && window.adDescription == nil
    }

    fileprivate static func isImportedSharedAdWindow(_ window: AdWindow) -> Bool {
        window.id.hasPrefix("shared-")
    }

    private static func isCueWindow(
        _ window: CrossUserAnalysisSnapshot.Window,
        normalizedDecisionState: String
    ) -> Bool {
        // playhead-ar60: DETECTION — a shareable-cue COUNT, not a skip. Same
        // reading as `AnalysisJobRunner.isCueWindow`.
        window.confidence >= CrossUserAnalysisSharingConstants.cueConfidenceThreshold
            && window.endTime > window.startTime
            && isShareableCueEligibilityGate(window.eligibilityGate, isAd: window.isAd)
            && (
                normalizedDecisionState == AdDecisionState.candidate.rawValue
                    || normalizedDecisionState == AdDecisionState.confirmed.rawValue
                    || normalizedDecisionState == AdDecisionState.applied.rawValue
            )
    }

    private static func isCueWindow(_ window: AdWindow) -> Bool {
        // playhead-ar60: DETECTION — a shareable-cue COUNT, not a skip. Same
        // reading as `AnalysisJobRunner.isCueWindow`.
        window.confidence >= CrossUserAnalysisSharingConstants.cueConfidenceThreshold
            && window.endTime > window.startTime
            && isShareableCueEligibilityGate(window.eligibilityGate, isAd: true)
            && (
                window.decisionState == AdDecisionState.candidate.rawValue
                    || window.decisionState == AdDecisionState.confirmed.rawValue
                    || window.decisionState == AdDecisionState.applied.rawValue
            )
    }
}
