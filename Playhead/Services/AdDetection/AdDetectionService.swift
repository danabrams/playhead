// AdDetectionService.swift
// Composes the detection layers and outputs AdWindows.
//
// Hot path: lexical scan -> acoustic boundary refinement -> classifier
//   Produces skip-ready AdWindows with decisionState = .candidate
//   ahead of the playhead.
//
// Backfill: re-classify on final-pass transcript -> metadata extraction
//   -> prior update -> promote to .confirmed or .suppressed.
//
// Results keyed by analysisAssetId in SQLite. Different audio bytes =
// different AnalysisAsset = fresh analysis (no stale cache).

import Foundation
import OSLog

// MARK: - Detection Configuration

struct AdDetectionConfig: Sendable {
    /// Minimum classifier probability to emit a candidate AdWindow.
    let candidateThreshold: Double
    /// Minimum classifier probability to auto-confirm during backfill.
    let confirmationThreshold: Double
    /// Maximum probability below which a candidate is suppressed.
    let suppressionThreshold: Double
    /// How far ahead of the playhead (seconds) to run hot-path detection.
    let hotPathLookahead: TimeInterval
    /// Detector version tag written to each AdWindow.
    let detectorVersion: String

    static let `default` = AdDetectionConfig(
        candidateThreshold: 0.40,
        confirmationThreshold: 0.70,
        suppressionThreshold: 0.25,
        hotPathLookahead: 90.0,
        detectorVersion: "detection-v1"
    )
}

// MARK: - Decision State

/// Lifecycle of an AdWindow from detection through confirmation.
enum AdDecisionState: String, Sendable {
    /// Initial detection from hot path -- skip-ready but not yet confirmed.
    case candidate
    /// Confirmed by backfill re-classification with full context.
    case confirmed
    /// Suppressed: below threshold after backfill re-classification.
    case suppressed
    /// Skip was applied to the listener (audio was skipped).
    case applied
    /// User tapped "Listen" — skip reverted, plays through the ad.
    case reverted
}

// MARK: - Boundary State

/// How the window boundaries were derived.
enum AdBoundaryState: String, Sendable {
    /// Rough boundaries from lexical scanner only.
    case lexical
    /// Boundaries refined using acoustic feature transitions.
    case acousticRefined
}

// MARK: - AdDetectionService

/// Composes LexicalScanner (Layer 1), acoustic boundary refinement (Layer 0),
/// ClassifierService (Layer 2), and MetadataExtractor (Layer 3) into a
/// unified detection pipeline with hot-path and backfill flows.
actor AdDetectionService {

    private let logger = Logger(subsystem: "com.playhead", category: "AdDetectionService")

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let classifier: ClassifierService
    private let metadataExtractor: MetadataExtractor
    private let config: AdDetectionConfig

    // MARK: - Cached State

    /// Scanner is recreated per-episode when profile changes.
    private var scanner: LexicalScanner
    /// Per-show priors parsed from the current PodcastProfile.
    private var showPriors: ShowPriors
    /// Episode duration for position-based scoring.
    private var episodeDuration: Double = 0

    // MARK: - Init

    init(
        store: AnalysisStore,
        classifier: ClassifierService = RuleBasedClassifier(),
        metadataExtractor: MetadataExtractor,
        config: AdDetectionConfig = .default,
        podcastProfile: PodcastProfile? = nil
    ) {
        self.store = store
        self.classifier = classifier
        self.metadataExtractor = metadataExtractor
        self.config = config
        self.scanner = LexicalScanner(podcastProfile: podcastProfile)
        self.showPriors = ShowPriors.from(profile: podcastProfile)
    }

    // MARK: - Profile Update

    /// Update the scanner and priors when the podcast profile changes.
    func updateProfile(_ profile: PodcastProfile?) {
        scanner = LexicalScanner(podcastProfile: profile)
        showPriors = ShowPriors.from(profile: profile)
    }

    // MARK: - Hot Path

    /// Run the hot-path detection pipeline on fast-pass transcript chunks
    /// and feature windows. Produces candidate AdWindows ahead of the playhead.
    ///
    /// Flow:
    ///   1. LexicalScanner -> candidate regions from transcript
    ///   2. Fetch overlapping FeatureWindows from SQLite
    ///   3. ClassifierService -> scored results with boundary refinement
    ///   4. Filter by candidateThreshold and persist as AdWindows
    ///   5. Return new AdWindows for SkipOrchestrator
    ///
    /// - Parameters:
    ///   - chunks: Fast-pass TranscriptChunks from TranscriptEngineService.
    ///   - analysisAssetId: The analysis asset being processed.
    ///   - episodeDuration: Total episode duration in seconds.
    /// - Returns: Newly detected AdWindows with decisionState = .candidate.
    func runHotPath(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        episodeDuration: Double
    ) async throws -> [AdWindow] {
        self.episodeDuration = episodeDuration
        guard !chunks.isEmpty else { return [] }

        // Layer 1: Lexical scan for candidate regions.
        let lexicalCandidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: analysisAssetId
        )

        guard !lexicalCandidates.isEmpty else {
            logger.info("Hot path: no lexical candidates from \(chunks.count) chunks")
            return []
        }

        logger.info("Hot path: \(lexicalCandidates.count) lexical candidates from \(chunks.count) chunks")

        // Layer 0 + Layer 2: Fetch features, classify, refine boundaries.
        let classifierResults = try await classifyCandidates(
            lexicalCandidates,
            analysisAssetId: analysisAssetId
        )

        // Filter by candidate threshold and build AdWindows.
        let adWindows = classifierResults
            .filter { $0.adProbability >= config.candidateThreshold }
            .map { result in
                buildAdWindow(
                    from: result,
                    boundaryState: .acousticRefined,
                    decisionState: .candidate,
                    evidenceText: lexicalCandidates
                        .first { $0.id == result.candidateId }?.evidenceText
                )
            }

        guard !adWindows.isEmpty else {
            logger.info("Hot path: all \(classifierResults.count) results below threshold")
            return []
        }

        // Persist to SQLite.
        try await store.insertAdWindows(adWindows)

        logger.info("Hot path: persisted \(adWindows.count) candidate AdWindows")

        return adWindows
    }

    // MARK: - Backfill

    /// Run the backfill pipeline: re-classify with final-pass transcript,
    /// extract metadata, update priors, promote/suppress candidates.
    ///
    /// Flow:
    ///   1. Re-run lexical scan on final-pass transcript chunks
    ///   2. Re-classify with full context
    ///   3. Promote high-confidence to .confirmed, suppress low-confidence
    ///   4. Run Layer 3 (metadata extraction) on confirmed windows
    ///   5. Update PodcastProfile priors
    ///
    /// - Parameters:
    ///   - chunks: Final-pass TranscriptChunks (full episode).
    ///   - analysisAssetId: The analysis asset being processed.
    ///   - podcastId: Podcast ID for profile prior updates.
    ///   - episodeDuration: Total episode duration in seconds.
    func runBackfill(
        chunks: [TranscriptChunk],
        analysisAssetId: String,
        podcastId: String,
        episodeDuration: Double
    ) async throws {
        self.episodeDuration = episodeDuration
        guard !chunks.isEmpty else { return }

        // 1. Re-run lexical scan on final transcript.
        let lexicalCandidates = scanner.scan(
            chunks: chunks,
            analysisAssetId: analysisAssetId
        )

        logger.info("Backfill: \(lexicalCandidates.count) lexical candidates from \(chunks.count) final chunks")

        // 2. Re-classify with full context.
        let classifierResults: [ClassifierResult]
        if !lexicalCandidates.isEmpty {
            classifierResults = try await classifyCandidates(
                lexicalCandidates,
                analysisAssetId: analysisAssetId
            )
        } else {
            classifierResults = []
        }

        // 3. Load existing candidate AdWindows for this asset.
        let existingWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
        let existingCandidates = existingWindows.filter {
            $0.decisionState == AdDecisionState.candidate.rawValue
        }

        // 4. Promote or suppress each existing candidate based on backfill results.
        var confirmedWindowIds: [String] = []
        for existing in existingCandidates {
            try Task.checkCancellation()

            let newDecision = resolveDecision(
                existing: existing,
                backfillResults: classifierResults
            )

            if newDecision != existing.decisionState {
                try await store.updateAdWindowDecision(
                    id: existing.id,
                    decisionState: newDecision
                )
            }

            if newDecision == AdDecisionState.confirmed.rawValue {
                confirmedWindowIds.append(existing.id)
            }
        }

        // 5. Insert any new backfill-only detections above confirmation threshold.
        let newBackfillWindows = buildNewBackfillWindows(
            classifierResults: classifierResults,
            existingWindows: existingWindows,
            analysisAssetId: analysisAssetId,
            lexicalCandidates: lexicalCandidates
        )
        if !newBackfillWindows.isEmpty {
            try await store.insertAdWindows(newBackfillWindows)
            confirmedWindowIds.append(contentsOf: newBackfillWindows.map(\.id))
            logger.info("Backfill: inserted \(newBackfillWindows.count) new confirmed windows")
        }

        // 6. Run Layer 3 metadata extraction on confirmed windows.
        let allWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
        let confirmedWindows = allWindows.filter {
            $0.decisionState == AdDecisionState.confirmed.rawValue
        }

        for window in confirmedWindows {
            try Task.checkCancellation()
            await extractAndPersistMetadata(
                window: window,
                chunks: chunks
            )
        }

        // 7. Update PodcastProfile priors from confirmed results.
        try await updatePriors(
            podcastId: podcastId,
            confirmedWindows: confirmedWindows,
            episodeDuration: episodeDuration
        )

        // 8. Update coverage watermark.
        if let maxEnd = confirmedWindows.map(\.endTime).max() {
            try await store.updateConfirmedAdCoverage(
                id: analysisAssetId,
                endTime: maxEnd
            )
        }

        logger.info("Backfill complete: \(confirmedWindows.count) confirmed, \(existingCandidates.count - confirmedWindowIds.count) suppressed")
    }

    // MARK: - User Behavior Feedback

    /// Record that the user rewound back into a skipped ad window,
    /// signaling a potential false positive. Updates the podcast profile.
    func recordListenRewind(
        windowId: String,
        podcastId: String
    ) async throws {
        // Revert the window (user tapped "Listen" to play through).
        try await store.updateAdWindowDecision(
            id: windowId,
            decisionState: AdDecisionState.reverted.rawValue
        )

        // Increment false-positive signal on the profile.
        guard let profile = try await store.fetchProfile(podcastId: podcastId) else {
            logger.warning("No profile found for podcast \(podcastId) during listen-rewind recording")
            return
        }
        let updatedProfile = PodcastProfile(
            podcastId: profile.podcastId,
            sponsorLexicon: profile.sponsorLexicon,
            normalizedAdSlotPriors: profile.normalizedAdSlotPriors,
            repeatedCTAFragments: profile.repeatedCTAFragments,
            jingleFingerprints: profile.jingleFingerprints,
            implicitFalsePositiveCount: profile.implicitFalsePositiveCount + 1,
            skipTrustScore: max(0, profile.skipTrustScore - 0.05),
            observationCount: profile.observationCount,
            mode: profile.mode,
            recentFalseSkipSignals: profile.recentFalseSkipSignals + 1
        )
        try await store.upsertProfile(updatedProfile)

        logger.info("Recorded listen-rewind for window \(windowId), podcast \(podcastId)")
    }

    // MARK: - Classification Pipeline

    /// Fetch feature windows for each lexical candidate and run the classifier.
    private func classifyCandidates(
        _ candidates: [LexicalCandidate],
        analysisAssetId: String
    ) async throws -> [ClassifierResult] {
        var inputs: [ClassifierInput] = []

        for candidate in candidates {
            // Layer 0: Fetch acoustic features overlapping this candidate.
            // Extend the search range slightly to allow boundary snapping.
            let margin = 5.0
            let featureWindows = try await store.fetchFeatureWindows(
                assetId: analysisAssetId,
                from: candidate.startTime - margin,
                to: candidate.endTime + margin
            )

            inputs.append(ClassifierInput(
                candidate: candidate,
                featureWindows: featureWindows,
                episodeDuration: episodeDuration
            ))
        }

        // Layer 2: Classify all candidates.
        return classifier.classify(inputs: inputs, priors: showPriors)
    }

    // MARK: - Decision Resolution

    /// Determine whether to confirm or suppress an existing candidate
    /// based on backfill classifier results.
    private func resolveDecision(
        existing: AdWindow,
        backfillResults: [ClassifierResult]
    ) -> String {
        // Find the backfill result that best overlaps this window.
        let bestMatch = backfillResults.first { result in
            let overlapStart = max(existing.startTime, result.startTime)
            let overlapEnd = min(existing.endTime, result.endTime)
            return overlapEnd - overlapStart > 0
        }

        guard let match = bestMatch else {
            // No backfill result overlaps -- suppress if confidence was borderline.
            if existing.confidence < config.confirmationThreshold {
                return AdDecisionState.suppressed.rawValue
            }
            return existing.decisionState
        }

        if match.adProbability >= config.confirmationThreshold {
            return AdDecisionState.confirmed.rawValue
        } else if match.adProbability < config.suppressionThreshold {
            return AdDecisionState.suppressed.rawValue
        }

        // Between suppression and confirmation: keep as candidate.
        return existing.decisionState
    }

    // MARK: - New Backfill Windows

    /// Build AdWindows for backfill-only detections that don't overlap
    /// any existing window.
    private func buildNewBackfillWindows(
        classifierResults: [ClassifierResult],
        existingWindows: [AdWindow],
        analysisAssetId: String,
        lexicalCandidates: [LexicalCandidate]
    ) -> [AdWindow] {
        classifierResults
            .filter { result in
                result.adProbability >= config.confirmationThreshold
                    && !overlapsExisting(result: result, existing: existingWindows)
            }
            .map { result in
                buildAdWindow(
                    from: result,
                    boundaryState: .acousticRefined,
                    decisionState: .confirmed,
                    evidenceText: lexicalCandidates
                        .first { $0.id == result.candidateId }?.evidenceText
                )
            }
    }

    /// Check whether a classifier result overlaps any existing AdWindow.
    private func overlapsExisting(
        result: ClassifierResult,
        existing: [AdWindow]
    ) -> Bool {
        existing.contains { window in
            let overlapStart = max(window.startTime, result.startTime)
            let overlapEnd = min(window.endTime, result.endTime)
            return overlapEnd - overlapStart > 0
        }
    }

    // MARK: - AdWindow Construction

    private func buildAdWindow(
        from result: ClassifierResult,
        boundaryState: AdBoundaryState,
        decisionState: AdDecisionState,
        evidenceText: String?
    ) -> AdWindow {
        AdWindow(
            id: UUID().uuidString,
            analysisAssetId: result.analysisAssetId,
            startTime: result.startTime,
            endTime: result.endTime,
            confidence: result.adProbability,
            boundaryState: boundaryState.rawValue,
            decisionState: decisionState.rawValue,
            detectorVersion: config.detectorVersion,
            advertiser: nil,
            product: nil,
            adDescription: nil,
            evidenceText: evidenceText,
            evidenceStartTime: result.startTime,
            metadataSource: "none",
            metadataConfidence: nil,
            metadataPromptVersion: nil,
            wasSkipped: false,
            userDismissedBanner: false
        )
    }

    // MARK: - Metadata Extraction

    /// Extract metadata for a confirmed window and persist to SQLite.
    private func extractAndPersistMetadata(
        window: AdWindow,
        chunks: [TranscriptChunk]
    ) async {
        // Skip if metadata is already current.
        if !MetadataExtractorFactory.needsReExtraction(
            currentPromptVersion: window.metadataPromptVersion,
            currentSource: window.metadataSource
        ) { return }

        // Gather transcript text overlapping this window.
        let overlappingText = chunks
            .filter { $0.startTime < window.endTime && $0.endTime > window.startTime }
            .map(\.text)
            .joined(separator: " ")

        guard !overlappingText.isEmpty else { return }

        do {
            guard let metadata = try await metadataExtractor.extract(
                evidenceText: overlappingText,
                windowStartTime: window.startTime,
                windowEndTime: window.endTime
            ) else { return }

            try await store.updateAdWindowMetadata(
                id: window.id,
                advertiser: metadata.advertiser,
                product: metadata.product,
                evidenceText: metadata.evidenceText,
                metadataSource: metadata.source,
                metadataConfidence: metadata.confidence,
                metadataPromptVersion: metadata.promptVersion
            )
        } catch {
            logger.warning("Metadata extraction failed for window \(window.id): \(error.localizedDescription)")
        }
    }

    // MARK: - Prior Updates

    /// Update PodcastProfile priors from confirmed ad windows.
    /// Learns ad slot positions and sponsor names over time.
    private func updatePriors(
        podcastId: String,
        confirmedWindows: [AdWindow],
        episodeDuration: Double
    ) async throws {
        guard !confirmedWindows.isEmpty, episodeDuration > 0 else { return }

        let existingProfile = try await store.fetchProfile(podcastId: podcastId)

        // Compute normalized ad slot positions from confirmed windows.
        let newSlotPositions = confirmedWindows.map { window in
            let center = (window.startTime + window.endTime) / 2.0
            return center / episodeDuration
        }

        // Merge with existing slot positions (exponential moving average).
        let mergedSlots: [Double]
        if let existing = existingProfile,
           let json = existing.normalizedAdSlotPriors,
           let data = json.data(using: .utf8),
           let existingSlots = try? JSONDecoder().decode([Double].self, from: data) {
            mergedSlots = mergeSlotPositions(
                existing: existingSlots,
                new: newSlotPositions
            )
        } else {
            mergedSlots = newSlotPositions
        }

        let slotsJSON: String?
        if let data = try? JSONEncoder().encode(mergedSlots) {
            slotsJSON = String(data: data, encoding: .utf8)
        } else {
            slotsJSON = nil
        }

        // Collect advertiser names from confirmed windows with metadata.
        let newSponsors = confirmedWindows
            .compactMap(\.advertiser)
            .map { $0.lowercased() }

        let mergedSponsorLexicon: String?
        if let existing = existingProfile?.sponsorLexicon {
            let existingNames = Set(
                existing.components(separatedBy: ",")
                    .map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
                    .filter { !$0.isEmpty }
            )
            let allNames = existingNames.union(newSponsors)
            mergedSponsorLexicon = allNames.sorted().joined(separator: ",")
        } else if !newSponsors.isEmpty {
            mergedSponsorLexicon = Set(newSponsors).sorted().joined(separator: ",")
        } else {
            mergedSponsorLexicon = existingProfile?.sponsorLexicon
        }

        let observationCount = (existingProfile?.observationCount ?? 0) + 1
        // Trust score approaches 1.0 as observations grow, but FP signals reduce it.
        let fpCount = existingProfile?.implicitFalsePositiveCount ?? 0
        let rawTrust = Double(observationCount) / (Double(observationCount) + 5.0)
        let fpPenalty = Double(fpCount) * 0.02
        let trustScore = max(0, min(1.0, rawTrust - fpPenalty))

        let updatedProfile = PodcastProfile(
            podcastId: podcastId,
            sponsorLexicon: mergedSponsorLexicon,
            normalizedAdSlotPriors: slotsJSON,
            repeatedCTAFragments: existingProfile?.repeatedCTAFragments,
            jingleFingerprints: existingProfile?.jingleFingerprints,
            implicitFalsePositiveCount: existingProfile?.implicitFalsePositiveCount ?? 0,
            skipTrustScore: trustScore,
            observationCount: observationCount,
            mode: existingProfile?.mode ?? "shadow",
            recentFalseSkipSignals: existingProfile?.recentFalseSkipSignals ?? 0
        )

        try await store.upsertProfile(updatedProfile)

        // Refresh the in-memory priors for subsequent use.
        showPriors = ShowPriors.from(profile: updatedProfile)
        scanner = LexicalScanner(podcastProfile: updatedProfile)

        logger.info("Updated priors for podcast \(podcastId): observations=\(observationCount) trust=\(trustScore, format: .fixed(precision: 2))")
    }

    /// Merge new slot positions with existing ones. Deduplicates slots that
    /// are within 5% of each other (same ad slot across episodes).
    private func mergeSlotPositions(
        existing: [Double],
        new: [Double]
    ) -> [Double] {
        let proximityThreshold = 0.05
        var merged = existing

        for newSlot in new {
            let alreadyExists = merged.contains { abs($0 - newSlot) < proximityThreshold }
            if !alreadyExists {
                merged.append(newSlot)
            } else {
                // Nudge existing toward the new observation (EMA with alpha=0.3).
                merged = merged.map { existing in
                    if abs(existing - newSlot) < proximityThreshold {
                        return existing * 0.7 + newSlot * 0.3
                    }
                    return existing
                }
            }
        }

        return merged.sorted()
    }
}
