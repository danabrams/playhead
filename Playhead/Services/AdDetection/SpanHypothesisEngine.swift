import Foundation

// MARK: - Anchor event plumbing

struct AnchorEvent: Sendable, Equatable {
    let anchorType: AnchorType
    let matchedText: String
    let startTime: Double
    let endTime: Double
    let weight: Double
    let sponsorEntity: NormalizedSponsor?
}

struct NormalizedSponsor: Sendable, Equatable, Hashable {
    let value: String

    init(_ rawValue: String) {
        self.value = Self.normalize(rawValue)
    }

    private static func normalize(_ rawValue: String) -> String {
        rawValue
            .lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }
            .joined(separator: " ")
    }
}

struct BodyEvidenceItem: Sendable, Equatable {
    let matchedText: String
    let timestamp: Double
    let weight: Double
    let category: LexicalPatternCategory
}

enum ClosingReason: String, Sendable, Equatable {
    case explicitClose
    case returnMarker
    case timeout
    case idleGap
}

enum SpanHypothesisState: String, Sendable, Equatable {
    case idle
    case seeded
    case accumulating
    case confirmed
    case closed
}

struct CandidateAdSpan: Sendable, Identifiable, Equatable {
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let confidence: Double
    let evidenceScore: Double
    let anchorType: AnchorType
    let sponsorEntity: NormalizedSponsor?
    let isSkipEligible: Bool
    let evidenceText: String
    let closingReason: ClosingReason
}

struct SpanHypothesis: Sendable, Equatable {
    let seedAnchor: AnchorEvent
    let anchorType: AnchorType
    let polarity: AnchorPolarity
    let windowDuration: TimeInterval
    let backwardSearchRadius: TimeInterval
    let forwardSearchRadius: TimeInterval
    var sponsorEntity: NormalizedSponsor?
    var supportingAnchors: [AnchorEvent]
    var bodyEvidence: [BodyEvidenceItem]
    var closingAnchor: AnchorEvent?
    var startCandidateTime: Double
    var endCandidateTime: Double
    var lastEvidenceTime: Double
    var state: SpanHypothesisState

    init(seedAnchor: AnchorEvent, config: AnchorTypeConfig) {
        self.seedAnchor = seedAnchor
        self.anchorType = seedAnchor.anchorType
        self.polarity = config.polarity
        self.windowDuration = config.windowDuration
        self.backwardSearchRadius = config.backwardSearchRadius
        self.forwardSearchRadius = config.forwardSearchRadius
        self.sponsorEntity = seedAnchor.sponsorEntity
        self.supportingAnchors = []
        self.bodyEvidence = []
        self.closingAnchor = nil
        self.startCandidateTime = seedAnchor.startTime - config.backwardSearchRadius
        self.endCandidateTime = seedAnchor.endTime + config.forwardSearchRadius
        self.lastEvidenceTime = seedAnchor.endTime
        self.state = .seeded
    }

    var allEvidenceTexts: [String] {
        var texts = [seedAnchor.matchedText]
        texts.append(contentsOf: supportingAnchors.map(\.matchedText))
        texts.append(contentsOf: bodyEvidence.map(\.matchedText))
        if let closingAnchor {
            texts.append(closingAnchor.matchedText)
        }
        return texts.filter { !$0.isEmpty }
    }

    var currentEvidenceScore: Double {
        score(at: lastEvidenceTime)
    }

    func anchorEvidenceScore(at time: Double, decayRate: Double = SpanHypothesisEngine.defaultDecayRate) -> Double {
        let seedScore = Self.weightedContribution(
            weight: seedAnchor.weight,
            timestamp: seedAnchor.endTime,
            at: time,
            decayRate: decayRate
        )
        let supportingScore = supportingAnchors.reduce(0) { partial, anchor in
            partial + Self.weightedContribution(
                weight: anchor.weight,
                timestamp: anchor.endTime,
                at: time,
                decayRate: decayRate
            )
        }
        return seedScore + supportingScore
    }

    func score(at time: Double, decayRate: Double = SpanHypothesisEngine.defaultDecayRate) -> Double {
        let anchorScore = anchorEvidenceScore(at: time, decayRate: decayRate)
        let bodyScore = bodyEvidence.reduce(0) { partial, item in
            partial + Self.weightedContribution(
                weight: item.weight,
                timestamp: item.timestamp,
                at: time,
                decayRate: decayRate
            )
        }
        return anchorScore + bodyScore
    }

    mutating func absorb(anchor: AnchorEvent) {
        supportingAnchors.append(anchor)
        sponsorEntity = sponsorEntity ?? anchor.sponsorEntity
        startCandidateTime = min(startCandidateTime, anchor.startTime - backwardSearchRadius)
        endCandidateTime = max(endCandidateTime, anchor.endTime + forwardSearchRadius)
        lastEvidenceTime = max(lastEvidenceTime, anchor.endTime)
        state = .accumulating
    }

    mutating func absorb(bodyEvidence evidence: BodyEvidenceItem) {
        bodyEvidence.append(evidence)
        startCandidateTime = min(startCandidateTime, evidence.timestamp - backwardSearchRadius)
        endCandidateTime = max(endCandidateTime, evidence.timestamp + forwardSearchRadius)
        lastEvidenceTime = max(lastEvidenceTime, evidence.timestamp)
        state = .accumulating
    }

    mutating func confirmIfNeeded(minConfirmedEvidence: Double, decayRate: Double = SpanHypothesisEngine.defaultDecayRate) {
        if score(at: lastEvidenceTime, decayRate: decayRate) >= minConfirmedEvidence {
            state = .confirmed
        }
    }

    mutating func close(
        analysisAssetId: String,
        closingReason: ClosingReason,
        closeTime: Double,
        minConfirmedEvidence: Double,
        decayRate: Double = SpanHypothesisEngine.defaultDecayRate
    ) -> CandidateAdSpan {
        let wasConfirmedBeforeClose = state == .confirmed
        lastEvidenceTime = max(lastEvidenceTime, closeTime)
        state = .closed
        let score = self.score(at: closeTime, decayRate: decayRate)
        let plausibleSpan = endCandidateTime > startCandidateTime
        let skipEligible = switch closingReason {
        case .explicitClose, .returnMarker:
            plausibleSpan && (wasConfirmedBeforeClose || score >= minConfirmedEvidence)
        case .idleGap, .timeout:
            plausibleSpan && score >= minConfirmedEvidence
        }
        return CandidateAdSpan(
            id: Self.makeId(
                analysisAssetId: analysisAssetId,
                startTime: startCandidateTime,
                endTime: endCandidateTime,
                anchorType: anchorType,
                sponsorEntity: sponsorEntity,
                closingReason: closingReason
            ),
            analysisAssetId: analysisAssetId,
            startTime: startCandidateTime,
            endTime: endCandidateTime,
            confidence: min(1.0, score / max(minConfirmedEvidence, 1.0)),
            evidenceScore: score,
            anchorType: anchorType,
            sponsorEntity: sponsorEntity,
            isSkipEligible: skipEligible,
            evidenceText: allEvidenceTexts.joined(separator: " | "),
            closingReason: closingReason
        )
    }

    private static func weightedContribution(
        weight: Double,
        timestamp: Double,
        at time: Double,
        decayRate: Double
    ) -> Double {
        let elapsed = max(0, time - timestamp)
        return weight * pow(decayRate, elapsed)
    }

    private static func makeId(
        analysisAssetId: String,
        startTime: Double,
        endTime: Double,
        anchorType: AnchorType,
        sponsorEntity: NormalizedSponsor?,
        closingReason: ClosingReason
    ) -> String {
        let sponsor = sponsorEntity?.value ?? "_"
        return [
            analysisAssetId,
            String(format: "%.3f", locale: Locale(identifier: "en_US_POSIX"), startTime),
            String(format: "%.3f", locale: Locale(identifier: "en_US_POSIX"), endTime),
            String(describing: anchorType),
            sponsor,
            closingReason.rawValue
        ].joined(separator: ":")
    }
}

// MARK: - Engine

struct SpanHypothesisEngine: Sendable {
    static let defaultDecayRate: Double = 0.95
    private static let strictOverlapThreshold: TimeInterval = 3.0

    let config: SpanHypothesisConfig
    private(set) var activeHypotheses: [SpanHypothesis] = []
    private(set) var closedHypotheses: [SpanHypothesis] = []

    init(config: SpanHypothesisConfig = .default) {
        self.config = config
    }

    mutating func ingest(_ hit: LexicalHit, analysisAssetId: String) -> [CandidateAdSpan] {
        if let event = Self.mapToAnchorEvent(hit) {
            return ingest(event, analysisAssetId: analysisAssetId)
        }
        if let bodyEvidence = Self.mapToBodyEvidence(hit) {
            return ingest(bodyEvidence, analysisAssetId: analysisAssetId)
        }
        return []
    }

    mutating func ingest(_ event: AnchorEvent, analysisAssetId: String) -> [CandidateAdSpan] {
        var emitted: [CandidateAdSpan] = closeStaleHypotheses(before: event.startTime, analysisAssetId: analysisAssetId)

        if event.anchorType == .transitionMarker && !event.isExplicitReturnMarker {
            emitted.append(contentsOf: ingest(
                BodyEvidenceItem(
                    matchedText: event.matchedText,
                    timestamp: event.startTime,
                    weight: event.weight,
                    category: .transitionMarker
                ),
                analysisAssetId: analysisAssetId
            ))
            return emitted
        }

        if event.isExplicitReturnMarker {
            if let index = bestCompatibleHypothesisIndex(for: event) {
                var hypothesis = activeHypotheses[index]
                hypothesis.absorb(anchor: event)
                hypothesis.confirmIfNeeded(minConfirmedEvidence: config.minConfirmedEvidence)
                hypothesis.closingAnchor = event
                let span = hypothesis.close(
                    analysisAssetId: analysisAssetId,
                    closingReason: .returnMarker,
                    closeTime: event.endTime,
                    minConfirmedEvidence: config.minConfirmedEvidence
                )
                emitClosingHypothesis(hypothesis, at: index)
                emitted.append(span)
            }
            return emitted
        }

        if event.anchorType.isExplicitCloseAnchor {
            if let index = bestCompatibleHypothesisIndex(for: event) {
                var hypothesis = activeHypotheses[index]
                hypothesis.absorb(anchor: event)
                hypothesis.confirmIfNeeded(minConfirmedEvidence: config.minConfirmedEvidence)
                hypothesis.closingAnchor = event
                let span = hypothesis.close(
                    analysisAssetId: analysisAssetId,
                    closingReason: .explicitClose,
                    closeTime: event.endTime,
                    minConfirmedEvidence: config.minConfirmedEvidence
                )
                emitClosingHypothesis(hypothesis, at: index)
                emitted.append(span)
            } else {
                var hypothesis = SpanHypothesis(
                    seedAnchor: event,
                    config: config.config(for: event.anchorType)
                )
                hypothesis.confirmIfNeeded(minConfirmedEvidence: config.minConfirmedEvidence)
                activeHypotheses.append(hypothesis)
            }
            return emitted
        }

        if let index = bestCompatibleHypothesisIndex(for: event) {
            activeHypotheses[index].absorb(anchor: event)
            activeHypotheses[index].confirmIfNeeded(minConfirmedEvidence: config.minConfirmedEvidence)
            emitted.append(contentsOf: mergeCompatibleHypotheses(around: index, analysisAssetId: analysisAssetId))
        } else {
            activeHypotheses.append(SpanHypothesis(seedAnchor: event, config: config.config(for: event.anchorType)))
        }

        return emitted
    }

    mutating func ingest(_ bodyEvidence: BodyEvidenceItem, analysisAssetId: String) -> [CandidateAdSpan] {
        guard !activeHypotheses.isEmpty else { return [] }

        if let index = bestBodyEvidenceHypothesisIndex(for: bodyEvidence) {
            activeHypotheses[index].absorb(bodyEvidence: bodyEvidence)
            activeHypotheses[index].confirmIfNeeded(minConfirmedEvidence: config.minConfirmedEvidence)
            return mergeCompatibleHypotheses(around: index, analysisAssetId: analysisAssetId)
        }

        return []
    }

    mutating func finish(analysisAssetId: String, at time: Double) -> [CandidateAdSpan] {
        var emitted: [CandidateAdSpan] = []
        while !activeHypotheses.isEmpty {
            var hypothesis = activeHypotheses.removeFirst()
            let span = hypothesis.close(
                analysisAssetId: analysisAssetId,
                closingReason: .timeout,
                closeTime: time,
                minConfirmedEvidence: config.minConfirmedEvidence
            )
            closedHypotheses.append(hypothesis)
            emitted.append(span)
        }
        return emitted.sorted { $0.startTime < $1.startTime }
    }

    static func mapToAnchorEvent(_ hit: LexicalHit) -> AnchorEvent? {
        switch hit.category {
        case .purchaseLanguage:
            return nil
        case .urlCTA where hit.weight < 0.95:
            return nil
        case .urlCTA:
            return AnchorEvent(
                anchorType: .url,
                matchedText: hit.matchedText,
                startTime: hit.startTime,
                endTime: hit.endTime,
                weight: hit.weight,
                sponsorEntity: nil
            )
        case .promoCode:
            return AnchorEvent(
                anchorType: .promoCode,
                matchedText: hit.matchedText,
                startTime: hit.startTime,
                endTime: hit.endTime,
                weight: hit.weight,
                sponsorEntity: extractSponsorEntity(from: hit.matchedText, anchorType: .promoCode)
            )
        case .sponsor:
            let matchedText = hit.matchedText.lowercased()
            let anchorType: AnchorType = matchedText.contains("brought to you by")
                || matchedText.contains("sponsored by")
                || matchedText.contains("thanks to our sponsor")
                || matchedText.contains("a word from our sponsor")
                || matchedText.contains("message from our sponsor")
                || matchedText.contains("supported by")
                ? .disclosure
                : .sponsorLexicon
            return AnchorEvent(
                anchorType: anchorType,
                matchedText: hit.matchedText,
                startTime: hit.startTime,
                endTime: hit.endTime,
                weight: hit.weight,
                sponsorEntity: extractSponsorEntity(from: hit.matchedText, anchorType: anchorType)
            )
        case .transitionMarker:
            return AnchorEvent(
                anchorType: .transitionMarker,
                matchedText: hit.matchedText,
                startTime: hit.startTime,
                endTime: hit.endTime,
                weight: hit.weight,
                sponsorEntity: nil
            )
        }
    }

    static func mapToBodyEvidence(_ hit: LexicalHit) -> BodyEvidenceItem? {
        switch hit.category {
        case .purchaseLanguage:
            return BodyEvidenceItem(
                matchedText: hit.matchedText,
                timestamp: hit.startTime,
                weight: hit.weight,
                category: hit.category
            )
        case .urlCTA where hit.weight < 0.95:
            return BodyEvidenceItem(
                matchedText: hit.matchedText,
                timestamp: hit.startTime,
                weight: hit.weight,
                category: hit.category
            )
        default:
            return nil
        }
    }

    private mutating func closeStaleHypotheses(before time: Double, analysisAssetId: String) -> [CandidateAdSpan] {
        guard !activeHypotheses.isEmpty else { return [] }

        var emitted: [CandidateAdSpan] = []
        var remaining: [SpanHypothesis] = []
        for hypothesis in activeHypotheses {
            if time - hypothesis.lastEvidenceTime > config.maxIdleGapSeconds {
                var closed = hypothesis
                let span = closed.close(
                    analysisAssetId: analysisAssetId,
                    closingReason: .idleGap,
                    closeTime: time,
                    minConfirmedEvidence: config.minConfirmedEvidence
                )
                closedHypotheses.append(closed)
                emitted.append(span)
            } else {
                remaining.append(hypothesis)
            }
        }
        activeHypotheses = remaining
        return emitted.sorted { $0.startTime < $1.startTime }
    }

    private func bestCompatibleHypothesisIndex(for event: AnchorEvent) -> Int? {
        let candidates = activeHypotheses.enumerated().filter { index, hypothesis in
            hypothesisCompatible(hypothesis, with: event) && eventFallsWithinWindow(hypothesis, event: event)
        }
        if candidates.isEmpty {
            return nil
        }

        let ordered = candidates.sorted { lhs, rhs in
            hypothesis(lhs.element, isBetterMatchThan: rhs.element, for: event)
        }
        return ordered.first?.offset
    }

    private func bestBodyEvidenceHypothesisIndex(for bodyEvidence: BodyEvidenceItem) -> Int? {
        activeHypotheses.enumerated()
            .filter { _, hypothesis in
                bodyEvidence.timestamp >= hypothesis.startCandidateTime - Self.strictOverlapThreshold
                    && bodyEvidence.timestamp <= hypothesis.endCandidateTime + Self.strictOverlapThreshold
            }
            .sorted { lhs, rhs in
                lhs.element.lastEvidenceTime > rhs.element.lastEvidenceTime
            }
            .first?.offset
    }

    private mutating func mergeCompatibleHypotheses(around index: Int, analysisAssetId: String) -> [CandidateAdSpan] {
        guard activeHypotheses.indices.contains(index) else { return [] }

        let mergeIndices = activeHypotheses.indices.filter { otherIndex in
            guard otherIndex != index else { return false }
            let other = activeHypotheses[otherIndex]
            return hypothesesShouldMerge(activeHypotheses[index], other)
        }

        guard !mergeIndices.isEmpty else { return [] }

        var mergedHypothesis = activeHypotheses[index]
        var removed: [Int] = []
        for otherIndex in mergeIndices.sorted(by: >) {
            mergedHypothesis = merge(mergedHypothesis, with: activeHypotheses[otherIndex])
            activeHypotheses.remove(at: otherIndex)
            removed.append(otherIndex)
        }
        activeHypotheses[index - removed.filter { $0 < index }.count] = mergedHypothesis
        return []
    }

    private func hypothesesShouldMerge(_ lhs: SpanHypothesis, _ rhs: SpanHypothesis) -> Bool {
        if let lhsSponsor = lhs.sponsorEntity, let rhsSponsor = rhs.sponsorEntity {
            return lhsSponsor == rhsSponsor
        }

        let overlap = hypothesisOverlap(lhs, rhs)
        return overlap > Self.strictOverlapThreshold
    }

    private func hypothesisCompatible(_ hypothesis: SpanHypothesis, with event: AnchorEvent) -> Bool {
        guard let hypothesisSponsor = hypothesis.sponsorEntity, let eventSponsor = event.sponsorEntity else {
            return true
        }
        return hypothesisSponsor == eventSponsor
    }

    private func hypothesis(_ lhs: SpanHypothesis, isBetterMatchThan rhs: SpanHypothesis, for event: AnchorEvent) -> Bool {
        if prefersPlausibilityRanking(for: event) {
            let lhsScore = terminalAnchorOwnershipScore(for: lhs, event: event)
            let rhsScore = terminalAnchorOwnershipScore(for: rhs, event: event)
            if lhsScore != rhsScore {
                return lhsScore > rhsScore
            }

            let lhsGap = max(0, event.startTime - lhs.lastEvidenceTime)
            let rhsGap = max(0, event.startTime - rhs.lastEvidenceTime)
            if lhsGap != rhsGap {
                return lhsGap < rhsGap
            }
        } else if lhs.lastEvidenceTime != rhs.lastEvidenceTime {
            return lhs.lastEvidenceTime > rhs.lastEvidenceTime
        }

        if lhs.lastEvidenceTime != rhs.lastEvidenceTime {
            return lhs.lastEvidenceTime > rhs.lastEvidenceTime
        }

        return lhs.seedAnchor.startTime < rhs.seedAnchor.startTime
    }

    private func prefersPlausibilityRanking(for event: AnchorEvent) -> Bool {
        event.sponsorEntity == nil && (event.anchorType.isExplicitCloseAnchor || event.isExplicitReturnMarker)
    }

    private func terminalAnchorOwnershipScore(for hypothesis: SpanHypothesis, event: AnchorEvent) -> Double {
        switch event.anchorType {
        case .promoCode, .url:
            return hypothesis.score(at: event.endTime)
        case .transitionMarker:
            return hypothesis.anchorEvidenceScore(at: event.endTime)
        case .disclosure, .sponsorLexicon, .fmPositive:
            return hypothesis.score(at: event.endTime)
        }
    }

    private func eventFallsWithinWindow(_ hypothesis: SpanHypothesis, event: AnchorEvent) -> Bool {
        let slack = (hypothesis.sponsorEntity == nil || event.sponsorEntity == nil)
            ? Self.strictOverlapThreshold
            : 0
        return event.startTime <= hypothesis.endCandidateTime + slack
            && event.endTime >= hypothesis.startCandidateTime - slack
    }

    private func hypothesisOverlap(_ lhs: SpanHypothesis, _ rhs: SpanHypothesis) -> TimeInterval {
        max(0, min(lhs.endCandidateTime, rhs.endCandidateTime) - max(lhs.startCandidateTime, rhs.startCandidateTime))
    }

    private mutating func merge(_ lhs: SpanHypothesis, with rhs: SpanHypothesis) -> SpanHypothesis {
        var merged = lhs
        if rhs.seedAnchor.startTime < merged.seedAnchor.startTime {
            merged = rhs
        }
        merged.supportingAnchors.append(contentsOf: lhs.supportingAnchors)
        merged.supportingAnchors.append(contentsOf: rhs.supportingAnchors)
        merged.bodyEvidence.append(contentsOf: lhs.bodyEvidence)
        merged.bodyEvidence.append(contentsOf: rhs.bodyEvidence)
        merged.startCandidateTime = min(lhs.startCandidateTime, rhs.startCandidateTime)
        merged.endCandidateTime = max(lhs.endCandidateTime, rhs.endCandidateTime)
        merged.lastEvidenceTime = max(lhs.lastEvidenceTime, rhs.lastEvidenceTime)
        if merged.sponsorEntity == nil {
            merged.sponsorEntity = lhs.sponsorEntity ?? rhs.sponsorEntity
        }
        merged.state = maxState(lhs.state, rhs.state)
        return merged
    }

    private mutating func emitClosingHypothesis(_ hypothesis: SpanHypothesis, at index: Int) {
        closedHypotheses.append(hypothesis)
        activeHypotheses.remove(at: index)
    }

    private func maxState(_ lhs: SpanHypothesisState, _ rhs: SpanHypothesisState) -> SpanHypothesisState {
        let ordering: [SpanHypothesisState: Int] = [
            .idle: 0,
            .seeded: 1,
            .accumulating: 2,
            .confirmed: 3,
            .closed: 4
        ]
        return (ordering[lhs] ?? 0) >= (ordering[rhs] ?? 0) ? lhs : rhs
    }

    private static func extractSponsorEntity(from matchedText: String, anchorType: AnchorType) -> NormalizedSponsor? {
        let normalized = normalizeSponsorText(matchedText)
        guard !normalized.isEmpty else { return nil }

        switch anchorType {
        case .promoCode:
            return extractPromoSponsor(from: normalized)
        case .disclosure, .sponsorLexicon:
            if let stripped = stripDisclosurePrefix(from: normalized) {
                return stripped.isEmpty ? nil : NormalizedSponsor(stripTrailingFillerWords(stripped))
            }
            return NormalizedSponsor(stripTrailingFillerWords(normalized))
        case .url, .fmPositive, .transitionMarker:
            return nil
        }
    }

    private static func normalizeSponsorText(_ text: String) -> String {
        text.lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }
            .joined(separator: " ")
    }

    private static func stripDisclosurePrefix(from normalized: String) -> String? {
        let prefixes = [
            "brought to you by",
            "sponsored by",
            "thanks to our sponsor",
            "this episode is sponsored",
            "this podcast is brought",
            "a word from our sponsor",
            "message from our sponsor",
            "supported by",
            "today s sponsor"
        ]

        for prefix in prefixes.sorted(by: { $0.count > $1.count }) {
            if normalized == prefix {
                return ""
            }
            if normalized.hasPrefix(prefix + " ") {
                let remainder = String(normalized.dropFirst(prefix.count)).trimmingCharacters(in: .whitespaces)
                return remainder
            }
        }
        return nil
    }

    private static func stripTrailingFillerWords(_ text: String) -> String {
        let fillerWords: Set<String> = [
            "today",
            "tonight",
            "now",
            "again",
            "here",
            "episode",
            "show",
            "podcast",
            "ad",
            "ads",
            "commercial",
            "break",
            "todays"
        ]

        let tokens = text.split(separator: " ").map(String.init)
        let trimmed = tokens.prefix { token in
            !fillerWords.contains(token)
        }
        return trimmed.joined(separator: " ")
    }

    private static func extractPromoSponsor(from normalized: String) -> NormalizedSponsor? {
        let patterns = [
            #"(?:use|enter|promo|discount|coupon) code [a-z0-9]+(?: at| for| with)? ([a-z0-9][a-z0-9 ]+)"#,
            #"use code [a-z0-9]+ at ([a-z0-9][a-z0-9 ]+)"#,
            #"code [a-z0-9]+ at ([a-z0-9][a-z0-9 ]+)"#
        ]

        for pattern in patterns {
            if let capture = firstCapture(in: normalized, pattern: pattern) {
                let cleaned = stripTrailingFillerWords(capture)
                if !cleaned.isEmpty {
                    return NormalizedSponsor(cleaned)
                }
            }
        }

        return nil
    }

    private static func firstCapture(in text: String, pattern: String) -> String? {
        guard let regex = try? NSRegularExpression(pattern: pattern, options: [.caseInsensitive]) else {
            return nil
        }
        let nsText = text as NSString
        let range = NSRange(location: 0, length: nsText.length)
        guard let match = regex.firstMatch(in: text, range: range), match.numberOfRanges > 1 else {
            return nil
        }
        return nsText.substring(with: match.range(at: 1))
    }
}

private extension AnchorType {
    var isExplicitCloseAnchor: Bool {
        switch self {
        case .url, .promoCode:
            return true
        case .disclosure, .sponsorLexicon, .fmPositive, .transitionMarker:
            return false
        }
    }
}

private extension AnchorEvent {
    var isExplicitReturnMarker: Bool {
        let normalized = matchedText.lowercased()
        return normalized.contains("back to the show") || normalized.contains("back to the episode")
    }
}
