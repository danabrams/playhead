// AdLikelihoodScanOrder.swift
// playhead-lxkq — STUB. See AdLikelihoodScanOrderTests.swift for the contract
// this must satisfy; the implementation lands in the next commit.

import Foundation

struct AdLikelihoodSeed: Sendable, Equatable {
    enum Kind: String, Sendable, Equatable, CaseIterable {
        case acousticSeam
        case evidenceAnchor
        case lexicalCue
        case rediffNeighbourhood
    }

    let startTime: Double
    let endTime: Double
    let kind: Kind
    let strength: Double

    init(startTime: Double, endTime: Double, kind: Kind, strength: Double) {
        self.startTime = startTime
        self.endTime = endTime
        self.kind = kind
        self.strength = strength
    }
}

enum AdLikelihoodScanOrder {

    static let defaultNeighbourhoodRadiusSeconds: Double = 90
    static let defaultMaxPromotedAudioSeconds: Double = 1_800
    static let maxSeedWidthSeconds: Double = 300

    static func weight(for kind: AdLikelihoodSeed.Kind) -> Double { 1.0 }

    static func order<Plan>(
        _ plans: [Plan],
        seeds: [AdLikelihoodSeed],
        radiusSeconds: Double = defaultNeighbourhoodRadiusSeconds,
        maxPromotedAudioSeconds: Double = defaultMaxPromotedAudioSeconds,
        span: (Plan) -> (start: Double, end: Double)
    ) -> [Plan] {
        plans
    }

    struct Neighbourhood: Sendable, Equatable {
        let lo: Double
        let hi: Double
        let score: Double
    }

    static func neighbourhoods(
        from seeds: [AdLikelihoodSeed],
        radiusSeconds: Double = defaultNeighbourhoodRadiusSeconds
    ) -> [Neighbourhood] {
        []
    }

    static func seeds(
        acousticBreaks: [AcousticBreak],
        evidenceCatalog: EvidenceCatalog?,
        lexicalCandidates: [LexicalCandidate]
    ) -> [AdLikelihoodSeed] {
        []
    }

    static func restoreOrder<Item>(_ items: [Item], by key: (Item) -> Int) -> [Item] {
        items
    }
}
