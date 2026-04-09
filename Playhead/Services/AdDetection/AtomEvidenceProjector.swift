// AtomEvidenceProjector.swift
// Phase 5 (playhead-4my.5.1): Projects Phase 4 region anchors + EvidenceCatalog
// entries + user corrections onto the per-atom timeline, producing [AtomEvidence].
//
// Design:
//   • Pure-Swift actor. No FM calls, no async network.
//   • Three anchor paths: FM consensus (>= .medium), EvidenceEntry (trustworthy
//     categories only), and Use C corroboration (single-window FM + acoustic break).
//   • .acoustic-origin regions populate hasAcousticBreakHint.
//   • All three Use C conditions (FM low-but-not-medium, break >= 0.5, within ±2) must
//     ALL fire simultaneously — neither signal alone anchors.
//   • atomOrdinal is the stable key; timing is carried forward from the atom.
//
// Anchor source precedence: any qualifying source makes isAnchored == true and
// contributes a distinct AnchorRef to anchorProvenance. Multiple sources on the
// same atom merge into the array — the decoder and UI use all of them.

import Foundation
import OSLog

// MARK: - AtomEvidenceProjector

actor AtomEvidenceProjector {

    private static let logger = Logger(
        subsystem: "com.playhead",
        category: "AtomEvidenceProjector"
    )

    /// Trustworthy EvidenceEntry categories that anchor an atom.
    /// .brandSpan is intentionally excluded — too noisy alone.
    private static let anchoringCategories: Set<EvidenceCategory> = [
        .url, .promoCode, .disclosurePhrase, .ctaPhrase
    ]

    // MARK: - Use C constants (from design doc)

    /// Minimum fmConsensusStrength for the single-window FM leg of Use C.
    private static let useCMinFMStrength: FMConsensusStrength = .low
    /// Maximum fmConsensusStrength (exclusive) for single-window FM in Use C.
    /// (Atoms with >= .medium are already anchored via the primary FM path.)
    private static let useCMaxFMStrength: FMConsensusStrength = .medium
    /// Minimum acoustic breakStrength for Use C.
    private static let useCMinBreakStrength: Double = 0.5
    /// Maximum atom-count distance for Use C acoustic break to count.
    private static let useCBreakRadiusAtoms: Int = 2

    // MARK: - Public API

    /// Project Phase 4 region bundles + evidence catalog + correction masks onto atoms.
    ///
    /// - Parameters:
    ///   - regions: Phase 4 RegionFeatureBundles (from RegionShadowObserver or equivalent).
    ///   - catalog: EvidenceCatalog for this transcript.
    ///   - atoms: Transcript atoms to annotate. Must be from the same asset.
    ///   - correctionMaskProvider: Supplies user correction overrides.
    /// - Returns: One AtomEvidence per input atom, in atom ordinal order.
    func project(
        regions: [RegionFeatureBundle],
        catalog: EvidenceCatalog,
        atoms: [TranscriptAtom],
        correctionMaskProvider: any CorrectionMaskProvider
    ) async -> [AtomEvidence] {
        guard !atoms.isEmpty else { return [] }

        let sortedAtoms = atoms.sorted { $0.atomKey.atomOrdinal < $1.atomKey.atomOrdinal }
        let assetId = sortedAtoms[0].atomKey.analysisAssetId
        let ordinalRange = sortedAtoms[0].atomKey.atomOrdinal ... sortedAtoms[sortedAtoms.count - 1].atomKey.atomOrdinal

        // Fetch correction masks for all atom ordinals in one async call.
        let masks = await correctionMaskProvider.correctionMasks(for: ordinalRange, in: assetId)

        // Build per-atom lookup for corrections.
        // Build maps keyed by ordinal for fast lookup.
        let atomsByOrdinal = Dictionary(uniqueKeysWithValues: sortedAtoms.map {
            ($0.atomKey.atomOrdinal, $0)
        })

        // MARK: Classify regions by anchor type

        // FM-consensus anchored regions (strength >= .medium).
        let fmConsensusRegions = regions.filter {
            $0.region.origins.contains(.foundationModel) &&
            $0.region.fmConsensusStrength >= .medium
        }

        // Single-window FM regions (>= .low, < .medium) for Use C.
        let singleWindowFMRegions = regions.filter {
            $0.region.origins.contains(.foundationModel) &&
            $0.region.fmConsensusStrength >= Self.useCMinFMStrength &&
            $0.region.fmConsensusStrength < Self.useCMaxFMStrength
        }

        // Acoustic-origin regions for hasAcousticBreakHint.
        let acousticRegions = regions.filter {
            $0.region.origins.contains(.acoustic)
        }

        // Collect all acoustic breaks from acoustic regions for Use C proximity checks.
        // Dedupe by (time) so we don't double-count breaks that appear on multiple regions.
        var seenBreakTimes = Set<Double>()
        var allAcousticBreaks: [(ordinal: Int, breakStrength: Double)] = []
        for r in acousticRegions {
            for brk in r.region.acousticBreaks {
                guard seenBreakTimes.insert(brk.time).inserted else { continue }
                // Find the atom containing this break time
                if let atom = sortedAtoms.first(where: {
                    $0.startTime <= brk.time && brk.time < $0.endTime
                }) {
                    allAcousticBreaks.append((ordinal: atom.atomKey.atomOrdinal, breakStrength: brk.breakStrength))
                }
            }
        }

        // EvidenceCatalog entries for anchoring categories (trustworthy only).
        let anchoringEntries = catalog.entries.filter {
            Self.anchoringCategories.contains($0.category)
        }

        // MARK: Build per-atom coverage sets

        // Which atom ordinals are covered by FM-consensus regions?
        var fmConsensusAtoms: [Int: (regionId: String, strength: Double)] = [:]
        for bundle in fmConsensusRegions {
            let r = bundle.region
            let regionId = Self.stableRegionId(bundle: bundle)
            for ordinal in r.firstAtomOrdinal ... r.lastAtomOrdinal {
                if fmConsensusAtoms[ordinal] == nil {
                    fmConsensusAtoms[ordinal] = (regionId, r.fmConsensusStrength.value)
                }
            }
        }

        // Which atom ordinals are covered by single-window FM regions?
        var singleWindowFMAtoms: [Int: String] = [:]
        for bundle in singleWindowFMRegions {
            let r = bundle.region
            let regionId = Self.stableRegionId(bundle: bundle)
            for ordinal in r.firstAtomOrdinal ... r.lastAtomOrdinal {
                if singleWindowFMAtoms[ordinal] == nil {
                    singleWindowFMAtoms[ordinal] = regionId
                }
            }
        }

        // Which atom ordinals are covered by acoustic-origin regions?
        var acousticCoveredOrdinals = Set<Int>()
        for bundle in acousticRegions {
            let r = bundle.region
            for ordinal in r.firstAtomOrdinal ... r.lastAtomOrdinal {
                acousticCoveredOrdinals.insert(ordinal)
            }
        }

        // Which atom ordinals have evidence catalog hits?
        var evidenceByOrdinal: [Int: [EvidenceEntry]] = [:]
        for entry in anchoringEntries {
            evidenceByOrdinal[entry.atomOrdinal, default: []].append(entry)
        }

        // MARK: Produce AtomEvidence for each atom

        var results: [AtomEvidence] = []
        results.reserveCapacity(sortedAtoms.count)

        for atom in sortedAtoms {
            let ordinal = atom.atomKey.atomOrdinal
            let correctionMask = masks[ordinal] ?? .none
            let hasAcousticBreakHint = acousticCoveredOrdinals.contains(ordinal)

            var anchorProvenance: [AnchorRef] = []

            // Path 1: FM consensus (strength >= .medium).
            if let fm = fmConsensusAtoms[ordinal] {
                anchorProvenance.append(.fmConsensus(
                    regionId: fm.regionId,
                    consensusStrength: fm.strength
                ))
            }

            // Path 2: EvidenceEntry (trustworthy categories).
            if let entries = evidenceByOrdinal[ordinal] {
                for entry in entries {
                    anchorProvenance.append(.evidenceCatalog(entry: entry))
                }
            }

            // Path 3: Use C — single-window FM + co-located acoustic break.
            if anchorProvenance.isEmpty,
               let singleFMRegionId = singleWindowFMAtoms[ordinal] {
                // Look for the nearest strong acoustic break within ±useCBreakRadiusAtoms.
                // Select by minimum abs(distance) among all qualifying breaks, not just the first.
                let nearestBreak = allAcousticBreaks
                    .filter { brk in
                        abs(brk.ordinal - ordinal) <= Self.useCBreakRadiusAtoms &&
                        brk.breakStrength >= Self.useCMinBreakStrength
                    }
                    .min(by: { abs($0.ordinal - ordinal) < abs($1.ordinal - ordinal) })
                if let nearestBreak {
                    anchorProvenance.append(.fmAcousticCorroborated(
                        regionId: singleFMRegionId,
                        breakStrength: nearestBreak.breakStrength
                    ))
                }
            }

            let isAnchored = !anchorProvenance.isEmpty && correctionMask != .userVetoed

            results.append(AtomEvidence(
                atomOrdinal: ordinal,
                startTime: atom.startTime,
                endTime: atom.endTime,
                isAnchored: isAnchored,
                anchorProvenance: anchorProvenance,
                hasAcousticBreakHint: hasAcousticBreakHint,
                correctionMask: correctionMask
            ))
        }

        Self.logger.info(
            "AtomEvidenceProjector: \(sortedAtoms.count) atoms → \(results.filter(\.isAnchored).count) anchored, \(results.filter(\.hasAcousticBreakHint).count) with acoustic break hints"
        )

        return results
    }

    // MARK: - Stable region ID

    /// Produces a stable string ID for a region:
    /// "\(analysisAssetId)#\(firstAtomOrdinal)-\(lastAtomOrdinal)"
    private static func stableRegionId(bundle: RegionFeatureBundle) -> String {
        let r = bundle.region
        return "\(r.analysisAssetId)#\(r.firstAtomOrdinal)-\(r.lastAtomOrdinal)"
    }
}
