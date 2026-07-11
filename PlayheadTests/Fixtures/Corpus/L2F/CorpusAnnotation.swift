// CorpusAnnotation.swift
// Schema for the labeled test corpus introduced by playhead-l2f.
//
// This is a NEW corpus format, distinct from the legacy
// `TestEpisodeAnnotation` used by the synthetic replay simulator.
// Annotations live on disk as JSON in `TestFixtures/Corpus/Annotations/`
// alongside their referenced audio files in `TestFixtures/Corpus/Audio/`.
//
// JSON shape mirrors the bead-l2f specification exactly:
//
// {
//   "episode_id": "corpus-001",
//   "show_name": "Example Podcast",
//   "duration_seconds": 3600,
//   "ad_windows": [
//     {
//       "start_seconds": 180.0,
//       "end_seconds": 240.0,
//       "advertiser": "Squarespace",
//       "product": "Website builder",
//       "ad_type": "host_read",
//       "transition_type": "explicit",
//       "confidence_notes": "Clear brought to you by intro"
//     }
//   ],
//   "content_windows": [
//     {
//       "start_seconds": 0.0,
//       "end_seconds": 180.0,
//       "notes": "Interview content — must NEVER be skipped"
//     }
//   ],
//   "variant_of": null,
//   "audio_fingerprint": "sha256:abc123..."
// }

import Foundation

// MARK: - CorpusAnnotation

/// Ground-truth annotation for a single test episode.
///
/// Decoded from a JSON file in `TestFixtures/Corpus/Annotations/`.
/// Snake-case JSON keys are mapped to camel-case Swift properties via
/// `JSONDecoder.KeyDecodingStrategy.convertFromSnakeCase` (see
/// ``CorpusAnnotation/decoder``).
struct CorpusAnnotation: Sendable, Codable, Equatable {
    /// Stable identifier for this episode (e.g. "corpus-001").
    let episodeId: String
    /// Human-readable show name (e.g. "Diary of a CEO").
    let showName: String
    /// Total episode duration in seconds.
    let durationSeconds: Double
    /// All labeled ad windows. Must not overlap each other or any content window.
    let adWindows: [AdWindow]
    /// All labeled content windows. Must partition the timeline together with `adWindows`.
    let contentWindows: [ContentWindow]
    /// If this is a dynamic-ad-insertion variant, the `episode_id` it varies from.
    let variantOf: String?
    /// SHA-256 hash of the referenced audio file, prefixed with `sha256:`.
    let audioFingerprint: String
    /// True when the annotation was created by the automated promotion pipeline.
    let autoPromoted: Bool?
    /// Promotion timestamp retained as source data rather than interpreted locally.
    let autoPromotedAt: String?
    /// Tool or workflow that produced the automatic annotation.
    let autoPromotedBy: String?
    /// Episode-level provenance. Unknown values are intentionally preserved.
    let provenance: [String]?
    /// Episode-level audit priority emitted by promotion tooling, when present.
    let auditPriority: Int?
    /// Durable, asset-bound human review evidence. Gold requires two distinct
    /// reviewers and two distinct canonical review artifacts.
    let reviewAttestations: [ReviewAttestation]?

    init(
        episodeId: String,
        showName: String,
        durationSeconds: Double,
        adWindows: [AdWindow],
        contentWindows: [ContentWindow],
        variantOf: String?,
        audioFingerprint: String,
        autoPromoted: Bool? = nil,
        autoPromotedAt: String? = nil,
        autoPromotedBy: String? = nil,
        provenance: [String]? = nil,
        auditPriority: Int? = nil,
        reviewAttestations: [ReviewAttestation]? = nil
    ) {
        self.episodeId = episodeId
        self.showName = showName
        self.durationSeconds = durationSeconds
        self.adWindows = adWindows
        self.contentWindows = contentWindows
        self.variantOf = variantOf
        self.audioFingerprint = audioFingerprint
        self.autoPromoted = autoPromoted
        self.autoPromotedAt = autoPromotedAt
        self.autoPromotedBy = autoPromotedBy
        self.provenance = provenance
        self.auditPriority = auditPriority
        self.reviewAttestations = reviewAttestations
    }

    struct ReviewAttestation: Sendable, Codable, Equatable {
        let reviewer: String
        let reviewedAt: String
        let audioFingerprint: String
        let reviewArtifactId: String
    }

    /// Quality tier used to keep proposals out of human-reviewed evaluation gates.
    enum LabelTier: String, Sendable, Codable, Equatable, Hashable, CaseIterable {
        case gold
        case silver
        case boundaryProposal = "boundary_proposal"
    }

    // MARK: AdWindow

    /// A labeled ad window with full metadata.
    struct AdWindow: Sendable, Codable, Equatable {
        /// Window start in seconds, ±0.5s precision.
        let startSeconds: Double
        /// Window end in seconds, ±0.5s precision.
        let endSeconds: Double
        /// Advertiser name (e.g. "Squarespace"). Optional for blended ads
        /// where the brand is implicit.
        let advertiser: String?
        /// Product mentioned (e.g. "Website builder").
        let product: String?
        /// How the ad is delivered.
        let adType: AdType
        /// How the transition into the ad is signalled. Optional because
        /// auto-promoted DAI spans emitted by `scripts/l2f-auto-promote.py`
        /// carry `transition_type: null` — the rediff-confirmed promotion
        /// path has no annotator-grade transition cue to assert.
        let transitionType: TransitionType?
        /// Free-form annotator notes about why this confidence was assigned.
        let confidenceNotes: String?
        /// True when this window came from the automatic promotion pipeline.
        let autoPromoted: Bool?
        /// Window-level promotion timestamp, when emitted by a producer.
        let autoPromotedAt: String?
        /// Window-level promotion tool marker, when emitted by a producer.
        let autoPromotedBy: String?
        /// Ordered evidence sources. Unknown source names survive round trips.
        let provenance: [String]?
        /// R3 proposals use priority 1; triangulated promotions use priority 3.
        let auditPriority: Int?

        init(
            startSeconds: Double,
            endSeconds: Double,
            advertiser: String?,
            product: String?,
            adType: AdType,
            transitionType: TransitionType?,
            confidenceNotes: String?,
            autoPromoted: Bool? = nil,
            autoPromotedAt: String? = nil,
            autoPromotedBy: String? = nil,
            provenance: [String]? = nil,
            auditPriority: Int? = nil
        ) {
            self.startSeconds = startSeconds
            self.endSeconds = endSeconds
            self.advertiser = advertiser
            self.product = product
            self.adType = adType
            self.transitionType = transitionType
            self.confidenceNotes = confidenceNotes
            self.autoPromoted = autoPromoted
            self.autoPromotedAt = autoPromotedAt
            self.autoPromotedBy = autoPromotedBy
            self.provenance = provenance
            self.auditPriority = auditPriority
        }

        /// Window length in seconds.
        var durationSeconds: Double { endSeconds - startSeconds }

        /// Tier derived from window-local metadata. Parent metadata is applied by
        /// ``CorpusAnnotation/labelTier(for:)``.
        var labelTier: LabelTier {
            CorpusAnnotation.derivedTier(
                autoPromoted: autoPromoted == true || autoPromotedAt != nil || autoPromotedBy != nil,
                provenance: provenance,
                auditPriority: auditPriority
            )
        }
    }

    enum AdType: String, Sendable, Codable, Equatable {
        case hostRead = "host_read"
        case dynamicInsertion = "dynamic_insertion"
        case blendedHostRead = "blended_host_read"
        case producedSegment = "produced_segment"
        case promo = "promo"
        /// Rediff-confirmed Dynamic Ad Insertion. Distinct from
        /// `dynamicInsertion` because `scripts/l2f-auto-promote.py`
        /// emits `"dai"` for rules R1/R3 (rediff-physical-DAI
        /// promotions) to preserve the rediff-provenance signal,
        /// while leaving `dynamic_insertion` available for
        /// drafter-passthrough cases that lack rediff confirmation.
        case dai = "dai"
    }

    enum TransitionType: String, Sendable, Codable, Equatable {
        /// Clear marker like "and now a word from our sponsor".
        case explicit
        /// Music or stinger, no spoken cue.
        case musical
        /// Direct cut with no signpost.
        case hardCut = "hard_cut"
        /// Host weaves the sponsor mention in mid-thought.
        case blended
    }

    // MARK: ContentWindow

    /// A labeled content window. The simulator must never skip into these.
    struct ContentWindow: Sendable, Codable, Equatable {
        let startSeconds: Double
        let endSeconds: Double
        /// Free-form notes for the annotator, e.g. "Interview content — must NEVER be skipped".
        let notes: String?

        var durationSeconds: Double { endSeconds - startSeconds }
    }

    /// Apply both episode- and window-level provenance. Automated parent
    /// metadata always wins, so an auto-promoted record can never become gold.
    func labelTier(for window: AdWindow) -> LabelTier {
        let sourceEpisodeTier = Self.derivedTier(
            autoPromoted: hasAutomaticEpisodeMarker,
            provenance: provenance,
            auditPriority: auditPriority
        )
        let episodeTier: LabelTier = sourceEpisodeTier == .gold && !hasVerifiedReviewAttestations
            ? .silver : sourceEpisodeTier
        if episodeTier == .boundaryProposal || window.labelTier == .boundaryProposal {
            return .boundaryProposal
        }
        if episodeTier == .silver || window.labelTier == .silver {
            return .silver
        }
        return .gold
    }

    /// Episode tier is the least-trusted tier present. Gold requires the
    /// explicit second-listener provenance transition.
    var labelTier: LabelTier {
        let sourceEpisodeTier = Self.derivedTier(
            autoPromoted: hasAutomaticEpisodeMarker,
            provenance: provenance,
            auditPriority: auditPriority
        )
        let episodeTier: LabelTier = sourceEpisodeTier == .gold && !hasVerifiedReviewAttestations
            ? .silver : sourceEpisodeTier
        return adWindows.reduce(episodeTier) { current, window in
            let tier = labelTier(for: window)
            if current == .boundaryProposal || tier == .boundaryProposal {
                return .boundaryProposal
            }
            if current == .silver || tier == .silver {
                return .silver
            }
            return .gold
        }
    }

    /// Gold precision/recall gates score whole episodes. Mixing an automatic
    /// window into a human episode would otherwise make a valid prediction on
    /// that window look like a gold false positive, so only all-gold episodes
    /// are eligible.
    var isEligibleForGoldEvaluation: Bool {
        labelTier == .gold
    }

    private var hasAutomaticEpisodeMarker: Bool {
        autoPromoted == true || autoPromotedAt != nil || autoPromotedBy != nil
    }

    var hasVerifiedReviewAttestations: Bool {
        guard let attestations = reviewAttestations, attestations.count == 2 else {
            return false
        }
        guard reviewAttestationsAreWellFormed else { return false }
        let reviewerLocale = Locale(identifier: "en_US_POSIX")
        let reviewers = Set(attestations.map {
            $0.reviewer.folding(options: [.caseInsensitive], locale: reviewerLocale)
        })
        let artifacts = Set(attestations.map(\.reviewArtifactId))
        return reviewers.count == 2 && artifacts.count == 2
    }

    var reviewAttestationsAreWellFormed: Bool {
        guard let attestations = reviewAttestations else { return true }
        let fingerprintPattern = #"^sha256:[0-9a-f]{64}$"#
        return attestations.allSatisfy({ attestation in
            let reviewer = attestation.reviewer.trimmingCharacters(in: .whitespacesAndNewlines)
            return !reviewer.isEmpty
                && reviewer == attestation.reviewer
                && attestation.audioFingerprint == audioFingerprint
                && attestation.reviewArtifactId.range(
                    of: fingerprintPattern,
                    options: .regularExpression
                ) != nil
                && Self.isCanonicalReviewTimestamp(attestation.reviewedAt)
        })
    }

    static func isCanonicalReviewTimestamp(_ value: String) -> Bool {
        guard value.range(
            of: #"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"#,
            options: .regularExpression
        ) != nil else { return false }
        let formatter = ISO8601DateFormatter()
        guard let parsed = formatter.date(from: value) else { return false }
        return formatter.string(from: parsed) == value
    }

    private static let automaticProvenance = Set(["rediff", "drafter", "pipeline"])
    private static let humanProvenance = Set(["human_reviewed"])

    var hasHumanOnlyProvenance: Bool {
        guard let provenance, !provenance.isEmpty else { return false }
        return Set(provenance.map { $0.lowercased() }) == Self.humanProvenance
    }

    private static func derivedTier(
        autoPromoted: Bool?,
        provenance: [String]?,
        auditPriority: Int?
    ) -> LabelTier {
        if auditPriority == 1 {
            return .boundaryProposal
        }
        if autoPromoted == true || auditPriority != nil {
            return .silver
        }
        guard let provenance else {
            // Missing provenance is not evidence of the second-listener gate.
            return .silver
        }
        guard !provenance.isEmpty else {
            // An explicit but empty source list is not evidence of human review.
            return .silver
        }
        let normalized = Set(provenance.map { $0.lowercased() })
        if !normalized.isDisjoint(with: automaticProvenance) {
            return .silver
        }
        if normalized.isSubset(of: humanProvenance) {
            return .gold
        }
        // Unknown provenance is preserved but cannot silently acquire gold status.
        return .silver
    }

    // MARK: - Decoder / Encoder

    /// Shared JSON decoder configured for the corpus snake-case format.
    static var decoder: JSONDecoder {
        let d = JSONDecoder()
        d.keyDecodingStrategy = .convertFromSnakeCase
        return d
    }

    /// Shared JSON encoder. Used by tests and tooling that round-trip
    /// annotations.
    static var encoder: JSONEncoder {
        let e = JSONEncoder()
        e.keyEncodingStrategy = .convertToSnakeCase
        e.outputFormatting = [.prettyPrinted, .sortedKeys]
        return e
    }
}
