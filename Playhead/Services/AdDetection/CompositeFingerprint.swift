// CompositeFingerprint.swift
// playhead-ef2.5.5: Composite fingerprint upgrade — combines transcript,
// acoustic, and sponsor-marker signatures for more robust ad copy matching.
//
// Same-show matching only. Cross-show matching is future work (separate bead).
//
// Design:
//   - TranscriptSignature: character 3-gram + MinHash for ASR-robust text matching
//   - AcousticSignature: lightweight acoustic sketch (music bed, prosody, spectral)
//   - SponsorMarkerSignature: structural marker presence + positions
//   - CompositeFingerprint: weighted combination with transcript-reliability gating
//   - CompositeFingerprintBuilder: construction from raw features

import Foundation

// MARK: - TranscriptSignature

/// Text-based fingerprint using character 3-grams and MinHash.
/// ASR-robust: character n-grams tolerate transcription errors better than
/// word-level features because a single misrecognized word only corrupts
/// a few n-grams rather than an entire token.
struct TranscriptSignature: Sendable, Equatable {
    /// Character 3-gram set from normalized transcript text.
    let ngramSet: Set<String>
    /// MinHash signature for fast approximate Jaccard estimation.
    /// Uses the same hash infrastructure as AdCopyFingerprintStore.
    let minhashSignature: [UInt64]

    /// Approximate Jaccard similarity via MinHash.
    func similarity(to other: TranscriptSignature) -> Float {
        Float(MinHashUtilities.jaccardSimilarity(minhashSignature, other.minhashSignature))
    }
}

// MARK: - TranscriptSignature n-gram config

private enum TranscriptNgramConfig {
    /// Character n-gram size. 3-grams are more robust to single-character
    /// ASR errors than the 4-grams used in AdCopyFingerprintStore.
    static let ngramSize = 3
}

// MARK: - AcousticSignature

/// Lightweight acoustic sketch for an ad span. Not a full audio fingerprint
/// (that would be substantial engineering) — instead captures coarse
/// structural features that recur across instances of the same ad.
struct AcousticSignature: Sendable, Equatable {
    /// Whether a music bed was detected in this span.
    let hasMusicBed: Bool
    /// Simplified energy contour of the music bed, 8-16 buckets.
    /// nil if no music bed detected.
    let musicBedContour: [Float]?
    /// Prosodic rhythm regularity (0-1). Professional ad reads tend toward
    /// higher steadiness than conversational speech.
    let avgProsodySteadiness: Float
    /// 8-bucket spectral energy summary. nil if not available.
    let spectralSketch: [Float]?

    /// Similarity to another acoustic signature. Weighted combination:
    /// - Music bed presence match: 0.3
    /// - Music bed contour correlation: 0.2 (when both present)
    /// - Prosody steadiness proximity: 0.3
    /// - Spectral sketch correlation: 0.2 (when both present)
    func similarity(to other: AcousticSignature) -> Float {
        var score: Float = 0
        var totalWeight: Float = 0

        // Music bed presence (0.3 weight)
        let musicWeight: Float = 0.3
        totalWeight += musicWeight
        if hasMusicBed == other.hasMusicBed {
            score += musicWeight
        }

        // Music bed contour (0.2 weight, when both available)
        if let c1 = musicBedContour, let c2 = other.musicBedContour,
           !c1.isEmpty, !c2.isEmpty {
            let contourWeight: Float = 0.2
            totalWeight += contourWeight
            score += contourWeight * cosineSimilarity(c1, c2)
        }

        // Prosody steadiness (0.3 weight)
        let prosodyWeight: Float = 0.3
        totalWeight += prosodyWeight
        let prosodyDiff = abs(avgProsodySteadiness - other.avgProsodySteadiness)
        score += prosodyWeight * max(0, 1.0 - prosodyDiff)

        // Spectral sketch (0.2 weight, when both available)
        if let s1 = spectralSketch, let s2 = other.spectralSketch,
           !s1.isEmpty, !s2.isEmpty {
            let spectralWeight: Float = 0.2
            totalWeight += spectralWeight
            score += spectralWeight * cosineSimilarity(s1, s2)
        }

        guard totalWeight > 0 else { return 0 }
        return score / totalWeight
    }
}

// MARK: - SponsorMarkerSignature

/// Structural sponsor markers within an ad span. The presence and relative
/// position of URLs, promo codes, and disclosures are strong recurring
/// signals for the same ad copy.
struct SponsorMarkerSignature: Sendable, Equatable {
    let hasURL: Bool
    let hasPromoCode: Bool
    let hasDisclosure: Bool
    /// Normalized position (0-1) within the span. nil if marker absent.
    let urlPosition: Float?
    let promoCodePosition: Float?
    let disclosurePosition: Float?

    /// Similarity based on marker presence and position alignment.
    /// Marker presence match contributes 0.6, position alignment 0.4.
    func similarity(to other: SponsorMarkerSignature) -> Float {
        var presenceScore: Float = 0
        var presenceCount: Float = 0
        var positionScore: Float = 0
        var positionCount: Float = 0

        // URL
        presenceCount += 1
        if hasURL == other.hasURL { presenceScore += 1 }
        if let p1 = urlPosition, let p2 = other.urlPosition {
            positionCount += 1
            positionScore += max(0, 1.0 - abs(p1 - p2) * 5.0)
        }

        // Promo code
        presenceCount += 1
        if hasPromoCode == other.hasPromoCode { presenceScore += 1 }
        if let p1 = promoCodePosition, let p2 = other.promoCodePosition {
            positionCount += 1
            positionScore += max(0, 1.0 - abs(p1 - p2) * 5.0)
        }

        // Disclosure
        presenceCount += 1
        if hasDisclosure == other.hasDisclosure { presenceScore += 1 }
        if let p1 = disclosurePosition, let p2 = other.disclosurePosition {
            positionCount += 1
            positionScore += max(0, 1.0 - abs(p1 - p2) * 5.0)
        }

        let normalizedPresence = presenceCount > 0 ? presenceScore / presenceCount : 0
        // When no positions are available, use a neutral midpoint rather than
        // echoing presence score (which would double-count presence match).
        let normalizedPosition = positionCount > 0 ? positionScore / positionCount : 0.5

        return normalizedPresence * 0.6 + normalizedPosition * 0.4
    }
}

// MARK: - CompositeFingerprint

/// Combines transcript, acoustic, and sponsor-marker signatures for
/// robust ad copy matching. Any sub-signature may be nil (missing data).
/// The composite score dynamically re-weights based on available signals
/// and transcript reliability.
struct CompositeFingerprint: Sendable, Equatable {
    let transcriptSignature: TranscriptSignature?
    let acousticSignature: AcousticSignature?
    let sponsorMarkers: SponsorMarkerSignature?

    /// Weighted composite similarity score.
    ///
    /// Base weights:
    ///   - Text:     0.5 x transcriptReliability (drops to 0 when unreliable)
    ///   - Acoustic:  0.3
    ///   - Marker:    0.2
    ///
    /// Weights are normalized to sum to 1.0 based on which sub-signatures
    /// are present in *both* fingerprints being compared.
    ///
    /// Same-show matching threshold: compositeScore >= 0.7 + anchor alignment
    /// + ownership validation. Cross-show matching is future work.
    func compositeScore(
        against other: CompositeFingerprint,
        transcriptReliability: Float
    ) -> Float {
        let clampedReliability = min(max(transcriptReliability, 0), 1)

        // Transcript reliability gate: below 0.3, text gets zero weight.
        let effectiveTextReliability: Float = clampedReliability < 0.3 ? 0 : clampedReliability

        var weightedSum: Float = 0
        var totalWeight: Float = 0

        // Text similarity
        if let ts1 = transcriptSignature, let ts2 = other.transcriptSignature,
           effectiveTextReliability > 0 {
            let textWeight: Float = 0.5 * effectiveTextReliability
            totalWeight += textWeight
            weightedSum += textWeight * ts1.similarity(to: ts2)
        }

        // Acoustic similarity
        if let as1 = acousticSignature, let as2 = other.acousticSignature {
            let acousticWeight: Float = 0.3
            totalWeight += acousticWeight
            weightedSum += acousticWeight * as1.similarity(to: as2)
        }

        // Marker similarity
        if let sm1 = sponsorMarkers, let sm2 = other.sponsorMarkers {
            let markerWeight: Float = 0.2
            totalWeight += markerWeight
            weightedSum += markerWeight * sm1.similarity(to: sm2)
        }

        guard totalWeight > 0 else { return 0 }
        return weightedSum / totalWeight
    }
}

// MARK: - CompositeFingerprintBuilder

/// Factory for constructing composite fingerprints from raw features.
/// Stateless — all methods are static.
enum CompositeFingerprintBuilder {

    /// Build a transcript signature from raw text.
    /// Returns nil if the text is empty after normalization.
    static func buildTranscriptSignature(
        text: String,
        transcriptReliability: Float
    ) -> TranscriptSignature? {
        // Gate: if reliability is too low, don't bother building the signature.
        // The composite scorer will also gate, but skipping construction saves work.
        guard transcriptReliability >= 0.3 else { return nil }

        let normalized = MinHashUtilities.normalizeText(text)
        guard !normalized.isEmpty else { return nil }

        let ngrams = generate3Grams(normalized)
        guard !ngrams.isEmpty else { return nil }

        let minhash = MinHashUtilities.computeMinHash(features: ngrams)
        return TranscriptSignature(ngramSet: ngrams, minhashSignature: minhash)
    }

    /// Build an acoustic signature from feature inputs.
    static func buildAcousticSignature(
        hasMusicBed: Bool,
        musicBedContour: [Float]? = nil,
        avgProsodySteadiness: Float,
        spectralSketch: [Float]? = nil
    ) -> AcousticSignature {
        AcousticSignature(
            hasMusicBed: hasMusicBed,
            musicBedContour: musicBedContour,
            avgProsodySteadiness: min(max(avgProsodySteadiness, 0), 1),
            spectralSketch: spectralSketch
        )
    }

    /// Build a sponsor marker signature from detection results.
    static func buildSponsorMarkerSignature(
        hasURL: Bool,
        hasPromoCode: Bool,
        hasDisclosure: Bool,
        urlPosition: Float? = nil,
        promoCodePosition: Float? = nil,
        disclosurePosition: Float? = nil
    ) -> SponsorMarkerSignature {
        SponsorMarkerSignature(
            hasURL: hasURL,
            hasPromoCode: hasPromoCode,
            hasDisclosure: hasDisclosure,
            urlPosition: hasURL ? urlPosition : nil,
            promoCodePosition: hasPromoCode ? promoCodePosition : nil,
            disclosurePosition: hasDisclosure ? disclosurePosition : nil
        )
    }

    /// Build a complete composite fingerprint from all available features.
    static func build(
        text: String? = nil,
        transcriptReliability: Float = 1.0,
        hasMusicBed: Bool? = nil,
        musicBedContour: [Float]? = nil,
        avgProsodySteadiness: Float? = nil,
        spectralSketch: [Float]? = nil,
        hasURL: Bool? = nil,
        hasPromoCode: Bool? = nil,
        hasDisclosure: Bool? = nil,
        urlPosition: Float? = nil,
        promoCodePosition: Float? = nil,
        disclosurePosition: Float? = nil
    ) -> CompositeFingerprint {
        let transcriptSig: TranscriptSignature?
        if let text {
            transcriptSig = buildTranscriptSignature(
                text: text,
                transcriptReliability: transcriptReliability
            )
        } else {
            transcriptSig = nil
        }

        let acousticSig: AcousticSignature?
        if let hasMusicBed, let avgProsodySteadiness {
            acousticSig = buildAcousticSignature(
                hasMusicBed: hasMusicBed,
                musicBedContour: musicBedContour,
                avgProsodySteadiness: avgProsodySteadiness,
                spectralSketch: spectralSketch
            )
        } else {
            acousticSig = nil
        }

        let markerSig: SponsorMarkerSignature?
        if let hasURL, let hasPromoCode, let hasDisclosure {
            markerSig = buildSponsorMarkerSignature(
                hasURL: hasURL,
                hasPromoCode: hasPromoCode,
                hasDisclosure: hasDisclosure,
                urlPosition: urlPosition,
                promoCodePosition: promoCodePosition,
                disclosurePosition: disclosurePosition
            )
        } else {
            markerSig = nil
        }

        return CompositeFingerprint(
            transcriptSignature: transcriptSig,
            acousticSignature: acousticSig,
            sponsorMarkers: markerSig
        )
    }

    // MARK: - Character 3-gram generation

    /// Generate character 3-grams from normalized text. Uses a smaller n-gram
    /// size than AdCopyFingerprintStore's 4-grams for better ASR error tolerance.
    static func generate3Grams(_ text: String) -> Set<String> {
        let size = TranscriptNgramConfig.ngramSize
        guard text.count >= size else {
            return text.isEmpty ? [] : [text]
        }
        var ngrams = Set<String>()
        let chars = Array(text)
        for i in 0...(chars.count - size) {
            ngrams.insert(String(chars[i..<(i + size)]))
        }
        return ngrams
    }
}

// MARK: - Cosine similarity helper

/// Cosine similarity between two Float vectors. Mismatched lengths are
/// handled by zero-padding the shorter vector (extra dimensions in the
/// longer vector contribute to its norm, reducing similarity). Returns 0
/// for zero-length or zero-norm vectors. Negative cosine values are
/// clamped to 0 since anti-correlation is not meaningful for this use case.
func cosineSimilarity(_ a: [Float], _ b: [Float]) -> Float {
    let len = max(a.count, b.count)
    guard len > 0 else { return 0 }

    var dot: Float = 0
    var normA: Float = 0
    var normB: Float = 0
    for i in 0..<len {
        let va = i < a.count ? a[i] : 0
        let vb = i < b.count ? b[i] : 0
        dot += va * vb
        normA += va * va
        normB += vb * vb
    }
    let denom = (normA * normB).squareRoot()
    guard denom > 0 else { return 0 }
    return max(0, dot / denom)
}
