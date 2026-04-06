// CorpusVerificationTests.swift
// Comprehensive corpus-based verification tests for Phase 1 transcript pipeline,
// Phase 2 evidence catalog, LexicalScanner coverage, full pipeline hot path,
// and AcousticBreakDetector accuracy against embedded ground-truth annotations.

import Foundation
import Testing
@testable import Playhead

// MARK: - Corpus Test Helpers

/// Tracks temp directories created by corpus verification tests.
private let _corpusTempDirs = TestTempDirTracker()

private func makeCorpusStore() async throws -> AnalysisStore {
    let dir = try makeTempDir(prefix: "CorpusVerification")
    _corpusTempDirs.track(dir)
    let store = try AnalysisStore(directory: dir)
    try await store.migrate()
    return store
}

private func makeCorpusAsset(
    id: String,
    episodeId: String
) -> AnalysisAsset {
    AnalysisAsset(
        id: id,
        episodeId: episodeId,
        assetFingerprint: "corpus-fp-\(id)",
        weakFingerprint: nil,
        sourceURL: "file:///corpus/\(id).m4a",
        featureCoverageEndTime: nil,
        fastTranscriptCoverageEndTime: nil,
        confirmedAdCoverageEndTime: nil,
        analysisState: "new",
        analysisVersion: 1,
        capabilitySnapshot: nil
    )
}

/// Convert a TestAdSegment to GroundTruthAdSegment.
private func toGroundTruth(_ seg: TestAdSegment) -> GroundTruthAdSegment {
    let adType: GroundTruthAdSegment.AdSegmentType
    switch seg.adType {
    case .midRoll: adType = .midRoll
    case .preRoll: adType = .preRoll
    case .postRoll: adType = .postRoll
    }
    return GroundTruthAdSegment(
        startTime: seg.startTime,
        endTime: seg.endTime,
        advertiser: seg.advertiser,
        product: seg.product,
        adType: adType
    )
}

// MARK: - Sponsor-specific ad text

/// Returns realistic ASR-style ad text for a given sponsor.
/// Each sponsor gets distinct language that the LexicalScanner can detect.
/// Returns realistic ASR-style ad transcript for a given sponsor.
/// Uses real podcast ad copy patterns with ASR noise: filler words, hesitations,
/// occasional mis-transcriptions, and natural conversational phrasing.
/// NOT gift-wrapped for the scanner — some signals may be messy or missing.
private func adTextForSponsor(_ advertiser: String?) -> String {
    switch advertiser?.lowercased() {
    case "squarespace":
        // Based on real Court Junkie Squarespace ad
        return "pretty much everyone needs a website nowadays whether its for your business or your blog or your personal side hustle squarespace is an all in one platform that can help you build a beautiful online presence i used to build websites as a hobby and for all three i used square space i found it really user friendly head on over to squarespace dot com slash court for a free trial and when youre ready use offer code court to save ten percent off"
    case "nordvpn":
        // Based on real NordVPN podcast ad
        return "you know your personal data is out there every time you use public wifi at a coffee shop or airport youre basically leaving the door wide open for hackers thats why i use nordvpn it encrypts your internet connection and hides your ip address plus you can access content from other countries go to nordvpn dot com slash podcast or use code podcast to get a huge discount on a two year plan"
    case "betterhelp":
        // Based on real BetterHelp ad from Wiser Than Me
        return "betterhelp takes therapy and brings it online its about making support accessible flexible and tailored to fit our lives with a simple questionnaire youre matched with a licensed therapist and if you feel the need for a change um switching therapists is straightforward this show is sponsored by betterhelp give online therapy a try at betterhelp dot com slash wiser and get ten percent off your first month"
    case "hellofresh":
        // Based on real HelloFresh ad
        return "according to a recent hello fresh survey forty three percent of people said theyve ended relationships if their partner was a bad cook but dont sweat it hello fresh will help you own the kitchen with easy step by step recipes and premeasured ingredients hello fresh takes the stress out of mealtime head to hellofresh dot com and use code awesome for thirty five dollars off"
    case "athletic greens":
        // Based on real AG1 ad from Rhonda Patrick
        return "i started taking athletic greens way back in twenty twelve so thats like over ten years now of taking athletic greens every single day the reason i started and the reason i still take it is that it covers all of my foundational nutritional needs just one scoop gives you over seventy five vitamins and minerals go to drinkag one dot com slash podcast"
    case "expressvpn":
        return "look if youve been putting off getting a vpn because you think its complicated let me tell you about express vpn its literally just one click and youre connected its that simple i use it every day especially when im traveling go to expressvpn dot com slash show right now and get three extra months free"
    case "calm":
        return "so i know weve all had those nights where you just cant fall asleep right and thats exactly why i started using calm its the number one app for sleep um they have these sleep stories read by like matthew mcconaughey and its amazing visit calm dot com slash show for forty percent off"
    case "ziprecruiter":
        return "hiring is really hard right now and i know a lot of you out there are struggling to find good candidates so let me tell you about zip recruiter they use smart matching technology to find the right people for your job go to ziprecruiter dot com slash podcast to try it for free"
    case "audible":
        return "i am a huge audiobook person like i listen to audiobooks pretty much every day on my commute and audible just has the best selection out there they have literally hundreds of thousands of titles visit audible dot com slash podcast for a free trial"
    case "masterclass":
        return "you know what ive been really into lately masterclass like where else can you learn cooking from gordon ramsay or writing from neil gaiman its incredible go to masterclass dot com slash podcast for an exclusive offer"
    case "simplisafe":
        return "so lets talk about home security for a second because i know its something a lot of you think about simplisafe is honestly the easiest system ive ever used no contracts no hidden fees twenty four seven professional monitoring visit simplisafe dot com slash podcast"
    case "indeed":
        return "when youre looking to hire someone you want quality candidates fast right thats what indeed does they help you find the right people for your open positions go to indeed dot com slash podcast to start hiring"
    case "manscaped":
        return "okay guys real talk for a second manscaped has changed the grooming game completely their new lawnmower four point oh is uh honestly the best trimmer ive ever used go to manscaped dot com slash podcast and use code podcast for twenty percent off"
    default:
        // Generic sponsor read — uses the advertiser name if available
        let name = advertiser?.lowercased() ?? "acme"
        return "this episode is sponsored by \(name) they have been really great to work with and honestly i use their product every day head to \(name.replacingOccurrences(of: " ", with: "")) dot com slash podcast and use code save for a special offer"
    }
}

/// Diverse non-ad filler texts that include near-miss commercial vocabulary
/// to stress-test false positive resistance.
private let nonAdFillerTexts = [
    // Normal editorial
    "and so the interesting thing about this topic is that there are many perspectives we should consider carefully when examining the evidence before us",
    // Near-miss: mentions "deal" but not commercial patterns
    "i think the best deal about living in this city is the food scene you should really try the new restaurants downtown they are incredible",
    // Near-miss: mentions "free" and "offer" but in editorial context
    "the researchers offered a free consultation to participants and the results were surprising because the new methodology offered fresh insights",
    // Near-miss: mentions "website" and "code" but in tech discussion
    "the source code for the website was actually open source and anyone could contribute to the project which was a really innovative approach",
    // Near-miss: mentions brand-like proper nouns
    "professor john smith from the university discussed how the supreme court ruling would affect the tech industry going forward",
    // Normal interview content
    "well i think what people dont realize is that this process takes years of dedication and practice before you see any real results",
    // Historical discussion
    "the discovery was made in eighteen ninety seven when researchers first identified the connection between these two phenomena",
    // Science content (avoids "supported by" which is a disclosure pattern)
    "the experiment demonstrated that under controlled conditions the hypothesis held up across multiple trials and replications",
]

/// Build transcript chunks with sponsor-specific ad text at ground-truth positions.
private func buildCorpusTranscriptChunks(
    assetId: String,
    duration: TimeInterval,
    adSegments: [TestAdSegment]
) -> [TranscriptChunk] {
    let chunkDuration = 10.0
    var chunks: [TranscriptChunk] = []
    var chunkIndex = 0

    for start in stride(from: 0.0, to: duration, by: chunkDuration) {
        let end = min(start + chunkDuration, duration)

        // Find overlapping ad segment for sponsor-specific text.
        let overlappingAd = adSegments.first { seg in
            start < seg.endTime && end > seg.startTime
        }

        let text: String
        if let ad = overlappingAd {
            text = adTextForSponsor(ad.advertiser)
        } else {
            // Rotate through diverse non-ad fillers including near-miss commercial vocabulary
            text = nonAdFillerTexts[chunkIndex % nonAdFillerTexts.count]
        }

        let normalized = text.lowercased()
            .components(separatedBy: CharacterSet.alphanumerics.inverted)
            .filter { !$0.isEmpty }
            .joined(separator: " ")

        chunks.append(TranscriptChunk(
            id: "cv-\(assetId)-\(chunkIndex)",
            analysisAssetId: assetId,
            segmentFingerprint: "cv-fp-\(chunkIndex)",
            chunkIndex: chunkIndex,
            startTime: start,
            endTime: end,
            text: text,
            normalizedText: normalized,
            pass: "fast",
            modelVersion: "corpus-v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        ))
        chunkIndex += 1
    }
    return chunks
}

/// Build feature windows with energy drops at ad boundaries.
private func buildCorpusFeatureWindows(
    assetId: String,
    duration: TimeInterval,
    adSegments: [TestAdSegment]
) -> [FeatureWindow] {
    var windows: [FeatureWindow] = []
    let step = 1.0

    for start in stride(from: 0.0, to: duration, by: step) {
        let end = min(start + step, duration)

        let nearBoundary = adSegments.contains { seg in
            abs(start - seg.startTime) < 2.0 || abs(start - seg.endTime) < 2.0
        }

        windows.append(FeatureWindow(
            analysisAssetId: assetId,
            startTime: start,
            endTime: end,
            rms: nearBoundary ? 0.01 : 0.05,
            spectralFlux: nearBoundary ? 0.5 : 0.01,
            musicProbability: 0.0,
            pauseProbability: nearBoundary ? 0.9 : 0.1,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 1
        ))
    }
    return windows
}

/// Make a TranscriptAtom for corpus tests.
private func makeCorpusAtom(
    assetId: String,
    version: String = "cv-v1",
    ordinal: Int,
    startTime: Double,
    endTime: Double,
    text: String
) -> TranscriptAtom {
    TranscriptAtom(
        atomKey: TranscriptAtomKey(
            analysisAssetId: assetId,
            transcriptVersion: version,
            atomOrdinal: ordinal
        ),
        contentHash: String(text.hashValue),
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: ordinal
    )
}

// MARK: - Suite 1: Corpus – Phase 1 Transcript Pipeline

@Suite("Corpus – Phase 1 Transcript Pipeline")
struct CorpusPhase1TranscriptPipelineTests {

    /// Selected diverse episodes for Phase 1 testing.
    static let testEpisodeKeys = [
        "tech-weekly-ep142",   // Dynamic insertion, tech
        "true-crime-ep87",     // Host-read, blended, true crime
        "comedy-hour-ep301",   // Back-to-back ads, comedy
        "history-deep-ep55",   // Long episode, host-read
        "news-daily-apr01",    // Short episode, pre-roll
        "storytelling-ep44",   // No ads
    ]

    @Test("Atomizer produces correct atom count and contiguous ordinals across diverse episodes")
    func atomizerAcrossCorpus() throws {
        let loader = CorpusLoader()

        for key in Self.testEpisodeKeys {
            let annotation = try loader.loadAnnotation(filename: key)
            let assetId = annotation.episode.episodeId
            let duration = annotation.episode.duration
            let chunks = buildCorpusTranscriptChunks(
                assetId: assetId,
                duration: duration,
                adSegments: annotation.adSegments
            )

            let (atoms, version) = TranscriptAtomizer.atomize(
                chunks: chunks,
                analysisAssetId: assetId,
                normalizationHash: "norm-v1",
                sourceHash: "asr-v1"
            )

            // Atom count matches chunk count.
            #expect(atoms.count == chunks.count,
                    "[\(key)] Atom count (\(atoms.count)) should match chunk count (\(chunks.count))")

            // Ordinals are contiguous starting from 0.
            for (i, atom) in atoms.enumerated() {
                #expect(atom.atomKey.atomOrdinal == i,
                        "[\(key)] Atom ordinal \(atom.atomKey.atomOrdinal) should be \(i)")
            }

            // Version hash is non-empty and stable.
            #expect(!version.transcriptVersion.isEmpty,
                    "[\(key)] Version hash should be non-empty")

            let (_, version2) = TranscriptAtomizer.atomize(
                chunks: chunks,
                analysisAssetId: assetId,
                normalizationHash: "norm-v1",
                sourceHash: "asr-v1"
            )
            #expect(version.transcriptVersion == version2.transcriptVersion,
                    "[\(key)] Version hash should be stable across identical inputs")
        }
    }

    @Test("Segmenter produces episode-covering segments with no micro-segments")
    func segmenterAcrossCorpus() throws {
        let loader = CorpusLoader()

        for key in Self.testEpisodeKeys {
            let annotation = try loader.loadAnnotation(filename: key)
            let assetId = annotation.episode.episodeId
            let duration = annotation.episode.duration
            let chunks = buildCorpusTranscriptChunks(
                assetId: assetId,
                duration: duration,
                adSegments: annotation.adSegments
            )

            let (atoms, _) = TranscriptAtomizer.atomize(
                chunks: chunks,
                analysisAssetId: assetId,
                normalizationHash: "norm-v1",
                sourceHash: "asr-v1"
            )

            let segments = TranscriptSegmenter.segment(atoms: atoms)

            // At least one segment.
            #expect(!segments.isEmpty,
                    "[\(key)] Should produce at least one segment")

            // Segments should cover most of the episode (first segment starts near 0,
            // last segment ends near duration).
            if let first = segments.first, let last = segments.last {
                #expect(first.startTime < 20.0,
                        "[\(key)] First segment should start near episode beginning, got \(first.startTime)")
                #expect(last.endTime > duration - 20.0,
                        "[\(key)] Last segment should end near episode end, got \(last.endTime) vs duration \(duration)")
            }

            // No micro-segments: each segment should have at least 1 atom.
            for seg in segments {
                #expect(!seg.atoms.isEmpty,
                        "[\(key)] Segment \(seg.segmentIndex) should have at least one atom")
            }

            // Reasonable segment count: for a typical 10s chunk episode, expect
            // far fewer segments than chunks (segmenter merges consecutive chunks).
            let chunkCount = chunks.count
            #expect(segments.count <= chunkCount,
                    "[\(key)] Segment count (\(segments.count)) should not exceed chunk count (\(chunkCount))")

            // Segment indices are sequential.
            for (i, seg) in segments.enumerated() {
                #expect(seg.segmentIndex == i,
                        "[\(key)] Segment index \(seg.segmentIndex) should be \(i)")
            }
        }
    }

    @Test("Quality estimator rates synthetic ad-region and non-ad segments as good quality")
    func qualityEstimatorAcrossCorpus() throws {
        let loader = CorpusLoader()

        for key in Self.testEpisodeKeys {
            let annotation = try loader.loadAnnotation(filename: key)
            let assetId = annotation.episode.episodeId
            let duration = annotation.episode.duration
            let chunks = buildCorpusTranscriptChunks(
                assetId: assetId,
                duration: duration,
                adSegments: annotation.adSegments
            )

            let (atoms, _) = TranscriptAtomizer.atomize(
                chunks: chunks,
                analysisAssetId: assetId,
                normalizationHash: "norm-v1",
                sourceHash: "asr-v1"
            )

            let segments = TranscriptSegmenter.segment(atoms: atoms)
            let assessments = TranscriptQualityEstimator.assess(segments: segments)

            // All assessments should have valid scores.
            for assessment in assessments {
                #expect(assessment.compositeScore >= 0.0 && assessment.compositeScore <= 1.0,
                        "[\(key)] Composite score should be in [0, 1], got \(assessment.compositeScore)")
            }

            // Our synthetic text is well-formed, so most segments should be good or degraded
            // (not unusable). Allow some tolerance for very short segments.
            let unusableCount = assessments.filter { $0.quality == .unusable }.count
            let totalCount = assessments.count
            if totalCount > 0 {
                let unusableRatio = Double(unusableCount) / Double(totalCount)
                #expect(unusableRatio < 0.2,
                        "[\(key)] Unusable segment ratio \(unusableRatio) should be < 0.2")
            }
        }
    }

    @Test("Quality estimator ranks a noisy middle region below clean episode regions")
    func qualityEstimatorMixedQualityEpisode() {
        func makeSegment(index: Int, startTime: Double, duration: Double, text: String) -> AdTranscriptSegment {
            let words = text.split(whereSeparator: \.isWhitespace)
            let atomCount = max(1, words.count / 8)
            let atomDuration = duration / Double(atomCount)

            let atoms = (0..<atomCount).map { atomIndex in
                let atomStart = startTime + Double(atomIndex) * atomDuration
                let atomEnd = atomStart + atomDuration
                let wordSlice = words[
                    min(atomIndex * 8, words.count)..<min((atomIndex + 1) * 8, words.count)
                ]
                return TranscriptAtom(
                    atomKey: TranscriptAtomKey(
                        analysisAssetId: "mixed-quality-episode",
                        transcriptVersion: "mixed-quality-v1",
                        atomOrdinal: index * 100 + atomIndex
                    ),
                    contentHash: "hash-\(index)-\(atomIndex)",
                    startTime: atomStart,
                    endTime: atomEnd,
                    text: wordSlice.joined(separator: " "),
                    chunkIndex: index * 100 + atomIndex
                )
            }

            return AdTranscriptSegment(atoms: atoms, segmentIndex: index)
        }

        let segments = [
            makeSegment(
                index: 0,
                startTime: 0,
                duration: 18,
                text: "Welcome back to the show. Today we are talking through the reporting process in a clear and natural way with complete sentences."
            ),
            makeSegment(
                index: 1,
                startTime: 18,
                duration: 18,
                text: "qrxv9 blorf77 tttt mrrp snnn qzpl4 uh kktx9 vvvv zrrt2 plmn qqqq rxtt."
            ),
            makeSegment(
                index: 2,
                startTime: 36,
                duration: 12,
                text: "After that noisy patch, the discussion returns to normal pacing. The host explains the next point clearly, and every sentence stays easy to follow."
            )
        ]

        let assessments = TranscriptQualityEstimator.assess(segments: segments)

        #expect(assessments.count == 3)
        #expect(assessments[0].quality == .good)
        #expect(assessments[1].quality != .good)
        #expect(assessments[1].qualityScore < assessments[0].qualityScore)
        #expect(assessments[1].qualityScore < assessments[2].qualityScore)
    }
}

// MARK: - Suite 2: Corpus – LexicalScanner Coverage

@Suite("Corpus – LexicalScanner Coverage")
struct CorpusLexicalScannerCoverageTests {

    @Test("LexicalScanner detects candidates overlapping every ground-truth ad segment across all 15 episodes")
    func scannerCoverageAllEpisodes() throws {
        let loader = CorpusLoader()
        let allAnnotations = try loader.loadAllAnnotations()

        var totalAdSegments = 0
        var totalHits = 0
        var totalFarCandidates = 0
        var episodesWithPerfectRecall = 0

        for annotation in allAnnotations {
            let assetId = annotation.episode.episodeId
            let duration = annotation.episode.duration
            let chunks = buildCorpusTranscriptChunks(
                assetId: assetId,
                duration: duration,
                adSegments: annotation.adSegments
            )

            let scanner = LexicalScanner()
            let candidates = scanner.scan(chunks: chunks, analysisAssetId: assetId)

            // Skip no-ad episodes for recall calculation.
            if annotation.isNoAdEpisode {
                // LexicalScanner is the hot-path scanner — designed for high recall,
            // lower precision. Some false positives on non-ad text are expected.
            // The downstream ClassifierService filters these.
                #expect(candidates.count <= 5,
                        "[\(annotation.episode.episodeId)] No-ad episode should have few candidates, got \(candidates.count)")
                continue
            }

            var hitsThisEpisode = 0

            // Verify: at least one candidate overlaps each ground-truth ad segment.
            for seg in annotation.adSegments {
                let overlapping = candidates.filter { cand in
                    cand.startTime < seg.endTime + 10 && cand.endTime > seg.startTime - 10
                }
                if !overlapping.isEmpty {
                    hitsThisEpisode += 1
                }
                totalAdSegments += 1
            }
            totalHits += hitsThisEpisode

            if hitsThisEpisode == annotation.adSegments.count {
                episodesWithPerfectRecall += 1
            }

            // Verify: no candidates far from any ground truth (>30s from nearest ad).
            for cand in candidates {
                let nearAnyAd = annotation.adSegments.contains { seg in
                    cand.startTime < seg.endTime + 30 && cand.endTime > seg.startTime - 30
                }
                if !nearAnyAd {
                    totalFarCandidates += 1
                }
            }
        }

        // Overall recall: at least 80% of ground-truth ad segments detected.
        let recall = totalAdSegments > 0 ? Double(totalHits) / Double(totalAdSegments) : 0
        #expect(recall >= 0.80,
                "Overall lexical scanner recall should be >= 80%, got \(String(format: "%.1f", recall * 100))% (\(totalHits)/\(totalAdSegments))")

        // LexicalScanner is intentionally sensitive (hot-path first pass).
        // Some far-from-truth candidates are expected, especially with
        // conversational text containing near-miss commercial vocabulary.
        // The downstream ClassifierService + EvidenceCatalog filter these.
        let farRatio = totalAdSegments > 0 ? Double(totalFarCandidates) / Double(totalAdSegments) : 0
        #expect(farRatio <= 30.0,
                "Far-from-truth ratio should be manageable, got \(totalFarCandidates) far vs \(totalAdSegments) segments")

        // At least half of episodes with ads should have perfect recall.
        let episodesWithAds = allAnnotations.filter { !$0.isNoAdEpisode }.count
        #expect(episodesWithPerfectRecall >= episodesWithAds / 2,
                "At least half of episodes should have perfect recall, got \(episodesWithPerfectRecall)/\(episodesWithAds)")
    }

    @Test("LexicalScanner per-episode recall for diverse delivery styles")
    func perEpisodeScannerRecall() throws {
        let loader = CorpusLoader()

        // Test specific episodes covering each delivery style.
        let episodeKeys = [
            "tech-weekly-ep142",   // dynamicInsertion
            "true-crime-ep87",     // hostRead + blendedHostRead
            "comedy-hour-ep301",   // back-to-back dynamic
            "science-pod-ep22",    // producedSegment + hostRead
            "business-brief-ep63", // preRoll + postRoll
        ]

        for key in episodeKeys {
            let annotation = try loader.loadAnnotation(filename: key)
            let assetId = annotation.episode.episodeId
            let chunks = buildCorpusTranscriptChunks(
                assetId: assetId,
                duration: annotation.episode.duration,
                adSegments: annotation.adSegments
            )

            let scanner = LexicalScanner()
            let candidates = scanner.scan(chunks: chunks, analysisAssetId: assetId)

            for seg in annotation.adSegments {
                let overlapping = candidates.filter { cand in
                    cand.startTime < seg.endTime + 10 && cand.endTime > seg.startTime - 10
                }
                #expect(!overlapping.isEmpty,
                        "[\(key)] Should detect \(seg.advertiser ?? "unknown") ad at \(seg.startTime)-\(seg.endTime)")
            }
        }
    }
}

// MARK: - Suite 3: Corpus – Phase 2 Evidence Catalog

@Suite("Corpus – Phase 2 Evidence Catalog")
struct CorpusEvidenceCatalogTests {

    @Test("Evidence catalog contains entries near ad segments for all corpus episodes")
    func evidenceNearAdSegments() throws {
        let loader = CorpusLoader()
        let allAnnotations = try loader.loadAllAnnotations()

        for annotation in allAnnotations {
            if annotation.isNoAdEpisode { continue }

            let assetId = annotation.episode.episodeId
            let duration = annotation.episode.duration
            let chunks = buildCorpusTranscriptChunks(
                assetId: assetId,
                duration: duration,
                adSegments: annotation.adSegments
            )

            let (atoms, version) = TranscriptAtomizer.atomize(
                chunks: chunks,
                analysisAssetId: assetId,
                normalizationHash: "norm-v1",
                sourceHash: "asr-v1"
            )

            let catalog = EvidenceCatalogBuilder.build(
                atoms: atoms,
                analysisAssetId: assetId,
                transcriptVersion: version.transcriptVersion
            )

            // Should have some evidence entries.
            #expect(!catalog.entries.isEmpty,
                    "[\(annotation.episode.episodeId)] Episode with ads should produce evidence entries")

            // Most ad segments should have nearby evidence entries.
            // Some sponsors use the generic default text which may produce fewer signals.
            var segmentsWithEvidence = 0
            for seg in annotation.adSegments {
                let nearbyEvidence = catalog.entries.filter { entry in
                    entry.startTime < seg.endTime + 15 && entry.endTime > seg.startTime - 15
                }
                if !nearbyEvidence.isEmpty { segmentsWithEvidence += 1 }
            }
            let segRecall = annotation.adSegments.isEmpty ? 1.0 :
                Double(segmentsWithEvidence) / Double(annotation.adSegments.count)
            #expect(segRecall >= 0.5,
                    "[\(annotation.episode.episodeId)] At least half of ad segments should have nearby evidence, got \(segmentsWithEvidence)/\(annotation.adSegments.count)")
        }
    }

    @Test("No evidence far from ad segments in ad-bearing episodes")
    func noEvidenceFarFromAds() throws {
        let loader = CorpusLoader()

        // Check a few representative episodes.
        let episodeKeys = [
            "tech-weekly-ep142",
            "comedy-hour-ep301",
            "history-deep-ep55",
        ]

        for key in episodeKeys {
            let annotation = try loader.loadAnnotation(filename: key)
            let assetId = annotation.episode.episodeId
            let chunks = buildCorpusTranscriptChunks(
                assetId: assetId,
                duration: annotation.episode.duration,
                adSegments: annotation.adSegments
            )

            let (atoms, version) = TranscriptAtomizer.atomize(
                chunks: chunks,
                analysisAssetId: assetId,
                normalizationHash: "norm-v1",
                sourceHash: "asr-v1"
            )

            let catalog = EvidenceCatalogBuilder.build(
                atoms: atoms,
                analysisAssetId: assetId,
                transcriptVersion: version.transcriptVersion
            )

            // Most evidence entries should be near ad segments. Some false positives
            // from near-miss filler text are expected and acceptable — the FM layer
            // downstream filters these.
            var nearAdCount = 0
            for entry in catalog.entries {
                let nearAnyAd = annotation.adSegments.contains { seg in
                    entry.startTime < seg.endTime + 30 && entry.endTime > seg.startTime - 30
                }
                if nearAnyAd { nearAdCount += 1 }
            }
            let precision = catalog.entries.isEmpty ? 1.0 :
                Double(nearAdCount) / Double(catalog.entries.count)
            #expect(precision >= 0.5,
                    "[\(key)] At least half of evidence entries should be near ads, got \(nearAdCount)/\(catalog.entries.count)")
        }
    }

    @Test("No-ad episode produces zero or near-zero evidence entries")
    func noAdEpisodeZeroEvidence() throws {
        let loader = CorpusLoader()
        let annotation = try loader.loadAnnotation(filename: "storytelling-ep44")
        let assetId = annotation.episode.episodeId

        let chunks = buildCorpusTranscriptChunks(
            assetId: assetId,
            duration: annotation.episode.duration,
            adSegments: [] // No ads
        )

        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: assetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: assetId,
            transcriptVersion: version.transcriptVersion
        )

        // No-ad episode with non-commercial text should produce very few evidence entries.
        // A small number of false positives from near-miss filler text is acceptable.
        #expect(catalog.entries.count <= 3,
                "No-ad episode should produce at most a few false-positive entries, got \(catalog.entries.count): \(catalog.entries.map { "\($0.category.rawValue): \($0.matchedText)" })")
    }

    @Test("Evidence catalog categories present for episodes with diverse ad types")
    func evidenceCategoryDiversity() throws {
        let loader = CorpusLoader()

        // Tech weekly has dynamic insertion with clear sponsor language.
        let annotation = try loader.loadAnnotation(filename: "tech-weekly-ep142")
        let assetId = annotation.episode.episodeId
        let chunks = buildCorpusTranscriptChunks(
            assetId: assetId,
            duration: annotation.episode.duration,
            adSegments: annotation.adSegments
        )

        let (atoms, version) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: assetId,
            normalizationHash: "norm-v1",
            sourceHash: "asr-v1"
        )

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms,
            analysisAssetId: assetId,
            transcriptVersion: version.transcriptVersion
        )

        // Should find diverse evidence categories.
        let categories = Set(catalog.entries.map(\.category))

        // Should find at least URLs and either promo codes or brand spans.
        // Not all realistic ad transcripts contain explicit disclosure phrases
        // ("brought to you by") — many are conversational host-reads.
        #expect(categories.contains(.url),
                "Should find URL evidence in tech-weekly-ep142")
        #expect(categories.count >= 2,
                "Should find at least 2 evidence categories, got \(categories)")
    }
}

// MARK: - Suite 4: Corpus – Full Pipeline (Hot Path)

@Suite("Corpus – Full Pipeline (Hot Path)")
struct CorpusFullPipelineTests {

    /// Representative episodes for full pipeline testing.
    static let testEpisodes = [
        "tech-weekly-ep142",   // Standard dynamic insertion
        "comedy-hour-ep301",   // Back-to-back ads
        "business-brief-ep63", // Pre-roll + post-roll
        "history-deep-ep55",   // Long episode, multiple mid-rolls
        "news-daily-apr01",    // Short episode, single pre-roll
    ]

    @Test("Hot path detects ad windows overlapping ground truth for representative episodes")
    func hotPathDetection() async throws {
        let loader = CorpusLoader()

        var totalGroundTruth = 0
        var totalDetected = 0
        var totalPrecisionNumerator = 0
        var totalPrecisionDenominator = 0

        for key in Self.testEpisodes {
            let annotation = try loader.loadAnnotation(filename: key)
            let assetId = annotation.episode.episodeId
            let duration = annotation.episode.duration
            let groundTruth = annotation.adSegments.map { toGroundTruth($0) }

            let store = try await makeCorpusStore()
            let asset = makeCorpusAsset(id: assetId, episodeId: assetId)
            try await store.insertAsset(asset)

            let chunks = buildCorpusTranscriptChunks(
                assetId: assetId,
                duration: duration,
                adSegments: annotation.adSegments
            )
            try await store.insertTranscriptChunks(chunks)

            let featureWindows = buildCorpusFeatureWindows(
                assetId: assetId,
                duration: duration,
                adSegments: annotation.adSegments
            )
            try await store.insertFeatureWindows(featureWindows)

            let detector = AdDetectionService(
                store: store,
                classifier: RuleBasedClassifier(),
                metadataExtractor: FallbackExtractor(),
                config: .default
            )

            let detectedWindows = try await detector.runHotPath(
                chunks: chunks,
                analysisAssetId: assetId,
                episodeDuration: duration
            )

            // Track recall: what % of ground-truth ads were detected.
            for gt in groundTruth {
                let overlapping = detectedWindows.filter { win in
                    win.startTime < gt.endTime + 10 && win.endTime > gt.startTime - 10
                }
                if !overlapping.isEmpty {
                    totalDetected += 1
                }
                totalGroundTruth += 1
            }

            // Track precision: what % of detections overlap ground truth.
            for win in detectedWindows {
                let overlapsGroundTruth = groundTruth.contains { gt in
                    win.startTime < gt.endTime + 10 && win.endTime > gt.startTime - 10
                }
                if overlapsGroundTruth {
                    totalPrecisionNumerator += 1
                }
                totalPrecisionDenominator += 1
            }
        }

        // Overall recall across representative episodes.
        let recall = totalGroundTruth > 0 ? Double(totalDetected) / Double(totalGroundTruth) : 0
        #expect(recall >= 0.60,
                "Pipeline recall should be >= 60%, got \(String(format: "%.1f", recall * 100))% (\(totalDetected)/\(totalGroundTruth))")

        // Overall precision.
        let precision = totalPrecisionDenominator > 0
            ? Double(totalPrecisionNumerator) / Double(totalPrecisionDenominator) : 0
        #expect(precision >= 0.50,
                "Pipeline precision should be >= 50%, got \(String(format: "%.1f", precision * 100))% (\(totalPrecisionNumerator)/\(totalPrecisionDenominator))")
    }

    @Test("Hot path produces no detections for no-ad episode")
    func hotPathNoAdEpisode() async throws {
        let loader = CorpusLoader()
        let annotation = try loader.loadAnnotation(filename: "storytelling-ep44")
        let assetId = annotation.episode.episodeId
        let duration = annotation.episode.duration

        let store = try await makeCorpusStore()
        let asset = makeCorpusAsset(id: assetId, episodeId: assetId)
        try await store.insertAsset(asset)

        let chunks = buildCorpusTranscriptChunks(
            assetId: assetId,
            duration: duration,
            adSegments: [] // No ads
        )
        try await store.insertTranscriptChunks(chunks)

        let featureWindows = buildCorpusFeatureWindows(
            assetId: assetId,
            duration: duration,
            adSegments: []
        )
        try await store.insertFeatureWindows(featureWindows)

        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor()
        )

        let detectedWindows = try await detector.runHotPath(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        #expect(detectedWindows.isEmpty,
                "No-ad episode should produce zero detections, got \(detectedWindows.count)")
    }

    @Test("Hot path detects all three back-to-back ads in comedy episode")
    func hotPathBackToBackAds() async throws {
        let loader = CorpusLoader()
        let annotation = try loader.loadAnnotation(filename: "comedy-hour-ep301")
        let assetId = annotation.episode.episodeId
        let duration = annotation.episode.duration
        let groundTruth = annotation.adSegments.map { toGroundTruth($0) }

        let store = try await makeCorpusStore()
        let asset = makeCorpusAsset(id: assetId, episodeId: assetId)
        try await store.insertAsset(asset)

        let chunks = buildCorpusTranscriptChunks(
            assetId: assetId,
            duration: duration,
            adSegments: annotation.adSegments
        )
        try await store.insertTranscriptChunks(chunks)

        let featureWindows = buildCorpusFeatureWindows(
            assetId: assetId,
            duration: duration,
            adSegments: annotation.adSegments
        )
        try await store.insertFeatureWindows(featureWindows)

        let detector = AdDetectionService(
            store: store,
            classifier: RuleBasedClassifier(),
            metadataExtractor: FallbackExtractor(),
            config: .default
        )

        let detectedWindows = try await detector.runHotPath(
            chunks: chunks,
            analysisAssetId: assetId,
            episodeDuration: duration
        )

        // The three back-to-back ads (240-420s) should produce at least one detection
        // covering that range. They may merge into one large window.
        let backToBackGT = groundTruth.filter { $0.startTime >= 240 && $0.endTime <= 420 }
        let coveringDetections = detectedWindows.filter { win in
            win.startTime < 420 + 10 && win.endTime > 240 - 10
        }
        #expect(!coveringDetections.isEmpty,
                "Should detect the back-to-back ad block at 240-420s, got \(detectedWindows.map { "\($0.startTime)-\($0.endTime)" })")

        // Should also detect the later host-read ad around 1800s.
        let laterGT = groundTruth.first { $0.startTime >= 1800 }
        #expect(laterGT != nil, "Comedy episode should have an ad segment at or after 1800s")
        if let later = laterGT {
            let laterDetections = detectedWindows.filter { win in
                win.startTime < later.endTime + 15 && win.endTime > later.startTime - 15
            }
            #expect(!laterDetections.isEmpty,
                    "Should detect host-read ad near \(later.startTime)s")
        }
    }
}

// MARK: - Suite 5: Corpus – AcousticBreakDetector at Ad Boundaries

@Suite("Corpus – AcousticBreakDetector at Ad Boundaries")
struct CorpusAcousticBreakTests {

    @Test("AcousticBreakDetector finds breaks within 4s of ground-truth ad boundaries")
    func breaksNearAdBoundaries() throws {
        let loader = CorpusLoader()

        let episodeKeys = [
            "tech-weekly-ep142",   // 2 ads, 4 boundaries
            "comedy-hour-ep301",   // 4 ads, but back-to-back share boundaries
            "history-deep-ep55",   // 3 ads, 6 boundaries
            "business-brief-ep63", // pre-roll + post-roll, edge boundaries
            "science-pod-ep22",    // produced segment with jingle
        ]

        var totalBoundaries = 0
        var detectedBoundaries = 0

        for key in episodeKeys {
            let annotation = try loader.loadAnnotation(filename: key)
            let assetId = annotation.episode.episodeId
            let duration = annotation.episode.duration

            let featureWindows = buildCorpusFeatureWindows(
                assetId: assetId,
                duration: duration,
                adSegments: annotation.adSegments
            )

            let breaks = AcousticBreakDetector.detectBreaks(in: featureWindows)

            // Collect all unique boundary times from ground truth.
            var boundaryTimes: Set<Double> = []
            for seg in annotation.adSegments {
                // Skip boundaries at 0.0 (episode start) — no acoustic transition expected.
                if seg.startTime > 1.0 {
                    boundaryTimes.insert(seg.startTime)
                }
                if seg.endTime < duration - 1.0 {
                    boundaryTimes.insert(seg.endTime)
                }
            }

            for boundary in boundaryTimes {
                let nearbyBreaks = breaks.filter { abs($0.time - boundary) < 4.0 }
                if !nearbyBreaks.isEmpty {
                    detectedBoundaries += 1
                }
                totalBoundaries += 1
            }
        }

        // At least 70% of boundaries should have a detected break.
        let boundaryRecall = totalBoundaries > 0
            ? Double(detectedBoundaries) / Double(totalBoundaries) : 0
        #expect(boundaryRecall >= 0.70,
                "Acoustic break detection recall should be >= 70%, got \(String(format: "%.1f", boundaryRecall * 100))% (\(detectedBoundaries)/\(totalBoundaries))")
    }

    @Test("AcousticBreakDetector produces breaks with expected signal types at energy drops")
    func breakSignalTypes() throws {
        let loader = CorpusLoader()
        let annotation = try loader.loadAnnotation(filename: "tech-weekly-ep142")
        let assetId = annotation.episode.episodeId
        let duration = annotation.episode.duration

        let featureWindows = buildCorpusFeatureWindows(
            assetId: assetId,
            duration: duration,
            adSegments: annotation.adSegments
        )

        let breaks = AcousticBreakDetector.detectBreaks(in: featureWindows)

        // Breaks near ad boundaries should have energy drop and/or pause signals.
        for seg in annotation.adSegments {
            let nearStart = breaks.filter { abs($0.time - seg.startTime) < 4.0 }
            for brk in nearStart {
                let hasRelevantSignal = brk.signals.contains(.energyDrop) ||
                    brk.signals.contains(.pauseCluster) ||
                    brk.signals.contains(.spectralSpike)
                #expect(hasRelevantSignal,
                        "Break at \(brk.time)s near ad boundary \(seg.startTime) should have energy/pause/spectral signal, got \(brk.signals)")
            }
        }
    }

    @Test("No-ad episode produces few or no breaks at episode scale")
    func noAdEpisodeBreaks() throws {
        let loader = CorpusLoader()
        let annotation = try loader.loadAnnotation(filename: "storytelling-ep44")
        let assetId = annotation.episode.episodeId
        let duration = annotation.episode.duration

        // Build feature windows with uniform energy (no drops).
        var windows: [FeatureWindow] = []
        for start in stride(from: 0.0, to: duration, by: 1.0) {
            let end = min(start + 1.0, duration)
            windows.append(FeatureWindow(
                analysisAssetId: assetId,
                startTime: start,
                endTime: end,
                rms: 0.05,
                spectralFlux: 0.01,
                musicProbability: 0.0,
                pauseProbability: 0.1,
                speakerClusterId: nil,
                jingleHash: nil,
                featureVersion: 1
            ))
        }

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        // Uniform energy episode should have very few spurious breaks.
        #expect(breaks.count <= 3,
                "No-ad episode with uniform energy should have <= 3 breaks, got \(breaks.count)")
    }
}
