// Phase2VerificationTests.swift
// Performance and scale verification tests for Phase 2 ad detection components:
// AcousticBreakDetector performance at episode scale, EvidenceCatalogBuilder
// scale behavior, and false-positive resilience under realistic speech.

import Foundation
import Testing
@testable import Playhead

// MARK: - Deterministic LCG Random

/// Simple linear congruential generator for reproducible test data.
/// Parameters from Numerical Recipes (a=1664525, c=1013904223, m=2^32).
private struct LCG {
    private var state: UInt32

    init(seed: UInt32) {
        self.state = seed
    }

    /// Returns a value in [0, 1).
    mutating func next() -> Double {
        state = state &* 1664525 &+ 1013904223
        return Double(state) / Double(UInt32.max)
    }

    /// Returns a value in [lo, hi).
    mutating func next(in range: ClosedRange<Double>) -> Double {
        let t = next()
        return range.lowerBound + t * (range.upperBound - range.lowerBound)
    }
}

// MARK: - Helpers

private func makeFeatureWindow(
    assetId: String = "perf-asset",
    startTime: Double,
    endTime: Double? = nil,
    rms: Double = 0.5,
    spectralFlux: Double = 0.1,
    musicProbability: Double = 0.0,
    speakerChangeProxyScore: Double = 0.0,
    musicBedChangeScore: Double = 0.0,
    pauseProbability: Double = 0.0
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime ?? startTime + 2.0,
        rms: rms,
        spectralFlux: spectralFlux,
        musicProbability: musicProbability,
        speakerChangeProxyScore: speakerChangeProxyScore,
        musicBedChangeScore: musicBedChangeScore,
        pauseProbability: pauseProbability,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 1
    )
}

private func makeAtom(
    assetId: String = "scale-asset",
    version: String = "v1",
    ordinal: Int,
    startTime: Double = 0,
    endTime: Double = 5,
    text: String = "hello world"
) -> TranscriptAtom {
    TranscriptAtom(
        atomKey: TranscriptAtomKey(
            analysisAssetId: assetId,
            transcriptVersion: version,
            atomOrdinal: ordinal
        ),
        contentHash: "deadbeef",
        startTime: startTime,
        endTime: endTime,
        text: text,
        chunkIndex: ordinal
    )
}

// MARK: - AcousticBreakDetector Performance

@Suite("AcousticBreakDetector Performance")
struct AcousticBreakDetectorPerformanceTests {

    /// Wall-clock budget for processing 1800 windows. The production
    /// goal (and what the algorithm achieves on an unloaded simulator)
    /// is ~30–80ms — well under the historical 100ms ceiling. We
    /// assert the *median* of N=10 iterations meets a 100ms budget
    /// (the production target) and use a generous per-iteration hang
    /// ceiling to catch genuine algorithmic regressions (anything
    /// that pushes the inner loop into a higher complexity class
    /// will blow well past the ceiling on every iteration). Median-
    /// based measurement is robust to the parallel-test CPU pressure
    /// on this 16 GB dev-machine — multiple `xcodebuild` jobs share
    /// cores and a single-sample wall-clock test reliably misses a
    /// tight budget under contention. See CLAUDE.md "Parallelism
    /// Ceiling" and the playhead-ss38 note in
    /// `LanePreemptionCoordinatorTests.promotionLatencyUnder100ms`,
    /// which is the template for this median pattern.
    private static let fullEpisodeBudgetMs: Double = 100.0
    private static let fullEpisodeHangCeilingMs: Double = 5_000.0
    private static let fullEpisodeIterations = 10

    @Test("Processes 1800 windows (60-min episode) under 100ms (median of 10)")
    func fullEpisodePerformance() {
        var rng = LCG(seed: 42)

        // Ad boundary timestamps (seconds)
        let adBoundaries: [Double] = [600, 1200, 2400, 3000]
        let windowDuration = 2.0
        let windowCount = 1800 // 60 min at 2s windows = 3600s total

        var windows: [FeatureWindow] = []
        windows.reserveCapacity(windowCount)

        for i in 0..<windowCount {
            let startTime = Double(i) * windowDuration
            var rms = 0.5 + rng.next(in: -0.1...0.1)
            var spectralFlux = 0.1 + rng.next(in: -0.05...0.05)
            var pauseProb = 0.1

            // Occasionally bump pause probability in normal speech
            if rng.next() < 0.05 {
                pauseProb = 0.3
            }

            // Insert ad boundary transitions: energy drop + pause cluster
            for boundary in adBoundaries {
                let distance = abs(startTime - boundary)
                if distance < 4.0 {
                    // Within 2 windows of boundary: sharp energy drop + high pause
                    rms = 0.1 + rng.next(in: 0.0...0.05)
                    spectralFlux = 0.5 + rng.next(in: 0.0...0.3)
                    pauseProb = 0.8 + rng.next(in: 0.0...0.15)
                }
            }

            // Clamp values to valid ranges
            rms = max(0.0, min(1.0, rms))
            spectralFlux = max(0.0, spectralFlux)
            pauseProb = max(0.0, min(1.0, pauseProb))

            windows.append(makeFeatureWindow(
                startTime: startTime,
                rms: rms,
                spectralFlux: spectralFlux,
                pauseProbability: pauseProb
            ))
        }

        // Run the detector N times and capture wall-clock samples. We
        // measure the median (typical case) rather than a single max
        // because xcodebuild runs 3000+ tests in parallel and any one
        // sample can be stretched 100ms–1s+ by cooperative-pool
        // scheduling regardless of the algorithm's actual cost. The
        // hang ceiling on `maxMs` still catches a runaway implementation
        // (every iteration would balloon, not just the unlucky one).
        // See bead playhead-rbv4 / playhead-ss38.
        let clock = ContinuousClock()
        var samplesMs: [Double] = []
        samplesMs.reserveCapacity(Self.fullEpisodeIterations)
        var lastBreaks: [AcousticBreak] = []

        for _ in 0..<Self.fullEpisodeIterations {
            let start = clock.now
            let breaks = AcousticBreakDetector.detectBreaks(in: windows)
            let elapsed = clock.now - start
            let elapsedMs = Double(elapsed.components.attoseconds) / 1e15 +
                             Double(elapsed.components.seconds) * 1000.0
            samplesMs.append(elapsedMs)
            lastBreaks = breaks
        }

        let sorted = samplesMs.sorted()
        let medianMs = sorted[sorted.count / 2]
        let maxMs = sorted.last!

        #expect(medianMs < Self.fullEpisodeBudgetMs,
                "Median (over \(Self.fullEpisodeIterations) iterations) should process 1800 windows in under \(Self.fullEpisodeBudgetMs)ms, was \(medianMs)ms (max=\(maxMs)ms, samples=\(samplesMs))")
        #expect(maxMs < Self.fullEpisodeHangCeilingMs,
                "Max iteration should not exceed hang ceiling of \(Self.fullEpisodeHangCeilingMs)ms, was \(maxMs)ms (median=\(medianMs)ms, samples=\(samplesMs))")

        // Sanity: should detect breaks near the 4 ad boundaries
        let detectedNearBoundary = adBoundaries.filter { boundary in
            lastBreaks.contains { abs($0.time - boundary) < 6.0 }
        }
        #expect(detectedNearBoundary.count >= 3,
                "Should detect breaks near most of the 4 ad boundaries, found \(detectedNearBoundary.count)")
    }
}

// MARK: - EvidenceCatalogBuilder Scale

@Suite("EvidenceCatalogBuilder Scale")
struct EvidenceCatalogBuilderScaleTests {

    /// Non-commercial filler text templates.
    private static let fillerTexts = [
        "and then we started talking about the weather patterns in the northeast",
        "i think that the most interesting part of the conversation was about history",
        "so basically what happened was they decided to change the entire approach",
        "you know when you think about it the data really supports a different conclusion",
        "let me tell you about what happened last week at the conference",
        "the research shows that this particular method is more effective overall",
        "we had a great discussion about the future of technology and innovation",
        "honestly i was surprised by how much progress they made in just one year",
        "the thing about this topic is that there are so many different perspectives",
        "what really stood out to me was the way they handled the entire situation",
    ]

    /// Sponsor ad-read text for injected ad atoms.
    private static let adReadTexts = [
        "this episode is brought to you by betterhelp visit betterhelp dot com slash podcast",
        "use code save20 at checkout for twenty percent off your first order",
        "betterhelp makes therapy accessible and affordable sign up today",
        "head to betterhelp dot com slash podcast to get started with a free trial",
        "brought to you by our friends at athletic greens go to athleticgreens dot com",
        "use code podcast at checkout for a special discount on your first purchase",
    ]

    @Test("200-atom episode produces catalog entries only near ad reads")
    func scaleWithRealisticEpisode() {
        let atomCount = 200
        let atomDuration = 18.0 // ~60 min / 200 atoms

        // Ad-read ranges (atom ordinals): 30-35, 100-105, 160-165
        let adRanges: [ClosedRange<Int>] = [30...35, 100...105, 160...165]

        var atoms: [TranscriptAtom] = []
        atoms.reserveCapacity(atomCount)

        for i in 0..<atomCount {
            let startTime = Double(i) * atomDuration
            let endTime = startTime + atomDuration

            let isAdAtom = adRanges.contains { $0.contains(i) }
            let text: String
            if isAdAtom {
                let adIndex = (i % Self.adReadTexts.count)
                text = Self.adReadTexts[adIndex]
            } else {
                let fillerIndex = (i % Self.fillerTexts.count)
                text = Self.fillerTexts[fillerIndex]
            }

            atoms.append(makeAtom(
                ordinal: i,
                startTime: startTime,
                endTime: endTime,
                text: text
            ))
        }

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "scale-asset", transcriptVersion: "v1"
        )

        // 1. Catalog should have entries (the ad reads contain URLs, promo codes, disclosures)
        #expect(!catalog.entries.isEmpty,
                "Catalog should extract evidence from the 3 ad-read sections")

        // 2. No entries from atoms far from any ad context.
        //    "Far" = ordinal outside any ad range and not within ±3 atoms of one.
        let adProximity: Set<Int> = {
            var s = Set<Int>()
            for r in adRanges {
                for o in max(0, r.lowerBound - 3)...min(atomCount - 1, r.upperBound + 3) {
                    s.insert(o)
                }
            }
            return s
        }()

        let farEntries = catalog.entries.filter { !adProximity.contains($0.atomOrdinal) }
        #expect(farEntries.isEmpty,
                "No evidence entries should come from atoms far from ad reads, found \(farEntries.count)")

        // 3. Total entry count is reasonable — not hundreds.
        //    3 ad reads x ~6 atoms each, each atom might produce a few entries.
        #expect(catalog.entries.count < 80,
                "Total entries should be reasonable, got \(catalog.entries.count)")
    }
}

// MARK: - AcousticBreakDetector Realistic Speech (No False Positives)

@Suite("AcousticBreakDetector Realistic Speech")
struct AcousticBreakDetectorRealisticSpeechTests {

    @Test("No false breaks from natural speech with occasional single-window pauses")
    func noFalsePositivesFromNaturalSpeech() {
        var rng = LCG(seed: 7)
        let windowCount = 300 // 10 minutes of 2s windows

        var windows: [FeatureWindow] = []
        windows.reserveCapacity(windowCount)
        var previousRMS = 0.50 // starting point for random walk

        for i in 0..<windowCount {
            let startTime = Double(i) * 2.0

            // Natural speech: RMS drifts gradually (correlated random walk).
            // Each window's RMS is the previous ±0.05, clamped to 0.30-0.70.
            // This models real speech where volume changes are gradual, not
            // random jumps from quiet to loud between adjacent 2s windows.
            let drift = rng.next(in: -0.05...0.05)
            let rms = max(0.30, min(0.70, previousRMS + drift))

            // Low spectral flux with mild variation
            let spectralFlux = 0.08 + rng.next(in: -0.03...0.03)

            // Occasional single-window pauses (natural breathing / sentence gaps)
            var pauseProb = 0.05 + rng.next(in: 0.0...0.1)
            if rng.next() < 0.08 { // ~8% chance of a single-window pause
                pauseProb = 0.5
            }

            previousRMS = rms

            windows.append(makeFeatureWindow(
                startTime: startTime,
                rms: rms,
                spectralFlux: max(0, spectralFlux),
                pauseProbability: min(1.0, pauseProb)
            ))
        }

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        // Natural speech variation should NOT produce any breaks.
        // The detector requires strong energy drops (>35%), sustained pause clusters,
        // or spectral spikes above the 80th percentile — none of which should occur
        // in this gentle variation.
        #expect(breaks.isEmpty,
                "Natural speech variation should produce zero breaks, got \(breaks.count)")
    }
}

// MARK: - Realistic Transcript Verification

/// Tests using real podcast ad transcripts (sourced from actual shows) and
/// real non-ad content to verify detection against ground truth.
@Suite("EvidenceCatalogBuilder Realistic Transcripts")
struct EvidenceCatalogBuilderRealisticTests {

    // MARK: - Real ad transcripts (ASR-style lowercase, minimal punctuation)

    /// BetterHelp host-read ad from The Antidote / Dr. Deloney
    private static let betterHelpAd = """
    if youre thinking of starting therapy give betterhelp a try its entirely online \
    designed to be convenient flexible and suited to your schedule visit betterhelp \
    dot com slash deloney today to get ten percent off your first month
    """

    /// Squarespace host-read ad from Court Junkie
    private static let squarespaceAd = """
    squarespace is an all in one platform that can help you build a beautiful online \
    presence and run your business from websites and online stores to marketing tools \
    and analytics head on over to squarespace dot com slash court for a free trial \
    and when youre ready to launch use offer code court to save ten percent off your \
    first purchase of a website or domain
    """

    /// Athletic Greens host-read ad from Flagrant
    private static let athleticGreensAd = """
    athletic greens is going to give you a free one year supply of immune supporting \
    vitamin d and five free travel packs with your first purchase all you have to do \
    is visit athleticgreens dot com slash flagrant to take ownership over your health
    """

    /// NordVPN host-read ad
    private static let nordVPNAd = """
    thats why i use nordvpn it encrypts your internet connection and hides your ip \
    address so no one can track what youre doing online go to nordvpn dot com slash \
    podcast or use code podcast to get a huge discount on a two year plan plus four \
    months free
    """

    /// HelloFresh mid-roll ad
    private static let helloFreshAd = """
    hellofresh takes the stress out of mealtime head to hellofresh dot com and fool \
    around with easy foolproof recipes that will have you both craving more use code \
    awesome for thirty five dollars off your first week of deliveries
    """

    // MARK: - Real non-ad content

    /// True crime narration
    private static let trueCrimeNarration = """
    a bridge tender making his morning rounds discovered something horrifying in the \
    waters below he observed what appeared to be a body near the shoreline and called \
    police immediately the victim was identified as fourteen year old shelle bojio who \
    had been stabbed over thirty times strangled and drowned former prosecutor robert \
    hayman described it as the most brutal one that i had seen and handled personally
    """

    /// Science discussion from Radiolab
    private static let scienceDiscussion = """
    charles fernyhough explains a fascinating discovery about how language allows us \
    to connect disparate ideas young children and rats both struggle to link spatial \
    information with color properties until around age six when linguistic development \
    enables this connection as fernyhough notes these different kinds of knowledge \
    cant talk to each other without language
    """

    /// Interview / design philosophy from Wiser Than Me
    private static let interviewContent = """
    i think a woman never looks more beautiful or more confident than when shes \
    comfortable and so i think comfort both physically and also emotionally and \
    artistically and creatively i thats when a woman feels true to herself ive been \
    a person that was organic things came my way and not came my way that way i have \
    learned to be more not even strategic but to think in steps
    """

    // MARK: - Tests

    @Test("Detects evidence in real BetterHelp ad transcript")
    func betterHelpAdDetection() {
        let atoms = splitIntoAtoms(Self.betterHelpAd, startOrdinal: 0)

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "real-ad", transcriptVersion: "v1"
        )

        // Should find: URL (betterhelp dot com), CTA (near disclosure context)
        let urls = catalog.entries(for: .url)
        #expect(urls.contains { $0.normalizedText.contains("betterhelp") },
                "Should detect BetterHelp URL")

        #expect(!catalog.entries.isEmpty, "Real ad should produce evidence entries")
    }

    @Test("Detects evidence in real Squarespace ad transcript")
    func squarespaceAdDetection() {
        let atoms = splitIntoAtoms(Self.squarespaceAd, startOrdinal: 0)

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "real-ad", transcriptVersion: "v1"
        )

        let urls = catalog.entries(for: .url)
        #expect(urls.contains { $0.normalizedText.contains("squarespace") },
                "Should detect Squarespace URL")

        let codes = catalog.entries(for: .promoCode)
        #expect(codes.contains { $0.normalizedText.contains("court") },
                "Should detect promo code 'court'")
    }

    @Test("Detects evidence in real NordVPN ad transcript")
    func nordVPNAdDetection() {
        let atoms = splitIntoAtoms(Self.nordVPNAd, startOrdinal: 0)

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "real-ad", transcriptVersion: "v1"
        )

        let urls = catalog.entries(for: .url)
        #expect(urls.contains { $0.normalizedText.contains("nordvpn") },
                "Should detect NordVPN URL")

        let codes = catalog.entries(for: .promoCode)
        #expect(codes.contains { $0.normalizedText.contains("podcast") },
                "Should detect promo code 'podcast'")
    }

    @Test("Detects evidence across multiple real ad styles")
    func multipleRealAds() {
        // Simulate a real episode: non-ad -> ad -> non-ad -> ad -> non-ad
        var atoms: [TranscriptAtom] = []
        var ordinal = 0

        // Non-ad intro (ordinals 0-4)
        for a in splitIntoAtoms(Self.scienceDiscussion, startOrdinal: ordinal) {
            atoms.append(a)
            ordinal = a.atomKey.atomOrdinal + 1
        }

        // First ad: Athletic Greens (ordinals 5-9ish)
        for a in splitIntoAtoms(Self.athleticGreensAd, startOrdinal: ordinal) {
            atoms.append(a)
            ordinal = a.atomKey.atomOrdinal + 1
        }

        // Non-ad middle (ordinals ~10-14)
        for a in splitIntoAtoms(Self.interviewContent, startOrdinal: ordinal) {
            atoms.append(a)
            ordinal = a.atomKey.atomOrdinal + 1
        }

        // Second ad: HelloFresh (ordinals ~15-19)
        for a in splitIntoAtoms(Self.helloFreshAd, startOrdinal: ordinal) {
            atoms.append(a)
            ordinal = a.atomKey.atomOrdinal + 1
        }

        // Non-ad outro (ordinals ~20-24)
        for a in splitIntoAtoms(Self.trueCrimeNarration, startOrdinal: ordinal) {
            atoms.append(a)
            ordinal = a.atomKey.atomOrdinal + 1
        }

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "real-episode", transcriptVersion: "v1"
        )

        // Should find evidence from both ads
        let urls = catalog.entries(for: .url)
        #expect(urls.contains { $0.normalizedText.contains("athleticgreens") ||
                                 $0.normalizedText.contains("athletic") },
                "Should detect Athletic Greens URL")
        #expect(urls.contains { $0.normalizedText.contains("hellofresh") },
                "Should detect HelloFresh URL")

        // Print catalog for manual inspection
        let rendered = catalog.renderForPrompt()
        #expect(!rendered.isEmpty)
    }

    @Test("Zero false positives on real non-ad content")
    func noFalsePositivesOnRealContent() {
        // Full episode of non-ad content only
        var atoms: [TranscriptAtom] = []
        var ordinal = 0

        for text in [Self.trueCrimeNarration, Self.scienceDiscussion, Self.interviewContent] {
            for a in splitIntoAtoms(text, startOrdinal: ordinal) {
                atoms.append(a)
                ordinal = a.atomKey.atomOrdinal + 1
            }
        }

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "non-ad", transcriptVersion: "v1"
        )

        // Non-ad content should produce zero or near-zero evidence entries.
        // Some URL-like patterns might appear in rare cases, but disclosure/promo should be empty.
        let disclosures = catalog.entries(for: .disclosurePhrase)
        let codes = catalog.entries(for: .promoCode)
        let ctas = catalog.entries(for: .ctaPhrase)
        let brands = catalog.entries(for: .brandSpan)

        #expect(disclosures.isEmpty, "Non-ad content should have no disclosure phrases")
        #expect(codes.isEmpty, "Non-ad content should have no promo codes")
        #expect(ctas.isEmpty, "Non-ad content should have no CTAs (gated behind commercial context)")
        #expect(brands.isEmpty, "Non-ad content should have no brand spans")

        // Total entries should be very low (maybe a few URL-like false positives at worst)
        #expect(catalog.entries.count <= 3,
                "Non-ad content should have at most a few entries, got \(catalog.entries.count)")
    }

    // MARK: - Helpers

    /// Split a long text into TranscriptAtoms of ~20 words each, simulating ASR chunking.
    private func splitIntoAtoms(_ text: String, startOrdinal: Int) -> [TranscriptAtom] {
        let words = text.split(separator: " ")
        let wordsPerAtom = 20
        var atoms: [TranscriptAtom] = []

        let chunkCount = max(1, (words.count + wordsPerAtom - 1) / wordsPerAtom)
        for i in 0..<chunkCount {
            let sliceStart = i * wordsPerAtom
            let sliceEnd = min(sliceStart + wordsPerAtom, words.count)
            let chunkText = words[sliceStart..<sliceEnd].joined(separator: " ")
            let ordinal = startOrdinal + i
            let startTime = Double(ordinal) * 10.0
            let endTime = startTime + 10.0

            atoms.append(makeAtom(
                ordinal: ordinal,
                startTime: startTime,
                endTime: endTime,
                text: chunkText
            ))
        }

        return atoms
    }
}
