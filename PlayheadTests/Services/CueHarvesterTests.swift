// CueHarvesterTests.swift
// Unit tests for Phase 2: AcousticBreakDetector, EvidenceCatalogBuilder,
// SponsorKnowledgeMatcher stub, AdCopyFingerprintMatcher stub.

import Foundation
import Testing
@testable import Playhead

// MARK: - Helpers

private func makeFeatureWindow(
    assetId: String = "asset-1",
    startTime: Double,
    endTime: Double? = nil,
    rms: Double = 0.5,
    spectralFlux: Double = 0.1,
    pauseProbability: Double = 0.0
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: assetId,
        startTime: startTime,
        endTime: endTime ?? startTime + 2.0,
        rms: rms,
        spectralFlux: spectralFlux,
        musicProbability: 0.0,
        pauseProbability: pauseProbability,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 1
    )
}

private func makeAtom(
    assetId: String = "asset-1",
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

// MARK: - AcousticBreakDetector Tests

@Suite("AcousticBreakDetector")
struct AcousticBreakDetectorTests {

    @Test("Detects energy drop between loud and quiet windows")
    func energyDrop() {
        // Loud speech -> sudden quiet (ad boundary pattern)
        let windows = [
            makeFeatureWindow(startTime: 0, rms: 0.6, spectralFlux: 0.1),
            makeFeatureWindow(startTime: 2, rms: 0.6, spectralFlux: 0.1),
            makeFeatureWindow(startTime: 4, rms: 0.6, spectralFlux: 0.1),
            // Drop: 0.6 -> 0.15 = 75% drop (well above 35% threshold)
            makeFeatureWindow(startTime: 6, rms: 0.15, spectralFlux: 0.1),
            makeFeatureWindow(startTime: 8, rms: 0.15, spectralFlux: 0.1),
        ]

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        // Exactly one transition point: windows[2]->windows[3] (0.6->0.15)
        // May also detect the rising edge at windows[3]->windows[4] if those are different
        let nearDrop = breaks.filter { $0.time >= 4.0 && $0.time <= 8.0 && $0.signals.contains(.energyDrop) }
        #expect(nearDrop.count == 1, "Should detect exactly one energy drop at the loud->quiet transition")
        #expect(nearDrop[0].signals.contains(.energyDrop),
                "Loud-to-quiet transition should be tagged as energyDrop")
    }

    @Test("No false positives from two near-silent windows")
    func silentWindowsNoFalsePositive() {
        // Both windows near silence — not a meaningful energy drop
        let windows = [
            makeFeatureWindow(startTime: 0, rms: 0.02),
            makeFeatureWindow(startTime: 2, rms: 0.01),
        ]

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        let energyDrops = breaks.filter { $0.signals.contains(.energyDrop) }
        #expect(energyDrops.isEmpty)
    }

    @Test("Detects spectral flux spike")
    func spectralSpike() {
        // Most windows have low spectral flux; one has a spike
        var windows: [FeatureWindow] = (0..<10).map { i in
            makeFeatureWindow(startTime: Double(i) * 2, spectralFlux: 0.1)
        }
        // Spike at window 5 (80th percentile of 10 values = top 2)
        windows[5] = makeFeatureWindow(startTime: 10, spectralFlux: 1.5)
        windows[9] = makeFeatureWindow(startTime: 18, spectralFlux: 1.5)

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        let spectralBreaks = breaks.filter { $0.signals.contains(.spectralSpike) }
        #expect(!spectralBreaks.isEmpty)
    }

    @Test("Detects pause cluster")
    func pauseCluster() {
        let windows = [
            makeFeatureWindow(startTime: 0, rms: 0.5, pauseProbability: 0.1),
            makeFeatureWindow(startTime: 2, rms: 0.5, pauseProbability: 0.1),
            // Pause cluster: 3 consecutive high-pause windows
            makeFeatureWindow(startTime: 4, rms: 0.1, pauseProbability: 0.8),
            makeFeatureWindow(startTime: 6, rms: 0.1, pauseProbability: 0.9),
            makeFeatureWindow(startTime: 8, rms: 0.1, pauseProbability: 0.7),
            makeFeatureWindow(startTime: 10, rms: 0.5, pauseProbability: 0.1),
        ]

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        let pauseBreaks = breaks.filter { $0.signals.contains(.pauseCluster) }
        #expect(!pauseBreaks.isEmpty)
        // Pause cluster should be detected around t=4 (start of cluster)
        let nearCluster = pauseBreaks.filter { $0.time >= 3.0 && $0.time <= 9.0 }
        #expect(!nearCluster.isEmpty)
    }

    @Test("Multi-signal breaks score higher than single-signal end-to-end")
    func multiSignalScoring() {
        // Create two distinct transitions:
        // Transition A at t=10: energy drop only (single signal)
        // Transition B at t=30: energy drop + pause cluster (multi signal)
        var windows: [FeatureWindow] = (0..<30).map { i in
            makeFeatureWindow(startTime: Double(i) * 2, rms: 0.5, spectralFlux: 0.1, pauseProbability: 0.1)
        }
        // Transition A: energy drop only at window 5 (t=10)
        windows[4] = makeFeatureWindow(startTime: 8, rms: 0.7, spectralFlux: 0.1, pauseProbability: 0.1)
        windows[5] = makeFeatureWindow(startTime: 10, rms: 0.1, spectralFlux: 0.1, pauseProbability: 0.1)

        // Transition B: energy drop + pause cluster at window 15 (t=30)
        windows[14] = makeFeatureWindow(startTime: 28, rms: 0.7, spectralFlux: 0.1, pauseProbability: 0.1)
        windows[15] = makeFeatureWindow(startTime: 30, rms: 0.1, spectralFlux: 0.1, pauseProbability: 0.9)
        windows[16] = makeFeatureWindow(startTime: 32, rms: 0.1, spectralFlux: 0.1, pauseProbability: 0.9)

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        let nearA = breaks.filter { $0.time >= 8 && $0.time <= 12 }
        let nearB = breaks.filter { $0.time >= 28 && $0.time <= 34 }

        #expect(!nearA.isEmpty, "Should detect transition A")
        #expect(!nearB.isEmpty, "Should detect transition B")

        if let a = nearA.first, let b = nearB.max(by: { $0.breakStrength < $1.breakStrength }) {
            #expect(b.breakStrength > a.breakStrength,
                    "Multi-signal break B should score higher than single-signal break A")
        }
    }

    @Test("Empty input returns no breaks")
    func emptyInput() {
        let breaks = AcousticBreakDetector.detectBreaks(in: [])
        #expect(breaks.isEmpty)
    }

    @Test("Single window returns no breaks")
    func singleWindow() {
        let windows = [makeFeatureWindow(startTime: 0)]
        let breaks = AcousticBreakDetector.detectBreaks(in: windows)
        #expect(breaks.isEmpty)
    }

    @Test("Breaks are sorted by time")
    func breaksSortedByTime() {
        // Create windows with multiple transition points
        let windows = [
            makeFeatureWindow(startTime: 0, rms: 0.7),
            makeFeatureWindow(startTime: 2, rms: 0.1),  // drop at t=2
            makeFeatureWindow(startTime: 4, rms: 0.7),
            makeFeatureWindow(startTime: 6, rms: 0.7),
            makeFeatureWindow(startTime: 8, rms: 0.1),  // drop at t=8
            makeFeatureWindow(startTime: 10, rms: 0.7),
        ]

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        for i in 1..<breaks.count {
            #expect(breaks[i].time >= breaks[i - 1].time)
        }
    }

    @Test("Break strength is bounded 0 to 1")
    func strengthBounded() {
        // Create windows with all three signal types overlapping
        var windows: [FeatureWindow] = (0..<20).map { i in
            makeFeatureWindow(startTime: Double(i) * 2, rms: 0.5, spectralFlux: 0.1, pauseProbability: 0.1)
        }
        // Create a mega-transition: energy drop + spectral spike + pause cluster at t=20
        windows[10] = makeFeatureWindow(startTime: 20, rms: 0.05, spectralFlux: 5.0, pauseProbability: 0.9)
        windows[11] = makeFeatureWindow(startTime: 22, rms: 0.05, spectralFlux: 0.1, pauseProbability: 0.9)

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        for b in breaks {
            #expect(b.breakStrength >= 0.0)
            #expect(b.breakStrength <= 1.0)
        }
    }

    @Test("Detects rising edge (silence to speech)")
    func risingEdge() {
        let windows = [
            makeFeatureWindow(startTime: 0, rms: 0.1, spectralFlux: 0.1),
            makeFeatureWindow(startTime: 2, rms: 0.1, spectralFlux: 0.1),
            // Rise: 0.1 -> 0.7 = 85% rise (well above 35% threshold)
            makeFeatureWindow(startTime: 4, rms: 0.7, spectralFlux: 0.1),
            makeFeatureWindow(startTime: 6, rms: 0.7, spectralFlux: 0.1),
        ]

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        let nearRise = breaks.filter { $0.time >= 2.0 && $0.time <= 6.0 }
        #expect(!nearRise.isEmpty, "Should detect rising edge")
        #expect(nearRise.contains { $0.signals.contains(.energyRise) },
                "Rising edge should be tagged as energyRise, not energyDrop")
    }

    @Test("Multi-signal merge produces single break with all signals")
    func multiSignalMerge() {
        // Create windows where energy drop, spectral spike, and pause cluster
        // all occur at the same boundary (around t=10)
        var windows: [FeatureWindow] = (0..<20).map { i in
            makeFeatureWindow(startTime: Double(i) * 2, rms: 0.5, spectralFlux: 0.1, pauseProbability: 0.1)
        }
        // Energy drop + spectral spike + pause at window 5 (t=10)
        windows[4] = makeFeatureWindow(startTime: 8, rms: 0.7, spectralFlux: 0.1, pauseProbability: 0.1)
        windows[5] = makeFeatureWindow(startTime: 10, rms: 0.1, spectralFlux: 5.0, pauseProbability: 0.9)
        windows[6] = makeFeatureWindow(startTime: 12, rms: 0.1, spectralFlux: 0.1, pauseProbability: 0.9)

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        // Find break(s) near the transition
        let nearTransition = breaks.filter { $0.time >= 8.0 && $0.time <= 14.0 }
        #expect(!nearTransition.isEmpty)

        // At least one break should have multiple signal types
        let multiSignal = nearTransition.filter { $0.signals.count >= 2 }
        #expect(!multiSignal.isEmpty, "Co-located signals should merge into multi-signal break")

        // Multi-signal breaks should score higher than single-signal ones
        if let multi = multiSignal.first {
            let singleSignalBreaks = breaks.filter { $0.signals.count == 1 }
            if let single = singleSignalBreaks.first {
                #expect(multi.breakStrength > single.breakStrength,
                        "Multi-signal break should score higher")
            }
        }
    }

    @Test("Handles uniform windows without false breaks")
    func uniformWindows() {
        // All windows identical — no transitions, no breaks expected
        // (except possibly spectral spikes if percentile is 0, but spectral is uniform too)
        let windows = (0..<50).map { i in
            makeFeatureWindow(startTime: Double(i) * 2, rms: 0.5, spectralFlux: 0.1, pauseProbability: 0.1)
        }

        let breaks = AcousticBreakDetector.detectBreaks(in: windows)

        // No energy drops (all same RMS), no pause clusters (all low),
        // spectral: all same value so percentile threshold = that value, nothing exceeds it
        #expect(breaks.isEmpty)
    }
}

// MARK: - EvidenceCatalogBuilder Tests

@Suite("EvidenceCatalogBuilder")
struct EvidenceCatalogBuilderTests {

    @Test("Extracts URLs from transcript")
    func extractsURLs() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "check out betterhelp.com for more info"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let urls = catalog.entries(for: .url)
        #expect(!urls.isEmpty)
        #expect(urls.contains { $0.normalizedText.contains("betterhelp") })
    }

    @Test("Extracts spoken URLs with 'dot com slash' pattern")
    func extractsSpokenURLs() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "go to betterhelp dot com slash podcast for a discount"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let urls = catalog.entries(for: .url)
        #expect(!urls.isEmpty)
    }

    @Test("Extracts promo codes")
    func extractsPromoCodes() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "use code podcast20 at checkout for twenty percent off"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let codes = catalog.entries(for: .promoCode)
        #expect(!codes.isEmpty)
        #expect(codes.contains { $0.matchedText.contains("podcast20") })
    }

    @Test("Extracts CTA phrases")
    func extractsCTAs() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "brought to you by acme corp"),
            makeAtom(ordinal: 1, startTime: 10, endTime: 20,
                     text: "sign up now and get your free trial today"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let ctas = catalog.entries(for: .ctaPhrase)
        #expect(!ctas.isEmpty, "CTA phrases should be extracted near commercial context")
    }

    @Test("Extracts disclosure phrases")
    func extractsDisclosures() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "this episode is brought to you by our friends at acme corp"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let disclosures = catalog.entries(for: .disclosurePhrase)
        #expect(!disclosures.isEmpty)
        #expect(disclosures.contains { $0.normalizedText.contains("brought to you by") })
    }

    @Test("Empty transcript produces empty catalog")
    func emptyTranscript() {
        let catalog = EvidenceCatalogBuilder.build(
            atoms: [], analysisAssetId: "a1", transcriptVersion: "v1"
        )

        #expect(catalog.entries.isEmpty)
    }

    @Test("No commercial evidence produces empty catalog")
    func noEvidence() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "today we are going to talk about the weather and gardening tips"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        // No commercial signals at all — entire catalog should be empty
        #expect(catalog.entries.isEmpty)
    }

    @Test("Evidence refs are stable and sequential")
    func stableRefs() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "brought to you by betterhelp use code save at betterhelp.com"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        // Refs should be 0, 1, 2, ...
        for (i, entry) in catalog.entries.enumerated() {
            #expect(entry.evidenceRef == i)
        }
    }

    @Test("Evidence refs are deterministic across repeated builds")
    func deterministic() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "sponsored by Acme. Visit acme.com slash offer. use code SAVE20."),
            makeAtom(ordinal: 1, startTime: 10, endTime: 20,
                     text: "sign up now for your free trial. link in the description."),
        ]

        let c1 = EvidenceCatalogBuilder.build(atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1")
        let c2 = EvidenceCatalogBuilder.build(atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1")

        #expect(c1.entries.count == c2.entries.count)
        for (a, b) in zip(c1.entries, c2.entries) {
            #expect(a.evidenceRef == b.evidenceRef)
            #expect(a.category == b.category)
            #expect(a.normalizedText == b.normalizedText)
            #expect(a.atomOrdinal == b.atomOrdinal)
        }
    }

    @Test("Deduplicates same evidence text within same category")
    func deduplicates() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "brought to you by Acme"),
            makeAtom(ordinal: 1, startTime: 10, endTime: 20,
                     text: "brought to you by Acme again"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let disclosures = catalog.entries(for: .disclosurePhrase)
        #expect(!disclosures.isEmpty, "Should find disclosure phrases")
        let broughtToYou = disclosures.filter { $0.normalizedText == "brought to you by" }
        // Should appear exactly once (first occurrence kept, duplicate removed)
        #expect(broughtToYou.count == 1)
        #expect(broughtToYou.first?.atomOrdinal == 0) // keeps earliest
    }

    @Test("Dedupes verb-prefix URL match with bare-domain URL match")
    func dedupesVerbPrefixURL() {
        // "visit example.com" matches both the `\bvisit \w+\.com` pattern and
        // the `\b\w+\.com\b` pattern. Before the fix this produced two url
        // entries for what is semantically a single URL mention.
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "please visit example.com for more info"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let urls = catalog.entries(for: .url)
        #expect(urls.count == 1, "Expected a single URL entry, got \(urls.count): \(urls.map(\.normalizedText))")
        #expect(urls.first?.normalizedText == "example.com")
        #expect(urls.first?.matchedText == "example.com")
        // And ensure no lingering "visit example.com" entry survived.
        #expect(!urls.contains { $0.normalizedText.contains("visit") })
    }

    @Test("Atom ordinals are correct on entries")
    func atomOrdinalsCorrect() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "No ads here just weather chat"),
            makeAtom(ordinal: 1, startTime: 10, endTime: 20,
                     text: "sponsored by Acme Corp today"),
            makeAtom(ordinal: 2, startTime: 20, endTime: 30,
                     text: "Back to the weather report"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let disclosures = catalog.entries(for: .disclosurePhrase)
        #expect(!disclosures.isEmpty, "Should find 'sponsored by' disclosure")
        for entry in disclosures {
            #expect(entry.atomOrdinal == 1) // only atom 1 has disclosure
        }
    }

    @Test("renderForPrompt produces expected format")
    func renderFormat() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "visit acme.com for details use code save20"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let rendered = catalog.renderForPrompt()
        // Each line should have format: [E0] "text" (category, atom N)
        for line in rendered.split(separator: "\n") {
            #expect(line.hasPrefix("[E"))
            #expect(line.contains("atom"))
        }
    }

    @Test("Brand spans extracted from disclosure phrases in lowercase ASR output")
    func brandSpansFromDisclosure() {
        // ASR output is typically all lowercase
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "this episode is sponsored by hello fresh and they make great meals"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let brands = catalog.entries(for: .brandSpan)
        #expect(!brands.isEmpty, "Should extract brand from 'sponsored by' pattern")
        // Stop-word trimming should prevent capturing "and they make great"
        #expect(brands.contains { $0.normalizedText == "hello fresh" },
                "Should extract exactly 'hello fresh', not trailing junk words")
        #expect(!brands.contains { $0.normalizedText.contains("and they") },
                "Should NOT include stop words after brand name")
    }

    @Test("Brand spans extracted from 'brought to you by' pattern")
    func brandSpansFromBroughtToYouBy() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "brought to you by our friends at betterhelp"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let brands = catalog.entries(for: .brandSpan)
        #expect(brands.contains { $0.normalizedText == "betterhelp" },
                "Should extract brand after 'brought to you by our friends at'")
    }

    @Test("Brand spans extracted as domain stems from URL patterns")
    func brandSpansFromDomainStems() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "head to betterhelp dot com slash podcast for a discount"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let brands = catalog.entries(for: .brandSpan)
        #expect(brands.contains { $0.normalizedText == "betterhelp" },
                "Should extract domain stem as brand span")
    }

    @Test("No brand spans from non-commercial atoms far from context")
    func noBrandSpansFarFromContext() {
        // Atom 0: has disclosure — creates commercial context for atoms 0-2
        // Atom 5: no disclosure, no commercial signals, far from atom 0
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "sponsored by acme corp for this episode"),
            makeAtom(ordinal: 5, startTime: 50, endTime: 60,
                     text: "anyway back to talking about gardening tips"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let brands = catalog.entries(for: .brandSpan)
        // Atom 0 should have brand from "sponsored by acme corp"
        #expect(brands.contains { $0.atomOrdinal == 0 && $0.normalizedText == "acme corp" },
                "Atom 0 with disclosure should produce brand span")
        // Atom 5 should have no brand — no commercial context
        let farBrands = brands.filter { $0.atomOrdinal == 5 }
        #expect(farBrands.isEmpty, "Atom far from commercial context should have no brands")
    }

    @Test("Brand name with stop words mid-name preserved correctly")
    func brandWithStopWordsMidName() {
        // "the north face" has "the" at the start — should be preserved
        // because trimming only removes TRAILING stop words
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "partnered with the north face for outdoor gear"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let brands = catalog.entries(for: .brandSpan)
        // "the north face for outdoor gear" -> trim trailing -> "the north face"
        #expect(brands.contains { $0.normalizedText == "the north face" },
                "Should preserve articles at start of brand name, trim trailing stop words")
    }

    @Test("No false brand from conversational 'thanks to' without 'sponsor'")
    func noFalseBrandFromThanksTo() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "check out acme.com for details"),
            // Within commercial context but "thanks to" is conversational
            makeAtom(ordinal: 1, startTime: 10, endTime: 20,
                     text: "thanks to our listeners for tuning in today"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let brands = catalog.entries(for: .brandSpan)
        // "thanks to our listeners" should NOT produce a brand
        // because the pattern now requires "thanks to our sponsor(s)"
        #expect(!brands.contains { $0.normalizedText.contains("listeners") },
                "Conversational 'thanks to' should not produce brand spans")
    }

    @Test("No false-positive promo codes from generic 'code' usage")
    func noFalsePositivePromoCodes() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "the source code works great in this repository"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let codes = catalog.entries(for: .promoCode)
        #expect(codes.isEmpty, "Generic 'code' usage should not produce promo code entries")
    }

    @Test("CTA phrases not extracted from non-commercial context")
    func ctaNotExtractedWithoutContext() {
        // "check it out" appears but there are no URLs, promos, or disclosures
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "i just found this great restaurant check it out sometime"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let ctas = catalog.entries(for: .ctaPhrase)
        #expect(ctas.isEmpty, "CTA phrases should not fire without commercial context")
    }

    @Test("CTA phrases extracted when near commercial context")
    func ctaExtractedInContext() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "brought to you by acme corp"),
            makeAtom(ordinal: 1, startTime: 10, endTime: 20,
                     text: "sign up now and get your free trial today"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let ctas = catalog.entries(for: .ctaPhrase)
        #expect(!ctas.isEmpty, "CTA phrases should be extracted near commercial context")
    }

    @Test("Bare domain URL subsumed when path URL exists")
    func urlSubsumption() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10,
                     text: "visit acme.com/offer for details at acme.com"),
        ]

        let catalog = EvidenceCatalogBuilder.build(
            atoms: atoms, analysisAssetId: "a1", transcriptVersion: "v1"
        )

        let urls = catalog.entries(for: .url)
        // "acme.com" should be subsumed by "acme.com/offer"
        #expect(urls.contains { $0.normalizedText.contains("acme.com/offer") },
                "Path URL should be present")
        #expect(!urls.contains { $0.normalizedText == "acme.com" },
                "Bare domain should be subsumed when path URL exists")
    }
}

// MARK: - SponsorKnowledgeMatcher Tests

@Suite("SponsorKnowledgeMatcher")
struct SponsorKnowledgeMatcherTests {

    @Test("Stub returns empty results")
    func stubReturnsEmpty() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10, text: "Some podcast text"),
        ]

        let matches = SponsorKnowledgeMatcher.match(atoms: atoms)
        #expect(matches.isEmpty)
    }

    @Test("Stub handles empty input")
    func stubEmptyInput() {
        let matches = SponsorKnowledgeMatcher.match(atoms: [])
        #expect(matches.isEmpty)
    }
}

// MARK: - AdCopyFingerprintMatcher Tests

@Suite("AdCopyFingerprintMatcher")
struct AdCopyFingerprintMatcherTests {

    @Test("Stub returns empty results")
    func stubReturnsEmpty() {
        let atoms = [
            makeAtom(ordinal: 0, startTime: 0, endTime: 10, text: "Some podcast text"),
        ]

        let matches = AdCopyFingerprintMatcher.match(atoms: atoms)
        #expect(matches.isEmpty)
    }

    @Test("Stub handles empty input")
    func stubEmptyInput() {
        let matches = AdCopyFingerprintMatcher.match(atoms: [])
        #expect(matches.isEmpty)
    }
}
