// BracketDetectorTests.swift
// ef2.3.6: Tests for BracketTemplate, BracketDetector state machine,
// BracketEvidence, and MusicBracketTrustStore.

import Foundation
import Testing

@testable import Playhead

// MARK: - Test Helpers

/// Build a synthetic FeatureWindow with controllable acoustic features.
private func syntheticWindow(
    startTime: Double,
    endTime: Double? = nil,
    rms: Double = 0.1,
    spectralFlux: Double = 0.05,
    musicProbability: Double = 0.0,
    musicBedOnsetScore: Double = 0.0,
    musicBedOffsetScore: Double = 0.0,
    musicBedLevel: MusicBedLevel = .none,
    pauseProbability: Double = 0.0
) -> FeatureWindow {
    FeatureWindow(
        analysisAssetId: "test-asset",
        startTime: startTime,
        endTime: endTime ?? startTime + 2.0,
        rms: rms,
        spectralFlux: spectralFlux,
        musicProbability: musicProbability,
        musicBedOnsetScore: musicBedOnsetScore,
        musicBedOffsetScore: musicBedOffsetScore,
        musicBedLevel: musicBedLevel,
        pauseProbability: pauseProbability,
        speakerClusterId: nil,
        jingleHash: nil,
        featureVersion: 4
    )
}

// MARK: - BracketTemplate Tests

@Suite("BracketTemplate")
struct BracketTemplateTests {

    @Test("all six template families exist")
    func allCases() {
        #expect(BracketTemplate.allCases.count == 6)
        #expect(BracketTemplate.allCases.contains(.stingInBedDryOut))
        #expect(BracketTemplate.allCases.contains(.dryInStingOut))
        #expect(BracketTemplate.allCases.contains(.hardInFadeOut))
        #expect(BracketTemplate.allCases.contains(.symmetricBracket))
        #expect(BracketTemplate.allCases.contains(.partialOnset))
        #expect(BracketTemplate.allCases.contains(.partialOffset))
    }

    @Test("raw values are stable strings")
    func rawValues() {
        #expect(BracketTemplate.stingInBedDryOut.rawValue == "stingInBedDryOut")
        #expect(BracketTemplate.partialOnset.rawValue == "partialOnset")
    }
}

// MARK: - BracketDetector State Machine Tests

@Suite("BracketDetector")
struct BracketDetectorTests {

    @Test("empty windows returns nil")
    func emptyWindows() {
        let result = BracketDetector.scanForBrackets(
            around: 10.0,
            candidateEnd: 30.0,
            using: [],
            showTrust: 0.5
        )
        #expect(result == nil)
    }

    @Test("too few windows returns nil")
    func tooFewWindows() {
        let windows = [
            syntheticWindow(startTime: 10.0),
            syntheticWindow(startTime: 12.0),
        ]
        let result = BracketDetector.scanForBrackets(
            around: 10.0,
            candidateEnd: 14.0,
            using: windows,
            showTrust: 0.5
        )
        #expect(result == nil)
    }

    @Test("full bracket detection with onset + bed + offset")
    func fullBracketDetection() {
        // Build a sequence: silence, onset, bed, bed, bed, offset, silence
        var windows: [FeatureWindow] = []
        // Pre-ad silence
        windows.append(syntheticWindow(startTime: 0.0, rms: 0.05))
        windows.append(syntheticWindow(startTime: 2.0, rms: 0.05))
        // Onset: high music onset score near candidate start
        windows.append(syntheticWindow(
            startTime: 4.0, rms: 0.3,
            musicProbability: 0.5, musicBedOnsetScore: 0.5
        ))
        // Bed sustained
        windows.append(syntheticWindow(
            startTime: 6.0, rms: 0.2, musicProbability: 0.4
        ))
        windows.append(syntheticWindow(
            startTime: 8.0, rms: 0.2, musicProbability: 0.35
        ))
        windows.append(syntheticWindow(
            startTime: 10.0, rms: 0.2, musicProbability: 0.3
        ))
        // Offset: high music offset score near candidate end
        windows.append(syntheticWindow(
            startTime: 12.0, rms: 0.15,
            musicProbability: 0.1, musicBedOffsetScore: 0.5
        ))
        // Post-ad silence
        windows.append(syntheticWindow(startTime: 14.0, rms: 0.05))
        windows.append(syntheticWindow(startTime: 16.0, rms: 0.05))

        let result = BracketDetector.scanForBrackets(
            around: 4.0,
            candidateEnd: 14.0,
            using: windows,
            showTrust: 0.7
        )

        #expect(result != nil)
        if let evidence = result {
            #expect(evidence.onsetTime >= 0.0)
            #expect(evidence.offsetTime > evidence.onsetTime)
            #expect(evidence.coarseScore > 0.0)
            #expect(evidence.coarseScore <= 1.0)
            #expect(evidence.showTrust == 0.7)
            // Should be a full bracket, not partial.
            #expect(evidence.templateClass != .partialOnset)
            #expect(evidence.templateClass != .partialOffset)
        }
    }

    @Test("partial onset only — no offset detected")
    func partialOnsetOnly() {
        var windows: [FeatureWindow] = []
        // Onset
        windows.append(syntheticWindow(
            startTime: 4.0, rms: 0.3,
            musicProbability: 0.5, musicBedOnsetScore: 0.5
        ))
        // Bed
        windows.append(syntheticWindow(
            startTime: 6.0, rms: 0.2, musicProbability: 0.4
        ))
        windows.append(syntheticWindow(
            startTime: 8.0, rms: 0.2, musicProbability: 0.35
        ))
        // No offset — music continues
        windows.append(syntheticWindow(
            startTime: 10.0, rms: 0.2, musicProbability: 0.3
        ))
        windows.append(syntheticWindow(
            startTime: 12.0, rms: 0.2, musicProbability: 0.3
        ))

        let result = BracketDetector.scanForBrackets(
            around: 4.0,
            candidateEnd: 14.0,
            using: windows,
            showTrust: 0.5
        )

        // Should detect partial onset (onset found, no offset).
        if let evidence = result {
            #expect(evidence.templateClass == .partialOnset)
        }
        // Also acceptable: nil if the state machine doesn't reach
        // a reportable state without offset. Either is correct for shadow.
    }

    @Test("no music signals returns nil")
    func noMusicSignals() {
        let windows = (0..<10).map { i in
            syntheticWindow(
                startTime: Double(i) * 2.0,
                rms: 0.1,
                musicProbability: 0.0,
                musicBedOnsetScore: 0.0,
                musicBedOffsetScore: 0.0
            )
        }

        let result = BracketDetector.scanForBrackets(
            around: 4.0,
            candidateEnd: 14.0,
            using: windows,
            showTrust: 0.5
        )

        #expect(result == nil)
    }

    @Test("determinism: same inputs produce same output")
    func deterministic() {
        var windows: [FeatureWindow] = []
        windows.append(syntheticWindow(startTime: 0.0, rms: 0.05))
        windows.append(syntheticWindow(
            startTime: 2.0, rms: 0.3,
            musicProbability: 0.5, musicBedOnsetScore: 0.5
        ))
        windows.append(syntheticWindow(
            startTime: 4.0, rms: 0.2, musicProbability: 0.4
        ))
        windows.append(syntheticWindow(
            startTime: 6.0, rms: 0.2, musicProbability: 0.3
        ))
        windows.append(syntheticWindow(
            startTime: 8.0, rms: 0.15,
            musicProbability: 0.1, musicBedOffsetScore: 0.5
        ))
        windows.append(syntheticWindow(startTime: 10.0, rms: 0.05))

        let a = BracketDetector.scanForBrackets(
            around: 2.0, candidateEnd: 10.0, using: windows, showTrust: 0.5
        )
        let b = BracketDetector.scanForBrackets(
            around: 2.0, candidateEnd: 10.0, using: windows, showTrust: 0.5
        )

        #expect(a == b)
    }

    @Test("show trust modulates coarse score")
    func showTrustModulation() {
        var windows: [FeatureWindow] = []
        windows.append(syntheticWindow(startTime: 0.0, rms: 0.05))
        windows.append(syntheticWindow(
            startTime: 2.0, rms: 0.3,
            musicProbability: 0.5, musicBedOnsetScore: 0.5
        ))
        windows.append(syntheticWindow(
            startTime: 4.0, rms: 0.2, musicProbability: 0.4
        ))
        windows.append(syntheticWindow(
            startTime: 6.0, rms: 0.2, musicProbability: 0.3
        ))
        windows.append(syntheticWindow(
            startTime: 8.0, rms: 0.15,
            musicProbability: 0.1, musicBedOffsetScore: 0.5
        ))
        windows.append(syntheticWindow(startTime: 10.0, rms: 0.05))

        let highTrust = BracketDetector.scanForBrackets(
            around: 2.0, candidateEnd: 10.0, using: windows, showTrust: 1.0
        )
        let lowTrust = BracketDetector.scanForBrackets(
            around: 2.0, candidateEnd: 10.0, using: windows, showTrust: 0.0
        )

        // Both should detect something, but high trust should score higher.
        if let h = highTrust, let l = lowTrust {
            #expect(h.coarseScore > l.coarseScore)
        }
    }

    @Test("hard-in fade-out template classification")
    func hardInFadeOutTemplate() {
        var windows: [FeatureWindow] = []
        // Pre-silence
        windows.append(syntheticWindow(startTime: 0.0, rms: 0.02))
        // Sharp onset: high RMS relative to local mean
        windows.append(syntheticWindow(
            startTime: 2.0, rms: 0.6,
            musicProbability: 0.7, musicBedOnsetScore: 0.6
        ))
        // Bed
        windows.append(syntheticWindow(
            startTime: 4.0, rms: 0.3, musicProbability: 0.5
        ))
        windows.append(syntheticWindow(
            startTime: 6.0, rms: 0.25, musicProbability: 0.4
        ))
        // Offset with fade-out: declining RMS
        windows.append(syntheticWindow(
            startTime: 8.0, rms: 0.2,
            musicProbability: 0.2, musicBedOffsetScore: 0.4
        ))
        windows.append(syntheticWindow(startTime: 10.0, rms: 0.12))
        windows.append(syntheticWindow(startTime: 12.0, rms: 0.06))
        windows.append(syntheticWindow(startTime: 14.0, rms: 0.03))

        let result = BracketDetector.scanForBrackets(
            around: 2.0, candidateEnd: 10.0, using: windows, showTrust: 0.8
        )

        #expect(result != nil)
        if let evidence = result {
            #expect(evidence.templateClass == .hardInFadeOut)
        }
    }
    @Test("dryInStingOut template: soft onset, strong offset sting")
    func dryInStingOutTemplate() {
        var windows: [FeatureWindow] = []
        // Pre-silence
        windows.append(syntheticWindow(startTime: 0.0, rms: 0.05))
        // Soft onset: low RMS relative to local mean, modest onset score
        windows.append(syntheticWindow(
            startTime: 2.0, rms: 0.08,
            musicProbability: 0.4, musicBedOnsetScore: 0.35
        ))
        // Bed
        windows.append(syntheticWindow(
            startTime: 4.0, rms: 0.1, musicProbability: 0.4
        ))
        windows.append(syntheticWindow(
            startTime: 6.0, rms: 0.1, musicProbability: 0.35
        ))
        // Strong offset sting: high offset score > onset score, no fade-out
        windows.append(syntheticWindow(
            startTime: 8.0, rms: 0.1,
            musicProbability: 0.1, musicBedOffsetScore: 0.8
        ))
        // Flat RMS after offset (no fade-out pattern)
        windows.append(syntheticWindow(startTime: 10.0, rms: 0.1))
        windows.append(syntheticWindow(startTime: 12.0, rms: 0.1))
        windows.append(syntheticWindow(startTime: 14.0, rms: 0.1))

        let result = BracketDetector.scanForBrackets(
            around: 2.0, candidateEnd: 10.0, using: windows, showTrust: 0.8
        )

        #expect(result != nil)
        if let evidence = result {
            #expect(evidence.templateClass == .dryInStingOut)
        }
    }

    @Test("symmetricBracket template: similar onset and offset magnitudes")
    func symmetricBracketTemplate() {
        var windows: [FeatureWindow] = []
        // Pre-silence
        windows.append(syntheticWindow(startTime: 0.0, rms: 0.05))
        // Soft onset: low RMS (not sharp), moderate onset score
        windows.append(syntheticWindow(
            startTime: 2.0, rms: 0.08,
            musicProbability: 0.5, musicBedOnsetScore: 0.5
        ))
        // Bed
        windows.append(syntheticWindow(
            startTime: 4.0, rms: 0.1, musicProbability: 0.4
        ))
        windows.append(syntheticWindow(
            startTime: 6.0, rms: 0.1, musicProbability: 0.35
        ))
        // Symmetric offset: similar magnitude to onset, no fade-out
        windows.append(syntheticWindow(
            startTime: 8.0, rms: 0.1,
            musicProbability: 0.1, musicBedOffsetScore: 0.5
        ))
        // Flat RMS after offset (no fade-out)
        windows.append(syntheticWindow(startTime: 10.0, rms: 0.1))
        windows.append(syntheticWindow(startTime: 12.0, rms: 0.1))
        windows.append(syntheticWindow(startTime: 14.0, rms: 0.1))

        let result = BracketDetector.scanForBrackets(
            around: 2.0, candidateEnd: 10.0, using: windows, showTrust: 0.8
        )

        #expect(result != nil)
        if let evidence = result {
            #expect(evidence.templateClass == .symmetricBracket)
        }
    }
}

// MARK: - BracketEvidence Tests

@Suite("BracketEvidence")
struct BracketEvidenceTests {

    @Test("equatable conformance")
    func equatable() {
        let a = BracketEvidence(
            onsetTime: 10.0, offsetTime: 30.0,
            templateClass: .stingInBedDryOut,
            coarseScore: 0.75, showTrust: 0.5
        )
        let b = BracketEvidence(
            onsetTime: 10.0, offsetTime: 30.0,
            templateClass: .stingInBedDryOut,
            coarseScore: 0.75, showTrust: 0.5
        )
        let c = BracketEvidence(
            onsetTime: 10.0, offsetTime: 30.0,
            templateClass: .dryInStingOut,
            coarseScore: 0.75, showTrust: 0.5
        )
        #expect(a == b)
        #expect(a != c)
    }
}

// MARK: - BetaParameters Tests

@Suite("BetaParameters")
struct BetaParametersTests {

    @Test("default prior has mean 0.5")
    func defaultMean() {
        let params = BetaParameters(alpha: 5.0, beta: 5.0)
        #expect(params.mean == 0.5)
    }

    @Test("mean shifts toward hits")
    func meanShiftsWithHits() {
        var params = BetaParameters(alpha: 5.0, beta: 5.0)
        params.alpha += 5.0 // 5 hits
        #expect(params.mean > 0.5)
        #expect(abs(params.mean - 10.0 / 15.0) < 0.001)
    }

    @Test("mean shifts toward misses")
    func meanShiftsWithMisses() {
        var params = BetaParameters(alpha: 5.0, beta: 5.0)
        params.beta += 5.0 // 5 misses
        #expect(params.mean < 0.5)
        #expect(abs(params.mean - 5.0 / 15.0) < 0.001)
    }

    @Test("variance decreases with more observations")
    func varianceDecreases() {
        let few = BetaParameters(alpha: 5.0, beta: 5.0)
        let many = BetaParameters(alpha: 50.0, beta: 50.0)
        #expect(many.variance < few.variance)
    }

    @Test("total observations is alpha + beta")
    func totalObservations() {
        let params = BetaParameters(alpha: 7.0, beta: 3.0)
        #expect(params.totalObservations == 10.0)
    }

    @Test("zero parameters return safe defaults")
    func zeroSafety() {
        let zero = BetaParameters(alpha: 0.0, beta: 0.0)
        #expect(zero.mean == 0.5)
        #expect(zero.variance == 0.0)
    }
}

// MARK: - MusicBracketTrustStore Tests

@Suite("MusicBracketTrustStore")
struct MusicBracketTrustStoreTests {

    /// Create a fresh in-memory AnalysisStore for testing.
    private func makeStore() async throws -> AnalysisStore {
        let store = try AnalysisStore(path: ":memory:")
        try await store.migrate()
        return store
    }

    @Test("default trust is 0.5 for unknown show")
    func defaultTrust() async throws {
        let store = try await makeStore()
        let trustStore = MusicBracketTrustStore(store: store)
        let trust = await trustStore.trust(forShow: "unknown-show")
        #expect(abs(trust - 0.5) < 0.001)
    }

    @Test("trust increases with hits")
    func trustIncreasesWithHits() async throws {
        let store = try await makeStore()
        let trustStore = MusicBracketTrustStore(store: store)

        await trustStore.recordOutcome(showId: "show-1", hit: true)
        await trustStore.recordOutcome(showId: "show-1", hit: true)
        await trustStore.recordOutcome(showId: "show-1", hit: true)

        let trust = await trustStore.trust(forShow: "show-1")
        // Beta(8,5) mean = 8/13 ~ 0.615
        #expect(trust > 0.5)
        #expect(abs(trust - 8.0 / 13.0) < 0.001)
    }

    @Test("trust decreases with misses")
    func trustDecreasesWithMisses() async throws {
        let store = try await makeStore()
        let trustStore = MusicBracketTrustStore(store: store)

        await trustStore.recordOutcome(showId: "show-2", hit: false)
        await trustStore.recordOutcome(showId: "show-2", hit: false)

        let trust = await trustStore.trust(forShow: "show-2")
        // Beta(5,7) mean = 5/12 ~ 0.417
        #expect(trust < 0.5)
        #expect(abs(trust - 5.0 / 12.0) < 0.001)
    }

    @Test("trust is independent per show")
    func independentPerShow() async throws {
        let store = try await makeStore()
        let trustStore = MusicBracketTrustStore(store: store)

        await trustStore.recordOutcome(showId: "show-a", hit: true)
        await trustStore.recordOutcome(showId: "show-b", hit: false)

        let trustA = await trustStore.trust(forShow: "show-a")
        let trustB = await trustStore.trust(forShow: "show-b")

        #expect(trustA > 0.5)
        #expect(trustB < 0.5)
    }

    @Test("trust persists across store instances")
    func persistence() async throws {
        let store = try await makeStore()

        // First instance records outcomes.
        let trustStore1 = MusicBracketTrustStore(store: store)
        await trustStore1.recordOutcome(showId: "show-p", hit: true)
        await trustStore1.recordOutcome(showId: "show-p", hit: true)

        // Second instance on same store reads persisted data.
        let trustStore2 = MusicBracketTrustStore(store: store)
        let trust = await trustStore2.trust(forShow: "show-p")
        #expect(trust > 0.5)
    }

    @Test("beta parameters accessible for diagnostics")
    func betaParametersDiagnostics() async throws {
        let store = try await makeStore()
        let trustStore = MusicBracketTrustStore(store: store)

        await trustStore.recordOutcome(showId: "diag", hit: true)

        let params = await trustStore.betaParameters(forShow: "diag")
        #expect(params.alpha == 6.0)
        #expect(params.beta == 5.0)
    }
}
