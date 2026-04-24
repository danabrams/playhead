// AcousticFeatureTestSupport.swift
// playhead-gtt9.12: shared builders for the AcousticFeatures test suite.

import Foundation
@testable import Playhead

enum AcousticFeatureFixtures {

    /// Build a single `FeatureWindow` with the fields the acoustic features care
    /// about. Remaining fields take reasonable defaults.
    static func window(
        assetId: String = "test-asset",
        startTime: Double,
        endTime: Double,
        rms: Double,
        spectralFlux: Double = 0.1,
        musicProbability: Double = 0,
        pauseProbability: Double = 0,
        speakerChangeProxyScore: Double = 0,
        musicBedLevel: MusicBedLevel = .none,
        speakerClusterId: Int? = nil
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: assetId,
            startTime: startTime,
            endTime: endTime,
            rms: rms,
            spectralFlux: spectralFlux,
            musicProbability: musicProbability,
            speakerChangeProxyScore: speakerChangeProxyScore,
            musicBedChangeScore: 0,
            musicBedOnsetScore: 0,
            musicBedOffsetScore: 0,
            musicBedLevel: musicBedLevel,
            pauseProbability: pauseProbability,
            speakerClusterId: speakerClusterId,
            jingleHash: nil,
            featureVersion: 4
        )
    }

    /// Stable 60-window episode fixture: alternating quiet speech and louder
    /// conversational peaks, no music. Useful as the "host content baseline"
    /// in feature tests.
    static func quietHostBaseline(count: Int = 60) -> [FeatureWindow] {
        var out: [FeatureWindow] = []
        out.reserveCapacity(count)
        for i in 0..<count {
            out.append(window(
                startTime: Double(i) * 2,
                endTime: Double(i + 1) * 2,
                rms: 0.20 + (Double(i % 5) * 0.005),
                spectralFlux: 0.05,
                musicProbability: 0.02,
                pauseProbability: 0.05,
                speakerChangeProxyScore: 0.05,
                speakerClusterId: i % 2
            ))
        }
        return out
    }
}
