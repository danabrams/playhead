import Foundation
import Testing

@testable import Playhead

@Suite("SpeakerLabelProvider")
struct SpeakerLabelProviderTests {

    // MARK: - AcousticSpeakerChangeProvider (fallback)

    @Test("acoustic provider reports no validated labels")
    func acousticProviderHasNoValidatedLabels() {
        let provider = AcousticSpeakerChangeProvider()
        #expect(provider.kind == .acousticProxy)
        #expect(!provider.hasValidatedLabels)
    }

    @Test("acoustic provider returns empty speaker labels")
    func acousticProviderReturnsEmptyLabels() {
        let provider = AcousticSpeakerChangeProvider()
        let labels = provider.speakerLabels(startTime: 0, endTime: 60)
        #expect(labels.isEmpty)
    }

    @Test("acoustic provider returns empty speaker changes")
    func acousticProviderReturnsEmptyChanges() {
        let provider = AcousticSpeakerChangeProvider()
        let changes = provider.speakerChanges(startTime: 0, endTime: 60)
        #expect(changes.isEmpty)
    }

    @Test("acoustic provider returns nil speakerId")
    func acousticProviderReturnsNilSpeakerId() {
        let provider = AcousticSpeakerChangeProvider()
        #expect(provider.speakerId(at: 30.0) == nil)
    }

    @Test("acoustic provider delegates to FeatureWindow score")
    func acousticProviderDelegatesToWindowScore() {
        let provider = AcousticSpeakerChangeProvider()
        let window = makeWindow(start: 10, end: 12, speakerChangeProxy: 0.73)
        let score = provider.speakerChangeProxyScore(
            for: window,
            previousWindow: nil,
            nextWindow: nil
        )
        expectApproximately(score, 0.73)
    }

    // MARK: - ValidatedSpeakerLabelProvider

    @Test("validated provider reports has validated labels")
    func validatedProviderHasLabels() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
        ])
        #expect(provider.kind == .validatedASR)
        #expect(provider.hasValidatedLabels)
    }

    @Test("validated provider returns speaker labels in range")
    func validatedProviderReturnsLabelsInRange() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
            SpeakerLabel(speakerId: 0, startTime: 60, endTime: 90),
        ])
        let labels = provider.speakerLabels(startTime: 25, endTime: 65)
        #expect(labels.count == 3) // overlaps all three ranges
    }

    @Test("validated provider detects speaker changes")
    func validatedProviderDetectsChanges() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
            SpeakerLabel(speakerId: 0, startTime: 60, endTime: 90),
        ])
        let changes = provider.speakerChanges(startTime: 0, endTime: 90)
        #expect(changes.count == 2)
        #expect(changes[0].time == 30)
        #expect(changes[0].fromSpeakerId == 0)
        #expect(changes[0].toSpeakerId == 1)
        #expect(changes[1].time == 60)
        #expect(changes[1].fromSpeakerId == 1)
        #expect(changes[1].toSpeakerId == 0)
    }

    @Test("validated provider returns no changes for same speaker")
    func validatedProviderNoChangesForSameSpeaker() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 0, startTime: 30, endTime: 60),
        ])
        let changes = provider.speakerChanges(startTime: 0, endTime: 60)
        #expect(changes.isEmpty)
    }

    @Test("adjacent nil-nil speakerIds produce no change event")
    func adjacentNilSpeakerIdsProduceNoChange() {
        // Swift's != returns false for nil != nil, so two adjacent labels
        // with unknown speakers should NOT produce a speaker change event.
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: nil, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: nil, startTime: 30, endTime: 60),
        ])
        let changes = provider.speakerChanges(startTime: 0, endTime: 60)
        #expect(changes.isEmpty)
    }

    @Test("validated provider returns speakerId at time")
    func validatedProviderReturnsSpeakerIdAtTime() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
        ])
        #expect(provider.speakerId(at: 15.0) == 0)
        #expect(provider.speakerId(at: 45.0) == 1)
    }

    // MARK: - speakerChangeProxyScore with validated labels

    @Test("validated provider returns 1.0 at turn boundary window")
    func validatedProviderReturnsOneAtBoundary() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
        ])
        // Window that contains the boundary at t=30
        let window = makeWindow(start: 28, end: 32, speakerChangeProxy: 0.1)
        let score = provider.speakerChangeProxyScore(
            for: window,
            previousWindow: nil,
            nextWindow: nil
        )
        expectApproximately(score, 1.0)
    }

    @Test("validated provider returns 0.0 away from turn boundary")
    func validatedProviderReturnsZeroAwayFromBoundary() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
        ])
        // Window far from any boundary
        let window = makeWindow(start: 10, end: 14, speakerChangeProxy: 0.1)
        let score = provider.speakerChangeProxyScore(
            for: window,
            previousWindow: nil,
            nextWindow: nil
        )
        expectApproximately(score, 0.0)
    }

    @Test("validated provider smooths +/-1 window at turn boundary")
    func validatedProviderSmoothsAdjacentWindows() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
        ])

        // The boundary is at t=30.
        // Window adjacent to boundary (previous window contains boundary).
        let prevWindow = makeWindow(start: 28, end: 32, speakerChangeProxy: 0.1)
        let currentWindow = makeWindow(start: 32, end: 36, speakerChangeProxy: 0.05)

        let score = provider.speakerChangeProxyScore(
            for: currentWindow,
            previousWindow: prevWindow,
            nextWindow: nil
        )
        // Should get smoothed score (0.4) since previous window has the boundary.
        expectApproximately(score, 0.4)
    }

    @Test("validated provider smooths next window at turn boundary")
    func validatedProviderSmoothsNextWindow() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
        ])

        // The boundary is at t=30.
        let currentWindow = makeWindow(start: 24, end: 28, speakerChangeProxy: 0.05)
        let nextWindow = makeWindow(start: 28, end: 32, speakerChangeProxy: 0.1)

        let score = provider.speakerChangeProxyScore(
            for: currentWindow,
            previousWindow: nil,
            nextWindow: nextWindow
        )
        // Should get smoothed score (0.4) since next window has the boundary.
        expectApproximately(score, 0.4)
    }

    // MARK: - Factory

    @Test("factory returns acoustic provider when no labels")
    func factoryReturnsAcousticWhenNoLabels() {
        let provider = SpeakerLabelProviderFactory.makeProvider(labels: nil)
        #expect(provider.kind == .acousticProxy)
        #expect(!provider.hasValidatedLabels)
    }

    @Test("factory returns acoustic provider when empty labels")
    func factoryReturnsAcousticWhenEmptyLabels() {
        let provider = SpeakerLabelProviderFactory.makeProvider(labels: [])
        #expect(provider.kind == .acousticProxy)
    }

    @Test("factory returns validated provider when labels present")
    func factoryReturnsValidatedWhenLabelsPresent() {
        let labels = [SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30)]
        let provider = SpeakerLabelProviderFactory.makeProvider(labels: labels)
        #expect(provider.kind == .validatedASR)
        #expect(provider.hasValidatedLabels)
    }

    @Test("ASR speaker labels availability check returns false")
    func asrSpeakerLabelsNotAvailable() {
        // Until iOS 26 ships and the API is verified, this must return false.
        #expect(!SpeakerLabelProviderFactory.isASRSpeakerLabelsAvailable)
    }

    // MARK: - TranscriptChunk speakerId

    @Test("TranscriptChunk speakerId defaults to nil")
    func transcriptChunkSpeakerIdDefaultsToNil() {
        let chunk = TranscriptChunk(
            id: "c1",
            analysisAssetId: "a1",
            segmentFingerprint: "fp",
            chunkIndex: 0,
            startTime: 0,
            endTime: 5,
            text: "hello",
            normalizedText: "hello",
            pass: "fast",
            modelVersion: "v1",
            transcriptVersion: nil,
            atomOrdinal: nil
        )
        #expect(chunk.speakerId == nil)
    }

    @Test("TranscriptChunk speakerId can be set")
    func transcriptChunkSpeakerIdCanBeSet() {
        let chunk = TranscriptChunk(
            id: "c1",
            analysisAssetId: "a1",
            segmentFingerprint: "fp",
            chunkIndex: 0,
            startTime: 0,
            endTime: 5,
            text: "hello",
            normalizedText: "hello",
            pass: "fast",
            modelVersion: "v1",
            transcriptVersion: nil,
            atomOrdinal: nil,
            speakerId: 2
        )
        #expect(chunk.speakerId == 2)
    }

    // MARK: - TranscriptAtom speakerId propagation

    @Test("TranscriptAtom speakerId defaults to nil")
    func transcriptAtomSpeakerIdDefaultsToNil() {
        let atom = TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "a1",
                transcriptVersion: "v1",
                atomOrdinal: 0
            ),
            contentHash: "hash",
            startTime: 0,
            endTime: 5,
            text: "hello",
            chunkIndex: 0
        )
        #expect(atom.speakerId == nil)
    }

    @Test("TranscriptAtom speakerId can be set")
    func transcriptAtomSpeakerIdCanBeSet() {
        let atom = TranscriptAtom(
            atomKey: TranscriptAtomKey(
                analysisAssetId: "a1",
                transcriptVersion: "v1",
                atomOrdinal: 0
            ),
            contentHash: "hash",
            startTime: 0,
            endTime: 5,
            text: "hello",
            chunkIndex: 0,
            speakerId: 3
        )
        #expect(atom.speakerId == 3)
    }

    @Test("atomizer propagates speakerId from chunk to atom")
    func atomizerPropagatesSpeakerId() {
        let chunks = [
            TranscriptChunk(
                id: "c1",
                analysisAssetId: "a1",
                segmentFingerprint: "fp1",
                chunkIndex: 0,
                startTime: 0,
                endTime: 5,
                text: "hello",
                normalizedText: "hello",
                pass: "final",
                modelVersion: "v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                speakerId: 0
            ),
            TranscriptChunk(
                id: "c2",
                analysisAssetId: "a1",
                segmentFingerprint: "fp2",
                chunkIndex: 1,
                startTime: 5,
                endTime: 10,
                text: "world",
                normalizedText: "world",
                pass: "final",
                modelVersion: "v1",
                transcriptVersion: nil,
                atomOrdinal: nil,
                speakerId: 1
            ),
        ]

        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: "a1",
            normalizationHash: "norm",
            sourceHash: "src"
        )

        #expect(atoms.count == 2)
        #expect(atoms[0].speakerId == 0)
        #expect(atoms[1].speakerId == 1)
    }

    @Test("atomizer preserves nil speakerId")
    func atomizerPreservesNilSpeakerId() {
        let chunks = [
            TranscriptChunk(
                id: "c1",
                analysisAssetId: "a1",
                segmentFingerprint: "fp1",
                chunkIndex: 0,
                startTime: 0,
                endTime: 5,
                text: "hello",
                normalizedText: "hello",
                pass: "final",
                modelVersion: "v1",
                transcriptVersion: nil,
                atomOrdinal: nil
                // speakerId defaults to nil
            ),
        ]

        let (atoms, _) = TranscriptAtomizer.atomize(
            chunks: chunks,
            analysisAssetId: "a1",
            normalizationHash: "norm",
            sourceHash: "src"
        )

        #expect(atoms[0].speakerId == nil)
    }

    // MARK: - Multiple speaker changes

    @Test("validated provider handles multiple rapid speaker changes")
    func validatedProviderMultipleChanges() {
        // Simulate a rapid-fire conversation with 3 speakers
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 10),
            SpeakerLabel(speakerId: 1, startTime: 10, endTime: 15),
            SpeakerLabel(speakerId: 2, startTime: 15, endTime: 20),
            SpeakerLabel(speakerId: 0, startTime: 20, endTime: 30),
        ])

        let changes = provider.speakerChanges(startTime: 0, endTime: 30)
        #expect(changes.count == 3) // changes at t=10, t=15, t=20

        // Window spanning t=10 boundary
        let window1 = makeWindow(start: 8, end: 12, speakerChangeProxy: 0)
        #expect(provider.speakerChangeProxyScore(for: window1, previousWindow: nil, nextWindow: nil) == 1.0)

        // Window spanning t=15 boundary
        let window2 = makeWindow(start: 13, end: 17, speakerChangeProxy: 0)
        #expect(provider.speakerChangeProxyScore(for: window2, previousWindow: nil, nextWindow: nil) == 1.0)
    }

    @Test("validated provider speakerId at exact boundary returns later speaker")
    func validatedProviderSpeakerIdAtBoundary() {
        let provider = ValidatedSpeakerLabelProvider(labels: [
            SpeakerLabel(speakerId: 0, startTime: 0, endTime: 30),
            SpeakerLabel(speakerId: 1, startTime: 30, endTime: 60),
        ])
        // At exactly t=30, the second label starts (startTime <= 30 && endTime > 30)
        #expect(provider.speakerId(at: 30.0) == 1)
    }

    // MARK: - Helpers

    private func makeWindow(
        start: Double,
        end: Double,
        speakerChangeProxy: Double
    ) -> FeatureWindow {
        FeatureWindow(
            analysisAssetId: "test-asset",
            startTime: start,
            endTime: end,
            rms: 0.1,
            spectralFlux: 0.05,
            musicProbability: 0,
            speakerChangeProxyScore: speakerChangeProxy,
            musicBedChangeScore: 0,
            pauseProbability: 0.1,
            speakerClusterId: nil,
            jingleHash: nil,
            featureVersion: 4
        )
    }

    private func expectApproximately(
        _ actual: Double,
        _ expected: Double,
        tolerance: Double = 0.01,
        sourceLocation: SourceLocation = #_sourceLocation
    ) {
        #expect(
            abs(actual - expected) < tolerance,
            "Expected \(expected) +/- \(tolerance), got \(actual)",
            sourceLocation: sourceLocation
        )
    }
}
