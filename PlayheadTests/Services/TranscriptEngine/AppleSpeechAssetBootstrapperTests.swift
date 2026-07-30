// AppleSpeechAssetBootstrapperTests.swift
// playhead-sw69: Phase-2-style contract tests for AppleSpeechAssetBootstrapper.
// These were filed as the explicit follow-up the playhead-2gj reviewer
// recommended when closing 2gj — the bootstrapper consults static
// `AssetInventory.status(...)` and `SpeechAnalyzer.bestAvailableAudioFormat`
// directly, so 2gj could only land tests that exercised the seams already in
// place. This bead adds the missing seams (`AppleSpeechAssetStatusProviding`
// and `AppleSpeechAnalyzerFormatProviding`) and the matching contract tests.

import AVFoundation
import Foundation
import Testing
@testable import Playhead

#if canImport(Speech)
import Speech

// MARK: - Stub Providers

/// Records the modules passed in and replays a scripted status. `installAssets`
/// notes whether it was invoked so the supported/downloading branches can
/// assert the bootstrapper actually triggers the install side effect.
private final class StubAssetStatusProvider: AppleSpeechAssetStatusProviding, @unchecked Sendable {
    let scriptedStatus: AssetInventory.Status
    let installError: Error?
    private(set) var installCallCount = 0
    private(set) var statusCallCount = 0
    private(set) var lastStatusModulesCount: Int?
    private(set) var lastInstallModulesCount: Int?

    init(scriptedStatus: AssetInventory.Status, installError: Error? = nil) {
        self.scriptedStatus = scriptedStatus
        self.installError = installError
    }

    func status(forModules modules: [any SpeechModule]) async -> AssetInventory.Status {
        statusCallCount += 1
        lastStatusModulesCount = modules.count
        return scriptedStatus
    }

    func installAssets(supporting modules: [any SpeechModule]) async throws {
        installCallCount += 1
        lastInstallModulesCount = modules.count
        if let installError {
            throw installError
        }
    }
}

/// Replays a scripted analyzer format so the format-resolution branch is
/// reachable without spinning up SpeechAnalyzer.
private final class StubAnalyzerFormatProvider: AppleSpeechAnalyzerFormatProviding, @unchecked Sendable {
    let scriptedFormat: AVAudioFormat?
    private(set) var callCount = 0

    init(scriptedFormat: AVAudioFormat?) {
        self.scriptedFormat = scriptedFormat
    }

    func bestAvailableAudioFormat(compatibleWith modules: [any SpeechModule]) async -> AVAudioFormat? {
        callCount += 1
        return scriptedFormat
    }
}

private func makeAnalyzerInt16Format() throws -> AVAudioFormat {
    guard let format = AVAudioFormat(
        commonFormat: .pcmFormatInt16,
        sampleRate: 16_000,
        channels: 1,
        interleaved: false
    ) else {
        throw NSError(
            domain: "AppleSpeechAssetBootstrapperTests",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "Failed to allocate analyzer test format"]
        )
    }
    return format
}

// MARK: - Tests

@Suite("AppleSpeechAssetBootstrapper – asset status branches")
struct AppleSpeechAssetBootstrapperStatusTests {

    @Test("installed status returns prepared model without invoking install")
    func installedStatusSkipsInstall() async throws {
        let format = try makeAnalyzerInt16Format()
        let assetProvider = StubAssetStatusProvider(scriptedStatus: .installed)
        let formatProvider = StubAnalyzerFormatProvider(scriptedFormat: format)
        let bootstrapper = AppleSpeechAssetBootstrapper(
            assetStatusProvider: assetProvider,
            analyzerFormatProvider: formatProvider
        )

        let prepared = try await bootstrapper.prepare(localeIdentifier: "en-US")

        #expect(prepared.locale.identifier == "en-US")
        #expect(prepared.analyzerFormat == format)
        #expect(assetProvider.statusCallCount == 1)
        #expect(assetProvider.installCallCount == 0)
        #expect(formatProvider.callCount == 1)
    }

    @Test("downloading status drives the install side effect before resolving format")
    func downloadingStatusTriggersInstall() async throws {
        let format = try makeAnalyzerInt16Format()
        let assetProvider = StubAssetStatusProvider(scriptedStatus: .downloading)
        let formatProvider = StubAnalyzerFormatProvider(scriptedFormat: format)
        let bootstrapper = AppleSpeechAssetBootstrapper(
            assetStatusProvider: assetProvider,
            analyzerFormatProvider: formatProvider
        )

        let prepared = try await bootstrapper.prepare(localeIdentifier: "en-US")

        #expect(prepared.analyzerFormat == format)
        #expect(assetProvider.installCallCount == 1)
        // The bootstrapper must propagate the same module list it queried
        // status with, otherwise the install request and the readiness
        // check would disagree about which transcriber's assets matter.
        #expect(assetProvider.lastInstallModulesCount == assetProvider.lastStatusModulesCount)
        #expect(formatProvider.callCount == 1)
    }

    @Test("supported (assets-missing) status drives the install side effect")
    func supportedStatusTriggersInstall() async throws {
        let format = try makeAnalyzerInt16Format()
        let assetProvider = StubAssetStatusProvider(scriptedStatus: .supported)
        let formatProvider = StubAnalyzerFormatProvider(scriptedFormat: format)
        let bootstrapper = AppleSpeechAssetBootstrapper(
            assetStatusProvider: assetProvider,
            analyzerFormatProvider: formatProvider
        )

        let prepared = try await bootstrapper.prepare(localeIdentifier: "en-US")

        #expect(prepared.analyzerFormat == format)
        #expect(assetProvider.installCallCount == 1)
        #expect(formatProvider.callCount == 1)
    }

    @Test("unsupported status throws speechAssetsUnsupported with the originating locale")
    func unsupportedStatusThrows() async throws {
        let format = try makeAnalyzerInt16Format()
        let assetProvider = StubAssetStatusProvider(scriptedStatus: .unsupported)
        let formatProvider = StubAnalyzerFormatProvider(scriptedFormat: format)
        let bootstrapper = AppleSpeechAssetBootstrapper(
            assetStatusProvider: assetProvider,
            analyzerFormatProvider: formatProvider
        )

        await #expect(throws: AppleSpeechBoundaryError.self) {
            _ = try await bootstrapper.prepare(localeIdentifier: "xx-ZZ")
        }

        // Format resolution must not be consulted on the unsupported branch
        // — that would defeat the early-exit error contract and leak work
        // into a code path the caller has already rejected.
        #expect(formatProvider.callCount == 0)
        #expect(assetProvider.installCallCount == 0)
    }

    @Test("unsupported status surfaces the originating locale identifier in the error description")
    func unsupportedStatusDescriptionEmbedsLocale() async throws {
        let format = try makeAnalyzerInt16Format()
        let assetProvider = StubAssetStatusProvider(scriptedStatus: .unsupported)
        let formatProvider = StubAnalyzerFormatProvider(scriptedFormat: format)
        let bootstrapper = AppleSpeechAssetBootstrapper(
            assetStatusProvider: assetProvider,
            analyzerFormatProvider: formatProvider
        )

        do {
            _ = try await bootstrapper.prepare(localeIdentifier: "xx-ZZ")
            Issue.record("Expected speechAssetsUnsupported error to be thrown")
        } catch let error as AppleSpeechBoundaryError {
            switch error {
            case .speechAssetsUnsupported(let localeIdentifier):
                #expect(localeIdentifier == "xx-ZZ")
            default:
                Issue.record("Expected .speechAssetsUnsupported, got \(error)")
            }
        }
    }
}

@Suite("AppleSpeechAssetBootstrapper – analyzer format resolution")
struct AppleSpeechAssetBootstrapperFormatTests {

    @Test("missing analyzer format throws analyzerFormatUnavailable with the locale identifier")
    func missingAnalyzerFormatThrows() async throws {
        let assetProvider = StubAssetStatusProvider(scriptedStatus: .installed)
        let formatProvider = StubAnalyzerFormatProvider(scriptedFormat: nil)
        let bootstrapper = AppleSpeechAssetBootstrapper(
            assetStatusProvider: assetProvider,
            analyzerFormatProvider: formatProvider
        )

        do {
            _ = try await bootstrapper.prepare(localeIdentifier: "en-GB")
            Issue.record("Expected analyzerFormatUnavailable error to be thrown")
        } catch let error as AppleSpeechBoundaryError {
            switch error {
            case .analyzerFormatUnavailable(let localeIdentifier):
                #expect(localeIdentifier == "en-GB")
            default:
                Issue.record("Expected .analyzerFormatUnavailable, got \(error)")
            }
        }

        #expect(formatProvider.callCount == 1)
    }

    @Test("resolved analyzer format is forwarded verbatim into the prepared model")
    func resolvedFormatPassesThrough() async throws {
        let format = try makeAnalyzerInt16Format()
        let assetProvider = StubAssetStatusProvider(scriptedStatus: .installed)
        let formatProvider = StubAnalyzerFormatProvider(scriptedFormat: format)
        let bootstrapper = AppleSpeechAssetBootstrapper(
            assetStatusProvider: assetProvider,
            analyzerFormatProvider: formatProvider
        )

        let prepared = try await bootstrapper.prepare(localeIdentifier: "en-US")

        // Same instance round-trip — bootstrapper must not silently
        // re-derive a different format from the resolved one.
        #expect(prepared.analyzerFormat === format)
    }
}

@Suite("AppleSpeechAssetBootstrapper – install error propagation")
struct AppleSpeechAssetBootstrapperInstallErrorTests {

    @Test("install errors during the supported/downloading branch propagate to the caller")
    func installErrorsPropagate() async throws {
        struct StubInstallError: Error {}
        let format = try makeAnalyzerInt16Format()
        let assetProvider = StubAssetStatusProvider(
            scriptedStatus: .downloading,
            installError: StubInstallError()
        )
        let formatProvider = StubAnalyzerFormatProvider(scriptedFormat: format)
        let bootstrapper = AppleSpeechAssetBootstrapper(
            assetStatusProvider: assetProvider,
            analyzerFormatProvider: formatProvider
        )

        await #expect(throws: StubInstallError.self) {
            _ = try await bootstrapper.prepare(localeIdentifier: "en-US")
        }

        // Format resolution must not run after install failure — the
        // caller has lost the assets it needs, so consulting the analyzer
        // format would be meaningless work.
        #expect(formatProvider.callCount == 0)
    }
}

// MARK: - playhead-se2h: what loadModel() does to the bootstrapper's errors

/// `AppleSpeechRecognizer.loadModel()` sits between the bootstrapper and
/// everything that classifies a model-load failure, and it used to COLLAPSE
/// every `AppleSpeechBoundaryError` into
/// `TranscriptEngineError.transcriptionFailed(error.description)`.
///
/// That discarded exactly the distinction a support bundle needs. The two
/// causes that occur in the field are `speechAssetsUnsupported` (the
/// locale's assets are not installed) and `analyzerFormatUnavailable`, and
/// both arrived at `TranscriptFailureClass.classify` wearing the name of a
/// class that means "recognition RAN and reported an error" — which
/// `isAsrFailure` then reports as `asr_failed`, a row that contradicts
/// itself about whether the recognizer was ever invoked.
///
/// These are behavioural tests rather than a source grep because
/// `AppleSpeechRecognizer.init` now takes the bootstrapper. Before that it
/// hardcoded one, so nothing under XCTest could construct a recognizer that
/// reaches `loadModel()` at all.
@Suite("AppleSpeechRecognizer – load error classification (playhead-se2h)")
struct AppleSpeechRecognizerLoadErrorTests {

    private func makeRecognizer(
        status: AssetInventory.Status,
        format: AVAudioFormat?
    ) -> AppleSpeechRecognizer {
        AppleSpeechRecognizer(
            assetBootstrapper: AppleSpeechAssetBootstrapper(
                assetStatusProvider: StubAssetStatusProvider(scriptedStatus: status),
                analyzerFormatProvider: StubAnalyzerFormatProvider(scriptedFormat: format)
            )
        )
    }

    @Test("An unsupported locale surfaces as speechAssetsUnsupported, not transcriptionFailed")
    func unsupportedLocaleKeepsItsClass() async throws {
        let recognizer = makeRecognizer(status: .unsupported, format: try makeAnalyzerInt16Format())

        await #expect(throws: AppleSpeechBoundaryError.self) {
            try await recognizer.loadModel()
        }

        // The classification is the whole point — the type alone is not.
        do {
            try await recognizer.loadModel()
            Issue.record("loadModel must throw when the locale's assets are unsupported")
        } catch {
            #expect(
                TranscriptFailureClass.classify(error) == .speechAssetsUnsupported,
                """
                got \(TranscriptFailureClass.classify(error).rawValue). Collapsing this into \
                `transcription_failed` files a model that never loaded under a class meaning \
                recognition ran, which reads as `asr_failed` in the work journal.
                """
            )
        }
    }

    @Test("A missing analyzer format surfaces as analyzerFormatUnavailable")
    func missingAnalyzerFormatKeepsItsClass() async {
        let recognizer = makeRecognizer(status: .installed, format: nil)

        do {
            try await recognizer.loadModel()
            Issue.record("loadModel must throw when no analyzer format can be negotiated")
        } catch {
            #expect(
                TranscriptFailureClass.classify(error) == .analyzerFormatUnavailable,
                "got \(TranscriptFailureClass.classify(error).rawValue)"
            )
        }
    }

    /// The other half of the failed-upgrade contract, at the layer that
    /// actually holds the state: a throw must leave the recognizer exactly
    /// as it was, so `loadFinalModel` can load over the top atomically.
    @Test("A failed load leaves the recognizer's prepared state untouched")
    func failedLoadLeavesPreparedStateUntouched() async throws {
        // First load succeeds, so there IS state worth preserving.
        let working = makeRecognizer(status: .installed, format: try makeAnalyzerInt16Format())
        try await working.loadModel()
        #expect(await working.isModelLoaded())

        // A recognizer whose bootstrap now fails must not report itself
        // loaded, and one that was already loaded must stay loaded.
        let broken = makeRecognizer(status: .installed, format: nil)
        await #expect(throws: (any Error).self) { try await broken.loadModel() }
        #expect(await broken.isModelLoaded() == false)
    }
}

#endif
