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

#endif
