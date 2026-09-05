import Foundation
import Testing
@testable import Playhead

/// playhead-86sfq — Settings' Clear Cached Audio no longer unlinks files
/// behind the DownloadManager's back; it hands the clear to whatever the view
/// wired, and does nothing if nothing was.
@MainActor
@Suite("playhead-86sfq: the Settings clear goes through the manager, or not at all")
struct SettingsAudioCacheClearTests {

    private func tempAudioDir(with files: [String]) throws -> URL {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("86sfq-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        for name in files {
            try Data(repeating: 0xAA, count: 4096).write(to: dir.appendingPathComponent(name))
        }
        return dir
    }

    private func viewModel(audioDir: URL) -> SettingsViewModel {
        let vm = SettingsViewModel()
        vm.storageReporter = StorageBreakdownReporter(
            audioDirectories: [audioDir],
            transcriptDirectories: [],
            volumeProbeURL: audioDir
        )
        return vm
    }

    @Test("the wired clearer runs exactly once")
    func wiredClearerRunsOnce() async throws {
        let dir = try tempAudioDir(with: ["ep1.mp3"])
        defer { try? FileManager.default.removeItem(at: dir) }
        let vm = viewModel(audioDir: dir)
        var calls = 0
        vm.audioCacheClearer = { calls += 1 }

        await vm.clearAudioCache()

        #expect(calls == 1)
        #expect(!vm.isClearingAudioCache, "the in-flight flag must reset")
    }

    @Test("the view model itself never unlinks audio — a no-op clearer leaves the files")
    func viewModelDoesNotUnlinkBehindTheManager() async throws {
        let dir = try tempAudioDir(with: ["ep1.mp3", "ep2.mp3"])
        defer { try? FileManager.default.removeItem(at: dir) }
        let vm = viewModel(audioDir: dir)
        vm.audioCacheClearer = { }

        await vm.clearAudioCache()

        let remaining = try FileManager.default.contentsOfDirectory(atPath: dir.path)
        #expect(
            remaining.count == 2,
            """
            the view model removed audio files itself; the only thing that may \
            empty that cache is DownloadManager.clearCache(), which cancels the \
            transfers that would otherwise re-deposit bytes. Remaining: \(remaining)
            """
        )
    }

    @Test("with nothing wired, the clear does nothing rather than bypass the manager")
    func nothingWiredIsFailClosed() async throws {
        let dir = try tempAudioDir(with: ["ep1.mp3"])
        defer { try? FileManager.default.removeItem(at: dir) }
        let vm = viewModel(audioDir: dir)
        vm.audioCacheClearer = nil

        await vm.clearAudioCache()

        #expect(try FileManager.default.contentsOfDirectory(atPath: dir.path).count == 1)
        #expect(!vm.isClearingAudioCache)
    }
}
