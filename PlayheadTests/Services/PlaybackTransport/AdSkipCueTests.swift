// AdSkipCueTests.swift
// playhead-nqwr: the two halves of the cue that have nothing to do with the
// transport — WHAT it is made of, and WHEN a second request is admitted.
//
// Neither needs an audio device, and that is the point of the split: the
// `AVAudioPlayer` is the part the simulator cannot verify anyway, while the
// re-trigger policy and the ON-by-default read are the parts that can be
// silently wrong on a real device for months.

@preconcurrency import AVFoundation
import Foundation
import Testing
@testable import Playhead

// MARK: - The placeholder sound

@Suite("AdSkipCue – the sound")
struct AdSkipCueSoundTests {

    /// Little-endian integer read, so the WAV assertions below name the field
    /// they are checking instead of an offset arithmetic expression.
    private func uint32(_ data: Data, at offset: Int) -> UInt32 {
        var value: UInt32 = 0
        for index in (0..<4).reversed() {
            value = (value << 8) | UInt32(data[offset + index])
        }
        return value
    }

    private func uint16(_ data: Data, at offset: Int) -> UInt16 {
        UInt16(data[offset]) | (UInt16(data[offset + 1]) << 8)
    }

    private func ascii(_ data: Data, at offset: Int) -> String {
        String(decoding: data[offset..<(offset + 4)], as: UTF8.self)
    }

    @Test("The placeholder is a well-formed 16-bit mono PCM WAV")
    func placeholderIsWellFormedWAV() {
        let data = AdSkipCueSound.placeholderToneWAVData()

        #expect(ascii(data, at: 0) == "RIFF")
        #expect(ascii(data, at: 8) == "WAVE")
        #expect(ascii(data, at: 12) == "fmt ")
        #expect(uint32(data, at: 16) == 16, "PCM fmt chunks are 16 bytes")
        #expect(uint16(data, at: 20) == 1, "format 1 == uncompressed PCM")
        #expect(uint16(data, at: 22) == 1, "the cue is MONO — see the asset spec")
        #expect(uint32(data, at: 24) == UInt32(AdSkipCueSound.placeholderSampleRate))
        #expect(uint16(data, at: 32) == 2, "block align = 1 channel * 16 bits")
        #expect(uint16(data, at: 34) == 16)
        #expect(ascii(data, at: 36) == "data")

        // The two length fields must agree with the payload, or a decoder
        // reads past the end or stops short.
        let declaredPayload = Int(uint32(data, at: 40))
        #expect(declaredPayload == data.count - 44)
        #expect(Int(uint32(data, at: 4)) == data.count - 8)
    }

    @Test("The placeholder is the declared length, at the declared rate")
    func placeholderHasDeclaredDuration() {
        let data = AdSkipCueSound.placeholderToneWAVData()
        let frames = (data.count - 44) / 2
        let seconds = Double(frames) / Double(AdSkipCueSound.placeholderSampleRate)
        #expect(abs(seconds - AdSkipCueSound.placeholderDurationSeconds) < 0.001)
    }

    /// The bound the whole "several skips land close together" argument rests
    /// on: a cue may never be longer than the window in which a second request
    /// is refused, or a cue can talk over itself.
    @Test("A cue can never outlive the window that refuses the next one")
    func placeholderFitsInsideRetriggerWindow() {
        #expect(
            Duration.seconds(AdSkipCueSound.placeholderDurationSeconds)
                <= AdSkipCueSound.retriggerWindow,
            """
            AdSkipCueSound.retriggerWindow is the asset-length bound quoted in \
            the drop-in instructions. A sound longer than it can overlap itself.
            """
        )
    }

    /// A hard cut to a non-zero sample reads as a glitch — which is the exact
    /// experience the cue exists to distinguish itself from — and a hard START
    /// clicks. Both ends are checked because the envelope has two halves and a
    /// mutation can remove either.
    @Test("The placeholder starts from silence and decays into it")
    func placeholderEnvelopeIsClosedAtBothEnds() throws {
        let data = AdSkipCueSound.placeholderToneWAVData()
        let payload = data.dropFirst(44)
        func sample(_ frame: Int) -> Int16 {
            let base = payload.startIndex + frame * 2
            let raw = UInt16(payload[base]) | (UInt16(payload[base + 1]) << 8)
            return Int16(bitPattern: raw)
        }
        let frames = payload.count / 2
        #expect(sample(0) == 0, "the attack ramp must start at digital silence")
        #expect(
            abs(Int(sample(frames - 1))) <= 1,
            """
            The buffer must END at digital silence. An exponential decay alone \
            leaves a step (measured at 112/32767 before the release ramp was \
            added), and a step is a click — the exact "glitch" reading this \
            cue exists to be the opposite of.
            """
        )
        // And it must actually make a sound in between — an all-zero buffer
        // would satisfy both assertions above.
        let peak = (0..<frames).map { abs(Int(sample($0))) }.max() ?? 0
        #expect(peak > 2_000, "the placeholder must be audible, peak=\(peak)")

        // `sample(0) == 0` alone does NOT prove there is an attack ramp: both
        // partials are sines, so the buffer starts at zero whether or not one
        // exists. What an attack ramp actually claims is that the first
        // millisecond is quiet RELATIVE to the body — so that is what is
        // measured. Without the ramp the signal reaches full amplitude inside
        // one period of the fundamental (~56 samples at 784 Hz / 44.1 kHz).
        let earlyPeak = (0..<60).map { abs(Int(sample($0))) }.max() ?? 0
        #expect(
            earlyPeak * 3 < peak,
            """
            The attack must RAMP: the first 60 samples peaked at \(earlyPeak) \
            against a body peak of \(peak). A sine that opens at full \
            amplitude has no step in value but a step in slope, and that is a \
            click.
            """
        )
    }

    @Test("AVAudioPlayer can decode the placeholder")
    func placeholderIsDecodable() throws {
        let player = try AVAudioPlayer(data: AdSkipCueSound.placeholderToneWAVData())
        #expect(
            abs(player.duration - AdSkipCueSound.placeholderDurationSeconds) < 0.01
        )
        #expect(player.numberOfChannels == 1)
    }

    // MARK: The drop-in contract

    @Test("With no asset bundled, the sound resolves to the placeholder")
    func resolvesToPlaceholderWithoutAsset() throws {
        let empty = try Self.makeBundle(containing: [:])
        defer { try? FileManager.default.removeItem(at: empty.url) }
        #expect(AdSkipCueSound.bundledAssetURL(in: empty.bundle) == nil)
        #expect(AdSkipCueSound.resolve(in: empty.bundle).source == .placeholderTone)
    }

    /// This is the test that makes the instructions in `AdSkipCueSound.swift`
    /// a promise rather than a hope: drop `AdSkipCue.<ext>` into
    /// `Playhead/Resources` and the placeholder stops being reachable, with no
    /// code change at all.
    @Test("A dropped-in asset wins, for every extension the header advertises",
          arguments: AdSkipCueSound.resourceExtensions)
    func droppedInAssetWins(ext: String) throws {
        let name = "\(AdSkipCueSound.resourceBaseName).\(ext)"
        let made = try Self.makeBundle(
            containing: [name: AdSkipCueSound.placeholderToneWAVData()]
        )
        defer { try? FileManager.default.removeItem(at: made.url) }

        let resolved = AdSkipCueSound.resolve(in: made.bundle)
        guard case .bundledAsset(let url) = resolved.source else {
            Issue.record("expected the bundled asset to win for .\(ext)")
            return
        }
        #expect(url.lastPathComponent == name)
    }

    /// Order matters: PCM containers are preferred so nothing has to spin up a
    /// decoder at a seam that has already spent 150 ms ducked.
    @Test("Extension precedence is the order the header advertises")
    func extensionPrecedenceIsDeclared() throws {
        let made = try Self.makeBundle(containing: [
            "\(AdSkipCueSound.resourceBaseName).m4a": Data([0]),
            "\(AdSkipCueSound.resourceBaseName).caf":
                AdSkipCueSound.placeholderToneWAVData()
        ])
        defer { try? FileManager.default.removeItem(at: made.url) }

        let url = try #require(AdSkipCueSound.bundledAssetURL(in: made.bundle))
        #expect(url.pathExtension == "caf", "PCM must be preferred over AAC")
        #expect(AdSkipCueSound.resourceExtensions.first == "caf")
    }

    /// A corrupt drop-in must degrade to silence. It must NOT crash, and — the
    /// half that matters for this bead — it must not fall back to reconfiguring
    /// anything: `makePreparedPlayer` simply answers `nil`.
    @Test("An undecodable asset yields no player rather than a crash")
    func undecodableAssetYieldsNoPlayer() throws {
        let made = try Self.makeBundle(containing: [
            "\(AdSkipCueSound.resourceBaseName).wav": Data("not audio".utf8)
        ])
        defer { try? FileManager.default.removeItem(at: made.url) }
        let sound = AdSkipCueSound.resolve(in: made.bundle)
        #expect(sound.makePreparedPlayer() == nil)
    }

    @Test("The placeholder produces a prepared player at the declared level")
    func placeholderProducesPreparedPlayer() throws {
        let player = try #require(
            AdSkipCueSound(source: .placeholderTone).makePreparedPlayer()
        )
        #expect(player.volume == AdSkipCueSound.level)
        #expect(player.numberOfLoops == 0, "a cue that loops is an alarm")
        // Not a taste threshold — the LEVEL is Dan's. This is the mechanism
        // claim underneath it: the cue is authored quiet because it plays
        // under a show that has just been ducked and is about to come back,
        // and it is deliberately NOT ducked with the episode (that is the
        // point of a separate player). Shipping it at full scale makes the
        // acknowledgement louder than the thing it acknowledges.
        #expect(AdSkipCueSound.level < 1.0,
                "observed \(AdSkipCueSound.level)")
    }

    // MARK: Helpers

    private static func makeBundle(
        containing files: [String: Data]
    ) throws -> (url: URL, bundle: Bundle) {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("nqwr-\(UUID().uuidString).bundle")
        try FileManager.default.createDirectory(
            at: url, withIntermediateDirectories: true
        )
        for (name, data) in files {
            try data.write(to: url.appendingPathComponent(name))
        }
        guard let bundle = Bundle(url: url) else {
            throw CocoaError(.fileNoSuchFile)
        }
        return (url, bundle)
    }
}

// MARK: - The switch

@Suite("AdSkipCue – the listener's switch")
struct AdSkipCueSettingsTests {

    private func isolatedDefaults() throws -> UserDefaults {
        let suite = "nqwr-settings-\(UUID().uuidString)"
        return try #require(UserDefaults(suiteName: suite))
    }

    /// Default #3, and the reason `isEnabled` reads `object(forKey:)`.
    /// `UserDefaults.bool(forKey:)` answers `false` for a key nobody has
    /// written, which would ship an ON-by-default flag OFF for every listener
    /// who never opens Settings — indistinguishable, on a device, from the
    /// silence this bead exists to remove.
    @Test("An untouched install has the cue ON")
    func defaultsToOn() throws {
        let defaults = try isolatedDefaults()
        #expect(defaults.object(forKey: AdSkipCueSettings.userDefaultsKey) == nil)
        #expect(defaults.bool(forKey: AdSkipCueSettings.userDefaultsKey) == false,
                "the trap this read avoids: bool(forKey:) is false for an absent key")
        #expect(AdSkipCueSettings.isEnabled(defaults))
        #expect(AdSkipCueSettings.defaultValue)
    }

    @Test("The switch round-trips in both directions")
    func roundTrips() throws {
        let defaults = try isolatedDefaults()
        AdSkipCueSettings.setEnabled(false, in: defaults)
        #expect(!AdSkipCueSettings.isEnabled(defaults))
        AdSkipCueSettings.setEnabled(true, in: defaults)
        #expect(AdSkipCueSettings.isEnabled(defaults))
    }
}

// MARK: - The re-trigger policy

@Suite("AdSkipCue – two skips on top of each other")
struct AdSkipCuePlayerTests {

    /// A hand-driven clock plus counters. `AdSkipCuePlayer`'s whole policy is
    /// expressible without an audio device, so it is measured without one.
    private final class Harness: @unchecked Sendable {
        private let lock = NSLock()
        private var _now = ContinuousClock.now
        private var _starts = 0
        private var _stops = 0

        var starts: Int { lock.lock(); defer { lock.unlock() }; return _starts }
        var stops: Int { lock.lock(); defer { lock.unlock() }; return _stops }

        func advance(by duration: Duration) {
            lock.lock(); defer { lock.unlock() }
            _now = _now.advanced(by: duration)
        }

        func makePlayer(
            window: Duration = AdSkipCueSound.retriggerWindow
        ) -> AdSkipCuePlayer {
            AdSkipCuePlayer(
                retriggerWindow: window,
                now: { [self] in lock.lock(); defer { lock.unlock() }; return _now },
                startSound: { [self] in lock.lock(); defer { lock.unlock() }; _starts += 1 },
                stopSound: { [self] in lock.lock(); defer { lock.unlock() }; _stops += 1 }
            )
        }
    }

    @Test("One skip sounds one cue")
    func oneSkipOneCue() {
        let harness = Harness()
        harness.makePlayer().playAdSkipCue()
        #expect(harness.starts == 1)
    }

    @Test("A second skip INSIDE the window is dropped, not overlapped")
    func secondSkipInsideWindowDropped() {
        let harness = Harness()
        let player = harness.makePlayer()
        player.playAdSkipCue()
        harness.advance(by: .milliseconds(100))
        player.playAdSkipCue()
        harness.advance(by: .milliseconds(100))
        player.playAdSkipCue()
        #expect(harness.starts == 1,
                "adjacent cuts are one break to the listener; one acknowledgement")
    }

    @Test("A skip after the window sounds again")
    func skipAfterWindowSoundsAgain() {
        let harness = Harness()
        let player = harness.makePlayer()
        player.playAdSkipCue()
        harness.advance(by: AdSkipCueSound.retriggerWindow)
        player.playAdSkipCue()
        #expect(harness.starts == 2)
    }

    /// The boundary is half-open on the same side as every other interval in
    /// this repo: a request AT the re-admit instant is admitted.
    @Test("The window is half-open: the boundary instant is admitted")
    func boundaryIsAdmitted() {
        let harness = Harness()
        let player = harness.makePlayer(window: .milliseconds(600))
        player.playAdSkipCue()
        harness.advance(by: .milliseconds(599))
        player.playAdSkipCue()
        #expect(harness.starts == 1)
        harness.advance(by: .milliseconds(1))
        player.playAdSkipCue()
        #expect(harness.starts == 2)
    }

    @Test("Stopping clears the window, so the next skip is heard")
    func stoppingClearsTheWindow() {
        let harness = Harness()
        let player = harness.makePlayer()
        player.playAdSkipCue()
        #expect(harness.starts == 1)
        player.stopAdSkipCue()
        #expect(harness.stops == 1)
        player.playAdSkipCue()
        #expect(harness.starts == 2,
                "a cue that was silenced is not a cue that is still sounding")
    }

    /// The production wiring must never do audio work on the caller. This is
    /// the only assertion that touches the real factory, and it deliberately
    /// asserts about TIME rather than about the sound: `PlaybackService.init`
    /// is on the launch path, and playhead-xul6 is what a synchronous
    /// framework read there costs.
    @Test("The production factory does no audio work on the caller")
    func productionFactoryIsCheapOnTheCaller() {
        let clock = ContinuousClock()
        let elapsed = clock.measure {
            _ = AdSkipCuePlayer.system()
        }
        #expect(elapsed < .milliseconds(50), "constructed in \(elapsed)")
    }
}
