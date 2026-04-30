// TranscriptDeepLinkRouterTests.swift
// playhead-m8v7: behaviour tests for the deep-link router that
// translates an incoming `playhead://` URL into a (resolve episode,
// play, seek) action sequence. The router is a pure value object —
// dependencies are passed as closures so the tests can stand it up
// without SwiftData / PlayheadRuntime.

import Foundation
import Testing
@testable import Playhead

@Suite("TranscriptDeepLinkRouter — incoming URL handling")
@MainActor
struct TranscriptDeepLinkRouterTests {

    /// Records the side effects the router performs on each call.
    private final class Recorder {
        var lookups: [String] = []
        var played: [String] = []
        var seeks: [TimeInterval] = []

        /// What the lookup closure should return for a given id.
        var stubResult: TranscriptDeepLinkRouter.ResolvedEpisode?
    }

    private func makeRouter(_ recorder: Recorder) -> TranscriptDeepLinkRouter {
        TranscriptDeepLinkRouter(
            resolveEpisode: { episodeId in
                recorder.lookups.append(episodeId)
                return recorder.stubResult
            },
            playEpisode: { resolved in
                recorder.played.append(resolved.episodeId)
            },
            seek: { time in
                recorder.seeks.append(time)
            }
        )
    }

    // MARK: - Happy path

    @Test("Valid URL → lookup, play, seek in order")
    func happyPathDispatch() async {
        let recorder = Recorder()
        recorder.stubResult = .init(episodeId: "ep-42")
        let router = makeRouter(recorder)

        let url = TranscriptDeepLink.url(episodeId: "ep-42", startTime: 765)
        let handled = await router.handle(url: url)

        #expect(handled == true)
        #expect(recorder.lookups == ["ep-42"])
        #expect(recorder.played == ["ep-42"])
        #expect(recorder.seeks == [765])
    }

    // MARK: - Invalid / unknown

    @Test("Wrong-scheme URL → not handled, no side effects")
    func wrongSchemeIgnored() async {
        let recorder = Recorder()
        recorder.stubResult = .init(episodeId: "ep-42")
        let router = makeRouter(recorder)

        let url = URL(string: "https://example.com/?t=10")!
        let handled = await router.handle(url: url)

        #expect(handled == false)
        #expect(recorder.lookups.isEmpty)
        #expect(recorder.played.isEmpty)
        #expect(recorder.seeks.isEmpty)
    }

    @Test("Unknown episode id → not handled, no play/seek dispatched")
    func unknownEpisode() async {
        let recorder = Recorder()
        recorder.stubResult = nil
        let router = makeRouter(recorder)

        let url = TranscriptDeepLink.url(episodeId: "ep-missing", startTime: 30)
        let handled = await router.handle(url: url)

        #expect(handled == false)
        #expect(recorder.lookups == ["ep-missing"])
        #expect(recorder.played.isEmpty)
        #expect(recorder.seeks.isEmpty)
    }

    @Test("URL with t=0 still seeks to 0")
    func zeroTimeSeek() async {
        let recorder = Recorder()
        recorder.stubResult = .init(episodeId: "ep-42")
        let router = makeRouter(recorder)

        let url = URL(string: "playhead://episode/ep-42?t=0")!
        let handled = await router.handle(url: url)

        #expect(handled == true)
        #expect(recorder.seeks == [0])
    }

    @Test("URL without t= still plays and seeks to 0")
    func missingTimeDefaultsToZero() async {
        let recorder = Recorder()
        recorder.stubResult = .init(episodeId: "ep-42")
        let router = makeRouter(recorder)

        let url = URL(string: "playhead://episode/ep-42")!
        let handled = await router.handle(url: url)

        #expect(handled == true)
        #expect(recorder.played == ["ep-42"])
        #expect(recorder.seeks == [0])
    }
}
