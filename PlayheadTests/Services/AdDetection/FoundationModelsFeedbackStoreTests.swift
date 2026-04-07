// FoundationModelsFeedbackStoreTests.swift
//
// Unit tests for `FoundationModelsFeedbackStore`. The store wraps Apple's
// `LanguageModelSession.logFeedbackAttachment` Data return value (the API
// returns `Foundation.Data`, NOT a URL — the framework does not write the
// blob to disk for us). Tests use a per-test temporary directory so they
// don't pollute the real Application Support directory.

import Foundation
import Testing
@testable import Playhead

@Suite("FoundationModelsFeedbackStore")
struct FoundationModelsFeedbackStoreTests {

    private static func makeTempDirectory() -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("FMFeedbackStoreTests-\(UUID().uuidString)", isDirectory: true)
        return url
    }

    @Test("captureRefusal writes the attachment to disk and exposes its URL")
    func capturesRefusalAttachment() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = FoundationModelsFeedbackStore(directory: dir)

        let blob = Data("apple-feedback-attachment-bytes".utf8)
        await store.storeAttachment(
            blob,
            kind: .coarseRefusal,
            windowContext: "window=1_of_3"
        )

        let urls = await store.capturedAttachmentURLs()
        #expect(urls.count == 1)
        let url = try #require(urls.first)
        #expect(FileManager.default.fileExists(atPath: url.path))
        let written = try Data(contentsOf: url)
        #expect(written == blob)
        #expect(url.lastPathComponent.contains("coarse-refusal"))
        #expect(url.lastPathComponent.contains("window=1_of_3"))
    }

    @Test("clearCapturedAttachments removes all files and the in-memory list")
    func clearAttachmentsRemovesAll() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = FoundationModelsFeedbackStore(directory: dir)

        await store.storeAttachment(
            Data("first".utf8),
            kind: .coarseRefusal,
            windowContext: "window=1_of_2"
        )
        await store.storeAttachment(
            Data("second".utf8),
            kind: .refinementDecodeFailure,
            windowContext: "refineWindow=2_source=1_stage=initial"
        )

        let beforeURLs = await store.capturedAttachmentURLs()
        #expect(beforeURLs.count == 2)
        for url in beforeURLs {
            #expect(FileManager.default.fileExists(atPath: url.path))
        }

        await store.clearCapturedAttachments()

        let afterURLs = await store.capturedAttachmentURLs()
        #expect(afterURLs.isEmpty)
        for url in beforeURLs {
            #expect(!FileManager.default.fileExists(atPath: url.path))
        }
    }

    @Test("reopened store enumerates attachments that were captured before relaunch")
    func reopenedStoreEnumeratesCapturedAttachments() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }

        do {
            let store = FoundationModelsFeedbackStore(directory: dir)
            await store.storeAttachment(
                Data("first".utf8),
                kind: .coarseRefusal,
                windowContext: "window=1_of_2"
            )
            await store.storeAttachment(
                Data("second".utf8),
                kind: .refinementRefusal,
                windowContext: "refineWindow=2_source=1_stage=initial"
            )
            let firstPassURLs = await store.capturedAttachmentURLs()
            #expect(firstPassURLs.count == 2)
        }

        let reopened = FoundationModelsFeedbackStore(directory: dir)
        let urls = await reopened.capturedAttachmentURLs()
        #expect(urls.count == 2)
        #expect(urls.allSatisfy { FileManager.default.fileExists(atPath: $0.path) })
    }

    @Test("clearCapturedAttachments removes on-disk files even if one was deleted externally")
    func clearAttachmentsSkipsMissingFiles() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }

        do {
            let store = FoundationModelsFeedbackStore(directory: dir)
            await store.storeAttachment(
                Data("first".utf8),
                kind: .coarseRefusal,
                windowContext: "window=1_of_2"
            )
            await store.storeAttachment(
                Data("second".utf8),
                kind: .refinementDecodeFailure,
                windowContext: "refineWindow=2_source=1_stage=initial"
            )
        }

        let reopened = FoundationModelsFeedbackStore(directory: dir)
        let urls = await reopened.capturedAttachmentURLs()
        #expect(urls.count == 2)

        try FileManager.default.removeItem(at: urls[0])

        await reopened.clearCapturedAttachments()

        let afterURLs = await reopened.capturedAttachmentURLs()
        #expect(afterURLs.isEmpty)
        for url in urls {
            #expect(!FileManager.default.fileExists(atPath: url.path))
        }
    }

    @Test("empty data is silently skipped and never written to disk")
    func emptyDataIsSkipped() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = FoundationModelsFeedbackStore(directory: dir)

        await store.storeAttachment(
            Data(),
            kind: .coarseRefusal,
            windowContext: "window=1_of_1"
        )

        let urls = await store.capturedAttachmentURLs()
        #expect(urls.isEmpty)
    }

    @Test("concurrent captures are serialized via the actor with no crash")
    func concurrentCapturesAreThreadSafe() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let store = FoundationModelsFeedbackStore(directory: dir)

        // 32 concurrent captures from independent tasks. The actor must
        // serialize these without dropping or duplicating entries.
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<32 {
                group.addTask {
                    await store.storeAttachment(
                        Data("blob-\(index)".utf8),
                        kind: .coarseRefusal,
                        windowContext: "window=\(index)_of_32"
                    )
                }
            }
        }

        let urls = await store.capturedAttachmentURLs()
        #expect(urls.count == 32)
    }

    @Test("the three desired-output strings describe the regression for Apple")
    func desiredOutputStringsAreInformative() {
        // Pinned strings — these end up inside the Feedback Assistant
        // attachment, so changing them changes what the FoundationModels
        // team reads. Update intentionally.
        #expect(FoundationModelsFeedbackStore.coarseRefusalDesiredOutput.contains("disposition=containsAd"))
        #expect(FoundationModelsFeedbackStore.refinementDecodeFailureDesiredOutput.contains("RefinementWindowSchema"))
        #expect(FoundationModelsFeedbackStore.refinementRefusalDesiredOutput.contains("RefinementWindowSchema"))
    }
}
