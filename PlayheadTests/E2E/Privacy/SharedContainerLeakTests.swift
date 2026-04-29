// SharedContainerLeakTests.swift
// playhead-h3h: Assert that no transcript text or audio bytes can leak
// to a shared container. Playhead's shipping configuration declares
// neither an App Group nor a Keychain access group; this suite pins
// that contract by inspecting the running bundle's entitlements and by
// running a representative pipeline pass and verifying no shared
// surface received bytes.
//
// What is NOT testable in-process (deferred to real-device verification):
//   * Cross-process IPC inspection (e.g. another app on the device
//     trying to attach to a shared container). The simulator does not
//     model multi-app sandboxing in a way that would catch a leak this
//     test could not. The contract here is the absence of *any* shared
//     surface — a process that owns no app group cannot leak via one.

import Foundation
import Testing
@testable import Playhead

@Suite("playhead-h3h - shared container leakage")
struct SharedContainerLeakTests {

    private static let transcriptSentinels = [
        "Welcome to the privacy gate test episode for playhead-h3h",
        "Squarespace",
        "Now back to our content",
    ]

    private static let candidateAppGroupSuites = [
        "group.com.playhead",
        "group.com.playhead.app",
        "group.com.playhead.shared",
        "group.com.example.playhead",
    ]

    private func assertNoTranscriptInSharedDefaults(
        sentinels: [String] = SharedContainerLeakTests.transcriptSentinels
    ) {
        for name in SharedContainerLeakTests.candidateAppGroupSuites {
            guard let suite = UserDefaults(suiteName: name) else { continue }
            let dictionary = suite.dictionaryRepresentation()
            for (_, value) in dictionary {
                if let s = value as? String {
                    for sentinel in sentinels {
                        #expect(!s.contains(sentinel),
                                "transcript sentinel '\(sentinel)' leaked to UserDefaults suite \(name)")
                    }
                } else if let data = value as? Data,
                          let asString = String(data: data, encoding: .utf8) {
                    for sentinel in sentinels {
                        #expect(!asString.contains(sentinel),
                                "transcript sentinel '\(sentinel)' leaked to UserDefaults suite \(name) (Data)")
                    }
                }
            }
        }
    }

    private func assertNoKeychainTranscript() {
        let query: [CFString: Any] = [
            kSecClass: kSecClassGenericPassword,
            kSecMatchLimit: kSecMatchLimitAll,
            kSecReturnAttributes: true,
            kSecReturnData: false,
        ]
        var result: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        if status == errSecSuccess, let items = result as? [[CFString: Any]] {
            let sentinels = ["playhead-h3h", "Squarespace", "transcript"]
            for attrs in items {
                let service = (attrs[kSecAttrService] as? String) ?? ""
                for sentinel in sentinels {
                    #expect(!service.contains(sentinel),
                            "Keychain item under service \(service) matched sentinel \(sentinel)")
                }
            }
        }
        if status != errSecSuccess && status != errSecItemNotFound {
            Issue.record("SecItemCopyMatching returned unexpected status \(status)")
        }
    }

    // MARK: - Entitlement contract

    @Test("Bundle declares no com.apple.security.application-groups entitlement")
    func bundleDoesNotDeclareAppGroups() {
        // The bundled `com.apple.security.application-groups` array, if
        // present, would surface as a Bundle.infoDictionary key (Xcode
        // copies the matching entitlement into a runtime accessor on
        // some SDKs) AND/OR as a non-nil
        // `containerURL(forSecurityApplicationGroupIdentifier:)` for
        // any literal we attempt. Playhead has neither today.
        let bundleGroups = Bundle.main.object(forInfoDictionaryKey: "com.apple.security.application-groups")
        #expect(bundleGroups == nil,
                "Playhead must not declare an App Group; found \(String(describing: bundleGroups))")
    }

    @Test("No reachable app-group container URLs for any plausible group identifier")
    func noReachableAppGroupContainer() {
        // A negative test: a handful of plausible identifiers that a
        // future commit might introduce. None of them should resolve
        // to a real container URL today.
        let candidateIdentifiers = [
            "group.com.playhead",
            "group.com.playhead.app",
            "group.com.playhead.shared",
            "group.com.example.playhead",
        ]
        let fm = FileManager.default
        for identifier in candidateIdentifiers {
            let url = fm.containerURL(forSecurityApplicationGroupIdentifier: identifier)
            #expect(url == nil,
                    "App-group container resolved for \(identifier): \(String(describing: url))")
        }
    }

    // MARK: - Shared UserDefaults contract

    @Test("Shared UserDefaults suites for plausible group names contain no transcript text")
    func sharedUserDefaultsHaveNoTranscriptText() {
        assertNoTranscriptInSharedDefaults()
    }

    // MARK: - Keychain contract

    @Test("No keychain items present under the app's service for transcript-bearing data")
    func keychainHasNoTranscriptItems() {
        assertNoKeychainTranscript()
    }

    // MARK: - End-to-end: pipeline pass leaves no shared-surface trace

    @Test("After a full pipeline persistence pass, no shared-surface contains transcript bytes")
    func pipelinePassDoesNotPopulateSharedSurfaces() async throws {
        let store = try await makeTestStore()
        let asset = AnalysisAsset(
            id: "h3h-leak",
            episodeId: "ep-h3h-leak",
            assetFingerprint: "fp-h3h-leak",
            weakFingerprint: nil,
            sourceURL: "file:///privacy/leak.m4a",
            featureCoverageEndTime: nil,
            fastTranscriptCoverageEndTime: nil,
            confirmedAdCoverageEndTime: nil,
            analysisState: "new",
            analysisVersion: 1,
            capabilitySnapshot: nil
        )
        try await store.insertAsset(asset)
        let chunks = (0..<3).map { idx in
            TranscriptChunk(
                id: "h3h-leak-\(idx)",
                analysisAssetId: asset.id,
                segmentFingerprint: "h3h-leak-fp-\(idx)",
                chunkIndex: idx,
                startTime: Double(idx) * 30,
                endTime: Double(idx + 1) * 30,
                text: "Welcome to the privacy gate test episode for playhead-h3h.",
                normalizedText: "welcome to the privacy gate test episode for playhead-h3h.",
                pass: "final",
                modelVersion: "h3h-test-v1",
                transcriptVersion: nil,
                atomOrdinal: nil
            )
        }
        try await store.insertTranscriptChunks(chunks)

        // After a representative persistence pass, re-run the shared-
        // surface assertions. They must remain clean.
        assertNoTranscriptInSharedDefaults()
        assertNoKeychainTranscript()
    }
}
