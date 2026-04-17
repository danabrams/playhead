// FixtureManifestIntegrityTests.swift
// Integrity gate for the device-lab fixture substrate defined in bead
// playhead-ym57. Validates that:
//   1. Every manifest entry's file exists on disk.
//   2. Every file's SHA-256 matches the manifest.
//   3. Every locked-core slot 1-8 is present.
//   4. Every referenced licensingRef anchor resolves in FIXTURES_LICENSING.md.
//
// This test lives in the PlayheadFastTests plan and MUST pass without any
// external downloads or legal sign-off. The locked-core 8 fixtures are
// synthetic byte-deterministic placeholders produced by
// `SyntheticFixtureGenerator.swift`; see FIXTURES_LICENSING.md for the policy.

import CryptoKit
import Foundation
import Testing
@testable import Playhead

// MARK: - Path Anchors
//
// Tests are loaded from the PlayheadTests bundle but the fixture bytes and
// manifest JSON live at canonical on-disk paths inside the repo. `#filePath`
// anchors us to this source file's location:
//     <repo>/PlayheadTests/Fixtures/FixtureManifestIntegrityTests.swift
// From there we can resolve the Corpus/ directory and repo-root paths.

private enum FixturePaths {

    static func thisFile(_ filePath: String = #filePath) -> URL {
        URL(fileURLWithPath: filePath)
    }

    static func corpusDir(_ filePath: String = #filePath) -> URL {
        thisFile(filePath)
            .deletingLastPathComponent() // PlayheadTests/Fixtures
            .appendingPathComponent("Corpus", isDirectory: true)
    }

    static func manifestURL(_ filePath: String = #filePath) -> URL {
        corpusDir(filePath).appendingPathComponent("fixtures-manifest.json")
    }

    static func mediaDir(_ filePath: String = #filePath) -> URL {
        corpusDir(filePath).appendingPathComponent("Media", isDirectory: true)
    }

    static func rotationSeedURL(_ filePath: String = #filePath) -> URL {
        corpusDir(filePath).appendingPathComponent("fixtures-rotation-seed.txt")
    }

    static func licensingDocURL(_ filePath: String = #filePath) -> URL {
        corpusDir(filePath).appendingPathComponent("FIXTURES_LICENSING.md")
    }
}

// MARK: - Integrity Suite

@Suite("FixtureManifest - Integrity (playhead-ym57)")
struct FixtureManifestIntegrityTests {

    // MARK: 1. Manifest schema loads cleanly

    @Test("fixtures-manifest.json exists and decodes")
    func manifestDecodes() throws {
        let data = try Data(contentsOf: FixturePaths.manifestURL())
        let manifest = try JSONDecoder().decode(FixtureManifest.self, from: data)

        #expect(manifest.version == 1, "Manifest schema version pinned to 1")
        #expect(!manifest.fixtures.isEmpty, "Manifest must list at least the locked-core 8")
    }

    // MARK: 2. Every manifest entry's file exists on disk

    @Test("Every manifest fixture file exists on disk")
    func allFixtureFilesExist() throws {
        let manifest = try Self.loadManifest()
        let corpusRoot = FixturePaths.corpusDir()

        for fixture in manifest.fixtures {
            let fileURL = corpusRoot.appendingPathComponent(fixture.file)
            #expect(
                FileManager.default.fileExists(atPath: fileURL.path),
                "Missing fixture file for \(fixture.id): \(fixture.file)"
            )
        }
    }

    // MARK: 3. SHA-256 of each file matches manifest

    @Test("Every fixture file's SHA-256 matches the manifest")
    func sha256Matches() throws {
        let manifest = try Self.loadManifest()
        let corpusRoot = FixturePaths.corpusDir()

        for fixture in manifest.fixtures {
            let fileURL = corpusRoot.appendingPathComponent(fixture.file)
            let data = try Data(contentsOf: fileURL)
            let hash = SHA256.hash(data: data)
            let hex = hash.map { String(format: "%02x", $0) }.joined()

            #expect(
                hex == fixture.sha256.lowercased(),
                "SHA-256 mismatch for \(fixture.id): expected \(fixture.sha256), got \(hex)"
            )
        }
    }

    // MARK: 4. Locked-core slots 1..8 all present exactly once

    @Test("Locked-core slots 1..8 all present exactly once")
    func lockedCoreSlotsPresent() throws {
        let manifest = try Self.loadManifest()
        let lockedSlots = manifest.fixtures.filter(\.locked).map(\.slot)

        #expect(lockedSlots.count == 8, "Locked-core must have exactly 8 fixtures, got \(lockedSlots.count)")

        let slotSet = Set(lockedSlots)
        #expect(slotSet == Set(1...8), "Locked-core slots must be {1,2,3,4,5,6,7,8}, got \(slotSet.sorted())")
    }

    // MARK: 5. Licensing anchor resolves in FIXTURES_LICENSING.md

    @Test("Every licensingRef anchor resolves in FIXTURES_LICENSING.md")
    func licensingRefsResolve() throws {
        let manifest = try Self.loadManifest()
        let licensingText = try String(contentsOf: FixturePaths.licensingDocURL(), encoding: .utf8)

        for fixture in manifest.fixtures {
            // licensingRef is of form "LICENSING.md#fixture-01" or similar;
            // we pull the fragment (after '#') and require it to appear as a
            // markdown anchor in the licensing doc. Markdown auto-anchors lower
            // the heading: "### Fixture-01" -> "#fixture-01". We search for the
            // fragment both as an explicit anchor marker and as a heading row.
            let ref = fixture.licensingRef
            guard let hashIdx = ref.firstIndex(of: "#") else {
                Issue.record("licensingRef for \(fixture.id) missing '#fragment': \(ref)")
                continue
            }
            let fragment = String(ref[ref.index(after: hashIdx)...])
            #expect(!fragment.isEmpty, "Empty licensingRef fragment for \(fixture.id)")

            let explicitAnchor = "<a id=\"\(fragment)\"></a>"
            let headingPattern = "\n#"  // any markdown heading line
            let hasExplicitAnchor = licensingText.contains(explicitAnchor)
            // Fallback: a heading whose generated slug equals fragment. We do a
            // conservative check: a line like "### fixture-01" (case-insensitive).
            let hasHeadingMatch = licensingText
                .lowercased()
                .split(separator: "\n")
                .contains { line in
                    line.starts(with: "#") && line.contains(fragment.lowercased())
                }
            _ = headingPattern

            #expect(
                hasExplicitAnchor || hasHeadingMatch,
                "FIXTURES_LICENSING.md missing anchor for \(fixture.id) (expected fragment: \(fragment))"
            )
        }
    }

    // MARK: 6. Rotation seed file exists

    @Test("fixtures-rotation-seed.txt exists and is non-empty")
    func rotationSeedExists() throws {
        let seedText = try String(contentsOf: FixturePaths.rotationSeedURL(), encoding: .utf8)
        let trimmed = seedText.trimmingCharacters(in: .whitespacesAndNewlines)
        #expect(!trimmed.isEmpty, "fixtures-rotation-seed.txt must contain a seed value")
    }

    // MARK: - Helpers

    private static func loadManifest() throws -> FixtureManifest {
        let data = try Data(contentsOf: FixturePaths.manifestURL())
        return try JSONDecoder().decode(FixtureManifest.self, from: data)
    }
}
