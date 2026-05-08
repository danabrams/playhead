// ChapterPlanCacheTests.swift
// playhead-au2v.1.1: Tests for `ChapterPlanCache` (get/put/invalidate,
// schema-version mismatch, decode-failure tolerance, concurrent access).
//
// Tests use a per-test temporary directory so they don't pollute the real
// `Application Support/ChapterPlanCache/` location and so concurrent test
// runs are isolated.

import Foundation
import Testing
@testable import Playhead

@Suite("ChapterPlanCache")
struct ChapterPlanCacheTests {

    private static func makeTempDirectory() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent(
                "ChapterPlanCacheTests-\(UUID().uuidString)",
                isDirectory: true
            )
    }

    private static func makePlan(
        contentHash: String,
        chapterCount: Int = 2,
        schemaVersion: Int = ChapterPlan.currentSchemaVersion
    ) -> ChapterPlan {
        var chapters: [ChapterEvidence] = []
        var t: TimeInterval = 0
        for index in 0..<chapterCount {
            chapters.append(ChapterEvidence(
                startTime: t,
                endTime: t + 60,
                title: "ch-\(index)",
                source: .inferred,
                disposition: .ambiguous,
                qualityScore: 0.5
            ))
            t += 60
        }
        return ChapterPlan(
            episodeContentHash: contentHash,
            chapters: chapters,
            planConfidence: 0.5,
            generatedAt: Date(timeIntervalSince1970: 1_700_000_000),
            schemaVersion: schemaVersion,
            generationDiagnostics: ChapterPlanDiagnostics(
                candidatesDetected: chapterCount,
                candidatesKept: chapterCount
            )
        )
    }

    // MARK: get / put / invalidate

    @Test("put then get round-trips an identical plan")
    func putGetRoundTrip() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        let plan = Self.makePlan(contentHash: "hash-A")
        let stored = await cache.put(contentHash: "hash-A", plan: plan)
        #expect(stored == true)

        let fetched = await cache.get(contentHash: "hash-A")
        #expect(fetched == plan)
    }

    @Test("get returns nil for an unknown hash")
    func getMissReturnsNil() async {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        let fetched = await cache.get(contentHash: "never-stored")
        #expect(fetched == nil)
    }

    @Test("invalidate removes a stored plan")
    func invalidateRemovesPlan() async {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        let plan = Self.makePlan(contentHash: "hash-B")
        _ = await cache.put(contentHash: "hash-B", plan: plan)
        #expect(await cache.get(contentHash: "hash-B") != nil)

        await cache.invalidate(contentHash: "hash-B")
        #expect(await cache.get(contentHash: "hash-B") == nil)
    }

    @Test("invalidate on a missing key is a silent no-op")
    func invalidateMissingIsNoOp() async {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        await cache.invalidate(contentHash: "absent")
        #expect(await cache.get(contentHash: "absent") == nil)
    }

    // MARK: schema-version mismatch

    @Test("schema-version mismatch on read is treated as a cache miss")
    func schemaMismatchIsCacheMiss() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        // Write a plan whose serialized schema version is *older* than
        // the current one. Without going through `put`, we want to
        // simulate "old version wrote this file, version got bumped,
        // now we're reading it back."
        let stalePlan = Self.makePlan(
            contentHash: "hash-stale",
            schemaVersion: ChapterPlan.currentSchemaVersion - 1
        )
        let stored = await cache.put(contentHash: "hash-stale", plan: stalePlan)
        #expect(stored == true)

        let fetched = await cache.get(contentHash: "hash-stale")
        #expect(fetched == nil)
    }

    // MARK: decode-failure tolerance

    @Test("a corrupted file on disk decodes to a cache miss without crashing")
    func corruptedFileIsCacheMiss() async throws {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        // Force directory creation, then write garbage at the
        // expected file URL for "hash-garbled".
        await cache.migrate()
        let resolvedDir = await cache.directory
        let filename = ChapterPlanCache.filename(forContentHash: "hash-garbled")
        let path = resolvedDir.appendingPathComponent(filename, isDirectory: false)
        try Data("not valid json {{{".utf8).write(to: path)

        let fetched = await cache.get(contentHash: "hash-garbled")
        #expect(fetched == nil)
    }

    // MARK: contentHash / plan key mismatch guard

    @Test("put refuses to store a plan whose embedded hash disagrees with the key")
    func putRefusesKeyMismatch() async {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        let plan = Self.makePlan(contentHash: "embedded")
        let stored = await cache.put(contentHash: "different-key", plan: plan)
        #expect(stored == false)

        // Neither key produced a file.
        #expect(await cache.get(contentHash: "embedded") == nil)
        #expect(await cache.get(contentHash: "different-key") == nil)
    }

    // MARK: re-open / persistence across instances

    @Test("a plan survives a fresh cache instance pointing at the same directory")
    func persistsAcrossInstances() async {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }

        do {
            let cache = ChapterPlanCache(directory: dir)
            _ = await cache.put(
                contentHash: "hash-persist",
                plan: Self.makePlan(contentHash: "hash-persist")
            )
        }
        let reopened = ChapterPlanCache(directory: dir)
        let fetched = await reopened.get(contentHash: "hash-persist")
        #expect(fetched != nil)
        #expect(fetched?.episodeContentHash == "hash-persist")
    }

    // MARK: concurrency

    @Test("concurrent puts across distinct keys all land safely")
    func concurrentPutsDistinctKeys() async {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        await withTaskGroup(of: Void.self) { group in
            for index in 0..<32 {
                group.addTask {
                    let hash = "hash-concurrent-\(index)"
                    _ = await cache.put(
                        contentHash: hash,
                        plan: Self.makePlan(contentHash: hash)
                    )
                }
            }
        }

        for index in 0..<32 {
            let hash = "hash-concurrent-\(index)"
            #expect(await cache.get(contentHash: hash) != nil)
        }
    }

    @Test("concurrent puts to the same key never produce a corrupted file")
    func concurrentPutsSameKey() async {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        await withTaskGroup(of: Void.self) { group in
            for index in 0..<16 {
                group.addTask {
                    let plan = Self.makePlan(
                        contentHash: "hash-shared",
                        chapterCount: index % 4 + 1
                    )
                    _ = await cache.put(contentHash: "hash-shared", plan: plan)
                }
            }
        }

        // After all writes complete, the file is a valid plan
        // (whichever writer landed last), never a torn file.
        let fetched = await cache.get(contentHash: "hash-shared")
        #expect(fetched != nil)
        #expect(fetched?.episodeContentHash == "hash-shared")
    }

    // MARK: sanitize() / filename() contract

    @Test("sanitize folds unsafe path characters to underscore")
    func sanitizeReplacesUnsafeChars() {
        let raw = "../etc/passwd"
        let safe = ChapterPlanCache.sanitize(contentHash: raw)
        #expect(!safe.contains("/"))
        #expect(!safe.contains("."))
    }

    @Test("sanitize preserves alphanumerics, dash, and underscore")
    func sanitizeKeepsAllowed() {
        let raw = "abcXYZ123-_"
        #expect(ChapterPlanCache.sanitize(contentHash: raw) == raw)
    }

    @Test("sanitize maps an empty hash to a stable placeholder")
    func sanitizeEmptyFallback() {
        #expect(ChapterPlanCache.sanitize(contentHash: "") == "__empty__")
    }

    @Test("filename has stable .json extension and embeds the disambiguation suffix")
    func filenameShape() {
        let name = ChapterPlanCache.filename(forContentHash: "abcXYZ-_")
        #expect(name.hasSuffix(".json"))
        // <safe>_<12-char-suffix>.json — the suffix comes after the
        // last underscore before the extension.
        let suffix = ChapterPlanCache.disambiguationSuffix(for: "abcXYZ-_")
        #expect(suffix.count == 12)
        #expect(name.contains("_\(suffix)."))
    }

    @Test("two hashes that fold to the same sanitized name still produce distinct filenames")
    func filenameAvoidsSanitizationCollisions() async throws {
        // "a/b" and "a_b" both sanitize to "a_b". Without a
        // disambiguation suffix they would write to the same file.
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        let planA = Self.makePlan(contentHash: "a/b")
        let planB = Self.makePlan(contentHash: "a_b")

        let storedA = await cache.put(contentHash: "a/b", plan: planA)
        let storedB = await cache.put(contentHash: "a_b", plan: planB)
        #expect(storedA == true)
        #expect(storedB == true)

        let fetchedA = await cache.get(contentHash: "a/b")
        let fetchedB = await cache.get(contentHash: "a_b")
        #expect(fetchedA?.episodeContentHash == "a/b")
        #expect(fetchedB?.episodeContentHash == "a_b")
        #expect(fetchedA != fetchedB)
    }

    @Test("disambiguation suffix is deterministic for a given input")
    func disambiguationSuffixDeterministic() {
        let s1 = ChapterPlanCache.disambiguationSuffix(for: "stable-input")
        let s2 = ChapterPlanCache.disambiguationSuffix(for: "stable-input")
        #expect(s1 == s2)
        #expect(s1.count == 12)
        // The suffix is lowercase ASCII hex by construction
        // (`String(format: "%02x", _)` over SHA256 bytes). Pin that
        // exact contract — `Character.isHexDigit` would accept
        // uppercase and non-ASCII Unicode hex digits we never emit.
        let asciiHex: Set<Character> = Set("0123456789abcdef")
        #expect(s1.allSatisfy { asciiHex.contains($0) })
    }

    // MARK: migrate() idempotency

    @Test("migrate is idempotent across repeated calls")
    func migrateIsIdempotent() async {
        let dir = Self.makeTempDirectory()
        defer { try? FileManager.default.removeItem(at: dir) }
        let cache = ChapterPlanCache(directory: dir)

        // First call resolves and creates the directory.
        await cache.migrate()
        #expect(FileManager.default.fileExists(atPath: dir.path))

        // Second call must be a silent no-op (no throw, dir still present).
        await cache.migrate()
        #expect(FileManager.default.fileExists(atPath: dir.path))

        // And subsequent puts/gets still work afterward.
        let stored = await cache.put(
            contentHash: "post-migrate",
            plan: Self.makePlan(contentHash: "post-migrate")
        )
        #expect(stored == true)
        #expect(await cache.get(contentHash: "post-migrate") != nil)
    }
}
