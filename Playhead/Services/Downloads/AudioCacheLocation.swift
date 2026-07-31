// AudioCacheLocation.swift
// playhead-b8hj: the audio cache lives at
// `<container>/Library/Caches/Playhead/AudioCache/{complete,partials}/<name>`,
// and BOTH halves of that prefix are unstable:
//
//   1. The Data-container UUID is rewritten on reinstall, on restore, and on
//      some TestFlight upgrades. The container CONTENTS survive; the path to
//      them does not. The owner's device carries 36 `analysis_assets` rows
//      spread across 12 distinct container UUIDs in 9 days of dogfooding, so
//      an absolute path stored at insert time names a directory that no
//      longer exists for all but the newest handful of rows.
//   2. `Library/Caches` is evictable by iOS at any time, so even inside the
//      CURRENT container the file can vanish while a row still claims it.
//
// The stable half is the FILENAME: `DownloadManager.safeFilename(for:)` is
// SHA-256 of the episode id, so `complete/<64-hex>.<ext>` survives a container
// move that rewrites everything to its left. This type is the one place that
// knows how to (a) strip the volatile prefix before persisting and (b) put a
// current-container prefix back on at read time.
//
// CONTRACT: `resolve` returns `nil` when the audio is genuinely absent. That
// is the NORMAL state after an eviction or a wipe, not an error — every caller
// must degrade to "not cached, re-download" rather than erroring or stranding
// the asset. Nothing here throws.

import Foundation

/// Encode/decode for a persisted reference to a file in the audio cache.
enum AudioCacheLocation {
    /// The per-episode artifact subdirectories of the audio-cache root.
    /// Mirrors `DownloadManager.completeDirectory` / `partialsDirectory`.
    static let knownSubdirectories: Set<String> = ["complete", "partials"]

    // MARK: - Encoding

    /// The container-portable string to persist for a local audio file.
    ///
    /// A file inside the audio cache is reduced to its `<subdirectory>/<name>`
    /// tail — everything volatile is dropped. Anything else (a corpus snapshot
    /// in the dump harness, a test fixture, a remote URL) is stored verbatim,
    /// because we have no basis for re-rooting it and a wrong prefix is worse
    /// than an honest one.
    ///
    /// The result is deliberately NOT a `file://` URL: a reader that has not
    /// been taught about this format gets `isFileURL == false` and resolves
    /// nothing, rather than confidently opening the wrong path.
    ///
    /// `cacheRoot` defaults to the global root rather than being threaded from
    /// the `DownloadManager` that produced `url`, because production builds
    /// exactly one manager and passes it no `cacheDirectory`
    /// (`PlayheadRuntime.init`). Encode and decode therefore always agree. A
    /// custom-rooted manager would simply fail the containment check and store
    /// an absolute path — degraded, never wrong — but if one is ever
    /// introduced in production, thread its `cacheDirectory` through here.
    static func portableString(
        for url: URL,
        cacheRoot: URL = DownloadManager.defaultCacheDirectory()
    ) -> String {
        guard url.isFileURL, let tail = cacheRelativeTail(of: url, under: cacheRoot) else {
            return url.absoluteString
        }
        return tail.joined(separator: "/")
    }

    // MARK: - Decoding

    /// Resolve a persisted reference against the CURRENT container.
    ///
    /// Resolution order:
    ///   1. If the stored value is an absolute file path that is usable as-is,
    ///      use it. This covers rows written in the current container and the
    ///      out-of-cache paths `portableString` stores verbatim.
    ///   2. Otherwise re-root the `<subdirectory>/<name>` tail — taken from the
    ///      stored relative form, or recovered from a stale absolute path — on
    ///      the current cache root.
    ///
    /// There is deliberately no third step. An earlier draft also scanned for
    /// the same stem under a different audio extension, to survive
    /// `DownloadManager.resolveExtension(for:)`'s `mp3` fallback. That is a
    /// resolution the resolver of record would REFUSE: `servingURLIfComplete`
    /// checks the `.pin` sidecar's byte length and strong hash, and returns
    /// `nil` outright when two audio siblings share a stem, "so an old
    /// extension sibling can never bypass its pin/hash disambiguation"
    /// (playhead-wrj8, `PlayheadRuntime`). A weaker second resolver handing the
    /// byte differ a truncated or stale A-side is worse than not resolving:
    /// the miss degrades to "not cached", the bad file mints a wrong mark.
    /// Extension drift therefore reads as not-cached and re-downloads, which is
    /// the same safe outcome as an eviction.
    ///
    /// - Parameter isUsable: the "this file is really here" predicate. Callers
    ///   pass the same anchor they would have applied to the raw URL, so
    ///   resolution can never admit a file the caller would have rejected.
    /// - Returns: `nil` when no usable file exists. NOT an error.
    static func resolve(
        _ stored: String,
        cacheRoot: URL = DownloadManager.defaultCacheDirectory(),
        isUsable: (URL) -> Bool
    ) -> URL? {
        let trimmed = stored.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }

        let absolute: URL?
        if let parsed = URL(string: trimmed), parsed.scheme != nil {
            // A remote enclosure URL names no local artifact at all.
            guard parsed.isFileURL else { return nil }
            absolute = parsed
        } else if trimmed.hasPrefix("/") {
            absolute = URL(fileURLWithPath: trimmed)
        } else {
            absolute = nil
        }

        if let absolute, isUsable(absolute) { return absolute }

        let components = absolute.map(\.pathComponents)
            ?? trimmed.split(separator: "/").map(String.init)
        guard let tail = cacheRelativeTail(of: components) else { return nil }

        let candidate = cacheRoot
            .appendingPathComponent(tail[0], isDirectory: true)
            .appendingPathComponent(tail[1])
        return isUsable(candidate) ? candidate : nil
    }

    // MARK: - Tail extraction

    /// The `<subdirectory>/<name>` tail of a URL known to sit under `root`, or
    /// `nil` when it does not sit there or is not shaped like an artifact.
    private static func cacheRelativeTail(of url: URL, under root: URL) -> [String]? {
        // Compared both as written and with symlinks resolved: on iOS the
        // container sits under `/var`, a symlink to `/private/var`, and
        // `resolvingSymlinksInPath()` only rewrites path prefixes that exist —
        // so the two sides can legitimately normalize differently.
        let candidates = [
            (url.standardizedFileURL.pathComponents, root.standardizedFileURL.pathComponents),
            (
                url.standardizedFileURL.resolvingSymlinksInPath().pathComponents,
                root.standardizedFileURL.resolvingSymlinksInPath().pathComponents
            )
        ]
        for (urlComponents, rootComponents) in candidates
        where urlComponents.count == rootComponents.count + 2
            && Array(urlComponents.prefix(rootComponents.count)) == rootComponents {
            return cacheRelativeTail(of: Array(urlComponents.suffix(2)))
        }
        return nil
    }

    /// The `<subdirectory>/<name>` tail of an arbitrary path decomposition.
    ///
    /// Only the LAST artifact subdirectory counts, and only the final component
    /// is kept as the name — so a stale absolute path and the relative form
    /// written for the same file normalize to the identical two components.
    ///
    /// The scan is positional, not root-anchored, so a MISSING path stored
    /// verbatim from outside the cache (a dump-harness snapshot at
    /// `<corpus>/<show>/complete/<x>.mp3`) will also be probed under the cache
    /// root. That is intentional — it is the same operation that rescues a
    /// stale container path, and the names are SHA-256 of an episode id, so a
    /// false hit would require a collision.
    private static func cacheRelativeTail(of components: [String]) -> [String]? {
        guard components.count >= 2 else { return nil }
        let name = components[components.count - 1]
        // `.`/`..` would re-root to a directory rather than an artifact. The
        // `isUsable` anchor already rejects those, but refusing to build the
        // path at all keeps traversal out of the resolved set entirely.
        guard !name.isEmpty, name != ".", name != ".." else { return nil }
        for index in stride(from: components.count - 2, through: 0, by: -1)
        where knownSubdirectories.contains(components[index]) {
            return [components[index], name]
        }
        return nil
    }
}
