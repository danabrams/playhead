// RediffRefetchSeams.swift
// playhead-xsdz.28: the environment seams the `RediffRefetchService` drives —
// episode enumeration, the ranged-GET rotation pre-check, the full re-fetch,
// the B-side fingerprint (OFF the hot actor), the outcome recorder, and the
// B-copy remover — plus their production conformers.
//
// The seams keep the whole re-fetch POLICY offline-testable: tests inject
// scripted samples/downloads and assert the pre-check skips non-rotators, the
// ≥24h gate holds, the B-copy is deleted, and bandwidth is accounted — without
// a network or a device. Production conformers wire URLSession + FileHandle +
// the xsdz.27 fingerprint extractor.
//
// NO HEAD / NO ETag ANYWHERE (spike §3/§7): the ranged sampler issues ONLY
// range GETs and reads the total length from `Content-Range`. There is no
// conditional-GET (`If-None-Match` / `If-Modified-Since`) and no HEAD request
// in this file — HEAD is broken on Acast and podtrac length-flaps seconds apart.

import Foundation
import os
import OSLog

// MARK: - Candidate value type

/// One episode the re-fetch sweep may act on. The enumerator snapshots the
/// download metadata + the durable re-fetch state so the sweep runs entirely on
/// `Sendable` values (no live SwiftData / model objects cross the actor).
struct RediffRefetchCandidate: Sendable, Equatable {
    /// `analysis_assets.id` — the key the played-copy fingerprint (A-side) and
    /// any resulting rediff slots are stored under (xsdz.27/.29).
    let assetId: String
    /// The current enclosure URL for the episode's audio. Re-resolved from the
    /// feed by the enumerator (spike §7: use the CURRENT enclosure URL, not a
    /// stale one) so an expired URL is skipped upstream.
    let enclosureURL: URL
    /// Unix seconds the played copy was downloaded — the ≥24h gate's baseline.
    let downloadedAt: Double
    /// The played copy's on-disk audio file — the LOCAL side of the pre-check.
    let localAudioURL: URL
    /// Durable re-fetch bookkeeping for this episode (backoff / retry budget).
    let attemptState: RediffRefetchPolicy.AttemptState
}

// MARK: - Remote sample

/// The remote copy's change-detection sample plus the bytes it cost to fetch
/// (head + tail, ~128 KB). Produced by a `RangedAudioSampling` conformer.
struct RemoteAudioSample: Sendable, Equatable {
    let fingerprint: RediffRefetchPolicy.AudioSampleFingerprint
    /// Bytes actually transferred for the sample (for bandwidth accounting).
    let bytesTransferred: Int
}

// MARK: - Seams

/// Snapshots the episodes eligible for a re-fetch sweep (downloaded copies with
/// a resolvable current enclosure URL) as `Sendable` value candidates.
protocol RediffRefetchEnumerating: Sendable {
    func candidates() async -> [RediffRefetchCandidate]
}

/// Fetches the remote head/tail sample via RANGE GETs ONLY (no HEAD, no ETag).
protocol RangedAudioSampling: Sendable {
    func sample(url: URL, headBytes: Int, tailBytes: Int) async throws -> RemoteAudioSample
}

/// Computes the local (played-copy) head/tail sample from the on-disk file.
protocol LocalAudioSampling: Sendable {
    func sample(fileURL: URL, headBytes: Int, tailBytes: Int) throws -> RediffRefetchPolicy.AudioSampleFingerprint
}

/// Streams a full re-fetch of the changed copy to a TRANSIENT temp file. The
/// caller ALWAYS deletes it after fingerprinting — the B-copy is never
/// persisted. Returns the temp file URL + its byte count.
protocol FullEpisodeFetching: Sendable {
    func download(url: URL) async throws -> (fileURL: URL, byteCount: Int)

    /// playhead-xsdz.36.2 (k-way): fetch under an EXPLICIT request-context
    /// persona so one injected fetcher can present K DISTINCT contexts across a
    /// k-way batch. The default IGNORES the persona (routes to `download(url:)`)
    /// so pre-k-way conformers/test doubles compile and behave unchanged; the
    /// production `URLSessionFullEpisodeFetcher` overrides it to stamp the
    /// persona. A `nil` persona means the conformer's own default context.
    func download(url: URL, persona: RediffFetchPersona?) async throws -> (fileURL: URL, byteCount: Int)
}

extension FullEpisodeFetching {
    func download(url: URL, persona: RediffFetchPersona?) async throws -> (fileURL: URL, byteCount: Int) {
        try await download(url: url)
    }
}

/// Decodes + resamples + fingerprints the B-side copy OFF the hot actor and
/// returns the subfingerprint stream. Conformers MUST NOT run the CPU-heavy
/// resample+fingerprint synchronously on a serial "hot" actor (xsdz.29 R5
/// residual): a full-episode resample would stall it. A plain `Sendable`
/// value-type conformer whose `async` body calls the pure extractor runs on the
/// generic concurrent executor, not any serial actor — that is the intended
/// shape.
protocol RediffBSideFingerprinting: Sendable {
    func fingerprint(fileURL: URL) async throws -> [UInt32]
}

/// Decodes an arbitrary audio file to mono 16 kHz PCM — the analysis pipeline's
/// decode rate (`AnalysisAudioService.targetSampleRate`). The one AVFoundation-
/// bound step; injected so the fingerprinter stays offline-testable. Live
/// wiring (reusing the existing decode path) lands with activation (xsdz.36).
protocol AudioFileDecoding: Sendable {
    func decodeMono16kHz(fileURL: URL) async throws -> [Float]
}

/// Records the terminal outcome of each candidate (skips, non-rotators,
/// rotations, failures) for dogfood accounting AND to persist the advanced
/// `AttemptState`. Default conformer just logs.
protocol RediffRefetchRecording: Sendable {
    func recordOutcome(_ outcome: RediffRefetchPolicy.Outcome) async
}

/// playhead-xsdz.36 ACTIVATION seam: consumes a freshly fetched, ROTATED
/// B-copy while it still exists on disk — the production conformer stages the
/// file into the `RediffBSideStagingProvider`, drives
/// `AdDetectionService.revalidateFromFeatures` (which runs the rediff slot
/// pass against the staged B-side), and unstages. The CALLER
/// (`RediffRefetchService.processCandidate`) still owns deletion of the file
/// via its `defer` — the never-persist-B contract is unchanged.
///
/// A throw means the B-side was NOT consumed: the candidate records `.failed`
/// (not resolved) so a later sweep re-fetches and retries under the R2
/// failure policy.
protocol RediffBSideConsuming: Sendable {
    func consumeRotatedBSide(assetId: String, fileURL: URL) async throws

    /// playhead-xsdz.36.2 (k-way): consume the K distinct-persona B-copies of a
    /// rotated candidate at ONCE — the production conformer stages ALL of them,
    /// drives ONE `revalidateFromFeatures` (so `computeByteAlignedPlayedSlots`
    /// aligns A vs each Bi and UNIONS the divergent regions), then unstages. The
    /// CALLER still owns deletion of every file via its `defer`. The default
    /// routes to the single-file `consumeRotatedBSide` with the FIRST copy so
    /// pre-k-way conformers/doubles are unchanged; an empty list is a no-op.
    ///
    /// A throw means NO B-side was consumed: the candidate records `.failed` so a
    /// later sweep re-fetches and retries under the R2 failure policy.
    func consumeRotatedBSides(assetId: String, fileURLs: [URL]) async throws
}

extension RediffBSideConsuming {
    func consumeRotatedBSides(assetId: String, fileURLs: [URL]) async throws {
        guard let first = fileURLs.first else { return }
        try await consumeRotatedBSide(assetId: assetId, fileURL: first)
    }
}

/// playhead-xsdz.36.4 DAY-0 seam: the FIRST-LISTEN marking path. Consumes the
/// k-way day-0 B-copies by BYTE-aligning them against the PINNED played A-side
/// (resolved read-only from the asset row inside the conformer — wrj8) and
/// minting MARK-ONLY banners for byte-EXACT, ≥2-persona-robust divergent slots.
///
/// This is the deterministic-certainty exception to the presence-gated mandate:
/// a byte-exact divergent region IS a dynamically-inserted ad segment,
/// sample-accurately — so it mints its OWN ad-presence core and does NOT depend
/// on any persisted transcript / analysis (which does not yet exist on a true
/// first listen — the very failure the day-0 `RediffBSideConsuming`/revalidate
/// route hit). The chroma differ is NEVER consulted here (byte-exact only).
///
/// Returns the number of marks minted. `0` ⇒ nothing byte-exact/robust was
/// found (or a chroma-only fallback) ⇒ the day-0 run must NOT resolve the
/// shared lagged state, so the lagged sweep still recovers the ads later. The
/// CALLER (`RediffRefetchService`) still owns deletion of every B-copy via its
/// `defer` — the never-persist-B contract is unchanged. Mark-only (no auto-skip).
protocol RediffDayZeroMinting: Sendable {
    func mintByteExactDayZeroMarks(assetId: String, bSideURLs: [URL]) async -> Int
}

/// The B-side decoded to an EMPTY fingerprint stream — nothing to diff, and
/// deterministic for the same copy (fingerprint-mismatch class).
struct RediffBSideEmptyStreamError: RediffFailureClassifiable, Equatable {
    var rediffFailureClass: RediffRefetchPolicy.FailureClass { .fingerprintMismatch }
}

/// Removes the transient B-copy temp file. A seam (not a bare `FileManager`
/// call) so a test can assert removal WITHOUT a filesystem AND so the real
/// FileManager remover can be exercised against a real temp file.
protocol RediffTempFileRemoving: Sendable {
    func remove(_ fileURL: URL)

    /// playhead-xsdz.36 (R1 hygiene, extended in R2): remove ORPHANED rediff
    /// B-side artifacts abandoned by a process that died mid-consume (jetsam
    /// is a routine BGProcessingTask fate). Two artifact classes, both
    /// prefix-scoped and age-floored:
    ///
    ///   * tmp/ B-copies — files the full fetcher staged under its
    ///     `rediff-bcopy-` prefix (~54 MB each; iOS purges tmp/ only
    ///     opportunistically);
    ///   * shard-cache directories — the chroma fallback's transient
    ///     `rediff-bside-<uuid>` decode entries in NON-purgeable Application
    ///     Support (~230 MB per decoded hour; every retry mints a new uuid,
    ///     so these accumulate with no other cleaner).
    ///
    /// Called once per BG fire, BEFORE the sweep. `age` guards the
    /// (test-only multi-instance) window where another service instance is
    /// mid-candidate: a live B-copy/decode is consumed within one fire, so
    /// anything older than `age` is unambiguously an orphan. Default no-op
    /// so spy conformers and the flag-OFF world are untouched.
    func removeOrphanedBCopies(olderThan age: TimeInterval)
}

extension RediffTempFileRemoving {
    func removeOrphanedBCopies(olderThan age: TimeInterval) {}
}

// MARK: - Request context (persona) + fetch hygiene

/// playhead-xsdz.45 (+ xsdz.36.2 enabler): the HTTP request-context a rediff
/// fetch presents to the DAI stack. Cross-network survey (2026-07-21): AdsWizz
/// (Conan, Fresh Air) and Art19 (Business Wars) PIN the ad fill per client —
/// a same-context double-fetch returns byte-identical bodies (nothing to
/// byte-align), while varying ONLY the `User-Agent` header yields a divergent
/// fill. This `Sendable` value type is the seam Unit 2 (k-way over multiple
/// personas) and Unit 3 (day-0 play-time rediff) plumb through the fetch
/// conformers; Unit 1 wires the production sweep under the single default
/// (AppleCoreMedia-iPhone) persona.
struct RediffFetchPersona: Sendable, Equatable {
    /// Stable identifier for logging / test membership assertions. NEVER sent.
    let name: String
    /// The `User-Agent` header value. `nil` OR empty ⇒ NO `User-Agent` header
    /// is set and the request goes out under the system default UA — this is
    /// the "empty-UA" persona's contract, and it matches a nil/absent persona.
    let userAgent: String?
    /// Optional `Accept` override. `nil` ⇒ leave CFNetwork's default (`*/*`).
    let accept: String?
    /// Optional `Accept-Language` override. `nil` ⇒ system default.
    let acceptLanguage: String?

    init(name: String, userAgent: String?, accept: String? = nil, acceptLanguage: String? = nil) {
        self.name = name
        self.userAgent = userAgent
        self.accept = accept
        self.acceptLanguage = acceptLanguage
    }

    /// Stamp this persona's request context onto `request`. The UA is applied
    /// ONLY when non-nil AND non-empty, so the empty-UA persona (and any nil
    /// field) leaves the header untouched — byte-identical to today's no-UA
    /// request. `Accept` / `Accept-Language` follow the same non-empty guard.
    func apply(to request: inout URLRequest) {
        if let userAgent, !userAgent.isEmpty {
            request.setValue(userAgent, forHTTPHeaderField: "User-Agent")
        }
        if let accept, !accept.isEmpty {
            request.setValue(accept, forHTTPHeaderField: "Accept")
        }
        if let acceptLanguage, !acceptLanguage.isEmpty {
            request.setValue(acceptLanguage, forHTTPHeaderField: "Accept-Language")
        }
    }

    // MARK: Curated bank

    // The divergence-reliable set (cross-network survey + the AdsWizz de-risk
    // probe). AppleCoreMedia-iPhone and AppleCoreMedia-Macintosh — the UAs
    // iOS's own media stack sends — both classify as "streaming" and ALWAYS
    // return distinct fills; Overcast and the empty-UA persona add extra
    // distinct stitches.
    //
    // DELIBERATELY EXCLUDED — curl and other generic/unclassified UAs. The
    // AdsWizz de-risk probe found AdsWizz classifies them "unclassified",
    // emits a `p_f_skip` token, and serves NO ad stitch (minimal/empty body),
    // which is useless for rediff (nothing to byte-align). Do NOT add a
    // curl/generic persona to this bank.

    /// iOS's own media-stack UA. The designated DEFAULT persona.
    static let appleCoreMediaIPhone = RediffFetchPersona(
        name: "applecoremedia-iphone",
        userAgent: "AppleCoreMedia/1.0.0.21G93 (iPhone; U; CPU OS 17_6_1 like Mac OS X; en_us)"
    )

    /// The Macintosh media-stack analogue — a reliably DISTINCT "streaming"
    /// fill vs the iPhone persona.
    static let appleCoreMediaMac = RediffFetchPersona(
        name: "applecoremedia-macintosh",
        userAgent: "AppleCoreMedia/1.0.0.23G93 (Macintosh; U; Intel Mac OS X 10_15_7; en_us)"
    )

    /// A popular third-party podcast client — an extra distinct stitch.
    static let overcast = RediffFetchPersona(
        name: "overcast",
        userAgent: "Overcast/3.0.0 (+http://overcast.fm/)"
    )

    /// Empty UA ⇒ NO explicit `User-Agent` header is set, so the request goes
    /// out under CFNetwork's system default UA (not a literally-empty header —
    /// the OS still fills one in). That system-default UA is itself distinct
    /// from the AppleCoreMedia personas, so it contributes an extra distinct
    /// fill in a k-way batch; it is also the safe stand-in for a nil/absent
    /// persona. Kept explicit in the bank so Unit 2 can name it as a member.
    static let emptyUA = RediffFetchPersona(name: "empty-ua", userAgent: "")

    /// The divergence-reliable bank Unit 2 (k-way) fans out over, default
    /// first. curl/generic UAs are excluded on purpose (see the note above).
    static let curatedBank: [RediffFetchPersona] = [
        .appleCoreMediaIPhone,
        .appleCoreMediaMac,
        .overcast,
        .emptyUA,
    ]

    /// The single persona the production sweep (Unit 1) fetches under.
    static let `default` = RediffFetchPersona.appleCoreMediaIPhone

    /// playhead-xsdz.36.2 (k-way): the first `count` DISTINCT personas from the
    /// curated bank in the divergence-reliable ORDER — iPhone → Mac → Overcast
    /// → empty. The AppleCoreMedia iPhone+Mac pair is the reliable divergence
    /// CORE (both classify "streaming", always distinct fills); Overcast and the
    /// empty-UA persona add extra distinct stitches. A k-way batch draws these
    /// K personas so every fetch presents a DISTINCT request context — never
    /// reusing a persona within a batch, and never a curl/generic UA (the bank
    /// excludes those; AdsWizz serves them ad-light).
    ///
    /// `count` is clamped to `[1, curatedBank.count]`: K=1 yields exactly
    /// `[.appleCoreMediaIPhone]` (== `.default`), so a K=1 batch is
    /// byte-identical on the wire to today's single default-persona fetch; a
    /// `count` above the bank size is capped at `curatedBank.count` (there are
    /// no further distinct personas to draw without reuse).
    static func kWayPersonas(count: Int) -> [RediffFetchPersona] {
        let k = max(1, min(count, curatedBank.count))
        return Array(curatedBank.prefix(k))
    }
}

/// playhead-xsdz.36.3: shared request hygiene for EVERY rediff enclosure fetch
/// — cache-busting + optional persona stamping. Per-request-rotating shows
/// (SYSK/Omny/Triton) returned byte-shrinking near-identical bodies until the
/// request was made UNIQUE: an in-session URL cache / CDN edge cache was
/// serving a stale copy and defeating rotation. Every rediff fetch therefore
/// (a) appends a unique `_cb` query item, (b) ignores local caches via the
/// request cache policy, and (c) runs on a `urlCache = nil` session — three
/// independent guards so no rediff body is ever served from a stale cache.
///
/// A cache-buster query param and `URLRequest.cachePolicy` are NOT conditional
/// requests, so the file's NO-HEAD / NO-ETag contract is preserved.
enum RediffFetchRequest {
    /// Unobtrusive cache-buster query-item name.
    static let cacheBusterQueryItem = "_cb"

    /// Token characters left un-encoded when the cache-buster value is stamped
    /// into the query: the RFC 3986 "unreserved" set, spelled out as ASCII
    /// `A-Za-z0-9-._~` ONLY. Deliberately NOT `CharacterSet.alphanumerics` —
    /// that set is the *Unicode* alphanumerics, so a non-ASCII injected token
    /// (e.g. `"café"`) would be left as a RAW byte, which then either voids
    /// `URLComponents.url` (silently dropping the whole `_cb` cache-buster) or
    /// emits an invalid non-ASCII query. The default UUID token is already
    /// within this ASCII set; any custom injected token — including a non-ASCII
    /// one — is defensively percent-encoded so it can never introduce a stray
    /// `&`/`#`/`=` or a raw non-ASCII byte that corrupts or voids the query.
    private static let tokenAllowed = CharacterSet(
        charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
    )

    /// Append a UNIQUE cache-buster query item to `url`, PRESERVING any existing
    /// query string BYTE-FOR-BYTE. Uses `percentEncodedQueryItems` (NOT
    /// `queryItems`): the latter decode/re-encode round-trip mangles pre-existing
    /// percent-encoding in the enclosure's query — `%2F`→`/`, `%2B`→`+`,
    /// `%3A`→`:` — which would silently change WHICH object a redirect/tracking
    /// param resolves to. `percentEncodedQueryItems` keeps existing items
    /// verbatim and appends `_cb` (its value pre-encoded via `tokenAllowed`).
    /// An existing `?a=b` is kept, never clobbered; the fragment is preserved.
    /// Falls back to the original URL if it cannot be decomposed (never expected
    /// for an http(s) enclosure).
    ///
    /// RESIDUAL RISK — signed-URL 403 (xsdz.36.3 self-review "F2", DEFERRED):
    /// `_cb` is appended UNCONDITIONALLY, so an enclosure whose signature
    /// covers the whole query string (e.g. an AWS SigV4 presigned URL) would
    /// reject the extra param with a 403. This is CONTAINED, not silently
    /// harmful: the 403 is a non-206 pre-check (`SampleError.notPartialContent`)
    /// classified `.transient`, swallowed per-candidate, and it fails on the
    /// ~128 KB ranged pre-check BEFORE the ~54 MB full fetch — no bandwidth
    /// storm, no sweep abort. The DAI stacks the cache-buster targets
    /// (AdsWizz/ART19/Megaphone) tolerate unknown extra params, and Playhead
    /// resolves PLAIN public podcast enclosure URLs (there is no presign/token
    /// logic anywhere in the feed/enclosure path), so no signed-URL feed is in
    /// today's candidate set. A host-aware / retry-without-`_cb`-on-4xx
    /// mitigation is deferred to a follow-up bead rather than gold-plated here.
    static func cacheBustedURL(_ url: URL, token: String) -> URL {
        guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return url
        }
        let encodedToken = token.addingPercentEncoding(withAllowedCharacters: tokenAllowed) ?? token
        var items = components.percentEncodedQueryItems ?? []
        items.append(URLQueryItem(name: cacheBusterQueryItem, value: encodedToken))
        components.percentEncodedQueryItems = items
        return components.url ?? url
    }

    /// Build the base GET request for a rediff fetch against an ALREADY
    /// cache-busted URL: WiFi-only, cache-ignoring, persona-stamped. Callers
    /// add the `Range` header (ranged pre-check) or nothing (full download).
    static func makeBaseRequest(cacheBustedURL: URL, persona: RediffFetchPersona?) -> URLRequest {
        var request = URLRequest(url: cacheBustedURL)
        request.httpMethod = "GET"
        request.allowsCellularAccess = false
        // Ignore any local cache entry — belt-and-suspenders with the `_cb`
        // query item and the `urlCache = nil` session so a per-request-rotating
        // DAI edge/URL cache can never serve a stale, byte-identical stitch
        // (playhead-xsdz.36.3).
        request.cachePolicy = .reloadIgnoringLocalCacheData
        persona?.apply(to: &request)
        return request
    }
}

// MARK: - Production conformers

/// URLSession-backed ranged sampler. Issues exactly two GETs — `bytes=0-…` for
/// the head (reading the total length from `Content-Range`) and
/// `bytes=(total-tail)-…` for the tail — and NEVER a HEAD or conditional GET.
/// WiFi is enforced by `allowsCellularAccess = false` (the BGTask supplies the
/// charging + network-present gate; this pins the WiFi half of the policy).
struct URLSessionRangedAudioSampler: RangedAudioSampling {
    let session: URLSession
    /// playhead-xsdz.45: the request-context this sampler fetches under. `nil`
    /// (default, and every pre-activation caller/test) ⇒ NO persona headers;
    /// the request matches the xsdz.28 one (system default UA) EXCEPT for the
    /// xsdz.36.3 cache-buster, which every rediff fetch carries. The production
    /// sweep is constructed with `.default` (AppleCoreMedia-iPhone).
    let persona: RediffFetchPersona?
    /// playhead-xsdz.36.3: per-sample cache-buster token generator. Called
    /// EXACTLY ONCE per `sample()` — the head and tail sub-requests share the
    /// token so they resolve to the same (busted) object and stay a coherent
    /// pair — and must return a UNIQUE token each call so no rediff pre-check
    /// is ever served a stale cached body. Injectable for deterministic tests;
    /// defaults to a UUID (a distinct token per fetch, as a k-way batch needs).
    let cacheBuster: @Sendable () -> String

    init(
        session: URLSession = URLSessionRangedAudioSampler.makeWiFiOnlySession(),
        persona: RediffFetchPersona? = nil,
        cacheBuster: @escaping @Sendable () -> String = { UUID().uuidString }
    ) {
        self.session = session
        self.persona = persona
        self.cacheBuster = cacheBuster
    }

    /// A WiFi-and-not-constrained URLSession for rediff traffic (spike §5:
    /// "~1 GB/week over WiFi is acceptable … unacceptable on cellular").
    static func makeWiFiOnlySession() -> URLSession {
        let config = URLSessionConfiguration.ephemeral
        config.allowsCellularAccess = false
        config.allowsConstrainedNetworkAccess = false
        config.allowsExpensiveNetworkAccess = false
        // playhead-xsdz.36.3: disable the in-memory URL cache entirely so a
        // rediff pre-check / full fetch can never be satisfied from a stale
        // cached body (belt-and-suspenders with the per-request `_cb`
        // cache-buster and the reload cache policy).
        config.urlCache = nil
        return URLSession(configuration: config)
    }

    enum SampleError: Error, Equatable {
        case notPartialContent(status: Int)
        case missingContentRange
        case unparsableTotalLength(String)
    }

    func sample(url: URL, headBytes: Int, tailBytes: Int) async throws -> RemoteAudioSample {
        // playhead-xsdz.36.3: ONE cache-buster per sample — the head and tail
        // sub-requests share it so they resolve to the same busted object; a
        // fresh token each sample defeats any stale URL/edge cache.
        let bustedURL = RediffFetchRequest.cacheBustedURL(url, token: cacheBuster())

        // Head-sample GET (Range bytes=0-(headBytes-1)) — a ranged GET, NOT an
        // HTTP HEAD — whose 206 `Content-Range` also yields the total length.
        let (headData, total) = try await rangedGet(url: bustedURL, start: 0, length: headBytes, expectContentRange: true)
        let totalLength = try requireTotal(total)

        // TAIL request. Clamp the start for a file smaller than the tail window
        // so head and tail may overlap (deterministic; both sides use the same
        // clamp, so equal copies still compare equal). Episodes are MB-scale so
        // this is theoretical.
        let tailStart = max(0, totalLength - Int64(tailBytes))
        let tailLength = Int(totalLength - tailStart)
        let (tailData, _) = try await rangedGet(url: bustedURL, start: tailStart, length: tailLength, expectContentRange: false)

        let fingerprint = RediffRefetchPolicy.sampleFingerprint(
            head: headData,
            tail: tailData,
            totalLength: totalLength
        )
        return RemoteAudioSample(
            fingerprint: fingerprint,
            bytesTransferred: headData.count + tailData.count
        )
    }

    /// One range GET. `url` is ALREADY cache-busted by `sample()`. Returns the
    /// body bytes and, when `expectContentRange`, the parsed total length from
    /// `Content-Range: bytes A-B/TOTAL`.
    private func rangedGet(
        url: URL,
        start: Int64,
        length: Int,
        expectContentRange: Bool
    ) async throws -> (Data, Int64?) {
        var request = RediffFetchRequest.makeBaseRequest(cacheBustedURL: url, persona: persona)
        let end = start + Int64(max(0, length - 1))
        request.setValue("bytes=\(start)-\(end)", forHTTPHeaderField: "Range")

        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw SampleError.notPartialContent(status: -1)
        }
        // A CDN honoring the range replies 206. (Anything else — 200 full body,
        // 416, redirect loop — is a range failure; the caller treats the whole
        // candidate as failed rather than trusting a mis-sized sample.)
        guard http.statusCode == 206 else {
            throw SampleError.notPartialContent(status: http.statusCode)
        }
        guard expectContentRange else { return (data, nil) }
        guard let contentRange = http.value(forHTTPHeaderField: "Content-Range") else {
            throw SampleError.missingContentRange
        }
        return (data, try Self.parseTotalLength(contentRange))
    }

    private func requireTotal(_ total: Int64?) throws -> Int64 {
        guard let total, total > 0 else { throw SampleError.missingContentRange }
        return total
    }

    /// Parse the total length from `Content-Range: bytes 0-65535/84496614` (the
    /// part after the last `/`). Throws for a missing slash or an unknown (`*`)
    /// total — the length signal is the ranged GET's `Content-Range`, NOT HEAD.
    static func parseTotalLength(_ contentRange: String) throws -> Int64 {
        guard let slash = contentRange.lastIndex(of: "/") else {
            throw SampleError.unparsableTotalLength(contentRange)
        }
        let totalPart = contentRange[contentRange.index(after: slash)...]
            .trimmingCharacters(in: .whitespaces)
        guard totalPart != "*", let total = Int64(totalPart) else {
            throw SampleError.unparsableTotalLength(contentRange)
        }
        return total
    }
}

/// URLSession-backed full re-fetch. Streams the whole episode to a UNIQUE temp
/// file the caller owns and deletes; WiFi-only, matching the sampler.
struct URLSessionFullEpisodeFetcher: FullEpisodeFetching {
    let session: URLSession
    /// playhead-xsdz.45: request-context for the full B-side fetch. `nil`
    /// (default, and every pre-activation caller/test) ⇒ NO persona headers;
    /// the request matches the xsdz.28 one EXCEPT for the xsdz.36.3
    /// cache-buster every rediff fetch carries. Production uses `.default`.
    let persona: RediffFetchPersona?
    /// playhead-xsdz.36.3: cache-buster token generator, called ONCE per
    /// `download()` so the B-side full fetch is a UNIQUE request (never a
    /// stale cached stitch). Injectable for tests; defaults to a UUID.
    let cacheBuster: @Sendable () -> String

    init(
        session: URLSession = URLSessionRangedAudioSampler.makeWiFiOnlySession(),
        persona: RediffFetchPersona? = nil,
        cacheBuster: @escaping @Sendable () -> String = { UUID().uuidString }
    ) {
        self.session = session
        self.persona = persona
        self.cacheBuster = cacheBuster
    }

    enum FetchError: Error, Equatable {
        case notOK(status: Int)
    }

    /// Filename prefix for the caller-owned B-copy temp file. Shared with
    /// `FileManagerTempFileRemover.removeOrphanedBCopies` so the orphan sweep
    /// can never drift from what `download` actually names its files.
    static let bcopyFilenamePrefix = "rediff-bcopy-"

    /// Fetch under this fetcher's OWN persona (the pre-k-way call, byte-identical
    /// to xsdz.45). k-way callers use `download(url:persona:)` to draw a distinct
    /// persona per fetch instead.
    func download(url: URL) async throws -> (fileURL: URL, byteCount: Int) {
        try await download(url: url, persona: persona)
    }

    /// playhead-xsdz.36.2 (k-way): fetch under an EXPLICIT persona (overriding
    /// this fetcher's stored default), still with a fresh per-download
    /// cache-buster. A k-way batch calls this K times with K distinct personas
    /// through one injected fetcher.
    func download(url: URL, persona: RediffFetchPersona?) async throws -> (fileURL: URL, byteCount: Int) {
        // playhead-xsdz.36.3: fresh cache-buster per download so the B-side
        // full fetch is a UNIQUE request (never a stale cached stitch).
        let bustedURL = RediffFetchRequest.cacheBustedURL(url, token: cacheBuster())
        let request = RediffFetchRequest.makeBaseRequest(cacheBustedURL: bustedURL, persona: persona)

        let fileManager = FileManager.default
        let (tempURL, response) = try await session.download(for: request)
        if let http = response as? HTTPURLResponse, !(200...299).contains(http.statusCode) {
            try? fileManager.removeItem(at: tempURL)
            throw FetchError.notOK(status: http.statusCode)
        }
        // Move OUT of the URLSession-owned temp (which the system reclaims) into
        // a location WE control and delete after fingerprinting.
        let destination = fileManager.temporaryDirectory
            .appendingPathComponent(Self.bcopyFilenamePrefix + UUID().uuidString)
        try? fileManager.removeItem(at: destination)
        try fileManager.moveItem(at: tempURL, to: destination)
        return (destination, Self.fileByteCount(at: destination))
    }

    /// Byte size of a file, or 0 if unreadable.
    static func fileByteCount(at url: URL) -> Int {
        guard let attrs = try? FileManager.default.attributesOfItem(atPath: url.path),
              let size = attrs[.size] as? Int else { return 0 }
        return size
    }
}

/// FileHandle-backed local sampler. Reads the head/tail windows off the on-disk
/// played copy so its sample is directly comparable to the remote ranged one.
struct FileHandleLocalAudioSampler: LocalAudioSampling {

    init() {}

    func sample(fileURL: URL, headBytes: Int, tailBytes: Int) throws -> RediffRefetchPolicy.AudioSampleFingerprint {
        let handle = try FileHandle(forReadingFrom: fileURL)
        defer { try? handle.close() }
        let totalLength = Int64(URLSessionFullEpisodeFetcher.fileByteCount(at: fileURL))

        let head = try handle.read(upToCount: headBytes) ?? Data()

        let tailStart = max(0, totalLength - Int64(tailBytes))
        try handle.seek(toOffset: UInt64(tailStart))
        let tailCount = Int(totalLength - tailStart)
        let tail = try handle.read(upToCount: tailCount) ?? Data()

        return RediffRefetchPolicy.sampleFingerprint(head: head, tail: tail, totalLength: totalLength)
    }
}

/// Production B-side fingerprinter: decode → the EXACT xsdz.27 resample +
/// fingerprint extractor (`EpisodeFingerprintCapture.fingerprints`), so A-side
/// and B-side are fingerprinted by one versioned `(resampler + fingerprinter)`
/// unit. A plain `Sendable` struct: its `async` body runs on the generic
/// executor, NOT any serial hot actor (xsdz.29 R5 residual).
struct EpisodeCaptureBSideFingerprinter: RediffBSideFingerprinting {
    let decoder: any AudioFileDecoding

    func fingerprint(fileURL: URL) async throws -> [UInt32] {
        let mono16kHz = try await decoder.decodeMono16kHz(fileURL: fileURL)
        return EpisodeFingerprintCapture.fingerprints(mono16kHz: mono16kHz)
    }
}

/// FileManager-backed temp-file remover. Swallows a remove error (the file may
/// already be gone) but logs it so a persistent-B-copy regression is visible.
struct FileManagerTempFileRemover: RediffTempFileRemoving {
    private let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    init() {}

    func remove(_ fileURL: URL) {
        let fileManager = FileManager.default
        guard fileManager.fileExists(atPath: fileURL.path) else { return }
        do {
            try fileManager.removeItem(at: fileURL)
        } catch {
            logger.error("Failed to delete B-copy \(fileURL.lastPathComponent, privacy: .public): \(String(describing: error), privacy: .public)")
        }
    }

    /// Sweep tmp/ for `rediff-bcopy-*` files older than `age`, then the
    /// shard cache for orphaned `rediff-bside-*` decode directories (see the
    /// protocol doc). Only rediff-owned prefixes are touched — nothing else
    /// in either location is ours to delete.
    func removeOrphanedBCopies(olderThan age: TimeInterval) {
        let fileManager = FileManager.default
        guard let entries = try? fileManager.contentsOfDirectory(
            at: fileManager.temporaryDirectory,
            includingPropertiesForKeys: [.contentModificationDateKey],
            options: [.skipsSubdirectoryDescendants]
        ) else { return }
        let cutoff = Date(timeIntervalSinceNow: -age)
        for url in entries
        where url.lastPathComponent.hasPrefix(URLSessionFullEpisodeFetcher.bcopyFilenamePrefix) {
            let modified = (try? url.resourceValues(forKeys: [.contentModificationDateKey]))?
                .contentModificationDate ?? .distantPast
            guard modified < cutoff else { continue }
            do {
                try fileManager.removeItem(at: url)
                logger.info("Removed orphaned B-copy \(url.lastPathComponent, privacy: .public)")
            } catch {
                logger.error("Failed to remove orphaned B-copy \(url.lastPathComponent, privacy: .public): \(String(describing: error), privacy: .public)")
            }
        }

        // playhead-xsdz.36 (R2): the chroma fallback decodes the B-side
        // through the analysis shard cache under a synthetic
        // `rediff-bside-<uuid>` id, evicted inline on both exits — but a
        // process death in between strands the directory (~230 MB per
        // decoded hour, non-purgeable Application Support, new uuid per
        // retry). Same age floor, same per-fire cadence as the tmp sweep.
        let removedDirs = AnalysisAudioService.removeOrphanedShardCacheDirectories(
            prefix: AnalysisAudioBSideDecoder.syntheticEpisodeIDPrefix,
            olderThan: age
        )
        for name in removedDirs {
            logger.info("Removed orphaned B-side shard cache \(name, privacy: .public)")
        }
    }
}

/// Default recorder: logs each outcome (bandwidth included) at info. Production
/// persistence of the advanced `AttemptState` lands with activation (xsdz.36);
/// this keeps the flag-OFF/shadow build observable without a store dependency.
struct LoggingRediffRefetchRecorder: RediffRefetchRecording {
    private let logger = Logger(subsystem: "com.playhead", category: "RediffRefetch")

    func recordOutcome(_ outcome: RediffRefetchPolicy.Outcome) async {
        switch outcome {
        case let .skippedIneligible(assetId, reason):
            logger.info("rediff-refetch skip assetId=\(assetId, privacy: .public) reason=\(String(describing: reason), privacy: .public)")
        case let .unchanged(assetId, cost, _):
            logger.info("rediff-refetch unchanged assetId=\(assetId, privacy: .public) precheckBytes=\(cost.precheckBytes, privacy: .public)")
        case let .rotated(assetId, cost, fingerprintCount, _):
            logger.info("rediff-refetch ROTATED assetId=\(assetId, privacy: .public) precheckBytes=\(cost.precheckBytes, privacy: .public) fullFetchBytes=\(cost.fullFetchBytes, privacy: .public) fpCount=\(fingerprintCount, privacy: .public)")
        case let .failed(assetId, cost, failureClass, newState, error):
            logger.error("rediff-refetch FAILED assetId=\(assetId, privacy: .public) bytes=\(cost.totalBytes, privacy: .public) class=\(failureClass.rawValue, privacy: .public) streak=\(newState.sameClassFailureStreak, privacy: .public) error=\(error, privacy: .public)")
        case let .dayZeroMarked(assetId, cost, markCount, _):
            logger.info("rediff DAY-0 MARKED assetId=\(assetId, privacy: .public) marks=\(markCount, privacy: .public) fullFetchBytes=\(cost.fullFetchBytes, privacy: .public)")
        case let .dayZeroUnmarked(assetId, cost, error):
            logger.info("rediff DAY-0 unmarked assetId=\(assetId, privacy: .public) fullFetchBytes=\(cost.fullFetchBytes, privacy: .public) error=\(error ?? "none", privacy: .public)")
        }
    }
}
