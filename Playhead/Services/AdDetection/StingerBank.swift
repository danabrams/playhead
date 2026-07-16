// StingerBank.swift
// playhead-l2f.6: bundled per-show stinger-anchor bank for boundary
// refinement.
//
// The bank is CURATED OFFLINE — `scripts/l2f-boundary-stinger-prototype.py
// --emit-bank` learns full-corpus per-show models from the gold ear-audit
// evaluation artifact + the retained corpus audio and writes
// `Playhead/Resources/StingerBank.json`. There is no on-device learning and
// no network fetch; the JSON ships in the app bundle and is versioned by
// `schemaVersion`.
//
// Join-key contract (the documented playhead-l2f.6 decision): production
// identifies a show inside `AdDetectionService.runBackfill` by the opaque
// `podcastId` string it receives — the podcast's RSS feed URL in the
// shipping app (`episode.podcast?.feedURL.absoluteString`, see
// `PlayheadRuntime.playEpisode`), and the corpus `showSlug` on the Catalyst
// pipeline-dump measurement path (the dump harness passes `entry.showSlug`
// as `podcastId`). Neither representation can be derived from the other at
// runtime, so every bank entry carries a `showKeys` ALIAS LIST holding both
// forms, and `entry(forShowKey:)` resolves by exact string match against
// any alias. This mirrors how every per-show store
// (`PerShowThresholdControllerStore`, `MusicBracketTrustStore`, …) keys on
// the same opaque `podcastId`.
//
// Loading follows the `PromptRedactor.loadDefault` fail-loud precedent:
// malformed JSON throws a typed error with a reason. The service-side
// consumer (`AdDetectionService.stingerBankIfEnabled`) catches, logs at
// `.error`, and degrades to "no bank" — refinement silently disabled, never
// a crash — but the loader itself never papers over a bad asset.

import Foundation

// MARK: - Errors

enum StingerBankError: Error, Equatable, CustomStringConvertible {
    /// `StingerBank.json` is not present in the bundle.
    case missingResource
    /// The resource exists but failed to decode or validate.
    case malformed(String)

    var description: String {
        switch self {
        case .missingResource:
            return "StingerBank.json missing from bundle"
        case .malformed(let reason):
            return "StingerBank.json malformed: \(reason)"
        }
    }
}

// MARK: - StingerTemplate

/// One learned stinger anchor for one side (pre = break start, post = break
/// end) of a show's ad breaks.
struct StingerTemplate: Sendable, Equatable {
    /// 50 Hz log-RMS envelope template (`log1p(rms * 100)` over 20 ms hops
    /// of 16 kHz mono PCM) — the exact transform the runtime applies to the
    /// decoded search span, so normalized cross-correlation is
    /// apples-to-apples.
    let template: [Float]
    /// Index into `template` that corresponds to the learned break edge.
    let edgeSampleIndex: Int
    /// Learned residual (seconds) between the template-mapped edge and the
    /// gold edge — corrects systematic straddle (e.g. music crossing the
    /// splice by ~1 s).
    let edgeOffsetSeconds: Double
    /// Learning confidence: the exemplar's median NCC against the show's
    /// other break clips, in (0, 1].
    let confidence: Double
    /// Number of gold breaks that support this template (exemplar +
    /// qualifying alignment targets).
    let support: Int

    /// playhead-l2f.6 acceptance gate for a refine-time match: the
    /// per-show confidence gate is `max(0.50, confidence - 0.15)`.
    var snapGate: Double { max(0.50, confidence - 0.15) }
}

// MARK: - StingerShowEntry

/// Per-show bank entry: optional pre/post stinger anchors plus an optional
/// pod-width grid.
struct StingerShowEntry: Sendable, Equatable {
    /// Alias list resolvable from production's `podcastId` (feed URL) or
    /// the corpus dump's `showSlug`. Exact-match join; see file header.
    let showKeys: [String]
    /// Human-readable show name (provenance/diagnostics only — never used
    /// for matching).
    let showName: String
    /// Anchor for the break START edge, when learned.
    let pre: StingerTemplate?
    /// Anchor for the break END edge, when learned.
    let post: StingerTemplate?
    /// Pod-width grid (seconds, e.g. 30.0) when the show's break widths
    /// cluster on multiples of it. Feeds the joint recipe's grid terms
    /// (on-grid pair bonus, off-grid inconsistency penalty, derived
    /// candidates).
    let podWidthGridSeconds: Double?
    /// playhead-xsdz.38: the show's largest observed on-grid pod multiple
    /// (morbid = 3 ⇒ pods ≤ 90 s). The joint refiner caps the grid multiple
    /// here so the pair bonus cannot stitch neighboring breaks' stingers
    /// into one super-window. `nil` = uncapped (legacy banks); only ever
    /// present alongside `podWidthGridSeconds`.
    let gridMaxPodMultiple: Int?
}

// MARK: - StingerBank

/// Decoded, validated stinger bank.
struct StingerBank: Sendable, Equatable {
    /// The bank schema version this binary understands.
    static let supportedSchemaVersion = 1
    /// Envelope frame rate the templates were learned at. The runtime
    /// envelope computation must match (see `StingerEnvelope`).
    static let requiredEnvelopeHz = 50
    /// PCM sample rate the templates were learned from. Must equal
    /// `AnalysisAudioService.targetSampleRate` so bundled templates and
    /// runtime envelopes share one acoustic space.
    static let requiredPCMSampleRate = 16_000
    /// Bundle resource name (no extension).
    static let resourceName = "StingerBank"

    let schemaVersion: Int
    let envelopeHz: Int
    let pcmSampleRate: Int
    let shows: [StingerShowEntry]

    /// Resolve the bank entry for a runtime show key (the `podcastId`
    /// `runBackfill` received). Exact match against any alias; `nil` input
    /// (unknown show) resolves to no entry.
    func entry(forShowKey key: String?) -> StingerShowEntry? {
        guard let key, !key.isEmpty else { return nil }
        return shows.first { $0.showKeys.contains(key) }
    }

    // MARK: - Loading

    /// Load and validate the bundled bank. Throws `StingerBankError` on a
    /// missing resource, undecodable JSON, or a payload that fails
    /// validation — malformed data is rejected loudly, never coerced.
    static func load(bundle: Bundle = .main) throws -> StingerBank {
        guard let url = bundle.url(
            forResource: resourceName,
            withExtension: "json"
        ) else {
            throw StingerBankError.missingResource
        }
        let data: Data
        do {
            data = try Data(contentsOf: url)
        } catch {
            throw StingerBankError.malformed("read failed: \(error.localizedDescription)")
        }
        return try decode(data)
    }

    /// Decode + validate a bank payload. Exposed separately from
    /// `load(bundle:)` so tests can feed malformed bytes directly.
    static func decode(_ data: Data) throws -> StingerBank {
        let payload: Payload
        do {
            payload = try JSONDecoder().decode(Payload.self, from: data)
        } catch {
            throw StingerBankError.malformed("decode failed: \(error)")
        }
        return try StingerBank(payload: payload)
    }

    // MARK: - Raw payload (JSON mirror)

    /// Direct JSON mirror; validation happens in `init(payload:)`.
    private struct Payload: Decodable {
        struct Side: Decodable {
            let template: [Float]
            let edgeSampleIndex: Int
            let edgeOffsetSeconds: Double
            let confidence: Double
            let support: Int
        }

        struct Show: Decodable {
            let showKeys: [String]
            let showName: String
            let pre: Side?
            let post: Side?
            let podWidthGridSeconds: Double?
            let gridMaxPodMultiple: Int?
        }

        let schemaVersion: Int
        let envelopeHz: Int
        let pcmSampleRate: Int
        let shows: [Show]
    }

    private init(payload: Payload) throws {
        guard payload.schemaVersion == Self.supportedSchemaVersion else {
            throw StingerBankError.malformed(
                "schemaVersion \(payload.schemaVersion) (expected \(Self.supportedSchemaVersion))"
            )
        }
        guard payload.envelopeHz == Self.requiredEnvelopeHz else {
            throw StingerBankError.malformed(
                "envelopeHz \(payload.envelopeHz) (expected \(Self.requiredEnvelopeHz))"
            )
        }
        guard payload.pcmSampleRate == Self.requiredPCMSampleRate else {
            throw StingerBankError.malformed(
                "pcmSampleRate \(payload.pcmSampleRate) (expected \(Self.requiredPCMSampleRate))"
            )
        }

        var seenKeys = Set<String>()
        var entries: [StingerShowEntry] = []
        entries.reserveCapacity(payload.shows.count)
        for show in payload.shows {
            guard !show.showKeys.isEmpty, show.showKeys.allSatisfy({ !$0.isEmpty }) else {
                throw StingerBankError.malformed(
                    "show \(show.showName): empty showKeys"
                )
            }
            for key in show.showKeys {
                guard seenKeys.insert(key).inserted else {
                    throw StingerBankError.malformed(
                        "duplicate showKey \(key) — entry resolution would be ambiguous"
                    )
                }
            }
            guard !show.showName.isEmpty else {
                throw StingerBankError.malformed(
                    "show with keys \(show.showKeys): empty showName"
                )
            }
            guard show.pre != nil || show.post != nil else {
                throw StingerBankError.malformed(
                    "show \(show.showName): no stinger sides — a grid alone is inert"
                )
            }
            if let grid = show.podWidthGridSeconds {
                guard grid.isFinite, grid > 0 else {
                    throw StingerBankError.malformed(
                        "show \(show.showName): podWidthGridSeconds \(grid) must be finite and > 0"
                    )
                }
            }
            if let maxMultiple = show.gridMaxPodMultiple {
                guard show.podWidthGridSeconds != nil else {
                    throw StingerBankError.malformed(
                        "show \(show.showName): gridMaxPodMultiple without podWidthGridSeconds — a multiple cap is meaningless without a grid"
                    )
                }
                guard maxMultiple >= 1 else {
                    throw StingerBankError.malformed(
                        "show \(show.showName): gridMaxPodMultiple \(maxMultiple) must be >= 1"
                    )
                }
            }
            entries.append(StingerShowEntry(
                showKeys: show.showKeys,
                showName: show.showName,
                pre: try Self.validatedTemplate(show.pre, show: show.showName, side: "pre"),
                post: try Self.validatedTemplate(show.post, show: show.showName, side: "post"),
                podWidthGridSeconds: show.podWidthGridSeconds,
                gridMaxPodMultiple: show.gridMaxPodMultiple
            ))
        }

        self.schemaVersion = payload.schemaVersion
        self.envelopeHz = payload.envelopeHz
        self.pcmSampleRate = payload.pcmSampleRate
        self.shows = entries
    }

    private static func validatedTemplate(
        _ side: Payload.Side?,
        show: String,
        side sideName: String
    ) throws -> StingerTemplate? {
        guard let side else { return nil }
        guard side.template.count >= requiredEnvelopeHz else {
            throw StingerBankError.malformed(
                "show \(show) \(sideName): template length \(side.template.count) below 1s minimum (\(requiredEnvelopeHz) frames)"
            )
        }
        guard side.template.allSatisfy(\.isFinite) else {
            throw StingerBankError.malformed(
                "show \(show) \(sideName): non-finite template value"
            )
        }
        guard (0..<side.template.count).contains(side.edgeSampleIndex) else {
            throw StingerBankError.malformed(
                "show \(show) \(sideName): edgeSampleIndex \(side.edgeSampleIndex) outside template (length \(side.template.count))"
            )
        }
        guard side.edgeOffsetSeconds.isFinite else {
            throw StingerBankError.malformed(
                "show \(show) \(sideName): non-finite edgeOffsetSeconds"
            )
        }
        guard side.confidence.isFinite, side.confidence > 0, side.confidence <= 1 else {
            throw StingerBankError.malformed(
                "show \(show) \(sideName): confidence \(side.confidence) outside (0, 1]"
            )
        }
        guard side.support >= 2 else {
            throw StingerBankError.malformed(
                "show \(show) \(sideName): support \(side.support) below minimum 2"
            )
        }
        return StingerTemplate(
            template: side.template,
            edgeSampleIndex: side.edgeSampleIndex,
            edgeOffsetSeconds: side.edgeOffsetSeconds,
            confidence: side.confidence,
            support: side.support
        )
    }
}
