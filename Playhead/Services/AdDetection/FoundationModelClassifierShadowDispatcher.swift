// FoundationModelClassifierShadowDispatcher.swift
// playhead-narl.2: Live `ShadowFMDispatcher` backed by a fresh
// `FoundationModelClassifier.Runtime` session per window.
//
// Q3 decision (narl.2 continuation): independent throttled queue with
// hard concurrency = 1 (serialized by actor isolation). Per-minute
// rate-limit is enforced UPSTREAM by `ShadowCaptureCoordinator`; the
// dispatcher is the lower-level seam that owns the actual FM call and
// its wire format.
//
// Wire format:
//   - `fmResponse` is a UTF-8 JSON encoding of `ShadowFMPayload`, a small
//     Codable value type holding the window's transcript text plus the
//     FM's `RefinementWindowSchema` response.
//   - `fmModelVersion` identifies the runtime's schema surface so the
//     harness (playhead-narl.1) can version-gate per row before decoding.
//
// The dispatcher fetches transcript chunks for the window from
// `AnalysisStore.fetchTranscriptChunks(assetId:)` (filtered to the
// `[windowStart, windowEnd]` range), joins their `text` with line
// separators, and submits that as the prompt to `respondRefinement`. The
// choice of `respondRefinement` over `respondCoarse` is deliberate — the
// refinement path produces a richer span-level schema that downstream
// narl.1/3 consumers can use to evaluate what `.allEnabled` would have
// produced for a window `.default` skipped. See the bead spec for the
// shadow-decisions.jsonl contract.

import Foundation
import os
import OSLog

// MARK: - ShadowFMPayload wire format

/// Serialized payload the shadow dispatcher persists per window. Captures
/// both the FM input (transcript text in the window) and the FM output
/// (`RefinementWindowSchema`) so downstream consumers can evaluate
/// `.allEnabled` decisions without re-running the model.
///
/// The top-level shape is versioned via `schemaVersion`; bump that constant
/// alongside any non-backward-compatible change. The harness in narl.1
/// version-gates at the per-row level.
struct ShadowFMPayload: Sendable, Codable, Equatable {
    /// Version of this payload's on-disk shape. Separate from
    /// `shadowSchemaVersion` (the JSONL line shape) — the payload is nested
    /// inside the JSONL `fmResponseBase64` field, so the two versions can
    /// evolve independently.
    let payloadSchemaVersion: Int
    /// Transcript text submitted to the FM, line-separated. Captured so the
    /// harness can re-derive prompts under alternate configs without
    /// re-reading the transcript store.
    let promptText: String
    /// FM refinement response, verbatim. Optional because a FM failure
    /// (decoding failure, refusal, throttle) is still a datum worth
    /// persisting — the harness treats "FM called, returned error" as
    /// distinct from "FM was never called".
    let refinementResponse: RefinementWindowSchema?
    /// Non-nil on failure paths: a short human-readable tag such as
    /// `"runtimeUnavailable"` / `"decodingFailure"` / `"refusal"`. The
    /// dispatcher populates this when the FM call itself threw but the
    /// dispatcher still wants to persist an audit row.
    let errorTag: String?
}

/// Current `ShadowFMPayload` schema version. Increment on breaking changes.
let shadowFMPayloadSchemaVersion: Int = 1

// MARK: - LiveShadowFMDispatcher

/// Shadow FM dispatcher that serializes calls through actor isolation
/// (concurrency = 1) and creates a fresh `SessionBox` per window via
/// `FoundationModelClassifier.Runtime.makeSession()`.
///
/// Production-wired from `PlayheadRuntime`. Tests substitute a stub
/// `ShadowFMDispatcher` directly (see `ShadowCaptureCoordinatorTests`); the
/// Live dispatcher is exercised by its own focused unit tests that inject
/// a deterministic `Runtime` fake.
actor LiveShadowFMDispatcher: ShadowFMDispatcher {

    private let store: AnalysisStore
    private let runtime: FoundationModelClassifier.Runtime
    /// The prompt prefix used to prewarm the session before submitting the
    /// refinement prompt. Matches `FoundationModelClassifier`'s
    /// `refinementPromptPrefix` so the FM runtime's prewarm cache hit
    /// semantics align with the production path.
    private let prewarmPrefix: String
    /// Human-readable model identifier persisted alongside each response so
    /// downstream consumers can version-gate before decoding.
    private let modelVersion: String
    private let logger: Logger

    init(
        store: AnalysisStore,
        runtime: FoundationModelClassifier.Runtime,
        prewarmPrefix: String = "Refine ad spans.",
        modelVersion: String = "refinement.v1",
        logger: Logger = Logger(
            subsystem: "com.playhead",
            category: "ShadowFMDispatcher"
        )
    ) {
        self.store = store
        self.runtime = runtime
        self.prewarmPrefix = prewarmPrefix
        self.modelVersion = modelVersion
        self.logger = logger
    }

    // MARK: - ShadowFMDispatcher

    func dispatchShadowCall(
        assetId: String,
        window: ShadowWindow,
        configVariant: ShadowConfigVariant
    ) async throws -> ShadowFMDispatchResult {
        let promptText = try await buildPrompt(assetId: assetId, window: window)

        // Create a fresh session per window — see narl.2 Q3. The session
        // is discarded after the call returns; its confined LiveSessionActor
        // goes with it.
        let session = await runtime.makeSession()
        await session.prewarm(prewarmPrefix)

        let payload: ShadowFMPayload
        do {
            let response = try await session.respondRefinement(promptText)
            payload = ShadowFMPayload(
                payloadSchemaVersion: shadowFMPayloadSchemaVersion,
                promptText: promptText,
                refinementResponse: response,
                errorTag: nil
            )
        } catch {
            // Persist the failure as a datum — narl.1 distinguishes
            // "FM called, errored" from "FM never called" when evaluating
            // `.allEnabled`.
            let tag = Self.errorTag(for: error)
            logger.warning(
                "shadow FM refinement failed: asset=\(assetId, privacy: .public) window=\(window.start, privacy: .public)..\(window.end, privacy: .public) tag=\(tag, privacy: .public) error=\(String(describing: error), privacy: .public)"
            )
            payload = ShadowFMPayload(
                payloadSchemaVersion: shadowFMPayloadSchemaVersion,
                promptText: promptText,
                refinementResponse: nil,
                errorTag: tag
            )
        }

        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let bytes: Data
        do {
            bytes = try encoder.encode(payload)
        } catch {
            // Encoding a Codable value with all-finite numbers and plain
            // Strings shouldn't fail — but if it does, surface the error
            // rather than silently persisting empty bytes (upsert
            // validation will reject empty payloads, which is the right
            // outcome). This closes the test-contract door the AC review
            // flagged: a dispatcher that returns empty bytes would look
            // indistinguishable from a "no-op dispatcher" at the
            // persistence layer.
            throw error
        }

        return ShadowFMDispatchResult(
            fmResponse: bytes,
            fmModelVersion: modelVersion
        )
    }

    // MARK: - Prompt assembly

    private func buildPrompt(
        assetId: String,
        window: ShadowWindow
    ) async throws -> String {
        let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
        // Filter to chunks that overlap the window. "Overlap" rather than
        // "strict inside" because transcript chunks don't align to the
        // shadow window grid — a chunk that starts inside the window but
        // ends just past it still contains relevant text the FM should see.
        let overlapping = chunks.filter { chunk in
            chunk.endTime > window.start && chunk.startTime < window.end
        }
        // Join with newlines so multi-chunk windows don't smash into a
        // single blob. Blank / whitespace-only chunks are dropped.
        let lines = overlapping
            .map(\.text)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        return lines.joined(separator: "\n")
    }

    // MARK: - Error classification

    /// Map a caught error to a short wire-format tag for the payload's
    /// `errorTag` field.
    ///
    /// Strategy:
    /// 1. Prefer typed matching via `SemanticScanStatus.from(error:)` —
    ///    this understands `LanguageModelSession.GenerationError` on
    ///    iOS 26+ (refusal, decodingFailure, exceededContextWindow,
    ///    assetsUnavailable, rateLimited, guardrailViolation,
    ///    unsupportedLocale) and `CancellationError`. This is the
    ///    order-independent, framework-version-resilient path.
    /// 2. Fall back to substring matching on `String(describing: error)`
    ///    for opaque errors that don't bridge to a known typed case.
    ///    Opaque errors still need a best-effort tag so downstream
    ///    harness code can distinguish "FM errored, wire format unknown"
    ///    from "FM refused" without re-running the model.
    ///
    /// Tag strings are stable; consumers filter on them. Keep new tags
    /// additive — don't rename existing ones.
    private static func errorTag(for error: any Error) -> String {
        // Primary: typed cases via the shared status mapper.
        if let typed = Self.typedErrorTag(for: error) {
            return typed
        }
        // Fallback: substring classification for opaque errors. Order
        // still matters here — a multi-keyword error string lands on the
        // first matching branch — but this path is only reached when no
        // typed case fired, which keeps blast radius narrow.
        return Self.substringErrorTag(for: error)
    }

    /// Match the error against known typed cases. Returns nil when the
    /// error doesn't bridge to any typed case we recognize.
    private static func typedErrorTag(for error: any Error) -> String? {
        // `CancellationError` maps to .cancelled via SemanticScanStatus; we
        // surface it explicitly so the wire tag is stable even if the
        // mapper grows additional cases.
        if error is CancellationError {
            return "cancelled"
        }

        let status = SemanticScanStatus.from(error: error)
        // `.failedTransient` is the mapper's "didn't match anything typed"
        // sentinel — defer to the substring fallback rather than tagging
        // `other` here (the substring path might still pick up a useful
        // signal from the error description).
        guard status != .failedTransient else { return nil }

        switch status {
        case .refusal:
            return "refusal"
        case .decodingFailure:
            return "decodingFailure"
        case .exceededContextWindow:
            return "exceededContextWindow"
        case .unavailable, .assetsUnavailable:
            return "runtimeUnavailable"
        case .unsupportedLocale:
            return "unsupportedLocale"
        case .guardrailViolation:
            return "guardrailViolation"
        case .rateLimited:
            return "rateLimited"
        case .cancelled:
            return "cancelled"
        // Non-error statuses (.queued/.running/.success/.noAds) and
        // statuses that shouldn't arise from an error surface
        // (.thermalDeferred — that's a capability-gate decision, not a
        // throw; permissive-* variants — set by the router, not thrown)
        // fall through to nil so the substring fallback can try.
        case .queued, .running, .success, .noAds,
             .thermalDeferred, .failedTransient,
             .permissiveRefusal, .permissiveDecodingFailure,
             .permissiveContextOverflow:
            return nil
        }
    }

    /// Substring classifier for opaque errors. Kept as a fallback so
    /// errors that don't bridge to `LanguageModelSession.GenerationError`
    /// (e.g. future framework errors, swift runtime errors wrapping a
    /// description) still get a best-effort tag. Order-sensitive — the
    /// first match wins, so earlier branches should be more specific.
    private static func substringErrorTag(for error: any Error) -> String {
        let description = String(describing: error).lowercased()
        if description.contains("refusal") { return "refusal" }
        if description.contains("decoding") { return "decodingFailure" }
        if description.contains("context") { return "exceededContextWindow" }
        if description.contains("unavailable") { return "runtimeUnavailable" }
        return "other"
    }
}
