// SpecialistShadowDispatcher.swift
// playhead-dsbc (Phase B1): shadow plumbing for the distilled specialist ad
// classifier. Mirrors `FoundationModelClassifierShadowDispatcher`.
//
// # Role
//
// Runs the specialist classifier alongside FM for a window, records the
// verdict, and ACTS ON NOTHING. There is no wire from this dispatcher into
// the production fusion / gate / policy path — a shadow call is
// side-effect-free on the live pipeline. The only side effect is the injected
// `record` sink (default: a structured `os_log` line).
//
// # No model in B1
//
// The dispatcher holds an OPTIONAL `SpecialistAdClassifier.Runtime`. In B1 it
// is injected `nil`, so `dispatchShadowCall` is fully inert (records nothing,
// returns `nil`). Phase B2 (phone-gated) supplies a `CoreAILanguageModel`-
// backed runtime; this file stays model-agnostic and takes no CoreAI dependency.
//
// # No persistence in B1
//
// Unlike the FM shadow dispatcher (which upserts a `ShadowFMResponse` row),
// this dispatcher does NOT touch the store on the write side — the `record`
// sink IS the seam. Store persistence / migration is deferred to a later
// phase. The dispatcher only READS transcript chunks to assemble the prompt,
// reusing the FM dispatcher's overlap-filter approach.

import Foundation
import os
import OSLog

// MARK: - SpecialistShadowPayload wire format

/// Serialized payload the shadow dispatcher hands to its `record` sink per
/// window. Captures both the classifier input (`promptText`) and the output
/// (`verdict`, or `errorTag` on failure) so a downstream harness can evaluate
/// the specialist's marks without re-running the model.
///
/// Versioned via `payloadSchemaVersion`; bump the constant below alongside any
/// non-backward-compatible change.
struct SpecialistShadowPayload: Sendable, Codable, Equatable {
    /// Version of this payload's shape. Increment on breaking changes.
    let payloadSchemaVersion: Int
    /// Transcript text submitted to the classifier, line-separated. Captured
    /// so a harness can re-derive prompts under alternate configs without
    /// re-reading the transcript store.
    let promptText: String
    /// The classifier's verdict, verbatim. `nil` on the failure path — a
    /// classify throw is still a datum worth recording (the harness treats
    /// "classifier called, errored" as distinct from "never called").
    let verdict: SpecialistVerdict?
    /// Non-nil on failure paths: a short human-readable tag such as
    /// `"runtimeUnavailable"` / `"decodingFailure"` / `"cancelled"`. Mirrors
    /// the FM dispatcher's error-tag contract with a simpler, model-agnostic
    /// tag set.
    let errorTag: String?
}

/// Current `SpecialistShadowPayload` schema version. Increment on breaking
/// changes.
let specialistShadowPayloadSchemaVersion: Int = 1

// MARK: - SpecialistShadowDispatcher protocol

/// Dispatches one shadow specialist call for a window under a given config
/// variant, returning the recorded payload (or `nil` when inert — no runtime
/// supplied).
///
/// Implementations MUST NOT participate in production gate decisions or write
/// back into the production fusion / gate / policy path — shadow calls are
/// side-effect-free on the live pipeline.
///
/// The live implementation (`LiveSpecialistShadowDispatcher`) bridges to a
/// `SpecialistAdClassifier.Runtime`; tests inject a stub runtime that returns
/// synthetic verdicts. Mirrors `ShadowFMDispatcher`.
protocol SpecialistShadowDispatcher: Sendable {
    func dispatchShadowCall(
        assetId: String,
        window: ShadowWindow,
        configVariant: ShadowConfigVariant
    ) async throws -> SpecialistShadowPayload?
}

// MARK: - LiveSpecialistShadowDispatcher

/// Shadow specialist dispatcher that serializes calls through actor isolation
/// (concurrency = 1) and creates a fresh session per window via
/// `SpecialistAdClassifier.Runtime.makeSession()`.
///
/// Fully inert when constructed with `runtime: nil` (the Phase B1 default) —
/// `dispatchShadowCall` records nothing and returns `nil`. Once a runtime is
/// supplied it builds the prompt from the window's overlapping transcript
/// chunks, calls `session.classify`, and hands a `SpecialistShadowPayload`
/// to the `record` sink. It NEVER mutates ad windows or decisions.
actor LiveSpecialistShadowDispatcher: SpecialistShadowDispatcher {

    private let store: AnalysisStore
    /// Optional so B1 can wire the dispatcher fully inert. `nil` ⇒ every
    /// `dispatchShadowCall` is a no-op (records nothing, returns `nil`).
    /// Phase B2 injects a `CoreAILanguageModel`-backed runtime.
    private let runtime: SpecialistAdClassifier.Runtime?
    /// The sole side-effect seam. Defaults to a structured `os_log` line; a
    /// test injects an in-memory recorder. B1 has no store persistence.
    private let record: @Sendable (SpecialistShadowPayload) -> Void
    private let logger: Logger

    init(
        store: AnalysisStore,
        runtime: SpecialistAdClassifier.Runtime?,
        record: (@Sendable (SpecialistShadowPayload) -> Void)? = nil,
        logger: Logger = Logger(
            subsystem: "com.playhead",
            category: "SpecialistShadowDispatcher"
        )
    ) {
        self.store = store
        self.runtime = runtime
        self.logger = logger
        if let record {
            self.record = record
        } else {
            // Default sink: a structured os_log line. `Logger` is Sendable,
            // so the closure captures a copy rather than reaching back into
            // the actor's isolated `logger`.
            let sinkLogger = logger
            self.record = { payload in
                // Compose plain Strings so the log line has no dependence on
                // os.Logger's per-type interpolation overloads.
                let isAd = payload.verdict.map { String($0.isAd) } ?? "nil"
                let confidence = payload.verdict.map { String(format: "%.3f", $0.confidence) } ?? "nil"
                let adClass = payload.verdict?.adClass ?? "nil"
                let tag = payload.errorTag ?? "none"
                sinkLogger.info(
                    "specialist shadow: isAd=\(isAd, privacy: .public) confidence=\(confidence, privacy: .public) class=\(adClass, privacy: .public) tag=\(tag, privacy: .public)"
                )
            }
        }
    }

    // MARK: - SpecialistShadowDispatcher

    func dispatchShadowCall(
        assetId: String,
        window: ShadowWindow,
        configVariant: ShadowConfigVariant
    ) async throws -> SpecialistShadowPayload? {
        // Inert until a runtime is supplied (Phase B2 wires the live one).
        // No prompt assembly, no store read, no record — byte-identical to
        // "the specialist never ran".
        guard let runtime else { return nil }

        let promptText = try await buildPrompt(assetId: assetId, window: window)

        // Fresh session per window — mirrors the FM shadow dispatcher's
        // per-window session lifetime. The session is discarded after the
        // call returns.
        let session = await runtime.makeSession()

        let payload: SpecialistShadowPayload
        do {
            let verdict = try await session.classify(promptText)
            payload = SpecialistShadowPayload(
                payloadSchemaVersion: specialistShadowPayloadSchemaVersion,
                promptText: promptText,
                verdict: verdict,
                errorTag: nil
            )
        } catch {
            // Record the failure as a datum — a harness distinguishes
            // "classifier called, errored" from "never called".
            let tag = Self.errorTag(for: error)
            logger.warning(
                "specialist shadow classify failed: asset=\(assetId, privacy: .public) window=\(window.start, privacy: .public)..\(window.end, privacy: .public) tag=\(tag, privacy: .public) error=\(String(describing: error), privacy: .public)"
            )
            payload = SpecialistShadowPayload(
                payloadSchemaVersion: specialistShadowPayloadSchemaVersion,
                promptText: promptText,
                verdict: nil,
                errorTag: tag
            )
        }

        // The ONLY side effect. Never mutates ad windows or decisions.
        record(payload)
        return payload
    }

    // MARK: - Prompt assembly

    /// Assemble the classifier prompt from the transcript chunks overlapping
    /// the window. Reuses `LiveShadowFMDispatcher.buildPrompt`'s approach:
    /// "overlap" rather than "strict inside" (chunks don't align to the
    /// window grid), joined with newlines, blank chunks dropped.
    private func buildPrompt(
        assetId: String,
        window: ShadowWindow
    ) async throws -> String {
        let chunks = try await store.fetchTranscriptChunks(assetId: assetId)
        let overlapping = chunks.filter { chunk in
            chunk.endTime > window.start && chunk.startTime < window.end
        }
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
    /// Kept deliberately simpler than the FM dispatcher's classifier: the
    /// specialist seam is model-agnostic, so it does not couple to
    /// FoundationModels' `GenerationError` taxonomy. `CancellationError` is
    /// matched typed (it has no useful description); everything else falls to
    /// an order-sensitive substring match. Tag strings are stable — consumers
    /// filter on them; keep new tags additive.
    private static func errorTag(for error: any Error) -> String {
        if error is CancellationError { return "cancelled" }
        let description = String(describing: error).lowercased()
        if description.contains("refusal") { return "refusal" }
        if description.contains("decoding") { return "decodingFailure" }
        if description.contains("context") { return "exceededContextWindow" }
        if description.contains("unavailable") { return "runtimeUnavailable" }
        return "other"
    }
}
