// SpecialistScanResult.swift
// playhead-b6jq PR 4 (Phase B2): the persistence row for a single distilled
// specialist host-read verdict over one candidate window.
//
// # What this is
//
// PR 4 adds a background SCAN phase (`BackfillJobPhase.specialistHostReadScan`)
// that runs the on-device specialist over candidate windows during backfill and
// persists the RAW verdict — nothing more. This value type mirrors
// `SemanticScanResult` (the FM scan row) but carries only what the specialist
// emits: `probabilityOfAd` (P(ad)) and `isAd` (raw P>=0.5), plus the identity /
// reuse-key fields that let a re-scan collapse onto the same row.
//
// # Acts on nothing
//
// PR 4 PERSISTS these rows and does nothing else. There is no τ=0.7 threshold,
// no mark/banner composition, and no auto-skip wiring here — those are PR 5's
// job (they consume `specialist_scan_results`). `adClass` stays `"hostRead"`;
// auto-skip remains deterministic-only. The stored `probabilityOfAd` is the
// already-clamped `SpecialistVerdict.confidence` (the `0...1` invariant lives on
// `SpecialistVerdict`, not here) — persist it verbatim.

import Foundation

/// One persisted specialist verdict for one transcript window.
///
/// `Sendable, Equatable` value type so the runner can build it off the store
/// actor and tests can assert field-for-field round-trips. Maps 1:1 to a
/// `specialist_scan_results` row.
struct SpecialistScanResult: Sendable, Equatable {
    /// Stable primary-key id for the row (deterministic from the reuse key so a
    /// re-scan reproduces the same id).
    let id: String
    /// Owning analysis asset (`analysis_assets.id`, FK ON DELETE CASCADE).
    let analysisAssetId: String
    /// Window start time in episode audio seconds.
    let windowStartTime: Double
    /// Window end time in episode audio seconds.
    let windowEndTime: Double
    /// `SpecialistVerdict.confidence` — the model's calibrated P(ad) in `0...1`.
    /// Persisted verbatim (already clamped by `SpecialistVerdict.init`). RAW —
    /// no τ threshold applied (that is PR 5).
    let probabilityOfAd: Double
    /// `SpecialistVerdict.isAd` — the raw `P(ad) >= 0.5` decision. RAW; NOT an
    /// auto-skip eligibility signal (auto-skip stays deterministic-only).
    let isAd: Bool
    /// Coarse class label from the verdict. `"hostRead"` for the live runtime;
    /// `nil` when the model emits none.
    let adClass: String?
    /// Model identity (`SpecialistModelResources.modelFolderName`). Part of the
    /// reuse-key domain so a model bump yields a fresh row.
    let modelVersion: String
    /// Detector version (`AdDetectionConfig.detectorVersion`). Part of the
    /// reuse-key domain.
    let detectorVersion: String
    /// Transcript version this scan was computed against. Part of the reuse-key
    /// domain so a transcript regen yields a fresh row (mirrors semantic).
    let transcriptVersion: String
    /// Canonical scan cohort JSON (mirrors `SemanticScanResult.scanCohortJSON`).
    /// Part of the reuse-key domain.
    let scanCohortJSON: String
    /// SHA-256 idempotency key over the reuse domain (see
    /// `AnalysisStore.specialistScanReuseKeyHash`). `UNIQUE(reuseKeyHash)` +
    /// `INSERT OR REPLACE` bounds row growth and makes re-runs idempotent.
    let reuseKeyHash: String
    /// Originating backfill phase (`BackfillJobPhase.specialistHostReadScan.rawValue`).
    /// Mirrors the semantic table's `jobPhase` discriminator.
    let jobPhase: String
    /// Wall-clock UNIX seconds at persist time.
    let createdAt: Double
}
