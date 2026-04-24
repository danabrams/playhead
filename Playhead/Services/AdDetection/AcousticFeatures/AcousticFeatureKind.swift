// AcousticFeatureKind.swift
// playhead-gtt9.12: Per-feature identity for the acoustic evidence expansion.
//
// Each case is one transcript-independent signal feeding the fusion pipeline.
// Pair with `AcousticFeatureFunnel` to track each feature's per-episode lifecycle
// (computed / produced-signal / passed-gate / included-in-fusion) — gtt9.12
// acceptance criterion #1.
//
// Pure value type; safe to pass across actor boundaries.

import Foundation

/// Identity of an acoustic feature contributing to ad evidence fusion.
enum AcousticFeatureKind: String, Sendable, Hashable, CaseIterable {
    /// Music bed probability. Pre-existing via `MusicBedClassifier`; this case
    /// lets the funnel track it alongside the new features.
    case musicBed
    /// Rolling-window LUFS delta vs. show-long baseline.
    case lufsShift
    /// Short-window crest-factor delta (dynamic range / compression signature).
    case dynamicRange
    /// Lightweight speaker-embedding shift across the window boundary.
    /// Today uses the speaker-change proxy; real embedding is gtt9.3/future work.
    case speakerShift
    /// Cepstral / MFCC distance between adjacent windows.
    case spectralShift
    /// Run of low-energy frames at plausible bumper boundary durations.
    case silenceBoundary
    /// Repetition fingerprint lookup against the shared ad catalog.
    /// Stubbed until gtt9.13's `AcousticFingerprint` / `AdCatalogStore` land.
    case repetitionFingerprint
    /// Onset density / rough BPM estimate.
    case tempoOnset
}
