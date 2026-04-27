// PipelineProgressStripView.swift
// Debug-only one-liner that surfaces the per-episode pipeline progress
// (download / transcription / analysis) as a monospaced row. Renders
// `DL nn% · TX nn% · AN nn%` with `--%` filling any slot whose fraction
// is `nil`. The strip is gated by an `@AppStorage("debug.showPipelineStrip")`
// flag at the call site (the three Activity row views) and exposed via a
// Settings toggle, so it stays off-by-default and adds zero visual weight
// in normal use.
//
// Scope: playhead-btoa.4 (last bead in the btoa epic). Beads .1–.3 plumbed
// the three optional `Double` fields end-to-end (`AnalysisStore` watermarks
// for transcript/analysis, `DownloadManager.progressSnapshot()` for
// download); this view is the visible piece.
//
// Design intent (docs/plans/2026-04-27-activity-pipeline-debug-strip-design.md):
//   * One mono line, three numbers — no bars, no animation.
//   * Font is mono-11 (one tick smaller than the rows' mono-12) so the
//     strip visually subordinates to the row's primary copy.
//   * Color is `AppColors.textSecondary`, matching the canonical
//     "secondary status line" treatment.
//   * Top padding 2pt only — column-aligned with the row's text on the
//     leading edge.
//
// The static `format(_:)` entry point is what the formatter tests pin —
// keeping the formatter independent of `View` construction means the
// test suite can run without booting SwiftUI.

import SwiftUI

// MARK: - PipelineProgressStripView

/// Debug pipeline-progress strip rendered under Activity rows when
/// `@AppStorage("debug.showPipelineStrip")` is `true`. Pure presentation:
/// the caller hands in the three optional fractions already plumbed onto
/// the row struct.
struct PipelineProgressStripView: View {

    /// Bytes-downloaded fraction `[0, 1]`; `nil` when no in-flight
    /// download is recorded for this episode this refresh.
    let downloadFraction: Double?

    /// Fast-transcript watermark / duration fraction `[0, 1]`; `nil`
    /// when watermark or duration is unknown.
    let transcriptFraction: Double?

    /// Confirmed-ad watermark / duration fraction `[0, 1]`; `nil` when
    /// watermark or duration is unknown.
    let analysisFraction: Double?

    /// Section identifier used to disambiguate the accessibility ID
    /// per Activity section. Expected values: `"now"`, `"upNext"`,
    /// `"paused"`. Keeping this a plain `String` (rather than an enum)
    /// matches the lightweight call-site shape the design doc specifies.
    let sectionId: String

    var body: some View {
        Text(line)
            .font(AppTypography.mono(size: 11, weight: .regular))
            .foregroundStyle(AppColors.textSecondary)
            // 2pt literal: the design doc pinned this exact value to
            // sit one tick under the row's primary copy. The Spacing
            // tokens start at `xxs = 4`, so no token matches.
            .padding(.top, 2)
            .accessibilityIdentifier("ActivityView.\(sectionId).pipelineStrip")
    }

    /// Rendered one-liner. Centralised here so the body stays a single
    /// `Text(line)` and the formatter can be exercised directly.
    private var line: String {
        "DL \(Self.format(downloadFraction)) · TX \(Self.format(transcriptFraction)) · AN \(Self.format(analysisFraction))"
    }

    /// Formatter contract pinned by `PipelineProgressStripViewTests`:
    ///   * `nil` → `"--%"` (the "we don't know" sentinel).
    ///   * Known fractions are clamped to `[0, 1]`, scaled to whole
    ///     percent, and rendered without leading zero padding.
    /// Static so the test suite can call it without constructing a
    /// SwiftUI `View`.
    static func format(_ fraction: Double?) -> String {
        guard let fraction else { return "--%" }
        let clamped = min(1.0, max(0.0, fraction))
        return "\(Int((clamped * 100).rounded()))%"
    }
}

// MARK: - Previews

#Preview("Pipeline strip — populated") {
    PipelineProgressStripView(
        downloadFraction: 1.0,
        transcriptFraction: 0.87,
        analysisFraction: 0.64,
        sectionId: "now"
    )
    .padding()
    .preferredColorScheme(.dark)
}

#Preview("Pipeline strip — partial (download known, others nil)") {
    PipelineProgressStripView(
        downloadFraction: 0.42,
        transcriptFraction: nil,
        analysisFraction: nil,
        sectionId: "upNext"
    )
    .padding()
    .preferredColorScheme(.dark)
}
