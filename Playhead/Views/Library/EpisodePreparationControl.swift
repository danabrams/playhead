// EpisodePreparationControl.swift
// playhead-3xtw: the per-episode "Download & Analyze on demand" control.
//
// Layer 3 — a THIN SwiftUI surface. All input-gathering + derivation is
// centralized in the list-level `EpisodePreparationStatusModel` (one batch
// of queries per refresh, shared across rows); this view only renders the
// pure `EpisodePreparationReadiness` it is handed and routes a tap to the
// model's (playback-free) prepare trigger. It renders a calm ✦ / ✓ glyph
// at rest, or a two-zone "readiness" bar (download zone → analyze zone)
// with a functional caption while working.

import SwiftUI

struct EpisodePreparationControl: View {

    let episode: Episode
    /// Shared, list-owned status model. Reads are cheap (dictionary
    /// lookups); the model publishes changes via `@Observable`.
    let model: EpisodePreparationStatusModel

    private var episodeId: String { episode.canonicalEpisodeKey }
    private var readiness: EpisodePreparationReadiness { model.readiness(for: episodeId) }

    var body: some View {
        Button {
            // NEVER starts playback — routes through the model's
            // playback-free preparation coordinator only.
            Task { await model.prepare(episode) }
        } label: {
            content
                .animation(Motion.quick, value: readiness)
        }
        .buttonStyle(.plain)
        .disabled(!model.isActionable(for: episodeId))
        .accessibilityLabel(accessibilityLabel)
        .accessibilityValue(accessibilityValue)
        .accessibilityIdentifier("EpisodeRow.preparationControl")
    }

    // MARK: - Rendering

    @ViewBuilder
    private var content: some View {
        switch readiness.state {
        case .idle:
            // The settled "prepare" glyph is the literal ✦ (a Text glyph,
            // not the `sparkle` SF Symbol — that symbol name is banned by
            // the Quiet Instrument design-token sweep).
            Text("\u{2726}")
                .font(.system(size: 14, weight: .regular))
                .foregroundStyle(AppColors.textSecondary)
                .frame(width: 22, height: 22)
                .contentShape(Rectangle())
        case .ready:
            glyph(systemName: "checkmark.circle", color: Palette.mutedSage)
        case .waitingForWifi:
            workingBar(showWifiWaiting: true)
        case .downloading, .analyzing:
            workingBar(showWifiWaiting: false)
        }
    }

    private func glyph(systemName: String, color: Color) -> some View {
        Image(systemName: systemName)
            .font(.system(size: 13, weight: .medium))
            .foregroundStyle(color)
            .frame(width: 22, height: 22)
            .contentShape(Rectangle())
    }

    /// The one segmented "readiness" bar: a download zone (fills first),
    /// a small divider gap at the handoff, then an analyze zone, with a
    /// functional caption underneath.
    private func workingBar(showWifiWaiting: Bool) -> some View {
        VStack(alignment: .trailing, spacing: 3) {
            if showWifiWaiting {
                Image(systemName: "wifi.slash")
                    .font(.system(size: 12, weight: .medium))
                    .foregroundStyle(AppColors.textSecondary)
                    .frame(height: Self.barHeight)
            } else {
                HStack(spacing: Self.dividerGap) {
                    zone(fill: readiness.downloadFraction)
                    zone(fill: readiness.analysisFraction)
                }
                .frame(height: Self.barHeight)
            }

            if let caption = episodePreparationCaption(readiness) {
                Text(caption)
                    .font(AppTypography.mono(size: 10, weight: .medium))
                    .foregroundStyle(AppColors.textTertiary)
                    .lineLimit(1)
            }
        }
        .contentShape(Rectangle())
    }

    /// A single zone of the readiness bar: a quiet track with a copper
    /// fill. Matches the inline playback progress-bar idiom used elsewhere
    /// in the row (track = textSecondary @20%, fill = accent).
    private func zone(fill: Double) -> some View {
        ZStack(alignment: .leading) {
            Capsule()
                .fill(AppColors.textSecondary.opacity(0.2))
                .frame(width: Self.zoneWidth, height: Self.barHeight)
            Capsule()
                .fill(AppColors.accent)
                .frame(width: Self.zoneWidth * min(1, max(0, fill)), height: Self.barHeight)
        }
        .frame(width: Self.zoneWidth, height: Self.barHeight)
    }

    private static let zoneWidth: CGFloat = 26
    private static let barHeight: CGFloat = 3
    private static let dividerGap: CGFloat = 4

    // MARK: - Accessibility

    private var accessibilityLabel: String {
        switch readiness.state {
        case .idle:           return "Download and analyze"
        case .waitingForWifi: return "Waiting for Wi‑Fi to download"
        case .downloading:    return "Downloading"
        case .analyzing:      return "Analyzing"
        case .ready:          return "Analysis ready"
        }
    }

    private var accessibilityValue: String {
        episodePreparationCaption(readiness) ?? ""
    }
}
