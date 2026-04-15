// AdRegionPopover.swift
// Phase 5 (playhead-u4d): Tap-to-explain popover for detected ad spans.
//
// Shows which upstream signals caused the span to be flagged, and surfaces
// the "This isn't an ad" gesture (wired to NoOpUserCorrectionStore in Phase 5;
// Phase 7 will replace with a persistent store).

import SwiftUI

// MARK: - AdRegionPopover

struct AdRegionPopover: View {

    let span: DecodedSpan
    let correctionStore: any UserCorrectionStore
    /// Callback to revert overlapping ad windows in the SkipOrchestrator.
    /// Injected by the caller; defaults to no-op for backward compatibility.
    var onRevertAdWindows: (DecodedSpan) async -> Void = { _ in }
    var onDismiss: () -> Void = {}

    @State private var showVetoConfirmation = false

    var body: some View {
        VStack(alignment: .leading, spacing: Spacing.sm) {
            header
            Divider()
                .foregroundStyle(AppColors.textTertiary.opacity(0.2))
            provenanceList
            Divider()
                .foregroundStyle(AppColors.textTertiary.opacity(0.2))
            notAnAdButton
        }
        .padding(Spacing.md)
        .frame(minWidth: 260, maxWidth: 340)
        .background(AppColors.surfaceElevated)
        .cornerRadius(12)
        .alert("Mark as not an ad?", isPresented: $showVetoConfirmation) {
            Button("Cancel", role: .cancel) {}
            Button("Confirm", role: .destructive) {
                Task {
                    await correctionStore.recordVeto(span: span)
                    // Revert overlapping ad windows in the orchestrator so the
                    // skip cue is removed and the timeline updates immediately.
                    await onRevertAdWindows(span)
                    onDismiss()
                }
            }
        } message: {
            Text("This will stop flagging this segment as an ad.")
        }
    }

    // MARK: - Subviews

    private var header: some View {
        VStack(alignment: .leading, spacing: Spacing.xxs) {
            HStack {
                Text("AD SEGMENT")
                    .font(AppTypography.sans(size: 10, weight: .semibold))
                    .foregroundStyle(AppColors.accent)
                    .tracking(1.0)
                Spacer()
                Text(durationLabel)
                    .font(AppTypography.mono(size: 10, weight: .medium))
                    .foregroundStyle(AppColors.textTertiary)
            }
            Text(timeRangeLabel)
                .font(AppTypography.mono(size: 11, weight: .regular))
                .foregroundStyle(AppColors.textSecondary)
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Ad segment from \(TimeFormatter.formatTime(span.startTime)) to \(TimeFormatter.formatTime(span.endTime)), \(durationLabel)")
    }

    private var provenanceList: some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            Text("DETECTED FROM")
                .font(AppTypography.sans(size: 10, weight: .semibold))
                .foregroundStyle(AppColors.textTertiary)
                .tracking(0.8)

            if span.anchorProvenance.isEmpty {
                Text("No specific signals available")
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textTertiary)
            } else {
                ForEach(Array(span.anchorProvenance.enumerated()), id: \.offset) { _, ref in
                    provenanceRow(for: ref)
                }
            }
        }
    }

    private var notAnAdButton: some View {
        Button {
            showVetoConfirmation = true
        } label: {
            HStack {
                Image(systemName: "hand.thumbsdown")
                    .font(.system(size: 13))
                Text("This isn't an ad")
                    .font(AppTypography.sans(size: 14, weight: .medium))
            }
            .foregroundStyle(AppColors.textSecondary)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .accessibilityLabel("Mark this segment as not an ad")
    }

    @ViewBuilder
    private func provenanceRow(for ref: AnchorRef) -> some View {
        HStack(alignment: .top, spacing: Spacing.xs) {
            Image(systemName: provenanceIcon(ref))
                .font(.system(size: 11))
                .foregroundStyle(AppColors.accent)
                .frame(width: 16)

            Text(provenanceDescription(ref))
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
                .fixedSize(horizontal: false, vertical: true)
        }
    }

    // MARK: - Helpers

    private var durationLabel: String {
        let secs = Int(span.duration.rounded())
        if secs >= 60 {
            return "\(secs / 60)m \(secs % 60)s"
        }
        return "\(secs)s"
    }

    private var timeRangeLabel: String {
        "\(TimeFormatter.formatTime(span.startTime)) – \(TimeFormatter.formatTime(span.endTime))"
    }

    private func provenanceIcon(_ ref: AnchorRef) -> String {
        switch ref {
        case .fmConsensus: return "brain.head.profile"
        case .evidenceCatalog: return "text.magnifyingglass"
        case .fmAcousticCorroborated: return "waveform.badge.magnifyingglass"
        case .userCorrection: return "hand.tap"
        }
    }

    private func provenanceDescription(_ ref: AnchorRef) -> String {
        switch ref {
        case .fmConsensus(_, let strength):
            let pct = Int((strength * 100).rounded())
            return "Foundation model consensus (\(pct)% strength)"
        case .evidenceCatalog(let entry):
            switch entry.category {
            case .url:
                return "URL: \"\(entry.matchedText)\""
            case .promoCode:
                return "Promo code: \"\(entry.matchedText)\""
            case .disclosurePhrase:
                return "Disclosure: \"\(entry.matchedText)\""
            case .ctaPhrase:
                return "Call to action: \"\(entry.matchedText)\""
            case .brandSpan:
                return "Brand mention: \"\(entry.matchedText)\""
            }
        case .fmAcousticCorroborated(_, let strength):
            let pct = Int((strength * 100).rounded())
            return "FM + acoustic break (\(pct)% break strength)"
        case .userCorrection:
            return "User-reported ad"
        }
    }
}
