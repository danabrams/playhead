// TranscriptPeekView.swift
// Pull-up sheet showing live transcript in serif type. Current segment
// highlighted with Copper, ad segments visually recessed. Auto-scrolls
// to track playback position.
//
// This is a preview of v0.2 full transcript view — read-only, no seek,
// no search. Just a peek.

import SwiftUI

// MARK: - TranscriptPeekView

struct TranscriptPeekView: View {

    @State var peekViewModel: TranscriptPeekViewModel

    /// Current playback time, driven by the parent NowPlayingViewModel.
    let currentTime: TimeInterval

    /// Phase 5 (u4d): User correction store for "This isn't an ad" gesture.
    /// Defaults to no-op; Phase 7 injects a real store via PlayheadRuntime.
    var correctionStore: any UserCorrectionStore = NoOpUserCorrectionStore()

    /// Phase 5 (u4d): Which decoded span's popover is currently showing.
    @State private var selectedDecodedSpan: DecodedSpan? = nil

    var body: some View {
        VStack(spacing: 0) {
            grabHandle
                .padding(.top, Spacing.sm)
                .padding(.bottom, Spacing.xs)

            headerBar
                .padding(.horizontal, Spacing.md)
                .padding(.bottom, Spacing.sm)

            Divider()
                .foregroundStyle(AppColors.textSecondary.opacity(0.2))

            if peekViewModel.isLoading {
                loadingState
            } else if peekViewModel.chunks.isEmpty {
                emptyState
            } else {
                transcriptScroll
            }
        }
        .background(AppColors.surface)
        .onChange(of: currentTime) { _, newTime in
            peekViewModel.updatePlaybackPosition(newTime)
        }
        .onAppear {
            peekViewModel.startPolling()
            peekViewModel.updatePlaybackPosition(currentTime)
        }
        .onDisappear {
            peekViewModel.stopPolling()
        }
    }
}

// MARK: - Subviews

private extension TranscriptPeekView {

    // MARK: Grab Handle

    var grabHandle: some View {
        Capsule()
            .fill(AppColors.textSecondary.opacity(0.3))
            .frame(width: 36, height: 5)
            .accessibilityHidden(true)
    }

    // MARK: Header

    var headerBar: some View {
        HStack {
            Text("TRANSCRIPT")
                .font(AppTypography.sans(size: 11, weight: .semibold))
                .foregroundStyle(AppColors.textTertiary)
                .tracking(1.4)

            Spacer()

            // Live indicator when chunks are still arriving
            if peekViewModel.chunks.contains(where: { $0.pass == "fast" }) {
                liveIndicator
            }
        }
        .accessibilityElement(children: .combine)
    }

    var liveIndicator: some View {
        HStack(spacing: Spacing.xxs) {
            Circle()
                .fill(AppColors.accent)
                .frame(width: 6, height: 6)

            Text("LIVE")
                .font(AppTypography.sans(size: 10, weight: .semibold))
                .foregroundStyle(AppColors.accent)
                .tracking(0.8)
        }
        .accessibilityLabel("Live transcript updating")
    }

    // MARK: Loading

    var loadingState: some View {
        VStack(spacing: Spacing.sm) {
            Spacer()
            ProgressView()
                .tint(AppColors.textSecondary)
                .accessibilityLabel("Preparing transcript")
            Text("Preparing transcript…")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
            Text("Downloading and analyzing audio")
                .font(AppTypography.sans(size: 11, weight: .regular))
                .foregroundStyle(AppColors.textTertiary.opacity(0.6))
            Spacer()
        }
        .frame(maxWidth: .infinity)
    }

    // MARK: Empty

    var emptyState: some View {
        VStack(spacing: Spacing.sm) {
            Spacer()
            Text("No transcript yet")
                .font(AppTypography.transcript)
                .foregroundStyle(AppColors.textTertiary)
            Text("Transcript will appear as the episode plays.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary.opacity(0.7))
                .multilineTextAlignment(.center)
            Spacer()
        }
        .frame(maxWidth: .infinity)
        .padding(.horizontal, Spacing.lg)
    }

    // MARK: Transcript Scroll

    var transcriptScroll: some View {
        ScrollViewReader { proxy in
            ScrollView(.vertical, showsIndicators: false) {
                LazyVStack(alignment: .leading, spacing: Spacing.xs) {
                    ForEach(
                        Array(peekViewModel.chunks.enumerated()),
                        id: \.element.id
                    ) { index, chunk in
                        chunkRow(chunk: chunk, index: index)
                            .id(chunk.id)
                    }

                    // Debug stats for TestFlight diagnostics
                    Text(peekViewModel.debugStats)
                        .font(AppTypography.mono(size: 9, weight: .medium))
                        .foregroundStyle(AppColors.textTertiary.opacity(0.5))
                        .frame(maxWidth: .infinity, alignment: .center)
                        .padding(.top, Spacing.sm)

                    // Bottom padding so the last chunk isn't flush with edge
                    Color.clear.frame(height: Spacing.xxl)
                }
                .padding(.horizontal, Spacing.md)
                .padding(.top, Spacing.sm)
            }
            .onChange(of: peekViewModel.activeChunkIndex) { _, newIndex in
                guard let idx = newIndex, idx < peekViewModel.chunks.count else { return }
                let targetId = peekViewModel.chunks[idx].id
                withAnimation(Motion.standard) {
                    proxy.scrollTo(targetId, anchor: .center)
                }
            }
        }
    }

    // MARK: Chunk Row

    func chunkRow(chunk: TranscriptChunk, index: Int) -> some View {
        let isActive = peekViewModel.activeChunkIndex == index

        // Legacy Phase 2 ad detection
        let isAd = peekViewModel.isAdSegment(
            startTime: chunk.startTime,
            endTime: chunk.endTime
        )
        let adScore = peekViewModel.adConfidence(
            startTime: chunk.startTime,
            endTime: chunk.endTime
        )

        // Phase 5 decoded spans overlapping this chunk
        let overlappingSpans = peekViewModel.decodedSpansOverlapping(
            startTime: chunk.startTime,
            endTime: chunk.endTime
        )
        let isDecodedAd = !overlappingSpans.isEmpty
        // Use the first overlapping span for the popover tap target
        let primarySpan = overlappingSpans.first

        return HStack(alignment: .top, spacing: 0) {
            // Phase 5: Left-edge accent bar for decoded ad spans (z-order below active bar)
            // 3pt wide × full row height, Copper color
            if isDecodedAd {
                Rectangle()
                    .fill(AppColors.accent)
                    .frame(width: 3)
            }

            HStack(alignment: .top, spacing: Spacing.xs) {
                // Legacy Copper accent bar for active chunk (z-order above decoded-ad bar)
                if isActive && !isDecodedAd {
                    RoundedRectangle(cornerRadius: 1.5)
                        .fill(AppColors.accent)
                        .frame(width: 3)
                        .transition(.opacity)
                }

                VStack(alignment: .leading, spacing: Spacing.xxs) {
                    // Phase 5: AD badge only on the FIRST chunk of a decoded span.
                    // A chunk is the first of its span when no previous chunk shares any
                    // of the same overlapping span IDs.
                    let isFirstChunkOfSpan: Bool = {
                        guard isDecodedAd else { return false }
                        guard index > 0 else { return true }
                        let prevChunk = peekViewModel.chunks[index - 1]
                        let prevSpanIds = Set(peekViewModel.decodedSpansOverlapping(
                            startTime: prevChunk.startTime,
                            endTime: prevChunk.endTime
                        ).map(\.id))
                        let currentSpanIds = Set(overlappingSpans.map(\.id))
                        return currentSpanIds.isDisjoint(with: prevSpanIds)
                    }()
                    if isFirstChunkOfSpan {
                        HStack(spacing: Spacing.xxs) {
                            Text("AD")
                                .font(AppTypography.sans(size: 10, weight: .semibold))
                                .foregroundStyle(AppColors.surface)
                                .padding(.horizontal, 5)
                                .padding(.vertical, 2)
                                .background(AppColors.accent)
                                .clipShape(Capsule())
                            Spacer()
                        }
                    }

                    // Timestamp (with ad score debug suffix when detected)
                    Text(timestampLabel(chunk: chunk, adScore: adScore))
                        .font(AppTypography.mono(size: 10, weight: .medium))
                        .foregroundStyle(
                            adScore != nil ? .red : (isActive ? AppColors.accent : AppColors.textTertiary)
                        )

                    // Transcript text
                    Text(chunk.text)
                        .font(AppTypography.transcript)
                        .foregroundStyle(chunkTextColor(isActive: isActive, isAd: isAd))
                        .opacity(isAd ? 0.45 : 1.0)
                        .italic(isAd)
                }
                .padding(.leading, (isActive && !isDecodedAd) ? 0 : Spacing.xs)
            }
            .padding(.vertical, Spacing.xxs)
            .padding(.leading, (isActive || isDecodedAd) ? 0 : 3 + Spacing.xs)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        // Phase 5: Background tint for decoded ad rows
        .background(isDecodedAd ? AppColors.accentSubtle : Color.clear)
        .contentShape(Rectangle())
        .onTapGesture {
            if let span = primarySpan {
                selectedDecodedSpan = span
            }
        }
        .popover(item: Binding(
            get: { selectedDecodedSpan.flatMap { s in overlappingSpans.first(where: { $0.id == s.id }) } },
            set: { selectedDecodedSpan = $0 }
        )) { span in
            AdRegionPopover(
                span: span,
                correctionStore: correctionStore,
                onDismiss: { selectedDecodedSpan = nil }
            )
        }
        .animation(Motion.quick, value: isActive)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(accessibilityLabel(
            chunk: chunk,
            isAd: isAd,
            overlappingSpans: overlappingSpans
        ))
    }

    // MARK: Helpers

    func chunkTextColor(isActive: Bool, isAd: Bool) -> Color {
        if isAd {
            return AppColors.textTertiary
        }
        return isActive ? AppColors.textPrimary : AppColors.textSecondary
    }

    func timestampLabel(chunk: TranscriptChunk, adScore: Double?) -> String {
        let ts = TimeFormatter.formatTime(chunk.startTime)
        guard let score = adScore else { return ts }
        return ts + String(format: " AD %.0f%%", score * 100)
    }

    /// Phase 5 (u4d): Accessibility label that includes decoded span info when present.
    func accessibilityLabel(
        chunk: TranscriptChunk,
        isAd: Bool,
        overlappingSpans: [DecodedSpan]
    ) -> String {
        let ts = TimeFormatter.formatTime(chunk.startTime)

        // Phase 5 decoded span takes precedence for the accessibility label.
        if let span = overlappingSpans.first {
            let secs = Int(span.duration.rounded())
            let provenanceSummary = provenanceSummary(span.anchorProvenance)
            return "Ad segment, \(secs) seconds, detected from \(provenanceSummary). \(ts): \(chunk.text)"
        }

        // Legacy ad window label.
        if isAd {
            return "Ad segment at \(ts): \(chunk.text)"
        }

        return "\(ts): \(chunk.text)"
    }

    private func provenanceSummary(_ refs: [AnchorRef]) -> String {
        if refs.isEmpty { return "unknown signals" }
        let descriptions = refs.prefix(3).map { ref -> String in
            switch ref {
            case .fmConsensus: return "FM consensus"
            case .evidenceCatalog(let e): return e.category.rawValue
            case .fmAcousticCorroborated: return "FM and acoustic break"
            }
        }
        return descriptions.joined(separator: ", ")
    }
}

// MARK: - Preview

// Preview requires a live AnalysisStore; use NowPlayingView preview instead.
