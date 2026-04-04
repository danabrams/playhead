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

    var body: some View {
        VStack(spacing: 0) {
            grabHandle
                .padding(.top, Spacing.sm)
                .padding(.bottom, Spacing.xs)

            headerBar
                .padding(.horizontal, Spacing.md)
                .padding(.bottom, Spacing.sm)

            Divider()
                .foregroundStyle(AppColors.secondary.opacity(0.2))

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
            .fill(AppColors.secondary.opacity(0.3))
            .frame(width: 36, height: 5)
            .accessibilityHidden(true)
    }

    // MARK: Header

    var headerBar: some View {
        HStack {
            Text("TRANSCRIPT")
                .font(AppTypography.sans(size: 11, weight: .semibold))
                .foregroundStyle(AppColors.metadata)
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
                .tint(AppColors.secondary)
                .accessibilityLabel("Preparing transcript")
            Text("Preparing transcript…")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.metadata)
            Text("Downloading and analyzing audio")
                .font(AppTypography.sans(size: 11, weight: .regular))
                .foregroundStyle(AppColors.metadata.opacity(0.6))
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
                .foregroundStyle(AppColors.metadata)
            Text("Transcript will appear as the episode plays.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.metadata.opacity(0.7))
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
        let isAd = peekViewModel.isAdSegment(
            startTime: chunk.startTime,
            endTime: chunk.endTime
        )
        let adScore = peekViewModel.adConfidence(
            startTime: chunk.startTime,
            endTime: chunk.endTime
        )

        return HStack(alignment: .top, spacing: Spacing.xs) {
            // Copper accent bar for active chunk
            if isActive {
                RoundedRectangle(cornerRadius: 1.5)
                    .fill(AppColors.accent)
                    .frame(width: 3)
                    .transition(.opacity)
            }

            VStack(alignment: .leading, spacing: Spacing.xxs) {
                // Timestamp + ad score debug badge
                HStack(spacing: Spacing.xs) {
                    Text(TimeFormatter.formatTime(chunk.startTime))
                        .font(AppTypography.mono(size: 10, weight: .medium))
                        .foregroundStyle(
                            isActive ? AppColors.accent : AppColors.metadata
                        )

                    if let score = adScore {
                        Text(String(format: "AD %.0f%%", score * 100))
                            .font(AppTypography.mono(size: 9, weight: .bold))
                            .foregroundStyle(.red)
                    }
                }

                // Transcript text
                Text(chunk.text)
                    .font(AppTypography.transcript)
                    .foregroundStyle(chunkTextColor(isActive: isActive, isAd: isAd))
                    .opacity(isAd ? 0.45 : 1.0)
                    .italic(isAd)
            }
        }
        .padding(.vertical, Spacing.xxs)
        .padding(.leading, isActive ? 0 : 3 + Spacing.xs) // Align text regardless of bar
        .animation(Motion.quick, value: isActive)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(isAd ? "Ad segment at \(TimeFormatter.formatTime(chunk.startTime)): \(chunk.text)" : "\(TimeFormatter.formatTime(chunk.startTime)): \(chunk.text)")
    }

    // MARK: Helpers

    func chunkTextColor(isActive: Bool, isAd: Bool) -> Color {
        if isAd {
            return AppColors.metadata
        }
        return isActive ? AppColors.text : AppColors.secondary
    }
}

// MARK: - Preview

// Preview requires a live AnalysisStore; use NowPlayingView preview instead.
