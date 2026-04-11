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

    /// Trust scoring service for recording false negative signals.
    /// Injected from PlayheadRuntime at the call site.
    var trustService: TrustScoringService?

    /// Podcast ID for the current episode, used for trust signal recording.
    var podcastId: String?

    /// Phase 5 (u4d): Which decoded span's popover is currently showing.
    @State private var selectedDecodedSpan: DecodedSpan? = nil

    /// False negative marking mode: when true, tapping chunks selects/deselects them for ad marking.
    @State private var isMarkingMode = false

    /// Indices of chunks selected as ad content in marking mode.
    @State private var markedChunkIndices: Set<Int> = []

    /// Confirmation alert for submitting marked chunks as false negative.
    @State private var showMarkConfirmation = false

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

            // Mark-as-ad mode toggle
            markModeToggle
        }
        .accessibilityElement(children: .combine)
    }

    var markModeToggle: some View {
        Button {
            if isMarkingMode {
                // Exiting marking mode — if chunks are selected, show confirmation
                if !markedChunkIndices.isEmpty {
                    showMarkConfirmation = true
                } else {
                    isMarkingMode = false
                }
            } else {
                isMarkingMode = true
                markedChunkIndices = []
            }
        } label: {
            HStack(spacing: 3) {
                Image(systemName: isMarkingMode
                    ? (markedChunkIndices.isEmpty ? "xmark" : "checkmark")
                    : "hand.tap"
                )
                .font(.system(size: 11, weight: .semibold))
                Text(isMarkingMode
                    ? (markedChunkIndices.isEmpty ? "Cancel" : "Done")
                    : "Mark ad"
                )
                .font(AppTypography.sans(size: 11, weight: .semibold))
            }
            .foregroundStyle(isMarkingMode ? AppColors.accent : AppColors.textSecondary)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(
                Capsule()
                    .fill((isMarkingMode ? AppColors.accent : AppColors.textSecondary).opacity(0.12))
            )
        }
        .frame(minWidth: 44, minHeight: 44)
        .accessibilityLabel(isMarkingMode ? "Finish marking" : "Mark sentences as ad")
        .accessibilityHint(isMarkingMode
            ? "Tap to submit selected sentences as an ad"
            : "Enter selection mode to tap sentences that are ads"
        )
        .alert("Mark as ad?", isPresented: $showMarkConfirmation) {
            Button("Cancel", role: .cancel) {
                // Stay in marking mode so user can adjust selection
            }
            Button("Report missed ad", role: .destructive) {
                submitMarkedChunks()
                isMarkingMode = false
                markedChunkIndices = []
            }
        } message: {
            Text("\(markedChunkIndices.count) sentence\(markedChunkIndices.count == 1 ? "" : "s") will be reported as a missed ad.")
        }
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
            .popover(item: $selectedDecodedSpan) { span in
                AdRegionPopover(
                    span: span,
                    correctionStore: correctionStore,
                    onDismiss: { selectedDecodedSpan = nil }
                )
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
        let overlappingSpans = peekViewModel.decodedSpansOverlapping(chunkIndex: index)
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
                        let prevSpanIds = Set(peekViewModel.decodedSpansOverlapping(
                            chunkIndex: index - 1
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
            if isMarkingMode {
                // Toggle selection in marking mode
                if markedChunkIndices.contains(index) {
                    markedChunkIndices.remove(index)
                } else {
                    markedChunkIndices.insert(index)
                }
            } else if let span = primarySpan {
                selectedDecodedSpan = span
            }
        }
        // Visual selection indicator in marking mode
        .overlay(alignment: .trailing) {
            if isMarkingMode {
                Image(systemName: markedChunkIndices.contains(index)
                    ? "checkmark.circle.fill"
                    : "circle"
                )
                .font(.system(size: 18))
                .foregroundStyle(markedChunkIndices.contains(index)
                    ? AppColors.accent
                    : AppColors.textTertiary.opacity(0.4)
                )
                .padding(.trailing, Spacing.xs)
            }
        }
        .background(
            markedChunkIndices.contains(index)
                ? AppColors.accent.opacity(0.08)
                : Color.clear
        )
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

    /// Submit the marked chunks as a false negative correction.
    /// Uses whole-asset scope (0...Int.max) because chunks don't carry atom ordinals
    /// and using chunk indices as ordinal proxies would produce semantically wrong
    /// scope data that breaks when per-span scope matching is added.
    func submitMarkedChunks() {
        guard !markedChunkIndices.isEmpty else { return }

        let assetId = peekViewModel.analysisAssetId

        let scope = CorrectionScope.exactSpan(
            assetId: assetId,
            ordinalRange: 0...Int.max
        )
        let event = CorrectionEvent(
            analysisAssetId: assetId,
            scope: scope.serialized,
            createdAt: Date().timeIntervalSince1970,
            source: .falseNegative,
            podcastId: podcastId
        )
        let trustSvc = trustService
        let pid = podcastId
        Task {
            do {
                try await correctionStore.record(event)
            } catch {
                // Best-effort — don't surface errors for corrections.
            }

            // Feed false-negative signal to TrustService (mirrors reportHearingAd).
            if let pid, let trustSvc {
                await trustSvc.recordFalseNegativeSignal(podcastId: pid)
            }
        }
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
