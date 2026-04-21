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

    /// Callback to revert ad windows for a decoded span (gpi: "not an ad" flow).
    var onRevertAdWindows: ((DecodedSpan) async -> Void)?

    /// Runtime reference for injecting user-marked ad corrections.
    /// Optional so existing callers (previews, tests) don't need to provide it.
    var runtime: PlayheadRuntime?

    /// Phase 5 (u4d): Which decoded span's popover is currently showing.
    @State private var selectedDecodedSpan: DecodedSpan? = nil

    /// False negative marking mode: when true, tapping chunks selects/deselects them for ad marking.
    @State private var isMarkingMode = false

    /// Indices of chunks selected as ad content in marking mode.
    @State private var markedChunkIndices: Set<Int> = []

    /// Confirmation alert for submitting marked chunks as false negative.
    @State private var showMarkConfirmation = false

    /// "Not an ad" marking mode: when true, tapping chunks selects/deselects them for veto.
    @State private var isNotAdMarkingMode = false

    /// Indices of chunks selected as "not an ad" in not-ad marking mode.
    @State private var notAdMarkedChunkIndices: Set<Int> = []

    /// Confirmation alert for submitting not-ad chunks.
    @State private var showNotAdConfirmation = false

    /// Last whole-second of `currentTime` applied to the view model. Used to
    /// coalesce sub-second `onChange` fires that would otherwise trigger
    /// same-frame state updates (SwiftUI warns; chunk boundaries are seconds-scale).
    @State private var lastAppliedSecond: Int = .min

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
            let second = Int(newTime)
            guard second != lastAppliedSecond else { return }
            lastAppliedSecond = second
            peekViewModel.updatePlaybackPosition(newTime)
        }
        .onAppear {
            peekViewModel.startPolling()
            lastAppliedSecond = Int(currentTime)
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

            // "Not an ad" mode toggle
            notAdModeToggle

            // Mark-as-ad mode toggle
            markModeToggle
        }
        .accessibilityElement(children: .combine)
    }

    var notAdModeToggle: some View {
        Button {
            if isNotAdMarkingMode {
                // Exiting not-ad mode — if chunks are selected, show confirmation
                if !notAdMarkedChunkIndices.isEmpty {
                    showNotAdConfirmation = true
                } else {
                    isNotAdMarkingMode = false
                }
            } else {
                // Enter not-ad mode, exit mark-ad mode if active
                isMarkingMode = false
                markedChunkIndices = []
                isNotAdMarkingMode = true
                notAdMarkedChunkIndices = []
            }
        } label: {
            HStack(spacing: 3) {
                Image(systemName: isNotAdMarkingMode
                    ? (notAdMarkedChunkIndices.isEmpty ? "xmark" : "checkmark")
                    : "hand.raised"
                )
                .font(.system(size: 11, weight: .semibold))
                Text(isNotAdMarkingMode
                    ? (notAdMarkedChunkIndices.isEmpty ? "Cancel" : "Done")
                    : "Not ad"
                )
                .font(AppTypography.sans(size: 11, weight: .semibold))
            }
            .foregroundStyle(isNotAdMarkingMode ? AppColors.textPrimary : AppColors.textSecondary)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(
                Capsule()
                    .fill((isNotAdMarkingMode ? AppColors.textPrimary : AppColors.textSecondary).opacity(0.12))
            )
        }
        .frame(minWidth: 44, minHeight: 44)
        .accessibilityLabel(isNotAdMarkingMode ? "Finish not-ad marking" : "Mark sentences as not an ad")
        .accessibilityHint(isNotAdMarkingMode
            ? "Tap to submit selected sentences as not an ad"
            : "Enter selection mode to tap sentences that are not ads"
        )
        .alert("Mark as not an ad?", isPresented: $showNotAdConfirmation) {
            Button("Cancel", role: .cancel) {
                // Stay in marking mode so user can adjust selection
            }
            Button("Dismiss ad", role: .destructive) {
                submitNotAdChunks()
                isNotAdMarkingMode = false
            }
        } message: {
            Text("\(notAdMarkedChunkIndices.count) sentence\(notAdMarkedChunkIndices.count == 1 ? "" : "s") will be marked as not an ad. Any overlapping ad detections will be dismissed.")
        }
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
                // Enter mark-ad mode, exit not-ad mode if active
                isNotAdMarkingMode = false
                notAdMarkedChunkIndices = []
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
        // Unified highlight: decoded spans OR user-marked AdWindows
        let isHighlighted = peekViewModel.isAdHighlighted(chunkIndex: index)
        // Use the first overlapping span for the popover tap target
        let primarySpan = overlappingSpans.first

        return HStack(alignment: .top, spacing: 0) {
            // Left-edge accent bar for ad regions (decoded spans or user-marked)
            // 3pt wide × full row height, Copper color
            if isHighlighted {
                Rectangle()
                    .fill(AppColors.accent)
                    .frame(width: 3)
            }

            HStack(alignment: .top, spacing: Spacing.xs) {
                // Legacy Copper accent bar for active chunk (z-order above ad bar)
                if isActive && !isHighlighted {
                    RoundedRectangle(cornerRadius: 1.5)
                        .fill(AppColors.accent)
                        .frame(width: 3)
                        .transition(.opacity)
                }

                VStack(alignment: .leading, spacing: Spacing.xxs) {
                    // AD badge on the first chunk of a decoded span, or on the
                    // first chunk of a user-marked ad region.
                    let isFirstChunkOfSpan: Bool = {
                        guard isHighlighted else { return false }
                        guard index > 0 else { return true }
                        // For decoded spans: check if previous chunk shares span IDs
                        if isDecodedAd {
                            let prevSpanIds = Set(peekViewModel.decodedSpansOverlapping(
                                chunkIndex: index - 1
                            ).map(\.id))
                            let currentSpanIds = Set(overlappingSpans.map(\.id))
                            return currentSpanIds.isDisjoint(with: prevSpanIds)
                        }
                        // For user-marked: show badge if previous chunk is not highlighted
                        return !peekViewModel.isAdHighlighted(chunkIndex: index - 1)
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
                .padding(.leading, (isActive && !isHighlighted) ? 0 : Spacing.xs)
            }
            .padding(.vertical, Spacing.xxs)
            .padding(.leading, (isActive || isHighlighted) ? 0 : 3 + Spacing.xs)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        // Background tint for ad rows (decoded spans or user-marked)
        .background(isHighlighted ? AppColors.accentSubtle : Color.clear)
        .contentShape(Rectangle())
        .onTapGesture {
            if isMarkingMode {
                // Toggle selection in mark-ad mode
                if markedChunkIndices.contains(index) {
                    markedChunkIndices.remove(index)
                } else {
                    markedChunkIndices.insert(index)
                }
            } else if isNotAdMarkingMode {
                // Toggle selection in not-ad mode
                if notAdMarkedChunkIndices.contains(index) {
                    notAdMarkedChunkIndices.remove(index)
                } else {
                    notAdMarkedChunkIndices.insert(index)
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
            } else if isNotAdMarkingMode {
                Image(systemName: notAdMarkedChunkIndices.contains(index)
                    ? "checkmark.circle.fill"
                    : "circle"
                )
                .font(.system(size: 18))
                .foregroundStyle(notAdMarkedChunkIndices.contains(index)
                    ? AppColors.textPrimary
                    : AppColors.textTertiary.opacity(0.4)
                )
                .padding(.trailing, Spacing.xs)
            }
        }
        .background(
            markedChunkIndices.contains(index)
                ? AppColors.accent.opacity(0.08)
                : (notAdMarkedChunkIndices.contains(index)
                    ? AppColors.textPrimary.opacity(0.06)
                    : Color.clear)
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
    ///
    /// playhead-98q: in addition to recording the CorrectionEvent, now also
    /// injects the selected chunk range into the skip orchestrator for
    /// immediate skip + UI update + persistence. The chunks already carry
    /// startTime/endTime, so no BoundaryExpander is needed.
    func submitMarkedChunks() {
        guard !markedChunkIndices.isEmpty else { return }

        // Clear selection immediately to prevent duplicate submissions
        // if the alert action fires more than once.
        let selectedIndices = markedChunkIndices
        markedChunkIndices = []

        let chunks = peekViewModel.chunks
        let selectedChunks = selectedIndices.compactMap { idx -> TranscriptChunk? in
            guard idx < chunks.count else { return nil }
            return chunks[idx]
        }
        guard !selectedChunks.isEmpty else { return }

        // Derive the time range from the selected chunks' min start / max end.
        let startTime = selectedChunks.map(\.startTime).min() ?? 0
        let endTime = selectedChunks.map(\.endTime).max() ?? 0

        let trustSvc = trustService
        let pid = podcastId
        let runtimeRef = runtime
        Task {
            // Inject the user-marked ad region for immediate skip + persistence.
            // PlayheadRuntime.injectUserMarkedAd handles both the orchestrator
            // injection and the AdWindow + CorrectionEvent persistence.
            if let runtimeRef {
                await runtimeRef.injectUserMarkedAd(start: startTime, end: endTime)
            }

            // Feed false-negative signal to TrustService (mirrors reportHearingAd).
            if let pid, let trustSvc {
                await trustSvc.recordFalseNegativeSignal(podcastId: pid)
            }
        }
    }

    /// Submit the not-ad marked chunks as a manual veto correction.
    /// Records a .manualVeto CorrectionEvent and calls onRevertAdWindows
    /// with a synthetic DecodedSpan covering the selected time range.
    func submitNotAdChunks() {
        guard !notAdMarkedChunkIndices.isEmpty else { return }

        // Capture and clear selection immediately.
        let selectedIndices = notAdMarkedChunkIndices
        notAdMarkedChunkIndices = []

        let chunks = peekViewModel.chunks
        let selectedChunks = selectedIndices.compactMap { idx -> TranscriptChunk? in
            guard idx < chunks.count else { return nil }
            return chunks[idx]
        }
        guard !selectedChunks.isEmpty else { return }

        let startTime = selectedChunks.map(\.startTime).min() ?? 0
        let endTime = selectedChunks.map(\.endTime).max() ?? 0
        let assetId = peekViewModel.analysisAssetId

        // Build a synthetic DecodedSpan covering the selected range.
        // Use a unique ID per veto to avoid collisions when the same asset
        // has multiple not-ad corrections at different time ranges.
        // Format times with fixed precision to avoid floating-point representation drift.
        let vetoId = String(format: "%@-veto-%.3f-%.3f", assetId, startTime, endTime)
        let syntheticSpan = DecodedSpan(
            id: vetoId,
            assetId: assetId,
            firstAtomOrdinal: 0,
            lastAtomOrdinal: Int.max,
            startTime: startTime,
            endTime: endTime,
            anchorProvenance: []
        )

        let trustSvc = trustService
        let pid = podcastId
        let revertCallback = onRevertAdWindows
        let store = correctionStore
        Task {
            // recordVeto persists a CorrectionEvent internally (exactSpan scope
            // + optional sponsorOnShow scope), so no separate store.record() call
            // is needed — that was causing a double-write inflating correction factors.
            await store.recordVeto(span: syntheticSpan)

            // Revert overlapping ad windows via the orchestrator callback.
            if let revertCallback {
                await revertCallback(syntheticSpan)
            }

            // Feed false-positive (false skip) signal to TrustService.
            if let pid, let trustSvc {
                await trustSvc.recordFalseSkipSignal(podcastId: pid)
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
            case .userCorrection: return "user-reported ad"
            }
        }
        return descriptions.joined(separator: ", ")
    }
}

// MARK: - Preview

// Preview requires a live AnalysisStore; use NowPlayingView preview instead.
