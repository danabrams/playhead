// FullTranscriptView.swift
// Full-screen, scrollable, browseable transcript for an episode. Auto-
// follows the current playback position when the user is not actively
// scrolling, surfaces a "Jump to now" affordance after a manual scroll,
// supports tap-to-seek on every paragraph, and exposes an in-episode
// search bar.
//
// playhead-9u0:
//   - Reads from the existing `TranscriptPeekDataSource` boundary so
//     this UI file never imports `AnalysisStore` (the
//     SurfaceStatusUILintTests sweep forbids that on Views/).
//   - The state machine + search live in `FullTranscriptViewModel`;
//     this file is a thin presentation layer over the view-model's
//     @Observable state.
//   - Renders into a `LazyVStack` so 1000+ chunk episodes don't
//     materialise the entire transcript on first load.

import SwiftUI

// MARK: - FullTranscriptView

struct FullTranscriptView: View {

    @State private var viewModel: FullTranscriptViewModel

    /// Current playback time in seconds. The presenter updates this on
    /// every PlaybackService tick; the view coalesces sub-second
    /// changes via `lastAppliedSecond` (mirrors the peek view).
    let currentTime: TimeInterval

    /// Episode duration. Used for the seek-while-loading guard.
    var duration: TimeInterval

    /// Invoked when the user taps a paragraph or activates the seek
    /// accessibility action. Receives the paragraph's startTime.
    let onSeek: (TimeInterval) -> Void

    /// Last whole-second of `currentTime` applied to the view-model.
    /// Coalesces sub-second `onChange` fires so the view-model's
    /// active-paragraph index doesn't churn at frame rate.
    @State private var lastAppliedSecond: Int = .min

    /// Driven by the search bar's `.searchable` modifier.
    @State private var searchText: String = ""

    /// Stable id for the SwiftUI `ScrollViewReader` to scroll to.
    /// Reset whenever a new auto-scroll target is computed.
    @State private var pendingScrollTarget: String?

    init(
        viewModel: FullTranscriptViewModel,
        currentTime: TimeInterval,
        duration: TimeInterval,
        onSeek: @escaping (TimeInterval) -> Void
    ) {
        self._viewModel = State(wrappedValue: viewModel)
        self.currentTime = currentTime
        self.duration = duration
        self.onSeek = onSeek
    }

    var body: some View {
        ZStack {
            AppColors.background
                .ignoresSafeArea()

            content
        }
        .navigationTitle("Transcript")
        .navigationBarTitleDisplayMode(.inline)
        .searchable(
            text: $searchText,
            placement: .navigationBarDrawer(displayMode: .always),
            prompt: "Search this episode"
        )
        .onChange(of: searchText) { _, newValue in
            viewModel.searchQuery = newValue
        }
        .task {
            await viewModel.load()
            lastAppliedSecond = Int(currentTime)
            viewModel.updatePlaybackPosition(currentTime)
        }
        .onChange(of: currentTime) { _, newTime in
            let second = Int(newTime)
            guard second != lastAppliedSecond else { return }
            lastAppliedSecond = second
            viewModel.updatePlaybackPosition(newTime)
        }
    }

    @ViewBuilder
    private var content: some View {
        if viewModel.isLoading {
            loadingState
        } else if viewModel.paragraphs.isEmpty {
            emptyState
        } else {
            transcriptScroll
        }
    }
}

// MARK: - States

private extension FullTranscriptView {

    var loadingState: some View {
        VStack(spacing: Spacing.sm) {
            ProgressView()
                .tint(AppColors.textSecondary)
            Text("Loading transcript…")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .accessibilityLabel("Loading transcript")
    }

    var emptyState: some View {
        VStack(spacing: Spacing.sm) {
            Text("No transcript yet")
                .font(AppTypography.transcript)
                .foregroundStyle(AppColors.textTertiary)
            Text("The transcript will be available once analysis finishes for this episode.")
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textTertiary.opacity(0.7))
                .multilineTextAlignment(.center)
                .padding(.horizontal, Spacing.lg)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .accessibilityElement(children: .combine)
        .accessibilityLabel("No transcript available yet")
    }
}

// MARK: - Transcript scroll

private extension FullTranscriptView {

    var transcriptScroll: some View {
        ScrollViewReader { proxy in
            ZStack(alignment: .bottom) {
                scrollList(proxy: proxy)

                if viewModel.scrollState == .userScrolled {
                    jumpToNowButton(proxy: proxy)
                        .padding(.bottom, Spacing.lg)
                        .transition(.opacity)
                }
            }
            .animation(Motion.standard, value: viewModel.scrollState)
            // Auto-scroll: tracks the active paragraph in autoScrolling state.
            .onChange(of: viewModel.autoScrollTarget) { _, newTarget in
                guard let id = newTarget else { return }
                withAnimation(Motion.standard) {
                    proxy.scrollTo(id, anchor: .center)
                }
            }
            // Search navigation: scroll to the current match when prev/next changes it.
            .onChange(of: viewModel.currentMatchPosition) { _, _ in
                guard let position = viewModel.currentMatchPosition,
                      position < viewModel.matchingParagraphIndices.count
                else {
                    return
                }
                let paragraphIndex = viewModel.matchingParagraphIndices[position]
                guard paragraphIndex < viewModel.paragraphs.count else { return }
                let id = viewModel.paragraphs[paragraphIndex].id
                withAnimation(Motion.standard) {
                    proxy.scrollTo(id, anchor: .center)
                }
            }
        }
    }

    @ViewBuilder
    func scrollList(proxy: ScrollViewProxy) -> some View {
        ScrollView(.vertical, showsIndicators: true) {
            LazyVStack(alignment: .leading, spacing: Spacing.md) {
                if !viewModel.matchingParagraphIndices.isEmpty {
                    searchToolbar
                        .padding(.bottom, Spacing.xs)
                }

                ForEach(Array(viewModel.paragraphs.enumerated()), id: \.element.id) { index, paragraph in
                    paragraphRow(paragraph: paragraph, index: index)
                        .id(paragraph.id)
                }

                // Bottom padding so the last paragraph isn't flush.
                Color.clear.frame(height: Spacing.xxl)
            }
            .padding(.horizontal, Spacing.md)
            .padding(.top, Spacing.sm)
        }
        .scrollDismissesKeyboard(.interactively)
        // Detect drag activity. SwiftUI's ScrollView doesn't expose
        // begin/end drag callbacks directly; a simultaneous DragGesture
        // catches the user's intent. The gesture only OBSERVES — it
        // doesn't consume the drag, so native scrolling still works.
        .simultaneousGesture(
            DragGesture(minimumDistance: 1)
                .onChanged { _ in
                    if viewModel.scrollState != .userScrolling {
                        viewModel.userBeganScrolling()
                    }
                }
                .onEnded { _ in
                    viewModel.userEndedScrolling()
                }
        )
    }
}

// MARK: - Paragraph row

private extension FullTranscriptView {

    func paragraphRow(paragraph: TranscriptParagraph, index: Int) -> some View {
        let isActive = viewModel.activeParagraphIndex == index

        return HStack(alignment: .top, spacing: Spacing.xs) {
            // Active-paragraph copper border (4pt, leading edge).
            if isActive {
                Rectangle()
                    .fill(AppColors.accent)
                    .frame(width: 4)
                    .accessibilityHidden(true)
            } else {
                Color.clear.frame(width: 4)
            }

            VStack(alignment: .leading, spacing: Spacing.xxs) {
                HStack(spacing: Spacing.xs) {
                    Text(TimeFormatter.formatTime(paragraph.startTime))
                        .font(AppTypography.timestamp)
                        .foregroundStyle(isActive ? AppColors.accent : AppColors.textTertiary)

                    if paragraph.isAd {
                        Text("AD")
                            .font(AppTypography.sans(size: 10, weight: .semibold))
                            .foregroundStyle(AppColors.surface)
                            .padding(.horizontal, 5)
                            .padding(.vertical, 2)
                            .background(AppColors.accent)
                            .clipShape(Capsule())
                            .accessibilityLabel("Advertisement")
                    }
                }

                Text(paragraphAttributedText(paragraph))
                    .font(AppTypography.transcript)
                    .foregroundStyle(paragraphTextColor(paragraph: paragraph, isActive: isActive))
                    .opacity(paragraph.isAd ? 0.7 : 1.0)
                    .italic(paragraph.isAd)
                    .fixedSize(horizontal: false, vertical: true)
            }
            .padding(.vertical, Spacing.xxs)

            Spacer(minLength: 0)
        }
        .padding(.vertical, Spacing.xs)
        .background(paragraph.isAd ? AppColors.accentSubtle : Color.clear)
        .contentShape(Rectangle())
        .onTapGesture {
            if let target = viewModel.tappedParagraph(at: index) {
                onSeek(target)
            }
        }
        .accessibilityElement(children: .combine)
        .accessibilityLabel(rowAccessibilityLabel(paragraph: paragraph, isActive: isActive))
        .accessibilityHint("Tap to play from this point in the episode.")
        .accessibilityAddTraits(.isButton)
        .accessibilityAction(named: Text("Play from here")) {
            if let target = viewModel.tappedParagraph(at: index) {
                onSeek(target)
            }
        }
    }

    /// Build an `AttributedString` for the paragraph that highlights
    /// every match of the active search query (Copper background at
    /// 30% opacity per the bead spec). Falls back to plain text when
    /// there is no query.
    func paragraphAttributedText(_ paragraph: TranscriptParagraph) -> AttributedString {
        var attributed = AttributedString(paragraph.text)
        let query = viewModel.searchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else { return attributed }

        // Find every case-insensitive occurrence of the query in the
        // paragraph's text and apply a copper highlight background.
        var searchStart = paragraph.text.startIndex
        while searchStart < paragraph.text.endIndex,
              let range = paragraph.text.range(
                of: query,
                options: .caseInsensitive,
                range: searchStart..<paragraph.text.endIndex
              ) {
            if let attrRange = Range(range, in: attributed) {
                attributed[attrRange].backgroundColor = AppColors.accent.opacity(0.3)
            }
            searchStart = range.upperBound
        }
        return attributed
    }

    func paragraphTextColor(paragraph: TranscriptParagraph, isActive: Bool) -> Color {
        if paragraph.isAd {
            return AppColors.textSecondary
        }
        return isActive ? AppColors.textPrimary : AppColors.textSecondary
    }

    func rowAccessibilityLabel(paragraph: TranscriptParagraph, isActive: Bool) -> String {
        let timestamp = TimeFormatter.formatTime(paragraph.startTime)
        let prefix: String
        if paragraph.isAd {
            prefix = "Advertisement at \(timestamp)"
        } else if isActive {
            prefix = "Currently playing at \(timestamp)"
        } else {
            prefix = "At \(timestamp)"
        }
        return "\(prefix). \(paragraph.text)"
    }
}

// MARK: - Search toolbar

private extension FullTranscriptView {

    var searchToolbar: some View {
        HStack(spacing: Spacing.sm) {
            Text(viewModel.matchCountLabel)
                .font(AppTypography.mono(size: 12, weight: .medium))
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityLabel("\(viewModel.matchCountLabel) matches")

            Spacer()

            Button {
                viewModel.previousMatch()
            } label: {
                Image(systemName: "chevron.up")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundStyle(AppColors.textSecondary)
                    .frame(width: 32, height: 32)
                    .contentShape(Rectangle())
            }
            .accessibilityLabel("Previous match")

            Button {
                viewModel.nextMatch()
            } label: {
                Image(systemName: "chevron.down")
                    .font(.system(size: 14, weight: .semibold))
                    .foregroundStyle(AppColors.textSecondary)
                    .frame(width: 32, height: 32)
                    .contentShape(Rectangle())
            }
            .accessibilityLabel("Next match")
        }
        .padding(.vertical, Spacing.xs)
        .padding(.horizontal, Spacing.sm)
        .background(
            RoundedRectangle(cornerRadius: CornerRadius.medium)
                .fill(AppColors.surface)
        )
    }
}

// MARK: - Jump to now

private extension FullTranscriptView {

    func jumpToNowButton(proxy: ScrollViewProxy) -> some View {
        Button {
            if let target = viewModel.jumpToNow() {
                withAnimation(Motion.standard) {
                    proxy.scrollTo(target, anchor: .center)
                }
            } else {
                // No active paragraph (e.g. before load); just exit
                // userScrolled state.
                _ = viewModel.jumpToNow()
            }
        } label: {
            HStack(spacing: Spacing.xs) {
                Image(systemName: "arrow.down.to.line")
                    .font(.system(size: 12, weight: .semibold))
                Text("Jump to now")
                    .font(AppTypography.sans(size: 13, weight: .semibold))
            }
            .foregroundStyle(AppColors.surface)
            .padding(.horizontal, Spacing.md)
            .padding(.vertical, Spacing.xs)
            .background(
                Capsule()
                    .fill(AppColors.accent)
            )
        }
        .frame(minHeight: 44)
        .accessibilityLabel("Jump to current playback position")
        .accessibilityHint("Resumes auto-scroll for the transcript")
    }
}
