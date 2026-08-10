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

    /// Episode duration, driven by the parent NowPlayingViewModel. Used to
    /// seed a coverage-free "mark the untranscribed tail" span that runs to
    /// the episode end (playhead-m1l9). Defaults to 0 (no tail affordance)
    /// so previews/tests that don't supply it are unaffected.
    var episodeDuration: TimeInterval = 0

    /// Trust scoring service for recording false negative signals.
    /// Injected from PlayheadRuntime at the call site.
    var trustService: TrustScoringService?

    /// Podcast ID for the current episode, used for trust signal recording.
    var podcastId: String?

    /// Callback to revert ad windows for a decoded span (gpi: "not an ad" flow).
    var onRevertAdWindows: ((DecodedSpan) async -> Bool)?

    /// Playback-context-bound writer for positive user corrections. `false`
    /// means the captured episode was replaced or persistence failed, so no
    /// trust signal may be recorded for the attempted mark.
    var onMarkAd: ((Double, Double) async -> Bool)?

    /// Phase 5 (u4d): Which decoded span's popover is currently showing.
    @State private var selectedDecodedSpan: DecodedSpan? = nil

    /// False negative marking mode: when true, tapping chunks selects/deselects them for ad marking.
    @State private var isMarkingMode = false

    /// Audio envelopes captured from chunks selected as ad content. These
    /// remain stable if polling changes the canonical row array.
    @State private var markedChunkSelections: Set<TranscriptChunkSelection> = []

    /// Confirmation alert for submitting marked chunks as false negative.
    @State private var showMarkConfirmation = false

    /// Confirmation alert for marking the untranscribed post-roll/tail as an ad.
    @State private var showTailMarkConfirmation = false

    /// Span captured when the tail affordance is tapped, submitted on confirm.
    @State private var pendingTailSpan: (start: Double, end: Double)? = nil

    /// "Not an ad" marking mode: when true, tapping chunks selects/deselects them for veto.
    @State private var isNotAdMarkingMode = false

    /// Audio envelopes captured from chunks selected as "not an ad".
    @State private var notAdMarkedChunkSelections: Set<TranscriptChunkSelection> = []

    /// Confirmation alert for submitting not-ad chunks.
    @State private var showNotAdConfirmation = false
    /// Prevents duplicate positive correction submissions and keeps their
    /// captured selections available until the durable callback succeeds.
    @State private var isSubmittingMarkAdCorrection = false
    /// Prevents duplicate taps while the orchestrator-owned correction
    /// transaction is suspended. The selected rows remain intact until that
    /// transaction succeeds so transient persistence failures are retryable.
    @State private var isSubmittingNotAdCorrection = false

    private var isSubmittingCorrection: Bool {
        isSubmittingMarkAdCorrection || isSubmittingNotAdCorrection
    }

    /// Audio envelopes represented by the current canonical projection.
    /// Watching this set lets polling retire selections whose rows were
    /// replaced with different bounds without tying selection to array offsets.
    private var visibleChunkSelections: Set<TranscriptChunkSelection> {
        Set(peekViewModel.chunks.map(TranscriptChunkSelection.init))
    }

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

            // playhead-m1l9: coverage-free affordance for a post-roll / tail
            // with no transcript under it — the chunk-selection "Mark ad" flow
            // can't reach it because there are no chunks to tap. playhead-7tn8:
            // that includes a stretch the pass DID scan and found no speech in
            // (a music-bedded ad), not just one it hasn't reached yet.
            if !peekViewModel.isLoading,
               let tailSpan = peekViewModel.untranscribedTailMarkSpan(
                   currentTime: currentTime,
                   episodeDuration: episodeDuration
               ) {
                untranscribedTailFooter(span: tailSpan)
            }
        }
        .background(AppColors.surface)
        // playhead-m1l9: the tail-mark confirmation lives on the always-present
        // body (not the conditionally-rendered footer) so it survives the
        // footer disappearing mid-interaction (e.g. playback reaching the end).
        .alert("Mark ad to the end?", isPresented: $showTailMarkConfirmation) {
            Button("Cancel", role: .cancel) {
                pendingTailSpan = nil
            }
            Button("Report missed ad", role: .destructive) {
                if let pending = pendingTailSpan {
                    submitUntranscribedTailMark(span: pending)
                }
            }
        } message: {
            Text("The untranscribed section from here to the end of the episode will be reported as an ad.")
        }
        .onChange(of: currentTime) { _, newTime in
            peekViewModel.updatePlaybackPosition(newTime)
        }
        .onChange(of: visibleChunkSelections) {
            markedChunkSelections = peekViewModel.reconciledSelections(
                markedChunkSelections
            )
            notAdMarkedChunkSelections = peekViewModel.reconciledSelections(
                notAdMarkedChunkSelections
            )
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
                if !notAdMarkedChunkSelections.isEmpty {
                    showNotAdConfirmation = true
                } else {
                    isNotAdMarkingMode = false
                }
            } else {
                // Enter not-ad mode, exit mark-ad mode if active
                isMarkingMode = false
                markedChunkSelections = []
                isNotAdMarkingMode = true
                notAdMarkedChunkSelections = []
            }
        } label: {
            HStack(spacing: 3) {
                Image(systemName: isNotAdMarkingMode
                    ? (notAdMarkedChunkSelections.isEmpty ? "xmark" : "checkmark")
                    : "hand.raised"
                )
                .font(.system(size: 11, weight: .semibold))
                Text(isNotAdMarkingMode
                    ? (notAdMarkedChunkSelections.isEmpty ? "Cancel" : "Done")
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
            }
        } message: {
            Text("\(notAdMarkedChunkSelections.count) sentence\(notAdMarkedChunkSelections.count == 1 ? "" : "s") will be marked as not an ad. Any overlapping ad detections will be dismissed.")
        }
        .disabled(isSubmittingCorrection)
    }

    var markModeToggle: some View {
        Button {
            if isMarkingMode {
                // Exiting marking mode — if chunks are selected, show confirmation
                if !markedChunkSelections.isEmpty {
                    showMarkConfirmation = true
                } else {
                    isMarkingMode = false
                }
            } else {
                // Enter mark-ad mode, exit not-ad mode if active
                isNotAdMarkingMode = false
                notAdMarkedChunkSelections = []
                isMarkingMode = true
                markedChunkSelections = []
            }
        } label: {
            HStack(spacing: 3) {
                Image(systemName: isMarkingMode
                    ? (markedChunkSelections.isEmpty ? "xmark" : "checkmark")
                    : "hand.tap"
                )
                .font(.system(size: 11, weight: .semibold))
                Text(isMarkingMode
                    ? (markedChunkSelections.isEmpty ? "Cancel" : "Done")
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
            }
        } message: {
            Text("\(markedChunkSelections.count) sentence\(markedChunkSelections.count == 1 ? "" : "s") will be reported as a missed ad.")
        }
        .disabled(isSubmittingCorrection)
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

    // MARK: Untranscribed-tail mark footer (playhead-m1l9)

    /// Pinned footer offering a coverage-free mark for a post-roll / tail with
    /// no transcript rows under it — whether the fast pass hasn't reached it
    /// yet or scanned it and found no speech (playhead-7tn8). Routes to the same
    /// `injectUserMarkedAd` path the player "Hearing an ad" button uses,
    /// seeding a span from the playhead to the episode end.
    func untranscribedTailFooter(span: (start: Double, end: Double)) -> some View {
        VStack(spacing: 0) {
            Divider()
                .foregroundStyle(AppColors.textSecondary.opacity(0.2))

            Button {
                pendingTailSpan = span
                showTailMarkConfirmation = true
            } label: {
                HStack(spacing: Spacing.sm) {
                    Rectangle()
                        .fill(AppColors.accent)
                        .frame(width: 3)
                        .frame(maxHeight: .infinity)

                    Image(systemName: "ear.fill")
                        .font(.system(size: 13, weight: .medium))
                        .foregroundStyle(AppColors.accent)

                    VStack(alignment: .leading, spacing: 1) {
                        Text("Hearing an ad past the transcript?")
                            .font(AppTypography.sans(size: 13, weight: .semibold))
                            .foregroundStyle(AppColors.textPrimary)
                        Text("Mark from here to the end as an ad")
                            .font(AppTypography.sans(size: 11, weight: .regular))
                            .foregroundStyle(AppColors.textTertiary)
                    }

                    Spacer()

                    Image(systemName: "chevron.right")
                        .font(.system(size: 11, weight: .semibold))
                        .foregroundStyle(AppColors.textTertiary.opacity(0.6))
                }
                .padding(.horizontal, Spacing.md)
                .padding(.vertical, Spacing.sm)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .frame(minHeight: 44)
            .disabled(isSubmittingCorrection)
        }
        .background(AppColors.surface)
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Mark an ad in the untranscribed section")
        .accessibilityHint("Reports the post-roll from the current position to the end of the episode as an ad")
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
                    onRevertAdWindows:
                        onRevertAdWindows ?? { _ in false },
                    onDismiss: { selectedDecodedSpan = nil }
                )
            }
        }
    }

    // MARK: Chunk Row

    func chunkRow(chunk: TranscriptChunk, index: Int) -> some View {
        let isActive = peekViewModel.activeChunkIndex == index
        let selection = TranscriptChunkSelection(chunk: chunk)

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
                    //
                    // playhead-d666 R1: the decision moved to the view model
                    // VERBATIM except that it now groups by CLAIMING spans. Kept
                    // inline it read the full overlap set, so a silenced
                    // music-only span straddling the row where a real span
                    // begins made that span look like a continuation and the
                    // whole ad region rendered with no badge at all.
                    if peekViewModel.showsAdBadge(chunkIndex: index) {
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
            // Keep the exact submitted selection immutable until the
            // orchestrator-owned transaction completes. Otherwise a user
            // could toggle rows while SQLite is suspended and either lose
            // retry state on failure or have a successful commit clear a
            // newer, unrelated selection.
            guard !isSubmittingCorrection else { return }
            if isMarkingMode {
                // Toggle selection in mark-ad mode
                if markedChunkSelections.contains(selection) {
                    markedChunkSelections.remove(selection)
                } else {
                    markedChunkSelections.insert(selection)
                }
            } else if isNotAdMarkingMode {
                // Toggle selection in not-ad mode
                if notAdMarkedChunkSelections.contains(selection) {
                    notAdMarkedChunkSelections.remove(selection)
                } else {
                    notAdMarkedChunkSelections.insert(selection)
                }
            } else if let span = primarySpan {
                selectedDecodedSpan = span
            }
        }
        // Visual selection indicator in marking mode
        .overlay(alignment: .trailing) {
            if isMarkingMode {
                Image(systemName: markedChunkSelections.contains(selection)
                    ? "checkmark.circle.fill"
                    : "circle"
                )
                .font(.system(size: 18))
                .foregroundStyle(markedChunkSelections.contains(selection)
                    ? AppColors.accent
                    : AppColors.textTertiary.opacity(0.4)
                )
                .padding(.trailing, Spacing.xs)
            } else if isNotAdMarkingMode {
                Image(systemName: notAdMarkedChunkSelections.contains(selection)
                    ? "checkmark.circle.fill"
                    : "circle"
                )
                .font(.system(size: 18))
                .foregroundStyle(notAdMarkedChunkSelections.contains(selection)
                    ? AppColors.textPrimary
                    : AppColors.textTertiary.opacity(0.4)
                )
                .padding(.trailing, Spacing.xs)
            }
        }
        .background(
            markedChunkSelections.contains(selection)
                ? AppColors.accent.opacity(0.08)
                : (notAdMarkedChunkSelections.contains(selection)
                    ? AppColors.textPrimary.opacity(0.06)
                    : Color.clear)
        )
        .animation(Motion.quick, value: isActive)
        .accessibilityElement(children: .combine)
        .accessibilityLabel(accessibilityLabel(
            chunk: chunk,
            isAd: isAd,
            overlappingSpans: overlappingSpans,
            // playhead-d666: the SAME predicate that decides the copper bar and
            // the AD badge. Without this the accessibility label announced "Ad
            // segment, detected from sustained music" on a row the fix had just
            // stopped painting — leaving a VoiceOver listener with the ad claim
            // a sighted listener no longer gets.
            makesAdClaim: isHighlighted
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
    /// `makesAdClaim` is `TranscriptPeekViewModel.isAdHighlighted` for this row
    /// — the one place that decides whether the app is asserting "this is an
    /// ad". It gates the decoded-span branch so the spoken label and the drawn
    /// one can never disagree (playhead-d666).
    ///
    /// DELIBERATELY NOT DEFAULTED. A default would have to be `true` to keep
    /// the old shape compiling, and `true` is the wrong answer — it is the bug
    /// this parameter exists to close. A new call site must say which it holds.
    func accessibilityLabel(
        chunk: TranscriptChunk,
        isAd: Bool,
        overlappingSpans: [DecodedSpan],
        makesAdClaim: Bool
    ) -> String {
        let ts = TimeFormatter.formatTime(chunk.startTime)

        // Phase 5 decoded span takes precedence for the accessibility label.
        if makesAdClaim, let span = overlappingSpans.first {
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
        guard !isSubmittingCorrection,
              !markedChunkSelections.isEmpty,
              let markAd = onMarkAd else {
            return
        }

        // Capture the exact submitted rows but retain them until the durable
        // callback succeeds so transient failure remains retryable.
        let selectedChunks = markedChunkSelections

        guard let selectedRange = peekViewModel.selectedChunkTimeRange(
            selections: selectedChunks
        ) else {
            return
        }
        let startTime = selectedRange.start
        let endTime = selectedRange.end

        let trustSvc = trustService
        let pid = podcastId
        isSubmittingMarkAdCorrection = true
        Task { @MainActor in
            defer { isSubmittingMarkAdCorrection = false }
            // The callback owns both persistence and live injection under the
            // immutable playback context captured when the sheet opened.
            guard await markAd(startTime, endTime) else { return }

            markedChunkSelections = []
            isMarkingMode = false

            // Trust follows the durable receipt; a stale/failed write must not
            // affect a show's score.
            if let pid, let trustSvc {
                await trustSvc.recordFalseNegativeSignal(podcastId: pid)
            }
        }
    }

    /// Submit a coverage-free mark for the untranscribed post-roll/tail
    /// (playhead-m1l9). Mirrors `submitMarkedChunks`' downstream calls —
    /// the bound callback commits the AdWindow/CorrectionEvent and live
    /// injection before the false-negative trust signal — but sources the span
    /// from the playhead-to-episode-end seed rather than existing chunks, so a
    /// tail with NO transcript chunks can still be marked.
    func submitUntranscribedTailMark(span: (start: Double, end: Double)) {
        let startTime = span.start
        let endTime = span.end
        guard !isSubmittingCorrection,
              startTime.isFinite,
              endTime.isFinite,
              startTime >= 0,
              endTime > startTime,
              let markAd = onMarkAd else {
            return
        }

        let trustSvc = trustService
        let pid = podcastId
        isSubmittingMarkAdCorrection = true
        Task { @MainActor in
            defer { isSubmittingMarkAdCorrection = false }
            guard await markAd(startTime, endTime) else {
                // Preserve the captured range and put the exact failed action
                // back in front of the user instead of silently losing it as
                // playback advances.
                showTailMarkConfirmation = true
                return
            }
            pendingTailSpan = nil
            if let pid, let trustSvc {
                await trustSvc.recordFalseNegativeSignal(podcastId: pid)
            }
        }
    }

    /// Submit the not-ad marked chunks through the orchestrator-owned
    /// correction transaction, using a synthetic DecodedSpan to carry the
    /// exact captured asset and range.
    func submitNotAdChunks() {
        guard !isSubmittingCorrection,
              !notAdMarkedChunkSelections.isEmpty,
              let revertCallback = onRevertAdWindows
        else {
            return
        }

        // Capture the exact selection submitted, but keep it visible until the
        // durable row+correction transaction succeeds. Clearing before the
        // callback made transient SQLite failures indistinguishable from
        // success and forced the user to reconstruct the selection.
        let selectedChunks = notAdMarkedChunkSelections

        guard let selectedRange = peekViewModel.selectedChunkTimeRange(
            selections: selectedChunks
        ) else {
            return
        }
        let startTime = selectedRange.start
        let endTime = selectedRange.end
        let assetId = peekViewModel.analysisAssetId

        // Build a synthetic DecodedSpan covering the selected range. This is
        // ONLY passed to `onRevertAdWindows`, which uses its startTime/endTime
        // to call orchestrator.revertByTimeRange. It is never persisted (that
        // path previously collapsed to scope `exactSpan:0:Int.max` — see
        // playhead-zskc).
        //
        // Format times with fixed precision to avoid floating-point
        // representation drift in the synthesized id.
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

        isSubmittingNotAdCorrection = true
        Task { @MainActor in
            defer { isSubmittingNotAdCorrection = false }
            // The callback owns the single SQLite transaction covering both
            // the CorrectionEvent and every exact AdWindow revision. Only a
            // committed action may clear the user's retry state.
            guard await revertCallback(syntheticSpan) else { return }
            notAdMarkedChunkSelections = []
            isNotAdMarkingMode = false
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
            case .classifierSeed: return "classifier"
            case .sustainedMusicOffset: return "sustained music"
            case .spliceSlot: return "audio splice"
            // playhead-6qvf: both rediff differ arms read the same to a user.
            // The byte/chroma split is a CERTAINTY distinction that governs
            // whether we may skip, not a different thing that happened, and
            // "Peace of Mind, Not Metrics" says the popover should not narrate
            // our internal confidence at them.
            case .rediffSlot, .rediffSlotChroma: return "re-download diff"
            }
        }
        return descriptions.joined(separator: ", ")
    }
}

// MARK: - Preview

// Preview requires a live AnalysisStore; use NowPlayingView preview instead.
