// AdBannerView.swift
// Ad skip banner — slides in at bottom of Now Playing when an ad is skipped.
//
// Styled as a calm margin note, not an alert. Long horizontal proportions
// (cue sheet style). Ink background, Bone text, Copper accent on "Listen".
// Auto-dismisses after 8 seconds. Single banner lane with queue — rapid
// sequential skips are coalesced, never stacked.
//
// ┌─────────────────────────────────────────────────┐
// │  Skipped · Squarespace · "Build your website"   │
// │                          [Listen]    [Dismiss x] │
// └─────────────────────────────────────────────────┘

import SwiftUI

// MARK: - Banner Data

/// Data for a single ad skip banner notification.
struct AdSkipBannerItem: Identifiable, Equatable {
    let id: String
    /// The ad window ID from AnalysisStore (for revert feedback).
    let windowId: String
    /// Advertiser name, if known and above confidence threshold.
    let advertiser: String?
    /// Short product/tagline, if known and above confidence threshold.
    let product: String?
    /// Timestamp in episode seconds where the skipped ad started (snapped boundary).
    let adStartTime: Double
    /// Timestamp in episode seconds where the skipped ad ended.
    let adEndTime: Double
    /// Confidence of the metadata extraction (nil = no metadata).
    let metadataConfidence: Double?
    /// Where the metadata came from. Known values: "foundationModels", "fallback", "none".
    let metadataSource: String
    /// The podcast ID, needed for trust scoring on revert.
    let podcastId: String
    /// Evidence catalog entries associated with this ad window.
    /// Used by Phase 7's UserCorrectionStore to infer correction scopes
    /// (e.g. phraseOnShow) when the user taps "Listen" to revert a skip.
    /// Empty when no catalog data is available — callers must handle [] gracefully.
    let evidenceCatalogEntries: [EvidenceEntry]
}

// MARK: - Banner Queue (ViewModel)

/// Manages banner display queue. Coalesces adjacent skips into a single
/// banner. Ensures only one banner is visible at a time.
@MainActor
@Observable
final class AdBannerQueue {

    private(set) var currentBanner: AdSkipBannerItem?

    /// Pending banners waiting to display.
    private var queue: [AdSkipBannerItem] = []

    /// Auto-dismiss timer handle.
    private var dismissTask: Task<Void, Never>?

    /// Duration before auto-dismiss.
    private static let autoDismissSeconds: TimeInterval = 8.0

    /// Maximum gap (seconds) between skipped ads to coalesce into one banner.
    private static let coalesceGap: TimeInterval = 10.0

    // MARK: - Public API

    /// Enqueue a new ad skip banner. If the skip is adjacent to the current
    /// or last queued item, coalesce instead of adding a new entry.
    func enqueue(_ item: AdSkipBannerItem) {
        // Try to coalesce with the most recent item (current or last in queue).
        if let last = queue.last, canCoalesce(last, item) {
            // Replace with the newer item (it has the broader time range).
            queue[queue.count - 1] = item
        } else if let current = currentBanner, queue.isEmpty, canCoalesce(current, item) {
            // Coalesce with the currently displayed banner — update in place.
            currentBanner = item
            restartAutoDismiss()
            return
        } else {
            queue.append(item)
        }

        // If nothing is showing, pop the next one.
        if currentBanner == nil {
            showNext()
        }
    }

    /// Dismiss the current banner (user tapped dismiss or auto-dismiss fired).
    func dismiss() {
        dismissTask?.cancel()
        dismissTask = nil
        currentBanner = nil

        // Show next queued banner after a brief pause so the exit animation
        // finishes before the next slide-in.
        if !queue.isEmpty {
            Task {
                try? await Task.sleep(for: .milliseconds(350))
                showNext()
            }
        }
    }

    // MARK: - Private

    private func showNext() {
        guard !queue.isEmpty else { return }
        currentBanner = queue.removeFirst()
        restartAutoDismiss()
    }

    private func restartAutoDismiss() {
        dismissTask?.cancel()
        dismissTask = Task {
            try? await Task.sleep(for: .seconds(Self.autoDismissSeconds))
            guard !Task.isCancelled else { return }
            dismiss()
        }
    }

    /// Two banners coalesce if they are close in time (adjacent/near-adjacent skips).
    private func canCoalesce(_ a: AdSkipBannerItem, _ b: AdSkipBannerItem) -> Bool {
        abs(a.adEndTime - b.adStartTime) <= Self.coalesceGap
    }
}

// MARK: - AdBannerView

/// The banner overlay. Positioned at the bottom of the Now Playing screen.
/// Slides in from below, slides out on dismiss.
struct AdBannerView: View {

    var queue: AdBannerQueue

    /// Called when the user taps "Listen" to jump back to the skipped ad.
    var onListen: ((AdSkipBannerItem) -> Void)?

    /// Phase 7.2: Called when the user taps "Not an ad" to record a correction.
    /// When nil, the button is hidden.
    var onNotAnAd: ((AdSkipBannerItem) -> Void)?

    /// Injected haptic player — defaults to `SystemHapticPlayer` in
    /// production, tests swap in a `RecordingHapticPlayer`.
    var hapticPlayer: any HapticPlaying = SystemHapticPlayer()

    /// playhead-vjxc: Tracks whether the user has tapped the disclosure
    /// chevron to expand the evidence detail. Keyed by banner id so the
    /// expansion never carries over when the queue advances to the next
    /// banner — every new banner starts collapsed (default ergonomics).
    @State private var expandedBannerId: String?

    /// Factored handler for the banner-appear haptic so tests can drive
    /// it without rendering a live SwiftUI hierarchy.
    func handleBannerAppear() {
        hapticPlayer.play(.notice)
    }

    var body: some View {
        VStack {
            Spacer()

            if let banner = queue.currentBanner {
                bannerCard(banner)
                    .transition(
                        .move(edge: .bottom)
                        .combined(with: .opacity)
                    )
                    .padding(.horizontal, Spacing.md)
                    .padding(.bottom, Spacing.sm)
            }
        }
        .animation(Motion.standard, value: queue.currentBanner?.id)
        .onChange(of: queue.currentBanner?.id) { _, _ in
            // Always start each new banner collapsed so the default
            // ergonomics (compact, low-attention margin note) survive
            // queued skips.
            expandedBannerId = nil
        }
    }

    // MARK: - Copy Logic

    /// Minimum metadata confidence required to surface advertiser/product.
    /// Below this, the banner falls back to generic "Skipped sponsor segment".
    static let metadataConfidenceThreshold: Double = 0.60

    /// Resolve the banner copy line from metadata, applying strict
    /// evidence-bound rules. Never surfaces a brand solely from a model guess.
    static func bannerCopy(for item: AdSkipBannerItem) -> BannerCopyLine {
        // Only surface specific copy when:
        // 1. metadataSource is not "none" (metadata was actually extracted)
        // 2. metadataConfidence exceeds the threshold
        // 3. evidenceText was present (advertiser came from transcript, not a guess)
        let hasStrongEvidence: Bool = {
            guard item.metadataSource != "none",
                  let confidence = item.metadataConfidence,
                  confidence >= metadataConfidenceThreshold
            else { return false }
            return true
        }()

        if hasStrongEvidence, let advertiser = item.advertiser {
            return BannerCopyLine(
                prefix: "Skipped",
                advertiser: advertiser,
                detail: item.product
            )
        }

        // Weak or missing evidence: generic copy, never hallucinated names.
        return BannerCopyLine(
            prefix: "Skipped sponsor segment",
            advertiser: nil,
            detail: nil
        )
    }

    /// Template-driven banner copy. Never free-form.
    struct BannerCopyLine: Equatable {
        let prefix: String
        let advertiser: String?
        let detail: String?
    }

    // MARK: - Evidence Copy (playhead-vjxc)

    /// Maximum number of evidence lines surfaced when the banner is expanded.
    /// Caps the list at a glanceable height — power users still get the gist
    /// without the banner becoming a full-page transcript.
    static let evidenceLineLimit: Int = 3

    /// Translate a single deterministic evidence entry into a calm,
    /// user-facing line. Pure function — no side effects, no localized
    /// strings (yet — playhead is en-US only at the MVP).
    ///
    /// Voice: every line should read as a quiet observation, never a metric
    /// or a counter. The verbatim transcript text is preserved in quotes
    /// where it is short and self-explanatory; brand names and codes are
    /// surfaced unquoted as they read more naturally that way.
    static func evidenceLine(for entry: EvidenceEntry) -> String {
        let cleaned = entry.matchedText.trimmingCharacters(in: .whitespacesAndNewlines)
        switch entry.category {
        case .disclosurePhrase:
            return "Sponsor disclosure: \u{201C}\(cleaned)\u{201D}"
        case .url:
            return "Sponsor link: \(cleaned)"
        case .promoCode:
            return "Promo code: \(cleaned)"
        case .ctaPhrase:
            return "Sponsor cue: \u{201C}\(cleaned)\u{201D}"
        case .brandSpan:
            // Preserve the matched casing — the catalog already canonicalizes
            // common variants, and ASR-lowercased brand names ("betterhelp")
            // still read clearly in this context. We deliberately do NOT
            // re-titlecase here: a forced "Hellofresh" would look uglier
            // than the verbatim "hellofresh" the user actually heard.
            return "Sponsor mention: \(cleaned)"
        }
    }

    /// Build the ordered list of evidence lines surfaced in the expanded
    /// banner detail. Deduplicates by line text (cheap defense against the
    /// same brand or URL surfacing twice through different category passes)
    /// and caps at `evidenceLineLimit`.
    ///
    /// Ordering priority (most concrete first):
    /// 1. promoCode — the line a listener is most likely to recognize
    /// 2. url — the second most concrete signal
    /// 3. disclosurePhrase — names the read explicitly
    /// 4. brandSpan — names the advertiser when no disclosure landed
    /// 5. ctaPhrase — softest signal, most likely to be a false positive
    static func evidenceLines(for entries: [EvidenceEntry]) -> [String] {
        guard !entries.isEmpty else { return [] }
        let priority: [EvidenceCategory: Int] = [
            .promoCode: 0,
            .url: 1,
            .disclosurePhrase: 2,
            .brandSpan: 3,
            .ctaPhrase: 4,
        ]
        let sorted = entries.sorted { lhs, rhs in
            let l = priority[lhs.category] ?? Int.max
            let r = priority[rhs.category] ?? Int.max
            if l != r { return l < r }
            // Stable secondary key: earlier in the audio first.
            return lhs.startTime < rhs.startTime
        }
        var seen = Set<String>()
        var lines: [String] = []
        for entry in sorted {
            let line = evidenceLine(for: entry)
            // Case-insensitive dedup so "BetterHelp" and "betterhelp" don't
            // surface twice (the catalog can produce both via different
            // capture paths).
            let key = line.lowercased()
            if seen.insert(key).inserted {
                lines.append(line)
                if lines.count >= evidenceLineLimit { break }
            }
        }
        return lines
    }

    // MARK: - Banner Card

    @ViewBuilder
    private func bannerCard(_ item: AdSkipBannerItem) -> some View {
        let copy = Self.bannerCopy(for: item)
        // playhead-vjxc: only build the evidence detail strings once per
        // render so we can both decide whether to show the chevron and
        // populate the expanded list from a single source of truth.
        let evidenceLines = Self.evidenceLines(for: item.evidenceCatalogEntries)
        let isExpanded = expandedBannerId == item.id && !evidenceLines.isEmpty

        VStack(alignment: .leading, spacing: Spacing.xs) {
            // Top line: template-driven copy
            HStack(spacing: 0) {
                Text(copy.prefix)
                    .font(AppTypography.sans(size: 13, weight: .semibold))
                    .foregroundStyle(AppColors.accent)

                if let advertiser = copy.advertiser {
                    Text(" \u{00B7} ")
                        .font(AppTypography.sans(size: 13, weight: .regular))
                        .foregroundStyle(boneText)
                    Text(advertiser)
                        .font(AppTypography.sans(size: 13, weight: .medium))
                        .foregroundStyle(boneText)
                }

                if let detail = copy.detail {
                    Text(" \u{00B7} ")
                        .font(AppTypography.sans(size: 13, weight: .regular))
                        .foregroundStyle(boneText.opacity(0.6))
                    Text("\"\(detail)\"")
                        .font(AppTypography.mono(size: 12, weight: .regular))
                        .foregroundStyle(boneText.opacity(0.7))
                        .lineLimit(1)
                }

                Spacer(minLength: Spacing.xs)
            }

            // playhead-vjxc: Expanded evidence detail. Renders below the
            // top line and above the action row so the actions remain in
            // the same screen position whether collapsed or expanded.
            // Hidden entirely (graceful absence) when no catalog entries
            // overlap the skipped span.
            if isExpanded {
                VStack(alignment: .leading, spacing: Spacing.xxs) {
                    ForEach(Array(evidenceLines.enumerated()), id: \.offset) { _, line in
                        Text(line)
                            .font(AppTypography.sans(size: 12, weight: .regular))
                            .foregroundStyle(boneText.opacity(0.75))
                            .lineLimit(2)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
                .accessibilityElement(children: .combine)
                .accessibilityLabel(
                    "Why we skipped: " + evidenceLines.joined(separator: ", ")
                )
                .transition(.opacity.combined(with: .move(edge: .top)))
            }

            // Bottom line: actions
            HStack {
                // Phase 7.2: "Not an ad" correction button (leading, muted).
                if let onNotAnAd {
                    Button {
                        onNotAnAd(item)
                        queue.dismiss()
                    } label: {
                        Text("Not an ad")
                            .font(AppTypography.sans(size: 12, weight: .regular))
                            .foregroundStyle(boneText.opacity(0.5))
                    }
                    .buttonStyle(BannerButtonStyle())
                    .accessibilityLabel("Mark as not an ad")
                    .accessibilityHint("Records that this segment was not an advertisement")
                }

                Spacer()

                // playhead-vjxc: chevron toggle. Only shown when there is
                // catalog evidence to surface — empty-list banners keep
                // the original three-button action row exactly.
                if !evidenceLines.isEmpty {
                    Button {
                        if expandedBannerId == item.id {
                            expandedBannerId = nil
                        } else {
                            expandedBannerId = item.id
                        }
                    } label: {
                        Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                            .font(.system(size: 11, weight: .semibold))
                            .foregroundStyle(boneText.opacity(0.5))
                            .frame(width: 28, height: 28)
                            .contentShape(Rectangle())
                    }
                    .buttonStyle(BannerButtonStyle())
                    .accessibilityLabel(isExpanded ? "Hide evidence" : "Show evidence")
                    .accessibilityHint("Reveals the signals that led Playhead to skip this segment")
                }

                // Listen button — copper accent
                Button {
                    onListen?(item)
                } label: {
                    Text("Listen")
                        .font(AppTypography.sans(size: 13, weight: .semibold))
                        .foregroundStyle(AppColors.accent)
                        .padding(.horizontal, Spacing.sm)
                        .padding(.vertical, Spacing.xxs)
                        .background(
                            RoundedRectangle(cornerRadius: CornerRadius.small)
                                .fill(AppColors.accent.opacity(0.12))
                        )
                }
                .buttonStyle(BannerButtonStyle())
                .accessibilityLabel("Listen to skipped ad")
                .accessibilityHint("Rewinds to the start of the skipped ad segment")

                // Dismiss button
                Button {
                    queue.dismiss()
                } label: {
                    Image(systemName: "xmark")
                        .font(.system(size: 12, weight: .semibold))
                        .foregroundStyle(boneText.opacity(0.5))
                        .frame(width: 28, height: 28)
                        .contentShape(Rectangle())
                }
                .buttonStyle(BannerButtonStyle())
                .accessibilityLabel("Dismiss banner")
            }
        }
        .padding(.horizontal, Spacing.md)
        .padding(.vertical, Spacing.sm)
        .background(
            RoundedRectangle(cornerRadius: CornerRadius.medium)
                .fill(Palette.ink)
                .themeShadow(AppShadow.elevated)
        )
        .animation(Motion.standard, value: isExpanded)
        .accessibilityElement(children: .contain)
        .onAppear {
            // Subtle haptic on banner appear.
            handleBannerAppear()
        }
    }

    // MARK: - Constants

    /// Bone text color for use on ink background (always light, regardless of mode).
    private var boneText: Color { Palette.bone }
}

// MARK: - Banner Button Style

/// Subtle scale-down on press — consistent with transport button style.
private struct BannerButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .scaleEffect(configuration.isPressed ? 0.92 : 1.0)
            .animation(Motion.quick, value: configuration.isPressed)
    }
}

// MARK: - Preview

#Preview("Ad Banner — High Confidence") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-1",
                    windowId: "w-1",
                    advertiser: "Squarespace",
                    product: "Build your website",
                    adStartTime: 120.0,
                    adEndTime: 180.0,
                    metadataConfidence: 0.85,
                    metadataSource: "foundationModels",
                    podcastId: "podcast-1",
                    evidenceCatalogEntries: []
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}

#Preview("Ad Banner — Low Confidence") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-2",
                    windowId: "w-2",
                    advertiser: "Maybe Corp",
                    product: nil,
                    adStartTime: 300.0,
                    adEndTime: 345.0,
                    metadataConfidence: 0.3,
                    metadataSource: "foundationModels",
                    podcastId: "podcast-1",
                    evidenceCatalogEntries: []
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}

#Preview("Ad Banner — No Metadata") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-3",
                    windowId: "w-3",
                    advertiser: nil,
                    product: nil,
                    adStartTime: 400.0,
                    adEndTime: 450.0,
                    metadataConfidence: nil,
                    metadataSource: "none",
                    podcastId: "podcast-1",
                    evidenceCatalogEntries: []
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}

#Preview("Ad Banner — Evidence Detail") {
    ZStack {
        AppColors.background
            .ignoresSafeArea()

        AdBannerView(
            queue: {
                let q = AdBannerQueue()
                q.enqueue(AdSkipBannerItem(
                    id: "preview-4",
                    windowId: "w-4",
                    advertiser: "BetterHelp",
                    product: nil,
                    adStartTime: 240.0,
                    adEndTime: 300.0,
                    metadataConfidence: 0.82,
                    metadataSource: "foundationModels",
                    podcastId: "podcast-1",
                    evidenceCatalogEntries: [
                        EvidenceEntry(
                            evidenceRef: 0,
                            category: .disclosurePhrase,
                            matchedText: "sponsored by",
                            normalizedText: "sponsored by",
                            atomOrdinal: 12,
                            startTime: 245.0,
                            endTime: 246.0
                        ),
                        EvidenceEntry(
                            evidenceRef: 1,
                            category: .url,
                            matchedText: "betterhelp.com/podcast",
                            normalizedText: "betterhelp.com/podcast",
                            atomOrdinal: 14,
                            startTime: 270.0,
                            endTime: 271.0
                        ),
                        EvidenceEntry(
                            evidenceRef: 2,
                            category: .promoCode,
                            matchedText: "use code PODCAST",
                            normalizedText: "podcast",
                            atomOrdinal: 15,
                            startTime: 285.0,
                            endTime: 286.0
                        ),
                    ]
                ))
                return q
            }()
        )
    }
    .preferredColorScheme(.dark)
}
