// DownloadNextView.swift
// playhead-hkg8 — "Download Next N" bulk affordance for the show page.
//
// Surface treatment (UI design §D):
//   "Download next [3 ▾] episodes"                   — stepper 1/3/5/10/25
//   "[ for ▾ ]"                                      — Flight/Commute/Workout/Generic
//   "Downloading 3 episodes (~640 MB). Will fit..."  — inline summary
//   Amber over-cap: "Not enough space — [Free up space →]"
//
// v1 scope:
//   - Generic is the default "for …" value and HIDES the phrase entirely
//     (no "for Generic" text — per spec).
//   - The "for …" picker only affects notification copy downstream
//     (Phase 2, §G), NEVER scheduler behavior in v1.
//   - "All unplayed" is deliberately NOT an option in v1.
//   - Deep link from "Free up space" into Settings → Storage is wired
//     behind a caller-supplied closure. Call sites (see
//     `EpisodeListView`) inject a closure that pushes `.storage` into
//     a shared `SettingsRouter` (playhead-l274), which the
//     `SettingsView` consumes to scroll/focus the Storage group.
//
// Copy lives in `DownloadNextCopy` so snapshot tests pin every
// user-visible string character-for-character. Any edit here is a
// product decision (update the bd spec and tests together).

import SwiftUI
import SwiftData

// MARK: - Verbatim copy

/// User-visible strings for the "Download Next N" bulk affordance.
/// Tests in `DownloadNextCopyTests` assert every one of these verbatim;
/// do not inline-literal any of these elsewhere in the view body.
enum DownloadNextCopy {

    /// Full button label pattern. The actual rendered button builds this
    /// up via interpolated SwiftUI subviews (stepper menu inline). The
    /// value here is the plain-text form used for accessibility and test
    /// assertions.
    static func buttonLabel(count: Int) -> String {
        "Download next \(count) episode\(count == 1 ? "" : "s")"
    }

    /// Within-cap inline summary (UI design §D verbatim pattern).
    /// Example: "Downloading 3 episodes (~640 MB). Will fit in your 10 GB cap."
    static func withinCapSummary(
        count: Int,
        estimatedMB: Int,
        capGB: Int
    ) -> String {
        "Downloading \(count) episode\(count == 1 ? "" : "s") (~\(estimatedMB) MB). Will fit in your \(capGB) GB cap."
    }

    /// Amber over-cap line. The "[Free up space →]" suffix is rendered
    /// as a tappable button in the view body; this string is just the
    /// lead-in phrase that precedes it. Kept as a distinct constant so
    /// tests can pin each fragment separately.
    static let overCapLead: String = "Not enough space"

    /// CTA label on the over-cap deep link.
    static let freeUpSpaceCTA: String = "Free up space \u{2192}"

    /// System-promise copy shown beneath the inline summary. Verbatim
    /// from the bd spec.
    static let systemPromise: String =
        "Media will be downloaded as fast as transport allows; analysis runs on each as capacity allows. No promise about full skip-readiness by any time."

    /// "for …" picker trailing-phrase copy. Generic is special-cased:
    /// the phrase is hidden entirely (no "for Generic" text). All other
    /// contexts render as "for <context>".
    static func forContextPhrase(_ context: DownloadTripContext) -> String? {
        switch context {
        case .generic:  return nil
        case .flight:   return "for Flight"
        case .commute:  return "for Commute"
        case .workout:  return "for Workout"
        }
    }
}

// MARK: - Trip context

/// The optional "for …" trip-context picker value. In v1 this only
/// influences the post-download notification copy (§G) — it does NOT
/// change scheduler behavior, slice sizing, or download order.
enum DownloadTripContext: String, CaseIterable, Sendable, Identifiable, Hashable {
    case generic
    case flight
    case commute
    case workout

    var id: String { rawValue }

    /// Menu-item label shown inside the "for …" picker (not the
    /// rendered trailing phrase — see `DownloadNextCopy.forContextPhrase`).
    var pickerLabel: String {
        switch self {
        case .generic: return "Generic"
        case .flight:  return "Flight"
        case .commute: return "Commute"
        case .workout: return "Workout"
        }
    }
}

// MARK: - Stepper options

/// The allowed "Download Next N" counts. Order is preserved in the UI
/// menu; default is 3 (per spec).
enum DownloadNextStepper {
    /// Allowed stepper values — 1, 3, 5, 10, 25. No "All unplayed" in v1.
    static let options: [Int] = [1, 3, 5, 10, 25]

    /// Default value shown when the show page first renders.
    static let defaultCount: Int = 3
}

// MARK: - Size estimation (pure)

/// Pure helpers for estimating the on-disk footprint of a proposed bulk
/// download. Snapshot tests pin the exact formatter output; the heuristic
/// itself (128 kbps fallback, 60-minute default when duration is
/// unknown) is documented here and can be tuned without touching the
/// view layer.
///
/// The numbers aren't meant to be exact — the user sees a ~MB figure and
/// a cap-fit decision, and the spec explicitly uses "~640 MB" as an
/// approximation. The goal is "within ~20% of the real download" so the
/// cap-fit decision is honest.
enum DownloadSizeEstimator {

    /// Assumed audio bitrate when no other signal is available. 128 kbps
    /// is the mid-range modern podcast encoding (Overcast/Apple Podcasts
    /// commonly serve between 64 and 256 kbps; 128 is the typical
    /// compromise). Bytes-per-second = 128_000 / 8 = 16_000.
    static let assumedBitrateBytesPerSecond: Int64 = 16_000

    /// Fallback episode duration (seconds) when `duration` is nil on the
    /// `Episode` model. 60 minutes is a defensible average across the
    /// catalogs Playhead sees; shorter news shows (~20m) and longer
    /// interview formats (2h+) both exist but the mean sits near an hour.
    static let fallbackDurationSeconds: TimeInterval = 60 * 60

    /// Estimate total bytes for a prefix of up to `count` episodes taken
    /// from `episodes` (newest-first order expected — caller's concern).
    /// Episodes without a known `duration` contribute the fallback
    /// one-hour estimate.
    static func estimatedBytes(
        for episodes: [Episode],
        count: Int
    ) -> Int64 {
        guard count > 0 else { return 0 }
        let prefix = episodes.prefix(max(0, count))
        return prefix.reduce(Int64(0)) { total, episode in
            let seconds = episode.duration ?? fallbackDurationSeconds
            let bytes = Int64((seconds * Double(assumedBitrateBytesPerSecond)).rounded())
            return total + bytes
        }
    }

    /// Convert a byte count to whole megabytes (SI: 1 MB = 1_000_000
    /// bytes), rounded to the nearest integer. Used for the inline-summary
    /// copy so snapshot tests have a deterministic, locale-free
    /// representation. Negative inputs clamp to 0.
    static func wholeMegabytes(_ bytes: Int64) -> Int {
        guard bytes > 0 else { return 0 }
        let mb = Double(bytes) / 1_000_000.0
        return Int(mb.rounded())
    }

    /// Convert a byte cap to whole gigabytes (SI: 1 GB = 1_000_000_000
    /// bytes), rounded to the nearest integer. Matches the user-visible
    /// unit used in Settings → Storage (e.g. "10 GB cap").
    static func wholeGigabytes(_ bytes: Int64) -> Int {
        guard bytes > 0 else { return 0 }
        let gb = Double(bytes) / 1_000_000_000.0
        return Int(gb.rounded())
    }
}

// MARK: - Cap-fit decision (pure)

/// The outcome of comparing an estimated download size to the configured
/// media cap. `.withinCap` means admission will succeed; `.overCap`
/// triggers the amber state and "Free up space →" CTA.
enum DownloadNextCapFit: Equatable {
    case withinCap(estimatedBytes: Int64, capBytes: Int64)
    case overCap(estimatedBytes: Int64, capBytes: Int64)

    /// Pure decision function. `currentUsedBytes` is the number of bytes
    /// the media pool is already using — the admission comparison is
    /// `used + estimated <= cap`. Defaults to 0 (fresh cache) for call
    /// sites that don't yet track live usage; wire it once the show page
    /// has an async size probe.
    static func decide(
        estimatedBytes: Int64,
        capBytes: Int64,
        currentUsedBytes: Int64 = 0
    ) -> DownloadNextCapFit {
        let projected = currentUsedBytes.addingReportingOverflow(estimatedBytes)
        let summed = projected.overflow ? Int64.max : projected.partialValue
        if summed > capBytes {
            return .overCap(estimatedBytes: estimatedBytes, capBytes: capBytes)
        }
        return .withinCap(estimatedBytes: estimatedBytes, capBytes: capBytes)
    }
}

// MARK: - DownloadNextView

/// Bulk "Download Next N" affordance rendered at the top of the show
/// page. Composes the stepper, the optional "for …" picker, the
/// inline-summary line, the amber over-cap state, and the system-promise
/// copy — per UI design §D.
///
/// All external side effects are expressed as closures so this view is
/// pure from a SwiftUI perspective and the tests don't need to drive
/// the full runtime.
struct DownloadNextView: View {

    /// Newest-first list of candidate episodes for the "take next N"
    /// prefix. The view never mutates this — it just reads duration.
    let episodes: [Episode]

    /// Cap in bytes that the over-cap decision is made against. In
    /// production this is `StorageBudgetSettings.load().mediaCapBytes`.
    let mediaCapBytes: Int64

    /// Bytes the media cache is currently using. Defaults to 0 until a
    /// live probe is wired in — the cap-fit math still works (the
    /// comparison becomes `estimated <= cap` in that case).
    var currentUsedBytes: Int64 = 0

    /// Called when the user taps the primary "Download next N episodes"
    /// button. Signature captures everything the caller needs to kick off
    /// the `DownloadManager.backgroundDownload(episodeId:from:)` loop
    /// (which the caller owns). Context is passed through for future
    /// notification-copy routing (v1 consumer: `LiveActivityCopy`).
    let onDownload: (_ episodes: [Episode], _ context: DownloadTripContext) -> Void

    /// Deep-link tap target for the amber "Free up space →" CTA. Default
    /// is a no-op — the preview and tests use that default. In the app,
    /// `EpisodeListView` injects a closure that calls
    /// `SettingsRouter.request(.storage)` so the Settings tab scrolls to
    /// the Storage group on next appearance (playhead-l274).
    var onFreeUpSpace: () -> Void = {}

    // MARK: UI state

    @State private var count: Int = DownloadNextStepper.defaultCount
    @State private var context: DownloadTripContext = .generic

    // MARK: Body

    var body: some View {
        VStack(alignment: .leading, spacing: Spacing.xs) {
            primaryRow
            forContextRow
            inlineSummary
        }
        .padding(.horizontal, Spacing.md)
        .padding(.vertical, Spacing.sm)
        .background(AppColors.surface)
        .overlay(
            RoundedRectangle(cornerRadius: CornerRadius.medium)
                .stroke(AppColors.textSecondary.opacity(0.15), lineWidth: 1)
        )
        .clipShape(RoundedRectangle(cornerRadius: CornerRadius.medium))
        .accessibilityIdentifier("downloadNext.container")
    }

    // MARK: - Pieces

    /// Primary "Download next [N ▾] episodes" button. The stepper lives
    /// INSIDE the button label as a Menu, keeping the single-sentence
    /// feel the UI design calls out.
    private var primaryRow: some View {
        Button {
            let picked = Array(episodes.prefix(count))
            onDownload(picked, context)
        } label: {
            HStack(spacing: 0) {
                Text("Download next ")
                Menu {
                    ForEach(DownloadNextStepper.options, id: \.self) { option in
                        Button {
                            count = option
                        } label: {
                            Text("\(option)")
                        }
                    }
                } label: {
                    HStack(spacing: 2) {
                        Text("\(count)")
                            .font(AppTypography.sans(size: 16, weight: .semibold))
                        Image(systemName: "chevron.down")
                            .font(.system(size: 10, weight: .semibold))
                    }
                    .foregroundStyle(AppColors.accent)
                    .accessibilityIdentifier("downloadNext.stepperMenu")
                }
                Text(" episode\(count == 1 ? "" : "s")")
            }
            .font(AppTypography.sans(size: 16, weight: .regular))
            .foregroundStyle(AppColors.textPrimary)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .buttonStyle(.plain)
        .accessibilityIdentifier("downloadNext.primaryButton")
        .accessibilityLabel(DownloadNextCopy.buttonLabel(count: count))
    }

    /// Trailing "[ for ▾ ]" picker. Generic is the default and hides the
    /// trailing phrase entirely; other contexts render "for <Context>".
    @ViewBuilder
    private var forContextRow: some View {
        HStack(spacing: Spacing.xs) {
            Menu {
                ForEach(DownloadTripContext.allCases) { option in
                    Button {
                        context = option
                    } label: {
                        Text(option.pickerLabel)
                    }
                }
            } label: {
                HStack(spacing: 2) {
                    Text("for")
                    Image(systemName: "chevron.down")
                        .font(.system(size: 10, weight: .semibold))
                }
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
            }
            .accessibilityIdentifier("downloadNext.forPicker")

            if let phrase = DownloadNextCopy.forContextPhrase(context) {
                Text(phrase)
                    .font(AppTypography.caption)
                    .foregroundStyle(AppColors.textSecondary)
                    .accessibilityIdentifier("downloadNext.forPhrase")
            }

            Spacer()
        }
    }

    /// Inline confirmation summary — within-cap green-light copy, or the
    /// amber over-cap "Free up space →" CTA — plus the system-promise
    /// paragraph (always shown).
    private var inlineSummary: some View {
        let estimated = DownloadSizeEstimator.estimatedBytes(
            for: episodes,
            count: count
        )
        let fit = DownloadNextCapFit.decide(
            estimatedBytes: estimated,
            capBytes: mediaCapBytes,
            currentUsedBytes: currentUsedBytes
        )

        return VStack(alignment: .leading, spacing: Spacing.xxs) {
            switch fit {
            case .withinCap:
                Text(
                    DownloadNextCopy.withinCapSummary(
                        count: count,
                        estimatedMB: DownloadSizeEstimator.wholeMegabytes(estimated),
                        capGB: DownloadSizeEstimator.wholeGigabytes(mediaCapBytes)
                    )
                )
                .font(AppTypography.caption)
                .foregroundStyle(AppColors.textSecondary)
                .accessibilityIdentifier("downloadNext.withinCapSummary")

            case .overCap:
                HStack(spacing: Spacing.xs) {
                    Text(DownloadNextCopy.overCapLead + " —")
                        .font(AppTypography.caption)
                        .foregroundStyle(.orange)
                        .accessibilityIdentifier("downloadNext.overCapLead")

                    Button {
                        onFreeUpSpace()
                    } label: {
                        Text(DownloadNextCopy.freeUpSpaceCTA)
                            .font(AppTypography.caption)
                            .foregroundStyle(.orange)
                            .underline()
                    }
                    .buttonStyle(.plain)
                    .accessibilityIdentifier("downloadNext.freeUpSpaceCTA")
                    .accessibilityLabel(DownloadNextCopy.freeUpSpaceCTA)
                }
            }

            Text(DownloadNextCopy.systemPromise)
                .font(AppTypography.timestamp)
                .foregroundStyle(AppColors.textTertiary)
                .fixedSize(horizontal: false, vertical: true)
                .accessibilityIdentifier("downloadNext.systemPromise")
        }
    }
}

// MARK: - Preview

#Preview("Download Next — within cap") {
    let previewEpisodes: [Episode] = (0..<3).map { i in
        Episode(
            feedItemGUID: "preview-\(i)",
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Episode \(i)",
            audioURL: URL(string: "https://example.com/\(i).mp3")!,
            duration: 60 * 60
        )
    }
    return DownloadNextView(
        episodes: previewEpisodes,
        mediaCapBytes: 10 * 1_000_000_000,
        onDownload: { _, _ in }
    )
    .padding()
    .background(AppColors.background)
    .preferredColorScheme(.dark)
}

#Preview("Download Next — over cap") {
    let bigEpisodes: [Episode] = (0..<25).map { i in
        Episode(
            feedItemGUID: "preview-big-\(i)",
            feedURL: URL(string: "https://example.com/feed.xml")!,
            title: "Episode \(i)",
            audioURL: URL(string: "https://example.com/\(i).mp3")!,
            duration: 60 * 60 * 10
        )
    }
    return DownloadNextView(
        episodes: bigEpisodes,
        mediaCapBytes: 1 * 1_000_000_000,
        onDownload: { _, _ in },
        onFreeUpSpace: {}
    )
    .padding()
    .background(AppColors.background)
    .preferredColorScheme(.dark)
}
