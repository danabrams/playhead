// SkipOrchestrator.swift
// Decision layer between ad detection and playback transport.
//
// Consumes AdWindows from AdDetectionService, applies skip policy
// (hysteresis, merging, suppression after seek),
// and pushes skip cues to PlaybackService as CMTimeRanges.
//
// Every skip decision is idempotent, keyed by
//   analysisAssetId + adWindowId + policyVersion.
//
// NEVER queries SQLite synchronously from the playback callback path.
// All state is maintained in-memory; SQLite writes are fire-and-forget
// for the decision log.

import CoreMedia
import Foundation
import OSLog

// MARK: - Future Contract Scaffold

/// Phase 6 contract scaffold introduced by bead 6.7 so the pending
/// AdDecisionResult-based tests compile against the planned production
/// symbol names before bead 6.4 wires the real behavior.
///
/// Naming note: `AdDecisionResult` (this type) is the **runtime per-window decision**
/// that `SkipOrchestrator` consumes during active playback. It is distinct from
/// `DecisionResultArtifact` (in AdDecisionResult.swift), which is the SQLite persistence
/// container that stores an array of these decisions as JSON. The separation is intentional:
/// one type is optimized for live evaluation, the other for durable storage.
enum AdDecisionEligibilityGate: String, Sendable {
    case eligible
    case blocked
}

struct AdDecisionResult: Sendable {
    let id: String
    let analysisAssetId: String
    let startTime: Double
    let endTime: Double
    let skipConfidence: Double
    let eligibilityGate: AdDecisionEligibilityGate
    let recomputationRevision: Int
}

// MARK: - Skip Decision State

/// Lifecycle of an AdWindow through the skip orchestrator.
/// Extends the detection-side states (candidate, confirmed, suppressed)
/// with skip-execution states.
enum SkipDecisionState: String, Sendable, CaseIterable {
    /// Detection produced a candidate -- not yet actionable.
    case candidate
    /// Detection confirmed the window -- eligible for skip policy.
    case confirmed
    /// Skip policy accepted and skip cue was fired.
    case applied
    /// Skip was suppressed by policy (too short, ambiguous, etc.).
    case suppressed
    /// User tapped "Listen" -- revert the skip.
    case reverted
}

// MARK: - Skip Policy Configuration

struct SkipPolicyConfig: Sendable {
    /// Hysteresis: probability threshold to enter ad state.
    let enterThreshold: Double
    /// Hysteresis: probability threshold to stay in ad state (lower).
    let stayThreshold: Double
    /// Merge adjacent ad windows with gaps smaller than this (seconds).
    let mergeGapSeconds: TimeInterval
    /// Ignore ad windows shorter than this unless sponsor evidence is strong.
    let minimumSpanSeconds: TimeInterval
    /// Confidence threshold for short-span override (strong sponsor evidence).
    let shortSpanOverrideConfidence: Double
    /// Seconds after a user seek during which auto-skip is suppressed.
    let seekSuppressionSeconds: TimeInterval
    /// Seconds of stability required after seek before re-enabling skip.
    let seekStabilitySeconds: TimeInterval
    /// Policy version tag for idempotency keys.
    let policyVersion: String
    /// Cushion (seconds) subtracted from the trailing edge of an ad pod when
    /// the next thing is program audio (or end-of-episode). Trades a small
    /// sliver of ad-tail for protection against program-start clipping.
    /// Applied per merged pod, not per individual ad — internal seams between
    /// ads in the same pod do not receive a cushion. Clamped at the pod start
    /// so the skip end can never precede the skip start.
    let adTrailingCushionSeconds: TimeInterval

    static let `default` = SkipPolicyConfig(
        enterThreshold: 0.65,
        stayThreshold: 0.45,
        mergeGapSeconds: 4.0,
        minimumSpanSeconds: 15.0,
        shortSpanOverrideConfidence: 0.85,
        seekSuppressionSeconds: 3.0,
        seekStabilitySeconds: 2.0,
        policyVersion: "skip-policy-v1",
        adTrailingCushionSeconds: 1.0
    )
}

// MARK: - Skip Decision Record

/// Immutable record of a skip decision for the evaluation harness.
struct SkipDecisionRecord: Sendable {
    let idempotencyKey: String
    let adWindowId: String
    let analysisAssetId: String
    let policyVersion: String
    let decision: SkipDecisionState
    let reason: String
    let originalStart: Double
    let originalEnd: Double
    let snappedStart: Double
    let snappedEnd: Double
    let confidence: Double
    let timestamp: Double
}

// MARK: - Managed Ad Window

/// In-memory representation of an AdWindow with skip orchestrator state.
private struct ManagedWindow: Sendable {
    let adWindow: AdWindow
    var decisionState: SkipDecisionState
    var snappedStart: Double
    var snappedEnd: Double
    var idempotencyKey: String
    /// Whether the skip cue has been pushed to PlaybackService.
    var cueActive: Bool
}

// MARK: - SkipOrchestrator

/// Consumes ad detection events and produces skip cues for PlaybackService.
/// Maintains hysteresis state, merges short gaps, and suppresses skips
/// after user seeks.
///
/// All decisions are logged for the evaluation harness.
actor SkipOrchestrator {

    private let logger = Logger(subsystem: "com.playhead", category: "SkipOrchestrator")

    /// Bug 5 (skip-cues-deletion): minimum confidence required for an
    /// `AdWindow` to be eligible for the `beginEpisode` preload that
    /// previously read from `skip_cues`. Mirrors the 0.7 threshold the
    /// (now-deleted) `SkipCueMaterializer` used. Kept private to this
    /// actor — the only consumer is `beginEpisode`.
    private static let preloadConfidenceThreshold: Double = 0.7

    /// Cycle-21 H-1: returns whether a decision state is allowed to
    /// flow through the `beginEpisode` preload into `receiveAdWindows`.
    /// `.candidate`, `.confirmed`, `.applied` are eligible; `.suppressed`
    /// (terminal "no-skip") and `.reverted` (user chose "Listen") are
    /// not. `.applied` is eligible so a previously-skipped ad pushes
    /// its cue on the next app launch (cross-launch auto-skip
    /// continuity); banner re-emission for those rows is suppressed in
    /// `beginEpisode` by pre-populating `banneredWindowIds`.
    ///
    /// Cycle-22 M-1: implemented as an exhaustive `switch` over
    /// `SkipDecisionState` (rather than an array of three cases) so
    /// the compiler forces a deliberate decision when a new case is
    /// added — the new case won't silently default to ineligible
    /// without an author choice.
    private static func isPreloadEligible(_ state: SkipDecisionState) -> Bool {
        switch state {
        case .candidate, .confirmed, .applied:
            return true
        case .suppressed, .reverted:
            return false
        }
    }

    /// Cycle-21 L-1: derived from `SkipDecisionState.allCases` (cycle-22
    /// M-1 made the enum `CaseIterable`) so the on-disk filter cannot
    /// drift from the in-actor enum across renames, rawValue changes,
    /// or new cases. The exhaustive partition lives in
    /// `isPreloadEligible(_:)`.
    private static let preloadEligibleDecisionStates: Set<String> = Set(
        SkipDecisionState.allCases
            .filter(SkipOrchestrator.isPreloadEligible)
            .map(\.rawValue)
    )

    // MARK: - Dependencies

    private let store: AnalysisStore
    private let config: SkipPolicyConfig
    private let trustService: TrustScoringService?

    // MARK: - Phase 7.2: User Correction Store

    /// Injected by PlayheadRuntime after init. Fire-and-forget writes; never throws.
    /// Optional so existing test setups that don't inject the store remain unaffected.
    private(set) var correctionStore: (any UserCorrectionStore)?

    // MARK: - playhead-xsdz.9: Hard-negative fingerprint bank

    /// Optional HARD-NEGATIVE fingerprint bank. When wired, a user "Listen"
    /// revert / "not an ad" veto of an auto-skipped (or markOnly) window is the
    /// confirmed-FP WRITE TRIGGER: the reverted window's `evidenceText` is
    /// ingested as a hard-negative via `recordConfirmedFalsePositive`. A bank is
    /// wired ONLY when the `AdDetectionConfig.crossEpisodeMemoryEnabled` feature
    /// flag is on (PlayheadRuntime constructs and injects it behind that flag),
    /// so the WHOLE feature — construction, migration, this write trigger, and
    /// the suppression read — rides the one off-by-default flag. `nil` (the
    /// default for all existing call sites AND the flag-OFF production default)
    /// ⇒ no negative-bank writes, byte-identical to pre-xsdz.9.
    ///
    /// MEMORY-POLLUTION GUARD: only reversions (confirmed FPs) write here. The
    /// auto-skip-eligible / catalog-ingress path NEVER writes to this bank.
    private(set) var negativeFingerprintBank: NegativeFingerprintBank?

    /// Install (or replace) the hard-negative bank post-init. Mirrors the
    /// runtime's post-init wiring of other optional dependencies.
    func setNegativeFingerprintBank(_ bank: NegativeFingerprintBank?) {
        self.negativeFingerprintBank = bank
    }

    // MARK: - playhead-xsdz.11: Per-show auto-skip threshold controller

    /// Optional per-show auto-skip threshold controller store. This is the
    /// WRITE path for the PI controller: a user "Listen" revert / "not an ad"
    /// veto of an auto-skipped (or markOnly) window is a FALSE-POSITIVE signal
    /// that RAISES the show's threshold (be more conservative). A store is wired
    /// ONLY when the `AdDetectionConfig.perShowThresholdControlEnabled` feature
    /// flag is on (PlayheadRuntime constructs and injects it behind that flag),
    /// so the WHOLE feature — construction, migration, this write trigger, and
    /// the offset read at the detection gate — rides the one off-by-default
    /// flag. `nil` (the default for all existing call sites AND the flag-OFF
    /// production default) ⇒ no controller writes, byte-identical to pre-xsdz.11.
    ///
    /// Miss-side note: the symmetric MISS signal (the user scrubbed through
    /// undetected ad content → LOWER the threshold) has no clean, distinct
    /// gesture at this orchestration layer today — the only "missed ad" signal
    /// is the explicit false-negative correction routed through the
    /// `UserCorrectionStore`, not a scrub-through. `recordThresholdControlMiss`
    /// is the defined miss-side API; it is wired at the false-negative seam
    /// where `podcastId` is available. The FP side (below) is fully wired.
    private(set) var perShowThresholdControllerStore: PerShowThresholdControllerStore?

    /// Install (or replace) the per-show threshold controller store post-init.
    /// Mirrors `setNegativeFingerprintBank`.
    func setPerShowThresholdControllerStore(_ store: PerShowThresholdControllerStore?) {
        self.perShowThresholdControllerStore = store
    }

    // MARK: - playhead-98co: asymmetric auto-skip edge padding

    /// Feature flag for the derived edge-padding policy (default OFF —
    /// `AutoSkipEdgePadding.isEnabledByDefault`; auto-skip itself is held
    /// behind Gate 2). OFF ⇒ byte-identical orchestrator behavior: skip
    /// cues use the snapped span bounds exactly as before.
    private(set) var edgePaddingEnabled: Bool = AutoSkipEdgePadding.isEnabledByDefault

    /// Flip the edge-padding policy and re-evaluate pending windows so a
    /// mid-episode change takes effect on the next cue push.
    func setEdgePaddingEnabled(_ enabled: Bool) {
        edgePaddingEnabled = enabled
        evaluateAndPush()
    }

    /// Per-window edge-anchor provenance, keyed by adWindowId. This is the
    /// stamping seam for the Gate-2 provenance bead (rediff byte-exact /
    /// stinger-snap traces are not yet persisted on AdWindow rows).
    /// Absent entry ⇒ both edges `.unanchored` — the conservative default
    /// under which flag-ON auto-skips nothing (derivation doc §8.5).
    private var edgeAnchorsByWindowId: [String: (start: AutoSkipEdgeAnchor, end: AutoSkipEdgeAnchor)] = [:]

    /// Record the edge-anchor provenance for a window and re-evaluate.
    func setEdgeAnchors(
        start: AutoSkipEdgeAnchor,
        end: AutoSkipEdgeAnchor,
        forWindowId id: String
    ) {
        edgeAnchorsByWindowId[id] = (start: start, end: end)
        evaluateAndPush()
    }

    /// Windows whose skip was explicitly user-initiated (manual "Skip Ad"
    /// tap). User-initiated skips are exempt from edge padding: the user
    /// chose the span deliberately. User-marked and accepted-suggestion
    /// windows are exempted via their `boundaryState` stamps
    /// ("userMarked" / "userConfirmedSuggested") in
    /// `isUserInitiatedSkip(_:)`; this set covers the manual-tap path whose
    /// window is an ordinary detection row.
    private var userInitiatedSkipWindowIds: Set<String> = []

    // MARK: - State

    /// All managed windows for the current episode, keyed by adWindowId.
    private var windows: [String: ManagedWindow] = [:]

    /// Current analysis asset ID.
    private var activeAssetId: String?

    /// Current episode ID (canonical episode key). Used for the
    /// `episode_id_hash` stamped on `auto_skip_fired` events so the hash
    /// byte-matches the one `EpisodeSurfaceStatusObserver` stamps on
    /// `ready_entered`. Windows/decisions remain keyed by `activeAssetId`.
    private var activeEpisodeId: String?

    /// Whether we are currently "in ad state" (hysteresis tracking).
    private var inAdState: Bool = false

    /// Timestamp of the most recent user-initiated seek.
    private var lastSeekTime: Date?

    /// Whether skip is currently suppressed due to recent seek.
    private var skipSuppressedAfterSeek: Bool = false

    /// Latest known playhead position.
    private var currentPlayheadTime: TimeInterval = 0

    /// Decision log for evaluation harness. Capped to prevent unbounded growth.
    private var decisionLog: [SkipDecisionRecord] = []
    private let decisionLogCapacity = 500

    /// Callback to push skip cues to PlaybackService.
    /// Set via `setSkipCueHandler`. Avoids direct PlaybackServiceActor coupling.
    private var skipCueHandler: (([CMTimeRange]) -> Void)?

    /// Per-show skip mode for the current episode. Loaded from TrustScoringService
    /// at episode start. Defaults to `.shadow` if no trust service is wired.
    private var activeSkipMode: SkipMode = .shadow

    /// Continuation-backed stream of applied ad segment time ranges (seconds).
    /// Consumers receive the full set of applied segments whenever the set changes.
    private var segmentContinuations: [UUID: AsyncStream<[(start: Double, end: Double)]>.Continuation] = [:]

    /// Continuation-backed stream of banner items.
    /// Emits once per window the first time it reaches .confirmed or .applied state.
    private var bannerContinuations: [UUID: AsyncStream<AdSkipBannerItem>.Continuation] = [:]

    /// Window IDs for which a banner has already been emitted. Prevents re-fires.
    private var banneredWindowIds: Set<String> = []

    /// Cycle-23 H-1: window IDs for which `emitBannerItem` was actually
    /// invoked this episode (not merely "banner suppression flagged").
    /// `banneredWindowIds` is populated both BY emission AND by
    /// `beginEpisode`'s pre-population for preloaded `.applied` rows
    /// — meaning a snapshot of `banneredWindowIds` cannot distinguish
    /// "the gate was pre-populated" from "the eval loop emitted and
    /// then inserted." This separate set records ONLY actual auto-
    /// skip-tier banner emissions, so tests can deterministically
    /// assert "no auto-skip banner was emitted for window X" without
    /// any iteration-order coupling.
    ///
    /// **Cycle-26 L-1: TEST-ONLY OBSERVABILITY.** Production logic does
    /// NOT read this set — the gate that suppresses re-emission is
    /// `banneredWindowIds`, not this. The only reader is
    /// `emittedAutoSkipBannersSnapshot()`, called from
    /// `SkipOrchestratorPreloadTests`. Three operations on this set are
    /// load-bearing for those tests; do NOT delete any of them as "dead
    /// state":
    ///   • the `insert` in `emitBannerItem` (records actual emissions),
    ///   • the `removeAll` in `beginEpisode` (resets per-episode state),
    ///   • the `removeAll` in `endEpisode` (the cross-episode regression
    ///     test `testEmittedAutoSkipBannersDoesNotLeakAcrossEpisodes`
    ///     fails if this clear is dropped).
    private var emittedAutoSkipBannerWindowIds: Set<String> = []

    /// playhead-gtt9.23: window IDs for which a `.suggest` tier banner has
    /// already been emitted. Tracked separately from `banneredWindowIds`
    /// (auto-skip-tier emissions) so the two paths don't collide on a
    /// gate-flip mid-episode (a window first seen as markOnly that later
    /// promotes to auto-skip is allowed to emit a fresh auto-skipped
    /// banner — the user-facing event "we just skipped this" is the new
    /// information, not a duplicate).
    private var suggestBanneredWindowIds: Set<String> = []

    /// playhead-gtt9.23: in-memory record of windows currently surfaced as
    /// suggest-tier markers. Keyed by `AdWindow.id`. We hold them here
    /// rather than in `windows` so the auto-skip evaluation loop never
    /// considers them — the tier is strictly a UI surface, not a skip
    /// candidate. Cleared at episode end.
    private var suggestWindows: [String: AdWindow] = [:]

    /// playhead-rfu-sad: tap-then-flip race guard. AdWindow ids that have
    /// already been promoted via `acceptSuggestedSkip` (the user tapped
    /// the suggest banner). A late-arriving ingest with the same id and
    /// gate cleared must NOT register a second managed window — the
    /// promoted UUID-keyed entry is already authoritative for that span.
    /// Bounded LRU so a long episode doesn't grow the set unboundedly;
    /// 256 ids covers any realistic single-episode tap volume.
    private var recentlyAcceptedSuggestIds: [String] = []
    private let recentlyAcceptedSuggestCapacity = 256

    /// The podcast ID for the current episode. Needed to populate banner items.
    private var activePodcastId: String?

    /// The deterministic evidence catalog for the current episode's transcript,
    /// pushed by `AnalysisCoordinator` whenever new transcript material lands.
    /// Sliced per-window when emitting banner items so callers see only the
    /// evidence that overlaps the skipped span. `nil` when no catalog has
    /// been pushed for the active asset — the banner falls back to an empty
    /// `evidenceCatalogEntries` array, which the UI handles gracefully.
    private var activeEvidenceCatalog: EvidenceCatalog?

    /// Hasher used to stamp `auto_skip_fired` events with a per-install
    /// episode ID hash. Production passes a closure bound to the shared
    /// `SurfaceStatusInvariantLogger` instance so the hash is byte-
    /// identical to the one `EpisodeSurfaceStatusObserver` stamps on
    /// `ready_entered`. Tests can pin the hash to a known value
    /// independent of the logger's installId.
    private let episodeIdHasher: @Sendable (String) -> String

    /// The audit logger instance that `auto_skip_fired` events are
    /// written to. Shared with `EpisodeSurfaceStatusObserver` so both
    /// producers of the false_ready_rate pair land on the same file
    /// with the same installId.
    private let invariantLogger: SurfaceStatusInvariantLogger

    // MARK: - playhead-xr3t: inventory sanity filter

    /// playhead-xr3t: post-hoc filter applied to spans arriving at the
    /// fusion → user-visible-skip-decision boundary. Constructed from
    /// `LightweightInventoryChecksSettings` at init time. When the flag
    /// is OFF the filter is a no-op pass-through and behaviour is
    /// byte-identical to the pre-bead orchestrator.
    ///
    /// The filter is stateless; per-episode context (duration,
    /// declared chapters) is supplied at evaluation time from the
    /// orchestrator's `activeEpisodeDuration` / `activeDeclaredChapters`
    /// fields below.
    private let inventoryFilter: InventorySanityFilter

    /// playhead-xr3t: episode duration for the active episode in
    /// seconds. Set in `beginEpisode` from
    /// `AnalysisAsset.episodeDurationSec` (best-effort fetch) and
    /// updatable mid-episode via `setEpisodeDuration(_:)` when the
    /// duration-backfill probe rewrites the row. `nil` when the
    /// asset row carries no duration yet — the filter treats that
    /// as "tail edge unknown" and applies only the head-edge rule.
    private var activeEpisodeDuration: Double?

    /// playhead-xr3t: declared (publisher-provided) content chapters
    /// for the active episode. Loaded by AdDetectionService on its
    /// metadata fetch and pushed via `setDeclaredChapters(_:)`. Only
    /// creator-source ChapterEvidence (id3, pc20, rssInline) is
    /// stored here — `.inferred` chapters are filtered out by
    /// `setDeclaredChapters` so the inventory filter cannot
    /// accidentally consult them.
    private var activeDeclaredChapters: [ChapterEvidence] = []

    // MARK: - Init

    /// - Parameters:
    ///   - invariantLogger: The audit logger instance this orchestrator
    ///     writes `auto_skip_fired` events to. Defaults to a fresh
    ///     instance — test suites that don't inspect the log get an
    ///     isolated logger per orchestrator (no cross-test file races).
    ///     Production passes the runtime-shared instance so the companion
    ///     `ready_entered` producer (EpisodeSurfaceStatusObserver) lands
    ///     on the same file with the same installId.
    ///   - episodeIdHasher: Hasher for the `episode_id_hash` field.
    ///     When `nil`, derived from `invariantLogger.hashEpisodeId` so
    ///     production events naturally pair with the observer's. Tests
    ///     that want a pinned hash pass a deterministic closure.
    init(
        store: AnalysisStore,
        config: SkipPolicyConfig = .default,
        trustService: TrustScoringService? = nil,
        correctionStore: (any UserCorrectionStore)? = nil,
        invariantLogger: SurfaceStatusInvariantLogger = SurfaceStatusInvariantLogger(),
        episodeIdHasher: (@Sendable (String) -> String)? = nil,
        // playhead-xr3t (review): default to a disabled no-op filter so
        // pre-existing test surface that constructs `SkipOrchestrator`
        // — and never sets episode-duration / declared-chapter context
        // — doesn't silently lose pre-roll/post-roll spans to the
        // head-/tail-edge rules. Production wires the real settings-
        // backed filter explicitly via `InventorySanityFilter
        // .production()` (see `PlayheadRuntime`), preserving the bead's
        // spec default ON for new builds. This avoids an implicit
        // `UserDefaults.standard` dependency leaking into every test
        // that didn't ask for one.
        inventoryFilter: InventorySanityFilter = InventorySanityFilter(isEnabled: false)
    ) {
        self.store = store
        self.config = config
        self.trustService = trustService
        self.correctionStore = correctionStore
        self.invariantLogger = invariantLogger
        self.episodeIdHasher = episodeIdHasher ?? { [invariantLogger] episodeId in
            invariantLogger.hashEpisodeId(episodeId)
        }
        self.inventoryFilter = inventoryFilter
    }

    // MARK: - Configuration

    /// Set the callback that pushes skip cues to PlaybackService.
    func setSkipCueHandler(_ handler: @escaping @Sendable ([CMTimeRange]) -> Void) {
        skipCueHandler = handler
    }

    // MARK: - Ad Segment Stream

    /// Returns an AsyncStream of applied ad segment ranges (in seconds).
    /// Each emission is the full current set. The stream ends when the
    /// continuation is cancelled or the orchestrator is deallocated.
    func appliedSegmentsStream() -> AsyncStream<[(start: Double, end: Double)]> {
        let id = UUID()
        return AsyncStream { continuation in
            self.segmentContinuations[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeSegmentContinuation(id: id)
                }
            }
        }
    }

    private func removeSegmentContinuation(id: UUID) {
        segmentContinuations.removeValue(forKey: id)
    }

    // MARK: - Banner Item Stream

    /// Returns an AsyncStream that emits an AdSkipBannerItem the first time
    /// each ad window transitions to .confirmed or .applied state.
    /// Each window fires at most once per episode, regardless of subsequent state changes.
    func bannerItemStream() -> AsyncStream<AdSkipBannerItem> {
        let id = UUID()
        return AsyncStream { continuation in
            self.bannerContinuations[id] = continuation
            continuation.onTermination = { @Sendable _ in
                Task { [weak self] in
                    await self?.removeBannerContinuation(id: id)
                }
            }
        }
    }

    private func removeBannerContinuation(id: UUID) {
        bannerContinuations.removeValue(forKey: id)
    }

    // MARK: - Evidence Catalog (banner transparency)

    /// Push the deterministic evidence catalog for the active asset. The
    /// catalog is sliced per-window when emitting banners so each banner
    /// carries only the evidence overlapping its skipped span.
    ///
    /// Callers may push successively richer catalogs as transcript material
    /// arrives (e.g. fast pass first, then final). Late-arriving catalogs do
    /// NOT retroactively update banners already emitted — those carry the
    /// snapshot taken at emit time.
    ///
    /// Mismatched-asset catalogs are dropped silently: the orchestrator only
    /// retains a catalog whose `analysisAssetId` matches `activeAssetId`. This
    /// guards against late deliveries that race a podcast/episode change.
    func setEvidenceCatalog(_ catalog: EvidenceCatalog) {
        guard let activeAssetId, catalog.analysisAssetId == activeAssetId else {
            // playhead-rfu-sad: bumped from .debug to .info. The
            // common case here is benign: an asset-switch race where a
            // catalog finished building for the previous episode just
            // as the user moved to the next one. The catalog is
            // correctly dropped and `evidenceCatalogEntries` falls back
            // to empty for the new asset, which the UI handles.
            // The non-benign case is a real wiring regression (catalog
            // dispatcher routed to the wrong orchestrator instance,
            // duplicate active orchestrators, etc.). Both cases want
            // visibility: at .debug it is invisible in release; at
            // .info it surfaces in os_log without polluting
            // .notice/.error budgets so the wiring regression is
            // diagnosable from a user log without drowning the signal.
            let activeDescription = self.activeAssetId ?? "nil"
            logger.info(
                "Dropping evidence catalog for non-active asset \(catalog.analysisAssetId, privacy: .public) (active=\(activeDescription, privacy: .public))"
            )
            return
        }
        activeEvidenceCatalog = catalog
    }

    /// Emit a banner item for the given managed window to all banner listeners.
    private func emitBannerItem(for managed: ManagedWindow) {
        guard !bannerContinuations.isEmpty else { return }
        let adWindow = managed.adWindow
        let podcastId = activePodcastId ?? ""
        let entries = catalogEntries(overlapping: managed.snappedStart, end: managed.snappedEnd)
        let item = AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: adWindow.id,
            advertiser: adWindow.advertiser,
            product: adWindow.product,
            adStartTime: managed.snappedStart,
            adEndTime: managed.snappedEnd,
            metadataConfidence: adWindow.metadataConfidence,
            metadataSource: adWindow.metadataSource,
            podcastId: podcastId,
            evidenceCatalogEntries: entries,
            tier: .autoSkipped
        )
        // Cycle-26 L-1 / Cycle-27 L-2: this insert is consumed by
        // `emittedAutoSkipBannersSnapshot()` from canary tests. The
        // production gate that prevents re-fires is `banneredWindowIds`
        // — NOT this set. The gate is written at four production sites
        // (pinned by source canary `BanneredWindowIdsInsertSiteCount`):
        // `evaluateAndPush`'s terminal-state branch and its promotion
        // branch (each before calling this method), `injectUserMarkedAd`
        // (also before calling this method), and `beginEpisode`'s
        // preload pre-population for `.applied` rows (which suppresses
        // without ever calling this method). Do not remove this line as
        // "dead state"; see field doc.
        emittedAutoSkipBannerWindowIds.insert(adWindow.id)
        for (_, continuation) in bannerContinuations {
            continuation.yield(item)
        }
    }

    /// playhead-gtt9.23: emit a suggest-tier banner for a markOnly window.
    /// Suggest banners ask the user "Sounds like a sponsor break. Skip?";
    /// they never imply a skip has happened. Persistence and trust signals
    /// are deferred to the user's tap (handled by `acceptSuggestedSkip` /
    /// `declineSuggestedSkip` below).
    private func emitSuggestBanner(for adWindow: AdWindow) {
        guard !bannerContinuations.isEmpty else { return }
        let podcastId = activePodcastId ?? ""
        let entries = catalogEntries(overlapping: adWindow.startTime, end: adWindow.endTime)
        let item = AdSkipBannerItem(
            id: UUID().uuidString,
            windowId: adWindow.id,
            advertiser: adWindow.advertiser,
            product: adWindow.product,
            adStartTime: adWindow.startTime,
            adEndTime: adWindow.endTime,
            metadataConfidence: adWindow.metadataConfidence,
            metadataSource: adWindow.metadataSource,
            podcastId: podcastId,
            evidenceCatalogEntries: entries,
            tier: .suggest
        )
        for (_, continuation) in bannerContinuations {
            continuation.yield(item)
        }
    }

    /// Slice catalog entries whose coverage span overlaps the skipped window.
    /// Returns an empty array when no catalog is available or none overlap.
    private func catalogEntries(overlapping start: Double, end: Double) -> [EvidenceEntry] {
        guard let catalog = activeEvidenceCatalog else { return [] }
        // Closed-interval overlap: an entry overlaps the window iff its
        // coverage span shares ANY point with [start, end], including the
        // boundaries themselves. We deliberately use `<=` on both sides so
        // zero-duration entries that fall exactly on a snapped boundary
        // still surface — typical for short FM-bounded ad windows where the
        // disclosure phrase straddles the snap edge.
        return catalog.entries.filter { entry in
            entry.coverageStartTime <= end && entry.coverageEndTime >= start
        }
    }

    /// Broadcast the current set of applied segments to all listeners.
    private func broadcastAppliedSegments() {
        let applied = windows.values
            .filter { $0.decisionState == .applied || $0.decisionState == .confirmed }
            .sorted { $0.snappedStart < $1.snappedStart }
            .map { (start: $0.snappedStart, end: $0.snappedEnd) }
        for (_, continuation) in segmentContinuations {
            continuation.yield(applied)
        }
    }

    // MARK: - Episode Lifecycle

    /// Begin orchestration for a new episode. Clears all prior state.
    /// - Parameters:
    ///   - analysisAssetId: The analysis asset being played. Continues to
    ///     key windows, decisions, and pre-materialized cue lookups.
    ///   - episodeId: The canonical episode key (the identity unit that
    ///     `EpisodeSurfaceStatusObserver` hashes onto `ready_entered`).
    ///     Required so `auto_skip_fired.episode_id_hash` byte-matches
    ///     `ready_entered.episode_id_hash` for the same episode —
    ///     `false_ready_rate` pairs the two by that hash.
    ///   - podcastId: The podcast's ID, used to load the per-show trust mode.
    func beginEpisode(
        analysisAssetId: String,
        episodeId: String,
        podcastId: String? = nil
    ) async {
        windows.removeAll()
        activeAssetId = analysisAssetId
        activeEpisodeId = episodeId
        activePodcastId = podcastId
        activeEvidenceCatalog = nil
        // playhead-xr3t: clear per-episode inventory-filter context.
        // Episode duration is rehydrated from the persisted asset row
        // immediately below; declared chapters arrive later via
        // `setDeclaredChapters(_:)` from the metadata fetch in
        // `AdDetectionService.runBackfill`.
        activeEpisodeDuration = nil
        activeDeclaredChapters = []
        inAdState = false
        lastSeekTime = nil
        skipSuppressedAfterSeek = false
        currentPlayheadTime = 0
        decisionLog.removeAll()
        banneredWindowIds.removeAll()
        emittedAutoSkipBannerWindowIds.removeAll()
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
        // playhead-98co: per-episode edge-padding state.
        edgeAnchorsByWindowId.removeAll()
        userInitiatedSkipWindowIds.removeAll()

        // Load per-show trust mode.
        if let podcastId, let trustService {
            activeSkipMode = await trustService.effectiveMode(podcastId: podcastId)
        } else {
            activeSkipMode = .shadow
        }

        // playhead-xr3t: hydrate the inventory filter's episode duration
        // from the persisted asset row. Best-effort: an absent row /
        // absent duration leaves `activeEpisodeDuration = nil`, and the
        // filter degrades to "head-edge guard only" (the safer failure
        // mode — under-filter rather than mis-reject on unknown
        // duration). The duration is refreshable mid-episode via
        // `setEpisodeDuration(_:)` once `AnalysisCoordinator`'s
        // duration-backfill probe rewrites the row.
        do {
            if let asset = try await store.fetchAsset(id: analysisAssetId),
               let duration = asset.episodeDurationSec,
               duration > 0,
               duration.isFinite {
                activeEpisodeDuration = duration
            }
        } catch {
            logger.debug(
                "beginEpisode: episode-duration lookup failed for \(analysisAssetId, privacy: .public): \(error.localizedDescription, privacy: .public)"
            )
        }

        // Bug 5 (skip-cues-deletion): pre-load directly from `ad_windows`,
        // filtered to confirmed-confidence rows. Replaces the prior path
        // that read from the now-deleted `skip_cues` table. The 0.7
        // threshold mirrors the cue materializer's threshold so the
        // preload set is byte-identical to what the cue table used to
        // contain at the same point in time. We forward the persisted
        // `AdWindow` rows directly to `receiveAdWindows` so the existing
        // event-stream path applies its standard logic — eligibilityGate,
        // banner state, decision-log dedup, etc. Forwarding the
        // unmodified row preserves any auto-skip / markOnly precision
        // gate stamped at write-time, which a synthesized "confirmed"
        // shape would silently strip.
        //
        // Cycle-21 H-1: preload `ad_windows` and forward through
        // `receiveAdWindows`. Filter excludes only `.suppressed`
        // (terminal "no-skip" — replay wastes memory) and `.reverted`
        // (user explicitly chose "Listen" — replay would risk pushing
        // a cue the user rejected). `.candidate` and `.confirmed` are
        // forwarded so the orchestrator can re-evaluate them.
        // `.applied` is forwarded so a previously-skipped ad pushes its
        // cue again on the next app launch — `evaluateAndPush`'s
        // terminal-state branch (the `decisionState == .applied` arm)
        // appends the row to `eligible` and `pushMergedCues` fires the
        // skip cue. Without that forwarding, cross-launch auto-skip
        // would silently regress: pre-pivot the `skip_cues` table re-
        // cued every confidence-passing row at episode start; now the
        // `ad_windows` rows must do the same job.
        //
        // Banner re-emission for the forwarded `.applied` rows is
        // suppressed by pre-populating `banneredWindowIds` BEFORE the
        // `receiveAdWindows` call. The terminal-state branch in
        // `evaluateAndPush` only emits a banner when
        // `!banneredWindowIds.contains(id)`; pre-populating the set
        // turns the banner emission off for an already-skipped ad
        // without affecting the cue push (the cue push happens via
        // `eligible.append` regardless of the banner gate).
        do {
            let preWindows = try await store.fetchAdWindows(assetId: analysisAssetId)
            let eligible = preWindows.filter {
                $0.confidence >= Self.preloadConfidenceThreshold
                    && $0.endTime > $0.startTime
                    && Self.preloadEligibleDecisionStates.contains($0.decisionState)
            }
            if !eligible.isEmpty {
                let appliedRawValue = SkipDecisionState.applied.rawValue
                for window in eligible where window.decisionState == appliedRawValue {
                    // Cycle-27 T-3 production-writer site (1 of 4): preload pre-population.
                    banneredWindowIds.insert(window.id)
                }
                await receiveAdWindows(eligible)
            }
        } catch {
            logger.warning("Failed to load preload ad_windows: \(error.localizedDescription)")
        }

        logger.info("Begin episode: asset=\(analysisAssetId)")
    }

    /// End orchestration for the current episode.
    func endEpisode() {
        let windowCount = windows.count
        let appliedCount = windows.values.filter { $0.decisionState == .applied }.count
        logger.info("End episode: \(windowCount) windows, \(appliedCount) applied, \(self.decisionLog.count) decisions logged")

        windows.removeAll()
        activeAssetId = nil
        activeEpisodeId = nil
        activePodcastId = nil
        activeEvidenceCatalog = nil
        // playhead-xr3t: clear per-episode inventory-filter context so
        // the next episode doesn't inherit stale duration/chapters.
        activeEpisodeDuration = nil
        activeDeclaredChapters = []
        inAdState = false
        banneredWindowIds.removeAll()
        emittedAutoSkipBannerWindowIds.removeAll()
        suggestBanneredWindowIds.removeAll()
        suggestWindows.removeAll()
        recentlyAcceptedSuggestIds.removeAll()
        pushSkipCues()
    }

    // MARK: - playhead-xr3t: inventory sanity context setters

    /// Update the episode duration used by the inventory sanity filter.
    /// Mirrors the `setEvidenceCatalog` pattern: late-arriving values
    /// are dropped when the active asset has switched (so a duration-
    /// backfill probe finishing mid-podcast-switch can't poison the
    /// new episode's tail-edge guard).
    ///
    /// Pass a non-positive or non-finite `duration` to clear the cached
    /// value (the filter will then degrade to head-edge-only behaviour).
    func setEpisodeDuration(_ duration: Double, analysisAssetId: String) {
        guard let activeAssetId, analysisAssetId == activeAssetId else {
            logger.debug(
                "setEpisodeDuration: dropping mismatched asset \(analysisAssetId, privacy: .public) (active=\(self.activeAssetId ?? "nil", privacy: .public))"
            )
            return
        }
        let previous = activeEpisodeDuration
        if duration > 0, duration.isFinite {
            activeEpisodeDuration = duration
        } else {
            activeEpisodeDuration = nil
        }
        // Re-evaluate managed windows against the freshest context.
        // Skip the pass when the new value is identical to what we
        // already had — nothing the filter sees would change.
        if previous != activeEpisodeDuration {
            reapplyInventoryFilterToManagedWindows()
        }
    }

    /// Push the publisher-declared content chapters for the active
    /// episode. Only creator-source ChapterEvidence (id3, pc20,
    /// rssInline) is retained — `.inferred` chapters are filtered out
    /// here as a defense-in-depth check so the filter cannot
    /// accidentally consult them, even if a future caller mixes
    /// sources.
    ///
    /// Chapters are sorted by `startTime` and any chapter missing an
    /// explicit `endTime` has it synthesized from the NEXT chapter's
    /// start time. The trailing chapter (last in the episode, no
    /// successor) is left open-ended — the filter explicitly does NOT
    /// reject overlap with an unbounded chapter, because over-rejection
    /// in xr3t means the user misses an expected ad-skip (e.g. a
    /// post-roll inside a publisher-declared "Outro" chapter).
    ///
    /// After the chapter context updates, currently-managed non-
    /// terminal windows are re-evaluated against the new filter
    /// context. Spans that the filter would now reject are removed
    /// from the active set (mirrors `retireAdWindows`). Without this
    /// pass, the `beginEpisode` preload (which runs BEFORE
    /// `AdDetectionService.runBackfill` pushes chapters) would let
    /// chapter-overlapping rows survive across launches even though
    /// the filter is supposed to reject them.
    ///
    /// Mismatched-asset pushes are dropped silently (asset-switch race
    /// guard), mirroring `setEvidenceCatalog`.
    func setDeclaredChapters(_ chapters: [ChapterEvidence], analysisAssetId: String) {
        guard let activeAssetId, analysisAssetId == activeAssetId else {
            logger.debug(
                "setDeclaredChapters: dropping mismatched asset \(analysisAssetId, privacy: .public) (active=\(self.activeAssetId ?? "nil", privacy: .public))"
            )
            return
        }
        activeDeclaredChapters = Self.normalizedDeclaredChapters(chapters)
        reapplyInventoryFilterToManagedWindows()
    }

    /// Sort, filter, and synthesize missing chapter end times from the
    /// next chapter's start. The trailing chapter (no successor) keeps
    /// `endTime == nil` and is treated as unbounded by the inventory
    /// filter (no rejection on overlap). See `setDeclaredChapters`
    /// for the rationale.
    private static func normalizedDeclaredChapters(
        _ chapters: [ChapterEvidence]
    ) -> [ChapterEvidence] {
        let creatorOnly = chapters.filter { $0.source.isCreatorSource }
        let sorted = creatorOnly.sorted { $0.startTime < $1.startTime }
        guard !sorted.isEmpty else { return [] }
        var result: [ChapterEvidence] = []
        result.reserveCapacity(sorted.count)
        for index in sorted.indices {
            let chapter = sorted[index]
            if chapter.endTime != nil {
                result.append(chapter)
                continue
            }
            // Synthesize end from the next chapter's start when it is
            // strictly greater than this chapter's start. If the next
            // chapter starts at the same time (degenerate), or the
            // current chapter is the last, leave `endTime` as nil —
            // the filter will skip rule (c) for unbounded chapters.
            let nextIndex = index + 1
            if nextIndex < sorted.count {
                let nextStart = sorted[nextIndex].startTime
                if nextStart > chapter.startTime, nextStart.isFinite {
                    let synthesized = ChapterEvidence(
                        startTime: chapter.startTime,
                        endTime: nextStart,
                        title: chapter.title,
                        source: chapter.source,
                        disposition: chapter.disposition,
                        qualityScore: chapter.qualityScore
                    )
                    result.append(synthesized)
                    continue
                }
            }
            result.append(chapter)
        }
        return result
    }

    /// Re-evaluate currently-managed windows against the inventory
    /// sanity filter using the freshest `activeEpisodeDuration` /
    /// `activeDeclaredChapters` context. Removes any window that the
    /// filter now rejects, subject to the user-already-acted guard
    /// below.
    ///
    /// Called after `setDeclaredChapters` and `setEpisodeDuration` so
    /// preloaded windows (which entered the active set before the
    /// chapter / duration context was available) are belatedly
    /// reconciled with the filter.
    ///
    /// Safety rules:
    ///   * `.reverted` — user explicitly chose "Listen". Never touch.
    ///   * `.applied` — the orchestrator has decided to auto-skip. We
    ///     may still retire IF the playhead has not yet reached the
    ///     window's start: yanking a cue the user is past, or one
    ///     they're currently inside, would be a UX bug. The retire
    ///     is purely the rule "if the filter would have rejected this
    ///     before it ever became user-visible, do so now."
    ///   * `.candidate` / `.confirmed` / `.suppressed` — re-evaluate
    ///     freely; nothing user-visible has happened.
    private func reapplyInventoryFilterToManagedWindows() {
        guard inventoryFilter.isEnabled, !windows.isEmpty else { return }
        var idsToRetire: [String] = []
        for (id, managed) in windows {
            switch managed.decisionState {
            case .reverted:
                continue
            case .applied:
                // Only retire if the playhead has not yet reached the
                // window's start. Otherwise the user has either heard
                // it or is hearing it now — silently dropping the
                // skip cue mid-stream is worse than the false-positive
                // skip we're trying to avoid.
                guard currentPlayheadTime < managed.snappedStart else { continue }
            case .candidate, .confirmed, .suppressed:
                break
            }

            let verdict = inventoryFilter.evaluate(
                startTime: managed.adWindow.startTime,
                endTime: managed.adWindow.endTime,
                episodeDuration: activeEpisodeDuration,
                declaredChapters: activeDeclaredChapters
            )
            if case let .rejected(reason) = verdict {
                logger.info(
                    "AdWindow \(id, privacy: .public) retroactively rejected by inventory sanity filter: \(reason.rawValue, privacy: .public)"
                )
                idsToRetire.append(id)
            }
        }
        guard !idsToRetire.isEmpty else { return }
        for id in idsToRetire {
            windows.removeValue(forKey: id)
            banneredWindowIds.remove(id)
        }
        evaluateAndPush()
    }

    // MARK: - Ad Window Event Stream

    /// Receive new or updated AdWindows from AdDetectionService.
    /// This is the primary event-stream entry point. Called whenever
    /// the detection pipeline produces or updates windows.
    func receiveAdWindows(_ adWindows: [AdWindow]) async {
        guard let assetId = activeAssetId else { return }

        for adWindow in adWindows {
            guard adWindow.analysisAssetId == assetId else { continue }

            let existingState = windows[adWindow.id]?.decisionState

            // Never process a window that was already applied or reverted.
            if existingState == .applied || existingState == .reverted {
                continue
            }

            // playhead-xr3t: post-hoc inventory sanity filter. Runs
            // BEFORE the eligibility-gate decode below so a rejected
            // span never enters the active window set (and therefore
            // never reaches `evaluateAndPush` / banner emission). The
            // filter is a no-op when its feature flag is OFF —
            // identical pre-Phase-3 behaviour, asserted by the rollback
            // tests.
            //
            // Already-managed windows (`existingState != nil`) bypass
            // the filter: if a window made it into the active set on
            // an earlier push it has already been validated, and a
            // mid-episode state update (e.g. a hot-path refresh) MUST
            // NOT silently drop it just because the duration/chapter
            // context arrived in a different order between pushes.
            if existingState == nil {
                let verdict = inventoryFilter.evaluate(
                    startTime: adWindow.startTime,
                    endTime: adWindow.endTime,
                    episodeDuration: activeEpisodeDuration,
                    declaredChapters: activeDeclaredChapters
                )
                if case let .rejected(reason) = verdict {
                    logger.info(
                        "AdWindow \(adWindow.id, privacy: .public) rejected by inventory sanity filter: \(reason.rawValue, privacy: .public)"
                    )
                    continue
                }
            }

            // playhead-gtt9.11: precision gate. A window stamped
            // `eligibilityGate = "markOnly"` is visible in the UI as a
            // possible-ad marker but must never be promoted into the auto-
            // skip path. Mirror the blocked-gate check in
            // `receiveAdDecisionResults` so both entry points honor the
            // precision contract.
            //
            // playhead-gtt9.23: route markOnly windows into the suggest
            // tier so the user can see them and tap-to-skip. The skip
            // path remains untouched — `suggestWindows` is stored
            // separately from `windows` and is never evaluated by
            // `evaluateAndPush()`. The only effect is one banner emission
            // (per window) on the existing `bannerItemStream`, tagged
            // `tier: .suggest` so the UI renders the medium-tier copy.
            //
            // L3: decode the persisted raw value through `SkipEligibilityGate`
            // rather than literal-string compare. The consumer cares about
            // exactly one case (`.markOnly`) — every other value (nil,
            // unknown raw values, and any other `SkipEligibilityGate`
            // case) decodes to a non-`.markOnly` result and falls through
            // to the standard eligible-skip path.
            //
            // Producer note: `AdWindow.eligibilityGate` has multiple
            // writers. The live precision-gate label
            // (`AdDetectionService.precisionGateLabel`, called from
            // both the hot-path post-classify site and the aggregator
            // promotion site) emits `"markOnly"` (which round-trips
            // as `SkipEligibilityGate.markOnly` — the case this
            // decode pins) and `"autoSkip"` (a literal that is NOT a
            // `SkipEligibilityGate` raw value and therefore decodes
            // to nil). Fusion stamps — the full `SkipEligibilityGate`
            // raw-value space, including `.eligible`, the blocked-*
            // cases, and `.cappedByFMSuppression` — originate only in
            // `AdDetectionService.runBackfill` via
            // `buildFusionAdWindow`, which writes
            // `decision.eligibilityGate.rawValue` directly. Those
            // fusion-stamped rows surface to every `receiveAdWindows`
            // caller, NOT just the preload + finalizeBackfill paths:
            //   - cross-launch preload (`beginEpisode`) reads them
            //     from the store on relaunch;
            //   - the final-pass backfill push delivers them
            //     in-memory immediately after `runBackfill`;
            //   - the hot-path push (`AnalysisCoordinator
            //     .handlePersistedTranscriptChunks`) delivers
            //     `runHotPathResult.windows` whose gate is normally
            //     the precision-gate literal, BUT
            //     `reconcileHotPathWindows` builds a `preservedWindow`
            //     that copies `existing.eligibilityGate` from a
            //     previously-persisted `decisionState == .candidate`
            //     row matched in the store. A backfill row written
            //     with `policyAction == .autoSkipEligible` and
            //     `decision.eligibilityGate != .eligible` is persisted
            //     with `decisionState == .candidate` by
            //     `buildFusionAdWindow`'s `policyAction` switch — so
            //     a fresh hot-path window overlapping that row will
            //     inherit the fusion stamp on the hot-path push.
            // The decode here is the producer-aware first half of the
            // guard pair: this branch handles `.markOnly` (the live
            // precision-gate value that round-trips as a known case)
            // by routing to the suggest tier. The companion fusion-
            // blocked-gate guard immediately below — added in
            // playhead-bq70 — restores symmetry with
            // `receiveAdDecisionResults` (which hard-filters to
            // `eligibilityGate == .eligible`) by dropping all other
            // recognised non-eligible cases before they reach
            // `evaluateAndPush`. nil / unknown-raw-value still falls
            // through to the standard managed path (the non-fusion
            // producer contract).
            let decodedGate = adWindow.eligibilityGate.flatMap { SkipEligibilityGate(rawValue: $0) }
            if decodedGate == .markOnly {
                logger.debug(
                    "AdWindow \(adWindow.id, privacy: .public) eligibilityGate=markOnly — surfacing as suggest tier"
                )
                if !suggestBanneredWindowIds.contains(adWindow.id),
                   !banneredWindowIds.contains(adWindow.id) {
                    suggestBanneredWindowIds.insert(adWindow.id)
                    suggestWindows[adWindow.id] = adWindow
                    emitSuggestBanner(for: adWindow)
                }
                continue
            }

            // playhead-bq70: symmetric blocked-gate guard. `receiveAdDecisionResults`
            // hard-filters its inputs to `eligibilityGate == .eligible`; this entry
            // point must honor the same precision contract for fusion-stamped rows
            // surfaced via the AdWindow path. The fusion stamps that originate in
            // `AdDetectionService.runBackfill` via `buildFusionAdWindow` write the
            // full `SkipEligibilityGate.rawValue` space — including the blocked
            // cases (`.blockedByEvidenceQuorum`, `.blockedByPolicy`,
            // `.blockedByUserCorrection`, `.cappedByFMSuppression`). These rows
            // surface to all three `receiveAdWindows` callers (cross-launch
            // preload, hot-path post-classify push, final-pass backfill push) —
            // see the producer-note block above. Without this guard a
            // `policyAction == .autoSkipEligible` row that fusion subsequently
            // demoted via `eligibilityGate != .eligible` (persisted as
            // `decisionState == .candidate`) would silently re-enter the
            // auto-skip path on any of the three callers, violating the
            // precision contract.
            //
            // Semantics chosen to match `receiveAdDecisionResults` exactly:
            // anything that decodes to a recognised `SkipEligibilityGate` case
            // OTHER than `.eligible` (the markOnly branch already returned
            // above) is dropped here. nil / unknown-future-raw-value still
            // flows through to the standard managed path — this preserves the
            // non-fusion producer contract (the live precision-gate label
            // emits `"autoSkip"` and `nil`, both of which decode to nil and
            // are NOT fusion stamps). See playhead-bq70 for the cycle history.
            if let decoded = decodedGate, decoded != .eligible {
                logger.debug(
                    "AdWindow \(adWindow.id, privacy: .public) eligibilityGate=\(decoded.rawValue, privacy: .public) — blocked, not adding to active windows"
                )
                continue
            }

            // playhead-rfu-sad: tap-then-flip race guard. The user
            // already accepted this id via the suggest banner — a
            // promoted UUID-keyed managed window is authoritative for
            // the span. A late-arriving non-markOnly ingest with the
            // SAME original id must NOT register a second `windows[id]`
            // entry: that would emit a duplicate auto-skip banner and a
            // duplicate `auto_skip_fired` audit event for one user
            // skip. The promoted entry has already fired (or will fire)
            // through evaluateAndPush.
            if recentlyAcceptedSuggestIds.contains(adWindow.id) {
                logger.debug(
                    "AdWindow \(adWindow.id, privacy: .public) ignored — already promoted via acceptSuggestedSkip"
                )
                continue
            }

            // playhead-rfu-sad: gate-flip race guard. A window first seen
            // as `markOnly` can later re-arrive with the gate cleared
            // (e.g. fusion now admits it as auto-skip eligible). Without
            // this clear, `suggestWindows[id]` would stay populated
            // while `windows[id]` also gets a parallel managed entry —
            // a still-visible suggest banner could re-fire
            // `acceptSuggestedSkip` and synthesize a duplicate managed
            // window via a fresh `UUID().uuidString` (see
            // `acceptSuggestedSkip`'s `promotedId`).
            if suggestWindows[adWindow.id] != nil {
                suggestWindows.removeValue(forKey: adWindow.id)
                logger.debug(
                    "AdWindow \(adWindow.id, privacy: .public) gate flipped from markOnly — cleared suggest entry"
                )
            }

            let incomingState = SkipDecisionState(rawValue: adWindow.decisionState) ?? .candidate

            // Build or update the managed window.
            let key = idempotencyKey(assetId: assetId, windowId: adWindow.id)

            let managed = ManagedWindow(
                adWindow: adWindow,
                decisionState: incomingState,
                snappedStart: adWindow.startTime,
                snappedEnd: adWindow.endTime,
                idempotencyKey: key,
                cueActive: false
            )
            windows[adWindow.id] = managed
        }

        // Re-evaluate all windows and push updated cues.
        evaluateAndPush()
    }

    func retireAdWindows(ids: Set<String>) async {
        guard !ids.isEmpty else { return }

        for id in ids {
            guard let existing = windows[id] else { continue }
            if existing.decisionState == .applied || existing.decisionState == .reverted {
                continue
            }
            windows.removeValue(forKey: id)
            banneredWindowIds.remove(id)
        }

        evaluateAndPush()
    }

    /// Receive fusion-based AdDecisionResults from AdDetectionService.
    ///
    /// This is the Phase 6 production entry point (playhead-4my.6.4). Replaces the
    /// raw AdWindow path for backfill-sourced decisions. The eligibility gate is
    /// checked before adding windows; blocked results are never promoted to applied.
    ///
    /// - Parameter results: Fusion decisions from BackfillEvidenceFusion + DecisionMapper.
    func receiveAdDecisionResults(_ results: [AdDecisionResult]) async {
        guard !results.isEmpty, let assetId = activeAssetId else { return }

        for result in results {
            guard result.analysisAssetId == assetId else { continue }

            let existingState = windows[result.id]?.decisionState

            // Never process a window that was already applied or reverted.
            if existingState == .applied || existingState == .reverted { continue }

            // Blocked gate: never add blocked results to the active window set.
            guard result.eligibilityGate == .eligible else {
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) gate=blocked — not adding to active windows"
                )
                continue
            }

            // playhead-xr3t: post-hoc inventory sanity filter. Mirrors
            // the symmetric guard in `receiveAdWindows`. Only applied
            // to fresh entries (`existingState == nil`); a result for
            // an already-managed window is allowed through so a
            // mid-episode state update doesn't silently drop it.
            if existingState == nil {
                let verdict = inventoryFilter.evaluate(
                    startTime: result.startTime,
                    endTime: result.endTime,
                    episodeDuration: activeEpisodeDuration,
                    declaredChapters: activeDeclaredChapters
                )
                if case let .rejected(reason) = verdict {
                    logger.info(
                        "AdDecisionResult \(result.id, privacy: .public) rejected by inventory sanity filter: \(reason.rawValue, privacy: .public)"
                    )
                    continue
                }
            }

            // playhead-rfu-sad: tap-then-flip race guard, fusion path.
            // Mirrors the guard in `receiveAdWindows` so a fusion-shared
            // id that promotes from blocked/markOnly to eligible AFTER
            // the user has already accepted the suggest banner doesn't
            // register a second managed window.
            if recentlyAcceptedSuggestIds.contains(result.id) {
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) ignored — already promoted via acceptSuggestedSkip"
                )
                continue
            }

            // playhead-rfu-sad: symmetric gate-flip clear. If a fusion
            // result for this id arrives eligible after the same id was
            // first surfaced as a markOnly suggest entry, drop the
            // suggest bookkeeping so a still-visible banner can't
            // re-fire `acceptSuggestedSkip` against a now-managed
            // window. Mirrors the clear in `receiveAdWindows`.
            if suggestWindows[result.id] != nil {
                suggestWindows.removeValue(forKey: result.id)
                logger.debug(
                    "AdDecisionResult \(result.id, privacy: .public) gate flipped from markOnly — cleared suggest entry"
                )
            }

            let key = idempotencyKey(assetId: assetId, windowId: result.id)

            // Build a synthetic AdWindow from the fusion decision so the existing
            // ManagedWindow + evaluateWindow machinery can handle it unchanged.
            let syntheticWindow = AdWindow(
                id: result.id,
                analysisAssetId: assetId,
                startTime: result.startTime,
                endTime: result.endTime,
                confidence: result.skipConfidence,
                boundaryState: "acousticRefined",
                decisionState: AdDecisionState.confirmed.rawValue,
                detectorVersion: "fusion-v1",
                advertiser: nil, product: nil, adDescription: nil,
                evidenceText: nil, evidenceStartTime: result.startTime,
                metadataSource: "fusion-v1", metadataConfidence: nil,
                metadataPromptVersion: nil,
                wasSkipped: false, userDismissedBanner: false
            )

            let managed = ManagedWindow(
                adWindow: syntheticWindow,
                decisionState: .confirmed,
                snappedStart: result.startTime,
                snappedEnd: result.endTime,
                idempotencyKey: key,
                cueActive: false
            )
            windows[result.id] = managed
        }

        evaluateAndPush()
    }

    // MARK: - Playback State Updates

    /// Update the current playhead position. Called from playback observer.
    func updatePlayheadTime(_ time: TimeInterval) {
        currentPlayheadTime = time

        // Check if seek suppression should be lifted.
        if skipSuppressedAfterSeek, let seekTime = lastSeekTime {
            let elapsed = Date().timeIntervalSince(seekTime)
            if elapsed >= config.seekStabilitySeconds {
                skipSuppressedAfterSeek = false
                logger.info("Skip suppression lifted after \(elapsed, format: .fixed(precision: 1))s stability")
                evaluateAndPush()
            }
        }
    }

    /// Record a user-initiated seek. Suppresses auto-skip until confidence
    /// re-stabilizes.
    func recordUserSeek(to time: TimeInterval) {
        lastSeekTime = Date()
        skipSuppressedAfterSeek = true
        currentPlayheadTime = time
        logger.info("User seek to \(time, format: .fixed(precision: 1))s -- skip suppressed")

        // Do NOT remove existing cues ahead of the new position.
        // Just suppress firing new ones until stability returns.
    }

    /// Record that the user tapped "Listen" to revert a skip.
    /// Also signals the trust engine (if wired) as a false-skip.
    func recordListenRevert(windowId: String, podcastId: String? = nil) async {
        guard var managed = windows[windowId] else { return }
        guard managed.decisionState != .reverted,
              managed.decisionState != .suppressed else { return }

        managed.decisionState = .reverted
        managed.cueActive = false
        windows[windowId] = managed

        logDecision(
            managed: managed,
            decision: .reverted,
            reason: "User tapped Listen"
        )

        // Persist decision state change.
        do {
            try await store.updateAdWindowDecision(
                id: windowId,
                decisionState: SkipDecisionState.reverted.rawValue
            )
        } catch {
            logger.warning("Failed to persist revert for \(windowId): \(error.localizedDescription)")
        }

        // Signal the trust engine about the false skip.
        if let podcastId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: podcastId)
        }

        // Phase 7.2 / playhead-zskc: persist a listenRevert CorrectionEvent
        // with window-precise time scope (fire-and-forget). AdWindow does not
        // carry atom ordinals, so we use the snapped start/end times directly
        // via the `.exactTimeSpan` correction scope.
        persistManualCorrectionVeto(
            startTime: managed.snappedStart,
            endTime: managed.snappedEnd,
            assetId: managed.adWindow.analysisAssetId,
            podcastId: podcastId,
            source: .listenRevert
        )

        // playhead-xsdz.9: a Listen revert is a CONFIRMED false positive —
        // ingest the wrongly-flagged window's ad-copy text as a hard negative
        // so future episodes with the same copy are suppressed. No-op when no
        // bank is wired — and a bank is wired ONLY when the
        // `crossEpisodeMemoryEnabled` feature flag is on (see PlayheadRuntime),
        // so this is inert in the flag-OFF production default.
        ingestNegativeFingerprint(
            text: managed.adWindow.evidenceText,
            podcastId: podcastId
        )

        // playhead-xsdz.11: a Listen revert of an auto-skip is the canonical
        // FALSE-POSITIVE signal — RAISE this show's auto-skip threshold (be more
        // conservative). No-op when no controller store is wired (flag-OFF
        // production default).
        recordThresholdControlSignal(.falsePositive, podcastId: podcastId)

        // Remove the cue and re-push.
        evaluateAndPush()
    }

    /// Revert all managed windows overlapping the given time range.
    /// Used by the "Not an ad" banner and "This isn't an ad" popover paths,
    /// which identify the ad by its time span rather than a specific windowId.
    ///
    /// playhead-hygc.1.8: also reverts overlapping `suggestWindows` (markOnly
    /// AdWindows surfaced as suggest-tier banners). Prior to this change
    /// `revertByTimeRange` only iterated `windows` (the auto-skip eligible
    /// dictionary), leaving algorithmic markOnly entries that the user has
    /// explicitly said weren't ads still visible on the timeline / available
    /// to be promoted via `acceptSuggestedSkip`. The May 6 dogfood eval found
    /// 8 of 12 falsePositive corrections were against markOnly windows — the
    /// suggest tier is the user-facing surface for borderline ads, so it must
    /// honor user vetoes the same way the auto-skip surface does.
    func revertByTimeRange(start: Double, end: Double, podcastId: String?) async {
        var revertedAny = false
        // Cycle 1 M2: track whether the gesture actually vetoed a managed
        // auto-skip window vs. only a suggest-tier banner. The R6 comment
        // below explicitly called out routing as the right fix when
        // calibration showed over-penalization; we're applying that fix.
        var revertedManagedAny = false
        // playhead-zskc: one user gesture produces one correction event — not
        // N events per overlapping window. Capture the analysisAssetId of any
        // reverted window (across BOTH the managed-window loop and the
        // playhead-hygc.1.8 suggest-tier loop below) so we can write a single
        // CorrectionEvent after both loops complete. (All managed and
        // suggest-tier windows on the orchestrator share the current
        // episode's assetId; if they ever diverge mid-transition, attributing
        // to the first-matched window is still more correct than writing N
        // duplicates.)
        var assetIdForVeto: String?

        for (id, var managed) in windows {
            // Skip already-terminal states that aren't active.
            guard managed.decisionState != .reverted,
                  managed.decisionState != .suppressed else { continue }

            // Check overlap: window overlaps [start, end] if
            // windowStart < end && windowEnd > start.
            guard managed.snappedStart < end, managed.snappedEnd > start else { continue }

            managed.decisionState = .reverted
            managed.cueActive = false
            windows[id] = managed
            revertedAny = true
            revertedManagedAny = true
            if assetIdForVeto == nil {
                assetIdForVeto = managed.adWindow.analysisAssetId
            }

            logDecision(
                managed: managed,
                decision: .reverted,
                reason: "User correction: not an ad (time range)"
            )

            // Persist decision state change.
            do {
                try await store.updateAdWindowDecision(
                    id: id,
                    decisionState: SkipDecisionState.reverted.rawValue
                )
            } catch {
                logger.warning("Failed to persist revert for \(id): \(error.localizedDescription)")
            }
        }

        // playhead-hygc.1.8: mirror the loop above for markOnly suggest-tier
        // windows. These never enter `windows` (suggest tier is intentionally
        // isolated from the auto-skip evaluation loop) so without this pass
        // the user's veto would bounce off the mark-only surface entirely.
        // We remove them outright rather than mark `decisionState = .reverted`
        // in-memory because suggestWindows is a UI-surface dictionary, not a
        // hysteresis state machine — once vetoed the suggest banner must
        // disappear and never re-emit. The persisted AdWindow row gets
        // `decisionState = .reverted` so a re-launch / replay does not
        // resurface the entry.
        //
        // R2 (hygc.1.8): snapshot the matching entries BEFORE mutating
        // `suggestWindows`. Mutating a Swift Dictionary mid-iteration is
        // documented as undefined behavior — even though COW happens to
        // make the current loop survive in practice, depending on that is
        // a maintenance hazard. Build the work list first, then mutate.
        let suggestRevertTargets: [(id: String, window: AdWindow)] =
            suggestWindows.compactMap { (id, suggested) in
                guard suggested.startTime < end, suggested.endTime > start else { return nil }
                return (id, suggested)
            }

        for (id, suggested) in suggestRevertTargets {
            suggestWindows.removeValue(forKey: id)
            // Also clear from the bannered set so a future ingest with the
            // same id doesn't immediately re-emit the suggest banner.
            suggestBanneredWindowIds.remove(id)
            revertedAny = true
            if assetIdForVeto == nil {
                assetIdForVeto = suggested.analysisAssetId
            }

            logger.info(
                "Revert (suggest tier): id=\(id, privacy: .public) range=[\(suggested.startTime), \(suggested.endTime)]"
            )

            do {
                try await store.updateAdWindowDecision(
                    id: id,
                    decisionState: SkipDecisionState.reverted.rawValue
                )
            } catch {
                logger.warning("Failed to persist suggest-tier revert for \(id): \(error.localizedDescription)")
            }
        }

        if revertedAny {
            // Persist a single manualVeto CorrectionEvent with precise time
            // scope per gesture. playhead-zskc: use the user-supplied
            // `start`/`end` (the time range the user identified) rather than
            // the managed window's snapped boundaries, so the correction
            // matches what the user actually gestured against when multiple
            // overlapping windows intersect the range.
            if let assetId = assetIdForVeto {
                persistManualCorrectionVeto(
                    startTime: start,
                    endTime: end,
                    assetId: assetId,
                    podcastId: podcastId,
                    source: .manualVeto
                )
            }

            // Signal trust engine once per user correction, not per window.
            //
            // Cycle 1 M2 (was playhead-hygc.1.8 R6): route by which surface
            // the user actually vetoed. A managed auto-skip revert is a
            // strong negative signal (the algorithm pre-committed and was
            // wrong) — use full-magnitude `recordFalseSkipSignal`. A revert
            // that touched ONLY the suggest-tier (markOnly) loop is a
            // weaker disagreement: the algorithm offered a banner and the
            // user said no, but no playback was ever altered. Use
            // `recordWeakFalseSkipSignal` so suggest-only reverts don't
            // skew trust scores at the same rate as auto-skip vetoes
            // (especially for new podcasts where the trust score is most
            // sensitive to early signal noise).
            if let podcastId, let trustService {
                if revertedManagedAny {
                    await trustService.recordFalseSkipSignal(podcastId: podcastId)
                } else {
                    await trustService.recordWeakFalseSkipSignal(podcastId: podcastId)
                }
            }

            // playhead-xsdz.11: feed the per-show threshold controller a
            // FALSE-POSITIVE signal ONLY when a MANAGED auto-skip window was
            // reverted — that is the "listened through an auto-skip" event the
            // bead names. A suggest-tier-only revert never altered playback, so
            // it is too weak to RAISE the auto-skip threshold (mirrors the
            // full-vs-weak trust-signal routing above). No-op when no store is
            // wired (flag-OFF production default).
            if revertedManagedAny {
                recordThresholdControlSignal(.falsePositive, podcastId: podcastId)
            }
            evaluateAndPush()
        }
    }

    /// Revert a specific window by ID using the manualVeto source.
    /// Same as recordListenRevert but uses .manualVeto correction source
    /// and does not imply a playback rewind.
    func revertWindow(windowId: String, podcastId: String? = nil) async {
        guard var managed = windows[windowId] else { return }
        guard managed.decisionState != .reverted,
              managed.decisionState != .suppressed else { return }

        managed.decisionState = .reverted
        managed.cueActive = false
        windows[windowId] = managed

        logDecision(
            managed: managed,
            decision: .reverted,
            reason: "User correction: not an ad (banner)"
        )

        // Persist decision state change.
        do {
            try await store.updateAdWindowDecision(
                id: windowId,
                decisionState: SkipDecisionState.reverted.rawValue
            )
        } catch {
            logger.warning("Failed to persist revert for \(windowId): \(error.localizedDescription)")
        }

        // Signal the trust engine about the false skip.
        if let podcastId, let trustService {
            await trustService.recordFalseSkipSignal(podcastId: podcastId)
        }

        // Persist a manualVeto CorrectionEvent with precise time scope.
        // playhead-zskc: use the managed window's snapped start/end so the
        // correction carries per-window precision rather than whole-episode.
        persistManualCorrectionVeto(
            startTime: managed.snappedStart,
            endTime: managed.snappedEnd,
            assetId: managed.adWindow.analysisAssetId,
            podcastId: podcastId,
            source: .manualVeto
        )

        // playhead-xsdz.11: a manual "not an ad" veto of a managed auto-skip
        // window is a confirmed FALSE POSITIVE — RAISE this show's threshold.
        // No-op when no controller store is wired (flag-OFF production default).
        recordThresholdControlSignal(.falsePositive, podcastId: podcastId)

        evaluateAndPush()
    }

    // MARK: - Correction persistence helper (playhead-zskc)

    /// Fire-and-forget a `.exactTimeSpan` CorrectionEvent through the
    /// injected correction store. Centralises the three manual-veto call
    /// sites (`recordListenRevert`, `revertByTimeRange`, `revertWindow`) so
    /// actor-isolated capture ritual and nil-store guard live in one place.
    /// playhead-rfu-sad: LRU bookkeeping for the tap-then-flip race
    /// guard. Insert if not already present, evict the oldest id when
    /// the bounded set is full. The set is small enough that a linear
    /// scan is cheaper than maintaining a parallel hash for membership.
    private func rememberAcceptedSuggestId(_ id: String) {
        if let existing = recentlyAcceptedSuggestIds.firstIndex(of: id) {
            // Move-to-front: refresh recency.
            recentlyAcceptedSuggestIds.remove(at: existing)
        }
        recentlyAcceptedSuggestIds.append(id)
        if recentlyAcceptedSuggestIds.count > recentlyAcceptedSuggestCapacity {
            recentlyAcceptedSuggestIds.removeFirst(
                recentlyAcceptedSuggestIds.count - recentlyAcceptedSuggestCapacity
            )
        }
    }

    private func persistManualCorrectionVeto(
        startTime: Double,
        endTime: Double,
        assetId: String,
        podcastId: String?,
        source: CorrectionSource
    ) {
        guard let correctionStore else { return }
        let store = correctionStore
        let pid = podcastId
        Task {
            await store.recordVeto(
                startTime: startTime,
                endTime: endTime,
                assetId: assetId,
                podcastId: pid,
                source: source
            )
        }
    }

    /// playhead-xsdz.9: confirmed-FP WRITE TRIGGER for the hard-negative bank.
    /// Called from the user-reversion seams (Listen revert / "not an ad")
    /// with the wrongly-flagged window's ad-copy text. Fire-and-forget; never
    /// throws (a bank-write failure must not break playback). No-op when no
    /// bank is wired or the text is empty.
    ///
    /// MEMORY-POLLUTION GUARD: this is the ONLY orchestrator seam that writes to
    /// the negative bank, and it is reached ONLY from reversion paths — never
    /// from the auto-skip-eligible path. The bank therefore ingests confirmed
    /// FPs exclusively.
    private func ingestNegativeFingerprint(text: String?, podcastId: String?) {
        guard let bank = negativeFingerprintBank else { return }
        guard let text, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        let pid = podcastId
        Task {
            do {
                _ = try await bank.recordConfirmedFalsePositive(text: text, showId: pid)
            } catch {
                // Non-fatal: a hard-negative we failed to record just means a
                // future near-match isn't suppressed. Playback is unaffected.
            }
        }
    }

    /// playhead-xsdz.11: per-show PI-controller WRITE TRIGGER.
    ///
    /// Folds one correction signal into the show's threshold-controller state:
    ///   • `.falsePositive` — the user listened through / reverted an
    ///     auto-skipped (or markOnly) window → RAISE the show's threshold.
    ///   • `.miss` — the user accepted a suggested skip we did NOT auto-skip
    ///     ("this WAS an ad") → LOWER the show's threshold.
    ///
    /// Fire-and-forget; never throws (a controller-write failure must not break
    /// playback). No-op when no store is wired (the flag-OFF production default)
    /// or `podcastId` is absent — the controller is per-show, so an anonymous
    /// correction has nowhere to land.
    private func recordThresholdControlSignal(
        _ signal: ThresholdControlSignal,
        podcastId: String?
    ) {
        guard let store = perShowThresholdControllerStore else { return }
        guard let podcastId, !podcastId.isEmpty else { return }
        Task {
            do {
                _ = try await store.record(signal: signal, forShow: podcastId)
            } catch {
                // Non-fatal: a missed controller update just means this show's
                // threshold doesn't move this once. Playback is unaffected.
            }
        }
    }

    /// playhead-xsdz.11: the defined MISS-side API. The user scrubbed through /
    /// reported undetected ad content on this show — LOWER the threshold (be
    /// more aggressive). Currently wired at the `acceptSuggestedSkip` seam (the
    /// only "we missed an ad" gesture that reaches this layer). Public so the
    /// false-negative-report UI path can call it directly when wired.
    func recordThresholdControlMiss(podcastId: String?) {
        recordThresholdControlSignal(.miss, podcastId: podcastId)
    }

    /// playhead-gtt9.23: User tapped "Skip" on a suggest-tier banner.
    /// Promotes the markOnly window into the active skip path with a
    /// user-confirmed confidence so the existing skip-cue machinery handles
    /// playback transport and persistence. Also records a `.falseNegative`
    /// CorrectionEvent — the user has just told us "this WAS an ad we
    /// didn't auto-skip," which is exactly the calibration signal that
    /// future threshold tuning needs.
    ///
    /// No-op when the window is not in the suggest set (e.g. already
    /// auto-skipped, dismissed, or never registered as markOnly).
    func acceptSuggestedSkip(windowId: String) async {
        guard let suggested = suggestWindows.removeValue(forKey: windowId) else { return }

        // playhead-rfu-sad: remember this id was promoted via tap so a
        // late-arriving ingest with the same id and a cleared gate
        // (tap-then-flip race) doesn't register a SECOND managed window
        // alongside the UUID-keyed entry produced below. See the LRU
        // check in `receiveAdWindows`.
        rememberAcceptedSuggestId(windowId)

        // Build a fresh confirmed AdWindow with the suggest window's span and
        // confidence pinned to 1.0. We deliberately do not reuse the original
        // markOnly window's id — its eligibilityGate would block it again.
        let promotedId = UUID().uuidString
        let assetId = suggested.analysisAssetId
        let promoted = AdWindow(
            id: promotedId,
            analysisAssetId: assetId,
            startTime: suggested.startTime,
            endTime: suggested.endTime,
            confidence: 1.0,
            boundaryState: "userConfirmedSuggested",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: suggested.detectorVersion,
            advertiser: suggested.advertiser,
            product: suggested.product,
            adDescription: suggested.adDescription,
            evidenceText: suggested.evidenceText,
            evidenceStartTime: suggested.evidenceStartTime,
            metadataSource: suggested.metadataSource,
            metadataConfidence: suggested.metadataConfidence,
            metadataPromptVersion: suggested.metadataPromptVersion,
            wasSkipped: false,
            userDismissedBanner: false
        )

        let key = idempotencyKey(assetId: assetId, windowId: promotedId)
        let managed = ManagedWindow(
            adWindow: promoted,
            decisionState: .confirmed,
            snappedStart: promoted.startTime,
            snappedEnd: promoted.endTime,
            idempotencyKey: key,
            cueActive: false
        )
        windows[promotedId] = managed

        logDecision(
            managed: managed,
            decision: .confirmed,
            reason: "User accepted suggested skip"
        )

        // Calibration signal: record a `.falseNegative` veto on the
        // suggest window's span. Same correction surface the
        // "Hearing an ad" button uses, so the existing trust pipeline
        // (Bug 4b: FN drops trust by `falseSignalPenalty`, mirroring FP)
        // picks it up unchanged.
        persistManualCorrectionVeto(
            startTime: suggested.startTime,
            endTime: suggested.endTime,
            assetId: assetId,
            podcastId: activePodcastId,
            source: .falseNegative
        )

        if let podcastId = activePodcastId, let trustService {
            await trustService.recordFalseNegativeSignal(podcastId: podcastId)
        }

        // playhead-xsdz.11: accepting a suggested skip we did NOT auto-skip is a
        // MISS signal ("this WAS an ad") — LOWER this show's auto-skip threshold
        // (be more aggressive). This is the cleanest miss-side gesture that
        // reaches the orchestrator; a literal "scrubbed through undetected
        // content" signal does not exist at this layer. No-op when no controller
        // store is wired (flag-OFF production default).
        recordThresholdControlMiss(podcastId: activePodcastId)

        evaluateAndPush()
    }

    /// playhead-gtt9.23: User dismissed a suggest-tier banner without
    /// skipping. We treat this as a soft "leave it alone" signal: the
    /// suggest window is dropped from the in-memory suggest set so its
    /// cue is gone from the UI, but no skip happens, no veto is recorded,
    /// and no trust signal fires. Future calibration work (gtt9.3) may
    /// promote this to an explicit decline signal; for now silence is
    /// the conservative reading.
    func declineSuggestedSkip(windowId: String) async {
        guard suggestWindows.removeValue(forKey: windowId) != nil else { return }
        logger.debug("User dismissed suggest banner for window \(windowId, privacy: .public)")
    }

    /// User tapped "Skip Ad" in manual mode. Promotes a confirmed window
    /// to applied and fires the skip cue.
    func applyManualSkip(windowId: String) async {
        guard var managed = windows[windowId] else { return }
        guard managed.decisionState == .confirmed else { return }

        // playhead-98co: a manual "Skip Ad" tap is user-initiated — the
        // window (an ordinary detection row) is exempt from edge padding
        // so the user's chosen span skips exactly.
        userInitiatedSkipWindowIds.insert(windowId)

        managed.decisionState = .applied
        managed.cueActive = true
        windows[windowId] = managed

        logDecision(
            managed: managed,
            decision: .applied,
            reason: "Manual skip by user"
        )

        // playhead-rfu-sad: emit `auto_skip_fired` for the manual-skip path
        // too. The ol05 audit log treats every applied window as a real
        // skip — manual taps and auto-mode promotions are equivalent
        // skips from the user's perspective and from the
        // `false_ready_rate` denominator's perspective. Hashing routes
        // through the same `episodeIdHasher` as the auto path so events
        // pair byte-identically with `ready_entered`.
        //
        // Shadow mode is a "log-only, never actually skip" mode — the
        // auto path explicitly does NOT emit (see evaluateWindow's
        // .shadow case). Mirror that gate here so a manual-skip tap
        // delivered in shadow mode (e.g. test harness, dogfood
        // toggle) doesn't pollute the audit log with a real skip
        // event that never produced a real user-facing skip.
        if activeSkipMode != .shadow {
            emitAutoSkipFiredAuditEvent(for: managed)
        }

        // Persist.
        let id = managed.adWindow.id
        Task { [store, logger] in
            do {
                try await store.updateAdWindowDecision(
                    id: id,
                    decisionState: SkipDecisionState.applied.rawValue
                )
                try await store.updateAdWindowWasSkipped(id: id, wasSkipped: true)
            } catch {
                logger.warning("Failed to persist manual skip for \(id): \(error.localizedDescription)")
            }
        }

        evaluateAndPush()
    }

    /// The active skip mode for the current episode.
    func currentSkipMode() -> SkipMode {
        activeSkipMode
    }

    /// Override the active skip mode for the current episode and re-evaluate pending windows.
    func setActiveSkipMode(_ mode: SkipMode) {
        activeSkipMode = mode
        evaluateAndPush()
    }

    /// Windows in the confirmed state (available for manual skip UI).
    func confirmedWindows() -> [AdWindow] {
        windows.values
            .filter { $0.decisionState == .confirmed }
            .sorted { $0.snappedStart < $1.snappedStart }
            .map(\.adWindow)
    }

    // MARK: - Decision Log Access

    /// Return the decision log for the evaluation harness.
    func getDecisionLog() -> [SkipDecisionRecord] {
        decisionLog
    }

    func activeWindowIDs() -> Set<String> {
        Set(windows.keys)
    }

    /// playhead-hygc.1.8: snapshot of suggest-tier (markOnly) window IDs.
    /// Mirrors `activeWindowIDs()` for the auto-skip dictionary and is
    /// used by revert-tier coverage tests to assert that a user veto via
    /// `revertByTimeRange` actually clears suggest entries.
    func activeSuggestWindowIDs() -> Set<String> {
        Set(suggestWindows.keys)
    }

    /// Snapshot of window IDs for which `emitBannerItem` actually
    /// reached the yield-to-subscriber path this episode. The set is
    /// populated ONLY by emission to active subscribers, never by
    /// pre-population — so a `.contains(id) == false` assertion proves
    /// no banner was emitted regardless of `evaluateAndPush` iteration
    /// order. Used by `testPreloadedAppliedWindowDoesNotEmitBanner` and
    /// `testEndEpisodeResetsEmittedAutoSkipBannersSet`.
    ///
    /// Cycle-25 L-2 precision: `emitBannerItem` early-returns when
    /// `bannerContinuations` is empty (no UI listening). The set is
    /// therefore populated only when emission has both a window AND
    /// a subscriber. Tests that rely on `.contains(id) == true` must
    /// subscribe to `bannerItemStream()` BEFORE `beginEpisode`,
    /// otherwise the emission never reaches the insert site and the
    /// snapshot stays empty (correctly — no banner was actually shown).
    ///
    /// Cycle-23 L-2: this is test-only observability — production
    /// callers must not couple to it.
    ///
    /// Cycle-28 L-C / Cycle-29 L-1 cross-reference: see the field doc
    /// on `emittedAutoSkipBannerWindowIds` for *why* this set exists
    /// instead of a snapshot of `banneredWindowIds`. The 4 production
    /// writers of `banneredWindowIds` are enumerated in `emitBannerItem`'s
    /// comment; of those, only `beginEpisode`'s preload pre-population
    /// inserts WITHOUT a corresponding `emitBannerItem` call, so it is
    /// the unique source of gate-snapshot ambiguity: a `banneredWindowIds`
    /// snapshot cannot distinguish "preload pre-populated this id" from
    /// "eval-loop emitted then inserted." This emission set is populated
    /// only by `emitBannerItem` and only after the subscriber-gate, so
    /// its absence/presence is unambiguous emission evidence regardless
    /// of preload state.
    func emittedAutoSkipBannersSnapshot() -> Set<String> {
        emittedAutoSkipBannerWindowIds
    }

    // MARK: - Core Skip Policy

    /// Evaluate all managed windows and determine which should have active
    /// skip cues. Applies hysteresis, merging, minimum span, seek suppression.
    private func evaluateAndPush() {
        guard activeAssetId != nil else { return }

        // 1. Collect eligible windows (confirmed or candidate with sufficient confidence).
        //    Sort by snappedStart so hysteresis (inAdState) is evaluated in temporal order.
        var eligible: [ManagedWindow] = []
        let sortedWindows = windows.sorted { $0.value.snappedStart < $1.value.snappedStart }
        for (id, var managed) in sortedWindows {
            // Skip already-terminal states.
            if managed.decisionState == .applied
                || managed.decisionState == .suppressed
                || managed.decisionState == .reverted {
                // Keep applied windows as active cues.
                if managed.decisionState == .applied {
                    // Emit a banner on first encounter (e.g. after applyManualSkip).
                    if !banneredWindowIds.contains(managed.adWindow.id) {
                        // Cycle-27 T-3 production-writer site (2 of 4): evaluateAndPush terminal-state branch.
                        banneredWindowIds.insert(managed.adWindow.id)
                        emitBannerItem(for: managed)
                    }
                    eligible.append(managed)
                }
                continue
            }

            let previousState = managed.decisionState
            let decision = evaluateWindow(&managed)
            if decision != previousState {
                managed.decisionState = decision
                windows[id] = managed
            }

            // Emit a banner the first time a window reaches .confirmed or .applied.
            if (decision == .confirmed || decision == .applied),
               !banneredWindowIds.contains(managed.adWindow.id) {
                // Cycle-27 T-3 production-writer site (3 of 4): evaluateAndPush promotion branch.
                banneredWindowIds.insert(managed.adWindow.id)
                emitBannerItem(for: managed)
            }

            if decision == .applied {
                eligible.append(managed)
            }
        }

        // 2. Compute each window's playback SKIP SPAN (playhead-98co:
        //    identity when edge padding is OFF — the default; the derived
        //    late-safe margins when ON), then merge adjacent spans with
        //    small gaps. Padding applies ONLY to the skip cues pushed to
        //    playback: banners, decision records, and the applied-segment
        //    broadcast below all keep the full snapped span.
        let skipSpans = eligible.compactMap { paddedCueSpan(for: $0) }
        let merged = mergeAdjacentWindows(skipSpans)

        // 3. Push skip cues to PlaybackService.
        pushMergedCues(merged)

        // 4. Broadcast updated segments to UI listeners.
        broadcastAppliedSegments()
    }

    // MARK: - playhead-98co: edge-padded skip spans

    /// The playback skip span for a managed window: the snapped span when
    /// edge padding is disabled or the skip is user-initiated; otherwise
    /// the `AutoSkipEdgePadding` late-safe window (shrink-only), or nil
    /// when no late-safe window exists (cue suppressed — the span keeps
    /// its banner/marker surfacing but is never auto-skipped).
    ///
    /// Also consulted by `evaluateWindow`'s auto-mode veto so a span with
    /// no late-safe window is demoted to `.confirmed` (markOnly behavior)
    /// BEFORE the `.applied` promotion — no `auto_skip_fired` audit event
    /// and no `inAdState` flip for a skip that will never fire.
    private func paddedCueSpan(for managed: ManagedWindow) -> (start: Double, end: Double)? {
        guard edgePaddingEnabled, !isUserInitiatedSkip(managed) else {
            return (start: managed.snappedStart, end: managed.snappedEnd)
        }
        // Per-edge anchor provenance is not yet persisted on AdWindow rows
        // (stinger snap traces and rediff slot provenance live inside
        // AdDetectionService). Until the Gate-2 stamping bead lands and
        // populates `setEdgeAnchors`, every pipeline edge classifies
        // `.unanchored` — under the derived policy that means flag-ON
        // auto-skips nothing, the intended conservative posture.
        let anchors = edgeAnchorsByWindowId[managed.adWindow.id]
            ?? (start: .unanchored, end: .unanchored)
        return AutoSkipEdgePadding.skipWindow(
            spanStart: managed.snappedStart,
            spanEnd: managed.snappedEnd,
            startAnchor: anchors.start,
            endAnchor: anchors.end,
            showKey: activePodcastId
        )
    }

    /// Whether this window's skip was explicitly user-initiated — exempt
    /// from edge padding (the user chose the span deliberately).
    private func isUserInitiatedSkip(_ managed: ManagedWindow) -> Bool {
        if userInitiatedSkipWindowIds.contains(managed.adWindow.id) {
            return true
        }
        switch managed.adWindow.boundaryState {
        case "userMarked", "userConfirmedSuggested":
            return true
        default:
            return false
        }
    }

    /// Evaluate a single window against skip policy. Returns the decision.
    private func evaluateWindow(_ managed: inout ManagedWindow) -> SkipDecisionState {
        let confidence = managed.adWindow.confidence
        let span = managed.snappedEnd - managed.snappedStart

        // Late detection: if the playhead is already past this window, never skip.
        if managed.snappedEnd <= currentPlayheadTime {
            let decision = SkipDecisionState.suppressed
            logDecision(managed: managed, decision: decision, reason: "Late detection -- playhead past window end")
            return decision
        }

        // Seek suppression: if user recently seeked, suppress new skips.
        if skipSuppressedAfterSeek {
            // Don't change state -- just don't promote to applied yet.
            return managed.decisionState
        }

        // Hysteresis: different thresholds for entering vs staying in ad state.
        let threshold = inAdState ? config.stayThreshold : config.enterThreshold

        if confidence < threshold {
            // Below threshold -- suppress if it was candidate.
            if managed.decisionState == .candidate {
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Below hysteresis threshold (\(confidence) < \(threshold))")
                return decision
            }
            // Confirmed but below stay threshold -- exit ad state.
            if managed.decisionState == .confirmed && confidence < config.stayThreshold {
                inAdState = false
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Exiting ad state: confidence dropped below stay threshold")
                return decision
            }
            return managed.decisionState
        }

        // Minimum span check.
        if span < config.minimumSpanSeconds {
            // Allow short spans only with very strong evidence.
            if confidence < config.shortSpanOverrideConfidence {
                let decision = SkipDecisionState.suppressed
                logDecision(managed: managed, decision: decision, reason: "Span too short (\(span)s < \(config.minimumSpanSeconds)s) without strong evidence")
                return decision
            }
        }

        // Boundary stability: only skip if the window boundary is stable
        // (not still being refined by incoming detection events).
        // Confirmed windows are considered stable; candidates must wait
        // for confirmation unless confidence is exceptionally high.
        if managed.decisionState == .candidate {
            // Candidates need confirmation before skipping.
            // In auto mode (trusted show), promote candidates above the
            // enter threshold without waiting for backfill confirmation.
            // Otherwise, only override if confidence is very high.
            if activeSkipMode == .auto && confidence >= config.enterThreshold {
                // Promote to confirmed — fall through to trust mode gate.
                managed.decisionState = .confirmed
            } else if confidence < config.shortSpanOverrideConfidence {
                return managed.decisionState
            }
        }

        // Trust mode gate: shadow mode logs only; manual mode marks confirmed
        // but does not auto-skip (UI shows a manual "Skip Ad" button instead).
        switch activeSkipMode {
        case .shadow:
            let decision = SkipDecisionState.confirmed
            logDecision(managed: managed, decision: decision, reason: "Shadow mode -- detection logged, no skip fired")
            return decision
        case .manual:
            let decision = SkipDecisionState.confirmed
            logDecision(managed: managed, decision: decision, reason: "Manual mode -- confirmed, awaiting user tap")
            return decision
        case .auto:
            // playhead-98co: edge-padding eligibility veto. When the
            // policy is enabled and this span has no late-safe skip
            // window (start edge unanchored/demoted, or the derived
            // margins consume the span), keep it .confirmed — markOnly
            // behavior: banner surfaces, no skip cue, no auto_skip_fired
            // audit event, no inAdState flip. Flag OFF (the default) or a
            // user-initiated skip always passes (paddedCueSpan returns
            // the snapped span unchanged).
            if paddedCueSpan(for: managed) == nil {
                let decision = SkipDecisionState.confirmed
                logDecision(
                    managed: managed,
                    decision: decision,
                    reason: "Edge padding: no late-safe skip window (start unanchored/demoted or margins consume span) -- markOnly"
                )
                return decision
            }
            break // Proceed to auto-skip below.
        }

        // All checks passed -- apply the skip.
        inAdState = true
        let decision = SkipDecisionState.applied
        logDecision(managed: managed, decision: decision, reason: "Skip policy accepted (auto mode)")

        // playhead-o45p: emit an auto_skip_fired event to the ol05 state-
        // transition log. Paired with readyEntered events on the same
        // episode_id_hash, this is the numerator/denominator source for
        // the Wave 4 false_ready_rate dogfood metric.
        emitAutoSkipFiredAuditEvent(for: managed)

        // Persist to SQLite (fire-and-forget from the actor).
        let windowId = managed.adWindow.id
        Task { [store, logger] in
            do {
                try await store.updateAdWindowDecision(
                    id: windowId,
                    decisionState: SkipDecisionState.applied.rawValue
                )
                try await store.updateAdWindowWasSkipped(id: windowId, wasSkipped: true)
            } catch {
                logger.warning("Failed to persist skip state for \(windowId): \(error.localizedDescription)")
            }
        }

        return decision
    }

    // MARK: - Audit Event Emission

    /// playhead-o45p / playhead-rfu-sad: emit an `auto_skip_fired` audit
    /// event for a window that has just transitioned to `.applied`.
    ///
    /// Hashing routes through `episodeIdHasher` so all skip-event
    /// producers (auto-mode evaluation, manual taps) stamp byte-identical
    /// episode hashes — `false_ready_rate` correlation breaks the moment
    /// hashes diverge across producers. Called from EVERY site that
    /// finalises a real skip:
    ///   - `evaluateWindow` auto-mode promotion
    ///   - `applyManualSkip` (manual user tap on a confirmed window)
    /// The suggested-skip path (`acceptSuggestedSkip`) builds a confirmed
    /// `ManagedWindow` and re-evaluates; whichever of the two sites above
    /// fires next picks up the emission.
    private func emitAutoSkipFiredAuditEvent(for managed: ManagedWindow) {
        guard let episodeId = activeEpisodeId else { return }
        let hashed = episodeIdHasher(episodeId)
        let startMs = Int((managed.snappedStart * 1000.0).rounded())
        let endMs = Int((managed.snappedEnd * 1000.0).rounded())
        invariantLogger.recordAutoSkipFired(
            episodeIdHash: hashed,
            windowStartMs: startMs,
            windowEndMs: endMs
        )
    }

    // MARK: - Window Merging

    /// Merge adjacent skip spans with gaps smaller than mergeGapSeconds.
    /// (playhead-98co: takes the already-computed skip spans — snapped
    /// bounds when edge padding is OFF, padded bounds when ON — so the
    /// merge semantics are identical in both states.)
    private func mergeAdjacentWindows(_ spans: [(start: Double, end: Double)]) -> [(start: Double, end: Double)] {
        let sorted = spans.sorted { $0.start < $1.start }
        guard let first = sorted.first else { return [] }

        var merged: [(start: Double, end: Double)] = []
        var currentStart = first.start
        var currentEnd = first.end

        for span in sorted.dropFirst() {
            if span.start <= currentEnd + config.mergeGapSeconds {
                // Merge: extend the current range.
                currentEnd = max(currentEnd, span.end)
            } else {
                // Gap too large: emit current range, start new one.
                merged.append((start: currentStart, end: currentEnd))
                currentStart = span.start
                currentEnd = span.end
            }
        }

        merged.append((start: currentStart, end: currentEnd))
        return merged
    }

    // MARK: - Cue Pushing

    /// Convert merged ranges to CMTimeRanges and push to PlaybackService.
    ///
    /// playhead-vn7n.2: each merged range's trailing edge is pulled in by
    /// `adTrailingCushionSeconds`, ceding a small sliver of ad-tail rather than
    /// risking a clip into program-start audio. Cushion is applied per pod
    /// (per merged range), not per individual ad — by construction of
    /// `mergeAdjacentWindows`, anything beyond a merged range is either
    /// program audio (gap > `mergeGapSeconds`) or end-of-episode, so it is
    /// safe to apply the cushion uniformly to every range end. End is
    /// clamped at the pod's start so the skip end never precedes the skip
    /// start (e.g., a 5 s ad with a 10 s cushion collapses to a zero-length
    /// cue at `adStart`).
    private func pushMergedCues(_ ranges: [(start: Double, end: Double)]) {
        // playhead-vn7n.1: diagnostic — log each cue we are about to push so we
        // can compare cue.end against the underlying detection AdWindow.endTime
        // (which has not been snap-expanded). The closest managed ad window
        // covering the cue range is selected as the underlying detection
        // reference (lookup is a single linear scan; cue lists are tiny).
        let sortedRanges = ranges.sorted { $0.start < $1.start }
        for (i, range) in sortedRanges.enumerated() {
            let underlyingEnd = nearestAdWindowEnd(forCueStart: range.start, cueEnd: range.end)
            let nextDistance: Double = i + 1 < sortedRanges.count
                ? sortedRanges[i + 1].start - range.end
                : -1.0
            logger.info(
                "pushMergedCues: cueStart=\(range.start, privacy: .public) cueEnd=\(range.end, privacy: .public) adWindowEnd=\(underlyingEnd, privacy: .public) distanceToNextCue=\(nextDistance, privacy: .public)"
            )
        }

        // Defensive: clamp to non-negative so a future misconfigured caller
        // can't invert the cushion (skip-end before ad-end).
        let cushion = max(0.0, config.adTrailingCushionSeconds)
        let cues = ranges.map { range -> CMTimeRange in
            let cushionedEnd = max(range.start, range.end - cushion)
            let start = CMTime(seconds: range.start, preferredTimescale: 600)
            let duration = CMTime(seconds: cushionedEnd - range.start, preferredTimescale: 600)
            return CMTimeRange(start: start, duration: duration)
        }
        pushSkipCues(cues)
    }

    /// Look up the underlying detection-side AdWindow.endTime for a merged
    /// cue range. Picks the managed window whose snapped range overlaps the
    /// cue and whose snappedEnd is closest to the cue end. Returns -1.0
    /// when no overlap is found (logged as a sentinel rather than NaN to
    /// keep the field log line shape stable).
    ///
    /// playhead-rfu-sad: candidates are sorted by `(gap, adWindow.id)` so
    /// ties (gap == 0, two windows ending at exactly the same time) pick
    /// a deterministic winner instead of whichever order
    /// `windows.values` happens to yield. Diagnostic-only output, but
    /// nondeterministic logging makes flaky test diagnostics harder.
    private func nearestAdWindowEnd(forCueStart cueStart: Double, cueEnd: Double) -> Double {
        let candidates = windows.values
            .filter { $0.snappedStart < cueEnd && $0.snappedEnd > cueStart }
            .map { (gap: abs($0.snappedEnd - cueEnd), id: $0.adWindow.id, end: $0.adWindow.endTime) }
            .sorted { lhs, rhs in
                if lhs.gap != rhs.gap { return lhs.gap < rhs.gap }
                return lhs.id < rhs.id
            }
        return candidates.first?.end ?? -1.0
    }

    /// Push skip cues to PlaybackService via the handler. Defaults to empty.
    private func pushSkipCues(_ cues: [CMTimeRange] = []) {
        skipCueHandler?(cues)
    }

    // MARK: - User Correction Injection

    /// Inject a user-marked ad segment immediately into the skip orchestrator.
    /// Creates a ManagedWindow with confidence=1.0 and .confirmed state, then
    /// evaluates and pushes skip cues so the segment takes effect in real time.
    ///
    /// Called from PlayheadRuntime when the user taps "Hearing an ad" or marks
    /// transcript chunks as an ad.
    func injectUserMarkedAd(start: Double, end: Double, analysisAssetId: String) {
        // Synthesize an AdWindow for the user-marked region.
        let windowId = UUID().uuidString
        let adWindow = AdWindow(
            id: windowId,
            analysisAssetId: analysisAssetId,
            startTime: start,
            endTime: end,
            confidence: 1.0,
            boundaryState: "userMarked",
            decisionState: AdDecisionState.confirmed.rawValue,
            detectorVersion: "userCorrection",
            advertiser: nil, product: nil, adDescription: nil,
            evidenceText: nil, evidenceStartTime: start,
            metadataSource: "userCorrection",
            metadataConfidence: nil, metadataPromptVersion: nil,
            wasSkipped: false, userDismissedBanner: false
        )

        let key = idempotencyKey(assetId: analysisAssetId, windowId: windowId)

        let managed = ManagedWindow(
            adWindow: adWindow,
            decisionState: .confirmed,
            snappedStart: start,
            snappedEnd: end,
            idempotencyKey: key,
            cueActive: false
        )
        windows[windowId] = managed

        // Emit banner before evaluateAndPush so listeners see the banner
        // even in shadow/manual mode where evaluateAndPush may not promote
        // to .applied.
        if !banneredWindowIds.contains(windowId) {
            // Cycle-27 T-3 production-writer site (4 of 4): injectUserMarkedAd manual entry point.
            banneredWindowIds.insert(windowId)
            emitBannerItem(for: managed)
        }

        evaluateAndPush()
    }

    // MARK: - Idempotency

    /// Build the idempotency key for a skip decision.
    private func idempotencyKey(assetId: String, windowId: String) -> String {
        "\(assetId):\(windowId):\(config.policyVersion)"
    }

    // MARK: - Decision Logging

    private func logDecision(
        managed: ManagedWindow,
        decision: SkipDecisionState,
        reason: String
    ) {
        let record = SkipDecisionRecord(
            idempotencyKey: managed.idempotencyKey,
            adWindowId: managed.adWindow.id,
            analysisAssetId: managed.adWindow.analysisAssetId,
            policyVersion: config.policyVersion,
            decision: decision,
            reason: reason,
            originalStart: managed.adWindow.startTime,
            originalEnd: managed.adWindow.endTime,
            snappedStart: managed.snappedStart,
            snappedEnd: managed.snappedEnd,
            confidence: managed.adWindow.confidence,
            timestamp: Date().timeIntervalSince1970
        )
        decisionLog.append(record)
        if decisionLog.count > decisionLogCapacity {
            decisionLog.removeFirst(decisionLog.count - decisionLogCapacity)
        }

        logger.info("Decision: \(decision.rawValue) window=\(managed.adWindow.id) [\(managed.snappedStart, format: .fixed(precision: 1))s-\(managed.snappedEnd, format: .fixed(precision: 1))s] reason=\(reason)")
    }
}
