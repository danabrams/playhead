// SkipPolicyMatrix.swift
// Phase 6 (playhead-4my.6.2): Policy matrix, decision cohort, and stability policy.
// ef2.6.2: Skip-policy override integration — per-show/per-type policy overrides
//          that gate eligibility without changing confidence scores.

import Foundation

// MARK: - SkipPolicyAction

enum SkipPolicyAction: String, Sendable, Codable, Hashable, CaseIterable {
    case autoSkipEligible // fusion can promote to skip
    case detectOnly       // show banner, never skip
    case suppress         // don't even show
    case logOnly          // record for telemetry only
}

// MARK: - SkipPolicyMatrix

/// Maps (CommercialIntent, AdOwnership) → SkipPolicyAction.
///
/// Phase 6.5 (playhead-4my.16): (.unknown, .unknown) → .detectOnly (banner, no auto-skip)
/// so Phase 7 (UserCorrections) has banner impressions to correct against.
/// Unknown intent with a known (non-unknown) ownership still returns .logOnly —
/// insufficient signal to act. Phase 8 (SponsorKnowledgeStore) will populate
/// intent/ownership for full matrix evaluation. FM does NOT classify commercial
/// intent in Phases 6–7.
struct SkipPolicyMatrix: Sendable {

    static let defaultAction: SkipPolicyAction = .logOnly

    static func action(for intent: CommercialIntent, ownership: AdOwnership) -> SkipPolicyAction {
        // All 25 (intent × ownership) combinations are enumerated explicitly so the
        // compiler catches any future cases added to either enum.
        switch (intent, ownership) {

        // ── paid intent ────────────────────────────────────────────────────────
        case (.paid, .thirdParty):
            // Classic insertion ad — auto-skip eligible.
            return .autoSkipEligible
        case (.paid, .show), (.paid, .network):
            // Show/network-produced paid content is ambiguous; Phase 8 resolves it.
            return .logOnly
        case (.paid, .guest):
            // Paid guest endorsement: treat like a detect-only mention until Phase 8.
            return .detectOnly
        case (.paid, .unknown):
            return .logOnly

        // ── owned intent ───────────────────────────────────────────────────────
        case (.owned, .show), (.owned, .network):
            // Show-owned promos: surface via banner, never skip automatically.
            return .detectOnly
        case (.owned, .thirdParty), (.owned, .guest), (.owned, .unknown):
            // Conflicting signals — insufficient data.
            return .logOnly

        // ── affiliate intent ───────────────────────────────────────────────────
        case (.affiliate, _):
            // Affiliate reads: always show banner regardless of ownership.
            return .detectOnly

        // ── organic / unknown intent ───────────────────────────────────────────
        case (.organic, _):
            // Organic content: suppress all cues.
            return .suppress
        case (.unknown, .unknown):
            // Phase 6.5 (playhead-4my.16): unknown-intent + unknown-ownership surfaces a
            // banner so Phase 7 (UserCorrections) has signal to learn from. Phase 8
            // (SponsorKnowledgeStore) populates intent/ownership for full matrix evaluation.
            return .detectOnly
        case (.unknown, _):
            return .logOnly
        }
    }

    // MARK: Override-aware action (ef2.6.2)

    /// Returns the effective policy action, consulting the override store first.
    ///
    /// Precedence (highest wins):
    ///   1. showLevel override (podcast + intent + ownership match)
    ///   2. showWide override (podcast match, any intent/ownership)
    ///   3. adType override (intent + ownership match, any podcast)
    ///   4. Default matrix (static mapping)
    ///
    /// Skip-policy overrides affect Stage 4 (eligibility gating) only.
    /// They do NOT modify confidence scores (Stage 2). This is the key
    /// distinction: "that was a house promo" (classification override) is
    /// different from "never skip house promos" (skip-policy override).
    static func action(
        for intent: CommercialIntent,
        ownership: AdOwnership,
        overrideStore: SkipPolicyOverrideStore?,
        podcastId: String?
    ) -> SkipPolicyAction {
        if let store = overrideStore,
           let overriddenAction = store.effectiveAction(
               for: intent, ownership: ownership, podcastId: podcastId
           ) {
            return overriddenAction
        }
        return action(for: intent, ownership: ownership)
    }
}

// MARK: - SkipPolicyOverrideScope (ef2.6.2)

/// Defines the scope at which a skip-policy override applies.
///
/// Three levels of specificity:
///   - `adType`: global — applies to all (intent, ownership) pairs matching the type.
///   - `showLevel`: per-podcast + per-type — overrides the policy for a specific
///     ad type on a specific show.
///   - `showWide`: per-podcast — overrides all ad types for a specific show.
///
/// Precedence: showLevel > showWide > adType (most specific wins).
enum SkipPolicyOverrideScope: Sendable, Equatable, Codable, Hashable {
    /// Override for all ads matching this (intent, ownership) pair, regardless of show.
    case adType(intent: CommercialIntent, ownership: AdOwnership)
    /// Override for a specific ad type on a specific show.
    case showLevel(podcastId: String, intent: CommercialIntent, ownership: AdOwnership)
    /// Override for all ad types on a specific show.
    case showWide(podcastId: String)

    // MARK: Serialization

    var serialized: String {
        switch self {
        case .adType(let intent, let ownership):
            return "adType:\(intent.rawValue):\(ownership.rawValue)"
        case .showLevel(let podcastId, let intent, let ownership):
            return "showLevel:\(podcastId):\(intent.rawValue):\(ownership.rawValue)"
        case .showWide(let podcastId):
            return "showWide:\(podcastId)"
        }
    }

    static func deserialize(_ string: String) -> SkipPolicyOverrideScope? {
        guard let typeEnd = string.firstIndex(of: ":") else { return nil }
        let typeStr = String(string[string.startIndex..<typeEnd])
        let remainder = String(string[string.index(after: typeEnd)...])

        switch typeStr {
        case "adType":
            let parts = remainder.split(separator: ":", maxSplits: 1).map(String.init)
            guard parts.count == 2,
                  let intent = CommercialIntent(rawValue: parts[0]),
                  let ownership = AdOwnership(rawValue: parts[1]) else { return nil }
            return .adType(intent: intent, ownership: ownership)
        case "showLevel":
            // remainder = "podcastId:intent:ownership"
            // podcastId may contain colons; intent and ownership are the last two parts.
            let parts = remainder.split(separator: ":", maxSplits: Int.max, omittingEmptySubsequences: false)
                .map(String.init)
            guard parts.count >= 3,
                  let intent = CommercialIntent(rawValue: parts[parts.count - 2]),
                  let ownership = AdOwnership(rawValue: parts[parts.count - 1]) else { return nil }
            let podcastId = parts[0..<(parts.count - 2)].joined(separator: ":")
            return .showLevel(podcastId: podcastId, intent: intent, ownership: ownership)
        case "showWide":
            guard !remainder.isEmpty else { return nil }
            return .showWide(podcastId: remainder)
        default:
            return nil
        }
    }
}

// MARK: - SkipPolicyOverride (ef2.6.2)

/// A single skip-policy override record.
///
/// Represents a user or system preference: "for this scope, use this action instead
/// of the default matrix result." Does NOT affect confidence scores — only
/// the Stage 4 eligibility gating.
struct SkipPolicyOverride: Sendable, Codable, Equatable {
    let scope: SkipPolicyOverrideScope
    let action: SkipPolicyAction
    /// Human-readable reason for audit and UI display.
    let reason: String
}

// MARK: - SkipPolicyOverrideStore protocol (ef2.6.2)

/// Protocol for querying skip-policy overrides.
///
/// Implementations store per-show or per-ad-type overrides and resolve them
/// with correct precedence: showLevel > showWide > adType.
protocol SkipPolicyOverrideStore: Sendable {
    /// Return the effective override action for the given intent/ownership/podcast,
    /// or nil if no override applies.
    ///
    /// Precedence: showLevel > showWide > adType.
    func effectiveAction(
        for intent: CommercialIntent,
        ownership: AdOwnership,
        podcastId: String?
    ) -> SkipPolicyAction?

    /// All stored overrides for diagnostics and UI.
    var allOverrides: [SkipPolicyOverride] { get }
}

// MARK: - InMemorySkipPolicyOverrideStore (ef2.6.2)

/// Thread-safe in-memory implementation for tests and early integration.
final class InMemorySkipPolicyOverrideStore: SkipPolicyOverrideStore, @unchecked Sendable {
    private let lock = NSLock()
    private var overrides: [SkipPolicyOverrideScope: SkipPolicyOverride] = [:]

    func addOverride(_ override: SkipPolicyOverride) {
        lock.lock()
        defer { lock.unlock() }
        overrides[override.scope] = override
    }

    func removeOverride(for scope: SkipPolicyOverrideScope) {
        lock.lock()
        defer { lock.unlock() }
        overrides.removeValue(forKey: scope)
    }

    func effectiveAction(
        for intent: CommercialIntent,
        ownership: AdOwnership,
        podcastId: String?
    ) -> SkipPolicyAction? {
        lock.lock()
        defer { lock.unlock() }

        // Precedence: showLevel > showWide > adType (most specific wins)
        if let podcastId {
            let showLevelScope = SkipPolicyOverrideScope.showLevel(
                podcastId: podcastId, intent: intent, ownership: ownership
            )
            if let showLevel = overrides[showLevelScope] {
                return showLevel.action
            }
            let showWideScope = SkipPolicyOverrideScope.showWide(podcastId: podcastId)
            if let showWide = overrides[showWideScope] {
                return showWide.action
            }
        }
        let adTypeScope = SkipPolicyOverrideScope.adType(intent: intent, ownership: ownership)
        if let adType = overrides[adTypeScope] {
            return adType.action
        }
        return nil
    }

    var allOverrides: [SkipPolicyOverride] {
        lock.lock()
        defer { lock.unlock() }
        return Array(overrides.values)
    }
}

// MARK: - DecisionCohort

/// Identifies the exact pipeline configuration used to produce a decision.
///
/// Changes to any hash field trigger decision recomputation from cached scan results
/// WITHOUT triggering FM rescans (scan results are keyed separately by ScanCohort).
struct DecisionCohort: Sendable, Codable, Hashable {
    let featurePipelineHash: String
    let fusionHash: String
    let policyHash: String
    let stabilityHash: String
    let appBuild: String
    /// Calibration profile version used for this decision (e.g. "v0", "v1").
    let calibrationVersion: String

    // IMPORTANT: bump each hash string manually whenever the corresponding
    // pipeline component changes. The date suffix is a documentation aid, not
    // a machine-readable field — there is no automated enforcement.
    static func production(appBuild: String, calibrationVersion: String = "v0") -> DecisionCohort {
        precondition(!appBuild.isEmpty, "appBuild must be non-empty — pass the real build number")
        return DecisionCohort(
            featurePipelineHash: "feature-v1-2026-04-10",
            fusionHash: "fusion-v1-2026-04-10",
            policyHash: "policy-v1-2026-04-10",
            stabilityHash: "stability-v1-2026-04-10",
            appBuild: appBuild,
            calibrationVersion: calibrationVersion
        )
    }
}

// MARK: - DecisionStabilityPolicy

/// Governs when a previously-applied skip cue can be removed.
///
/// Removal requires BOTH:
///   (a) score drops below stayThreshold  AND
///   (b) at least one of: user correction, FM negative (strong certainty + good quality),
///       or transcript content materially changed.
///
/// Cohort-only changes (same transcript, new OS model) cannot remove cues unless
/// the score drops below suppressionThreshold. User correction always overrides.
struct DecisionStabilityPolicy: Sendable {
    let stayThreshold: Double
    let suppressionThreshold: Double

    init(stayThreshold: Double = 0.45, suppressionThreshold: Double = 0.25) {
        precondition(suppressionThreshold < stayThreshold,
            "suppressionThreshold (\(suppressionThreshold)) must be strictly less than stayThreshold (\(stayThreshold))")
        self.stayThreshold = stayThreshold
        self.suppressionThreshold = suppressionThreshold
    }

    static let `default` = DecisionStabilityPolicy()

    func canRemoveCue(
        currentScore: Double,
        userCorrected: Bool,
        fmNegativeWithStrongCertainty: Bool,
        transcriptVersionChanged: Bool,
        cohortOnlyChange: Bool
    ) -> Bool {
        if userCorrected { return true }
        if cohortOnlyChange { return currentScore < suppressionThreshold }
        let scoreBelowThreshold = currentScore < stayThreshold
        let hasCounterEvidence = fmNegativeWithStrongCertainty || transcriptVersionChanged
        return scoreBelowThreshold && hasCounterEvidence
    }
}
