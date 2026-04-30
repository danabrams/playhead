// UserPreferences.swift
// User preference model persisted with SwiftData.

import Foundation
import SwiftData

// MARK: - UserPreferences

@Model
final class UserPreferences {
    var skipBehavior: SkipBehavior
    var playbackSpeed: Double
    var skipIntervals: SkipIntervals
    var backgroundProcessingEnabled: Bool
    /// Whether background downloads (especially the maintenance lane,
    /// playhead-24cm) may use cellular data. Mirrored into a
    /// `UserPreferencesSnapshot` UserDefaults slot so the download
    /// manager can consult it from contexts where SwiftData isn't
    /// available (e.g. URLSession configuration at app boot).
    var allowsCellular: Bool

    /// playhead-zp0x: whether the single notification-permission ask
    /// (gated to non-Generic Download-Next-N submits) has been
    /// performed. Once true — regardless of whether the user accepted
    /// or denied — we never re-ask. Defaults to `false` so first-
    /// launch users see the ask exactly once when they first pick a
    /// trip context other than Generic.
    var notificationPermissionAsked: Bool = false

    /// playhead-jzik: whether on-device episode summaries (and the
    /// expandable subtitle they back) are enabled. Default ON because
    /// the FM cost is negligible (one extraction per asset, gated on
    /// transcript coverage) and the surface enriches the backlog
    /// browse without any user-facing "AI" framing. Setting this to
    /// `false` halts the backfill coordinator and leaves the
    /// `episode_summaries` table alone — re-enabling resumes
    /// generation against any rows still missing or stale.
    var episodeSummariesEnabled: Bool = true

    init(
        skipBehavior: SkipBehavior = .auto,
        playbackSpeed: Double = 1.0,
        skipIntervals: SkipIntervals = .init(),
        backgroundProcessingEnabled: Bool = true,
        allowsCellular: Bool = true,
        notificationPermissionAsked: Bool = false,
        episodeSummariesEnabled: Bool = true
    ) {
        self.skipBehavior = skipBehavior
        self.playbackSpeed = playbackSpeed
        self.skipIntervals = skipIntervals
        self.backgroundProcessingEnabled = backgroundProcessingEnabled
        self.allowsCellular = allowsCellular
        self.notificationPermissionAsked = notificationPermissionAsked
        self.episodeSummariesEnabled = episodeSummariesEnabled
    }
}

// MARK: - SkipBehavior

enum SkipBehavior: Int, Codable, Sendable, CaseIterable {
    case auto
    case manual
    case off
}

// MARK: - SkipIntervals

/// Configuration for how far forward/backward the skip buttons jump.
struct SkipIntervals: Codable, Sendable, Equatable {
    var forwardSeconds: TimeInterval
    var backwardSeconds: TimeInterval

    init(forwardSeconds: TimeInterval = 30, backwardSeconds: TimeInterval = 15) {
        self.forwardSeconds = forwardSeconds
        self.backwardSeconds = backwardSeconds
    }
}
