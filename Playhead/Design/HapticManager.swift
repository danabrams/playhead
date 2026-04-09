// HapticManager.swift
// Shared haptic feedback generators. Reuses static instances to avoid the
// overhead of creating a new generator on every tap. Each method calls
// prepare() before firing to ensure the Taptic Engine is warm.

import SwiftUI
import UIKit

// MARK: - HapticManager

@MainActor
enum HapticManager {

    private static let lightGenerator = UIImpactFeedbackGenerator(style: .light)
    private static let mediumGenerator = UIImpactFeedbackGenerator(style: .medium)
    private static let softGenerator = UIImpactFeedbackGenerator(style: .soft)
    private static let notificationGenerator = UINotificationFeedbackGenerator()

    static func light() {
        lightGenerator.prepare()
        lightGenerator.impactOccurred()
    }

    static func medium() {
        mediumGenerator.prepare()
        mediumGenerator.impactOccurred()
    }

    static func soft() {
        softGenerator.prepare()
        softGenerator.impactOccurred()
    }

    static func notification(_ type: UINotificationFeedbackGenerator.FeedbackType) {
        notificationGenerator.prepare()
        notificationGenerator.notificationOccurred(type)
    }
}

// MARK: - Haptic Events (bead-spec semantic layer)

/// Semantic haptic events. Each maps to a single underlying feedback kind.
enum HapticEvent: Equatable, CaseIterable {
    /// Ad skip confirmation — medium impact.
    case skip
    /// Transport / UI control tap — light impact.
    case control
    /// Save / success confirmation — success notification.
    case save
    /// Menu/picker open — medium impact.
    case menuOpen
    /// Soft cushioned tap — banner appearance, gentle notice.
    case notice

    /// Describes the underlying UIKit generator call used by this event.
    /// Exposed so tests can assert the mapping without firing real haptics.
    enum Mapping: Equatable {
        case impact(UIImpactFeedbackGenerator.FeedbackStyle)
        case notification(UINotificationFeedbackGenerator.FeedbackType)
    }

    var mapping: Mapping {
        switch self {
        case .skip:     return .impact(.medium)
        case .control:  return .impact(.light)
        case .save:     return .notification(.success)
        case .menuOpen: return .impact(.medium)
        case .notice:   return .impact(.soft)
        }
    }
}

// MARK: - HapticPlaying Protocol

/// Seam that lets tests substitute a recording fake for the real hardware.
///
/// The protocol itself is `Sendable` and not actor-isolated so values can
/// live in nonisolated contexts (e.g. SwiftUI `EnvironmentKey.defaultValue`,
/// which is required to be nonisolated). The single requirement is
/// `@MainActor` because real haptics must fire on the main thread.
protocol HapticPlaying: Sendable {
    @MainActor func play(_ event: HapticEvent)
}

/// Production implementation that routes through `HapticManager`.
struct SystemHapticPlayer: HapticPlaying {
    @MainActor func play(_ event: HapticEvent) {
        switch event.mapping {
        case .impact(let style):
            switch style {
            case .light:
                HapticManager.light()
            case .medium:
                HapticManager.medium()
            case .soft:
                HapticManager.soft()
            case .heavy:
                HapticManager.medium()
            case .rigid:
                HapticManager.medium()
            @unknown default:
                assertionFailure("Unhandled impact style \(style)")
                HapticManager.medium()
            }
        case .notification(let type):
            HapticManager.notification(type)
        }
    }
}

// NOTE: all view-layer haptics now route through the injected
// `HapticPlaying` seam (`NowPlayingBar`, `SpeedSelectorView`,
// `TimelineRailView`, `NowPlayingView.TransportButton`,
// `EpisodeListView`, `AdBannerView`). The only remaining references to
// `HapticManager.light()/medium()/soft()/notification()` should be the
// definitions above and `SystemHapticPlayer.play(_:)`.
