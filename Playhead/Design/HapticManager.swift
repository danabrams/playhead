// HapticManager.swift
// Shared haptic feedback generators. Reuses static instances to avoid the
// overhead of creating a new generator on every tap. Each method calls
// prepare() before firing to ensure the Taptic Engine is warm.

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
enum HapticEvent: Equatable {
    /// Ad skip confirmation — medium impact.
    case skip
    /// Transport / UI control tap — light impact.
    case control
    /// Save / success confirmation — success notification.
    case save

    /// Describes the underlying UIKit generator call used by this event.
    /// Exposed so tests can assert the mapping without firing real haptics.
    enum Mapping: Equatable {
        case impact(UIImpactFeedbackGenerator.FeedbackStyle)
        case notification(UINotificationFeedbackGenerator.FeedbackType)
    }

    var mapping: Mapping {
        switch self {
        case .skip:    return .impact(.medium)
        case .control: return .impact(.light)
        case .save:    return .notification(.success)
        }
    }
}

// MARK: - HapticPlaying Protocol

/// Seam that lets tests substitute a recording fake for the real hardware.
@MainActor
protocol HapticPlaying {
    func play(_ event: HapticEvent)
}

/// Production implementation that routes through `HapticManager`.
@MainActor
struct SystemHapticPlayer: HapticPlaying {
    func play(_ event: HapticEvent) {
        switch event.mapping {
        case .impact(.medium):
            HapticManager.medium()
        case .impact(.light):
            HapticManager.light()
        case .impact(.soft):
            HapticManager.soft()
        case .impact:
            HapticManager.medium()
        case .notification(let type):
            HapticManager.notification(type)
        }
    }
}
