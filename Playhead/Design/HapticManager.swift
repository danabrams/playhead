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
