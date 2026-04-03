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

    init(
        skipBehavior: SkipBehavior = .auto,
        playbackSpeed: Double = 1.0,
        skipIntervals: SkipIntervals = .init(),
        backgroundProcessingEnabled: Bool = true
    ) {
        self.skipBehavior = skipBehavior
        self.playbackSpeed = playbackSpeed
        self.skipIntervals = skipIntervals
        self.backgroundProcessingEnabled = backgroundProcessingEnabled
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
