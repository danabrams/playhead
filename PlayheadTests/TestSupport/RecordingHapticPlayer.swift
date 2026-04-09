// RecordingHapticPlayer.swift
// Shared test double that captures HapticEvent plays for assertions.
// Extracted from DesignTokenHapticTests so multiple haptic test files
// can depend on TestSupport rather than another test file.

import UIKit
@testable import Playhead

@MainActor
final class RecordingHapticPlayer: HapticPlaying {
    var played: [HapticEvent] = []
    func play(_ event: HapticEvent) { played.append(event) }
}
