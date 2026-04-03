// TimeFormatter.swift
// Shared time formatting used across Now Playing, Transcript Peek,
// Episode List, and any future time-displaying views.

import Foundation

// MARK: - TimeFormatter

enum TimeFormatter {

    /// Formats seconds as "H:MM:SS" or "M:SS".
    /// Returns "0:00" for non-finite or negative values.
    static func formatTime(_ seconds: TimeInterval) -> String {
        guard seconds.isFinite, seconds >= 0 else { return "0:00" }
        let totalSeconds = Int(seconds)
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        let secs = totalSeconds % 60
        if hours > 0 {
            return String(format: "%d:%02d:%02d", hours, minutes, secs)
        }
        return String(format: "%d:%02d", minutes, secs)
    }

    /// Formats a duration as "Xh Ym" or "X min".
    /// Returns empty string for non-finite or negative values.
    static func formatDuration(_ seconds: TimeInterval) -> String {
        guard seconds.isFinite, seconds >= 0 else { return "" }
        let totalSeconds = Int(seconds)
        let hours = totalSeconds / 3600
        let minutes = (totalSeconds % 3600) / 60
        if hours > 0 {
            return "\(hours)h \(minutes)m"
        }
        return "\(minutes) min"
    }
}
