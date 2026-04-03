// Typography.swift
// App-wide type scale and font definitions.
//
// Font strategy:
//   UI sans:       Instrument Sans > Inter Tight > Geist > system default (.systemFont)
//   Editorial:     Newsreader > Source Serif 4 > system serif (.design(.serif))
//   Mono:          IBM Plex Mono > Geist Mono > system mono (.design(.monospaced))
//
// Custom fonts are resolved at runtime; if none are bundled, the system
// fallback is used automatically. This keeps the binary small at launch and
// lets us add custom fonts later without code changes.

import SwiftUI

// MARK: - Font Family Resolution

/// Resolves preferred font families with automatic fallback.
private enum FontFamily {

    /// First available family name from `candidates`, or nil for system default.
    static func resolve(_ candidates: [String]) -> String? {
        let available = Set(UIFont.familyNames)
        return candidates.first { available.contains($0) }
    }

    /// UI sans-serif candidates.
    static let sansFamily = resolve([
        "Instrument Sans",
        "Inter Tight",
        "Geist"
    ])

    /// Editorial serif candidates.
    static let serifFamily = resolve([
        "Newsreader",
        "Source Serif 4"
    ])

    /// Monospace candidates.
    static let monoFamily = resolve([
        "IBM Plex Mono",
        "Geist Mono"
    ])
}

// MARK: - AppTypography

/// Semantic type roles for the app.
///
/// Usage: `AppTypography.title`, `AppTypography.body`, etc.
/// Each role returns a `Font` that resolves to the best available family.
enum AppTypography {

    // MARK: Semantic Roles

    /// Large titles, screen headers.
    /// Sans-serif, 28pt, semibold.
    static let title: Font = sans(size: 28, weight: .semibold)

    /// Body text, primary readable content.
    /// Sans-serif, 16pt, regular.
    static let body: Font = sans(size: 16, weight: .regular)

    /// Small labels, supporting text.
    /// Sans-serif, 13pt, regular.
    static let caption: Font = sans(size: 13, weight: .regular)

    /// Timestamps, durations, numeric metadata.
    /// Monospace, 13pt, medium — tabular figures for alignment.
    static let timestamp: Font = mono(size: 13, weight: .medium)

    /// Transcript text display.
    /// Serif, 17pt, regular — designed for comfortable reading.
    static let transcript: Font = serif(size: 17, weight: .regular)

    // MARK: Factories

    /// Sans-serif font with automatic fallback to system default.
    static func sans(size: CGFloat, weight: Font.Weight) -> Font {
        if let family = FontFamily.sansFamily {
            return .custom(family, size: size).weight(weight)
        }
        return .system(size: size, weight: weight, design: .default)
    }

    /// Serif font with automatic fallback to system serif.
    static func serif(size: CGFloat, weight: Font.Weight) -> Font {
        if let family = FontFamily.serifFamily {
            return .custom(family, size: size).weight(weight)
        }
        return .system(size: size, weight: weight, design: .serif)
    }

    /// Monospace font with automatic fallback to system mono.
    static func mono(size: CGFloat, weight: Font.Weight) -> Font {
        if let family = FontFamily.monoFamily {
            return .custom(family, size: size).weight(weight)
        }
        return .system(size: size, weight: weight, design: .monospaced)
    }
}

// MARK: - Preview

#Preview("Typography Scale") {
    ScrollView {
        VStack(alignment: .leading, spacing: 24) {
            Text("Title — Screen Headers")
                .font(AppTypography.title)

            Text("Body — Primary readable content across the app. This is the default text style for most UI elements and should feel comfortable at any length.")
                .font(AppTypography.body)

            Text("Caption — Supporting labels and small text")
                .font(AppTypography.caption)

            Text("01:23:45")
                .font(AppTypography.timestamp)

            Text("Transcript — And so the host continued, describing the product in detail, mentioning the sponsor by name several times throughout the segment.")
                .font(AppTypography.transcript)
        }
        .padding()
    }
}
