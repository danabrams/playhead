// Typography.swift
// App-wide type scale and font definitions.
//
// Font strategy:
//   UI sans:       Instrument Sans > Inter Tight > Geist > system (.systemFont)
//   Editorial:     Newsreader > Source Serif 4 > system serif (.design(.serif))
//   Mono:          IBM Plex Mono > Geist Mono > system mono (.design(.monospaced))
//
// Custom fonts are resolved at runtime; if none are bundled the system
// fallback is used automatically. Every semantic role scales with Dynamic
// Type via `.custom(_, size:, relativeTo:)`.

import SwiftUI
import UIKit

// MARK: - Font Family Classes

enum FontFamilyClass: Equatable {
    case sans
    case serif
    case mono
}

// MARK: - Family Resolver (testable)

/// Pure, injectable resolver. `Typography.swift` is the only caller that
/// hits `UIFont.familyNames`; tests pass in their own `availableFamilies`.
enum FontFamilyResolver {
    static func resolve(candidates: [String], availableFamilies: Set<String>) -> String? {
        candidates.first { availableFamilies.contains($0) }
    }

    static var systemAvailableFamilies: Set<String> {
        Set(UIFont.familyNames)
    }
}

private enum FontFamily {
    static let sansFamily: String? = FontFamilyResolver.resolve(
        candidates: ["Instrument Sans", "Inter Tight", "Geist"],
        availableFamilies: FontFamilyResolver.systemAvailableFamilies
    )
    static let serifFamily: String? = FontFamilyResolver.resolve(
        candidates: ["Newsreader", "Source Serif 4"],
        availableFamilies: FontFamilyResolver.systemAvailableFamilies
    )
    static let monoFamily: String? = FontFamilyResolver.resolve(
        candidates: ["IBM Plex Mono", "Geist Mono"],
        availableFamilies: FontFamilyResolver.systemAvailableFamilies
    )
}

// MARK: - Typography Roles & Descriptors

/// Every semantic role in the Quiet Instrument type scale.
enum TypographyRole: String, CaseIterable {
    case title
    case headline
    case body
    case caption
    case timestamp
    case transcript
    case transcriptCaption
}

/// Testable description of a semantic type role.
struct TypographyDescriptor: Equatable {
    let role: TypographyRole
    let family: FontFamilyClass
    let baseSize: CGFloat
    let weight: Font.Weight
    let textStyle: Font.TextStyle
}

// MARK: - AppTypography

enum AppTypography {

    // MARK: Semantic roles (SwiftUI Font)

    static let title: Font             = font(for: .title)
    static let headline: Font          = font(for: .headline)
    static let body: Font              = font(for: .body)
    static let caption: Font           = font(for: .caption)
    static let timestamp: Font         = font(for: .timestamp)
    static let transcript: Font        = font(for: .transcript)
    static let transcriptCaption: Font = font(for: .transcriptCaption)

    // MARK: Descriptors

    static func descriptor(for role: TypographyRole) -> TypographyDescriptor {
        switch role {
        case .title:
            return .init(role: .title, family: .sans, baseSize: 28, weight: .semibold, textStyle: .title)
        case .headline:
            return .init(role: .headline, family: .sans, baseSize: 20, weight: .semibold, textStyle: .title3)
        case .body:
            return .init(role: .body, family: .sans, baseSize: 17, weight: .regular, textStyle: .body)
        case .caption:
            return .init(role: .caption, family: .sans, baseSize: 13, weight: .regular, textStyle: .caption)
        case .timestamp:
            return .init(role: .timestamp, family: .mono, baseSize: 13, weight: .medium, textStyle: .caption)
        case .transcript:
            return .init(role: .transcript, family: .serif, baseSize: 17, weight: .regular, textStyle: .body)
        case .transcriptCaption:
            return .init(role: .transcriptCaption, family: .serif, baseSize: 13, weight: .regular, textStyle: .caption)
        }
    }

    static func font(for role: TypographyRole) -> Font {
        let d = descriptor(for: role)
        return makeFont(family: d.family, size: d.baseSize, weight: d.weight, relativeTo: d.textStyle)
    }

    // MARK: Factories

    /// Sans-serif font with automatic fallback to system default.
    /// Kept as free function so existing call sites (e.g. OnboardingView which
    /// uses `AppTypography.sans(size: 36, weight: .semibold)`) still compile.
    static func sans(size: CGFloat, weight: Font.Weight) -> Font {
        if let family = FontFamily.sansFamily {
            return .custom(family, size: size).weight(weight)
        }
        return .system(size: size, weight: weight, design: .default)
    }

    static func serif(size: CGFloat, weight: Font.Weight) -> Font {
        if let family = FontFamily.serifFamily {
            return .custom(family, size: size).weight(weight)
        }
        return .system(size: size, weight: weight, design: .serif)
    }

    static func mono(size: CGFloat, weight: Font.Weight) -> Font {
        if let family = FontFamily.monoFamily {
            return .custom(family, size: size).weight(weight)
        }
        return .system(size: size, weight: weight, design: .monospaced)
    }

    // MARK: Private

    private static func makeFont(family: FontFamilyClass, size: CGFloat, weight: Font.Weight,
                                 relativeTo textStyle: Font.TextStyle) -> Font {
        let resolvedFamily: String?
        let systemDesign: Font.Design
        switch family {
        case .sans:
            resolvedFamily = FontFamily.sansFamily
            systemDesign = .default
        case .serif:
            resolvedFamily = FontFamily.serifFamily
            systemDesign = .serif
        case .mono:
            resolvedFamily = FontFamily.monoFamily
            systemDesign = .monospaced
        }
        if let family = resolvedFamily {
            // Dynamic Type scales relative to `textStyle`.
            return .custom(family, size: size, relativeTo: textStyle).weight(weight)
        }
        return .system(textStyle, design: systemDesign, weight: weight)
    }
}

// MARK: - Preview

#Preview("Typography Scale") {
    ScrollView {
        VStack(alignment: .leading, spacing: 24) {
            Text("Title — Screen Headers").font(AppTypography.title)
            Text("Headline — Section titles").font(AppTypography.headline)
            Text("Body — Primary readable content across the app.")
                .font(AppTypography.body)
            Text("Caption — Supporting labels and small text")
                .font(AppTypography.caption)
            Text("01:23:45").font(AppTypography.timestamp)
            Text("Transcript — And so the host continued, describing the product in detail.")
                .font(AppTypography.transcript)
            Text("Transcript caption — speaker attribution")
                .font(AppTypography.transcriptCaption)
        }
        .padding()
    }
}
