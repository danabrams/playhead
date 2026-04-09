// Theme.swift
// Central design theme: spacing, corner radii, shadows, and animation curves.
//
// "Quiet Instrument" motion philosophy:
// - Precise, not bouncy — no spring physics, no parallax, no shimmer
// - Animations are quick and intentional, like a well-damped needle
// - Cards use long horizontal proportions (16:9 or wider)
// - Transitions feel mechanical and exact, never playful

import SwiftUI

// MARK: - Spacing

/// 4-point spatial scale. All layout dimensions derive from these values.
enum Spacing {
    /// 4pt — tightest spacing (icon padding, inline gaps)
    static let xxs: CGFloat = 4
    /// 8pt — compact spacing (between related elements)
    static let xs: CGFloat = 8
    /// 12pt — default inner padding
    static let sm: CGFloat = 12
    /// 16pt — standard content padding
    static let md: CGFloat = 16
    /// 24pt — section separation
    static let lg: CGFloat = 24
    /// 32pt — major section breaks
    static let xl: CGFloat = 32
    /// 48pt — screen-level top/bottom padding
    static let xxl: CGFloat = 48
}

// MARK: - Corner Radii

/// Corner radius tokens. Restrained, never bubbly: small=4, medium=8, large=12.
/// The `sm/md/lg` aliases exist for backward-compatibility with call sites
/// predating the semantic names.
enum CornerRadius {
    /// 4pt — buttons, small chips
    static let small: CGFloat = 4
    /// 8pt — cards, input fields
    static let medium: CGFloat = 8
    /// 12pt — sheets, modals
    static let large: CGFloat = 12

    // MARK: Legacy aliases
    static let sm: CGFloat = small
    static let md: CGFloat = medium
    static let lg: CGFloat = large
    /// 16pt — extra-large; not part of the bead spec but retained for the
    /// preview catalog and any hero cards that need a larger radius.
    static let xl: CGFloat = 16
}

// MARK: - Shadows

/// Shadow definitions for elevation levels. Subtle and warm-neutral.
enum AppShadow {
    /// Slight lift for cards on a surface.
    static let card = ShadowStyle(
        color: Color.black.opacity(0.08),
        radius: 4,
        x: 0,
        y: 2
    )

    /// Elevated elements (popovers, floating action areas).
    static let elevated = ShadowStyle(
        color: Color.black.opacity(0.12),
        radius: 12,
        x: 0,
        y: 4
    )

    struct ShadowStyle: Sendable {
        let color: Color
        let radius: CGFloat
        let x: CGFloat
        let y: CGFloat
    }
}

// MARK: - Shadow View Modifier

extension View {
    /// Apply a theme shadow style.
    func themeShadow(_ style: AppShadow.ShadowStyle) -> some View {
        self.shadow(color: style.color, radius: style.radius, x: style.x, y: style.y)
    }
}

// MARK: - Animation Curves

/// Curve kind recorded in a `MotionDescriptor`. Tests assert against this
/// enum so they don't have to parse SwiftUI's opaque `Animation` internals.
enum MotionCurveKind: String, Equatable {
    case linear
    case easeIn
    case easeOut
    case easeInOut
    case timingCurve
    // The following are forbidden by the "Quiet Instrument" design rules and
    // are enumerated only so tests can assert their absence.
    case spring
    case interpolatingSpring
    case bouncy
}

/// Cubic Bezier control points for a custom timing curve.
struct MotionControlPoints: Equatable {
    let c1x: Double
    let c1y: Double
    let c2x: Double
    let c2y: Double
}

/// A testable description of a motion token. Wraps a SwiftUI `Animation`
/// while preserving the metadata needed to prove we're not using springs.
struct MotionDescriptor: Equatable {
    let name: String
    let kind: MotionCurveKind
    let duration: Double
    let controlPoints: MotionControlPoints?

    /// The underlying SwiftUI `Animation` ready to hand to `.animation(_:value:)`.
    var animation: Animation {
        switch kind {
        case .linear:
            return .linear(duration: duration)
        case .easeIn:
            return .easeIn(duration: duration)
        case .easeOut:
            return .easeOut(duration: duration)
        case .easeInOut:
            return .easeInOut(duration: duration)
        case .timingCurve:
            guard let c = controlPoints else { return .easeInOut(duration: duration) }
            return .timingCurve(c.c1x, c.c1y, c.c2x, c.c2y, duration: duration)
        case .spring, .interpolatingSpring, .bouncy:
            // These are declared forbidden; fall back to a safe ease-out if
            // anything ever tries to instantiate them via this wrapper.
            return .easeOut(duration: duration)
        }
    }
}

/// Animation presets. All eased, never springy.
/// "Like a well-damped needle" — quick, precise, no overshoot.
///
/// Spec:
/// - `quick`       = 0.15s
/// - `standard`    = 0.25s
/// - `deliberate`  = 0.4s
/// - `preciseEase` = custom cubic (0.2, 0.0, 0.0, 1.0) — fast settle, no overshoot
/// - `transport`   = linear (for scrubber / playhead tracking)
enum Motion {

    // MARK: Descriptors (testable)

    static let quickDescriptor = MotionDescriptor(
        name: "quick", kind: .easeInOut, duration: 0.15, controlPoints: nil
    )
    static let standardDescriptor = MotionDescriptor(
        name: "standard", kind: .easeInOut, duration: 0.25, controlPoints: nil
    )
    static let deliberateDescriptor = MotionDescriptor(
        name: "deliberate", kind: .easeInOut, duration: 0.4, controlPoints: nil
    )
    static let preciseEaseDescriptor = MotionDescriptor(
        name: "preciseEase",
        kind: .timingCurve,
        duration: 0.25,
        controlPoints: MotionControlPoints(c1x: 0.2, c1y: 0.0, c2x: 0.0, c2y: 1.0)
    )
    static let transportDescriptor = MotionDescriptor(
        name: "transport", kind: .linear, duration: 0.15, controlPoints: nil
    )

    // MARK: SwiftUI Animations (call sites)

    /// 0.15s ease-in-out — taps, small state flips.
    static let quick: Animation = quickDescriptor.animation
    /// 0.25s ease-in-out — default content transitions.
    static let standard: Animation = standardDescriptor.animation
    /// 0.4s ease-in-out — deliberate, noticeable transitions.
    static let deliberate: Animation = deliberateDescriptor.animation
    /// Custom fast-settle curve with zero overshoot (Material "decelerate").
    static let preciseEase: Animation = preciseEaseDescriptor.animation
    /// Linear curve for the transport / scrubber (no easing artifacts while scrubbing).
    static let transport: Animation = transportDescriptor.animation
}

// MARK: - Card Proportions

/// Aspect ratios for card-style layouts.
/// Cards in Playhead use long horizontal proportions.
enum CardProportion {
    /// 16:9 — standard wide card
    static let wide: CGFloat = 16.0 / 9.0
    /// 2:1 — extra-wide banner-style card
    static let banner: CGFloat = 2.0 / 1.0
    /// 3:1 — narrow strip (e.g. now-playing bar)
    static let strip: CGFloat = 3.0 / 1.0
}

// MARK: - Debug Placeholder Overlay

/// In DEBUG builds, overlays a conspicuous label on views that lack real content.
/// Use on any view that is a temporary stand-in for dynamic data (images, text, etc.)
/// so unwired placeholders are impossible to miss during development.
///
/// Usage:
///     RoundedRectangle(cornerRadius: CornerRadius.md)
///         .fill(AppColors.surface)
///         .debugPlaceholder("Artwork")
extension View {
    func debugPlaceholder(_ label: String) -> some View {
        #if DEBUG
        self.overlay(
            Text("⚠ \(label)")
                .font(.system(size: 10, weight: .bold, design: .monospaced))
                .foregroundStyle(.white)
                .padding(.horizontal, 4)
                .padding(.vertical, 2)
                .background(Color.red.opacity(0.85))
                .cornerRadius(3)
        )
        #else
        self
        #endif
    }
}

// MARK: - Preview

#Preview("Theme Tokens") {
    ScrollView {
        VStack(alignment: .leading, spacing: Spacing.lg) {
            Text("Spacing Scale")
                .font(AppTypography.title)

            HStack(spacing: Spacing.xs) {
                spacingBox("xxs", Spacing.xxs)
                spacingBox("xs", Spacing.xs)
                spacingBox("sm", Spacing.sm)
                spacingBox("md", Spacing.md)
                spacingBox("lg", Spacing.lg)
            }

            Text("Corner Radii")
                .font(AppTypography.title)

            HStack(spacing: Spacing.md) {
                radiusBox("sm", CornerRadius.sm)
                radiusBox("md", CornerRadius.md)
                radiusBox("lg", CornerRadius.lg)
                radiusBox("xl", CornerRadius.xl)
            }

            Text("Shadows")
                .font(AppTypography.title)

            HStack(spacing: Spacing.lg) {
                shadowBox("card", AppShadow.card)
                shadowBox("elevated", AppShadow.elevated)
            }

            Text("Card Proportions")
                .font(AppTypography.title)

            VStack(spacing: Spacing.sm) {
                proportionBox("wide (16:9)", CardProportion.wide)
                proportionBox("banner (2:1)", CardProportion.banner)
                proportionBox("strip (3:1)", CardProportion.strip)
            }
        }
        .padding(Spacing.md)
    }
    .background(AppColors.background)
}

@ViewBuilder
private func spacingBox(_ label: String, _ size: CGFloat) -> some View {
    VStack(spacing: 4) {
        Rectangle()
            .fill(AppColors.accent)
            .frame(width: size, height: 40)
        Text(label)
            .font(AppTypography.caption)
            .foregroundStyle(AppColors.metadata)
    }
}

@ViewBuilder
private func radiusBox(_ label: String, _ radius: CGFloat) -> some View {
    VStack(spacing: 4) {
        RoundedRectangle(cornerRadius: radius)
            .fill(AppColors.surface)
            .frame(width: 48, height: 48)
            .overlay(
                RoundedRectangle(cornerRadius: radius)
                    .stroke(AppColors.secondary, lineWidth: 1)
            )
        Text(label)
            .font(AppTypography.caption)
            .foregroundStyle(AppColors.metadata)
    }
}

@MainActor @ViewBuilder
private func shadowBox(_ label: String, _ style: AppShadow.ShadowStyle) -> some View {
    VStack(spacing: 4) {
        RoundedRectangle(cornerRadius: CornerRadius.md)
            .fill(AppColors.surface)
            .frame(width: 80, height: 48)
            .themeShadow(style)
        Text(label)
            .font(AppTypography.caption)
            .foregroundStyle(AppColors.metadata)
    }
}

@MainActor @ViewBuilder
private func proportionBox(_ label: String, _ ratio: CGFloat) -> some View {
    VStack(alignment: .leading, spacing: 4) {
        RoundedRectangle(cornerRadius: CornerRadius.md)
            .fill(AppColors.surface)
            .frame(height: 40)
            .frame(maxWidth: .infinity)
            .aspectRatio(ratio, contentMode: .fit)
            .themeShadow(AppShadow.card)
        Text(label)
            .font(AppTypography.caption)
            .foregroundStyle(AppColors.metadata)
    }
}
