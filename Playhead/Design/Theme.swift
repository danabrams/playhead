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

/// Corner radius tokens. Smaller for dense UI, larger for prominent cards.
enum CornerRadius {
    /// 4pt — buttons, small chips
    static let sm: CGFloat = 4
    /// 8pt — cards, input fields
    static let md: CGFloat = 8
    /// 12pt — sheets, modals
    static let lg: CGFloat = 12
    /// 16pt — hero cards, bottom sheets
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

/// Animation presets. All eased, never springy.
/// "Like a well-damped needle" — quick, precise, no overshoot.
enum Motion {
    /// Standard interaction feedback (0.2s ease-in-out).
    static let quick: Animation = .easeInOut(duration: 0.2)

    /// Content transitions, screen changes (0.3s ease-in-out).
    static let standard: Animation = .easeInOut(duration: 0.3)

    /// Deliberate, noticeable transitions (0.45s ease-in-out).
    static let slow: Animation = .easeInOut(duration: 0.45)

    /// Fade-in for lazy-loaded content (0.25s ease-out).
    static let fadeIn: Animation = .easeOut(duration: 0.25)
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
