// Colors.swift
// App color palette with light/dark mode support.
//
// "Quiet Instrument" palette rules:
// - Copper is the signal accent — used sparingly for active states, progress, key CTAs
// - Most screens live in Ink (dark) / Bone (light)
// - Muted Sage and Soft Steel are supporting tones for metadata, dividers, secondary text
// - No saturated or neon colors anywhere in the UI

import SwiftUI

// MARK: - Raw Palette

/// The raw hex palette. Every named color has a single canonical value.
/// Light/dark adaptation is handled by the semantic layer below.
enum Palette {
    /// #0E1116 — deepest background, dark mode primary
    static let ink = Color(hex: 0x0E1116)
    /// #1A1F27 — elevated surfaces in dark mode
    static let charcoal = Color(hex: 0x1A1F27)
    /// #F3EEE4 — warm off-white, light mode primary
    static let bone = Color(hex: 0xF3EEE4)
    /// #C96A3D — the only warm accent; use sparingly
    static let copper = Color(hex: 0xC96A3D)
    /// #8C9B90 — quiet green-grey for secondary elements
    static let mutedSage = Color(hex: 0x8C9B90)
    /// #95A0AE — cool grey for metadata and timestamps
    static let softSteel = Color(hex: 0x95A0AE)
}

// MARK: - Semantic Colors

/// Semantic color roles that adapt to light and dark mode.
/// Usage: `AppColors.background`, `AppColors.accent`, etc.
enum AppColors {

    /// Primary background.
    /// Dark: Ink (#0E1116)  Light: Bone (#F3EEE4)
    static let background = Color.dynamicColor(
        light: Palette.bone,
        dark: Palette.ink
    )

    /// Elevated surface (cards, sheets, grouped sections).
    /// Dark: Charcoal (#1A1F27)  Light: White
    static let surface = Color.dynamicColor(
        light: .white,
        dark: Palette.charcoal
    )

    /// Primary text.
    /// Dark: Bone (#F3EEE4)  Light: Ink (#0E1116)
    static let text = Color.dynamicColor(
        light: Palette.ink,
        dark: Palette.bone
    )

    /// Signal accent — Copper. Same in both modes.
    /// Use sparingly: active playhead, key CTAs, progress indicators.
    static let accent = Palette.copper

    /// Secondary text and icons.
    /// Dark: Soft Steel (#95A0AE)  Light: Muted Sage (#8C9B90)
    static let secondary = Color.dynamicColor(
        light: Palette.mutedSage,
        dark: Palette.softSteel
    )

    /// Metadata, timestamps, tertiary labels.
    /// Soft Steel in both modes (slightly reduced opacity in light mode).
    static let metadata = Color.dynamicColor(
        light: Palette.softSteel.opacity(0.8),
        dark: Palette.softSteel
    )
}

// MARK: - Color Helpers

extension Color {
    /// Create a color from a hex integer (e.g. 0x0E1116).
    init(hex: UInt32) {
        let r = Double((hex >> 16) & 0xFF) / 255.0
        let g = Double((hex >> 8) & 0xFF) / 255.0
        let b = Double(hex & 0xFF) / 255.0
        self.init(red: r, green: g, blue: b)
    }

    /// Returns a color that resolves to `light` or `dark` depending on the
    /// current color scheme. Works without an asset catalog.
    static func dynamicColor(light: Color, dark: Color) -> Color {
        Color(UIColor { traits in
            traits.userInterfaceStyle == .dark
                ? UIColor(dark)
                : UIColor(light)
        })
    }
}

// MARK: - Preview

#Preview("Color Palette") {
    ScrollView {
        VStack(spacing: 16) {
            Group {
                swatch("Ink", Palette.ink)
                swatch("Charcoal", Palette.charcoal)
                swatch("Bone", Palette.bone)
                swatch("Copper", Palette.copper)
                swatch("Muted Sage", Palette.mutedSage)
                swatch("Soft Steel", Palette.softSteel)
            }

            Divider().padding(.vertical, 8)

            Group {
                semanticSwatch("background", AppColors.background)
                semanticSwatch("surface", AppColors.surface)
                semanticSwatch("text", AppColors.text)
                semanticSwatch("accent", AppColors.accent)
                semanticSwatch("secondary", AppColors.secondary)
                semanticSwatch("metadata", AppColors.metadata)
            }
        }
        .padding()
    }
}

@MainActor
@ViewBuilder
private func swatch(_ name: String, _ color: Color) -> some View {
    HStack {
        RoundedRectangle(cornerRadius: 6)
            .fill(color)
            .frame(width: 48, height: 48)
            .overlay(
                RoundedRectangle(cornerRadius: 6)
                    .stroke(Color.gray.opacity(0.3), lineWidth: 1)
            )
        Text(name)
            .font(.system(.body, design: .monospaced))
        Spacer()
    }
}

@MainActor
@ViewBuilder
private func semanticSwatch(_ name: String, _ color: Color) -> some View {
    HStack {
        RoundedRectangle(cornerRadius: 6)
            .fill(color)
            .frame(width: 48, height: 48)
            .overlay(
                RoundedRectangle(cornerRadius: 6)
                    .stroke(Color.gray.opacity(0.3), lineWidth: 1)
            )
        Text(".\(name)")
            .font(.system(.caption, design: .monospaced))
            .foregroundStyle(AppColors.text)
        Spacer()
    }
}
